#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    prune_missing_manifest_entries.py [--apply]
#
# DESCRIPTION
#    One-time maintenance script — NOT part of the daily pipeline and not
#    invoked by run_imos_pipeline.sh. Run manually, once, after
#    compare_manifests.py/retract_diff.py's S3-Inventory-key URL-decode
#    fix has landed.
#
#    Streams every persistent manifest (both NCI_MANIFEST and
#    WAVE_BUOY_MANIFEST) and drops any row whose file does not actually
#    exist on disk under its final destination (AODN_BASE, or
#    WAVE_BUOY_BASE for a wave buoy key — see config.is_wave_buoy_key).
#    This catches accumulated stale manifest entries regardless of how
#    they got that way — e.g. a key that was recorded and requested in
#    its still-URL-encoded form before the decode fix, and so could never
#    download successfully under the name the manifest claims, or any
#    other legacy entry with no corresponding file.
#
#    Deliberately does NOT touch any local files. Unlike retract_diff.py,
#    every row this script drops already has no file to move — the whole
#    point is "the file isn't there" — so this only ever rewrites
#    manifest CSVs.
#
#    Defaults to a dry run: reports what would be dropped (per prefix,
#    and a final total) without changing anything on disk. Pass --apply
#    to actually rewrite the manifests.
#
#    Once entries are pruned here, nothing further is needed — the
#    pipeline's own next scheduled run naturally rediscovers each pruned
#    key as "new" (compare_manifests.py) and re-downloads it under its
#    correct name (download.py).
#
#    Existence checks are the bottleneck (each is a blocking stat() call
#    over /g/data, and one prefix alone — AUV — has ~82M rows), so they
#    run concurrently across --workers threads, BATCH_SIZE rows at a
#    time: threads (not processes) because the wait is on network I/O,
#    which releases the GIL, so many stat()s can be in flight at once
#    with none of multiprocessing's pickling/IPC overhead; batching keeps
#    memory bounded to one batch regardless of a prefix's total size.
#    Uses os.stat() directly rather than os.path.exists(), which silently
#    treats *any* OSError as "missing" — a transient NFS hiccup could
#    otherwise get misread as a genuinely absent file and wrongly drop a
#    real entry. Only FileNotFoundError means "missing" here; anything
#    else propagates up to the per-prefix retry() below instead of being
#    guessed at.
#
# VERSION
#    1.0: Initial implementation
#    1.1: Existence checks parallelized across a thread pool (see above)
#         instead of one blocking stat() call at a time — a full run
#         across every manifest was originally estimated at anywhere from
#         hours to multiple days, sequentially. Also switched from
#         os.path.exists() to an explicit os.stat()/FileNotFoundError
#         check, fixing a latent correctness gap (see above) that
#         predates this version but was worth closing while touching this
#         code path.
#
# ================================================================
# END_OF_HEADER
# ================================================================

import argparse
import concurrent.futures
import csv
import os

from common import highlight, peak_memory_gb, retry, setup_logging
from config import (
    AODN_BASE,
    NCI_MANIFEST,
    WAVE_BUOY_BASE,
    WAVE_BUOY_MANIFEST,
    is_wave_buoy_key,
)

MANIFEST_DIRS = (NCI_MANIFEST, WAVE_BUOY_MANIFEST)
BATCH_SIZE = 2000
DEFAULT_WORKERS = 16


def _file_exists(path):
    """True if path exists, False if it definitely doesn't (ENOENT). Any
    other OSError (e.g. a transient NFS hiccup) is NOT swallowed — unlike
    os.path.exists()'s built-in behavior — so a flaky stat() can't get
    silently misread as "missing"; it propagates up instead, triggering
    prune_missing_prefix's caller-level retry() rather than guessing."""
    try:
        os.stat(path)
        return True
    except FileNotFoundError:
        return False


def _local_path(row):
    key = row[0]
    base = WAVE_BUOY_BASE if is_wave_buoy_key(key) else AODN_BASE
    return os.path.join(base, key)


def _process_batch(batch, executor, prefix, log, missing_list_fh, writer):
    paths = [_local_path(row) for row in batch]
    exists_flags = executor.map(_file_exists, paths)

    kept = 0
    dropped = 0
    for row, path, exists in zip(batch, paths, exists_flags):
        if exists:
            kept += 1
            if writer is not None:
                writer.writerow(row)
        else:
            dropped += 1
            log.info(f"{prefix}: MISSING {path}")
            missing_list_fh.write(path + "\n")
    return kept, dropped


def prune_missing_prefix(manifest_dir, prefix, apply_changes, log, missing_list_fh, workers):
    """Stream one persistent manifest BATCH_SIZE rows at a time, checking
    each batch's files against disk concurrently across `workers` threads
    (see module docstring). In dry-run mode (apply_changes=False),
    nothing is written — rows are only read and reported. In apply mode,
    survivors are streamed straight to a tmp file, atomically swapped in
    at the end (or discarded if nothing needed dropping), same pattern as
    retract_diff.py's process_manifest. Returns (kept, dropped)."""
    persistent_csv = os.path.join(manifest_dir, f"{prefix}.csv")
    tmp_csv = persistent_csv + ".tmp"

    kept = 0
    dropped = 0

    f_out = open(tmp_csv, "w", newline="") if apply_changes else None
    writer = csv.writer(f_out) if f_out else None
    try:
        with open(persistent_csv, newline="") as f_in, \
                concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            reader = csv.reader(f_in)
            header = next(reader, None)
            if writer is not None and header is not None:
                writer.writerow(header)

            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    b_kept, b_dropped = _process_batch(batch, executor, prefix, log, missing_list_fh, writer)
                    kept += b_kept
                    dropped += b_dropped
                    batch = []
            if batch:
                b_kept, b_dropped = _process_batch(batch, executor, prefix, log, missing_list_fh, writer)
                kept += b_kept
                dropped += b_dropped
    finally:
        if f_out is not None:
            f_out.close()

    if apply_changes:
        if dropped:
            os.replace(tmp_csv, persistent_csv)
        else:
            os.remove(tmp_csv)

    return kept, dropped


def main():
    parser = argparse.ArgumentParser(
        description="Drop persistent manifest entries whose file doesn't exist on disk."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually rewrite manifests. Without this, only reports what would be dropped.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Concurrent existence-check threads per prefix (default {DEFAULT_WORKERS}).",
    )
    args = parser.parse_args()

    log, run_id, log_path = setup_logging("prune_missing_manifest_entries")
    highlight(log, f"Log file: {log_path}")
    highlight(log, f"Workers: {args.workers}")
    highlight(log, "APPLY MODE — manifests will be rewritten" if args.apply else "DRY RUN — no manifests will be modified")

    missing_list_path = os.path.join(os.path.dirname(log_path), f"{run_id}_missing.lst")
    highlight(log, f"Missing-paths list: {missing_list_path}")

    total_kept = 0
    total_dropped = 0
    with open(missing_list_path, "w") as missing_list_fh:
        for manifest_dir in MANIFEST_DIRS:
            for csv_name in sorted(os.listdir(manifest_dir)):
                if not csv_name.endswith(".csv"):
                    continue
                prefix = csv_name[:-len(".csv")]
                kept, dropped = retry(
                    prune_missing_prefix, manifest_dir, prefix, args.apply, log, missing_list_fh, args.workers
                )
                total_kept += kept
                total_dropped += dropped
                if dropped:
                    highlight(log, f"{prefix}: {dropped} missing entrie(s) found ({kept} kept)")

    highlight(
        log,
        f"Done — {total_dropped} missing entrie(s) found across all manifests ({total_kept} kept), "
        f"peak RAM {peak_memory_gb():.2f} GB"
        + ("" if args.apply else " — DRY RUN, nothing written. Re-run with --apply to commit."),
    )


if __name__ == "__main__":
    main()
