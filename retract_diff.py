#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    retract_diff.py
#
# DESCRIPTION
#    Reverse of compare_manifests.py's "shard diff": finds files that are
#    recorded in a persistent manifest (nci_imos_manifest/<prefix>.csv) but
#    are no longer present in the latest IMOS inventory — i.e. retracted
#    upstream since they were downloaded. This is stage 5 of the pipeline,
#    run after mv_to_prod has moved newly-downloaded files into staging.
#
#    For each retracted filepath (e.g. "IMOS/AATAMS/x.nc"):
#      - If it exists under STAGING_BASE, it is moved to
#        OUTGOING_BASE/<run_datestamp>/IMOS/AATAMS/x.nc, preserving the
#        full key structure (no flattening).
#      - If it does not exist under STAGING_BASE, that's an expected case
#        rather than an error: download.py deliberately merges temp
#        manifests into the persistent manifest even for files that failed
#        to download (e.g. already retracted before ever being fetched), so
#        a manifest entry is not a guarantee the file ever landed on disk.
#        This is logged and the run continues.
#    Either way, the entry is removed from the persistent manifest — the
#    file is genuinely gone from the source, so there's nothing further to
#    track.
#
#    Also prunes OUTGOING_BASE: any dated subfolder older than
#    OUTGOING_RETENTION_DAYS is deleted, so retracted files don't
#    accumulate there indefinitely.
#
# SAFETY GUARDS
#    A near-empty or truncated inventory scan (e.g. get_imos_manifest.py
#    downloaded a partial set of CSVs) would otherwise look identical to
#    "everything got retracted", which would move the world into outgoing.
#    Two guards protect against that:
#      - Global: if the total live keys found across all prefixes in the
#        latest inventory is below MIN_LIVE_INVENTORY_RATIO of the total
#        entries currently tracked across all persistent manifests, the
#        whole run aborts before touching anything (PIPELINE_ABORT).
#      - Per-prefix: if one specific prefix's manifest has entries but the
#        latest inventory has zero live keys for that prefix (while other
#        prefixes look normal), that prefix alone is skipped and logged
#        rather than treated as a full retraction.
#
# MEMORY
#    Unlike compare_manifests.py's shard_existing (which only needs to hold
#    what's already been downloaded and tracked — bounded by how much of
#    the mirror is complete), this script needs to know every key that
#    currently exists upstream, across the whole bucket, to find what's
#    missing. On a mirror that's still catching up on backlog, that's a
#    meaningfully bigger set. A real run OOM'd holding it all in memory at
#    once (confirmed via dmesg: ~30GB resident on a 32GB VM). So the live
#    inventory is streamed to small per-prefix files under
#    RETRACT_SCRATCH_DIR during the single CSV scan instead of being kept
#    in a dict of sets, and each persistent manifest is diffed/rewritten
#    one row at a time rather than loading it fully into memory twice
#    (once as the original rows, once as the surviving rows). Peak memory
#    is now bounded by whichever single prefix is largest (e.g. Argo), not
#    the sum of every prefix in the bucket.
#
# REQUIRES
#    boto3
#
# VERSION
#    1.0: Initial implementation
#    1.1: Added the two safety guards described above.
#    1.2: Stream live inventory keys to per-prefix scratch files instead
#         of holding the whole bucket's keys in memory, and stream each
#         persistent manifest's rewrite row-by-row instead of holding both
#         the original and surviving rows in memory. Fixes a real OOM on
#         the datamover VM. Behavior/output unchanged — memory only.
#    1.3: count_persistent_entries_by_prefix and the per-prefix
#         process_manifest call now retry (see common.retry) on a
#         transient read failure instead of aborting the whole run —
#         persistent manifests live on /g/data mounted over the network
#         onto the datamover VM, where a brief hiccup can surface as a
#         spurious mid-read error.
#    1.4: Logging overhaul — one log file per run (see common.py's
#         setup_logging), with a handful of highlight() lines (log path,
#         inventory date, outgoing directory, the PIPELINE_ABORT alarm,
#         final summary) surfaced to the top-level pipeline log;
#         everything else stays file-only.
#
# ================================================================
# END_OF_HEADER
# ================================================================

import csv
import datetime
import os
import shutil
import sys
import time

from common import find_latest_date_dir, highlight, peak_memory_gb, retry, setup_logging
from config import (
    IMOS_PREFIX,
    MANIFEST_MAIN,
    MIN_LIVE_INVENTORY_RATIO,
    NCI_MANIFEST,
    OUTGOING_BASE,
    OUTGOING_RETENTION_DAYS,
    RETRACT_SCRATCH_DIR,
    STAGING_BASE,
)


def stream_inventory_keys_to_scratch(date_dir, scratch_dir, log):
    """Scan the latest inventory CSVs once, writing each key straight to a
    per-prefix file under scratch_dir (one key per line) instead of
    holding every key in memory. Returns {prefix: live_key_count}."""
    if os.path.isdir(scratch_dir):
        shutil.rmtree(scratch_dir)
    os.makedirs(scratch_dir, exist_ok=True)

    live_counts = {}
    file_handles = {}  # prefix -> file handle
    inventory_csvs = [f for f in os.listdir(date_dir) if f.endswith(".csv")]
    log.info(f"Scanning {len(inventory_csvs)} inventory CSV files")

    try:
        for csv_name in inventory_csvs:
            with open(os.path.join(date_dir, csv_name), newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 5:
                        continue
                    key = row[1]
                    if not key.startswith(IMOS_PREFIX):
                        continue
                    prefix = key[len(IMOS_PREFIX):].split("/", 1)[0]

                    if prefix not in file_handles:
                        file_handles[prefix] = open(os.path.join(scratch_dir, f"{prefix}.keys"), "w")
                        live_counts[prefix] = 0

                    file_handles[prefix].write(key + "\n")
                    live_counts[prefix] += 1
    finally:
        for fh in file_handles.values():
            fh.close()

    return live_counts


def load_live_keys_for_prefix(scratch_dir, prefix):
    """Read one prefix's streamed live-key file back into a set. Meant to
    be called just before that prefix is processed, so only one prefix's
    keys are ever held in memory at a time."""
    path = os.path.join(scratch_dir, f"{prefix}.keys")
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {line.rstrip("\n") for line in f}


def count_persistent_entries_by_prefix(nci_manifest):
    """Number of data rows (excluding header) in each persistent manifest
    under nci_manifest, keyed by prefix. Retries a transient per-file read
    failure (see common.retry) instead of aborting the whole run — counts
    stream row-by-row rather than materializing the file, same as before."""
    def _count_one(path):
        with open(path, newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # header
            return sum(1 for _ in reader)

    counts = {}
    for csv_name in sorted(os.listdir(nci_manifest)):
        if not csv_name.endswith(".csv"):
            continue
        prefix = csv_name[:-len(".csv")]
        counts[prefix] = retry(_count_one, os.path.join(nci_manifest, csv_name))
    return counts


def move_retracted_file(key, run_datestamp, log):
    """Move a retracted file from staging to the outgoing quarantine,
    preserving its key structure. Returns True if a file was actually
    moved, False if nothing was found on disk at the expected location."""
    src = os.path.join(STAGING_BASE, key)
    if not os.path.exists(src):
        log.warning(f"{key}: retracted, no local file found under {STAGING_BASE} — removing from manifest only")
        return False

    dst = os.path.join(OUTGOING_BASE, run_datestamp, key)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.move(src, dst)
    log.info(f"{key}: retracted — moved {src} -> {dst}")
    return True


def process_manifest(prefix, live_keys, run_datestamp, log):
    """Diff one persistent manifest against its live inventory keys, move
    any retracted files, and rewrite the manifest without them. Streams
    row-by-row (read, decide, write survivor immediately) rather than
    materializing the original and surviving rows fully in memory at the
    same time. Returns the number of entries retracted."""
    persistent_csv = os.path.join(NCI_MANIFEST, f"{prefix}.csv")
    tmp_csv = persistent_csv + ".tmp"

    retracted = 0
    with open(persistent_csv, newline="") as f_in, open(tmp_csv, "w", newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        header = next(reader, None)
        if header is not None:
            writer.writerow(header)

        for row in reader:
            key = row[0]
            if key in live_keys:
                writer.writerow(row)
                continue
            move_retracted_file(key, run_datestamp, log)
            retracted += 1

    if retracted:
        os.replace(tmp_csv, persistent_csv)
        log.info(f"{prefix}: {retracted} entrie(s) retracted and removed from persistent manifest")
    else:
        os.remove(tmp_csv)

    return retracted


def cleanup_old_outgoing(outgoing_base, retention_days, log):
    """Delete dated subfolders under outgoing_base older than
    retention_days."""
    if not os.path.isdir(outgoing_base):
        return

    cutoff = datetime.datetime.now() - datetime.timedelta(days=retention_days)
    for name in os.listdir(outgoing_base):
        path = os.path.join(outgoing_base, name)
        if not os.path.isdir(path):
            continue
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(path))
        if mtime < cutoff:
            shutil.rmtree(path)
            log.info(f"removed old outgoing folder: {path} (last modified {mtime:%Y-%m-%d})")


def main():
    start = time.perf_counter()
    date_dir_name = find_latest_date_dir(MANIFEST_MAIN)
    if date_dir_name is None:
        print("No dated inventory folder found under", MANIFEST_MAIN)
        return
    date_dir = os.path.join(MANIFEST_MAIN, date_dir_name)
    run_datestamp = datetime.datetime.now().strftime("%Y-%m-%d")

    log, run_id, log_path = setup_logging("retract_diff")
    highlight(log, f"Log file: {log_path}")
    highlight(log, f"Using inventory date: {date_dir_name}")
    log.info(f"NCI_MANIFEST  = {NCI_MANIFEST}")
    log.info(f"STAGING_BASE  = {STAGING_BASE}")
    highlight(log, f"OUTGOING_BASE: {OUTGOING_BASE}")

    live_counts = stream_inventory_keys_to_scratch(date_dir, RETRACT_SCRATCH_DIR, log)
    persistent_counts = count_persistent_entries_by_prefix(NCI_MANIFEST)

    total_live_keys = sum(live_counts.values())
    total_persistent_entries = sum(persistent_counts.values())
    if total_persistent_entries > 0 and total_live_keys < total_persistent_entries * MIN_LIVE_INVENTORY_RATIO:
        ratio = total_live_keys / total_persistent_entries
        highlight(
            log,
            f"[ALARM] PIPELINE_ABORT: latest inventory has only {total_live_keys} live key(s) under "
            f"{IMOS_PREFIX}, but persistent manifests currently track {total_persistent_entries} "
            f"({ratio:.0%}, below the {MIN_LIVE_INVENTORY_RATIO:.0%} minimum). This looks like an "
            f"incomplete or broken inventory scan rather than genuine mass retraction — aborting "
            f"without touching any files or manifests.",
            level="error",
        )
        shutil.rmtree(RETRACT_SCRATCH_DIR, ignore_errors=True)
        sys.exit(1)

    total_retracted = 0
    for prefix, entry_count in persistent_counts.items():
        if live_counts.get(prefix, 0) == 0 and entry_count:
            log.warning(
                f"{prefix}: no live keys found in the latest inventory for this prefix "
                f"(manifest has {entry_count} entrie(s)) — skipping to avoid mass-retracting it"
            )
            continue
        live_keys = load_live_keys_for_prefix(RETRACT_SCRATCH_DIR, prefix)
        # Retrying the whole call is safe even if a prior attempt got partway
        # through moving files before failing: a retry just re-reads the
        # still-untouched persistent_csv (process_manifest only replaces it
        # at the very end) and re-processes every row: files already moved
        # out on the failed attempt simply hit the expected "already retracted,
        # no local file found" branch and get cleaned up from the manifest
        # like normal, so no special-casing is needed here.
        total_retracted += retry(process_manifest, prefix, live_keys, run_datestamp, log)
        live_keys = None  # release before moving to the next prefix

    shutil.rmtree(RETRACT_SCRATCH_DIR, ignore_errors=True)
    cleanup_old_outgoing(OUTGOING_BASE, OUTGOING_RETENTION_DAYS, log)

    elapsed = (time.perf_counter() - start) / 60
    highlight(
        log,
        f"Done — {total_retracted} entrie(s) retracted in {elapsed:.1f} min "
        f"(peak RAM {peak_memory_gb():.2f} GB)",
    )


if __name__ == "__main__":
    main()
