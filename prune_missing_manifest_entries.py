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
# VERSION
#    1.0: Initial implementation
#
# ================================================================
# END_OF_HEADER
# ================================================================

import argparse
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


def prune_missing_prefix(manifest_dir, prefix, apply_changes, log):
    """Stream one persistent manifest, checking every row's file against
    disk. In dry-run mode (apply_changes=False), nothing is written —
    rows are only read and reported. In apply mode, survivors are
    streamed straight to a tmp file, atomically swapped in at the end
    (or discarded if nothing needed dropping), same pattern as
    retract_diff.py's process_manifest. Returns (kept, dropped)."""
    persistent_csv = os.path.join(manifest_dir, f"{prefix}.csv")
    tmp_csv = persistent_csv + ".tmp"

    kept = 0
    dropped = 0

    f_out = open(tmp_csv, "w", newline="") if apply_changes else None
    writer = csv.writer(f_out) if f_out else None
    try:
        with open(persistent_csv, newline="") as f_in:
            reader = csv.reader(f_in)
            header = next(reader, None)
            if writer is not None and header is not None:
                writer.writerow(header)

            for row in reader:
                key = row[0]
                base = WAVE_BUOY_BASE if is_wave_buoy_key(key) else AODN_BASE
                local_path = os.path.join(base, key)
                if os.path.exists(local_path):
                    kept += 1
                    if writer is not None:
                        writer.writerow(row)
                else:
                    dropped += 1
                    log.info(f"{prefix}: MISSING {local_path}")
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
    args = parser.parse_args()

    log, run_id, log_path = setup_logging("prune_missing_manifest_entries")
    highlight(log, f"Log file: {log_path}")
    highlight(log, "APPLY MODE — manifests will be rewritten" if args.apply else "DRY RUN — no manifests will be modified")

    total_kept = 0
    total_dropped = 0
    for manifest_dir in MANIFEST_DIRS:
        for csv_name in sorted(os.listdir(manifest_dir)):
            if not csv_name.endswith(".csv"):
                continue
            prefix = csv_name[:-len(".csv")]
            kept, dropped = retry(prune_missing_prefix, manifest_dir, prefix, args.apply, log)
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
