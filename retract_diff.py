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
# REQUIRES
#    boto3
#
# VERSION
#    1.0: Initial implementation
#
# ================================================================
# END_OF_HEADER
# ================================================================

import csv
import datetime
import os
import shutil
import time

from common import find_latest_date_dir, peak_memory_gb, setup_logging
from config import (
    IMOS_PREFIX,
    LOG_DIR,
    MANIFEST_MAIN,
    NCI_MANIFEST,
    OUTGOING_BASE,
    OUTGOING_RETENTION_DAYS,
    STAGING_BASE,
)


def load_inventory_keys_by_prefix(date_dir, log):
    """Scan the latest inventory CSVs once and bucket every key by its
    top-level IMOS/<prefix>/ folder, so each persistent manifest can be
    diffed against exactly the prefix's current live keys."""
    keys_by_prefix = {}
    inventory_csvs = [f for f in os.listdir(date_dir) if f.endswith(".csv")]
    log.info(f"Scanning {len(inventory_csvs)} inventory CSV files")

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
                keys_by_prefix.setdefault(prefix, set()).add(key)

    return keys_by_prefix


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
    any retracted files, and rewrite the manifest without them. Returns
    the number of entries retracted."""
    persistent_csv = os.path.join(NCI_MANIFEST, f"{prefix}.csv")

    with open(persistent_csv, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        rows = list(reader)

    kept_rows = []
    retracted = 0
    for row in rows:
        key = row[0]
        if key in live_keys:
            kept_rows.append(row)
            continue
        move_retracted_file(key, run_datestamp, log)
        retracted += 1

    if retracted:
        with open(persistent_csv, "w", newline="") as f:
            writer = csv.writer(f)
            if header is not None:
                writer.writerow(header)
            writer.writerows(kept_rows)
        log.info(f"{prefix}: {retracted} entrie(s) retracted and removed from persistent manifest")

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

    log = setup_logging(LOG_DIR, os.path.join("retract_diff", date_dir_name))
    log.info(f"Using inventory date: {date_dir_name}")
    log.info(f"NCI_MANIFEST  = {NCI_MANIFEST}")
    log.info(f"STAGING_BASE  = {STAGING_BASE}")
    log.info(f"OUTGOING_BASE = {OUTGOING_BASE}")

    keys_by_prefix = load_inventory_keys_by_prefix(date_dir, log)

    total_retracted = 0
    for csv_name in sorted(os.listdir(NCI_MANIFEST)):
        if not csv_name.endswith(".csv"):
            continue
        prefix = csv_name[:-len(".csv")]
        live_keys = keys_by_prefix.get(prefix, set())
        total_retracted += process_manifest(prefix, live_keys, run_datestamp, log)

    log.info(f"Total retracted: {total_retracted}")

    cleanup_old_outgoing(OUTGOING_BASE, OUTGOING_RETENTION_DAYS, log)

    elapsed = (time.perf_counter() - start) / 60
    log.info(f"Elapsed: {elapsed:.1f} minutes")
    log.info(f"Peak RAM: {peak_memory_gb():.2f} GB")
    log.info("Done.")


if __name__ == "__main__":
    main()
