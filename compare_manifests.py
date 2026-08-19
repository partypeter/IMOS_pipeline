#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    compare_manifests.py
#
# DESCRIPTION
#    Compares the latest local S3 inventory CSVs against per-prefix NCI
#    manifests (one CSV per top-level IMOS/<prefix>/ folder), and writes
#    any files found in the inventory but missing from the relevant
#    manifest into a per-prefix temp manifest under nci_imos_manifest/temp/.
#    This is stage 2 of the pipeline ("shard diff") — its temp manifests
#    drive download.py (stage 3), which downloads the new files and merges
#    each temp manifest back into its persistent manifest.
#
#    If a prefix already has a pending temp manifest (i.e. a previous batch
#    hasn't been confirmed and merged yet), that prefix is skipped entirely
#    for this run, so a re-run never overwrites or double-queues a pending
#    batch.
#
#    Manifests for newly-discovered IMOS prefixes are created automatically.
#
#    Assumes the whole-bucket inventory CSVs for the date being used have
#    already been downloaded and decompressed (see get_imos_manifest.py)
#    into MANIFEST_MAIN/<date>/*.csv.
#
#    Persistent manifests live at nci_imos_manifest/<prefix>.csv
#    Temp manifests live at       nci_imos_manifest/temp/<prefix>.csv
#    Both use header: filepath,checksum
#
# REQUIRES
#    boto3
#
# VERSION
#    1.0: Initial implementation
#    1.1: Paths, logging, S3 client, and date-dir lookup moved to
#         config.py / common.py.
#
# ================================================================
# END_OF_HEADER
# ================================================================

import csv
import os
import time

from common import find_latest_date_dir, get_s3_client, peak_memory_gb, setup_logging
from config import IMOS_PREFIX, LOG_DIR, MANIFEST_MAIN, NCI_MANIFEST, SOURCE_BUCKET, TEMP_DIR


def get_imos_prefixes(s3):
    """Top-level folder names under s3://imos-data/IMOS/, e.g. 'AATAMS'."""
    resp = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=IMOS_PREFIX, Delimiter="/")
    prefixes = []
    for cp in resp.get("CommonPrefixes", []):
        # cp["Prefix"] looks like "IMOS/AATAMS/"
        name = cp["Prefix"][len(IMOS_PREFIX):].rstrip("/")
        if name:
            prefixes.append(name)
    return prefixes


def load_existing_filepaths(manifest_csv):
    existing = set()
    with open(manifest_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing.add(row["filepath"])
    return existing


def main():
    start = time.perf_counter()
    date_dir_name = find_latest_date_dir(MANIFEST_MAIN)
    if date_dir_name is None:
        print("No dated inventory folder found under", MANIFEST_MAIN)
        return
    date_dir = os.path.join(MANIFEST_MAIN, date_dir_name)

    log = setup_logging(LOG_DIR, os.path.join("manifest_comparison", date_dir_name))
    log.info(f"Using inventory date: {date_dir_name}")
    log.info(f"MANIFEST_MAIN = {MANIFEST_MAIN}")
    log.info(f"NCI_MANIFEST  = {NCI_MANIFEST}")
    log.info(f"TEMP_DIR      = {TEMP_DIR}")

    os.makedirs(NCI_MANIFEST, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    if os.listdir(TEMP_DIR):
        log.info(f"{TEMP_DIR} is not empty — a previous batch is still pending confirmation. Aborting.")
        return

    s3 = get_s3_client()
    prefixes = get_imos_prefixes(s3)
    log.info(f"Found {len(prefixes)} IMOS prefixes")

    shard_existing = {}
    valid_prefixes = set()
    for prefix in prefixes:
        temp_csv = os.path.join(TEMP_DIR, f"{prefix}.csv")
        if os.path.exists(temp_csv):
            log.info(f"{prefix}: pending temp manifest already exists — skipping until confirmed/merged")
            continue

        shard_csv = os.path.join(NCI_MANIFEST, f"{prefix}.csv")
        if not os.path.exists(shard_csv):
            with open(shard_csv, "w", newline="") as f:
                csv.writer(f).writerow(["filepath", "checksum"])
            log.info(f"Created new manifest for prefix '{prefix}': {shard_csv}")

        shard_existing[prefix] = load_existing_filepaths(shard_csv)
        valid_prefixes.add(prefix)

    # New entries are streamed straight to each prefix's temp CSV as they're
    # found, via a file handle opened lazily on first use — this avoids
    # holding a second full copy of the new-entries data in memory
    # alongside shard_existing (which roughly doubled peak RAM on a
    # bootstrap-style run where most files are new).
    temp_writers = {}  # prefix -> (file handle, csv.writer)
    new_counts = {prefix: 0 for prefix in valid_prefixes}

    inventory_csvs = [f for f in os.listdir(date_dir) if f.endswith(".csv")]
    log.info(f"Scanning {len(inventory_csvs)} inventory CSV files")

    try:
        for csv_name in inventory_csvs:
            with open(os.path.join(date_dir, csv_name), newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 5:
                        continue
                    key, etag = row[1], row[4]

                    if not key.startswith(IMOS_PREFIX):
                        continue
                    remainder = key[len(IMOS_PREFIX):]
                    prefix = remainder.split("/", 1)[0]

                    if prefix not in valid_prefixes:
                        continue
                    if key in shard_existing[prefix]:
                        continue

                    if prefix not in temp_writers:
                        temp_csv = os.path.join(TEMP_DIR, f"{prefix}.csv")
                        fh = open(temp_csv, "w", newline="")
                        writer = csv.writer(fh)
                        writer.writerow(["filepath", "checksum"])
                        temp_writers[prefix] = (fh, writer)

                    temp_writers[prefix][1].writerow([key, etag])
                    shard_existing[prefix].add(key)  # avoid duplicate adds within this run
                    new_counts[prefix] += 1
    finally:
        for fh, _ in temp_writers.values():
            fh.close()

    for prefix, count in new_counts.items():
        if count:
            log.info(f"{prefix}: wrote {count} new file(s) to temp manifest")

    elapsed = (time.perf_counter() - start) / 60
    log.info(f"Elapsed: {elapsed:.1f} minutes")
    log.info(f"Peak RAM: {peak_memory_gb():.2f} GB")
    log.info("Done.")


if __name__ == "__main__":
    main()
