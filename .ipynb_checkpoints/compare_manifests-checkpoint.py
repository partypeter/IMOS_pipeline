#!/usr/bin/env python3
"""
Compares the latest local S3 inventory CSVs against per-prefix NCI manifests
(one CSV per top-level IMOS/<prefix>/ folder), and writes any files found in
the inventory but missing from the relevant manifest into a per-prefix temp
manifest under nci_manifest/temp/. Those temp manifests are intended to drive
Globus batch file generation; once Globus signals the transfer is complete,
a separate step should append each temp manifest into its persistent
manifest and delete the temp file.

If a prefix already has a pending temp manifest (i.e. a previous batch
hasn't been confirmed and merged yet), that prefix is skipped entirely for
this run, so a re-run never overwrites or double-queues a pending batch.

Manifests for newly-discovered IMOS prefixes are created automatically.

Assumes the whole-bucket inventory CSVs for the date being used have
already been downloaded and decompressed (see download_inventory_files.py)
into manifest_main/<date>/*.csv.

Persistent manifests live at nci_manifest/<prefix>.csv
Temp manifests live at      nci_manifest/temp/<prefix>.csv
Both use header: filepath,checksum

Log files live at log_dir/<date>/<YYYYMMDDHHMMSS>.txt

Requires: boto3
"""

import csv
import datetime
import logging
import os
import re
import sys
import time
import resource

import boto3
from botocore import UNSIGNED
from botocore.client import Config


source_bucket = "imos-data"


imos_prefix = "IMOS/"
manifest_main = "/g/data/sh74/admin/globus/test/"
nci_manifest = os.path.join(manifest_main, "nci_imos_manifest")
temp_dir = os.path.join(nci_manifest, "temp")
log_dir = os.path.join(manifest_main, "logs/manifest_comparison")

date_dir_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z$")


def setup_logging(date_dir_name, log_dir):
    log_subdir = os.path.join(log_dir, date_dir_name)
    os.makedirs(log_subdir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = os.path.join(log_subdir, f"{timestamp}.txt")

    log = logging.getLogger()
    log.setLevel(logging.INFO)

    for handler in log.handlers[:]:
        handler.close()
        log.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s %(message)s")

    file_handler = logging.FileHandler(log_path, mode="a")
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)

    return log


def get_imos_prefixes(s3):
    """Top-level folder names under s3://imos-data/IMOS/, e.g. 'AATAMS'."""
    resp = s3.list_objects_v2(Bucket=source_bucket, Prefix=imos_prefix, Delimiter="/")
    prefixes = []
    for cp in resp.get("CommonPrefixes", []):
        # cp["Prefix"] looks like "IMOS/AATAMS/"
        name = cp["Prefix"][len(imos_prefix):].rstrip("/")
        if name:
            prefixes.append(name)
    return prefixes


def find_latest_date_dir(outdir):
    candidates = [
        d for d in os.listdir(outdir)
        if date_dir_re.match(d) and os.path.isdir(os.path.join(outdir, d))
    ]
    if not candidates:
        return None
    # Date strings sort correctly as-is (YYYY-MM-DDTHH-MM-Z-like format).
    return sorted(candidates)[-1]


def load_existing_filepaths(manifest_csv):
    existing = set()
    with open(manifest_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing.add(row["filepath"])
    return existing


def main():
    start = time.perf_counter()
    date_dir_name = find_latest_date_dir(manifest_main)
    if date_dir_name is None:
        print("No dated inventory folder found under", manifest_main)
        return
    date_dir = os.path.join(manifest_main, date_dir_name)

    log = setup_logging(date_dir_name, log_dir)
    log.info(f"Using inventory date: {date_dir_name}")
    log.info(f"manifest_main = {manifest_main}")
    log.info(f"nci_manifest  = {nci_manifest}")
    log.info(f"temp_dir      = {temp_dir}")
    log.info(f"log_dir       = {log_dir}")

    os.makedirs(nci_manifest, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    if os.listdir(temp_dir):
        log.info(f"{temp_dir} is not empty — a previous batch is still pending confirmation. Aborting.")
        return

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    prefixes = get_imos_prefixes(s3)
    log.info(f"Found {len(prefixes)} IMOS prefixes")

    shard_existing = {}
    valid_prefixes = set()
    for prefix in prefixes:
        temp_csv = os.path.join(temp_dir, f"{prefix}.csv")
        if os.path.exists(temp_csv):
            log.info(f"{prefix}: pending temp manifest already exists — skipping until confirmed/merged")
            continue

        shard_csv = os.path.join(nci_manifest, f"{prefix}.csv")
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

                    if not key.startswith(imos_prefix):
                        continue
                    remainder = key[len(imos_prefix):]
                    prefix = remainder.split("/", 1)[0]

                    if prefix not in valid_prefixes:
                        continue
                    if key in shard_existing[prefix]:
                        continue

                    if prefix not in temp_writers:
                        temp_csv = os.path.join(temp_dir, f"{prefix}.csv")
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

    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_gb = peak_kb / (1024 ** 2)
    elapsed = (time.perf_counter() - start)/60
    print(f"Elapsed: {elapsed:.1f} minutes")
    print(f"{peak_gb:.2f} GB")

    log.info("Done.")


if __name__ == "__main__":
    main()