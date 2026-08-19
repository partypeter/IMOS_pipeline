#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    get_imos_manifest.py
#
# DESCRIPTION
#    Finds the previous day's S3 Inventory manifest for the IMOS daily
#    whole-bucket inventory, downloads it, then downloads and decompresses
#    every data file it lists into a dated directory under MANIFEST_MAIN
#    (config.py). This is stage 1 of the pipeline — its output feeds
#    compare_manifests.py.
#
#    Also prunes old dated inventory directories, keeping only the most
#    recent few, so MANIFEST_MAIN doesn't grow unbounded.
#
# REQUIRES
#    boto3 (pip install boto3 --break-system-packages)
#
# VERSION
#    1.0: Initial implementation
#    1.1: Paths and logging moved to config.py / common.py; cleanup now
#         only ever touches dated inventory directories (previously relied
#         on alphabetical sort ordering to spare other subdirectories).
#
# ================================================================
# END_OF_HEADER
# ================================================================

import datetime
import gzip
import json
import os
import shutil

from common import DATE_DIR_RE, get_s3_client, setup_logging
from config import BASE_S3_PREFIX, INVENTORY_BUCKET, LOG_DIR, MANIFEST_MAIN


def cleanup_old_manifests(manifest_main, log, keep=3):
    """Remove all but the `keep` most recent dated inventory directories.
    Only directories matching the dated-inventory name pattern are ever
    considered, so other subdirectories under manifest_main (e.g.
    nci_imos_manifest, download_test_datamover, logs) are never touched."""
    entries = [
        d for d in os.listdir(manifest_main)
        if DATE_DIR_RE.match(d) and os.path.isdir(os.path.join(manifest_main, d))
    ]
    entries.sort()
    to_remove = entries[:-keep] if len(entries) > keep else []
    for d in to_remove:
        path = os.path.join(manifest_main, d)
        shutil.rmtree(path)
        log.info(f"removed old manifest dir: {path}")


def main():
    os.makedirs(MANIFEST_MAIN, exist_ok=True)

    prev_date = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    manifest_prefix = f"{prev_date}T01-00Z"

    log = setup_logging(LOG_DIR, os.path.join("manifest_download", manifest_prefix))

    day_dir = os.path.join(MANIFEST_MAIN, manifest_prefix)
    os.makedirs(day_dir, exist_ok=True)
    existing = [f for f in os.listdir(day_dir) if f.endswith(".csv") or f.endswith(".gz")]
    if existing:
        log.info(f"CSV or GZ files already present in {day_dir} — skipping.")
        return
    log.info(f"Manifest date: {manifest_prefix}")
    s3 = get_s3_client()
    manifest_key = f"{BASE_S3_PREFIX}/{manifest_prefix}/manifest.json"
    manifest_local = os.path.join(day_dir, "manifest.json")
    log.info(f"Fetching manifest from: s3://{INVENTORY_BUCKET}/{manifest_key}")
    s3.download_file(INVENTORY_BUCKET, manifest_key, manifest_local)
    log.info(f"download: s3://{INVENTORY_BUCKET}/{manifest_key} to {manifest_local}")
    with open(manifest_local) as f:
        manifest = json.load(f)
    # destinationBucket in the manifest is an ARN like
    # "arn:aws:s3:::imos-data-inventory" — take the bucket name after the
    # final colon.
    dest_bucket = manifest["destinationBucket"].split(":::")[-1]
    log.info(f"Destination bucket: {dest_bucket}")
    log.info(f"Output directory:   {day_dir}")
    for entry in manifest["files"]:
        key = entry["key"]
        local_gz = os.path.join(day_dir, os.path.basename(key))
        s3.download_file(dest_bucket, key, local_gz)
        log.info(f"download: s3://{dest_bucket}/{key} to {local_gz}")
        local_csv = local_gz[:-3] if local_gz.endswith(".gz") else local_gz
        with gzip.open(local_gz, "rb") as f_in, open(local_csv, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(local_gz)
        log.info(f"decompressed: {local_gz} -> {local_csv}")
    log.info("Done.")

    cleanup_old_manifests(MANIFEST_MAIN, log)


if __name__ == "__main__":
    main()
