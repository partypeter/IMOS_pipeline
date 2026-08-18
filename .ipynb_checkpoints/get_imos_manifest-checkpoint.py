#!/usr/bin/env python3
"""
Finds the previous day's S3 Inventory manifest for the IMOS daily
whole-bucket inventory, downloads it, then downloads and decompresses
every data file it lists.
Requires: boto3 (pip install boto3 --break-system-packages)
"""
import datetime
import gzip
import json
import logging
import os
import shutil
import sys
import boto3
from botocore import UNSIGNED
from botocore.client import Config

inventory_bucket = "imos-data-inventory"
base_s3_prefix = "imos-data/Daily_Inventory_Whole_Bucket"
manifest_main = "/g/data/sh74/admin/globus/test/"

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

def main():
    os.makedirs(manifest_main, exist_ok=True)

    prev_date = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    manifest_prefix = f"{prev_date}T01-00Z"

    log_dir = os.path.join(manifest_main, "logs/manifest_download")
    log = setup_logging(manifest_prefix, log_dir)

    day_dir = os.path.join(manifest_main, manifest_prefix)
    os.makedirs(day_dir, exist_ok=True)
    existing = [f for f in os.listdir(day_dir) if f.endswith(".csv") or f.endswith(".gz")]
    if existing:
        log.info(f"CSV or GZ files already present in {day_dir} — skipping.")
        return
    log.info(f"Manifest date: {manifest_prefix}")
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    manifest_key = f"{base_s3_prefix}/{manifest_prefix}/manifest.json"
    manifest_local = os.path.join(day_dir, "manifest.json")
    log.info(f"Fetching manifest from: s3://{inventory_bucket}/{manifest_key}")
    s3.download_file(inventory_bucket, manifest_key, manifest_local)
    log.info(f"download: s3://{inventory_bucket}/{manifest_key} to {manifest_local}")
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


if __name__ == "__main__":
    main()