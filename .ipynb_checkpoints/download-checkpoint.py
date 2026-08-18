#!/usr/bin/env python3
"""
Downloads every file listed in the per-prefix temp manifests under temp_dir,
recreating the S3 key structure under outdir (e.g. key "IMOS/AATAMS/x.nc"
lands at outdir/IMOS/AATAMS/x.nc).

Deliberately simple and sequential — no parallelism yet. The point of this
run is just to see how it performs and whether parallelism actually turns
out to be necessary before adding that complexity.

Once all downloads are attempted, each temp manifest under temp_dir is
appended into its persistent manifest under nci_manifest and then deleted —
regardless of whether every individual file succeeded. A file that failed
to download (e.g. NoSuchKey, because it was retracted from the source
bucket before ever being downloaded) still gets merged into the persistent
manifest as if present; this is a deliberate, accepted simplification —
a separate reverse-diff script (persistent manifest vs. latest inventory,
not yet built) is responsible for catching retracted/missing files, so
this script doesn't need to handle that case itself.

Checksums: requests ChecksumMode='ENABLED' on every GetObject and, when S3
reports ChecksumType == 'FULL_OBJECT' (a checksum that's a genuine hash of
the whole file, not a multipart composite), verifies the download by
computing the same algorithm locally while streaming and comparing digests.
Preference order: SHA256, SHA1, CRC64NVME, CRC32C, CRC32 — first one S3
actually returned wins. The filepath,checksum temp CSVs are only used for
the file list; the checksum column (S3 Inventory ETag) is intentionally
ignored, since it isn't a reliable full-object hash for multipart uploads.

If no FULL_OBJECT checksum is available for a file (common for older
objects), falls back to a Content-Length size check and logs that no
checksum verification was possible.

Everything — per-file speed, per-file checksum result, and an overall
summary — goes to a single timestamped log file under log_dir.

Requires: boto3, awscrt (for CRC32C/CRC64NVME verification when available;
falls back gracefully to SHA256/SHA1/CRC32 if awscrt isn't installed)
"""

import csv
import datetime
import logging
import os
import sys
import time
import resource

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.httpchecksum import Crc32Checksum, Sha1Checksum, Sha256Checksum

try:
    from botocore.httpchecksum import CrtCrc32cChecksum, CrtCrc64NvmeChecksum
    have_crt_checksums = True
except ImportError:
    have_crt_checksums = False

source_bucket = "imos-data"
manifest_main = "/g/data/sh74/admin/globus/test/"
nci_manifest = os.path.join(manifest_main, "nci_imos_manifest")
temp_dir = os.path.join(manifest_main, "nci_imos_manifest/temp")
outdir = os.path.join(manifest_main, "download_test")  
log_dir = os.path.join(manifest_main, "logs/download_test")

chunk_size = 8 * 1024 * 1024  # 8 MB
progress_interval = 3600  # seconds — how often to log a running speed estimate

# Preference order: (response field name, checksum class)
checksum_candidates = [("ChecksumSHA256", Sha256Checksum), ("ChecksumSHA1", Sha1Checksum)]
if have_crt_checksums:
    checksum_candidates += [
        ("ChecksumCRC64NVME", CrtCrc64NvmeChecksum),
        ("ChecksumCRC32C", CrtCrc32cChecksum),
    ]
checksum_candidates.append(("ChecksumCRC32", Crc32Checksum))


def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = os.path.join(log_dir, f"{timestamp}.txt")

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


def iter_temp_manifest_keys(temp_dir):
    for csv_name in sorted(os.listdir(temp_dir)):
        if not csv_name.endswith(".csv"):
            continue
        with open(os.path.join(temp_dir, csv_name), newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row["filepath"]


def download_one(s3, key, log):
    local_path = os.path.join(outdir, key)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        response = s3.get_object(Bucket=source_bucket, Key=key, ChecksumMode="ENABLED")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        log.error(f"{key}: FAILED to fetch ({error_code}) — skipping")
        return None

    checksum = None
    field_name = None
    if response.get("ChecksumType") == "FULL_OBJECT":
        for candidate_field, checksum_cls in checksum_candidates:
            if candidate_field in response:
                field_name = candidate_field
                checksum = checksum_cls()
                break

    start = time.perf_counter()
    bytes_written = 0
    with open(local_path, "wb") as f:
        for chunk in response["Body"].iter_chunks(chunk_size=chunk_size):
            f.write(chunk)
            bytes_written += len(chunk)
            if checksum is not None:
                checksum.update(chunk)
    elapsed = time.perf_counter() - start

    speed_mbps = (bytes_written / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
    log.info(f"{key}: {bytes_written} bytes in {elapsed:.2f}s ({speed_mbps:.2f} MB/s)")

    if checksum is not None:
        expected = response[field_name]
        actual = checksum.b64digest()
        if actual == expected:
            log.info(f"{key}: checksum OK ({field_name})")
        else:
            log.error(f"{key}: CHECKSUM MISMATCH ({field_name}) expected={expected} actual={actual}")
    else:
        expected_size = response.get("ContentLength")
        if expected_size is not None and expected_size == bytes_written:
            log.info(f"{key}: no full-object checksum available — size check OK ({bytes_written} bytes)")
        else:
            log.warning(f"{key}: no full-object checksum available — size mismatch (expected={expected_size}, got={bytes_written})")

    return bytes_written


def merge_temp_into_persistent(log):
    for csv_name in sorted(os.listdir(temp_dir)):
        if not csv_name.endswith(".csv"):
            continue
        temp_csv = os.path.join(temp_dir, csv_name)
        persistent_csv = os.path.join(nci_manifest, csv_name)

        try:
            with open(temp_csv, newline="") as f:
                rows = list(csv.DictReader(f))

            if not os.path.exists(persistent_csv):
                with open(persistent_csv, "w", newline="") as f:
                    csv.writer(f).writerow(["filepath", "checksum"])

            with open(persistent_csv, "a", newline="") as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow([row["filepath"], row["checksum"]])

            os.remove(temp_csv)
            log.info(f"{csv_name}: merged {len(rows)} entrie(s) into persistent manifest, temp file removed")
        except Exception as e:
            log.error(f"{csv_name}: FAILED to merge into persistent manifest ({e}) — temp file left in place")


def main():
    overall_start = time.perf_counter()
    log = setup_logging(log_dir)
    log.info(f"temp_dir = {temp_dir}")
    log.info(f"outdir   = {outdir}")

    os.makedirs(outdir, exist_ok=True)

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    total_files = 0
    total_failed = 0
    total_bytes = 0
    last_report = overall_start
    for key in iter_temp_manifest_keys(temp_dir):
        result = download_one(s3, key, log)
        if result is None:
            total_failed += 1
        else:
            total_bytes += result
            total_files += 1

        now = time.perf_counter()
        if now - last_report >= progress_interval:
            elapsed_so_far = now - overall_start
            speed_so_far = (total_bytes / (1024 * 1024)) / elapsed_so_far if elapsed_so_far > 0 else 0.0
            log.info(
                f"Progress: {total_files} file(s) ok, {total_failed} failed, {total_bytes / (1024 * 1024):.2f} MB, "
                f"{elapsed_so_far / 3600:.2f}h elapsed, {speed_so_far:.2f} MB/s average so far"
            )
            last_report = now

    log.info("Downloads complete — merging temp manifests into persistent manifest")
    merge_temp_into_persistent(log)

    overall_elapsed = time.perf_counter() - overall_start
    overall_speed = (total_bytes / (1024 * 1024)) / overall_elapsed if overall_elapsed > 0 else 0.0

    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_gb = peak_kb / (1024 ** 2)

    log.info(
        f"Done. {total_files} file(s) ok, {total_failed} failed, {total_bytes / (1024 * 1024):.2f} MB, "
        f"{overall_elapsed:.2f}s total, {overall_speed:.2f} MB/s average, "
        f"{peak_gb:.2f} GB peak RAM"
    )


if __name__ == "__main__":
    main()