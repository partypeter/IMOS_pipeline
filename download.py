#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    download.py
#
# DESCRIPTION
#    Downloads every file listed in the per-prefix temp manifests under
#    TEMP_DIR and WAVE_BUOY_TEMP_DIR (MANIFEST_GROUPS — one pair per
#    manifest group, see config.manifest_dir_for_prefix()), recreating
#    the S3 key structure under OUTDIR (e.g. key "IMOS/AATAMS/x.nc" lands
#    at OUTDIR/IMOS/AATAMS/x.nc) — or under WAVE_BUOY_OUTDIR instead, for
#    a key that resolves to one of EXPLICIT_PREFIXES' wave buoy shards
#    (config.is_wave_buoy_key), since that data gets moved to its own
#    separate destination (see mv_to_prod/coordinator.sh). This is
#    stage 3 of the pipeline, consuming the temp manifests written by
#    compare_manifests.py (stage 2).
#
#    Deliberately simple and sequential — no parallelism. The point of this
#    run is just to see how it performs and whether parallelism actually
#    turns out to be necessary before adding that complexity.
#
#    Once all downloads are attempted, each temp manifest under TEMP_DIR is
#    appended into its persistent manifest under NCI_MANIFEST, de-duplicated,
#    and then deleted — regardless of whether every individual file
#    succeeded. A file that failed to download (e.g. NoSuchKey, because it
#    was retracted from the source bucket before ever being downloaded)
#    still gets merged into the persistent manifest as if present; this is
#    a deliberate, accepted simplification — retract_diff.py (stage 5) is
#    responsible for catching retracted/missing files on a later run, so
#    this script doesn't need to handle that case itself.
#
# CHECKSUMS
#    Requests ChecksumMode='ENABLED' on every GetObject and, when S3
#    reports ChecksumType == 'FULL_OBJECT' (a checksum that's a genuine
#    hash of the whole file, not a multipart composite), verifies the
#    download by computing the same algorithm locally while streaming and
#    comparing digests. Preference order: SHA256, SHA1, CRC64NVME, CRC32C,
#    CRC32 — first one S3 actually returned wins. The filepath,checksum
#    temp CSVs are only used for the file list; the checksum column (S3
#    Inventory ETag) is intentionally ignored, since it isn't a reliable
#    full-object hash for multipart uploads.
#
#    If no FULL_OBJECT checksum is available for a file (common for older
#    objects), falls back to a Content-Length size check and logs that no
#    checksum verification was possible.
#
#    Everything — per-file speed, per-file checksum result, and an overall
#    summary — goes to a single log file for this run (see common.py's
#    setup_logging / config.py's stage_log_path).
#
# REQUIRES
#    boto3, awscrt (for CRC32C/CRC64NVME verification when available;
#    falls back gracefully to SHA256/SHA1/CRC32 if awscrt isn't installed)
#
# VERSION
#    1.0: Initial implementation
#    1.1: Paths, logging, and S3 client moved to config.py / common.py;
#         merge_temp_into_persistent now de-duplicates each persistent
#         manifest after appending (previously a plain append with no
#         duplicate check).
#    1.2: Logging overhaul — one log file per run (see common.py's
#         setup_logging), with a handful of highlight() lines (log path,
#         output directory, hourly progress, merge failures, final
#         summary) surfaced to the top-level pipeline log; everything
#         else stays file-only.
#    1.3: Wave buoy data (EXPLICIT_PREFIXES) now downloads into
#         WAVE_BUOY_OUTDIR instead of OUTDIR, per an explicit request to
#         land it at its own separate destination.
#    1.4: Wave buoy temp manifests now live under WAVE_BUOY_TEMP_DIR
#         instead of TEMP_DIR, merging into WAVE_BUOY_MANIFEST instead of
#         NCI_MANIFEST — iter_temp_manifest_keys() and
#         merge_temp_into_persistent() both now iterate both
#         (temp_dir, manifest_dir) pairs instead of just one.
#    1.5: iter_temp_manifest_keys() now retries a temp manifest's read
#         (see common.retry) instead of aborting the whole run — same
#         "_csv.Error: line contains NUL" spurious-read failure over
#         /g/data that compare_manifests.py's load_existing_filepaths was
#         already hardened against; download.py's own read of these files
#         had no equivalent protection until now. Fixes a real production
#         crash.
#    1.6: Added apply_acls() — after each run, recursively sets the
#         sh74/rt82 group ACLs (default + existing) on OUTDIR and
#         WAVE_BUOY_OUTDIR via setfacl, per an explicit request. A failure
#         here is logged but not fatal — it shouldn't undo an otherwise
#         successful download run.
#
# ================================================================
# END_OF_HEADER
# ================================================================

import csv
import os
import subprocess
import time

from botocore.exceptions import ClientError
from botocore.httpchecksum import Crc32Checksum, Sha1Checksum, Sha256Checksum

try:
    from botocore.httpchecksum import CrtCrc32cChecksum, CrtCrc64NvmeChecksum
    have_crt_checksums = True
except ImportError:
    have_crt_checksums = False

from common import dedupe_manifest, get_s3_client, highlight, peak_memory_gb, retry, setup_logging
from config import (
    NCI_MANIFEST,
    OUTDIR,
    SOURCE_BUCKET,
    TEMP_DIR,
    WAVE_BUOY_MANIFEST,
    WAVE_BUOY_OUTDIR,
    WAVE_BUOY_TEMP_DIR,
    is_wave_buoy_key,
)

# (temp_dir, manifest_dir) pairs — one per manifest group. See
# config.manifest_dir_for_prefix()/temp_dir_for_prefix().
MANIFEST_GROUPS = ((TEMP_DIR, NCI_MANIFEST), (WAVE_BUOY_TEMP_DIR, WAVE_BUOY_MANIFEST))

chunk_size = 8 * 1024 * 1024  # 8 MB
progress_interval = 3600  # seconds — how often to log a running speed estimate

# ACL spec applied to every file/dir downloaded this run (see apply_acls()):
# sh74/rt82 groups get read+execute, rt82_w gets read+write+execute, and
# access is otherwise closed off to "other" — shared read access for both
# projects plus write access for the rt82 write group.
ACL_SPEC = "g::r-x,g:sh74:r-x,g:sh74_w:r-x,g:rt82:r-x,g:rt82_w:rwx,mask:rwx,other:---"

# Preference order: (response field name, checksum class)
checksum_candidates = [("ChecksumSHA256", Sha256Checksum), ("ChecksumSHA1", Sha1Checksum)]
if have_crt_checksums:
    checksum_candidates += [
        ("ChecksumCRC64NVME", CrtCrc64NvmeChecksum),
        ("ChecksumCRC32C", CrtCrc32cChecksum),
    ]
checksum_candidates.append(("ChecksumCRC32", Crc32Checksum))


def _read_temp_manifest_filepaths(path):
    """Read one temp manifest's filepath column fully into a list, retrying
    the whole read on failure (see common.retry) — this file lives on
    /g/data mounted over the network onto the datamover VM, where a brief
    hiccup can surface as a spurious mid-read error (e.g. _csv.Error: line
    contains NUL). Temp manifests only ever hold one run's newly-queued
    files, so materializing the (small) list is not a memory concern."""
    def _read():
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            return [row["filepath"] for row in reader]

    return retry(_read)


def iter_temp_manifest_keys(temp_dirs):
    for temp_dir in temp_dirs:
        for csv_name in sorted(os.listdir(temp_dir)):
            if not csv_name.endswith(".csv"):
                continue
            yield from _read_temp_manifest_filepaths(os.path.join(temp_dir, csv_name))


def download_one(s3, key, log):
    outdir = WAVE_BUOY_OUTDIR if is_wave_buoy_key(key) else OUTDIR
    local_path = os.path.join(outdir, key)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        response = s3.get_object(Bucket=SOURCE_BUCKET, Key=key, ChecksumMode="ENABLED")
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
    for temp_dir, manifest_dir in MANIFEST_GROUPS:
        for csv_name in sorted(os.listdir(temp_dir)):
            if not csv_name.endswith(".csv"):
                continue
            temp_csv = os.path.join(temp_dir, csv_name)
            persistent_csv = os.path.join(manifest_dir, csv_name)

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

                dedupe_manifest(persistent_csv, log)
            except Exception as e:
                highlight(
                    log,
                    f"{csv_name}: FAILED to merge into persistent manifest ({e}) — temp file left in place",
                    level="error",
                )


def apply_acls(folder, log):
    """Recursively set the ACL_SPEC group ACLs on everything downloaded into
    folder this run — both the default ACL (-d, inherited by files/dirs
    created here in future runs) and the ACL on what's already there."""
    for args in (["-R", "-d", "-m", ACL_SPEC, folder], ["-R", "-m", ACL_SPEC, folder]):
        try:
            subprocess.run(["setfacl", *args], check=True)
        except (subprocess.CalledProcessError, OSError) as e:
            highlight(log, f"setfacl {' '.join(args)}: FAILED ({e})", level="error")


def main():
    overall_start = time.perf_counter()
    log, run_id, log_path = setup_logging("download")
    highlight(log, f"Log file: {log_path}")
    log.info(f"TEMP_DIR = {TEMP_DIR}")
    log.info(f"WAVE_BUOY_TEMP_DIR = {WAVE_BUOY_TEMP_DIR}")
    highlight(log, f"OUTDIR: {OUTDIR}")
    highlight(log, f"WAVE_BUOY_OUTDIR: {WAVE_BUOY_OUTDIR}")

    os.makedirs(OUTDIR, exist_ok=True)
    os.makedirs(WAVE_BUOY_OUTDIR, exist_ok=True)

    s3 = get_s3_client()

    total_files = 0
    total_failed = 0
    total_bytes = 0
    last_report = overall_start
    for key in iter_temp_manifest_keys([TEMP_DIR, WAVE_BUOY_TEMP_DIR]):
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
            highlight(
                log,
                f"Progress: {total_files} file(s) ok, {total_failed} failed, {total_bytes / (1024 * 1024):.2f} MB, "
                f"{elapsed_so_far / 3600:.2f}h elapsed, {speed_so_far:.2f} MB/s average so far",
            )
            last_report = now

    log.info("Downloads complete — merging temp manifests into persistent manifest")
    merge_temp_into_persistent(log)

    log.info("Applying ACLs")
    apply_acls(OUTDIR, log)
    apply_acls(WAVE_BUOY_OUTDIR, log)

    overall_elapsed = time.perf_counter() - overall_start
    overall_speed = (total_bytes / (1024 * 1024)) / overall_elapsed if overall_elapsed > 0 else 0.0

    highlight(
        log,
        f"Done — {total_files} file(s) ok, {total_failed} failed, {total_bytes / (1024 * 1024):.2f} MB, "
        f"{overall_elapsed:.2f}s total, {overall_speed:.2f} MB/s average, "
        f"{peak_memory_gb():.2f} GB peak RAM",
    )


if __name__ == "__main__":
    main()
