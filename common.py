# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    common.py
#
# DESCRIPTION
#    Shared helpers used by every Python stage of the IMOS pipeline
#    (get_imos_manifest.py, compare_manifests.py, download.py,
#    retract_diff.py): logging setup, an unsigned S3 client, peak-RAM
#    reporting, finding the latest dated inventory directory, a
#    retry-on-transient-error CSV reader, and de-duplicating a persistent
#    manifest CSV.
#
# VERSION
#    1.0: Initial implementation
#    1.1: Added read_csv_rows (retries a transient read failure instead of
#         aborting outright — persistent manifests live on /g/data mounted
#         over the network onto the datamover VM, where a brief hiccup can
#         surface as a spurious mid-read error). dedupe_manifest's rewrite
#         is now atomic (write to a temp file, then os.replace()) instead
#         of truncating the original file in place, so an interruption
#         partway through can no longer leave a corrupted manifest.
#
# ================================================================
# END_OF_HEADER
# ================================================================

import csv
import datetime
import logging
import os
import re
import resource
import sys
import time

import boto3
from botocore import UNSIGNED
from botocore.client import Config

DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z$")


def setup_logging(log_dir, subdir=None):
    """Set up a logger that writes to a timestamped file under log_dir
    (optionally nested under subdir) and echoes to stdout."""
    target_dir = os.path.join(log_dir, subdir) if subdir else log_dir
    os.makedirs(target_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = os.path.join(target_dir, f"{timestamp}.txt")

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


def get_s3_client():
    """Anonymous (unsigned) S3 client — the IMOS inventory and data
    buckets are public and don't require credentials."""
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def peak_memory_gb():
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak_kb / (1024 ** 2)


def find_latest_date_dir(base_dir):
    """Most recent dated inventory directory (name like
    '2026-08-18T01-00Z') directly under base_dir, or None if there isn't
    one. Date strings sort correctly as plain strings."""
    candidates = [
        d for d in os.listdir(base_dir)
        if DATE_DIR_RE.match(d) and os.path.isdir(os.path.join(base_dir, d))
    ]
    if not candidates:
        return None
    return sorted(candidates)[-1]


def retry(func, *args, retries=3, delay_seconds=2, **kwargs):
    """Call func(*args, **kwargs), retrying on any exception up to
    `retries` times with a `delay_seconds` pause between attempts, before
    giving up and letting the last exception propagate. For operations
    that depend on a network-mounted filesystem, where a transient hiccup
    shouldn't abort the whole pipeline run."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(delay_seconds)
    raise last_exc


def read_csv_rows(path, retries=3, delay_seconds=2):
    """Read a CSV file fully into (header, rows), retrying the whole read
    if it fails partway through (e.g. a transient hiccup on a
    network-mounted filesystem)."""
    def _read():
        with open(path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            rows = list(reader)
        return header, rows

    return retry(_read, retries=retries, delay_seconds=delay_seconds)


def dedupe_manifest(persistent_csv, log):
    """Remove duplicate filepath rows from a persistent manifest CSV,
    keeping the first occurrence of each filepath. Rewrites the file only
    if duplicates were found, atomically (write to a temp file, then
    os.replace() over the original) so an interruption partway through
    can't leave a corrupted manifest. Returns the number of rows
    removed."""
    header, rows = read_csv_rows(persistent_csv)

    seen = set()
    deduped = []
    for row in rows:
        filepath = row[0]
        if filepath in seen:
            continue
        seen.add(filepath)
        deduped.append(row)

    removed = len(rows) - len(deduped)
    if removed:
        tmp_csv = persistent_csv + ".tmp"
        with open(tmp_csv, "w", newline="") as f:
            writer = csv.writer(f)
            if header is not None:
                writer.writerow(header)
            writer.writerows(deduped)
        os.replace(tmp_csv, persistent_csv)
        log.info(f"{os.path.basename(persistent_csv)}: removed {removed} duplicate entrie(s)")

    return removed
