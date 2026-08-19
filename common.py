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
#    reporting, finding the latest dated inventory directory, and
#    de-duplicating a persistent manifest CSV.
#
# VERSION
#    1.0: Initial implementation
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


def dedupe_manifest(persistent_csv, log):
    """Remove duplicate filepath rows from a persistent manifest CSV,
    keeping the first occurrence of each filepath. Rewrites the file only
    if duplicates were found. Returns the number of rows removed."""
    with open(persistent_csv, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        rows = list(reader)

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
        with open(persistent_csv, "w", newline="") as f:
            writer = csv.writer(f)
            if header is not None:
                writer.writerow(header)
            writer.writerows(deduped)
        log.info(f"{os.path.basename(persistent_csv)}: removed {removed} duplicate entrie(s)")

    return removed
