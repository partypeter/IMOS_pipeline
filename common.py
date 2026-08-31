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
#    1.2: setup_logging reworked for the pipeline-wide logging overhaul:
#         one log file per run per stage (config.stage_log_path), shared
#         across stages via PIPELINE_RUN_ID when run under
#         run_imos_pipeline.sh, so every log from one pipeline run is
#         findable by that one ID; stdout echo now only happens for a
#         standalone/manual invocation (no PIPELINE_RUN_ID set), so an
#         orchestrated run's routine detail no longer duplicates into the
#         top-level pipeline log. Added highlight(), for the handful of
#         lines (paths, structural events, one-line summaries) that
#         should surface at the pipeline level. Formatter now includes
#         the log level, so every log line is greppable by severity.
#    1.3: dedupe_manifest now has a disk-based path (GNU sort -u) for
#         manifests above LARGE_PREFIX_ROW_THRESHOLD rows, same pattern
#         compare_manifests.py already uses for its diff. Fixes a real
#         OOM: download.py calls dedupe_manifest after every merge,
#         unconditionally, including on AUV.csv (~82M rows) — fully
#         materializing that file (list of rows + a seen set + a deduped
#         list) needed far more than the datamover VM's 32GB. The
#         in-memory path is unchanged for every manifest below threshold.
#    1.4: Added a dedicated MOVE log level + log_move() helper, so
#         retract_diff.py's file-move lines are tagged [MOVE] instead of
#         generic [INFO] — unifies its look with mv_to_prod.sh's own
#         [MOVE]/[MOVED] bash log lines, per an explicit request.
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
import shutil
import subprocess
import sys
import time

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from config import DEDUPE_SCRATCH_DIR, LARGE_PREFIX_ROW_THRESHOLD, stage_log_path

DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z$")

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Dedicated level for file-move events, tagged [MOVE] like mv_to_prod.sh's
# own [MOVE]/[MOVED] log lines instead of blending into generic [INFO] —
# see log_move() below. Sits between INFO and WARNING so it's never
# filtered out by setup_logging's log.setLevel(logging.INFO).
MOVE = 25
logging.addLevelName(MOVE, "MOVE")


def setup_logging(stage_name, run_id=None):
    """Set up the root logger for one pipeline stage, writing to that
    stage's own dedicated file for this run (config.stage_log_path).

    run_id defaults to PIPELINE_RUN_ID (set by run_imos_pipeline.sh so
    every stage in one pipeline run shares the same ID) or, for a
    standalone/manual invocation, a freshly generated timestamp — same
    behavior as before this file-per-stage convention existed.

    Detail (log.info/warning/error) goes to the file only. When running
    standalone (PIPELINE_RUN_ID unset) it's also echoed to stdout, same
    as before, for live-tailing during manual/interactive runs. Under the
    orchestrator (PIPELINE_RUN_ID set), stdout is left to highlight() —
    otherwise run_imos_pipeline.sh's stage capture would duplicate every
    line of detail into the top-level pipeline log.

    Returns (log, run_id, log_path)."""
    orchestrated = "PIPELINE_RUN_ID" in os.environ
    run_id = run_id or os.environ.get("PIPELINE_RUN_ID") or datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = stage_log_path(stage_name, run_id)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    log = logging.getLogger()
    log.setLevel(logging.INFO)

    for handler in log.handlers[:]:
        handler.close()
        log.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.FileHandler(log_path, mode="a")
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    if not orchestrated:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        log.addHandler(stream_handler)

    return log, run_id, log_path


def highlight(log, message, level="info"):
    """Log a headline-level message: goes into this stage's own detailed
    log file like any other log call (so that file alone stays a
    complete narrative), and is also printed to stdout so
    run_imos_pipeline.sh's stage capture can surface it in the top-level
    pipeline log. Use this only for the handful of lines that belong at
    pipeline level — this stage's own log path, a few key file/folder
    paths, rare structural events, and one-line run summaries. Everything
    else should go through log.info/warning/error directly, which stays
    file-only under the orchestrator (see setup_logging)."""
    getattr(log, level)(message)
    print(f"{datetime.datetime.now().strftime(TIMESTAMP_FORMAT)} [{level.upper()}] {message}", flush=True)


def log_move(log, message):
    """Log a file-move event at the dedicated MOVE level (see above), so
    it's tagged [MOVE] the same way mv_to_prod.sh tags its own move lines
    — greppable/visually consistent across the two move-related logs
    (mv_to_prod.sh's bash log and retract_diff.py's Python log) instead of
    blending into a generic [INFO] line."""
    log.log(MOVE, message)


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


def count_csv_rows(path):
    """Count data rows in a CSV file (excluding the header), streaming
    line-by-line so it never materializes the file's contents — used to
    decide whether a manifest is large enough to need the disk-based
    diff path instead of an in-memory one."""
    with open(path, newline="") as f:
        count = sum(1 for _ in f)
    return max(count - 1, 0)


def dedupe_manifest(persistent_csv, log):
    """Remove duplicate filepath rows from a persistent manifest CSV.
    Rewrites the file only if duplicates were found, atomically (write to
    a temp file, then os.replace() over the original) so an interruption
    partway through can't leave a corrupted manifest. Returns the number
    of rows removed.

    Manifests above LARGE_PREFIX_ROW_THRESHOLD rows (config.py) use an
    external sort -u instead of materializing every row in memory — the
    same disk-based approach compare_manifests.py already uses for its
    diff, and for the same reason: this pipeline tracks at least one
    prefix (AUV) with ~82M rows, and reading that fully into Python
    objects (a rows list, a seen set, a deduped list) needs far more than
    the datamover VM's 32GB. That path keeps the first occurrence of each
    filepath in the sorted (not original file) order; the small path
    below keeps the first occurrence in original file order. Nothing
    downstream depends on manifest row order or which duplicate survives,
    so this difference doesn't matter in practice."""
    row_count = count_csv_rows(persistent_csv)
    if row_count > LARGE_PREFIX_ROW_THRESHOLD:
        return _dedupe_manifest_large(persistent_csv, row_count, log)

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


def _dedupe_manifest_large(persistent_csv, row_count, log):
    """dedupe_manifest()'s disk-based path (see its docstring). Extracts
    the data rows as plain "filepath,checksum" lines (same convention
    compare_manifests.py's large-prefix scratch files already use —
    assumes no embedded commas, true of every filepath/checksum this
    pipeline has ever seen), sorts+dedups them with GNU sort -u -k1,1
    (never holding the file's contents in memory), then reassembles
    header + deduped rows atomically."""
    os.makedirs(DEDUPE_SCRATCH_DIR, exist_ok=True)
    base = os.path.basename(persistent_csv)
    data_only = os.path.join(DEDUPE_SCRATCH_DIR, f"{base}.data")
    deduped_path = os.path.join(DEDUPE_SCRATCH_DIR, f"{base}.deduped")

    with open(persistent_csv, newline="") as f_in:
        reader = csv.reader(f_in)
        header = next(reader, None)
        with open(data_only, "w") as f_out:
            for row in reader:
                f_out.write(",".join(row) + "\n")

    subprocess.run(
        ["sort", "-t", ",", "-k1,1", "-u", "-T", DEDUPE_SCRATCH_DIR, "-o", deduped_path, data_only],
        check=True,
        env={**os.environ, "LC_ALL": "C"},
    )
    os.remove(data_only)

    with open(deduped_path) as f:
        deduped_count = sum(1 for _ in f)
    removed = row_count - deduped_count

    if removed:
        tmp_csv = persistent_csv + ".tmp"
        with open(tmp_csv, "w") as f_out:
            if header is not None:
                f_out.write(",".join(header) + "\n")
            with open(deduped_path) as f_in:
                shutil.copyfileobj(f_in, f_out)
        os.replace(tmp_csv, persistent_csv)
        log.info(f"{base}: removed {removed} duplicate entrie(s) via disk-based dedup")

    os.remove(deduped_path)
    return removed
