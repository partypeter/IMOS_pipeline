# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    config.py
#
# DESCRIPTION
#    Single source of truth for the paths and bucket names used by every
#    Python stage of the IMOS pipeline (get_imos_manifest.py,
#    compare_manifests.py, download.py, retract_diff.py). Import from here
#    instead of hardcoding paths in individual scripts.
#
#    The bash side of the pipeline (run_imos_pipeline.sh and
#    mv_to_prod/coordinator.sh) has its own config.sh with the equivalent
#    values, since bash can't import this module directly. The two files
#    must be kept in sync by hand — see config.sh.
#
# VERSION
#    1.0: Initial implementation
#    1.1: LOG_DIR renamed to LOGS_BASE to match config.sh's existing name
#         and meaning exactly (they used to refer to two different paths
#         under the same name across the two files — the direct cause of
#         an earlier mv_to_prod log-path regression). Added
#         stage_log_path(), the canonical per-run log path formula shared
#         by every stage — mirrored in config.sh.
#
# ================================================================
# END_OF_HEADER
# ================================================================

import os

# S3 buckets / prefixes
SOURCE_BUCKET = "imos-data"
INVENTORY_BUCKET = "imos-data-inventory"
BASE_S3_PREFIX = "imos-data/Daily_Inventory_Whole_Bucket"
IMOS_PREFIX = "IMOS/"

# Base directory tree for manifests, downloads, and logs
MANIFEST_MAIN = "/g/data/sh74/admin/globus/test/"

NCI_MANIFEST = os.path.join(MANIFEST_MAIN, "nci_imos_manifest")
TEMP_DIR = os.path.join(NCI_MANIFEST, "temp")
OUTDIR = os.path.join(MANIFEST_MAIN, "download_test_datamover")
LOGS_BASE = os.path.join(MANIFEST_MAIN, "logs")


def stage_log_path(stage_name, run_id):
    """Canonical per-run log file path for one pipeline stage: one file
    per run, named by the run ID shared across every stage of a given
    pipeline invocation (see common.py::setup_logging) so every log
    belonging to one run is trivially found by that ID alone. Mirrored in
    config.sh's stage_log_path() for the bash-side stages (pipeline,
    mv_to_prod) — keep the two in sync by hand, same as the other
    values shared between config.py and config.sh."""
    return os.path.join(LOGS_BASE, stage_name, f"{run_id}.log")

# retract_diff.py's scratch space for per-prefix live-key files — keeps
# the live inventory off the heap (see retract_diff.py's module docstring
# for why). Must be on real disk (/g/data), not /tmp — /tmp is commonly
# tmpfs (RAM-backed) on HPC/VM nodes, which would defeat the purpose.
RETRACT_SCRATCH_DIR = os.path.join(NCI_MANIFEST, "temp_retract_diff")

# Where mv_to_prod lands files (mirrors mv_to_prod/coordinator.sh's
# target_path, which now sources this same value via config.sh) and where
# retract_diff.py moves files that have been retracted upstream.
STAGING_BASE = "/g/data/sh74/admin/staging"
OUTGOING_BASE = "/g/data/sh74/admin/outgoing"
OUTGOING_RETENTION_DAYS = 28

# retract_diff.py sanity check: if the latest inventory scan turns up fewer
# live keys than this fraction of what's currently tracked across all
# persistent manifests, that's treated as a broken/incomplete inventory
# scan rather than genuine mass retraction, and the whole run aborts
# without touching any files or manifests.
MIN_LIVE_INVENTORY_RATIO = 0.5

# compare_manifests.py: prefixes whose persistent manifest has more rows
# than this switch from the in-memory diff to an external sort+join diff
# (see compare_manifests.py's module docstring) — a prefix this large
# can't be held in memory as a Python set without risking OOM on the
# datamover VM.
LARGE_PREFIX_ROW_THRESHOLD = 5_000_000

# compare_manifests.py's scratch space for the sort+join diff used on
# large prefixes. Must be on real disk (/g/data), not /tmp — /tmp is
# commonly tmpfs (RAM-backed) or size-constrained on HPC/VM nodes, which
# would defeat the purpose (confirmed empirically: GNU sort ran out of
# space under /tmp on the datamover VM but succeeded once pointed here).
COMPARE_SCRATCH_DIR = os.path.join(NCI_MANIFEST, "temp_compare_manifests")
