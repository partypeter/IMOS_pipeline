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
#    1.2: Switched to the final production folder structure: MANIFEST_MAIN
#         now points at admin/ops/ (was admin/globus/test/); dated
#         inventory folders moved to their own INVENTORY_DIR subfolder
#         instead of living directly under MANIFEST_MAIN; OUTDIR is now a
#         standalone path (admin/incoming) instead of nested under
#         MANIFEST_MAIN; STAGING_BASE renamed AODN_BASE and repointed at
#         the final hosting location (IMOS_AODN), since it's no longer an
#         intermediate staging step. Added EXCLUDED_PREFIXES (currently
#         just eMII, per an explicit exclusion directive) — see
#         compare_manifests.py and retract_diff.py for how it's enforced.
#    1.3: Added EXPLICIT_PREFIXES and resolve_prefix() — the pipeline
#         previously only ever tracked things under IMOS/, auto-discovered
#         by listing that one folder. Wave buoy data lives under other
#         top-level bucket folders entirely (e.g. Bureau_of_Meteorology/),
#         each with one fixed, known root — no discovery needed, just an
#         explicit root-prefix -> shard-name mapping. resolve_prefix() is
#         the single place both IMOS/ and EXPLICIT_PREFIXES matching
#         happens, shared by compare_manifests.py and retract_diff.py so
#         the two can't drift apart on how a key maps to a shard.
#    1.4: Added DEDUPE_SCRATCH_DIR for common.py::dedupe_manifest()'s
#         disk-based path on manifests above LARGE_PREFIX_ROW_THRESHOLD —
#         fixes an OOM (download.py's post-merge dedup fully materializing
#         AUV.csv, ~82M rows, in memory).
#    1.5: Added WAVE_BUOY_OUTDIR/WAVE_BUOY_BASE and is_wave_buoy_key() —
#         wave buoy data (EXPLICIT_PREFIXES) now lands in its own
#         destination, separate from AODN_BASE, per an explicit request.
#         download.py and retract_diff.py both route on is_wave_buoy_key()
#         so the two agree on where a given key's file actually lives.
#    1.6: WAVE_BUOY_OUTDIR moved to a sibling of OUTDIR instead of nested
#         inside it — fixes a real bug where OUTDIR's own mv_to_prod
#         route swept WAVE_BUOY_OUTDIR into AODN_BASE wholesale.
#    1.7: Added WAVE_BUOY_MANIFEST — wave buoy manifests now live in
#         their own directory, separate from NCI_MANIFEST (which stays
#         IMOS-only, no migration needed for existing data), per an
#         explicit request. manifest_dir_for_prefix()/temp_dir_for_prefix()
#         are the single place compare_manifests.py/download.py/
#         retract_diff.py all decide which directory a given prefix's
#         files live under.
#    1.8: WAVE_BUOY_BASE moved to its finalised location
#         (/g/data/sh74/wave_buoy_data), replacing the interim
#         admin/staging/wave_buoy_data path.
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

# Non-IMOS top-level bucket folders the pipeline also tracks, each mapped
# to its manifest shard name. Unlike IMOS/<prefix>/, which is
# auto-discovered by listing the bucket (see compare_manifests.py's
# get_imos_prefixes), these roots are fixed and known in advance, so
# there's nothing to discover — every key under a given root always maps
# to that one shard. See config.resolve_prefix().
EXPLICIT_PREFIXES = {
    "Bureau_of_Meteorology/WAVE-BUOYS/REALTIME/": "Bureau_of_Meteorology",
    "CSIRO/WAVE-BUOYS/REALTIME/": "CSIRO",
    "Deakin_University/WAVE-BUOYS/REALTIME/": "Deakin_University",
    "Department_for_Environment_and_Water-South_Australia/WAVE-BUOYS/REALTIME/":
        "Department_for_Environment_and_Water-South_Australia",
    "Department_of_Environment_and_Science-Queensland/WAVE-BUOYS/REALTIME/":
        "Department_of_Environment_and_Science-Queensland",
    "Department_of_Planning_and_Environment-New_South_Wales/WAVE-BUOYS/REALTIME/":
        "Department_of_Planning_and_Environment-New_South_Wales",
    "Department_of_Planning_and_Environment-New_South_Wales/Manly_Hydraulics_Laboratory/WAVE-BUOYS/REALTIME/":
        "Department_of_Planning_and_Environment-New_South_Wales_Manly_Hydraulics_Laboratory",
    "Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/":
        "Department_of_Transport-Western_Australia",
    "Flinders_University/WAVE-BUOYS/REALTIME/": "Flinders_University",
    "Gippsland-Ports-Victoria/WAVE-BUOYS/REALTIME/": "Gippsland-Ports-Victoria",
    "Pilbara_Ports_Authority/WAVE-BUOYS/REALTIME/": "Pilbara_Ports_Authority",
    "UWA/WAVE-BUOYS/REALTIME/": "UWA",
}
# Note: IMOS/COASTAL-WAVE-BUOYS/WAVE-BUOYS/REALTIME/ is NOT listed here —
# it's under IMOS/, so it's already auto-discovered as prefix
# "COASTAL-WAVE-BUOYS" without needing an explicit entry.

# Top-level folders (shard names, not S3 paths) that must never be
# tracked or downloaded, regardless of what's present upstream.
# compare_manifests.py filters these out of prefix discovery so nothing
# new is ever queued for them; retract_diff.py actively purges any
# already-tracked entries for them on every run (see both files'
# docstrings). Applies equally to IMOS/ and EXPLICIT_PREFIXES shards.
EXCLUDED_PREFIXES = {"eMII"}


def resolve_prefix(key):
    """Map an S3 key to its manifest shard name, or None if the key isn't
    tracked by the pipeline at all.

    IMOS/-rooted keys are sharded dynamically by their first path segment
    (e.g. "IMOS/AATAMS/x.nc" -> "AATAMS") — see compare_manifests.py's
    get_imos_prefixes() for how those shard names get discovered.
    EXPLICIT_PREFIXES keys map to a fixed shard name instead; checked
    longest-root-first so a more specific nested root (e.g. the NSW/Manly
    Hydraulics Laboratory entry) always wins over a shorter parent root,
    should one ever be added later."""
    if key.startswith(IMOS_PREFIX):
        remainder = key[len(IMOS_PREFIX):]
        return remainder.split("/", 1)[0] or None
    for root, shard_name in sorted(EXPLICIT_PREFIXES.items(), key=lambda kv: -len(kv[0])):
        if key.startswith(root):
            return shard_name
    return None


def is_wave_buoy_key(key):
    """True if key resolves to one of EXPLICIT_PREFIXES' wave buoy
    shards. download.py uses this to pick which OUTDIR to download into;
    retract_diff.py uses it to pick which base to look for an
    already-downloaded file under — see WAVE_BUOY_OUTDIR/WAVE_BUOY_BASE
    below. Computed fresh each call (not cached) so it stays correct if
    EXPLICIT_PREFIXES is ever monkeypatched, e.g. in tests."""
    return resolve_prefix(key) in EXPLICIT_PREFIXES.values()


def is_wave_buoy_prefix(prefix):
    """True if prefix (a manifest shard name, e.g. 'CSIRO' or 'AATAMS' —
    not a full S3 key) is one of EXPLICIT_PREFIXES' wave buoy shards.
    Used by manifest_dir_for_prefix()/temp_dir_for_prefix() below to
    route a shard to the right manifest directory."""
    return prefix in EXPLICIT_PREFIXES.values()


def manifest_dir_for_prefix(prefix):
    """Which persistent-manifest directory a shard's CSV lives under:
    WAVE_BUOY_MANIFEST for a wave buoy shard, NCI_MANIFEST (unchanged,
    IMOS-only) for everything else. The single place
    compare_manifests.py/download.py/retract_diff.py all decide this, so
    they can't drift apart on where a given prefix's manifest lives."""
    return WAVE_BUOY_MANIFEST if is_wave_buoy_prefix(prefix) else NCI_MANIFEST


def temp_dir_for_prefix(prefix):
    """Which temp-manifest (pending-batch) directory a shard's CSV lives
    under — mirrors manifest_dir_for_prefix()."""
    return WAVE_BUOY_TEMP_DIR if is_wave_buoy_prefix(prefix) else TEMP_DIR

# Base directory tree for manifests, downloads, and logs
MANIFEST_MAIN = "/g/data/sh74/admin/ops/"

# Dated whole-bucket inventory folders (get_imos_manifest.py's output;
# compare_manifests.py/retract_diff.py's input) live under here, not
# directly under MANIFEST_MAIN.
INVENTORY_DIR = os.path.join(MANIFEST_MAIN, "IMOS_inventory")

NCI_MANIFEST = os.path.join(MANIFEST_MAIN, "nci_imos_manifest")
TEMP_DIR = os.path.join(NCI_MANIFEST, "temp")

# Wave buoy manifests live in their own directory, separate from
# NCI_MANIFEST (which stays IMOS-only — no migration needed for the
# manifests already there), per an explicit request. See
# manifest_dir_for_prefix()/temp_dir_for_prefix() above.
WAVE_BUOY_MANIFEST = os.path.join(MANIFEST_MAIN, "wave_buoy_manifest")
WAVE_BUOY_TEMP_DIR = os.path.join(WAVE_BUOY_MANIFEST, "temp")

# download.py's download destination / mv_to_prod's source_path — a
# standalone path, not nested under MANIFEST_MAIN.
OUTDIR = "/g/data/sh74/admin/incoming"

# Wave buoy data (EXPLICIT_PREFIXES) downloads here instead of OUTDIR, so
# mv_to_prod/coordinator.sh can drain it to WAVE_BUOY_BASE in its own
# separate pass — one mv_to_prod run only ever moves one source tree to
# one target tree, so everything landing in the same OUTDIR would all end
# up under the same destination (AODN_BASE). Must be a sibling of OUTDIR,
# NOT nested inside it — OUTDIR is itself the main route's source_path,
# so mv_to_prod's Pass 1 would otherwise see this as just another
# top-level folder under OUTDIR and sweep it into AODN_BASE wholesale
# (confirmed: this happened on a real run before this was fixed).
WAVE_BUOY_OUTDIR = "/g/data/sh74/admin/incoming_wave_buoy"

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

# Where mv_to_prod lands files — the final, permanent hosting location
# (mirrors mv_to_prod/coordinator.sh's target_path, which sources this
# same value via config.sh) — and where retract_diff.py looks for
# already-downloaded files to move when they're retracted upstream.
AODN_BASE = "/g/data/sh74/IMOS_AODN"

# Where mv_to_prod lands wave buoy data specifically (see WAVE_BUOY_OUTDIR
# above), kept separate from AODN_BASE per an explicit request. Final
# hosting location (was an interim staging path under admin/staging/
# before this).
WAVE_BUOY_BASE = "/g/data/sh74/wave_buoy_data"

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

# common.py's dedupe_manifest() scratch space for its disk-based path,
# used on persistent manifests above LARGE_PREFIX_ROW_THRESHOLD (e.g.
# AUV) — same reasoning as COMPARE_SCRATCH_DIR, must be on real disk, not
# /tmp.
DEDUPE_SCRATCH_DIR = os.path.join(NCI_MANIFEST, "temp_dedupe_manifest")
