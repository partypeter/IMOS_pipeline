#!/bin/bash
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    coordinator.sh
#
# DESCRIPTION
#    User-facing configuration and submission script for mv_files.sh.
#    Sets all PBS flags and job variables, then submits mv_files.sh
#    via qsub. The PBS job name and log folder name are always identical.
#    All output from mv_files.sh is written to a timestamped log file
#    inside the log folder; the PBS output log will be empty.
#
# USAGE
#    bash coordinator.sh
#
# VERSION
#    1.0: Initial implementation
#    1.1: source_path/target_path/log_path now derived from the repo's
#         central config.sh instead of being hardcoded here.
#    1.2: Logging overhaul — log_path (a directory) replaced by log_file
#         (the exact file to write, via config.sh's stage_log_path),
#         named by RUN_ID like every other stage so it's findable
#         alongside the rest of a pipeline run's logs. RUN_ID falls back
#         to a fresh timestamp when run standalone (no PIPELINE_RUN_ID
#         from run_imos_pipeline.sh), same pattern as the Python stages.
#         The inventory batch this run is moving (previously only in the
#         log folder name) and the log file path are now echoed near the
#         top instead.
#    1.3: Final production folder structure: dated inventory folders now
#         live under INVENTORY_DIR (not directly under MANIFEST_MAIN);
#         target_path now sources AODN_BASE (renamed from STAGING_BASE —
#         it's the final hosting location, not an intermediate staging
#         step).
#    1.4: Runs mv_to_prod.sh twice — once for the main route
#         (OUTDIR -> AODN_BASE, as before) and once for wave buoy data
#         (WAVE_BUOY_OUTDIR -> WAVE_BUOY_BASE), which now lands at its own
#         separate destination per an explicit request. One mv_to_prod
#         run only ever moves one source tree to one target tree, so this
#         couldn't be done as a single pass — each route gets its own log
#         file (stage names "mv_to_prod_main"/"mv_to_prod_wave_buoy") and
#         its own post-move empty-check, sharing the same overwrite/
#         dry_run/create_latest_link policy.
#    1.5: Dropped the speculative mkdir -p of source_path/target_path
#         before each route — it was creating an empty destination folder
#         (e.g. AODN_BASE/wave_buoy) even when there was nothing to move
#         there. source_path is already guaranteed to exist (download.py
#         creates OUTDIR/WAVE_BUOY_OUTDIR unconditionally); target_path is
#         expected to already exist (a one-time manual mkdir for
#         WAVE_BUOY_BASE's first-ever run) — mv_to_prod.sh fails loudly
#         via the existing PIPELINE_ABORT path if it doesn't, rather than
#         silently pre-creating it.
#
# ================================================================
# END_OF_HEADER
# ================================================================

# ===============================================================
# USER CONFIGURATION
# ===============================================================

# Central path config (repo root) — source_path/target_path/log_file below
# are derived from this instead of being hardcoded here.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/config.sh"

log() {
    local level="$1"
    shift
    echo "$(date '+%Y-%m-%d %H:%M:%S') [${level}] $*"
}

RUN_ID="${PIPELINE_RUN_ID:-$(date +%Y%m%d%H%M%S)}"

# Inventory batch this run is moving — informational only now (used to be
# baked into the log folder name; see log_file below).
latest=$(find "${INVENTORY_DIR}" -maxdepth 1 -type d -regextype posix-extended -regex '.*/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}Z' -printf '%f\n' | sort | tail -n 1)
log INFO "Inventory batch: ${latest}"

# Overwrite existing files in target path?  true|false
overwrite=true

# Dry run mode?  true|false
dry_run=false

# Create a latest symlink alongside vYYYYMMDD version folders?  true|false
create_latest_link=false

# SUBMISSION — do not edit below this line
# ===============================================================

# Runs one mv_to_prod.sh pass for one (source, target) route, then its
# post-move empty-check/cleanup — same logic as before, just parameterized
# so it can run once per route instead of being hardcoded to one pair.
run_mv_to_prod_route() {
    local route_name="$1"
    local source_path="$2"
    local target_path="$3"

    local log_file
    log_file="$(stage_log_path "$route_name" "${RUN_ID}")"
    log INFO "[${route_name}] Log file: ${log_file}"
    mkdir -p "$(dirname "$log_file")"

    export source_path target_path log_file overwrite dry_run create_latest_link
    bash mv_to_prod.sh
    local mv_status=$?

    if [ "$mv_status" -ne 0 ]; then
        log ERROR "[ALARM] PIPELINE_ABORT: [${route_name}] mv_to_prod.sh failed (exit code ${mv_status}) — aborting before cleanup."
        exit "$mv_status"
    fi

    # Remove all empty folders — but only if the source is genuinely empty
    # of files first. Any remaining file means something went wrong in the
    # move and deleting directories could lose data, so raise the alarm
    # and stop the pipeline instead.
    local remaining_files
    remaining_files=$(find "$source_path" \( -type f -o -type l \) | wc -l)

    if [ "$remaining_files" -eq 0 ]; then
        log INFO "[${route_name}] No files remaining under ${source_path} — removing empty subdirectories."
        if [ "$dry_run" = false ]; then
            find "$source_path" -mindepth 1 -type d -empty -delete
        fi
    else
        log ERROR "[ALARM] PIPELINE_ABORT: [${route_name}] ${remaining_files} file(s) still present under ${source_path} after move — refusing to delete directories."
        exit 1
    fi
}

run_mv_to_prod_route "mv_to_prod_main" "${OUTDIR}" "${AODN_BASE}"
run_mv_to_prod_route "mv_to_prod_wave_buoy" "${WAVE_BUOY_OUTDIR}" "${WAVE_BUOY_BASE}"
