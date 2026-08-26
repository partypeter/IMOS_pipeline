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

# Source and target paths (no trailing slash)
source_path="${OUTDIR}"
target_path="${AODN_BASE}"

# Exact log file to write (created automatically before submission)
log_file="$(stage_log_path mv_to_prod "${RUN_ID}")"
log INFO "Log file: ${log_file}"

# Overwrite existing files in target path?  true|false
overwrite=true

# Dry run mode?  true|false
dry_run=false

# Create a latest symlink alongside vYYYYMMDD version folders?  true|false
create_latest_link=false

# SUBMISSION — do not edit below this line
# ===============================================================

# Create log folder before submission so PBS can write its output there
mkdir -p "$(dirname "$log_file")"

# Export all variables so qsub -V passes them to mv_files.sh
export source_path target_path log_file overwrite dry_run create_latest_link

bash mv_to_prod.sh

# Check if source_path is empty; remove empty folders
mv_status=$?

if [ "$mv_status" -ne 0 ]; then
  log ERROR "[ALARM] PIPELINE_ABORT: mv_to_prod.sh failed (exit code ${mv_status}) — aborting before cleanup."
  exit "$mv_status"
fi

# Remove all empty folders — but only if the source is genuinely empty of
# files first. Any remaining file means something went wrong in the move
# and deleting directories could lose data, so raise the alarm and stop
# the pipeline instead.
remaining_files=$(find "$source_path" \( -type f -o -type l \) | wc -l)

if [ "$remaining_files" -eq 0 ]; then
  log INFO "No files remaining under ${source_path} — removing empty subdirectories."
  if [ "$dry_run" = false ]; then
    find "$source_path" -mindepth 1 -type d -empty -delete
  fi
else
  log ERROR "[ALARM] PIPELINE_ABORT: ${remaining_files} file(s) still present under ${source_path} after move — refusing to delete directories."
  exit 1
fi
