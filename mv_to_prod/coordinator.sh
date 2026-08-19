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
#
# ================================================================
# END_OF_HEADER
# ================================================================

# ===============================================================
# USER CONFIGURATION
# ===============================================================

# Central path config (repo root) — source_path/target_path/log_path below
# are derived from this instead of being hardcoded here.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/config.sh"

# Job name — also used as the log folder name
# Preferrably use Jira / Helpdesk ticket numbers
# Log files get a time stamp so multiple logs under same ticket
# (job_name) are fine.
latest=$(find "${MANIFEST_MAIN}" -maxdepth 1 -type d -regextype posix-extended -regex '.*/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}Z' -printf '%f\n' | sort | tail -n 1)
job_name="mv_to_prod/$latest"

# Source and target paths (no trailing slash)
source_path="${OUTDIR}"
target_path="${STAGING_BASE}"

# Log folder (created automatically before submission)
log_path="${LOG_DIR}/${job_name}"

# Overwrite existing files in target path?  true|false
overwrite=true

# Dry run mode?  true|false
dry_run=false

# Create a latest symlink alongside vYYYYMMDD version folders?  true|false
create_latest_link=false

# SUBMISSION — do not edit below this line
# ===============================================================

# Create log folder before submission so PBS can write its output there
mkdir -p "$log_path"

# Export all variables so qsub -V passes them to mv_files.sh
export source_path target_path log_path overwrite dry_run create_latest_link

bash mv_to_prod.sh

# Check if source_path is empty; remove empty folders
mv_status=$?

if [ "$mv_status" -ne 0 ]; then
  echo "[ALARM] PIPELINE_ABORT: mv_to_prod.sh failed (exit code ${mv_status}) — aborting before cleanup."
  exit "$mv_status"
fi

# Remove all empty folders — but only if the source is genuinely empty of
# files first. Any remaining file means something went wrong in the move
# and deleting directories could lose data, so raise the alarm and stop
# the pipeline instead.
remaining_files=$(find "$source_path" \( -type f -o -type l \) | wc -l)

if [ "$remaining_files" -eq 0 ]; then
  echo "No files remaining under ${source_path} — removing empty subdirectories."
  if [ "$dry_run" = false ]; then
    find "$source_path" -mindepth 1 -type d -empty -delete
  fi
else
  echo "[ALARM] PIPELINE_ABORT: ${remaining_files} file(s) still present under ${source_path} after move — refusing to delete directories."
  exit 1
fi
