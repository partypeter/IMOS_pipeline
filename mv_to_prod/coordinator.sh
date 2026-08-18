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
#
# ================================================================
# END_OF_HEADER
# ================================================================

# ===============================================================
# USER CONFIGURATION
# ===============================================================

# Job name — also used as the log folder name
# Preferrably use Jira / Helpdesk ticket numbers
# Log files get a time stamp so multiple logs under same ticket 
# (job_name) are fine.
latest=$(find /g/data/sh74/admin/globus/test -maxdepth 1 -type d -name '2026*' -printf '%f\n' | sort | tail -n 1)
job_name="mv_to_prod/$latest"

# Source and target paths (no trailing slash)
source_path="/g/data/sh74/admin/globus/test/download_test_datamover"
target_path="/g/data/sh74/admin/staging"

# Log folder (created automatically before submission)
log_path="/g/data/sh74/admin/globus/test/logs/${job_name}"

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
