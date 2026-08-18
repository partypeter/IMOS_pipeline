#!/usr/bin/env bash
#
# Runs the four-stage IMOS onboarding pipeline in order:
#   1. get_imos_manifest.py       - fetch latest S3 inventory manifest
#   2. shard_diff.py         - diff inventory against persistent manifests, populate temp_dir
#   3. download_new_files.py - download new files, merge temp manifests into persistent
#   4. mv_to_prod/coordinator.py - move downloaded files into staging, then verify
#                                  source is empty and clean up empty directories
#
# Each stage must exit 0 before the next one runs. If a stage fails, the
# pipeline stops immediately and the failure is logged - it will NOT
# continue on to later stages with a partial/inconsistent state.
#
# Stage 4 is itself a bash script (despite the .py name) and internally
# calls "bash mv_to_prod.sh" using a bare relative path, so it must be run
# from its own directory - run_stage cd's into each script's directory
# before invoking it, which is harmless for the other stages too.
#
# Exit code: 0 if all stages succeeded, non-zero (the failing stage's exit
# code) otherwise. A PIPELINE_ABORT-tagged line in the log marks a
# deliberate abort raised by a stage itself (currently only stage 4),
# as opposed to an ordinary uncaught error.

set -uo pipefail

PYTHON="/home/900/hxh900/myenv/bin/python3"
BASH_BIN="bash"

SCRIPT_DIR="/g/data/sh74/admin/globus/test/scripts"
LOG_DIR="/g/data/sh74/admin/globus/test/logs/pipeline"

GET_MANIFEST="${SCRIPT_DIR}/get_imos_manifest.py"
SHARD_DIFF="${SCRIPT_DIR}/shard_diff.py"
DOWNLOAD_NEW="${SCRIPT_DIR}/download_new_files.py"
MV_TO_PROD_COORDINATOR="${SCRIPT_DIR}/mv_to_prod/coordinator.py"

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date +%Y%m%d%H%M%S)"
PIPELINE_LOG="${LOG_DIR}/${TIMESTAMP}.txt"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "${PIPELINE_LOG}"
}

run_stage() {
    local name="$1"
    local interpreter="$2"
    local script="$3"

    log "Starting stage: ${name}"
    (
        cd "$(dirname "${script}")" && "${interpreter}" "$(basename "${script}")"
    ) >> "${PIPELINE_LOG}" 2>&1
    local status=$?

    if [ ${status} -ne 0 ]; then
        log "Stage FAILED: ${name} (exit code ${status}) - aborting pipeline"
        exit ${status}
    fi

    log "Stage completed: ${name}"
}

log "=== Pipeline run started ==="

run_stage "get_imos_manifest"       "${PYTHON}"   "${GET_MANIFEST}"
run_stage "shard_diff"         "${PYTHON}"   "${SHARD_DIFF}"
run_stage "download_new_files" "${PYTHON}"   "${DOWNLOAD_NEW}"
run_stage "mv_to_prod"         "${BASH_BIN}" "${MV_TO_PROD_COORDINATOR}"

log "=== Pipeline run completed successfully ==="
exit 0
