#!/usr/bin/env bash
#
# Runs the three-stage IMOS onboarding pipeline in order:
#   1. get_manifest.py     - fetch latest S3 inventory manifest
#   2. shard_diff.py       - diff inventory against persistent manifests, populate temp_dir
#   3. download_new_files.py - download new files, merge temp manifests into persistent
#
# Each stage must exit 0 before the next one runs. If a stage fails, the
# pipeline stops immediately and the failure is logged - it will NOT
# continue on to later stages with a partial/inconsistent state.
#
# Exit code: 0 if all stages succeeded, non-zero (the failing stage's exit
# code) otherwise.

set -uo pipefail

PYTHON="/home/900/hxh900/myenv/bin/python3"

SCRIPT_DIR="/g/data/sh74/admin/globus/test/scripts"
LOG_DIR="/g/data/sh74/admin/globus/test/logs/pipeline"

GET_MANIFEST="${SCRIPT_DIR}/get_manifest.py"
SHARD_DIFF="${SCRIPT_DIR}/shard_diff.py"
DOWNLOAD_NEW="${SCRIPT_DIR}/download_new_files.py"

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date +%Y%m%d%H%M%S)"
PIPELINE_LOG="${LOG_DIR}/${TIMESTAMP}.txt"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "${PIPELINE_LOG}"
}

run_stage() {
    local name="$1"
    local script="$2"

    log "Starting stage: ${name}"
    "${PYTHON}" "${script}" >> "${PIPELINE_LOG}" 2>&1
    local status=$?

    if [ ${status} -ne 0 ]; then
        log "Stage FAILED: ${name} (exit code ${status}) - aborting pipeline"
        exit ${status}
    fi

    log "Stage completed: ${name}"
}

log "=== Pipeline run started ==="

run_stage "get_manifest"       "${GET_MANIFEST}"
run_stage "shard_diff"         "${SHARD_DIFF}"
run_stage "download_new_files" "${DOWNLOAD_NEW}"

log "=== Pipeline run completed successfully ==="
exit 0
