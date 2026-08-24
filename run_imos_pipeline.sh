#!/usr/bin/env bash
#
# Runs the five-stage IMOS onboarding pipeline in order:
#   1. get_imos_manifest.py       - fetch latest S3 inventory manifest
#   2. compare_manifests.py       - diff inventory against persistent manifests, populate temp_dir
#   3. download.py                - download new files, merge temp manifests into persistent
#   4. mv_to_prod/coordinator.sh  - move downloaded files into staging, then verify
#                                   source is empty and clean up empty directories
#   5. retract_diff.py            - diff persistent manifests against the latest inventory the
#                                   other way around: move files retracted upstream out of
#                                   staging into the outgoing quarantine, and prune old
#                                   quarantine folders
#
# Each stage must exit 0 before the next one runs. If a stage fails, the
# pipeline stops immediately and the failure is logged - it will NOT
# continue on to later stages with a partial/inconsistent state.
#
# Stage 4 is itself a bash script and internally calls "bash mv_to_prod.sh"
# using a bare relative path, so it must be run from its own directory -
# run_stage cd's into each script's directory before invoking it, which is
# harmless for the other stages too.
#
# Exit code: 0 if all stages succeeded, non-zero (the failing stage's exit
# code) otherwise. A PIPELINE_ABORT-tagged line in the log marks a
# deliberate abort raised by a stage itself (currently only stage 4),
# as opposed to an ordinary uncaught error.
#
# LOGGING: every stage of one pipeline run shares a single PIPELINE_RUN_ID
# (exported below), used as the filename for that stage's own log under
# config.sh's stage_log_path convention (LOGS_BASE/<stage>/<run_id>.log,
# mirrored in config.py) — so every log produced by one run is findable
# by that one ID. Each stage writes its own detailed log to that file;
# only a curated handful of lines (paths, structural events, one-line
# summaries — see each script's highlight() calls) land in this top-level
# pipeline log, via run_stage's stdout+stderr capture below.

set -uo pipefail

SCRIPT_DIR_SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR_SELF}/config.sh"

GET_MANIFEST="${SCRIPT_DIR}/get_imos_manifest.py"
COMPARE_MANIFESTS="${SCRIPT_DIR}/compare_manifests.py"
DOWNLOAD="${SCRIPT_DIR}/download.py"
MV_TO_PROD_COORDINATOR="${SCRIPT_DIR}/mv_to_prod/coordinator.sh"
RETRACT_DIFF="${SCRIPT_DIR}/retract_diff.py"

PIPELINE_RUN_ID="$(date +%Y%m%d%H%M%S)"
export PIPELINE_RUN_ID

PIPELINE_LOG="$(stage_log_path pipeline "${PIPELINE_RUN_ID}")"
mkdir -p "$(dirname "${PIPELINE_LOG}")"

log() {
    local level="$1"
    shift
    echo "$(date '+%Y-%m-%d %H:%M:%S') [${level}] $*" | tee -a "${PIPELINE_LOG}"
}

run_stage() {
    local name="$1"
    local interpreter="$2"
    local script="$3"

    log INFO "Starting stage: ${name}"
    (
        cd "$(dirname "${script}")" && "${interpreter}" "$(basename "${script}")"
    ) >> "${PIPELINE_LOG}" 2>&1
    local status=$?

    if [ ${status} -ne 0 ]; then
        log ERROR "Stage FAILED: ${name} (exit code ${status}) - aborting pipeline"
        exit ${status}
    fi

    log INFO "Stage completed: ${name}"
}

log INFO "=== Pipeline run started (run_id=${PIPELINE_RUN_ID}) ==="

run_stage "get_imos_manifest"    "${PYTHON}"   "${GET_MANIFEST}"
run_stage "compare_manifests"    "${PYTHON}"   "${COMPARE_MANIFESTS}"
run_stage "download"             "${PYTHON}"   "${DOWNLOAD}"
run_stage "mv_to_prod"           "bash"        "${MV_TO_PROD_COORDINATOR}"
run_stage "retract_diff"         "${PYTHON}"   "${RETRACT_DIFF}"

log INFO "=== Pipeline run completed successfully (run_id=${PIPELINE_RUN_ID}) ==="
exit 0
