#!/usr/bin/env bash
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    config.sh
#
# DESCRIPTION
#    Single source of truth for the paths used by the bash side of the
#    IMOS pipeline (run_imos_pipeline.sh, mv_to_prod/coordinator.sh).
#    Source this file instead of hardcoding paths.
#
#    The Python side (get_imos_manifest.py, compare_manifests.py,
#    download.py, retract_diff.py) has its own config.py with the
#    equivalent values, since bash can't import a Python module. The two
#    files must be kept in sync by hand:
#      MANIFEST_MAIN  <-> config.py MANIFEST_MAIN
#      LOGS_BASE      <-> config.py LOGS_BASE
#      OUTDIR         <-> config.py OUTDIR
#      STAGING_BASE   <-> config.py STAGING_BASE
#      stage_log_path <-> config.py stage_log_path()
#
# VERSION
#    1.0: Initial implementation
#    1.1: Dropped LOG_DIR (was only ever used for the pipeline's own log
#         file, and its name collided confusingly with config.py's
#         differently-scoped LOG_DIR — the direct cause of an earlier
#         mv_to_prod log-path regression). Added stage_log_path(), the
#         canonical per-run log path formula shared by every stage.
#
# ================================================================
# END_OF_HEADER
# ================================================================

PYTHON="/home/900/hxh900/myenv/bin/python3"
SCRIPT_DIR="/g/data/sh74/admin/globus/test/scripts"

MANIFEST_MAIN="/g/data/sh74/admin/globus/test/"
LOGS_BASE="${MANIFEST_MAIN}logs"

# Where mv_to_prod lands files (mv_to_prod/coordinator.sh's target_path)
# and where download.py puts files it downloads (mv_to_prod's source_path).
STAGING_BASE="/g/data/sh74/admin/staging"
OUTDIR="${MANIFEST_MAIN}download_test_datamover"

# Canonical per-run log file path for one pipeline stage — mirrors
# config.py's stage_log_path(). Usage: stage_log_path <stage_name> <run_id>
stage_log_path() {
    echo "${LOGS_BASE}/$1/$2.log"
}
