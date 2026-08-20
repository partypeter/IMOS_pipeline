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
#      LOGS_BASE      <-> config.py LOG_DIR
#      OUTDIR         <-> config.py OUTDIR
#      STAGING_BASE   <-> config.py STAGING_BASE
#
# VERSION
#    1.0: Initial implementation
#
# ================================================================
# END_OF_HEADER
# ================================================================

PYTHON="/home/900/hxh900/myenv/bin/python3"
SCRIPT_DIR="/g/data/sh74/admin/globus/test/scripts"

MANIFEST_MAIN="/g/data/sh74/admin/globus/test/"
LOGS_BASE="${MANIFEST_MAIN}logs"
LOG_DIR="${LOGS_BASE}/pipeline"

# Where mv_to_prod lands files (mv_to_prod/coordinator.sh's target_path)
# and where download.py puts files it downloads (mv_to_prod's source_path).
STAGING_BASE="/g/data/sh74/admin/staging"
OUTDIR="${MANIFEST_MAIN}download_test_datamover"
