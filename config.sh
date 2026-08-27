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
#      MANIFEST_MAIN    <-> config.py MANIFEST_MAIN
#      INVENTORY_DIR    <-> config.py INVENTORY_DIR
#      LOGS_BASE        <-> config.py LOGS_BASE
#      OUTDIR           <-> config.py OUTDIR
#      AODN_BASE        <-> config.py AODN_BASE
#      WAVE_BUOY_OUTDIR <-> config.py WAVE_BUOY_OUTDIR
#      WAVE_BUOY_BASE   <-> config.py WAVE_BUOY_BASE
#      stage_log_path   <-> config.py stage_log_path()
#
# VERSION
#    1.0: Initial implementation
#    1.1: Dropped LOG_DIR (was only ever used for the pipeline's own log
#         file, and its name collided confusingly with config.py's
#         differently-scoped LOG_DIR — the direct cause of an earlier
#         mv_to_prod log-path regression). Added stage_log_path(), the
#         canonical per-run log path formula shared by every stage.
#    1.2: Switched to the final production folder structure: MANIFEST_MAIN
#         now points at admin/ops/ (was admin/globus/test/), SCRIPT_DIR
#         moved under it; added INVENTORY_DIR (dated inventory folders no
#         longer live directly under MANIFEST_MAIN); OUTDIR is now a
#         standalone path (admin/incoming); STAGING_BASE renamed
#         AODN_BASE and repointed at the final hosting location
#         (IMOS_AODN), since it's no longer an intermediate staging step.
#    1.3: Added WAVE_BUOY_OUTDIR/WAVE_BUOY_BASE — wave buoy data now
#         lands in its own destination, drained by its own mv_to_prod
#         pass (see coordinator.sh), separate from AODN_BASE.
#
# ================================================================
# END_OF_HEADER
# ================================================================

PYTHON="/home/900/hxh900/myenv/bin/python3"
SCRIPT_DIR="/g/data/sh74/admin/ops/scripts"

MANIFEST_MAIN="/g/data/sh74/admin/ops/"
INVENTORY_DIR="${MANIFEST_MAIN}IMOS_inventory"
LOGS_BASE="${MANIFEST_MAIN}logs"

# Where mv_to_prod lands files — the final, permanent hosting location
# (mv_to_prod/coordinator.sh's target_path) — and where download.py puts
# files it downloads (mv_to_prod's source_path).
AODN_BASE="/g/data/sh74/IMOS_AODN"
OUTDIR="/g/data/sh74/admin/incoming"

# Wave buoy data's own destination, kept separate from AODN_BASE/OUTDIR
# per an explicit request (see coordinator.sh's second mv_to_prod route).
# WAVE_BUOY_BASE is currently an interim staging location — will move to
# /g/data/sh74/wave_buoy_data once finalised.
WAVE_BUOY_OUTDIR="${OUTDIR}/wave_buoy"
WAVE_BUOY_BASE="/g/data/sh74/admin/staging/wave_buoy_data"

# Canonical per-run log file path for one pipeline stage — mirrors
# config.py's stage_log_path(). Usage: stage_log_path <stage_name> <run_id>
stage_log_path() {
    echo "${LOGS_BASE}/$1/$2.log"
}
