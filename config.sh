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
#      OPS_GDATA        <-> config.py OPS_GDATA
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
#    1.4: WAVE_BUOY_OUTDIR moved to a sibling of OUTDIR instead of nested
#         inside it — OUTDIR is the main mv_to_prod route's source_path,
#         so nesting meant that route's Pass 1 saw WAVE_BUOY_OUTDIR as
#         just another folder under OUTDIR and swept it into AODN_BASE
#         wholesale (confirmed on a real run).
#    1.5: PYTHON now defaults to "python3" (resolved from PATH) instead
#         of a hardcoded venv path under one specific user's home
#         directory — that path isn't valid for a different (e.g. shared)
#         user running this pipeline with packages installed directly
#         rather than in a dedicated virtualenv. Set PYTHON to an
#         absolute interpreter path here if you do need a specific one.
#    1.6: WAVE_BUOY_BASE moved to its finalised location
#         (/g/data/sh74/wave_buoy_data), replacing the interim
#         admin/staging/wave_buoy_data path.
#    1.7: WAVE_BUOY_BASE nested under AODN_BASE
#         (AODN_BASE/wave_buoy_data) instead of being a sibling — per
#         an explicit request. Safe unlike the earlier WAVE_BUOY_OUTDIR
#         nesting bug: that was a source_path nested inside another
#         route's source_path (which mv_to_prod.sh's Pass 1 BFS walks);
#         WAVE_BUOY_BASE is only ever a target_path, and neither
#         mv_to_prod.sh pass ever scans a target_path's own contents,
#         so the main route (source=OUTDIR, target=AODN_BASE) never
#         sees this subfolder.
#    1.8: MANIFEST_MAIN (and with it INVENTORY_DIR) moved to the datamover
#         VM's local volume (/scratch/ops), mirroring config.py 1.11. Logs
#         and scripts stay on /g/data under OPS_GDATA.
#
# ================================================================
# END_OF_HEADER
# ================================================================

PYTHON="python3"
OPS_GDATA="/g/data/sh74/admin/ops/"
SCRIPT_DIR="${OPS_GDATA}scripts"

# Manifests and inventory live on the VM's local volume (see VERSION 1.8).
MANIFEST_MAIN="/scratch/ops/"
INVENTORY_DIR="${MANIFEST_MAIN}IMOS_inventory"
LOGS_BASE="${OPS_GDATA}logs"

# Where mv_to_prod lands files — the final, permanent hosting location
# (mv_to_prod/coordinator.sh's target_path) — and where download.py puts
# files it downloads (mv_to_prod's source_path).
AODN_BASE="/g/data/sh74/IMOS_AODN"
OUTDIR="/g/data/sh74/admin/incoming"

# Wave buoy data's own destination (see coordinator.sh's second
# mv_to_prod route). WAVE_BUOY_OUTDIR must still be a sibling of OUTDIR,
# not nested inside it (see VERSION 1.4 above) — that's a source_path.
# WAVE_BUOY_BASE is nested under AODN_BASE (see VERSION 1.7) — that's a
# target_path, so the source-nesting bug's precedent doesn't apply.
WAVE_BUOY_OUTDIR="/g/data/sh74/admin/incoming_wave_buoy"
WAVE_BUOY_BASE="${AODN_BASE}/wave_buoy_data"

# Canonical per-run log file path for one pipeline stage — mirrors
# config.py's stage_log_path(). Usage: stage_log_path <stage_name> <run_id>
stage_log_path() {
    echo "${LOGS_BASE}/$1/$2.log"
}
