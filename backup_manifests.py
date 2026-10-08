#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    backup_manifests.py
#
# DESCRIPTION
#    Stage 6 of the pipeline: copies every persistent manifest from the
#    datamover VM's local volume (NCI_MANIFEST, WAVE_BUOY_MANIFEST) to
#    MANIFEST_BACKUP on /g/data, as <MANIFEST_BACKUP>/<dir name>/<prefix>.csv.
#    The backup is what Gadi-side tools (IMOS-auxiliary repo) read, and what to
#    restore from if the VM volume is lost.
#
#    Each copy is read back from storage before it replaces the previous
#    backup (common.replace_if_intact), since large writes over the VM's
#    NFS mount have lost data before. A manifest whose size and mtime
#    match its backup is skipped. Backups of manifests that no longer
#    exist locally are removed. Any failed copy leaves that backup as it
#    was and makes the stage exit non-zero; the manifests themselves are
#    never touched.
#
# VERSION
#    1.0: Initial implementation
#
# ================================================================
# END_OF_HEADER
# ================================================================

import os
import shutil
import sys

from common import highlight, replace_if_intact, setup_logging
from config import MANIFEST_BACKUP, NCI_MANIFEST, WAVE_BUOY_MANIFEST

CHUNK = 16 * 1024 * 1024


def backup_one(src, dst):
    """Copy src to dst via a temp file, checked by replace_if_intact.
    Returns False if dst was already up to date."""
    s = os.stat(src)
    try:
        d = os.stat(dst)
        if d.st_size == s.st_size and d.st_mtime_ns == s.st_mtime_ns:
            return False
    except FileNotFoundError:
        pass
    tmp = dst + ".tmp"
    lines = 0
    with open(src, "rb") as fin, open(tmp, "wb") as fout:
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            lines += chunk.count(b"\n")
            fout.write(chunk)
    shutil.copystat(src, tmp)
    replace_if_intact(tmp, dst, lines)
    return True


def main():
    log, run_id, log_path = setup_logging("backup_manifests")
    highlight(log, f"Log file: {log_path}")
    copied = skipped = removed = 0
    failed = []
    for manifest_dir in (NCI_MANIFEST, WAVE_BUOY_MANIFEST):
        backup_dir = os.path.join(MANIFEST_BACKUP, os.path.basename(os.path.normpath(manifest_dir)))
        os.makedirs(backup_dir, exist_ok=True)
        names = sorted(n for n in os.listdir(manifest_dir) if n.endswith(".csv"))
        for name in names:
            src = os.path.join(manifest_dir, name)
            dst = os.path.join(backup_dir, name)
            try:
                if backup_one(src, dst):
                    copied += 1
                    log.info(f"backed up {src} -> {dst}")
                else:
                    skipped += 1
            except Exception as e:
                failed.append(name)
                highlight(log, f"{src}: backup FAILED ({e}) — previous backup left in place", level="error")
        for name in sorted(set(os.listdir(backup_dir)) - set(names)):
            if name.endswith(".csv"):
                os.remove(os.path.join(backup_dir, name))
                removed += 1
                log.info(f"removed backup of a manifest that no longer exists: {name}")
    highlight(log, f"Done — {copied} copied, {skipped} unchanged, {removed} removed, {len(failed)} failed, into {MANIFEST_BACKUP}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
