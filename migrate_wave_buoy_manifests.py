#!/usr/bin/env python3
"""One-off migration: move existing wave buoy persistent/temp manifest
CSVs out of NCI_MANIFEST/TEMP_DIR into WAVE_BUOY_MANIFEST/WAVE_BUOY_TEMP_DIR.
Run once on the datamover VM after pulling the manifest-folder-separation
change, before the next pipeline run. Safe to re-run (skips files already
moved). Delete this file once you've run it.
"""
import os
import shutil

from config import (
    NCI_MANIFEST,
    TEMP_DIR,
    WAVE_BUOY_MANIFEST,
    WAVE_BUOY_TEMP_DIR,
    is_wave_buoy_prefix,
)


def migrate(src_dir, dst_dir):
    if not os.path.isdir(src_dir):
        print(f"skip (missing): {src_dir}")
        return
    os.makedirs(dst_dir, exist_ok=True)
    for csv_name in sorted(os.listdir(src_dir)):
        if not csv_name.endswith(".csv"):
            continue
        prefix = csv_name[: -len(".csv")]
        if not is_wave_buoy_prefix(prefix):
            continue
        src = os.path.join(src_dir, csv_name)
        dst = os.path.join(dst_dir, csv_name)
        if os.path.exists(dst):
            print(f"skip (already at destination): {csv_name}")
            continue
        shutil.move(src, dst)
        print(f"moved: {src} -> {dst}")


if __name__ == "__main__":
    migrate(NCI_MANIFEST, WAVE_BUOY_MANIFEST)
    migrate(TEMP_DIR, WAVE_BUOY_TEMP_DIR)
