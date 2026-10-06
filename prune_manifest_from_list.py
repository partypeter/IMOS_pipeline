#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    prune_manifest_from_list.py <missing-list> [--apply]
#
# DESCRIPTION
#    One-time maintenance tool, not part of the daily pipeline. Drops
#    persistent-manifest rows for files listed as missing in a plain list
#    of absolute paths, one per line — normally prune_scan.pbs's
#    missing.lst. The next pipeline run then sees those files as new and
#    re-downloads them.
#
#    Entries can be files or directories: a directory entry covers every
#    manifest row underneath it.
#
#    Every entry is re-checked on disk first, and anything that exists now
#    is left alone, since the list may be hours or days old. That's one
#    check per list entry, not per manifest row. Only the manifests the
#    list touches are read, each in one sequential pass.
#
#    Defaults to a dry run. With --apply it refuses to start unless it
#    runs as the owner of every manifest it would rewrite: a rewritten
#    manifest is owned by whoever rewrote it, and the pipeline user must
#    still be able to append to it. Run it outside the nightly pipeline's
#    window.
#
# VERSION
#    1.0: Initial implementation (replaces apply_pruned_entries_from_log.py,
#         which only read prune_missing_manifest_entries.py's log format)
#
# ================================================================
# END_OF_HEADER
# ================================================================

import argparse
import csv
import os
import sys

from common import highlight, peak_memory_gb, retry, setup_logging
from config import manifest_dir_for_prefix, resolve_prefix
from prune_scan_helper import key_for


def exists(path):
    """True/False for present/absent. Any other OSError propagates (and is
    retried by the caller) instead of being read as "missing"."""
    try:
        os.lstat(path)
        return True
    except FileNotFoundError:
        return False


def load_candidates(list_path, log):
    """Returns ({prefix: {key, ...}}, exists_now_count, unresolved_count)."""
    by_prefix = {}
    exists_now = unresolved = 0
    with open(list_path) as f:
        for line in f:
            path = line.rstrip("\n").rstrip("/")
            if not path:
                continue
            key = key_for(path)
            prefix = resolve_prefix(key) if key else None
            if prefix is None or not os.path.exists(manifest_path(prefix)):
                unresolved += 1
                log.warning(f"UNRESOLVED (not under AODN_BASE/WAVE_BUOY_BASE, or no manifest for it): {path}")
                continue
            if retry(exists, path):
                exists_now += 1
                log.info(f"{prefix}: EXISTS NOW, keeping: {path}")
                continue
            by_prefix.setdefault(prefix, set()).add(key)
    return by_prefix, exists_now, unresolved


def covering_entry(key, missing):
    """The list entry covering key — key itself or one of its parent
    directories — or None."""
    if key in missing:
        return key
    i = key.rfind("/")
    while i > 0:
        if key[:i] in missing:
            return key[:i]
        i = key.rfind("/", 0, i)
    return None


def manifest_path(prefix):
    return os.path.join(manifest_dir_for_prefix(prefix), f"{prefix}.csv")


def drop_from_manifest(prefix, missing, apply_changes, log):
    """Stream one persistent manifest, dropping rows covered by `missing`.
    Same atomic tmp-then-replace pattern as retract_diff.py's
    process_manifest. Returns (kept, dropped, matched_entries)."""
    persistent_csv = manifest_path(prefix)
    tmp_csv = persistent_csv + ".tmp"

    kept = dropped = 0
    matched = set()
    f_out = open(tmp_csv, "w", newline="") if apply_changes else None
    writer = csv.writer(f_out) if f_out else None
    try:
        with open(persistent_csv, newline="") as f_in:
            reader = csv.reader(f_in)
            header = next(reader, None)
            if writer is not None and header is not None:
                writer.writerow(header)
            for row in reader:
                entry = covering_entry(row[0], missing)
                if entry is None:
                    kept += 1
                    if writer is not None:
                        writer.writerow(row)
                else:
                    dropped += 1
                    matched.add(entry)
                    log.info(f"{prefix}: DROPPED {row[0]}")
    finally:
        if f_out is not None:
            f_out.close()

    if apply_changes:
        if dropped:
            os.replace(tmp_csv, persistent_csv)
        else:
            os.remove(tmp_csv)

    return kept, dropped, matched


def main():
    parser = argparse.ArgumentParser(
        description="Drop manifest rows for files listed as missing in a plain path list."
    )
    parser.add_argument("list_path", help="Plain list of absolute paths, one per line (files or directories).")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually rewrite manifests. Without this, only reports what would be dropped.",
    )
    args = parser.parse_args()

    log, run_id, log_path = setup_logging("prune_manifest_from_list")
    highlight(log, f"Log file: {log_path}")
    highlight(log, f"Missing list: {args.list_path}")
    highlight(log, "APPLY MODE — manifests will be rewritten" if args.apply else "DRY RUN — no manifests will be modified")

    by_prefix, exists_now, unresolved = load_candidates(args.list_path, log)
    highlight(
        log,
        f"{sum(len(v) for v in by_prefix.values())} entrie(s) still missing across {len(by_prefix)} prefix(es); "
        f"{exists_now} exist again now (kept); {unresolved} unresolved",
    )

    if args.apply:
        foreign = [manifest_path(p) for p in by_prefix if os.stat(manifest_path(p)).st_uid != os.geteuid()]
        if foreign:
            highlight(
                log,
                f"Not running as the owner of: {', '.join(foreign)} — run as the pipeline user "
                f"so the rewritten manifests keep their owner. Nothing changed.",
                level="error",
            )
            sys.exit(1)

    total_dropped = 0
    for prefix, missing in sorted(by_prefix.items()):
        kept, dropped, matched = retry(drop_from_manifest, prefix, missing, args.apply, log)
        total_dropped += dropped
        for entry in sorted(missing - matched):
            log.info(f"{prefix}: NOT IN MANIFEST (already gone): {entry}")
        highlight(
            log,
            f"{prefix}: {dropped} row(s) dropped ({kept} kept); "
            f"{len(matched)} of {len(missing)} list entrie(s) matched, {len(missing - matched)} not in manifest",
        )

    highlight(
        log,
        f"Done — {total_dropped} row(s) dropped, peak RAM {peak_memory_gb():.2f} GB"
        + ("" if args.apply else " — DRY RUN, nothing written. Re-run with --apply to commit."),
    )


if __name__ == "__main__":
    main()
