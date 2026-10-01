#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    apply_pruned_entries_from_log.py <dry-run-log-path> [--apply]
#
# DESCRIPTION
#    Companion to prune_missing_manifest_entries.py — reuses a completed
#    (or interrupted) dry run's own log file to drop the already-
#    identified missing entries from the persistent manifests directly,
#    without re-running the slow, stat()-bound existence scan a second
#    time.
#
#    Parses every "<prefix>: MISSING <path>" line out of the given log
#    file (prune_missing_manifest_entries.py's own log format), groups
#    the recovered keys by prefix, and for each prefix streams its
#    persistent manifest once, dropping any row whose key matches one of
#    that prefix's reported-missing paths. No filesystem existence
#    checks are performed here at all — this trusts the dry run's
#    findings as-is.
#
#    Deliberately reads the dry run's .log file rather than its
#    companion _missing.lst file: Python's logging handlers flush after
#    every record, so the .log file is reliably complete up to the exact
#    moment a run was interrupted (e.g. killed), while _missing.lst is a
#    plain buffered file write that can silently lose its last buffer's
#    worth of lines on an abrupt kill — safe to use even against a dry
#    run that was never allowed to finish cleanly.
#
#    For each prefix, warns if the number of rows actually dropped from
#    the current manifest doesn't match the number of missing keys
#    parsed from the log — since nothing here is independently
#    re-verified against disk, a mismatch is worth investigating before
#    trusting the result.
#
#    Defaults to a dry run: reports what would be dropped (per prefix,
#    and a final total) without changing anything. Pass --apply to
#    actually rewrite the manifests.
#
# VERSION
#    1.0: Initial implementation
#
# ================================================================
# END_OF_HEADER
# ================================================================

import argparse
import csv
import os
import re

from common import highlight, peak_memory_gb, retry, setup_logging
from config import AODN_BASE, WAVE_BUOY_BASE, is_wave_buoy_prefix, manifest_dir_for_prefix

MISSING_LINE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[INFO\] ([^:]+): MISSING (.+)$")


def parse_missing_log(log_path):
    """Parse every "<prefix>: MISSING <path>" line out of a
    prune_missing_manifest_entries.py log file. Returns
    {prefix: {key, ...}} — each path is converted back to the manifest
    key it came from (relative to whichever base applies to that
    prefix's shard — see config.is_wave_buoy_prefix), deduplicated via
    the set, which also absorbs any duplicate lines left behind by a
    prefix that got retried after a transient failure."""
    missing_by_prefix = {}
    with open(log_path) as f:
        for line in f:
            m = MISSING_LINE_RE.match(line.rstrip("\n"))
            if not m:
                continue
            prefix, path = m.group(1), m.group(2)
            base = WAVE_BUOY_BASE if is_wave_buoy_prefix(prefix) else AODN_BASE
            key = os.path.relpath(path, base)
            missing_by_prefix.setdefault(prefix, set()).add(key)
    return missing_by_prefix


def drop_keys_from_prefix(prefix, missing_keys, apply_changes, log):
    """Stream one persistent manifest, dropping any row whose key is in
    missing_keys. Same atomic-replace pattern as
    prune_missing_manifest_entries.py's prune_missing_prefix. Returns
    (kept, dropped)."""
    manifest_dir = manifest_dir_for_prefix(prefix)
    persistent_csv = os.path.join(manifest_dir, f"{prefix}.csv")
    tmp_csv = persistent_csv + ".tmp"

    kept = 0
    dropped = 0

    f_out = open(tmp_csv, "w", newline="") if apply_changes else None
    writer = csv.writer(f_out) if f_out else None
    try:
        with open(persistent_csv, newline="") as f_in:
            reader = csv.reader(f_in)
            header = next(reader, None)
            if writer is not None and header is not None:
                writer.writerow(header)

            for row in reader:
                if row[0] in missing_keys:
                    dropped += 1
                    log.info(f"{prefix}: DROPPED {row[0]}")
                else:
                    kept += 1
                    if writer is not None:
                        writer.writerow(row)
    finally:
        if f_out is not None:
            f_out.close()

    if apply_changes:
        if dropped:
            os.replace(tmp_csv, persistent_csv)
        else:
            os.remove(tmp_csv)

    return kept, dropped


def main():
    parser = argparse.ArgumentParser(
        description="Drop manifest entries already identified as missing by a "
        "prune_missing_manifest_entries.py dry run, without re-scanning the filesystem."
    )
    parser.add_argument("log_path", help="Path to the dry run's .log file (not its _missing.lst file).")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually rewrite manifests. Without this, only reports what would be dropped.",
    )
    args = parser.parse_args()

    log, run_id, log_path = setup_logging("apply_pruned_manifest_entries")
    highlight(log, f"Log file: {log_path}")
    highlight(log, f"Source dry-run log: {args.log_path}")
    highlight(log, "APPLY MODE — manifests will be rewritten" if args.apply else "DRY RUN — no manifests will be modified")

    missing_by_prefix = parse_missing_log(args.log_path)
    total_parsed = sum(len(keys) for keys in missing_by_prefix.values())
    highlight(log, f"Parsed {total_parsed} unique missing entrie(s) across {len(missing_by_prefix)} prefix(es)")

    total_kept = 0
    total_dropped = 0
    for prefix, missing_keys in sorted(missing_by_prefix.items()):
        kept, dropped = retry(drop_keys_from_prefix, prefix, missing_keys, args.apply, log)
        total_kept += kept
        total_dropped += dropped
        highlight(log, f"{prefix}: {dropped} entrie(s) dropped ({kept} kept)")
        if dropped != len(missing_keys):
            log.warning(
                f"{prefix}: expected to drop {len(missing_keys)} entrie(s) parsed from the log, "
                f"but only matched {dropped} row(s) in the current manifest — "
                f"investigate before trusting --apply for this prefix"
            )

    highlight(
        log,
        f"Done — {total_dropped} entrie(s) dropped across all manifests ({total_kept} kept), "
        f"peak RAM {peak_memory_gb():.2f} GB"
        + ("" if args.apply else " — DRY RUN, nothing written. Re-run with --apply to commit."),
    )


if __name__ == "__main__":
    main()
