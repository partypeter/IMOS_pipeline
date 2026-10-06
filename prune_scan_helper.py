#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    prune_scan_helper.py keys <paths_out> <roots_out> <shallow_missing_out>
#    prune_scan_helper.py summary <missing_list>
#
# DESCRIPTION
#    Python half of prune_scan.pbs (one-time maintenance, not part of the
#    daily pipeline). Imports config.py only — no boto3/common.py — so it
#    runs on a Gadi compute node's system python3 (3.6).
#
#    keys:    writes every persistent-manifest entry as an absolute local
#             path (base + key; one per line, unsorted), plus the set of
#             directories two levels below each base that those entries
#             live under — the roots prune_scan.pbs lists on disk. The odd
#             entry with fewer than three path components can't be covered
#             by such a root, so it's checked directly here instead and,
#             if absent, written to shallow_missing_out.
#    summary: per-prefix counts of a missing-paths list.
#
# VERSION
#    1.0: Initial implementation
#
# ================================================================
# END_OF_HEADER
# ================================================================

import collections
import csv
import os
import sys

from config import AODN_BASE, NCI_MANIFEST, WAVE_BUOY_BASE, WAVE_BUOY_MANIFEST, resolve_prefix

# (manifest directory, local base its keys live under)
GROUPS = ((NCI_MANIFEST, AODN_BASE), (WAVE_BUOY_MANIFEST, WAVE_BUOY_BASE))


def key_for(path):
    """Manifest key for an absolute local path, or None. WAVE_BUOY_BASE is
    nested under AODN_BASE, so it has to be tried first."""
    for base in (WAVE_BUOY_BASE, AODN_BASE):
        if path.startswith(base + "/"):
            return path[len(base) + 1:]
    return None


def cmd_keys(paths_out, roots_out, shallow_out):
    roots = set()
    with open(paths_out, "w") as out, open(shallow_out, "w") as shallow:
        for manifest_dir, base in GROUPS:
            for name in sorted(os.listdir(manifest_dir)):
                if not name.endswith(".csv"):
                    continue
                count = 0
                with open(os.path.join(manifest_dir, name), newline="") as f:
                    reader = csv.reader(f)
                    next(reader, None)
                    for row in reader:
                        if not row:
                            continue
                        key = row[0]
                        path = base + "/" + key
                        parts = key.split("/")
                        if len(parts) >= 3:
                            roots.add(base + "/" + parts[0] + "/" + parts[1])
                            out.write(path + "\n")
                        elif not os.path.lexists(path):
                            shallow.write(path + "\n")
                        count += 1
                print("{}: {} entries".format(os.path.join(manifest_dir, name), count), flush=True)
    with open(roots_out, "w") as f:
        for root in sorted(roots):
            f.write(root + "\n")


def cmd_summary(missing_list):
    counts = collections.Counter()
    with open(missing_list) as f:
        for line in f:
            key = key_for(line.rstrip("\n"))
            counts[(resolve_prefix(key) if key else None) or "(unresolved)"] += 1
    for prefix, n in sorted(counts.items()):
        print("{}\t{}".format(prefix, n))
    print("TOTAL\t{}".format(sum(counts.values())))


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "keys":
        cmd_keys(*sys.argv[2:])
    elif len(sys.argv) == 3 and sys.argv[1] == "summary":
        cmd_summary(sys.argv[2])
    else:
        sys.exit("usage: prune_scan_helper.py keys <paths_out> <roots_out> <shallow_out> | summary <missing_list>")
