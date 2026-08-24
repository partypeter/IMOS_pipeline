#!/usr/bin/env python3
# ------------------------------------------------------------------
#- IMPLEMENTATION
#- author      Hannes Hollmann, National Computational Infrastructure
#- license     GNU General Public License
# ------------------------------------------------------------------
# SYNOPSIS
#    compare_manifests.py
#
# DESCRIPTION
#    Compares the latest local S3 inventory CSVs against per-prefix NCI
#    manifests (one CSV per top-level IMOS/<prefix>/ folder), and writes
#    any files found in the inventory but missing from the relevant
#    manifest into a per-prefix temp manifest under nci_imos_manifest/temp/.
#    This is stage 2 of the pipeline ("shard diff") — its temp manifests
#    drive download.py (stage 3), which downloads the new files and merges
#    each temp manifest back into its persistent manifest.
#
#    If a prefix already has a pending temp manifest (i.e. a previous batch
#    hasn't been confirmed and merged yet), that prefix is skipped entirely
#    for this run, so a re-run never overwrites or double-queues a pending
#    batch.
#
#    Manifests for newly-discovered IMOS prefixes are created automatically.
#
#    Assumes the whole-bucket inventory CSVs for the date being used have
#    already been downloaded and decompressed (see get_imos_manifest.py)
#    into MANIFEST_MAIN/<date>/*.csv.
#
#    Persistent manifests live at nci_imos_manifest/<prefix>.csv
#    Temp manifests live at       nci_imos_manifest/temp/<prefix>.csv
#    Both use header: filepath,checksum
#
#    Most prefixes' persistent manifests are small enough to diff entirely
#    in memory (see load_existing_filepaths). A prefix whose manifest
#    exceeds LARGE_PREFIX_ROW_THRESHOLD rows (config.py) instead uses an
#    on-disk sort+join diff (see prepare_large_prefix_reference and
#    reconcile_large_prefix) — GNU sort/join scale past available RAM by
#    spilling to disk, unlike a Python set, which needs the whole prefix
#    resident in memory at once. This split exists because one prefix
#    (AUV, ~82M genuinely unique files) is too large to hold in RAM even
#    when processed on its own, on the 32GB datamover VM this runs on.
#
# REQUIRES
#    boto3
#
# VERSION
#    1.0: Initial implementation
#    1.1: Paths, logging, S3 client, and date-dir lookup moved to
#         config.py / common.py.
#    1.2: Added a disk-based sort+join diff path for prefixes whose
#         persistent manifest is too large to hold in memory (fixes an
#         OOM on the AUV prefix); the in-memory path is unchanged for
#         every other prefix.
#
# ================================================================
# END_OF_HEADER
# ================================================================

import csv
import os
import subprocess
import time

from common import count_csv_rows, find_latest_date_dir, get_s3_client, peak_memory_gb, read_csv_rows, setup_logging
from config import (
    COMPARE_SCRATCH_DIR,
    IMOS_PREFIX,
    LARGE_PREFIX_ROW_THRESHOLD,
    LOG_DIR,
    MANIFEST_MAIN,
    NCI_MANIFEST,
    SOURCE_BUCKET,
    TEMP_DIR,
)


def get_imos_prefixes(s3):
    """Top-level folder names under s3://imos-data/IMOS/, e.g. 'AATAMS'."""
    resp = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=IMOS_PREFIX, Delimiter="/")
    prefixes = []
    for cp in resp.get("CommonPrefixes", []):
        # cp["Prefix"] looks like "IMOS/AATAMS/"
        name = cp["Prefix"][len(IMOS_PREFIX):].rstrip("/")
        if name:
            prefixes.append(name)
    return prefixes


def load_existing_filepaths(manifest_csv):
    """Retries a transient read failure (see common.read_csv_rows) instead
    of aborting the whole run outright — this manifest lives on /g/data
    mounted over the network onto the datamover VM, where a brief hiccup
    can surface as a spurious mid-read error."""
    _, rows = read_csv_rows(manifest_csv)
    return {row[0] for row in rows}


def prepare_large_prefix_reference(prefix, shard_csv, scratch_dir):
    """For a large prefix, extract its persistent manifest's filepath
    column to a scratch file and sort it with GNU sort — the reference
    side of reconcile_large_prefix's join. Runs entirely outside Python's
    memory (one line read/written at a time; sort itself spills to disk),
    so this scales regardless of how many rows the prefix has."""
    extracted = os.path.join(scratch_dir, f"{prefix}_persistent_filepaths.txt")
    sorted_path = os.path.join(scratch_dir, f"{prefix}_persistent_filepaths.sorted.txt")

    with open(shard_csv, newline="") as fin, open(extracted, "w") as fout:
        reader = csv.reader(fin)
        next(reader, None)  # header
        for row in reader:
            fout.write(row[0] + "\n")

    subprocess.run(
        ["sort", "-T", scratch_dir, "-o", sorted_path, extracted],
        check=True,
        env={**os.environ, "LC_ALL": "C"},
    )
    os.remove(extracted)
    return sorted_path


def reconcile_large_prefix(prefix, raw_inventory_path, sorted_persistent_path, scratch_dir, temp_csv, log):
    """Phase 3 for a large prefix: sort the raw (filepath,etag) rows
    collected for it during the inventory scan, then join against its
    sorted persistent filepaths to find inventory rows with no match —
    i.e. new files. `join -v1` keeps an unmatched line from the inventory
    side whole, so the etag column survives; a plain `comm` would only
    tell us the filepaths matched or didn't, since it compares whole
    lines. The join's output is streamed straight to disk one row at a
    time, so memory use here stays flat regardless of prefix size."""
    sorted_inventory_path = os.path.join(scratch_dir, f"{prefix}_inventory.sorted.csv")
    subprocess.run(
        ["sort", "-t", ",", "-k1,1", "-T", scratch_dir, "-o", sorted_inventory_path, raw_inventory_path],
        check=True,
        env={**os.environ, "LC_ALL": "C"},
    )

    staged_csv = temp_csv + ".building"
    count = 0
    with open(staged_csv, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["filepath", "checksum"])
        proc = subprocess.Popen(
            ["join", "-t", ",", "-v1", "-1", "1", "-2", "1", sorted_inventory_path, sorted_persistent_path],
            stdout=subprocess.PIPE,
            text=True,
            env={**os.environ, "LC_ALL": "C"},
        )
        for line in proc.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            filepath, etag = line.split(",", 1)
            writer.writerow([filepath, etag])
            count += 1
        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, proc.args)

    if count:
        os.replace(staged_csv, temp_csv)
        log.info(f"{prefix}: wrote {count} new file(s) to temp manifest")
    else:
        os.remove(staged_csv)

    for path in (raw_inventory_path, sorted_inventory_path, sorted_persistent_path):
        if os.path.exists(path):
            os.remove(path)

    return count


def main():
    start = time.perf_counter()
    date_dir_name = find_latest_date_dir(MANIFEST_MAIN)
    if date_dir_name is None:
        print("No dated inventory folder found under", MANIFEST_MAIN)
        return
    date_dir = os.path.join(MANIFEST_MAIN, date_dir_name)

    log = setup_logging(LOG_DIR, os.path.join("manifest_comparison", date_dir_name))
    log.info(f"Using inventory date: {date_dir_name}")
    log.info(f"MANIFEST_MAIN = {MANIFEST_MAIN}")
    log.info(f"NCI_MANIFEST  = {NCI_MANIFEST}")
    log.info(f"TEMP_DIR      = {TEMP_DIR}")

    os.makedirs(NCI_MANIFEST, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(COMPARE_SCRATCH_DIR, exist_ok=True)

    if os.listdir(TEMP_DIR):
        log.info(f"{TEMP_DIR} is not empty — a previous batch is still pending confirmation. Aborting.")
        return

    s3 = get_s3_client()
    prefixes = get_imos_prefixes(s3)
    log.info(f"Found {len(prefixes)} IMOS prefixes")

    # Phase 1: per-prefix setup. Prefixes below LARGE_PREFIX_ROW_THRESHOLD
    # keep the fast in-memory path (shard_existing); prefixes at/above it
    # get their persistent filepaths extracted and sorted on disk instead,
    # since holding that many entries as a Python set risks OOM (see
    # module docstring).
    shard_existing = {}
    large_prefixes = {}  # prefix -> sorted persistent filepaths (scratch path)
    valid_prefixes = set()
    for prefix in prefixes:
        temp_csv = os.path.join(TEMP_DIR, f"{prefix}.csv")
        if os.path.exists(temp_csv):
            log.info(f"{prefix}: pending temp manifest already exists — skipping until confirmed/merged")
            continue

        shard_csv = os.path.join(NCI_MANIFEST, f"{prefix}.csv")
        if not os.path.exists(shard_csv):
            with open(shard_csv, "w", newline="") as f:
                csv.writer(f).writerow(["filepath", "checksum"])
            log.info(f"Created new manifest for prefix '{prefix}': {shard_csv}")

        row_count = count_csv_rows(shard_csv)
        if row_count > LARGE_PREFIX_ROW_THRESHOLD:
            log.info(
                f"{prefix}: {row_count} rows exceeds LARGE_PREFIX_ROW_THRESHOLD "
                f"({LARGE_PREFIX_ROW_THRESHOLD}) — using disk-based diff"
            )
            large_prefixes[prefix] = prepare_large_prefix_reference(prefix, shard_csv, COMPARE_SCRATCH_DIR)
        else:
            shard_existing[prefix] = load_existing_filepaths(shard_csv)
        valid_prefixes.add(prefix)

    # Phase 2: inventory scan (same single pass over every inventory CSV
    # for both kinds of prefix). Small prefixes get the existing in-memory
    # membership check. Large prefixes can't be checked yet — inventory
    # rows arrive in arbitrary order — so every matching row is appended
    # to a raw scratch file, reconciled against the sorted persistent
    # manifest afterward in phase 3.
    #
    # New entries for small prefixes are streamed straight to each
    # prefix's temp CSV as they're found, via a file handle opened lazily
    # on first use — this avoids holding a second full copy of the
    # new-entries data in memory alongside shard_existing (which roughly
    # doubled peak RAM on a bootstrap-style run where most files are new).
    temp_writers = {}  # prefix -> (file handle, csv.writer) — small prefixes
    raw_writers = {}  # prefix -> file handle — large prefixes' scratch input to phase 3
    new_counts = {prefix: 0 for prefix in valid_prefixes}

    inventory_csvs = [f for f in os.listdir(date_dir) if f.endswith(".csv")]
    log.info(f"Scanning {len(inventory_csvs)} inventory CSV files")

    try:
        for csv_name in inventory_csvs:
            with open(os.path.join(date_dir, csv_name), newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 5:
                        continue
                    key, etag = row[1], row[4]

                    if not key.startswith(IMOS_PREFIX):
                        continue
                    remainder = key[len(IMOS_PREFIX):]
                    prefix = remainder.split("/", 1)[0]

                    if prefix not in valid_prefixes:
                        continue

                    if prefix in large_prefixes:
                        if prefix not in raw_writers:
                            raw_path = os.path.join(COMPARE_SCRATCH_DIR, f"{prefix}_inventory_raw.csv")
                            raw_writers[prefix] = open(raw_path, "w")
                        raw_writers[prefix].write(f"{key},{etag}\n")
                        continue

                    if key in shard_existing[prefix]:
                        continue

                    if prefix not in temp_writers:
                        temp_csv = os.path.join(TEMP_DIR, f"{prefix}.csv")
                        fh = open(temp_csv, "w", newline="")
                        writer = csv.writer(fh)
                        writer.writerow(["filepath", "checksum"])
                        temp_writers[prefix] = (fh, writer)

                    temp_writers[prefix][1].writerow([key, etag])
                    shard_existing[prefix].add(key)  # avoid duplicate adds within this run
                    new_counts[prefix] += 1
    finally:
        for fh, _ in temp_writers.values():
            fh.close()
        for fh in raw_writers.values():
            fh.close()

    # Phase 3: reconcile large prefixes on disk now that the full day's
    # inventory rows for each of them have been collected.
    for prefix, sorted_persistent_path in large_prefixes.items():
        if prefix not in raw_writers:
            # No inventory rows matched this prefix at all this run.
            os.remove(sorted_persistent_path)
            continue
        raw_path = os.path.join(COMPARE_SCRATCH_DIR, f"{prefix}_inventory_raw.csv")
        temp_csv = os.path.join(TEMP_DIR, f"{prefix}.csv")
        new_counts[prefix] = reconcile_large_prefix(
            prefix, raw_path, sorted_persistent_path, COMPARE_SCRATCH_DIR, temp_csv, log
        )

    for prefix, count in new_counts.items():
        if count and prefix not in large_prefixes:
            log.info(f"{prefix}: wrote {count} new file(s) to temp manifest")

    elapsed = (time.perf_counter() - start) / 60
    log.info(f"Elapsed: {elapsed:.1f} minutes")
    log.info(f"Peak RAM: {peak_memory_gb():.2f} GB")
    log.info("Done.")


if __name__ == "__main__":
    main()
