# IMOS_pipeline

A nightly pipeline that mirrors the public [IMOS](https://imos.org.au) /
[AODN](https://aodn.org.au) data bucket on AWS S3 (`s3://imos-data`) to the
National Computational Infrastructure (NCI), Australia. The mirror lives on
NCI's `/g/data` storage under project `sh74`, so researchers can work with the
data directly on NCI systems.

Every night, the pipeline compares Amazon's daily S3 Inventory of the bucket
with a record of what has already been mirrored. It then downloads new files
and moves files that were removed upstream out of the mirror. It never lists
the bucket directly and never re-copies data it already holds.

## How it works

`run_imos_pipeline.sh` runs six stages in order and stops at the first one
that fails:

| Stage | Script | What it does |
|---|---|---|
| 1 | `get_imos_manifest.py` | Downloads the latest daily S3 Inventory of the bucket |
| 2 | `compare_manifests.py` | Compares the inventory with the per-prefix manifests (the record of what's mirrored) and queues new files |
| 3 | `download.py` | Downloads the queued files, verifying each against its S3 checksum where one exists (file size otherwise), then records them in the manifests |
| 4 | `mv_to_prod/coordinator.sh` | Moves the downloaded files from staging into the public mirror |
| 5 | `retract_diff.py` | Moves files that no longer exist upstream into a quarantine folder, which is cleared after 28 days |
| 6 | `backup_manifests.py` | Copies the manifests to `/g/data` as a backup |

There is one manifest per top-level `IMOS/<prefix>/` folder, discovered
automatically. A few non-`IMOS/` roots holding partner wave buoy data are
tracked as well (`EXPLICIT_PREFIXES` in `config.py`).

## Safeguards

- **Retraction guard.** If the latest inventory lists fewer than 90% of the
  files currently tracked, `retract_diff.py` aborts. A broken or incomplete
  inventory can never empty the mirror.
- **Checked rewrites.** A rewritten manifest only replaces the original after
  it has been read back from storage with the expected line count and no NUL
  bytes.
- **No double queuing.** A prefix with a batch still pending is skipped
  rather than queued twice.
- **Awkward keys handled.** Keys from the S3 Inventory are URL-decoded, so
  filenames containing spaces, quotes, commas, `@` and similar characters are
  handled correctly.
- **Folder markers ignored.** S3 "folder marker" objects (keys ending in `/`)
  are skipped.

## Configuration

Python stages read their paths and settings from `config.py`, and the bash
stages from `config.sh`. The two must be kept in sync by hand. The defaults
match the NCI deployment:
- manifests, inventory and scratch space are on the data-mover VM's local
  disk;
- downloaded data, logs and the manifest backup are on `/g/data/sh74`.

## Requirements

- Python 3.6+ with `boto3`; `awscrt` is optional, for CRC32C/CRC64NVME
  checksum verification
- GNU coreutils (`sort`, `join`) and `setfacl`
- Anonymous read access to `s3://imos-data` and `s3://imos-data-inventory`
  (no AWS credentials needed)

## Running

```
bash run_imos_pipeline.sh
```

At NCI this runs nightly from cron. Each stage writes its own log to
`LOGS_BASE/<stage>/<run id>.log`, and a short summary of every stage goes to
`LOGS_BASE/pipeline/<run id>.log`.

## Limitations

- A file is only downloaded once. If an object is later replaced upstream
  under the same key, the change is not picked up.
- New data appears in the mirror about one day behind S3, because Amazon
  generates the S3 Inventory daily.

## Author and license

Hannes Hollmann, National Computational Infrastructure (NCI), Australia.
Released under the GNU General Public License v3.0 (see `LICENSE`). To cite this
software, see `CITATION.cff` or GitHub's "Cite this repository" button.
