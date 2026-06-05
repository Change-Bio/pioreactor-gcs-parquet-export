# pioreactor-gcs-parquet-export

A leader-only [Pioreactor](https://pioreactor.com) plugin that **incrementally exports the
leader's SQLite database to Parquet in a Google Cloud Storage bucket**, partitioned by
experiment, on startup and then on a configurable interval. The result is a cloud
**data lake** you can query with [polars](https://pola.rs), DuckDB, BigQuery, etc. —
without touching the reactor.

---

## What it does

The Pioreactor leader stores all timeseries and event data in a local SQLite database
(`~/.pioreactor/storage/pioreactor.sqlite`). This plugin runs a background job on the
leader that, each cycle:

1. Looks at every **experiment-scoped table** (any table with an `experiment` column).
2. Exports **only the rows added since last time** to Parquet (incremental — see below).
3. Uploads them to `gs://<bucket>/…`, laid out so each experiment is its own partition.
4. Snapshots a few small **reference tables** (`experiments`, `workers`, …) to `_meta/`.

It reads the **durable SQLite database**, not the live MQTT stream — so a restart, crash,
or network blip never loses data; the next cycle simply resumes from the last watermark.

---

## GCS layout

```
gs://<bucket>/<prefix>/
  <table>/experiment_slug=<slug>/part-<lo>-<hi>.parquet   ← per-experiment data (incremental)
  _meta/<table>/source=<label>/<table>.parquet            ← reference tables (per-source snapshot)
```

Example:

```
gs://my-bucket/
  od_readings/experiment_slug=My_Run_2026-02-01/part-000000000001-000000200000.parquet
  od_readings/experiment_slug=My_Run_2026-02-01/part-000000200001-000000245000.parquet
  growth_rates/experiment_slug=My_Run_2026-02-01/part-000000000001-000000000153.parquet
  _meta/experiments/source=leader1/experiments.parquet
  _meta/workers/source=leader1/workers.parquet
```

### Two partitioning schemes, and why

- **Data tables → partitioned by `experiment_slug`.** Each experiment's rows live under
  `<table>/experiment_slug=<slug>/`. The partition key is deliberately named
  **`experiment_slug`, not `experiment`** — a Hive partition key must not duplicate a
  column inside the files, or readers shadow the real value and BigQuery rejects the
  table. The **authoritative experiment name stays in the data** (every row has the real
  `experiment` column, spaces and all); `<slug>` is just a path-safe rendering for
  browsing/partition-pruning.

- **`_meta/` tables → partitioned by `source`.** `experiments`, `workers`, and the
  worker-assignment tables aren't experiment-scoped — they're small whole-table
  snapshots, re-written in full every cycle. If two sources wrote the same
  `_meta/experiments.parquet`, they'd clobber each other. Instead each source owns
  `_meta/<table>/source=<label>/`, overwriting only its own partition. On read they union
  into one table with a `source` column. (`source` defaults to the leader's unit name; set
  `source_label` to give each cluster/archive a distinct one.)

### Incremental model

Per cycle, each data table is scanned **once in `rowid` order** (`WHERE rowid > watermark`,
which rides SQLite's integer-primary-key index) and the rows are partitioned by experiment
in memory. The high-water `rowid` per table is persisted in a small JSON state file
(`~/.pioreactor/storage/gcs_export_state.json`). Consequences:

- First run **backfills everything**; later runs upload only new rows as **immutable part
  files** (named by their `rowid` range, so re-runs are idempotent).
- An unchanged table costs a single `MAX(rowid)` check — effectively free.
- Filtering by `experiment` in SQL is deliberately avoided (no index on it → full scans).
- **Self-heal:** if a table's max `rowid` goes *backwards* (e.g. a `VACUUM` renumbered
  rows), the watermark resets and the table re-backfills. See *Limitations*.

### Merging multiple sources into one lake

Because data partitions are keyed by experiment and `_meta` by `source`, **several leaders
(or a historical archive from another database) can write into the same bucket** and read
back as one dataset — provided experiment names are unique across sources (Pioreactor
experiment names usually are). Give each source a distinct `source_label`. Any tool that
produces the **same per-table Parquet schema** under the same paths participates in the
same lake.

---

## Querying

```python
import polars as pl

# one experiment, one table — direct path:
pl.read_parquet("gs://my-bucket/od_readings/experiment_slug=My_Run_2026-02-01/*.parquet")

# a whole table across all experiments/sources, filtered on the real name:
(pl.scan_parquet("gs://my-bucket/od_readings/**/*.parquet", hive_partitioning=True)
   .filter(pl.col("experiment") == "My Run 2026-02-01")
   .collect())

# all experiments metadata across sources:
pl.read_parquet("gs://my-bucket/_meta/experiments/**/*.parquet", hive_partitioning=True)
```

**BigQuery:** define one external table per Pioreactor table with source URI
`gs://my-bucket/<table>/*` and Hive partitioning enabled (partition column
`experiment_slug`).

---

## Configuration — `[gcs_parquet_export.config]`

| key | default | meaning |
|---|---|---|
| `bucket` | (none — **required**) | destination bucket, e.g. `gs://my-bucket` |
| `prefix` | (empty) | optional path prefix under the bucket |
| `sync_interval_seconds` | `3600` | seconds between syncs (the first runs immediately on start) |
| `data_tables` | `auto` | `auto` = every table with an `experiment` column; or a comma list |
| `exclude` | (empty) | tables to drop from the `auto` data set (e.g. reference tables you ship via `meta_tables`) |
| `meta_tables` | (empty) | whole tables snapshotted to `_meta/<t>/source=<label>/` each cycle |
| `source_label` | this unit's name | label for this source's `_meta` partitions; set distinct per cluster sharing a bucket |
| `gcloud_path` | `/home/pioreactor/google-cloud-sdk/bin/gcloud` | gcloud binary used for uploads |
| `gcp_project` | (empty) | project passed to `gcloud storage cp` (if needed) |
| `staging_dir` | `/tmp/pioreactor_gcs_export` | local scratch for Parquet before upload |
| `batch_rows` | `200000` | rows per part file (bounds memory during backfill) |
| `db_path` / `state_path` | storage dir | SQLite source / watermark state file |

---

## Prerequisites

- An authenticated **`gcloud` SDK on the leader** with write access to the bucket. The
  plugin shells out to `gcloud storage cp` (rather than adding a Python GCS client, which
  may have no wheel for the Pi — see below), so it reuses whatever identity `gcloud` is
  configured with. A service account with `roles/storage.objectAdmin` on the bucket is
  typical.
- The destination **bucket must already exist**.

## Install

```bash
# on the leader
pio plugins install pioreactor-gcs-parquet-export
# or from source / a fork
pio plugins install pioreactor-gcs-parquet-export \
  --source https://github.com/<you>/pioreactor-gcs-parquet-export/archive/main.zip
```

`post_install.sh` enables a systemd unit so the job **auto-starts on boot**:
`pioreactor_startup_run@gcs_parquet_export.service`. Run it manually any time with
`pio run gcs_parquet_export` (add `--sync-interval-seconds 60` for a quick test). It also
appears in the web UI under Activities, with live status settings (last sync time/result,
rows/files uploaded).

> **Stopping the job:** always use `pio kill --job-name gcs_parquet_export`, never `kill`.
> A hard kill leaves a stale entry in Pioreactor's job-metadata cache that blocks restarts.

---

## Design notes (why duckdb, and the Raspberry Pi)

Pioreactor leaders are often a **32-bit `armv7l`** Python even on a 64-bit kernel. On that
platform `pyarrow`/`polars` have no wheels, and duckdb's downloadable extensions
(`sqlite_scanner`, etc.) aren't available. So this plugin:

- reads with the **stdlib `sqlite3`** module (always present),
- writes Parquet with **duckdb's bundled core** writer — staging rows as newline-delimited
  JSON and loading them via `read_json` (duckdb `executemany` is ~200× slower for bulk),
- pins **`duckdb==1.5.1`** (newest version with a prebuilt `armv7l`/cp313 wheel),
- uploads via the leader's existing **`gcloud`** rather than a Python cloud client.

Net new dependency: just `duckdb`.

---

## Known limitations

- **rowid watermark** assumes append-only tables (Pioreactor timeseries are). A manual
  `VACUUM` renumbers rowids; the self-heal guard detects the shrink and re-backfills that
  table, which **duplicates** part files for it. If that happens, delete the affected
  `<table>/experiment_slug=*/` objects and let it rebuild.
- Reads the **live WAL database read-only** (concurrent reads are safe); it is not a
  per-cycle consistent snapshot.
- Merging multiple sources assumes **experiment names are unique across sources**.

## Development

```bash
python -m venv .venv-dev && .venv-dev/bin/pip install duckdb polars pytest
.venv-dev/bin/python -m pytest pioreactor_gcs_parquet_export/test_exporter.py   # off-reactor core
# on a Pioreactor (needs the pioreactor package):
pytest pioreactor_gcs_parquet_export/test_gcs_parquet_export.py
```

The core export logic (`exporter.py`) has no `pioreactor` imports, so it runs and tests
anywhere; `gcs_parquet_export.py` is the thin BackgroundJob wrapper.

## License

MIT — see LICENSE.txt
