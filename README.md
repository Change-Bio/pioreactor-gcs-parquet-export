# pioreactor-gcs-parquet-export

Leader-only Pioreactor plugin that incrementally exports the leader's SQLite data to
**Parquet in a GCS bucket**, partitioned by experiment, on startup and then on a
configurable interval. Built for downstream querying with **polars** and **BigQuery**.

## How it works

- A background job (`gcs_parquet_export`) runs on the leader. On startup and every
  `sync_interval_seconds` it reads the durable SQLite DB (read-only, WAL-safe) — *not*
  MQTT — so nothing is lost across restarts/disconnections.
- **Incremental by `rowid`** per `(table, experiment)`: each cycle uploads only new
  rows as immutable Parquet *part files*. A watermark state file
  (`~/.pioreactor/storage/gcs_export_state.json`) tracks progress. First cycle
  backfills everything.
- **duckdb** streams `SQLite → Parquet` directly (typed, low memory). Upload is via
  the leader's existing `gcloud storage cp` (service-account auth already configured).

## GCS layout

```
gs://<bucket>/<prefix>/
  <table>/experiment_slug=<slug>/part-<lo>-<hi>.parquet   # per-experiment data, incremental
  _meta/<table>.parquet                                   # whole metadata tables, overwritten each cycle
```

The partition key is **`experiment_slug`** (a path-safe slug), deliberately *not*
`experiment`: a Hive partition key must not duplicate an in-data column, or polars
shadows it and BigQuery rejects the table. The **authoritative experiment name stays
in the data** (every row has the real `experiment` column) — filter on that.

### Querying

```python
import polars as pl
# one table, one experiment (direct path):
pl.read_parquet("gs://<bucket>/od_readings/experiment_slug=260526_V12/*.parquet")
# whole table, filter on the real name:
(pl.scan_parquet("gs://<bucket>/od_readings/**/*.parquet", hive_partitioning=True)
   .filter(pl.col("experiment") == "Run 2026-02-23 Variant 5").collect())
```

BigQuery: one external table per Pioreactor table, source `gs://<bucket>/<table>/*`,
Hive partitioning on `experiment_slug`.

## Configuration (`[gcs_parquet_export.config]`)

| key | default | meaning |
|---|---|---|
| `bucket` | (none — **required**) | destination bucket, e.g. `gs://your-bucket` |
| `prefix` | (empty) | optional path prefix under the bucket |
| `sync_interval_seconds` | `3600` | seconds between syncs (first runs immediately) |
| `data_tables` | `auto` | `auto` (every table with an `experiment` column) or a comma list |
| `exclude` | reference tables | tables removed from the `auto` data set (shipped via `meta_tables` instead) |
| `meta_tables` | see config | whole tables written to `_meta/` each cycle |
| `gcloud_path` | `/home/pioreactor/google-cloud-sdk/bin/gcloud` | gcloud binary |
| `gcp_project` | (empty) | project for `gcloud storage cp` (set if gcloud needs it explicitly) |
| `staging_dir` | `/tmp/pioreactor_gcs_export` | local scratch for parquet before upload |
| `db_path` / `state_path` | storage dir | SQLite source / watermark state |

## Prerequisites

- A `gcloud` SDK on the leader, authenticated (e.g. as a service account) with write
  access to the destination bucket.
- The destination bucket must exist and the identity `gcloud` runs as needs
  `roles/storage.objectAdmin` (or equivalent) on it.

## Install

```bash
# on the leader
pio plugins install pioreactor-gcs-parquet-export
# or from source
pio plugins install --source /path/to/pioreactor-gcs-parquet-export
```

`post_install.sh` enables `pioreactor_startup_run@gcs_parquet_export.service` so it
runs on boot. Start manually with `pio run gcs_parquet_export` (optionally
`--sync-interval-seconds 60` for a quick test).

## Known limitations

- The rowid watermark assumes append-only tables (Pioreactor time-series are). A
  manual `VACUUM` can renumber rowids; a self-heal guard detects the shrink/mismatch
  and re-backfills that `(table, experiment)`, which **duplicates** part files in GCS
  for that key. If it fires, delete the affected
  `…/<table>/experiment_slug=<slug>/` prefix and let it rebuild.
- Reads the live WAL DB read-only (per-cycle consistency is not snapshotted).

## Development / tests

```bash
python -m venv .venv-dev && .venv-dev/bin/pip install duckdb polars pytest
.venv-dev/bin/python -m pytest pioreactor_gcs_parquet_export/test_exporter.py   # off-reactor core
# on a Pioreactor: pytest pioreactor_gcs_parquet_export/test_gcs_parquet_export.py
```

## License

MIT — see LICENSE.txt
