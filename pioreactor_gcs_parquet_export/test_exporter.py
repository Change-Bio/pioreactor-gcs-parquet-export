# -*- coding: utf-8 -*-
"""
Off-reactor tests for the pure export logic (no pioreactor needed).
Run with the dev venv: .venv-dev/bin/python -m pytest pioreactor_gcs_parquet_export/test_exporter.py
"""
import os
import sqlite3

import polars as pl
import pytest

from pioreactor_gcs_parquet_export import exporter as ex


# --------------------------------------------------------------------------- fixtures


def make_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE od_readings (experiment TEXT, pioreactor_unit TEXT, timestamp TEXT, od_reading REAL)"
    )
    conn.execute("CREATE TABLE experiments (experiment TEXT, created_at TEXT, description TEXT)")
    conn.execute("CREATE TABLE workers (pioreactor_unit TEXT, added_at TEXT)")
    # no experiment column -> should never be a data table
    conn.execute("CREATE TABLE config_files_histories (filename TEXT, data TEXT)")
    conn.executemany(
        "INSERT INTO experiments VALUES (?,?,?)",
        [("Run A Variant 5", "2026-02-23T00:00:00Z", "first"), ("260526_V12", "2026-05-26T00:00:00Z", "")],
    )
    conn.execute("INSERT INTO workers VALUES ('tars','2026-01-01')")
    conn.execute("INSERT INTO config_files_histories VALUES ('config.ini','x')")
    conn.executemany(
        "INSERT INTO od_readings VALUES (?,?,?,?)",
        [("Run A Variant 5", "tars", f"t{i}", 0.1 * i) for i in range(5)]
        + [("260526_V12", "tars", f"t{i}", 1.0 * i) for i in range(3)],
    )
    conn.commit()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    conn.close()


def make_cfg(tmp_path, db):
    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir()
    return {
        "bucket": str(bucket_dir),
        "prefix": "",
        "data_tables": "auto",
        "exclude": ["experiments"],
        "meta_tables": ["experiments", "workers"],
        "gcloud_path": "gcloud",
        "gcp_project": "",
        "staging_dir": str(tmp_path / "staging"),
        "db_path": str(db),
        "state_path": str(tmp_path / "state.json"),
    }, str(bucket_dir)


def local_uploader(local_path, dest):
    """Fake GCS: dest is a local filesystem path under the 'bucket' dir."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with open(local_path, "rb") as s, open(dest, "wb") as d:
        d.write(s.read())


# --------------------------------------------------------------------------- unit tests


def test_slugify():
    assert ex.slugify_experiment("Run 2026-02-23 Variant 5") == "Run_2026-02-23_Variant_5"
    assert ex.slugify_experiment("  weird/name?!  ") == "weird_name"
    assert ex.slugify_experiment("") == "unnamed"


def test_gcs_join():
    assert ex.gcs_join("gs://b", "", "od_readings", "experiment=X", "p.parquet") == "gs://b/od_readings/experiment=X/p.parquet"
    assert ex.gcs_join("gs://b/", "pre/", "t") == "gs://b/pre/t"


def test_discovery_and_resolve(tmp_path):
    db = tmp_path / "t.sqlite"
    make_db(str(db))
    conn = ex._ro_connect(str(db))
    assert set(ex.discover_experiment_tables(conn)) == {"od_readings", "experiments"}
    # 'auto' minus exclude drops experiments + non-experiment tables
    assert ex.resolve_data_tables(conn, "auto", ["experiments"]) == ["od_readings"]
    conn.close()


def test_full_sync_and_hive_readback(tmp_path):
    db = tmp_path / "t.sqlite"
    make_db(str(db))
    cfg, bucket = make_cfg(tmp_path, db)

    stats = ex.sync_once(cfg, uploader=local_uploader)
    assert stats["errors"] == []
    assert stats["rows_uploaded"] == 8  # 5 + 3
    assert stats["files_uploaded"] == 2 + 2  # 2 experiment parts + 2 meta tables

    # hive read-back the way the user will (polars), filter one experiment
    df = pl.read_parquet(f"{bucket}/od_readings/**/*.parquet", hive_partitioning=True)
    assert df.height == 8
    # partition key is experiment_slug (path); experiment (real name) is in the data
    assert "experiment_slug" in df.columns and "experiment" in df.columns
    assert set(df["experiment"].unique().to_list()) == {"Run A Variant 5", "260526_V12"}
    assert set(df["experiment_slug"].unique().to_list()) == {"Run_A_Variant_5", "260526_V12"}
    # the in-data experiment column is authoritative (real name w/ spaces), not shadowed
    assert df.filter(pl.col("experiment") == "Run A Variant 5").height == 5
    # meta present
    assert pl.read_parquet(f"{bucket}/_meta/experiments.parquet").height == 2
    assert pl.read_parquet(f"{bucket}/_meta/workers.parquet").height == 1


def test_incremental_only_new_rows(tmp_path):
    db = tmp_path / "t.sqlite"
    make_db(str(db))
    cfg, bucket = make_cfg(tmp_path, db)

    s1 = ex.sync_once(cfg, uploader=local_uploader)
    files_after_first = sorted(os.listdir(f"{bucket}/od_readings/experiment_slug=260526_V12"))

    # second run with no new data -> no new part files, no rows
    s2 = ex.sync_once(cfg, uploader=local_uploader)
    assert s2["rows_uploaded"] == 0
    assert s2["files_uploaded"] == 2  # only the two meta tables re-uploaded

    # add 2 new rows to one experiment, re-sync -> exactly 2 new rows, one new part
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO od_readings VALUES (?,?,?,?)",
        [("260526_V12", "tars", f"new{i}", 9.0) for i in range(2)],
    )
    conn.commit()
    conn.close()
    s3 = ex.sync_once(cfg, uploader=local_uploader)
    assert s3["rows_uploaded"] == 2
    files_after_third = sorted(os.listdir(f"{bucket}/od_readings/experiment_slug=260526_V12"))
    assert len(files_after_third) == len(files_after_first) + 1  # a new immutable part

    # total rows across all parts for that experiment == 5 now
    df = pl.read_parquet(f"{bucket}/od_readings/experiment_slug=260526_V12/*.parquet")
    assert df.height == 5


def test_self_heal_on_shrink(tmp_path):
    db = tmp_path / "t.sqlite"
    make_db(str(db))
    cfg, bucket = make_cfg(tmp_path, db)
    ex.sync_once(cfg, uploader=local_uploader)

    # Simulate a VACUUM-like rebuild: delete all rows for an experiment and re-insert fewer.
    conn = sqlite3.connect(str(db))
    conn.execute("DELETE FROM od_readings WHERE experiment='260526_V12'")
    conn.execute("INSERT INTO od_readings VALUES ('260526_V12','tars','reborn', 7.0)")
    conn.commit()
    conn.execute("VACUUM")
    conn.commit()
    conn.close()

    # state still has the old (higher) watermark/count -> guard should re-backfill
    s = ex.sync_once(cfg, uploader=local_uploader)
    assert s["errors"] == []
    # the experiment now has 1 row; the latest part should contain it
    df = pl.read_parquet(f"{bucket}/od_readings/experiment_slug=260526_V12/*.parquet")
    assert df.filter(pl.col("timestamp") == "reborn").height >= 1


def test_sub_batching_produces_multiple_parts(tmp_path):
    db = tmp_path / "t.sqlite"
    make_db(str(db))
    cfg, bucket = make_cfg(tmp_path, db)
    cfg["batch_rows"] = 2  # force multiple parts for the 5-row experiment
    stats = ex.sync_once(cfg, uploader=local_uploader)
    assert stats["errors"] == []
    parts = sorted(os.listdir(f"{bucket}/od_readings/experiment_slug=Run_A_Variant_5"))
    assert len(parts) == 3  # 5 rows / batch 2 -> ceil = 3 parts
    # no rows lost or duplicated across parts
    df = pl.read_parquet(f"{bucket}/od_readings/experiment_slug=Run_A_Variant_5/*.parquet")
    assert df.height == 5


def test_without_rowid_table_skipped(tmp_path):
    db = tmp_path / "t.sqlite"
    make_db(str(db))
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE weird (experiment TEXT, k TEXT, PRIMARY KEY (experiment, k)) WITHOUT ROWID"
    )
    conn.execute("INSERT INTO weird VALUES ('260526_V12','a')")
    conn.commit()
    conn.close()
    cfg, bucket = make_cfg(tmp_path, db)
    logs = []
    stats = ex.sync_once(cfg, uploader=local_uploader, log=lambda lvl, m: logs.append((lvl, m)))
    assert stats["errors"] == []
    assert any("WITHOUT ROWID" in m for lvl, m in logs)
    assert not os.path.exists(f"{bucket}/weird")
