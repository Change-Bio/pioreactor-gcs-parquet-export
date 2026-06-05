# -*- coding: utf-8 -*-
"""
Core export logic for pioreactor-gcs-parquet-export.

Deliberately free of any `pioreactor` imports so it can be unit-tested off-reactor
with nothing but stdlib sqlite3 + duckdb. The BackgroundJob in
``gcs_parquet_export.py`` is a thin wrapper that wires config + a periodic timer +
MQTT status fields around ``sync_once``.

Data flow per cycle:
  SQLite (system of record, read-only)  --duckdb COPY-->  local parquet  --uploader-->  GCS

Incremental by rowid per (table, experiment): each cycle exports only rows with
rowid in (saved_watermark, current_max] as an immutable part file. A self-heal
guard re-backfills if the table appears to have shrunk/renumbered (e.g. VACUUM).
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import tempfile
from typing import Callable, Iterable, Optional

import duckdb

# ----------------------------------------------------------------------------- helpers

_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")


def slugify_experiment(name: str) -> str:
    """Path-safe segment for an experiment name. The authoritative name is also a
    column inside every parquet (we SELECT *), so this is purely organisational."""
    slug = _SLUG_RE.sub("_", name.strip())
    return slug.strip("_") or "unnamed"


def _ro_connect(db_path: str) -> sqlite3.Connection:
    """Read-only sqlite connection (safe against the live WAL DB)."""
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def gcs_join(*parts: str) -> str:
    """Join GCS path parts, collapsing empty segments and stray slashes."""
    cleaned = []
    for i, p in enumerate(parts):
        if p is None:
            continue
        p = p.strip()
        if not p:
            continue
        cleaned.append(p if i == 0 else p.strip("/"))
    return "/".join(s.rstrip("/") if i == 0 else s for i, s in enumerate(cleaned))


# ----------------------------------------------------------------------------- introspection


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()
    return any(c[1] == column for c in cols)


def table_has_rowid(conn: sqlite3.Connection, table: str) -> bool:
    """WITHOUT ROWID tables raise when selecting rowid; detect that defensively."""
    try:
        conn.execute(f"SELECT rowid FROM {_quote_ident(table)} LIMIT 1").fetchone()
        return True
    except sqlite3.OperationalError:
        return False


def discover_experiment_tables(conn: sqlite3.Connection) -> list[str]:
    """Every base table that has an `experiment` column, sorted."""
    names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]
    return [t for t in names if table_has_column(conn, t, "experiment")]


def resolve_data_tables(
    conn: sqlite3.Connection, data_tables: str, exclude: Iterable[str]
) -> list[str]:
    """`data_tables` is 'auto' (discover) or a comma list. Always minus `exclude`."""
    exclude = set(exclude)
    if data_tables.strip().lower() == "auto":
        candidates = discover_experiment_tables(conn)
    else:
        candidates = [t.strip() for t in data_tables.split(",") if t.strip()]
    return [t for t in candidates if t not in exclude and table_exists(conn, t)]


def experiments_in_table(conn: sqlite3.Connection, table: str) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT experiment FROM {_quote_ident(table)} "
            "WHERE experiment IS NOT NULL ORDER BY experiment"
        ).fetchall()
    ]


def partition_bounds(conn: sqlite3.Connection, table: str, experiment: str) -> tuple[int, int]:
    """(max_rowid, count) for one experiment partition. max_rowid is 0 if empty."""
    row = conn.execute(
        f"SELECT COALESCE(MAX(rowid), 0), COUNT(*) FROM {_quote_ident(table)} "
        "WHERE experiment = ?",
        (experiment,),
    ).fetchone()
    return int(row[0]), int(row[1])


def count_up_to(conn: sqlite3.Connection, table: str, experiment: str, rowid: int) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM {_quote_ident(table)} "
        "WHERE experiment = ? AND rowid <= ?",
        (experiment, rowid),
    ).fetchone()
    return int(row[0])


# ----------------------------------------------------------------------------- state (watermarks)


def load_state(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)  # atomic


# ----------------------------------------------------------------------------- duckdb export


def attach_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL sqlite")
    con.execute("LOAD sqlite")
    con.execute(f"ATTACH {_quote_literal(db_path)} AS pr (TYPE sqlite, READ_ONLY)")
    return con


def export_window_parquet(
    con: duckdb.DuckDBPyConnection, table: str, experiment: str, lo: int, hi: int, out_path: str
) -> None:
    """COPY rows (rowid in (lo, hi]) for one experiment to a parquet file."""
    sql = (
        f"COPY (SELECT * FROM pr.{_quote_ident(table)} "
        f"WHERE experiment = {_quote_literal(experiment)} "
        f"AND rowid > {int(lo)} AND rowid <= {int(hi)}) "
        f"TO {_quote_literal(out_path)} (FORMAT PARQUET)"
    )
    con.execute(sql)


def export_full_parquet(con: duckdb.DuckDBPyConnection, table: str, out_path: str) -> None:
    """COPY a whole table to a parquet file (used for _meta tables)."""
    con.execute(
        f"COPY (SELECT * FROM pr.{_quote_ident(table)}) "
        f"TO {_quote_literal(out_path)} (FORMAT PARQUET)"
    )


# ----------------------------------------------------------------------------- upload


def make_gcloud_uploader(gcloud_path: str, project: str) -> Callable[[str, str], None]:
    """Returns uploader(local_path, gcs_dest) that shells out to `gcloud storage cp`,
    reusing the service-account auth already configured on the leader."""

    def _upload(local_path: str, gcs_dest: str) -> None:
        cmd = [gcloud_path, "storage", "cp", local_path, gcs_dest]
        if project:
            cmd += ["--project", project]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"gcloud storage cp failed ({result.returncode}): {result.stderr.strip()}"
            )

    return _upload


# ----------------------------------------------------------------------------- orchestration


def _noop_log(level: str, msg: str) -> None:
    pass


def sync_once(
    cfg: dict,
    uploader: Optional[Callable[[str, str], None]] = None,
    log: Callable[[str, str], None] = _noop_log,
    should_continue: Callable[[], bool] = lambda: True,
) -> dict:
    """Run one full sync cycle. Returns stats dict.

    cfg keys: bucket, prefix, data_tables, exclude(list), meta_tables(list),
              gcloud_path, gcp_project, staging_dir, db_path, state_path.
    `uploader(local, dest)` defaults to a real gcloud uploader.
    `log(level, msg)` where level in {debug,info,warning,error}.
    `should_continue()` lets the caller abort mid-cycle (e.g. job left READY).
    """
    if uploader is None:
        uploader = make_gcloud_uploader(cfg["gcloud_path"], cfg.get("gcp_project", ""))

    bucket = cfg["bucket"]
    prefix = cfg.get("prefix", "")
    staging_dir = cfg.get("staging_dir", tempfile.gettempdir())
    state_path = cfg["state_path"]
    db_path = cfg["db_path"]

    os.makedirs(staging_dir, exist_ok=True)
    stats = {"rows_uploaded": 0, "files_uploaded": 0, "errors": []}

    conn = _ro_connect(db_path)
    con = attach_duckdb(db_path)
    try:
        state = load_state(state_path)
        data_tables = resolve_data_tables(conn, cfg.get("data_tables", "auto"), cfg.get("exclude", []))
        log("info", f"data tables: {len(data_tables)} -> {', '.join(data_tables)}")

        # --- per-experiment incremental data tables ---
        for table in data_tables:
            if not should_continue():
                log("info", "aborting sync (job no longer ready)")
                break
            if not table_has_rowid(conn, table):
                log("warning", f"{table}: WITHOUT ROWID — skipping (incremental needs rowid)")
                continue
            tstate = state.setdefault(table, {})
            for exp in experiments_in_table(conn, table):
                if not should_continue():
                    break
                try:
                    cur_max, cur_cnt = partition_bounds(conn, table, exp)
                    saved = tstate.get(exp)
                    lo = 0
                    if saved:
                        saved_rowid = int(saved.get("rowid", 0))
                        saved_cnt = int(saved.get("count", 0))
                        if cur_max < saved_rowid or count_up_to(conn, table, exp, saved_rowid) != saved_cnt:
                            log("warning", f"{table}/{exp}: watermark mismatch (shrink/VACUUM?) — re-backfilling")
                            lo = 0
                        else:
                            lo = saved_rowid
                    if cur_max <= lo:
                        continue  # nothing new
                    slug = slugify_experiment(exp)
                    fname = f"part-{lo + 1:012d}-{cur_max:012d}.parquet"
                    local = os.path.join(staging_dir, f"{table}__{slug}__{fname}")
                    # Partition key is `experiment_slug` (NOT `experiment`): a Hive
                    # partition key must not duplicate an in-data column name, or
                    # polars shadows the real name and BigQuery rejects the table.
                    # The authoritative experiment name stays in the data (SELECT *).
                    dest = gcs_join(bucket, prefix, table, f"experiment_slug={slug}", fname)
                    export_window_parquet(con, table, exp, lo, cur_max, local)
                    uploader(local, dest)
                    os.remove(local)
                    tstate[exp] = {"rowid": cur_max, "count": cur_cnt}
                    save_state(state_path, state)  # persist progress incrementally
                    stats["rows_uploaded"] += (cur_cnt if lo == 0 else cur_cnt - count_up_to(conn, table, exp, lo))
                    stats["files_uploaded"] += 1
                    log("debug", f"uploaded {dest}")
                except Exception as e:  # noqa: BLE001 — one partition failing must not kill the cycle
                    msg = f"{table}/{exp}: {e}"
                    stats["errors"].append(msg)
                    log("error", msg)

        # --- whole metadata tables (full overwrite each cycle) ---
        for table in cfg.get("meta_tables", []):
            if not should_continue():
                break
            if not table_exists(conn, table):
                continue
            try:
                local = os.path.join(staging_dir, f"_meta__{table}.parquet")
                dest = gcs_join(bucket, prefix, "_meta", f"{table}.parquet")
                export_full_parquet(con, table, local)
                uploader(local, dest)
                os.remove(local)
                stats["files_uploaded"] += 1
                log("debug", f"uploaded {dest}")
            except Exception as e:  # noqa: BLE001
                msg = f"_meta/{table}: {e}"
                stats["errors"].append(msg)
                log("error", msg)

        return stats
    finally:
        con.close()
        conn.close()
