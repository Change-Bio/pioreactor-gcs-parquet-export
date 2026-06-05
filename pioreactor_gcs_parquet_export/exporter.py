# -*- coding: utf-8 -*-
"""
Core export logic for pioreactor-gcs-parquet-export.

Deliberately free of any `pioreactor` imports so it can be unit-tested off-reactor
with nothing but stdlib sqlite3 + duckdb. The BackgroundJob in
``gcs_parquet_export.py`` is a thin wrapper that wires config + a periodic timer +
MQTT status fields around ``sync_once``.

Data flow per cycle:
  SQLite (read-only, stdlib sqlite3)  ->  duckdb in-memory table  --COPY-->  local parquet  --uploader-->  GCS

We read rows with stdlib sqlite3 and write parquet with duckdb's *bundled core*
writer. We deliberately do NOT use duckdb's `sqlite_scanner` extension: on the
Raspberry Pi leader (reported by duckdb as linux_i686) that extension isn't
downloadable, but the core parquet writer needs no extension.

Incremental by rowid per (table, experiment): each cycle exports only rows with
rowid in (saved_watermark, current_max] as immutable part files, sub-batched to
bound memory. A self-heal guard re-backfills if the table appears to have
shrunk/renumbered (e.g. VACUUM).
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from typing import Callable, Iterable, Optional

import duckdb

DEFAULT_BATCH_ROWS = 200_000

_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")


# ----------------------------------------------------------------------------- helpers


def slugify_experiment(name: str) -> str:
    """Path-safe segment for an experiment name. The authoritative name is also a
    column inside every parquet, so this is purely organisational."""
    slug = _SLUG_RE.sub("_", name.strip())
    return slug.strip("_") or "unnamed"


def _ro_connect(db_path: str) -> sqlite3.Connection:
    """Read-only sqlite connection (safe against the live WAL DB)."""
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def gcs_join(*parts: str) -> str:
    """Join GCS path parts, collapsing empty segments and stray slashes."""
    cleaned = []
    for i, p in enumerate(parts):
        if not p or not p.strip():
            continue
        cleaned.append(p if i == 0 else p.strip("/"))
    return "/".join(s.rstrip("/") if i == 0 else s for i, s in enumerate(cleaned))


# ----------------------------------------------------------------------------- introspection


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _columns(conn: sqlite3.Connection, table: str) -> list:
    """[(name, declared_type)] in table order."""
    return [(c[1], c[2]) for c in conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()]


def table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(name == column for name, _ in _columns(conn, table))


def table_has_rowid(conn: sqlite3.Connection, table: str) -> bool:
    """WITHOUT ROWID tables raise when selecting rowid; detect that defensively."""
    try:
        conn.execute(f"SELECT rowid FROM {_quote_ident(table)} LIMIT 1").fetchone()
        return True
    except sqlite3.OperationalError:
        return False


def discover_experiment_tables(conn: sqlite3.Connection) -> list:
    names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]
    return [t for t in names if table_has_column(conn, t, "experiment")]


def resolve_data_tables(conn: sqlite3.Connection, data_tables: str, exclude: Iterable[str]) -> list:
    exclude = set(exclude)
    if data_tables.strip().lower() == "auto":
        candidates = discover_experiment_tables(conn)
    else:
        candidates = [t.strip() for t in data_tables.split(",") if t.strip()]
    return [t for t in candidates if t not in exclude and table_exists(conn, t)]


def experiments_in_table(conn: sqlite3.Connection, table: str) -> list:
    return [
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT experiment FROM {_quote_ident(table)} "
            "WHERE experiment IS NOT NULL ORDER BY experiment"
        ).fetchall()
    ]


def partition_bounds(conn: sqlite3.Connection, table: str, experiment: str) -> tuple:
    """(max_rowid, count) for one experiment partition. max_rowid is 0 if empty."""
    row = conn.execute(
        f"SELECT COALESCE(MAX(rowid), 0), COUNT(*) FROM {_quote_ident(table)} WHERE experiment = ?",
        (experiment,),
    ).fetchone()
    return int(row[0]), int(row[1])


def count_up_to(conn: sqlite3.Connection, table: str, experiment: str, rowid: int) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM {_quote_ident(table)} WHERE experiment = ? AND rowid <= ?",
        (experiment, rowid),
    ).fetchone()
    return int(row[0])


# ----------------------------------------------------------------------------- state (watermarks)


def load_state(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)  # atomic


# ----------------------------------------------------------------------------- parquet writing


def _sqlite_decl_to_duckdb(decl: str) -> str:
    """Map a SQLite declared type to a duckdb column type (SQLite affinity rules)."""
    d = (decl or "").upper()
    if "INT" in d:
        return "BIGINT"
    if "CHAR" in d or "CLOB" in d or "TEXT" in d:
        return "VARCHAR"
    if "BLOB" in d:
        return "BLOB"
    if "REAL" in d or "FLOA" in d or "DOUB" in d:
        return "DOUBLE"
    return "VARCHAR"  # NUMERIC/unknown -> safe default


def _write_parquet(col_defs: list, rows: list, out_path: str) -> None:
    """col_defs: [(name, duckdb_type)]. Writes rows to a parquet file via duckdb core."""
    con = duckdb.connect()  # in-memory; no extensions needed for parquet COPY
    try:
        ddl = ", ".join(f"{_quote_ident(n)} {t}" for n, t in col_defs)
        con.execute(f"CREATE TABLE s ({ddl})")
        if rows:
            placeholders = ", ".join(["?"] * len(col_defs))
            con.executemany(f"INSERT INTO s VALUES ({placeholders})", rows)
        con.execute(f"COPY s TO '{out_path}' (FORMAT PARQUET)")
    finally:
        con.close()


def export_full_parquet(conn: sqlite3.Connection, table: str, out_path: str) -> int:
    """Write a whole table to one parquet file (used for _meta tables). Returns row count."""
    cols = _columns(conn, table)
    col_list = ", ".join(_quote_ident(n) for n, _ in cols)
    rows = conn.execute(f"SELECT {col_list} FROM {_quote_ident(table)}").fetchall()
    col_defs = [(n, _sqlite_decl_to_duckdb(t)) for n, t in cols]
    _write_parquet(col_defs, rows, out_path)
    return len(rows)


def _fetch_window(conn: sqlite3.Connection, table: str, experiment: str, lo: int, hi: int, limit: int):
    """Return (data_rows, batch_hi) for rowids in (lo, hi], up to `limit` rows, ordered.
    data_rows excludes the leading rowid; batch_hi is the max rowid in the batch (or None)."""
    cols = _columns(conn, table)
    col_list = ", ".join(_quote_ident(n) for n, _ in cols)
    raw = conn.execute(
        f"SELECT rowid, {col_list} FROM {_quote_ident(table)} "
        "WHERE experiment = ? AND rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
        (experiment, lo, hi, limit),
    ).fetchall()
    if not raw:
        return [], None, cols
    data = [r[1:] for r in raw]
    batch_hi = raw[-1][0]
    return data, batch_hi, cols


# ----------------------------------------------------------------------------- upload


def make_gcloud_uploader(gcloud_path: str, project: str) -> Callable[[str, str], None]:
    """uploader(local_path, gcs_dest) that shells out to `gcloud storage cp`, reusing
    the service-account auth already configured on the leader."""
    import subprocess

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
    """Run one full sync cycle. Returns {rows_uploaded, files_uploaded, errors}.

    cfg keys: bucket, prefix, data_tables, exclude(list), meta_tables(list),
              gcloud_path, gcp_project, staging_dir, db_path, state_path, batch_rows(opt).
    """
    if uploader is None:
        uploader = make_gcloud_uploader(cfg["gcloud_path"], cfg.get("gcp_project", ""))

    bucket = cfg["bucket"]
    if not bucket:
        raise ValueError("config 'bucket' is empty — set [gcs_parquet_export.config] bucket")
    prefix = cfg.get("prefix", "")
    staging_dir = cfg.get("staging_dir", "/tmp/pioreactor_gcs_export")
    state_path = cfg["state_path"]
    db_path = cfg["db_path"]
    batch_rows = int(cfg.get("batch_rows") or DEFAULT_BATCH_ROWS)

    os.makedirs(staging_dir, exist_ok=True)
    stats = {"rows_uploaded": 0, "files_uploaded": 0, "errors": []}

    conn = _ro_connect(db_path)
    try:
        state = load_state(state_path)
        data_tables = resolve_data_tables(conn, cfg.get("data_tables", "auto"), cfg.get("exclude", []))
        log("info", f"data tables: {len(data_tables)} -> {', '.join(data_tables)}")

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
                    running = count_up_to(conn, table, exp, lo)
                    cursor = lo
                    while cursor < cur_max:
                        if not should_continue():
                            break
                        data, batch_hi, cols = _fetch_window(conn, table, exp, cursor, cur_max, batch_rows)
                        if not data:
                            break
                        # Partition key is `experiment_slug` (NOT `experiment`): a Hive
                        # partition key must not duplicate an in-data column name, or
                        # polars shadows the real name and BigQuery rejects the table.
                        fname = f"part-{cursor + 1:012d}-{batch_hi:012d}.parquet"
                        local = os.path.join(staging_dir, f"{table}__{slug}__{fname}")
                        dest = gcs_join(bucket, prefix, table, f"experiment_slug={slug}", fname)
                        col_defs = [(n, _sqlite_decl_to_duckdb(t)) for n, t in cols]
                        _write_parquet(col_defs, data, local)
                        uploader(local, dest)
                        os.remove(local)
                        running += len(data)
                        tstate[exp] = {"rowid": batch_hi, "count": running}
                        save_state(state_path, state)  # persist progress per part
                        stats["rows_uploaded"] += len(data)
                        stats["files_uploaded"] += 1
                        log("debug", f"uploaded {dest} ({len(data)} rows)")
                        cursor = batch_hi
                except Exception as e:  # noqa: BLE001 — one partition failing must not kill the cycle
                    msg = f"{table}/{exp}: {e}"
                    stats["errors"].append(msg)
                    log("error", msg)

        # whole metadata tables (full overwrite each cycle)
        for table in cfg.get("meta_tables", []):
            if not should_continue():
                break
            if not table_exists(conn, table):
                continue
            try:
                local = os.path.join(staging_dir, f"_meta__{table}.parquet")
                dest = gcs_join(bucket, prefix, "_meta", f"{table}.parquet")
                n = export_full_parquet(conn, table, local)
                uploader(local, dest)
                os.remove(local)
                stats["files_uploaded"] += 1
                log("debug", f"uploaded {dest} ({n} rows)")
            except Exception as e:  # noqa: BLE001
                msg = f"_meta/{table}: {e}"
                stats["errors"].append(msg)
                log("error", msg)

        return stats
    finally:
        conn.close()
