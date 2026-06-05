# -*- coding: utf-8 -*-
"""
Core export logic for pioreactor-gcs-parquet-export.

Deliberately free of any `pioreactor` imports so it can be unit-tested off-reactor
with nothing but stdlib sqlite3 + duckdb. The BackgroundJob in
``gcs_parquet_export.py`` is a thin wrapper that wires config + a periodic timer +
MQTT status fields around ``sync_once``.

Data flow per cycle:
  SQLite (read-only, stdlib sqlite3)  ->  duckdb in-memory table  --COPY-->  local parquet  --uploader-->  GCS

Why this shape:
- We read with stdlib sqlite3 and write parquet with duckdb's *bundled core* writer.
  We do NOT use duckdb's `sqlite_scanner` extension: on the Pi leader (which duckdb
  reports as linux_i686) that extension isn't downloadable, but core parquet needs none.
- Incremental by a per-TABLE rowid watermark. Each cycle scans a table ONCE in rowid
  order (`WHERE rowid > watermark`, which rides the integer-PK index) and partitions
  the rows by experiment in Python. Filtering by `experiment` in SQL instead would do
  a full table scan per experiment per batch (no index on `experiment`) — pathologically
  slow on big tables. With this shape, an unchanged table costs a single O(1)
  `MAX(rowid)` check, and new rows cost one sequential scan of just the tail.

A self-heal guard re-backfills a table if its max rowid went backwards (e.g. VACUUM
renumbered rowids) — see "Known limitations" in the README.
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


def _quote_lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


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


def table_max_rowid(conn: sqlite3.Connection, table: str) -> int:
    """MAX(rowid) — O(1) on a rowid table. 0 if empty."""
    row = conn.execute(f"SELECT COALESCE(MAX(rowid), 0) FROM {_quote_ident(table)}").fetchone()
    return int(row[0])


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


def _json_default(o):
    # sqlite only ever hands us str/int/float/None/bytes; bytes (BLOB) aren't JSON-able.
    if isinstance(o, (bytes, bytearray)):
        return o.decode("utf-8", "replace")
    return str(o)


def _write_parquet(col_defs: list, rows: list, out_path: str) -> None:
    """col_defs: [(name, duckdb_type)]. Writes rows to a parquet file via duckdb core.

    We stage the rows as newline-delimited JSON and let duckdb's C++ reader load them,
    rather than `executemany` (which is ~200x slower — row-by-row INSERT is a duckdb
    anti-pattern). JSON (vs CSV) preserves NULL-vs-"" and escapes arbitrary text losslessly.
    """
    con = duckdb.connect()  # in-memory; parquet + json readers are bundled core
    try:
        if not rows:
            ddl = ", ".join(f"{_quote_ident(n)} {t}" for n, t in col_defs)
            con.execute(f"CREATE TABLE s ({ddl})")
            con.execute(f"COPY s TO {_quote_lit(out_path)} (FORMAT PARQUET)")
            return
        names = [n for n, _ in col_defs]
        jsonl = out_path + ".jsonl"
        try:
            with open(jsonl, "w") as f:
                for r in rows:
                    f.write(json.dumps(dict(zip(names, r)), default=_json_default))
                    f.write("\n")
            colspec = "{" + ", ".join(f"{_quote_lit(n)}: {_quote_lit(t)}" for n, t in col_defs) + "}"
            con.execute(
                f"COPY (SELECT * FROM read_json({_quote_lit(jsonl)}, "
                f"format='newline_delimited', columns={colspec})) "
                f"TO {_quote_lit(out_path)} (FORMAT PARQUET)"
            )
        finally:
            try:
                os.remove(jsonl)
            except OSError:
                pass
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


def _export_table(conn, con_cfg, table, state, uploader, log, should_continue, stats) -> None:
    """Incrementally export one data table: single rowid-ordered scan, partition by
    experiment in Python, one part file per (experiment, batch)."""
    bucket, prefix, staging_dir, state_path, batch_rows = con_cfg

    cur_max = table_max_rowid(conn, table)
    saved_rowid = int(state.get(table, {}).get("rowid", 0))
    if cur_max < saved_rowid:
        log("warning", f"{table}: max rowid went backwards ({cur_max} < {saved_rowid}) "
                        "(shrink/VACUUM?) — re-backfilling")
        saved_rowid = 0
    if cur_max <= saved_rowid:
        return  # nothing new — O(1) for unchanged tables

    cols = _columns(conn, table)
    col_defs = [(n, _sqlite_decl_to_duckdb(t)) for n, t in cols]
    col_list = ", ".join(_quote_ident(n) for n, _ in cols)
    names = [n for n, _ in cols]
    exp_idx = names.index("experiment")

    cursor = saved_rowid
    while cursor < cur_max:
        if not should_continue():
            break
        raw = conn.execute(
            f"SELECT rowid, {col_list} FROM {_quote_ident(table)} "
            "WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
            (cursor, cur_max, batch_rows),
        ).fetchall()
        if not raw:
            break
        batch_lo, batch_hi = cursor, raw[-1][0]

        # group this batch's rows by experiment (data columns = row[1:], minus the rowid)
        groups: dict = {}
        for r in raw:
            exp = r[1 + exp_idx]
            if exp is None:
                continue
            groups.setdefault(exp, []).append(r[1:])

        for exp, rows in groups.items():
            slug = slugify_experiment(exp)
            # Partition key is `experiment_slug` (NOT `experiment`): a Hive partition key
            # must not duplicate an in-data column, or polars shadows the real name and
            # BigQuery rejects the table. The real name stays in the data (we SELECT *).
            fname = f"part-{batch_lo + 1:012d}-{batch_hi:012d}.parquet"
            local = os.path.join(staging_dir, f"{table}__{slug}__{fname}")
            dest = gcs_join(bucket, prefix, table, f"experiment_slug={slug}", fname)
            _write_parquet(col_defs, rows, local)
            uploader(local, dest)
            os.remove(local)
            stats["rows_uploaded"] += len(rows)
            stats["files_uploaded"] += 1
            log("debug", f"uploaded {dest} ({len(rows)} rows)")

        cursor = batch_hi
        state[table] = {"rowid": cursor}
        save_state(state_path, state)  # persist progress per batch


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
    con_cfg = (bucket, prefix, staging_dir, state_path, batch_rows)

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
            try:
                _export_table(conn, con_cfg, table, state, uploader, log, should_continue, stats)
            except Exception as e:  # noqa: BLE001 — one table failing must not kill the cycle
                msg = f"{table}: {e}"
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
