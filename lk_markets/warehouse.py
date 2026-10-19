"""Save raw API responses into the DuckDB warehouse (schema `raw`).

Each row is one API record kept exactly as received (JSON). dbt parses it later,
so a parsing mistake never loses data: fix the SQL and rebuild.
"""
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse.duckdb"


def save_raw(table: str, records: list[tuple[str, object]], db_path: Path = DB_PATH) -> int:
    """Append (key, payload) records to raw.<table>, stamped with the fetch time in UTC."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.now(UTC).replace(tzinfo=None)
    with duckdb.connect(str(db_path)) as con:
        con.execute("create schema if not exists raw")
        con.execute(f"create table if not exists raw.{table} (key varchar, fetched_at_utc timestamp, payload json)")
        if records:
            con.executemany(f"insert into raw.{table} values (?, ?, ?)",
                            [(key, fetched_at, json.dumps(payload)) for key, payload in records])
    return len(records)


def query(sql: str, db_path: Path = DB_PATH) -> list[tuple]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(sql).fetchall()
