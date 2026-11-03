"""Load generated rows into the PostgreSQL source schema (idempotent upserts)."""

from __future__ import annotations

from datetime import datetime

from psycopg2.extras import Json, execute_values

from saas_platform.generator import GeneratedData
from saas_platform.schema import LOAD_ORDER, SOURCE_TABLES, ddl


def init_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(ddl())
    conn.commit()


def reset_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS src CASCADE; DROP SCHEMA IF EXISTS etl CASCADE;")
    conn.commit()
    init_db(conn)


def table_columns(conn, table: str, schema: str = "src") -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s "
            "ORDER BY ordinal_position",
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]


def apply_additive_schema_change(conn) -> None:
    """Simulates an upstream additive change: product_events gains a `device` column."""
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE src.product_events ADD COLUMN IF NOT EXISTS device TEXT")
    conn.commit()


def insert_rows(conn, table: str, rows: list[dict], page_size: int = 5000) -> int:
    """Upsert rows. Existing rows are only overwritten when the incoming watermark is newer."""
    if not rows:
        return 0
    spec = SOURCE_TABLES[table]
    present = set(table_columns(conn, table))
    cols = [c for c in rows[0] if c in present]
    update_cols = [c for c in cols if c != spec.pk]
    sql = (
        f"INSERT INTO src.{table} AS t ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({spec.pk}) DO UPDATE SET "
        + ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
        + f" WHERE t.{spec.watermark} < EXCLUDED.{spec.watermark}"
    )
    values = [
        tuple(Json(r[c]) if c == "properties" and r[c] is not None else r[c] for c in cols) for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=page_size)
    conn.commit()
    return len(rows)


def load_as_of(conn, data: GeneratedData, until: datetime | None = None) -> dict[str, int]:
    visible = data.as_of(until)
    return {t: insert_rows(conn, t, visible[t]) for t in LOAD_ORDER}
