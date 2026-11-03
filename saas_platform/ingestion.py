"""Incremental ingestion: PostgreSQL -> Bronze Parquet.

Strategy: keyset cursor on (watermark_column, primary_key), strictly greater than the stored cursor.
- Rows sharing a timestamp are safe because the pk breaks ties.
- The cursor only advances AFTER the Parquet file is durably written (at-least-once delivery).
- Batch ids are deterministic (hash of table + cursor range) so a retry overwrites the same file.
Limitation: a source transaction that commits late with a watermark <= the stored cursor is missed.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor

from saas_platform import bronze
from saas_platform.config import Settings
from saas_platform.loader import table_columns
from saas_platform.logging_utils import get_logger, kv
from saas_platform.schema import SOURCE_TABLES, TableSpec

log = get_logger("ingestion")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class SchemaDriftError(RuntimeError):
    """Source table is missing columns required by the contract (incompatible change)."""


@dataclass
class IngestResult:
    table: str
    rows: int = 0
    batches: int = 0
    extra_columns: list[str] = field(default_factory=list)


def check_schema(conn, spec: TableSpec) -> list[str]:
    """Return extra (additive) columns; raise on missing required columns."""
    actual = table_columns(conn, spec.name)
    missing = [c for c in spec.columns if c not in actual]
    if missing:
        raise SchemaDriftError(
            f"src.{spec.name} is missing required columns: {missing}. Ingestion halted; "
            "watermark NOT advanced."
        )
    return [c for c in actual if c not in spec.columns]


def get_watermark(conn, table: str) -> tuple[datetime, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT last_wm, last_pk FROM etl.watermarks WHERE table_name=%s", (table,))
        row = cur.fetchone()
    return (row[0], row[1]) if row and row[0] is not None else (EPOCH, -1)


def _batch_id(table: str, frm: tuple, to: tuple) -> str:
    return hashlib.sha1(
        f"{table}|{frm[0].isoformat()}|{frm[1]}|{to[0].isoformat()}|{to[1]}".encode()
    ).hexdigest()[:12]


def ingest_table(
    conn, settings: Settings, table: str, run_id: str | None = None, batch_size: int | None = None
) -> IngestResult:
    spec = SOURCE_TABLES[table]
    run_id = run_id or uuid.uuid4().hex[:8]
    batch_size = batch_size or settings.batch_size
    result = IngestResult(table)
    result.extra_columns = check_schema(conn, spec)
    if result.extra_columns:
        log.warning(kv(event="schema_additive_change", table=table, extra=",".join(result.extra_columns)))

    while True:
        wm, pk = get_watermark(conn, table)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM src.{table} WHERE ({spec.watermark}, {spec.pk}) > (%s, %s) "
                f"ORDER BY {spec.watermark}, {spec.pk} LIMIT %s",
                (wm, pk, batch_size),
            )
            rows = cur.fetchall()
        if not rows:
            break
        now = datetime.now(timezone.utc)
        last = rows[-1]
        to = (last[spec.watermark], last[spec.pk])
        bid = _batch_id(table, (wm, pk), to)
        frame = bronze.make_bronze_frame(table, spec.pk, spec.watermark, rows, bid, run_id, now)
        path = bronze.write_batch(frame, settings.bronze_dir, table, now, bid)  # 1) durable write
        with conn.cursor() as cur:  # 2) then advance cursor + audit, atomically
            cur.execute(
                "INSERT INTO etl.watermarks (table_name,last_wm,last_pk,last_run_at) VALUES (%s,%s,%s,now()) "
                "ON CONFLICT (table_name) DO UPDATE SET last_wm=EXCLUDED.last_wm, last_pk=EXCLUDED.last_pk, "
                "last_run_at=now()",
                (table, to[0], to[1]),
            )
            cur.execute(
                "INSERT INTO etl.ingest_batches (batch_id,table_name,run_id,row_count,from_wm,to_wm,file_path,"
                "extra_columns,finished_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now()) "
                "ON CONFLICT (batch_id) DO UPDATE SET run_id=EXCLUDED.run_id, row_count=EXCLUDED.row_count, "
                "file_path=EXCLUDED.file_path, extra_columns=EXCLUDED.extra_columns, finished_at=now()",
                (bid, table, run_id, len(rows), wm, to[0], str(path), ",".join(result.extra_columns)),
            )
        conn.commit()
        result.rows += len(rows)
        result.batches += 1
        log.info(kv(event="batch_ingested", table=table, batch=bid, rows=len(rows)))

    with conn.cursor() as cur:  # record that we checked, even when nothing was new (freshness)
        cur.execute(
            "INSERT INTO etl.watermarks (table_name,last_run_at) VALUES (%s,now()) "
            "ON CONFLICT (table_name) DO UPDATE SET last_run_at=now()",
            (table,),
        )
    conn.commit()
    log.info(kv(event="table_ingested", table=table, rows=result.rows, batches=result.batches))
    return result


def ingest_all(conn, settings: Settings, run_id: str | None = None) -> dict[str, IngestResult]:
    return {t: ingest_table(conn, settings, t, run_id) for t in SOURCE_TABLES}
