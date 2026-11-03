from datetime import datetime, timedelta, timezone

import pytest

from saas_platform import bronze, ingestion, loader
from saas_platform.generator import generate

pytestmark = pytest.mark.integration
T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def user(i, ts):
    return {
        "user_id": i,
        "email": f"u{i}@x.test",
        "signup_ts": ts,
        "country": "US",
        "segment": "smb",
        "acquisition_channel": None,
        "created_at": ts,
        "updated_at": ts,
    }


def bronze_pks(settings, table="users"):
    return sorted(bronze.read_table(settings.bronze_dir, table)["_source_pk"])


def test_incremental_then_rerun_ingests_nothing(conn, settings):
    loader.insert_rows(conn, "users", [user(i, T0 + timedelta(hours=i)) for i in range(1, 6)])
    assert ingestion.ingest_table(conn, settings, "users").rows == 5
    assert ingestion.ingest_table(conn, settings, "users").rows == 0  # idempotent rerun
    loader.insert_rows(conn, "users", [user(6, T0 + timedelta(hours=9))])
    assert ingestion.ingest_table(conn, settings, "users").rows == 1  # only the new row
    assert bronze_pks(settings) == [1, 2, 3, 4, 5, 6]


def test_rows_sharing_a_timestamp_are_not_lost_across_batches(conn, settings):
    same = T0 + timedelta(hours=1)
    loader.insert_rows(conn, "users", [user(i, same) for i in range(1, 8)])
    res = ingestion.ingest_table(conn, settings, "users", batch_size=2)  # cursor must split inside a tie
    assert res.batches == 4 and res.rows == 7
    assert bronze_pks(settings) == list(range(1, 8))


def test_updated_row_is_reingested_as_new_version(conn, settings):
    loader.insert_rows(conn, "users", [user(1, T0)])
    ingestion.ingest_table(conn, settings, "users")
    changed = dict(user(1, T0), country="IN", updated_at=T0 + timedelta(days=1))
    loader.insert_rows(conn, "users", [changed])
    assert ingestion.ingest_table(conn, settings, "users").rows == 1
    assert len(bronze.read_table(settings.bronze_dir, "users")) == 2  # both versions kept (immutable bronze)


def test_bronze_write_failure_does_not_advance_watermark(conn, settings, monkeypatch):
    loader.insert_rows(conn, "users", [user(1, T0), user(2, T0 + timedelta(hours=1))])

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(bronze, "write_batch", boom)
    with pytest.raises(OSError):
        ingestion.ingest_table(conn, settings, "users")
    conn.rollback()
    assert ingestion.get_watermark(conn, "users")[1] == -1
    monkeypatch.undo()
    assert ingestion.ingest_table(conn, settings, "users").rows == 2  # retry succeeds


def test_retry_after_crash_between_file_and_cursor_does_not_duplicate_files(conn, settings):
    loader.insert_rows(conn, "users", [user(1, T0)])
    ingestion.ingest_table(conn, settings, "users")
    with conn.cursor() as cur:  # simulate "file written but watermark commit lost"
        cur.execute("DELETE FROM etl.watermarks")
    conn.commit()
    ingestion.ingest_table(conn, settings, "users")
    assert len(list(settings.bronze_dir.glob("users/*/*.parquet"))) == 1


def test_additive_schema_change_is_tolerated_and_recorded(conn, settings):
    loader.insert_rows(conn, "product_events", [])
    loader.apply_additive_schema_change(conn)
    res = ingestion.ingest_table(conn, settings, "product_events")
    assert res.extra_columns == ["device"]


def test_missing_required_column_halts_ingestion_safely(conn, settings):
    loader.insert_rows(conn, "users", [user(1, T0)])
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE src.users DROP COLUMN country")
    conn.commit()
    with pytest.raises(ingestion.SchemaDriftError):
        ingestion.ingest_table(conn, settings, "users")
    assert ingestion.get_watermark(conn, "users")[1] == -1
    assert not list(settings.bronze_dir.glob("users/*/*.parquet"))


def test_generated_dataset_loads_and_ingests_fully(conn, settings):
    data = generate(3, 60, days=60)
    loader.load_as_of(conn, data)
    res = ingestion.ingest_all(conn, settings)
    assert {k: v.rows for k, v in res.items()} == {k: len(v) for k, v in data.as_of(None).items()}
