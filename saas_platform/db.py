"""PostgreSQL connection helpers."""

from __future__ import annotations

import time

import psycopg2

from saas_platform.config import Settings


def connect(settings: Settings, dbname: str | None = None):
    return psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=dbname or settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
        options="-c timezone=UTC",
        connect_timeout=5,
    )


def wait_for_db(settings: Settings, timeout_s: int = 60):
    deadline = time.time() + timeout_s
    last: Exception | None = None
    while time.time() < deadline:
        try:
            return connect(settings)
        except psycopg2.OperationalError as exc:  # server not ready yet
            last = exc
            time.sleep(2)
    raise RuntimeError(f"PostgreSQL not reachable after {timeout_s}s: {last}")
