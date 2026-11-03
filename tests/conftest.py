"""Shared fixtures. Integration tests use a throw-away PostgreSQL database created per test session."""

from __future__ import annotations

import os
import uuid
from dataclasses import replace

import psycopg2
import pytest

from saas_platform import loader
from saas_platform.config import Settings
from saas_platform.db import connect


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: needs a PostgreSQL server")


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    return Settings.from_env()


@pytest.fixture(scope="session")
def pg_database(base_settings):
    """Create an isolated database; skip (or fail, if REQUIRE_POSTGRES=1) when no server is reachable."""
    name = f"saas_test_{uuid.uuid4().hex[:8]}"
    try:
        admin = connect(base_settings, dbname="postgres")
    except psycopg2.OperationalError as exc:
        if os.environ.get("REQUIRE_POSTGRES") == "1":
            pytest.fail(f"PostgreSQL required but unreachable: {exc}")
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE DATABASE {name}")
    yield name
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    admin.close()


@pytest.fixture()
def settings(base_settings, pg_database, tmp_path):
    return replace(base_settings, pg_database=pg_database, data_dir=tmp_path / "data", batch_size=1000)


@pytest.fixture()
def conn(settings):
    c = connect(settings)
    loader.reset_db(c)
    yield c
    c.close()
