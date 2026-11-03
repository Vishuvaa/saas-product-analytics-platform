"""Configuration from environment variables (and an optional local .env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "saas"
    pg_user: str = "saas"
    pg_password: str = ""
    data_dir: Path = PROJECT_ROOT / "data"
    quarantine_max_rate: float = 0.05
    freshness_max_hours: float = 24.0
    batch_size: int = 50_000

    @classmethod
    def from_env(cls, **overrides) -> Settings:
        data_dir = Path(os.environ.get("DATA_DIR", "./data"))
        if not data_dir.is_absolute():
            data_dir = PROJECT_ROOT / data_dir
        s = cls(
            pg_host=os.environ.get("PGHOST", "localhost"),
            pg_port=int(os.environ.get("PGPORT", "5432")),
            pg_database=os.environ.get("PGDATABASE", "saas"),
            pg_user=os.environ.get("PGUSER", "saas"),
            pg_password=os.environ.get("PGPASSWORD", ""),
            data_dir=data_dir,
            quarantine_max_rate=float(os.environ.get("QUARANTINE_MAX_RATE", "0.05")),
            freshness_max_hours=float(os.environ.get("FRESHNESS_MAX_HOURS", "24")),
        )
        return replace(s, **overrides) if overrides else s

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quality_dir(self) -> Path:
        return self.data_dir / "quality"

    @property
    def outputs_dir(self) -> Path:
        return self.data_dir / "outputs"
