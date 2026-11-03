# Linux/macOS equivalents of the PowerShell scripts.
PY=.venv/bin/python
setup:
	python3 -m venv .venv && $(PY) -m pip install -e ".[dev]" && cp -n .env.example .env || true
	docker compose up -d --wait postgres && $(PY) -m saas_platform init-db
demo:
	$(PY) -m saas_platform generate --users 500 --days 120 --until-day 90 && $(PY) -m saas_platform run
	$(PY) -m saas_platform generate --users 500 --days 120 --until-day 120 --add-device-column && $(PY) -m saas_platform run
	$(PY) -m saas_platform run && $(PY) -m saas_platform analytics --show 6
test:
	REQUIRE_POSTGRES=1 $(PY) -m ruff check . && REQUIRE_POSTGRES=1 $(PY) -m pytest -v -rs
benchmark:
	$(PY) scripts/benchmark.py
clean:
	$(PY) -c "from saas_platform.config import Settings; from saas_platform.pipeline import reset_local_data as r; print(r(Settings.from_env()))"
down:
	docker compose down
