# Operational runbook

| Symptom | Likely cause | Action |
|---|---|---|
| Airflow `quality_gate` task failed | an error-severity check failed | open `data/quality/quality_report_<run_id>.json` → `failed_error_checks`; fix cause; clear the task (safe: idempotent) |
| `freshness_lag[...]` fails | source got rows after ingestion finished | rerun the DAG / `python -m saas_platform run` |
| `reconcile_bronze_to_silver` fails | Silver stale or partially written | rerun `silver`; if persistent, inspect `silver/quarantine/*.parquet` |
| `rejected_rate` fails | upstream sent many invalid rows | group quarantine by `reject_reason`; talk to producer; raise `QUARANTINE_MAX_RATE` only deliberately |
| `SchemaDriftError` | required source column removed/renamed | cursor was NOT advanced; fix source or update contract in `schema.py`, then rerun |
| ingestion task crashed mid-run | any | just retry; batch ids are deterministic so files are overwritten, cursor only moves after a durable write |

**Backfill / reprocess**: delete `data/silver`, `data/gold` and run (Silver/Gold are rebuilt from Bronze). To re-ingest from source: `TRUNCATE etl.watermarks`, delete `data/bronze` (or keep it: duplicate versions are handled by Silver, flagged by the `duplicate_processing` warning).
**Clean up**: `scripts\clean.ps1` (data layers), `-Full` (also drop schemas), `-Down -Volumes` (also delete Docker volumes).
