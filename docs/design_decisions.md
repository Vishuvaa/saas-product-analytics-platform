# Design decisions and trade-offs

1. **Keyset cursor `(updated_at, pk)` instead of timestamp-only or CDC.** Handles ties and is trivial to reason about. Cost: misses rows committed late with an older timestamp; documented. Alternative: sequence/CDC (Debezium), or a lookback window with dedupe.
2. **Bronze stores the whole row as a JSON payload.** Tolerates additive schema change with no code change and is fully replayable. Cost: parsing in Silver. Alternative: typed Bronze + schema evolution handling.
3. **Silver/Gold are full deterministic rebuilds.** Idempotent by construction and late data is trivially correct. Cost: O(total history) per run. Upgrade path: partitioned incremental Silver.
4. **Quarantine instead of drop or fail.** Bad rows are preserved with a reason and counted; the pipeline only fails when the *rate* exceeds a threshold or an invariant breaks.
5. **MRR from the subscription lifecycle, not payments.** Avoids double counting renewals/refunds/upgrades; payments are shown as cash alongside.
6. **Parquet + DuckDB.** Columnar files are cheap, portable and cloud-ready (S3); DuckDB gives full SQL on them with zero servers. Not a warehouse: no concurrent writers, no governance.
7. **pandas for Silver, DuckDB SQL for Gold.** Row-level validation reads naturally in pandas; set-based modeling reads naturally in SQL. At larger scale Silver would also move to SQL/Spark.
8. **Airflow DAG is a thin wrapper.** All logic is in importable, tested functions; the DAG only defines order, retries and the schedule.
9. **Not included (by design for an MVP):** Spark, Kafka, dbt, Kubernetes, Terraform.
