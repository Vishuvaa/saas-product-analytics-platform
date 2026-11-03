# Benchmark (2026-10-24)

Environment: Python 3.12.3, Linux 6.18.44-fc-v153, pandas 2.2.3, duckdb 1.1.3.
Single run per size, local PostgreSQL, no tuning, no warm-up. Treat as indicative only.

|   users |   source_rows |   events |   generate_s |   load_to_postgres_s |   ingest_s |   silver_s |   gold_s |   quality_s |   analytics_s |   pipeline_total_s |   rerun_total_s_no_new_data |   rows_per_s_pipeline | quality_checks_passed   |
|--------:|--------------:|---------:|-------------:|---------------------:|-----------:|-----------:|---------:|------------:|--------------:|-------------------:|----------------------------:|----------------------:|:------------------------|
|     500 |         12329 |    11257 |         0.12 |                 0.38 |      0.382 |      0.291 |    0.054 |       0.092 |         0.1   |              0.919 |                       0.519 |                 13416 | 77/77                   |
|    3000 |         73385 |    66929 |         0.88 |                 2.41 |      1.726 |      1.419 |    0.098 |       0.158 |         0.117 |              3.518 |                       2.001 |                 20860 | 77/77                   |
