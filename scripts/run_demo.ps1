# End-to-end demo: two incremental batches (second one has an additive schema change), then a no-op rerun.
. "$PSScriptRoot\common.ps1"

Write-Host "== Batch 1: source data visible up to day 90 =="
Invoke-Platform @("generate", "--users", "500", "--days", "120", "--until-day", "90")
Invoke-Platform @("run")

Write-Host "== Batch 2: remaining days + late events + new events.device column =="
Invoke-Platform @("generate", "--users", "500", "--days", "120", "--until-day", "120", "--add-device-column")
Invoke-Platform @("run")

Write-Host "== Rerun with no new data (idempotency: ingested rows must all be 0) =="
Invoke-Platform @("run")

Write-Host "== Analytics (DuckDB over Gold Parquet) =="
Invoke-Platform @("analytics", "--show", "6")
Write-Host "Quality report: data\quality\latest.json   CSV outputs: data\outputs\analytics\"
