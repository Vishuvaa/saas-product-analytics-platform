# Starts Airflow (UI http://localhost:8080, user admin / password admin - LOCAL ONLY). Unverified in authoring sandbox.
. "$PSScriptRoot\common.ps1"
Invoke-Checked "docker" @("compose", "up", "-d", "--build")
Write-Host "Open http://localhost:8080 , unpause DAG 'saas_product_analytics' and trigger it."
Write-Host "Load source data first: .\scripts\run_demo.ps1 (or: python -m saas_platform generate)."
