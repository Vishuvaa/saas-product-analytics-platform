# Safe cleanup. Default: delete generated local data layers only. -Full also drops DB schemas.
# -Down stops containers; -Volumes additionally deletes the PostgreSQL + Airflow volumes.
param([switch]$Full, [switch]$Down, [switch]$Volumes)
. "$PSScriptRoot\common.ps1"
if ($Full) { Invoke-Platform @("reset", "--yes") }
else {
    foreach ($d in "bronze", "silver", "gold", "quality", "outputs") {
        $p = Join-Path $Root "data\$d"
        if (Test-Path $p) { Remove-Item -Recurse -Force $p; Write-Host "removed $p" }
    }
}
if ($Down -and $Volumes) { Invoke-Checked "docker" @("compose", "down", "-v") }
elseif ($Down) { Invoke-Checked "docker" @("compose", "down") }
