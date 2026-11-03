# Shared helpers (dot-sourced by the other scripts).
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Py = Join-Path $Root ".venv\Scripts\python.exe"

function Invoke-Checked {
    param([Parameter(Mandatory)][string]$Exe, [string[]]$Arguments)
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE): $Exe $($Arguments -join ' ')" }
}
function Invoke-Platform { param([string[]]$Arguments) Invoke-Checked $Py (@("-m", "saas_platform") + $Arguments) }
