# One-time setup: virtualenv, dependencies, .env, PostgreSQL container, schemas.
. "$PSScriptRoot\common.ps1"

if (-not (Test-Path $Py)) {
    Invoke-Checked "python" @("-m", "venv", ".venv")
}
Invoke-Checked $Py @("-m", "pip", "install", "--upgrade", "pip")
Invoke-Checked $Py @("-m", "pip", "install", "-e", ".[dev]")

if (-not (Test-Path ".env")) {
    $pw = -join ((48..57) + (97..122) | Get-Random -Count 24 | ForEach-Object { [char]$_ })
    (Get-Content ".env.example") -replace "change_me_local_only", $pw | Set-Content ".env" -Encoding ascii
    Write-Host "Created .env with a random local database password (never commit .env)."
}

Invoke-Checked "docker" @("compose", "up", "-d", "--wait", "postgres")
Invoke-Platform @("init-db")
Write-Host "Setup complete. Next: .\scripts\run_demo.ps1"
