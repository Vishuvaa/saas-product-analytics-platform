# Pre-publish checks + commit (+ optional push). Never force-pushes, never asks for credentials.
# Usage:  .\scripts\publish.ps1                         (checks + local commit)
#         .\scripts\publish.ps1 -RepoUrl https://github.com/<you>/saas-product-analytics-platform.git   (also pushes)
param([string]$RepoUrl = "")
. "$PSScriptRoot\common.ps1"

if (-not (Test-Path ".git")) { Invoke-Checked "git" @("init", "-b", "main") }

Write-Host "== Secret / artifact scan =="
$bad = git ls-files --others --cached --exclude-standard | Where-Object { $_ -match '(^|/)\.env$|\.parquet$|\.duckdb$|(^|/)data/|\.venv/' }
if ($bad) { throw "Refusing to continue; these files must not be committed:`n$($bad -join "`n")" }
$hits = git grep -n -I -E "(password|secret|token)\s*=\s*['""][^'""]{6,}" -- . ":!.env.example" ":!tests" ":!scripts" ":!docker-compose.yml" ":!.github" 2>$null
if ($hits) { Write-Warning "Possible hard-coded secrets, review manually:`n$($hits -join "`n")" }

Write-Host "== Lint + tests =="
& "$PSScriptRoot\test.ps1"

Invoke-Checked "git" @("add", "-A")
Write-Host "== Staged files =="; git status --short
$staged = git diff --cached --name-only
if ($staged) { Invoke-Checked "git" @("commit", "-m", "Add SaaS Product Analytics Data Platform (Postgres -> Bronze/Silver/Gold -> DuckDB, Airflow, tests, CI)") }

if ($RepoUrl) {
    $existing = git remote get-url origin 2>$null
    if ($existing -and $existing -ne $RepoUrl) { throw "Remote 'origin' already points to $existing - not overwriting. Resolve manually." }
    if (-not $existing) { Invoke-Checked "git" @("remote", "add", "origin", $RepoUrl) }
    Invoke-Checked "git" @("push", "-u", "origin", "main")   # plain push; will fail rather than overwrite
    Write-Host "Pushed. Verify at $RepoUrl"
} else {
    Write-Host "Committed locally. To publish: create an EMPTY repo on GitHub, then rerun with -RepoUrl <url>."
}
