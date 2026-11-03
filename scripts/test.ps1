# Lint + full test suite (integration tests need the PostgreSQL container from setup.ps1).
. "$PSScriptRoot\common.ps1"
$env:REQUIRE_POSTGRES = "1"   # fail (not skip) if PostgreSQL is unreachable
Invoke-Checked $Py @("-m", "ruff", "check", ".")
Invoke-Checked $Py @("-m", "pytest", "-v", "-rs")
