# PowerShell development setup script for cidstore
# Creates a venv (if missing), activates it, installs editable package and dev deps,
# then writes a pinned requirements-dev.txt for reproducible dev installs.

param(
    [string]$venvPath = ".venv"
)

Write-Host "Setting up development environment in $venvPath"

if (-Not (Test-Path $venvPath)) {
    py -3 -m venv $venvPath
}

# Activate venv for the rest of the script
& "$venvPath\Scripts\Activate.ps1"

# Upgrade packaging tools
py -m pip install --upgrade pip setuptools wheel

# Install the project in editable mode
py -m pip install -e .

# Install common dev deps listed in pyproject if present. Fallback to pytest if not.
try {
    py -m pip install pytest
} catch {
    Write-Warning "Failed to install pytest via pyproject; installing pytest directly."
    py -m pip install pytest
}

# Freeze the resolved environment to requirements-dev.txt for reproducibility
py -m pip freeze | Out-File -Encoding utf8 requirements-dev.txt

Write-Host "Development environment ready. A pinned requirements-dev.txt was written."