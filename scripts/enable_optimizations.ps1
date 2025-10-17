# Enable Python 3.13 optimizations for CIDStore
# Run this script to set environment variables for your session:
#   . .\scripts\enable_optimizations.ps1
# Or add to your PowerShell profile for permanent configuration

Write-Host "Enabling Python 3.13 optimizations..." -ForegroundColor Green
Write-Host ""

# Check if Python supports free-threading
$supportsGIL = py -3.13 -c "import sys; print(hasattr(sys, '_is_gil_enabled'))" 2>$null

if ($supportsGIL -eq "True") {
    # Enable free-threading (GIL-free mode)
    $env:PYTHON_GIL = "0"
    Write-Host "✓ Free-threading enabled (PYTHON_GIL=0)" -ForegroundColor Green
} else {
    Write-Host "⚠ Free-threading NOT available" -ForegroundColor Yellow
    Write-Host "  Your Python build doesn't support --disable-gil" -ForegroundColor Gray
    Write-Host "  To enable: rebuild Python with ./configure --disable-gil" -ForegroundColor Gray
}

# Enable JIT compiler
$env:PYTHON_JIT = "1"
Write-Host "✓ JIT compiler enabled (PYTHON_JIT=1)" -ForegroundColor Green

# Enable auto-optimization for CIDStore
$env:CIDSTORE_AUTO_OPTIMIZE = "1"
Write-Host "✓ Auto-optimization enabled (CIDSTORE_AUTO_OPTIMIZE=1)" -ForegroundColor Green

Write-Host ""
Write-Host "Python 3.13 optimizations are now configured." -ForegroundColor Cyan
Write-Host ""

if ($supportsGIL -eq "True") {
    Write-Host "To make this permanent, add these lines to your PowerShell profile:" -ForegroundColor Yellow
    Write-Host '  $env:PYTHON_GIL = "0"' -ForegroundColor Gray
    Write-Host '  $env:PYTHON_JIT = "1"' -ForegroundColor Gray
    Write-Host '  $env:CIDSTORE_AUTO_OPTIMIZE = "1"' -ForegroundColor Gray
} else {
    Write-Host "To make JIT permanent, add these lines to your PowerShell profile:" -ForegroundColor Yellow
    Write-Host '  $env:PYTHON_JIT = "1"' -ForegroundColor Gray
    Write-Host '  $env:CIDSTORE_AUTO_OPTIMIZE = "1"' -ForegroundColor Gray
}

Write-Host ""
Write-Host "To verify configuration, run: python pyproject_config.py" -ForegroundColor Cyan
