#!/usr/bin/env bash
set -euo pipefail
# Usage: run_tests_with_built_python.sh <install-prefix>
PREFIX=${1:-/opt/python3.13-ff}

echo "Detecting python in ${PREFIX}/bin"
if [ -x "${PREFIX}/bin/python" ]; then
  PY=${PREFIX}/bin/python
elif [ -x "${PREFIX}/bin/python3.13" ]; then
  PY=${PREFIX}/bin/python3.13
elif [ -x "${PREFIX}/bin/python3.13t" ]; then
  PY=${PREFIX}/bin/python3.13t
else
  echo "No python binary found in ${PREFIX}/bin" >&2
  ls -l "${PREFIX}/bin" || true
  exit 1
fi

echo "Using interpreter: ${PY}"
${PY} -V || true

echo "Build supports free-threading="
${PY} -c "import sys; print(hasattr(sys, '_is_gil_enabled'))"
echo "Build supports JIT="
${PY} -c "import sys; print(hasattr(sys, '_is_jit_enabled'))"

echo "Upgrading pip and installing test deps (if requirements.txt exists)"
${PY} -m pip install --upgrade pip setuptools wheel || true
echo "Installing project and test dependencies"
if [ -f pyproject.toml ]; then
  # Install package in editable mode with dev extras (which includes pytest)
  ${PY} -m pip install -e ".[dev]" || true
fi
# Some test code imports httpx and expects pytest-asyncio to be available for asyncio marks.
# Install these explicitly into the built interpreter so test collection succeeds.
${PY} -m pip install httpx pytest-asyncio || true
if [ -f requirements.txt ]; then
  ${PY} -m pip install -r requirements.txt || true
fi

echo "Running tests with free-threading + JIT flags (if supported)"
${PY} -X gil=0 -X jit=1 -m pytest tests/ -q
