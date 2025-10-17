#!/usr/bin/env bash
set -euo pipefail

PKG_PREFIX="/opt/python3.13-ff"
PY_BIN="${PKG_PREFIX}/bin/python3"
OVERALL_TIMEOUT=${1:-3600}   # seconds (default 1 hour)
PER_TEST_TIMEOUT=${2:-120}   # seconds (default 2 minutes)

echo "=== run_tests_wsl.sh: running full pytest suite under $PY_BIN ==="

if [ ! -x "$PY_BIN" ]; then
  echo "$PY_BIN not found or not executable; build python first (scripts/ci_build_in_container.sh)"
  exit 1
fi

echo "Installing test dependencies into $PY_BIN (pytest, pytest-timeout, pytest-xdist, pytest-asyncio, anyio)"
"$PY_BIN" -m pip install --upgrade pip setuptools wheel >/dev/null
"$PY_BIN" -m pip install -q pytest pytest-timeout pytest-xdist pytest-asyncio anyio

LOGFILE="/tmp/cidstore_full_test_run.log"
echo "Logging test output to $LOGFILE"

echo "Running pytest (overall timeout=${OVERALL_TIMEOUT}s, per-test timeout=${PER_TEST_TIMEOUT}s)"

# Use timeout to cap the entire run, and pass per-test timeout via plugin
timeout ${OVERALL_TIMEOUT}s "$PY_BIN" -X gil=0 -m pytest -q --maxfail=1 --disable-warnings --timeout=${PER_TEST_TIMEOUT} 2>&1 | tee "$LOGFILE"

RC=${PIPESTATUS[0]:-0}
echo "pytest exited with code $RC"
if [ $RC -ne 0 ]; then
  echo "First lines of log (for quick triage):"
  head -n 200 "$LOGFILE" || true
  echo "Tail of log (last 200 lines):"
  tail -n 200 "$LOGFILE" || true
fi

echo "=== run_tests_wsl.sh: finished ==="
exit $RC
