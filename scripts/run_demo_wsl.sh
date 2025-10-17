#!/usr/bin/env bash
set -euo pipefail

PKG_PREFIX="/opt/python3.13-ff"
BIN_DIR="$PKG_PREFIX/bin"
TIMEOUT_SECS=${1:-300}

echo "=== run_demo_wsl.sh: run free_threading demo (timeout=${TIMEOUT_SECS}s) ==="

if [ ! -d "$BIN_DIR" ]; then
  echo "$BIN_DIR not found; build interpreter first (scripts/ci_build_in_container.sh)"
  exit 1
fi

# discover python binary
PY_BIN=""
for p in "$BIN_DIR"/python*; do
  if [ -x "$p" ] && [[ "$(basename $p)" =~ ^python ]]; then
    PY_BIN="$p"
    break
  fi
done

if [ -z "$PY_BIN" ]; then
  echo "No python binary found under $BIN_DIR"
  exit 1
fi

echo "Using python: $PY_BIN"

echo "--- Attempting to install runtime deps (if requirements.txt exists) ---"
if [ -f requirements.txt ]; then
  "$PY_BIN" -m pip install --disable-pip-version-check -q -r requirements.txt || true
else
  echo "No requirements.txt found; skipping pip install"
fi

echo "--- Running demo with -X gil=0 -X jit=1 (jit may be ignored) ---"
echo "Demo output will be saved to /tmp/free_threading_demo.log"

# Use timeout to avoid hangs
timeout ${TIMEOUT_SECS}s "$PY_BIN" -X gil=0 -X jit=1 examples/free_threading_demo.py > /tmp/free_threading_demo.log 2>&1 || {
  echo "Demo exited with non-zero status or timed out; see /tmp/free_threading_demo.log"
}

echo "--- Tail of demo log ---"
tail -n 200 /tmp/free_threading_demo.log || true

echo "=== run_demo_wsl.sh: finished ==="
