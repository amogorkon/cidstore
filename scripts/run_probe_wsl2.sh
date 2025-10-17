#!/usr/bin/env bash
set -euo pipefail

echo "=== run_probe_wsl2.sh: probe CPython build & run tests (robust) ==="

PKG_PREFIX="/opt/python3.13-ff"
BIN_DIR="$PKG_PREFIX/bin"

echo "Listing $BIN_DIR"
if [ -d "$BIN_DIR" ]; then
  ls -la "$BIN_DIR"
else
  echo "$BIN_DIR does not exist"
fi

# Find first executable python binary in bin dir
PY_BIN=""
if [ -d "$BIN_DIR" ]; then
  for p in "$BIN_DIR"/python*; do
    if [ -x "$p" ] && [[ "$(basename $p)" =~ ^python ]]; then
      PY_BIN="$p"
      break
    fi
  done
fi

if [ -z "$PY_BIN" ]; then
  echo "No suitable python binary found under $BIN_DIR"
  exit 0
fi

echo "Using python binary: $PY_BIN"

echo "--- Basic probe ---"

"$PY_BIN" - <<'PY'
import sys
print('executable:', sys.executable)
print('version:', sys.version)
print('has _is_gil_enabled:', hasattr(sys, '_is_gil_enabled'))
print('has _is_jit_enabled:', hasattr(sys, '_is_jit_enabled'))
try:
  print('call _is_gil_enabled:', sys._is_gil_enabled())
except Exception as e:
  print('call _is_gil_enabled raised:', repr(e))
try:
  print('call _is_jit_enabled:', sys._is_jit_enabled())
except Exception as e:
  print('call _is_jit_enabled raised:', repr(e))
PY

echo
echo "--- Try -X jit=1 (may be a no-op) ---"

"$PY_BIN" -X jit=1 - <<'PY' 2>&1 || true
import sys
print('has _is_jit_enabled:', hasattr(sys, '_is_jit_enabled'))
try:
  print('runtime _is_jit_enabled:', sys._is_jit_enabled())
except Exception as e:
  print('calling _is_jit_enabled raised', type(e), e)
PY

echo
echo "--- Install pytest & pytest-timeout into built interpreter ---"
"$PY_BIN" -m pip install --disable-pip-version-check -q pytest pytest-timeout || true

echo
echo "--- Run tests/test_python313_features.py under -X gil=0 with timeout 120s ---"
"$PY_BIN" -X gil=0 -m pytest tests/test_python313_features.py -q --timeout=120 || true

echo
echo "=== run_probe_wsl2.sh: finished ==="
