#!/usr/bin/env bash
set -euo pipefail

echo "=== run_probe_wsl.sh: probe CPython build & run tests (safe, contained) ==="

echo "--- Checking configure help for JIT support ---"
if [ -d /tmp/cpython-src/Python-3.13.7 ]; then
  cd /tmp/cpython-src/Python-3.13.7
  ./configure --help 2>/dev/null | sed -n '1,400p' | grep -i jit || echo "(no --disable-gil/--enable-jit configure flag found)"
else
  echo "Source dir not found: /tmp/cpython-src/Python-3.13.7"
fi

echo
echo "--- Probe built python for GIL/JIT attributes ---"
if [ -x /opt/python3.13-ff/bin/python ]; then
  /opt/python3.13-ff/bin/python - <<'PY'
import sys
print('python_executable:', sys.executable)
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
else
  echo "/opt/python3.13-ff/bin/python not found"
fi

echo
echo "--- Try running with -X jit=1 (may be a no-op or error) ---"
if [ -x /opt/python3.13-ff/bin/python ]; then
  /opt/python3.13-ff/bin/python -X jit=1 - <<'PY' 2>&1 || true
import sys
print('has _is_jit_enabled:', hasattr(sys, '_is_jit_enabled'))
try:
    print('runtime _is_jit_enabled:', sys._is_jit_enabled())
except Exception as e:
    print('calling _is_jit_enabled raised', type(e), e)
PY
else
  echo "/opt/python3.13-ff/bin/python not found"
fi

echo
echo "--- Install pytest-timeout & pytest into built interpreter (quiet) ---"
if [ -x /opt/python3.13-ff/bin/python ]; then
  /opt/python3.13-ff/bin/python -m pip install --disable-pip-version-check -q pytest-timeout pytest || true
else
  echo "Skipping pip install: built interpreter missing"
fi

echo
echo "--- Run tests/test_python313_features.py with -X gil=0 and --timeout=120s ---"
if [ -x /opt/python3.13-ff/bin/python ]; then
  # run under -X gil=0 to enable free-threading at runtime; pytest-timeout ensures we don't hang indefinitely
  /opt/python3.13-ff/bin/python -X gil=0 -m pytest tests/test_python313_features.py -q --timeout=120 || true
else
  echo "Skipping tests: built interpreter missing"
fi

echo
echo "=== run_probe_wsl.sh: finished ==="
