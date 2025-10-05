#!/usr/bin/env bash
# POSIX shell dev setup script for cidstore
# Creates a venv (if missing), activates it, installs editable package and dev deps,
# then writes a pinned requirements-dev.txt for reproducible dev installs.

VENV_PATH=".venv"

if [ ! -d "$VENV_PATH" ]; then
  python3 -m venv "$VENV_PATH"
fi

# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .
python -m pip install pytest || true
python -m pip freeze > requirements-dev.txt

echo "Development environment ready. A pinned requirements-dev.txt was written."