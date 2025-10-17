#!/usr/bin/env bash
set -euo pipefail
# Helper to run inside an Ubuntu container to install build deps and invoke
# the repository's ci_build_python.sh. Keeps complex steps inside /scripts.

PY_VERSION=${1:-3.13.7}
PREFIX=${2:-/opt/python3.13-ff}
NUMJOBS=${3:-$(nproc)}
CACHE_DIR=${4:-/root/.cache/python-src}

echo "Installing build dependencies..."
apt-get update
apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
  libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev libgdbm-dev \
  libnss3-dev libffi-dev liblzma-dev libxml2-dev libxmlsec1-dev ca-certificates \
  pkg-config libhdf5-dev

echo "Running ci_build_python.sh (will build CPython ${PY_VERSION})"
chmod +x ./scripts/ci_build_python.sh
./scripts/ci_build_python.sh "${PY_VERSION}" "${PREFIX}" "${NUMJOBS}" "${CACHE_DIR}"

echo "Verifying built interpreter:"
# detect python binary under $PREFIX/bin (python, python3.13, python3.13t)
if [ -x "${PREFIX}/bin/python" ]; then
  PY="${PREFIX}/bin/python"
elif [ -x "${PREFIX}/bin/python3.13" ]; then
  PY="${PREFIX}/bin/python3.13"
elif [ -x "${PREFIX}/bin/python3.13t" ]; then
  PY="${PREFIX}/bin/python3.13t"
else
  echo "No python binary found in ${PREFIX}/bin" >&2
  ls -l "${PREFIX}/bin" || true
  exit 1
fi

"${PY}" -V
"${PY}" -c "import sys; print('supports_free_threading=', hasattr(sys,'_is_gil_enabled'), 'supports_jit=', hasattr(sys,'_is_jit_enabled'))"

echo "Done. The built interpreter is installed at ${PREFIX}"
