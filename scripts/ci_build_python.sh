#!/usr/bin/env bash
set -euo pipefail
# Simple script to build CPython from source for CI with optional free-threading
# Usage: ./scripts/ci_build_python.sh <version> <install-prefix>
# Example: ./scripts/ci_build_python.sh 3.13.7 /opt/python3.13-ff

PY_VERSION=${1:-3.13.7}
PREFIX=${2:-$PWD/.python-build}
NUMJOBS=${3:-$(nproc)}
# Optional cache directory for the downloaded Python tarball (used by CI caching)
CACHE_DIR=${4:-${HOME}/.cache/python-src}

mkdir -p "$CACHE_DIR"


echo "Building CPython ${PY_VERSION} -> ${PREFIX} (jobs=${NUMJOBS})"
mkdir -p /tmp/cpython-src
cd /tmp/cpython-src

TAR=Python-${PY_VERSION}.tgz
if [ -f "${CACHE_DIR}/${TAR}" ]; then
  echo "Using cached tarball: ${CACHE_DIR}/${TAR}"
  cp "${CACHE_DIR}/${TAR}" ./
else
  wget https://www.python.org/ftp/python/${PY_VERSION}/${TAR}
  cp "$TAR" "${CACHE_DIR}/${TAR}"
fi

rm -rf Python-${PY_VERSION}
tar xf $TAR
cd Python-${PY_VERSION}

# Configure options - prefer free-threading build when available
CONFIGURE_FLAGS=(--prefix="${PREFIX}" --enable-optimizations)

echo "Checking whether --disable-gil is supported by configure..."
if ./configure --help 2>/dev/null | grep -q -- --disable-gil; then
  CONFIGURE_FLAGS+=(--disable-gil)
  echo "--disable-gil supported: enabling free-threading build"
else
  echo "--disable-gil not supported by this CPython source/configure: continuing without it"
fi

echo "Configuring with: ${CONFIGURE_FLAGS[*]}"
./configure "${CONFIGURE_FLAGS[@]}"

echo "Compiling (this can take a while)"
make -j ${NUMJOBS}

echo "Installing into ${PREFIX}"
make install

echo "CPython ${PY_VERSION} installed to ${PREFIX}"
echo "If --disable-gil was used, verify with: ${PREFIX}/bin/python -c \"import sys; print(hasattr(sys,'_is_gil_enabled'))\""
