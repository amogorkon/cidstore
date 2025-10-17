#!/usr/bin/env bash
# Wrapper script for GitHub Actions to call the repository build script
set -euo pipefail
REPO_ROOT=$(dirname "$(dirname "$(realpath "$0")")")/../../
exec "$REPO_ROOT/scripts/ci_build_python.sh" "$@"
