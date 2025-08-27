#!/usr/bin/env bash
set -euo pipefail

# Run Python bindings tests with uv ensuring deps are present.
# Usage: ./bindings/python/scripts/uv-test.sh [pytest-args...]

cd "$(dirname "$0")/.."/tests

# Ensure openpyxl + pytest available; uv will respect inline metadata or we can pass deps explicitly.
if command -v uv >/dev/null 2>&1; then
  uv run --with pytest --with openpyxl pytest "$@"
else
  echo "uv is required. Install from https://github.com/astral-sh/uv" >&2
  exit 1
fi

