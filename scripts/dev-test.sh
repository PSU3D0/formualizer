#!/usr/bin/env bash
set -euo pipefail

# Ensure we're at repo root
cd "$(dirname "$(realpath "$0")")/.."

# Build Python extension into a venv via uv + maturin, then run pytest
# Requires: uv, maturin, Rust toolchain

if ! command -v uv >/dev/null 2>&1; then
  echo "ERROR: 'uv' not found. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
  exit 1
fi
if ! command -v maturin >/dev/null 2>&1; then
  echo "INFO: Installing maturin into the uv virtualenv..." >&2
fi

# Create an isolated venv named .venv (local to repo)
uv venv --python 3.10 --seed .venv
source .venv/bin/activate

# Install maturin in venv if missing
if ! command -v maturin >/dev/null 2>&1; then
  uv pip install maturin
fi

# Build and develop-install the extension
pushd bindings/python >/dev/null
maturin develop --release
popd >/dev/null

# Install test dependencies into the active venv
uv pip install -q pytest openpyxl pytest-cov

# Run pytest from the active venv so it can import the built extension
pytest -q bindings/python/tests
