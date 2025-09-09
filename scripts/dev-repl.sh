#!/usr/bin/env bash
# Robust dev workflow to build latest native Python extension and launch IPython
# - Uses uv to manage the virtualenv and packages
# - Cleans stale native libs that can shadow new builds
# - Rebuilds via maturin develop --release
# - Launches IPython in the correct venv

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-${ROOT_DIR}/.venv}"
PYTHON_VERSION="${PYTHON_VERSION:-3.10}"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--py <version>]

Options:
  --py <version>   Python version for the uv venv (default: ${PYTHON_VERSION})

Environment vars:
  VENV_DIR         Override venv path (default: ${VENV_DIR})
  PYTHON_VERSION   Override default Python version (default: ${PYTHON_VERSION})
USAGE
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --py)
      shift
      [[ $# -gt 0 ]] || { echo "--py requires a version" >&2; exit 2; }
      PYTHON_VERSION="$1"
      ;;
    -h|--help)
      usage; exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2; usage; exit 2
      ;;
  esac
  shift || true
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: '$1' not found in PATH" >&2
    exit 1
  }
}

echo "[dev-repl] Repo: ${ROOT_DIR}"
require_cmd uv

echo "[dev-repl] Ensuring venv at ${VENV_DIR} (python ${PYTHON_VERSION})"
uv venv --python "${PYTHON_VERSION}" --seed "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

echo "[dev-repl] Upgrading build + REPL tooling (pip, maturin, ipython)"
uv pip install -U pip maturin ipython >/dev/null

echo "[dev-repl] Cleaning stale native libs in source package to avoid shadowing"
find "${ROOT_DIR}/bindings/python/formualizer" -maxdepth 1 -type f -name 'formualizer*.so' -print -delete || true

echo "[dev-repl] Removing any previously installed formualizer from venv"
uv pip uninstall -y formualizer >/dev/null 2>&1 || true

echo "[dev-repl] Building extension in editable mode via maturin (release)"
pushd "${ROOT_DIR}/bindings/python" >/dev/null
maturin develop --release
popd >/dev/null

echo "[dev-repl] Build complete. Launching IPython in venv (Ctrl-D to exit)"
IPYTHONSTARTUP="" "${VENV_DIR}/bin/ipython"
