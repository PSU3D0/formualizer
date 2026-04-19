#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ $# -lt 1 ]]; then
    echo 'Usage: ./scripts/smoke-pyodide-wheel.sh dist/pyodide/<wheel>.whl' >&2
    exit 1
fi

WHEEL_PATH="$1"
if [[ ! -f "$WHEEL_PATH" ]]; then
    echo "Wheel not found: $WHEEL_PATH" >&2
    exit 1
fi

# Match the default pinned in build-pyodide-wheel.sh. The smoke test uses the
# Pyodide npm package at this version to host the wheel under test.
PYODIDE_NPM_VERSION="${PYODIDE_NPM_VERSION:-0.29.3}"
PYODIDE_TMPDIR="$(mktemp -d)"
trap 'rm -rf "$PYODIDE_TMPDIR"' EXIT

printf 'Running Pyodide smoke test\n'
printf '  wheel:   %s\n' "$WHEEL_PATH"
printf '  pyodide: %s\n' "$PYODIDE_NPM_VERSION"

npm --prefix "$PYODIDE_TMPDIR" install --no-save "pyodide@${PYODIDE_NPM_VERSION}" >/dev/null
cp scripts/pyodide-smoke.mjs "$PYODIDE_TMPDIR/pyodide-smoke.mjs"

node "$PYODIDE_TMPDIR/pyodide-smoke.mjs" \
    "$WHEEL_PATH" \
    "$ROOT_DIR/scripts/pyodide-smoke.py"
