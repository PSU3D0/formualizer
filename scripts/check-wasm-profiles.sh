#!/usr/bin/env bash
# check-wasm-profiles.sh — Validate wasm runtime profile contracts.
#
# Usage:
#   scripts/check-wasm-profiles.sh portable    # assert zero wasm-bindgen
#   scripts/check-wasm-profiles.sh wasm-js     # assert wasm-bindgen is present
#
# cargo tree exits 0 in both cases; the distinguishing signal is whether
# stdout is non-empty (match found) or empty (no match / "nothing to print").
#
# Exit 0 = contract satisfied; non-zero = violation detected.

set -euo pipefail

MODE="${1:-}"
if [[ -z "$MODE" ]]; then
  echo "Usage: $0 <portable|wasm-js>" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Run cargo tree and capture only stdout (warnings go to stderr).
tree_stdout() {
  local features="$1"
  cargo tree \
    -p formualizer \
    --no-default-features \
    --features "$features" \
    --target wasm32-unknown-unknown \
    -e normal \
    -i wasm-bindgen \
    2>/dev/null
}

# --------------------------------------------------------------------------
# portable profile — zero wasm-bindgen allowed
# --------------------------------------------------------------------------
check_portable() {
  echo "[portable-wasm] Checking dep tree for wasm-bindgen..."
  local result
  result=$(tree_stdout "portable-wasm")

  if [[ -z "$result" ]]; then
    echo "[portable-wasm] ✅  zero wasm-bindgen in dep tree — contract satisfied."
    return 0
  fi

  echo "[portable-wasm] ❌  wasm-bindgen found in portable dep tree — VIOLATION:" >&2
  echo "$result" >&2
  return 1
}

# --------------------------------------------------------------------------
# wasm-js profile — wasm-bindgen must be present (browser runtime intact)
# --------------------------------------------------------------------------
check_wasm_js() {
  echo "[wasm-js] Checking dep tree contains wasm-bindgen..."
  local result
  result=$(tree_stdout "wasm-js")

  if [[ -n "$result" ]]; then
    echo "[wasm-js] ✅  wasm-bindgen present in wasm-js dep tree — contract satisfied."
    return 0
  fi

  echo "[wasm-js] ❌  wasm-bindgen missing from wasm-js dep tree — browser runtime broken." >&2
  return 1
}

cd "$REPO_ROOT"

case "$MODE" in
  portable)  check_portable ;;
  wasm-js)   check_wasm_js  ;;
  *)
    echo "Unknown mode: $MODE  (expected 'portable' or 'wasm-js')" >&2
    exit 1
    ;;
esac
