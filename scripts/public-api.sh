#!/usr/bin/env bash

set -euo pipefail

readonly PUBLIC_API_VERSION="0.52.0"
readonly INSTALLER_TOOLCHAIN="1.93.0"
readonly INSTALLER_RUSTC="rustc 1.93.0 (254b59607 2026-01-19)"
readonly RUSTDOC_TOOLCHAIN="nightly-2026-02-16"
readonly RUSTDOC_RUSTC="rustc 1.95.0-nightly (873b4beb0 2026-02-15)"
readonly TARGET="x86_64-unknown-linux-gnu"
readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SNAPSHOT_DIR="${ROOT}/public-api"
readonly TARGET_DIR="${ROOT}/target/public-api"

readonly -a ALL_CRATES=(
  formualizer
  formualizer-common
  formualizer-parse
  formualizer-eval
  formualizer-workbook
  formualizer-sheetport
  sheetport-spec
  formualizer-macros
)
declare -a SELECTED_CRATES=()

usage() {
  cat >&2 <<USAGE
Usage: scripts/public-api.sh <setup|check|update> [crate ...]

Crates:
  ${ALL_CRATES[*]}

With no crate names, check and update process every crate. setup accepts the
same optional validated crate names for a consistent interface, but tool setup
is shared by all crates.
USAGE
}

feature_profile() {
  case "$1" in
    formualizer)
      printf '%s\n' 'common,parse,eval,workbook,sheetport,calamine,json,csv,umya,tracing,tracing_chrome,system-clock'
      ;;
    formualizer-common)
      printf '%s\n' 'serde'
      ;;
    formualizer-parse)
      printf '%s\n' 'serde'
      ;;
    formualizer-eval)
      printf '%s\n' 'system-clock,tracing,tracing_chrome,perf_instrumentation,formula_plane_diagnostics'
      ;;
    formualizer-workbook)
      printf '%s\n' 'system-clock,json,csv,calamine,umya,mmap,io_builtins,import_range,webservice,tracing,perf_instrumentation,compression,calamine_integration,umya_integration,wasm_plugins,wasm_runtime_wasmtime'
      ;;
    formualizer-sheetport)
      printf '%s\n' 'system-clock,umya'
      ;;
    sheetport-spec|formualizer-macros)
      printf '%s\n' ''
      ;;
    *)
      return 1
      ;;
  esac
}

is_known_crate() {
  local candidate="$1"
  local known
  for known in "${ALL_CRATES[@]}"; do
    if [[ "$candidate" == "$known" ]]; then
      return 0
    fi
  done
  return 1
}

select_crates() {
  if (( $# == 0 )); then
    SELECTED_CRATES=("${ALL_CRATES[@]}")
    return
  fi

  SELECTED_CRATES=()
  local crate
  for crate in "$@"; do
    if ! is_known_crate "$crate"; then
      echo "error: unknown public API crate: ${crate}" >&2
      usage
      exit 2
    fi
    SELECTED_CRATES+=("$crate")
  done
}

ensure_toolchain() {
  local toolchain="$1"
  local expected="$2"
  local actual=""

  actual="$(rustup run "$toolchain" rustc --version 2>/dev/null || true)"
  if [[ "$actual" != "$expected" ]]; then
    echo "Installing Rust toolchain ${toolchain} (found: ${actual:-absent})"
    rustup toolchain install "$toolchain" --profile minimal
  fi

  actual="$(rustup run "$toolchain" rustc --version 2>/dev/null || true)"
  if [[ "$actual" != "$expected" ]]; then
    echo "error: ${toolchain} resolved to '${actual}', expected '${expected}'" >&2
    exit 1
  fi
  echo "Verified ${toolchain}: ${actual}"
}

setup_tools() {
  command -v rustup >/dev/null 2>&1 || {
    echo "error: rustup is required" >&2
    exit 1
  }
  command -v cargo >/dev/null 2>&1 || {
    echo "error: cargo is required" >&2
    exit 1
  }

  ensure_toolchain "$INSTALLER_TOOLCHAIN" "$INSTALLER_RUSTC"
  ensure_toolchain "$RUSTDOC_TOOLCHAIN" "$RUSTDOC_RUSTC"

  if ! rustup target list --installed --toolchain "$RUSTDOC_TOOLCHAIN" | grep -Fxq "$TARGET"; then
    echo "Installing target ${TARGET} for ${RUSTDOC_TOOLCHAIN}"
    rustup target add "$TARGET" --toolchain "$RUSTDOC_TOOLCHAIN"
  fi

  local actual=""
  actual="$(cargo public-api --version 2>/dev/null || true)"
  if [[ "$actual" != "cargo-public-api ${PUBLIC_API_VERSION}" ]]; then
    echo "Installing cargo-public-api ${PUBLIC_API_VERSION} (found: ${actual:-absent})"
    local -a install_args=(
      "+${INSTALLER_TOOLCHAIN}"
      install
      cargo-public-api
      --version "$PUBLIC_API_VERSION"
      --locked
    )
    if [[ -n "$actual" ]]; then
      install_args+=(--force)
    fi
    cargo "${install_args[@]}"
  fi

  actual="$(cargo public-api --version 2>/dev/null || true)"
  if [[ "$actual" != "cargo-public-api ${PUBLIC_API_VERSION}" ]]; then
    echo "error: found '${actual}', expected 'cargo-public-api ${PUBLIC_API_VERSION}'" >&2
    exit 1
  fi
  echo "Verified ${actual}"
  echo "Verified target ${TARGET} for ${RUSTDOC_TOOLCHAIN}"
}

generate_snapshot() {
  local crate="$1"
  local output="$2"
  local features
  features="$(feature_profile "$crate")"

  echo "Generating ${crate} public API..." >&2
  local -a args=(
    "+${RUSTDOC_TOOLCHAIN}"
    public-api
    --package "$crate"
    --no-default-features
    --target "$TARGET"
    --omit blanket-impls,auto-trait-impls
    --color never
  )
  if [[ -n "$features" ]]; then
    args+=(--features "$features")
  fi

  cargo "${args[@]}" >"$output"
}

prepare_target_dir() {
  mkdir -p "${TARGET_DIR}/doc" "${TARGET_DIR}/${TARGET}"
  rm -rf "${TARGET_DIR:?}/${TARGET}/doc"
  # Proc-macro rustdoc JSON is emitted in the host doc directory even when a
  # target is explicit. Make cargo-public-api's target-specific lookup resolve
  # both host proc-macro and target library output without changing the target.
  ln -s ../doc "${TARGET_DIR}/${TARGET}/doc"
}

generate_all() {
  local output_dir="$1"
  local crate
  mkdir -p "$output_dir"
  prepare_target_dir
  for crate in "${SELECTED_CRATES[@]}"; do
    generate_snapshot "$crate" "${output_dir}/${crate}.txt"
  done
}

check_snapshots() {
  local generated_dir="$1"
  local crate
  local changed=0

  for crate in "${SELECTED_CRATES[@]}"; do
    local expected="${SNAPSHOT_DIR}/${crate}.txt"
    local generated="${generated_dir}/${crate}.txt"
    if [[ ! -f "$expected" ]]; then
      echo "error: missing snapshot ${expected#"${ROOT}/"}" >&2
      changed=1
      continue
    fi
    if ! diff -u "$expected" "$generated"; then
      changed=1
    fi
  done

  if (( changed != 0 )); then
    cat >&2 <<INSTRUCTIONS

Public Rust API snapshots are out of date.
Review the diff, then regenerate the intended baseline with:
  scripts/public-api.sh update${SELECTED_CRATES[*]:+ ${SELECTED_CRATES[*]}}
Re-run:
  scripts/public-api.sh check${SELECTED_CRATES[*]:+ ${SELECTED_CRATES[*]}}
Commit the reviewed public-api/*.txt changes with the API change.
INSTRUCTIONS
    return 1
  fi

  echo "Public Rust API snapshots are current (${SELECTED_CRATES[*]})."
}

main() {
  if (( $# == 0 )); then
    usage
    exit 2
  fi

  local action="$1"
  shift
  case "$action" in
    setup|check|update) ;;
    *)
      echo "error: unknown action: ${action}" >&2
      usage
      exit 2
      ;;
  esac

  select_crates "$@"

  if [[ "$action" == "setup" ]]; then
    setup_tools
    return
  fi

  if [[ "$(cargo public-api --version 2>/dev/null || true)" != "cargo-public-api ${PUBLIC_API_VERSION}" ]] ||
     [[ "$(rustup run "$RUSTDOC_TOOLCHAIN" rustc --version 2>/dev/null || true)" != "$RUSTDOC_RUSTC" ]] ||
     ! rustup target list --installed --toolchain "$RUSTDOC_TOOLCHAIN" | grep -Fxq "$TARGET"; then
    echo "error: pinned public API tools are not ready; run scripts/public-api.sh setup" >&2
    exit 1
  fi

  export LC_ALL=C
  export TZ=UTC
  export CARGO_TERM_COLOR=never
  export CARGO_TARGET_DIR="$TARGET_DIR"
  unset RUSTFLAGS RUSTDOCFLAGS CARGO_BUILD_TARGET

  local generated_dir
  generated_dir="$(mktemp -d "${TMPDIR:-/tmp}/formualizer-public-api.XXXXXX")"
  trap "rm -rf '$generated_dir'" EXIT

  cd "$ROOT"
  generate_all "$generated_dir"

  if [[ "$action" == "check" ]]; then
    check_snapshots "$generated_dir"
    return
  fi

  mkdir -p "$SNAPSHOT_DIR"
  local crate
  for crate in "${SELECTED_CRATES[@]}"; do
    cp "${generated_dir}/${crate}.txt" "${SNAPSHOT_DIR}/${crate}.txt"
  done
  echo "Updated public Rust API snapshots (${SELECTED_CRATES[*]})."
}

main "$@"
