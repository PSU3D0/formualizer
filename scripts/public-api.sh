#!/usr/bin/env bash

set -Eeuo pipefail

readonly PUBLIC_API_VERSION="0.52.0"
readonly PUBLIC_API_VERSION_REQ="=${PUBLIC_API_VERSION}"
readonly INSTALLER_TOOLCHAIN="1.93.0"
readonly INSTALLER_RUSTC="rustc 1.93.0 (254b59607 2026-01-19)"
readonly RUSTDOC_TOOLCHAIN="nightly-2026-02-16"
readonly RUSTDOC_RUSTC="rustc 1.95.0-nightly (873b4beb0 2026-02-15)"
readonly TARGET="x86_64-unknown-linux-gnu"
readonly SCRIPT_DIR="$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly ROOT="$(CDPATH= cd -P -- "${SCRIPT_DIR}/.." && pwd -P)"
readonly SNAPSHOT_DIR="${ROOT}/public-api"
readonly TARGET_ROOT="${ROOT}/target"
readonly TARGET_DIR="${TARGET_ROOT}/public-api"
readonly LOCK_DIR="${TARGET_ROOT}/public-api-lock"
readonly LOCK_PATH="${LOCK_DIR}/snapshots.lock"
readonly TOOL_ROOT="${TARGET_ROOT}/public-api-tools/cargo-public-api-${PUBLIC_API_VERSION}"
readonly PUBLIC_API_BIN="${TOOL_ROOT}/bin/cargo-public-api"
readonly ISOLATED_CARGO_HOME="${TARGET_ROOT}/public-api-cargo-home"

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
declare -A TRANSACTION_ORIGINAL_STATE=()

TMP_BASE=""
GENERATED_DIR=""
TRANSACTION_DIR=""
TRANSACTION_ACTIVE=0
TRANSACTION_ROLLBACK_FAILED=0
LOCK_FD=""

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

excluded_features() {
  case "$1" in
    formualizer)
      printf '%s\n' 'js-runtime,wasm-js'
      ;;
    formualizer-eval|formualizer-workbook|formualizer-sheetport)
      printf '%s\n' 'benchmark_internal,js-runtime'
      ;;
    *)
      printf '%s\n' ''
      ;;
  esac
}

alias_features() {
  case "$1" in
    formualizer)
      printf '%s\n' 'portable-wasm'
      ;;
    *)
      printf '%s\n' ''
      ;;
  esac
}

default_features() {
  case "$1" in
    formualizer-macros|sheetport-spec)
      printf '%s\n' ''
      ;;
    *)
      printf '%s\n' 'default'
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
  local crate selected
  for crate in "$@"; do
    if ! is_known_crate "$crate"; then
      echo "error: unknown public API crate: ${crate}" >&2
      usage
      exit 2
    fi
    for selected in "${SELECTED_CRATES[@]}"; do
      if [[ "$crate" == "$selected" ]]; then
        echo "error: duplicate public API crate: ${crate}" >&2
        usage
        exit 2
      fi
    done
    SELECTED_CRATES+=("$crate")
  done
}

assert_repo_containment() {
  local path="$1"
  case "$path" in
    "${ROOT}"/*) ;;
    *)
      echo "error: path escapes repository root: ${path}" >&2
      return 1
      ;;
  esac

  if [[ "$path" == *'/../'* || "$path" == */.. || "$path" == *'/./'* || "$path" == */. ]]; then
    echo "error: non-normalized repository path: ${path}" >&2
    return 1
  fi
}

assert_no_symlink_components() {
  local path="$1"
  assert_repo_containment "$path"

  local relative="${path#"${ROOT}/"}"
  local current="$ROOT"
  local -a components=()
  IFS='/' read -r -a components <<<"$relative"
  local index
  for (( index = 0; index < ${#components[@]}; index++ )); do
    [[ -n "${components[index]}" ]] || {
      echo "error: empty path component in ${path}" >&2
      return 1
    }
    current="${current}/${components[index]}"
    if [[ -L "$current" ]]; then
      echo "error: refusing symlinked repository path component: ${current#"${ROOT}/"}" >&2
      return 1
    fi
    if (( index + 1 < ${#components[@]} )) && [[ -e "$current" && ! -d "$current" ]]; then
      echo "error: non-directory repository path component: ${current#"${ROOT}/"}" >&2
      return 1
    fi
  done
}

assert_safe_snapshot_destination() {
  local crate="$1"
  local destination="${SNAPSHOT_DIR}/${crate}.txt"
  assert_no_symlink_components "$destination"
  [[ "$(dirname -- "$destination")" == "$SNAPSHOT_DIR" ]] || {
    echo "error: snapshot destination escaped snapshot directory: ${destination}" >&2
    return 1
  }
  if [[ -e "$destination" && ! -f "$destination" ]]; then
    echo "error: snapshot destination is not a regular file: ${destination#"${ROOT}/"}" >&2
    return 1
  fi
}

preflight_repo_paths() {
  assert_no_symlink_components "$SNAPSHOT_DIR"
  assert_no_symlink_components "$TARGET_ROOT"
  assert_no_symlink_components "$TARGET_DIR"
  assert_no_symlink_components "$LOCK_DIR"
  assert_no_symlink_components "$LOCK_PATH"
  assert_no_symlink_components "$TOOL_ROOT"
  assert_no_symlink_components "$PUBLIC_API_BIN"
  assert_no_symlink_components "$ISOLATED_CARGO_HOME"

  if [[ ! -d "$SNAPSHOT_DIR" ]]; then
    echo "error: snapshot directory is missing or not a directory: ${SNAPSHOT_DIR#"${ROOT}/"}" >&2
    return 1
  fi

  local directory
  for directory in "$TARGET_ROOT" "$TARGET_DIR" "$LOCK_DIR" "$TOOL_ROOT" "$ISOLATED_CARGO_HOME"; do
    if [[ -e "$directory" && ! -d "$directory" ]]; then
      echo "error: expected directory path is not a directory: ${directory#"${ROOT}/"}" >&2
      return 1
    fi
  done
  if [[ -e "$LOCK_PATH" && ! -f "$LOCK_PATH" ]]; then
    echo "error: public API lock path is not a regular file: ${LOCK_PATH#"${ROOT}/"}" >&2
    return 1
  fi
  if [[ -e "$PUBLIC_API_BIN" && ! -f "$PUBLIC_API_BIN" ]]; then
    echo "error: isolated cargo-public-api path is not a regular file: ${PUBLIC_API_BIN#"${ROOT}/"}" >&2
    return 1
  fi

  local crate
  for crate in "${SELECTED_CRATES[@]}"; do
    assert_safe_snapshot_destination "$crate"
  done
}

sanitize_build_environment() {
  export LC_ALL=C
  export TZ=UTC
  export CARGO_TERM_COLOR=never
  export CARGO_TARGET_DIR="$TARGET_DIR"
  export CARGO_HOME="$ISOLATED_CARGO_HOME"
  unset \
    RUSTFLAGS \
    RUSTDOCFLAGS \
    CARGO_ENCODED_RUSTFLAGS \
    CARGO_ENCODED_RUSTDOCFLAGS \
    CARGO_BUILD_TARGET \
    CARGO_BUILD_RUSTFLAGS \
    CARGO \
    RUSTC \
    RUSTDOC \
    RUSTC_WRAPPER \
    RUSTC_WORKSPACE_WRAPPER \
    CARGO_BUILD_RUSTC \
    CARGO_BUILD_RUSTDOC \
    CARGO_BUILD_RUSTC_WRAPPER \
    CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER \
    RUSTC_BOOTSTRAP
}

ensure_safe_local_directories() {
  preflight_repo_paths
  mkdir -p -- "$TARGET_DIR" "$TOOL_ROOT" "$ISOLATED_CARGO_HOME"
  preflight_repo_paths
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

  ensure_safe_local_directories
  ensure_toolchain "$INSTALLER_TOOLCHAIN" "$INSTALLER_RUSTC"
  ensure_toolchain "$RUSTDOC_TOOLCHAIN" "$RUSTDOC_RUSTC"

  if ! rustup target list --installed --toolchain "$RUSTDOC_TOOLCHAIN" | grep -Fxq "$TARGET"; then
    echo "Installing target ${TARGET} for ${RUSTDOC_TOOLCHAIN}"
    rustup target add "$TARGET" --toolchain "$RUSTDOC_TOOLCHAIN"
  fi

  local actual=""
  if [[ -x "$PUBLIC_API_BIN" && ! -L "$PUBLIC_API_BIN" ]]; then
    actual="$("$PUBLIC_API_BIN" --version 2>/dev/null || true)"
  fi
  if [[ "$actual" != "cargo-public-api ${PUBLIC_API_VERSION}" ]]; then
    echo "Installing isolated cargo-public-api ${PUBLIC_API_VERSION_REQ} (found: ${actual:-absent})"
    local -a install_args=(
      "+${INSTALLER_TOOLCHAIN}"
      install
      cargo-public-api
      --version "$PUBLIC_API_VERSION_REQ"
      --locked
      --root "$TOOL_ROOT"
    )
    if [[ -e "$PUBLIC_API_BIN" ]]; then
      install_args+=(--force)
    fi
    cargo "${install_args[@]}"
  fi

  actual="$("$PUBLIC_API_BIN" --version 2>/dev/null || true)"
  if [[ "$actual" != "cargo-public-api ${PUBLIC_API_VERSION}" ]]; then
    echo "error: found '${actual}', expected 'cargo-public-api ${PUBLIC_API_VERSION}'" >&2
    exit 1
  fi
  echo "Verified isolated ${actual}: ${PUBLIC_API_BIN#"${ROOT}/"}"
  echo "Verified target ${TARGET} for ${RUSTDOC_TOOLCHAIN}"
}

verify_tools() {
  local actual=""
  if [[ -x "$PUBLIC_API_BIN" && ! -L "$PUBLIC_API_BIN" ]]; then
    actual="$("$PUBLIC_API_BIN" --version 2>/dev/null || true)"
  fi
  if [[ "$actual" != "cargo-public-api ${PUBLIC_API_VERSION}" ]] ||
     [[ "$(rustup run "$RUSTDOC_TOOLCHAIN" rustc --version 2>/dev/null || true)" != "$RUSTDOC_RUSTC" ]] ||
     ! rustup target list --installed --toolchain "$RUSTDOC_TOOLCHAIN" | grep -Fxq "$TARGET"; then
    echo "error: pinned public API tools are not ready; run scripts/public-api.sh setup" >&2
    exit 1
  fi
}

acquire_process_lock() {
  local action="$1"
  command -v flock >/dev/null 2>&1 || {
    echo "error: flock is required for public API snapshot concurrency safety" >&2
    exit 1
  }

  preflight_repo_paths
  mkdir -p -- "$LOCK_DIR"
  preflight_repo_paths
  exec {LOCK_FD}>>"$LOCK_PATH"
  if [[ -L "$LOCK_PATH" || ! -f "$LOCK_PATH" ]]; then
    echo "error: public API lock path became unsafe: ${LOCK_PATH#"${ROOT}/"}" >&2
    exec {LOCK_FD}>&-
    LOCK_FD=""
    exit 1
  fi

  case "$action" in
    check)
      echo "Waiting for shared public API snapshot lock..." >&2
      flock --shared "$LOCK_FD"
      ;;
    update)
      echo "Waiting for exclusive public API snapshot lock..." >&2
      flock --exclusive "$LOCK_FD"
      ;;
    *)
      echo "error: internal lock request for unsupported action: ${action}" >&2
      exit 1
      ;;
  esac
  echo "Acquired ${action} public API snapshot lock." >&2
}

release_process_lock() {
  [[ -n "$LOCK_FD" ]] || return 0
  flock --unlock "$LOCK_FD" || true
  exec {LOCK_FD}>&-
  LOCK_FD=""
}

validate_feature_policy() {
  command -v python3 >/dev/null 2>&1 || {
    echo "error: python3 is required to validate public API feature policy" >&2
    exit 1
  }

  local policy=""
  local crate
  for crate in "${ALL_CRATES[@]}"; do
    policy+="${crate}|$(feature_profile "$crate")|$(excluded_features "$crate")|$(alias_features "$crate")|$(default_features "$crate")"$'\n'
  done

  local metadata
  metadata="$(cargo "+${RUSTDOC_TOOLCHAIN}" metadata --manifest-path "${ROOT}/Cargo.toml" --no-deps --format-version 1)"
  PUBLIC_API_FEATURE_POLICY="$policy" python3 -c '
import json
import os
import sys

metadata = json.load(sys.stdin)
packages = {package["name"]: set(package["features"]) for package in metadata["packages"]}
problems = []
separator = ", "

def parse_csv(value):
    return {item for item in value.split(",") if item}

for line in os.environ["PUBLIC_API_FEATURE_POLICY"].splitlines():
    crate, enabled, excluded, aliases, defaults = line.split("|", 4)
    categories = {
        "enabled native": parse_csv(enabled),
        "explicitly excluded": parse_csv(excluded),
        "alias": parse_csv(aliases),
        "default": parse_csv(defaults),
    }
    if crate not in packages:
        problems.append(f"{crate}: governed package missing from cargo metadata")
        continue
    seen = {}
    for category, features in categories.items():
        for feature in features:
            if feature in seen:
                problems.append(
                    f"{crate}: feature {feature!r} classified as both {seen[feature]} and {category}"
                )
            seen[feature] = category
    actual = packages[crate]
    classified = set(seen)
    unknown = sorted(actual - classified)
    stale = sorted(classified - actual)
    if unknown:
        problems.append(f"{crate}: unclassified Cargo feature(s): {separator.join(unknown)}")
    if stale:
        problems.append(f"{crate}: classified feature(s) missing from manifest: {separator.join(stale)}")

if problems:
    print("error: public API feature policy is incomplete:", file=sys.stderr)
    for problem in problems:
        print(f"  - {problem}", file=sys.stderr)
    print(
        "Classify each feature in scripts/public-api.sh as enabled native, "
        "explicitly excluded, alias, or default; then review and regenerate snapshots.",
        file=sys.stderr,
    )
    raise SystemExit(1)
' <<<"$metadata"
}

canonicalize_temp_base() {
  local requested="${TMPDIR:-/tmp}"
  if [[ "$requested" != /* ]]; then
    echo "error: TMPDIR must be an absolute path: ${requested}" >&2
    return 1
  fi
  TMP_BASE="$(CDPATH= cd -P -- "$requested" 2>/dev/null && pwd -P)" || {
    echo "error: TMPDIR is not an accessible directory: ${requested}" >&2
    return 1
  }
  [[ -d "$TMP_BASE" && -w "$TMP_BASE" ]] || {
    echo "error: TMPDIR is not a writable directory: ${TMP_BASE}" >&2
    return 1
  }
}

initialize_generated_dir() {
  canonicalize_temp_base
  GENERATED_DIR="$(mktemp -d -- "${TMP_BASE}/formualizer-public-api.XXXXXXXX")"
  [[ "$GENERATED_DIR" == "${TMP_BASE}"/formualizer-public-api.* ]] || {
    echo "error: mktemp returned a path outside canonical TMPDIR: ${GENERATED_DIR}" >&2
    return 1
  }
  [[ -d "$GENERATED_DIR" && ! -L "$GENERATED_DIR" ]] || {
    echo "error: generated temporary path is not a safe directory: ${GENERATED_DIR}" >&2
    return 1
  }
  [[ "$(CDPATH= cd -P -- "$(dirname -- "$GENERATED_DIR")" && pwd -P)" == "$TMP_BASE" ]] || {
    echo "error: generated temporary directory escaped canonical TMPDIR" >&2
    return 1
  }
}

safe_remove_generated_dir() {
  [[ -n "$GENERATED_DIR" ]] || return 0
  if [[ "$GENERATED_DIR" == "${TMP_BASE}"/formualizer-public-api.* &&
        -d "$GENERATED_DIR" && ! -L "$GENERATED_DIR" &&
        "$(CDPATH= cd -P -- "$(dirname -- "$GENERATED_DIR")" 2>/dev/null && pwd -P)" == "$TMP_BASE" ]]; then
    rm -rf -- "$GENERATED_DIR"
  else
    echo "warning: refusing to remove unsafe temporary path: ${GENERATED_DIR}" >&2
  fi
  GENERATED_DIR=""
}

safe_remove_transaction_dir() {
  [[ -n "$TRANSACTION_DIR" ]] || return 0
  if [[ "$TRANSACTION_DIR" == "${SNAPSHOT_DIR}"/.public-api-transaction.* &&
        -d "$TRANSACTION_DIR" && ! -L "$TRANSACTION_DIR" &&
        "$(CDPATH= cd -P -- "$(dirname -- "$TRANSACTION_DIR")" 2>/dev/null && pwd -P)" == "$SNAPSHOT_DIR" ]]; then
    rm -rf -- "$TRANSACTION_DIR"
    TRANSACTION_DIR=""
  else
    echo "warning: refusing to remove unsafe transaction path: ${TRANSACTION_DIR}" >&2
    return 1
  fi
}

cleanup_on_exit() {
  local status=$?
  trap - EXIT
  if (( TRANSACTION_ACTIVE != 0 )); then
    rollback_transaction || status=70
  fi
  if (( TRANSACTION_ROLLBACK_FAILED == 0 )); then
    safe_remove_transaction_dir || status=70
  fi
  safe_remove_generated_dir
  release_process_lock
  exit "$status"
}

remove_transaction_destination() {
  local destination="$1"
  assert_no_symlink_components "$destination" || return 1
  if [[ -d "$destination" && ! -L "$destination" ]]; then
    echo "error: refusing to remove unexpected directory during rollback: ${destination}" >&2
    return 1
  fi
  if [[ -e "$destination" || -L "$destination" ]]; then
    rm -f -- "$destination"
  fi
}

rollback_transaction() {
  (( TRANSACTION_ACTIVE != 0 )) || return 0
  echo "Rolling back public API snapshot transaction..." >&2
  local failed=0
  local index crate destination backup staged state
  for (( index = ${#SELECTED_CRATES[@]} - 1; index >= 0; index-- )); do
    crate="${SELECTED_CRATES[index]}"
    destination="${SNAPSHOT_DIR}/${crate}.txt"
    backup="${TRANSACTION_DIR}/backups/${crate}.txt"
    staged="${TRANSACTION_DIR}/staged/${crate}.txt"
    state="${TRANSACTION_ORIGINAL_STATE[$crate]}"
    if [[ "$state" == present ]]; then
      if [[ -f "$backup" && ! -L "$backup" ]]; then
        if remove_transaction_destination "$destination"; then
          mv -- "$backup" "$destination" || failed=1
        else
          failed=1
        fi
      fi
    elif [[ ! -e "$staged" && ! -L "$staged" ]]; then
      remove_transaction_destination "$destination" || failed=1
    fi
  done

  if (( failed != 0 )); then
    TRANSACTION_ROLLBACK_FAILED=1
    echo "error: snapshot rollback was incomplete; backups retained at ${TRANSACTION_DIR}" >&2
    return 1
  fi
  TRANSACTION_ACTIVE=0
  echo "Public API snapshot transaction rolled back." >&2
}

transaction_error_handler() {
  local status=$?
  trap - ERR INT TERM
  rollback_transaction || status=70
  exit "$status"
}

transaction_int_handler() {
  trap - ERR INT TERM
  rollback_transaction || true
  exit 130
}

transaction_term_handler() {
  trap - ERR INT TERM
  rollback_transaction || true
  exit 143
}

managed_doc_link_is_valid() {
  local doc_link="$1"
  [[ -L "$doc_link" ]] &&
    [[ "$(readlink -- "$doc_link")" == ../doc ]] &&
    [[ "$(realpath -m -- "$doc_link")" == "${TARGET_DIR}/doc" ]]
}

prepare_target_dir() {
  assert_no_symlink_components "$TARGET_DIR"
  mkdir -p -- "${TARGET_DIR}/doc" "${TARGET_DIR}/${TARGET}"
  assert_no_symlink_components "${TARGET_DIR}/doc"
  assert_no_symlink_components "${TARGET_DIR}/${TARGET}"

  local doc_link="${TARGET_DIR}/${TARGET}/doc"
  if [[ -L "$doc_link" ]]; then
    if ! managed_doc_link_is_valid "$doc_link"; then
      echo "error: refusing unexpected proc-macro doc symlink: ${doc_link#"${ROOT}/"}" >&2
      return 1
    fi
  elif [[ -e "$doc_link" ]]; then
    echo "error: expected proc-macro doc path to be absent or the managed ../doc symlink: ${doc_link#"${ROOT}/"}" >&2
    return 1
  elif ! ln -s -- ../doc "$doc_link"; then
    # Concurrent shared checks may both initialize the managed bridge.
    if ! managed_doc_link_is_valid "$doc_link"; then
      echo "error: failed to create the managed proc-macro doc symlink: ${doc_link#"${ROOT}/"}" >&2
      return 1
    fi
  fi
}

generate_snapshot() {
  local crate="$1"
  local output="$2"
  local features
  features="$(feature_profile "$crate")"

  echo "Generating ${crate} public API..." >&2
  local -a args=(
    --package "$crate"
    --no-default-features
    --target "$TARGET"
    --omit blanket-impls,auto-trait-impls
    --color never
  )
  if [[ -n "$features" ]]; then
    args+=(--features "$features")
  fi

  RUSTUP_TOOLCHAIN="$RUSTDOC_TOOLCHAIN" "$PUBLIC_API_BIN" "${args[@]}" >"$output"
  [[ -f "$output" && ! -L "$output" ]] || {
    echo "error: generator did not create a regular snapshot for ${crate}" >&2
    return 1
  }
}

generate_all() {
  local output_dir="$1"
  local crate
  prepare_target_dir
  for crate in "${SELECTED_CRATES[@]}"; do
    generate_snapshot "$crate" "${output_dir}/${crate}.txt"
  done
}

check_snapshots() {
  local generated_dir="$1"
  local crate
  local drift=0
  local operational_failure=0

  for crate in "${SELECTED_CRATES[@]}"; do
    local expected="${SNAPSHOT_DIR}/${crate}.txt"
    local generated="${generated_dir}/${crate}.txt"
    if [[ ! -f "$expected" ]]; then
      echo "error: missing snapshot ${expected#"${ROOT}/"}" >&2
      drift=1
      continue
    fi

    local diff_status
    set +e
    diff -u -- "$expected" "$generated"
    diff_status=$?
    set -e
    case "$diff_status" in
      0) ;;
      1) drift=1 ;;
      *)
        echo "error: diff failed operationally for ${crate} (status ${diff_status})" >&2
        operational_failure=1
        ;;
    esac
  done

  if (( operational_failure != 0 )); then
    echo "Public Rust API comparison could not complete; fix the operational error and re-run the check." >&2
    return 2
  fi

  if (( drift != 0 )); then
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

stage_snapshot_transaction() {
  preflight_repo_paths
  TRANSACTION_DIR="$(mktemp -d -- "${SNAPSHOT_DIR}/.public-api-transaction.XXXXXXXX")"
  [[ "$TRANSACTION_DIR" == "${SNAPSHOT_DIR}"/.public-api-transaction.* &&
        -d "$TRANSACTION_DIR" && ! -L "$TRANSACTION_DIR" ]] || {
    echo "error: failed to create a safe snapshot transaction directory" >&2
    return 1
  }
  mkdir -- "${TRANSACTION_DIR}/staged" "${TRANSACTION_DIR}/backups"

  local crate source staged destination
  for crate in "${SELECTED_CRATES[@]}"; do
    source="${GENERATED_DIR}/${crate}.txt"
    staged="${TRANSACTION_DIR}/staged/${crate}.txt"
    destination="${SNAPSHOT_DIR}/${crate}.txt"
    [[ -f "$source" && ! -L "$source" ]] || {
      echo "error: generated snapshot is not a regular file: ${source}" >&2
      return 1
    }
    assert_safe_snapshot_destination "$crate"
    assert_no_symlink_components "$staged"
    assert_no_symlink_components "${TRANSACTION_DIR}/backups/${crate}.txt"
    cp -- "$source" "$staged"
    [[ -f "$staged" && ! -L "$staged" ]] || {
      echo "error: failed to stage regular snapshot for ${crate}" >&2
      return 1
    }
    if [[ -e "$destination" ]]; then
      TRANSACTION_ORIGINAL_STATE[$crate]=present
    else
      TRANSACTION_ORIGINAL_STATE[$crate]=absent
    fi
  done
}

publish_snapshot_transaction() {
  local fail_after="${PUBLIC_API_TEST_FAIL_AFTER_INSTALL:-0}"
  [[ "$fail_after" =~ ^[0-9]+$ ]] || {
    echo "error: PUBLIC_API_TEST_FAIL_AFTER_INSTALL must be a non-negative integer" >&2
    return 1
  }

  TRANSACTION_ACTIVE=1
  trap transaction_error_handler ERR
  trap transaction_int_handler INT
  trap transaction_term_handler TERM

  local installed=0
  local crate destination staged backup
  for crate in "${SELECTED_CRATES[@]}"; do
    destination="${SNAPSHOT_DIR}/${crate}.txt"
    staged="${TRANSACTION_DIR}/staged/${crate}.txt"
    backup="${TRANSACTION_DIR}/backups/${crate}.txt"
    assert_safe_snapshot_destination "$crate"
    assert_no_symlink_components "$staged"
    assert_no_symlink_components "$backup"
    [[ -f "$staged" && ! -L "$staged" ]] || {
      echo "error: staged snapshot is not a regular file: ${staged}" >&2
      return 1
    }
    if [[ -e "$backup" || -L "$backup" ]]; then
      echo "error: snapshot backup destination already exists: ${backup}" >&2
      return 1
    fi
    if [[ "${TRANSACTION_ORIGINAL_STATE[$crate]}" == present ]]; then
      mv -- "$destination" "$backup"
    fi
    mv -- "$staged" "$destination"
    (( installed += 1 ))
    if (( fail_after > 0 && installed == fail_after )); then
      echo "error: injected snapshot publication failure after ${installed} install(s)" >&2
      return 97
    fi
  done

  TRANSACTION_ACTIVE=0
  trap - ERR INT TERM
  safe_remove_transaction_dir
  echo "Updated public Rust API snapshots (${SELECTED_CRATES[*]})."
}

update_snapshots() {
  stage_snapshot_transaction
  publish_snapshot_transaction
}

main() {
  trap cleanup_on_exit EXIT

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
  sanitize_build_environment
  preflight_repo_paths

  if [[ "$action" == setup ]]; then
    setup_tools
    return
  fi

  verify_tools
  acquire_process_lock "$action"
  initialize_generated_dir
  validate_feature_policy

  CDPATH= cd -P -- "$ROOT"
  generate_all "$GENERATED_DIR"

  if [[ "$action" == check ]]; then
    check_snapshots "$GENERATED_DIR"
    return
  fi

  update_snapshots
}

main "$@"
