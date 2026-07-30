#!/usr/bin/env bash

set -euo pipefail

readonly ROOT="$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
readonly EVAL_SOURCE="${ROOT}/crates/formualizer-eval/src/lib.rs"
readonly COMMON_SOURCE="${ROOT}/crates/formualizer-common/src/lib.rs"
readonly EVAL_SNAPSHOT="${ROOT}/public-api/formualizer-eval.txt"
readonly COMMON_SNAPSHOT="${ROOT}/public-api/formualizer-common.txt"
readonly TEST_DIR="$(mktemp -d -- "${TMPDIR:-/tmp}/formualizer-public-api-concurrency.XXXXXXXX")"

A_PID=""
B_PID=""

interrupt_handler() {
  exit 130
}

terminate_handler() {
  exit 143
}

initial_cleanup() {
  rm -rf -- "$TEST_DIR"
}
trap initial_cleanup EXIT
trap interrupt_handler INT
trap terminate_handler TERM

cleanup() {
  touch "${TEST_DIR}/release" 2>/dev/null || true
  for pid in "$A_PID" "$B_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done

  cp -- "${TEST_DIR}/eval-source" "$EVAL_SOURCE"
  cp -- "${TEST_DIR}/common-source" "$COMMON_SOURCE"
  cp -- "${TEST_DIR}/eval-snapshot" "$EVAL_SNAPSHOT"
  cp -- "${TEST_DIR}/common-snapshot" "$COMMON_SNAPSHOT"
  rm -rf -- "$TEST_DIR"
}

cd "$ROOT"
for path in "$EVAL_SOURCE" "$COMMON_SOURCE" "$EVAL_SNAPSHOT" "$COMMON_SNAPSHOT"; do
  if ! git diff --quiet -- "$path" || ! git diff --cached --quiet -- "$path"; then
    echo "error: concurrency test requires a clean ${path#"${ROOT}/"}" >&2
    exit 1
  fi
done

cp -- "$EVAL_SOURCE" "${TEST_DIR}/eval-source"
cp -- "$COMMON_SOURCE" "${TEST_DIR}/common-source"
cp -- "$EVAL_SNAPSHOT" "${TEST_DIR}/eval-snapshot"
cp -- "$COMMON_SNAPSHOT" "${TEST_DIR}/common-snapshot"
trap cleanup EXIT
mkdir -- "${TEST_DIR}/bin"

cat >"${TEST_DIR}/bin/mv" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
/usr/bin/mv "$@"
arguments=("$@")
destination="${arguments[${#arguments[@]} - 1]}"
if [[ "$destination" == "${PUBLIC_API_CONCURRENCY_ROOT}/public-api/formualizer-eval.txt" &&
      ! -e "${PUBLIC_API_CONCURRENCY_STATE}/ready" ]]; then
  touch "${PUBLIC_API_CONCURRENCY_STATE}/ready"
  while [[ ! -e "${PUBLIC_API_CONCURRENCY_STATE}/release" ]]; do
    sleep 0.05
  done
fi
WRAPPER
chmod +x "${TEST_DIR}/bin/mv"

printf '\npub fn __public_api_concurrency_a() {}\n' >>"$EVAL_SOURCE"
printf '\npub fn __public_api_concurrency_a() {}\n' >>"$COMMON_SOURCE"

env \
  PATH="${TEST_DIR}/bin:${PATH}" \
  PUBLIC_API_CONCURRENCY_ROOT="$ROOT" \
  PUBLIC_API_CONCURRENCY_STATE="$TEST_DIR" \
  scripts/public-api.sh update formualizer-eval formualizer-common \
  >"${TEST_DIR}/a.log" 2>&1 &
A_PID=$!

for (( attempt = 0; attempt < 1200; attempt++ )); do
  [[ -e "${TEST_DIR}/ready" ]] && break
  if ! kill -0 "$A_PID" 2>/dev/null; then
    cat "${TEST_DIR}/a.log" >&2
    echo "error: update A exited before its first snapshot install pause" >&2
    exit 1
  fi
  sleep 0.05
done
[[ -e "${TEST_DIR}/ready" ]] || {
  echo "error: timed out waiting for update A publication pause" >&2
  exit 1
}

cp -- "${TEST_DIR}/eval-source" "$EVAL_SOURCE"
cp -- "${TEST_DIR}/common-source" "$COMMON_SOURCE"
printf '\npub fn __public_api_concurrency_b() {}\n' >>"$EVAL_SOURCE"
printf '\npub fn __public_api_concurrency_b() {}\n' >>"$COMMON_SOURCE"

scripts/public-api.sh update formualizer-eval formualizer-common \
  >"${TEST_DIR}/b.log" 2>&1 &
B_PID=$!

for (( attempt = 0; attempt < 200; attempt++ )); do
  grep -Fq 'Waiting for exclusive public API snapshot lock...' "${TEST_DIR}/b.log" && break
  sleep 0.05
done
grep -Fq 'Waiting for exclusive public API snapshot lock...' "${TEST_DIR}/b.log" || {
  cat "${TEST_DIR}/b.log" >&2
  echo "error: update B did not reach the exclusive lock" >&2
  exit 1
}
sleep 0.5
kill -0 "$B_PID"
if grep -Fq 'Acquired update public API snapshot lock.' "${TEST_DIR}/b.log"; then
  echo "error: update B acquired the lock while update A was paused" >&2
  exit 1
fi

touch "${TEST_DIR}/release"
wait "$A_PID"
A_PID=""
wait "$B_PID"
B_PID=""

grep -Fxq 'pub fn formualizer_eval::__public_api_concurrency_b()' "$EVAL_SNAPSHOT"
grep -Fxq 'pub fn formualizer_common::__public_api_concurrency_b()' "$COMMON_SNAPSHOT"
! grep -Fq '__public_api_concurrency_a' "$EVAL_SNAPSHOT"
! grep -Fq '__public_api_concurrency_a' "$COMMON_SNAPSHOT"

grep -Fq 'Acquired update public API snapshot lock.' "${TEST_DIR}/b.log"
echo "Concurrent updates serialized to the complete B snapshot set."

cleanup
trap - EXIT INT TERM
scripts/public-api.sh check formualizer-eval formualizer-common
