#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
ROOT=$(pwd)
PYTHON_BINDING="bindings/python"
INSPECT="$PYTHON_BINDING/src/inspect.rs"
ERRORS="$PYTHON_BINDING/src/errors.rs"
BACKUP="$PYTHON_BINDING/.inspect-mutation-backup"
PYTHON="${PYTHON:-$ROOT/$PYTHON_BINDING/.venv/bin/python}"
MATURIN="${MATURIN:-$ROOT/$PYTHON_BINDING/.venv/bin/maturin}"

rm -rf "$BACKUP"
mkdir -p "$BACKUP"
cp "$INSPECT" "$BACKUP/inspect.rs"
cp "$ERRORS" "$BACKUP/errors.rs"
restore() {
  cp "$BACKUP/inspect.rs" "$INSPECT"
  cp "$BACKUP/errors.rs" "$ERRORS"
}
cleanup() {
  restore
  rm -rf "$BACKUP"
}
trap cleanup EXIT

run_mutant() {
  local name="$1"
  restore
  shift
  "$@"
  (cd "$PYTHON_BINDING" && PYO3_PYTHON="${PYO3_PYTHON:-/usr/bin/python3.12}" "$MATURIN" develop --quiet)
  if "$PYTHON" -m pytest "$PYTHON_BINDING/tests/test_inspection.py" -q --no-cov >/dev/null 2>&1; then
    echo "SURVIVED  $name"
    return 1
  fi
  echo "KILLED    $name"
}

mutate_drop_field() {
  sed -i '0,/fn value_included(&self) -> bool {/!b;n;s/self.inner.value_included/false/' "$INSPECT"
}
mutate_wrong_order() {
  sed -i '/fn dependents(&self) -> Vec<PyDependent>/,/^    }/ s/\.into_iter()/\.into_iter().rev()/' "$INSPECT"
}
mutate_swap_omitted() {
  sed -i '0,/core::OmittedCount::AtLeast(_) => PyOmittedCountKind::AtLeast/s//core::OmittedCount::AtLeast(_) => PyOmittedCountKind::Exact/' "$INSPECT"
}
mutate_mapping() {
  sed -i '/fn __getitem__(&self, address:/,/^    }/ s/^            index,$/            index: 0,/' "$INSPECT"
}
mutate_repr() {
  sed -i 's/"TraceGraph(nodes={}/"TraceGraph(debug={:?}, nodes={}/' "$INSPECT"
  sed -i '/"TraceGraph(debug=/,/self.inner.graph.truncation.incomplete/ s/^            self.inner.graph.nodes.len(),/            \&self.inner.graph, self.inner.graph.nodes.len(),/' "$INSPECT"
}
mutate_exception() {
  sed -i '0,/SheetNotFoundError::new_err/s//InvalidInspectionAddressError::new_err/' "$ERRORS"
}
mutate_ignored_max_results() {
  sed -i 's/\.with_max_results(max_results)//' "$INSPECT"
}

run_mutant "drop value_included field" mutate_drop_field
run_mutant "reverse dependent ordering" mutate_wrong_order
run_mutant "swap AtLeast to Exact" mutate_swap_omitted
run_mutant "mapping returns wrong node" mutate_mapping
run_mutant "unbounded TraceGraph repr" mutate_repr
run_mutant "wrong missing-sheet exception" mutate_exception
run_mutant "ignore max_results keyword" mutate_ignored_max_results
