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
  sed -i '/fn value_included(&self) -> bool {/,/^    }/ s/self.inner.value_included/false/' "$INSPECT"
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
mutate_m1_nested_truncation() {
  sed -i '/fn truncation_dict/,/^}/ s/out.set_item("incomplete", value.incomplete)?;/out.set_item("incomplete", false)?;/' "$INSPECT"
  sed -i '/fn truncation_dict/,/^}/ { /    match value.omitted {/,/    }/c\    let _ = value.omitted;\
    out.set_item("omitted", py.None())?;
  }' "$INSPECT"
}
mutate_m3c_sheet_blind_mapping() {
  sed -i 's/            .map(|(index, address)| (address, index))/            .map(|(index, address)| { let bare = address.rsplit('\''!'\'').next().unwrap_or(\&address).to_string(); (bare, index) })/' "$INSPECT"
  sed -i '/fn __getitem__(&self, address:/,/^    }/ s/        let index =/        let address = address.rsplit('\''!'\'').next().unwrap_or(address);\
        let index =/' "$INSPECT"
}
mutate_m4_swapped_stamp_fields() {
  sed -i '/fn mutation_revision/,/^    }/ s/self.inner.mutation_revision/self.inner.recalc_epoch/' "$INSPECT"
  sed -i '/fn recalc_epoch/,/^    }/ s/self.inner.recalc_epoch/self.inner.mutation_revision/' "$INSPECT"
}
mutate_m5_include_values() {
  sed -i '/fn inspect_cell/,/fn precedents/ s/with_include_values(include_values)/with_include_values(true)/' "$INSPECT"
}
mutate_m7_expected_stamp() {
  sed -i '/        if let Some(stamp) = expected_stamp {/,/        }/c\        if let Some(stamp) = expected_stamp {\
            let _ = stamp;\
        }' "$INSPECT"
}
mutate_m8_direction() {
  sed -i 's/            .with_direction(direction.into())/            .with_direction({ let _: core::TraceDirection = direction.into(); core::TraceDirection::Precedents })/' "$INSPECT"
}
mutate_m12_max_depth() {
  sed -i 's/            .with_max_depth(max_depth)/            .with_max_depth({ let _ = max_depth; 6 })/' "$INSPECT"
}
mutate_m18_trace_budgets() {
  sed -i '/let options = core::TraceOptions/,/with_include_values/ s/            .with_max_links(max_links)/            .with_max_links({ let _ = max_links; 1024 })/' "$INSPECT"
  sed -i '/let options = core::TraceOptions/,/with_include_values/ s/            .with_range_member_budget(range_member_budget)/            .with_range_member_budget({ let _ = range_member_budget; 256 })/' "$INSPECT"
}

run_mutant "drop value_included field" mutate_drop_field
run_mutant "reverse dependent ordering" mutate_wrong_order
run_mutant "swap AtLeast to Exact" mutate_swap_omitted
run_mutant "mapping returns wrong node" mutate_mapping
run_mutant "unbounded TraceGraph repr" mutate_repr
run_mutant "wrong missing-sheet exception" mutate_exception
run_mutant "ignore max_results keyword" mutate_ignored_max_results
run_mutant "M1 drop nested truncation content" mutate_m1_nested_truncation
run_mutant "M3c sheet-blind node mapping" mutate_m3c_sheet_blind_mapping
run_mutant "M4 swap StateStamp fields" mutate_m4_swapped_stamp_fields
run_mutant "M5 ignore inspect_cell include_values" mutate_m5_include_values
run_mutant "M7 ignore expected_stamp" mutate_m7_expected_stamp
run_mutant "M8 ignore trace direction" mutate_m8_direction
run_mutant "M12 ignore trace max_depth" mutate_m12_max_depth
run_mutant "M18 ignore trace link budgets" mutate_m18_trace_budgets
