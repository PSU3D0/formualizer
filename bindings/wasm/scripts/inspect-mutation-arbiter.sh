#!/usr/bin/env bash
set -euo pipefail

# Mutation gate for the WASM inspection transcription layer. Each mutant is
# applied to a clean checkout, exercised through wasm-pack's Node runner, and
# restored before the next mutant.
ROOT=$(git rev-parse --show-toplevel)
INSPECT="$ROOT/bindings/wasm/src/inspect.rs"
ERRORS="$ROOT/bindings/wasm/src/errors.rs"
INSPECT_BACKUP=$(mktemp)
ERRORS_BACKUP=$(mktemp)
RESULTS=$(mktemp)

restore() {
  cp "$INSPECT_BACKUP" "$INSPECT"
  cp "$ERRORS_BACKUP" "$ERRORS"
}

cleanup() {
  restore
  rm -f "$INSPECT_BACKUP" "$ERRORS_BACKUP" "$RESULTS"
}
trap cleanup EXIT

if ! git -C "$ROOT" diff --quiet || ! git -C "$ROOT" diff --cached --quiet; then
  echo "WASM inspection mutation arbiter requires a clean checkout" >&2
  exit 2
fi

cp "$INSPECT" "$INSPECT_BACKUP"
cp "$ERRORS" "$ERRORS_BACKUP"

run_mutant() {
  local name=$1
  local source=$2
  local expression=$3
  restore
  sed -i "$expression" "$source"
  if cmp -s "$INSPECT_BACKUP" "$INSPECT" && cmp -s "$ERRORS_BACKUP" "$ERRORS"; then
    echo "$name: HARNESS ERROR (mutation did not apply)" | tee -a "$RESULTS"
    return
  fi
  if (cd "$ROOT/bindings/wasm" && wasm-pack test --node --test inspect_wasm) \
    >"/tmp/formualizer-wasm-inspect-mutant-$name.log" 2>&1; then
    echo "$name: SURVIVED" | tee -a "$RESULTS"
  else
    echo "$name: KILLED" | tee -a "$RESULTS"
  fi
}

run_mutant drop-value-included "$INSPECT" \
  's/value_included: value.value_included/value_included: false/'
run_mutant lossy-number-stamp "$INSPECT" \
  's/mutation_revision: String,/mutation_revision: f64,/; s/mutation_revision: value.mutation_revision.to_string()/mutation_revision: value.mutation_revision as f64/'
run_mutant swap-omitted-tags "$INSPECT" \
  's/^    Exact {/    #[serde(rename = "atLeast")]\n    Exact {/; s/^    AtLeast {/    #[serde(rename = "exact")]\n    AtLeast {/'
run_mutant wrong-staleness-casing "$INSPECT" \
  's/^    Current,/    #[serde(rename = "Current")]\n    Current,/'
run_mutant ignore-max-results "$INSPECT" \
  's/options = options.with_max_results(value)/options = options.with_max_results(256)/'
run_mutant swap-sheet-error-code "$ERRORS" \
  's/InspectError::SheetNotFound { .. } => "SHEET_NOT_FOUND"/InspectError::SheetNotFound { .. } => "INVALID_ADDRESS"/'
run_mutant drop-formula-provenance "$INSPECT" \
  's/Ok((WireLinkKind::Formula, Some(wire_provenance(provenance)?)))/Ok((WireLinkKind::Formula, None))/'

# Fresh adversarial battery F1-F7, F12, F13, and F16. F8 is excluded because
# no current engine error carries a distinct display string and message-free
# code/display are near-equivalent. F18 is equivalent: core never emits an
# omitted count with incomplete=false.
run_mutant F1-swap-rangearea-start-fields "$INSPECT" \
  '/impl From<&RangeArea> for WireRangeArea/,/^}/ { s/start_row: value.start_row/start_row: value.start_column/; s/start_column: value.start_column/start_column: value.start_row/; }'
run_mutant F2-ignore-rangepage-offset "$INSPECT" \
  's/options = options.with_offset(parse_safe_u64(value, "offset")?)/options = options.with_offset(parse_safe_u64(value, "offset")? * 0)/'
run_mutant F3-drop-dependent-via "$INSPECT" \
  's/via: dependent.via.iter().map(Into::into).collect()/via: Vec::new()/'
run_mutant F4-reverse-trace-roots "$INSPECT" \
  's/report.roots.iter().map(|root| root.0)/report.roots.iter().rev().map(|root| root.0)/'
run_mutant F5-ignore-trace-include-values "$INSPECT" \
  '/fn trace_options/,/^}/ { s/if let Some(value) = raw.include_values/if let Some(_value) = raw.include_values/; s/options = options.with_include_values(value)/options = options.with_include_values(true)/; }'
run_mutant F6-ignore-trace-max-nodes "$INSPECT" \
  '/fn trace_options/,/^}/ s/options = options.with_max_nodes(value)/options = options.with_max_nodes(value.max(512))/'
run_mutant F7-ignore-precedent-max-work "$INSPECT" \
  '/fn precedent_options/,/^}/ s/options = options.with_max_work(parse_safe_u64(value, "maxWork")?)/let _ = parse_safe_u64(value, "maxWork")?/'
run_mutant F12-ignore-rangepage-include-values "$INSPECT" \
  '/fn range_page_options/,/^}/ { s/if let Some(value) = raw.include_values/if let Some(_value) = raw.include_values/; s/options = options.with_include_values(value)/options = options.with_include_values(true)/; }'
run_mutant F13-ignore-dependents-max-work "$INSPECT" \
  '/fn dependents_options/,/^}/ s/options = options.with_max_work(parse_safe_u64(value, "maxWork")?)/let _ = parse_safe_u64(value, "maxWork")?/'
run_mutant F16-invalid-options-code-swap "$ERRORS" \
  's/InspectError::InvalidOptions { .. } => "INVALID_OPTIONS"/InspectError::InvalidOptions { .. } => "SHEET_NOT_FOUND"/'
run_mutant skip-explicit-unknown-key-enforcement "$INSPECT" \
  '/fn parse_object/,/^}/ s/reject_unknown_keys(&raw, context, allowed)/Ok(())/'

restore
if grep -q 'SURVIVED\|HARNESS ERROR' "$RESULTS"; then
  echo "WASM inspection mutation gate failed" >&2
  exit 1
fi
echo "all WASM inspection mutants killed"
