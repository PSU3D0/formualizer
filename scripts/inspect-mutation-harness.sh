#!/usr/bin/env bash
set -euo pipefail

# Focused mutation gate for the semantic claims in engine::inspect.
# Run from a clean checkout. Each mutant is restored before the next one.
ROOT=$(git rev-parse --show-toplevel)
SOURCE="$ROOT/crates/formualizer-eval/src/engine/inspect.rs"
BACKUP=$(mktemp)
RESULTS=$(mktemp)
trap 'cp "$BACKUP" "$SOURCE"; rm -f "$BACKUP" "$RESULTS"' EXIT

if ! git -C "$ROOT" diff --quiet || ! git -C "$ROOT" diff --cached --quiet; then
  echo "inspect mutation harness requires a clean checkout" >&2
  exit 2
fi
cp "$SOURCE" "$BACKUP"

run_mutant() {
  local name=$1
  local sed_expression=$2
  local test_filter=$3
  cp "$BACKUP" "$SOURCE"
  sed -i "$sed_expression" "$SOURCE"
  if cmp -s "$BACKUP" "$SOURCE"; then
    echo "$name: HARNESS ERROR (mutation did not apply)" | tee -a "$RESULTS"
    return 2
  fi
  if cargo test -q -p formualizer-eval "$test_filter" >"/tmp/formualizer-inspect-mutant-$name.log" 2>&1; then
    echo "$name: SURVIVED" | tee -a "$RESULTS"
  else
    echo "$name: KILLED" | tee -a "$RESULTS"
  fi
}

cd "$ROOT"
run_mutant disposition-confusion \
  '/fn attach_cell_target/,/fn attach_range_targets/ s/LinkDisposition::Convergent/LinkDisposition::Cycle/' \
  public_trace_distinguishes_diamond_convergence_and_cycles
run_mutant dedup-order \
  '/impl<R: EvaluationContext> ReferenceVisitor/,/source.visit_declared_references/ s/self.precedents.push(Precedent/self.precedents.insert(0, Precedent/' \
  public_precedents_preserve_source_order_shape_and_first_occurrence
run_mutant budget-off-by-one \
  '/impl<R: EvaluationContext> ReferenceVisitor/,/source.visit_declared_references/ s/self.precedents.len() >= self.max_links/self.precedents.len() > self.max_links/' \
  trace_budgets_are_global_and_have_boundary_exactness
run_mutant staleness-never-vs-dirty \
  '/fn snapshot_for_key/,/pub fn inspect_cell/ s/if cached_value.is_none()/if false \&\& cached_value.is_none()/' \
  snapshots_are_honest_for_empty_never_evaluated_and_dirty_cached_formulas
run_mutant stamp-omission \
  '/fn inspect_stamp/,/fn inspect_source/ s/mutation_revision: self.inspection_mutation_revision()/mutation_revision: 0/' \
  every_public_inspection_call_is_state_preserving_and_stamped
run_mutant spill-via-dropped \
  '/let mut dependents: Vec<_>/,/dependents.sort_by/ s/Dependent { cell, via }/Dependent { cell, via: Vec::new() }/' \
  spill_roles_links_readers_and_pages_use_public_entry_points
run_mutant omitted-lower-bound-as-exact \
  '/fn collect_dependents/,/pub fn dependents/ s/OmittedCount::AtLeast/OmittedCount::Exact/g' \
  public_dependents_include_direct_finite_and_infinite_range_readers

cp "$BACKUP" "$SOURCE"
if grep -q 'SURVIVED\|HARNESS ERROR' "$RESULTS"; then
  echo "mutation gate failed" >&2
  exit 1
fi
echo "all inspect mutants killed"
