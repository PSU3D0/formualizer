#!/usr/bin/env bash
set -euo pipefail

# Focused mutation gate for the semantic claims in engine::inspect and its
# bounded compressed-range visitor. Run from a clean checkout; every source is
# restored before the next mutant.
ROOT=$(git rev-parse --show-toplevel)
INSPECT="$ROOT/crates/formualizer-eval/src/engine/inspect.rs"
RANGE_DEPS="$ROOT/crates/formualizer-eval/src/engine/graph/range_deps.rs"
INSPECT_BACKUP=$(mktemp)
RANGE_DEPS_BACKUP=$(mktemp)
RESULTS=$(mktemp)
restore() {
  cp "$INSPECT_BACKUP" "$INSPECT"
  cp "$RANGE_DEPS_BACKUP" "$RANGE_DEPS"
}
trap 'restore; rm -f "$INSPECT_BACKUP" "$RANGE_DEPS_BACKUP" "$RESULTS"' EXIT

if ! git -C "$ROOT" diff --quiet || ! git -C "$ROOT" diff --cached --quiet; then
  echo "inspect mutation harness requires a clean checkout" >&2
  exit 2
fi
cp "$INSPECT" "$INSPECT_BACKUP"
cp "$RANGE_DEPS" "$RANGE_DEPS_BACKUP"

run_mutant() {
  local name=$1
  local source=$2
  local sed_expression=$3
  local test_filter=$4
  restore
  sed -i "$sed_expression" "$source"
  if cmp -s "$INSPECT_BACKUP" "$INSPECT" && cmp -s "$RANGE_DEPS_BACKUP" "$RANGE_DEPS"; then
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
# Reviewer's 16-mutant set.
run_mutant m01-canonical-casing "$INSPECT" \
  '/fn canonical_cell/,/fn canonical_area/ s/sheet: self.graph.sheet_name(sheet_id).to_string()/sheet: address.sheet.clone()/' \
  public_precedents_preserve_source_order_shape_and_first_occurrence
run_mutant m02-spill-extent-off-by-one "$INSPECT" \
  '/fn spill_role/,/fn merge_omitted/ s/end_row: er + 1/end_row: er/' \
  spill_roles_links_readers_and_pages_use_public_entry_points
run_mutant m03-revision-check-bypassed "$INSPECT" \
  '/pub fn range_page/,/^    }/ s/expected != stamp/false/' \
  whole_column_empty_extent_and_revision_checked_paging_are_semantic
run_mutant m04a-row-major-row-swapped "$INSPECT" \
  '/for offset in start..end/,/items.push/ s/offset \/ width/offset % width/' \
  range_pages_pin_row_major_order_and_exact_termination
run_mutant m04b-row-major-column-swapped "$INSPECT" \
  '/for offset in start..end/,/items.push/ s/offset % width/offset \/ width/' \
  range_pages_pin_row_major_order_and_exact_termination
run_mutant m05-next-offset-last-page "$INSPECT" \
  's/(end < total).then_some(end)/(end <= total).then_some(end)/' \
  range_pages_pin_row_major_order_and_exact_termination
run_mutant m06-incomplete-never-set "$INSPECT" \
  '/fn attach_cell_target/,/fn attach_range_targets/ s/truncation.incomplete = true/truncation.incomplete = false/' \
  trace_budgets_are_global_and_have_boundary_exactness
run_mutant m07-max-links-per-node "$INSPECT" \
  's/options.max_links.saturating_sub(links_used)/options.max_links/' \
  trace_max_links_is_global_across_nodes
run_mutant m08-compressed-cycle-convergent "$INSPECT" \
  '/fn classify_cycle_dispositions/,/Build a bounded/ s/target.disposition = LinkDisposition::Cycle/target.disposition = LinkDisposition::Convergent/' \
  compressed_self_containing_range_is_cycle_even_with_no_member_budget
run_mutant m09-stamp-omission "$INSPECT" \
  '/fn inspect_stamp/,/fn inspect_source/ s/mutation_revision: self.inspection_mutation_revision()/mutation_revision: 0/' \
  every_public_inspection_call_is_state_preserving_and_stamped
run_mutant m10-via-reverse-sort "$INSPECT" \
  's/via.sort_by(address_cmp);/via.sort_by(|left, right| address_cmp(right, left));/' \
  spill_dependent_via_is_sorted_and_ordinary_via_is_empty
run_mutant m11-name-resolution-unresolved "$INSPECT" \
  's/NamedDefinition::Literal(value) => NameResolution::Literal(value.clone())/NamedDefinition::Literal(_) => NameResolution::Unresolved/' \
  names_and_structured_tables_retain_symbolic_resolution
run_mutant m12b-stripe-category-dedup-removed "$RANGE_DEPS" \
  's/if !seen.insert(dependent) {/if false \&\& !seen.insert(dependent) {/' \
  range_stripe_category_dedup_preserves_exact_work_boundary
run_mutant m13-block-stripe-skipped "$RANGE_DEPS" \
  's/if key.stripe_type == StripeType::Block \&\& !self.config.enable_block_stripes {/if key.stripe_type == StripeType::Block {/' \
  bounded_dependents_cover_block_stripes_and_cross_sheet_ranges
run_mutant m14-dependents-omitted-always-none "$INSPECT" \
  's/known_omitted_dependent.then_some(OmittedCount::AtLeast(1))/None/' \
  zero_budget_matrix_obeys_anchor_minimum_convention
run_mutant m15-precedent-dedup-removed "$INSPECT" \
  '/impl<R: EvaluationContext> ReferenceVisitor/,/self.precedents.push/ s/if self$/if false \&\& self/' \
  public_precedents_preserve_source_order_shape_and_first_occurrence

# FormulaPlane adapter mutants. The run-union mutant substitutes one run-wide
# representative placement for the queried placement, which is the observable
# failure produced by answering from a run aggregate rather than instantiating
# the dependency template at the requested cell.
run_mutant fp-run-region-union "$INSPECT" \
  '/fn instantiated_span_references/,/fn visit_formula_plane_dependents/ s/placement\.row/span.domain.iter().next()?.row/g; /fn instantiated_span_references/,/fn visit_formula_plane_dependents/ s/placement\.col/span.domain.iter().next()?.col/g' \
  formula_plane_span_adapter_reports_per_placement_source_ordered_precedents
run_mutant fp-wrong-placement-row "$INSPECT" \
  '/fn instantiated_span_references/,/fn visit_formula_plane_dependents/ s/placement\.row/placement.row.saturating_add(1)/g' \
  formula_plane_span_adapter_reports_per_placement_source_ordered_precedents
run_mutant fp-route-span-to-legacy "$INSPECT" \
  '/fn span_placement/,/fn dirty_domain_contains/ s/if self\.engine\.config\.formula_plane_mode !=/if true || self.engine.config.formula_plane_mode !=/' \
  formula_plane_span_adapter_reports_per_placement_source_ordered_precedents
run_mutant fp-drop-source-order "$INSPECT" \
  '/fn instantiated_span_references/,/fn visit_formula_plane_dependents/ s/for dependency in \&summary\.dependencies/for dependency in summary.dependencies.iter().rev()/' \
  formula_plane_span_adapter_reports_per_placement_source_ordered_precedents
run_mutant fp-force-ast-fallback "$INSPECT" \
  '/fn instantiated_span_references/,/fn visit_formula_plane_dependents/ s/        let authority =/        return None; let authority =/' \
  formula_plane_span_adapter_reports_per_placement_source_ordered_precedents
run_mutant fp-ignore-whole-span-dirty "$INSPECT" \
  '/fn span_placement_is_dirty/,/fn instantiate_axis/ s/        if self$/        if false \&\& self/' \
  dirty_snapshots_cover_whole_span_and_incomplete_closure_fallbacks
run_mutant fp-ignore-incomplete-dirty-closure "$INSPECT" \
  's/if closure\.incomplete {/if false \&\& closure.incomplete {/' \
  dirty_snapshots_cover_whole_span_and_incomplete_closure_fallbacks

# B1 inverse mutants: both must be killed by non-tree-path cycle coverage.
run_mutant cycle-revert-to-parent-chain "$INSPECT" \
  's/Self::classify_cycle_dispositions(&mut nodes);/\/\/ parent-chain-only mutant: skip completed-graph post-pass/' \
  cycle_dispositions_are_reachability_based_for_entangled_graphs
run_mutant cycle-never-non-tree-path "$INSPECT" \
  's/source == target_index || reachability\[target_index\]\[source\]/source == target_index || false \&\& reachability[target_index][source]/' \
  cycle_dispositions_are_reachability_based_for_entangled_graphs

restore
if grep -q 'SURVIVED\|HARNESS ERROR' "$RESULTS"; then
  echo "mutation gate failed" >&2
  exit 1
fi
echo "all inspect mutants killed"
