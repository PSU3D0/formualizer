# FP6 Patterning, Normalization, and Demotion Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: design shore-up only. No production code changes.

## Verdict

Patterning is not safe for broad implementation until the runtime plan explicitly
separates passive scanner evidence from authority-grade promotion.

The current source has useful inputs:

- `template_canonical.rs` builds a deterministic canonical template payload from
  AST + placement anchor and preserves literals, relative/absolute axes,
  function names, sheet bindings, and reject labels.
- `span_store.rs` builds passive `FormulaRunStore` row/column/singleton
  descriptors from scanner cells, but uses scanner `source_template_id: String`
  and sheet display names. It is diagnostic, not runtime authority.
- `dependency_summary.rs` supports only `StaticPointwise` cell precedent
  summaries for the current accepted class, with passive run-level region
  instantiation, excluded-cell summaries, reverse queries, and demotion reasons.
- `ingest_builder.rs` still materializes one graph formula vertex/AST assignment
  per loaded formula and rewrites structured references before graph ingest.

Safe kickoff posture:

```text
FP6.1 can define stores/IDs/domains.
FP6.2 can start only after pattern acceptance tests enforce exact authority keys,
no scanner-ID authority, and counted fallback.
Normalization/repatterning should remain representation-only and should not be
wired into edits until FormulaOverlay state-machine tests exist.
```

## 1. Authority-Grade Promotion vs Passive Scanner Diagnostics

### Passive scanner evidence

Passive FormulaPlane artifacts may propose candidates, but must not decide
runtime authority.

Acceptable inputs from passive phases:

```text
candidate cells
source diagnostic template ids
contiguous row/column candidate runs
hole/exception diagnostics
dependency-summary comparison reports
materialization opportunity counters
```

Not acceptable as runtime authority:

```text
FormulaRunId as FormulaSpanId
FormulaRunStore as SpanStore
source_template_id as TemplateKey
sheet display name as SheetId
parse_ok/unsupported/dynamic/volatile booleans as complete semantic proof
passive rectangle_deferred_count as rectangle support
passive gaps as edit/punchout state
```

The active runtime builder should treat passive scanner output as a candidate
iterator only:

```text
scanner candidate group
  -> reparse/reuse parsed ASTs under runtime placement context
  -> canonicalize each placement or verify loader shared-formula metadata
  -> build authority TemplateKey
  -> build exact bounded dependency summary
  -> compare to planner/oracle where applicable
  -> accept span or fallback with reason
```

### Authority-grade acceptance criteria

A formula placement may join an accepted runtime span only if all of these hold:

1. The formula has a parsed AST available, or loader shared-formula metadata has
   been converted into an equivalent AST/template and still passes canonical
   verification.
2. `canonicalize_template(ast, placement_row, placement_col)` yields no
   authority reject reasons under the active runtime class.
3. The full canonical key payload is equal across all placements in the family.
   Hash equality alone is diagnostic only.
4. Sheet bindings are resolved to stable runtime sheet IDs/generations. Display
   names may be retained for diagnostics but must not be the identity key.
5. Function identity is supported by the active scalar semantics and the active
   dependency contract. Unknown/custom, volatile, dynamic-reference,
   reference-returning, local-environment, spill/array, named/table, structured,
   3D, external, open-range, and whole-axis cases remain legacy unless a later
   exact contract accepts them.
6. The dependency summary is exact and bounded for the active phase. For M1 this
   means the current `StaticPointwise` cell precedent class or a narrower exact
   subset.
7. The accepted placement domain has no overlap conflict with another active
   span unless normalization proves an exact merge/split.
8. FormulaOverlay punchouts are represented outside the span; they do not make
   non-equivalent formulas part of the span.
9. The builder can produce counters for accepted cells, fallback cells, spans,
   templates, avoided formula vertices, avoided AST roots, and avoided edge rows.

If any criterion fails, the cell or region remains legacy with an explicit
fallback reason.

## 2. Template Identity and Key Components

### Required `TemplateKey` components

The runtime `TemplateKey` should be stronger than the passive diagnostic key.
It should include:

```text
canonical expression payload
canonical reject-label payload, or an explicit supported-class marker
normalized function identities and arities
function-owned dependency contract fingerprint/version for every function whose
  contract affects promotion
dependency-summary class and summary fingerprint
sheet binding mode resolved to runtime SheetId/generation
relative/absolute anchor axes
literal values by value/bit pattern
reference context for by-value vs by-reference positions
parser/canonicalization version
structured-reference rewrite policy version, if structured refs are ever accepted
collect-policy fingerprint used for dependency oracle comparison
```

Existing `FormulaTemplateKey` already preserves a full payload and stable hash.
Runtime interning must compare full payloads, not just `stable_hash()` or
`diagnostic_id()`.

### Anchors and axes

Current canonicalization models references as:

```text
RelativeToPlacement { offset }
AbsoluteVc { index }
OpenStart / OpenEnd / WholeAxis / Unsupported
```

Runtime policy:

- Relative axes are allowed only when they can be instantiated for every
  placement in the candidate domain without coordinate underflow/overflow.
- Absolute axes are allowed, but dirty projection may become whole-span if an
  absolute precedent is shared by many placements.
- Mixed anchors are allowed only if dependency summaries remain exact and
  bounded; they must be covered by tests.
- Open, whole-axis, and unsupported axes reject M1 promotion.

### Sheet identity

Current passive `SheetBinding::ExplicitName { name }` is not sufficient for
runtime authority.

Runtime conversion should be:

```text
CurrentSheet -> placement sheet_id/generation
ExplicitName -> resolved sheet_id/generation at promotion time
unresolved/ambiguous/tombstoned sheet -> fallback
sheet rename/delete -> invalidate or demote affected spans/templates
```

A runtime template key may retain the display name as debug text, but equality
must use stable sheet identity.

### Function identity and contracts

For first runtime promotion, supported scalar formulas should use existing scalar
evaluator semantics; no span-aware function kernel is required.

Function participation in promotion should still be explicit:

```text
function canonical name + arity + scalar support + dependency contract result
```

Function-owned dependency contracts from `function_contract.rs` are the right
source for future range/reduction acceptance, but `None` must mean conservative
fallback. Do not introduce registry side tables or FormulaPlane sidecar name
lists for dependency authority.

### Dependency-summary compatibility

A template is span-authoritative only when its dependency summary is compatible
with the span's placement shape.

Initial M1 policy:

```text
FormulaClass::StaticPointwise
precedents: finite affine cell patterns only
no reject reasons
no planner under-approximation
```

Later accepted summaries can add finite range/reduction shapes, but only after
projection/index tests prove no under-approximation.

Compatibility requires both:

```text
forward containment: every placement's precedents are represented
reverse containment: every changed precedent region can find affected placements
```

Whole-span dirty may over-dirty after candidate discovery, but it does not relax
summary compatibility.

## 3. Initial Placement Shapes and Fallback Reasons

### Shape names

There is naming drift between docs and source:

- Active docs use `RowRun { col, row_start, row_end }` for vertical placement.
- Passive `FormulaRunShape::Column` represents a vertical run.
- Passive `FormulaRunShape::Row` represents a horizontal run.

The runtime should either adopt neutral names or document the mapping in tests:

```rust
pub enum PlacementDomain {
    VerticalRun { col: u32, row_start: u32, row_end: u32 },
    HorizontalRun { row: u32, col_start: u32, col_end: u32 },
    Rect { row_start: u32, row_end: u32, col_start: u32, col_end: u32 },
    SparseOffsets { anchor_row: u32, anchor_col: u32, offsets_id: OffsetSetId },
}
```

If the active names remain `RowRun`/`ColRun`, tests must lock their meaning.

### Shape acceptance

#### Vertical run

Accept when:

```text
same sheet_id/generation
same canonical TemplateKey
same result column
contiguous row interval
all placements valid after reference instantiation
bounded dependency summary
no overlap with an existing authoritative span
```

#### Horizontal run

Accept when:

```text
same sheet_id/generation
same canonical TemplateKey
same result row
contiguous column interval
all placements valid after reference instantiation
bounded dependency summary
no overlap conflict
```

#### Rect

Rectangles are not authority-grade in current passive `span_store.rs`; it only
counts `rectangle_deferred_count`. Runtime rect support needs its own exact
builder.

Accept only simple rectangles where:

```text
all cells in the rectangle are present or explicit FormulaOverlay holes
all included cells share the same TemplateKey and summary
row/column traversal is deterministic
result_region maps 1:1 to placement domain for scalar formulas
no unsupported internal dependencies/cycles
```

For M1, it is safer to accept vertical/horizontal runs first and keep rectangle
promotion default-off/test-only until the rect builder has no-under-merge tests.

#### Singleton

Singletons do not provide compression. They should remain legacy unless they are
temporary FormulaOverlay `FormulaOverride` records, local repattern seeds, or a
necessary bridge for demotion/materialization tests. They must not be counted as
compact span wins.

### Fallback reasons

Use explicit reason labels, not a generic unsupported bucket:

```text
parse_error
canonical_reject:dynamic_reference
canonical_reject:volatile
canonical_reject:unknown_or_custom_function
canonical_reject:local_environment
canonical_reject:reference_returning
canonical_reject:array_or_spill
canonical_reject:named_reference
canonical_reject:structured_reference
canonical_reject:three_d_reference
canonical_reject:external_reference
canonical_reject:open_range_reference
canonical_reject:whole_axis_reference
sheet_identity_unstable
dependency_summary_rejected
dependency_summary_under_approx
projection_unbounded
internal_span_dependency
shape_too_small
shape_overlap_conflict
rect_builder_not_enabled
too_many_holes
too_many_exceptions
normalization_budget_exceeded
structural_transform_unsupported
lazy_materialization_requested
```

## 4. Normalization Policies

Normalization is representation-only. It must preserve:

```text
FormulaOverlay > span/template > legacy formula
user value overlay > computed overlay > base
no under-approximated dirty routing
```

Edits should first update FormulaOverlay and value overlays. Normalization may
later choose a better representation, but it is not the edit semantic.

### Small holes

Policy:

```text
keep the span
record holes/punchouts in FormulaOverlay or span mask/projection
exclude holes from effective domain
rebuild or stale SpanDomainIndex, SpanDependencyIndex, and projection cache
```

Suggested initial thresholds:

```text
max_explicit_holes_per_span = 4096       // mirrors dependency_summary default
max_hole_density_for_keep = 10%          // tunable representation heuristic
min_span_len_after_holes = 2             // exact: otherwise no compression claim
```

The exact value is tunable; the invariant is not. If explicit holes exceed the
configured cap, split or demote. Do not silently ignore holes.

### Interval holes

Policy:

```text
if holes form one or more contiguous intervals and both sides remain useful,
  split into adjacent spans plus FormulaOverlay entries for edited cells;
else keep as sparse holes if under budget;
else demote affected region.
```

Suggested initial split rule:

```text
split when interval hole length >= 8 and both resulting spans have len >= 16
```

This is a representation heuristic and must be configurable/internal. The test
contract should assert semantic equivalence, not the exact threshold, except for
threshold-unit tests.

### Adjacent compatible span merge

Merge only when all are true:

```text
same sheet_id/generation
same TemplateKey full payload
same dependency-summary fingerprint
same placement/result shape family and aligned axis
no conflicting FormulaOverlay entries across the merged domain
same span state/eval mode
same structural epoch
merged dependency summaries remain exact and bounded
merged effective domain is representable within mask/projection budget
```

Merging adjacent spans with different source diagnostic template IDs is allowed
only if the authority `TemplateKey` and dependency summaries are equal. Merging
based on diagnostic IDs alone is forbidden.

### Many exceptions

A high exception count is not a correctness problem by itself, but it may destroy
compression and index/query efficiency.

Suggested initial demotion threshold:

```text
exception_count > 4096
or exception_density > 25%
or projected effective span cells < 2
```

Demotion can target the whole span or a subregion only if subregion boundaries
are exact and indexes/dirty can be updated atomically. Otherwise demote the whole
span.

### Paste-local repatterning

Paste should be a bulk operation:

```text
1. Query SpanDomainIndex for intersecting spans.
2. Apply FormulaOverlay/value overlay state transitions for pasted cells.
3. Mark intersecting spans/projection caches/indexes stale.
4. Build candidate groups inside paste region plus a small adjacent halo.
5. Verify every candidate with authority-grade TemplateKey and summary checks.
6. Create/merge spans for exact repeated groups.
7. Leave unsupported/unique formulas as FormulaOverride or LegacyOwned.
8. Dirty affected result regions and downstream dependents.
```

Suggested halo:

```text
same row/column contiguous neighbors until template mismatch, overlay barrier,
sheet boundary, or configured max scan cells
```

The halo size is tunable. Exact verification is not.

### Background/global optimize

A global optimize pass may rebuild spans from current formula authority state,
but it must be explicitly invoked or internal/default-off. It must not be hidden
inside ordinary cell reads.

## 5. Demotion Contract

Demotion means FormulaPlane no longer claims formula authority for a placement
or region.

### Required state updates

For each demoted span or region:

1. Mark the affected `FormulaSpan` slots as `Demoting`/`Demoted` or split them
   into valid remaining spans with new generations.
2. Remove or stale entries from `SpanDomainIndex` and `SpanDependencyIndex`.
3. Invalidate `SpanProjectionCache` entries for affected span IDs and overlay
   epochs.
4. Clear `SpanDirtyStore` entries for stale span generations.
5. Create FormulaOverlay entries for placements that now require legacy formula
   authority, usually `LegacyOwned(VertexId)` after materialization or
   `FormulaOverride(FormulaTemplateId)` for formula-only override state.
6. Materialize graph formula vertices only for demoted placements that must be
   legacy-owned. Count every created vertex, AST root, and edge row.
7. Preserve user value overlays and explicit `Empty` semantics.
8. Mark affected result regions and downstream dependents dirty via legacy graph
   routing plus FormulaPlane sidecar routing.
9. Bump FormulaPlane, span-store, dependency-index, domain-index, and overlay
   epochs as applicable.
10. Record fallback/demotion counters and reason labels.

### Demotion reason labels

Use precise labels:

```text
demotion:normalization_too_many_holes
demotion:normalization_too_many_exceptions
demotion:normalization_overlap_conflict
demotion:structural_transform_unsupported
demotion:sheet_identity_invalidated
demotion:dependency_summary_stale
demotion:internal_dependency_detected
demotion:materialization_request
demotion:evaluator_unsupported
demotion:index_epoch_stale_unrecoverable
```

### Atomicity

Demotion must be transaction-friendly. If graph materialization fails, the
runtime must roll back FormulaPlane store/index/overlay changes or leave the span
legacy-owned only after all required graph state is valid. A half-demoted span is
a correctness bug.

## 6. Suggested Thresholds and Exactness Boundaries

### Exact policy, not tunable

These must be exact and tested:

```text
TemplateKey full payload equality
stable sheet ID/generation equality
function contract support/fallback decision
dependency summary no-under-approximation
placement-domain coordinate validity
span overlap conflict detection
FormulaOverlay authority precedence
effective domain = domain - intrinsic mask - FormulaOverlay projection
dirty routing no-under-approximation
demotion state/index/dirty invalidation
no per-placement graph/AST/edge allocation for accepted spans
```

### Tunable representation heuristics

These may be configured and changed after profiling:

```text
minimum span length to promote
minimum rectangle area to promote
maximum explicit holes/exceptions before split/demote
hole-density threshold
interval-hole split threshold
adjacent-merge scan budget
paste-local halo size
background optimize batch size
rectangle detection algorithm
```

Recommended initial values:

```text
min_run_len_for_compact_authority = 2 for correctness tests, 8 for perf claims
min_rect_area_for_compact_authority = 4 for tests, higher for perf claims
max_explicit_excluded_cells = 4096
max_hole_density_for_keep = 10%
max_exception_density_for_keep = 25%
interval_split_min_hole_len = 8
interval_split_min_side_len = 16
paste_repattern_max_scan_cells = 100_000 internal/manual, lower in unit tests
```

Threshold changes must affect only representation choice. They must not change
observable values or dirty correctness.

## 7. Acceptance Tests

### Template identity and promotion

Suggested location: `crates/formualizer-eval/src/formula_plane/pattern.rs` or a
new runtime pattern module.

```text
runtime_template_key_uses_full_payload_not_hash_only
runtime_template_key_distinguishes_literal_values
runtime_template_key_distinguishes_mixed_anchors
runtime_template_key_resolves_current_sheet_to_sheet_id
runtime_template_key_rejects_unstable_explicit_sheet_name
runtime_template_key_includes_dependency_summary_fingerprint
runtime_template_key_includes_function_contract_fingerprint
passive_source_template_id_is_not_runtime_authority
scanner_candidate_group_reverified_before_promotion
unknown_custom_function_rejected_for_runtime_promotion
volatile_dynamic_spill_reference_returning_formulas_fallback
```

### Placement shapes

Suggested location: `crates/formualizer-eval/src/formula_plane/pattern.rs` and
integration tests in `crates/formualizer-eval/src/engine/tests/formula_plane_placement.rs`.

```text
vertical_run_same_template_promotes_to_one_span
horizontal_run_same_template_promotes_to_one_span
rect_same_template_promotes_only_when_every_included_cell_verified
rect_promotion_rejects_missing_unmasked_cell
singleton_does_not_count_as_compact_authority_win
overlapping_candidate_runs_choose_non_overlapping_authority
row_column_shape_names_are_not_swapped
mixed_anchor_run_promotion_matches_legacy_oracle
cross_sheet_static_ref_requires_stable_sheet_id
```

### Dependency-summary compatibility

Suggested location: `crates/formualizer-eval/src/formula_plane/dependency_summary.rs`
for pure summary tests and engine tests for planner comparison.

```text
static_pointwise_summary_required_for_m1_promotion
finite_range_summary_fallback_until_contract_enabled
dependency_summary_under_approx_rejects_promotion
dependency_summary_over_approx_allows_whole_span_dirty_when_counted
absolute_precedent_promotes_with_whole_span_dirty_projection
relative_precedent_promotes_with_same_axis_projection_later
internal_span_dependency_rejects_initial_promotion
```

### Normalization

Suggested location: `crates/formualizer-eval/src/formula_plane/normalization.rs`.

```text
single_hole_keeps_span_and_projection_excludes_cell
sparse_holes_under_budget_keep_span
holes_over_budget_demote_or_split_with_reason
interval_hole_splits_when_threshold_met
interval_hole_keeps_sparse_when_side_spans_too_small
adjacent_compatible_spans_merge
adjacent_spans_with_different_template_key_do_not_merge
adjacent_spans_with_different_dependency_summary_do_not_merge
many_exceptions_demote_with_counter
normalization_preserves_formula_resolution_outputs
normalization_rebuilds_or_stales_sidecar_indexes
```

### Paste-local repatterning

Suggested location: `crates/formualizer-eval/src/engine/tests/formula_plane_paste_repattern.rs`.

```text
paste_repeated_formula_block_forms_new_span_after_exact_verification
paste_unique_formulas_creates_overrides_not_span
paste_over_existing_span_uses_bulk_punchouts_not_per_cell_splits
paste_repattern_merges_with_adjacent_compatible_span
paste_repattern_stops_at_overlay_barrier
paste_repattern_preserves_legacy_outputs
```

### Demotion

Suggested location: `crates/formualizer-eval/src/formula_plane/demotion.rs` and
engine integration tests.

```text
demotion_removes_span_domain_index_entries
demotion_removes_span_dependency_index_entries
demotion_invalidates_projection_cache_and_dirty_generation
demotion_materializes_legacy_vertices_only_for_demoted_cells
demotion_marks_downstream_dependents_dirty
demotion_preserves_user_value_overlay_and_explicit_empty
demotion_records_reason_and_materialization_counters
demotion_rollback_restores_span_overlay_and_indexes
```

### Compact-authority counters

Suggested location: `crates/formualizer-eval/src/engine/tests/formula_plane_observability.rs`.

```text
accepted_run_counts_one_template_one_span
accepted_run_avoids_per_placement_vertices_ast_and_edges
normalization_split_updates_span_counts_without_value_change
demotion_decrements_accepted_span_cells_and_increments_fallback_cells
fallback_reason_histogram_distinguishes_canonical_dependency_and_shape_reasons
```

## 8. Integration Points

### `TemplateStore`

Patterning creates or looks up `FormulaTemplateId` by full authority key. It does
not store ASTs in spans or overlay exceptions.

### `SpanStore`

Patterning creates compact span records:

```text
id + generation + sheet_id + template_id + domain + result_region + mask/state/version
```

No inline exception maps, dependency indexes, or AST clones.

### `FormulaOverlay`

Edits and paste operations create FormulaOverlay entries first. Normalization may
remove entries only when exact reabsorption is proven.

### `SpanDomainIndex` / `SpanDependencyIndex`

Every promotion, split, merge, or demotion must update or stale indexes. Exact
filtering remains mandatory after index lookup.

### Graph ingest/build

`ingest_builder.rs` currently assigns ASTs and formula vertices per formula.
Runtime placement must avoid this path for accepted spans in scoped opt-in
fixtures, or it cannot claim materialization avoidance.

### Dependency summaries

Current `StaticPointwise` summaries are a narrow safe seed. Range/reduction
families require explicit future support through function-owned contracts and
sidecar dirty projection tests.

## 9. Non-Goals

- Do not implement production code in this shore-up phase.
- Do not replace the graph or remove legacy formula authority.
- Do not make passive `FormulaRunStore` the runtime span store.
- Do not require loader shared-formula metadata for first runtime authority.
- Do not support dynamic, volatile, spill, reference-returning, names/tables,
  structured references, 3D, external references, open ranges, or whole-axis
  references in M1.
- Do not implement span-aware function kernels as part of patterning.
- Do not optimize singleton formulas and count them as compressed authority.
- Do not split spans eagerly on every edit; FormulaOverlay punchouts are the edit
  semantic.

## 10. Circuit Breakers

Stop and replan if an implementation does any of the following:

- Promotes a span using scanner `source_template_id` without full runtime
  canonical verification.
- Uses sheet display names as runtime authority across rename/delete.
- Merges formulas based on hash equality, diagnostic IDs, formula text, or loader
  metadata without full `TemplateKey` equality.
- Accepts formulas with dependency summaries that are rejected, unbounded, or
  under-approximating.
- Treats passive rectangle diagnostics as rectangle runtime support.
- Stores holes/exceptions inline in `FormulaSpan` instead of masks/overlay-owned
  projection state.
- Splits spans per edited cell during paste/clear instead of using bulk overlay
  operations and later normalization.
- Demotes a span without staling/removing all sidecar index entries and dirty
  generations.
- Claims compact authority while creating one graph formula vertex, AST root, or
  edge set per accepted placement.
- Hides fallback, demotion, or lazy materialization counters.

## 11. Recommended Doc Updates

Add to `FORMULA_PLANE_IMPLEMENTATION_PLAN.md` before FP6.2 dispatch:

```text
FP6.2 must define authority TemplateKey components and prove passive scanner IDs
are candidate evidence only.
```

Add to FP6.2 acceptance tests:

```text
passive_source_template_id_is_not_runtime_authority
runtime_template_key_uses_full_payload_not_hash_only
static_pointwise_summary_required_for_m1_promotion
accepted_run_avoids_per_placement_vertices_ast_and_edges
```

Add to FP6.8 normalization:

```text
normalization is representation-only; edits are FormulaOverlay state changes
first, and split/merge/demote must update indexes, dirty state, epochs, and
counters atomically.
```

Add a naming guard for placement shapes so agents do not confuse passive
`FormulaRunShape::Row`/`Column` with active `RowRun`/`ColRun` semantics.
