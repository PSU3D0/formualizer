# FP6 Structural Transform Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: structural edit contract for FormulaPlane spans, formula overlays, dependency summaries, sidecar indexes, dirty state, and demotion. No source code changes are proposed here.

## Verdict

Structural support should not be part of the first FormulaPlane authority milestone except as a conservative fallback/demotion gate.

Recommended staging:

```text
M1 / FP6.4:
  structural edit while FormulaPlane enabled
    -> demote or disable affected FormulaPlane authority before/around the edit
    -> let existing graph + Arrow structural paths preserve current behavior

FP6.9:
  add exact transform support for simple shifts/shrinks/splits
    -> transform spans, overlays, dependency summaries, dirty/indexes together
    -> demote anything not proven exact
```

The source already has substantial structural machinery in the legacy path. FormulaPlane must integrate with it; it must not reimplement a parallel spreadsheet structural semantics engine first.

## Source Reality Check

Current structural behavior is graph/Arrow-first:

- `Engine::insert_rows`, `delete_rows`, `insert_columns`, and `delete_columns` in `crates/formualizer-eval/src/engine/eval.rs` call `VertexEditor`, mirror the operation into `ArrowSheet`, update row visibility where applicable, and mark topology edited.
- `VertexEditor` in `crates/formualizer-eval/src/engine/graph/editor/vertex_editor.rs` moves/deletes graph vertices, uses `ReferenceAdjuster` to rewrite formula ASTs, adjusts named ranges, marks changed formulas dirty, and returns `ShiftSummary`.
- `ArrowSheet::insert_rows`, `delete_rows`, `insert_columns`, and `delete_columns` in `crates/formualizer-eval/src/arrow_store/mod.rs` shift base lanes and existing overlays. Chunk slicing preserves both user `overlay` and `computed_overlay` for retained rows.
- Atomic actions currently allow logged row/column inserts with Arrow undo recording, but reject `delete_rows` and `delete_columns` under the conservative rollback policy.
- Sheet rename/remove paths update graph and Arrow sheet names/removal. Existing tests in `arrow_canonical_611.rs` assert that sheet deletion and structural deletes create/propagate `#REF!`, and sheet rename rewrites sheet locators.
- FormulaPlane source is still passive. There is no runtime `FormulaPlane`, `SpanStore`, `FormulaOverlay`, sidecar index, or structural transform hook yet.

Existing tests to treat as semantic oracles:

```text
crates/formualizer-eval/src/engine/tests/arrow_canonical_611.rs
crates/formualizer-eval/src/engine/tests/arrow_sparse_structural_ops.rs
crates/formualizer-eval/src/engine/tests/row_operations.rs
crates/formualizer-eval/src/engine/tests/column_operations.rs
crates/formualizer-eval/src/engine/tests/range_operations.rs
crates/formualizer-eval/src/engine/tests/transactions.rs
crates/formualizer-eval/src/engine/tests/range_dependencies.rs
```

## Coordinate And Authority Rules

Structural contracts must use one internal coordinate convention:

```text
public Engine API: 1-based rows/columns
VertexEditor / graph structural ops: 0-based AbsCoord
FormulaPlane runtime stores: choose and document one convention, preferably 0-based internal RegionKey with explicit public conversion boundaries
```

FormulaPlane structural transforms must update or invalidate every derived authority layer:

```text
SpanStore domains/result regions
TemplateStore AST/canonical key/dependency summary
FormulaOverlay punchouts/exceptions
SpanDomainIndex
SpanDependencyIndex
FormulaOverlayIndex
SpanProjectionCache
SpanDirtyStore
computed result regions / downstream dirty regions
observability counters and fallback reasons
```

A structural edit is never an index-only update.

## Initial MVP Strategy

Until FP6.9 exact structural transforms land, use a conservative fallback:

```text
on structural edit against sheet S:
  find spans whose placement/result domains are on S
  find spans whose dependency summaries mention S
  find FormulaOverlay entries on S
  demote affected spans or disable FormulaPlane authority for the workbook region
  clear/stale sidecar indexes and dirty entries
  route the edit through existing Engine/VertexEditor/Arrow paths
  count demotion reason = structural_transform_unsupported
```

Two acceptable MVP variants:

1. **Affected-sheet demotion**: demote spans whose domains or precedent summaries intersect the structural target sheet.
2. **Workbook FormulaPlane disable-on-structural**: disable FormulaPlane authority after the first unsupported structural edit and fall back to legacy for subsequent recalculation until a full rebuild/repattern pass.

Variant 1 is less disruptive but requires reliable span-domain and dependency indexes. Variant 2 is simpler and safer for an early default-off beta. Either is preferable to preserving spans after an unproven structural transform.

## Transform Classification

For FP6.9, classify each structural edit relative to both span result/placement domains and instantiated precedent regions.

```rust
pub enum StructuralRelationship {
    Before,
    After,
    IntersectsPrefix,
    IntersectsInterior,
    IntersectsSuffix,
    CoversAll,
    ContainsBoundary,
    CrossSheetPrecedent,
    SheetMetadataOnly,
}

pub enum StructuralTransformAction {
    NoOp,
    Shift,
    Expand,
    Shrink,
    Hole,
    Split,
    Remove,
    Recanonicalize,
    RebuildIndexes,
    Demote,
}
```

The operation must be exact under the current template/dependency contract or the affected span demotes.

## Operation x Relationship Matrix

The tables below describe target FP6.9 behavior. MVP behavior may demote anywhere the target action is not yet implemented.

### Placement/result span domain

| Operation relationship | Target action | Index policy | MVP policy |
|---|---|---|---|
| Insert rows before row-run/rect span on same sheet | Shift domain/result rows by `+count`; shift intrinsic masks and FormulaOverlay entries in/after span; recanonicalize template if references changed | Rebuild `SpanDomainIndex`, `FormulaOverlayIndex`, `SpanDependencyIndex`, projection cache | Demote affected sheet/spans unless simple whole-domain shift is implemented and tested |
| Insert rows after span and after all span precedents | NoOp for span domain; no template change | No rebuild required beyond global epoch if implementation uses coarse invalidation | NoOp |
| Insert rows inside row-run/rect span | Inserted cells have no span formula authority; retain old cells by split or interval hole; suffix shifts by `+count`; dirty affected result region | Rebuild all sidecar indexes and projection cache | Demote whole span |
| Insert rows exactly at span start | Treat as before-span shift if inserted rows are not intended to inherit formulas; span shifts down; inserted rows empty | Rebuild domain/overlay indexes | Demote unless tested as pure shift |
| Insert rows exactly at span end | Treat as after-span NoOp unless dependency summaries expand by formula semantics; inserted rows empty | Usually no domain index rebuild, but global epoch may bump | NoOp if no dependency impact |
| Delete rows strictly before span | Shift domain/result rows by `-deleted_count`; shift masks/overlays | Rebuild domain/overlay indexes | Demote unless simple shift implemented |
| Delete rows after span and after all precedents | NoOp | No rebuild except coarse epoch | NoOp |
| Delete rows overlapping span prefix/suffix | Shrink domain if remaining placements preserve one exact template; otherwise split or demote | Rebuild all sidecar indexes | Demote whole span |
| Delete rows inside span interior | Delete covered placements; either split into two spans, represent an interval hole, or demote | Rebuild all sidecar indexes | Demote whole span |
| Delete rows covering whole span | Remove span; remove/expire overlay entries in deleted rows; clear dirty entries | Rebuild indexes; count span removal | Remove/demote span |
| Insert columns before col-run/rect span on same sheet | Shift domain/result columns by `+count`; shift masks/overlays; recanonicalize if refs changed | Rebuild indexes | Demote unless simple shift implemented |
| Insert columns after span and precedents | NoOp | No rebuild except coarse epoch | NoOp |
| Insert columns inside col-run/rect span | Inserted cells empty; split/hole suffix; dirty affected result region | Rebuild all sidecar indexes | Demote whole span |
| Delete columns strictly before span | Shift domain/result columns by `-deleted_count`; shift masks/overlays | Rebuild indexes | Demote unless simple shift implemented |
| Delete columns overlapping span prefix/suffix/interior | Shrink/split/hole only if exact; otherwise demote | Rebuild all sidecar indexes | Demote whole span |
| Delete columns covering whole span | Remove span and matching overlay entries | Rebuild indexes; count span removal | Remove/demote span |
| Whole row/column insert before/inside/after span | Same as row/column insert above; whole-axis only describes user operation shape, not a different formula semantic | Same as above | Same as above |
| Whole row/column delete before/inside/after span | Same as row/column delete above; deletions can create `#REF!` through template refs | Same as above | Same as above |

### Precedent regions used by a span

| Operation relationship | Target action | Dependency-summary policy | MVP policy |
|---|---|---|---|
| Insert rows/cols before precedent region on same sheet | Shift instantiated precedent regions; apply existing `ReferenceAdjuster` semantics to template AST; recompute dependency summary | Recanonicalize template and rebuild `SpanDependencyIndex`; reject if summary changes to unsupported/unbounded | Demote spans mentioning target sheet unless exact recanonicalization is implemented |
| Insert rows/cols inside finite precedent range | Expand/shift range only if existing `ReferenceAdjuster` semantics prove exact; recanonicalize and recompute summary | Rebuild dependency index; dirty whole span initially | Demote |
| Insert rows/cols after precedent region | NoOp unless placement domain also shifts | No dependency index rebuild except coarse epoch | NoOp |
| Delete rows/cols before precedent region | Shift precedent region; recanonicalize/recompute | Rebuild dependency index | Demote unless exact shift tested |
| Delete rows/cols inside finite precedent range | Contract range or create `#REF!` according to existing graph semantics; exact support only if adjusted template remains supported | Rebuild dependency index; dirty whole span or demote on `#REF!` | Demote |
| Delete rows/cols covering a point precedent | Formula becomes `#REF!` or unsupported; span cannot keep optimized authority | Remove/deactivate dependency entries; dirty downstream | Demote/materialize to legacy `#REF!` path |
| Delete rows/cols covering whole finite range precedent | Usually `#REF!` or empty/contracted range depending existing adjuster behavior; support only by oracle parity | Recompute summary or demote | Demote |
| Operation on unrelated sheet | NoOp unless template has explicit cross-sheet refs to that sheet | No index rebuild except coarse epoch | NoOp |
| Operation on cross-sheet precedent sheet | Same as same-sheet precedent rules; sheet identity must use stable `SheetId`, display name diagnostic only | Rebuild dependency index for affected spans | Demote affected cross-sheet spans |
| Whole-row/whole-column precedent dependencies | Use explicit whole-axis side buckets; transform only with exact whole-axis contract | Rebuild side buckets; never encode as giant bounded intervals | Demote until whole-axis summaries are authority-grade |

### Sheet operations

| Sheet operation | Target action | Index/dirty policy | MVP policy |
|---|---|---|---|
| Rename sheet containing span domain | Keep stable `SheetId`; update diagnostics/formula text rendering; recanonicalize templates only if canonical key includes display names | Rebuild or update sheet-name diagnostic maps; mark virtual formula text cache stale | Invalidate caches; preserve span only if runtime uses stable `SheetId`; otherwise demote |
| Rename sheet used as cross-sheet precedent | Preserve dependency by stable `SheetId`; rewrite virtual formula display text to new name; recompute summary if sheet binding uses names | Rebuild dependency index if keys include display names; dirty affected spans conservatively | Demote if stable sheet identity is not implemented |
| Rename unrelated sheet | NoOp | NoOp except sheet-registry epoch | NoOp |
| Delete sheet containing span domain | Remove all spans on deleted sheet; remove overlay entries and dirty entries for that sheet | Drop index entries for deleted sheet; dirty external dependents as legacy graph does | Remove/demote spans before/with sheet delete |
| Delete sheet used as precedent by spans elsewhere | Affected formulas become `#REF!` or demote; FormulaPlane cannot keep normal dependency authority | Remove dependency index entries; dirty whole affected spans/downstream dependents | Demote/materialize affected spans to legacy or mark workbook FormulaPlane disabled |
| Delete unrelated sheet | NoOp | NoOp except sheet-registry epoch | NoOp |
| Re-add/rename sheet that heals prior `#REF!` | Repattern/rebuild only through explicit rebuild pass; do not silently heal stale spans | Clear stale indexes and rerun placement/dependency analysis if enabled | Legacy handles; FormulaPlane remains disabled/demoted until rebuild |

## Formula Relocation And Dependency Summary Recompute

Exact structural preservation requires recanonicalization, not ad hoc coordinate math.

Recommended algorithm for FP6.9:

```text
for each affected span:
  1. choose representative placement anchor(s) sufficient for the span shape
  2. reify the stored template AST at the old representative placement
  3. apply the existing ReferenceAdjuster with the same ShiftOperation as the graph
  4. transform the placement/result domain and overlay/mask coordinates
  5. canonicalize the adjusted AST at the transformed representative placement
  6. recompute the TemplateDependencySummary under the active collect policy
  7. prove every retained placement in the transformed domain maps to the same canonical template and exact bounded summary
  8. intern the adjusted template or reuse an existing template
  9. rebuild SpanDependencyIndex and SpanDomainIndex entries
  10. dirty affected result regions and downstream dependents
```

If any step cannot prove exactness, demote the affected span or region.

Special requirements:

- Absolute, relative, mixed, and explicit cross-sheet references must follow existing `ReferenceAdjuster` semantics exactly.
- Named ranges and tables remain unsupported for authority unless dependency summaries and structural relocation are exact.
- `#REF!` introduced by deletion is an unsupported optimized-authority state for MVP. Demote/materialize rather than keeping a normal span.
- Whole-axis dependencies require explicit side-bucket summaries and exact relocation support before they can survive structural edits.
- Internal span dependencies remain unsupported; structural edits must not create hidden cycles outside graph cycle detection.

## FormulaOverlay And Value Overlay Interactions

FormulaOverlay controls formula authority. Arrow overlays control values. Structural transforms must keep them separate.

### FormulaOverlay entries

- Entries on rows/columns before the operation target remain unchanged.
- Entries after an insertion shift by `+count` on the edited axis.
- Entries after a deletion shift by `-deleted_count` on the edited axis.
- Entries inside a deleted row/column window are removed or converted to logged undo records.
- Entries inside an inserted row/column window are absent by default; inserted cells do not inherit span formulas unless a later explicit fill/repattern policy says so.
- Entries whose parent span demotes must either become `LegacyOwned(VertexId)` entries for materialized formulas or be removed after legacy authority is installed.

### Value and computed overlays

The Arrow structural path already shifts user and computed overlays for retained chunks. FormulaPlane must still handle formula semantics:

- A `ValueOverride` punchout must shift/delete with the cell, matching the user value overlay.
- A `Cleared` punchout must shift/delete with the cell and continue masking span formula authority after a shift.
- Computed results for transformed spans should be considered stale after structural edits; dirty affected spans and downstream regions before reads can claim refreshed semantics.
- Inserted holes should read as empty unless user/base values are explicitly inserted by the existing Arrow path.
- On demotion, do not leave a span that can recompute over user value overrides or clears.

### Rollback/transactions

FormulaPlane structural metadata must either participate in the existing action/undo journaling or structural edits with FormulaPlane authority must take the conservative unsupported path.

MVP rule:

```text
If the current Engine action path cannot undo FormulaPlane structural metadata,
then FormulaPlane structural authority must demote/disable before the logged edit,
or the operation must be rejected under the same conservative policy as delete rows/cols.
```

## Dirty And Index Invalidation Policy

Structural operations must bump epochs and invalidate derived data even if the final action is demotion.

Recommended epochs:

```rust
FormulaPlaneEpoch
SpanStoreEpoch
TemplateStoreEpoch
FormulaOverlayEpoch
SpanDomainIndexEpoch
SpanDependencyIndexEpoch
FormulaOverlayIndexEpoch
ProjectionCacheEpoch
SpanDirtyEpoch
SheetRegistryEpoch
```

Derived index rule:

```text
incremental update only for simple, tested shifts
otherwise mark stale and rebuild from authoritative stores before query/eval
```

Mandatory invalidations:

| Event | Required invalidation |
|---|---|
| span domain shift/split/shrink/remove | `SpanDomainIndex`, projection cache, span dirty entries for old version |
| template AST adjusted | `TemplateStore` version, dependency summary, `SpanDependencyIndex`, schedule cache |
| FormulaOverlay entries shifted/deleted | `FormulaOverlayIndex`, projection cache, effective-domain cache |
| sheet rename/delete | sheet-registry epoch, all indexes using sheet keys, formula text cache |
| demotion | all sidecar entries for demoted span, span dirty state, compact-authority counters |
| structural op begins but exact transform unsupported | disable/demote affected authority before normal recalc claims FormulaPlane output |

Dirty marking requirements:

- Dirty affected span result regions after any transform, even if old computed values were shifted by Arrow.
- Dirty downstream legacy graph dependents of affected span result regions.
- Dirty downstream span dependents through `SpanDependencyIndex` if span-to-span dependencies are enabled; otherwise demote or whole-workbook fallback.
- Structural edits that delete precedents or sheets must propagate `#REF!` through downstream formulas, matching `arrow_canonical_611` behavior.

## Demotion Contract

Demotion is not just flipping a state flag.

A demotion must:

```text
1. mark FormulaSpan state = Demoted/Removed with a new generation or version
2. remove/stale SpanDomainIndex entries
3. remove/stale SpanDependencyIndex entries
4. shift/remove FormulaOverlay entries or convert to LegacyOwned entries as needed
5. clear SpanProjectionCache entries
6. clear or reject stale SpanDirtyStore entries
7. materialize legacy formula vertices only if needed for observable formula/eval semantics
8. dirty downstream dependents of the old result region
9. increment fallback/demotion counters with structural reason labels
10. ensure FormulaResolution no longer returns SpanPlacement for demoted cells
```

Fallback reason labels should distinguish:

```text
structural_transform_unsupported
structural_insert_inside_span
structural_delete_inside_span
structural_delete_precedent_ref_error
structural_sheet_delete_domain
structural_sheet_delete_precedent
structural_sheet_identity_unstable
structural_formula_recanonicalization_failed
structural_dependency_summary_unsupported_after_adjust
structural_undo_journal_unavailable
```

## Test-First Acceptance Plan

### Unit tests under FormulaPlane modules

Suggested locations:

```text
crates/formualizer-eval/src/formula_plane/structural.rs
crates/formualizer-eval/src/formula_plane/formula_overlay.rs
crates/formualizer-eval/src/formula_plane/region_index.rs
crates/formualizer-eval/src/formula_plane/runtime_store.rs
```

Tests:

```text
structural_region_insert_before_shifts_row_run_domain
structural_region_insert_after_domain_is_noop
structural_region_delete_before_shifts_row_run_domain_up
structural_region_delete_covering_domain_removes_span
structural_region_insert_inside_domain_requires_split_hole_or_demote
structural_region_delete_inside_domain_requires_shrink_split_or_demote
structural_overlay_entries_shift_with_insert_rows
structural_overlay_entries_delete_with_deleted_rows
structural_value_override_and_formula_overlay_shift_together
structural_domain_index_rebuild_drops_old_span_coordinates
structural_dependency_index_rebuild_drops_old_precedent_coordinates
structural_projection_cache_rejects_stale_overlay_epoch
structural_demote_invalidates_resolution_for_old_span_generation
```

### Dependency/template tests

Suggested locations:

```text
crates/formualizer-eval/src/formula_plane/dependency_summary.rs
crates/formualizer-eval/src/formula_plane/template_canonical.rs
crates/formualizer-eval/src/formula_plane/structural.rs
```

Tests:

```text
structural_recanonicalize_after_insert_rows_matches_reference_adjuster
structural_recanonicalize_after_insert_columns_matches_reference_adjuster
structural_delete_precedent_point_returns_ref_or_demotes
structural_insert_before_precedent_recomputes_summary_without_under_approx
structural_delete_inside_range_precedent_demotes_until_range_contract_supported
structural_cross_sheet_insert_before_precedent_demotes_or_recomputes_exactly
structural_sheet_rename_keeps_stable_sheet_id_in_template_key
structural_sheet_delete_precedent_marks_span_unsupported_ref
```

### Engine integration tests

Suggested location:

```text
crates/formualizer-eval/src/engine/tests/formula_plane_structural.rs
```

Tests:

```text
formula_plane_disabled_structural_matches_arrow_canonical_insert_rows
formula_plane_disabled_structural_matches_arrow_canonical_delete_rows
formula_plane_mvp_insert_rows_before_span_demotes_or_shifts_without_wrong_values
formula_plane_mvp_insert_rows_inside_span_demotes_and_matches_legacy
formula_plane_mvp_delete_rows_inside_span_demotes_and_matches_legacy
formula_plane_mvp_insert_columns_before_span_demotes_or_shifts_without_wrong_values
formula_plane_mvp_delete_columns_covering_span_removes_authority_and_matches_legacy
formula_plane_structural_delete_precedent_row_demotes_to_ref_and_propagates_downstream
formula_plane_structural_sheet_rename_preserves_or_demotes_with_legacy_parity
formula_plane_structural_sheet_delete_domain_removes_spans_and_overlay_entries
formula_plane_structural_sheet_delete_precedent_demotes_and_propagates_ref
formula_plane_structural_rebuilds_indexes_no_stale_domain_hits
formula_plane_structural_counters_report_demotion_reason
```

### Action/rollback tests

Suggested locations:

```text
crates/formualizer-eval/src/engine/tests/formula_plane_structural_actions.rs
crates/formualizer-eval/src/engine/tests/engine_action_rollback_615.rs
crates/formualizer-eval/src/engine/tests/engine_atomic_actions_618.rs
```

Tests:

```text
formula_plane_insert_rows_action_rolls_back_span_metadata_or_demotes_before_edit
formula_plane_delete_rows_atomic_rejected_or_demotes_consistently
formula_plane_structural_undo_restores_formula_overlay_epoch
formula_plane_structural_rollback_rebuilds_sidecar_indexes
```

### Observability assertions

Any structural FormulaPlane test should assert relevant counters:

```text
structural_ops_seen
structural_spans_shifted
structural_spans_split
structural_spans_shrunk
structural_spans_removed
structural_spans_demoted
structural_templates_recanonicalized
structural_dependency_summaries_recomputed
structural_index_rebuilds
structural_formula_overlay_entries_shifted
structural_formula_overlay_entries_deleted
structural_ref_error_demotions
fallback_reasons.structural_*
```

## Non-Goals

- Full Excel table auto-fill semantics for inserted rows/columns.
- Automatic formula fill into inserted holes inside a span.
- Preserving spans through volatile, dynamic, spill, reference-returning, table/name, 3D, external, or local-environment dependencies.
- Generic arbitrary 2D geometry transformation beyond spreadsheet-shaped row/column operations.
- Optimizing structural edit wall time before correctness/demotion counters pass.
- Public FormulaPlane structural APIs.
- Graph amputation or bypassing existing `VertexEditor`/`ReferenceAdjuster` semantics.
- Broad span-aware function kernels as part of structural support.

## Circuit Breakers

Stop and replan if any implementation:

- preserves a span after a structural edit without recanonicalizing the template and recomputing an exact dependency summary;
- treats a structural edit as only `SpanDomainIndex` or `SpanDependencyIndex` maintenance;
- uses sheet display names as runtime authority across rename/delete instead of stable sheet identity;
- lets FormulaOverlay punchouts drift away from matching user value overlay coordinates;
- leaves stale sidecar index entries for removed, shifted, or demoted spans;
- allows stale `SpanDirtyStore` entries from old span generations to schedule work;
- keeps optimized authority for formulas that should now be `#REF!` after a delete;
- silently materializes per-cell legacy vertices without `legacy_materialized_cells` and structural fallback counters;
- splits spans per cell on insert/paste/delete when a bulk demotion or interval hole is the intended policy;
- bypasses existing graph dirty propagation for downstream legacy dependents;
- changes public/default structural semantics when FormulaPlane is disabled;
- extends atomic structural operations without FormulaPlane metadata rollback or explicit conservative rejection;
- claims structural support for cross-sheet spans before sheet identity, rename, delete, and tombstone behavior are tested.

## Recommended Doc Updates

Add a short structural gate to `FORMULA_PLANE_IMPLEMENTATION_PLAN.md` before FP6.9:

```text
Before FP6.9 exact transforms, structural edits in FormulaPlane-enabled mode must
conservatively demote affected spans or disable FormulaPlane authority for the
workbook/affected sheet. No span may survive a structural edit unless template
recanonicalization, dependency-summary recomputation, FormulaOverlay coordinate
updates, dirty propagation, and index rebuilds all pass targeted tests.
```

Add a runtime architecture invariant:

```text
Structural transforms update formula authority, value storage, dependency
summaries, dirty state, and all sidecar indexes as one logical operation; if any
part cannot be transformed exactly, the affected FormulaPlane authority demotes.
```
