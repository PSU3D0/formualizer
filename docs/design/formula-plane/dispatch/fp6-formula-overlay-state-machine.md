# FP6 FormulaOverlay Edit/Punchout State Machine Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: design shore-up only; no production code changes.

## Verdict

FormulaOverlay is load-bearing enough that it should receive its own local
contract before FP6.1/FP6.6 implementation. The current architecture has the
right principle:

```text
FormulaOverlay / user override > FormulaPlane span/template > legacy graph formula
user value overlay > computed overlay > base lanes
```

but agents still need a precise state machine to avoid confusing formula
authority with Arrow value overlays or eagerly splitting spans on every edit.

Recommended readiness:

| Area | Verdict | Reason |
|---|---|---|
| FP6.1 storage vocabulary | PASS with this contract | The overlay can be implemented behavior-inert as an internal store with IDs, epochs, and lookup semantics. |
| FP6.6 edit semantics | WARN | Requires state-transition, undo/rollback, dirty, and value-overlay integration tests before engine edit paths change. |
| Broad paste/structural behavior | WARN/DEFER | Design now; implement after row-run authority and basic punchout tests pass. |

## Source Reality Check

- There is no runtime `FormulaOverlay` source module yet. Existing
  `crates/formualizer-eval/src/formula_plane/` is passive FormulaPlane bridge
  infrastructure.
- Existing `EvalConfig::write_formula_overlay_enabled` and
  `computed_overlay` in `arrow_store/mod.rs` are value/result-plane concepts,
  not FormulaPlane formula-authority punchouts.
- Current Arrow read precedence is implemented as:

```text
ColumnChunk.overlay -> ColumnChunk.computed_overlay -> base lanes
```

- Current engine edit paths write graph authority first:
  - `Engine::set_cell_value` calls `graph.set_cell_value(...)` then writes the
    Arrow user/delta overlay.
  - `Engine::set_cell_formula` calls `graph.set_cell_formula_with_volatility(...)`
    then clears the Arrow user/delta overlay for that cell.
- Current rollback/action support has graph `ChangeEvent` and Arrow undo batches,
  but no FormulaPlane/FormulaOverlay undo component. FormulaOverlay mutations
  must be added to that journal before transactional edit paths modify it.
- `formula_overlay_writeback.rs` currently tests scalar formula result writeback
  into Arrow computed overlay. It is an important value-plane precedent, but it
  is not the new FormulaOverlay formula-authority layer.

## Terms And Coordinate Contract

FormulaOverlay entries should use FormulaPlane internal coordinates:

```text
sheet_id: stable engine SheetId, not sheet display name
row0/col0: zero-based placement/result coordinates, matching graph AbsCoord
region: inclusive or half-open by explicit Region type, but one convention only
```

Public APIs remain 1-based Excel-style at the boundary. Conversion must happen
before FormulaOverlay lookup/mutation.

For the first scalar span runtime, placement coordinate and result coordinate are
identical. The overlay entry should still name the scope explicitly so future
array/spill support does not inherit an ambiguous convention:

```rust
pub enum FormulaOverlayScope {
    PlacementDomain(RegionSet),
    ResultRegion(RegionSet),
}
```

M1 should only accept scalar spans and should require:

```text
FormulaOverlayScope::PlacementDomain == FormulaOverlayScope::ResultRegion
```

for any entry that masks a span placement. Non-scalar result regions should stay
legacy until the contract is extended.

## Proposed Store Shape

FormulaOverlay should be an internal store under
`crates/formualizer-eval/src/formula_plane/`.

```rust
pub(crate) struct FormulaOverlay {
    entries: GenerationalArena<FormulaOverlayRecord>,
    index: FormulaOverlayIndex,
    epoch: FormulaOverlayEpoch,
    projection_epoch: FormulaOverlayProjectionEpoch,
}

pub(crate) struct FormulaOverlayRecord {
    id: FormulaOverlayEntryId,
    generation: u32,
    sheet_id: SheetId,
    domain: PlacementDomain,
    result_region: ResultRegion,
    kind: FormulaOverlayEntryKind,
    source_span: Option<FormulaSpanRef>,
    created_at_plane_epoch: FormulaPlaneEpoch,
    overlay_epoch: FormulaOverlayEpoch,
    value_plane_epoch: Option<ValuePlaneEpoch>,
    graph_topology_epoch: Option<u64>,
    reason: FormulaOverlayReason,
}

pub(crate) struct FormulaSpanRef {
    span_id: FormulaSpanId,
    span_generation: u32,
    span_version: u32,
}
```

`FormulaOverlayRecord` is authority metadata only. It should not own cell values
except possibly small diagnostics. User-visible values live in the Arrow user
value overlay or legacy graph value path according to the current engine mode.

## Proposed Entry Variants

```rust
pub(crate) enum FormulaOverlayEntryKind {
    /// A formula definition overrides the span/template at this placement/domain.
    /// The template is interned in TemplateStore; no inline AST ownership here.
    FormulaOverride {
        template_id: FormulaTemplateId,
        template_generation: u32,
    },

    /// A user value edit masks formula authority for this placement/domain.
    /// The actual value is in the value plane.
    ValueOverride,

    /// A clear operation removes formula authority and produces current clear
    /// value semantics, normally explicit Empty in the user value overlay for a
    /// span-covered cell so old computed/base values do not resurface.
    Cleared,

    /// A legacy graph vertex owns this placement after materialization or
    /// partial demotion. FormulaPlane must not also evaluate it.
    LegacyOwned {
        vertex_id: VertexId,
        graph_epoch: u64,
    },

    /// A blocked/unsupported punchout before or instead of materialization.
    /// This is acceptable as a transient/error state but should not be used to
    /// hide an observable formula that must evaluate.
    Unsupported {
        reason: UnsupportedReason,
    },
}
```

Recommended reason labels:

```rust
pub(crate) enum FormulaOverlayReason {
    ValueEditPunchout,
    ClearPunchout,
    FormulaEditCompatibleReabsorbed,
    FormulaEditOverride,
    PasteValueBlock,
    PasteFormulaBlock,
    UnsupportedFormulaMaterialized,
    PublicApiMaterialization,
    StructuralDemotion,
    NormalizationDemotion,
    UndoRestore,
    RollbackRestore,
}
```

## Formula Authority Semantics

Formula resolution should use this sequence:

```text
1. FormulaOverlayIndex query at placement/result coordinate.
2. Exact-filter overlay records by sheet/domain/generation.
3. If an active overlay record exists:
   - FormulaOverride -> overlay formula authority.
   - ValueOverride   -> no formula authority; user value owns value plane.
   - Cleared         -> no formula authority; clear semantics own value plane.
   - LegacyOwned     -> legacy graph vertex authority.
   - Unsupported     -> fallback/error/materialize according to operation policy.
4. Else SpanDomainIndex query.
5. Else legacy graph formula lookup.
```

`ValueOverride` and `Cleared` are formula tombstones. They do not produce a
formula, but they are still formula-authority records because they prevent a
span from recomputing that placement.

## Value-Plane Semantics

FormulaOverlay never replaces Arrow value precedence:

```text
user/delta overlay > computed overlay > base lanes
```

Required value-plane effects:

| FormulaOverlay kind | Value plane requirement |
|---|---|
| `FormulaOverride` | Clear user/delta overlay for target cells so computed result can surface after recalculation. |
| `ValueOverride` | Write user value(s) to user/delta overlay; stale computed overlay may remain lower priority but should be invalidated/cleared when available. |
| `Cleared` | Write explicit `Empty` under current clear semantics for span-covered cells so lower computed/base values do not resurface. |
| `LegacyOwned` | Follow legacy graph edit semantics; clear or write Arrow overlays according to whether the materialized cell is formula or value. |
| `Unsupported` | No hidden value change; either materialize legacy or surface a counted fallback/error path. |

Important distinction:

```text
Removing an Arrow user overlay lets computed/base values surface.
Writing explicit Empty masks computed/base values.
```

Formula edit reabsorb should remove FormulaOverlay tombstones and clear user
value overlay only when the resulting formula authority will recompute the cell.
Clear/value edit should write explicit user-plane values.

## State Transition Table

The table uses these abbreviations:

```text
FA = formula-authority effect
VP = value-plane effect
DI = dirty/index invalidation
CT = counter/fallback labels
```

| Current authority | Operation | FA | VP | DI | CT |
|---|---|---|---|---|---|
| SpanPlacement | value edit scalar | Insert/replace `ValueOverride` for placement. Span no longer owns formula at that placement. | Write edited value to Arrow user/delta overlay. Preserve user overlay precedence over stale computed/base. | Bump FormulaOverlay epoch; update FormulaOverlayIndex; invalidate projection cache for source span; dirty affected result cell and downstream dependents. | `formula_overlay_value_override_created`, `value_edit_punchout`, `value_overlay_write_count` |
| SpanPlacement | value edit range/paste block | Insert region `ValueOverride` records for intersection with effective span domains; do not split spans per cell. | Bulk write values to user/delta overlay. | Bulk index update; projection cache invalidated for all intersecting spans; dirty result region union. | `formula_overlay_bulk_value_override_created`, `paste_value_block_punchout`, `bulk_overlay_region_count` |
| SpanPlacement | clear scalar | Insert/replace `Cleared` for placement. | Write explicit `Empty` under current clear semantics; ensure old computed/base value cannot resurface. | Bump overlay epoch; update index; invalidate projection cache; dirty result cell/downstream. | `formula_overlay_cleared_created`, `clear_punchout`, `explicit_empty_user_overlay_write_count` |
| SpanPlacement | clear range | Insert region `Cleared` records for span intersections; avoid per-cell span splitting. | Bulk explicit `Empty` writes for cleared cells according to current clear semantics. | Bulk index update; invalidate all intersecting span projections; dirty cleared result regions. | `formula_overlay_bulk_cleared_created`, `clear_region_punchout` |
| SpanPlacement | formula edit, exact same canonical template/dependency contract | Reabsorb: remove any existing overlay entry for the placement/domain. Span continues to own formula. | Clear user/delta overlay for target cell so computed span result can surface. | Remove overlay record; bump epoch; invalidate projection cache; dirty placement for recomputation. | `formula_overlay_reabsorbed_count`, `formula_edit_compatible_reabsorbed` |
| SpanPlacement | formula edit, supported but different template | Insert `FormulaOverride { template_id }` for scalar or region, or enqueue local repatterning to form a new span if repeated. | Clear user/delta overlay; computed output will come from override/span eval. | Update overlay index; invalidate projection cache; update dependency sidecar for override if it participates; dirty result/downstream. | `formula_overlay_formula_override_created`, `formula_edit_override`, `local_repattern_candidate_count` |
| SpanPlacement | formula edit, unsupported/dynamic/volatile/opaque | Materialize legacy vertex and insert `LegacyOwned { vertex_id }`; if materialization is deferred, insert counted `Unsupported`. | Clear user/delta overlay for formula; legacy computed write path owns results. | Update graph topology/deps; update FormulaOverlayIndex; invalidate span projection; dirty downstream through graph and sidecar. | `formula_overlay_legacy_owned_created`, `unsupported_formula_materialized`, `legacy_materialized_cells` |
| SpanPlacement | public API demands concrete legacy formula vertex | Insert `LegacyOwned { vertex_id }` before/while materializing graph vertex. | No value change except normal formula recomputation path. | Graph topology changes; overlay/domain/dependency indexes updated; dirty downstream if formula authority changes. | `public_api_materialization`, `legacy_materialized_cells` |
| ValueOverride | formula edit compatible with source span | Remove `ValueOverride` and reabsorb if exact template/dependency compatibility holds. | Clear user/delta value overlay. | Bump overlay epoch; projection cache rebuild; dirty placement/downstream. | `formula_overlay_reabsorbed_count`, `value_override_removed_for_formula` |
| ValueOverride | formula edit incompatible but supported | Replace with `FormulaOverride`. | Clear user/delta overlay. | Update overlay index payload/version; dirty placement/downstream. | `formula_overlay_value_to_formula_override`, `formula_edit_override` |
| ValueOverride | clear | Replace with `Cleared`. | Write explicit `Empty`. | Bump overlay/value epochs; dirty placement/downstream. | `formula_overlay_value_to_cleared`, `clear_punchout` |
| Cleared | value edit | Replace with `ValueOverride`. | Write edited user value. | Bump overlay/value epochs; dirty placement/downstream. | `formula_overlay_cleared_to_value_override`, `value_edit_punchout` |
| Cleared | compatible formula edit | Remove `Cleared`; reabsorb into span. | Clear explicit Empty user overlay. | Bump overlay epoch; projection cache rebuild; dirty recomputation. | `formula_overlay_reabsorbed_count`, `cleared_reabsorbed` |
| FormulaOverride | value edit | Replace with `ValueOverride`; override template no longer owns placement. | Write user value. | Remove/update override dependency entry; invalidate indexes; dirty downstream. | `formula_overlay_formula_to_value_override`, `value_edit_punchout` |
| FormulaOverride | formula edit compatible with source span | Remove `FormulaOverride` if it equals source span template; otherwise replace with new override. | Clear user/delta overlay. | Rebuild override dependency entry; dirty placement/downstream. | `formula_overlay_override_replaced_or_reabsorbed` |
| LegacyOwned | value edit | Use legacy graph `set_cell_value`; optionally remove `LegacyOwned` only if graph no longer has formula and FormulaPlane domain no longer covers, otherwise keep tombstone. | Current legacy value edit mirrors to Arrow user overlay. | Graph dirty routing plus FormulaOverlay epoch if entry changes. | `legacy_owned_value_edit`, `legacy_path_cells` |
| LegacyOwned | formula edit | Use legacy graph `set_cell_formula`; keep `LegacyOwned` unless exact reabsorb is explicitly requested and safe. | Current legacy formula edit clears Arrow user overlay. | Graph topology/deps update; dirty graph and sidecar dependents. | `legacy_owned_formula_edit`, `legacy_path_cells` |
| Any active entry | undo/rollback restore previous state | Restore previous FormulaOverlay record set from FormulaOverlay undo batch. | Restore Arrow value/computed overlays via existing Arrow undo batch or matching new undo record. | Restore/rebuild overlay index and projection cache; bump or restore epochs consistently; dirty affected regions if rollback is visible. | `formula_overlay_undo_restore`, `formula_overlay_rollback_restore` |
| Span/region demotion | demote whole span | Deactivate span; materialize legacy vertices for active effective domain or mark region legacy-owned as required. | Preserve current observable values; formulas recompute through legacy. | Remove/stale SpanDomainIndex/SpanDependencyIndex entries; clear span dirty; FormulaOverlay entries for demoted region removed or converted to `LegacyOwned`; dirty downstream. | `span_demoted`, `legacy_materialized_cells`, `demotion_reason_*` |
| Span/region demotion | demote partial region | Insert region/scalar `LegacyOwned` entries after materializing graph vertices for the partial region. | Preserve current observable values. | FormulaOverlayIndex update; projection cache subtracts region; dependency sidecar removes partial authority; dirty downstream. | `partial_span_demoted`, `legacy_materialized_cells` |

## Undo/Rollback Contract

FormulaOverlay mutations must participate in transactional rollback before engine
edit paths modify runtime FormulaPlane state.

Recommended journal addition:

```rust
pub(crate) struct FormulaOverlayUndoBatch {
    before_epoch: FormulaOverlayEpoch,
    after_epoch: FormulaOverlayEpoch,
    ops: Vec<FormulaOverlayUndoOp>,
}

pub(crate) enum FormulaOverlayUndoOp {
    Inserted { id: FormulaOverlayEntryId },
    Removed { record: FormulaOverlayRecord },
    Replaced { before: FormulaOverlayRecord, after_id: FormulaOverlayEntryId },
    IndexRebuilt { previous_epoch: FormulaOverlayProjectionEpoch },
}
```

`Engine::action_atomic` and `action_with_logger` currently account for graph and
Arrow-truth overlays. A FormulaOverlay-aware edit must add an undo component that
is applied with graph and Arrow rollback, or the edit must remain outside the
FormulaPlane runtime path.

Rollback invariants:

1. FormulaOverlay records and index contents match the restored epoch.
2. Arrow user/computed overlays match restored value semantics.
3. Span projection cache is rebuilt or marked stale.
4. Dirty entries from the failed action are removed or tagged stale by epoch.
5. No FormulaOverlay tombstone remains without its matching value-plane effect.

## Effective Span Domain And Projection

FormulaPlane evaluation must operate on the effective span domain:

```text
effective_domain(span) =
  span.domain
  - intrinsic mask
  - FormulaOverlay active punchout domains for that span/domain
```

Where active punchouts include:

```text
ValueOverride
Cleared
FormulaOverride not owned by this span
LegacyOwned
Unsupported
```

A `FormulaOverride` may be evaluated by FormulaPlane if it is represented as a
separate override template/span task, but it is still a punchout from the source
span.

Projection cache inputs:

```text
span id + generation + version
FormulaOverlay epoch
FormulaOverlayIndex epoch
intrinsic mask epoch
normalization epoch
```

Projection cache invalidation triggers:

- create/update/remove overlay entry intersecting span domain;
- span domain/version change;
- span mask change;
- demotion/materialization;
- structural transform affecting span or overlay domains;
- sheet delete/rename when sheet identity changes or tombstones.

## Bulk Operations Strategy

Bulk edits should be region-first:

```text
1. Query SpanDomainIndex for spans intersecting the edited region.
2. Query FormulaOverlayIndex for existing entries intersecting the edited region.
3. Compute intersections in FormulaPlane coordinates.
4. Insert/replace region overlay records where possible.
5. Apply value-plane bulk writes separately.
6. Invalidate projections once per affected span, not once per cell.
7. Dirty unioned result regions.
```

Do not eagerly split spans per cell for paste/clear. Splitting or merging spans
belongs to later normalization, not edit semantics.

Recommended region record policy:

- use one region `ValueOverride`/`Cleared` record when a rectangular paste/clear
  region intersects a span in a rectangular domain;
- use interval records for row-run/col-run intersections;
- fall back to sparse overlay entries only when the edited shape is genuinely
  sparse;
- defer span splitting until normalization proves it is representation-cheaper
  and semantically exact.

For formula paste:

```text
classify formulas by exact canonical template and dependency summary
same source span template -> remove punchouts/reabsorb
supported repeated new family -> create new span or region FormulaOverride
unique supported formula -> scalar FormulaOverride or legacy fallback by policy
unsupported formula -> LegacyOwned/materialize
```

## Dirty And Index Invalidation Rules

Every FormulaOverlay mutation must produce a changed result region. For scalar
M1 spans this is the edited cell/region. Later array/spill spans must map
placement changes to result regions explicitly.

Dirty flow:

```text
FormulaOverlay mutation
  -> affected result region
  -> sidecar downstream dirty routing + legacy graph dirty routing
  -> SpanDirtyStore union for affected spans
  -> graph dirty vertices for legacy dependents
```

Index invalidation:

| Mutation | FormulaOverlayIndex | SpanProjectionCache | SpanDependencyIndex | SpanDomainIndex |
|---|---|---|---|---|
| ValueOverride/Cleared insert | insert/update entry | stale affected spans | unchanged, but effective domain changes | unchanged |
| FormulaOverride insert | insert/update entry | stale source span; maybe build override deps | add/update override deps if evaluated by FormulaPlane | unchanged unless new span formed |
| LegacyOwned insert | insert/update entry | stale source span | remove/effective-domain subtract; graph owns deps | unchanged for source span geometry |
| Overlay removal/reabsorb | remove entry | stale affected span | restore source span effective deps | unchanged |
| Whole span demotion | remove or convert entries | drop cache for span | remove span entries | remove/deactivate span |
| Structural shift | transform/rebuild entries | rebuild | rebuild | rebuild |

A stale index must rebuild or produce a counted fallback before answering. It
must not silently return candidates from a prior overlay epoch.

## Counters And Labels

Minimum counters for FormulaOverlay work:

```text
formula_overlay_entries_total
formula_overlay_entries_by_kind
formula_overlay_epoch
formula_overlay_index_epoch
formula_overlay_index_entries
formula_overlay_projection_cache_invalidations
formula_overlay_projection_cache_rebuilds
formula_overlay_value_override_created
formula_overlay_cleared_created
formula_overlay_formula_override_created
formula_overlay_legacy_owned_created
formula_overlay_unsupported_created
formula_overlay_entries_removed
formula_overlay_reabsorbed_count
formula_overlay_bulk_region_records_created
legacy_materialized_cells
value_overlay_write_count
explicit_empty_user_overlay_write_count
dirty_regions_from_formula_overlay
fallback_reasons_by_label
```

Suggested fallback/demotion labels:

```text
value_edit_punchout
clear_punchout
paste_value_block_punchout
formula_edit_override
formula_edit_compatible_reabsorbed
unsupported_formula_materialized
public_api_materialization
structural_demote_span
normalization_demote_dense_exceptions
rollback_restore
stale_overlay_epoch_rebuild
```

## Test-First Acceptance List

### Unit tests under `crates/formualizer-eval/src/formula_plane/formula_overlay.rs`

```text
formula_overlay_insert_value_override_bumps_epoch
formula_overlay_insert_cleared_bumps_epoch
formula_overlay_replace_entry_preserves_generation_staleness
formula_overlay_removal_rejects_stale_entry_id
formula_overlay_bulk_region_query_returns_intersecting_entries_only
formula_overlay_region_entry_exact_filters_bucket_overreturn
formula_overlay_entries_are_keyed_by_stable_sheet_id_not_display_name
formula_overlay_value_override_has_no_inline_value_payload
formula_overlay_formula_override_uses_template_id_not_inline_ast
formula_overlay_legacy_owned_records_vertex_escape_hatch
formula_overlay_unsupported_is_counted_and_not_silent
```

### Unit tests under `crates/formualizer-eval/src/formula_plane/formula_resolution.rs`

```text
formula_resolution_prefers_value_override_tombstone_over_span
formula_resolution_prefers_cleared_tombstone_over_span
formula_resolution_prefers_formula_override_over_span
formula_resolution_prefers_legacy_owned_over_span
formula_resolution_reabsorbed_compatible_formula_returns_span
formula_resolution_outside_overlay_returns_span_then_legacy
formula_resolution_does_not_materialize_legacy_for_span_lookup
```

### Unit tests under `crates/formualizer-eval/src/formula_plane/span_projection.rs`

```text
effective_span_domain_excludes_value_override
effective_span_domain_excludes_cleared
effective_span_domain_excludes_formula_override_from_source_span
effective_span_domain_excludes_legacy_owned
effective_span_domain_reincludes_cell_after_reabsorb
projection_cache_invalidates_on_overlay_epoch_change
projection_cache_rejects_stale_span_generation
```

### Integration tests under `crates/formualizer-eval/src/engine/tests/formula_plane_overlay_state.rs`

```text
value_edit_inside_span_punches_out_formula_authority_and_writes_user_overlay
clear_inside_span_creates_cleared_punchout_and_explicit_empty
same_template_formula_edit_reabsorbs_into_span_and_clears_user_overlay
different_supported_formula_edit_creates_formula_override
unsupported_formula_edit_materializes_legacy_owned_cell
paste_values_over_span_creates_region_punchout_not_per_cell_span_splits
paste_same_template_formulas_reabsorbs_region
paste_unsupported_formulas_materializes_and_counts_legacy_cells
undo_value_edit_restores_formula_overlay_and_arrow_value_overlay
rollback_failed_formula_edit_restores_overlay_index_and_projection_cache
formula_overlay_change_dirties_downstream_legacy_dependent
formula_overlay_change_dirties_downstream_span_dependent
```

### Existing value-plane regression tests to keep in suite

```text
formula_scalar_writeback_overlays_arrow_when_enabled
computed_overlay_explicit_empty_masks_base_value
user_overlay_precedence_survives_formula_overlay_punchout
```

If the last two do not already exist in the active worktree, add them before
FormulaOverlay edit integration.

## Integration Points

### FormulaResolution

FormulaResolution owns authority ordering. It should query FormulaOverlay first
and treat `ValueOverride`/`Cleared` as final no-formula results, not as absence
that allows the span to surface.

### SpanDomainIndex

SpanDomainIndex answers only geometric span coverage. It does not know whether a
placement is punched out. FormulaOverlay projection subtracts punchouts after
span-domain lookup.

### FormulaOverlayIndex

FormulaOverlayIndex supports cell/region queries over overlay records. It may
over-return, but exact filtering must use the authoritative record domain and
current record generation.

### SpanDirtyStore

Overlay mutations create dirty regions. SpanDirtyStore unions dirty domains by
span id/version after effective-domain projection. Stale dirty entries must be
ignored after overlay or span epoch changes.

### Graph Formula Storage

`LegacyOwned` is the only FormulaOverlay variant that points at graph formula
authority. Creating it requires a real graph vertex and graph dependency update.
It must be counted and must not happen as a hidden convenience in formula lookup.

### Arrow Value/Computed Overlays

FormulaOverlay determines formula authority only. Arrow overlays determine cell
values. State transitions must explicitly pair FormulaOverlay mutations with the
required Arrow user/computed overlay effect.

### Rollback/Transactions

FormulaOverlay requires an undo batch integrated with current graph and Arrow
rollback. Until that exists, FormulaOverlay-aware edits should be test-only or
outside atomic edit paths.

## Non-Goals

- Do not replace Arrow user/computed overlays with FormulaOverlay.
- Do not store user values inside FormulaOverlay entries.
- Do not make FormulaOverlay a public API surface in FP6.1/FP6.6.
- Do not support non-scalar span result regions until placement/result scoping is
  explicitly extended.
- Do not implement span splitting as the semantic response to every edit.
- Do not add span-aware function kernels as part of FormulaOverlay.
- Do not optimize unsupported/dynamic/volatile formulas through FormulaOverlay.

## Circuit Breakers

Stop and replan if an implementation:

- stores FormulaOverlay entries in `ColumnChunk.overlay` or
  `ColumnChunk.computed_overlay`;
- treats `ValueOverride` or `Cleared` as absence and lets the source span
  recompute that placement;
- writes `Empty` when it meant to remove a user overlay, or removes a user
  overlay when it meant explicit clear masking;
- creates per-cell span splits for rectangular paste/clear instead of region
  punchouts;
- creates a graph vertex for every accepted span placement during formula lookup
  or value edits;
- materializes legacy vertices without inserting/counting `LegacyOwned` or a
  demotion reason;
- changes public/default edit behavior before an explicit opt-in gate;
- updates FormulaOverlay without updating/invalidating FormulaOverlayIndex and
  SpanProjectionCache;
- hooks FormulaOverlay edits into atomic actions without FormulaOverlay undo
  records;
- uses sheet display names as runtime FormulaOverlay authority keys;
- silently keeps optimized span authority alive after unsupported structural
  transforms.

## Recommended Phase Placement

- FP6.1: implement only the inert store, IDs, epochs, record variants, basic
  lookup, and unit tests. No engine edit behavior changes.
- FP6.3: implement FormulaOverlayIndex as one of the three sidecar indexes and
  prove exact-filtering/no-under-return behavior.
- FP6.4: span evaluator consumes effective domains that subtract FormulaOverlay
  punchouts.
- FP6.6: wire value/formula/clear/paste edit transitions into engine paths only
  after undo/rollback, dirty, value-plane, and resolution tests exist.
