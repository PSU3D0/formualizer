# FP6 FormulaResolution and Lazy Materialization Shore-Up

Date: 2026-05-03
Branch: `formula-plane/bridge`
Scope: report-only shore-up for FormulaPlane formula authority resolution and lazy legacy materialization. No production code changes.

## 1. Verdict

`FormulaResolution` is the first runtime seam that must be made precise before FP6.1 store work grows into engine behavior. The current architecture direction is correct, but the local contract needs sharper types and phase gates because current source is graph-vertex-centric:

- `Engine::get_cell(...)` reconstructs formula ASTs from graph `VertexId` formula storage.
- `Workbook::get_formula(...)` calls staged-formula text first, then `Engine::get_cell(...)` and pretty-prints the AST.
- `DependencyGraph` owns `cell_to_vertex`, `vertex_formulas`, and formula AST arena storage.
- `evaluate_vertex_impl(...)` evaluates graph formula vertices through `Interpreter::new_with_cell(...)`.
- `VertexEditor` and structural/undo helpers frequently ask for a concrete `VertexId` and graph AST.
- Existing FormulaPlane source is passive (`FormulaRunStore`, passive template IDs, passive dependency summaries); it is not yet runtime formula authority.

Recommendation: FP6.1 may implement `FormulaResolution` and related stores as internal/test-only vocabulary, but no engine/public lookup path should switch to it until tests prove virtual formula lookup does not materialize graph vertices.

## 2. Source Reality Check

Relevant current source observations:

- `crates/formualizer-eval/src/formula_plane/ids.rs` only defines passive `FormulaTemplateId` and `FormulaRunId`. Runtime `FormulaSpanId`, `FormulaOverlayEntryId`, generations, epochs, and materialization handles do not exist yet.
- `crates/formualizer-eval/src/formula_plane/span_store.rs` is explicitly passive and diagnostic. Its `FormulaRunDescriptor` uses sheet display names and `source_template_id`; these must not become runtime authority IDs.
- `crates/formualizer-eval/src/formula_plane/template_canonical.rs` has authority-grade canonical payloads, but explicit sheets are still display-name based in passive mode. Runtime placement must bind names to stable `SheetId`/sheet epoch.
- `crates/formualizer-eval/src/engine/sheet_registry.rs` assigns stable `SheetId` values and preserves IDs across removal by tombstoning the name. Rename keeps the same `SheetId` but changes display text.
- `crates/formualizer-eval/src/reference.rs` uses `SheetId = u16` and `CellRef { sheet_id, coord }`; coordinates are internal 0-based `Coord`, while public APIs pass 1-based row/column.
- `DependencyGraph::set_cell_formula_with_volatility(...)` creates/updates a graph vertex, stores AST in the graph arena, extracts dependency edges, marks the vertex dirty, and returns affected vertices.
- `DependencyGraph::get_formula(...)` reconstructs an AST from a graph formula vertex and is explicitly not for hot paths.
- `Engine::set_cell_formula(...)` calls the graph setter and clears any user delta overlay for that cell so computed formula output is not masked by stale user value overlay.
- `Engine::get_cell(...)` currently returns `None` if no graph vertex exists, even if a future span would cover the cell. FormulaPlane virtual formula lookup must route around this without forcing `VertexId` creation.

## 3. Proposed Internal Types

These types should live under `crates/formualizer-eval/src/formula_plane/` and remain `pub(crate)` until an explicit stable-contract decision promotes anything.

### 3.1 Sheet and cell keys

Use current engine identity internally:

```rust
pub(crate) struct FormulaCellKey {
    pub sheet_id: SheetId,
    pub row0: u32,
    pub col0: u32,
}
```

Conversion rules:

```text
public API sheet name + 1-based row/col
  -> SheetRegistry existing SheetId
  -> FormulaCellKey / CellRef with 0-based row0/col0
```

Display names are diagnostics and text-rendering inputs only. Runtime authority keys must not use sheet names. Add a FormulaPlane sheet epoch or tombstone generation before cross-sheet spans become authoritative:

```rust
pub(crate) struct RuntimeSheetKey {
    pub sheet_id: SheetId,
    pub generation: u32,
}
```

For M1 same-sheet spans, `generation` can be maintained by FormulaPlane as a local epoch that increments on sheet remove/delete/structural invalidation. Rename should not change formula authority if references are sheet-id bound, but formula text rendering should use the current registry name.

### 3.2 Generational handles

Every cross-store handle should carry generation/version data so stale query results cannot mutate live state:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaSpanRef {
    pub id: FormulaSpanId,
    pub generation: u32,
    pub version: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaTemplateRef {
    pub id: FormulaTemplateId,
    pub generation: u32,
    pub version: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaOverlayRef {
    pub id: FormulaOverlayEntryId,
    pub generation: u32,
    pub overlay_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaResolutionToken {
    pub plane_epoch: u64,
    pub overlay_epoch: u64,
    pub span_epoch: u64,
}
```

A mutating API that receives a handle must validate it or re-resolve. Stale IDs should return a typed error, never silently fall through to legacy graph authority.

```rust
pub(crate) enum FormulaPlaneLookupError {
    UnknownSheet,
    StaleHandle,
    StaleIndex,
    RemovedSpan,
    RemovedTemplate,
    RemovedOverlayEntry,
}
```

### 3.3 FormulaResolution

Formula resolution should represent formula authority, not value authority:

```rust
pub(crate) enum FormulaResolution {
    Overlay(OverlayFormulaResolution),
    SpanPlacement(SpanFormulaResolution),
    LegacyVertex(LegacyFormulaResolution),
    Empty(FormulaAbsence),
}

pub(crate) enum OverlayFormulaResolution {
    FormulaOverride {
        entry: FormulaOverlayRef,
        template: FormulaTemplateRef,
        cell: FormulaCellKey,
    },
    LegacyOwned {
        entry: FormulaOverlayRef,
        vertex_id: VertexId,
        cell: FormulaCellKey,
    },
    ValueOverride {
        entry: FormulaOverlayRef,
        cell: FormulaCellKey,
    },
    Cleared {
        entry: FormulaOverlayRef,
        cell: FormulaCellKey,
    },
    Unsupported {
        entry: FormulaOverlayRef,
        reason: UnsupportedReason,
        cell: FormulaCellKey,
    },
}

pub(crate) struct SpanFormulaResolution {
    pub span: FormulaSpanRef,
    pub template: FormulaTemplateRef,
    pub placement: PlacementCoord,
    pub cell: FormulaCellKey,
    pub resolution_token: FormulaResolutionToken,
}

pub(crate) struct LegacyFormulaResolution {
    pub vertex_id: VertexId,
    pub cell: FormulaCellKey,
}

pub(crate) enum FormulaAbsence {
    NoCell,
    ValueCell,
    ClearedByOverlay,
    ValueOverrideByOverlay,
    RemovedSheet,
}
```

Important semantic distinction:

- `Overlay::FormulaOverride` is a formula.
- `Overlay::LegacyOwned` delegates formula authority to a graph vertex and is still an overlay punchout from the span perspective.
- `Overlay::ValueOverride` and `Overlay::Cleared` mean there is no formula at that placement, even if a span geometrically covers it.
- `Empty` outside FormulaOverlay means neither span nor graph provides formula authority.

### 3.4 FormulaHandle

A handle is a short-lived, revalidatable descriptor suitable for lookup/rendering/eval planning. It must not imply graph materialization.

```rust
pub(crate) enum FormulaHandle {
    TemplatePlacement {
        template: FormulaTemplateRef,
        span: Option<FormulaSpanRef>,
        placement: PlacementCoord,
        cell: FormulaCellKey,
        resolution_token: FormulaResolutionToken,
    },
    LegacyVertex {
        vertex_id: VertexId,
        cell: FormulaCellKey,
    },
    Empty,
}
```

Suggested read APIs:

```rust
impl FormulaPlane {
    pub(crate) fn resolve_formula_at(
        &self,
        cell: FormulaCellKey,
        graph: &DependencyGraph,
    ) -> Result<FormulaResolution, FormulaPlaneLookupError>;

    pub(crate) fn formula_handle_at(
        &self,
        cell: FormulaCellKey,
        graph: &DependencyGraph,
    ) -> Result<FormulaHandle, FormulaPlaneLookupError>;

    pub(crate) fn render_formula_ast(
        &self,
        handle: &FormulaHandle,
        graph: &DependencyGraph,
    ) -> Result<Option<ASTNode>, FormulaPlaneLookupError>;
}
```

`render_formula_ast(...)` for a span placement should relocate the stored template to the requested placement and return an AST without creating a graph vertex. `render_formula_text(...)` can be a thin caller using the existing pretty-printer after AST relocation.

### 3.5 Materialization types

Lazy materialization is an explicit escape hatch:

```rust
pub(crate) struct FormulaMaterializationRequest {
    pub cell: FormulaCellKey,
    pub expected: Option<FormulaResolutionToken>,
    pub reason: MaterializationReason,
    pub policy: MaterializationPolicy,
}

pub(crate) enum MaterializationReason {
    LegacyInteropRequiresVertex,
    UnsupportedFormulaEdit,
    StructuralTransformFallback,
    PublicApiConcreteVertexDemand,
    DebugInspection,
    Demotion,
}

pub(crate) enum MaterializationPolicy {
    SingleCellOnly,
    Region { region: RegionSet },
    RejectIfNotSpanOwned,
    ReResolveIfStale,
}

pub(crate) enum FormulaMaterializationResult {
    Materialized {
        entry: FormulaOverlayRef,
        vertex_id: VertexId,
        created_vertex: bool,
        ast_materialized: bool,
    },
    AlreadyLegacy {
        vertex_id: VertexId,
    },
    NotFormula {
        absence: FormulaAbsence,
    },
    Rejected {
        reason: MaterializationRejectReason,
    },
}
```

Materialization counters must distinguish `reason`, `created_vertex`, `ast_materialized`, and whether this was a public/debug escape hatch vs an unsupported edit/demotion path.

## 4. Exact Resolution Order

Engine-level public formula lookup should preserve existing staged-formula behavior first. `Workbook::get_formula(...)` currently checks `Engine::get_staged_formula_text(...)` before graph AST lookup; FormulaPlane should not shadow that while deferred graph building is active.

For resolved/non-staged cells, internal formula resolution order is:

1. Convert external coordinates to `FormulaCellKey` using `SheetRegistry`. Unknown sheet -> no resolution or current public error semantics.
2. Query `FormulaOverlayIndex` by `FormulaCellKey` and exact-filter overlay entries.
3. If a non-stale overlay entry exists:
   - `FormulaOverride(template_id)` -> `FormulaResolution::Overlay(FormulaOverride)`.
   - `LegacyOwned(vertex_id)` -> `FormulaResolution::Overlay(LegacyOwned)` if the vertex still exists and maps to the same cell; stale vertex -> stale handle/demotion repair path.
   - `ValueOverride` -> no formula, with span punched out.
   - `Cleared` -> no formula, with span punched out.
   - `Unsupported(reason)` -> no optimized formula authority; if concrete formula exists it should be represented as `LegacyOwned`, not hidden here.
4. Query `SpanDomainIndex` by `FormulaCellKey` and exact-filter geometric coverage.
5. Validate span generation/version/template generation and effective domain membership. Effective domain is placement domain minus intrinsic mask minus FormulaOverlay projection.
6. If active, return `FormulaResolution::SpanPlacement` with template, placement coordinate, and resolution token. Do not touch `DependencyGraph::cell_to_vertex` or `vertex_formulas`.
7. Query legacy graph `get_vertex_for_cell(...)`; if the vertex exists and `vertex_has_formula(...)`, return `FormulaResolution::LegacyVertex`.
8. Otherwise return `FormulaResolution::Empty`.

This order intentionally treats `ValueOverride` and `Cleared` as formula-authority tombstones. They should prevent falling through to the span.

## 5. Coordinate And Sheet Identity Requirements

- Internal FormulaPlane lookup uses `SheetId` plus 0-based row/column, aligned with `CellRef` and graph internals.
- Public 1-based row/column conversion happens only at engine/workbook boundaries.
- Template relocation should use placement coordinates in the same 0-based space as `CellRef`; formula text rendering converts to A1 notation later.
- Runtime template keys should bind explicit sheet references to stable sheet IDs when the workbook registry is available. Display names can be preserved for diagnostics and pretty-print output.
- Sheet rename should not invalidate same-sheet or sheet-id-bound span authority by itself. It may invalidate cached formula text strings because pretty output names change.
- Sheet remove/delete must invalidate or demote spans/templates/overlays referencing the removed sheet. Existing `SheetRegistry::remove(...)` tombstones the name while preserving the numeric ID, so FormulaPlane needs a sheet tombstone/generation epoch to reject stale handles.
- Cross-sheet FormulaPlane promotion should remain disabled until explicit-sheet binding and remove/rename invalidation tests exist.

## 6. Current Call Sites That Need Care

### 6.1 Formula text / AST lookup

`Engine::get_cell(...)` currently reconstructs AST only from graph vertices. `Workbook::get_formula(...)` depends on this. FormulaPlane should add an internal virtual lookup path instead of materializing:

```text
Workbook::get_formula
  -> staged formula text, if present
  -> Engine::get_formula_text_at / get_cell via FormulaResolution
  -> pretty-print relocated virtual AST for SpanPlacement
```

Do not make `get_formula` call materialization merely to satisfy the old graph AST path.

### 6.2 Graph evaluation

`evaluate_vertex_impl(...)` is graph-vertex based. It should not be reused by creating temporary formula vertices for span placements. Span evaluation needs a separate scalar placement loop that uses the stored template and `Interpreter::new_with_cell(...)` with the target `CellRef`.

### 6.3 VertexEditor, undo, and structural operations

`VertexEditor::get_formula_ast(...)`, `set_cell_formula(...)`, structural reference adjustment, and undo logic frequently assume formulas are graph vertices. Early FP6 should not route these broad edit paths through FormulaPlane implicitly. When an operation truly requires graph mutation, it must call an explicit materialization API with reason counters.

High-risk call sites:

- cell value edit over a span: should create FormulaOverlay `ValueOverride` and user value overlay, not materialize unless a later path demands it;
- formula edit over a span: compatible formulas can reabsorb; unsupported formulas materialize or become `FormulaOverride` by explicit transition;
- structural edit: unsupported transforms should demote/materialize region explicitly, not iterate `vertices_with_formulas()` and silently miss spans.

### 6.4 Bulk ingest and staged formulas

`bulk_set_formulas(...)`, `build_graph_all(...)`, and workbook backends still hand formulas to graph-building paths. FP6.2 promotion can later intercept safe families, but FP6.1 resolution should not change those paths. Staged formula text remains its own pre-graph loader state.

### 6.5 Debug and diagnostics

Graph debug APIs that expose `VertexId` should continue to report graph vertices only unless a new FormulaPlane diagnostic view is added. Do not materialize spans for debug listing by default.

## 7. Lazy Materialization State Transition

Materialization should be explicit, counted, and transactional from FormulaPlane's perspective.

### 7.1 Preconditions

- Caller passes `FormulaMaterializationRequest` with a reason.
- FormulaPlane re-resolves the cell, validating the optional `FormulaResolutionToken`.
- If the cell is already `LegacyVertex` or `Overlay::LegacyOwned`, return `AlreadyLegacy`.
- If the cell is `ValueOverride`, `Cleared`, or `Empty`, return `NotFormula` unless policy explicitly allows formula insertion.
- If the span/template/overlay handle is stale, re-resolve or return `Rejected(StaleHandle)` according to policy.

### 7.2 Single-cell span-owned materialization

For `SpanPlacement`:

```text
1. Reconstruct relocated AST from TemplateStore for the target placement.
2. Begin a FormulaPlane mutation transaction / write guard.
3. Reserve a FormulaOverlay punchout for the cell so the span can no longer own it.
4. Create or update the graph formula vertex using existing graph formula insertion semantics.
5. Replace the reserved punchout with FormulaOverlayEntry::LegacyOwned(vertex_id).
6. Invalidate FormulaOverlayIndex, SpanProjectionCache, SpanDomain/effective-domain cache, and dirty entries for stale span generation/version as needed.
7. Dirty downstream dependents of the materialized result cell through graph routing and sidecar changed-region routing.
8. Increment materialization and fallback counters.
9. Return Materialized { entry, vertex_id, created_vertex, ast_materialized: true }.
```

Step 3 and step 5 may be implemented as a single atomic mutation in a single-threaded engine, but the semantic requirement is that no subsequent span resolution can still claim that placement after the graph vertex is created.

### 7.3 Value-plane interaction

Materializing a formula should follow the existing `Engine::set_cell_formula(...)` behavior and clear stale user delta overlay for that cell, otherwise value overlay precedence could mask the formula result. If materialization is only for an equivalent virtual span formula, the old computed value can remain visible until recalc because observable formula/value should be unchanged; if the formula changes, the old computed value must be cleared or dirtied according to existing formula-edit semantics.

A value edit is not materialization. It should instead create `FormulaOverlayEntry::ValueOverride` and write the user value overlay.

### 7.4 Region materialization / demotion

Region materialization should be a later explicit demotion operation:

```text
for placement in region effective domain:
  materialize single cell with reason Demotion or StructuralTransformFallback
then retire or shrink/demote the span
rebuild sidecar indexes
```

Do not let generic APIs materialize entire spans because they asked for one formula object.

## 8. Invariants

1. Formula resolution order is FormulaOverlay > FormulaPlane span/template > legacy graph formula.
2. Value precedence remains user/edit overlay > computed overlay > base; FormulaResolution never decides value reads from Arrow overlays.
3. `ValueOverride` and `Cleared` are formula tombstones for span placements.
4. A virtual formula lookup must not allocate a graph vertex, AST arena root, or dependency edge set.
5. Lazy materialization requires an explicit reason and counter label.
6. `LegacyOwned(vertex_id)` must point to a live graph formula vertex at the same `FormulaCellKey`; stale vertices are rejected or repaired through re-resolution.
7. Handles are short-lived; stale generation/version/epoch mismatches force re-resolution.
8. Sheet display names are not runtime authority keys.
9. Materialization invalidates FormulaOverlay projection caches and any effective-domain caches for the affected span.
10. Unsupported/dynamic/volatile/opaque spans must not be materialized as optimized authority. They stay legacy or demote with counters.

## 9. Fallback And Demotion Rules

Fallback reason labels should be specific enough to explain why compact authority was lost:

```rust
pub(crate) enum FormulaResolutionFallbackReason {
    UnsupportedDependencySummary,
    UnsupportedEvaluatorConstruct,
    DynamicOrVolatileFormula,
    InternalSpanDependency,
    StaleResolutionHandle,
    RemovedSheet,
    SheetIdentityUnavailable,
    MaterializationRequested,
    PublicApiRequiredLegacyVertex,
    StructuralTransformUnsupported,
    FormulaOverlayLegacyOwned,
    FormulaOverlayValueOverride,
    FormulaOverlayCleared,
}
```

Demotion from span authority must:

- remove/stale sidecar index entries for the demoted span/region;
- update FormulaResolution so future lookups do not return span authority;
- clear or union dirty span entries as appropriate;
- dirty downstream dependents of affected result regions;
- count materialized cells, legacy vertices created, AST roots created, and edge rows created;
- preserve public/default behavior.

## 10. Test-First Acceptance List

Suggested unit-test locations are under `crates/formualizer-eval/src/formula_plane/` unless noted.

### 10.1 FormulaResolution unit tests

Location: `crates/formualizer-eval/src/formula_plane/formula_resolution.rs`

```text
formula_resolution_prefers_overlay_formula_override_over_span_and_legacy
formula_resolution_prefers_overlay_legacy_owned_over_span
formula_resolution_value_override_returns_no_formula_and_masks_span
formula_resolution_cleared_returns_empty_even_when_span_covers_cell
formula_resolution_returns_span_placement_without_legacy_materialization
formula_resolution_outside_span_falls_back_to_legacy_vertex
formula_resolution_returns_empty_for_value_cell_without_formula
formula_resolution_rejects_stale_span_generation
formula_resolution_rejects_stale_template_generation
formula_resolution_re_resolves_after_overlay_epoch_change
```

### 10.2 Coordinate/sheet identity tests

Location: `crates/formualizer-eval/src/formula_plane/formula_resolution.rs` and later engine integration tests.

```text
formula_resolution_uses_internal_zero_based_cell_key
formula_resolution_converts_public_one_based_coords_once_at_engine_boundary
formula_resolution_uses_sheet_id_not_display_name_for_authority
formula_resolution_sheet_rename_preserves_span_authority_and_rerenders_text
formula_resolution_sheet_remove_invalidates_or_demotes_span_authority
cross_sheet_span_promotion_rejects_until_sheet_identity_contract_exists
```

### 10.3 Virtual lookup / no materialization tests

Location: `crates/formualizer-eval/src/engine/tests/formula_plane_resolution.rs`

```text
public_get_formula_on_span_cell_returns_virtual_relocated_text
public_get_formula_on_span_cell_does_not_create_graph_vertex
public_get_formula_on_span_cell_does_not_increase_formula_ast_root_count
engine_get_cell_can_return_virtual_formula_without_vertex
workbook_get_formula_preserves_staged_formula_text_precedence
formula_lookup_on_punched_out_value_cell_returns_none
formula_lookup_on_legacy_owned_punchout_returns_legacy_formula_text
```

### 10.4 Lazy materialization tests

Location: `crates/formualizer-eval/src/formula_plane/materialization.rs` plus `crates/formualizer-eval/src/engine/tests/formula_plane_materialization.rs`.

```text
materialize_span_placement_creates_one_legacy_vertex_and_overlay_punchout
materialize_span_placement_does_not_materialize_neighbor_cells
materialize_span_placement_reuses_existing_vertex_when_safe
materialize_stale_resolution_token_re_resolves_or_rejects
materialize_value_override_returns_not_formula
materialize_cleared_returns_not_formula
materialize_legacy_owned_returns_already_legacy
materialize_records_reason_and_created_vertex_counters
materialize_invalidates_formula_overlay_index_and_projection_cache
materialize_marks_downstream_dependents_dirty
```

### 10.5 Default behavior guards

Location: `crates/formualizer-eval/src/engine/tests/formula_plane_default_off.rs`

```text
formula_plane_default_off_get_cell_matches_legacy
formula_plane_default_off_get_formula_matches_legacy
formula_plane_default_off_set_formula_uses_existing_graph_path
formula_plane_default_off_no_virtual_resolution_counters_increment
```

## 11. Integration Points

### FormulaOverlay

FormulaResolution consumes FormulaOverlay as the first formula-authority layer. Overlay entries must be indexed separately from value/computed Arrow overlays and must carry enough scope to answer cell/region queries. `ValueOverride` and `Cleared` are semantic punchouts and should exclude placements from effective span domains.

### SpanDomainIndex

FormulaResolution uses `SpanDomainIndex` only for geometric ownership lookup. The index may over-return, but resolution must exact-filter against authoritative span domain and then subtract FormulaOverlay projection. SpanDomainIndex must not decide FormulaOverlay semantics.

### Graph formula storage

Legacy fallback uses `DependencyGraph::get_vertex_for_cell(...)` plus `vertex_has_formula(...)` and `get_formula(...)`/arena retrieval. Materialization uses existing graph formula insertion paths to preserve dependency extraction, structured-reference rewrite, volatility flags, dirty propagation, and AST arena semantics. This is an escape hatch, not formula lookup's normal path.

### Public/default behavior

Default behavior must remain graph-backed. FormulaPlane-enabled tests may route formula lookup through FormulaResolution, but public APIs should only observe identical formula text/value behavior. Staged formula text precedence should remain visible before graph/FormulaPlane lookup while deferred graph building is active.

## 12. Non-Goals

- No public FormulaPlane API in FP6.1.
- No graph amputation.
- No graph-native `SpanProxy` requirement for FormulaResolution.
- No materialization during ordinary formula lookup.
- No span-aware function API.
- No cross-sheet runtime authority until sheet-id/generation tests exist.
- No structural edit support beyond explicit reject/demote/materialize contracts.
- No reuse of passive `FormulaRunId` as runtime `FormulaSpanId`.

## 13. Circuit Breakers

Stop and replan if implementation does any of the following:

- `get_formula`, `get_cell`, debug lookup, or formula rendering materializes graph vertices for span-owned cells by default.
- A span-owned dense row run produces one graph formula vertex, one AST root, or one dependency edge set per placement while claiming compact authority.
- `FormulaOverlay` punchouts are stored in Arrow value/computed overlays instead of formula-plane overlay storage.
- `ValueOverride` or `Cleared` falls through to span formula authority.
- `LegacyOwned(vertex_id)` can point to a stale/moved/non-formula graph vertex without detection.
- Sheet display names become runtime authority keys for spans/templates/overlays.
- Stale FormulaResolution handles mutate current FormulaPlane state without re-resolution.
- Public/default behavior changes before an explicit opt-in gate and parity tests.
- Structural edit code iterates only graph `vertices_with_formulas()` and silently ignores span-owned formulas.
- Lazy materialization has no explicit reason counter or hides created vertices/ASTs/edges from observability.

## 14. Recommended Doc Update Summary

The active docs should absorb these local contracts in abbreviated form:

- add `FormulaResolution` variants that distinguish `FormulaOverride`, `LegacyOwned`, `ValueOverride`, and `Cleared`;
- define `FormulaCellKey` as `SheetId + 0-based row/col`, with public conversion at boundaries;
- require generational handles and stale-id rejection/re-resolution;
- state that formula lookup must render virtual relocated AST/text without graph materialization;
- state that lazy materialization is explicit, counted, and creates a FormulaOverlay punchout plus graph vertex atomically;
- add default-off and no-materialization tests to FP6.1/FP6.2 gates.
