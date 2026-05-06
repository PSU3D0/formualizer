# Sheet-rename dirty scope investigation

> Authored from a read-only `plan` agent investigation (gpt-5.5), reviewed and
> materialized by PM. Anchors every claim in code with file:line refs.

## 1. Reproduction & timing data

### Scenario shape

Source: `crates/formualizer-bench-core/src/scenarios/s036_multi_sheet_with_sheet_rename.rs`.

- Medium scale row count: `10_000` at `s036_multi_sheet_with_sheet_rename.rs:32-35`.
- Sheets: `DataA`, `DataB`, `Sheet1` (created at `:63-68`).
- `DataA`/`DataB` value rows: populated `1..=DATA_ROWS` where `DATA_ROWS=1_000` at `:12, :70-77`. (Dispatch summary said 10k; actual is 1k. Doesn't change conclusions.)
- `Sheet1` formulas: `=DataA!A{r} + DataB!A{r}` for `1..=rows` at `:80-84`.
- Expected values: `data_a(row, completed_cycles) + data_b(row)` at `:177-178`.
- Edit cycles:
  - `DataA -> DataAA` at `:129-132`.
  - `DataAA -> DataA` at `:134-136`.
  - `DataB -> DataBB` at `:138-140`.
  - `DataBB -> DataB` at `:142-144`.
  - `DataA!A{edited_row}` value edit at `:146-148`.

### Timing data (medium 10k)

```
Off mode:
  recalc_0..3:  0.17, 0.20, 0.24, 0.27 ms  (after rename DataA->DataAA, back, DataB->DataBB, back)
  recalc_4:     0.40 ms  (after value edit)

Auth mode:
  recalc_0..3:  3.09, 2.90, 3.56, 2.75 ms  (10-18x slower)
  recalc_4:     0.17 ms
  spans=1 throughout
```

## 2. Root cause analysis

### Hot-path entry: rename

1. `Workbook::rename_sheet` at `crates/formualizer-workbook/src/workbook.rs:2016-2024`.
2. `Engine::rename_sheet(sheet_id, new_name)` at `crates/formualizer-eval/src/engine/eval.rs:1627-1652`. The relevant body:
   - read `old_name` (`:1628`)
   - update Arrow sheet storage (`:1630-1632`)
   - call `self.graph.rename_sheet(...)` (`:1635`)
   - on success rename staged formulas (`:1636-1637`)
   - mark every vertex on the renamed sheet dirty (`:1638-1643`)
   - **`self.record_formula_plane_structural_change(StructuralScope::Sheet(sheet_id))` (`:1644`)** ← problem
   - mark topology edited (`:1645`)

### Graph rename path

3. `DependencyGraph::rename_sheet` at `crates/formualizer-eval/src/engine/graph/sheets.rs:242-282`. Validates name (`:243-265`), renames the registry entry (`:266`), heals orphans (`:271`), scans formula vertices and updates AST refs from `old_name` to `new_name` (`:273-282`).
4. `SheetRegistry::rename` at `crates/formualizer-eval/src/engine/sheet_registry.rs:78-108`. **Preserves `SheetId`**; only changes name maps (`:100-106`).

### Why graph formula dependencies normally do not change on rename

- Known sheet refs are stored in arena as `SheetKey::Id(id)`, not the display name (`crates/formualizer-eval/src/engine/arena/data_store.rs:445-457` for cells, `:470-482` for ranges).
- Reconstructing an AST from arena converts `SheetKey::Id(id)` back to the **current** registry name (`:660-668` cells, `:682-690` ranges).
- `DependencyGraph::rename_sheet` calls `sheet_reg.rename` BEFORE retrieving ASTs. So `retrieve_ast` already produces the new display name; the subsequent `update_sheet_references(Some(&old_name), new_name)` finds nothing to change.
- `ASTNode::update_sheet_references` only mutates references whose current sheet string matches the target (`crates/formualizer-parse/src/parser.rs:2046-2057`).

**Conclusion**: rename preserves SheetId AND preserves stored arena references. No actual dependency state changes.

### Why Off mode finishes recalc in 0.2ms

- `Engine::rename_sheet` at `:1638-1643` marks vertices on the renamed sheet dirty. Those vertices are value cells in DataA/DataB.
- `mark_vertex_dirty` does NOT propagate to dependents (`crates/formualizer-eval/src/engine/graph/mod.rs:3598-3601`).
- `get_evaluation_vertices` filters to formula/named-formula only (`graph/mod.rs:2140-2159`).
- Therefore: legacy recalc finds **no formula work** after rename. The 0.17-0.27ms is bookkeeping/scheduling.

### Why Auth mode pays 3ms per rename

- `record_formula_plane_structural_change(StructuralScope::Sheet(sheet_id))` records `RegionPattern::whole_sheet(sheet_id)` at `eval.rs:5466-5469`.
- `FormulaAuthority::record_changed_region` pushes to `pending_changed_regions` at `crates/formualizer-eval/src/formula_plane/authority.rs:61-65`.
- Auth `evaluate_all` enters `evaluate_authoritative_formula_plane_all` at `eval.rs:6760`. With span_seed_mode=DirtyClosure and pending_changed_regions=[whole_sheet(DataA)]:
  - Build mixed schedule (`:6788`).
  - Build consumer-read indexes from active spans' read summaries at `:6908-6914`. Sheet1's span has 2 read deps: DataA col A, DataB col A.
  - `compute_dirty_closure` (`:6993-6997`) projects `whole_sheet(DataA)` through `DirtyProjectionRule::project_changed_region` (`producer.rs:923-942`).
  - For affine same-row/same-col projection (s036's shape), `WholeSheet(DataA)` projects to the **whole Sheet1 result region** (`producer.rs:444-469`, `:561-584`).
  - Span eval evaluates all 10,000 Sheet1 placements (`span_eval.rs:221-235`).

That's the 3ms: 10,000 placement re-evaluations triggered by metadata-only events.

### Verification answers

1. **Does sheet rename actually invalidate computed results?** No. SheetRegistry preserves SheetId; arena refs are by SheetId; values are unchanged.
2. **What does `RegionPattern::whole_sheet` mean to the consumer-read index?** Matches every indexed consumer read region whose `sheet_id` matches AND whose extents intersect all rows/cols (`region_index.rs:442-519`). Keyed by SheetId, NOT name.
3. **Do any actual span-eval values change in s036 between renames?** No. Same SheetId, same precedents, same values.
4. **Why does Off mode finish in 0.2ms?** Value vertices dirtied; formulas filtered out by `get_evaluation_vertices`.
5. **What's the right granularity?** None. Rename is metadata-only.
6. **Read summaries propagate by SheetId correctly across rename?** Yes. The bug isn't sheet-id confusion; it's that rename should never have produced a data-region change record at all.

## 3. Design space

### Option A: Don't record any FormulaPlane changed region for sheet rename ✅ recommended

`Engine::rename_sheet` keeps graph/Arrow/staged-formula updates but stops calling `record_formula_plane_structural_change`. No `RegionPattern` recorded. FormulaPlane pending changed regions remain empty after rename.

**Why correct:**
- Rename is display metadata only.
- SheetIds stable. Arena refs by SheetId. Read summaries by SheetId.
- Existing span evaluation reads the same source cells after rename.
- Auth evaluate_all builds mixed schedule with no pending regions and no dirty legacy formulas → no span work.

**Expected effect on s036:**
- recalc_0..3 Auth: 2.75-3.56ms → near 0.2ms (parity with Off).
- `result.computed_vertices` after rename = 0.
- recalc_4 (after value edit) remains correct because `set_cell_value` records a point changed region at `eval.rs:5377-5399`.

### Option B: Record a name-only metadata event (rejected)

Adds API surface for no current consumer. Existing formula AST reconstruction already reflects the new sheet name through `SheetKey::Id` (`data_store.rs:660-668`).

### Option C: Keep whole-sheet record but add scheduler skip path (rejected)

Treats metadata as data and compensates later. Cannot distinguish rename-only from real whole-sheet edits using `RegionPattern::WholeSheet`. Would preserve avoidable scheduler/closure work.

## 4. Recommended fix

Single-line change in `Engine::rename_sheet` at `eval.rs:1636-1645`:

**Before:**
```rust
self.rename_staged_formula_sheet(&old_name, new_name);
let sheet_vertices: Vec<VertexId> = self.graph.vertices_in_sheet(sheet_id).collect();
for v_id in sheet_vertices {
    self.graph.mark_vertex_dirty(v_id);
}
self.record_formula_plane_structural_change(StructuralScope::Sheet(sheet_id));  // <<<
self.mark_topology_edited();
```

**After:**
```rust
self.rename_staged_formula_sheet(&old_name, new_name);
let sheet_vertices: Vec<VertexId> = self.graph.vertices_in_sheet(sheet_id).collect();
for v_id in sheet_vertices {
    self.graph.mark_vertex_dirty(v_id);
}
// Sheet rename is metadata-only and preserves SheetId. References resolve by
// SheetId, so no FormulaPlane changed region is required. Removing this avoids
// re-evaluating every span that reads the renamed sheet (s036 case).
self.mark_topology_edited();
```

### Why keep legacy dirty marking

- It's not the source of the regression.
- In s036 it produces no formula evaluation work because values are filtered out by `get_evaluation_vertices`.
- Removing it would be a broader behavior change requiring its own audit.
- PM scope is FormulaPlane sheet-rename dirty scope.

## 5. Test-driven validation strategy

### Test A: rename referenced sheet, no span work

Location: `crates/formualizer-eval/src/engine/tests/formula_plane_structural.rs`.

Name: `formula_plane_authoritative_sheet_rename_is_metadata_only_for_cross_sheet_span`

- Create authoritative engine (`authoritative_engine()` helper at `:21-25`).
- Add DataA, DataB. Populate DataA!A1:A100, DataB!A1:A100.
- Ingest 100 formulas on Sheet1!A1:A100: `=DataA!A{r}+DataB!A{r}`.
- Assert: `graph_formula_vertex_count == 0`, `formula_plane_active_span_count == 1`.
- Run `evaluate_all`. Snapshot Sheet1!A1, A50, A100.
- Rename DataA → DataAA.
- Run `evaluate_all`.
- Assert: `result.computed_vertices == 0`, sampled values unchanged, `formula_plane_active_span_count == 1`.
- Rename DataAA → DataA.
- Run `evaluate_all`. Same assertions.

### Test B: real value edit after rename still dirties bounded work

Same test or companion:
- After rename cycle, edit `DataA!A50` value.
- Run `evaluate_all`.
- Assert: `result.computed_vertices <= 1`, Sheet1!A50 changes, neighbors unchanged.

### Test C: read summaries stay sheet-id based across rename

- Build the 100-row cross-sheet span.
- Record baseline counts: `formula_plane_consumer_read_entries == 2`.
- Rename DataA. Assert same counts.
- Edit DataAA!A10. Run evaluate_all. Assert only Sheet1!A10 changes.

### Perf gate: s036 Auth rename recalc within 1.5x Off

```bash
cargo run --release -p formualizer-bench-core --features formualizer_runner --bin probe-corpus -- \
  --label sheet-rename-dirty-scope-verify \
  --scale medium \
  --modes off,auth \
  --include s036-multi-sheet-with-sheet-rename
```

Pass condition: for cycles 0..3, `auth_recalc_ms < off_recalc_ms * 1.5`.

### Cargo invocations

```bash
cargo test -p formualizer-eval formula_plane_authoritative_sheet_rename_is_metadata_only_for_cross_sheet_span --lib -- --nocapture
cargo test -p formualizer-eval formula_plane_structural --lib -- --nocapture
cargo test -p formualizer-eval formula_plane_span_read_summary_resolves_cross_sheet_binding --lib -- --nocapture
```

## 6. Risks and rollback

### Risk 1: Display formula text

Formula display reconstructs ASTs from `SheetKey::Id` via current sheet registry name (`data_store.rs:660-668`). FormulaPlane `read_cell_formula_ast` reconstructs through the current sheet registry (`eval.rs:5754-5788`). Display remains correct.

`TemplateRecord::formula_text` retains original text but current formula retrieval doesn't depend on it.

### Risk 2: Future formula merging after rename

Canonical keys can include explicit sheet names through `SheetBinding::ExplicitName` (`template_canonical.rs:152-159`, `:1113-1119`). Existing spans keep their original canonical key. New formulas ingested after rename may use the new name → potentially separate span. Not a recalc correctness issue. Separate concern.

### Risk 3: Removed sheet / row-column structural edits unaffected

- `RemovedSheet` removes spans + marks active dirty + rebuilds indexes (`eval.rs:5470-5493`). Unchanged.
- `AllSheets` marks all active dirty + rebuilds indexes (`eval.rs:5495-5498`). Unchanged.
- Row/column insert/delete still use `StructuralScope::Sheet(sheet_id)` (`eval.rs:3763, 3789, 3819, 3849`). Unchanged. (For row/col the read-region change is real.)

### Rollback

Revert one line: restore `self.record_formula_plane_structural_change(StructuralScope::Sheet(sheet_id))` at `eval.rs:1644`. No data/format migration.

## 7. Open questions for PM

1. **s036 fixture**: dispatch said DataA/DataB have 10k value rows; actual is 1k (`:12`). Doesn't affect this fix. Worth fixing the scenario for consistency? Separate issue.
2. **Perf gate ownership**: should the 1.5x assertion live in the probe harness, CI, or PM benchmark gate? Defer to existing convention.
3. **Span merging across rename**: spans retain canonical keys with old explicit names. Future formulas with new names may not merge. Out of scope for this fix.

PM decision: **Ship Option A. The other questions are non-blocking.**
