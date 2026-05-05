# Structural-op span handling under FormulaPlane AuthoritativeExperimental

Investigation memo authored before the fix landed. Recorded here for future
reference. The recommended approach (Option A: pre-structural drop-and-demote)
was implemented and validated.

## 1. Problem statement

FormulaPlane `AuthoritativeExperimental` mishandled structural row/column
operations when active spans existed.

Two distinct symptoms:

1. **Correctness**: span outputs continued to evaluate and write at their
   pre-structural `PlacementDomain` coordinates. For column insert/delete this
   was visibly wrong: the Arrow store and legacy graph moved cells/columns,
   but FormulaPlane spans retained absolute old coordinates and wrote
   computed results back into old positions.
2. **Performance**: structural ops caused broad span recomputation instead of
   bounded dirty work. `mark_all_active_spans_dirty()` bumped `indexes_epoch`
   forcing `WholeAll`, OR a whole-sheet changed region produced a conservative
   dirty closure that demoted every span to whole-recompute.

Key code observations (pre-fix):

- Span domains are absolute coordinates (`PlacementDomain::{RowRun, ColRun, Rect}`
  in `formula_plane/runtime.rs`).
- Templates store an arena AST + 1-indexed `origin_row/origin_col`
  (`TemplateRecord` in `formula_plane/runtime.rs`).
- Span evaluation uses current span domain coordinates directly
  (`SpanEvaluator::evaluate_task` in `formula_plane/span_eval.rs`).
- Writes go through `SpanComputedWriteSink::push_cell(placement, value)` which
  writes to placement coords (stale post-op).
- `Engine::insert_columns` direct path records `StructuralScope::Sheet`
  (just changed-region tracking, no span removal).
- `edit_with_logger` path catches `VertexMoved`/`FormulaAdjusted` events and
  routes them as `StructuralScope::AllSheets` which DOES
  `mark_all_active_spans_dirty()` + `rebuild_indexes()` — but neither shifts
  nor removes spans.

## 2. Current data flow on a structural op (pre-fix)

Trace: `Engine::insert_columns(sheet, 3, 1)` against a workbook with active
3-column span family.

### Legacy graph

`VertexEditor::insert_columns`:
- Collects vertices in sheet whose `coord.col() >= before`.
- Moves those right by `count`, emits `VertexMoved` events.
- Iterates `vertices_with_formulas`, applies `ReferenceAdjuster::adjust_ast`
  to each, writes adjusted ASTs back.
- Adjusts named ranges.

Important nuance: accepted FormulaPlane span cells DO NOT have legacy graph
formula vertices (placement counts them as `formula_vertices_avoided`). So
`VertexEditor` shifts only existing graph vertices, NOT the accepted span
cells themselves.

### Arrow store

`asheet.insert_columns(before0, count)` inserts empty columns and shifts old
columns right. Computed overlays attached to columns shift WITH the columns.
For the repro: B (cached A+1 outputs) stays at B; old C (cached A*2 outputs)
moves to D; old D (cached A-3 outputs) moves to E; new C is empty.

### FormulaAuthority

Direct path: `record_changed_region(whole_sheet)`. Spans are NOT removed,
shifted, or modified. Templates are NOT adjusted. Read summaries are NOT
regenerated.

Logged path: `mark_all_active_spans_dirty` bumps `indexes_epoch` (forces
`WholeAll`), records each active span's CURRENT (stale) result region as a
changed region.

### evaluate_all after the op

`active_span_count() > 0` (spans not removed) → enters FP path.
`SpanEvaluator::evaluate_task` for each active span:
- `placements = span.domain.iter()` — STALE coords.
- For each placement: row_delta computed against template origin (correct
  given the placement, but the placement is stale).
- `evaluate_arena_ast_with_offset(template.ast_id, ...)` reads from the arrow
  store at coords `(placement.row, placement.col)` — but those coords now
  point at SHIFTED data.
- `sink.push_cell(placement, value)` writes back to STALE coords.

The visible bug:
- C5 (placement was originally col=2 0-indexed, now points at col=2 which is
  actually col=3 in Excel, where new C should be empty): span re-evaluates
  =A5*2 = 10 and writes to OLD col=2 → Excel C5 = 10. WRONG; should be empty.
- D5 (was at col=3 0-indexed, now col=3 which Excel calls D5; legacy formula
  was =A5*2 originally and shifted to here, but legacy now says =A5*2): D5 =
  10. But also: the span output for col=3 was written. Race depends on order.

## 3. Why row insert "looked broadly correct" but column insert broke

For row insert: spans are vertical (RowRun, single column). Arrow row insertion
shifts cached computed results down. Spans still write to old row interval.
Coincidentally most values still look right because the formulas like =A{r}+1
read column A which ALSO shifted along the row axis. Inserted-row gap gets
filled with 0-valued span outputs (A{r}=empty → 0*2=0) instead of None (Off
semantics).

For column insert: span domain's column is FIXED. Arrow column shifts move
cached values right. Spans write to OLD column position which now should be
empty. Visible duplicate values across adjacent columns.

## 4. Options considered

### Option A: Drop-and-demote (RECOMMENDED, IMPLEMENTED)

Drop affected spans BEFORE structural op reaches `VertexEditor`. For each
placement, materialize a per-cell formula AST (anchored to that placement)
via the relocation primitive, ingest it as a legacy graph formula vertex,
remove the span, then let `VertexEditor` do its normal shift + AST adjustment.

Pros:
- Fixes correctness completely (no stale span writes possible).
- Reuses existing battle-tested `ReferenceAdjuster` for formula shift.
- No arena mutation.
- No new public API.
- Performance recovers to legacy-equivalent.

Cons:
- Loses span optimization for affected sheets.
- Future re-promotion is a separate workstream.

### Option B: Shift span domains in lockstep

Update span.domain, result_region, read summaries, region indexes, and
template ASTs (or template offset metadata) to follow the structural op.

Pros:
- Preserves span optimization across structural ops.

Cons:
- Template AST is interned/shared in the arena. Cannot mutate. Requires
  either new ASTs (B1) or runtime offset metadata (B2).
- Domain split/merge for inserts/deletes is complex.
- Read-summary regeneration may expose unsupported cases.
- Easy to fix one case while leaving edge cases broken.
- High implementation cost (8-12 files, 800-1500+ LOC).

### Options C, D, E

Considered and rejected. C requires re-placement infrastructure that doesn't
exist. D requires shadow legacy vertices that contradict the FP design
intent. E is a deferred variant of A that introduces correctness windows.

## 5. Recommendation: Option A — pre-structural drop-and-demote

Implementation sketch:

1. Add `relocate_ast_for_template_placement(ast, row_delta, col_delta)` —
   non-mutating clone of the parser AST with relative refs shifted.
2. Add `Engine::demote_spans_for_structural_op(sheet_id)`:
   - Conservative scope: ALL active spans on the affected sheet.
   - For each span: enumerate placements, compute deltas against template
     origin, materialize per-cell ASTs.
   - Run materialized ASTs through `IngestPipeline` for fresh dep plans.
   - Remove spans + remove source-span overlays.
   - Clear computed-overlay cells for demoted placements.
   - Materialize formulas via `bulk_set_formulas_with_plans`.
   - Reset `formula_plane_indexes_epoch_seen`.
3. Insert demotion call BEFORE `VertexEditor::*` in:
   - `Engine::insert_rows` / `delete_rows` / `insert_columns` / `delete_columns`
   - Logged paths: `EngineAction::insert_rows` / `insert_columns` (the
     `edit_with_logger` callers).
4. Strengthen s032/s033/s034/s035 invariants from `NoErrorCells` to per-row
   `CellEquals` against legacy semantics.

What's deferred:
- Precise overlap-only demotion (vs all-spans-on-sheet conservative demotion).
- Automatic re-promotion after structural op.
- Span-domain shifting (Option B) entirely.

## 6. Risk areas

- AST relocation must produce identical semantics to runtime offset eval.
- Undo/redo must record demotion-created vertices coherently.
- Stale computed overlays must be cleared (was part of the visible bug).
- Cross-sheet refs are out of scope; current placement already rejects them.

## 7. Test surface

Implemented:
- `formula_plane_authoritative_column_insert_shifts_span_outputs_correctly`
- `formula_plane_authoritative_column_delete_shifts_span_outputs_correctly`
- `formula_plane_authoritative_row_insert_shifts_span_outputs_correctly`
- `formula_plane_authoritative_row_delete_shifts_span_outputs_correctly`
- AST relocation primitive unit tests (3).
- Strengthened corpus invariants for s032/s033/s034/s035.

Future:
- Edge cases: insert at start/end of span, insert in middle of rect, delete
  entire span, delete partial overlap, cross-sheet refs.
- Undo/redo of structural ops.
- Punchout/overlay preservation during demotion.

## 8. Hedge audit (forbidden patterns)

- Feature flag for the fix → out.
- Mark scenarios `expected_to_fail_under` → out.
- Bump `indexes_epoch` and call it done → that's the current bug.
- "Drop spans, rely on legacy graph" without materializing → loses formulas.
- Mutate arena ASTs in place → never.
- Skip overlay cleanup → stale overlays were part of the bug.

## Result

Implemented as recommended. Validation:
- All 4 corpus scenarios (s032-s035) now pass with strengthened
  per-cell-equality invariants under both Off and Auth.
- Recalc perf for column ops: was 142x/298x slower under Auth, now ~equal.
- All workspace tests pass; no regressions.
- 1429 → 1436 unit tests (added 7).
