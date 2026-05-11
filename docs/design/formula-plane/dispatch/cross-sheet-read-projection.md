# Cross-sheet read projection in FormulaPlane

PM-authored memo. Captures the gap, the design, and what should be deferred.

## Problem

FormulaPlane span promotion currently rejects any formula with an explicit
sheet binding. s017 (`=Data!A{r} * 2`) and the s016/s030 family rows all
fail to promote even though the cross-sheet ref is the cleanest possible
case: a single relative-cell ref to a different sheet.

Three rejection sites carry the constraint:

1. `crates/formualizer-eval/src/formula_plane/placement.rs:160`
   `CandidateAnalysis::from_ingested` — the LIVE production check. Bails
   if `FLAG_EXPLICIT_SHEET` is set.

2. `crates/formualizer-eval/src/formula_plane/placement.rs:198`
   `analyze_candidate` — test-only path. Same check via
   `CanonicalTemplateFlag::ExplicitSheetBinding`.

3. `crates/formualizer-eval/src/formula_plane/producer.rs:296`
   `SpanReadSummary::from_formula_summary` — bails if any precedent's
   `SheetBinding != CurrentSheet`.

And the FP8 ingest pipeline's parallel read-projection helper:

4. `crates/formualizer-eval/src/engine/ingest_pipeline.rs:787`
   `compute_read_projections` — bails if `reference.sheet.is_some()`.

## What's already in place

The downstream machinery is largely sheet-agnostic and ready:

- `Region` is sheet-id qualified (region_index.rs).
- `SheetRegionIndex` buckets entries by sheet (region_index.rs:306).
- `compute_dirty_closure` queries the consumer-read index per changed
  region; queries are sheet-id sensitive automatically (producer.rs:540).
- Changed-region recording uses the actual cell's sheet_id
  (eval.rs:5375 `record_formula_plane_changed_cell` resolves the sheet
  name to id via `sheet_id_mut`).
- Canonical template encodes sheet binding in the template key
  (template_canonical.rs:1112), so two formulas with different sheet
  bindings get different canonical keys → different templates → no
  collision.
- The template anchor (template.origin_row/col, fixed in commit
  `e627548`) is the FORMULA cell's coordinate. Cross-sheet refs are
  relative to that anchor on a DIFFERENT sheet — e.g., `=Data!A1` at
  Sheet1!B1 (template origin row=1, col=2) means "row offset 0 from
  origin, col offset -1 from origin, on sheet Data". That math works
  unchanged: `evaluate_arena_ast_with_offset` shifts relative axes only,
  preserves sheet binding in the AST.

## What needs to change

The constraint is essentially "we can't represent a read region on a
different sheet than the formula". Lifting it requires:

### 1. Drop the early rejections

- `placement.rs:160` (live): remove the `FLAG_EXPLICIT_SHEET` early bail.
- `placement.rs:198` (test): same.

After this, the candidate's `read_projections` field needs to carry the
target sheet for each projection.

### 2. Sheet-tag the read projections

`DirtyProjectionRule` itself is sheet-agnostic axis math (preserve as-is).

Wrap projections with the target sheet:

```rust
struct ReadProjection {
    target_sheet_id: SheetId,
    rule: DirtyProjectionRule,
}
```

Update:
- `CandidateAnalysis::read_projections: Vec<ReadProjection>` (placement.rs:143).
- `IngestedFormula::read_projections: Option<Vec<ReadProjection>>`
  (ingest_pipeline.rs:700).
- `compute_read_projections` accepts a sheet registry and resolves
  `Some(name)` → `SheetId`. When `None`, target = formula's own sheet.
- `span_read_summary_from_projections` and `span_read_summary_for_domain`
  use each projection's `target_sheet_id` instead of the formula's
  sheet_id when computing `read_region_for_result`.

### 3. Lift the producer-side restriction

`SpanReadSummary::from_formula_summary` (producer.rs:280) currently
takes a single `sheet_id` and rejects non-`CurrentSheet` precedents.
Change so it:

- Accepts a sheet registry (or a name->id resolver closure).
- For each `PrecedentPattern::Cell { sheet, .. }`:
  - If `sheet == SheetBinding::CurrentSheet`, target_sheet = the
    formula's own sheet.
  - If `sheet == SheetBinding::ExplicitName { name }`, target_sheet =
    sheet_registry.get_id(&name). If not found, return
    `ProjectionFallbackReason::UnsupportedSheetBinding` (sheet doesn't
    exist; the formula will produce #REF! at eval, but FP can't reason
    about it).
- Construct read regions on the correct target sheet.

### 4. Sheet name resolution failure handling

If a cross-sheet ref names a sheet that doesn't exist, the formula will
produce `#REF!` at eval. The placement layer should fall back to legacy
in this case so legacy semantics drive the error reporting:
return `PlacementFallbackReason::CrossSheetOrSheetMismatch` (or a more
specific reason like `UnknownSheetBinding`).

### 5. AST eval verification

`evaluate_arena_ast_with_offset` (interpreter.rs:332) shifts relative
row/col axes but preserves the reference's sheet binding. Confirm by
reading the implementation.

## What stays the same

- Dirty propagation: works automatically because consumer-read index is
  sheet-bucketed.
- Template AST: shared across placements regardless of sheet binding.
- Span eval: per-placement row/col delta works on relative axes;
  cross-sheet refs preserve their sheet binding.
- Structural ops: spans on Sheet1 with cross-sheet refs to Data are
  affected by structural ops on Sheet1 (their domain is on Sheet1).
  Structural ops on Data only affect them via the changed-region
  pathway, which already works.

## What's deferred

- Cross-sheet REFS WITHIN ranges: `=SUM(Data!A1:A10)` still hits
  `FiniteRangeUnsupported`. That's the next dispatch (range-arg
  precedent support).
- Implicit sheet binding (`Data!` with empty name parts) and 3D refs
  (`Sheet1:Sheet3!A1`): out of scope.
- External sources (`[1]Sheet1!A1`): out of scope.
- Cross-sheet structural ops causing demotion: the existing demote-
  before-structural-op fix (commit `13fa172`) drops all spans on the
  affected sheet. Cross-sheet spans with reads on the affected sheet
  are NOT currently demoted by that path; they stay live with stale
  read regions. Audit needed but probably OK since the read region is
  sheet-id keyed.

  Actually — verify: if Data has structural rows inserted, do any spans
  with read regions on Data need demotion or do they self-update via
  the normal dirty-region pathway? The current demotion only acts on
  spans whose `domain.sheet_id` matches the structural sheet. Cross-
  sheet read-region spans would NOT be demoted, and after a structural
  op on Data, their read region coordinates would be stale relative to
  the post-shift Arrow store. **This is a probable second-order bug
  that this dispatch should also fix or explicitly defer with a
  documented test case.**

## Risk

- Sheet lookup failures during ingest (sheet referenced doesn't exist
  yet at ingest time). Need to either:
  (a) reject promotion with a fallback reason (safe) and rely on
      late re-promotion (which doesn't exist; deferred);
  (b) defer placement until all sheets are known.
  Option (a) is the conservative path. The formula stays legacy, which
  is correct: if the sheet doesn't exist, the formula produces #REF!
  anyway; legacy handles that fine.

- Case sensitivity: sheet registry's `get_id` is case-sensitive. The
  parser preserves case. If the user types `=data!A1` while the sheet
  is named `Data`, we'd fail to resolve. But this is a wider engine
  consistency concern — match what `set_cell_formula` does today.

- Cross-sheet structural-op demotion (see "What's deferred" §). If we
  don't handle it now, add a regression test that documents the gap
  with `expected_to_fail_under` so the corpus tracks it.

## Test surface

New unit tests in `crates/formualizer-eval/src/formula_plane/dependency_summary.rs`
or `producer.rs` `#[cfg(test)] mod tests`:

- `=Sheet2!A1+1` placed on Sheet1, where Sheet2 exists and Sheet1 exists,
  produces a `SpanReadSummary` with one dependency on Sheet2.
- `=Sheet2!A1*2` family produces spans with cross-sheet read regions.
- Cross-sheet ref to a non-existent sheet falls back to legacy with a
  fallback reason.
- Mixed same-sheet + cross-sheet refs in the same formula
  (`=A1 + Sheet2!A1`) — both should appear in the read summary on
  their respective sheets.

End-to-end test in `engine/tests/`:

- Build a workbook with Data sheet (1k rows of values) and Sheet1 with
  `=Data!A{r} * 2` family. Evaluate. Assert spans > 0. Edit a cell on
  Data. Evaluate. Assert dependent rows updated correctly.

Corpus:

- s017: should now produce `spans > 0` under Auth. Strengthen invariants
  to assert correct cross-cycle accounting.
- s016: same — multi-sheet workbook with cross-sheet family.
- s030 (Family sheet): should now produce spans for the
  `=Data!A{r} * 2 + Data!B{r}` family.

## Recommendation

Land all four code changes (rejections lifted, sheet-tagged projections,
producer accepts cross-sheet refs, ingest pipeline resolves sheet names)
in one dispatch. They're tightly coupled — partial application would
leave broken intermediate states.

Add the cross-sheet structural-op demotion gap as either:
- A separate regression test that DOCUMENTS the gap.
- Or extend `Engine::demote_spans_for_structural_op` to ALSO demote
  spans whose read regions cross into the affected sheet.

The latter is the right answer (correctness > scope discipline) but
adds complexity. Decide based on what the agent finds during read.
