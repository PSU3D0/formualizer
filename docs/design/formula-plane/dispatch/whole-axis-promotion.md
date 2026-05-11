# Whole-axis (whole-column) precedent support in FormulaPlane

> Authored from a read-only `plan` agent investigation (gpt-5.5), reviewed and
> materialized by PM. Anchors every claim in code with file:line refs.

## 1. Reproduction & motivation

A/B repro at 10k rows / 10k formulas
(`crates/formualizer-bench-core/examples/repro_whole_col_vs_finite.rs`):

```
=SUM($A:$A)               Off recalc 4850ms   Auth recalc 4773ms  (spans=0; whole-axis rejected)
=SUM($A$1:$A$10000)       Off recalc 2405ms   Auth recalc    0.81ms  (spans=1; constant-result broadcast)
```

Auth-finite case = 0.81ms. Auth-whole-column = 4773ms. **Lifting the whole-axis rejection alone may unlock ~5800x speedup for `=SUM($A:$A)`.**

The constant-result broadcast path lives in `SpanEvaluator::evaluate_task` at `crates/formualizer-eval/src/formula_plane/span_eval.rs:128-180`. For `=SUM($A:$A)`, target is to enter that same path.

The repro also includes `=SUM($A:$A) - A{r}` at `repro_whole_col_vs_finite.rs:56-66` for mixed-precedent verification. Mixed case should promote to one span but NOT use constant-result broadcast (because `A{r}` differs per placement).

## 2. Why this matters (real-world workloads)

- `=SUM($A:$A)`, `=COUNT($A:$A)`, `=AVERAGE($A:$A)`, `=COUNTA($B:$B)` — common headers/footers.
- `=SUMIFS($B:$B, $A:$A, "Type1")`, `=COUNTIFS($A:$A, ">0")` — common reporting formulas.
- `=SUM(Sheet2!$A:$A)` — cross-sheet summaries.

Corpus scenario `s026-whole-column-refs-in-50k-formulas` (`s026_whole_column_refs_in_50k_formulas.rs`) builds `=SUM($A:$A) - A{r}` × up to 50k rows with single-cell edit cycles. Its tag comment explicitly notes `span_count=0` for whole-column SUM (`:50-53`). This fix targets that scenario directly.

`VLOOKUP(key, $A:$D, 2, FALSE)` is real but blocked by `is_known_static_function` not including `VLOOKUP`/`MATCH` (`template_canonical.rs:827-915`). Adding lookup functions to the allowlist is a separate concern.

## 3. Current rejection trace

Five blocker sites identified, four direct + one parallel arena path.

### Direct blockers

1. **Template canonicalization records `WholeAxisReference` reject label**
   - `Canonicalizer::classify_range_bounds` records `CanonicalRejectReason::WholeAxisReference` when range axis has `(None, None)` bounds (`template_canonical.rs:714-720`).
   - Important: it still continues to canonicalize. `axis_pair_from_range` produces `(WholeAxis, WholeAxis)` AxisRefs (`template_canonical.rs:738-750`). Canonical key is built (`:371-374`).
   - The reject LABEL makes `template.labels.is_authority_supported()` false because authority support requires `reject_reasons.is_empty()` (`template_canonical.rs:189-191`).
   - `placement::analyze_candidate` rejects any template whose labels are not authority-supported (`placement.rs:235-239`).

2. **Dependency summary `reject_non_finite_range`**
   - Handles `CanonicalReference::Range` arm (`dependency_summary.rs:794-833`).
   - Calls `reject_non_finite_range` (`:803-810`) which sets `has_whole_axis = true` for any `AxisRef::WholeAxis` (`:844-854`), inserts `DependencyRejectReason::WholeAxisUnsupported` (`:858-862`), and returns `false` (`:873`).

3. **`axis_kinds_match` excludes WholeAxis**
   - Currently accepts only relative/relative and absolute/absolute pairs (`dependency_summary.rs:999-1009`). Without updating this helper, `(WholeAxis, WholeAxis)` would move from `WholeAxisUnsupported` to `MixedAxisRangeUnsupported`.

4. **Producer projection construction `AxisProjection::from_axis_ref`**
   - Rejects `AxisRef::WholeAxis` with `ProjectionFallbackReason::UnsupportedAxis` (`producer.rs:511-525`).
   - Even after summary acceptance, projection construction would still fail.

### Structural ingest path

5. **`compute_read_projections` requires all four bounds finite**
   - The structural AST walk requires `Some(...)` for all four bounds (`ingest_pipeline.rs:866-878`).
   - `start_row`/`end_row` `None` for `$A:$A` returns `UnsupportedDependencySummary`.

### Parallel arena canonical path

6. **Arena canonical labels reject whole-axis**
   - `compact_ref_type_from_ast` converts missing start to `0` and missing end to `u32::MAX` (`ingest_pipeline.rs:1079-1097`).
   - Arena canonical metadata treats `(start == 0, end == u32::MAX)` as `REJECT_WHOLE_AXIS_REFERENCE` (`arena/canonical.rs:313-318`).
   - `CandidateAnalysis::from_ingested` rejects any nonzero canonical rejects (`placement.rs:172-175`).

**The complete fix must update all six sites.** Deleting one rejection alone is insufficient.

## 4. Existing whole-axis infrastructure (already wired on the runtime side)

Region representation:
- `Region::{WholeRow, WholeCol, WholeSheet}` exist (`region_index.rs:70-99`).
- Constructors `Region::whole_row`, `Region::whole_col`, `Region::whole_sheet` (`:139-149`).
- `axis_extents` maps `WholeRow` to `(Span, All)`, `WholeCol` to `(All, Span)`, `WholeSheet` to `(All, All)` (`:228-230`).
- `AxisExtent::intersects` treats `All` as intersecting any extent (`:253-260`).

Index storage and query:
- `SheetRegionIndex::insert` stores whole rows/cols/sheets in dedicated maps (`:450-457`).
- `SheetRegionIndex::query` collects whole-axis candidates (`:528-545`).
- Existing tests cover whole-row/whole-col query semantics (`:822-826, 1133-1147, 1149-1164`).

Projection helpers already understand whole-axis:
- `query_extents` maps whole-axis patterns correctly (`producer.rs:944-952`).

What's wired:
- Storage and querying of `Region::WholeCol` ✓
- Cross-sheet sheet IDs in `Region` ✓
- Dirty index matching cell edits against whole-column read regions ✓

What's missing:
- Dependency summary refuses to produce whole-axis precedents.
- Projection construction can't convert `AxisRef::WholeAxis`.
- `DirtyProjectionRule` has no source-aware whole-column rule.
- Constant-result classification needs updating.
- Structural ingest rejects missing range bounds.
- Arena canonical labels reject whole-axis.

## 5. Design space

### Pattern representation

`AffineRectPattern` already stores `AxisRef` values (`dependency_summary.rs:83-89`). `AxisRef::WholeAxis` is a valid variant (`template_canonical.rs:164-170`).

**Therefore: no new `PrecedentPattern::WholeAxisRange` needed.** Existing `PrecedentPattern::Range(AffineRectPattern)` is sufficient after rejection is lifted.

### Projection rule

`AxisProjection` represents only finite axes (`producer.rs:505-509`). For whole-column, one axis is unbounded and the other is finite/projectable. Not affine in both axes.

Semantics for whole-column read:
- Read region: `Region::WholeCol { sheet_id, col }`.
- Any change intersecting that read region dirties the entire consumer result region.
- For `$A:$A`, every placement reads the same source column → constant in placement sense.
- For `A:A` (relative column), source column shifts with placement → conservatively non-constant.

**Cannot reuse `WholeResult` directly:** `WholeResult` cannot compute its read region (`producer.rs:416`). Storing source read regions outside `DirtyProjectionRule` would require larger interface changes to `ReadProjection` (`:385-390`).

**Recommended: new variant `DirtyProjectionRule::WholeColumnRange { col_start, col_end }`** that:
- `read_region_for_result` returns the whole-column source.
- `project_changed_region` returns whole consumer result on intersection.
- Constant-result classification is TRUE when both `col_start` and `col_end` are absolute.

### Multi-column support via interface change

For `$A:$D`, emit 4 `WholeCol` read regions rather than adding new index variants.

**Add:**
```rust
pub(crate) fn read_regions_for_result(
    self,
    sheet_id: SheetId,
    result_region: Region,
) -> Result<Vec<Region>, ProjectionFallbackReason>
```

Existing `AffineCell`/`AffineRange` rules wrap their single result in `vec![...]`. `WholeColumnRange` returns one `Region::whole_col` per source column.

### Constant-result classification update

Today `is_constant_projection` at `placement.rs:200-219` and `is_constant_result` at `dependency_summary.rs:54-66` use `axis_is_absolute`. Update to treat `AxisRef::WholeAxis` as placement-invariant:
- New helper `axis_is_placement_invariant`: `AbsoluteVc(_) || WholeAxis` → true; `RelativeToPlacement` → false; open/unsupported → false.
- `WholeColumnRange` is constant when finite column axes are absolute.
- `WholeResult` remains non-constant (no source-region semantics).

### Scope: whole-column only

Whole-row support is mechanically similar but multi-row whole-row intervals could expand to up to 1,048,576 rows and the region index has no `WholeRowInterval`. Whole-row is a follow-up.

## 6. Recommended fix

12 mechanical steps. Apply unconditionally. No feature flags.

### Step 1: Template canonicalization

Stop recording `CanonicalRejectReason::WholeAxisReference` for `(None, None)` bounds (`template_canonical.rs:714-720`). Keep recording `OpenRangeReference` for `(None, Some)`/`(Some, None)`. Keep `axis_pair_from_range` mapping `(None, None)` to `(WholeAxis, WholeAxis)`.

### Step 2: Arena canonical metadata

Stop setting `CanonicalLabels::REJECT_WHOLE_AXIS_REFERENCE` for `(start==0, end==u32::MAX)` in `classify_range_axis` (`arena/canonical.rs:313-318`). Keep `REJECT_OPEN_RANGE_REFERENCE`.

### Step 3: Dependency summary

`reject_non_finite_range` at `dependency_summary.rs:844-873`: stop rejecting `AxisRef::WholeAxis`. Keep rejecting `OpenStart`/`OpenEnd`/`Unsupported`.

`axis_kinds_match` at `:999-1009`: return true for `(WholeAxis, WholeAxis)`.

Still rejected:
- `$A$1:$A` (contains `OpenEnd`).
- `A:$A` (mixed finite endpoint kinds).
- Top-level `=$A:$A` (not in supported function-arg context).

### Step 4: Constant-result classification

Replace `axis_is_absolute` in `dependency_summary.rs:54-66` with helper `axis_is_placement_invariant` treating `AbsoluteVc` AND `WholeAxis` as placement-invariant. `RelativeToPlacement` non-constant. Open/unsupported non-constant (defensive default).

### Step 5: Projection rule extension

Add to `DirtyProjectionRule` (`producer.rs:380-410`):
```rust
WholeColumnRange {
    col_start: AxisProjection,
    col_end: AxisProjection,
},
```

Add new method:
```rust
pub(crate) fn read_regions_for_result(
    self,
    sheet_id: SheetId,
    result_region: Region,
) -> Result<Vec<Region>, ProjectionFallbackReason>
```

For `AffineCell`/`AffineRange`: wrap existing `read_region_for_result` result in `vec![...]`.

For `WholeResult`: keep returning `RequiresExplicitReadRegion`.

For `WholeColumnRange`: compute source column extent from `col_start`/`col_end` and bounded result column extent. Use existing `range_source_extent_for_result` logic. Require finite column extent. Emit one `Region::whole_col(sheet_id, col)` per column. Use `CoordinateOverflow` for invalid shifted relative extents. Use `UnsupportedAxis` if endpoint kinds differ.

**Bound the projected column count.** Reject if it exceeds a threshold (recommend 256 or similar). Common cases (`$A:$A`, `$A:$D`) are small.

### Step 6: Projection dirty behavior

In `project_changed_region`, add `WholeColumnRange` arm. After existing intersection check (`producer.rs:444-446`), return `ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![result_region]))`.

### Step 7: SpanReadSummary construction

`SpanReadSummary::from_formula_summary` (`producer.rs:293-340`): in `PrecedentPattern::Range` arm, detect whole-column shape (`start_row == WholeAxis && end_row == WholeAxis && start_col/end_col finite`). Build `WholeColumnRange`. Push one `SpanReadDependency` per returned read region. Reject whole-row for this patch with `UnsupportedAxis`.

Cross-sheet: use existing `sheet_registry.get_id(name)` resolution (`producer.rs:298-323`). Emit `Region::whole_col(target_sheet_id, col)`.

### Step 8: Structural ingest path

`compute_read_projections` (`ingest_pipeline.rs:852-915`): in `ReferenceType::Range` arm, before requiring all four bounds finite, detect whole-column shape (`start_row.is_none() && end_row.is_none() && start_col.is_some() && end_col.is_some()`). Reject single-bound-missing rows. Reject whole-row. Require `start_col_abs == end_col_abs`. Push `WholeColumnRange`.

### Step 9: Placement read-summary

`placement::span_read_summary_for_domain` (`placement.rs:463-481`): call new `read_regions_for_result` and push one dependency per region.

### Step 10: Ingest read-summary

`ingest_pipeline::span_read_summary_from_projections` (`ingest_pipeline.rs:981-1003`): call `read_regions_for_result`.

### Step 11: is_constant_projection

`placement::is_constant_projection` (`:200-219`): classify `WholeColumnRange` as constant when both finite column projections are absolute.

### Step 12: Function constraints unchanged

Do NOT broaden `is_known_static_function`. `SUM`, `AVERAGE`, `COUNT`, `COUNTA`, `SUMIF`, `SUMIFS`, `COUNTIF`, `COUNTIFS` already in allowlist. `ROW`, `COLUMN`, `ROWS`, `COLUMNS`, `MATCH`, `VLOOKUP` not — and should not be changed in this patch. `INDEX`/`OFFSET` already handled as by-ref/dynamic.

### Internal dependency guard preserved

Placement guard at `placement.rs:400-409` rejects spans whose read region intersects their own result region. This prevents formulas in column A reading `$A:$A`. Keep unchanged. Formulas in column B reading `$A:$A` will promote.

## 7. Composition with existing precedent kinds

### Case: `=SUM($A:$A)`
- One whole-column precedent.
- Projection: `WholeColumnRange { col_start: Absolute(0), col_end: Absolute(0) }`.
- Read summary: `Region::WholeCol { sheet_id, col: 0 }`.
- `is_constant_result == true`.
- Edit any cell in col A → intersects → whole span dirty → constant-result broadcast.

### Case: `=SUM($A:$A) - A{r}`
- Two precedents: whole-column + relative cell `A{r}`.
- Whole-column precedent constant. `A{r}` is `AffineCell` with relative row.
- `.all(...)` constant-result test → false.
- Family promotes if it meets size requirements. Per-placement eval (because `A{r}` differs).
- Read summary has both `WholeCol(A)` AND relative-cell read region.
- Edit in col A may match both. Dirty domain merging handles the union (`producer.rs:42-78`).

### Case: `=SUMIFS($B:$B, $A:$A, "Type1")`
- Two whole-column precedents (criteria range + sum range). Both absolute → both constant.
- Constant-result == true. Single broadcast eval.
- Edit in either column dirties whole span.

### Case: `=SUMIFS($B:$B, $A:$A, A{r})`
- Two whole-column + one relative cell criterion.
- Constant-result == false.
- Whole-column read regions still in summary. Dirty propagation correct.

### Case: cross-sheet `=SUM(DataA!$A:$A)`
- `SheetBinding::ExplicitName` resolved via `sheet_registry.get_id(name)`.
- Emits `Region::whole_col(data_sheet_id, 0)`.
- Region-index queries are sheet-id scoped. Cross-sheet dirty propagation composes.

### Case: `=VLOOKUP("k", $A:$D, 2, FALSE)`
- Whole-column projection CAN represent `$A:$D` (4 WholeCol regions).
- BUT formula won't promote because `VLOOKUP` not in `is_known_static_function`.
- PM decides separately. Independent of this patch.

## 8. Test-driven validation

### Dependency-summary tests

- `=SUM($A:$A)` summarizes as `FormulaClass::StaticPointwise` with one `PrecedentPattern::Range` (rows WholeAxis, cols AbsoluteVc(1)).
- `=SUM($A$1:$A)` still rejected with `OpenRangeUnsupported`.
- Top-level `=$A:$A` still rejected.
- `=SUM(A:$A)` still rejected as mixed.
- Constant: `=SUM($A:$A)` constant. `=SUM($A:$A) - A1` non-constant. `=SUM(A:A)` conservatively non-constant.

### Template-canonical tests

- Whole-axis no longer makes `labels.is_authority_supported()` false.
- Open-ended ranges still do.
- Canonical expression still contains `AxisRef::WholeAxis`.

### Arena canonical tests

- Compact whole-column range no longer sets `REJECT_WHOLE_AXIS_REFERENCE`.
- Open ranges still set `REJECT_OPEN_RANGE_REFERENCE`.

### Producer tests

- `WholeColumnRange` for `$A:$A` produces exactly `Region::whole_col(sheet, 0)`.
- For `$A:$D` produces 4 `WholeCol` regions.
- Edit `Region::point(sheet, row, 0)` projects to whole consumer result.
- Edit outside read column → `NoIntersection`.
- Cross-sheet `=Data!$A:$A` resolves to `Region::whole_col(data_id, 0)`.

### Placement tests

- 10k formulas `=SUM($A:$A)` → 1 span, `is_constant_result == true`, read summary has whole-col dep.
- 10k formulas `=SUM($A:$A) - A{r}` → 1 span, `is_constant_result == false`, both whole-col + relative-cell deps.

### Ingest-pipeline tests

- `compute_read_projections("=SUM($A:$A)")` returns `WholeColumnRange`.
- `compute_read_projections("=$A:$A")` rejects.
- `compute_read_projections("=SUM($A$1:$A)")` rejects.
- Structural and summary paths agree for whole-column formulas.

### End-to-end FormulaPlane tests

- 10k `=SUM($A:$A)`: span_count=1, recalc < 5ms (vs current 4773ms — target 1000x).
- 10k `=SUM($A:$A) - A{r}`: span_count=1, non-constant span produces correct per-row values.
- 10k `=SUMIFS($B:$B, $A:$A, "Type1")`: span_count=1, constant-result broadcast.
- Cross-sheet 10k `=SUM(DataA!$A:$A)`: 1 span, dirty recalc on `DataA!A5000` edit.

### Negative tests

- `=SUM($A$1:$A)` rejects.
- `=$A:$A` rejects.
- Whole-row `=SUM($1:$1)` rejects in this patch.

### Corpus validation

Re-run s026 under Auth. Must promote (span_count > 0). Outputs correct across edit cycles.

```bash
cargo run --release -p formualizer-bench-core --features formualizer_runner --bin probe-corpus -- \
  --label whole-axis-promotion-verify \
  --scale medium \
  --modes off,auth \
  --include 's026-whole-column-refs-in-50k-formulas'
```

Pass condition: s026 medium Auth recalc < 50ms (currently 4827ms; ~96x target conservative for `SUM - A{r}` non-constant case).

## 9. Risks and rollback

### Risk: Over-invalidation from whole-column reads
- Expected and correct. Edit in referenced column dirties whole consumer result.

### Risk: Self/internal dependencies on same sheet
- Existing placement guard (`placement.rs:400-409`) prevents formulas in column A reading `$A:$A`. Keep unchanged.

### Risk: Multi-column `$A:$D`
- Emitting one `WholeCol` per source column is exact. Common cases small. Reject excessive projected column counts (threshold 256) with `UnsupportedAxis`.

### Risk: Two canonical systems must stay aligned
- Both template-canonical AND arena-canonical reject whole-axis today. BOTH must be updated in the same patch. If only one is updated, promotion is path-dependent.

### Risk: Relative `A:A`
- Recommended impl can promote it but keeps it non-constant. Conservative. Avoids incorrect broadcast across rectangular/copied-across domains.

### Risk: Function semantics
- Not allowing every by-ref/dynamic function is preserved by existing allowlist/context checks. `ROW(A:A)`, `INDEX`/`OFFSET` remain rejected appropriately.

### Rollback
- Revert projection-rule addition and restore whole-axis rejection in template/arena/dependency summary.
- No feature flag. No data migration. No parser AST mutation. No cache invalidation.

## 10. Open questions for PM

1. **Scope to whole-column only, defer whole-row?** Recommendation: yes. Whole-row interval support requires new `Region::WholeRowInterval` and is not driven by current measurements.

2. **Add VLOOKUP/MATCH to `is_known_static_function` in this patch?** Recommendation: NO. Independent semantic review needed. Separate dispatch.

3. **Diagnostic flag for "contains whole-axis reference" (non-reject)?** Recommendation: yes if PM wants observability continuity. Today `WholeAxisReference` is visible as a canonical reject reason; after the fix it should be visible as a non-reject diagnostic flag for promoted-formula counting.

4. **`$A:$XFD` (16,384 columns)?** Recommendation: bound projected column count at a reasonable threshold (256 default). Real workloads are 1-4 columns. Beyond threshold: reject with `UnsupportedAxis`. Avoids `Region::WholeColInterval` in this patch.

5. **`is_constant_projection` for relative `A:A`?** Recommendation: keep conservatively non-constant. Future work could refine if rectangular row-runs are common.

PM decisions: **Confirm scope (whole-column only), confirm VLOOKUP separate, accept threshold of 256 columns, accept `A:A` conservative non-constant.**
