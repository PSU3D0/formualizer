# FP7 Performance Findings

Date: 2026-05-04
Status: probe-driven characterization; informs FP1 (visitor) and FP2 (canonicalize-on-demand) work.

## Methodology

`probe-fp-scenarios` (crates/formualizer-bench-core/src/bin/probe-fp-scenarios.rs) builds XLSX fixtures via the standard umya path, loads through `Workbook` under `FormulaPlaneMode::Off` and `AuthoritativeExperimental`, and times load + first eval + warm eval + 5 edit cycles. Sample run at rows=50000:

| scenario | formulas | spans | vertices Off | vertices Auth | load Off→Auth | first Off→Auth | edit Off→Auth |
|---|---:|---:|---:|---:|---|---|---|
| two-trivial-families | 100,000 | 2 | 100,000 | 0 | 1.04x | 1.19x | 1.01x |
| single-trivial-family | 50,000 | 1 | 50,000 | 0 | 1.18x | 0.92x | 1.40x |
| fixed-anchor-family | 50,000 | 1 | 50,000 | 0 | 0.89x | 0.91x | **0.54x** |
| five-families | 250,000 | 5 | 250,000 | 0 | 0.92x | 1.29x | 1.08x |
| heavy-arith-family | 50,000 | 1 | 50,000 | 0 | 0.82x | **0.14x** | 1.26x |
| all-unique-singletons | 50,000 | 0 | 50,000 | 50,000 | **0.68x** | 1.32x | 0.93x |
| long-chain-family | 50,000 | 0 | 50,000 | 50,000 | **0.49x** | 1.29x | 1.36x |
| rect-family-10cols | 500,000 | 10 | 500,000 | 0 | 0.80x | 1.19x | **0.58x** |
| two-anchored-families | 100,000 | 2 | 100,000 | 0 | 0.84x | 0.97x | **0.45x** |

> Speedup ratio is `Off / Auth`. Numbers below 1.0 mean FP is slower.

## Wins

```text
Vertex / memory     200k -> 0 on accepted families (consistent across rows)
Load (best case)    +18% on single trivial family
First eval (best)   +29% on five-family workbook
Edit recalc (best)  +40% on single trivial family
Long-chain          O(N^2) edit-recalc cliff eliminated by rejecting
                    internal-dep families at placement
```

## Three structural perf issues identified

### Issue A: per-placement AST clone (LARGEST)

`SpanEvaluator::evaluate_task` clones the canonical AST tree per placement and evaluates it via the standalone `Interpreter::evaluate_ast`. Legacy graph eval uses `Interpreter::evaluate_arena_ast(ast_id, data_store, ...)` which walks an arena-backed AST without cloning.

```text
heavy_arith eval_first @ 50k:   38ms Off vs 281ms Auth   = -86%
fixed_anchor edit_recalc @ 50k:  3.7ms Off vs 6.9ms Auth = -46%
two_anchored edit @ 100k:        6.5ms vs 14.3ms          = -55%
```

When all cells are dirty (first eval, or whole-result edits), per-cell clone tax dominates. For 50k cells with 8 ops each: 400k node clones plus 400k tree traversals.

**Fix direction (defers per-placement caching, structural-edit safe):**

```text
relocation-aware AST visitor that walks the canonical arena AST and
applies (placement - origin) at each Reference node visit.
zero allocation per cell.
no derived/cached state -> structural edits invalidate nothing new.
```

Do **not** intern relocated ASTs per placement: that creates a new cache invalidation surface for structural edits.

### Issue B: load-path canonicalization overhead even on fallback

`canonicalize_template` runs per formula record, and `place_candidate_family` runs per template group, even when families are all singletons or get rejected.

```text
all-unique-singletons load:   842ms -> 1247ms  (-32%)
long-chain-family load:       835ms -> 1712ms  (-50%)
```

Both produce 0 spans yet pay full FP analysis cost.

**Fix direction:**

```text
short-circuit canonicalization for trivially-unique single-record
template groups (e.g. when template payload is unique to that one
candidate, skip place_candidate_family entirely and emit Legacy).
```

### Issue C: internal-dependency families silently accepted (FIXED)

`B2 = B1+A2` style chains were placed as one span and the runtime produced O(N²) edit recalc.

```text
long-chain @ 20k edit:  1807ms (FP) vs 25ms (Off)  -- 72x regression
after fix:                22ms (FP, falls back to legacy)
```

Closed by `PlacementFallbackReason::InternalDependency` (commit 56c650b).

## Diagnosis: where does the headline 30% win come from, and why isn't it bigger?

The FP win on first-eval comes from:

```text
- skipping graph vertex/edge construction (large absolute fixed cost)
- skipping per-vertex dirty bit / scheduler dispatch
- 1 span with N placements amortizes template intern + read-summary build
```

But it loses some of that back per cell to:

```text
- AST clone per placement (Issue A)
- standalone Interpreter construction per placement
- relocation cost
```

For trivial formulas the wins dominate. For heavy formulas, per-cell tax dominates and FP is net slower.

The original spike's 5.7ms / 100k = 57ns/cell amortized was on `=A1+1` (single ADD). At that scale the per-cell tax was ~irrelevant compared to graph savings.

## What's missing for a radical FP win

**Issue A is the single biggest lever.** Eliminating per-cell AST clone via a relocation-aware visitor would close the heavy-arith gap and likely tip every other scenario from "FP slightly slower" to "FP equal-or-faster".

Beyond that, span-aware vectorized kernels are the next tier — recognize that `=$A$1 + col_offset` over 50k cells is a single Arrow scalar broadcast over a column slot and emit one Arrow operation instead of N interp calls. That's where the architectural radical win lands.

## Action items

```text
[done]   Reject internal-dep families at placement
[done]   Probe rework: realistic formula counts, anchored variants
[done]   Issue A: relocation-aware visitor for SpanEvaluator (commit 2fe6fd1)
[next]   Issue B: short-circuit canonicalization for trivially-unique groups
[plan]   Uniform-value span broadcast (50-100x on absolute-only families)
[plan]   Direct DenseRange writes for Rect/RowRun/ColRun spans
[future] Span-aware vectorized kernels for known function shapes
```

## Post-Issue-A probe (rows=50000)

| scenario              | first eval Off→Auth | edit Off→Auth |
|---|---|---|
| two-trivial-families  | 96→25ms (3.84x)     | 2.54→2.33ms (1.09x) |
| single-trivial-family | 46→12ms (3.83x)     | 1.23→1.25ms (0.98x) |
| fixed-anchor-family   | 39→11ms (3.55x)     | 4.04→2.58ms (1.56x) |
| five-families         | 260→69ms (3.77x)    | 6.34→5.93ms (1.07x) |
| heavy-arith-family    | 36→44ms (0.82x)     | 1.39→1.36ms (1.02x) |
| rect-family-10cols    | 527→124ms (4.25x)   | 50.25→28.41ms (1.77x) |
| two-anchored-families | 62→23ms (2.70x)     | 7.37→5.25ms (1.40x) |

All family-engaged scenarios now show 2.7–4.25x first-eval speedup and
1.4–1.8x edit-recalc speedup. Heavy arithmetic is at parity.

## Remaining gaps

- **Load tax** on no-span workbooks (`all-unique-singletons`, `long-chain-family`):
  -32% to -50% load. Issue B (canonicalization short-circuit) addresses this.
- **Heavy-arith first eval** at parity but not winning. Span kernels would
  unlock 5–10x here by recognizing repeated arithmetic shapes.
- **Edit recalc on trivial families** at parity (~1.0–1.1x) because dirty
  closure says "all dirty" for absolute-anchor cells edited at A1; the work
  is the same as Off mode. Bounded dirty already correct; gain is bounded
  by per-cell interpret cost which is now floor.

