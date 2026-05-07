# FormulaPlane span-eval acceleration: first-principles investigation

> Authored from a read-only `plan` agent investigation (gpt-5.5), reviewed and
> materialized by PM. Anchors every claim in code with file:line refs.

## 1. Summary of findings

### TLDR

The 10k SUMIFS benchmark is not telling us that FormulaPlane promotion is inherently bad for relative criteria. It is telling us that the current span runtime promotes a family, then evaluates the promoted family with a less capable execution model than legacy:

1. **Legacy evaluates independent formula vertices in parallel.** `evaluate_all_legacy_impl` uses the graph scheduler then `evaluate_layer_parallel` when a layer has > 1 vertex and a thread pool exists (`engine/eval.rs:7140-7146`). The parallel path uses Rayon `par_iter` over vertices (`engine/eval.rs:10943-10975`).

2. **Span eval evaluates placements serially inside one span task.** `SpanEvaluator::evaluate_task` enters a single `for placement in placements` loop (`formula_plane/span_eval.rs:177-209`). There is no use of `context.thread_pool()`, Rayon, or per-placement parallelism in this loop.

3. **Therefore variants 3 and 4 compare parallel legacy work against serial span work.** Both paths do roughly the same logical computation today: one SUMIFS per formula/placement, each dispatching through `eval_if_family` (`criteria_aggregates.rs:70-657`). But legacy parallelizes those 10k independent SUMIFS calls; span eval serializes them. **That is the strongest code-anchored explanation for the 60-76% regression.**

4. **The benchmark's theoretical minimum is much lower than both legacy and current Auth for variants 2/3/4.** In `repro_sumifs_variants.rs`, the criterion values cycle through only three strings (`"Type1"`, `"Type2"`, `"Type3"`). The true minimum for equality-style SUMIFS is not 10k scans; it is one grouped aggregation/index build over the data range plus 10k lookups, or at worst three SUMIFS evaluations plus 10k broadcasts/lookups.

5. **The existing criteria mask hook is not a cross-formula cache.** `FunctionContext::get_criteria_mask` delegates to `EvaluationContext::build_criteria_mask` (`traits.rs:1502-1509`), and `Engine::build_criteria_mask` immediately calls `compute_criteria_mask` (`engine/eval.rs:10028-10043`). There is no persistent key-value cache. Repeated `"Type1"` criteria recompute masks across formulas in both legacy and span modes.

6. **The AST planner is not consulted in span per-placement eval.** Span eval calls `Interpreter::evaluate_arena_ast_with_offset` (`span_eval.rs:195-202`), which constructs an interpreter with `disable_ast_planner: true` (`interpreter.rs:332-349`). The arena evaluator directly walks `AstNodeData` (`interpreter.rs:352-566`). The planner is only built in `evaluate_ast_uncached` for tree ASTs when `disable_ast_planner` is false (`interpreter.rs:569-681`). Also: today's planner is shallow — `ChunkedReduce` is selected (`planner.rs:305-323`) but `eval_with_plan` does not execute a special chunked algorithm for SUMIFS; it falls back to normal function dispatch (`interpreter.rs:704-761`).

7. **Literal parameterization in any position is tractable.** Current canonicalization includes literal values in the key (`template_canonical.rs:476-500, 997-1047`), which is why variant 2 does not promote. A generalized "AST structure modulo literal slots anywhere" key is **O(AST size) per formula** using a single tree walk and hash. It does not require enumerating every concrete shape or every subset of literal positions. The combinatorial/NP-hard boundary appears only if we ask for arbitrary maximum common subgraphs, algebraic equivalence, commutative/associative rewrites, or non-tree subexpression matching.

8. **The PM's strategic target is achievable in a qualified sense.** Any non-volatile, non-dynamic, acyclic, dependency-summarizable shape can be made *promotable* with a sufficiently general template/parameter model. But "promotable AND beneficial" is not mathematically guaranteed for every shape: if every placement has unique parameter values and the function has no exploitable structure, the lower bound is still N evaluations. The correct architecture is therefore:
   - promote broad pure/static families;
   - ensure the fallback family executor is at least as good as legacy, including parallelism;
   - add family-aware plans that exploit constants, repeated parameter values, vectorized pointwise evaluation, and function-specific indexes.

### Concrete first-step recommendations

1. **Fix the immediate regression by making non-constant span per-placement evaluation parallel when placements are independent.** This directly addresses the verified mechanical gap: legacy is parallel, span is serial. It should be done before widening promotion further.

2. **Add a general family parameter model, not SUMIFS-argument-specific literal hacks.** Canonicalize ASTs modulo literal slots at any position, collect per-placement literal bindings, and classify relative reference/value parameters by context.

3. **Add parameter-key memoization to span eval.** For pure value-parameter families, evaluate the parameter vector per placement, group identical vectors, evaluate the template once per unique vector, and broadcast to placements in that bucket. This makes variants 2 and 3 with three distinct criteria approach the theoretical minimum even before a SUMIFS-specific index.

4. **Add a family-aware criteria aggregate plan for SUMIFS/COUNTIFS/AVERAGEIFS.** For static ranges and variable equality criteria, build a grouped aggregate index once per family and answer each placement by lookup. This changes variant 3/4 from O(N·R) scans to O(R + N), even when all criteria are unique.

5. **Do not rely on "tighten the promotion gate" as the strategic fix.** A temporary regression guard is acceptable if needed, but demoting broad families contradicts the target. The durable fix is to make promoted execution at least match legacy and then exploit family structure.

---

## 2. The empirical 5-variant matrix

Benchmark: `crates/formualizer-bench-core/examples/repro_sumifs_variants.rs`. ROWS=10,000.

Data sheet: `Data!A{r}` cycles `"Type1"`/`"Type2"`/`"Type3"` (`r%3`); `Data!B{r}` is numeric `r`. Formula sheet: formulas in `Sheet1!A`, key column in `Sheet1!B{r}` cycles same three strings.

| Variant | Off recalc | Auth recalc | spans | Auth/Off |
|---|---:|---:|---:|---:|
| 1. `=SUMIFS(...,"Type1")` constant literal | 5184ms | **2ms** | 1 | 0.0004x |
| 2. `=SUMIFS(...,"Type{N}")` varying literal (s014) | 5100ms | 5239ms | **0** | 1.03x |
| 3. `=SUMIFS(..., B{r})` relative cell-ref criterion | 5198ms | **8310ms** | 1 | **1.60x** |
| 4. `=SUMIFS($B:$B, $A:$A, B{r})` whole-col + relative | 5170ms | **9094ms** | 1 | **1.76x** |
| 5. `=SUMIFS($B:$B, $A:$A, "Type1")` whole-col + constant | 5153ms | **2.47ms** | 1 | 0.0005x |

### What it demonstrates

- Constant-result broadcast (variants 1 & 5) works and matches theoretical minimum.
- Varying literals (variant 2) currently DON'T form a family — different canonical_hash per placement.
- Relative cell-ref criteria (variants 3 & 4) DO promote but execute slower than legacy. This is the new regression.
- The benchmark contains exploitable redundancy (K=3 distinct criterion values across 10k formulas) that neither path exploits today.

---

## 3. First-principles cost model

### General family model

Let `N` = placements; `S` = structural AST shape modulo placement-specific bindings; `P_i` = parameter vector for placement `i`; `f_S(P_i)` = value at placement `i`; `K` = distinct parameter vectors among the N placements; `R` = scanned data range size for range/criteria functions.

```
minimum work = cost of evaluating f_S over family parameter space + W(N)
```

### Cases

1. **All parameters constant.** `minimum = E(S, P) + W(N)`. (Constant-result broadcast case.)

2. **K distinct parameter values, opaque function.** `minimum = K · E(S, P) + N parameter-key lookups + W(N)`. Memoization decisive when K small.

3. **Function has exploitable structure (e.g., SUMIFS equality).** `build aggregate: O(R)`, `answer N placements: O(N)`, `total: O(R + N + W(N))`. Beats memoization when K is large.

4. **Pointwise arithmetic over relative refs (e.g., `=A{r}*2`).** `minimum = N reads + N ops + W(N)`. No sublinear in N. Acceleration comes from reducing constants.

5. **Black-box pure function, all unique parameters.** `minimum = N evaluations + W(N)`. Promotion can reduce constants but not asymptotic cost.

### Theoretical minimums for the 5 variants

| Variant | Minimum work | Current Off | Current Auth |
|---|---|---:|---:|
| 1. Constant literal | 1 SUMIFS + N writes | 5184ms (10k evals) | 2ms (≈minimum) ✓ |
| 2. Varying literal (K=3) | 3 SUMIFS + N lookups, OR O(R+N) index | 5100ms | 5239ms (no fold) ✗ |
| 3. Relative cell-ref (K=3 in benchmark) | 3 SUMIFS + N lookups, OR O(R+N) index | 5198ms | 8310ms (10k serial) ✗ |
| 4. Whole-col + relative (K=3) | Same as 3 | 5170ms | 9094ms ✗ |
| 5. Whole-col + constant | 1 SUMIFS + N writes | 5153ms | 2.47ms (≈minimum) ✓ |

**Both Off and Auth are massively suboptimal for variants 2/3/4** because neither exploits parameter redundancy nor uses a SUMIFS family aggregate plan.

### "Works well" reference cases

- **`=A{r}*2`**: minimum = N reads + N mults + N writes. Auth wins by reducing per-vertex graph overhead.
- **`=$A$1+1`**: minimum = 1 read + 1 add + N writes. Constant-result branch implements this.
- **`=SUM($A:$A)`**: minimum = 1 SUM over R + N writes. Constant-result with whole-axis support implements this.

---

## 4. Why variants 3/4 regress: code-anchored mechanical explanation

### Legacy path

1. `evaluate_all_legacy_impl` → graph schedule → `evaluate_layer_parallel` when layer > 1 vertex + thread pool exists (`engine/eval.rs:7140-7146`).
2. Parallel path: `thread_pool.install` then `group.par_iter()` over vertices (`engine/eval.rs:10943-10975`).
3. Each `evaluate_vertex_immutable` → `Interpreter::new_with_cell` → `evaluate_arena_ast` (`engine/eval.rs:8649-8811`).
4. Arena eval walks `AstNodeData` directly, dispatches functions (`interpreter.rs:352-566`).
5. SUMIFS dispatches to `eval_if_family` (`criteria_aggregates.rs:1087-1090, 70-657`).

### Span/Auth path

1. `evaluate_authoritative_formula_plane_all` (`engine/eval.rs:6784-6791`).
2. `SpanEvaluator::evaluate_task` (`span_eval.rs`):
   - validate, build placements (`:108-117`);
   - one base interpreter (`:119`);
   - **non-constant: enter `for placement in placements` loop (`:177-209`)**;
   - per placement: overlay check, delta arithmetic, current-cell interpreter, `evaluate_arena_ast_with_offset`, push.
3. `evaluate_arena_ast_with_offset` constructs interpreter with `disable_ast_planner: true` (`interpreter.rs:332-349`) then calls `evaluate_arena_ast`.
4. From there: same SUMIFS dispatch as legacy.

### Same logical work, different parallelism

For variant 3, both paths execute one SUMIFS per output cell. There's no family-level grouping in span eval, no "same criterion → reuse result", no grouped SUMIFS index.

**The verified mechanical gap:**

```
legacy: N independent SUMIFS calls spread across Rayon threads
span:   N independent SUMIFS calls in one serial loop
```

`EvalConfig::default` has `enable_parallel: true` (`engine/mod.rs:658-662`); Engine creates thread pool when enabled (`engine/eval.rs:1210-1218`). Span eval has `EvaluationContext::thread_pool` available (`traits.rs:1152-1156`, `engine/eval.rs:9431-9432`) but doesn't use it.

This is the core cause of the 60-76% regression for variants 3 and 4.

### Secondary span-only overheads

Per placement: overlay lookup, row/col delta, current-cell interpreter, offset interpreter construction, reference relocation through `shift_axis_for_offset` (`interpreter.rs:1530-1589`), result conversion, buffer push. Real but secondary relative to lost parallelism.

### Criteria mask cache: verified no cross-formula sharing

`FunctionContext::get_criteria_mask` exists (`traits.rs:1304-1314, 1400-1410`); `DefaultFunctionContext` delegates to `Engine::build_criteria_mask` (`traits.rs:1502-1509`); Engine impl calls `compute_criteria_mask` directly (`engine/eval.rs:10028-10043`) — no persistent map. **Per-formula sharing rate: effectively 0% in BOTH modes.** The mask is reused within one SUMIFS call across row chunks, but rebuilt across formulas.

---

## 5. The pattern-matching decision problem and complexity

### Formal restatement

Two subproblems:
1. **Family detection**: which formulas share enough structure?
2. **Family evaluation**: can it amortize work across placements?

These are distinct. Detecting a family doesn't imply an accelerative evaluator exists for it.

### Current framing: structural equivalence modulo placement shifts

References as `AxisRef::RelativeToPlacement { offset }` / `AbsoluteVc { index }` (`template_canonical.rs:160-170`). Placement requires identical canonical hash (`placement.rs:331-339`).

```
Per formula canonicalization: O(AST size)
Hash/bucket: O(M) expected
Total: O(total AST size)
```

**Polynomial. Linear in input. No NP-hardness.**

### Plus literal substitution (needed for variant 2 + PM's "any literal in any position")

Single-pass canonicalization:
```
fn canonicalize_expr(expr):
    match expr:
      Literal(value):
          slot_id = next_slot()
          key.write("lit_slot", optional kind)
          bindings.push(value)
      Reference(ref): key.write(canonical_affine_reference(ref))
      Function(name, args): key.write("fn", name, args.len); recurse
      Binary(op, l, r): key.write("binary", op); recurse
      ...
```

Outputs: structural key + parameter slot descriptors + per-formula binding vector.

```
Per formula: O(AST size)
Hash/bucket: O(M)
Within family parameter matrix: O(N · L)
```

**Tractable. No pairwise comparisons. No subset enumeration.**

### Avoiding subset explosion

Naive thought: enumerate every subset of literal positions that can vary → O(2^L). **Don't do this.** Use the maximally-generalized literal-slot key:
- all literal positions become slots;
- formulas with same structure enter the same bucket;
- after bucket formation, scan each slot:
  - if all values equal across the bucket → mark slot constant;
  - if values differ → mark slot varying.

**Linear in number of bindings.**

### NP-hardness boundary

NP-hard problem class only enters if FormulaPlane tries to discover:
- algebraic equivalence (`a+b == b+a`);
- commutative/associative reordering;
- maximum common subgraph in DAGs;
- arbitrary non-aligned subexpression extraction.

**FormulaPlane does not need the NP-hard version for the PM's stated goal.** The practical target is ordered AST structural equivalence modulo placement-shifts, literal slots, and selected context-aware parameter slots. Polynomial. Effectively linear.

---

## 6. AST planner: current state, latent structure, deepening opportunities

### Current planner

`crates/formualizer-eval/src/planner.rs`:
- `ExecStrategy::{Sequential, ArgParallel, ChunkedReduce}` (`:14-19`)
- `NodeHints` with `repeated_fp_count` (`:35-40`)
- `PlanNode` with strategy and children (`:50-53`)
- `Planner::plan` annotates and selects (`:109-115`)

Annotations: purity, volatility, cost, repeated subtree fingerprints (`:118-300`).

Selection: volatile/short-circuit → Sequential; pure with large range scans → ChunkedReduce; high cost/fanout → ArgParallel; otherwise Sequential (`:305-323`).

### Current invocation

- Tree-AST path: `evaluate_ast_uncached` builds Planner if `disable_ast_planner=false` (`interpreter.rs:569-681`).
- Arena path: `evaluate_arena_ast` walks `AstNodeData` directly. **No planner.**
- Span per-placement: `evaluate_arena_ast_with_offset` sets `disable_ast_planner: true` (`interpreter.rs:332-349`) then calls arena evaluator. **No planner.**
- Legacy graph formula recalc: `evaluate_vertex_immutable` calls `evaluate_arena_ast` (`engine/eval.rs:8807-8811`). **No planner.**

So the current planner is NOT a major differentiator between legacy and span eval today. **Both bypass it for normal formula evaluation.** It applies mainly to direct interpreter API calls and named formulas.

### What benefits does it currently provide?

Limited.
- `eval_with_plan` mostly defers to existing evaluation (`interpreter.rs:704-761`).
- `ArgParallel` prewarms pure args sequentially (`:735-757`).
- `ChunkedReduce` falls through to normal function dispatch — **no special chunked algorithm for SUMIFS**.

### Latent structure: repeated subexpression hints

`NodeHints::repeated_fp_count` records repeated subtree fingerprints (`planner.rs:35-40, 278-300`) but no code in `select` or `eval_with_plan` acts on it. **Diagnostic / unused in execution.**

Could support: common-subexpression evaluation within one formula, family-level recognition of repeated parameter slots, memoization decisions.

### Deeper: family-aware planner

Current planner plans one AST for one evaluation. FormulaPlane needs plans over `(template AST, placement domain, parameter slots, dependency/read summaries)`.

Proposed surface:

```rust
pub struct FamilyPlanInput<'a> {
    pub ast_id: AstNodeId,
    pub template_origin: (u32, u32),
    pub domain: &'a PlacementDomain,
    pub parameter_slots: &'a [ParameterSlot],
    pub read_summary: &'a SpanReadSummary,
    pub function_caps: &'a dyn FunctionLookup,
}

pub enum FamilyPlanNode {
    BroadcastConstant { scalar_plan: ScalarPlan },
    PerPlacement { scalar_plan: ScalarPlan, execution: PlacementExecution },
    Memoized { key_slots: Vec<ParameterSlotId>, scalar_plan: ScalarPlan },
    VectorizedPointwise { ops: Vec<VectorOp> },
    CriteriaAggregateIndex {
        aggregate: CriteriaAggregateKind,
        range_bindings: StaticRangeBindings,
        criteria_slots: Vec<ParameterSlotId>,
        semantics: CriteriaSemantics,
    },
}

pub enum PlacementExecution {
    Serial,
    Parallel { chunk_size: usize },
}
```

This planner decides: constant broadcast, per-placement parallel fallback, parameter memoization, vectorized pointwise, function-specific family plans (starting with criteria aggregates).

---

## 7. Literal parameterization: tractability analysis

### Current: literal values part of canonical key

`CanonicalLiteral` preserves values (`template_canonical.rs:96-109`); `canonicalize_literal` records exact values (`:476-500`); `write_literal_key` writes them to the key (`:997-1047`); `place_analyzed_family` rejects mismatched hashes (`placement.rs:331-339`).

**Variant 2 doesn't promote because of this.**

### Required: any literal in any AST position

Two formulas in same literal-parameterized family if they have identical ordered AST structure after replacing literal nodes with literal slots, while preserving function names, operator kinds, child order, reference affine structure, context.

### Algorithm

Single-pass canonicalization. O(AST size) per formula. O(M) bucket. O(N·L) bind-scan.

### Algorithm avoids subset explosion

Maximally-generalized key + post-bucket binding scan. **Linear, not 2^L.**

### Correctness risks

1. Preserve literal type/value in binding vector. No coercion at canonicalization.
2. Preserve context. Literal in criteria-expression context ≠ literal as range/reference. Current canonicalization records reference contexts (`template_canonical.rs:111-124`); dependency summary has criteria contexts (`dependency_summary.rs:34-41, 956-974`).
3. Don't treat varying literals as compile-time constants. Planner needs constant/varying metadata per slot.
4. Include error/date/time/array exact behavior. `LiteralValue` variants must be preserved exactly.

### Is generalized literal parameterization NP-hard?

**No.** Ordered-tree hashing modulo literal leaves: O(total AST size). NP-hardness only if FormulaPlane tries algebraic equivalence, commutative reordering, max common subgraph in DAGs, or non-aligned subexpression extraction. **PM's requirement does not require those.**

---

## 8. Strategic options with tradeoff analysis

### Option A: Tighten the promotion gate

Cost: low. Unlocks: prevents regressions only. Doesn't unlock: variants 2/3/4 acceleration. Strategic role: temporary safety valve. **Not the durable fix.**

### Option B: Make per-placement span eval at least match legacy

Cost: medium. Unlocks: removes variants 3/4 regression; restores parity. Doesn't unlock: sublinear acceleration. Runtime: same O(N·E) work, parallel wall-time.

Implementation: use `context.thread_pool()` (`traits.rs:1152-1156`); split placements into chunks; evaluate in parallel; merge writes deterministically. Legacy already does this via `thread_pool.install` + `par_iter` (`engine/eval.rs:10970-10975`).

Correctness risks: output ordering for overlays; cancellation parity; formula overlay punchouts; safety of evaluation context for parallel use (legacy already evaluates `&self` in parallel — should compose).

**Strategic assessment: this is the first concrete implementation step.**

### Option C: General literal parameterization in canonical keys

Cost: medium. Unlocks: variant 2 promotion; foundation for memoization and family plans. Doesn't unlock by itself: speedups (executor still serial).

**Should land with or after Option B**, not before, to avoid promoting more families into a weak runtime.

### Option D: Parameter memoization within span eval

Cost: medium. Unlocks: repeated literal/criteria values; broad pure shapes with repeated params. Doesn't unlock: K=N cases; functions whose semantics depend on reference identity.

Implementation: compute parameter key per placement, group by key, evaluate template once per key, broadcast to group.

```
O(N · param_eval_cost + K · template_eval_cost + N writes)
```

Correctness: key must include enough semantic info; floats/errors/blanks/dates/arrays must hash exactly; context distinguishes value parameters from reference parameters.

**Strategic assessment: general counterpart to constant broadcast. Addresses theoretical minimum for redundant parameters.**

### Option E: Family-aware AST planner output

Cost: medium-high. Unlocks: general architecture for broad promotability. Doesn't unlock immediately: speed without concrete plan nodes.

**This is the right architecture. Should be built incrementally around concrete plan nodes.**

### Option F: Pre-built indexes for criterion-style lookups

Cost: medium for single equality criterion; high for full Excel criteria semantics. Unlocks: variants 3/4 even when every criterion is unique; massive improvements for real dashboards. Doesn't unlock: arbitrary non-criteria functions.

Runtime: `build O(R) + lookup O(1)/placement = O(R + N)`.

Correctness: Excel criteria semantics rich (numbers/text coercion, blanks, wildcards, comparison ops, case-insensitive text, errors); whole-col used bounds align with `RangeView` semantics; invalidate on data edits.

**Strategic assessment: best match for benchmark and real spreadsheet workloads. Implement as a family plan, not scattered special cases inside scalar SUMIFS.**

---

## 9. Concrete first-step recommendations

Recommended dispatch order:

### Recommendation 1: Implement parallel non-constant span placement evaluation

**Rationale**: verified cause of variant 3/4 regression. Required before broadening promotion further.

Implementation:
1. Constant branch unchanged.
2. Non-constant branch: if placement count > threshold AND `context.thread_pool()` exists AND span has no internal deps (already enforced at `placement.rs:394-407`) → run placements in parallel chunks.
3. Each worker evaluates placement via `evaluate_arena_ast_with_offset`, returns `(placement, OverlayValue, sequence_index)`.
4. Main thread sorts by placement order if needed, pushes into `SpanComputedWriteSink`.

Expected impact: variants 3/4 should match or beat Off depending on graph overhead and thread scheduling. Won't reach 2ms class.

Rejected alternative: demote variant 3/4. Hides regression but doesn't improve FormulaPlane.

### Recommendation 2: General parameter-slot model

**Rationale**: needed for "any literal in any position." Foundation for memoization and family planning.

Implementation:
1. Two canonicalization keys:
   - exact key (current behavior, retained for diagnostics)
   - parameterized structural key with literal slots
2. Slot descriptors: id, AST path, kind (literal / value-reference / reference-identity / future subexpression), analyzer context.
3. Per-placement binding vectors.
4. Placement groups by parameterized structural key when exact key differs only by literal slots.

Expected impact: variant 2 can promote. Future family plans operate over parameter vectors.

Rejected alternative: special-case SUMIFS arg index 2. Violates PM's requirement, doesn't scale.

### Recommendation 3: Family-level memoization by parameter value

**Rationale**: benchmark variants 2/3 have K=3 distinct criteria. Memoization is direct generalization of constant-result broadcast.

Implementation:
1. Per placement, evaluate cheap+safe parameter slots:
   - literal slots: direct value;
   - value-context relative cell refs: cell value;
   - more complex value subexpressions: evaluate if pure and dependency-contained.
2. Serialize parameter values to stable key.
3. Group placements by key.
4. Evaluate template once per key. Broadcast result.

Expected impact: variant 2 from 10k SUMIFS to 3 SUMIFS. Variant 3 benchmark from 10k to 3 SUMIFS. General repeated-parameter families improve without function-specific logic.

Boundary: K=N → memoization barely helps. That's where Recommendation 4 (function-specific family plans) is needed.

### Recommendation 4: SUMIFS family aggregate index plan

**Rationale**: theoretical minimum for criteria aggregates is O(R + N), not K scans. Solves both repeated and unique criteria.

First implementation target: `SUMIFS(static_sum, static_criteria, variable_equality_criterion)`, same for COUNTIFS / AVERAGEIFS.

Implementation:
1. Detect: function is SUMIFS/COUNTIFS/AVERAGEIFS; range args static across placements; criterion arg is parameter slot; criteria are equality without wildcards (v1).
2. Build aggregate map: key = normalized criterion value (existing criteria semantics); value = sum/count.
3. Per placement: compute criterion parameter, lookup, write result.
4. Invalidation based on read summary + data snapshot/dirty region.

Expected impact: variants 3/4 → near O(R + N), not O(N·R). Whole-col cases improve when used bounds normalized.

Rejected alternative: rely only on criteria mask caching. Helps repeated criteria but doesn't handle K=N as well as grouped aggregation.

### Recommendation 5: Deepen planner as FAMILY planner

**Rationale**: existing planner not a powerful SUMIFS accelerator. Just enabling per-placement doesn't address family-level amortization.

Implementation: introduce `FamilyPlanner` (alongside scalar `Planner` for fallback). Add plan nodes in order:
1. `BroadcastConstant`
2. `PerPlacementParallel`
3. `MemoizedByParameterKey`
4. `CriteriaAggregateIndex`
5. `VectorizedPointwise`

Expected impact: scalable strategy avoiding shape enumeration. Aligns with PM's NP concern.

Rejected alternative: build a library of concrete formula string patterns. Not tractable.

---

## 10. Open questions for PM

1. **How aggressive should broad promotion be before family plans are complete?** Recommendation: after per-placement parallel parity lands, promote broad pure/static families even if first family plan is fallback parallel evaluation.

2. **Should literal parameterization wildcard literal type or preserve literal kind in structural key?** Recommendation: wildcard all literal values, preserve type/value data in bindings. If PM prefers safer narrower families first, preserve literal kind in key.

3. **First SUMIFS criteria semantics slice to optimize?** Recommendation: equality (text and numeric) using existing `parse_criteria`/`criteria_match` semantics. Comparisons/wildcards later as plan expansions.

4. **Performance target: "never worse than parallel legacy" or "never worse than single-threaded legacy"?** Recommendation: parallel legacy bar (default config has `enable_parallel: true`).

5. **Memory budget for family indexes?** Grouped SUMIFS indexes can be large on high-cardinality data. Recommendation: tie to existing overlay/memory configuration or add FormulaPlane family-plan memory budget.

6. **Keep current exact canonical hashes for diagnostics?** Recommendation: yes. Keep exact key, add parameterized key. Diagnostics report both.

7. **How broad is "non-volatile/non-dynamic" with respect to functions that are pure but reference-sensitive?** Some functions pure but depend on reference identity (e.g., ROW(A1)). Recommendation: classify parameter slots by value-vs-reference context. Initially accelerate value-context families.
