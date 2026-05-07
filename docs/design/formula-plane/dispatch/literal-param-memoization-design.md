# Literal parameterization + parameter-key memoization design

> Authored from a read-only `plan` agent investigation (gpt-5.5), reviewed and
> materialized by PM. Anchors every claim in code with file:line refs.
> Combined dispatch — these two features share the parameter-slot substrate.

## 1. Summary of recommended architecture

### TLDR

Implement one combined FormulaPlane dispatch that introduces a shared parameter-slot substrate and uses it for both:
1. **literal parameterization**: formulas that differ only by literal values fold into the same family;
2. **parameter-key memoization**: non-constant spans evaluate once per unique parameter tuple and broadcast to placements with the same tuple.

Addresses two real remaining SUMIFS findings:
- Variant 2 (s014) does not fold because literal values are baked into `canonical_hash`.
- Variants 3/4 fold but evaluate once per placement even when only K=3 distinct criterion values exist.

Architecture is internal to FormulaPlane. No public APIs, no `EvalConfig` toggles, no feature flags, no parser-arena mutation, no Rayon/parallel execution.

### Verified current state

- `CanonicalLiteral` preserves literal kind+value at `template_canonical.rs:96-130`. Canonicalization at `:476-500`. Key serialization at `:997-1047`.
- `CandidateAnalysis` has `canonical_hash`/`canonical_key` (`placement.rs:154-180`). Family rejects on hash mismatch (`:331-339`). Template interned with first.canonical_key (`:411-420`).
- Ingest groups by `(sheet_id, ingested.canonical_hash)` (`engine/eval.rs:2620-2660, 2699-2760, 2825`).
- `FormulaSpan` has no per-placement binding metadata (`runtime.rs:295-360`).
- `SpanEvaluator::evaluate_task` has constant-result + non-constant branches (`span_eval.rs:99-220`). Non-constant uses `evaluate_arena_ast_with_offset` which disables planner (`interpreter.rs:332-350`).
- Structural demotion relocates one template AST per placement, no per-placement substitution (`engine/eval.rs:3614-3738`).
- `LiteralValue::Number(f64)` inherits f64 PartialEq → NaN ≠ NaN even though `LiteralValue` impls `Eq`. Cannot use as memo key directly. `formualizer-common/src/value.rs:83-116`.
- `ExcelError` derives `PartialEq + Eq + Hash` over kind/message/context/extra (`formualizer-common/src/error.rs:131-135`).

### Recommended end state

Each ingested formula produces:
1. **Exact key** (current behavior — kept for diagnostics).
2. **Parameterized structural key**: literals replaced by `LiteralSlot(slot_id)` markers.
3. **Literal binding vector**: per-formula concrete literal values indexed by `slot_id`.
4. **Value-reference slot descriptors**: for memoization, descriptors for finite relative cell references in value-like contexts. Concrete values resolved per evaluation call.

Family bucketing changes from `(sheet_id, canonical_hash)` to `(sheet_id, parameterized_canonical_hash)`, with full parameterized-key equality guard inside placement to prevent hash-collision merges.

Span eval gains a third branch:
```
constant broadcast → memoized parameter-key broadcast → legacy per-placement
```

Memoized branch builds parameter keys per writable dirty placement, groups, evaluates once per unique key, broadcasts. Memo cache strictly per `evaluate_task` call, dropped on return.

Complexity:
- Current non-constant: `O(N · Eval(template))`
- Memoized with K unique keys: `O(N · KeyBuild + K · Eval(template) + N · Broadcast)`
- For SUMIFS K=3 N=5000: 5000 SUMIFS evals → 3 SUMIFS evals + 5000 cheap writes
- Degenerate K=N: short-circuits after bounded sampling, falls back with O(1) bounded extra work

## 2. Parameter-slot canonicalization

### Recommended design

Add parameter-slot canonicalization as additional output, not replacement.

```rust
pub(crate) struct FormulaCanonicalKeys {
    pub(crate) exact_key: Arc<str>,
    pub(crate) exact_hash: u64,
    pub(crate) parameterized_key: Arc<str>,
    pub(crate) parameterized_hash: u64,
    pub(crate) literal_slot_descriptors: Arc<[LiteralSlotDescriptor]>,
    pub(crate) literal_bindings: Box<[LiteralValue]>,
}

pub(crate) struct LiteralSlotDescriptor {
    pub(crate) slot_id: LiteralSlotId,
    pub(crate) preorder_index: u32,
    pub(crate) context: SlotContext,
    pub(crate) original_kind: LiteralKind,
}

#[repr(transparent)]
pub(crate) struct LiteralSlotId(pub(crate) u16);

pub(crate) enum SlotContext {
    Value, CriteriaExpressionArg, CriteriaRangeArg, Reference,
    ByRefArg, LocalBinding, ImplicitIntersection, CallArgument, Unknown,
}

pub(crate) enum LiteralKind {
    Int, Number, Text, Boolean, Error,
    Date, DateTime, Time, Duration, Empty, Pending,
}
```

Exact key remains identical to current behavior.

Parameterized structural key writes parameterizable literals as `lit_slot(<slot_id>)` instead of `lit(int:5)`.

Example: `=SUMIFS($C:$C,$B:$B,"s001")` and `=SUMIFS($C:$C,$B:$B,"s014")` produce different exact keys but same parameterized key:
```
fn("SUMIFS"; ref(...); ref(...); lit_slot(0))
```
with binding vectors `[Text("s001")]` and `[Text("s014")]`.

### Slot positional representation: pre-order index

Use deterministic pre-order traversal index over parameterizable literal occurrences. Same child order as `write_expr_key` (`:388-460, 940-990`):
- unary child, binary left then right, function args in call order, array rows row-major.
- Each parameterizable literal receives next `LiteralSlotId`.
- Structural key writes `lit_slot(<slot_id>)`. Binding vector stores at `binding[slot_id]`.

Compact (u16/u32, not variable-length path), stable, deterministic, directly indexes binding vector. Matches existing canonical traversal/serialization order.

Rejected: path-from-root. Useful for diagnostics but bulky; doesn't improve correctness once structural key guarantees identical tree shape.

### Literal-context preservation: in descriptor only

Slot id is sufficient for runtime lookup because structural key encodes parent context. Store `SlotContext` in descriptor for diagnostics, assertions, future internal logic, test coverage.

Parameterized structural key does NOT need to write context inside `lit_slot(...)` — context is already implied by parent nodes (e.g., `fn("SUMIFS"; ..., lit_slot(0))` already identifies the slot as criteria-expression position via `function_arg_context`).

### Literal type-vs-value: wildcard kind in key, preserve in bindings/memo

Parameterized key writes only `lit_slot(0)`, not `lit_slot_text(0)` etc.

Safe because:
- Family membership is a grouping decision.
- Runtime evaluation receives concrete `LiteralValue` per placement.
- Function dispatch branches on concrete value.
- Memoization keys include concrete typed parameter atoms — different values/kinds don't share cached results unless atom is identical.
- Literal arena nodes return `data_store.retrieve_value(*vref)` (`interpreter.rs:355-360`); with parameter-binding context, slot returns bound concrete value.

Concrete: `=A1 + 1` and `=A1 + "1"` share parameterized structural key but don't share memo result (atoms differ: `Int(1)` vs `Text("1")`).

Rejected alternative (include kind in key): reduces grouping, doesn't add correctness.

### Array literals: keep rejected

`LiteralValue::Array` already self-rejects via `CanonicalRejectReason::ArrayLiteral` (`template_canonical.rs:485-487`). Preserve. No slot descriptor for arrays. Don't quietly widen authority support.

### Empty/Pending/Error/Date/etc.: parameterize all scalar variants except Array

Variant-by-variant analysis confirms safe parameterization for: `Int`, `Number`, `Text`, `Boolean`, `Date`, `DateTime`, `Time`, `Duration`, `Empty`, `Pending`, `Error`. All except Array.

`LiteralValue::Error(e)` hashes `ExcelError` which derives `Hash + Eq + PartialEq` over kind/message/context/extra. Memo key uses exact error identity, not Excel display equivalence.

## 3. Per-placement binding storage

### Recommended design: BindingStore in FormulaPlane

Add internal store:
```rust
pub(crate) struct FormulaPlane {
    pub(crate) templates: TemplateStore,
    pub(crate) spans: SpanStore,
    pub(crate) span_read_summaries: SpanReadSummaryStore,
    pub(crate) binding_sets: BindingStore,    // NEW
    pub(crate) formula_overlay: FormulaOverlay,
    pub(crate) projection_cache: SpanProjectionCache,
    pub(crate) dirty: SpanDirtyStore,
    epoch: FormulaPlaneEpoch,
}
```

Extend `FormulaSpan`/`NewFormulaSpan` with `binding_set_id: Option<SpanBindingSetId>`.

```rust
pub(crate) struct SpanBindingSet {
    pub(crate) span_ref: FormulaSpanRef,
    pub(crate) literal_slots: Arc<[LiteralSlotDescriptor]>,
    pub(crate) unique_literal_bindings: Vec<Box<[LiteralValue]>>,  // dictionary-encoded
    pub(crate) placement_literal_binding_ids: Box<[u32]>,           // index into unique_literal_bindings
    pub(crate) value_ref_slots: Arc<[ValueRefSlotDescriptor]>,
    pub(crate) template_slot_map: TemplateSlotMap,
}
```

`placement_literal_binding_ids` indexed by placement ordinal in `span.domain.iter()` order.

**Dictionary encoding is mandatory** for memory safety. N=5000 placements with K=3 distinct literals stores K binding vectors + N u32 ids, not N full binding vectors.

### BindingVector representation

```rust
Box<[LiteralValue]>  // for each unique binding vector
Box<[u32]>           // for per-placement ids
```

Not `Cow<LiteralValue>` (lifetimes don't survive placement). Not `Arc<LiteralValue>` per scalar (pointer chasing overhead; vector-level dedup handles memory case).

### Value-context relative reference values

Do NOT persist evaluated cell values in `BindingStore`. Only descriptors:
```rust
pub(crate) struct ValueRefSlotDescriptor {
    pub(crate) slot_id: ValueRefSlotId,
    pub(crate) preorder_index: u32,
    pub(crate) context: SlotContext,
    pub(crate) reference_pattern: CanonicalReference,
}
```

Reason: precedent cells change between evaluations; persisting evaluated values would require dependency invalidation. Per-call memo capture handles this.

### Placement ordinal helper

```rust
impl PlacementDomain {
    pub(crate) fn ordinal_of(&self, placement: PlacementCoord) -> Option<usize>;
}
```

Must match `domain.iter()` order exactly.

### Eviction and lifetime

Span-lifetime data:
- Inserted with span.
- Removed when span removed.
- Removed when span demoted.
- Persists across overlay punchouts (overlays don't alter span domain).
- Rebuilt on re-ingest.

Modify `FormulaPlane::remove_span` (`runtime.rs:641-647`) to also remove associated binding set.

## 4. Span eval with memoization

### Recommended branch order

```rust
if span.is_constant_result {
    return evaluate_constant_broadcast(...);
}

if let Some(plan) = span.parametric_eval_plan(...)
   && should_try_memoization(plan, placements) {
    if let Some(report) = evaluate_memoized(...) {
        return Ok(report);
    }
}

evaluate_per_placement_current_path(...)
```

No public toggle. Self-selecting via internal span metadata + bounded sampling.

### Parameter key contents

```rust
pub(crate) struct ParameterKey {
    pub(crate) atoms: Box<[ParameterAtom]>,
}

pub(crate) enum ParameterAtom {
    Int(i64),
    NumberBits(u64),     // f64::to_bits — not LiteralValue::Number directly
    Text(Arc<str>),
    Boolean(bool),
    Date(String), DateTime(String), Time(String), Duration(String),
    Empty, Pending,
    Error {
        kind: ExcelErrorKind,
        message: Option<Arc<str>>,
        context_row: Option<u32>,
        context_col: Option<u32>,
        extra: ErrorExtraAtom,
    },
    ResidualRowDelta(i64),
    ResidualColDelta(i64),
}
```

**Do NOT use `LiteralValue` directly** — `Number(f64)` PartialEq has NaN ≠ NaN even though `LiteralValue` has `Eq`.

Atom order:
1. literal slots in `LiteralSlotId` order;
2. value-ref slots in `ValueRefSlotId` order;
3. residual relocation discriminator atoms when required.

Deterministic round-trip for mixed slots.

### Literal slot values in key

Per placement: get ordinal → read `binding_id = placement_literal_binding_ids[ordinal]` → get `unique_literal_bindings[binding_id]` → convert each `LiteralValue` to `ParameterAtom`.

### Value-context relative reference values in key

For each `ValueRefSlotDescriptor`:
1. Instantiate canonical relative cell reference at placement.
2. Resolve cell's current value via evaluation context.
3. Convert resulting `LiteralValue` to `ParameterAtom`.

Context rule:

**Include** finite relative cell refs in:
- `AnalyzerContext::Value`
- `AnalyzerContext::CriteriaExpressionArg`

**Do NOT include** in:
- `AnalyzerContext::Reference`
- `AnalyzerContext::CriteriaRangeArg`
- `AnalyzerContext::ByRefArg`
- `AnalyzerContext::LocalBinding`
- `AnalyzerContext::ImplicitIntersection`

Substrate exists at `dependency_summary.rs:34-43, 937-985`.

### Residual relocation discriminator

Memoization is valid only when all placement-varying influences are either:
1. included in key as literal/value-ref atoms, or
2. included in key as residual row/col deltas, or
3. proven placement-invariant.

```rust
pub(crate) struct ParametricEvalPlan {
    pub(crate) literal_slots: Arc<[LiteralSlotDescriptor]>,
    pub(crate) value_ref_slots: Arc<[ValueRefSlotDescriptor]>,
    pub(crate) residual_relocation: ResidualRelocationMode,
}

pub(crate) enum ResidualRelocationMode {
    None,
    IncludeRowDelta,
    IncludeColDelta,
    IncludeRowAndColDelta,
}
```

Rules:
- All relative finite cell refs in value-like context represented as value-ref slots, all other refs absolute/whole-axis placement-invariant → `None`.
- Any remaining reference-context or by-ref relative axis can change with placement → include corresponding row/col delta.
- Both axes can change → both.
- Unsupported reference relocation → don't memoize, fallback.

For SUMIFS target `=SUMIFS($C:$C,$B:$B,B{r})`:
- Sum range absolute whole-col, criteria range absolute whole-col, criterion `B{r}` value-like relative.
- `residual_relocation = None`. Key is just evaluated `B{r}` value.
- Complexity: O(N keys + K SUMIFS + N writes).

### Representative placement

For each unique key, store first placement:
```rust
struct MemoGroup {
    representative: PlacementCoord,
    placements: Vec<PlacementCoord>,
    literal_binding_id: Option<u32>,
    key_atoms: Box<[ParameterAtom]>,
}
```

Evaluation uses representative's current cell + relocation offset.

Valid because:
- Value-ref slots: representative has same evaluated tuple as all in group.
- Literal slots: interpreter binding supplies group's concrete values.
- Residual relative refs: row/col delta in key when needed; all in group share residual behavior.
- Placement-invariant refs: relocation doesn't affect referenced region.

### Degenerate K=N short-circuit

Bounded sampling gate:

```rust
const MEMO_SAMPLE_LIMIT: usize = 64;
const MEMO_MIN_SAMPLE_DUPLICATES: usize = 1;
const MEMO_MAX_UNIQUE_RATIO_NUM: usize = 3;
const MEMO_MAX_UNIQUE_RATIO_DEN: usize = 4;
const MEMO_MAX_ENTRIES_PER_TASK: usize = 16_384;
const MAX_BINDING_SET_BYTES: usize = 8 * 1024 * 1024;
```

Algorithm:
1. Sample up to 64 writable placements. Build keys for sample only.
2. If sample has no duplicates → fallback.
3. If sample unique/sample_len > 3/4 → fallback.
4. Otherwise build full grouping map.

For literal-only spans: also use binding dictionary cardinality. If `unique_literal_bindings.len() == placement_count` and no value-ref slots → skip without sampling.

During full grouping: if `unique_count * 4 > writable_count * 3` → abort grouping before evaluation, fallback. Map discarded on abort.

### Complexity

- Favorable K << N: `O(N · key_build + K · eval + N · write)`.
- All-unique common case: `O(sample_limit · key_build + N · current_eval)`. With sample_limit=64, extra work bounded.

## 5. AST-level substitution mechanism

Hybrid:
- **Literal slots** → interpreter-level binding context (Option B).
- **Value-ref slots** → representative placement + key grouping (Option D).
- **Residual relative refs** → row/col delta in key when needed.
- **Demotion** → tree clone + literal substitution + relocation (Option A).

### Literal slots: interpreter binding context

```rust
pub(crate) struct InterpreterParameterBindings<'a> {
    pub(crate) literal_slots_by_node: &'a [Option<LiteralSlotId>],
    pub(crate) literal_values: &'a [LiteralValue],
}

impl Interpreter {
    pub(crate) fn with_parameter_bindings(
        &self,
        bindings: InterpreterParameterBindings<'a>,
    ) -> Interpreter<'a>;
}
```

Modify arena literal evaluation at `interpreter.rs:355-360`:

```rust
AstNodeData::Literal(vref) => {
    if let Some(slot_id) = self.parameter_bindings.literal_slot_for(node_id) {
        return Ok(CalcValue::Scalar(
            self.parameter_bindings.literal_values[slot_id].clone()
        ));
    }
    Ok(CalcValue::Scalar(data_store.retrieve_value(*vref)))
}
```

Does not mutate parser arena or arena AST. Evaluation-context substitution.

### Value-ref slots: representative placement only (no substitution)

Use first placement in each key group as representative. Evaluate original template at that placement. Value-context relative refs resolve to the same values represented in the key. Broadcast to group.

Correct only because key includes evaluated values AND residual relative refs are guarded by `ResidualRelocationMode`.

### TemplateSlotMap

```rust
pub(crate) struct TemplateSlotMap {
    pub(crate) literal_slots_by_arena_node: Box<[Option<LiteralSlotId>]>,
}
```

Construction: traverse origin candidate's arena AST in same pre-order as parameterized canonicalization. Assign literal slot ids. Store node-id-to-slot mapping for origin `template.ast_id`.

Invariant: origin AST's literal slot descriptor sequence MUST match every candidate's. `place_analyzed_family` asserts this before inserting span.

### Demotion

Current path at `engine/eval.rs:3614-3738` doesn't substitute literals.

Required:
```rust
for each placement:
    ast = retrieve template AST
    if span has literal slots:
        binding = binding_for_placement(placement)
        ast = substitute_literal_slots_in_tree(ast, binding)
    ast = relocate_ast_for_template_placement(ast, row_delta, col_delta)
    ingest FormulaAstInput::Tree(ast)
```

Add structural helper:
```rust
pub(crate) fn substitute_literal_slots_for_template_placement(
    ast: &ASTNode,
    slot_descriptors: &[LiteralSlotDescriptor],
    binding: &[LiteralValue],
) -> ASTNode;
```

Clones tree, replaces literal nodes by pre-order slot id. Does NOT mutate parser arena.

Value-ref slots NOT substituted during demotion — demoted formulas remain formulas with references, not frozen values.

## 6. Family acceptance gate widening

### Extend IngestedFormula and CandidateAnalysis

```rust
pub(crate) struct IngestedFormula {
    pub(crate) ast_id: AstNodeId,
    pub(crate) placement: CellRef,

    pub(crate) exact_canonical_hash: u64,
    pub(crate) exact_canonical_key: Arc<str>,

    pub(crate) parameterized_canonical_hash: u64,
    pub(crate) parameterized_canonical_key: Arc<str>,

    pub(crate) literal_slot_descriptors: Arc<[LiteralSlotDescriptor]>,
    pub(crate) literal_bindings: Box<[LiteralValue]>,
    pub(crate) value_ref_slot_descriptors: Arc<[ValueRefSlotDescriptor]>,

    pub(crate) labels: CanonicalLabels,
    // ... existing fields
}
```

`CandidateAnalysis` extended similarly.

### Family gate

```rust
if analyses.iter().any(|a| {
    a.parameterized_canonical_hash != first.parameterized_canonical_hash
        || a.parameterized_canonical_key != first.parameterized_canonical_key
}) {
    mark_all_legacy(... NonEquivalentTemplate ...);
    return report;
}
```

**Full key equality required** — hash equality alone is not correctness proof.

### Constant-result check

Update rule:
```rust
is_constant_result =
    read_projections_are_constant
    && all placements have the same literal binding vector
    && value_ref_slot_descriptors.is_empty()
```

If literals vary → results can vary → constant broadcast wrong → memoized eval is correct path.

### Binding set construction in placement

In `place_analyzed_family` after domain detection:
1. Sort/align candidates to `domain.iter()` order.
2. Build `unique_literal_bindings` (dictionary-encode).
3. Build `placement_literal_binding_ids`.
4. Verify slot descriptor equality across candidates.
5. Build `value_ref_slot_descriptors` from first analysis, verify equality across candidates.
6. Insert binding set into `FormulaPlane.binding_sets`.
7. Insert span with `binding_set_id`.

### Template interning

Change `TemplateStore` to intern by parameterized key. Extend `TemplateRecord`:
```rust
pub(crate) struct TemplateRecord {
    // ... existing
    pub(crate) exact_canonical_key: Arc<str>,
    pub(crate) parameterized_canonical_key: Arc<str>,
    // ... existing
}
```

For templates with no slots: `exact == parameterized`. Preserves diagnostics.

## 7. Edge-case regression analysis

### 7.1 Memory hazard: K cached results for N placements
**Mitigation**: per-task memo, bounded sampling, unique-ratio guard, full grouping abort.
**Test**: `formula_plane_memo_all_unique_parameters_falls_back_after_sample`

### 7.2 Memory hazard: large literal strings in binding vectors
**Mitigation**: dictionary encoding + 8MB cap. Fallback to legacy.
**Tests**: `formula_plane_literal_bindings_are_dictionary_encoded`, `formula_plane_literal_binding_memory_cap_falls_back`

### 7.3 Memory hazard: memo cache lifetime
**Mitigation**: per-call lifetime, dropped on `evaluate_task` return.
**Test**: `formula_plane_memo_cache_is_per_evaluate_task`

### 7.4 K=N overhead regression
**Mitigation**: sample gate skips before allocation; literal-only spans skip via dictionary cardinality.
**Test**: `formula_plane_memo_sampling_skips_unique_value_refs`

### 7.5 Hash collisions
**Mitigation**: HashMap uses hash for lookup but full ParameterAtom equality. Family placement checks full key, not just hash. Parameter atoms encode typed values deterministically.
**Tests**: `formula_plane_parameter_key_hash_collision_does_not_merge_results`, `formula_plane_parameterized_canonical_hash_collision_does_not_merge_family`

### 7.6 Floating-point exact comparisons
**Mitigation**: `ParameterAtom::NumberBits(u64)` uses `f64::to_bits()`. Don't use `LiteralValue` as key. -0.0 and +0.0 distinct (conservative).
**Tests**: `formula_plane_parameter_key_uses_number_bits`, `formula_plane_parameter_key_nan_reflexive`, `formula_plane_parameter_key_negative_zero_distinct`

### 7.7 Errors
**Mitigation**: `ParameterAtom::Error` includes kind/message/context/extra exactly. Conservative — may miss memo hits, but won't produce wrong shared results.
**Test**: `formula_plane_parameter_key_error_includes_message_and_context`

### 7.8 Date/time/duration
**Mitigation**: `ParameterAtom` stores typed content. No coercion to serial numbers. Matches current canonical string treatment.
**Tests**: `formula_plane_parameter_key_dates_are_typed_not_numeric`, `formula_plane_parameter_key_duration_is_typed_not_numeric`

### 7.9 Array literals
**Mitigation**: preserve `CanonicalRejectReason::ArrayLiteral`. No slot for arrays.
**Tests**: `formula_plane_array_literal_is_not_parameterized`, `formula_plane_array_literal_still_falls_back`

### 7.10 Volatile/dynamic templates
**Mitigation**: keep volatile/dynamic rejection before placement. Memoization requires accepted FormulaPlane template.
**Tests**: `formula_plane_volatile_template_not_memoized`, `formula_plane_dynamic_template_not_memoized`

### 7.11 Reference-identity-sensitive functions
**Mitigation**: value-ref slots only for `Value` and `CriteriaExpressionArg` contexts. Strengthen `function_arg_context` for ROW/COLUMN/AREAS/SHEET as by-ref. (INDEX/OFFSET already mapped.)
**Tests**: `formula_plane_row_reference_identity_not_value_parameterized`, `formula_plane_index_offset_byref_not_value_parameterized`, `formula_plane_criteria_range_not_value_parameterized`

### 7.12 Mixed literal and value-ref slots
**Mitigation**: fixed key atom order: literal → value-ref → residual deltas. Descriptor equality verified during placement.
**Test**: `formula_plane_memo_mixed_literal_and_value_ref_slots_key_order_is_deterministic`

## 8. Eviction strategy

### Per-evaluation-call memo lifetime

```rust
let mut memo: FxHashMap<ParameterKey, MemoValue> = FxHashMap::default();
```

Local to `SpanEvaluator::evaluate_task`. Dropped before return.

Reasons:
- No stale-result risk across `evaluate_all`.
- No dependency invalidation machinery.
- No memory growth across recalc cycles.
- No public API.
- Dirty evaluation already scopes the relevant placement set.

### Capacity bounds

```rust
const MEMO_SAMPLE_LIMIT: usize = 64;
const MEMO_MAX_UNIQUE_RATIO_NUM: usize = 3;
const MEMO_MAX_UNIQUE_RATIO_DEN: usize = 4;
const MEMO_MAX_ENTRIES_PER_TASK: usize = 16_384;
const MAX_BINDING_SET_BYTES: usize = 8 * 1024 * 1024;
```

If expected/observed unique count exceeds bounds → skip/abort, run current per-placement branch.

### Persistent cross-call caching: rejected

Persistent memo requires invalidation keyed by dependency regions and value versions. FormulaPlane dirty system schedules span recomputation but doesn't provide per-parameter invalidation. Target win is reducing duplicate work within ONE span eval call (especially K << N cases). Per-call caching captures that without stale risk.

## 9. Test plan

### 9.1-9.7 Literal parameterization
- `formula_plane_parameterized_literals_fold_same_structure` — varying literal SUMIFS folds.
- `formula_plane_exact_canonical_key_retained_for_diagnostics`
- `formula_plane_literal_slot_wildcards_kind_but_binding_preserves_type`
- `formula_plane_array_literal_remains_rejected_after_literal_parameterization`
- `formula_plane_empty_literal_parameterizes`, `..._pending_...`, `..._error_...`
- `formula_plane_binding_store_dictionary_encodes_repeated_vectors`
- `formula_plane_binding_set_removed_with_span`, `formula_plane_demoted_parameterized_span_materializes_bound_literals`

### 9.8-9.12 Memoization
- `formula_plane_memoizes_value_context_relative_cell_refs` — variant 3 K=3 → 3 evals.
- `formula_plane_memoizes_varying_literal_slots` — variant 2 K=3 → 3 evals.
- `formula_plane_memoizes_mixed_literal_and_value_ref_parameters`
- `formula_plane_memo_residual_relative_reference_includes_row_delta` — `=A{r} + SUM(C{r}:D{r})` correctness.
- `formula_plane_memo_skips_all_unique_literal_bindings`, `formula_plane_memo_sampling_skips_all_unique_value_refs`

### 9.13-9.17 Edge cases
- Float key semantics (3 tests).
- Date/time key semantics (1 test).
- Volatile/dynamic rejection (2 tests).
- Reference identity (3 tests).
- Hash collision guard (2 tests).

## 10. Risks and rollback

### Primary risks

1. Incomplete context classification for ref-sensitive functions → strengthen by-ref contracts (ROW/COLUMN/AREAS/SHEET).
2. Memory growth from large literal bindings → dictionary encoding + 8MB cap.
3. NaN/float equality bugs → `ParameterAtom::NumberBits`, not `LiteralValue::Eq`.
4. Demotion loses literal variation → demotion substitutes literal slots before relocation.
5. Hash-only family grouping → full parameterized key equality check in placement.

### Rollback

Internal-code rollback only. No public API. No workbook format. No EvalConfig. Existing legacy/Off paths unchanged.

If correctness issue: remove/disable internal memo branch, retain literal parameterization only after verifying demotion+binding correctness. If parameterization itself implicated: revert grouping to exact canonical key.

## 11. Open questions for PM (PM decisions)

1. **Internal binding memory cap**: 8MiB per span binding set. **PM: confirmed.**
2. **Memo unique-ratio threshold**: 3/4. **PM: confirmed.**
3. **Exact error identity** (kind+message+context, not Excel display equivalence). **PM: confirmed.**
4. **Reference-sensitive function contract list**: classify ROW/COLUMN/AREAS/SHEET as by-ref/reference-sensitive. INDEX/OFFSET already mapped. **PM: confirmed — required in this dispatch.**
5. **Sample limit**: 64. **PM: confirmed.**

PM decisions: **all 5 confirmed.** Build dispatch may proceed.
