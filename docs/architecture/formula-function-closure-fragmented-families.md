# Formula Function Closure and Fragmented Source Families

Status: accepted function-closure/fragmentation substrate plus reviewed E0–E5 transaction plan
Branch baseline: `feat/registry-function-closure-fragments` at `2ab54062`

Tranches A–D implement registry-driven function closure, bounded partition evidence, coordinate replay, and one-analysis fragmented Shadow preparation. Eager fragmented authority remains disabled until the E0–E5 transaction foundation in §6.5 and §9 is complete.

## 1. Decision and sequence

This milestone has two strictly serial tracks:

1. **Common function closure.** Replace FormulaPlane's function-name allowlists with registry-resolved, function-owned semantics and a conservative recursive syntactic-argument dependency default. Ordinary registered functions, including future ordinary functions, become eligible without adding their names to a second list. Exceptional semantics remain explicit and fail closed.
2. **Fragmented source families.** After function closure is authoritative, permit one source-declared shared family with bounded holes or ordinary exceptions to be transactionally partitioned into existing `PlacementDomain::{RowRun, ColRun, Rect}` spans plus exact per-cell fallback. Every fragment shares the family's one anchor AST, canonical analysis, and relocation state. No hole-bearing domain or second geometry authority is introduced.

The compatibility rule is unchanged from `formula-family-stream-ingest.md`:

> FormulaPlane authority is optional. Registry, contract, AST, dependency, placement, structural, and transaction proofs must all succeed; otherwise the affected proposal follows the exact per-cell Calamine replay oracle without missing, duplicated, or partially authoritative formulas.

The tracks must not be combined in one writer swatch. Fragmentation may expose more cells to authority and therefore starts only after registry-wide semantic conformance and function differential tests are green.

## 2. Baseline findings resolved by Tranches A–D

The committed anchor-once implementation provides the correct substrate:

- `crates/formualizer-workbook/src/backends/calamine/formula_replay.rs` owns bounded memory/native spooling and the sequence-sensitive `expand_shared_formula_into` replay oracle.
- `crates/formualizer-workbook/src/backends/calamine/compressed_evidence.rs` accepts only clean monotonic occupancy; holes and ordinary/other-family conflicts currently replay the whole family.
- `crates/formualizer-eval/src/engine/formula_source.rs` transports backend-neutral `SourceFormulaFamily` proposals through `SourceFamilyMembers::{CompleteDomain, ExplicitMembers}` and `PlacementDomainTransport`; deferred packages own a non-clone replay source.
- `crates/formualizer-eval/src/engine/eval.rs` centralizes eager/deferred lifecycle operations behind `SourceFormulaIngress`, prepares complete-domain families, builds exact fallback graphs, and only then commits prepared spans. This ordering is the atomicity seam to preserve.
- `crates/formualizer-eval/src/formula_plane/placement.rs` has one-analysis compressed preparation, `LiteralBindingEncoding::Broadcast`, shared domain/dependency/internal-dependency/size/memory gates, and infallible commit.
- `crates/formualizer-eval/src/formula_plane/runtime.rs` already stores `SpanAstRelocation`; `span_eval.rs` evaluates the anchor arena AST with placement offsets. Existing edit, cycle-demotion, and structural-shift paths can materialize or demote spans.
- `crates/formualizer-eval/src/engine/tests/formula_plane_lookup_semantics.rs` covers first/middle/last virtual formula relocation. The PR #179 completion fix routes `EvaluationContext::formula_text_at_cell` through the same FormulaPlane-aware `Engine::get_cell` seam and adds first/middle/last source-family `FORMULATEXT` coverage. Future formula-inspection APIs must preserve that authority-first ordering.

At the planning baseline, the function boundary was not ready for closure:

- **Blocker:** `crates/formualizer-eval/src/formula_plane/template_canonical.rs` classifies dynamic, local-environment, volatile, reference-returning, array/spill, and approximately 100 "known static" functions by hard-coded names. `is_known_static_function` is a secondary supported-function list and cannot remain an eligibility authority.
- **Blocker:** `crates/formualizer-eval/src/formula_plane/dependency_summary.rs` calls that list and duplicates name-based argument contexts for criteria functions, lookup/reference functions, LET/LAMBDA, and range acceptance.
- **High:** `crates/formualizer-eval/src/function_contract.rs` is opt-in and only represents static scalar, reduction, and criteria aggregation. `Function::dependency_contract` defaults to `None`, so it cannot provide automatic closure for ordinary current/future functions.
- **High:** `FnCaps` in `crates/formualizer-eval/src/function.rs` identifies volatility, dynamic dependency, reference return, and short-circuiting, but not local environments, spill/array result shape, or whether a missing explicit contract may safely use the recursive syntactic default.
- **High:** `ArgSchema` in `crates/formualizer-eval/src/args.rs` describes validation/coercion, shape, and `by_ref`; it does not describe dependency precision, lazy branch semantics, local binding names/bodies, or runtime-selected references. It must be advisory input, not dependency authority.
- **High:** some exceptional registrations are incomplete or inconsistent. For example `RandArrayFn` returns empty caps despite its comment saying it is not pure; LET/LAMBDA expose `PURE | SHORT_CIRCUIT` but no local-environment marker. Registry-wide auditing is a prerequisite, not cleanup deferred behind authority.
- **Medium:** `FunctionDependencyContract` is currently exercised mainly by colocated tests; FormulaPlane does not consume it. Planner lookup already resolves registered functions, and graph formula analysis already asks registry caps for volatile/dynamic behavior, proving registry lookup is an established seam.
- **Medium:** custom functions default to `PURE`, `dependency_contract(None)`, and potentially unusable schema. Treating those defaults as proof would be unsafe because a custom implementation may read context dynamically or return references without declaring it.

Conclusion: keep `FnCaps` and `arg_schema`, but add **one central semantic contract extension** consumed by canonicalization and dependency summarization. Do not grow `FunctionDependencyContract` into a second partially overlapping capability bag, and do not require every normal function to write a specialized contract.

## 3. Compatibility invariants

1. Off mode, exact replay, evaluated values/errors, canonical formula API output, diagnostics, dirty propagation, cycle behavior, and edit/structural results remain the oracle.
2. No supported-function-name allowlist decides eligibility. Names may appear only in tests/fixtures, aliases, builtin registration, or function-owned specialized implementations.
3. Registry resolution uses namespace, canonical registration identity, aliases, and Excel-prefix stripping exactly as `function_registry::get` does. Parser spelling is not semantic identity.
4. An unresolved function, ambiguous local call, unsafe custom function, capability/contract contradiction, unsupported reference context, dynamic dependency, spill/array result, or relocation uncertainty rejects authority.
5. Ordinary functions are admitted by the default semantic contract when their call AST is structurally relocatable and their capabilities are safe. A specialized dependency contract improves precision; absence alone does not reject an ordinary builtin.
6. Recursive dependency derivation is conservative: every syntactic argument subtree contributes all statically discoverable dependencies unless an explicit function-owned contract safely changes roles. It must never under-approximate the fixed planner oracle.
7. Volatile functions are not promoted in this milestone. Short-circuiting may be promoted only while dependency tracking remains the union of all branches; evaluation laziness remains the interpreter's responsibility.
8. Dynamic/reference-returning/local-environment/spill semantics default to fallback. Their capability markers are necessary but not sufficient for future admission.
9. One fragmented family has one source identity, anchor AST/analysis, and relocation recipe, but each committed fragment is an ordinary existing span with its own domain/read summary/binding set/span ID.
10. Holes and exception cells are never synthesized, masked inside a rectangle, or hidden by a span. They are exact replay records/legacy graph cells.
11. Fragment preparation is family-atomic. Either all proposed spans and all fallback cells are preflighted and committed as one disposition, or the complete source family replays.
12. Fragment and exclusion limits disable only the optimization. Evidence/spool limits retain their current fail-closed behavior.
13. Eager authority lands before deferred fragmented authority. Deferred authority is enabled only for `AuthoritativeExperimental` after selected-build, invalidation, structure, exact replay, and bounded-transaction lifecycle tests pass; Off and Shadow remain replay-only.

## 4. Central function semantic contract

Add a single function-owned contract to `crates/formualizer-eval/src/function_contract.rs` and expose it from `Function`:

```rust
pub struct FunctionSemanticContract {
    pub dependency: FunctionDependencySemantics,
    pub evaluation: FunctionEvaluationSemantics,
    pub result: FunctionResultSemantics,
    pub environment: FunctionEnvironmentSemantics,
    pub context: FunctionContextDependence,
    pub precision: Option<FunctionDependencyContract>,
}

pub enum FunctionDependencySemantics {
    RecursiveSyntacticArgs,
    Dynamic,
    Unsupported,
}

pub enum FunctionEvaluationSemantics {
    Eager,
    ShortCircuit,
}

pub enum FunctionResultSemantics {
    ScalarValue,
    MayReturnReference,
    MaySpill,
    Unknown,
}

pub enum FunctionEnvironmentSemantics {
    None,
    LocalBindings,
    Unknown,
}

pub enum FunctionContextDependence {
    None,
    PlacementDependent,
    WorkbookMetadata,
    LocaleOrConfiguration,
    Unsupported,
}
```

Exact naming may follow crate conventions, but there must be one immutable contract returned for an arity/callsite and one validator that reconciles it with `FnCaps`, `arg_schema`, and `FunctionDependencyContract`.

### 4.1 Defaults and trust boundary

- Builtins registered through a sealed crate-private path may use `RecursiveSyntacticArgs + Eager + ScalarValue + no local environment + no context dependence` as the default only after the registry conformance audit proves no exceptional builtin inherits it accidentally.
- Public external/custom registrations do **not** inherit that trusted default. Explicit custom contracts are caller assertions and must pass non-panicking conformance inspection. Legacy custom functions continue to evaluate but are FormulaPlane-ineligible. This is the safe compatibility default.
- `FnCaps::{VOLATILE,DYNAMIC_DEPENDENCY,RETURNS_REFERENCE,SHORT_CIRCUIT}` must agree with the semantic contract. Contradictions fail conformance and reject authority in release builds.
- Add caps (or equivalent semantic fields) for `LOCAL_ENVIRONMENT` and `MAY_SPILL`; do not infer either from names.
- `PURE` is not dependency safety. It cannot opt a custom function into authority by itself.
- `arg_schema.by_ref` and range shape can refine argument context, but absent, panicking, or variadic schemas cannot erase syntactic dependencies or establish safety.
- Registry entries carry an atomic semantic generation. Replacement removes aliases owned by the replaced registration, invalidates cached analyses, and demotes affected spans. Canonical equality stores the versioned semantic payload plus generation; hashes are acceleration only.

### 4.2 Function categories

- **Ordinary scalar/elementwise/reduction:** recursively visit all arguments; finite cells/ranges become precedents. Reduction contracts can improve range/read projection precision but are not admission tickets.
- **Criteria aggregation:** function-owned precision describes criteria ranges, criteria expressions, and optional value ranges. Dependencies include each range and references embedded in criteria expressions. Keep existing `CriteriaAggregationDependencyContract`; route contexts from it rather than function names.
- **Lookup:** default union covers lookup key, lookup table, selectors, fallback expressions, and return array. A future lookup precision contract may narrow result read regions only if differential tests prove no under-approximation. `INDEX`/`CHOOSE` reference contexts remain fallback when they can return a reference.
- **Short-circuit (`IF`, `IFS`, `SWITCH`, `IFERROR`, `IFNA`, `AND`, `OR`):** summarize every branch syntactically, preserve `SHORT_CIRCUIT` for runtime ordering, and do not speculate branch reachability. This is safe over-approximation.
- **Volatile (`RAND`, `NOW`, `TODAY`, `RANDBETWEEN`, `RANDARRAY`, volatile custom):** fallback. Audit parser flags against registry caps but make the registry contract authoritative.
- **Context-dependent:** `ROW()`/`COLUMN()` are placement-dependent and must never be inferred constant from an empty syntactic read set. Workbook metadata and locale/configuration readers fall back until their invalidation semantics exist.
- **Dynamic/reference-returning (`INDIRECT`, `OFFSET`, reference `INDEX`/`CHOOSE`, by-ref custom):** fallback unless a later specialized contract proves a bounded static result; not part of this milestone.
- **LET/LAMBDA/calls:** fallback. Local names shadow workbook names and bodies have lexical environments/call semantics that the current canonical/dependency model does not represent.
- **Spill/array:** fallback; this plan does not add result-region shape authority.
- **Custom:** explicit safe contract plus conformance may admit a custom function; legacy/default custom registration remains evaluable but FormulaPlane-ineligible. Registration replacement invalidates cached semantic identities before subsequent ingest.

### 4.3 Generic derivation

Refactor `template_canonical.rs` so `CanonicalExpr::Function` retains a resolved function semantic identity (namespace/canonical name plus contract fingerprint) or receives it through an analyzer context; do not put `Arc<dyn Function>` in canonical keys. The algorithm is:

1. Normalize parser spelling only for registry lookup; resolve through `function_registry::get`.
2. Validate arity, semantic contract, caps, and schema consistency.
3. Reject exceptional result/environment/dependency semantics and volatility.
4. Canonicalize every argument structurally without name branches.
5. In `dependency_summary.rs`, recursively analyze every argument in value context by default. Finite references/ranges are unioned and deduplicated.
6. If `precision` is present, map argument roles through the contract and derive the same or more precise summary. On malformed role/arity/schema combinations, reject rather than reverting to a potentially unsafe interpretation.
7. Compare derived finite regions symbolically by sheet and containment against the fixed planner `CollectPolicy`; any under-approximation is a test failure and release fallback. Bounded cell expansion is only an independent test cross-check, and reporting separates compared and rejected cells, ranges, and names.

`function_arg_slot_context`, `function_arg_context`, `function_accepts_range_at`, `is_known_static_function`, and all semantic name classifiers are deleted or reduced to non-authoritative migration assertions. A grep-based test prevents reintroduction of a FormulaPlane supported-name table.

## 5. Calamine/AST relocation and syntax closure

At the planning baseline, `validate_anchor_once_syntax` in `placement.rs` rejected every function before FormulaPlane semantics were consulted. Tranche B replaced its narrow node allowlist with a recursive **relocation validator** independent of function eligibility:

- literals and supported operators recurse;
- function calls recurse into arguments and defer call semantics to the resolved function contract;
- finite A1 cell/range references retain checked domain-wide XLSX bounds;
- names, tables, external/3-D, open/whole-axis where relocation equivalence is unproven, calls, spills, and local environments conservatively replay;
- parser/arena AST relocation and Calamine lexical expansion are compared at domain corners, each relative/absolute boundary class, and sampled interiors.

The differential oracle is `expand_shared_formula_into -> normalize -> parse -> canonical AST`, compared with the anchor AST relocated by the same arena relocation path used by formula lookup, edit materialization, demotion, and span evaluation. Compare structure/reference coordinates and canonical formula output, not source spelling or hashes alone. Functions with literal commas, strings resembling references, nested calls, criteria strings, quoted sheet names, aliases, and Excel prefixes belong in the generated corpus.

## 6. Fragmented source-family design

### 6.1 Evidence and bounded partition

Extend `compressed_evidence.rs` to retain bounded exact exclusions for an otherwise monotonic declared rectangle:

```rust
struct FragmentableFamilyEvidence {
    anchor: AnchorDescriptor,
    declared: SourceRect,
    expected_cursor: SourceCoord,
    exclusions: Vec<SourceExclusion>,
    member_count: u64,
}

enum SourceExclusion {
    Hole(SourceCoord),
    OrdinaryFormula(SourceCoord),
}
```

Do not add native holes to `PlacementDomain`, reuse descriptive `span_store.rs` gap vocabulary as authority, or maintain both a rectangle-minus-holes model and fragment list. The source evidence builder owns observed runs/exclusions; one deterministic partitioner is the sole conversion to existing domains.

Initial eligibility is deliberately narrow:

- one valid anchor at declared start, monotonic source order, no duplicate coordinate/anchor, no out-of-range or other-family ownership conflict;
- bounded holes and ordinary formula exceptions only; malformed/unsupported records and ambiguity replay whole-family;
- default caps: at most 64 exclusions and 128 output fragments per family, charged to existing per-family/per-sheet evidence bytes; tune only from benchmarks;
- preserve constant-state expected-cursor evidence and bounded point exclusions; never retain one observed interval per row;
- partition work is O(exclusions + fragments), never O(declared area), and cap breach immediately discards optional fragment evidence.

Partition deterministically in row-major order. Subtract sorted point exclusions from the declared rectangle and coalesce only consecutive rows with identical closed column intervals. Width-one multirow groups become `RowRun`, one-row wider groups become `ColRun`, multirow wider groups become `Rect`, and exact single cells replay rather than becoming one-cell spans. The partitioner returns disjoint, sorted domains whose union plus exclusions exactly equals the declared range. If this proof, checked area arithmetic, or cap accounting fails, return `WholeFamilyReplay`.

An ordinary exception's source record remains ordinary replay. A true hole remains blank/literal according to existing source/value authority. Family members isolated into cells too small for placement are replayed from the shared-family spool with Calamine expansion.

### 6.2 One anchor, many preparations

Extend the hidden backend-neutral source-family transport rather than adding an XLSX- or shared-formula-shaped evaluator event. The engine proposal carries one source-family identity and template origin, multiple existing placement-domain transports, every formula excluded from direct authority with typed legacy ownership, and exact count reconciliation for the declared rectangle. Calamine-specific XML metadata remains private to `compressed_evidence.rs` and its replay owner.

```rust
pub struct PartitionedSourceFormulaFamily {
    pub source_id: SourceFamilyId,
    pub template_origin0: SourceCoord,
    pub template_text: Arc<str>,
    pub declared: SourceRect,
    pub surviving_member_count: u64,
    pub fragments: Vec<PlacementDomainTransport>,
    pub legacy_members: ExplicitPartitionLegacyMembers,
    pub reconciliation: PartitionReconciliation,
}

pub struct PartitionLegacyMember {
    pub coord: SourceCoord,
    pub kind: PartitionLegacyMemberKind,
}

pub enum PartitionLegacyMemberKind {
    SharedFamilyMember,
    OrdinaryException,
}
```

The transport proves that fragment areas plus shared legacy members equal the surviving shared-family count and that shared members, ordinary exceptions, and holes exactly cover the declared rectangle. Hole coordinates do not become evaluator formulas or a second geometry model; their count is sufficient once disjoint fragments and typed legacy coordinates are validated. Names may follow crate conventions, but this is a capability proposal, not a new evaluator-side Calamine event contract. Engine preparation parses, interns, ingests, canonicalizes, and analyzes the template origin once. For each fragment it reuses the immutable `CandidateAnalysis` and calls the existing placement gates with that fragment's independent domain origin. Binding sets and read summaries are fragment-owned; AST relocation points to the same anchor `ast_id`, row, and column, not a re-anchored clone. A fragment need not contain the source anchor.

Small/internal-dependent/unsupported fragments do not silently fall back independently in the first authority slice. Any preparation rejection causes whole-family replay. This conservative atomic rule avoids order-dependent mixed dispositions. A later measured optimization may permit bounded per-fragment fallback through a separately reviewed transaction contract.

### 6.3 Atomic disposition and source ordering

Introduce a backend-neutral prepared partition disposition in `engine/formula_source.rs` containing all prepared spans, the exact replay plan, reconciliation counts, and no committed authority. Complete it through `SourceFormulaIngress` so eager and later deferred lifecycle ownership remain centralized:

1. finalize source partition and replay requirements;
2. parse/analyze each anchor once;
3. prepare every fragment and exact fallback graph record;
4. build all fallback graph work, including ordinary exceptions and shared isolated cells;
5. verify disjointness and `direct shared + legacy shared == surviving shared`, then `surviving shared + ordinary exceptions + holes == declared area` after duplicate rules;
6. only then commit the prepared graph and FormulaPlane append batches, including their prebuilt index deltas.

A failure before the first commit replays the whole family. Multi-fragment authority requires a preallocated infallible batch commit or rollback primitive: all logical capacity and index work is preflighted, with no async cancellation point or fallible operation between fragment writes. Process OOM/panic is outside this transaction claim, but a loop that can fail logically halfway is not atomic.

Spool replay filters emission by `(family, coordinate disposition)`, not merely family ID. `FormulaReplayDisposition` uses a compact family default plus bounded shared-coordinate overrides, so a direct fragmented family does not expand into per-cell state. Its ordinary-coordinate ownership map binds non-shared exception records to the same atomic family disposition. Shared anchors are always processed to establish Calamine expansion state even when the anchor coordinate is direct; emission filtering occurs afterward. Fragment fallback uses a legacy-graph-only prepared path so ordinary exclusions cannot independently create overlapping FormulaPlane authority. Preserve XML source ordering and the existing duplicate resolver. Never replay an exception twice or skip a family cell because its containing rectangle was promoted.

### 6.4 Runtime, edits, structure, and cycles

Each fragment is an existing independent span, so scheduling, dirty projection, producer indexes, and cycle discovery reuse current code. Required family-level behavior:

- formula API lookup relocates from the shared anchor for every fragment placement;
- editing a span member demotes/splits only its containing span under existing behavior; other fragments remain valid, while source-family provenance is no longer runtime authority;
- inserting/deleting rows/columns classifies each fragment through `structural_shift.rs`; any uncertain fragment demotes exactly. Shared anchor IDs may remain shared if relocation descriptors are updated independently;
- whole-family source identity is not required after load. Runtime spans must not depend on the spool;
- mixed-schedule cycle detection may demote one or several fragments. Materialization uses each fragment's `SpanAstRelocation`; no family-wide live transaction is needed after initial commit;
- dependency edges/read regions are per fragment. A read intersecting any fragment's own result region triggers the existing internal-dependency gate for that fragment during preparation; because initial disposition is family-atomic, it replays the family.

Deferred `DeferredFormulaPackage` uses the same prepared source transaction and coordinate disposition as eager authority in `AuthoritativeExperimental`. Package replay is completed during selected/all-sheet build; accepted fragment legacy records remain transaction-owned, ordered fallback records are parsed before finalization, and package invalidation or suppression forces exact legacy materialization. Invalidated partitions remain replay-routing evidence registered as legacy-only, so ordinary exceptions retain family ownership even though the partition is ineligible for authority. Off and Shadow continue whole-family replay. Selected-build, rename/remove, replacement invalidation, random formula read, structure, and spool cleanup remain lifecycle gates rather than alternate authority paths.

### 6.5 Transaction-foundation sequence

Adversarial Tranche E attempts proved that eager activation cannot safely be added inside `finish_eager_compressed_formula_sources`. The prerequisite sequence is:

1. **E0 — complete replay ownership.** Carry typed shared/ordinary legacy coordinates, declared-range reconciliation, and compact coordinate dispositions while fragmented families remain replay-only.
2. **E1 — immutable semantic planning snapshot.** Resolve transaction callsites through a versioned, non-caching registry snapshot; Excel-prefix resolution must not populate aliases. Publication holds an epoch read guard but performs no function resolution.
3. **E2 — prepared legacy graph plan.** Plan checked sheet/vertex IDs, formula assignments, dependencies, dirty state, and exact capacity reservations without mutation. Commit only bounded delta work and never clone or rebuild the existing graph.
4. **E3 — checked FormulaPlane append batch.** Preassign checked IDs, deduplicate immutable stores, prove overlap, reserve touched indexes, and install incremental producer/consumer deltas without `rebuild_indexes`.
5. **E4 — composed Shadow transaction.** Combine replay, legacy graph, authority, and unpublished report deltas; fix stale-disposition exclusivity; exercise the complete fault matrix with no authority publication.
6. **E5 — eager activation.** Publish eager fragmented authority only after E4 passes independent review.
7. **E6 — deferred activation.** Reuse E0-E5 preparation and finalization during selected/all deferred package builds; never introduce a deferred-only transaction, geometry model, or index rebuild.

E2 and E3 have no semantic dependency on each other, but repository work remains single-writer and is reviewed one phase at a time. E4 depends on E0-E3; E5 and E6 are lifecycle activations, not new transaction designs.

## 7. FormulaPlane gate reuse

No new placement policy is authorized. Both whole and fragmented anchor-once proposals must reuse:

- canonical exact/parameterized keys and label support;
- registry semantic resolution and generic dependency summary;
- projected reads, unknown sheet/name behavior, dirty projection, internal dependency;
- `MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS` and constant-result exception;
- `MAX_BINDING_SET_BYTES` with broadcast literals;
- existing `RowRun`/`ColRun`/`Rect`, scheduler, cycle demotion, structural classification, overlays, and formula API relocation.

Refactor only enough to expose one immutable anchor analysis to N preparations. If whole-family and fragmented paths produce different fallback reasons for the same domain facts, default to replay and fix the shared gate rather than adding a fragment exception.

## 8. Telemetry, coverage, and benchmarks

Extend `FormulaIngestReport`, source-family report, and `probe-formula-family-ingest.rs` with saturating counters and bounded reason enums. Keep registry-snapshot, call-template, and formula-cell counts distinct:

- registered functions seen/eligible/default-contract/specialized/rejected by semantic category;
- function-call templates and cells admitted; fallback histogram for unresolved, unsafe custom, volatile, dynamic, reference, local environment, spill, contract contradiction, relocation mismatch;
- fragmented families seen/prepared/promoted/replayed; holes, ordinary exceptions, fragment count/domain-kind histogram, isolated fallback cells, cap/partition/preparation failures;
- anchor parses/ASTs/analyses remain one per family; report analyses reused across fragments;
- reconciliation: direct span cells, shared replay cells, ordinary exception cells, holes, surviving source cells;
- cycle/structural demotions by fragmented origin (observational only; no source identity dependency).

Coverage measurement must use `function_registry::snapshot_registered()` after builtin registration. Report canonical registrations by semantic category, safe-default conformance, specialized precision, unsafe/rejected, and observed corpus calls. Aliases do not inflate function counts. The release artifact records the registry snapshot count/fingerprint and fallback histogram, never a maintained expected-name list.

Benchmark gates use cold child processes and five-run medians on one recorded machine. Every artifact records build profile, allocator, CPU state, fixture hash, baseline SHA, median, and MAD. Comparisons between fixtures with different intrinsic formula-evaluation work gate load/ingest phases only; total-time gates require equivalent evaluation work:

- existing 100k/1M clean-family direct gates and fallback <=15% overhead remain green;
- a nested ordinary-function family (at least 100k) has one anchor analysis, zero descendant graph vertices, no function-name fallback, and <=10% authoritative load/ingest median overhead versus the current arithmetic direct fixture;
- registry closure corpus has zero planner under-approximations and no correctness mismatch; coverage percentage is reported, not gamed by weakening contracts;
- fragmented 100k fixtures with 1, 8, and 64 bounded exclusions directly promote expected fragments, retain O(exclusions + fragments) evidence, and are at least 25% faster and 40% lower RSS than forced whole-family replay;
- cap+1, conflict, disorder, preparation-failure, and deferred fixtures replay exactly with <=15% overhead;
- no benchmark gate may justify increasing caps, accepting volatile/dynamic semantics, or adding a hole domain.

## 9. Serial test-driven implementation swatches

Every swatch uses the same parent-orchestrated loop: one writer scoped only to that swatch, a fresh adversarial reviewer, a focused fix pass for concrete findings, and fresh validation. Any unresolved blocker/high finding stops the swatch. Function closure and fragmentation never share a writer swatch.

### Swatch 0: freeze semantic and relocation oracles

Add test-only registry classification, scalar/context-dependence fixtures, symbolic finite-region planner comparison, Calamine-versus-AST relocation, alias/prefix/replacement fixtures, and a production grep guard for semantic function-name tables. Record current mismatches explicitly. Do not change production eligibility.

**Gate:** all existing tests pass; every baseline mismatch is a named fixture; compared and rejected cells/ranges/names are reported separately.

### Swatch 1: central semantic contract and registry conformance

Add the context-aware semantic contract, sealed trusted-builtin registration, untrusted custom defaults, non-panicking conformance inspection, semantic generations, owned-alias replacement cleanup, cache invalidation, and affected-span demotion. Audit every exceptional builtin. FormulaPlane does not consume the contract yet.

**Gate:** every registered builtin conforms; legacy custom evaluation remains compatible; alias replacement cannot retain stale semantic identity; no supported-name list is added.

### Swatch 2: migrate both canonical systems

Migrate `formula_plane/template_canonical.rs` and `engine/arena/canonical.rs` to registry-resolved, generation-bearing semantic identities. Remove their semantic function-name classifiers while preserving namespace, alias, and Excel-prefix behavior.

**Gate:** existing safe canonical keys remain collision-safe; unresolved, contradictory, context-unsafe, and exceptional calls reject with typed reasons; the grep guard confirms both classifiers are gone.

### Swatch 3: migrate both dependency consumers

Migrate `formula_plane/dependency_summary.rs` and `engine/ingest_pipeline.rs` to recursive syntactic argument union plus function-owned precision contracts. Compare finite regions symbolically against the fixed planner and remove name-based argument-context/range-acceptance helpers.

**Gate:** zero planner under-approximations; malformed precision rejects; criteria parity and conservative lookup/short-circuit union pass; the production grep guard finds no supported-function table.

### Swatch 4: function-aware relocation in Shadow

Formula-inspection authority routing is already completed with PR #179. Replace anchor syntax's function rejection with structural relocation plus registry semantic eligibility, initially in Shadow. Differential-test nested calls, aliases/prefixes, criteria strings, quoted sheets, and XLSX boundaries.

**Gate:** accepted families have one anchor analysis and no Shadow authority writes; every relocation mismatch or uncertain node replays.

### Swatch 5: authoritative common-function closure

Enable the already-proven closure for clean source families only. Reuse every placement, dependency, runtime, edit, structural, cycle, and fallback gate. Add no fragmentation or new domain.

**Gate:** Off/forced-replay/direct values, formula inspection, diagnostics, and edits agree; registry conformance and symbolic no-under-approximation gates are mandatory; existing performance gates remain green.

### Swatch 6: bounded exclusions and pure partitioner

In Calamine evidence, retain constant-state expected-cursor proof plus bounded point exclusions. Produce only existing `PlacementDomain` shapes and exact fallback members. Keep production replay-only.

**Gate:** deterministic union/disjointness properties, exact reconciliation, O(exclusions + fragments) state/work, cap and cap+1 behavior, and unchanged clean-family representation.

### Swatch 7: coordinate-disposition replay in replay-only mode

Teach the Calamine-owned replay package to process every anchor for expansion state and filter only emission by exact family/coordinate disposition. Preserve source ordering and duplicate resolution. Do not prepare or commit fragmented authority.

**Gate:** no skipped or double-emitted formulas across anchors, ordinary exceptions, isolated shared members, duplicates, disorder, and fallback; eager/deferred behavior remains whole-family replay.

### Swatch 8: one-analysis multi-fragment preparation in Shadow

Extend backend-neutral source-family transport with partition proposals. Separate immutable template/relocation origin from each fragment domain origin, reuse one anchor analysis, and build a family-atomic prepared disposition without authority writes.

**Gate:** O(fragments), not O(cells), preparation; fragment domains need not contain the anchor; any fragment rejection discards the complete disposition and replays the family.

### Swatch 9 / E0: complete disposition ownership

Carry the declared range, typed shared/ordinary legacy members, count reconciliation, and compact replay disposition through the backend-neutral seam. Fragmented families remain replay-only.

**Gate:** ordinary exceptions have exactly one family owner; direct family defaults plus bounded overrides preserve anchor processing, source order, and exact replay without O(domain area) state.

### Swatch 10 / E1: immutable semantic planning snapshot

Add non-caching read-only prefix resolution and a versioned registry snapshot consumed by canonical and dependency planning. The publication guard performs no resolution.

**Gate:** concurrent replacement cannot produce mixed semantics or deadlock; accepted clean-family function authority remains unchanged.

### Swatch 11 / E2: prepared legacy graph plan

Plan checked graph IDs, formulas, dependencies, dirty state, and capacity before mutation. Commit only transaction-local delta work.

**Gate:** every injected planning failure leaves the complete engine digest unchanged, and tiny transactions remain flat against increasing pre-existing graph size.

### Swatch 12 / E3: checked FormulaPlane append batch

Preflight checked IDs, immutable-store deduplication, overlap, capacities, and incremental producer/consumer index deltas.

**Gate:** no logical failure exists after the first write; fragmented publication never clones authority or scans/rebuilds existing indexes.

### Swatch 13 / E4: composed transaction in Shadow

Compose exact replay, legacy graph, FormulaPlane append, reconciliation, stale-disposition ownership, and unpublished report deltas without enabling fragmented authority.

**Gate:** the full replay/parse/epoch/ID/reserve/overlap/reconciliation/cancellation fault matrix publishes no partial state or telemetry.

### Swatch 14 / E5: eager fragmented authority

Enable eager partition dispositions through `SourceFormulaIngress` using only the reviewed composed commit. Deferred fragmented evidence remains replay-only until Swatch 15.

**Gate:** direct shared + legacy shared equals surviving shared; adding ordinary exceptions and holes equals the declared area; Off/Shadow remain unchanged; edit, structural, formula lookup, cycle, scaling, and full validation suites pass.

### Swatch 15: deferred lifecycle and rollout

Enable deferred authority by routing `DeferredFormulaPackage` through the same source preparation and finalization used by eager authority. Selected-build isolation, package move/drop, random formula lookup, rename/remove, replacement invalidation, structural materialization, cleanup, bounded telemetry, coverage artifacts, and the benchmark matrix remain rollout gates.

**Gate:** eager/deferred parity, no spool leak or cross-sheet consume, no descendant strings, and every functional/performance gate in section 8. Off and Shadow remain replay-only, and deferred activation fails closed if any gate fails.

## 10. Validation matrix

Each implementation swatch runs its focused tests plus, before authority changes merge:

```text
cargo test -p formualizer-eval function_contract
cargo test -p formualizer-eval custom_function_registry_compat
cargo test -p formualizer-eval formula_plane
cargo test -p formualizer-eval formula_plane_ingest_shadow
cargo test -p formualizer-workbook --features calamine --test calamine shared_formulas
cargo test -p formualizer-workbook --features calamine --test load_limits
cargo test -p formualizer-bench-core --features formualizer_runner --bin probe-formula-family-ingest
cargo fmt --all -- --check
cargo clippy -p formualizer-eval -p formualizer-workbook --all-targets --features formualizer-workbook/calamine -- -D warnings
```

The parent records the writer SHA/diff, fresh reviewer findings by severity/path, fix-worker delta, validation commands, registry coverage snapshot, differential mismatch count, fallback histogram, benchmark machine, and cold-run medians. A self-review or prose claim is not an acceptance gate.

## 11. Explicit non-goals

- No public supported-function list, force-promotion switch, or promise that all 400+ functions promote immediately.
- No admission of volatile, dynamic dependency, reference-returning, LET/LAMBDA/local calls, spill/array, table, external, or 3-D semantics.
- No native hole/exclusion/sparse `PlacementDomain`, masks over result rectangles, or reuse of descriptive `FormulaRunStore` as authority.
- No inference/merging of ordinary formula families or adjacent source families.
- No change to Calamine malformed-source semantics, duplicate resolution, source spooling, parse policy, minimum span size, binding cap, cycle policy, or adaptive partition architecture.
- No full sheet/workbook transaction claim; atomicity is the prepared source-family disposition only.
- No raw source-spelling preservation beyond existing canonical formula APIs.

## 12. Review findings and residual risks

Tranches A–D resolved the original function-name authority, custom trust, relocation, dependency, partition, and source-order replay findings. E0 adds typed ordinary-exception ownership and exact declared-range reconciliation. Eager fragmented authority remains blocked on these independently verified foundations:

- **Blocker / E1:** global registry resolution may write-lock while populating prefix aliases; transaction planning requires an immutable non-caching semantic snapshot before an epoch guard can span publication.
- **Blocker / E2:** `BulkIngestBuilder` interleaves fallible planning with graph mutation and may rebuild work proportional to the existing graph; fragmented fallback requires a bounded prepared legacy plan.
- **Blocker / E3:** FormulaPlane store IDs and producer/consumer indexes lack one checked incremental append transaction; publication must not rebuild all existing authority indexes.
- **Blocker / E4:** nested fallback reports, stale preparation ownership, graph work, and authority work are not yet one unpublished, mutually exclusive disposition.
- **High:** graph vertex and sheet ID exhaustion need typed preflight failures on the prepared transaction path.
- **Medium:** structural edits and cycle demotion may destroy the runtime relationship between fragments. That is acceptable because source-family identity is ingest provenance, not runtime authority.
- **Medium:** the benchmark's allocator-tagged heap and phase timers are nullable. RSS/spool/evidence counters remain usable, but stronger subsystem attribution is residual tooling work.

The safe rule remains fail closed: unresolved identity, contradictory contract, exceptional behavior, relocation mismatch, partition uncertainty, epoch mismatch, or preparation failure replays exact formulas with no partial authority or telemetry.
