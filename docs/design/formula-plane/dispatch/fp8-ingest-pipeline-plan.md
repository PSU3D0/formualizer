# FP8 IngestPipeline Plan: Arena-Native Canonicalization

Date: 2026-05-04  
Status: read-only architectural plan; no production source changes.

## Goal and stance

FP8 replaces the current eager, tree-based FormulaPlane canonicalizer with arena-native canonical metadata computed while formulas are ingested. The end state is one authoritative ingest walk that takes parsed ASTs (or existing raw arena ids), applies placement-dependent rewrites, interns a canonical arena form, derives dependency planning records, and returns all flags needed by FormulaPlane and graph materialization. The standalone `canonicalize_template` logic is not mirrored long-term; its node-contribution rules are moved into arena interning and covered by parity tests until deletion.

The design is intentionally opinionated: **the FormulaPlane ingest path stores canonical, anchor-free references in the arena**. Public unplaced AST interning remains available as a raw/literal compatibility path, but any placed formula entering graph or FormulaPlane authority goes through `IngestPipeline` and receives a placed canonical `AstNodeId`. This is the only option that gives O(1) family detection by id equality instead of payload strings.

## 1. Code path inventory

| Path | Current walk / call | Side effects | FP8 target |
|---|---|---:|---|
| `formula_plane/template_canonical.rs:329-1588` | Full AST canonicalization, labels, reject reasons, payload string/hash. | No | Replace with arena interning contribution logic; keep only parity adapter during migration. |
| `formula_plane/dependency_summary.rs:509-620` | Calls `canonicalize_template`, walks `CanonicalExpr` to summarize affine deps. | No | Fold summary production into `IngestPipeline`; later remove when placement consumes arena metadata/read summary. |
| `formula_plane/placement.rs:120-210` | Retrieves arena AST, re-canonicalizes candidate, summarizes, compares payloads. | No | Consume `IngestedFormula.ast_id`, `canonical_hash`, labels, and read summary directly. |
| `formula_plane/diagnostics.rs:88-120` | Diagnostic-only canonicalization. | No | Keep as compatibility wrapper backed by new arena metadata and optional explain adapter. |
| `engine/eval.rs:2522-2628` | Shadow ingest retrieves AST, canonicalizes for grouping, then placement re-walks. | No graph mutation, scratch FormulaPlane only. | Fold into `IngestPipeline`; group by canonical `AstNodeId`. |
| `engine/eval.rs:2630-2894` | Authoritative ingest retrieves AST, analyzes candidate, groups by payload, places spans, emits fallback batches. | Mutates FormulaAuthority/FormulaPlane indexes. | Fold candidate analysis and grouping into `IngestPipeline`; placement reads precomputed metadata. |
| `engine/eval.rs:2895-2940` | Routes batches to FormulaPlane analysis then `BulkIngestBuilder`. | Mutates graph/authority via callees. | Entry seam remains public; intern/plan through pipeline. |
| `engine/eval.rs:3010-3108` | Deferred formula build parses text, interns raw AST, constructs records. | Mutates staged queue then graph through ingest. | Keep API; call pipeline after parse, preferably avoiding raw retrieve. |
| `engine/eval.rs:5240-5280` | `set_cell_formula` / `bulk_set_formulas` compute volatility by tree walk before graph set. | Mutates graph, dirty regions, topology. | Route through pipeline; volatility/dynamic from labels. |
| `engine/eval.rs:5350-5400` and `get_cell` | Retrieves arena AST for public display/readback. | No | Keep; for canonical ids reconstruct formula at placement or return saved formula text. |
| `engine/eval.rs:7773-7795` | Engine-provider volatility tree walk. | No | Replace for ingested formulas with arena label; retain for raw helper/tests until cold-path switch. |
| `engine/ingest_builder.rs:90-450` | Bulk ingest rewrites structured refs, computes volatile/dynamic tree/arena, plans deps, stores ASTs. | Creates vertices/placeholders/ranges/names/tables; mutates graph. | Its formula loop becomes a consumer of `IngestedFormula.dep_plan` or a batch pipeline result. |
| `engine/plan.rs:66-460` | Tree `collect_references` and arena reference collection build `DependencyPlan`. | Mutates `SheetRegistry` by resolving/creating sheet ids. | Dependency collection folded into pipeline; planner data structure retained. |
| `engine/graph/formula_analysis.rs:35-944` | Tree and arena dep extraction, LET/LAMBDA local scopes, volatile/dynamic. | Creates placeholder vertices, pending names, name/table attachments via callers. | Keep graph extraction for legacy/cold paths initially; hot ingest uses pipeline, preserving side-effect materialization layer. |
| `engine/graph/mod.rs:1426-1560` | Single formula set: structured-ref rewrite, dep extraction, store AST, volatile/dynamic. | Mutates graph vertices, edges, names, pending refs, dirty. | Pipeline front-end plus existing graph side-effect application. |
| `engine/graph/mod.rs:1585-1728` | In-place structured-ref rewrite over AST. | No, but consults table registry and returns errors. | Move into pipeline before canonical intern; keep helper for compatibility until Phase 4. |
| `engine/graph/mod.rs:2089-2200` | Bulk set formulas: volatility, dependency plan, store ASTs. | Mutates graph in batches. | Route through batch `IngestPipeline`. |
| `engine/graph/mod.rs:3445-3500` | Rebuild deps after structural AST changes. | Mutates edges/names/ranges. | Phase 4 cold path; pipeline replans changed formulas. |
| `engine/arena/data_store.rs:298-820` | Converts `ASTNode` to compact arena and reconstructs it; detects table rewrites. | Mutates DataStore arenas/interner. | Extend to canonical interning mode and metadata; keep raw convert/reconstruct APIs. |
| `engine/arena/data_store.rs:331-366` | Arena walk for context-dependent structured refs. | No | Replace with label `NeedsPlacementRewrite` / `ContainsStructuredRef`. |
| `engine/graph/sheets.rs:190-360` | Retrieves, adjusts, and re-stores ASTs for sheet/row/col structural ops. | Mutates formulas and dependencies. | Keep tree rewrite in Phase 1-3; Phase 4 replans through pipeline after rewrite. |
| `engine/graph/editor/reference_adjuster.rs` and `ast_utils.rs` | Structural/reference adjustment over AST. | No direct graph mutation. | Keep tree-walking; arena-native rewrite is out of scope. |
| `interpreter.rs:197-1050` | Evaluates AST trees and references; arena evaluator also walks compact arena elsewhere. | Evaluation reads/writes through context/function calls. | Keep runtime walkers; teach arena evaluator canonical refs with placement. |
| `planner.rs:111-360` | Per-eval AST planner/annotations. | No | Keep; unrelated to ingest canonicalization. |
| `traits.rs:233-800` | Argument coercion/reference extraction from AST and arena fallback reconstruction. | No | Keep; display/coercion compatibility. |
| Workbook backends `calamine.rs:720-910`, `umya.rs:1314-1414`, `json.rs:811-884` | Parse formula text, call `intern_formula_ast`, build `FormulaIngestRecord`, call `ingest_formula_batches`. | Mutate engine only through public API. | No signature changes; records still accepted, pipeline canonicalizes at ingest. |

## 2. Arena entry shape change

Use an entry wrapper rather than bloating every enum variant:

```rust
pub struct AstNodeEntry {
    pub data: AstNodeData,
    pub meta: AstNodeMetadata,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct AstNodeMetadata {
    pub canonical_hash: u64,
    pub labels: CanonicalLabels,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CanonicalLabels {
    pub flags: u64,
    pub rejects: u64,
}
```

`AstArena.nodes` becomes `Vec<AstNodeEntry>`, while `get(id)` still returns `&AstNodeData` and a new `metadata(id)` returns `AstNodeMetadata`. Memory cost is 24 bytes per unique node (`u64` hash + two `u64` bitsets), likely rounded into the entry alignment. This is preferable to a parallel vector during FP8 because interning needs child metadata immediately; if memory probes show unacceptable growth, `metadata: Vec<AstNodeMetadata>` can be split without API changes.

Core flags and consumers:

| Label | Bottom-up computation | Replaces today | Consumers |
|---|---|---|---|
| `RelativeOnly`, `AbsoluteOnly`, `MixedAnchors` | Reference axes set relative/absolute bits; parent ORs children; mixed if both. | `CanonicalTemplateFlag::{RelativeReferenceAxis, AbsoluteReferenceAxis, MixedAnchors}` | Placement, future uniform broadcast. |
| `Volatile` | Function caps/name volatile OR child volatile OR parser volatile bit if available. | `is_ast_volatile*`, canonical volatile reject. | Graph volatile marking, span rejection. |
| `Dynamic` | Function caps `DYNAMIC_DEPENDENCY` or hard-coded `INDIRECT/OFFSET` OR children. | `is_ast_dynamic`, canonical dynamic reject. | Graph dynamic flag, span rejection. |
| `ContainsStructuredRef` | Reference `CompactRefType::Table` or child. | canonical structured reject; `ast_needs_structural_rewrite`. | Pipeline rewrite/fallback, diagnostics. |
| `NeedsPlacementRewrite` | Unnamed table / `#ThisRow` form or child. | `ast_needs_structural_rewrite`. | Pipeline invokes table-context rewrite before canonical intern. |
| `ContainsName` | NamedRange/external-cell-as-name or child. | `F_HAS_NAMES`, canonical named reject. | Dependency plan names, span rejection. |
| `ContainsTable` | Table/external-range-as-table or child. | `F_HAS_TABLES`, canonical table reject. | Dependency plan tables, span rejection. |
| `ContainsRange` | Range/open/whole-axis/3D range or child. | `F_HAS_RANGES`, `FiniteRangeReference`. | Plan/range dependency paths, span rejection or summary. |
| `ContainsArray` | Array node, array literal value, array/spill function, or child. | `F_LIKELY_ARRAY`, canonical array reject. | Span rejection, eval hints. |
| `ContainsLetLambda` | Function name `LET`/`LAMBDA` or child. | canonical local-environment reject; formula_analysis local scopes. | Span rejection; dependency extraction scoping guard. |
| `ContainsFunction` | Function node or child. | canonical function flag. | Diagnostics and planner heuristics. |
| `ExplicitSheet`, `CurrentSheet` | Reference sheet key presence/absence. | canonical sheet binding flags. | Cross-sheet span fallback, sheet rename risks. |
| Reject bits | Node-local unsupported kind OR child rejects. | `CanonicalRejectReason` set. | Placement fallback, diagnostics parity. |

Canonical hash is computed bottom-up from `(node kind, canonical operands, child hashes, labels that affect family identity)`. Literal values hash by value bits exactly as `CanonicalLiteral`; functions normalize names by uppercasing and stripping `_XLFN.`, `_XLL.`, `_XLWS.`. Reject labels participate in the hash during parity so unsupported families do not merge accidentally.

## 3. Reference normalization at intern time

FP8 canonical ingest stores **anchor-free delta references** for placed formulas. Literal-coordinate storage is retained only for raw `intern_formula_ast` compatibility and non-ingest diagnostics. Canonical reference variants should be added rather than overloading literal fields:

```rust
pub enum CompactRefType {
    Cell { sheet: Option<SheetKey>, row: u32, col: u32, row_abs: bool, col_abs: bool },
    Range { /* existing literal form */ },
    CellRelative { sheet: Option<SheetKey>, row_delta: i32, col_delta: i32,
                   row_abs: bool, col_abs: bool, row: u32, col: u32 },
    RangeRelative { sheet: Option<SheetKey>, start_row_axis: CompactAxis,
                    start_col_axis: CompactAxis, end_row_axis: CompactAxis,
                    end_col_axis: CompactAxis },
    /* existing non-cell refs */
}

pub enum CompactAxis { Relative(i32), Absolute(u32), OpenStart, OpenEnd, WholeAxis }
```

A pure `$A$1` becomes absolute axes; `A1` in a formula at `B5` becomes `row_delta = -4`, `col_delta = -1`. Mixed anchors keep per-axis mode. This makes equal formulas at different placements intern to the same root id, so family grouping becomes `BTreeMap<(sheet_id, ast_id)>` plus shape/domain splitting.

Consequences:

* `intern_formula_ast(ast)` keeps returning a raw literal-coordinate id. It is not a family fingerprint. Documentation will say placed ingest canonicalizes separately.
* `IngestPipeline::ingest_formula` requires `placement`; public APIs already have sheet/row/col at ingest/set time.
* Display/text emission should prefer `formula_text` when present. When reconstructing from canonical arena, apply placement to relative axes to emit literal A1 coordinates.
* Structural ops continue to rewrite literal ASTs in Phase 1-3. If a canonical id must be structurally adjusted, reconstruct-at-placement, apply existing tree adjuster, and re-run the pipeline. Arena-native structural rewrite is explicitly out of scope.
* Sheet keys should canonicalize to `SheetKey::Id` when resolvable. `SheetKey::Name` remains for unresolved names and must be invalidated/replanned on sheet creation/rename.

## 4. IngestPipeline module shape

Place the module at `crates/formualizer-eval/src/engine/ingest_pipeline.rs` and keep it `pub(crate)` initially.

```rust
pub(crate) struct IngestPipeline<'a> {
    data_store: &'a mut DataStore,
    sheet_registry: &'a mut SheetRegistry,
    names: &'a NameRegistry,
    tables: &'a TableRegistry,
    sources: &'a SourceRegistry,
    function_provider: &'a dyn FunctionProvider,
    policy: CollectPolicy,
}

impl<'a> IngestPipeline<'a> {
    pub fn new(/* borrows above */) -> Self;
    pub fn ingest_formula(
        &mut self,
        ast: FormulaAstInput<'_>,
        placement: CellRef,
        formula_text: Option<Arc<str>>,
    ) -> Result<IngestedFormula, ExcelError>;
    pub fn ingest_batch<I>(&mut self, formulas: I) -> Result<Vec<IngestedFormula>, ExcelError>;
}

pub enum FormulaAstInput<'a> { Tree(ASTNode), RawArena(AstNodeId) }

pub struct IngestedFormula {
    pub ast_id: AstNodeId,
    pub placement: CellRef,
    pub canonical_hash: u64,
    pub labels: CanonicalLabels,
    pub dep_plan: DependencyPlan,
    pub read_summary: Option<SpanReadSummary>,
    pub formula_text: Option<Arc<str>>,
}
```

The pipeline should not directly mutate `DependencyGraph` in Phase 1. It returns a plan; graph code remains responsible for placeholders, vertices, name/table attachments, range indexes, and dirty flags so existing side effects stay centralized. A later internal adapter may borrow `&mut DependencyGraph` to apply a plan, but that is not necessary for canonicalization.

Algorithm for each formula:

1. Resolve placement to sheet id and validate one-based coordinates.
2. If input is raw arena and labels say `NeedsPlacementRewrite`, reconstruct at placement; otherwise walk raw arena directly. If input is tree, use it directly.
3. Apply structured-ref rewrite inside the pipeline before canonical interning. Errors match current `rewrite_structured_references_for_cell` exactly (`#NAME?`, `#REF!`, `NImpl` messages).
4. Walk bottom-up once. For each node, intern children first, compute labels/rejects, compute canonical hash, collect dependency references/ranges/names/tables into a per-formula plan row, and insert canonical node into the arena using canonical data as the dedup key.
5. Return `IngestedFormula`; callers decide span placement or graph materialization.

Error semantics: parse errors remain outside the pipeline in existing loader code. Placement/rewrite/planning errors return `Err` where current `set_cell_formula` would fail; authoritative ingest converts per-candidate unsupported labels into fallback records, not errors, exactly as today. Missing raw arena ids are `ExcelErrorKind::Value` with the current message.

## 5. Migration phases

| Phase | Scope | Est. diff / files | Risk | Tests | Validation gate |
|---|---|---|---|---|---|
| 1. Additive substrate | Add `AstNodeEntry`, `AstNodeMetadata`, `CanonicalLabels`, canonical interning mode, `IngestPipeline` returning unused results. No call-site switch. | 1.5-2.5k LOC; arena, data_store, new module, module exports. | Medium | Unit tests for hash/labels/ref normalization. | Full suite; no measurable perf change. |
| 2. Parity infrastructure | Add old-vs-new harness over fixtures/probes; map old payload strings to new hashes via diagnostics table. | 0.8-1.2k LOC; tests plus debug adapters. | Medium | Integration parity tests default-on. | Any dep/hash/eval/rewrite/display divergence blocks. |
| 3. Hot ingest switch | Route `BulkIngestBuilder::finish`, `Engine::set_cell_formula`, `bulk_set_formulas`, shadow/authoritative analysis through pipeline. Keep old code behind parity/cfg. | 1.5-2k LOC; eval, graph, ingest_builder, placement. | High | Existing suite plus probe scenarios. | Full suite; probe no regressions; no-span load at or near 1.0x. |
| 4. Cold path switch | Named/source/table definitions that contain formulas, staged build paths, undo/redo apply, structural dependency rebuilds use pipeline after existing tree rewrites. | 1-1.5k LOC; graph/sheets/editor/eval. | Medium-high | Structural, named range, undo/redo parity tests. | Full suite; structural FormulaPlane probes. |
| 5. Cleanup | Remove standalone canonicalizer/summary if dead, update FormulaPlacementCandidate/Template to consume metadata, document `intern_formula_ast`. | -1k to -2k LOC net; formula_plane modules/docs. | Medium | Remove parity dependency on deleted code after golden coverage. | Full suite; public API docs updated. |

## 6. Library API compatibility

| Public item | Type/signature change? | Behavior change? | Compatibility decision |
|---|---:|---:|---|
| `FormulaIngestRecord { row, col, ast_id, formula_text }` | No | `ast_id` may be raw on input; internally canonical id is produced. | Stable; do not expose new fields. |
| `FormulaIngestBatch` / `FormulaIngestReport` | No | Counters should remain semantically identical. | Stable; optional future diagnostic counters only additive. |
| `Engine::ingest_formula_batches` | No | Faster canonical path; same fallback/materialization behavior. | Stable. |
| `Engine::set_cell_formula`, `set_cell_formula_ref` | No | Volatile/dynamic/rewrite results must match. | Stable; pipeline errors mirror current graph errors. |
| `Engine::bulk_set_formulas` | No | Same return count and graph effects. | Stable. |
| `Engine::intern_formula_ast` / `engine::Engine::intern_formula_ast` wrapper | No | Remains raw literal interning; not a canonical family id. | Stable but document semantics; consider deprecation only after adding a public placed ingest API. |
| `DependencyGraph::set_cell_formula`, `bulk_set_formulas`, `store_ast`, `get_formula_ast` | No public signature change. | Internal implementation may use pipeline. | Stable. |
| Workbook backends `calamine`, `umya`, `json` | No | Continue parse -> intern -> record -> ingest. | Stable; later may call a convenience API but no consumer breakage. |
| Public workbook formula cache APIs | No | None. | Out of FP8 path. |
| Test helpers constructing `ASTNode` / records | No | Raw ids still accepted. | Stable. |

## 7. Parity test harness specification

Add `crates/formualizer-eval/tests/fp8_ingest_pipeline_parity.rs` plus a small `cfg(test)` adapter exposing old canonical payloads and new metadata. It runs by default in `cargo test`; an env var `FZ_FP8_PARITY_PROBES=1` can add larger generated probe scenarios, but the default corpus must stay under a few seconds.

Inputs:

* Formula strings from existing FormulaPlane tests and dependency-summary tests.
* Workbook-loader fixtures through `FormulaIngestBatch` construction.
* Generated scenarios from `probe-fp-scenarios`: relative runs, absolute anchors, mixed anchors, cross-sheet refs, ranges, names, tables, structured `#ThisRow`, volatile/dynamic functions, LET/LAMBDA, arrays, parse-recovery formulas.

Assertions per formula/batch:

* Old `canonicalize_template` payload/reject labels maps to new `canonical_hash`/labels using a deterministic side table in the test.
* Dependency plans match: targets, cells, ranges, names, tables, flags, unresolved names, placeholders requested.
* Placement decisions match: accepted spans, fallback reasons, domains, internal-dependency rejection.
* Structural-ref rewrite success/error and rewritten display text match.
* Evaluation results after materializing both paths into fresh engines match for first eval and one edit cycle.
* Reconstructed/display formula text matches existing behavior when `formula_text` is absent; when present, preservation is exact.

Divergence output must include sheet/cell/formula text, old payload, new hash, old/new labels, old/new deps, and a minimized node path if hash contribution differs. Any divergence is a blocker, not a warning.

## 8. Risk inventory

| Risk | Mitigation | Gate |
|---|---|---|
| Hidden AST walkers missed (display, undo, debug). | Inventory above; keep raw APIs and reconstruct-at-placement. | Full suite plus parity display assertions. |
| LET/LAMBDA scoping changes dependency extraction. | Keep LET/LAMBDA rejected for spans; graph planner retains scoped extraction until pipeline has equivalent tests. | LET/LAMBDA parity cases. |
| Structured-ref rewrite depends on placement/table context. | Rewrite inside pipeline before canonical intern; exact error messages preserved. | Table `#ThisRow` parity and existing table tests. |
| Volatile/dynamic flags differ because function provider differs from global registry. | Pipeline receives engine `FunctionProvider` and falls back as current engine helper does. | Volatile/dynamic tests and random builtins tests. |
| Cross-sheet `SheetKey::Id` vs `Name` changes hash across renames. | Use ids for resolved sheets; record unresolved-name label; replan on sheet registry changes. | Sheet rename/remove tests. |
| Canonical ids stored by external users. | `intern_formula_ast` remains raw and stable; canonical ids are internal results. | API compatibility tests. |
| Tests assert canonical payload strings. | Parity harness documents mapping; cleanup updates tests to labels/hash diagnostics. | Phase 2/5 gates. |
| Structural ops on canonical refs. | Defer arena-native rewrite; reconstruct literal at placement, use existing adjusters, re-ingest. | Structural op suite. |
| Hash collision merges formulas. | Dedup verifies canonical `AstNodeData` equality plus metadata; optional debug collision assertion. | Arena unit tests/fuzz cases. |

## 9. Rejected alternatives

**Mirror `canonicalize_template` as a parallel arena walker.** This might reduce immediate retrieve/clone overhead, but it preserves two sources of truth and duplicates 1500+ lines of subtle reference/function/reject logic. FP8 should move the logic into interning, not maintain a second canonicalizer forever.

**Pre-group by raw formula text with hand-written A1 detection.** This is attractive for no-span singleton short-circuiting but correctness-fragile. Equivalent relative formulas can have different text, and identical text at different placements can mean different dependencies. It would also miss mixed anchors, structured refs, names, and locale/function normalization.

**Merge canonicalization into the parser.** The parser lacks engine context: sheet ids, table metadata, names, sources, function provider capabilities, and formula placement. Parser-level canonicalization would either be incomplete or would force engine state into parsing, breaking layering.

## 10. Out of scope

FP8 does not implement arena-native structural AST rewrite, live table-edit formula transforms, span-aware vectorized kernels, uniform-value broadcast, direct DenseRange writes for spans, removal of the legacy `DependencyGraph`, graph proxy vertices for spans, arena GC/compaction, public API redesign, or parser changes. Those become easier after FP8 because canonical labels and hashes are arena-resident, but they are separate workstreams.
