# Formula Family Anchor-Once Ingest

Status: Implemented and validated on `perf/calamine-036-single-pass`

## 1. Decision

The next milestone replaces per-descendant materialization for clean explicit OOXML shared-formula families in `AuthoritativeExperimental` mode. The Calamine worksheet XML stream is still consumed once, but a clean family is represented by its anchor, declared rectangle, and compressed observed-placement evidence. Its anchor formula is normalized, parsed, canonicalized, and dependency-analyzed once. FormulaPlane placement is prepared from that one analysis plus a relocation descriptor and then committed directly.

The compatibility rule is stricter than the optimization:

> A family may use anchor-once authority only when the source evidence and every existing FormulaPlane semantic gate prove that all descendants are exact relocations of one supported anchor. Otherwise the complete family follows the current per-cell oracle, including its ordering, text expansion, parse-policy, graph, value, edit, and dependency behavior.

This milestone does not infer families from ordinary formulas, add FormulaPlane domains with holes or exceptions, reconstruct arrays or data tables, or redesign adaptive partitioning.

## 2. Feasibility and required replay boundary

A strictly one-pass implementation that neither retains records nor provides replay is not correct. XML order is not guaranteed; an anchor may arrive after descendants, a duplicate anchor or member may arrive at end of sheet, and an ordinary formula may late-conflict with a previously clean declared range. Once such an anomaly appears, exact fallback needs every earlier member coordinate and the anchor history used by the current sequence-sensitive expander. Calamine's descendant metadata contains only `si`, not expanded text. That information cannot be recreated from the last record alone.

Calamine 0.36 already exposes the minimum upstream facts through `next_cell_with_formula_metadata` and exposes the authoritative expansion implementation through `expand_shared_formula_into`. No upstream API change is required for this milestone. An upstream API that emitted a stable, owned shared-formula relocation program would simplify ownership, but it would not eliminate the need to retain or replay earlier records after a late anomaly.

The simplest robust approach is therefore a sheet-local replay spool:

- consume each Calamine cell record exactly once;
- append a compact source-formula record to a sequential spool before discarding borrowed Calamine data;
- retain in memory only anchors, compressed placement/occupancy evidence, exceptions, and bounded diagnostics;
- delete the spool without replay after every family on the sheet commits directly;
- replay it once only when one or more families or ordinary records require exact per-cell materialization.

Native builds use an anonymous temporary file after a small memory prefix (recommended 1 MiB). Environments without filesystem-backed temporary storage use bounded chunked memory through the same `FormulaReplaySpool` interface. The in-memory backend must have an explicit byte limit derived from `WorkbookLoadLimits`; exhaustion is a typed load-limit error, not silent formula loss. Reopening or rereading the XLSX worksheet is not recommended: it violates the one-Calamine-record-pass goal, depends on archive seek/reopen behavior, and doubles XML decoding. Copying Calamine's formula rewriter into `formualizer-eval` is also rejected because it creates two semantic authorities.

The spool is not a retained source event per descendant in heap memory. It is bounded RSS, sequential recovery state. Clean-family steady-state heap approaches O(families + runs + exceptions), while fallback is permitted O(formula count) work and spool bytes.

### 2.1 Adversarial-review amendments

These amendments are normative where later explanatory sections conflict:

1. Initial direct promotion accepts only the monotonic stream fast path. Any coordinate disorder, forward anchor, evidence-cap uncertainty, or ownership ambiguity replays the sheet/family exactly. Arbitrary-order direct promotion requires a later external-sort design.
2. A directly promoted XLSX family must have exactly one non-empty anchor at `declared_range.start`; merely containing the anchor is insufficient for this milestone.
3. Anchor-once authority begins with a statically proven formula subset whose parsed-reference relocation is differential-tested against Calamine at domain boundaries. Unsupported syntax or any possible coordinate overflow replays. Source metadata alone never proves semantic relocation.
4. Anchor-once placement uses a new compressed preparation/commit/report API. It must not call APIs that allocate one `CandidateAnalysis`, result, binding, or coordinate-map entry per descendant.
5. Initial formula API parity means placement-correct virtual AST lookup and existing canonical-print behavior. Raw source spelling is not baseline behavior. If source provenance is later retained, it is span-scoped—not template-scoped—and structural rewrites drop or replace it.
6. Runtime schedule cycle discovery remains post-commit. The direct span must therefore carry enough anchor AST relocation state to demote exactly; this milestone does not claim sheet-local preflight of cross-span or cross-sheet cycles.
7. Initial authority is eager-only and family-atomic. Deferred spool/package lifecycle and full sheet rollback are later serial milestones, not prerequisites hidden inside the first authority swatch.
8. Spooling requires explicit per-sheet and per-workbook byte/file limits, secure native-file creation and cleanup, a no-disk policy, and a separately bounded memory/WASM backend before production use.
9. Evidence-cap exhaustion forces replay for every unresolved family on the sheet; classification never continues with incomplete occupancy.
10. The benchmark harness must run one cold child per mode and distinguish collector/spool/preparation memory from whole-process RSS before percentage gates become release gates.

## 3. Current baseline and measured problem

The implemented baseline is documented in `docs/architecture/formula-family-stream-ingest.md`. Relevant seams are:

- `crates/formualizer-workbook/src/backends/calamine.rs` reads `next_cell_with_formula_metadata`, builds one `FormulaSourceEvent` per formula, then `expand_source_events_per_cell` allocates one `Arc<str>` for every shared descendant.
- `crates/formualizer-eval/src/engine/formula_source.rs` owns the full event vector beside the full `FormulaIngestBatch`.
- `crates/formualizer-eval/src/engine/formula_family.rs` builds compact indices, but only after all source events and expanded records exist.
- `crates/formualizer-eval/src/engine/eval.rs` parses every expanded formula, creates one `FormulaPlacementCandidate`, calls `IngestPipeline::ingest_formula` for every cell, and only then groups source families.
- `crates/formualizer-eval/src/formula_plane/placement.rs` now separates `prepare_family_placement` and `commit_prepared_family`, but preparation still requires one `CandidateAnalysis` and literal-binding entry per placement.
- `FormulaPlane::TemplateStore` retains one origin AST and formula text for a promoted template; current placement already avoids descendant graph vertices after promotion.

The report at `scratch/formula-family-ingest-bench/REPORT.md` shows why the next step is needed. At 100k, authoritative load still takes 1040.68 ms versus 1066.39 ms Off and retains an estimated 6,000,256 bytes of collector evidence. At 1M, the 60,000,256-byte estimate exceeds the 8 MiB family cap, all cells fall back, authoritative load takes 16.245 s, and the two-mode process reaches 2,593,204 KiB RSS. The target is to remove per-descendant expanded strings, events, AST/analysis records, staging entries, and placement vectors from the clean path rather than merely compacting a second index.

## 4. Compatibility invariants

1. **Exact oracle fallback.** Replay uses the current `expand_shared_formula_into` algorithm and current source-sequence semantics. It does not pretty-print or independently reinterpret a shared formula.
2. **Formula API parity.** Formula reads for every promoted placement produce the same placement-relocated AST and canonical `Workbook::get_formula` output as the current per-cell path. Preserving raw Calamine source spelling is optional provenance work and is not required for initial authority.
3. **Value authority.** Cached values from formula-bearing records never enter Arrow literal storage. Literal/formula duplicate-coordinate behavior remains the current sequence-pinned behavior.
4. **Semantic authority.** Source `si`, declared range, and compressed runs select a candidate set only. FormulaPlane canonicalization, dependency summary, dirty projection, internal-dependency, cycle, binding-memory, minimum-domain, shape, and mode gates remain authoritative.
5. **No partial family.** A source family is prepared and committed atomically. Any source, parse, canonical, placement, transaction, or limit rejection routes all surviving members through the oracle.
6. **Order correctness.** Every XML permutation preserves values and formula API behavior through exact replay. Initial direct promotion is limited to the monotonic streaming fast path; disorder, including a forward anchor, conservatively replays. Permutation-invariant direct promotion is deferred until external sorting or equivalent bounded evidence exists.
7. **Declared range is evidence, not occupancy.** No formula is synthesized solely because a coordinate lies in `ref`; no work scans declared area.
8. **Modes.** `Off` remains the exact per-cell path. `Shadow` may run compressed classification and anchor preparation but writes no FormulaPlane authority. `AuthoritativeExperimental` alone may direct-commit. Eager authority lands first; deferred packages remain replay-only until their separate ownership/indexing milestone passes parity gates.
9. **Limits.** Source records count at the existing populated-cell boundary. A large declared area does not count as observed cells. Spool and evidence limits cannot turn a formula into a blank or a partial span.
10. **Transactions.** Initial authority guarantees family-level atomicity: fallback graph planning and every placement preflight complete before a family span commits. Existing sheet-level mutation semantics remain unchanged. Full Arrow/graph/diagnostic/stat rollback is a separate engine transaction project and is not claimed by this milestone.

## 5. Source-side representations and ownership

### 5.1 Sequential replay spool

Add private workbook-side types in `crates/formualizer-workbook/src/backends/calamine/formula_replay.rs` (or a private module beside `calamine.rs`):

```rust
trait FormulaReplaySpool {
    fn append(&mut self, record: SpoolFormulaRecord<'_>) -> Result<SpoolOffset, SpoolError>;
    fn replay(&mut self) -> Result<FormulaReplayIter<'_>, SpoolError>;
    fn encoded_bytes(&self) -> u64;
    fn storage_kind(&self) -> SpoolStorageKind;
}

enum SpoolFormulaRecord<'a> {
    Ordinary { sequence: u64, coord0: SourceCoord, text: &'a str },
    SharedAnchor {
        sequence: u64,
        coord0: SourceCoord,
        shared_index: usize,
        declared_range: Option<SourceRect>,
        text: &'a str,
    },
    SharedDescendant { sequence: u64, coord0: SourceCoord, shared_index: usize },
    Unsupported { sequence: u64, coord0: SourceCoord },
}
```

Records use a versioned length-delimited binary encoding with checked varints, not `bincode`/unstable Rust layout. Cached values need not be spooled after the formula-value suppression decision and source counters are recorded, because neither direct placement nor fallback consumes them. If provenance APIs later require cached values after load, add a tagged optional payload without changing version 1 decoding.

The adapter owns the spool from first formula record through engine disposition and fallback replay. A replay iterator borrows it; no engine object stores a file handle. Temporary files are created with owner-only permissions, unlinked where supported, never logged by path, flushed before replay, and removed on success, error, or unwind.

### 5.2 Compressed sheet evidence

Replace `Vec<FormulaSourceEvent>` as the collector input with streaming builders:

```rust
struct SharedFamilyBuilder {
    id: SourceFamilyId,
    anchors: SmallVec<[AnchorDescriptor; 1]>,
    declared_range: Option<SourceRect>,
    placements: PlacementRunBuilder,
    member_count: u64,
    duplicate_count: u64,
    out_of_range_count: u64,
    first_sequence: u64,
    last_sequence: u64,
    anomaly_bits: FamilyAnomalyBits,
}

struct CompressedFamilyEvidence {
    id: SourceFamilyId,
    anchor: AnchorDescriptor,
    declared_range: SourceRect,
    observed: PlacementRuns,
    member_count: u64,
    anomaly_bits: FamilyAnomalyBits,
}

enum PlacementRuns {
    RowRuns(Vec<RowInterval>),
    ColRuns(Vec<ColInterval>),
    RectRows(Vec<RowInterval>),
    Points(Vec<SourceCoord>),
}
```

`PlacementRunBuilder` initially accepts only monotonic coordinate order and coalesces the current run with constant state. A backward coordinate, forward anchor, or ordering ambiguity marks every unresolved family on the sheet replay-required; direct promotion does not attempt an ordered row map. For a valid row-major rectangle, finalization proves checked area equals unique count and each completed row covers the declared columns. For a row or column run it proves contiguous endpoints and count. Evidence-cap exhaustion likewise forces sheet replay and stops optional evidence growth.

A sheet-level compressed formula occupancy index receives every formula coordinate, including ordinary formulas and other families. It stores merged row intervals plus a bounded map for duplicate owners. Rectangle conflict checks query intervals; they never enumerate missing declared cells. Mixed shared/ordinary formulas, duplicate records, and cross-family coordinate conflicts set anomaly bits on all affected families.

Ownership is sheet-local. Builders borrow no Calamine strings. `AnchorDescriptor` owns exactly one `Arc<str>` per observed anchor; ordinary formulas exist only in the spool until replay. Finalized evidence moves into an engine batch and is dropped after commit or fallback. Forward descendants need only their coordinate/run evidence and spool record; they do not allocate pending expanded text.

### 5.3 Engine transport

Replace the descendant-oriented `FormulaSourceIngestBatch` use for this path with an additive hidden API; keep existing constructors for other backends/tests:

```rust
pub struct FormulaAnchorOnceBatch {
    pub sheet_name: Arc<str>,
    pub families: Vec<CompressedSharedFamily>,
    pub ordinary_formula_count: u64,
    pub source_counters: FormulaSourceCounters,
}

pub struct CompressedSharedFamily {
    pub source_id: SourceFamilyId,
    pub anchor_coord0: SourceCoord,
    pub anchor_text: Arc<str>,
    pub declared_range: SourceRect,
    pub observed_domain: CompressedPlacementDomain,
    pub member_count: u64,
    pub source_verdict: SourceFamilyVerdict,
}
```

No descendant text, `FormulaSourceEvent`, `FormulaIngestRecord`, AST ID, or analysis crosses this seam for a source-clean family. Source-rejected families are represented by count/reason only; their formulas arrive later through replay as ordinary `FormulaIngestBatch` fallback records.

## 6. Stream, classify, and replay algorithm

For each sheet:

1. Create local Arrow ingest state, source counters, compressed occupancy, family builders, and replay spool.
2. For each Calamine record, enforce coordinate and populated-cell limits immediately. For a formula, suppress its cached value, append one compact spool record, and update counters/evidence. For a literal, use the existing dense/sparse Arrow path and duplicate-coordinate rules.
3. At end of XML, finalize family builders without area scans. A source-clean family requires a monotonic stream, one non-empty anchor exactly at the valid declared range start, unique occupancy exactly equal to the range, no out-of-range member, and no ordinary/unsupported/other-family conflict.
4. Submit only source-clean compressed families to anchor preparation. Source-rejected families are marked replay-required.
5. If all formula records direct-commit and there are no ordinary formulas, discard the spool. Otherwise replay it once. During replay, run the extracted current oracle state machine: ordinary text is emitted directly; descendants before an anchor queue by `si`; each anchor updates the same map and drains the same pending coordinates; later descendants use the current anchor. Use `expand_shared_formula_into` into one reusable `String`, then normalize and immediately parse/stage/materialize. Do not retain expanded strings beyond the fallback record's existing lifetime.
6. Filter replay output by disposition: members of committed families are skipped; every ordinary formula and every member of a fallback family is emitted. Duplicate-coordinate resolution is applied exactly where the current oracle applies it, before committing either authority.

A family initially source-clean can still receive `ReplayRequired` from engine preparation because of parse policy or a FormulaPlane gate. That is the late semantic fallback case; the spool remains available until all dispositions are known. A spool error or unrecoverable missing anchor fails the sheet before commit according to the pinned current policy. The current silent omission of a never-anchored descendant must not change in this milestone unless separately approved; tests record it explicitly.

## 7. Anchor-once semantic preparation

### 7.1 Parse and canonical analysis

Add an engine-private preparation API in `engine/formula_ingest.rs` and `formula_plane/placement.rs`:

```rust
fn prepare_anchor_once_family(
    anchor: FormulaPlacementCandidate,
    observed: CompressedPlacementDomain,
    ctx: &mut FormulaIngestPipeline,
) -> Result<PreparedAnchorOnceFamily, FamilyFallbackReason>;

fn prepare_relocated_family_placement(
    anchor: AnchorAnalysis,
    domain: PlacementDomain,
) -> Result<PreparedFamilyPlacement, PlacementFallbackReason>;
```

Normalize anchor text exactly once and invoke the configured `FormulaParsePolicy` once. Recovery/no-AST rejects direct placement and triggers replay, so fallback obtains the same per-cell diagnostics as today. Successful parse invokes `IngestPipeline::ingest_formula` once at the anchor, producing one AST, canonical keys, slot descriptors, dependency plan, read projections, named references, and template slot map.

A separate safe-syntax validator then proves every relative reference remains inside XLSX bounds over the complete domain and rejects names, token forms, or operators whose Calamine lexical relocation is not known to match AST relocation. Property tests compare Calamine-expanded-and-parsed formulas with relocated anchor ASTs at corners and boundary classes. The validator is an allowlist that grows only from differential evidence; unsupported syntax replays.

### 7.2 Reusing every placement gate

Do not create a second permissive placement implementation. Refactor current `prepare_family_placement` around a private `FamilyPlacementFacts` provider used by both paths:

```rust
trait FamilyPlacementFacts {
    fn domain(&self) -> Result<PlacementDomain, PlacementFallbackReason>;
    fn anchor(&self) -> &CandidateAnalysis;
    fn member_count(&self) -> u64;
    fn canonical_equivalence(&self) -> Result<(), PlacementFallbackReason>;
    fn binding_encoding(&self) -> Result<SpanBindingSet, PlacementFallbackReason>;
    fn projected_reads(&self) -> Result<SpanReadSummary, PlacementFallbackReason>;
}
```

The existing per-cell provider keeps current behavior. The anchor-once provider is valid only for a source-clean OOXML shared family and derives facts as follows:

- **Shape/duplicates/gaps:** convert the proven compressed observed domain to the existing `PlacementDomain::{RowRun, ColRun, Rect}`; singleton and unsupported shape reasons are unchanged.
- **Canonical support/equivalence:** run the existing canonicalization labels and exact/parameterized key checks on the anchor. Validate the anchor arena AST with the existing relocatability walk. OOXML shared descendants are defined as reference relocation of that anchor; no descendant can introduce a different operator, function, literal, name, or slot shape. The source-clean proof plus relocatability substitutes for N identical canonical analyses.
- **Dependencies/dirty projection:** obtain the existing dependency summary/read projections at the anchor, then project them over the whole existing domain using the same `SpanReadSummary` and `ResultRegion` constructors. Run current unsupported-sheet, named-reference, internal/self-dependency, and dirty-projection checks against the whole domain.
- **Literal/value-reference bindings:** shared-formula relocation changes references only. Anchor literal bindings are broadcast; value-reference slots use the existing relative/absolute descriptors and template slot map. Add `LiteralBindingEncoding::Broadcast` (or represent it as one dictionary value with no placement-ID vector) so no vector proportional to member count is allocated. Apply the unchanged `MAX_BINDING_SET_BYTES` to the encoded payload.
- **Minimum size/constant result:** use the unchanged threshold and constant-result exception.
- **Cycles:** retain current schedule-build cycle demotion. Demotion must call the formula text relocation/replay materializer before graph authority is installed; it cannot assume descendant ASTs already exist.
- **Commit:** `PreparedFamilyPlacement` owns one candidate/analysis, the existing domain/result/read/binding records, and counts. `commit_prepared_family` remains the only mutating step and is infallible after preflight.

Acceptance requires gate-by-gate differential tests proving that the per-cell and anchor-once providers return the same accept/fallback class for generated relocatable families. Hash equality alone is never accepted; the exact canonical key remains in the template record.

## 8. Virtual AST lookup, edits, and demotion

Initial authority does not preserve raw Calamine spelling. `Workbook::get_formula` already canonical-prints an AST, so the compatibility requirement is a placement-correct virtual AST. A promoted span stores its anchor AST, anchor coordinate, and proven relocation policy. FormulaPlane must resolve authority before graph lookup and relocate that AST to the requested placement with the existing AST relocation machinery.

The accepted syntax subset must prove, with checked domain-wide bounds, that FormulaPlane AST relocation and Calamine lexical expansion parse to equivalent ASTs at every boundary class. References that can move outside XLSX bounds, unsupported names/token forms, or any differential disagreement force replay. This gate is deliberately narrower than Calamine's shared-formula syntax in the first release.

Editing one placement materializes that placement's relocated AST through existing FormulaPlane edit logic and leaves unaffected placements virtual. Pure domain splits copy the anchor relocation descriptor. A structural operation that rewrites the template replaces the descriptor with the rewritten AST state; it must not retain stale source provenance. Whole-span and scheduler cycle demotion may spend O(member count) to materialize placement ASTs. Runtime cycle discovery remains post-commit, so this demotion path is a mandatory authority prerequisite.

If exact source spelling becomes a product requirement later, add a span-scoped provenance recipe. Never attach source spelling or a Calamine relocator ID to an interned template: canonically identical templates may originate from different anchors and spellings.

## 9. Deferred, Off, and Shadow behavior

- **Off:** do not build family analyses or authority. Stream to the spool and replay once into the current per-cell eager/deferred path. This removes the current full event vector but intentionally remains O(formula count). Source counters remain populated; promotion counters are zero.
- **Shadow:** finalize compressed evidence and run anchor-once preparation without commit. Replay all formulas into the current graph path. Compare shadow disposition with the per-cell placement oracle in tests. Shadow memory is O(families + runs + exceptions) plus spool, not O(descendant events).
- **AuthoritativeExperimental eager:** prepare compressed families first, replay only ordinary/rejected families, then atomically commit direct spans and graph fallback.
- **Deferred:** remains spool-backed replay-only through the eager authority milestone. A later `DeferredFormulaSheetPackage` owns a sealed, non-clone spool and compressed batch with explicit move/drop/index semantics. Only after Phase 7 may selected deferred builds use direct authority. Interactive replacement marks the affected family replay-required; rename updates package identity; remove drops package and spool; selected-sheet build never consumes another sheet's spool.

After Phase 7, eager and deferred reports must reconcile after build. Staging formula reads use bounded spool indexing; they never require retained descendant expanded text.

## 10. Family disposition and failure boundaries

Initial authority provides a family-level disposition transaction, not full sheet rollback. For each source family, all fallible source validation, anchor parsing, dependency analysis, compressed placement preparation, formula relocation preflight, and fallback graph planning complete before its span commits. A family disposition is therefore either one prepared direct span or a complete replay/materialization plan.

Arrow construction, sheet-shell reservation, immutable AST interning, diagnostics, and workbook-wide defined-name sequencing retain current loader behavior in this milestone. The implementation must not describe those existing mutations as transactional. A later engine project may add reversible `PreparedSheetIngest`, but eager direct authority is gated only on ensuring no span commits before its exact fallback plan can still succeed.

Cross-span and cross-sheet cycles remain discovered during the existing mixed-schedule build after spans exist. The span's anchor AST relocation descriptor—not a live spool—is the guaranteed demotion source. Runtime cycle demotion materializes placements through that descriptor and follows current authority teardown. Failure injection must prove this path cannot leave a span and legacy vertices simultaneously authoritative.

Deferred spool ownership and full sheet/package lifecycle are isolated in Phase 7. No eager authority code may borrow a deferred file handle or rely on cloning a spool.

## 11. Anomalies and limits

The following always force exact family fallback: forward anchor with no eventual anchor, duplicate or empty anchor, duplicate member/coordinate, missing or invalid declared range, hole, ordinary/other-family exception, out-of-range descendant, unsupported metadata, parse recovery/rejection, unbuildable text recipe, unsupported existing domain, or any existing placement gate.

Forward anchors remain semantically valid through exact replay but are not eligible for the initial monotonic direct path. XML order and `si` order never finalize a family early. A declared range may be huge, but occupancy equality is proved from checked area, bounds, counts, and compressed intervals. Holes and exceptions are diagnostic facts only; this milestone does not create rich FormulaPlane domains for them.

Recommended internal defaults:

- 8 MiB compressed evidence per family and 64 MiB per sheet, retained from the current policy but now charged to actual runs/exceptions rather than members;
- 1 MiB in-memory spool prefix, then native tempfile;
- explicit no-filesystem spool byte cap no greater than the configured load byte budget; add a `max_formula_spool_bytes` load limit if no existing byte budget can safely derive it;
- 64 retained coordinate samples per fallback reason, exact saturating counters;
- checked `u64` encoded-byte, area, member, and offset accounting.

Evidence-cap exhaustion marks affected families fallback-required and stops optional evidence growth. It does not discard spool records. Spool-cap exhaustion is a load error because exact fallback can no longer be guaranteed. Actual coordinates still enforce row/column/logical-cell limits at observation; declared ranges cannot evade or inflate those counters.

## 12. Telemetry and rollback

Extend `FormulaIngestReport` and benchmark output with saturating counters:

- `source_formula_records_spooled`, `source_spool_encoded_bytes`, `source_spool_peak_memory_bytes`, `source_spool_spilled_bytes`, `source_spool_replays`;
- `source_placement_runs`, `source_placement_exceptions`, `source_evidence_peak_bytes`;
- `source_anchor_parses`, `source_descendant_parses_avoided`, `source_descendant_asts_avoided`, `source_descendant_strings_avoided`, `source_descendant_events_avoided`, `source_staging_entries_avoided`;
- `source_family_anchor_once_prepared`, `source_family_promoted`, `source_family_promoted_cells`, `source_family_replayed`, `source_family_fallback_cells`;
- exact fallback keys including all current source and `PlacementFallbackReason` gates plus `TextRecipeUnsupported`, `SpoolLimit`, `SpoolIo`, and `TransactionPreflight`.

`AdapterLoadStats.formula_cells_observed`, `formula_cells_handed_to_engine`, and `shared_formula_tags_observed` retain their present meanings. A direct family still counts every observed and handed-to-engine formula even though only one AST is parsed. Logs expose counts, storage kind, and bounded coordinates, never formula text, cached values, or spool paths.

Rollback is operationally simple: keep the existing anchor-once path behind `FormulaPlaneMode::AuthoritativeExperimental`; retain the source-aware per-cell replay entry point; and provide a crate-private force-replay test hook. Any preparation uncertainty returns a fallback reason. A release rollback can disable anchor-once dispatch without changing files or workbook semantics. Do not add a public force-promotion switch.

## 13. Differential oracle

Extract the current sequence-sensitive expander in `calamine.rs` into the spool replay module before changing behavior. It remains the only fallback oracle and calls `expand_shared_formula_into`. For each corpus workbook, run:

1. Off/per-cell spool replay;
2. Shadow compressed preparation plus replay;
3. authoritative forced replay;
4. authoritative anchor-once candidate.

Compare sorted cell formula text, formula AST canonical output, parse diagnostics, evaluated values/errors, dependency edges/read regions, graph/span counts, edit results, dirty propagation, dimensions, duplicate-coordinate outcome, load stats, and source report reconciliation. For direct authority assert `promoted_cells + replayed_fallback_cells` equals oracle formula cells after current duplicate resolution.

Property tests generate valid family rectangles/runs, absolute/relative/mixed references, strings that resemble references, quoted sheet names, ranges, numeric/string/error literals, XML permutations, forward anchors, late duplicates, holes, conflicts, repeated worksheet-local `si`, and boundary coordinates. Valid-family permutation tests require identical output. Malformed tests compare exact source-sequence oracle behavior.

Text recipe tests compare every generated member against Calamine, not merely canonical AST equality. Placement-provider tests compare all existing fallback reasons between N-cell analysis and anchor-once facts; any disagreement defaults to fallback.

## 14. Revised serial test-driven phases

Each phase is one reviewable writer swatch. Later authority remains disabled until every earlier fallback and demotion gate is green.

### Phase 0: freeze the adapter oracle

**Red tests:** malformed order; duplicate/missing/empty anchors; anchor not equal to range start; duplicate formula and formula/literal coordinates in both orders; boundary references; forward anchor; non-monotonic rows; unsupported metadata; every parse policy.

**Change:** extract the current sequence-sensitive expansion state machine without changing allocation or behavior. Pin the current adapter oracle—not Calamine's convenience-reader range-start behavior—as compatibility authority.

**Gate:** byte/file paths, eager/deferred, and all FormulaPlane modes match current formulas, canonical output, values, diagnostics, dimensions, statistics, and errors.

### Phase 1: bounded in-memory replay codec

**Red tests:** versioned round-trip; maximum coordinates; malformed/truncated records; checked offsets and lengths; replay parity; bounded parse-cache parity; missing-anchor omission; injected append/replay failures.

**Change:** encode source formula records sequentially behind `FormulaReplaySpool`; begin with memory chunks only. Replay feeds the unchanged per-cell path and preserves the current parse cache.

**Gate:** every mode still replays; 100k forced replay stays within 10% wall time and measured memory noise. No direct authority or tempfile dependency.

### Phase 2: explicit limits and secure native spill

**Red tests:** per-sheet/per-workbook encoded-byte and file-count limits; memory-prefix release; secure create-new/owner-only permissions; flush/seek; cleanup on success/error/unwind; disk-full/I/O failure; no-disk policy; bounded memory/WASM backend exhaustion.

**Change:** add explicit `WorkbookLoadLimits` spool limits and typed errors, native secure tempfile spill, workbook aggregate accounting, and bounded no-filesystem backend.

**Gate:** no unbounded spool exists in any target. Security and cleanup behavior is directly observed. Production still replays every formula.

### Phase 3: monotonic compressed evidence fast path

**Red tests:** vertical/horizontal/rectangle run construction; anchor-first monotonic streams; late holes/conflicts/duplicates; range-start mismatch; evidence cap; many tiny families; full-sheet/two-point input. Forward anchor or any coordinate disorder must mark replay-required rather than allocate an ordered row map.

**Change:** stream constant-state runs and occupancy alongside the spool. Source-clean requires one non-empty anchor at range start, exact checked occupancy, and no conflict. On disorder or incomplete evidence, stop optional growth and replay every unresolved family on that sheet.

**Gate:** 1M clean vertical evidence is O(families + runs), with no descendant `FormulaSourceEvent`, expanded string, row-map entry, or coordinate vector. Off and forced replay remain exact.

### Phase 4: compressed FormulaPlane preparation in Shadow

**Red tests:** one anchor parse/AST/analysis; a narrow safe syntax matrix; checked boundary references; Calamine-expanded-versus-relocated AST property tests; canonical/dependency/dirty/internal-dependency/name/sheet/size/binding/shape gate parity; zero authority writes.

**Change:** add a dedicated compressed preparation and report API carrying domain and counts rather than per-descendant analyses/results. Refactor shared semantic gates without allocating placement maps or binding vectors proportional to members. Broadcast literals are constant-size.

**Gate:** Shadow records one parse and N-1 avoided strings/events/analyses for accepted syntax. Every disagreement or unsupported syntax replays. Existing per-cell placement behavior is unchanged.

### Phase 5: virtual AST and demotion prerequisites

**Red tests:** formula API canonical output at first/middle/last placements; value/edit parity; per-placement edit; row/column split and rewrite; whole-span demotion; post-commit mixed-schedule cycle demotion; cross-sheet references; injected relocation failures.

**Change:** make FormulaPlane authority resolve before graph formula lookup, relocate the anchor AST per placement, and materialize exact ASTs during edit/demotion. Span splits preserve relocation state; template rewrites replace it. Do not add raw source-text provenance.

**Gate:** all edit, structural, and runtime demotion paths work without spool lifetime or descendant ASTs. No template-interning provenance leak is possible.

### Phase 6: eager family-atomic authority

**Red tests:** direct RowRun/ColRun/Rect; every source and semantic fallback; fallback graph-plan failure before commit; no double ingest; ordinary/shared mixtures; reconciliation; post-load cycles; Off/Shadow unchanged.

**Change:** in `AuthoritativeExperimental` eager loading, plan all fallback graph work and compressed placement before any family commit. Commit through the compressed result API. Retain current sheet-level mutation behavior; do not claim full sheet rollback.

**Gate:** a family either has one direct span or exact per-cell fallback, never partial authority. 100k and 1M clean monotonic fixtures directly promote with one AST and zero descendant graph vertices.

### Phase 7: deferred package lifecycle

**Red tests:** package move/drop semantics; selected-sheet build isolation; rename/remove; random formula read; interactive replacement invalidation; undo/redo; spool cleanup; eager/deferred report and value parity.

**Change:** define non-clone spool ownership and bounded indexing explicitly. Deferred packages replay by default until selected; direct authority uses the same preparation only after lifecycle tests pass.

**Gate:** no package duplicates file ownership, consumes another sheet, leaks a spool, or retains descendant expanded strings. Eager and deferred dispositions reconcile after build.

### Phase 8: benchmark and rollout gate

Upgrade the probe to launch one cold child per mode and forced-replay disposition. Emit machine-readable collection/preparation/replay timing, spool bytes/storage, parses, ASTs, graph vertices, spans, allocator-tagged high-water counters, and `/usr/bin/time -v` RSS. Use five-run medians on one recorded machine.

**100k gate:** one span/AST/analysis, zero descendant records/strings/staging entries/vertices, at least 25% faster load than forced replay, at least 40% lower RSS than current authoritative baseline, and no fallback.

**1M gate:** direct promotion succeeds with one run/span/AST/analysis, at least 40% faster than forced replay, total RSS no more than 700 MiB and at least 65% below the 2,593,204 KiB baseline, and allocator-tagged formula/evidence heap below 128 MiB excluding Arrow/fixed stores.

Fallback fixtures must exactly match replay with no more than 15% overhead. If a gate fails, keep eager/deferred authority disabled; do not broaden syntax, raise limits, or weaken compatibility.

## 15. Exact file/API change map

- `crates/formualizer-workbook/src/backends/calamine.rs`: stream into spool/builders; remove clean-path event and expanded-formula vectors; produce prepared sheet input; retain Calamine expansion oracle.
- `crates/formualizer-workbook/src/backends/calamine/formula_replay.rs` (new): versioned spool, native/memory backends, sequence replay, exact expansion, cleanup.
- `crates/formualizer-eval/src/engine/formula_source.rs`: add compressed batch/domain/counter types and the hidden `FormulaTextRelocator` seam; keep legacy source-event API additive until migration tests pass.
- `crates/formualizer-eval/src/engine/formula_family.rs`: replace event-slice collector for Calamine with streaming run/occupancy builders and source verdicts; no FormulaPlane mutation.
- `crates/formualizer-eval/src/engine/formula_ingest.rs`: anchor-once batch intake, dispositions, transaction package, telemetry, replay reconciliation.
- `crates/formualizer-eval/src/engine/eval.rs`: eager/deferred package ownership, selected build, rename/remove/edit invalidation, transaction commit, virtual formula lookup before graph lookup.
- `crates/formualizer-eval/src/formula_plane/placement.rs`: `FamilyPlacementFacts`, anchor-once preparation, shared existing gates, constant-size broadcast binding, infallible commit.
- `crates/formualizer-eval/src/formula_plane/runtime.rs`: template text recipe and broadcast binding representation; no new `PlacementDomain`.
- `crates/formualizer-eval/src/formula_plane/authority.rs` and structural/edit paths: virtual text/AST resolution and exact demotion materialization.
- `crates/formualizer-workbook/tests/calamine/shared_formulas.rs` and engine FormulaPlane tests: differential, anomaly, deferred, transaction, edit, and text suites.
- `crates/formualizer-bench-core/src/bin/probe-formula-family-ingest.rs`: spool/run/parse/AST/RSS counters and acceptance output.

## 16. Review findings and residual risks

- **Blocker:** `crates/formualizer-workbook/src/backends/calamine.rs` currently expands all source events before engine disposition. Removing those strings without spool/replay would make late fallback impossible.
- **Blocker:** `crates/formualizer-eval/src/formula_plane/placement.rs` currently requires N analyses and builds placement maps/vectors proportional to N. Direct anchor-only transport cannot use it unchanged while claiming O(families + runs).
- **Blocker:** placement-correct descendant AST lookup and demotion do not exist without per-cell vertices. A validated safe-syntax subset plus virtual AST relocation must land before direct authority; raw source spelling is deferred.
- **High:** `crates/formualizer-eval/src/engine/eval.rs` currently exposes graph-only formula AST/text reads in key paths. Promoted descendants with no vertices require FormulaPlane-first lookup and tested demotion.
- **High:** current load mutation is not a full sheet transaction. This milestone guarantees only family-level fallback planning before span commit and must not claim Arrow/diagnostic/workbook rollback.
- **Medium:** native tempfile spooling bounds RSS but introduces disk capacity and I/O failure modes. Limits, cleanup, permissions, and telemetry need explicit tests; no-filesystem targets need a stated byte cap.
- **Medium:** cycle rejection occurs after initial placement during mixed schedule construction. Anchor-once spans therefore require tested AST relocation/materialization for runtime demotion; cycles are not part of sheet preflight.
- **Medium:** the benchmark baseline is one observational run. Final percentage gates require repeated cold-process medians on the same machine and separate forced-replay comparisons.

Residual risk remains in proving the initial AST-relocation allowlist equivalent to Calamine expansion across all domain boundaries. The safe rollout rule is simple: unsupported syntax, possible coordinate overflow, disorder, or any differential mismatch replays per cell. This preserves correctness while intentionally narrowing optimization coverage.

## 17. Non-goals

This milestone does not:

- infer families from ordinary formulas or merge adjacent source families;
- authorize holes, exceptions, sparse sets, or multiple runs as one FormulaPlane span;
- reconstruct array, spill, or data-table roles hidden by Calamine 0.36;
- change current `PlacementDomain`, minimum-domain policy, dependency semantics, cycle policy, graph authority, or adaptive partition design;
- promise zero disk bytes for a million-member family;
- change malformed missing-anchor behavior without separate compatibility approval.
