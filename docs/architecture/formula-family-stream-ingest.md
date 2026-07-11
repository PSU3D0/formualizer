# Formula Family Stream Ingest

Status: Implemented (conservative shared-family slice)

## 1. Decision and scope

The implemented ingest path adds a provenance-preserving event seam between a workbook
source and FormulaPlane ingest. For known Calamine 0.36 variants the seam is lossless
with respect to information exposed by Calamine; unavailable or future metadata produces
a typed error rather than an invented formula. The first producer is the Calamine 0.36
XLSX cell stream. Source-declared shared-formula families are additional placement
evidence; they do not replace FormulaPlane canonicalization, dependency analysis, or the
legacy per-cell graph path.

The governing rule is:

> Source metadata may make promotion cheaper or more conservative, but may never decide
> formula semantics or correctness. Every cell that cannot be proven safe for direct
> family promotion follows the exact existing per-cell path.

The production implementation reads each XLSX cell record once, retains a source event,
expands shared descendants with `expand_shared_formula_into`, suppresses formula cached
values from the value plane, and sends source-aware batches through centralized ingest in
both eager and deferred modes. Off mode materializes the exact per-cell path.
AuthoritativeExperimental mode may promote only a complete, eligible declared family;
every rejection uses those same expanded per-cell records. There is no public promotion
or forced-fallback toggle.

In scope:

- the source-event contract and Calamine XML stream-order contract;
- source-family collection and placement evidence;
- conservative promotion of well-formed declared shared families;
- exact fallback, limits, diagnostics, and eager/deferred parity;
- a phased, tests-first path that does not require generalized span geometry.

Not in scope for this milestone:

- changing formula evaluation semantics, the Arrow value-plane contract, graph authority,
  scheduling, dirty propagation, structural edits, or `PlacementDomain`;
- inferring families from ordinary formulas until the final phase;
- treating shared index (`si`) as a canonical template identity;
- array/spill or data-table reconstruction;
- accepting holes or exceptions into an authoritative contiguous span;
- changing the FormulaPlane minimum-domain, dependency, binding-memory, cycle, or mode
  gates; or changing workbook persistence/public interchange formats.

## 2. Implemented baseline

`formualizer-workbook` uses Calamine 0.36 and changes
`crates/formualizer-workbook/src/backends/calamine.rs` from separate value/formula ranges
to `worksheet_cells_reader` plus `next_cell_with_formula_metadata`. The loader currently:

1. observes cell records in XML order and updates dimensions from actual coordinates;
2. stages ordinary and shared-anchor text immediately;
3. keeps `shared_index -> (anchor coordinate, template)` and buffers descendants seen
   before an anchor;
4. expands each descendant with Calamine's `expand_shared_formula_into`;
5. excludes every formula record's cached `<v>` from Arrow values;
6. parses eagerly with a per-sheet text cache, or stores text in `StagedSheet` when
   `defer_graph_building` is enabled;
7. funnels eager batches and deferred `build_graph_*` batches through
   `Engine::ingest_formula_batches`.

FormulaPlane currently groups analyzed records by
`(SheetId, parameterized_canonical_hash, candidate.col)`, splits 4-connected components,
and asks `placement.rs` to accept `RowRun`, `ColRun`, or `Rect`. Canonical template and
dependency support, not source metadata, determine authority. `span_store.rs` separately
has descriptive run/hole/exception vocabulary, but it is not loader authority and must
not be repurposed as a correctness shortcut.

The implemented source seam retains declared ranges and source-family identity, handles
forward anchors, classifies duplicate/missing anchors and unknown metadata fail-closed,
and preserves exact expanded fallback records. The family collector and its 8 MiB
per-family/64 MiB per-sheet internal evidence caps are optimization-only; hitting a cap
continues per-cell ingest.

## 3. Compatibility invariants

Every phase must preserve all of these invariants.

1. **Off-mode identity.** With `FormulaPlaneMode::Off`, graph formulas, formula text,
   values/errors, parse diagnostics, dimensions, and load failures are identical to the
   Stage 1 per-cell path.
2. **Fallback identity.** A rejected family materializes exactly the same per-cell
   expanded formula records, in the same effective duplicate-coordinate semantics, as
   Stage 1. Fallback never reparses a different spelling to obtain a result.
3. **Canonical authority.** Only FormulaPlane canonicalization and dependency analysis
   choose `FormulaTemplateId`. Source family identity is never a template key.
4. **Cached-value identity.** A formula-bearing record never enters the Arrow base-value
   plane. `KeepCachedValue` behavior remains the existing behavior: the cached value is
   observable to ingest diagnostics/provenance if available, but is not loaded as a
   literal or used to bypass parse/evaluation.
5. **Coordinate identity.** Source coordinates are zero-based at the adapter seam and
   converted exactly once to one-based `FormulaIngestRecord` coordinates. Bounds are
   checked before arithmetic.
6. **Mode parity.** Eager and deferred loading produce the same FormulaPlane ingest
   report, graph/span population, formula text, diagnostics, and evaluated cells. The
   only intended difference is when parsing and graph/family promotion occur. Deferred
   provenance therefore lands before either shadow classification or authoritative
   source-family promotion is enabled.
7. **Order independence.** Reordering otherwise identical formula events may change
   diagnostics that explicitly report source sequence, but not final formulas,
   promotion verdict, or values. The Stage 1 oracle remains available for malformed
   cases whose duplicate-coordinate semantics are inherently order-sensitive.
8. **No partial authority.** A family is committed atomically. On any family-level
   rejection, every member not already superseded by the existing duplicate-coordinate
   rule falls back per cell; no member is dropped.
9. **Limits do not weaken.** Existing row, column, logical-cell, and populated-record
   limits are enforced at the same point or earlier. Optimization metadata cannot let a
   workbook evade a load limit.
10. **Backend compatibility.** Umya, JSON, Python bindings, and callers constructing
    `FormulaIngestRecord` continue unchanged while the new API is additive and internal.

## 4. Formula source event seam

### 4.1 Implemented API

The implementation lives in
`crates/formualizer-eval/src/engine/formula_source.rs`. Because the producer lives in the
separate `formualizer-workbook` crate, expose only the minimal transport constructors and
batch types as `#[doc(hidden)] pub`; collector and normalization internals remain
`pub(crate)`. Do not move the contract to `formualizer-common`, which must not acquire an
engine dependency:

```rust
#[doc(hidden)]
pub struct FormulaSourceEvent {
    pub sheet_name: Arc<str>,
    pub coord0: SourceCoord,
    pub source_sequence: u64,
    pub formula: FormulaSourceKind,
    pub cached: SourceCachedValue,
}

#[doc(hidden)]
pub struct SourceCoord { pub row: u32, pub col: u32 }

#[doc(hidden)]
pub enum FormulaSourceKind {
    Ordinary { formula: Arc<str>, metadata: FormulaMetadataEnvelope },
    SharedAnchor {
        family: SourceFamilyId,
        declared_range: Option<SourceRect>,
        formula: Arc<str>,
        metadata: FormulaMetadataEnvelope,
    },
    SharedDescendant {
        family: SourceFamilyId,
        metadata: FormulaMetadataEnvelope,
    },
    Unsupported {
        formula_if_available: Option<Arc<str>>,
        metadata: FormulaMetadataEnvelope,
    },
}

#[doc(hidden)]
pub struct SourceFamilyId {
    pub sheet_instance: u32,
    pub shared_index: usize,
}

#[doc(hidden)]
pub enum SourceCachedValue {
    // Calamine 0.36 collapses missing and empty `<v>` into DataRef::Empty.
    AbsentOrEmpty,
    Present(LiteralValue),
    Unrepresentable,
}

#[doc(hidden)]
pub enum FormulaMetadataEnvelope {
    XlsxShared { shared_index: usize, parsed_range: Option<SourceRect> },
    XlsxOrdinary,
}
```

The checked-in types carry this information content (with `XlsxUnknown` as the unknown
envelope).
The first API is intentionally Calamine-specific: Calamine exposes parsed `Dimensions`
but not raw range spelling, unknown attributes, array/data-table role metadata, or a
missing-versus-empty cached-value bit. The transport must not claim to preserve those
unavailable facts. Generalize backend/dialect identity only when a second producer has a
concrete need.

`source_sequence` is monotonically increasing per worksheet reader and exists only for
provenance, deterministic diagnostics, and reproduction. It is not placement order.
`sheet_instance` prevents the same `si` on two sheets from colliding. It is assigned from
sheet enumeration, not a mutable FormulaPlane `SheetId`.

The adapter emits one event for every formula-bearing cell. Literal-only records remain
on the existing Arrow ingest path. The implementation collects events in a per-sheet vector because eager
`FormulaIngestBatch` already has per-sheet lifetime. An iterator/sink transport remains a
possible future memory optimization.

### 4.2 Lossless means

For every formula-bearing source record, the seam preserves:

- worksheet instance and coordinate;
- source sequence;
- whether it was ordinary, anchor, descendant, or unsupported;
- worksheet-local source family key when supplied;
- anchor formula text exactly as exposed by Calamine (normalization with `=` happens
  only in the existing parse/stage adapter);
- declared shared range independently of observed members;
- cached value as representable by Calamine, even though load policy suppresses it;
- all known metadata exposed by the producer.

Expansion is a derived view. It must not overwrite the source event. Formula parsing,
canonicalization, and interning are also derived views. Tests must be able to inspect an
event and its expanded per-cell oracle record side by side.

### 4.3 Unknown and malformed metadata

`XlsxFormulaMetadata` is `#[non_exhaustive]`. A wildcard match must never silently turn a
formula cell into a blank. The rules are:

- a future/unknown Calamine metadata variant exposes neither generic formula text nor raw
  attributes, so emit `UnknownUnrecoverable` with coordinate and cached value, increment
  `source_event_unknown_unrecoverable`, and return a typed load error before committing
  that sheet;
- malformed XML/attributes rejected by Calamine: preserve the current Calamine load
  error in phases 1-2; later error wrapping may add sheet/sequence context without
  changing the source error category;
- invalid coordinate/range or overflow: reject family promotion. If per-cell text can be
  expanded safely, fall back; otherwise fail load. Never clamp a declared range;
- an empty shared anchor formula is malformed for promotion. Its descendants follow the
  same recoverable/unrecoverable rule as a missing anchor;
- Calamine classifies array and data-table formula tags as `Normal`; source-aware code
  therefore makes no role claim and never directly promotes them based on metadata. Rich
  role support requires a future upstream Calamine API.

This is a fail-closed contract: family optimization is optional, but inventing or losing
a formula is not.

## 5. Calamine XML stream-order contract

Calamine's `XlsxCellReader` returns cell records in physical worksheet XML order. That
usually resembles row-major order but XLSX does not make it a correctness prerequisite,
and malformed or generated files can contain non-monotonic coordinates. Formula ingest
therefore promises only:

- events preserve reader order and receive increasing `source_sequence`;
- `si` is worksheet-local and neither increasing nor unique to one anchor in malformed
  input;
- an anchor may precede or follow any descendant;
- coordinates may repeat, move backward, lie outside `<dimension>`, or lie outside a
  declared shared range;
- family finalization occurs at end of worksheet (or explicit collector flush), never
  because a coordinate passed the declared range end;
- declared range is a hint/evidence claim, not an instruction to synthesize members;
- all family verdicts are computed from a deterministic coordinate-sorted view plus
  explicit duplicate/conflict facts, never from arrival order.

Ordering is useful for the dense Arrow value fast path and for resolving the Stage 1
oracle, but it is never a correctness dependency for family membership or promotion.
Forward anchors are held as unresolved evidence until the anchor appears or the sheet
ends. The collector does not emit a promoted family early.

## 6. Identity model

### 6.1 Source-family identity

A source family answers “which records did the producer claim were related?” Its identity
is `(backend, sheet_instance, dialect, source_key)`. For XLSX shared formulas,
`source_key` is `si`. It is provenance only and has worksheet lifetime.

A duplicate anchor with the same source family ID does not create a new canonical
identity. It marks the source family ambiguous. The collector records every anchor and
source sequence. Direct promotion rejects the entire ambiguous source family. The
per-cell oracle expands each descendant according to the established Stage 1 behavior
for that phase; before changing that behavior, tests pin whether the most recently seen
anchor or the first following anchor applies. No duplicate anchor may silently merge two
templates into one span.

### 6.2 Canonical-template identity

A canonical template answers “are these parsed formulas semantically relocatable under
FormulaPlane's current policy?” It remains the
`parameterized_canonical_hash/key -> FormulaTemplateId` result from
`ingest_pipeline`, `template_canonical.rs`, and `dependency_summary.rs`. Hash equality is
only a grouping accelerator; exact canonical keys and placement analysis remain the
collision check.

One source family may normalize to multiple canonical templates and must then fall back
in the direct-shared phase. Multiple source families may normalize to one canonical
template but remain separate promotion proposals in that phase. Merging them is ordinary
formula inference, deferred to the final phase.

## 7. Declared range, occupancy, and anomalies

`declared_range` and `observed_occupancy` are independent facts.

- **Declared shared range:** the anchor's optional XLSX `ref`, normalized to an inclusive,
  zero-based `SourceRect` only after validating order, bounds, and overflow. It may be
  absent, too large, too small, or wrong.
- **Observed family occupancy:** unique coordinates carrying that family's anchor or
  descendant tag. It never includes coordinates merely covered by `ref`.
- **Observed formula occupancy:** all formula-bearing coordinates on the sheet, including
  ordinary, unsupported, and other-family records. It is used to classify exceptions and
  conflicts; it does not assign them to the family.

An observed family cell absent from the declared range is an out-of-range descendant.
A coordinate in the declared range with no family record is a hole. A coordinate in the
declared range occupied by an ordinary formula or another family is an exception. A
literal/blank coordinate inside the range is a hole, not an implicit formula. Mixed
shared/unshared records therefore remain separate cells even when canonicalization later
finds equivalent text.

Direct shared-family promotion requires all of the following:

1. exactly one non-empty anchor and no duplicate family coordinate;
2. a valid declared range containing the anchor;
3. every observed member lies in the declared range;
4. observed family occupancy equals every coordinate in the declared range;
5. no ordinary/unsupported/other-family formula conflicts at those coordinates;
6. every descendant expands successfully against the sole anchor;
7. expanded formulas pass the existing parse policy, canonical-equivalence,
   dependency-summary, shape, minimum-size, memory, and FormulaPlane mode gates;
8. the declared rectangle maps exactly to one currently supported `RowRun`, `ColRun`, or
   `Rect` (a one-cell range remains per-cell; current minimum-domain rules still apply).

Missing range, missing anchor, duplicate anchor/member, holes, exceptions, mixed records,
out-of-range descendants, mismatched canonical templates, and malformed/unknown metadata
all reject direct promotion. Rejection is not a load error when exact per-cell formulas
are available: every recoverable member goes through existing fallback. An unresolved
shared descendant has no formula text and is therefore an unrecoverable load error unless
the source API supplies an independent expanded text. This intentionally replaces silent
dropping only in the gated phase that has a corpus and explicit compatibility approval.

## 8. Placement evidence and promotion boundary

### 8.1 Lossless evidence

`crates/formualizer-eval/src/engine/formula_family.rs` implements the collector-facing
evidence model. Its compact private representation is equivalent to:

```rust
pub(crate) struct FormulaFamilyEvidence {
    pub source_id: SourceFamilyId,
    pub anchors: Vec<AnchorEvidence>,
    pub declared_ranges: Vec<DeclaredRangeEvidence>,
    pub members: Vec<MemberEvidence>,
    pub conflicts: Vec<OccupancyConflict>,
    pub truncation: Option<EvidenceLimitReason>,
}

pub(crate) struct MemberEvidence {
    pub coord0: SourceCoord,
    pub source_sequence: u64,
    pub role: SourceMemberRole,
    pub expanded_formula: ExpansionOutcome,
}

pub(crate) enum ExpansionOutcome {
    Exact(Arc<str>),
    Pending,
    Failed(Arc<str>),
    Unavailable,
}

pub(crate) enum EvidenceCoordSet {
    SortedPoints(Arc<[SourceCoord]>),

}
```

The implemented collector stores members exactly as points plus explicit duplicate
records. Sorting/deduplication produces a view and never deletes the raw conflict fact.
Do not represent the family solely as its declared rectangle. Do not reuse
`FormulaRunStore`: that store is descriptive canonical-template output, whereas this is
source provenance and may contain malformed evidence.

### 8.2 Normalization and promotion

The boundary is a pure function:

```rust
fn normalize_family_evidence(
    evidence: &FormulaFamilyEvidence,
    sheet_occupancy: &FormulaOccupancyIndex,
    limits: &FormulaFamilyLimits,
) -> FamilyNormalization;

fn prepare_family_placement(
    normalized: &NormalizedFamily,
    analyses: &[CandidateAnalysis],
) -> Result<PreparedFamilyPlacement, FamilyFallbackReason>;
```

`FamilyNormalization` contains the immutable evidence, sorted unique occupancy,
holes/exceptions/out-of-range facts, exact per-cell oracle records, and a source-evidence
verdict. It performs no graph writes and does not choose a FormulaPlane domain. The shared
pure placement-preparation function remains the sole authority that derives and validates
today's `PlacementDomain::{RowRun, ColRun, Rect}` from candidate analyses. Future richer
occupancy/domain representations can be added behind this boundary without changing
source events or pretending old domains can express them.

Commit occurs later, inside centralized formula ingest: analyze every per-cell formula
with the existing ingest pipeline, compare exact canonical keys, then prepare placement
without mutating FormulaPlane. If any check or preparation fails, discard the proposal
and use its precomputed per-cell oracle records. A successful plan is consumed by one
infallible commit, making promotion transactional at family granularity.

The current placement path may still split canonical ordinary-formula components. The
direct source path must not broaden a declared family, bridge holes, consume an exception,
or merge adjacent source families.

## 9. Parsing, cached values, and load limits

### 9.1 Parsing

Anchor/ordinary text is normalized with a leading `=` exactly where Stage 1 does it.
Shared descendants are expanded using `calamine::expand_shared_formula_into` with the
anchor and target zero-based coordinates; the resulting exact text is then normalized
and parsed under the existing `FormulaParsePolicy`. The per-sheet text parse cache remains
an optimization only. Parse-cache hits must produce the same `AstNodeId`/diagnostic
behavior as the current path.

The source event and expanded text survive until the family verdict. After fallback or
successful commit, they may be released. Formula text retained by
`FormulaIngestRecord`/FormulaPlane follows existing policy. A parse recovery that yields
no AST still counts as an observed formula and family fallback; it must not turn the rest
of a malformed family into an authoritative partial span.

### 9.2 Cached values

`SourceCachedValue` is captured before policy to the fidelity Calamine exposes;
`DataRef::Empty` is recorded as `AbsentOrEmpty`. Formula records never call
`data_ref_to_literal`/`data_ref_to_overlay`, preserving Stage 1. Neither shadow nor
authoritative promotion reads cached values as formula results. Array/data-table cached
results are not reconstructed. Diagnostics must not print cached text/value payloads by
default because workbooks may contain sensitive data.

### 9.3 Bounds and pathological cases

Existing `WorkbookLoadLimits` remains authoritative:

- dimensions are checked initially and whenever an observed coordinate extends them;
- observed populated budget remains `value_cells_observed + formulas_observed`, counted
  once per source cell record under existing semantics;
- declared shared area does not count as observed cells and is never materialized merely
  to find holes;
- a declared range outside row/column limits rejects promotion and follows recoverable
  fallback; an actual out-of-bounds observed coordinate keeps the existing load error;
- pending descendants, source events, and family points are bounded by observed formula
  records, not declared range area.

Below the evidence cap, collection and indexing use O((N+F) log N) time and O(N+F)
auxiliary memory for N observed source records and F family records, with no declared-area
scan. Base source events and formula text remain bounded by the workbook load budget rather
than the promotion cap. Add internal `FormulaFamilyLimits` derived from existing limits
to bound promotion-only evidence; do not add public configuration in the first slice:

- maximum retained family evidence points per sheet: no greater than
  `max_sheet_logical_cells` and `usize::MAX`;
- maximum retained formula/evidence bytes per family and sheet, using checked/saturating
  accounting; initial recommended caps are 8 MiB per family (matching placement binding
  memory scale) and 64 MiB per sheet;
- maximum conflict diagnostics retained per reason (recommended 64), while counters
  remain exact;
- no scan proportional to declared rectangle area. Equality with a rectangle is proven
  by checked area, unique count, min/max bounds, and row/column run validation over
  observed sorted points.

Before collector-wide occupancy or range indexes are allocated, preflight their estimated
point and byte cost. If a cap would be exceeded, disable family collection for the sheet,
increment the evidence-limit counter, and continue the Stage 1
per-cell expansion/staging stream. This is an optimization fallback, not a workbook load
failure. Exact per-cell records remain subject to the pre-existing load budget. A sheet with one million one-cell families, alternating coordinates, duplicate anchors,
or a full-sheet declared range with two observed records therefore uses O(observed
formulas plus formula bytes) base-ingest memory and bounded promotion metadata; it never
allocates O(declared area). A strict process-memory guarantee requires a future total
formula-byte limit or spooling and is not claimed here.

Forward-anchor pending storage uses coordinates/events only and shares the same evidence
budget. Once capped, promotion is disabled but Stage 1's exact fallback requirement
still needs expansion. Because descendants without an anchor cannot be expanded until
flush, their minimal records must remain bounded by the existing observed formula limit;
if the public load budget allows more than process-safe storage, a later spool-to-disk
strategy is permitted. It must preserve sequence and must not change semantics.

## 10. Eager and deferred behavior

Eager mode builds events and exact per-cell oracle records during sheet streaming, then
passes a source-aware batch to centralized ingest after the Arrow sheet is installed.
Source metadata conversion and expansion validation must finish before formula parsing,
AST interning, deferred staging, Arrow installation, or graph ingest begins. The current
engine APIs do not provide a transaction that can roll back parse diagnostics, interned
ASTs, or staged formulas if a later parse or sheet-install operation fails, so this phase
does not claim such rollback. The implementable boundary is fail-closed metadata
validation before those mutations; workbook-wide rollback and rollback of subsequent
parse/AST/staging mutations remain outside this milestone.

Deferred mode must retain source events/evidence, not only expanded text, otherwise
source-family identity is lost before `build_graph_all`. Extend `StagedSheet` additively:

```rust
struct StagedFormulaEntry {
    row: u32,
    col: u32,
    text: String,
    provenance: Option<Arc<FormulaSourceProvenance>>,
}
```

or store a parallel insertion-ordered `StagedFormulaSourceMap` keyed by coordinate. The
chosen representation must preserve current O(1) stage/get, insertion order,
duplicate-coordinate replacement, rename/remove, undo/redo, and formula-text APIs.
Generic interactive `stage_formula_text` creates ordinary/no-source provenance.
`build_graph_all` and `build_graph_for_sheets` drain provenance with text and call the
same source-aware ingest API as eager mode. Selected-sheet building must not consume
other sheets' source evidence. Rename/remove and changelog replay move/remove evidence
atomically with staged text.

Deferred staging now retains source events with expanded text and feeds the same
source-aware ingest path as eager mode. Eager and deferred family promotion therefore use
the same collector and placement boundary.

## 11. Observability

Extend `FormulaIngestReport` additively (or add a nested, defaulted internal
`FormulaSourceIngestReport`) with saturating counters:

- `source_formula_events`, `source_ordinary_events`, `source_shared_anchor_events`,
  `source_shared_descendant_events`, `source_unknown_events`;
- `source_families_seen`, `source_families_well_formed`,
  `source_family_cells_seen`;
- `source_forward_descendants`, `source_duplicate_anchors`,
  `source_duplicate_members`, `source_missing_anchors`, `source_missing_ranges`,
  `source_holes`, `source_exceptions`, `source_out_of_range_descendants`,
  `source_malformed_metadata`;
- `source_family_shadow_eligible`, `source_family_promoted`,
  `source_family_promoted_cells`, `source_family_fallback_cells`;
- `source_evidence_limit_fallbacks`, `source_evidence_peak_bytes` (an allocation-size
  estimate, not allocator-observed resident bytes), `source_pending_peak_cells`;
- exact fallback reason keys: `MissingAnchor`, `DuplicateAnchor`, `MissingDeclaredRange`,
  `InvalidDeclaredRange`, `Hole`, `Exception`, `MixedSourceRecords`,
  `OutOfRangeDescendant`, `ExpansionFailed`, `UnknownMetadata`, `ParseRejected`,
  `CanonicalMismatch`, `UnsupportedCurrentDomain`, `ExistingPlacementGate`, and
  `EvidenceLimit`.

Counters are mode-independent observations where possible. In Off mode promotion
counters are zero but event/anomaly counters may be populated. `AdapterLoadStats` keeps
its existing fields; `shared_formula_tags_observed` must equal anchors plus descendants,
including families that fall back. Debug logs may print counts and coordinates capped to
a small sample, never all formula/cached text. Tracing spans include backend, sheet,
event/family counts, fallback reason, and bytes, with no formula payload.

## 12. Differential oracle

The oracle is a pure Stage 1 expander extracted from current Calamine logic:

```rust
fn expand_source_events_per_cell(
    events: &[FormulaSourceEvent],
) -> Result<Vec<ExpandedFormulaCell>, SourceFormulaError>;
```

It uses `expand_shared_formula_into`, current duplicate-coordinate staging semantics,
normalization, and parse policy, but no family promotion. Every phase runs two paths from
the same events:

- oracle: expand/materialize all cells per current behavior;
- candidate: collect/normalize/promote with fallback.

Compare sorted `(sheet, row, col, normalized formula text)`, parse diagnostics, evaluated
values/errors, graph formula text reads, dimensions, load stats, and formula ingest
counts. In authoritative mode also assert promoted cells plus fallback materialized cells
equals oracle formula cells after duplicate-coordinate resolution. Run the oracle against
`FormulaPlaneMode::Off`; run candidate in Off, Shadow, and AuthoritativeExperimental.
For malformed cases where Stage 1 silently drops an unresolved descendant, initially
record the discrepancy as a corpus expectation; changing it to a typed error requires the
phase-4 gate and a release note.

Property tests generate event permutations, mixed absolute/relative references,
worksheet-local repeated `si`, duplicate coordinates, ranges, holes, and conflicts.
Permutation invariance applies to well-formed unique-coordinate families. Malformed
order-sensitive cases compare to the explicitly versioned Stage 1 oracle.

## 13. Serial gated phases

Phases 1-5 below are implemented. Their gates remain as the regression contract for Off,
Shadow, AuthoritativeExperimental, eager, and deferred configurations.

### Phase 1: corpus and contract (implemented)

**Likely files/APIs**

- This document.
- `crates/formualizer-workbook/tests/calamine/shared_formulas.rs`: extend the existing ZIP
  XML rewrite fixture helper.
- New `crates/formualizer-workbook/tests/calamine/formula_source_corpus.rs` or fixture
  files under `crates/formualizer-workbook/tests/fixtures/formula-source/`.
- Optional test-only Stage 1 oracle in
  `crates/formualizer-workbook/tests/calamine/formula_source_oracle.rs`.

**Tests first**

Pin ordinary, vertical/horizontal/rectangular shared ranges; mixed/absolute references;
forward anchor; lower `si` later in XML; absent range; missing/empty/duplicate anchors;
duplicate members; holes; ordinary and other-family exceptions; mixed shared/unshared
records; descendants outside range; understated dimensions; non-monotonic and duplicate
coordinates; malformed `si`/`ref`; array and data-table tags as Calamine `Normal`; and
file and bytes adapters. Synthetic future-variant tests begin in Phase 2, once the event
contract exists. Assert cached formula values do not
enter Arrow.

**Acceptance gate**

The current Stage 1 implementation passes all existing tests. New corpus tests either pin
current behavior or are `#[ignore]` with a named future phase and an explicit expected
result; there are no vague TODO assertions. The corpus documents which malformed inputs
Calamine rejects before FormulaPlane sees them.

**Differential strategy**

Snapshot exact expanded formula coordinates/text from current
`expand_shared_formula_into` and evaluated Off-mode values. Keep fixture generation
deterministic and inspect generated sheet XML in each test.

**Benchmarks**

Add cases to `crates/formualizer-bench-core/src/bin/probe-load-envelope.rs` or a new
`probe-formula-source-ingest`: ordinary formulas, one large shared family, many tiny
families, forward-anchor family, and pathological irregular metadata. Record wall time,
peak RSS where available, formulas/sec, and load stats; no performance gate yet.

**Rollback/fallback**

Tests/docs only; no runtime change. Remove only the new test fixtures if Calamine cannot
represent a case, retaining a synthetic contract test.

**Non-goals**

No event API, no counters, no behavior change, no promotion.

### Phase 2: event seam preserving current behavior (implemented)

**Likely files/APIs**

- New `crates/formualizer-eval/src/engine/formula_source.rs` for event/provenance types.
- `crates/formualizer-eval/src/engine/mod.rs` for internal exports.
- `crates/formualizer-workbook/src/backends/calamine.rs`: map metadata to events and run
  the extracted per-cell expander.
- `crates/formualizer-eval/src/engine/formula_ingest.rs`: optional provenance field on an
  additive `FormulaSourceIngestBatch`; do not break `FormulaIngestRecord::new`.
- Unit tests in `crates/formualizer-workbook/src/backends/calamine.rs` and corpus tests.

**Tests first**

Event snapshots assert every field, sequence, declared range, cached-value presence, and
forward-anchor provenance. Differential tests assert byte-for-byte normalized formula
text and all Stage 1 outputs for existing supported inputs. Add a synthetic unknown
variant test so wildcard behavior cannot be forgotten.

**Acceptance gate**

Off/Shadow/Authoritative outputs, existing `AdapterLoadStats`, eager/deferred behavior,
load-limit errors, and parse diagnostics match Stage 1 on the corpus. No family collector
or promotion reads provenance. Benchmark regression limits: shared-family load time no
more than 10% or noise-adjusted 2 ms (whichever is larger), and peak RSS no more than one
owned event record per formula beyond the already-owned per-cell record.

**Differential strategy**

Run old extracted expander and event expander from the same synthetic records. For real
XLSX, compare against the Phase 1 snapshots and `worksheet_formula` only as a test oracle,
never as production fallback.

**Rollback/fallback**

There is no old inline-expander production gate. The merge state has one production
expander and oracle-only test coverage.
Any event conversion uncertainty for a known variant routes to existing per-cell
behavior; a synthetic future/unknown variant is pinned as a typed error, because Calamine
provides no generic text with which exact fallback could be reconstructed.

**Non-goals**

No FormulaPlane grouping change, no new authority, no family collector. Phase 2 may add
the compact provenance payload to source-aware batches but does not yet persist it through
deferred staging.

### Phase 3: deferred staging integration (implemented)

**Likely files/APIs**

- `crates/formualizer-eval/src/engine/eval.rs`: `StagedFormulaEntry` with provenance or
  parallel `StagedFormulaSourceMap`; update stage/remove/get/rename/build/undo/redo paths.
- `crates/formualizer-eval/src/engine/formula_source.rs` and `formula_ingest.rs` for an
  owned compact provenance record.
- `crates/formualizer-workbook/src/backends/calamine.rs` to stage source-aware formulas.
- Existing deferred tests in
  `crates/formualizer-eval/src/engine/tests/formula_plane_ingest_shadow.rs` and new
  Calamine eager/deferred differential tests.

**Tests first**

Eager/deferred event/provenance and existing authority parity; build-all/build-selected;
cross-sheet refs;
rename/remove; duplicate-coordinate replacement; staged formula text read; interactive
replacement clears source provenance; undo/redo; parse errors; load and evidence caps.

**Acceptance gate**

Deferred and eager runs have identical formula/event counters, retained provenance, and
final existing authority. Family counters begin in Phase 4.
Staged operations remain O(1) for stage/get. No provenance remains after drain/removal.
No selected-sheet build consumes another sheet's evidence.

**Differential strategy**

Load each corpus fixture eagerly and deferred, force fallback on/off, then compare reports,
baseline stats, formulas, values, and post-edit recalculation.

**Benchmarks**

Repeat the existing 50k deferred staging case and 1M shared-family case. Stage/get remains
linear total/O(1) each; compact provenance overhead is measured and remains O(observed
formula records plus formula bytes).

**Rollback/fallback**

If provenance is invalidated by an interactive edit, clear that cell's source provenance
and preserve expanded staged text. There is no promotion yet, so rollback is simply the
existing source-unaware deferred path.

**Non-goals**

No changelog format guarantee for source provenance after graph build; no public exposure.

### Phase 4: shadow family collector (implemented)

**Likely files/APIs**

- New `crates/formualizer-eval/src/engine/formula_family.rs` for collector, evidence,
  normalization, limits, and reasons.
- `crates/formualizer-eval/src/engine/formula_ingest.rs` for report counters and a
  source-aware batch accepted alongside current batches.
- `crates/formualizer-eval/src/engine/eval.rs` to invoke collector in Shadow only and feed
  expanded per-cell records unchanged to existing ingest.
- Tests in new
  `crates/formualizer-eval/src/engine/tests/formula_family_ingest_shadow.rs`, plus
  Calamine corpus integration tests.

**Tests first**

Unit-test every anomaly, declared/observed distinction, order permutation, checked area,
no declared-area scan, deterministic normalization, evidence caps, exact counters, and
collision-safe canonical comparison. Integration tests assert graph/span counts and values are unchanged from Phase 3.

**Acceptance gate**

Shadow collector has zero authority writes. For every corpus family,
`eligible + fallback` reconciles exactly with observed family cells; counter totals are
stable under valid event permutations. Pathological declared full-sheet/two-point input performs work proportional to the two
observed records plus index overhead, never to the declared sheet area, and evidence
memory respects caps. Off-mode overhead is an empty branch.

**Differential strategy**

Compare collector oracle records to Phase 3 expansion and compare its eligibility
prediction to a scratch `place_candidate_family_with_analyses` result, without committing.
Investigate every disagreement; default verdict is fallback.

**Benchmarks**

Gate Off mode at noise; Shadow shared-family throughput regression <=10%; irregular family collection follows
the implemented O((N+F) log N) bound in observed records; report peak evidence bytes.

**Rollback/fallback**

Disable collector globally on any cap/internal error, increment `EvidenceLimit` or
`CollectorInternal`, and continue exact per-cell ingest. A sheet-level kill switch is
sufficient; never abort a valid workbook solely because shadow analysis failed.

**Non-goals**

No authoritative promotion based on source metadata and no richer spans.

### Phase 5: conservative direct shared-family promotion (implemented)

**Likely files/APIs**

- `crates/formualizer-eval/src/engine/eval.rs`: source-family-first candidate grouping in
  `analyze_formula_plane_authoritative_ingest`. Source grouping bypasses the current
  ordinary `(sheet, canonical_hash, candidate.col)` bucket, which effectively limits
  ordinary discovery to vertical families; it does not change ordinary inference.
- `crates/formualizer-eval/src/formula_plane/placement.rs`: split current placement into a
  pure `prepare_family_placement` and an infallible `commit_prepared_family`. Source
  metadata selects only the exact candidate set; the shared preparation function still
  derives domain, origin, exact canonical equality, bindings, dependency summaries,
  minimum size, memory caps, cycle/internal-dependency gates, and fallback reason.
- `crates/formualizer-eval/src/formula_plane/runtime.rs`: no domain changes.
- `crates/formualizer-eval/src/engine/formula_ingest.rs`: promoted/fallback counters.
- Calamine authoritative integration tests and
  `formula_family_ingest_authoritative.rs`.

**Tests first**

Promote only complete one-anchor declared families that exactly form RowRun/ColRun/Rect.
For every anomaly, assert zero source-direct promoted cells and exact per-cell fallback.
Assert canonical mismatch, dependency rejection, small domains, binding cap, cycles, and
mode Off all retain existing placement behavior. Test hash collision by injected keys.
Test edits/evaluation parity after promotion using existing FormulaPlane acceptance tests.

**Acceptance gate**

Cell-by-cell values/errors and formula text match Off oracle before and after edits.
`promoted_cells + fallback_cells` reconciles with oracle cells. No malformed family has
partial authority. Current FormulaPlane thresholds and fallback reasons remain intact.
Array/data-table records do not direct-promote. The unrecoverable unknown/missing-anchor
policy is approved explicitly before replacing Stage 1 silent omission with typed error.

**Differential strategy**

A test-only force-fallback switch runs the same workbook twice. Compare Off/per-cell,
authoritative forced fallback, and authoritative direct promotion. Include the full
FormulaPlane coverage corpus, not only shared-formula fixtures.

**Benchmarks**

Measure 100, 10k, 100k, and 1M member shared families. Gate no regression for forced
fallback; direct promotion should reduce canonical grouping/placement work or memory and
must not exceed per-cell peak memory. Many tiny/malformed families remain within the O((N+F) log N) collector bound.

**Rollback/fallback**

There is no production direct-promotion toggle. A crate-private test-only forced-fallback
hook supports the differential oracle. Any family normalization, analysis, or pure
placement-preparation rejection uses precomputed exact per-cell records. No FormulaPlane mutation occurs before preparation
succeeds; commit consumes one complete plan and cannot partially promote a family. After a span is committed, failures use existing FormulaPlane demotion rules;
there is no loader-specific recovery path.

**Non-goals**

No holes/exceptions in spans, no merging source families, no ordinary inference changes.

### Deferred future work: ordinary-formula inference and richer spans

**Likely files/APIs**

- `crates/formualizer-eval/src/engine/formula_family.rs`: ordinary inferred family IDs
  distinct from source IDs.
- `crates/formualizer-eval/src/formula_plane/span_store.rs`: reuse descriptive
  holes/exceptions only through an explicit conversion.
- `crates/formualizer-eval/src/formula_plane/runtime.rs`, `placement.rs`, scheduler,
  structural and dependency-summary modules for new domain kinds.
- `docs/architecture/adaptive-formula-partition.md` for interval-set lifecycle alignment.

**Tests first**

Inference must prove canonical equivalence without source claims. Rich domain tests cover
holes, exceptions, multiple runs/rectangles, structural edits, dirtiness, cycles, and
persistence. Differential tests remain cell-by-cell against per-cell Off mode.

**Acceptance gate**

A separate reviewed architecture amendment defines authority, scheduling, dirty
projection, structural behavior, and memory for every richer domain. This document alone
does not authorize those changes.

**Benchmarks**

Sparse ordinary formulas pay no family machinery floor; irregular occupancy remains
proportional to observed records; richer spans demonstrate a measured win over multiple
current domains.

**Rollback/fallback**

Normalization can always project only the supported contiguous subsets or reject the
whole proposal to exact per-cell behavior. Never approximate holes with a rectangle.

**Non-goals**

This milestone does not schedule or implement ordinary inference or richer span domains;
they require a separate reviewed architecture amendment.

## 14. Implemented slice and deferred boundaries

The checked-in production slice includes:

1. Owned source events, declared ranges, roles, sequence, cached-value fidelity, and
   source-aware eager/deferred batches.
2. A pure Calamine event-to-per-cell expander used as the exact fallback record source.
3. A bounded family collector with anomaly/fallback counters and no declared-area scan.
4. Conservative direct promotion in `AuthoritativeExperimental` through the existing
   FormulaPlane placement gates; Off mode retains per-cell graph materialization.
5. Public ingest reports for source/family/promoted/fallback counts, while collection
   limits and the test-only forced-fallback oracle remain non-public.

The implementation does not add a `PlacementDomain`, does not make source identity a
canonical template identity, and does not expose a production switch that bypasses
family validation.

### Explicitly deferred

- Richer spans with holes, exceptions, multiple runs, or sparse occupancy are deferred to
  a separate architecture amendment.
- Richer ordinary-formula family inference is deferred. The pre-existing ordinary
  canonical grouping remains unchanged and is not evidence for source-direct promotion.
- Strict byte-bounded ingest is deferred. Current 8 MiB per-family and 64 MiB per-sheet
  estimates bound optional promotion evidence only; source events, exact expanded
  formula text, pending forward descendants, and legacy graph materialization remain
  bounded by workbook cell limits rather than a strict total byte budget. A strict
  guarantee requires a total formula-byte limit or spooling.

## 15. Required validation commands

Run from the repository root, adapting feature syntax only if Cargo reports a package
feature mismatch:

```text
cargo test -p formualizer-workbook --features calamine calamine
cargo test -p formualizer-workbook --features calamine --test load_limits
cargo test -p formualizer-eval formula_plane_ingest_shadow
cargo test -p formualizer-eval formula_plane
cargo test -p formualizer-workbook --features calamine
cargo fmt --all -- --check
cargo clippy -p formualizer-workbook -p formualizer-eval --all-targets --features formualizer-workbook/calamine -- -D warnings
```

For phases with benchmarks, record command, commit, mode, fixture size, elapsed time,
peak RSS if available, event/family counters, graph vertices, active spans, and fallback
reasons. A benchmark result without its forced-fallback oracle is not an acceptance
result.

## 16. Review checklist

- Does every formula-bearing record become an event or a typed load error?
- Can source sequence be permuted without changing valid-family results?
- Are source-family and canonical-template IDs visibly different types?
- Is declared range retained separately from observed occupancy?
- Are holes, exceptions, duplicates, forward/missing anchors, mixed records, and
  out-of-range descendants represented rather than inferred away?
- Does every rejected family retain exact per-cell expanded formula text?
- Is promotion one pure normalization/proposal followed by atomic commit?
- Is all work O(observed records), with no declared-area scan?
- Do evidence caps disable only optimization?
- Are cached formula values still excluded from Arrow?
- Do eager/deferred and Off/Shadow/Authoritative modes reconcile?
- Do unknown metadata variants fail closed, while arrays/data tables receive no source-role claim that Calamine cannot support?
- Did the implemented slice avoid changing `PlacementDomain` and current FormulaPlane
  correctness gates?
