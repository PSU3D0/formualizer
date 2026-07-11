# Source Formula Family Ingest

Status: Implemented

## Purpose

Formualizer can ingest a source-declared formula family without materializing every
member as a string, AST, analysis, or graph vertex. The capability is backend-neutral;
the first producer is the Calamine XLSX adapter.

The optimization does not change formula semantics. A family receives direct
FormulaPlane authority only when its source evidence is complete and every existing
engine placement gate accepts it. Any uncertainty uses backend-owned exact replay.

This design does not infer families from ordinary formulas, add sparse FormulaPlane
domains, reconstruct array or data-table formulas, or broaden supported formula syntax.

## Ownership boundary

The workbook backend owns:

- source syntax and metadata interpretation;
- observed occupancy and proof that a family is complete;
- source-family identity;
- exact formula expansion and replay;
- replay spool storage, limits, and cleanup.

The evaluation engine owns:

- formula parsing and AST interning;
- canonicalization and dependency analysis;
- syntax, coordinate, load-limit, shape, size, and memory gates;
- FormulaPlane preparation, authority commit, and later demotion;
- ingest telemetry publication.

A declared source range is evidence, not occupancy. Neither backend nor engine creates a
formula merely because a coordinate lies inside that range.

## Generic contract

The hidden cross-crate contract lives in
`crates/formualizer-eval/src/engine/formula_source.rs`:

```rust
struct SourceFormulaFamily {
    source_id: SourceFamilyId,
    anchor_coord0: SourceCoord,
    anchor_text: Arc<str>,
    members: SourceFamilyMembers,
    member_count: u64,
}

enum SourceFamilyMembers {
    CompleteDomain(PlacementDomainTransport),
    ExplicitMembers(ExplicitSourceFamilyMembers),
}

enum PlacementDomainTransport {
    RowRun { row_start: u32, row_end: u32, col: u32 },
    ColRun { row: u32, col_start: u32, col_end: u32 },
    Rect(SourceRect),
}
```

Coordinates are inclusive and zero-based at this boundary. `SourceFamilyId` is opaque to
the engine; it exists for dispositions, deferred invalidation, and replay skip sets. It
is not a canonical template key.

A `CompleteDomain` is a backend assertion that every coordinate in one existing
`RowRun`, `ColRun`, or `Rect` is an observed member. Validation requires:

- anchor and domain coordinates within `WorkbookLoadLimits`;
- anchor at the domain origin;
- `member_count` equal to the checked domain area;
- domain area within the logical-cell limit.

Only a validated complete domain can use anchor-once preparation.

`ExplicitSourceFamilyMembers` is structurally capped at 4,096 coordinates and is also
checked against workbook limits, duplicates, count mismatch, and a missing anchor. It is
bounded fallback evidence, not a persistent sparse FormulaPlane domain. The current
production source does not promote explicit-member families.

Ordinary per-cell sources continue to use `FormulaIngestBatch`. Backend interaction with
family preparation and replay is grouped behind `SourceFormulaIngress`; adapters can
inspect dispositions but cannot commit FormulaPlane authority themselves.

## Calamine producer

Calamine-specific interpretation remains in
`crates/formualizer-workbook/src/backends/calamine.rs` and its private
`compressed_evidence` and `formula_replay` modules. No XLSX metadata interpretation lives
in `formualizer-eval`.

The adapter consumes each worksheet cell record once. For every formula record it:

1. suppresses the cached formula value from Arrow literal storage;
2. appends a compact, versioned record to the sheet-local replay spool;
3. updates source counters and bounded monotonic family evidence;
4. retains one owned anchor string only when needed by evidence.

The evidence collector proves clean vertical, horizontal, and row-major rectangular
families with constant run state. It rejects direct eligibility on coordinate disorder,
forward anchors, duplicate coordinates or anchors, holes, conflicts, invalid or missing
ranges, range-start mismatch, unsupported records, or evidence-cap exhaustion. It never
scans work proportional to a declared range's area.

Evidence accounting is bounded. Once its cap is reached, optional classification stops
and unresolved source formulas remain recoverable from the spool. A cap disables the
optimization; it does not lose formulas or weaken workbook load limits.

## Exact replay

`formula_replay.rs` is the source-syntax authority for fallback. Its spool uses a
versioned, length-delimited checked-varint encoding. Memory-only use is bounded; native
builds may spill to an owner-only temporary file under explicit sheet, workbook, memory,
and file-count limits. Cleanup occurs on success, error, and unwind.

Replay preserves source sequence and current duplicate/anchor behavior. Ordinary text is
emitted directly. Shared descendants are expanded with Calamine's
`expand_shared_formula_into`, using the applicable source anchor and one reusable string
buffer. Forward descendants remain pending until an anchor is observed. Declared ranges
never synthesize missing members or clip observed out-of-range members.

Replay can skip only families that the same engine has prepared for direct authority.
Every ordinary formula, source-rejected family, and engine-rejected family follows the
exact per-cell path. Unsupported metadata or inability to retain exact replay state is a
typed load failure rather than silent formula loss.

A Calamine-local event expander remains under `cfg(test)` only. Tests compare it directly
with production spool replay; it is not a production transport or second fallback
implementation. Codec boundary, corruption, spill, and replay tests all exercise the
production encoder.

## Engine preparation and authority

For each complete candidate, the engine:

1. validates the generic contract and workbook limits;
2. normalizes and parses the anchor once;
3. validates the conservative anchor-relocation syntax allowlist across the domain;
4. interns and ingests one anchor AST;
5. creates one `CandidateAnalysis`;
6. runs the shared FormulaPlane domain, dependency, internal-dependency, size, binding,
   and memory gates;
7. returns either an opaque prepared placement or a replay reason.

Preparation does not commit authority. `FormulaCompressedPreparation` carries a private,
engine-unique token. Finalization verifies every token and sheet before using prepared
AST or sheet identifiers, so a preparation cannot be transferred to another `Engine`.

On eager authoritative ingest, all exact fallback graph work completes before direct
spans are installed. A successful family retains one anchor AST and analysis, a broadcast
binding when applicable, and no descendant graph vertices. FormulaPlane remains the sole
authority for formula lookup, dependency scheduling, edits, structural operations, cycle
demotion, and span teardown.

## Modes

- **Off:** replay every formula into the ordinary per-cell graph path. Family authority is
  disabled; source and spool telemetry remain meaningful.
- **Shadow:** prepare complete candidates without committing them, then replay every
  formula. Shadow reports accepted/fallback dispositions and avoided-work estimates.
- **AuthoritativeExperimental:** replay ordinary and rejected formulas, then commit
  complete prepared families atomically.

The safe-syntax allowlist is intentionally narrower than Calamine expansion. Unsupported
functions, names, reference forms, or possible coordinate overflow replay exactly; this
feature does not widen formula support.

## Eager and deferred lifecycle

Eager Calamine loading owns the spool until engine dispositions are known. It replays
only when required, sends exact fallback batches through centralized ingest, and then
asks the engine to finalize its own preparations.

Deferred loading moves a sealed `DeferredFormulaPackage` into staged engine state. The
package owns its replay object, generic families, report, invalidated-family set, and
suppressed edited coordinates. Rename moves the package; sheet removal drops it;
interactive replacement invalidates the affected family and suppresses the replaced
source coordinate.

`build_graph_all` and `build_graph_for_sheets` share one processor while retaining their
pre-consolidation orchestration semantics. All-sheet builds keep map iteration order and
clear the parse cache between sheets. A single multi-sheet selected build preserves the
caller's sheet order and shares one parse cache across those sheets; only requested
packages are removed. This is compatibility behavior, not a stronger ordering guarantee:
for repeated malformed text, a selected-build cache hit can suppress a later duplicate
diagnostic exactly as before.

Deferred preprocessing is transactional for package lock, replay, and formula-parse
failures:

- no FormulaPlane or graph formula authority is committed;
- no ingest report or replay count is published;
- diagnostics produced by the failed attempt are discarded;
- staged packages and edits are restored for retry;
- a successful retry publishes one replay.

A poisoned package remains staged but cannot be retried safely without replacement.
Once authority commit starts, packages are consumed exactly once; commit-stage failures
are intentionally outside the retryable preprocessing boundary.

## Invariants

1. Formula-bearing cached values never become Arrow literals.
2. Actual source records, not declared area, determine occupancy and load accounting.
3. Source identity selects a candidate set but never canonical formula identity.
4. A family has either one complete span or exact per-cell fallback, never partial
   authority.
5. Fallback uses backend expansion, source order, formula spelling, and parse policy.
6. Promotion uses only existing `RowRun`, `ColRun`, and `Rect` domains.
7. Evidence and explicit-member limits disable optimization rather than correctness.
8. Eager, deferred, all-sheet, and selected-sheet paths use the same engine preparation
   and authority gates.
9. Cross-engine preparations are rejected before prepared identifiers are used.
10. Unsupported or uncertain syntax replays; it is never accepted speculatively.

## Telemetry

`FormulaCompressedSourceReport` carries backend observations and spool/evidence counters.
`FormulaIngestReport` publishes formula records, spool bytes and replays, clean and replay
families, fallback reasons, anchor parses/ASTs/analyses, avoided descendant work, promoted
cells, materialized graph cells, and Shadow estimates. Counters use saturating arithmetic.

Telemetry describes work that occurred: contract rejection before parsing does not count
an anchor parse, and a failed deferred preprocessing attempt publishes no replay or ingest
report. Formula text, cached payloads, and temporary spool paths are not logged.

## Historical note

An earlier implementation transported evaluator-owned `FormulaSourceEvent`, XLSX
metadata envelopes, cached-value variants, and `FormulaSourceIngestBatch`, then rebuilt
families in `formualizer-eval/src/engine/formula_family.rs`. That duplicate metadata and
occupancy pipeline was removed. Calamine now owns source interpretation, bounded evidence,
and exact replay, while the evaluator receives only generic complete-family candidates
and ordinary fallback batches.
