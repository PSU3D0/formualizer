# Work Roadmap

This roadmap sequences the major engineering tickets needed to ship two credible releases:
1) An "agent-first" release centered on SheetPort + determinism + boring ingestion.
2) A "spreadsheet parity" release that meaningfully challenges IronCalc-class engines for headless workloads.

Tickets live in `tickets/` and are written to support parallel work in separate worktrees.

Principles
- Correctness over function count.
- Determinism + provenance are product requirements for agent use.
- Prefer end-to-end slices: parse -> resolve -> eval -> binding -> tests.
- Minimize cross-ticket file conflicts by sequencing core semantic changes first.

## Dependency Graph

```
M0: 001 (clippy + corpus) | 002 (locale contract) | 003 (docs accuracy) | 102 (iterative calc decision)
     |
     v
M1: 301a (determinism) | 302 (CSV) | 401 (WASM fix) | 402 (Python stubs)  [all parallel]
     |
     v
M2: 101 (spill) ───────> 103 (@) + 201 (tables) + 202 (names)  [spill lands first, then rest parallel]
     |                        |
     v                        v
M3: 301b (provenance) + hardening + docs
```

**Key constraint**: Ticket 101 (dynamic arrays/spill) is a hard dependency for 103, 201, and 202. Spill changes fundamental grid semantics (when arrays occupy cells, how dependency edges work). The downstream tickets must be compatible with whatever spill model 101 establishes. Do not start 103/201/202 until 101's spill manager API is defined.

## Milestone 0 - Baseline Quality Gates (blocks everything)

Order
1) `tickets/00-foundation/001-quality-gates-ci-corpus.md` - Clippy + corpus harness
2) `tickets/00-foundation/002-locale-contract.md` - Document invariant-locale contract
3) `tickets/00-foundation/003-docs-accuracy.md` - Fix AGENTS.md, stale references
4) `tickets/10-engine/102-iterative-calc-decision.md` - Decide on iterative calc support

Why first
- Re-enabling clippy and establishing a corpus harness prevents regressions while multiple agents work in parallel.
- The corpus provides a place to encode acceptance tests for every subsequent ticket.
- The locale contract and docs accuracy are small items that establish trust and prevent confusion.
- The iterative calc decision is a compatibility policy that affects how we communicate limitations.

Note: Clippy triage may surface hundreds of warnings across 110K LOC. Budget 2-4 hours. Seed the corpus with time-independent fixtures only (no NOW/TODAY/RAND until 301a lands).

Definition of done (milestone)
- CI runs clippy with `-D warnings`.
- Corpus harness exists and is wired into CI.
- `locale.rs` has documented invariant-only contract.
- `AGENTS.md` accurately reflects crate structure.
- Iterative calc position is documented (recommended: explicitly reject with load-time warning).

## Milestone 1 - Agent-First Release Slice

This milestone produces a release that is immediately useful to AI/agent systems even before full spreadsheet parity.

Parallelization plan
- Agent A: deterministic evaluation mode (clock injection)
- Agent B: CSV backend
- Agent C: WASM AST correctness + packaging
- Agent D: Python type stub cleanup
- Integration agent: resolves conflicts, updates docs, extends corpus fixtures

Order and dependencies
1) `tickets/30-agent/301a-deterministic-evaluation.md`
   - Dependency: milestone 0 recommended but not required.
   - Unblocks: stable corpus snapshots for volatile formulas, reproducible SheetPort runs.
2) `tickets/30-agent/302-csv-backend.md`
   - Independent; can run in parallel with determinism.
3) `tickets/40-bindings/401-wasm-ast-refs-and-ts-packaging.md`
   - Independent; can run in parallel.
4) `tickets/40-bindings/402-python-type-stub-cleanup.md`
   - Independent; can run in parallel.

Milestone 1 checklist
- [ ] Deterministic mode documented and exposed via SheetPort and Python binding.
- [ ] NOW/TODAY return injected timestamp under deterministic mode.
- [ ] CSV read/write works with stable options (UTF-8, configurable delimiter).
- [ ] WASM parsing returns correct reference coordinates.
- [ ] WASM package entrypoint is coherent (TS wrapper wired in or removed).
- [ ] Python `.pyi` has no duplicate declarations; mypy passes clean.

## Milestone 2 - Spreadsheet Parity: Core Model Semantics

This milestone targets features that are prerequisites for being taken seriously as an IronCalc challenger.

Parallelization plan
- Agent D: dynamic arrays + spill (101) - **lands first**
- Agent E: defined names (202) - **after spill API is defined**
- Agent F: tables + structured refs (201) - **after spill API is defined**
- Agent G: implicit intersection (103) - **after spill model is established**
- Agent H: provenance/changelog (301b) - **independent, can run any time**
- Integration agent: ensures consistent resolver APIs and avoids duplicated semantics

**Critical sequencing**: 101 must land (or at least define its spill manager API) before 103, 201, and 202 begin implementation. 201 and 202 both touch `resolver.rs` — coordinate via integration agent or sequence 202 first (names are simpler and establish resolver patterns that tables can follow).

Order and dependencies
1) `tickets/10-engine/101-dynamic-arrays-spill.md`
   - **Hard dependency for 103, 201, 202**. Changes fundamental grid semantics.
   - Consider splitting into 101a (spill manager), 101b (eval integration), 101c (workbook/binding integration).
2) `tickets/10-engine/103-implicit-intersection-operator.md`
   - Depends on 101 (@ is the complement of spill: reduces arrays to scalars).
   - Partially depends on 201 for `[@Column]` syntax.
3) `tickets/20-model/202-named-ranges-defined-names.md`
   - Engine layer already has CRUD + dependency tracking (graph/names.rs). Work is primarily wiring workbook resolver to engine + completing NamedDefinition::Range eval.
   - Depends on 101 for range-valued names that produce arrays.
   - Consider deferring formula-backed names to a follow-up.
4) `tickets/20-model/201-tables-structured-references.md`
   - Depends on 101 for table column references that produce arrays.
   - Conflicts with 202 in resolver.rs — sequence 202 first or use integration agent.
5) `tickets/30-agent/301b-changelog-provenance.md`
   - Independent of the above. Can run any time in M2 or M3.

Milestone 2 checklist
- [ ] Spill semantics are end-to-end: SEQUENCE/FILTER/SORT produce grid-occupying results.
- [ ] Spill conflicts produce `#SPILL!` and clear correctly.
- [ ] `@` operator tokenizes, parses, and evaluates (row/column intersection).
- [ ] Defined names resolve with correct scope precedence and graph dependencies.
- [ ] Range-valued named definitions evaluate (not `#N/IMPL`).
- [ ] Structured references work for a documented subset and roundtrip via JSON backend.
- [ ] `[@Column]` (this-row) resolves correctly in table context.
- [ ] SheetPort reads spilled output values correctly.

## Milestone 3 - Release Hardening

Order
- Expand corpus fixtures to cover:
  - spill conflict/resize
  - name scope precedence
  - table selectors and this-row semantics
  - CSV import + formula overlay
  - implicit intersection edge cases
  - iterative calc rejection (if Option A)
- Add binding-level end-to-end tests for at least one representative feature per milestone.
- Complete 301b (provenance/changelog) if not done in M2.
- Update public docs (`README.md`, `V1_RELEASE.md`) with a supported-feature matrix.
- Address WASM `Instant::now` limitation (web_time crate or documented skip).
- Address WASM date/time/error string degradation (structured JS objects).

## Cross-ticket compatibility checklist (applies to all milestones)
- Every new semantic feature has:
  - at least one engine-level test (Rust)
  - at least one integration-level test (workbook or binding)
  - a corpus fixture entry (when deterministic)
- New errors are stable: kind is consistent and payload is actionable.
- Public APIs document any deliberate incompatibilities vs Excel.

## Suggested worktree assignment

| Worktree | Tickets | Notes |
|----------|---------|-------|
| worktree-quality | 001, 002, 003, 102 | M0 foundation, can be one agent |
| worktree-agent-determinism | 301a | M1 |
| worktree-agent-csv | 302 | M1 |
| worktree-wasm | 401 | M1 |
| worktree-python-stubs | 402 | M1, small enough to combine with another |
| worktree-engine-spill | 101 | M2, lands first |
| worktree-model-names | 202 | M2, after spill API defined |
| worktree-model-tables | 201 | M2, after 202 or with integration agent |
| worktree-implicit-intersection | 103 | M2, after spill model established |
| worktree-provenance | 301b | M2/M3, independent |
