# Option E execution plan — `AxisRange` migration

> See `sheet-region-index-tail-extent-precision.md` for the architectural memo and the rejected alternatives. This document is the phased execution plan for the adopted Option E.

## Goals & non-goals

**Goal:** Replace the current `Region` enum + parallel `AxisExtent` / `QueryAxisExtent` / `BoundedAxisExtent` types with a unified per-axis `AxisRange = Point | Span | From | To | All` model. Eliminate sentinel `u32::MAX` as a tail carrier. Restore full structural-tail precision.

**Non-goals:** Don't change FormulaPlane semantics. Don't change span eval. Don't reorganize crate structure. Don't expand to v0.7 lookup-cache work or lazy-reads (Position 3). Each phase is independently revertible without losing earlier-phase work.

## Sequencing principle

The migration uses **strangler-fig** discipline: introduce the new type alongside the old, prove correctness, then collapse old representations. No phase removes a public-ish API or breaks downstream callers without first standing up the replacement.

Each phase ships in its own branch/worktree, gets its own dispatch, has its own validation gate, and commits independently. **Rollback at any phase boundary returns to a known-good state without losing prior phases.**

## Phase summary

| Phase | Title | Branch | LOC | Time | Depends on | Ships in |
|---:|---|---|---:|---:|---|---|
| 0 | Option A — half-open variants | `formula-plane/region-tail-extent-variants` | ~500 | 2-3d | v0.6.0-rc1 | v0.6.x |
| 1 | `AxisRange` type internal-only | `formula-plane/axis-range-internal` | ~400 | 2-3d | Phase 0 | v0.7 (or .8 dev) |
| 2 | `SheetRegionIndex` axis-range dispatch | `formula-plane/region-index-axis-range` | ~600 | 4-5d | Phase 1 | v0.8 dev |
| 3 | Producer / dirty-closure axis-range propagation | `formula-plane/dirty-closure-axis-range` | ~400 | 3-4d | Phase 2 | v0.8 dev |
| 4 | `Region` variant collapse | `formula-plane/region-variant-collapse` | ~250 | 2-3d | Phase 3 | v0.8 dev |
| 5 | Test consolidation + benchmark hardening | `formula-plane/region-axis-test-pass` | ~450 | 2-3d | Phase 4 | v0.8 release |

**Total: 6 phases, ~2,600 LOC, ~3 weeks of focused build dispatch.**

The cumulative work is a single coherent migration but no two phases conflict at the file level except where explicitly noted. Phases 0-1 ship in different release cycles; phases 2-5 can stack within v0.8 development.

## Phase 0 — Option A: half-open variants (proving step)

### Scope
- Add `Region::RowsFrom { sheet_id, row_start }` and `Region::ColsFrom { sheet_id, col_start }` variants.
- Add dedicated `rows_from`/`cols_from` HashMap-keyed indexes in `SheetRegionIndex` mirroring the existing `whole_rows`/`whole_cols` precedent.
- Replace the s035 workaround `structural_change_scope_for_region` with precise tail-extent recording.
- Update `intersects()`, `axis_extents()`, `query_extents()`, `bounded_extents()`, `project_changed_region()` to handle the new variants.
- Updates to existing 209 formula_plane unit tests: minor (most don't pattern-match the new variants because they're new).
- New tests: structural-tail precision regression (s035-shape) verifying surviving spans recompute zero placements.

### Branch & worktree
- Branch: `formula-plane/region-tail-extent-variants`
- Worktree: `.worktrees/formula-plane-region-tail-extent-variants`
- Forks from: `formula-plane/fp6-runtime-20260503` HEAD (post-rc1)

### Subagent dispatch
- Single build agent, model `openai-codex/gpt-5.5`.
- Brief: cite this document and `sheet-region-index-tail-extent-precision.md` (Phase 0 scope = Option A).
- Hard-scoped: don't touch `AxisExtent`/`QueryAxisExtent`/`BoundedAxisExtent` internals beyond adding `From` arms. Keep `Region` enum stable except for the 2 new variants.

### Acceptance gate
- All existing 209 + 15 (formula_plane + engine/tests) tests pass.
- New regression test: s035-shape workbook with column-delete outside any span's read/result region surviving spans recompute 0 placements (assert via `EvalResult.computed_vertices`).
- Probe-corpus medium: s034/s035 phase_recalc < 100ms (down from current ~50-200ms broadening tax).
- Workbook-tests + bench-core tests pass.

### Rollback
- Revert single commit. Workaround `structural_change_scope_for_region` (the WholeSheet broadening) is still in tree until this phase commits — explicitly removed at the end of phase 0. Rollback restores the broadening; correctness preserved.

### What's committed
- One commit titled `feat(formula-plane): half-open RowsFrom/ColsFrom region variants for structural tail precision`.
- Body: explicitly call out the s035 perf delta restored, link the memo, document that this is "Phase 0 / proving step" of the longer Option E migration plan.

### PM checkpoint after Phase 0
**Decision gate:** does Option A's design hold up under production usage? Run for ≥1 release cycle. If new sharp edges surface (overflow at projection time, query overhead regression, etc.), they're caught in scope at small surface — informing Phase 1+ design.

## Phase 1 — `AxisRange` type, internal-only

### Scope
- Add `AxisRange` enum to `region_index.rs`:
  ```rust
  pub(crate) enum AxisRange {
      Point(u32),
      Span(u32, u32),
      From(u32),
      To(u32),
      All,
  }
  ```
- Implement `AxisRange::intersects`, `AxisRange::contains`, `AxisRange::query_bounds`, `AxisRange::project_through_offset`, `AxisRange::is_bounded`.
- Replace `enum AxisExtent` (currently `Span | All` in `region_index.rs:236`) and the parallel `QueryAxisExtent` / `BoundedAxisExtent` (in `producer.rs:930+`) with internal use of `AxisRange`. Keep adapter functions for now where producer.rs's `bounded_extents()` returns `None` for tails — i.e., the API shape returned by producer helpers stays compatible at the boundary.
- `Region::axis_extents()` returns `(AxisRange, AxisRange)` instead of `(AxisExtent, AxisExtent)`.
- All `intersects`/projection logic is keyed off `AxisRange` arithmetic.
- **No public API change.** `Region` enum, constructors, and pattern-matching call sites unchanged.

### Branch & worktree
- Branch: `formula-plane/axis-range-internal`
- Worktree: `.worktrees/formula-plane-axis-range-internal`
- Forks from: `main` after Phase 0 has merged.

### Subagent dispatch
- Single build agent.
- Brief includes:
  - `AxisRange` is an internal-only type; the public `Region` enum stays stable.
  - Replace `AxisExtent` (`region_index.rs:236-258`) and the parallel `QueryAxisExtent`/`BoundedAxisExtent` types in producer.rs with `AxisRange`-based logic.
  - Keep `query_extents()` / `bounded_extents()` API signatures stable (they return `Option<(AxisRange, AxisRange)>` instead of the current parallel types). The function behavior is preserved; only return types change.
  - Mathematical equivalence test: existing axis-extent intersection arithmetic must produce identical results pre/post.
- Hard scope: don't change `Region` variants; don't change index data structures; don't touch `compute_dirty_closure` semantics.

### Acceptance gate
- All existing tests pass.
- New `AxisRange` unit tests: ~30 tests covering all 25 (Point|Span|From|To|All)² × intersection combinations. Plus offset projection arithmetic (especially `From(N) + offset` overflow handling).
- Performance microbench: `intersects()` cycle count comparable to current implementation (no regression). The new arms can be a small constant slower (e.g., 5-10%) but not 2x.
- Workbook-tests + bench-core: full parity at small scale, 0 divergences.

### Rollback
- Single revert. No external API changes; rolling back is purely internal type change.

### What's committed
- One commit titled `refactor(formula-plane): introduce AxisRange internal type for axis arithmetic`.

### PM checkpoint after Phase 1
- Verify benchmarks (Off↔Auth parity perf summary) show no regression on the corpus.
- If any regression on a specific scenario class, halt and investigate before Phase 2.

## Phase 2 — `SheetRegionIndex` axis-range dispatch

### Scope
This is the biggest phase. Rewrite `SheetRegionIndex` to dispatch insertion and query by `AxisRange` kind pairs instead of `Region` variant matches.

**Insertion path:**
- `insert_entry(region: Region, value: T) -> usize` — keep the public signature.
- Internally: extract `(rows: AxisRange, cols: AxisRange) = region.axis_ranges()`.
- Dispatch to one of these index families based on the `AxisRange` kind pair:
  - `(Point, Point)` → `points` map
  - `(Point, Span/From/To)` → `points_by_row` (or current `row_intervals`)
  - `(Span/From/To, Point)` → `points_by_col` (or current `col_intervals`)
  - `(Span, Span)` (bounded) → `rect_buckets` (existing path)
  - `(Span/From, From/Span)` (any tail) → new `tail_rects` family with row/col boundary indexes
  - `(All, Point)` → `whole_cols`
  - `(Point, All)` → `whole_rows`
  - `(All, All)` → `whole_sheets`
  - `(From, All)` / `(All, From)` → `tail_extents` family

**Query path:**
- `query(query: Region) -> RegionQueryResult<T>` — keep the public signature.
- Internally: dispatch by query's `AxisRange` kind pair, walking only the index families that could possibly contain matches.
- The 5 current `collect_*_candidates` functions consolidate into one `collect_candidates` that uses axis-range kind dispatch.

**Critical invariant:** for every (insert kind, query kind) pair, the new collector must return a candidate set that is a superset of all true intersections (so exact-filtering catches the rest). Test exhaustively: 5×5 = 25 cases.

### Branch & worktree
- Branch: `formula-plane/region-index-axis-range`
- Worktree: `.worktrees/formula-plane-region-index-axis-range`
- Forks from: `main` after Phase 1 has merged.

### Subagent dispatch
- Chain: planner agent first to produce the dispatch table for the 5×5 cases, then a build agent.
- Brief includes:
  - The 5×5 kind-pair dispatch table is THE design artifact for this phase. Produce it explicitly in `docs/design/formula-plane/region-index-axis-range-dispatch.md` during planning.
  - For each kind pair: which index families to insert into, which to query, what the worst-case complexity is.
  - Bucket explosion is FORBIDDEN: any kind pair containing `From`/`To` MUST route to the new `tail_extents` family, not `rect_buckets`.
  - Exact-filter post-step (using `region.intersects(&query)` at `region_index.rs:386-399`) remains the safety net.

### Acceptance gate
- All existing 20 region_index unit tests pass.
- New tests: 5×5 kind-pair insertion+query matrix, ~25 tests verifying superset-of-true-intersections.
- Bucket-explosion regression: a test that inserts/queries unbounded tail regions and asserts memory stays under 50MB and time stays under 100ms. (The test that originally OOMed.)
- All `formula_plane` and `engine/tests` suites pass.
- Probe-corpus medium parity: 0 divergences across all scenarios.
- Performance microbench: query for common case (single point edit → consumer-read query) within 5% of current.

### Rollback
- Single revert. Phase 1's `AxisRange` type stays in place; `SheetRegionIndex` returns to variant-dispatch.

### What's committed
- One commit titled `refactor(formula-plane): SheetRegionIndex axis-range kind dispatch`.

### PM checkpoint after Phase 2
- Re-run full corpus parity at medium scale.
- Specifically validate: structural-op-heavy scenarios (s034/s035) maintain Phase 0 precision; nothing else regresses.
- If regression on common-case point queries (>5% slowdown), halt and investigate.

## Phase 3 — Producer / dirty-closure axis-range propagation

### Scope
- `DirtyProjectionRule::project_changed_region` accepts axis-range "changed" extents end-to-end. Add `From` arms to the projection logic where they fit.
- `query_extents` / `bounded_extents` consolidate into a single `query_axes(region) -> (AxisRange, AxisRange)` and `bounded_axes(region) -> Option<(BoundedRange, BoundedRange)>` where `BoundedRange` is the finite subset of `AxisRange`.
- `compute_dirty_closure` propagates axis-range domains. Specifically: when fed an `AxisRange::From(N)` changed region, propagation must use the affine offset arithmetic correctly without overflowing at `u32::MAX`.
- `FormulaConsumerReadIndex::query_changed_region` uses the new types throughout.
- `FormulaProducerResultIndex` updates to use `AxisRange` extents.

### Branch & worktree
- Branch: `formula-plane/dirty-closure-axis-range`
- Worktree: `.worktrees/formula-plane-dirty-closure-axis-range`
- Forks from: `main` after Phase 2 has merged.

### Subagent dispatch
- Single build agent.
- Brief: focus on `producer.rs:413+` (`DirtyProjectionRule`) and `producer.rs:944-1014` (`bounded_extents` / `query_extents`).
- Hard scope: don't change `SheetRegionIndex` (Phase 2's territory). Don't change `Region` shape (Phase 4).
- Critical: overflow-safe arithmetic for `From(N)` projections. Add `u32::checked_add`/`checked_sub` everywhere; explicit overflow tests.

### Acceptance gate
- All existing 26 producer unit tests pass.
- New tests: dirty-closure propagation through `From`-tail changed regions. Verify span recompute counts match Phase 0 baseline.
- Overflow regression: `From(u32::MAX - 10)` projection through any affine offset doesn't panic.
- Probe-corpus medium parity: 0 divergences. Specifically s029/s039/s055 must NOT have regressed.

### Rollback
- Single revert. Phases 1 and 2 stay; producer returns to current arithmetic.

### What's committed
- Title: `refactor(formula-plane): dirty-closure projection through axis-range domain`.

### PM checkpoint after Phase 3
- Run the complete validation gate: full medium parity, full small parity, all scenarios.
- Run probe-corpus perf benchmarks: confirm s034/s035 phase_recalc precision matches Phase 0; confirm no regression elsewhere.

## Phase 4 — `Region` variant collapse

### Scope
This is the cohesion payoff. Collapse `Region` from 7 variants + 2 from Phase 0 (= 9 total) into the canonical form:

```rust
pub(crate) struct Region {
    pub(crate) sheet_id: SheetId,
    pub(crate) rows: AxisRange,
    pub(crate) cols: AxisRange,
}
```

- Keep `Region` as a type alias for `Region` (backward-compat sugar) for one minor release.
- All 9 constructor methods now build `Region` with the appropriate axis-range pair:
  - `Region::point(sheet, row, col)` → `Region { sheet, rows: Point(row), cols: Point(col) }`
  - `Region::rect(sheet, rs, re, cs, ce)` → `Region { sheet, rows: Span(rs, re), cols: Span(cs, ce) }`
  - `Region::whole_row(sheet, row)` → `Region { sheet, rows: Point(row), cols: All }`
  - `Region::whole_col(sheet, col)` → `Region { sheet, rows: All, cols: Point(col) }`
  - `Region::whole_sheet(sheet)` → `Region { sheet, rows: All, cols: All }`
  - `Region::col_interval(sheet, col, rs, re)` → `Region { sheet, rows: Span(rs, re), cols: Point(col) }`
  - `Region::row_interval(sheet, row, cs, ce)` → `Region { sheet, rows: Point(row), cols: Span(cs, ce) }`
  - `Region::rows_from(sheet, row_start)` → `Region { sheet, rows: From(row_start), cols: All }`
  - `Region::cols_from(sheet, col_start)` → `Region { sheet, rows: All, cols: From(col_start) }`
- Update all 211 constructor call sites + 11 match-arm sites in eval.rs / scheduler.rs / producer.rs.
- The match arms become `match (region.rows.kind(), region.cols.kind()) { (Point, Point) => ..., ... }` or use accessor methods on `Region` directly.

### Branch & worktree
- Branch: `formula-plane/region-variant-collapse`
- Worktree: `.worktrees/formula-plane-region-variant-collapse`
- Forks from: `main` after Phase 3 has merged.

### Subagent dispatch
- Single build agent.
- Brief includes:
  - This is a mechanical refactor; the compiler guides each call site.
  - Add accessor methods on `Region` (`is_point()`, `is_whole_row()`, `is_whole_col()`, `is_whole_sheet()`, `is_rows_from()`, `is_cols_from()`, `as_rect()`, `as_point()`, etc.) to simplify the call-site updates.
  - Keep the `Region` constructor sugar functions as static methods on `Region`.
  - Update test fixtures and assertions; the test surface is mechanical updates.
- Hard scope: don't change semantics; don't change index families; don't change projection.

### Acceptance gate
- All existing tests pass with mechanical updates.
- Full medium parity: 0 divergences.
- Full benchmark sweep: no scenario shows >5% perf delta from Phase 3 baseline.

### Rollback
- Single revert. Phases 1, 2, 3 stay intact. Variant enum is restored.

### What's committed
- Title: `refactor(formula-plane): collapse Region variants into Region { axes }`.

### PM checkpoint after Phase 4
- Architectural cleanup is essentially complete.
- Run the full benchmark + parity sweep.
- Update internal documentation referring to `Region` to reference `Region`.

## Phase 5 — Test consolidation + benchmark hardening

### Scope
- Consolidate the test suites that now have duplicated coverage (some Phase 0/1/2 tests cover ground that's now redundant).
- Add property-based tests over the AxisRange algebra: `(intersects . from_axis) ≡ (axis_intersects)`, `(project_through_offset(zero) ≡ identity)`, `(query.intersects(indexed) ≡ candidate_set.contains(indexed))`.
- Add benchmark suite for the structural-op + dirty-closure paths to track regression over time.
- Update design documents: revise `sheet-region-index-tail-extent-precision.md` to mark "Phase E complete"; cross-reference Region API.

### Branch & worktree
- Branch: `formula-plane/region-axis-test-pass`
- Worktree: `.worktrees/formula-plane-region-axis-test-pass`
- Forks from: `main` after Phase 4 merged.

### Subagent dispatch
- Single build agent.
- Brief: focus on test cleanup and property tests via `proptest`. Keep production code unchanged unless cleanup is forced.

### Acceptance gate
- All tests pass.
- Property tests catch obvious algebraic invariant violations (run with --test-threads=1 to ensure deterministic).
- Benchmark suite produces baseline numbers committed to docs.
- Architectural memo at `sheet-region-index-tail-extent-precision.md` updated to status: complete.

### Rollback
- Pure additive; no semantic rollback needed.

### What's committed
- Title: `chore(formula-plane): consolidate AxisRange tests + benchmark suite`.

### PM checkpoint after Phase 5
- Option E migration complete. v0.8 release candidate.

## Cross-phase machinery

### Subagent prompts (per phase)

Each phase's dispatch is a single build agent with the standard fp6-lineage prompt template:

1. Hard scope (no v0.7 features, no lookup-cache, no parallel span eval changes).
2. Worktree path.
3. Phase doc reference (e.g., `docs/design/formula-plane/dispatch/region-index-axis-range-phase{N}.md`).
4. Acceptance gate (specific tests + parity numbers).
5. Hard FORBIDDEN PATTERNS (no commits, stage with `git add -A`, no hedge language, etc.).
6. Stop conditions (regression on existing tests, perf regression >5%, OOM, etc.).
7. PM commits.

### Worktree creation

```bash
cd /home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/formualizer/platform-dev/codebase/oss/formualizer
git worktree add .worktrees/formula-plane-region-tail-extent-variants -b formula-plane/region-tail-extent-variants <base>
# At Phase 0 base = formula-plane/fp6-runtime-20260503 HEAD
# At Phase 1+ base = main after prior phase merged
```

### Memory safety per dispatch

- `CARGO_BUILD_JOBS=8` is the default in `~/.cargo/config.toml`.
- Build/test commands run via `systemd-run --user --scope -p MemoryMax=20G -p MemorySwapMax=0` for any phase that touches the test suite.
- sccache config keeps `incremental=true` for dev/test.
- Each phase's first build is cold; subsequent reuse via sccache.

### Validation gate per phase (standard)

```bash
cargo fmt --all -- --check
cargo clippy -p formualizer-eval --all-targets -- -D warnings
cargo clippy -p formualizer-workbook --all-targets -- -D warnings
cargo clippy -p formualizer-bench-core --all-targets -- -D warnings
cargo clippy -p formualizer-bench-core --features formualizer_runner --all-targets -- -D warnings

cargo test -p formualizer-eval --release  --quiet
cargo test -p formualizer-workbook --release --quiet
cargo test --workspace --release --quiet
cargo test fp8_ingest_pipeline_parity --release --quiet

cargo build -p formualizer-bench-core --features formualizer_runner --release \
  --bin probe-corpus --bin probe-corpus-parity

./target/release/probe-corpus-parity --scale small  --label phase{N}-small
./target/release/probe-corpus-parity --scale medium --label phase{N}-medium --enable-parallel true --phase-timeout-ms 60000
./target/release/probe-corpus       --scale medium --label phase{N}-perf  --modes off,auth --enable-parallel true --phase-timeout-ms 60000
```

### Per-phase commit policy

PM commits each phase to `main` after acceptance gate passes:
- One commit per phase (squash merge or rebase from worktree).
- Commit message body: phase name, scope summary, validation results, rollback instructions.
- Tag commits with `axis-range-phase-N` for easy navigation.

After all 6 phases complete:
- PR titled `formula-plane: complete AxisRange / Option E migration` summarizing the architectural cleanup.

### Dependencies & merge order

Strict linear: 0 → 1 → 2 → 3 → 4 → 5. No phase begins until previous merges to `main`.

Exception: Phase 5 (test consolidation) is independent enough that it could be deferred or interleaved with Phase 0-4 cleanup work. Treat as a soft dependency.

### Decision gates between phases

- **After Phase 0 (Option A):** PM decides whether to commit to full Option E. If "no", Phase 0 is a complete enough deliverable for v0.6.x precision recovery; phases 1-5 deferred.
- **After Phase 2 (the largest):** PM decides whether to ship Phases 1+2 in v0.7 as an internal-only refactor (not user-visible) or hold for v0.8. The architectural cleanup is mostly invisible to users.
- **After Phase 4 (variant collapse):** v0.8 release candidate ready.

### What ships when

- **v0.6.0:** WholeSheet broadening (current state, already committed).
- **v0.6.x point release:** Phase 0 (Option A: half-open variants, precision recovery).
- **v0.7 (optional):** Phases 1-2 if PM chooses to land internal refactor early. v0.7 still has user-facing deliverables: Phase 2c lookup cache, native parallel refinements, s029/s039/s055 dirty-closure fixes.
- **v0.8:** Phases 3-5 (or Phases 1-5 if v0.7 doesn't take 1-2). Full Option E completion.
- **v0.9:** Stable post-Option-E; reassess `Region` → `Region` typedef removal (final cleanup of backward-compat sugar).

### Risk per phase

| Phase | Risk | Why |
|---:|---|---|
| 0 | **Low** | Additive only; existing variants unchanged; small surface (~500 LOC); validates the design |
| 1 | **Low-medium** | Internal refactor; type-driven; existing API unchanged. Could break perf if AxisRange dispatch costs more than Span/All branch in `intersects()` |
| 2 | **Medium-high** | Largest surface; index family rewrite; 5×5 dispatch matrix is intricate. Risk of over-broad query expansion regressing common-case perf |
| 3 | **Medium** | Producer/projection rewrite; overflow arithmetic risk. Producer.rs is large (~1700 LOC) and sensitive |
| 4 | **Low** | Mechanical refactor; compiler-guided; backward-compat sugar preserved |
| 5 | **Very low** | Test consolidation; pure additive |

## Final notes

- **Each phase ships independently to main.** Even if Phase 3 reveals an unforeseen issue blocking Phase 4, the work in Phases 0-2 is on main and benefits the codebase.
- **Phase 0 is the only one that's user-visible** in terms of perf delta. Subsequent phases are architectural cohesion plays with neutral perf impact.
- **The 6-phase plan is total ~3 weeks of focused build dispatch.** With the existing parallel-agent OOM safeguards (jobs=8, mem-cap), each phase's validation gate is bounded by ~30-60min of cargo work + agent reasoning.
- **PM has a hard rollback at every phase boundary.** No phase is irreversible without losing earlier work.
