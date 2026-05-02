# Eval Flush + Unified Overlay Storage — Program Overview

Date: 2026-05-04
Branch (doc owner): `formula-plane/bridge`
Status: discovery / pre-investigation
Author intent: strategic context handoff for a deeper read-only investigation
on a fresh worktree off `origin/main`.

This document is **not** an implementation plan. It is the strategic context
for an investigation that will produce a design/feasibility memo.

---

## 1. Where this program came from

The current line of work began as the FormulaPlane bridge program. The
original goal was concrete:

> Reduce graph allocations on dense / shared-formula workbooks by
> representing copied formula runs as `(template, placements)` instead of
> `N ASTs + N vertices + N edge sets`.

Over the FP1 → FP4.B.1R sequence, we built the substrate needed to do that
safely: passive run/template observability, authority-grade canonical
template identity, passive dependency summaries with no under-approximation,
and function-owned dependency contracts (with the small but important
correction from FormulaPlane sidecar to `Function::dependency_contract(...)`
opt-in).

We then ran a **deliberately throwaway feasibility spike** to convert the
paper compact-representation accounting into measured numbers. The spike was
run on an in-memory synthetic fixture only (no XLSX loader involvement),
intentionally broke correctness for everything except `load → full eval →
read`, and was discarded after measurement. See:

- plan: `docs/design/formula-plane/dispatch/fpx-bridge-allocation-spike-plan.md`
- report: `docs/design/formula-plane/dispatch/fpx-bridge-allocation-spike-report.md`
  (lives on the throwaway worktree, see §6 below for its handling)
- artifacts: `target/fpx-bridge-allocation/581353b/`

That spike answered a single question:

> If we eliminate per-cell AST/vertex/edge materialization for one dense run
> and accept zero edit/dirty/volatile/structural support, do graph build
> time, RSS/allocation count, and full-eval time actually move?

It also surfaced the architectural discovery this document is about.

---

## 2. What the spike measured (rows = 100,000)

Synthetic fixture: `Sheet1!A1 = 1.0`; `Sheet1!B1..B100000 = "=$A$1*2"`;
single authority template, single dense column run.

| Metric | baseline | spike | spike/baseline |
|---|---:|---:|---:|
| Ingest median (ms) | 390.240 | 7.151 | **54.6× faster** |
| Full eval median (ms) | 60.936 | 36.768 | 1.66× faster |
| RSS peak (MB) | 240.863 | 37.297 | **6.46× smaller** |
| Graph vertices | 100,001 | 2 | 50,000× fewer |
| Graph edges | 100,000 | 1 | 100,000× fewer |
| AST roots | 100,000 | 1 | 100,000× fewer |
| `all_results_match` | true | true | match |

Verdict per the plan's heuristic: `BRIDGE_INCONCLUSIVE`. RSS clears the
validation threshold easily; full eval misses it (1.66× vs. required 2×).

But the inconclusive verdict is itself informative, in a useful direction:

- **Ingest, RSS, and graph allocation collapsed by 1–5 orders of magnitude.**
  The compact representation hypothesis is real; per-cell AST/vertex/edge
  materialization is genuinely the load and RSS bottleneck on dense runs.
  This is no longer paper.
- **Full eval did not collapse.** The spike still wrote 100k result cells one
  at a time into the value plane (per-cell `mirror_value_to_computed_overlay`
  calls), and that fill loop is now the dominant cost. The cost moved from
  "evaluate N times" to "write N cells".

The architectural question that remains is therefore not "how do we make
formula evaluation cheaper for dense runs" — the bridge already did that.
The question is **what is the storage and commit model for run-shaped
results, in a way that is consistent with the existing arrow_store + overlay
architecture?**

---

## 3. The architectural discovery

### 3.1 ComputedOverlay is a real prior, not scaffolding

`ComputedOverlay` was introduced by `cd73d90 Add computed overlay, fix
scheduler mirroring`. It is a Phase 0/1 piece of the arrow_store
architecture. It has been there long enough to acquire load-bearing
production behavior:

- per-chunk `computed_overlay: Overlay` lives inside every `ColumnChunk`;
- read cascade is `overlay → computed_overlay → base`, applied uniformly
  through ~12 hits in `range_view.rs` and several hits in `eval.rs`;
- there is a budget/mirroring system with auto-disable on overflow, and a
  compaction path (`compact_computed_overlay_chunk`,
  `compact_all_computed_overlays`) that merges overlay deltas into base
  lanes with strict invariants (e.g.
  `temporal_tags_preserved_across_computed_overlay_compaction`).

So this is genuine production architecture, not new ground.

### 3.2 But the scheduler/evaluator doesn't batch writes

The scheduler is already layered. `evaluate_layer_sequential` and
`evaluate_layer_parallel` operate on `Layer { vertices: Vec<VertexId> }`
and the apply path is, in principle, the natural place for a single
"per-layer commit" boundary.

Today it isn't used that way. Each evaluated vertex calls
`mirror_value_to_computed_overlay(...)` immediately during its apply path,
which:

- looks up the chunk by row offset;
- inserts one `(offset, OverlayValue)` entry into a per-chunk
  `HashMap<usize, OverlayValue>`;
- updates the byte-budget;
- conditionally compacts on budget overflow.

So a layer of 100,000 vertices produces 100,000 `HashMap::insert` calls
plus 100,000 byte-budget updates plus possible interleaved compaction. That
is the cost the spike's full-eval bar was measuring.

### 3.3 `OverlayValue` is the same idea as type-tag + lanes, implemented twice

`ColumnChunk` stores values as a tagged-union per row using:

```text
type_tag: UInt8Array              # which lane to look in
numbers:  Option<Float64Array>    # may be None if no numerics in this chunk
booleans: Option<BooleanArray>    # may be None
text:     Option<ArrayRef>        # may be None
errors:   Option<UInt8Array>      # may be None
```

`OverlayValue` is a Rust enum that carries the same kind of mixed-type
information per cell, but in HashMap form. The two representations exist
for different reasons:

- **base lanes** are immutable, columnar, large, encoded for read
  throughput;
- **overlay** is mutable, sparse, per-cell, encoded for write/edit
  responsiveness.

But they encode *the same logical data* in *different shapes*. That is a
duplication smell, and the duplication is what blocks the natural shape of
"a layer commits one batched delta into the storage".

### 3.4 Sorted/coalesced flush is an unexplored win independent of the bridge

If per-vertex apply is replaced with "collect all writes for the layer,
sort by `(sheet, col, row)`, coalesce adjacent same-value writes, commit
once at layer end", several wins land *without any bridge code at all*:

- a layer of 10,000 formulas like `=$A$1*2` (which all produce `2.0`
  because `A1=1`) becomes one run-shaped commit, not 10,000 inserts;
- a layer where 90% of cells are zero and 10% are non-zero compresses
  naturally (one big "all zero" run plus 10% per-cell writes);
- `IF(condition, X, Y)` patterns produce alternating runs of X and Y;
- even non-spike workloads with spatial locality benefit, because the
  flush amortizes Arrow lane build cost and avoids HashMap-probe
  overhead per write.

This is a refactor of how the evaluator commits, not a new feature, and it
is benefit-aligned with most dense-formula workloads, not just the
synthetic fixture.

---

## 4. The proposed direction

### 4.1 Reframing

The bridge is not "the next thing". The bridge is "a feature add on top of
a unified overlay + batched flush model." The unified-overlay-and-flush
work is what the bridge requires to be small, and it is independently
valuable for any dense-locality workload.

Specifically:

- The piece of the bridge that *is* genuinely new — **scheduler / evaluator
  treating a run as a single work unit** — only pays off cleanly if the
  storage layer can absorb a run-shaped commit. Today it cannot, so the
  bridge spike collapsed eval cost into write cost.
- If the storage layer *can* absorb a run-shaped commit, then the bridge
  becomes a much smaller change: emit one run-shaped write intent into the
  layer flush, instead of N per-cell intents.

So the program reorders to put the storage and flush model first.

### 4.2 The conjecture being investigated

Stated bluntly:

> Replace `OverlayValue` with a representation that mirrors the base
> lane/tag design: a (small or sparse) Arrow-shaped overlay with the same
> type_tag and per-lane structure as base chunks, with optional run
> encoding for batched writes. Make per-layer batched commit the default
> path for computed results, with sort + coalesce inside the flush.

Two related claims:

1. **Unified storage shape.** `OverlayValue` and per-lane base storage are
   two encodings of the same idea. A unified shape — sparse / run-encoded
   Arrow-shaped overlay fragments that compact into the base lanes —
   removes a real piece of architectural debt and removes the impedance
   mismatch between overlay and base.

2. **Per-layer batched commit.** The scheduler is already layered; the
   commit path should be too. Sorting and coalescing inside the flush
   produces run-shaped writes naturally for any layer with locality. This
   is span-agnostic: it benefits non-bridge dense workloads, and it is
   the necessary substrate for the bridge to commit once-per-run instead
   of N-times-per-run.

Together, these two change the economics of the bridge: the bridge stops
being a special case and becomes the natural shape of "a layer that
produces one logical value spanning a placement region".

### 4.3 Phased shape (informal)

This is the *approximate* phasing the investigation will validate or refute.
None of these phases is committed. None has a plan yet.

```text
Phase 0  (investigation)
  fresh worktree off origin/main, fresh agent, deep read of:
    arrow_store overlay/base lane model,
    range_view read-cascade,
    eval.rs apply / mirror paths,
    scheduler layer model and parallel apply,
    compaction and budget paths.
  Produce a design/feasibility memo.
  Optional small flush-coalescing measurement only if memo authoring
  surfaces a question that requires it.

Phase 1  (unified overlay storage)
  Replace OverlayValue with lane-typed Arrow-shaped overlay fragments.
  Keep per-cell write API for user edits; add coalescing-friendly
  primitives for computed flushes.
  Preserve read cascade semantics and compaction invariants exactly.

Phase 2  (per-layer batched commit)
  Refactor evaluator apply paths to defer computed writes to a per-layer
  flush. Sort + coalesce inside flush. Run-encoded overlay fragments for
  contiguous identical writes.

Phase 3  (bridge work, much smaller than originally framed)
  Scheduler + evaluator extension: a layer entry can be Vertex(VertexId)
  or Run(FormulaRunId). Compute the run once, emit one run-shaped write
  intent into the per-layer flush. Run-level dirty propagation, edit
  handling, range queries, etc. ladder onto this.

Phase 4  (deferred)
  RunEnd encoding in base lanes themselves, only if measurements after
  Phase 2/3 show base-storage RSS is the next bottleneck.
```

Each phase is independently shippable. Phase 1+2 land wins on dense
non-bridge workloads. Phase 3 lands the bridge. Phase 4 is dependent on
real measurement, not anticipated need.

---

## 5. Why this is the right pre-work for the bridge

### 5.1 The bridge becomes much smaller

With Phase 1+2 in place, the bridge's "produce a run write" simply becomes
"emit a run-encoded write intent into the layer flush" — no new write API,
no special-case overlay path, no parallel storage hierarchy. The new
correctness work for runs (reverse-dependent invariant, run-level dirty
propagation, edit-inside-run, range queries) becomes the focus, not the
storage plumbing.

### 5.2 Wins for non-bridge workloads

Sorted-flush coalescing helps any dense layer, not just runs. This means
we can ship a measurable improvement before any span code lands. That is
genuinely independent value, not just bridge enablement.

### 5.3 We pay down architectural debt

`Overlay` as a HashMap and `OverlayValue` as a parallel tagged union were
Phase 0/1 expedients. Making overlay storage consistent with base storage
is the right shape for v1, and avoids the repeated "is this the overlay
or the base?" branching that read paths have today.

### 5.4 The spike's RSS win lands in production, not just benchmarks

Per-cell overlay storage at 100k entries per run would burn the overlay
budget and trigger `mirror_value_to_computed_overlay` auto-disable on real
workbooks. That undoes the bridge's RSS payoff in any production-shaped
workload. Run-encoded overlay fragments keep RSS small and keep the
mirroring path enabled.

---

## 6. Constraints and explicit non-goals

### 6.1 Worktrees to preserve

- `formula-plane/bridge` (this worktree) — preserves the FormulaPlane
  bridge program, the FP4.A passive dependency summaries, the function-
  owned dependency contracts (FP4.B.1R), and the spike *plan* commit.
  This stays as-is. Do not rebase, do not merge, do not delete.
- `formula-plane/spike-allocation-20260502` — the throwaway spike
  implementation. The spike *report* and artifact directory are on this
  branch. Per the spike plan §9, only the report and artifacts should be
  cherry-picked to a small report-only follow-up branch on
  `formula-plane/bridge`; the implementation source must not merge.
  Pending: cherry-pick. The branch itself can be deleted after that.
- `migration/core-overlay` — historical Core+Overlay migration worktree;
  paused, do not touch.
- All other `phase-*-build` and `phase-*-review` worktrees — historical,
  do not touch.

### 6.2 What this program is not

- This is not a continuation of the Core+Overlay production closeout.
  Phases 9.Q.5 / 9.Q.6 remain paused. This program does not reopen them.
- This is not a public API change. The function call API
  (`Function::eval`, `Function::eval_reference`, `ArgumentHandle`,
  `FunctionContext`) does not change.
- This is not a scheduler restructure. The scheduler stays layered;
  parallel evaluation stays. The change is in the *commit* path, not the
  schedule shape.
- This is not a new feature flag long-term. Short-term feature gate
  during development is acceptable; the goal is one unified path.
- This is not a `RangeView` API change. `RangeView` already iterates
  lane-typed chunks; the storage change should be transparent to its
  callers.
- This is not Run-encoded *base* lanes (Phase 4). Base lanes can stay
  dense initially.
- This is not a bridge correctness ladder. Phase 3 is deferred to *after*
  Phase 1+2 land.

### 6.3 What is preserved from previous program work

- `Function::dependency_contract(arity) -> Option<FunctionDependencyContract>`
  on `crates/formualizer-eval/src/function.rs` and the colocated builtin
  opt-ins. This is the right ownership model and it stays.
- `formula_plane/template_canonical.rs`, `formula_plane/dependency_summary.rs`,
  `formula_plane/span_store.rs`, `formula_plane/span_counters.rs`,
  `formula_plane/diagnostics.rs`. These remain passive infrastructure.
  They are inputs to Phase 3 (bridge) but not Phase 1/2.
- The `function_contract.rs` types in `crates/formualizer-eval/src/`. These
  are stable and used by builtin opt-ins.
- The spike report and its artifact directory (after cherry-pick).

---

## 7. Things the deeper investigation must answer

These are the questions the next phase (a fresh worktree off `origin/main`,
fresh agent, read-only investigation) needs to produce defensible answers
to. The investigation should produce a design/feasibility memo, not code.

### 7.1 Storage model

1. Can a unified Arrow-lane-shaped overlay representation faithfully
   express everything `OverlayValue` expresses today, including:
   - all `OverlayValue` variants (Empty, Number, DateTime, Duration,
     Boolean, Text, Error, Pending);
   - the byte-budget accounting model used by
     `disable_computed_overlay_mirroring_due_to_budget`;
   - per-chunk locality;
   - sparse `sparse_chunks` placement?
2. What is the smallest set of Arrow array shapes needed for sparse and
   run-encoded overlay fragments?
3. Is it cleaner to keep `Overlay` and add a sibling `RunOverlay`, or to
   make `Overlay` itself encoding-aware?
4. How does mixed encoding compose? A column might have a dense base, a
   small per-cell user-edit overlay, and a large run-encoded computed
   overlay simultaneously.

### 7.2 Read cascade

5. The read cascade `overlay → computed_overlay → base` is hit in many
   places. Audit every reader (`range_view.rs`, `eval.rs`, callers of
   `Overlay::get` / `Overlay::any_in_range`). Does every reader compose
   with lane-typed sparse overlay storage, or are there call sites that
   fundamentally assume HashMap semantics?
6. What is the read-path performance impact of lane-typed sparse overlay
   vs. HashMap probes? (Probably better; needs to be confirmed.)

### 7.3 Compaction and invariants

7. Compaction merges overlay into base. With unified storage, compaction
   becomes encoding-to-encoding. What are the invariants
   (`temporal_tags_preserved_across_computed_overlay_compaction`,
   canonical value contract `arrow_canonical_606`, mirror lifecycle) that
   must survive, and how do they translate?
8. Does the compaction path need to preserve run encoding when the
   accumulated overlay is run-shaped, or does it always materialize to
   dense in the base? (Phase 4 question, but the answer affects whether
   Phase 1+2 alone leave RSS wins on the floor.)

### 7.4 Budget and mirroring

9. The byte-budget today is `OverlayValue::estimated_payload_bytes` per
   entry. With Arrow-shaped overlays, the byte estimate is well-defined
   per-lane. Does the budget accounting become more accurate, less
   accurate, harder to compute? The
   `disable_computed_overlay_mirroring_due_to_budget` safety valve cannot
   regress.

### 7.5 Per-layer batched commit

10. What's the smallest API change to the apply paths
    (`evaluate_layer_sequential_effects`,
    `evaluate_layer_parallel_with_delta_effects`, etc.) that defers
    computed writes to a per-layer flush?
11. How do parallel apply paths emit thread-local write intents, and how
    does the flush coalesce them deterministically?
12. Sort key for flush: `(sheet_id, col, row)`. Is there a case where a
    different sort order produces materially better coalescing
    (e.g. row-major because most templates expand by column)?
13. Coalesce gap rule: only contiguous offsets, never collapse gaps. What
    is the smallest property test that pins this?

### 7.6 Concurrency

14. Today's `mirror_value_to_computed_overlay` is synchronous per-vertex.
    Per-layer flush moves the write commit out of the parallel section.
    Does the existing parallel apply path
    (`apply_parallel_vertex_result`) need restructuring, or does it
    naturally emit thread-local write intents that the flush merges?
15. How does this interact with `DeltaCollector` (the "what changed since
    last eval" tracking)?

### 7.7 Spill, dynamic, volatile

16. Spill (dynamic array) results today use a per-cell overlay write per
    spilled cell. Does spill compose naturally with run-shaped flush
    (a spill region is a run with one value per cell, not necessarily a
    coalescible run)?
17. Volatile redirty operates per-vertex. Does anything about per-layer
    flush change volatile correctness?
18. Dynamic functions (INDIRECT, OFFSET) re-target on retarget. Does
    layer-flush change anything about virtual-dependency telemetry?

### 7.8 User-edit overlay

19. The user-edit overlay `pub overlay: Overlay` is per-cell by API
    (`set_cell_value`). Does it use the same unified storage as
    computed_overlay, or is it kept HashMap-backed because user edits are
    inherently sparse and arbitrary? Recommendation in §4.2 was "same
    storage, different commit model". Confirm or reject.

### 7.9 Save/output

20. Save paths (XLSX, JSON) read through the same cascade. With unified
    overlay storage, does save need any changes, or does the read path
    flatten naturally?

### 7.10 Risk surfaces

21. What is the test surface that will catch regressions during the
    refactor? Identify the canary-suite-grade tests that must remain
    green throughout.
22. What is the smallest reversible feature gate during development?

---

## 8. Suggested investigation execution

### 8.1 Worktree

- Create a fresh worktree off `origin/main` (not off
  `formula-plane/bridge`). The investigation should be uncontaminated by
  in-flight FormulaPlane bridge documents and FP4 phase docs, except as
  references.
- Suggested branch name: `eval-flush/investigation-XXXX` where XXXX is a
  short tag (date or sequence).

### 8.2 Agent

- A single fresh `openai-codex/gpt-5.5` task subagent, dispatched with a
  detailed brief that points at this document and the specific files to
  audit.
- Read-only for code. Allowed to write design docs and feasibility memos.
- Not allowed to dispatch sub-agents, make code changes, or run anything
  beyond build / test commands needed to confirm baseline.

### 8.3 Output

- A design/feasibility memo at:
  `docs/design/eval-flush/dispatch/investigation-memo-XXXX.md`
- Audit findings (read-cascade audit, compaction-invariant audit,
  budget-accounting audit) as appendices in the same doc or as siblings
  under `docs/design/eval-flush/dispatch/`.
- A go/no-go-with-reasons recommendation for Phase 1+2 as a real program.
- Optionally: identification of a small, low-cost measurement that would
  confirm or refute the per-layer-flush eval-time win in isolation, if
  the memo authoring surfaces a question the docs alone cannot answer.
  Measurement is *optional*, not required, and must not exceed bounded
  cost.

### 8.4 What the brief should explicitly forbid

- No code refactor of overlay or evaluator.
- No bridge work.
- No changes to function trait or contract types.
- No public API surface changes.
- No spike implementation.
- No merging of the investigation branch into anything.

### 8.5 What happens after the memo

- Human review of the memo.
- If the memo's recommendation is "go", a Phase 1 implementation plan is
  drafted (separately, also reviewed) and an implementation worktree is
  created off `origin/main` with a fresh agent.
- If the recommendation is "no-go", the bridge program restarts with
  Phase 3 (run-as-work-unit) on the existing per-cell overlay path,
  accepting the eval-time and RSS limits the spike measured.

---

## 9. Strategic posture

To restate the program's honest current posture, for any future reader:

- The Core+Overlay production closeout is **paused**. Phases 9.Q.5/9.Q.6
  remain not-resumed.
- The FormulaPlane bridge program is **paused at FP4.B.1R**. The
  function-owned dependency contracts and passive dependency summaries
  are landed on `formula-plane/bridge` and remain useful substrate.
- FP4.B.2 / FP4.B.3 / FP4.B.4 / FP4.B.5 / FP4.B.6 / FP4.B.7 / FP4.C /
  FP4.D / FP5 / FP6 / FP7 are explicitly **not the next thing**. They
  are not cancelled, but they are not the immediate priority.
- The bridge spike `BRIDGE_INCONCLUSIVE` verdict is **not the basis for
  abandoning the bridge**. It is the basis for inserting the
  unified-overlay-and-flush program *before* the bridge's correctness
  ladder.
- This program is the next strategic priority, behind a deeper
  investigation that confirms feasibility on the existing
  arrow_store + scheduler + overlay shape.

That investigation is what this document leads into. It does not begin
without explicit user approval.

---

## 10. Summary in one paragraph

The bridge spike confirmed that representing dense formula runs
compactly collapses ingest, RSS, and graph allocation by orders of
magnitude. It also surfaced that the remaining cost is per-cell overlay
write commits, not formula evaluation. The remediation isn't a bigger
bridge; it's unifying overlay storage with the base lane/tag model,
making per-layer batched flush the default commit path, and letting the
bridge become a small "a layer entry can be a run" extension on top of
that. This is independently valuable for any dense-locality workload,
not just runs. The next step is a deep read-only investigation on a
fresh worktree off `origin/main`, with a fresh agent, producing a
design/feasibility memo. Implementation does not begin until the memo is
reviewed.
