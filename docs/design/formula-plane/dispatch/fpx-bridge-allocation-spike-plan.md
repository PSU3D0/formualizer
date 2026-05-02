# FPX Bridge Allocation Spike — Plan

Date: 2026-05-04
Branch (plan owner): `formula-plane/bridge`
Plan commit will live on `formula-plane/bridge`. Implementation must be on a
separate, throwaway branch. The implementation branch must **not** be merged.

## 1. Why this spike exists

FP2.B/FP3 produced compact-representation accounting numbers such as a
`50,000x` ratio for `fanout_100k`. Those are paper estimates, not measured
runtime wins. Before investing further in correctness scaffolding (FP4.B
expansion, FP4.C, FP4.D, FP5, FP6, FP7), we need one falsifiable measurement:

> If we eliminate per-cell AST/vertex/edge materialization for one dense run
> and accept zero edit/dirty/volatile/structural support, do graph build time,
> RSS/allocation count, and full-eval time actually move?

This spike answers that single question. Nothing else.

## 2. Honest framing

The spike intentionally breaks correctness everywhere except the one fixture's
load → full eval → read path. It is a feasibility probe, not the start of a
real implementation. The implementation branch is to be discarded after the
report is written.

If the spike succeeds, we then design a real bridge with the correctness work
(reverse-dependent invariant, run-level dirty propagation, edit handling,
result storage strategy, volatile/dynamic/structural propagation, cycle
reasoning, run-aware lookup semantics) on a fresh, properly-architected branch.

If the spike fails or shows marginal numbers, we drop the bridge approach as
the primary lever and redirect optimization effort.

## 3. Fixture

Synthetic, in-memory, built directly through engine APIs. **No XLSX loader,
no Calamine, no Umya, no scenarios.yaml, no shared-formula loader work.**

Spec:

- one sheet, `Sheet1`;
- `A1` = number `1.0`;
- `B1..B100000` each contain the formula `=$A$1*2`;
- exactly one true authority template for the entire run;
- one absolute precedent: `A1`;
- no chain, no editing, no volatile, no dynamic, no cross-sheet, no structural
  change, no name, no table, no array.

This dodges most of the correctness scaffolding the bridge would otherwise
need. The fixture is chosen precisely so the bridge can be evaluated under
maximally favorable conditions.

If 100k rows is impractical for a quick run, also support smaller sweeps:
1k, 10k, 100k. All three sizes must be measured.

## 4. Two ingest paths to compare

### 4.1 Baseline path (legacy)

Standard ingest as today:

- via `Engine`'s public formula-write APIs or
  `crate::engine::ingest_builder::BulkIngestBuilder`;
- produces N formula vertices, N AST roots, N graph edges to `A1`, full
  per-cell formula metadata.

This is the existing path. Do not modify it.

### 4.2 Spike "bridge" path

Add a **new, opt-in** ingest entry point gated behind a non-default,
doc-hidden feature `fpx_bridge_spike` on `formualizer-eval`:

- accept a `(template_ast, placement_range)` plus the precedent set;
- store **one** template AST (one allocation in the AST arena);
- create **one** template-vertex (or equivalent) representing the run with
  outgoing edges to the precedents;
- do **not** create per-cell formula vertices, per-cell ASTs, or per-cell
  outgoing edges to precedents;
- on full eval, compute the template once, then write the result into the
  Arrow-backed value plane at every coord in the placement range (this uses
  the existing value-storage path so reads of `B5` etc. work normally);
- do **not** wire dirty propagation, edits, or any change handling for cells
  inside the run.

The spike path may live in a new module
`crates/formualizer-eval/src/formula_plane/spike_run_ingest.rs` or similar.
Do not put it on the public default surface. Do not couple it to
`scan-formula-templates` or the FormulaPlane diagnostics feature.

### 4.3 Constraints

- Existing public APIs must compile unchanged with the feature off.
- Existing tests must pass with the feature off.
- With the feature on, only the new spike module and the synthetic fixture
  exercise the new code path. No production code path is allowed to call into
  the spike path.

## 5. What is intentionally not implemented

The spike **must break** the following. Do not attempt to support them:

- single-cell edit inside the run;
- dirty propagation through a run cell;
- volatile/dynamic functions involving the run;
- structural row/column insert/delete;
- cycle detection involving run cells;
- name/table mutation involving the run;
- range queries (`SUM`, `VLOOKUP`, etc.) into the run's result region from
  outside the spike fixture;
- any incremental recalc of the run.

Tests that exercise any of those for the spike path are explicitly out of
scope. The spike is a load-then-full-eval-then-read probe.

## 6. Measurement protocol

A new bench-only binary
`crates/formualizer-bench-core/src/bin/spike-bridge-allocation.rs`
must support:

```text
spike-bridge-allocation \
  --mode {baseline, spike} \
  --rows {1000, 10000, 100000} \
  --repeats {default 5} \
  --warmup {default 1} \
  --json-out <path>
```

Each run produces a JSON record with at minimum:

```json
{
  "mode": "baseline|spike",
  "rows": 100000,
  "ingest_ms_min": ...,
  "ingest_ms_median": ...,
  "ingest_ms_max": ...,
  "full_eval_ms_min": ...,
  "full_eval_ms_median": ...,
  "full_eval_ms_max": ...,
  "rss_peak_mb": ...,
  "allocator_extra": { ... },   // any additional allocator counters available
  "graph_vertex_count": ...,
  "graph_edge_count": ...,
  "ast_root_count": ...,
  "formula_cell_count": ...,
  "spot_check_results": {
    "B1": ...,
    "B5": ...,
    "B100000": ...
  },
  "all_results_match": true,
  "head": "<git rev-parse --short HEAD>"
}
```

RSS sampling:

- prefer `procfs::Process::stat()` peak RSS where available;
- otherwise document the proxy used.

Allocator counters:

- if `jemalloc-ctl` is already a workspace dep (it is, per #66), expose
  `stats.allocated` before ingest, after ingest, after full eval;
- otherwise note unavailable and rely on RSS only.

Time accounting:

- use `crate::instant::FzInstant` everywhere;
- run each `(mode, rows)` combination `--repeats` times after `--warmup`
  warmups; emit min, median, max;
- run baseline and spike in **separate processes** to avoid allocator state
  bleed.

Correctness check:

- after each `(mode, rows)` run, read `B1`, `B5`, `B100`, `B(rows/2)`,
  `B(rows)`. Each must equal `A1 * 2 = 2`.
- additionally, do a full scan: every `B[i]` value for i in `1..=rows` must
  equal `2`. Record `all_results_match`.
- if any spike result does not match baseline within strict equality for
  numeric values, mark the spike RUN as **CORRECTNESS_FAIL** and continue.
  Do not silently accept divergence.

Sweep target sizes:

```
rows ∈ {1000, 10000, 100000}
modes ∈ {baseline, spike}
```

So six rows×modes combinations. Each gets `--repeats` measured runs.

## 7. Where artifacts live

```
target/fpx-bridge-allocation/<head>/
  baseline_1000.json
  baseline_10000.json
  baseline_100000.json
  spike_1000.json
  spike_10000.json
  spike_100000.json
  build.stdout.log
  build.stderr.log
  notes.md
```

Where `<head>` is the implementation branch HEAD short hash.

## 8. Report

Author the report at:

```
docs/design/formula-plane/dispatch/fpx-bridge-allocation-spike-report.md
```

The report must:

- explain spike scope verbatim from this plan;
- include a results table `mode × rows` for `ingest_ms_median`,
  `full_eval_ms_median`, `rss_peak_mb`, `graph_vertex_count`,
  `graph_edge_count`, `ast_root_count`;
- include compact ratio: `baseline / spike` for each measured metric and each
  size;
- include `all_results_match` per run;
- include a "What this does not prove" section explicitly listing the
  correctness work intentionally skipped (item 5);
- record the implementation branch name, HEAD commit, and the discard
  recommendation;
- end with one of the following verdicts:
  - `BRIDGE_VALIDATED` — wins are present and large enough to justify the
    correctness work;
  - `BRIDGE_INCONCLUSIVE` — wins are mixed; needs follow-up before decision;
  - `BRIDGE_INVALIDATED` — wins are absent or marginal; discard the bridge
    approach as the primary lever.
- the verdict is recommendation only. Final decision belongs to the human
  reviewer.

Suggested heuristic for verdict (the report should state and apply it):

- `BRIDGE_VALIDATED` if at `rows=100000`, both:
  - `spike full_eval_ms_median <= 0.50 * baseline full_eval_ms_median`, and
  - `spike rss_peak_mb <= 0.60 * baseline rss_peak_mb`;
- `BRIDGE_INVALIDATED` if at `rows=100000`, both:
  - `spike full_eval_ms_median >= 0.85 * baseline full_eval_ms_median`, and
  - `spike rss_peak_mb >= 0.85 * baseline rss_peak_mb`;
- `BRIDGE_INCONCLUSIVE` otherwise.

These thresholds are deliberately wide because this is a single fixture with
no shared-formula loader; we are looking for an order-of-magnitude signal,
not a small percentage.

## 9. Branch and merge policy

- Plan commit lives on `formula-plane/bridge` and IS preserved.
- Implementation lives on a fresh worktree, branch
  `formula-plane/spike-allocation-XXXX` off the current
  `formula-plane/bridge` HEAD.
- Implementation branch must NOT be merged into `formula-plane/bridge` or
  `main`.
- Implementation branch is expected to be discarded once the report and
  artifacts are committed back to `formula-plane/bridge`.
- Only the report and the raw artifact directory get merged back to
  `formula-plane/bridge`. The actual `spike_run_ingest.rs` and the
  `fpx_bridge_spike` feature are discarded.

To make this concrete, the implementer should:

1. work on `formula-plane/spike-allocation-XXXX`;
2. produce `target/fpx-bridge-allocation/<head>/...` artifacts;
3. write the report and copy artifacts and report into a single commit on a
   separate small branch `formula-plane/spike-allocation-report-XXXX`
   branched off `formula-plane/bridge`, containing **only**:
   - the report markdown;
   - the artifacts directory (or a tarball of it);
   - no source changes;
4. the human reviewer fast-forwards `formula-plane/bridge` to that
   report-only branch.

## 10. Validation

The implementer must, on the implementation branch:

```bash
timeout 10m cargo fmt --all -- --check
timeout 15m cargo test -p formualizer-eval --quiet
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
timeout 15m cargo test -p formualizer-eval --features fpx_bridge_spike --quiet
timeout 30m target/release/spike-bridge-allocation --mode baseline --rows 100000 --json-out target/fpx-bridge-allocation/<head>/baseline_100000.json
timeout 30m target/release/spike-bridge-allocation --mode spike    --rows 100000 --json-out target/fpx-bridge-allocation/<head>/spike_100000.json
... etc for all six combinations ...
```

All bounded. No long fuzz, no soak, no native_best harness, no cross-engine
comparison.

If any required test or build step fails, halt and report; do not broaden
scope to fix unrelated issues.

## 11. Stop conditions

Halt and write a partial report instead of broadening if:

- adding the spike ingest path would require touching `DependencyGraph`
  internals beyond a single localized hook (e.g., adding a new optional
  helper);
- the value-plane write path cannot be reused without modifying production
  read semantics;
- correctness fails on the spot-check (full result scan must match);
- jemalloc/RSS measurement is unavailable on the runner;
- any path requires modifying a public API surface;
- the spike begins to require dirty/volatile/structural support to even
  function for the load-eval-read measurement.

In any of those cases, stop, write what you have, document what blocked,
and propose a smaller follow-up spike rather than expanding this one.

## 12. Strict non-goals

This spike must NOT:

- modify or remove existing FormulaPlane code (`formula_plane/*.rs`,
  `function_contract.rs`, builtin opt-ins);
- change `function_registry`, `Function` trait, or any builtin function;
- modify ingest_builder's existing public path;
- modify `evaluate_all` or scheduler behavior on the existing path;
- introduce a public default-on feature;
- couple the spike path to FormulaPlane diagnostics or scanner JSON;
- claim runtime wins anywhere outside the spike report;
- update `REPHASE_PLAN.md` or other forward-looking docs;
- merge anything into `formula-plane/bridge` other than the report-only
  follow-up branch.

## 13. Post-spike

Independent of verdict, the human reviewer will:

- decide whether to proceed with bridge correctness work;
- decide whether to redirect optimization effort to other levers (loader
  rewrite, hashing, dependency planner, dirty propagation reform on the
  existing graph);
- decide whether to schedule a parallel "shared-formula loader" track.

The spike report itself does not approve or schedule any of those.
