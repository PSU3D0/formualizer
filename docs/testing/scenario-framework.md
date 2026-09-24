# Scenario framework

One scenario definition serves behavioral tests, edge cases, Off/Auth parity, structural goldens, trace recording and benchmark consumption. Scenarios are plain data in `formualizer-testkit`; the engine contributes structured `fz.*` trace events behind its `tracing` feature. Nothing here changes evaluation behavior, and everything is compiled out of default builds.

## Layers

| Layer | Module | Purpose |
|---|---|---|
| Shape | `testkit::shape` | Engine-free workbook description: sheets, value regions, formula families, boundaries, gaps, roles. Scale-relative so one shape renders at any size. |
| Materialize | `testkit::materialize` | Turns a Shape into an artifact: `Xlsx` (written by umya, plus `patch_part` for XML injection) or `WorkbookApi` (three distinct routes: per-cell, batch `set_values`/`set_formulas`, `write_range`). At run time an xlsx artifact is loaded by either the Calamine or the Umya reader (`run::XlsxReader`); these are separate provenances because they take different ingest routes. `EngineDirect` is declared and unsupported. |
| Scenario | `testkit::scenario`, `testkit::witnesses` | `ScenarioSpec` = shape + `Script` + per-step `Expect` + tags + modes + size. The fourteen pre-M4 witnesses live in `witnesses`. |
| Run | `testkit::run` | `run(spec, mode, size, materializer, recorder)`; the `Recorder` tracing layer; structural checks. |
| Runner | `tests/scenarios.rs` | libtest-mimic binary: one named test per `(spec, mode)`, filters, `--record`, `--rung`. |
| Pins | `testkit::pins` | Program 1 M0 behavioral pins (`pin.*`): FORM-000117 structural undo, sheet delete and re-add, the preparation-failure table per route, and the M3 span-ownership regressions as value-level scenarios with first-principles column models. Registered at the default rung. |
| Adapter | `bench-core::scenarios::unified_registry` | Exposes the 87 legacy `Scenario` impls as specs beside the witnesses; `probe-unified-registry` consumes the union. |

Feature gating: testkit `default = ["xlsx"]`; `workbook` adds the API materializer, scenarios, runner and Recorder (and turns on `formualizer-eval/tracing`). `--no-default-features` keeps `shape` engine-free, which is how `formualizer-eval` dev-tests use it.

## Writing a shape

```rust
use formualizer_testkit::shape::{Cell, Extent, Range, Role, Scale, Shape, r#gen};

let shape = Shape::new().scale(Scale::rows(4096)).sheet("S", |s| {
    s.values(Range::col("A", Extent::Abs(1)..=Extent::Rows), r#gen::constant(Cell::number(1.0)));
    s.family("recur", Range::col("B", Extent::Abs(1)..=Extent::Rows), "=C{r-1}+A{r}")
        .boundary("B1", "=A1");
    s.family("half", Range::col("C", Extent::Abs(1)..=Extent::Rows), "=B{r}/2");
    s.role("A1", Role::PrimaryInput);
    s.role("recur", Role::PrimaryFamily);
});
```

Placeholders: `{r}`, `{c}` (column letter), `{r+k}`/`{r-k}`, `{c+k}`/`{c-k}`, `{n}` rows, `{m}` cols, `{end}` (last row for column and rect families, last column letter for row families). Out-of-grid results are a materialization error, never a clamp. Extents: `Abs`, `Rows`, `Cols`, `RowsMinus(k)`, `ColsMinus(k)`, `RowsPlus(k)`, `ColsPlus(k)`. Combinators: `boundary`, `except`, `override_cell`, `gap_every(k)`, `blocks(size, gap)`. Roles let scripts pick edit targets without per-scenario code.

## Scripts and expectations

`Script::standard()` is load, prepare, evaluate, no-op, set primary input, override interior, formula-to-literal, undo, redo, insert rows, delete rows. `witnesses::witness_script()` evaluates after every mutation so value models can be checked at settled states; use it when you attach oracles.

`Expect` per step index: `Value`, `Oracle(closure)`, `NoErrors(sheet)`, `Parity` (Off versus Auth for every rendered cell), `Structure(StructureExpect)`. Structural fields come from two sources: `EngineBaselineStats` (`active_spans`, `arena_nodes`, `graph_vertices`, `graph_edges`) and recorded `fz.*` events counted cumulatively through the step (`placed_families`, `rejected_families`, `demotions` with optional reason, `topology_outcome`). A `StructureExpect` may be scoped by `mode` and `provenance`; unscoped expectations apply everywhere. Event-derived fields require a Recorder and fail loudly without one.

`expected_failures` mark a tracked engine defect for a mode and, optionally, one provenance, with a required `FailureFingerprint`: the failing step, whether its action or an expectation failed, and the complete message (which names the cell and the observed value, or the error text). Only a run that fails with exactly that fingerprint is reported as KNOWN (printed to stderr with the reason). A matching run that fails at another step or with another message fails with "expected failure did not match", so a marker cannot hide an unrelated regression; a matching run that passes fails with "expected failure did not occur", so a marker is removed when its defect is fixed. Take the fingerprint from the observed KNOWN output and never weaken an oracle to make a step pass. Legacy corpus markers carry the same fingerprint as `ExpectedFailure::runner_failure`.

## Recorder

`Recorder` is a `tracing_subscriber::Layer` that buckets `fz.*` events and span names by step. Recorded runs are serialized process-wide because tracing's callsite-interest cache is global; a concurrent unrecorded run can otherwise register a callsite as never-interested. The runner refuses to record when `config.eval.enable_parallel` is true.

## Running

```bash
# all scenarios at the default rung (256), both modes, both provenances
cargo test -p formualizer-testkit --features workbook --test scenarios
# by name, by mode, with recording
cargo test -p formualizer-testkit --features workbook --test scenarios -- witness.coupled --mode authoritative --record
# by tag; dimensions ANDed, values within a dimension ORed
cargo test -p formualizer-testkit --features workbook --test scenarios -- --tag-filter 'family:coupled,independent provenance:workbook-api'
# provenance values: workbook-api, xlsx-calamine, xlsx-umya
FZ_SCENARIO_FILTER='engine:demoted' cargo test -p formualizer-testkit --features workbook --test scenarios
# ladder
cargo test -p formualizer-testkit --features workbook --test scenarios -- --rung 100 witness
cargo test -p formualizer-testkit --features workbook --test scenarios -- --rung all --list
# unified registry (legacy corpus + witnesses) through the runner
cargo run -p formualizer-bench-core --features formualizer_runner --bin probe-unified-registry -- --filter 'family:coupled' --mode authoritative
```

Tag dimensions: `family`, `orientation`, `provenance`, `lifecycle`, `engine`, `purpose`, `size`. `provenance` is a run-time axis (which materializer), not a property baked into a spec.

## Ladder

Rungs: Small 16, 100, 256; Medium 1,000, 4,096; Large 16,384, 100,000; Nightly 1,000,000. Witness value models and structural goldens are bound to their row count, so `--rung` selects a registry rather than overriding a size. Large and Nightly run only when requested explicitly or with `FZ_SCENARIO_NIGHTLY=1`. The 14 x 8 product is full coverage by design; it is small enough that a pairwise reduction would only hide combinations.

Goldens change with size because of the 100-cell promotion threshold: independent promotes at 100 rows but not at 16; blocks at 100 rows is a single 99-cell block and does not promote; coupled at 100 rows places only the C column (the B family is 99 cells plus a boundary) and then demotes it against the legacy B chain. Nightly rungs carry no structural goldens until measured.

## Adding a witness

Add a `Kind` variant in `witnesses.rs`, its shape, its first-principles value model under the lifecycle states (`a1`, interior override, inserted row), its rung-aware structural observation, and tags. It then appears at every rung and in the unified registry. Record the observation with `--record` before encoding it; do not copy the 256-row golden to other rungs.

## Legacy corpus

`bench-core::scenarios::adapt_scenario` maps a legacy `Scenario` onto a spec: `build_fixture` becomes an xlsx fixture source, edit cycles become custom steps followed by evaluation, invariants become expectations, `expected_to_fail_under` becomes `expected_failures`. The 87 legacy scenarios are not rewritten; they can be migrated to shapes one at a time.

## Facts the goldens currently encode

- **FORM-000128.** A family loaded through the Calamine reader reaches the engine as ordinary source events and is never offered for span placement in Auth mode; the same file through the Umya reader, or the same cells through the API, form a span. Every witness carries a `provenance:xlsx-calamine` golden of zero placements and a `provenance:xlsx-umya` golden equal to the API golden, so a fix will show as a golden change.
- **Orientation.** The same independent family along a row is singletons through the API because ordinary grouping is keyed by column. The `horizontal` witness encodes zero placements.
- **Coupled columns.** Two acyclic coupled families are placed and then both demoted as `CycleMember` by the producer-granular scheduler. The `coupled` witness encodes two placements, two demotions, zero active spans.
