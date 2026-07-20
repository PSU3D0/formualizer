# C6 Native Target Calibration

Status: Phase-A measurement harness; benchmark and documentation changes only

The harness compares public native evaluation paths in fresh processes without changing engine semantics, authority, budgets, defaults, or algorithms. The original scalar CLI remains the default: 50,000 formulas, seven samples, the `full,cells,plan,sheetport` paths, and the tiny/medium/full 12-case matrix.

Schema v3 adds a `targets` path and these fixture families:

| family | supported paths | selector set |
|---|---|---|
| `scalar` | `full`, `cells`, `plan`, `sheetport` | original independent scalar terminals |
| `cross-sheet` | `full`, `cells`, `targets`, `plan`, `sheetport` | one alternating two-sheet chain terminal |
| `names` | `full`, `targets`, `plan` | workbook- and sheet-scoped names |
| `names` | `sheetport` | workbook-scoped name only |
| `layout` | `full`, `sheetport` | bounded table layout plus a scalar breadth dependency |
| `native-table` | `full`, `targets`, `plan`, `sheetport` | headers, body, and totals |
| `dynamic` | `full`, `cells`, `targets`, `plan` | numeric runtime-text result and explicit `#REF!` result |
| `dynamic` | `sheetport` | numeric runtime-text result only |

The matrix is hard-coded and tested. Unsupported combinations fail instead of being represented through a less specific API. Native breadth families support only `--scope full`; this is the single target scope used at 50k.

## Fixtures and Oracles

Each family gets one deterministic, immutable XLSX generated before samples. The matrix records one SHA-256 per family and every randomized sample reads the same family file in a fresh process. Every fixture contains exactly the requested formula count and an eight-formula branch that is prepared, evaluated, and dirtied before the measured path. The editable value is local to the selected formula package (`ChainA!A1` for cross-sheet and `Chain!A1` otherwise), matching the scalar control and allowing retained plans to remain valid across ordinary value edits.

Every breadth family also has a `full` control. It uses `prepare_graph_all` and `evaluate_all`, then the same typed output reader as its targeted paths. Full controls require zero staged formulas and the exact family-specific post-evaluation dirty count, proving the unrelated staged and dirty branches were broadly consumed rather than retained. Focused tests cover every family full control with three warm edits and every breadth full control with zero edits, including the long-tier repeat count.

The full-control dirty residual is independently derived from graph behavior rather than fitted per family. `Dirty!A1`, an edited non-formula source, contributes one vertex. If at least one ordinary warm edit runs, `Chain!A1` (or `ChainA!A1`) contributes one more, regardless of repeat count. The two dynamic `INDIRECT` formulas are volatile and are therefore re-dirtied after every evaluation. `Engine::define_table` registers the `C6Table` metadata vertex as a dependent of its table range; after a warm edit changes the table's formula values, propagation dirties that metadata vertex, which is not a formula evaluation vertex and therefore remains dirty. It contributes zero without a warm edit and one with any positive repeat count. The public baseline reports only the aggregate dirty count and cannot expose that vertex identity; the table gate is therefore the narrow aggregate count implied by this engine behavior, and its structural oracle discloses that limitation. No other family has a persistent component.

- `cross-sheet` alternates every recurrence edge between `ChainA` and `ChainB`. Its initial and edited terminal values use the closed form `B_n = B_1*r^(n-1) + 0.00001*(r^(n-1)-1)/(r-1)`. Eight unrelated formulas must remain staged and the dirty branch must remain dirty.
- `names` registers `WorkbookOutput` and sheet-scoped `Names!SheetOutput` through the public workbook name API after load. Initial targets point at two formula outputs. All ordinary warm value edits run first against those bindings with retained-plan reuse. A separate post-warm probe then moves both bindings to independent rebound outputs. A retained plan must reject that symbol revision with exactly `PlanStale(Symbols)`; one rebuilt plan must return the rebound oracle. The probe intentionally performs no subsequent value edit: harness development exposed a propagation concern for value edits after a name-binding mutation, so that combination is a follow-up and is not measured here. SheetPort is intentionally limited to `WorkbookOutput`, because its current name selector has no sheet-scope field.
- `layout` uses the actual SheetPort `first_blank_row` layout resolver with `max_scan_rows: 8`. The returned structural oracle is exactly `Layout!A2:D2`, with row 3 as the blank guard. SheetPort conservatively targets the complete A2:D9 scan envelope before resolving output bounds, so below-guard `Layout!C4` is prepared and must evaluate to 999 even though it is excluded from the returned table. A second formula at `Layout!C10` is below the envelope, but staged discovery commits the whole Layout-sheet source package once C4 is selected; after that commit C10 is not staged, remains Empty, and is not scheduled. Exact selected work is the long chain plus C4 and C10, while exactly six formulas on the separate Retained sheet stay staged. The one-row result and envelope extending past row 3 prove that blank termination, not envelope exhaustion, controls returned output. These gates distinguish output termination, conservative preparation demand, package commit, and scheduled evaluation. Batch output and restored input are checked, and restoration latency is separated with the public progress callback.
- `native-table` has header, body, and totals rows with string, integer, number, and date values. Direct paths use three `EvaluationTarget::Table` selections; SheetPort uses three full-v0 native table selectors. Typed values are represented as tagged v3 values rather than compared through debug or JSON spellings. The number/date outputs have independent analytical oracles for every edit.
- `dynamic` targets `INDIRECT("Chain!A1")` from the `Dynamic` sheet and a long chain. Initial preparation must report workbook scope and the exact widening mask `DynamicReference | RuntimeTextReference == 0b11`; both the expected and observed masks are recorded structurally, and extra bits fail the gate. Direct paths also target `INDIRECT("Missing!A1")` and require a typed `#REF!` literal on every evaluation.

The family gates require exact load/setup formula counts, exact selected-staging or widened-workbook counts, independent initial/warm typed or numeric outputs, applicable staged/dirty retention, name staleness/rebuild, layout bounds/guard retention, native-table areas/types, and SheetPort batch restoration. Matrix summaries include only `status: ok` children and include all family gates in the displayed gate result.

## Verified API Boundaries

The Phase-A design was checked against current public APIs before implementation. Four boundaries constrain the benchmark:

1. Calamine currently reports no native table metadata. The fixture carries table cells, while each child registers the same `C6Table` metadata through the public engine table API. This `public_native_table_registration` phase is timed and summarized as selector setup for all table paths; it is never labeled evaluation.
2. SheetPort name selectors currently resolve with `scope_sheet: None`. Its names case therefore selects only the workbook-scoped name. Direct `targets` and `plan` cases cover both workbook and sheet scopes; parity keys include the selector set so unlike outputs are never compared.
3. SheetPort validates outputs against the manifest schema and does not provide an unconstrained error-valued scalar schema. The dynamic SheetPort case selects the numeric result only. `cells`, `targets`, and `plan` preserve and compare the explicit `#REF!` value. This unsupported SheetPort selector/output combination is not forced into the API.
4. SheetPort does not expose resolved layout bounds as a public return value. The harness proves returned bounds `A2:D2` structurally through the one-row/four-column typed table and `max_scan_rows: 8` extending beyond blank row 3. Public planning first targets conservative A2:D9, then staged source-package fallback commits both Layout formulas. Exact C4=999, C10=Empty-but-committed, selected work, and six separately staged Retained formulas distinguish preparation and scheduling from blank-terminated output resolution.

Family `plan` cases build once and reuse the same revision-bound plan across ordinary local value edits. Any value-only `PlanStale` is a failed sample and gate. When `--warm-repeats 0`, no value edit was requested, so the reuse gate is omitted as not applicable; the names staleness probe still runs and must pass. The names case completes all requested ordinary `Chain!A1` warm edits with retained-plan reuse, then changes both name bindings in a separate post-warm probe. It proves the old retained plan returns exactly `PlanStale(Symbols)`, rebuilds once for that structural mutation, and evaluates the rebound outputs. The binding/stale-probe/rebuild work is recorded in `name_binding_probe`, not in cold setup or ordinary warm distributions. It deliberately stops after the rebound evaluation and does not measure a post-binding value edit because of the propagation concern described above.

## Commands

Focused validation:

```bash
cargo test -p formualizer-bench-core --features c6_calibration \
  --lib \
  --bin probe-c6-target-locality \
  --bin probe-c6-target-locality-matrix
cargo check -p formualizer-bench-core --features c6_calibration --bins
cargo clippy -p formualizer-bench-core --features c6_calibration --bins --tests -- -D warnings
cargo fmt --all -- --check
```

Required all-family 1k CI smoke:

```bash
cargo run --release -p formualizer-bench-core --features c6_calibration \
  --bin probe-c6-target-locality-matrix -- \
  --tier all-family-smoke \
  --output-dir target/c6-calibration/all-family-1k
```

Required seven-sample 50k breadth matrix (run after smoke):

```bash
cargo run --release -p formualizer-bench-core --features c6_calibration \
  --bin probe-c6-target-locality-matrix -- \
  --tier breadth50k \
  --output-dir target/c6-calibration/native-breadth-50k
```

Required original 12-case, seven-sample scalar 250k matrix. The tier enforces at least 1,800 seconds per child:

```bash
cargo run --release -p formualizer-bench-core --features c6_calibration \
  --bin probe-c6-target-locality-matrix -- \
  --tier scalar250k \
  --output-dir target/c6-calibration/scalar-250k
```

`--tier largest-safe` is deliberately rejected in Phase A. Without `--tier`, the existing scalar defaults and explicit `--formulas`, `--samples`, `--paths`, and `--scopes` behavior remain available. Manual native runs must choose a supported family/path and `--scope full`.

## Schema v3 and Output

`matrix-raw.json` retains the v1 scalar identity, formula, fixture, job, result, summary, and parity fields. Additive v3 fields include:

- matrix `families` and `fixtures`, where every fixture entry has `family`, `path`, and `sha256`;
- job and summary `family`, plus summary `selector_set`;
- child `family`, `path_schema_version: 3`, and `selector_set`;
- child tagged `typed_outputs` and `typed_expected_outputs` (`string`, `integer`, `number`, `date`, `boolean`, `empty`, or `error`);
- child `family_gates`, `structural_oracles`, and `plan_stale_reason`;
- phases `selector_setup` for initial public name/table registration and `name_binding_probe` for the post-warm name mutation/staleness contract.

The legacy `fixture_path`, `fixture_sha256`, debug `outputs`, numeric analytical fields, and scalar fields remain populated for backward compatibility. For a multi-family run, the legacy fixture fields identify the first recorded family; `fixtures` is authoritative. For schema-v3 breadth children, legacy `oracle_within_one_percent` carries the exact family-locality/full-control boolean rather than a one-percent approximation; `exact_locality_counts_passed` is the authoritative field.

`matrix-summary.md` reports median / nearest-rank p95 / MAD / maximum, successful count, selector setup, plan preparation, evaluations, batch restoration, RSS, selected/prepared work, gate status, and parity by family/scope/selector set. With seven child samples, per-child p95 is the maximum. Warm distributions pool all warm calls.

The runner builds fixtures outside timed samples, randomizes jobs deterministically, invokes every sample in a fresh process, uses `/usr/bin/time -v` where available, drains stdout/stderr concurrently, and kills the whole process group on timeout. Timeout and child failures remain typed results; raw JSON, stderr, external-time logs, and all completed samples are written before the matrix returns failure.

## Interpretation Limits

- Timing `includes` arrays are authoritative. `cells` and SheetPort APIs combine stages that public calls do not expose separately.
- Initial table/name registration is selector setup, not evaluation. Ordinary warm value mutations are in the authoritative edit phase, while the post-warm name mutation is in `name_binding_probe`; layout resolution occurs inside the public SheetPort call.
- Family plans are retained across value-only edits. Names perform exactly one post-warm rebuild after the deliberate binding change yields `PlanStale(Symbols)`; `name_binding_probe` includes the stale probe, rebuild, evaluation, and rebound read.
- Name and dynamic SheetPort cases intentionally have narrower selector sets than direct paths; parity never compares different selector sets.
- RSS/HWM and existing request-ledger counters are reported because there is no stable allocator-specific counter.
- This tranche makes no recommendation about budgets, defaults, authority, or algorithms and does not cover historical binaries, largest-safe, WASM/no-disk, or non-default native modes.
