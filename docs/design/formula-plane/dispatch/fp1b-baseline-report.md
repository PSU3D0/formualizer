# FP1.B baseline report

Date: 2026-04-29  
Branch: `formula-plane/bridge`  
Raw artifact directory: `target/fp1b-baseline/6322615`  
Implementation commit: `f867cad` (`feat(formula-plane): add fp1b runner observability`)

## Scope

FP1.B adds bounded, read-only observability before FP2. It does not alter workbook defaults, formula parsing behavior in production paths, dependency graph construction semantics, or evaluation semantics.

The governed runner now supports `--backend umya|calamine` with the same scenario metadata, correctness checks, full evaluation, and incremental operation loop for both backends. The backend comparison in this report uses the six FP1 synthetic scenarios in `native_best` mode.

## Commands run

Validation:

```bash
RUN_DIR=target/fp1b-baseline/$(git rev-parse --short HEAD)
mkdir -p "$RUN_DIR"
timeout 60s uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-suite | tee "$RUN_DIR/validate-suite.log"
timeout 60s uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-plans | tee "$RUN_DIR/validate-plans.log"
```

Build:

```bash
timeout 10m cargo build --release -p formualizer-bench-core --features formualizer_runner --bin run-formualizer-native --bin scan-formula-templates \
  > "$RUN_DIR/build-release.stdout.log" \
  2> "$RUN_DIR/build-release.stderr.log"
```

Parser-backed formula template scan:

```bash
for s in headline_100k_single_edit chain_100k fanout_100k inc_cross_sheet_mesh_3x25k agg_countifs_multi_criteria_100k agg_mixed_rollup_grid_2k_reports; do
  timeout 2m target/release/scan-formula-templates \
    --scenarios benchmarks/scenarios.yaml \
    --scenario "$s" \
    --root . \
    > "$RUN_DIR/$s.formula-templates.json"
done
```

Governed runner timing:

```bash
for backend in umya calamine; do
  for s in headline_100k_single_edit chain_100k fanout_100k inc_cross_sheet_mesh_3x25k agg_countifs_multi_criteria_100k agg_mixed_rollup_grid_2k_reports; do
    /usr/bin/time -v timeout 15m env FZ_DEBUG_LOAD=1 target/release/run-formualizer-native \
      --scenarios benchmarks/scenarios.yaml \
      --scenario "$s" \
      --root . \
      --mode native_best \
      --backend "$backend" \
      > "$RUN_DIR/$s.$backend.native_best.json" \
      2> "$RUN_DIR/$s.$backend.native_best.stderr.log"
  done
done
```

Post-change validation:

```bash
timeout 10m cargo fmt --all -- --check
timeout 10m cargo test -p formualizer-common --quiet
timeout 15m cargo test -p formualizer-eval --quiet
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
timeout 15m cargo test -p formualizer-workbook --features umya,calamine --quiet
```

Result: passed.

## Governed backend timing

`load_ms` remains the existing combined runner metric. `open_read_ms` and `workbook_ingest_ms` are additive `metrics.extra` fields, with `load_ms ~= open_read_ms + workbook_ingest_ms`. RSS is parsed from `/usr/bin/time -v` stderr.

| Scenario | Backend | Load ms | Open/read ms | Workbook ingest ms | Full eval ms | Incremental ms | RSS MB | Adapter formulas observed | Adapter values observed | Value slots handed | Formulas handed |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `agg_countifs_multi_criteria_100k` | umya | 2163.7 | 959.5 | 1204.2 | 440.4 | 9.979 | 322.0 | 1000 | 505011 | 506011 | 1000 |
| `agg_countifs_multi_criteria_100k` | calamine | 554.7 | 0.2 | 554.4 | 433.7 | 10.874 | 85.8 | 1000 | 505011 | 506011 | 1000 |
| `agg_mixed_rollup_grid_2k_reports` | umya | 387.2 | 154.3 | 233.0 | 404.5 | 1.321 | 77.9 | 12000 | 62015 | 74015 | 12000 |
| `agg_mixed_rollup_grid_2k_reports` | calamine | 211.6 | 0.3 | 211.3 | 378.9 | 1.483 | 55.2 | 12000 | 62015 | 74015 | 12000 |
| `chain_100k` | umya | 981.4 | 260.9 | 720.5 | 79.9 | 64.064 | 208.7 | 99999 | 1 | 100000 | 99999 |
| `chain_100k` | calamine | 906.7 | 0.2 | 906.5 | 80.7 | 64.100 | 213.0 | 99999 | 1 | 100000 | 99999 |
| `fanout_100k` | umya | 962.8 | 269.4 | 693.5 | 60.4 | 40.581 | 190.2 | 100000 | 1 | 200000 | 100000 |
| `fanout_100k` | calamine | 850.5 | 0.2 | 850.3 | 55.4 | 36.345 | 207.6 | 100000 | 1 | 200000 | 100000 |
| `headline_100k_single_edit` | umya | 1574.4 | 424.9 | 1149.5 | 93.1 | 18.274 | 254.1 | 100001 | 100000 | 300000 | 100001 |
| `headline_100k_single_edit` | calamine | 1210.0 | 0.2 | 1209.8 | 111.1 | 23.494 | 248.9 | 100001 | 100000 | 300000 | 100001 |
| `inc_cross_sheet_mesh_3x25k` | umya | 814.0 | 263.1 | 550.8 | 28.7 | 0.016 | 141.9 | 50000 | 75000 | 125000 | 50000 |
| `inc_cross_sheet_mesh_3x25k` | calamine | 728.5 | 0.2 | 728.3 | 30.3 | 0.027 | 127.5 | 50000 | 75000 | 125000 | 50000 |

### Timing interpretation notes

- Calamine `open_read_ms` is near zero because `open_workbook` is mostly lazy for these XLSX files; actual sheet IO/materialization appears in `workbook_ingest_ms`.
- Umya `open_read_ms` includes eager workbook DOM read plus the existing table-header side scan.
- `adapter_value_slots_handed_to_engine` is dense sheet slot materialization into Arrow ingest, not just non-empty cells.
- `adapter_value_cells_observed` counts backend-observed non-empty values before dense handoff.
- `adapter_formula_cells_observed` and `adapter_formula_cells_handed_to_engine` matched graph formula roots for these six scenarios.

## Formula template scan

`scan-formula-templates` reads XLSX OOXML directly, extracts formula cells and raw shared-formula tags, parses formula text with `formualizer_parse`, and emits conservative canonical template IDs. Relative A1 references are normalized against each formula anchor where the parser exposes cell/range references. Unsupported/dynamic labels are emitted instead of claiming full coverage.

| Scenario | Formula cells | Templates | Repeated templates | Repeated-template cells | Column runs | Row runs | Holes | Exceptions | Raw shared `<f t=shared>` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `agg_countifs_multi_criteria_100k` | 1000 | 1 | 1 | 1000 | 1 | 0 | 0 | 0 | 0 |
| `agg_mixed_rollup_grid_2k_reports` | 12000 | 5 | 5 | 12000 | 5 | 0 | 0 | 0 | 0 |
| `chain_100k` | 99999 | 1 | 1 | 99999 | 1 | 0 | 0 | 0 | 0 |
| `fanout_100k` | 100000 | 1 | 1 | 100000 | 1 | 0 | 0 | 0 | 0 |
| `headline_100k_single_edit` | 100001 | 2 | 1 | 100000 | 1 | 0 | 0 | 0 | 0 |
| `inc_cross_sheet_mesh_3x25k` | 50000 | 2 | 2 | 50000 | 2 | 0 | 0 | 0 | 0 |

### Template interpretation notes

- Generated corpus formulas are emitted as expanded normal formulas; no raw OOXML shared-formula tags are present in this six-scenario corpus.
- Column runs dominate because the generated repeated formulas are vertical template runs in these scenarios.
- The scanner canonicalizes parser-visible cell/range references and labels named, dynamic, external, 3D, structured, and parse-error cases conservatively.
- Template IDs are stable FNV-1a hashes over canonical AST text plus labels; they are bench-only IDs, not public API.

## Code surfaces added

- `run-formualizer-native --backend umya|calamine` with governed scenario operation handling and correctness for both backends.
- `metrics.extra.backend`, `metrics.extra.open_read_ms`, and `metrics.extra.workbook_ingest_ms` while retaining `metrics.load_ms`.
- `AdapterLoadStats` plus `Workbook::from_reader_with_adapter_stats` for read-only bench counters.
- Umya/Calamine adapter counters for formula cells observed, formula cells handed to engine, non-empty values observed, and dense value slots handed to engine.
- `scan-formula-templates` bench binary for read-only OOXML formula/template/shared-tag visibility.

## Remaining gaps

| Gap | Impact | Recommended owner |
|---|---|---|
| Shared-formula adapter counters remain `None` | Runner cannot yet compare adapter expansion/preservation of shared formula tags | FP2 loader/span plumbing or a targeted raw-OOXML counter thread-through |
| Scanner canonicalization is conservative, not exhaustive Excel semantics | Dynamic arrays, structured refs, external refs, and unsupported parser cases are labeled rather than normalized | FP2 template taxonomy work |
| Calamine open/read split reflects lazy open semantics | Open/read is not directly comparable to Umya eager DOM read | Keep both `open_read_ms` and `workbook_ingest_ms` in reports |
| Incremental runner op still uses current `native_best` behavior | It is correctness-preserving but not a new minimal dirty-frontier scheduler | FP2 scheduler/span evaluation |
| Dense value slot handoff remains coarse | It measures current Arrow materialization, not future sparse/span materialization | FP2 materialization counters |

## Status

**PASS for FP1.B scope.** The baseline now separates load phases, records backend mode, runs governed Umya and Calamine scenarios with correctness checks, exposes low-risk adapter handoff counters, and provides parser-backed bench-only formula template/template-run/raw-shared visibility for the six FP1 synthetic scenarios.
