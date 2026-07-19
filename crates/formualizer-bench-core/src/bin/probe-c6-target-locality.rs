#[cfg(feature = "c6_calibration")]
use std::collections::BTreeMap;
#[cfg(feature = "c6_calibration")]
use std::path::PathBuf;
#[cfg(feature = "c6_calibration")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "c6_calibration")]
use std::time::Instant;

#[cfg(feature = "c6_calibration")]
use anyhow::{Context, Result};
#[cfg(feature = "c6_calibration")]
use clap::{Parser, Subcommand};
#[cfg(feature = "c6_calibration")]
use formualizer_bench_core::c6_calibration::{
    CalibrationPath, ChildReport, EngineTelemetry, FixtureShape, GraphSnapshot, PhaseTimings,
    TargetScope, TimedPhase, allowed_oracle, analytical_expected_outputs, checksum_values,
    generate_fixture, manifest_yaml, sha256_file,
};
#[cfg(feature = "c6_calibration")]
use formualizer_common::LiteralValue;
#[cfg(feature = "c6_calibration")]
use formualizer_sheetport::{
    BatchInput, BatchOptions, EvalOptions, InputUpdate, PortValue, SheetPort,
};
#[cfg(feature = "c6_calibration")]
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
#[cfg(feature = "c6_calibration")]
use sheetport_spec::Manifest;

#[cfg(not(feature = "c6_calibration"))]
fn main() {
    eprintln!("This binary requires feature `c6_calibration`");
    std::process::exit(2);
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Parser)]
#[command(about = "C6 target-locality fixture generator and fresh-process child probe")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Subcommand)]
enum Command {
    Generate {
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long)]
        formulas: u32,
    },
    Sample {
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long)]
        formulas: u32,
        #[arg(long, value_enum)]
        path: CalibrationPath,
        #[arg(long, value_enum)]
        scope: TargetScope,
        #[arg(long, default_value_t = 3)]
        warm_repeats: usize,
    },
}

#[cfg(feature = "c6_calibration")]
fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Generate { fixture, formulas } => {
            let shape = generate_fixture(&fixture, formulas)?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "fixture": fixture,
                    "shape": shape,
                    "sha256": sha256_file(&fixture)?,
                }))?
            );
        }
        Command::Sample {
            fixture,
            formulas,
            path,
            scope,
            warm_repeats,
        } => println!(
            "{}",
            serde_json::to_string(&run_sample(&fixture, formulas, path, scope, warm_repeats)?)?
        ),
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

#[cfg(feature = "c6_calibration")]
fn run_sample(
    fixture: &std::path::Path,
    formulas: u32,
    path: CalibrationPath,
    scope: TargetScope,
    warm_repeats: usize,
) -> Result<ChildReport> {
    let shape = FixtureShape::new(formulas)?;
    let fixture_sha256 = sha256_file(fixture)?;
    let mut phases = PhaseTimings::default();
    let mut telemetry = BTreeMap::new();

    let load_started = Instant::now();
    let reader = CalamineAdapter::open_path(fixture).context("open fixture with Calamine")?;
    let mut workbook = Workbook::from_reader(
        reader,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .context("interactive deferred workbook load")?;
    phases.load = Some(TimedPhase::new(
        elapsed_ms(load_started),
        &["xlsx_open", "load"],
    ));
    let graph_after_load = graph_snapshot(&workbook);

    // Establish a genuinely prepared and dirty branch while leaving all other branches staged.
    // This setup is identical for every path and is reported rather than hidden in load/evaluation.
    let setup_started = Instant::now();
    workbook
        .prepare_graph_for_cells(&[("Dirty", shape.dirty_formulas, 2)])
        .context("prepare unrelated dirty branch")?;
    workbook
        .evaluate_cell("Dirty", shape.dirty_formulas, 2)
        .context("evaluate unrelated dirty branch")?;
    workbook
        .set_value("Dirty", 1, 1, LiteralValue::Number(40.0))
        .context("dirty unrelated branch")?;
    phases.unrelated_dirty_setup = Some(TimedPhase::new(
        elapsed_ms(setup_started),
        &[
            "target_resolution",
            "preparation",
            "evaluation",
            "unrelated_edit",
        ],
    ));
    let graph_after_setup = graph_snapshot(&workbook);

    let mut outputs = Vec::new();
    let path_result = match path {
        CalibrationPath::Full => run_full(
            &mut workbook,
            shape,
            scope,
            warm_repeats,
            &mut phases,
            &mut telemetry,
            &mut outputs,
        ),
        CalibrationPath::Cells => run_cells(
            &mut workbook,
            shape,
            scope,
            warm_repeats,
            &mut phases,
            &mut telemetry,
            &mut outputs,
        ),
        CalibrationPath::Plan => run_plan(
            &mut workbook,
            shape,
            scope,
            warm_repeats,
            &mut phases,
            &mut telemetry,
            &mut outputs,
        ),
        CalibrationPath::Sheetport => run_sheetport(
            &mut workbook,
            shape,
            scope,
            warm_repeats,
            &mut phases,
            &mut telemetry,
            &mut outputs,
        ),
    };
    let mut typed_error = path_result.err().map(|error| format!("{error:#}"));

    let graph_after_run = graph_snapshot(&workbook);
    let target_path = path != CalibrationPath::Full;
    let locality_scope = scope != TargetScope::Full;
    let expected_prepared = if target_path {
        let already_prepared = if scope == TargetScope::Full {
            shape.dirty_formulas
        } else {
            0
        };
        u64::from(shape.oracle(scope)) - u64::from(already_prepared)
    } else {
        u64::from(shape.formulas - shape.dirty_formulas)
    };
    let expected_staged = if target_path && locality_scope {
        u64::from(shape.formulas - shape.dirty_formulas - shape.oracle(scope))
    } else {
        0
    };
    let expected_dirty = match (target_path && locality_scope, warm_repeats > 0) {
        (true, true) => 10,
        (true, false) => 9,
        (false, true) => 2,
        (false, false) => 0,
    };
    let unrelated_staged_retained = (target_path && locality_scope)
        .then_some(graph_after_run.staged_formulas as u64 == expected_staged);
    let unrelated_dirty_retained =
        (target_path && locality_scope).then_some(graph_after_run.dirty_vertices == expected_dirty);
    let prepared_delta = graph_after_run
        .graph_formula_vertices
        .saturating_sub(graph_after_setup.graph_formula_vertices) as u64;
    let max_staged_selected = telemetry
        .values()
        .map(|stats| stats.staged_selected)
        .max()
        .unwrap_or(0);
    let oracle_within_one_percent = target_path.then_some(
        prepared_delta <= allowed_oracle(shape.oracle(scope))
            && telemetry
                .values()
                .all(|stats| stats.staged_selected <= allowed_oracle(shape.oracle(scope))),
    );
    let exact_fixture_counts_passed = graph_after_load.graph_formula_vertices == 0
        && graph_after_load.staged_formulas == shape.formulas as usize
        && graph_after_load.dirty_vertices == 0
        && graph_after_setup.graph_formula_vertices == shape.dirty_formulas as usize
        && graph_after_setup.staged_formulas == (shape.formulas - shape.dirty_formulas) as usize
        && graph_after_setup.dirty_vertices == 9;
    let exact_locality_counts_passed = prepared_delta == expected_prepared
        && graph_after_run.staged_formulas as u64 == expected_staged
        && graph_after_run.dirty_vertices == expected_dirty
        && max_staged_selected == if target_path { expected_prepared } else { 0 };

    let analytical_expected_outputs = analytical_expected_outputs(shape, scope, warm_repeats);
    let analytical_output_oracle_passed = if typed_error.is_none() {
        match validate_analytical_outputs(&outputs, &analytical_expected_outputs) {
            Ok(()) => Some(true),
            Err(error) => {
                typed_error = Some(format!("analytical output oracle mismatch: {error:#}"));
                Some(false)
            }
        }
    } else {
        None
    };
    if typed_error.is_none() && (!exact_fixture_counts_passed || !exact_locality_counts_passed) {
        typed_error = Some(format!(
            "exact fixture/locality assertion failed: fixture={exact_fixture_counts_passed}, locality={exact_locality_counts_passed}, prepared={prepared_delta}/{expected_prepared}, staged={}/{expected_staged}, dirty={}/{expected_dirty}, staged_selected={max_staged_selected}",
            graph_after_run.staged_formulas, graph_after_run.dirty_vertices
        ));
    }
    let flat_outputs = outputs.iter().flatten().cloned().collect::<Vec<_>>();
    let (current_rss_bytes, peak_rss_bytes) = process_memory_bytes();

    Ok(ChildReport {
        schema_version: 2,
        path,
        scope,
        formulas,
        reachable_oracle: shape.oracle(scope),
        fixture_sha256,
        status: if typed_error.is_some() {
            "path_error"
        } else {
            "ok"
        }
        .to_string(),
        phases,
        output_checksum: typed_error
            .is_none()
            .then(|| checksum_values(&flat_outputs)),
        outputs,
        analytical_expected_outputs,
        analytical_output_oracle_passed,
        typed_error,
        telemetry,
        graph_after_load: Some(graph_after_load),
        graph_after_setup: Some(graph_after_setup),
        graph_after_run: Some(graph_after_run),
        unrelated_staged_retained,
        unrelated_dirty_retained,
        oracle_within_one_percent,
        exact_fixture_counts_passed: Some(exact_fixture_counts_passed),
        exact_locality_counts_passed: Some(exact_locality_counts_passed),
        current_rss_bytes,
        peak_rss_bytes,
    })
}

#[cfg(feature = "c6_calibration")]
fn run_full(
    workbook: &mut Workbook,
    shape: FixtureShape,
    scope: TargetScope,
    warm_repeats: usize,
    phases: &mut PhaseTimings,
    telemetry: &mut BTreeMap<String, EngineTelemetry>,
    outputs: &mut Vec<Vec<String>>,
) -> Result<()> {
    let resolve = Instant::now();
    let cells = shape.output_cells(scope);
    phases.bind_target_resolution = Some(TimedPhase::new(
        elapsed_ms(resolve),
        &["target_address_construction"],
    ));
    let prepare = Instant::now();
    workbook.prepare_graph_all()?;
    phases.preparation_plan_build =
        Some(TimedPhase::new(elapsed_ms(prepare), &["prepare_graph_all"]));
    let evaluate = Instant::now();
    workbook.evaluate_all()?;
    phases.first_evaluation = Some(TimedPhase::new(elapsed_ms(evaluate), &["evaluate_all"]));
    telemetry.insert("first_evaluation".to_string(), collect_telemetry(workbook));
    let read = Instant::now();
    outputs.push(read_cells(workbook, &cells));
    phases.output_read = Some(TimedPhase::new(elapsed_ms(read), &["get_value"]));

    for repeat in 0..warm_repeats {
        edit_input(workbook, shape, scope, repeat, phases)?;
        let evaluate = Instant::now();
        workbook.evaluate_all()?;
        phases
            .warm_evaluation
            .push(TimedPhase::new(elapsed_ms(evaluate), &["evaluate_all"]));
        telemetry.insert(format!("warm_{repeat}"), collect_telemetry(workbook));
        let read = Instant::now();
        outputs.push(read_cells(workbook, &cells));
        phases
            .warm_output_read
            .push(TimedPhase::new(elapsed_ms(read), &["get_value"]));
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn run_cells(
    workbook: &mut Workbook,
    shape: FixtureShape,
    scope: TargetScope,
    warm_repeats: usize,
    phases: &mut PhaseTimings,
    telemetry: &mut BTreeMap<String, EngineTelemetry>,
    outputs: &mut Vec<Vec<String>>,
) -> Result<()> {
    let resolve = Instant::now();
    let cells = shape.cell_targets(scope);
    phases.bind_target_resolution = Some(TimedPhase::new(
        elapsed_ms(resolve),
        &["cell_target_construction"],
    ));
    let evaluate = Instant::now();
    outputs.push(literals(workbook.evaluate_cells(&cells)?));
    phases.first_evaluation = Some(TimedPhase::new(
        elapsed_ms(evaluate),
        &[
            "target_preparation",
            "ephemeral_plan_build",
            "evaluation",
            "output_return",
        ],
    ));
    telemetry.insert("first_evaluation".to_string(), collect_telemetry(workbook));

    for repeat in 0..warm_repeats {
        edit_input(workbook, shape, scope, repeat, phases)?;
        let evaluate = Instant::now();
        outputs.push(literals(workbook.evaluate_cells(&cells)?));
        phases.warm_evaluation.push(TimedPhase::new(
            elapsed_ms(evaluate),
            &[
                "target_preparation",
                "ephemeral_plan_build",
                "evaluation",
                "output_return",
            ],
        ));
        telemetry.insert(format!("warm_{repeat}"), collect_telemetry(workbook));
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn run_plan(
    workbook: &mut Workbook,
    shape: FixtureShape,
    scope: TargetScope,
    warm_repeats: usize,
    phases: &mut PhaseTimings,
    telemetry: &mut BTreeMap<String, EngineTelemetry>,
    outputs: &mut Vec<Vec<String>>,
) -> Result<()> {
    let resolve = Instant::now();
    let targets = shape.targets(scope);
    let cells = shape.output_cells(scope);
    phases.bind_target_resolution = Some(TimedPhase::new(
        elapsed_ms(resolve),
        &["typed_target_construction"],
    ));
    let prepare = Instant::now();
    let plan = workbook.build_recalc_plan_for_targets(&targets)?;
    phases.preparation_plan_build = Some(TimedPhase::new(
        elapsed_ms(prepare),
        &["target_preparation", "revision_bound_plan_build"],
    ));
    telemetry.insert("plan_build".to_string(), collect_telemetry(workbook));
    let evaluate = Instant::now();
    workbook.evaluate_with_plan(&plan)?;
    phases.first_evaluation = Some(TimedPhase::new(
        elapsed_ms(evaluate),
        &["evaluate_with_plan"],
    ));
    telemetry.insert("first_evaluation".to_string(), collect_telemetry(workbook));
    let read = Instant::now();
    outputs.push(read_cells(workbook, &cells));
    phases.output_read = Some(TimedPhase::new(elapsed_ms(read), &["get_value"]));

    for repeat in 0..warm_repeats {
        edit_input(workbook, shape, scope, repeat, phases)?;
        let evaluate = Instant::now();
        workbook.evaluate_with_plan(&plan)?;
        phases.warm_evaluation.push(TimedPhase::new(
            elapsed_ms(evaluate),
            &["evaluate_with_plan"],
        ));
        telemetry.insert(format!("warm_{repeat}"), collect_telemetry(workbook));
        let read = Instant::now();
        outputs.push(read_cells(workbook, &cells));
        phases
            .warm_output_read
            .push(TimedPhase::new(elapsed_ms(read), &["get_value"]));
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn run_sheetport(
    workbook: &mut Workbook,
    shape: FixtureShape,
    scope: TargetScope,
    warm_repeats: usize,
    phases: &mut PhaseTimings,
    telemetry: &mut BTreeMap<String, EngineTelemetry>,
    outputs: &mut Vec<Vec<String>>,
) -> Result<()> {
    let bind = Instant::now();
    let manifest = Manifest::from_yaml_str(&manifest_yaml(shape, scope))?;
    let mut sheetport = SheetPort::new(workbook, manifest)?;
    phases.bind_target_resolution = Some(TimedPhase::new(
        elapsed_ms(bind),
        &[
            "manifest_parse",
            "manifest_bind",
            "workbook_selector_validation",
        ],
    ));

    let first = Instant::now();
    let first_output = sheetport.evaluate_once(EvalOptions::default())?;
    phases.first_evaluation = Some(TimedPhase::new(
        elapsed_ms(first),
        &[
            "selector_resolution",
            "target_preparation",
            "evaluation",
            "sheetport_output_read",
        ],
    ));
    outputs.push(sheetport_values(&first_output, shape, scope));
    telemetry.insert(
        "first_evaluation".to_string(),
        collect_telemetry(sheetport.workbook()),
    );

    let completion_ms = Arc::new(Mutex::new(Vec::new()));
    let last = Arc::new(Mutex::new(Instant::now()));
    let completion_capture = Arc::clone(&completion_ms);
    let last_capture = Arc::clone(&last);
    let plan_started = Instant::now();
    let batch_options = BatchOptions {
        progress: Some(Box::new(move |_| {
            let now = Instant::now();
            let mut previous = last_capture.lock().expect("progress clock lock");
            completion_capture
                .lock()
                .expect("progress samples lock")
                .push(now.duration_since(*previous).as_secs_f64() * 1000.0);
            *previous = now;
        })),
        ..BatchOptions::default()
    };
    let (batch_results, batch_total_ms) = {
        let mut batch = sheetport.batch(batch_options)?;
        phases.preparation_plan_build = Some(TimedPhase::new(
            elapsed_ms(plan_started),
            &[
                "input_baseline_read",
                "selector_resolution",
                "target_preparation",
                "revision_bound_plan_build",
            ],
        ));
        telemetry.insert(
            "sheetport_batch_plan_build".to_string(),
            collect_telemetry(batch.workbook_for_benchmark()),
        );
        let cases = (0..warm_repeats)
            .map(|repeat| {
                let mut update = InputUpdate::new();
                update.insert(
                    "input",
                    PortValue::Scalar(LiteralValue::Number(2.0 + repeat as f64)),
                );
                BatchInput::new(format!("warm_{repeat}"), update)
            })
            .collect::<Vec<_>>();
        let run_started = Instant::now();
        *last.lock().expect("progress clock lock") = run_started;
        let results = batch.run(cases)?;
        let total_ms = elapsed_ms(run_started);
        telemetry.insert(
            "sheetport_batch_execution".to_string(),
            collect_telemetry(batch.workbook_for_benchmark()),
        );
        (results, total_ms)
    };
    let samples = completion_ms.lock().expect("progress samples lock").clone();
    for duration in &samples {
        phases.warm_evaluation.push(TimedPhase::new(
            *duration,
            &["input_edit", "plan_evaluation", "sheetport_output_read"],
        ));
    }
    let samples_total_ms = samples.iter().sum::<f64>();
    anyhow::ensure!(
        batch_total_ms >= samples_total_ms,
        "SheetPort batch timing accounting is negative: total={batch_total_ms:.6}ms, iterations={samples_total_ms:.6}ms"
    );
    let restore_ms = batch_total_ms - samples_total_ms;
    phases.batch_restore = Some(TimedPhase::new(
        restore_ms,
        &[
            "baseline_restore_edit",
            "plan_evaluation",
            "sheetport_output_read",
        ],
    ));
    for result in batch_results {
        outputs.push(sheetport_values(&result.outputs, shape, scope));
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn edit_input(
    workbook: &mut Workbook,
    shape: FixtureShape,
    scope: TargetScope,
    repeat: usize,
    phases: &mut PhaseTimings,
) -> Result<()> {
    let edit = Instant::now();
    workbook.set_value(
        shape.edit_sheet(scope),
        1,
        1,
        LiteralValue::Number(2.0 + repeat as f64),
    )?;
    phases.edit.push(TimedPhase::new(
        elapsed_ms(edit),
        &["value_edit_inside_selected_closure"],
    ));
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn validate_analytical_outputs(outputs: &[Vec<String>], expected: &[Vec<f64>]) -> Result<()> {
    anyhow::ensure!(
        outputs.len() == expected.len(),
        "evaluation count: actual {}, expected {}",
        outputs.len(),
        expected.len()
    );
    for (evaluation, (actual_values, expected_values)) in outputs.iter().zip(expected).enumerate() {
        anyhow::ensure!(
            actual_values.len() == expected_values.len(),
            "evaluation {evaluation} output count: actual {}, expected {}",
            actual_values.len(),
            expected_values.len()
        );
        for (output, (actual, expected)) in actual_values.iter().zip(expected_values).enumerate() {
            let actual = parse_numeric_literal(actual).with_context(|| {
                format!("evaluation {evaluation} output {output} is not numeric: {actual}")
            })?;
            let tolerance = 1e-9 * expected.abs().max(1.0);
            anyhow::ensure!(
                (actual - expected).abs() <= tolerance,
                "evaluation {evaluation} output {output}: actual {actual:.17}, analytical {expected:.17}, tolerance {tolerance:.3e}"
            );
        }
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn parse_numeric_literal(value: &str) -> Result<f64> {
    let number = value
        .strip_prefix("Number(")
        .and_then(|value| value.strip_suffix(')'))
        .or_else(|| {
            value
                .strip_prefix("Int(")
                .and_then(|value| value.strip_suffix(')'))
        })
        .context("LiteralValue debug representation")?;
    number.parse().context("numeric LiteralValue payload")
}

#[cfg(feature = "c6_calibration")]
fn literals(values: Vec<LiteralValue>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| format!("{value:?}"))
        .collect()
}

#[cfg(feature = "c6_calibration")]
fn read_cells(workbook: &Workbook, cells: &[(&str, u32, u32)]) -> Vec<String> {
    cells
        .iter()
        .map(|(sheet, row, col)| {
            format!(
                "{:?}",
                workbook
                    .get_value(sheet, *row, *col)
                    .unwrap_or(LiteralValue::Empty)
            )
        })
        .collect()
}

#[cfg(feature = "c6_calibration")]
fn sheetport_values(
    snapshot: &formualizer_sheetport::OutputSnapshot,
    shape: FixtureShape,
    scope: TargetScope,
) -> Vec<String> {
    (0..shape.output_cells(scope).len())
        .map(|index| match snapshot.get(&format!("output_{index}")) {
            Some(PortValue::Scalar(value)) => format!("{value:?}"),
            other => format!("{other:?}"),
        })
        .collect()
}

#[cfg(feature = "c6_calibration")]
fn graph_snapshot(workbook: &Workbook) -> GraphSnapshot {
    let stats = workbook.engine().baseline_stats();
    GraphSnapshot {
        graph_vertices: stats.graph_vertex_count,
        graph_formula_vertices: stats.graph_formula_vertex_count,
        graph_edges: stats.graph_edge_count,
        dirty_vertices: stats.dirty_vertex_count,
        evaluation_vertices: stats.evaluation_vertex_count,
        staged_formulas: stats.staged_formula_count,
        active_spans: stats.formula_plane_active_span_count,
    }
}

#[cfg(feature = "c6_calibration")]
fn collect_telemetry(workbook: &Workbook) -> EngineTelemetry {
    let baseline = workbook.engine().baseline_stats();
    let Some(request) = workbook.engine().last_evaluation_resource_request_stats() else {
        return EngineTelemetry {
            graph_vertices: baseline.graph_vertex_count,
            graph_formula_vertices: baseline.graph_formula_vertex_count,
            graph_edges: baseline.graph_edge_count,
            dirty_vertices: baseline.dirty_vertex_count,
            staged_formulas: baseline.staged_formula_count,
            active_spans: baseline.formula_plane_active_span_count,
            ..EngineTelemetry::default()
        };
    };
    EngineTelemetry {
        request_id: request.request_id,
        request_kind: request.kind.as_str().to_string(),
        request_outcome: request.outcome.as_str().to_string(),
        graph_vertices: baseline.graph_vertex_count,
        graph_formula_vertices: baseline.graph_formula_vertex_count,
        graph_edges: baseline.graph_edge_count,
        dirty_vertices: baseline.dirty_vertex_count,
        staged_formulas: baseline.staged_formula_count,
        active_spans: baseline.formula_plane_active_span_count,
        target_requested: request.target_requested,
        target_normalized_regions: request.target_normalized_regions,
        staged_selected: request.staged_selected,
        staged_retained: request.staged_retained,
        preparation_scope_level: request.target_scope_level,
        widening_reason_bits: request.target_widening_reason_bits,
        target_commit_estimated_work: request.target_commit_estimated_work,
        target_commit_actual_work: request.target_commit_actual_work,
        work_charged: request.ledger.work_charged,
        topology_strategy: request.topology.strategy.as_str().to_string(),
        topology_cache_outcome: request.topology.cache_outcome.as_str().to_string(),
        topology_producers: request.topology.producers_observed,
        topology_candidates: request.topology.candidates_observed,
        topology_edges: request.topology.edges_observed,
        topology_retained_bytes: request.topology.retained_bytes_observed,
        exact_pass_count: request.topology.exact_pass_count,
        native_topology_disk_bytes: request.topology.native_topology_disk_bytes,
        fallback_materialized_cells: request.fallback_materialized_cells,
        cycle_materialized_cells: request.cycle_materialized_cells,
        dirty_lease_outcome: request.dirty_lease.as_str().to_string(),
        retained_current: request.ledger.retained_current,
        retained_peak: request.ledger.retained_peak,
        scratch_current: request.ledger.scratch_current,
        scratch_peak: request.ledger.scratch_peak,
        graph_source_scratch_estimated: request.graph_source_scratch_estimated,
        graph_source_scratch_observed: request.graph_source_scratch_observed,
        request_total_ms: request.phases.total_ns as f64 / 1_000_000.0,
        graph_prepare_ms: request.phases.staged_prepare_ns as f64 / 1_000_000.0,
        topology_ms: request.phases.topology_ns as f64 / 1_000_000.0,
        materialization_ms: request.phases.materialization_ns as f64 / 1_000_000.0,
        evaluation_ms: request.phases.evaluation_ns as f64 / 1_000_000.0,
    }
}

#[cfg(feature = "c6_calibration")]
fn process_memory_bytes() -> (Option<u64>, Option<u64>) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    let parse = |prefix: &str| {
        status.lines().find_map(|line| {
            line.strip_prefix(prefix)?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
    };
    (parse("VmRSS:"), parse("VmHWM:"))
}

#[cfg(all(test, feature = "c6_calibration"))]
mod tests {
    use super::*;
    use clap::Parser;

    fn temp_fixture(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "formualizer-c6-{label}-{}-{}.xlsx",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ))
    }

    #[test]
    fn parses_sample_command() {
        let cli = Cli::try_parse_from([
            "probe",
            "sample",
            "--fixture",
            "fixture.xlsx",
            "--formulas",
            "500",
            "--path",
            "sheetport",
            "--scope",
            "medium",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Sample {
                path: CalibrationPath::Sheetport,
                scope: TargetScope::Medium,
                ..
            }
        ));
    }

    #[test]
    fn fixture_bytes_are_deterministic() {
        let first = temp_fixture("determinism-a");
        let second = temp_fixture("determinism-b");
        generate_fixture(&first, 200).unwrap();
        generate_fixture(&second, 200).unwrap();
        assert_eq!(sha256_file(&first).unwrap(), sha256_file(&second).unwrap());
        let _ = std::fs::remove_file(first);
        let _ = std::fs::remove_file(second);
    }

    #[test]
    fn smoke_paths_have_output_parity_and_target_locality() {
        let fixture = temp_fixture("path-parity");
        generate_fixture(&fixture, 200).unwrap();
        let reports = CalibrationPath::ALL
            .into_iter()
            .map(|path| run_sample(&fixture, 200, path, TargetScope::Tiny, 1).unwrap())
            .collect::<Vec<_>>();
        let expected = &reports[0].outputs;
        assert!(reports.iter().all(|report| report.outputs == *expected));
        assert!(
            reports
                .iter()
                .filter(|report| report.path != CalibrationPath::Full)
                .all(|report| report.oracle_within_one_percent == Some(true))
        );
        assert!(
            reports
                .iter()
                .filter(|report| report.path != CalibrationPath::Full)
                .all(|report| report.unrelated_staged_retained == Some(true))
        );
        let _ = std::fs::remove_file(fixture);
    }
}
