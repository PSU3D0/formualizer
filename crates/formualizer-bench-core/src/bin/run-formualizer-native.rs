#[cfg(feature = "formualizer_runner")]
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Instant,
};

#[cfg(feature = "formualizer_runner")]
use serde_json::json;

#[cfg(feature = "formualizer_runner")]
use anyhow::{Context, Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use formualizer_bench_core::{BenchmarkResult, BenchmarkSuite, CorrectnessResult, MetricsResult};

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --bin run-formualizer-native -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    run()
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = "benchmarks/scenarios.yaml")]
    scenarios: PathBuf,
    #[arg(long)]
    scenario: String,
    #[arg(long, default_value = ".")]
    root: PathBuf,
    #[arg(long, default_value = "native_best")]
    mode: String,
    #[arg(long)]
    reuse_recalc_plan: bool,
    #[arg(long, value_enum, default_value_t = BackendMode::Umya)]
    backend: BackendMode,
    /// Opt into experimental FormulaPlane span evaluation for this run.
    #[arg(long)]
    span_evaluation: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum BackendMode {
    Umya,
    Calamine,
}

#[cfg(feature = "formualizer_runner")]
impl BackendMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Umya => "umya",
            Self::Calamine => "calamine",
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn run() -> Result<()> {
    use formualizer_workbook::{
        CalamineAdapter, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
    };

    let cli = Cli::parse();
    let suite = BenchmarkSuite::from_yaml_path(&cli.scenarios)
        .with_context(|| format!("load scenarios {}", cli.scenarios.display()))?;
    let scenario = suite
        .scenario(&cli.scenario)
        .with_context(|| format!("unknown scenario: {}", cli.scenario))?;

    let root = cli.root.canonicalize().unwrap_or(cli.root.clone());
    let workbook_path = resolve_output_path(&root, &scenario.source.workbook_path);
    if !workbook_path.exists() {
        bail!(
            "workbook not found: {} (run generate-corpus first)",
            workbook_path.display()
        );
    }

    let workbook_config = || {
        WorkbookConfig::ephemeral().with_span_evaluation(
            cli.span_evaluation || cli.mode.contains("span") || cli.mode.contains("formula_plane"),
        )
    };

    let load_start = Instant::now();
    let (mut wb, open_read_ms, workbook_ingest_ms, adapter_load_stats) = match cli.backend {
        BackendMode::Umya => {
            let open_start = Instant::now();
            let backend = UmyaAdapter::open_path(&workbook_path)
                .map_err(|e| anyhow::anyhow!("open workbook via umya: {e}"))?;
            let open_read_ms = open_start.elapsed().as_secs_f64() * 1000.0;
            let ingest_start = Instant::now();
            let (wb, stats) = Workbook::from_reader_with_adapter_stats(
                backend,
                LoadStrategy::EagerAll,
                workbook_config(),
            )
            .map_err(|e| anyhow::anyhow!("load workbook into engine via umya: {e}"))?;
            let workbook_ingest_ms = ingest_start.elapsed().as_secs_f64() * 1000.0;
            (wb, open_read_ms, workbook_ingest_ms, stats)
        }
        BackendMode::Calamine => {
            let open_start = Instant::now();
            let backend = CalamineAdapter::open_path(&workbook_path)
                .map_err(|e| anyhow::anyhow!("open workbook via calamine: {e}"))?;
            let open_read_ms = open_start.elapsed().as_secs_f64() * 1000.0;
            let ingest_start = Instant::now();
            let (wb, stats) = Workbook::from_reader_with_adapter_stats(
                backend,
                LoadStrategy::EagerAll,
                workbook_config(),
            )
            .map_err(|e| anyhow::anyhow!("load workbook into engine via calamine: {e}"))?;
            let workbook_ingest_ms = ingest_start.elapsed().as_secs_f64() * 1000.0;
            (wb, open_read_ms, workbook_ingest_ms, stats)
        }
    };
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;
    let load_engine_stats = wb.engine().baseline_stats();

    let mut full_eval_ms: Option<f64> = None;
    let mut incremental_us: Option<f64> = None;
    let mut full_eval_computed_vertices: Option<u64> = None;
    let mut full_eval_engine_elapsed_ms: Option<f64> = None;
    let mut incremental_pending_engine_stats = None;
    let mut incremental_computed_vertices: Option<u64> = None;
    let mut incremental_engine_elapsed_ms: Option<f64> = None;
    // Full-workbook recalc-plan reuse is primarily intended for stable-topology workloads
    // whose dirty frontier stays large enough to amortize schedule rebuild cost (for example,
    // deep chains and broad fanout). It remains correctness-safe on tiny-frontier cases like
    // headline single-edit probes, but those may still favor the baseline incremental path.
    let use_recalc_plan = cli.reuse_recalc_plan || mode_requests_recalc_plan(&cli.mode);
    let mut cached_plan: Option<_> = None;
    let mut notes = Vec::new();
    let mut plan_builds = 0u64;
    let mut plan_reuses = 0u64;
    let mut plan_fallbacks = 0u64;
    let mut plan_invalidations = 0u64;
    let mut plan_layer_count: Option<u64> = None;
    let mut plan_has_dynamic_refs = false;

    for op in &scenario.operations {
        match op.op.as_str() {
            "load" => {}
            "evaluate_all" => {
                let t0 = Instant::now();
                let eval_result = wb
                    .evaluate_all()
                    .map_err(|e| anyhow::anyhow!("evaluate_all: {e}"))?;
                full_eval_ms = Some(t0.elapsed().as_secs_f64() * 1000.0);
                full_eval_computed_vertices = Some(eval_result.computed_vertices as u64);
                full_eval_engine_elapsed_ms = Some(eval_result.elapsed.as_secs_f64() * 1000.0);
                if use_recalc_plan {
                    let plan = wb.build_recalc_plan().map_err(|e| {
                        anyhow::anyhow!("build_recalc_plan after evaluate_all: {e}")
                    })?;
                    plan_layer_count = Some(plan.layer_count() as u64);
                    plan_has_dynamic_refs = plan.has_dynamic_refs();
                    cached_plan = Some(plan);
                    plan_builds += 1;
                }
            }
            "evaluate_incremental" => {
                incremental_pending_engine_stats = Some(wb.engine().baseline_stats());
                let t0 = Instant::now();
                let incremental_eval_result;
                let reused_plan = if use_recalc_plan {
                    if let Some(plan) = cached_plan.as_ref() {
                        if plan.has_dynamic_refs() {
                            notes.push(
                                "recalc_plan_fallback:evaluate_all(dynamic_refs_present)"
                                    .to_string(),
                            );
                            let eval_result = wb.evaluate_all().map_err(|e| {
                                anyhow::anyhow!(
                                    "evaluate_incremental/evaluate_all(dynamic fallback): {e}"
                                )
                            })?;
                            incremental_eval_result = Some(eval_result);
                            plan_fallbacks += 1;
                            false
                        } else {
                            let eval_result = wb.evaluate_with_plan(plan).map_err(|e| {
                                anyhow::anyhow!("evaluate_incremental/evaluate_with_plan: {e}")
                            })?;
                            incremental_eval_result = Some(eval_result);
                            plan_reuses += 1;
                            true
                        }
                    } else {
                        notes.push("recalc_plan_fallback:evaluate_all(no_cached_plan)".to_string());
                        let eval_result = wb.evaluate_all().map_err(|e| {
                            anyhow::anyhow!(
                                "evaluate_incremental/evaluate_all(no cached plan): {e}"
                            )
                        })?;
                        incremental_eval_result = Some(eval_result);
                        plan_fallbacks += 1;
                        false
                    }
                } else {
                    let eval_result = wb
                        .evaluate_all()
                        .map_err(|e| anyhow::anyhow!("evaluate_incremental/evaluate_all: {e}"))?;
                    incremental_eval_result = Some(eval_result);
                    false
                };
                incremental_us = Some(t0.elapsed().as_secs_f64() * 1_000_000.0);
                if let Some(eval_result) = incremental_eval_result {
                    incremental_computed_vertices = Some(eval_result.computed_vertices as u64);
                    incremental_engine_elapsed_ms =
                        Some(eval_result.elapsed.as_secs_f64() * 1000.0);
                }

                if use_recalc_plan && !reused_plan {
                    let plan = wb.build_recalc_plan().map_err(|e| {
                        anyhow::anyhow!("build_recalc_plan after incremental fallback: {e}")
                    })?;
                    plan_layer_count = Some(plan.layer_count() as u64);
                    plan_has_dynamic_refs = plan.has_dynamic_refs();
                    cached_plan = Some(plan);
                    plan_builds += 1;
                }
            }
            "edit_set_value" => {
                let sheet = arg_str(op, "sheet")?;
                let row = arg_u32(op, "row")?;
                let col = arg_u32(op, "col")?;
                let value = arg_literal_value(op, "value")?;
                wb.set_value(&sheet, row, col, value)
                    .map_err(|e| anyhow::anyhow!("set_value: {e}"))?;
            }
            "edit_set_formula" => {
                let sheet = arg_str(op, "sheet")?;
                let row = arg_u32(op, "row")?;
                let col = arg_u32(op, "col")?;
                let formula = arg_str(op, "formula")?;
                wb.set_formula(&sheet, row, col, &formula)
                    .map_err(|e| anyhow::anyhow!("set_formula: {e}"))?;
                if use_recalc_plan {
                    invalidate_cached_plan(
                        &mut cached_plan,
                        &mut notes,
                        &mut plan_invalidations,
                        "edit_set_formula",
                    );
                }
            }
            "add_sheet" => {
                let sheet = arg_str(op, "sheet")?;
                wb.add_sheet(&sheet)
                    .map_err(|e| anyhow::anyhow!("add_sheet: {e}"))?;
                if use_recalc_plan {
                    invalidate_cached_plan(
                        &mut cached_plan,
                        &mut notes,
                        &mut plan_invalidations,
                        "add_sheet",
                    );
                }
            }
            "remove_sheet" => {
                let sheet = arg_str(op, "sheet")?;
                wb.delete_sheet(&sheet)
                    .map_err(|e| anyhow::anyhow!("delete_sheet: {e}"))?;
                if use_recalc_plan {
                    invalidate_cached_plan(
                        &mut cached_plan,
                        &mut notes,
                        &mut plan_invalidations,
                        "remove_sheet",
                    );
                }
            }
            "insert_rows" => {
                let sheet = arg_str(op, "sheet")?;
                let before = arg_u32(op, "before")?;
                let count = arg_u32(op, "count")?;
                wb.engine_mut()
                    .insert_rows(&sheet, before, count)
                    .map_err(|e| anyhow::anyhow!("insert_rows: {e}"))?;
                if use_recalc_plan {
                    invalidate_cached_plan(
                        &mut cached_plan,
                        &mut notes,
                        &mut plan_invalidations,
                        "insert_rows",
                    );
                }
            }
            "rename_sheet" => {
                if let (Ok(old), Ok(new)) = (arg_str(op, "old"), arg_str(op, "new")) {
                    wb.rename_sheet(&old, &new)
                        .map_err(|e| anyhow::anyhow!("rename_sheet old/new: {e}"))?;
                } else if let (Ok(old), Ok(new)) = (arg_str(op, "sheet"), arg_str(op, "new")) {
                    wb.rename_sheet(&old, &new)
                        .map_err(|e| anyhow::anyhow!("rename_sheet sheet/new: {e}"))?;
                } else {
                    bail!("rename_sheet op requires old+new or sheet+new")
                }
                if use_recalc_plan {
                    invalidate_cached_plan(
                        &mut cached_plan,
                        &mut notes,
                        &mut plan_invalidations,
                        "rename_sheet",
                    );
                }
            }
            "read_cells" => {}
            unsupported => bail!("unsupported op in native adapter: {unsupported}"),
        }
    }

    let final_engine_stats = wb.engine().baseline_stats();

    let correctness = verify_correctness(&mut wb, scenario, &root)?;
    let status = if correctness.passed { "ok" } else { "invalid" }.to_string();

    let mut metrics_extra = BTreeMap::new();
    macro_rules! insert_engine_stats {
        ($prefix:expr, $stats:expr) => {{
            let prefix = $prefix;
            let stats = $stats;
            metrics_extra.insert(
                format!("{prefix}_graph_vertex_count"),
                json!(stats.graph_vertex_count as u64),
            );
            metrics_extra.insert(
                format!("{prefix}_graph_formula_vertex_count"),
                json!(stats.graph_formula_vertex_count as u64),
            );
            metrics_extra.insert(
                format!("{prefix}_graph_edge_count"),
                json!(stats.graph_edge_count as u64),
            );
            metrics_extra.insert(
                format!("{prefix}_dirty_vertex_count"),
                json!(stats.dirty_vertex_count as u64),
            );
            metrics_extra.insert(
                format!("{prefix}_evaluation_vertex_count"),
                json!(stats.evaluation_vertex_count as u64),
            );
            metrics_extra.insert(
                format!("{prefix}_formula_ast_root_count"),
                json!(stats.formula_ast_root_count as u64),
            );
            metrics_extra.insert(
                format!("{prefix}_formula_ast_node_count"),
                json!(stats.formula_ast_node_count as u64),
            );
            metrics_extra.insert(
                format!("{prefix}_staged_formula_count"),
                json!(stats.staged_formula_count as u64),
            );
        }};
    }
    metrics_extra.insert("backend".to_string(), json!(cli.backend.as_str()));
    metrics_extra.insert("open_read_ms".to_string(), json!(open_read_ms));
    metrics_extra.insert("workbook_ingest_ms".to_string(), json!(workbook_ingest_ms));
    insert_adapter_load_stats(&mut metrics_extra, adapter_load_stats.as_ref());
    insert_engine_stats!("load", load_engine_stats);
    insert_engine_stats!("final", final_engine_stats);
    if let Some(stats) = incremental_pending_engine_stats {
        insert_engine_stats!("incremental_pending", stats);
    }
    if let Some(value) = full_eval_computed_vertices {
        metrics_extra.insert("full_eval_computed_vertices".to_string(), json!(value));
    }
    if let Some(value) = full_eval_engine_elapsed_ms {
        metrics_extra.insert("full_eval_engine_elapsed_ms".to_string(), json!(value));
    }
    if let Some(value) = incremental_computed_vertices {
        metrics_extra.insert("incremental_computed_vertices".to_string(), json!(value));
    }
    if let Some(value) = incremental_engine_elapsed_ms {
        metrics_extra.insert("incremental_engine_elapsed_ms".to_string(), json!(value));
    }
    metrics_extra.insert("recalc_plan_requested".to_string(), json!(use_recalc_plan));
    metrics_extra.insert("recalc_plan_builds".to_string(), json!(plan_builds));
    metrics_extra.insert("recalc_plan_reuses".to_string(), json!(plan_reuses));
    metrics_extra.insert("recalc_plan_fallbacks".to_string(), json!(plan_fallbacks));
    metrics_extra.insert(
        "recalc_plan_invalidations".to_string(),
        json!(plan_invalidations),
    );
    metrics_extra.insert(
        "recalc_plan_last_has_dynamic_refs".to_string(),
        json!(plan_has_dynamic_refs),
    );
    if let Some(layer_count) = plan_layer_count {
        metrics_extra.insert(
            "recalc_plan_last_layer_count".to_string(),
            json!(layer_count),
        );
    }

    let result = BenchmarkResult {
        engine: "formualizer_rust_native".to_string(),
        scenario: scenario.id.clone(),
        mode: cli.mode,
        status,
        metrics: MetricsResult {
            load_ms: Some(load_ms),
            full_eval_ms,
            incremental_us,
            peak_rss_mb: None,
            extra: metrics_extra,
        },
        correctness,
        notes,
        timestamp: chrono::Utc::now().to_rfc3339(),
        meta: BTreeMap::from([("backend".to_string(), json!(cli.backend.as_str()))]),
    };

    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
fn resolve_output_path(root: &Path, workbook_path: &str) -> PathBuf {
    let p = PathBuf::from(workbook_path);
    if p.is_absolute() { p } else { root.join(p) }
}

#[cfg(feature = "formualizer_runner")]
fn insert_adapter_load_stats(
    metrics_extra: &mut BTreeMap<String, serde_json::Value>,
    stats: Option<&formualizer_workbook::AdapterLoadStats>,
) {
    let Some(stats) = stats else {
        return;
    };
    if let Some(value) = stats.formula_cells_observed {
        metrics_extra.insert("adapter_formula_cells_observed".to_string(), json!(value));
    }
    if let Some(value) = stats.value_cells_observed {
        metrics_extra.insert("adapter_value_cells_observed".to_string(), json!(value));
    }
    if let Some(value) = stats.value_slots_handed_to_engine {
        metrics_extra.insert(
            "adapter_value_slots_handed_to_engine".to_string(),
            json!(value),
        );
    }
    if let Some(value) = stats.formula_cells_handed_to_engine {
        metrics_extra.insert(
            "adapter_formula_cells_handed_to_engine".to_string(),
            json!(value),
        );
    }
    if let Some(value) = stats.shared_formula_tags_observed {
        metrics_extra.insert(
            "adapter_shared_formula_tags_observed".to_string(),
            json!(value),
        );
    }
}

#[cfg(feature = "formualizer_runner")]
fn mode_requests_recalc_plan(mode: &str) -> bool {
    matches!(mode, "native_best_cached_plan") || mode.ends_with("_cached_plan")
}

#[cfg(feature = "formualizer_runner")]
fn invalidate_cached_plan<T>(
    cached_plan: &mut Option<T>,
    notes: &mut Vec<String>,
    invalidation_count: &mut u64,
    reason: &str,
) {
    if cached_plan.take().is_some() {
        *invalidation_count += 1;
        notes.push(format!("recalc_plan_invalidated:{reason}"));
    }
}

#[cfg(feature = "formualizer_runner")]
fn arg_str(op: &formualizer_bench_core::Operation, key: &str) -> Result<String> {
    op.args
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
        .with_context(|| format!("op={} missing string arg: {}", op.op, key))
}

#[cfg(feature = "formualizer_runner")]
fn arg_u32(op: &formualizer_bench_core::Operation, key: &str) -> Result<u32> {
    op.args
        .get(key)
        .and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .with_context(|| format!("op={} missing u32 arg: {}", op.op, key))
}

#[cfg(feature = "formualizer_runner")]
fn arg_literal_value(
    op: &formualizer_bench_core::Operation,
    key: &str,
) -> Result<formualizer_workbook::LiteralValue> {
    use formualizer_workbook::LiteralValue;

    let v = op
        .args
        .get(key)
        .with_context(|| format!("op={} missing arg: {}", op.op, key))?;

    if let Some(i) = v.as_i64() {
        return Ok(LiteralValue::Int(i));
    }
    if let Some(f) = v.as_f64() {
        return Ok(LiteralValue::Number(f));
    }
    if let Some(b) = v.as_bool() {
        return Ok(LiteralValue::Boolean(b));
    }
    if let Some(s) = v.as_str() {
        return Ok(LiteralValue::Text(s.to_string()));
    }
    if v.is_null() {
        return Ok(LiteralValue::Empty);
    }

    bail!("unsupported value arg type for op={}: {}", op.op, v)
}

#[cfg(feature = "formualizer_runner")]
fn parse_cell_ref(cell_ref: &str) -> Result<(String, u32, u32)> {
    let (sheet, a1) = if let Some((sheet, a1)) = cell_ref.split_once('!') {
        (sheet.to_string(), a1)
    } else {
        ("Sheet1".to_string(), cell_ref)
    };

    let mut col: u32 = 0;
    let mut row_str = String::new();
    for ch in a1.chars() {
        if ch.is_ascii_alphabetic() {
            let up = ch.to_ascii_uppercase() as u8;
            let n = (up - b'A' + 1) as u32;
            col = col * 26 + n;
        } else if ch.is_ascii_digit() {
            row_str.push(ch);
        }
    }

    let row: u32 = row_str
        .parse()
        .with_context(|| format!("invalid row in A1 ref: {}", cell_ref))?;

    if row == 0 || col == 0 {
        bail!("invalid A1 ref: {}", cell_ref);
    }

    Ok((sheet, row, col))
}

#[cfg(feature = "formualizer_runner")]
fn verify_correctness(
    wb: &mut formualizer_workbook::Workbook,
    scenario: &formualizer_bench_core::Scenario,
    root: &Path,
) -> Result<CorrectnessResult> {
    use formualizer_workbook::LiteralValue;

    let mut mismatches = 0u64;
    let mut details = Vec::new();
    let expected_values = scenario.verify.expected_values(root)?;
    let formula_checks = scenario.verify.formula_checks(root)?;

    for (cell_ref, expected) in &expected_values {
        let (sheet, row, col) = parse_cell_ref(cell_ref)?;
        let actual = wb
            .evaluate_cell(&sheet, row, col)
            .map_err(|e| anyhow::anyhow!("evaluate expected cell {cell_ref}: {e}"))?;

        let matches = if let Some(n) = expected.as_f64() {
            match actual {
                LiteralValue::Number(v) => (v - n).abs() < 1e-9,
                LiteralValue::Int(v) => ((v as f64) - n).abs() < 1e-9,
                _ => false,
            }
        } else if let Some(i) = expected.as_i64() {
            matches!(actual, LiteralValue::Int(v) if v == i)
                || matches!(actual, LiteralValue::Number(v) if (v - (i as f64)).abs() < 1e-9)
        } else if let Some(b) = expected.as_bool() {
            matches!(actual, LiteralValue::Boolean(v) if v == b)
        } else if let Some(s) = expected.as_str() {
            matches!(actual, LiteralValue::Text(ref v) if v == s)
        } else if expected.is_null() {
            matches!(actual, LiteralValue::Empty)
        } else {
            false
        };

        if !matches {
            mismatches += 1;
            details.push(format!(
                "expected mismatch at {cell_ref}: expected={expected}, actual={actual:?}"
            ));
        }
    }

    for check in &formula_checks {
        if check.check_type == "non_error" {
            let (sheet, row, col) = parse_cell_ref(&check.cell)?;
            let actual = wb
                .evaluate_cell(&sheet, row, col)
                .map_err(|e| anyhow::anyhow!("evaluate formula check cell {}: {e}", check.cell))?;
            if matches!(actual, LiteralValue::Error(_)) {
                mismatches += 1;
                details.push(format!("formula check non_error failed at {}", check.cell));
            }
        }
    }

    Ok(CorrectnessResult {
        passed: mismatches == 0,
        mismatches,
        details,
    })
}

#[cfg(all(test, feature = "formualizer_runner"))]
mod tests {
    use super::{invalidate_cached_plan, mode_requests_recalc_plan};

    #[test]
    fn detects_cached_plan_modes() {
        assert!(mode_requests_recalc_plan("native_best_cached_plan"));
        assert!(mode_requests_recalc_plan("runtime_parity_cached_plan"));
        assert!(!mode_requests_recalc_plan("native_best"));
    }

    #[test]
    fn only_records_invalidation_when_a_plan_exists() {
        let mut cached_plan = Some(42u32);
        let mut notes = Vec::new();
        let mut invalidations = 0u64;

        invalidate_cached_plan(
            &mut cached_plan,
            &mut notes,
            &mut invalidations,
            "insert_rows",
        );
        assert!(cached_plan.is_none());
        assert_eq!(invalidations, 1);
        assert_eq!(notes, vec!["recalc_plan_invalidated:insert_rows"]);

        invalidate_cached_plan(
            &mut cached_plan,
            &mut notes,
            &mut invalidations,
            "rename_sheet",
        );
        assert_eq!(invalidations, 1);
        assert_eq!(notes.len(), 1);
    }
}
