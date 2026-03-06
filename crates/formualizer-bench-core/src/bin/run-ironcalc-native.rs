use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use formualizer_bench_core::{
    BenchmarkResult, BenchmarkSuite, CorrectnessResult, MetricsResult, Operation, Scenario,
};

#[cfg(not(feature = "ironcalc_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `ironcalc_runner`: cargo run -p formualizer-bench-core --features ironcalc_runner --bin run-ironcalc-native -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "ironcalc_runner")]
fn main() -> Result<()> {
    run()
}

#[cfg(feature = "ironcalc_runner")]
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
}

#[cfg(feature = "ironcalc_runner")]
fn run() -> Result<()> {
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

    let load_start = Instant::now();
    let (mut model, fallback_note) = load_model_for_scenario(&workbook_path, scenario)?;
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;

    let mut full_eval_ms: Option<f64> = None;
    let mut incremental_us: Option<f64> = None;

    for op in &scenario.operations {
        match op.op.as_str() {
            "load" => {}
            "evaluate_all" => {
                let t0 = Instant::now();
                model.evaluate();
                full_eval_ms = Some(t0.elapsed().as_secs_f64() * 1000.0);
            }
            "evaluate_incremental" => {
                let t0 = Instant::now();
                model.evaluate();
                incremental_us = Some(t0.elapsed().as_secs_f64() * 1_000_000.0);
            }
            "edit_set_value" => {
                let sheet_name = arg_str(op, "sheet")?;
                let row = i32::try_from(arg_u32(op, "row")?)
                    .with_context(|| format!("row out of range for i32 in op={}", op.op))?;
                let col = i32::try_from(arg_u32(op, "col")?)
                    .with_context(|| format!("col out of range for i32 in op={}", op.op))?;
                let sheet = sheet_index_by_name(&model, &sheet_name)
                    .with_context(|| format!("sheet not found: {sheet_name}"))?;
                let input = arg_as_ironcalc_input(op, "value")?;
                model
                    .set_user_input(sheet, row, col, input)
                    .map_err(|e| anyhow::anyhow!("set_user_input: {e}"))?;
            }
            "edit_set_formula" => {
                let sheet_name = arg_str(op, "sheet")?;
                let row = i32::try_from(arg_u32(op, "row")?)
                    .with_context(|| format!("row out of range for i32 in op={}", op.op))?;
                let col = i32::try_from(arg_u32(op, "col")?)
                    .with_context(|| format!("col out of range for i32 in op={}", op.op))?;
                let sheet = sheet_index_by_name(&model, &sheet_name)
                    .with_context(|| format!("sheet not found: {sheet_name}"))?;
                let formula = arg_str(op, "formula")?;
                let input = if formula.starts_with('=') {
                    formula
                } else {
                    format!("={formula}")
                };
                model
                    .set_user_input(sheet, row, col, input)
                    .map_err(|e| anyhow::anyhow!("set_user_input/formula: {e}"))?;
            }
            "add_sheet" => {
                let sheet = arg_str(op, "sheet")?;
                model
                    .add_sheet(&sheet)
                    .map_err(|e| anyhow::anyhow!("add_sheet: {e}"))?;
            }
            "remove_sheet" => {
                let sheet = arg_str(op, "sheet")?;
                model
                    .delete_sheet_by_name(&sheet)
                    .map_err(|e| anyhow::anyhow!("delete_sheet_by_name: {e}"))?;
            }
            "insert_rows" => {
                let sheet_name = arg_str(op, "sheet")?;
                let before = i32::try_from(arg_u32(op, "before")?)
                    .with_context(|| format!("before out of range for i32 in op={}", op.op))?;
                let count = i32::try_from(arg_u32(op, "count")?)
                    .with_context(|| format!("count out of range for i32 in op={}", op.op))?;
                let sheet = sheet_index_by_name(&model, &sheet_name)
                    .with_context(|| format!("sheet not found: {sheet_name}"))?;
                model
                    .insert_rows(sheet, before, count)
                    .map_err(|e| anyhow::anyhow!("insert_rows: {e}"))?;
            }
            "rename_sheet" => {
                if let (Ok(old), Ok(new)) = (arg_str(op, "old"), arg_str(op, "new")) {
                    model
                        .rename_sheet(&old, &new)
                        .map_err(|e| anyhow::anyhow!("rename_sheet old/new: {e}"))?;
                } else if let (Ok(old), Ok(new)) = (arg_str(op, "sheet"), arg_str(op, "new")) {
                    model
                        .rename_sheet(&old, &new)
                        .map_err(|e| anyhow::anyhow!("rename_sheet sheet/new: {e}"))?;
                } else {
                    bail!("rename_sheet op requires old+new or sheet+new")
                }
            }
            "read_cells" => {}
            unsupported => bail!("unsupported op in ironcalc adapter: {unsupported}"),
        }
    }

    let correctness = verify_correctness(&model, scenario, &root)?;
    let status = if correctness.passed { "ok" } else { "invalid" }.to_string();

    let result = BenchmarkResult {
        engine: "ironcalc_rust_native".to_string(),
        scenario: scenario.id.clone(),
        mode: cli.mode,
        status,
        metrics: MetricsResult {
            load_ms: Some(load_ms),
            full_eval_ms,
            incremental_us,
            peak_rss_mb: None,
            extra: BTreeMap::new(),
        },
        correctness,
        notes: fallback_note.into_iter().collect(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        meta: BTreeMap::new(),
    };

    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[cfg(feature = "ironcalc_runner")]
fn load_model_for_scenario(
    workbook_path: &Path,
    scenario: &Scenario,
) -> Result<(ironcalc::base::Model<'static>, Option<String>)> {
    use ironcalc::import::load_from_xlsx;

    let loaded = std::panic::catch_unwind(|| {
        load_from_xlsx(&workbook_path.to_string_lossy(), "en", "UTC", "en")
    });
    match loaded {
        Ok(Ok(model)) => Ok((model, None)),
        Ok(Err(err)) => {
            let model = build_model_fallback(scenario)
                .with_context(|| format!("fallback build after load_from_xlsx error: {err}"))?;
            Ok((
                model,
                Some(format!(
                    "fallback_model_built=true; load_from_xlsx_error={err}"
                )),
            ))
        }
        Err(_) => {
            let model = build_model_fallback(scenario)
                .with_context(|| "fallback build after load_from_xlsx panic")?;
            Ok((
                model,
                Some("fallback_model_built=true; load_from_xlsx_panicked=true".to_string()),
            ))
        }
    }
}

#[cfg(feature = "ironcalc_runner")]
fn cfg_u32(s: &Scenario, pointer: &str, default: u32) -> u32 {
    s.source
        .config
        .as_ref()
        .and_then(|v| v.pointer(pointer))
        .and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(default)
}

#[cfg(feature = "ironcalc_runner")]
fn render_formula_template(template: &str, row: u32, fact_last_row: Option<u32>) -> String {
    let mut formula = template.replace("{row}", &row.to_string());
    if let Some(fact_last_row) = fact_last_row {
        formula = formula.replace("{fact_last_row}", &fact_last_row.to_string());
    }
    formula
}

#[cfg(feature = "ironcalc_runner")]
fn build_model_fallback(scenario: &Scenario) -> Result<ironcalc::base::Model<'static>> {
    let mut model = ironcalc::base::Model::new_empty("bench", "en", "UTC", "en")
        .map_err(|e| anyhow::anyhow!("Model::new_empty: {e}"))?;

    match scenario.id.as_str() {
        "headline_100k_single_edit" => {
            let rows = cfg_u32(scenario, "/sheets/0/rows", 100_000);
            for r in 1..=rows {
                let rr = i32::try_from(r).with_context(|| "rows overflow i32")?;
                model
                    .set_user_input(0, rr, 1, r.to_string())
                    .map_err(|e| anyhow::anyhow!("set_user_input headline value: {e}"))?;
                model
                    .set_user_input(0, rr, 2, format!("=A{r}*2"))
                    .map_err(|e| anyhow::anyhow!("set_user_input headline formula: {e}"))?;
            }
            model
                .set_user_input(0, 1, 3, format!("=SUM(B1:B{rows})"))
                .map_err(|e| anyhow::anyhow!("set_user_input headline rollup: {e}"))?;
        }
        "chain_100k" => {
            let rows = cfg_u32(scenario, "/sheets/0/rows", 100_000);
            model
                .set_user_input(0, 1, 1, "1".to_string())
                .map_err(|e| anyhow::anyhow!("set_user_input chain seed: {e}"))?;
            for r in 2..=rows {
                let rr = i32::try_from(r).with_context(|| "rows overflow i32")?;
                model
                    .set_user_input(0, rr, 1, format!("=A{}+1", r - 1))
                    .map_err(|e| anyhow::anyhow!("set_user_input chain formula: {e}"))?;
            }
        }
        "fanout_100k" => {
            let rows = cfg_u32(scenario, "/sheets/0/rows", 100_000);
            model
                .set_user_input(0, 1, 1, "1".to_string())
                .map_err(|e| anyhow::anyhow!("set_user_input fanout seed: {e}"))?;
            for r in 1..=rows {
                let rr = i32::try_from(r).with_context(|| "rows overflow i32")?;
                model
                    .set_user_input(0, rr, 2, format!("=A$1*{r}"))
                    .map_err(|e| anyhow::anyhow!("set_user_input fanout formula: {e}"))?;
            }
        }
        "inc_sparse_dirty_region_1m" => {
            let rows = cfg_u32(scenario, "/sheets/0/rows", 1_000_000);
            let block_rows = [
                1, 125_001, 250_001, 375_001, 500_001, 625_001, 750_001, 875_001,
            ];

            for (idx, row) in block_rows.into_iter().enumerate() {
                let rr = i32::try_from(row).with_context(|| "block row overflow i32")?;
                let seed = (((idx as u32) + 1) * 10).to_string();
                model
                    .set_user_input(0, rr, 1, seed)
                    .map_err(|e| anyhow::anyhow!("set_user_input sparse seed: {e}"))?;
                model
                    .set_user_input(0, rr, 2, format!("=A{row}*2"))
                    .map_err(|e| anyhow::anyhow!("set_user_input sparse formula B: {e}"))?;
                model
                    .set_user_input(0, rr, 3, format!("=B{row}+5"))
                    .map_err(|e| anyhow::anyhow!("set_user_input sparse formula C: {e}"))?;
                model
                    .set_user_input(0, rr, 4, format!("=SUM(B{row}:C{row})"))
                    .map_err(|e| anyhow::anyhow!("set_user_input sparse formula D: {e}"))?;
            }

            let tail_row = i32::try_from(rows).with_context(|| "tail row overflow i32")?;
            model
                .set_user_input(0, tail_row, 1, "3".to_string())
                .map_err(|e| anyhow::anyhow!("set_user_input sparse tail value: {e}"))?;
            model
                .set_user_input(0, tail_row, 2, format!("=A{rows}+1"))
                .map_err(|e| anyhow::anyhow!("set_user_input sparse tail formula: {e}"))?;
        }
        "inc_cross_sheet_mesh_3x25k" => {
            let rows = cfg_u32(scenario, "/sheets/0/rows", 25_000);

            model
                .rename_sheet("Sheet1", "Inputs")
                .map_err(|e| anyhow::anyhow!("rename_sheet Inputs: {e}"))?;
            model
                .add_sheet("CalcA")
                .map_err(|e| anyhow::anyhow!("add_sheet CalcA: {e}"))?;
            model
                .add_sheet("CalcB")
                .map_err(|e| anyhow::anyhow!("add_sheet CalcB: {e}"))?;

            let inputs = 0u32;
            let calca = 1u32;
            let calcb = 2u32;

            for r in 1..=rows {
                let rr = i32::try_from(r).with_context(|| "rows overflow i32")?;
                model
                    .set_user_input(inputs, rr, 1, r.to_string())
                    .map_err(|e| anyhow::anyhow!("inputs col A: {e}"))?;
                model
                    .set_user_input(inputs, rr, 2, (r * 2).to_string())
                    .map_err(|e| anyhow::anyhow!("inputs col B: {e}"))?;
                model
                    .set_user_input(inputs, rr, 3, ((r % 10) + 1).to_string())
                    .map_err(|e| anyhow::anyhow!("inputs col C: {e}"))?;

                model
                    .set_user_input(calca, rr, 1, format!("=Inputs!A{r}+Inputs!B{r}"))
                    .map_err(|e| anyhow::anyhow!("CalcA col A: {e}"))?;

                model
                    .set_user_input(calcb, rr, 1, format!("=CalcA!A{r}*Inputs!C{r}"))
                    .map_err(|e| anyhow::anyhow!("CalcB col A: {e}"))?;
            }
        }
        "sumifs_fact_table_100k" => {
            let fact_rows = cfg_u32(scenario, "/sheets/0/rows", 100_000);
            let report_rows = cfg_u32(scenario, "/layout/report_rows", 1_000);
            let regions = ["North", "South", "East", "West"];
            let products = ["A", "B", "C", "D", "E"];
            let channels = ["Online", "Retail", "Partner"];

            model
                .add_sheet("Facts")
                .map_err(|e| anyhow::anyhow!("add_sheet Facts: {e}"))?;
            model
                .add_sheet("Report")
                .map_err(|e| anyhow::anyhow!("add_sheet Report: {e}"))?;

            let facts = 1u32;
            let report = 2u32;

            model
                .set_user_input(facts, 1, 1, "Region".to_string())
                .map_err(|e| anyhow::anyhow!("facts header A: {e}"))?;
            model
                .set_user_input(facts, 1, 2, "Product".to_string())
                .map_err(|e| anyhow::anyhow!("facts header B: {e}"))?;
            model
                .set_user_input(facts, 1, 3, "Channel".to_string())
                .map_err(|e| anyhow::anyhow!("facts header C: {e}"))?;
            model
                .set_user_input(facts, 1, 4, "Qty".to_string())
                .map_err(|e| anyhow::anyhow!("facts header D: {e}"))?;
            model
                .set_user_input(facts, 1, 5, "Price".to_string())
                .map_err(|e| anyhow::anyhow!("facts header E: {e}"))?;
            model
                .set_user_input(facts, 1, 6, "Revenue".to_string())
                .map_err(|e| anyhow::anyhow!("facts header F: {e}"))?;

            for i in 0..fact_rows {
                let row = i + 2;
                let rr = i32::try_from(row).with_context(|| "fact row overflow i32")?;
                let idx = i as usize;
                let region = regions[idx % regions.len()];
                let product = products[(idx / regions.len()) % products.len()];
                let channel = channels[(idx / (regions.len() * products.len())) % channels.len()];
                let qty = (i % 17) + 1;
                let price = (i % 23) + 10;

                model
                    .set_user_input(facts, rr, 1, region.to_string())
                    .map_err(|e| anyhow::anyhow!("facts region: {e}"))?;
                model
                    .set_user_input(facts, rr, 2, product.to_string())
                    .map_err(|e| anyhow::anyhow!("facts product: {e}"))?;
                model
                    .set_user_input(facts, rr, 3, channel.to_string())
                    .map_err(|e| anyhow::anyhow!("facts channel: {e}"))?;
                model
                    .set_user_input(facts, rr, 4, qty.to_string())
                    .map_err(|e| anyhow::anyhow!("facts qty: {e}"))?;
                model
                    .set_user_input(facts, rr, 5, price.to_string())
                    .map_err(|e| anyhow::anyhow!("facts price: {e}"))?;
                model
                    .set_user_input(facts, rr, 6, format!("=D{row}*E{row}"))
                    .map_err(|e| anyhow::anyhow!("facts revenue: {e}"))?;
            }

            model
                .set_user_input(report, 1, 1, "Region".to_string())
                .map_err(|e| anyhow::anyhow!("report header A: {e}"))?;
            model
                .set_user_input(report, 1, 2, "Product".to_string())
                .map_err(|e| anyhow::anyhow!("report header B: {e}"))?;
            model
                .set_user_input(report, 1, 3, "Channel".to_string())
                .map_err(|e| anyhow::anyhow!("report header C: {e}"))?;
            model
                .set_user_input(report, 1, 4, "Revenue".to_string())
                .map_err(|e| anyhow::anyhow!("report header D: {e}"))?;

            for i in 0..report_rows {
                let row = i + 2;
                let rr = i32::try_from(row).with_context(|| "report row overflow i32")?;
                let idx = i as usize;
                let region = regions[idx % regions.len()];
                let product = products[(idx / regions.len()) % products.len()];
                let channel = channels[(idx / (regions.len() * products.len())) % channels.len()];

                model
                    .set_user_input(report, rr, 1, region.to_string())
                    .map_err(|e| anyhow::anyhow!("report region: {e}"))?;
                model
                    .set_user_input(report, rr, 2, product.to_string())
                    .map_err(|e| anyhow::anyhow!("report product: {e}"))?;
                model
                    .set_user_input(report, rr, 3, channel.to_string())
                    .map_err(|e| anyhow::anyhow!("report channel: {e}"))?;
                model
                    .set_user_input(
                        report,
                        rr,
                        4,
                        format!(
                            "=SUMIFS(Facts!$F:$F,Facts!$A:$A,A{row},Facts!$B:$B,B{row},Facts!$C:$C,C{row})"
                        ),
                    )
                    .map_err(|e| anyhow::anyhow!("report sumifs: {e}"))?;
            }
        }
        "agg_countifs_multi_criteria_100k" => {
            let fact_rows = cfg_u32(scenario, "/sheets/0/rows", 100_000);
            let report_rows = cfg_u32(scenario, "/layout/report_rows", 1_000);
            let fact_last_row = fact_rows + 1;
            let report_countifs_formula = "=COUNTIFS(Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row},Facts!$E$2:$E${fact_last_row},\">=\"&E{row})";
            let regions = ["North", "South", "East", "West"];
            let products = ["A", "B", "C", "D", "E"];
            let channels = ["Online", "Retail", "Partner"];
            let statuses = ["Open", "Closed", "Pending", "Escalated"];
            let min_qty_cycle = [3_u32, 6, 9, 12];

            model
                .add_sheet("Facts")
                .map_err(|e| anyhow::anyhow!("add_sheet Facts: {e}"))?;
            model
                .add_sheet("Report")
                .map_err(|e| anyhow::anyhow!("add_sheet Report: {e}"))?;

            let facts = 1u32;
            let report = 2u32;

            model
                .set_user_input(facts, 1, 1, "Region".to_string())
                .map_err(|e| anyhow::anyhow!("countifs facts header A: {e}"))?;
            model
                .set_user_input(facts, 1, 2, "Product".to_string())
                .map_err(|e| anyhow::anyhow!("countifs facts header B: {e}"))?;
            model
                .set_user_input(facts, 1, 3, "Channel".to_string())
                .map_err(|e| anyhow::anyhow!("countifs facts header C: {e}"))?;
            model
                .set_user_input(facts, 1, 4, "Status".to_string())
                .map_err(|e| anyhow::anyhow!("countifs facts header D: {e}"))?;
            model
                .set_user_input(facts, 1, 5, "Qty".to_string())
                .map_err(|e| anyhow::anyhow!("countifs facts header E: {e}"))?;

            for i in 0..fact_rows {
                let row = i + 2;
                let rr = i32::try_from(row).with_context(|| "countifs fact row overflow i32")?;
                let idx = i as usize;
                let region = regions[idx % regions.len()];
                let product = products[(idx / regions.len()) % products.len()];
                let channel = channels[(idx / (regions.len() * products.len())) % channels.len()];
                let status = statuses
                    [(idx / (regions.len() * products.len() * channels.len())) % statuses.len()];
                let qty = ((i / 240) % 12) + 1;

                model
                    .set_user_input(facts, rr, 1, region.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs facts region: {e}"))?;
                model
                    .set_user_input(facts, rr, 2, product.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs facts product: {e}"))?;
                model
                    .set_user_input(facts, rr, 3, channel.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs facts channel: {e}"))?;
                model
                    .set_user_input(facts, rr, 4, status.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs facts status: {e}"))?;
                model
                    .set_user_input(facts, rr, 5, qty.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs facts qty: {e}"))?;
            }

            model
                .set_user_input(report, 1, 1, "Region".to_string())
                .map_err(|e| anyhow::anyhow!("countifs report header A: {e}"))?;
            model
                .set_user_input(report, 1, 2, "Product".to_string())
                .map_err(|e| anyhow::anyhow!("countifs report header B: {e}"))?;
            model
                .set_user_input(report, 1, 3, "Channel".to_string())
                .map_err(|e| anyhow::anyhow!("countifs report header C: {e}"))?;
            model
                .set_user_input(report, 1, 4, "Status".to_string())
                .map_err(|e| anyhow::anyhow!("countifs report header D: {e}"))?;
            model
                .set_user_input(report, 1, 5, "MinQty".to_string())
                .map_err(|e| anyhow::anyhow!("countifs report header E: {e}"))?;
            model
                .set_user_input(report, 1, 6, "Count".to_string())
                .map_err(|e| anyhow::anyhow!("countifs report header F: {e}"))?;

            for i in 0..report_rows {
                let row = i + 2;
                let rr = i32::try_from(row).with_context(|| "countifs report row overflow i32")?;
                let idx = i as usize;
                let region = regions[idx % regions.len()];
                let product = products[(idx / regions.len()) % products.len()];
                let channel = channels[(idx / (regions.len() * products.len())) % channels.len()];
                let status = statuses
                    [(idx / (regions.len() * products.len() * channels.len())) % statuses.len()];
                let min_qty = min_qty_cycle[idx % min_qty_cycle.len()];

                model
                    .set_user_input(report, rr, 1, region.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs report region: {e}"))?;
                model
                    .set_user_input(report, rr, 2, product.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs report product: {e}"))?;
                model
                    .set_user_input(report, rr, 3, channel.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs report channel: {e}"))?;
                model
                    .set_user_input(report, rr, 4, status.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs report status: {e}"))?;
                model
                    .set_user_input(report, rr, 5, min_qty.to_string())
                    .map_err(|e| anyhow::anyhow!("countifs report min qty: {e}"))?;
                model
                    .set_user_input(
                        report,
                        rr,
                        6,
                        render_formula_template(report_countifs_formula, row, Some(fact_last_row)),
                    )
                    .map_err(|e| anyhow::anyhow!("countifs report formula: {e}"))?;
            }
        }
        "agg_mixed_rollup_grid_2k_reports" => {
            let fact_rows = cfg_u32(scenario, "/sheets/0/rows", 10_000);
            let report_rows = cfg_u32(scenario, "/layout/report_rows", 500);
            let fact_last_row = fact_rows + 1;
            let facts_revenue_formula = "=E{row}*F{row}";
            let report_units_formula = "=SUMIFS(Facts!$E$2:$E${fact_last_row},Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})";
            let report_countifs_formula = "=COUNTIFS(Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})";
            let report_averageifs_formula = "=AVERAGEIFS(Facts!$F$2:$F${fact_last_row},Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})";
            let report_price_total_formula = "=SUMIFS(Facts!$F$2:$F${fact_last_row},Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})";
            let regions = ["North", "South", "East", "West"];
            let products = ["A", "B", "C", "D", "E"];
            let channels = ["Online", "Retail", "Partner"];
            let quarters = ["Q1", "Q2", "Q3", "Q4"];

            model
                .add_sheet("Facts")
                .map_err(|e| anyhow::anyhow!("add_sheet Facts: {e}"))?;
            model
                .add_sheet("Report")
                .map_err(|e| anyhow::anyhow!("add_sheet Report: {e}"))?;

            let facts = 1u32;
            let report = 2u32;

            model
                .set_user_input(facts, 1, 1, "Region".to_string())
                .map_err(|e| anyhow::anyhow!("mixed facts header A: {e}"))?;
            model
                .set_user_input(facts, 1, 2, "Product".to_string())
                .map_err(|e| anyhow::anyhow!("mixed facts header B: {e}"))?;
            model
                .set_user_input(facts, 1, 3, "Channel".to_string())
                .map_err(|e| anyhow::anyhow!("mixed facts header C: {e}"))?;
            model
                .set_user_input(facts, 1, 4, "Quarter".to_string())
                .map_err(|e| anyhow::anyhow!("mixed facts header D: {e}"))?;
            model
                .set_user_input(facts, 1, 5, "Units".to_string())
                .map_err(|e| anyhow::anyhow!("mixed facts header E: {e}"))?;
            model
                .set_user_input(facts, 1, 6, "Price".to_string())
                .map_err(|e| anyhow::anyhow!("mixed facts header F: {e}"))?;
            model
                .set_user_input(facts, 1, 7, "Revenue".to_string())
                .map_err(|e| anyhow::anyhow!("mixed facts header G: {e}"))?;

            for i in 0..fact_rows {
                let row = i + 2;
                let rr = i32::try_from(row).with_context(|| "mixed fact row overflow i32")?;
                let idx = i as usize;
                let region_idx = idx % regions.len();
                let product_idx = (idx / regions.len()) % products.len();
                let channel_idx = (idx / (regions.len() * products.len())) % channels.len();
                let quarter_idx =
                    (idx / (regions.len() * products.len() * channels.len())) % quarters.len();
                let units = ((i / 240) % 9) + 1 + region_idx as u32;
                let price = ((i / 2_160) % 15) + 10 + product_idx as u32 + quarter_idx as u32;

                model
                    .set_user_input(facts, rr, 1, regions[region_idx].to_string())
                    .map_err(|e| anyhow::anyhow!("mixed facts region: {e}"))?;
                model
                    .set_user_input(facts, rr, 2, products[product_idx].to_string())
                    .map_err(|e| anyhow::anyhow!("mixed facts product: {e}"))?;
                model
                    .set_user_input(facts, rr, 3, channels[channel_idx].to_string())
                    .map_err(|e| anyhow::anyhow!("mixed facts channel: {e}"))?;
                model
                    .set_user_input(facts, rr, 4, quarters[quarter_idx].to_string())
                    .map_err(|e| anyhow::anyhow!("mixed facts quarter: {e}"))?;
                model
                    .set_user_input(facts, rr, 5, units.to_string())
                    .map_err(|e| anyhow::anyhow!("mixed facts units: {e}"))?;
                model
                    .set_user_input(facts, rr, 6, price.to_string())
                    .map_err(|e| anyhow::anyhow!("mixed facts price: {e}"))?;
                model
                    .set_user_input(
                        facts,
                        rr,
                        7,
                        render_formula_template(facts_revenue_formula, row, None),
                    )
                    .map_err(|e| anyhow::anyhow!("mixed facts revenue: {e}"))?;
            }

            model
                .set_user_input(report, 1, 1, "Region".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header A: {e}"))?;
            model
                .set_user_input(report, 1, 2, "Product".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header B: {e}"))?;
            model
                .set_user_input(report, 1, 3, "Channel".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header C: {e}"))?;
            model
                .set_user_input(report, 1, 4, "Quarter".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header D: {e}"))?;
            model
                .set_user_input(report, 1, 5, "Units".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header E: {e}"))?;
            model
                .set_user_input(report, 1, 6, "Orders".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header F: {e}"))?;
            model
                .set_user_input(report, 1, 7, "AvgPrice".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header G: {e}"))?;
            model
                .set_user_input(report, 1, 8, "PriceTotal".to_string())
                .map_err(|e| anyhow::anyhow!("mixed report header H: {e}"))?;

            for i in 0..report_rows {
                let row = i + 2;
                let rr = i32::try_from(row).with_context(|| "mixed report row overflow i32")?;
                let idx = i as usize;
                let region = regions[idx % regions.len()];
                let product = products[(idx / regions.len()) % products.len()];
                let channel = channels[(idx / (regions.len() * products.len())) % channels.len()];
                let quarter = quarters
                    [(idx / (regions.len() * products.len() * channels.len())) % quarters.len()];

                model
                    .set_user_input(report, rr, 1, region.to_string())
                    .map_err(|e| anyhow::anyhow!("mixed report region: {e}"))?;
                model
                    .set_user_input(report, rr, 2, product.to_string())
                    .map_err(|e| anyhow::anyhow!("mixed report product: {e}"))?;
                model
                    .set_user_input(report, rr, 3, channel.to_string())
                    .map_err(|e| anyhow::anyhow!("mixed report channel: {e}"))?;
                model
                    .set_user_input(report, rr, 4, quarter.to_string())
                    .map_err(|e| anyhow::anyhow!("mixed report quarter: {e}"))?;
                model
                    .set_user_input(
                        report,
                        rr,
                        5,
                        render_formula_template(report_units_formula, row, Some(fact_last_row)),
                    )
                    .map_err(|e| anyhow::anyhow!("mixed report units formula: {e}"))?;
                model
                    .set_user_input(
                        report,
                        rr,
                        6,
                        render_formula_template(report_countifs_formula, row, Some(fact_last_row)),
                    )
                    .map_err(|e| anyhow::anyhow!("mixed report count formula: {e}"))?;
                model
                    .set_user_input(
                        report,
                        rr,
                        7,
                        render_formula_template(
                            report_averageifs_formula,
                            row,
                            Some(fact_last_row),
                        ),
                    )
                    .map_err(|e| anyhow::anyhow!("mixed report average formula: {e}"))?;
                model
                    .set_user_input(
                        report,
                        rr,
                        8,
                        render_formula_template(
                            report_price_total_formula,
                            row,
                            Some(fact_last_row),
                        ),
                    )
                    .map_err(|e| anyhow::anyhow!("mixed report price total formula: {e}"))?;
            }
        }
        other => {
            bail!("no fallback builder implemented for scenario id: {other}");
        }
    }

    Ok(model)
}

#[cfg(feature = "ironcalc_runner")]
fn resolve_output_path(root: &Path, workbook_path: &str) -> PathBuf {
    let p = PathBuf::from(workbook_path);
    if p.is_absolute() { p } else { root.join(p) }
}

#[cfg(feature = "ironcalc_runner")]
fn arg_str(op: &Operation, key: &str) -> Result<String> {
    op.args
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
        .with_context(|| format!("op={} missing string arg: {}", op.op, key))
}

#[cfg(feature = "ironcalc_runner")]
fn arg_u32(op: &Operation, key: &str) -> Result<u32> {
    op.args
        .get(key)
        .and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .with_context(|| format!("op={} missing u32 arg: {}", op.op, key))
}

#[cfg(feature = "ironcalc_runner")]
fn arg_as_ironcalc_input(op: &Operation, key: &str) -> Result<String> {
    let v = op
        .args
        .get(key)
        .with_context(|| format!("op={} missing arg: {}", op.op, key))?;

    if let Some(i) = v.as_i64() {
        return Ok(i.to_string());
    }
    if let Some(f) = v.as_f64() {
        return Ok(f.to_string());
    }
    if let Some(b) = v.as_bool() {
        return Ok(if b {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        });
    }
    if let Some(s) = v.as_str() {
        return Ok(s.to_string());
    }
    if v.is_null() {
        return Ok(String::new());
    }

    bail!("unsupported value arg type for op={}: {}", op.op, v)
}

#[cfg(feature = "ironcalc_runner")]
fn sheet_index_by_name(model: &ironcalc::base::Model<'_>, name: &str) -> Result<u32> {
    model
        .workbook
        .get_worksheet_names()
        .iter()
        .position(|n| n == name)
        .map(|i| i as u32)
        .with_context(|| format!("sheet not found: {name}"))
}

#[cfg(feature = "ironcalc_runner")]
fn parse_cell_ref(cell_ref: &str) -> Result<(String, i32, i32)> {
    let (sheet, a1) = if let Some((sheet, a1)) = cell_ref.split_once('!') {
        (sheet.to_string(), a1)
    } else {
        ("Sheet1".to_string(), cell_ref)
    };

    let mut col: i32 = 0;
    let mut row_str = String::new();
    for ch in a1.chars() {
        if ch.is_ascii_alphabetic() {
            let up = ch.to_ascii_uppercase() as u8;
            let n = (up - b'A' + 1) as i32;
            col = col * 26 + n;
        } else if ch.is_ascii_digit() {
            row_str.push(ch);
        }
    }

    let row: i32 = row_str
        .parse()
        .with_context(|| format!("invalid row in A1 ref: {}", cell_ref))?;

    if row <= 0 || col <= 0 {
        bail!("invalid A1 ref: {}", cell_ref);
    }

    Ok((sheet, row, col))
}

#[cfg(feature = "ironcalc_runner")]
fn parse_ironcalc_number(s: &str) -> Option<f64> {
    let compact = s.replace(',', "");
    compact.parse::<f64>().ok()
}

#[cfg(feature = "ironcalc_runner")]
fn is_ironcalc_error_text(s: &str) -> bool {
    let u = s.trim().to_ascii_uppercase();
    u.starts_with('#')
}

#[cfg(feature = "ironcalc_runner")]
fn verify_correctness(
    model: &ironcalc::base::Model<'_>,
    scenario: &Scenario,
    root: &Path,
) -> Result<CorrectnessResult> {
    let mut mismatches = 0u64;
    let mut details = Vec::new();
    let expected_values = scenario.verify.expected_values(root)?;
    let formula_checks = scenario.verify.formula_checks(root)?;

    for (cell_ref, expected) in &expected_values {
        let (sheet_name, row, col) = parse_cell_ref(cell_ref)?;
        let sheet = sheet_index_by_name(model, &sheet_name)?;
        let actual = model
            .get_formatted_cell_value(sheet, row, col)
            .map_err(|e| anyhow::anyhow!("get_formatted_cell_value {cell_ref}: {e}"))?;

        let matches = if let Some(n) = expected.as_f64() {
            parse_ironcalc_number(&actual)
                .map(|v| (v - n).abs() < 1e-9)
                .unwrap_or(false)
        } else if let Some(i) = expected.as_i64() {
            parse_ironcalc_number(&actual)
                .map(|v| (v - (i as f64)).abs() < 1e-9)
                .unwrap_or(false)
        } else if let Some(b) = expected.as_bool() {
            let au = actual.trim().to_ascii_uppercase();
            (b && au == "TRUE") || (!b && au == "FALSE")
        } else if let Some(s) = expected.as_str() {
            actual == s
        } else if expected.is_null() {
            actual.is_empty()
        } else {
            false
        };

        if !matches {
            mismatches += 1;
            details.push(format!(
                "expected mismatch at {cell_ref}: expected={expected}, actual={actual}"
            ));
        }
    }

    for check in &formula_checks {
        if check.check_type == "non_error" {
            let (sheet_name, row, col) = parse_cell_ref(&check.cell)?;
            let sheet = sheet_index_by_name(model, &sheet_name)?;
            let actual = model
                .get_formatted_cell_value(sheet, row, col)
                .map_err(|e| anyhow::anyhow!("formula check {}: {e}", check.cell))?;
            if is_ironcalc_error_text(&actual) {
                mismatches += 1;
                details.push(format!(
                    "formula check non_error failed at {} ({actual})",
                    check.cell
                ));
            }
        }
    }

    Ok(CorrectnessResult {
        passed: mismatches == 0,
        mismatches,
        details,
    })
}
