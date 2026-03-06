use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail};
use clap::Parser;
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
}

#[cfg(feature = "formualizer_runner")]
fn run() -> Result<()> {
    use formualizer_workbook::{
        LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
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

    let load_start = Instant::now();
    let backend = UmyaAdapter::open_path(&workbook_path)
        .map_err(|e| anyhow::anyhow!("open workbook via umya: {e}"))?;
    let mut wb =
        Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .map_err(|e| anyhow::anyhow!("load workbook into engine: {e}"))?;
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;

    let mut full_eval_ms: Option<f64> = None;
    let mut incremental_us: Option<f64> = None;

    for op in &scenario.operations {
        match op.op.as_str() {
            "load" => {}
            "evaluate_all" => {
                let t0 = Instant::now();
                wb.evaluate_all()
                    .map_err(|e| anyhow::anyhow!("evaluate_all: {e}"))?;
                full_eval_ms = Some(t0.elapsed().as_secs_f64() * 1000.0);
            }
            "evaluate_incremental" => {
                let t0 = Instant::now();
                wb.evaluate_all()
                    .map_err(|e| anyhow::anyhow!("evaluate_incremental/evaluate_all: {e}"))?;
                incremental_us = Some(t0.elapsed().as_secs_f64() * 1_000_000.0);
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
            }
            "add_sheet" => {
                let sheet = arg_str(op, "sheet")?;
                wb.add_sheet(&sheet)
                    .map_err(|e| anyhow::anyhow!("add_sheet: {e}"))?;
            }
            "remove_sheet" => {
                let sheet = arg_str(op, "sheet")?;
                wb.delete_sheet(&sheet)
                    .map_err(|e| anyhow::anyhow!("delete_sheet: {e}"))?;
            }
            "insert_rows" => {
                let sheet = arg_str(op, "sheet")?;
                let before = arg_u32(op, "before")?;
                let count = arg_u32(op, "count")?;
                wb.engine_mut()
                    .insert_rows(&sheet, before, count)
                    .map_err(|e| anyhow::anyhow!("insert_rows: {e}"))?;
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
            }
            "read_cells" => {}
            unsupported => bail!("unsupported op in native adapter: {unsupported}"),
        }
    }

    let correctness = verify_correctness(&mut wb, scenario)?;
    let status = if correctness.passed { "ok" } else { "invalid" }.to_string();

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
            extra: BTreeMap::new(),
        },
        correctness,
        notes: vec![],
        timestamp: chrono::Utc::now().to_rfc3339(),
        meta: BTreeMap::new(),
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
) -> Result<CorrectnessResult> {
    use formualizer_workbook::LiteralValue;

    let mut mismatches = 0u64;
    let mut details = Vec::new();

    for (cell_ref, expected) in &scenario.verify.expected {
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

    for check in &scenario.verify.formula_checks {
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
