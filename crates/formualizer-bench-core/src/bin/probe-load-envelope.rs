#[cfg(feature = "formualizer_runner")]
use std::{
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "formualizer_runner")]
use anyhow::{Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use serde::Serialize;

#[cfg(feature = "formualizer_runner")]
use formualizer_testkit::write_workbook;
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookLoadLimits,
};

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --bin probe-load-envelope -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    let report = run(cli)?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Parser)]
#[command(about = "Generate and probe large-workbook load/eval envelopes")]
struct Cli {
    #[arg(long, value_enum)]
    scenario: ScenarioKind,

    #[arg(long, value_enum, default_value_t = BackendKind::Umya)]
    backend: BackendKind,

    /// Logical row count for the primary large sheet.
    #[arg(long)]
    rows: u32,

    /// Logical column count for the primary large sheet.
    #[arg(long)]
    logical_cols: u32,

    /// Number of report rows for report-style scenarios.
    #[arg(long, default_value_t = 256)]
    report_rows: u32,

    /// Number of actively populated numeric columns for the linear rollup scenario.
    #[arg(long, default_value_t = 4)]
    active_cols: u32,

    /// Workbook logical cell budget used for the load probe.
    #[arg(long, default_value_t = 128_000_000)]
    logical_cell_budget: u64,

    /// Sparse-sheet guard threshold used for the load probe.
    #[arg(long, default_value_t = 250_000)]
    sparse_sheet_threshold: u64,

    /// Max logical/populated ratio used for the load probe.
    #[arg(long, default_value_t = 1_024)]
    max_sparse_ratio: u64,

    /// Advisory per-phase threshold for load/evaluate timing.
    #[arg(long, default_value_t = 60)]
    timeout_seconds: u64,

    /// Optional path for the generated workbook. If omitted, a temp path is used.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Measure an existing workbook instead of generating one first.
    #[arg(long)]
    input: Option<PathBuf>,

    /// Generate the workbook and exit without loading/evaluating it.
    #[arg(long)]
    generate_only: bool,

    /// Keep the generated workbook on disk even when using a temp output path.
    #[arg(long)]
    keep_workbook: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ScenarioKind {
    LinearRollup,
    SumifsReport,
    WholeColumnSummary,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, ValueEnum)]
enum BackendKind {
    Umya,
    Calamine,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ProbeReport {
    backend: &'static str,
    scenario: &'static str,
    workbook_path: String,
    logical_rows: u32,
    logical_cols: u32,
    logical_cells: u64,
    populated_cells_estimate: u64,
    advisory_timeout_seconds: u64,
    status: &'static str,
    generation_ms: f64,
    load_ms: Option<f64>,
    evaluate_ms: Option<f64>,
    load_within_budget: Option<bool>,
    evaluate_within_budget: Option<bool>,
    error: Option<String>,
}

#[cfg(feature = "formualizer_runner")]
fn run(cli: Cli) -> Result<ProbeReport> {
    let logical_cols = cli.logical_cols.max(1);
    let logical_rows = cli.rows.max(1);
    let using_temp_output = cli.output.is_none() && cli.input.is_none();
    let output = match cli.input.clone().or_else(|| cli.output.clone()) {
        Some(path) => path,
        None => temp_workbook_path(cli.scenario, logical_rows, logical_cols),
    };

    let (populated_cells_estimate, generation_ms) = if cli.input.is_some() {
        (0, 0.0)
    } else {
        eprintln!(
            "[probe] generating backend={} scenario={} rows={} logical_cols={} path={}",
            cli.backend.label(),
            cli.scenario.label(),
            logical_rows,
            logical_cols,
            output.display()
        );
        let start = Instant::now();
        let populated = generate_workbook(&output, &cli)?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        eprintln!("[probe] generated in {:.1} ms", elapsed);
        (populated, elapsed)
    };

    eprintln!(
        "[probe] measuring backend={} scenario={} rows={} logical_cols={} path={}",
        cli.backend.label(),
        cli.scenario.label(),
        logical_rows,
        logical_cols,
        output.display()
    );

    if cli.generate_only {
        return Ok(ProbeReport {
            backend: cli.backend.label(),
            scenario: cli.scenario.label(),
            workbook_path: output.display().to_string(),
            logical_rows,
            logical_cols,
            logical_cells: u64::from(logical_rows) * u64::from(logical_cols),
            populated_cells_estimate,
            advisory_timeout_seconds: cli.timeout_seconds,
            status: "generated",
            generation_ms,
            load_ms: None,
            evaluate_ms: None,
            load_within_budget: None,
            evaluate_within_budget: None,
            error: None,
        });
    }

    let limits = WorkbookLoadLimits {
        max_sheet_rows: 1_048_576,
        max_sheet_cols: 16_384,
        max_sheet_logical_cells: cli.logical_cell_budget,
        sparse_sheet_cell_threshold: cli.sparse_sheet_threshold,
        max_sparse_cell_ratio: cli.max_sparse_ratio,
    };

    let load_start = Instant::now();
    eprintln!("[probe] opening workbook backend={}", cli.backend.label());

    let mut workbook = match cli.backend {
        BackendKind::Umya => {
            let backend = match UmyaAdapter::open_path(&output) {
                Ok(backend) => backend,
                Err(err) => {
                    return Ok(ProbeReport {
                        backend: cli.backend.label(),
                        scenario: cli.scenario.label(),
                        workbook_path: output.display().to_string(),
                        logical_rows,
                        logical_cols,
                        logical_cells: u64::from(logical_rows) * u64::from(logical_cols),
                        populated_cells_estimate,
                        advisory_timeout_seconds: cli.timeout_seconds,
                        status: "open_error",
                        generation_ms,
                        load_ms: None,
                        evaluate_ms: None,
                        load_within_budget: None,
                        evaluate_within_budget: None,
                        error: Some(err.to_string()),
                    });
                }
            };

            eprintln!("[probe] loading workbook into engine");
            match Workbook::from_reader(
                backend,
                LoadStrategy::EagerAll,
                formualizer_workbook::WorkbookConfig::ephemeral()
                    .with_ingest_limits(limits.clone()),
            ) {
                Ok(wb) => wb,
                Err(err) => {
                    return Ok(ProbeReport {
                        backend: cli.backend.label(),
                        scenario: cli.scenario.label(),
                        workbook_path: output.display().to_string(),
                        logical_rows,
                        logical_cols,
                        logical_cells: u64::from(logical_rows) * u64::from(logical_cols),
                        populated_cells_estimate,
                        advisory_timeout_seconds: cli.timeout_seconds,
                        status: "load_error",
                        generation_ms,
                        load_ms: Some(load_start.elapsed().as_secs_f64() * 1000.0),
                        evaluate_ms: None,
                        load_within_budget: None,
                        evaluate_within_budget: None,
                        error: Some(err.to_string()),
                    });
                }
            }
        }
        BackendKind::Calamine => {
            let backend = match CalamineAdapter::open_path(&output) {
                Ok(backend) => backend,
                Err(err) => {
                    return Ok(ProbeReport {
                        backend: cli.backend.label(),
                        scenario: cli.scenario.label(),
                        workbook_path: output.display().to_string(),
                        logical_rows,
                        logical_cols,
                        logical_cells: u64::from(logical_rows) * u64::from(logical_cols),
                        populated_cells_estimate,
                        advisory_timeout_seconds: cli.timeout_seconds,
                        status: "open_error",
                        generation_ms,
                        load_ms: None,
                        evaluate_ms: None,
                        load_within_budget: None,
                        evaluate_within_budget: None,
                        error: Some(err.to_string()),
                    });
                }
            };

            eprintln!("[probe] loading workbook into engine");
            match Workbook::from_reader(
                backend,
                LoadStrategy::EagerAll,
                formualizer_workbook::WorkbookConfig::ephemeral()
                    .with_ingest_limits(limits.clone()),
            ) {
                Ok(wb) => wb,
                Err(err) => {
                    return Ok(ProbeReport {
                        backend: cli.backend.label(),
                        scenario: cli.scenario.label(),
                        workbook_path: output.display().to_string(),
                        logical_rows,
                        logical_cols,
                        logical_cells: u64::from(logical_rows) * u64::from(logical_cols),
                        populated_cells_estimate,
                        advisory_timeout_seconds: cli.timeout_seconds,
                        status: "load_error",
                        generation_ms,
                        load_ms: Some(load_start.elapsed().as_secs_f64() * 1000.0),
                        evaluate_ms: None,
                        load_within_budget: None,
                        evaluate_within_budget: None,
                        error: Some(err.to_string()),
                    });
                }
            }
        }
    };
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("[probe] load complete in {:.1} ms", load_ms);

    let eval_start = Instant::now();
    eprintln!("[probe] evaluating workbook");
    if let Err(err) = workbook.evaluate_all() {
        return Ok(ProbeReport {
            backend: cli.backend.label(),
            scenario: cli.scenario.label(),
            workbook_path: output.display().to_string(),
            logical_rows,
            logical_cols,
            logical_cells: u64::from(logical_rows) * u64::from(logical_cols),
            populated_cells_estimate,
            advisory_timeout_seconds: cli.timeout_seconds,
            status: "evaluate_error",
            generation_ms,
            load_ms: Some(load_ms),
            evaluate_ms: Some(eval_start.elapsed().as_secs_f64() * 1000.0),
            load_within_budget: Some(load_ms <= cli.timeout_seconds as f64 * 1000.0),
            evaluate_within_budget: None,
            error: Some(err.to_string()),
        });
    }
    let evaluate_ms = eval_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("[probe] evaluation complete in {:.1} ms", evaluate_ms);

    if !cli.keep_workbook && using_temp_output {
        let _ = std::fs::remove_file(&output);
    }

    Ok(ProbeReport {
        backend: cli.backend.label(),
        scenario: cli.scenario.label(),
        workbook_path: output.display().to_string(),
        logical_rows,
        logical_cols,
        logical_cells: u64::from(logical_rows) * u64::from(logical_cols),
        populated_cells_estimate,
        advisory_timeout_seconds: cli.timeout_seconds,
        status: if load_ms > cli.timeout_seconds as f64 * 1000.0
            || evaluate_ms > cli.timeout_seconds as f64 * 1000.0
        {
            "threshold_exceeded"
        } else {
            "ok"
        },
        generation_ms,
        load_ms: Some(load_ms),
        evaluate_ms: Some(evaluate_ms),
        load_within_budget: Some(load_ms <= cli.timeout_seconds as f64 * 1000.0),
        evaluate_within_budget: Some(evaluate_ms <= cli.timeout_seconds as f64 * 1000.0),
        error: None,
    })
}

#[cfg(feature = "formualizer_runner")]
impl ScenarioKind {
    fn label(self) -> &'static str {
        match self {
            ScenarioKind::LinearRollup => "linear_rollup",
            ScenarioKind::SumifsReport => "sumifs_report",
            ScenarioKind::WholeColumnSummary => "whole_column_summary",
        }
    }
}

#[cfg(feature = "formualizer_runner")]
impl BackendKind {
    fn label(self) -> &'static str {
        match self {
            BackendKind::Umya => "umya",
            BackendKind::Calamine => "calamine",
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn temp_workbook_path(scenario: ScenarioKind, rows: u32, cols: u32) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "formualizer-{}-{}x{}-{}-{}.xlsx",
        scenario.label(),
        rows,
        cols,
        std::process::id(),
        stamp,
    ))
}

#[cfg(feature = "formualizer_runner")]
fn generate_workbook(path: &Path, cli: &Cli) -> Result<u64> {
    match cli.scenario {
        ScenarioKind::LinearRollup => {
            generate_linear_rollup(path, cli.rows, cli.logical_cols, cli.active_cols)
        }
        ScenarioKind::SumifsReport => {
            generate_sumifs_report(path, cli.rows, cli.logical_cols, cli.report_rows)
        }
        ScenarioKind::WholeColumnSummary => {
            generate_whole_column_summary(path, cli.rows, cli.logical_cols, cli.report_rows)
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn generate_linear_rollup(
    path: &Path,
    rows: u32,
    logical_cols: u32,
    active_cols: u32,
) -> Result<u64> {
    let active_cols = active_cols.clamp(2, logical_cols.max(2));
    let mut populated = 0u64;
    write_workbook(path, |book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        for row in 1..=rows {
            sheet
                .get_cell_mut((1, row))
                .set_value_number((row as f64 % 10_000.0) + 1.0);
            populated += 1;

            for col in 2..=active_cols {
                let formula = match col {
                    2 => format!("=A{row}*2"),
                    3 => format!("=B{row}+5"),
                    _ => format!("={}{}+A{}", col_name(col - 1), row, row),
                };
                sheet.get_cell_mut((col, row)).set_formula(formula);
                populated += 1;
            }
        }

        // Sentinel forces the logical rectangle to the requested width/height.
        if logical_cols > active_cols {
            sheet
                .get_cell_mut((logical_cols, rows))
                .set_value_number((rows as f64) * 0.001);
            populated += 1;
        }
    });
    Ok(populated)
}

#[cfg(feature = "formualizer_runner")]
fn generate_sumifs_report(
    path: &Path,
    rows: u32,
    logical_cols: u32,
    report_rows: u32,
) -> Result<u64> {
    if logical_cols < 6 {
        bail!("sumifs_report requires logical_cols >= 6");
    }

    let regions = ["North", "South", "East", "West"];
    let products = ["A", "B", "C", "D", "E"];
    let channels = ["Online", "Retail", "Partner"];
    let mut populated = 0u64;

    write_workbook(path, |book| {
        {
            let facts = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            facts.set_name("Facts");
        }
        book.new_sheet("Report").expect("report sheet");

        {
            let facts = book.get_sheet_by_name_mut("Facts").expect("Facts exists");
            for (col, header) in ["Region", "Product", "Channel", "Units", "Price", "Revenue"]
                .iter()
                .enumerate()
            {
                facts.get_cell_mut(((col + 1) as u32, 1)).set_value(*header);
                populated += 1;
            }

            for row in 2..=rows + 1 {
                let idx = (row - 2) as usize;
                facts
                    .get_cell_mut((1, row))
                    .set_value(regions[idx % regions.len()]);
                facts
                    .get_cell_mut((2, row))
                    .set_value(products[idx % products.len()]);
                facts
                    .get_cell_mut((3, row))
                    .set_value(channels[idx % channels.len()]);
                facts
                    .get_cell_mut((4, row))
                    .set_value_number(((idx % 97) + 1) as f64);
                facts
                    .get_cell_mut((5, row))
                    .set_value_number((((idx * 7) % 37) + 10) as f64);
                facts
                    .get_cell_mut((6, row))
                    .set_formula(format!("=D{row}*E{row}"));
                populated += 6;
            }

            if logical_cols > 6 {
                facts
                    .get_cell_mut((logical_cols, rows + 1))
                    .set_value_number(1.0);
                populated += 1;
            }
        }

        {
            let report = book.get_sheet_by_name_mut("Report").expect("Report exists");
            for (col, header) in ["Region", "Product", "Channel", "Revenue"]
                .iter()
                .enumerate()
            {
                report
                    .get_cell_mut(((col + 1) as u32, 1))
                    .set_value(*header);
                populated += 1;
            }

            for row in 2..=report_rows + 1 {
                let idx = (row - 2) as usize;
                report
                    .get_cell_mut((1, row))
                    .set_value(regions[idx % regions.len()]);
                report
                    .get_cell_mut((2, row))
                    .set_value(products[idx % products.len()]);
                report
                    .get_cell_mut((3, row))
                    .set_value(channels[idx % channels.len()]);
                report.get_cell_mut((4, row)).set_formula(format!(
                    "=SUMIFS(Facts!$F:$F,Facts!$A:$A,A{row},Facts!$B:$B,B{row},Facts!$C:$C,C{row})"
                ));
                populated += 4;
            }
        }
    });

    Ok(populated)
}

#[cfg(feature = "formualizer_runner")]
fn generate_whole_column_summary(
    path: &Path,
    rows: u32,
    logical_cols: u32,
    report_rows: u32,
) -> Result<u64> {
    if logical_cols < 2 {
        bail!("whole_column_summary requires logical_cols >= 2");
    }

    let categories = ["North", "South", "East", "West"];
    let mut populated = 0u64;

    write_workbook(path, |book| {
        {
            let data = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            data.set_name("Data");
        }
        book.new_sheet("Summary").expect("summary sheet");

        {
            let data = book.get_sheet_by_name_mut("Data").expect("Data exists");
            data.get_cell_mut((1, 1)).set_value("Value");
            data.get_cell_mut((2, 1)).set_value("Category");
            populated += 2;

            for row in 2..=rows + 1 {
                let idx = (row - 2) as usize;
                data.get_cell_mut((1, row))
                    .set_value_number(((idx % 10_000) + 1) as f64);
                data.get_cell_mut((2, row))
                    .set_value(categories[idx % categories.len()]);
                populated += 2;
            }

            if logical_cols > 2 {
                data.get_cell_mut((logical_cols, rows + 1))
                    .set_value_number(1.0);
                populated += 1;
            }
        }

        {
            let summary = book
                .get_sheet_by_name_mut("Summary")
                .expect("Summary exists");
            summary.get_cell_mut((1, 1)).set_value("Category");
            summary.get_cell_mut((2, 1)).set_value("TotalValue");
            summary.get_cell_mut((3, 1)).set_value("RowCount");
            populated += 3;

            for row in 2..=report_rows + 1 {
                let category = categories[((row - 2) as usize) % categories.len()];
                summary.get_cell_mut((1, row)).set_value(category);
                summary
                    .get_cell_mut((2, row))
                    .set_formula(format!("=SUMIFS(Data!$A:$A,Data!$B:$B,A{row})"));
                summary
                    .get_cell_mut((3, row))
                    .set_formula(format!("=COUNTIF(Data!$B:$B,A{row})"));
                populated += 3;
            }
        }
    });

    Ok(populated)
}

#[cfg(feature = "formualizer_runner")]
fn col_name(mut col: u32) -> String {
    let mut out = String::new();
    while col > 0 {
        let rem = ((col - 1) % 26) as u8;
        out.insert(0, (b'A' + rem) as char);
        col = (col - 1) / 26;
    }
    out
}
