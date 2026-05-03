#[cfg(feature = "formualizer_runner")]
use std::{path::PathBuf, time::Instant};

#[cfg(feature = "formualizer_runner")]
use anyhow::{Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::Parser;
#[cfg(feature = "formualizer_runner")]
use formualizer_testkit::write_workbook;
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
#[cfg(feature = "formualizer_runner")]
use serde::Serialize;

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --bin probe-finance-recalc -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    let report = run_probe(&cli)?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
#[command(about = "Finance-shaped repeated edit/recalc probe for Formualizer native runner")]
struct Cli {
    #[arg(long, default_value_t = 50_000)]
    rows: usize,
    #[arg(long, default_value_t = 10)]
    cycles: usize,
    #[arg(long, default_value_t = 16)]
    dense_edit_len: usize,
    #[arg(long, default_value_t = 16)]
    sparse_edits: usize,
    #[arg(long, default_value = "phase-candidate")]
    label: String,
    /// Optional XLSX fixture path. Defaults under target/finance-recalc-fixtures/.
    #[arg(long)]
    workbook_path: Option<PathBuf>,
    /// Reuse --workbook-path if it already exists instead of regenerating it.
    #[arg(long)]
    reuse_workbook: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct FinanceRecalcProbeReport {
    label: String,
    rows: usize,
    cycles: usize,
    dense_edit_len: usize,
    sparse_edits: usize,
    workbook_path: String,
    reused_workbook: bool,
    fixture_gen_ms: f64,
    load_ms: f64,
    setup_ms: f64,
    initial_eval_ms: f64,
    total_recalc_ms: f64,
    recalc_ms_p50: f64,
    recalc_ms_p95: f64,
    recalc_ms_max: f64,
    final_rollup: f64,
    expected_final_rollup: f64,
    checksum: f64,
    rss_current_mb: Option<f64>,
    rss_peak_mb: Option<f64>,
    cycles_detail: Vec<FinanceRecalcCycleReport>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct FinanceRecalcCycleReport {
    cycle: usize,
    edit_kind: &'static str,
    edit_ms: f64,
    recalc_ms: f64,
    rollup: f64,
    expected_rollup: f64,
}

#[cfg(feature = "formualizer_runner")]
struct FinanceProbeWorkbook {
    workbook: Workbook,
    units: Vec<f64>,
    prices: Vec<f64>,
    multiplier: f64,
}

#[cfg(feature = "formualizer_runner")]
fn run_probe(cli: &Cli) -> Result<FinanceRecalcProbeReport> {
    if cli.rows == 0 {
        bail!("--rows must be > 0");
    }
    if cli.cycles == 0 {
        bail!("--cycles must be > 0");
    }

    let workbook_path = cli
        .workbook_path
        .clone()
        .unwrap_or_else(|| default_workbook_path(&cli.label, cli.rows));

    let (fixture_gen_ms, reused_workbook) = if cli.reuse_workbook && workbook_path.exists() {
        (0.0, true)
    } else {
        let gen_start = Instant::now();
        generate_finance_fixture(&workbook_path, cli.rows)?;
        (gen_start.elapsed().as_secs_f64() * 1000.0, false)
    };

    let load_start = Instant::now();
    let mut probe = FinanceProbeWorkbook::load(&workbook_path, cli.rows)?;
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;
    let setup_ms = fixture_gen_ms + load_ms;

    let initial_start = Instant::now();
    probe
        .workbook
        .evaluate_all()
        .map_err(|e| anyhow::anyhow!("initial evaluate_all: {e}"))?;
    let initial_eval_ms = initial_start.elapsed().as_secs_f64() * 1000.0;
    probe.assert_rollup("initial")?;

    let mut cycles_detail = Vec::with_capacity(cli.cycles);
    for cycle in 0..cli.cycles {
        let edit_start = Instant::now();
        let edit_kind = probe.apply_cycle_edit(cycle, cli.dense_edit_len, cli.sparse_edits)?;
        let edit_ms = edit_start.elapsed().as_secs_f64() * 1000.0;

        let recalc_start = Instant::now();
        probe
            .workbook
            .evaluate_all()
            .map_err(|e| anyhow::anyhow!("evaluate_all cycle {cycle}: {e}"))?;
        let recalc_ms = recalc_start.elapsed().as_secs_f64() * 1000.0;

        let expected_rollup = probe.expected_rollup();
        let rollup = probe.rollup()?;
        if (rollup - expected_rollup).abs() > 1e-6 {
            bail!("cycle {cycle}: rollup mismatch: got {rollup}, expected {expected_rollup}");
        }
        cycles_detail.push(FinanceRecalcCycleReport {
            cycle,
            edit_kind,
            edit_ms,
            recalc_ms,
            rollup,
            expected_rollup,
        });
    }

    let mut recalc_times: Vec<f64> = cycles_detail.iter().map(|cycle| cycle.recalc_ms).collect();
    recalc_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let total_recalc_ms = recalc_times.iter().sum();
    let recalc_ms_p50 = percentile(&recalc_times, 0.50);
    let recalc_ms_p95 = percentile(&recalc_times, 0.95);
    let recalc_ms_max = recalc_times.last().copied().unwrap_or(0.0);
    let final_rollup = probe.rollup()?;
    let expected_final_rollup = probe.expected_rollup();
    let checksum = cycles_detail
        .iter()
        .map(|cycle| cycle.rollup)
        .fold(final_rollup, |acc, value| acc + value);
    let (rss_current_mb, rss_peak_mb) = linux_rss_mb();

    Ok(FinanceRecalcProbeReport {
        label: cli.label.clone(),
        rows: cli.rows,
        cycles: cli.cycles,
        dense_edit_len: cli.dense_edit_len,
        sparse_edits: cli.sparse_edits,
        workbook_path: workbook_path.display().to_string(),
        reused_workbook,
        fixture_gen_ms,
        load_ms,
        setup_ms,
        initial_eval_ms,
        total_recalc_ms,
        recalc_ms_p50,
        recalc_ms_p95,
        recalc_ms_max,
        final_rollup,
        expected_final_rollup,
        checksum,
        rss_current_mb,
        rss_peak_mb,
        cycles_detail,
    })
}

#[cfg(feature = "formualizer_runner")]
impl FinanceProbeWorkbook {
    fn load(path: &PathBuf, rows: usize) -> Result<Self> {
        let backend = UmyaAdapter::open_path(path)
            .map_err(|e| anyhow::anyhow!("open fixture via umya {}: {e}", path.display()))?;
        let workbook =
            Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
                .map_err(|e| anyhow::anyhow!("load fixture into workbook: {e}"))?;
        let (units, prices) = deterministic_inputs(rows);
        Ok(Self {
            workbook,
            units,
            prices,
            multiplier: 1.0,
        })
    }

    fn apply_cycle_edit(
        &mut self,
        cycle: usize,
        dense_edit_len: usize,
        sparse_edits: usize,
    ) -> Result<&'static str> {
        match cycle % 3 {
            0 => {
                self.multiplier = 1.0 + ((cycle % 5) as f64);
                self.workbook
                    .set_value("Sheet1", 1, 6, LiteralValue::Number(self.multiplier))
                    .map_err(|e| anyhow::anyhow!("set multiplier cycle {cycle}: {e}"))?;
                Ok("multiplier")
            }
            1 => {
                let len = dense_edit_len.min(self.units.len()).max(1);
                let start = (cycle * 37) % self.units.len();
                for idx in 0..len {
                    let row0 = (start + idx) % self.units.len();
                    let value = 1000.0 + cycle as f64 + idx as f64;
                    self.units[row0] = value;
                    self.workbook
                        .set_value("Sheet1", row0 as u32 + 1, 1, LiteralValue::Number(value))
                        .map_err(|e| anyhow::anyhow!("set dense unit cycle {cycle}: {e}"))?;
                }
                Ok("dense_units")
            }
            _ => {
                let edits = sparse_edits.min(self.prices.len()).max(1);
                for idx in 0..edits {
                    let row0 = (cycle * 53 + idx * 97) % self.prices.len();
                    let value = 20.0 + ((cycle + idx) % 23) as f64;
                    self.prices[row0] = value;
                    self.workbook
                        .set_value("Sheet1", row0 as u32 + 1, 2, LiteralValue::Number(value))
                        .map_err(|e| anyhow::anyhow!("set sparse price cycle {cycle}: {e}"))?;
                }
                Ok("sparse_prices")
            }
        }
    }

    fn expected_rollup(&self) -> f64 {
        self.units
            .iter()
            .zip(self.prices.iter())
            .map(|(unit, price)| unit * price * self.multiplier)
            .sum()
    }

    fn rollup(&self) -> Result<f64> {
        match self.workbook.get_value("Sheet1", 1, 7) {
            Some(LiteralValue::Number(value)) => Ok(value),
            Some(other) => bail!("expected numeric rollup, got {other:?}"),
            None => bail!("missing rollup"),
        }
    }

    fn assert_rollup(&self, label: &str) -> Result<()> {
        let rollup = self.rollup()?;
        let expected = self.expected_rollup();
        if (rollup - expected).abs() > 1e-6 {
            bail!("{label}: rollup mismatch: got {rollup}, expected {expected}");
        }
        Ok(())
    }
}

#[cfg(feature = "formualizer_runner")]
fn generate_finance_fixture(path: &PathBuf, rows: usize) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("create fixture dir {}: {e}", parent.display()))?;
    }
    write_workbook(path, |book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for row0 in 0..rows {
            let row = row0 as u32 + 1;
            let unit = (row0 + 1) as f64;
            let price = 10.0 + (row0 % 17) as f64;
            sheet.get_cell_mut((1, row)).set_value_number(unit);
            sheet.get_cell_mut((2, row)).set_value_number(price);
            sheet
                .get_cell_mut((3, row))
                .set_formula(format!("=A{row}*B{row}*$F$1"));
        }
        sheet.get_cell_mut((6, 1)).set_value_number(1.0);
        sheet
            .get_cell_mut((7, 1))
            .set_formula(format!("=SUM(C1:C{rows})"));
    });
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
fn deterministic_inputs(rows: usize) -> (Vec<f64>, Vec<f64>) {
    let mut units = Vec::with_capacity(rows);
    let mut prices = Vec::with_capacity(rows);
    for row0 in 0..rows {
        units.push((row0 + 1) as f64);
        prices.push(10.0 + (row0 % 17) as f64);
    }
    (units, prices)
}

#[cfg(feature = "formualizer_runner")]
fn default_workbook_path(label: &str, rows: usize) -> PathBuf {
    let safe_label: String = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    PathBuf::from("target")
        .join("finance-recalc-fixtures")
        .join(format!("{safe_label}-{rows}.xlsx"))
}

#[cfg(feature = "formualizer_runner")]
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len().saturating_sub(1)) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(feature = "formualizer_runner")]
fn linux_rss_mb() -> (Option<f64>, Option<f64>) {
    let status = std::fs::read_to_string("/proc/self/status").ok();
    let Some(status) = status else {
        return (None, None);
    };
    let mut current = None;
    let mut peak = None;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            current = parse_status_kb(rest).map(|kb| kb as f64 / 1024.0);
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            peak = parse_status_kb(rest).map(|kb| kb as f64 / 1024.0);
        }
    }
    (current, peak)
}

#[cfg(feature = "formualizer_runner")]
fn parse_status_kb(value: &str) -> Option<u64> {
    value
        .split_whitespace()
        .next()
        .and_then(|token| token.parse::<u64>().ok())
}
