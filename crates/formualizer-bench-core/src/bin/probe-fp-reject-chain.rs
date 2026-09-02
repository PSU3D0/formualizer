//! Scratch probe: FormulaPlane family-rejection cost on an incremental chain.
//!
//! Builds `A1=1`, `A{r}=A{r-1}+1` for r=2..N (one candidate family, rejected
//! with `InternalDependency`) and times first eval under FormulaPlane Off vs
//! AuthoritativeExperimental.
//!
//! `--spans K` adds K unrelated two-column families (`value`, `=value*2+1`)
//! of the same row count. That is the #405 shape: a rejected legacy column next
//! to accepted spans used to make the mixed-schedule dirty projection O(rows²).
//! `--max-ratio R` turns the probe into a gate: it exits non-zero when
//! Authoritative first eval exceeds R × Off for any row count.
//!
//! ```bash
//! cargo run -p formualizer-bench-core --features formualizer_runner \
//!   --release --bin probe-fp-reject-chain -- --rows 10000,25000,50000,100000
//! cargo run -p formualizer-bench-core --features formualizer_runner \
//!   --release --bin probe-fp-reject-chain -- --rows 5000,10000,20000 --spans 2 --max-ratio 3
//! ```

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("requires feature `formualizer_runner`");
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
mod probe {
    use std::time::Instant;

    use anyhow::Result;
    use clap::Parser;
    use formualizer_eval::engine::FormulaPlaneMode;
    use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

    #[derive(Debug, Parser)]
    pub struct Cli {
        #[arg(long, default_value = "10000,25000,50000,100000")]
        rows: String,
        /// Interleaved repetitions per (rows, mode); min is reported.
        #[arg(long, default_value_t = 3)]
        reps: u32,
        /// Unrelated two-column span families (value column + `=value*2+1`)
        /// of the same row count placed next to the chain (#405 shape).
        #[arg(long, default_value_t = 0)]
        spans: u32,
        /// Fail (exit 1) when Authoritative first eval exceeds this multiple of
        /// Off for any row count. Runs under `--min-off-ms` are compared
        /// against `--min-off-ms` instead of the measured Off time so tiny
        /// workbooks do not trip the gate on timer noise.
        #[arg(long)]
        max_ratio: Option<f64>,
        #[arg(long, default_value_t = 20)]
        min_off_ms: u128,
    }

    fn run_mode(n: u32, spans: u32, mode: FormulaPlaneMode) -> Result<(u128, String)> {
        let config = WorkbookConfig::interactive().with_formula_plane_mode(mode);
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("S")?;
        wb.set_value("S", 1, 1, LiteralValue::Number(1.0))?;
        let formulas: Vec<Vec<String>> = (2..=n).map(|r| vec![format!("=A{}+1", r - 1)]).collect();
        wb.set_formulas("S", 2, 1, &formulas)?;
        for family in 0..spans {
            let value_col = 2 + family * 2;
            let formula_col = value_col + 1;
            let value_ref = column_name(value_col);
            for r in 1..=n {
                wb.set_value("S", r, value_col, LiteralValue::Number(f64::from(r)))?;
            }
            let formulas: Vec<Vec<String>> = (1..=n)
                .map(|r| vec![format!("={value_ref}{r}*2+1")])
                .collect();
            wb.set_formulas("S", 1, formula_col, &formulas)?;
        }
        let start = Instant::now();
        wb.evaluate_all()?;
        let first_eval_ms = start.elapsed().as_millis();
        let reasons = format!(
            "{:?}",
            wb.engine().formula_ingest_report_total().fallback_reasons
        );
        // sanity: last cell value
        let last = wb.get_value("S", n, 1);
        anyhow::ensure!(
            matches!(last, Some(LiteralValue::Number(v)) if v == n as f64),
            "value mismatch at A{n}: {last:?}"
        );
        for family in 0..spans {
            let formula_col = 3 + family * 2;
            let got = wb.get_value("S", n, formula_col);
            anyhow::ensure!(
                matches!(got, Some(LiteralValue::Number(v)) if v == f64::from(n) * 2.0 + 1.0),
                "value mismatch at span family {family} row {n}: {got:?}"
            );
        }
        Ok((first_eval_ms, reasons))
    }

    fn column_name(col: u32) -> String {
        let mut col = col;
        let mut name = Vec::new();
        while col > 0 {
            let rem = (col - 1) % 26;
            name.push(char::from(b'A' + rem as u8));
            col = (col - 1) / 26;
        }
        name.iter().rev().collect()
    }

    pub fn main() -> Result<()> {
        let cli = Cli::parse();
        println!("rows\toff_ms(min)\tauth_ms(min)\tpenalty_ms\tratio");
        let mut violations = Vec::new();
        for part in cli.rows.split(',') {
            let n: u32 = part.trim().parse()?;
            let mut off_min = u128::MAX;
            let mut auth_min = u128::MAX;
            let mut reasons = String::new();
            for _ in 0..cli.reps.max(1) {
                let (off_ms, _) = run_mode(n, cli.spans, FormulaPlaneMode::Off)?;
                let (auth_ms, auth_reasons) =
                    run_mode(n, cli.spans, FormulaPlaneMode::AuthoritativeExperimental)?;
                off_min = off_min.min(off_ms);
                auth_min = auth_min.min(auth_ms);
                reasons = auth_reasons;
            }
            let baseline = off_min.max(cli.min_off_ms);
            let ratio = auth_min as f64 / baseline as f64;
            println!(
                "{n}\t{off_min}\t{auth_min}\t{}\t{ratio:.2}\t{reasons}",
                auth_min as i128 - off_min as i128
            );
            if let Some(max_ratio) = cli.max_ratio
                && ratio > max_ratio
            {
                violations.push(format!(
                    "rows={n}: authoritative {auth_min} ms is {ratio:.2}x off ({off_min} ms, baseline {baseline} ms), limit {max_ratio}x"
                ));
            }
        }
        if !violations.is_empty() {
            eprintln!("FormulaPlane first-eval ratio gate FAILED:");
            for line in &violations {
                eprintln!("  {line}");
            }
            std::process::exit(1);
        }
        Ok(())
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    probe::main()
}
