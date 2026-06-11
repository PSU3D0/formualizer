//! Scratch probe: FormulaPlane family-rejection cost on an incremental chain.
//!
//! Builds `A1=1`, `A{r}=A{r-1}+1` for r=2..N (one candidate family, rejected
//! with `InternalDependency`) and times first eval under FormulaPlane Off vs
//! AuthoritativeExperimental.
//!
//! ```bash
//! cargo run -p formualizer-bench-core --features formualizer_runner \
//!   --release --bin probe-fp-reject-chain -- --rows 10000,25000,50000,100000
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
    }

    fn run_mode(n: u32, mode: FormulaPlaneMode) -> Result<(u128, String)> {
        let config = WorkbookConfig::interactive().with_formula_plane_mode(mode);
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("S")?;
        wb.set_value("S", 1, 1, LiteralValue::Number(1.0))?;
        let formulas: Vec<Vec<String>> = (2..=n).map(|r| vec![format!("=A{}+1", r - 1)]).collect();
        wb.set_formulas("S", 2, 1, &formulas)?;
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
        Ok((first_eval_ms, reasons))
    }

    pub fn main() -> Result<()> {
        let cli = Cli::parse();
        println!("rows\toff_ms(min)\tauth_ms(min)\tpenalty_ms");
        for part in cli.rows.split(',') {
            let n: u32 = part.trim().parse()?;
            let mut off_min = u128::MAX;
            let mut auth_min = u128::MAX;
            let mut reasons = String::new();
            for _ in 0..cli.reps.max(1) {
                let (off_ms, _) = run_mode(n, FormulaPlaneMode::Off)?;
                let (auth_ms, auth_reasons) =
                    run_mode(n, FormulaPlaneMode::AuthoritativeExperimental)?;
                off_min = off_min.min(off_ms);
                auth_min = auth_min.min(auth_ms);
                reasons = auth_reasons;
            }
            println!(
                "{n}\t{off_min}\t{auth_min}\t{}\t{reasons}",
                auth_min as i128 - off_min as i128
            );
        }
        Ok(())
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    probe::main()
}
