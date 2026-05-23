#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("Need formualizer_runner feature");
}

#[cfg(feature = "formualizer_runner")]
fn main() {
    use formualizer_common::LiteralValue;
    use formualizer_eval::engine::FormulaPlaneMode;
    use formualizer_workbook::{Workbook, WorkbookConfig};
    use std::time::Instant;

    const ROWS: u32 = 10_000;

    fn build(formula_template: &str, mode: FormulaPlaneMode) -> Workbook {
        let mut config = WorkbookConfig::interactive();
        // Note: interactive() mode (defer_graph_building=true) is required to
        // exercise the FormulaPlane family-promotion path via per-cell
        // set_formula. With ephemeral() mode set_formula creates legacy graph
        // vertices directly and never promotes; only batch ingest from XLSX or
        // ingest_formula_batches() exercises the family-promotion path in that
        // mode. The corpus probe loads via UmyaAdapter which uses batch ingest.
        config.eval.formula_plane_mode = mode;
        let mut wb = Workbook::new_with_config(config);
        for r in 1u32..=ROWS {
            wb.set_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
        }
        for r in 1u32..=ROWS {
            wb.set_formula(
                "Sheet1",
                r,
                2,
                &formula_template.replace("{r}", &r.to_string()),
            )
            .unwrap();
        }
        wb
    }

    fn time_run(label: &str, formula: &str, mode: FormulaPlaneMode) {
        let mut wb = build(formula, mode);
        let t0 = Instant::now();
        wb.evaluate_all().unwrap();
        let first = t0.elapsed();
        // Edit one input value to dirty everything
        wb.set_value("Sheet1", ROWS / 2, 1, LiteralValue::Number(999_999.0))
            .unwrap();
        let t1 = Instant::now();
        wb.evaluate_all().unwrap();
        let recalc = t1.elapsed();
        // Sample value to verify correctness
        let sample = wb.get_value("Sheet1", 1, 2);
        let spans = wb.engine().baseline_stats().formula_plane_active_span_count;
        println!(
            "{:50} mode={:?}  first={:>9.2}ms  recalc={:>9.2}ms  spans={}  B1={:?}",
            label,
            mode,
            first.as_secs_f64() * 1000.0,
            recalc.as_secs_f64() * 1000.0,
            spans,
            sample
        );
    }

    println!("ROWS = {ROWS}");
    println!();

    // Whole-column form
    time_run(
        "=SUM($A:$A) - A{r}        whole-col",
        "=SUM($A:$A) - A{r}",
        FormulaPlaneMode::Off,
    );
    time_run(
        "=SUM($A:$A) - A{r}        whole-col",
        "=SUM($A:$A) - A{r}",
        FormulaPlaneMode::AuthoritativeExperimental,
    );

    println!();

    // Equivalent finite-range form
    let finite = format!("=SUM($A$1:$A${ROWS}) - A{{r}}");
    time_run(
        "=SUM($A$1:$A$N) - A{r}     finite-range",
        &finite,
        FormulaPlaneMode::Off,
    );
    time_run(
        "=SUM($A$1:$A$N) - A{r}     finite-range",
        &finite,
        FormulaPlaneMode::AuthoritativeExperimental,
    );

    println!();

    // Pure whole-column SUM (no per-row subtraction) for upper bound
    time_run(
        "=SUM($A:$A)               whole-col, no per-row",
        "=SUM($A:$A)",
        FormulaPlaneMode::Off,
    );
    time_run(
        "=SUM($A:$A)               whole-col, no per-row",
        "=SUM($A:$A)",
        FormulaPlaneMode::AuthoritativeExperimental,
    );

    println!();

    // Pure finite-range SUM (no per-row subtraction)
    let finite_no_sub = format!("=SUM($A$1:$A${ROWS})");
    time_run(
        "=SUM($A$1:$A$N)           finite, no per-row",
        &finite_no_sub,
        FormulaPlaneMode::Off,
    );
    time_run(
        "=SUM($A$1:$A$N)           finite, no per-row",
        &finite_no_sub,
        FormulaPlaneMode::AuthoritativeExperimental,
    );
}
