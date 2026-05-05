// Repro: span over-broadens across volatile/error gaps. Documented bug.
//
// When formulas of identical shape (e.g. =A{r}*2) are intermixed with
// volatile (RAND/NOW/TODAY) or error-producing (#DIV/0!) formulas in the
// same column, Auth mode's span placement collapses the non-volatile
// rows into a single span template that does not respect the per-row
// reference offset. Result: rows after a volatile/error break inherit
// stale values from earlier rows.
//
// Off mode is correct.
//
// Cataloged via:
//   crates/formualizer-bench-core/src/scenarios/s021_volatile_functions_sprinkled.rs
//   crates/formualizer-bench-core/src/scenarios/s025_errors_propagating_through_family.rs
//
// Both scenarios use ExpectedFailure { mode: AuthOnly, .. } so the corpus
// runner records the failure without breaking the smoke run.

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This example requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --release --example repro_intermixed_family"
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() {
    use formualizer_common::LiteralValue;
    use formualizer_eval::engine::FormulaPlaneMode;
    use formualizer_workbook::{Workbook, WorkbookConfig};

    fn run(label: &str, mode: FormulaPlaneMode) {
        let mut config = WorkbookConfig::interactive();
        config.eval.formula_plane_mode = mode;
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("S").unwrap();
        for r in 1u32..=20 {
            wb.set_value("S", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
            let formula = if r % 10 == 0 {
                format!("=A{r}*RAND()")
            } else {
                format!("=A{r}*2")
            };
            wb.set_formula("S", r, 2, &formula).unwrap();
        }
        wb.evaluate_all().unwrap();

        println!("=== {label} ===");
        let mut wrong = 0;
        for r in 1u32..=20 {
            let b = wb.get_value("S", r, 2);
            let exp_str = if r % 10 == 0 {
                "(rand)".to_string()
            } else {
                format!("{}", r * 2)
            };
            let mark = if r % 10 == 0 {
                "(rand)"
            } else {
                let exp: f64 = (r * 2) as f64;
                match &b {
                    Some(LiteralValue::Number(n)) if (n - exp).abs() < 1e-6 => "OK",
                    _ => {
                        wrong += 1;
                        "**WRONG**"
                    }
                }
            };
            println!("  row {r:>2}: B={:?}  expected={}  [{}]", b, exp_str, mark);
        }
        println!("  -> {wrong} wrong of 18 non-volatile rows");
    }

    run("Auth (default)", FormulaPlaneMode::AuthoritativeExperimental);
    run("Off", FormulaPlaneMode::Off);
}
