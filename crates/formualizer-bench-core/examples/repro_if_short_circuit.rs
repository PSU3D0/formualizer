//! Probe: does FormulaPlane span eval honor IF short-circuit?
//!
//! Setup: a column of formulas of the form `=IF(A{r}>0, A{r}*2, 1/0)`.
//! - Off-mode (legacy): IF short-circuits, never evaluates `1/0`. Result: A{r}*2 for positive A.
//! - Auth-mode: span eval should produce same result.
//!
//! If span eval forces evaluation of both branches during key build or AST traversal,
//! we'll see #DIV/0! errors propagating where they shouldn't.

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

    const ROWS: u32 = 200;

    fn build(formula_template: impl Fn(u32) -> String, mode: FormulaPlaneMode) -> Workbook {
        let mut config = WorkbookConfig::interactive();
        config.eval.formula_plane_mode = mode;
        config.eval.enable_parallel = false;
        let mut wb = Workbook::new_with_config(config);
        for r in 1u32..=ROWS {
            // All positive values, so IF condition always true → else branch never evaluates.
            // Use 3 distinct values to enable memoization (K << N).
            let v = ((r % 3) + 1) as f64;
            wb.set_value("Sheet1", r, 1, LiteralValue::Number(v))
                .unwrap();
        }
        for r in 1u32..=ROWS {
            wb.set_formula("Sheet1", r, 2, &formula_template(r))
                .unwrap();
        }
        wb
    }

    fn time_run(label: &str, mode: FormulaPlaneMode, builder: impl Fn(u32) -> String) {
        let mut wb = build(builder, mode);
        let t0 = Instant::now();
        wb.evaluate_all().unwrap();
        let elapsed = t0.elapsed();
        let mode_label = match mode {
            FormulaPlaneMode::Off => "Off",
            FormulaPlaneMode::AuthoritativeExperimental => "Auth",
            _ => "Other",
        };
        let spans = wb.engine().baseline_stats().formula_plane_active_span_count;

        // Sample three rows. With A1=2, A50=2, A200=2 (using r%3+1: r=1→2, r=50→2, r=200→2).
        // Expected: A*2 = 4 for all three.
        let r1 = wb.get_value("Sheet1", 1, 2);
        let r50 = wb.get_value("Sheet1", 50, 2);
        let r200 = wb.get_value("Sheet1", 200, 2);

        // Are any cells holding an error (would indicate else-branch was evaluated)?
        let mut error_count = 0u32;
        for r in 1..=ROWS {
            if let Some(LiteralValue::Error(_)) = wb.get_value("Sheet1", r, 2) {
                error_count += 1;
            }
        }
        println!(
            "{:50} {:>4}  first={:>7.2}ms  spans={}  B1={:?}  B50={:?}  B200={:?}  errors={}",
            label,
            mode_label,
            elapsed.as_secs_f64() * 1000.0,
            spans,
            r1,
            r50,
            r200,
            error_count
        );
    }

    println!("ROWS = {ROWS}\n");

    // Variant A: =IF(condition, safe, error_if_evaluated)
    println!("--- IF with else-branch that errors if evaluated ---");
    let v1 = |r: u32| format!("=IF(A{r}>0, A{r}*2, 1/0)");
    time_run("=IF(A>0, A*2, 1/0)", FormulaPlaneMode::Off, v1);
    time_run(
        "=IF(A>0, A*2, 1/0)",
        FormulaPlaneMode::AuthoritativeExperimental,
        v1,
    );
    println!();

    // Variant B: AND short-circuit
    println!("--- AND with second arg that errors if evaluated ---");
    let v2 = |r: u32| format!("=AND(A{r}<0, 1/0=1)");
    time_run("=AND(A<0, 1/0=1)", FormulaPlaneMode::Off, v2);
    time_run(
        "=AND(A<0, 1/0=1)",
        FormulaPlaneMode::AuthoritativeExperimental,
        v2,
    );
    println!();

    // Variant C: IFERROR wrapping
    println!("--- IFERROR with potentially-erroring branch ---");
    let v3 = |r: u32| format!("=IFERROR(A{r}*2, 1/0)");
    time_run("=IFERROR(A*2, 1/0)", FormulaPlaneMode::Off, v3);
    time_run(
        "=IFERROR(A*2, 1/0)",
        FormulaPlaneMode::AuthoritativeExperimental,
        v3,
    );
    println!();

    // Variant D: IFS chain
    println!("--- IFS short-circuit chain ---");
    let v4 = |r: u32| format!("=IFS(A{r}>0, A{r}*2, A{r}<0, A{r}*3, TRUE, 1/0)");
    time_run(
        "=IFS(A>0, A*2, A<0, A*3, TRUE, 1/0)",
        FormulaPlaneMode::Off,
        v4,
    );
    time_run(
        "=IFS(A>0, A*2, A<0, A*3, TRUE, 1/0)",
        FormulaPlaneMode::AuthoritativeExperimental,
        v4,
    );
}
