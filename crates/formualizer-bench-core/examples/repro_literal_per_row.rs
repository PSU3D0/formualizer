//! Test: does the literal-parameterization bug affect non-VLOOKUP functions?

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("Need formualizer_runner feature");
}

#[cfg(feature = "formualizer_runner")]
fn main() {
    use formualizer_common::LiteralValue;
    use formualizer_eval::engine::FormulaPlaneMode;
    use formualizer_workbook::{Workbook, WorkbookConfig};

    fn build(mode: FormulaPlaneMode, formula_template: impl Fn(u32) -> String) -> Workbook {
        let mut config = WorkbookConfig::interactive();
        config.eval.formula_plane_mode = mode;
        config.eval.enable_parallel = false;
        let mut wb = Workbook::new_with_config(config);
        for r in 1u32..=200 {
            wb.set_value("Sheet1", r, 1, LiteralValue::Number((r * 100) as f64))
                .unwrap();
            wb.set_formula("Sheet1", r, 2, &formula_template(r))
                .unwrap();
        }
        wb
    }

    fn dump(wb: &mut Workbook, label: &str) {
        wb.evaluate_all().unwrap();
        let active = wb.engine().baseline_stats().formula_plane_active_span_count;
        let samples: Vec<_> = [1u32, 5, 10, 50, 100, 200]
            .iter()
            .map(|&r| wb.get_value("Sheet1", r, 2).unwrap_or(LiteralValue::Empty))
            .collect();
        println!("  {label:30} spans={active} samples={samples:?}");
    }

    // Variant 1: =A1 + {r} (all-relative ref + literal). The literal varies per placement.
    println!("--- =A{{r}} + {{r}} (literal varies per row) ---");
    let v1 = |r: u32| format!("=A{r} + {r}");
    println!("  expected: 101, 505, 1010, 5050, 10100, 20200");
    let mut wb = build(FormulaPlaneMode::Off, v1);
    dump(&mut wb, "Off");
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v1);
    dump(&mut wb, "Auth");
    println!();

    // Variant 2: =SUM(A{r}, {r}) with literal varying.
    println!("--- =SUM(A{{r}}, {{r}}) ---");
    let v2 = |r: u32| format!("=SUM(A{r}, {r})");
    println!("  expected: 101, 505, 1010, 5050, 10100, 20200");
    let mut wb = build(FormulaPlaneMode::Off, v2);
    dump(&mut wb, "Off");
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v2);
    dump(&mut wb, "Auth");
    println!();

    // Variant 3: =IF(A{r} > {r}, 1, 0) — literal varies.
    println!("--- =IF(A{{r}} > {{r}}, 1, 0) ---");
    let v3 = |r: u32| format!("=IF(A{r} > {r}, 1, 0)");
    println!("  expected: all 1.0 (since A{{r}}=r*100 > r)");
    let mut wb = build(FormulaPlaneMode::Off, v3);
    dump(&mut wb, "Off");
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v3);
    dump(&mut wb, "Auth");
    println!();

    // Variant 4: =MOD({r}, 2) — pure literal-only formula (no refs).
    println!("--- =MOD({{r}}, 2) (no refs, only literals) ---");
    let v4 = |r: u32| format!("=MOD({r}, 2)");
    println!("  expected: 1, 1, 0, 0, 0, 0");
    let mut wb = build(FormulaPlaneMode::Off, v4);
    dump(&mut wb, "Off");
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v4);
    dump(&mut wb, "Auth");
}
