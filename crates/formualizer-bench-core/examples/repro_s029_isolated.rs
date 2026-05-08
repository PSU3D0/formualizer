//! Minimal repro of s029-style multi-literal formula correctness bug under Auth.
//!
//! s029 formulas have multiple literal `{r}` integer values (one per VLOOKUP key,
//! one per IF condition, one per LEN concatenation). After lookup-family
//! promotion (commit pending), Auth produces identical values across rows that
//! should produce distinct values.

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
        // Data sheet: A column = row number, B column = row * 10.
        wb.add_sheet("Data").unwrap();
        for r in 1u32..=200 {
            wb.set_value("Data", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
            wb.set_value("Data", r, 2, LiteralValue::Number((r * 10) as f64))
                .unwrap();
        }
        // Calc sheet
        wb.add_sheet("Calc").unwrap();
        for r in 1u32..=200 {
            wb.set_formula("Calc", r, 1, &formula_template(r)).unwrap();
        }
        wb
    }

    fn dump(wb: &mut Workbook, label: &str, sample_rows: &[u32]) {
        wb.evaluate_all().unwrap();
        let active = wb.engine().baseline_stats().formula_plane_active_span_count;
        let vals: Vec<_> = sample_rows
            .iter()
            .map(|&r| wb.get_value("Calc", r, 1).unwrap_or(LiteralValue::Empty))
            .collect();
        println!("  {label:42} spans={active} vals={vals:?}");
    }

    let samples = [1u32, 5, 10, 50, 100, 150, 200];

    println!("--- variant A: simple VLOOKUP with literal key (s050-style) ---");
    let v_a = |r: u32| format!("=VLOOKUP({r}, Data!$A$1:$B$200, 2, FALSE)");
    println!("  expected: 10, 50, 100, 500, 1000, 1500, 2000");
    let mut wb = build(FormulaPlaneMode::Off, v_a);
    dump(&mut wb, "Off", &samples);
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v_a);
    dump(&mut wb, "Auth", &samples);
    println!();

    println!("--- variant B: VLOOKUP + literal-arg IF ---");
    let v_b = |r: u32| {
        format!("=VLOOKUP({r}, Data!$A$1:$B$200, 2, FALSE) + IF(MOD({r}, 2) = 0, 100, 200)")
    };
    println!("  expected per sample: 210, 250, 200, 600, 1100, 1600, 2100");
    let mut wb = build(FormulaPlaneMode::Off, v_b);
    dump(&mut wb, "Off", &samples);
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v_b);
    dump(&mut wb, "Auth", &samples);
    println!();

    println!("--- variant C: two VLOOKUPs with different literal keys ---");
    let v_c = |r: u32| {
        format!(
            "=VLOOKUP({r}, Data!$A$1:$B$200, 2, FALSE) + IFERROR(VLOOKUP({key2}, Data!$A$1:$B$200, 2, FALSE), 0)",
            key2 = r * 7
        )
    };
    println!("  expected: r=1 (10+70), r=5 (50+350), r=10 (100+700), r=50 (500+0), ...");
    let mut wb = build(FormulaPlaneMode::Off, v_c);
    dump(&mut wb, "Off", &samples);
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v_c);
    dump(&mut wb, "Auth", &samples);
    println!();

    println!("--- variant D: full s029 shape ---");
    let v_d = |r: u32| {
        format!(
            "=VLOOKUP({r}, Data!$A$1:$B$200, 2, FALSE) + IFERROR(VLOOKUP({key2}, Data!$A$1:$B$200, 2, FALSE), 0) + IF(MOD({r}, 2) = 0, 100, 200) + LEN(\"row-\" & {r})",
            key2 = r * 7
        )
    };
    let mut wb = build(FormulaPlaneMode::Off, v_d);
    dump(&mut wb, "Off", &samples);
    let mut wb = build(FormulaPlaneMode::AuthoritativeExperimental, v_d);
    dump(&mut wb, "Auth", &samples);
}
