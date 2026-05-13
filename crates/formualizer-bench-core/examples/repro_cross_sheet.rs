#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("Need formualizer_runner feature");
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
        wb.add_sheet("Data").unwrap();
        for r in 1u32..=20 {
            wb.set_value("Data", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
        }
        for r in 1u32..=20 {
            wb.set_formula("Sheet1", r, 1, &format!("=Data!A{r} * 2"))
                .unwrap();
        }
        wb.evaluate_all().unwrap();
        let stats = wb.engine().baseline_stats();
        println!(
            "{label}: spans={} formula_vertices={}",
            stats.formula_plane_active_span_count, stats.graph_formula_vertex_count
        );
        let mut wrong = 0;
        for r in 1u32..=20 {
            let exp = (r as f64) * 2.0;
            match wb.get_value("Sheet1", r, 1) {
                Some(LiteralValue::Number(n)) if (n - exp).abs() < 1e-9 => {}
                other => {
                    wrong += 1;
                    println!("  row {r}: WRONG: {other:?} expected {exp}");
                }
            }
        }
        if wrong == 0 {
            println!("  initial values correct");
        }

        // Edit Data!A5 = 1000.
        wb.set_value("Data", 5, 1, LiteralValue::Number(1000.0))
            .unwrap();
        wb.evaluate_all().unwrap();
        match wb.get_value("Sheet1", 5, 1) {
            Some(LiteralValue::Number(n)) if (n - 2000.0).abs() < 1e-9 => {
                println!("  Sheet1!A5 after Data!A5=1000: 2000 OK")
            }
            other => println!("  Sheet1!A5 WRONG after edit: {other:?}"),
        }
        // Verify other rows unchanged.
        for r in [1u32, 10, 20] {
            let exp = (r as f64) * 2.0;
            match wb.get_value("Sheet1", r, 1) {
                Some(LiteralValue::Number(n)) if (n - exp).abs() < 1e-9 => {}
                other => println!("  row {r} drifted: {other:?} expected {exp}"),
            }
        }
    }

    run("Off", FormulaPlaneMode::Off);
    run("Auth", FormulaPlaneMode::AuthoritativeExperimental);
}
