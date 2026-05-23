#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("Need formualizer_runner feature");
}

#[cfg(feature = "formualizer_runner")]
fn main() {
    use formualizer_common::{LiteralValue, RangeAddress};
    use formualizer_eval::engine::FormulaPlaneMode;
    use formualizer_workbook::{Workbook, WorkbookConfig, traits::NamedRangeScope};

    fn run(label: &str, mode: FormulaPlaneMode) {
        println!("\n=== {label} ===");
        let mut config = WorkbookConfig::interactive();
        config.eval.formula_plane_mode = mode;
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("Data").unwrap();
        for r in 1u32..=300 {
            wb.set_value("Data", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
        }
        let addr1 = RangeAddress::new("Data".to_string(), 1, 1, 100, 1).unwrap();
        wb.define_named_range("DataRange", &addr1, NamedRangeScope::Workbook)
            .unwrap();
        wb.set_formula("Sheet1", 1, 1, "=SUM(DataRange)").unwrap();
        wb.evaluate_all().unwrap();
        println!(
            "After define A1:A100  -> A1={:?}  (expect 5050)",
            wb.get_value("Sheet1", 1, 1)
        );

        let addr2 = RangeAddress::new("Data".to_string(), 1, 1, 200, 1).unwrap();
        wb.update_named_range("DataRange", &addr2, NamedRangeScope::Workbook)
            .unwrap();
        wb.evaluate_all().unwrap();
        println!(
            "After update to A1:A200 -> A1={:?}  (expect 20100)",
            wb.get_value("Sheet1", 1, 1)
        );

        let addr3 = RangeAddress::new("Data".to_string(), 50, 1, 150, 1).unwrap();
        wb.update_named_range("DataRange", &addr3, NamedRangeScope::Workbook)
            .unwrap();
        wb.evaluate_all().unwrap();
        let expected3: f64 = (50..=150).map(|x| x as f64).sum();
        println!(
            "After update to A50:A150 -> A1={:?}  (expect {expected3})",
            wb.get_value("Sheet1", 1, 1)
        );

        let addr4 = RangeAddress::new("Data".to_string(), 100, 1, 300, 1).unwrap();
        wb.update_named_range("DataRange", &addr4, NamedRangeScope::Workbook)
            .unwrap();
        wb.evaluate_all().unwrap();
        let expected4: f64 = (100..=300).map(|x| x as f64).sum();
        println!(
            "After update to A100:A300 -> A1={:?}  (expect {expected4})",
            wb.get_value("Sheet1", 1, 1)
        );
    }

    run("Off", FormulaPlaneMode::Off);
    run("Auth", FormulaPlaneMode::AuthoritativeExperimental);
}
