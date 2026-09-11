use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

#[test]
fn bessel_safety_has_consistent_formula_results() {
    for span in [false, true] {
        let mut wb =
            Workbook::new_with_config(WorkbookConfig::interactive().with_span_evaluation(span));
        wb.add_sheet("S").unwrap();
        for (row, formula, expected) in [
            (1, "BESSELJ(0.06,100)", 5.522_273_948_726_5e-311),
            (2, "BESSELY(0.06,100)", -5.76410997427483e307),
            (3, "BESSELJ(1e-12,13)", 1.9603324996120135e-170),
            (4, "IFERROR(BESSELJ(3e9,2e9),42)", 42.0),
            (5, "IFERROR(BESSELY(3e9,2e9),42)", 42.0),
        ] {
            wb.set_formula("S", row, 1, formula).unwrap();
            wb.evaluate_all().unwrap();
            let Some(LiteralValue::Number(actual)) = wb.get_value("S", row, 1) else {
                panic!("expected numeric result for {formula}");
            };
            assert!(
                (actual / expected - 1.0).abs() < 2e-12,
                "{formula}: {actual:e}"
            );
        }
        for formula in ["BESSELJ(1,2147483647)", "BESSELY(1,2147483647)"] {
            wb.set_formula("S", 6, 1, formula).unwrap();
            wb.evaluate_all().unwrap();
            let Some(LiteralValue::Error(error)) = wb.get_value("S", 6, 1) else {
                panic!("expected explicit work-limit failure for {formula}");
            };
            assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Num);
        }
    }
}
