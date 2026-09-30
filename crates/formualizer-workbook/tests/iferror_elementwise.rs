//! Workbook-level IFERROR/IFNA array behaviour: spilled results, reductions,
//! recalculation after edits, and parity across evaluation modes.

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_workbook::{Workbook, WorkbookConfig};

fn render(value: Option<LiteralValue>) -> String {
    match value {
        Some(LiteralValue::Number(n)) => format!("{n}"),
        Some(LiteralValue::Int(i)) => format!("{}", i as f64),
        Some(LiteralValue::Error(e)) => e.kind.to_string(),
        Some(LiteralValue::Text(s)) => format!("{s:?}"),
        Some(other) => format!("{other:?}"),
        None => "<none>".into(),
    }
}

fn row(wb: &Workbook, row: u32, cols: std::ops::RangeInclusive<u32>) -> String {
    cols.map(|c| render(wb.get_value("S", row, c)))
        .collect::<Vec<_>>()
        .join(",")
}

fn col(wb: &Workbook, col: u32, rows: std::ops::RangeInclusive<u32>) -> String {
    rows.map(|r| render(wb.get_value("S", r, col)))
        .collect::<Vec<_>>()
        .join(",")
}

fn configs() -> Vec<(&'static str, WorkbookConfig)> {
    vec![
        ("interactive", WorkbookConfig::interactive()),
        (
            "span",
            WorkbookConfig::interactive().with_span_evaluation(true),
        ),
        (
            "formula-plane",
            WorkbookConfig::interactive()
                .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
        ),
    ]
}

/// A1:A3 = 1,0,2 ; B1:B3 = 10,20,30 ; C1:C3 = 1,#N/A,#DIV/0!
fn seeded(config: WorkbookConfig) -> Workbook {
    let mut wb = Workbook::new_with_config(config);
    wb.add_sheet("S").unwrap();
    let error = |kind| LiteralValue::Error(ExcelError::new(kind));
    for (r, a, b, c) in [
        (
            1,
            LiteralValue::Number(1.0),
            10.0,
            LiteralValue::Number(1.0),
        ),
        (
            2,
            LiteralValue::Number(0.0),
            20.0,
            error(ExcelErrorKind::Na),
        ),
        (
            3,
            LiteralValue::Number(2.0),
            30.0,
            error(ExcelErrorKind::Div),
        ),
    ] {
        wb.set_value("S", r, 1, a).unwrap();
        wb.set_value("S", r, 2, LiteralValue::Number(b)).unwrap();
        wb.set_value("S", r, 3, c).unwrap();
    }
    wb
}

#[test]
fn iferror_and_ifna_spill_elementwise_results() {
    for (label, config) in configs() {
        let mut wb = seeded(config);
        let cells = [
            (1, 5, "=IFERROR(1/A1:A3,-1)"),
            (1, 6, "=IFERROR(1/A1:A3,B1:B3)"),
            (1, 7, "=IFERROR(C1:C3,0)"),
            (1, 8, "=IFNA(C1:C3,0)"),
            (1, 9, "=SUM(IFERROR(1/A1:A3,0))"),
            (5, 1, "=IFERROR(1/{0,4},8)"),
            (6, 1, "=IFERROR(1/{0,4},NA())"),
            (7, 1, "=IFERROR({#N/A,2},{1;2})"),
            (10, 1, "=IFERROR({#N/A,#N/A,#N/A},{8,9})"),
            (11, 1, "=IFERROR(1/0,{5,6})"),
            (12, 1, "=IFERROR(7,1/0)"),
            (13, 1, "=IFERROR(1/0,)"),
            (14, 1, "=IFERROR(Z1,8)"),
        ];
        for (r, c, formula) in cells {
            wb.set_formula("S", r, c, formula).unwrap();
        }
        wb.evaluate_all().unwrap();

        assert_eq!(col(&wb, 5, 1..=3), "1,-1,0.5", "{label}");
        assert_eq!(col(&wb, 6, 1..=3), "1,20,0.5", "{label}");
        assert_eq!(col(&wb, 7, 1..=3), "1,0,0", "{label}");
        assert_eq!(col(&wb, 8, 1..=3), "1,0,#DIV/0!", "{label}");
        assert_eq!(col(&wb, 9, 1..=1), "1.5", "{label}");
        assert_eq!(row(&wb, 5, 1..=2), "8,0.25", "{label}");
        assert_eq!(row(&wb, 6, 1..=2), "#N/A,0.25", "{label}");
        assert_eq!(row(&wb, 7, 1..=2), "1,2", "{label}");
        assert_eq!(row(&wb, 8, 1..=2), "2,2", "{label}");
        assert_eq!(row(&wb, 10, 1..=1), "#VALUE!", "{label}");
        assert_eq!(row(&wb, 11, 1..=2), "5,6", "{label}");
        assert_eq!(row(&wb, 12, 1..=1), "7", "{label}");
        assert_eq!(row(&wb, 13, 1..=1), "0", "{label}");
        assert_eq!(row(&wb, 14, 1..=1), "0", "{label}");
    }
}

#[test]
fn elementwise_guards_follow_source_edits() {
    for (label, config) in configs() {
        let mut wb = seeded(config);
        wb.set_formula("S", 1, 5, "=IFERROR(1/A1:A3,-1)").unwrap();
        wb.set_formula("S", 1, 6, "=IFNA(C1:C3,0)").unwrap();
        wb.set_formula("S", 1, 7, "=SUM(IFERROR(C1:C3,100))")
            .unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(col(&wb, 5, 1..=3), "1,-1,0.5", "{label}");
        assert_eq!(col(&wb, 7, 1..=1), "201", "{label}");

        // Remove the zero divisor: every element is clean again.
        wb.set_value("S", 2, 1, LiteralValue::Number(4.0)).unwrap();
        // Replace the stored #DIV/0! with #N/A: IFNA now replaces it.
        wb.set_value(
            "S",
            3,
            3,
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)),
        )
        .unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(col(&wb, 5, 1..=3), "1,0.25,0.5", "{label}");
        assert_eq!(col(&wb, 6, 1..=3), "1,0,0", "{label}");
        assert_eq!(col(&wb, 7, 1..=1), "201", "{label}");

        // Formula-produced errors in the referenced range are replaced too.
        wb.set_formula("S", 1, 3, "=1/0").unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(col(&wb, 6, 1..=3), "#DIV/0!,0,0", "{label}");
        assert_eq!(col(&wb, 7, 1..=1), "300", "{label}");
    }
}

#[test]
fn per_row_scalar_guards_keep_family_results() {
    // A relative per-row IFERROR is a formula family; clean members may take
    // the typed lift, error members the builtin. Both must agree with the
    // per-cell result in every mode.
    for (label, config) in configs() {
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("S").unwrap();
        for r in 1..=40u32 {
            let divisor = if r % 3 == 0 { 0.0 } else { r as f64 };
            wb.set_value("S", r, 1, LiteralValue::Number(divisor))
                .unwrap();
            wb.set_formula("S", r, 2, &format!("=IFERROR(1/A{r},-1)"))
                .unwrap();
            wb.set_formula("S", r, 3, &format!("=IFNA(IF(A{r}=0,NA(),A{r}),-2)"))
                .unwrap();
        }
        wb.evaluate_all().unwrap();
        for r in 1..=40u32 {
            let (b, c) = if r % 3 == 0 {
                ("-1".to_string(), "-2".to_string())
            } else {
                (format!("{}", 1.0 / r as f64), format!("{}", r as f64))
            };
            assert_eq!(row(&wb, r, 2..=3), format!("{b},{c}"), "{label} row {r}");
        }
    }
}

#[test]
fn over_cap_materialization_is_num_and_dormant_fallback_is_ignored() {
    let mut wb = seeded(WorkbookConfig::interactive());
    wb.set_formula("S", 1, 5, "=IFERROR(1/(SEQUENCE(4097)-1),SEQUENCE(1,4097))")
        .unwrap();
    wb.set_formula("S", 1, 6, "=SUM(IFERROR(B1:B3,SEQUENCE(1000000000)))")
        .unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(col(&wb, 5, 1..=1), "#NUM!");
    assert_eq!(col(&wb, 6, 1..=1), "60");
}
