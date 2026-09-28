use formualizer_common::ExcelErrorKind;
use formualizer_workbook::{LiteralValue, Workbook};

fn error_kind(value: Option<LiteralValue>) -> Option<ExcelErrorKind> {
    match value {
        Some(LiteralValue::Error(error)) => Some(error.kind),
        _ => None,
    }
}

/// An error value passed where the criteria family expects a range is the
/// function's result, as in Excel. A `#REF!` left behind by a deleted column
/// used to read as a range that matches nothing, returning a plausible 0.
#[test]
fn error_value_in_a_range_argument_is_the_result() {
    let mut wb = Workbook::new();
    wb.add_sheet("Sheet1").unwrap();
    for row in 1..=3 {
        wb.set_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        wb.set_value("Sheet1", row, 2, LiteralValue::Number(10.0))
            .unwrap();
    }
    let formulas = [
        "=SUMIF(#REF!,1,B1:B3)",
        "=SUMIF(A1:A3,1,#REF!)",
        "=COUNTIF(#REF!,1)",
        "=AVERAGEIF(#REF!,1,B1:B3)",
        "=SUMIFS(B1:B3,#REF!,1)",
        "=SUMIFS(#REF!,A1:A3,1)",
        "=SUMIFS(B1:B3,A1:A3,1,#REF!,1)",
        "=COUNTIFS(A1:A3,1,#REF!,1)",
        "=AVERAGEIFS(B1:B3,#REF!,1)",
        "=SUMIFS(B1:B3,IF(TRUE,#N/A),1)",
        "=SUMIF(A1:A3,1,IF(TRUE,#N/A))",
        "=COUNTIF(IF(TRUE,1/0),1)",
    ];
    for (index, formula) in formulas.iter().enumerate() {
        wb.set_formula("Sheet1", index as u32 + 1, 4, formula)
            .unwrap();
    }
    wb.evaluate_all().unwrap();
    for (index, formula) in formulas.iter().enumerate() {
        // Every expectation was checked in Excel 365: an error computed at
        // run time keeps its own kind.
        let expected = if formula.contains("#N/A") {
            ExcelErrorKind::Na
        } else if formula.contains("1/0") {
            ExcelErrorKind::Div
        } else {
            ExcelErrorKind::Ref
        };
        assert_eq!(
            error_kind(wb.get_value("Sheet1", index as u32 + 1, 4)),
            Some(expected),
            "{formula}"
        );
    }
}

/// Errors held in the cells of a criteria range are not matched and do not
/// poison the result; only an error standing in for the range itself does.
#[test]
fn error_cells_inside_a_criteria_range_are_still_skipped() {
    let mut wb = Workbook::new();
    wb.add_sheet("Sheet1").unwrap();
    wb.set_formula("Sheet1", 1, 1, "=1/0").unwrap();
    wb.set_value("Sheet1", 2, 1, LiteralValue::Number(1.0))
        .unwrap();
    wb.set_value("Sheet1", 1, 2, LiteralValue::Number(5.0))
        .unwrap();
    wb.set_value("Sheet1", 2, 2, LiteralValue::Number(7.0))
        .unwrap();
    wb.set_formula("Sheet1", 1, 3, "=SUMIFS(B1:B2,A1:A2,1)")
        .unwrap();
    wb.set_formula("Sheet1", 2, 3, "=COUNTIF(A1:A2,1)").unwrap();
    wb.set_formula("Sheet1", 3, 3, "=SUMIF(A1,1,B1)").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(
        wb.get_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(0.0))
    );
}
