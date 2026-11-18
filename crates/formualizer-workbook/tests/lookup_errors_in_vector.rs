//! The snippet of issue #586 through the workbook API: LOOKUP steps over the
//! errors of its lookup vector, as Excel does, instead of turning left at
//! them. Excel 16.0 gives 30, 30 and 9, and 30 for the control.

use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

#[test]
fn lookup_steps_over_errors_in_the_lookup_vector() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    // A1:C1 = 1, 0, 1 (the middle element makes 1/(x<>0) an error).
    for (col, v) in [(1, 1.0), (2, 0.0), (3, 1.0)] {
        wb.set_value("S", 1, col, LiteralValue::Number(v)).unwrap();
    }
    // A2:C2 = 10, 20, 30.
    for (col, v) in [(1, 10.0), (2, 20.0), (3, 30.0)] {
        wb.set_value("S", 2, col, LiteralValue::Number(v)).unwrap();
    }
    // A7:E7 = 5, <empty>, <empty>, 7, 9 (the last-non-blank idiom).
    for (col, v) in [(1, 5.0), (4, 7.0), (5, 9.0)] {
        wb.set_value("S", 7, col, LiteralValue::Number(v)).unwrap();
    }
    let cases = [
        (4, "=LOOKUP(2,{1,#DIV/0!,1},{10,20,30})", 30.0),
        (5, "=LOOKUP(2,1/(A1:C1<>0),A2:C2)", 30.0),
        (8, "=LOOKUP(2,1/(A7:E7<>\"\"),A7:E7)", 9.0),
        (6, "=LOOKUP(2,{1,1,1},{10,20,30})", 30.0),
    ];
    for (row, formula, _) in cases {
        wb.set_formula("S", row, 1, formula).unwrap();
    }
    for (row, formula, excel) in cases {
        assert_eq!(
            wb.evaluate_cell("S", row, 1).unwrap(),
            LiteralValue::Number(excel),
            "{formula}"
        );
    }
}
