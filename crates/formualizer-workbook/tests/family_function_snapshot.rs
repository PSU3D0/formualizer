use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::{CustomFnOptions, Workbook, WorkbookMode};
use std::sync::Arc;

// The same workbook exercises walk, lift, criteria and nested invariant calls,
// alongside an isolated single cell. Input edits explicitly schedule each pass;
// registration alone does not promise to dirty already evaluated formulas.
#[test]
fn family_function_snapshot_registration_replacement_and_builtin_override()
-> Result<(), Box<dyn std::error::Error>> {
    let mut wb = Workbook::new_with_mode(WorkbookMode::Ephemeral);
    for row in 1..=64 {
        wb.set_value("Sheet1", row, 1, LiteralValue::Number(1.0))?;
        wb.set_formula("Sheet1", row, 2, &format!("=ROUNDTRIP_FN(A{row})"))?;
        wb.set_formula(
            "Sheet1",
            row,
            3,
            &format!("=ABS(A{row})+ROUNDTRIP_FN($A$1)"),
        )?;
        wb.set_formula(
            "Sheet1",
            row,
            4,
            &format!("=COUNTIF($A$1:$A$64,ROUNDTRIP_FN(A{row}))"),
        )?;
        wb.set_formula(
            "Sheet1",
            row,
            5,
            &format!("=IF(A{row}>0,ROUNDTRIP_FN(A{row}),0)"),
        )?;
    }
    wb.set_formula("Sheet1", 1, 8, "=ROUNDTRIP_FN(A1)+10")?;
    wb.evaluate_all()?;
    for col in [2, 3, 4, 5, 8] {
        assert!(
            matches!(wb.get_value("Sheet1", 1, col), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Name)
        );
    }
    for result in [1.0, 2.0] {
        if result == 2.0 {
            wb.unregister_custom_function("ROUNDTRIP_FN")?;
        }
        wb.register_custom_function(
            "ROUNDTRIP_FN",
            CustomFnOptions::default(),
            Arc::new(move |_: &[LiteralValue]| Ok(LiteralValue::Number(result))),
        )?;
        // Also replace the builtin implementation used to select the lift tier.
        wb.register_custom_function(
            "ABS",
            CustomFnOptions {
                allow_override_builtin: true,
                ..Default::default()
            },
            Arc::new(|_: &[LiteralValue]| Ok(LiteralValue::Number(100.0))),
        )?;
        for row in 1..=64 {
            wb.set_value("Sheet1", row, 1, LiteralValue::Number(result))?;
        }
        wb.evaluate_all()?;
        for row in 1..=64 {
            for (col, expected) in [(2, result), (3, 100.0 + result), (4, 64.0), (5, result)] {
                assert_eq!(
                    wb.get_value("Sheet1", row, col),
                    Some(LiteralValue::Number(expected)),
                    "row={row} col={col}"
                );
            }
        }
        assert_eq!(
            wb.get_value("Sheet1", 1, 8),
            Some(LiteralValue::Number(result + 10.0))
        );
        wb.unregister_custom_function("ABS")?;
    }
    wb.unregister_custom_function("ROUNDTRIP_FN")?;
    for row in 1..=64 {
        wb.set_value("Sheet1", row, 1, LiteralValue::Number(3.0))?;
    }
    wb.evaluate_all()?;
    for col in [2, 3, 4, 5, 8] {
        assert!(
            matches!(wb.get_value("Sheet1", 1, col), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Name)
        );
    }
    Ok(())
}
