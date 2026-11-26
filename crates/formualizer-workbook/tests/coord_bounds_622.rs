//! Regression tests for #622: `Workbook` write APIs must validate 1-based Excel
//! coordinates (`1..=1_048_576`, `1..=16_384`), returning `Err(IoError)` instead
//! of panicking out of bounds or silently mapping row/col `0` to `A1`.

use formualizer_common::LiteralValue;
use formualizer_workbook::{CellData, Workbook, WorkbookConfig};
use std::collections::BTreeMap;

const MAX_ROW: u32 = 1_048_576;
const MAX_COL: u32 = 16_384;

fn configs() -> [WorkbookConfig; 2] {
    [WorkbookConfig::ephemeral(), WorkbookConfig::interactive()]
}

#[test]
fn set_value_rejects_zero_and_out_of_bounds_without_mutating_a1() {
    for cfg in configs() {
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        for (row, col) in [
            (0, 0),
            (0, 1),
            (1, 0),
            (1, MAX_COL + 1),
            (MAX_ROW + 1, 1),
            (MAX_ROW + 2, 1),
            (1, 8_826_682),
        ] {
            let err = wb
                .set_value("S", row, col, LiteralValue::Number(7.0))
                .expect_err(&format!("set_value({row}, {col}) must fail"));
            let msg = err.to_string();
            assert!(
                msg.contains(&format!("R{row}C{col}")),
                "error message should name coordinate R{row}C{col}, got: {msg}"
            );
            assert_eq!(
                wb.get_value("S", 1, 1),
                None,
                "invalid set_value({row}, {col}) must not write A1"
            );
        }
    }
}

#[test]
fn set_formula_rejects_zero_and_out_of_bounds() {
    for cfg in configs() {
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        for (row, col) in [
            (0, 0),
            (0, 1),
            (1, 0),
            (1, MAX_COL + 1),
            (MAX_ROW + 1, 1),
            (MAX_ROW + 2, 1),
        ] {
            let err = wb
                .set_formula("S", row, col, "=1")
                .expect_err(&format!("set_formula({row}, {col}) must fail"));
            let msg = err.to_string();
            assert!(
                msg.contains(&format!("R{row}C{col}")),
                "error message should name coordinate R{row}C{col}, got: {msg}"
            );
            assert_eq!(
                wb.get_formula("S", 1, 1),
                None,
                "invalid set_formula({row}, {col}) must not write A1"
            );
        }
    }
}

#[test]
fn write_range_rejects_zero_and_out_of_bounds_atomically() {
    for cfg in configs() {
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        for (bad_row, bad_col) in [
            (0, 0),
            (0, 1),
            (1, 0),
            (1, MAX_COL + 1),
            (MAX_ROW + 1, 1),
            (1, 8_826_682),
        ] {
            let mut cells = BTreeMap::new();
            cells.insert((1, 1), CellData::from_value(42.0));
            cells.insert((bad_row, bad_col), CellData::from_value(1.0));

            let err = wb.write_range("S", (1, 1), cells).expect_err(&format!(
                "write_range with ({bad_row}, {bad_col}) must fail"
            ));
            let msg = err.to_string();
            assert!(
                msg.contains(&format!("R{bad_row}C{bad_col}")),
                "error message should name coordinate R{bad_row}C{bad_col}, got: {msg}"
            );
            assert_eq!(
                wb.get_value("S", 1, 1),
                None,
                "failing write_range must leave workbook unchanged"
            );
        }
    }
}

#[test]
fn set_values_and_set_formulas_reject_zero_and_out_of_bounds() {
    for cfg in configs() {
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        // Zero or OOB anchors
        for (row, col) in [(0, 1), (1, 0), (0, 0), (MAX_ROW + 1, 1), (1, MAX_COL + 1)] {
            assert!(
                wb.set_values("S", row, col, &[vec![LiteralValue::Number(1.0)]])
                    .is_err(),
                "set_values({row}, {col}) must fail"
            );
            assert!(
                wb.set_formulas("S", row, col, &[vec!["=1".to_string()]])
                    .is_err(),
                "set_formulas({row}, {col}) must fail"
            );
        }

        // Valid anchor, extent overflows past MAX_ROW or MAX_COL
        assert!(
            wb.set_values(
                "S",
                MAX_ROW,
                1,
                &[
                    vec![LiteralValue::Number(1.0)],
                    vec![LiteralValue::Number(2.0)],
                ],
            )
            .is_err()
        );
        assert!(
            wb.set_values(
                "S",
                1,
                MAX_COL,
                &[vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)]],
            )
            .is_err()
        );
        assert!(
            wb.set_formulas(
                "S",
                MAX_ROW,
                1,
                &[vec!["=1".to_string()], vec!["=2".to_string()]],
            )
            .is_err()
        );
        assert!(
            wb.set_formulas("S", 1, MAX_COL, &[vec!["=1".to_string(), "=2".to_string()]],)
                .is_err()
        );

        assert_eq!(wb.get_value("S", 1, 1), None);
        assert_eq!(wb.get_value("S", MAX_ROW, 1), None);
        assert_eq!(wb.get_value("S", 1, MAX_COL), None);
    }
}

#[test]
fn valid_excel_grid_boundaries_still_succeed() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
    wb.set_value("S", MAX_ROW, MAX_COL, LiteralValue::Number(2.0))
        .unwrap();
    wb.set_formula("S", MAX_ROW, 1, "=1+1").unwrap();

    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(1.0)));
    assert_eq!(
        wb.get_value("S", MAX_ROW, MAX_COL),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        wb.evaluate_cell("S", MAX_ROW, 1).unwrap(),
        LiteralValue::Number(2.0)
    );
}

#[test]
fn engine_and_workbook_action_reject_zero_and_out_of_bounds() {
    use formualizer_eval::engine::{Engine, EvalConfig};
    use formualizer_eval::test_workbook::TestWorkbook;
    use formualizer_parse::parse;

    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();

    for (row, col) in [(0, 0), (0, 1), (1, 0), (1, MAX_COL + 1), (MAX_ROW + 1, 1)] {
        assert!(
            engine
                .set_cell_value("S", row, col, LiteralValue::Number(7.0))
                .is_err(),
            "Engine::set_cell_value({row}, {col}) must fail"
        );
        assert!(
            engine
                .set_cell_formula("S", row, col, parse("=1").unwrap())
                .is_err(),
            "Engine::set_cell_formula({row}, {col}) must fail"
        );
        assert!(
            engine
                .bulk_set_formulas("S", vec![(row, col, parse("=1").unwrap())])
                .is_err(),
            "Engine::bulk_set_formulas({row}, {col}) must fail"
        );
    }
    assert_eq!(engine.get_cell_value("S", 1, 1), None);

    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive());
    wb.add_sheet("S").unwrap();
    let res = wb.action("bad-coords", |tx| {
        assert!(tx.set_value("S", 0, 0, LiteralValue::Number(7.0)).is_err());
        assert!(
            tx.set_value("S", 1, MAX_COL + 1, LiteralValue::Number(7.0))
                .is_err()
        );
        assert!(tx.set_formula("S", MAX_ROW + 1, 1, "=1").is_err());
        assert!(
            tx.set_values("S", 0, 1, &[vec![LiteralValue::Number(1.0)]])
                .is_err()
        );
        let mut cells = BTreeMap::new();
        cells.insert((1, MAX_COL + 1), CellData::from_value(1.0));
        assert!(tx.write_range("S", (1, 1), cells).is_err());
        Ok(())
    });
    assert!(res.is_ok());
    assert_eq!(wb.get_value("S", 1, 1), None);
}
