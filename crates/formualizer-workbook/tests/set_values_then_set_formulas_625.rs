//! Regression tests for #625: writing `LiteralValue::Empty` (`null`) entries via
//! `set_values` and then installing formulas in those cells via `set_formulas`
//! must not lose formula results when delta overlays remain uncompacted.

use formualizer_common::LiteralValue;
use formualizer_workbook::{Workbook, WorkbookConfig};

#[test]
fn issue_625_exact_repro_set_values_with_nulls_then_set_formulas() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive());
    wb.add_sheet("S").unwrap();

    wb.set_values("S", 3072, 6, &[vec![LiteralValue::Number(99.0)]])
        .unwrap();

    let mut top = 3073u32;
    while top <= 4100 {
        let end = (top + 1024).min(4101);
        let mut grid = Vec::with_capacity((end - top) as usize);
        for _ in top..end {
            grid.push(vec![
                LiteralValue::Number(2.0),
                LiteralValue::Number(3.0),
                LiteralValue::Empty,
                LiteralValue::Empty,
                LiteralValue::Empty,
            ]);
        }
        wb.set_values("S", top, 2, &grid).unwrap();
        top += 1024;
    }

    let formulas: Vec<Vec<String>> = (3073u32..=4100)
        .map(|r| vec![format!("=B{r}*C{r}")])
        .collect();
    wb.set_formulas("S", 3073, 6, &formulas).unwrap();
    wb.evaluate_all().unwrap();

    let mut wrong = Vec::new();
    for r in 3073u32..=4100 {
        let got = wb.get_value("S", r, 6);
        if got != Some(LiteralValue::Number(6.0)) {
            wrong.push((r, got));
        }
    }
    assert!(
        wrong.is_empty(),
        "expected 0 lost formulas across rows 3073..=4100, got {} wrong (first few: {:?})",
        wrong.len(),
        &wrong[..wrong.len().min(10)]
    );
    assert_eq!(wb.get_value("S", 3072, 6), Some(LiteralValue::Number(99.0)));
}

#[test]
fn set_values_with_nulls_then_set_formulas_across_multiple_bands_past_4096() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive());
    wb.add_sheet("S").unwrap();

    let total_rows = 5200u32;
    let mut top = 1u32;
    while top <= total_rows {
        let end = (top + 1024).min(total_rows + 1);
        let grid: Vec<Vec<LiteralValue>> = (top..end)
            .map(|r| {
                vec![
                    LiteralValue::Number(r as f64),
                    LiteralValue::Number(2.0),
                    LiteralValue::Empty,
                ]
            })
            .collect();
        wb.set_values("S", top, 1, &grid).unwrap();
        top += 1024;
    }

    let formulas: Vec<Vec<String>> = (1u32..=total_rows)
        .map(|r| vec![format!("=A{r}*B{r}")])
        .collect();
    wb.set_formulas("S", 1, 3, &formulas).unwrap();
    wb.evaluate_all().unwrap();

    for r in 1u32..=total_rows {
        assert_eq!(
            wb.get_value("S", r, 3),
            Some(LiteralValue::Number((r as f64) * 2.0)),
            "row {r} formula result was lost or shadowed"
        );
    }
}

#[test]
fn set_values_empty_and_nonempty_then_set_formulas_single_row_and_demand_eval() {
    // Demand-driven evaluation (`evaluate_cell`) uses target preparation rather
    // than `build_graph_all`; delta overlays must be cleared on that path too.
    for config in [
        WorkbookConfig::interactive(),
        {
            let mut c = WorkbookConfig::ephemeral();
            c.eval.defer_graph_building = true;
            c
        },
        WorkbookConfig::ephemeral(),
    ] {
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("S").unwrap();

        wb.set_values(
            "S",
            1,
            1,
            &[vec![
                LiteralValue::Number(4.0),
                LiteralValue::Number(5.0),
                LiteralValue::Empty,
                LiteralValue::Number(999.0),
            ]],
        )
        .unwrap();

        wb.set_formulas(
            "S",
            1,
            3,
            &[vec!["=A1*B1".to_string(), "=A1+B1".to_string()]],
        )
        .unwrap();

        assert_eq!(
            wb.evaluate_cell("S", 1, 3).unwrap(),
            LiteralValue::Number(20.0)
        );
        assert_eq!(wb.get_value("S", 1, 3), Some(LiteralValue::Number(20.0)));

        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 1, 4), Some(LiteralValue::Number(9.0)));
    }
}

#[test]
fn engine_bulk_set_formulas_clears_delta_overlay() {
    use formualizer_eval::engine::{Engine, EvalConfig};
    use formualizer_eval::test_workbook::TestWorkbook;
    use formualizer_parse::parse;

    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    engine
        .set_cell_value("S", 1, 1, LiteralValue::Number(3.0))
        .unwrap();
    engine
        .set_cell_value("S", 1, 2, LiteralValue::Number(7.0))
        .unwrap();
    engine
        .set_cell_value("S", 1, 3, LiteralValue::Empty)
        .unwrap();
    engine
        .set_cell_value("S", 1, 4, LiteralValue::Number(123.0))
        .unwrap();

    engine
        .bulk_set_formulas(
            "S",
            vec![
                (1, 3, parse("=A1*B1").unwrap()),
                (1, 4, parse("=A1+B1").unwrap()),
            ],
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("S", 1, 3),
        Some(LiteralValue::Number(21.0))
    );
    assert_eq!(
        engine.get_cell_value("S", 1, 4),
        Some(LiteralValue::Number(10.0))
    );
}
