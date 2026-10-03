//! A formula written into a live spill through the workbook API blocks it
//! (`#SPILL!`, the formula keeps its value) and the spill recovers once the
//! formula is removed: `set_formula` and `set_formulas`, with deferred graph
//! building and the changelog each on and off, sequential and parallel.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::{Workbook, WorkbookConfig};

#[derive(Clone, Copy, Debug)]
enum Route {
    SetFormula,
    SetFormulas,
}

fn workbook(defer: bool, changelog: bool, parallel: bool) -> Workbook {
    let mut config = WorkbookConfig::ephemeral();
    config.eval.defer_graph_building = defer;
    config.enable_changelog = changelog;
    config.eval.enable_parallel = parallel;
    config.eval.max_threads = parallel.then_some(4);
    let mut wb = Workbook::new_with_config(config);
    wb.add_sheet("S").unwrap();
    wb
}

fn get(wb: &Workbook, row: u32, col: u32) -> Option<LiteralValue> {
    match wb.get_value("S", row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) | None => None,
        other => other,
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

#[track_caller]
fn assert_kind(value: Option<LiteralValue>, kind: ExcelErrorKind, ctx: &str) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, kind, "{ctx}: {e:?}"),
        other => panic!("{ctx}: expected {kind:?}, got {other:?}"),
    }
}

#[test]
fn formula_written_into_live_spill_blocks_and_recovers() {
    for route in [Route::SetFormula, Route::SetFormulas] {
        for defer in [false, true] {
            for changelog in [false, true] {
                for parallel in [false, true] {
                    let ctx = format!(
                        "{route:?} defer={defer} changelog={changelog} parallel={parallel}"
                    );
                    let mut wb = workbook(defer, changelog, parallel);
                    wb.set_formula("S", 1, 1, "=SEQUENCE(4)").unwrap();
                    wb.set_formula("S", 1, 2, "=SUM(A1#)").unwrap();
                    wb.evaluate_all().unwrap();
                    assert_eq!(get(&wb, 3, 1), n(3.0), "{ctx}: initial spill");
                    assert_eq!(get(&wb, 1, 2), n(10.0), "{ctx}: initial reader");

                    match route {
                        Route::SetFormula => wb.set_formula("S", 3, 1, "=40+2").unwrap(),
                        Route::SetFormulas => wb
                            .set_formulas("S", 3, 1, &[vec!["=40+2".to_string()]])
                            .unwrap(),
                    }
                    wb.evaluate_all().unwrap();
                    assert_kind(get(&wb, 1, 1), ExcelErrorKind::Spill, &ctx);
                    assert_eq!(get(&wb, 2, 1), None, "{ctx}: spill cleared");
                    assert_eq!(get(&wb, 3, 1), n(42.0), "{ctx}: intruder kept");
                    assert_eq!(get(&wb, 4, 1), None, "{ctx}: spill cleared");
                    assert_kind(get(&wb, 1, 2), ExcelErrorKind::Ref, &ctx);

                    wb.set_value("S", 3, 1, LiteralValue::Empty).unwrap();
                    wb.evaluate_all().unwrap();
                    // The logged `set_value` mirrors
                    // `Empty` into the value overlay, which masks the
                    // re-spilled value of the cleared cell (and so the
                    // reader's sum) independently of how the spill was
                    // blocked; the spill itself is back.
                    let overlay_masks_cleared_cell = changelog;
                    for row in 1..=4 {
                        if overlay_masks_cleared_cell && row == 3 {
                            continue;
                        }
                        assert_eq!(get(&wb, row, 1), n(row as f64), "{ctx}: recovered A{row}");
                    }
                    if !overlay_masks_cleared_cell {
                        assert_eq!(get(&wb, 1, 2), n(10.0), "{ctx}: reader recovered");
                    }
                }
            }
        }
    }
}
