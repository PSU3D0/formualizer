use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[test]
fn spill_overlay_writeback_visible_via_arrow() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg.clone());

    // Build a 1-column sheet with empty base values
    let sheet = "SpillSheet";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 2, 8);
        for _ in 0..4 {
            ab.append_row(sheet, &[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
        }
        ab.finish().unwrap();
    }

    // Place a VSTACK that spills 2x1 into column 1 starting at A2
    // =VSTACK(1,2)
    let ast = formualizer_parse::parser::parse("=VSTACK(1,2)").unwrap();
    engine.set_cell_formula(sheet, 2, 1, ast).unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Read via Arrow directly to confirm values are mirrored into overlay
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let av = asheet.range_view(0, 0, 3, 1); // rows 0..=3, cols 0..=1
    // Expect row 1 col 0 = 1, row 2 col 0 = 2
    match av.get_cell(1, 0) {
        LiteralValue::Number(n) => assert!((n - 1.0).abs() < 1e-9),
        other => panic!("expected 1.0 at (2,1) from overlay, got {other:?}"),
    }
    match av.get_cell(2, 0) {
        LiteralValue::Number(n) => assert!((n - 2.0).abs() < 1e-9),
        other => panic!("expected 2.0 at (3,1) from overlay, got {other:?}"),
    }
}
