use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[test]
fn formula_scalar_writeback_overlays_arrow_when_enabled() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg.clone());

    // Build a 1-column sheet with 3 rows base-empty
    let sheet = "SheetWB";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        ab.finish().unwrap();
    }

    // Put a simple scalar formula in row 2, col 1
    let ast = formualizer_parse::parser::parse("=1+2").unwrap();
    engine.set_cell_formula(sheet, 2, 1, ast).unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Read via ArrowRangeView directly and expect 3.0 at row index 1
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let av = asheet.range_view(0, 0, 2, 0); // rows 0..=2, col 0
    match av.get_cell(1, 0) {
        LiteralValue::Number(n) => assert!((n - 3.0).abs() < 1e-9),
        other => panic!("expected number 3.0 from overlay, got {other:?}"),
    }
}
