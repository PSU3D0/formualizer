use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[test]
fn overlay_compacts_on_threshold_via_set_cell_value() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    // Build Arrow sheet with 1 column, 64 rows (single chunk of 64)
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 1, 64);
        for _ in 0..64 {
            ab.append_row("S", &[LiteralValue::Empty]).unwrap();
        }
        let _ = ab.finish().unwrap();
    }

    // Two edits in same chunk → overlay len 2, threshold len/50 = 1 => compaction triggers
    engine
        .set_cell_value("S", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("S", 2, 1, LiteralValue::Number(2.0))
        .unwrap();

    let sheet = engine.sheet_store().sheet("S").unwrap();
    assert_eq!(sheet.columns[0].chunks.len(), 1);
    assert_eq!(sheet.columns[0].chunks[0].overlay.len(), 0);
}
