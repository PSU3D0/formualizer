use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[test]
fn sparse_sheet_growth_avoids_tiny_chunk_explosion() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    // Seed a small physical extent (100 rows) then grow to a much larger row via overlay.
    engine
        .set_cell_value("Sheet1", 100, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 60_256, 1, LiteralValue::Int(1))
        .unwrap();

    let asheet = engine.sheet_store().sheet("Sheet1").unwrap();

    // Growth should not create ~600+ chunk boundaries of size 100.
    // Exact chunking isn't important, but it must remain coarse-grained.
    assert!(
        asheet.chunk_starts.len() <= 8,
        "expected coarse chunking; got {} chunk starts",
        asheet.chunk_starts.len()
    );
    assert_eq!(asheet.nrows as usize, 60_256);
}
