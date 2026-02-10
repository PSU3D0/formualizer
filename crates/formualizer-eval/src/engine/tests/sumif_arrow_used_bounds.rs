use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

// Repro: Arrow used-bounds cover only top rows; graph values exist below.
// With the used-bounds fix, SUMIF over whole columns should include edited rows.
#[test]
fn sumif_whole_column_includes_post_edit_rows_when_arrow_reads_disabled() {
    let cfg = arrow_eval_config(); // enable Arrow store and Arrow used-bounds (gated by has_edited)
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    // Install a small Arrow sheet with 2 columns (S and P) covering only first 10 rows
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 20, 8); // at least 20 columns so we can use S (19) and P (16)
        // Fill first 10 rows with non-matching values
        for _ in 0..10 {
            // columns are 0-based: col 19 (S=19) and col 16 (P=16) in 1-based Excel
            let mut row = vec![LiteralValue::Empty; 20];
            row[15] = LiteralValue::Number(0.0); // P column (1-based 16)
            row[18] = LiteralValue::Text("noop".into()); // S column (1-based 19)
            ab.append_row("Sheet1", &row).unwrap();
        }
        let _ = ab.finish().unwrap();
    }

    // Now edit graph values outside Arrow's top-10 used region
    // Put S50 = "target", P50 = 123
    engine
        .set_cell_value("Sheet1", 50, 19, LiteralValue::Text("target".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 50, 16, LiteralValue::Int(123))
        .unwrap();
    // Criteria in D3 = "target"
    engine
        .set_cell_value("Sheet1", 3, 4, LiteralValue::Text("target".into()))
        .unwrap();

    // SUMIF(S:S, D3, P:P) in A1
    let ast = parse("=SUMIF(S:S, D3, P:P)").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    // Evaluate and assert it finds row 50 despite Arrow used-bounds covering only top rows
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(123.0)
    );
}
