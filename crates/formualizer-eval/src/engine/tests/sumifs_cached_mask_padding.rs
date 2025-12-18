use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn sumifs_cached_mask_padding_uses_slice_and_padding_branches() {
    crate::builtins::math::criteria_aggregates::test_hooks::reset_cached_mask_counters();

    let mut cfg = arrow_eval_config();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    // Force multiple row chunks and uneven used regions between sum and criteria ranges.
    // A:A has used rows up to 256; B:B has used rows up to 100.
    let chunk_rows = 64usize;
    let total_rows = 256u32;
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 2, chunk_rows);
        for i in 0..total_rows {
            let a = LiteralValue::Int((i + 1) as i64);
            // "Yes" at a few rows, and at row 100 to set used region.
            let b = if i == 0 || i == 50 || i == 99 {
                LiteralValue::Text("Yes".into())
            } else {
                LiteralValue::Empty
            };
            ab.append_row("Sheet1", &[a, b]).unwrap();
        }
        ab.finish().unwrap();
    }

    // Drive SUMIFS through the engine to ensure it uses cached criteria masks.
    let formula = parse("=SUMIFS(A:A, B:B, \"Yes\")").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, formula).unwrap();
    engine.evaluate_cell("Sheet1", 1, 1).unwrap();

    // Result: sum of rows 1, 51, 100 (1-based) => 1 + 51 + 100 = 152
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(152.0)
    );

    let (slice_fast, pad_partial, pad_all_fill) =
        crate::builtins::math::criteria_aggregates::test_hooks::cached_mask_counters();
    assert!(slice_fast >= 1, "expected at least one slice-fast branch");
    assert!(pad_partial >= 1, "expected partial-pad branch");
    assert!(pad_all_fill >= 1, "expected fill-only branch");
}
