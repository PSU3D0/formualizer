use crate::engine::eval::visibility_mask_test_hooks;
use crate::engine::{Engine, RowVisibilitySource, VisibilityMaskMode};
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext as _;
use formualizer_common::LiteralValue;

use super::common::arrow_eval_config;

fn seed_rows(engine: &mut Engine<TestWorkbook>, rows: u32) {
    for r in 1..=rows {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Int(r as i64))
            .unwrap();
    }
}

#[test]
fn visibility_mask_cache_reuses_same_key() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    seed_rows(&mut engine, 5);
    engine
        .set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)
        .unwrap();

    let view = engine
        .sheet_store()
        .sheet("Sheet1")
        .expect("sheet")
        .range_view(0, 0, 4, 0);

    visibility_mask_test_hooks::reset();
    let _ = engine.build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeManualHidden);
    let _ = engine.build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeManualHidden);

    let (hits, misses, evictions) = visibility_mask_test_hooks::counters();
    assert_eq!(misses, 1);
    assert_eq!(hits, 1);
    assert_eq!(evictions, 0);
}

#[test]
fn visibility_mask_cache_invalidates_on_visibility_version_change() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    seed_rows(&mut engine, 5);

    visibility_mask_test_hooks::reset();
    {
        let view = engine
            .sheet_store()
            .sheet("Sheet1")
            .expect("sheet")
            .range_view(0, 0, 4, 0);
        let _ = engine.build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeManualHidden);
    }

    engine
        .set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Manual)
        .unwrap();

    {
        let view = engine
            .sheet_store()
            .sheet("Sheet1")
            .expect("sheet")
            .range_view(0, 0, 4, 0);
        let _ = engine.build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeManualHidden);
    }

    let (hits, misses, _) = visibility_mask_test_hooks::counters();
    assert_eq!(hits, 0);
    assert_eq!(misses, 2);
}

#[test]
fn visibility_mask_cache_isolated_by_mode_and_span() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    seed_rows(&mut engine, 6);
    engine
        .set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)
        .unwrap();

    let view_short = engine
        .sheet_store()
        .sheet("Sheet1")
        .expect("sheet")
        .range_view(0, 0, 2, 0);
    let view_long = engine
        .sheet_store()
        .sheet("Sheet1")
        .expect("sheet")
        .range_view(0, 0, 5, 0);

    visibility_mask_test_hooks::reset();

    let _ = engine.build_row_visibility_mask(&view_short, VisibilityMaskMode::ExcludeManualHidden);
    let _ = engine.build_row_visibility_mask(&view_short, VisibilityMaskMode::ExcludeFilterHidden);
    let _ = engine.build_row_visibility_mask(&view_long, VisibilityMaskMode::ExcludeManualHidden);
    let _ = engine.build_row_visibility_mask(&view_short, VisibilityMaskMode::ExcludeManualHidden);

    let (hits, misses, _) = visibility_mask_test_hooks::counters();
    assert_eq!(misses, 3);
    assert_eq!(hits, 1);
}
