use crate::engine::{Engine, RowVisibilitySource, VisibilityMaskMode};
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext as _;
use arrow_array::Array as _;
use formualizer_common::LiteralValue;

use super::common::arrow_eval_config;

fn mask_to_vec(mask: &arrow_array::BooleanArray) -> Vec<bool> {
    (0..mask.len())
        .map(|i| {
            if mask.is_null(i) {
                false
            } else {
                mask.value(i)
            }
        })
        .collect()
}

#[test]
fn row_visibility_masks_cover_all_modes() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());

    for row in 1..=5 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(row as i64))
            .unwrap();
    }

    engine
        .set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)
        .unwrap();
    engine
        .set_row_hidden("Sheet1", 4, true, RowVisibilitySource::Filter)
        .unwrap();

    let asheet = engine.sheet_store().sheet("Sheet1").expect("sheet exists");
    let view = asheet.range_view(0, 0, 4, 0);

    let include_all = engine
        .build_row_visibility_mask(&view, VisibilityMaskMode::IncludeAll)
        .expect("mask");
    assert_eq!(
        mask_to_vec(&include_all),
        vec![true, true, true, true, true]
    );

    let manual = engine
        .build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeManualHidden)
        .expect("mask");
    assert_eq!(mask_to_vec(&manual), vec![true, false, true, true, true]);

    let filter = engine
        .build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeFilterHidden)
        .expect("mask");
    assert_eq!(mask_to_vec(&filter), vec![true, true, true, false, true]);

    let combined = engine
        .build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeManualOrFilterHidden)
        .expect("mask");
    assert_eq!(mask_to_vec(&combined), vec![true, false, true, false, true]);
}

#[test]
fn row_visibility_mask_is_empty_for_non_materialized_rows() {
    let engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let asheet = engine.sheet_store().sheet("Sheet1").expect("sheet exists");

    let view = asheet.range_view(100, 0, 120, 0);
    let mask = engine
        .build_row_visibility_mask(&view, VisibilityMaskMode::ExcludeManualOrFilterHidden)
        .expect("mask");

    assert_eq!(mask.len(), 0);
}
