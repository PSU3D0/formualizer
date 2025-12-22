use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use arrow_array::Array as _;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ReferenceType;

#[test]
fn criteria_mask_text_is_built_per_chunk_and_handles_empty_string_semantics() {
    crate::engine::eval::criteria_mask_test_hooks::reset_text_segment_counters();

    let mut cfg = arrow_eval_config();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    // Force multiple chunks.
    let chunk_rows = 64usize;
    let total_rows = 256u32;
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 1, chunk_rows);
        // All empty; we'll test Eq("") and Ne("") behavior.
        for _ in 0..total_rows {
            ab.append_row("Sheet1", &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }

    let rng = ReferenceType::range(
        Some("Sheet1".to_string()),
        Some(1),
        Some(1),
        Some(total_rows),
        Some(1),
    );
    let view = engine.resolve_range_view(&rng, "Sheet1").unwrap();

    // Eq("") should treat Empty as equal and yield all true (no nulls).
    let pred_eq_empty = crate::args::parse_criteria(&LiteralValue::Text("".into())).unwrap();
    let mask_eq = engine
        .build_criteria_mask(&view, 0, &pred_eq_empty)
        .expect("mask");
    assert_eq!(mask_eq.len(), total_rows as usize);
    assert_eq!(mask_eq.null_count(), 0);
    for i in 0..mask_eq.len() {
        assert!(mask_eq.value(i));
    }

    // Ne("") should yield all nulls on an all-empty column (nilike(null, "") == null).
    let pred_ne_empty = crate::args::parse_criteria(&LiteralValue::Text("<>".into())).unwrap();
    let mask_ne = engine
        .build_criteria_mask(&view, 0, &pred_ne_empty)
        .expect("mask");
    assert_eq!(mask_ne.len(), total_rows as usize);
    assert_eq!(mask_ne.null_count(), total_rows as usize);

    // Ensure we actually walked chunks and hit the all-null segment fast path.
    let (segments_total, segments_all_null) =
        crate::engine::eval::criteria_mask_test_hooks::text_segment_counters();
    assert!(segments_total >= 4, "expected multiple chunks");
    assert_eq!(segments_total, segments_all_null);
}

#[test]
fn criteria_mask_text_matches_values_across_chunks() {
    crate::engine::eval::criteria_mask_test_hooks::reset_text_segment_counters();

    let mut cfg = arrow_eval_config();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    let chunk_rows = 64usize;
    let total_rows = 256u32;
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 1, chunk_rows);
        for i in 0..total_rows {
            let v = if i % 50 == 0 {
                LiteralValue::Text("Yes".into())
            } else {
                LiteralValue::Empty
            };
            ab.append_row("Sheet1", &[v]).unwrap();
        }
        ab.finish().unwrap();
    }

    let rng = ReferenceType::range(
        Some("Sheet1".to_string()),
        Some(1),
        Some(1),
        Some(total_rows),
        Some(1),
    );
    let view = engine.resolve_range_view(&rng, "Sheet1").unwrap();

    let pred = crate::args::parse_criteria(&LiteralValue::Text("Yes".into())).unwrap();
    let mask = engine.build_criteria_mask(&view, 0, &pred).expect("mask");

    assert_eq!(mask.len(), total_rows as usize);
    // For non-empty comparisons, empties remain nulls in the mask.
    assert!(mask.null_count() > 0);

    for i in 0..mask.len() {
        if (i as u32).is_multiple_of(50) {
            assert!(mask.is_valid(i) && mask.value(i));
        } else {
            assert!(!mask.is_valid(i));
        }
    }

    let (segments_total, segments_all_null) =
        crate::engine::eval::criteria_mask_test_hooks::text_segment_counters();
    assert!(segments_total >= 4);
    assert!(
        segments_all_null < segments_total,
        "expected mixed segments"
    );
}
