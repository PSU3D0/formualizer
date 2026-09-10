use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use arrow_array::Array;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::ReferenceType;

#[test]
fn blank_masks_use_cell_types_across_base_overlay_and_chunk_slices() {
    let values = vec![
        LiteralValue::Number(1.0),
        LiteralValue::Boolean(true),
        LiteralValue::Empty,
        LiteralValue::Text(String::new()),
        LiteralValue::Text("x".into()),
        LiteralValue::Error(ExcelError::new_na()),
        LiteralValue::Number(0.0),
        LiteralValue::Boolean(false),
    ];
    for overlay in [false, true] {
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        {
            let mut ingest = engine.begin_bulk_ingest_arrow();
            ingest.add_sheet("S", 1, 3);
            for v in &values {
                ingest
                    .append_row(
                        "S",
                        &[if overlay {
                            LiteralValue::Text("old".into())
                        } else {
                            v.clone()
                        }],
                    )
                    .unwrap();
            }
            ingest.finish().unwrap();
        }
        if overlay {
            for (i, v) in values.iter().enumerate() {
                engine
                    .set_cell_value("S", i as u32 + 1, 1, v.clone())
                    .unwrap();
            }
        }
        for (start, end) in [(1, 8), (2, 7), (3, 4), (1, 2)] {
            let range =
                ReferenceType::range(Some("S".into()), Some(start), Some(1), Some(end), Some(1));
            let view = engine.resolve_range_view(&range, "S").unwrap();
            for criterion in ["", "<>"] {
                let pred =
                    crate::args::parse_criteria(&LiteralValue::Text(criterion.into())).unwrap();
                for _ in 0..3 {
                    let mask = engine.build_criteria_mask(&view, 0, &pred).unwrap();
                    assert_eq!(mask.len(), (end - start + 1) as usize);
                    for (i, value) in values[(start - 1) as usize..end as usize]
                        .iter()
                        .enumerate()
                    {
                        let blank = matches!(value, LiteralValue::Empty)
                            || matches!(value, LiteralValue::Text(s) if s.is_empty());
                        assert_eq!(
                            mask.is_valid(i) && mask.value(i),
                            if criterion.is_empty() { blank } else { !blank },
                            "overlay={overlay} range={start}:{end} criterion={criterion} row={i}"
                        );
                    }
                }
            }
        }
    }
}
