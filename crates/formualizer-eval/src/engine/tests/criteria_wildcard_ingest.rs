use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ReferenceType;

#[test]
fn wildcard_masks_preserve_scalar_contract_for_base_and_overlay() {
    for overlay in [false, true] {
        for value in [
            LiteralValue::Number(123.0),
            LiteralValue::Boolean(true),
            LiteralValue::Empty,
        ] {
            let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
            {
                let mut ingest = engine.begin_bulk_ingest_arrow();
                ingest.add_sheet("S", 1, 3);
                for row in 0..9 {
                    ingest
                        .append_row(
                            "S",
                            &[if row == 4 && !overlay {
                                value.clone()
                            } else {
                                LiteralValue::Text("abc".into())
                            }],
                        )
                        .unwrap();
                }
                ingest.finish().unwrap();
            }
            let range = ReferenceType::range(Some("S".into()), Some(2), Some(1), Some(8), Some(1));
            let pred = crate::args::parse_criteria(&LiteralValue::Text("*".into())).unwrap();
            if overlay {
                let view = engine.resolve_range_view(&range, "S").unwrap();
                assert_eq!(
                    engine
                        .build_criteria_mask(&view, 0, &pred)
                        .unwrap()
                        .true_count(),
                    7
                );
                engine.set_cell_value("S", 5, 1, value.clone()).unwrap();
            }
            let view = engine.resolve_range_view(&range, "S").unwrap();
            assert_eq!(
                engine
                    .build_criteria_mask(&view, 0, &pred)
                    .unwrap()
                    .true_count(),
                7,
                "mixed data must retain a cacheable scalar-equivalent mask"
            );
            for pattern in ["1*", "?", "TRUE*", "~*"] {
                let pred =
                    crate::args::parse_criteria(&LiteralValue::Text(pattern.into())).unwrap();
                let mask = engine.build_criteria_mask(&view, 0, &pred).unwrap();
                for row in 0..7 {
                    assert_eq!(
                        mask.value(row),
                        crate::builtins::criteria_match(&pred, &view.get_cell(row, 0)),
                        "{pattern} row={row}"
                    );
                }
            }
        }
    }
}
