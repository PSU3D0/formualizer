use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ReferenceType;

#[test]
fn whole_column_reference_uses_configured_cap_when_bounds_unknown() {
    let wb = TestWorkbook::new();
    let cfg = EvalConfig {
        max_open_ended_rows: 100_000,
        ..Default::default()
    };
    let cap_rows = cfg.max_open_ended_rows as usize;
    let mut engine = Engine::new(wb, cfg);

    // Install an Arrow sheet with no used cells in column A,
    // so used-bounds cannot be inferred for A:A.
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 1, 256);
        for _ in 0..256 {
            ab.append_row("Sheet1", &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }

    // Whole-column reference A:A => start/end rows are None.
    let r = ReferenceType::Range {
        sheet: Some("Sheet1".to_string()),
        start_row: None,
        start_col: Some(1),
        end_row: None,
        end_col: Some(1),
        start_row_abs: false,
        start_col_abs: false,
        end_row_abs: false,
        end_col_abs: false,
    };

    let view = engine.resolve_range_view(&r, "Sheet1").unwrap();
    let (rows, cols) = view.dims();

    assert_eq!(cols, 1);
    assert_eq!(rows, cap_rows, "expected A:A to use configured cap");
}
