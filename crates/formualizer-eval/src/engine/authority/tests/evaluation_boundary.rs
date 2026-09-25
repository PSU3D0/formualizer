//! M1b scope boundary: unsupported work never executes a legacy plan.
use crate::engine::{CancelToken, ChangeLog, Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, ExcelErrorKind};
use formualizer_parse::parse;

#[test]
fn unsupported_structural_state_rejects_all_evaluation_routes() {
    let config = EvalConfig {
        formula_plane_mode: FormulaPlaneMode::Off,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            2,
            1,
            formualizer_common::LiteralValue::Number(1.0),
        )
        .unwrap();
    // Any out-of-scope state (structural edits left scope in M3): the
    // typed error is the host's, whatever the operation.
    engine.graph.authority_mark_unsupported("insert_rows");
    let before = engine.get_cell_value("Sheet1", 2, 1);
    let check = |error: ExcelError| {
        assert_eq!(error.kind, ExcelErrorKind::NImpl);
        assert_eq!(
            error.message.as_deref(),
            Some("unified_authority: Unsupported { operation: \"insert_rows\" }")
        );
    };
    check(engine.evaluate_all().unwrap_err());
    check(engine.evaluate_all_with_delta().unwrap_err());
    check(
        engine
            .evaluate_all_cancellable(CancelToken::new())
            .unwrap_err(),
    );
    check(
        engine
            .evaluate_all_logged(&mut ChangeLog::new())
            .unwrap_err(),
    );
    check(engine.evaluate_cell("Sheet1", 2, 1).unwrap_err());
    check(engine.evaluate_cells(&[("Sheet1", 2, 1)]).unwrap_err());
    check(
        engine
            .evaluate_cells_cancellable(&[("Sheet1", 2, 1)], CancelToken::new())
            .unwrap_err(),
    );
    check(
        engine
            .evaluate_cells_with_delta(&[("Sheet1", 2, 1)])
            .unwrap_err(),
    );
    assert_eq!(engine.get_cell_value("Sheet1", 2, 1), before);
}
