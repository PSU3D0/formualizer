//! Legacy behavior pinned for open Excel/LibreOffice oracle items.
//!
//! Each test records what the engine does today where the reference semantics
//! are still open (Program 1 semantics log: OQ-1, OQ-S1-1, OQ-S2-1..4,
//! OQ-3D-1). Later milestones match this behavior until an oracle decides
//! otherwise; a change here must be logged there, never silently absorbed.

use crate::engine::graph::editor::reference_adjuster::MoveReferenceAdjuster;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use formualizer_parse::pretty::canonical_formula;

fn formula_text(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> String {
    let (ast, _) = engine.get_cell(sheet, row, col).expect("cell exists");
    canonical_formula(&ast.expect("formula cell"))
}

fn error_kind(value: Option<LiteralValue>) -> ExcelErrorKind {
    match value {
        Some(LiteralValue::Error(error)) => error.kind,
        other => panic!("expected an error value, got {other:?}"),
    }
}

fn three_d_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for sheet in ["Sheet2", "Sheet3", "Sheet4"] {
        engine.add_sheet(sheet).unwrap();
    }
    for sheet in ["Sheet1", "Sheet2", "Sheet3"] {
        engine
            .set_cell_value(sheet, 5, 1, LiteralValue::Number(1.0))
            .unwrap();
        engine
            .set_cell_value(sheet, 6, 1, LiteralValue::Number(10.0))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet4", 1, 1, parse("=SUM(Sheet1:Sheet3!A5)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

/// OQ-S1-1: a row insert on a member sheet leaves 3-D reference text as is.
/// 3-D references are not evaluated today (`#N/IMPL!`).
#[test]
fn oq_s1_1_three_d_reference_text_ignores_member_row_insert() {
    let mut engine = three_d_engine();
    assert_eq!(
        error_kind(engine.get_cell_value("Sheet4", 1, 1)),
        ExcelErrorKind::NImpl
    );
    engine.insert_rows("Sheet2", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        formula_text(&engine, "Sheet4", 1, 1),
        "=SUM(Sheet1:Sheet3!A5)"
    );
    assert_eq!(
        error_kind(engine.get_cell_value("Sheet4", 1, 1)),
        ExcelErrorKind::NImpl
    );
}

/// OQ-3D-1: deleting a 3-D endpoint sheet succeeds and leaves the text as is.
#[test]
fn oq_3d_1_endpoint_sheet_delete_keeps_three_d_text() {
    for endpoint in ["Sheet3", "Sheet1"] {
        let mut engine = three_d_engine();
        let sheet_id = engine.sheet_id(endpoint).unwrap();
        engine.remove_sheet(sheet_id).unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(
            formula_text(&engine, "Sheet4", 1, 1),
            "=SUM(Sheet1:Sheet3!A5)",
            "{endpoint}"
        );
        assert_eq!(
            error_kind(engine.get_cell_value("Sheet4", 1, 1)),
            ExcelErrorKind::NImpl,
            "{endpoint}"
        );
    }
}

/// Moves `Sheet1!A1:B2` to `Sheet{to+1}!D4` and renders the adjusted formula
/// that lives on `Sheet{formula_sheet+1}`; `None` means the text is unchanged.
fn moved(to_sheet: u16, formula: &str, formula_sheet: u16) -> Option<String> {
    let adjuster = MoveReferenceAdjuster::new(
        0,
        "Sheet1".to_string(),
        0,
        0,
        1,
        1,
        to_sheet,
        format!("Sheet{}", to_sheet + 1),
        3,
        3,
    );
    adjuster
        .adjust_if_references(&parse(formula).unwrap(), formula_sheet)
        .map(|ast| canonical_formula(&ast))
}

/// References fully inside the moved block follow it (baseline for OQ-S2-*).
#[test]
fn move_translates_references_fully_inside_the_source() {
    assert_eq!(moved(0, "=A1", 0).as_deref(), Some("=D4"));
    assert_eq!(moved(0, "=$A$1", 0).as_deref(), Some("=$D$4"));
    assert_eq!(moved(0, "=SUM(A1:B2)", 0).as_deref(), Some("=SUM(D4:E5)"));
}

/// OQ-S2-1: references into the destination (cells the move overwrites) are
/// not adjusted.
#[test]
fn oq_s2_1_references_into_overwritten_cells_are_unchanged() {
    assert_eq!(moved(0, "=D4", 0), None);
    assert_eq!(moved(0, "=SUM(D4:E5)", 0), None);
}

/// OQ-S2-2: ranges that only partly overlap the source are not adjusted.
#[test]
fn oq_s2_2_partial_overlap_ranges_are_unchanged() {
    assert_eq!(moved(0, "=SUM(A1:A5)", 0), None);
    assert_eq!(moved(0, "=SUM(B2:C3)", 0), None);
    assert_eq!(moved(1, "=SUM(A1:A5)", 0), None);
}

/// OQ-S2-3: names are never rewritten by a move (text level; the moved
/// formula is re-bound wherever it lands).
#[test]
fn oq_s2_3_names_are_not_rewritten_by_moves() {
    assert_eq!(moved(0, "=Name1+A1", 0).as_deref(), Some("=Name1 + D4"));
    assert_eq!(
        moved(1, "=Name1+A1", 0).as_deref(),
        Some("=Name1 + Sheet2!D4")
    );
    assert_eq!(moved(1, "=Name1", 0), None);
}

/// OQ-S2-4: qualifier rendering. A cross-sheet move qualifies the new target;
/// an explicit qualifier is kept even when it names the formula's own sheet;
/// an unqualified reference on another sheet is a different cell.
#[test]
fn oq_s2_4_move_qualifier_rendering() {
    assert_eq!(moved(1, "=A1", 0).as_deref(), Some("=Sheet2!D4"));
    assert_eq!(
        moved(1, "=SUM(A1:B2)", 0).as_deref(),
        Some("=SUM(Sheet2!D4:E5)")
    );
    assert_eq!(moved(1, "=Sheet1!A1", 1).as_deref(), Some("=Sheet2!D4"));
    assert_eq!(moved(0, "=Sheet1!A1", 1).as_deref(), Some("=Sheet1!D4"));
    assert_eq!(moved(0, "=A1", 1), None);
    assert_eq!(moved(1, "=A1", 1), None);
}

/// OQ-1: error kinds and phases for missing tables, sheets and externals on
/// the Engine `set_cell_formula` route.
#[test]
fn oq_1_missing_target_kinds_and_phases_on_set_cell_formula() {
    let set = |formula: &str| {
        // Pins the Strict preparation phase (explicit since 0.10's
        // BestEffort default).
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_preparation_policy(crate::engine::PreparationPolicy::Strict),
        );
        let result = engine.set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap());
        (engine, result)
    };
    for (formula, kind, message) in [
        (
            "=SUM(MissingTable[Amount])",
            ExcelErrorKind::Name,
            "Undefined table: MissingTable",
        ),
        (
            "=NoSheet!A1",
            ExcelErrorKind::Ref,
            "Sheet not found: NoSheet",
        ),
        (
            "=[1]Sheet1!A1",
            ExcelErrorKind::Name,
            "Undefined name: [1]Sheet1!A1",
        ),
        (
            "=SUM([1]Sheet1!A1:B2)",
            ExcelErrorKind::Name,
            "Undefined table: [1]Sheet1!A1:B2",
        ),
    ] {
        let (_, result) = set(formula);
        let error = result.expect_err(formula);
        assert_eq!(error.kind, kind, "{formula}");
        assert_eq!(error.message.as_deref(), Some(message), "{formula}");
    }
    for (formula, kind) in [
        ("=MissingName", ExcelErrorKind::Name),
        ("=SUM([1]Sheet1!$B:$B)", ExcelErrorKind::Ref),
    ] {
        let (mut engine, result) = set(formula);
        result.expect(formula);
        engine.evaluate_all().unwrap();
        assert_eq!(
            error_kind(engine.get_cell_value("Sheet1", 1, 1)),
            kind,
            "{formula}"
        );
    }
}
