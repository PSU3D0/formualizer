//! FORM211: declared dynamic-array anchors (imported source spill identity).
//!
//! `Engine::declare_dynamic_array_anchor` gives a formula vertex the identity
//! an XLSX XLDAPR anchor carries. Without a committed spill, a declared anchor
//! holding a current scalar resolves `A1#` / `_xlfn.ANCHORARRAY(A1)` as its
//! own 1x1 cell. A committed multi-cell spill still uses the registry, and
//! blocked, erroring and undeclared anchors keep `#REF!`.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn engine(parallel: bool) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: parallel,
            max_threads: if parallel { Some(4) } else { None },
            ..Default::default()
        },
    )
}

fn f(e: &mut Engine<TestWorkbook>, row: u32, col: u32, src: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(src).unwrap())
        .unwrap();
}

fn set(e: &mut Engine<TestWorkbook>, row: u32, col: u32, v: f64) {
    e.set_cell_value("Sheet1", row, col, LiteralValue::Number(v))
        .unwrap();
}

fn get(e: &Engine<TestWorkbook>, row: u32, col: u32) -> Option<LiteralValue> {
    match e.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) | None => None,
        other => other,
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

#[track_caller]
fn assert_err(value: Option<LiteralValue>, kind: ExcelErrorKind, ctx: &str) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, kind, "{ctx}: {e:?}"),
        other => panic!("{ctx}: expected {kind:?}, got {other:?}"),
    }
}

#[track_caller]
fn assert_ref(value: Option<LiteralValue>, ctx: &str) {
    assert_err(value, ExcelErrorKind::Ref, ctx);
}

/// Readers of the anchor at A1 in row 1: B1 `SUM(A1#)`, C1
/// `SUM(_xlfn.ANCHORARRAY(A1))`, D1 `ROWS(A1#)`.
fn readers(e: &mut Engine<TestWorkbook>) {
    f(e, 1, 2, "=SUM(A1#)");
    f(e, 1, 3, "=SUM(_xlfn.ANCHORARRAY(A1))");
    f(e, 1, 4, "=ROWS(A1#)");
}

#[track_caller]
fn assert_readers(e: &Engine<TestWorkbook>, sum: f64, rows: f64, ctx: &str) {
    assert_eq!(get(e, 1, 2), n(sum), "{ctx}: SUM(A1#)");
    assert_eq!(get(e, 1, 3), n(sum), "{ctx}: SUM(ANCHORARRAY(A1))");
    assert_eq!(get(e, 1, 4), n(rows), "{ctx}: ROWS(A1#)");
}

#[track_caller]
fn assert_readers_ref(e: &Engine<TestWorkbook>, ctx: &str) {
    for col in 2..=4 {
        assert_ref(get(e, 1, col), &format!("{ctx}: col {col}"));
    }
}

#[test]
fn declared_sequence_one_is_a_one_by_one_spill() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        f(&mut e, 1, 1, "=SEQUENCE(1,1,7)");
        readers(&mut e);
        e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, 1, 1), n(7.0), "scalarized anchor");
        assert_readers(&e, 7.0, 1.0, &format!("declared p={parallel}"));
    }
}

#[test]
fn undeclared_sequence_one_stays_ref() {
    let mut e = engine(false);
    f(&mut e, 1, 1, "=SEQUENCE(1,1,7)");
    readers(&mut e);
    e.evaluate_all().unwrap();
    assert_readers_ref(&e, "undeclared");
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);
}

#[test]
fn declaring_after_evaluation_dirties_the_readers() {
    let mut e = engine(false);
    f(&mut e, 1, 1, "=SEQUENCE(1,1,7)");
    readers(&mut e);
    e.evaluate_all().unwrap();
    assert_readers_ref(&e, "before declaring");
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_readers(&e, 7.0, 1.0, "after declaring");
}

#[test]
fn replacing_or_clearing_the_formula_drops_the_declaration() {
    // Replacing with the identical formula is still a replacement.
    let mut e = engine(false);
    f(&mut e, 1, 1, "=SEQUENCE(1,1,7)");
    readers(&mut e);
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_readers(&e, 7.0, 1.0, "declared");
    f(&mut e, 1, 1, "=SEQUENCE(1,1,7)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 1, 1), n(7.0));
    assert_readers_ref(&e, "after replacement");
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);

    // Overwriting with a value.
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_readers(&e, 7.0, 1.0, "redeclared");
    set(&mut e, 1, 1, 7.0);
    e.evaluate_all().unwrap();
    assert_readers_ref(&e, "after value overwrite");
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);

    // Bulk replacement.
    f(&mut e, 1, 1, "=SEQUENCE(1,1,7)");
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_readers(&e, 7.0, 1.0, "declared again");
    e.bulk_set_formulas("Sheet1", vec![(1, 1, parse("=SEQUENCE(1,1,7)").unwrap())])
        .unwrap();
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);
    // The bulk path dirties only the written cells, not their readers
    // (existing behavior), so re-enter the readers to observe the result.
    readers(&mut e);
    e.evaluate_all().unwrap();
    assert_readers_ref(&e, "after bulk replacement");
}

#[test]
fn declared_anchor_grows_into_the_registry_and_shrinks_back_to_one() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        set(&mut e, 1, 6, 1.0);
        f(&mut e, 1, 1, "=SEQUENCE($F$1)");
        readers(&mut e);
        e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_readers(&e, 1.0, 1.0, "1x1");
        for (rows, sum) in [(3.0, 6.0), (1.0, 1.0), (4.0, 10.0), (1.0, 1.0)] {
            set(&mut e, 1, 6, rows);
            e.evaluate_all().unwrap();
            let ctx = format!("rows={rows} p={parallel}");
            assert_readers(&e, sum, rows, &ctx);
            assert_eq!(
                e.graph.spill_registry_counts().0,
                usize::from(rows > 1.0),
                "{ctx}: multi-cell spills use the registry"
            );
        }
    }
}

#[test]
fn blocked_or_erroring_declared_anchor_stays_ref() {
    let mut e = engine(false);
    set(&mut e, 1, 6, 3.0);
    f(&mut e, 1, 1, "=SEQUENCE($F$1)");
    set(&mut e, 3, 1, 9.0); // blocker inside the spill
    readers(&mut e);
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_err(get(&e, 1, 1), ExcelErrorKind::Spill, "blocked anchor");
    assert_readers_ref(&e, "blocked");
    // Shrinking to one cell clears the obstruction: 1x1 again.
    set(&mut e, 1, 6, 1.0);
    e.evaluate_all().unwrap();
    assert_readers(&e, 1.0, 1.0, "unblocked 1x1");

    let mut e = engine(false);
    f(&mut e, 1, 1, "=1/0");
    readers(&mut e);
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_readers_ref(&e, "erroring");

    let mut e = engine(false);
    f(&mut e, 1, 1, "=Z99");
    readers(&mut e);
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert!(get(&e, 1, 1).is_none_or(|v| v == LiteralValue::Number(0.0)));
    if get(&e, 1, 1).is_none() {
        assert_readers_ref(&e, "empty result");
    }
}

#[test]
fn only_formula_cells_can_be_declared() {
    let mut e = engine(false);
    set(&mut e, 1, 1, 1.0);
    f(&mut e, 1, 2, "=1+1");
    for (sheet, row, col) in [
        ("Sheet1", 1, 1),
        ("Sheet1", 5, 5),
        ("Nope", 1, 2),
        ("Sheet1", 0, 2),
    ] {
        let err = e
            .declare_dynamic_array_anchor(sheet, row, col)
            .expect_err("not a formula cell");
        assert_eq!(err.kind, ExcelErrorKind::Ref);
    }
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);
    e.declare_dynamic_array_anchor("Sheet1", 1, 2).unwrap();
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 1);
}

#[test]
fn staged_formulas_must_be_built_before_declaring() {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            defer_graph_building: true,
            ..Default::default()
        },
    );
    e.stage_formula_text("Sheet1", 1, 1, "=SEQUENCE(1,1,7)".to_string());
    e.stage_formula_text("Sheet1", 1, 2, "=SUM(A1#)".to_string());
    assert!(e.declare_dynamic_array_anchor("Sheet1", 1, 1).is_err());
    e.build_graph_all().unwrap();
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 1, 2), n(7.0));
}

/// Structural edits: the vertex model moves the anchor's vertex (same id,
/// new cell), and a move clears the declaration. An edit that leaves the
/// anchor in place keeps it, even when it adjusts the anchor's references.
#[test]
fn structural_edits_that_move_the_anchor_clear_the_declaration() {
    let setup = || {
        let mut e = engine(false);
        set(&mut e, 1, 6, 1.0);
        f(&mut e, 1, 1, "=SEQUENCE($F$1)");
        readers(&mut e);
        e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_readers(&e, 1.0, 1.0, "declared");
        e
    };

    // Rows inserted below the anchor: it does not move; identity is kept.
    let mut e = setup();
    e.insert_rows("Sheet1", 5, 2).unwrap();
    e.evaluate_all().unwrap();
    assert_readers(&e, 1.0, 1.0, "insert rows below");
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 1);

    // Columns inserted between the anchor and its input (F1 -> H1): the
    // anchor's formula is adjusted, the anchor stays at A1; identity kept.
    e.insert_columns("Sheet1", 5, 2).unwrap();
    e.evaluate_all().unwrap();
    assert_readers(&e, 1.0, 1.0, "insert columns right of the readers");
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 1);

    // A row inserted above moves the anchor (A1 -> A2) and its readers,
    // whose references follow it (A2#). The declaration is cleared.
    let mut e = setup();
    e.insert_rows("Sheet1", 1, 1).unwrap();
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 2, 1), n(1.0), "moved anchor");
    for col in 2..=4 {
        assert_ref(get(&e, 2, col), &format!("row insert above: col {col}"));
    }
    // Declaring at the new cell restores it.
    e.declare_dynamic_array_anchor("Sheet1", 2, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 2, 2), n(1.0));

    // A column inserted before the anchor moves it too.
    let mut e = setup();
    e.insert_columns("Sheet1", 1, 1).unwrap();
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);
    e.evaluate_all().unwrap();
    assert_ref(get(&e, 1, 3), "column insert before");

    // Deleting the anchor's row removes the vertex.
    let mut e = setup();
    e.delete_rows("Sheet1", 1, 1).unwrap();
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);
}

#[test]
fn removing_the_sheet_drops_the_declaration() {
    let mut e = engine(false);
    f(&mut e, 1, 1, "=SEQUENCE(1)");
    e.add_sheet("Other").unwrap();
    e.declare_dynamic_array_anchor("Sheet1", 1, 1).unwrap();
    e.set_cell_formula("Other", 1, 1, parse("=SUM(Sheet1!A1#)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Other", 1, 1).map(|v| match v {
            LiteralValue::Int(i) => LiteralValue::Number(i as f64),
            v => v,
        }),
        n(1.0),
        "cross-sheet reader"
    );
    let sheet1 = e.sheet_id("Sheet1").unwrap();
    e.remove_sheet(sheet1).unwrap();
    assert_eq!(e.graph.declared_dynamic_anchor_count(), 0);
}
