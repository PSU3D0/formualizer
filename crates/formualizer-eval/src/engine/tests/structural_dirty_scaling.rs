//! Regression and scaling tests for structural edit, range op, undo, and redo
//! dirty propagation (#577).
//!
//! Work is asserted via `Engine::dirty_propagation_visits()` (and
//! `DependencyGraph::dirty_propagation_visits()`), verifying O(N) linear
//! propagation instead of O(N^2) per-event / per-cell closure walks.

use crate::engine::graph::editor::transaction_context::TransactionContext;
use crate::engine::graph::editor::{change_log::ChangeLog, undo_engine::UndoEngine};
use crate::engine::{DependencyGraph, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn col_label(mut col_1based: u32) -> String {
    let mut chars = Vec::new();
    while col_1based > 0 {
        let rem = (col_1based - 1) % 26;
        chars.push((b'A' + rem as u8) as char);
        col_1based = (col_1based - 1) / 26;
    }
    chars.reverse();
    chars.into_iter().collect()
}

fn build_row_chain(n: u32, family_execution: bool) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        family_execution,
        ..Default::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine.graph.sheet_id_mut("S");
    engine
        .set_cell_formula("S", 1, 1, parse("=1").unwrap())
        .unwrap();
    for r in 2..=n + 1 {
        engine
            .set_cell_formula("S", r, 1, parse(format!("=A{}+1", r - 1)).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn insert_rows_undo_redo_chain_linear_visits_and_exact_values() {
    crate::builtins::load_builtins();
    let n = 2_000u32;

    for family_execution in [true, false] {
        let mut e = build_row_chain(n, family_execution);
        assert_eq!(
            e.get_cell_value("S", n + 1, 1),
            Some(LiteralValue::Number((n + 1) as f64))
        );

        let sid = e.graph.sheet_id("S").unwrap();
        let mut log = ChangeLog::new();
        let mut undo = UndoEngine::new();

        // Insert 1 row before 0-based row 1 (Excel row 2), shifting A2..=A(n+1) down to A3..=A(n+2).
        let v0 = e.dirty_propagation_visits();
        e.edit_with_logger(&mut log, |ed| ed.insert_rows(sid, 1, 1))
            .unwrap()
            .unwrap();
        let insert_visits = e.dirty_propagation_visits() - v0;
        assert!(
            insert_visits <= 2 * u64::from(n),
            "family_execution={family_execution}: insert_rows visits must be O(N), got {insert_visits} for N={n}"
        );

        e.evaluate_all().unwrap();
        assert_eq!(e.get_cell_value("S", 1, 1), Some(LiteralValue::Number(1.0)));
        assert_eq!(e.get_cell_value("S", 3, 1), Some(LiteralValue::Number(2.0)));
        assert_eq!(
            e.get_cell_value("S", n + 2, 1),
            Some(LiteralValue::Number((n + 1) as f64))
        );

        // Undo insert_rows.
        let v0 = e.dirty_propagation_visits();
        e.undo_logged(&mut undo, &mut log).unwrap();
        let undo_visits = e.dirty_propagation_visits() - v0;
        assert!(
            undo_visits <= 2 * u64::from(n),
            "family_execution={family_execution}: undo_logged visits must be O(N), got {undo_visits} for N={n}"
        );

        e.evaluate_all().unwrap();
        assert_eq!(e.get_cell_value("S", 2, 1), Some(LiteralValue::Number(2.0)));
        assert_eq!(
            e.get_cell_value("S", n + 1, 1),
            Some(LiteralValue::Number((n + 1) as f64))
        );

        // Redo insert_rows.
        let v0 = e.dirty_propagation_visits();
        e.redo_logged(&mut undo, &mut log).unwrap();
        let redo_visits = e.dirty_propagation_visits() - v0;
        assert!(
            redo_visits <= 2 * u64::from(n),
            "family_execution={family_execution}: redo_logged visits must be O(N), got {redo_visits} for N={n}"
        );

        e.evaluate_all().unwrap();
        assert_eq!(e.get_cell_value("S", 3, 1), Some(LiteralValue::Number(2.0)));
        assert_eq!(
            e.get_cell_value("S", n + 2, 1),
            Some(LiteralValue::Number((n + 1) as f64))
        );

        // Verify live dependency chain after redo by editing A1.
        e.set_cell_formula("S", 1, 1, parse("=10").unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(
            e.get_cell_value("S", n + 2, 1),
            Some(LiteralValue::Number((n + 10) as f64))
        );
    }
}

#[test]
fn delete_rows_undo_redo_chain_linear_visits_and_exact_values() {
    crate::builtins::load_builtins();
    let n = 1_000u32;

    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.graph.sheet_id_mut("S");
    e.set_cell_formula("S", 1, 1, parse("=10").unwrap())
        .unwrap();
    // Row 2 is an unused spacer row that will be deleted, shifting rows 3..=n+2 up to 2..=n+1.
    e.set_cell_formula("S", 2, 1, parse("=999").unwrap())
        .unwrap();
    e.set_cell_formula("S", 3, 1, parse("=A1+1").unwrap())
        .unwrap();
    for r in 4..=n + 2 {
        e.set_cell_formula("S", r, 1, parse(format!("=A{}+1", r - 1)).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", n + 2, 1),
        Some(LiteralValue::Number((10 + n) as f64))
    );

    let sid = e.graph.sheet_id("S").unwrap();
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();

    // Delete 0-based row 1 (Excel row 2).
    let v0 = e.dirty_propagation_visits();
    e.edit_with_logger(&mut log, |ed| ed.delete_rows(sid, 1, 1))
        .unwrap()
        .unwrap();
    let delete_visits = e.dirty_propagation_visits() - v0;
    assert!(
        delete_visits <= 3 * u64::from(n),
        "delete_rows visits must be O(N), got {delete_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", 2, 1),
        Some(LiteralValue::Number(11.0))
    );
    assert_eq!(
        e.get_cell_value("S", n + 1, 1),
        Some(LiteralValue::Number((10 + n) as f64))
    );

    // Undo delete_rows.
    let v0 = e.dirty_propagation_visits();
    e.undo_logged(&mut undo, &mut log).unwrap();
    let undo_visits = e.dirty_propagation_visits() - v0;
    assert!(
        undo_visits <= 3 * u64::from(n),
        "delete_rows undo visits must be O(N), got {undo_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", 2, 1),
        Some(LiteralValue::Number(999.0))
    );
    assert_eq!(
        e.get_cell_value("S", n + 2, 1),
        Some(LiteralValue::Number((10 + n) as f64))
    );

    // Redo delete_rows.
    let v0 = e.dirty_propagation_visits();
    e.redo_logged(&mut undo, &mut log).unwrap();
    let redo_visits = e.dirty_propagation_visits() - v0;
    assert!(
        redo_visits <= 3 * u64::from(n),
        "delete_rows redo visits must be O(N), got {redo_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", n + 1, 1),
        Some(LiteralValue::Number((10 + n) as f64))
    );
}

#[test]
fn insert_and_delete_columns_undo_redo_chain_linear_visits_and_exact_values() {
    crate::builtins::load_builtins();
    let n = 500u32;

    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.graph.sheet_id_mut("S");
    e.set_cell_formula("S", 1, 1, parse("=1").unwrap()).unwrap();
    for c in 2..=n + 1 {
        let prev = col_label(c - 1);
        e.set_cell_formula("S", 1, c, parse(format!("={prev}1+1")).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", 1, n + 1),
        Some(LiteralValue::Number((n + 1) as f64))
    );

    let sid = e.graph.sheet_id("S").unwrap();
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();

    // Insert 1 column before 0-based col 1 (Excel col B).
    let v0 = e.dirty_propagation_visits();
    e.edit_with_logger(&mut log, |ed| ed.insert_columns(sid, 1, 1))
        .unwrap()
        .unwrap();
    let insert_visits = e.dirty_propagation_visits() - v0;
    assert!(
        insert_visits <= 3 * u64::from(n),
        "insert_columns visits must be O(N), got {insert_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("S", 1, 3), Some(LiteralValue::Number(2.0)));
    assert_eq!(
        e.get_cell_value("S", 1, n + 2),
        Some(LiteralValue::Number((n + 1) as f64))
    );

    let v0 = e.dirty_propagation_visits();
    e.undo_logged(&mut undo, &mut log).unwrap();
    let undo_visits = e.dirty_propagation_visits() - v0;
    assert!(
        undo_visits <= 3 * u64::from(n),
        "insert_columns undo visits must be O(N), got {undo_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", 1, n + 1),
        Some(LiteralValue::Number((n + 1) as f64))
    );

    let v0 = e.dirty_propagation_visits();
    e.redo_logged(&mut undo, &mut log).unwrap();
    let redo_visits = e.dirty_propagation_visits() - v0;
    assert!(
        redo_visits <= 3 * u64::from(n),
        "insert_columns redo visits must be O(N), got {redo_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", 1, n + 2),
        Some(LiteralValue::Number((n + 1) as f64))
    );

    // Now delete the inserted empty column (0-based col 1) and test delete/undo/redo.
    let v0 = e.dirty_propagation_visits();
    e.edit_with_logger(&mut log, |ed| ed.delete_columns(sid, 1, 1))
        .unwrap()
        .unwrap();
    let del_visits = e.dirty_propagation_visits() - v0;
    assert!(
        del_visits <= 3 * u64::from(n),
        "delete_columns visits must be O(N), got {del_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", 1, n + 1),
        Some(LiteralValue::Number((n + 1) as f64))
    );

    let v0 = e.dirty_propagation_visits();
    e.undo_logged(&mut undo, &mut log).unwrap();
    let del_undo_visits = e.dirty_propagation_visits() - v0;
    assert!(
        del_undo_visits <= 3 * u64::from(n),
        "delete_columns undo visits must be O(N), got {del_undo_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", 1, n + 2),
        Some(LiteralValue::Number((n + 1) as f64))
    );
}

#[test]
fn action_atomic_insert_rows_undo_redo_linear_visits_and_exact_values() {
    crate::builtins::load_builtins();
    let n = 1_000u32;
    let mut e = build_row_chain(n, true);
    let mut undo = UndoEngine::new();

    let v0 = e.dirty_propagation_visits();
    let (_, journal) = e
        .action_atomic_journal("insert_row", |tx| tx.insert_rows("S", 2, 1))
        .unwrap();
    let insert_visits = e.dirty_propagation_visits() - v0;
    undo.push_action(journal);
    assert!(
        insert_visits <= 3 * u64::from(n),
        "action_atomic insert_rows visits must be O(N), got {insert_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", n + 2, 1),
        Some(LiteralValue::Number((n + 1) as f64))
    );

    let v0 = e.dirty_propagation_visits();
    e.undo_action(&mut undo).unwrap();
    let undo_visits = e.dirty_propagation_visits() - v0;
    assert!(
        undo_visits <= 3 * u64::from(n),
        "undo_action visits must be O(N), got {undo_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", n + 1, 1),
        Some(LiteralValue::Number((n + 1) as f64))
    );

    let v0 = e.dirty_propagation_visits();
    e.redo_action(&mut undo).unwrap();
    let redo_visits = e.dirty_propagation_visits() - v0;
    assert!(
        redo_visits <= 3 * u64::from(n),
        "redo_action visits must be O(N), got {redo_visits} for N={n}"
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S", n + 2, 1),
        Some(LiteralValue::Number((n + 1) as f64))
    );
}

#[test]
fn clear_move_copy_range_over_chain_linear_visits() {
    crate::builtins::load_builtins();
    let n = 500u32;

    // clear_range over the first half of a chain: must not do O(N^2) mark_as_ref_error closures.
    {
        let mut e = build_row_chain(n, true);
        let sid = e.graph.sheet_id("S").unwrap();
        let v0 = e.dirty_propagation_visits();
        let clear_summary = {
            let mut ed = crate::engine::VertexEditor::new(&mut e.graph);
            ed.clear_range(sid, 0, 0, n / 2, 0).unwrap()
        };
        let clear_visits = e.dirty_propagation_visits() - v0;
        assert_eq!(clear_summary.cells_affected, (n / 2 + 1) as usize);
        assert!(
            clear_visits <= 2 * u64::from(n),
            "clear_range visits must be O(N), got {clear_visits} for N={n}"
        );
        // Remaining tail A(n/2 + 2)..=A(n + 1) must all be marked dirty.
        let dirty = e.graph.get_evaluation_vertices();
        assert_eq!(dirty.len(), (n - n / 2) as usize);
        let boundary = e.graph.make_cell_ref_internal(sid, n / 2 + 1, 0);
        let boundary_vid = e.graph.get_vertex_id_for_address(&boundary).unwrap();
        assert!(e.graph.is_ref_error(boundary_vid));
    }

    // move_range of a chain to another column: must be linear.
    {
        let mut e = build_row_chain(n, true);
        let sid = e.graph.sheet_id("S").unwrap();
        let v0 = e.dirty_propagation_visits();
        {
            let mut ed = crate::engine::VertexEditor::new(&mut e.graph);
            ed.move_range(sid, 0, 0, n, 0, sid, 0, 2).unwrap();
        }
        let move_visits = e.dirty_propagation_visits() - v0;
        assert!(
            move_visits <= 3 * u64::from(n),
            "move_range visits must be O(N), got {move_visits} for N={n}"
        );
    }

    // copy_range of a chain to another column: must be linear.
    {
        let mut e = build_row_chain(n, true);
        let sid = e.graph.sheet_id("S").unwrap();
        let v0 = e.dirty_propagation_visits();
        {
            let mut ed = crate::engine::VertexEditor::new(&mut e.graph);
            ed.copy_range(sid, 0, 0, n, 0, sid, 0, 2).unwrap();
        }
        let copy_visits = e.dirty_propagation_visits() - v0;
        assert!(
            copy_visits <= 3 * u64::from(n),
            "copy_range visits must be O(N), got {copy_visits} for N={n}"
        );
    }
}

#[test]
fn transaction_context_rollback_over_chain_linear_visits() {
    let n = 500u32;
    let mut graph = DependencyGraph::new_with_config(EvalConfig::default());
    let sid = graph.sheet_id_mut("S");
    graph
        .set_cell_formula("S", 1, 1, parse("=1").unwrap())
        .unwrap();
    for r in 2..=n + 1 {
        graph
            .set_cell_formula("S", r, 1, parse(format!("=A{}+1", r - 1)).unwrap())
            .unwrap();
    }
    let eval_verts = graph.get_evaluation_vertices();
    graph.clear_dirty_flags(&eval_verts);

    let v0 = graph.dirty_propagation_visits();
    {
        let mut tx = TransactionContext::new(&mut graph);
        tx.begin().unwrap();
        {
            let mut ed = tx.editor();
            ed.insert_rows(sid, 1, 1).unwrap();
        }
        tx.rollback().unwrap();
    }
    let visits = graph.dirty_propagation_visits() - v0;
    assert!(
        visits <= 3 * u64::from(n),
        "TransactionContext insert+rollback visits must be O(N), got {visits} for N={n}"
    );
}

#[test]
fn delete_rows_pre_edit_value_reader_dirtied_inside_deferred_scope() {
    // Regression guard for coordinate-frame safety when `delete_rows` runs inside a
    // deferred-dirty scope: a value cell in the deleted band has no vertex, so its
    // cross-sheet reader chain must be dirtied in the PRE-EDIT frame before rows shift.
    crate::builtins::load_builtins();
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.set_cell_value("S1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.set_cell_value("S1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    e.set_cell_formula("S2", 1, 1, parse("=S1!A1").unwrap())
        .unwrap();
    e.set_cell_formula("S2", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S2", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        e.get_cell_value("S2", 1, 2),
        Some(LiteralValue::Number(11.0))
    );

    let sid1 = e.graph.sheet_id("S1").unwrap();
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();

    e.edit_with_logger(&mut log, |ed| ed.delete_rows(sid1, 0, 1))
        .unwrap()
        .unwrap();
    e.evaluate_all().unwrap();
    assert!(matches!(
        e.get_cell_value("S2", 1, 1),
        Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Ref
    ));
    assert!(matches!(
        e.get_cell_value("S2", 1, 2),
        Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Ref
    ));

    e.undo_logged(&mut undo, &mut log).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S2", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        e.get_cell_value("S2", 1, 2),
        Some(LiteralValue::Number(11.0))
    );
}
