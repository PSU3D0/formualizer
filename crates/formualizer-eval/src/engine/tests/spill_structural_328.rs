//! FORM-000328: structural edits move Arrow cells while the committed spill
//! registry must never mistake shifted user cells for generated projections.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn form_000328_insert_above_spill_preserves_shifted_user_value() {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            family_execution: false,
            ..Default::default()
        },
    );
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=SEQUENCE(A1)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    e.set_cell_value("Sheet1", 4, 2, LiteralValue::Number(99.))
        .unwrap();
    e.insert_rows("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(99.))
    );
}

fn fixture(family: bool) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            family_execution: family,
            ..Default::default()
        },
    );
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.))
        .unwrap();
    e.set_cell_formula("Sheet1", 3, 4, parse("=SEQUENCE(A1,3)").unwrap())
        .unwrap();
    e
}

fn shift(e: &mut Engine<TestWorkbook>, op: usize, pos: u32, count: u32) {
    match op {
        0 => e.insert_rows("Sheet1", pos, count).unwrap(),
        1 => e.delete_rows("Sheet1", pos, count).unwrap(),
        2 => e.insert_columns("Sheet1", pos, count).unwrap(),
        _ => e.delete_columns("Sheet1", pos, count).unwrap(),
    };
}

fn values(e: &Engine<TestWorkbook>) -> Vec<LiteralValue> {
    (1..=12)
        .flat_map(|r| {
            (1..=11).map(move |c| {
                e.get_cell_value("Sheet1", r, c)
                    .unwrap_or(LiteralValue::Empty)
            })
        })
        .collect()
}

#[test]
fn structural_spill_matches_never_installed_control() {
    for family in [false, true] {
        for op in 0..4 {
            for pos in if op < 2 { [2, 3, 5, 9] } else { [3, 4, 5, 8] } {
                for count in [1, 2] {
                    for height in [1., 5., 7.] {
                        for after in [false, true] {
                            for user in 0..3 {
                                let mut a = fixture(family);
                                let mut b = fixture(family);
                                a.evaluate_all().unwrap();
                                for e in [&mut a, &mut b] {
                                    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(height))
                                        .unwrap();
                                    match user {
                                        1 => e
                                            .set_cell_value(
                                                "Sheet1",
                                                6,
                                                6,
                                                LiteralValue::Number(99.),
                                            )
                                            .unwrap(),
                                        2 => {
                                            e.set_cell_formula(
                                                "Sheet1",
                                                6,
                                                6,
                                                parse("=99").unwrap(),
                                            )
                                            .unwrap();
                                        }
                                        _ => (),
                                    }
                                }
                                if after {
                                    a.evaluate_all().unwrap();
                                    b.evaluate_all().unwrap();
                                }
                                shift(&mut a, op, pos, count);
                                shift(&mut b, op, pos, count);
                                a.evaluate_all().unwrap();
                                b.evaluate_all().unwrap();
                                assert_eq!(
                                    values(&a),
                                    values(&b),
                                    "family={family} op={op} pos={pos} count={count} height={height} after={after} user={user}"
                                );
                                assert_eq!(
                                    a.graph.spill_registry_counts(),
                                    b.graph.spill_registry_counts()
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn structural_spill_moved_blockers_wake_after_removal() {
    use formualizer_common::ExcelErrorKind;
    for family in [false, true] {
        for columns in [false, true] {
            for formula in [false, true] {
                let mut e = fixture(family);
                if columns {
                    e.set_cell_formula("Sheet1", 3, 4, parse("=SEQUENCE(3,5)").unwrap())
                        .unwrap();
                }
                e.evaluate_all().unwrap();
                let (r, c) = if columns { (4, 10) } else { (9, 4) };
                if formula {
                    e.set_cell_formula("Sheet1", r, c, parse("=99").unwrap())
                        .unwrap();
                } else {
                    e.set_cell_value("Sheet1", r, c, LiteralValue::Number(99.))
                        .unwrap();
                }
                if columns {
                    e.delete_columns("Sheet1", 6, 2).unwrap();
                } else {
                    e.delete_rows("Sheet1", 5, 2).unwrap();
                }
                e.evaluate_all().unwrap();
                let (r, c) = if columns { (4, 8) } else { (7, 4) };
                assert_eq!(
                    e.get_cell_value("Sheet1", r, c),
                    Some(LiteralValue::Number(99.))
                );
                assert!(
                    matches!(e.get_cell_value("Sheet1",3,4),Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Spill)
                );
                e.set_cell_value("Sheet1", r, c, LiteralValue::Empty)
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(
                    e.get_cell_value("Sheet1", 3, 4),
                    Some(LiteralValue::Number(1.))
                );
                assert!(e.graph.spill_registry_counts().1 > 0);
            }
        }
    }
}

#[test]
fn structural_spill_clears_compacted_projection_values() {
    crate::builtins::load_builtins();
    for family in [false, true] {
        for op in 0..4 {
            let mut e = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    enable_parallel: false,
                    family_execution: family,
                    max_overlay_memory_bytes: Some(1),
                    ..Default::default()
                },
            );
            e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.))
                .unwrap();
            e.set_cell_formula("Sheet1", 3, 4, parse("=SEQUENCE(A1,3)").unwrap())
                .unwrap();
            e.evaluate_all().unwrap();
            assert!(e.debug_overlay_compactions() > 0);
            // The old output is already in base lanes; subsequent Empty
            // tombstones must survive structural computed-cache invalidation.
            e.config.max_overlay_memory_bytes = None;
            e.set_cell_value("Sheet1", 6, 6, LiteralValue::Number(99.))
                .unwrap();
            e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.))
                .unwrap();
            shift(&mut e, op, if op < 2 { 2 } else { 3 }, 1);
            e.evaluate_all().unwrap();
            let (r, c) = match op {
                0 => (7, 6),
                1 => (5, 6),
                2 => (6, 7),
                _ => (6, 5),
            };
            assert_eq!(
                e.get_cell_value("Sheet1", r, c),
                Some(LiteralValue::Number(99.))
            );
            let (r, c) = match op {
                0 => (8, 4),
                1 => (6, 4),
                2 => (7, 5),
                _ => (7, 3),
            };
            assert_eq!(
                e.get_cell_value("Sheet1", r, c),
                None,
                "compacted orphan op={op}"
            );
        }
    }
}

#[test]
fn structural_spill_snapshot_preserves_preexisting_intrusion_ownership() {
    use crate::engine::{EditorError, graph::editor::undo_engine::UndoEngine};
    for family in [false, true] {
        for columns in [false, true] {
            for abort in [false, true] {
                let mut e = fixture(family);
                e.evaluate_all().unwrap();
                // Direct child edits leave a pending spill with ownership holes.
                e.set_cell_value("Sheet1", 5, 6, LiteralValue::Number(99.))
                    .unwrap();
                e.set_cell_formula("Sheet1", 6, 6, parse("=88").unwrap())
                    .unwrap();
                let before = values(&e);
                let result = e.action_atomic_journal("shift pending intrusion", |tx| {
                    if columns {
                        tx.insert_columns("Sheet1", 3, 2)?;
                    } else {
                        tx.insert_rows("Sheet1", 2, 2)?;
                    }
                    // A second shift exercises operation-local coordinate frames.
                    tx.insert_rows("Sheet1", 2, 1)?;
                    if abort {
                        return Err(EditorError::TransactionFailed {
                            reason: "intentional".into(),
                        });
                    }
                    Ok(())
                });
                let mut undo = UndoEngine::new();
                if abort {
                    assert!(result.is_err());
                } else {
                    undo.push_action(result.unwrap().1);
                    e.evaluate_all().unwrap();
                    e.undo_action(&mut undo).unwrap();
                }
                assert_eq!(
                    values(&e),
                    before,
                    "restored pending snapshot family={family} columns={columns} abort={abort}"
                );
                let sheet = e.graph.sheet_id("Sheet1").unwrap();
                let user = crate::reference::CellRef::new(
                    sheet,
                    crate::reference::Coord::new(4, 5, true, true),
                );
                assert_eq!(e.graph.spill_registry_anchor_for_cell(user), None);
                e.evaluate_all().unwrap();
                assert_eq!(
                    e.get_cell_value("Sheet1", 5, 6),
                    Some(LiteralValue::Number(99.))
                );
                assert_eq!(
                    e.get_cell_value("Sheet1", 6, 6),
                    Some(LiteralValue::Number(88.))
                );
                assert_eq!(e.get_cell_value("Sheet1", 7, 6), None);
                e.set_cell_value("Sheet1", 5, 6, LiteralValue::Empty)
                    .unwrap();
                e.set_cell_value("Sheet1", 6, 6, LiteralValue::Empty)
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(
                    e.get_cell_value("Sheet1", 7, 6),
                    Some(LiteralValue::Number(15.))
                );
            }
        }
    }
}

#[test]
fn structural_spill_insert_journal_restores_projection_and_user_cells() {
    use crate::engine::{EditorError, graph::editor::undo_engine::UndoEngine};
    for family in [false, true] {
        for columns in [false, true] {
            for child_first in [false, true] {
                for formula in [false, true] {
                    for abort in [false, true] {
                        let mut e = fixture(family);
                        e.evaluate_all().unwrap();
                        let original = values(&e);
                        let result = e.action_atomic_journal("structural spill", |tx| {
                            let edit = |tx: &mut crate::engine::EngineAction<'_, TestWorkbook>,
                                        r,
                                        c| {
                                tx.set_cell_value("Sheet1", r, c, LiteralValue::Number(99.))?;
                                if formula {
                                    tx.set_cell_formula("Sheet1", r + 1, c, parse("=88").unwrap())?;
                                } else {
                                    tx.set_cell_value(
                                        "Sheet1",
                                        r + 1,
                                        c,
                                        LiteralValue::Number(88.),
                                    )?;
                                }
                                Ok::<_, EditorError>(())
                            };
                            if child_first {
                                edit(tx, 5, 6)?;
                            }
                            if columns {
                                tx.insert_columns("Sheet1", 3, 2)?;
                            } else {
                                tx.insert_rows("Sheet1", 2, 2)?;
                            }
                            if !child_first {
                                edit(tx, if columns { 5 } else { 7 }, if columns { 8 } else { 6 })?;
                            }
                            if abort {
                                return Err(EditorError::TransactionFailed {
                                    reason: "intentional".into(),
                                });
                            }
                            Ok(())
                        });
                        if abort {
                            assert!(result.is_err());
                            assert_eq!(values(&e), original, "rollback before evaluate");
                            e.evaluate_all().unwrap();
                            assert_eq!(values(&e), original, "rollback after evaluate");
                        } else {
                            let (_, journal) = result.unwrap();
                            let mut undo = UndoEngine::new();
                            undo.push_action(journal);
                            e.evaluate_all().unwrap();
                            let edited = values(&e);
                            e.undo_action(&mut undo).unwrap();
                            assert_eq!(values(&e), original, "undo before evaluate");
                            e.evaluate_all().unwrap();
                            assert_eq!(values(&e), original, "undo after evaluate");
                            e.redo_action(&mut undo).unwrap();
                            e.evaluate_all().unwrap();
                            assert_eq!(values(&e), edited, "redo");
                        }
                        // Restored ownership must still clear cleanly on later shrink.
                        if abort {
                            e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.))
                                .unwrap();
                            e.evaluate_all().unwrap();
                            assert_eq!(e.get_cell_value("Sheet1", 7, 6), None);
                        }
                    }
                }
            }
        }
    }
}

// Compacted EngineAction regressions ported from the independent R328 review.
mod compacted_action_review {
    use super::*;
    use crate::engine::{EditorError, graph::editor::undo_engine::UndoEngine};
    use formualizer_common::LiteralValue as V;
    fn fixture(compact: bool) -> Engine<TestWorkbook> {
        crate::builtins::load_builtins();
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                enable_parallel: false,
                family_execution: false,
                max_overlay_memory_bytes: compact.then_some(1),
                ..Default::default()
            },
        );
        e.set_cell_value("Sheet1", 1, 1, V::Number(4.)).unwrap();
        e.set_cell_formula("Sheet1", 4, 4, parse("=SEQUENCE(A1,A1,11)").unwrap())
            .unwrap();
        e
    }
    fn grid(e: &Engine<TestWorkbook>) -> Vec<V> {
        (1..=13)
            .flat_map(|r| {
                (1..=13).map(move |c| e.get_cell_value("Sheet1", r, c).unwrap_or(V::Empty))
            })
            .collect()
    }
    #[test]
    fn insert_history_second_undo_and_failed_action() {
        let mut mismatch = false;
        for columns in [false, true] {
            for child_first in [false, true] {
                for abort in [false, true] {
                    let mut e = fixture(true);
                    e.evaluate_all().unwrap();
                    assert!(e.debug_overlay_compactions() > 0);
                    e.config.max_overlay_memory_bytes = None;
                    let original = grid(&e);
                    let result = e.action_atomic_journal("independent", |tx| {
                        if child_first {
                            tx.set_cell_value("Sheet1", 6, 6, V::Number(87.))?;
                            tx.set_cell_formula("Sheet1", 7, 6, parse("=92").unwrap())?;
                        }
                        if columns {
                            tx.insert_columns("Sheet1", 4, 1)?;
                        } else {
                            tx.insert_rows("Sheet1", 4, 1)?;
                        }
                        let (r, c) = if columns { (6, 7) } else { (7, 6) };
                        tx.set_cell_value("Sheet1", r, c, V::Number(97.))?;
                        tx.set_cell_formula("Sheet1", r + 1, c, parse("=103").unwrap())?;
                        // Many ordinary edits must not trigger rescans of prior action events.
                        for n in 20..=120 {
                            tx.set_cell_value("Sheet1", n, 20, V::Number(f64::from(n)))?;
                        }
                        if abort {
                            return Err(EditorError::TransactionFailed {
                                reason: "expected".into(),
                            });
                        }
                        Ok(())
                    });
                    if abort {
                        assert!(result.is_err());
                        assert_eq!(grid(&e), original);
                        e.evaluate_all().unwrap();
                        assert_eq!(grid(&e), original);
                    } else {
                        let mut u = UndoEngine::new();
                        u.push_action(result.unwrap().1);
                        e.evaluate_all().unwrap();
                        let edited = grid(&e);
                        e.undo_action(&mut u).unwrap();
                        assert_eq!(grid(&e), original);
                        e.evaluate_all().unwrap();
                        assert_eq!(grid(&e), original);
                        e.redo_action(&mut u).unwrap();
                        e.evaluate_all().unwrap();
                        if grid(&e) != edited {
                            mismatch = true;
                            eprintln!("redo mismatch columns={columns} child_first={child_first}");
                        }
                        e.undo_action(&mut u).unwrap();
                        assert_eq!(grid(&e), original);
                        e.evaluate_all().unwrap();
                        assert_eq!(grid(&e), original);
                        e.set_cell_value("Sheet1", 1, 1, V::Number(1.)).unwrap();
                        e.evaluate_all().unwrap();
                        assert_eq!(e.get_cell_value("Sheet1", 7, 7), None);
                    }
                }
            }
        }
        assert!(
            !mismatch,
            "redo differed from first forward state; all second undos and aborts completed"
        );
    }
    #[test]
    fn compacted_action_child_before_insert_minimal() {
        for columns in [false, true] {
            let mut e = fixture(true);
            e.evaluate_all().unwrap();
            assert!(e.debug_overlay_compactions() > 0);
            e.config.max_overlay_memory_bytes = None;
            e.action_atomic_journal("minimal", |tx| {
                tx.set_cell_value("Sheet1", 6, 6, V::Number(87.))?;
                tx.set_cell_formula("Sheet1", 7, 6, parse("=92").unwrap())?;
                if columns {
                    tx.insert_columns("Sheet1", 4, 1)?;
                } else {
                    tx.insert_rows("Sheet1", 4, 1)?;
                }
                Ok(())
            })
            .unwrap();
            e.evaluate_all().unwrap();
            let (r, c) = if columns { (4, 6) } else { (6, 4) };
            assert_eq!(
                e.get_cell_value("Sheet1", r, c).unwrap_or(V::Empty),
                V::Empty,
                "orphan columns={columns} at=({r},{c})"
            );
            // Historical projection coordinates edited by the user must retain
            // their literal/formula results rather than receiving Empty masks.
            for old_row in 4..=7 {
                for old_col in 4..=7 {
                    if (old_row, old_col) == (4, 4) {
                        continue; // The surviving anchor reports the blocked spill.
                    }
                    let (row, col) = if columns {
                        (old_row, old_col + 1)
                    } else {
                        (old_row + 1, old_col)
                    };
                    let expected = match (old_row, old_col) {
                        (6, 6) => V::Number(87.),
                        (7, 6) => V::Number(92.),
                        _ => V::Empty,
                    };
                    assert_eq!(
                        e.get_cell_value("Sheet1", row, col).unwrap_or(V::Empty),
                        expected,
                        "columns={columns} at=({row},{col})"
                    );
                }
            }
        }
    }
}
