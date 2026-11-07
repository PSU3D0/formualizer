//! FORM-000319 R3: failed actions restore projection ownership, not just values.
//! Ports the independent review_round2_state and rollback-probe literal cases.
use formualizer_common::LiteralValue as V;
use formualizer_workbook::{IoError, Workbook, WorkbookConfig};

fn abort() -> IoError {
    IoError::Backend {
        backend: "regression".into(),
        message: "deliberate abort".into(),
    }
}

fn snapshot(w: &Workbook) -> Vec<Option<V>> {
    [2, 5]
        .into_iter()
        .flat_map(|col| (1..=5).map(move |row| w.get_value("S", row, col)))
        .collect()
}

fn assert_projection(w: &Workbook) {
    for col in [2, 5] {
        for row in 1..=5 {
            assert!(
                w.engine().cell_has_spill_projection("S", row, col),
                "lost ownership at row={row} col={col}"
            );
        }
    }
}

fn make(config: WorkbookConfig) -> Workbook {
    let mut w = Workbook::new_with_config(config);
    w.add_sheet("S").unwrap();
    for col in [2, 5] {
        w.set_formula("S", 1, col, "=SEQUENCE(5)").unwrap();
    }
    w.evaluate_all().unwrap();
    w
}

#[test]
fn aborted_literal_action_must_keep_projection_on_recalc() {
    for family in [false, true] {
        let mut config = WorkbookConfig::interactive();
        config.eval.family_execution = family;
        config.eval.enable_parallel = false;
        let mut w = make(config);
        let original = snapshot(&w);
        let result: Result<(), IoError> = w.action("abort literal child", |a| {
            a.set_value("S", 3, 5, V::Number(66.))?;
            Err(abort())
        });
        assert!(result.is_err());
        assert_eq!(snapshot(&w), original, "immediate rollback");
        w.evaluate_all().unwrap();
        assert_eq!(
            snapshot(&w),
            original,
            "post-rollback recalc family={family}"
        );
        assert_projection(&w);
    }
}

#[test]
fn literal_rollback_matrix_preserves_ownership_and_later_edits() {
    for family in [false, true] {
        for log in [false, true] {
            for defer in [false, true] {
                // No outer action, empty outer action, or an already populated one.
                for outer in 0..3 {
                    for evaluate in [false, true] {
                        for multiple in [false, true] {
                            let mut config = WorkbookConfig::ephemeral();
                            config.eval.family_execution = family;
                            config.eval.defer_graph_building = defer;
                            config.eval.enable_parallel = false;
                            config.enable_changelog = log;
                            let mut w = make(config);
                            let original = snapshot(&w);
                            if outer != 0 {
                                w.begin_action("outer");
                            }
                            if outer == 2 {
                                w.set_value("S", 20, 1, V::Number(7.)).unwrap();
                            }
                            let len = w.changelog().len();
                            let revision = w.changelog().spill_clear_revision();
                            let result: Result<(), IoError> =
                                w.action("abort literal children", |a| {
                                    a.set_value("S", 3, 5, V::Number(66.))?;
                                    if multiple {
                                        a.set_value("S", 4, 5, V::Number(55.))?;
                                        a.set_value("S", 3, 2, V::Number(99.))?;
                                    }
                                    Err(abort())
                                });
                            assert!(result.is_err());
                            assert_eq!(w.changelog().len(), len);
                            assert_eq!(w.changelog().spill_clear_revision(), revision);
                            assert_eq!(snapshot(&w), original, "immediate rollback");
                            assert_projection(&w);
                            if evaluate {
                                w.evaluate_all().unwrap();
                                assert_eq!(snapshot(&w), original, "post-abort recalc");
                                assert_projection(&w);
                            }
                            if outer == 2 {
                                assert_eq!(w.get_value("S", 20, 1), Some(V::Number(7.)));
                            }
                            // Continue editing both spills, including the restored child.
                            // An atomic journal provides history even with the log disabled.
                            w.action("later edits", |a| {
                                a.set_value("S", 3, 5, V::Number(22.))?;
                                a.set_value("S", 3, 2, V::Number(99.))?;
                                a.set_formula("S", 4, 2, "=88")?;
                                Ok(())
                            })
                            .unwrap();
                            if outer != 0 {
                                w.end_action();
                            }
                            w.undo().unwrap();
                            assert_eq!(snapshot(&w), original, "undo later edits");
                            w.evaluate_all().unwrap();
                            assert_eq!(snapshot(&w), original, "recalc after later edits");
                            assert_projection(&w);
                            w.redo().unwrap();
                            w.evaluate_all().unwrap();
                            assert_eq!(w.get_value("S", 3, 5), Some(V::Number(22.)));
                            assert_eq!(w.get_value("S", 3, 2), Some(V::Number(99.)));
                            assert_eq!(w.get_value("S", 4, 2), Some(V::Number(88.)));
                            w.undo().unwrap();
                            w.evaluate_all().unwrap();
                            assert_eq!(snapshot(&w), original, "second undo");
                            assert_projection(&w);
                            // Shrink proves that projected children are still engine-owned.
                            for col in [2, 5] {
                                w.set_formula("S", 1, col, "=SEQUENCE(2)").unwrap();
                            }
                            w.evaluate_all().unwrap();
                            for col in [2, 5] {
                                for row in 3..=5 {
                                    assert!(matches!(
                                        w.get_value("S", row, col),
                                        None | Some(V::Empty)
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
