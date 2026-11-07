//! FORM-000328: structural edits retire old projections before moving user cells.
use formualizer_common::LiteralValue as V;
#[cfg(feature = "umya")]
use formualizer_workbook::{CalamineAdapter, LoadStrategy, SpreadsheetReader};
use formualizer_workbook::{IoError, Workbook, WorkbookConfig};

fn fixture(family: bool, logged: bool) -> Workbook {
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.family_execution = family;
    cfg.eval.enable_parallel = false;
    cfg.eval.defer_graph_building = logged;
    cfg.enable_changelog = logged;
    let mut w = Workbook::new_with_config(cfg);
    w.add_sheet("S").unwrap();
    w.set_value("S", 1, 1, V::Number(5.)).unwrap();
    w.set_formula("S", 3, 4, "=SEQUENCE(A1,3)").unwrap();
    w
}

fn shift(w: &mut Workbook, op: usize, pos: u32, count: u32) {
    let e = w.engine_mut();
    match op {
        0 => e.insert_rows("S", pos, count).unwrap(),
        1 => e.delete_rows("S", pos, count).unwrap(),
        2 => e.insert_columns("S", pos, count).unwrap(),
        _ => e.delete_columns("S", pos, count).unwrap(),
    };
}

fn values(w: &Workbook) -> Vec<V> {
    (1..=12)
        .flat_map(|r| (1..=11).map(move |c| w.get_value("S", r, c).unwrap_or(V::Empty)))
        .collect()
}

#[cfg(feature = "umya")]
fn clean_load(w: Workbook, family: bool) -> Workbook {
    let bytes = w.to_xlsx_bytes().unwrap();
    let reader = CalamineAdapter::open_bytes(bytes).unwrap();
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.family_execution = family;
    cfg.eval.enable_parallel = false;
    Workbook::from_reader(reader, LoadStrategy::EagerAll, cfg).unwrap()
}

#[test]
fn structural_spills_match_clean_control_across_workbook_routes() {
    for family in [false, true] {
        for route in 0..if cfg!(feature = "umya") { 3 } else { 2 } {
            for op in 0..4 {
                for pos in if op < 2 { [2, 3, 5, 9] } else { [3, 4, 5, 8] } {
                    for count in [1, 2] {
                        for height in [1., 5., 7.] {
                            for after in [false, true] {
                                for user in 0..3 {
                                    let mut a = fixture(family, route == 1);
                                    let mut b = fixture(family, route == 1);
                                    #[cfg(feature = "umya")]
                                    if route == 2 {
                                        a = clean_load(a, family);
                                        b = clean_load(b, family);
                                    }
                                    a.evaluate_all().unwrap();
                                    for w in [&mut a, &mut b] {
                                        w.set_value("S", 1, 1, V::Number(height)).unwrap();
                                        match user {
                                            1 => w.set_value("S", 6, 6, V::Number(99.)).unwrap(),
                                            2 => w.set_formula("S", 6, 6, "=99").unwrap(),
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
                                        "family={family} route={route} op={op} pos={pos} count={count} height={height} after={after} user={user}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn structural_spills_preserve_workbook_child_action_history() {
    workbook_child_action_history(true);
    workbook_child_action_history(false);
}

fn workbook_child_action_history(logged: bool) {
    for family in [false, true] {
        for op in 0..4 {
            for atomic in [false, true] {
                for formula in [false, true] {
                    for abort in [false, true] {
                        if !atomic && (abort || !logged) {
                            continue;
                        }
                        let mut w = fixture(family, logged);
                        w.evaluate_all().unwrap();
                        shift(&mut w, op, if op < 2 { 2 } else { 3 }, 1);
                        w.evaluate_all().unwrap();
                        let original = values(&w);
                        let (r, c) = match op {
                            0 => (6, 6),
                            1 => (4, 6),
                            2 => (5, 7),
                            _ => (5, 5),
                        };
                        if atomic {
                            let result: Result<(), IoError> =
                                w.action("children after shift", |a| {
                                    a.set_value("S", r, c, V::Number(99.))?;
                                    if formula {
                                        a.set_formula("S", r + 1, c, "=88")?;
                                    } else {
                                        a.set_value("S", r + 1, c, V::Number(88.))?;
                                    }
                                    if abort {
                                        return Err(IoError::Unsupported {
                                            feature: "intentional abort".into(),
                                            context: "structural spill".into(),
                                        });
                                    }
                                    Ok(())
                                });
                            if abort {
                                assert!(result.is_err());
                            } else {
                                result.unwrap();
                            }
                        } else {
                            w.begin_action("children after shift");
                            w.set_value("S", r, c, V::Number(99.)).unwrap();
                            if formula {
                                w.set_formula("S", r + 1, c, "=88").unwrap();
                            } else {
                                w.set_value("S", r + 1, c, V::Number(88.)).unwrap();
                            }
                            w.end_action();
                        }
                        if abort {
                            assert_eq!(values(&w), original, "rollback before recalc");
                            w.evaluate_all().unwrap();
                            assert_eq!(values(&w), original);
                        } else {
                            w.evaluate_all().unwrap();
                            let edited = values(&w);
                            assert_eq!(w.get_value("S", r, c), Some(V::Number(99.)));
                            w.undo().unwrap();
                            assert_eq!(values(&w), original, "undo before recalc");
                            w.evaluate_all().unwrap();
                            assert_eq!(values(&w), original, "undo after recalc");
                            w.redo().unwrap();
                            w.evaluate_all().unwrap();
                            assert_eq!(values(&w), edited);
                        }
                    }
                }
            }
        }
    }
}
