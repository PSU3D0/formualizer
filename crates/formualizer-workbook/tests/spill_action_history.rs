//! FORM-000319: action-wide spill snapshots must survive later cell undo entries.
//! Adapted from the independent bulk_undo and review_014_acceptance probes.
use formualizer_common::LiteralValue as V;
use formualizer_workbook::{CellData, Workbook, WorkbookConfig};
use std::collections::BTreeMap;

fn snapshot(w: &Workbook) -> Vec<Option<V>> {
    [2, 5]
        .into_iter()
        .flat_map(|col| (1..=5).map(move |row| w.get_value("S", row, col)))
        .collect()
}

fn cells(route: usize) -> BTreeMap<(u32, u32), CellData> {
    let mut cells = BTreeMap::new();
    cells.insert(
        (3, 2),
        if route == 3 {
            CellData::from_formula("=99")
        } else {
            CellData::from_value(V::Number(99.))
        },
    );
    cells.insert(
        (4, 2),
        if route == 3 || route == 4 {
            CellData::from_formula("=88")
        } else {
            CellData::from_value(V::Number(88.))
        },
    );
    cells
}

fn exercise(route: usize) {
    for family in [false, true] {
        for log in [false, true] {
            for defer in [false, true] {
                for atomic in [false, true] {
                    // begin/end without logging has no history, as on the base.
                    if !atomic && !log {
                        continue;
                    }
                    for evaluated in [false, true] {
                        let context = format!(
                            "route={route} family={family} log={log} defer={defer} atomic={atomic} evaluated={evaluated}"
                        );
                        let mut c = WorkbookConfig::ephemeral();
                        c.eval.family_execution = family;
                        c.eval.defer_graph_building = defer;
                        c.eval.enable_parallel = false;
                        c.enable_changelog = log;
                        let mut w = Workbook::new_with_config(c);
                        w.add_sheet("S").unwrap();
                        w.set_value("S", 1, 1, V::Number(5.)).unwrap();
                        for col in [2, 5] {
                            w.set_formula("S", 1, col, "=SEQUENCE(A1)").unwrap();
                        }
                        w.evaluate_all().unwrap();
                        let original = snapshot(&w);
                        if atomic {
                            w.action("spill children", |a| {
                                match route {
                                    0 => a.set_values(
                                        "S",
                                        3,
                                        2,
                                        &[vec![V::Number(99.)], vec![V::Number(88.)]],
                                    )?,
                                    1 => {
                                        a.set_value("S", 3, 2, V::Number(99.))?;
                                        a.set_value("S", 4, 2, V::Number(88.))?;
                                    }
                                    2..=4 => a.write_range("S", (3, 2), cells(route))?,
                                    5 => {
                                        a.set_formula("S", 3, 2, "=99")?;
                                        a.set_formula("S", 4, 2, "=88")?;
                                    }
                                    6 => {
                                        a.set_values(
                                            "S",
                                            3,
                                            2,
                                            &[vec![V::Number(99.)], vec![V::Number(88.)]],
                                        )?;
                                        a.set_values(
                                            "S",
                                            3,
                                            5,
                                            &[vec![V::Number(66.)], vec![V::Number(55.)]],
                                        )?;
                                    }
                                    9 => {
                                        a.set_formula("S", 3, 2, "=99")?;
                                        a.set_formula("S", 4, 2, "=88")?;
                                    }
                                    10 => {
                                        a.set_value("S", 3, 2, V::Number(99.))?;
                                        a.set_formula("S", 4, 2, "=88")?;
                                    }
                                    7 | 8 => {
                                        a.set_value("S", 3, 2, V::Number(99.))?;
                                        if route == 7 {
                                            a.set_value("S", 1, 2, V::Number(77.))?;
                                        } else {
                                            a.set_formula("S", 1, 2, "=77")?;
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                                Ok(())
                            })
                            .unwrap();
                        } else {
                            w.begin_action("spill children");
                            match route {
                                0 => w
                                    .set_values(
                                        "S",
                                        3,
                                        2,
                                        &[vec![V::Number(99.)], vec![V::Number(88.)]],
                                    )
                                    .unwrap(),
                                1 => {
                                    w.set_value("S", 3, 2, V::Number(99.)).unwrap();
                                    w.set_value("S", 4, 2, V::Number(88.)).unwrap();
                                }
                                2..=4 => w.write_range("S", (3, 2), cells(route)).unwrap(),
                                5 => w
                                    .set_formulas(
                                        "S",
                                        3,
                                        2,
                                        &[vec!["=99".into()], vec!["=88".into()]],
                                    )
                                    .unwrap(),
                                6 => {
                                    w.set_values(
                                        "S",
                                        3,
                                        2,
                                        &[vec![V::Number(99.)], vec![V::Number(88.)]],
                                    )
                                    .unwrap();
                                    w.set_values(
                                        "S",
                                        3,
                                        5,
                                        &[vec![V::Number(66.)], vec![V::Number(55.)]],
                                    )
                                    .unwrap();
                                }
                                9 => {
                                    w.set_formula("S", 3, 2, "=99").unwrap();
                                    w.set_formula("S", 4, 2, "=88").unwrap();
                                }
                                10 => {
                                    w.set_value("S", 3, 2, V::Number(99.)).unwrap();
                                    w.set_formula("S", 4, 2, "=88").unwrap();
                                }
                                7 | 8 => {
                                    w.set_value("S", 3, 2, V::Number(99.)).unwrap();
                                    if route == 7 {
                                        w.set_value("S", 1, 2, V::Number(77.)).unwrap();
                                    } else {
                                        w.set_formula("S", 1, 2, "=77").unwrap();
                                    }
                                }
                                _ => unreachable!(),
                            }
                            w.end_action();
                        }
                        if evaluated {
                            w.evaluate_all().unwrap();
                        }
                        w.undo().unwrap();
                        assert_eq!(snapshot(&w), original, "immediate undo: {context}");
                        w.evaluate_all().unwrap();
                        assert_eq!(snapshot(&w), original, "recalculated undo: {context}");
                        w.redo().unwrap();
                        w.evaluate_all().unwrap();
                        assert_eq!(
                            w.get_value("S", 3, 2),
                            Some(V::Number(99.)),
                            "redo: {context}"
                        );
                        for col in [2, 5] {
                            for row in 1..=5 {
                                let value = w.get_value("S", row, col);
                                let expected = if col == 5 && route != 6 {
                                    Some(row as f64)
                                } else if row == 1 && !(col == 2 && matches!(route, 7 | 8)) {
                                    assert!(
                                        matches!(value, Some(V::Error(ref e)) if e.kind == formualizer_common::ExcelErrorKind::Spill),
                                        "redo anchor: {context}"
                                    );
                                    continue;
                                } else {
                                    match (col, row) {
                                        (2, 1) => Some(77.),
                                        (2, 3) => Some(99.),
                                        (2, 4) if !matches!(route, 7 | 8) => Some(88.),
                                        (5, 3) => Some(66.),
                                        (5, 4) => Some(55.),
                                        _ => None,
                                    }
                                };
                                match expected {
                                    Some(n) => assert_eq!(
                                        value,
                                        Some(V::Number(n)),
                                        "redo: {context} row={row} col={col}"
                                    ),
                                    None => assert!(
                                        matches!(value, None | Some(V::Empty)),
                                        "redo emptiness: {context} row={row} col={col}: {value:?}"
                                    ),
                                }
                            }
                        }
                        w.undo().unwrap();
                        w.evaluate_all().unwrap();
                        assert_eq!(snapshot(&w), original, "second undo: {context}");
                        // Ownership proof: restored children must clear on shrink.
                        w.set_value("S", 1, 1, V::Number(1.)).unwrap();
                        w.evaluate_all().unwrap();
                        for col in [2, 5] {
                            for row in 2..=5 {
                                assert!(
                                    matches!(w.get_value("S", row, col), None | Some(V::Empty)),
                                    "shrink: {context} row={row} col={col}"
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
fn bulk_values() {
    exercise(0);
}
#[test]
fn multiple_values() {
    exercise(1);
}
#[test]
fn range_values() {
    exercise(2);
}
#[test]
fn range_formulas() {
    exercise(3);
}
#[test]
fn mixed_range() {
    exercise(4);
}
#[test]
fn formula_batch() {
    exercise(5);
}
#[test]
fn two_spills() {
    exercise(6);
}
#[test]
fn child_then_literal_anchor() {
    exercise(7);
}
#[test]
fn child_then_formula_anchor() {
    exercise(8);
}
#[test]
fn multiple_formulas() {
    exercise(9);
}
#[test]
fn mixed_cell_calls() {
    exercise(10);
}
