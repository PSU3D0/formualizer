//! Preserved independent 0.14 acceptance probes for FORM-000319/FORM-000327.
use formualizer_common::{ExcelErrorKind, LiteralValue as V};
use formualizer_workbook::{CellData, Workbook, WorkbookConfig};
use std::collections::BTreeMap;
fn make(family: bool, log: bool, defer: bool) -> Workbook {
    let mut c = WorkbookConfig::ephemeral();
    c.eval.family_execution = family;
    c.eval.defer_graph_building = defer;
    c.eval.enable_parallel = false;
    c.enable_changelog = log;
    let mut w = Workbook::new_with_config(c);
    w.add_sheet("S").unwrap();
    w.set_value("S", 1, 1, V::Number(5.)).unwrap();
    w.set_formula("S", 1, 2, "=SEQUENCE(A1)").unwrap();
    w.evaluate_all().unwrap();
    w
}
fn val(w: &Workbook, r: u32, n: f64) {
    assert_eq!(w.get_value("S", r, 2), Some(V::Number(n)));
}
fn empty(w: &Workbook, r: u32) {
    assert!(
        matches!(w.get_value("S", r, 2), None | Some(V::Empty)),
        "row {r}: {:?}",
        w.get_value("S", r, 2)
    );
}
#[test]
fn review_undo_ownership_after_shrink() {
    for family in [false, true] {
        for log in [false, true] {
            for defer in [false, true] {
                for bulk in [false, true] {
                    let mut w = make(family, log, defer);
                    if log {
                        w.begin_action("child edits");
                        if bulk {
                            w.set_values("S", 3, 2, &[vec![V::Number(99.)], vec![V::Number(88.)]])
                                .unwrap();
                        } else {
                            w.set_value("S", 3, 2, V::Number(99.)).unwrap();
                        }
                        w.end_action();
                    } else {
                        w.action("children", |a| {
                            if bulk {
                                a.set_values(
                                    "S",
                                    3,
                                    2,
                                    &[vec![V::Number(99.)], vec![V::Number(88.)]],
                                )
                            } else {
                                a.set_value("S", 3, 2, V::Number(99.))
                            }
                        })
                        .unwrap();
                    }
                    w.evaluate_all().unwrap();
                    w.undo().unwrap();
                    w.evaluate_all().unwrap();
                    val(&w, 3, 3.);
                    val(&w, 4, 4.);
                    w.set_value("S", 1, 1, V::Number(1.)).unwrap();
                    w.evaluate_all().unwrap();
                    empty(&w, 3);
                    empty(&w, 4);
                    println!("undo shrink family={family} log={log} defer={defer} bulk={bulk}");
                }
            }
        }
    }
}
#[test]
fn review_all_edit_routes_block_and_recover() {
    for family in [false, true] {
        for log in [false, true] {
            for defer in [false, true] {
                for route in 0..5 {
                    let mut w = make(family, log, defer);
                    match route {
                        0 => w.set_value("S", 4, 2, V::Number(99.)).unwrap(),
                        1 => w.set_values("S", 4, 2, &[vec![V::Number(99.)]]).unwrap(),
                        2 => {
                            let mut cells = BTreeMap::new();
                            cells.insert(
                                (4, 2),
                                CellData {
                                    value: Some(V::Number(99.)),
                                    formula: None,
                                    style: None,
                                },
                            );
                            w.write_range("S", (4, 2), cells).unwrap();
                        }
                        3 => w
                            .action("child", |a| a.set_value("S", 4, 2, V::Number(99.)))
                            .unwrap(),
                        _ => w
                            .action("children", |a| {
                                a.set_values("S", 4, 2, &[vec![V::Number(99.)]])
                            })
                            .unwrap(),
                    }
                    w.evaluate_all().unwrap();
                    val(&w, 4, 99.);
                    assert!(
                        matches!(w.get_value("S",1,2),Some(V::Error(e)) if e.kind==ExcelErrorKind::Spill),
                        "route={route} log={log} family={family}"
                    );
                    w.set_value("S", 4, 2, V::Empty).unwrap();
                    w.evaluate_all().unwrap();
                    val(&w, 5, 5.);
                }
            }
        }
    }
}
#[test]
fn review_blocker_outside_extent() {
    for family in [false, true] {
        for log in [false, true] {
            for preinstalled in [false, true] {
                let mut w = make(family, log, false);
                w.set_value("S", 1, 1, V::Number(1.)).unwrap();
                if preinstalled {
                    w.evaluate_all().unwrap();
                }
                w.set_value("S", 9, 2, V::Number(99.)).unwrap();
                w.set_value("S", 8, 2, V::Number(88.)).unwrap();
                w.set_value("S", 1, 1, V::Number(10.)).unwrap();
                w.evaluate_all().unwrap();
                w.set_value("S", 8, 2, V::Empty).unwrap();
                assert_eq!(w.evaluate_all().unwrap().computed_vertices, 1);
                val(&w, 9, 99.);
                w.set_value("S", 9, 2, V::Empty).unwrap();
                assert_eq!(w.evaluate_all().unwrap().computed_vertices, 1);
                val(&w, 10, 10.);
                w.set_value("S", 20, 2, V::Number(7.)).unwrap();
                assert_eq!(w.evaluate_all().unwrap().computed_vertices, 0);
            }
        }
    }
}

#[test]
fn review_first_request_configs() {
    use formualizer_eval::engine::EvaluationTarget;
    for config in 0..3 {
        for family in [false, true] {
            for api in 0..4 {
                let mut w = if config == 0 {
                    Workbook::new()
                } else {
                    let mut c = if config == 1 {
                        WorkbookConfig::ephemeral()
                    } else {
                        WorkbookConfig::interactive()
                    };
                    c.eval.family_execution = family;
                    c.eval.enable_parallel = config == 2;
                    c.eval.max_threads = Some(4);
                    Workbook::new_with_config(c)
                };
                w.add_sheet("S").unwrap();
                w.set_formula("S", 1, 4, "=SEQUENCE(4)").unwrap();
                w.set_formula("S", 1, 6, "=INDIRECT(\"D4\")+1").unwrap();
                match api {
                    0 => {
                        w.evaluate_all().unwrap();
                    }
                    1 => {
                        w.evaluate_cell("S", 1, 6).unwrap();
                    }
                    2 => {
                        w.evaluate_targets(&[EvaluationTarget::Cell {
                            sheet: "S".into(),
                            row: 1,
                            col: 6,
                        }])
                        .unwrap();
                    }
                    _ => {
                        w.engine_mut().evaluate_all_for_snapshot(None).unwrap();
                    }
                }
                assert_eq!(
                    w.get_value("S", 1, 6),
                    Some(V::Number(5.)),
                    "config={config} family={family} api={api}"
                );
            }
        }
    }
}
#[test]
fn review_formula_batch_routes() {
    for family in [false, true] {
        for log in [false, true] {
            for defer in [false, true] {
                for route in 0..3 {
                    let mut w = make(family, log, defer);
                    match route {
                        0 => w.set_formulas("S", 4, 2, &[vec!["=99".into()]]).unwrap(),
                        1 => {
                            let mut cells = BTreeMap::new();
                            cells.insert((4, 2), CellData::from_formula("=99"));
                            w.write_range("S", (4, 2), cells).unwrap();
                        }
                        _ => w
                            .action("formula", |a| a.set_formula("S", 4, 2, "=99"))
                            .unwrap(),
                    }
                    w.evaluate_all().unwrap();
                    val(&w, 4, 99.);
                    assert!(
                        matches!(w.get_value("S",1,2),Some(V::Error(e)) if e.kind==ExcelErrorKind::Spill)
                    );
                    w.set_value("S", 1, 1, V::Number(1.)).unwrap();
                    w.evaluate_all().unwrap();
                    val(&w, 4, 99.);
                }
            }
        }
    }
}
