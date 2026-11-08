use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::{Workbook, WorkbookConfig};

fn make(family: bool, defer: bool, log: bool) -> Workbook {
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.family_execution = family;
    cfg.eval.defer_graph_building = defer;
    cfg.eval.enable_parallel = false;
    cfg.enable_changelog = log;
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Number(5.)).unwrap();
    wb.set_formula("S", 1, 2, "=SEQUENCE(A1)").unwrap();
    wb.set_formula("S", 1, 3, "=B1+1").unwrap();
    wb
}
#[test]
fn workbook_user_values_survive_spill_shrink_clear_and_relocation() {
    for family in [false, true] {
        for defer in [false, true] {
            for log in [false, true] {
                for bulk in [false, true] {
                    for action in 0..3 {
                        let mut wb = make(family, defer, log);
                        wb.evaluate_all().unwrap();
                        wb.set_value("S", 1, 1, LiteralValue::Number(1.)).unwrap();
                        if bulk {
                            wb.set_values("S", 4, 2, &[vec![LiteralValue::Number(99.)]])
                                .unwrap();
                        } else {
                            wb.set_value("S", 4, 2, LiteralValue::Number(99.)).unwrap();
                        }
                        match action {
                            0 => {}
                            1 => {
                                wb.set_value("S", 1, 2, LiteralValue::Empty).unwrap();
                            }
                            _ => {
                                wb.set_value("S", 1, 2, LiteralValue::Empty).unwrap();
                                wb.set_formula("S", 1, 5, "=SEQUENCE(A1)").unwrap();
                            }
                        }
                        wb.evaluate_all().unwrap();
                        assert_eq!(
                            wb.get_value("S", 4, 2),
                            Some(LiteralValue::Number(99.)),
                            "family={family} defer={defer} log={log} bulk={bulk} action={action}"
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn workbook_literal_and_formula_intrusions_block_live_spill() {
    for family in [false, true] {
        for log in [false, true] {
            for formula in [false, true] {
                let mut wb = make(family, true, log);
                wb.evaluate_all().unwrap();
                if formula {
                    wb.set_formula("S", 4, 2, "=99").unwrap();
                } else {
                    wb.set_value("S", 4, 2, LiteralValue::Number(99.)).unwrap();
                }
                wb.evaluate_all().unwrap();
                assert_eq!(wb.get_value("S", 4, 2), Some(LiteralValue::Number(99.)));
                assert!(matches!(wb.get_value("S",1,2),
                    Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Spill));
            }
        }
    }
}
#[cfg(feature = "umya")]
#[test]
fn xlsx_roundtrip_keeps_user_value_after_spill_shrink() {
    use formualizer_workbook::{CalamineAdapter, LoadStrategy, SpreadsheetReader};
    for family in [false, true] {
        let mut wb = make(family, true, true);
        wb.evaluate_all().unwrap();
        wb.set_value("S", 1, 1, LiteralValue::Number(1.)).unwrap();
        wb.set_value("S", 4, 2, LiteralValue::Number(99.)).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 4, 2), Some(LiteralValue::Number(99.)));
        let bytes = wb.to_xlsx_bytes().unwrap();
        #[cfg(feature = "xlsx-recalc")]
        let bytes = formualizer_workbook::recalculate_xlsx_bytes(&bytes, Default::default())
            .unwrap()
            .bytes;
        let reader = CalamineAdapter::open_bytes(bytes).unwrap();
        let mut cfg = WorkbookConfig::ephemeral();
        cfg.eval.family_execution = family;
        let mut loaded = Workbook::from_reader(reader, LoadStrategy::EagerAll, cfg).unwrap();
        loaded.evaluate_all().unwrap();
        assert_eq!(loaded.get_value("S", 4, 2), Some(LiteralValue::Number(99.)));
    }
}

#[test]
fn child_literal_undo_redo_restores_projection_ownership() {
    for family in [false, true] {
        for evaluated in [false, true] {
            for log in [false, true] {
                let mut wb = make(family, true, log);
                wb.evaluate_all().unwrap();
                if log {
                    wb.set_value("S", 4, 2, LiteralValue::Number(99.)).unwrap();
                } else {
                    wb.action("edit spill child", |a| {
                        a.set_value("S", 4, 2, LiteralValue::Number(99.))
                    })
                    .unwrap();
                }
                if evaluated {
                    wb.evaluate_all().unwrap();
                }
                wb.undo().unwrap();
                wb.evaluate_all().unwrap_or_else(|err| {
                    panic!("undo recalc family={family} evaluated={evaluated} log={log}: {err:?}")
                });
                for row in 1..=5 {
                    assert_eq!(
                        wb.get_value("S", row, 2),
                        Some(LiteralValue::Number(row as f64)),
                        "family={family} evaluated={evaluated} log={log}"
                    );
                }
                wb.redo().unwrap();
                wb.evaluate_all().unwrap();
                assert_eq!(wb.get_value("S", 4, 2), Some(LiteralValue::Number(99.)));
                assert!(matches!(wb.get_value("S",1,2),
                    Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Spill));
                wb.undo().unwrap();
                wb.evaluate_all().unwrap();
                assert_eq!(wb.get_value("S", 4, 2), Some(LiteralValue::Number(4.)));
            }
        }
    }
}

#[cfg(feature = "umya")]
#[test]
fn xlsx_load_then_edit_shrinks_and_unblocks_spill() {
    use formualizer_workbook::{CalamineAdapter, LoadStrategy, SpreadsheetReader};
    for family in [false, true] {
        let wb = make(family, true, true);
        let reader = CalamineAdapter::open_bytes(wb.to_xlsx_bytes().unwrap()).unwrap();
        let mut cfg = WorkbookConfig::interactive();
        cfg.eval.family_execution = family;
        let mut loaded = Workbook::from_reader(reader, LoadStrategy::EagerAll, cfg).unwrap();
        loaded.evaluate_all().unwrap();
        loaded
            .set_value("S", 4, 2, LiteralValue::Number(99.))
            .unwrap();
        loaded.evaluate_all().unwrap();
        assert!(matches!(loaded.get_value("S",1,2),
            Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Spill));
        loaded.set_value("S", 4, 2, LiteralValue::Empty).unwrap();
        loaded.evaluate_all().unwrap();
        assert_eq!(loaded.get_value("S", 5, 2), Some(LiteralValue::Number(5.)));
        loaded
            .set_value("S", 1, 1, LiteralValue::Number(1.))
            .unwrap();
        loaded
            .set_value("S", 4, 2, LiteralValue::Number(99.))
            .unwrap();
        loaded.evaluate_all().unwrap();
        assert_eq!(loaded.get_value("S", 4, 2), Some(LiteralValue::Number(99.)));
    }
}
