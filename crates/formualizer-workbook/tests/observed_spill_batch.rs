use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_eval::engine::EvaluationTarget;
use formualizer_workbook::{Workbook, WorkbookConfig, traits::NamedRangeScope};

#[test]
fn minimal_default_workbook_future_spill() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 4, "=SEQUENCE(4)").unwrap();
    wb.set_formula("S", 1, 6, "=INDIRECT(\"D4\")+1").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 1, 6), Some(LiteralValue::Number(5.)));
}

#[test]
fn first_request_observed_future_spill_batch() {
    let mut failures = Vec::new();
    for family in [false, true] {
        for names in [false, true] {
            for offset in [false, true] {
                for tail in [false, true] {
                    for api in 0..4 {
                        let mut config = WorkbookConfig::interactive();
                        config.eval.family_execution = family;
                        config.eval.enable_parallel = true;
                        config.eval.max_threads = Some(4);
                        let mut wb = Workbook::new_with_config(config);
                        wb.add_sheet("S").unwrap();
                        wb.set_formula("S", 1, 4, "=SEQUENCE(4)").unwrap();
                        wb.set_formula(
                            "S",
                            1,
                            6,
                            if offset {
                                "=OFFSET(D1,3,0)+1"
                            } else {
                                "=INDIRECT(\"D4\")+1"
                            },
                        )
                        .unwrap();
                        if names {
                            wb.define_named_range(
                                "SpillCell",
                                &RangeAddress::new("S", 4, 4, 4, 4).unwrap(),
                                NamedRangeScope::Workbook,
                            )
                            .unwrap();
                        }
                        if tail {
                            wb.set_formula("S", 2, 6, "=F1+1").unwrap();
                        }
                        let row = if tail { 2 } else { 1 };
                        match api {
                            0 => {
                                wb.evaluate_all().unwrap();
                            }
                            1 => {
                                wb.evaluate_cell("S", row, 6).unwrap();
                            }
                            2 => {
                                wb.evaluate_targets(&[EvaluationTarget::Cell {
                                    sheet: "S".into(),
                                    row,
                                    col: 6,
                                }])
                                .unwrap();
                            }
                            _ => {
                                wb.engine_mut().evaluate_all_for_snapshot(None).unwrap();
                            }
                        }
                        if wb.get_value("S", 1, 6) != Some(LiteralValue::Number(5.))
                            || (tail && wb.get_value("S", 2, 6) != Some(LiteralValue::Number(6.)))
                        {
                            failures.push((
                                family,
                                names,
                                offset,
                                tail,
                                api,
                                wb.get_value("S", 1, 6),
                                wb.get_value("S", 2, 6),
                            ));
                        }
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}
