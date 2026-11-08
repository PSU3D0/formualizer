use super::*;

fn job_step(e: &mut Engine<TestWorkbook>) -> StepReport {
    loop {
        let report = e
            .step_recalc(EvaluationStop::after(Duration::ZERO))
            .unwrap();
        if !matches!(
            report.phase,
            StepPhase::Scheduling | StepPhase::BuildingGraph
        ) {
            return report;
        }
    }
}
fn flags(e: &Engine<TestWorkbook>) -> Vec<bool> {
    (1..=20)
        .flat_map(|row| (1..=2).map(move |col| (row, col)))
        .map(|(r, c)| {
            let id = e
                .graph
                .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", r, c))
                .unwrap();
            e.graph.is_dirty(id)
        })
        .collect()
}
fn foreign(e: &mut Engine<TestWorkbook>, mode: usize, plan: &RecalcPlan) -> Result<(), ExcelError> {
    let id = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 1))
        .unwrap();
    let cells = [("Sheet1", 20, 1)];
    let targets = [EvaluationTarget::Cell {
        sheet: "Sheet1".into(),
        row: 20,
        col: 1,
    }];
    match mode {
        0 => e.evaluate_all().map(|_| ()),
        1 => e.evaluate_all_until(EvaluationStop::none()).map(|_| ()),
        2 => e.evaluate_all_cancellable(CancelToken::new()).map(|_| ()),
        3 => e.evaluate_cell("Sheet1", 20, 1).map(|_| ()),
        4 => e.evaluate_cells(&cells).map(|_| ()),
        5 => e
            .evaluate_cells_cancellable(&cells, CancelToken::new())
            .map(|_| ()),
        6 => e.evaluate_until(&cells).map(|_| ()),
        7 => e
            .evaluate_until_cancellable(&["Sheet1!A20"], CancelToken::new())
            .map(|_| ()),
        8 => e.evaluate_targets(&targets).map(|_| ()),
        9 => e
            .evaluate_targets_with_options(&targets, Default::default())
            .map(|_| ()),
        10 => e.evaluate_targets_with_delta(&targets).map(|_| ()),
        11 => e.evaluate_cells_with_delta(&cells).map(|_| ()),
        12 => e
            .evaluate_cells_with_delta_policy(&cells, EvalDeltaCompatibilityPolicy::Unlimited)
            .map(|_| ()),
        13 => e.evaluate_cells_with_target_delta(&cells).map(|_| ()),
        14 => e.evaluate_all_with_delta().map(|_| ()),
        15 => e
            .evaluate_all_with_delta_policy(EvalDeltaCompatibilityPolicy::Unlimited)
            .map(|_| ()),
        16 => e.evaluate_all_with_target_delta().map(|_| ()),
        17 => e.evaluate_vertex(id).map(|_| ()),
        18 => e.evaluate_recalc_plan(plan).map(|_| ()),
        19 => e
            .evaluate_recalc_plan_with_controls(plan, Some(CancelToken::new()), None)
            .map(|_| ()),
        20 => e.evaluate_all_logged(&mut ChangeLog::new()).map(|_| ()),
        21 => e.evaluate_all_for_snapshot(None).map(|_| ()),
        22 => e
            .evaluate_all_for_snapshot(Some(CancelToken::new()))
            .map(|_| ()),
        _ => unreachable!(),
    }
}
fn refusal_matrix(modes: &[usize]) {
    for family in [false, true] {
        for &mode in modes {
            let mut e = chain(family);
            f(&mut e, 1, 1, "=RAND()+IFERROR(PHASE2_REFUSAL_MISSING(),0)");
            f(&mut e, 1, 2, "=A20+1");
            for row in 2..=20 {
                f(&mut e, row, 2, &format!("=B{}+1", row - 1));
            }
            let clock = Arc::new(Clock(AtomicUsize::new(0)));
            e.set_clock(clock.clone());
            e.enable_operation_diagnostics(10);
            let plan = e.build_recalc_plan().unwrap();
            let clock_before = clock.0.load(Ordering::SeqCst);
            e.begin_recalc(Default::default()).unwrap();
            e.resumable_stop_after_units_for_test = Some(1);
            job_step(&mut e);
            job_step(&mut e);
            assert!(e.has_active_recalc_job());
            assert!(v(&e, 2, 1).is_some(), "dependent must have installed");
            let before_flags = flags(&e);
            let before_values: Vec<_> = (1..=20)
                .flat_map(|r| (1..=2).map(move |c| (r, c)))
                .map(|(r, c)| v(&e, r, c))
                .collect();
            let before_roots = e.operation_diagnostics().unwrap().unwrap().roots;
            let before_request = e
                .last_evaluation_resource_request_stats()
                .unwrap()
                .request_id;
            assert!(!before_flags[0] && !before_flags[2]);
            let error = foreign(&mut e, mode, &plan).unwrap_err();
            assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Value);
            assert!(
                matches!(
                    error.extra(),
                    formualizer_common::ExcelErrorExtra::RecalcJobActive
                ),
                "untyped refusal: mode={mode} family={family}: {error:?}"
            );
            assert!(e.has_active_recalc_job());
            assert_eq!(
                flags(&e),
                before_flags,
                "mode={mode} family={family}: refusal changed dirtiness"
            );
            assert_eq!(
                before_values,
                (1..=20)
                    .flat_map(|r| (1..=2).map(move |c| (r, c)))
                    .map(|(r, c)| v(&e, r, c))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                before_roots,
                e.operation_diagnostics().unwrap().unwrap().roots
            );
            assert_eq!(
                before_request,
                e.last_evaluation_resource_request_stats()
                    .unwrap()
                    .request_id
            );
            assert_eq!(clock.0.load(Ordering::SeqCst) - clock_before, 1);
            assert_eq!(e.recalc_epoch, 0);
            let result = loop {
                let r = job_step(&mut e);
                if let Some(result) = r.result {
                    break result;
                }
            };
            assert_eq!(
                result.computed_vertices, 40,
                "mode={mode} family={family}: rejected work must not be recomputed"
            );
            assert_eq!(e.recalc_epoch, 1);
            assert_eq!(
                e.operation_diagnostics().unwrap().unwrap().roots,
                before_roots
            );
            e.begin_recalc(Default::default()).unwrap();
            job_step(&mut e);
            let error = foreign(&mut e, mode, &plan).unwrap_err();
            assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Value);
            assert!(
                matches!(
                    error.extra(),
                    formualizer_common::ExcelErrorExtra::RecalcJobActive
                ),
                "untyped refusal: mode={mode} family={family}: {error:?}"
            );
            e.cancel_recalc().unwrap();
            e.evaluate_all().unwrap();
            assert_eq!(clock.0.load(Ordering::SeqCst) - clock_before, 3);
            let LiteralValue::Number(root) = v(&e, 1, 1).unwrap() else {
                panic!("RAND root");
            };
            assert_eq!(v(&e, 20, 2), Some(LiteralValue::Number(root + 39.)));
            println!(
                "REFUSAL mode={mode} family={family} unchanged, computed=40, cancel/standalone passed"
            );
        }
    }
}
#[test]
fn snapshot_none_refusal_preserves_installed_rand_and_dependents() {
    if isolated_test("fix_round1::snapshot_none_refusal_preserves_installed_rand_and_dependents") {
        return;
    }
    refusal_matrix(&[21]);
}
#[test]
fn snapshot_cancel_refusal_preserves_installed_rand_and_dependents() {
    if isolated_test("fix_round1::snapshot_cancel_refusal_preserves_installed_rand_and_dependents")
    {
        return;
    }
    refusal_matrix(&[22]);
}
#[test]
fn all_23_foreign_routes_preserve_active_job() {
    if isolated_test("fix_round1::all_23_foreign_routes_preserve_active_job") {
        return;
    }
    refusal_matrix(&(0..23).collect::<Vec<_>>());
}
#[test]
fn unchanged_spills_do_not_visit_installed_history() {
    for n in [64, 256] {
        let mut e = engine(false);
        e.add_sheet("Aux").unwrap();
        e.set_cell_formula("Aux", 1, 1, parse("=SEQUENCE(2)").unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        for row in 1..=n {
            f(&mut e, row, 1, if row == 1 { "=1" } else { "=A1+1" });
        }
        e.begin_recalc(Default::default()).unwrap();
        e.resumable_stop_after_units_for_test = Some(1);
        let mut computed = 0;
        loop {
            let r = job_step(&mut e);
            computed += r.step_computed_vertices;
            if r.phase == StepPhase::Finished {
                break;
            }
        }
        assert_eq!(computed, n as usize);
        assert_eq!(
            e.resumable_fr5_examined_for_test(),
            0,
            "an unchanged spill must not inspect cumulative history"
        );
    }
}
#[test]
fn fr5_reports_already_reported_reader_again_and_progress_monotone() {
    for family in [false, true] {
        for cap in [0, 1, 1024] {
            let mut e = spill(family);
            let sheet = e.graph.sheet_id("Sheet1").unwrap();
            let options = RecalcJobOptions {
                max_delta_records: cap,
                ..Default::default()
            };
            e.begin_recalc(options).unwrap();
            e.resumable_stop_after_units_for_test = Some(1);
            let mut last_pass = 0;
            let mut last_done = 0;
            let mut reader_changes = 0;
            for _ in 0..100 {
                let before: Vec<_> = (1..=7)
                    .flat_map(|r| (1..=4).map(move |c| (r, c)))
                    .map(|(r, c)| ((r, c), v(&e, r, c)))
                    .collect();
                let r = job_step(&mut e);
                if r.progress.pass == last_pass {
                    assert!(r.progress.completed_in_pass >= last_done);
                }
                last_pass = r.progress.pass;
                last_done = r.progress.completed_in_pass;
                assert!(r.progress.completed_in_pass <= r.progress.scheduled_in_pass);
                assert_eq!(r.result.is_some(), r.phase == StepPhase::Finished);
                for ((row, col), old) in before {
                    let new = v(&e, row, col);
                    if old != new {
                        if row == 5 && col == 3 {
                            reader_changes += 1;
                        }
                        assert!(
                            r.changed.overflow_sheets.contains(&sheet)
                                || r.changed.delta.records.iter().any(|x| {
                                    let (a, b, c, d) = x.bounds();
                                    a < row && row - 1 <= c && b < col && col - 1 <= d
                                }),
                            "missing delta family={family} cap={cap} {row},{col}: {old:?}->{new:?}"
                        );
                    }
                }
                if r.phase == StepPhase::Finished {
                    break;
                }
            }
            println!("FR5_DELTA family={family} cap={cap} reader_changes={reader_changes}");
            assert!(
                reader_changes >= 2,
                "fixture must re-report a changed installed reader"
            );
            assert_eq!(v(&e, 5, 3), Some(LiteralValue::Number(10.)));
            assert_eq!(v(&e, 6, 3), Some(LiteralValue::Number(11.)));
        }
    }
}

#[test]
fn fr5_does_not_let_a_later_slice_clear_a_stale_descendant() {
    for family in [false, true] {
        let mut e = engine(family);
        e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.))
            .unwrap();
        f(&mut e, 5, 3, "=B5*2");
        f(&mut e, 6, 3, "=C5+1");
        f(&mut e, 1, 4, "=A1+1");
        f(&mut e, 1, 2, "=IF(D1>3,SEQUENCE(5),D1)");
        e.begin_recalc(Default::default()).unwrap();
        e.resumable_stop_after_units_for_test = Some(1);
        job_step(&mut e);
        job_step(&mut e);
        assert_eq!(v(&e, 5, 3), Some(LiteralValue::Number(0.)));
        assert_eq!(v(&e, 1, 4), Some(LiteralValue::Number(6.)));
        assert!(v(&e, 6, 3).is_none());
        e.prioritize_recalc(&[EvaluationTarget::Cell {
            sheet: "Sheet1".into(),
            row: 1,
            col: 2,
        }])
        .unwrap();
        job_step(&mut e);
        assert_eq!(v(&e, 1, 2), Some(LiteralValue::Number(1.)));
        let mut finished = false;
        for _ in 0..30 {
            let r = job_step(&mut e);
            if r.phase == StepPhase::Finished {
                finished = true;
                break;
            }
        }
        assert!(finished);
        assert_eq!(v(&e, 5, 3), Some(LiteralValue::Number(10.)));
        assert_eq!(
            v(&e, 6, 3),
            Some(LiteralValue::Number(11.)),
            "FR5 must survive a descendant committing in a subsequent slice"
        );
    }
}

#[test]
fn cross_step_spill_resize_rechecks_installed_readers_and_deltas() {
    for family in [false, true] {
        for cross_sheet in [false, true] {
            for grow in [false, true] {
                let mut e = engine(family);
                let spill_sheet = if cross_sheet {
                    e.add_sheet("Aux").unwrap();
                    "Aux"
                } else {
                    "Sheet1"
                };
                let (old_len, new_len, old_sum, new_sum) = if grow {
                    (3., 6., 0., 15.)
                } else {
                    (6., 2., 15., 0.)
                };
                e.set_cell_value(spill_sheet, 1, 1, LiteralValue::Number(old_len))
                    .unwrap();
                e.set_cell_formula(spill_sheet, 1, 2, parse("=SEQUENCE(A1)").unwrap())
                    .unwrap();
                f(
                    &mut e,
                    1,
                    3,
                    if cross_sheet {
                        "=SUM(Aux!B4:B8)"
                    } else {
                        "=SUM(B4:B8)"
                    },
                );
                f(&mut e, 2, 3, "=C1+1");
                for col in 10..210 {
                    f(&mut e, 1, col, &format!("={col}"));
                }
                let reader_sheet = e.graph.sheet_id("Sheet1").unwrap();
                let target = |sheet: &str, row, col| EvaluationTarget::Cell {
                    sheet: sheet.into(),
                    row,
                    col,
                };
                e.begin_recalc(Default::default()).unwrap();
                e.resumable_stop_after_units_for_test = Some(1);
                e.prioritize_recalc(&[target(spill_sheet, 1, 2)]).unwrap();
                job_step(&mut e); // k: install the original spill.
                e.prioritize_recalc(&[target("Sheet1", 1, 3)]).unwrap();
                job_step(&mut e); // k+1: install its reader.
                assert_eq!(v(&e, 1, 3), Some(LiteralValue::Number(old_sum)));
                e.prioritize_recalc(&[target("Sheet1", 2, 3)]).unwrap();
                job_step(&mut e);
                for _ in 0..2 {
                    assert_ne!(job_step(&mut e).phase, StepPhase::Finished);
                }
                e.set_cell_value(spill_sheet, 1, 1, LiteralValue::Number(new_len))
                    .unwrap();
                e.prioritize_recalc(&[target(spill_sheet, 1, 2)]).unwrap();
                job_step(&mut e); // k+5: replace that same installed spill.
                let reader = e
                    .graph
                    .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 3))
                    .unwrap();
                assert!(
                    e.graph.is_dirty(reader),
                    "changed spill must re-dirty its previously installed reader"
                );
                let mut finished = false;
                let mut changes = 0;
                for _ in 0..500 {
                    let before = v(&e, 1, 3);
                    let report = job_step(&mut e);
                    if before != v(&e, 1, 3) {
                        changes += 1;
                        assert!(
                            report.changed.overflow_sheets.contains(&reader_sheet)
                                || report.changed.delta.records.iter().any(|r| {
                                    let (r0, c0, r1, c1) = r.bounds();
                                    r.sheet_id() == reader_sheet
                                        && r0 == 0
                                        && r1 >= r0
                                        && c0 <= 2
                                        && c1 >= 2
                                }),
                            "recomputed reader must be reported again"
                        );
                    }
                    if report.phase == StepPhase::Finished {
                        finished = true;
                        break;
                    }
                }
                assert!(finished);
                assert_eq!(changes, 1);
                assert_eq!(v(&e, 1, 3), Some(LiteralValue::Number(new_sum)));
                assert_eq!(v(&e, 2, 3), Some(LiteralValue::Number(new_sum + 1.)));
                assert!(
                    e.resumable_fr5_examined_for_test() <= 8,
                    "settlement must inspect only the spill-affected cone"
                );
                println!(
                    "CROSS_STEP family={family} cross_sheet={cross_sheet} grow={grow} affected={}",
                    e.resumable_fr5_examined_for_test()
                );
            }
        }
    }
}
