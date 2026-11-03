use super::*;
#[test]
fn original_cycle_dissolved_before_first_execution() {
    let mut failures = Vec::new();
    for family in [false, true] {
        let make = || {
            let mut e = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    family_execution: family,
                    enable_parallel: false,
                    ..EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.))
                },
            );
            f(&mut e, 1, 1, "=A1+1");
            for r in 2..=30 {
                f(&mut e, r, 1, &format!("=A{}+1", r - 1));
            }
            f(&mut e, 1, 3, "=A30+1");
            e
        };
        let mut e = make();
        let mut b = make();
        let cancel = CancelToken::new();
        cancel.cancel();
        incomplete(
            e.evaluate_all_until(EvaluationStop::cancel(cancel))
                .unwrap(),
        );
        assert_eq!(v(&e, 1, 1), None);
        f(&mut e, 1, 1, "=7");
        e.resumable_stop_after_units_for_test = Some(1);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(7.)));
        f(&mut e, 1, 1, "=A1+1");
        drain(&mut e);
        b.evaluate_all().unwrap();
        println!(
            "ORIGINAL_BEFORE_EXEC family={family} actual={:?} expected={:?}",
            v(&e, 1, 1),
            v(&b, 1, 1)
        );
        if v(&e, 1, 1) != v(&b, 1, 1) {
            failures.push(family);
        }
    }
    assert!(failures.is_empty(), "original cycle failures {failures:?}");
}
#[test]
fn precancel_hard_deadline_and_no_execution() {
    for family in [false, true] {
        let mut e = chain(family);
        let cancel = CancelToken::new();
        cancel.cancel();
        e.set_evaluation_resource_budgets(EvaluationBudgets {
            deadline: DeadlineResourceBudget {
                max_elapsed: Some(Duration::ZERO),
            },
            ..EvaluationBudgets::default()
        });
        assert!(
            e.evaluate_all_until(EvaluationStop::cancel(cancel))
                .is_err()
        );
        assert!(!e.has_pending_resumable_evaluation());
        for r in 1..=20 {
            assert_eq!(v(&e, r, 1), None);
        }
    }
}
#[test]
fn precancel_plan_timing() {
    let mut e = engine(false);
    for r in 1..=2000 {
        f(&mut e, r, 1, if r == 1 { "=1" } else { "=A1+1" });
    }
    let cancel = CancelToken::new();
    cancel.cancel();
    let t = std::time::Instant::now();
    incomplete(
        e.evaluate_all_until(EvaluationStop::cancel(cancel))
            .unwrap(),
    );
    println!("PRECANCEL initial_plan_2000_us={}", t.elapsed().as_micros());
    for r in 1..=2000 {
        assert_eq!(v(&e, r, 1), None);
    }
}

#[derive(Debug)]
struct UnknownProviderValue(Arc<AtomicUsize>);
impl Function for UnknownProviderValue {
    fn name(&self) -> &'static str {
        "REVIEW2_UNKNOWN_PROVIDER"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Ok(CalcValue::Scalar(LiteralValue::Number(
            self.0.load(Ordering::SeqCst) as f64,
        )))
    }
}
#[test]
fn unknown_provider_semantic_change() {
    if isolated_test("review_risks::unknown_provider_semantic_change") {
        return;
    }
    for family in [false, true] {
        let value = Arc::new(AtomicUsize::new(1));
        let make = || {
            let provider = TestWorkbook::new()
                .without_planning_revision()
                .with_function(Arc::new(UnknownProviderValue(value.clone())));
            let mut e = Engine::new(
                provider,
                EvalConfig {
                    family_execution: family,
                    enable_parallel: false,
                    ..EvalConfig::default()
                },
            );
            f(&mut e, 1, 1, "=REVIEW2_UNKNOWN_PROVIDER()");
            for r in 2..=20 {
                f(&mut e, r, 1, &format!("=A{}+1", r - 1));
            }
            f(&mut e, 1, 3, "=A20+1");
            e
        };
        let mut e = make();
        let mut b = make();
        let mut ordinary = make();
        ordinary.evaluate_all().unwrap();
        e.resumable_stop_after_units_for_test = Some(1);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
        value.store(9, Ordering::SeqCst);
        drain(&mut e);
        b.evaluate_all().unwrap();
        println!(
            "UNKNOWN_PROVIDER family={family} actual={:?}/{:?} expected={:?}/{:?}",
            v(&e, 1, 1),
            v(&e, 20, 1),
            v(&b, 1, 1),
            v(&b, 20, 1)
        );
        // R3 contract: None means stable within a logical recalculation; the
        // ordinary API likewise has no invalidation signal across separate calls.
        ordinary.evaluate_all().unwrap();
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
        assert_eq!(v(&ordinary, 1, 1), Some(LiteralValue::Number(1.)));
        assert_eq!(v(&ordinary, 20, 1), Some(LiteralValue::Number(20.)));
        assert_eq!(v(&b, 1, 1), Some(LiteralValue::Number(9.)));
        println!(
            "ORDINARY_UNKNOWN family={family} stale={:?}/{:?}",
            v(&ordinary, 1, 1),
            v(&ordinary, 20, 1)
        );
        for engine in [&mut e, &mut ordinary] {
            let root = engine
                .graph
                .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 1))
                .unwrap();
            engine.graph.mark_dirty_many(&[root]);
        }
        e.evaluate_all().unwrap();
        ordinary.evaluate_all().unwrap();
        assert_eq!(v(&e, 20, 1), Some(LiteralValue::Number(28.)));
        assert_eq!(v(&ordinary, 20, 1), Some(LiteralValue::Number(28.)));
    }
}

fn risk_cycle_engine(family: bool, kind: usize) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            enable_parallel: false,
            ..EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.))
        },
    );
    f(&mut e, 1, 1, if kind == 1 { "=A2+1" } else { "=A1+1" });
    f(&mut e, 2, 1, "=A1+1");
    if kind == 2 {
        f(&mut e, 1, 2, "=B1+10");
    }
    for r in 3..=30 {
        f(&mut e, r, 1, &format!("=A{}+1", r - 1));
    }
    f(&mut e, 1, 3, "=A30+1");
    for r in 2..=30 {
        f(&mut e, r, 3, &format!("=C{}+1", r - 1));
    }
    e
}
fn risk_copy(from: &Engine<TestWorkbook>, to: &mut Engine<TestWorkbook>) {
    for r in 1..=30 {
        for c in 1..=3 {
            if let Some(id) = to
                .graph
                .get_vertex_id_for_address(&to.graph.make_cell_ref("Sheet1", r, c))
                .filter(|&id| to.graph.is_live_formula_vertex(id))
                && let Some(value) = v(from, r, c)
            {
                to.apply_effect(
                    &crate::engine::effects::Effect::WriteCell {
                        vertex_id: id,
                        value,
                    },
                    None,
                    None,
                )
                .unwrap();
            }
        }
    }
}
#[test]
fn cycle_membership_growth_shrink_merge() {
    for family in [false, true] {
        for kind in 0..3 {
            let mut e = risk_cycle_engine(family, kind);
            let mut b = risk_cycle_engine(family, kind);
            e.resumable_stop_after_units_for_test = Some(2);
            incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
            risk_copy(&e, &mut b);
            for x in [&mut e, &mut b] {
                match kind {
                    0 => f(x, 1, 1, "=A1+A2"),
                    1 => f(x, 1, 1, "=A1+1"),
                    2 => {
                        f(x, 1, 1, "=A1+B1+1");
                        f(x, 1, 2, "=B1+A1+10");
                    }
                    _ => unreachable!(),
                }
            }
            drain(&mut e);
            b.evaluate_all().unwrap();
            for r in 1..=30 {
                for c in 1..=3 {
                    assert_eq!(
                        v(&e, r, c),
                        v(&b, r, c),
                        "family={family} kind={kind} ({r},{c})"
                    );
                }
            }
            println!("MEMBERSHIP family={family} kind={kind} pass");
        }
    }
}

#[test]
fn precancel_planning_hard_scratch_limit() {
    let mut e = chain(false);
    let cancel = CancelToken::new();
    cancel.cancel();
    e.set_evaluation_resource_budgets(EvaluationBudgets {
        scratch: ScratchResourceBudget {
            schedule_discovery_bytes: Some(0),
            ..ScratchResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    });
    let result = e.evaluate_all_until(EvaluationStop::cancel(cancel));
    println!("PRECANCEL hard scratch result={result:?}");
    assert!(result.is_err());
    assert!(!e.has_pending_resumable_evaluation());
    for r in 1..=20 {
        assert_eq!(v(&e, r, 1), None);
    }
}

#[test]
fn retained_original_cycle_seed_is_captured_before_dissolution() {
    if isolated_test("review_risks::retained_original_cycle_seed_is_captured_before_dissolution") {
        return;
    }
    for family in [false, true] {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: family,
                enable_parallel: false,
                ..EvalConfig::default().with_cycle(CycleConfig::iterate(100, 0.001))
            },
        );
        f(&mut e, 1, 1, "=IF(A1<2,A1+1,2)");
        for r in 2..=30 {
            f(&mut e, r, 1, &format!("=A{}+1", r - 1));
        }
        f(&mut e, 1, 3, "=A30+1");
        e.evaluate_all().unwrap();
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(2.)));
        assert_eq!(e.baseline_stats().retained_scc_members, 1);
        // Keep unrelated work pending while the retained SCC starts clean.
        f(&mut e, 1, 4, "=1");
        for r in 2..=30 {
            f(&mut e, r, 4, &format!("=D{}+1", r - 1));
        }
        let token = CancelToken::new();
        token.cancel();
        incomplete(e.evaluate_all_until(EvaluationStop::cancel(token)).unwrap());
        f(&mut e, 1, 1, "=7");
        e.resumable_stop_after_units_for_test = Some(1);
        for _ in 0..10 {
            incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
            if v(&e, 1, 1) == Some(LiteralValue::Number(7.)) {
                break;
            }
        }
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(7.)));
        f(&mut e, 1, 1, "=A1+1");
        drain(&mut e);
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(102.)));
    }
}
