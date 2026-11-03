use crate::engine::*;
use crate::function::Function;
use crate::test_workbook::TestWorkbook;
use crate::timezone::{ClockProvider, TimeZoneSpec};
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

fn engine(family: bool) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            enable_parallel: false,
            temporal_egress: TemporalEgress::Serial,
            ..EvalConfig::default()
        },
    )
}
fn f(e: &mut Engine<TestWorkbook>, r: u32, c: u32, s: &str) {
    e.set_cell_formula("Sheet1", r, c, parse(s).unwrap())
        .unwrap();
}
fn v(e: &Engine<TestWorkbook>, r: u32, c: u32) -> Option<LiteralValue> {
    e.get_cell_value("Sheet1", r, c)
}
fn chain(family: bool) -> Engine<TestWorkbook> {
    let mut e = engine(family);
    f(&mut e, 1, 1, "=1");
    for r in 2..=20 {
        f(&mut e, r, 1, &format!("=A{}+1", r - 1));
    }
    e
}
fn incomplete(p: EvaluationProgress) -> IncompleteEvaluation {
    match p {
        EvaluationProgress::Incomplete(i) => i,
        other => panic!("expected incomplete: {other:?}"),
    }
}
fn drain(e: &mut Engine<TestWorkbook>) -> usize {
    for n in 1..=200 {
        if let EvaluationProgress::Complete(_) = e
            .evaluate_all_until(EvaluationStop::after(Duration::ZERO))
            .unwrap()
        {
            assert!(!e.has_pending_resumable_evaluation());
            return n;
        }
    }
    panic!("no progress");
}
#[test]
fn zero_budget_progress() {
    for family in [false, true] {
        let mut e = chain(family);
        let p = incomplete(
            e.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
                .unwrap(),
        );
        assert!(p.computed_vertices > 0);
        assert!(e.has_pending_resumable_evaluation());
        assert!(drain(&mut e) <= 19);
        assert_eq!(v(&e, 20, 1), Some(LiteralValue::Number(20.)));
    }
}
#[derive(Debug)]
struct Sleep;
impl Function for Sleep {
    fn name(&self) -> &'static str {
        "RESUMABLE_SLEEP"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        std::thread::sleep(Duration::from_millis(10));
        Ok(CalcValue::Scalar(LiteralValue::Number(1.)))
    }
}
#[test]
fn wall_clock_slow_unit_finishes() {
    crate::function_registry::register_function(Arc::new(Sleep));
    let mut e = engine(false);
    f(&mut e, 1, 1, "=RESUMABLE_SLEEP()");
    f(&mut e, 2, 1, "=A1+1");
    incomplete(
        e.evaluate_all_until(EvaluationStop::after(Duration::from_millis(1)))
            .unwrap(),
    );
    assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
    drain(&mut e);
    assert_eq!(v(&e, 2, 1), Some(LiteralValue::Number(2.)));
}
#[test]
fn buffered_installation_boundary() {
    for family in [false, true] {
        let mut e = engine(family);
        e.resumable_stop_after_units_for_test = Some(1);
        for r in 1..=64 {
            e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
            f(&mut e, r, 2, &format!("=A{r}*2"));
        }
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        for r in 1..=64 {
            let vertex = e
                .graph
                .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", r, 2))
                .unwrap();
            assert!(
                e.graph.is_dirty(vertex)
                    || v(&e, r, 2) == Some(LiteralValue::Number(r as f64 * 2.)),
                "clean unpublished row {r}"
            );
        }
        drain(&mut e);
        for r in 1..=64 {
            assert_eq!(v(&e, r, 2), Some(LiteralValue::Number(r as f64 * 2.)));
        }
    }
}
#[derive(Debug)]
struct Cancel;
impl Function for Cancel {
    fn name(&self) -> &'static str {
        "RESUMABLE_CANCEL"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if let Some(t) = ctx.cancellation_token() {
            t.cancel();
        }
        Ok(CalcValue::Scalar(LiteralValue::Number(1.)))
    }
}
#[test]
fn cancellation_retains_four_installed_units() {
    crate::function_registry::register_function(Arc::new(Cancel));
    let mut e = chain(false);
    f(&mut e, 5, 1, "=A4+RESUMABLE_CANCEL()");
    let p = incomplete(
        e.evaluate_all_until(EvaluationStop::cancel(CancelToken::new()))
            .unwrap(),
    );
    assert_eq!(p.reason, StopReason::Cancelled);
    assert_eq!(e.graph.get_evaluation_vertices().len(), 16);
    assert_eq!(v(&e, 4, 1), Some(LiteralValue::Number(4.)));
    assert_eq!(v(&e, 5, 1), None);
    drain(&mut e);
    assert_eq!(v(&e, 20, 1), Some(LiteralValue::Number(20.)));
}
fn spill(family: bool) -> Engine<TestWorkbook> {
    spill_into(engine(family))
}
fn spill_into(mut e: Engine<TestWorkbook>) -> Engine<TestWorkbook> {
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.))
        .unwrap();
    f(&mut e, 5, 3, "=B5*2");
    f(&mut e, 6, 3, "=C5+1");
    f(&mut e, 1, 4, "=A1+1");
    f(&mut e, 2, 4, "=D1+1");
    f(&mut e, 3, 4, "=D2+1");
    f(&mut e, 1, 2, "=IF(D3>3,SEQUENCE(5),D3)");
    e
}
#[test]
fn late_first_spill_fr5() {
    for family in [false, true] {
        let mut e = spill(family);
        incomplete(
            e.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
                .unwrap(),
        );
        drain(&mut e);
        assert_eq!(v(&e, 5, 3), Some(LiteralValue::Number(10.)));
        assert_eq!(v(&e, 6, 3), Some(LiteralValue::Number(11.)));
    }
}
#[test]
fn foreign_requests_end_logical_recalculation() {
    for family in [false, true] {
        for mode in 0..4 {
            let mut e = spill(family);
            incomplete(
                e.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
                    .unwrap(),
            );
            match mode {
                0 => {
                    e.evaluate_cell("Sheet1", 1, 4).unwrap();
                }
                1 => {
                    e.evaluate_cells(&[("Sheet1", 1, 4)]).unwrap();
                }
                2 => {
                    e.evaluate_targets(&[EvaluationTarget::Cell {
                        sheet: "Sheet1".into(),
                        row: 1,
                        col: 4,
                    }])
                    .unwrap();
                }
                _ => {
                    e.evaluate_all_cancellable(CancelToken::new()).unwrap();
                }
            }
            assert!(!e.has_pending_resumable_evaluation());
            drain(&mut e);
            assert_eq!(v(&e, 5, 3), Some(LiteralValue::Number(10.)));
            assert_eq!(v(&e, 6, 3), Some(LiteralValue::Number(11.)));
        }
    }
}
#[derive(Debug)]
struct Clock(AtomicUsize);
impl ClockProvider for Clock {
    fn timezone(&self) -> &TimeZoneSpec {
        &TimeZoneSpec::Utc
    }
    fn now(&self) -> chrono::NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + chrono::Duration::days(self.0.fetch_add(1, Ordering::SeqCst) as i64)
    }
}
#[test]
fn seeded_rand_now_one_logical_recalculation() {
    for family in [false, true] {
        let mut e = chain(family);
        let mut reference = chain(family);
        let clock = Arc::new(Clock(AtomicUsize::new(0)));
        e.set_clock(clock.clone());
        reference.set_clock(Arc::new(Clock(AtomicUsize::new(0))));
        for x in [&mut e, &mut reference] {
            f(x, 1, 2, "=RAND()");
            f(x, 2, 2, "=A20+RAND()+NOW()");
        }
        reference.evaluate_all().unwrap();
        let before = clock.0.load(Ordering::SeqCst);
        incomplete(
            e.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
                .unwrap(),
        );
        drain(&mut e);
        assert_eq!(clock.0.load(Ordering::SeqCst) - before, 1);
        assert_eq!(e.recalc_epoch, 1);
        for r in 1..=2 {
            assert_eq!(v(&e, r, 2), v(&reference, r, 2));
        }
    }
}
#[test]
fn diagnostics_union_survives_completion() {
    let mut e = engine(false);
    e.enable_operation_diagnostics(10);
    f(&mut e, 1, 1, "=IFERROR(RESUMABLE_MISSING(),7)");
    for r in 2..=20 {
        f(&mut e, r, 1, &format!("=A{}+1", r - 1));
    }
    incomplete(
        e.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
            .unwrap(),
    );
    assert_eq!(
        e.operation_diagnostics()
            .unwrap()
            .unwrap()
            .logical_root_cell_count,
        1
    );
    drain(&mut e);
    assert_eq!(
        e.operation_diagnostics()
            .unwrap()
            .unwrap()
            .logical_root_cell_count,
        1
    );
}
#[test]
fn iterative_cycle_atomic_and_redirty_once() {
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
            f(&mut e, 2, 1, "=A1+1");
            for r in 3..=20 {
                f(&mut e, r, 1, &format!("=A{}+1", r - 1));
            }
            e
        };
        let mut e = make();
        let mut reference = make();
        reference.evaluate_all().unwrap();
        incomplete(
            e.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
                .unwrap(),
        );
        assert_eq!(v(&e, 1, 1), v(&reference, 1, 1));
        drain(&mut e);
        for r in 1..=20 {
            assert_eq!(v(&e, r, 1), v(&reference, r, 1));
        }
        assert_eq!(
            e.graph.get_evaluation_vertices().len(),
            reference.graph.get_evaluation_vertices().len()
        );
    }
}
#[test]
fn iterative_edits_reuse_the_logical_initial_seed() {
    let make = || {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.)),
        );
        e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(1.))
            .unwrap();
        f(&mut e, 1, 1, "=A1+B1");
        for r in 2..=20 {
            f(&mut e, r, 1, &format!("=A{}+1", r - 1));
        }
        e
    };
    let mut e = make();
    let mut reference = make();
    incomplete(
        e.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
            .unwrap(),
    );
    assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
    for x in [&mut e, &mut reference] {
        x.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(2.))
            .unwrap();
    }
    drain(&mut e);
    reference.evaluate_all().unwrap();
    for r in 1..=20 {
        assert_eq!(v(&e, r, 1), v(&reference, r, 1));
    }
}

#[test]
fn hard_errors_are_errors() {
    let mut e = chain(false);
    e.set_evaluation_resource_budgets(EvaluationBudgets {
        deadline: DeadlineResourceBudget {
            max_elapsed: Some(Duration::ZERO),
        },
        ..EvaluationBudgets::default()
    });
    assert!(e.evaluate_all_until(EvaluationStop::none()).is_err());
    assert!(!e.has_pending_resumable_evaluation());
    assert_eq!(e.graph.get_evaluation_vertices().len(), 20);
    e.set_evaluation_resource_budgets(EvaluationBudgets::default());
    for r in 1..=64 {
        e.set_cell_value("Sheet1", r, 3, LiteralValue::Number(r as f64))
            .unwrap();
        f(&mut e, r, 4, &format!("=C{r}*2"));
    }
    e.fail_evaluation_commit_preflight_once_for_test();
    assert!(e.evaluate_all_until(EvaluationStop::none()).is_err());
    assert!(!e.has_pending_resumable_evaluation());
    e.evaluate_all().unwrap();
    assert_eq!(v(&e, 20, 1), Some(LiteralValue::Number(20.)));
}
#[test]
fn seeded_random_workbooks_and_edits() {
    for family in [false, true] {
        for seed in 1..=12u64 {
            let mut rng = seed;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let make = || {
                let mut e = spill_into(Engine::new(
                    TestWorkbook::new(),
                    EvalConfig {
                        family_execution: family,
                        enable_parallel: false,
                        ..EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.))
                    },
                ));
                f(&mut e, 1, 2, "=IF(D3>3,SEQUENCE(A1),D3)");
                f(&mut e, 1, 10, "=J1+1");
                f(&mut e, 2, 10, "=J1+1");
                for r in 1..=12 {
                    e.set_cell_value(
                        "Sheet1",
                        r,
                        6,
                        LiteralValue::Number((r as u64 + seed) as f64),
                    )
                    .unwrap();
                    f(&mut e, r, 7, &format!("=F{r}*2"));
                    f(&mut e, r, 8, &format!("=SUMIF(F1:F12,\">0\",G1:G12)+G{r}"));
                }
                f(&mut e, 1, 9, "=INDIRECT(\"C6\")+OFFSET(B1,4,0)+RAND()");
                e
            };
            let mut e = make();
            let mut reference = make();
            let mut complete = false;
            for call in 0..200 {
                if call < 4 && next() % 2 == 0 {
                    let x = (next() % 6 + 1) as f64;
                    for xengine in [&mut e, &mut reference] {
                        xengine
                            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(x))
                            .unwrap();
                        xengine
                            .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(x))
                            .unwrap();
                        // Alternate blocked and unblocked extents while also growing
                        // and shrinking the anchor through A1.
                        xengine
                            .set_cell_value(
                                "Sheet1",
                                4,
                                2,
                                if call % 2 == 0 {
                                    LiteralValue::Number(99.)
                                } else {
                                    LiteralValue::Empty
                                },
                            )
                            .unwrap();
                    }
                }
                e.resumable_stop_after_units_for_test = Some((next() % 4) as usize);
                if let EvaluationProgress::Complete(_) =
                    e.evaluate_all_until(EvaluationStop::none()).unwrap()
                {
                    complete = true;
                    break;
                }
            }
            assert!(complete, "seed {seed} family {family}");
            reference.evaluate_all().unwrap();
            for r in 1..=12 {
                for c in 1..=10 {
                    assert_eq!(
                        v(&e, r, c),
                        v(&reference, r, c),
                        "seed {seed} family {family} ({r},{c})"
                    );
                }
            }
        }
    }
}
