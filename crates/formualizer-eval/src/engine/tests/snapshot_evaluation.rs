use crate::engine::inspect::Staleness;
use crate::engine::{CancelToken, CycleConfig, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{CellAddress, LiteralValue};
use formualizer_parse::parser::parse;
#[derive(Debug)]
struct TickingClock(std::sync::atomic::AtomicU64);
impl crate::timezone::ClockProvider for TickingClock {
    fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
        &crate::timezone::TimeZoneSpec::Utc
    }
    fn now(&self) -> chrono::NaiveDateTime {
        let tick = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        chrono::DateTime::from_timestamp(1_600_000_000 + tick as i64, 0)
            .unwrap()
            .naive_utc()
    }
}
fn engine() -> Engine<TestWorkbook> {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    for (col, f) in [
        (2, "=RAND()"),
        (3, "=B1+0"),
        (4, "=NOW()"),
        (5, "=SUM(OFFSET(A1,0,0))"),
        (6, "=E1+0"),
    ] {
        e.set_cell_formula("Sheet1", 1, col, parse(f).unwrap())
            .unwrap();
    }
    e
}
#[test]
fn snapshot_is_current_and_every_followup_entry_restores_volatile_work() {
    for targeted in [0, 1, 2] {
        let mut e = engine();
        e.set_clock(std::sync::Arc::new(TickingClock(
            std::sync::atomic::AtomicU64::new(0),
        )));
        e.evaluate_all_for_snapshot(None).unwrap();
        let first = e.get_cell_value("Sheet1", 1, 2).unwrap();
        let first_now = e.get_cell_value("Sheet1", 1, 4).unwrap();
        e.set_workbook_seed(123);
        for col in 2..=6 {
            assert_eq!(
                e.inspect_cell_result(&CellAddress::new("Sheet1", 1, col).unwrap())
                    .unwrap()
                    .staleness,
                Staleness::Current
            );
        }
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 2),
            e.get_cell_value("Sheet1", 1, 3)
        );
        e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(8.0))
            .unwrap();
        match targeted {
            0 => {
                e.evaluate_all().unwrap();
            }
            1 => {
                e.evaluate_until(&[("Sheet1", 1, 3), ("Sheet1", 1, 4), ("Sheet1", 1, 6)])
                    .unwrap();
            }
            _ => {
                e.evaluate_cell("Sheet1", 1, 3).unwrap();
                e.evaluate_cell("Sheet1", 1, 4).unwrap();
                e.evaluate_cell("Sheet1", 1, 6).unwrap();
            }
        }
        assert_ne!(e.get_cell_value("Sheet1", 1, 2), Some(first));
        assert_ne!(e.get_cell_value("Sheet1", 1, 4), Some(first_now));
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 2),
            e.get_cell_value("Sheet1", 1, 3)
        );
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 6),
            Some(LiteralValue::Number(8.0))
        );
        assert_eq!(
            e.inspect_cell_result(&CellAddress::new("Sheet1", 1, 2).unwrap())
                .unwrap()
                .staleness,
            Staleness::Dirty
        );
    }
}
#[test]
fn cancellation_does_not_leave_snapshot_mode_active() {
    let mut e = engine();
    let token = CancelToken::new();
    token.cancel();
    assert!(e.evaluate_all_for_snapshot(Some(token)).is_err());
    e.evaluate_all().unwrap();
    assert_eq!(
        e.inspect_cell_result(&CellAddress::new("Sheet1", 1, 2).unwrap())
            .unwrap()
            .staleness,
        Staleness::Dirty
    );
    e.evaluate_all_for_snapshot(None).unwrap();
    assert_eq!(
        e.inspect_cell_result(&CellAddress::new("Sheet1", 1, 2).unwrap())
            .unwrap()
            .staleness,
        Staleness::Current
    );
}
#[test]
fn iterative_scc_redirty_is_not_suppressed_by_snapshot_evaluation() {
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig::iterate(3, 0.001)),
    );
    e.set_cell_formula("Sheet1", 1, 1, parse("=A1+1").unwrap())
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        e.inspect_cell_result(&CellAddress::new("Sheet1", 1, 1).unwrap())
            .unwrap()
            .staleness,
        Staleness::Dirty
    );
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn resource_error_restores_the_normal_volatile_mode() {
    let mut e = engine();
    e.evaluate_all_for_snapshot(None).unwrap();
    e.set_evaluation_resource_budgets(crate::engine::EvaluationBudgets {
        work: crate::engine::WorkResourceBudget {
            max_work_units: Some(0),
        },
        ..Default::default()
    });
    assert!(e.evaluate_all_for_snapshot(None).is_err());
    e.set_evaluation_resource_budgets(Default::default());
    e.evaluate_all().unwrap();
    assert!(matches!(
        e.inspect_cell_result(&CellAddress::new("Sheet1", 1, 2).unwrap())
            .unwrap()
            .staleness,
        Staleness::Dirty
    ));
}

struct PanicFn;
impl crate::function::Function for PanicFn {
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "SNAPSHOT_TEST_PANIC"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        panic!("snapshot test panic")
    }
}
#[test]
fn panic_during_snapshot_evaluation_does_not_leave_snapshot_mode_active() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(PanicFn));
    let mut e = Engine::new(wb, EvalConfig::default());
    e.set_cell_formula("Sheet1", 1, 1, parse("=RAND()").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=SNAPSHOT_TEST_PANIC()").unwrap())
        .unwrap();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = e.evaluate_all_for_snapshot(None);
    }));
    assert!(caught.is_err(), "the test function must panic");
    // A caller that keeps the engine and evaluates normally must get the
    // ordinary next-cycle volatile redirty again.
    e.set_cell_formula("Sheet1", 1, 2, parse("=1").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.inspect_cell_result(&CellAddress::new("Sheet1", 1, 1).unwrap())
            .unwrap()
            .staleness,
        Staleness::Dirty
    );
}
