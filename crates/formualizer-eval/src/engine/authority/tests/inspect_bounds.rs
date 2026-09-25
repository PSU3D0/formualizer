//! Bounded inspection (red team #2): `dependents` and dependent `trace`
//! with a small `max_work` enumerate no more readers than the budget pays
//! for. Allocation counts and peak bytes stay constant as the fan-out grows
//! (red team Appendix A: N formulas `=$A$1+1` read A1).

use super::alloc::measure;
use crate::engine::inspect::{DependentsOptions, TraceDirection, TraceOptions};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::CellAddress;
use formualizer_parse::parser::parse;

fn fan_out(n: u32) -> Engine<TestWorkbook> {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=n {
        e.set_cell_formula("Sheet1", r, 2, parse("=$A$1+1").unwrap())
            .unwrap();
    }
    // Sync the host so the query itself is measured.
    e.graph.authority().unwrap();
    e
}

#[test]
fn small_dependents_budgets_do_not_expand_the_fan_out() {
    let a1 = CellAddress::new("Sheet1", 1, 1).unwrap();
    for budget in [1u64, 2, 5] {
        let mut seen = Vec::new();
        for n in [100u32, 1_000, 10_000] {
            let e = fan_out(n);
            let options = DependentsOptions::default().with_max_work(budget);
            e.dependents(&a1, &options).unwrap();
            let (result, m) = measure(None, || e.dependents(&a1, &options).unwrap());
            // One unit for the queried cell, one per reported reader.
            assert_eq!(result.dependents.len() as u64, budget - 1);
            assert!(result.truncation.incomplete);
            seen.push((m.allocs, m.peak));
        }
        assert!(
            seen.windows(2).all(|w| w[0] == w[1]),
            "budget {budget}: allocations grow with the fan-out: {seen:?}"
        );
    }
    // Unbounded, every reader is still reported.
    let e = fan_out(1_000);
    let all = e
        .dependents(
            &a1,
            &DependentsOptions::default()
                .with_max_work(10_000)
                .with_max_results(10_000),
        )
        .unwrap();
    assert_eq!(all.dependents.len(), 1_000);
    assert!(!all.truncation.incomplete);
}

#[test]
fn small_trace_budgets_do_not_expand_the_fan_out() {
    let a1 = CellAddress::new("Sheet1", 1, 1).unwrap();
    let mut seen = Vec::new();
    for n in [100u32, 1_000, 10_000] {
        let e = fan_out(n);
        let options = TraceOptions::default()
            .with_direction(TraceDirection::Dependents)
            .with_max_work(3);
        // Warm up process-wide lazy state outside the measurement.
        e.trace(std::slice::from_ref(&a1), &options).unwrap();
        let (_, m) = measure(None, || {
            e.trace(std::slice::from_ref(&a1), &options).unwrap()
        });
        seen.push((m.allocs, m.peak));
    }
    assert!(
        seen.windows(2).all(|w| w[0] == w[1]),
        "trace allocations grow with the fan-out: {seen:?}"
    );
}
