use super::*;
#[test]
fn job_scalar_reference_run_delta_covers_array_input_projection() {
    if isolated_test("array_delta::job_scalar_reference_run_delta_covers_array_input_projection") {
        return;
    }
    crate::builtins::load_builtins();
    let make = || {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                enable_parallel: false,
                ..Default::default()
            },
        );
        e.set_cell_value(
            "Sheet1",
            1,
            1,
            LiteralValue::Array(vec![vec![
                LiteralValue::Number(3.),
                LiteralValue::Number(4.),
            ]]),
        )
        .unwrap();
        for r in 1..=8 {
            e.set_cell_formula("Sheet1", r, 2, parse("=$A$1").unwrap())
                .unwrap();
        }
        e
    };
    let mut expected = make();
    expected.evaluate_all().unwrap();
    let mut e = make();
    e.begin_recalc(Default::default()).unwrap();
    let mut changed = std::collections::BTreeSet::new();
    loop {
        let r = e
            .step_recalc(EvaluationStop::after(Duration::ZERO))
            .unwrap();
        for rect in r.changed.delta.records {
            let (r0, c0, r1, c1) = rect.bounds();
            for row in r0..=r1 {
                for col in c0..=c1 {
                    changed.insert((row + 1, col + 1));
                }
            }
        }
        assert!(r.changed.overflow_sheets.is_empty());
        if r.phase == StepPhase::Finished {
            break;
        }
    }
    for row in 1..=8 {
        for col in 2..=3 {
            let value = expected.get_cell_value("Sheet1", row, col);
            assert_eq!(e.get_cell_value("Sheet1", row, col), value);
            if value.is_some_and(|v| v != LiteralValue::Empty) {
                assert!(changed.contains(&(row, col)), "missing {row},{col}");
            }
        }
    }
}
