use super::*;
use crate::engine::named_range::{NameScope, NamedDefinition};

fn review_compare(a: &Engine<TestWorkbook>, b: &Engine<TestWorkbook>, label: &str) {
    for r in 1..=24 {
        for c in 1..=12 {
            assert_eq!(v(a, r, c), v(b, r, c), "{label} ({r},{c})");
        }
    }
}

fn review_mutate(e: &mut Engine<TestWorkbook>, mode: usize) {
    match mode {
        0 => f(e, 1, 1, "=77"),
        1 => f(e, 19, 1, "=77"),
        2 => {
            e.insert_rows("Sheet1", 1, 1).unwrap();
        }
        3 => {
            e.delete_rows("Sheet1", 1, 1).unwrap();
        }
        4 => {
            e.insert_columns("Sheet1", 1, 1).unwrap();
        }
        5 => {
            e.delete_columns("Sheet1", 1, 1).unwrap();
        }
        6 => {
            e.add_sheet("Added").unwrap();
            e.set_cell_formula("Added", 1, 1, parse("=Sheet1!A20").unwrap())
                .unwrap();
        }
        7 => {
            let id = e.graph.sheet_id("Aux").unwrap();
            e.rename_sheet(id, "Renamed").unwrap();
        }
        8 => {
            let id = e.graph.sheet_id("Aux").unwrap();
            e.remove_sheet(id).unwrap();
        }
        9 => {
            e.update_name(
                "ReviewName",
                NamedDefinition::Literal(LiteralValue::Number(90.)),
                NameScope::Workbook,
            )
            .unwrap();
        }
        10 => {
            e.delete_name("ReviewName", NameScope::Workbook).unwrap();
        }
        11 => {
            f(e, 1, 1, "=A2");
        }
        12 => {
            f(e, 1, 1, "=4");
        }
        13 => {
            e.set_cell_value("Sheet1", 1, 8, LiteralValue::Text("A18".into()))
                .unwrap();
        }
        _ => unreachable!(),
    }
}

// Ordinary evaluation oracle with exactly the visible formula values at the
// cycle-forming edit. Install caches through the normal effects boundary;
// do not run the control first, since that would finish pending precedents.
fn copy_current_formula_values(from: &Engine<TestWorkbook>, to: &mut Engine<TestWorkbook>) {
    for r in 1..=24 {
        for c in 1..=12 {
            let vertex = to
                .graph
                .get_vertex_id_for_address(&to.graph.make_cell_ref("Sheet1", r, c));
            if let Some(vertex) = vertex.filter(|&v| to.graph.is_live_formula_vertex(v))
                && let Some(value) = v(from, r, c)
            {
                to.apply_effect(
                    &crate::engine::effects::Effect::WriteCell {
                        vertex_id: vertex,
                        value,
                    },
                    None,
                    None,
                )
                .unwrap();
            }
        }
    }
    review_compare(from, to, "current-value oracle before edit");
}
fn run_review_mutation_matrix(modes: &[usize], job: bool) {
    let mut failures = Vec::new();
    for family in [false, true] {
        for parallel in [false, true] {
            for &mode in modes {
                for cut in [1, 3, 9] {
                    let make = || {
                        let mut e = Engine::new(
                            TestWorkbook::new(),
                            EvalConfig {
                                family_execution: family,
                                enable_parallel: parallel,
                                ..EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.))
                            },
                        );
                        e.add_sheet("Aux").unwrap();
                        e.set_cell_value("Aux", 1, 1, LiteralValue::Number(7.))
                            .unwrap();
                        e.define_name(
                            "ReviewName",
                            NamedDefinition::Literal(LiteralValue::Number(3.)),
                            NameScope::Workbook,
                        )
                        .unwrap();
                        f(&mut e, 1, 1, if mode == 12 { "=A1+1" } else { "=1" });
                        for r in 2..=20 {
                            f(&mut e, r, 1, &format!("=A{}+1", r - 1));
                        }
                        f(&mut e, 1, 2, "=Aux!A1+ReviewName+A20");
                        e.set_cell_value("Sheet1", 1, 8, LiteralValue::Text("A3".into()))
                            .unwrap();
                        f(&mut e, 1, 9, "=INDIRECT(H1)");
                        f(&mut e, 2, 9, "=OFFSET(A1,1,0)");
                        e
                    };
                    let mut e = make();
                    let mut reference = make();
                    e.resumable_stop_after_units_for_test = Some(cut);
                    let _ = test_recalc_slice(&mut e, EvaluationStop::none(), job).unwrap();
                    if mode == 11 {
                        // F3 decision: a cycle created during the logical
                        // calculation starts from current values, not a fresh
                        // engine's missing formula caches.
                        copy_current_formula_values(&e, &mut reference);
                    }
                    review_mutate(&mut e, mode);
                    review_mutate(&mut reference, mode);
                    let a = test_recalc_slice(&mut e, EvaluationStop::none(), job);
                    let b = reference.evaluate_all();
                    let label =
                        format!("mutation {mode} family {family} parallel {parallel} cut {cut}");
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match (a, b) {
                            (Ok(EvaluationProgress::Complete(_)), Ok(_)) => {
                                review_compare(&e, &reference, &label)
                            }
                            (Ok(EvaluationProgress::Incomplete(_)), Ok(_)) => {
                                while matches!(
                                    test_recalc_slice(&mut e, EvaluationStop::none(), job).unwrap(),
                                    EvaluationProgress::Incomplete(_)
                                ) {}
                                review_compare(&e, &reference, &label);
                            }
                            (Err(a), Err(b)) => assert_eq!(a, b, "{label}"),
                            (a, b) => panic!("{label}: {a:?} vs {b:?}"),
                        }));
                    if result.is_err() {
                        failures.push(label);
                    }
                }
            }
        }
    }
    println!("MUTATIONS failures={failures:?}");
    assert!(failures.is_empty(), "{} matrix failures", failures.len());
}

#[test]
fn review_min_formula_dynamic() {
    let mut failures = Vec::new();
    for family in [false, true] {
        for len in 3..=12 {
            for cut in 1..=len {
                let make = || {
                    let mut e = engine(family);
                    f(&mut e, 1, 1, "=1");
                    for r in 2..=len {
                        f(&mut e, r, 1, &format!("=A{}+1", r - 1));
                    }
                    e.set_cell_value("Sheet1", 1, 8, LiteralValue::Text("A3".into()))
                        .unwrap();
                    f(&mut e, 1, 9, "=INDIRECT(H1)");
                    e
                };
                let mut e = make();
                let mut b = make();
                e.resumable_stop_after_units_for_test = Some(cut as usize);
                let p = e.evaluate_all_until(EvaluationStop::none()).unwrap();
                if !matches!(p, EvaluationProgress::Incomplete(_)) {
                    continue;
                }
                let pre = v(&e, 1, 9);
                f(&mut e, 1, 1, "=77");
                f(&mut b, 1, 1, "=77");
                drain(&mut e);
                b.evaluate_all().unwrap();
                if v(&e, 1, 9) != v(&b, 1, 9) {
                    let id = e
                        .graph
                        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 9))
                        .unwrap();
                    println!(
                        "MINFORM family={family} len={len} cut={cut} pre={pre:?} actual={:?} expected={:?} dirty={}",
                        v(&e, 1, 9),
                        v(&b, 1, 9),
                        e.graph.is_dirty(id)
                    );
                    failures.push((family, len, cut));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{} failures", failures.len());
}

#[test]
fn review_min_new_builtin_after_slice() {
    for (family, job) in [(false, false), (true, false), (false, true), (true, true)] {
        let make = || {
            let mut e = chain(family);
            e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(-5.))
                .unwrap();
            e
        };
        let mut e = make();
        let mut b = make();
        incomplete(test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap());
        f(&mut e, 20, 1, "=ABS(B1)");
        f(&mut b, 20, 1, "=ABS(B1)");
        while matches!(
            test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap(),
            EvaluationProgress::Incomplete(_)
        ) {}
        b.evaluate_all().unwrap();
        println!(
            "NEWFUNCTION actual={:?} expected={:?}",
            v(&e, 20, 1),
            v(&b, 20, 1)
        );
        assert_eq!(v(&e, 20, 1), v(&b, 20, 1));
    }
}

#[test]
fn review_min_dynamic_value_edit() {
    for family in [false, true] {
        let make = || {
            let mut e = engine(family);
            e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.))
                .unwrap();
            for r in 2..=6 {
                f(&mut e, r, 1, &format!("=A{}+1", r - 1));
            }
            f(&mut e, 1, 2, "=INDIRECT(\"A3\")");
            e
        };
        let mut e = make();
        let mut b = make();
        e.resumable_stop_after_units_for_test = Some(4);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        println!("DYNVALUE before={:?}", v(&e, 1, 2));
        for x in [&mut e, &mut b] {
            x.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(77.))
                .unwrap();
        }
        drain(&mut e);
        b.evaluate_all().unwrap();
        println!(
            "DYNVALUE family={family} actual={:?} expected={:?}",
            v(&e, 1, 2),
            v(&b, 1, 2)
        );
        assert_eq!(v(&e, 1, 2), v(&b, 1, 2));
    }
}

#[test]
fn review_mutation_matrix() {
    // These 156 cases retain the strict fresh-uninterrupted oracle.
    run_review_mutation_matrix(
        &(0..14).filter(|&mode| mode != 11).collect::<Vec<_>>(),
        false,
    );
}
#[test]
fn review_mutation_matrix_cycle_creation_f3() {
    // The twelve F3 cases use the maintainer-approved current-value oracle.
    run_review_mutation_matrix(&[11], false);
}
#[test]
fn job_review_mutation_matrix() {
    run_review_mutation_matrix(&(0..14).collect::<Vec<_>>(), true);
}
#[derive(Debug)]
struct Replacement {
    name: &'static str,
    value: f64,
}
impl Function for Replacement {
    fn name(&self) -> &'static str {
        self.name
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Ok(CalcValue::Scalar(LiteralValue::Number(self.value)))
    }
}
#[test]
fn review_min_registry_dirty() {
    if isolated_test("mutations::review_min_registry_dirty") {
        return;
    }
    for (family, job) in [(false, false), (true, false), (false, true), (true, true)] {
        const NAME: &str = "RESUMABLE_REPLACE_PENDING";
        crate::function_registry::register_function(Arc::new(Replacement {
            name: NAME,
            value: 1.,
        }));
        let mut e = engine(family);
        f(&mut e, 1, 1, "=1");
        f(&mut e, 2, 1, "=A1+RESUMABLE_REPLACE_PENDING()");
        f(&mut e, 3, 1, "=A2+1");
        incomplete(test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap());
        assert_eq!(v(&e, 2, 1), None);
        crate::function_registry::register_function(Arc::new(Replacement {
            name: NAME,
            value: 9.,
        }));
        while matches!(
            test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap(),
            EvaluationProgress::Incomplete(_)
        ) {}
        assert_eq!(v(&e, 2, 1), Some(LiteralValue::Number(10.)));
    }
}
#[test]
fn review_custom_registration_between_calls() {
    if isolated_test("mutations::review_custom_registration_between_calls") {
        return;
    }
    for (family, job) in [(false, false), (true, false), (false, true), (true, true)] {
        const NAME: &str = "RESUMABLE_REPLACE_INSTALLED";
        crate::function_registry::register_function(Arc::new(Replacement {
            name: NAME,
            value: 1.,
        }));
        let mut e = chain(family);
        f(&mut e, 1, 1, "=RESUMABLE_REPLACE_INSTALLED()");
        incomplete(test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap());
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
        crate::function_registry::register_function(Arc::new(Replacement {
            name: NAME,
            value: 9.,
        }));
        // A mutation consumes the engine's usual semantic-epoch guard. The
        // pending recalculation must retain its own epoch for invalidation.
        e.set_cell_value("Sheet1", 1, 12, LiteralValue::Number(1.))
            .unwrap();
        while matches!(
            test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap(),
            EvaluationProgress::Incomplete(_)
        ) {}
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(9.)));
        assert_eq!(v(&e, 20, 1), Some(LiteralValue::Number(28.)));
        assert_eq!(e.recalc_epoch, 1);
    }
}

#[derive(Debug)]
struct ProviderValue(Arc<AtomicUsize>);
impl Function for ProviderValue {
    fn name(&self) -> &'static str {
        "RESUMABLE_PROVIDER_REVISION"
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
fn review_provider_revision_invalidates_installed_work() {
    for (family, job) in [(false, false), (true, false), (false, true), (true, true)] {
        let value = Arc::new(AtomicUsize::new(1));
        let provider = TestWorkbook::new().with_function(Arc::new(ProviderValue(value.clone())));
        let revision = provider.planning_revision_handle();
        let mut e = Engine::new(
            provider,
            EvalConfig {
                family_execution: family,
                ..EvalConfig::default()
            },
        );
        f(&mut e, 1, 1, "=RESUMABLE_PROVIDER_REVISION()");
        for r in 2..=20 {
            f(&mut e, r, 1, &format!("=A{}+1", r - 1));
        }
        incomplete(test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap());
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
        value.store(9, Ordering::SeqCst);
        revision.fetch_add(1, Ordering::SeqCst);
        e.set_cell_value("Sheet1", 1, 12, LiteralValue::Number(1.))
            .unwrap();
        while matches!(
            test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap(),
            EvaluationProgress::Incomplete(_)
        ) {}
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(9.)));
        assert_eq!(v(&e, 20, 1), Some(LiteralValue::Number(28.)));
    }
}
#[test]
fn review_mutations_preserve_clock_and_rand_epoch() {
    for (family, job) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut e = chain(family);
        let clock = Arc::new(Clock(AtomicUsize::new(0)));
        e.set_clock(clock.clone());
        f(&mut e, 1, 2, "=NOW()");
        f(&mut e, 2, 2, "=RAND()");
        e.resumable_stop_after_units_for_test = Some(3);
        let before = clock.0.load(Ordering::SeqCst);
        incomplete(test_recalc_slice(&mut e, EvaluationStop::none(), job).unwrap());
        let now = v(&e, 1, 2).expect("NOW installed");
        let rand = v(&e, 2, 2).expect("RAND installed");
        assert_eq!(e.recalc_epoch, 0);
        e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(99.))
            .unwrap();
        while matches!(
            test_recalc_slice(&mut e, EvaluationStop::after(Duration::ZERO), job).unwrap(),
            EvaluationProgress::Incomplete(_)
        ) {}
        assert_eq!(v(&e, 1, 2), Some(now));
        assert_eq!(v(&e, 2, 2), Some(rand));
        assert_eq!(clock.0.load(Ordering::SeqCst) - before, 1);
        assert_eq!(e.recalc_epoch, 1);
    }
}

fn cycle_change_engine(family: bool, existing: bool) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            enable_parallel: false,
            ..EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.))
        },
    );
    f(&mut e, 1, 1, if existing { "=A1+1" } else { "=1" });
    for r in 2..=20 {
        f(&mut e, r, 1, &format!("=A{}+1", r - 1));
    }
    // A tail after A20 keeps the recalculation pending even when family
    // execution lifts the entire first chain into one atomic unit.
    f(&mut e, 1, 3, "=A20+1");
    for r in 2..=20 {
        f(&mut e, r, 3, &format!("=C{}+1", r - 1));
    }
    e
}
#[test]
fn newly_formed_cycle_uses_current_values() {
    for family in [false, true] {
        let mut e = cycle_change_engine(family, false);
        let mut ordinary = cycle_change_engine(family, false);
        e.resumable_stop_after_units_for_test = Some(2);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        copy_current_formula_values(&e, &mut ordinary);
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
        assert_eq!(v(&e, 2, 1), Some(LiteralValue::Number(2.)));
        for x in [&mut e, &mut ordinary] {
            f(x, 1, 1, "=A2");
        }
        drain(&mut e);
        ordinary.evaluate_all().unwrap();
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(2.)));
        assert_eq!(v(&e, 2, 1), Some(LiteralValue::Number(3.)));
        review_compare(&e, &ordinary, "brand-new cycle");
    }
}
#[test]
fn changed_cycle_membership_uses_current_values() {
    for family in [false, true] {
        let mut e = cycle_change_engine(family, true);
        let mut ordinary = cycle_change_engine(family, true);
        e.resumable_stop_after_units_for_test = Some(1);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        copy_current_formula_values(&e, &mut ordinary);
        for x in [&mut e, &mut ordinary] {
            f(x, 1, 1, "=A1+A2");
        }
        drain(&mut e);
        ordinary.evaluate_all().unwrap();
        assert_eq!(v(&ordinary, 1, 1), Some(LiteralValue::Number(1.)));
        assert_eq!(v(&ordinary, 2, 1), Some(LiteralValue::Number(2.)));
        review_compare(&e, &ordinary, "new SCC containing an earlier cycle member");
    }
}
#[test]
fn newly_created_cycle_reformation_uses_current_values() {
    for family in [false, true] {
        let mut e = cycle_change_engine(family, false);
        e.resumable_stop_after_units_for_test = Some(2);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        f(&mut e, 1, 1, "=A1+A2");
        e.resumable_stop_after_units_for_test = Some(1);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(3.)));
        f(&mut e, 1, 1, "=7");
        e.resumable_stop_after_units_for_test = Some(2);
        incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
        assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(7.)));
        assert_eq!(v(&e, 2, 1), Some(LiteralValue::Number(8.)));
        let mut ordinary = cycle_change_engine(family, false);
        f(&mut ordinary, 1, 1, "=7");
        copy_current_formula_values(&e, &mut ordinary);
        for x in [&mut e, &mut ordinary] {
            f(x, 1, 1, "=A1+A2");
        }
        drain(&mut e);
        ordinary.evaluate_all().unwrap();
        assert_eq!(v(&ordinary, 1, 1), Some(LiteralValue::Number(15.)));
        review_compare(&e, &ordinary, "re-formed new cycle");
    }
}

#[test]
fn original_cycle_reformation_keeps_original_seed() {
    for family in [false, true] {
        for pre_cancelled in [false, true] {
            let mut e = cycle_change_engine(family, true);
            e.resumable_stop_after_units_for_test = Some(1);
            if pre_cancelled {
                let cancel = CancelToken::new();
                cancel.cancel();
                incomplete(
                    e.evaluate_all_until(EvaluationStop::cancel(cancel))
                        .unwrap(),
                );
                assert_eq!(v(&e, 1, 1), None);
            }
            incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
            assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
            f(&mut e, 1, 1, "=7");
            incomplete(e.evaluate_all_until(EvaluationStop::none()).unwrap());
            assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(7.)));
            f(&mut e, 1, 1, "=A1+1");
            drain(&mut e);
            // This SCC existed at logical start: preserve the original helper's
            // pre-recalc seed, even though its members ran acyclically in between.
            assert_eq!(v(&e, 1, 1), Some(LiteralValue::Number(1.)));
        }
    }
}
