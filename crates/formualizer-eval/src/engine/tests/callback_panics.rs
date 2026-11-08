use crate::{
    engine::{Engine, EvalConfig, EvaluationStop, StepPhase},
    function::{FamilyKernel, FnCaps, Function},
    test_workbook::TestWorkbook,
    traits::*,
};
use formualizer_common::{ExcelError, ExcelErrorExtra, LiteralValue};
use formualizer_parse::parser::{TableReference, parse};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Metadata {
    mode: u8,
    armed: Arc<AtomicBool>,
}
impl Function for Metadata {
    fn name(&self) -> &'static str {
        "FAULT_METADATA"
    }
    fn caps(&self) -> FnCaps {
        if self.mode == 0 && self.armed.load(Ordering::SeqCst) {
            panic!("caps metadata fault")
        };
        FnCaps::PURE
    }
    fn reports_known_limitations(&self) -> bool {
        if self.mode == 1 && self.armed.load(Ordering::SeqCst) {
            panic!("reporting metadata fault")
        };
        true
    }
    fn family_kernel(&self) -> Option<FamilyKernel> {
        if self.mode == 2 && self.armed.load(Ordering::SeqCst) {
            panic!("family metadata fault")
        };
        None
    }
    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        if (self.mode == 3 && self.armed.load(Ordering::SeqCst))
            || (self.mode == 6 && self.armed.swap(false, Ordering::SeqCst))
        {
            panic!("schema metadata fault")
        };
        &[]
    }
    fn min_args(&self) -> usize {
        if self.mode == 4 && self.armed.load(Ordering::SeqCst) {
            panic!("arity metadata fault")
        };
        0
    }
    fn propagate_format(&self, _: &CalcValue<'_>) -> Option<crate::format::FormatId> {
        if self.mode == 5 && self.armed.load(Ordering::SeqCst) {
            panic!("format metadata fault")
        };
        None
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Ok(CalcValue::Scalar(LiteralValue::Number(7.)))
    }
}
fn run<R: EvaluationContext>(e: &mut Engine<R>, job: bool) {
    if job {
        e.begin_recalc(Default::default()).unwrap();
        for _ in 0..1000 {
            let report = e
                .step_recalc(EvaluationStop::after(std::time::Duration::ZERO))
                .unwrap();
            if report.phase == StepPhase::Finished {
                return;
            }
        }
        panic!("job failed to finish");
    } else {
        e.evaluate_all().unwrap();
    }
}
fn check_error<R: EvaluationContext>(e: &Engine<R>, row: u32, name: &str) {
    assert!(!e.is_poisoned());
    let Some(LiteralValue::Error(error)) = e.get_cell_value("Sheet1", row, 1) else {
        panic!("missing typed callback failure at {row}")
    };
    assert!(
        matches!(error.extra(),ExcelErrorExtra::FunctionPanicked{function,message:Some(_)} if function==name),
        "{error:?}"
    );
}
#[test]
fn runtime_metadata_faults_are_request_scoped_cell_failures() {
    for mode in 0..7 {
        for family in [false, true] {
            for job in [false, true] {
                let armed = Arc::new(AtomicBool::new(false));
                let mut e = Engine::new(
                    TestWorkbook::new().with_function(Arc::new(Metadata {
                        mode,
                        armed: armed.clone(),
                    })),
                    EvalConfig {
                        family_execution: family,
                        enable_parallel: false,
                        ..Default::default()
                    },
                );
                e.configure_operation_diagnostics(Default::default());
                for row in 1..=32 {
                    e.set_cell_formula("Sheet1", row, 1, parse("=FAULT_METADATA()").unwrap())
                        .unwrap();
                }
                armed.store(true, Ordering::SeqCst);
                // family_kernel is queried only when family planning is enabled.
                if mode == 2 && !family {
                    run(&mut e, job);
                    continue;
                }
                run(&mut e, job);
                for row in 1..=32 {
                    check_error(&e, row, "FAULT_METADATA");
                }
                assert_eq!(
                    e.operation_diagnostics()
                        .unwrap()
                        .unwrap()
                        .logical_root_cell_count,
                    32
                );
                armed.store(false, Ordering::SeqCst);
                for row in 1..=32 {
                    e.set_cell_formula("Sheet1", row, 1, parse("=FAULT_METADATA()").unwrap())
                        .unwrap();
                }
                run(&mut e, job);
                for row in 1..=32 {
                    assert_eq!(
                        e.get_cell_value("Sheet1", row, 1),
                        Some(LiteralValue::Number(7.))
                    );
                }
            }
        }
    }
}

#[test]
fn metadata_faults_without_evidence_remain_request_scoped() {
    for mode in 0..7 {
        for family in [false, true] {
            for job in [false, true] {
                if mode == 2 && !family {
                    continue;
                }
                let armed = Arc::new(AtomicBool::new(false));
                let mut e = Engine::new(
                    TestWorkbook::new().with_function(Arc::new(Metadata {
                        mode,
                        armed: armed.clone(),
                    })),
                    EvalConfig {
                        family_execution: family,
                        enable_parallel: false,
                        ..Default::default()
                    },
                );
                for row in 1..=32 {
                    e.set_cell_formula("Sheet1", row, 1, parse("=FAULT_METADATA()").unwrap())
                        .unwrap();
                }
                armed.store(true, Ordering::SeqCst);
                run(&mut e, job);
                for row in 1..=32 {
                    check_error(&e, row, "FAULT_METADATA");
                }
                armed.store(false, Ordering::SeqCst);
                for row in 1..=32 {
                    e.set_cell_formula("Sheet1", row, 1, parse("=FAULT_METADATA()").unwrap())
                        .unwrap();
                }
                run(&mut e, job);
                for row in 1..=32 {
                    assert_eq!(
                        e.get_cell_value("Sheet1", row, 1),
                        Some(LiteralValue::Number(7.))
                    );
                }
            }
        }
    }
}

struct Sources {
    base: TestWorkbook,
    armed: Arc<AtomicBool>,
}
impl ReferenceResolver for Sources {
    fn resolve_cell_reference(
        &self,
        s: Option<&str>,
        r: u32,
        c: u32,
    ) -> Result<LiteralValue, ExcelError> {
        self.base.resolve_cell_reference(s, r, c)
    }
}
impl RangeResolver for Sources {
    fn resolve_range_reference(
        &self,
        s: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        self.base.resolve_range_reference(s, sr, sc, er, ec)
    }
}
impl NamedRangeResolver for Sources {
    fn resolve_named_range_reference(&self, n: &str) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        self.base.resolve_named_range_reference(n)
    }
}
impl TableResolver for Sources {
    fn resolve_table_reference(&self, t: &TableReference) -> Result<Box<dyn Table>, ExcelError> {
        self.base.resolve_table_reference(t)
    }
}
impl FunctionProvider for Sources {
    fn get_function(&self, ns: &str, n: &str) -> Option<Arc<dyn Function>> {
        self.base.get_function(ns, n)
    }
}
impl SourceResolver for Sources {
    fn resolve_source_table(&self, _: &str) -> Result<Box<dyn Table>, ExcelError> {
        assert!(
            !self.armed.load(Ordering::SeqCst),
            "source table callback fault"
        );
        Ok(Box::new(SourceTable))
    }

    fn resolve_source_scalar(&self, _: &str) -> Result<LiteralValue, ExcelError> {
        assert!(
            !self.armed.load(Ordering::SeqCst),
            "source scalar callback fault"
        );
        Ok(LiteralValue::Number(7.))
    }
}
impl Resolver for Sources {}
impl EvaluationContext for Sources {}
#[test]
fn direct_and_wrapped_source_faults_keep_source_attribution_and_recover() {
    for family in [false, true] {
        for job in [false, true] {
            for expression in ["=ReviewSource", "=ABS(ReviewSource)"] {
                let armed = Arc::new(AtomicBool::new(true));
                let mut e = Engine::new(
                    Sources {
                        base: TestWorkbook::new(),
                        armed: armed.clone(),
                    },
                    EvalConfig {
                        family_execution: family,
                        enable_parallel: false,
                        ..Default::default()
                    },
                );
                e.configure_operation_diagnostics(Default::default());
                e.define_source_scalar("ReviewSource", Some(1)).unwrap();
                e.set_cell_formula("Sheet1", 1, 1, parse(expression).unwrap())
                    .unwrap();
                run(&mut e, job);
                check_error(&e, 1, "resolver:resolve_source_scalar:ReviewSource");
                assert_eq!(
                    e.operation_diagnostics()
                        .unwrap()
                        .unwrap()
                        .logical_root_cell_count,
                    1
                );
                armed.store(false, Ordering::SeqCst);
                e.set_cell_formula("Sheet1", 1, 1, parse(expression).unwrap())
                    .unwrap();
                run(&mut e, job);
                assert_eq!(
                    e.get_cell_value("Sheet1", 1, 1),
                    Some(LiteralValue::Number(7.))
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
struct SourceTable;
impl Table for SourceTable {
    fn get_cell(&self, _: usize, _: &str) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Number(7.))
    }
    fn get_column(&self, _: &str) -> Result<Box<dyn Range>, ExcelError> {
        Ok(Box::new(InMemoryRange::new(vec![vec![
            LiteralValue::Number(7.),
        ]])))
    }
    fn columns(&self) -> Vec<String> {
        vec!["Amount".into()]
    }
    fn data_height(&self) -> usize {
        1
    }
    fn clone_box(&self) -> Box<dyn Table> {
        Box::new(self.clone())
    }
}
#[test]
fn table_resolver_faults_keep_attribution_and_cache_is_not_poisoned() {
    for family in [false, true] {
        for job in [false, true] {
            let armed = Arc::new(AtomicBool::new(true));
            let mut e = Engine::new(
                Sources {
                    base: TestWorkbook::new(),
                    armed: armed.clone(),
                },
                EvalConfig {
                    family_execution: family,
                    enable_parallel: false,
                    ..Default::default()
                },
            );
            e.configure_operation_diagnostics(Default::default());
            e.define_source_table("ReviewTable", Some(1)).unwrap();
            e.set_cell_formula("Sheet1", 1, 1, parse("=SUM(ReviewTable[Amount])").unwrap())
                .unwrap();
            run(&mut e, job);
            check_error(&e, 1, "resolver:resolve_source_table:ReviewTable");
            assert_eq!(
                e.operation_diagnostics()
                    .unwrap()
                    .unwrap()
                    .logical_root_cell_count,
                1
            );
            armed.store(false, Ordering::SeqCst);
            e.set_cell_formula("Sheet1", 1, 1, parse("=SUM(ReviewTable[Amount])").unwrap())
                .unwrap();
            run(&mut e, job);
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 1),
                Some(LiteralValue::Number(7.))
            );
        }
    }
}

#[derive(Debug)]
struct FaultClock {
    armed: Arc<AtomicBool>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl crate::timezone::ClockProvider for FaultClock {
    fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
        &crate::timezone::TimeZoneSpec::Utc
    }
    fn now(&self) -> chrono::NaiveDateTime {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!self.armed.load(Ordering::SeqCst), "clock callback fault");
        chrono::DateTime::UNIX_EPOCH.naive_utc()
    }
}
#[test]
fn failed_clock_sample_is_frozen_for_job_and_only_clock_roots_fail() {
    for job in [false, true] {
        for family in [false, true] {
            let armed = Arc::new(AtomicBool::new(false));
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut e = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    family_execution: family,
                    enable_parallel: false,
                    ..Default::default()
                },
            );
            e.set_clock(Arc::new(FaultClock {
                armed: armed.clone(),
                calls: calls.clone(),
            }));
            e.configure_operation_diagnostics(Default::default());
            for (row, text) in [
                (1, "=NOW()"),
                (2, "=TODAY()"),
                (3, "=1+2"),
                (4, "=IFERROR(NOW(),99)"),
            ] {
                e.set_cell_formula("Sheet1", row, 1, parse(text).unwrap())
                    .unwrap();
            }
            calls.store(0, Ordering::SeqCst);
            armed.store(true, Ordering::SeqCst);
            if job {
                e.begin_recalc(Default::default()).unwrap();
                for _ in 0..20 {
                    let report = e
                        .step_recalc(EvaluationStop::after(std::time::Duration::ZERO))
                        .unwrap();
                    // Removing the source fault mid-job must not replace the failed sample.
                    armed.store(false, Ordering::SeqCst);
                    if report.phase == StepPhase::Finished {
                        break;
                    }
                }
                assert!(!e.has_active_recalc_job());
            } else {
                run(&mut e, false);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            check_error(&e, 1, "resolver:clock");
            check_error(&e, 2, "resolver:clock");
            assert_eq!(
                e.get_cell_value("Sheet1", 3, 1),
                Some(LiteralValue::Number(3.))
            );
            assert_eq!(
                e.get_cell_value("Sheet1", 4, 1),
                Some(LiteralValue::Number(99.))
            );
            assert_eq!(
                e.operation_diagnostics()
                    .unwrap()
                    .unwrap()
                    .logical_root_cell_count,
                3
            );
            armed.store(false, Ordering::SeqCst);
            run(&mut e, job);
            assert!(!e.is_poisoned());
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert!(!matches!(
                e.get_cell_value("Sheet1", 1, 1),
                Some(LiteralValue::Error(_))
            ));
        }
    }
}

struct HostMetadata {
    base: TestWorkbook,
    mode: u8,
    armed: Arc<AtomicBool>,
}
impl ReferenceResolver for HostMetadata {
    fn resolve_cell_reference(
        &self,
        s: Option<&str>,
        r: u32,
        c: u32,
    ) -> Result<LiteralValue, ExcelError> {
        self.base.resolve_cell_reference(s, r, c)
    }
}
impl RangeResolver for HostMetadata {
    fn resolve_range_reference(
        &self,
        s: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        self.base.resolve_range_reference(s, sr, sc, er, ec)
    }
}
impl NamedRangeResolver for HostMetadata {
    fn resolve_named_range_reference(&self, n: &str) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        self.base.resolve_named_range_reference(n)
    }
}
impl TableResolver for HostMetadata {
    fn resolve_table_reference(&self, t: &TableReference) -> Result<Box<dyn Table>, ExcelError> {
        self.base.resolve_table_reference(t)
    }
}
impl FunctionProvider for HostMetadata {
    fn get_function(&self, ns: &str, n: &str) -> Option<Arc<dyn Function>> {
        assert!(
            !(self.mode == 2 && n == "ABS" && self.armed.load(Ordering::SeqCst)),
            "provider lookup fault"
        );
        self.base.get_function(ns, n)
    }
    fn planning_semantic_revision(&self) -> Option<u64> {
        assert!(
            !(self.mode == 3 && self.armed.load(Ordering::SeqCst)),
            "provider revision fault"
        );
        Some(1)
    }
}
impl SourceResolver for HostMetadata {
    fn resolve_source_scalar(&self, _: &str) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Number(7.))
    }
    fn resolve_source_table(&self, _: &str) -> Result<Box<dyn Table>, ExcelError> {
        Ok(Box::new(SourceTable))
    }
    fn source_scalar_version(&self, _: &str) -> Option<u64> {
        assert!(
            !(self.mode == 0 && self.armed.load(Ordering::SeqCst)),
            "scalar version fault"
        );
        Some(1)
    }
    fn source_table_version(&self, _: &str) -> Option<u64> {
        assert!(
            !(self.mode == 1 && self.armed.load(Ordering::SeqCst)),
            "table version fault"
        );
        Some(1)
    }
}
impl Resolver for HostMetadata {}
impl EvaluationContext for HostMetadata {}
#[test]
fn source_versions_and_function_provider_callbacks_are_contained_and_recover() {
    for mode in 0..4 {
        for family in [false, true] {
            for job in [false, true] {
                let armed = Arc::new(AtomicBool::new(false));
                let mut e = Engine::new(
                    HostMetadata {
                        base: TestWorkbook::new(),
                        mode,
                        armed: armed.clone(),
                    },
                    EvalConfig {
                        family_execution: family,
                        enable_parallel: false,
                        ..Default::default()
                    },
                );
                e.configure_operation_diagnostics(Default::default());
                e.define_source_scalar("ReviewSource", None).unwrap();
                e.define_source_table("ReviewTable", None).unwrap();
                let expression = match mode {
                    0 => "=ReviewSource",
                    1 => "=SUM(ReviewTable[Amount])",
                    2 => "=ABS(ReviewSource)",
                    _ => "=1+2",
                };
                e.set_cell_formula("Sheet1", 1, 1, parse(expression).unwrap())
                    .unwrap();
                armed.store(true, Ordering::SeqCst);
                let result = if job {
                    e.begin_recalc(Default::default()).unwrap();
                    e.step_recalc(EvaluationStop::none())
                        .map(|_| ())
                        .map_err(|e| match e {
                            crate::engine::RecalcJobError::Evaluation(error) => error,
                            other => panic!("{other:?}"),
                        })
                } else {
                    e.evaluate_all().map(|_| ())
                };
                assert!(!e.is_poisoned());
                let expected = match mode {
                    0 => "resolver:source_scalar_version:ReviewSource",
                    1 => "resolver:source_table_version:ReviewTable",
                    2 => "resolver:get_function:ABS",
                    _ => "resolver:planning_semantic_revision",
                };
                if let Err(error) = result {
                    assert!(
                        matches!(error.extra(),ExcelErrorExtra::FunctionPanicked{function,..} if function==expected),
                        "{error:?}"
                    );
                } else {
                    check_error(&e, 1, expected);
                    assert_eq!(
                        e.operation_diagnostics()
                            .unwrap()
                            .unwrap()
                            .logical_root_cell_count,
                        1
                    );
                }
                armed.store(false, Ordering::SeqCst);
                e.set_cell_formula("Sheet1", 1, 1, parse(expression).unwrap())
                    .unwrap();
                run(&mut e, job);
                assert_eq!(
                    e.get_cell_value("Sheet1", 1, 1),
                    Some(LiteralValue::Number(if mode == 3 { 3. } else { 7. }))
                );
            }
        }
    }
}
