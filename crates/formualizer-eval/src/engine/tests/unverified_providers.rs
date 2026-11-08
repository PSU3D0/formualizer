use super::common::arrow_eval_config;
use crate::{
    args::ArgSchema,
    engine::{Engine, EvalConfig},
    function::{FamilyKernel, FnCaps, Function},
    operation_diagnostics::{OperationDiagnosticOptions, UnavailableOperation},
    test_workbook::TestWorkbook,
    traits::{ArgumentHandle, CalcValue, FunctionContext},
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::{Arc, LazyLock};

struct Provider {
    name: &'static str,
    reporting: bool,
    error: bool,
}
impl Function for Provider {
    fn name(&self) -> &'static str {
        self.name
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        self.name == "MATCH"
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn reports_known_limitations(&self) -> bool {
        self.reporting
    }
    fn family_kernel(&self) -> Option<FamilyKernel> {
        (self.name == "MATCH").then_some(FamilyKernel::Lookup)
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if self.error {
            Err(ExcelError::new(ExcelErrorKind::Div))
        } else {
            Ok(CalcValue::Scalar(LiteralValue::Number(42.0)))
        }
    }
}

fn evaluate(
    name: &'static str,
    reporting: bool,
    error: bool,
    active: bool,
    lift: bool,
    kernels: bool,
) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let wb = TestWorkbook::new().with_function(Arc::new(Provider {
        name,
        reporting,
        error,
    }));
    let mut engine = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: false,
            family_lift: lift,
            family_kernels: kernels,
            ..arrow_eval_config()
        },
    );
    engine.configure_operation_diagnostics(OperationDiagnosticOptions {
        sample_limit: 0,
        track_unverified_providers: true,
        ..Default::default()
    });
    engine
        .set_cell_value("Sheet1", 1, 26, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 26, LiteralValue::Number(2.0))
        .unwrap();
    let call = if name == "MATCH" {
        "MATCH($Z$1,$Z$1:$Z$2)".to_string()
    } else {
        format!("{name}($Z$1)")
    };
    for row in 1..=8 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))
            .unwrap();
        let expression = if active {
            format!("=IF(A{row}>0,IFERROR({call},7),5)")
        } else {
            format!("=IF(A{row}>0,5,{call})")
        };
        engine
            .set_cell_formula("Sheet1", row, 2, parse(&expression).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    for row in 1..=8 {
        let expected = if !active {
            5.0
        } else if error {
            7.0
        } else {
            42.0
        };
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(expected))
        );
    }
    engine
}

#[test]
fn recalc_contract_unverified_provider_follows_executing_instance() {
    for name in ["LOCAL_COVERAGE", "ABS", "STDEV"] {
        for (lift, kernels) in [(false, false), (true, false), (false, true)] {
            for error in [false, true] {
                let engine = evaluate(name, false, error, true, lift, kernels);
                let report = engine.operation_diagnostics().unwrap().unwrap();
                assert_eq!(
                    report.logical_root_cell_count, 8,
                    "name={name} lift={lift} kernels={kernels} error={error}: {report:?}"
                );
                assert!(report.roots.iter().all(|root| matches!(
                    root.operation.as_ref(), UnavailableOperation::UnverifiedProvider { name: actual }
                    if actual == name
                )));
                assert!(report.samples.is_empty());
                assert_eq!(report.physical_function_dispatches, None);
            }
        }
    }
}

#[test]
fn recalc_contract_unverified_provider_cannot_inherit_a_family_kernel_attestation() {
    for (lift, kernels) in [(false, false), (true, false), (false, true)] {
        let engine = evaluate("MATCH", false, false, true, lift, kernels);
        let report = engine.operation_diagnostics().unwrap().unwrap();
        assert_eq!(report.logical_root_cell_count, 8, "{report:?}");
    }
}

#[test]
fn recalc_contract_unverified_provider_explicit_contract_and_inactive_controls() {
    for (reporting, active) in [(true, true), (false, false)] {
        let engine = evaluate("LOCAL_COVERAGE", reporting, false, active, true, true);
        assert!(
            engine
                .operation_diagnostics()
                .unwrap()
                .unwrap()
                .roots
                .is_empty()
        );
    }
}

#[test]
fn recalc_contract_unverified_provider_genuine_builtin_and_alias_controls() {
    crate::builtins::load_builtins();
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    engine.configure_operation_diagnostics(OperationDiagnosticOptions {
        track_unverified_providers: true,
        ..Default::default()
    });
    for (row, formula) in [(1, "=ABS(-2)"), (2, "=STDEV({1;2})"), (3, "=_xlfn.ABS(-3)")] {
        engine
            .set_cell_formula("Sheet1", row, 1, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert!(
        engine
            .operation_diagnostics()
            .unwrap()
            .unwrap()
            .roots
            .is_empty()
    );
}

#[test]
fn recalc_contract_reference_dispatch_is_measured_as_execution() {
    crate::builtins::load_builtins();
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine.configure_operation_diagnostics(OperationDiagnosticOptions {
        physical_telemetry: true,
        ..Default::default()
    });
    let cell = crate::CellRef::new(0, crate::reference::Coord::new(0, 1, true, true));
    let interpreter = crate::interpreter::Interpreter::new_with_cell(&engine, "Sheet1", cell);
    interpreter
        .evaluate_ast_as_reference(&parse("=INDIRECT(\"A1\")").unwrap())
        .unwrap();
    let report = engine.operation_diagnostics().unwrap().unwrap();
    assert_eq!(report.physical_function_dispatches, Some(1));
    assert!(report.roots.is_empty());
}

struct ReferenceProvider;
impl Function for ReferenceProvider {
    fn name(&self) -> &'static str {
        "LOCAL_REFERENCE"
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE | FnCaps::RETURNS_REFERENCE
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        panic!("this provider's value fallback must not run")
    }
    fn eval_reference<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Option<Result<formualizer_parse::parser::ReferenceType, ExcelError>> {
        Some(Ok(formualizer_parse::parser::ReferenceType::from_string(
            "A1",
        )
        .unwrap()))
    }
}
#[test]
fn recalc_contract_unverified_provider_reference_routes_keep_origins_and_values() {
    crate::builtins::load_builtins();
    let mut engine = Engine::new(
        TestWorkbook::new().with_function(Arc::new(ReferenceProvider)),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    engine.configure_operation_diagnostics(OperationDiagnosticOptions {
        track_unverified_providers: true,
        ..Default::default()
    });
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(17.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUM(LOCAL_REFERENCE())").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(17.0))
    );
    let cell = crate::CellRef::new_absolute(0, 0, 2);
    let interpreter = crate::interpreter::Interpreter::new_with_cell(&engine, "Sheet1", cell);
    interpreter
        .evaluate_ast_as_reference(&parse("=LOCAL_REFERENCE()").unwrap())
        .unwrap();
    let report = engine.operation_diagnostics().unwrap().unwrap();
    assert_eq!(report.logical_root_cell_count, 2, "{report:?}");
    assert!(report.roots.iter().all(|root| matches!(
        root.operation.as_ref(), UnavailableOperation::UnverifiedProvider { name }
        if name == "LOCAL_REFERENCE"
    )));
    let columns: Vec<_> = report
        .roots
        .iter()
        .map(|root| root.cell.unwrap().coord.col())
        .collect();
    assert_eq!(columns, vec![1, 2]);
}
