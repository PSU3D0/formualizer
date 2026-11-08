use super::common::arrow_eval_config;
use crate::{
    args::ArgSchema,
    engine::{Engine, EvalConfig},
    function::{FamilyKernel, FnCaps, Function},
    operation_diagnostics::{OperationDiagnosticOptions, UnavailableOperation},
    test_workbook::TestWorkbook,
    traits::{ArgumentHandle, CalcValue, FunctionContext},
};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::{Arc, LazyLock};

struct KernelProvider {
    name: &'static str,
    reporting: bool,
    limitation: bool,
}
impl Function for KernelProvider {
    fn name(&self) -> &'static str {
        self.name
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn reports_known_limitations(&self) -> bool {
        self.reporting
    }
    fn family_kernel(&self) -> Option<FamilyKernel> {
        Some(match self.name {
            "ABS" => FamilyKernel::Abs,
            "SUM" => FamilyKernel::Sum,
            "COUNTIF" => FamilyKernel::CriteriaAggregate,
            _ => unreachable!(),
        })
    }
    fn min_args(&self) -> usize {
        if self.name == "COUNTIF" { 2 } else { 1 }
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        static ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        static TWO: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::any(), ArgSchema::any()]);
        if self.name == "COUNTIF" { &TWO } else { &ONE }
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if self.limitation {
            ctx.report_unavailable_operation(UnavailableOperation::UnverifiedFunctionMode {
                name: self.name.into(),
                mode: "declared_provider_mode",
            });
        }
        Ok(CalcValue::Scalar(LiteralValue::Number(
            if self.name == "COUNTIF" { 2.0 } else { 1.0 },
        )))
    }
}
fn kernel_case(
    name: &'static str,
    reporting: bool,
    limitation: bool,
    tracking: bool,
    active: bool,
    lift: bool,
    kernels: bool,
) -> crate::operation_diagnostics::OperationDiagnostics {
    crate::builtins::load_builtins();
    let mut engine = Engine::new(
        TestWorkbook::new().with_function(Arc::new(KernelProvider {
            name,
            reporting,
            limitation,
        })),
        EvalConfig {
            enable_parallel: false,
            family_lift: lift,
            family_kernels: kernels,
            ..arrow_eval_config()
        },
    );
    engine.configure_operation_diagnostics(OperationDiagnosticOptions {
        track_unverified_providers: tracking,
        sample_limit: 0,
        ..Default::default()
    });
    for row in 1..=8 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))
            .unwrap();
        let call = if name == "COUNTIF" {
            format!("COUNTIF($A$1:$A$2,A{row})")
        } else {
            format!("{name}(A{row})")
        };
        let formula = if active {
            format!("={call}")
        } else {
            format!("=IF(TRUE,7,{call})")
        };
        engine
            .set_cell_formula("Sheet1", row, 2, parse(&formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    for row in 1..=8 {
        let expected = if !active {
            7.0
        } else if name == "COUNTIF" {
            2.0
        } else {
            1.0
        };
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(expected))
        );
    }
    engine.operation_diagnostics().unwrap().unwrap()
}
#[test]
fn recalc_contract_provider_uncertainty_survives_optimized_kernels() {
    let mut failures = Vec::new();
    for name in ["ABS", "SUM", "COUNTIF"] {
        for (lift, kernels) in [(false, false), (true, false), (false, true), (true, true)] {
            let report = kernel_case(name, false, false, true, true, lift, kernels);
            if report.logical_root_cell_count != 8 {
                failures.push(format!("{name} lift={lift} kernels={kernels}: {report:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
#[test]
fn recalc_contract_provider_kernel_reporting_and_inactive_controls() {
    for name in ["ABS", "SUM", "COUNTIF"] {
        for (reporting, active) in [(true, true), (false, false)] {
            let report = kernel_case(name, reporting, false, true, active, true, true);
            assert!(
                report.roots.is_empty(),
                "{name} reporting={reporting} active={active}: {report:?}"
            );
        }
    }
}

struct Nested {
    name: &'static str,
}
impl Function for Nested {
    fn name(&self) -> &'static str {
        self.name
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let _ = args[0].value()?;
        Ok(CalcValue::Scalar(LiteralValue::Number(1.0)))
    }
}
#[test]
fn recalc_contract_provider_outer_uncertainty_is_independent_of_descendants() {
    let mut failures = Vec::new();
    for text in [
        "=EVIDENCE_OUTER(EVIDENCE_INNER(A1))",
        "=EVIDENCE_OUTER(IFERROR(CELL(\"format\",A1),1))",
    ] {
        for tree in [false, true] {
            crate::builtins::load_builtins();
            let wb = TestWorkbook::new()
                .with_function(Arc::new(Nested {
                    name: "EVIDENCE_OUTER",
                }))
                .with_function(Arc::new(Nested {
                    name: "EVIDENCE_INNER",
                }));
            let mut e = Engine::new(
                wb,
                EvalConfig {
                    enable_parallel: false,
                    ..arrow_eval_config()
                },
            );
            e.configure_operation_diagnostics(OperationDiagnosticOptions {
                track_unverified_providers: true,
                ..Default::default()
            });
            e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
                .unwrap();
            if tree {
                let interpreter = crate::interpreter::Interpreter::new_with_cell(
                    &e,
                    "Sheet1",
                    crate::CellRef::new_absolute(0, 0, 1),
                );
                assert_eq!(
                    interpreter
                        .evaluate_ast(&parse(text).unwrap())
                        .unwrap()
                        .into_literal(),
                    LiteralValue::Number(1.0)
                );
            } else {
                e.set_cell_formula("Sheet1", 1, 2, parse(text).unwrap())
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(
                    e.get_cell_value("Sheet1", 1, 2),
                    Some(LiteralValue::Number(1.0))
                );
            }
            let report = e.operation_diagnostics().unwrap().unwrap();
            let outer = report.roots.iter().any(|root| {
                matches!(root.operation.as_ref(),
                UnavailableOperation::UnverifiedProvider { name } if name=="EVIDENCE_OUTER")
            });
            if report.logical_cell_reason_count != 2 || !outer {
                failures.push(format!("{text} tree={tree}: {report:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn recalc_contract_attested_provider_mode_events_survive_kernels_without_uncertainty_tracking() {
    let mut failures = Vec::new();
    for name in ["ABS", "SUM", "COUNTIF"] {
        for tracking in [false, true] {
            let report = kernel_case(name, true, true, tracking, true, true, true);
            if report.logical_root_cell_count != 8 {
                failures.push(format!("{name} tracking={tracking}: {report:?}"));
            }
            assert!(
                report.roots.iter().all(|root| !matches!(
                    root.operation.as_ref(),
                    UnavailableOperation::UnverifiedProvider { .. }
                )),
                "explicit contract must not become unverified provider"
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn recalc_contract_provider_kernel_evidence_preserves_builtin_and_no_collector_lifts() {
    for builtin in [false, true] {
        for evidence in [false, true] {
            crate::builtins::load_builtins();
            let workbook = if builtin {
                TestWorkbook::new()
            } else {
                TestWorkbook::new().with_function(Arc::new(KernelProvider {
                    name: "ABS",
                    reporting: true,
                    limitation: false,
                }))
            };
            let mut engine = Engine::new(
                workbook,
                EvalConfig {
                    enable_parallel: false,
                    family_lift: true,
                    family_kernels: false,
                    ..arrow_eval_config()
                },
            );
            if evidence {
                engine.configure_operation_diagnostics(OperationDiagnosticOptions::default());
            }
            for row in 1..=8 {
                engine
                    .set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))
                    .unwrap();
                engine
                    .set_cell_formula("Sheet1", row, 2, parse(format!("=ABS(A{row})")).unwrap())
                    .unwrap();
            }
            engine.evaluate_all().unwrap();
            let lifted = engine.lifted_members_for_test();
            if evidence && !builtin {
                assert_eq!(lifted, 0);
            } else {
                assert!(
                    lifted > 0,
                    "builtin={builtin} evidence={evidence}: no actual lift"
                );
            }
            for row in 1..=8 {
                assert_eq!(
                    engine.get_cell_value("Sheet1", row, 2),
                    Some(LiteralValue::Number(1.0))
                );
            }
            if evidence {
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
    }
}
