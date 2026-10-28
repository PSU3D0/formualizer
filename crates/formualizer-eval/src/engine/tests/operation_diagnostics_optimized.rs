use super::common::arrow_eval_config;
use crate::{
    args::ArgSchema,
    engine::{Engine, EvalConfig},
    function::{FamilyKernel, FnCaps, Function},
    operation_diagnostics::UnavailableOperation,
    test_workbook::TestWorkbook,
    traits::{ArgumentHandle, CalcValue, FunctionContext},
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::{Arc, LazyLock};

struct Provider {
    name: &'static str,
    unavailable: bool,
    explicit: bool,
}
impl Function for Provider {
    fn name(&self) -> &'static str {
        self.name
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn min_args(&self) -> usize {
        1
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
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let _ = args[0].value()?;
        if self.unavailable {
            if self.explicit {
                ctx.report_unavailable_operation(UnavailableOperation::UnimplementedFunction {
                    name: self.name.into(),
                });
                return Ok(CalcValue::Scalar(LiteralValue::Number(1.0)));
            }
            return Err(ExcelError::new(ExcelErrorKind::NImpl));
        }
        Ok(CalcValue::Scalar(LiteralValue::Number(1.0)))
    }
}

fn repeated(
    formula: &str,
    name: &'static str,
    unavailable: bool,
    explicit: bool,
    lift: bool,
    kernels: bool,
    limit: usize,
    parallel: bool,
) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let wb = TestWorkbook::new().with_function(Arc::new(Provider {
        name,
        unavailable,
        explicit,
    }));
    let mut e = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: parallel,
            family_lift: lift,
            family_kernels: kernels,
            ..arrow_eval_config()
        },
    );
    e.enable_operation_diagnostics(limit);
    for row in 1..=8 {
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))
            .unwrap();
        e.set_cell_formula(
            "Sheet1",
            row,
            2,
            parse(formula.replace('@', &row.to_string())).unwrap(),
        )
        .unwrap();
    }
    for row in 1..=2 {
        e.set_cell_value("Sheet1", row, 26, LiteralValue::Number(row as f64))
            .unwrap();
    }
    e.evaluate_all().unwrap();
    e
}

#[test]
fn recalc_contract_diagnostics_pure_active_inactive_lift_and_binding() {
    for (lift, kernels) in [(false, false), (true, false), (false, true)] {
        for active in [false, true] {
            for explicit in [false, true] {
                for limit in [0, 1, 3] {
                    let formula = if active {
                        "=IF(A@>0,ABS($Z$1),7)"
                    } else {
                        "=IF(A@>0,7,ABS($Z$1))"
                    };
                    let e = repeated(formula, "ABS", true, explicit, lift, kernels, limit, false);
                    let r = e.operation_diagnostics().unwrap();
                    assert_eq!(
                        r.logical_cell_reason_count,
                        if active { 8 } else { 0 },
                        "{formula} lift={lift} kernels={kernels} explicit={explicit}: {r:?}"
                    );
                    assert_eq!(r.samples.len(), if active { limit } else { 0 });
                    if lift || kernels {
                        assert!(
                            r.physical_unavailable_observations > 0,
                            "speculation must have occurred"
                        );
                    }
                    for row in 1..=8 {
                        let value = e.get_cell_value("Sheet1", row, 2).unwrap();
                        if active && !explicit {
                            assert!(
                                matches!(value, LiteralValue::Error(e) if e.kind == ExcelErrorKind::NImpl)
                            );
                        } else {
                            assert_eq!(value, LiteralValue::Number(if active { 1.0 } else { 7.0 }));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn recalc_contract_diagnostics_criteria_exception_and_inactive_control() {
    for formula in [
        "=COUNTIF($Z$1:$Z$2,ABS(A@))",
        "=COUNTIF($Z$1:$Z$2,IF(A@>0,1,ABS($Z$1)))",
    ] {
        let e = repeated(formula, "ABS", true, true, false, true, 3, false);
        let r = e.operation_diagnostics().unwrap();
        assert_eq!(
            r.logical_cell_reason_count,
            if formula.contains(",IF(") { 0 } else { 8 },
            "{formula}: {r:?}"
        );
    }
}

#[test]
fn recalc_contract_diagnostics_unaffected_optimization_retained() {
    let lift = repeated(
        "=IF(A@>0,ABS($Z$1),7)",
        "ABS",
        false,
        false,
        true,
        false,
        3,
        false,
    );
    assert!(lift.lifted_members_for_test() > 0);
    assert_eq!(
        lift.operation_diagnostics()
            .unwrap()
            .physical_unavailable_observations,
        0
    );
    let bound = repeated(
        "=IF(A@>0,ABS($Z$1),7)",
        "ABS",
        false,
        false,
        false,
        true,
        3,
        false,
    );
    assert!(bound.invariant_bound_members_for_test() > 0);
    let criteria = repeated(
        "=COUNTIF($Z$1:$Z$2,ABS(A@))",
        "ABS",
        false,
        false,
        false,
        true,
        3,
        false,
    );
    assert!(criteria.criteria_kernel_members_for_test() > 0);
    let memo = repeated(
        "=MATCH(A@,$Z$1:$Z$2,0)",
        "MATCH",
        false,
        false,
        false,
        true,
        3,
        false,
    );
    assert!(memo.memo_hits_for_test() > 0);
    assert_eq!(
        memo.operation_diagnostics()
            .unwrap()
            .logical_cell_reason_count,
        0
    );
}

#[test]
fn recalc_contract_diagnostics_memo_result_and_key_evidence() {
    for parallel in [false, true] {
        let e = repeated(
            "=MATCH(A@,$Z$1:$Z$2,0)",
            "MATCH",
            true,
            true,
            false,
            true,
            3,
            parallel,
        );
        let r = e.operation_diagnostics().unwrap();
        assert_eq!(r.logical_cell_reason_count, 8, "{r:?}");
        assert_eq!(
            r.physical_unavailable_observations, 8,
            "debug oracles cannot add observations"
        );
        assert_eq!(
            e.memo_hits_for_test(),
            0,
            "exceptional results cannot erase events on reuse"
        );
        let e = repeated(
            "=MATCH(ABS(A@),$Z$1:$Z$2,0)",
            "ABS",
            true,
            true,
            false,
            true,
            3,
            parallel,
        );
        assert_eq!(
            e.operation_diagnostics().unwrap().logical_cell_reason_count,
            8
        );
    }
}
