use super::common::arrow_eval_config;
use crate::{
    engine::{Engine, EvalConfig},
    operation_diagnostics::{OperationDiagnostics, UnavailableOperation as Op},
    test_workbook::TestWorkbook,
};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn evaluate(formula: &str) -> (Engine<TestWorkbook>, OperationDiagnostics) {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    e.enable_operation_diagnostics(32);
    for row in 1..=3 {
        for (col, value) in [
            (1, LiteralValue::Text("x".into())),
            (2, LiteralValue::Text("p".into())),
            (3, LiteralValue::Int(row as i64 * 10)),
            (4, LiteralValue::Int(row as i64 * 100)),
        ] {
            e.set_cell_value("Sheet1", row, col, value).unwrap();
        }
    }
    e.set_cell_formula("Sheet1", 1, 8, parse(formula).expect(formula))
        .unwrap();
    e.evaluate_all().unwrap();
    let report = e.take_operation_diagnostics().unwrap().unwrap();
    (e, report)
}
fn known(name: &str, mode: &'static str) -> Op {
    Op::UnimplementedFunctionMode {
        name: name.into(),
        mode,
    }
}
fn unknown(name: &str, mode: &'static str) -> Op {
    Op::UnverifiedFunctionMode {
        name: name.into(),
        mode,
    }
}
fn check(formula: &str, expected: &[Op]) {
    let (engine, r) = evaluate(formula);
    println!(
        "MODE_RECEIPT {formula} value={:?} root_cells={} reasons={} operations={:?}",
        engine.get_cell_value("Sheet1", 1, 8),
        r.logical_root_cell_count,
        r.logical_cell_reason_count,
        r.roots
            .iter()
            .map(|r| r.operation.as_ref())
            .collect::<Vec<_>>()
    );
    let mut actual: Vec<_> = r
        .roots
        .iter()
        .map(|root| {
            let cell = root.cell.expect("cell origin");
            assert_eq!((cell.coord.row(), cell.coord.col()), (0, 7));
            root.operation.as_ref().clone()
        })
        .collect();
    let mut expected = expected.to_vec();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected, "{formula}: {r:?}");
    assert_eq!(r.logical_cell_reason_count, expected.len(), "{formula}");
    assert_eq!(
        r.logical_root_cell_count,
        usize::from(!expected.is_empty()),
        "{formula}"
    );
}
fn exercised(formula: &str, expected: &[Op]) {
    check(formula, expected);
    check(
        &format!("=IFERROR({},7)", formula.trim_start_matches('=')),
        expected,
    );
    check(
        &format!("=IF(FALSE,{},7)", formula.trim_start_matches('=')),
        &[],
    );
}

#[test]
fn provider_mode_cell_and_n() {
    exercised(
        "=CELL(\"row\")",
        &[known("CELL", "implicit_active_cell_reference")],
    );
    for mode in [
        "color",
        "filename",
        "format",
        "parentheses",
        "prefix",
        "protect",
        "width",
    ] {
        exercised(&format!("=CELL(\"{mode}\",A1)"), &[known("CELL", mode)]);
    }
    for formula in ["=N({1,2})", "=N(C1:C2)"] {
        exercised(formula, &[known("N", "array_implicit_intersection")]);
        assert_eq!(
            evaluate(formula).0.get_cell_value("Sheet1", 1, 8),
            Some(LiteralValue::Number(0.0))
        );
    }
    for formula in [
        "=CELL(\"bogus\")",
        "=CELL(7)",
        "=CELL(\"row\",A1)",
        "=CELL(\"format\",42)",
        "=CELL(\"format\",1/0)",
        "=CELL(1/0)",
        "=N(3)",
        "=N(TRUE)",
        "=N(1/0)",
        "=N()",
        "=N(1,2)",
    ] {
        check(formula, &[]);
    }
}

#[test]
fn provider_mode_pivot_defaults_and_options() {
    let base = "A1:A3,B1:B3,C1:C3,\"SUM\"";
    for (tail, modes) in [
        (
            "",
            vec![
                known("PIVOTBY", "automatic_headers"),
                known("PIVOTBY", "automatic_row_totals"),
                known("PIVOTBY", "automatic_column_totals"),
            ],
        ),
        (
            ",0",
            vec![
                known("PIVOTBY", "automatic_row_totals"),
                known("PIVOTBY", "automatic_column_totals"),
            ],
        ),
        (
            ",0,-1,0,0,0",
            vec![known("PIVOTBY", "negative_row_total_depth")],
        ),
        (
            ",0,0,0,-1,0",
            vec![known("PIVOTBY", "negative_column_total_depth")],
        ),
        (
            ",0,0,2,0,0",
            vec![known("PIVOTBY", "row_sort_by_value_column")],
        ),
        (
            ",0,0,0,0,2",
            vec![unknown("PIVOTBY", "column_sort_by_value_column")],
        ),
        (",1,0,0,0,0", vec![unknown("PIVOTBY", "header_display")]),
        (
            ",0,0,0,0,0,{TRUE;FALSE;TRUE}",
            vec![known("PIVOTBY", "filter_array")],
        ),
        (",0,0,0,0,0,,2", vec![]),
        (
            ",0,0,{1;1},0,0",
            vec![unknown("PIVOTBY", "row_sort_vector")],
        ),
    ] {
        exercised(&format!("=PIVOTBY({base}{tail})"), &modes);
    }
    for tail in [
        ",0,0,0,0,0",
        ",2,1,1,1,-1",
        ",4,0,0,0,0",
        ",0,2,0,0,0",
        ",0,0,99,0,0",
        ",0,0,0,0,0,{TRUE;FALSE}",
        ",0,0,0,0,0,,5",
    ] {
        check(&format!("=PIVOTBY({base}{tail})"), &[]);
    }
}

#[test]
fn provider_mode_pivot_shapes_callable_hierarchy() {
    exercised(
        "=PIVOTBY(A1:A3,B1:C3,C1:C3,\"SUM\",0,0,0,0,0)",
        &[
            known("PIVOTBY", "multiple_column_fields"),
            unknown("PIVOTBY", "column_hierarchy"),
        ],
    );
    exercised(
        "=PIVOTBY(A1:A3,B1:B3,C1:D3,\"SUM\",0,0,0,0,0)",
        &[known("PIVOTBY", "multiple_values")],
    );
    exercised(
        "=PIVOTBY(A1:B3,B1:B3,C1:C3,\"SUM\",0,2,0,0,0)",
        &[
            unknown("PIVOTBY", "row_hierarchy"),
            unknown("PIVOTBY", "row_subtotals"),
        ],
    );
    exercised(
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,LAMBDA(x,SUM(x)),0,0,0,0,0)",
        &[known("PIVOTBY", "callable_aggregation")],
    );
    for f in [
        "=PIVOTBY(A1:A2,B1:B3,C1:D3,\"SUM\",0,0,0,0,0)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"BAD\",0,0,0,0,0)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,1/0,0,0,0,0,0)",
    ] {
        check(f, &[]);
    }
}
#[test]
fn provider_mode_groupby_value_sort() {
    exercised(
        "=GROUPBY(A1:A3,C1:D3,\"SUM\",0,0,2)",
        &[known("GROUPBY", "sort_by_value_column")],
    );
    for f in [
        "=GROUPBY(A1:A3,C1:D3,\"SUM\",0,0,1)",
        "=GROUPBY(A1:A3,C1:D3,\"SUM\",0,0,99)",
        "=GROUPBY(A1:A2,C1:D3,\"SUM\",0,0,2)",
    ] {
        check(f, &[]);
    }
}
#[test]
fn provider_mode_aggregate_validation_precedes_report() {
    for function in 12..=19 {
        for option in 0..=7 {
            let k = match function {
                18 => ",0.5", // PERCENTILE.EXC requires 0 < k < 1.
                14..=19 => ",1",
                _ => "",
            };
            exercised(
                &format!("=AGGREGATE({function},{option},C1:C3{k})"),
                &[Op::UnimplementedFunction {
                    name: "AGGREGATE".into(),
                }],
            );
        }
        for option in ["8", "-1", "0.5", "1/0"] {
            check(&format!("=AGGREGATE({function},{option},C1:C3,1)"), &[]);
        }
        if function >= 14 {
            check(&format!("=AGGREGATE({function},0,C1:C3)"), &[]);
            check(&format!("=AGGREGATE({function},0,C1:C3,)"), &[]);
        }
    }
    for f in [
        "=AGGREGATE(9,0,C1:C3)",
        "=AGGREGATE(20,0,C1:C3)",
        "=AGGREGATE(0,0,C1:C3)",
    ] {
        check(f, &[]);
    }
}
#[test]
fn provider_mode_indirect_r1c1_and_invalid() {
    for reference in [
        "R1C3",
        "R[0]C[-5]",
        "RC",
        "R1C3:R2C3",
        "Sheet1!R1C3",
        "'Sheet1'!R1C3",
    ] {
        for wrapper in ["INDIRECT", "SUM"] {
            let f = if wrapper == "SUM" {
                format!("=SUM(INDIRECT(\"{reference}\",FALSE))")
            } else {
                format!("=INDIRECT(\"{reference}\",FALSE)")
            };
            // Keep the pre-existing INDIRECT operation identity for compatibility.
            let op = if wrapper == "SUM" && (reference.contains('[') || reference == "RC") {
                unknown("INDIRECT", "r1c1_relative_origin")
            } else {
                Op::UnimplementedFunction {
                    name: "INDIRECT".into(),
                }
            };
            exercised(&f, &[op]);
        }
    }
    for reference in ["R0C0", "R99999999C1", "R[bad]C1", "A1", "MissingName"] {
        check(&format!("=INDIRECT(\"{reference}\",FALSE)"), &[]);
    }
    for f in [
        "=INDIRECT(\"C1\",TRUE)",
        "=INDIRECT(1/0,FALSE)",
        "=INDIRECT(\"R1C1\",1/0)",
    ] {
        check(f, &[]);
    }
}

#[test]
fn provider_mode_selected_matrix_continues_after_each_failure() {
    // Each entry executes independently, so baseline evidence covers every
    // selected seam even when an earlier mode is still missing.
    let cases = [
        ("=N({1,2})", known("N", "array_implicit_intersection")),
        (
            "=CELL(\"row\")",
            known("CELL", "implicit_active_cell_reference"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",,0,0,0,0)",
            unknown("PIVOTBY", "automatic_headers"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,,0,0,0)",
            known("PIVOTBY", "automatic_row_totals"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,,0)",
            known("PIVOTBY", "automatic_column_totals"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,-1,0,0,0)",
            known("PIVOTBY", "negative_row_total_depth"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,-1,0)",
            known("PIVOTBY", "negative_column_total_depth"),
        ),
        (
            "=PIVOTBY({\"a\";\"b\"},{\"p\";\"p\"},{10;5},\"SUM\",0,0,2,0,0)",
            known("PIVOTBY", "row_sort_by_value_column"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,0,2)",
            unknown("PIVOTBY", "column_sort_by_value_column"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",1,0,0,0,0)",
            unknown("PIVOTBY", "header_display"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,0,0,{TRUE;FALSE;TRUE})",
            known("PIVOTBY", "filter_array"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,LAMBDA(x,SUM(x)),0,0,0,0,0)",
            known("PIVOTBY", "callable_aggregation"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,{1;1},0,0)",
            unknown("PIVOTBY", "row_sort_vector"),
        ),
        (
            "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,0,{1;1})",
            unknown("PIVOTBY", "column_sort_vector"),
        ),
        (
            "=GROUPBY({\"a\";\"b\"},{10;5},\"SUM\",0,0,2)",
            known("GROUPBY", "sort_by_value_column"),
        ),
        (
            "=INDIRECT(\"R1C3\",FALSE)",
            Op::UnimplementedFunction {
                name: "INDIRECT".into(),
            },
        ),
        (
            "=SUM(INDIRECT(\"R[0]C[-5]\",FALSE))",
            unknown("INDIRECT", "r1c1_relative_origin"),
        ),
        (
            "=INDIRECT(\"R1:R2\",FALSE)",
            unknown("INDIRECT", "r1c1_axis_reference"),
        ),
        (
            "=INDIRECT(\"C1:C2\",FALSE)",
            unknown("INDIRECT", "r1c1_axis_reference"),
        ),
    ];
    let mut failed = Vec::new();
    for (formula, operation) in cases {
        if std::panic::catch_unwind(|| check(formula, &[operation])).is_err() {
            failed.push(formula);
        }
    }
    assert!(
        failed.is_empty(),
        "selected modes without expected receipts: {failed:?}"
    );
}

#[test]
fn provider_mode_unverified_pivot_surface_and_invalid_controls() {
    exercised(
        "=PIVOTBY(A1:B3,B1:B3,C1:C3,\"SUM\",0,0,2,0,0)",
        &[
            unknown("PIVOTBY", "row_hierarchy"),
            unknown("PIVOTBY", "row_field_sort"),
        ],
    );
    exercised(
        "=PIVOTBY(A1:A3,B1:C3,C1:C3,\"SUM\",0,0,0,0,2)",
        &[
            known("PIVOTBY", "multiple_column_fields"),
            unknown("PIVOTBY", "column_hierarchy"),
            unknown("PIVOTBY", "column_field_sort"),
        ],
    );
    exercised(
        "=PIVOTBY({\"r\";\"a\";\"a\"},{\"c\";\"p\";\"p\"},{\"v\";10;20},\"SUM\",,0,0,0,0)",
        &[known("PIVOTBY", "automatic_headers")],
    );
    exercised(
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,{\"SUM\";\"SUM\"},0,0,0,0,0)",
        &[unknown("PIVOTBY", "aggregation_vector")],
    );
    exercised(
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,LAMBDA(x,y,SUM(x)/SUM(y)),0,0,0,0,0,,2)",
        &[
            known("PIVOTBY", "callable_aggregation"),
            unknown("PIVOTBY", "relative_to"),
        ],
    );
    exercised(
        "=PIVOTBY(A1:B3,B1:C3,C1:C3,\"SUM\",0,2,0,2,0)",
        &[
            known("PIVOTBY", "multiple_column_fields"),
            unknown("PIVOTBY", "row_hierarchy"),
            unknown("PIVOTBY", "column_hierarchy"),
            unknown("PIVOTBY", "row_subtotals"),
            unknown("PIVOTBY", "column_subtotals"),
        ],
    );
    for f in [
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,LAMBDA(x,y,z,SUM(x)),0,0,0,0,0)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0.5,0,0,0,0)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,-2,0,0,0)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,{1;2},0,0)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,0,0,{TRUE;FALSE;1/0})",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,0,0,{1;0;1})",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,0,0,,1/0)",
        "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,2.5)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,{1/0},0,0,0,0,0)",
        "=PIVOTBY(A1:A3,B1:B3,C1:C3,{1,2;3,4},0,0,0,0,0)",
        "=AGGREGATE(14,0,C1:C3,)",
    ] {
        check(f, &[]);
    }
    // No data returns before shape reporting, as the original guard did.
    check("=PIVOTBY(A1:A1,B1:C1,C1:D1,\"SUM\",3,0,0,0,0)", &[]);
}

#[test]
fn provider_mode_reporting_preserves_legacy_values() {
    let numeric = |f: &str, expected| {
        assert_eq!(
            evaluate(f).0.get_cell_value("Sheet1", 1, 8),
            Some(LiteralValue::Number(expected)),
            "{f}"
        )
    };
    numeric("=IFERROR(CELL(\"format\",A1),7)", 7.0);
    numeric("=IFERROR(CELL(\"row\"),7)", 7.0);
    numeric("=IFERROR(N({1,2}),7)", 0.0);
    numeric(
        "=IFERROR(PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,{1;1},0,0),7)",
        7.0,
    );
    let (omitted, _) = evaluate("=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",,0,0,0,0)");
    assert_eq!(
        omitted.get_cell_value("Sheet1", 1, 9),
        Some(LiteralValue::Number(60.0))
    );
    let (absent, _) = evaluate("=PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\")");
    assert_eq!(
        absent.get_cell_value("Sheet1", 2, 9),
        Some(LiteralValue::Number(50.0))
    );
    numeric(
        "=IFERROR(PIVOTBY(A1:A3,B1:B3,C1:C3,\"SUM\",0,0,0,0,0,{TRUE;FALSE;TRUE}),7)",
        7.0,
    );
    numeric(
        "=IFERROR(PIVOTBY(A1:A3,B1:B3,C1:C3,LAMBDA(x,SUM(x)),0,0,0,0,0),7)",
        7.0,
    );
    let (e, _) = evaluate("=PIVOTBY({\"a\";\"b\"},{\"p\";\"p\"},{10;5},\"SUM\",0,0,2,0,0)");
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 8),
        Some(LiteralValue::Text("a".into()))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 9),
        Some(LiteralValue::Number(10.0))
    );
    for (f, kind) in [
        (
            "=INDIRECT(\"R1C3\",FALSE)",
            formualizer_common::ExcelErrorKind::Ref,
        ),
        (
            "=INDIRECT(\"R1C3:R2C3\",FALSE)",
            formualizer_common::ExcelErrorKind::Ref,
        ),
        (
            "=AGGREGATE(12,8,C1:C3)",
            formualizer_common::ExcelErrorKind::Value,
        ),
        (
            "=AGGREGATE(14,0,C1:C3)",
            formualizer_common::ExcelErrorKind::Value,
        ),
    ] {
        assert!(
            matches!(evaluate(f).0.get_cell_value("Sheet1",1,8),Some(LiteralValue::Error(e)) if e.kind==kind),
            "{f}"
        );
    }
}

#[test]
fn provider_mode_names_and_tables_are_resolved_before_dialect() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::{CellRef, Coord, RangeRef};
    for f in [
        "=INDIRECT(\"RealName\",FALSE)",
        "=SUM(INDIRECT(\"RealName\",FALSE))",
        "=SUM(INDIRECT(\"Sales[Amount]\",FALSE))",
    ] {
        let (mut e, _) = evaluate("=1");
        let sheet = e.sheet_id("Sheet1").unwrap();
        e.define_name(
            "RealName",
            NamedDefinition::Cell(CellRef::new(sheet, Coord::from_excel(1, 3, true, true))),
            NameScope::Workbook,
        )
        .unwrap();
        e.define_table(
            "Sales",
            RangeRef::new(
                CellRef::new(sheet, Coord::from_excel(1, 3, true, true)),
                CellRef::new(sheet, Coord::from_excel(3, 3, true, true)),
            ),
            true,
            vec!["Amount".into()],
            false,
        )
        .unwrap();
        e.set_cell_formula("Sheet1", 1, 8, parse(f).unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        let r = e.take_operation_diagnostics().unwrap().unwrap();
        assert_eq!(r.logical_cell_reason_count, 0, "{f}: {r:?}");
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 8),
            Some(LiteralValue::Number(if f.contains("Sales") {
                50.0
            } else {
                10.0
            }))
        );
    }
}

#[test]
fn provider_mode_prefix_and_local_override_follow_provider() {
    crate::builtins::load_builtins();
    crate::function_registry::register_alias("", "FORM303_CELL_ALIAS", "", "CELL");
    check(
        "=FORM303_CELL_ALIAS(\"format\",A1)",
        &[known("CELL", "format")],
    );
    check("=STDEV({1;2})", &[]);
    check("=_xlfn.CELL(\"format\",A1)", &[known("CELL", "format")]);
    check(
        "=_xlfn.N({1,2})",
        &[known("N", "array_implicit_intersection")],
    );
    use crate::{
        function::Function,
        traits::{ArgumentHandle, CalcValue, FunctionContext},
    };
    struct Override(&'static str);
    impl Function for Override {
        fn name(&self) -> &'static str {
            self.0
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _: &'c [ArgumentHandle<'a, 'b>],
            _: &dyn FunctionContext<'b>,
        ) -> Result<CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(CalcValue::Scalar(LiteralValue::Number(777.0)))
        }
    }
    for (name, formula) in [
        ("CELL", "=CELL(\"row\")"),
        ("N", "=N({1,2})"),
        ("PIVOTBY", "=PIVOTBY({1},{1},{1},\"SUM\")"),
        ("GROUPBY", "=GROUPBY({1},{1},\"SUM\",0,0,2)"),
        ("INDIRECT", "=INDIRECT(\"R1C1\",FALSE)"),
        ("AGGREGATE", "=AGGREGATE(12,0,{1})"),
    ] {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(Override(name)));
        let mut e = Engine::new(wb, arrow_eval_config());
        e.enable_operation_diagnostics(10);
        e.set_cell_formula("Sheet1", 1, 8, parse(formula).unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        let r = e.take_operation_diagnostics().unwrap().unwrap();
        assert_eq!(r.logical_cell_reason_count, 0, "{formula}: {r:?}");
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 8),
            Some(LiteralValue::Number(777.0))
        );
    }
}

#[test]
fn provider_mode_review_groupby_selected_modes() {
    let mut failures = Vec::new();
    for (formula, modes) in [
        (
            "=GROUPBY(A1:A3,C1:C3,LAMBDA(x,SUM(x)),0,0,0)",
            vec![unknown("GROUPBY", "callable_aggregation")],
        ),
        (
            "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,0,{TRUE;FALSE;TRUE})",
            vec![unknown("GROUPBY", "filter_array")],
        ),
        (
            "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,{1;1})",
            vec![unknown("GROUPBY", "sort_vector")],
        ),
        (
            "=GROUPBY({\"a\";\"b\"},{10;5},\"SUM\")",
            vec![
                unknown("GROUPBY", "automatic_headers"),
                unknown("GROUPBY", "automatic_totals"),
            ],
        ),
        (
            "=GROUPBY({\"a\";\"b\"},{10;5},\"SUM\",,0,0)",
            vec![unknown("GROUPBY", "automatic_headers")],
        ),
        (
            "=GROUPBY(A1:A3,C1:C3,\"SUM\",1,0,0)",
            vec![unknown("GROUPBY", "header_display")],
        ),
        (
            "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,-1,0)",
            vec![unknown("GROUPBY", "negative_total_depth")],
        ),
        (
            "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,2,0)",
            vec![
                unknown("GROUPBY", "row_hierarchy"),
                unknown("GROUPBY", "subtotals"),
            ],
        ),
        (
            "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,0,2)",
            vec![
                unknown("GROUPBY", "row_hierarchy"),
                unknown("GROUPBY", "field_sort"),
            ],
        ),
    ] {
        if std::panic::catch_unwind(|| exercised(formula, &modes)).is_err() {
            failures.push(formula);
        }
    }
    assert!(
        failures.is_empty(),
        "GROUPBY selections without expected receipts: {failures:?}"
    );
}

#[test]
fn provider_mode_review_groupby_controls() {
    for formula in [
        "=GROUPBY(A1:A3,C1:D3,\"SUM\",0,0,0)",
        "=GROUPBY(A1:A3,C1:C3,\"BAD\",0,0,0)",
        "=GROUPBY(A1:A3,C1:C3,1/0,0,0,0)",
        "=GROUPBY(A1:A3,C1:C3,LAMBDA(x,y,SUM(x)),0,0,0)",
        "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,99,0)",
        "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,99)",
        "=GROUPBY(A1:A2,C1:C3,\"SUM\",0,0,0,{TRUE;FALSE;TRUE})",
        "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,0,{TRUE;FALSE})",
        "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,0,{TRUE;FALSE;1/0})",
        "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,{1;99})",
        "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,0,,1,0)",
    ] {
        check(formula, &[]);
    }
    let (e, _) = evaluate("=GROUPBY(A1:A3,C1:D3,\"SUM\",0,0,0)");
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 9),
        Some(LiteralValue::Number(60.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 10),
        Some(LiteralValue::Number(600.0))
    );
}

#[test]
fn provider_mode_review_aggregate_domains_and_errors() {
    let mut failures = Vec::new();
    for formula in [
        "=AGGREGATE(14,0,C2:C3,0)",
        "=AGGREGATE(14,0,C2:C3,-1)",
        "=AGGREGATE(14,0,C2:C3,3)",
        "=AGGREGATE(14,0,C2:C3,1/0)",
        "=AGGREGATE(14,0,C2:C3,\"bad\")",
        "=AGGREGATE(15,0,C2:C3,3)",
        "=AGGREGATE(16,0,C2:C3,2)",
        "=AGGREGATE(16,0,C2:C3,-0.1)",
        "=AGGREGATE(17,0,C2:C3,5)",
        "=AGGREGATE(18,0,C2:C3,0)",
        "=AGGREGATE(18,0,C2:C3,1)",
        "=AGGREGATE(18,0,C2:C3,0.1)",
        "=AGGREGATE(19,0,C2:C3,5)",
        "=AGGREGATE(19,0,C2:C3,0)",
        "=AGGREGATE(19,0,C2:C3,1)",
        "=AGGREGATE(12,0,1/0)",
        "=AGGREGATE(13,0,1/0)",
        "=AGGREGATE(14,0,{1;1/0},1)",
        "=AGGREGATE(14,0,C2:C3,1,2)",
    ] {
        if std::panic::catch_unwind(|| exercised(formula, &[])).is_err() {
            failures.push(formula);
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
    for formula in [
        "=AGGREGATE(14,0,C2:C3,1)",
        "=AGGREGATE(16,0,C2:C3,0.5)",
        "=AGGREGATE(17,0,C2:C3,1)",
        "=AGGREGATE(18,0,C2:C3,0.5)",
        "=AGGREGATE(19,0,C1:C3,2)",
        "=AGGREGATE(14,6,{1;1/0;2},1)",
    ] {
        exercised(
            formula,
            &[Op::UnimplementedFunction {
                name: "AGGREGATE".into(),
            }],
        );
    }
}

#[test]
fn provider_mode_review_indirect_reference_arity() {
    for formula in [
        "=INDIRECT(\"R1C1\",FALSE,1)",
        "=SUM(INDIRECT(\"R1C1\",FALSE,1))",
        "=ISREF(INDIRECT(\"R1C1\",FALSE,1))",
        "=SUM(INDIRECT())",
    ] {
        exercised(formula, &[]);
    }
}

#[test]
fn provider_mode_review_aggregate_uncertainty_and_error_kinds() {
    for formula in ["=AGGREGATE(12,0,\"5\")", "=AGGREGATE(14,0,C1:C3,{1,2})"] {
        exercised(formula, &[unknown("AGGREGATE", "data_or_k_domain")]);
    }
    for (formula, kind) in [
        (
            "=AGGREGATE(12,0,1/0)",
            formualizer_common::ExcelErrorKind::Div,
        ),
        (
            "=AGGREGATE(14,0,C1:C3,1/0)",
            formualizer_common::ExcelErrorKind::Div,
        ),
        (
            "=AGGREGATE(14,0,{1;1/0},1)",
            formualizer_common::ExcelErrorKind::Div,
        ),
        (
            "=AGGREGATE(14,0,C2:C3,3)",
            formualizer_common::ExcelErrorKind::Num,
        ),
        (
            "=AGGREGATE(14,0,C2:C3,\"bad\")",
            formualizer_common::ExcelErrorKind::Value,
        ),
        (
            "=GROUPBY(A1:A3,C1:C3,1/0,0,0,0)",
            formualizer_common::ExcelErrorKind::Div,
        ),
    ] {
        let (e, r) = evaluate(formula);
        assert_eq!(r.logical_cell_reason_count, 0, "{formula}: {r:?}");
        assert!(
            matches!(e.get_cell_value("Sheet1",1,8),Some(LiteralValue::Error(error)) if error.kind==kind),
            "{formula}: {:?}",
            e.get_cell_value("Sheet1", 1, 8)
        );
    }
}

#[test]
fn provider_mode_callable_consumer_actual_selections() {
    let mut failures = Vec::new();
    for (function, prefix, suffix) in [
        ("PIVOTBY", "A1:A3,B1:B3,C1:C3,", ",0,0,0,0,0"),
        ("GROUPBY", "A1:A3,C1:C3,", ",0,0,0"),
    ] {
        for (aggregation, operations) in [
            (
                "SUM",
                vec![unknown(function, "function_reference_aggregation")],
            ),
            (
                "_xlfn.SUM",
                vec![unknown(function, "function_reference_aggregation")],
            ),
            (
                "HSTACK(SUM,AVERAGE)",
                vec![unknown("HSTACK", "function_reference_array")],
            ),
            (
                "VSTACK(LAMBDA(x,SUM(x)),LAMBDA(x,MAX(x)))",
                vec![unknown("VSTACK", "callable_array_materialization")],
            ),
        ] {
            let formula = format!("={function}({prefix}{aggregation}{suffix})");
            if std::panic::catch_unwind(|| exercised(&formula, &operations)).is_err() {
                failures.push(formula);
            }
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}

#[test]
fn provider_mode_callable_consumer_ordinary_controls() {
    for (function, prefix, suffix) in [
        ("PIVOTBY", "A1:A3,B1:B3,C1:C3,", ",0,0,0,0,0"),
        ("GROUPBY", "A1:A3,C1:C3,", ",0,0,0"),
    ] {
        for aggregation in [
            "MISSING_AGG_NAME",
            "1/0",
            "HSTACK(MISSING_AGG_NAME,1)",
            "HSTACK(1/0,1)",
        ] {
            exercised(&format!("={function}({prefix}{aggregation}{suffix})"), &[]);
        }
        check(&format!("=LET(SUM,9,{function}({prefix}SUM{suffix}))"), &[]);
        check(
            &format!("=LET(SUM,1/0,{function}({prefix}SUM{suffix}))"),
            &[],
        );
        check(
            &format!("=LET(SUM,\"SUM\",{function}({prefix}SUM{suffix}))"),
            &[],
        );
    }
}

#[test]
fn provider_mode_callable_consumer_real_name_error_shadowing() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    for function in ["PIVOTBY", "GROUPBY"] {
        for value in [
            LiteralValue::Int(9),
            LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Name,
            )),
            LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Div,
            )),
        ] {
            let (mut e, _) = evaluate("=1");
            e.define_name("SUM", NamedDefinition::Literal(value), NameScope::Workbook)
                .unwrap();
            let formula = if function == "PIVOTBY" {
                "=PIVOTBY(A1:A3,B1:B3,C1:C3,SUM,0,0,0,0,0)"
            } else {
                "=GROUPBY(A1:A3,C1:C3,SUM,0,0,0)"
            };
            e.set_cell_formula("Sheet1", 1, 8, parse(formula).unwrap())
                .unwrap();
            e.evaluate_all().unwrap();
            let report = e.take_operation_diagnostics().unwrap().unwrap();
            assert_eq!(report.logical_cell_reason_count, 0, "{formula}: {report:?}");
        }
    }
}

#[test]
fn provider_mode_groupby_field_relationship() {
    for relationship in [0, 1] {
        exercised(
            &format!("=GROUPBY(A1:B3,C1:C3,\"SUM\",0,0,0,,{relationship})"),
            &[unknown("GROUPBY", "field_relationship")],
        );
    }
    exercised(
        "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,2,0,,1)",
        &[
            unknown("GROUPBY", "field_relationship"),
            unknown("GROUPBY", "table_subtotals"),
        ],
    );
    for relationship in [0, 1] {
        check(
            &format!("=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,0,,{relationship})"),
            &[],
        );
    }
    for formula in [
        "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,0,0,,2)",
        "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,0,0,,-1)",
        "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,0,0,,0.5)",
        "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,0,0,,1/0)",
        "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,0,0,,1,0)",
        "=GROUPBY(A1:B3,C1:C3,SUM,0,0,0,,1,0)",
        "=GROUPBY(A1:B3,C1:C3,GAP_ARITY_SHOULD_NOT_RUN(),0,0,0,,1,0)",
        "=GROUPBY(A1:B3,C1:C3,\"SUM\",0,99,0,,1)",
    ] {
        exercised(formula, &[]);
    }
}

#[test]
fn provider_mode_callable_consumer_retains_legacy_values() {
    for (function, prefix, suffix) in [
        ("PIVOTBY", "A1:A3,B1:B3,C1:C3,", ",0,0,0,0,0"),
        ("GROUPBY", "A1:A3,C1:C3,", ",0,0,0"),
    ] {
        for (aggregation, kind) in [
            ("SUM", formualizer_common::ExcelErrorKind::Name),
            (
                "HSTACK(SUM,AVERAGE)",
                formualizer_common::ExcelErrorKind::Name,
            ),
            (
                "VSTACK(LAMBDA(x,SUM(x)),LAMBDA(x,MAX(x)))",
                formualizer_common::ExcelErrorKind::Calc,
            ),
        ] {
            let formula = format!("={function}({prefix}{aggregation}{suffix})");
            let (e, _) = evaluate(&formula);
            assert!(
                matches!(e.get_cell_value("Sheet1",1,8),Some(LiteralValue::Error(error)) if error.kind==kind),
                "{formula}: {:?}",
                e.get_cell_value("Sheet1", 1, 8)
            );
            let (caught, _) = evaluate(&format!("=IFERROR({},7)", formula.trim_start_matches('=')));
            assert_eq!(
                caught.get_cell_value("Sheet1", 1, 8),
                Some(LiteralValue::Number(7.0))
            );
        }
    }
    for relationship in [0, 1] {
        let (e, r) = evaluate(&format!(
            "=GROUPBY(A1:A3,C1:C3,\"SUM\",0,0,0,,{relationship})"
        ));
        assert_eq!(r.logical_cell_reason_count, 0);
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 9),
            Some(LiteralValue::Number(60.0))
        );
    }
}

fn producer_receipt(
    formula: &str,
    engine: &Engine<TestWorkbook>,
    report: &OperationDiagnostics,
    expected: &[Op],
) {
    producer_value_receipt(
        formula,
        engine.get_cell_value("Sheet1", 1, 8),
        report,
        expected,
    );
}
fn producer_value_receipt(
    formula: &str,
    value: Option<LiteralValue>,
    report: &OperationDiagnostics,
    expected: &[Op],
) {
    println!(
        "EXPRESSION_PRODUCER_RECEIPT {formula} value={:?} root_cells={} reasons={} operations={:?}",
        value,
        report.logical_root_cell_count,
        report.logical_cell_reason_count,
        report
            .roots
            .iter()
            .map(|root| root.operation.as_ref())
            .collect::<Vec<_>>()
    );
    let mut actual: Vec<_> = report
        .roots
        .iter()
        .map(|root| {
            let cell = root.cell.expect("producer cell");
            assert_eq!((cell.coord.row(), cell.coord.col()), (0, 7));
            root.operation.as_ref().clone()
        })
        .collect();
    let mut expected = expected.to_vec();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected, "{formula}: {report:?}");
    assert_eq!(
        report.logical_cell_reason_count,
        expected.len(),
        "{formula}"
    );
    assert_eq!(
        report.logical_root_cell_count,
        usize::from(!expected.is_empty()),
        "{formula}"
    );
}
fn producer_check(formula: &str, expected: &[Op]) {
    let (e, r) = evaluate(formula);
    producer_receipt(formula, &e, &r, expected);
}
fn producer_exercised(formula: &str, expected: &[Op]) {
    producer_check(formula, expected);
    producer_check(
        &format!("=IFERROR({},7)", formula.trim_start_matches('=')),
        expected,
    );
    producer_check(
        &format!("=IF(FALSE,{},7)", formula.trim_start_matches('=')),
        &[],
    );
}
fn if_producer() -> Op {
    unknown("IF", "function_reference_selected_value")
}
fn let_producer() -> Op {
    unknown("LET", "function_reference_binding")
}

#[test]
fn provider_mode_expression_producer_selected_matrix() {
    let mut failures = Vec::new();
    for (formula, operation) in [
        (
            "=GROUPBY(A1:A3,C1:C3,IF(TRUE,SUM,AVERAGE),0,0,0)",
            if_producer(),
        ),
        (
            "=GROUPBY(A1:A3,C1:C3,HSTACK(IF(TRUE,SUM,AVERAGE)),0,0,0)",
            if_producer(),
        ),
        (
            "=LET(f,SUM,GROUPBY(A1:A3,C1:C3,HSTACK(f),0,0,0))",
            let_producer(),
        ),
        ("=IF(TRUE,SUM,AVERAGE)", if_producer()),
        ("=IF(FALSE,7,AVERAGE)", if_producer()),
        ("=SUM(IF(TRUE,SUM,A1))", if_producer()),
        ("=ISREF(IF(TRUE,SUM,A1))", if_producer()),
        ("=LET(f,SUM,7)", let_producer()),
        (
            "=LET(f,SUM,IF(FALSE,GROUPBY(A1:A3,C1:C3,HSTACK(f),0,0,0),7))",
            let_producer(),
        ),
        ("=IF({TRUE;TRUE},SUM,7)", if_producer()),
        ("=IF({FALSE;FALSE},7,AVERAGE)", if_producer()),
        ("=IF({TRUE;FALSE},SUM,AVERAGE)", if_producer()),
        ("=IF({TRUE;FALSE},7,SUM)", if_producer()),
    ] {
        if std::panic::catch_unwind(|| producer_exercised(formula, &[operation])).is_err() {
            failures.push(formula);
        }
    }
    assert!(
        failures.is_empty(),
        "missing actual producer receipts: {failures:?}"
    );
}

#[test]
fn provider_mode_expression_producer_neutral_controls() {
    for formula in [
        "=IF(FALSE,SUM,7)",
        "=IF(TRUE,7,AVERAGE)",
        "=IF(1/0,SUM,AVERAGE)",
        "=IF(TRUE,1/0,AVERAGE)",
        "=IF(TRUE,MISSING_FUNCTION_VALUE,7)",
        "=IF(FALSE,LET(f,SUM,7),7)",
        "=IF(TRUE,7,LET(f,SUM,7))",
        "=IF({FALSE;FALSE},SUM,7)",
        "=IF({TRUE;TRUE},7,AVERAGE)",
        "=IF({1/0;1/0},SUM,AVERAGE)",
        "=IF(TRUE,SUM,AVERAGE,7)",
        "=LET(7,0,later,SUM,7)",
        "=LET(SUM,9,f,SUM,f)",
        "=LET(SUM,1/0,f,SUM,f)",
        "=LET(SUM,\"SUM\",f,SUM,f)",
        "=LET(SUM,9,LET(f,SUM,f))",
        "=LET(first,9,SUM,first,f,SUM,f)",
    ] {
        producer_exercised(formula, &[]);
    }
}

#[test]
fn provider_mode_expression_producer_raw_callable_preserved() {
    for formula in [
        "=IF(TRUE,LAMBDA(x,x),7)",
        "=IF(FALSE,7,LAMBDA(x,x))",
        "=LET(f,LAMBDA(x,x),7)",
        "=LET(f,IF(TRUE,LAMBDA(x,x),7),7)",
        "=LET(f,LAMBDA(x,x),f(2))",
    ] {
        producer_exercised(formula, &[]);
    }
    producer_exercised(
        "=GROUPBY(A1:A3,C1:C3,IF(TRUE,LAMBDA(x,SUM(x)),7),0,0,0)",
        &[unknown("GROUPBY", "callable_aggregation")],
    );
    producer_exercised(
        "=LET(f,IF(TRUE,LAMBDA(x,x),7),HSTACK(f,f))",
        &[unknown("HSTACK", "callable_array_materialization")],
    );
}

#[test]
fn provider_mode_expression_producer_real_names_and_errors() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    for sheet_scope in [false, true] {
        for value in [
            LiteralValue::Int(9),
            LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Name,
            )),
            LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Div,
            )),
            LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Calc,
            )),
            LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Value,
            )),
            LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::NImpl,
            )),
        ] {
            for formula in ["=IF(TRUE,SUM,7)", "=SUM(IF(TRUE,SUM,A1))", "=LET(f,SUM,f)"] {
                let (mut e, _) = evaluate("=1");
                let scope = if sheet_scope {
                    NameScope::Sheet(e.sheet_id("Sheet1").unwrap())
                } else {
                    NameScope::Workbook
                };
                e.define_name("SUM", NamedDefinition::Literal(value.clone()), scope)
                    .unwrap();
                e.set_cell_formula("Sheet1", 1, 8, parse(formula).unwrap())
                    .unwrap();
                e.evaluate_all().unwrap();
                let r = e.take_operation_diagnostics().unwrap().unwrap();
                producer_receipt(formula, &e, &r, &[]);
            }
        }
    }
}

#[test]
fn provider_mode_expression_producer_identity_no_invocation_and_failed_binding() {
    use crate::{
        function::Function,
        traits::{ArgumentHandle, CalcValue, FunctionContext},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Callback(Arc<AtomicUsize>);
    impl Function for Callback {
        fn name(&self) -> &'static str {
            "PRODUCER_CALLBACK"
        }
        fn aliases(&self) -> &'static [&'static str] {
            &["PRODUCER_CALLBACK_ALIAS"]
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _: &'c [ArgumentHandle<'a, 'b>],
            _: &dyn FunctionContext<'b>,
        ) -> Result<CalcValue<'b>, formualizer_common::ExcelError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("classification must never invoke a callback")
        }
    }
    struct FailedBinding;
    impl Function for FailedBinding {
        fn name(&self) -> &'static str {
            "PRODUCER_FAIL_BINDING"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _: &'c [ArgumentHandle<'a, 'b>],
            _: &dyn FunctionContext<'b>,
        ) -> Result<CalcValue<'b>, formualizer_common::ExcelError> {
            Err(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Div,
            ))
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new()
        .with_function(Arc::new(Callback(calls.clone())))
        .with_function(Arc::new(FailedBinding));
    let mut e = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    e.enable_operation_diagnostics(32);
    let mut failures = Vec::new();
    for (formula, expected) in [
        ("=IF(TRUE,PRODUCER_CALLBACK,7)", vec![if_producer()]),
        ("=IF(FALSE,7,PRODUCER_CALLBACK_ALIAS)", vec![if_producer()]),
        ("=LET(f,PRODUCER_CALLBACK_ALIAS,7)", vec![let_producer()]),
        ("=IF(FALSE,PRODUCER_CALLBACK_ALIAS,7)", vec![]),
        ("=IF(FALSE,LET(f,PRODUCER_CALLBACK,7),7)", vec![]),
        ("=LET(first,PRODUCER_FAIL_BINDING(),later,SUM,7)", vec![]),
    ] {
        e.set_cell_formula("Sheet1", 1, 8, parse(formula).unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        let r = e.take_operation_diagnostics().unwrap().unwrap();
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            producer_receipt(formula, &e, &r, &expected)
        }))
        .is_err()
        {
            failures.push(formula);
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(failures.is_empty(), "{failures:?}");
}

#[test]
fn provider_mode_expression_producer_tree_arena_reference_agree() {
    for (formula, operation) in [
        ("=IFERROR(IF(TRUE,SUM,AVERAGE),7)", if_producer()),
        ("=IFERROR(SUM(IF(TRUE,SUM,A1)),7)", if_producer()),
        ("=IFERROR(ISREF(IF(TRUE,SUM,A1)),7)", if_producer()),
        (
            "=IFERROR(LET(f,SUM,GROUPBY(A1:A3,C1:C3,HSTACK(f),0,0,0)),7)",
            let_producer(),
        ),
    ] {
        let (arena, arena_report) = evaluate(formula);
        let arena_value = arena.get_cell_value("Sheet1", 1, 8).unwrap();
        producer_receipt(
            formula,
            &arena,
            &arena_report,
            std::slice::from_ref(&operation),
        );
        // A fresh engine has an active collector; evaluate() intentionally
        // finalizes its collector when returning a request receipt.
        let mut tree = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                enable_parallel: false,
                ..arrow_eval_config()
            },
        );
        tree.enable_operation_diagnostics(32);
        for row in 1..=3 {
            for (col, value) in [
                (1, LiteralValue::Text("x".into())),
                (2, LiteralValue::Text("p".into())),
                (3, LiteralValue::Int(row as i64 * 10)),
            ] {
                tree.set_cell_value("Sheet1", row, col, value).unwrap();
            }
        }
        tree.set_cell_formula("Sheet1", 1, 8, parse("=1").unwrap())
            .unwrap();
        let cell = crate::CellRef::new(
            tree.sheet_id("Sheet1").unwrap(),
            crate::Coord::from_excel(1, 8, true, true),
        );
        let interp = crate::interpreter::Interpreter::new_with_cell(&tree, "Sheet1", cell);
        let tree_value = interp
            .evaluate_ast(&parse(formula).unwrap())
            .unwrap()
            .into_literal();
        assert_eq!(tree_value, arena_value, "{formula}");
        let tree_report = tree.operation_diagnostics().unwrap().unwrap();
        producer_value_receipt(formula, Some(tree_value), &tree_report, &[operation]);
    }
}
