//! Parsed engine dispatch coverage for TEXT; values pin existing behavior.
use crate::{
    engine::{Engine, EvalConfig},
    operation_diagnostics::{OperationDiagnosticOptions, UnavailableOperation},
    test_workbook::TestWorkbook,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn text(s: &str) -> LiteralValue {
    LiteralValue::Text(s.into())
}
fn error(kind: ExcelErrorKind) -> LiteralValue {
    LiteralValue::Error(ExcelError::new(kind))
}

fn check(formula: &str, expected: LiteralValue, mode: Option<&'static str>) {
    crate::builtins::load_builtins();
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            family_lift: false,
            family_kernels: false,
            ..super::common::arrow_eval_config()
        },
    );
    engine.configure_operation_diagnostics(OperationDiagnosticOptions::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let actual = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    match (&actual, &expected) {
        (LiteralValue::Error(a), LiteralValue::Error(b)) => assert_eq!(a.kind, b.kind, "{formula}"),
        _ => assert_eq!(actual, expected, "{formula}"),
    }
    let report = engine.operation_diagnostics().unwrap().unwrap();
    let count = usize::from(mode.is_some());
    assert_eq!(
        report.logical_cell_reason_count, count,
        "{formula}: {report:?}"
    );
    assert_eq!(
        report.logical_root_cell_count, count,
        "{formula}: {report:?}"
    );
    assert_eq!(report.roots.len(), count, "{formula}: {report:?}");
    if let Some(mode) = mode {
        let root = &report.roots[0];
        assert_eq!(root.cell.unwrap().coord.row(), 0);
        assert_eq!(root.cell.unwrap().coord.col(), 0);
        assert_eq!(
            root.operation.as_ref(),
            &UnavailableOperation::UnverifiedFunctionMode {
                name: "TEXT".into(),
                mode
            }
        );
    }
}

#[test]
fn text_coverage_scientific() {
    check(
        r#"=TEXT(1234,"0.00E+00")"#,
        text("1234.00"),
        Some("final_format_heuristic"),
    );
}
#[test]
fn text_coverage_fraction() {
    check(
        r##"=TEXT(1.5,"# ?/?")"##,
        text("1.5"),
        Some("final_format_heuristic"),
    );
}
#[test]
fn text_coverage_conditional() {
    check(
        r#"=TEXT(5,"[>10]0;""small""")"#,
        text("5"),
        Some("final_format_heuristic"),
    );
}
#[test]
fn text_coverage_conditional_routing_negative() {
    check(
        r#"=TEXT(-45306,"[<0]""negative"";dddd")"#,
        text("Monday"),
        Some("conditional_routing"),
    );
}

#[test]
fn text_coverage_conditional_routing_zero() {
    check(
        r#"=TEXT(0,"[=0]""zero"";dddd;yyyy")"#,
        text("1900"),
        Some("conditional_routing"),
    );
}

#[test]
fn text_coverage_conditional_routing_controls() {
    for (formula, expected) in [
        (r#"=TEXT(45306,"dddd")"#, "Monday"),
        (r#"=TEXT(-45306,"yyyy;dddd")"#, "Monday"),
        (r#"=TEXT(0,"dddd;dddd;yyyy")"#, "1900"),
        (r#"=TEXT(-45306,"""[<0]negative"";dddd")"#, "Monday"),
        (r#"=TEXT(-45306,"\[<0]""negative"";dddd")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;""[=0]zero""")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;\[=0]""zero""")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;[$-407]dddd")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;[Color0]0")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;*x0")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;_x0")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;e")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;dddd;dddd;@")"#, "Monday"),
        (r#"=TEXT(45306,"dddd;dddd;dddd;[=0]@")"#, "Monday"),
    ] {
        check(formula, text(expected), None);
    }
    check(
        r#"=IF(FALSE,TEXT(-45306,"[<0]""negative"";dddd"),7)"#,
        LiteralValue::Number(7.0),
        None,
    );
    check(
        r#"=IFERROR(TEXT(-45306,"[<0]""negative"";dddd"),7)"#,
        text("Monday"),
        Some("conditional_routing"),
    );
    check(
        r#"=TEXT(45306,"dddd;[>0]""positive""")"#,
        text("Monday"),
        Some("conditional_routing"),
    );
    check(
        r#"=IFERROR(TEXT(0,"[=0]""zero"";dddd;yyyy"),7)"#,
        text("1900"),
        Some("conditional_routing"),
    );
}

#[test]
fn text_coverage_conditional_errors_direct() {
    check(
        r#"=TEXT(-2958466,"[<0]""negative"";yyyy")"#,
        error(ExcelErrorKind::Value),
        Some("conditional_routing"),
    );
}

#[test]
fn text_coverage_conditional_errors_caught() {
    check(
        r#"=IFERROR(TEXT(-2958466,"[<0]""negative"";yyyy"),7)"#,
        LiteralValue::Number(7.0),
        Some("conditional_routing"),
    );
}

#[test]
fn text_coverage_conditional_errors_controls() {
    // A selected section's tokenization failure is provisional too.
    check(
        r#"=TEXT(-45306,"[<0]""negative"";[Color0]yyyy")"#,
        error(ExcelErrorKind::Value),
        Some("conditional_routing"),
    );
    // The rendered-length failure is another selected-section early exit.
    check(
        r#"=TEXT(-45306,"[<0]""negative"";"&CHAR(34)&REPT("x",256)&CHAR(34)&"yyyy")"#,
        error(ExcelErrorKind::Value),
        Some("conditional_routing"),
    );
    for formula in [
        r#"=TEXT(2958466,"yyyy")"#,
        r#"=TEXT(-1,"yyyy")"#,
        r#"=TEXT(-45306,"yyyy;[Color0]yyyy")"#,
        r#"=TEXT(-2958466,"""[<0]negative"";yyyy")"#,
        r#"=TEXT(-2958466,"\[<0]""negative"";yyyy")"#,
        // Whole-format Invalid remains established before section choice.
        r#"=TEXT(-2958466,"[<0]""negative"";yyyy"&CHAR(34))"#,
        r#"=TEXT(-2958466,"[<0]""negative"";yyyy"&CHAR(92))"#,
        r#"=TEXT(-2958466,"[<0]""negative"";yyyy;yyyy;@;0")"#,
    ] {
        check(formula, error(ExcelErrorKind::Value), None);
    }
    check(
        r#"=TEXT(-45306,"[<0]""negative"";dddd")"#,
        text("Monday"),
        Some("conditional_routing"),
    );
    check(
        r#"=IF(FALSE,TEXT(-2958466,"[<0]""negative"";yyyy"),7)"#,
        LiteralValue::Number(7.0),
        None,
    );
    check(
        r#"=IFERROR(TEXT(2958466,"yyyy"),7)"#,
        LiteralValue::Number(7.0),
        None,
    );
    check(
        r#"=TEXT(1/0,"[<0]""negative"";yyyy")"#,
        error(ExcelErrorKind::Div),
        None,
    );
}

#[test]
fn text_coverage_final_heuristics() {
    for (formula, expected) in [
        (r#"=TEXT(5,"0;0;0;@")"#, "5"),
        (r#"=TEXT(5,"[Red]")"#, "5"),
        (r#"=TEXT(5,"?")"#, "5"),
        (r#"=TEXT(5,"*x0")"#, "5"),
        (r#"=TEXT(5,"_x0")"#, "5"),
        (r#"=TEXT(5,"e")"#, "5"),
        (r#"=TEXT(5,"[$-F800]")"#, "5"),
        (r#"=TEXT(5,"General")"#, "5"),
    ] {
        check(formula, text(expected), Some("final_format_heuristic"));
    }
}

#[test]
fn text_coverage_locale_parser_uncertainty() {
    check(
        r#"=TEXT(45306,"[$-407]dddd")"#,
        text("Monday"),
        Some("locale_format"),
    );
    check(
        r#"=TEXT(45306,"""[$-407]""dddd")"#,
        text("[$-407]Monday"),
        None,
    );
    // Only the selected date section determines locale uncertainty.
    check(r#"=TEXT(45306,"dddd;[$-407]dddd")"#, text("Monday"), None);
    check(
        r#"=IF(FALSE,TEXT(45306,"[$-407]dddd"),7)"#,
        LiteralValue::Number(7.0),
        None,
    );
    check(
        r#"=IFERROR(TEXT(45306,"[$-407]dddd"),7)"#,
        text("Monday"),
        Some("locale_format"),
    );
}

#[test]
fn text_coverage_input_coercion() {
    for input in ["$5", "1/2", "1.234,56", "garbage7"] {
        check(
            &format!("=TEXT(\"{input}\",\"00\")"),
            error(ExcelErrorKind::Value),
            Some("input_coercion"),
        );
    }
    check(
        r#"=TEXT("abc","0;0;0;@")"#,
        text("abc"),
        Some("final_format_heuristic"),
    );
}

#[test]
fn text_coverage_established_controls() {
    for (formula, expected) in [
        (r#"=TEXT(12.3,"0.00")"#, "12.30"),
        (r#"=TEXT(5,"0"" kg""")"#, "5 kg"),
        (r#"=TEXT(45306,"dddd")"#, "Monday"),
        (r#"=TEXT(1.5,"[h]:mm")"#, "36:00"),
        (r#"=TEXT("12.3","0.00")"#, "12.30"),
        (r#"=TEXT("4/19/2001","yyyy")"#, "2001"),
        (r#"=TEXT(TRUE,"0")"#, "1"),
        (r#"=TEXT(Z1,"0")"#, "0"),
        (r#"=TEXT("abc","00")"#, "abc"),
        (r#"=TEXT(5,"")"#, ""),
        (r#"=TEXT(5,"""0.00E+00""")"#, "0.00E+00"),
        (r#"=TEXT(5,"[Red]0")"#, "5"),
    ] {
        check(formula, text(expected), None);
    }
}

#[test]
fn text_coverage_invalid_propagated_and_inactive() {
    for formula in [
        r#"=TEXT(5,"0 kg")"#,
        r#"=TEXT(5,"[Color0]0")"#,
        r#"=TEXT(5,"0"&CHAR(34))"#,
        r#"=TEXT(5,"0"&CHAR(92))"#,
    ] {
        check(formula, error(ExcelErrorKind::Value), None);
    }
    check(r#"=TEXT(1/0,"0.00E+00")"#, error(ExcelErrorKind::Div), None);
    check(r#"=TEXT(5,1/0)"#, error(ExcelErrorKind::Div), None);
    check(
        r#"=IF(FALSE,TEXT(5,"0.00E+00"),7)"#,
        LiteralValue::Number(7.0),
        None,
    );
    check(
        r#"=IFERROR(TEXT(1/0,"0.00E+00"),7)"#,
        LiteralValue::Number(7.0),
        None,
    );
    check(
        r#"=IFERROR(TEXT("$5","00"),7)"#,
        LiteralValue::Number(7.0),
        Some("input_coercion"),
    );
    check(
        r#"=IFERROR(TEXT(1234,"0.00E+00"),7)"#,
        text("1234.00"),
        Some("final_format_heuristic"),
    );
}

#[test]
fn text_coverage_array_scalar_selection() {
    check(r#"=TEXT({12.3;5},"0.00")"#, text("12.30"), None);
    check(
        r#"=TEXT({1234;5},"0.00E+00")"#,
        text("1234.00"),
        Some("final_format_heuristic"),
    );
}
