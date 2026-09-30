use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;

fn check(formula: &str, expected: LiteralValue) {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                formula_plane_mode: mode,
                ..EvalConfig::default()
            },
        );
        for (i, text) in [
            "a*",
            "a~*",
            "apple",
            "a?",
            "a~?",
            "a~",
            "a~~",
            "a.b[+](c)^$",
            "axabQcdZ",
            "axabQcd",
            "a_%literal",
            "axyliteral",
        ]
        .iter()
        .enumerate()
        {
            engine
                .set_cell_value(
                    "Sheet1",
                    i as u32 + 1,
                    6,
                    LiteralValue::Text((*text).into()),
                )
                .unwrap();
            engine
                .set_cell_value(
                    "Sheet1",
                    i as u32 + 1,
                    7,
                    LiteralValue::Number(10_f64.powi(i as i32 + 1)),
                )
                .unwrap();
        }
        engine
            .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        let actual = engine.get_cell_value("Sheet1", 1, 1).unwrap();
        match (&actual, &expected) {
            (LiteralValue::Number(a), LiteralValue::Int(b)) => {
                assert_eq!(*a, *b as f64, "{mode:?}: {formula}")
            }
            _ => assert_eq!(actual, expected, "{mode:?}: {formula}"),
        }
    }
}

#[test]
fn wildcard_escapes_weighted_sumif_range() {
    check("=SUMIF(F1:F3,\"a~*\",G1:G3)", LiteralValue::Int(10));
}

#[test]
fn wildcard_escapes_countif_range() {
    check("=COUNTIF(F1:F3,\"a~*\")", LiteralValue::Int(1));
    // Distinguish equal counts by including only the escaped spelling as a decoy.
    check("=COUNTIF(F2:F3,\"a~*\")", LiteralValue::Int(0));
}

#[test]
fn wildcard_escapes_criteria_matrix() {
    for (pattern, sum) in [
        ("a~*", 10),
        ("a~?", 10000),
        ("a~~", 1000000),
        ("a.b[+](c)^$", 100000000),
        ("a*b?c*d?", 1000000000),
        ("a_%*", 100000000000),
    ] {
        check(
            &format!("=SUMIF(F1:F12,\"{pattern}\",G1:G12)"),
            LiteralValue::Int(sum),
        );
        check(
            &format!("=COUNTIF(F1:F12,\"{pattern}\")"),
            LiteralValue::Int(1),
        );
    }
    check(
        "=SUMIF({\"axabQcdZ\",\"axabQcd\"},\"a*b?c*d?\",{7,70})",
        LiteralValue::Int(7),
    );
    check(
        "=COUNTIF({\"a_%literal\",\"axyliteral\"},\"a_%*\")",
        LiteralValue::Int(1),
    );
    for (pattern, sum) in [("a~*", 10), ("a~?", 1000), ("a~~", 10000)] {
        check(
            &format!(
                "=SUMIF({{\"a*\";\"a~*\";\"a?\";\"a~\";\"apple\"}},\"{pattern}\",{{10;100;1000;10000;100000}})"
            ),
            LiteralValue::Int(sum),
        );
        check(
            &format!("=COUNTIF({{\"a*\";\"a~*\";\"a?\";\"a~\";\"apple\"}},\"{pattern}\")"),
            LiteralValue::Int(1),
        );
    }
}

#[test]
fn wildcard_escapes_search_literal_star() {
    check("=SEARCH(\"~*\",\"a*b\")", LiteralValue::Int(2));
    check("=SEARCH(\"~*\",F1)", LiteralValue::Int(2));
    check("=SEARCH(\"~*\",INDEX(F1:F2,2))", LiteralValue::Int(3));
    check(
        "=SEARCH(\"~*\",INDEX({\"a*b\",\"ab*\"},1,2))",
        LiteralValue::Int(3),
    );
}

#[test]
fn wildcard_escapes_search_matrix() {
    for (pattern, hay, start, expected) in [
        ("~?", "a?b", 1, 2),
        ("~~", "a~b", 1, 2),
        ("~", "a~b", 1, 2),
        ("~x", "a~xb", 1, 2),
        ("~*", "a*b*c", 3, 4),
        ("a*b?c*d?", "--axabQcdZ!", 1, 3),
        (".[+](c)^$", "x.[+](c)^$y", 1, 2),
        ("*~?", "abc?tail", 1, 1),
        ("~*?", "é*x", 1, 2),
    ] {
        check(
            &format!("=SEARCH(\"{pattern}\",\"{hay}\",{start})"),
            LiteralValue::Int(expected),
        );
    }
    check(
        "=SEARCH(\"~*\",\"abc\")",
        LiteralValue::Error(ExcelError::new_value()),
    );
}
