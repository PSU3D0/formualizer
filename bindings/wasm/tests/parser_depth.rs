#![cfg(target_arch = "wasm32")]

use formualizer_wasm::{ASTNode, Parser, Workbook, parse};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

fn expected_error(shape: &str) -> &'static str {
    if matches!(shape, "power" | "arithmetic" | "postfix") {
        "Formula AST height limit exceeded"
    } else {
        "Formula nesting too deep (max 72)"
    }
}

fn accepted_formulas() -> [(&'static str, String); 6] {
    [
        (
            "parentheses",
            format!("={}1{}", "(".repeat(64), ")".repeat(64)),
        ),
        ("sum", format!("={}1{}", "SUM(".repeat(64), ")".repeat(64))),
        ("flat-height", format!("={}A1", "A1+".repeat(255))),
        ("postfix-height", format!("=1{}", "%".repeat(255))),
        ("power-height", format!("={}1", "1^".repeat(255))),
        (
            "if",
            format!("={}1{}", "IF(A1>0,".repeat(64), ",0)".repeat(64)),
        ),
    ]
}

fn hostile_formulas() -> [(&'static str, String); 9] {
    [
        (
            "parentheses",
            format!("={}1{}", "(".repeat(1000), ")".repeat(1000)),
        ),
        ("unary", format!("={}1", "-".repeat(1000))),
        (
            "sum",
            format!("={}1{}", "SUM(".repeat(1000), ")".repeat(1000)),
        ),
        (
            "right-infix",
            format!("={}1{}", "1+(".repeat(1000), ")".repeat(1000)),
        ),
        (
            "if",
            format!("={}1{}", "IF(A1>0,".repeat(1000), ",0)".repeat(1000)),
        ),
        (
            "arrays",
            format!("={}1{}", "{".repeat(1000), "}".repeat(1000)),
        ),
        ("power", format!("={}1", "1^".repeat(1000))),
        ("arithmetic", format!("={}1", "1+".repeat(1000))),
        ("postfix", format!("=1{}", "%".repeat(1000))),
    ]
}

fn assert_depth_error(result: Result<ASTNode, JsValue>, shape: &str) {
    let error = match result {
        Ok(_) => panic!("deep formula must return a parser error"),
        Err(error) => error,
    };
    let message = error
        .as_string()
        .expect("parser errors are thrown as strings");
    assert!(
        message.contains("Parser error: ") && message.contains(expected_error(shape)),
        "unexpected parser error: {message}"
    );
}

#[wasm_bindgen_test]
fn test_top_level_parse_depth_boundary_and_rejections() {
    for (shape, formula) in accepted_formulas() {
        let ast = parse(&formula, None)
            .unwrap_or_else(|error| panic!("{shape} boundary should parse: {error:?}"));
        assert!(ast.to_json().unwrap().is_object());
        assert!(!ast.to_string().is_empty());
        drop(ast);
    }

    for (shape, formula) in hostile_formulas() {
        let result = parse(&formula, None);
        assert_depth_error(result, shape);
    }

    let ast = parse("=A1+1", None).expect("normal parse after depth errors");
    assert_eq!(ast.get_type(), "binaryOp");
    drop(ast);
}

#[wasm_bindgen_test]
fn test_stateful_parser_depth_errors_and_fresh_parser_success() {
    for (shape, formula) in accepted_formulas() {
        let mut parser = Parser::new(&formula, None)
            .unwrap_or_else(|error| panic!("{shape} boundary should tokenize: {error:?}"));
        let ast = parser
            .parse()
            .unwrap_or_else(|error| panic!("{shape} boundary should parse: {error:?}"));
        assert!(ast.to_json().unwrap().is_object());
        assert!(!ast.to_string().is_empty());
        drop(ast);
        drop(parser);
    }

    for (shape, formula) in hostile_formulas() {
        let mut parser = Parser::new(&formula, None)
            .unwrap_or_else(|error| panic!("{shape} tokenizer should accept formula: {error:?}"));
        assert_depth_error(parser.parse(), shape);
        drop(parser);
    }

    let mut parser = Parser::new("=SUM(A1:A2)", None).expect("fresh parser construction");
    let ast = parser.parse().expect("fresh parser succeeds after errors");
    assert!(ast.to_json().unwrap().is_object());
    assert!(ast.to_string().contains("Function"));
    drop(ast);
    drop(parser);
}

/// Height exercised through the workbook. Release builds must evaluate the
/// default AST-height limit (256) on the default 1 MiB wasm stack. Unoptimized
/// builds use several times more stack per level, so they check a lower height.
const WORKBOOK_HEIGHT: usize = if cfg!(debug_assertions) {
    DEBUG_HEIGHT
} else {
    256
};
const DEBUG_HEIGHT: usize = 64;

/// Formulas at the default AST-height limit must evaluate through the
/// workbook API on the default wasm stack, not only parse.
#[wasm_bindgen_test]
fn test_default_height_formulas_evaluate_through_workbook() {
    let h = WORKBOOK_HEIGHT;
    let levels = 30.min(h / 4);
    let concat = format!(
        "={}&\",\"",
        (1..=h / 2)
            .map(|i| format!("A{i}"))
            .collect::<Vec<_>>()
            .join("&\",\"&")
    );
    let cases = [
        ("add", format!("={}A1", "A1+".repeat(h - 1)), Some(h as f64)),
        ("power", format!("={}1", "1^".repeat(h - 1)), Some(1.0)),
        ("concat", concat, None),
        (
            "nested",
            format!(
                "={}{}A1{}",
                "1+(".repeat(levels),
                "A1+".repeat(h - levels - 1),
                ")".repeat(levels)
            ),
            Some(h as f64),
        ),
    ];
    let wb = Workbook::new(None).unwrap();
    wb.add_sheet("Sheet1".to_string()).unwrap();
    wb.set_value("Sheet1".to_string(), 1, 1, JsValue::from_f64(1.0))
        .unwrap();
    for (col, (shape, formula, _)) in cases.iter().enumerate() {
        let col = col as u32 + 3;
        let ast = parse(formula, None).unwrap_or_else(|e| panic!("{shape}: {e:?}"));
        drop(ast);
        if h == 256 {
            assert!(
                parse(&format!("{formula}&1"), None).is_err(),
                "{shape} must sit at the height limit"
            );
        }
        wb.set_formula("Sheet1".to_string(), 1, col, formula.clone())
            .unwrap_or_else(|e| panic!("{shape}: {e:?}"));
    }
    wb.evaluate_all().unwrap();
    for (col, (shape, _, expected)) in cases.iter().enumerate() {
        let value = wb
            .evaluate_cell("Sheet1".to_string(), 1, col as u32 + 3)
            .unwrap_or_else(|e| panic!("{shape}: {e:?}"));
        match expected {
            Some(n) => assert_eq!(value.as_f64(), Some(*n), "{shape}"),
            None => assert!(
                value.as_string().is_some_and(|s| s.starts_with("1,")),
                "{shape}: {value:?}"
            ),
        }
    }
}
