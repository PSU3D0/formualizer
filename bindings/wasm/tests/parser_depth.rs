#![cfg(target_arch = "wasm32")]

use formualizer_wasm::{ASTNode, Parser, parse};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

const DEPTH_ERROR: &str = "Formula nesting too deep (max 72)";

fn accepted_formulas() -> [(&'static str, String); 3] {
    [
        (
            "parentheses",
            format!("={}1{}", "(".repeat(64), ")".repeat(64)),
        ),
        ("sum", format!("={}1{}", "SUM(".repeat(64), ")".repeat(64))),
        (
            "if",
            format!("={}1{}", "IF(A1>0,".repeat(64), ",0)".repeat(64)),
        ),
    ]
}

fn hostile_formulas() -> [(&'static str, String); 7] {
    [
        (
            "parentheses",
            format!("={}1{}", "(".repeat(5000), ")".repeat(5000)),
        ),
        ("unary", format!("={}1", "-".repeat(5000))),
        (
            "sum",
            format!("={}1{}", "SUM(".repeat(5000), ")".repeat(5000)),
        ),
        (
            "right-infix",
            format!("={}1{}", "1+(".repeat(5000), ")".repeat(5000)),
        ),
        (
            "if",
            format!("={}1{}", "IF(A1>0,".repeat(5000), ",0)".repeat(5000)),
        ),
        (
            "arrays",
            format!("={}1{}", "{".repeat(5000), "}".repeat(5000)),
        ),
        ("power", format!("={}1", "1^".repeat(5000))),
    ]
}

fn assert_depth_error(result: Result<ASTNode, JsValue>) {
    let error = match result {
        Ok(_) => panic!("deep formula must return a parser error"),
        Err(error) => error,
    };
    let message = error
        .as_string()
        .expect("parser errors are thrown as strings");
    assert!(
        message.contains("Parser error: ") && message.contains(DEPTH_ERROR),
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

    for (_shape, formula) in hostile_formulas() {
        let result = parse(&formula, None);
        assert_depth_error(result);
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
        assert_depth_error(parser.parse());
        drop(parser);
    }

    let mut parser = Parser::new("=SUM(A1:A2)", None).expect("fresh parser construction");
    let ast = parser.parse().expect("fresh parser succeeds after errors");
    assert!(ast.to_json().unwrap().is_object());
    assert!(ast.to_string().contains("Function"));
    drop(ast);
    drop(parser);
}
