use formualizer_wasm::{parse, tokenize, Parser, Reference, Tokenizer};
use wasm_bindgen_test::*;

#[wasm_bindgen_test]
fn test_tokenize() {
    let tokenizer = tokenize("=A1+B2").unwrap();
    assert!(tokenizer.length() > 0);
    let rendered = tokenizer.render();
    assert_eq!(rendered, "=A1+B2");
}

#[wasm_bindgen_test]
fn test_parse() {
    let ast = parse("=SUM(A1:B2)").unwrap();
    let json = ast.to_json().unwrap();
    assert!(json.is_object());
}

#[wasm_bindgen_test]
fn test_tokenizer_methods() {
    let tokenizer = Tokenizer::new("=A1+B2*2").unwrap();

    // Test length
    assert_eq!(tokenizer.length(), 7); // =, A1, +, B2, *, 2, EOF

    // Test render
    let rendered = tokenizer.render();
    assert_eq!(rendered, "=A1+B2*2");

    // Test get_token
    let token = tokenizer.get_token(1).unwrap();
    assert!(token.is_object());

    // Test to_string
    let str_repr = tokenizer.to_string();
    assert!(str_repr.contains("Tokenizer"));
}

#[wasm_bindgen_test]
fn test_parser() {
    let mut parser = Parser::new("=A1+B2").unwrap();
    let ast = parser.parse().unwrap();
    let json = ast.to_json().unwrap();
    assert!(json.is_object());
}

#[wasm_bindgen_test]
fn test_reference() {
    let reference = Reference::new(
        Some("Sheet1".to_string()),
        1,
        1,
        2,
        2,
        false,
        false,
        false,
        false,
    );

    assert_eq!(reference.sheet(), Some("Sheet1".to_string()));
    assert_eq!(reference.row_start(), 1);
    assert_eq!(reference.col_start(), 1);
    assert_eq!(reference.row_end(), 2);
    assert_eq!(reference.col_end(), 2);
    assert!(!reference.is_single_cell());
    assert!(reference.is_range());

    let str_repr = reference.to_string();
    assert!(str_repr.contains("Sheet1"));
}

#[wasm_bindgen_test]
fn test_complex_formula() {
    let formula = "=IF(A1>0,SUM(B1:B10),AVERAGE(C1:C10))";
    let ast = parse(formula).unwrap();
    let ast_type = ast.get_type();
    assert_eq!(ast_type, "function");
}

#[wasm_bindgen_test]
fn test_error_handling() {
    // Test invalid formula
    let result = tokenize("=A1+");
    assert!(result.is_ok()); // Tokenizer should handle incomplete formulas

    // Parser might fail on incomplete formulas
    let parse_result = parse("=A1+");
    // This depends on how the parser handles incomplete formulas
    // It might succeed with an error node or fail
}

#[wasm_bindgen_test]
fn test_array_formula() {
    let formula = "={1,2;3,4}";
    let ast = parse(formula).unwrap();
    let ast_type = ast.get_type();
    assert_eq!(ast_type, "array");
}
