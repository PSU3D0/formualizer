#![cfg(target_arch = "wasm32")]

use formualizer_wasm::{
    parse, tokenize, FormulaDialect, Parser, Reference, SheetPortSession, Tokenizer, Workbook,
};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::*;

#[wasm_bindgen_test]
fn test_tokenize() {
    let tokenizer = tokenize("=A1+B2", None).unwrap();
    assert!(tokenizer.length() > 0);
    let rendered = tokenizer.render();
    assert_eq!(rendered, "=A1+B2");
}

#[wasm_bindgen_test]
fn test_parse() {
    let ast = parse("=SUM(A1:B2)", None).unwrap();
    let json = ast.to_json().unwrap();
    assert!(json.is_object());
}

#[wasm_bindgen_test]
fn test_openformula_dialect() {
    let tokenizer = Tokenizer::new("=SUM([.A1];[.A2])", Some(FormulaDialect::OpenFormula)).unwrap();
    assert_eq!(tokenizer.render(), "=SUM([.A1];[.A2])");

    let ast = parse(
        "=SUM([Sheet One.A1:.B2])",
        Some(FormulaDialect::OpenFormula),
    )
    .unwrap();
    assert_eq!(ast.get_type(), "function");
}

#[wasm_bindgen_test]
fn test_tokenizer_methods() {
    let tokenizer = Tokenizer::new("=A1+B2*2", None).unwrap();

    // Test length
    assert_eq!(tokenizer.length(), 5); // A1, +, B2, *, 2

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
    let mut parser = Parser::new("=A1+B2", None).unwrap();
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
    let ast = parse(formula, None).unwrap();
    let ast_type = ast.get_type();
    assert_eq!(ast_type, "function");
}

#[wasm_bindgen_test]
fn test_error_handling() {
    // Test invalid formula
    let result = tokenize("=A1+", None);
    assert!(result.is_ok()); // Tokenizer should handle incomplete formulas

    // Parser might fail on incomplete formulas
    let _ = parse("=A1+", None);
    // This depends on how the parser handles incomplete formulas
    // It might succeed with an error node or fail
}

#[wasm_bindgen_test]
fn test_array_formula() {
    let formula = "={1,2;3,4}";
    let ast = parse(formula, None).unwrap();
    let ast_type = ast.get_type();
    assert_eq!(ast_type, "array");
}

#[wasm_bindgen_test]
fn test_workbook_sheet_eval() {
    let wb = Workbook::new();
    wb.add_sheet("Data".to_string());
    // Set values via workbook
    wb.set_value("Data".to_string(), 1, 1, JsValue::from_f64(1.0))
        .unwrap();
    wb.set_value("Data".to_string(), 1, 2, JsValue::from_f64(2.0))
        .unwrap();
    // Set formula
    wb.set_formula("Data".to_string(), 1, 3, "=A1+B1".to_string())
        .unwrap();
    // Ensure sheet facade works without triggering evaluation (Instant::now unsupported in wasm32 tests)
    wb.add_sheet("Sheet2".to_string());
    let sheet = wb.sheet("Sheet2".to_string());
    sheet.set_value(1, 1, JsValue::from_f64(10.0)).unwrap();
    sheet.set_formula(1, 2, "=A1*3".to_string()).unwrap();
    let formula = sheet.get_formula(1, 2).unwrap();
    assert_eq!(formula, "=A1*3");
}

#[wasm_bindgen_test]
fn test_changelog_undo_redo() {
    let wb = Workbook::new();
    wb.add_sheet("S".to_string());
    wb.set_changelog_enabled(true).unwrap();
    wb.set_value("S".to_string(), 1, 1, JsValue::from_f64(10.0))
        .unwrap();
    // Change value in a second op (no explicit action needed)
    wb.set_value("S".to_string(), 1, 1, JsValue::from_f64(20.0))
        .unwrap();

    // Undo: back to 10
    wb.undo().unwrap();
    let sheet = wb.sheet("S".to_string());
    let v = sheet.get_value(1, 1).unwrap();
    assert_eq!(v.as_f64().unwrap(), 10.0);

    // Redo: back to 20
    wb.redo().unwrap();
    let v2 = sheet.get_value(1, 1).unwrap();
    assert_eq!(v2.as_f64().unwrap(), 20.0);
}

const SHEETPORT_MANIFEST: &str = r#"
spec: fio
spec_version: "0.3.0"
manifest:
  id: wasm-sheetport-tests
  name: WASM SheetPort Session Tests
  workbook:
    uri: memory://wasm-sheetport.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: demand
    dir: in
    shape: scalar
    location:
      a1: Inputs!A1
    schema:
      type: number
  - id: mix
    dir: in
    shape: record
    location:
      a1: Inputs!B1:C1
    schema:
      kind: record
      fields:
        qty:
          type: integer
          location:
            a1: Inputs!B1
          constraints:
            min: 0
        label:
          type: string
          location:
            a1: Inputs!C1
    default:
      qty: 1
      label: seed
  - id: plan_output
    dir: out
    shape: scalar
    location:
      a1: Outputs!A1
    schema:
      type: number
"#;

fn build_sheetport_workbook() -> Workbook {
    let wb = Workbook::new();
    wb.add_sheet("Inputs".to_string());
    wb.add_sheet("Outputs".to_string());

    wb.set_value("Inputs".to_string(), 1, 1, JsValue::from_f64(120.0))
        .unwrap();
    wb.set_value("Inputs".to_string(), 1, 2, JsValue::from_f64(3.0))
        .unwrap();
    wb.set_value("Inputs".to_string(), 1, 3, JsValue::from_str("seed"))
        .unwrap();
    wb.set_value("Outputs".to_string(), 1, 1, JsValue::from_f64(42.0))
        .unwrap();
    wb
}

#[wasm_bindgen_test]
fn test_sheetport_session_read_write_roundtrip() {
    let wb = build_sheetport_workbook();
    let mut session =
        SheetPortSession::from_manifest_yaml(SHEETPORT_MANIFEST.to_string(), &wb).unwrap();

    let inputs = session.read_inputs().unwrap();
    let inputs_obj: js_sys::Object = inputs.into();
    let demand = js_sys::Reflect::get(&inputs_obj, &JsValue::from_str("demand"))
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(demand, 120.0);

    let mix = js_sys::Reflect::get(&inputs_obj, &JsValue::from_str("mix"))
        .unwrap()
        .dyn_into::<js_sys::Object>()
        .unwrap();
    let qty = js_sys::Reflect::get(&mix, &JsValue::from_str("qty"))
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(qty, 3.0);
    let label = js_sys::Reflect::get(&mix, &JsValue::from_str("label"))
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(label, "seed");

    let ports = session
        .describe_ports()
        .unwrap()
        .dyn_into::<js_sys::Array>()
        .unwrap();
    assert_eq!(ports.length(), 3);

    let updates = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &updates,
        &JsValue::from_str("demand"),
        &JsValue::from_f64(250.5),
    );
    let mix_update = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &mix_update,
        &JsValue::from_str("qty"),
        &JsValue::from_f64(7.0),
    );
    let _ = js_sys::Reflect::set(
        &updates,
        &JsValue::from_str("mix"),
        &JsValue::from(mix_update),
    );
    session.write_inputs(JsValue::from(updates)).unwrap();

    let refreshed = session
        .read_inputs()
        .unwrap()
        .dyn_into::<js_sys::Object>()
        .unwrap();
    let demand_after = js_sys::Reflect::get(&refreshed, &JsValue::from_str("demand"))
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(demand_after, 250.5);
    let mix_after = js_sys::Reflect::get(&refreshed, &JsValue::from_str("mix"))
        .unwrap()
        .dyn_into::<js_sys::Object>()
        .unwrap();
    let qty_after = js_sys::Reflect::get(&mix_after, &JsValue::from_str("qty"))
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(qty_after, 7.0);
    let label_after = js_sys::Reflect::get(&mix_after, &JsValue::from_str("label"))
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(label_after, "seed");

    // Workbook reflects updates
    let sheet = wb.sheet("Inputs".to_string());
    let stored = sheet.get_value(1, 1).unwrap();
    assert_eq!(stored.as_f64().unwrap(), 250.5);

    let outputs = session.evaluate_once(JsValue::UNDEFINED).unwrap();
    let outputs_obj: js_sys::Object = outputs.into();
    let plan_output = js_sys::Reflect::get(&outputs_obj, &JsValue::from_str("plan_output"))
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(plan_output, 42.0);
}

#[wasm_bindgen_test]
fn test_sheetport_session_constraint_error() {
    let wb = build_sheetport_workbook();
    let mut session =
        SheetPortSession::from_manifest_yaml(SHEETPORT_MANIFEST.to_string(), &wb).unwrap();

    let updates = js_sys::Object::new();
    let mix_update = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &mix_update,
        &JsValue::from_str("qty"),
        &JsValue::from_f64(-4.0),
    );
    let _ = js_sys::Reflect::set(
        &updates,
        &JsValue::from_str("mix"),
        &JsValue::from(mix_update),
    );

    let err = session.write_inputs(JsValue::from(updates)).unwrap_err();
    let error: js_sys::Error = err.dyn_into().unwrap();
    let kind = js_sys::Reflect::get(error.as_ref(), &JsValue::from_str("kind"))
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(kind, "ConstraintViolation");

    let violations = js_sys::Reflect::get(error.as_ref(), &JsValue::from_str("violations"))
        .unwrap()
        .dyn_into::<js_sys::Array>()
        .unwrap();
    assert!(violations.length() > 0);
}
