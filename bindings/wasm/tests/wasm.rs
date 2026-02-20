#![cfg(target_arch = "wasm32")]

use formualizer_wasm::{
    FormulaDialect, Parser, Reference, SheetPortSession, Tokenizer, Workbook, parse, tokenize,
};
use js_sys::{Function, Object, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::*;

fn js_get(obj: &js_sys::Object, key: &str) -> JsValue {
    js_sys::Reflect::get(obj, &JsValue::from_str(key)).unwrap()
}

fn js_get_string(obj: &js_sys::Object, key: &str) -> String {
    js_get(obj, key).as_string().unwrap()
}

fn js_get_f64(obj: &js_sys::Object, key: &str) -> f64 {
    js_get(obj, key).as_f64().unwrap()
}

fn js_get_bool(obj: &js_sys::Object, key: &str) -> bool {
    js_get(obj, key).as_bool().unwrap()
}

fn set_prop(obj: &Object, key: &str, value: JsValue) {
    Reflect::set(obj, &JsValue::from_str(key), &value).unwrap();
}

fn make_custom_fn_options(
    min_args: Option<f64>,
    max_args: Option<Option<f64>>,
    volatile: Option<bool>,
    thread_safe: Option<bool>,
    deterministic: Option<bool>,
    allow_override_builtin: Option<bool>,
) -> JsValue {
    let options = Object::new();

    if let Some(value) = min_args {
        set_prop(&options, "minArgs", JsValue::from_f64(value));
    }
    if let Some(value) = max_args {
        match value {
            Some(max) => set_prop(&options, "maxArgs", JsValue::from_f64(max)),
            None => set_prop(&options, "maxArgs", JsValue::NULL),
        }
    }
    if let Some(value) = volatile {
        set_prop(&options, "volatile", JsValue::from_bool(value));
    }
    if let Some(value) = thread_safe {
        set_prop(&options, "threadSafe", JsValue::from_bool(value));
    }
    if let Some(value) = deterministic {
        set_prop(&options, "deterministic", JsValue::from_bool(value));
    }
    if let Some(value) = allow_override_builtin {
        set_prop(&options, "allowOverrideBuiltin", JsValue::from_bool(value));
    }

    options.into()
}

fn assert_ast_reference(
    node: &js_sys::Object,
    sheet: Option<&str>,
    row_start: u32,
    col_start: u32,
    row_end: u32,
    col_end: u32,
    row_abs_start: bool,
    col_abs_start: bool,
    row_abs_end: bool,
    col_abs_end: bool,
) {
    assert_eq!(js_get_string(node, "type"), "reference");

    let ref_obj: js_sys::Object = js_get(node, "reference").dyn_into().unwrap();

    let sheet_val = js_get(&ref_obj, "sheet");
    let got_sheet = sheet_val.as_string();
    assert_eq!(got_sheet.as_deref(), sheet);

    assert_eq!(js_get_f64(&ref_obj, "rowStart") as u32, row_start);
    assert_eq!(js_get_f64(&ref_obj, "colStart") as u32, col_start);
    assert_eq!(js_get_f64(&ref_obj, "rowEnd") as u32, row_end);
    assert_eq!(js_get_f64(&ref_obj, "colEnd") as u32, col_end);

    assert_eq!(js_get_bool(&ref_obj, "rowAbsStart"), row_abs_start);
    assert_eq!(js_get_bool(&ref_obj, "colAbsStart"), col_abs_start);
    assert_eq!(js_get_bool(&ref_obj, "rowAbsEnd"), row_abs_end);
    assert_eq!(js_get_bool(&ref_obj, "colAbsEnd"), col_abs_end);
}

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
fn test_ast_reference_a1() {
    let ast = parse("=A1", None).unwrap();
    let json = ast.to_json().unwrap();
    let node: js_sys::Object = json.dyn_into().unwrap();
    assert_ast_reference(&node, None, 1, 1, 1, 1, false, false, false, false);
}

#[wasm_bindgen_test]
fn test_ast_reference_range_a1_b2() {
    let ast = parse("=SUM(A1:B2)", None).unwrap();
    let json = ast.to_json().unwrap();
    let node: js_sys::Object = json.dyn_into().unwrap();
    assert_eq!(js_get_string(&node, "type"), "function");

    let args: js_sys::Array = js_get(&node, "args").dyn_into().unwrap();
    assert!(args.length() >= 1);
    let arg0: js_sys::Object = args.get(0).dyn_into().unwrap();
    assert_ast_reference(&arg0, None, 1, 1, 2, 2, false, false, false, false);
}

#[wasm_bindgen_test]
fn test_ast_reference_sheet_qualified() {
    let ast = parse("='My Sheet'!C3", None).unwrap();
    let json = ast.to_json().unwrap();
    let node: js_sys::Object = json.dyn_into().unwrap();
    assert_ast_reference(
        &node,
        Some("My Sheet"),
        3,
        3,
        3,
        3,
        false,
        false,
        false,
        false,
    );
}

#[wasm_bindgen_test]
fn test_ast_reference_absolute() {
    let ast = parse("=$A$1", None).unwrap();
    let json = ast.to_json().unwrap();
    let node: js_sys::Object = json.dyn_into().unwrap();
    assert_ast_reference(&node, None, 1, 1, 1, 1, true, true, true, true);
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
    wb.add_sheet("Data".to_string()).unwrap();
    // Set values via workbook
    wb.set_value("Data".to_string(), 1, 1, JsValue::from_f64(1.0))
        .unwrap();
    wb.set_value("Data".to_string(), 1, 2, JsValue::from_f64(2.0))
        .unwrap();
    // Set formula
    wb.set_formula("Data".to_string(), 1, 3, "=A1+B1".to_string())
        .unwrap();

    // Workbook evaluation should work under wasm-pack test (Node).
    let v = wb.evaluate_cell("Data".to_string(), 1, 3).unwrap();
    assert_eq!(v.as_f64().unwrap(), 3.0);

    // Ensure sheet facade works
    wb.add_sheet("Sheet2".to_string()).unwrap();
    let sheet = wb.sheet("Sheet2".to_string()).unwrap();
    sheet.set_value(1, 1, JsValue::from_f64(10.0)).unwrap();
    sheet.set_formula(1, 2, "=A1*3".to_string()).unwrap();
    let formula = sheet.get_formula(1, 2).unwrap();
    assert_eq!(formula, "=A1*3");

    let v2 = sheet.evaluate_cell(1, 2).unwrap();
    assert_eq!(v2.as_f64().unwrap(), 30.0);
}

#[wasm_bindgen_test]
fn test_register_simple_function_and_evaluate() {
    let wb = Workbook::new();
    wb.add_sheet("Sheet1".to_string()).unwrap();

    let callback = Function::new_with_args("a, b", "return a + b;");
    wb.register_function(
        "js_add".to_string(),
        callback,
        Some(make_custom_fn_options(
            Some(2.0),
            Some(Some(2.0)),
            None,
            None,
            None,
            None,
        )),
    )
    .unwrap();

    wb.set_formula("Sheet1".to_string(), 1, 1, "=JS_ADD(2,3)".to_string())
        .unwrap();
    let value = wb.evaluate_cell("Sheet1".to_string(), 1, 1).unwrap();
    assert_eq!(value.as_f64().unwrap(), 5.0);
}

#[wasm_bindgen_test]
fn test_register_array_mapping_behavior() {
    let wb = Workbook::new();
    wb.add_sheet("Sheet1".to_string()).unwrap();

    wb.set_value("Sheet1".to_string(), 1, 1, JsValue::from_f64(1.0))
        .unwrap();
    wb.set_value("Sheet1".to_string(), 1, 2, JsValue::from_f64(2.0))
        .unwrap();
    wb.set_value("Sheet1".to_string(), 2, 1, JsValue::from_f64(3.0))
        .unwrap();
    wb.set_value("Sheet1".to_string(), 2, 2, JsValue::from_f64(4.0))
        .unwrap();

    let callback = Function::new_with_args(
        "grid",
        "return grid.map((row, r) => row.map((value, c) => value + (r + 1) * 10 + (c + 1)));",
    );
    wb.register_function(
        "map_grid".to_string(),
        callback,
        Some(make_custom_fn_options(
            Some(1.0),
            Some(Some(1.0)),
            None,
            None,
            None,
            None,
        )),
    )
    .unwrap();

    wb.set_formula("Sheet1".to_string(), 1, 3, "=MAP_GRID(A1:B2)".to_string())
        .unwrap();
    wb.evaluate_all().unwrap();

    let sheet = wb.sheet("Sheet1".to_string()).unwrap();
    assert_eq!(sheet.get_value(1, 3).unwrap().as_f64().unwrap(), 12.0);
    assert_eq!(sheet.get_value(1, 4).unwrap().as_f64().unwrap(), 14.0);
    assert_eq!(sheet.get_value(2, 3).unwrap().as_f64().unwrap(), 24.0);
    assert_eq!(sheet.get_value(2, 4).unwrap().as_f64().unwrap(), 26.0);
}

#[wasm_bindgen_test]
fn test_register_function_js_throw_maps_to_excel_error() {
    let wb = Workbook::new();
    wb.add_sheet("Sheet1".to_string()).unwrap();

    let callback = Function::new_with_args("x", "throw new Error('kaboom\\ninternal');");
    wb.register_function(
        "explode".to_string(),
        callback,
        Some(make_custom_fn_options(
            Some(1.0),
            Some(Some(1.0)),
            None,
            None,
            None,
            None,
        )),
    )
    .unwrap();

    wb.set_formula("Sheet1".to_string(), 1, 1, "=EXPLODE(1)".to_string())
        .unwrap();
    let value = wb.evaluate_cell("Sheet1".to_string(), 1, 1).unwrap();

    let text = value.as_string().unwrap();
    assert!(text.contains("#VALUE!"));
    if text.len() > "#VALUE!".len() {
        assert!(!text.contains('\n'));
        assert!(!text.contains('\r'));
    }
}

#[wasm_bindgen_test]
fn test_unregister_function_behavior() {
    let wb = Workbook::new();
    wb.add_sheet("Sheet1".to_string()).unwrap();

    let callback = Function::new_with_args("", "return 7;");
    wb.register_function(
        "temp_fn".to_string(),
        callback,
        Some(make_custom_fn_options(
            Some(0.0),
            Some(Some(0.0)),
            None,
            None,
            None,
            None,
        )),
    )
    .unwrap();

    wb.unregister_function("temp_fn".to_string()).unwrap();

    wb.set_formula("Sheet1".to_string(), 1, 1, "=TEMP_FN()".to_string())
        .unwrap();
    let value = wb.evaluate_cell("Sheet1".to_string(), 1, 1).unwrap();
    assert!(value.as_string().unwrap().contains("#NAME?"));
}

#[wasm_bindgen_test]
fn test_list_functions_metadata_contents() {
    let wb = Workbook::new();

    wb.register_function(
        "alpha".to_string(),
        Function::new_with_args("", "return 1;"),
        Some(make_custom_fn_options(
            Some(0.0),
            Some(Some(0.0)),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
        )),
    )
    .unwrap();

    wb.register_function(
        "beta".to_string(),
        Function::new_with_args("x", "return x;"),
        Some(make_custom_fn_options(
            Some(1.0),
            Some(None),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        )),
    )
    .unwrap();

    let list = wb.list_functions().unwrap();
    assert_eq!(list.length(), 2);

    let alpha: Object = list.get(0).dyn_into().unwrap();
    assert_eq!(js_get_string(&alpha, "name"), "ALPHA");
    assert_eq!(js_get_f64(&alpha, "minArgs"), 0.0);
    assert_eq!(js_get_f64(&alpha, "maxArgs"), 0.0);
    assert!(!js_get_bool(&alpha, "volatile"));
    assert!(js_get_bool(&alpha, "threadSafe"));
    assert!(!js_get_bool(&alpha, "deterministic"));
    assert!(!js_get_bool(&alpha, "allowOverrideBuiltin"));

    let beta: Object = list.get(1).dyn_into().unwrap();
    assert_eq!(js_get_string(&beta, "name"), "BETA");
    assert_eq!(js_get_f64(&beta, "minArgs"), 1.0);
    assert!(js_get(&beta, "maxArgs").is_null());
    assert!(js_get_bool(&beta, "volatile"));
    assert!(!js_get_bool(&beta, "threadSafe"));
    assert!(js_get_bool(&beta, "deterministic"));
    assert!(js_get_bool(&beta, "allowOverrideBuiltin"));
}

#[wasm_bindgen_test]
fn test_changelog_undo_redo() {
    let wb = Workbook::new();
    wb.add_sheet("S".to_string()).unwrap();
    wb.set_changelog_enabled(true).unwrap();
    wb.set_value("S".to_string(), 1, 1, JsValue::from_f64(10.0))
        .unwrap();
    // Change value in a second op (no explicit action needed)
    wb.set_value("S".to_string(), 1, 1, JsValue::from_f64(20.0))
        .unwrap();

    // Undo: back to 10
    wb.undo().unwrap();
    let sheet = wb.sheet("S".to_string()).unwrap();
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
    wb.add_sheet("Inputs".to_string()).unwrap();
    wb.add_sheet("Outputs".to_string()).unwrap();

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
    let sheet = wb.sheet("Inputs".to_string()).unwrap();
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
