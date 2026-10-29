//! Actual wasm32 request-bound and canonical DTO transport controls.
use super::source_calculation::*;
use js_sys::{Object, Reflect};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;

fn record(key: &str, value: &JsValue) -> JsValue {
    let object = Object::new();
    Reflect::set(&object, &key.into(), value).unwrap();
    object.into()
}

#[wasm_bindgen_test]
fn portable_normalization_exact_depth_node_and_string_bounds() {
    // Root has depth zero. Thirty-two enclosing records admit the leaf.
    let mut depth: JsValue = true.into();
    for _ in 0..32 {
        depth = record("x", &depth);
    }
    assert!(normalize_for_test(&depth).is_ok());
    depth = record("x", &depth);
    assert!(normalize_for_test(&depth).is_err());
    let wide = Object::new();
    for i in 0..4095 {
        Reflect::set(&wide, &format!("k{i}").into(), &JsValue::TRUE).unwrap();
    }
    assert!(normalize_for_test(wide.as_ref()).is_ok());
    Reflect::set(&wide, &"oneOver".into(), &JsValue::TRUE).unwrap();
    assert!(normalize_for_test(wide.as_ref()).is_err());
    assert!(normalize_for_test(&JsValue::from_str(&"x".repeat(65536))).is_ok());
    assert!(normalize_for_test(&JsValue::from_str(&"x".repeat(65537))).is_err());
    assert!(normalize_for_test(&record(&"k".repeat(65536), &JsValue::TRUE)).is_ok());
    assert!(normalize_for_test(&record(&"k".repeat(65537), &JsValue::TRUE)).is_err());
}

#[wasm_bindgen_test]
fn canonical_diagnostics_preserve_tags_null_zero_and_exact_large_strings() {
    use formualizer::workbook::{SourceOperationDiagnostics, SourceUnavailableOperation};
    let mut report = SourceOperationDiagnostics::default();
    for (counter, expected) in [
        (None, JsValue::NULL),
        (Some(0), JsValue::from_f64(0.0)),
        (Some(u64::MAX), JsValue::from_str("18446744073709551615")),
    ] {
        report.physical_function_dispatches = counter;
        let js = js_sys::JSON::parse(&serde_json::to_string(&report).unwrap()).unwrap();
        assert!(Object::is(
            &Reflect::get(&js, &"physicalFunctionDispatches".into()).unwrap(),
            &expected
        ));
    }
    for (operation, tag) in [
        (
            SourceUnavailableOperation::UnverifiedProvider {
                name: "CUSTOM".into(),
            },
            "unverified_provider",
        ),
        (
            SourceUnavailableOperation::UnverifiedFunctionMode {
                name: "IF".into(),
                mode: "callable".into(),
            },
            "unverified_function_mode",
        ),
        (
            SourceUnavailableOperation::UnimplementedFunctionMode {
                name: "CELL".into(),
                mode: "format".into(),
            },
            "unimplemented_function_mode",
        ),
    ] {
        let js = js_sys::JSON::parse(&serde_json::to_string(&operation).unwrap()).unwrap();
        assert_eq!(
            Reflect::get(&js, &"kind".into())
                .unwrap()
                .as_string()
                .unwrap(),
            tag
        );
    }
    // No provider registration or cancellation API is added for adapter coverage.
}

#[wasm_bindgen_test]
fn preset_selected_before_overrides_and_strict_early_return_keeps_limits() {
    let request = prepare(
        &js_sys::JSON::parse(
            r#"{"profile":"workbook","limits":{"maxCells":17},"physicalTelemetry":true}"#,
        )
        .unwrap(),
    )
    .unwrap();
    let mut options = request.initial_options().unwrap();
    options.error_location_limit = 0;
    apply(&mut options, &request).unwrap();
    assert_eq!(options.error_location_limit, 0);
    assert_eq!(options.limits.max_columns, 16384);
    assert_eq!(options.limits.max_cells, 17);
    assert!(options.physical_telemetry);
    let request = prepare(
        &js_sys::JSON::parse(r#"{"limits":{"maxCells":0},"physicalTelemetry":true}"#).unwrap(),
    )
    .unwrap();
    let mut options = request.initial_options().unwrap();
    apply(&mut options, &request).unwrap();
    assert_eq!(options.limits.max_cells, 0);
    assert!(options.physical_telemetry);
    assert!(options.calculation.is_none());
}
