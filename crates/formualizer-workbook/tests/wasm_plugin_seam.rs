use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_workbook::{CustomFnOptions, WasmFunctionSpec, Workbook, WorkbookMode};
use std::sync::Arc;

fn workbook() -> Workbook {
    Workbook::new_with_mode(WorkbookMode::Ephemeral)
}

#[test]
fn host_callback_registration_still_works() {
    let mut wb = workbook();

    wb.register_custom_function(
        "ADD_TWO",
        CustomFnOptions {
            min_args: 1,
            max_args: Some(1),
            ..Default::default()
        },
        Arc::new(
            |args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                let value = match args.first() {
                    Some(LiteralValue::Number(n)) => *n,
                    Some(LiteralValue::Int(i)) => *i as f64,
                    _ => 0.0,
                };
                Ok(LiteralValue::Number(value + 2.0))
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=ADD_TWO(40)").unwrap();
    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(42.0)
    );
}

#[cfg(not(feature = "wasm_plugins"))]
#[test]
fn register_wasm_function_requires_feature_by_default() {
    let mut wb = workbook();

    let err = wb
        .register_wasm_function(
            "WASM_ADD",
            CustomFnOptions::default(),
            WasmFunctionSpec::new("plugin://math", "eval", 1),
        )
        .expect_err("default build should gate wasm plugin registration");

    assert_eq!(err.kind, ExcelErrorKind::NImpl);
    let message = err.message.unwrap_or_default();
    assert!(message.contains("wasm_plugins"));
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn register_wasm_function_is_stubbed_when_feature_enabled() {
    let mut wb = workbook();

    let err = wb
        .register_wasm_function(
            "WASM_ADD",
            CustomFnOptions::default(),
            WasmFunctionSpec::new("plugin://math", "eval", 1),
        )
        .expect_err("phase 4 should still return a pending-runtime stub error");

    assert_eq!(err.kind, ExcelErrorKind::NImpl);
    let message = err.message.unwrap_or_default();
    assert!(message.contains("runtime integration is pending"));
}
