use formualizer_common::ExcelErrorKind;
use formualizer_workbook::{Workbook, WorkbookMode};

#[cfg(feature = "wasm_plugins")]
use formualizer_common::{ExcelError, LiteralValue};
#[cfg(feature = "wasm_plugins")]
use formualizer_workbook::{
    CustomFnOptions, WasmFunctionSpec, WasmModuleManifest, WasmRuntimeHint, WasmUdfRuntime,
};

#[cfg(feature = "wasm_plugins")]
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
use std::borrow::Cow;

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
use wasm_encoder::{
    CodeSection, CustomSection, ExportKind, ExportSection, Function, FunctionSection, Instruction,
    Module, TypeSection, ValType,
};

const VALID_MANIFEST: &str = include_str!("fixtures/wasm_manifest/valid_v1.json");
#[cfg(feature = "wasm_plugins")]
const INVALID_SCHEMA_MANIFEST: &str = include_str!("fixtures/wasm_manifest/invalid_schema.json");

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
const DIV_ONLY_MANIFEST: &str = r#"{
  "schema": "formualizer.udf.module/v1",
  "module": {
    "id": "plugin://math/div",
    "version": "1.0.0",
    "abi": 1,
    "codec": 1
  },
  "functions": [
    {
      "id": 1,
      "name": "SAFE_DIV",
      "aliases": ["DIV_SAFE"],
      "export": "fn_safe_div",
      "min_args": 2,
      "max_args": 2,
      "volatile": false,
      "deterministic": true,
      "thread_safe": true
    }
  ]
}"#;

fn workbook() -> Workbook {
    Workbook::new_with_mode(WorkbookMode::Ephemeral)
}

#[cfg(feature = "wasm_plugins")]
#[derive(Clone, Copy)]
enum FakeRuntimeMode {
    Divide,
    AlwaysError,
}

#[cfg(feature = "wasm_plugins")]
struct FakeWasmRuntime {
    mode: FakeRuntimeMode,
    calls: Arc<AtomicUsize>,
}

#[cfg(feature = "wasm_plugins")]
impl FakeWasmRuntime {
    fn new(mode: FakeRuntimeMode, calls: Arc<AtomicUsize>) -> Self {
        Self { mode, calls }
    }
}

#[cfg(feature = "wasm_plugins")]
impl WasmUdfRuntime for FakeWasmRuntime {
    fn can_bind_functions(&self) -> bool {
        true
    }

    fn validate_module(
        &self,
        _module_id: &str,
        _wasm_bytes: &[u8],
        _manifest: &WasmModuleManifest,
    ) -> Result<(), ExcelError> {
        Ok(())
    }

    fn invoke(
        &self,
        _module_id: &str,
        _export_name: &str,
        _function_name: &str,
        _codec_version: u32,
        args: &[LiteralValue],
        _runtime_hint: Option<&WasmRuntimeHint>,
    ) -> Result<LiteralValue, ExcelError> {
        self.calls.fetch_add(1, Ordering::Relaxed);

        match self.mode {
            FakeRuntimeMode::AlwaysError => {
                Err(ExcelError::new(ExcelErrorKind::Value).with_message("fake runtime failure"))
            }
            FakeRuntimeMode::Divide => {
                if args.len() != 2 {
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("expected exactly 2 args"));
                }

                let lhs = match args[0] {
                    LiteralValue::Number(n) => n,
                    LiteralValue::Int(i) => i as f64,
                    _ => {
                        return Err(ExcelError::new(ExcelErrorKind::Value)
                            .with_message("lhs must be numeric"));
                    }
                };
                let rhs = match args[1] {
                    LiteralValue::Number(n) => n,
                    LiteralValue::Int(i) => i as f64,
                    _ => {
                        return Err(ExcelError::new(ExcelErrorKind::Value)
                            .with_message("rhs must be numeric"));
                    }
                };

                if rhs == 0.0 {
                    return Err(
                        ExcelError::new(ExcelErrorKind::Div).with_message("division by zero")
                    );
                }

                Ok(LiteralValue::Number(lhs / rhs))
            }
        }
    }
}

fn push_leb_u32(out: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn wasm_module_with_manifest(manifest_json: &str) -> Vec<u8> {
    let section_name = formualizer_workbook::WASM_MANIFEST_SECTION_V1.as_bytes();
    let manifest = manifest_json.as_bytes();

    let mut section_payload = Vec::new();
    push_leb_u32(&mut section_payload, section_name.len() as u32);
    section_payload.extend_from_slice(section_name);
    section_payload.extend_from_slice(manifest);

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"\0asm");
    bytes.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]);
    bytes.push(0x00);
    push_leb_u32(&mut bytes, section_payload.len() as u32);
    bytes.extend_from_slice(&section_payload);
    bytes
}

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
fn wasm_module_with_manifest_and_div_export(manifest_json: &str) -> Vec<u8> {
    let mut module = Module::new();

    let mut types = TypeSection::new();
    types
        .ty()
        .function([ValType::F64, ValType::F64], [ValType::F64]);
    module.section(&types);

    let mut funcs = FunctionSection::new();
    funcs.function(0);
    module.section(&funcs);

    let mut exports = ExportSection::new();
    exports.export("fn_safe_div", ExportKind::Func, 0);
    module.section(&exports);

    let mut code = CodeSection::new();
    let mut function = Function::new([]);
    function.instruction(&Instruction::LocalGet(0));
    function.instruction(&Instruction::LocalGet(1));
    function.instruction(&Instruction::F64Div);
    function.instruction(&Instruction::End);
    code.function(&function);
    module.section(&code);

    module.section(&CustomSection {
        name: Cow::Borrowed(formualizer_workbook::WASM_MANIFEST_SECTION_V1),
        data: Cow::Owned(manifest_json.as_bytes().to_vec()),
    });

    module.finish()
}

#[cfg(not(feature = "wasm_plugins"))]
#[test]
fn register_wasm_module_requires_feature_by_default() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);

    let err = wb
        .register_wasm_module_bytes("plugin://finance/core", &bytes)
        .expect_err("default build should gate wasm module registration");

    assert_eq!(err.kind, ExcelErrorKind::NImpl);
    assert!(err.message.unwrap_or_default().contains("wasm_plugins"));
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn register_list_unregister_module_lifecycle() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);

    let info = wb
        .register_wasm_module_bytes("plugin://finance/core", &bytes)
        .unwrap();
    assert_eq!(info.module_id, "plugin://finance/core");
    assert_eq!(info.function_count, 2);
    assert_eq!(info.module_size_bytes, bytes.len());

    let listed = wb.list_wasm_modules();
    assert_eq!(listed, vec![info.clone()]);

    wb.unregister_wasm_module("plugin://finance/core").unwrap();
    assert!(wb.list_wasm_modules().is_empty());
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn inspect_wasm_module_is_effect_free() {
    let wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);

    let inspected = wb.inspect_wasm_module_bytes(&bytes).unwrap();
    assert_eq!(inspected.module_id, "plugin://finance/core");
    assert_eq!(inspected.function_count, 2);

    // Ensure inspection does not register/attach module.
    assert!(wb.list_wasm_modules().is_empty());
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn duplicate_module_id_is_rejected() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);

    wb.register_wasm_module_bytes("plugin://finance/core", &bytes)
        .unwrap();

    let err = wb
        .register_wasm_module_bytes("plugin://finance/core", &bytes)
        .expect_err("duplicate module id should be rejected");

    assert_eq!(err.kind, ExcelErrorKind::Name);
    assert!(
        err.message
            .unwrap_or_default()
            .contains("already registered")
    );
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn invalid_manifest_is_rejected() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(INVALID_SCHEMA_MANIFEST);

    let err = wb
        .register_wasm_module_bytes("plugin://finance/core", &bytes)
        .expect_err("invalid manifest should fail registration");

    assert_eq!(err.kind, ExcelErrorKind::Value);
    assert!(
        err.message
            .unwrap_or_default()
            .contains("Unsupported WASM manifest schema")
    );
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn requested_module_id_must_match_manifest() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);

    let err = wb
        .register_wasm_module_bytes("plugin://other", &bytes)
        .expect_err("module id mismatch should fail registration");

    assert_eq!(err.kind, ExcelErrorKind::Value);
    assert!(
        err.message
            .unwrap_or_default()
            .contains("module id mismatch")
    );
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn attach_and_bind_aliases_match_register_behavior() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);

    wb.attach_wasm_module_bytes("plugin://finance/core", &bytes)
        .unwrap();

    let err = wb
        .bind_wasm_function(
            "WASM_ADD",
            CustomFnOptions::default(),
            WasmFunctionSpec::new("plugin://finance/core", "fn_safe_div", 1),
        )
        .expect_err("pending runtime should still gate bind alias by default");

    assert_eq!(err.kind, ExcelErrorKind::NImpl);
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn register_wasm_function_validates_module_and_export_before_binding() {
    let mut wb = workbook();

    let missing_module = wb
        .register_wasm_function(
            "WASM_ADD",
            CustomFnOptions::default(),
            WasmFunctionSpec::new("plugin://finance/core", "fn_safe_div", 1),
        )
        .expect_err("missing module should fail before runtime binding");
    assert_eq!(missing_module.kind, ExcelErrorKind::Name);

    let bytes = wasm_module_with_manifest(VALID_MANIFEST);
    wb.register_wasm_module_bytes("plugin://finance/core", &bytes)
        .unwrap();

    let missing_export = wb
        .register_wasm_function(
            "WASM_ADD",
            CustomFnOptions::default(),
            WasmFunctionSpec::new("plugin://finance/core", "fn_missing", 1),
        )
        .expect_err("missing export should fail");
    assert_eq!(missing_export.kind, ExcelErrorKind::Name);

    let codec_mismatch = wb
        .register_wasm_function(
            "WASM_ADD",
            CustomFnOptions::default(),
            WasmFunctionSpec::new("plugin://finance/core", "fn_safe_div", 99),
        )
        .expect_err("codec mismatch should fail");
    assert_eq!(codec_mismatch.kind, ExcelErrorKind::NImpl);
    assert!(
        codec_mismatch
            .message
            .unwrap_or_default()
            .contains("codec mismatch")
    );
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn register_wasm_function_with_pending_runtime_returns_nimpl() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);
    wb.register_wasm_module_bytes("plugin://finance/core", &bytes)
        .unwrap();

    let pending_runtime = wb
        .register_wasm_function(
            "WASM_DIV",
            CustomFnOptions {
                min_args: 2,
                max_args: Some(2),
                ..Default::default()
            },
            WasmFunctionSpec::new("plugin://finance/core", "fn_safe_div", 1),
        )
        .expect_err("default runtime remains pending");

    assert_eq!(pending_runtime.kind, ExcelErrorKind::NImpl);
    assert!(
        pending_runtime
            .message
            .unwrap_or_default()
            .contains("runtime integration is pending")
    );
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn fake_runtime_binding_evaluates_through_wasm_runtime_abstraction() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);
    wb.register_wasm_module_bytes("plugin://finance/core", &bytes)
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    wb.set_wasm_runtime(Arc::new(FakeWasmRuntime::new(
        FakeRuntimeMode::Divide,
        calls.clone(),
    )));

    wb.register_wasm_function(
        "WASM_DIV",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        WasmFunctionSpec::new("plugin://finance/core", "fn_safe_div", 1),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=WASM_DIV(20,4)").unwrap();
    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(5.0)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[cfg(feature = "wasm_plugins")]
#[test]
fn fake_runtime_binding_uses_arity_and_error_mapping() {
    let mut wb = workbook();
    let bytes = wasm_module_with_manifest(VALID_MANIFEST);
    wb.register_wasm_module_bytes("plugin://finance/core", &bytes)
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    wb.set_wasm_runtime(Arc::new(FakeWasmRuntime::new(
        FakeRuntimeMode::Divide,
        calls.clone(),
    )));

    wb.register_wasm_function(
        "WASM_DIV",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        WasmFunctionSpec::new("plugin://finance/core", "fn_safe_div", 1),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=WASM_DIV(20)").unwrap();
    let arity_err = wb.evaluate_cell("Sheet1", 1, 1).unwrap();
    let LiteralValue::Error(arity_err) = arity_err else {
        panic!("expected VALUE error for arity mismatch");
    };
    assert_eq!(arity_err.kind, ExcelErrorKind::Value);
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let calls = Arc::new(AtomicUsize::new(0));
    wb.set_wasm_runtime(Arc::new(FakeWasmRuntime::new(
        FakeRuntimeMode::AlwaysError,
        calls.clone(),
    )));

    wb.unregister_custom_function("WASM_DIV").unwrap();
    wb.register_wasm_function(
        "WASM_DIV",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        WasmFunctionSpec::new("plugin://finance/core", "fn_safe_div", 1),
    )
    .unwrap();

    wb.set_formula("Sheet1", 2, 1, "=WASM_DIV(20,4)").unwrap();
    let runtime_err = wb.evaluate_cell("Sheet1", 2, 1).unwrap();
    let LiteralValue::Error(runtime_err) = runtime_err else {
        panic!("expected runtime error to propagate");
    };
    assert_eq!(runtime_err.kind, ExcelErrorKind::Value);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
#[test]
fn wasmtime_runtime_can_bind_and_evaluate_numeric_export() {
    let mut wb = workbook();
    wb.use_wasmtime_runtime();

    let bytes = wasm_module_with_manifest_and_div_export(DIV_ONLY_MANIFEST);
    wb.attach_wasm_module_bytes("plugin://math/div", &bytes)
        .unwrap();

    wb.bind_wasm_function(
        "WASM_DIV",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        WasmFunctionSpec::new("plugin://math/div", "fn_safe_div", 1),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=WASM_DIV(20,4)").unwrap();
    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(5.0)
    );
}
