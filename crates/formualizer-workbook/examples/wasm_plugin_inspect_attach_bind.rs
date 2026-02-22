#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
use formualizer_common::LiteralValue;
#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
use formualizer_workbook::{CustomFnOptions, WasmFunctionSpec, Workbook};

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
use std::{borrow::Cow, fs};
#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
use wasm_encoder::{
    CodeSection, CustomSection, ExportKind, ExportSection, Function, FunctionSection, Instruction,
    Module, TypeSection, ValType,
};

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
const MANIFEST: &str = r#"{
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

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut wb = Workbook::new();
    wb.use_wasmtime_runtime();
    wb.add_sheet("Sheet1")?;

    let module_bytes = wasm_module_with_manifest_and_div_export(MANIFEST);
    let temp = tempfile::tempdir()?;
    let module_path = temp.path().join("math_div.wasm");
    fs::write(&module_path, &module_bytes)?;

    // Effect-free inspect
    let info = wb.inspect_wasm_module_file(&module_path)?;
    println!("inspected module: {info:?}");

    // Explicit workbook-local attach and bind
    wb.attach_wasm_module_file(&module_path)?;
    wb.bind_wasm_function(
        "WASM_DIV",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        WasmFunctionSpec::new("plugin://math/div", "fn_safe_div", 1),
    )?;

    wb.set_formula("Sheet1", 1, 1, "=WASM_DIV(20,4)")?;
    let value = wb.evaluate_cell("Sheet1", 1, 1)?;
    assert_eq!(value, LiteralValue::Number(5.0));

    println!("WASM_DIV(20,4) = {value:?}");
    Ok(())
}

#[cfg(not(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("This example requires a native target with `--features wasm_runtime_wasmtime`.");
}
