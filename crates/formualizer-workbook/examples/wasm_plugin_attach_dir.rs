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
fn manifest(module_id: &str, fn_name: &str, export_name: &str) -> String {
    format!(
        r#"{{
  "schema": "formualizer.udf.module/v1",
  "module": {{
    "id": "{module_id}",
    "version": "1.0.0",
    "abi": 1,
    "codec": 1
  }},
  "functions": [
    {{
      "id": 1,
      "name": "{fn_name}",
      "aliases": [],
      "export": "{export_name}",
      "min_args": 2,
      "max_args": 2,
      "volatile": false,
      "deterministic": true,
      "thread_safe": true
    }}
  ]
}}"#
    )
}

#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
fn wasm_module_with_binary_export(
    manifest_json: &str,
    export_name: &str,
    op: Instruction<'static>,
) -> Vec<u8> {
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
    exports.export(export_name, ExportKind::Func, 0);
    module.section(&exports);

    let mut code = CodeSection::new();
    let mut function = Function::new([]);
    function.instruction(&Instruction::LocalGet(0));
    function.instruction(&Instruction::LocalGet(1));
    function.instruction(&op);
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

    let temp = tempfile::tempdir()?;
    let dir = temp.path();

    let add_manifest = manifest("plugin://math/add", "WASM_ADD", "fn_add");
    let mul_manifest = manifest("plugin://math/mul", "WASM_MUL", "fn_mul");

    fs::write(
        dir.join("math_add.wasm"),
        wasm_module_with_binary_export(&add_manifest, "fn_add", Instruction::F64Add),
    )?;
    fs::write(
        dir.join("math_mul.wasm"),
        wasm_module_with_binary_export(&mul_manifest, "fn_mul", Instruction::F64Mul),
    )?;

    let attached = wb.attach_wasm_modules_dir(dir)?;
    println!("attached modules: {attached:?}");

    wb.bind_wasm_function(
        "WASM_ADD",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        WasmFunctionSpec::new("plugin://math/add", "fn_add", 1),
    )?;

    wb.bind_wasm_function(
        "WASM_MUL",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        WasmFunctionSpec::new("plugin://math/mul", "fn_mul", 1),
    )?;

    wb.set_formula("Sheet1", 1, 1, "=WASM_ADD(10,5)")?;
    wb.set_formula("Sheet1", 2, 1, "=WASM_MUL(10,5)")?;

    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 1)?,
        LiteralValue::Number(15.0)
    );
    assert_eq!(
        wb.evaluate_cell("Sheet1", 2, 1)?,
        LiteralValue::Number(50.0)
    );

    Ok(())
}

#[cfg(not(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("This example requires a native target with `--features wasm_runtime_wasmtime`.");
}
