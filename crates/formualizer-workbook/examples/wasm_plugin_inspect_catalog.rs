#[cfg(all(feature = "wasm_plugins", not(target_arch = "wasm32")))]
use formualizer_workbook::Workbook;

#[cfg(all(feature = "wasm_plugins", not(target_arch = "wasm32")))]
use std::fs;

#[cfg(all(feature = "wasm_plugins", not(target_arch = "wasm32")))]
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

#[cfg(all(feature = "wasm_plugins", not(target_arch = "wasm32")))]
fn module_with_manifest(manifest_json: &str) -> Vec<u8> {
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

#[cfg(all(feature = "wasm_plugins", not(target_arch = "wasm32")))]
fn manifest(module_id: &str, function_name: &str, export_name: &str) -> String {
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
      "name": "{function_name}",
      "aliases": [],
      "export": "{export_name}",
      "min_args": 1,
      "max_args": 1,
      "volatile": false,
      "deterministic": true,
      "thread_safe": true
    }}
  ]
}}"#
    )
}

#[cfg(all(feature = "wasm_plugins", not(target_arch = "wasm32")))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wb = Workbook::new();
    let temp = tempfile::tempdir()?;

    fs::write(
        temp.path().join("finance.wasm"),
        module_with_manifest(&manifest(
            "plugin://finance/core",
            "XNPV_PLUS",
            "fn_xnpv_plus",
        )),
    )?;
    fs::write(
        temp.path().join("stats.wasm"),
        module_with_manifest(&manifest(
            "plugin://stats/core",
            "ROBUST_AVG",
            "fn_robust_avg",
        )),
    )?;

    let modules = wb.inspect_wasm_modules_dir(temp.path())?;
    for info in modules {
        println!("found module: {} ({})", info.module_id, info.version);
    }

    // Inspection is effect-free:
    assert!(wb.list_wasm_modules().is_empty());
    Ok(())
}

#[cfg(not(all(feature = "wasm_plugins", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("This example requires a native target with `--features wasm_plugins`.");
}
