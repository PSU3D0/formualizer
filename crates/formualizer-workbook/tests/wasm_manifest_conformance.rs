#![cfg(feature = "wasm_plugins")]

use formualizer_common::ExcelErrorKind;
use formualizer_workbook::{parse_wasm_manifest_json, validate_wasm_manifest};

const VALID_MANIFEST: &str = include_str!("fixtures/wasm_manifest/valid_v1.json");
const INVALID_SCHEMA_MANIFEST: &str = include_str!("fixtures/wasm_manifest/invalid_schema.json");
const INVALID_DUPLICATE_ALIAS_MANIFEST: &str =
    include_str!("fixtures/wasm_manifest/invalid_duplicate_alias.json");
const INVALID_ARITY_MANIFEST: &str = include_str!("fixtures/wasm_manifest/invalid_arity.json");

#[test]
fn valid_manifest_fixture_passes_validation() {
    let manifest = parse_wasm_manifest_json(VALID_MANIFEST.as_bytes()).unwrap();
    validate_wasm_manifest(&manifest).unwrap();
    assert_eq!(manifest.functions.len(), 2);
    assert_eq!(manifest.module.id, "plugin://finance/core");
}

#[test]
fn invalid_schema_fixture_fails_validation() {
    let err = parse_wasm_manifest_json(INVALID_SCHEMA_MANIFEST.as_bytes()).unwrap_err();
    assert_eq!(err.kind, ExcelErrorKind::Value);
    assert!(
        err.message
            .unwrap_or_default()
            .contains("Unsupported WASM manifest schema")
    );
}

#[test]
fn duplicate_alias_fixture_fails_validation() {
    let err = parse_wasm_manifest_json(INVALID_DUPLICATE_ALIAS_MANIFEST.as_bytes()).unwrap_err();
    assert_eq!(err.kind, ExcelErrorKind::Value);
    assert!(
        err.message
            .unwrap_or_default()
            .contains("Duplicate WASM function name or alias")
    );
}

#[test]
fn invalid_arity_fixture_fails_validation() {
    let err = parse_wasm_manifest_json(INVALID_ARITY_MANIFEST.as_bytes()).unwrap_err();
    assert_eq!(err.kind, ExcelErrorKind::Value);
    assert!(
        err.message
            .unwrap_or_default()
            .contains("Invalid WASM function arity")
    );
}
