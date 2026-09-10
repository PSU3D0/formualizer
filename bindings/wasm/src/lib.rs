use js_sys::{Object, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;

mod ast;
mod dialect;
mod errors;
mod inspect;
mod parser;
mod reference;
mod sheetport;
mod token;
mod tokenizer;
mod utils;
mod workbook;

pub use ast::*;
pub use dialect::*;
pub use errors::*;
pub use parser::*;
pub use reference::*;
pub use sheetport::*;
pub use token::*;
pub use tokenizer::*;
pub use workbook::*;

#[wasm_bindgen(start)]
pub fn init() {
    utils::set_panic_hook();
}

#[wasm_bindgen]
pub fn tokenize(formula: &str, dialect: Option<FormulaDialect>) -> Result<Tokenizer, JsValue> {
    Tokenizer::new(formula, dialect)
}

#[wasm_bindgen]
pub fn parse(formula: &str, dialect: Option<FormulaDialect>) -> Result<ASTNode, JsValue> {
    parser::parse_formula(formula, dialect)
}

fn xlsx_summary_to_js(
    summary: formualizer::workbook::RecalculateSummary,
) -> Result<JsValue, JsValue> {
    let out = Object::new();
    Reflect::set(
        &out,
        &JsValue::from_str("status"),
        &JsValue::from_str(summary.status.as_str()),
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("evaluated"),
        &JsValue::from_f64(summary.evaluated as f64),
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("errors"),
        &JsValue::from_f64(summary.errors as f64),
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("total_formulas"),
        &JsValue::from_f64(summary.evaluated as f64),
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("total_errors"),
        &JsValue::from_f64(summary.errors as f64),
    )?;
    let sheet_entries = js_sys::Array::new();
    for (name, stats) in summary.sheets {
        let sheet = Object::new();
        Reflect::set(
            &sheet,
            &JsValue::from_str("evaluated"),
            &JsValue::from_f64(stats.evaluated as f64),
        )?;
        Reflect::set(
            &sheet,
            &JsValue::from_str("errors"),
            &JsValue::from_f64(stats.errors as f64),
        )?;
        let entry = js_sys::Array::new();
        entry.push(&JsValue::from_str(&name));
        entry.push(&sheet);
        sheet_entries.push(&entry);
    }
    // Sheet names are user data: fromEntries creates own data properties even
    // for __proto__, rather than invoking Object.prototype's inherited setter.
    let sheets = Object::from_entries(&sheet_entries)?;
    Reflect::set(&out, &JsValue::from_str("sheets"), &sheets)?;
    if !summary.error_summary.is_empty() {
        let errors = Object::new();
        for (token, info) in summary.error_summary {
            let error = Object::new();
            Reflect::set(
                &error,
                &JsValue::from_str("count"),
                &JsValue::from_f64(info.count as f64),
            )?;
            let locations = js_sys::Array::new();
            for location in info.locations {
                locations.push(&JsValue::from_str(&location));
            }
            Reflect::set(&error, &JsValue::from_str("locations"), &locations)?;
            if info.locations_truncated > 0 {
                Reflect::set(
                    &error,
                    &JsValue::from_str("locations_truncated"),
                    &JsValue::from_f64(info.locations_truncated as f64),
                )?;
            }
            Reflect::set(&errors, &JsValue::from_str(&token), &error)?;
        }
        Reflect::set(&out, &JsValue::from_str("error_summary"), &errors)?;
    }
    Ok(out.into())
}

/// Recalculate formula caches while preserving all unrelated XLSX package members.
///
/// The returned `bytes` property is a real `Uint8Array`; no base64 or integer-array
/// encoding is used. `error_location_limit` optionally caps locations per error token.
#[wasm_bindgen(js_name = "recalculateXlsxBytes")]
pub fn recalculate_xlsx_bytes(
    bytes: Uint8Array,
    error_location_limit: Option<u32>,
) -> Result<JsValue, JsValue> {
    let mut options = formualizer::workbook::XlsxRecalculateOptions::default();
    if let Some(limit) = error_location_limit {
        options.error_location_limit = limit as usize;
    }
    // This admission check happens before copying the JS typed array into Rust memory.
    if bytes.length() as usize > options.limits.max_input_bytes {
        return Err(JsValue::from(js_sys::Error::new(
            "recalculate XLSX failed: input size limit exceeded",
        )));
    }
    let input = bytes.to_vec();
    let result = formualizer::workbook::recalculate_xlsx_bytes(&input, options)
        .map_err(errors::workbook_error_to_js)?;
    let out = Object::new();
    let output = Uint8Array::from(result.bytes.as_slice());
    Reflect::set(&out, &JsValue::from_str("bytes"), &output)?;
    Reflect::set(
        &out,
        &JsValue::from_str("summary"),
        &xlsx_summary_to_js(result.summary)?,
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("formula_cells"),
        &JsValue::from_f64(result.formula_cells as f64),
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("cache_cells_changed"),
        &JsValue::from_f64(result.cache_cells_changed as f64),
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("worksheet_parts_changed"),
        &JsValue::from_f64(result.worksheet_parts_changed as f64),
    )?;
    Ok(out.into())
}
