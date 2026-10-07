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
            let messages = js_sys::Array::new();
            for message in info.messages {
                messages.push(&message.as_deref().map_or(JsValue::NULL, JsValue::from_str));
            }
            Reflect::set(&error, &JsValue::from_str("messages"), &messages)?;
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
    let unknown = js_sys::Array::new();
    for (name, cells) in summary.unknown_functions {
        let entry = Object::new();
        Reflect::set(
            &entry,
            &JsValue::from_str("name"),
            &JsValue::from_str(&name),
        )?;
        Reflect::set(
            &entry,
            &JsValue::from_str("cells"),
            &JsValue::from_f64(cells as f64),
        )?;
        unknown.push(&entry);
    }
    Reflect::set(&out, &JsValue::from_str("unknown_functions"), &unknown)?;
    Ok(out.into())
}

/// Apply `{ rngSeed, deterministicTimestampUtc, deterministicTimezone }`,
/// spelled as in `SheetPortSession.evaluateOnce`. `rngSeed` may also be a
/// `bigint`, so the echoed 64-bit `seed` can be passed back.
fn apply_reproducibility_options(
    config: &mut formualizer::eval::engine::EvalConfig,
    options: &JsValue,
) -> Result<(), JsValue> {
    use formualizer::eval::{engine::DeterministicMode, timezone::TimeZoneSpec};
    if options.is_null() || options.is_undefined() {
        return Ok(());
    }
    let obj = options
        .clone()
        .dyn_into::<Object>()
        .map_err(|_| utils::js_error("recalculateXlsxBytes options must be an object"))?;
    if let Some(value) = sheetport::get_optional_value(&obj, "rngSeed")? {
        const MESSAGE: &str = "rngSeed must be a non-negative safe integer or a u64 bigint";
        config.workbook_seed = if value.is_bigint() {
            js_sys::BigInt::from(value)
                .to_string(10)
                .ok()
                .and_then(|text| String::from(text).parse::<u64>().ok())
                .ok_or_else(|| utils::js_error(MESSAGE))?
        } else {
            let n = value.as_f64().ok_or_else(|| utils::js_error(MESSAGE))?;
            if !n.is_finite() || n < 0.0 || n.fract() != 0.0 || n > 9_007_199_254_740_991.0 {
                return Err(utils::js_error(MESSAGE));
            }
            n as u64
        };
    }
    let timestamp = sheetport::get_optional_value(&obj, "deterministicTimestampUtc")?;
    let timezone = sheetport::get_optional_value(&obj, "deterministicTimezone")?;
    if let Some(value) = timestamp {
        let mode = DeterministicMode::Enabled {
            timestamp_utc: sheetport::parse_timestamp_utc(&value)?,
            timezone: match timezone {
                Some(value) => sheetport::parse_timezone_spec(&value)?,
                None => TimeZoneSpec::Utc,
            },
        };
        mode.validate().map_err(|e| {
            utils::js_error(format!(
                "`deterministicTimezone`: {}",
                e.message.unwrap_or_default()
            ))
        })?;
        config.deterministic_mode = mode;
    } else if timezone.is_some() {
        return Err(utils::js_error(
            "deterministicTimezone requires deterministicTimestampUtc",
        ));
    }
    Ok(())
}

/// `{ now, timezone, fixed }`, as in the CLI's JSON report: `now` is an
/// RFC 3339 string in the offset that was applied (for `Local`, the host
/// offset at that instant), or null without a clock.
fn clock_to_js(
    config: &formualizer::eval::engine::EvalConfig,
    clock_now_utc: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<JsValue, JsValue> {
    use formualizer::eval::timezone::TimeZoneSpec;
    let zone = config.deterministic_mode.timezone();
    let clock = Object::new();
    let now = clock_now_utc.map_or(JsValue::NULL, |now| {
        let offset = zone
            .fixed_offset()
            .unwrap_or_else(|| *now.with_timezone(&chrono::Local).offset());
        JsValue::from_str(
            &now.with_timezone(&offset)
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        )
    });
    Reflect::set(&clock, &JsValue::from_str("now"), &now)?;
    let label = match zone {
        TimeZoneSpec::Local => "Local".to_owned(),
        TimeZoneSpec::Utc => "UTC".to_owned(),
        TimeZoneSpec::FixedOffsetSeconds(secs) => {
            let sign = if *secs < 0 { '-' } else { '+' };
            let s = secs.unsigned_abs();
            format!("{sign}{:02}:{:02}", s / 3600, s / 60 % 60)
        }
    };
    Reflect::set(
        &clock,
        &JsValue::from_str("timezone"),
        &JsValue::from_str(&label),
    )?;
    Reflect::set(
        &clock,
        &JsValue::from_str("fixed"),
        &JsValue::from_bool(config.deterministic_mode.is_enabled()),
    )?;
    Ok(clock.into())
}

/// Recalculate formula caches while preserving all unrelated XLSX package members.
///
/// The returned `bytes` property is a real `Uint8Array`; no base64 or integer-array
/// encoding is used. `error_location_limit` optionally caps locations per error token.
/// `options` optionally fixes the clock and RAND seed
/// (`{ rngSeed, deterministicTimestampUtc, deterministicTimezone }`); the
/// result's `clock` and `seed` replay the run.
#[wasm_bindgen(js_name = "recalculateXlsxBytes")]
pub fn recalculate_xlsx_bytes(
    bytes: Uint8Array,
    error_location_limit: Option<u32>,
    options: JsValue,
) -> Result<JsValue, JsValue> {
    let mut recalc_options = formualizer::workbook::XlsxRecalculateOptions::default();
    if let Some(limit) = error_location_limit {
        recalc_options.error_location_limit = limit as usize;
    }
    apply_reproducibility_options(&mut recalc_options.eval_config, &options)?;
    let options = recalc_options;
    let config = options.eval_config.clone();
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
    Reflect::set(
        &out,
        &JsValue::from_str("clock"),
        &clock_to_js(&config, result.clock_now_utc)?,
    )?;
    Reflect::set(
        &out,
        &JsValue::from_str("seed"),
        &js_sys::BigInt::from(config.workbook_seed).into(),
    )?;
    Ok(out.into())
}
