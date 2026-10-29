use js_sys::Uint8Array;
#[cfg(feature = "xlsx-recalc")]
use js_sys::{Object, Reflect};
use wasm_bindgen::prelude::*;

mod ast;
mod dialect;
mod errors;
mod inspect;
mod parser;
mod reference;
mod sheetport;
#[cfg(feature = "xlsx-recalc")]
mod source_calculation;
#[cfg(all(test, target_arch = "wasm32", feature = "xlsx-recalc"))]
mod source_calculation_tests;
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

/// Define own data fields without invoking setters inherited by result records.
#[cfg(feature = "xlsx-recalc")]
fn set_result_field(target: &Object, key: &JsValue, value: &JsValue) -> Result<bool, JsValue> {
    // A null-prototype descriptor also excludes inherited get/set attributes.
    let descriptor = Object::create(JsValue::NULL.unchecked_ref());
    Reflect::set(&descriptor, &"value".into(), value)?;
    for attribute in ["writable", "enumerable", "configurable"] {
        Reflect::set(&descriptor, &attribute.into(), &JsValue::TRUE)?;
    }
    Reflect::define_property(target, key, &descriptor)
}

#[cfg(feature = "xlsx-recalc")]
fn xlsx_summary_to_js(
    summary: formualizer::workbook::RecalculateSummary,
) -> Result<JsValue, JsValue> {
    let out = Object::new();
    set_result_field(
        &out,
        &JsValue::from_str("status"),
        &JsValue::from_str(summary.status.as_str()),
    )?;
    set_result_field(
        &out,
        &JsValue::from_str("evaluated"),
        &JsValue::from_f64(summary.evaluated as f64),
    )?;
    set_result_field(
        &out,
        &JsValue::from_str("errors"),
        &JsValue::from_f64(summary.errors as f64),
    )?;
    set_result_field(
        &out,
        &JsValue::from_str("total_formulas"),
        &JsValue::from_f64(summary.evaluated as f64),
    )?;
    set_result_field(
        &out,
        &JsValue::from_str("total_errors"),
        &JsValue::from_f64(summary.errors as f64),
    )?;
    let sheet_entries = js_sys::Array::new();
    for (name, stats) in summary.sheets {
        let sheet = Object::new();
        set_result_field(
            &sheet,
            &JsValue::from_str("evaluated"),
            &JsValue::from_f64(stats.evaluated as f64),
        )?;
        set_result_field(
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
    set_result_field(&out, &JsValue::from_str("sheets"), &sheets)?;
    if !summary.error_summary.is_empty() {
        let errors = Object::new();
        for (token, info) in summary.error_summary {
            let error = Object::new();
            set_result_field(
                &error,
                &JsValue::from_str("count"),
                &JsValue::from_f64(info.count as f64),
            )?;
            let locations = js_sys::Array::new();
            for location in info.locations {
                locations.push(&JsValue::from_str(&location));
            }
            set_result_field(&error, &JsValue::from_str("locations"), &locations)?;
            let messages = js_sys::Array::new();
            for message in info.messages {
                messages.push(&message.as_deref().map_or(JsValue::NULL, JsValue::from_str));
            }
            set_result_field(&error, &JsValue::from_str("messages"), &messages)?;
            if info.locations_truncated > 0 {
                set_result_field(
                    &error,
                    &JsValue::from_str("locations_truncated"),
                    &JsValue::from_f64(info.locations_truncated as f64),
                )?;
            }
            set_result_field(&errors, &JsValue::from_str(&token), &error)?;
        }
        set_result_field(&out, &JsValue::from_str("error_summary"), &errors)?;
    }
    let unknown = js_sys::Array::new();
    for (name, cells) in summary.unknown_functions {
        let entry = Object::new();
        set_result_field(
            &entry,
            &JsValue::from_str("name"),
            &JsValue::from_str(&name),
        )?;
        set_result_field(
            &entry,
            &JsValue::from_str("cells"),
            &JsValue::from_f64(cells as f64),
        )?;
        unknown.push(&entry);
    }
    set_result_field(&out, &JsValue::from_str("unknown_functions"), &unknown)?;
    Ok(out.into())
}

/// Apply `{ rngSeed, deterministicTimestampUtc, deterministicTimezone }`,
/// spelled as in `SheetPortSession.evaluateOnce`. `rngSeed` may also be a
/// `bigint`, so the echoed 64-bit `seed` can be passed back.
#[cfg(feature = "xlsx-recalc")]
fn apply_reproducibility_options(
    config: &mut formualizer::eval::engine::EvalConfig,
    options: &JsValue,
) -> Result<(), JsValue> {
    use formualizer::eval::{engine::DeterministicMode, timezone::TimeZoneSpec};
    if options.is_null() || options.is_undefined() {
        return Ok(());
    }
    let obj = Object::try_from(options)
        .ok_or_else(|| utils::js_error("recalculateXlsxBytes options must be an object"))?;
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
                e.message().unwrap_or_default()
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

/// Apply `externalLinks` (`'cached'`, the default, or `'refuse'`).
#[cfg(feature = "xlsx-recalc")]
fn apply_external_links_option(
    recalc: &mut formualizer::workbook::XlsxRecalculateOptions,
    options: &JsValue,
) -> Result<(), JsValue> {
    use formualizer::workbook::ExternalLinkPolicy;
    let Some(obj) = Object::try_from(options) else {
        return Ok(());
    };
    if let Some(value) = sheetport::get_optional_value(obj, "externalLinks")? {
        recalc.external_links = match value.as_string().as_deref() {
            Some("cached") => ExternalLinkPolicy::Cached,
            Some("refuse") => ExternalLinkPolicy::Refuse,
            _ => {
                return Err(utils::js_error(
                    "externalLinks must be 'cached' or 'refuse'",
                ));
            }
        };
    }
    Ok(())
}

/// `{ now, timezone, fixed }`, as in the CLI's JSON report: `now` is an
/// RFC 3339 string in the offset that was applied (for `Local`, the host
/// offset at that instant), or null without a clock.
#[cfg(feature = "xlsx-recalc")]
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
    set_result_field(&clock, &JsValue::from_str("now"), &now)?;
    let label = match zone {
        TimeZoneSpec::Local => "Local".to_owned(),
        TimeZoneSpec::Utc => "UTC".to_owned(),
        TimeZoneSpec::FixedOffsetSeconds(secs) => {
            let sign = if *secs < 0 { '-' } else { '+' };
            let s = secs.unsigned_abs();
            format!("{sign}{:02}:{:02}", s / 3600, s / 60 % 60)
        }
    };
    set_result_field(
        &clock,
        &JsValue::from_str("timezone"),
        &JsValue::from_str(&label),
    )?;
    set_result_field(
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
/// result's `clock` and `seed` replay the run. `externalLinks: 'refuse'`
/// refuses a workbook whose formulas read external link values instead of
/// using the values cached in it (`'cached'`, the default).
#[wasm_bindgen(js_name = "recalculateXlsxBytes")]
pub fn recalculate_xlsx_bytes(
    bytes: Uint8Array,
    error_location_limit: Option<u32>,
    options: JsValue,
) -> Result<JsValue, JsValue> {
    #[cfg(not(feature = "xlsx-recalc"))]
    {
        let _ = (bytes, error_location_limit, options);
        Err(utils::js_error(
            "native XLSX recalculation requires the xlsx-recalc feature",
        ))
    }
    #[cfg(feature = "xlsx-recalc")]
    {
        let request = source_calculation::prepare(&options)?;
        let mut recalc_options = request.initial_options()?;
        if let Some(limit) = error_location_limit {
            recalc_options.error_location_limit = limit as usize;
        }
        apply_reproducibility_options(&mut recalc_options.eval_config, &request.options)?;
        apply_external_links_option(&mut recalc_options, &request.options)?;
        source_calculation::apply(&mut recalc_options, &request)?;
        let options = recalc_options;
        let mut config = options.eval_config.clone();
        // This admission check happens before copying the JS typed array into Rust memory.
        if bytes.length() as usize > options.limits.max_input_bytes {
            return Err(JsValue::from(js_sys::Error::new(
                "recalculate XLSX failed: input size limit exceeded",
            )));
        }
        let input = bytes.to_vec();
        let result = formualizer::workbook::recalculate_xlsx_bytes(&input, options)
            .map_err(errors::workbook_error_to_js)?;
        let out = source_calculation::receipt(&result)?;
        if let Some(calculation) = &result.calculation {
            config.workbook_seed = calculation.workbook_seed;
            config.deterministic_mode = match calculation.fixed_now_utc {
                Some(timestamp_utc) => formualizer::eval::engine::DeterministicMode::Enabled {
                    timestamp_utc,
                    timezone: calculation.timezone.clone(),
                },
                None => formualizer::eval::engine::DeterministicMode::Disabled {
                    timezone: calculation.timezone.clone(),
                },
            };
        }
        let output = Uint8Array::from(result.bytes.as_slice());
        set_result_field(&out, &JsValue::from_str("bytes"), &output)?;
        set_result_field(
            &out,
            &JsValue::from_str("summary"),
            &xlsx_summary_to_js(result.summary)?,
        )?;
        set_result_field(
            &out,
            &JsValue::from_str("formula_cells"),
            &JsValue::from_f64(result.formula_cells as f64),
        )?;
        set_result_field(
            &out,
            &JsValue::from_str("cache_cells_changed"),
            &JsValue::from_f64(result.cache_cells_changed as f64),
        )?;
        set_result_field(
            &out,
            &JsValue::from_str("worksheet_parts_changed"),
            &JsValue::from_f64(result.worksheet_parts_changed as f64),
        )?;
        if result.external_links_used > 0 {
            let links = Object::new();
            set_result_field(
                &links,
                &JsValue::from_str("links_used"),
                &JsValue::from_f64(result.external_links_used as f64),
            )?;
            set_result_field(&links, &JsValue::from_str("refreshed"), &JsValue::FALSE)?;
            // `refuse` refuses instead of reading link values.
            set_result_field(
                &links,
                &JsValue::from_str("policy"),
                &JsValue::from_str("cached"),
            )?;
            set_result_field(&out, &JsValue::from_str("external_links"), &links)?;
        }
        set_result_field(
            &out,
            &JsValue::from_str("clock"),
            &clock_to_js(&config, result.clock_now_utc)?,
        )?;
        set_result_field(
            &out,
            &JsValue::from_str("seed"),
            &js_sys::BigInt::from(config.workbook_seed).into(),
        )?;
        Ok(out.into())
    }
}
