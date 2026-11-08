//! Portable source-policy transport, using the workbook crate's serde contract.
use formualizer::workbook::{
    SourceCalculationPolicy, SourceRecalculateLimits, XlsxRecalculateOptions,
};
use js_sys::{Object, Reflect};
use wasm_bindgen::{JsCast, JsValue};

const MAX_DEPTH: usize = 32;
const MAX_NODES: usize = 4096;
const MAX_STRING_UNITS: u32 = 65_536;

fn request_error(message: &str) -> JsValue {
    super::utils::js_error(format!("portable recalculation request: {message}"))
}

/// Read only own enumerable data properties. Descriptors snapshot each value
/// exactly once, without invoking accessors or following inherited properties.
fn fields(value: &JsValue) -> Result<Vec<(String, JsValue)>, JsValue> {
    if !value.is_object() || value.is_null() || js_sys::Array::is_array(value) {
        return Err(request_error("expected a plain object"));
    }
    let prototype = Reflect::get_prototype_of(value)?;
    if !prototype.is_null() && !Object::is(&prototype, &Object::get_prototype_of(&Object::new())) {
        return Err(request_error(
            "only plain objects or null-prototype records are accepted",
        ));
    }
    let keys = Reflect::own_keys(value)?;
    if keys.length() as usize > MAX_NODES {
        return Err(request_error("property limit exceeded"));
    }
    let object = value.unchecked_ref::<Object>();
    let mut entries = Vec::with_capacity(keys.length() as usize);
    for key in keys.iter() {
        if !key.is_string() || js_sys::JsString::from(key.clone()).length() > MAX_STRING_UNITS {
            return Err(request_error("keys must be bounded strings"));
        }
        let descriptor = Reflect::get_own_property_descriptor(object, &key)?;
        // The native descriptor is an ordinary object. Check its OWN value
        // field: Object.prototype pollution must not disguise an accessor.
        if descriptor.is_undefined()
            || !Object::has_own(descriptor.unchecked_ref::<Object>(), &"value".into())
        {
            return Err(request_error("accessor properties are not accepted"));
        }
        if Reflect::get(&descriptor, &"enumerable".into())?.as_bool() != Some(true) {
            return Err(request_error("properties must be enumerable"));
        }
        entries.push((
            key.as_string().unwrap(),
            Reflect::get(&descriptor, &"value".into())?,
        ));
    }
    Ok(entries)
}

/// Produce the exact JSON value decoded by upstream in one bounded traversal.
/// No subsequent serde-wasm-bindgen traversal can consume a different value.
fn normalize(
    value: &JsValue,
    ancestors: &mut Vec<JsValue>,
    nodes: &mut usize,
) -> Result<serde_json::Value, JsValue> {
    *nodes += 1;
    if *nodes > MAX_NODES {
        return Err(request_error("node limit exceeded"));
    }
    if ancestors.len() > MAX_DEPTH {
        return Err(request_error("depth limit exceeded"));
    }
    if value.is_null() {
        return Ok(serde_json::Value::Null);
    }
    if let Some(value) = value.as_bool() {
        return Ok(value.into());
    }
    if let Some(number) = value.as_f64() {
        if !number.is_finite() {
            return Err(request_error("numbers must be finite"));
        }
        return Ok(
            if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0 {
                serde_json::Value::from(number as i64)
            } else {
                serde_json::Value::Number(serde_json::Number::from_f64(number).unwrap())
            },
        );
    }
    if value.is_string() {
        if js_sys::JsString::from(value.clone()).length() > MAX_STRING_UNITS {
            return Err(request_error("string limit exceeded"));
        }
        return Ok(value.as_string().unwrap().into());
    }
    if ancestors.iter().any(|parent| Object::is(parent, value)) {
        return Err(request_error("cyclic object"));
    }
    let entries = fields(value)?;
    if entries.len() > MAX_NODES - *nodes {
        return Err(request_error("node limit exceeded"));
    }
    ancestors.push(value.clone());
    let mut object = serde_json::Map::new();
    for (key, value) in entries {
        object.insert(key, normalize(&value, ancestors, nodes)?);
    }
    ancestors.pop();
    Ok(serde_json::Value::Object(object))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn normalize_for_test(value: &JsValue) -> Result<serde_json::Value, JsValue> {
    normalize(value, &mut Vec::new(), &mut 0)
}

pub(crate) struct PreparedRequest {
    pub options: JsValue,
    calculation: Option<SourceCalculationPolicy>,
    limits: Option<SourceRecalculateLimits>,
    physical_telemetry: bool,
}

impl PreparedRequest {
    /// Select the complete preset before any explicit legacy options are merged.
    pub(crate) fn initial_options(&self) -> Result<XlsxRecalculateOptions, JsValue> {
        let object = self.options.unchecked_ref::<Object>();
        let Some(profile) = super::sheetport::get_optional_value(object, "profile")? else {
            return Ok(XlsxRecalculateOptions::default());
        };
        match profile.as_string().as_deref() {
            Some("strict") => Ok(XlsxRecalculateOptions::default()),
            Some("workbook") => Ok(XlsxRecalculateOptions::workbook()),
            _ => Err(request_error("profile must be strict or workbook")),
        }
    }
}

/// Snapshot the outer options before legacy parsing. Legacy bigint seeds and
/// Date timestamps keep their established APIs; calculation is JSON-style only.
pub(crate) fn prepare(value: &JsValue) -> Result<PreparedRequest, JsValue> {
    // Missing fields must not be filled from Object.prototype by legacy readers.
    let object = Object::create(JsValue::NULL.unchecked_ref());
    let mut calculation = None;
    let mut limits = None;
    let mut physical_telemetry = false;
    let mut nodes = 0;
    if !value.is_null() && !value.is_undefined() {
        for (key, value) in fields(value)? {
            if !matches!(
                key.as_str(),
                "profile"
                    | "calculation"
                    | "requireComplete"
                    | "rngSeed"
                    | "deterministicTimestampUtc"
                    | "deterministicTimezone"
                    | "externalLinks"
                    | "limits"
                    | "physicalTelemetry"
            ) {
                return Err(request_error(&format!(
                    "unknown recalculation option: {key}"
                )));
            }
            if key == "physicalTelemetry" {
                if !value.is_undefined() {
                    physical_telemetry = normalize(&value, &mut Vec::new(), &mut nodes)?
                        .as_bool()
                        .ok_or_else(|| request_error("physicalTelemetry must be boolean"))?;
                }
            } else if key == "limits" {
                if !value.is_null() && !value.is_undefined() {
                    limits = Some(
                        serde_json::from_value(normalize(&value, &mut Vec::new(), &mut nodes)?)
                            .map_err(|error| request_error(&error.to_string()))?,
                    );
                }
            } else if key == "calculation" {
                if !value.is_null() && !value.is_undefined() {
                    let normalized = normalize(&value, &mut Vec::new(), &mut nodes)?;
                    calculation = Some(
                        serde_json::from_value(normalized)
                            .map_err(|error| request_error(&error.to_string()))?,
                    );
                }
            } else {
                Reflect::set(&object, &key.into(), &value)?;
            }
        }
    }
    Ok(PreparedRequest {
        options: object.into(),
        calculation,
        limits,
        physical_telemetry,
    })
}

pub(crate) fn apply(
    options: &mut XlsxRecalculateOptions,
    request: &PreparedRequest,
) -> Result<(), JsValue> {
    let object = request.options.unchecked_ref::<Object>();
    let get = |key: &str| super::sheetport::get_optional_value(object, key);
    if let Some(limits) = &request.limits {
        limits
            .apply(&mut options.limits)
            .map_err(super::errors::workbook_error_to_js)?;
    }
    options.physical_telemetry = request.physical_telemetry;
    if let Some(required) = get("requireComplete")? {
        options.require_complete = required
            .as_bool()
            .ok_or_else(|| super::utils::js_error("requireComplete must be boolean"))?;
    }
    let mut policy = if let Some(calculation) = &request.calculation {
        calculation.clone()
    } else if let Some(policy) = &options.calculation {
        policy.clone()
    } else {
        return Ok(());
    };
    let conflict =
        |field| super::utils::js_error(format!("calculation conflicts with legacy {field}"));
    if get("rngSeed")?.is_some() {
        let value = options.eval_config.workbook_seed;
        if policy.workbook_seed.is_some_and(|v| v != value) {
            return Err(conflict("rngSeed"));
        }
        policy.workbook_seed = Some(value);
    }
    if let formualizer::eval::engine::DeterministicMode::Enabled {
        timestamp_utc,
        timezone,
    } = &options.eval_config.deterministic_mode
    {
        let timestamp = get("deterministicTimestampUtc")?.is_some();
        if timestamp {
            if policy.fixed_now_utc.is_some_and(|v| v != *timestamp_utc) {
                return Err(conflict("deterministicTimestampUtc"));
            }
            policy.fixed_now_utc = Some(*timestamp_utc);
        }
        if get("deterministicTimezone")?.is_some() {
            if policy.timezone.as_ref().is_some_and(|v| v != timezone) {
                return Err(conflict("deterministicTimezone"));
            }
            policy.timezone = Some(timezone.clone());
        } else if timestamp && policy.timezone.is_none() {
            policy.timezone = Some(timezone.clone());
        }
    }
    policy
        .validate()
        .map_err(super::errors::workbook_error_to_js)?;
    options.calculation = Some(policy);
    Ok(())
}

/// The shared DTO serializer owns receipt encodings (including large counters).
fn receipt_value(result: &formualizer::workbook::XlsxRecalculateResult) -> serde_json::Value {
    serde_json::json!({
        "calculation": result.calculation,
        "coverage": result.coverage,
        "coverage_scope": result.coverage_scope,
        "caches_invalidated": result.caches_invalidated,
        "calculation_flags_changed": result.calculation_flags_changed,
        "operation_diagnostics": result.operation_diagnostics,
    })
}

pub(crate) fn receipt(
    result: &formualizer::workbook::XlsxRecalculateResult,
) -> Result<Object, JsValue> {
    // JSON-compatible serialization keeps canonical receipt field names and exact
    // seed/budget strings, without turning dictionaries into JS Map instances.
    let value = receipt_value(result);
    // JSON.parse creates own data properties. Return that object directly:
    // Object.assign into a fresh ordinary object would invoke inherited setters.
    Ok(js_sys::JSON::parse(&value.to_string())?.unchecked_into())
}
