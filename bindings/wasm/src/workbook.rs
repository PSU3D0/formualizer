use crate::utils::{js_error, js_error_with_cause};
use formualizer::common::error::{ExcelError, ExcelErrorKind};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use wasm_bindgen::prelude::*;

#[derive(Default)]
struct JsCallbackRegistry {
    next_id: u64,
    callbacks: BTreeMap<u64, js_sys::Function>,
}

impl JsCallbackRegistry {
    fn insert(&mut self, callback: js_sys::Function) -> u64 {
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let id = self.next_id;
        self.callbacks.insert(id, callback);
        id
    }

    fn remove(&mut self, callback_id: u64) {
        self.callbacks.remove(&callback_id);
    }

    fn call(&self, callback_id: u64, args: &js_sys::Array) -> Result<JsValue, ExcelError> {
        let callback = self.callbacks.get(&callback_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message("JS callback is no longer registered")
        })?;
        callback
            .apply(&JsValue::UNDEFINED, args)
            .map_err(js_exception_to_excel_value)
    }
}

thread_local! {
    static JS_CALLBACK_REGISTRY: RefCell<JsCallbackRegistry> = RefCell::new(JsCallbackRegistry::default());
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsRegisterFunctionOptions {
    min_args: Option<usize>,
    max_args: Option<usize>,
    volatile: Option<bool>,
    thread_safe: Option<bool>,
    deterministic: Option<bool>,
    allow_override_builtin: Option<bool>,
}

#[derive(Debug)]
struct JsCustomFnHandler {
    callback_id: u64,
}

impl JsCustomFnHandler {
    fn new(callback_id: u64) -> Self {
        Self { callback_id }
    }
}

impl formualizer::workbook::CustomFnHandler for JsCustomFnHandler {
    fn call(
        &self,
        args: &[formualizer::LiteralValue],
    ) -> Result<formualizer::LiteralValue, ExcelError> {
        let js_args = js_sys::Array::new();
        for arg in args {
            js_args.push(&literal_to_js(arg.clone()));
        }

        let value = JS_CALLBACK_REGISTRY
            .with(|registry| registry.borrow().call(self.callback_id, &js_args))?;
        Ok(js_to_literal(&value))
    }
}

fn normalize_custom_function_name(name: &str) -> Result<String, JsValue> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(js_error("Function name cannot be empty"));
    }
    Ok(trimmed.to_ascii_uppercase())
}

fn register_js_callback(callback: js_sys::Function) -> u64 {
    JS_CALLBACK_REGISTRY.with(|registry| registry.borrow_mut().insert(callback))
}

fn unregister_js_callback(callback_id: u64) {
    JS_CALLBACK_REGISTRY.with(|registry| registry.borrow_mut().remove(callback_id));
}

fn parse_custom_function_options(
    raw: Option<JsValue>,
) -> Result<formualizer::workbook::CustomFnOptions, JsValue> {
    let parsed = if let Some(value) = raw {
        if value.is_null() || value.is_undefined() {
            JsRegisterFunctionOptions::default()
        } else {
            serde_wasm_bindgen::from_value::<JsRegisterFunctionOptions>(value)
                .map_err(|err| js_error(format!("invalid custom function options: {err}")))?
        }
    } else {
        JsRegisterFunctionOptions::default()
    };

    let mut options = formualizer::workbook::CustomFnOptions::default();
    if let Some(v) = parsed.min_args {
        options.min_args = v;
    }
    if parsed.max_args.is_some() {
        options.max_args = parsed.max_args;
    }
    if let Some(v) = parsed.volatile {
        options.volatile = v;
    }
    if let Some(v) = parsed.thread_safe {
        options.thread_safe = v;
    }
    if let Some(v) = parsed.deterministic {
        options.deterministic = v;
    }
    if let Some(v) = parsed.allow_override_builtin {
        options.allow_override_builtin = v;
    }

    Ok(options)
}

fn sanitize_js_error_detail(detail: &str) -> String {
    let normalized = detail
        .replace(['\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if normalized.chars().count() <= 240 {
        return normalized;
    }

    let mut truncated = String::new();
    for ch in normalized.chars().take(240) {
        truncated.push(ch);
    }
    truncated.push_str("...");
    truncated
}

fn js_exception_to_excel_value(err: JsValue) -> ExcelError {
    if let Some(error) = err.dyn_ref::<js_sys::Error>() {
        let error_name = error.name();
        let detail = sanitize_js_error_detail(&error.message().as_string().unwrap_or_default());
        let message = if detail.is_empty() {
            format!("JS callback threw {error_name}")
        } else {
            format!("JS callback threw {error_name}: {detail}")
        };
        return ExcelError::new(ExcelErrorKind::Value).with_message(message);
    }

    if let Some(detail) = err.as_string() {
        let detail = sanitize_js_error_detail(&detail);
        let message = if detail.is_empty() {
            "JS callback threw".to_string()
        } else {
            format!("JS callback threw: {detail}")
        };
        return ExcelError::new(ExcelErrorKind::Value).with_message(message);
    }

    let detail = js_sys::JSON::stringify(&err)
        .ok()
        .and_then(|value| value.as_string())
        .map(|value| sanitize_js_error_detail(&value))
        .unwrap_or_else(|| "non-string exception".to_string());

    ExcelError::new(ExcelErrorKind::Value).with_message(format!("JS callback threw: {detail}"))
}

pub(crate) fn js_to_literal(value: &JsValue) -> formualizer::LiteralValue {
    use formualizer::LiteralValue;

    if value.is_null() || value.is_undefined() {
        return LiteralValue::Empty;
    }

    if let Some(b) = value.as_bool() {
        return LiteralValue::Boolean(b);
    }

    if let Some(s) = value.as_string() {
        return LiteralValue::Text(s);
    }

    if let Some(n) = value.as_f64() {
        if n.fract() == 0.0 && n.is_finite() && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            return LiteralValue::Int(n as i64);
        }
        return LiteralValue::Number(n);
    }

    if js_sys::Array::is_array(value) {
        let arr = js_sys::Array::from(value);
        let has_nested_rows = arr.iter().any(|item| js_sys::Array::is_array(&item));

        if has_nested_rows {
            let mut rows = Vec::with_capacity(arr.length() as usize);
            for item in arr.iter() {
                if js_sys::Array::is_array(&item) {
                    let row_arr = js_sys::Array::from(&item);
                    let row = row_arr.iter().map(|cell| js_to_literal(&cell)).collect();
                    rows.push(row);
                } else {
                    rows.push(vec![js_to_literal(&item)]);
                }
            }
            return LiteralValue::Array(rows);
        }

        let row = arr.iter().map(|cell| js_to_literal(&cell)).collect();
        return LiteralValue::Array(vec![row]);
    }

    // Fallback string representation for unsupported objects.
    LiteralValue::Text(format!("{value:?}"))
}

pub(crate) fn literal_to_js(v: formualizer::LiteralValue) -> JsValue {
    match v {
        formualizer::LiteralValue::Empty => JsValue::NULL,
        formualizer::LiteralValue::Boolean(b) => JsValue::from_bool(b),
        formualizer::LiteralValue::Int(i) => JsValue::from_f64(i as f64),
        formualizer::LiteralValue::Number(n) => JsValue::from_f64(n),
        formualizer::LiteralValue::Text(s) => JsValue::from_str(&s),
        formualizer::LiteralValue::Date(d) => JsValue::from_str(&d.to_string()),
        formualizer::LiteralValue::DateTime(dt) => JsValue::from_str(&dt.to_string()),
        formualizer::LiteralValue::Time(t) => JsValue::from_str(&t.to_string()),
        formualizer::LiteralValue::Duration(dur) => JsValue::from_str(&format!("{dur:?}")),
        formualizer::LiteralValue::Array(values) => {
            let outer = js_sys::Array::new();
            for row in values {
                let arr = js_sys::Array::new();
                for cell in row {
                    arr.push(&literal_to_js(cell));
                }
                outer.push(&arr);
            }
            outer.into()
        }
        formualizer::LiteralValue::Pending => JsValue::from_str("Pending"),
        formualizer::LiteralValue::Error(err) => JsValue::from_str(&err.to_string()),
    }
}

fn set(obj: &js_sys::Object, key: &str, value: JsValue) -> Result<(), JsValue> {
    js_sys::Reflect::set(obj, &JsValue::from_str(key), &value)
        .map(|_| ())
        .map_err(|err| js_error_with_cause(format!("failed to set `{key}`"), err))
}

fn eval_plan_to_js(plan: &formualizer::EvalPlan) -> Result<JsValue, JsValue> {
    let obj = js_sys::Object::new();
    set(
        &obj,
        "total_vertices_to_evaluate",
        JsValue::from_f64(plan.total_vertices_to_evaluate as f64),
    )?;
    let layers = js_sys::Array::new();
    for layer in &plan.layers {
        let entry = js_sys::Object::new();
        set(
            &entry,
            "vertex_count",
            JsValue::from_f64(layer.vertex_count as f64),
        )?;
        set(
            &entry,
            "parallel_eligible",
            JsValue::from_bool(layer.parallel_eligible),
        )?;
        let sample_cells = js_sys::Array::new();
        for cell in &layer.sample_cells {
            sample_cells.push(&JsValue::from_str(cell));
        }
        set(&entry, "sample_cells", sample_cells.into())?;
        layers.push(&entry);
    }
    set(&obj, "layers", layers.into())?;
    set(
        &obj,
        "cycles_detected",
        JsValue::from_f64(plan.cycles_detected as f64),
    )?;
    set(
        &obj,
        "dirty_count",
        JsValue::from_f64(plan.dirty_count as f64),
    )?;
    set(
        &obj,
        "volatile_count",
        JsValue::from_f64(plan.volatile_count as f64),
    )?;
    set(
        &obj,
        "parallel_enabled",
        JsValue::from_bool(plan.parallel_enabled),
    )?;
    set(
        &obj,
        "estimated_parallel_layers",
        JsValue::from_f64(plan.estimated_parallel_layers as f64),
    )?;
    let targets = js_sys::Array::new();
    for cell in &plan.target_cells {
        targets.push(&JsValue::from_str(cell));
    }
    set(&obj, "target_cells", targets.into())?;
    Ok(obj.into())
}

#[wasm_bindgen]
pub struct Workbook {
    inner: Arc<RwLock<formualizer::workbook::Workbook>>,
    cancel_flag: Arc<std::sync::atomic::AtomicBool>,
    callback_ids: Arc<RwLock<BTreeMap<String, u64>>>,
}

impl Default for Workbook {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(formualizer::workbook::Workbook::new())),
            cancel_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            callback_ids: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Clone for Workbook {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cancel_flag: Arc::clone(&self.cancel_flag),
            callback_ids: Arc::clone(&self.callback_ids),
        }
    }
}

impl Drop for Workbook {
    fn drop(&mut self) {
        if Arc::strong_count(&self.callback_ids) != 1 {
            return;
        }

        let mut ids = match self.callback_ids.write() {
            Ok(ids) => ids,
            Err(_) => return,
        };

        for callback_id in ids.values().copied().collect::<Vec<_>>() {
            unregister_js_callback(callback_id);
        }
        ids.clear();
    }
}

#[wasm_bindgen]
impl Workbook {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Workbook {
        Workbook::default()
    }

    /// Construct from a JSON workbook string (feature: json)
    #[wasm_bindgen(js_name = "fromJson")]
    pub fn from_json(json: String) -> Result<Workbook, JsValue> {
        #[cfg(feature = "json")]
        {
            use formualizer::workbook::backends::JsonAdapter;
            use formualizer::workbook::traits::SpreadsheetReader;
            let adapter = <JsonAdapter as SpreadsheetReader>::open_bytes(json.into_bytes())
                .map_err(|e| js_error(format!("open failed: {e}")))?;
            let cfg = formualizer::workbook::WorkbookConfig::interactive();
            let wb = formualizer::workbook::Workbook::from_reader(
                adapter,
                formualizer::workbook::LoadStrategy::EagerAll,
                cfg,
            )
            .map_err(|e| js_error(format!("load failed: {e}")))?;
            Ok(Workbook {
                inner: Arc::new(RwLock::new(wb)),
                cancel_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                callback_ids: Arc::new(RwLock::new(BTreeMap::new())),
            })
        }
        #[cfg(not(feature = "json"))]
        {
            let _ = json;
            Err(js_error("json feature not enabled"))
        }
    }

    #[wasm_bindgen(js_name = "addSheet")]
    pub fn add_sheet(&self, name: String) -> Result<(), JsValue> {
        self.inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .add_sheet(&name)
            .map_err(|e| js_error(format!("add_sheet failed: {e}")))
    }

    #[wasm_bindgen(js_name = "sheetNames")]
    pub fn sheet_names(&self) -> js_sys::Array {
        let arr = js_sys::Array::new();
        let names = self
            .inner
            .read()
            .ok()
            .map(|w| w.sheet_names())
            .unwrap_or_default();
        for s in names.into_iter() {
            arr.push(&JsValue::from_str(&s));
        }
        arr
    }

    /// Register a workbook-local custom function backed by a JavaScript callback.
    #[wasm_bindgen(js_name = "registerFunction")]
    pub fn register_function(
        &self,
        name: String,
        callback: js_sys::Function,
        options: Option<JsValue>,
    ) -> Result<(), JsValue> {
        let canonical_name = normalize_custom_function_name(&name)?;
        let options = parse_custom_function_options(options)?;
        let callback_id = register_js_callback(callback);
        let handler = Arc::new(JsCustomFnHandler::new(callback_id));

        {
            let mut wb = self
                .inner
                .write()
                .map_err(|_| js_error("failed to lock workbook for write"))?;
            if let Err(err) = wb.register_custom_function(&name, options, handler) {
                unregister_js_callback(callback_id);
                return Err(js_error(format!(
                    "register_function failed for {canonical_name}: {err}"
                )));
            }
        }

        let mut callback_ids = match self.callback_ids.write() {
            Ok(ids) => ids,
            Err(_) => {
                unregister_js_callback(callback_id);
                let _ = self
                    .inner
                    .write()
                    .map_err(|_| js_error("failed to lock workbook for write"))?
                    .unregister_custom_function(&name);
                return Err(js_error("failed to lock callback registry for write"));
            }
        };
        if let Some(previous_id) = callback_ids.insert(canonical_name, callback_id) {
            unregister_js_callback(previous_id);
        }

        Ok(())
    }

    /// Unregister a previously registered workbook-local custom function.
    #[wasm_bindgen(js_name = "unregisterFunction")]
    pub fn unregister_function(&self, name: String) -> Result<(), JsValue> {
        let canonical_name = normalize_custom_function_name(&name)?;

        self.inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .unregister_custom_function(&name)
            .map_err(|err| js_error(format!("unregister_function failed for {name}: {err}")))?;

        if let Some(callback_id) = self
            .callback_ids
            .write()
            .map_err(|_| js_error("failed to lock callback registry for write"))?
            .remove(&canonical_name)
        {
            unregister_js_callback(callback_id);
        }

        Ok(())
    }

    /// List workbook-local custom functions and metadata.
    #[wasm_bindgen(js_name = "listFunctions")]
    pub fn list_functions(&self) -> Result<js_sys::Array, JsValue> {
        let wb = self
            .inner
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;

        let out = js_sys::Array::new();
        for info in wb.list_custom_functions() {
            let obj = js_sys::Object::new();
            set(&obj, "name", JsValue::from_str(&info.name))?;
            set(
                &obj,
                "minArgs",
                JsValue::from_f64(info.options.min_args as f64),
            )?;
            if let Some(max_args) = info.options.max_args {
                set(&obj, "maxArgs", JsValue::from_f64(max_args as f64))?;
            } else {
                set(&obj, "maxArgs", JsValue::NULL)?;
            }
            set(&obj, "volatile", JsValue::from_bool(info.options.volatile))?;
            set(
                &obj,
                "threadSafe",
                JsValue::from_bool(info.options.thread_safe),
            )?;
            set(
                &obj,
                "deterministic",
                JsValue::from_bool(info.options.deterministic),
            )?;
            set(
                &obj,
                "allowOverrideBuiltin",
                JsValue::from_bool(info.options.allow_override_builtin),
            )?;
            out.push(&obj);
        }

        Ok(out)
    }

    #[wasm_bindgen(js_name = "getNamedRanges")]
    pub fn get_named_ranges(&self, sheet: Option<String>) -> Result<js_sys::Array, JsValue> {
        let wb = self
            .inner
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;
        let engine = wb.engine();

        let entries = if let Some(sheet_name) = sheet.as_deref() {
            let sheet_id = engine
                .sheet_id(sheet_name)
                .ok_or_else(|| js_error(format!("Sheet not found: {sheet_name}")))?;
            engine.named_ranges_snapshot_for_sheet(sheet_id)
        } else {
            engine.named_ranges_snapshot()
        };

        let out = js_sys::Array::new();
        for entry in entries {
            let obj = js_sys::Object::new();
            set(&obj, "name", JsValue::from_str(&entry.name))?;

            match entry.scope {
                formualizer::eval::engine::named_range::NameScope::Workbook => {
                    set(&obj, "scope", JsValue::from_str("workbook"))?;
                    set(&obj, "scope_sheet", JsValue::NULL)?;
                }
                formualizer::eval::engine::named_range::NameScope::Sheet(sheet_id) => {
                    set(&obj, "scope", JsValue::from_str("sheet"))?;
                    set(
                        &obj,
                        "scope_sheet",
                        JsValue::from_str(engine.sheet_name(sheet_id)),
                    )?;
                }
            }

            match entry.definition {
                formualizer::eval::engine::named_range::NamedDefinition::Cell(cell) => {
                    set(&obj, "kind", JsValue::from_str("cell"))?;
                    set(
                        &obj,
                        "sheet",
                        JsValue::from_str(engine.sheet_name(cell.sheet_id)),
                    )?;
                    let row = cell.coord.row() + 1;
                    let col = cell.coord.col() + 1;
                    set(&obj, "start_row", JsValue::from_f64(row as f64))?;
                    set(&obj, "start_col", JsValue::from_f64(col as f64))?;
                    set(&obj, "end_row", JsValue::from_f64(row as f64))?;
                    set(&obj, "end_col", JsValue::from_f64(col as f64))?;
                }
                formualizer::eval::engine::named_range::NamedDefinition::Range(range) => {
                    set(&obj, "kind", JsValue::from_str("range"))?;
                    set(
                        &obj,
                        "start_sheet",
                        JsValue::from_str(engine.sheet_name(range.start.sheet_id)),
                    )?;
                    set(
                        &obj,
                        "end_sheet",
                        JsValue::from_str(engine.sheet_name(range.end.sheet_id)),
                    )?;
                    set(
                        &obj,
                        "start_row",
                        JsValue::from_f64((range.start.coord.row() + 1) as f64),
                    )?;
                    set(
                        &obj,
                        "start_col",
                        JsValue::from_f64((range.start.coord.col() + 1) as f64),
                    )?;
                    set(
                        &obj,
                        "end_row",
                        JsValue::from_f64((range.end.coord.row() + 1) as f64),
                    )?;
                    set(
                        &obj,
                        "end_col",
                        JsValue::from_f64((range.end.coord.col() + 1) as f64),
                    )?;
                    if range.start.sheet_id == range.end.sheet_id {
                        set(
                            &obj,
                            "sheet",
                            JsValue::from_str(engine.sheet_name(range.start.sheet_id)),
                        )?;
                    }
                }
                formualizer::eval::engine::named_range::NamedDefinition::Literal(value) => {
                    set(&obj, "kind", JsValue::from_str("literal"))?;
                    set(&obj, "value", literal_to_js(value))?;
                }
                formualizer::eval::engine::named_range::NamedDefinition::Formula { .. } => {
                    set(&obj, "kind", JsValue::from_str("formula"))?;
                }
            }

            out.push(&obj);
        }

        Ok(out)
    }

    /// Get a sheet facade by name (creates if missing)
    #[wasm_bindgen(js_name = "sheet")]
    pub fn sheet(&self, name: String) -> Result<Sheet, JsValue> {
        self.inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .add_sheet(&name)
            .map_err(|e| js_error(format!("failed to ensure sheet exists: {e}")))?;

        Ok(Sheet {
            wb: self.inner.clone(),
            name,
        })
    }

    #[wasm_bindgen(js_name = "setValue")]
    pub fn set_value(
        &self,
        sheet: String,
        row: u32,
        col: u32,
        value: JsValue,
    ) -> Result<(), JsValue> {
        let lv = js_to_literal(&value);
        self.inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .set_value(&sheet, row, col, lv)
            .map_err(|e| js_error(format!("set_value failed for {sheet}!R{row}C{col}: {e}")))
    }

    #[wasm_bindgen(js_name = "setFormula")]
    pub fn set_formula(
        &self,
        sheet: String,
        row: u32,
        col: u32,
        formula: String,
    ) -> Result<(), JsValue> {
        self.inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .set_formula(&sheet, row, col, &formula)
            .map_err(|e| js_error(format!("set_formula failed for {sheet}!R{row}C{col}: {e}")))
    }

    #[wasm_bindgen(js_name = "evaluateCell")]
    pub fn evaluate_cell(&self, sheet: String, row: u32, col: u32) -> Result<JsValue, JsValue> {
        let v = self
            .inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .evaluate_cell(&sheet, row, col)
            .map_err(|e| {
                js_error(format!(
                    "evaluate_cell failed for {sheet}!R{row}C{col}: {e}"
                ))
            })?;
        Ok(literal_to_js(v))
    }

    #[wasm_bindgen(js_name = "evaluateAll")]
    pub fn evaluate_all(&self) -> Result<(), JsValue> {
        let mut wb = self
            .inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?;
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);
        wb.evaluate_all_cancellable(self.cancel_flag.clone())
            .map_err(|e| js_error(format!("evaluate_all failed: {e}")))?;
        Ok(())
    }

    #[wasm_bindgen(js_name = "evaluateCells")]
    pub fn evaluate_cells(&self, targets: js_sys::Array) -> Result<js_sys::Array, JsValue> {
        let mut target_vec = Vec::with_capacity(targets.length() as usize);
        let mut sheet_names = Vec::new(); // Keep strings alive
        for i in 0..targets.length() {
            let item = targets.get(i);
            let arr: js_sys::Array = item.into();
            let sheet = arr
                .get(0)
                .as_string()
                .ok_or_else(|| js_error(format!("invalid sheet name at index {i}")))?;
            let _row = arr
                .get(1)
                .as_f64()
                .ok_or_else(|| js_error(format!("invalid row at index {i}")))?
                as u32;
            let _col = arr
                .get(2)
                .as_f64()
                .ok_or_else(|| js_error(format!("invalid col at index {i}")))?
                as u32;
            sheet_names.push(sheet);
        }

        for (i, name) in sheet_names.iter().enumerate() {
            let arr: js_sys::Array = targets.get(i as u32).into();
            let row = arr.get(1).as_f64().unwrap() as u32;
            let col = arr.get(2).as_f64().unwrap() as u32;
            target_vec.push((name.as_str(), row, col));
        }

        let mut wb = self
            .inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?;
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let results = wb
            .evaluate_cells_cancellable(&target_vec, self.cancel_flag.clone())
            .map_err(|e| js_error(format!("evaluate_cells failed: {e}")))?;

        let out = js_sys::Array::new();
        for v in results {
            out.push(&literal_to_js(v));
        }
        Ok(out)
    }

    #[wasm_bindgen(js_name = "getEvalPlan")]
    pub fn get_eval_plan(&self, targets: js_sys::Array) -> Result<JsValue, JsValue> {
        let mut target_vec = Vec::with_capacity(targets.length() as usize);
        for i in 0..targets.length() {
            let item = targets.get(i);
            let arr: js_sys::Array = item.into();
            let sheet = arr
                .get(0)
                .as_string()
                .ok_or_else(|| js_error(format!("invalid sheet name at index {i}")))?;
            let row = arr
                .get(1)
                .as_f64()
                .ok_or_else(|| js_error(format!("invalid row at index {i}")))?
                as u32;
            let col = arr
                .get(2)
                .as_f64()
                .ok_or_else(|| js_error(format!("invalid col at index {i}")))?
                as u32;
            if row == 0 || col == 0 {
                return Err(js_error(format!(
                    "row/col are 1-based at index {i} (row={row}, col={col})"
                )));
            }
            target_vec.push((sheet, row, col));
        }

        let refs: Vec<(&str, u32, u32)> = target_vec
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let wb = self
            .inner
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;
        let plan = wb
            .get_eval_plan(&refs)
            .map_err(|e| js_error(format!("get_eval_plan failed: {e}")))?;
        eval_plan_to_js(&plan)
    }

    #[wasm_bindgen]
    pub fn cancel(&self) {
        self.cancel_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[wasm_bindgen(js_name = "resetCancel")]
    pub fn reset_cancel(&self) {
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    // ----- Changelog / Undo / Redo -----
    #[wasm_bindgen(js_name = "setChangelogEnabled")]
    pub fn set_changelog_enabled(&self, enabled: bool) -> Result<(), JsValue> {
        let mut wb = self
            .inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?;
        wb.set_changelog_enabled(enabled);
        Ok(())
    }

    #[wasm_bindgen(js_name = "beginAction")]
    pub fn begin_action(&self, description: String) -> Result<(), JsValue> {
        let mut wb = self
            .inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?;
        wb.begin_action(description);
        Ok(())
    }

    #[wasm_bindgen(js_name = "endAction")]
    pub fn end_action(&self) -> Result<(), JsValue> {
        let mut wb = self
            .inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?;
        wb.end_action();
        Ok(())
    }

    #[wasm_bindgen]
    pub fn undo(&self) -> Result<(), JsValue> {
        self.inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .undo()
            .map_err(|e| js_error(format!("undo failed: {e}")))
    }

    #[wasm_bindgen]
    pub fn redo(&self) -> Result<(), JsValue> {
        self.inner
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .redo()
            .map_err(|e| js_error(format!("redo failed: {e}")))
    }

    pub(crate) fn inner_arc(&self) -> Arc<RwLock<formualizer::workbook::Workbook>> {
        Arc::clone(&self.inner)
    }
}

#[wasm_bindgen]
pub struct Sheet {
    wb: Arc<RwLock<formualizer::workbook::Workbook>>,
    name: String,
}

#[wasm_bindgen]
impl Sheet {
    #[wasm_bindgen(js_name = "setValue")]
    pub fn set_value(&self, row: u32, col: u32, value: JsValue) -> Result<(), JsValue> {
        let lv = js_to_literal(&value);
        self.wb
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .set_value(&self.name, row, col, lv)
            .map_err(|e| {
                js_error(format!(
                    "set_value failed for {sheet}!R{row}C{col}: {e}",
                    sheet = self.name
                ))
            })
    }

    #[wasm_bindgen(js_name = "setFormula")]
    pub fn set_formula(&self, row: u32, col: u32, formula: String) -> Result<(), JsValue> {
        self.wb
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .set_formula(&self.name, row, col, &formula)
            .map_err(|e| {
                js_error(format!(
                    "set_formula failed for {sheet}!R{row}C{col}: {e}",
                    sheet = self.name
                ))
            })
    }

    #[wasm_bindgen(js_name = "getValue")]
    pub fn get_value(&self, row: u32, col: u32) -> Result<JsValue, JsValue> {
        let v = self
            .wb
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?
            .get_value(&self.name, row, col)
            .unwrap_or(formualizer::LiteralValue::Empty);
        Ok(literal_to_js(v))
    }

    #[wasm_bindgen(js_name = "getFormula")]
    pub fn get_formula(&self, row: u32, col: u32) -> Option<String> {
        self.wb.read().ok()?.get_formula(&self.name, row, col)
    }

    #[wasm_bindgen(js_name = "setValues")]
    pub fn set_values(
        &self,
        start_row: u32,
        start_col: u32,
        data: js_sys::Array,
    ) -> Result<(), JsValue> {
        // data: Array<Array<any>>
        let mut rows: Vec<Vec<formualizer::LiteralValue>> =
            Vec::with_capacity(data.length() as usize);
        for r in 0..data.length() {
            let row_val = data.get(r);
            let row_arr: js_sys::Array = row_val.into();
            let mut row_vec = Vec::with_capacity(row_arr.length() as usize);
            for c in 0..row_arr.length() {
                row_vec.push(js_to_literal(&row_arr.get(c)));
            }
            rows.push(row_vec);
        }
        let mut wb = self
            .wb
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?;
        wb.begin_action("batch: set values".to_string());
        let res = wb
            .set_values(&self.name, start_row, start_col, &rows)
            .map_err(|e| {
                js_error(format!(
                    "set_values failed for {sheet}!R{start_row}C{start_col}: {e}",
                    sheet = self.name
                ))
            });
        wb.end_action();
        res
    }

    #[wasm_bindgen(js_name = "setFormulas")]
    pub fn set_formulas(
        &self,
        start_row: u32,
        start_col: u32,
        data: js_sys::Array,
    ) -> Result<(), JsValue> {
        // data: Array<Array<string>>
        let mut rows: Vec<Vec<String>> = Vec::with_capacity(data.length() as usize);
        for r in 0..data.length() {
            let row_val = data.get(r);
            let row_arr: js_sys::Array = row_val.into();
            let mut row_vec = Vec::with_capacity(row_arr.length() as usize);
            for c in 0..row_arr.length() {
                let s = row_arr.get(c).as_string().unwrap_or_default();
                row_vec.push(s);
            }
            rows.push(row_vec);
        }
        let mut wb = self
            .wb
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?;
        wb.begin_action("batch: set formulas".to_string());
        let res = wb
            .set_formulas(&self.name, start_row, start_col, &rows)
            .map_err(|e| {
                js_error(format!(
                    "set_formulas failed for {sheet}!R{start_row}C{start_col}: {e}",
                    sheet = self.name
                ))
            });
        wb.end_action();
        res
    }

    #[wasm_bindgen(js_name = "evaluateCell")]
    pub fn evaluate_cell(&self, row: u32, col: u32) -> Result<JsValue, JsValue> {
        let v = self
            .wb
            .write()
            .map_err(|_| js_error("failed to lock workbook for write"))?
            .evaluate_cell(&self.name, row, col)
            .map_err(|e| {
                js_error(format!(
                    "evaluate_cell failed for {sheet}!R{row}C{col}: {e}",
                    sheet = self.name
                ))
            })?;
        Ok(literal_to_js(v))
    }

    /// Read a rectangular range of values as a 2D array (no evaluation)
    #[wasm_bindgen(js_name = "readRange")]
    pub fn read_range(
        &self,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> Result<js_sys::Array, JsValue> {
        let addr = formualizer::workbook::RangeAddress::new(
            &self.name, start_row, start_col, end_row, end_col,
        )
        .map_err(JsValue::from)?;
        let vals = self
            .wb
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?
            .read_range(&addr);
        let outer = js_sys::Array::new();
        for row in vals {
            let arr = js_sys::Array::new();
            for v in row {
                arr.push(&literal_to_js(v));
            }
            outer.push(&arr);
        }
        Ok(outer)
    }
}
