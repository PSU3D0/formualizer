use std::sync::{Arc, RwLock};
use wasm_bindgen::prelude::*;

pub(crate) fn js_to_literal(value: &JsValue) -> formualizer_common::LiteralValue {
    use formualizer_common::LiteralValue;
    if value.is_null() || value.is_undefined() {
        LiteralValue::Empty
    } else if let Some(b) = value.as_bool() {
        LiteralValue::Boolean(b)
    } else if let Some(s) = value.as_string() {
        LiteralValue::Text(s)
    } else if let Some(n) = value.as_f64() {
        if n.fract() == 0.0 && n.is_finite() && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            LiteralValue::Int(n as i64)
        } else {
            LiteralValue::Number(n)
        }
    } else {
        // Fallback string representation for unsupported objects
        LiteralValue::Text(format!("{value:?}"))
    }
}

pub(crate) fn literal_to_js(v: formualizer_common::LiteralValue) -> JsValue {
    match v {
        formualizer_common::LiteralValue::Empty => JsValue::NULL,
        formualizer_common::LiteralValue::Boolean(b) => JsValue::from_bool(b),
        formualizer_common::LiteralValue::Int(i) => JsValue::from_f64(i as f64),
        formualizer_common::LiteralValue::Number(n) => JsValue::from_f64(n),
        formualizer_common::LiteralValue::Text(s) => JsValue::from_str(&s),
        formualizer_common::LiteralValue::Date(d) => JsValue::from_str(&d.to_string()),
        formualizer_common::LiteralValue::DateTime(dt) => JsValue::from_str(&dt.to_string()),
        formualizer_common::LiteralValue::Time(t) => JsValue::from_str(&t.to_string()),
        formualizer_common::LiteralValue::Duration(dur) => JsValue::from_str(&format!("{dur:?}")),
        formualizer_common::LiteralValue::Array(values) => {
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
        formualizer_common::LiteralValue::Pending => JsValue::from_str("Pending"),
        formualizer_common::LiteralValue::Error(err) => JsValue::from_str(&err.to_string()),
    }
}

fn set(obj: &js_sys::Object, key: &str, value: JsValue) -> Result<(), JsValue> {
    js_sys::Reflect::set(obj, &JsValue::from_str(key), &value)
        .map_err(|_| JsValue::from_str("object set failed"))
}

fn eval_plan_to_js(
    plan: &formualizer_eval::engine::eval::EvalPlan,
) -> Result<JsValue, JsValue> {
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
    inner: Arc<RwLock<formualizer_workbook::Workbook>>,
    cancel_flag: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for Workbook {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(formualizer_workbook::Workbook::new())),
            cancel_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

impl Clone for Workbook {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cancel_flag: Arc::clone(&self.cancel_flag),
        }
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
            use formualizer_workbook::backends::JsonAdapter;
            use formualizer_workbook::traits::SpreadsheetReader;
            let adapter = <JsonAdapter as SpreadsheetReader>::open_bytes(json.into_bytes())
                .map_err(|e| JsValue::from_str(&format!("open failed: {e}")))?;
            let cfg = formualizer_workbook::WorkbookConfig::interactive();
            let wb = formualizer_workbook::Workbook::from_reader(
                adapter,
                formualizer_workbook::LoadStrategy::EagerAll,
                cfg,
            )
            .map_err(|e| JsValue::from_str(&format!("load failed: {e}")))?;
            Ok(Workbook {
                inner: Arc::new(RwLock::new(wb)),
                cancel_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            })
        }
        #[cfg(not(feature = "json"))]
        {
            let _ = json;
            Err(JsValue::from_str("json feature not enabled"))
        }
    }

    #[wasm_bindgen(js_name = "addSheet")]
    pub fn add_sheet(&self, name: String) {
        if let Ok(mut wb) = self.inner.write() {
            wb.add_sheet(&name);
        }
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

    /// Get a sheet facade by name (creates if missing)
    #[wasm_bindgen(js_name = "sheet")]
    pub fn sheet(&self, name: String) -> Sheet {
        if let Ok(mut wb) = self.inner.write() {
            wb.add_sheet(&name);
        }
        Sheet {
            wb: self.inner.clone(),
            name,
        }
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
            .map_err(|_| JsValue::from_str("lock"))?
            .set_value(&sheet, row, col, lv)
            .map_err(|e| JsValue::from_str(&e.to_string()))
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
            .map_err(|_| JsValue::from_str("lock"))?
            .set_formula(&sheet, row, col, &formula)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "evaluateCell")]
    pub fn evaluate_cell(&self, sheet: String, row: u32, col: u32) -> Result<JsValue, JsValue> {
        let v = self
            .inner
            .write()
            .map_err(|_| JsValue::from_str("lock"))?
            .evaluate_cell(&sheet, row, col)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(literal_to_js(v))
    }

    #[wasm_bindgen(js_name = "evaluateAll")]
    pub fn evaluate_all(&self) -> Result<(), JsValue> {
        let mut wb = self.inner.write().map_err(|_| JsValue::from_str("lock"))?;
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);
        wb.evaluate_all_cancellable(self.cancel_flag.clone())
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
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
                .ok_or_else(|| JsValue::from_str("Invalid sheet name"))?;
            let _row = arr
                .get(1)
                .as_f64()
                .ok_or_else(|| JsValue::from_str("Invalid row"))? as u32;
            let _col = arr
                .get(2)
                .as_f64()
                .ok_or_else(|| JsValue::from_str("Invalid col"))? as u32;
            sheet_names.push(sheet);
        }

        for (i, name) in sheet_names.iter().enumerate() {
            let arr: js_sys::Array = targets.get(i as u32).into();
            let row = arr.get(1).as_f64().unwrap() as u32;
            let col = arr.get(2).as_f64().unwrap() as u32;
            target_vec.push((name.as_str(), row, col));
        }

        let mut wb = self.inner.write().map_err(|_| JsValue::from_str("lock"))?;
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let results = wb
            .evaluate_cells_cancellable(&target_vec, self.cancel_flag.clone())
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

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
                .ok_or_else(|| JsValue::from_str("Invalid sheet name"))?;
            let row = arr
                .get(1)
                .as_f64()
                .ok_or_else(|| JsValue::from_str("Invalid row"))? as u32;
            let col = arr
                .get(2)
                .as_f64()
                .ok_or_else(|| JsValue::from_str("Invalid col"))? as u32;
            if row == 0 || col == 0 {
                return Err(JsValue::from_str("Row/col are 1-based"));
            }
            target_vec.push((sheet, row, col));
        }

        let refs: Vec<(&str, u32, u32)> = target_vec
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let wb = self.inner.read().map_err(|_| JsValue::from_str("lock"))?;
        let plan = wb
            .get_eval_plan(&refs)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
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
        let mut wb = self.inner.write().map_err(|_| JsValue::from_str("lock"))?;
        wb.set_changelog_enabled(enabled);
        Ok(())
    }

    #[wasm_bindgen(js_name = "beginAction")]
    pub fn begin_action(&self, description: String) -> Result<(), JsValue> {
        let mut wb = self.inner.write().map_err(|_| JsValue::from_str("lock"))?;
        wb.begin_action(description);
        Ok(())
    }

    #[wasm_bindgen(js_name = "endAction")]
    pub fn end_action(&self) -> Result<(), JsValue> {
        let mut wb = self.inner.write().map_err(|_| JsValue::from_str("lock"))?;
        wb.end_action();
        Ok(())
    }

    #[wasm_bindgen]
    pub fn undo(&self) -> Result<(), JsValue> {
        self.inner
            .write()
            .map_err(|_| JsValue::from_str("lock"))?
            .undo()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen]
    pub fn redo(&self) -> Result<(), JsValue> {
        self.inner
            .write()
            .map_err(|_| JsValue::from_str("lock"))?
            .redo()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    pub(crate) fn inner_arc(&self) -> Arc<RwLock<formualizer_workbook::Workbook>> {
        Arc::clone(&self.inner)
    }
}

#[wasm_bindgen]
pub struct Sheet {
    wb: Arc<RwLock<formualizer_workbook::Workbook>>,
    name: String,
}

#[wasm_bindgen]
impl Sheet {
    #[wasm_bindgen(js_name = "setValue")]
    pub fn set_value(&self, row: u32, col: u32, value: JsValue) -> Result<(), JsValue> {
        let lv = js_to_literal(&value);
        self.wb
            .write()
            .map_err(|_| JsValue::from_str("lock"))?
            .set_value(&self.name, row, col, lv)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "setFormula")]
    pub fn set_formula(&self, row: u32, col: u32, formula: String) -> Result<(), JsValue> {
        self.wb
            .write()
            .map_err(|_| JsValue::from_str("lock"))?
            .set_formula(&self.name, row, col, &formula)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "getValue")]
    pub fn get_value(&self, row: u32, col: u32) -> Result<JsValue, JsValue> {
        let v = self
            .wb
            .read()
            .map_err(|_| JsValue::from_str("lock"))?
            .get_value(&self.name, row, col)
            .unwrap_or(formualizer_common::LiteralValue::Empty);
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
        let mut rows: Vec<Vec<formualizer_common::LiteralValue>> =
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
        let mut wb = self.wb.write().map_err(|_| JsValue::from_str("lock"))?;
        wb.begin_action("batch: set values".to_string());
        let res = wb
            .set_values(&self.name, start_row, start_col, &rows)
            .map_err(|e| JsValue::from_str(&e.to_string()));
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
        let mut wb = self.wb.write().map_err(|_| JsValue::from_str("lock"))?;
        wb.begin_action("batch: set formulas".to_string());
        let res = wb
            .set_formulas(&self.name, start_row, start_col, &rows)
            .map_err(|e| JsValue::from_str(&e.to_string()));
        wb.end_action();
        res
    }

    #[wasm_bindgen(js_name = "evaluateCell")]
    pub fn evaluate_cell(&self, row: u32, col: u32) -> Result<JsValue, JsValue> {
        let v = self
            .wb
            .write()
            .map_err(|_| JsValue::from_str("lock"))?
            .evaluate_cell(&self.name, row, col)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
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
        let addr = formualizer_workbook::RangeAddress::new(
            &self.name, start_row, start_col, end_row, end_col,
        )
        .map_err(JsValue::from)?;
        let vals = self
            .wb
            .read()
            .map_err(|_| JsValue::from_str("lock"))?
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
