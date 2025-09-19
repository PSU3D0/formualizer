use std::sync::{Arc, RwLock};
use wasm_bindgen::prelude::*;

fn js_to_literal(value: &JsValue) -> formualizer_common::LiteralValue {
    use formualizer_common::LiteralValue;
    if value.is_null() || value.is_undefined() {
        LiteralValue::Empty
    } else if let Some(b) = value.as_bool() {
        LiteralValue::Boolean(b)
    } else if let Some(s) = value.as_string() {
        LiteralValue::Text(s)
    } else if let Some(n) = value.as_f64() {
        // Heuristic: integers are still numbers here; consumers can decide
        LiteralValue::Number(n)
    } else {
        LiteralValue::Empty
    }
}

fn literal_to_js(v: formualizer_common::LiteralValue) -> JsValue {
    match v {
        formualizer_common::LiteralValue::Empty => JsValue::NULL,
        formualizer_common::LiteralValue::Boolean(b) => JsValue::from_bool(b),
        formualizer_common::LiteralValue::Int(i) => JsValue::from_f64(i as f64),
        formualizer_common::LiteralValue::Number(n) => JsValue::from_f64(n),
        formualizer_common::LiteralValue::Text(s) => JsValue::from_str(&s),
        _ => JsValue::from_str(&format!("{v:?}")),
    }
}

#[wasm_bindgen]
pub struct Workbook {
    inner: Arc<RwLock<formualizer_workbook::Workbook>>,
}

impl Default for Workbook {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(formualizer_workbook::Workbook::new())),
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
            let cfg = formualizer_eval::engine::EvalConfig::default();
            let wb = formualizer_workbook::Workbook::from_reader(
                adapter,
                formualizer_workbook::LoadStrategy::EagerAll,
                cfg,
            )
            .map_err(|e| JsValue::from_str(&format!("load failed: {e}")))?;
            Ok(Workbook {
                inner: Arc::new(RwLock::new(wb)),
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
