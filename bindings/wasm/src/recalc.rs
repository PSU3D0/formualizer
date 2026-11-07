use crate::Workbook;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct RecalcJob;

#[wasm_bindgen]
impl RecalcJob {
    pub fn step(&self, _budget_ms: Option<f64>) -> Result<JsValue, JsValue> {
        Err(js_sys::Error::new("raw job binding not implemented").into())
    }
    pub fn prioritize(&self, _ranges: JsValue) -> Result<(), JsValue> { Ok(()) }
    pub fn cancel(&self) -> Result<JsValue, JsValue> { Ok(JsValue::NULL) }
    #[wasm_bindgen(getter)]
    pub fn active(&self) -> Result<bool, JsValue> { Ok(false) }
}
#[wasm_bindgen]
impl Workbook {
    #[wasm_bindgen(js_name = beginRecalc)]
    pub fn begin_recalc(&self, _options: Option<JsValue>) -> Result<RecalcJob, JsValue> { Ok(RecalcJob) }
    #[wasm_bindgen(getter, js_name = hasActiveRecalcJob)]
    pub fn has_active_recalc_job(&self) -> Result<bool, JsValue> { Ok(false) }
    #[wasm_bindgen(js_name = hasPendingInRect)]
    pub fn has_pending_in_rect(&self, _area: JsValue) -> Result<bool, JsValue> { Ok(false) }
    #[wasm_bindgen(js_name = pendingCellsInRect)]
    pub fn pending_cells_in_rect(&self, _area: JsValue) -> Result<JsValue, JsValue> { Ok(js_sys::Array::new().into()) }
}
