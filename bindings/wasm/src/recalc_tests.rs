//! Boundary cases requiring native error/state construction, still run in Node.
use crate::{Workbook, recalc::engine_error};
use formualizer::eval::engine::{EvaluationStop, RecalcJobError};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;

fn code(value: &JsValue) -> String {
    js_sys::Reflect::get(value, &"code".into())
        .unwrap()
        .as_string()
        .unwrap()
}

#[wasm_bindgen_test]
fn raw_lifecycle_errors_distinguish_poison_and_implicit_pending() {
    assert_eq!(
        code(&engine_error(RecalcJobError::Poisoned)),
        "ENGINE_POISONED"
    );
    assert_eq!(
        code(&engine_error(RecalcJobError::NoActiveJob)),
        "NO_ACTIVE_JOB"
    );
    let wb = Workbook::new(None).unwrap();
    wb.add_sheet("S".into()).unwrap();
    wb.set_value("S".into(), 1, 1, 1.0.into()).unwrap();
    for row in 2..=20 {
        wb.set_formula("S".into(), row, 1, format!("=A{}+1", row - 1))
            .unwrap();
    }
    wb.inner
        .write()
        .unwrap()
        .evaluate_all_until(EvaluationStop::after(std::time::Duration::ZERO))
        .unwrap();
    let error = wb.begin_recalc(None).err().unwrap();
    assert_eq!(code(&error), "ALREADY_PENDING");
    assert_eq!(
        js_sys::Reflect::get(&error, &"explicit".into())
            .unwrap()
            .as_bool(),
        Some(false)
    );
}
