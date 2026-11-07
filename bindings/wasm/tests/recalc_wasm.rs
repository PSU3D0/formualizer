#![cfg(target_arch = "wasm32")]
use formualizer_wasm::Workbook;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

fn field(value: &JsValue, key: &str) -> JsValue {
    js_sys::Reflect::get(value, &key.into()).unwrap()
}
fn workbook() -> Workbook {
    let wb = Workbook::new(None).unwrap();
    wb.add_sheet("S".into()).unwrap();
    wb.set_formula("S".into(), 1, 1, "=1+2".into()).unwrap();
    wb
}
#[wasm_bindgen_test]
fn raw_job_owns_engine_until_finished() {
    let wb = workbook();
    let job = wb.begin_recalc(None).unwrap();
    assert!(job.active().unwrap(), "beginRecalc must admit a real engine job");
    assert!(wb.has_active_recalc_job().unwrap());
    let report = job.step(None).unwrap();
    assert_eq!(field(&report, "phase").as_string().as_deref(), Some("Finished"));
    assert!(!job.active().unwrap());
    assert_eq!(wb.sheet("S".into()).unwrap().get_value(1, 1).unwrap().as_f64(), Some(3.0));
}
#[wasm_bindgen_test]
fn raw_job_refuses_foreign_evaluation() {
    let wb = workbook();
    let _job = wb.begin_recalc(None).unwrap();
    let error = wb.evaluate_all().expect_err("foreign evaluation must refuse");
    assert_eq!(field(&error, "code").as_string().as_deref(), Some("RECALC_JOB_ACTIVE"));
}
