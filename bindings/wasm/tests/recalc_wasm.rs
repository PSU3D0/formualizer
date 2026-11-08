#![cfg(target_arch = "wasm32")]
use formualizer_wasm::{RecalcJob, Workbook};
use js_sys::{Array, Function, Object, Reflect};
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_test::*;

fn field(value: &JsValue, key: &str) -> JsValue {
    Reflect::get(value, &key.into()).unwrap()
}
fn js(value: serde_json::Value) -> JsValue {
    js_sys::JSON::parse(&value.to_string()).unwrap()
}
fn area() -> JsValue {
    js(serde_json::json!({"sheet":"S","startRow":1,"startColumn":1,"endRow":20,"endColumn":2}))
}
fn code(error: JsValue) -> String {
    field(&error, "code").as_string().unwrap()
}
fn phase(report: &JsValue) -> String {
    field(report, "phase").as_string().unwrap()
}
fn zero() -> Option<JsValue> {
    Some(0.0.into())
}
fn workbook() -> Workbook {
    let wb = Workbook::new(None).unwrap();
    wb.add_sheet("S".into()).unwrap();
    wb.set_formula("S".into(), 1, 1, "=1+2".into()).unwrap();
    wb
}
fn chain(options: Option<JsValue>) -> Workbook {
    let wb = Workbook::new(options).unwrap();
    wb.add_sheet("S".into()).unwrap();
    wb.set_value("S".into(), 1, 1, 1.0.into()).unwrap();
    for row in 2..=20 {
        wb.set_formula("S".into(), row, 1, format!("=A{}+1", row - 1))
            .unwrap();
    }
    wb
}
fn finish(job: &RecalcJob, budget: Option<JsValue>) -> Vec<JsValue> {
    let mut reports = Vec::new();
    for _ in 0..200 {
        let report = job.step(budget.clone()).unwrap();
        let terminal = phase(&report) == "Finished";
        reports.push(report);
        if terminal {
            return reports;
        }
    }
    panic!("job failed to finish");
}
#[wasm_bindgen_test]
fn raw_job_owns_engine_until_finished() {
    let wb = workbook();
    let job = wb.begin_recalc(None).unwrap();
    assert!(
        job.active().unwrap(),
        "beginRecalc must admit a real engine job"
    );
    assert!(wb.has_active_recalc_job().unwrap());
    let report = job.step(None).unwrap();
    assert_eq!(phase(&report), "Finished");
    assert!(!job.active().unwrap());
    assert_eq!(
        wb.sheet("S".into())
            .unwrap()
            .get_value(1, 1)
            .unwrap()
            .as_f64(),
        Some(3.0)
    );
}
#[wasm_bindgen_test]
fn raw_job_refuses_foreign_evaluation() {
    let wb = workbook();
    let job = wb.begin_recalc(None).unwrap();
    assert_eq!(code(wb.evaluate_all().unwrap_err()), "RECALC_JOB_ACTIVE");
    assert_eq!(
        code(wb.evaluate_cell("S".into(), 1, 1).unwrap_err()),
        "RECALC_JOB_ACTIVE"
    );
    let sheet = wb.sheet("S".into()).unwrap();
    assert_eq!(
        code(sheet.evaluate_cell(1, 1).unwrap_err()),
        "RECALC_JOB_ACTIVE"
    );
    let targets = Array::new();
    let target = Array::new();
    target.push(&"S".into());
    target.push(&1.into());
    target.push(&1.into());
    targets.push(&target);
    assert_eq!(
        code(wb.evaluate_cells(targets).unwrap_err()),
        "RECALC_JOB_ACTIVE"
    );
    let error = wb.begin_recalc(None).err().unwrap();
    assert_eq!(field(&error, "explicit").as_bool(), Some(true));
    assert_eq!(code(error), "ALREADY_PENDING");
    job.cancel().unwrap();
    wb.evaluate_all().unwrap();
}
#[wasm_bindgen_test]
fn raw_zero_budget_progress_matches_full_evaluation_and_reports_detach() {
    let wb = chain(None);
    let direct = chain(None);
    direct.evaluate_all().unwrap();
    let job = wb.begin_recalc(None).unwrap();
    let first = job.step(zero()).unwrap();
    let before = js_sys::JSON::stringify(&first).unwrap();
    assert!(field(&first, "result").is_null());
    let reports = finish(&job, zero());
    assert_eq!(js_sys::JSON::stringify(&first).unwrap(), before);
    assert!(reports.len() >= 2);
    let last = reports.last().unwrap();
    assert_eq!(
        field(&field(last, "result"), "computedVertices").as_f64(),
        Some(19.0)
    );
    let progress = field(last, "progress");
    for key in [
        "pass",
        "completedInPass",
        "scheduledInPass",
        "cumulative",
        "stagedRemaining",
    ] {
        assert!(field(&progress, key).as_f64().is_some(), "{key}");
    }
    assert!(field(&progress, "totalExact").as_bool().is_some());
    assert!(field(&progress, "priorityPending").as_bool().is_some());
    assert!(!last.is_instance_of::<js_sys::Map>());
    let unit = field(last, "longestUnit");
    assert!(field(&unit, "kind").as_string().is_some());
    assert!(field(&unit, "elapsedMs").as_f64().unwrap() >= 0.0);
    let cell = field(&unit, "representativeCell");
    if !cell.is_null() {
        assert_eq!(field(&cell, "sheet").as_string().as_deref(), Some("S"));
        assert!(field(&cell, "row").as_f64().unwrap() >= 1.0);
        assert!(field(&cell, "column").as_f64().unwrap() >= 1.0);
    }
    Reflect::set(&progress, &"cumulative".into(), &99999.into()).unwrap();
    assert!(!job.active().unwrap());
    for row in 1..=20 {
        assert_eq!(
            wb.sheet("S".into()).unwrap().get_value(row, 1).unwrap(),
            direct.sheet("S".into()).unwrap().get_value(row, 1).unwrap()
        );
    }
}
#[wasm_bindgen_test]
fn raw_null_and_undefined_budgets_are_unbounded() {
    for budget in [None, Some(JsValue::NULL), Some(JsValue::UNDEFINED)] {
        let wb = chain(None);
        let job = wb.begin_recalc(None).unwrap();
        assert_eq!(phase(&job.step(budget).unwrap()), "Finished");
    }
}
#[wasm_bindgen_test]
fn raw_invalid_budgets_do_not_end_the_job() {
    let wb = workbook();
    let job = wb.begin_recalc(None).unwrap();
    for value in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MAX] {
        assert_eq!(
            code(job.step(Some(value.into())).unwrap_err()),
            "INVALID_OPTIONS"
        );
        assert!(job.active().unwrap());
    }
    for value in [JsValue::TRUE, "1".into(), Array::new().into()] {
        assert_eq!(code(job.step(Some(value)).unwrap_err()), "INVALID_OPTIONS");
    }
    assert_eq!(phase(&job.step(None).unwrap()), "Finished");
}
#[wasm_bindgen_test]
fn raw_options_validate_before_admission() {
    let wb = workbook();
    for value in [
        js(serde_json::json!({"unknown":true})),
        js(serde_json::json!({"collectDeltas":1})),
        js(serde_json::json!({"maxDeltaRecords":-1})),
        js(serde_json::json!({"maxDeltaRecords":1.5})),
        js(serde_json::json!({"maxDeltaRecords":4294967296_u64})),
        js(serde_json::json!({"priority":{}})),
        JsValue::TRUE,
        Array::new().into(),
    ] {
        assert_eq!(
            code(wb.begin_recalc(Some(value)).err().unwrap()),
            "INVALID_OPTIONS"
        );
        assert!(!wb.has_active_recalc_job().unwrap());
    }
    let symbol = Object::new();
    Reflect::set(&symbol, &js_sys::Symbol::iterator(), &true.into()).unwrap();
    assert_eq!(
        code(wb.begin_recalc(Some(symbol.into())).err().unwrap()),
        "INVALID_OPTIONS"
    );
    let job = wb.begin_recalc(None).unwrap();
    assert!(job.active().unwrap());
}
#[wasm_bindgen_test]
fn raw_rectangles_reject_wrap_fraction_open_and_unknown_fields() {
    let wb = workbook();
    for (key, value) in [
        ("startRow", JsValue::from_f64(0.0)),
        ("startRow", JsValue::from_f64(-1.0)),
        ("startRow", JsValue::from_f64(4294967297.0)),
        ("startRow", JsValue::from_f64(1.25)),
        ("endRow", JsValue::from_f64(1048577.0)),
        ("endColumn", JsValue::from_f64(16385.0)),
        ("startColumn", JsValue::from_f64(f64::NAN)),
        ("endColumn", JsValue::from_f64(f64::INFINITY)),
        ("endRow", JsValue::UNDEFINED),
        ("endColumn", JsValue::NULL),
        ("surprise", JsValue::TRUE),
        ("sheet", JsValue::from_str("Missing")),
    ] {
        let rect = area();
        Reflect::set(&rect, &key.into(), &value).unwrap();
        assert_eq!(
            code(wb.has_pending_in_rect(rect.clone()).unwrap_err()),
            "INVALID_ADDRESS"
        );
        let list = Array::new();
        list.push(&rect);
        let options = Object::new();
        Reflect::set(&options, &"priority".into(), &list).unwrap();
        assert_eq!(
            code(wb.begin_recalc(Some(options.into())).err().unwrap()),
            "INVALID_ADDRESS"
        );
        assert!(!wb.has_active_recalc_job().unwrap());
    }
    let rect = area();
    Reflect::set(&rect, &"startRow".into(), &21.into()).unwrap();
    assert_eq!(
        code(wb.pending_cells_in_rect(rect).unwrap_err()),
        "INVALID_ADDRESS"
    );
}
#[wasm_bindgen_test]
fn raw_pending_and_changed_are_bounded_named_objects() {
    let wb = chain(None);
    let viewport = area();
    Reflect::set(&viewport, &"startRow".into(), &3.into()).unwrap();
    Reflect::set(&viewport, &"endRow".into(), &5.into()).unwrap();
    assert!(wb.has_pending_in_rect(viewport.clone()).unwrap());
    let pending = Array::from(&wb.pending_cells_in_rect(viewport.clone()).unwrap());
    assert_eq!(pending.length(), 3);
    for (index, cell) in pending.iter().enumerate() {
        assert_eq!(field(&cell, "sheet").as_string().as_deref(), Some("S"));
        assert_eq!(field(&cell, "row").as_f64(), Some(index as f64 + 3.0));
        assert_eq!(field(&cell, "column").as_f64(), Some(1.0));
    }
    let job = wb.begin_recalc(None).unwrap();
    let report = job.step(None).unwrap();
    let changed = Array::from(&field(&report, "changed"));
    assert!(changed.length() > 0);
    for rect in changed {
        assert_eq!(field(&rect, "sheet").as_string().as_deref(), Some("S"));
        for key in ["startRow", "startColumn", "endRow", "endColumn"] {
            assert!(field(&rect, key).as_f64().unwrap() >= 1.0);
        }
        assert!(field(&rect, "sheetId").is_undefined());
    }
    assert!(!wb.has_pending_in_rect(viewport.clone()).unwrap());
    assert_eq!(
        Array::from(&wb.pending_cells_in_rect(viewport).unwrap()).length(),
        0
    );
}
#[wasm_bindgen_test]
fn raw_deltas_can_be_disabled_or_overflow_to_sheet_names() {
    for collect in [false, true] {
        let wb = workbook();
        let job = wb
            .begin_recalc(Some(js(serde_json::json!({
                "collectDeltas":collect,"maxDeltaRecords":0
            }))))
            .unwrap();
        let report = job.step(None).unwrap();
        assert_eq!(Array::from(&field(&report, "changed")).length(), 0);
        let overflow = Array::from(&field(&report, "overflowSheets"));
        assert_eq!(overflow.length(), u32::from(collect));
        if collect {
            assert_eq!(overflow.get(0).as_string().as_deref(), Some("S"));
        }
    }
}
#[wasm_bindgen_test]
fn raw_edit_and_priority_replan_without_losing_job_identity() {
    let wb = chain(None);
    let list = Array::new();
    list.push(&area());
    let options = Object::new();
    Reflect::set(&options, &"priority".into(), &list).unwrap();
    let job = wb.begin_recalc(Some(options.into())).unwrap();
    let first = job.step(zero()).unwrap();
    assert_eq!(
        field(&field(&first, "progress"), "priorityPending").as_bool(),
        Some(true)
    );
    let before = js_sys::JSON::stringify(&first).unwrap();
    let pass = loop {
        let report = job.step(zero()).unwrap();
        if field(&field(&report, "progress"), "cumulative")
            .as_f64()
            .unwrap()
            > 0.0
        {
            break field(&field(&report, "progress"), "pass").as_f64().unwrap();
        }
    };
    wb.set_value("S".into(), 1, 1, 10.0.into()).unwrap();
    job.prioritize(list.into()).unwrap();
    let reports = finish(&job, zero());
    assert_eq!(phase(reports.last().unwrap()), "Finished");
    assert!(
        field(&field(reports.last().unwrap(), "progress"), "pass")
            .as_f64()
            .unwrap()
            > pass
    );
    assert_eq!(
        wb.sheet("S".into())
            .unwrap()
            .get_value(20, 1)
            .unwrap()
            .as_f64(),
        Some(29.0)
    );
    assert_eq!(js_sys::JSON::stringify(&first).unwrap(), before);
}
#[wasm_bindgen_test]
fn raw_stale_handles_cannot_touch_newer_job_across_clones() {
    let wb = workbook();
    let clone = wb.clone();
    let old = wb.begin_recalc(None).unwrap();
    assert_eq!(
        code(clone.begin_recalc(None).err().unwrap()),
        "ALREADY_PENDING"
    );
    let cancelled = old.cancel().unwrap();
    assert_eq!(phase(&cancelled), "Cancelled");
    assert!(field(&cancelled, "result").is_null());
    let newer = clone.begin_recalc(None).unwrap();
    assert!(!old.active().unwrap());
    assert_eq!(code(old.step(None).unwrap_err()), "NO_ACTIVE_JOB");
    assert_eq!(
        code(old.prioritize(Array::new().into()).unwrap_err()),
        "NO_ACTIVE_JOB"
    );
    assert_eq!(code(old.cancel().unwrap_err()), "NO_ACTIVE_JOB");
    assert!(newer.active().unwrap());
    assert_eq!(phase(&newer.step(None).unwrap()), "Finished");
    assert_eq!(code(newer.cancel().unwrap_err()), "NO_ACTIVE_JOB");
}
#[wasm_bindgen_test]
fn raw_cancel_flags_refusal_explicit_reset_and_fresh_admission() {
    let wb = workbook();
    let job = wb.begin_recalc(None).unwrap();
    wb.cancel();
    assert!(job.cancel_requested());
    assert_eq!(code(wb.evaluate_all().unwrap_err()), "RECALC_JOB_ACTIVE");
    assert_eq!(
        code(wb.evaluate_cells(Array::new()).unwrap_err()),
        "RECALC_JOB_ACTIVE"
    );
    assert_eq!(
        code(wb.begin_recalc(None).err().unwrap()),
        "ALREADY_PENDING"
    );
    assert!(job.cancel_requested());
    assert_eq!(phase(&job.step(None).unwrap()), "Cancelled");
    assert!(wb.has_pending_in_rect(area()).unwrap());
    let next = wb.begin_recalc(None).unwrap();
    assert!(!next.cancel_requested());
    wb.cancel();
    wb.reset_cancel();
    assert!(!next.cancel_requested());
    assert_eq!(phase(&next.step(None).unwrap()), "Finished");
}
#[wasm_bindgen_test]
fn raw_driver_reservation_survives_terminal_step_and_stale_cleanup() {
    let wb = workbook();
    let alias = wb.clone();
    let job = wb.begin_recalc_driver(None).unwrap();
    assert_eq!(phase(&job.step(None).unwrap()), "Finished");
    assert!(!job.active().unwrap());
    assert!(!wb.has_active_recalc_job().unwrap());
    wb.cancel();
    assert!(job.cancel_requested());
    assert_eq!(
        code(alias.begin_recalc(None).err().unwrap()),
        "ALREADY_PENDING"
    );
    assert_eq!(
        code(alias.begin_recalc_driver(None).err().unwrap()),
        "ALREADY_PENDING"
    );
    assert_eq!(code(alias.evaluate_all().unwrap_err()), "RECALC_JOB_ACTIVE");
    assert!(job.cancel_requested());
    assert!(job.release_driver().unwrap());
    assert!(!job.cancel_requested());
    let next = alias.begin_recalc_driver(None).unwrap();
    wb.cancel();
    assert!(!job.release_driver().unwrap());
    assert!(!job.cancel_requested());
    assert!(next.cancel_requested());
    next.cancel().unwrap();
    assert!(next.release_driver().unwrap());
}
#[wasm_bindgen_test]
fn raw_driver_pause_release_keeps_engine_job_active() {
    let wb = chain(None);
    let job = wb.begin_recalc_driver(None).unwrap();
    job.step(zero()).unwrap();
    assert!(job.release_driver().unwrap());
    assert!(job.active().unwrap());
    assert_eq!(
        code(wb.begin_recalc(None).err().unwrap()),
        "ALREADY_PENDING"
    );
    finish(&job, None);
}
#[wasm_bindgen_test]
fn raw_job_keeps_workbook_and_udf_registry_alive_after_wrapper_drop() {
    let wb = workbook();
    wb.register_function("KEEP".into(), Function::new_no_args("return 42"), None)
        .unwrap();
    wb.set_formula("S".into(), 1, 1, "=KEEP()".into()).unwrap();
    let sheet = wb.sheet("S".into()).unwrap();
    let job = wb.begin_recalc(None).unwrap();
    drop(wb);
    assert_eq!(phase(&job.step(None).unwrap()), "Finished");
    assert_eq!(sheet.get_value(1, 1).unwrap().as_f64(), Some(42.0));
}
#[wasm_bindgen_test]
fn raw_udf_reentry_fails_promptly_and_lock_free_cancel_is_honored() {
    let wb = workbook();
    let slot: Rc<RefCell<Option<Rc<RecalcJob>>>> = Rc::default();
    let inside = slot.clone();
    let alias = wb.clone();
    let callback = Closure::<dyn FnMut() -> f64>::new(move || {
        let borrowed = inside.borrow();
        let job = borrowed.as_ref().unwrap();
        assert_eq!(
            code(alias.begin_recalc(None).err().unwrap()),
            "REENTRANT_CALL"
        );
        assert_eq!(
            code(alias.has_active_recalc_job().unwrap_err()),
            "REENTRANT_CALL"
        );
        assert_eq!(
            code(alias.has_pending_in_rect(area()).unwrap_err()),
            "REENTRANT_CALL"
        );
        assert_eq!(
            code(alias.pending_cells_in_rect(area()).unwrap_err()),
            "REENTRANT_CALL"
        );
        assert_eq!(code(job.step(None).unwrap_err()), "REENTRANT_CALL");
        assert_eq!(
            code(job.prioritize(Array::new().into()).unwrap_err()),
            "REENTRANT_CALL"
        );
        assert_eq!(code(job.cancel().unwrap_err()), "REENTRANT_CALL");
        assert_eq!(code(job.active().unwrap_err()), "REENTRANT_CALL");
        alias.cancel();
        assert!(job.cancel_requested());
        7.0
    });
    wb.register_function(
        "REENTER".into(),
        callback.as_ref().unchecked_ref::<Function>().clone(),
        None,
    )
    .unwrap();
    wb.set_formula("S".into(), 1, 1, "=REENTER()".into())
        .unwrap();
    let job = Rc::new(wb.begin_recalc(None).unwrap());
    *slot.borrow_mut() = Some(job.clone());
    let report = job.step(None).unwrap();
    assert_eq!(phase(&report), "Cancelled");
    assert!(!job.active().unwrap());
    slot.borrow_mut().take();
}
#[wasm_bindgen_test]
fn raw_udf_throw_remains_an_excel_value_error() {
    let wb = workbook();
    wb.register_function(
        "THROW".into(),
        Function::new_no_args("throw new TypeError('raw callback detail')"),
        None,
    )
    .unwrap();
    wb.set_formula("S".into(), 1, 1, "=THROW()".into()).unwrap();
    let job = wb.begin_recalc(None).unwrap();
    assert_eq!(phase(&job.step(None).unwrap()), "Finished");
    assert_eq!(
        wb.sheet("S".into())
            .unwrap()
            .get_value(1, 1)
            .unwrap()
            .as_string()
            .as_deref(),
        Some("#VALUE!")
    );
    let snapshot = wb
        .inspect_cell_js(
            js(serde_json::json!({"sheet":"S","row":1,"column":1})),
            None,
        )
        .unwrap();
    let value = field(&field(&snapshot, "cell"), "value");
    assert_eq!(
        field(&value, "code").as_string().as_deref(),
        Some("#VALUE!")
    );
}
#[wasm_bindgen_test]
fn raw_resource_failure_ends_job_and_budget_accumulates_across_steps() {
    let wb = chain(Some(js(serde_json::json!({"maxWorkUnits":24}))));
    let job = wb.begin_recalc(None).unwrap();
    let mut successful = 0;
    let error = loop {
        match job.step(zero()) {
            Ok(report) => {
                assert_ne!(phase(&report), "Finished");
                successful += 1;
                assert!(successful < 100);
            }
            Err(error) => break error,
        }
    };
    assert!(successful >= 2);
    assert_eq!(
        field(&error, "name").as_string().as_deref(),
        Some("RecalcJobError")
    );
    assert_eq!(
        field(&error, "resource_reason").as_string().as_deref(),
        Some("work_units")
    );
    assert!(field(&error, "limit").as_string().is_some());
    assert!(
        field(&field(&error, "cause"), "excel_kind")
            .as_string()
            .is_some()
    );
    assert_eq!(code(error), "EVALUATION");
    assert!(!job.active().unwrap());
    assert!(!wb.has_active_recalc_job().unwrap());
    assert_eq!(code(job.step(None).unwrap_err()), "NO_ACTIVE_JOB");
    let retry = wb.begin_recalc(None).unwrap();
    assert!(retry.active().unwrap());
    retry.cancel().unwrap();
}
#[wasm_bindgen_test]
fn raw_json_loader_has_shared_identity_and_job_support() {
    let wb = Workbook::from_json(
        r#"{"sheets":{"S":{"cells":[{"row":1,"col":1,"formula":"1+2"}]}}}"#.into(),
    )
    .unwrap();
    let job = wb.clone().begin_recalc(None).unwrap();
    drop(wb);
    assert_eq!(phase(&job.step(None).unwrap()), "Finished");
}
