//! Thin ownership and serialization boundary for engine-owned recalculation jobs.
use crate::Workbook;
use crate::errors::excel_error_to_js;
use formualizer::eval::engine::{self, EvaluationStop, EvaluationTarget, RecalcJobError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use std::sync::{RwLockReadGuard, RwLockWriteGuard, TryLockError};
use wasm_bindgen::prelude::*;

type CoreWorkbook = formualizer::workbook::Workbook;

/// Lifecycle/validation errors are ordinary JS Error objects with stable fields.
pub(crate) fn job_error(code: &str, message: &str) -> JsValue {
    let error = js_sys::Error::new(message);
    error.set_name("RecalcJobError");
    let _ = js_sys::Reflect::set(&error, &"kind".into(), &"RecalcJobError".into());
    let _ = js_sys::Reflect::set(&error, &"code".into(), &code.into());
    error.into()
}

pub(crate) fn engine_error(error: RecalcJobError) -> JsValue {
    let code = match &error {
        RecalcJobError::NoActiveJob => "NO_ACTIVE_JOB",
        RecalcJobError::AlreadyPending { .. } => "ALREADY_PENDING",
        RecalcJobError::Poisoned => "ENGINE_POISONED",
        RecalcJobError::Evaluation(_) => "EVALUATION",
        _ => "EVALUATION",
    };
    let out = if let RecalcJobError::Evaluation(ref source) = error {
        // Retain Excel/resource fields, and preserve a separately typed cause.
        let out = excel_error_to_js(source.clone());
        let cause = excel_error_to_js(source.clone());
        let _ = js_sys::Reflect::set(&out, &"cause".into(), &cause);
        let _ = js_sys::Reflect::set(&out, &"name".into(), &"RecalcJobError".into());
        let _ = js_sys::Reflect::set(&out, &"code".into(), &code.into());
        out
    } else {
        job_error(code, &error.to_string())
    };
    if let RecalcJobError::AlreadyPending { explicit } = error {
        let _ = js_sys::Reflect::set(&out, &"explicit".into(), &explicit.into());
    }
    out
}

fn lock_error<T>(error: TryLockError<T>) -> JsValue {
    match error {
        TryLockError::WouldBlock => job_error(
            "REENTRANT_CALL",
            "workbook is busy evaluating a host callback",
        ),
        TryLockError::Poisoned(_) => job_error("ENGINE_POISONED", "workbook lock is poisoned"),
    }
}

impl Workbook {
    pub(crate) fn job_read(&self) -> Result<RwLockReadGuard<'_, CoreWorkbook>, JsValue> {
        self.inner.try_read().map_err(lock_error)
    }
    pub(crate) fn job_write(&self) -> Result<RwLockWriteGuard<'_, CoreWorkbook>, JsValue> {
        self.inner.try_write().map_err(lock_error)
    }
}

fn invalid_options(message: &str) -> JsValue {
    job_error("INVALID_OPTIONS", message)
}
fn invalid_address(message: &str) -> JsValue {
    job_error("INVALID_ADDRESS", message)
}

fn keys(value: &JsValue, allowed: &[&str], address: bool) -> Result<(), JsValue> {
    let invalid = |message: &str| {
        if address {
            invalid_address(message)
        } else {
            invalid_options(message)
        }
    };
    if !value.is_object() || value.is_null() || js_sys::Array::is_array(value) {
        return Err(invalid("expected an object"));
    }
    for key in js_sys::Reflect::own_keys(value)? {
        if !key
            .as_string()
            .is_some_and(|name| allowed.contains(&name.as_str()))
        {
            return Err(invalid("unknown field"));
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Area {
    sheet: String,
    start_row: f64,
    start_column: f64,
    end_row: f64,
    end_column: f64,
}
impl Area {
    fn parse(value: JsValue) -> Result<Self, JsValue> {
        keys(
            &value,
            &["sheet", "startRow", "startColumn", "endRow", "endColumn"],
            true,
        )?;
        let area: Self = serde_wasm_bindgen::from_value(value)
            .map_err(|_| invalid_address("expected a bounded RangeArea"))?;
        for (number, max) in [
            (area.start_row, 1_048_576.0),
            (area.end_row, 1_048_576.0),
            (area.start_column, 16_384.0),
            (area.end_column, 16_384.0),
        ] {
            if !number.is_finite() || number.fract() != 0.0 || number < 1.0 || number > max {
                return Err(invalid_address(
                    "coordinates must be finite 1-based integers within the Excel grid",
                ));
            }
        }
        if area.sheet.is_empty()
            || area.start_row > area.end_row
            || area.start_column > area.end_column
        {
            return Err(invalid_address(
                "RangeArea must have a sheet and ordered bounds",
            ));
        }
        Ok(area)
    }
    fn target(&self) -> EvaluationTarget {
        EvaluationTarget::Range(formualizer::common::RangeAddress {
            sheet: self.sheet.clone(),
            start_row: self.start_row as u32,
            start_col: self.start_column as u32,
            end_row: self.end_row as u32,
            end_col: self.end_column as u32,
        })
    }
    fn sheet_id(&self, wb: &CoreWorkbook) -> Option<formualizer::common::SheetId> {
        wb.engine().sheet_id(&self.sheet)
    }
}

fn areas(value: JsValue) -> Result<Vec<Area>, JsValue> {
    if !js_sys::Array::is_array(&value) {
        return Err(invalid_options(
            "priority must be an array of bounded RangeArea objects",
        ));
    }
    js_sys::Array::from(&value)
        .iter()
        .map(Area::parse)
        .collect()
}

fn options(value: Option<JsValue>) -> Result<(engine::RecalcJobOptions, Vec<Area>), JsValue> {
    let mut options = engine::RecalcJobOptions::default();
    let Some(value) = value.filter(|v| !v.is_null() && !v.is_undefined()) else {
        return Ok((options, Vec::new()));
    };
    keys(
        &value,
        &["collectDeltas", "maxDeltaRecords", "priority"],
        false,
    )?;
    let get = |key: &str| js_sys::Reflect::get(&value, &key.into());
    let collect = get("collectDeltas")?;
    if !collect.is_undefined() {
        options.collect_deltas = collect
            .as_bool()
            .ok_or_else(|| invalid_options("collectDeltas must be boolean"))?;
    }
    let max = get("maxDeltaRecords")?;
    if !max.is_undefined() {
        let max = max
            .as_f64()
            .ok_or_else(|| invalid_options("maxDeltaRecords must be an integer"))?;
        if !max.is_finite() || max.fract() != 0.0 || max < 0.0 || max > isize::MAX as f64 {
            return Err(invalid_options("maxDeltaRecords exceeds host count bounds"));
        }
        options.max_delta_records = max as usize;
    }
    let priority = get("priority")?;
    let priority = if priority.is_undefined() {
        Vec::new()
    } else {
        areas(priority)?
    };
    options.initial_priority = priority.iter().map(Area::target).collect();
    Ok((options, priority))
}

fn stop(value: Option<JsValue>, workbook: &Workbook) -> Result<EvaluationStop, JsValue> {
    let stop = match value.filter(|v| !v.is_null() && !v.is_undefined()) {
        None => EvaluationStop::none(),
        Some(value) => {
            let ms = value
                .as_f64()
                .ok_or_else(|| invalid_options("budgetMs must be a number"))?;
            if !ms.is_finite() || ms < 0.0 {
                return Err(invalid_options("budgetMs must be finite and non-negative"));
            }
            let duration = std::time::Duration::try_from_secs_f64(ms / 1000.0)
                .map_err(|_| invalid_options("budgetMs exceeds duration bounds"))?;
            EvaluationStop::after(duration)
        }
    };
    Ok(stop.with_cancel(engine::CancelToken::from_flag(workbook.cancel_flag.clone())))
}

/// Owns the Workbook identity, not a borrowed JS wrapper. Dropping a handle does
/// not cancel: only an explicit operation or a terminal engine step ends a job.
#[wasm_bindgen]
pub struct RecalcJob {
    workbook: Workbook,
    generation: u64,
}
impl RecalcJob {
    fn ensure_generation(&self) -> Result<(), JsValue> {
        if self.workbook.job_generation.load(Ordering::SeqCst) != self.generation {
            return Err(engine_error(RecalcJobError::NoActiveJob));
        }
        Ok(())
    }
    fn is_current(&self, wb: &CoreWorkbook) -> bool {
        self.workbook.job_generation.load(Ordering::SeqCst) == self.generation
            && wb.has_active_recalc_job()
    }
}
#[wasm_bindgen]
impl RecalcJob {
    pub fn step(&self, budget_ms: Option<JsValue>) -> Result<JsValue, JsValue> {
        self.ensure_generation()?;
        let stop = stop(budget_ms, &self.workbook)?;
        let detached = {
            let mut wb = self.workbook.job_write()?;
            if !self.is_current(&wb) {
                drop(wb);
                return Err(engine_error(RecalcJobError::NoActiveJob));
            }
            let report = wb.step_recalc(stop);
            // Conversion to JS and all host getters/setters run after unlock.
            report.map(|report| report_value(&wb, report))
        };
        to_js(&detached.map_err(engine_error)?)
    }
    pub fn prioritize(&self, ranges: JsValue) -> Result<(), JsValue> {
        self.ensure_generation()?;
        let ranges = areas(ranges)?;
        let result = {
            let mut wb = self.workbook.job_write()?;
            if !self.is_current(&wb) {
                drop(wb);
                return Err(engine_error(RecalcJobError::NoActiveJob));
            }
            if ranges.iter().any(|area| area.sheet_id(&wb).is_none()) {
                drop(wb);
                return Err(invalid_address("sheet does not exist"));
            }
            wb.prioritize_recalc(&ranges.iter().map(Area::target).collect::<Vec<_>>())
        };
        result.map_err(engine_error)
    }
    pub fn cancel(&self) -> Result<JsValue, JsValue> {
        self.ensure_generation()?;
        let detached = {
            let mut wb = self.workbook.job_write()?;
            if !self.is_current(&wb) {
                drop(wb);
                return Err(engine_error(RecalcJobError::NoActiveJob));
            }
            wb.cancel_recalc().map(|report| report_value(&wb, report))
        };
        detached.map_or(Ok(JsValue::NULL), |value| to_js(&value))
    }
    #[wasm_bindgen(getter)]
    pub fn active(&self) -> Result<bool, JsValue> {
        if self.workbook.job_generation.load(Ordering::SeqCst) != self.generation {
            return Ok(false);
        }
        let wb = self.workbook.job_read()?;
        Ok(
            self.workbook.job_generation.load(Ordering::SeqCst) == self.generation
                && wb.has_active_recalc_job(),
        )
    }
}
impl Workbook {
    fn begin_job(&self, value: Option<JsValue>, driver: bool) -> Result<RecalcJob, JsValue> {
        let (options, ranges) = options(value)?;
        let mut wb = self.job_write()?;
        if self.driver_generation.load(Ordering::SeqCst) != 0 {
            drop(wb);
            return Err(engine_error(RecalcJobError::AlreadyPending {
                explicit: true,
            }));
        }
        if ranges.iter().any(|area| area.sheet_id(&wb).is_none()) {
            drop(wb);
            return Err(invalid_address("sheet does not exist"));
        }
        let result = wb.begin_recalc(options);
        if let Err(error) = result {
            drop(wb);
            return Err(engine_error(error));
        }
        let generation = self.job_generation.fetch_add(1, Ordering::SeqCst) + 1;
        if driver {
            self.driver_generation.store(generation, Ordering::SeqCst);
        }
        self.cancel_flag.store(false, Ordering::SeqCst);
        drop(wb);
        Ok(RecalcJob {
            workbook: self.clone(),
            generation,
        })
    }
}
#[wasm_bindgen]
impl RecalcJob {
    /// Internal bridge; stale cleanup is harmless and never clears a newer flag.
    #[wasm_bindgen(js_name = _releaseDriver)]
    pub fn release_driver(&self) -> Result<bool, JsValue> {
        if self.workbook.driver_generation.load(Ordering::SeqCst) != self.generation
            || self.workbook.job_generation.load(Ordering::SeqCst) != self.generation
        {
            return Ok(false);
        }
        // Reject release from inside an executing UDF. No JS runs while held.
        let guard = self.workbook.job_read()?;
        self.workbook.cancel_flag.store(false, Ordering::SeqCst);
        self.workbook.driver_generation.store(0, Ordering::SeqCst);
        drop(guard);
        Ok(true)
    }
    /// Internal lock-free query, including during terminal callbacks.
    #[wasm_bindgen(getter, js_name = _cancelRequested)]
    pub fn cancel_requested(&self) -> bool {
        self.workbook.job_generation.load(Ordering::SeqCst) == self.generation
            && self.workbook.cancel_flag.load(Ordering::SeqCst)
    }
}
#[wasm_bindgen]
impl Workbook {
    /// Successful admission starts a fresh cancellation signal. Refused admission
    /// and stepping never clear it; resetCancel remains an explicit host override.
    #[wasm_bindgen(js_name = beginRecalc)]
    pub fn begin_recalc(&self, value: Option<JsValue>) -> Result<RecalcJob, JsValue> {
        self.begin_job(value, false)
    }
    /// Internal bridge for JS drivers: reservation survives the terminal step.
    #[wasm_bindgen(js_name = _beginRecalcDriver)]
    pub fn begin_recalc_driver(&self, value: Option<JsValue>) -> Result<RecalcJob, JsValue> {
        self.begin_job(value, true)
    }
    #[wasm_bindgen(getter, js_name = hasActiveRecalcJob)]
    pub fn has_active_recalc_job(&self) -> Result<bool, JsValue> {
        Ok(self.job_read()?.has_active_recalc_job())
    }
    #[wasm_bindgen(js_name = hasPendingInRect)]
    pub fn has_pending_in_rect(&self, area: JsValue) -> Result<bool, JsValue> {
        let area = Area::parse(area)?;
        let wb = self.job_read()?;
        let Some(sheet) = area.sheet_id(&wb) else {
            drop(wb);
            return Err(invalid_address("sheet does not exist"));
        };
        Ok(wb.has_pending_in_rect(
            sheet,
            area.start_row as u32 - 1,
            area.start_column as u32 - 1,
            area.end_row as u32 - 1,
            area.end_column as u32 - 1,
        ))
    }
    #[wasm_bindgen(js_name = pendingCellsInRect)]
    pub fn pending_cells_in_rect(&self, area: JsValue) -> Result<JsValue, JsValue> {
        let area = Area::parse(area)?;
        let detached = {
            let wb = self.job_read()?;
            let Some(sheet) = area.sheet_id(&wb) else {
                drop(wb);
                return Err(invalid_address("sheet does not exist"));
            };
            let delta = wb.pending_cells_in_rect(
                sheet,
                area.start_row as u32 - 1,
                area.start_column as u32 - 1,
                area.end_row as u32 - 1,
                area.end_column as u32 - 1,
            );
            let mut cells = Vec::new();
            for record in delta.records {
                let (r0, c0, r1, c1) = record.bounds();
                for row in r0..=r1 {
                    for column in c0..=c1 {
                        cells.push(
                            json!({"sheet": area.sheet, "row": row + 1, "column": column + 1}),
                        );
                    }
                }
            }
            cells
        };
        to_js(&detached)
    }
}

fn to_js(value: &impl Serialize) -> Result<JsValue, JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|error| job_error("EVALUATION", &error.to_string()))
}

fn report_value(wb: &CoreWorkbook, report: engine::StepReport) -> Value {
    let progress = report.progress;
    let changed: Vec<_> = report
        .changed
        .delta
        .records
        .iter()
        .map(|record| {
            let (r0, c0, r1, c1) = record.bounds();
            json!({"sheet": wb.engine().sheet_name(record.sheet_id()),
            "startRow": r0 + 1, "startColumn": c0 + 1, "endRow": r1 + 1, "endColumn": c1 + 1})
        })
        .collect();
    let overflow: Vec<_> = report
        .changed
        .overflow_sheets
        .iter()
        .map(|sheet| wb.engine().sheet_name(*sheet).to_owned())
        .collect();
    json!({
        "phase": format!("{:?}", report.phase),
        "progress": {
            "pass": progress.pass, "completedInPass": progress.completed_in_pass,
            "scheduledInPass": progress.scheduled_in_pass,
            "cumulative": progress.cumulative_computed_vertices,
            "totalExact": progress.total_exact, "stagedRemaining": progress.staged_formulas_remaining,
            "priorityPending": progress.priority_pending
        },
        "stepComputed": report.step_computed_vertices,
        "stepElapsedMs": report.step_elapsed.as_secs_f64() * 1000.0,
        "changed": changed, "overflowSheets": overflow,
        "longestUnit": report.longest_unit.map(|unit| json!({
            "kind": format!("{:?}", unit.kind),
            "representativeCell": unit.representative_cell.map(|cell| json!({
                "sheet": wb.engine().sheet_name(cell.sheet_id()), "row": cell.row0() + 1, "column": cell.col0() + 1
            })),
            "vertexCount": unit.vertex_count, "elapsedMs": unit.elapsed.as_secs_f64() * 1000.0
        })),
        "result": report.result.map(|result| json!({
            "computedVertices": result.computed_vertices, "cycleErrors": result.cycle_errors,
            "elapsedMs": result.elapsed.as_secs_f64() * 1000.0
        }))
    })
}
