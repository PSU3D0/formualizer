use formualizer::common::error::{ExcelError, ExcelErrorExtra};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, prelude::*};

pub(crate) fn workbook_error_to_js(error: formualizer::workbook::IoError) -> JsValue {
    workbook_error_ref_to_js(&error)
}

pub(crate) fn inspect_error_to_js(error: formualizer::InspectError) -> JsValue {
    use formualizer::InspectError;

    let code = match &error {
        InspectError::SheetNotFound { .. } => "SHEET_NOT_FOUND",
        InspectError::InvalidAddress { .. } => "INVALID_ADDRESS",
        InspectError::InvalidOptions { .. } => "INVALID_OPTIONS",
        InspectError::DependencyStateUnavailable { .. } => "DEPENDENCY_STATE_UNAVAILABLE",
        InspectError::RevisionMismatch { .. } => "REVISION_MISMATCH",
        InspectError::ResourceExhausted { .. } => "RESOURCE_EXHAUSTED",
        _ => "INSPECT_ERROR",
    };
    let js_error = js_sys::Error::new(&error.to_string());
    let object = js_error.unchecked_ref::<js_sys::Object>();
    set_string(object, "code", code);
    set_string(object, "kind", "InspectError");
    set_string(object, "inspect_code", code);
    js_error.into()
}

pub(crate) fn workbook_error_ref_to_js(error: &formualizer::workbook::IoError) -> JsValue {
    match error {
        formualizer::workbook::IoError::Engine(error) => {
            let js_error: js_sys::Error = excel_error_to_js(error.clone()).unchecked_into();
            let object = js_error.unchecked_ref::<js_sys::Object>();
            set_string(object, "kind", "Engine");
            set_string(object, "workbook_kind", "Engine");
            js_error.into()
        }
        formualizer::workbook::IoError::CalculationDeferred { reason, context } => {
            let js_error = js_sys::Error::new(&error.to_string());
            let object = js_error.unchecked_ref::<js_sys::Object>();
            set_string(object, "name", "CalculationDeferred");
            set_string(object, "kind", "CalculationDeferred");
            set_string(object, "reason", &format!("{reason:?}"));
            set_string(object, "context", context);
            js_error.into()
        }
        other => JsValue::from(js_sys::Error::new(&other.to_string())),
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use formualizer::common::error::{
        ExcelErrorKind, PreparationStaleReason, ResourceExhaustionDetail, ResourceExhaustionReason,
    };
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    fn job_poison_and_function_panic_metadata_remain_machine_readable() {
        let error =
            ExcelError::new(ExcelErrorKind::Value).with_extra(ExcelErrorExtra::FunctionPanicked {
                function: "resolver:source".into(),
                message: Some("available panic detail".into()),
            });
        let js = excel_error_to_js(error);
        assert_eq!(
            js_sys::Reflect::get(&js, &"code".into())
                .unwrap()
                .as_string()
                .as_deref(),
            Some("FUNCTION_PANICKED")
        );
        let extra = js_sys::Reflect::get(&js, &"extra".into()).unwrap();
        assert_eq!(
            js_sys::Reflect::get(&extra, &"function".into())
                .unwrap()
                .as_string()
                .as_deref(),
            Some("resolver:source")
        );
        assert_eq!(
            js_sys::Reflect::get(&extra, &"message".into())
                .unwrap()
                .as_string()
                .as_deref(),
            Some("available panic detail")
        );
        let poisoned = excel_error_to_js(
            ExcelError::new(ExcelErrorKind::Value).with_extra(ExcelErrorExtra::EnginePoisoned),
        );
        assert_eq!(
            js_sys::Reflect::get(&poisoned, &"code".into())
                .unwrap()
                .as_string()
                .as_deref(),
            Some("ENGINE_POISONED")
        );
    }

    #[wasm_bindgen_test]
    fn workbook_deferral_is_typed_and_cancel_remains_engine_error() {
        use formualizer::workbook::{CalculationDeferredReason, IoError};
        let deferred = workbook_error_to_js(IoError::CalculationDeferred {
            reason: CalculationDeferredReason::KnownCoverageIncomplete,
            context: "S!A1".into(),
        });
        let field = |value: &JsValue, key: &str| {
            js_sys::Reflect::get(value, &JsValue::from_str(key))
                .unwrap()
                .as_string()
                .unwrap()
        };
        assert_eq!(field(&deferred, "kind"), "CalculationDeferred");
        assert_eq!(field(&deferred, "reason"), "KnownCoverageIncomplete");
        assert_eq!(field(&deferred, "context"), "S!A1");
        let cancelled =
            workbook_error_to_js(IoError::Engine(ExcelError::new(ExcelErrorKind::Cancelled)));
        assert_eq!(field(&cancelled, "kind"), "Engine");
        assert_eq!(
            field(&cancelled, "excel_kind"),
            ExcelErrorKind::Cancelled.to_string()
        );
    }

    #[wasm_bindgen_test]
    fn resource_u64_fields_are_lossless_decimal_strings() {
        let above_safe_integer = 9_007_199_254_740_993_u64;
        let error = ExcelError::new(ExcelErrorKind::NImpl)
            .with_message("resource message")
            .with_extra(ExcelErrorExtra::Resource {
                detail: Box::new(ResourceExhaustionDetail {
                    reason: ResourceExhaustionReason::GraphEdges,
                    limit: above_safe_integer,
                    observed: above_safe_integer + 1,
                    request_id: Some(above_safe_integer + 2),
                }),
            });
        let js_error: js_sys::Error = excel_error_to_js(error).unchecked_into();
        for (key, expected) in [
            ("limit", above_safe_integer),
            ("observed", above_safe_integer + 1),
            ("request_id", above_safe_integer + 2),
        ] {
            let value = js_sys::Reflect::get(js_error.as_ref(), &JsValue::from_str(key))
                .unwrap()
                .as_string()
                .unwrap();
            assert_eq!(value, expected.to_string());
        }
        assert_eq!(
            js_error.message().as_string().unwrap(),
            "#N/IMPL!: resource message [resource graph_edges 9007199254740994/9007199254740993]"
        );
    }

    #[wasm_bindgen_test]
    fn preparation_stale_reason_is_exposed_as_a_stable_string() {
        let error = ExcelError::new(ExcelErrorKind::Cancelled).with_extra(
            ExcelErrorExtra::PreparationStale {
                reason: PreparationStaleReason::Semantic,
            },
        );
        let js_error: js_sys::Error = excel_error_to_js(error).unchecked_into();
        let reason = js_sys::Reflect::get(
            js_error.as_ref(),
            &JsValue::from_str("preparation_stale_reason"),
        )
        .unwrap()
        .as_string()
        .unwrap();
        assert_eq!(reason, "semantic");
    }
}

fn set_string(object: &js_sys::Object, key: &str, value: &str) {
    let _ = js_sys::Reflect::set(object, &JsValue::from_str(key), &JsValue::from_str(value));
}

pub(crate) fn excel_error_to_js(error: ExcelError) -> JsValue {
    let js_error = js_sys::Error::new(&error.to_string());
    let object = js_error.unchecked_ref::<js_sys::Object>();
    set_string(object, "kind", &error.kind.to_string());
    set_string(object, "excel_kind", &error.kind.to_string());
    if let Some(message) = error.message() {
        let _ = js_sys::Reflect::set(
            object,
            &JsValue::from_str("excel_message"),
            &JsValue::from_str(message),
        );
    }
    match error.extra() {
        ExcelErrorExtra::None | ExcelErrorExtra::Spill { .. } => {}
        ExcelErrorExtra::RecalcJobActive => {
            set_string(object, "code", "RECALC_JOB_ACTIVE");
        }
        ExcelErrorExtra::EnginePoisoned => {
            set_string(object, "code", "ENGINE_POISONED");
        }
        ExcelErrorExtra::FunctionPanicked { function, message } => {
            set_string(object, "code", "FUNCTION_PANICKED");
            set_string(object, "function", function);
            let message = message.as_ref().map_or(JsValue::NULL, |value| value.into());
            let _ = js_sys::Reflect::set(object, &"panic_message".into(), &message);
            let extra = js_sys::Object::new();
            set_string(&extra, "kind", "FunctionPanicked");
            set_string(&extra, "function", function);
            let _ = js_sys::Reflect::set(&extra, &"message".into(), &message);
            let _ = js_sys::Reflect::set(object, &"extra".into(), &extra);
        }
        ExcelErrorExtra::Resource { detail } => {
            set_string(object, "resource_reason", detail.reason.as_str());
            // Decimal strings are the stable binding contract for Rust u64 values.
            set_string(object, "limit", &detail.limit.to_string());
            set_string(object, "observed", &detail.observed.to_string());
            let request_id = detail
                .request_id
                .map_or(JsValue::NULL, |id| JsValue::from_str(&id.to_string()));
            let _ = js_sys::Reflect::set(object, &JsValue::from_str("request_id"), &request_id);
        }
        ExcelErrorExtra::PreparationStale { reason } => {
            set_string(object, "preparation_stale_reason", reason.as_str());
        }
        ExcelErrorExtra::PlanStale { reason } => {
            set_string(object, "plan_stale_reason", reason.as_str());
        }
        _ => {}
    }
    js_error.into()
}

#[wasm_bindgen]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenizerError {
    message: String,
    pos: Option<usize>,
}

#[wasm_bindgen]
impl TokenizerError {
    #[wasm_bindgen(constructor)]
    pub fn new(message: String, pos: Option<usize>) -> TokenizerError {
        TokenizerError { message, pos }
    }

    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn pos(&self) -> Option<usize> {
        self.pos
    }

    #[wasm_bindgen(js_name = "toString")]
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        match self.pos {
            Some(pos) => format!(
                "TokenizerError: {message} at position {pos}",
                message = self.message
            ),
            None => format!("TokenizerError: {message}", message = self.message),
        }
    }
}

#[wasm_bindgen]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParserError {
    message: String,
    pos: Option<usize>,
}

#[wasm_bindgen]
impl ParserError {
    #[wasm_bindgen(constructor)]
    pub fn new(message: String, pos: Option<usize>) -> ParserError {
        ParserError { message, pos }
    }

    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn pos(&self) -> Option<usize> {
        self.pos
    }

    #[wasm_bindgen(js_name = "toString")]
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        match self.pos {
            Some(pos) => format!(
                "ParserError: {message} at position {pos}",
                message = self.message
            ),
            None => format!("ParserError: {message}", message = self.message),
        }
    }
}
