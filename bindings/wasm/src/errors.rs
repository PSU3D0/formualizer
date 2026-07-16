use formualizer::common::error::{ExcelError, ExcelErrorExtra};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, prelude::*};

pub(crate) fn workbook_error_to_js(error: formualizer::workbook::IoError) -> JsValue {
    workbook_error_ref_to_js(&error)
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
        other => JsValue::from(js_sys::Error::new(&other.to_string())),
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use formualizer::common::error::{
        ExcelErrorKind, ResourceExhaustionDetail, ResourceExhaustionReason,
    };
    use wasm_bindgen_test::wasm_bindgen_test;

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
}

fn set_string(object: &js_sys::Object, key: &str, value: &str) {
    let _ = js_sys::Reflect::set(object, &JsValue::from_str(key), &JsValue::from_str(value));
}

pub(crate) fn excel_error_to_js(error: ExcelError) -> JsValue {
    let js_error = js_sys::Error::new(&error.to_string());
    let object = js_error.unchecked_ref::<js_sys::Object>();
    set_string(object, "kind", &error.kind.to_string());
    set_string(object, "excel_kind", &error.kind.to_string());
    if let Some(message) = &error.message {
        let _ = js_sys::Reflect::set(
            object,
            &JsValue::from_str("excel_message"),
            &JsValue::from_str(message),
        );
    }
    match &error.extra {
        ExcelErrorExtra::None | ExcelErrorExtra::Spill { .. } => {}
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
