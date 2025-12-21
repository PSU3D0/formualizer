use wasm_bindgen::JsCast;

pub fn set_panic_hook() {
    #[cfg(feature = "console_panic")]
    console_error_panic_hook::set_once();
}

pub fn js_error(message: impl AsRef<str>) -> wasm_bindgen::JsValue {
    wasm_bindgen::JsValue::from(js_sys::Error::new(message.as_ref()))
}

pub fn js_error_with_cause(
    message: impl AsRef<str>,
    cause: wasm_bindgen::JsValue,
) -> wasm_bindgen::JsValue {
    let error = js_sys::Error::new(message.as_ref());
    let object = error.unchecked_ref::<js_sys::Object>();
    let _ = js_sys::Reflect::set(object, &wasm_bindgen::JsValue::from_str("cause"), &cause);
    wasm_bindgen::JsValue::from(error)
}
