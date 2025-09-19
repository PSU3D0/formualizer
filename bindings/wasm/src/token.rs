use formualizer_parse::Token as CoreToken;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Token {
    pub token_type: String,
    pub value: String,
    pub pos: usize,
}

impl Token {
    pub fn from_core(core_token: CoreToken) -> Self {
        Token {
            token_type: format!("{:?}", core_token.token_type),
            value: core_token.value,
            pos: core_token.start, // Use start position as pos
        }
    }
}

#[wasm_bindgen]
pub struct JsToken {
    inner: Token,
}

#[wasm_bindgen]
impl JsToken {
    #[wasm_bindgen(getter, js_name = "tokenType")]
    pub fn token_type(&self) -> String {
        self.inner.token_type.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn value(&self) -> String {
        self.inner.value.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn pos(&self) -> usize {
        self.inner.pos
    }

    #[wasm_bindgen(js_name = "toString")]
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        format!(
            "Token({token_type}: '{value}' at {pos})",
            token_type = self.inner.token_type,
            value = self.inner.value,
            pos = self.inner.pos
        )
    }

    #[wasm_bindgen(js_name = "toJSON")]
    pub fn to_json(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.inner).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}
