use formualizer::Token as CoreToken;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Token {
    pub token_type: String,
    pub subtype: String,
    pub value: String,
    pub pos: usize,
    pub end: usize,
}

impl Token {
    pub fn from_core(core_token: CoreToken) -> Self {
        Token {
            token_type: format!("{:?}", core_token.token_type),
            subtype: format!("{:?}", core_token.subtype),
            value: core_token.value,
            pos: core_token.start, // Use start position as pos
            end: core_token.end,
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
    pub fn subtype(&self) -> String {
        self.inner.subtype.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn pos(&self) -> usize {
        self.inner.pos
    }

    #[wasm_bindgen(getter)]
    pub fn end(&self) -> usize {
        self.inner.end
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
        crate::utils::to_js_value(&self.inner)
    }
}
