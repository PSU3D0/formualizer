use crate::token::Token;
use formualizer_core::Tokenizer as CoreTokenizer;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Tokenizer {
    inner: CoreTokenizer,
}

#[wasm_bindgen]
impl Tokenizer {
    #[wasm_bindgen(constructor)]
    pub fn new(formula: &str) -> Result<Tokenizer, JsValue> {
        CoreTokenizer::new(formula)
            .map(|inner| Tokenizer { inner })
            .map_err(|e| {
                JsValue::from_str(&format!(
                    "Tokenizer error: {} at position {}",
                    e.message, e.pos
                ))
            })
    }

    #[wasm_bindgen(js_name = "tokens")]
    pub fn tokens(&self) -> Result<JsValue, JsValue> {
        let tokens: Vec<Token> = self
            .inner
            .items
            .iter()
            .map(|token| Token::from_core(token.clone()))
            .collect();

        serde_wasm_bindgen::to_value(&tokens).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "render")]
    pub fn render(&self) -> String {
        self.inner.render()
    }

    #[wasm_bindgen(js_name = "length")]
    pub fn length(&self) -> usize {
        self.inner.items.len()
    }

    #[wasm_bindgen(js_name = "getToken")]
    pub fn get_token(&self, index: usize) -> Result<JsValue, JsValue> {
        if index >= self.inner.items.len() {
            return Err(JsValue::from_str("Index out of range"));
        }

        let token = Token::from_core(self.inner.items[index].clone());
        serde_wasm_bindgen::to_value(&token).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "toString")]
    pub fn to_string(&self) -> String {
        format!("Tokenizer({} tokens)", self.inner.items.len())
    }
}
