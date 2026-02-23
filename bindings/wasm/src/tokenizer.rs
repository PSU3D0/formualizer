use crate::{FormulaDialect, token::Token};
use formualizer::{FormulaDialect as CoreFormulaDialect, Tokenizer as CoreTokenizer};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Tokenizer {
    inner: CoreTokenizer,
}

#[wasm_bindgen]
impl Tokenizer {
    #[wasm_bindgen(constructor)]
    pub fn new(formula: &str, dialect: Option<FormulaDialect>) -> Result<Tokenizer, JsValue> {
        let dialect: CoreFormulaDialect = dialect.map(Into::into).unwrap_or_default();
        CoreTokenizer::new_with_dialect(formula, dialect)
            .map(|inner| Tokenizer { inner })
            .map_err(|e| {
                JsValue::from_str(&format!(
                    "Tokenizer error: {message} at position {pos}",
                    message = e.message,
                    pos = e.pos
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

        crate::utils::to_js_value(&tokens)
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
        crate::utils::to_js_value(&token)
    }

    #[wasm_bindgen(js_name = "toString")]
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        format!("Tokenizer({} tokens)", self.inner.items.len())
    }
}
