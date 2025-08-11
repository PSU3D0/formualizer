use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

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
    pub fn to_string(&self) -> String {
        match self.pos {
            Some(pos) => format!("TokenizerError: {} at position {}", self.message, pos),
            None => format!("TokenizerError: {}", self.message),
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
    pub fn to_string(&self) -> String {
        match self.pos {
            Some(pos) => format!("ParserError: {} at position {}", self.message, pos),
            None => format!("ParserError: {}", self.message),
        }
    }
}
