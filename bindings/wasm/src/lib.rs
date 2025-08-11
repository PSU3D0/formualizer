use wasm_bindgen::prelude::*;

mod ast;
mod errors;
mod parser;
mod reference;
mod token;
mod tokenizer;
mod utils;

pub use ast::*;
pub use errors::*;
pub use parser::*;
pub use reference::*;
pub use token::*;
pub use tokenizer::*;

#[wasm_bindgen(start)]
pub fn main() {
    utils::set_panic_hook();
}

#[wasm_bindgen]
pub fn tokenize(formula: &str) -> Result<Tokenizer, JsValue> {
    Tokenizer::new(formula)
}

#[wasm_bindgen]
pub fn parse(formula: &str) -> Result<ASTNode, JsValue> {
    parser::parse_formula(formula)
}
