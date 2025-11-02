use wasm_bindgen::prelude::*;

mod ast;
mod dialect;
mod errors;
mod parser;
mod reference;
mod sheetport;
mod token;
mod tokenizer;
mod utils;
mod workbook;

pub use ast::*;
pub use dialect::*;
pub use errors::*;
pub use parser::*;
pub use reference::*;
pub use sheetport::*;
pub use token::*;
pub use tokenizer::*;
pub use workbook::*;

#[wasm_bindgen(start)]
pub fn init() {
    utils::set_panic_hook();
}

#[wasm_bindgen]
pub fn tokenize(formula: &str, dialect: Option<FormulaDialect>) -> Result<Tokenizer, JsValue> {
    Tokenizer::new(formula, dialect)
}

#[wasm_bindgen]
pub fn parse(formula: &str, dialect: Option<FormulaDialect>) -> Result<ASTNode, JsValue> {
    parser::parse_formula(formula, dialect)
}
