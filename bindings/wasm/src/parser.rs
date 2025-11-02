use crate::{FormulaDialect, ast::ASTNode};
use formualizer_parse::{
    FormulaDialect as CoreFormulaDialect, Tokenizer as CoreTokenizer, parse_with_dialect,
    parser::Parser as CoreParser,
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn parse_formula(formula: &str, dialect: Option<FormulaDialect>) -> Result<ASTNode, JsValue> {
    let dialect: CoreFormulaDialect = dialect.map(Into::into).unwrap_or_default();

    parse_with_dialect(formula, dialect)
        .map(ASTNode::from_core)
        .map_err(|e| {
            JsValue::from_str(&format!(
                "Parser error: {} at position {:?}",
                e.message, e.position
            ))
        })
}

#[wasm_bindgen]
pub struct Parser {
    inner: CoreParser,
}

#[wasm_bindgen]
impl Parser {
    #[wasm_bindgen(constructor)]
    pub fn new(formula: &str, dialect: Option<FormulaDialect>) -> Result<Parser, JsValue> {
        let dialect: CoreFormulaDialect = dialect.map(Into::into).unwrap_or_default();
        let tokenizer = CoreTokenizer::new_with_dialect(formula, dialect).map_err(|e| {
            JsValue::from_str(&format!(
                "Tokenizer error: {} at position {}",
                e.message, e.pos
            ))
        })?;

        Ok(Parser {
            inner: CoreParser::new_with_dialect(tokenizer.items.clone(), false, dialect),
        })
    }

    #[wasm_bindgen]
    pub fn parse(&mut self) -> Result<ASTNode, JsValue> {
        self.inner.parse().map(ASTNode::from_core).map_err(|e| {
            JsValue::from_str(&format!(
                "Parser error: {} at position {:?}",
                e.message, e.position
            ))
        })
    }
}
