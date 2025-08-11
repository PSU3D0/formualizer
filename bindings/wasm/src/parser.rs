use crate::ast::ASTNode;
use formualizer_core::{parser::Parser as CoreParser, Tokenizer as CoreTokenizer};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn parse_formula(formula: &str) -> Result<ASTNode, JsValue> {
    let tokenizer = CoreTokenizer::new(formula).map_err(|e| {
        JsValue::from_str(&format!(
            "Tokenizer error: {} at position {}",
            e.message, e.pos
        ))
    })?;

    let mut parser = CoreParser::new(tokenizer.items.clone(), false);
    let ast = parser.parse().map_err(|e| {
        JsValue::from_str(&format!(
            "Parser error: {} at position {:?}",
            e.message, e.position
        ))
    })?;

    Ok(ASTNode::from_core(ast))
}

#[wasm_bindgen]
pub struct Parser {
    inner: CoreParser,
}

#[wasm_bindgen]
impl Parser {
    #[wasm_bindgen(constructor)]
    pub fn new(formula: &str) -> Result<Parser, JsValue> {
        let tokenizer = CoreTokenizer::new(formula).map_err(|e| {
            JsValue::from_str(&format!(
                "Tokenizer error: {} at position {}",
                e.message, e.pos
            ))
        })?;

        Ok(Parser {
            inner: CoreParser::new(tokenizer.items.clone(), false),
        })
    }

    #[wasm_bindgen]
    pub fn parse(&mut self) -> Result<ASTNode, JsValue> {
        self.inner
            .parse()
            .map(|ast| ASTNode::from_core(ast))
            .map_err(|e| {
                JsValue::from_str(&format!(
                    "Parser error: {} at position {:?}",
                    e.message, e.position
                ))
            })
    }
}
