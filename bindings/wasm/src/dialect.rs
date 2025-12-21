use formualizer::FormulaDialect as CoreFormulaDialect;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormulaDialect {
    Excel,
    OpenFormula,
}

impl From<FormulaDialect> for CoreFormulaDialect {
    fn from(dialect: FormulaDialect) -> Self {
        match dialect {
            FormulaDialect::Excel => CoreFormulaDialect::Excel,
            FormulaDialect::OpenFormula => CoreFormulaDialect::OpenFormula,
        }
    }
}

impl From<CoreFormulaDialect> for FormulaDialect {
    fn from(dialect: CoreFormulaDialect) -> Self {
        match dialect {
            CoreFormulaDialect::Excel => FormulaDialect::Excel,
            CoreFormulaDialect::OpenFormula => FormulaDialect::OpenFormula,
        }
    }
}
