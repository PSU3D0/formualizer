use std::fmt::{self, Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormulaDialect {
    Excel,
    OpenFormula,
}

impl Default for FormulaDialect {
    fn default() -> Self {
        FormulaDialect::Excel
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsingError {
    InvalidReference(String),
}

impl Display for ParsingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
