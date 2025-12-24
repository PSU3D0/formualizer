use std::fmt::{self, Display};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FormulaDialect {
    #[default]
    Excel,
    OpenFormula,
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
