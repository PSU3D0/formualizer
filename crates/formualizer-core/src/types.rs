use std::fmt::{self, Display};

#[derive(Debug, Clone, PartialEq)]
pub enum ParsingError {
    InvalidReference(String),
}

impl Display for ParsingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
