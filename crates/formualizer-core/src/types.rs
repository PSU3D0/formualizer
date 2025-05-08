use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq)]
pub enum ParsingError {
    InvalidReference(String),
}

impl Display for ParsingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// An **interpeter** LiteralValue. This is distinct
/// from the possible types that can be stored in a cell.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Int(i64),
    Number(f64),
    Text(String),
    Boolean(bool),
    Error(String),
    Array(Vec<Vec<LiteralValue>>), // For array results
    Empty,                         // For empty cells/optional arguments
}

impl Hash for LiteralValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            LiteralValue::Int(i) => i.hash(state),
            LiteralValue::Number(n) => n.to_bits().hash(state),
            LiteralValue::Text(s) => s.hash(state),
            LiteralValue::Boolean(b) => b.hash(state),
            LiteralValue::Error(e) => e.hash(state),
            LiteralValue::Array(a) => a.hash(state),
            LiteralValue::Empty => state.write_u8(0),
        }
    }
}

impl Eq for LiteralValue {}

impl Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::Int(i) => write!(f, "{}", i),
            LiteralValue::Number(n) => write!(f, "{}", n),
            LiteralValue::Text(s) => write!(f, "{}", s),
            LiteralValue::Boolean(b) => write!(f, "{}", b),
            LiteralValue::Error(e) => write!(f, "{}", e),
            LiteralValue::Array(a) => write!(f, "{:?}", a),
            LiteralValue::Empty => write!(f, ""),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueError {
    ImplicitIntersection(String),
}

impl LiteralValue {
    /// Coerce
    pub fn coerce_to_single_value(&self) -> Result<LiteralValue, ValueError> {
        match self {
            LiteralValue::Array(arr) => {
                // Excel's implicit intersection or single LiteralValue coercion logic here
                // Simplest: take top-left or return #LiteralValue! if not 1x1
                if arr.len() == 1 && arr[0].len() == 1 {
                    Ok(arr[0][0].clone())
                } else if arr.is_empty() || arr[0].is_empty() {
                    Ok(LiteralValue::Empty) // Or maybe error?
                } else {
                    Err(ValueError::ImplicitIntersection(
                        "#LiteralValue! Implicit intersection failed".to_string(),
                    ))
                }
            }
            _ => Ok(self.clone()),
        }
    }
}
