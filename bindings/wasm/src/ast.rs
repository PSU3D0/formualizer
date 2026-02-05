use formualizer::parse::{ASTNode as CoreASTNode, ASTNodeType};
use formualizer::LiteralValue;
use formualizer::ReferenceType;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ASTNodeData {
    Number {
        value: f64,
    },
    Text {
        value: String,
    },
    Boolean {
        value: bool,
    },
    Reference {
        reference: ReferenceData,
    },
    Function {
        name: String,
        args: Vec<ASTNodeData>,
    },
    BinaryOp {
        op: String,
        left: Box<ASTNodeData>,
        right: Box<ASTNodeData>,
    },
    UnaryOp {
        op: String,
        operand: Box<ASTNodeData>,
    },
    Array {
        elements: Vec<Vec<ASTNodeData>>,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceData {
    pub sheet: Option<String>,
    pub row_start: usize,
    pub col_start: usize,
    pub row_end: usize,
    pub col_end: usize,
    pub row_abs_start: bool,
    pub col_abs_start: bool,
    pub row_abs_end: bool,
    pub col_abs_end: bool,
}

impl ASTNodeData {
    fn from_literal(lit: &LiteralValue) -> Self {
        match lit {
            LiteralValue::Number(n) => ASTNodeData::Number { value: *n },
            LiteralValue::Int(i) => ASTNodeData::Number { value: *i as f64 },
            LiteralValue::Text(s) => ASTNodeData::Text { value: s.clone() },
            LiteralValue::Boolean(b) => ASTNodeData::Boolean { value: *b },
            LiteralValue::Error(e) => ASTNodeData::Error {
                message: format!("{e:?}"),
            },
            LiteralValue::Empty => ASTNodeData::Text {
                value: String::new(),
            },
            LiteralValue::Array(arr) => ASTNodeData::Array {
                elements: arr
                    .iter()
                    .map(|row| row.iter().map(Self::from_literal).collect())
                    .collect(),
            },
            LiteralValue::Date(d) => ASTNodeData::Text {
                value: d.to_string(),
            },
            LiteralValue::DateTime(dt) => ASTNodeData::Text {
                value: dt.to_string(),
            },
            LiteralValue::Time(t) => ASTNodeData::Text {
                value: t.to_string(),
            },
            LiteralValue::Duration(d) => ASTNodeData::Text {
                value: format!("{d:?}"),
            },
            LiteralValue::Pending => ASTNodeData::Text {
                value: "#PENDING!".to_string(),
            },
        }
    }

    fn from_core(node: &CoreASTNode) -> Self {
        match &node.node_type {
            ASTNodeType::Literal(lit) => match lit {
                LiteralValue::Number(n) => ASTNodeData::Number { value: *n },
                LiteralValue::Int(i) => ASTNodeData::Number { value: *i as f64 },
                LiteralValue::Text(s) => ASTNodeData::Text { value: s.clone() },
                LiteralValue::Boolean(b) => ASTNodeData::Boolean { value: *b },
                LiteralValue::Error(e) => ASTNodeData::Error {
                    message: format!("{e:?}"),
                },
                LiteralValue::Empty => ASTNodeData::Text {
                    value: String::new(),
                },
                LiteralValue::Array(arr) => ASTNodeData::Array {
                    elements: arr
                        .iter()
                        .map(|row| row.iter().map(Self::from_literal).collect())
                        .collect(),
                },
                LiteralValue::Date(d) => ASTNodeData::Text {
                    value: d.to_string(),
                },
                LiteralValue::DateTime(dt) => ASTNodeData::Text {
                    value: dt.to_string(),
                },
                LiteralValue::Time(t) => ASTNodeData::Text {
                    value: t.to_string(),
                },
                LiteralValue::Duration(d) => ASTNodeData::Text {
                    value: format!("{d:?}"),
                },
                LiteralValue::Pending => ASTNodeData::Text {
                    value: "#PENDING!".to_string(),
                },
            },
            ASTNodeType::Reference {
                original,
                reference,
            } => match Self::reference_data_from_core(reference) {
                Ok(reference) => ASTNodeData::Reference { reference },
                Err(reason) => ASTNodeData::Error {
                    message: format!("Unsupported reference '{original}' in WASM AST: {reason}"),
                },
            },
            ASTNodeType::Function { name, args } => ASTNodeData::Function {
                name: name.clone(),
                args: args.iter().map(Self::from_core).collect(),
            },
            ASTNodeType::BinaryOp { op, left, right } => ASTNodeData::BinaryOp {
                op: op.clone(),
                left: Box::new(Self::from_core(left)),
                right: Box::new(Self::from_core(right)),
            },
            ASTNodeType::UnaryOp { op, expr } => ASTNodeData::UnaryOp {
                op: op.clone(),
                operand: Box::new(Self::from_core(expr)),
            },
            ASTNodeType::Array(rows) => ASTNodeData::Array {
                elements: rows
                    .iter()
                    .map(|row| row.iter().map(Self::from_core).collect())
                    .collect(),
            },
        }
    }

    fn reference_data_from_core(reference: &ReferenceType) -> Result<ReferenceData, String> {
        match reference {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => Ok(ReferenceData {
                sheet: sheet.clone(),
                row_start: *row as usize,
                col_start: *col as usize,
                row_end: *row as usize,
                col_end: *col as usize,
                row_abs_start: *row_abs,
                col_abs_start: *col_abs,
                row_abs_end: *row_abs,
                col_abs_end: *col_abs,
            }),
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                let (Some(sr), Some(sc), Some(er), Some(ec)) =
                    (start_row, start_col, end_row, end_col)
                else {
                    return Err(
                        "range references with open row/col bounds are not yet represented"
                            .to_string(),
                    );
                };

                Ok(ReferenceData {
                    sheet: sheet.clone(),
                    row_start: *sr as usize,
                    col_start: *sc as usize,
                    row_end: *er as usize,
                    col_end: *ec as usize,
                    row_abs_start: *start_row_abs,
                    col_abs_start: *start_col_abs,
                    row_abs_end: *end_row_abs,
                    col_abs_end: *end_col_abs,
                })
            }
            ReferenceType::External(_) => Err("external references are not supported".to_string()),
            ReferenceType::Table(_) => Err("table references are not supported".to_string()),
            ReferenceType::NamedRange(_) => Err("named ranges are not supported".to_string()),
        }
    }
}

#[wasm_bindgen]
pub struct ASTNode {
    data: ASTNodeData,
}

impl ASTNode {
    pub fn from_core(node: CoreASTNode) -> Self {
        ASTNode {
            data: ASTNodeData::from_core(&node),
        }
    }
}

#[wasm_bindgen]
impl ASTNode {
    #[wasm_bindgen(js_name = "toJSON")]
    pub fn to_json(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.data).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "toString")]
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        format!("{:?}", self.data)
    }

    #[wasm_bindgen(js_name = "getType")]
    pub fn get_type(&self) -> String {
        match &self.data {
            ASTNodeData::Number { .. } => "number".to_string(),
            ASTNodeData::Text { .. } => "text".to_string(),
            ASTNodeData::Boolean { .. } => "boolean".to_string(),
            ASTNodeData::Reference { .. } => "reference".to_string(),
            ASTNodeData::Function { .. } => "function".to_string(),
            ASTNodeData::BinaryOp { .. } => "binaryOp".to_string(),
            ASTNodeData::UnaryOp { .. } => "unaryOp".to_string(),
            ASTNodeData::Array { .. } => "array".to_string(),
            ASTNodeData::Error { .. } => "error".to_string(),
        }
    }
}
