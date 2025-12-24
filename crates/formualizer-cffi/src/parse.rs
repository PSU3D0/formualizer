use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode as CoreASTNode, ASTNodeType, ReferenceType};
use formualizer_parse::tokenizer::Token as CoreToken;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CffiASTNode {
    Number {
        value: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    Text {
        value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    Boolean {
        value: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    Empty {
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    Error {
        kind: String,
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    Reference {
        sheet: Option<String>,
        reference: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    Function {
        name: String,
        args: Vec<CffiASTNode>,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    BinaryOp {
        op: String,
        left: Box<CffiASTNode>,
        right: Box<CffiASTNode>,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    UnaryOp {
        op: String,
        operand: Box<CffiASTNode>,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
    Array {
        elements: Vec<Vec<CffiASTNode>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<[usize; 2]>,
    },
}

#[derive(Serialize, Deserialize)]
pub struct CffiToken {
    pub value: String,
    pub token_type: String,
    pub subtype: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<[usize; 2]>,
}

impl CffiASTNode {
    pub fn from_core(node: &CoreASTNode, include_spans: bool) -> Self {
        let span = if include_spans {
            node.source_token.as_ref().map(|t| [t.start, t.end])
        } else {
            None
        };

        match &node.node_type {
            ASTNodeType::Literal(lit) => match lit {
                LiteralValue::Int(i) => CffiASTNode::Number {
                    value: *i as f64,
                    span,
                },
                LiteralValue::Number(n) => CffiASTNode::Number { value: *n, span },
                LiteralValue::Text(s) => CffiASTNode::Text {
                    value: s.clone(),
                    span,
                },
                LiteralValue::Boolean(b) => CffiASTNode::Boolean { value: *b, span },
                LiteralValue::Empty => CffiASTNode::Empty { span },
                LiteralValue::Error(e) => CffiASTNode::Error {
                    kind: format!("{:?}", e.kind),
                    message: e.message.clone(),
                    span,
                },
                LiteralValue::Array(arr) => CffiASTNode::Array {
                    elements: arr
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|v| Self::from_literal(v, include_spans))
                                .collect()
                        })
                        .collect(),
                    span,
                },
                _ => CffiASTNode::Text {
                    value: lit.to_string(),
                    span,
                },
            },
            ASTNodeType::Reference {
                original,
                reference,
            } => {
                let sheet = match reference {
                    ReferenceType::Cell { sheet, .. } => sheet.clone(),
                    ReferenceType::Range { sheet, .. } => sheet.clone(),
                    _ => None,
                };
                CffiASTNode::Reference {
                    sheet,
                    reference: original.clone(),
                    span,
                }
            }
            ASTNodeType::Function { name, args } => CffiASTNode::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::from_core(a, include_spans))
                    .collect(),
                span,
            },
            ASTNodeType::BinaryOp { op, left, right } => CffiASTNode::BinaryOp {
                op: op.clone(),
                left: Box::new(Self::from_core(left, include_spans)),
                right: Box::new(Self::from_core(right, include_spans)),
                span,
            },
            ASTNodeType::UnaryOp { op, expr } => CffiASTNode::UnaryOp {
                op: op.clone(),
                operand: Box::new(Self::from_core(expr, include_spans)),
                span,
            },
            ASTNodeType::Array(rows) => CffiASTNode::Array {
                elements: rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|a| Self::from_core(a, include_spans))
                            .collect()
                    })
                    .collect(),
                span,
            },
        }
    }

    fn from_literal(lit: &LiteralValue, _include_spans: bool) -> Self {
        match lit {
            LiteralValue::Int(i) => CffiASTNode::Number {
                value: *i as f64,
                span: None,
            },
            LiteralValue::Number(n) => CffiASTNode::Number {
                value: *n,
                span: None,
            },
            LiteralValue::Text(s) => CffiASTNode::Text {
                value: s.clone(),
                span: None,
            },
            LiteralValue::Boolean(b) => CffiASTNode::Boolean {
                value: *b,
                span: None,
            },
            LiteralValue::Empty => CffiASTNode::Empty { span: None },
            LiteralValue::Error(e) => CffiASTNode::Error {
                kind: format!("{:?}", e.kind),
                message: e.message.clone(),
                span: None,
            },
            LiteralValue::Array(arr) => CffiASTNode::Array {
                elements: arr
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| Self::from_literal(v, _include_spans))
                            .collect()
                    })
                    .collect(),
                span: None,
            },
            _ => CffiASTNode::Text {
                value: lit.to_string(),
                span: None,
            },
        }
    }
}

impl CffiToken {
    pub fn from_core(token: &CoreToken, include_spans: bool) -> Self {
        CffiToken {
            value: token.value.clone(),
            token_type: format!("{:?}", token.token_type),
            subtype: format!("{:?}", token.subtype),
            span: if include_spans {
                Some([token.start, token.end])
            } else {
                None
            },
        }
    }
}
