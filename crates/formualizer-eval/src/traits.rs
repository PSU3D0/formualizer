//! formualizer-eval – core traits (object-safe)
//!
//! Save/replace as `src/traits.rs`

use std::any::Any;
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType, TableSpecifier};

use crate::interpreter::Interpreter;
use formualizer_common::{
    ArgSpec, LiteralValue,
    error::{ExcelError, ExcelErrorKind},
};

/* ───────────────────────────── Range ───────────────────────────── */

pub trait Range: Debug + Send + Sync {
    fn get(&self, row: usize, col: usize) -> Result<LiteralValue, ExcelError>;
    fn dimensions(&self) -> (usize, usize);

    fn is_sparse(&self) -> bool {
        false
    }

    // Handle infinite ranges (A:A, 1:1)
    fn is_infinite(&self) -> bool {
        false
    }

    fn materialise(&self) -> Cow<'_, [Vec<LiteralValue>]> {
        Cow::Owned(
            (0..self.dimensions().0)
                .map(|r| {
                    (0..self.dimensions().1)
                        .map(|c| self.get(r, c).unwrap_or(LiteralValue::Empty))
                        .collect()
                })
                .collect(),
        )
    }

    fn iter_cells<'a>(&'a self) -> Box<dyn Iterator<Item = LiteralValue> + 'a> {
        let (rows, cols) = self.dimensions();
        Box::new((0..rows).flat_map(move |r| (0..cols).map(move |c| self.get(r, c).unwrap())))
    }
    fn iter_rows<'a>(&'a self) -> Box<dyn Iterator<Item = Vec<LiteralValue>> + 'a> {
        let (rows, cols) = self.dimensions();
        Box::new((0..rows).map(move |r| (0..cols).map(|c| self.get(r, c).unwrap()).collect()))
    }

    /* down-cast hook for SIMD back-ends */
    fn as_any(&self) -> &dyn Any;
}

/* blanket dyn passthrough */
impl Range for Box<dyn Range> {
    fn get(&self, r: usize, c: usize) -> Result<LiteralValue, ExcelError> {
        (**self).get(r, c)
    }
    fn dimensions(&self) -> (usize, usize) {
        (**self).dimensions()
    }
    fn is_sparse(&self) -> bool {
        (**self).is_sparse()
    }
    fn materialise(&self) -> Cow<'_, [Vec<LiteralValue>]> {
        (**self).materialise()
    }
    fn iter_cells<'a>(&'a self) -> Box<dyn Iterator<Item = LiteralValue> + 'a> {
        (**self).iter_cells()
    }
    fn iter_rows<'a>(&'a self) -> Box<dyn Iterator<Item = Vec<LiteralValue>> + 'a> {
        (**self).iter_rows()
    }
    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }
}

/* ────────────────────── ArgumentHandle helpers ───────────────────── */

pub type CowValue<'a> = Cow<'a, LiteralValue>;

pub enum EvaluatedArg<'a> {
    LiteralValue(CowValue<'a>),
    Range(Box<dyn Range>),
}

pub struct ArgumentHandle<'a, 'b> {
    node: &'a ASTNode,
    interp: &'a Interpreter<'b>,
}

impl<'a, 'b> ArgumentHandle<'a, 'b> {
    pub(crate) fn new(node: &'a ASTNode, interp: &'a Interpreter<'b>) -> Self {
        Self { node, interp }
    }

    pub fn value(&self) -> Result<CowValue<'_>, ExcelError> {
        if let ASTNodeType::Literal(ref v) = self.node.node_type {
            return Ok(Cow::Borrowed(v));
        }
        self.interp.evaluate_ast(self.node).map(Cow::Owned)
    }

    pub fn range(&self) -> Result<Box<dyn Range>, ExcelError> {
        match &self.node.node_type {
            ASTNodeType::Reference { reference, .. } => {
                self.interp.context.resolve_range_like(reference)
            }
            ASTNodeType::Array(rows) => {
                let mut materialized = Vec::new();
                for row in rows {
                    let mut materialized_row = Vec::new();
                    for cell in row {
                        materialized_row.push(self.interp.evaluate_ast(cell)?);
                    }
                    materialized.push(materialized_row);
                }
                Ok(Box::new(InMemoryRange::new(materialized)))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Expected a range, got {:?}", self.node.node_type))),
        }
    }

    pub fn value_or_range(&self) -> Result<EvaluatedArg<'_>, ExcelError> {
        self.range()
            .map(EvaluatedArg::Range)
            .or_else(|_| self.value().map(EvaluatedArg::LiteralValue))
    }

    pub fn ast(&self) -> &'a ASTNode {
        self.node
    }

    /* tiny validator helper for macro */
    pub fn matches_kind(&self, k: formualizer_common::ArgKind) -> Result<bool, ExcelError> {
        Ok(match k {
            formualizer_common::ArgKind::Any => true,
            formualizer_common::ArgKind::Range => self.range().is_ok(),
            formualizer_common::ArgKind::Number => matches!(
                self.value()?.as_ref(),
                LiteralValue::Number(_) | LiteralValue::Int(_)
            ),
            formualizer_common::ArgKind::Text => {
                matches!(self.value()?.as_ref(), LiteralValue::Text(_))
            }
            formualizer_common::ArgKind::Logical => {
                matches!(self.value()?.as_ref(), LiteralValue::Boolean(_))
            }
        })
    }
}

/* simple Vec-backed range */
#[derive(Debug, Clone)]
pub struct InMemoryRange {
    data: Vec<Vec<LiteralValue>>,
}
impl InMemoryRange {
    pub fn new(d: Vec<Vec<LiteralValue>>) -> Self {
        Self { data: d }
    }
}
impl Range for InMemoryRange {
    fn get(&self, r: usize, c: usize) -> Result<LiteralValue, ExcelError> {
        Ok(self
            .data
            .get(r)
            .and_then(|row| row.get(c))
            .cloned()
            .unwrap_or(LiteralValue::Empty))
    }
    fn dimensions(&self) -> (usize, usize) {
        (self.data.len(), self.data.first().map_or(0, |r| r.len()))
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/* ───────────────────────── Table abstraction ───────────────────────── */

pub trait Table: Debug + Send + Sync {
    fn get_cell(&self, row: usize, column: &str) -> Result<LiteralValue, ExcelError>;
    fn get_column(&self, column: &str) -> Result<Box<dyn Range>, ExcelError>;
    fn clone_box(&self) -> Box<dyn Table>;
}
impl Table for Box<dyn Table> {
    fn get_cell(&self, r: usize, c: &str) -> Result<LiteralValue, ExcelError> {
        (**self).get_cell(r, c)
    }
    fn get_column(&self, c: &str) -> Result<Box<dyn Range>, ExcelError> {
        (**self).get_column(c)
    }
    fn clone_box(&self) -> Box<dyn Table> {
        (**self).clone_box()
    }
}

/* ─────────────────────── Resolver super-trait ─────────────────────── */

pub trait ReferenceResolver: Send + Sync {
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError>;
}
pub trait RangeResolver: Send + Sync {
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError>;
}
pub trait NamedRangeResolver: Send + Sync {
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError>;
}
pub trait TableResolver: Send + Sync {
    fn resolve_table_reference(
        &self,
        tref: &formualizer_core::parser::TableReference,
    ) -> Result<Box<dyn Table>, ExcelError>;
}
pub trait Resolver: ReferenceResolver + RangeResolver + NamedRangeResolver + TableResolver {
    fn resolve_range_like(&self, r: &ReferenceType) -> Result<Box<dyn Range>, ExcelError> {
        match r {
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => self.resolve_range_reference(
                sheet.as_deref(),
                *start_row,
                *start_col,
                *end_row,
                *end_col,
            ),
            ReferenceType::Table(tref) => {
                let t = self.resolve_table_reference(tref)?;
                if let Some(TableSpecifier::Column(c)) = &tref.specifier {
                    t.get_column(c)
                } else {
                    Err(ExcelError::new(ExcelErrorKind::Ref).with_message(format!(
                        "Table specifier {:?} not supported",
                        tref.specifier
                    )))
                }
            }
            ReferenceType::NamedRange(n) => {
                let v = self.resolve_named_range_reference(n)?;
                Ok(Box::new(InMemoryRange::new(v)))
            }
            ReferenceType::Cell { sheet, row, col } => {
                let v = self.resolve_cell_reference(sheet.as_deref(), *row, *col)?;
                Ok(Box::new(InMemoryRange::new(vec![vec![v]])))
            }
        }
    }
}

/* ───────────────────── EvaluationContext = Resolver+Fns ───────────── */

pub trait FunctionProvider: Send + Sync {
    fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn Function>>;
}
pub trait EvaluationContext: Resolver + FunctionProvider {}
impl<T> EvaluationContext for T where T: Resolver + FunctionProvider {}

/// Excel-style callable – **object-safe** (no associated consts)
pub trait Function: Send + Sync + 'static {
    /* metadata getters */
    fn name(&self) -> &'static str;
    fn namespace(&self) -> &'static str {
        ""
    }
    fn volatile(&self) -> bool {
        false
    }
    fn min_args(&self) -> usize {
        0
    }
    fn variadic(&self) -> bool {
        false
    }
    fn arg_schema(&self) -> &'static [ArgSpec] {
        &[]
    }

    /* core work */
    fn eval<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError>;
}
