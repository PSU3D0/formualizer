//! L keys: literal-erased relative templates (design §4.2).
//!
//! One pre-order walk of a formula's arena tree yields a token stream in
//! which every literal is replaced by a slot marker and every reference
//! axis is encoded relative to the formula's placement (relative axes as
//! offsets, absolute axes as indexes, open bounds as a marker). Two cells
//! join one family node only if their streams are equal on one sheet, i.e.
//! they are the same relative template up to literal slots. The literals,
//! in the same pre-order, are the cell's slot row. Templates with table,
//! 3-D or external references are not relocatable (the FormulaPlane
//! validator's rule) and stay ungrouped singletons.

use crate::engine::arena::{AstNodeData, AstNodeId, CompactRefType, DataStore, SheetKey, ValueRef};
use smallvec::SmallVec;

const T_LIT: u64 = 1;
const T_OMIT: u64 = 2;
const T_CELL: u64 = 3;
const T_RANGE: u64 = 4;
const T_NAME: u64 = 5;
const T_TABLE: u64 = 6;
const T_EXTERNAL: u64 = 7;
const T_CELL3D: u64 = 8;
const T_RANGE3D: u64 = 9;
const T_UNARY: u64 = 10;
const T_BINARY: u64 = 11;
const T_FUNC: u64 = 12;
const T_ARRAY: u64 = 13;
const T_MISSING: u64 = 14;

const OPEN: u64 = 3 << 62;

fn axis(value: u32, abs: bool, placement: u32) -> u64 {
    if abs {
        (1 << 62) | u64::from(value)
    } else {
        // 1-based stored value relative to the 1-based anchor.
        let off = i64::from(value) - (i64::from(placement) + 1);
        (2 << 62) | (off as i32 as u32 as u64)
    }
}

fn open_axis(value: u32, sentinel: u32, abs: bool, placement: u32) -> u64 {
    if value == sentinel {
        OPEN
    } else {
        axis(value, abs, placement)
    }
}

fn sheet_token(s: Option<SheetKey>) -> u64 {
    match s {
        None => 0,
        Some(SheetKey::Id(id)) => (1 << 32) | u64::from(id),
        Some(SheetKey::Name(sid)) => (2 << 32) | u64::from(sid.as_u32()),
    }
}

/// A formula's template facts: its L token stream (when relocatable) and
/// its literal values in pre-order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TemplateFacts {
    pub tokens: Vec<u64>,
    pub literals: SmallVec<[ValueRef; 4]>,
    pub relocatable: bool,
}

/// Walk `ast` placed at 0-based `(row, col)`.
pub fn template_facts(ds: &DataStore, ast: AstNodeId, row: u32, col: u32) -> TemplateFacts {
    let mut f = TemplateFacts {
        tokens: Vec::with_capacity(16),
        literals: SmallVec::new(),
        relocatable: true,
    };
    let mut stack: Vec<AstNodeId> = vec![ast];
    while let Some(id) = stack.pop() {
        let Some(node) = ds.get_node(id) else {
            f.tokens.push(T_MISSING);
            f.relocatable = false;
            continue;
        };
        match *node {
            AstNodeData::Literal(v) => {
                f.tokens.push(T_LIT);
                f.literals.push(v);
            }
            AstNodeData::Omitted => f.tokens.push(T_OMIT),
            AstNodeData::Reference { ref_type, .. } => match ref_type {
                CompactRefType::Cell {
                    sheet,
                    row: r,
                    col: c,
                    row_abs,
                    col_abs,
                } => f.tokens.extend([
                    T_CELL,
                    sheet_token(sheet),
                    axis(r, row_abs, row),
                    axis(c, col_abs, col),
                ]),
                CompactRefType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                } => f.tokens.extend([
                    T_RANGE,
                    sheet_token(sheet),
                    open_axis(start_row, 0, start_row_abs, row),
                    open_axis(start_col, 0, start_col_abs, col),
                    open_axis(end_row, u32::MAX, end_row_abs, row),
                    open_axis(end_col, u32::MAX, end_col_abs, col),
                ]),
                CompactRefType::NamedRange(sid) => {
                    f.tokens.extend([T_NAME, u64::from(sid.as_u32())]);
                }
                CompactRefType::Table { name_id, .. } => {
                    f.tokens.extend([T_TABLE, u64::from(name_id.as_u32())]);
                    f.relocatable = false;
                }
                CompactRefType::External { raw_id, .. } => {
                    f.tokens.extend([T_EXTERNAL, u64::from(raw_id.as_u32())]);
                    f.relocatable = false;
                }
                CompactRefType::Cell3D { .. } => {
                    f.tokens.push(T_CELL3D);
                    f.relocatable = false;
                }
                CompactRefType::Range3D { .. } => {
                    f.tokens.push(T_RANGE3D);
                    f.relocatable = false;
                }
            },
            AstNodeData::UnaryOp { op_id, expr_id } => {
                f.tokens.extend([T_UNARY, u64::from(op_id.as_u32())]);
                stack.push(expr_id);
            }
            AstNodeData::BinaryOp {
                op_id,
                left_id,
                right_id,
            } => {
                f.tokens.extend([T_BINARY, u64::from(op_id.as_u32())]);
                stack.push(right_id);
                stack.push(left_id);
            }
            AstNodeData::Function {
                name_id,
                args_count,
                ..
            } => {
                f.tokens
                    .extend([T_FUNC, u64::from(name_id.as_u32()), u64::from(args_count)]);
                if let Some(args) = ds.get_args(id) {
                    stack.extend(args.iter().rev().copied());
                }
            }
            AstNodeData::Array { rows, cols, .. } => {
                f.tokens
                    .extend([T_ARRAY, (u64::from(rows) << 16) | u64::from(cols)]);
                if let Some((_, _, elems)) = ds.get_array_elems(id) {
                    stack.extend(elems.iter().rev().copied());
                }
            }
        }
    }
    f
}

/// The literal nodes of `ast` in the pre-order of [`template_facts`] (the
/// order of a cell's literal slot row). Arena nodes are hash-consed, so one
/// node id can occur at several positions.
pub fn template_literal_nodes(ds: &DataStore, ast: AstNodeId) -> SmallVec<[AstNodeId; 4]> {
    let mut out = SmallVec::new();
    let mut stack: Vec<AstNodeId> = vec![ast];
    while let Some(id) = stack.pop() {
        let Some(node) = ds.get_node(id) else {
            continue;
        };
        match *node {
            AstNodeData::Literal(_) => out.push(id),
            AstNodeData::UnaryOp { expr_id, .. } => stack.push(expr_id),
            AstNodeData::BinaryOp {
                left_id, right_id, ..
            } => {
                stack.push(right_id);
                stack.push(left_id);
            }
            AstNodeData::Function { .. } => {
                if let Some(args) = ds.get_args(id) {
                    stack.extend(args.iter().rev().copied());
                }
            }
            AstNodeData::Array { .. } => {
                if let Some((_, _, elems)) = ds.get_array_elems(id) {
                    stack.extend(elems.iter().rev().copied());
                }
            }
            AstNodeData::Omitted | AstNodeData::Reference { .. } => {}
        }
    }
    out
}
