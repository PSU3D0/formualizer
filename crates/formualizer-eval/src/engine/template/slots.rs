//! Template slot maps: literal slots (by arena node) and finite relative
//! value-reference slots of a canonical template.

use rustc_hash::FxHashMap;

use formualizer_common::LiteralValue;

use crate::engine::arena::{AstNodeData, AstNodeId, DataStore};

use super::canonical::{
    AxisRef, CanonicalExpr, CanonicalReference, SlotContext, function_argument_slot_context,
};
use super::domain::{TemplateSlotMap, ValueRefSlotDescriptor, ValueRefSlotId};

pub(crate) fn value_ref_slot_descriptors(expr: &CanonicalExpr) -> Vec<ValueRefSlotDescriptor> {
    fn walk(expr: &CanonicalExpr, out: &mut Vec<ValueRefSlotDescriptor>, preorder: &mut u32) {
        match expr {
            CanonicalExpr::Literal(_) | CanonicalExpr::Omitted => {}
            CanonicalExpr::Reference { context, reference } => {
                let slot_context = match context {
                    super::canonical::CanonicalReferenceContext::Value => SlotContext::Value,
                    super::canonical::CanonicalReferenceContext::Reference => {
                        SlotContext::Reference
                    }
                    super::canonical::CanonicalReferenceContext::CallArgument { .. } => {
                        SlotContext::CallArgument
                    }
                    super::canonical::CanonicalReferenceContext::FunctionArgument {
                        function,
                        arg_index,
                    } => function_argument_slot_context(function, *arg_index),
                };
                if matches!(
                    slot_context,
                    SlotContext::Value | SlotContext::CriteriaExpressionArg
                ) && finite_relative_cell(reference)
                {
                    out.push(ValueRefSlotDescriptor {
                        slot_id: ValueRefSlotId(u16::try_from(out.len()).unwrap_or(u16::MAX)),
                        preorder_index: *preorder,
                        context: slot_context,
                        reference_pattern: reference.clone(),
                    });
                }
                *preorder = preorder.saturating_add(1);
            }
            CanonicalExpr::Unary { expr, .. } => walk(expr, out, preorder),
            CanonicalExpr::Binary { left, right, .. } => {
                walk(left, out, preorder);
                walk(right, out, preorder);
            }
            CanonicalExpr::Function { args, .. } | CanonicalExpr::CallUnsupported { args, .. } => {
                if let CanonicalExpr::CallUnsupported { callee, .. } = expr {
                    walk(callee, out, preorder);
                }
                for arg in args {
                    walk(arg, out, preorder);
                }
            }
            CanonicalExpr::ArrayUnsupported { rows } => {
                for row in rows {
                    for expr in row {
                        walk(expr, out, preorder);
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    let mut preorder = 0u32;
    walk(expr, &mut out, &mut preorder);
    out
}

fn finite_relative_cell(reference: &CanonicalReference) -> bool {
    matches!(
        reference,
        CanonicalReference::Cell {
            row: AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. },
            col: AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. },
            ..
        }
    ) && match reference {
        CanonicalReference::Cell { row, col, .. } => {
            matches!(row, AxisRef::RelativeToPlacement { .. })
                || matches!(col, AxisRef::RelativeToPlacement { .. })
        }
        _ => false,
    }
}

pub(crate) fn build_template_slot_map(
    origin_ast_id: AstNodeId,
    data_store: &DataStore,
    expr: &CanonicalExpr,
) -> TemplateSlotMap {
    fn walk(
        node_id: AstNodeId,
        data_store: &DataStore,
        next: &mut u16,
        out: &mut FxHashMap<AstNodeId, super::canonical::LiteralSlotId>,
    ) {
        let Some(node) = data_store.get_node(node_id) else {
            return;
        };
        match node {
            AstNodeData::Literal(vref) => {
                let value = data_store.retrieve_value(*vref);
                if !matches!(value, LiteralValue::Array(_)) {
                    out.insert(node_id, super::canonical::LiteralSlotId(*next));
                    *next = next.saturating_add(1);
                }
            }
            AstNodeData::Reference { .. } | AstNodeData::Omitted => {}
            AstNodeData::UnaryOp { expr_id, .. } => walk(*expr_id, data_store, next, out),
            AstNodeData::BinaryOp {
                left_id, right_id, ..
            } => {
                walk(*left_id, data_store, next, out);
                walk(*right_id, data_store, next, out);
            }
            AstNodeData::Function { .. } => {
                if let Some(args) = data_store.get_args(node_id) {
                    for arg in args {
                        walk(*arg, data_store, next, out);
                    }
                }
            }
            AstNodeData::Array { .. } => {}
        }
    }
    let mut map = FxHashMap::default();
    let mut next = 0u16;
    walk(origin_ast_id, data_store, &mut next, &mut map);
    let (residual_relative_row, residual_relative_col) = residual_relative_axes(expr);
    TemplateSlotMap {
        literal_slots_by_arena_node: map,
        residual_relative_row,
        residual_relative_col,
    }
}

fn residual_relative_axes(expr: &CanonicalExpr) -> (bool, bool) {
    fn reference_axes(reference: &CanonicalReference) -> (bool, bool) {
        match reference {
            CanonicalReference::Cell { row, col, .. } => (
                matches!(row, AxisRef::RelativeToPlacement { .. }),
                matches!(col, AxisRef::RelativeToPlacement { .. }),
            ),
            CanonicalReference::Range {
                start_row,
                end_row,
                start_col,
                end_col,
                ..
            } => (
                matches!(start_row, AxisRef::RelativeToPlacement { .. })
                    || matches!(end_row, AxisRef::RelativeToPlacement { .. }),
                matches!(start_col, AxisRef::RelativeToPlacement { .. })
                    || matches!(end_col, AxisRef::RelativeToPlacement { .. }),
            ),
            // Named references are placement-invariant (all-absolute after
            // resolution), so they contribute no residual relative axes.
            CanonicalReference::Named { .. } => (false, false),
            CanonicalReference::Unsupported { .. } => (false, false),
        }
    }

    fn walk(expr: &CanonicalExpr, row: &mut bool, col: &mut bool) {
        match expr {
            CanonicalExpr::Literal(_) | CanonicalExpr::Omitted => {}
            CanonicalExpr::Reference { context, reference } => {
                let slot_context = match context {
                    super::canonical::CanonicalReferenceContext::Value => SlotContext::Value,
                    super::canonical::CanonicalReferenceContext::Reference => {
                        SlotContext::Reference
                    }
                    super::canonical::CanonicalReferenceContext::CallArgument { .. } => {
                        SlotContext::CallArgument
                    }
                    super::canonical::CanonicalReferenceContext::FunctionArgument {
                        function,
                        arg_index,
                    } => function_argument_slot_context(function, *arg_index),
                };
                let captured_as_value_slot = matches!(
                    slot_context,
                    SlotContext::Value | SlotContext::CriteriaExpressionArg
                ) && finite_relative_cell(reference);
                if !captured_as_value_slot {
                    let (has_row, has_col) = reference_axes(reference);
                    *row |= has_row;
                    *col |= has_col;
                }
            }
            CanonicalExpr::Unary { expr, .. } => walk(expr, row, col),
            CanonicalExpr::Binary { left, right, .. } => {
                walk(left, row, col);
                walk(right, row, col);
            }
            CanonicalExpr::Function { args, .. } => {
                for arg in args {
                    walk(arg, row, col);
                }
            }
            CanonicalExpr::CallUnsupported { callee, args } => {
                walk(callee, row, col);
                for arg in args {
                    walk(arg, row, col);
                }
            }
            CanonicalExpr::ArrayUnsupported { rows } => {
                for cells in rows {
                    for cell in cells {
                        walk(cell, row, col);
                    }
                }
            }
        }
    }

    let mut row = false;
    let mut col = false;
    walk(expr, &mut row, &mut col);
    (row, col)
}
