//! FormulaPlane structural-edit helpers.
//!
//! The helpers in this module are intentionally conservative MVP utilities for
//! demoting optimized spans before row/column structural edits.  They materialize
//! per-placement ASTs without mutating the shared arena AST.

use formualizer_common::{ExcelError, ExcelErrorKind};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Re-anchor a FormulaPlane template AST at a concrete placement.
///
/// FormulaPlane templates are stored at their canonical origin.  Runtime span
/// evaluation applies `(row_delta, col_delta)` virtually when resolving relative
/// references.  Pre-structural demotion needs an owned parser AST with those
/// same offsets materialized so the legacy graph/`ReferenceAdjuster` path can
/// handle the upcoming structural operation.
///
/// This is deliberately a simple offset relocation for relative cell/range axes;
/// it is not a structural insert/delete transform and therefore does not use
/// `ReferenceAdjuster`.
pub(crate) fn relocate_ast_for_template_placement(
    ast: &ASTNode,
    row_delta: i64,
    col_delta: i64,
) -> Result<ASTNode, ExcelError> {
    let node_type = match &ast.node_type {
        ASTNodeType::Literal(value) => ASTNodeType::Literal(value.clone()),
        ASTNodeType::Reference {
            original,
            reference,
        } => ASTNodeType::Reference {
            original: original.clone(),
            reference: relocate_reference_for_offset(reference, row_delta, col_delta)?,
        },
        ASTNodeType::UnaryOp { op, expr } => ASTNodeType::UnaryOp {
            op: op.clone(),
            expr: Box::new(relocate_ast_for_template_placement(
                expr, row_delta, col_delta,
            )?),
        },
        ASTNodeType::BinaryOp { op, left, right } => ASTNodeType::BinaryOp {
            op: op.clone(),
            left: Box::new(relocate_ast_for_template_placement(
                left, row_delta, col_delta,
            )?),
            right: Box::new(relocate_ast_for_template_placement(
                right, row_delta, col_delta,
            )?),
        },
        ASTNodeType::Function { name, args } => ASTNodeType::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| relocate_ast_for_template_placement(arg, row_delta, col_delta))
                .collect::<Result<_, _>>()?,
        },
        ASTNodeType::Call { callee, args } => ASTNodeType::Call {
            callee: Box::new(relocate_ast_for_template_placement(
                callee, row_delta, col_delta,
            )?),
            args: args
                .iter()
                .map(|arg| relocate_ast_for_template_placement(arg, row_delta, col_delta))
                .collect::<Result<_, _>>()?,
        },
        ASTNodeType::Array(rows) => ASTNodeType::Array(
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|arg| relocate_ast_for_template_placement(arg, row_delta, col_delta))
                        .collect::<Result<_, _>>()
                })
                .collect::<Result<_, _>>()?,
        ),
    };

    Ok(ASTNode {
        node_type,
        source_token: ast.source_token.clone(),
        contains_volatile: ast.contains_volatile,
    })
}

fn relocate_reference_for_offset(
    reference: &ReferenceType,
    row_delta: i64,
    col_delta: i64,
) -> Result<ReferenceType, ExcelError> {
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => Ok(ReferenceType::Cell {
            sheet: sheet.clone(),
            row: shift_axis_for_offset(*row, row_delta, *row_abs)?,
            col: shift_axis_for_offset(*col, col_delta, *col_abs)?,
            row_abs: *row_abs,
            col_abs: *col_abs,
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
        } => Ok(ReferenceType::Range {
            sheet: sheet.clone(),
            start_row: shift_optional_axis_for_offset(*start_row, row_delta, *start_row_abs)?,
            start_col: shift_optional_axis_for_offset(*start_col, col_delta, *start_col_abs)?,
            end_row: shift_optional_axis_for_offset(*end_row, row_delta, *end_row_abs)?,
            end_col: shift_optional_axis_for_offset(*end_col, col_delta, *end_col_abs)?,
            start_row_abs: *start_row_abs,
            start_col_abs: *start_col_abs,
            end_row_abs: *end_row_abs,
            end_col_abs: *end_col_abs,
        }),
        ReferenceType::NamedRange(_)
        | ReferenceType::Table(_)
        | ReferenceType::Cell3D { .. }
        | ReferenceType::Range3D { .. }
        | ReferenceType::External(_) => Err(unsupported_reference_relocation_error()),
    }
}

fn shift_optional_axis_for_offset(
    value: Option<u32>,
    delta: i64,
    is_absolute: bool,
) -> Result<Option<u32>, ExcelError> {
    value
        .map(|value| shift_axis_for_offset(value, delta, is_absolute))
        .transpose()
}

fn shift_axis_for_offset(value: u32, delta: i64, is_absolute: bool) -> Result<u32, ExcelError> {
    if is_absolute {
        return Ok(value);
    }
    let shifted = i64::from(value) + delta;
    if shifted < 1 || shifted > i64::from(u32::MAX) {
        return Err(unsupported_reference_relocation_error());
    }
    Ok(shifted as u32)
}

fn unsupported_reference_relocation_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::Ref)
        .with_message("Unsupported reference relocation for FormulaPlane structural demotion")
}

#[cfg(test)]
mod tests {
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNodeType, ReferenceType, parse};

    use super::*;

    #[test]
    fn relocates_relative_refs_without_moving_absolute_axes() {
        let ast = parse("=A1+$B1+A$1+$B$1").unwrap();
        let relocated = relocate_ast_for_template_placement(&ast, 2, 3).unwrap();
        let ASTNodeType::BinaryOp { .. } = relocated.node_type else {
            panic!("expected expression AST");
        };
    }

    #[test]
    fn relocates_reference_node_cell_axes() {
        let ast = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1".to_string(),
                reference: ReferenceType::Cell {
                    sheet: None,
                    row: 1,
                    col: 1,
                    row_abs: false,
                    col_abs: false,
                },
            },
            None,
        );
        let relocated = relocate_ast_for_template_placement(&ast, 4, 2).unwrap();
        match relocated.node_type {
            ASTNodeType::Reference {
                reference: ReferenceType::Cell { row, col, .. },
                ..
            } => {
                assert_eq!(row, 5);
                assert_eq!(col, 3);
            }
            other => panic!("unexpected AST: {other:?}"),
        }
    }

    #[test]
    fn preserves_literals() {
        let ast = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        assert_eq!(
            relocate_ast_for_template_placement(&ast, 1, 1).unwrap(),
            ast
        );
    }
}
