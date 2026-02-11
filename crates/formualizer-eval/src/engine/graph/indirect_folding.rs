//! Constant-folding pass for INDIRECT function calls.
//!
//! At graph-build time, if all inputs to an INDIRECT call are static
//! (literal strings, plain-value cell references, and `&` concatenation),
//! we can resolve the reference string and replace the INDIRECT AST node
//! with a normal Reference node. This makes the dependency graph correct
//! without requiring special runtime handling.

use super::DependencyGraph;
use crate::reference::{CellRef, Coord};
use crate::engine::vertex::VertexKind;
use crate::SheetId;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

impl DependencyGraph {
    /// Attempt to replace INDIRECT calls in the AST with resolved Reference nodes.
    /// Only succeeds when all string-building inputs are static (plain values, no formulas).
    pub fn try_fold_indirect(&self, ast: &mut ASTNode, current_sheet: SheetId) {
        // Walk the AST recursively, folding INDIRECT nodes in-place
        match &mut ast.node_type {
            ASTNodeType::Function { name, args } => {
                // First recurse into arguments (bottom-up folding)
                for arg in args.iter_mut() {
                    self.try_fold_indirect(arg, current_sheet);
                }
                // Then try to fold this node if it's INDIRECT
                let is_indirect = {
                    let upper = name.to_uppercase();
                    upper == "INDIRECT" || upper == "_XLFN.INDIRECT"
                };
                if is_indirect {
                    self.try_fold_indirect_call(ast, current_sheet);
                }
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.try_fold_indirect(left, current_sheet);
                self.try_fold_indirect(right, current_sheet);
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.try_fold_indirect(expr, current_sheet);
            }
            ASTNodeType::Array(rows) => {
                for row in rows.iter_mut() {
                    for cell in row.iter_mut() {
                        self.try_fold_indirect(cell, current_sheet);
                    }
                }
            }
            // Literals and References — nothing to fold
            _ => {}
        }
    }

    fn try_fold_indirect_call(&self, ast: &mut ASTNode, current_sheet: SheetId) {
        // Extract the first argument (ref_text expression)
        let first_arg = match &ast.node_type {
            ASTNodeType::Function { args, .. } if !args.is_empty() => &args[0],
            _ => return,
        };

        // Try to statically evaluate the argument to a string
        let ref_string = match self.try_eval_static_string(first_arg, current_sheet) {
            Some(s) => s,
            None => return, // Not statically resolvable — leave AST unchanged
        };

        // Parse the string as an Excel reference
        let reference = match ReferenceType::from_string(&ref_string) {
            Ok(r) => r,
            Err(_) => return, // Invalid reference — leave for runtime to produce #REF!
        };

        // Replace the INDIRECT call with a direct Reference node
        ast.node_type = ASTNodeType::Reference {
            original: ref_string,
            reference,
        };
    }

    fn try_eval_static_string(&self, ast: &ASTNode, current_sheet: SheetId) -> Option<String> {
        match &ast.node_type {
            ASTNodeType::Literal(lit) => match lit {
                LiteralValue::Text(s) => Some(s.clone()),
                LiteralValue::Number(n) => {
                    // Format as integer when there's no fractional part (common for row numbers)
                    let i = *n as i64;
                    if (*n - i as f64).abs() < f64::EPSILON {
                        Some(i.to_string())
                    } else {
                        Some(n.to_string())
                    }
                }
                LiteralValue::Int(i) => Some(i.to_string()),
                _ => None,
            },

            ASTNodeType::Reference {
                reference: ReferenceType::Cell { sheet, row, col, .. },
                ..
            } => {
                // Resolve the sheet name
                let sheet_name = match sheet {
                    Some(s) => s.as_str(),
                    None => self.sheet_name(current_sheet),
                };
                let sheet_id = self.sheet_id(sheet_name)?;

                // Look up the cell in the graph
                let coord = Coord::from_excel(*row, *col, true, true);
                let addr = CellRef::new(sheet_id, coord);
                let &vid = self.cell_to_vertex.get(&addr)?;

                // Only use if cell is a plain value (not a formula)
                match self.store.kind(vid) {
                    VertexKind::Cell => {}
                    _ => return None, // Formula cell or other kind — not static
                }

                // Get the value
                let val = self.get_cell_value(sheet_name, *row, *col)?;
                match val {
                    LiteralValue::Text(s) => Some(s),
                    LiteralValue::Number(n) => {
                        let i = n as i64;
                        if (n - i as f64).abs() < f64::EPSILON {
                            Some(i.to_string())
                        } else {
                            Some(n.to_string())
                        }
                    }
                    LiteralValue::Int(i) => Some(i.to_string()),
                    _ => None,
                }
            }

            ASTNodeType::BinaryOp { op, left, right } if op == "&" => {
                let l = self.try_eval_static_string(left, current_sheet)?;
                let r = self.try_eval_static_string(right, current_sheet)?;
                Some(format!("{}{}", l, r))
            }

            _ => None, // Not statically resolvable
        }
    }
}

/// Check whether an AST contains any INDIRECT function calls.
pub fn contains_indirect(ast: &ASTNode) -> bool {
    match &ast.node_type {
        ASTNodeType::Function { name, args } => {
            let upper = name.to_uppercase();
            if upper == "INDIRECT" || upper == "_XLFN.INDIRECT" {
                return true;
            }
            args.iter().any(contains_indirect)
        }
        ASTNodeType::BinaryOp { left, right, .. } => {
            contains_indirect(left) || contains_indirect(right)
        }
        ASTNodeType::UnaryOp { expr, .. } => contains_indirect(expr),
        ASTNodeType::Array(rows) => rows.iter().any(|row| row.iter().any(contains_indirect)),
        _ => false,
    }
}
