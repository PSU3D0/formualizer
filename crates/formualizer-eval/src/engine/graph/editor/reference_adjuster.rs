use crate::reference::{CellRef, Coord};
use formualizer_parse::parser::{ASTNode, ASTNodeType};

/// Centralized reference adjustment logic for structural changes
pub struct ReferenceAdjuster;

#[derive(Debug, Clone)]
pub enum ShiftOperation {
    InsertRows {
        sheet_id: u16,
        before: u32,
        count: u32,
    },
    DeleteRows {
        sheet_id: u16,
        start: u32,
        count: u32,
    },
    InsertColumns {
        sheet_id: u16,
        before: u32,
        count: u32,
    },
    DeleteColumns {
        sheet_id: u16,
        start: u32,
        count: u32,
    },
}

impl ReferenceAdjuster {
    pub fn new() -> Self {
        Self
    }

    /// Adjust an AST for a shift operation, preserving source tokens
    pub fn adjust_ast(&self, ast: &ASTNode, op: &ShiftOperation) -> ASTNode {
        match &ast.node_type {
            ASTNodeType::Reference {
                original,
                reference,
            } => {
                let adjusted = self.adjust_reference(reference, op);
                ASTNode {
                    node_type: ASTNodeType::Reference {
                        original: original.clone(),
                        reference: adjusted,
                    },
                    source_token: ast.source_token.clone(),
                    contains_volatile: ast.contains_volatile,
                }
            }
            ASTNodeType::BinaryOp {
                op: bin_op,
                left,
                right,
            } => ASTNode {
                node_type: ASTNodeType::BinaryOp {
                    op: bin_op.clone(),
                    left: Box::new(self.adjust_ast(left, op)),
                    right: Box::new(self.adjust_ast(right, op)),
                },
                source_token: ast.source_token.clone(),
                contains_volatile: ast.contains_volatile,
            },
            ASTNodeType::UnaryOp { op: un_op, expr } => ASTNode {
                node_type: ASTNodeType::UnaryOp {
                    op: un_op.clone(),
                    expr: Box::new(self.adjust_ast(expr, op)),
                },
                source_token: ast.source_token.clone(),
                contains_volatile: ast.contains_volatile,
            },
            ASTNodeType::Function { name, args } => ASTNode {
                node_type: ASTNodeType::Function {
                    name: name.clone(),
                    args: args.iter().map(|arg| self.adjust_ast(arg, op)).collect(),
                },
                source_token: ast.source_token.clone(),
                contains_volatile: ast.contains_volatile,
            },
            _ => ast.clone(),
        }
    }

    /// Adjust a cell reference for a shift operation
    /// Returns None if the cell is deleted
    pub fn adjust_cell_ref(&self, cell_ref: &CellRef, op: &ShiftOperation) -> Option<CellRef> {
        let coord = cell_ref.coord;
        let adjusted_coord = match op {
            ShiftOperation::InsertRows {
                sheet_id,
                before,
                count,
            } if cell_ref.sheet_id == *sheet_id => {
                if coord.row_abs() || coord.row < *before {
                    // Absolute references or cells before insert point don't move
                    coord
                } else {
                    // Shift down
                    Coord::new(
                        coord.row + count,
                        coord.col,
                        coord.row_abs(),
                        coord.col_abs(),
                    )
                }
            }
            ShiftOperation::DeleteRows {
                sheet_id,
                start,
                count,
            } if cell_ref.sheet_id == *sheet_id => {
                if coord.row_abs() {
                    // Absolute references don't adjust
                    coord
                } else if coord.row >= *start && coord.row < start + count {
                    // Cell deleted
                    return None;
                } else if coord.row >= start + count {
                    // Shift up
                    Coord::new(
                        coord.row - count,
                        coord.col,
                        coord.row_abs(),
                        coord.col_abs(),
                    )
                } else {
                    // Before delete range, no change
                    coord
                }
            }
            ShiftOperation::InsertColumns {
                sheet_id,
                before,
                count,
            } if cell_ref.sheet_id == *sheet_id => {
                if coord.col_abs() || coord.col < *before {
                    // Absolute references or cells before insert point don't move
                    coord
                } else {
                    // Shift right
                    Coord::new(
                        coord.row,
                        coord.col + count,
                        coord.row_abs(),
                        coord.col_abs(),
                    )
                }
            }
            ShiftOperation::DeleteColumns {
                sheet_id,
                start,
                count,
            } if cell_ref.sheet_id == *sheet_id => {
                if coord.col_abs() {
                    // Absolute references don't adjust
                    coord
                } else if coord.col >= *start && coord.col < start + count {
                    // Cell deleted
                    return None;
                } else if coord.col >= start + count {
                    // Shift left
                    Coord::new(
                        coord.row,
                        coord.col - count,
                        coord.row_abs(),
                        coord.col_abs(),
                    )
                } else {
                    // Before delete range, no change
                    coord
                }
            }
            _ => coord,
        };

        Some(CellRef::new(cell_ref.sheet_id, adjusted_coord))
    }

    /// Adjust a reference type (cell or range) for a shift operation
    fn adjust_reference(
        &self,
        reference: &formualizer_parse::parser::ReferenceType,
        op: &ShiftOperation,
    ) -> formualizer_parse::parser::ReferenceType {
        use formualizer_parse::parser::ReferenceType;

        match reference {
            ReferenceType::Cell { sheet, row, col } => {
                // Create a temporary CellRef to reuse adjustment logic
                // For now, assume same sheet if no sheet specified
                let temp_ref = CellRef::new(
                    match op {
                        ShiftOperation::InsertRows { sheet_id, .. }
                        | ShiftOperation::DeleteRows { sheet_id, .. }
                        | ShiftOperation::InsertColumns { sheet_id, .. }
                        | ShiftOperation::DeleteColumns { sheet_id, .. } => *sheet_id,
                    },
                    Coord::new(*row, *col, false, false), // Assume relative for now
                );

                match self.adjust_cell_ref(&temp_ref, op) {
                    None => {
                        // Cell was deleted, create a special marker
                        // We'll need to handle this at a higher level
                        ReferenceType::Cell {
                            sheet: Some("#REF".to_string()),
                            row: 0,
                            col: 0,
                        }
                    }
                    Some(adjusted) => ReferenceType::Cell {
                        sheet: sheet.clone(),
                        row: adjusted.coord.row,
                        col: adjusted.coord.col,
                    },
                }
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                // Check if this is an unbounded (infinite) range
                // Unbounded column: A:A has no row bounds (both None)
                // Unbounded row: 1:1 has no column bounds (both None)
                let is_unbounded_column = start_row.is_none() && end_row.is_none();
                let is_unbounded_row = start_col.is_none() && end_col.is_none();

                // Don't adjust unbounded ranges - they conceptually represent "all rows/columns"
                // and should remain unchanged during structural operations
                if is_unbounded_column || is_unbounded_row {
                    return reference.clone();
                }

                // Adjust range boundaries based on operation
                let (adj_start_row, adj_end_row) = match op {
                    ShiftOperation::InsertRows { before, count, .. } => {
                        // Only adjust if both bounds are present (bounded range)
                        match (start_row, end_row) {
                            (Some(start), Some(end)) => {
                                let adj_start = if *start >= *before {
                                    start + count
                                } else {
                                    *start
                                };
                                let adj_end = if *end >= *before { end + count } else { *end };
                                (Some(adj_start), Some(adj_end))
                            }
                            // Preserve None values for partially bounded ranges
                            _ => (*start_row, *end_row),
                        }
                    }
                    ShiftOperation::DeleteRows { start, count, .. } => {
                        // Only adjust if both bounds are present
                        match (start_row, end_row) {
                            (Some(range_start), Some(range_end)) => {
                                if *range_end < *start || *range_start >= start + count {
                                    // Range outside delete area
                                    let adj_start = if *range_start >= start + count {
                                        range_start - count
                                    } else {
                                        *range_start
                                    };
                                    let adj_end = if *range_end >= start + count {
                                        range_end - count
                                    } else {
                                        *range_end
                                    };
                                    (Some(adj_start), Some(adj_end))
                                } else if *range_start >= *start && *range_end < start + count {
                                    // Entire range deleted - mark with special sheet name
                                    return ReferenceType::Range {
                                        sheet: Some("#REF".to_string()),
                                        start_row: Some(0),
                                        start_col: Some(0),
                                        end_row: Some(0),
                                        end_col: Some(0),
                                    };
                                } else {
                                    // Range partially overlaps delete area
                                    let adj_start = if *range_start < *start {
                                        *range_start
                                    } else {
                                        *start
                                    };
                                    let adj_end = if *range_end >= start + count {
                                        range_end - count
                                    } else {
                                        start - 1
                                    };
                                    (Some(adj_start), Some(adj_end))
                                }
                            }
                            // Preserve None values for partially bounded ranges
                            _ => (*start_row, *end_row),
                        }
                    }
                    _ => (*start_row, *end_row),
                };

                // Similar logic for columns
                let (adj_start_col, adj_end_col) = match op {
                    ShiftOperation::InsertColumns { before, count, .. } => {
                        // Only adjust if both bounds are present
                        match (start_col, end_col) {
                            (Some(start), Some(end)) => {
                                let adj_start = if *start >= *before {
                                    start + count
                                } else {
                                    *start
                                };
                                let adj_end = if *end >= *before { end + count } else { *end };
                                (Some(adj_start), Some(adj_end))
                            }
                            // Preserve None values
                            _ => (*start_col, *end_col),
                        }
                    }
                    ShiftOperation::DeleteColumns { start, count, .. } => {
                        // Only adjust if both bounds are present
                        match (start_col, end_col) {
                            (Some(range_start), Some(range_end)) => {
                                if *range_end < *start || *range_start >= start + count {
                                    // Range outside delete area
                                    let adj_start = if *range_start >= start + count {
                                        range_start - count
                                    } else {
                                        *range_start
                                    };
                                    let adj_end = if *range_end >= start + count {
                                        range_end - count
                                    } else {
                                        *range_end
                                    };
                                    (Some(adj_start), Some(adj_end))
                                } else if *range_start >= *start && *range_end < start + count {
                                    // Entire range deleted - mark with special sheet name
                                    return ReferenceType::Range {
                                        sheet: Some("#REF".to_string()),
                                        start_row: Some(0),
                                        start_col: Some(0),
                                        end_row: Some(0),
                                        end_col: Some(0),
                                    };
                                } else {
                                    // Range partially overlaps delete area
                                    let adj_start = if *range_start < *start {
                                        *range_start
                                    } else {
                                        *start
                                    };
                                    let adj_end = if *range_end >= start + count {
                                        range_end - count
                                    } else {
                                        start - 1
                                    };
                                    (Some(adj_start), Some(adj_end))
                                }
                            }
                            // Preserve None values
                            _ => (*start_col, *end_col),
                        }
                    }
                    _ => (*start_col, *end_col),
                };

                ReferenceType::Range {
                    sheet: sheet.clone(),
                    start_row: adj_start_row,
                    start_col: adj_start_col,
                    end_row: adj_end_row,
                    end_col: adj_end_col,
                }
            }
            _ => reference.clone(),
        }
    }
}

impl Default for ReferenceAdjuster {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper for adjusting references when copying/moving ranges
pub struct RelativeReferenceAdjuster {
    row_offset: i32,
    col_offset: i32,
}

impl RelativeReferenceAdjuster {
    pub fn new(row_offset: i32, col_offset: i32) -> Self {
        Self {
            row_offset,
            col_offset,
        }
    }

    pub fn adjust_formula(&self, ast: &ASTNode) -> ASTNode {
        match &ast.node_type {
            ASTNodeType::Reference {
                original,
                reference,
            } => {
                let adjusted = self.adjust_reference(reference);
                ASTNode {
                    node_type: ASTNodeType::Reference {
                        original: original.clone(),
                        reference: adjusted,
                    },
                    source_token: ast.source_token.clone(),
                    contains_volatile: ast.contains_volatile,
                }
            }
            ASTNodeType::BinaryOp { op, left, right } => ASTNode {
                node_type: ASTNodeType::BinaryOp {
                    op: op.clone(),
                    left: Box::new(self.adjust_formula(left)),
                    right: Box::new(self.adjust_formula(right)),
                },
                source_token: ast.source_token.clone(),
                contains_volatile: ast.contains_volatile,
            },
            ASTNodeType::UnaryOp { op, expr } => ASTNode {
                node_type: ASTNodeType::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(self.adjust_formula(expr)),
                },
                source_token: ast.source_token.clone(),
                contains_volatile: ast.contains_volatile,
            },
            ASTNodeType::Function { name, args } => ASTNode {
                node_type: ASTNodeType::Function {
                    name: name.clone(),
                    args: args.iter().map(|arg| self.adjust_formula(arg)).collect(),
                },
                source_token: ast.source_token.clone(),
                contains_volatile: ast.contains_volatile,
            },
            _ => ast.clone(),
        }
    }

    fn adjust_reference(
        &self,
        reference: &formualizer_parse::parser::ReferenceType,
    ) -> formualizer_parse::parser::ReferenceType {
        use formualizer_parse::parser::ReferenceType;

        match reference {
            ReferenceType::Cell { sheet, row, col } => {
                // Only adjust relative references
                // TODO: Check for absolute references when we have that info
                let new_row = (*row as i32 + self.row_offset).max(1) as u32;
                let new_col = (*col as i32 + self.col_offset).max(1) as u32;

                ReferenceType::Cell {
                    sheet: sheet.clone(),
                    row: new_row,
                    col: new_col,
                }
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                // Adjust range boundaries
                let adj_start_row = start_row.map(|r| (r as i32 + self.row_offset).max(1) as u32);
                let adj_start_col = start_col.map(|c| (c as i32 + self.col_offset).max(1) as u32);
                let adj_end_row = end_row.map(|r| (r as i32 + self.row_offset).max(1) as u32);
                let adj_end_col = end_col.map(|c| (c as i32 + self.col_offset).max(1) as u32);

                ReferenceType::Range {
                    sheet: sheet.clone(),
                    start_row: adj_start_row,
                    start_col: adj_start_col,
                    end_row: adj_end_row,
                    end_col: adj_end_col,
                }
            }
            _ => reference.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_parse::parser::parse;

    fn format_formula(ast: &ASTNode) -> String {
        // TODO: Use the actual formualizer_parse::parser::to_string when available
        // For now, a simple representation
        format!("{ast:?}")
    }

    #[test]
    fn test_reference_adjustment_on_row_insert() {
        let adjuster = ReferenceAdjuster::new();

        // Formula: =A5+B10
        let ast = parse("=A5+B10").unwrap();

        // Insert 2 rows before row 7
        let adjusted = adjuster.adjust_ast(
            &ast,
            &ShiftOperation::InsertRows {
                sheet_id: 0,
                before: 7,
                count: 2,
            },
        );

        // A5 unchanged (before insert point), B10 -> B12
        // Verify by checking the AST structure
        if let ASTNodeType::BinaryOp { left, right, .. } = &adjusted.node_type {
            if let ASTNodeType::Reference {
                reference: left_ref,
                ..
            } = &left.node_type
            {
                if let formualizer_parse::parser::ReferenceType::Cell { row, col, .. } = left_ref {
                    assert_eq!(*row, 5); // A5 unchanged
                    assert_eq!(*col, 1);
                }
            }
            if let ASTNodeType::Reference {
                reference: right_ref,
                ..
            } = &right.node_type
            {
                if let formualizer_parse::parser::ReferenceType::Cell { row, col, .. } = right_ref {
                    assert_eq!(*row, 12); // B10 -> B12
                    assert_eq!(*col, 2);
                }
            }
        }
    }

    #[test]
    fn test_reference_adjustment_on_column_delete() {
        let adjuster = ReferenceAdjuster::new();

        // Formula: =C1+F1
        let ast = parse("=C1+F1").unwrap();

        // Delete columns B and C (columns 2 and 3)
        let adjusted = adjuster.adjust_ast(
            &ast,
            &ShiftOperation::DeleteColumns {
                sheet_id: 0,
                start: 2, // Column B
                count: 2,
            },
        );

        // C1 -> #REF! (deleted), F1 -> D1 (shifted left by 2)
        if let ASTNodeType::BinaryOp { left, right, .. } = &adjusted.node_type {
            if let ASTNodeType::Reference {
                reference: left_ref,
                ..
            } = &left.node_type
            {
                // C1 should become #REF! (marked with special sheet name)
                if let formualizer_parse::parser::ReferenceType::Cell { sheet, row, col } = left_ref
                {
                    assert_eq!(sheet.as_deref(), Some("#REF"));
                    assert_eq!(*row, 0);
                    assert_eq!(*col, 0);
                }
            }
            if let ASTNodeType::Reference {
                reference: right_ref,
                ..
            } = &right.node_type
            {
                if let formualizer_parse::parser::ReferenceType::Cell { row, col, .. } = right_ref {
                    assert_eq!(*row, 1); // Row unchanged
                    assert_eq!(*col, 4); // F1 (col 6) -> D1 (col 4)
                }
            }
        }
    }

    #[test]
    fn test_range_reference_adjustment() {
        let adjuster = ReferenceAdjuster::new();

        // Formula: =SUM(A1:A10)
        let ast = parse("=SUM(A1:A10)").unwrap();

        // Insert 3 rows before row 5
        let adjusted = adjuster.adjust_ast(
            &ast,
            &ShiftOperation::InsertRows {
                sheet_id: 0,
                before: 5,
                count: 3,
            },
        );

        // Range should expand: A1:A10 -> A1:A13
        if let ASTNodeType::Function { args, .. } = &adjusted.node_type {
            if let Some(first_arg) = args.first() {
                if let ASTNodeType::Reference { reference, .. } = &first_arg.node_type {
                    if let formualizer_parse::parser::ReferenceType::Range {
                        start_row,
                        end_row,
                        ..
                    } = reference
                    {
                        assert_eq!(start_row.unwrap_or(0), 1); // A1 start unchanged
                        assert_eq!(end_row.unwrap_or(0), 13); // A10 -> A13
                    }
                }
            }
        }
    }

    #[test]
    fn test_relative_reference_copy() {
        let adjuster = RelativeReferenceAdjuster::new(2, 3); // Move 2 rows down, 3 cols right

        // Formula: =A1+B2
        let ast = parse("=A1+B2").unwrap();
        let adjusted = adjuster.adjust_formula(&ast);

        // A1 -> D3, B2 -> E4
        if let ASTNodeType::BinaryOp { left, right, .. } = &adjusted.node_type {
            if let ASTNodeType::Reference {
                reference: left_ref,
                ..
            } = &left.node_type
            {
                if let formualizer_parse::parser::ReferenceType::Cell { row, col, .. } = left_ref {
                    assert_eq!(*row, 3); // A1 (1,1) -> D3 (3,4)
                    assert_eq!(*col, 4);
                }
            }
            if let ASTNodeType::Reference {
                reference: right_ref,
                ..
            } = &right.node_type
            {
                if let formualizer_parse::parser::ReferenceType::Cell { row, col, .. } = right_ref {
                    assert_eq!(*row, 4); // B2 (2,2) -> E4 (4,5)
                    assert_eq!(*col, 5);
                }
            }
        }
    }

    #[test]
    fn test_absolute_reference_preservation() {
        let adjuster = ReferenceAdjuster::new();

        // Test with absolute row references ($5)
        let cell_abs_row = CellRef::new(
            0,
            Coord::new(5, 2, true, false), // Row 5 absolute, col 2 relative
        );

        // Insert rows before the absolute reference
        let result = adjuster.adjust_cell_ref(
            &cell_abs_row,
            &ShiftOperation::InsertRows {
                sheet_id: 0,
                before: 3,
                count: 2,
            },
        );

        // Absolute row should not change
        assert!(result.is_some());
        let adjusted = result.unwrap();
        assert_eq!(adjusted.coord.row, 5); // Row stays at 5
        assert_eq!(adjusted.coord.col, 2); // Column unchanged
        assert!(adjusted.coord.row_abs());
        assert!(!adjusted.coord.col_abs());
    }

    #[test]
    fn test_absolute_column_preservation() {
        let adjuster = ReferenceAdjuster::new();

        // Test with absolute column references ($B)
        let cell_abs_col = CellRef::new(
            0,
            Coord::new(5, 2, false, true), // Row 5 relative, col 2 absolute
        );

        // Delete columns before the absolute reference
        let result = adjuster.adjust_cell_ref(
            &cell_abs_col,
            &ShiftOperation::DeleteColumns {
                sheet_id: 0,
                start: 1,
                count: 1,
            },
        );

        // Absolute column should not change
        assert!(result.is_some());
        let adjusted = result.unwrap();
        assert_eq!(adjusted.coord.row, 5); // Row unchanged
        assert_eq!(adjusted.coord.col, 2); // Column stays at 2 despite deletion
        assert!(!adjusted.coord.row_abs());
        assert!(adjusted.coord.col_abs());
    }

    #[test]
    fn test_mixed_absolute_relative_references() {
        let adjuster = ReferenceAdjuster::new();

        // Test 1: $A5 (col absolute, row relative) with row insertion
        let mixed1 = CellRef::new(
            0,
            Coord::new(5, 1, false, true), // Row 5 relative, col 1 absolute
        );

        let result1 = adjuster.adjust_cell_ref(
            &mixed1,
            &ShiftOperation::InsertRows {
                sheet_id: 0,
                before: 3,
                count: 2,
            },
        );

        assert!(result1.is_some());
        let adj1 = result1.unwrap();
        assert_eq!(adj1.coord.row, 7); // Row 5 -> 7 (shifted)
        assert_eq!(adj1.coord.col, 1); // Column stays at 1 (absolute)

        // Test 2: B$10 (col relative, row absolute) with column deletion
        let mixed2 = CellRef::new(
            0,
            Coord::new(10, 3, true, false), // Row 10 absolute, col 3 relative
        );

        let result2 = adjuster.adjust_cell_ref(
            &mixed2,
            &ShiftOperation::DeleteColumns {
                sheet_id: 0,
                start: 1,
                count: 1,
            },
        );

        assert!(result2.is_some());
        let adj2 = result2.unwrap();
        assert_eq!(adj2.coord.row, 10); // Row stays at 10 (absolute)
        assert_eq!(adj2.coord.col, 2); // Column 3 -> 2 (shifted left)
    }

    #[test]
    fn test_fully_absolute_reference() {
        let adjuster = ReferenceAdjuster::new();

        // Test $A$1 - fully absolute
        let fully_abs = CellRef::new(
            0,
            Coord::new(1, 1, true, true), // Both row and col absolute
        );

        // Try various operations - nothing should change

        // Insert rows
        let result1 = adjuster.adjust_cell_ref(
            &fully_abs,
            &ShiftOperation::InsertRows {
                sheet_id: 0,
                before: 1,
                count: 5,
            },
        );
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().coord.row, 1);
        assert_eq!(result1.unwrap().coord.col, 1);

        // Delete columns
        let result2 = adjuster.adjust_cell_ref(
            &fully_abs,
            &ShiftOperation::DeleteColumns {
                sheet_id: 0,
                start: 0,
                count: 1,
            },
        );
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().coord.row, 1);
        assert_eq!(result2.unwrap().coord.col, 1);
    }

    #[test]
    fn test_deleted_reference_becomes_ref_error() {
        let adjuster = ReferenceAdjuster::new();

        // Test deleting a cell that's referenced
        let cell = CellRef::new(
            0,
            Coord::new(5, 3, false, false), // Row 5, col 3, both relative
        );

        // Delete the row containing the cell
        let result = adjuster.adjust_cell_ref(
            &cell,
            &ShiftOperation::DeleteRows {
                sheet_id: 0,
                start: 5,
                count: 1,
            },
        );

        // Should return None to indicate deletion
        assert!(result.is_none());

        // Delete the column containing the cell
        let result2 = adjuster.adjust_cell_ref(
            &cell,
            &ShiftOperation::DeleteColumns {
                sheet_id: 0,
                start: 3,
                count: 1,
            },
        );

        // Should return None to indicate deletion
        assert!(result2.is_none());
    }

    #[test]
    fn test_range_expansion_on_insert() {
        let adjuster = ReferenceAdjuster::new();

        // Test that ranges expand when rows/cols are inserted within them
        let ast = parse("=SUM(B2:D10)").unwrap();

        // Insert rows in the middle of the range
        let adjusted = adjuster.adjust_ast(
            &ast,
            &ShiftOperation::InsertRows {
                sheet_id: 0,
                before: 5,
                count: 3,
            },
        );

        // Range should expand: B2:D10 -> B2:D13
        if let ASTNodeType::Function { args, .. } = &adjusted.node_type {
            if let Some(first_arg) = args.first() {
                if let ASTNodeType::Reference { reference, .. } = &first_arg.node_type {
                    if let formualizer_parse::parser::ReferenceType::Range {
                        start_row,
                        end_row,
                        start_col,
                        end_col,
                        ..
                    } = reference
                    {
                        assert_eq!(*start_row, Some(2)); // Start unchanged
                        assert_eq!(*end_row, Some(13)); // End expanded from 10 to 13
                        assert_eq!(*start_col, Some(2)); // B column
                        assert_eq!(*end_col, Some(4)); // D column
                    }
                }
            }
        }
    }

    #[test]
    fn test_range_contraction_on_delete() {
        let adjuster = ReferenceAdjuster::new();

        // Test that ranges contract when rows/cols are deleted within them
        let ast = parse("=SUM(A5:A20)").unwrap();

        // Delete rows in the middle of the range
        let adjusted = adjuster.adjust_ast(
            &ast,
            &ShiftOperation::DeleteRows {
                sheet_id: 0,
                start: 10,
                count: 5,
            },
        );

        // Range should contract: A5:A20 -> A5:A15
        if let ASTNodeType::Function { args, .. } = &adjusted.node_type {
            if let Some(first_arg) = args.first() {
                if let ASTNodeType::Reference { reference, .. } = &first_arg.node_type {
                    if let formualizer_parse::parser::ReferenceType::Range {
                        start_row,
                        end_row,
                        ..
                    } = reference
                    {
                        assert_eq!(*start_row, Some(5)); // Start unchanged
                        assert_eq!(*end_row, Some(15)); // End contracted from 20 to 15
                    }
                }
            }
        }
    }
}
