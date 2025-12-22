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
                if coord.row_abs() || coord.row() < *before {
                    // Absolute references or cells before insert point don't move
                    coord
                } else {
                    // Shift down
                    Coord::new(
                        coord.row() + count,
                        coord.col(),
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
                } else if coord.row() >= *start && coord.row() < start + count {
                    // Cell deleted
                    return None;
                } else if coord.row() >= start + count {
                    // Shift up
                    Coord::new(
                        coord.row() - count,
                        coord.col(),
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
                if coord.col_abs() || coord.col() < *before {
                    // Absolute references or cells before insert point don't move
                    coord
                } else {
                    // Shift right
                    Coord::new(
                        coord.row(),
                        coord.col() + count,
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
                } else if coord.col() >= *start && coord.col() < start + count {
                    // Cell deleted
                    return None;
                } else if coord.col() >= start + count {
                    // Shift left
                    Coord::new(
                        coord.row(),
                        coord.col() - count,
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

        let shared = reference.to_sheet_ref_lossy();

        match (reference, shared) {
            (
                ReferenceType::Cell {
                    sheet,
                    row_abs,
                    col_abs,
                    ..
                },
                Some(crate::reference::SharedRef::Cell(cell)),
            ) => {
                let sheet_id = match op {
                    ShiftOperation::InsertRows { sheet_id, .. }
                    | ShiftOperation::DeleteRows { sheet_id, .. }
                    | ShiftOperation::InsertColumns { sheet_id, .. }
                    | ShiftOperation::DeleteColumns { sheet_id, .. } => *sheet_id,
                };
                let temp_ref = CellRef::new(
                    sheet_id,
                    Coord::new(cell.coord.row(), cell.coord.col(), *row_abs, *col_abs),
                );

                match self.adjust_cell_ref(&temp_ref, op) {
                    None => ReferenceType::Cell {
                        sheet: Some("#REF".to_string()),
                        row: 0,
                        col: 0,
                        row_abs: *row_abs,
                        col_abs: *col_abs,
                    },
                    Some(adjusted) => ReferenceType::Cell {
                        sheet: sheet.clone(),
                        row: adjusted.coord.row() + 1,
                        col: adjusted.coord.col() + 1,
                        row_abs: *row_abs,
                        col_abs: *col_abs,
                    },
                }
            }
            (
                ReferenceType::Range {
                    sheet,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                    ..
                },
                Some(crate::reference::SharedRef::Range(range)),
            ) => {
                let is_unbounded_column = range.start_row.is_none() && range.end_row.is_none();
                let is_unbounded_row = range.start_col.is_none() && range.end_col.is_none();
                if is_unbounded_column || is_unbounded_row {
                    return reference.clone();
                }

                let sr = range.start_row;
                let sc = range.start_col;
                let er = range.end_row;
                let ec = range.end_col;

                let adjust_insert = |b: formualizer_common::AxisBound, before: u32, count: u32| {
                    if b.abs {
                        b.index
                    } else if b.index >= before {
                        b.index + count
                    } else {
                        b.index
                    }
                };

                let adjust_delete = |idx: u32, abs: bool, start: u32, count: u32| {
                    if abs {
                        idx
                    } else if idx >= start + count {
                        idx - count
                    } else if idx >= start {
                        start
                    } else {
                        idx
                    }
                };

                let (adj_sr0, adj_er0) = match op {
                    ShiftOperation::InsertRows { before, count, .. } => (
                        sr.map(|b| adjust_insert(b, *before, *count)),
                        er.map(|b| adjust_insert(b, *before, *count)),
                    ),
                    ShiftOperation::DeleteRows { start, count, .. } => match (sr, er) {
                        (Some(range_start), Some(range_end))
                            if !range_start.abs && !range_end.abs =>
                        {
                            let range_start = range_start.index;
                            let range_end = range_end.index;
                            if range_end < *start || range_start >= start + count {
                                let adj_start = if range_start >= start + count {
                                    range_start - count
                                } else {
                                    range_start
                                };
                                let adj_end = if range_end >= start + count {
                                    range_end - count
                                } else {
                                    range_end
                                };
                                (Some(adj_start), Some(adj_end))
                            } else if range_start >= *start && range_end < start + count {
                                return ReferenceType::Range {
                                    sheet: Some("#REF".to_string()),
                                    start_row: Some(0),
                                    start_col: Some(0),
                                    end_row: Some(0),
                                    end_col: Some(0),
                                    start_row_abs: *start_row_abs,
                                    start_col_abs: *start_col_abs,
                                    end_row_abs: *end_row_abs,
                                    end_col_abs: *end_col_abs,
                                };
                            } else {
                                let adj_start = if range_start < *start {
                                    range_start
                                } else {
                                    *start
                                };
                                let adj_end = if range_end >= start + count {
                                    range_end - count
                                } else {
                                    start.saturating_sub(1)
                                };
                                (Some(adj_start), Some(adj_end))
                            }
                        }
                        (Some(range_start), Some(range_end)) => {
                            let adj_start =
                                adjust_delete(range_start.index, range_start.abs, *start, *count);
                            let adj_end =
                                adjust_delete(range_end.index, range_end.abs, *start, *count);
                            (Some(adj_start), Some(adj_end))
                        }
                        _ => (
                            sr.map(|b| adjust_delete(b.index, b.abs, *start, *count)),
                            er.map(|b| adjust_delete(b.index, b.abs, *start, *count)),
                        ),
                    },
                    _ => (sr.map(|b| b.index), er.map(|b| b.index)),
                };

                let (adj_sc0, adj_ec0) = match op {
                    ShiftOperation::InsertColumns { before, count, .. } => (
                        sc.map(|b| adjust_insert(b, *before, *count)),
                        ec.map(|b| adjust_insert(b, *before, *count)),
                    ),
                    ShiftOperation::DeleteColumns { start, count, .. } => match (sc, ec) {
                        (Some(range_start), Some(range_end))
                            if !range_start.abs && !range_end.abs =>
                        {
                            let range_start = range_start.index;
                            let range_end = range_end.index;
                            if range_end < *start || range_start >= start + count {
                                let adj_start = if range_start >= start + count {
                                    range_start - count
                                } else {
                                    range_start
                                };
                                let adj_end = if range_end >= start + count {
                                    range_end - count
                                } else {
                                    range_end
                                };
                                (Some(adj_start), Some(adj_end))
                            } else if range_start >= *start && range_end < start + count {
                                return ReferenceType::Range {
                                    sheet: Some("#REF".to_string()),
                                    start_row: Some(0),
                                    start_col: Some(0),
                                    end_row: Some(0),
                                    end_col: Some(0),
                                    start_row_abs: *start_row_abs,
                                    start_col_abs: *start_col_abs,
                                    end_row_abs: *end_row_abs,
                                    end_col_abs: *end_col_abs,
                                };
                            } else {
                                let adj_start = if range_start < *start {
                                    range_start
                                } else {
                                    *start
                                };
                                let adj_end = if range_end >= start + count {
                                    range_end - count
                                } else {
                                    start.saturating_sub(1)
                                };
                                (Some(adj_start), Some(adj_end))
                            }
                        }
                        (Some(range_start), Some(range_end)) => {
                            let adj_start =
                                adjust_delete(range_start.index, range_start.abs, *start, *count);
                            let adj_end =
                                adjust_delete(range_end.index, range_end.abs, *start, *count);
                            (Some(adj_start), Some(adj_end))
                        }
                        _ => (
                            sc.map(|b| adjust_delete(b.index, b.abs, *start, *count)),
                            ec.map(|b| adjust_delete(b.index, b.abs, *start, *count)),
                        ),
                    },
                    _ => (sc.map(|b| b.index), ec.map(|b| b.index)),
                };

                ReferenceType::Range {
                    sheet: sheet.clone(),
                    start_row: adj_sr0.map(|i| i + 1),
                    start_col: adj_sc0.map(|i| i + 1),
                    end_row: adj_er0.map(|i| i + 1),
                    end_col: adj_ec0.map(|i| i + 1),
                    start_row_abs: *start_row_abs,
                    start_col_abs: *start_col_abs,
                    end_row_abs: *end_row_abs,
                    end_col_abs: *end_col_abs,
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

        let Some(shared) = reference.to_sheet_ref_lossy() else {
            return reference.clone();
        };

        match (reference, shared) {
            (ReferenceType::Cell { sheet, .. }, crate::reference::SharedRef::Cell(cell)) => {
                let owned = cell.into_owned();
                let row0 = owned.coord.row();
                let col0 = owned.coord.col();
                let row_abs = owned.coord.row_abs();
                let col_abs = owned.coord.col_abs();

                let new_row0 = if row_abs {
                    row0
                } else {
                    (row0 as i32 + self.row_offset).max(0) as u32
                };
                let new_col0 = if col_abs {
                    col0
                } else {
                    (col0 as i32 + self.col_offset).max(0) as u32
                };

                ReferenceType::Cell {
                    sheet: sheet.clone(),
                    row: new_row0 + 1,
                    col: new_col0 + 1,
                    row_abs,
                    col_abs,
                }
            }
            (ReferenceType::Range { sheet, .. }, crate::reference::SharedRef::Range(range)) => {
                let owned = range.into_owned();

                let adj_axis = |b: formualizer_common::AxisBound, off: i32| {
                    if b.abs {
                        b.index
                    } else {
                        (b.index as i32 + off).max(0) as u32
                    }
                };

                let adj_start_row = owned.start_row.map(|b| adj_axis(b, self.row_offset) + 1);
                let adj_start_col = owned.start_col.map(|b| adj_axis(b, self.col_offset) + 1);
                let adj_end_row = owned.end_row.map(|b| adj_axis(b, self.row_offset) + 1);
                let adj_end_col = owned.end_col.map(|b| adj_axis(b, self.col_offset) + 1);

                let start_row_abs = owned.start_row.map(|b| b.abs).unwrap_or(false);
                let start_col_abs = owned.start_col.map(|b| b.abs).unwrap_or(false);
                let end_row_abs = owned.end_row.map(|b| b.abs).unwrap_or(false);
                let end_col_abs = owned.end_col.map(|b| b.abs).unwrap_or(false);

                ReferenceType::Range {
                    sheet: sheet.clone(),
                    start_row: adj_start_row,
                    start_col: adj_start_col,
                    end_row: adj_end_row,
                    end_col: adj_end_col,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                }
            }
            _ => reference.clone(),
        }
    }
}

/// Helper for adjusting references to moved ranges.
/// This is used when a block of cells is moved; any formula references to cells
/// fully inside the source rectangle are translated to the destination.
pub struct MoveReferenceAdjuster {
    from_sheet_id: crate::SheetId,
    from_sheet_name: String,
    from_start_row: u32,
    from_start_col: u32,
    from_end_row: u32,
    from_end_col: u32,
    to_sheet_id: crate::SheetId,
    to_sheet_name: String,
    row_offset: i32,
    col_offset: i32,
}

impl MoveReferenceAdjuster {
    pub fn new(
        from_sheet_id: crate::SheetId,
        from_sheet_name: String,
        from_start_row: u32,
        from_start_col: u32,
        from_end_row: u32,
        from_end_col: u32,
        to_sheet_id: crate::SheetId,
        to_sheet_name: String,
        row_offset: i32,
        col_offset: i32,
    ) -> Self {
        Self {
            from_sheet_id,
            from_sheet_name,
            from_start_row,
            from_start_col,
            from_end_row,
            from_end_col,
            to_sheet_id,
            to_sheet_name,
            row_offset,
            col_offset,
        }
    }

    pub fn adjust_if_references(
        &self,
        formula: &ASTNode,
        formula_sheet_id: crate::SheetId,
    ) -> Option<ASTNode> {
        let (adjusted, changed) = self.adjust_ast_inner(formula, formula_sheet_id);
        if changed { Some(adjusted) } else { None }
    }

    fn adjust_ast_inner(&self, ast: &ASTNode, formula_sheet_id: crate::SheetId) -> (ASTNode, bool) {
        match &ast.node_type {
            ASTNodeType::Reference {
                original,
                reference,
            } => {
                let (adjusted_ref, changed) = self.adjust_reference(reference, formula_sheet_id);
                if !changed {
                    return (ast.clone(), false);
                }
                (
                    ASTNode {
                        node_type: ASTNodeType::Reference {
                            original: original.clone(),
                            reference: adjusted_ref,
                        },
                        source_token: ast.source_token.clone(),
                        contains_volatile: ast.contains_volatile,
                    },
                    true,
                )
            }
            ASTNodeType::BinaryOp { op, left, right } => {
                let (l_adj, l_ch) = self.adjust_ast_inner(left, formula_sheet_id);
                let (r_adj, r_ch) = self.adjust_ast_inner(right, formula_sheet_id);
                if !l_ch && !r_ch {
                    return (ast.clone(), false);
                }
                (
                    ASTNode {
                        node_type: ASTNodeType::BinaryOp {
                            op: op.clone(),
                            left: Box::new(l_adj),
                            right: Box::new(r_adj),
                        },
                        source_token: ast.source_token.clone(),
                        contains_volatile: ast.contains_volatile,
                    },
                    true,
                )
            }
            ASTNodeType::UnaryOp { op, expr } => {
                let (e_adj, e_ch) = self.adjust_ast_inner(expr, formula_sheet_id);
                if !e_ch {
                    return (ast.clone(), false);
                }
                (
                    ASTNode {
                        node_type: ASTNodeType::UnaryOp {
                            op: op.clone(),
                            expr: Box::new(e_adj),
                        },
                        source_token: ast.source_token.clone(),
                        contains_volatile: ast.contains_volatile,
                    },
                    true,
                )
            }
            ASTNodeType::Function { name, args } => {
                let mut any = false;
                let new_args: Vec<_> = args
                    .iter()
                    .map(|a| {
                        let (adj, ch) = self.adjust_ast_inner(a, formula_sheet_id);
                        any |= ch;
                        adj
                    })
                    .collect();
                if !any {
                    return (ast.clone(), false);
                }
                (
                    ASTNode {
                        node_type: ASTNodeType::Function {
                            name: name.clone(),
                            args: new_args,
                        },
                        source_token: ast.source_token.clone(),
                        contains_volatile: ast.contains_volatile,
                    },
                    true,
                )
            }
            ASTNodeType::Array(rows) => {
                let mut any = false;
                let new_rows: Vec<_> = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|c| {
                                let (adj, ch) = self.adjust_ast_inner(c, formula_sheet_id);
                                any |= ch;
                                adj
                            })
                            .collect()
                    })
                    .collect();
                if !any {
                    return (ast.clone(), false);
                }
                (
                    ASTNode {
                        node_type: ASTNodeType::Array(new_rows),
                        source_token: ast.source_token.clone(),
                        contains_volatile: ast.contains_volatile,
                    },
                    true,
                )
            }
            _ => (ast.clone(), false),
        }
    }

    fn adjust_reference(
        &self,
        reference: &formualizer_parse::parser::ReferenceType,
        formula_sheet_id: crate::SheetId,
    ) -> (formualizer_parse::parser::ReferenceType, bool) {
        use formualizer_parse::parser::ReferenceType;

        let sheet_matches_source = |sheet: &Option<String>| {
            if let Some(name) = sheet.as_deref() {
                name == self.from_sheet_name
            } else {
                formula_sheet_id == self.from_sheet_id
            }
        };

        if !sheet_matches_source(match reference {
            ReferenceType::Cell { sheet, .. } => sheet,
            ReferenceType::Range { sheet, .. } => sheet,
            _ => &None,
        }) {
            return (reference.clone(), false);
        }

        let Some(shared) = reference.to_sheet_ref_lossy() else {
            return (reference.clone(), false);
        };

        match (reference, shared) {
            (ReferenceType::Cell { sheet, .. }, crate::reference::SharedRef::Cell(cell)) => {
                let owned = cell.into_owned();
                let row0 = owned.coord.row();
                let col0 = owned.coord.col();
                let row_abs = owned.coord.row_abs();
                let col_abs = owned.coord.col_abs();

                if row0 < self.from_start_row
                    || row0 > self.from_end_row
                    || col0 < self.from_start_col
                    || col0 > self.from_end_col
                {
                    return (reference.clone(), false);
                }

                let new_row0 = (row0 as i32 + self.row_offset).max(0) as u32;
                let new_col0 = (col0 as i32 + self.col_offset).max(0) as u32;

                let new_sheet = if self.to_sheet_id != self.from_sheet_id {
                    Some(self.to_sheet_name.clone())
                } else {
                    sheet.clone()
                };

                (
                    ReferenceType::Cell {
                        sheet: new_sheet,
                        row: new_row0 + 1,
                        col: new_col0 + 1,
                        row_abs,
                        col_abs,
                    },
                    true,
                )
            }
            (ReferenceType::Range { sheet, .. }, crate::reference::SharedRef::Range(range)) => {
                let owned = range.into_owned();
                let (Some(sr), Some(sc), Some(er), Some(ec)) = (
                    owned.start_row,
                    owned.start_col,
                    owned.end_row,
                    owned.end_col,
                ) else {
                    return (reference.clone(), false);
                };

                let sr0 = sr.index;
                let sc0 = sc.index;
                let er0 = er.index;
                let ec0 = ec.index;
                let start_row_abs = sr.abs;
                let start_col_abs = sc.abs;
                let end_row_abs = er.abs;
                let end_col_abs = ec.abs;

                let fully_contained = sr0 >= self.from_start_row
                    && er0 <= self.from_end_row
                    && sc0 >= self.from_start_col
                    && ec0 <= self.from_end_col;
                if !fully_contained {
                    return (reference.clone(), false);
                }

                let new_sr0 = (sr0 as i32 + self.row_offset).max(0) as u32;
                let new_er0 = (er0 as i32 + self.row_offset).max(0) as u32;
                let new_sc0 = (sc0 as i32 + self.col_offset).max(0) as u32;
                let new_ec0 = (ec0 as i32 + self.col_offset).max(0) as u32;

                let new_sheet = if self.to_sheet_id != self.from_sheet_id {
                    Some(self.to_sheet_name.clone())
                } else {
                    sheet.clone()
                };

                (
                    ReferenceType::Range {
                        sheet: new_sheet,
                        start_row: Some(new_sr0 + 1),
                        start_col: Some(new_sc0 + 1),
                        end_row: Some(new_er0 + 1),
                        end_col: Some(new_ec0 + 1),
                        start_row_abs,
                        start_col_abs,
                        end_row_abs,
                        end_col_abs,
                    },
                    true,
                )
            }
            _ => (reference.clone(), false),
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
                reference: formualizer_parse::parser::ReferenceType::Cell { row, col, .. },
                ..
            } = &left.node_type
            {
                assert_eq!(*row, 5); // A5 unchanged
                assert_eq!(*col, 1);
            }
            if let ASTNodeType::Reference {
                reference: formualizer_parse::parser::ReferenceType::Cell { row, col, .. },
                ..
            } = &right.node_type
            {
                assert_eq!(*row, 12); // B10 -> B12
                assert_eq!(*col, 2);
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
                reference:
                    formualizer_parse::parser::ReferenceType::Cell {
                        sheet, row, col, ..
                    },
                ..
            } = &left.node_type
            {
                assert_eq!(sheet.as_deref(), Some("#REF"));
                assert_eq!(*row, 0);
                assert_eq!(*col, 0);
            }
            if let ASTNodeType::Reference {
                reference: formualizer_parse::parser::ReferenceType::Cell { row, col, .. },
                ..
            } = &right.node_type
            {
                assert_eq!(*row, 1); // Row unchanged
                assert_eq!(*col, 4); // F1 (col 6) -> D1 (col 4)
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
        if let ASTNodeType::Function { args, .. } = &adjusted.node_type
            && let Some(ASTNodeType::Reference {
                reference:
                    formualizer_parse::parser::ReferenceType::Range {
                        start_row, end_row, ..
                    },
                ..
            }) = args.first().map(|arg| &arg.node_type)
        {
            assert_eq!(start_row.unwrap_or(0), 1); // A1 start unchanged
            assert_eq!(end_row.unwrap_or(0), 13); // A10 -> A13
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
                reference: formualizer_parse::parser::ReferenceType::Cell { row, col, .. },
                ..
            } = &left.node_type
            {
                assert_eq!(*row, 3); // A1 (1,1) -> D3 (3,4)
                assert_eq!(*col, 4);
            }
            if let ASTNodeType::Reference {
                reference: formualizer_parse::parser::ReferenceType::Cell { row, col, .. },
                ..
            } = &right.node_type
            {
                assert_eq!(*row, 4); // B2 (2,2) -> E4 (4,5)
                assert_eq!(*col, 5);
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
        assert_eq!(adjusted.coord.row(), 5); // Row stays at 5
        assert_eq!(adjusted.coord.col(), 2); // Column unchanged
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
        assert_eq!(adjusted.coord.row(), 5); // Row unchanged
        assert_eq!(adjusted.coord.col(), 2); // Column stays at 2 despite deletion
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
        assert_eq!(adj1.coord.row(), 7); // Row 5 -> 7 (shifted)
        assert_eq!(adj1.coord.col(), 1); // Column stays at 1 (absolute)

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
        assert_eq!(adj2.coord.row(), 10); // Row stays at 10 (absolute)
        assert_eq!(adj2.coord.col(), 2); // Column 3 -> 2 (shifted left)
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
        assert_eq!(result1.unwrap().coord.row(), 1);
        assert_eq!(result1.unwrap().coord.col(), 1);

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
        assert_eq!(result2.unwrap().coord.row(), 1);
        assert_eq!(result2.unwrap().coord.col(), 1);
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
        if let ASTNodeType::Function { args, .. } = &adjusted.node_type
            && let Some(ASTNodeType::Reference {
                reference:
                    formualizer_parse::parser::ReferenceType::Range {
                        start_row,
                        end_row,
                        start_col,
                        end_col,
                        ..
                    },
                ..
            }) = args.first().map(|arg| &arg.node_type)
        {
            assert_eq!(*start_row, Some(2)); // Start unchanged
            assert_eq!(*end_row, Some(13)); // End expanded from 10 to 13
            assert_eq!(*start_col, Some(2)); // B column
            assert_eq!(*end_col, Some(4)); // D column
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
        if let ASTNodeType::Function { args, .. } = &adjusted.node_type
            && let Some(ASTNodeType::Reference {
                reference:
                    formualizer_parse::parser::ReferenceType::Range {
                        start_row, end_row, ..
                    },
                ..
            }) = args.first().map(|arg| &arg.node_type)
        {
            assert_eq!(*start_row, Some(5)); // Start unchanged
            assert_eq!(*end_row, Some(15)); // End contracted from 20 to 15
        }
    }
}
