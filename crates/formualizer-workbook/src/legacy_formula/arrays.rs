//! Array ownership retained by ordinary XLSX loaders.
use formualizer_eval::{engine::Engine, traits::EvaluationContext};
use std::collections::BTreeSet;

pub(crate) fn populated_coordinates(
    arrow: &formualizer_eval::arrow_store::ArrowSheet,
) -> BTreeSet<(u32, u32)> {
    let mut coordinates = BTreeSet::new();
    for column in &arrow.columns {
        for (index, chunk) in column.chunks.iter().enumerate().chain(
            column
                .sparse_chunks
                .iter()
                .map(|(index, chunk)| (*index, chunk)),
        ) {
            let start = arrow
                .chunk_starts
                .get(index)
                .copied()
                .unwrap_or(index * arrow.chunk_rows);
            for (offset, tag) in chunk.type_tag.values().iter().enumerate() {
                if *tag != formualizer_eval::arrow_store::TypeTag::Empty as u8 {
                    coordinates.insert(((start + offset + 1) as u32, column.index + 1));
                }
            }
            for (offset, _) in chunk.overlay.iter() {
                coordinates.insert(((start + offset + 1) as u32, column.index + 1));
            }
        }
    }
    coordinates
}

#[derive(Clone, Debug)]
pub(crate) struct ArrayFormula {
    pub row: u32,
    pub col: u32,
    pub last_row: u32,
    pub last_col: u32,
    pub dynamic: bool,
}
impl ArrayFormula {
    pub fn single(row: u32, col: u32, dynamic: bool) -> Self {
        Self {
            row,
            col,
            last_row: row,
            last_col: col,
            dynamic,
        }
    }
    pub fn from_range(row: u32, col: u32, range: &str, dynamic: bool) -> Option<Self> {
        let range = range.trim();
        let (start, end) = range.split_once(':').unwrap_or((range, range));
        let (r1, c1, _, _) = formualizer_common::coord::parse_a1_1based(start).ok()?;
        let (r2, c2, _, _) = formualizer_common::coord::parse_a1_1based(end).ok()?;
        (r1 == row && c1 == col && r2 >= row && c2 >= col).then_some(Self {
            row,
            col,
            last_row: r2,
            last_col: c2,
            dynamic,
        })
    }
    pub fn owns_child(&self, row: u32, col: u32) -> bool {
        row >= self.row
            && row <= self.last_row
            && col >= self.col
            && col <= self.last_col
            && (row != self.row || col != self.col)
    }
    fn populated_children(
        &self,
        arrow: &formualizer_eval::arrow_store::ArrowSheet,
    ) -> Vec<(u32, u32)> {
        if self.row == self.last_row && self.col == self.last_col {
            return Vec::new();
        }
        let first_row0 = (self.row - 1) as usize;
        let last_row0 = (self.last_row - 1) as usize;
        let mut coordinates = BTreeSet::new();
        for column in &arrow.columns {
            let col1 = column.index + 1;
            if col1 < self.col || col1 > self.last_col {
                continue;
            }
            for (index, chunk) in column.chunks.iter().enumerate().chain(
                column
                    .sparse_chunks
                    .iter()
                    .map(|(index, chunk)| (*index, chunk)),
            ) {
                let start = arrow
                    .chunk_starts
                    .get(index)
                    .copied()
                    .unwrap_or(index * arrow.chunk_rows);
                let len = chunk.type_tag.len();
                let chunk_end = arrow
                    .chunk_starts
                    .get(index + 1)
                    .copied()
                    .unwrap_or_else(|| start.saturating_add(len.max(arrow.chunk_rows)));
                if chunk_end <= first_row0 || start > last_row0 {
                    continue;
                }
                if len > 0 {
                    let lo = first_row0.saturating_sub(start);
                    let hi = last_row0.saturating_sub(start).min(len - 1);
                    if lo <= hi {
                        let tags = chunk.type_tag.values();
                        for (offset, tag) in tags[lo..=hi].iter().enumerate() {
                            if *tag != formualizer_eval::arrow_store::TypeTag::Empty as u8 {
                                let row1 = (start + lo + offset + 1) as u32;
                                if self.owns_child(row1, col1) {
                                    coordinates.insert((row1, col1));
                                }
                            }
                        }
                    }
                }
                for (offset, _) in chunk.overlay.iter() {
                    let row1 = (start + offset + 1) as u32;
                    if self.owns_child(row1, col1) {
                        coordinates.insert((row1, col1));
                    }
                }
            }
        }
        coordinates.into_iter().collect()
    }
    pub fn declare<C: EvaluationContext>(
        &self,
        engine: &mut Engine<C>,
        sheet: &str,
    ) -> Result<(), formualizer_common::ExcelError> {
        // Empty source formulas and KeepCachedValue recovery need no array
        // ownership. Leave the loader's existing cached-value policy intact.
        let Some((Some(ast), _)) = engine.get_cell(sheet, self.row, self.col) else {
            return Ok(());
        };
        let rows = self.last_row - self.row + 1;
        let cols = self.last_col - self.col + 1;
        let over_cap = !self.dynamic
            && u64::from(rows) * u64::from(cols) > u64::from(engine.config.spill.max_spill_cells);
        // Discard the admitted anchor's saved output through the normal edit
        // path, then restore its formula. Cached fallback stays untouched above.
        engine.set_cell_value(
            sheet,
            self.row,
            self.col,
            formualizer_common::LiteralValue::Empty,
        )?;
        if over_cap {
            let error =
                formualizer_common::ExcelError::new(formualizer_common::ExcelErrorKind::Spill)
                    .with_message("Fixed array extent exceeds configured spill cell limit");
            return engine.set_cell_formula(
                sheet,
                self.row,
                self.col,
                formualizer_parse::ASTNode::new(
                    formualizer_parse::ASTNodeType::Literal(
                        formualizer_common::LiteralValue::Error(error),
                    ),
                    None,
                ),
            );
        }
        engine.set_cell_formula(sheet, self.row, self.col, ast)?;
        // Clear only populated cached children, after admission. Using the
        // normal cell edit path removes source caches without leaving an Empty
        // source overlay that would hide freshly calculated spill values.
        let children = engine
            .sheet_store()
            .sheet(sheet)
            .map(|arrow| self.populated_children(arrow))
            .unwrap_or_default();
        for (row, col) in children {
            // A real formula is a blocker, not a generated value cache.
            if !engine
                .get_cell(sheet, row, col)
                .is_some_and(|(ast, _)| ast.is_some())
            {
                engine.set_cell_value(sheet, row, col, formualizer_common::LiteralValue::Empty)?;
            }
        }
        let declaration = if self.dynamic {
            engine.declare_dynamic_array_anchor(sheet, self.row, self.col)
        } else {
            engine.declare_fixed_array_formula(sheet, self.row, self.col, rows, cols)
        };
        match declaration {
            Err(error) if error.kind == formualizer_common::ExcelErrorKind::Spill => {
                // A bounded ownership refusal is local to this formula, just
                // like an evaluation-time spill failure, not a workbook error.
                engine.set_cell_formula(
                    sheet,
                    self.row,
                    self.col,
                    formualizer_parse::ASTNode::new(
                        formualizer_parse::ASTNodeType::Literal(
                            formualizer_common::LiteralValue::Error(error),
                        ),
                        None,
                    ),
                )
            }
            other => other,
        }
    }
}
