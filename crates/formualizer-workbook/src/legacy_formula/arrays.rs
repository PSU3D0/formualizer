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
    pub fn from_range(row: u32, col: u32, range: &str, dynamic: bool) -> Option<Self> {
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
        // Discard the admitted anchor's saved output through the normal edit
        // path, then restore its formula. Cached fallback stays untouched above.
        engine.set_cell_value(
            sheet,
            self.row,
            self.col,
            formualizer_common::LiteralValue::Empty,
        )?;
        engine.set_cell_formula(sheet, self.row, self.col, ast)?;
        // Clear only populated cached children, after admission. Using the
        // normal cell edit path removes source caches without leaving an Empty
        // source overlay that would hide freshly calculated spill values.
        let children: Vec<_> = engine
            .sheet_store()
            .sheet(sheet)
            .map(populated_coordinates)
            .unwrap_or_default()
            .into_iter()
            .filter(|&(row, col)| self.owns_child(row, col))
            .collect();
        for (row, col) in children {
            // A real formula is a blocker, not a generated value cache.
            if !engine
                .get_cell(sheet, row, col)
                .is_some_and(|(ast, _)| ast.is_some())
            {
                engine.set_cell_value(sheet, row, col, formualizer_common::LiteralValue::Empty)?;
            }
        }
        let rows = self.last_row - self.row + 1;
        let cols = self.last_col - self.col + 1;
        let over_cap = !self.dynamic
            && u64::from(rows) * u64::from(cols) > u64::from(engine.config.spill.max_spill_cells);
        let declaration = if over_cap {
            Err(
                formualizer_common::ExcelError::new(formualizer_common::ExcelErrorKind::Spill)
                    .with_message("Fixed array extent exceeds configured spill cell limit"),
            )
        } else if self.dynamic {
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
