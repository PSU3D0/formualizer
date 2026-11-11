//! Array ownership retained by ordinary XLSX loaders.
use formualizer_eval::{engine::Engine, traits::EvaluationContext};

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
    #[cfg(any(feature = "umya", feature = "umya3"))]
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
        if self.dynamic {
            engine.declare_dynamic_array_anchor(sheet, self.row, self.col)
        } else {
            engine.declare_fixed_array_formula(
                sheet,
                self.row,
                self.col,
                self.last_row - self.row + 1,
                self.last_col - self.col + 1,
            )
        }
    }
}
