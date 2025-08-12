use crate::traits::{CellData, SpreadsheetWriter};
use std::collections::BTreeMap;

/// Write operation journal entry.
#[derive(Clone, Debug)]
pub enum WriteOp {
    Cell {
        sheet: String,
        row: u32,
        col: u32,
        data: CellData,
    },
    Range {
        sheet: String,
        cells: BTreeMap<(u32, u32), CellData>,
    },
    Clear {
        sheet: String,
        start: (u32, u32),
        end: (u32, u32),
    },
    CreateSheet {
        name: String,
    },
    DeleteSheet {
        name: String,
    },
    RenameSheet {
        old: String,
        new: String,
    },
}

/// Transaction wrapper applying a set of write operations atomically to an underlying writer.
/// Atomicity guarantee is best-effort: either all operations are applied in insertion order
/// or none (if dropped without commit). Durability depends on backend flush/save semantics.
pub struct WriteTransaction<'a, W: SpreadsheetWriter> {
    writer: &'a mut W,
    operations: Vec<WriteOp>,
    committed: bool,
    validated: bool,
}

impl<'a, W: SpreadsheetWriter> WriteTransaction<'a, W> {
    pub fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            operations: Vec::new(),
            committed: false,
            validated: false,
        }
    }

    pub fn write_cell(&mut self, sheet: &str, row: u32, col: u32, data: CellData) -> &mut Self {
        self.operations.push(WriteOp::Cell {
            sheet: sheet.to_string(),
            row,
            col,
            data,
        });
        self
    }

    pub fn write_range(&mut self, sheet: &str, cells: BTreeMap<(u32, u32), CellData>) -> &mut Self {
        self.operations.push(WriteOp::Range {
            sheet: sheet.to_string(),
            cells,
        });
        self
    }

    pub fn clear_range(&mut self, sheet: &str, start: (u32, u32), end: (u32, u32)) -> &mut Self {
        self.operations.push(WriteOp::Clear {
            sheet: sheet.to_string(),
            start,
            end,
        });
        self
    }

    pub fn create_sheet(&mut self, name: &str) -> &mut Self {
        self.operations.push(WriteOp::CreateSheet {
            name: name.to_string(),
        });
        self
    }

    pub fn delete_sheet(&mut self, name: &str) -> &mut Self {
        self.operations.push(WriteOp::DeleteSheet {
            name: name.to_string(),
        });
        self
    }

    pub fn rename_sheet(&mut self, old: &str, new: &str) -> &mut Self {
        self.operations.push(WriteOp::RenameSheet {
            old: old.to_string(),
            new: new.to_string(),
        });
        self
    }

    /// Validate operations for basic invariants (order-sensitive checks can be added later).
    pub fn validate(&mut self) -> Result<(), W::Error> {
        self.validated = true; // placeholder for future validation logic
        Ok(())
    }

    /// Apply all operations; calls flush() at end for durability. Consumes self.
    pub fn commit(mut self) -> Result<(), W::Error> {
        if self.committed {
            return Ok(());
        }
        if !self.validated {
            self.validate()?;
        }
        for op in &self.operations {
            match op {
                WriteOp::Cell {
                    sheet,
                    row,
                    col,
                    data,
                } => {
                    self.writer.write_cell(sheet, *row, *col, data.clone())?;
                }
                WriteOp::Range { sheet, cells } => {
                    self.writer.write_range(sheet, cells.clone())?;
                }
                WriteOp::Clear { sheet, start, end } => {
                    self.writer.clear_range(sheet, *start, *end)?;
                }
                WriteOp::CreateSheet { name } => {
                    self.writer.create_sheet(name)?;
                }
                WriteOp::DeleteSheet { name } => {
                    self.writer.delete_sheet(name)?;
                }
                WriteOp::RenameSheet { old, new } => {
                    self.writer.rename_sheet(old, new)?;
                }
            }
        }
        // Attempt durability operations; propagate flush error if occurs
        self.writer.flush()?;
        // save may be a heavier operation; ignore error? propagate for now
        self.writer.save()?;
        self.committed = true;
        Ok(())
    }

    /// Explicit rollback just marks as committed (journal dropped without applying).
    pub fn rollback(mut self) {
        self.committed = true;
    }

    pub fn operations(&self) -> &[WriteOp] {
        &self.operations
    }
}

impl<'a, W: SpreadsheetWriter> Drop for WriteTransaction<'a, W> {
    fn drop(&mut self) {
        // If not committed, journal is discarded (rollback semantics)
    }
}
