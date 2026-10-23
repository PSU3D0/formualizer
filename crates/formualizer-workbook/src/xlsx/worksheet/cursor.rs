//! Worksheet modal spine and namespace-path reconstruction.
//! Opaque subtrees retain their source depth; no source XML or evaluation text changes.
use super::*;

/// Semantic position; the tokenizer's namespace path is the only path stack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Position {
    Root,
    Worksheet,
    SheetData,
    Row,
    Cell,
    CellFormula,
    CellValue,
    CellInline,
    CellInlineText,
    Dimension,
    SheetFormatPr,
    Cols,
    AutoFilter,
    MergeCells,
    MergeCell,
    TableParts,
    TablePart,
    ExtLst,
    Other { depth: usize },
}

pub(super) struct Cursor {
    pub(super) position: Position,
    /// Position immediately outside an opaque subtree. Nested unknown
    /// elements share its depth witness instead of another path stack.
    pub(super) restore: Position,
}
impl Cursor {
    pub(super) fn new() -> Self {
        Self {
            position: Position::Root,
            restore: Position::Root,
        }
    }
    pub(super) fn open(&mut self, tag: Tag, local: &str, depth: usize) {
        if matches!(self.position, Position::Other { .. }) {
            return;
        }
        let next = if tag.main {
            match (self.position, tag.local) {
                (Position::Root, Local::Worksheet) => Position::Worksheet,
                (Position::Worksheet, Local::SheetData) => Position::SheetData,
                (Position::Worksheet, Local::Dimension) => Position::Dimension,
                (Position::Worksheet, Local::SheetFormatPr) => Position::SheetFormatPr,
                (Position::Worksheet, Local::AutoFilter) => Position::AutoFilter,
                (Position::Worksheet, Local::MergeCells) => Position::MergeCells,
                (Position::Worksheet, Local::TableParts) => Position::TableParts,
                (Position::Worksheet, _) if local == "cols" => Position::Cols,
                (Position::Worksheet, _) if local == "extLst" => Position::ExtLst,
                (Position::SheetData, Local::Row) => Position::Row,
                (Position::Row, Local::C) => Position::Cell,
                (Position::Cell, Local::F) => Position::CellFormula,
                (Position::Cell, Local::V) => Position::CellValue,
                (Position::Cell, Local::Is) => Position::CellInline,
                (Position::CellInline, Local::T) => Position::CellInlineText,
                (Position::MergeCells, Local::MergeCell) => Position::MergeCell,
                (Position::TableParts, Local::TablePart) => Position::TablePart,
                _ => Position::Other { depth },
            }
        } else {
            Position::Other { depth }
        };
        if matches!(next, Position::Other { .. }) {
            self.restore = self.position;
        }
        self.position = next;
    }
    /// Expand the ordinary modal spine only when a cold/rich event needs
    /// the original namespace path. Hot events carry no Element stack entry.
    pub(super) fn materialize<'a>(
        &self,
        path: &mut Vec<xml::Element<'a>>,
        depth: usize,
    ) -> Result<(), IoError> {
        while path.len() < depth {
            let local = match path.len() {
                2 => "row",
                3 => "c",
                4 => match self.position {
                    Position::CellFormula => "f",
                    Position::CellValue => "v",
                    _ => "is",
                },
                5 => "t",
                _ => {
                    return Err(unsupported(
                        "unavailable worksheet namespace spine",
                        "worksheet",
                    ));
                }
            };
            path.push(xml::Element {
                ns: xml::Ns::Main,
                local,
                qualified: local,
            });
        }
        Ok(())
    }
    pub(super) fn direct(&self, depth: usize) -> bool {
        matches!(
            self.position,
            Position::CellFormula | Position::CellValue | Position::CellInline
        ) || (depth == 5
            && matches!(self.position, Position::Other { depth: 5 })
            && self.restore == Position::Cell)
    }
    pub(super) fn close(&mut self, depth: usize) {
        self.position = match self.position {
            Position::Other { depth: outer } if outer == depth => self.restore,
            Position::Other { .. } => return,
            Position::Worksheet => Position::Root,
            Position::SheetData => Position::Worksheet,
            Position::Row => Position::SheetData,
            Position::Cell => Position::Row,
            Position::CellFormula | Position::CellValue | Position::CellInline => Position::Cell,
            Position::CellInlineText => Position::CellInline,
            Position::MergeCell => Position::MergeCells,
            Position::TablePart => Position::TableParts,
            Position::Dimension
            | Position::SheetFormatPr
            | Position::Cols
            | Position::AutoFilter
            | Position::MergeCells
            | Position::TableParts
            | Position::ExtLst => Position::Worksheet,
            Position::Root => Position::Root,
        };
    }
}
