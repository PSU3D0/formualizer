//! Opt-in adapter for upstream Umya 3.x, sharing the Umya 2.x adapter algorithms.
use umya_spreadsheet::Workbook as Spreadsheet;
use umya_spreadsheet3 as umya_spreadsheet;

impl WorkbookCompat for Spreadsheet {
    fn lookup_sheet_index(&self, index: usize) -> Option<&Worksheet> {
        self.sheet(index).ok()
    }
    fn lookup_sheet(&self, name: &str) -> Option<&Worksheet> {
        self.sheet_by_name(name).ok()
    }
    fn lookup_sheet_mut(&mut self, name: &str) -> Option<&mut Worksheet> {
        self.sheet_by_name_mut(name).ok()
    }
}

impl WorksheetCompat for Worksheet {
    fn adapter_cells(&self) -> Vec<&umya_spreadsheet::Cell> {
        self.cells()
    }
}

#[path = "umya3_import.rs"]
mod import;
use import::read_source as read_input;
pub use import::{read_document, read_document_path, read_document_reader};

#[path = "umya3_export.rs"]
mod export;
pub use export::{write_document, write_document_path, write_document_writer};

include!("umya_impl.rs");
