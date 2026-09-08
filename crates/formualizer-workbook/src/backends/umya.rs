//! Adapter for the existing Umya 2.x API.
use umya_spreadsheet::Spreadsheet;

impl WorkbookCompat for Spreadsheet {
    fn lookup_sheet_index(&self, index: usize) -> Option<&Worksheet> {
        self.get_sheet(&index)
    }
    fn lookup_sheet(&self, name: &str) -> Option<&Worksheet> {
        self.get_sheet_by_name(name)
    }
    fn lookup_sheet_mut(&mut self, name: &str) -> Option<&mut Worksheet> {
        self.get_sheet_by_name_mut(name)
    }
}

impl WorksheetCompat for Worksheet {
    fn adapter_cells(&self) -> Vec<&umya_spreadsheet::Cell> {
        self.get_cell_collection()
    }
}

fn read_input(mut reader: impl std::io::Read) -> Result<Vec<u8>, umya_spreadsheet::XlsxError> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn read_document(bytes: &[u8]) -> Result<Spreadsheet, umya_spreadsheet::XlsxError> {
    umya_spreadsheet::reader::xlsx::read_reader(std::io::Cursor::new(bytes), true)
}

fn write_document_path(
    book: &Spreadsheet,
    path: impl AsRef<std::path::Path>,
) -> Result<(), umya_spreadsheet::XlsxError> {
    umya_spreadsheet::writer::xlsx::write(book, path)
}
fn write_document_writer(
    book: &Spreadsheet,
    writer: impl std::io::Write,
) -> Result<(), umya_spreadsheet::XlsxError> {
    umya_spreadsheet::writer::xlsx::write_writer(book, writer)
}

include!("umya_impl.rs");

impl UmyaAdapter {
    // The existing Workbook save convenience API constructs this 2.x adapter.
    pub(crate) fn set_date_system(&mut self, date_system: formualizer_eval::engine::DateSystem) {
        self.date_system = date_system;
    }
}
