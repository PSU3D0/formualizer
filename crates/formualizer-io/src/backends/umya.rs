#![cfg(feature = "umya")]

use crate::traits::{AccessGranularity, BackendCaps, CellData, SheetData, SpreadsheetReader};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use umya_spreadsheet::{reader::xlsx, CellRawValue, CellValue, Spreadsheet};

pub struct UmyaAdapter {
    workbook: RwLock<Spreadsheet>,
    lazy: bool,
}

impl UmyaAdapter {
    fn convert_cell_value(cv: &CellValue) -> Option<LiteralValue> {
        // Value portion
        let raw = cv.get_raw_value();
        // Skip empty
        if raw.is_empty() {
            return None;
        }
        // Errors
        if raw.is_error() {
            // Map string representation -> error kind
            let txt = cv.get_value();
            let kind = match txt.as_ref() {
                "#DIV/0!" => ExcelErrorKind::Div,
                "#N/A" => ExcelErrorKind::Na,
                "#NAME?" => ExcelErrorKind::Name,
                "#NULL!" => ExcelErrorKind::Null,
                "#NUM!" => ExcelErrorKind::Num,
                "#REF!" => ExcelErrorKind::Ref,
                "#VALUE!" => ExcelErrorKind::Value,
                _ => ExcelErrorKind::Value,
            };
            return Some(LiteralValue::Error(ExcelError::new(kind)));
        }
        match raw {
            CellRawValue::Numeric(n) => Some(LiteralValue::Number(*n)),
            CellRawValue::Bool(b) => Some(LiteralValue::Boolean(*b)),
            CellRawValue::String(s) => Some(LiteralValue::Text(s.to_string())),
            CellRawValue::RichText(rt) => Some(LiteralValue::Text(rt.get_text().to_string())),
            CellRawValue::Lazy(s) => {
                // attempt parse
                let txt = s.as_ref();
                if let Ok(n) = txt.parse::<f64>() {
                    Some(LiteralValue::Number(n))
                } else if txt.eq_ignore_ascii_case("TRUE") {
                    Some(LiteralValue::Boolean(true))
                } else if txt.eq_ignore_ascii_case("FALSE") {
                    Some(LiteralValue::Boolean(false))
                } else {
                    Some(LiteralValue::Text(txt.to_string()))
                }
            }
            CellRawValue::Error(_) => unreachable!(),
            CellRawValue::Empty => None,
        }
    }
}

impl SpreadsheetReader for UmyaAdapter {
    type Error = umya_spreadsheet::XlsxError;

    fn access_granularity(&self) -> AccessGranularity {
        AccessGranularity::Sheet
    }

    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            read: true,
            formulas: true,
            lazy_loading: self.lazy,
            random_access: false,
            styles: true,
            ..Default::default()
        }
    }

    fn sheet_names(&self) -> Result<Vec<String>, Self::Error> {
        // Need write lock to deserialize sheets lazily
        let mut wb = self.workbook.write();
        let count = wb.get_sheet_count();
        let mut names = Vec::with_capacity(count);
        for i in 0..count {
            wb.read_sheet(i); // ensure sheet deserialized
            if let Some(s) = wb.get_sheet(&i) {
                names.push(s.get_name().to_string());
            }
        }
        Ok(names)
    }

    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // Prefer lazy read for large files; expose both later
        let sheet = xlsx::lazy_read(path.as_ref())?; // workbook partially loaded
        Ok(Self {
            workbook: RwLock::new(sheet),
            lazy: true,
        })
    }

    fn open_reader(_reader: Box<dyn Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // Not implemented yet
        Err(umya_spreadsheet::XlsxError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "open_reader unsupported for UmyaAdapter",
        )))
    }

    fn open_bytes(_data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Err(umya_spreadsheet::XlsxError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "open_bytes unsupported for UmyaAdapter",
        )))
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error> {
        // Fallback: read whole sheet then filter
        let data = self.read_sheet(sheet)?;
        Ok(data
            .cells
            .into_iter()
            .filter(|((r, c), _)| *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1)
            .collect())
    }

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error> {
        let mut wb = self.workbook.write();
        // Ensure sheet deserialized
        wb.read_sheet_by_name(sheet);
        let ws = wb
            .get_sheet_by_name(sheet)
            .ok_or_else(|| umya_spreadsheet::XlsxError::CellError("sheet not found".into()))?;
        let mut cells_map: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
        for cell in ws.get_cell_collection() {
            // returns Vec<&Cell>
            let coord = cell.get_coordinate();
            let col = *coord.get_col_num();
            let row = *coord.get_row_num();
            let cv = cell.get_cell_value();
            let formula = if cv.is_formula() {
                let f = cv.get_formula();
                if f.is_empty() {
                    None
                } else {
                    Some(if f.starts_with('=') {
                        f.to_string()
                    } else {
                        format!("={}", f)
                    })
                }
            } else {
                None
            };
            let value = Self::convert_cell_value(cv);
            if value.is_none() && formula.is_none() {
                continue;
            }
            cells_map.insert(
                (row, col),
                CellData {
                    value,
                    formula,
                    style: None,
                },
            );
        }
        let dims = cells_map.keys().fold((0u32, 0u32), |mut acc, (r, c)| {
            if *r > acc.0 {
                acc.0 = *r;
            }
            if *c > acc.1 {
                acc.1 = *c;
            }
            acc
        });
        Ok(SheetData {
            cells: cells_map,
            dimensions: Some(dims),
            tables: vec![],
            named_ranges: vec![],
            date_system_1904: false,
            merged_cells: vec![],
            hidden: false,
        })
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        let wb = self.workbook.read();
        let ws = wb.get_sheet_by_name(sheet)?;
        let mut max_r = 0;
        let mut max_c = 0;
        for cell in ws.get_cell_collection() {
            let coord = cell.get_coordinate();
            let r = *coord.get_row_num();
            let c = *coord.get_col_num();
            if r > max_r {
                max_r = r;
            }
            if c > max_c {
                max_c = c;
            }
        }
        Some((max_r, max_c))
    }

    fn is_loaded(&self, sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        // In lazy mode, after first read_sheet call it's loaded; simplistic: if deserialized
        let wb = self.workbook.read();
        wb.get_sheet_by_name(sheet).is_some()
    }
}
