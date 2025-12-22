use crate::traits::{
    AccessGranularity, BackendCaps, CellData, NamedRange, NamedRangeScope, SheetData,
    SpreadsheetReader, SpreadsheetWriter,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue, RangeAddress};
use formualizer_parse::parser::ReferenceType;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io::Read;
use std::path::Path;
use umya_spreadsheet::{
    CellRawValue, CellValue, Spreadsheet,
    reader::xlsx,
    structs::{DefinedName, Worksheet},
};

pub struct UmyaAdapter {
    workbook: RwLock<Spreadsheet>,
    lazy: bool,
    original_path: Option<std::path::PathBuf>,
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
            write: true,
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
        // Use full read (not lazy) so that save operations don't hit deserialization assertions
        let sheet = xlsx::read(path.as_ref())?;
        Ok(Self {
            workbook: RwLock::new(sheet),
            lazy: false,
            original_path: Some(path.as_ref().to_path_buf()),
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
            tables: Self::collect_tables(ws),
            named_ranges: Self::collect_named_ranges(sheet, &wb, ws),
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

impl SpreadsheetWriter for UmyaAdapter {
    type Error = umya_spreadsheet::XlsxError;

    fn write_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        data: CellData,
    ) -> Result<(), Self::Error> {
        let mut wb = self.workbook.write();
        // If sheet missing create before any deserialize attempts
        if wb.get_sheet_by_name(sheet).is_none() {
            let _ = wb.new_sheet(sheet);
            // Ensure it's marked deserialized for writer
            wb.read_sheet_collection();
        }
        // Now safely attempt to access mut sheet
        let ws = wb.get_sheet_by_name_mut(sheet).ok_or_else(|| {
            umya_spreadsheet::XlsxError::CellError("sheet create/load failure".into())
        })?;
        // umya uses (col,row)
        let cell = ws.get_cell_mut((col, row));
        if let Some(v) = data.value {
            match v {
                LiteralValue::Number(n) => {
                    cell.set_value_number(n);
                }
                LiteralValue::Int(i) => {
                    cell.set_value_number(i as f64);
                }
                LiteralValue::Boolean(b) => {
                    cell.set_value_bool(b);
                }
                LiteralValue::Text(s) => {
                    cell.set_value(s);
                }
                LiteralValue::Error(e) => {
                    cell.set_value(e.kind.to_string());
                }
                LiteralValue::Empty => {
                    cell.set_blank();
                }
                LiteralValue::Array(_arr) => {
                    // Flatten first element as placeholder (TODO: proper array spill to grid)
                    cell.set_value("#ARRAY");
                }
                LiteralValue::Date(d) => {
                    cell.set_value(d.to_string());
                }
                LiteralValue::DateTime(dt) => {
                    cell.set_value(dt.to_string());
                }
                LiteralValue::Time(t) => {
                    cell.set_value(t.format("%H:%M:%S").to_string());
                }
                LiteralValue::Duration(dur) => {
                    cell.set_value(format!("PT{}S", dur.num_seconds()));
                }
                LiteralValue::Pending => {
                    cell.set_value("#PENDING");
                }
            }
        } else {
            // Clear value if none provided
            cell.set_blank();
        }
        if let Some(f) = data.formula {
            if let Some(stripped) = f.strip_prefix('=') {
                cell.set_formula(stripped); // umya stores formula without leading '='
            } else {
                cell.set_formula(f);
            }
        }
        Ok(())
    }

    fn write_range(
        &mut self,
        sheet: &str,
        cells: BTreeMap<(u32, u32), CellData>,
    ) -> Result<(), Self::Error> {
        for ((r, c), cd) in cells.into_iter() {
            self.write_cell(sheet, r, c, cd)?;
        }
        Ok(())
    }

    fn clear_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<(), Self::Error> {
        let mut wb = self.workbook.write();
        wb.read_sheet_by_name(sheet);
        let ws = match wb.get_sheet_by_name_mut(sheet) {
            Some(s) => s,
            None => return Ok(()), // nothing to clear
        };
        for r in start.0..=end.0 {
            for c in start.1..=end.1 {
                ws.get_cell_mut((c, r)).set_blank();
            }
        }
        Ok(())
    }

    fn create_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        let mut wb = self.workbook.write();
        if wb.get_sheet_by_name(name).is_none() {
            let _ = wb.new_sheet(name);
        }
        Ok(())
    }

    fn delete_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        let mut wb = self.workbook.write();
        let _ = wb.remove_sheet_by_name(name); // ignore error if sheet not present
        Ok(())
    }

    fn rename_sheet(&mut self, old: &str, new: &str) -> Result<(), Self::Error> {
        let mut wb = self.workbook.write();
        if let Some(s) = wb.get_sheet_by_name_mut(old) {
            s.set_name(new);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        // No-op: writes are already in-memory. Keep for interface parity.
        Ok(())
    }

    fn save_to<'a>(
        &mut self,
        dest: crate::traits::SaveDestination<'a>,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        use crate::traits::SaveDestination;
        match dest {
            SaveDestination::InPlace => {
                let path = self.original_path.as_ref().ok_or_else(|| {
                    umya_spreadsheet::XlsxError::Io(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "InPlace save unavailable: no original path",
                    ))
                })?;
                let mut wb = self.workbook.write();
                // Force deserialize each sheet explicitly (more robust than collection helper alone)
                let count = wb.get_sheet_count();
                for i in 0..count {
                    wb.read_sheet(i);
                }
                umya_spreadsheet::writer::xlsx::write(&wb, path)?;
                Ok(None)
            }
            SaveDestination::Path(p) => {
                let mut wb = self.workbook.write();
                let count = wb.get_sheet_count();
                for i in 0..count {
                    wb.read_sheet(i);
                }
                umya_spreadsheet::writer::xlsx::write(&wb, p)?;
                Ok(None)
            }
            SaveDestination::Writer(w) => {
                let mut wb = self.workbook.write();
                let count = wb.get_sheet_count();
                for i in 0..count {
                    wb.read_sheet(i);
                }
                umya_spreadsheet::writer::xlsx::write_writer(&wb, w)?;
                Ok(None)
            }
            SaveDestination::Bytes => {
                let mut wb = self.workbook.write();
                let count = wb.get_sheet_count();
                for i in 0..count {
                    wb.read_sheet(i);
                }
                let mut buf: Vec<u8> = Vec::new();
                umya_spreadsheet::writer::xlsx::write_writer(&wb, &mut buf)?;
                Ok(Some(buf))
            }
        }
    }
}

impl UmyaAdapter {
    fn collect_named_ranges(
        sheet_name: &str,
        workbook: &Spreadsheet,
        worksheet: &Worksheet,
    ) -> Vec<NamedRange> {
        let mut ranges = Vec::new();
        let mut seen: HashSet<(NamedRangeScope, String)> = HashSet::new();

        for defined in worksheet.get_defined_names() {
            if let Some(named) = Self::convert_defined_name(defined, sheet_name) {
                let key = (named.scope.clone(), named.name.clone());
                if seen.insert(key) {
                    ranges.push(named);
                }
            }
        }

        for defined in workbook.get_defined_names() {
            if let Some(named) = Self::convert_defined_name(defined, sheet_name) {
                let key = (named.scope.clone(), named.name.clone());
                if seen.insert(key) {
                    ranges.push(named);
                }
            }
        }

        ranges
    }

    fn collect_tables(worksheet: &Worksheet) -> Vec<crate::traits::TableDefinition> {
        worksheet
            .get_tables()
            .iter()
            .map(|t| {
                let (beg, end) = t.get_area();
                let headers = t
                    .get_columns()
                    .iter()
                    .map(|c| c.get_name().to_string())
                    .collect();
                crate::traits::TableDefinition {
                    name: t.get_name().to_string(),
                    range: (
                        *beg.get_row_num(),
                        *beg.get_col_num(),
                        *end.get_row_num(),
                        *end.get_col_num(),
                    ),
                    headers,
                    totals_row: *t.get_totals_row_shown(),
                }
            })
            .collect()
    }

    fn convert_defined_name(defined: &DefinedName, current_sheet: &str) -> Option<NamedRange> {
        let raw = defined.get_address();
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.contains(',') {
            return None;
        }

        let reference = ReferenceType::from_string(trimmed).ok()?;

        let (sheet_name, start_row, start_col, end_row, end_col) = match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet = sheet.unwrap_or_else(|| current_sheet.to_string());
                (sheet, row, col, row, col)
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let sr = start_row?;
                let sc = start_col?;
                let er = end_row.unwrap_or(sr);
                let ec = end_col.unwrap_or(sc);
                let sheet = sheet.unwrap_or_else(|| current_sheet.to_string());
                (sheet, sr, sc, er, ec)
            }
            _ => return None,
        };

        if sheet_name != current_sheet {
            return None;
        }

        let scope = if defined.has_local_sheet_id() {
            NamedRangeScope::Sheet
        } else {
            NamedRangeScope::Workbook
        };

        let address = RangeAddress::new(sheet_name, start_row, start_col, end_row, end_col).ok()?;

        Some(NamedRange {
            name: defined.get_name().to_string(),
            scope,
            address,
        })
    }
}

impl<R> formualizer_eval::engine::ingest::EngineLoadStream<R> for UmyaAdapter
where
    R: formualizer_eval::traits::EvaluationContext,
{
    type Error = crate::error::IoError;

    fn stream_into_engine(
        &mut self,
        engine: &mut formualizer_eval::engine::Engine<R>,
    ) -> Result<(), Self::Error> {
        use crate::error::IoError;
        use formualizer_eval::arrow_store::IngestBuilder;
        use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
        use formualizer_eval::reference::{CellRef, Coord};

        let names = self
            .sheet_names()
            .map_err(|e| IoError::from_backend("umya", e))?;
        for n in &names {
            engine.add_sheet(n).map_err(IoError::Engine)?;
        }

        let prev_index_mode = engine.config.sheet_index_mode;
        engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::Lazy);
        let prev_range_limit = engine.config.range_expansion_limit;
        engine.config.range_expansion_limit = 0;

        engine.set_first_load_assume_new(true);
        engine.reset_ensure_touched();

        let chunk_rows: usize = 32 * 1024;

        for n in &names {
            let sheet_data = self
                .read_sheet(n)
                .map_err(|e| IoError::from_backend("umya", e))?;
            let dims = sheet_data.dimensions.unwrap_or_else(|| {
                sheet_data
                    .cells
                    .keys()
                    .fold((0u32, 0u32), |mut acc, (r, c)| {
                        if *r > acc.0 {
                            acc.0 = *r;
                        }
                        if *c > acc.1 {
                            acc.1 = *c;
                        }
                        acc
                    })
            });
            let rows = dims.0 as usize;
            let cols = dims.1 as usize;

            let mut aib = IngestBuilder::new(n, cols, chunk_rows, engine.config.date_system);
            for r in 1..=rows {
                let mut row_vals = vec![LiteralValue::Empty; cols];
                for c in 1..=cols {
                    if let Some(cd) = sheet_data.cells.get(&(r as u32, c as u32))
                        && let Some(v) = &cd.value
                    {
                        row_vals[c - 1] = v.clone();
                    }
                }
                aib.append_row(&row_vals).map_err(IoError::Engine)?;
            }
            let asheet = aib.finish();
            let store = engine.sheet_store_mut();
            if let Some(pos) = store.sheets.iter().position(|s| s.name.as_ref() == n) {
                store.sheets[pos] = asheet;
            } else {
                store.sheets.push(asheet);
            }

            // Register native tables before formula ingest.
            if let Some(sheet_id) = engine.sheet_id(n) {
                for table in &sheet_data.tables {
                    let (sr, sc, er, ec) = table.range;
                    let sr0 = sr.saturating_sub(1);
                    let sc0 = sc.saturating_sub(1);
                    let er0 = er.saturating_sub(1);
                    let ec0 = ec.saturating_sub(1);
                    let start_ref = CellRef::new(sheet_id, Coord::new(sr0, sc0, true, true));
                    let end_ref = CellRef::new(sheet_id, Coord::new(er0, ec0, true, true));
                    let range_ref = formualizer_eval::reference::RangeRef::new(start_ref, end_ref);
                    engine.define_table(
                        &table.name,
                        range_ref,
                        table.headers.clone(),
                        table.totals_row,
                    )?;
                }
            }

            if engine.config.defer_graph_building {
                for ((row, col), cd) in &sheet_data.cells {
                    if let Some(f) = &cd.formula {
                        if f.is_empty() {
                            continue;
                        }
                        engine.stage_formula_text(n, *row, *col, f.clone());
                    }
                }
            } else {
                let mut builder = engine.begin_bulk_ingest();
                let sid = builder.add_sheet(n);
                for ((row, col), cd) in &sheet_data.cells {
                    if let Some(f) = &cd.formula {
                        if f.is_empty() {
                            continue;
                        }
                        let with_eq = if f.starts_with('=') {
                            f.clone()
                        } else {
                            format!("={f}")
                        };
                        let parsed = formualizer_parse::parser::parse(&with_eq)
                            .map_err(|e| IoError::from_backend("umya", e))?;
                        builder.add_formulas(sid, std::iter::once((*row, *col, parsed)));
                    }
                }
                let _ = builder.finish();
            }

            let Some(sheet_id) = engine.sheet_id(n) else {
                continue;
            };
            for named in &sheet_data.named_ranges {
                if named.address.sheet != *n {
                    continue;
                }
                let addr = &named.address;
                let sr0 = addr.start_row.saturating_sub(1);
                let sc0 = addr.start_col.saturating_sub(1);
                let er0 = addr.end_row.saturating_sub(1);
                let ec0 = addr.end_col.saturating_sub(1);

                let start_coord = Coord::new(sr0, sc0, true, true);
                let end_coord = Coord::new(er0, ec0, true, true);
                let start_ref = CellRef::new(sheet_id, start_coord);
                let end_ref = CellRef::new(sheet_id, end_coord);

                let definition = if sr0 == er0 && sc0 == ec0 {
                    NamedDefinition::Cell(start_ref)
                } else {
                    let range_ref = formualizer_eval::reference::RangeRef::new(start_ref, end_ref);
                    NamedDefinition::Range(range_ref)
                };

                let scope = match named.scope {
                    crate::traits::NamedRangeScope::Workbook => NameScope::Workbook,
                    crate::traits::NamedRangeScope::Sheet => NameScope::Sheet(sheet_id),
                };

                engine.define_name(&named.name, definition, scope)?;
            }
        }

        for n in &names {
            engine.finalize_sheet_index(n);
        }

        engine.set_first_load_assume_new(false);
        engine.reset_ensure_touched();
        engine.set_sheet_index_mode(prev_index_mode);
        engine.config.range_expansion_limit = prev_range_limit;
        Ok(())
    }
}
