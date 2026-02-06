use crate::traits::SpreadsheetReader;
use crate::traits::{DefinedNameDefinition, DefinedNameScope};
use formualizer_common::{
    error::{ExcelError, ExcelErrorKind},
    LiteralValue,
};
use formualizer_eval::function::Function;
use formualizer_eval::traits::{
    FunctionProvider, InMemoryRange, NamedRangeResolver, Range, RangeResolver, ReferenceResolver,
    Resolver, Table, TableResolver,
};
use formualizer_parse::parser::TableReference;
use parking_lot::RwLock;
use std::sync::Arc;

/// Minimal resolver for ranges/tables (NOT cells - Engine handles cells from graph)
pub struct IoResolver<B: SpreadsheetReader> {
    backend: RwLock<B>,
}

impl<B: SpreadsheetReader> IoResolver<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend: RwLock::new(backend),
        }
    }
}

// IoResolver does NOT implement ReferenceResolver (Engine handles cells from graph)

impl<B: SpreadsheetReader> RangeResolver for IoResolver<B> {
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        let sheet_name = sheet.ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Missing sheet name")
        })?;
        let (sr, sc, er, ec) = normalize_range(sr, sc, er, ec)?;

        // Read from backend with interior mutability
        let mut guard = self.backend.write();
        let map = guard
            .read_range(sheet_name, (sr, sc), (er, ec))
            .map_err(|e| ExcelError::new(ExcelErrorKind::NImpl).with_message(e.to_string()))?;

        let height = (er - sr + 1) as usize;
        let width = (ec - sc + 1) as usize;
        let mut rows = vec![vec![LiteralValue::Empty; width]; height];
        for ((r, c), cell) in map.into_iter() {
            let rr = (r - sr) as usize;
            let cc = (c - sc) as usize;
            if let Some(v) = cell.value {
                rows[rr][cc] = v;
            } else {
                rows[rr][cc] = LiteralValue::Empty;
            }
        }
        Ok(Box::new(InMemoryRange::new(rows)))
    }
}

impl<B: SpreadsheetReader> NamedRangeResolver for IoResolver<B> {
    fn resolve_named_range_reference(
        &self,
        _name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        // Check if backend supports named ranges
        if !self.backend.read().capabilities().named_ranges {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("Backend doesn't support named ranges"));
        }

        let name = _name;

        let mut guard = self.backend.write();
        let defined = guard
            .defined_names()
            .map_err(|e| ExcelError::new(ExcelErrorKind::NImpl).with_message(e.to_string()))?;

        // Collect candidates (workbook + any sheet scopes). Since this resolver doesn't have
        // a current-sheet context, sheet-scoped names are only resolvable when unique.
        let mut matches = defined
            .into_iter()
            .filter(|dn| dn.name == name)
            .collect::<Vec<_>>();

        if matches.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Undefined name: {name}")));
        }

        // Prefer workbook-scoped if present and unique; otherwise require exactly one match.
        let chosen = if let Some(wb) = matches
            .iter()
            .position(|dn| matches!(dn.scope, DefinedNameScope::Workbook))
        {
            // If both workbook + sheet scoped exist, this is ambiguous without a sheet context.
            if matches.len() > 1 {
                return Err(ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Ambiguous name without sheet context: {name}")));
            }
            matches.swap_remove(wb)
        } else {
            if matches.len() != 1 {
                return Err(ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Ambiguous name without sheet context: {name}")));
            }
            matches.pop().unwrap()
        };

        match chosen.definition {
            DefinedNameDefinition::Range { address } => {
                // Delegate to RangeResolver reading from backend.
                let range = guard
                    .read_range(
                        &address.sheet,
                        (address.start_row, address.start_col),
                        (address.end_row, address.end_col),
                    )
                    .map_err(|e| {
                        ExcelError::new(ExcelErrorKind::NImpl).with_message(e.to_string())
                    })?;

                let h = (address.end_row - address.start_row + 1) as usize;
                let w = (address.end_col - address.start_col + 1) as usize;
                let mut rows = vec![vec![LiteralValue::Empty; w]; h];
                for ((r, c), cell) in range.into_iter() {
                    let rr = (r - address.start_row) as usize;
                    let cc = (c - address.start_col) as usize;
                    rows[rr][cc] = cell.value.unwrap_or(LiteralValue::Empty);
                }
                Ok(rows)
            }
            DefinedNameDefinition::Literal { value } => Ok(vec![vec![value]]),
        }
    }
}

impl<B: SpreadsheetReader> TableResolver for IoResolver<B> {
    fn resolve_table_reference(
        &self,
        _tref: &TableReference,
    ) -> Result<Box<dyn Table>, ExcelError> {
        // Check if backend supports tables
        if !self.backend.read().capabilities().tables {
            return Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("Backend doesn't support tables"));
        }

        let tref = _tref;

        // Locate the table definition (tables are workbook-global; scan sheets).
        let mut guard = self.backend.write();
        let sheets = guard
            .sheet_names()
            .map_err(|e| ExcelError::new(ExcelErrorKind::NImpl).with_message(e.to_string()))?;

        let mut found: Option<(String, crate::traits::TableDefinition)> = None;
        for s in sheets {
            let sd = guard
                .read_sheet(&s)
                .map_err(|e| ExcelError::new(ExcelErrorKind::NImpl).with_message(e.to_string()))?;
            if let Some(td) = sd.tables.into_iter().find(|t| t.name == tref.name) {
                found = Some((s, td));
                break;
            }
        }

        let (sheet, table) = found.ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Undefined table: {}", tref.name))
        })?;

        let (sr, sc, er, ec) = table.range;
        let map = guard
            .read_range(&sheet, (sr, sc), (er, ec))
            .map_err(|e| ExcelError::new(ExcelErrorKind::NImpl).with_message(e.to_string()))?;
        let height = (er - sr + 1) as usize;
        let width = (ec - sc + 1) as usize;
        let mut rows = vec![vec![LiteralValue::Empty; width]; height];
        for ((r, c), cell) in map.into_iter() {
            let rr = (r - sr) as usize;
            let cc = (c - sc) as usize;
            rows[rr][cc] = cell.value.unwrap_or(LiteralValue::Empty);
        }

        Ok(Box::new(BackendTable {
            headers: table.headers,
            header_row: table.header_row,
            totals_row: table.totals_row,
            full: rows,
        }))
    }
}

#[derive(Clone, Debug)]
struct BackendTable {
    headers: Vec<String>,
    header_row: bool,
    totals_row: bool,
    full: Vec<Vec<LiteralValue>>, // includes header/totals rows as present
}

impl BackendTable {
    fn col_index(&self, header: &str) -> Option<usize> {
        self.headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(header))
    }

    fn body_bounds(&self) -> (usize, usize) {
        let h = self.full.len();
        let start = if self.header_row { 1 } else { 0 };
        let end_exclusive = if self.totals_row && h > 0 {
            h.saturating_sub(1)
        } else {
            h
        };
        (start.min(h), end_exclusive.min(h))
    }
}

impl Table for BackendTable {
    fn get_cell(&self, r: usize, c: &str) -> Result<LiteralValue, ExcelError> {
        let idx = self.col_index(c).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Column refers to unknown table column".to_string())
        })?;
        let (start, end_excl) = self.body_bounds();
        let body_h = end_excl.saturating_sub(start);
        if r >= body_h {
            return Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Row out of range for table data".to_string()));
        }
        Ok(self.full[start + r]
            .get(idx)
            .cloned()
            .unwrap_or(LiteralValue::Empty))
    }

    fn get_column(&self, c: &str) -> Result<Box<dyn Range>, ExcelError> {
        let idx = self.col_index(c).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Column refers to unknown table column".to_string())
        })?;
        let (start, end_excl) = self.body_bounds();
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(end_excl.saturating_sub(start));
        for r in start..end_excl {
            let v = self.full[r]
                .get(idx)
                .cloned()
                .unwrap_or(LiteralValue::Empty);
            out.push(vec![v]);
        }
        Ok(Box::new(InMemoryRange::new(out)))
    }

    fn columns(&self) -> Vec<String> {
        self.headers.clone()
    }

    fn data_height(&self) -> usize {
        let (start, end_excl) = self.body_bounds();
        end_excl.saturating_sub(start)
    }

    fn has_headers(&self) -> bool {
        self.header_row
    }

    fn has_totals(&self) -> bool {
        self.totals_row
    }

    fn headers_row(&self) -> Option<Box<dyn Range>> {
        if !self.header_row || self.full.is_empty() {
            return None;
        }
        Some(Box::new(InMemoryRange::new(vec![self.full[0].clone()])))
    }

    fn totals_row(&self) -> Option<Box<dyn Range>> {
        if !self.totals_row || self.full.is_empty() {
            return None;
        }
        Some(Box::new(InMemoryRange::new(vec![self.full
            [self.full.len() - 1]
            .clone()])))
    }

    fn data_body(&self) -> Option<Box<dyn Range>> {
        let (start, end_excl) = self.body_bounds();
        if start >= end_excl {
            return Some(Box::new(InMemoryRange::new(vec![])));
        }
        Some(Box::new(InMemoryRange::new(
            self.full[start..end_excl].to_vec(),
        )))
    }

    fn clone_box(&self) -> Box<dyn Table> {
        Box::new(self.clone())
    }
}

impl<B: SpreadsheetReader> FunctionProvider for IoResolver<B> {
    fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn Function>> {
        // Delegate to global registry
        formualizer_eval::function_registry::get(ns, name)
    }
}

// IoResolver needs to implement ReferenceResolver for cells
// Even though Engine handles cells from graph, trait requires it
impl<B: SpreadsheetReader> ReferenceResolver for IoResolver<B> {
    fn resolve_cell_reference(
        &self,
        _sheet: Option<&str>,
        _row: u32,
        _col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        // IoResolver doesn't handle cells - Engine reads from graph
        // This is just to satisfy the trait requirement
        Err(ExcelError::new(ExcelErrorKind::Ref)
            .with_message("IoResolver doesn't handle cell references"))
    }
}

impl<B: SpreadsheetReader> Resolver for IoResolver<B> {}

fn normalize_range(
    sr: Option<u32>,
    sc: Option<u32>,
    er: Option<u32>,
    ec: Option<u32>,
) -> Result<(u32, u32, u32, u32), ExcelError> {
    // Default to single cell if not specified
    let sr = sr.unwrap_or(1);
    let sc = sc.unwrap_or(1);
    let er = er.unwrap_or(sr);
    let ec = ec.unwrap_or(sc);

    // Validate range
    if sr > er || sc > ec {
        return Err(ExcelError::new(ExcelErrorKind::Ref).with_message("Invalid range: start > end"));
    }

    Ok((sr, sc, er, ec))
}
