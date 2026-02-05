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

        // TODO: Implement table support
        Err(ExcelError::new(ExcelErrorKind::NImpl).with_message("Tables not yet implemented"))
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
