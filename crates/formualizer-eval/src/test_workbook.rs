//! crates/formualizer-eval/src/test_workbook.rs
//! --------------------------------------------
//! Lightweight in-memory workbook for unit/prop tests.
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::range_stream::RangeStorage;
use crate::function::Function;
use crate::traits::{
    EvaluationContext, FunctionProvider, NamedRangeResolver, Range, RangeResolver,
    ReferenceResolver, Resolver, Table, TableResolver,
};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_core::{
    ExcelErrorKind,
    parser::{ReferenceType, TableReference},
};

type V = LiteralValue;
type CellKey = (u32, u32); // 1-based (row, col)

#[derive(Default, Clone)]
struct Sheet {
    cells: HashMap<CellKey, V>,
}

#[derive(Default)]
pub struct TestWorkbook {
    sheets: HashMap<String, Sheet>,
    named: HashMap<String, Vec<Vec<V>>>,
    tables: HashMap<String, Box<dyn Table>>,
    fns: HashMap<(&'static str, &'static str), Arc<dyn Function>>,
}

impl TestWorkbook {
    /* ─────────────── constructors ─────────────── */
    pub fn new() -> Self {
        Self::default()
    }

    /* ─────────────── cell helpers ─────────────── */
    pub fn with_cell<S: Into<String>>(mut self, sheet: S, row: u32, col: u32, v: V) -> Self {
        let sh = self.sheets.entry(sheet.into()).or_default();
        sh.cells.insert((row, col), v);
        self
    }

    pub fn with_cell_a1<S: Into<String>, A: AsRef<str>>(self, sheet: S, a1: A, v: V) -> Self {
        let (col, row) = parse_a1(a1.as_ref()).expect("bad A1 ref in with_cell_a1");
        self.with_cell(sheet, row, col, v)
    }

    pub fn with_range<S: Into<String>>(
        mut self,
        sheet: S,
        row: u32,
        col: u32,
        data: Vec<Vec<V>>,
    ) -> Self {
        let sh = self.sheets.entry(sheet.into()).or_default();
        for (r_off, r) in data.into_iter().enumerate() {
            for (c_off, v) in r.into_iter().enumerate() {
                sh.cells.insert((row + r_off as u32, col + c_off as u32), v);
            }
        }
        self
    }

    /* ─────────────── named ranges ─────────────── */
    pub fn with_named_range<S: Into<String>>(mut self, name: S, data: Vec<Vec<V>>) -> Self {
        self.named.insert(name.into(), data);
        self
    }

    /* ─────────────── tables (placeholder) ─────── */
    pub fn with_table<T: Table + 'static, S: Into<String>>(mut self, name: S, table: T) -> Self {
        self.tables.insert(name.into(), Box::new(table));
        self
    }

    /* ─────────────── function helpers ─────────── */
    pub fn with_function(mut self, func: Arc<dyn Function>) -> Self {
        let ns = func.namespace();
        let name = func.name();
        self.fns.insert((ns, name), func);
        self
    }

    /* ─────────────── interpreter shortcut ─────── */
    pub fn interpreter(&self) -> crate::interpreter::Interpreter<'_> {
        crate::interpreter::Interpreter::new(self, "Sheet1")
    }
}

/* ─────────────────────── trait impls ─────────────────────── */
impl EvaluationContext for TestWorkbook {
    fn resolve_range_storage<'c>(
        &'c self,
        reference: &ReferenceType,
        _current_sheet: &str,
    ) -> Result<RangeStorage<'c>, ExcelError> {
        let range_box = self.resolve_range_like(reference)?;
        let data = range_box.materialise().into_owned();
        Ok(RangeStorage::Owned(Cow::Owned(data)))
    }
}
impl ReferenceResolver for TestWorkbook {
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<V, ExcelError> {
        let sheet_name = sheet.unwrap_or("Sheet1");
        self.sheets
            .get(sheet_name)
            .and_then(|sh| sh.cells.get(&(row, col)).cloned())
            .ok_or_else(|| ExcelError::from(ExcelErrorKind::Ref))
    }
}

impl RangeResolver for TestWorkbook {
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        let (sr, sc, er, ec) = match (sr, sc, er, ec) {
            (Some(sr), Some(sc), Some(er), Some(ec)) => (sr, sc, er, ec),
            _ => return Err(ExcelError::from(ExcelErrorKind::NImpl)),
        };
        let sheet_name = sheet.unwrap_or("Sheet1");
        let sh = self
            .sheets
            .get(sheet_name)
            .ok_or_else(|| ExcelError::from(ExcelErrorKind::Ref))?;
        let mut data = Vec::with_capacity((er - sr + 1) as usize);
        for r in sr..=er {
            let mut row_vec = Vec::with_capacity((ec - sc + 1) as usize);
            for c in sc..=ec {
                row_vec.push(sh.cells.get(&(r, c)).cloned().unwrap_or(V::Empty));
            }
            data.push(row_vec);
        }
        Ok(Box::new(crate::traits::InMemoryRange::new(data)))
    }
}

impl NamedRangeResolver for TestWorkbook {
    fn resolve_named_range_reference(&self, name: &str) -> Result<Vec<Vec<V>>, ExcelError> {
        self.named
            .get(name)
            .cloned()
            .ok_or_else(|| ExcelError::from(ExcelErrorKind::Name))
    }
}

impl TableResolver for TestWorkbook {
    fn resolve_table_reference(&self, tref: &TableReference) -> Result<Box<dyn Table>, ExcelError> {
        self.tables
            .get(&tref.name)
            .map(|table_box| table_box.as_ref().clone_box())
            .ok_or_else(|| ExcelError::from(ExcelErrorKind::NImpl))
    }
}

impl FunctionProvider for TestWorkbook {
    fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn Function>> {
        self.fns.get(&(ns, name)).cloned()
    }
}

/* blanket */
impl Resolver for TestWorkbook {}

/* ─────────────────────── A1 parser ───────────────────────── */
fn parse_a1(a1: &str) -> Option<(u32, u32)> {
    let s = a1.replace('$', "").to_uppercase();
    let mut col = 0u32;
    let mut row_str = String::new();
    for ch in s.chars() {
        if ch.is_ascii_alphabetic() {
            col = col * 26 + (ch as u32 - 'A' as u32 + 1);
        } else if ch.is_ascii_digit() {
            row_str.push(ch);
        } else {
            return None;
        }
    }
    if col == 0 || row_str.is_empty() {
        return None;
    }
    let row = row_str.parse::<u32>().ok()?;
    Some((col, row))
}
