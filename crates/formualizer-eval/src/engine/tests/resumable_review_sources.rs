use crate::engine::{Engine, EvalConfig};
use crate::traits::{
    EvaluationContext, FunctionProvider, NamedRangeResolver, Range, RangeResolver,
    ReferenceResolver, Resolver, SourceResolver, Table, TableResolver,
};
use formualizer_common::LiteralValue;
use formualizer_common::error::{ExcelError, ExcelErrorKind};
use formualizer_parse::parser::TableReference;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
struct MemTable {
    headers: Vec<String>,
    data: Vec<Vec<LiteralValue>>,
}

impl Table for MemTable {
    fn get_cell(&self, row: usize, column: &str) -> Result<LiteralValue, ExcelError> {
        let col_idx = self
            .headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(column))
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
        self.data
            .get(row)
            .and_then(|r| r.get(col_idx))
            .cloned()
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
    }

    fn get_column(&self, column: &str) -> Result<Box<dyn Range>, ExcelError> {
        let col_idx = self
            .headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(column))
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
        let mut col: Vec<Vec<LiteralValue>> = Vec::with_capacity(self.data.len());
        for r in &self.data {
            col.push(vec![r.get(col_idx).cloned().unwrap_or(LiteralValue::Empty)]);
        }
        Ok(Box::new(crate::traits::InMemoryRange::new(col)))
    }

    fn columns(&self) -> Vec<String> {
        self.headers.clone()
    }

    fn data_height(&self) -> usize {
        self.data.len()
    }

    fn has_headers(&self) -> bool {
        true
    }

    fn has_totals(&self) -> bool {
        false
    }

    fn headers_row(&self) -> Option<Box<dyn Range>> {
        Some(Box::new(crate::traits::InMemoryRange::new(vec![
            self.headers
                .iter()
                .cloned()
                .map(LiteralValue::Text)
                .collect(),
        ])))
    }

    fn data_body(&self) -> Option<Box<dyn Range>> {
        Some(Box::new(crate::traits::InMemoryRange::new(
            self.data.clone(),
        )))
    }

    fn clone_box(&self) -> Box<dyn Table> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default)]
struct SourceCtx {
    scalars: Arc<RwLock<HashMap<String, LiteralValue>>>,
    tables: Arc<RwLock<HashMap<String, Arc<dyn Table>>>>,
    scalar_calls: Arc<AtomicUsize>,
    table_calls: Arc<AtomicUsize>,
}

impl SourceCtx {
    fn set_scalar(&self, name: &str, v: LiteralValue) {
        self.scalars.write().unwrap().insert(name.to_string(), v);
    }

    fn set_table(&self, name: &str, t: Arc<dyn Table>) {
        self.tables.write().unwrap().insert(name.to_string(), t);
    }

    fn scalar_calls(&self) -> usize {
        self.scalar_calls.load(Ordering::Relaxed)
    }

    fn table_calls(&self) -> usize {
        self.table_calls.load(Ordering::Relaxed)
    }
}

impl ReferenceResolver for SourceCtx {
    fn resolve_cell_reference(
        &self,
        _sheet: Option<&str>,
        _row: u32,
        _col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::NImpl))
    }
}

impl RangeResolver for SourceCtx {
    fn resolve_range_reference(
        &self,
        _sheet: Option<&str>,
        _sr: Option<u32>,
        _sc: Option<u32>,
        _er: Option<u32>,
        _ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::NImpl))
    }
}

impl NamedRangeResolver for SourceCtx {
    fn resolve_named_range_reference(
        &self,
        _name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::Name))
    }
}

impl TableResolver for SourceCtx {
    fn resolve_table_reference(
        &self,
        _tref: &TableReference,
    ) -> Result<Box<dyn Table>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::NImpl))
    }
}

impl SourceResolver for SourceCtx {
    fn resolve_source_scalar(&self, name: &str) -> Result<LiteralValue, ExcelError> {
        self.scalar_calls.fetch_add(1, Ordering::Relaxed);
        self.scalars
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Name))
    }

    fn resolve_source_table(&self, name: &str) -> Result<Box<dyn Table>, ExcelError> {
        self.table_calls.fetch_add(1, Ordering::Relaxed);
        let t = self
            .tables
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Name))?;
        Ok(t.clone_box())
    }
}

impl FunctionProvider for SourceCtx {
    fn planning_semantic_revision(&self) -> Option<u64> {
        Some(0)
    }

    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        crate::function_registry::get(ns, name)
    }

    fn get_function_for_planning(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        crate::function_registry::get_for_planning(ns, name)
    }
}

impl Resolver for SourceCtx {}
impl EvaluationContext for SourceCtx {}



use crate::engine::{EvaluationStop, EvaluationProgress};
use formualizer_parse::parser::parse;

#[test]
fn source_change_after_installed_dynamic_reader() {
    source_change_matrix(false);
}
#[test]
fn table_source_change_after_installed_dynamic_reader() {
    source_change_matrix(true);
}
fn source_change_matrix(table: bool) {
    crate::builtins::load_builtins();
    let mut failures=Vec::new();
    let mut exercised=0;
    for family in [false,true] { for mode in 0..2 {
        let mut route_cases=0;
        for cut in 1..=8 {
            let ctx=SourceCtx::default();
            let set_source=|value| {
                if table {ctx.set_table("Foo",Arc::new(MemTable{
                    headers:vec!["Amount".into()],
                    data:vec![vec![LiteralValue::Number(value)]],
                }));} else {ctx.set_scalar("Foo",LiteralValue::Number(value));}
            };
            set_source(1.);
            let make=|| {
                let mut e=Engine::new(ctx.clone(),EvalConfig{family_execution:family,enable_parallel:false,..EvalConfig::default()});
                if table {e.define_source_table("Foo",Some(1)).unwrap();}
                else {e.define_source_scalar("Foo",Some(1)).unwrap();}
                e.set_cell_formula("Sheet1",1,1,parse(if table {"=SUM(Foo[Amount])"}else{"=Foo"}).unwrap()).unwrap();
                for r in 2..=20 {e.set_cell_formula("Sheet1",r,1,parse(format!("=A{}+1",r-1)).unwrap()).unwrap();}
                e.set_cell_formula("Sheet1",1,2,parse("=INDIRECT(\"A3\")").unwrap()).unwrap();
                e
            };
            let mut e=make();let mut ordinary=make();
            e.resumable_stop_after_units_for_test=Some(cut);
            let progress=e.evaluate_all_until(EvaluationStop::none()).unwrap();
            if !matches!(progress,EvaluationProgress::Incomplete(_)) || e.get_cell_value("Sheet1",1,2)!=Some(LiteralValue::Number(3.)){continue;}
            exercised+=1;route_cases+=1;
            let revision=e.inspection_mutation_revision();
            set_source(77.);
            for x in [&mut e,&mut ordinary] {
                if mode==1 {x.invalidate_source("Foo").unwrap();}
                else if table {x.set_source_table_version("Foo",Some(2)).unwrap();}
                else {x.set_source_scalar_version("Foo",Some(2)).unwrap();}
            }
            e.resumable_stop_after_units_for_test=None;
            for call in 0..100 {
                if matches!(e.evaluate_all_until(EvaluationStop::none()).unwrap(),EvaluationProgress::Complete(_)){break;}
                assert!(call<99);
            }
            ordinary.evaluate_all().unwrap();
            let actual=e.get_cell_value("Sheet1",1,2);
            let expected=ordinary.get_cell_value("Sheet1",1,2);
            let published=e.inspection_mutation_revision()!=revision;
            println!("SOURCE table={table} family={family} mode={mode} cut={cut} actual={actual:?} expected={expected:?} revision_published={published}");
            if actual!=expected || !published {failures.push((family,mode,cut));}
            if table {assert!(ctx.table_calls()>0);}else{assert!(ctx.scalar_calls()>0);}
        }
        assert!(route_cases>0,"no installed-reader cases: table={table} family={family} mode={mode}");
    }}
    println!("SOURCE table={table} exercised={exercised} failures={failures:?}");
    assert!(failures.is_empty(),"source mutation failures {failures:?}");
}
