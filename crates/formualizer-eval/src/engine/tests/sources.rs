use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
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
    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        crate::function_registry::get(ns, name)
    }
}

impl Resolver for SourceCtx {}
impl EvaluationContext for SourceCtx {}

#[test]
fn undeclared_source_is_name_error() {
    let ctx = SourceCtx::default();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    let ast = formualizer_parse::parser::parse("=Foo").unwrap();
    let err = engine
        .set_cell_formula("Sheet1", 1, 1, ast)
        .expect_err("undeclared source should be a hard error");
    assert_eq!(err.kind, ExcelErrorKind::Name);
}

#[test]
fn staged_build_undeclared_reference_evaluates_to_name_error() {
    let ctx = SourceCtx::default();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    engine.stage_formula_text("Sheet1", 1, 1, "Foo".to_string());
    engine.build_graph_all().unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Name),
        other => panic!("expected #NAME?, got {other:?}"),
    }
}

#[test]
fn staged_build_declared_unresolved_source_yields_ref() {
    let ctx = SourceCtx::default();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    engine.define_source_scalar("Foo", Some(1)).unwrap();

    engine.stage_formula_text("Sheet1", 1, 1, "Foo".to_string());
    engine.build_graph_all().unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF!, got {other:?}"),
    }
}

#[test]
fn declared_source_missing_is_ref_error() {
    let ctx = SourceCtx::default();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_scalar("Foo", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=Foo").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    match engine.evaluate_cell("Sheet1", 1, 1).unwrap() {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF!, got {other:?}"),
    }
}

#[test]
fn declared_table_source_missing_is_ref_error() {
    let ctx = SourceCtx::default();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=SUM(Sales[Amount])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    match engine.evaluate_cell("Sheet1", 1, 1).unwrap() {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF!, got {other:?}"),
    }
}

#[test]
fn scalar_source_invalidates_dependents() {
    let ctx = SourceCtx::default();
    ctx.set_scalar("Foo", LiteralValue::Int(1));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_scalar("Foo", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=Foo").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    let v1 = engine
        .evaluate_cell("Sheet1", 1, 1)
        .unwrap()
        .expect("computed value");
    assert_eq!(v1, LiteralValue::Number(1.0));

    ctx.set_scalar("Foo", LiteralValue::Int(2));
    engine.invalidate_source("Foo").unwrap();

    let v2 = engine
        .evaluate_cell("Sheet1", 1, 1)
        .unwrap()
        .expect("computed value");
    assert_eq!(v2, LiteralValue::Number(2.0));
}

#[test]
fn table_source_cached_within_evaluate_until() {
    let ctx = SourceCtx::default();
    let table = MemTable {
        headers: vec!["Region".into(), "Amount".into()],
        data: vec![
            vec![LiteralValue::Text("N".into()), LiteralValue::Number(10.0)],
            vec![LiteralValue::Text("S".into()), LiteralValue::Number(20.0)],
        ],
    };
    ctx.set_table("Sales", Arc::new(table));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=SUM(Sales[Amount]) + SUM(Sales[Amount])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    let v = engine
        .evaluate_cell("Sheet1", 1, 1)
        .unwrap()
        .expect("computed value");
    assert_eq!(v, LiteralValue::Number(60.0));
    assert_eq!(ctx.table_calls(), 1);
}

#[test]
fn workbook_name_beats_source_scalar() {
    let ctx = SourceCtx::default();
    ctx.set_scalar("Foo", LiteralValue::Number(999.0));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(111.0))
        .unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let target = CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true));
    engine
        .define_name("Foo", NamedDefinition::Cell(target), NameScope::Workbook)
        .unwrap();

    engine.define_source_scalar("Foo", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=Foo").unwrap();
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    let v = engine
        .evaluate_cell("Sheet1", 1, 2)
        .unwrap()
        .expect("computed value");
    assert_eq!(v, LiteralValue::Number(111.0));
    assert_eq!(ctx.scalar_calls(), 0);
}

#[test]
fn workbook_table_beats_source_table() {
    let ctx = SourceCtx::default();
    let source_table = MemTable {
        headers: vec!["Region".into(), "Amount".into()],
        data: vec![
            vec![LiteralValue::Text("X".into()), LiteralValue::Number(1000.0)],
            vec![LiteralValue::Text("Y".into()), LiteralValue::Number(2000.0)],
        ],
    };
    ctx.set_table("Sales", Arc::new(source_table));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 2, LiteralValue::Number(20.0))
        .unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let start = CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true));
    let end = CellRef::new(sheet_id, Coord::from_excel(3, 2, true, true));
    let range = RangeRef::new(start, end);

    engine
        .define_table(
            "Sales",
            range,
            vec!["Region".into(), "Amount".into()],
            false,
        )
        .unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=SUM(Sales[Amount])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 4, ast).unwrap();

    let v = engine
        .evaluate_cell("Sheet1", 1, 4)
        .unwrap()
        .expect("computed value");
    assert_eq!(v, LiteralValue::Number(30.0));
    assert_eq!(ctx.table_calls(), 0);
}

#[test]
fn volatile_source_recomputes_without_invalidate() {
    let ctx = SourceCtx::default();
    ctx.set_scalar("Foo", LiteralValue::Number(1.0));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_scalar("Foo", None).unwrap();

    let ast = formualizer_parse::parser::parse("=Foo").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(ctx.scalar_calls(), 1);

    ctx.set_scalar("Foo", LiteralValue::Number(2.0));
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(ctx.scalar_calls(), 2);
}

#[test]
fn nonvolatile_source_stays_stale_without_invalidate() {
    let ctx = SourceCtx::default();
    ctx.set_scalar("Foo", LiteralValue::Number(1.0));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_scalar("Foo", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=Foo").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(ctx.scalar_calls(), 1);

    ctx.set_scalar("Foo", LiteralValue::Number(2.0));
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(ctx.scalar_calls(), 1);

    engine.invalidate_source("Foo").unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(ctx.scalar_calls(), 2);
}

#[test]
fn cache_shared_across_vertices_in_evaluate_all() {
    let ctx = SourceCtx::default();
    let table = MemTable {
        headers: vec!["Amount".into()],
        data: vec![
            vec![LiteralValue::Number(10.0)],
            vec![LiteralValue::Number(20.0)],
        ],
    };
    ctx.set_table("Sales", Arc::new(table));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let a1 = formualizer_parse::parser::parse("=SUM(Sales[Amount])").unwrap();
    let a2 = formualizer_parse::parser::parse("=SUM(Sales[Amount])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, a1).unwrap();
    engine.set_cell_formula("Sheet1", 2, 1, a2).unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(30.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(30.0))
    );
    assert_eq!(ctx.table_calls(), 1);
}

#[test]
fn cache_clears_between_calls() {
    let ctx = SourceCtx::default();
    let table = MemTable {
        headers: vec!["Amount".into()],
        data: vec![
            vec![LiteralValue::Number(10.0)],
            vec![LiteralValue::Number(20.0)],
        ],
    };
    ctx.set_table("Sales", Arc::new(table));

    let mut engine: Engine<_> = Engine::new(ctx.clone(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=SUM(Sales[Amount])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    engine.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(ctx.table_calls(), 1);

    engine.invalidate_source("Sales").unwrap();
    engine.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(ctx.table_calls(), 2);
}

#[test]
fn column_range_reversed_order() {
    let ctx = SourceCtx::default();
    let table = MemTable {
        headers: vec!["A".into(), "B".into()],
        data: vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Number(10.0)],
            vec![LiteralValue::Number(2.0), LiteralValue::Number(20.0)],
        ],
    };
    ctx.set_table("Sales", Arc::new(table));

    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let forward = formualizer_parse::parser::parse("=SUM(Sales[A:B])").unwrap();
    let reverse = formualizer_parse::parser::parse("=SUM(Sales[B:A])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, forward).unwrap();
    engine.set_cell_formula("Sheet1", 2, 1, reverse).unwrap();

    engine.evaluate_all().unwrap();
    let a = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    let b = engine.get_cell_value("Sheet1", 2, 1).unwrap();
    assert_eq!(a, b);
    assert_eq!(a, LiteralValue::Number(33.0));
}

#[test]
fn totals_missing_is_empty() {
    let ctx = SourceCtx::default();
    let table = MemTable {
        headers: vec!["Amount".into()],
        data: vec![
            vec![LiteralValue::Number(10.0)],
            vec![LiteralValue::Number(20.0)],
        ],
    };
    ctx.set_table("Sales", Arc::new(table));

    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=SUM(Sales[#Totals])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(0.0))
    );
}

#[test]
fn unsupported_specifiers_error_kind() {
    let ctx = SourceCtx::default();
    let table = MemTable {
        headers: vec!["A".into()],
        data: vec![vec![LiteralValue::Number(1.0)]],
    };
    ctx.set_table("Sales", Arc::new(table));

    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();

    engine.define_source_table("Sales", Some(1)).unwrap();

    let ast = formualizer_parse::parser::parse("=SUM(Sales[@])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    match engine.evaluate_cell("Sheet1", 1, 1).unwrap() {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::NImpl),
        other => panic!("expected #N/IMPL!, got {other:?}"),
    }
}

#[test]
fn define_source_duplicate_name_rejected() {
    let ctx = SourceCtx::default();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    engine.define_source_scalar("Foo", Some(1)).unwrap();
    let err = engine
        .define_source_table("Foo", Some(1))
        .expect_err("cannot define scalar and table under same name");
    assert_eq!(err.kind, ExcelErrorKind::Name);
}

#[test]
fn invalidate_unknown_source_errors() {
    let ctx = SourceCtx::default();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    let err = engine
        .invalidate_source("Nope")
        .expect_err("unknown source should error");
    assert_eq!(err.kind, ExcelErrorKind::Name);
}
