use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
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

use crate::engine::{EvaluationProgress, EvaluationStop};
use formualizer_parse::parser::parse;

#[test]
fn source_change_after_installed_dynamic_reader() {
    source_change_matrix(false, false, false);
}
#[test]
fn table_source_change_after_installed_dynamic_reader() {
    source_change_matrix(true, false, false);
}
#[test]
fn named_source_change_after_installed_dynamic_reader() {
    source_change_matrix(false, true, false);
}
#[test]
fn named_table_source_change_after_installed_dynamic_reader() {
    source_change_matrix(true, true, false);
}
#[test]
fn job_source_change_matrix() {
    for table in [false, true] {
        for named in [false, true] {
            source_change_matrix(table, named, true);
        }
    }
}
fn source_change_matrix(table: bool, named: bool, job: bool) {
    crate::builtins::load_builtins();
    let mut failures = Vec::new();
    let mut exercised = 0;
    for family in [false, true] {
        for mode in 0..2 {
            let mut route_cases = 0;
            for cut in 1..=8 {
                let ctx = SourceCtx::default();
                let set_source = |value| {
                    if table {
                        ctx.set_table(
                            "Foo",
                            Arc::new(MemTable {
                                headers: vec!["Amount".into()],
                                data: vec![vec![LiteralValue::Number(value)]],
                            }),
                        );
                    } else {
                        ctx.set_scalar("Foo", LiteralValue::Number(value));
                    }
                };
                set_source(1.);
                let make = || {
                    let mut e = Engine::new(
                        ctx.clone(),
                        EvalConfig {
                            family_execution: family,
                            enable_parallel: false,
                            ..EvalConfig::default()
                        },
                    );
                    if table {
                        e.define_source_table("Foo", Some(1)).unwrap();
                    } else {
                        e.define_source_scalar("Foo", Some(1)).unwrap();
                    }
                    e.set_cell_formula(
                        "Sheet1",
                        1,
                        1,
                        parse(if table { "=SUM(Foo[Amount])" } else { "=Foo" }).unwrap(),
                    )
                    .unwrap();
                    for r in 2..=20 {
                        e.set_cell_formula(
                            "Sheet1",
                            r,
                            1,
                            parse(format!("=A{}+1", r - 1)).unwrap(),
                        )
                        .unwrap();
                    }
                    if named {
                        e.define_name(
                            "SourceAlias",
                            NamedDefinition::Cell(CellRef::new(
                                e.sheet_id("Sheet1").unwrap(),
                                Coord::from_excel(3, 1, true, true),
                            )),
                            NameScope::Workbook,
                        )
                        .unwrap();
                    }
                    e.set_cell_formula(
                        "Sheet1",
                        1,
                        2,
                        parse(if named {
                            "=INDIRECT(\"SourceAlias\")"
                        } else {
                            "=INDIRECT(\"A3\")"
                        })
                        .unwrap(),
                    )
                    .unwrap();
                    e
                };
                let mut e = make();
                let mut ordinary = make();
                e.resumable_stop_after_units_for_test = Some(cut);
                let progress =
                    super::test_recalc_slice(&mut e, EvaluationStop::none(), job).unwrap();
                if !matches!(progress, EvaluationProgress::Incomplete(_))
                    || e.get_cell_value("Sheet1", 1, 2) != Some(LiteralValue::Number(3.))
                {
                    continue;
                }
                exercised += 1;
                route_cases += 1;
                let revision = e.inspection_mutation_revision();
                set_source(77.);
                for x in [&mut e, &mut ordinary] {
                    if mode == 1 {
                        x.invalidate_source("Foo").unwrap();
                    } else if table {
                        x.set_source_table_version("Foo", Some(2)).unwrap();
                    } else {
                        x.set_source_scalar_version("Foo", Some(2)).unwrap();
                    }
                }
                e.resumable_stop_after_units_for_test = None;
                for call in 0..100 {
                    if matches!(
                        super::test_recalc_slice(&mut e, EvaluationStop::none(), job).unwrap(),
                        EvaluationProgress::Complete(_)
                    ) {
                        break;
                    }
                    assert!(call < 99);
                }
                ordinary.evaluate_all().unwrap();
                let actual = e.get_cell_value("Sheet1", 1, 2);
                let expected = ordinary.get_cell_value("Sheet1", 1, 2);
                let ordinary_revision_preserved = e.inspection_mutation_revision() == revision;
                println!(
                    "SOURCE table={table} named={named} family={family} mode={mode} cut={cut} actual={actual:?} expected={expected:?} ordinary_revision_preserved={ordinary_revision_preserved}"
                );
                if actual != expected || !ordinary_revision_preserved {
                    failures.push((family, mode, cut));
                }
                if table {
                    assert!(ctx.table_calls() > 0);
                } else {
                    assert!(ctx.scalar_calls() > 0);
                }
            }
            assert!(
                route_cases > 0,
                "no installed-reader cases: table={table} family={family} mode={mode}"
            );
        }
    }
    println!("SOURCE table={table} named={named} exercised={exercised} failures={failures:?}");
    assert!(failures.is_empty(), "source mutation failures {failures:?}");
}

fn lookup_source_engine(formulas: u32, rows: u32) -> Engine<SourceCtx> {
    let ctx = SourceCtx::default();
    ctx.set_scalar("Foo", LiteralValue::Number(1.));
    let mut e = Engine::new(
        ctx,
        EvalConfig {
            family_execution: true,
            enable_parallel: false,
            ..EvalConfig::default()
        },
    );
    e.define_source_scalar("Foo", Some(1)).unwrap();
    e.define_source_table("Unused", Some(1)).unwrap();
    let mut ingest = e.begin_bulk_ingest_arrow();
    ingest.add_sheet("Sheet1", 4, 32768);
    for r in 1..=rows {
        ingest
            .append_row(
                "Sheet1",
                &[
                    LiteralValue::Empty,
                    LiteralValue::Empty,
                    LiteralValue::Empty,
                    LiteralValue::Number(r as f64),
                ],
            )
            .unwrap();
    }
    ingest.finish().unwrap();
    let text = format!("=MATCH(Foo,D$1:D${rows},0)");
    for r in 1..=formulas {
        e.set_cell_formula("Sheet1", r, 1, parse(&text).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    assert_eq!(e.lookup_index_flights_built_for_test(), 1);
    e.reset_recalc_reuse_probe();
    e
}

#[test]
fn source_setters_preserve_ordinary_snapshot_and_lookup_reuse() {
    let mut e = lookup_source_engine(64, 4096);
    let revision = e.inspection_mutation_revision();
    let builds = e.lookup_index_flights_built_for_test();
    for mode in 0..3 {
        match mode {
            0 => e.set_source_scalar_version("Foo", Some(2)).unwrap(),
            1 => e.set_source_table_version("Unused", Some(2)).unwrap(),
            _ => e.invalidate_source("Foo").unwrap(),
        }
        assert_eq!(
            e.inspection_mutation_revision(),
            revision,
            "source setter {mode} changed ordinary snapshot"
        );
        e.evaluate_all().unwrap();
        assert_eq!(
            e.lookup_index_flights_built_for_test(),
            builds,
            "source setter {mode} discarded reusable lookup index"
        );
        assert_eq!(
            e.get_cell_value("Sheet1", 64, 1),
            Some(LiteralValue::Number(1.))
        );
    }
}

#[test]
#[ignore = "release/LTO paired ordinary source-version lookup benchmark"]
fn paired_source_bump_large_lookup_cost() {
    let mut old = lookup_source_engine(10000, 100000);
    let mut new = lookup_source_engine(10000, 100000);
    for pair in 0..5 {
        for public in if pair % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let e = if public { &mut new } else { &mut old };
            let before = e.lookup_index_flights_built_for_test();
            let revision = e.inspection_mutation_revision();
            let start = std::time::Instant::now();
            for i in 0..3 {
                let version = Some((pair * 3 + i + 2) as u64);
                if public {
                    e.set_source_scalar_version("Foo", version).unwrap();
                } else {
                    e.graph.set_source_scalar_version("Foo", version).unwrap();
                }
                e.evaluate_all().unwrap();
            }
            let elapsed_us = start.elapsed().as_micros();
            let after = e.lookup_index_flights_built_for_test();
            println!(
                "SOURCE_COST pair={pair} public={public} elapsed_us={elapsed_us} reuse={:?} lookup_before={before} lookup_after={after}",
                e.recalc_reuse_probe()
            );
            assert_eq!(after, before, "public={public}: lookup axis was unchanged");
            assert_eq!(e.inspection_mutation_revision(), revision);
            assert_eq!(
                e.get_cell_value("Sheet1", 10000, 1),
                Some(LiteralValue::Number(1.))
            );
        }
    }
}
