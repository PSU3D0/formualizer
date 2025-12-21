use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime};
use formualizer_common::{
    RangeAddress,
    error::{ExcelError, ExcelErrorKind},
};
#[cfg(feature = "json")]
use formualizer_workbook::JsonAdapter;
use formualizer_workbook::{CellData, LiteralValue, SpreadsheetReader, SpreadsheetWriter};

#[cfg(feature = "json")]
#[test]
fn json_loader_declares_sources_before_formulas() {
    use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};

    let bytes = br#"{
        "version": 1,
        "sources": [
            { "type": "scalar", "name": "Foo", "version": 1 }
        ],
        "sheets": {
            "S": {
                "cells": [
                    { "row": 1, "col": 1, "formula": "Foo" }
                ]
            }
        }
    }"#
    .to_vec();

    let adapter = JsonAdapter::open_bytes(bytes).unwrap();
    let cfg = WorkbookConfig::interactive();
    let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg).unwrap();
    let v = wb.evaluate_cell("S", 1, 1).unwrap();

    match v {
        LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF!, got {other:?}"),
    }
}

#[cfg(feature = "json")]
#[test]
fn json_loader_declares_sources_before_formula_ingest_eager_mode() {
    use formualizer_eval::engine::ingest::EngineLoadStream;
    use formualizer_eval::engine::{Engine, EvalConfig};
    use formualizer_eval::traits::{
        EvaluationContext, FunctionProvider, NamedRangeResolver, Range, RangeResolver,
        ReferenceResolver, Resolver, SourceResolver, Table, TableResolver,
    };
    use formualizer_parse::parser::TableReference;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    #[derive(Clone)]
    struct Ctx {
        v: Arc<RwLock<LiteralValue>>,
        calls: Arc<AtomicUsize>,
    }

    impl Ctx {
        fn new(v: LiteralValue) -> Self {
            Self {
                v: Arc::new(RwLock::new(v)),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn set(&self, v: LiteralValue) {
            *self.v.write().unwrap() = v;
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl ReferenceResolver for Ctx {
        fn resolve_cell_reference(
            &self,
            _sheet: Option<&str>,
            _row: u32,
            _col: u32,
        ) -> Result<LiteralValue, formualizer_common::error::ExcelError> {
            Err(formualizer_common::error::ExcelError::new(
                ExcelErrorKind::NImpl,
            ))
        }
    }

    impl RangeResolver for Ctx {
        fn resolve_range_reference(
            &self,
            _sheet: Option<&str>,
            _sr: Option<u32>,
            _sc: Option<u32>,
            _er: Option<u32>,
            _ec: Option<u32>,
        ) -> Result<Box<dyn Range>, formualizer_common::error::ExcelError> {
            Err(formualizer_common::error::ExcelError::new(
                ExcelErrorKind::NImpl,
            ))
        }
    }

    impl NamedRangeResolver for Ctx {
        fn resolve_named_range_reference(
            &self,
            _name: &str,
        ) -> Result<Vec<Vec<LiteralValue>>, formualizer_common::error::ExcelError> {
            Err(formualizer_common::error::ExcelError::new(
                ExcelErrorKind::Name,
            ))
        }
    }

    impl TableResolver for Ctx {
        fn resolve_table_reference(
            &self,
            _tref: &TableReference,
        ) -> Result<Box<dyn Table>, formualizer_common::error::ExcelError> {
            Err(formualizer_common::error::ExcelError::new(
                ExcelErrorKind::NImpl,
            ))
        }
    }

    impl SourceResolver for Ctx {
        fn resolve_source_scalar(
            &self,
            _name: &str,
        ) -> Result<LiteralValue, formualizer_common::error::ExcelError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.v.read().unwrap().clone())
        }
    }

    impl FunctionProvider for Ctx {
        fn get_function(
            &self,
            ns: &str,
            name: &str,
        ) -> Option<std::sync::Arc<dyn formualizer_eval::function::Function>> {
            formualizer_eval::function_registry::get(ns, name)
        }
    }

    impl Resolver for Ctx {}
    impl EvaluationContext for Ctx {}

    let ctx = Ctx::new(LiteralValue::Number(1.0));

    let bytes = br#"{
        "version": 1,
        "sources": [
            { "type": "scalar", "name": "Foo", "version": 1 }
        ],
        "sheets": {
            "S": {
                "cells": [
                    { "row": 1, "col": 1, "formula": "Foo" }
                ]
            }
        }
    }"#
    .to_vec();

    let mut adapter = JsonAdapter::open_bytes(bytes).unwrap();

    let cfg = EvalConfig {
        defer_graph_building: false,
        ..Default::default()
    };

    let mut engine: Engine<_> = Engine::new(ctx.clone(), cfg);
    adapter.stream_into_engine(&mut engine).unwrap();

    let v1 = engine
        .evaluate_cell("S", 1, 1)
        .unwrap()
        .expect("computed value");
    assert_eq!(v1, LiteralValue::Number(1.0));

    ctx.set(LiteralValue::Number(2.0));
    engine.invalidate_source("Foo").unwrap();

    let v2 = engine
        .evaluate_cell("S", 1, 1)
        .unwrap()
        .expect("computed value");
    assert_eq!(v2, LiteralValue::Number(2.0));
    assert!(ctx.calls() >= 2);
}

#[cfg(feature = "json")]
#[test]
fn json_roundtrip_in_memory_bytes() {
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();
    adapter
        .write_cell("Sheet1", 1, 1, CellData::from_value(42.0))
        .unwrap();
    adapter
        .write_cell("Sheet1", 2, 1, CellData::from_formula("=A1*2"))
        .unwrap();

    let bytes = adapter.save_to_bytes().unwrap();

    let mut adapter2 = JsonAdapter::open_bytes(bytes).unwrap();
    let data = adapter2.read_sheet("Sheet1").unwrap();

    assert_eq!(
        data.cells.get(&(1, 1)).unwrap().value,
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(
        data.cells.get(&(2, 1)).unwrap().formula.as_deref(),
        Some("=A1*2")
    );
}

#[cfg(feature = "json")]
#[test]
fn json_schema_shape() {
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();
    adapter
        .write_cell("Sheet1", 1, 1, CellData::from_value(42.0))
        .unwrap();

    let s = adapter.to_json_string().unwrap();
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();

    assert!(v["sheets"].is_object());
    assert!(v["sheets"]["Sheet1"].is_object());
    assert!(v["sheets"]["Sheet1"]["cells"].is_array());
    assert_eq!(v["sheets"]["Sheet1"]["cells"][0]["row"], 1);
    assert_eq!(v["sheets"]["Sheet1"]["cells"][0]["col"], 1);
    assert_eq!(v["sheets"]["Sheet1"]["cells"][0]["value"]["type"], "Number");
    assert_eq!(v["sheets"]["Sheet1"]["cells"][0]["value"]["value"], 42.0);
}

#[cfg(feature = "json")]
#[test]
fn json_transaction_commit_and_rollback() {
    use formualizer_workbook::WriteTransaction;

    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();

    {
        let mut tx = WriteTransaction::new(&mut adapter);
        tx.write_cell("Sheet1", 1, 1, CellData::from_value(1.0));
        // Drop without commit -> rollback
    }

    // Should be empty still
    let sheet = adapter.read_sheet("Sheet1").unwrap();
    assert!(sheet.cells.is_empty());

    {
        let mut tx = WriteTransaction::new(&mut adapter);
        tx.write_cell("Sheet1", 1, 1, CellData::from_value(10.0));
        tx.commit().unwrap();
    }
    let sheet = adapter.read_sheet("Sheet1").unwrap();
    assert_eq!(
        sheet.cells.get(&(1, 1)).unwrap().value,
        Some(LiteralValue::Number(10.0))
    );
}

#[cfg(feature = "json")]
#[test]
fn json_value_variants_roundtrip() {
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();

    let d = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
    let dt = NaiveDateTime::new(d, NaiveTime::from_hms_opt(3, 4, 5).unwrap());
    let t = NaiveTime::from_hms_opt(12, 34, 56).unwrap();
    let dur = ChronoDuration::seconds(3600);

    adapter
        .write_cell("Sheet1", 1, 1, CellData::from_value(LiteralValue::Date(d)))
        .unwrap();
    adapter
        .write_cell(
            "Sheet1",
            2,
            1,
            CellData::from_value(LiteralValue::DateTime(dt)),
        )
        .unwrap();
    adapter
        .write_cell("Sheet1", 3, 1, CellData::from_value(LiteralValue::Time(t)))
        .unwrap();
    adapter
        .write_cell(
            "Sheet1",
            4,
            1,
            CellData::from_value(LiteralValue::Duration(dur)),
        )
        .unwrap();
    adapter
        .write_cell(
            "Sheet1",
            5,
            1,
            CellData::from_value(LiteralValue::Array(vec![
                vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
                vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
            ])),
        )
        .unwrap();
    adapter
        .write_cell(
            "Sheet1",
            6,
            1,
            CellData::from_value(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            ))),
        )
        .unwrap();
    adapter
        .write_cell("Sheet1", 7, 1, CellData::from_value(LiteralValue::Pending))
        .unwrap();

    let bytes = adapter.save_to_bytes().unwrap();
    let mut adapter2 = JsonAdapter::open_bytes(bytes).unwrap();
    let sheet = adapter2.read_sheet("Sheet1").unwrap();

    assert!(
        matches!(sheet.cells.get(&(1,1)).unwrap().value, Some(LiteralValue::Date(d2)) if d2==d)
    );
    assert!(
        matches!(sheet.cells.get(&(2,1)).unwrap().value, Some(LiteralValue::DateTime(dt2)) if dt2==dt)
    );
    assert!(
        matches!(sheet.cells.get(&(3,1)).unwrap().value, Some(LiteralValue::Time(t2)) if t2==t)
    );
    assert!(
        matches!(sheet.cells.get(&(4,1)).unwrap().value, Some(LiteralValue::Duration(d2)) if d2==dur)
    );
    assert!(
        matches!(sheet.cells.get(&(6,1)).unwrap().value, Some(LiteralValue::Error(ref e)) if e == "#DIV/0!")
    );
    assert!(matches!(
        sheet.cells.get(&(7, 1)).unwrap().value,
        Some(LiteralValue::Pending)
    ));
    if let Some(LiteralValue::Array(arr)) = &sheet.cells.get(&(5, 1)).unwrap().value {
        assert_eq!(arr.len(), 2);
        assert!(matches!(arr[0][0], LiteralValue::Number(n) if (n-1.0).abs()<1e-9));
        assert!(matches!(arr[1][1], LiteralValue::Number(n) if (n-4.0).abs()<1e-9));
    } else {
        panic!("Array not roundtripped");
    }
}

#[cfg(feature = "json")]
#[test]
fn json_metadata_roundtrip() {
    use formualizer_workbook::traits::NamedRangeScope;
    use formualizer_workbook::{MergedRange, NamedRange, TableDefinition};
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Meta").unwrap();

    adapter.set_dimensions("Meta", Some((100, 50)));
    adapter.set_date_system_1904("Meta", true);
    adapter.set_merged_cells(
        "Meta",
        vec![MergedRange {
            start_row: 1,
            start_col: 1,
            end_row: 2,
            end_col: 3,
        }],
    );
    adapter.set_tables(
        "Meta",
        vec![TableDefinition {
            name: "T1".into(),
            range: (1, 1, 10, 3),
            headers: vec!["A".into(), "B".into(), "C".into()],
            totals_row: false,
        }],
    );
    adapter.set_named_ranges(
        "Meta",
        vec![NamedRange {
            name: "R1".into(),
            scope: NamedRangeScope::Workbook,
            address: RangeAddress::new("Meta", 1, 1, 2, 2).unwrap(),
        }],
    );

    let bytes = adapter.save_to_bytes().unwrap();
    let mut adapter2 = JsonAdapter::open_bytes(bytes).unwrap();
    let sheet = adapter2.read_sheet("Meta").unwrap();

    assert_eq!(sheet.dimensions, Some((100, 50)));
    assert!(sheet.date_system_1904);
    assert_eq!(sheet.merged_cells.len(), 1);
    assert_eq!(sheet.tables.len(), 1);
    assert_eq!(sheet.named_ranges.len(), 1);
}
