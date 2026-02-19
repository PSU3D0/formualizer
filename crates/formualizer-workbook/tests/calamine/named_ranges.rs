use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::traits::{DefinedNameDefinition, DefinedNameScope};
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};

#[test]
fn calamine_defined_names_include_open_ended_workbook_range() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value("Professional");
        sh.get_cell_mut((2, 1)).set_value_number(123.0);
        sh.add_defined_name("Split", "Sheet1!$A:$B")
            .expect("add defined name");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let names = backend.defined_names().unwrap();
    let split = names
        .iter()
        .find(|dn| dn.name == "Split")
        .expect("Split should be imported by calamine");

    assert_eq!(split.scope, DefinedNameScope::Workbook);
    assert!(split.scope_sheet.is_none());

    match &split.definition {
        DefinedNameDefinition::Range { address } => {
            assert_eq!(address.sheet, "Sheet1");
            assert_eq!(address.start_row, 1);
            assert_eq!(address.start_col, 1);
            assert_eq!(address.end_col, 2);
            assert_eq!(address.end_row, 1_048_576);
        }
        other => panic!("expected range definition, got {other:?}"),
    }
}

#[test]
fn calamine_stream_into_engine_evaluates_vlookup_named_range() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value("Professional");
        sh.get_cell_mut((2, 1)).set_value_number(123.0);
        sh.add_defined_name("Split", "Sheet1!$A:$B")
            .expect("add defined name");
        sh.get_cell_mut((3, 1))
            .set_formula("=VLOOKUP(\"Professional\", Split, 2, FALSE())");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3).unwrap(),
        LiteralValue::Number(123.0)
    );
}
