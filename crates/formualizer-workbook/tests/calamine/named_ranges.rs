use crate::common::build_workbook;
use formualizer_common::error::ExcelErrorKind;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::traits::{DefinedNameDefinition, DefinedNameScope};
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};
use std::io::Read;

fn defined_name_entity_spacing_fixture() -> Vec<u8> {
    let path = build_workbook(|book| {
        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        sheet1.get_cell_mut((1, 1)).set_formula("=TightName");
        sheet1.get_cell_mut((1, 2)).set_formula("=SpacedName");
        sheet1.get_cell_mut((1, 3)).set_formula("=LeftSpacedName");
        sheet1.get_cell_mut((1, 4)).set_formula("=LocalSpacedName");
        sheet1
            .add_defined_name("TightName", "'Alpha&Beta Rates'!$A$1")
            .expect("add tight defined name");
        sheet1
            .add_defined_name("SpacedName", "'Alpha & Beta Rates'!$A$1")
            .expect("add spaced defined name");
        sheet1
            .add_defined_name("LeftSpacedName", "'Gamma &Delta Rates'!$A$1")
            .expect("add one-sided-space defined name");
        // A sheet-scoped name (localSheetId) whose target also has an entity.
        sheet1
            .add_defined_name("LocalSpacedName", "'Alpha & Beta Rates'!$A$1")
            .expect("add sheet-scoped spaced defined name");
        sheet1
            .get_defined_names_mut()
            .last_mut()
            .expect("sheet-scoped defined name")
            .set_local_sheet_id(0);

        book.new_sheet("Alpha&Beta Rates")
            .expect("add tight sheet")
            .get_cell_mut((1, 1))
            .set_value_number(11.0);
        book.new_sheet("Alpha & Beta Rates")
            .expect("add spaced sheet")
            .get_cell_mut((1, 1))
            .set_value_number(22.0);
        book.new_sheet("Gamma &Delta Rates")
            .expect("add one-sided-space sheet")
            .get_cell_mut((1, 1))
            .set_value_number(33.0);
    });
    let bytes = std::fs::read(path).expect("read invented workbook");

    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&bytes)).expect("open xlsx zip");
    let mut workbook_xml = String::new();
    archive
        .by_name("xl/workbook.xml")
        .expect("workbook.xml")
        .read_to_string(&mut workbook_xml)
        .expect("read workbook.xml");
    assert!(workbook_xml.contains("'Alpha&amp;Beta Rates'!$A$1"));
    assert!(workbook_xml.contains("'Alpha &amp; Beta Rates'!$A$1"));
    assert!(workbook_xml.contains("'Gamma &amp;Delta Rates'!$A$1"));
    assert!(workbook_xml.contains("localSheetId=\"0\""));

    bytes
}

fn assert_defined_name_entity_spacing(mut backend: CalamineAdapter) {
    let expected_sheets = [
        "Sheet1",
        "Alpha&Beta Rates",
        "Alpha & Beta Rates",
        "Gamma &Delta Rates",
    ];
    assert_eq!(backend.sheet_names().unwrap(), expected_sheets);

    let names = backend.defined_names().expect("read defined names");
    for (name, expected_sheet, expected_scope_sheet) in [
        ("TightName", "Alpha&Beta Rates", None),
        ("SpacedName", "Alpha & Beta Rates", None),
        ("LeftSpacedName", "Gamma &Delta Rates", None),
        ("LocalSpacedName", "Alpha & Beta Rates", Some("Sheet1")),
    ] {
        let definition = names
            .iter()
            .find(|defined| defined.name == name)
            .unwrap_or_else(|| panic!("missing {name}"));
        let expected_scope = if expected_scope_sheet.is_some() {
            DefinedNameScope::Sheet
        } else {
            DefinedNameScope::Workbook
        };
        assert_eq!(definition.scope, expected_scope, "scope of {name}");
        assert_eq!(
            definition.scope_sheet.as_deref(),
            expected_scope_sheet,
            "scope sheet of {name}"
        );
        match &definition.definition {
            DefinedNameDefinition::Range { address } => {
                assert_eq!(address.sheet, expected_sheet, "target sheet for {name}");
                assert_eq!(
                    (
                        address.start_row,
                        address.start_col,
                        address.end_row,
                        address.end_col
                    ),
                    (1, 1, 1, 1),
                    "target cell for {name}"
                );
            }
            other => panic!("expected range definition for {name}, got {other:?}"),
        }
    }

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend
        .stream_into_engine(&mut engine)
        .expect("stream invented workbook");
    engine.evaluate_all().expect("evaluate invented workbook");

    let registered: Vec<String> = engine
        .sheet_store()
        .sheets
        .iter()
        .map(|sheet| sheet.name.to_string())
        .collect();
    assert_eq!(
        registered, expected_sheets,
        "no phantom sheet may be registered"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(11.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(22.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(33.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(22.0))
    );
}

#[test]
fn defined_name_entity_spacing_open_path_preserves_distinct_targets() {
    let bytes = defined_name_entity_spacing_fixture();
    let file = tempfile::NamedTempFile::new().expect("create temp xlsx");
    std::fs::write(file.path(), bytes).expect("write temp xlsx");
    let backend = CalamineAdapter::open_path(file.path()).expect("open workbook from path");
    assert_defined_name_entity_spacing(backend);
}

#[test]
fn defined_name_entity_spacing_open_bytes_preserves_distinct_targets() {
    let bytes = defined_name_entity_spacing_fixture();
    let backend = CalamineAdapter::open_bytes(bytes).expect("open workbook from bytes");
    assert_defined_name_entity_spacing(backend);
}

fn assert_name_error(value: LiteralValue) {
    match value {
        LiteralValue::Error(err) => assert_eq!(err.kind, ExcelErrorKind::Name),
        other => panic!("expected #NAME?, got {other:?}"),
    }
}

#[test]
fn calamine_defined_names_preserve_sheet_scope_and_base_sheet_resolution() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("Sheet2");

        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        sheet1.get_cell_mut((1, 1)).set_value_number(9.0);
        sheet1
            .add_defined_name("LocalOnly", "$A$1")
            .expect("add local defined name without sheet prefix");
        let local = sheet1
            .get_defined_names_mut()
            .last_mut()
            .expect("local defined name");
        local.set_local_sheet_id(0);

        let sheet2 = book.get_sheet_by_name_mut("Sheet2").expect("Sheet2");
        sheet2.get_cell_mut((1, 1)).set_value_number(42.0);
        sheet2
            .add_defined_name("GlobalOnly", "Sheet2!$A$1")
            .expect("add workbook defined name");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let names = backend.defined_names().unwrap();

    let local = names
        .iter()
        .find(|dn| dn.name == "LocalOnly")
        .expect("LocalOnly should be imported by calamine");
    assert_eq!(local.scope, DefinedNameScope::Sheet);
    assert_eq!(local.scope_sheet.as_deref(), Some("Sheet1"));
    match &local.definition {
        DefinedNameDefinition::Range { address } => {
            assert_eq!(address.sheet, "Sheet1");
            assert_eq!(address.start_row, 1);
            assert_eq!(address.start_col, 1);
            assert_eq!(address.end_row, 1);
            assert_eq!(address.end_col, 1);
        }
        other => panic!("expected range definition, got {other:?}"),
    }

    let global = names
        .iter()
        .find(|dn| dn.name == "GlobalOnly")
        .expect("GlobalOnly should be imported by calamine");
    assert_eq!(global.scope, DefinedNameScope::Workbook);
    assert!(global.scope_sheet.is_none());
}

#[test]
fn calamine_defined_names_from_bytes_preserve_sheet_scope_and_base_sheet_resolution() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("Sheet2");

        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        sheet1.get_cell_mut((1, 1)).set_value_number(9.0);
        sheet1
            .add_defined_name("LocalOnly", "$A$1")
            .expect("add local defined name without sheet prefix");
        let local = sheet1
            .get_defined_names_mut()
            .last_mut()
            .expect("local defined name");
        local.set_local_sheet_id(0);

        let sheet2 = book.get_sheet_by_name_mut("Sheet2").expect("Sheet2");
        sheet2.get_cell_mut((1, 1)).set_value_number(42.0);
        sheet2
            .add_defined_name("GlobalOnly", "Sheet2!$A$1")
            .expect("add workbook defined name");
    });
    let bytes = std::fs::read(path).expect("read workbook bytes");

    let mut backend = CalamineAdapter::open_bytes(bytes).expect("open workbook from bytes");
    let names = backend.defined_names().unwrap();

    let local = names
        .iter()
        .find(|dn| dn.name == "LocalOnly")
        .expect("LocalOnly should be imported by calamine");
    assert_eq!(local.scope, DefinedNameScope::Sheet);
    assert_eq!(local.scope_sheet.as_deref(), Some("Sheet1"));

    let global = names
        .iter()
        .find(|dn| dn.name == "GlobalOnly")
        .expect("GlobalOnly should be imported by calamine");
    assert_eq!(global.scope, DefinedNameScope::Workbook);
    assert!(global.scope_sheet.is_none());
}

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

#[test]
fn calamine_sheet_local_name_resolves_only_on_declaring_sheet() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("Sheet2");

        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        sheet1.get_cell_mut((1, 1)).set_value_number(7.0);
        sheet1
            .add_defined_name("LocalOnly", "Sheet1!$A$1")
            .expect("add local defined name");
        let local = sheet1
            .get_defined_names_mut()
            .last_mut()
            .expect("local defined name");
        local.set_local_sheet_id(0);
        sheet1.get_cell_mut((2, 1)).set_formula("=LocalOnly*2");

        let sheet2 = book.get_sheet_by_name_mut("Sheet2").expect("Sheet2");
        sheet2.get_cell_mut((2, 1)).set_formula("=LocalOnly");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    let local_value = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert!(matches!(local_value, LiteralValue::Number(n) if (n - 14.0).abs() < 1e-9));

    let other_sheet_value = engine.get_cell_value("Sheet2", 1, 2).unwrap();
    assert_name_error(other_sheet_value);
}

#[test]
fn calamine_sheet_local_name_shadows_workbook_name_only_on_declaring_sheet() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("Sheet2");

        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        sheet1.get_cell_mut((1, 1)).set_value_number(10.0);
        sheet1
            .add_defined_name("ScopedValue", "Sheet1!$A$1")
            .expect("add local defined name");
        let local = sheet1
            .get_defined_names_mut()
            .last_mut()
            .expect("local defined name");
        local.set_local_sheet_id(0);
        sheet1.get_cell_mut((2, 1)).set_formula("=ScopedValue*2");

        let sheet2 = book.get_sheet_by_name_mut("Sheet2").expect("Sheet2");
        sheet2.get_cell_mut((1, 1)).set_value_number(100.0);
        sheet2
            .add_defined_name("ScopedValue", "Sheet2!$A$1")
            .expect("add workbook defined name");
        sheet2.get_cell_mut((2, 1)).set_formula("=ScopedValue*2");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    let local_value = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert!(matches!(local_value, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9));

    let workbook_value = engine.get_cell_value("Sheet2", 1, 2).unwrap();
    assert!(matches!(workbook_value, LiteralValue::Number(n) if (n - 200.0).abs() < 1e-9));
}

#[test]
fn calamine_same_local_name_on_multiple_sheets_is_isolated() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("Data");

        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        sheet1.get_cell_mut((1, 1)).set_value_number(2.0);
        sheet1
            .add_defined_name("LocalDup", "Sheet1!$A$1")
            .expect("add Sheet1 local name");
        let local = sheet1
            .get_defined_names_mut()
            .last_mut()
            .expect("Sheet1 local defined name");
        local.set_local_sheet_id(0);
        sheet1.get_cell_mut((2, 1)).set_formula("=LocalDup*3");

        let data = book.get_sheet_by_name_mut("Data").expect("Data");
        data.get_cell_mut((1, 1)).set_value_number(5.0);
        data.add_defined_name("LocalDup", "Data!$A$1")
            .expect("add Data local name");
        let local = data
            .get_defined_names_mut()
            .last_mut()
            .expect("Data local defined name");
        local.set_local_sheet_id(1);
        data.get_cell_mut((2, 1)).set_formula("=LocalDup*3");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    let sheet1_value = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert!(matches!(sheet1_value, LiteralValue::Number(n) if (n - 6.0).abs() < 1e-9));

    let data_value = engine.get_cell_value("Data", 1, 2).unwrap();
    assert!(matches!(data_value, LiteralValue::Number(n) if (n - 15.0).abs() < 1e-9));
}
