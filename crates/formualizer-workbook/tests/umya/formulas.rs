// Integration test for Umya backend; run with `--features umya`.

use crate::common::build_workbook;
use formualizer_common::ExcelErrorKind;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig, FormulaParsePolicy};
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
use std::fs::File;
use std::io::{Read, Write};
use zip::write::FileOptions;

fn rewrite_sheet_formula(path: &std::path::Path, sheet_xml: &str, from: &str, to: &str) {
    let in_file = File::open(path).expect("open input xlsx");
    let mut zin = zip::ZipArchive::new(in_file).expect("zip open");

    let out_path = path.with_file_name("fixture-formula-rewrite.xlsx");
    let out_file = File::create(&out_path).expect("create output xlsx");
    let mut zout = zip::ZipWriter::new(out_file);
    let options = FileOptions::default();

    let mut replaced = false;

    for i in 0..zin.len() {
        let mut f = zin.by_index(i).expect("zip entry");
        let name = f.name().to_string();

        if f.is_dir() {
            let _ = zout.add_directory(name, options);
            continue;
        }

        let mut data = Vec::new();
        f.read_to_end(&mut data).expect("read entry");

        if name == sheet_xml {
            let mut xml = String::from_utf8(data).expect("sheet xml utf8");
            let before = xml.clone();
            xml = xml.replace(from, to);
            replaced = xml != before;
            data = xml.into_bytes();
        }

        zout.start_file(name, options).expect("zip write start");
        zout.write_all(&data).expect("zip write");
    }

    zout.finish().expect("zip finish");
    assert!(
        replaced,
        "expected to rewrite formula `{from}` in `{sheet_xml}`"
    );

    std::fs::copy(&out_path, path).expect("overwrite fixture");
}

#[test]
fn umya_extracts_formulas_and_normalizes_equals() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(10); // A1
        sh.get_cell_mut((2, 1)).set_formula("A1+5"); // B1 no '='
        sh.get_cell_mut((1, 2)).set_formula("=A1*2"); // A2
        sh.get_cell_mut((2, 2)).set_value_number(3); // B2
    });
    let mut backend = UmyaAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::FastBatch);
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 2) {
        // B1
        Some(LiteralValue::Number(n)) => assert!((n - 15.0).abs() < 1e-9),
        other => panic!("Unexpected B1: {:?}", other),
    }
    match engine.get_cell_value("Sheet1", 2, 1) {
        // A2
        Some(LiteralValue::Number(n)) => assert!((n - 20.0).abs() < 1e-9),
        other => panic!("Unexpected A2: {:?}", other),
    }
}

#[test]
fn umya_lowercase_ref_literal_in_formula_xml_loads_successfully() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("source");
        let source_sheet = book.get_sheet_by_name_mut("source").unwrap();
        source_sheet.get_cell_mut((1, 1)).set_value_number(7);

        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("source!A1");
    });

    // Mimic legacy corrupted XML formula text seen in production files.
    rewrite_sheet_formula(
        &path,
        "xl/worksheets/sheet1.xml",
        "source!A1",
        "source!#ref!",
    );

    let mut backend = UmyaAdapter::open_path(&path).expect("open patched workbook");
    let sheet = backend.read_sheet("Sheet1").expect("read sheet");
    assert_eq!(
        sheet
            .cells
            .get(&(1, 1))
            .and_then(|cell| cell.formula.clone()),
        Some("=source!#ref!".to_string())
    );

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend
        .stream_into_engine(&mut engine)
        .expect("lowercase #ref should ingest");
}

#[test]
fn umya_strict_mode_reports_parser_error_for_invalid_error_literal() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("source");
        let source_sheet = book.get_sheet_by_name_mut("source").unwrap();
        source_sheet.get_cell_mut((1, 1)).set_value_number(7);

        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("source!A1");
    });

    rewrite_sheet_formula(
        &path,
        "xl/worksheets/sheet1.xml",
        "source!A1",
        "source!#bad!",
    );

    let mut backend = UmyaAdapter::open_path(&path).expect("open patched workbook");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    let err = backend.stream_into_engine(&mut engine).unwrap_err();
    let msg = err.to_string();

    assert!(msg.contains("Formula parse error"), "{msg}");
    assert!(msg.contains("ParserError"), "{msg}");
    assert!(msg.contains("Invalid error code"), "{msg}");
}

#[test]
fn umya_interactive_mode_coerces_malformed_formula_to_error() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("source");
        let source_sheet = book.get_sheet_by_name_mut("source").unwrap();
        source_sheet.get_cell_mut((1, 1)).set_value_number(7);

        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("source!A1");
    });
    rewrite_sheet_formula(
        &path,
        "xl/worksheets/sheet1.xml",
        "source!A1",
        "source!#bad!",
    );

    let backend = UmyaAdapter::open_path(&path).expect("open patched workbook");
    let mut wb = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load workbook");

    let value = wb
        .evaluate_cell("Sheet1", 1, 1)
        .expect("evaluate malformed cell");
    assert!(matches!(
        value,
        LiteralValue::Error(ref e) if e.kind == ExcelErrorKind::Error
    ));

    let diags = wb.engine().formula_parse_diagnostics();
    assert_eq!(diags.len(), 1);
    assert_eq!(diags[0].sheet, "Sheet1");
    assert_eq!(diags[0].row, 1);
    assert_eq!(diags[0].col, 1);
    assert_eq!(diags[0].policy, FormulaParsePolicy::CoerceToError);
}

#[test]
fn umya_eager_ingest_coerces_malformed_formula_when_policy_set() {
    let path = build_workbook(|book| {
        let _ = book.new_sheet("source");
        let source_sheet = book.get_sheet_by_name_mut("source").unwrap();
        source_sheet.get_cell_mut((1, 1)).set_value_number(7);

        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("source!A1");
    });
    rewrite_sheet_formula(
        &path,
        "xl/worksheets/sheet1.xml",
        "source!A1",
        "source!#bad!",
    );

    let mut backend = UmyaAdapter::open_path(&path).expect("open patched workbook");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let cfg = EvalConfig {
        defer_graph_building: false,
        formula_parse_policy: FormulaParsePolicy::CoerceToError,
        ..Default::default()
    };
    let mut engine: Engine<_> = Engine::new(ctx, cfg);

    backend
        .stream_into_engine(&mut engine)
        .expect("ingest malformed formula with coerce policy");
    engine.evaluate_all().expect("evaluate");

    assert!(matches!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Error
    ));

    let diags = engine.formula_parse_diagnostics();
    assert_eq!(diags.len(), 1);
    assert_eq!(diags[0].policy, FormulaParsePolicy::CoerceToError);
}
