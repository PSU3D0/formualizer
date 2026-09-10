// Integration test for Calamine backend; run with `--features calamine,umya`.
use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};
use std::io::{Cursor, Read, Write};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

fn inject_external_link_rels(bytes: Vec<u8>, idx: u32, target: &str) -> Vec<u8> {
    let reader = Cursor::new(bytes);
    let mut archive = ZipArchive::new(reader).unwrap();

    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let name = entry.name().to_string();
        if entry.is_dir() {
            let _ = writer.add_directory(name, options);
            continue;
        }

        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        writer.start_file(name, options).unwrap();
        writer.write_all(&data).unwrap();
    }

    let rels_name = format!("xl/externalLinks/_rels/externalLink{idx}.xml.rels");
    let rels_xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLinkPath\" Target=\"{target}\" TargetMode=\"External\"/>\n</Relationships>\n"
    );
    let _ = writer.add_directory("xl/externalLinks/_rels/".to_string(), options);
    writer.start_file(rels_name, options).unwrap();
    writer.write_all(rels_xml.as_bytes()).unwrap();

    writer.finish().unwrap().into_inner()
}

#[test]
fn calamine_extracts_formulas_and_normalizes_equals() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(10); // A1
        sh.get_cell_mut((2, 1)).set_formula("A1+5"); // B1 no leading '='
        sh.get_cell_mut((1, 2)).set_formula("=A1*2"); // A2 with leading '='
        sh.get_cell_mut((2, 2)).set_value_number(3); // B2 value only
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 2) {
        // B1
        Some(LiteralValue::Number(n)) => assert!((n - 15.0).abs() < 1e-9, "Expected 15 got {n}"),
        other => panic!("Unexpected B1: {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 2, 1) {
        // A2
        Some(LiteralValue::Number(n)) => assert!((n - 20.0).abs() < 1e-9, "Expected 20 got {n}"),
        other => panic!("Unexpected A2: {other:?}"),
    }
}

#[test]
fn calamine_failed_preparation_preserves_source_inspection_and_history() {
    use formualizer_common::CellAddress;
    use formualizer_eval::engine::inspect::SnapshotOptions;
    use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};

    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("1+2");
        sh.get_cell_mut((2, 1)).set_formula("NOSHEET!A1");
        sh.get_cell_mut((3, 1)).set_formula("\"NOSHEET!A1\"");
    });
    for targeted in [false, true] {
        let adapter = CalamineAdapter::open_path(&path).unwrap();
        let mut wb = Workbook::from_reader(
            adapter,
            LoadStrategy::EagerAll,
            WorkbookConfig::interactive(),
        )
        .unwrap();
        let original: Vec<_> = (1..=3)
            .map(|col| wb.get_formula("Sheet1", 1, col).unwrap())
            .collect();
        for _ in 0..2 {
            for col in 1..=3 {
                let text = &original[col as usize - 1];
                assert_eq!(wb.get_formula("Sheet1", 1, col).as_ref(), Some(text));
                let expected = formualizer_parse::pretty::canonical_formula(
                    &formualizer_parse::parse(format!("={}", text.trim_start_matches('=')))
                        .unwrap(),
                );
                let report = wb
                    .engine()
                    .inspect_cell(
                        &CellAddress::new("Sheet1", 1, col).unwrap(),
                        &SnapshotOptions::default(),
                    )
                    .unwrap();
                assert_eq!(report.cell.formula, Some(expected));
            }
            if targeted {
                assert!(wb.evaluate_cell("Sheet1", 1, 2).is_err());
            } else {
                assert!(wb.evaluate_all().is_err());
            }
        }
        // Logged edits and undo must use the retained source, not imported caches.
        wb.set_formula("Sheet1", 1, 2, "=10").unwrap();
        wb.undo().unwrap();
        assert_eq!(wb.get_formula("Sheet1", 1, 2).as_ref(), Some(&original[1]));
        wb.redo().unwrap();
        assert_eq!(
            wb.evaluate_cell("Sheet1", 1, 2).unwrap(),
            LiteralValue::Number(10.0)
        );
        assert_eq!(
            wb.evaluate_cell("Sheet1", 1, 1).unwrap(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            wb.evaluate_cell("Sheet1", 1, 3).unwrap(),
            LiteralValue::Text("NOSHEET!A1".into())
        );
    }
}

#[test]
fn calamine_ordinary_targets_isolate_unrelated_preparation_failures() {
    use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for (col, formula) in [(1, "1+2"), (2, "NOSHEET!A1"), (3, "B1+1"), (4, "A1+5")] {
            sh.get_cell_mut((col, 1)).set_formula(formula);
        }
    });
    let adapter = CalamineAdapter::open_path(&path).unwrap();
    let mut wb = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(3.0)
    );
    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 4).unwrap(),
        LiteralValue::Number(8.0)
    );
    assert_eq!(
        wb.get_formula("Sheet1", 1, 2)
            .unwrap()
            .trim_start_matches('='),
        "NOSHEET!A1"
    );
    for _ in 0..2 {
        assert!(wb.evaluate_cell("Sheet1", 1, 2).is_err());
        assert!(wb.evaluate_cell("Sheet1", 1, 3).is_err());
        assert!(wb.evaluate_all().is_err());
    }
    // Editing a consumed formula must not be overwritten by residual spool replay.
    wb.set_formula("Sheet1", 1, 1, "=20").unwrap();
    wb.add_sheet("NOSHEET").unwrap();
    wb.set_value("NOSHEET", 1, 1, LiteralValue::Number(42.0))
        .unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(
        wb.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(43.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(25.0))
    );
}

#[test]
#[ignore = "manual indexed target timing probe"]
fn calamine_indexed_target_cost_probe() {
    use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};
    use std::time::Instant;
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for row in 1..=10_000 {
            sh.get_cell_mut((1, row)).set_formula(format!("{row}+2"));
        }
        sh.get_cell_mut((2, 1)).set_formula("NOSHEET!A1");
    });
    let started = Instant::now();
    let adapter = CalamineAdapter::open_path(&path).unwrap();
    let mut wb = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    let load = started.elapsed();
    let started = Instant::now();
    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(3.0)
    );
    let first = started.elapsed();
    let started = Instant::now();
    for row in 2..=101 {
        assert_eq!(
            wb.evaluate_cell("Sheet1", row, 1).unwrap(),
            LiteralValue::Number((row + 2) as f64)
        );
    }
    eprintln!(
        "indexed-target 10001 formulas load={load:?} first={first:?} next100={:?}",
        started.elapsed()
    );
    assert_eq!(
        wb.engine()
            .formula_ingest_report_total()
            .source_formula_records_spooled,
        10_001
    );
    assert!(wb.get_formula("Sheet1", 1, 2).is_some());
}

#[test]
#[ignore = "manual successful load/preparation timing probe"]
fn calamine_source_retention_cost_probe() {
    use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};
    use std::time::Instant;
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for row in 1..=10_000 {
            sh.get_cell_mut((1, row)).set_formula(format!("{row}+2"));
        }
    });
    for _ in 0..5 {
        let started = Instant::now();
        let adapter = CalamineAdapter::open_path(&path).unwrap();
        let mut wb = Workbook::from_reader(
            adapter,
            LoadStrategy::EagerAll,
            WorkbookConfig::interactive(),
        )
        .unwrap();
        let load = started.elapsed();
        let started = Instant::now();
        wb.engine_mut().build_graph_all().unwrap();
        let preparation = started.elapsed();
        assert_eq!(
            wb.evaluate_cell("Sheet1", 10_000, 1).unwrap(),
            LiteralValue::Number(10_002.0)
        );
        eprintln!("source-retention 10000 formulas load={load:?} preparation={preparation:?}");
    }
}

#[test]
fn calamine_error_cells_map() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("=1/0"); // #DIV/0!
    });
    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let sheet = backend.read_sheet("Sheet1").unwrap();
    // Formula node will exist; value is None until evaluation – we focus on later error propagation
    assert!(sheet.cells.contains_key(&(1, 1)));
}

#[test]
fn calamine_loads_external_link_index_formulas() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1))
            .set_formula("=SUM([33]Sheet1!$B:$B)");
    });

    let bytes = std::fs::read(&path).expect("read workbook bytes");
    let bytes = inject_external_link_rels(bytes, 33, "file:///C:/tmp/external.xlsx");
    std::fs::write(&path, bytes).expect("rewrite workbook with external link rels");

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    assert_eq!(
        backend.external_link_target(33),
        Some("file:///C:/tmp/external.xlsx")
    );

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.build_graph_all().unwrap();
}

#[test]
fn calamine_loads_external_link_index_formulas_from_bytes() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1))
            .set_formula("=SUM([33]Sheet1!$B:$B)");
    });

    let bytes = std::fs::read(&path).expect("read workbook bytes");
    let bytes = inject_external_link_rels(bytes, 33, "file:///C:/tmp/external.xlsx");

    let mut backend = CalamineAdapter::open_bytes(bytes).expect("open workbook from bytes");
    assert_eq!(
        backend.external_link_target(33),
        Some("file:///C:/tmp/external.xlsx")
    );

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.build_graph_all().unwrap();
}
