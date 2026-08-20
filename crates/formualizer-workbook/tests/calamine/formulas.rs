// Integration test for Calamine backend; run with `--features calamine,umya`.
use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, WorkbookConfig};
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

/// Inject an in-workbook external link (spec §10) into an `.xlsx` zip: a
/// `xl/externalLinks/externalLink1.xml` part with cached `sheetData` values,
/// plus the `<externalReferences>` entry and the matching workbook relationship.
/// `cells` maps cell refs to cached string values (e.g. `("A1", "42")`).
fn inject_inbook_external_link(bytes: Vec<u8>, sheet_name: &str, cells: &[(&str, &str)]) -> Vec<u8> {
    let reader = Cursor::new(bytes);
    let mut archive = ZipArchive::new(reader).unwrap();

    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);

    let mut sheet_data = String::new();
    for (cell_ref, value) in cells {
        let row: u32 = cell_ref
            .chars()
            .skip_while(|c| c.is_ascii_alphabetic())
            .collect::<String>()
            .parse()
            .expect("row from cell ref");
        sheet_data.push_str(&format!(
            "<row r=\"{row}\"><c r=\"{cell_ref}\"><v>{value}</v></c></row>"
        ));
    }
    let ext_xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\
         <externalLink xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" \
         xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">\
         <externalBook r:id=\"rId1\"><sheetNames><sheetName val=\"{sheet_name}\"/></sheetNames>\
         <sheetDataSet><sheetData sheetId=\"0\">{sheet_data}</sheetData></sheetDataSet>\
         </externalBook></externalLink>\n"
    );

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let name = entry.name().to_string();
        if entry.is_dir() {
            let _ = writer.add_directory(name, options);
            continue;
        }
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        let data = match name.as_str() {
            "xl/workbook.xml" => String::from_utf8(data)
                .expect("workbook.xml utf8")
                .replace(
                    "</workbook>",
                    "<externalReferences><externalReference r:id=\"rId9000\"/></externalReferences></workbook>",
                )
                .into_bytes(),
            "xl/_rels/workbook.xml.rels" => String::from_utf8(data)
                .expect("workbook rels utf8")
                .replace(
                    "</Relationships>",
                    "<Relationship Id=\"rId9000\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLink\" Target=\"externalLinks/externalLink1.xml\"/></Relationships>",
                )
                .into_bytes(),
            _ => data,
        };
        writer.start_file(name, options).unwrap();
        writer.write_all(&data).unwrap();
    }
    let _ = writer.add_directory("xl/externalLinks/".to_string(), options);
    writer.start_file("xl/externalLinks/externalLink1.xml", options).unwrap();
    writer.write_all(ext_xml.as_bytes()).unwrap();
    writer.finish().unwrap().into_inner()
}

#[test]
fn calamine_evaluates_inbook_external_reference_from_cached_values() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("[1]Sheet1!A1"); // B1
    });

    let bytes = std::fs::read(&path).expect("read workbook bytes");
    let bytes = inject_inbook_external_link(bytes, "Sheet1", &[("A1", "42")]);
    std::fs::write(&path, bytes).expect("rewrite workbook with external link part");

    let backend = CalamineAdapter::open_path(&path).unwrap();
    assert_eq!(
        backend.external_cached_sources(),
        &[("[1]Sheet1!A1".to_string(), LiteralValue::Number(42.0))]
    );

    let mut wb = formualizer_workbook::Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral(),
    )
    .unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(
        wb.get_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn calamine_inbook_external_reference_without_cache_is_undefined_name() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("[1]Sheet1!A1"); // B1
    });

    // No external link part injected: the reference has no cached source, so
    // evaluation must fail gracefully (#NAME?: Undefined name) instead of
    // panicking. `interactive` defers graph building, matching the report path.
    let backend = CalamineAdapter::open_path(&path).unwrap();
    assert!(backend.external_cached_sources().is_empty());

    let mut wb = formualizer_workbook::Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    let res = wb.evaluate_all();
    match res {
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                msg.contains("#NAME?") || msg.contains("Undefined name"),
                "expected undefined-name error, got: {msg}"
            );
        }
        Ok(_) => assert!(matches!(
            wb.get_value("Sheet1", 1, 2),
            Some(LiteralValue::Error(_))
        )),
    }
}
