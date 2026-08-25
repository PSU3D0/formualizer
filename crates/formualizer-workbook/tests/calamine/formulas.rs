// Integration test for Calamine backend; run with `--features calamine,umya`.
use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{
    CalamineAdapter, ExternalCachedSource, LiteralValue, LoadStrategy, SpreadsheetReader,
    WorkbookConfig,
};
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
fn inject_inbook_external_link(
    bytes: Vec<u8>,
    sheet_name: &str,
    cells: &[(&str, &str)],
) -> Vec<u8> {
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
    writer
        .start_file("xl/externalLinks/externalLink1.xml", options)
        .unwrap();
    writer.write_all(ext_xml.as_bytes()).unwrap();
    writer.finish().unwrap().into_inner()
}

#[test]
fn calamine_scan_exposes_structured_cached_sources() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("[1]Sheet1!A1"); // B1
    });

    let bytes = std::fs::read(&path).expect("read workbook bytes");
    let bytes = inject_inbook_external_link(bytes, "Sheet1", &[("A1", "42")]);
    std::fs::write(&path, bytes).expect("rewrite workbook with external link part");

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    // Deferred scan: empty until `scan_external_cached_sources` is called (the
    // `Workbook::from_reader` path does this with ingest limits applied).
    assert!(backend.external_cached_sources().is_empty());
    let limits = formualizer_eval::engine::WorkbookLoadLimits::default();
    backend.scan_external_cached_sources(&limits).unwrap();
    assert_eq!(
        backend.external_cached_sources(),
        &[ExternalCachedSource {
            book_index: 1,
            sheet: "Sheet1".to_string(),
            row: 1,
            col: 1,
            value: LiteralValue::Number(42.0),
        }]
    );
    assert_eq!(
        backend.load_stats().unwrap().external_cached_source_cells,
        Some(1)
    );
    assert_eq!(
        backend.load_stats().unwrap().external_link_scan_failures,
        Some(0)
    );
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

    let mut wb = formualizer_workbook::Workbook::from_reader(
        CalamineAdapter::open_path(&path).unwrap(),
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

/// Cached sources resolve structurally: authored variants of the same reference
/// (`$` anchors, sheet-name case, spacing) must all hit the same canonical key.
#[test]
fn calamine_evaluates_inbook_external_reference_structurally() {
    for formula in [
        "[1]Sheet1!A1",
        "[1]Sheet1!$A$1",
        "[1]sheet1!A1",
        "'[1]Sheet1'!A1",
    ] {
        let path = build_workbook(|book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
            sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
            sh.get_cell_mut((2, 1)).set_formula(formula); // B1
        });

        let bytes = std::fs::read(&path).expect("read workbook bytes");
        let bytes = inject_inbook_external_link(bytes, "Sheet1", &[("A1", "42")]);
        std::fs::write(&path, bytes).expect("rewrite workbook with external link part");

        let mut wb = formualizer_workbook::Workbook::from_reader(
            CalamineAdapter::open_path(&path).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        )
        .unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Sheet1", 1, 2),
            Some(LiteralValue::Number(42.0)),
            "formula {formula:?} should resolve to the cached value"
        );
    }
}

/// A sheet name that needs quoting in an authored reference (contains a space)
/// still matches a cached source for the same sheet.
#[test]
fn calamine_inbook_external_reference_punctuated_sheet_name() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("'[1]My Sheet'!A1"); // B1
    });

    let bytes = std::fs::read(&path).expect("read workbook bytes");
    let bytes = inject_inbook_external_link(bytes, "My Sheet", &[("A1", "42")]);
    std::fs::write(&path, bytes).expect("rewrite workbook with external link part");

    let mut wb = formualizer_workbook::Workbook::from_reader(
        CalamineAdapter::open_path(&path).unwrap(),
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

/// The ingest-limit entry cap bounds how many cached external cells are seeded.
#[test]
fn calamine_external_cached_sources_respect_ingest_cap() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("[1]Sheet1!A100"); // B1
    });

    let bytes = std::fs::read(&path).expect("read workbook bytes");
    // Cache 100 cells (A1..A100); a tiny cap must stop well before that.
    let cells: Vec<(String, String)> = (1..=100)
        .map(|row| (format!("A{row}"), format!("{row}")))
        .collect();
    let cell_refs: Vec<(&str, &str)> = cells
        .iter()
        .map(|(r, v)| (r.as_str(), v.as_str()))
        .collect();
    let bytes = inject_inbook_external_link(bytes, "Sheet1", &cell_refs);
    std::fs::write(&path, bytes).expect("rewrite workbook with external link part");

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let limits = formualizer_eval::engine::WorkbookLoadLimits {
        max_external_cached_cells: 5,
        ..Default::default()
    };
    backend.scan_external_cached_sources(&limits).unwrap();
    assert_eq!(
        backend.load_stats().unwrap().external_cached_source_cells,
        Some(5)
    );
    assert_eq!(backend.external_cached_sources().len(), 5);
}

/// A declared external reference whose part is missing records a scan failure
/// instead of silently loading an empty cache.
#[test]
fn calamine_external_link_scan_failures_are_recorded() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("[1]Sheet1!A1"); // B1
    });

    // Inject the `<externalReferences>` entry but *not* the externalLink part.
    let bytes = std::fs::read(&path).expect("read workbook bytes");
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
        let data = if name == "xl/workbook.xml" {
            String::from_utf8(data)
                .expect("workbook.xml utf8")
                .replace(
                    "</workbook>",
                    "<externalReferences><externalReference r:id=\"rId9000\"/></externalReferences></workbook>",
                )
                .into_bytes()
        } else {
            data
        };
        writer.start_file(name, options).unwrap();
        writer.write_all(&data).unwrap();
    }
    let bytes = writer.finish().unwrap().into_inner();
    std::fs::write(&path, bytes).expect("rewrite workbook");

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let limits = formualizer_eval::engine::WorkbookLoadLimits::default();
    backend.scan_external_cached_sources(&limits).unwrap();
    assert_eq!(
        backend.load_stats().unwrap().external_link_scan_failures,
        Some(1),
        "missing externalLink part should be recorded as a scan failure"
    );
    assert!(backend.external_cached_sources().is_empty());
}

/// The `resolve_external_cached_sources` knob disables cached-source seeding,
/// restoring the pre-#363 behavior where the reference fails with `#NAME?`.
#[test]
fn calamine_inbook_external_reference_can_be_disabled() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(42.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("[1]Sheet1!A1"); // B1
    });

    let bytes = std::fs::read(&path).expect("read workbook bytes");
    let bytes = inject_inbook_external_link(bytes, "Sheet1", &[("A1", "42")]);
    std::fs::write(&path, bytes).expect("rewrite workbook with external link part");

    let config = WorkbookConfig::interactive().with_external_cached_sources(false);
    let mut wb = formualizer_workbook::Workbook::from_reader(
        CalamineAdapter::open_path(&path).unwrap(),
        LoadStrategy::EagerAll,
        config,
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
