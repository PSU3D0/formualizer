use formualizer_common::LiteralValue;
#[cfg(any(feature = "umya", feature = "calamine"))]
use formualizer_workbook::SpreadsheetReader;
use formualizer_workbook::{
    CellData, LoadStrategy, SpreadsheetWriter, Workbook, WorkbookConfig, WorkbookLoadLimits,
};

fn assert_sparse_arrow_storage(
    wb: &Workbook,
    sheet: &str,
    expected_rows: u32,
    expected_cols: usize,
) {
    let asheet = wb
        .engine()
        .sheet_store()
        .sheet(sheet)
        .expect("arrow sheet exists");
    assert_eq!(asheet.nrows, expected_rows);
    assert_eq!(asheet.columns.len(), expected_cols);
    let dense_chunks: usize = asheet.columns.iter().map(|col| col.chunks.len()).sum();
    let max_dense_chunks = asheet
        .chunk_starts
        .len()
        .saturating_mul(asheet.columns.len());
    assert!(
        dense_chunks < max_dense_chunks,
        "sparse/adaptive ingest should not materialize every dense chunk"
    );
    assert!(
        asheet
            .columns
            .iter()
            .any(|col| !col.sparse_chunks.is_empty()),
        "expected at least one populated sparse chunk"
    );
}

#[cfg(feature = "json")]
use formualizer_workbook::JsonAdapter;
#[cfg(feature = "umya")]
use formualizer_workbook::UmyaAdapter;

#[cfg(all(feature = "calamine", feature = "umya"))]
use formualizer_workbook::CalamineAdapter;

#[cfg(feature = "umya")]
fn sparse_xlsx_bytes() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book
        .get_sheet_by_name_mut("Sheet1")
        .expect("default sheet exists");
    sheet.get_cell_mut((1, 1_000)).set_value_number(1.0);

    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut buf).expect("write xlsx bytes");
    buf
}

#[cfg(all(feature = "calamine", feature = "umya"))]
fn out_of_order_sparse_xlsx_bytes() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book
        .get_sheet_by_name_mut("Sheet1")
        .expect("default sheet exists");
    sheet.get_cell_mut((1, 200)).set_value_number(200.0);
    sheet.get_cell_mut((1, 1)).set_value_number(1.0);

    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut buf).expect("write xlsx bytes");

    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(buf)).expect("read xlsx zip");
    let mut out = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).expect("zip entry");
        let name = file.name().to_string();
        out.start_file(name.as_str(), options)
            .expect("start zip entry");
        if name == "xl/worksheets/sheet1.xml" {
            let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <dimension ref="A1:A200"/>
  <sheetData>
    <row r="200"><c r="A200"><v>200</v></c></row>
    <row r="1"><c r="A1"><v>1</v></c></row>
  </sheetData>
</worksheet>"#;
            std::io::Write::write_all(&mut out, xml.as_bytes()).expect("write replacement sheet");
        } else {
            std::io::copy(&mut file, &mut out).expect("copy zip entry");
        }
    }
    out.finish().unwrap().into_inner()
}

#[cfg(all(feature = "calamine", feature = "umya"))]
fn shared_formula_xlsx_bytes(rows: u32) -> Vec<u8> {
    use std::io::{Cursor, Write};
    use zip::write::SimpleFileOptions;

    let mut sheet_data = String::new();
    for row in 1..=rows {
        let formula = if row == 1 {
            format!(r#"<f t="shared" si="0" ref="B1:B{rows}">A1*2</f>"#)
        } else {
            r#"<f t="shared" si="0"/>"#.to_string()
        };
        sheet_data.push_str(&format!(
            r#"<row r="{row}"><c r="A{row}"><v>{row}</v></c><c r="B{row}">{formula}<v>9999</v></c></row>"#
        ));
    }
    let sheet_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <dimension ref="A1:B{rows}"/>
  <sheetData>{sheet_data}</sheetData>
</worksheet>"#
    );

    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("[Content_Types].xml", options).unwrap();
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"#).unwrap();
    zip.start_file("_rels/.rels", options).unwrap();
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#).unwrap();
    zip.start_file("xl/workbook.xml", options).unwrap();
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#).unwrap();
    zip.start_file("xl/_rels/workbook.xml.rels", options)
        .unwrap();
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"#).unwrap();
    zip.start_file("xl/styles.xml", options).unwrap();
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs count="1"><xf numFmtId="0"/></cellXfs></styleSheet>"#).unwrap();
    zip.start_file("xl/worksheets/sheet1.xml", options).unwrap();
    zip.write_all(sheet_xml.as_bytes()).unwrap();
    zip.finish().unwrap().into_inner()
}

#[cfg(feature = "umya")]
fn sparse_whole_column_summary_xlsx_bytes() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book
        .get_sheet_by_name_mut("Sheet1")
        .expect("default sheet exists");
    let mut row = 1u32;
    while row <= 999_001 {
        sheet.get_cell_mut((1, row)).set_value_number(row as f64);
        row += 1_000;
    }
    sheet.get_cell_mut((3, 1)).set_formula("=SUM(A:A)");

    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut buf).expect("write xlsx bytes");
    buf
}

fn sparse_limits() -> WorkbookLoadLimits {
    WorkbookLoadLimits {
        max_sheet_rows: 10_000,
        max_sheet_cols: 100,
        max_sheet_logical_cells: u64::MAX,
        sparse_sheet_cell_threshold: 100,
        max_sparse_cell_ratio: 10,
    }
}

#[cfg(feature = "json")]
#[test]
fn json_loader_rejects_dense_sheet_over_logical_budget() {
    let mut adapter = JsonAdapter::new();
    for row in 1..=11 {
        for col in 1..=10 {
            adapter
                .write_cell("S", row, col, CellData::from_value(1.0))
                .expect("write dense cell");
        }
    }

    let mut cfg = WorkbookConfig::ephemeral();
    cfg.ingest_limits = WorkbookLoadLimits {
        max_sheet_rows: 10_000,
        max_sheet_cols: 10_000,
        max_sheet_logical_cells: 100,
        sparse_sheet_cell_threshold: u64::MAX,
        max_sparse_cell_ratio: u64::MAX,
    };

    let err = match Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg) {
        Ok(_) => panic!("dense sheet should hit logical budget"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("logical-cell budget"),
        "unexpected error: {msg}"
    );
}

#[cfg(feature = "json")]
#[test]
fn json_loader_uses_sparse_initial_ingest_over_sparse_guardrail_shape() {
    let mut adapter = JsonAdapter::new();
    adapter
        .write_cell("S", 1_000, 1, CellData::from_value(1.0))
        .expect("write sparse cell");

    let cfg = WorkbookConfig::ephemeral().with_ingest_limits(sparse_limits());
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg)
        .expect("sparse sheet should load through sparse initial ingest");
    assert_eq!(wb.get_value("S", 1_000, 1), Some(LiteralValue::Number(1.0)));
    assert_sparse_arrow_storage(&wb, "S", 1_000, 1);
}

#[cfg(feature = "json")]
#[test]
fn json_sparse_initial_ingest_does_not_mask_formula_with_cached_value() {
    let mut adapter = JsonAdapter::new();
    adapter
        .write_cell("S", 1_000, 1, CellData::from_value(2.0))
        .expect("write sparse precedent");
    adapter
        .write_cell(
            "S",
            1_000,
            2,
            CellData {
                value: Some(LiteralValue::Number(999.0)),
                formula: Some("=A1000*2".to_string()),
                style: None,
            },
        )
        .expect("write formula with stale cached value");

    let cfg = WorkbookConfig::ephemeral().with_ingest_limits(sparse_limits());
    let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg)
        .expect("sparse sheet should load through sparse initial ingest");
    wb.evaluate_all().expect("evaluate formulas");
    assert_eq!(wb.get_value("S", 1_000, 2), Some(LiteralValue::Number(4.0)));
}

#[cfg(feature = "umya")]
#[test]
fn umya_loader_uses_sparse_initial_ingest_over_sparse_guardrail_shape() {
    let adapter = UmyaAdapter::open_bytes(sparse_xlsx_bytes()).expect("open sparse xlsx bytes");
    let cfg = WorkbookConfig::ephemeral().with_ingest_limits(sparse_limits());

    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg)
        .expect("sparse xlsx should load through sparse initial ingest");
    assert_eq!(
        wb.get_value("Sheet1", 1_000, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_sparse_arrow_storage(&wb, "Sheet1", 1_000, 1);
}

#[cfg(all(feature = "calamine", feature = "umya"))]
#[test]
fn calamine_loader_uses_sparse_initial_ingest_over_sparse_guardrail_shape() {
    use std::io::Write;

    let bytes = sparse_xlsx_bytes();
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp xlsx");
    tmp.write_all(&bytes).expect("persist xlsx");

    let adapter = CalamineAdapter::open_path(tmp.path()).expect("open sparse xlsx path");
    let cfg = WorkbookConfig::ephemeral().with_ingest_limits(sparse_limits());

    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg)
        .expect("sparse xlsx should load through sparse initial ingest");
    assert_eq!(
        wb.get_value("Sheet1", 1_000, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_sparse_arrow_storage(&wb, "Sheet1", 1_000, 1);
}

#[cfg(all(feature = "calamine", feature = "umya"))]
#[test]
fn calamine_loader_preserves_out_of_order_sparse_cells() {
    use std::io::Write;

    let bytes = out_of_order_sparse_xlsx_bytes();
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp xlsx");
    tmp.write_all(&bytes).expect("persist xlsx");

    let adapter = CalamineAdapter::open_path(tmp.path()).expect("open out-of-order xlsx path");
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
        .expect("out-of-order sparse sheet should load");
    assert_eq!(
        wb.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 200, 1),
        Some(LiteralValue::Number(200.0))
    );
}

#[cfg(all(feature = "calamine", feature = "umya"))]
#[test]
fn calamine_loader_loads_shared_formula_semantics() {
    use std::io::Write;

    let rows = 128;
    let bytes = shared_formula_xlsx_bytes(rows);
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp xlsx");
    tmp.write_all(&bytes).expect("persist xlsx");

    let adapter = CalamineAdapter::open_path(tmp.path()).expect("open shared formula xlsx path");
    let (mut wb, load_stats) = Workbook::from_reader_with_adapter_stats(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral().with_span_evaluation(true),
    )
    .expect("shared formula workbook should load");

    assert_eq!(
        load_stats
            .expect("adapter stats should be present")
            .formula_cells_observed,
        Some(rows as u64)
    );

    wb.evaluate_all().expect("evaluate shared formulas");
    assert_eq!(
        wb.get_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", rows, 2),
        Some(LiteralValue::Number((rows * 2) as f64))
    );
}

#[cfg(all(feature = "calamine", feature = "umya"))]
#[test]
fn calamine_sparse_initial_ingest_evaluates_whole_column_summary() {
    use std::io::Write;

    let bytes = sparse_whole_column_summary_xlsx_bytes();
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp xlsx");
    tmp.write_all(&bytes).expect("persist xlsx");

    let adapter = CalamineAdapter::open_path(tmp.path()).expect("open sparse xlsx path");
    let mut wb =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .expect("tall sparse whole-column workbook should load");
    assert_sparse_arrow_storage(&wb, "Sheet1", 999_001, 3);

    wb.evaluate_all().expect("evaluate sparse whole-column SUM");
    assert_eq!(
        wb.get_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(499_501_000.0))
    );
}
