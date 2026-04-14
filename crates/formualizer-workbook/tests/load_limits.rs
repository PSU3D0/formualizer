use formualizer_workbook::{
    CellData, LoadStrategy, SpreadsheetReader, SpreadsheetWriter, Workbook, WorkbookConfig,
    WorkbookLoadLimits,
};

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
fn json_loader_rejects_sparse_sheet_over_guardrail() {
    let mut adapter = JsonAdapter::new();
    adapter
        .write_cell("S", 1_000, 1, CellData::from_value(1.0))
        .expect("write sparse cell");

    let cfg = WorkbookConfig::ephemeral().with_ingest_limits(sparse_limits());
    let err = match Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg) {
        Ok(_) => panic!("sparse sheet should hit guardrail"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("sparse-sheet guardrail"),
        "unexpected error: {msg}"
    );
}

#[cfg(feature = "umya")]
#[test]
fn umya_loader_rejects_sparse_sheet_over_guardrail() {
    let adapter = UmyaAdapter::open_bytes(sparse_xlsx_bytes()).expect("open sparse xlsx bytes");
    let cfg = WorkbookConfig::ephemeral().with_ingest_limits(sparse_limits());

    let err = match Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg) {
        Ok(_) => panic!("sparse xlsx should hit guardrail"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("sparse-sheet guardrail"),
        "unexpected error: {msg}"
    );
}

#[cfg(feature = "umya")]
fn dense_xlsx_bytes(rows: u32, cols: u32) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book
        .get_sheet_by_name_mut("Sheet1")
        .expect("default sheet exists");
    for row in 1..=rows {
        for col in 1..=cols {
            sheet.get_cell_mut((col, row)).set_value_number(1.0);
        }
    }
    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut buf).expect("write xlsx bytes");
    buf
}

#[cfg(feature = "umya")]
#[test]
fn recalculate_file_with_config_rejects_over_budget() {
    use std::io::Write;

    let bytes = dense_xlsx_bytes(11, 10);
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp xlsx");
    tmp.write_all(&bytes).expect("persist xlsx");

    let mut limits = WorkbookLoadLimits::default();
    limits.max_sheet_logical_cells = 50;

    let err = match formualizer_workbook::recalculate_file_with_config(
        tmp.path(),
        None,
        Some(limits),
    ) {
        Ok(_) => panic!("dense workbook should hit logical-cell budget"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("logical-cell budget"),
        "unexpected error: {msg}"
    );
}

#[cfg(feature = "umya")]
#[test]
fn recalculate_file_with_config_none_matches_default() {
    use std::io::Write;

    let bytes = dense_xlsx_bytes(3, 3);
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp xlsx");
    tmp.write_all(&bytes).expect("persist xlsx");

    let summary =
        formualizer_workbook::recalculate_file_with_config(tmp.path(), None, None)
            .expect("default budget should succeed");
    assert_eq!(summary.errors, 0);
}

#[cfg(all(feature = "calamine", feature = "umya"))]
#[test]
fn calamine_loader_rejects_sparse_sheet_over_guardrail() {
    use std::io::Write;

    let bytes = sparse_xlsx_bytes();
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp xlsx");
    tmp.write_all(&bytes).expect("persist xlsx");

    let adapter = CalamineAdapter::open_path(tmp.path()).expect("open sparse xlsx path");
    let cfg = WorkbookConfig::ephemeral().with_ingest_limits(sparse_limits());

    let err = match Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg) {
        Ok(_) => panic!("sparse xlsx should hit guardrail"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("sparse-sheet guardrail"),
        "unexpected error: {msg}"
    );
}
