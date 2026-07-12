use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig, FormulaPlaneMode};
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};
use std::io::{Cursor, Read, Write};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

fn epoch_replay_xlsx(rows: u32, mixed: bool) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
    for row in 1..=rows {
        sheet.get_cell_mut((1, row)).set_value_number(row as f64);
        sheet
            .get_cell_mut((2, row))
            .set_formula(format!("A{row}+1"));
    }
    if mixed {
        sheet.get_cell_mut("C1").set_formula("A1*2");
    }
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();

    let mut input = ZipArchive::new(Cursor::new(original)).unwrap();
    let mut output = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    for index in 0..input.len() {
        let mut entry = input.by_index(index).unwrap();
        let name = entry.name().to_string();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        if name == "xl/worksheets/sheet1.xml" {
            let mut xml = String::from_utf8(bytes).unwrap();
            xml = xml.replace(
                "<f>A1+1</f>",
                &format!("<f t=\"shared\" si=\"31\" ref=\"B1:B{rows}\">A1+1</f>"),
            );
            for row in 2..=rows {
                xml = xml.replace(
                    &format!("<f>A{row}+1</f>"),
                    "<f t=\"shared\" si=\"31\"></f>",
                );
            }
            bytes = xml.into_bytes();
        }
        output.start_file(name, options).unwrap();
        output.write_all(&bytes).unwrap();
    }
    output.finish().unwrap().into_inner()
}

fn assert_stale_adapter_replays_all(mixed: bool, alias: &'static str) {
    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
    engine.set_before_prepared_span_commit_hook(move || {
        formualizer_eval::function_registry::register_alias("", alias, "", "ABS");
    });
    let mut adapter = CalamineAdapter::open_bytes(epoch_replay_xlsx(100, mixed)).unwrap();
    adapter.stream_into_engine(&mut engine).unwrap();

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        engine.baseline_stats().graph_formula_vertex_count,
        if mixed { 101 } else { 100 }
    );
    engine.evaluate_all().unwrap();
    for row in 1..=100 {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(row as f64 + 1.0)),
            "row {row}"
        );
    }
    if mixed {
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 3),
            Some(LiteralValue::Number(2.0))
        );
    }
}

#[test]
fn calamine_all_direct_epoch_change_replays_complete_legacy_graph() {
    assert_stale_adapter_replays_all(false, "__CALAMINE_ALL_DIRECT_EPOCH__");
}

#[test]
fn calamine_mixed_epoch_change_replays_complete_legacy_graph() {
    assert_stale_adapter_replays_all(true, "__CALAMINE_MIXED_EPOCH__");
}
