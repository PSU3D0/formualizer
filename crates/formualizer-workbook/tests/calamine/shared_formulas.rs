use formualizer_common::RangeAddress;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{
    Engine, EvalConfig, FormulaIngestReport, FormulaParsePolicy, FormulaPlaneMode,
};
use formualizer_workbook::LiteralValue;
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

fn genuinely_shared_xlsx() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
    sheet.get_cell_mut("A1").set_value_number(10);
    sheet.get_cell_mut("A2").set_value_number(20);
    sheet.get_cell_mut("A3").set_value_number(30);
    sheet.get_cell_mut("C1").set_value_number(1);
    sheet.get_cell_mut("B1").set_formula("A1+$C$1");
    sheet.get_cell_mut("B2").set_formula("A2+$C$1");
    sheet.get_cell_mut("B3").set_formula("A3+$C$1");
    sheet.get_cell_mut("E4").set_value_number(1);
    sheet.get_cell_mut("F4").set_value_number(2);
    sheet.get_cell_mut("G4").set_value_number(3);
    sheet.get_cell_mut("E5").set_formula("$E4+E$4+$E$4");
    sheet.get_cell_mut("F5").set_formula("$E4+F$4+$E$4");
    sheet.get_cell_mut("G5").set_formula("$E4+G$4+$E$4");
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
            let xml = String::from_utf8(bytes).unwrap();
            let xml = xml
                .replace(
                    "<f>A1+$C$1</f>",
                    "<f t=\"shared\" si=\"9\" ref=\"B1:B3\">A1+$C$1</f>",
                )
                .replace("<f>A2+$C$1</f>", "<f t=\"shared\" si=\"9\"></f>")
                .replace("<f>A3+$C$1</f>", "<f t=\"shared\" si=\"9\"></f>")
                // A lower shared index appears later in XML stream order and
                // expands horizontally with mixed/absolute axes.
                .replace(
                    "<f>$E4+E$4+$E$4</f>",
                    "<f t=\"shared\" si=\"2\" ref=\"E5:G5\">$E4+E$4+$E$4</f>",
                )
                .replace("<f>$E4+F$4+$E$4</f>", "<f t=\"shared\" si=\"2\"></f>")
                .replace("<f>$E4+G$4+$E$4</f>", "<f t=\"shared\" si=\"2\"></f>");
            assert!(xml.contains("t=\"shared\""));
            bytes = xml.into_bytes();
        }
        output.start_file(name, options).unwrap();
        output.write_all(&bytes).unwrap();
    }
    output.finish().unwrap().into_inner()
}

fn large_shared_vertical_xlsx(rows: u32, anchor_formula: &str) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
    for row in 1..=rows {
        sheet.get_cell_mut((1, row)).set_value_number(row as f64);
        sheet
            .get_cell_mut((2, row))
            .set_formula(format!("A{row}+1"));
    }
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |mut xml| {
        xml = xml.replace(
            "<f>A1+1</f>",
            &format!("<f t=\"shared\" si=\"12\" ref=\"B1:B{rows}\">{anchor_formula}</f>"),
        );
        for row in 2..=rows {
            xml = xml.replace(
                &format!("<f>A{row}+1</f>"),
                "<f t=\"shared\" si=\"12\"></f>",
            );
        }
        xml
    })
}

fn two_shared_sheets_xlsx() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    book.new_sheet("Other").unwrap();
    for name in ["Sheet1", "Other"] {
        let sheet = book.get_sheet_by_name_mut(name).unwrap();
        sheet.get_cell_mut("A1").set_value_number(1);
        sheet.get_cell_mut("A2").set_value_number(2);
        sheet.get_cell_mut("B1").set_formula("A1+1");
        sheet.get_cell_mut("B2").set_formula("A2+1");
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
        if name.starts_with("xl/worksheets/sheet") && name.ends_with(".xml") {
            let xml = String::from_utf8(bytes)
                .unwrap()
                .replace(
                    "<f>A1+1</f>",
                    "<f t=\"shared\" si=\"1\" ref=\"B1:B2\">A1+1</f>",
                )
                .replace("<f>A2+1</f>", "<f t=\"shared\" si=\"1\"></f>");
            bytes = xml.into_bytes();
        }
        output.start_file(name, options).unwrap();
        output.write_all(&bytes).unwrap();
    }
    output.finish().unwrap().into_inner()
}

fn column_letters(mut col1: u32) -> String {
    let mut out = String::new();
    while col1 != 0 {
        col1 -= 1;
        out.push((b'A' + (col1 % 26) as u8) as char);
        col1 /= 26;
    }
    out.chars().rev().collect()
}

fn constant_shared_shape_xlsx(cells: &[&str], declared_ref: &str) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
    for cell in cells {
        sheet.get_cell_mut(*cell).set_formula("$A$1000+1");
    }
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |xml| {
        let anchor = format!("<f t=\"shared\" si=\"33\" ref=\"{declared_ref}\">$A$1000+1</f>");
        xml.replacen("<f>$A$1000+1</f>", &anchor, 1)
            .replace("<f>$A$1000+1</f>", "<f t=\"shared\" si=\"33\"></f>")
    })
}

fn rewrite_sheet_xml(original: Vec<u8>, rewrite: impl FnOnce(String) -> String) -> Vec<u8> {
    let mut input = ZipArchive::new(Cursor::new(original)).unwrap();
    let mut output = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut rewrite = Some(rewrite);
    for index in 0..input.len() {
        let mut entry = input.by_index(index).unwrap();
        let name = entry.name().to_string();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        if name == "xl/worksheets/sheet1.xml" {
            bytes = rewrite.take().unwrap()(String::from_utf8(bytes).unwrap()).into_bytes();
        }
        output.start_file(name, options).unwrap();
        output.write_all(&bytes).unwrap();
    }
    output.finish().unwrap().into_inner()
}

fn malformed_shared_family_xlsx() -> Vec<u8> {
    rewrite_sheet_xml(genuinely_shared_xlsx(), |xml| {
        xml.replace(
            "<f t=\"shared\" si=\"9\" ref=\"B1:B3\">A1+$C$1</f>",
            "<f t=\"shared\" si=\"9\" ref=\"B1:B3\">1+</f>",
        )
    })
}

fn forward_derived_before_anchor_xlsx() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
    for (row, value) in [(1, 10), (2, 20), (3, 30)] {
        sheet.get_cell_mut((1, row)).set_value_number(value);
        sheet
            .get_cell_mut((2, row))
            .set_formula(format!("A{row}+$C$1"));
    }
    sheet.get_cell_mut("C1").set_value_number(1);
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |xml| {
        let xml = xml
            .replace("<f>A1+$C$1</f>", "<f t=\"shared\" si=\"4\"></f>")
            .replace(
                "<f>A2+$C$1</f>",
                "<f t=\"shared\" si=\"4\" ref=\"B1:B3\">A2+$C$1</f>",
            )
            .replace("<f>A3+$C$1</f>", "<f t=\"shared\" si=\"4\"></f>");
        assert!(xml.find("si=\"4\"></f>").unwrap() < xml.find("ref=\"B1:B3\"").unwrap());
        xml
    })
}

fn understated_formula_dimensions_xlsx() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
    sheet.get_cell_mut("A1").set_value_number(7);
    sheet.get_cell_mut("J20").set_formula("A1*3");
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |xml| {
        let xml = xml.replace("<dimension ref=\"A1:J20\"/>", "<dimension ref=\"A1:A1\"/>");
        assert!(xml.contains("<c r=\"J20\"><f>A1*3</f><v/></c>"));
        xml
    })
}

fn formula_roles_xlsx() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
    sheet.get_cell_mut("A1").set_formula("1+1");
    sheet.get_cell_mut("B1").set_formula("2+2");
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |xml| {
        xml.replace("<f>1+1</f>", "<f t=\"array\" ref=\"A1:A1\">1+1</f>")
            .replace("<f>2+2</f>", "<f t=\"dataTable\" ref=\"B1:B1\">2+2</f>")
    })
}

fn duplicate_formula_literal_xlsx(formula_first: bool) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    book.get_sheet_by_name_mut("Sheet1")
        .unwrap()
        .get_cell_mut("A1")
        .set_formula("1+1");
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |xml| {
        let formula = "<c r=\"A1\"><f>1+1</f><v/></c>";
        let duplicate = "<c r=\"A1\"><v>99</v></c>";
        let replacement = if formula_first {
            format!("{formula}{duplicate}")
        } else {
            format!("{duplicate}{formula}")
        };
        xml.replace(formula, &replacement)
    })
}

fn cached_malformed_formula_xlsx() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    book.get_sheet_by_name_mut("Sheet1")
        .unwrap()
        .get_cell_mut("A1")
        .set_formula("1+1");
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |xml| {
        let xml = xml.replace("<f>1+1</f><v/>", "<f>1+</f><v>99</v>");
        assert!(xml.contains("<f>1+</f><v>99</v>"));
        xml
    })
}

fn malformed_shared_attribute_xlsx(attribute: &str) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    book.get_sheet_by_name_mut("Sheet1")
        .unwrap()
        .get_cell_mut("A1")
        .set_formula("1+1");
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    rewrite_sheet_xml(original, |xml| {
        xml.replace(
            "<f>1+1</f>",
            &format!("<f t=\"shared\" {attribute}>1+1</f>"),
        )
    })
}

fn assert_shared_load(mut adapter: CalamineAdapter) {
    let mut engine = Engine::new(
        formualizer_eval::test_workbook::TestWorkbook::new(),
        EvalConfig::default(),
    );
    adapter.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();
    for (row, expected) in [(1, 11.0), (2, 21.0), (3, 31.0)] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2).unwrap(),
            LiteralValue::Number(expected)
        );
    }
    for (col, expected) in [(5, 3.0), (6, 4.0), (7, 5.0)] {
        assert_eq!(
            engine.get_cell_value("Sheet1", 5, col).unwrap(),
            LiteralValue::Number(expected)
        );
    }
    let stats = adapter.load_stats().unwrap();
    assert_eq!(stats.formula_cells_observed, Some(6));
    assert_eq!(stats.formula_cells_handed_to_engine, Some(6));
    assert_eq!(stats.shared_formula_tags_observed, Some(6));
}

#[test]
fn cached_formula_values_remain_suppressed_when_parse_policy_keeps_cached_value() {
    for deferred in [false, true] {
        let config = EvalConfig {
            formula_parse_policy: FormulaParsePolicy::KeepCachedValue,
            defer_graph_building: deferred,
            ..EvalConfig::default()
        };
        let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
        let mut adapter = CalamineAdapter::open_bytes(cached_malformed_formula_xlsx()).unwrap();
        adapter.stream_into_engine(&mut engine).unwrap();
        if deferred {
            engine.build_graph_all().unwrap();
        }
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            None,
            "deferred={deferred}"
        );
        let stats = adapter.load_stats().unwrap();
        assert_eq!(stats.formula_cells_observed, Some(1));
        assert_eq!(stats.value_cells_observed, Some(0));
    }
}

#[test]
fn duplicate_formula_literal_ordering_keeps_formula_authority() {
    for formula_first in [true, false] {
        let mut adapter =
            CalamineAdapter::open_bytes(duplicate_formula_literal_xlsx(formula_first)).unwrap();
        let mut engine = Engine::new(
            formualizer_eval::test_workbook::TestWorkbook::new(),
            EvalConfig::default(),
        );
        adapter.stream_into_engine(&mut engine).unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(2.0)),
            "formula_first={formula_first}"
        );
        let stats = adapter.load_stats().unwrap();
        assert_eq!(stats.formula_cells_observed, Some(1));
        assert_eq!(stats.value_cells_observed, Some(1));
    }
}

#[test]
fn shared_formula_stream_expands_relative_and_absolute_refs_from_bytes() {
    assert_shared_load(CalamineAdapter::open_bytes(genuinely_shared_xlsx()).unwrap());
}

#[test]
fn shared_formula_stream_works_for_file_path() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), genuinely_shared_xlsx()).unwrap();
    assert_shared_load(CalamineAdapter::open_path(file.path()).unwrap());
}

#[test]
fn shared_formula_outputs_match_across_formula_plane_modes_and_deferred_ingest() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut eager_report: Option<FormulaIngestReport> = None;
        for deferred in [false, true] {
            let config = EvalConfig {
                formula_plane_mode: mode,
                defer_graph_building: deferred,
                ..EvalConfig::default()
            };
            let mut engine =
                Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
            let mut adapter = CalamineAdapter::open_bytes(genuinely_shared_xlsx()).unwrap();
            adapter.stream_into_engine(&mut engine).unwrap();
            if deferred {
                engine.build_graph_all().unwrap();
            }
            engine.evaluate_all().unwrap();
            assert_eq!(
                engine.get_cell_value("Sheet1", 3, 2).unwrap(),
                LiteralValue::Number(31.0),
                "mode={mode:?}, deferred={deferred}"
            );
            assert_eq!(
                engine.get_cell_value("Sheet1", 5, 7).unwrap(),
                LiteralValue::Number(5.0),
                "mode={mode:?}, deferred={deferred}"
            );
            let report = engine.last_formula_ingest_report().unwrap().clone();
            assert_eq!(report.source_formula_events, 6);
            assert_eq!(report.source_shared_anchor_events, 2);
            assert_eq!(report.source_shared_descendant_events, 4);
            assert_eq!(report.source_family_promoted, 0);
            assert_eq!(report.source_family_fallback, 2);
            assert_eq!(report.source_family_fallback_cells, 6);
            assert!(
                !report
                    .fallback_reasons
                    .contains_key("CompressedEvidenceReplayOnly")
            );
            if deferred {
                let eager = eager_report.as_ref().unwrap();
                assert_eq!(report.source_formula_events, eager.source_formula_events);
                assert_eq!(
                    report.source_family_fallback_cells,
                    eager.source_family_fallback_cells
                );
                assert_eq!(
                    report.source_family_promoted_cells,
                    eager.source_family_promoted_cells
                );
            } else {
                eager_report = Some(report);
            }
            let stats = adapter.load_stats().unwrap();
            assert_eq!(stats.formula_cells_observed, Some(6));
            assert_eq!(stats.formula_cells_handed_to_engine, Some(6));
        }
    }
}

#[test]
fn authoritative_eager_commits_row_col_and_rect_domains_directly() {
    let horizontal: Vec<String> = (2..=101)
        .map(column_letters)
        .map(|col| format!("{col}1"))
        .collect();
    let rect: Vec<String> = (1..=10)
        .flat_map(|row| (2..=11).map(move |col| format!("{}{}", column_letters(col), row)))
        .collect();
    for (cells, declared_ref) in [(horizontal, "B1:CW1"), (rect, "B1:K10")] {
        let refs: Vec<&str> = cells.iter().map(String::as_str).collect();
        let config = EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
        let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
        let mut adapter =
            CalamineAdapter::open_bytes(constant_shared_shape_xlsx(&refs, declared_ref)).unwrap();
        adapter.stream_into_engine(&mut engine).unwrap();
        let report = engine.last_formula_ingest_report().unwrap();
        assert_eq!(
            report.source_family_promoted, 1,
            "{declared_ref}: {report:?}"
        );
        assert_eq!(report.source_family_promoted_cells, 100);
        assert_eq!(report.graph_formula_cells_materialized, 0);
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    }
}

#[test]
fn authoritative_eager_commits_clean_family_without_descendant_graph_materialization() {
    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
    let mut adapter = CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, "A1+1")).unwrap();
    adapter.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_ingest_report().unwrap();
    assert_eq!(report.formula_cells_seen, 100);
    assert_eq!(report.graph_formula_cells_materialized, 0);
    assert_eq!(report.source_family_promoted, 1);
    assert_eq!(report.source_family_promoted_cells, 100);
    assert_eq!(report.source_formula_records_spooled, 100);
    assert!(report.source_spool_encoded_bytes > 100);
    assert!(report.source_spool_peak_memory_bytes > 0);
    assert_eq!(report.source_spool_spilled_bytes, 0);
    assert_eq!(report.source_spool_replays, 0);
    assert_eq!(report.source_family_fallback, 0);
    assert_eq!(report.source_anchor_parses, 1);
    assert_eq!(report.source_anchor_asts, 1);
    assert_eq!(report.source_anchor_analyses, 1);
    assert_eq!(report.source_descendant_strings_avoided, 99);
    assert_eq!(report.source_descendant_events_avoided, 99);
    assert_eq!(report.source_descendant_analyses_avoided, 99);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Number(101.0))
    );
    let stats = adapter.load_stats().unwrap();
    assert_eq!(stats.formula_cells_observed, Some(100));
    assert_eq!(stats.formula_cells_handed_to_engine, Some(100));
}

#[test]
fn authoritative_deferred_package_builds_direct_without_descendant_staging() {
    let config = EvalConfig {
        formula_plane_mode: FormulaPlaneMode::AuthoritativeExperimental,
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
    let mut adapter = CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, "A1+1")).unwrap();
    adapter.stream_into_engine(&mut engine).unwrap();

    assert_eq!(engine.staged_formula_count(), 100);
    assert_eq!(
        engine.get_staged_formula_text("Sheet1", 50, 2).as_deref(),
        Some("A50+1")
    );
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.build_graph_all().unwrap();

    let report = engine.last_formula_ingest_report().unwrap();
    assert_eq!(report.source_family_promoted, 1);
    assert_eq!(report.source_family_promoted_cells, 100);
    assert_eq!(report.source_descendant_strings_avoided, 99);
    assert_eq!(report.graph_formula_cells_materialized, 0);
    assert!(!engine.has_staged_formulas());
}

#[test]
fn deferred_all_and_selected_builds_are_differentially_identical() {
    fn loaded() -> Engine<formualizer_eval::test_workbook::TestWorkbook> {
        let config = EvalConfig {
            formula_plane_mode: FormulaPlaneMode::AuthoritativeExperimental,
            defer_graph_building: true,
            ..EvalConfig::default()
        };
        let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
        let mut adapter = CalamineAdapter::open_bytes(two_shared_sheets_xlsx()).unwrap();
        adapter.stream_into_engine(&mut engine).unwrap();
        engine
    }

    let mut all = loaded();
    let mut selected = loaded();
    all.build_graph_all().unwrap();
    selected.build_graph_for_sheets(["Sheet1"]).unwrap();
    assert!(selected.has_staged_formulas());
    selected.build_graph_for_sheets(["Other"]).unwrap();
    assert!(!selected.has_staged_formulas());

    all.evaluate_all().unwrap();
    selected.evaluate_all().unwrap();
    for sheet in ["Sheet1", "Other"] {
        for row in 1..=2 {
            for col in 1..=2 {
                assert_eq!(
                    selected.get_cell_value(sheet, row, col),
                    all.get_cell_value(sheet, row, col),
                    "{sheet}!R{row}C{col}"
                );
            }
        }
    }
    assert_eq!(
        selected.formula_ingest_report_total(),
        all.formula_ingest_report_total()
    );
    assert_eq!(
        selected.baseline_stats().formula_plane_active_span_count,
        all.baseline_stats().formula_plane_active_span_count
    );
}

#[test]
fn deferred_structural_edits_materialize_before_shift_and_match_eager_coordinates() {
    fn loaded(deferred: bool) -> Engine<formualizer_eval::test_workbook::TestWorkbook> {
        let config = EvalConfig {
            formula_plane_mode: FormulaPlaneMode::AuthoritativeExperimental,
            defer_graph_building: deferred,
            ..EvalConfig::default()
        };
        let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
        let mut adapter =
            CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, "A1+1")).unwrap();
        adapter.stream_into_engine(&mut engine).unwrap();
        engine
    }
    fn snapshot(
        engine: &Engine<formualizer_eval::test_workbook::TestWorkbook>,
    ) -> Vec<Option<LiteralValue>> {
        (1..=105)
            .flat_map(|row| (1..=4).map(move |col| engine.get_cell_value("Sheet1", row, col)))
            .collect()
    }

    for operation in 0..4 {
        let mut eager = loaded(false);
        let mut deferred = loaded(true);
        match operation {
            0 => {
                eager.insert_rows("Sheet1", 50, 2).unwrap();
                deferred.insert_rows("Sheet1", 50, 2).unwrap();
            }
            1 => {
                eager.delete_rows("Sheet1", 50, 2).unwrap();
                deferred.delete_rows("Sheet1", 50, 2).unwrap();
            }
            2 => {
                eager.insert_columns("Sheet1", 2, 1).unwrap();
                deferred.insert_columns("Sheet1", 2, 1).unwrap();
            }
            _ => {
                eager.delete_columns("Sheet1", 1, 1).unwrap();
                deferred.delete_columns("Sheet1", 1, 1).unwrap();
            }
        }
        eager.evaluate_all().unwrap();
        deferred.evaluate_all().unwrap();
        assert_eq!(
            snapshot(&deferred),
            snapshot(&eager),
            "operation {operation}"
        );
        assert!(!deferred.has_staged_formulas());
    }
}

#[test]
fn deferred_package_rename_moves_identity_and_remove_drops_it() {
    let config = EvalConfig {
        formula_plane_mode: FormulaPlaneMode::AuthoritativeExperimental,
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
    let mut adapter = CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, "A1+1")).unwrap();
    adapter.stream_into_engine(&mut engine).unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    engine.rename_sheet(sheet_id, "Renamed").unwrap();
    assert_eq!(
        engine.get_staged_formula_text("Renamed", 75, 2).as_deref(),
        Some("A75+1")
    );
    assert!(engine.get_staged_formula_text("Sheet1", 75, 2).is_none());
    engine.add_sheet("Keep").unwrap();
    engine.remove_sheet(sheet_id).unwrap();
    assert!(!engine.has_staged_formulas());
}

#[test]
fn deferred_selected_build_isolates_packages_and_replacement_invalidates_family() {
    let config = EvalConfig {
        formula_plane_mode: FormulaPlaneMode::AuthoritativeExperimental,
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
    let mut adapter = CalamineAdapter::open_bytes(two_shared_sheets_xlsx()).unwrap();
    adapter.stream_into_engine(&mut engine).unwrap();

    engine.build_graph_for_sheets(["Sheet1"]).unwrap();
    assert_eq!(
        engine.get_staged_formula_text("Other", 2, 2).as_deref(),
        Some("A2+1")
    );
    assert_eq!(engine.staged_formula_count(), 2);

    engine.stage_formula_text("Other", 2, 2, "=A2+40".to_string());
    engine.build_graph_all().unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Other", 2, 2),
        Some(LiteralValue::Number(42.0))
    );
    assert!(!engine.has_staged_formulas());
}

#[test]
fn compressed_shadow_prepares_one_anchor_and_replays_every_cell_eager_and_deferred() {
    let mut eager = None;
    for deferred in [false, true] {
        let config = EvalConfig {
            formula_plane_mode: FormulaPlaneMode::Shadow,
            defer_graph_building: deferred,
            ..EvalConfig::default()
        };
        let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
        let mut adapter =
            CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, "A1+1")).unwrap();
        adapter.stream_into_engine(&mut engine).unwrap();
        if deferred {
            engine.build_graph_all().unwrap();
        }
        let report = engine.last_formula_ingest_report().unwrap().clone();
        assert_eq!(report.source_anchor_parses, 1);
        assert_eq!(report.source_anchor_asts, 1);
        assert_eq!(report.source_anchor_analyses, 1);
        assert_eq!(report.source_descendant_strings_avoided, 99);
        assert_eq!(report.source_descendant_events_avoided, 99);
        assert_eq!(report.source_descendant_analyses_avoided, 99);
        assert_eq!(report.source_compressed_families_prepared, 1);
        assert_eq!(report.source_compressed_cells_prepared, 100);
        assert_eq!(report.graph_formula_cells_materialized, 100);
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
        if deferred {
            assert_eq!(eager.as_ref(), Some(&report));
        } else {
            eager = Some(report);
        }
    }
}

#[test]
fn calamine_expansion_matches_anchor_relocation_ast_at_domain_corners() {
    let mut config = WorkbookConfig::ephemeral();
    config.eval.formula_plane_mode = FormulaPlaneMode::Shadow;
    let (workbook, _) = Workbook::from_reader_with_adapter_stats(
        CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, "$A1+A$1+$A$1+1")).unwrap(),
        LoadStrategy::EagerAll,
        config,
    )
    .unwrap();

    for row in [1, 2, 50, 100] {
        let expanded = workbook.get_formula("Sheet1", row, 2).unwrap();
        let expanded = if expanded.starts_with('=') {
            expanded
        } else {
            format!("={expanded}")
        };
        let expected = format!("=$A{row}+A$1+$A$1+1");
        assert_eq!(
            formualizer_parse::parser::parse(&expanded)
                .unwrap()
                .fingerprint(),
            formualizer_parse::parser::parse(&expected)
                .unwrap()
                .fingerprint(),
            "row={row}, canonical={expanded}"
        );
    }
}

#[test]
fn injected_calamine_arena_relocation_mismatch_replays_complete_shadow_family() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let comparisons = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&comparisons);
    let mut adapter =
        CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, "ABS(A1)+1")).unwrap();
    adapter.set_shadow_relocation_comparator_for_test(move |expanded, relocated| {
        assert_eq!(expanded.fingerprint(), relocated.fingerprint());
        observed.fetch_add(1, Ordering::Relaxed) != 49
    });

    let mut engine = Engine::new(
        formualizer_eval::test_workbook::TestWorkbook::new(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow),
    );
    adapter.stream_into_engine(&mut engine).unwrap();

    assert_eq!(comparisons.load(Ordering::Relaxed), 100);
    let report = engine.last_formula_ingest_report().unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 0, "{report:?}");
    assert_eq!(report.source_compressed_families_prepared, 0, "{report:?}");
    assert_eq!(report.source_compressed_cells_prepared, 0, "{report:?}");
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.graph_formula_cells_materialized, 100, "{report:?}");
    assert_eq!(report.source_spool_replays, 1, "{report:?}");
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 100);

    engine.evaluate_all().unwrap();
    for row in 1..=100 {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(row as f64 + 1.0)),
            "fallback row {row}"
        );
        let fallback_ast = engine.get_cell("Sheet1", row, 2).unwrap().0.unwrap();
        assert_eq!(
            fallback_ast.fingerprint(),
            formualizer_parse::parser::parse(&format!("=ABS(A{row})+1"))
                .unwrap()
                .fingerprint(),
            "fallback formula order at row {row}"
        );
    }
}

#[test]
fn compressed_shadow_accepts_nested_registry_functions_without_authoritative_source_promotion() {
    let fixture = || large_shared_vertical_xlsx(100, "SUM('Sheet1'!A1,'Sheet1'!$A1)+_xlfn.ABS(A1)");
    let mut shadow = Engine::new(
        formualizer_eval::test_workbook::TestWorkbook::new(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow),
    );
    CalamineAdapter::open_bytes(fixture())
        .unwrap()
        .stream_into_engine(&mut shadow)
        .unwrap();
    let report = shadow.last_formula_ingest_report().unwrap();
    assert_eq!(report.source_compressed_families_prepared, 1, "{report:?}");
    assert_eq!(shadow.baseline_stats().formula_plane_active_span_count, 0);

    let mut authoritative = Engine::new(
        formualizer_eval::test_workbook::TestWorkbook::new(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    );
    CalamineAdapter::open_bytes(fixture())
        .unwrap()
        .stream_into_engine(&mut authoritative)
        .unwrap();
    let report = authoritative.last_formula_ingest_report().unwrap();
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(
        report.fallback_reasons.get("UnsupportedAnchorSyntax"),
        Some(&1),
        "{report:?}"
    );
}

#[test]
fn compressed_shadow_rejects_unsupported_syntax_and_boundary_overflow() {
    for (formula, reason) in [
        ("RAND()+A1", "AnchorFunctionSemanticsUnsupported"),
        ("A1048576+1", "UnsupportedAnchorReference"),
    ] {
        let config = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
        let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
        let mut adapter =
            CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, formula)).unwrap();
        adapter.stream_into_engine(&mut engine).unwrap();
        let report = engine.last_formula_ingest_report().unwrap();
        assert_eq!(report.source_anchor_parses, 1);
        assert_eq!(report.source_compressed_families_prepared, 0);
        assert_eq!(report.fallback_reasons.get(reason), Some(&1), "{report:?}");
        assert_eq!(report.graph_formula_cells_materialized, 100);
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    }
}

#[test]
fn malformed_eligible_family_reconciles_without_partial_authority_eager_and_deferred() {
    for policy in [
        FormulaParsePolicy::KeepCachedValue,
        FormulaParsePolicy::AsText,
        FormulaParsePolicy::CoerceToError,
    ] {
        for deferred in [false, true] {
            let config = EvalConfig::default()
                .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
                .with_formula_parse_policy(policy);
            let config = EvalConfig {
                defer_graph_building: deferred,
                ..config
            };
            let mut engine =
                Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
            let mut adapter = CalamineAdapter::open_bytes(malformed_shared_family_xlsx()).unwrap();
            adapter.stream_into_engine(&mut engine).unwrap();
            if deferred {
                engine.build_graph_all().unwrap();
            }
            let report = engine.last_formula_ingest_report().unwrap();
            assert!(
                report.fallback_reasons.contains_key("AnchorParseRejected") || deferred,
                "{report:?}"
            );
            assert_eq!(report.source_family_fallback_cells, 6, "{report:?}");
            assert_eq!(report.source_family_promoted, 0);
        }
    }
}

#[test]
fn strict_parse_policy_preserves_malformed_formula_error() {
    let config = EvalConfig::default().with_formula_parse_policy(FormulaParsePolicy::Strict);
    let mut engine = Engine::new(formualizer_eval::test_workbook::TestWorkbook::new(), config);
    let mut adapter = CalamineAdapter::open_bytes(malformed_shared_family_xlsx()).unwrap();
    let error = adapter.stream_into_engine(&mut engine).unwrap_err();
    assert!(error.to_string().contains("Formula parse error"), "{error}");
}

#[test]
fn shared_formula_stream_buffers_forward_derived_coordinates() {
    let mut adapter = CalamineAdapter::open_bytes(forward_derived_before_anchor_xlsx()).unwrap();
    let mut engine = Engine::new(
        formualizer_eval::test_workbook::TestWorkbook::new(),
        EvalConfig::default(),
    );
    adapter.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();
    for (row, expected) in [(1, 11.0), (2, 21.0), (3, 31.0)] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2).unwrap(),
            LiteralValue::Number(expected)
        );
    }
    let stats = adapter.load_stats().unwrap();
    assert_eq!(stats.formula_cells_observed, Some(3));
    assert_eq!(stats.formula_cells_handed_to_engine, Some(3));
    assert_eq!(stats.shared_formula_tags_observed, Some(3));
}

fn assert_understated_formula_dimensions(adapter: CalamineAdapter) {
    let (mut workbook, stats) = Workbook::from_reader_with_adapter_stats(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral(),
    )
    .unwrap();
    assert_eq!(workbook.sheet_dimensions("Sheet1"), Some((20, 10)));
    let arrow_sheet = workbook.engine().sheet_store().sheet("Sheet1").unwrap();
    assert_eq!(arrow_sheet.nrows, 20);
    assert_eq!(arrow_sheet.columns.len(), 10);

    workbook.evaluate_all().unwrap();
    assert_eq!(
        workbook.get_value("Sheet1", 20, 10),
        Some(LiteralValue::Number(21.0))
    );
    let range = RangeAddress::new("Sheet1", 20, 9, 20, 10).unwrap();
    assert_eq!(
        workbook.read_range(&range),
        vec![vec![LiteralValue::Empty, LiteralValue::Number(21.0)]]
    );

    let stats = stats.unwrap();
    assert_eq!(stats.value_cells_observed, Some(1));
    assert_eq!(stats.value_slots_handed_to_engine, Some(1));
    assert_eq!(stats.formula_cells_observed, Some(1));
    assert_eq!(stats.formula_cells_handed_to_engine, Some(1));
}

#[test]
fn formula_only_record_expands_arrow_dimensions_from_bytes() {
    assert_understated_formula_dimensions(
        CalamineAdapter::open_bytes(understated_formula_dimensions_xlsx()).unwrap(),
    );
}

#[test]
fn formula_only_record_expands_arrow_dimensions_from_file() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), understated_formula_dimensions_xlsx()).unwrap();
    assert_understated_formula_dimensions(CalamineAdapter::open_path(file.path()).unwrap());
}

#[test]
fn calamine_classifies_array_and_data_table_tags_as_normal() {
    use calamine::{Xlsx, XlsxFormulaMetadata, open_workbook_from_rs};

    let mut workbook: Xlsx<_> =
        open_workbook_from_rs(Cursor::new(formula_roles_xlsx())).expect("open role fixture");
    let mut reader = workbook
        .worksheet_cells_reader("Sheet1")
        .expect("open worksheet stream");
    let mut formulas = Vec::new();
    while let Some(record) = reader.next_cell_with_formula_metadata().unwrap() {
        if let Some(formula) = record.formula {
            formulas.push(formula);
        }
    }
    assert_eq!(
        formulas,
        vec![
            XlsxFormulaMetadata::Normal {
                formula: "1+1".to_string(),
            },
            XlsxFormulaMetadata::Normal {
                formula: "2+2".to_string(),
            },
        ]
    );
}

#[test]
fn malformed_shared_si_and_ref_are_rejected_by_calamine_before_source_seam() {
    use calamine::{Xlsx, open_workbook_from_rs};

    for (attribute, expected) in [
        (
            "si=\"not-a-number\" ref=\"A1:A1\"",
            "si attribute must be a number",
        ),
        ("si=\"1\" ref=\"not-a-range\"", "Expecting alphanumeric"),
    ] {
        let mut workbook: Xlsx<_> =
            open_workbook_from_rs(Cursor::new(malformed_shared_attribute_xlsx(attribute)))
                .expect("workbook container remains valid");
        let mut reader = workbook
            .worksheet_cells_reader("Sheet1")
            .expect("open worksheet stream");
        let error = reader
            .next_cell_with_formula_metadata()
            .expect_err("Calamine must reject malformed shared metadata");
        assert!(
            error.to_string().contains(expected),
            "unexpected error for {attribute}: {error}"
        );
    }
}

#[test]
fn swatch0_calamine_expansion_matches_ast_relocation_corpus() {
    for (formula, expected_for_row) in [
        ("SUM(A1,$A1,A$1,$A$1)", "SUM(A{row},$A{row},A$1,$A$1)"),
        ("IF(A1=\"A1\",A1,$A1)", "IF(A{row}=\"A1\",A{row},$A{row})"),
        (
            "COUNTIF(A1:A2,\">0\")+A1",
            "COUNTIF(A{row}:A{next},\">0\")+A{row}",
        ),
        (
            "_xlfn.ABS(SUM('Sheet1'!A1,'Sheet1'!$A1))",
            "_xlfn.ABS(SUM('Sheet1'!A{row},'Sheet1'!$A{row}))",
        ),
    ] {
        let (workbook, _) = Workbook::from_reader_with_adapter_stats(
            CalamineAdapter::open_bytes(large_shared_vertical_xlsx(100, formula)).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        )
        .unwrap();
        for row in [1, 2, 50, 100] {
            let expanded = workbook.get_formula("Sheet1", row, 2).unwrap();
            let expanded = if expanded.starts_with('=') {
                expanded
            } else {
                format!("={expanded}")
            };
            let expected = expected_for_row
                .replace("{row}", &row.to_string())
                .replace("{next}", &(row + 1).to_string());
            let expanded_ast = formualizer_parse::parser::parse(&expanded).unwrap();
            assert_eq!(
                expanded_ast.fingerprint(),
                formualizer_parse::parser::parse(format!("={expected}"))
                    .unwrap()
                    .fingerprint(),
                "formula={formula}, row={row}, expanded={expanded}"
            );
            let anchor_ast = formualizer_parse::parser::parse(format!("={formula}")).unwrap();
            let relocated =
                formualizer_eval::formula_plane::structural::relocate_ast_for_template_placement(
                    &anchor_ast,
                    i64::from(row - 1),
                    0,
                )
                .unwrap();
            assert_eq!(
                expanded_ast.fingerprint(),
                relocated.fingerprint(),
                "Calamine/arena relocation mismatch: formula={formula}, row={row}"
            );
        }
    }
}
