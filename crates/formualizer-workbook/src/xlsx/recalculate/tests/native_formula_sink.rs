use super::*;
use crate::xlsx::recalculate::{
    metadata::{SharedStringTable, Styles},
    values::{Context, ValueSink, dimension_hint},
};
use crate::{CalamineAdapter, SpreadsheetReader, workbook::WBResolver};
use formualizer_common::DateSystem;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{
    EvalConfig, FormulaCompressedSourceBatch, FormulaIngestBatch, FormulaParsePolicy,
};
use std::io::{Cursor, Write};
use zip::{ZipWriter, write::SimpleFileOptions};
const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

fn probe(worksheet: &[u8]) -> Vec<u8> {
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    let mut add = |name: &str, data: &[u8]| {
        zip.start_file(name, SimpleFileOptions::default()).unwrap();
        zip.write_all(data).unwrap();
    };
    add("[Content_Types].xml", b"<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"xml\" ContentType=\"application/xml\"/></Types>");
    add("_rels/.rels", b"<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"doc\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>");
    add("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><sheets><sheet name=\"Probe\" sheetId=\"1\" r:id=\"sheet\"/></sheets></workbook>").as_bytes());
    add("xl/_rels/workbook.xml.rels", b"<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"sheet\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/probe.xml\"/></Relationships>");
    add("xl/worksheets/probe.xml", worksheet);
    zip.finish().unwrap().into_inner()
}
fn plan(data: &[u8]) -> SheetPlan {
    let sheet::Scanned::Done(scan) = sheet::scanner::scan_emitting(
        data,
        &XlsxRecalculateOptions::default(),
        sheet::ScanStrategy::Indexed,
        &mut 0,
        &mut 0,
        |_| Ok(()),
    )
    .unwrap() else {
        panic!("indexed scan")
    };
    SheetPlan {
        data: data.to_vec(),
        cells: scan.cells,
        index: scan.index,
        hidden_rows: scan.hidden_rows,
        rows_hidden_by_default: scan.rows_hidden_by_default,
        active_filters: scan.active_filters,
        tables: Vec::new(),
        ownership: Default::default(),
        serialized: scan.serialized,
        bounds: scan.bounds,
    }
}
fn text(engine: &Engine<WBResolver>, row: u32, col: u32) -> Option<String> {
    engine
        .get_staged_formula_text("Probe", row, col)
        .or_else(|| {
            engine
                .get_cell("Probe", row, col)
                .and_then(|(ast, _)| ast.map(|a| formualizer_parse::pretty::canonical_formula(&a)))
        })
}
fn load(data: &[u8], config: EvalConfig, native: bool) -> Engine<WBResolver> {
    let mut engine = Engine::new(WBResolver::default(), config);
    if !native {
        CalamineAdapter::open_bytes(probe(data))
            .unwrap()
            .stream_into_engine(&mut engine)
            .unwrap();
        return engine;
    }
    engine.adopt_file_sheets(["Probe"]).unwrap();
    let plan = plan(data);
    let strings = SharedStringTable::default();
    let styles = Styles::default();
    let options = XlsxRecalculateOptions::default();
    let limits = engine.workbook_load_limits().clone();
    let mut values = ValueSink::new(Context {
        name: "Probe",
        source: &plan.data,
        strings: &strings,
        styles: &styles,
        dimensions: dimension_hint(data, &options),
        chunk_rows: 16384,
        date_system: DateSystem::Excel1900,
        limits: &limits,
    })
    .unwrap();
    sheet::scanner::scan_emitting(
        &plan.data,
        &options,
        sheet::ScanStrategy::Indexed,
        &mut 0,
        &mut 0,
        |event| values.push(event),
    )
    .unwrap();
    for cell in &plan.cells {
        values.observe_formula(cell.row, cell.col).unwrap();
    }
    let formulas = stage(
        &mut engine,
        "Probe",
        0,
        &plan,
        &EvaluationTexts::default(),
        WorkbookSpoolUsage::default(),
        &options,
    )
    .unwrap();
    let arrow = values.finish(plan.bounds).unwrap().arrow;
    let store = engine.sheet_store_mut();
    if let Some(at) = store.sheets.iter().position(|s| s.name.as_ref() == "Probe") {
        store.sheets[at] = arrow;
    } else {
        store.sheets.push(arrow);
    }
    let StagedFormulas {
        records,
        report,
        families,
        partitioned,
        preparation,
        deferred,
        formulas_observed,
        shared_tags,
        spool_bytes,
        spool_spilled,
    } = formulas;
    assert_eq!(formulas_observed, 8);
    assert_eq!(shared_tags, 6);
    assert_eq!(report.source_spool_encoded_bytes, spool_bytes);
    assert_eq!(report.source_spool_spill_files, u64::from(spool_spilled));
    if let Some(package) = deferred {
        engine.source_formula_ingress().stage_deferred(package);
    } else {
        let batch = FormulaIngestBatch::new("Probe".to_owned(), records);
        if let Some(prepared) = preparation {
            engine
                .source_formula_ingress()
                .finish_prepared(vec![(batch, report, prepared)])
                .unwrap();
        } else {
            engine
                .source_formula_ingress()
                .ingest_replay_batches(vec![(
                    batch,
                    FormulaCompressedSourceBatch::with_proposals(
                        "Probe".to_owned(),
                        report,
                        families,
                        partitioned,
                    ),
                )])
                .unwrap();
        }
    }
    engine
}

#[test]
fn permitted_pure_utilities_have_one_counted_native_surface() {
    take_utility_calls();
    let mut replay = FormulaReplayState::default();
    for (sequence, record) in [
        OwnedSpoolFormulaRecord::SharedAnchor {
            sequence: 0,
            coord0: SourceCoord { row: 0, col: 1 },
            shared_index: 0,
            declared_range: None,
            text: "A1+1".into(),
        },
        OwnedSpoolFormulaRecord::SharedDescendant {
            sequence: 1,
            coord0: SourceCoord { row: 1, col: 1 },
            shared_index: 0,
        },
    ]
    .into_iter()
    .enumerate()
    {
        replay
            .emit_record(
                record,
                "Probe",
                |_, _| true,
                |actual, _, _, _| {
                    assert_eq!(actual, sequence as u64);
                    Ok(())
                },
            )
            .unwrap();
    }
    assert!(parse_error_token("#DIV/0!").is_ok());
    assert_eq!(take_utility_calls(), (1, 1));
}

#[test]
fn native_staging_keeps_w1_modes_and_counters() {
    let data = format!(
        "<worksheet xmlns=\"{MAIN}\"><dimension ref=\"A1:D8\"/><sheetData>\
        <row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"B1\"><f t=\"shared\" si=\"0\" ref=\"B1:B3\">A1+1</f><v>11</v></c></row>\
        <row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"B2\"><f t=\"shared\" si=\"0\"/><v>21</v></c></row>\
        <row r=\"3\"><c r=\"A3\"><v>30</v></c><c r=\"B3\"><f t=\"shared\" si=\"0\"/><v>31</v></c></row>\
        <row r=\"4\"><c r=\"C4\"><f t=\"shared\" si=\"1\" ref=\"C4:C6\">a4+1</f><v>1</v></c></row>\
        <row r=\"5\"><c r=\"C5\"><f t=\"shared\" si=\"1\"/><v>1</v></c></row>\
        <row r=\"6\"><c r=\"C6\"><f t=\"shared\" si=\"1\"/><v>1</v></c></row>\
        <row r=\"7\"><c r=\"D7\"><f>1+</f><v>0</v></c></row>\
        <row r=\"8\"><c r=\"D8\"><f>SUM(A1:A3)</f><v>60</v></c></row>\
        </sheetData></worksheet>"
    );
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for deferred in [false, true] {
            let config = EvalConfig {
                formula_plane_mode: mode,
                defer_graph_building: deferred,
                formula_parse_policy: FormulaParsePolicy::CoerceToError,
                ..Default::default()
            };
            let mut old = load(data.as_bytes(), config.clone(), false);
            let mut native = load(data.as_bytes(), config, true);
            old.evaluate_all().unwrap();
            native.evaluate_all().unwrap();
            assert_eq!(
                native.formula_ingest_report_total(),
                old.formula_ingest_report_total(),
                "{mode:?}/{deferred}"
            );
            assert_eq!(
                native.formula_parse_diagnostics(),
                old.formula_parse_diagnostics(),
                "{mode:?}/{deferred}"
            );
            for cell in &plan(data.as_bytes()).cells {
                assert_eq!(
                    text(&native, cell.row, cell.col),
                    text(&old, cell.row, cell.col),
                    "{mode:?}/{deferred} {}",
                    cell.address()
                );
                assert_eq!(
                    native.get_cell_value("Probe", cell.row, cell.col),
                    old.get_cell_value("Probe", cell.row, cell.col)
                );
            }
        }
    }
}

#[test]
fn evaluation_text_preserves_xml10_and_does_not_mutate_marks() {
    let data = format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>\"a\u{85}b\u{2028}c\"</f><v>0</v></c></row></sheetData></worksheet>"
    );
    let plan = plan(data.as_bytes());
    let original = plan.cells[0].formula_text.clone();
    let texts = EvaluationTexts::default();
    let OwnedSpoolFormulaRecord::Ordinary { text, .. } =
        source_record(&plan, &plan.cells[0], &texts, 0).unwrap()
    else {
        panic!("ordinary")
    };
    assert_eq!(text, "\"a\u{85}b\u{2028}c\"");
    assert_eq!(plan.cells[0].formula_text, original);
    let changed = EvaluationTexts::new(vec![FormulaTextTransform {
        span: plan.cells[0].formula_open.end..plan.cells[0].formula_end - 4,
        text: "@A1".into(),
    }])
    .unwrap();
    assert_eq!(
        changed
            .text(&plan.cells[0], &plan.data, &plan.cells[0].formula_text)
            .unwrap(),
        "@A1"
    );
    assert_eq!(plan.cells[0].formula_text, original);
    assert!(
        EvaluationTexts::new(vec![
            FormulaTextTransform {
                span: 1..3,
                text: "x".into()
            },
            FormulaTextTransform {
                span: 1..3,
                text: "x".into()
            }
        ])
        .is_err()
    );
}
