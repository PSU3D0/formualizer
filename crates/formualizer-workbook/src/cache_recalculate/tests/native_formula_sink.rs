use super::*;
use crate::cache_recalculate::{
    native_metadata::{SharedStringTable, Styles},
    native_values::{Context, RawCell, ValueSink, dimension_hint},
};
use crate::{CalamineAdapter, SpreadsheetReader, workbook::WBResolver};
use calamine::Reader;
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
    let sheet::Scanned::Done(scan) = sheet::machine::scan_emitting(
        data,
        &XlsxRecalculateOptions::default(),
        sheet::Mode::Indexed,
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
    // This isolated fixture uses only unstyled numeric literals. Corpus raw
    // metadata coverage below uses the complete contextual admission view.
    sheet::machine::scan_emitting(
        &plan.data,
        &options,
        sheet::Mode::Indexed,
        &mut 0,
        &mut 0,
        |event| {
            values.push(
                event,
                RawCell {
                    kind: None,
                    style: None,
                    payload: None,
                },
            )
        },
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
    let FormulaPlane {
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
    UTILITY_CALLS.with(|c| c.set((0, 0)));
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
    assert_eq!(UTILITY_CALLS.with(|c| c.get()), (1, 1));
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
    assert_eq!(changed.text(&plan.cells[0], &plan.data).unwrap(), "@A1");
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

#[test]
fn raw_formula_envelopes_and_cache_type_refusals_match_pinned_reader() {
    use crate::cache_recalculate::{apply_patches, ingest_view};
    let mut cases = Vec::new();
    for (kind, si, reference) in [
        ("shared", "+0", "A1:A2"),
        ("shared", "&#48;", "A1:A2"),
        ("shared", "0", "&#65;1:A2"),
        ("&#115;hared", "0", "A1:A2"),
    ] {
        cases.push(format!("<row r=\"1\"><c r=\"A1\"><f t=\"{kind}\" si=\"{si}\" ref=\"{reference}\">1</f><v>1</v></c></row>\
            <row r=\"2\"><c r=\"A2\"><f t=\"{kind}\" si=\"{si}\"/><v>1</v></c></row>"));
    }
    for (kind, cache) in [
        ("&#110;", "<v>1</v>"),
        ("&#110;", ""),
        ("&#110;", "<v>bad</v>"),
        ("&#98;", "<v>bad</v>"),
    ] {
        cases.push(format!(
            "<row r=\"1\"><c r=\"A1\" t=\"{kind}\"><f>1</f>{cache}</c></row>"
        ));
    }
    for body in cases {
        let data = format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{body}</sheetData></worksheet>");
        let plan = plan(data.as_bytes());
        let view = apply_patches(
            data.as_bytes(),
            ingest_view::patches(&plan, &None).unwrap(),
            1 << 20,
        )
        .unwrap();
        let mut book = calamine::Xlsx::new(Cursor::new(probe(&view))).unwrap();
        let mut reader = book.worksheet_cells_reader("Probe").unwrap();
        let mut old = Vec::new();
        let mut error = None;
        let mut cache_without_metadata = 0;
        loop {
            match reader.next_cell_with_formula_metadata() {
                Ok(Some(record)) => {
                    if let Some(metadata) = record.formula {
                        old.push((
                            SourceCoord {
                                row: record.pos.0,
                                col: record.pos.1,
                            },
                            expected(metadata),
                        ));
                    } else if record.value != calamine::DataRef::Empty {
                        cache_without_metadata += 1;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error = Some(reader_error(e).to_string());
                    break;
                }
            }
        }
        let texts = EvaluationTexts::default();
        let native: Result<Vec<_>, _> = plan
            .cells
            .iter()
            .map(|cell| source_record(&plan, cell, &texts, 0))
            .map(|r| r.map(signature))
            .collect();
        match native {
            Ok(native) => {
                assert!(error.is_none(), "{body}: {error:?}");
                assert_eq!(native, old, "{body}");
            }
            Err(native) => assert_eq!(Some(native.to_string()), error, "{body}"),
        }
        if body.contains("&#115;hared") {
            // Pinned read_formula returns Some("") even for an empty f.
            // Unknown raw shared type therefore becomes an empty ordinary
            // formula, not a cached literal; its cache must remain excluded.
            assert_eq!(cache_without_metadata, 0);
        }
    }
}

#[test]
fn table_evaluation_text_matches_transport_without_editing_source() {
    use crate::cache_recalculate::{apply_patches, package, table_lowering, tables};
    let data = format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData>\
        <row r=\"1\"><c r=\"C1\"><f>SUM(T[Qty])</f><v>0</v></c></row>\
        <row r=\"2\"><c r=\"B2\"><f>T[[#This Row],[Qty]]</f><v>0</v></c></row>\
        </sheetData></worksheet>"
    );
    let mut plan = plan(data.as_bytes());
    let original: Vec<_> = plan.cells.iter().map(|c| c.formula_text.clone()).collect();
    let table = format!(
        "<table xmlns=\"{MAIN}\" id=\"1\" name=\"T\" displayName=\"T\" ref=\"A1:B3\" headerRowCount=\"1\" totalsRowShown=\"0\">\
        <tableColumns count=\"2\"><tableColumn id=\"1\" name=\"Qty\"/><tableColumn id=\"2\" name=\"Cost\"/></tableColumns></table>"
    );
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    zip.start_file("table.xml", SimpleFileOptions::default())
        .unwrap();
    zip.write_all(table.as_bytes()).unwrap();
    let bytes = zip.finish().unwrap().into_inner();
    let mut archive = package::admit(&bytes, &XlsxRecalculateOptions::default()).unwrap();
    let options = XlsxRecalculateOptions::default();
    plan.tables.push(
        tables::parse(
            &mut archive,
            "table.xml",
            &options,
            &mut tables::Budget::default(),
        )
        .unwrap(),
    );
    let mut sheets = vec![package::Sheet {
        name: "R&D".into(),
        part: "sheet.xml".into(),
        inert: false,
        kind: "worksheet",
        sheet_id: 1,
        tables: BTreeMap::new(),
    }];
    let mut plans = vec![plan];
    let plan = &plans[0];
    let changes = table_lowering::lowered_texts("R&D", plan, &sheets, &plans, &options).unwrap();
    assert_eq!(changes.len(), 2);
    let texts = EvaluationTexts::new(changes).unwrap();
    let view = apply_patches(
        &plan.data,
        table_lowering::patches("R&D", plan, &sheets, &plans, &options).unwrap(),
        1 << 20,
    )
    .unwrap();
    let mut book = calamine::Xlsx::new(Cursor::new(probe(&view))).unwrap();
    let mut reader = book.worksheet_cells_reader("Probe").unwrap();
    for cell in &plan.cells {
        let record = reader.next_cell_with_formula_metadata().unwrap().unwrap();
        assert_eq!(
            signature(source_record(plan, cell, &texts, 0).unwrap()).1,
            expected(record.formula.unwrap())
        );
        assert!(!texts.text(cell, &plan.data).unwrap().contains('['));
    }
    assert_eq!(
        plan.cells
            .iter()
            .map(|c| c.formula_text.clone())
            .collect::<Vec<_>>(),
        original
    );
    assert_eq!(plan.data, data.as_bytes());

    // A reference from another sheet must preserve the qualifier's ampersand
    // in direct text exactly as escape -> patched XML -> reader does.
    let cross = format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"C1\"><f>SUM(T[Qty])</f><v>0</v></c></row></sheetData></worksheet>"
    );
    plans.push(self::plan(cross.as_bytes()));
    sheets.push(package::Sheet {
        name: "Other".into(),
        part: "other.xml".into(),
        inert: false,
        kind: "worksheet",
        sheet_id: 2,
        tables: BTreeMap::new(),
    });
    let plan = &plans[1];
    let changes = table_lowering::lowered_texts("Other", plan, &sheets, &plans, &options).unwrap();
    assert_eq!(changes.len(), 1);
    assert!(changes[0].text.contains("R&D"));
    let texts = EvaluationTexts::new(changes).unwrap();
    let view = apply_patches(
        &plan.data,
        table_lowering::patches("Other", plan, &sheets, &plans, &options).unwrap(),
        1 << 20,
    )
    .unwrap();
    let mut book = calamine::Xlsx::new(Cursor::new(probe(&view))).unwrap();
    let record = book
        .worksheet_cells_reader("Probe")
        .unwrap()
        .next_cell_with_formula_metadata()
        .unwrap()
        .unwrap();
    assert_eq!(
        signature(source_record(plan, &plan.cells[0], &texts, 0).unwrap()).1,
        expected(record.formula.unwrap())
    );
}

#[derive(Debug, PartialEq)]
enum Metadata {
    Ordinary(String),
    Anchor(usize, Option<(SourceCoord, SourceCoord)>, String),
    Descendant(usize),
}
fn signature(record: OwnedSpoolFormulaRecord) -> (SourceCoord, Metadata) {
    match record {
        OwnedSpoolFormulaRecord::Ordinary { coord0, text, .. } => {
            (coord0, Metadata::Ordinary(text))
        }
        OwnedSpoolFormulaRecord::SharedAnchor {
            coord0,
            shared_index,
            declared_range,
            text,
            ..
        } => (
            coord0,
            Metadata::Anchor(shared_index, declared_range.map(|r| (r.start, r.end)), text),
        ),
        OwnedSpoolFormulaRecord::SharedDescendant {
            coord0,
            shared_index,
            ..
        } => (coord0, Metadata::Descendant(shared_index)),
        OwnedSpoolFormulaRecord::Unsupported { .. } => panic!("unsupported admitted metadata"),
    }
}
fn expected(record: calamine::XlsxFormulaMetadata) -> Metadata {
    match record {
        calamine::XlsxFormulaMetadata::Normal { formula } => Metadata::Ordinary(formula),
        calamine::XlsxFormulaMetadata::Shared {
            shared_index,
            range,
            formula,
        } => Metadata::Anchor(
            shared_index,
            range.map(|r| {
                (
                    SourceCoord {
                        row: r.start.0,
                        col: r.start.1,
                    },
                    SourceCoord {
                        row: r.end.0,
                        col: r.end.1,
                    },
                )
            }),
            formula,
        ),
        calamine::XlsxFormulaMetadata::SharedDerived { shared_index } => {
            Metadata::Descendant(shared_index)
        }
        _ => panic!("unsupported reader metadata"),
    }
}

#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_native_formula_records_match_contextual_calamine() {
    use crate::cache_recalculate::{
        admit_source, apply_patches, inert_sheets, ingest_view, legacy_intersection, package,
        table_lowering,
    };
    crate::cache_recalculate::tests::scan_differential::exclusive(|| {
        let paths =
            std::fs::read_to_string(std::env::var("FORMUALIZER_SCAN_DIFF_FILES").unwrap()).unwrap();
        let options = XlsxRecalculateOptions::default();
        let (mut files, mut sheets, mut formulas, mut legacy_roots, mut table_roots) =
            (0, 0, 0, 0, 0);
        for path in paths.lines().map(str::trim).filter(|s| !s.is_empty()) {
            let bytes = std::fs::read(path).unwrap();
            let mut admitted = admit_source(&bytes, &options).unwrap();
            let chain = if legacy_intersection::needs_calc_chain(&admitted.plans, &options).unwrap()
            {
                let part = package::calc_chain_part(&mut admitted.archive, &options);
                legacy_intersection::calc_chain(
                    &mut admitted.archive,
                    part.as_deref(),
                    &admitted.sheets,
                    &options,
                )
                .unwrap()
            } else {
                Default::default()
            };
            let names: Vec<_> = admitted
                .plans
                .iter()
                .flat_map(|p| &p.tables)
                .map(|t| t.name.to_lowercase())
                .collect();
            let mut texts = Vec::new();
            let mut edits = package::Edits {
                replace: BTreeMap::new(),
                add: BTreeMap::new(),
            };
            for (i, (sheet, plan)) in admitted.sheets.iter().zip(&admitted.plans).enumerate() {
                let external = admitted.external.as_ref().map(|e| &e.range_names);
                let mut changes = Vec::new();
                let mut patches = ingest_view::patches(plan, &options.cancel).unwrap();
                if !chain.is_empty() {
                    let (legacy, stats) = legacy_intersection::lowered_texts(
                        i, plan, &chain, &names, external, &options,
                    )
                    .unwrap();
                    legacy_roots += stats.rewritten;
                    changes.extend(legacy);
                    patches.extend(
                        legacy_intersection::patches(i, plan, &chain, &names, external, &options)
                            .unwrap()
                            .0,
                    );
                }
                if !names.is_empty() {
                    let table = table_lowering::lowered_texts(
                        &sheet.name,
                        plan,
                        &admitted.sheets,
                        &admitted.plans,
                        &options,
                    )
                    .unwrap();
                    table_roots += table.len();
                    changes.extend(table);
                    patches.extend(
                        table_lowering::patches(
                            &sheet.name,
                            plan,
                            &admitted.sheets,
                            &admitted.plans,
                            &options,
                        )
                        .unwrap(),
                    );
                }
                texts.push(EvaluationTexts::new(changes).unwrap());
                if !patches.is_empty() {
                    edits.replace.insert(
                        sheet.part.clone(),
                        apply_patches(&plan.data, patches, options.limits.max_worksheet_bytes)
                            .unwrap(),
                    );
                }
            }
            inert_sheets::project(
                &mut admitted.archive,
                &admitted.sheets,
                &mut edits,
                &options,
            )
            .unwrap();
            let view = package::rewrite(&bytes, &mut admitted.archive, &edits, &options).unwrap();
            let mut book = calamine::Xlsx::new(Cursor::new(view)).unwrap();
            for ((sheet, plan), texts) in admitted.sheets.iter().zip(&admitted.plans).zip(texts) {
                let mut native = plan
                    .cells
                    .iter()
                    .map(|cell| source_record(plan, cell, &texts, 0).unwrap())
                    .map(signature);
                let mut reader = book.worksheet_cells_reader(&sheet.name).unwrap();
                while let Some(record) = reader.next_cell_with_formula_metadata().unwrap() {
                    if let Some(metadata) = record.formula {
                        let coord = SourceCoord {
                            row: record.pos.0,
                            col: record.pos.1,
                        };
                        assert_eq!(
                            native.next(),
                            Some((coord, expected(metadata))),
                            "{path} / {}",
                            sheet.name
                        );
                        formulas += 1;
                    }
                }
                assert!(
                    native.next().is_none(),
                    "{path} / {}: extra native formula",
                    sheet.name
                );
                sheets += 1;
            }
            files += 1;
        }
        assert!(files > 0);
        eprintln!(
            "native formula records: {files} packages / {sheets} sheets / {formulas} formulas / {legacy_roots} legacy roots / {table_roots} table roots, zero divergences"
        );
    });
}
