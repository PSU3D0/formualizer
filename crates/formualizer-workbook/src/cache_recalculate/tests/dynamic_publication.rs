//! FORM211-C/D/E internals: the geometry plan's binding requests,
//! cancellation after evaluation and the unowned-payload guard. Lifecycle
//! cases run through the public API in
//! `tests/xlsx_source_recalculate_spills.rs`.
use super::super::{
    PhaseClock, XlsxRecalculateOptions, admit_source, apply_patches, evaluate,
    geometry::BindingRequest, ingest_source, plan_spill_publication, publish,
    recalculate_xlsx_bytes,
};
use super::dynamic_admission::{SHEET, TYPES, WB_RELS, edit, pack, package, producer, refused};
use crate::IoError;
use formualizer_common::ExcelErrorKind;
use formualizer_eval::engine::{CancelToken, FormulaParsePolicy};
use std::collections::{BTreeMap, HashSet};
use std::io::{Cursor, Read};

type Parts = BTreeMap<String, String>;

fn sheet_xml(bytes: &[u8]) -> String {
    let mut z = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut s = String::new();
    z.by_name(SHEET).unwrap().read_to_string(&mut s).unwrap();
    s
}

/// The parent `new-vertical` shape: an ordinary formula that spills.
fn new_vertical() -> Parts {
    let rows = concat!(
        "<row r=\"1\" spans=\"1:3\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>3</v></c></row>",
        "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\"><f>_xlfn.SEQUENCE($B$1)</f><v>99</v></c></row>",
        "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
        "<row r=\"10\" spans=\"1:3\"><c r=\"C10\"><f>SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
        "<row r=\"11\" spans=\"1:3\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
    );
    let p = package("A1:C11", rows, "");
    let mut p = edit(
        p,
        WB_RELS,
        "<Relationship Id=\"rId5\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata\" Target=\"metadata.xml\"/>",
        "",
    );
    p.remove("xl/metadata.xml");
    edit(
        p,
        TYPES,
        "<Override PartName=\"/xl/metadata.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"/>",
        "",
    )
}

#[test]
fn new_ordinary_spill_plan_binds_the_anchor() {
    let source = pack(&new_vertical());
    let options = XlsxRecalculateOptions::default();
    let admission = admit_source(&source, &options).unwrap();
    let mut ingested =
        ingest_source(&source, admission, &options, &mut PhaseClock::from_env()).unwrap();
    evaluate(&mut ingested.engine, &options).unwrap();
    let coerced: HashSet<_> = ingested
        .engine
        .formula_parse_diagnostics()
        .iter()
        .filter(|d| d.policy == FormulaParsePolicy::CoerceToError)
        .map(|d| (d.sheet.clone(), d.row, d.col))
        .collect();
    let mut requests = Vec::new();
    let planned = plan_spill_publication(
        &ingested.engine,
        &ingested.sheets,
        &ingested.plans,
        &coerced,
        &options,
        &mut |sheet, request| {
            requests.push((sheet.to_owned(), request));
            Ok(7)
        },
    )
    .unwrap();
    assert_eq!(
        requests,
        vec![(
            "Sheet1".to_owned(),
            BindingRequest {
                row: 2,
                col: 3,
                multi_cell: true
            }
        )]
    );
    // C2, C3, C4, C9, C10; the anchor is one formula.
    assert_eq!((planned.changed, planned.summary.evaluated), (5, 3));
    let patches = planned.patches.into_iter().next().unwrap();
    let xml = apply_patches(&ingested.plans[0].data, patches, usize::MAX).unwrap();
    let xml = String::from_utf8(xml).unwrap();
    // t="array"/ref set, the resolver's cm inserted, rows inserted in order.
    let rows = concat!(
        "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\" cm=\"7\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
        "<row r=\"3\"><c r=\"C3\"><v>2</v></c></row><row r=\"4\"><c r=\"C4\"><v>3</v></c></row>",
        "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>6</v></c></row>",
        "<row r=\"10\" spans=\"1:3\"><c r=\"C10\"><f>SUM(_xlfn.ANCHORARRAY(C2))</f><v>6</v></c></row>",
    );
    assert!(xml.contains(rows), "{xml}");
}

#[test]
fn cancellation_after_evaluation_publishes_nothing() {
    let source = pack(&producer());
    let token = CancelToken::new();
    let options = XlsxRecalculateOptions {
        cancel: Some(token.clone()),
        ..Default::default()
    };
    let admission = admit_source(&source, &options).unwrap();
    let count = admission.formula_count;
    let mut ingested =
        ingest_source(&source, admission, &options, &mut PhaseClock::from_env()).unwrap();
    evaluate(&mut ingested.engine, &options).unwrap();
    token.cancel();
    match publish(
        &source,
        ingested,
        count,
        &options,
        &mut PhaseClock::from_env(),
    ) {
        Err(IoError::Engine(e)) => assert_eq!(e.kind, ExcelErrorKind::Cancelled),
        Err(other) => panic!("expected cancellation, got {other:?}"),
        Ok(_) => panic!("published after cancellation"),
    }
}

#[test]
fn spill_over_an_unowned_empty_payload_is_refused_not_overwritten() {
    // `<v></v>` outside the prior footprint reads as empty, so the engine
    // spills over it; the writer still refuses to replace a source payload
    // that ownership does not cover.
    let p = edit(
        producer(),
        SHEET,
        "<row r=\"9\"",
        "<row r=\"6\"><c r=\"C6\" s=\"1\"><v></v></c></row><row r=\"9\"",
    );
    refused(
        recalculate_xlsx_bytes(&pack(&p), Default::default()),
        "dynamic spill over an unowned source value",
    );
    // A styled empty shell there is not an obstruction and is filled.
    let p = edit(
        producer(),
        SHEET,
        "<row r=\"9\"",
        "<row r=\"6\"><c r=\"C6\" s=\"1\"/></row><row r=\"9\"",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert!(sheet_xml(&out.bytes).contains("<c r=\"C6\" s=\"1\"><v>5</v></c>"));
    let again = recalculate_xlsx_bytes(&out.bytes, Default::default()).unwrap();
    assert_eq!(again.bytes, out.bytes);
}
