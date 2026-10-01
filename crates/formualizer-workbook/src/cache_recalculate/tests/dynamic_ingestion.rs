//! FORM211-B: transient ingestion view (masked old children, normalized
//! anchors) and declared-anchor identity.
//! Engine state is checked on the ingested and evaluated engine before
//! publication; `dynamic_publication` checks the published output.
use super::super::{
    Ingested, XlsxRecalculateOptions, admit_source, apply_patches, evaluate, ingest_source,
    ingest_view, recalculate_xlsx_bytes,
};
use super::dynamic_admission::{
    MAIN, OFFICE, PRODUCER_ROWS, SHEET, TWO_ANCHOR_ROWS, TYPES, WB_RELS, edit, pack, package,
    producer, producer_wide,
};
use crate::workbook::WBResolver;
use formualizer_common::{CellAddress, ExcelErrorKind, LiteralValue, RangeAddress};
use formualizer_eval::engine::Engine;
use formualizer_eval::engine::inspect::{SnapshotOptions, SpillRole};
use std::collections::BTreeMap;

type Parts = BTreeMap<String, String>;

/// Admit and ingest, then hand the (not yet evaluated) engine to `check`.
fn with_ingested<T>(p: &Parts, check: impl FnOnce(&mut Ingested<'_>) -> T) -> T {
    let bytes = pack(p);
    let options = XlsxRecalculateOptions::default();
    let admission = admit_source(&bytes, &options).expect("admitted");
    let mut ingested = ingest_source(&bytes, admission, &options).expect("ingested");
    check(&mut ingested)
}
/// Admit, ingest and evaluate.
fn with_evaluated<T>(p: &Parts, check: impl FnOnce(&Engine<WBResolver>) -> T) -> T {
    with_ingested(p, |ingested| {
        evaluate(&mut ingested.engine, &XlsxRecalculateOptions::default()).expect("evaluated");
        check(&ingested.engine)
    })
}
/// The first worksheet's transient ingestion view.
fn view(p: &Parts) -> String {
    let bytes = pack(p);
    let options = XlsxRecalculateOptions::default();
    let admission = admit_source(&bytes, &options).expect("admitted");
    let plan = &admission.plans[0];
    let patches = ingest_view::patches(plan, &None).expect("view patches");
    String::from_utf8(apply_patches(&plan.data, patches, usize::MAX).expect("view")).unwrap()
}
fn address(sheet: &str, cell: &str) -> CellAddress {
    let (row, col) = a1(cell);
    CellAddress::new(sheet, row, col).unwrap()
}
fn a1(cell: &str) -> (u32, u32) {
    let (r, c, _, _) = formualizer_common::coord::parse_a1_1based(cell).unwrap();
    (r, c)
}
fn value_at(e: &Engine<WBResolver>, sheet: &str, cell: &str) -> Option<LiteralValue> {
    let (row, col) = a1(cell);
    match e.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) | None => None,
        other => other,
    }
}
fn value(e: &Engine<WBResolver>, cell: &str) -> Option<LiteralValue> {
    value_at(e, "Sheet1", cell)
}
fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}
#[track_caller]
fn assert_err(value: Option<LiteralValue>, kind: ExcelErrorKind, ctx: &str) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, kind, "{ctx}: {e:?}"),
        other => panic!("{ctx}: expected {kind:?}, got {other:?}"),
    }
}
fn formula(e: &Engine<WBResolver>, cell: &str) -> Option<String> {
    e.inspect_cell(&address("Sheet1", cell), &SnapshotOptions::default())
        .unwrap()
        .cell
        .formula
}
fn extent(e: &Engine<WBResolver>, cell: &str) -> Option<RangeAddress> {
    match e
        .inspect_cell_result(&address("Sheet1", cell))
        .unwrap()
        .spill
    {
        Some(SpillRole::Anchor { extent }) => Some(extent),
        _ => None,
    }
}
fn range(first: &str, last: &str) -> RangeAddress {
    let (sr, sc) = a1(first);
    let (er, ec) = a1(last);
    RangeAddress::new("Sheet1", sr, sc, er, ec).unwrap()
}
#[track_caller]
fn assert_column(e: &Engine<WBResolver>, col: &str, rows: std::ops::RangeInclusive<u32>) {
    for (i, row) in rows.enumerate() {
        assert_eq!(
            value(e, &format!("{col}{row}")),
            n(i as f64 + 1.0),
            "{col}{row}"
        );
    }
}

#[test]
fn existing_spill_old_children_are_masked_and_the_anchor_grows() {
    // Cached 1,2,3 at C2:C4 and an independent input B1=5.
    let p = producer();
    let v = view(&p);
    // Old child caches are gone; shells and styles stay.
    assert!(v.contains("<c r=\"C3\" s=\"1\"></c><"), "{v}");
    assert!(v.contains("<c r=\"C4\" s=\"1\"></c><"), "{v}");
    // Anchors replay as ordinary formulas with unchanged text.
    assert!(
        v.contains("<c r=\"C2\" s=\"1\" ><f  >_xlfn.SEQUENCE($B$1)</f><v>1</v></c>"),
        "{v}"
    );
    assert!(
        v.contains("<c r=\"C10\" ><f  >SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c>"),
        "{v}"
    );
    assert!(!v.contains("cm=") && !v.contains("t=\"array\"") && !v.contains("ref=\"C"));
    // Everything else is byte-identical.
    assert!(v.contains("<c r=\"B1\"><v>5</v></c>") && v.contains("<f>SUM(C2#)</f><v>99</v>"));

    with_ingested(&p, |ingested| {
        // The authoritative worksheet bytes are not edited.
        assert_eq!(ingested.plans[0].data, p[SHEET].as_bytes());
        let e = &mut ingested.engine;
        // No value-plane entries from proven old children.
        assert_eq!(value(e, "C3"), None);
        assert_eq!(value(e, "C4"), None);
        assert_eq!(value(e, "B1"), n(5.0));
        // Each anchor is staged once, as a formula; children are not formulas.
        for cell in ["C2", "C9", "C10"] {
            assert!(formula(e, cell).is_some(), "{cell}");
        }
        for cell in ["C3", "C4", "C5"] {
            assert!(formula(e, cell).is_none(), "{cell}");
        }
        evaluate(e, &XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(extent(e, "C2"), Some(range("C2", "C6")));
        assert_column(e, "C", 2..=6);
        assert_eq!(value(e, "C9"), n(15.0), "SUM(C2#)");
        assert_eq!(value(e, "C10"), n(15.0), "SUM(_xlfn.ANCHORARRAY(C2))");
    });
    // The grown spill publishes (FORM211-C/D): C5, C6, C9 and C10.
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(out.cache_cells_changed, 4);
}

#[test]
fn stale_spill_error_caches_are_masked_not_refused() {
    let p = edit(
        producer(),
        SHEET,
        "<c r=\"C3\" s=\"1\"><v>2</v></c>",
        "<c r=\"C3\" s=\"1\" t=\"e\"><v>#SPILL!</v></c>",
    );
    let p = edit(
        p,
        SHEET,
        "<c r=\"C4\" s=\"1\"><v>3</v></c>",
        "<c r=\"C4\" s=\"1\" t=\"b\"><v>2</v></c>",
    );
    // The anchor itself carries a stale `#SPILL!` cache: cache clearing and
    // anchor normalization coalesce on one cell.
    let p = edit(
        p,
        SHEET,
        "<c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c>",
        "<c r=\"C2\" s=\"1\" cm=\"1\" t=\"e\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>#SPILL!</v></c>",
    );
    let v = view(&p);
    assert!(v.contains("<c r=\"C3\" s=\"1\" ></c>"), "{v}");
    assert!(v.contains("<c r=\"C4\" s=\"1\" ></c>"), "{v}");
    assert!(v.contains("<f  >_xlfn.SEQUENCE($B$1)</f>"), "{v}");
    with_evaluated(&p, |e| {
        assert_eq!(extent(e, "C2"), Some(range("C2", "C6")));
        assert_column(e, "C", 2..=6);
        assert_eq!(value(e, "C9"), n(15.0));
    });
}

/// `SEQUENCE($B$1,2)` with prior footprint C2:D5: D3 is a styled shell, C4 a
/// stale cache, C3/D4/C5/D5 are not serialized, D6 is an outside input.
fn sparse(b1: u32) -> Parts {
    let rows = format!(
        "<row r=\"1\"><c r=\"B1\"><v>{b1}</v></c></row>{}",
        concat!(
            "<row r=\"2\"><c r=\"C2\" cm=\"1\"><f t=\"array\" ref=\"C2:D5\">_xlfn.SEQUENCE($B$1,2)</f><v>1</v></c></row>",
            "<row r=\"3\"><c r=\"D3\" s=\"1\"/></row>",
            "<row r=\"4\" spans=\"3:3\"><c r=\"C4\"><v>5</v></c></row>",
            "<row r=\"5\"/>",
            "<row r=\"6\"><c r=\"D6\"><v>9</v></c></row>",
            "<row r=\"7\"><c r=\"B7\"><f>SUM(C2#)</f></c></row>",
        )
    );
    let tail = "<mergeCells count=\"1\"><mergeCell ref=\"E7:F8\"/></mergeCells>";
    package("B1:F8", &rows, tail)
}

#[test]
fn sparse_and_missing_children_are_masked_without_manufacturing_cells() {
    let p = sparse(4);
    let v = view(&p);
    assert!(v.contains("<c r=\"D3\" s=\"1\"/>"), "{v}");
    assert!(
        v.contains("<row r=\"4\" spans=\"3:3\"><c r=\"C4\"></c></row>"),
        "{v}"
    );
    assert!(v.contains("<row r=\"5\"/>") && !v.contains("r=\"C3\"") && !v.contains("r=\"D5\""));
    with_evaluated(&p, |e| {
        assert_eq!(extent(e, "C2"), Some(range("C2", "D5")));
        for (i, cell) in ["C2", "D2", "C3", "D3", "C4", "D4", "C5", "D5"]
            .iter()
            .enumerate()
        {
            assert_eq!(value(e, cell), n(i as f64 + 1.0), "{cell}");
        }
        assert_eq!(value(e, "D6"), n(9.0));
        assert_eq!(value(e, "B7"), n(36.0));
    });
}

#[test]
fn genuine_outside_footprint_obstruction_still_blocks() {
    // Five rows need D6, a source input outside the prior footprint.
    let p = sparse(5);
    with_evaluated(&p, |e| {
        assert_err(value(e, "C2"), ExcelErrorKind::Spill, "anchor");
        assert_eq!(extent(e, "C2"), None);
        assert_eq!(value(e, "D6"), n(9.0), "the input is not overwritten");
        assert_eq!(value(e, "C4"), None, "masked children stay empty");
        assert_err(
            value(e, "B7"),
            ExcelErrorKind::Ref,
            "SUM(C2#) of a blocked anchor",
        );
    });
    // The blocked anchor publishes a typed `#SPILL!` (FORM211-C/D).
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(out.summary.error_summary["#SPILL!"].count, 1);
}

#[test]
fn two_anchors_sharing_metadata_both_spill() {
    let p = package("A1:F11", TWO_ANCHOR_ROWS, "");
    with_ingested(&p, |ingested| {
        let e = &mut ingested.engine;
        for cell in ["C3", "C4", "F3", "F4"] {
            assert_eq!(value(e, cell), None, "{cell}");
        }
        evaluate(e, &XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(extent(e, "C2"), Some(range("C2", "C5")));
        assert_eq!(extent(e, "F2"), Some(range("F2", "F5")));
        assert_column(e, "C", 2..=5);
        assert_column(e, "F", 2..=5);
        assert_eq!(value(e, "C9"), n(10.0));
        assert_eq!(value(e, "C10"), n(10.0));
    });
}

#[test]
fn cross_sheet_dependent_reads_the_new_spill() {
    let sheet2 = format!(
        "<worksheet xmlns=\"{MAIN}\"><dimension ref=\"A1:B1\"/><sheetData><row r=\"1\"><c r=\"A1\"><f>SUM(Sheet1!C2#)</f><v>0</v></c><c r=\"B1\"><f>SUM(_xlfn.ANCHORARRAY(Sheet1!$C$2))</f><v>0</v></c></row></sheetData></worksheet>"
    );
    let mut p = edit(
        producer(),
        "xl/workbook.xml",
        "</sheets>",
        "<sheet name=\"Sheet2\" sheetId=\"2\" r:id=\"rId2\"/></sheets>",
    );
    p = edit(
        p,
        WB_RELS,
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rId2\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"
        ),
    );
    p = edit(
        p,
        TYPES,
        "</Types>",
        "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>",
    );
    p.insert("xl/worksheets/sheet2.xml".into(), sheet2);
    with_evaluated(&p, |e| {
        assert_eq!(extent(e, "C2"), Some(range("C2", "C6")));
        assert_eq!(value_at(e, "Sheet2", "A1"), n(15.0));
        assert_eq!(value_at(e, "Sheet2", "B1"), n(15.0));
    });
}

/// Readers of the 1x1 declared anchor C10 (`SUM(_xlfn.ANCHORARRAY(C2))`).
fn with_c10_readers(p: Parts) -> Parts {
    edit(
        p,
        SHEET,
        "<v>99</v></c></row><row r=\"11\"",
        "<v>99</v></c><c r=\"E10\"><f>SUM(C10#)</f><v>0</v></c><c r=\"F10\"><f>ROWS(_xlfn.ANCHORARRAY(C10))</f><v>0</v></c></row><row r=\"11\"",
    )
}

#[test]
fn one_by_one_declared_anchors_resolve_as_one_cell_spills() {
    // C10 is a declared 1x1 anchor whose own formula reads C2's spill.
    let p = with_c10_readers(producer_wide());
    with_evaluated(&p, |e| {
        assert_eq!(value(e, "C10"), n(15.0), "SUM(_xlfn.ANCHORARRAY(C2))");
        assert_eq!(value(e, "E10"), n(15.0), "SUM(C10#)");
        assert_eq!(value(e, "F10"), n(1.0), "ROWS(ANCHORARRAY(C10))");
    });

    // An existing 3-cell spill that collapses to one cell keeps its identity.
    let collapsed = edit(
        with_c10_readers(producer_wide()),
        SHEET,
        "<c r=\"B1\"><v>5</v></c>",
        "<c r=\"B1\"><v>1</v></c>",
    );
    with_evaluated(&collapsed, |e| {
        assert_eq!(value(e, "C2"), n(1.0));
        assert_eq!(
            extent(e, "C2"),
            None,
            "a 1x1 result is committed as a scalar"
        );
        assert_eq!(value(e, "C3"), None, "the masked old child stays empty");
        assert_eq!(value(e, "C9"), n(1.0), "SUM(C2#)");
        assert_eq!(value(e, "C10"), n(1.0), "SUM(_xlfn.ANCHORARRAY(C2))");
        assert_eq!(value(e, "E10"), n(1.0));
    });
    // The collapse publishes with the anchor's 1x1 extent (FORM211-C/D).
    let out = recalculate_xlsx_bytes(&pack(&collapsed), Default::default()).unwrap();
    assert_eq!(out.summary.errors, 0);

    // Contrast: the same cell as an ordinary (undeclared) formula.
    let undeclared = edit(
        with_c10_readers(producer_wide()),
        SHEET,
        "<c r=\"C10\" cm=\"1\"><f t=\"array\" ref=\"C10\">",
        "<c r=\"C10\"><f>",
    );
    with_evaluated(&undeclared, |e| {
        assert_eq!(value(e, "C10"), n(15.0));
        assert_err(value(e, "E10"), ExcelErrorKind::Ref, "undeclared C10#");
    });
}

#[test]
fn formula_counts_do_not_depend_on_generated_caches() {
    let variants = [
        producer(),
        // Children serialized as styled shells.
        edit(
            edit(
                producer(),
                SHEET,
                "<c r=\"C3\" s=\"1\"><v>2</v></c>",
                "<c r=\"C3\" s=\"1\"/>",
            ),
            SHEET,
            "<c r=\"C4\" s=\"1\"><v>3</v></c>",
            "<c r=\"C4\" s=\"1\"/>",
        ),
        // Children not serialized at all.
        package(
            "A1:C11",
            &PRODUCER_ROWS
                .replace(
                    "<row r=\"3\" spans=\"1:3\"><c r=\"C3\" s=\"1\"><v>2</v></c></row>",
                    "",
                )
                .replace(
                    "<row r=\"4\" spans=\"1:3\"><c r=\"C4\" s=\"1\"><v>3</v></c></row>",
                    "",
                ),
            "",
        ),
    ];
    for (i, p) in variants.iter().enumerate() {
        let bytes = pack(p);
        let admission = admit_source(&bytes, &Default::default()).unwrap();
        assert_eq!(admission.formula_count, 3, "variant {i}");
        with_ingested(p, |ingested| {
            let e = &mut ingested.engine;
            for cell in ["C2", "C9", "C10"] {
                assert!(formula(e, cell).is_some(), "variant {i}: {cell}");
            }
            for cell in ["C3", "C4"] {
                assert!(formula(e, cell).is_none(), "variant {i}: {cell}");
                assert_eq!(value(e, cell), None, "variant {i}: {cell}");
            }
            evaluate(e, &XlsxRecalculateOptions::default()).unwrap();
            assert_eq!(value(e, "C9"), n(15.0), "variant {i}");
        });
    }
}

#[test]
fn anchors_are_declared_under_deferred_graph_building() {
    let collapsed = edit(
        producer(),
        SHEET,
        "<c r=\"B1\"><v>5</v></c>",
        "<c r=\"B1\"><v>1</v></c>",
    );
    let bytes = pack(&collapsed);
    let mut options = XlsxRecalculateOptions::default();
    options.eval_config.defer_graph_building = true;
    let admission = admit_source(&bytes, &options).unwrap();
    let mut ingested = ingest_source(&bytes, admission, &options).unwrap();
    assert!(!ingested.engine.has_staged_formulas());
    evaluate(&mut ingested.engine, &options).unwrap();
    assert_eq!(value(&ingested.engine, "C9"), n(1.0), "SUM(C2#)");
    assert_eq!(value(&ingested.engine, "C10"), n(1.0));
}
