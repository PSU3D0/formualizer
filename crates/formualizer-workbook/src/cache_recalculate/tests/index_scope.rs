//! The per-cell source index is built only for worksheets that need it:
//! admitted anchors (at admission) or a new multi-cell spill (after
//! evaluation). Scalar-only worksheets never build one.
use super::super::{XlsxRecalculateOptions, admit_source, recalculate_xlsx_bytes, sheet};
use super::dynamic_admission::{OFFICE, TYPES, WB_RELS, edit, pack, package, producer};
use std::collections::BTreeMap;

type Parts = BTreeMap<String, String>;

fn indexed_scans() -> usize {
    sheet::INDEXED_SCANS.with(|n| n.get())
}
/// Indexed scans performed by `f` on this thread.
fn counting<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = indexed_scans();
    let out = f();
    (out, indexed_scans() - before)
}
/// A second worksheet with only scalar formulas.
fn with_scalar_sheet(p: Parts) -> Parts {
    let mut p = edit(
        p,
        "xl/workbook.xml",
        "</sheets>",
        "<sheet name=\"Scalars\" sheetId=\"2\" r:id=\"rId9\"/></sheets>",
    );
    p = edit(
        p,
        WB_RELS,
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rId9\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"
        ),
    );
    p = edit(
        p,
        TYPES,
        "</Types>",
        "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>",
    );
    p.insert(
        "xl/worksheets/sheet2.xml".into(),
        format!(
            "<worksheet xmlns=\"{}\"><sheetData><row r=\"1\"><c r=\"A1\"><v>4</v></c><c r=\"B1\"><f>A1*2</f><v>0</v></c></row></sheetData></worksheet>",
            super::dynamic_admission::MAIN
        ),
    );
    p
}
/// The producer shape with only scalar formulas (the metadata part stays).
fn scalar_only() -> Parts {
    let rows = "<row r=\"1\"><c r=\"A1\"><v>3</v></c><c r=\"B1\"><f>A1+1</f><v>9</v></c></row>";
    package("A1:B1", rows, "")
}

#[test]
fn scalar_workbooks_build_no_source_index() {
    let bytes = pack(&with_scalar_sheet(scalar_only()));
    let options = XlsxRecalculateOptions::default();
    let (admission, scans) = counting(|| admit_source(&bytes, &options).unwrap());
    assert_eq!(scans, 0);
    assert!(admission.plans.iter().all(|p| p.index.is_none()));
    assert!(
        admission.metadata.is_some(),
        "a metadata part alone is not enough"
    );
    drop(admission);
    let (out, scans) = counting(|| recalculate_xlsx_bytes(&bytes, options).unwrap());
    assert_eq!(scans, 0);
    assert_eq!(out.cache_cells_changed, 2);
}

#[test]
fn cse_indexes_only_its_sheet() {
    let bytes = pack(&with_scalar_sheet(edit(
        producer(),
        "xl/worksheets/sheet1.xml",
        " cm=\"1\"",
        "",
    )));
    let (admission, scans) =
        counting(|| admit_source(&bytes, &XlsxRecalculateOptions::default()).unwrap());
    assert_eq!(scans, 1);
    assert!(admission.plans[0].index.is_some());
    assert!(admission.plans[1].index.is_none());
}

#[test]
fn only_the_sheet_with_admitted_anchors_is_indexed() {
    let bytes = pack(&with_scalar_sheet(producer()));
    let options = XlsxRecalculateOptions::default();
    let (admission, scans) = counting(|| admit_source(&bytes, &options).unwrap());
    assert_eq!(scans, 1);
    assert!(admission.plans[0].index.is_some());
    assert!(admission.plans[1].index.is_none());
    drop(admission);
    let (out, scans) = counting(|| recalculate_xlsx_bytes(&bytes, options).unwrap());
    assert_eq!(scans, 1, "no further index after evaluation");
    assert_eq!(out.worksheet_parts_changed, 2);
}

#[test]
fn a_new_spill_indexes_only_its_sheet_after_evaluation() {
    let rows = "<row r=\"1\"><c r=\"B1\"><v>3</v></c></row><row r=\"2\"><c r=\"C2\"><f>_xlfn.SEQUENCE($B$1)</f><v>0</v></c></row>";
    let p = with_scalar_sheet(package("B1:C2", rows, ""));
    let bytes = pack(&p);
    let options = XlsxRecalculateOptions::default();
    let (admission, scans) = counting(|| admit_source(&bytes, &options).unwrap());
    assert_eq!(scans, 0);
    assert!(admission.plans.iter().all(|p| p.index.is_none()));
    drop(admission);
    let (out, scans) = counting(|| recalculate_xlsx_bytes(&bytes, options).unwrap());
    assert_eq!(scans, 1);
    assert!(out.bytes.len() > bytes.len());
    // The published spill now carries cm, so its sheet is indexed at
    // admission; the scalar sheet still is not.
    let (again, scans) =
        counting(|| recalculate_xlsx_bytes(&out.bytes, Default::default()).unwrap());
    assert_eq!(scans, 1);
    assert_eq!(again.bytes, out.bytes);
}
