#![cfg(feature = "xlsx-recalc")]
//! Source recalculation admits the extension markup Excel 2010+ writes
//! routinely (see `tests/fixtures/excel_extensions/generate.py`) where both
//! readers provably ignore it, and keeps refusing lookalikes elsewhere.
use calamine::{Data, Reader, Xlsx};
use formualizer_workbook::recalculate_xlsx_bytes;
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
};
use zip::{ZipArchive, ZipWriter};

macro_rules! fixture {
    ($name:literal) => {
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/excel_extensions/",
            $name
        ))
        .as_slice()
    };
}
const DATE1904: &[u8] = fixture!("date1904_x15.xlsx");
const DATE1900: &[u8] = fixture!("date1900_x15.xlsx");
const WORKBOOK: &str = "xl/workbook.xml";
const SHEET: &str = "xl/worksheets/sheet1.xml";
const X15_EXT: &str = "<ext uri=\"{140A7094-0E35-4892-8432-C4D2E57EDEB5}\" xmlns:x15=\"http://schemas.microsoft.com/office/spreadsheetml/2010/11/main\"><x15:workbookPr chartTrackingRefBase=\"1\"/></ext>";
const X15_PR: &str = "<x15:workbookPr chartTrackingRefBase=\"1\"/>";

fn members(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    (0..z.len())
        .map(|i| {
            let mut f = z.by_index(i).unwrap();
            let mut out = Vec::new();
            f.read_to_end(&mut out).unwrap();
            (f.name().to_owned(), out)
        })
        .collect()
}
fn text(bytes: &[u8], name: &str) -> String {
    String::from_utf8(members(bytes).remove(name).unwrap()).unwrap()
}
/// Repack with one member's text replaced (`old` must occur exactly once).
fn edit(bytes: &[u8], name: &str, old: &str, new: &str) -> Vec<u8> {
    let mut parts = members(bytes);
    let body = String::from_utf8(parts[name].clone()).unwrap();
    assert_eq!(body.matches(old).count(), 1, "{name}: {old}");
    parts.insert(name.to_owned(), body.replace(old, new).into_bytes());
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, body) in parts {
        z.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        z.write_all(&body).unwrap();
    }
    z.finish().unwrap().into_inner()
}
fn number(bytes: &[u8], sheet: &str, cell: (u32, u32)) -> f64 {
    let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
    match x.worksheet_range(sheet).unwrap().get_value(cell) {
        Some(Data::Float(f)) => *f,
        Some(Data::Int(i)) => *i as f64,
        Some(Data::DateTime(d)) => d.as_f64(),
        other => panic!("{sheet}!{cell:?}: {other:?}"),
    }
}
fn recalc(bytes: &[u8]) -> Vec<u8> {
    let out = recalculate_xlsx_bytes(bytes, Default::default())
        .unwrap_or_else(|e| panic!("{e}"))
        .bytes;
    let again = recalculate_xlsx_bytes(&out, Default::default()).unwrap();
    assert_eq!(again.bytes, out, "second run is a no-op");
    out
}
/// Every member except the recalculated worksheet keeps its bytes.
fn untouched_except_sheet(input: &[u8], output: &[u8]) {
    let (mut before, mut after) = (members(input), members(output));
    assert_ne!(before.remove(SHEET), after.remove(SHEET));
    assert_eq!(before, after);
}
fn refusal(bytes: &[u8]) -> String {
    match recalculate_xlsx_bytes(bytes, Default::default()) {
        Ok(_) => panic!("admitted"),
        Err(e) => e.to_string(),
    }
}

/// (D1 = DATE(2024,1,15), D2 = DATE(1904,1,2)) per date system.
fn dates(out: &[u8]) -> [f64; 6] {
    [
        number(out, "Dates", (0, 3)),
        number(out, "Dates", (1, 3)),
        number(out, "Dates", (0, 1)),
        number(out, "Dates", (1, 1)),
        number(out, "Dates", (0, 2)) - number(out, "Dates", (0, 0)),
        number(out, "Dates", (0, 4)),
    ]
}

#[test]
fn x15_workbook_pr_keeps_the_main_date_system() {
    for (input, expected) in [
        (DATE1904, [43844.0, 1.0, 2024.0, 1904.0, 1.0, 15.0]),
        (DATE1900, [45306.0, 1463.0, 2024.0, 1904.0, 1.0, 15.0]),
    ] {
        assert!(text(input, WORKBOOK).contains(X15_PR));
        let out = recalc(input);
        assert_eq!(dates(&out), expected);
        untouched_except_sheet(input, &out);
        // The same workbook without the extension computes the same values.
        let bare = edit(input, WORKBOOK, X15_EXT, "");
        assert_eq!(dates(&recalc(&bare)), expected);
    }
}

#[test]
fn x15_workbook_pr_lookalikes_are_refused() {
    let lookalike = "foreign workbook metadata lookalike";
    let twice = format!("{X15_PR}{X15_PR}");
    for (old, new, reason) in [
        // A date system on the extension element would make readers disagree.
        (
            X15_PR,
            "<x15:workbookPr chartTrackingRefBase=\"1\" date1904=\"0\"/>",
            lookalike,
        ),
        (
            X15_PR,
            "<x15:workbookPr chartTrackingRefBase=\"2\"/>",
            lookalike,
        ),
        (X15_PR, twice.as_str(), lookalike),
        (
            "{140A7094-0E35-4892-8432-C4D2E57EDEB5}",
            "{00000000-0E35-4892-8432-C4D2E57EDEB5}",
            lookalike,
        ),
        // Outside the extension list.
        (
            "<bookViews>",
            "<x15:workbookPr chartTrackingRefBase=\"1\"/><bookViews>",
            lookalike,
        ),
        // A main-namespace duplicate in the extension list.
        (
            X15_PR,
            "<workbookPr date1904=\"0\"/>",
            "duplicate/misplaced workbookPr",
        ),
        // An unknown namespace.
        (
            X15_PR,
            "<u:workbookPr xmlns:u=\"urn:unknown\" chartTrackingRefBase=\"1\"/>",
            lookalike,
        ),
        // Calamine and the defined-name scan would read these as names.
        (
            X15_PR,
            "<x15:definedNames><x15:definedName name=\"N\">1</x15:definedName></x15:definedNames>",
            lookalike,
        ),
    ] {
        for input in [DATE1904, DATE1900] {
            let error = refusal(&edit(input, WORKBOOK, old, new));
            assert!(error.contains(reason), "{new}: {error}");
        }
    }
}
