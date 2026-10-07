//! Error reasons, unknown-function summaries and the display-precision
//! cache rule of the source-preserving recalculation path.
#![cfg(feature = "xlsx-recalc")]
use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
};
use zip::{ZipArchive, ZipWriter};

const SHEET: &str = "xl/worksheets/sheet1.xml";
const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

fn package(rows: &str, names: &str) -> Vec<u8> {
    let parts: BTreeMap<&str, String> = [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets>{names}</workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        (SHEET, format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{rows}</sheetData></worksheet>")),
    ]
    .into_iter()
    .collect();
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, body) in parts {
        z.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
fn sheet_xml(bytes: &[u8]) -> String {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut s = String::new();
    z.by_name(SHEET).unwrap().read_to_string(&mut s).unwrap();
    s
}

fn recalc(bytes: &[u8]) -> formualizer_workbook::XlsxRecalculateResult {
    recalculate_xlsx_bytes(bytes, XlsxRecalculateOptions::default()).unwrap()
}
fn row(cells: &str) -> String {
    format!("<row r=\"1\">{cells}</row>")
}
/// (location, message) of every listed `#NAME?`.
fn name_errors(out: &formualizer_workbook::XlsxRecalculateResult) -> Vec<(String, Option<String>)> {
    let summary = &out.summary.error_summary["#NAME?"];
    assert_eq!(summary.locations.len(), summary.messages.len());
    summary
        .locations
        .iter()
        .cloned()
        .zip(summary.messages.iter().cloned())
        .collect()
}

#[test]
fn name_error_reasons_survive_every_name_path() {
    for (formula, reason, unknown) in [
        ("SPDVOL(1)", "Unknown function: SPDVOL", Some("SPDVOL")),
        ("1+SPDVOL(1)", "Unknown function: SPDVOL", Some("SPDVOL")),
        (
            "_xll.EURO(1)",
            "Unknown function: _xll.EURO",
            Some("_xll.EURO"),
        ),
        (
            "_xlfn.NOTAFUNCTION(1)",
            "Unknown function: _xlfn.NOTAFUNCTION",
            Some("_xlfn.NOTAFUNCTION"),
        ),
        ("Macro1(2)", "Unknown function: Macro1", Some("Macro1")),
        ("NoSuchName", "Undefined name: NoSuchName", None),
        ("NoSuchName*2", "Undefined name: NoSuchName", None),
        (
            "SUM(NoSuchName,SPDVOL(1))",
            "Undefined name: NoSuchName",
            None,
        ),
    ] {
        let input = package(
            &row(&format!(
                "<c r=\"A1\"><f>{formula}</f><v>1</v></c><c r=\"B1\"><f>A1+1</f><v>2</v></c>"
            )),
            "",
        );
        let out = recalc(&input);
        // Written, not refused; the inheriting cell has no reason of its own.
        assert_eq!(out.summary.errors, 2, "{formula}");
        assert_eq!(
            name_errors(&out),
            vec![
                ("Sheet1!A1".into(), Some(reason.to_owned())),
                ("Sheet1!B1".into(), None)
            ],
            "{formula}"
        );
        let expected: BTreeMap<String, usize> =
            unknown.into_iter().map(|n| (n.to_owned(), 1)).collect();
        assert_eq!(out.summary.unknown_functions, expected, "{formula}");
        assert!(
            sheet_xml(&out.bytes).contains("<c r=\"A1\" t=\"e\"><f>"),
            "{formula}"
        );
    }
}

#[test]
fn known_prefixed_functions_and_let_locals_have_no_unknown_reason() {
    // LAMBDA defined names are refused by this writer, so they are not tested.
    let input = package(
        &row(concat!(
            "<c r=\"A1\"><f>_xlfn.STDEV.S(1,2)</f><v>0</v></c>",
            "<c r=\"B1\"><f>_xlfn.LET(_xlpm.v,2,_xlpm.v*3)</f><v>0</v></c>",
            "<c r=\"D1\"><f>_xlfn.LET(_xlpm.v,2,_xlpm.v+NOPE(1))</f><v>0</v></c>",
        )),
        "",
    );
    let out = recalc(&input);
    assert_eq!(
        name_errors(&out),
        vec![(
            "Sheet1!D1".into(),
            Some("Unknown function: NOPE".to_owned())
        )]
    );
    assert_eq!(out.summary.errors, 1);
}

#[test]
fn shared_formula_members_get_reasons_and_counts_are_complete_beyond_the_limit() {
    let rows: String = (1..=5)
        .map(|r| {
            let f = if r == 1 {
                "<f t=\"shared\" ref=\"A1:A5\" si=\"0\">SPDVOL(B1)</f>".to_owned()
            } else {
                "<f t=\"shared\" si=\"0\"/>".to_owned()
            };
            format!(
                "<row r=\"{r}\"><c r=\"A{r}\">{f}<v>1</v></c><c r=\"B{r}\"><v>{r}</v></c></row>"
            )
        })
        .collect::<String>()
        + "<row r=\"6\"><c r=\"A6\"><f>_xll.EURO(1)</f><v>1</v></c></row>";
    let input = package(&rows, "");
    let options = XlsxRecalculateOptions {
        error_location_limit: 2,
        ..Default::default()
    };
    let out = recalculate_xlsx_bytes(&input, options).unwrap();
    assert_eq!(out.summary.errors, 6);
    assert_eq!(
        out.summary.unknown_functions,
        BTreeMap::from([("SPDVOL".to_owned(), 5), ("_xll.EURO".to_owned(), 1)])
    );
    let listed = name_errors(&out);
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .all(|(_, m)| m.as_deref() == Some("Unknown function: SPDVOL"))
    );
    let out = recalc(&input);
    assert_eq!(
        name_errors(&out)[1],
        (
            "Sheet1!A2".into(),
            Some("Unknown function: SPDVOL".to_owned())
        )
    );
}

/// One formula per (cache, formula) pair in row 1, columns A, B, ...
fn tolerance_case(cache: &str, formula: &str) -> Vec<u8> {
    package(
        &row(&format!("<c r=\"A1\"><f>{formula}</f><v>{cache}</v></c>")),
        "",
    )
}

#[test]
fn caches_within_one_unit_in_the_15th_digit_are_current_and_keep_their_bytes() {
    for (cache, formula) in [
        // Excel's sequential SUM vs a lane-order sum: a rounding-boundary straddle.
        ("53433.999999999949", "53433.99999999998"),
        ("8.8664999999999985", "8.8665"),
        ("-8.8664999999999985", "-8.8665"),
        ("1.00000000000001", "1"),
        // Across a power of ten the larger magnitude sets the unit.
        ("9.99999999999995", "10"),
        ("10", "9.99999999999995"),
        ("1.2345678901234567E+300", "1.2345678901234561E+300"),
        ("1.2345678901234567E-300", "1.2345678901234561E-300"),
        ("0", "0"),
        ("-0", "0"),
    ] {
        let input = tolerance_case(cache, formula);
        let out = recalc(&input);
        assert_eq!(out.cache_cells_changed, 0, "{cache} vs {formula}");
        assert_eq!(out.worksheet_parts_changed, 0, "{cache} vs {formula}");
        assert_eq!(out.bytes, input, "{cache} vs {formula}");
    }
}

#[test]
fn larger_differences_zero_residues_and_type_changes_are_written() {
    for (cache, formula) in [
        ("1.00000000000002", "1"),
        ("9.9999999999998", "10"),
        ("53433.9999999999", "53434.0000000002"),
        // A zero cache against a cancellation residue (no zero snap yet).
        ("0", "0.1+0.2-0.3"),
        ("1E-300", "0"),
        ("-1", "1"),
        ("-1E-20", "1E-20"),
    ] {
        let out = recalc(&tolerance_case(cache, formula));
        assert_eq!(out.cache_cells_changed, 1, "{cache} vs {formula}");
    }
    // Type changes: text, boolean and error caches for a numeric result.
    for cell in [
        "<c r=\"A1\" t=\"str\"><f>1</f><v>1</v></c>",
        "<c r=\"A1\" t=\"b\"><f>1</f><v>1</v></c>",
        "<c r=\"A1\" t=\"e\"><f>1</f><v>#N/A</v></c>",
    ] {
        let out = recalc(&package(&row(cell), ""));
        assert_eq!(out.cache_cells_changed, 1, "{cell}");
    }
}

#[test]
fn edited_input_is_still_written_untouched_cells_keep_excel_digits_and_rerun_is_idempotent() {
    let rows = |b: &str| {
        row(&format!(
            "<c r=\"A1\"><f>53433.99999999998</f><v>53433.999999999949</v></c><c r=\"B1\"><v>{b}</v></c><c r=\"C1\"><f>B1*2</f><v>17.732999999999997</v></c>"
        ))
    };
    // Within tolerance everywhere: byte-identical.
    let input = package(&rows("8.8665"), "");
    let out = recalc(&input);
    assert_eq!(out.bytes, input);
    // B1 edited: C1 is rewritten, A1 keeps Excel's digits.
    let input = package(&rows("10"), "");
    let out = recalc(&input);
    assert_eq!(out.cache_cells_changed, 1);
    let xml = sheet_xml(&out.bytes);
    assert!(xml.contains("<v>53433.999999999949</v>"), "{xml}");
    assert!(
        xml.contains("<c r=\"C1\"><f>B1*2</f><v>20</v></c>"),
        "{xml}"
    );
    let again = recalc(&out.bytes);
    assert_eq!(again.cache_cells_changed, 0);
    assert_eq!(again.bytes, out.bytes);
}

#[test]
fn precision_as_displayed_is_computed_in_full_precision() {
    // `fullPrecision="0"` is Excel's "precision as displayed"; it is not
    // emulated or refused, and calcPr is kept as is.
    let calc = "<calcPr calcId=\"191029\" fullPrecision=\"0\"/>";
    let input = package(
        &row("<c r=\"A1\" s=\"0\"><v>0.123456</v></c><c r=\"B1\"><f>A1*3</f><v>0.37</v></c>"),
        calc,
    );
    let out = recalc(&input);
    assert_eq!(out.cache_cells_changed, 1);
    assert!(sheet_xml(&out.bytes).contains("<v>0.37036"));
    let mut z = ZipArchive::new(Cursor::new(&out.bytes)).unwrap();
    let mut workbook = String::new();
    z.by_name("xl/workbook.xml")
        .unwrap()
        .read_to_string(&mut workbook)
        .unwrap();
    assert!(workbook.contains(calc));
}
