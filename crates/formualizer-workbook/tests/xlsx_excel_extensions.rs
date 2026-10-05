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
const CONTROLS: &[u8] = fixture!("controls.xlsx");
const CONTROLS_PLAIN: &[u8] = fixture!("controls_plain.xlsx");
const TABLES: &[u8] = fixture!("tables_xr.xlsx");
const WORKBOOK: &str = "xl/workbook.xml";
const SHEET: &str = "xl/worksheets/sheet1.xml";
const TABLE: &str = "xl/tables/table1.xml";
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

fn control_values(out: &[u8]) -> Vec<f64> {
    let mut v: Vec<f64> = (0..5).map(|r| number(out, "Data", (r, 1))).collect();
    v.extend((0..3).map(|r| number(out, "Data", (r, 3))));
    v
}
const OLE_OBJECTS: &str = "<oleObjects><mc:AlternateContent xmlns:mc=\"http://schemas.openxmlformats.org/markup-compatibility/2006\"><mc:Choice Requires=\"x14\"><oleObject progId=\"Paint.Picture\" shapeId=\"1026\"><objectPr defaultSize=\"0\"><anchor moveWithCells=\"1\"><from><xdr:col>0</xdr:col><xdr:colOff>0</xdr:colOff><xdr:row>0</xdr:row><xdr:rowOff>28575</xdr:rowOff></from><to><xdr:col>3</xdr:col><xdr:colOff>466725</xdr:colOff><xdr:row>1</xdr:row><xdr:rowOff>314325</xdr:rowOff></to></anchor></objectPr></oleObject></mc:Choice><mc:Fallback><oleObject progId=\"Paint.Picture\" shapeId=\"1026\"/></mc:Fallback></mc:AlternateContent></oleObjects>";

#[test]
fn form_controls_and_x14_validations_compute_as_without_them() {
    let sheet = text(CONTROLS, SHEET);
    assert!(sheet.contains("<xdr:row>3</xdr:row>") && sheet.contains("<xm:f>"));
    let plain = recalc(CONTROLS_PLAIN);
    let expected = vec![20.0, 40.0, 60.0, 80.0, 100.0, 300.0, 2.0, 10.0];
    assert_eq!(control_values(&plain), expected);
    let out = recalc(CONTROLS);
    assert_eq!(control_values(&out), expected);
    untouched_except_sheet(CONTROLS, &out);
    // The extension markup after sheetData is carried through verbatim.
    let tail = |s: &str| s[s.find("</sheetData>").unwrap()..].to_owned();
    assert_eq!(tail(&text(&out, SHEET)), tail(&sheet));
    // OLE object anchors, as Excel writes them.
    let ole = edit(
        CONTROLS,
        SHEET,
        "<extLst>",
        &format!("{OLE_OBJECTS}<extLst>"),
    );
    assert_eq!(control_values(&recalc(&ole)), expected);
}

#[test]
fn worksheet_lookalikes_in_data_bearing_or_unknown_positions_are_refused() {
    let lookalike = "foreign worksheet lookalike";
    let in_row = "<row r=\"2\"><mc:AlternateContent><mc:Choice Requires=\"x14\"><anchor><from><xdr:row>4</xdr:row></from></anchor></mc:Choice></mc:AlternateContent>";
    for (old, new) in [
        // Calamine reads any `row` or `f` inside sheetData as data.
        ("<row r=\"2\">", in_row),
        (
            "<c r=\"B1\"><f>A1*2</f>",
            "<c r=\"B1\"><f>A1*2</f><extLst><ext uri=\"{0}\"><xm:f xmlns:xm=\"http://schemas.microsoft.com/office/excel/2006/main\">A1*3</xm:f></ext></extLst>",
        ),
        // An unknown namespace in the anchor.
        (
            "<xdr:row>3</xdr:row>",
            "<u:row xmlns:u=\"urn:unknown\">3</u:row>",
        ),
        // Drawing names other than the anchor row/column, and anchors
        // outside markup-compatibility content.
        ("<xdr:row>3</xdr:row>", "<xdr:c>3</xdr:c>"),
        (
            "<extLst>",
            "<controls><control shapeId=\"1\"><controlPr><anchor><from><xdr:row>0</xdr:row></from></anchor></controlPr></control></controls><extLst>",
        ),
        (
            "<xdr:row>3</xdr:row>",
            "<xdr:row>3</xdr:row><xdr:sheetData/>",
        ),
        // xm:f outside the worksheet extension list.
        (
            "<controls>",
            "<controls><xm:f xmlns:xm=\"http://schemas.microsoft.com/office/excel/2006/main\">1</xm:f>",
        ),
        // Calamine reads merge cells by local name anywhere in the part.
        (
            "<xm:sqref>",
            "<x14:mergeCells><x14:mergeCell ref=\"A1:B2\"/></x14:mergeCells><xm:sqref>",
        ),
    ] {
        let error = refusal(&edit(CONTROLS, SHEET, old, new));
        assert!(error.contains(lookalike), "{new}: {error}");
    }
}

/// Under sheetData Calamine's cell reader treats every element inside a cell
/// as cell payload: an extension list holding `xm:f` there is not ignored.
#[test]
fn calamine_does_not_ignore_extension_content_inside_cells() {
    let formula = |bytes: &[u8], cell| {
        let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
        x.worksheet_formula("Data")
            .map(|r| r.get_value(cell).cloned())
    };
    // A recalculated copy, whose caches Calamine can decode.
    let base = recalc(CONTROLS_PLAIN);
    assert_eq!(formula(&base, (0, 1)).unwrap().as_deref(), Some("A1*2"));
    let cell = text(&base, SHEET);
    let b1 = &cell[cell.find("<c r=\"B1\"").unwrap()..];
    let b1 = &b1[..b1.find("</c>").unwrap()];
    let cell_ext = edit(
        &base,
        SHEET,
        b1,
        &format!(
            "{b1}<extLst><ext uri=\"{{0}}\"><xm:f xmlns:xm=\"http://schemas.microsoft.com/office/excel/2006/main\">A1*3</xm:f></ext></extLst>"
        ),
    );
    assert!(formula(&cell_ext, (0, 1)).is_err());
}

#[test]
fn excel_revision_attributes_on_tables_are_admitted_and_preserved() {
    let table = text(TABLES, TABLE);
    assert!(table.contains("mc:Ignorable=\"xr xr3\"") && table.contains("xr3:uid="));
    let out = recalc(TABLES);
    assert_eq!(number(&out, "Sales", (0, 3)), 9.0);
    assert_eq!(number(&out, "Sales", (1, 3)), 24.5);
    assert_eq!(members(&out)[TABLE], members(TABLES)[TABLE]);
    untouched_except_sheet(TABLES, &out);
}

#[test]
fn other_table_extension_attributes_are_refused() {
    for (old, new) in [
        (
            "revision3\" id=\"1\"",
            "revision3\" id=\"1\" xmlns:u=\"urn:unknown\" u:flag=\"1\"",
        ),
        (
            "revision3\" id=\"1\"",
            "revision3\" id=\"1\" xr3:uid=\"{0}\"",
        ),
        ("mc:Ignorable=\"xr xr3\"", "mc:Ignorable=\"\""),
        (
            "<tableColumn id=\"1\"",
            "<tableColumn mc:Ignorable=\"xr3\" id=\"1\"",
        ),
        (
            "<tableColumn id=\"1\"",
            "<tableColumn xr:uid=\"{0}\" id=\"1\"",
        ),
        (
            "<tableColumns count=\"2\"",
            "<tableColumns xr:uid=\"{0}\" count=\"2\"",
        ),
    ] {
        let error = refusal(&edit(TABLES, TABLE, old, new));
        assert!(
            error.contains("unsupported table attribute"),
            "{new}: {error}"
        );
    }
}
