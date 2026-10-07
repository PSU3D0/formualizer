#![cfg(feature = "xlsx-recalc")]
//! Stored (non-formula) cell values keep their Excel identity on import:
//! zero-length strings are empty text, not blank cells, and date-formatted
//! numbers keep every bit of their serial. Covered on both the cache
//! recalculation path and the Calamine workbook loader.
use calamine::{Data, Reader, Xlsx};
use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
    XlsxRecalculateOptions, recalculate_xlsx_bytes,
};
use std::io::{Cursor, Write};
use zip::ZipWriter;

const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

/// A one-sheet package with a shared-string table and a style sheet whose
/// `cellXfs` are: 0 General, 1 `mm-dd-yy` (built-in 14), 2 `h:mm AM/PM`.
fn package(rows: &str, shared: &[&str]) -> Vec<u8> {
    let sst: String = shared
        .iter()
        .map(|s| {
            if s.is_empty() {
                "<si><t/></si>".to_owned()
            } else {
                format!("<si><t>{s}</t></si>")
            }
        })
        .collect();
    let parts = [
        (
            "[Content_Types].xml",
            "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/><Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/><Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/></Types>".to_owned(),
        ),
        (
            "_rels/.rels",
            format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>"),
        ),
        (
            "xl/workbook.xml",
            format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>"),
        ),
        (
            "xl/_rels/workbook.xml.rels",
            format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId2\" Type=\"{OFFICE}/sharedStrings\" Target=\"sharedStrings.xml\"/><Relationship Id=\"rId3\" Type=\"{OFFICE}/styles\" Target=\"styles.xml\"/></Relationships>"),
        ),
        (
            "xl/sharedStrings.xml",
            format!("<sst xmlns=\"{MAIN}\" count=\"{n}\" uniqueCount=\"{n}\">{sst}</sst>", n = shared.len()),
        ),
        (
            "xl/styles.xml",
            format!("<styleSheet xmlns=\"{MAIN}\"><numFmts count=\"1\"><numFmt numFmtId=\"164\" formatCode=\"h:mm AM/PM\"/></numFmts><fonts count=\"1\"><font><sz val=\"11\"/><name val=\"Calibri\"/></font></fonts><fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills><borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs><cellXfs count=\"3\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/><xf numFmtId=\"14\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/><xf numFmtId=\"164\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/></cellXfs><cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles></styleSheet>"),
        ),
        (
            "xl/worksheets/sheet1.xml",
            format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{rows}</sheetData></worksheet>"),
        ),
    ];
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .last_modified_time(zip::DateTime::from_date_and_time(2020, 1, 2, 3, 4, 6).unwrap());
    for (name, body) in parts {
        z.start_file(name, options).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}

fn cached(bytes: &[u8], a1: (u32, u32)) -> Data {
    let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
    x.worksheet_range("Sheet1")
        .unwrap()
        .get_value(a1)
        .cloned()
        .unwrap_or(Data::Empty)
}

fn formula(r: u32, text: &str) -> String {
    format!("<c r=\"B{r}\"><f>{text}</f></c>")
}

/// Column A: A1 `title` (shared), A2 shared `""`, A3 `t="str"` with an
/// empty `<v>`, A4 inline `""`, A5 never written. Column B: formulas over
/// them, with no caches.
fn empty_text_workbook() -> Vec<u8> {
    let b = [
        "COUNTA(A1:A5)",
        "A2",
        "ISBLANK(A2)",
        "A2=\"\"",
        "LEN(A2)",
        "ISBLANK(A3)",
        "ISBLANK(A4)",
        "ISBLANK(A5)",
        "ISTEXT(A2)",
    ];
    let a = [
        "<c r=\"A1\" t=\"s\"><v>0</v></c>",
        "<c r=\"A2\" t=\"s\"><v>1</v></c>",
        "<c r=\"A3\" t=\"str\"><v></v></c>",
        "<c r=\"A4\" t=\"inlineStr\"><is><t></t></is></c>",
    ];
    let rows: String = b
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let r = i as u32 + 1;
            format!(
                "<row r=\"{r}\">{}{}</row>",
                a.get(i).copied().unwrap_or(""),
                formula(r, &f.replace('"', "&quot;"))
            )
        })
        .collect();
    package(&rows, &["title", ""])
}

#[test]
fn stored_empty_strings_are_empty_text_on_recalculation() {
    let out =
        recalculate_xlsx_bytes(&empty_text_workbook(), XlsxRecalculateOptions::default()).unwrap();
    let b = |r: u32| cached(&out.bytes, (r - 1, 1));
    assert_eq!(b(1), Data::Float(4.0), "COUNTA counts stored empty text");
    assert_eq!(
        b(2),
        Data::String(String::new()),
        "a reference returns \"\""
    );
    assert_eq!(b(3), Data::Bool(false), "ISBLANK of shared \"\"");
    assert_eq!(b(4), Data::Bool(true), "=A2=\"\"");
    assert_eq!(b(5), Data::Float(0.0), "LEN");
    assert_eq!(b(6), Data::Bool(false), "ISBLANK of t=str \"\"");
    assert_eq!(b(7), Data::Bool(false), "ISBLANK of inline \"\"");
    assert_eq!(b(8), Data::Bool(true), "ISBLANK of a never-written cell");
    assert_eq!(b(9), Data::Bool(true), "ISTEXT of shared \"\"");
}

#[test]
fn stored_empty_strings_are_empty_text_on_calamine_load() {
    let adapter = CalamineAdapter::open_bytes(empty_text_workbook()).unwrap();
    let mut wb =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .unwrap();
    for r in 2..=4 {
        assert_eq!(
            wb.get_value("Sheet1", r, 1),
            Some(LiteralValue::Text(String::new())),
            "A{r}"
        );
    }
    let mut b = |r: u32| wb.evaluate_cell("Sheet1", r, 2).unwrap();
    assert_eq!(b(1), LiteralValue::Number(4.0));
    assert_eq!(b(2), LiteralValue::Text(String::new()));
    assert_eq!(b(3), LiteralValue::Boolean(false));
    assert_eq!(b(4), LiteralValue::Boolean(true));
    assert_eq!(b(8), LiteralValue::Boolean(true));
}

#[test]
fn recalculation_over_stored_empty_text_with_correct_caches_is_a_byte_noop() {
    let input = package(
        "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><f>COUNTA(A1:A2)</f><v>2</v></c></row>\
         <row r=\"2\"><c r=\"A2\" t=\"s\"><v>1</v></c><c r=\"B2\" t=\"str\"><f>A2</f><v></v></c></row>",
        &["title", ""],
    );
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(out.cache_cells_changed, 0);
    assert_eq!(out.bytes, input);
}

/// Serials with sub-second parts under date and time formats, plus the same
/// serial under General. Column B references them; column C compares them
/// with the literal serial.
fn date_workbook() -> Vec<u8> {
    let rows: String = [
        (" s=\"2\"", "36538.361239814803"),
        (" s=\"1\"", "36538.361160763903"),
        ("", "36538.361239814803"),
        (" s=\"1\"", "-1.5"),
    ]
    .iter()
    .enumerate()
    .map(|(i, (style, v))| {
        let r = i + 1;
        format!(
            "<row r=\"{r}\"><c r=\"A{r}\"{style}><v>{v}</v></c><c r=\"B{r}\"><f>A{r}</f></c>\
             <c r=\"C{r}\"><f>A{r}={v}</f></c></row>"
        )
    })
    .collect();
    package(&rows, &[])
}

/// The stored serials as `f64` (the XML spells them with more digits).
const SERIALS: [f64; 4] = [36538.3612398148, 36538.3611607639, 36538.3612398148, -1.5];

#[test]
fn date_formatted_numbers_keep_their_exact_serial_on_recalculation() {
    let out = recalculate_xlsx_bytes(&date_workbook(), XlsxRecalculateOptions::default()).unwrap();
    for (i, serial) in SERIALS.into_iter().enumerate() {
        let r = i as u32;
        assert_eq!(
            cached(&out.bytes, (r, 1)),
            Data::Float(serial),
            "B{}",
            r + 1
        );
        assert_eq!(cached(&out.bytes, (r, 2)), Data::Bool(true), "C{}", r + 1);
    }
}

#[test]
fn date_formatted_numbers_keep_their_exact_serial_on_calamine_load() {
    // Serial egress shows the stored double itself; the default native egress
    // presents a date-formatted cell as a second-precision chrono value.
    let mut config = WorkbookConfig::ephemeral();
    config.eval.temporal_egress = formualizer_eval::engine::TemporalEgress::Serial;
    let adapter = CalamineAdapter::open_bytes(date_workbook()).unwrap();
    let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap();
    for (i, serial) in SERIALS.into_iter().enumerate() {
        let r = i as u32 + 1;
        assert_eq!(
            wb.get_value("Sheet1", r, 1),
            Some(LiteralValue::Number(serial)),
            "A{r}"
        );
        assert_eq!(
            wb.evaluate_cell("Sheet1", r, 3).unwrap(),
            LiteralValue::Boolean(true),
            "C{r}"
        );
    }
}

#[cfg(feature = "umya")]
#[test]
fn stored_empty_strings_and_date_serials_on_umya_load() {
    use formualizer_workbook::UmyaAdapter;
    let adapter = UmyaAdapter::open_bytes(empty_text_workbook()).unwrap();
    let mut wb =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .unwrap();
    // umya-spreadsheet itself reads a self-closing `<t/>` (shared or inline)
    // as no value, so only the `t="str"` cell keeps its empty text here.
    let mut b = |r: u32| wb.evaluate_cell("Sheet1", r, 2).unwrap();
    assert_eq!(b(6), LiteralValue::Boolean(false));
    assert_eq!(b(8), LiteralValue::Boolean(true));

    let mut config = WorkbookConfig::ephemeral();
    config.eval.temporal_egress = formualizer_eval::engine::TemporalEgress::Serial;
    let adapter = UmyaAdapter::open_bytes(date_workbook()).unwrap();
    let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap();
    for r in 1..=4 {
        assert_eq!(
            wb.evaluate_cell("Sheet1", r, 3).unwrap(),
            LiteralValue::Boolean(true),
            "C{r}"
        );
    }
}
