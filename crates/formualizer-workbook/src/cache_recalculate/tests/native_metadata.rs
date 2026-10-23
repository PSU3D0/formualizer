//! Compare every positional SST entry and every style, including unused ones,
//! through a synthetic worksheet served by the pinned Calamine public reader.
use super::super::{IoError, XlsxRecalculateOptions, native_metadata as native};
use calamine::{DataRef, Reader, Xlsx};
use formualizer_eval::format::FormatId;
use std::{
    io::{Cursor, Read, Write},
    path::Path,
};
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};

const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const VALUES: [f64; 6] = [-1.0, 0.0, 0.5, 1.0, 1.25, 60.0];

pub(super) fn annotation(value: &DataRef<'_>) -> Option<FormatId> {
    match value {
        DataRef::DateTime(dt) if dt.is_duration() => Some(FormatId::DURATION),
        DataRef::DateTime(dt) if (0.0..1.0).contains(&dt.as_f64()) => Some(FormatId::TIME),
        DataRef::DateTime(dt) if dt.as_f64().fract().abs() > f64::EPSILON => {
            Some(FormatId::DATETIME)
        }
        DataRef::DateTime(_) => Some(FormatId::DATE),
        _ => None,
    }
}
pub(super) fn probe(sst: Option<&[u8]>, styles: Option<&[u8]>, worksheet: &[u8]) -> Vec<u8> {
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
    if let Some(data) = sst {
        add("xl/sharedStrings.xml", data);
    }
    if let Some(data) = styles {
        add("xl/styles.xml", data);
    }
    zip.finish().unwrap().into_inner()
}
fn compare(
    sst: Option<&[u8]>,
    styles: Option<&[u8]>,
    label: &str,
) -> Result<(usize, usize), IoError> {
    let options = XlsxRecalculateOptions::default();
    let strings = sst
        .map(|data| native::SharedStringTable::parse(data, &options))
        .transpose()?
        .unwrap_or_default();
    let formats = styles
        .map(|data| native::Styles::parse(data, &options))
        .transpose()?
        .unwrap_or_default();
    let mut xml = format!("<worksheet xmlns=\"{MAIN}\"><sheetData>");
    for i in 0..strings.len().max(formats.len() * VALUES.len()) {
        let row = i + 1;
        xml.push_str(&format!("<row r=\"{row}\">"));
        if i < strings.len() {
            xml.push_str(&format!("<c r=\"A{row}\" t=\"s\"><v>{i}</v></c>"));
        }
        if i < formats.len() * VALUES.len() {
            xml.push_str(&format!(
                "<c r=\"B{row}\" s=\"{}\"><v>{}</v></c>",
                i / VALUES.len(),
                VALUES[i % VALUES.len()]
            ));
        }
        xml.push_str("</row>");
    }
    xml.push_str("</sheetData></worksheet>");
    let mut book = Xlsx::new(Cursor::new(probe(sst, styles, xml.as_bytes()))).unwrap();
    let mut reader = book.worksheet_cells_reader("Probe").unwrap();
    let (mut string_count, mut style_values) = (0, 0);
    while let Some(record) = reader.next_cell_with_formula_metadata().unwrap() {
        let index = record.pos.0 as usize;
        if record.pos.1 == 0 {
            let DataRef::SharedString(actual) = record.value else {
                panic!("{label}: non-SST value")
            };
            assert_eq!(
                strings.get(index).unwrap(),
                actual,
                "{label}: SST index {index}"
            );
            string_count += 1;
        } else {
            let style = index / VALUES.len();
            assert_eq!(
                formats.format(Some(&style.to_string()), VALUES[index % VALUES.len()]),
                annotation(&record.value),
                "{label}: style {style} / value {}",
                VALUES[index % VALUES.len()]
            );
            style_values += 1;
        }
    }
    assert_eq!(string_count, strings.len());
    assert_eq!(style_values, formats.len() * VALUES.len());
    assert!(strings.get(strings.len()).is_err());
    Ok((string_count, formats.len()))
}
fn package(path: &Path) -> (usize, usize) {
    let bytes = std::fs::read(path).unwrap();
    let mut archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut read = |name: &str| {
        let mut data = Vec::new();
        archive.by_name(name).ok().map(|mut file| {
            file.read_to_end(&mut data).unwrap();
            data
        })
    };
    let sst = read("xl/sharedStrings.xml");
    let styles = read("xl/styles.xml");
    compare(
        sst.as_deref(),
        styles.as_deref(),
        &path.display().to_string(),
    )
    .unwrap()
}
#[test]
fn positional_empty_strings_and_number_formats_match_calamine() {
    let sst = format!(
        concat!(
            "<sst xmlns=\"{}\"><si/><si><t/></si><si><t>  a  </t></si>",
            "<si><r><t> x </t></r><rPh><t>omit</t></rPh><r><t xml:space=\"preserve\"> y </t></r></si>",
            "<si><t>_x000D__x005F_x000A_ </t></si><si><t>a\u{85}b\u{2028}c\r\nd&amp;e</t></si></sst>"
        ),
        MAIN
    );
    let styles = format!(
        concat!(
            "<styleSheet xmlns=\"{}\"><numFmts>",
            "<numFmt numFmtId=\"164\" formatCode=\"[hh]:mm:ss\"/><numFmt numFmtId=\"165\" formatCode=\"0 &amp;quot;days&amp;quot;\"/>",
            "<numFmt numFmtId=\"166\" formatCode=\"_d0\"/><numFmt numFmtId=\"167\" formatCode=\"0;yyyy\"/>",
            "<numFmt numFmtId=\"168\" formatCode=\"AM/PM\"/></numFmts><cellXfs>",
            "<xf/><xf numFmtId=\"14\"/><xf numFmtId=\"46\"/><xf numFmtId=\"164\"/><xf numFmtId=\"165\"/>",
            "<xf numFmtId=\"166\"/><xf numFmtId=\"167\"/><xf numFmtId=\"168\"/><xf numFmtId=\"014\"/>",
            "<xf numFmtId=\"&#49;4\"/></cellXfs></styleSheet>"
        ),
        MAIN
    );
    assert_eq!(
        compare(
            Some(sst.as_bytes()),
            Some(styles.as_bytes()),
            "edge metadata"
        )
        .unwrap(),
        (6, 10)
    );
}
#[test]
fn absent_and_invalid_style_tokens_match_reader_fallback() {
    let options = XlsxRecalculateOptions::default();
    let data = format!(
        "<styleSheet xmlns=\"{MAIN}\"><cellXfs><xf numFmtId=\"46\"/><xf numFmtId=\"14\"/></cellXfs></styleSheet>"
    );
    let formats = native::Styles::parse(data.as_bytes(), &options).unwrap();
    assert_eq!(formats.format(None, 1.25), None);
    assert_eq!(formats.format(Some("bad"), 1.25), Some(FormatId::DURATION));
    assert_eq!(formats.format(Some("1"), 1.25), Some(FormatId::DATETIME));
    assert_eq!(formats.format(Some("999"), 1.25), None);
    let tokens = [
        None,
        Some("bad"),
        Some("1"),
        Some("999"),
        Some("&#49;"),
        Some("-1"),
        Some("+1"),
        Some("00"),
        Some("1x"),
        Some("0001"),
        Some(""),
        Some(" 1 "),
        Some("&#48;"),
    ];
    let mut xml = format!("<worksheet xmlns=\"{MAIN}\"><sheetData>");
    for (index, token) in tokens.iter().enumerate() {
        let row = index + 1;
        let attr = token.map_or(String::new(), |s| format!(" s=\"{s}\""));
        xml.push_str(&format!(
            "<row r=\"{row}\"><c r=\"A{row}\"{attr}><v>1.25</v></c></row>"
        ));
    }
    xml.push_str("</sheetData></worksheet>");
    let mut book = Xlsx::new(Cursor::new(probe(
        None,
        Some(data.as_bytes()),
        xml.as_bytes(),
    )))
    .unwrap();
    let mut reader = book.worksheet_cells_reader("Probe").unwrap();
    while let Some(record) = reader.next_cell_with_formula_metadata().unwrap() {
        let token = tokens[record.pos.0 as usize];
        assert_eq!(
            formats.format(token, 1.25),
            annotation(&record.value),
            "raw style token {token:?}"
        );
    }
}
#[test]
fn raw_type_entities_are_not_calamine_type_tokens() {
    let xml = format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"&#110;\"><v>1</v></c></row></sheetData></worksheet>"
    );
    let mut book = Xlsx::new(Cursor::new(probe(None, None, xml.as_bytes()))).unwrap();
    let mut reader = book.worksheet_cells_reader("Probe").unwrap();
    assert!(
        matches!(reader.next_cell_with_formula_metadata(), Err(calamine::XlsxError::CellTAttribute(raw)) if raw == "&#110;")
    );
}
#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_metadata_entries_match_calamine() {
    let paths =
        std::fs::read_to_string(std::env::var("FORMUALIZER_SCAN_DIFF_FILES").unwrap()).unwrap();
    let (mut files, mut strings, mut styles) = (0, 0, 0);
    for path in paths.lines().map(str::trim).filter(|s| !s.is_empty()) {
        let (s, f) = package(Path::new(path));
        files += 1;
        strings += s;
        styles += f;
    }
    assert!(files > 0);
    eprintln!(
        "native metadata: {files} packages / {strings} SST entries / {styles} style indices (6 values each), zero divergences"
    );
}
