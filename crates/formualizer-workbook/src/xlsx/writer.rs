//! Native OOXML `.xlsx` serializer for in-memory [`Workbook`] instances.
//!
//! Emits a self-contained SpreadsheetML package using only `zip` and UTF-8 XML
//! formatting — zero `umya-spreadsheet` dependency.
//!
//! Package layout:
//! - `[Content_Types].xml`
//! - `_rels/.rels`
//! - `xl/workbook.xml` (sheet order, `workbookPr date1904="1"` when 1904, `<calcPr>`)
//! - `xl/_rels/workbook.xml.rels`
//! - `xl/styles.xml` (minimal valid stylesheet; temporal values serialize as
//!   date-system-aware numeric serials per #355, leaving number-format fidelity
//!   to the format channel)
//! - `xl/worksheets/sheet{N}.xml` (numbers, inline strings, booleans, Excel-standard
//!   error cells, and formulas with cached values)

use crate::calc_pr::{calc_settings_from_cycle, render_calc_pr};
use crate::error::{IoError, col_to_a1};
use crate::workbook::Workbook;
use formualizer_common::{
    DateSystem, ExcelErrorKind, LiteralValue, datetime_to_serial_for, duration_to_days_fraction,
    time_to_fraction,
};
use std::fmt::Write as _;
use std::io::{Cursor, Write};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const MAIN_NS: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const REL_NS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const DOC_REL_NS: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const CONTENT_TYPES_NS: &str = "http://schemas.openxmlformats.org/package/2006/content-types";

const RELS_XML: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;

// Minimal valid OOXML stylesheet accepted by Excel, LibreOffice, openpyxl,
// umya-spreadsheet, and Formualizer's native reader.
const STYLES_XML: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>"#;

pub(crate) fn write_workbook_to_xlsx_bytes(workbook: &Workbook) -> Result<Vec<u8>, IoError> {
    let raw_sheet_names = workbook.sheet_names();
    let has_sheets = !raw_sheet_names.is_empty();
    let sheet_names: Vec<String> = if has_sheets {
        raw_sheet_names
    } else {
        vec!["Sheet1".to_string()]
    };

    for name in &sheet_names {
        if name.chars().any(is_illegal_xml10_char) {
            return Err(IoError::Unsupported {
                feature: "control character in worksheet name".into(),
                context: format!(
                    "worksheet name contains XML 1.0 illegal control characters: {name:?}"
                ),
            });
        }
    }

    let date_system = workbook.engine().config.date_system;
    let mut calc_settings = calc_settings_from_cycle(&workbook.engine().config.cycle);
    if let Some(parsed) = workbook.loaded_calc_settings() {
        calc_settings.calc_mode = parsed.calc_mode.clone();
        calc_settings.full_calc_on_load = parsed.full_calc_on_load;
    }

    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut out = Vec::with_capacity(4096);
    {
        let mut zip = ZipWriter::new(Cursor::new(&mut out));

        write_zip_entry(
            &mut zip,
            "[Content_Types].xml",
            options,
            build_content_types_xml(sheet_names.len()).as_bytes(),
        )?;
        write_zip_entry(&mut zip, "_rels/.rels", options, RELS_XML.as_bytes())?;
        write_zip_entry(
            &mut zip,
            "xl/workbook.xml",
            options,
            build_workbook_xml(&sheet_names, date_system, &calc_settings).as_bytes(),
        )?;
        write_zip_entry(
            &mut zip,
            "xl/_rels/workbook.xml.rels",
            options,
            build_workbook_rels_xml(sheet_names.len()).as_bytes(),
        )?;
        write_zip_entry(&mut zip, "xl/styles.xml", options, STYLES_XML.as_bytes())?;

        let mut sheet_xml = String::with_capacity(4096);
        for (idx, name) in sheet_names.iter().enumerate() {
            sheet_xml.clear();
            if has_sheets {
                write_worksheet_xml(&mut sheet_xml, workbook, name, date_system);
            } else {
                write_empty_worksheet_xml(&mut sheet_xml);
            }
            let part = format!("xl/worksheets/sheet{}.xml", idx + 1);
            write_zip_entry(&mut zip, &part, options, sheet_xml.as_bytes())?;
        }

        zip.finish()
            .map_err(|e| IoError::from_backend("xlsx-writer", e))?;
    }

    Ok(out)
}

fn write_zip_entry<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    name: &str,
    options: SimpleFileOptions,
    bytes: &[u8],
) -> Result<(), IoError> {
    zip.start_file(name, options)
        .map_err(|e| IoError::from_backend("xlsx-writer", e))?;
    zip.write_all(bytes)?;
    Ok(())
}

fn build_content_types_xml(sheet_count: usize) -> String {
    let mut xml = String::with_capacity(512 + sheet_count * 128);
    xml.push_str(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#);
    let _ = write!(
        xml,
        r#"<Types xmlns="{CONTENT_TYPES_NS}"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>"#
    );
    for idx in 1..=sheet_count {
        let _ = write!(
            xml,
            r#"<Override PartName="/xl/worksheets/sheet{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>"#
        );
    }
    xml.push_str("</Types>");
    xml
}

fn build_workbook_xml(
    sheet_names: &[String],
    date_system: DateSystem,
    calc_settings: &crate::traits::CalcSettings,
) -> String {
    let mut xml = String::with_capacity(512 + sheet_names.len() * 64);
    xml.push_str(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#);
    let _ = write!(
        xml,
        r#"<workbook xmlns="{MAIN_NS}" xmlns:r="{DOC_REL_NS}">"#
    );
    if date_system == DateSystem::Excel1904 {
        xml.push_str(r#"<workbookPr date1904="1"/>"#);
    }
    xml.push_str("<sheets>");
    for (idx, name) in sheet_names.iter().enumerate() {
        let sheet_id = idx + 1;
        xml.push_str(r#"<sheet name=""#);
        push_xml_escaped_attr(&mut xml, name);
        let _ = write!(xml, r#"" sheetId="{sheet_id}" r:id="rId{sheet_id}"/>"#);
    }
    xml.push_str("</sheets>");
    xml.push_str(&render_calc_pr(calc_settings, None));
    xml.push_str("</workbook>");
    xml
}

fn build_workbook_rels_xml(sheet_count: usize) -> String {
    let mut xml = String::with_capacity(256 + sheet_count * 128);
    xml.push_str(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#);
    let _ = write!(xml, r#"<Relationships xmlns="{REL_NS}">"#);
    for idx in 1..=sheet_count {
        let _ = write!(
            xml,
            r#"<Relationship Id="rId{idx}" Type="{DOC_REL_NS}/worksheet" Target="worksheets/sheet{idx}.xml"/>"#
        );
    }
    let styles_rid = sheet_count + 1;
    let _ = write!(
        xml,
        r#"<Relationship Id="rId{styles_rid}" Type="{DOC_REL_NS}/styles" Target="styles.xml"/>"#
    );
    xml.push_str("</Relationships>");
    xml
}

fn write_empty_worksheet_xml(out: &mut String) {
    out.push_str(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#);
    let _ = write!(
        out,
        r#"<worksheet xmlns="{MAIN_NS}"><sheetData/></worksheet>"#
    );
}

fn write_worksheet_xml(
    out: &mut String,
    workbook: &Workbook,
    sheet_name: &str,
    date_system: DateSystem,
) {
    let dims = workbook
        .sheet_dimensions(sheet_name)
        .filter(|&(r, c)| r > 0 && c > 0);
    let Some((max_row, max_col)) = dims else {
        write_empty_worksheet_xml(out);
        return;
    };

    out.push_str(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#);
    let _ = write!(out, r#"<worksheet xmlns="{MAIN_NS}">"#);
    if max_row == 1 && max_col == 1 {
        out.push_str(r#"<dimension ref="A1"/>"#);
    } else {
        let end_col = col_to_a1(max_col);
        let _ = write!(out, r#"<dimension ref="A1:{end_col}{max_row}"/>"#);
    }
    out.push_str("<sheetData>");

    // Precompute column letters once per sheet (`1..=max_col`) to avoid
    // repeated heap allocations on large sheets.
    let col_letters: Vec<String> = (1..=max_col).map(col_to_a1).collect();
    let mut row_buf = String::with_capacity(256);

    for row in 1..=max_row {
        row_buf.clear();
        for (col_idx, col_a1) in col_letters.iter().enumerate() {
            let col = (col_idx as u32) + 1;
            let formula = workbook.get_formula(sheet_name, row, col);
            let value = workbook.get_value(sheet_name, row, col);

            match (formula, value) {
                (Some(formula_text), cached) => {
                    write_formula_cell(
                        &mut row_buf,
                        col_a1,
                        row,
                        &formula_text,
                        cached.as_ref(),
                        date_system,
                    );
                }
                (None, Some(val)) => {
                    write_value_cell(&mut row_buf, col_a1, row, &val, date_system);
                }
                (None, None) => {}
            }
        }
        if !row_buf.is_empty() {
            let _ = write!(out, r#"<row r="{row}">"#);
            out.push_str(&row_buf);
            out.push_str("</row>");
        }
    }

    out.push_str("</sheetData></worksheet>");
}

/// Classic OOXML `t="e"` error tokens accepted by both Excel and Calamine/source-recalc
/// on literal and formula cells.
fn is_classic_ooxml_error(kind: ExcelErrorKind) -> bool {
    matches!(
        kind,
        ExcelErrorKind::Null
            | ExcelErrorKind::Div
            | ExcelErrorKind::Value
            | ExcelErrorKind::Ref
            | ExcelErrorKind::Name
            | ExcelErrorKind::Num
            | ExcelErrorKind::Na
    )
}

/// Excel-standard error tokens valid as cached formula `<v>` payloads with `t="e"`
/// (classic OOXML errors plus Excel 365 `#SPILL!` and `#CALC!`).
/// Engine-only errors (`#CIRC!`, `#N/IMPL!`, `#ERROR!`, `#CANCELLED!`) are not
/// Excel `t="e"` tokens and serialize as string payloads instead.
fn is_excel_formula_cache_error(kind: ExcelErrorKind) -> bool {
    is_classic_ooxml_error(kind) || matches!(kind, ExcelErrorKind::Spill | ExcelErrorKind::Calc)
}

fn write_formula_cell(
    out: &mut String,
    col_a1: &str,
    row: u32,
    formula: &str,
    cached: Option<&LiteralValue>,
    date_system: DateSystem,
) {
    let formula_body = formula.strip_prefix('=').unwrap_or(formula);

    // Determine optional `t="..."` cell type from cached value.
    enum CachedEncoding<'a> {
        None,
        Number(f64),
        Int(i64),
        Bool(bool),
        Str(std::borrow::Cow<'a, str>),
        Error(ExcelErrorKind),
    }

    let enc = match cached {
        None | Some(LiteralValue::Empty | LiteralValue::Pending) => CachedEncoding::None,
        Some(LiteralValue::Int(i)) => CachedEncoding::Int(*i),
        Some(LiteralValue::Number(n)) => {
            if n.is_finite() {
                CachedEncoding::Number(*n)
            } else {
                CachedEncoding::Error(ExcelErrorKind::Num)
            }
        }
        Some(LiteralValue::Boolean(b)) => CachedEncoding::Bool(*b),
        Some(LiteralValue::Text(s)) => CachedEncoding::Str(std::borrow::Cow::Borrowed(s.as_str())),
        Some(LiteralValue::Error(e)) => {
            if is_excel_formula_cache_error(e.kind) {
                CachedEncoding::Error(e.kind)
            } else {
                CachedEncoding::Str(std::borrow::Cow::Owned(e.kind.to_string()))
            }
        }
        Some(LiteralValue::Date(d)) => match d.and_hms_opt(0, 0, 0) {
            Some(dt) => CachedEncoding::Number(datetime_to_serial_for(date_system, &dt)),
            None => CachedEncoding::Error(ExcelErrorKind::Num),
        },
        Some(LiteralValue::DateTime(dt)) => {
            CachedEncoding::Number(datetime_to_serial_for(date_system, dt))
        }
        Some(LiteralValue::Time(t)) => CachedEncoding::Number(time_to_fraction(t)),
        Some(LiteralValue::Duration(d)) => CachedEncoding::Number(duration_to_days_fraction(d)),
        Some(LiteralValue::Array(_)) => CachedEncoding::Error(ExcelErrorKind::Value),
    };

    let cell_type = match &enc {
        CachedEncoding::None | CachedEncoding::Int(_) | CachedEncoding::Number(_) => None,
        CachedEncoding::Bool(_) => Some("b"),
        CachedEncoding::Str(_) => Some("str"),
        CachedEncoding::Error(_) => Some("e"),
    };

    let _ = write!(out, r#"<c r="{col_a1}{row}""#);
    if let Some(t) = cell_type {
        let _ = write!(out, r#" t="{t}""#);
    }
    out.push_str("><f>");
    push_xml_escaped_text(out, formula_body);
    out.push_str("</f>");

    match enc {
        CachedEncoding::None => {}
        CachedEncoding::Int(i) => {
            let _ = write!(out, "<v>{i}</v>");
        }
        CachedEncoding::Number(n) => {
            let _ = write!(out, "<v>{n}</v>");
        }
        CachedEncoding::Bool(b) => {
            out.push_str(if b { "<v>1</v>" } else { "<v>0</v>" });
        }
        CachedEncoding::Str(ref s) => {
            if needs_xml_space_preserve(s) {
                out.push_str(r#"<v xml:space="preserve">"#);
            } else {
                out.push_str("<v>");
            }
            push_xml_escaped_ooxml_string(out, s);
            out.push_str("</v>");
        }
        CachedEncoding::Error(kind) => {
            let _ = write!(out, "<v>{kind}</v>");
        }
    }

    out.push_str("</c>");
}

fn write_value_cell(
    out: &mut String,
    col_a1: &str,
    row: u32,
    value: &LiteralValue,
    date_system: DateSystem,
) {
    match value {
        LiteralValue::Empty | LiteralValue::Pending => {}
        LiteralValue::Int(i) => {
            let _ = write!(out, r#"<c r="{col_a1}{row}"><v>{i}</v></c>"#);
        }
        LiteralValue::Number(n) => {
            if n.is_finite() {
                let _ = write!(out, r#"<c r="{col_a1}{row}"><v>{n}</v></c>"#);
            } else {
                let _ = write!(
                    out,
                    r#"<c r="{col_a1}{row}" t="e"><v>{}</v></c>"#,
                    ExcelErrorKind::Num
                );
            }
        }
        LiteralValue::Boolean(b) => {
            let bit = if *b { '1' } else { '0' };
            let _ = write!(out, r#"<c r="{col_a1}{row}" t="b"><v>{bit}</v></c>"#);
        }
        LiteralValue::Text(s) => {
            write_inline_string_cell(out, col_a1, row, s);
        }
        LiteralValue::Error(e) => {
            if is_classic_ooxml_error(e.kind) {
                let _ = write!(out, r#"<c r="{col_a1}{row}" t="e"><v>{}</v></c>"#, e.kind);
            } else {
                // Non-classic literal error tokens (`#SPILL!`, `#CALC!`, and engine-only
                // `#CIRC!`, `#N/IMPL!`, `#ERROR!`, `#CANCELLED!`) are emitted as inline
                // strings so both Excel and Formualizer's source-preserving recalculation
                // accept the exported archive (matching legacy Umya behavior).
                let token = e.kind.to_string();
                write_inline_string_cell(out, col_a1, row, &token);
            }
        }
        LiteralValue::Date(d) => match d.and_hms_opt(0, 0, 0) {
            Some(dt) => {
                let serial = datetime_to_serial_for(date_system, &dt);
                let _ = write!(out, r#"<c r="{col_a1}{row}"><v>{serial}</v></c>"#);
            }
            None => {
                let _ = write!(
                    out,
                    r#"<c r="{col_a1}{row}" t="e"><v>{}</v></c>"#,
                    ExcelErrorKind::Num
                );
            }
        },
        LiteralValue::DateTime(dt) => {
            let serial = datetime_to_serial_for(date_system, dt);
            let _ = write!(out, r#"<c r="{col_a1}{row}"><v>{serial}</v></c>"#);
        }
        LiteralValue::Time(t) => {
            let serial = time_to_fraction(t);
            let _ = write!(out, r#"<c r="{col_a1}{row}"><v>{serial}</v></c>"#);
        }
        LiteralValue::Duration(d) => {
            let serial = duration_to_days_fraction(d);
            let _ = write!(out, r#"<c r="{col_a1}{row}"><v>{serial}</v></c>"#);
        }
        LiteralValue::Array(_) => {
            let _ = write!(
                out,
                r#"<c r="{col_a1}{row}" t="e"><v>{}</v></c>"#,
                ExcelErrorKind::Value
            );
        }
    }
}

fn write_inline_string_cell(out: &mut String, col_a1: &str, row: u32, s: &str) {
    let _ = write!(out, r#"<c r="{col_a1}{row}" t="inlineStr"><is><t"#);
    if needs_xml_space_preserve(s) {
        out.push_str(r#" xml:space="preserve""#);
    }
    out.push('>');
    push_xml_escaped_ooxml_string(out, s);
    out.push_str("</t></is></c>");
}

fn needs_xml_space_preserve(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    bytes.first().is_some_and(u8::is_ascii_whitespace)
        || bytes.last().is_some_and(u8::is_ascii_whitespace)
        || s.contains('\n')
        || s.contains('\r')
        || s.contains('\t')
        || s.contains("  ")
}

fn push_xml_escaped_attr(out: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\r' => out.push_str("&#xD;"),
            '\n' => out.push_str("&#xA;"),
            '\t' => out.push_str("&#x9;"),
            c if is_illegal_xml10_char(c) => {
                let _ = write!(out, "_x{:04X}_", c as u32);
            }
            c => out.push(c),
        }
    }
}

fn push_xml_escaped_text(out: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#xD;"),
            c if is_illegal_xml10_char(c) => {
                let _ = write!(out, "_x{:04X}_", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Escape text for OOXML `ST_Xstring` elements (`<t>`, `<v t="str">`).
///
/// Escapes XML metacharacters, preserves `\r` via `&#xD;`, encodes XML 1.0
/// illegal control characters as `_xHHHH_`, and escapes any literal `_xHHHH_`
/// (`_x[0-9A-Fa-f]{4}_`) sequence as `_x005F_xHHHH_` per OOXML spec so readers
/// that decode OOXML hex escapes recover the exact original string.
fn push_xml_escaped_ooxml_string(out: &mut String, s: &str) {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'_'
            && i + 7 <= bytes.len()
            && bytes[i + 1] == b'x'
            && bytes[i + 6] == b'_'
            && bytes[i + 2..i + 6].iter().all(u8::is_ascii_hexdigit)
        {
            out.push_str("_x005F_");
            i += 1;
            continue;
        }
        let ch = s[i..].chars().next().expect("valid utf-8 boundary");
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#xD;"),
            c if is_illegal_xml10_char(c) => {
                let _ = write!(out, "_x{:04X}_", c as u32);
            }
            c => out.push(c),
        }
        i += ch.len_utf8();
    }
}

fn is_illegal_xml10_char(c: char) -> bool {
    let cp = c as u32;
    (cp < 0x20 && !matches!(c, '\t' | '\n' | '\r')) || matches!(cp, 0xFFFE | 0xFFFF)
}
