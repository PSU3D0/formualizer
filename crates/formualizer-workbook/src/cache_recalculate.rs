//! Cache-only XLSX recalculation. This deliberately edits only worksheet formula
//! cache payloads; all other package members are raw-copied.
use crate::{CalamineAdapter, IoError, SpreadsheetReader, workbook::WBResolver};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{CancelToken, Engine, EvalConfig};
use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use zip::{ZipArchive, ZipWriter};

use super::recalculate::{DEFAULT_ERROR_LOCATION_LIMIT, RecalculateStatus, RecalculateSummary};

/// Resource admission limits for [`recalculate_xlsx_bytes`].
#[derive(Debug, Clone)]
pub struct XlsxRecalculateLimits {
    pub max_input_bytes: usize,
    pub max_entries: usize,
    pub max_expanded_bytes: usize,
    pub max_worksheet_bytes: usize,
    pub max_formula_cells: usize,
}
impl Default for XlsxRecalculateLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 << 20,
            max_entries: 10_000,
            max_expanded_bytes: 256 << 20,
            max_worksheet_bytes: 64 << 20,
            max_formula_cells: 100_000,
        }
    }
}

/// Options for the cache-only XLSX recalculator.
#[derive(Debug, Clone)]
pub struct XlsxRecalculateOptions {
    pub eval_config: EvalConfig,
    pub cancel: Option<CancelToken>,
    pub limits: XlsxRecalculateLimits,
    pub error_location_limit: usize,
}
impl Default for XlsxRecalculateOptions {
    fn default() -> Self {
        Self {
            eval_config: EvalConfig::default(),
            cancel: None,
            limits: XlsxRecalculateLimits::default(),
            error_location_limit: DEFAULT_ERROR_LOCATION_LIMIT,
        }
    }
}

/// Result of a cache-only recalculation.
#[derive(Debug, Clone)]
pub struct XlsxRecalculateResult {
    pub bytes: Vec<u8>,
    pub summary: RecalculateSummary,
    pub formula_cells: usize,
    pub cache_cells_changed: usize,
    pub worksheet_parts_changed: usize,
}

fn unsupported(feature: impl Into<String>, context: impl Into<String>) -> IoError {
    IoError::Unsupported {
        feature: feature.into(),
        context: context.into(),
    }
}
fn checkpoint(token: &Option<CancelToken>) -> Result<(), IoError> {
    if token.as_ref().is_some_and(CancelToken::is_cancelled) {
        Err(IoError::Engine(formualizer_common::ExcelError::new(
            formualizer_common::ExcelErrorKind::Cancelled,
        )))
    } else {
        Ok(())
    }
}
fn attr(tag: &str, name: &str) -> Option<String> {
    let needle = format!("{name}=\"");
    let p = tag.find(&needle)? + needle.len();
    let end = tag[p..].find('"')? + p;
    Some(tag[p..end].to_owned())
}
fn xml_escape(text: &str) -> Result<String, IoError> {
    let mut out = String::new();
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\t'
            | '\n'
            | '\r'
            | ' '..='\u{d7ff}'
            | '\u{e000}'..='\u{fffd}'
            | '\u{10000}'..='\u{10ffff}' => out.push(c),
            _ => {
                return Err(unsupported(
                    "formula cached text control character",
                    "worksheet cache",
                ));
            }
        }
    }
    // SpreadsheetML treats _xHHHH_ as an escape. Escape the leading underscore
    // so literal escape-looking user text round-trips as text.
    let mut i = 0;
    while i + 7 <= out.len() {
        if out.as_bytes()[i] == b'_'
            && out.as_bytes()[i + 1] == b'x'
            && out.as_bytes()[i + 6] == b'_'
            && out[i + 2..i + 6].bytes().all(|b| b.is_ascii_hexdigit())
        {
            out.replace_range(i..i + 1, "_x005F_");
            i += 7;
        } else {
            i += 1;
        }
    }
    Ok(out)
}
fn cache(value: &LiteralValue) -> Result<(Option<&'static str>, String), IoError> {
    match value {
        LiteralValue::Int(v) => Ok((None, v.to_string())),
        LiteralValue::Number(v) if v.is_finite() => Ok((None, v.to_string())),
        LiteralValue::Boolean(v) => Ok((Some("b"), if *v { "1".into() } else { "0".into() })),
        LiteralValue::Text(v) => Ok((Some("str"), xml_escape(v)?)),
        LiteralValue::Error(v) => Ok((Some("e"), xml_escape(&v.to_string())?)),
        LiteralValue::Empty => Ok((Some("str"), String::new())),
        LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => Err(unsupported(
            "temporal formula result",
            "cache-only writer currently accepts engine scalar serials only",
        )),
        LiteralValue::Array(_) => Err(unsupported("multi-cell formula spill", "cache-only writer")),
        LiteralValue::Pending => Err(unsupported("pending formula result", "cache-only writer")),
        LiteralValue::Number(_) => Err(unsupported(
            "non-finite formula result",
            "cache-only writer",
        )),
    }
}
#[derive(Clone)]
struct FormulaCell {
    sheet: String,
    row: u32,
    col: u32,
    part: String,
    cell_start: usize,
    cell_end: usize,
}
fn coord(s: &str) -> Option<(u32, u32)> {
    let mut col = 0u32;
    let mut pos = 0;
    for b in s.bytes() {
        if b.is_ascii_alphabetic() {
            col = col
                .checked_mul(26)?
                .checked_add(u32::from(b.to_ascii_uppercase() - b'A' + 1))?;
            pos += 1
        } else {
            break;
        }
    }
    if pos == 0 {
        return None;
    }
    let row = s[pos..].parse().ok()?;
    Some((row, col))
}
fn sheets(
    bytes: &[u8],
    archive: &mut ZipArchive<Cursor<&[u8]>>,
) -> Result<Vec<(String, String)>, IoError> {
    let mut wb = String::new();
    archive
        .by_name("xl/workbook.xml")
        .map_err(|_| unsupported("missing workbook.xml", "XLSX package"))?
        .read_to_string(&mut wb)?;
    let mut rel = String::new();
    archive
        .by_name("xl/_rels/workbook.xml.rels")
        .map_err(|_| unsupported("missing workbook relationships", "XLSX package"))?
        .read_to_string(&mut rel)?;
    if wb.contains("<!DOCTYPE")
        || wb.contains("<!ENTITY")
        || rel.contains("<!DOCTYPE")
        || rel.contains("<!ENTITY")
    {
        return Err(unsupported("DTD/entity XML", "workbook metadata"));
    }
    let mut ids = BTreeMap::new();
    let mut at = 0;
    while let Some(i) = wb[at..].find("<sheet ") {
        let i = at + i;
        let e = wb[i..]
            .find('>')
            .ok_or_else(|| unsupported("malformed sheet", "workbook.xml"))?
            + i;
        let t = &wb[i..=e];
        let name =
            attr(t, "name").ok_or_else(|| unsupported("sheet without name", "workbook.xml"))?;
        let id = attr(t, "r:id")
            .ok_or_else(|| unsupported("sheet without relationship", "workbook.xml"))?;
        ids.insert(id, name);
        at = e + 1;
    }
    let mut out = Vec::new();
    at = 0;
    while let Some(i) = rel[at..].find("<Relationship ") {
        let i = at + i;
        let e = rel[i..]
            .find('>')
            .ok_or_else(|| unsupported("malformed relationship", "workbook.xml.rels"))?
            + i;
        let t = &rel[i..=e];
        if attr(t, "Type")
            .as_deref()
            .is_some_and(|x| x.ends_with("/worksheet"))
        {
            let id = attr(t, "Id").ok_or_else(|| {
                unsupported("worksheet relationship without id", "workbook.xml.rels")
            })?;
            let target = attr(t, "Target").ok_or_else(|| {
                unsupported("worksheet relationship without target", "workbook.xml.rels")
            })?;
            if target.contains("..") || target.starts_with('/') || target.contains(':') {
                return Err(unsupported("worksheet relationship target", target));
            }
            if let Some(name) = ids.remove(&id) {
                out.push((name, format!("xl/{target}")));
            }
        }
        at = e + 1;
    }
    if !ids.is_empty() || out.is_empty() {
        return Err(unsupported(
            "ambiguous or unresolved worksheet relationships",
            "workbook.xml",
        ));
    }
    let _ = bytes;
    Ok(out)
}
fn scan(
    part: &str,
    xml: &str,
    sheet: &str,
    out: &mut Vec<FormulaCell>,
    limit: usize,
) -> Result<(), IoError> {
    if xml.contains("<!DOCTYPE") || xml.contains("<!ENTITY") || xml.contains("externalLink") {
        return Err(unsupported("DTD/entity or external link", part));
    }
    let mut at = 0;
    while let Some(p) = xml[at..].find("<c") {
        let start = at + p;
        let next = xml.as_bytes().get(start + 2).copied();
        if !matches!(next, Some(b' ') | Some(b'>')) {
            at = start + 2;
            continue;
        }
        let open_end = xml[start..]
            .find('>')
            .ok_or_else(|| unsupported("malformed cell XML", part))?
            + start;
        let end = xml[open_end..]
            .find("</c>")
            .ok_or_else(|| unsupported("unclosed cell XML", part))?
            + open_end
            + 4;
        let cell = &xml[start..end];
        if let Some(fp) = cell.find("<f") {
            let f_end = cell[fp..]
                .find('>')
                .ok_or_else(|| unsupported("malformed formula XML", part))?
                + fp;
            let f = &cell[fp..=f_end];
            if let Some(kind) = attr(f, "t")
                && kind != "shared"
            {
                return Err(unsupported(format!("formula kind {kind}"), part));
            }
            if f.contains("ref=") && attr(f, "t").as_deref() != Some("shared") {
                return Err(unsupported("dynamic/array formula metadata", part));
            }
            let tag = &xml[start..=open_end];
            let r = attr(tag, "r")
                .ok_or_else(|| unsupported("formula cell without coordinate", part))?;
            let (row, col) =
                coord(&r).ok_or_else(|| unsupported("invalid formula coordinate", r))?;
            out.push(FormulaCell {
                sheet: sheet.into(),
                row,
                col,
                part: part.into(),
                cell_start: start,
                cell_end: end,
            });
            if out.len() > limit {
                return Err(unsupported("formula cell limit", part));
            }
        }
        at = end;
    }
    Ok(())
}
fn patch_cell(old: &str, value: &LiteralValue) -> Result<String, IoError> {
    let (ty, payload) = cache(value)?;
    let open_end = old.find('>').unwrap();
    let old_tag = &old[..=open_end];
    let old_type = attr(old_tag, "t");
    let expected_type = ty.map(str::to_owned);
    if old_type == expected_type
        && old[open_end + 1..].contains(&format!("<v>{payload}</v>"))
        && !old[open_end + 1..].contains("<is>")
    {
        return Ok(old.to_owned());
    }
    let mut tag = old[..=open_end].to_string();
    // Formula caches are never shared strings. Remove an existing t attribute and normalize.
    for needle in [" t=\""] {
        while let Some(p) = tag.find(needle) {
            let e = tag[p + needle.len()..]
                .find('"')
                .ok_or_else(|| unsupported("malformed cell type", "worksheet"))?
                + p
                + needle.len()
                + 1;
            tag.replace_range(p..e, "");
        }
    }
    if let Some(ty) = ty {
        tag.insert_str(tag.len() - 1, &format!(" t=\"{ty}\""));
    }
    let rest = &old[open_end + 1..old.len() - 4];
    let f_end = rest
        .find("</f>")
        .ok_or_else(|| unsupported("formula without closing f", "worksheet"))?
        + 4;
    let after = &rest[f_end..];
    // An inline-string child cannot coexist with the cache representation.
    let after = if after.trim_start().starts_with("<is>") {
        let s = after.find("<is>").unwrap();
        let e = after[s..]
            .find("</is>")
            .ok_or_else(|| unsupported("malformed inline string", "worksheet"))?
            + s
            + 5;
        format!("{}{}", &after[..s], &after[e..])
    } else {
        after.to_string()
    };
    let without_v = if let Some(v) = after.find("<v>") {
        let e = after[v..]
            .find("</v>")
            .ok_or_else(|| unsupported("malformed cached value", "worksheet"))?
            + v
            + 4;
        format!("{}{}", &after[..v], &after[e..])
    } else {
        after
    };
    Ok(format!(
        "{tag}{}<v>{payload}</v>{}</c>",
        &rest[..f_end],
        without_v
    ))
}

/// Recalculate physical ordinary/shared formulas and return an XLSX whose only
/// semantic edits are their cached values. Unsupported package constructs fail closed.
pub fn recalculate_xlsx_bytes(
    bytes: &[u8],
    options: XlsxRecalculateOptions,
) -> Result<XlsxRecalculateResult, IoError> {
    checkpoint(&options.cancel)?;
    if bytes.len() > options.limits.max_input_bytes {
        return Err(unsupported("input size limit", "XLSX package"));
    }
    let mut archive =
        ZipArchive::new(Cursor::new(bytes)).map_err(|e| IoError::from_backend("zip", e))?;
    if archive.len() > options.limits.max_entries {
        return Err(unsupported("ZIP entry limit", "XLSX package"));
    }
    let mut total = 0usize;
    for i in 0..archive.len() {
        let f = archive
            .by_index(i)
            .map_err(|e| IoError::from_backend("zip", e))?;
        if f.encrypted() {
            return Err(unsupported("encrypted ZIP member", f.name()));
        }
        total = total
            .checked_add(usize::try_from(f.size()).unwrap_or(usize::MAX))
            .ok_or_else(|| unsupported("ZIP expansion limit", "XLSX package"))?;
    }
    if total > options.limits.max_expanded_bytes {
        return Err(unsupported("ZIP expansion limit", "XLSX package"));
    }
    if archive
        .file_names()
        .any(|n| n.starts_with("_xmlsignatures/") || n.contains("origin.sigs"))
    {
        return Err(unsupported("package digital signature", "XLSX package"));
    }
    let mappings = sheets(bytes, &mut archive)?;
    let mut cells = Vec::new();
    let mut xmls = BTreeMap::new();
    for (name, part) in &mappings {
        checkpoint(&options.cancel)?;
        let mut xml = String::new();
        archive
            .by_name(part)
            .map_err(|_| unsupported("missing worksheet", part))?
            .read_to_string(&mut xml)?;
        if xml.len() > options.limits.max_worksheet_bytes {
            return Err(unsupported("worksheet size limit", part));
        }
        scan(
            part,
            &xml,
            name,
            &mut cells,
            options.limits.max_formula_cells,
        )?;
        xmls.insert(part.clone(), xml);
    }
    if cells.is_empty() {
        return Ok(XlsxRecalculateResult {
            bytes: bytes.to_vec(),
            summary: RecalculateSummary::default(),
            formula_cells: 0,
            cache_cells_changed: 0,
            worksheet_parts_changed: 0,
        });
    }
    checkpoint(&options.cancel)?;
    let mut adapter = CalamineAdapter::open_bytes(bytes.to_vec()).map_err(IoError::Calamine)?;
    let mut engine: Engine<WBResolver> = Engine::new(WBResolver::default(), options.eval_config);
    adapter.stream_into_engine(&mut engine)?;
    let targets: Vec<_> = cells
        .iter()
        .map(|c| (c.sheet.as_str(), c.row, c.col))
        .collect();
    let values = if let Some(cancel) = options.cancel.clone() {
        engine.evaluate_cells_cancellable(&targets, cancel)?
    } else {
        engine.evaluate_cells(&targets)?
    };
    let formula_count = targets.len();
    drop(targets);
    let mut summary = RecalculateSummary::default();
    let mut updates: BTreeMap<String, Vec<(FormulaCell, LiteralValue)>> = BTreeMap::new();
    for (cell, value) in cells.into_iter().zip(values) {
        let value = value.ok_or_else(|| {
            unsupported(
                "absent formula result",
                format!("{}!{}:{}", cell.sheet, cell.row, cell.col),
            )
        })?;
        let s = summary.sheets.entry(cell.sheet.clone()).or_default();
        s.evaluated += 1;
        summary.evaluated += 1;
        if let LiteralValue::Error(e) = &value {
            summary.errors += 1;
            s.errors += 1;
            let q = summary.error_summary.entry(e.kind.to_string()).or_default();
            q.count += 1;
            if q.locations.len() < options.error_location_limit {
                q.locations.push(format!(
                    "{}!{}{}",
                    cell.sheet,
                    crate::error::col_to_a1(cell.col),
                    cell.row
                ))
            } else {
                q.locations_truncated += 1
            }
        }
        updates
            .entry(cell.part.clone())
            .or_default()
            .push((cell, value));
    }
    summary.status = if summary.errors == 0 {
        RecalculateStatus::Success
    } else {
        RecalculateStatus::ErrorsFound
    };
    checkpoint(&options.cancel)?;
    let mut replacements = BTreeMap::new();
    for (part, mut entries) in updates {
        let xml = xmls.remove(&part).unwrap();
        entries.sort_by_key(|(c, _)| std::cmp::Reverse(c.cell_start));
        let mut patched = xml.clone();
        for (c, v) in entries {
            let replacement = patch_cell(&patched[c.cell_start..c.cell_end], &v)?;
            patched.replace_range(c.cell_start..c.cell_end, &replacement);
        }
        if patched != xml {
            replacements.insert(part, patched.into_bytes());
        }
    }
    if replacements.is_empty() {
        return Ok(XlsxRecalculateResult {
            bytes: bytes.to_vec(),
            summary,
            formula_cells: formula_count,
            cache_cells_changed: 0,
            worksheet_parts_changed: 0,
        });
    }
    checkpoint(&options.cancel)?;
    let mut source =
        ZipArchive::new(Cursor::new(bytes)).map_err(|e| IoError::from_backend("zip", e))?;
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..source.len() {
        checkpoint(&options.cancel)?;
        let file = source
            .by_index(i)
            .map_err(|e| IoError::from_backend("zip", e))?;
        if let Some(data) = replacements.get(file.name()) {
            let options =
                zip::write::SimpleFileOptions::default().compression_method(file.compression());
            writer
                .start_file(file.name(), options)
                .map_err(|e| IoError::from_backend("zip", e))?;
            writer.write_all(data)?;
        } else {
            writer
                .raw_copy_file(file)
                .map_err(|e| IoError::from_backend("zip", e))?;
        }
    }
    let bytes = writer
        .finish()
        .map_err(|e| IoError::from_backend("zip", e))?
        .into_inner();
    Ok(XlsxRecalculateResult {
        bytes,
        summary,
        formula_cells: formula_count,
        cache_cells_changed: formula_count,
        worksheet_parts_changed: replacements.len(),
    })
}

/// Native snapshot + same-directory temporary + atomic-replace wrapper. The
/// source is read before any destination mutation; it does not provide CAS
/// against unrelated concurrent writers.
#[cfg(not(target_arch = "wasm32"))]
pub fn recalculate_xlsx_file(
    input: &Path,
    output: Option<&Path>,
    options: XlsxRecalculateOptions,
) -> Result<XlsxRecalculateResult, IoError> {
    let source = std::fs::read(input)?;
    let result = recalculate_xlsx_bytes(&source, options)?;
    let dest = output.unwrap_or(input);
    let dir = dest.parent().unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(dir)?;
    temp.write_all(&result.bytes)?;
    temp.flush()?;
    temp.persist(dest).map_err(|e| IoError::Io(e.error))?;
    Ok(result)
}
