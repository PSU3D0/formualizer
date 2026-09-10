//! Bounded package admission and relationship-aware workbook discovery.
mod content_types;
mod rewrite;
use super::{IoError, XlsxRecalculateOptions, checkpoint, unsupported, xml};
pub(super) use rewrite::rewrite;
use std::collections::{BTreeMap, HashSet};
use std::io::{Cursor, Read};
use zip::ZipArchive;

pub(super) type Archive<'a> = ZipArchive<Cursor<&'a [u8]>>;
#[derive(Debug)]
pub(super) struct Relationship {
    pub kind: String,
    pub target: Option<String>,
}
#[derive(Debug)]
pub(super) struct Sheet {
    pub name: String,
    pub part: String,
}
fn u16_at(bytes: &[u8], offset: usize) -> Result<usize, IoError> {
    let b = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| unsupported("truncated ZIP metadata", "XLSX package"))?;
    Ok(u16::from_le_bytes([b[0], b[1]]) as usize)
}
fn u32_at(bytes: &[u8], offset: usize) -> Result<usize, IoError> {
    let b = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| unsupported("truncated ZIP metadata", "XLSX package"))?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
}
fn checked_end(start: usize, length: usize, bound: usize) -> Result<usize, IoError> {
    start
        .checked_add(length)
        .filter(|end| *end <= bound)
        .ok_or_else(|| unsupported("ZIP metadata range", "XLSX package"))
}
fn part_name(name: &str) -> Result<(), IoError> {
    if name.is_empty() || name.starts_with('/') || name.contains(['\\', '\0', ':', '?', '#', '%']) {
        return Err(unsupported("non-canonical ZIP part name", "XLSX package"));
    }
    let name = name.strip_suffix('/').unwrap_or(name);
    if name.split('/').any(|p| matches!(p, "" | "." | "..")) {
        return Err(unsupported("non-canonical ZIP part path", "XLSX package"));
    }
    Ok(())
}
// ZIP7 indexes members by name and can hide duplicate central-directory names.
// This is a bounded metadata audit, NOT a ZIP writer/decoder. ZIP64, arbitrary
// entry extras/comments and split archives are explicitly unsupported because
// the raw-copy API cannot preserve their complete container metadata contract.
fn audit_directory(
    bytes: &[u8],
    archive: &Archive<'_>,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if archive.offset() != 0 {
        return Err(unsupported("prefixed ZIP archive", "XLSX package"));
    }
    let start = usize::try_from(archive.central_directory_start())
        .map_err(|_| unsupported("ZIP offset overflow", "XLSX package"))?;
    let mut at = start;
    let mut names = HashSet::new();
    let mut ranges = Vec::new();
    while bytes.get(at..at + 4) == Some(b"PK\x01\x02") {
        checkpoint(&options.cancel)?;
        let fixed_end = checked_end(at, 46, bytes.len())?;
        let name_len = u16_at(bytes, at + 28)?;
        let extra_len = u16_at(bytes, at + 30)?;
        let comment_len = u16_at(bytes, at + 32)?;
        let end = checked_end(fixed_end, name_len + extra_len + comment_len, bytes.len())?;
        let raw_name = &bytes[fixed_end..fixed_end + name_len];
        let name = std::str::from_utf8(raw_name)
            .map_err(|_| unsupported("non-UTF-8 ZIP name", "XLSX package"))?;
        part_name(name)?;
        if !names.insert(name) {
            return Err(unsupported("duplicate ZIP member", "XLSX package"));
        }
        if names.len() > options.limits.max_entries {
            return Err(unsupported("ZIP entry count limit", "XLSX package"));
        }
        if extra_len != 0 || comment_len != 0 {
            return Err(unsupported("ZIP entry extra metadata or comment", name));
        }
        if u16_at(bytes, at + 34)? != 0 {
            return Err(unsupported("split ZIP archive", name));
        }
        let flags = u16_at(bytes, at + 8)?;
        if flags & 1 != 0 {
            return Err(unsupported("encrypted ZIP member", name));
        }
        if flags & 8 != 0 {
            return Err(unsupported("ZIP data descriptor", name));
        }
        if !raw_name.is_ascii() && flags & (1 << 11) == 0 {
            return Err(unsupported("ambiguous ZIP name encoding", name));
        }
        let compressed = u32_at(bytes, at + 20)?;
        let expanded = u32_at(bytes, at + 24)?;
        let local = u32_at(bytes, at + 42)?;
        if [compressed, expanded, local].contains(&(u32::MAX as usize)) {
            return Err(unsupported("ZIP64 member", name));
        }
        checked_end(local, 30, start)?;
        if bytes.get(local..local + 4) != Some(b"PK\x03\x04") {
            return Err(unsupported("invalid ZIP local header", name));
        }
        let local_name_len = u16_at(bytes, local + 26)?;
        let local_extra = u16_at(bytes, local + 28)?;
        let data = checked_end(local + 30, local_name_len + local_extra, start)?;
        if local_extra != 0 {
            return Err(unsupported("ZIP local extra metadata", name));
        }
        if bytes.get(local + 30..local + 30 + local_name_len) != Some(raw_name)
            || u16_at(bytes, local + 6)? != flags
            || u16_at(bytes, local + 8)? != u16_at(bytes, at + 10)?
            || bytes[local + 14..local + 26] != bytes[at + 16..at + 28]
            || bytes[local + 10..local + 14] != bytes[at + 12..at + 16]
            || u16_at(bytes, local + 4)? != u16_at(bytes, at + 6)?
        {
            return Err(unsupported("inconsistent ZIP local/central metadata", name));
        }
        ranges.push((local, checked_end(data, compressed, start)?));
        at = end;
    }
    if bytes.get(at..at + 4) != Some(b"PK\x05\x06") {
        return Err(unsupported(
            "ZIP64 or unsupported ZIP footer",
            "XLSX package",
        ));
    }
    checked_end(at, 22, bytes.len())?;
    if u16_at(bytes, at + 4)? != 0
        || u16_at(bytes, at + 6)? != 0
        || u16_at(bytes, at + 8)? != names.len()
        || u16_at(bytes, at + 10)? != names.len()
        || archive.len() != names.len()
        || u32_at(bytes, at + 12)? != at - start
        || u32_at(bytes, at + 16)? != start
        || checked_end(at + 22, u16_at(bytes, at + 20)?, bytes.len())? != bytes.len()
    {
        return Err(unsupported(
            "inconsistent ZIP directory/footer",
            "XLSX package",
        ));
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|w| w[0].1 > w[1].0) {
        return Err(unsupported("overlapping ZIP members", "XLSX package"));
    }
    Ok(())
}
pub(super) fn admit<'a>(
    bytes: &'a [u8],
    options: &XlsxRecalculateOptions,
) -> Result<Archive<'a>, IoError> {
    checkpoint(&options.cancel)?;
    if bytes.len() > options.limits.max_input_bytes {
        return Err(unsupported("input byte limit", "XLSX package"));
    }
    // Bound the declared directory before ZIP7 allocates its member index.
    let mut footers = Vec::new();
    for at in bytes.len().saturating_sub(65_557)..bytes.len().saturating_sub(21) {
        if bytes.get(at..at + 4) == Some(b"PK\x05\x06")
            && at + 22 + u16_at(bytes, at + 20)? == bytes.len()
        {
            footers.push(at);
        }
    }
    if footers.len() != 1 {
        return Err(unsupported("ambiguous/missing ZIP footer", "XLSX package"));
    }
    let footer = footers[0];
    if u16_at(bytes, footer + 10)? > options.limits.max_entries
        || u16_at(bytes, footer + 10)? == u16::MAX as usize
    {
        return Err(unsupported(
            "ZIP entry count limit or ZIP64",
            "XLSX package",
        ));
    }
    if u32_at(bytes, footer + 16)?.checked_add(u32_at(bytes, footer + 12)?) != Some(footer) {
        return Err(unsupported(
            "ZIP64 or inconsistent directory extent",
            "XLSX package",
        ));
    }
    let mut archive =
        ZipArchive::new(Cursor::new(bytes)).map_err(|e| IoError::from_backend("zip", e))?;
    audit_directory(bytes, &archive, options)?;
    let mut total = 0usize;
    let mut buffer = [0u8; 64 * 1024];
    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| IoError::from_backend("zip", e))?;
        if file.encrypted() {
            return Err(unsupported("encrypted ZIP member", file.name()));
        }
        if file.name().starts_with("_xmlsignatures/") || file.name().ends_with("origin.sigs") {
            return Err(unsupported("package digital signature", "XLSX package"));
        }
        if file.name().starts_with("xl/externalLinks/")
            || file.name() == "xl/metadata.xml"
            || file.name().starts_with("xl/richData/")
        {
            return Err(unsupported(
                "external links or rich/dynamic cell metadata",
                "XLSX package",
            ));
        }
        let mut member_bytes = 0u64;
        loop {
            checkpoint(&options.cancel)?;
            let n = file.read(&mut buffer)?;
            member_bytes += n as u64;
            if n == 0 {
                break;
            }
            total = total
                .checked_add(n)
                .ok_or_else(|| unsupported("expanded byte overflow", "XLSX package"))?;
            if total > options.limits.max_expanded_bytes {
                return Err(unsupported("actual ZIP expansion limit", "XLSX package"));
            }
        }
        if member_bytes != file.size() {
            return Err(unsupported("ZIP expanded-size mismatch", file.name()));
        }
    }
    Ok(archive)
}
pub(super) fn read_part(
    archive: &mut Archive<'_>,
    name: &str,
    limit: usize,
) -> Result<Vec<u8>, IoError> {
    let file = archive
        .by_name(name)
        .map_err(|e| IoError::from_backend("zip", e))?;
    let mut bytes = Vec::new();
    file.take((limit as u64).saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(unsupported("XML part byte limit", name));
    }
    Ok(bytes)
}
pub(super) fn relationships(
    archive: &mut Archive<'_>,
    source: &str,
    options: &XlsxRecalculateOptions,
) -> Result<BTreeMap<String, Relationship>, IoError> {
    let (parent, name) = source.rsplit_once('/').unwrap_or(("", source));
    let part = if source.is_empty() {
        "_rels/.rels".into()
    } else if parent.is_empty() {
        format!("_rels/{name}.rels")
    } else {
        format!("{parent}/_rels/{name}.rels")
    };
    let data = read_part(archive, &part, options.limits.max_worksheet_bytes)?;
    let mut result = BTreeMap::new();
    xml::walk(&data, options, |path, node| {
        if !matches!(node.kind, xml::Kind::Open { .. }) {
            return Ok(());
        }
        if path.len() == 1 && !xml::path_is(path, xml::RELS, &["Relationships"]) {
            return Err(unsupported("relationship XML root/namespace", &part));
        }
        if path.last().is_some_and(|e| e.local == "Relationship") {
            if !xml::path_is(path, xml::RELS, &["Relationships", "Relationship"]) {
                return Err(unsupported("ambiguous relationship element", &part));
            }
            let mode = node.value("TargetMode");
            if !matches!(mode, None | Some("Internal" | "External")) {
                return Err(unsupported("invalid relationship target mode", &part));
            }
            let target = node.required("Target")?;
            if mode != Some("External") && (target.contains('%') || target.contains("//")) {
                return Err(unsupported(
                    "encoded/non-canonical internal relationship target",
                    &part,
                ));
            }
            let target = if mode == Some("External") {
                None
            } else {
                Some(crate::xlsx_path::resolve(source, target)?)
            };
            let rel = Relationship {
                kind: node.required("Type")?.to_owned(),
                target,
            };
            if result
                .insert(node.required("Id")?.to_owned(), rel)
                .is_some()
            {
                return Err(unsupported("duplicate relationship ID", &part));
            }
        }
        Ok(())
    })?;
    Ok(result)
}
pub(super) fn discover(
    archive: &mut Archive<'_>,
    options: &XlsxRecalculateOptions,
) -> Result<(Vec<Sheet>, formualizer_common::DateSystem), IoError> {
    let root = relationships(archive, "", options)?;
    if root.values().any(|r| r.kind.contains("digital-signature")) {
        return Err(unsupported(
            "package digital signature",
            "root relationships",
        ));
    }
    let document_type = format!("{}/officeDocument", xml::OFFICE);
    let documents: Vec<_> = root.values().filter(|r| r.kind == document_type).collect();
    if documents.len() != 1 || documents[0].target.as_deref() != Some("xl/workbook.xml") {
        return Err(unsupported(
            "unsupported officeDocument mapping",
            "cache-only ingestion requires xl/workbook.xml",
        ));
    }
    let relations = relationships(archive, "xl/workbook.xml", options)?;
    for rel in relations.values() {
        if rel.kind.ends_with("/externalLink") || rel.kind.contains("digital-signature") {
            return Err(unsupported(
                "external workbook link/signature",
                "workbook relationships",
            ));
        }
        for (kind, expected) in [
            ("styles", "xl/styles.xml"),
            ("sharedStrings", "xl/sharedStrings.xml"),
        ] {
            if rel.kind == format!("{}/{kind}", xml::OFFICE)
                && rel.target.as_deref() != Some(expected)
            {
                return Err(unsupported("unsupported adapter metadata mapping", kind));
            }
        }
    }
    let data = read_part(
        archive,
        "xl/workbook.xml",
        options.limits.max_worksheet_bytes,
    )?;
    let mut sheets = Vec::new();
    let mut names = HashSet::new();
    let mut targets = HashSet::new();
    let mut epoch = formualizer_common::DateSystem::Excel1900;
    let mut workbook_pr = false;
    let mut metadata_sections = HashSet::new();
    let mut defined_names = HashSet::new();
    let mut sheet_ids = HashSet::new();
    xml::walk(&data, options, |path, node| {
        if !matches!(node.kind, xml::Kind::Open { .. }) {
            return Ok(());
        }
        let e = path.last().expect("open XML element");
        if path.len() == 1 && !xml::path_is(path, xml::MAIN, &["workbook"]) {
            return Err(unsupported("workbook XML root/namespace", "XLSX package"));
        }
        if [
            "workbook",
            "sheets",
            "sheet",
            "definedNames",
            "definedName",
            "workbookPr",
            "calcPr",
        ]
        .contains(&e.local.as_str())
            && e.ns != xml::MAIN
        {
            return Err(unsupported("foreign workbook metadata lookalike", &e.local));
        }
        if matches!(e.local.as_str(), "sheets" | "definedNames" | "calcPr")
            && (!xml::path_is(path, xml::MAIN, &["workbook", e.local.as_str()])
                || !metadata_sections.insert(e.local.clone()))
        {
            return Err(unsupported(
                "duplicate/misplaced workbook metadata",
                "workbook XML",
            ));
        }
        if e.local == "definedName" {
            if !xml::path_is(
                path,
                xml::MAIN,
                &["workbook", "definedNames", "definedName"],
            ) {
                return Err(unsupported("misplaced defined name", "workbook XML"));
            }
            let scope = node
                .value("localSheetId")
                .map(str::parse::<usize>)
                .transpose()
                .map_err(|_| unsupported("invalid defined-name scope", "workbook XML"))?;
            if !defined_names.insert((scope, node.required("name")?.to_ascii_lowercase())) {
                return Err(unsupported("duplicate defined name", "workbook XML"));
            }
        }
        if e.local == "workbookPr" {
            if workbook_pr || !xml::path_is(path, xml::MAIN, &["workbook", "workbookPr"]) {
                return Err(unsupported(
                    "duplicate/misplaced workbookPr",
                    "workbook XML",
                ));
            }
            workbook_pr = true;
            epoch = match node.value("date1904") {
                None | Some("0" | "false") => formualizer_common::DateSystem::Excel1900,
                Some("1" | "true") => formualizer_common::DateSystem::Excel1904,
                _ => return Err(unsupported("invalid workbook date system", "workbook XML")),
            };
        }
        if e.local == "externalReferences" {
            return Err(unsupported("external workbook references", "workbook XML"));
        }
        if e.local == "sheet" {
            if !xml::path_is(path, xml::MAIN, &["workbook", "sheets", "sheet"]) {
                return Err(unsupported("misplaced sheet declaration", "workbook XML"));
            }
            let sheet_id = node
                .required("sheetId")?
                .parse::<u32>()
                .map_err(|_| unsupported("invalid sheet ID", "workbook XML"))?;
            if sheet_id == 0 || !sheet_ids.insert(sheet_id) {
                return Err(unsupported("duplicate/invalid sheet ID", "workbook XML"));
            }
            let name = node.required("name")?;
            if !names.insert(name.to_lowercase()) {
                return Err(unsupported("duplicate sheet name", "workbook XML"));
            }
            let id = node
                .attribute(xml::OFFICE, "id")
                .ok_or_else(|| unsupported("missing sheet relationship", "workbook XML"))?;
            if id.qualified != "r:id" {
                return Err(unsupported(
                    "adapter requires r:id sheet attribute",
                    "workbook XML",
                ));
            }
            let rel = relations
                .get(&id.value)
                .ok_or_else(|| unsupported("missing worksheet relationship", "workbook XML"))?;
            if rel.kind != format!("{}/worksheet", xml::OFFICE) {
                return Err(unsupported("non-worksheet sheet", "workbook XML"));
            }
            let part = rel
                .target
                .clone()
                .ok_or_else(|| unsupported("external worksheet", "workbook XML"))?;
            if !targets.insert(part.clone()) {
                return Err(unsupported("duplicate worksheet target", "workbook XML"));
            }
            sheets.push(Sheet {
                name: name.to_owned(),
                part,
            });
        }
        Ok(())
    })?;
    if sheets.is_empty() {
        return Err(unsupported("workbook without worksheets", "XLSX package"));
    }
    if defined_names
        .iter()
        .any(|(scope, _)| scope.is_some_and(|i| i >= sheets.len()))
    {
        return Err(unsupported(
            "out-of-range defined-name scope",
            "workbook XML",
        ));
    }
    for (name, root) in [
        ("xl/styles.xml", "styleSheet"),
        ("xl/sharedStrings.xml", "sst"),
    ] {
        if archive.file_names().any(|n| n == name) {
            let kind = if root == "sst" {
                "sharedStrings"
            } else {
                "styles"
            };
            if !relations.values().any(|r| {
                r.kind == format!("{}/{kind}", xml::OFFICE) && r.target.as_deref() == Some(name)
            }) {
                return Err(unsupported("unrelated adapter metadata part", name));
            }
            validate_aux(archive, name, root, options)?;
        }
    }
    for sheet in &sheets {
        let (parent, name) = sheet.part.rsplit_once('/').unwrap_or(("", &sheet.part));
        let rel_part = format!("{parent}/_rels/{name}.rels");
        if archive.file_names().any(|n| n == rel_part) {
            for rel in relationships(archive, &sheet.part, options)?.values() {
                if rel.kind == format!("{}/table", xml::OFFICE) {
                    // The existing CalamineAdapter does not hydrate the engine
                    // table registry. Preserving XML alone would silently make
                    // valid structured references evaluate against missing data.
                    return Err(unsupported(
                        "table metadata ingestion is not supported",
                        "cache-only recalculation",
                    ));
                }
            }
        }
    }
    content_types::validate(archive, &sheets, options)?;
    Ok((sheets, epoch))
}
fn validate_aux(
    archive: &mut Archive<'_>,
    part: &str,
    root: &str,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    let data = read_part(archive, part, options.limits.max_worksheet_bytes)?;
    xml::walk(&data, options, |path, node| {
        if let xml::Kind::Open { .. } = node.kind {
            let e = path.last().expect("open XML element");
            if path.len() == 1 && (e.ns != xml::MAIN || e.local != root) {
                return Err(unsupported("auxiliary XML root/namespace", part));
            }
            if [
                "numFmt",
                "xf",
                "si",
                "t",
                "r",
                "table",
                "tableColumn",
                "tableColumns",
            ]
            .contains(&e.local.as_str())
                && e.ns != xml::MAIN
            {
                return Err(unsupported("foreign adapter metadata lookalike", part));
            }
            if matches!(
                e.local.as_str(),
                "calculatedColumnFormula" | "totalsRowFormula"
            ) {
                return Err(unsupported("table-managed formula metadata", part));
            }
        }
        Ok(())
    })
}
