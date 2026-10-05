//! Bounded package admission and relationship-aware workbook discovery.
mod content_types;
mod rewrite;
use super::{IoError, XlsxRecalculateOptions, checkpoint, unsupported, xml};
pub(super) use content_types::add_override;
pub(super) use rewrite::{Edits, rewrite};
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
    pub tables: BTreeMap<String, String>,
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
/// One audited ZIP32 member, in central-directory order.
#[derive(Debug)]
pub(super) struct Member {
    /// Offset and length of the central-directory record.
    pub central: usize,
    pub central_len: usize,
    /// Offsets of the local header, the payload, the payload end and the
    /// member end (after its data descriptor, if any).
    pub local: usize,
    pub data: usize,
    pub data_end: usize,
    pub end: usize,
}
/// The audited directory: its start, its end record and every member.
#[derive(Debug)]
pub(super) struct Directory {
    pub start: usize,
    pub footer: usize,
    pub members: Vec<Member>,
}
/// General-purpose flags admitted: deflate option bits 1-2, data
/// descriptor (bit 3) and UTF-8 names (bit 11).
const ADMITTED_FLAGS: usize = 0x0002 | 0x0004 | 0x0008 | 0x0800;
/// Encryption (bit 0), strong encryption (bit 6), masked directory (bit 13).
const ENCRYPTION_FLAGS: usize = 0x0001 | 0x0040 | 0x2000;
fn le16(bytes: &[u8], offset: usize) -> usize {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]]) as usize
}
/// Microsoft packaging growth hint (Excel, System.IO.Packaging): signature
/// 0xA028, the initially requested padding size (any value; Excel writes
/// the padding length or 0) and zero padding bytes.
fn growth_hint(data: &[u8]) -> bool {
    data.len() >= 4 && le16(data, 0) == 0xA028 && data[4..].iter().all(|b| *b == 0)
}
/// Info-ZIP extended timestamp: a flag byte (bits 0-2: mtime, atime,
/// ctime) and one 32-bit time per flag in local headers; central records
/// carry the modification time only, or every flagged time.
fn extended_timestamp(data: &[u8], central: bool) -> bool {
    let Some(&flags) = data.first() else {
        return false;
    };
    let all = 1 + 4 * (flags & 7).count_ones() as usize;
    flags & !7 == 0
        && (data.len() == all || (central && data.len() == 1 + 4 * (flags & 1) as usize))
}
/// Info-ZIP Unix UID/GID ("ux", version 1): sized UID, sized GID.
fn unix_owner(data: &[u8]) -> bool {
    let mut at = 1;
    if data.first() != Some(&1) {
        return false;
    }
    for _ in 0..2 {
        let Some(&size) = data.get(at) else {
            return false;
        };
        if !(1..=8).contains(&size) {
            return false;
        }
        at += 1 + size as usize;
    }
    at == data.len()
}
/// NTFS times: a zero reserved word and at most one attribute, tag 1 with
/// three 64-bit times.
fn ntfs_times(data: &[u8]) -> bool {
    data.len() >= 4
        && data[..4] == [0; 4]
        && (data.len() == 4 || (data.len() == 32 && le16(data, 4) == 1 && le16(data, 6) == 24))
}
/// Admit only metadata-only extra fields that do not change how member data
/// is located or decoded; every block is parsed in full.
fn extra_fields(block: &[u8], central: bool, name: &str) -> Result<(), IoError> {
    let mut seen = Vec::new();
    let mut at = 0;
    while at < block.len() {
        let malformed = || unsupported("malformed ZIP extra field", name);
        let header = block.get(at..at + 4).ok_or_else(malformed)?;
        let id = le16(header, 0);
        let data = block
            .get(at + 4..at + 4 + le16(header, 2))
            .ok_or_else(malformed)?;
        if seen.contains(&id) {
            return Err(unsupported("duplicate ZIP extra field", name));
        }
        seen.push(id);
        let valid = match id {
            0xA220 => growth_hint(data),
            0x5455 => extended_timestamp(data, central),
            0x7875 => unix_owner(data),
            0x000A => ntfs_times(data),
            0x0001 => return Err(unsupported("ZIP64 member", name)),
            0x9901 => return Err(unsupported("encrypted ZIP member", name)),
            _ => {
                return Err(unsupported(
                    format!("unsupported ZIP extra field 0x{id:04X}"),
                    name,
                ));
            }
        };
        if !valid {
            return Err(unsupported(
                format!("malformed ZIP extra field 0x{id:04X}"),
                name,
            ));
        }
        at += 4 + data.len();
    }
    Ok(())
}
/// The single end-of-central-directory record whose comment ends the input.
fn footer(bytes: &[u8]) -> Result<usize, IoError> {
    let mut footers = Vec::new();
    for at in bytes.len().saturating_sub(65_557)..bytes.len().saturating_sub(21) {
        if bytes.get(at..at + 4) == Some(b"PK\x05\x06")
            && at + 22 + u16_at(bytes, at + 20)? == bytes.len()
        {
            footers.push(at);
        }
    }
    match footers[..] {
        [at] => Ok(at),
        _ => Err(unsupported("ambiguous/missing ZIP footer", "XLSX package")),
    }
}
/// Length of the data descriptor after `data_end`: 16 bytes with the
/// optional signature or 12 without. Its CRC-32 and 32-bit sizes must equal
/// the central record's; a ZIP64 descriptor never matches.
fn descriptor(bytes: &[u8], data_end: usize, central: &[u8], name: &str) -> Result<usize, IoError> {
    let signed = bytes
        .get(data_end..data_end + 16)
        .is_some_and(|d| d[..4] == *b"PK\x07\x08" && d[4..] == *central);
    let unsigned = bytes.get(data_end..data_end + 12) == Some(central);
    match (signed, unsigned) {
        (true, false) => Ok(16),
        (false, true) => Ok(12),
        (true, true) => Err(unsupported("ambiguous ZIP data descriptor", name)),
        (false, false) => Err(unsupported("ZIP data descriptor mismatch", name)),
    }
}
// ZIP7 indexes members by name and can hide duplicate central-directory names.
// This is a bounded metadata audit, NOT a ZIP writer/decoder. It runs before
// ZIP7 parses the directory. The central directory is authoritative; local
// headers and data descriptors must agree with it, and members must tile the
// bytes before the directory. ZIP64, encryption, entry comments, split
// archives and extra fields outside a metadata-only allow-list are refused.
pub(super) fn audit_directory(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
) -> Result<Directory, IoError> {
    let footer = footer(bytes)?;
    let count = u16_at(bytes, footer + 10)?;
    if count > options.limits.max_entries || count == u16::MAX as usize {
        return Err(unsupported(
            "ZIP entry count limit or ZIP64",
            "XLSX package",
        ));
    }
    let start = u32_at(bytes, footer + 16)?;
    if start.checked_add(u32_at(bytes, footer + 12)?) != Some(footer) {
        return Err(unsupported(
            "ZIP64 or inconsistent directory extent",
            "XLSX package",
        ));
    }
    if u16_at(bytes, footer + 4)? != 0
        || u16_at(bytes, footer + 6)? != 0
        || u16_at(bytes, footer + 8)? != count
    {
        return Err(unsupported(
            "inconsistent ZIP directory/footer",
            "XLSX package",
        ));
    }
    let mut at = start;
    let mut names = HashSet::new();
    let mut members = Vec::new();
    while bytes.get(at..at + 4) == Some(b"PK\x01\x02") {
        checkpoint(&options.cancel)?;
        let fixed_end = checked_end(at, 46, footer)?;
        let name_len = u16_at(bytes, at + 28)?;
        let extra_len = u16_at(bytes, at + 30)?;
        let comment_len = u16_at(bytes, at + 32)?;
        let end = checked_end(fixed_end, name_len + extra_len + comment_len, footer)?;
        let raw_name = &bytes[fixed_end..fixed_end + name_len];
        let name = std::str::from_utf8(raw_name)
            .map_err(|_| unsupported("non-UTF-8 ZIP name", "XLSX package"))?;
        part_name(name)?;
        if !names.insert(name) {
            return Err(unsupported("duplicate ZIP member", "XLSX package"));
        }
        if names.len() > count {
            return Err(unsupported(
                "inconsistent ZIP directory/footer",
                "XLSX package",
            ));
        }
        if comment_len != 0 {
            return Err(unsupported("ZIP entry comment", name));
        }
        if u16_at(bytes, at + 34)? != 0 {
            return Err(unsupported("split ZIP archive", name));
        }
        let flags = u16_at(bytes, at + 8)?;
        if flags & ENCRYPTION_FLAGS != 0 {
            return Err(unsupported("encrypted ZIP member", name));
        }
        if flags & !ADMITTED_FLAGS != 0 {
            return Err(unsupported("unsupported ZIP general-purpose flags", name));
        }
        if !matches!(u16_at(bytes, at + 10)?, 0 | 8) {
            return Err(unsupported("unsupported ZIP compression method", name));
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
        extra_fields(
            &bytes[fixed_end + name_len..fixed_end + name_len + extra_len],
            true,
            name,
        )?;
        checked_end(local, 30, start)?;
        if bytes.get(local..local + 4) != Some(b"PK\x03\x04") {
            return Err(unsupported("invalid ZIP local header", name));
        }
        let local_name_len = u16_at(bytes, local + 26)?;
        let local_extra = u16_at(bytes, local + 28)?;
        let data = checked_end(local + 30, local_name_len + local_extra, start)?;
        if bytes.get(local + 30..local + 30 + local_name_len) != Some(raw_name)
            || u16_at(bytes, local + 4)? != u16_at(bytes, at + 6)?
            || u16_at(bytes, local + 6)? != flags
            || bytes[local + 8..local + 14] != bytes[at + 10..at + 16]
        {
            return Err(unsupported("inconsistent ZIP local/central metadata", name));
        }
        extra_fields(&bytes[data - local_extra..data], false, name)?;
        let central = &bytes[at + 16..at + 28];
        let data_end = checked_end(data, compressed, start)?;
        let member_end = if flags & 8 == 0 {
            if bytes[local + 14..local + 26] != *central {
                return Err(unsupported("inconsistent ZIP local/central metadata", name));
            }
            data_end
        } else {
            // A deferred CRC/size field is zero or already final.
            if (0..3).any(|i| {
                let field = &bytes[local + 14 + 4 * i..local + 18 + 4 * i];
                field != [0; 4] && field != &central[4 * i..4 * i + 4]
            }) {
                return Err(unsupported("inconsistent ZIP local/central metadata", name));
            }
            let length = descriptor(bytes, data_end, central, name)?;
            checked_end(data_end, length, start)?
        };
        members.push(Member {
            central: at,
            central_len: end - at,
            local,
            data,
            data_end,
            end: member_end,
        });
        at = end;
    }
    if at != footer || members.len() != count {
        return Err(unsupported(
            "inconsistent ZIP directory/footer",
            "XLSX package",
        ));
    }
    // Members tile the bytes before the directory: no overlap, no gap.
    let mut ranges: Vec<_> = members.iter().map(|m| (m.local, m.end)).collect();
    ranges.sort_unstable();
    let mut next = 0;
    for (local, end) in ranges {
        if local < next {
            return Err(unsupported("overlapping ZIP members", "XLSX package"));
        }
        if local > next {
            return Err(unsupported("unaccounted ZIP bytes", "XLSX package"));
        }
        next = end;
    }
    if next != start {
        return Err(unsupported("unaccounted ZIP bytes", "XLSX package"));
    }
    Ok(Directory {
        start,
        footer,
        members,
    })
}
/// Parse the audited directory with ZIP7 and confirm it agrees with the audit.
fn open<'a>(bytes: &'a [u8], directory: &Directory) -> Result<Archive<'a>, IoError> {
    let archive =
        ZipArchive::new(Cursor::new(bytes)).map_err(|e| IoError::from_backend("zip", e))?;
    if archive.offset() != 0
        || archive.len() != directory.members.len()
        || usize::try_from(archive.central_directory_start()).ok() != Some(directory.start)
    {
        return Err(unsupported(
            "prefixed or inconsistent ZIP archive",
            "XLSX package",
        ));
    }
    Ok(archive)
}
/// Sum of the members' declared expanded sizes (the audit bounds each to
/// ZIP32; admission verified them against the decoded bytes).
pub(super) fn expanded_size(archive: &mut Archive<'_>) -> Result<usize, IoError> {
    let mut total = 0usize;
    for i in 0..archive.len() {
        let size = archive
            .by_index_raw(i)
            .map_err(|e| IoError::from_backend("zip", e))?
            .size();
        total = usize::try_from(size)
            .ok()
            .and_then(|size| total.checked_add(size))
            .ok_or_else(|| unsupported("ZIP expanded-size overflow", "workbook"))?;
    }
    Ok(total)
}
pub(super) fn admit<'a>(
    bytes: &'a [u8],
    options: &XlsxRecalculateOptions,
) -> Result<Archive<'a>, IoError> {
    checkpoint(&options.cancel)?;
    if bytes.len() > options.limits.max_input_bytes {
        return Err(unsupported("input byte limit", "XLSX package"));
    }
    // Bound and audit the declared directory before ZIP7 allocates its
    // member index or parses extra fields.
    let directory = audit_directory(bytes, options)?;
    let mut archive = open(bytes, &directory)?;
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
        if file.name().starts_with("xl/externalLinks/") || file.name().starts_with("xl/richData/") {
            return Err(unsupported(
                "external links or rich value data",
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
/// Excel 2013 (x15) SpreadsheetML extension namespace.
const X15: &str = "http://schemas.microsoft.com/office/spreadsheetml/2010/11/main";
/// The workbook `ext` URI under which Excel 2013+ writes `x15:workbookPr`.
const X15_WORKBOOK_PR_EXT: &str = "{140A7094-0E35-4892-8432-C4D2E57EDEB5}";
/// Excel 2013+ writes `<x15:workbookPr chartTrackingRefBase="1"/>` in the
/// workbook `extLst`. Calamine matches `workbookPr` by local name and resets
/// its own 1904 flag from it, but that flag only reaches
/// `ExcelDateTime::is_1904`, which ingestion never reads: values are taken as
/// raw serials and the date system comes from the main `workbookPr` here.
/// Admit exactly Excel's form, once; refuse any other position, extension
/// URI or attribute (a `date1904` here would make the readers disagree).
fn x15_workbook_pr(
    path: &[xml::Element],
    node: &xml::Node,
    ext_uri: Option<&str>,
    seen: &mut bool,
) -> Result<(), IoError> {
    let placed = path.len() == 4
        && xml::path_is(&path[..3], xml::MAIN, &["workbook", "extLst", "ext"])
        && ext_uri.is_some_and(|u| u.eq_ignore_ascii_case(X15_WORKBOOK_PR_EXT));
    if !placed || std::mem::replace(seen, true) {
        return Err(unsupported(
            "foreign workbook metadata lookalike",
            "misplaced or duplicate x15:workbookPr",
        ));
    }
    if let xml::Kind::Open { attributes, .. } = &node.kind {
        for a in attributes {
            if !(a.ns.is_empty()
                && a.local == "chartTrackingRefBase"
                && matches!(a.value.as_str(), "0" | "1" | "false" | "true"))
            {
                return Err(unsupported(
                    "foreign workbook metadata lookalike",
                    format!("x15:workbookPr attribute {}", a.qualified),
                ));
            }
        }
    }
    Ok(())
}
/// Workbook discovery. Also returns the single relationship-resolved sheet
/// metadata part, if any.
pub(super) fn discover(
    archive: &mut Archive<'_>,
    options: &XlsxRecalculateOptions,
) -> Result<(Vec<Sheet>, formualizer_common::DateSystem, Option<String>), IoError> {
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
    let metadata = sheet_metadata_part(archive, &relations)?;
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
    let mut ext_uri: Option<String> = None;
    let mut x15_seen = false;
    let mut metadata_sections = HashSet::new();
    let mut defined_names = HashSet::new();
    let mut sheet_ids = HashSet::new();
    #[cfg(not(feature = "system-clock"))]
    let mut clock_name: Option<(String, String)> = None;
    xml::walk(&data, options, |path, node| {
        #[cfg(not(feature = "system-clock"))]
        if xml::path_is(
            path,
            xml::MAIN,
            &["workbook", "definedNames", "definedName"],
        ) {
            match &node.kind {
                xml::Kind::Open { empty: false, .. } => {
                    clock_name = Some((node.required("name")?.to_owned(), String::new()))
                }
                xml::Kind::Text(text) => {
                    if let Some((_, formula)) = &mut clock_name {
                        formula.push_str(text);
                    }
                }
                xml::Kind::Close => {
                    if let Some((name, formula)) = clock_name.take() {
                        super::wall_clock_guard::validate(
                            &formula,
                            &format!("defined name {name}"),
                            options,
                        )?;
                    }
                }
                _ => {}
            }
        }
        if !matches!(node.kind, xml::Kind::Open { .. }) {
            return Ok(());
        }
        let e = path.last().expect("open XML element");
        if path.len() == 1 && !xml::path_is(path, xml::MAIN, &["workbook"]) {
            return Err(unsupported("workbook XML root/namespace", "XLSX package"));
        }
        if xml::path_is(path, xml::MAIN, &["workbook", "extLst", "ext"]) {
            ext_uri = node.value("uri").map(str::to_owned);
        }
        if e.ns == X15 && e.local == "workbookPr" {
            x15_workbook_pr(path, &node, ext_uri.as_deref(), &mut x15_seen)?;
            return Ok(());
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
                tables: BTreeMap::new(),
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
    let mut table_targets = HashSet::new();
    for sheet in &mut sheets {
        let (parent, name) = sheet.part.rsplit_once('/').unwrap_or(("", &sheet.part));
        let rel_part = format!("{parent}/_rels/{name}.rels");
        if archive.file_names().any(|n| n == rel_part) {
            for (id, rel) in relationships(archive, &sheet.part, options)? {
                if rel.kind == format!("{}/queryTable", xml::OFFICE) {
                    return Err(unsupported(
                        "connection-backed worksheet table",
                        &sheet.part,
                    ));
                }
                if rel.kind == format!("{}/table", xml::OFFICE) {
                    let target = rel
                        .target
                        .ok_or_else(|| unsupported("external table relationship", &sheet.part))?;
                    if !archive.file_names().any(|n| n == target)
                        || !table_targets.insert(target.clone())
                    {
                        return Err(unsupported("missing/duplicate table target", &sheet.part));
                    }
                    sheet.tables.insert(id, target);
                }
            }
        }
    }
    content_types::validate(archive, &sheets, metadata.as_deref(), options)?;
    Ok((sheets, epoch, metadata))
}
/// The workbook relationship part.
pub(super) const WORKBOOK_RELS: &str = "xl/_rels/workbook.xml.rels";
/// Append one `Relationship` element to a validated relationship part.
/// The new `Id` is `rId<n>` with `n` above every existing numeric `rIdN`,
/// so it collides with no existing ID; all other bytes are kept.
pub(super) fn add_relationship(
    archive: &mut Archive<'_>,
    source: &str,
    kind: &str,
    target: &str,
    options: &XlsxRecalculateOptions,
) -> Result<Vec<u8>, IoError> {
    let (parent, name) = source.rsplit_once('/').unwrap_or(("", source));
    let part = format!("{parent}/_rels/{name}.rels");
    let resolved = crate::xlsx_path::resolve(source, target)?;
    let existing = relationships(archive, source, options)?;
    if existing
        .values()
        .any(|r| r.kind == kind || r.target.as_deref() == Some(resolved.as_str()))
    {
        return Err(unsupported("duplicate package relationship", &part));
    }
    let next = existing
        .keys()
        .filter_map(|id| id.strip_prefix("rId")?.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| unsupported("relationship ID overflow", &part))?;
    let id = format!("rId{next}");
    if existing.contains_key(&id) {
        return Err(unsupported("relationship ID collision", &part));
    }
    let data = read_part(archive, &part, options.limits.max_worksheet_bytes)?;
    let element = |prefix: &str| {
        format!("<{prefix}Relationship Id=\"{id}\" Type=\"{kind}\" Target=\"{target}\"/>")
    };
    append_child(&data, &part, element, options)
}
/// Insert one child element at the end of the root element (expanding a
/// self-closing root), in the root's prefix. Every other byte is kept.
pub(super) fn append_child(
    data: &[u8],
    part: &str,
    element: impl FnOnce(&str) -> String,
    options: &XlsxRecalculateOptions,
) -> Result<Vec<u8>, IoError> {
    let mut root: Option<(String, std::ops::Range<usize>, bool)> = None;
    let mut close = None;
    xml::walk(data, options, |path, node| {
        match node.kind {
            xml::Kind::Open { empty, .. } if path.len() == 1 => {
                root = Some((path[0].qualified.clone(), node.span.clone(), empty));
            }
            xml::Kind::Close if path.len() == 1 => close = Some(node.span.start),
            _ => {}
        }
        Ok(())
    })?;
    let (qualified, open, empty) = root.ok_or_else(|| unsupported("missing XML root", part))?;
    let prefix = qualified
        .rsplit_once(':')
        .map_or(String::new(), |(p, _)| format!("{p}:"));
    let child = element(&prefix);
    let patch = if empty {
        super::Patch {
            span: open.end - 2..open.end,
            replacement: format!(">{child}</{qualified}>").into_bytes(),
        }
    } else {
        let at = close.ok_or_else(|| unsupported("unbalanced XML root", part))?;
        super::Patch {
            span: at..at,
            replacement: child.into_bytes(),
        }
    };
    super::apply_patches(data, vec![patch], options.limits.max_worksheet_bytes)
}
/// Re-audit a package that gained members: the bounded ZIP directory audit,
/// workbook discovery with its relationship/content-type agreement and the
/// sheet metadata part, read in full (CRC-checked) and parsed.
pub(super) fn check_output(bytes: &[u8], options: &XlsxRecalculateOptions) -> Result<(), IoError> {
    let directory = audit_directory(bytes, options)?;
    let mut archive = open(bytes, &directory)?;
    let (_, _, metadata) = discover(&mut archive, options)?;
    let part =
        metadata.ok_or_else(|| unsupported("unrelated added metadata part", "XLSX output"))?;
    super::dynamic_metadata::parse(&mut archive, &part, options)?;
    Ok(())
}
/// Exactly zero or one internal sheetMetadata relationship whose target
/// exists; no metadata member may exist without that relationship.
fn sheet_metadata_part(
    archive: &Archive<'_>,
    relations: &BTreeMap<String, Relationship>,
) -> Result<Option<String>, IoError> {
    use super::dynamic_metadata::SHEET_METADATA_RELATIONSHIP;
    let mut part = None;
    for rel in relations
        .values()
        .filter(|r| r.kind == SHEET_METADATA_RELATIONSHIP)
    {
        let target = rel.target.clone().ok_or_else(|| {
            unsupported(
                "external sheet metadata relationship",
                "workbook relationships",
            )
        })?;
        if part.replace(target).is_some() {
            return Err(unsupported(
                "duplicate sheet metadata relationship",
                "workbook relationships",
            ));
        }
    }
    if let Some(name) = &part
        && !archive.file_names().any(|n| n == name)
    {
        return Err(unsupported("missing sheet metadata part", name));
    }
    if archive.file_names().any(|n| n == "xl/metadata.xml")
        && part.as_deref() != Some("xl/metadata.xml")
    {
        return Err(unsupported(
            "unrelated sheet metadata part",
            "xl/metadata.xml",
        ));
    }
    Ok(part)
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
