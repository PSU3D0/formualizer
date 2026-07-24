//! Experimental, bounded S0/S1 calculation-package feasibility surface.
//!
//! This module is intentionally feature-gated. `FZCP_S1_EXPERIMENTAL` is a
//! deterministic canonical blob, not a production format and not an XLSX
//! round-trip format. The S1 container is deliberately not ZIP: it is a fixed
//! ASCII header followed by compact canonical JSON and one LF. It contains a
//! whole modeled workbook; it performs no closure pruning. Determinism is
//! covered by independent in-process inspections/builds; fresh-process
//! determinism remains explicitly unproven by this unit-test spike.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use std::sync::atomic::{AtomicBool, Ordering};

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_eval::arrow_store::StoredCellValue;
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::engine::{
    CycleConfig, CycleDetection, CyclePolicy, DateSystem, RowVisibilitySource,
};
use formualizer_eval::reference::{CellRef, Coord, RangeRef};
use formualizer_parse::parser::ReferenceType;
use quick_xml::Reader as XmlReader;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::QName;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

use crate::traits::CalcSettings;
use crate::{IoError, Workbook, WorkbookConfig};

pub const FORMAT_VERSION: &str = "fzcp-s1-experimental-v0";
const BLOB_HEADER: &[u8] = b"FZCP_S1_EXPERIMENTAL\n";
const ENGINE_SEMANTIC_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryLimits {
    pub max_source_bytes: u64,
    pub max_manifest_bytes: u64,
    pub max_entries: usize,
    pub max_entry_uncompressed_bytes: u64,
    pub max_total_compressed_bytes: u64,
    pub max_total_uncompressed_bytes: u64,
    pub max_expansion_ratio: u64,
}

impl Default for DiscoveryLimits {
    fn default() -> Self {
        Self {
            max_source_bytes: 256 * 1024 * 1024,
            max_manifest_bytes: 1024 * 1024,
            max_entries: 4096,
            max_entry_uncompressed_bytes: 64 * 1024 * 1024,
            max_total_compressed_bytes: 256 * 1024 * 1024,
            max_total_uncompressed_bytes: 512 * 1024 * 1024,
            max_expansion_ratio: 200,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Calamine,
    Umya,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendFidelity {
    pub backend: BackendKind,
    pub formulas: bool,
    pub canonical_formula_text: bool,
    pub range_defined_names: bool,
    pub literal_defined_names: bool,
    pub unsupported_defined_name_evidence: bool,
    pub tables: bool,
    pub date_system_1904: bool,
    pub hidden_sheets: bool,
    pub manual_hidden_rows: bool,
    pub filter_hidden_rows: bool,
    pub cached_formula_results: bool,
    pub stored_empty_text: bool,
}

impl BackendFidelity {
    pub fn for_backend(backend: BackendKind) -> Self {
        match backend {
            BackendKind::Calamine => Self {
                backend,
                formulas: true,
                canonical_formula_text: true,
                range_defined_names: true,
                literal_defined_names: false,
                unsupported_defined_name_evidence: false,
                tables: false,
                date_system_1904: false,
                hidden_sheets: false,
                manual_hidden_rows: false,
                filter_hidden_rows: false,
                cached_formula_results: false,
                stored_empty_text: false,
            },
            BackendKind::Umya => Self {
                backend,
                formulas: true,
                canonical_formula_text: true,
                range_defined_names: true,
                literal_defined_names: false,
                unsupported_defined_name_evidence: false,
                tables: true,
                date_system_1904: false,
                hidden_sheets: false,
                manual_hidden_rows: true,
                filter_hidden_rows: true,
                cached_formula_results: true,
                stored_empty_text: true,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinedNameSourceKind {
    Range,
    Literal,
    UnsupportedFormula,
    UnsupportedList,
    UnsupportedOther,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinedNameSource {
    pub name: String,
    pub local_sheet_id: Option<u32>,
    pub scope_sheet: Option<String>,
    pub definition: String,
    pub kind: DefinedNameSourceKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZipEntryFact {
    pub name: String,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalCategory {
    UnsupportedSource,
    LossyBackend,
    ActiveContent,
    ExternalDependency,
    ManifestIncompatible,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusalReason {
    pub category: RefusalCategory,
    pub code: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AdmissionDecision {
    Admitted,
    Refused { reasons: Vec<RefusalReason> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityReport {
    pub report_version: String,
    pub source_digest_sha256: String,
    pub manifest_digest_sha256: String,
    pub canonical_manifest_json: serde_json::Value,
    pub backend: BackendFidelity,
    pub entries: Vec<ZipEntryFact>,
    pub entry_count: usize,
    pub all_entry_paths_safe: bool,
    pub total_compressed_bytes: u64,
    pub total_uncompressed_bytes: u64,
    pub aggregate_expansion_ratio_milli: u64,
    pub max_entry_expansion_ratio_milli: u64,
    pub date_1904: bool,
    pub defined_names: Vec<DefinedNameSource>,
    pub formula_cells: u64,
    pub formula_cells_with_cached_results: u64,
    pub external_formula_references: u64,
    pub hidden_sheets: Vec<String>,
    /// OOXML row `hidden` facts; OOXML alone does not distinguish manual from filter provenance.
    pub source_hidden_rows: u64,
    pub table_parts: Vec<String>,
    pub table_part_count: usize,
    pub macros_or_vba_parts: Vec<String>,
    pub ole_parts: Vec<String>,
    pub external_link_parts: Vec<String>,
    pub external_relationship_targets: Vec<String>,
    pub connection_parts: Vec<String>,
    pub unmodeled_active_parts: Vec<String>,
    pub decision: AdmissionDecision,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DiscoveryError {
    #[error("discovery cancelled")]
    Cancelled,
    #[error("source byte limit exceeded: {actual} > {limit}")]
    SourceBytes { actual: u64, limit: u64 },
    #[error("manifest byte limit exceeded: {actual} > {limit}")]
    ManifestBytes { actual: u64, limit: u64 },
    #[error("ZIP entry count limit exceeded: {actual} > {limit}")]
    EntryCount { actual: usize, limit: usize },
    #[error("unsafe ZIP entry path: {name}")]
    UnsafePath { name: String },
    #[error("duplicate ZIP entry name: {name}")]
    DuplicateEntry { name: String },
    #[error("ZIP entry `{name}` uncompressed limit exceeded: {actual} > {limit}")]
    EntryUncompressed {
        name: String,
        actual: u64,
        limit: u64,
    },
    #[error("ZIP compressed byte limit exceeded: {actual} > {limit}")]
    TotalCompressed { actual: u64, limit: u64 },
    #[error("ZIP uncompressed byte limit exceeded: {actual} > {limit}")]
    TotalUncompressed { actual: u64, limit: u64 },
    #[error("ZIP expansion ratio limit exceeded for `{name}`")]
    ExpansionRatio { name: String },
    #[error("invalid XLSX ZIP: {0}")]
    Zip(String),
    #[error("invalid OOXML in `{entry}`: {message}")]
    Xml { entry: String, message: String },
    #[error("manifest cannot be canonicalized: {0}")]
    Manifest(String),
}

#[derive(Clone)]
struct EntryMeta {
    index: usize,
    name: String,
    compressed: u64,
    uncompressed: u64,
}

fn checkpoint(cancel: Option<&AtomicBool>) -> Result<(), DiscoveryError> {
    if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        Err(DiscoveryError::Cancelled)
    } else {
        Ok(())
    }
}

fn safe_entry_name(name: &str) -> bool {
    if name.is_empty()
        || name.starts_with('/')
        || name.starts_with('\\')
        || name.contains('\\')
        || name.contains('\0')
    {
        return false;
    }
    if name.as_bytes().get(1) == Some(&b':') {
        return false;
    }
    name.split('/').all(|part| !matches!(part, "" | "." | ".."))
        || (name.ends_with('/')
            && name[..name.len() - 1]
                .split('/')
                .all(|part| !matches!(part, "" | "." | "..")))
}

fn ratio_exceeds(uncompressed: u64, compressed: u64, limit: u64) -> bool {
    if uncompressed == 0 {
        return false;
    }
    if compressed == 0 {
        return true;
    }
    uncompressed > compressed.saturating_mul(limit)
}

fn ratio_milli(uncompressed: u64, compressed: u64) -> u64 {
    if uncompressed == 0 {
        0
    } else if compressed == 0 {
        u64::MAX
    } else {
        uncompressed.saturating_mul(1000) / compressed
    }
}

fn digest_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn reject_non_finite_yaml(value: &serde_yaml::Value) -> Result<(), DiscoveryError> {
    match value {
        serde_yaml::Value::Number(number)
            if number.as_f64().is_some_and(|value| !value.is_finite()) =>
        {
            Err(DiscoveryError::Manifest(
                "non-finite YAML numbers are not canonicalizable".to_string(),
            ))
        }
        serde_yaml::Value::Sequence(values) => values.iter().try_for_each(reject_non_finite_yaml),
        serde_yaml::Value::Mapping(values) => values.iter().try_for_each(|(key, value)| {
            reject_non_finite_yaml(key)?;
            reject_non_finite_yaml(value)
        }),
        serde_yaml::Value::Tagged(tagged) => reject_non_finite_yaml(&tagged.value),
        _ => Ok(()),
    }
}

fn canonical_manifest(bytes: &[u8]) -> Result<(serde_json::Value, Vec<u8>), DiscoveryError> {
    let yaml: serde_yaml::Value = serde_yaml::from_slice(bytes)
        .map_err(|error| DiscoveryError::Manifest(error.to_string()))?;
    reject_non_finite_yaml(&yaml)?;
    let value =
        serde_json::to_value(yaml).map_err(|error| DiscoveryError::Manifest(error.to_string()))?;
    let canonical =
        serde_json::to_vec(&value).map_err(|error| DiscoveryError::Manifest(error.to_string()))?;
    Ok((value, canonical))
}

fn attr(
    reader: &XmlReader<&[u8]>,
    start: &BytesStart<'_>,
    key: &[u8],
    entry: &str,
) -> Result<Option<String>, DiscoveryError> {
    for attribute in start.attributes() {
        let attribute = attribute.map_err(|error| DiscoveryError::Xml {
            entry: entry.to_string(),
            message: error.to_string(),
        })?;
        if attribute.key == QName(key) {
            return attribute
                .decode_and_unescape_value(reader.decoder())
                .map(|value| Some(value.into_owned()))
                .map_err(|error| DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                });
        }
    }
    Ok(None)
}

fn validate_attributes(
    reader: &XmlReader<&[u8]>,
    start: &BytesStart<'_>,
    entry: &str,
) -> Result<(), DiscoveryError> {
    for attribute in start.attributes() {
        let attribute = attribute.map_err(|error| DiscoveryError::Xml {
            entry: entry.to_string(),
            message: error.to_string(),
        })?;
        attribute
            .decode_and_unescape_value(reader.decoder())
            .map_err(|error| DiscoveryError::Xml {
                entry: entry.to_string(),
                message: error.to_string(),
            })?;
    }
    Ok(())
}

fn required_attr(
    reader: &XmlReader<&[u8]>,
    start: &BytesStart<'_>,
    key: &[u8],
    entry: &str,
) -> Result<String, DiscoveryError> {
    attr(reader, start, key, entry)?.ok_or_else(|| DiscoveryError::Xml {
        entry: entry.to_string(),
        message: format!(
            "missing required `{}` attribute",
            String::from_utf8_lossy(key)
        ),
    })
}

fn xml_bool(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "TRUE" | "True" | "on")
}

#[derive(Debug)]
struct WorkbookSheet {
    name: String,
    relationship_id: String,
}

#[derive(Debug)]
struct ParsedWorkbook {
    date_1904: bool,
    hidden_sheets: Vec<String>,
    defined_names: Vec<DefinedNameSource>,
    sheets: Vec<WorkbookSheet>,
}

fn parse_workbook_xml(bytes: &[u8], entry: &str) -> Result<ParsedWorkbook, DiscoveryError> {
    let mut reader = XmlReader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut inner = Vec::new();
    let mut date_1904 = false;
    let mut sheets = Vec::new();
    let mut hidden_sheets = Vec::new();
    let mut raw_names = Vec::new();

    loop {
        buffer.clear();
        let event = reader.read_event_into(&mut buffer);
        if let Ok(Event::Start(start) | Event::Empty(start)) = &event {
            validate_attributes(&reader, start, entry)?;
        }
        match event {
            Ok(Event::Empty(start)) | Ok(Event::Start(start))
                if start.local_name().as_ref() == b"workbookPr" =>
            {
                date_1904 = attr(&reader, &start, b"date1904", entry)?
                    .is_some_and(|value| xml_bool(&value));
            }
            Ok(Event::Empty(start)) | Ok(Event::Start(start))
                if start.local_name().as_ref() == b"sheet" =>
            {
                let name = required_attr(&reader, &start, b"name", entry)?;
                let relationship_id = required_attr(&reader, &start, b"r:id", entry)?;
                if attr(&reader, &start, b"state", entry)?.is_some_and(|state| state != "visible") {
                    hidden_sheets.push(name.clone());
                }
                sheets.push(WorkbookSheet {
                    name,
                    relationship_id,
                });
            }
            Ok(Event::Empty(start)) if start.local_name().as_ref() == b"definedName" => {
                let name = required_attr(&reader, &start, b"name", entry)?;
                let local_sheet_id = attr(&reader, &start, b"localSheetId", entry)?
                    .map(|value| {
                        value.parse::<u32>().map_err(|error| DiscoveryError::Xml {
                            entry: entry.to_string(),
                            message: format!("invalid localSheetId: {error}"),
                        })
                    })
                    .transpose()?;
                raw_names.push((name, local_sheet_id, String::new()));
            }
            Ok(Event::Start(start)) if start.local_name().as_ref() == b"definedName" => {
                let name = required_attr(&reader, &start, b"name", entry)?;
                let local_sheet_id = attr(&reader, &start, b"localSheetId", entry)?
                    .map(|value| {
                        value.parse::<u32>().map_err(|error| DiscoveryError::Xml {
                            entry: entry.to_string(),
                            message: format!("invalid localSheetId: {error}"),
                        })
                    })
                    .transpose()?;
                let mut definition = String::new();
                loop {
                    inner.clear();
                    let event = reader.read_event_into(&mut inner);
                    if let Ok(Event::Start(start) | Event::Empty(start)) = &event {
                        validate_attributes(&reader, start, entry)?;
                    }
                    match event {
                        Ok(Event::Text(text)) => {
                            definition.push_str(&text.xml10_content().map_err(|error| {
                                DiscoveryError::Xml {
                                    entry: entry.to_string(),
                                    message: error.to_string(),
                                }
                            })?)
                        }
                        Ok(Event::GeneralRef(entity)) => {
                            definition.push('&');
                            definition.push_str(&entity.decode().map_err(|error| {
                                DiscoveryError::Xml {
                                    entry: entry.to_string(),
                                    message: error.to_string(),
                                }
                            })?);
                            definition.push(';');
                        }
                        Ok(Event::End(end)) if end.local_name().as_ref() == b"definedName" => break,
                        Ok(Event::Eof) => {
                            return Err(DiscoveryError::Xml {
                                entry: entry.to_string(),
                                message: "unexpected EOF in definedName".to_string(),
                            });
                        }
                        Err(error) => {
                            return Err(DiscoveryError::Xml {
                                entry: entry.to_string(),
                                message: error.to_string(),
                            });
                        }
                        _ => {}
                    }
                }
                raw_names.push((name, local_sheet_id, definition));
            }
            Ok(Event::Eof) => break,
            Err(error) => {
                return Err(DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                });
            }
            _ => {}
        }
    }

    let sheet_names = sheets
        .iter()
        .map(|sheet| sheet.name.as_str())
        .collect::<Vec<_>>();
    let mut defined_names = raw_names
        .into_iter()
        .map(|(name, local_sheet_id, definition)| {
            let scope_sheet = local_sheet_id.and_then(|index| {
                sheet_names
                    .get(index as usize)
                    .map(|name| (*name).to_string())
            });
            let kind = classify_defined_name(&definition, scope_sheet.as_deref());
            DefinedNameSource {
                name,
                local_sheet_id,
                scope_sheet,
                definition,
                kind,
            }
        })
        .collect::<Vec<_>>();
    defined_names.sort_by(|a, b| {
        a.local_sheet_id
            .cmp(&b.local_sheet_id)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.definition.cmp(&b.definition))
    });
    hidden_sheets.sort();
    Ok(ParsedWorkbook {
        date_1904,
        hidden_sheets,
        defined_names,
        sheets,
    })
}

fn classify_defined_name(definition: &str, scope_sheet: Option<&str>) -> DefinedNameSourceKind {
    let mut value = definition.trim();
    if let Some(rest) = value.strip_prefix('=') {
        value = rest.trim();
    }
    if value.contains(',') {
        return DefinedNameSourceKind::UnsupportedList;
    }
    if value.eq_ignore_ascii_case("TRUE")
        || value.eq_ignore_ascii_case("FALSE")
        || value.parse::<f64>().is_ok()
        || (value.starts_with('"') && value.ends_with('"') && value.len() >= 2)
    {
        return DefinedNameSourceKind::Literal;
    }
    if let Ok(reference) = ReferenceType::from_string(value) {
        let has_sheet = match reference {
            ReferenceType::Cell { ref sheet, .. } | ReferenceType::Range { ref sheet, .. } => {
                sheet.is_some() || scope_sheet.is_some()
            }
            _ => false,
        };
        if has_sheet {
            return DefinedNameSourceKind::Range;
        }
    }
    if formualizer_parse::parser::parse(format!("={value}")).is_ok() {
        DefinedNameSourceKind::UnsupportedFormula
    } else {
        DefinedNameSourceKind::UnsupportedOther
    }
}

fn has_external_workbook_reference(formula: &str) -> bool {
    let bytes = formula.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'[' {
            let Some(relative_end) = bytes[index + 1..].iter().position(|byte| *byte == b']')
            else {
                return false;
            };
            let end = index + 1 + relative_end;
            let workbook = &formula[index + 1..end];
            let prefix_is_token_boundary = index == 0
                || !matches!(
                    bytes[index - 1],
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b']'
                );
            let suffix = &formula[end + 1..];
            let sheet_len = suffix
                .chars()
                .take_while(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '_' | '.' | ' ')
                })
                .map(char::len_utf8)
                .sum::<usize>();
            let suffix_has_sheet_bang =
                sheet_len > 0 && suffix.as_bytes().get(sheet_len) == Some(&b'!');
            if prefix_is_token_boundary
                && !workbook.is_empty()
                && workbook.bytes().all(|byte| byte.is_ascii_digit())
                && suffix_has_sheet_bang
            {
                return true;
            }
            if index > 0 && bytes[index - 1] == b'\'' {
                let suffix = &formula[end + 1..];
                if suffix.contains("'!") {
                    return true;
                }
            }
            index = end;
        }
        index += 1;
    }
    false
}

fn scan_worksheet_xml(bytes: &[u8], entry: &str) -> Result<(u64, u64, u64, u64), DiscoveryError> {
    let mut reader = XmlReader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut in_cell = false;
    let mut cell_formula = false;
    let mut cell_value = false;
    let mut in_formula = false;
    let mut formulas = 0u64;
    let mut cached = 0u64;
    let mut hidden_rows = 0u64;
    let mut external_formula_references = 0u64;

    loop {
        buffer.clear();
        let event = reader.read_event_into(&mut buffer);
        if let Ok(Event::Start(start) | Event::Empty(start)) = &event {
            validate_attributes(&reader, start, entry)?;
        }
        match event {
            Ok(Event::Start(start)) if start.local_name().as_ref() == b"c" => {
                in_cell = true;
                cell_formula = false;
                cell_value = false;
            }
            Ok(Event::Start(start)) if in_cell && start.local_name().as_ref() == b"f" => {
                cell_formula = true;
                in_formula = true;
            }
            Ok(Event::Empty(start)) if in_cell && start.local_name().as_ref() == b"f" => {
                cell_formula = true;
            }
            Ok(Event::Text(text)) if in_formula => {
                let formula = text.xml10_content().map_err(|error| DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                })?;
                if has_external_workbook_reference(&formula) {
                    external_formula_references = external_formula_references.saturating_add(1);
                }
            }
            Ok(Event::End(end)) if end.local_name().as_ref() == b"f" => {
                in_formula = false;
            }
            Ok(Event::Start(start)) | Ok(Event::Empty(start))
                if in_cell && start.local_name().as_ref() == b"v" =>
            {
                cell_value = true;
            }
            Ok(Event::End(end)) if end.local_name().as_ref() == b"c" => {
                if cell_formula {
                    formulas = formulas.saturating_add(1);
                    if cell_value {
                        cached = cached.saturating_add(1);
                    }
                }
                in_cell = false;
            }
            Ok(Event::Empty(start)) | Ok(Event::Start(start))
                if start.local_name().as_ref() == b"row" =>
            {
                if attr(&reader, &start, b"hidden", entry)?.is_some_and(|v| xml_bool(&v)) {
                    hidden_rows = hidden_rows.saturating_add(1);
                }
            }
            Ok(Event::Eof) => break,
            Err(error) => {
                return Err(DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                });
            }
            _ => {}
        }
    }
    Ok((formulas, cached, hidden_rows, external_formula_references))
}

#[derive(Clone, Debug)]
struct PackageRelationship {
    id: String,
    relationship_type: String,
    target: String,
    external: bool,
}

fn parse_relationships(
    bytes: &[u8],
    entry: &str,
) -> Result<Vec<PackageRelationship>, DiscoveryError> {
    let mut reader = XmlReader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut relationships = Vec::new();
    loop {
        buffer.clear();
        let event = reader.read_event_into(&mut buffer);
        if let Ok(Event::Start(start) | Event::Empty(start)) = &event {
            validate_attributes(&reader, start, entry)?;
        }
        match event {
            Ok(Event::Empty(start)) | Ok(Event::Start(start))
                if start.local_name().as_ref() == b"Relationship" =>
            {
                relationships.push(PackageRelationship {
                    id: required_attr(&reader, &start, b"Id", entry)?,
                    relationship_type: required_attr(&reader, &start, b"Type", entry)?,
                    target: required_attr(&reader, &start, b"Target", entry)?,
                    external: attr(&reader, &start, b"TargetMode", entry)?
                        .is_some_and(|mode| mode.eq_ignore_ascii_case("external")),
                });
            }
            Ok(Event::Eof) => break,
            Err(error) => {
                return Err(DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                });
            }
            _ => {}
        }
    }
    Ok(relationships)
}

fn relationship_part_name(source_part: &str) -> String {
    match source_part.rsplit_once('/') {
        Some((directory, file)) => format!("{directory}/_rels/{file}.rels"),
        None => format!("_rels/{source_part}.rels"),
    }
}

fn resolve_relationship_target(source_part: &str, target: &str) -> Option<String> {
    if target.is_empty()
        || target.starts_with('/')
        || target.starts_with('\\')
        || target.contains('\\')
        || target.contains('\0')
        || target.contains('?')
        || target.contains('#')
        || target.contains("://")
    {
        return None;
    }
    let mut parts = source_part
        .rsplit_once('/')
        .map(|(directory, _)| directory.split('/').collect::<Vec<_>>())
        .unwrap_or_default();
    for part in target.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            value if value.as_bytes().get(1) == Some(&b':') => return None,
            value => parts.push(value),
        }
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

fn relationship_kind(relationship_type: &str) -> &str {
    relationship_type
        .rsplit('/')
        .next()
        .unwrap_or(relationship_type)
}

fn validate_xml_document(bytes: &[u8], entry: &str) -> Result<(), DiscoveryError> {
    let mut reader = XmlReader::from_reader(bytes);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let event = reader.read_event_into(&mut buffer);
        if let Ok(Event::Start(start) | Event::Empty(start)) = &event {
            validate_attributes(&reader, start, entry)?;
        }
        match event {
            Ok(Event::Text(text)) => {
                text.xml10_content().map_err(|error| DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                })?;
            }
            Ok(Event::GeneralRef(entity)) => {
                entity.decode().map_err(|error| DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                })?;
            }
            Ok(Event::Eof) => return Ok(()),
            Err(error) => {
                return Err(DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                });
            }
            _ => {}
        }
    }
}

fn parse_content_types(bytes: &[u8], entry: &str) -> Result<bool, DiscoveryError> {
    let mut reader = XmlReader::from_reader(bytes);
    let mut buffer = Vec::new();
    let mut macro_content_type = false;
    loop {
        buffer.clear();
        let event = reader.read_event_into(&mut buffer);
        if let Ok(Event::Start(start) | Event::Empty(start)) = &event {
            validate_attributes(&reader, start, entry)?;
        }
        match event {
            Ok(Event::Empty(start)) | Ok(Event::Start(start))
                if matches!(start.local_name().as_ref(), b"Default" | b"Override") =>
            {
                let content_type = required_attr(&reader, &start, b"ContentType", entry)?;
                let lower = content_type.to_ascii_lowercase();
                macro_content_type |= lower.contains("macroenabled")
                    || lower.contains("vbaproject")
                    || lower.contains("application/vnd.ms-office.vba");
            }
            Ok(Event::Eof) => break,
            Err(error) => {
                return Err(DiscoveryError::Xml {
                    entry: entry.to_string(),
                    message: error.to_string(),
                });
            }
            _ => {}
        }
    }
    Ok(macro_content_type)
}

/// Explicitly understood calculation-inert/modelled OOXML parts. Workbook,
/// worksheet, and their relationship parts are admitted separately after
/// relationship resolution, so relocation does not weaken this allow-list.
fn statically_allowed_part(name: &str) -> bool {
    matches!(
        name,
        "[Content_Types].xml"
            | "_rels/.rels"
            | "docProps/app.xml"
            | "docProps/core.xml"
            | "docProps/custom.xml"
            | "xl/sharedStrings.xml"
            | "xl/styles.xml"
            | "xl/calcChain.xml"
            | "xl/persons/person.xml"
    ) || name.starts_with("xl/theme/theme") && name.ends_with(".xml")
        || name.starts_with("xl/tables/table") && name.ends_with(".xml")
        || name.starts_with("xl/comments") && name.ends_with(".xml")
        || name.starts_with("xl/threadedComments/") && name.ends_with(".xml")
}

/// Perform bounded pre-adapter discovery. Limit/cancellation failures return no
/// report; unsupported but completely discovered sources return a typed refusal
/// inside the complete report.
pub fn inspect_xlsx_source(
    source: &[u8],
    manifest: &[u8],
    backend: BackendKind,
    limits: &DiscoveryLimits,
    cancel: Option<&AtomicBool>,
) -> Result<CapabilityReport, DiscoveryError> {
    checkpoint(cancel)?;
    let source_len = source.len() as u64;
    if source_len > limits.max_source_bytes {
        return Err(DiscoveryError::SourceBytes {
            actual: source_len,
            limit: limits.max_source_bytes,
        });
    }
    let manifest_len = manifest.len() as u64;
    if manifest_len > limits.max_manifest_bytes {
        return Err(DiscoveryError::ManifestBytes {
            actual: manifest_len,
            limit: limits.max_manifest_bytes,
        });
    }
    let (manifest_value, canonical_manifest) = canonical_manifest(manifest)?;
    let manifest_digest = digest_hex(&canonical_manifest);

    let mut archive = ZipArchive::new(Cursor::new(source))
        .map_err(|error| DiscoveryError::Zip(error.to_string()))?;
    if archive.len() > limits.max_entries {
        return Err(DiscoveryError::EntryCount {
            actual: archive.len(),
            limit: limits.max_entries,
        });
    }

    let mut entries = Vec::with_capacity(archive.len());
    let mut names = BTreeSet::new();
    let mut total_compressed = 0u64;
    let mut total_uncompressed = 0u64;
    let mut max_ratio = 0u64;
    for index in 0..archive.len() {
        checkpoint(cancel)?;
        let file = archive
            .by_index(index)
            .map_err(|error| DiscoveryError::Zip(error.to_string()))?;
        let name = file.name().to_string();
        if !safe_entry_name(&name) {
            return Err(DiscoveryError::UnsafePath { name });
        }
        if !names.insert(name.clone()) {
            return Err(DiscoveryError::DuplicateEntry { name });
        }
        let compressed = file.compressed_size();
        let uncompressed = file.size();
        if uncompressed > limits.max_entry_uncompressed_bytes {
            return Err(DiscoveryError::EntryUncompressed {
                name,
                actual: uncompressed,
                limit: limits.max_entry_uncompressed_bytes,
            });
        }
        if ratio_exceeds(uncompressed, compressed, limits.max_expansion_ratio) {
            return Err(DiscoveryError::ExpansionRatio { name });
        }
        total_compressed = total_compressed.saturating_add(compressed);
        total_uncompressed = total_uncompressed.saturating_add(uncompressed);
        if total_compressed > limits.max_total_compressed_bytes {
            return Err(DiscoveryError::TotalCompressed {
                actual: total_compressed,
                limit: limits.max_total_compressed_bytes,
            });
        }
        if total_uncompressed > limits.max_total_uncompressed_bytes {
            return Err(DiscoveryError::TotalUncompressed {
                actual: total_uncompressed,
                limit: limits.max_total_uncompressed_bytes,
            });
        }
        max_ratio = max_ratio.max(ratio_milli(uncompressed, compressed));
        entries.push(EntryMeta {
            index,
            name,
            compressed,
            uncompressed,
        });
    }
    if ratio_exceeds(
        total_uncompressed,
        total_compressed,
        limits.max_expansion_ratio,
    ) {
        return Err(DiscoveryError::ExpansionRatio {
            name: "<aggregate>".to_string(),
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    let mut digest = Sha256::new();
    let mut part_bytes = BTreeMap::new();
    let mut macros = Vec::new();
    let mut ole = Vec::new();
    let mut external_links = Vec::new();
    let mut connections = Vec::new();
    let mut table_parts = BTreeSet::new();
    let mut macro_content_type = false;

    for metadata in &entries {
        checkpoint(cancel)?;
        let mut file = archive
            .by_index(metadata.index)
            .map_err(|error| DiscoveryError::Zip(error.to_string()))?;
        let capacity = usize::try_from(metadata.uncompressed).unwrap_or(usize::MAX);
        let mut bytes = Vec::with_capacity(capacity.min(1024 * 1024));
        file.by_ref()
            .take(limits.max_entry_uncompressed_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|error| DiscoveryError::Zip(error.to_string()))?;
        if bytes.len() as u64 > limits.max_entry_uncompressed_bytes
            || bytes.len() as u64 != metadata.uncompressed
        {
            return Err(DiscoveryError::EntryUncompressed {
                name: metadata.name.clone(),
                actual: bytes.len() as u64,
                limit: limits.max_entry_uncompressed_bytes,
            });
        }

        digest.update((metadata.name.len() as u64).to_be_bytes());
        digest.update(metadata.name.as_bytes());
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(&bytes);

        let lower = metadata.name.to_ascii_lowercase();
        if lower.ends_with(".xml") {
            validate_xml_document(&bytes, &metadata.name)?;
        }
        if metadata.name == "[Content_Types].xml" {
            macro_content_type = parse_content_types(&bytes, &metadata.name)?;
        }
        if lower.contains("vbaproject") || lower.ends_with(".xlsm") {
            macros.push(metadata.name.clone());
        }
        if lower.starts_with("xl/embeddings/")
            || lower.ends_with(".ole")
            || lower.ends_with(".bin") && lower.contains("ole")
        {
            ole.push(metadata.name.clone());
        }
        if lower.contains("externallink") {
            external_links.push(metadata.name.clone());
        }
        if lower.ends_with("connections.xml") {
            connections.push(metadata.name.clone());
        }
        if metadata.name.starts_with("xl/tables/table") && metadata.name.ends_with(".xml") {
            table_parts.insert(metadata.name.clone());
        }
        part_bytes.insert(metadata.name.clone(), bytes);
    }

    let mut relationships = BTreeMap::new();
    for (name, bytes) in &part_bytes {
        if name.ends_with(".rels") {
            relationships.insert(name.clone(), parse_relationships(bytes, name)?);
        }
    }

    let mut allowed_parts = part_bytes
        .keys()
        .filter(|name| statically_allowed_part(name))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut relationship_failures = Vec::new();
    let mut external_targets = Vec::new();
    let root_relationships = relationships.get("_rels/.rels");
    if root_relationships.is_none() {
        relationship_failures.push("missing _rels/.rels".to_string());
    }
    let office_relationships = root_relationships
        .into_iter()
        .flatten()
        .filter(|relationship| {
            relationship_kind(&relationship.relationship_type) == "officeDocument"
        })
        .collect::<Vec<_>>();
    if office_relationships.len() != 1 {
        relationship_failures.push(format!(
            "expected exactly one officeDocument relationship, found {}",
            office_relationships.len()
        ));
    }

    let workbook_part = office_relationships.first().and_then(|relationship| {
        if relationship.external {
            relationship_failures.push("officeDocument relationship is external".to_string());
            return None;
        }
        let Some(target) = resolve_relationship_target("", &relationship.target) else {
            relationship_failures
                .push("officeDocument relationship target is invalid or escaping".to_string());
            return None;
        };
        if !part_bytes.contains_key(&target) {
            relationship_failures.push(format!("officeDocument target `{target}` is missing"));
            return None;
        }
        allowed_parts.insert(target.clone());
        Some(target)
    });

    let mut parsed_workbook = None;
    let mut worksheet_parts = Vec::new();
    if let Some(workbook_part) = workbook_part.as_deref() {
        let workbook_relationship_part = relationship_part_name(workbook_part);
        allowed_parts.insert(workbook_relationship_part.clone());
        let workbook_relationships = relationships.get(&workbook_relationship_part);
        if workbook_relationships.is_none() {
            relationship_failures.push(format!(
                "missing workbook relationships `{workbook_relationship_part}`"
            ));
        }
        let parsed = parse_workbook_xml(&part_bytes[workbook_part], workbook_part)?;
        if let Some(workbook_relationships) = workbook_relationships {
            for sheet in &parsed.sheets {
                let matches = workbook_relationships
                    .iter()
                    .filter(|relationship| relationship.id == sheet.relationship_id)
                    .collect::<Vec<_>>();
                if matches.len() != 1 {
                    relationship_failures.push(format!(
                        "sheet `{}` relationship `{}` is missing or duplicated",
                        sheet.name, sheet.relationship_id
                    ));
                    continue;
                }
                let relationship = matches[0];
                if relationship.external
                    || relationship_kind(&relationship.relationship_type) != "worksheet"
                {
                    relationship_failures.push(format!(
                        "sheet `{}` relationship is not an internal worksheet",
                        sheet.name
                    ));
                    continue;
                }
                let Some(target) = resolve_relationship_target(workbook_part, &relationship.target)
                else {
                    relationship_failures.push(format!(
                        "sheet `{}` relationship target is invalid or escaping",
                        sheet.name
                    ));
                    continue;
                };
                if !part_bytes.contains_key(&target) {
                    relationship_failures.push(format!(
                        "sheet `{}` target `{target}` is missing",
                        sheet.name
                    ));
                    continue;
                }
                allowed_parts.insert(target.clone());
                let worksheet_relationship_part = relationship_part_name(&target);
                if relationships.contains_key(&worksheet_relationship_part) {
                    allowed_parts.insert(worksheet_relationship_part);
                }
                worksheet_parts.push(target);
            }
        }
        parsed_workbook = Some(parsed);
    }

    // Resolve known inert/modelled relationship targets from the relationship
    // parts that are themselves admitted. Unknown relationship types stay out
    // of the allow-list and therefore force a typed refusal below.
    let relationship_parts = allowed_parts
        .iter()
        .filter(|name| name.ends_with(".rels"))
        .cloned()
        .collect::<Vec<_>>();
    for relationship_part in relationship_parts {
        let source_part = if relationship_part == "_rels/.rels" {
            String::new()
        } else {
            relationship_part
                .strip_suffix(".rels")
                .and_then(|name| {
                    name.rsplit_once("/_rels/")
                        .map(|(dir, file)| format!("{dir}/{file}"))
                })
                .unwrap_or_default()
        };
        for relationship in relationships.get(&relationship_part).into_iter().flatten() {
            if relationship.external {
                if relationship_kind(&relationship.relationship_type) == "externalLink" {
                    external_targets.push(relationship.target.clone());
                }
                continue;
            }
            let Some(target) = resolve_relationship_target(&source_part, &relationship.target)
            else {
                relationship_failures.push(format!(
                    "relationship `{}` in `{relationship_part}` has an invalid or escaping target",
                    relationship.id
                ));
                continue;
            };
            if !part_bytes.contains_key(&target) {
                relationship_failures.push(format!(
                    "relationship `{}` target `{target}` is missing",
                    relationship.id
                ));
                continue;
            }
            let kind = relationship_kind(&relationship.relationship_type);
            if matches!(
                kind,
                "officeDocument"
                    | "worksheet"
                    | "styles"
                    | "sharedStrings"
                    | "theme"
                    | "table"
                    | "comments"
                    | "threadedComment"
                    | "person"
                    | "calcChain"
                    | "core-properties"
                    | "extended-properties"
                    | "custom-properties"
            ) {
                if kind == "table" {
                    table_parts.insert(target.clone());
                }
                allowed_parts.insert(target);
            }
        }
    }

    let mut active = part_bytes
        .keys()
        .filter(|name| !name.ends_with('/') && !allowed_parts.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    let (date_1904, hidden_sheets, defined_names) = parsed_workbook
        .map(|workbook| {
            (
                workbook.date_1904,
                workbook.hidden_sheets,
                workbook.defined_names,
            )
        })
        .unwrap_or_default();
    let mut formula_cells = 0u64;
    let mut cached_formula_cells = 0u64;
    let mut hidden_rows = 0u64;
    let mut external_formula_references = 0u64;
    for entry in worksheet_parts {
        checkpoint(cancel)?;
        let (formulas, cached, rows, external_references) =
            scan_worksheet_xml(&part_bytes[&entry], &entry)?;
        formula_cells = formula_cells.saturating_add(formulas);
        cached_formula_cells = cached_formula_cells.saturating_add(cached);
        hidden_rows = hidden_rows.saturating_add(rows);
        external_formula_references =
            external_formula_references.saturating_add(external_references);
    }

    if !part_bytes.contains_key("[Content_Types].xml") {
        relationship_failures.push("missing [Content_Types].xml".to_string());
    }
    if macro_content_type {
        macros.push("[Content_Types].xml".to_string());
    }
    macros.sort();
    macros.dedup();
    ole.sort();
    external_links.sort();
    external_targets.sort();
    external_targets.dedup();
    connections.sort();
    active.sort();
    active.dedup();

    let fidelity = BackendFidelity::for_backend(backend);
    let mut reasons = Vec::new();
    let mut reason = |category, code: &str, detail: String| {
        reasons.push(RefusalReason {
            category,
            code: code.to_string(),
            detail,
        });
    };
    if !relationship_failures.is_empty() {
        reason(
            RefusalCategory::UnsupportedSource,
            "invalid_ooxml_relationships",
            relationship_failures.join("; "),
        );
    }
    if manifest_value
        .get("spec")
        .and_then(serde_json::Value::as_str)
        != Some("fio")
        || !manifest_value
            .get("ports")
            .is_some_and(serde_json::Value::is_array)
    {
        reason(
            RefusalCategory::ManifestIncompatible,
            "manifest_shape_incompatible",
            "manifest must declare spec=fio and contain a ports array".to_string(),
        );
    }
    if date_1904 && !fidelity.date_system_1904 {
        reason(
            RefusalCategory::LossyBackend,
            "date1904_not_transportable",
            "source declares the 1904 date system but the selected XLSX adapter hardcodes 1900"
                .to_string(),
        );
    }
    let unsupported_names = defined_names
        .iter()
        .filter(|name| {
            !matches!(
                name.kind,
                DefinedNameSourceKind::Range | DefinedNameSourceKind::Literal
            )
        })
        .count();
    if unsupported_names > 0 {
        reason(
            RefusalCategory::UnsupportedSource,
            "unsupported_defined_names",
            format!("{unsupported_names} source defined names are not Range/Literal definitions"),
        );
    }
    let literal_names = defined_names
        .iter()
        .filter(|name| name.kind == DefinedNameSourceKind::Literal)
        .count();
    if literal_names > 0 && !fidelity.literal_defined_names {
        reason(
            RefusalCategory::LossyBackend,
            "literal_defined_names_lost",
            format!(
                "{literal_names} literal defined names are not transported by the selected XLSX adapter"
            ),
        );
    }
    if !macros.is_empty() {
        reason(
            RefusalCategory::ActiveContent,
            "macros_or_vba",
            format!("{} macro/VBA parts detected", macros.len()),
        );
    }
    if !ole.is_empty() {
        reason(
            RefusalCategory::ActiveContent,
            "ole_embeddings",
            format!("{} OLE/embedding parts detected", ole.len()),
        );
    }
    if !external_links.is_empty() || !external_targets.is_empty() || external_formula_references > 0
    {
        reason(
            RefusalCategory::ExternalDependency,
            "external_links",
            "external workbook parts or external relationships are present".to_string(),
        );
    }
    if !connections.is_empty() {
        reason(
            RefusalCategory::ActiveContent,
            "connections",
            "active workbook connections are present".to_string(),
        );
    }
    if !active.is_empty() {
        reason(
            RefusalCategory::ActiveContent,
            "unmodeled_active_parts",
            format!("{} unmodeled active OOXML parts detected", active.len()),
        );
    }
    if !table_parts.is_empty() && !fidelity.tables {
        reason(
            RefusalCategory::LossyBackend,
            "tables_lost",
            format!(
                "{} source table parts are not transported by the selected XLSX adapter",
                table_parts.len()
            ),
        );
    }
    if !hidden_sheets.is_empty() && !fidelity.hidden_sheets {
        reason(
            RefusalCategory::LossyBackend,
            "hidden_sheet_state_lost",
            "the selected backend/engine model does not retain hidden sheet state".to_string(),
        );
    }
    if hidden_rows > 0 && !fidelity.manual_hidden_rows {
        reason(
            RefusalCategory::LossyBackend,
            "hidden_row_state_lost",
            "the selected backend does not retain source hidden rows".to_string(),
        );
    }
    reasons.sort_by(|a, b| a.code.cmp(&b.code).then_with(|| a.detail.cmp(&b.detail)));

    let public_entries = entries
        .iter()
        .map(|entry| ZipEntryFact {
            name: entry.name.clone(),
            compressed_bytes: entry.compressed,
            uncompressed_bytes: entry.uncompressed,
        })
        .collect::<Vec<_>>();
    let table_parts = table_parts.into_iter().collect::<Vec<_>>();
    Ok(CapabilityReport {
        report_version: "fzcp-s0-experimental-v0".to_string(),
        source_digest_sha256: format!("{:x}", digest.finalize()),
        manifest_digest_sha256: manifest_digest,
        canonical_manifest_json: manifest_value,
        backend: fidelity,
        entry_count: public_entries.len(),
        all_entry_paths_safe: true,
        entries: public_entries,
        total_compressed_bytes: total_compressed,
        total_uncompressed_bytes: total_uncompressed,
        aggregate_expansion_ratio_milli: ratio_milli(total_uncompressed, total_compressed),
        max_entry_expansion_ratio_milli: max_ratio,
        date_1904,
        defined_names,
        formula_cells,
        formula_cells_with_cached_results: cached_formula_cells,
        external_formula_references,
        hidden_sheets,
        source_hidden_rows: hidden_rows,
        table_part_count: table_parts.len(),
        table_parts,
        macros_or_vba_parts: macros,
        ole_parts: ole,
        external_link_parts: external_links,
        external_relationship_targets: external_targets,
        connection_parts: connections,
        unmodeled_active_parts: active,
        decision: if reasons.is_empty() {
            AdmissionDecision::Admitted
        } else {
            AdmissionDecision::Refused { reasons }
        },
    })
}

const EXCEL_MAX_ROWS: u32 = 1_048_576;
const EXCEL_MAX_COLS: u32 = 16_384;

#[derive(Clone, Debug)]
pub struct PackageLimits {
    pub max_logical_cells: u64,
    pub max_stored_cells: u64,
    pub max_formula_cells: u64,
    pub max_package_bytes: u64,
}

impl Default for PackageLimits {
    fn default() -> Self {
        Self {
            max_logical_cells: 5_000_000,
            max_stored_cells: 2_000_000,
            max_formula_cells: 1_000_000,
            max_package_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PackageBuildOptions<'a> {
    pub source_digest_sha256: &'a str,
    pub manifest: &'a serde_json::Value,
    pub target_metadata: &'a serde_json::Value,
    pub backend: BackendKind,
    pub limits: PackageLimits,
    pub cancel: Option<&'a AtomicBool>,
}

#[derive(Clone, Debug, Default)]
pub struct PackageExpectation<'a> {
    pub source_digest_sha256: Option<&'a str>,
    pub manifest_digest_sha256: Option<&'a str>,
}

#[derive(Debug, thiserror::Error)]
pub enum PackageError {
    #[error("package operation cancelled")]
    Cancelled,
    #[error("package resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("unsupported modeled state: {0}")]
    Unsupported(String),
    #[error("invalid package: {0}")]
    Invalid(String),
    #[error("stale package binding `{field}`: expected `{expected}`, got `{actual}`")]
    Stale {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error(transparent)]
    Workbook(#[from] IoError),
    #[error(transparent)]
    Excel(#[from] ExcelError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PackageDocument {
    format: String,
    engine_semantic_version: String,
    source_digest_sha256: String,
    manifest_digest_sha256: String,
    backend: BackendKind,
    manifest: serde_json::Value,
    target_metadata: serde_json::Value,
    workbook: EncodedWorkbook,
    exclusions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedWorkbook {
    settings: EncodedSettings,
    sheets: Vec<EncodedSheet>,
    names: Vec<EncodedName>,
    tables: Vec<EncodedTable>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedSettings {
    date_system: String,
    cycle_detection: String,
    cycle_policy: EncodedCyclePolicy,
    calc: Option<EncodedCalcSettings>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EncodedCyclePolicy {
    Error,
    Iterate {
        max_iterations: u32,
        max_change_bits: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedCalcSettings {
    iterate: bool,
    iterate_count: Option<u32>,
    iterate_delta_bits: Option<u64>,
    calc_mode: Option<String>,
    full_calc_on_load: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedSheet {
    name: String,
    rows: u32,
    cols: u32,
    manual_hidden_rows: Vec<u32>,
    filter_hidden_rows: Vec<u32>,
    cells: Vec<EncodedCell>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedCell {
    row: u32,
    col: u32,
    value: Option<EncodedValue>,
    formula: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum EncodedValue {
    NumberBits(u64),
    Integer(i64),
    DateTimeBits(u64),
    DurationBits(u64),
    Boolean(bool),
    Text(String),
    Date(String),
    DateTime(String),
    Time(String),
    DurationNanos(i64),
    Error(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedName {
    name: String,
    scope_sheet: Option<String>,
    definition: EncodedNameDefinition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EncodedNameDefinition {
    Range {
        sheet: String,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    },
    Literal {
        value: EncodedValue,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedTable {
    name: String,
    sheet: String,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
    header_row: bool,
    headers: Vec<String>,
    totals_row: bool,
}

fn package_checkpoint(cancel: Option<&AtomicBool>) -> Result<(), PackageError> {
    if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        Err(PackageError::Cancelled)
    } else {
        Ok(())
    }
}

fn stored_to_encoded(value: StoredCellValue) -> Result<EncodedValue, PackageError> {
    match value {
        StoredCellValue::NumberBits(bits) => Ok(EncodedValue::NumberBits(bits)),
        StoredCellValue::DateTimeBits(bits) => Ok(EncodedValue::DateTimeBits(bits)),
        StoredCellValue::DurationBits(bits) => Ok(EncodedValue::DurationBits(bits)),
        StoredCellValue::Boolean(value) => Ok(EncodedValue::Boolean(value)),
        StoredCellValue::Text(value) => Ok(EncodedValue::Text(value)),
        StoredCellValue::ErrorCode(code) => Ok(EncodedValue::Error(error_name(
            formualizer_eval::arrow_store::unmap_error_code(code),
        ))),
        StoredCellValue::Pending => Err(PackageError::Unsupported(
            "Pending is private runtime state".to_string(),
        )),
    }
}

fn literal_to_encoded(value: &LiteralValue) -> Result<EncodedValue, PackageError> {
    match value {
        LiteralValue::Int(value) => Ok(EncodedValue::Integer(*value)),
        LiteralValue::Number(value) => Ok(EncodedValue::NumberBits(value.to_bits())),
        LiteralValue::Boolean(value) => Ok(EncodedValue::Boolean(*value)),
        LiteralValue::Text(value) => Ok(EncodedValue::Text(value.clone())),
        LiteralValue::Error(error) => Ok(EncodedValue::Error(error_name(error.kind))),
        LiteralValue::Date(value) => Ok(EncodedValue::Date(value.to_string())),
        LiteralValue::DateTime(value) => Ok(EncodedValue::DateTime(value.to_string())),
        LiteralValue::Time(value) => Ok(EncodedValue::Time(value.to_string())),
        LiteralValue::Duration(value) => Ok(EncodedValue::DurationNanos(
            value.num_nanoseconds().ok_or_else(|| {
                PackageError::Unsupported("name literal duration exceeds nanosecond range".into())
            })?,
        )),
        LiteralValue::Empty => Err(PackageError::Unsupported(
            "empty literal names are not supported in S1".to_string(),
        )),
        LiteralValue::Pending | LiteralValue::Array(_) => Err(PackageError::Unsupported(
            "runtime/array literal names are not supported in S1".to_string(),
        )),
    }
}

fn encoded_to_literal(value: &EncodedValue) -> Result<LiteralValue, PackageError> {
    match value {
        EncodedValue::NumberBits(bits) => Ok(LiteralValue::Number(f64::from_bits(*bits))),
        EncodedValue::Integer(value) => Ok(LiteralValue::Int(*value)),
        EncodedValue::DateTimeBits(bits) => Ok(LiteralValue::Number(f64::from_bits(*bits))),
        EncodedValue::DurationBits(bits) => {
            let serial = f64::from_bits(*bits);
            let nanos = (serial * 86_400_000_000_000.0).round();
            if !nanos.is_finite() || nanos < i64::MIN as f64 || nanos > i64::MAX as f64 {
                return Err(PackageError::Invalid("invalid duration bits".to_string()));
            }
            Ok(LiteralValue::Duration(chrono::Duration::nanoseconds(
                nanos as i64,
            )))
        }
        EncodedValue::Boolean(value) => Ok(LiteralValue::Boolean(*value)),
        EncodedValue::Text(value) => Ok(LiteralValue::Text(value.clone())),
        EncodedValue::Date(value) => chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(LiteralValue::Date)
            .map_err(|error| PackageError::Invalid(error.to_string())),
        EncodedValue::DateTime(value) => value
            .parse::<chrono::NaiveDateTime>()
            .map(LiteralValue::DateTime)
            .map_err(|error| PackageError::Invalid(error.to_string())),
        EncodedValue::Time(value) => value
            .parse::<chrono::NaiveTime>()
            .map(LiteralValue::Time)
            .map_err(|error| PackageError::Invalid(error.to_string())),
        EncodedValue::DurationNanos(value) => Ok(LiteralValue::Duration(
            chrono::Duration::nanoseconds(*value),
        )),
        EncodedValue::Error(name) => Ok(LiteralValue::Error(ExcelError::new(parse_error_name(
            name,
        )?))),
    }
}

fn error_name(kind: ExcelErrorKind) -> String {
    format!("{kind:?}")
}

fn parse_error_name(name: &str) -> Result<ExcelErrorKind, PackageError> {
    Ok(match name {
        "Null" => ExcelErrorKind::Null,
        "Ref" => ExcelErrorKind::Ref,
        "Name" => ExcelErrorKind::Name,
        "Value" => ExcelErrorKind::Value,
        "Div" => ExcelErrorKind::Div,
        "Na" => ExcelErrorKind::Na,
        "Num" => ExcelErrorKind::Num,
        "Error" => ExcelErrorKind::Error,
        "NImpl" => ExcelErrorKind::NImpl,
        "Spill" => ExcelErrorKind::Spill,
        "Calc" => ExcelErrorKind::Calc,
        "Circ" => ExcelErrorKind::Circ,
        "Cancelled" => ExcelErrorKind::Cancelled,
        other => return Err(PackageError::Invalid(format!("unknown error kind {other}"))),
    })
}

fn canonical_formula(formula: &str) -> Result<String, PackageError> {
    let normalized = if formula.starts_with('=') {
        formula.to_string()
    } else {
        format!("={formula}")
    };
    let ast = formualizer_parse::parser::parse(&normalized).map_err(|error| {
        PackageError::Unsupported(format!("formula cannot be represented exactly: {error}"))
    })?;
    Ok(formualizer_parse::pretty::canonical_formula(&ast))
}

fn volatile_function_name(node: &formualizer_parse::parser::ASTNode) -> Option<&str> {
    use formualizer_parse::parser::ASTNodeType;

    match &node.node_type {
        ASTNodeType::Function { name, args } => {
            if ["RAND", "RANDBETWEEN", "RANDARRAY", "NOW", "TODAY"]
                .iter()
                .any(|volatile| name.eq_ignore_ascii_case(volatile))
            {
                Some(name)
            } else {
                args.iter().find_map(volatile_function_name)
            }
        }
        ASTNodeType::UnaryOp { expr, .. } => volatile_function_name(expr),
        ASTNodeType::BinaryOp { left, right, .. } => {
            volatile_function_name(left).or_else(|| volatile_function_name(right))
        }
        ASTNodeType::Call { callee, args } => {
            volatile_function_name(callee).or_else(|| args.iter().find_map(volatile_function_name))
        }
        ASTNodeType::Array(rows) => rows.iter().flatten().find_map(volatile_function_name),
        ASTNodeType::Literal(_) | ASTNodeType::Reference { .. } => None,
    }
}

fn reject_unbound_volatile_formula(formula: &str) -> Result<(), PackageError> {
    let normalized = if formula.starts_with('=') {
        formula.to_string()
    } else {
        format!("={formula}")
    };
    let ast = formualizer_parse::parser::parse(&normalized)
        .map_err(|error| PackageError::Invalid(format!("invalid formula: {error}")))?;
    if let Some(function) = volatile_function_name(&ast) {
        return Err(PackageError::Unsupported(format!(
            "volatile function {function} requires deterministic clock/RNG binding not modeled by S1"
        )));
    }
    Ok(())
}

fn encode_settings(workbook: &Workbook) -> EncodedSettings {
    let config = workbook.eval_config();
    let cycle_policy = match config.cycle.policy {
        CyclePolicy::Error => EncodedCyclePolicy::Error,
        CyclePolicy::Iterate {
            max_iterations,
            max_change,
        } => EncodedCyclePolicy::Iterate {
            max_iterations,
            max_change_bits: max_change.to_bits(),
        },
    };
    EncodedSettings {
        date_system: match config.date_system {
            DateSystem::Excel1900 => "excel_1900",
            DateSystem::Excel1904 => "excel_1904",
        }
        .to_string(),
        cycle_detection: match config.cycle.detection {
            CycleDetection::Static => "static",
            CycleDetection::Runtime => "runtime",
        }
        .to_string(),
        cycle_policy,
        calc: workbook
            .loaded_calc_settings()
            .map(|settings| EncodedCalcSettings {
                iterate: settings.iterate,
                iterate_count: settings.iterate_count,
                iterate_delta_bits: settings.iterate_delta.map(f64::to_bits),
                calc_mode: settings.calc_mode.clone(),
                full_calc_on_load: settings.full_calc_on_load,
            }),
    }
}

fn encode_names(workbook: &Workbook) -> Result<Vec<EncodedName>, PackageError> {
    let engine = workbook.fzcp_engine();
    let mut names = Vec::new();
    for snapshot in engine.named_ranges_snapshot() {
        let scope_sheet = match snapshot.scope {
            NameScope::Workbook => None,
            NameScope::Sheet(sheet_id) => Some(engine.sheet_name(sheet_id).to_string()),
        };
        let definition = match snapshot.definition {
            NamedDefinition::Cell(cell) => EncodedNameDefinition::Range {
                sheet: engine.sheet_name(cell.sheet_id).to_string(),
                start_row: cell.coord.row() + 1,
                start_col: cell.coord.col() + 1,
                end_row: cell.coord.row() + 1,
                end_col: cell.coord.col() + 1,
            },
            NamedDefinition::Range(range) if range.start.sheet_id == range.end.sheet_id => {
                EncodedNameDefinition::Range {
                    sheet: engine.sheet_name(range.start.sheet_id).to_string(),
                    start_row: range.start.coord.row() + 1,
                    start_col: range.start.coord.col() + 1,
                    end_row: range.end.coord.row() + 1,
                    end_col: range.end.coord.col() + 1,
                }
            }
            NamedDefinition::Range(_) => {
                return Err(PackageError::Unsupported(
                    "cross-sheet name ranges are not modeled".to_string(),
                ));
            }
            NamedDefinition::Literal(value) => EncodedNameDefinition::Literal {
                value: literal_to_encoded(&value)?,
            },
            NamedDefinition::Formula { .. } => {
                return Err(PackageError::Unsupported(
                    "formula-backed names are outside the S1 subset".to_string(),
                ));
            }
        };
        names.push(EncodedName {
            name: snapshot.name,
            scope_sheet,
            definition,
        });
    }
    names.sort_by(|a, b| {
        a.scope_sheet
            .cmp(&b.scope_sheet)
            .then_with(|| a.name.cmp(&b.name))
    });
    Ok(names)
}

fn encode_tables(workbook: &Workbook) -> Vec<EncodedTable> {
    workbook
        .fzcp_engine()
        .table_metadata_snapshot()
        .into_iter()
        .map(|table| EncodedTable {
            name: table.name,
            sheet: table.sheet,
            start_row: table.start_row,
            start_col: table.start_col,
            end_row: table.end_row,
            end_col: table.end_col,
            header_row: table.header_row,
            headers: table.headers,
            totals_row: table.totals_row,
        })
        .collect()
}

/// Serialize the complete currently modeled workbook. No pruning occurs.
pub fn build_calculation_package(
    workbook: &Workbook,
    options: PackageBuildOptions<'_>,
) -> Result<Vec<u8>, PackageError> {
    package_checkpoint(options.cancel)?;
    if !workbook.list_custom_functions().is_empty() {
        return Err(PackageError::Unsupported(
            "custom function/provider fingerprints are not modeled".to_string(),
        ));
    }
    if workbook.fzcp_engine().computed_overlay_compactions() != 0 {
        return Err(PackageError::Unsupported(
            "computed formula/spill overlays were compacted into source lanes; stored-value provenance is no longer trustworthy"
                .to_string(),
        ));
    }
    let canonical_manifest = serde_json::to_vec(options.manifest)
        .map_err(|error| PackageError::Invalid(error.to_string()))?;
    let manifest_digest = digest_hex(&canonical_manifest);
    let mut logical_cells = 0u64;
    let mut stored_cells = 0u64;
    let mut formula_cells = 0u64;
    let mut sheets = Vec::new();

    for sheet_name in workbook.sheet_names() {
        package_checkpoint(options.cancel)?;
        let (rows, cols) = workbook.sheet_dimensions(&sheet_name).unwrap_or((0, 0));
        if (rows == 0) != (cols == 0) {
            return Err(PackageError::Invalid(format!(
                "sheet `{sheet_name}` has inconsistent zero/nonzero dimensions {rows}x{cols}"
            )));
        }
        logical_cells = logical_cells.saturating_add(u64::from(rows) * u64::from(cols));
        if logical_cells > options.limits.max_logical_cells {
            return Err(PackageError::ResourceLimit(format!(
                "logical cells {logical_cells} exceed {}",
                options.limits.max_logical_cells
            )));
        }
        let mut cells = Vec::new();
        let mut manual_hidden_rows = Vec::new();
        let mut filter_hidden_rows = Vec::new();
        for row in 1..=rows {
            if row % 1024 == 0 {
                package_checkpoint(options.cancel)?;
            }
            let engine = workbook.fzcp_engine();
            if engine
                .is_row_hidden(&sheet_name, row, Some(RowVisibilitySource::Manual))
                .unwrap_or(false)
            {
                manual_hidden_rows.push(row);
            }
            if engine
                .is_row_hidden(&sheet_name, row, Some(RowVisibilitySource::Filter))
                .unwrap_or(false)
            {
                filter_hidden_rows.push(row);
            }
            for col in 1..=cols {
                let formula = workbook
                    .get_formula(&sheet_name, row, col)
                    .map(|formula| {
                        let formula = canonical_formula(&formula)?;
                        reject_unbound_volatile_formula(&formula)?;
                        Ok::<_, PackageError>(formula)
                    })
                    .transpose()?;
                let value = if formula.is_none() {
                    workbook
                        .export_stored_cell(&sheet_name, row, col)
                        .map(stored_to_encoded)
                        .transpose()?
                } else {
                    None
                };
                if formula.is_none() && value.is_none() {
                    continue;
                }
                if formula.is_some() {
                    formula_cells = formula_cells.saturating_add(1);
                    if formula_cells > options.limits.max_formula_cells {
                        return Err(PackageError::ResourceLimit(format!(
                            "formula cells exceed {}",
                            options.limits.max_formula_cells
                        )));
                    }
                } else {
                    stored_cells = stored_cells.saturating_add(1);
                    if stored_cells > options.limits.max_stored_cells {
                        return Err(PackageError::ResourceLimit(format!(
                            "stored cells exceed {}",
                            options.limits.max_stored_cells
                        )));
                    }
                }
                cells.push(EncodedCell {
                    row,
                    col,
                    value,
                    formula,
                });
            }
        }
        sheets.push(EncodedSheet {
            name: sheet_name,
            rows,
            cols,
            manual_hidden_rows,
            filter_hidden_rows,
            cells,
        });
    }

    let document = PackageDocument {
        format: FORMAT_VERSION.to_string(),
        engine_semantic_version: ENGINE_SEMANTIC_VERSION.to_string(),
        source_digest_sha256: options.source_digest_sha256.to_string(),
        manifest_digest_sha256: manifest_digest,
        backend: options.backend,
        manifest: options.manifest.clone(),
        target_metadata: options.target_metadata.clone(),
        workbook: EncodedWorkbook {
            settings: encode_settings(workbook),
            sheets,
            names: encode_names(workbook)?,
            tables: encode_tables(workbook),
        },
        exclusions: vec![
            "cached_formula_results".to_string(),
            "private_runtime_ids".to_string(),
            "recalc_plans".to_string(),
            "xlsx_roundtrip_metadata".to_string(),
        ],
    };
    package_checkpoint(options.cancel)?;
    let payload =
        serde_json::to_vec(&document).map_err(|error| PackageError::Invalid(error.to_string()))?;
    let package_len = BLOB_HEADER
        .len()
        .saturating_add(payload.len())
        .saturating_add(1) as u64;
    if package_len > options.limits.max_package_bytes {
        return Err(PackageError::ResourceLimit(format!(
            "package bytes {package_len} exceed {}",
            options.limits.max_package_bytes
        )));
    }
    let mut bytes = Vec::with_capacity(package_len as usize);
    bytes.extend_from_slice(BLOB_HEADER);
    bytes.extend_from_slice(&payload);
    bytes.push(b'\n');
    Ok(bytes)
}

pub struct LoadedCalculationPackage {
    pub workbook: Workbook,
    pub manifest: serde_json::Value,
    pub target_metadata: serde_json::Value,
    pub source_digest_sha256: String,
    pub manifest_digest_sha256: String,
    pub backend: BackendKind,
}

fn decode_settings(
    settings: &EncodedSettings,
) -> Result<(WorkbookConfig, Option<CalcSettings>), PackageError> {
    let date_system = match settings.date_system.as_str() {
        "excel_1900" => DateSystem::Excel1900,
        "excel_1904" => DateSystem::Excel1904,
        other => {
            return Err(PackageError::Invalid(format!(
                "unknown date system {other}"
            )));
        }
    };
    let detection = match settings.cycle_detection.as_str() {
        "static" => CycleDetection::Static,
        "runtime" => CycleDetection::Runtime,
        other => {
            return Err(PackageError::Invalid(format!(
                "unknown cycle detection {other}"
            )));
        }
    };
    let policy = match settings.cycle_policy {
        EncodedCyclePolicy::Error => CyclePolicy::Error,
        EncodedCyclePolicy::Iterate {
            max_iterations,
            max_change_bits,
        } => CyclePolicy::Iterate {
            max_iterations,
            max_change: f64::from_bits(max_change_bits),
        },
    };
    let cycle = CycleConfig { detection, policy };
    cycle.validate().map_err(PackageError::Invalid)?;
    let mut config = WorkbookConfig::interactive();
    config.eval.date_system = date_system;
    config.eval.cycle = cycle;
    let calc = settings.calc.as_ref().map(|settings| CalcSettings {
        iterate: settings.iterate,
        iterate_count: settings.iterate_count,
        iterate_delta: settings.iterate_delta_bits.map(f64::from_bits),
        calc_mode: settings.calc_mode.clone(),
        full_calc_on_load: settings.full_calc_on_load,
    });
    Ok((config, calc))
}

fn encoded_stored(value: &EncodedValue) -> Result<StoredCellValue, PackageError> {
    match value {
        EncodedValue::NumberBits(bits) => Ok(StoredCellValue::NumberBits(*bits)),
        EncodedValue::DateTimeBits(bits) => Ok(StoredCellValue::DateTimeBits(*bits)),
        EncodedValue::DurationBits(bits) => Ok(StoredCellValue::DurationBits(*bits)),
        EncodedValue::Boolean(value) => Ok(StoredCellValue::Boolean(*value)),
        EncodedValue::Text(value) => Ok(StoredCellValue::Text(value.clone())),
        EncodedValue::Error(name) => Ok(StoredCellValue::ErrorCode(
            formualizer_eval::arrow_store::map_error_code(parse_error_name(name)?),
        )),
        EncodedValue::Integer(_)
        | EncodedValue::Date(_)
        | EncodedValue::DateTime(_)
        | EncodedValue::Time(_)
        | EncodedValue::DurationNanos(_) => Err(PackageError::Invalid(
            "typed literal-name value used in a cell record".to_string(),
        )),
    }
}

fn validate_package_formula(formula: &str) -> Result<(), PackageError> {
    let canonical = canonical_formula(formula).map_err(|error| {
        PackageError::Invalid(format!("package formula is not parseable: {error}"))
    })?;
    if canonical != formula {
        return Err(PackageError::Invalid(format!(
            "package formula is not canonical: expected `{canonical}`"
        )));
    }
    reject_unbound_volatile_formula(formula)
}

fn valid_range(
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
    rows: u32,
    cols: u32,
) -> bool {
    start_row >= 1
        && start_col >= 1
        && start_row <= end_row
        && start_col <= end_col
        && end_row <= rows
        && end_col <= cols
        && end_row <= EXCEL_MAX_ROWS
        && end_col <= EXCEL_MAX_COLS
}

fn validate_encoded_workbook(
    workbook: &EncodedWorkbook,
    limits: &PackageLimits,
) -> Result<(), PackageError> {
    let mut sheet_names = BTreeSet::new();
    let mut sheet_dimensions = BTreeMap::new();
    let mut logical_cells = 0u64;
    let mut stored_cells = 0u64;
    let mut formula_cells = 0u64;
    for sheet in &workbook.sheets {
        if sheet.name.is_empty() || !sheet_names.insert(sheet.name.as_str()) {
            return Err(PackageError::Invalid(format!(
                "empty or duplicate sheet name `{}`",
                sheet.name
            )));
        }
        if sheet.rows > EXCEL_MAX_ROWS || sheet.cols > EXCEL_MAX_COLS {
            return Err(PackageError::Invalid(format!(
                "sheet `{}` dimensions {}x{} exceed Excel's {}x{} limits",
                sheet.name, sheet.rows, sheet.cols, EXCEL_MAX_ROWS, EXCEL_MAX_COLS
            )));
        }
        if (sheet.rows == 0) != (sheet.cols == 0) {
            return Err(PackageError::Invalid(format!(
                "sheet `{}` has inconsistent zero/nonzero dimensions {}x{}",
                sheet.name, sheet.rows, sheet.cols
            )));
        }
        sheet_dimensions.insert(sheet.name.as_str(), (sheet.rows, sheet.cols));
        logical_cells = logical_cells
            .checked_add(u64::from(sheet.rows) * u64::from(sheet.cols))
            .ok_or_else(|| PackageError::ResourceLimit("logical cell count overflow".into()))?;
        if logical_cells > limits.max_logical_cells {
            return Err(PackageError::ResourceLimit(format!(
                "logical cells {logical_cells} exceed {}",
                limits.max_logical_cells
            )));
        }
        let mut previous = None;
        for cell in &sheet.cells {
            if cell.row == 0
                || cell.col == 0
                || cell.row > sheet.rows
                || cell.col > sheet.cols
                || cell.value.is_some() == cell.formula.is_some()
            {
                return Err(PackageError::Invalid(format!(
                    "invalid cell record at {}!R{}C{}",
                    sheet.name, cell.row, cell.col
                )));
            }
            let coord = (cell.row, cell.col);
            if previous.is_some_and(|prior| prior >= coord) {
                return Err(PackageError::Invalid(format!(
                    "cells are not strictly coordinate-sorted in {}",
                    sheet.name
                )));
            }
            previous = Some(coord);
            if let Some(formula) = &cell.formula {
                validate_package_formula(formula)?;
                formula_cells = formula_cells.saturating_add(1);
            } else {
                stored_cells = stored_cells.saturating_add(1);
            }
        }
        if formula_cells > limits.max_formula_cells {
            return Err(PackageError::ResourceLimit(format!(
                "formula cells exceed {}",
                limits.max_formula_cells
            )));
        }
        if stored_cells > limits.max_stored_cells {
            return Err(PackageError::ResourceLimit(format!(
                "stored cells exceed {}",
                limits.max_stored_cells
            )));
        }
        for rows in [&sheet.manual_hidden_rows, &sheet.filter_hidden_rows] {
            if rows.windows(2).any(|pair| pair[0] >= pair[1])
                || rows.iter().any(|row| *row == 0 || *row > sheet.rows)
            {
                return Err(PackageError::Invalid(format!(
                    "invalid hidden-row records in {}",
                    sheet.name
                )));
            }
        }
    }

    let mut encoded_names = BTreeSet::new();
    for name in &workbook.names {
        if name.name.is_empty()
            || !encoded_names.insert((name.scope_sheet.as_deref(), name.name.as_str()))
        {
            return Err(PackageError::Invalid(format!(
                "empty or duplicate encoded name `{}`",
                name.name
            )));
        }
        if let Some(scope_sheet) = name.scope_sheet.as_deref()
            && !sheet_dimensions.contains_key(scope_sheet)
        {
            return Err(PackageError::Invalid(format!(
                "unknown name scope sheet `{scope_sheet}`"
            )));
        }
        if let EncodedNameDefinition::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        } = &name.definition
        {
            let Some(&(rows, cols)) = sheet_dimensions.get(sheet.as_str()) else {
                return Err(PackageError::Invalid(format!(
                    "unknown name range sheet `{sheet}`"
                )));
            };
            if !valid_range(*start_row, *start_col, *end_row, *end_col, rows, cols) {
                return Err(PackageError::Invalid(format!(
                    "invalid range for encoded name `{}`",
                    name.name
                )));
            }
        }
    }

    let mut table_names = BTreeSet::new();
    for table in &workbook.tables {
        if table.name.is_empty() || !table_names.insert(table.name.as_str()) {
            return Err(PackageError::Invalid(format!(
                "empty or duplicate table name `{}`",
                table.name
            )));
        }
        let Some(&(rows, cols)) = sheet_dimensions.get(table.sheet.as_str()) else {
            return Err(PackageError::Invalid(format!(
                "unknown table sheet `{}`",
                table.sheet
            )));
        };
        if !valid_range(
            table.start_row,
            table.start_col,
            table.end_row,
            table.end_col,
            rows,
            cols,
        ) {
            return Err(PackageError::Invalid(format!(
                "invalid range for table `{}`",
                table.name
            )));
        }
    }
    Ok(())
}

/// Load the experimental blob and enforce optional source/manifest bindings.
pub fn load_calculation_package(
    bytes: &[u8],
    expectation: PackageExpectation<'_>,
) -> Result<LoadedCalculationPackage, PackageError> {
    let load_limits = PackageLimits::default();
    if bytes.len() as u64 > load_limits.max_package_bytes {
        return Err(PackageError::ResourceLimit(format!(
            "package bytes {} exceed {}",
            bytes.len(),
            load_limits.max_package_bytes
        )));
    }
    let payload = bytes
        .strip_prefix(BLOB_HEADER)
        .ok_or_else(|| PackageError::Invalid("missing experimental FZCP header".to_string()))?;
    let document: PackageDocument = serde_json::from_slice(payload)
        .map_err(|error| PackageError::Invalid(error.to_string()))?;
    if document.format != FORMAT_VERSION {
        return Err(PackageError::Invalid(format!(
            "unsupported format {}",
            document.format
        )));
    }
    validate_encoded_workbook(&document.workbook, &load_limits)?;
    if document.engine_semantic_version != ENGINE_SEMANTIC_VERSION {
        return Err(PackageError::Stale {
            field: "engine_semantic_version",
            expected: ENGINE_SEMANTIC_VERSION.to_string(),
            actual: document.engine_semantic_version,
        });
    }
    if let Some(expected) = expectation.source_digest_sha256
        && expected != document.source_digest_sha256
    {
        return Err(PackageError::Stale {
            field: "source_digest_sha256",
            expected: expected.to_string(),
            actual: document.source_digest_sha256,
        });
    }
    if let Some(expected) = expectation.manifest_digest_sha256
        && expected != document.manifest_digest_sha256
    {
        return Err(PackageError::Stale {
            field: "manifest_digest_sha256",
            expected: expected.to_string(),
            actual: document.manifest_digest_sha256,
        });
    }
    let actual_manifest_digest = digest_hex(
        serde_json::to_vec(&document.manifest)
            .map_err(|error| PackageError::Invalid(error.to_string()))?,
    );
    if actual_manifest_digest != document.manifest_digest_sha256 {
        return Err(PackageError::Invalid(
            "manifest digest does not match embedded manifest".to_string(),
        ));
    }

    let (mut config, calc_settings) = decode_settings(&document.workbook.settings)?;
    if let Some(first_sheet) = document.workbook.sheets.first() {
        config.eval.default_sheet_name = first_sheet.name.clone();
    }
    let mut workbook = Workbook::new_with_config(config);
    for sheet in &document.workbook.sheets {
        workbook.add_sheet(&sheet.name)?;
        workbook.fzcp_ensure_dimensions(&sheet.name, sheet.rows, sheet.cols);
    }
    for name in &document.workbook.names {
        let scope = match &name.scope_sheet {
            Some(sheet) => {
                NameScope::Sheet(workbook.fzcp_engine().sheet_id(sheet).ok_or_else(|| {
                    PackageError::Invalid(format!("unknown name scope sheet {sheet}"))
                })?)
            }
            None => NameScope::Workbook,
        };
        let definition = match &name.definition {
            EncodedNameDefinition::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                let sheet_id = workbook
                    .fzcp_engine()
                    .sheet_id(sheet)
                    .ok_or_else(|| PackageError::Invalid(format!("unknown name sheet {sheet}")))?;
                let start = CellRef::new(
                    sheet_id,
                    Coord::from_excel(*start_row, *start_col, true, true),
                );
                let end = CellRef::new(sheet_id, Coord::from_excel(*end_row, *end_col, true, true));
                if start == end {
                    NamedDefinition::Cell(start)
                } else {
                    NamedDefinition::Range(RangeRef::new(start, end))
                }
            }
            EncodedNameDefinition::Literal { value } => {
                NamedDefinition::Literal(encoded_to_literal(value)?)
            }
        };
        workbook
            .fzcp_engine_mut()
            .define_name(&name.name, definition, scope)?;
    }
    for table in &document.workbook.tables {
        let sheet_id = workbook
            .fzcp_engine()
            .sheet_id(&table.sheet)
            .ok_or_else(|| PackageError::Invalid(format!("unknown table sheet {}", table.sheet)))?;
        let range = RangeRef::new(
            CellRef::new(
                sheet_id,
                Coord::from_excel(table.start_row, table.start_col, true, true),
            ),
            CellRef::new(
                sheet_id,
                Coord::from_excel(table.end_row, table.end_col, true, true),
            ),
        );
        workbook.fzcp_engine_mut().define_table(
            &table.name,
            range,
            table.header_row,
            table.headers.clone(),
            table.totals_row,
        )?;
    }
    for sheet in &document.workbook.sheets {
        for cell in &sheet.cells {
            if let Some(value) = &cell.value {
                workbook.fzcp_restore_stored_cell(
                    &sheet.name,
                    cell.row,
                    cell.col,
                    &encoded_stored(value)?,
                )?;
            }
        }
        for row in &sheet.manual_hidden_rows {
            workbook
                .fzcp_engine_mut()
                .set_row_hidden(&sheet.name, *row, true, RowVisibilitySource::Manual)
                .map_err(|error| PackageError::Invalid(error.to_string()))?;
        }
        for row in &sheet.filter_hidden_rows {
            workbook
                .fzcp_engine_mut()
                .set_row_hidden(&sheet.name, *row, true, RowVisibilitySource::Filter)
                .map_err(|error| PackageError::Invalid(error.to_string()))?;
        }
    }
    for sheet in &document.workbook.sheets {
        for cell in &sheet.cells {
            if let Some(formula) = &cell.formula {
                workbook.set_formula(&sheet.name, cell.row, cell.col, formula)?;
            }
        }
    }
    workbook.fzcp_set_calc_settings(calc_settings);

    Ok(LoadedCalculationPackage {
        workbook,
        manifest: document.manifest,
        target_metadata: document.target_metadata,
        source_digest_sha256: document.source_digest_sha256,
        manifest_digest_sha256: document.manifest_digest_sha256,
        backend: document.backend,
    })
}

/// Canonical S0 report bytes for audit/digest tests.
pub fn canonical_report_bytes(report: &CapabilityReport) -> Result<Vec<u8>, serde_json::Error> {
    let mut bytes = serde_json::to_vec(report)?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Convenience used by tests and callers that bind an already canonical manifest.
pub fn manifest_digest(manifest: &serde_json::Value) -> Result<String, serde_json::Error> {
    serde_json::to_vec(manifest).map(digest_hex)
}
