//! Strict cache-only XLSX recalculation, without a rich document model.
//! Unsupported package/formula cases fail before any output is published.
mod package;
mod sheet;
mod xml;

use super::recalculate::{DEFAULT_ERROR_LOCATION_LIMIT, RecalculateStatus, RecalculateSummary};
use crate::{CalamineAdapter, IoError, SpreadsheetReader, workbook::WBResolver};
use formualizer_common::{CellAddress, DateSystem, LiteralValue};
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::inspect::{SnapshotOptions, Staleness};
use formualizer_eval::engine::{CancelToken, Engine, EvalConfig, FormulaParsePolicy};
use std::collections::{BTreeMap, HashSet};
#[cfg(not(target_arch = "wasm32"))]
use std::io::Read;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::ops::Range;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

/// Bounds apply to actual decompression, XML depth/cells and output, not only ZIP headers.
#[derive(Debug, Clone)]
pub struct XlsxRecalculateLimits {
    pub max_input_bytes: usize,
    pub max_entries: usize,
    pub max_expanded_bytes: usize,
    pub max_worksheet_bytes: usize,
    pub max_formula_cells: usize,
    pub max_output_bytes: usize,
    pub max_xml_depth: usize,
    pub max_cells: usize,
    /// Limits the width of Calamine's per-column ingestion builders.
    pub max_columns: u32,
}
impl Default for XlsxRecalculateLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 << 20,
            max_entries: 10_000,
            max_expanded_bytes: 256 << 20,
            max_worksheet_bytes: 128 << 20,
            max_formula_cells: 100_000,
            max_output_bytes: 64 << 20,
            max_xml_depth: 128,
            max_cells: 8_000_000,
            max_columns: 256,
        }
    }
}
/// The source date system is authoritative; other evaluation policies come from `eval_config`.
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
/// `cache_cells_changed` counts physical caches actually patched, not engine deltas.
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

#[derive(Debug, PartialEq)]
enum Cache {
    Number(f64),
    Boolean(bool),
    Text(String),
    Error(String),
    Empty,
}
impl Cache {
    fn from_value(value: LiteralValue, system: DateSystem) -> Result<Self, IoError> {
        Ok(match value {
            LiteralValue::Boolean(b) => Self::Boolean(b),
            LiteralValue::Text(text) => {
                // _xHHHH_ has application-level escape semantics that differ
                // across cached-string readers. Do not silently corrupt it.
                if text.as_bytes().windows(7).any(|w| {
                    w[0] == b'_'
                        && w[1] == b'x'
                        && w[6] == b'_'
                        && w[2..6].iter().all(u8::is_ascii_hexdigit)
                }) {
                    return Err(unsupported(
                        "escape-looking cached text",
                        "cache-only writer",
                    ));
                }
                if !text.chars().all(|c| matches!(c, '\t'|'\n'|'\r'|' '..='\u{d7ff}'|'\u{e000}'..='\u{fffd}'|'\u{10000}'..='\u{10ffff}')) {
                    return Err(unsupported("XML-invalid cached text control", "cache-only writer"));
                }
                Self::Text(text)
            }
            LiteralValue::Error(error) => {
                let token = error.kind.to_string();
                if !matches!(
                    token.as_str(),
                    "#DIV/0!"
                        | "#N/A"
                        | "#NAME?"
                        | "#NULL!"
                        | "#NUM!"
                        | "#REF!"
                        | "#VALUE!"
                        | "#SPILL!"
                        | "#CALC!"
                ) {
                    return Err(unsupported(
                        "engine-specific error has no approved XLSX cache encoding",
                        token,
                    ));
                }
                Self::Error(token)
            }
            LiteralValue::Empty => Self::Empty,
            LiteralValue::Array(_) => {
                return Err(unsupported("array formula result", "cache-only writer"));
            }
            LiteralValue::Pending => {
                return Err(unsupported("pending formula result", "cache-only writer"));
            }
            value => {
                let mut serial = value.as_serial_number_for(system).ok_or_else(|| {
                    unsupported("unrepresentable scalar cache", "cache-only writer")
                })?;
                // The common helper retains historical whole-second duration
                // conversion; preserve the fractional remainder here as well.
                if let LiteralValue::Duration(duration) = value {
                    serial += f64::from(duration.subsec_nanos()) / 86_400_000_000_000.0;
                }
                if !serial.is_finite() {
                    return Err(unsupported("non-finite formula cache", "cache-only writer"));
                }
                Self::Number(serial)
            }
        })
    }
    fn kind(&self) -> Option<&'static str> {
        match self {
            Self::Number(_) | Self::Empty => None,
            Self::Boolean(_) => Some("b"),
            Self::Text(_) => Some("str"),
            Self::Error(_) => Some("e"),
        }
    }
    fn text(&self) -> String {
        match self {
            Self::Number(n) => n.to_string(),
            Self::Boolean(b) => if *b { "1" } else { "0" }.into(),
            Self::Text(t) | Self::Error(t) => quick_xml::escape::escape(t).replace('\r', "&#13;"),
            Self::Empty => String::new(),
        }
    }
    fn matches(&self, cell: &sheet::Cell) -> bool {
        if cell.inline.is_some() {
            return false;
        }
        let Some(v) = &cell.value else {
            return false;
        };
        match (self, cell.kind.as_deref()) {
            (Self::Number(n), None | Some("n")) => v
                .text
                .trim()
                .parse::<f64>()
                .ok()
                .is_some_and(|old| old.is_finite() && old == *n),
            (Self::Empty, None | Some("n")) => v.text.is_empty(),
            (Self::Boolean(b), Some("b")) => {
                matches!((v.text.trim(), b), ("1", true) | ("0", false))
            }
            (Self::Text(t), Some("str")) => &v.text == t,
            (Self::Error(e), Some("e")) => &v.text == e,
            _ => false,
        }
    }
}
struct Patch {
    span: Range<usize>,
    replacement: Vec<u8>,
}
fn cache_patches(xml: &[u8], cell: &sheet::Cell, value: &Cache, patches: &mut Vec<Patch>) {
    let wanted = value.kind();
    let current = cell.kind.as_deref().filter(|t| *t != "n");
    if wanted != current {
        let (span, replacement) = match &cell.kind_span {
            Some(span) => (
                span.clone(),
                wanted
                    .map(|t| format!("t=\"{t}\"").into_bytes())
                    .unwrap_or_default(),
            ),
            None => (
                cell.open_end - 1..cell.open_end - 1,
                wanted
                    .map(|t| format!(" t=\"{t}\"").into_bytes())
                    .unwrap_or_default(),
            ),
        };
        patches.push(Patch { span, replacement });
    }
    if let Some(span) = &cell.inline {
        patches.push(Patch {
            span: span.clone(),
            replacement: Vec::new(),
        });
    }
    let prefix = cell
        .qualified
        .rsplit_once(':')
        .map(|(p, _)| format!("{p}:"))
        .unwrap_or_default();
    let name = format!("{prefix}v");
    let text = value.text();
    let replacement = if let Some(v) = &cell.value {
        let mut out = if v.empty {
            xml[v.span.start..v.span.end - 2].to_vec()
        } else {
            xml[v.span.start..v.open_end - 1].to_vec()
        };
        if matches!(value, Cache::Empty) {
            out.extend_from_slice(b"/>");
        } else {
            out.push(b'>');
            out.extend_from_slice(text.as_bytes());
            if v.empty {
                out.extend_from_slice(format!("</{}>", v.qualified).as_bytes());
            } else {
                out.extend_from_slice(&xml[v.close_start..v.span.end]);
            }
        }
        out
    } else if matches!(value, Cache::Empty) {
        format!("<{name}/>").into_bytes()
    } else {
        format!("<{name}>{text}</{name}>").into_bytes()
    };
    patches.push(Patch {
        span: cell
            .value
            .as_ref()
            .map(|v| v.span.clone())
            .unwrap_or(cell.formula_end..cell.formula_end),
        replacement,
    });
}
fn apply_patches(bytes: &[u8], mut patches: Vec<Patch>, limit: usize) -> Result<Vec<u8>, IoError> {
    patches.sort_by_key(|p| p.span.start);
    let mut length = bytes.len();
    let mut previous = 0;
    for patch in &patches {
        if patch.span.start < previous
            || patch.span.end > bytes.len()
            || patch.span.start > patch.span.end
        {
            return Err(unsupported("overlapping cache edit spans", "worksheet"));
        }
        previous = patch.span.end;
        length = length
            .checked_sub(patch.span.len())
            .and_then(|n| n.checked_add(patch.replacement.len()))
            .ok_or_else(|| unsupported("cache output size overflow", "worksheet"))?;
        if length > limit {
            return Err(unsupported("worksheet output byte limit", "worksheet"));
        }
    }
    // One append pass, rather than repeatedly shifting the remainder of XML.
    let mut out = Vec::with_capacity(length);
    previous = 0;
    for patch in patches {
        out.extend_from_slice(&bytes[previous..patch.span.start]);
        out.extend_from_slice(&patch.replacement);
        previous = patch.span.end;
    }
    out.extend_from_slice(&bytes[previous..]);
    Ok(out)
}
struct BoundedOutput {
    cursor: Cursor<Vec<u8>>,
    limit: usize,
}
impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .cursor
            .position()
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > self.limit as u64)
        {
            return Err(std::io::Error::other("XLSX output byte limit"));
        }
        self.cursor.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.cursor.flush()
    }
}
impl Seek for BoundedOutput {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(from)
    }
}

/// Recalculate ordinary/shared formula caches without importing/rewriting rich
/// workbook structures. Unsupported geometry/metadata cases return an error;
/// there is no lossy fallback. Exact no-ops return the original package bytes.
pub fn recalculate_xlsx_bytes(
    bytes: &[u8],
    options: XlsxRecalculateOptions,
) -> Result<XlsxRecalculateResult, IoError> {
    let mut archive = package::admit(bytes, &options)?;
    let (sheets, date_system) = package::discover(&mut archive, &options)?;
    let mut plans = Vec::new();
    let mut observed = 0;
    let mut logical_cells = 0u64;
    let mut formula_count = 0usize;
    for sheet in &sheets {
        checkpoint(&options.cancel)?;
        let data = package::read_part(
            &mut archive,
            &sheet.part,
            options.limits.max_worksheet_bytes,
        )?;
        let cells = sheet::scan(&data, &options, &mut observed, &mut logical_cells)?;
        formula_count = formula_count
            .checked_add(cells.len())
            .ok_or_else(|| unsupported("formula count overflow", "workbook"))?;
        if formula_count > options.limits.max_formula_cells {
            return Err(unsupported("formula cell count limit", "workbook"));
        }
        plans.push((data, cells));
    }
    let empty_result = |summary| XlsxRecalculateResult {
        bytes: bytes.to_vec(),
        summary,
        formula_cells: formula_count,
        cache_cells_changed: 0,
        worksheet_parts_changed: 0,
    };
    if formula_count == 0 {
        checkpoint(&options.cancel)?;
        if bytes.len() > options.limits.max_output_bytes {
            return Err(unsupported("output byte limit", "XLSX package"));
        }
        return Ok(empty_result(RecalculateSummary::default()));
    }
    checkpoint(&options.cancel)?;
    // Calamine 0.36 cannot decode every legal/stale cache representation,
    // although formula ingestion ignores cached results. Clear those caches in a
    // bounded, transient ingestion view; the authoritative package stays intact.
    let mut view_parts = BTreeMap::new();
    for (sheet, (data, cells)) in sheets.iter().zip(&plans) {
        let mut patches = Vec::new();
        for cell in cells {
            if !sheet::readable_scalar_cache(cell, data)
                || matches!(cell.kind.as_deref(), Some("s" | "inlineStr" | "d"))
            {
                cache_patches(data, cell, &Cache::Empty, &mut patches);
            }
        }
        if !patches.is_empty() {
            view_parts.insert(
                sheet.part.clone(),
                apply_patches(data, patches, options.limits.max_worksheet_bytes)?,
            );
        }
    }
    let ingest_bytes = if view_parts.is_empty() {
        bytes.to_vec()
    } else {
        package::rewrite(bytes, &mut archive, &view_parts, &options)?
    };
    let opened = if let Some(cancel) = options.cancel.clone() {
        CalamineAdapter::open_bytes_cancellable(ingest_bytes, cancel)
    } else {
        CalamineAdapter::open_bytes(ingest_bytes)
    };
    checkpoint(&options.cancel)?;
    let mut adapter = opened.map_err(IoError::Calamine)?;
    if adapter.sheet_names().map_err(IoError::Calamine)?
        != sheets.iter().map(|s| s.name.clone()).collect::<Vec<_>>()
    {
        return Err(unsupported(
            "adapter/preflight sheet mapping mismatch",
            "workbook",
        ));
    }
    let mut config = options.eval_config.clone();
    config.date_system = date_system;
    // XLSX dates are serial caches. Native chrono materialization cannot retain
    // Excel-1900 phantom serial 60 and can discard fractional duration precision.
    config.temporal_egress = formualizer_eval::engine::TemporalEgress::Serial;
    let mut engine: Engine<WBResolver> = Engine::new(WBResolver::default(), config);
    let mut load_limits = engine.workbook_load_limits().clone();
    load_limits.max_sheet_cols = load_limits.max_sheet_cols.min(options.limits.max_columns);
    load_limits.max_sheet_logical_cells = load_limits
        .max_sheet_logical_cells
        .min(options.limits.max_cells as u64);
    load_limits.max_formula_spool_bytes_per_sheet = load_limits
        .max_formula_spool_bytes_per_sheet
        .min(options.limits.max_expanded_bytes as u64);
    load_limits.max_formula_spool_bytes_per_workbook = load_limits
        .max_formula_spool_bytes_per_workbook
        .min(options.limits.max_expanded_bytes as u64);
    engine.set_workbook_load_limits(load_limits);
    let ingested = adapter.stream_into_engine(&mut engine);
    checkpoint(&options.cancel)?;
    ingested?;
    drop(adapter);
    checkpoint(&options.cancel)?;
    if let Some(cancel) = options.cancel.clone() {
        engine.evaluate_all_cancellable(cancel)?;
    } else {
        engine.evaluate_all()?;
    }
    checkpoint(&options.cancel)?;
    let coerced: HashSet<_> = engine
        .formula_parse_diagnostics()
        .iter()
        .filter(|d| d.policy == FormulaParsePolicy::CoerceToError)
        .map(|d| (d.sheet.clone(), d.row, d.col))
        .collect();
    let mut summary = RecalculateSummary::default();
    let mut changed = 0usize;
    let mut replacements = BTreeMap::new();
    let mut expanded = usize::try_from(
        archive
            .decompressed_size()
            .ok_or_else(|| unsupported("ZIP expanded-size overflow", "workbook"))?,
    )
    .map_err(|_| unsupported("ZIP expanded-size overflow", "workbook"))?;
    for (sheet, (data, cells)) in sheets.iter().zip(plans) {
        let mut patches = Vec::new();
        for cell in cells {
            checkpoint(&options.cancel)?;
            let address = CellAddress::new(&sheet.name, cell.row, cell.col)
                .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
            let snapshot = engine
                .inspect_cell(&address, &SnapshotOptions::default())
                .map_err(|e| IoError::from_backend("xlsx-inspect", e))?
                .cell;
            use formualizer_eval::engine::inspect::SpillRole;
            if snapshot.spill.as_ref().is_some_and(|spill| match spill {
                SpillRole::Anchor { extent } => {
                    extent.start_row != extent.end_row || extent.start_col != extent.end_col
                }
                SpillRole::Member { .. } => true,
                _ => true,
            }) {
                return Err(unsupported(
                    "materialized multi-cell dynamic spill",
                    &sheet.name,
                ));
            }
            let mut value = snapshot
                .value
                .ok_or_else(|| unsupported("absent formula result", &sheet.name))?;
            if matches!(&value,LiteralValue::Array(rows) if rows.len()==1 && rows[0].len()==1) {
                value = value
                    .coerce_to_single_value()
                    .map_err(|_| unsupported("non-scalar result", "cache-only writer"))?;
            }
            if snapshot.formula.is_none()
                && !(matches!(value, LiteralValue::Error(_))
                    && coerced.contains(&(sheet.name.clone(), cell.row, cell.col)))
            {
                return Err(unsupported("source formula was not ingested", &sheet.name));
            }
            if snapshot.staleness != Staleness::Current {
                return Err(unsupported("formula result is not current", &sheet.name));
            }
            let stats = summary.sheets.entry(sheet.name.clone()).or_default();
            stats.evaluated += 1;
            summary.evaluated += 1;
            if let LiteralValue::Error(error) = &value {
                summary.errors += 1;
                stats.errors += 1;
                let errors = summary
                    .error_summary
                    .entry(error.kind.to_string())
                    .or_default();
                errors.count += 1;
                if errors.locations.len() < options.error_location_limit {
                    errors
                        .locations
                        .push(format!("{}!{}", sheet.name, cell.address));
                } else {
                    errors.locations_truncated += 1;
                }
            }
            let cache = Cache::from_value(value, engine.config.date_system)?;
            if !cache.matches(&cell) {
                changed += 1;
                cache_patches(&data, &cell, &cache, &mut patches);
            }
        }
        if !patches.is_empty() {
            let patched = apply_patches(&data, patches, options.limits.max_worksheet_bytes)?;
            expanded = expanded
                .checked_sub(data.len())
                .and_then(|n| n.checked_add(patched.len()))
                .ok_or_else(|| unsupported("expanded output overflow", "workbook"))?;
            if expanded > options.limits.max_expanded_bytes {
                return Err(unsupported("expanded output byte limit", "workbook"));
            }
            replacements.insert(sheet.part.clone(), patched);
        }
    }
    summary.status = if summary.errors == 0 {
        RecalculateStatus::Success
    } else {
        RecalculateStatus::ErrorsFound
    };
    checkpoint(&options.cancel)?;
    if replacements.is_empty() {
        if bytes.len() > options.limits.max_output_bytes {
            return Err(unsupported("output byte limit", "XLSX package"));
        }
        return Ok(empty_result(summary));
    }
    let output = package::rewrite(bytes, &mut archive, &replacements, &options)?;
    checkpoint(&options.cancel)?;
    Ok(XlsxRecalculateResult {
        bytes: output,
        summary,
        formula_cells: formula_count,
        cache_cells_changed: changed,
        worksheet_parts_changed: replacements.len(),
    })
}

/// Native bounded snapshot + same-directory temporary + atomic replace. This
/// is not CAS against unrelated writers; callers retain their source authority.
/// Symlink destinations are rejected. No failure/cancellation publishes bytes.
#[cfg(not(target_arch = "wasm32"))]
pub fn recalculate_xlsx_file(
    input: &Path,
    output: Option<&Path>,
    options: XlsxRecalculateOptions,
) -> Result<XlsxRecalculateResult, IoError> {
    checkpoint(&options.cancel)?;
    let mut source = Vec::new();
    std::fs::File::open(input)?
        .take((options.limits.max_input_bytes as u64).saturating_add(1))
        .read_to_end(&mut source)?;
    let result = recalculate_xlsx_bytes(&source, options.clone())?;
    let dest = output.unwrap_or(input);
    let metadata = match std::fs::symlink_metadata(dest) {
        Ok(m) => Some(m),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    if metadata
        .as_ref()
        .is_some_and(|m| m.file_type().is_symlink())
    {
        return Err(unsupported("symlink destination", "atomic XLSX output"));
    }
    checkpoint(&options.cancel)?;
    if dest == input && result.bytes == source {
        return Ok(result);
    }
    let dir = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(dir)?;
    for chunk in result.bytes.chunks(64 * 1024) {
        checkpoint(&options.cancel)?;
        temp.write_all(chunk)?;
    }
    if let Some(metadata) = metadata {
        temp.as_file().set_permissions(metadata.permissions())?;
    }
    temp.as_file().sync_all()?;
    checkpoint(&options.cancel)?;
    temp.persist(dest).map_err(|e| IoError::Io(e.error))?;
    Ok(result)
}
