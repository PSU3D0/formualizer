//! Strict cache-only XLSX recalculation, without a rich document model.
//! Unsupported package/formula cases fail before any output is published.
mod dynamic_metadata;
mod error_reasons;
pub(crate) mod external_links;
mod geometry;
mod ingest_view;
mod legacy_intersection;
mod package;
mod phase_clock;
mod result_projection;
mod shared_qualifiers;
mod sheet;
mod table_lowering;
mod tables;
#[cfg(test)]
mod tests;
mod visibility_guard;
#[cfg(not(feature = "system-clock"))]
mod wall_clock_guard;
mod xml;

use super::recalculate::{DEFAULT_ERROR_LOCATION_LIMIT, RecalculateStatus, RecalculateSummary};
use crate::{CalamineAdapter, IoError, SpreadsheetReader, workbook::WBResolver};
use chrono::{DateTime, Utc};
use formualizer_common::{CellAddress, DateSystem, LiteralValue};
use formualizer_eval::engine::DeterministicMode;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::inspect::{SnapshotOptions, Staleness};
use formualizer_eval::engine::{
    CancelToken, Engine, EvalConfig, FormulaParsePolicy, SpillBoundsPolicy, SpillConfig,
    SpillConflictPolicy,
};
use phase_clock::PhaseClock;
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
/// What recalculation does when formulas read external workbook links.
/// Links are never refreshed under either policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExternalLinkPolicy {
    /// Read the values Excel last stored in the workbook for each link
    /// (reported in [`XlsxRecalculateResult::external_links_used`]).
    #[default]
    Cached,
    /// Refuse (`IoError::Unsupported`, feature `external link values`) a
    /// workbook whose calculation reads any external link value; workbooks
    /// whose links nothing reads still recalculate. Decided after
    /// calculation, so other refusals keep their reasons; nothing is
    /// published.
    Refuse,
}
/// The source date system is authoritative; other evaluation policies come from `eval_config`.
#[derive(Debug, Clone)]
pub struct XlsxRecalculateOptions {
    pub eval_config: EvalConfig,
    pub cancel: Option<CancelToken>,
    pub limits: XlsxRecalculateLimits,
    pub error_location_limit: usize,
    /// See [`ExternalLinkPolicy`]; defaults to reading cached values.
    pub external_links: ExternalLinkPolicy,
}
impl Default for XlsxRecalculateOptions {
    fn default() -> Self {
        Self {
            eval_config: EvalConfig::default(),
            cancel: None,
            limits: XlsxRecalculateLimits::default(),
            error_location_limit: DEFAULT_ERROR_LOCATION_LIMIT,
            external_links: ExternalLinkPolicy::default(),
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
    /// The UTC instant `NOW()`/`TODAY()` observed in this run, interpreted in
    /// `eval_config.deterministic_mode.timezone()`. It is the fixed timestamp
    /// in deterministic mode, otherwise the single system-clock sample (whole
    /// seconds) taken immediately before evaluation. `None` only when the build has no
    /// system clock and no fixed timestamp was supplied (such builds refuse
    /// `TODAY`/`NOW` workbooks).
    pub clock_now_utc: Option<DateTime<Utc>>,
    /// External links whose cached values the calculation read (each
    /// `xl/externalLinks` part a formula or used defined name references).
    /// Links are never refreshed: these values are the ones Excel last
    /// stored in the workbook.
    pub external_links_used: usize,
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
                .is_some_and(|old| display_equal(old, *n)),
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
/// True when a cached number `old` and a computed number `new` agree within
/// one unit in the 15th significant digit (Excel's display precision): both
/// finite and equal, or of the same sign with
/// `|old - new| <= 10^(E - 14)`, `E = floor(log10(max(|old|, |new|)))`.
/// Exact zero agrees only with exact zero (either sign). `E` is the exact
/// decimal exponent of the larger magnitude; the bound is the double nearest
/// `10^(E - 14)`, so for subnormal magnitudes the rule tightens towards
/// exact equality. Such a cache is current: its bytes are kept.
pub(crate) fn display_equal(old: f64, new: f64) -> bool {
    if !(old.is_finite() && new.is_finite()) {
        return false;
    }
    if old == new {
        return true;
    }
    if old == 0.0 || new == 0.0 || old.is_sign_negative() != new.is_sign_negative() {
        return false;
    }
    let larger = old.abs().max(new.abs());
    // 10^E <= larger, so the bound never exceeds larger * 1e-14: anything
    // ten times further apart is stale without deriving the exponent.
    if (old - new).abs() > larger * 1e-13 {
        return false;
    }
    // 18 significant digits never round a double below a power of ten up
    // to it, so this exponent is floor(log10) of the exact value.
    let scientific = format!("{larger:.17e}");
    let Some(exponent) = scientific
        .rsplit_once('e')
        .and_then(|(_, e)| e.parse::<i32>().ok())
    else {
        return false;
    };
    let Ok(bound) = format!("1e{}", exponent - 14).parse::<f64>() else {
        return false;
    };
    (old - new).abs() <= bound
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
    // An insertion sorts before a replacement starting at the same offset.
    patches.sort_by_key(|p| (p.span.start, p.span.end));
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

/// Worksheet source, formula cells and prior-footprint dynamic-array
/// ownership. The per-cell source index exists only for worksheets with
/// admitted anchors; a sheet with a new spill builds it after evaluation.
struct SheetPlan {
    tables: Vec<tables::Table>,
    hidden_rows: Vec<u32>,
    /// `sheetFormatPr/@zeroHeight`: row visibility is not provable.
    rows_hidden_by_default: bool,
    active_filters: Vec<sheet::SourceRect>,
    data: Vec<u8>,
    cells: Vec<sheet::Cell>,
    index: Option<sheet::SourceIndex>,
    ownership: dynamic_metadata::SheetOwnership,
    /// Serialized `<c>` elements, counted toward the generated-cell bound.
    serialized: usize,
    /// Logical `(max_row, max_col)` counted toward the workbook area bound.
    bounds: (u32, u32),
}
struct SourceAdmission<'a> {
    archive: package::Archive<'a>,
    sheets: Vec<package::Sheet>,
    date_system: DateSystem,
    plans: Vec<SheetPlan>,
    formula_count: usize,
    metadata: Option<dynamic_metadata::DynamicMetadata>,
    /// External sources served from the link caches, when the workbook
    /// has links.
    external: Option<external_links::Plan>,
}
/// Bounded package/worksheet admission. This also resolves the XLDAPR
/// metadata chain and returns disjoint prior-footprint ownership per
/// worksheet, or a precise unsupported error.
fn admit_source<'a>(
    bytes: &'a [u8],
    options: &XlsxRecalculateOptions,
) -> Result<SourceAdmission<'a>, IoError> {
    let mut archive = package::admit(bytes, options)?;
    let package::Discovery {
        sheets,
        date_system,
        metadata: metadata_part,
        external_links,
        defined_names,
    } = package::discover(&mut archive, options)?;
    let links = if external_links.is_empty() {
        None
    } else {
        let links = external_links::parse(&mut archive, &external_links, options)?;
        links.check_kinds()?;
        Some(links)
    };
    let metadata = metadata_part
        .map(|part| dynamic_metadata::parse(&mut archive, &part, options))
        .transpose()?;
    let mut plans = Vec::new();
    let mut observed = 0;
    let mut logical_cells = 0u64;
    let mut formula_count = 0usize;
    let mut table_budget = tables::Budget::default();
    let mut table_names = HashSet::new();
    for sheet in &sheets {
        checkpoint(&options.cancel)?;
        let data = package::read_part(
            &mut archive,
            &sheet.part,
            options.limits.max_worksheet_bytes,
        )?;
        let counted = (observed, logical_cells);
        let mut scanned = sheet::scan(
            &data,
            options,
            if sheet.tables.is_empty() {
                sheet::Mode::Plain
            } else {
                sheet::Mode::TablePlain
            },
            &mut observed,
            &mut logical_cells,
        )?;
        if matches!(scanned, sheet::Scanned::NeedsIndex) {
            // Dynamic cell metadata: index this sheet only.
            (observed, logical_cells) = counted;
            scanned = sheet::scan(
                &data,
                options,
                sheet::Mode::Indexed,
                &mut observed,
                &mut logical_cells,
            )?;
        }
        let sheet::Scanned::Done(mut scan) = scanned else {
            return Err(unsupported("unindexed dynamic cell metadata", "worksheet"));
        };
        let mut table_ids = HashSet::new();
        let mut tables = Vec::new();
        for id in &scan.table_ids {
            if !table_ids.insert(id) {
                return Err(unsupported("duplicate tablePart", &sheet.name));
            }
            let part = sheet
                .tables
                .get(id)
                .ok_or_else(|| unsupported("missing/wrong-kind table relationship", &sheet.name))?;
            let table = tables::parse(&mut archive, part, options, &mut table_budget)?;
            if !table_names.insert(table.name.to_lowercase()) {
                return Err(unsupported("table name collision", &table.name));
            }
            if tables
                .iter()
                .any(|t: &tables::Table| t.rect.intersects(table.rect))
            {
                return Err(unsupported("overlapping tables", &table.name));
            }
            // Without a `<dimension>`, trailing empty table rows/columns are
            // not serialized; admit them within the sheet limits, counting the
            // grown logical area exactly as a declared dimension would be.
            if !scan.has_dimension
                && (table.rect.last_row > scan.bounds.0 || table.rect.last_col > scan.bounds.1)
            {
                let grown = (
                    scan.bounds.0.max(table.rect.last_row),
                    scan.bounds.1.max(table.rect.last_col),
                );
                if grown.1 > options.limits.max_columns {
                    return Err(unsupported("worksheet width limit", &table.name));
                }
                logical_cells = (logical_cells
                    - u64::from(scan.bounds.0) * u64::from(scan.bounds.1))
                .checked_add(u64::from(grown.0) * u64::from(grown.1))
                .ok_or_else(|| unsupported("logical area overflow", "workbook"))?;
                if logical_cells > options.limits.max_cells as u64 {
                    return Err(unsupported("workbook logical cell limit", "workbook"));
                }
                scan.bounds = grown;
            }
            tables::validate(
                &table,
                &scan.cells,
                scan.index
                    .as_ref()
                    .map_or(scan.table_merges.as_slice(), |i| i.merges.as_slice()),
                scan.bounds,
                options,
            )?;
            tables.push(table);
        }
        if table_ids.len() != sheet.tables.len() {
            return Err(unsupported("unreferenced table relationship", &sheet.name));
        }
        #[cfg(not(feature = "system-clock"))]
        for cell in &scan.cells {
            wall_clock_guard::validate(
                &cell.formula_text,
                &format!("{}!{}", sheet.name, cell.address),
                options,
            )?;
        }
        formula_count = formula_count
            .checked_add(scan.cells.len())
            .ok_or_else(|| unsupported("formula count overflow", "workbook"))?;
        if formula_count > options.limits.max_formula_cells {
            return Err(unsupported("formula cell count limit", "workbook"));
        }
        let ownership = match &scan.index {
            Some(index) => {
                dynamic_metadata::own_sheet(index, &scan.cells, metadata.as_ref(), options)?
            }
            None => dynamic_metadata::SheetOwnership::default(),
        };
        plans.push(SheetPlan {
            hidden_rows: scan.hidden_rows,
            rows_hidden_by_default: scan.rows_hidden_by_default,
            active_filters: scan.active_filters,
            tables,
            data,
            cells: scan.cells,
            index: scan.index,
            ownership,
            serialized: scan.serialized,
            bounds: scan.bounds,
        });
    }
    if !table_names.is_empty() {
        let workbook = package::read_part(
            &mut archive,
            "xl/workbook.xml",
            options.limits.max_worksheet_bytes,
        )?;
        let table_name_list: Vec<String> = table_names.iter().cloned().collect();
        xml::walk(&workbook, options, |path, node| {
            if xml::path_is(
                path,
                xml::MAIN,
                &["workbook", "definedNames", "definedName"],
            ) && let xml::Kind::Text(text) = &node.kind
                && text.contains('[')
            {
                return Err(unsupported(
                    "defined-name formula contains structured references",
                    "table-bearing workbook",
                ));
            }
            if xml::path_is(
                path,
                xml::MAIN,
                &["workbook", "definedNames", "definedName"],
            ) && let xml::Kind::Text(text) = &node.kind
            {
                table_lowering::validate_defined_name(text, &table_name_list)?;
            }
            if xml::path_is(
                path,
                xml::MAIN,
                &["workbook", "definedNames", "definedName"],
            ) && let Some(n) = node.value("name")
                && table_names.contains(&n.to_lowercase())
            {
                return Err(unsupported("table/defined-name collision", n));
            }
            Ok(())
        })?;
    }
    // Every external reference read must map exactly to cached cells.
    let external = links
        .map(|links| external_links::plan(&links, &sheets, &plans, &defined_names, options))
        .transpose()?;
    Ok(SourceAdmission {
        archive,
        sheets,
        date_system,
        plans,
        formula_count,
        metadata,
        external,
    })
}
/// A permissive engine spill policy could overwrite source-unowned inputs or
/// publish a truncated spill; refuse anything but the defaults wherever a
/// dynamic array is involved (admitted anchors before evaluation, new
/// spills at projection). Scalar workbooks are unaffected.
fn check_spill_policy(spill: &SpillConfig) -> Result<(), IoError> {
    if spill.conflict_policy != SpillConflictPolicy::Error {
        return Err(unsupported(
            "non-default spill conflict policy",
            "source-preserving spill recalculation",
        ));
    }
    if spill.bounds_policy != SpillBoundsPolicy::Strict {
        return Err(unsupported(
            "non-default spill bounds policy",
            "source-preserving spill recalculation",
        ));
    }
    Ok(())
}

/// Recalculate ordinary/shared formula caches and the supported subset of
/// dynamic arrays (see `docs/cache-only-xlsx.md`) without importing or
/// rewriting rich workbook structures. Unsupported geometry/metadata cases
/// return an error; there is no lossy fallback. Exact no-ops return the
/// original package bytes.
pub fn recalculate_xlsx_bytes(
    bytes: &[u8],
    options: XlsxRecalculateOptions,
) -> Result<XlsxRecalculateResult, IoError> {
    // The engine would otherwise fall back to the system clock and the run
    // would report an instant it did not use.
    options.eval_config.deterministic_mode.validate()?;
    let mut clock = PhaseClock::from_env();
    let admission = admit_source(bytes, &options)?;
    clock.lap("admit (package, worksheet scan, tables)");
    if admission
        .plans
        .iter()
        .any(|p| !p.ownership.anchors.is_empty())
    {
        check_spill_policy(&options.eval_config.spill)?;
    }
    let formula_count = admission.formula_count;
    if formula_count == 0 && admission.plans.iter().all(|p| p.tables.is_empty()) {
        checkpoint(&options.cancel)?;
        if bytes.len() > options.limits.max_output_bytes {
            return Err(unsupported("output byte limit", "XLSX package"));
        }
        let mut result = unchanged(bytes, formula_count, RecalculateSummary::default());
        result.clock_now_utc = clock_instant(&options);
        return Ok(result);
    }
    let mut ingested = ingest_source(bytes, admission, &options, &mut clock)?;
    let clock_now_utc = evaluate(&mut ingested.engine, &options)?;
    clock.lap("evaluate (incl. deferred graph build)");
    // Deferred graph building parses formulas during evaluation.
    refuse_parse_failure(&ingested.engine, ingested.refuse_parse_failures)?;
    clock.families(&ingested.engine);
    let first_read = ingested.external_first_read.take();
    let mut result = publish(bytes, ingested, formula_count, &options, &mut clock)?;
    // Decided last, so every other refusal keeps its reason and a refusal
    // here means the run succeeds with cached values.
    if options.external_links == ExternalLinkPolicy::Refuse && result.external_links_used > 0 {
        let n = result.external_links_used;
        return Err(unsupported(
            "external link values",
            format!(
                "{} (recalculating would use the values cached in the workbook for {n} external link{}; links are never refreshed)",
                first_read.unwrap_or_else(|| "workbook".into()),
                if n == 1 { "" } else { "s" }
            ),
        ));
    }
    result.clock_now_utc = clock_now_utc;
    clock.total(formula_count);
    Ok(result)
}
/// The instant this run's `NOW()`/`TODAY()` observe: the fixed timestamp in
/// deterministic mode, otherwise one system-clock sample (when the build has a
/// system clock).
fn clock_instant(options: &XlsxRecalculateOptions) -> Option<DateTime<Utc>> {
    match &options.eval_config.deterministic_mode {
        DeterministicMode::Enabled { timestamp_utc, .. } => Some(*timestamp_utc),
        // NOW() has one-second resolution. A whole-second sample is exactly
        // what the caches record and round-trips through every binding.
        #[cfg(feature = "system-clock")]
        DeterministicMode::Disabled { .. } => {
            Some(chrono::SubsecRound::trunc_subsecs(Utc::now(), 0))
        }
        #[cfg(not(feature = "system-clock"))]
        DeterministicMode::Disabled { .. } => None,
    }
}
/// The admitted source with its ingested, not yet evaluated, engine.
struct Ingested<'a> {
    archive: package::Archive<'a>,
    sheets: Vec<package::Sheet>,
    plans: Vec<SheetPlan>,
    engine: Engine<WBResolver>,
    /// The validated sheet metadata part, if the package has one.
    metadata: Option<dynamic_metadata::DynamicMetadata>,
    /// The caller's policy was strict: a recorded parse failure refuses.
    refuse_parse_failures: bool,
    /// See [`XlsxRecalculateResult::external_links_used`].
    external_links_used: usize,
    /// The first external read, when any.
    external_first_read: Option<String>,
}
/// Under the default strict policy, a stored formula the parser cannot read
/// is an unsupported feature of this input, not an engine failure. Ingestion
/// records parse failures instead of stopping; this refuses with the first.
fn refuse_parse_failure(engine: &Engine<WBResolver>, refuse: bool) -> Result<(), IoError> {
    match engine.formula_parse_diagnostics().first() {
        Some(d) if refuse => {
            let col = formualizer_common::col_letters_from_1based(d.col)
                .unwrap_or_else(|_| d.col.to_string());
            Err(unsupported(
                "unparseable formula",
                format!("{}!{col}{}: {}", d.sheet, d.row, d.message),
            ))
        }
        _ => Ok(()),
    }
}
/// Build the transient ingestion view, replay it through Calamine into a new
/// engine, validate calculation names and declare every
/// admitted dynamic-array anchor. Nothing is evaluated or published.
fn ingest_source<'a>(
    bytes: &[u8],
    admission: SourceAdmission<'a>,
    options: &XlsxRecalculateOptions,
    clock: &mut PhaseClock,
) -> Result<Ingested<'a>, IoError> {
    let SourceAdmission {
        mut archive,
        sheets,
        date_system,
        plans,
        formula_count: _,
        metadata,
        external,
    } = admission;
    let anchors = plans.iter().any(|p| !p.ownership.anchors.is_empty());
    checkpoint(&options.cancel)?;
    // Calamine 0.36 cannot decode every legal/stale cache representation,
    // although formula ingestion ignores cached results. Clear those caches,
    // and mask/normalize dynamic arrays, in a bounded transient ingestion
    // view; the authoritative package stays intact.
    let has_tables = plans.iter().any(|p| !p.tables.is_empty());
    // Legacy implicit intersection for formulas Excel calculated (listed in
    // the calc chain); see `legacy_intersection`.
    let calc_chain_part = package::calc_chain_part(&mut archive, options);
    let calc_chain = legacy_intersection::calc_chain(
        &mut archive,
        calc_chain_part.as_deref(),
        &sheets,
        options,
    )?;
    let table_names: Vec<String> = plans
        .iter()
        .flat_map(|p| &p.tables)
        .map(|t| t.name.to_lowercase())
        .collect();
    let mut view_parts = BTreeMap::new();
    for (index, (sheet, plan)) in sheets.iter().zip(&plans).enumerate() {
        let mut patches = ingest_view::patches(plan, &options.cancel)?;
        if !calc_chain.is_empty() {
            let (legacy, _) = legacy_intersection::patches(
                index,
                plan,
                &calc_chain,
                &table_names,
                external.as_ref().map(|e| &e.range_names),
                options,
            )?;
            patches.extend(legacy);
        }
        if has_tables {
            patches.extend(table_lowering::patches(
                &sheet.name,
                plan,
                &sheets,
                &plans,
                options,
            )?);
        }
        if !patches.is_empty() {
            view_parts.insert(
                sheet.part.clone(),
                apply_patches(&plan.data, patches, options.limits.max_worksheet_bytes)?,
            );
        }
    }
    let ingest_bytes = if view_parts.is_empty() {
        bytes.to_vec()
    } else {
        let edits = package::Edits {
            replace: view_parts,
            add: BTreeMap::new(),
        };
        package::rewrite(bytes, &mut archive, &edits, options)?
    };
    clock.lap("ingest view (calc chain, patches, package rewrite)");
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
    let refuse_parse_failures = config.formula_parse_policy == FormulaParsePolicy::Strict;
    if refuse_parse_failures {
        config.formula_parse_policy = FormulaParsePolicy::CoerceToError;
    }
    // Policy: CSE evaluation bypasses family memoization, whose admitted
    // cached results are not declaration-sensitive. Non-CSE runs are unchanged.
    if plans
        .iter()
        .any(|p| p.ownership.anchors.values().any(|a| a.binding.is_none()))
    {
        config.family_execution = false;
    }
    // XLSX dates are serial caches. Native chrono materialization cannot retain
    // Excel-1900 phantom serial 60 and can discard fractional duration precision.
    config.temporal_egress = formualizer_eval::engine::TemporalEgress::Serial;
    // External references read the link caches through engine sources named
    // by their reference text, defined before any formula is staged.
    let (external_values, external_links_used, external_first_read) = match external {
        Some(plan) => {
            adapter.skip_external_names(plan.skipped_names);
            (
                Some(std::sync::Arc::new(plan.values)),
                plan.links_used,
                plan.first_read,
            )
        }
        None => (None, 0, None),
    };
    let resolver = external_values
        .clone()
        .map_or_else(WBResolver::default, WBResolver::with_external_values);
    let mut engine: Engine<WBResolver> = Engine::new(resolver, config);
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
    if let Some(values) = &external_values {
        engine.adopt_file_sheets(sheets.iter().map(|s| s.name.as_str()))?;
        // Cached values never change during a run: a fixed source version.
        for name in values.scalar_names() {
            engine.define_source_scalar(name, Some(0))?;
        }
        for name in values.range_names() {
            engine.define_source_table(name, Some(0))?;
        }
        adapter.admit_external_references(values.clone());
    }
    adapter.validate_calculation_names(&[])?;
    if plans.iter().any(|p| !p.tables.is_empty()) {
        use formualizer_eval::reference::{CellRef, Coord, RangeRef};
        engine.adopt_file_sheets(sheets.iter().map(|s| s.name.as_str()))?;
        for (sheet, plan) in sheets.iter().zip(&plans) {
            let id = engine.sheet_id(&sheet.name).expect("adopted worksheet");
            for table in &plan.tables {
                checkpoint(&options.cancel)?;
                let r = table.rect;
                engine.define_table(
                    &table.name,
                    RangeRef::new(
                        CellRef::new(id, Coord::from_excel(r.first_row, r.first_col, true, true)),
                        CellRef::new(id, Coord::from_excel(r.last_row, r.last_col, true, true)),
                    ),
                    table.header,
                    table.columns.iter().map(|c| c.name.clone()).collect(),
                    table.totals,
                )?;
            }
        }
    }
    checkpoint(&options.cancel)?;
    clock.lap("calamine open, names, tables");
    let ingested = adapter.stream_into_engine(&mut engine);
    checkpoint(&options.cancel)?;
    ingested?;
    clock.lap("stream into engine");
    refuse_parse_failure(&engine, refuse_parse_failures)?;
    for (sheet, plan) in sheets.iter().zip(&plans) {
        for table in &plan.tables {
            if table.header {
                for (i, column) in table.columns.iter().enumerate() {
                    checkpoint(&options.cancel)?;
                    let address = CellAddress::new(
                        &sheet.name,
                        table.rect.first_row,
                        table.rect.first_col + i as u32,
                    )
                    .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
                    let snapshot = engine
                        .inspect_cell(&address, &SnapshotOptions::default())
                        .map_err(|e| IoError::from_backend("xlsx-inspect", e))?
                        .cell;
                    if snapshot.formula.is_some()
                        || snapshot.value != Some(LiteralValue::Text(column.name.clone()))
                    {
                        return Err(unsupported(
                            "table header cached text mismatch",
                            format!("table {} column {}", table.name, column.name),
                        ));
                    }
                }
            }
        }
    }
    let unimported = adapter.unimported_document_names();
    if !unimported.is_empty() {
        // Metadata-only names may be omitted from the engine only if no source
        // calculation references them. Only a formula whose source text names
        // one can; a shared formula's copies name what its anchor names. Inspect
        // those after shared-formula replay but before evaluation/publication.
        let mut source_formulas = Vec::new();
        for (sheet, plan) in sheets.iter().zip(&plans) {
            for cell in &plan.cells {
                checkpoint(&options.cancel)?;
                let text = cell.formula_text.to_ascii_lowercase();
                if !unimported.iter().any(|name| text.contains(name.as_str())) {
                    continue;
                }
                let address = CellAddress::new(&sheet.name, cell.row, cell.col)
                    .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
                if let Some(formula) = engine
                    .inspect_cell(&address, &SnapshotOptions::default())
                    .map_err(|e| IoError::from_backend("xlsx-inspect", e))?
                    .cell
                    .formula
                {
                    source_formulas.push(formula);
                }
            }
        }
        adapter.validate_calculation_names(&source_formulas)?;
        clock.lap("document-name reference check");
    }
    visibility_guard::validate(&engine, &sheets, &plans, options)?;
    checkpoint(&options.cancel)?;
    drop(adapter);
    if anchors {
        declare_anchors(&mut engine, &sheets, &plans, options)?;
        refuse_parse_failure(&engine, refuse_parse_failures)?;
    }
    clock.lap("post-ingest checks (headers, visibility, anchors)");
    Ok(Ingested {
        archive,
        sheets,
        plans,
        engine,
        metadata,
        refuse_parse_failures,
        external_links_used,
        external_first_read,
    })
}
/// Give every admitted anchor its source spill identity, so a current
/// scalar (1x1) result still resolves `A1#`/`ANCHORARRAY(A1)`. A formula
/// coerced to a parse error has no formula vertex and stays undeclared.
fn declare_anchors(
    engine: &mut Engine<WBResolver>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if engine.has_staged_formulas() {
        // Deferred graph building: anchors need formula vertices.
        engine.build_graph_all()?;
        checkpoint(&options.cancel)?;
    }
    let coerced: HashSet<_> = engine
        .formula_parse_diagnostics()
        .iter()
        .filter(|d| d.policy == FormulaParsePolicy::CoerceToError)
        .map(|d| (d.sheet.clone(), d.row, d.col))
        .collect();
    for (sheet, plan) in sheets.iter().zip(plans) {
        for (&(row, col), anchor) in &plan.ownership.anchors {
            checkpoint(&options.cancel)?;
            let declaration = if anchor.binding.is_none() {
                let extent = anchor.footprint;
                engine.declare_fixed_array_formula(
                    &sheet.name,
                    row,
                    col,
                    extent.last_row - extent.first_row + 1,
                    extent.last_col - extent.first_col + 1,
                )
            } else {
                engine.declare_dynamic_array_anchor(&sheet.name, row, col)
            };
            if declaration.is_err() && !coerced.contains(&(sheet.name.clone(), row, col)) {
                return Err(unsupported(
                    "dynamic array anchor was not ingested as a formula",
                    &sheet.name,
                ));
            }
        }
    }
    Ok(())
}
/// Evaluate once and return the clock instant the run observed. A system
/// clock is pinned to one UTC sample so the reported instant is exactly what
/// `NOW()`/`TODAY()` saw (the engine also samples once per request).
fn evaluate(
    engine: &mut Engine<WBResolver>,
    options: &XlsxRecalculateOptions,
) -> Result<Option<DateTime<Utc>>, IoError> {
    checkpoint(&options.cancel)?;
    let now = clock_instant(options);
    #[cfg(feature = "system-clock")]
    if let (Some(now), DeterministicMode::Disabled { timezone }) =
        (now, &options.eval_config.deterministic_mode)
    {
        engine.set_clock(std::sync::Arc::new(
            formualizer_eval::timezone::FixedClock::new(now, timezone.clone()),
        ));
    }
    engine.evaluate_all_for_snapshot(options.cancel.clone())?;
    checkpoint(&options.cancel)?;
    Ok(now)
}
fn unchanged(
    bytes: &[u8],
    formula_count: usize,
    summary: RecalculateSummary,
) -> XlsxRecalculateResult {
    XlsxRecalculateResult {
        bytes: bytes.to_vec(),
        summary,
        formula_cells: formula_count,
        cache_cells_changed: 0,
        worksheet_parts_changed: 0,
        clock_now_utc: None,
        external_links_used: 0,
    }
}
/// The validated value of one source formula, exactly as the scalar writer
/// requires it: present, 1x1 arrays coerced, ingested as a formula (or a
/// coerced parse error) and current.
fn validated_result(
    value: Option<LiteralValue>,
    has_formula: bool,
    staleness: Staleness,
    coerced: bool,
    sheet: &str,
) -> Result<LiteralValue, IoError> {
    let mut value = value.ok_or_else(|| unsupported("absent formula result", sheet))?;
    if matches!(&value,LiteralValue::Array(rows) if rows.len()==1 && rows[0].len()==1) {
        value = value
            .coerce_to_single_value()
            .map_err(|_| unsupported("non-scalar result", "cache-only writer"))?;
    }
    if !(has_formula || (matches!(value, LiteralValue::Error(_)) && coerced)) {
        return Err(unsupported("source formula was not ingested", sheet));
    }
    if staleness != Staleness::Current {
        return Err(unsupported(
            format!("formula result is not current: {staleness:?} after evaluation"),
            sheet,
        ));
    }
    Ok(value)
}
/// Count one evaluated source formula (anchors included) in the summary.
/// A `#NAME?` without a message gets the reason its own formula gives.
fn record_result(
    summary: &mut RecalculateSummary,
    engine: &Engine<WBResolver>,
    sheet: &str,
    cell: &sheet::Cell,
    value: &LiteralValue,
    limit: usize,
) {
    let address = &cell.address;
    let location = || format!("{sheet}!{address}");
    match value {
        LiteralValue::Error(error)
            if error.kind == formualizer_common::ExcelErrorKind::Name
                && error.message.is_none() =>
        {
            let mut error = error.clone();
            error.message = error_reasons::name_error_reason(engine, sheet, cell.row, cell.col);
            summary.record(sheet, location, &LiteralValue::Error(error), limit);
        }
        _ => summary.record(sheet, location, value, limit),
    }
}
/// Package edits produced by a publication path.
struct Publication {
    summary: RecalculateSummary,
    changed: usize,
    /// Worksheet replacements, plus metadata, relationship
    /// and content-type edits.
    edits: package::Edits,
    worksheets: usize,
}
/// Account a replaced (`old` bytes) or added (`old == 0`) member against
/// the expanded-output bound.
fn expand(
    expanded: &mut usize,
    old: usize,
    new: usize,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    *expanded = expanded
        .checked_sub(old)
        .and_then(|n| n.checked_add(new))
        .ok_or_else(|| unsupported("expanded output overflow", "workbook"))?;
    if *expanded > options.limits.max_expanded_bytes {
        return Err(unsupported("expanded output byte limit", "workbook"));
    }
    Ok(())
}
/// Apply one worksheet's patches within the worksheet and expanded-output
/// bounds.
fn replace_sheet(
    publication: &mut Publication,
    expanded: &mut usize,
    sheet: &package::Sheet,
    data: &[u8],
    patches: Vec<Patch>,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if patches.is_empty() {
        return Ok(());
    }
    let patched = apply_patches(data, patches, options.limits.max_worksheet_bytes)?;
    expand(expanded, data.len(), patched.len(), options)?;
    publication
        .edits
        .replace
        .insert(sheet.part.clone(), patched);
    publication.worksheets += 1;
    Ok(())
}
/// Validate every evaluated result, build the worksheet edits and write the
/// package. Nothing is published before every check passed.
fn publish(
    bytes: &[u8],
    ingested: Ingested<'_>,
    formula_count: usize,
    options: &XlsxRecalculateOptions,
    clock: &mut PhaseClock,
) -> Result<XlsxRecalculateResult, IoError> {
    let Ingested {
        mut archive,
        sheets,
        plans,
        engine,
        metadata,
        refuse_parse_failures: _,
        external_links_used,
        external_first_read: _,
    } = ingested;
    let coerced: HashSet<_> = engine
        .formula_parse_diagnostics()
        .iter()
        .filter(|d| d.policy == FormulaParsePolicy::CoerceToError)
        .map(|d| (d.sheet.clone(), d.row, d.col))
        .collect();
    // Declared central-directory sizes: ZIP7's decompressed_size() is None
    // for members with data descriptors, which admission accepts.
    let mut expanded = package::expanded_size(&mut archive)?;
    let Publication {
        mut summary,
        changed,
        edits,
        worksheets,
    } = spill_publication(
        &engine,
        &mut archive,
        &sheets,
        &plans,
        metadata.as_ref(),
        &coerced,
        &mut expanded,
        options,
    )?;
    clock.lap("publication (validate results, build patches)");
    summary.status = if summary.errors == 0 {
        RecalculateStatus::Success
    } else {
        RecalculateStatus::ErrorsFound
    };
    checkpoint(&options.cancel)?;
    if edits.is_empty() {
        if bytes.len() > options.limits.max_output_bytes {
            return Err(unsupported("output byte limit", "XLSX package"));
        }
        let mut result = unchanged(bytes, formula_count, summary);
        result.external_links_used = external_links_used;
        return Ok(result);
    }
    let output = package::rewrite(bytes, &mut archive, &edits, options)?;
    clock.lap("package rewrite");
    if !edits.add.is_empty() {
        // An added part must agree with its relationship and content type.
        package::check_output(&output, options)?;
    }
    checkpoint(&options.cancel)?;
    Ok(XlsxRecalculateResult {
        bytes: output,
        summary,
        formula_cells: formula_count,
        cache_cells_changed: changed,
        worksheet_parts_changed: worksheets,
        clock_now_utc: None,
        external_links_used,
    })
}
/// Publication: scalar caches and dynamic-array geometry, plus, when an
/// anchor needs
/// a new XLDAPR binding, the metadata part edit (or addition with its
/// relationship and content-type override).
#[allow(clippy::too_many_arguments)]
fn spill_publication(
    engine: &Engine<WBResolver>,
    archive: &mut package::Archive<'_>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    metadata: Option<&dynamic_metadata::DynamicMetadata>,
    coerced: &HashSet<(String, u32, u32)>,
    expanded: &mut usize,
    options: &XlsxRecalculateOptions,
) -> Result<Publication, IoError> {
    let mut binder = dynamic_metadata::Binder::new(metadata);
    let planned = plan_spill_publication(
        engine,
        sheets,
        plans,
        coerced,
        options,
        &mut |sheet, request| {
            if !request.multi_cell {
                return Err(unsupported(
                    "dynamic array binding requested for a one-cell result",
                    format!("{sheet} R{}C{}", request.row, request.col),
                ));
            }
            binder.bind(options)
        },
    )?;
    let mut publication = Publication {
        summary: planned.summary,
        changed: planned.changed,
        edits: package::Edits::default(),
        worksheets: 0,
    };
    for ((sheet, plan), patches) in sheets.iter().zip(plans).zip(planned.patches) {
        checkpoint(&options.cancel)?;
        replace_sheet(
            &mut publication,
            expanded,
            sheet,
            &plan.data,
            patches,
            options,
        )?;
    }
    if let Some(part) = binder.finish(options)? {
        checkpoint(&options.cancel)?;
        let edits = &mut publication.edits;
        if part.added {
            expand(expanded, 0, part.bytes.len(), options)?;
            for (name, data) in [
                (
                    package::WORKBOOK_RELS,
                    package::add_relationship(
                        archive,
                        "xl/workbook.xml",
                        dynamic_metadata::SHEET_METADATA_RELATIONSHIP,
                        "metadata.xml",
                        options,
                    )?,
                ),
                (
                    "[Content_Types].xml",
                    package::add_override(
                        archive,
                        &part.part,
                        dynamic_metadata::SHEET_METADATA_CONTENT_TYPE,
                        options,
                    )?,
                ),
            ] {
                let old = archive
                    .by_name(name)
                    .map_err(|e| IoError::from_backend("zip", e))?
                    .size();
                expand(expanded, old as usize, data.len(), options)?;
                edits.replace.insert(name.to_owned(), data);
            }
            edits.add.insert(part.part, part.bytes);
        } else {
            let old = metadata.map_or(0, |m| m.source_len());
            expand(expanded, old, part.bytes.len(), options)?;
            edits.replace.insert(part.part, part.bytes);
        }
    }
    Ok(publication)
}
/// [`geometry::BindingResolver`] with the worksheet name.
type SheetBindingResolver<'a> =
    dyn FnMut(&str, geometry::BindingRequest) -> Result<u32, IoError> + 'a;
/// Validated projection and geometry of every worksheet, not yet applied.
struct SpillPlan {
    summary: RecalculateSummary,
    changed: usize,
    /// Ordered patches per worksheet, parallel to the sheet plans.
    patches: Vec<Vec<Patch>>,
}
/// Project every source formula (nothing materialized), check the spill
/// bounds, then build one ordered geometry plan per worksheet, reading each
/// member at the final engine state. `bind` resolves a requested XLDAPR
/// binding to a one-based `cm`.
fn plan_spill_publication(
    engine: &Engine<WBResolver>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    coerced: &HashSet<(String, u32, u32)>,
    options: &XlsxRecalculateOptions,
    bind: &mut SheetBindingResolver<'_>,
) -> Result<SpillPlan, IoError> {
    let date_system = engine.config.date_system;
    let mut publication = SpillPlan {
        summary: RecalculateSummary::default(),
        changed: 0,
        patches: Vec::with_capacity(plans.len()),
    };
    // 1. Project every source formula; nothing is materialized yet.
    let mut projected = Vec::with_capacity(plans.len());
    for (sheet, plan) in sheets.iter().zip(plans) {
        // Scalar caches are encoded into patches right away, as on the
        // pre-spill writer; only anchors are retained for the bounds pass.
        let mut scalar_patches = Vec::new();
        let mut anchors = Vec::new();
        for (i, cell) in plan.cells.iter().enumerate() {
            checkpoint(&options.cancel)?;
            let prior = plan
                .ownership
                .anchors
                .get(&(cell.row, cell.col))
                .filter(|a| a.formula == i);
            let summary = &mut publication.summary;
            let projection = result_projection::project_formula(
                engine,
                &sheet.name,
                cell,
                i,
                prior,
                coerced.contains(&(sheet.name.clone(), cell.row, cell.col)),
                date_system,
                &mut |value| {
                    record_result(
                        summary,
                        engine,
                        &sheet.name,
                        cell,
                        value,
                        options.error_location_limit,
                    )
                },
            )?;
            match projection {
                result_projection::Projection::Scalar(cache) => {
                    if !cache.matches(cell) {
                        publication.changed += 1;
                        cache_patches(&plan.data, cell, &cache, &mut scalar_patches);
                    }
                }
                result_projection::Projection::Anchor(anchor) => {
                    check_spill_policy(&options.eval_config.spill)?;
                    use result_projection::Shape;
                    let spill = match anchor.shape {
                        Shape::Spill(extent) => Some(extent),
                        Shape::Collapsed | Shape::Blocked | Shape::Error => None,
                    };
                    // A source record keeps describing the anchor unless it
                    // is a collapsed record and the result spills again.
                    let binding = match anchor.prior {
                        Some(prior)
                            if !prior
                                .binding
                                .is_some_and(|b| b.collapsed && spill.is_some()) =>
                        {
                            geometry::Binding::Keep
                        }
                        _ => geometry::Binding::NeedsXldapr,
                    };
                    anchors.push(geometry::AnchorEdit {
                        row: anchor.row,
                        col: anchor.col,
                        formula: anchor.formula,
                        spill,
                        cache: anchor.cache,
                        binding,
                    });
                }
            }
        }
        projected.push((scalar_patches, anchors));
    }
    // 2. Bounds before any member value is read: width, merges, generated
    //    serialized cells and the logical area, within the existing limits.
    let mut serialized = 0u64;
    let mut inserted = 0u64;
    let mut logical = 0u64;
    // A sheet with a new spill but no admitted anchor gets its source index
    // now, from the retained worksheet bytes; sheets without any anchor
    // projection never build one.
    let mut built = Vec::with_capacity(plans.len());
    for (plan, (_, anchors)) in plans.iter().zip(&projected) {
        checkpoint(&options.cancel)?;
        built.push(if plan.index.is_none() && !anchors.is_empty() {
            match sheet::scan(&plan.data, options, sheet::Mode::Indexed, &mut 0, &mut 0)? {
                sheet::Scanned::Done(scan) => scan.index,
                sheet::Scanned::NeedsIndex => None,
            }
        } else {
            None
        });
    }
    let indexes: Vec<Option<&sheet::SourceIndex>> = plans
        .iter()
        .zip(&built)
        .map(|(plan, built)| plan.index.as_ref().or(built.as_ref()))
        .collect();
    for ((plan, (_, anchors)), index) in plans.iter().zip(&projected).zip(&indexes) {
        serialized = serialized.saturating_add(plan.serialized as u64);
        let preflight = match index {
            Some(index) => geometry::preflight(index, anchors, options)?,
            None if anchors.is_empty() => geometry::Preflight::default(),
            None => {
                return Err(unsupported(
                    "spill geometry without a source index",
                    "worksheet",
                ));
            }
        };
        inserted = inserted.saturating_add(preflight.inserted);
        let rows = plan.bounds.0.max(preflight.bounds.0);
        let cols = plan.bounds.1.max(preflight.bounds.1);
        logical = logical.saturating_add(u64::from(rows) * u64::from(cols));
    }
    if serialized.saturating_add(inserted) > options.limits.max_cells as u64 {
        return Err(unsupported("generated spill cell limit", "workbook"));
    }
    if logical > options.limits.max_cells as u64 {
        return Err(unsupported("workbook logical cell limit", "workbook"));
    }
    // 3. One ordered geometry plan per worksheet.
    for (((sheet, plan), (mut patches, anchors)), index) in
        sheets.iter().zip(plans).zip(projected).zip(indexes)
    {
        let Some(index) = index.filter(|_| !anchors.is_empty()) else {
            // No dynamic array on this sheet: scalar caches only.
            ingest_view::coalesce(&mut patches)?;
            publication.patches.push(patches);
            continue;
        };
        let edits = geometry::plan(
            plan,
            index,
            &anchors,
            &mut |anchor, row, col| {
                result_projection::member(
                    engine,
                    &sheet.name,
                    (anchor.row, anchor.col),
                    row,
                    col,
                    date_system,
                )
            },
            &mut |request| bind(&sheet.name, request),
            &options.cancel,
        )?;
        publication.changed += edits.caches_changed;
        patches.extend(edits.patches);
        // Scalar and geometry edits touch disjoint cells; merging them
        // refuses any equal-offset insertion conflict or overlap.
        ingest_view::coalesce(&mut patches)?;
        publication.patches.push(patches);
    }
    Ok(publication)
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

#[cfg(test)]
mod display_precision_tests {
    use super::display_equal;
    // Excel's 17-digit cache spellings are kept verbatim.
    #[allow(clippy::excessive_precision)]
    #[test]
    fn one_unit_in_the_15th_significant_digit() {
        for (a, b, equal) in [
            (53433.999999999949, 53433.99999999998, true),
            (8.8664999999999985, 8.8665, true),
            (-8.8664999999999985, -8.8665, true),
            (1.00000000000001, 1.0, true),
            (1.00000000000002, 1.0, false),
            (9.99999999999995, 10.0, true),
            (9.9999999999998, 10.0, false),
            // 1e23 is not a double: its neighbours straddle the power of ten.
            (99999999999999991611392.0, 1.0000000000000001e23, true),
            (0.0, -0.0, true),
            (0.0, 5.551115123125783e-17, false),
            (0.0, f64::MIN_POSITIVE, false),
            (1.0, -1.0, false),
            (1e-20, -1e-20, false),
            (f64::MAX, f64::MAX * (1.0 - 1e-15), true),
            (f64::MAX, f64::MAX * (1.0 - 7e-15), false),
            (1.2345678901234567e-300, 1.2345678901234561e-300, true),
            // Subnormal magnitudes: the bound is the double nearest 10^(E-14).
            (5e-324, 1e-323, false),
            (1.000000000000001e-310, 1.0e-310, true),
            (f64::INFINITY, f64::INFINITY, false),
            (f64::NAN, f64::NAN, false),
            (f64::NAN, 1.0, false),
        ] {
            assert_eq!(display_equal(a, b), equal, "{a:e} vs {b:e}");
            assert_eq!(display_equal(b, a), equal, "{b:e} vs {a:e}");
        }
    }
}
