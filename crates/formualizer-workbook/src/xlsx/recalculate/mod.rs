//! Strict cache-only XLSX recalculation, without a rich document model.
//! Unsupported package/formula cases fail before any output is published.
pub(super) use crate::xlsx::metadata::dynamic as dynamic_metadata;
mod error_reasons;
pub(crate) use crate::cached_external_links as external_links;
mod geometry;
mod guards;
pub(super) mod ownership;
pub(super) use guards::inert as inert_sheets;
pub(super) mod publication;
pub(super) use publication as ingest_view;
use publication::patches::cache_patches;
pub(super) use publication::patches::{BoundedOutput, Patch, apply_patches};
#[cfg(test)]
use publication::{plan_spill_publication, publish_caches as publish};
mod transforms;
pub(super) use transforms::legacy as legacy_intersection;
mod admission;
pub(super) use crate::xlsx::ingest::formulas;
mod ingestion;
pub(super) use crate::xlsx::ingest::names;
pub(super) use crate::xlsx::ingest::values;
pub(super) use crate::xlsx::metadata;
pub(crate) use crate::xlsx::package;
mod clock;
mod projection;
mod summary;
use summary::{record_result, unchanged, validated_result};
pub(super) mod shared_qualifiers;
pub(super) use crate::xlsx::metadata::tables;
pub(crate) use crate::xlsx::worksheet as sheet;
pub(super) use transforms::tables as table_lowering;
#[cfg(test)]
pub(super) mod tests;
use guards::visibility as visibility_guard;
#[cfg(not(feature = "system-clock"))]
pub(super) use guards::wall_clock as wall_clock_guard;
pub(super) mod xml;

#[cfg(test)]
use crate::cache_recalculate::XlsxRecalculateLimits;
use crate::recalculate::{RecalculateStatus, RecalculateSummary};
pub(super) use crate::{IoError, workbook::WBResolver};
use chrono::{DateTime, Utc};
use clock::{PhaseClock, clock_instant};
use formualizer_common::{CellAddress, DateSystem, LiteralValue};
use formualizer_eval::engine::DeterministicMode;
use formualizer_eval::engine::inspect::{SnapshotOptions, Staleness};
use formualizer_eval::engine::{
    CancelToken, Engine, FormulaParsePolicy, SpillBoundsPolicy, SpillConfig, SpillConflictPolicy,
};
use ingestion::ingest_source;
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::io::Cursor;
#[cfg(not(target_arch = "wasm32"))]
use std::io::Read;
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;
use std::ops::Range;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

pub(super) use crate::cache_recalculate::{
    ExternalLinkPolicy, XlsxRecalculateOptions, XlsxRecalculateResult,
};
pub(super) fn unsupported(feature: impl Into<String>, context: impl Into<String>) -> IoError {
    IoError::Unsupported {
        feature: feature.into(),
        context: context.into(),
    }
}
pub(super) fn checkpoint(token: &Option<CancelToken>) -> Result<(), IoError> {
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
    fn matches(&self, cell: &sheet::FormulaMark, bytes: &[u8]) -> bool {
        if cell.inline.is_some() {
            return false;
        }
        let Some(v) = &cell.value else {
            return false;
        };
        match (self, cell.kind.as_deref()) {
            (Self::Number(n), None | Some("n")) => display_equal(v.number, *n),
            (Self::Empty, None | Some("n")) => v.text(bytes).is_empty(),
            (Self::Boolean(b), Some("b")) => {
                matches!((v.text(bytes).trim(), b), ("1", true) | ("0", false))
            }
            (Self::Text(t), Some("str")) => v.text(bytes).as_ref() == t,
            (Self::Error(e), Some("e")) => v.text(bytes).as_ref() == e,
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
/// Evaluation text, separate from the immutable source formula and its marks.
/// The native sink consumes this text without constructing patched XML.
pub(super) struct FormulaTextTransform {
    pub(super) span: Range<usize>,
    pub(super) text: String,
}

/// Worksheet source, formula cells and prior-footprint dynamic-array
/// ownership. The per-cell source index exists only for worksheets with
/// admitted anchors; a sheet with a new spill builds it after evaluation.
pub(crate) struct SheetPlan {
    pub(super) tables: Vec<tables::Table>,
    pub(super) hidden_rows: Vec<u32>,
    /// `sheetFormatPr/@zeroHeight`: row visibility is not provable.
    pub(super) rows_hidden_by_default: bool,
    pub(super) active_filters: Vec<sheet::SourceRect>,
    pub(super) data: Vec<u8>,
    pub(crate) cells: Vec<sheet::FormulaMark>,
    pub(super) index: Option<sheet::SourceIndex>,
    pub(super) ownership: dynamic_metadata::SheetOwnership,
    /// Serialized `<c>` elements, counted toward the generated-cell bound.
    pub(super) serialized: usize,
    /// Logical `(max_row, max_col)` counted toward the workbook area bound.
    pub(super) bounds: (u32, u32),
}
pub(super) struct SourceAdmission<'a> {
    pub(super) archive: package::Archive<'a>,
    pub(super) sheets: Vec<package::Sheet>,
    pub(super) date_system: DateSystem,
    pub(super) plans: Vec<SheetPlan>,
    pub(super) formula_count: usize,
    values: Vec<admission::ValueResult>,
    pub(super) native_metadata_error: Option<IoError>,
    pub(super) metadata: Option<dynamic_metadata::DynamicMetadata>,
    /// External sources served from the link caches, when the workbook
    /// has links.
    pub(super) external: Option<external_links::Plan>,
}
/// Bounded package/worksheet admission. This also resolves the XLDAPR
/// metadata chain and returns disjoint prior-footprint ownership per
/// worksheet, or a precise unsupported error.
pub(super) fn admit_source<'a>(
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
    // Reader-shaped errors must not outrank source/table/ownership admission.
    let value_metadata = admission::Metadata::read(&mut archive, options);
    let mut values = Vec::with_capacity(sheets.len());
    let mut plans = Vec::new();
    let mut observed = 0;
    let mut logical_cells = 0u64;
    let mut formula_count = 0usize;
    let mut table_budget = tables::Budget::default();
    let mut table_names = HashSet::new();
    for sheet in &sheets {
        checkpoint(&options.cancel)?;
        let data = if sheet.inert {
            inert_sheets::EMPTY.as_bytes().to_vec()
        } else {
            package::take_part(
                &mut archive,
                &sheet.part,
                options.limits.max_worksheet_bytes,
            )?
        };
        let counted = (observed, logical_cells);
        let mut scanned = admission::scan(
            &data,
            &sheet.name,
            date_system,
            value_metadata.as_ref().ok(),
            options,
            if sheet.tables.is_empty() {
                sheet::ScanStrategy::Plain
            } else {
                sheet::ScanStrategy::TablePlain
            },
            (&mut observed, &mut logical_cells),
        )?;
        if matches!(scanned, admission::ScannedValues::NeedsIndex) {
            // Dynamic cell metadata: index this sheet only.
            (observed, logical_cells) = counted;
            scanned = admission::scan(
                &data,
                &sheet.name,
                date_system,
                value_metadata.as_ref().ok(),
                options,
                sheet::ScanStrategy::Indexed,
                (&mut observed, &mut logical_cells),
            )?;
        }
        let admission::ScannedValues::Done {
            scan,
            values: scanned_values,
        } = scanned
        else {
            return Err(unsupported("unindexed dynamic cell metadata", "worksheet"));
        };
        let mut scan = *scan;
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
                &format!("{}!{}", sheet.name, cell.address()),
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
        values.push(scanned_values);
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
    package::release_value_metadata(&mut archive);
    Ok(SourceAdmission {
        archive,
        sheets,
        date_system,
        plans,
        formula_count,
        values,
        native_metadata_error: value_metadata.err(),
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
    let mut ingested = ingest_source(admission, &options, &mut clock)?;
    let clock_now_utc = evaluate(&mut ingested.engine, &options)?;
    clock.lap("evaluate (incl. deferred graph build)");
    // Deferred graph building parses formulas during evaluation.
    refuse_parse_failure(&ingested.engine, ingested.refuse_parse_failures)?;
    clock.families(&ingested.engine);
    let first_read = ingested.external_first_read.take();
    let mut result =
        publication::publish_caches(bytes, ingested, formula_count, &options, &mut clock)?;
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
// This is only a prefilter for the AST-based refusal check. Retain string
// mentions conservatively, including literal INDIRECT targets and odd names.
fn formula_mentions_unimported_name(
    formula: &str,
    names: &std::collections::HashSet<String>,
    unicode: bool,
) -> bool {
    let contains = |token: &str| {
        names.contains(&token.to_ascii_lowercase())
            || (unicode && !token.is_ascii() && names.contains(&token.to_lowercase()))
    };
    let token_matches = |token: &str| {
        let folded = token.to_ascii_lowercase();
        let mut bare = folded.as_str();
        while let Some(rest) = ["_xlfn.", "_xlws.", "_xlpm.", "_xll."]
            .iter()
            .find_map(|prefix| bare.strip_prefix(prefix))
        {
            bare = rest;
        }
        contains(token) || (bare.len() != token.len() && contains(bare))
    };
    let normalized = format!("={}", formula.trim_start_matches('='));
    let Ok(tokens) = formualizer_parse::TokenStream::new(&normalized) else {
        // Never exclude a formula whose tokenization needs recovery.
        return true;
    };
    for i in 0..tokens.len() {
        let Some(token) = tokens.get(i) else { continue };
        let text = token.value.trim_end_matches('(');
        let text = text
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .unwrap_or(text);
        if token_matches(text) || token_matches(text.rsplit('!').next().unwrap_or(text)) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod name_prefilter_tests {
    use super::formula_mentions_unimported_name;
    use std::collections::HashSet;

    #[test]
    fn qualified_prefixed_and_literal_name_tokens_are_retained() {
        let names: HashSet<_> = ["bad", "a.b", "a\\b", "ñame", "a b", ".x", "abc?", "1abc"]
            .into_iter()
            .map(String::from)
            .collect();
        for formula in [
            "BAD+1",
            "Sheet2!BAD",
            "'My Sheet'!BAD",
            "_xlfn._xlws._xlpm.BAD",
            "a.b+1",
            "a\\b+1",
            "ÑAME+1",
            ".x+1",
            "abc?+1",
            "Sheet2!abc?",
            "1abc+1",
            "INDIRECT(\"a b\")",
            "INDIRECT(\"'My Sheet'!BAD\")",
            "\"BAD\"&\"x\"",
        ] {
            assert!(
                formula_mentions_unimported_name(formula, &names, true),
                "{formula}"
            );
        }
        assert!(!formula_mentions_unimported_name("BADLY+A1", &names, true));
        assert!(!formula_mentions_unimported_name("ÑAME+1", &names, false));
    }

    #[test]
    fn unused_name_prefilter_scales_with_formula_tokens() {
        let names = (0..8_000).map(|i| format!("nm_{i}")).collect();
        let started = std::time::Instant::now();
        for _ in 0..20_000 {
            assert!(!formula_mentions_unimported_name("A1+123", &names, true));
        }
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
    }
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
