//! Native evaluation-text transport into the unchanged W1 formula ingress.
//! Source marks/text stay immutable for cache-only publication.
use super::{FormulaTextTransform, IoError, SheetPlan, sheet, unsupported};
use formualizer_eval::engine::{SourceCoord, SourceRect};
use quick_xml::events::{BytesStart, Event};
use std::borrow::Cow;
use std::collections::BTreeMap;

// These pure W1 modules have no workbook-reader dependency. Share their exact
// implementation until W5 removes the old backend, then give them one home.
use crate::source_formula_evidence as compressed_evidence;
mod formula_replay {
    // Resolve the unchanged replay module's import through the one permitted
    // native utility wrapper. Only error types are visible: no reader surface.
    mod calamine {
        pub(super) use super::super::expand_shared_formula_into;
        pub(super) use ::calamine::{Error, XlsxError};
    }
    include!("../backends/calamine/formula_replay.rs");
}
use formula_replay::OwnedSpoolFormulaRecord;

#[cfg(test)]
thread_local! {
    static UTILITY_CALLS: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(test)]
pub(super) fn take_utility_calls() -> (u64, u64) {
    UTILITY_CALLS.with(|calls| calls.replace((0, 0)))
}

/// Allowed pinned text utility, only on W1's uncertified placement fallback.
fn expand_shared_formula_into(
    formula: &str,
    start: (u32, u32),
    end: (u32, u32),
    out: &mut String,
) -> Result<(), calamine::XlsxError> {
    #[cfg(test)]
    UTILITY_CALLS.with(|calls| {
        let (e, p) = calls.get();
        calls.set((e + 1, p));
    });
    calamine::expand_shared_formula_into(formula, start, end, out)
}

/// Allowed pinned error-token utility. Scanner callers are coordinated at B4.
pub(in crate::cache_recalculate) fn parse_error_token(
    token: &str,
) -> Result<calamine::CellErrorType, calamine::XlsxError> {
    #[cfg(test)]
    UTILITY_CALLS.with(|calls| {
        let (e, p) = calls.get();
        calls.set((e, p + 1));
    });
    token.parse()
}

fn shared_source_index(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

#[derive(Default)]
pub(super) struct EvaluationTexts {
    texts: BTreeMap<usize, String>,
}
impl EvaluationTexts {
    /// Preserve the old surgical edit-conflict rule without editing XML.
    pub(super) fn new(mut transforms: Vec<FormulaTextTransform>) -> Result<Self, IoError> {
        transforms.sort_by_key(|t| (t.span.start, t.span.end));
        let mut previous = 0;
        let mut texts = BTreeMap::new();
        for transform in transforms {
            if transform.span.start < previous || transform.span.start > transform.span.end {
                return Err(unsupported("overlapping cache edit spans", "worksheet"));
            }
            previous = transform.span.end;
            texts.insert(transform.span.start, transform.text);
        }
        Ok(Self { texts })
    }
    fn text<'a>(&'a self, cell: &'a sheet::Cell, data: &'a [u8]) -> Result<Cow<'a, str>, IoError> {
        if let Some(text) = self.texts.get(&cell.formula_open.end) {
            return Ok(Cow::Borrowed(text));
        }
        let raw = &data[cell.formula_open.end..cell.formula_end];
        // Admission's XML 1.1 normalization differs from the pinned XML 1.0
        // formula reader only for literal NEL/LS (including CR+NEL). Numeric
        // character references were already decoded without normalization.
        if !raw.contains(&0xC2) && !raw.contains(&0xE2) {
            return Ok(Cow::Borrowed(&cell.formula_text));
        }
        let mut reader =
            quick_xml::Reader::from_reader(&data[cell.formula_open.start..cell.formula_end]);
        let mut text = String::new();
        loop {
            match reader.read_event().map_err(xml_error)? {
                Event::Text(t) => text.push_str(&t.xml10_content().map_err(xml_error)?),
                Event::GeneralRef(r) => match r.decode().map_err(xml_error)?.as_ref() {
                    "amp" => text.push('&'),
                    "lt" => text.push('<'),
                    "gt" => text.push('>'),
                    "quot" => text.push('"'),
                    "apos" => text.push('\''),
                    _ => text.push(
                        r.resolve_char_ref()
                            .map_err(xml_error)?
                            .ok_or_else(|| unsupported("unknown XML entity", "XLSX XML"))?,
                    ),
                },
                Event::Eof => break,
                _ => {}
            }
        }
        Ok(Cow::Owned(text))
    }
}
fn xml_error(error: impl std::error::Error) -> IoError {
    IoError::from_backend("xlsx-formula", error)
}
fn opening(bytes: &[u8]) -> Result<BytesStart<'_>, IoError> {
    match quick_xml::Reader::from_reader(bytes)
        .read_event()
        .map_err(xml_error)?
    {
        Event::Start(e) | Event::Empty(e) => Ok(e),
        _ => Err(unsupported("unavailable formula opening tag", "worksheet")),
    }
}
fn reader_error(error: calamine::XlsxError) -> IoError {
    IoError::Calamine(calamine::Error::Xlsx(error))
}

/// Source admission already proves the decoded coordinate and grid bounds.
/// Preserve the pinned reader's remaining raw-byte rejection, not a new policy.
fn raw_coordinate(raw: &[u8]) -> Result<(), calamine::XlsxError> {
    for byte in raw {
        if !byte.is_ascii_alphanumeric() {
            return Err(calamine::XlsxError::Alphanumeric(*byte));
        }
    }
    Ok(())
}

/// Convert one retained source formula into the pinned metadata envelope,
/// without a worksheet reader or formula-sized transport copy.
fn source_record(
    plan: &SheetPlan,
    cell: &sheet::Cell,
    texts: &EvaluationTexts,
    sequence: u64,
) -> Result<OwnedSpoolFormulaRecord, IoError> {
    let coord0 = SourceCoord {
        row: cell.row - 1,
        col: cell.col - 1,
    };
    let c = opening(&plan.data[cell.span.start..cell.open_end])?;
    let mut raw_kind = None;
    for attribute in c.attributes().with_checks(false) {
        let attribute = attribute.map_err(xml_error)?;
        match attribute.key.as_ref() {
            b"r" => raw_coordinate(&attribute.value).map_err(reader_error)?,
            b"t" => raw_kind = Some(attribute.value),
            _ => {}
        }
    }
    // The old view clears unreadable/typed formula caches; all others must
    // still exhibit raw type refusals even though no cache enters Arrow.
    let cleared = !sheet::readable_scalar_cache(cell, &plan.data)
        || matches!(cell.kind.as_deref(), Some("s" | "inlineStr" | "d"));
    let kind_kept = !cleared || cell.kind.as_deref() == Some("n");
    let cache_error = if kind_kept && (cleared || cell.value.is_some()) {
        raw_kind
            .as_deref()
            .filter(|kind| {
                !matches!(
                    *kind,
                    b"s" | b"b" | b"d" | b"e" | b"str" | b"n" | b"inlineStr" | b"is"
                )
            })
            .map(|kind| {
                calamine::XlsxError::CellTAttribute(String::from_utf8_lossy(kind).into_owned())
            })
    } else {
        None
    };
    let f = opening(&plan.data[cell.formula_open.clone()])?;
    let mut t = None;
    let mut si = None;
    let mut reference = None;
    for attribute in f.attributes().with_checks(false) {
        let attribute = attribute.map_err(xml_error)?;
        match attribute.key.as_ref() {
            b"t" => t = Some(attribute.value),
            b"si" => si = Some(attribute.value),
            b"ref" => reference = Some(attribute.value),
            _ => {}
        }
    }
    let record = (|| {
        if t.as_deref() == Some(b"shared") {
            let index = si.as_deref().ok_or(calamine::XlsxError::Unexpected(
                "si attribute is mandatory if it is shared",
            ))?;
            let shared_index = (!index.is_empty() && index.iter().all(u8::is_ascii_digit))
                .then(|| std::str::from_utf8(index).ok()?.parse::<usize>().ok())
                .flatten()
                .ok_or(calamine::XlsxError::Unexpected(
                    "si attribute must be a number",
                ))?;
            if let Some(reference) = reference.as_deref() {
                for coord in reference.split(|b| *b == b':') {
                    raw_coordinate(coord)?;
                }
                let range = cell.shared_rect().expect("admission proved shared ref");
                Ok(OwnedSpoolFormulaRecord::SharedAnchor {
                    sequence,
                    coord0,
                    shared_index,
                    declared_range: Some(SourceRect {
                        start: SourceCoord {
                            row: range.first_row - 1,
                            col: range.first_col - 1,
                        },
                        end: SourceCoord {
                            row: range.last_row - 1,
                            col: range.last_col - 1,
                        },
                    }),
                    text: texts
                        .text(cell, &plan.data)
                        .map_err(|error| {
                            calamine::XlsxError::Io(std::io::Error::other(error.to_string()))
                        })?
                        .into_owned(),
                })
            } else {
                Ok(OwnedSpoolFormulaRecord::SharedDescendant {
                    sequence,
                    coord0,
                    shared_index,
                })
            }
        } else {
            // An empty f still produces Normal("") in the pinned reader, even
            // when its raw t spelling is not recognized as shared metadata.
            Ok(OwnedSpoolFormulaRecord::Ordinary {
                sequence,
                coord0,
                text: texts
                    .text(cell, &plan.data)
                    .map_err(|error| {
                        calamine::XlsxError::Io(std::io::Error::other(error.to_string()))
                    })?
                    .into_owned(),
            })
        }
    })();
    // read_formula_record runs when f is encountered; a v preceding f can
    // therefore win an otherwise simultaneous raw metadata/type refusal.
    match (record, cache_error) {
        (Err(_formula), Some(cache))
            if cell
                .value
                .as_ref()
                .is_some_and(|v| v.span.start < cell.formula_open.start) =>
        {
            Err(reader_error(cache))
        }
        (Err(formula), _) => Err(reader_error(formula)),
        (Ok(_), Some(cache)) => Err(reader_error(cache)),
        (Ok(record), None) => Ok(record),
    }
}

/// Only on a failed value plane, preserve the reader's interleaved envelope
/// error order against a later literal/row error. This visits retained formula
/// headers before that byte offset, never rescans the worksheet body.
pub(super) fn validate_reader_prefix(
    plan: &SheetPlan,
    texts: &EvaluationTexts,
    before: usize,
) -> Result<(), IoError> {
    for (sequence, cell) in plan
        .cells
        .iter()
        .enumerate()
        .take_while(|(_, cell)| cell.span.start < before)
    {
        source_record(plan, cell, texts, sequence as u64)?;
    }
    Ok(())
}

use super::{XlsxRecalculateOptions, checkpoint};
use crate::workbook::WBResolver;
use compressed_evidence::{EvidenceRecord, MonotonicFormulaEvidence};
use formualizer_eval::engine::{
    AstNodeId, DeferredFormulaPackage, Engine, ExplicitPartitionLegacyMembers,
    FormulaCompressedPreparation, FormulaCompressedSourceReport, FormulaFamilyGrouper,
    FormulaIngestRecord, FormulaPlaneMode, FormulaSpoolDiskPolicy, PartitionLegacyMember,
    PartitionLegacyMemberKind, PartitionReconciliation, PartitionedSourceFormulaFamily,
    SourceFamilyId, SourceFormulaFamily, SourceFormulaOrder,
};
use formula_replay::{
    CalamineDeferredFormulaReplay as SourceDeferredFormulaReplay, FormulaReplaySpool,
    FormulaReplayState, FormulaSpoolLimits, HybridFormulaReplaySpool,
    replay_spool_per_cell_filtered_with_family,
};
use rustc_hash::FxHashMap;
use std::sync::Arc;

#[derive(Clone, Copy, Default)]
pub(super) struct WorkbookSpoolUsage {
    pub bytes: u64,
    pub files: u32,
}
pub(super) struct FormulaPlane {
    pub records: Vec<FormulaIngestRecord>,
    pub report: FormulaCompressedSourceReport,
    pub families: Vec<SourceFormulaFamily>,
    pub partitioned: Vec<PartitionedSourceFormulaFamily>,
    pub preparation: Option<FormulaCompressedPreparation>,
    pub deferred: Option<DeferredFormulaPackage>,
    pub formulas_observed: usize,
    pub shared_tags: usize,
    pub spool_bytes: u64,
    pub spool_spilled: bool,
}
struct Staging {
    parse_cache: FxHashMap<String, Option<AstNodeId>>,
    records: Vec<FormulaIngestRecord>,
    grouper: FormulaFamilyGrouper,
}
impl Staging {
    fn new() -> Self {
        let mut parse_cache = FxHashMap::default();
        parse_cache.reserve(4096);
        Self {
            parse_cache,
            records: Vec::new(),
            grouper: FormulaFamilyGrouper::new(),
        }
    }
    /// Unchanged ordinary/legacy fallback staging policy from the W1 adapter.
    fn stage(
        &mut self,
        engine: &mut Engine<WBResolver>,
        sheet: &str,
        coord: SourceCoord,
        formula: &str,
        shared: Option<usize>,
    ) -> Result<(), calamine::Error> {
        let (row, col) = (coord.row + 1, coord.col + 1);
        let normalized = if formula.starts_with('=') {
            formula.to_owned()
        } else {
            format!("={formula}")
        };
        if engine.config.defer_graph_building {
            engine.stage_formula_text(sheet, row, col, normalized);
            return Ok(());
        }
        self.grouper.note_formula();
        let record = if let Some(cached) = self.parse_cache.get(&normalized) {
            self.grouper.note_parse_cache_hit();
            cached.map(|ast_id| {
                engine.note_staged_formula(&mut self.grouper, row, col, ast_id);
                FormulaIngestRecord::new(row, col, ast_id, Some(Arc::from(normalized.as_str())))
            })
        } else if let Some(record) = engine.stage_relocated_text(
            &mut self.grouper,
            row,
            col,
            &normalized,
            shared.map(|index| index as u64),
        ) {
            Some(record)
        } else {
            self.grouper.note_parse(normalized.len());
            let parsed = formualizer_parse::parser::parse(&normalized);
            let parsed_text = parsed.is_ok();
            let parsed = match parsed {
                Ok(ast) => Some(ast),
                Err(error) => engine
                    .handle_formula_parse_error(sheet, row, col, &normalized, error.to_string())
                    .map_err(engine_error)?,
            };
            match parsed {
                Some(ast) => {
                    let record = engine.stage_formula_ast(&mut self.grouper, row, col, &ast, None);
                    if parsed_text {
                        engine.note_parsed_text(
                            &mut self.grouper,
                            row,
                            col,
                            &normalized,
                            &ast,
                            &record,
                            shared.map(|index| index as u64),
                        );
                    }
                    if record.is_family_member() {
                        Some(record)
                    } else {
                        let id = record.ast_id;
                        self.parse_cache.insert(normalized.clone(), Some(id));
                        Some(FormulaIngestRecord::new(
                            row,
                            col,
                            id,
                            Some(Arc::from(normalized)),
                        ))
                    }
                }
                None => {
                    self.parse_cache.insert(normalized, None);
                    None
                }
            }
        };
        if let Some(record) = record {
            self.records.push(record);
        }
        Ok(())
    }
}
fn engine_error(error: impl std::fmt::Display) -> calamine::Error {
    calamine::Error::Io(std::io::Error::other(error.to_string()))
}

/// Prepare the same W1 evidence, bounded replay and certification proposals as
/// the adapter, consuming only retained source-order formula records. All
/// graph mutation and compressed replay remain in the existing engine ingress.
pub(super) fn stage(
    engine: &mut Engine<WBResolver>,
    sheet: &str,
    sheet_instance: u32,
    plan: &SheetPlan,
    texts: &EvaluationTexts,
    usage: WorkbookSpoolUsage,
    options: &XlsxRecalculateOptions,
) -> Result<FormulaPlane, IoError> {
    checkpoint(&options.cancel)?;
    let deferred = engine.config.defer_graph_building;
    let eager = !deferred && engine.config.formula_plane_mode == FormulaPlaneMode::Off;
    let mut replay = eager.then(FormulaReplayState::default);
    let limits = engine.workbook_load_limits();
    let mut spool = (!eager).then(|| {
        HybridFormulaReplaySpool::new(FormulaSpoolLimits {
            sheet_bytes: limits.max_formula_spool_bytes_per_sheet,
            workbook_bytes_remaining: limits
                .max_formula_spool_bytes_per_workbook
                .saturating_sub(usage.bytes),
            workbook_bytes_used: usage.bytes,
            memory_prefix_bytes: limits.formula_spool_memory_prefix_bytes,
            memory_only_bytes: limits.max_formula_spool_memory_bytes,
            allow_disk: limits.formula_spool_disk_policy == FormulaSpoolDiskPolicy::NativeSpill,
            spill_files_remaining: limits
                .max_formula_spool_files_per_workbook
                .saturating_sub(usage.files),
            spill_files_limit: limits.max_formula_spool_files_per_workbook,
        })
    });
    let mut staging = Staging::new();
    let mut evidence = MonotonicFormulaEvidence::new();
    let mut coordinates = Vec::new();
    let mut count = 0usize;
    let mut shared_tags = 0;
    for cell in &plan.cells {
        checkpoint(&options.cancel)?;
        let record = source_record(plan, cell, texts, count as u64)?;
        let coord0 = SourceCoord {
            row: cell.row - 1,
            col: cell.col - 1,
        };
        let observation = match &record {
            OwnedSpoolFormulaRecord::Ordinary { .. } => EvidenceRecord::Ordinary,
            OwnedSpoolFormulaRecord::SharedAnchor {
                shared_index,
                declared_range,
                text,
                ..
            } => {
                shared_tags += 1;
                EvidenceRecord::Anchor {
                    family: SourceFamilyId {
                        sheet_instance,
                        source_index: shared_source_index(*shared_index),
                    },
                    range: *declared_range,
                    text,
                }
            }
            OwnedSpoolFormulaRecord::SharedDescendant { shared_index, .. } => {
                shared_tags += 1;
                EvidenceRecord::Descendant {
                    family: SourceFamilyId {
                        sheet_instance,
                        source_index: shared_source_index(*shared_index),
                    },
                }
            }
            OwnedSpoolFormulaRecord::Unsupported { .. } => EvidenceRecord::Unsupported,
        };
        evidence.observe_ordered(coord0, SourceFormulaOrder::new(count as u64), observation);
        if deferred {
            coordinates.push(coord0);
        }
        if let Some(spool) = spool.as_mut() {
            spool
                .append(record.as_borrowed())
                .map_err(|error| IoError::Calamine(engine_error(error)))?;
        } else if let Some(replay) = replay.as_mut() {
            replay
                .emit_record(
                    record,
                    sheet,
                    |_, _| true,
                    |_, coord, text, shared| {
                        checkpoint(&options.cancel).map_err(engine_error)?;
                        staging.stage(engine, sheet, coord, text, shared)
                    },
                )
                .map_err(IoError::Calamine)?;
        }
        count += 1;
    }
    let output = evidence.finish();
    let mut report = output.report;
    let families = output.families;
    let partitioned = output
        .fragmented
        .into_iter()
        .map(|proposal| {
            let mut members: Vec<_> = proposal
                .fallback_members
                .into_iter()
                .map(|coord| PartitionLegacyMember {
                    coord,
                    kind: PartitionLegacyMemberKind::SharedFamilyMember,
                })
                .collect();
            let mut ordinary_exceptions = 0u64;
            let mut holes = 0u64;
            for exclusion in proposal.exclusions {
                match exclusion {
                    compressed_evidence::SourceExclusion::Hole(_) => holes += 1,
                    compressed_evidence::SourceExclusion::OrdinaryFormula(coord) => {
                        ordinary_exceptions += 1;
                        members.push(PartitionLegacyMember {
                            coord,
                            kind: PartitionLegacyMemberKind::OrdinaryException,
                        });
                    }
                }
            }
            Ok(PartitionedSourceFormulaFamily {
                source_id: proposal.source_id,
                source_order: proposal.source_order,
                template_origin0: proposal.anchor_coord0,
                template_text: proposal.anchor_text,
                declared: proposal.declared,
                surviving_member_count: proposal.member_count,
                fragments: proposal.fragments,
                legacy_members: ExplicitPartitionLegacyMembers::try_new(members)
                    .map_err(|error| IoError::Calamine(engine_error(error)))?,
                reconciliation: PartitionReconciliation {
                    shared_members: proposal.member_count,
                    ordinary_exceptions,
                    holes,
                },
            })
        })
        .collect::<Result<Vec<_>, IoError>>()?;
    let spool_bytes = if count == 0 {
        0
    } else {
        spool.as_ref().map_or(0, |s| s.encoded_bytes())
    };
    let spool_spilled = spool.as_ref().is_some_and(|s| s.spilled());
    if std::env::var("FZ_DEBUG_LOAD")
        .ok()
        .is_some_and(|value| value != "0")
    {
        eprintln!(
            "[fz][native] formula spool storage={:?}",
            spool.as_ref().map(FormulaReplaySpool::storage_kind)
        );
    }
    report.source_formula_records_spooled = if eager { 0 } else { count as u64 };
    report.source_spool_encoded_bytes = spool_bytes;
    report.source_spool_peak_memory_bytes = spool.as_ref().map_or(0, |s| s.peak_memory_bytes());
    report.source_spool_spilled_bytes = if spool_spilled { spool_bytes } else { 0 };
    report.source_spool_spill_files = u64::from(spool_spilled);
    checkpoint(&options.cancel)?;
    let preparation = if engine.config.formula_plane_mode
        == FormulaPlaneMode::AuthoritativeExperimental
        && !deferred
    {
        Some(
            engine
                .source_formula_ingress()
                .prepare_eager_proposals(
                    sheet,
                    &families,
                    &partitioned,
                    count as u64,
                    Box::new(SourceDeferredFormulaReplay::new(
                        spool.take().expect("formula spool"),
                        sheet.to_owned(),
                        sheet_instance,
                    )),
                )
                .map_err(|error| IoError::Calamine(engine_error(error)))?,
        )
    } else {
        None
    };
    if preparation.is_none() && !deferred && !eager {
        report.source_spool_replays = 1;
        replay_spool_per_cell_filtered_with_family(
            spool.as_mut().expect("formula spool"),
            sheet,
            |_| false,
            |coord, text, shared| {
                checkpoint(&options.cancel).map_err(engine_error)?;
                staging.stage(engine, sheet, coord, text, shared)
            },
        )
        .map_err(IoError::Calamine)?;
    }
    engine.finish_family_grouper(&mut staging.grouper);
    let package = deferred.then(|| {
        DeferredFormulaPackage::new_with_source_coordinates(
            sheet.to_owned(),
            report.clone(),
            families.clone(),
            partitioned.clone(),
            coordinates,
            Box::new(SourceDeferredFormulaReplay::new(
                spool.take().expect("deferred formula spool"),
                sheet.to_owned(),
                sheet_instance,
            )),
        )
        .with_complete_coordinate_coverage()
    });
    Ok(FormulaPlane {
        records: staging.records,
        report,
        families,
        partitioned,
        preparation,
        deferred: package,
        formulas_observed: count,
        shared_tags,
        spool_bytes,
        spool_spilled,
    })
}

#[cfg(test)]
#[path = "tests/native_formula_sink.rs"]
mod tests;
