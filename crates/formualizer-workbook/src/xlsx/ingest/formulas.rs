//! Evaluation-text staging and exact source-order formula replay.
//! Source marks/text stay immutable for cache-only publication.
use crate::xlsx::recalculate::{FormulaTextTransform, IoError, SheetPlan, sheet, unsupported};
use formualizer_eval::engine::{SourceCoord, SourceRect};
use quick_xml::events::Event;
use std::borrow::Cow;
use std::collections::BTreeMap;

#[cfg(test)]
use crate::xlsx::ingest::utilities::parse_error_token;
#[cfg(test)]
pub(in crate::xlsx) use crate::xlsx::ingest::utilities::take_utility_calls;
use crate::xlsx::ingest::{evidence as compressed_evidence, replay as formula_replay};
use formula_replay::OwnedSpoolFormulaRecord;

fn shared_source_index(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

#[derive(Default)]
pub(in crate::xlsx) struct EvaluationTexts {
    texts: BTreeMap<usize, String>,
}
impl EvaluationTexts {
    /// Preserve the old surgical edit-conflict rule without editing XML.
    pub(in crate::xlsx) fn new(mut transforms: Vec<FormulaTextTransform>) -> Result<Self, IoError> {
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
    fn text<'a>(
        &'a self,
        cell: &'a sheet::FormulaMark,
        data: &'a [u8],
        source_text: &'a str,
    ) -> Result<Cow<'a, str>, IoError> {
        if let Some(text) = self.texts.get(&cell.formula_open.end) {
            return Ok(Cow::Borrowed(text));
        }
        let raw = &data[cell.formula_open.end..cell.formula_end];
        // Admission's XML 1.1 normalization differs from the pinned XML 1.0
        // formula reader only for literal NEL/LS (including CR+NEL). Numeric
        // character references were already decoded without normalization.
        if !raw.contains(&0xC2) && !raw.contains(&0xE2) {
            return Ok(Cow::Borrowed(source_text));
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
/// Convert the scanner's source event using its already-validated raw envelope.
/// Transformed evaluation text stays separate from immutable source text/marks.
fn source_record(
    plan: &SheetPlan,
    cell: &sheet::FormulaMark,
    texts: &EvaluationTexts,
    sequence: u64,
) -> Result<OwnedSpoolFormulaRecord, IoError> {
    use crate::xlsx::worksheet::events::FormulaKind;
    if let Some(evidence) = &cell.compatibility {
        evidence.validate()?;
    }
    let event = cell.source_event(sequence);
    debug_assert_eq!(event.offset, event.envelope.cell.start);
    let coord0 = SourceCoord {
        row: event.row - 1,
        col: event.col - 1,
    };
    let raw_shared = cell
        .compatibility
        .as_ref()
        .map_or(cell.formula_kind == "shared", |evidence| evidence.shared);
    let evaluation = |text| {
        texts
            .text(cell, &plan.data, text)
            .map(Cow::into_owned)
            .map_err(|error| {
                IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                    crate::xlsx::types::XlsxError::Io(std::io::Error::other(error.to_string())),
                ))
            })
    };
    if !raw_shared {
        let text = match event.kind {
            FormulaKind::Ordinary { text } | FormulaKind::SharedAnchor { text, .. } => text,
            FormulaKind::SharedDescendant { .. } => &cell.formula_text,
            FormulaKind::Unsupported { kind, text } => {
                debug_assert_ne!(kind, "shared");
                text
            }
        };
        return Ok(OwnedSpoolFormulaRecord::Ordinary {
            sequence: event.sequence,
            coord0,
            text: evaluation(text)?,
        });
    }
    match event.kind {
        FormulaKind::SharedAnchor {
            shared_index,
            declared_range,
            text,
        } => Ok(OwnedSpoolFormulaRecord::SharedAnchor {
            sequence: event.sequence,
            coord0,
            shared_index: shared_index.expect("validated shared index") as usize,
            declared_range: Some(SourceRect {
                start: SourceCoord {
                    row: declared_range.first_row - 1,
                    col: declared_range.first_col - 1,
                },
                end: SourceCoord {
                    row: declared_range.last_row - 1,
                    col: declared_range.last_col - 1,
                },
            }),
            text: evaluation(text)?,
        }),
        FormulaKind::SharedDescendant { shared_index } => {
            Ok(OwnedSpoolFormulaRecord::SharedDescendant {
                sequence: event.sequence,
                coord0,
                shared_index: shared_index.expect("validated shared index") as usize,
            })
        }
        _ => unreachable!("strict source envelope proved shared metadata"),
    }
}

/// Only on a failed value plane, check earlier scanner evidence without
/// rebuilding records, parsing headers or evaluating formula text.
pub(in crate::xlsx) fn validate_reader_prefix(
    plan: &SheetPlan,
    _texts: &EvaluationTexts,
    before: usize,
) -> Result<(), IoError> {
    for cell in plan
        .cells
        .iter()
        .take_while(|cell| cell.span.start < before)
    {
        if let Some(evidence) = &cell.compatibility {
            evidence.validate()?;
        }
    }
    Ok(())
}

use crate::workbook::WBResolver;
use crate::xlsx::recalculate::{XlsxRecalculateOptions, checkpoint};
use compressed_evidence::{EvidenceRecord, MonotonicFormulaEvidence};
use formualizer_eval::engine::{
    DeferredFormulaPackage, Engine, FormulaCompressedPreparation, FormulaCompressedSourceReport,
    FormulaIngestRecord, FormulaPlaneMode, FormulaSpoolDiskPolicy, PartitionedSourceFormulaFamily,
    SourceFamilyId, SourceFormulaFamily, SourceFormulaOrder,
};
use formula_replay::{
    FormulaReplaySpool, FormulaReplayState, FormulaSpoolLimits, HybridFormulaReplaySpool,
    NativeDeferredFormulaReplay as SourceDeferredFormulaReplay,
    replay_spool_per_cell_filtered_with_family,
};

pub(in crate::xlsx) use crate::xlsx::ingest::staging::WorkbookSpoolUsage;
use crate::xlsx::ingest::staging::{FormulaStaging, engine_error};
pub(in crate::xlsx) struct StagedFormulas {
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

/// Prepare the same source evidence, bounded replay and certification proposals as
/// the adapter, consuming only retained source-order formula records. All
/// graph mutation and compressed replay remain in the existing engine ingress.
pub(in crate::xlsx) fn stage(
    engine: &mut Engine<WBResolver>,
    sheet: &str,
    sheet_instance: u32,
    plan: &SheetPlan,
    texts: &EvaluationTexts,
    usage: WorkbookSpoolUsage,
    options: &XlsxRecalculateOptions,
) -> Result<StagedFormulas, IoError> {
    let records = plan
        .cells
        .iter()
        .enumerate()
        .map(|(sequence, cell)| source_record(plan, cell, texts, sequence as u64));
    stage_records(
        engine,
        sheet,
        sheet_instance,
        records,
        usage,
        &options.cancel,
    )
}

/// Stage source records independently of retained publication marks.
/// Both readers select the same eager, bounded replay and deferred modes here.
pub(in crate::xlsx) fn stage_records<C: formualizer_eval::traits::EvaluationContext>(
    engine: &mut Engine<C>,
    sheet: &str,
    sheet_instance: u32,
    records: impl IntoIterator<Item = Result<OwnedSpoolFormulaRecord, IoError>>,
    usage: WorkbookSpoolUsage,
    cancel: &Option<formualizer_eval::engine::CancelToken>,
) -> Result<StagedFormulas, IoError> {
    stage_records_produced::<C, false>(
        engine,
        sheet,
        sheet_instance,
        |emit| {
            for record in records {
                emit(record?)?;
            }
            Ok(())
        },
        usage,
        cancel,
        &Default::default(),
    )
}

pub(in crate::xlsx) fn stage_records_produced<
    C: formualizer_eval::traits::EvaluationContext,
    const FILTER: bool,
>(
    engine: &mut Engine<C>,
    sheet: &str,
    sheet_instance: u32,
    produce: impl FnOnce(
        &mut dyn FnMut(OwnedSpoolFormulaRecord) -> Result<(), IoError>,
    ) -> Result<(), IoError>,
    usage: WorkbookSpoolUsage,
    cancel: &Option<formualizer_eval::engine::CancelToken>,
    refused: &std::collections::HashSet<(u32, u32)>,
) -> Result<StagedFormulas, IoError> {
    checkpoint(cancel)?;
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
    let mut staging = FormulaStaging::new();
    let mut evidence = MonotonicFormulaEvidence::new();
    let mut coordinates = Vec::new();
    let mut count = 0usize;
    let mut shared_tags = 0;
    produce(&mut |record| {
        checkpoint(cancel)?;
        let coord0 = match &record {
            OwnedSpoolFormulaRecord::Ordinary { coord0, .. }
            | OwnedSpoolFormulaRecord::SharedAnchor { coord0, .. }
            | OwnedSpoolFormulaRecord::SharedDescendant { coord0, .. }
            | OwnedSpoolFormulaRecord::Unsupported { coord0, .. } => *coord0,
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
                .map_err(|error| IoError::XlsxRead(engine_error(error)))?;
        } else if let Some(replay) = replay.as_mut() {
            replay
                .emit_record(
                    record,
                    sheet,
                    |_, _| true,
                    |_, coord, text, shared| {
                        checkpoint(cancel).map_err(engine_error)?;
                        staging.stage(
                            engine,
                            sheet,
                            (coord.row, coord.col),
                            if FILTER && refused.contains(&(coord.row, coord.col)) {
                                "#REF!"
                            } else {
                                text
                            },
                            shared,
                            false,
                        )
                    },
                )
                .map_err(IoError::XlsxRead)?;
        }
        count += 1;
        Ok(())
    })?;
    let output = evidence.finish();
    let mut report = output.report;
    let families = output.families;
    let partitioned = output
        .fragmented
        .into_iter()
        .map(|proposal| {
            crate::xlsx::ingest::partition::convert(proposal)
                .map_err(|error| IoError::XlsxRead(engine_error(error)))
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
    checkpoint(cancel)?;
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
                .map_err(|error| IoError::XlsxRead(engine_error(error)))?,
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
                checkpoint(cancel).map_err(engine_error)?;
                staging.stage(
                    engine,
                    sheet,
                    (coord.row, coord.col),
                    if FILTER && refused.contains(&(coord.row, coord.col)) {
                        "#REF!"
                    } else {
                        text
                    },
                    shared,
                    false,
                )
            },
        )
        .map_err(IoError::XlsxRead)?;
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
    Ok(StagedFormulas {
        records: staging.formulas,
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
#[path = "../recalculate/tests/native_formula_sink.rs"]
mod tests;
