use crate::load_limits::enforce_sheet_dimension_limits;
use crate::traits::{
    AccessGranularity, AdapterLoadStats, BackendCaps, CalcSettings, CellData, DefinedName,
    DefinedNameDefinition, DefinedNameScope, MergedRange, SheetData, SpreadsheetReader,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek};
use std::path::Path;
use std::sync::Arc;

use calamine::{
    Data, DataRef, Range, Reader, Xlsx, XlsxFormulaMetadata, open_workbook, open_workbook_from_rs,
};
use formualizer_common::RangeAddress;
use formualizer_eval::arrow_store::{IngestBuilder, OverlayValue, map_error_code};
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{
    DeferredFormulaPackage, Engine as EvalEngine, FormulaCompressedPreparation,
    FormulaCompressedSourceBatch, FormulaCompressedSourceReport, FormulaIngestBatch,
    FormulaIngestRecord, FormulaSpoolDiskPolicy, SourceCoord, SourceFamilyId, SourceFormulaFamily,
    SourceRect,
};
use formualizer_eval::traits::EvaluationContext;
use formualizer_parse::parser::{ASTNode, ReferenceType};
use quick_xml::Reader as XmlReader;
use quick_xml::events::{BytesRef, BytesStart, Event};
use quick_xml::name::QName;
use zip::ZipArchive;

mod compressed_evidence;
mod formula_replay;

use compressed_evidence::{EvidenceRecord, MonotonicFormulaEvidence};
use formula_replay::{
    CalamineDeferredFormulaReplay, FormulaReplaySpool, FormulaSpoolLimits,
    HybridFormulaReplaySpool, SpoolFormulaRecord, replay_spool_per_cell_filtered_with_family,
};

enum CalamineWorkbook {
    File(Xlsx<BufReader<File>>),
    Bytes(Xlsx<Cursor<Vec<u8>>>),
}

impl CalamineWorkbook {
    fn worksheet_range(&mut self, sheet: &str) -> Result<Range<Data>, calamine::Error> {
        match self {
            Self::File(workbook) => workbook.worksheet_range(sheet).map_err(Into::into),
            Self::Bytes(workbook) => workbook.worksheet_range(sheet).map_err(Into::into),
        }
    }

    fn worksheet_formula(&mut self, sheet: &str) -> Result<Range<String>, calamine::Error> {
        match self {
            Self::File(workbook) => workbook.worksheet_formula(sheet).map_err(Into::into),
            Self::Bytes(workbook) => workbook.worksheet_formula(sheet).map_err(Into::into),
        }
    }
}

struct DebugTimer {
    #[cfg(not(target_arch = "wasm32"))]
    started: std::time::Instant,
}

struct DenseState {
    aib: IngestBuilder,
    row_vals: Vec<LiteralValue>,
    current_row0: usize,
    rows_appended: usize,
    row_started: bool,
}

#[derive(Clone, Copy)]
struct WorkbookSpoolUsage {
    bytes: u64,
    files: u32,
}

struct StreamWorksheetOptions {
    chunk_rows: usize,
    debug: bool,
    workbook_spool_usage: WorkbookSpoolUsage,
    shadow_relocation_comparator: Option<ShadowRelocationComparator>,
}

struct FormulaStaging {
    parse_cache: rustc_hash::FxHashMap<String, Option<formualizer_eval::engine::AstNodeId>>,
    formulas: Vec<FormulaIngestRecord>,
    observed: usize,
    handed_to_engine: usize,
}

impl FormulaStaging {
    fn new() -> Self {
        let mut parse_cache = rustc_hash::FxHashMap::default();
        parse_cache.reserve(4096);
        Self {
            parse_cache,
            formulas: Vec::new(),
            observed: 0,
            handed_to_engine: 0,
        }
    }
}

struct StreamedSheet {
    arrow_sheet: formualizer_eval::arrow_store::ArrowSheet,
    dimensions: (usize, usize),
    max_col_seen: usize,
    used_sparse_fallback: bool,
    value_cells_observed: usize,
    values_handed_to_engine: usize,
    formulas_observed: usize,
    formulas_handed_to_engine: usize,
    formulas: Vec<FormulaIngestRecord>,
    formula_source_report: FormulaCompressedSourceReport,
    compressed_families: Vec<SourceFormulaFamily>,
    direct_preparation: Option<FormulaCompressedPreparation>,
    deferred_package: Option<DeferredFormulaPackage>,
    shared_formula_tags: usize,
    formula_spool_bytes: u64,
    formula_spool_spilled: bool,
    stream_millis: u128,
}

#[inline]
fn data_ref_to_literal(value: &DataRef<'_>) -> Option<LiteralValue> {
    match value {
        DataRef::Empty => None,
        DataRef::String(s) if s.is_empty() => None,
        DataRef::SharedString("") => None,
        DataRef::String(s) => Some(LiteralValue::Text(s.clone())),
        DataRef::SharedString(s) => Some(LiteralValue::Text((*s).to_string())),
        DataRef::Float(f) => Some(LiteralValue::Number(*f)),
        DataRef::Int(i) => Some(LiteralValue::Number(*i as f64)),
        DataRef::Bool(b) => Some(LiteralValue::Boolean(*b)),
        DataRef::Error(e) => Some(LiteralValue::Error(ExcelError::new(
            match CalamineAdapter::calamine_error_code(e) {
                1 => ExcelErrorKind::Null,
                2 => ExcelErrorKind::Ref,
                3 => ExcelErrorKind::Name,
                4 => ExcelErrorKind::Value,
                5 => ExcelErrorKind::Div,
                6 => ExcelErrorKind::Na,
                7 => ExcelErrorKind::Num,
                _ => ExcelErrorKind::Error,
            },
        ))),
        DataRef::DateTime(dt) => Some(LiteralValue::from_serial_number(dt.as_f64())),
        DataRef::DateTimeIso(s) => Some(LiteralValue::Text(s.clone())),
        DataRef::DurationIso(s) => Some(LiteralValue::Text(s.clone())),
    }
}

#[inline]
fn data_ref_to_overlay(value: &DataRef<'_>) -> Option<OverlayValue> {
    match value {
        DataRef::Empty => None,
        DataRef::String(s) if s.is_empty() => None,
        DataRef::SharedString("") => None,
        DataRef::String(s) => Some(OverlayValue::Text(Arc::from(s.as_str()))),
        DataRef::SharedString(s) => Some(OverlayValue::Text(Arc::from(*s))),
        DataRef::Float(f) => Some(OverlayValue::Number(*f)),
        DataRef::Int(i) => Some(OverlayValue::Number(*i as f64)),
        DataRef::Bool(b) => Some(OverlayValue::Boolean(*b)),
        DataRef::Error(e) => Some(OverlayValue::Error(CalamineAdapter::calamine_error_code(e))),
        DataRef::DateTime(dt) => Some(OverlayValue::DateTime(dt.as_f64())),
        DataRef::DateTimeIso(s) => Some(OverlayValue::Text(Arc::from(s.as_str()))),
        DataRef::DurationIso(s) => Some(OverlayValue::Text(Arc::from(s.as_str()))),
    }
}

impl DebugTimer {
    fn start() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            started: std::time::Instant::now(),
        }
    }

    fn elapsed_millis(&self) -> u128 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started.elapsed().as_millis()
        }
        #[cfg(target_arch = "wasm32")]
        {
            0
        }
    }
}

type ShadowRelocationComparator = Arc<dyn Fn(&ASTNode, &ASTNode) -> bool + Send + Sync>;

pub struct CalamineAdapter {
    workbook: RwLock<CalamineWorkbook>,
    loaded_sheets: HashSet<String>,
    cached_names: Option<Vec<String>>,
    defined_names: Vec<DefinedName>,
    external_link_targets: BTreeMap<u32, String>,
    calc_settings: Option<CalcSettings>,
    load_stats: AdapterLoadStats,
    shadow_relocation_comparator: Option<ShadowRelocationComparator>,
}

impl CalamineAdapter {
    const EXCEL_MAX_ROWS: u32 = 1_048_576;

    #[doc(hidden)]
    pub fn set_shadow_relocation_comparator_for_test(
        &mut self,
        comparator: impl Fn(&ASTNode, &ASTNode) -> bool + Send + Sync + 'static,
    ) {
        self.shadow_relocation_comparator = Some(Arc::new(comparator));
    }

    fn shadow_relocation_matches(
        comparator: &ShadowRelocationComparator,
        family: &SourceFormulaFamily,
        coord0: SourceCoord,
        expanded_formula: &str,
    ) -> bool {
        let expanded_formula = format!("={}", expanded_formula.trim_start_matches('='));
        let anchor_formula = format!("={}", family.anchor_text.trim_start_matches('='));
        let Ok(expanded) = formualizer_parse::parser::parse(&expanded_formula) else {
            return false;
        };
        let Ok(anchor) = formualizer_parse::parser::parse(&anchor_formula) else {
            return false;
        };
        let Ok(relocated) =
            formualizer_eval::formula_plane::structural::relocate_ast_for_template_placement(
                &anchor,
                i64::from(coord0.row) - i64::from(family.anchor_coord0.row),
                i64::from(coord0.col) - i64::from(family.anchor_coord0.col),
            )
        else {
            return false;
        };
        comparator(&expanded, &relocated)
    }
    const EXCEL_MAX_COLS: u32 = 16_384;

    fn stage_formula<C: EvaluationContext>(
        engine: &mut EvalEngine<C>,
        sheet: &str,
        position: (u32, u32),
        formula: &str,
        debug: bool,
        staging: &mut FormulaStaging,
    ) -> Result<(), calamine::Error> {
        let excel_row = position.0 + 1;
        let excel_col = position.1 + 1;
        let normalized = if formula.starts_with('=') {
            formula.to_string()
        } else {
            format!("={formula}")
        };
        if debug && staging.observed < 16 {
            eprintln!("[fz][load] formula observed at R{excel_row}C{excel_col}");
        }
        if engine.config.defer_graph_building {
            engine.stage_formula_text(sheet, excel_row, excel_col, normalized);
            staging.handed_to_engine += 1;
        } else {
            let ast_id = if let Some(cached) = staging.parse_cache.get(&normalized) {
                *cached
            } else {
                let parsed = match formualizer_parse::parser::parse(&normalized) {
                    Ok(parsed) => Some(parsed),
                    Err(error) => engine
                        .handle_formula_parse_error(
                            sheet,
                            excel_row,
                            excel_col,
                            &normalized,
                            error.to_string(),
                        )
                        .map_err(|error| {
                            calamine::Error::Io(std::io::Error::other(error.to_string()))
                        })?,
                };
                let ast_id = parsed.as_ref().map(|ast| engine.intern_formula_ast(ast));
                staging.parse_cache.insert(normalized.clone(), ast_id);
                ast_id
            };
            if let Some(ast_id) = ast_id {
                staging.formulas.push(FormulaIngestRecord::new(
                    excel_row,
                    excel_col,
                    ast_id,
                    Some(Arc::<str>::from(normalized)),
                ));
                staging.handed_to_engine += 1;
            }
        }
        staging.observed += 1;
        Ok(())
    }

    fn stream_worksheet<RS, C>(
        workbook: &mut Xlsx<RS>,
        sheet: &str,
        engine: &mut EvalEngine<C>,
        sheet_instance: u32,
        options: StreamWorksheetOptions,
    ) -> Result<StreamedSheet, calamine::Error>
    where
        RS: Read + Seek,
        C: EvaluationContext,
    {
        let timer = DebugTimer::start();
        let StreamWorksheetOptions {
            chunk_rows,
            debug,
            workbook_spool_usage,
            shadow_relocation_comparator,
        } = options;
        let mut reader = workbook
            .worksheet_cells_reader(sheet)
            .map_err(calamine::Error::Xlsx)?;
        let declared = reader.dimensions();
        let mut dims_rows = (declared.end.0 as usize + 1).max(1);
        let mut dims_cols = (declared.end.1 as usize + 1).max(1);
        enforce_sheet_dimension_limits(
            "calamine",
            sheet,
            dims_rows as u32,
            dims_cols as u32,
            engine.workbook_load_limits(),
        )
        .map_err(|error| calamine::Error::Io(std::io::Error::other(error.to_string())))?;

        let force_sparse_from_start = (dims_rows as u64).saturating_mul(dims_cols as u64)
            > engine.workbook_load_limits().max_sheet_logical_cells;
        let mut dense = (!force_sparse_from_start).then(|| DenseState {
            aib: IngestBuilder::new(sheet, dims_cols, chunk_rows, engine.config.date_system),
            row_vals: vec![LiteralValue::Empty; dims_cols],
            current_row0: 0,
            rows_appended: 0,
            row_started: false,
        });
        let mut sparse = force_sparse_from_start.then(|| {
            formualizer_eval::arrow_store::ArrowSheet::new_sparse(
                sheet, dims_cols, dims_rows, chunk_rows,
            )
        });
        let mut used_sparse_fallback = force_sparse_from_start;
        let mut max_row_seen = 0usize;
        let mut max_col_seen = 0usize;
        let mut value_cells_observed = 0usize;
        let mut values_handed_to_engine = 0usize;
        let mut formula_staging = FormulaStaging::new();
        let mut formula_count = 0usize;
        let mut formula_evidence = MonotonicFormulaEvidence::new();
        let spool_limits = engine.workbook_load_limits();
        let workbook_bytes_remaining = spool_limits
            .max_formula_spool_bytes_per_workbook
            .saturating_sub(workbook_spool_usage.bytes);
        let spill_files_remaining = spool_limits
            .max_formula_spool_files_per_workbook
            .saturating_sub(workbook_spool_usage.files);
        let mut formula_spool = HybridFormulaReplaySpool::new(FormulaSpoolLimits {
            sheet_bytes: spool_limits.max_formula_spool_bytes_per_sheet,
            workbook_bytes_remaining,
            workbook_bytes_used: workbook_spool_usage.bytes,
            memory_prefix_bytes: spool_limits.formula_spool_memory_prefix_bytes,
            memory_only_bytes: spool_limits.max_formula_spool_memory_bytes,
            allow_disk: spool_limits.formula_spool_disk_policy
                == FormulaSpoolDiskPolicy::NativeSpill,
            spill_files_remaining,
            spill_files_limit: spool_limits.max_formula_spool_files_per_workbook,
        });
        let mut last_formula_coord = None;
        let mut shared_formula_tags = 0usize;

        while let Some(record) = reader
            .next_cell_with_formula_metadata()
            .map_err(calamine::Error::Xlsx)?
        {
            let (row0, col0) = record.pos;
            let row = row0 as usize;
            let col = col0 as usize;
            if row >= dims_rows || col >= dims_cols {
                dims_rows = dims_rows.max(row + 1);
                dims_cols = dims_cols.max(col + 1);
                enforce_sheet_dimension_limits(
                    "calamine",
                    sheet,
                    dims_rows as u32,
                    dims_cols as u32,
                    engine.workbook_load_limits(),
                )
                .map_err(|error| calamine::Error::Io(std::io::Error::other(error.to_string())))?;
            }
            max_row_seen = max_row_seen.max(row);
            max_col_seen = max_col_seen.max(col);

            let has_formula = record.formula.is_some();
            if let Some(metadata) = record.formula {
                if u64::try_from(value_cells_observed)
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::try_from(formula_count).unwrap_or(u64::MAX))
                    .saturating_add(1)
                    > engine.workbook_load_limits().max_sheet_logical_cells
                {
                    return Err(calamine::Error::Io(std::io::Error::other(format!(
                        "Workbook load budget exceeded in calamine for sheet {sheet}: observed populated cell count exceeds configured logical-cell budget of {}",
                        engine.workbook_load_limits().max_sheet_logical_cells
                    ))));
                }
                let coord0 = SourceCoord {
                    row: row0,
                    col: col0,
                };
                let source_sequence = formula_count as u64;
                match metadata {
                    XlsxFormulaMetadata::Normal { formula } => {
                        formula_evidence.observe(coord0, EvidenceRecord::Ordinary);
                        formula_spool.append(SpoolFormulaRecord::Ordinary {
                            sequence: source_sequence,
                            coord0,
                            text: &formula,
                        })
                    }
                    XlsxFormulaMetadata::Shared {
                        shared_index,
                        range,
                        formula,
                    } => {
                        shared_formula_tags += 1;
                        let declared_range = range.map(|range| SourceRect {
                            start: SourceCoord {
                                row: range.start.0,
                                col: range.start.1,
                            },
                            end: SourceCoord {
                                row: range.end.0,
                                col: range.end.1,
                            },
                        });
                        let family = SourceFamilyId {
                            sheet_instance,
                            source_index: shared_index,
                        };
                        formula_evidence.observe(
                            coord0,
                            EvidenceRecord::Anchor {
                                family,
                                range: declared_range,
                                text: &formula,
                            },
                        );
                        formula_spool.append(SpoolFormulaRecord::SharedAnchor {
                            sequence: source_sequence,
                            coord0,
                            shared_index,
                            declared_range,
                            text: &formula,
                        })
                    }
                    XlsxFormulaMetadata::SharedDerived { shared_index } => {
                        shared_formula_tags += 1;
                        formula_evidence.observe(
                            coord0,
                            EvidenceRecord::Descendant {
                                family: SourceFamilyId {
                                    sheet_instance,
                                    source_index: shared_index,
                                },
                            },
                        );
                        formula_spool.append(SpoolFormulaRecord::SharedDescendant {
                            sequence: source_sequence,
                            coord0,
                            shared_index,
                        })
                    }
                    _ => {
                        formula_evidence.observe(coord0, EvidenceRecord::Unsupported);
                        formula_spool.append(SpoolFormulaRecord::Unsupported {
                            sequence: source_sequence,
                            coord0,
                        })
                    }
                }
                .map_err(|error| calamine::Error::Io(std::io::Error::other(error.to_string())))?;
                last_formula_coord = Some((row, col));
                if let Some(state) = dense.as_mut()
                    && state.row_started
                    && state.current_row0 == row
                    && col < state.row_vals.len()
                {
                    state.row_vals[col] = LiteralValue::Empty;
                }
                formula_count += 1;
            }

            // Preserve existing KeepCachedValue behavior: a formula's cached
            // value is not handed to the value plane.
            if has_formula {
                continue;
            }
            let Some(literal) = data_ref_to_literal(&record.value) else {
                continue;
            };
            value_cells_observed += 1;
            if u64::try_from(value_cells_observed)
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(formula_count).unwrap_or(u64::MAX))
                > engine.workbook_load_limits().max_sheet_logical_cells
            {
                return Err(calamine::Error::Io(std::io::Error::other(format!(
                    "Workbook load budget exceeded in calamine for sheet {sheet}: observed populated cell count exceeds configured logical-cell budget of {}",
                    engine.workbook_load_limits().max_sheet_logical_cells
                ))));
            }
            if last_formula_coord == Some((row, col)) {
                continue;
            }

            if let Some(arrow_sheet) = sparse.as_mut() {
                if let Some(value) = data_ref_to_overlay(&record.value) {
                    arrow_sheet.set_sparse_overlay_value(row, col, value);
                    values_handed_to_engine += 1;
                }
                continue;
            }

            let state = dense.as_mut().expect("dense or sparse ingest mode");
            let non_monotonic = state.row_started && row < state.current_row0;
            let col_overflow = col >= state.row_vals.len();
            let gap_rows = if state.row_started {
                row.saturating_sub(state.current_row0)
            } else {
                row
            };
            let large_gap = gap_rows > 128;
            let would_exceed_dense_budget =
                u64::try_from(state.rows_appended.saturating_mul(state.row_vals.len()))
                    .unwrap_or(u64::MAX)
                    > engine.workbook_load_limits().max_sheet_logical_cells;
            if non_monotonic || col_overflow || large_gap || would_exceed_dense_budget {
                let mut state = dense.take().expect("dense state present");
                if state.row_started && state.current_row0 == state.rows_appended {
                    state.aib.append_row(&state.row_vals).map_err(|error| {
                        calamine::Error::Io(std::io::Error::other(error.to_string()))
                    })?;
                    state.rows_appended += 1;
                }
                let mut arrow_sheet = state.aib.finish();
                arrow_sheet.ensure_row_capacity(dims_rows.max(row + 1));
                if col >= arrow_sheet.columns.len() {
                    arrow_sheet.insert_columns(
                        arrow_sheet.columns.len(),
                        col + 1 - arrow_sheet.columns.len(),
                    );
                }
                if let Some(value) = data_ref_to_overlay(&record.value) {
                    arrow_sheet.set_sparse_overlay_value(row, col, value);
                    values_handed_to_engine += 1;
                }
                sparse = Some(arrow_sheet);
                used_sparse_fallback = true;
                continue;
            }

            if !state.row_started {
                while state.rows_appended < row {
                    state
                        .aib
                        .append_row(&vec![LiteralValue::Empty; state.row_vals.len()])
                        .map_err(|error| {
                            calamine::Error::Io(std::io::Error::other(error.to_string()))
                        })?;
                    state.rows_appended += 1;
                }
                state.current_row0 = row;
                state.row_started = true;
            } else if row > state.current_row0 {
                state.aib.append_row(&state.row_vals).map_err(|error| {
                    calamine::Error::Io(std::io::Error::other(error.to_string()))
                })?;
                state.rows_appended += 1;
                state.row_vals.fill(LiteralValue::Empty);
                while state.rows_appended < row {
                    state
                        .aib
                        .append_row(&vec![LiteralValue::Empty; state.row_vals.len()])
                        .map_err(|error| {
                            calamine::Error::Io(std::io::Error::other(error.to_string()))
                        })?;
                    state.rows_appended += 1;
                }
                state.current_row0 = row;
            }
            state.row_vals[col] = literal;
            values_handed_to_engine += 1;
        }

        // Replay and validate the complete sheet-local source stream before any
        // formula staging, parsing, or graph mutation. The Arrow result is also
        // still local and is installed by the caller only after this succeeds.
        let compressed_evidence = formula_evidence.finish();
        let mut formula_source_report = compressed_evidence.report;
        let mut compressed_families = compressed_evidence.families;
        // Fragment proposals remain replay-only until the backend-neutral Shadow
        // transport accepts them. Retaining the bounded proposal here proves the
        // evidence path without changing production authority.
        let _fragmented_families = compressed_evidence.fragmented;
        let formula_spool_bytes = if formula_count == 0 {
            0
        } else {
            formula_spool.encoded_bytes()
        };
        let formula_spool_spilled = formula_spool.spilled();
        formula_source_report.source_formula_records_spooled = formula_count as u64;
        formula_source_report.source_spool_encoded_bytes = formula_spool_bytes;
        formula_source_report.source_spool_peak_memory_bytes = formula_spool.peak_memory_bytes();
        formula_source_report.source_spool_spilled_bytes = if formula_spool_spilled {
            formula_spool_bytes
        } else {
            0
        };
        let _formula_spool_storage = formula_spool.storage_kind();
        debug_assert!(formula_count == 0 || formula_spool_bytes >= 5);
        let direct_preparation = if engine.config.formula_plane_mode
            == formualizer_eval::engine::FormulaPlaneMode::AuthoritativeExperimental
            && !engine.config.defer_graph_building
        {
            Some(
                engine
                    .source_formula_ingress()
                    .prepare_families(sheet, &compressed_families)
                    .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?,
            )
        } else {
            None
        };
        let all_formulas_direct = direct_preparation
            .as_ref()
            .is_some_and(|preparation| preparation.direct_cell_count() == formula_count as u64);
        if !all_formulas_direct && !engine.config.defer_graph_building {
            formula_source_report.source_spool_replays = 1;
            let compare_shadow = engine.config.formula_plane_mode
                == formualizer_eval::engine::FormulaPlaneMode::Shadow;
            let mut relocation_mismatches = BTreeSet::new();
            replay_spool_per_cell_filtered_with_family(
                &mut formula_spool,
                sheet,
                |shared_index| {
                    direct_preparation.as_ref().is_some_and(|preparation| {
                        preparation.is_direct(SourceFamilyId {
                            sheet_instance,
                            source_index: shared_index,
                        })
                    })
                },
                |coord0, formula, shared_index| {
                    if compare_shadow
                        && let (Some(comparator), Some(shared_index)) =
                            (shadow_relocation_comparator.as_ref(), shared_index)
                        && let Some(family) = compressed_families
                            .iter()
                            .find(|family| family.source_id.source_index == shared_index)
                        && !Self::shadow_relocation_matches(comparator, family, coord0, formula)
                    {
                        relocation_mismatches.insert(shared_index);
                    }
                    Self::stage_formula(
                        engine,
                        sheet,
                        (coord0.row, coord0.col),
                        formula,
                        debug,
                        &mut formula_staging,
                    )
                },
            )?;
            if !relocation_mismatches.is_empty() {
                compressed_families.retain(|family| {
                    !relocation_mismatches.contains(&family.source_id.source_index)
                });
            }
        }

        let mut formula_spool = Some(formula_spool);
        let direct_preparation = direct_preparation.map(|preparation| {
            let replay: Box<dyn formualizer_eval::engine::DeferredFormulaReplay> =
                Box::new(CalamineDeferredFormulaReplay::new(
                    formula_spool.take().expect("eager formula spool available"),
                    sheet.to_string(),
                    sheet_instance,
                ));
            preparation.with_exact_replay(
                std::sync::Arc::new(std::sync::Mutex::new(replay)),
                Default::default(),
            )
        });

        if u64::try_from(value_cells_observed)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(formula_count).unwrap_or(u64::MAX))
            > engine.workbook_load_limits().max_sheet_logical_cells
        {
            return Err(calamine::Error::Io(std::io::Error::other(format!(
                "Workbook load budget exceeded in calamine for sheet {sheet}: observed populated cell count exceeds configured logical-cell budget of {}",
                engine.workbook_load_limits().max_sheet_logical_cells
            ))));
        }
        enforce_sheet_dimension_limits(
            "calamine",
            sheet,
            dims_rows as u32,
            dims_cols as u32,
            engine.workbook_load_limits(),
        )
        .map_err(|error| calamine::Error::Io(std::io::Error::other(error.to_string())))?;

        let mut arrow_sheet = if let Some(mut arrow_sheet) = sparse {
            arrow_sheet.ensure_row_capacity(dims_rows.max(max_row_seen + 1));
            arrow_sheet
        } else {
            let mut state = dense.take().expect("dense state present");
            if state.row_started {
                state.aib.append_row(&state.row_vals).map_err(|error| {
                    calamine::Error::Io(std::io::Error::other(error.to_string()))
                })?;
            }
            let mut arrow_sheet = state.aib.finish();
            arrow_sheet.ensure_row_capacity(dims_rows.max(max_row_seen + 1));
            arrow_sheet
        };
        if dims_cols > arrow_sheet.columns.len() {
            arrow_sheet.insert_columns(
                arrow_sheet.columns.len(),
                dims_cols - arrow_sheet.columns.len(),
            );
        }
        let deferred_package = engine.config.defer_graph_building.then(|| {
            DeferredFormulaPackage::new(
                sheet.to_string(),
                formula_source_report.clone(),
                compressed_families.clone(),
                Box::new(CalamineDeferredFormulaReplay::new(
                    formula_spool
                        .take()
                        .expect("deferred formula spool available"),
                    sheet.to_string(),
                    sheet_instance,
                )),
            )
        });
        Ok(StreamedSheet {
            arrow_sheet,
            dimensions: (dims_rows, dims_cols),
            max_col_seen,
            used_sparse_fallback,
            value_cells_observed,
            values_handed_to_engine,
            formulas_observed: formula_count,
            formulas_handed_to_engine: formula_count,
            formulas: formula_staging.formulas,
            formula_source_report,
            compressed_families,
            direct_preparation,
            deferred_package,
            shared_formula_tags,
            formula_spool_bytes,
            formula_spool_spilled,
            stream_millis: timer.elapsed_millis(),
        })
    }

    pub fn external_link_target(&self, index: u32) -> Option<&str> {
        self.external_link_targets.get(&index).map(|s| s.as_str())
    }

    fn normalize_open_ended_bounds(
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    ) -> Option<(u32, u32, u32, u32)> {
        let mut sr = start_row;
        let mut sc = start_col;
        let mut er = end_row;
        let mut ec = end_col;

        if sr.is_none() && er.is_none() {
            sr = Some(1);
            er = Some(Self::EXCEL_MAX_ROWS);
        }
        if sc.is_none() && ec.is_none() {
            sc = Some(1);
            ec = Some(Self::EXCEL_MAX_COLS);
        }

        if sr.is_some() && er.is_none() {
            er = Some(Self::EXCEL_MAX_ROWS);
        }
        if er.is_some() && sr.is_none() {
            sr = Some(1);
        }

        if sc.is_some() && ec.is_none() {
            ec = Some(Self::EXCEL_MAX_COLS);
        }
        if ec.is_some() && sc.is_none() {
            sc = Some(1);
        }

        let sr = sr?;
        let sc = sc?;
        let er = er?;
        let ec = ec?;

        if er < sr || ec < sc {
            return None;
        }

        Some((sr, sc, er, ec))
    }

    fn convert_defined_name(
        name: &str,
        raw_formula: &str,
        local_sheet_id: Option<usize>,
        sheet_names: &[String],
    ) -> Option<DefinedName> {
        let mut trimmed = raw_formula.trim();
        if let Some(rest) = trimmed.strip_prefix('=') {
            trimmed = rest.trim();
        }
        if trimmed.is_empty() || trimmed.contains(',') {
            return None;
        }

        let reference = ReferenceType::from_string(trimmed).ok()?;
        let scope_sheet = local_sheet_id.and_then(|idx| sheet_names.get(idx).cloned());
        let scope = if scope_sheet.is_some() {
            DefinedNameScope::Sheet
        } else {
            DefinedNameScope::Workbook
        };
        let base_sheet = scope_sheet.as_deref();

        let (sheet_name, start_row, start_col, end_row, end_col) = match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet = sheet.or_else(|| base_sheet.map(|s| s.to_string()))?;
                (sheet, row, col, row, col)
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let (sr, sc, er, ec) =
                    Self::normalize_open_ended_bounds(start_row, start_col, end_row, end_col)?;
                let sheet = sheet.or_else(|| base_sheet.map(|s| s.to_string()))?;
                (sheet, sr, sc, er, ec)
            }
            _ => return None,
        };

        let address = RangeAddress::new(sheet_name, start_row, start_col, end_row, end_col).ok()?;

        Some(DefinedName {
            name: name.to_string(),
            scope,
            scope_sheet,
            definition: DefinedNameDefinition::Range { address },
        })
    }

    fn decode_attr<R: BufRead>(
        reader: &XmlReader<R>,
        start: &BytesStart<'_>,
        key: &[u8],
    ) -> Option<String> {
        start
            .attributes()
            .filter_map(Result::ok)
            .find(|attr| attr.key == QName(key))
            .and_then(|attr| {
                attr.decode_and_unescape_value(reader.decoder())
                    .ok()
                    .map(|v| v.into_owned())
            })
    }

    fn append_xml_entity(
        entity: &BytesRef<'_>,
        buffer: &mut String,
    ) -> Result<(), quick_xml::Error> {
        let decoded = entity.decode()?;
        match decoded.as_ref() {
            "lt" => buffer.push('<'),
            "gt" => buffer.push('>'),
            "amp" => buffer.push('&'),
            "apos" => buffer.push('\''),
            "quot" => buffer.push('"'),
            _ => {
                if let Some(ch) = entity.resolve_char_ref()? {
                    buffer.push(ch);
                } else {
                    return Err(quick_xml::Error::Escape(
                        quick_xml::escape::EscapeError::UnrecognizedEntity(
                            0..0,
                            format!("&{decoded};"),
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn fallback_defined_names_from_workbook<R>(
        workbook: &Xlsx<R>,
        sheet_names: &[String],
    ) -> Vec<DefinedName>
    where
        R: Read + Seek,
    {
        let mut out = Vec::new();
        let mut seen: HashSet<(DefinedNameScope, Option<String>, String)> = HashSet::new();

        for (name, formula) in workbook.defined_names() {
            if let Some(converted) = Self::convert_defined_name(name, formula, None, sheet_names) {
                let key = (
                    converted.scope.clone(),
                    converted.scope_sheet.clone(),
                    converted.name.clone(),
                );
                if seen.insert(key) {
                    out.push(converted);
                }
            }
        }

        out
    }

    fn scan_defined_names_from_reader<R>(reader: R, sheet_names: &[String]) -> Vec<DefinedName>
    where
        R: Read + Seek,
    {
        let mut archive = match ZipArchive::new(reader) {
            Ok(a) => a,
            Err(_) => return Vec::new(),
        };
        let entry = match archive.by_name("xl/workbook.xml") {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };

        // Calamine's public defined_names() surface flattens OOXML defined names to
        // (name, formula_text) and drops localSheetId. We recover only the scoped
        // defined-name metadata we need here with a targeted streaming pass over
        // workbook.xml, avoiding a full file String allocation or any sheet XML reparse.
        let mut xml = XmlReader::from_reader(BufReader::new(entry));
        xml.config_mut().trim_text(true);

        let mut out = Vec::new();
        let mut seen: HashSet<(DefinedNameScope, Option<String>, String)> = HashSet::new();
        let mut buf = Vec::new();
        let mut inner_buf = Vec::new();
        let mut in_defined_names = false;

        loop {
            buf.clear();
            match xml.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) if e.local_name().as_ref() == b"definedNames" => {
                    in_defined_names = true;
                }
                Ok(Event::End(ref e)) if e.local_name().as_ref() == b"definedNames" => {
                    break;
                }
                Ok(Event::Start(ref e))
                    if in_defined_names && e.local_name().as_ref() == b"definedName" =>
                {
                    let name = Self::decode_attr(&xml, e, b"name");
                    let local_sheet_id = Self::decode_attr(&xml, e, b"localSheetId")
                        .and_then(|v| v.parse::<usize>().ok());
                    let mut value = String::new();

                    loop {
                        inner_buf.clear();
                        match xml.read_event_into(&mut inner_buf) {
                            Ok(Event::Text(t)) => match t.xml10_content() {
                                Ok(text) => value.push_str(&text),
                                Err(_) => return Vec::new(),
                            },
                            Ok(Event::GeneralRef(entity)) => {
                                if Self::append_xml_entity(&entity, &mut value).is_err() {
                                    return Vec::new();
                                }
                            }
                            Ok(Event::End(end)) if end.name() == e.name() => break,
                            Ok(Event::Eof) => return Vec::new(),
                            Err(_) => return Vec::new(),
                            _ => {}
                        }
                    }

                    if let Some(name) = name
                        && let Some(converted) =
                            Self::convert_defined_name(&name, &value, local_sheet_id, sheet_names)
                    {
                        let key = (
                            converted.scope.clone(),
                            converted.scope_sheet.clone(),
                            converted.name.clone(),
                        );
                        if seen.insert(key) {
                            out.push(converted);
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Err(_) => return Vec::new(),
                _ => {}
            }
        }

        out
    }

    fn scan_external_link_targets_from_reader<R>(reader: R) -> BTreeMap<u32, String>
    where
        R: Read + Seek,
    {
        let mut archive = match ZipArchive::new(reader) {
            Ok(a) => a,
            Err(_) => return BTreeMap::new(),
        };

        fn extract_target(xml: &str) -> Option<String> {
            let key = "Target=\"";
            let start = xml.find(key)? + key.len();
            let end = xml[start..].find('"')? + start;
            Some(xml[start..end].to_string())
        }

        let mut out = BTreeMap::new();
        for i in 0..archive.len() {
            let mut entry = match archive.by_index(i) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.name().to_string();
            let Some(rest) = name.strip_prefix("xl/externalLinks/_rels/externalLink") else {
                continue;
            };
            let Some(num_str) = rest.strip_suffix(".xml.rels") else {
                continue;
            };
            let Ok(idx) = num_str.parse::<u32>() else {
                continue;
            };

            let mut xml = String::new();
            if entry.read_to_string(&mut xml).is_ok()
                && let Some(target) = extract_target(&xml)
            {
                out.insert(idx, target);
            }
        }
        out
    }

    /// Parse the workbook-level `<calcPr>` settings (spec §9) straight from the
    /// `.xlsx` zip — calamine does not surface these. Reuses the shared
    /// `calc_pr` parser; returns `None` when `xl/workbook.xml` is missing or has
    /// no `<calcPr>` element.
    fn scan_calc_settings_from_reader<R>(reader: R) -> Option<CalcSettings>
    where
        R: Read + Seek,
    {
        let mut archive = ZipArchive::new(reader).ok()?;
        let mut entry = archive.by_name("xl/workbook.xml").ok()?;
        let mut xml = Vec::new();
        entry.read_to_end(&mut xml).ok()?;
        crate::calc_pr::parse_calc_pr(&xml)
    }

    fn calamine_error_code(e: &calamine::CellErrorType) -> u8 {
        let kind = match e {
            calamine::CellErrorType::Div0 => ExcelErrorKind::Div,
            calamine::CellErrorType::NA => ExcelErrorKind::Na,
            calamine::CellErrorType::Name => ExcelErrorKind::Name,
            calamine::CellErrorType::Null => ExcelErrorKind::Null,
            calamine::CellErrorType::Num => ExcelErrorKind::Num,
            calamine::CellErrorType::Ref => ExcelErrorKind::Ref,
            calamine::CellErrorType::Value => ExcelErrorKind::Value,
            _ => ExcelErrorKind::Error,
        };
        map_error_code(kind)
    }

    fn range_to_cells(
        range: &Range<Data>,
        formulas: Option<&Range<String>>,
    ) -> BTreeMap<(u32, u32), CellData> {
        let mut cells = BTreeMap::new();

        // We use the cells() iterator which gives us actual positions

        // Process values using actual positions

        let start_row = range.start().unwrap_or_default().0 as usize;
        let start_col = range.start().unwrap_or_default().1 as usize;

        for (row, col, val) in range.used_cells() {
            // Calamine uses 0-based indexing, convert to 1-based for Excel
            let excel_row = (row + start_row + 1) as u32;
            let excel_col = (col + start_col + 1) as u32;

            // Convert value (skip empty cells and empty strings)
            let value = match val {
                Data::Empty => None,
                Data::String(s) if s.is_empty() => None, // Treat empty strings as no value
                Data::String(s) => Some(LiteralValue::Text(s.clone())),
                Data::Float(f) => Some(LiteralValue::Number(*f)),
                Data::Int(i) => Some(LiteralValue::Int(*i)),
                Data::Bool(b) => Some(LiteralValue::Boolean(*b)),
                Data::Error(e) => {
                    let kind = match e {
                        calamine::CellErrorType::Div0 => ExcelErrorKind::Div,
                        calamine::CellErrorType::NA => ExcelErrorKind::Na,
                        calamine::CellErrorType::Name => ExcelErrorKind::Name,
                        calamine::CellErrorType::Null => ExcelErrorKind::Null,
                        calamine::CellErrorType::Num => ExcelErrorKind::Num,
                        calamine::CellErrorType::Ref => ExcelErrorKind::Ref,
                        calamine::CellErrorType::Value => ExcelErrorKind::Value,
                        _ => ExcelErrorKind::Value,
                    };
                    Some(LiteralValue::Error(ExcelError::new(kind)))
                }
                Data::DateTime(dt) => Some(LiteralValue::from_serial_number(dt.as_f64())),
                Data::DateTimeIso(s) => Some(LiteralValue::Text(s.clone())),
                Data::DurationIso(s) => Some(LiteralValue::Text(s.clone())),
            };

            if value.is_some() {
                cells.insert(
                    (excel_row, excel_col),
                    CellData {
                        value,
                        formula: None,
                        style: None,
                    },
                );
            }
        }

        // Process formulas using their actual positions
        if let Some(frm_range) = formulas {
            let start_row = frm_range.start().unwrap_or_default().0 as usize;
            let start_col = frm_range.start().unwrap_or_default().1 as usize;

            for (row, col, formula) in frm_range.used_cells() {
                if !formula.is_empty() {
                    // Convert to 1-based Excel coordinates
                    let excel_row = (row + start_row + 1) as u32;
                    let excel_col = (col + start_col + 1) as u32;

                    // Ensure formula starts with '=' for proper parsing
                    let formula_with_eq = if formula.starts_with('=') {
                        formula.clone()
                    } else {
                        format!("={formula}")
                    };

                    // Update existing cell or create new one with formula
                    cells
                        .entry((excel_row, excel_col))
                        .and_modify(|cell| cell.formula = Some(formula_with_eq.clone()))
                        .or_insert_with(|| CellData {
                            value: None,
                            formula: Some(formula_with_eq),
                            style: None,
                        });
                }
            }
        }

        cells
    }
}

impl SpreadsheetReader for CalamineAdapter {
    type Error = calamine::Error;

    fn access_granularity(&self) -> AccessGranularity {
        AccessGranularity::Sheet
    }

    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            read: true,
            formulas: true,
            named_ranges: true,
            lazy_loading: false,
            random_access: false,
            styles: false,
            bytes_input: true,
            // conservative defaults
            date_system_1904: false,
            merged_cells: false,
            rich_text: false,
            hyperlinks: false,
            data_validations: false,
            shared_formulas: false,
            ..Default::default()
        }
    }

    fn sheet_names(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.cached_names.clone().unwrap_or_default())
    }

    fn load_stats(&self) -> Option<AdapterLoadStats> {
        Some(self.load_stats.clone())
    }

    fn defined_names(&mut self) -> Result<Vec<DefinedName>, Self::Error> {
        Ok(self.defined_names.clone())
    }

    fn calc_settings(&self) -> Option<CalcSettings> {
        self.calc_settings.clone()
    }

    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let path = path.as_ref();
        let external_link_targets = match File::open(path) {
            Ok(file) => Self::scan_external_link_targets_from_reader(BufReader::new(file)),
            Err(_) => BTreeMap::new(),
        };
        let calc_settings = File::open(path)
            .ok()
            .and_then(|file| Self::scan_calc_settings_from_reader(BufReader::new(file)));
        let workbook: Xlsx<BufReader<File>> = open_workbook(path)?;
        let sheet_names = workbook.sheet_names().to_vec();
        let defined_names = if workbook.defined_names().is_empty() {
            Vec::new()
        } else {
            let parsed = match File::open(path) {
                Ok(file) => {
                    Self::scan_defined_names_from_reader(BufReader::new(file), &sheet_names)
                }
                Err(_) => Vec::new(),
            };
            if parsed.is_empty() {
                Self::fallback_defined_names_from_workbook(&workbook, &sheet_names)
            } else {
                parsed
            }
        };
        Ok(Self {
            workbook: RwLock::new(CalamineWorkbook::File(workbook)),
            loaded_sheets: HashSet::new(),
            cached_names: Some(sheet_names),
            defined_names,
            external_link_targets,
            calc_settings,
            load_stats: AdapterLoadStats::default(),
            shadow_relocation_comparator: None,
        })
    }

    fn open_reader(mut reader: Box<dyn Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let mut data = Vec::new();
        reader.read_to_end(&mut data).map_err(calamine::Error::Io)?;
        Self::open_bytes(data)
    }

    fn open_bytes(data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let external_link_targets =
            Self::scan_external_link_targets_from_reader(Cursor::new(data.as_slice()));
        let calc_settings = Self::scan_calc_settings_from_reader(Cursor::new(data.as_slice()));
        let workbook: Xlsx<Cursor<Vec<u8>>> = open_workbook_from_rs(Cursor::new(data.clone()))?;
        let sheet_names = workbook.sheet_names().to_vec();
        let defined_names = if workbook.defined_names().is_empty() {
            Vec::new()
        } else {
            let parsed =
                Self::scan_defined_names_from_reader(Cursor::new(data.as_slice()), &sheet_names);
            if parsed.is_empty() {
                Self::fallback_defined_names_from_workbook(&workbook, &sheet_names)
            } else {
                parsed
            }
        };

        Ok(Self {
            workbook: RwLock::new(CalamineWorkbook::Bytes(workbook)),
            loaded_sheets: HashSet::new(),
            cached_names: Some(sheet_names),
            defined_names,
            external_link_targets,
            calc_settings,
            load_stats: AdapterLoadStats::default(),
            shadow_relocation_comparator: None,
        })
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error> {
        // Calamine loads entire sheet; filter after read_sheet
        let data = self.read_sheet(sheet)?;
        Ok(data
            .cells
            .into_iter()
            .filter(|((r, c), _)| *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1)
            .collect())
    }

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error> {
        // Values
        let mut wb = self.workbook.write();
        let range = wb.worksheet_range(sheet)?;
        // Formulas (same dims as range, may be empty strings)
        let formulas = wb.worksheet_formula(sheet).ok();

        let dims = (range.height() as u32, range.width() as u32);
        let cells = Self::range_to_cells(&range, formulas.as_ref());

        self.loaded_sheets.insert(sheet.to_string());

        Ok(SheetData {
            cells,
            dimensions: Some(dims),
            tables: vec![],
            named_ranges: vec![],
            date_system_1904: false, // calamine XLSX currently doesn’t expose this
            merged_cells: Vec::<MergedRange>::new(),
            hidden: false,
            // Explicit fallback: calamine does not expose row visibility metadata.
            row_hidden_manual: vec![],
            // Explicit fallback: filter-hidden row state is unavailable via calamine.
            row_hidden_filter: vec![],
        })
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        let mut wb = self.workbook.write();
        wb.worksheet_range(sheet)
            .ok()
            .map(|r| (r.height() as u32, r.width() as u32))
    }

    fn is_loaded(&self, sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        self.loaded_sheets.contains(sheet)
    }
}

impl<R> EngineLoadStream<R> for CalamineAdapter
where
    R: EvaluationContext,
{
    type Error = calamine::Error;

    fn stream_into_engine(&mut self, engine: &mut EvalEngine<R>) -> Result<(), Self::Error> {
        use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
        use formualizer_eval::reference::{CellRef, Coord};

        #[cfg(feature = "tracing")]
        let _span_load = tracing::info_span!(
            "io_stream_into_engine",
            backend = "calamine",
            formula_records = true,
        )
        .entered();

        // Calamine 0.36 streams cached values and formula metadata from each XLSX
        // cell record in one pass. FormulaPlane staging and authoritative family
        // grouping remain unchanged downstream.
        let debug = std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .is_some_and(|v| v != "0");
        let t0 = DebugTimer::start();
        let names = self.sheet_names()?;
        if debug {
            eprintln!("[fz][load] calamine: {} sheets", names.len());
        }
        for n in &names {
            #[cfg(feature = "tracing")]
            let _span_sheet = tracing::info_span!("io_load_sheet", sheet = n.as_str()).entered();
            engine
                .add_sheet(n.as_str())
                .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
        }

        let prev_index_mode = engine.config.sheet_index_mode;
        engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::Lazy);
        let prev_range_limit = engine.config.range_expansion_limit;
        engine.config.range_expansion_limit = 0;
        let prev_first_load = engine.first_load_assume_new();
        engine.set_first_load_assume_new(true);
        engine.reset_ensure_touched();

        let load_result = (|| -> Result<(), calamine::Error> {
            let chunk_rows: usize = 32 * 1024;
            let mut total_values = 0usize;
            let mut total_value_cells_observed = 0usize;
            let mut total_formulas = 0usize;
            let mut total_formula_handed_to_engine = 0usize;
            let mut total_shared_formula_tags = 0usize;
            let mut workbook_spool_bytes_used = 0u64;
            let mut workbook_spill_files_used = 0u32;
            let mut eager_formula_batches: Vec<(FormulaIngestBatch, FormulaCompressedSourceBatch)> =
                Vec::new();
            let mut eager_direct_batches: Vec<(
                FormulaIngestBatch,
                FormulaCompressedSourceReport,
                FormulaCompressedPreparation,
            )> = Vec::new();

            for (sheet_instance, n) in names.iter().enumerate() {
                let t_sheet = DebugTimer::start();
                if debug {
                    eprintln!("[fz][load] >> sheet '{n}'");
                }
                #[cfg(feature = "tracing")]
                let _span_sheet =
                    tracing::info_span!("io_populate_sheet", sheet = n.as_str()).entered();

                let shadow_relocation_comparator =
                    self.shadow_relocation_comparator.as_ref().map(Arc::clone);
                let streamed = {
                    let mut workbook = self.workbook.write();
                    match &mut *workbook {
                        CalamineWorkbook::File(workbook) => Self::stream_worksheet(
                            workbook,
                            n,
                            engine,
                            sheet_instance as u32,
                            StreamWorksheetOptions {
                                chunk_rows,
                                debug,
                                workbook_spool_usage: WorkbookSpoolUsage {
                                    bytes: workbook_spool_bytes_used,
                                    files: workbook_spill_files_used,
                                },
                                shadow_relocation_comparator: shadow_relocation_comparator.clone(),
                            },
                        ),
                        CalamineWorkbook::Bytes(workbook) => Self::stream_worksheet(
                            workbook,
                            n,
                            engine,
                            sheet_instance as u32,
                            StreamWorksheetOptions {
                                chunk_rows,
                                debug,
                                workbook_spool_usage: WorkbookSpoolUsage {
                                    bytes: workbook_spool_bytes_used,
                                    files: workbook_spill_files_used,
                                },
                                shadow_relocation_comparator,
                            },
                        ),
                    }?
                };
                let StreamedSheet {
                    arrow_sheet: asheet,
                    dimensions: (dims_rows, dims_cols),
                    max_col_seen,
                    used_sparse_fallback,
                    value_cells_observed: sheet_value_cells_observed,
                    values_handed_to_engine,
                    formulas_observed: parsed_n,
                    formulas_handed_to_engine: formula_handed_to_engine,
                    formulas,
                    formula_source_report,
                    compressed_families,
                    direct_preparation,
                    deferred_package,
                    shared_formula_tags,
                    formula_spool_bytes,
                    formula_spool_spilled,
                    stream_millis,
                } = streamed;
                workbook_spool_bytes_used = workbook_spool_bytes_used
                    .checked_add(formula_spool_bytes)
                    .expect("spool workbook accounting was preflighted");
                if formula_spool_spilled {
                    workbook_spill_files_used = workbook_spill_files_used
                        .checked_add(1)
                        .expect("spool file accounting was preflighted");
                }
                total_values += values_handed_to_engine;
                total_value_cells_observed += sheet_value_cells_observed;
                total_shared_formula_tags += shared_formula_tags;

                let store = engine.sheet_store_mut();
                if let Some(pos) = store.sheets.iter().position(|s| s.name.as_ref() == n) {
                    store.sheets[pos] = asheet;
                } else {
                    store.sheets.push(asheet);
                }

                if engine.config.defer_graph_building {
                    if let Some(package) = deferred_package {
                        engine.source_formula_ingress().stage_deferred(package);
                    }
                } else if !formulas.is_empty() || formula_source_report.source_formula_events != 0 {
                    let batch = FormulaIngestBatch::new(n.clone(), formulas);
                    if let Some(preparation) = direct_preparation {
                        eager_direct_batches.push((batch, formula_source_report, preparation));
                    } else {
                        eager_formula_batches.push((
                            batch,
                            FormulaCompressedSourceBatch::with_families(
                                n.clone(),
                                formula_source_report,
                                compressed_families,
                            ),
                        ));
                    }
                }

                total_formulas += parsed_n;
                total_formula_handed_to_engine += formula_handed_to_engine;
                if debug {
                    eprintln!(
                        "[fz][load]    streamed rows={} cols={} max_record_col={} sparse_fallback={} values={} formulas={} in {} ms",
                        dims_rows,
                        dims_cols,
                        max_col_seen + 1,
                        used_sparse_fallback,
                        sheet_value_cells_observed,
                        parsed_n,
                        stream_millis,
                    );
                    eprintln!(
                        "[fz][load] << sheet '{}' staged in {} ms",
                        n,
                        t_sheet.elapsed_millis()
                    );
                }
                self.loaded_sheets.insert(n.to_string());

                let row_hidden_manual: &[u32] = &[];
                let row_hidden_filter: &[u32] = &[];
                for row in row_hidden_manual {
                    engine
                        .set_row_hidden(
                            n,
                            *row,
                            true,
                            formualizer_eval::engine::RowVisibilitySource::Manual,
                        )
                        .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
                }
                for row in row_hidden_filter {
                    engine
                        .set_row_hidden(
                            n,
                            *row,
                            true,
                            formualizer_eval::engine::RowVisibilitySource::Filter,
                        )
                        .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
                }
            }

            if !engine.config.defer_graph_building && !eager_formula_batches.is_empty() {
                engine
                    .source_formula_ingress()
                    .ingest_replay_batches(eager_formula_batches)
                    .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
            }
            if !eager_direct_batches.is_empty() {
                engine
                    .source_formula_ingress()
                    .finish_prepared(eager_direct_batches)
                    .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
            }

            {
                use rustc_hash::FxHashSet;

                let defined = self.defined_names()?;
                let mut seen: FxHashSet<(DefinedNameScope, Option<String>, String)> =
                    FxHashSet::default();

                for dn in defined {
                    let key = (dn.scope.clone(), dn.scope_sheet.clone(), dn.name.clone());
                    if !seen.insert(key) {
                        continue;
                    }

                    let scope = match dn.scope {
                        DefinedNameScope::Workbook => NameScope::Workbook,
                        DefinedNameScope::Sheet => {
                            let sheet_name = dn.scope_sheet.as_deref().ok_or_else(|| {
                                calamine::Error::Io(std::io::Error::other(format!(
                                    "sheet-scoped defined name `{}` missing scope_sheet",
                                    dn.name
                                )))
                            })?;
                            let sid = engine.sheet_id(sheet_name).ok_or_else(|| {
                                calamine::Error::Io(std::io::Error::other(format!(
                                    "scope sheet not found: {sheet_name}"
                                )))
                            })?;
                            NameScope::Sheet(sid)
                        }
                    };

                    let definition = match dn.definition {
                        DefinedNameDefinition::Range { address } => {
                            let sheet_id = engine
                                .sheet_id(&address.sheet)
                                .or_else(|| engine.add_sheet(&address.sheet).ok())
                                .ok_or_else(|| {
                                    calamine::Error::Io(std::io::Error::other(format!(
                                        "sheet not found: {}",
                                        address.sheet
                                    )))
                                })?;

                            let sr0 = address.start_row.saturating_sub(1);
                            let sc0 = address.start_col.saturating_sub(1);
                            let er0 = address.end_row.saturating_sub(1);
                            let ec0 = address.end_col.saturating_sub(1);

                            let start_ref =
                                CellRef::new(sheet_id, Coord::new(sr0, sc0, true, true));
                            if sr0 == er0 && sc0 == ec0 {
                                NamedDefinition::Cell(start_ref)
                            } else {
                                let end_ref =
                                    CellRef::new(sheet_id, Coord::new(er0, ec0, true, true));
                                let range_ref =
                                    formualizer_eval::reference::RangeRef::new(start_ref, end_ref);
                                NamedDefinition::Range(range_ref)
                            }
                        }
                        DefinedNameDefinition::Literal { value } => NamedDefinition::Literal(value),
                    };

                    engine
                        .define_name(&dn.name, definition, scope)
                        .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
                }
            }

            if debug {
                eprintln!(
                    "[fz][load] done: values={}, formulas={}, total={} ms",
                    total_values,
                    total_formulas,
                    t0.elapsed_millis(),
                );
            }
            for n in &names {
                engine.finalize_sheet_index(n);
            }

            self.load_stats = AdapterLoadStats {
                formula_cells_observed: Some(total_formulas as u64),
                value_cells_observed: Some(total_value_cells_observed as u64),
                value_slots_handed_to_engine: Some(total_values as u64),
                formula_cells_handed_to_engine: Some(total_formula_handed_to_engine as u64),
                shared_formula_tags_observed: Some(total_shared_formula_tags as u64),
            };
            Ok(())
        })();

        // Restore every temporary engine setting even when parsing, limits, or
        // graph ingest exits early.
        engine.set_first_load_assume_new(prev_first_load);
        engine.reset_ensure_touched();
        engine.set_sheet_index_mode(prev_index_mode);
        engine.config.range_expansion_limit = prev_range_limit;
        load_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_calamine_error_variants_preserve_generic_error_semantics() {
        let error = calamine::CellErrorType::GettingData;
        assert!(matches!(
            data_ref_to_literal(&DataRef::Error(error.clone())),
            Some(LiteralValue::Error(ref value)) if value.kind == ExcelErrorKind::Error
        ));
        assert!(matches!(
            data_ref_to_overlay(&DataRef::Error(error)),
            Some(OverlayValue::Error(8))
        ));
    }
}
