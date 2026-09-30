use crate::SheetId;

mod criteria;
mod exact;
mod family;
mod kernels;
mod lift;
mod memo;
use crate::arrow_store::{OverlayFragment, OverlayValue, SheetStore};
#[cfg(test)]
use crate::engine::Scheduler;
use crate::engine::arena::AstNodeId;
use crate::engine::eval_delta::{
    DeltaCollector, DeltaMode, EvalDelta, EvalDeltaCompatibilityPolicy,
};
use crate::engine::graph::editor::change_log::MutationCapture;
use crate::engine::graph::prepared_legacy_graph::PreparedLegacyGraphPlan;
use crate::engine::ingest_pipeline::{DependencyPlanRow, FormulaAstInput};
use crate::engine::live_edges::{LiveEdgeCollector, RecordingContext};
use crate::engine::live_graph::analyze_live_graph;
use crate::engine::lookup_index_cache::{
    BuildOutcome, LookupAxis, LookupIndex, LookupIndexCache, LookupIndexCacheReport,
    LookupIndexKey, estimate_bytes,
};
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::range_view::RangeView;
use crate::engine::row_visibility::RowVisibilityState;
use crate::engine::spill::{RegionLockManager, SpillMeta, SpillShape};
use crate::engine::target_preparation::{
    StagedFormulaIndex, StagedFormulaLease, StagedPackageLease,
};
use crate::engine::used_extent::{
    ExtentPolicy, OpenRangeBounds, resolve_used_extent_with_fallback,
};
use crate::engine::virtual_deps::VirtualDepBuilder;
use family::{LayerUnit, layer_units, unit_members};

#[path = "freshness.rs"]
mod freshness;
use crate::engine::template::region::Region;
use crate::engine::{
    ChangeLogger, CycleDetection, CyclePolicy, DependencyGraph, EvalConfig, EvaluationRequestKind,
    EvaluationRequestOutcome, EvaluationResourceBaselineStats, EvaluationResourceRequestStats,
    FormulaDirtyLeaseOutcome, FormulaIngestBatch, FormulaIngestRecord, FormulaIngestReport,
    FormulaParseDiagnostic, FormulaParsePolicy, FormulaPlaneMode, ResourceLedger,
    RowVisibilitySource, ScheduleUnit, VertexId, VertexKind, VisibilityMaskMode,
};
use crate::function::FnCaps;
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord, RangeRef};
use crate::traits::FunctionProvider;
use crate::traits::{EvaluationContext, ReferenceInfo, Resolver};
use formualizer_common::{
    CoordBuildHasher, LiteralValue, col_letters_from_1based, parse_a1_1based,
};
use formualizer_parse::parser::ReferenceType;
use formualizer_parse::{ASTNode, ASTNodeType, ExcelError, ExcelErrorKind};
use rayon::ThreadPoolBuilder;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

type StagedFormulaEntry = (u32, u32, String);
type StagedSheetParts = (
    Vec<StagedFormulaEntry>,
    Option<crate::engine::DeferredFormulaPackage>,
);

/// Per-sheet staged-formula store (NOTE(#126) follow-up).
///
/// Ingest consumers (`build_graph_all`/`build_graph_for_sheets`) walk staged
/// entries in INSERTION order, so the order-preserving `Vec` stays the
/// canonical storage; a `(row, col) → index` map removes the linear dup-scan
/// that made `stage_formula_text`/`get_staged_formula_text` O(staged-on-sheet)
/// per call (O(n²) for an n-formula deferred load on one sheet — ~570 ms for
/// 50k stages, release, before the index). `stage`/`get` are O(1);
/// `remove` keeps the old O(n) `Vec::remove` (rare path, order preserved).
#[derive(Default)]
pub(crate) struct StagedSheet {
    entries: Vec<StagedFormulaEntry>,
    index: FxHashMap<(u32, u32), usize>,
    deferred_package: Option<crate::engine::DeferredFormulaPackage>,
}

impl StagedSheet {
    fn invalidate_all_deferred_families(package: &mut crate::engine::DeferredFormulaPackage) {
        package
            .invalidated
            .extend(package.families.iter().map(|family| family.source_id));
        package.invalidated.extend(
            package
                .partitioned_families
                .iter()
                .map(|family| family.source_id),
        );
    }

    fn invalidate_deferred_at(&mut self, row: u32, col: u32) {
        let Some(package) = self.deferred_package.as_mut() else {
            return;
        };
        if package.suppressed.contains(&(row, col)) {
            // Previously consumed coordinates are already excluded from residual
            // authority. Edits/deletion must not invalidate untouched fragments.
            return;
        }
        if package.source_geometry_complete
            && package.families.is_empty()
            && package.partitioned_families.is_empty()
        {
            if let Some((row0, col0)) = row.checked_sub(1).zip(col.checked_sub(1))
                && package
                    .source_coordinates
                    .binary_search(&crate::engine::SourceCoord {
                        row: row0,
                        col: col0,
                    })
                    .is_ok()
            {
                package.suppressed.insert((row, col));
            }
            return;
        }
        let family = package
            .replay
            .lock()
            .ok()
            .and_then(|mut replay| replay.formula_at(row, col).ok())
            .flatten()
            .and_then(|record| record.partition_owner.or(record.family));
        if let Some(family) = family {
            package.invalidated.insert(family);
        } else {
            // A poisoned lock, replay failure, malformed spool, or missing
            // lookup result must never let a possibly edited family commit.
            // Fail closed at package scope before suppressing the edited cell.
            Self::invalidate_all_deferred_families(package);
        }
        package.suppressed.insert((row, col));
    }

    fn reconcile_attached_deferred_package(&mut self) {
        let Some(package) = self.deferred_package.as_mut() else {
            return;
        };
        if self.entries.is_empty() {
            return;
        }

        let replayed = (|| {
            let mut disposition = crate::engine::FormulaReplayDisposition::default();
            for partition in &package.partitioned_families {
                disposition.register_partition(partition, false)?;
            }
            package
                .replay
                .lock()
                .map_err(|_| "deferred formula spool lock poisoned".to_string())?
                .replay_partitioned(&disposition, &package.partitioned_families)
        })();

        let Ok(records) = replayed else {
            Self::invalidate_all_deferred_families(package);
            package
                .suppressed
                .extend(self.entries.iter().map(|(row, col, _)| (*row, *col)));
            return;
        };
        let mut owners = FxHashMap::default();
        for record in &records {
            owners
                .entry((record.row, record.col))
                .or_insert(record.partition_owner.or(record.family));
        }
        for (row, col, _) in &self.entries {
            if let Some(family) = owners.get(&(*row, *col)).copied().flatten() {
                package.invalidated.insert(family);
            } else {
                Self::invalidate_all_deferred_families(package);
            }
            package.suppressed.insert((*row, *col));
        }
        package.reconciliation_replay = Some(records);
    }

    fn stage(&mut self, row: u32, col: u32, text: String) {
        self.invalidate_deferred_at(row, col);
        match self.index.entry((row, col)) {
            std::collections::hash_map::Entry::Occupied(slot) => {
                self.entries[*slot.get()].2 = text;
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(self.entries.len());
                self.entries.push((row, col, text));
            }
        }
    }

    fn remove(&mut self, row: u32, col: u32) -> Option<String> {
        let deferred_text = self.get(row, col);
        self.invalidate_deferred_at(row, col);
        let Some(idx) = self.index.remove(&(row, col)) else {
            return deferred_text;
        };
        let (_, _, text) = self.entries.remove(idx);
        // `Vec::remove` shifted everything after `idx` left by one.
        for slot in self.index.values_mut() {
            if *slot > idx {
                *slot -= 1;
            }
        }
        Some(text)
    }

    fn get_ordinary(&self, row: u32, col: u32) -> Option<&str> {
        self.index
            .get(&(row, col))
            .map(|&i| self.entries[i].2.as_str())
    }

    fn remove_ordinary(&mut self, row: u32, col: u32) -> Option<String> {
        let idx = self.index.remove(&(row, col))?;
        let (_, _, text) = self.entries.remove(idx);
        for slot in self.index.values_mut() {
            if *slot > idx {
                *slot -= 1;
            }
        }
        Some(text)
    }

    fn get(&self, row: u32, col: u32) -> Option<String> {
        if let Some(text) = self.get_ordinary(row, col) {
            return Some(text.to_string());
        }
        let package = self.deferred_package.as_ref()?;
        if package.suppressed.contains(&(row, col)) {
            return None;
        }
        package
            .replay
            .lock()
            .ok()?
            .formula_at(row, col)
            .ok()?
            .map(|record| record.text)
    }

    fn len(&self) -> usize {
        self.deferred_package
            .as_ref()
            .map_or(self.entries.len(), |package| {
                (if package.source_geometry_complete
                    && (package.coordinates_cover_families
                        || (package.families.is_empty() && package.partitioned_families.is_empty()))
                {
                    package.source_coordinates.len()
                } else {
                    usize::try_from(package.report.source_formula_records_spooled)
                        .unwrap_or(usize::MAX)
                })
                .saturating_sub(package.suppressed.len())
                .saturating_add(self.entries.len())
            })
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.deferred_package.is_none()
    }

    fn into_parts(self) -> StagedSheetParts {
        (self.entries, self.deferred_package)
    }
}

type StagedFormulaMap = std::collections::HashMap<String, StagedSheet>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PreparationRegion {
    sheet: String,
    sheet_id: SheetId,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
}

// Target roots dedupe by identity; they keep their first-seen order.
struct OrderedTargetProducers {
    ordered: Vec<crate::engine::target_preparation::TargetProducer>,
    seen: FxHashSet<crate::engine::target_preparation::TargetProducer>,
}

impl OrderedTargetProducers {
    fn with_capacity(capacity: usize) -> Result<Self, std::collections::TryReserveError> {
        let mut ordered = Vec::new();
        ordered.try_reserve(capacity)?;
        let mut seen = FxHashSet::default();
        seen.try_reserve(capacity)?;
        Ok(Self { ordered, seen })
    }

    fn from_ordered(
        ordered: Vec<crate::engine::target_preparation::TargetProducer>,
    ) -> Result<Self, std::collections::TryReserveError> {
        let mut seen = FxHashSet::default();
        seen.try_reserve(ordered.len())?;
        seen.extend(ordered.iter().copied());
        Ok(Self { ordered, seen })
    }

    fn push(
        &mut self,
        producer: crate::engine::target_preparation::TargetProducer,
    ) -> Result<bool, std::collections::TryReserveError> {
        #[cfg(test)]
        TARGET_ROOT_DEDUP_PROBES.with(|probes| probes.set(probes.get().saturating_add(1)));
        if self.seen.contains(&producer) {
            return Ok(false);
        }
        self.seen.try_reserve(1)?;
        self.ordered.try_reserve(1)?;
        self.seen.insert(producer);
        self.ordered.push(producer);
        Ok(true)
    }

    fn len(&self) -> usize {
        self.ordered.len()
    }

    fn into_vec(self) -> Vec<crate::engine::target_preparation::TargetProducer> {
        self.ordered
    }
}

#[cfg(test)]
thread_local! {
    static TARGET_ROOT_DEDUP_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn target_root_allocation_error(observed: usize, request_id: Option<u64>) -> ExcelError {
    crate::engine::ResourceLedgerError::Exhausted(formualizer_common::ResourceExhaustionDetail {
        reason: formualizer_common::ResourceExhaustionReason::ScratchMemory,
        limit: u64::MAX,
        observed: observed as u64,
        request_id,
    })
    .into_excel_error()
}

#[derive(Debug)]
struct PreparedOrdinaryStagedFormula {
    sheet: String,
    sheet_id: SheetId,
    lease: StagedFormulaLease,
    ast_id: Option<AstNodeId>,
    plan: Option<DependencyPlanRow>,
}

struct PreparedTargetSourcePackage {
    sheet: String,
    sheet_id: SheetId,
    lease: StagedPackageLease,
    selected_points: Option<BTreeSet<(u32, u32)>>,
    complete_selections: BTreeSet<crate::engine::SourceFamilyId>,
    deferred_shared: bool,
    direct_domains: Vec<(
        crate::engine::SourceFamilyId,
        crate::engine::PlacementDomainTransport,
    )>,
    source_report: crate::engine::FormulaCompressedSourceReport,
    replay_records: Vec<crate::engine::DeferredReplayFormula>,
    spool_replays: u64,
    disposition: crate::engine::FormulaReplayDisposition,
    legacy: Vec<(u32, u32, AstNodeId, DependencyPlanRow)>,
    direct_families: usize,
    direct_cells: u64,
    direct_fragments: u64,
    direct_complete_families: u64,
    direct_complete_cells: u64,
    direct_partition_families: u64,
    direct_partition_cells: u64,
    anchor_parses: u64,
    anchor_asts: u64,
    anchor_analyses: u64,
}

impl PreparedTargetSourcePackage {
    fn empty_selection(sheet: &str, sheet_id: SheetId, lease: StagedPackageLease) -> Self {
        Self {
            sheet: sheet.to_owned(),
            sheet_id,
            lease,
            selected_points: Some(BTreeSet::new()),
            complete_selections: Default::default(),
            deferred_shared: false,
            direct_domains: Vec::new(),
            source_report: Default::default(),
            replay_records: Vec::new(),
            spool_replays: 0,
            disposition: Default::default(),
            legacy: Vec::new(),
            direct_families: 0,
            direct_cells: 0,
            direct_fragments: 0,
            direct_complete_families: 0,
            direct_complete_cells: 0,
            direct_partition_families: 0,
            direct_partition_cells: 0,
            anchor_parses: 0,
            anchor_asts: 0,
            anchor_analyses: 0,
        }
    }

    fn direct_contains(&self, row: u32, col: u32) -> bool {
        self.direct_domains.iter().any(|(_, domain)| {
            let rect = domain.rect();
            row > rect.start.row
                && row <= rect.end.row + 1
                && col > rect.start.col
                && col <= rect.end.col + 1
        })
    }

    fn fallback_records(&self) -> impl Iterator<Item = &crate::engine::DeferredReplayFormula> {
        self.replay_records.iter().filter(|record| {
            let Some((row, col)) = record.row.checked_sub(1).zip(record.col.checked_sub(1)) else {
                return true;
            };
            let coord = crate::engine::SourceCoord { row, col };
            let disposition = match record.family {
                Some(family) => self.disposition.shared_disposition(family, coord),
                None => self.disposition.ordinary_disposition(coord).0,
            };
            !matches!(
                disposition,
                crate::engine::FormulaReplayCoordinateDisposition::Direct
                    | crate::engine::FormulaReplayCoordinateDisposition::Suppressed
            )
        })
    }
}

type PreparedFormulaBatches = Vec<FormulaIngestBatch>;
type StagedFormulaBatches = Vec<(String, StagedSheet)>;

type CompressedReplayBatch = (
    FormulaIngestBatch,
    crate::engine::FormulaCompressedSourceBatch,
);
type PreparedSourceBatch = (
    FormulaIngestBatch,
    crate::engine::FormulaCompressedSourceReport,
    crate::engine::FormulaCompressedPreparation,
);
type PreparedStagedFormulaBatches = (
    PreparedFormulaBatches,
    Vec<CompressedReplayBatch>,
    Vec<PreparedSourceBatch>,
    // Formulas whose planning can fail (see `formula_may_fail_planning`).
    FxHashSet<crate::engine::arena::AstNodeId>,
);

/// Backend-neutral source-family ingress. Adapters may prepare candidates and
/// submit exact replay; every formula is still materialized per cell.
#[doc(hidden)]
pub struct SourceFormulaIngress<'a, R> {
    engine: &'a mut Engine<R>,
}

impl<R> SourceFormulaIngress<'_, R>
where
    R: EvaluationContext,
{
    pub fn prepare_families(
        &mut self,
        sheet_name: &str,
        families: &[crate::engine::SourceFormulaFamily],
    ) -> Result<crate::engine::FormulaCompressedPreparation, ExcelError> {
        self.engine.observe_function_semantic_epoch()?;
        Ok(self
            .engine
            .prepare_source_formula_families(sheet_name, families))
    }

    pub fn prepare_eager_proposals(
        &mut self,
        sheet_name: &str,
        families: &[crate::engine::SourceFormulaFamily],
        partitions: &[crate::engine::PartitionedSourceFormulaFamily],
        formula_record_count: u64,
        replay: Box<dyn crate::engine::DeferredFormulaReplay>,
    ) -> Result<crate::engine::FormulaCompressedPreparation, ExcelError> {
        self.engine.observe_function_semantic_epoch()?;
        let replay = Arc::new(std::sync::Mutex::new(replay));
        self.engine.prepare_source_formula_proposals(
            sheet_name,
            families,
            partitions,
            partitions,
            formula_record_count,
            replay,
            &BTreeSet::new(),
            &Default::default(),
            None,
        )
    }

    pub fn stage_deferred(&mut self, package: crate::engine::DeferredFormulaPackage) {
        self.engine.stage_deferred_formula_package(package);
    }

    pub fn ingest_replay_batches(
        &mut self,
        batches: Vec<(
            FormulaIngestBatch,
            crate::engine::FormulaCompressedSourceBatch,
        )>,
    ) -> Result<FormulaIngestReport, ExcelError> {
        self.engine
            .ingest_compressed_formula_source_batches(batches)
    }

    pub fn finish_prepared(
        &mut self,
        batches: Vec<(
            FormulaIngestBatch,
            crate::engine::FormulaCompressedSourceReport,
            crate::engine::FormulaCompressedPreparation,
        )>,
    ) -> Result<FormulaIngestReport, ExcelError> {
        self.engine.finish_compressed_formula_sources(batches)
    }
}

// Computed-write coalescing pays a fixed grouping/planning cost. For very narrow
// layers there is not enough work to amortize it, and the direct point-write path
// is faster while preserving the same visibility semantics.
const COMPUTED_WRITE_COALESCING_MIN_LAYER_WIDTH: usize = 8;

/// Equal values, numbers by bits (debug oracles).
#[cfg(debug_assertions)]
fn same_value_bits(a: &LiteralValue, b: &LiteralValue) -> bool {
    match (a, b) {
        (LiteralValue::Number(x), LiteralValue::Number(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

/// Whether a layer's computed writes are buffered: not for a chain unit,
/// whose members read the ones written before them.
fn buffer_layer_writes(layer: &crate::engine::scheduler::Layer) -> bool {
    !layer.sequential && layer.vertices.len() >= COMPUTED_WRITE_COALESCING_MIN_LAYER_WIDTH
}
/// Adaptive layer parallelism (see `evaluate_layer_parallel`): a layer runs
/// sequentially until either it has run `PARALLEL_LAYER_PROBE` or the rest,
/// at the rate so far, is estimated at `PARALLEL_LAYER_WORTH` or more; then
/// the rest goes to the thread pool. Tuned on Enron first eval (probe 30 /
/// 100 / 300 µs, worth 50 / 150 / 400 µs; 300 / 150 best, no workbook
/// slower).
/// Candidate count from which schedule preparation sorts on the pool.
const PARALLEL_SCHEDULE_MIN_CANDIDATES: usize = 16 * 1024;
/// Plan reuse: a schedule of more candidates than this can become the
/// base, and a request takes the base restricted to it when the base is at
/// most `BASE_SCHEDULE_RATIO` times the request.
const BASE_SCHEDULE_MIN_VERTICES: usize = 64;
const BASE_SCHEDULE_RATIO: usize = 256;
/// A restricted schedule keeps the base's layers, which can split a family
/// run the planner would keep whole (each piece then builds its own
/// criteria index): below this many candidates planning costs less than
/// that (real_ops_model: 75 candidates, 31 us to plan, +0.3 ms restricted).
const BASE_SCHEDULE_MIN_REQUEST: usize = 128;
const PARALLEL_LAYER_PROBE: std::time::Duration = std::time::Duration::from_micros(300);
const PARALLEL_LAYER_WORTH: std::time::Duration = std::time::Duration::from_micros(150);
/// A member this expensive (ns, measured by the probe) is its own task.
const EXPENSIVE_VERTEX_NS: u128 = 10_000;
/// [`Engine::live_cancellation_after_work`] messages.
const UNIT_CANCELLED: &str = "Evaluation cancelled; the evaluated unit was not committed";
const GROUP_CANCELLED: &str =
    "Parallel evaluation cancelled; the evaluated group was not committed";
const SCC_CANCELLED: &str = "Evaluation cancelled; the evaluated cycle was not completed";

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ComputedWrite {
    Cell {
        seq: u64,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        value: OverlayValue,
        format_id: Option<crate::format::FormatId>,
    },
    Rect {
        seq: u64,
        sheet_id: SheetId,
        sr0: u32,
        sc0: u32,
        values: Vec<Vec<OverlayValue>>,
    },
    /// Consecutive rows `row0..` of one column (a family run's commit),
    /// each with its format.
    Run {
        seq: u64,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        entries: Vec<(OverlayValue, Option<crate::format::FormatId>)>,
    },
}

impl ComputedWrite {
    #[inline]
    pub(crate) fn seq(&self) -> u64 {
        match self {
            ComputedWrite::Cell { seq, .. }
            | ComputedWrite::Rect { seq, .. }
            | ComputedWrite::Run { seq, .. } => *seq,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ComputedWriteBuffer {
    writes: Vec<ComputedWrite>,
    next_seq: u64,
    estimated_bytes: usize,
    formats_present: bool,
}

impl ComputedWriteBuffer {
    const ENTRY_BASE_BYTES: usize = 32;

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.writes.len()
    }

    #[inline]
    pub(crate) fn writes(&self) -> &[ComputedWrite] {
        &self.writes
    }

    #[inline]
    pub(crate) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    #[cfg(feature = "tracing")]
    fn traced_cell_count(&self) -> usize {
        self.writes
            .iter()
            .map(|write| match write {
                ComputedWrite::Cell { .. } => 1,
                ComputedWrite::Rect { values, .. } => values.iter().map(Vec::len).sum(),
                ComputedWrite::Run { entries, .. } => entries.len(),
            })
            .sum()
    }

    pub(crate) fn push_cell(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        value: OverlayValue,
    ) {
        self.push_cell_with_format(sheet_id, row0, col0, value, None);
    }

    pub(crate) fn push_cell_with_format(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        value: OverlayValue,
        format_id: Option<crate::format::FormatId>,
    ) {
        let format_id = format_id.filter(|id| *id != crate::format::FormatId::GENERAL);
        self.formats_present |= format_id.is_some();
        let seq = self.next_sequence();
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_add(Self::estimate_value_bytes(&value));
        self.writes.push(ComputedWrite::Cell {
            seq,
            sheet_id,
            row0,
            col0,
            value,
            format_id,
        });
    }

    pub(crate) fn push_column_run(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        mut entries: Vec<(OverlayValue, Option<crate::format::FormatId>)>,
    ) {
        let seq = self.next_sequence();
        let mut added = 0usize;
        for (value, format_id) in entries.iter_mut() {
            *format_id = format_id.filter(|id| *id != crate::format::FormatId::GENERAL);
            self.formats_present |= format_id.is_some();
            added = added.saturating_add(Self::estimate_value_bytes(value));
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(added);
        self.writes.push(ComputedWrite::Run {
            seq,
            sheet_id,
            row0,
            col0,
            entries,
        });
    }

    pub(crate) fn push_rect(
        &mut self,
        sheet_id: SheetId,
        sr0: u32,
        sc0: u32,
        values: Vec<Vec<OverlayValue>>,
    ) {
        let seq = self.next_sequence();
        let added = values
            .iter()
            .flat_map(|row| row.iter())
            .map(Self::estimate_value_bytes)
            .fold(0usize, usize::saturating_add);
        self.estimated_bytes = self.estimated_bytes.saturating_add(added);
        self.writes.push(ComputedWrite::Rect {
            seq,
            sheet_id,
            sr0,
            sc0,
            values,
        });
    }

    pub(crate) fn clear(&mut self) {
        self.writes.clear();
        self.estimated_bytes = 0;
        self.formats_present = false;
    }

    fn take_writes(&mut self) -> (Vec<ComputedWrite>, bool) {
        self.estimated_bytes = 0;
        let formats_present = std::mem::take(&mut self.formats_present);
        (std::mem::take(&mut self.writes), formats_present)
    }

    fn next_sequence(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        seq
    }

    #[inline]
    fn estimate_value_bytes(value: &OverlayValue) -> usize {
        Self::ENTRY_BASE_BYTES.saturating_add(value.estimated_payload_bytes())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ComputedWriteChunkKey {
    sheet_id: SheetId,
    col0: u32,
    chunk_idx: usize,
    chunk_start_row0: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ComputedWriteChunkEntryPlan {
    pub(crate) row_in_chunk: usize,
    pub(crate) seq: u64,
    pub(crate) value: OverlayValue,
    pub(crate) format_id: Option<crate::format::FormatId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComputedWriteChunkPlanShape {
    Point,
    SparseOffsets {
        entries: usize,
        span_len: usize,
    },
    DenseRange {
        start: usize,
        len: usize,
    },
    RunRange {
        start: usize,
        len: usize,
        runs: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComputedWriteFormatClear {
    Range { start: usize, end: usize },
    Offsets(Vec<usize>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ComputedWriteChunkFormatEffect {
    NoFormatWork,
    ClearStale(ComputedWriteFormatClear),
    SetExplicit(Vec<(usize, Option<crate::format::FormatId>)>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ComputedWriteChunkPlan {
    pub(crate) sheet_id: SheetId,
    pub(crate) col0: u32,
    pub(crate) chunk_idx: usize,
    pub(crate) chunk_start_row0: u32,
    pub(crate) entries: Vec<ComputedWriteChunkEntryPlan>,
    pub(crate) shape: ComputedWriteChunkPlanShape,
    pub(crate) format_effect: ComputedWriteChunkFormatEffect,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ComputedWriteCoalescingPlan {
    pub(crate) chunks: Vec<ComputedWriteChunkPlan>,
    pub(crate) input_cells: usize,
    pub(crate) coalesced_cells: usize,
    pub(crate) overwritten_cells: usize,
}

impl ComputedWriteCoalescingPlan {
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

impl ComputedWriteChunkPlan {
    fn from_group(
        key: ComputedWriteChunkKey,
        mut entries: Vec<ComputedWriteChunkEntryPlan>,
        formats_present: bool,
        computed_lane_has_formats: bool,
    ) -> (Self, usize) {
        entries.sort_by_key(|entry| (entry.row_in_chunk, entry.seq));
        let input_len = entries.len();
        let mut coalesced: Vec<ComputedWriteChunkEntryPlan> = Vec::with_capacity(input_len);
        for entry in entries {
            if let Some(prev) = coalesced.last_mut()
                && prev.row_in_chunk == entry.row_in_chunk
            {
                *prev = entry;
                continue;
            }
            coalesced.push(entry);
        }
        let overwritten = input_len.saturating_sub(coalesced.len());
        let shape = Self::classify_shape(&coalesced);
        let format_effect = Self::classify_format_effect(
            &coalesced,
            &shape,
            formats_present,
            computed_lane_has_formats,
        );
        (
            Self {
                sheet_id: key.sheet_id,
                col0: key.col0,
                chunk_idx: key.chunk_idx,
                chunk_start_row0: key.chunk_start_row0,
                entries: coalesced,
                shape,
                format_effect,
            },
            overwritten,
        )
    }

    fn classify_format_effect(
        entries: &[ComputedWriteChunkEntryPlan],
        shape: &ComputedWriteChunkPlanShape,
        formats_present: bool,
        computed_lane_has_formats: bool,
    ) -> ComputedWriteChunkFormatEffect {
        if !formats_present {
            return if computed_lane_has_formats {
                ComputedWriteChunkFormatEffect::ClearStale(Self::format_clear_spec(entries, shape))
            } else {
                ComputedWriteChunkFormatEffect::NoFormatWork
            };
        }

        if entries.iter().all(|entry| entry.format_id.is_none()) {
            return if computed_lane_has_formats {
                ComputedWriteChunkFormatEffect::ClearStale(Self::format_clear_spec(entries, shape))
            } else {
                ComputedWriteChunkFormatEffect::NoFormatWork
            };
        }

        ComputedWriteChunkFormatEffect::SetExplicit(
            entries
                .iter()
                .map(|entry| (entry.row_in_chunk, entry.format_id))
                .collect(),
        )
    }

    fn format_clear_spec(
        entries: &[ComputedWriteChunkEntryPlan],
        shape: &ComputedWriteChunkPlanShape,
    ) -> ComputedWriteFormatClear {
        match shape {
            ComputedWriteChunkPlanShape::DenseRange { start, len }
            | ComputedWriteChunkPlanShape::RunRange { start, len, .. } => {
                ComputedWriteFormatClear::Range {
                    start: *start,
                    end: start.saturating_add(*len),
                }
            }
            ComputedWriteChunkPlanShape::Point
            | ComputedWriteChunkPlanShape::SparseOffsets { .. } => {
                ComputedWriteFormatClear::Offsets(
                    entries.iter().map(|entry| entry.row_in_chunk).collect(),
                )
            }
        }
    }

    fn classify_shape(entries: &[ComputedWriteChunkEntryPlan]) -> ComputedWriteChunkPlanShape {
        debug_assert!(!entries.is_empty());
        if entries.len() == 1 {
            return ComputedWriteChunkPlanShape::Point;
        }

        let start = entries[0].row_in_chunk;
        let end = entries[entries.len() - 1].row_in_chunk;
        let span_len = end.saturating_sub(start).saturating_add(1);
        if span_len != entries.len() {
            return ComputedWriteChunkPlanShape::SparseOffsets {
                entries: entries.len(),
                span_len,
            };
        }

        let runs = Self::run_count(entries);
        if runs < entries.len() {
            ComputedWriteChunkPlanShape::RunRange {
                start,
                len: entries.len(),
                runs,
            }
        } else {
            ComputedWriteChunkPlanShape::DenseRange {
                start,
                len: entries.len(),
            }
        }
    }

    fn run_count(entries: &[ComputedWriteChunkEntryPlan]) -> usize {
        let mut runs = 0usize;
        let mut prev: Option<&OverlayValue> = None;
        for entry in entries {
            if prev != Some(&entry.value) {
                runs = runs.saturating_add(1);
                prev = Some(&entry.value);
            }
        }
        runs
    }
}

#[cfg(feature = "tracing")]
#[derive(Default)]
struct TraceEvaluationCounters {
    computed_vertices: usize,
    cycles: usize,
}

pub struct Engine<R> {
    pub(crate) graph: DependencyGraph,
    resolver: R,
    pub config: EvalConfig,
    workbook_load_limits: crate::engine::WorkbookLoadLimits,
    /// Clock for volatile date/time builtins, wrapped in a per-recalc
    /// snapshot: sampled once at the start of every evaluation request
    /// ([`Self::begin_evaluation_request`]) so all `NOW()`/`TODAY()` reads in
    /// one recalc — including SCC iteration passes — agree (spec §7.11).
    clock: crate::timezone::SnapshotClock,
    thread_pool: Option<Arc<rayon::ThreadPool>>,
    pub recalc_epoch: u64,
    snapshot_id: std::sync::atomic::AtomicU64,
    topology_epoch: u64,
    cached_static_schedule: Option<CachedScheduleEntry>,
    /// Program 3 (plan reuse): schedules of recent earlier requests, most
    /// recent last (a user alternating between a few inputs recalculates
    /// the same few closures). Bounded by `RECENT_SCHEDULES` entries and
    /// `RECENT_SCHEDULE_VERTICES` candidate vertices in total.
    recent_schedules: Vec<CachedScheduleEntry>,
    /// Program 3 (plan reuse): the largest current schedule seen (usually
    /// the first evaluation's). A request it covers, and that is not much
    /// smaller, takes its schedule restricted to the request instead of
    /// planning (`Schedule::restrict`).
    base_schedule: Option<CachedScheduleEntry>,
    #[cfg(any(test, feature = "benchmark_internal"))]
    recalc_reuse_probe: std::sync::Mutex<RecalcReuseProbe>,
    spill_mgr: ShimSpillManager,
    /// Arrow-backed storage for sheet values (Phase A)
    arrow_sheets: SheetStore,
    /// Workbook-local number-format registry.
    format_registry: crate::format::FormatRegistry,
    /// Derived formula formats keyed by grid position, never graph vertex identity.
    derived_formats: crate::engine::derived_formats::DerivedFormats,
    #[cfg(test)]
    derived_format_operations_for_test: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    family_members_for_test: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    invariant_bound_members_for_test: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    lifted_members_for_test: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    chained_members_for_test: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    lane_clean_reads_for_test: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    criteria_kernel_members_for_test: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    memo_hits_for_test: std::sync::atomic::AtomicU64,
    /// Authority build last compressed (`maybe_compress_formulas`).
    compressed_at_build: Option<u64>,
    #[cfg(test)]
    computed_overlay_set_explicit_entry_operations_for_test: u64,
    #[cfg(test)]
    computed_overlay_stale_clear_range_effects_for_test: u64,
    #[cfg(test)]
    computed_overlay_stale_clear_offset_attempts_for_test: u64,
    #[cfg(test)]
    computed_format_vector_allocations_for_test: std::sync::atomic::AtomicU64,
    /// True if any edit after bulk load; disables Arrow reads for parity
    has_edited: bool,
    /// Overlay compaction counter (Phase C instrumentation)
    overlay_compactions: u64,

    // Overlay memory observability / budget (ticket 503)
    computed_overlay_bytes_estimate: usize,
    computed_overlay_mirroring_disabled: bool,
    /// When true, RangeView resolution materializes from graph/Arrow base per-cell.
    /// This preserves correctness if we stop mirroring formula/spill outputs into computed overlays.
    pub(crate) force_materialize_range_views: bool,
    // Pass-scoped cache for Arrow used-row bounds per column
    row_bounds_cache: std::sync::RwLock<Option<RowBoundsCache>>,
    // Snapshot-scoped final used-axis bounds for open-ended references.
    used_axis_bounds_cache: std::sync::RwLock<Option<UsedAxisBoundsCache>>,
    lookup_index_cache: LookupIndexCache,
    source_cache: Arc<std::sync::RwLock<SourceCache>>,
    /// Identity binding for opaque source-family preparations.
    source_formula_token: Arc<()>,
    /// Dedicated identity binding for reusable recalculation plans.
    recalc_plan_token: Arc<()>,
    /// Staged formulas by sheet when `defer_graph_building` is enabled.
    staged_formulas: StagedFormulaMap,
    /// Presence and generation authority for ordinary staged formula discovery.
    staged_formula_index: StagedFormulaIndex,
    // Occupancy invalidation only: never a formula/read dependency.
    blocked_pending_spills: Vec<(VertexId, CellRef, Region)>,
    /// Per-sheet row visibility sidecar state.
    row_visibility: FxHashMap<SheetId, RowVisibilityState>,
    /// Cached row visibility masks keyed by sheet/span/mode/version.
    row_visibility_mask_cache: std::sync::RwLock<
        FxHashMap<VisibilityMaskCacheKey, std::sync::Arc<arrow_array::BooleanArray>>,
    >,
    /// Non-fatal malformed formula diagnostics captured during ingest/graph-build.
    formula_parse_diagnostics: Vec<FormulaParseDiagnostic>,
    /// Last centralized formula ingest report.
    last_formula_ingest_report: Option<FormulaIngestReport>,
    /// Aggregate centralized formula ingest report for this engine.
    formula_ingest_report_total: FormulaIngestReport,
    /// Transient cancellation flag used during evaluation
    active_cancel_flag: Option<crate::engine::CancelToken>,
    /// Transient absolute deadline used by composed target and plan requests.
    active_evaluation_deadline: Option<Instant>,

    /// Engine-level action depth.
    ///
    /// Ticket 614 introduces `Engine::action` as a stable, commit-only transaction surface.
    /// Nested actions are currently disallowed (deterministic rule) and will return an error.
    action_depth: u32,

    // Phase 3b virtual-dependency convergence telemetry
    last_virtual_dep_telemetry: VirtualDepTelemetry,
    virtual_dep_fallback_activations: u64,

    // Runtime-cycle SCC evaluation telemetry (RFC #112, Stage 2)
    last_cycle_telemetry: CycleTelemetry,

    // C0 evaluation-resource observability. IDs are never reset or reused.
    next_evaluation_resource_request_id: u64,
    evaluation_resource_request_depth: usize,
    active_evaluation_resource_request: Option<EvaluationResourceRequestStats>,
    last_evaluation_resource_request: Option<EvaluationResourceRequestStats>,
    evaluation_resource_baseline: EvaluationResourceBaselineStats,
    evaluation_resource_request_started_at: Option<crate::instant::FzInstant>,
    evaluation_resource_budgets: crate::engine::EvaluationBudgets,
    evaluation_resource_config_diagnostic:
        Option<crate::engine::EvaluationResourceConfigDiagnostic>,
    active_resource_ledger: Option<ResourceLedger>,
    source_cache_footprints: Vec<std::sync::Weak<std::sync::atomic::AtomicU64>>,
    source_cache_accounted: u64,

    /// SCC members that entered iterative calculation (`CyclePolicy::Iterate`
    /// with a witnessed live cycle) during the current evaluation request
    /// and must re-run on the next one.
    ///
    /// Excel re-evaluates circular cells on EVERY recalc (the accumulator
    /// contract, spec §4/§7.6), but this engine's dirty model marks SCC
    /// members clean after a recalc and would otherwise skip them forever.
    /// Resolution: members of iterating SCCs are redirtied volatile-like at
    /// the end of the same recalc that iterated them
    /// ([`Self::redirty_for_next_recalc`], called wherever
    /// `redirty_volatiles` runs). The set is per-recalc, never persisted:
    /// if an edit breaks the cycle, the next recalc's SCC task either does
    /// not exist or settles as phantom, nothing re-registers, and the
    /// redirty chain stops by itself.
    ///
    /// SCCs that landed on an exact fixed point are exempt: they go to
    /// [`Self::retained_scc_members`] instead and stay clean until the dirty
    /// graph (or a config change) reaches them (#368).
    pending_iterative_redirty: Vec<VertexId>,
    /// Members of iterating SCCs retained across recalcs (#368), keyed to the
    /// id of the retained SCC they belong to (ids come from
    /// `next_retained_scc_id`; grouping is only used for telemetry).
    ///
    /// An SCC is retained when the recalc that iterated it stopped because
    /// every member reproduced its previous value exactly (|Δ| = 0, or
    /// identity for non-numeric members; never NaN-converged), before the
    /// `max_iterations` cap, with no volatile or dynamic-reference member.
    /// Such an SCC is a fixed point of its own inputs: running it again with
    /// the same inputs cannot change any value, so it is not redirtied. The
    /// dirty graph remains the validity authority — any edit that reaches a
    /// member dirties it like any other formula and the SCC task re-runs.
    /// Membership is dropped when a member runs in an SCC task again (it is
    /// then re-retained or re-registered for per-recalc redirty), when the
    /// vertex is deleted, or when
    /// [`Self::reconcile_retained_sccs_at_request_begin`] invalidates the
    /// whole set because a config knob outside the graph changed.
    retained_scc_members: FxHashMap<VertexId, u64>,
    next_retained_scc_id: u64,
    /// [`Self::retained_scc_config_fingerprint`] as of the last retention.
    /// `Engine::config` is a public field, so knobs that change a retained
    /// SCC's result (cycle policy/tolerance, date system, determinism,
    /// volatile seeding) can change between recalcs without touching the
    /// graph; a mismatch at request begin dirties every retained member.
    /// Only meaningful while `retained_scc_members` is non-empty.
    retained_scc_config_fingerprint: u64,
    /// Function-registry semantic epoch and runtime-provider revision as of
    /// the last time retained SCCs were reconciled against them. A newer
    /// epoch dirties only the retained members whose formula calls a changed
    /// function (or every member when the change log is incomplete).
    retained_scc_function_epoch_seen: u64,
    retained_scc_provider_revision_seen: Option<u64>,
    /// Retained members that were already dirty when the current request
    /// began, with the SCC id they carried. A member still holding that id
    /// at the end of the request was not touched by any SCC task — its
    /// cycle dissolved (it evaluated as an ordinary formula) or the request
    /// never reached it — so [`Self::redirty_for_next_recalc`] drops it from
    /// the retained set instead of letting it linger.
    retained_scc_dirty_at_begin: Vec<(VertexId, u64)>,

    /// Final committed values of iterating-SCC members (spec §4 persistence).
    /// In canonical (value-cache disabled) mode the computed overlay is the
    /// ONLY home of a formula's value, and structural edits clear computed
    /// overlays wholesale (`clear_computed_overlay_after_row/_col`) —
    /// destroying iteration state (accumulators reset to 0; found by the
    /// iterate edge corpus). This snapshot lets the next SCC task re-seed
    /// members whose overlay entry vanished. Members registered for
    /// per-recalc redirty are refreshed by
    /// [`Self::redirty_for_next_recalc`]; retained members are written once
    /// when retained. Entries are dropped when the member's SCC task ends
    /// without iterating or when the vertex is deleted. Empty unless
    /// something iterated — zero cost otherwise.
    iterative_state_values: FxHashMap<VertexId, LiteralValue>,

    /// Global function-registry semantic epoch last observed.
    function_semantic_epoch_seen: u64,
    /// Runtime-provider semantic revision last observed.
    function_provider_revision_seen: Option<u64>,

    #[cfg(feature = "tracing")]
    trace_evaluation_counters: TraceEvaluationCounters,
    #[cfg(test)]
    evaluation_request_begin_count_for_test: u64,
    #[cfg(any(test, feature = "test-support"))]
    before_prepared_span_commit_hook: Option<Box<dyn FnOnce() + Send + Sync>>,
    #[cfg(test)]
    before_target_preparation_commit_hook: Option<Box<dyn FnOnce() + Send + Sync>>,
    #[cfg(test)]
    before_target_planning_snapshot_hook: Option<Box<dyn FnOnce() + Send + Sync>>,
    #[cfg(test)]
    inject_target_semantic_stale_once_for_test: bool,
    #[cfg(test)]
    force_virtual_dep_changes_remaining_for_test: usize,
    /// Dynamic-read freshness state (design §8.2).
    freshness: freshness::Freshness,
    #[cfg(test)]
    fail_evaluation_commit_preflight_once_for_test: bool,
    #[cfg(test)]
    target_preparation_fault_for_test:
        Option<crate::engine::target_preparation::TargetPreparationFault>,
    #[cfg(test)]
    force_non_cycle_schedule_fallback_for_test: bool,
    #[cfg(test)]
    before_legacy_fallback_final_provider_sample_hook: Option<Box<dyn FnOnce() + Send + Sync>>,
    #[cfg(test)]
    after_eager_proposal_commit_hook: Option<Box<dyn FnOnce() + Send + Sync>>,
}

/// This wrapper is intentionally thin for ticket 614 (commit-only): it delegates to existing
/// `Engine` edit methods and does not create changelog boundaries or implement rollback.
impl<R: EvaluationContext> Engine<R> {
    pub(crate) fn ingest_pipeline(&mut self) -> crate::engine::ingest_pipeline::IngestPipeline<'_> {
        self.graph.ingest_pipeline(&self.resolver)
    }
}

pub struct EngineAction<'a, R>
where
    R: EvaluationContext,
{
    engine: &'a mut Engine<R>,
    name: String,
    // Complete private mutation capture used by atomic actions.
    // Stored as a raw pointer to avoid creating aliasing `&mut` borrows alongside `&mut Engine`.
    capture: Option<*mut MutationCapture>,
    // Optional Arrow undo journal used by `Engine::action_atomic`.
    // Stored as a raw pointer to avoid aliasing issues with `&mut Engine`.
    arrow_undo: Option<*mut crate::engine::ArrowUndoBatch>,
    // True when this EngineAction must enforce conservative atomic transaction policy.
    atomic_policy: bool,
}

impl<'a, R> EngineAction<'a, R>
where
    R: EvaluationContext,
{
    #[inline]
    fn addr_for(&mut self, sheet: &str, row: u32, col: u32) -> crate::reference::CellRef {
        let sheet_id = self.engine.graph.sheet_id_mut(sheet);
        let coord = crate::reference::Coord::from_excel(row, col, true, true);
        crate::reference::CellRef::new(sheet_id, coord)
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), crate::engine::EditorError> {
        if self.capture.is_some() {
            let old_value = self.engine.read_cell_value(sheet, row, col);
            let mut old_formula = self.engine.read_cell_formula_ast(sheet, row, col);
            let addr = self.addr_for(sheet, row, col);
            let Some(capture_ptr) = self.capture else {
                return Err(crate::engine::EditorError::TransactionFailed {
                    reason: "action_with_logger: missing mutation capture".to_string(),
                });
            };

            // For atomic journal mode, record computed overlay effects for this cell.
            // Delta-overlay undo is recorded semantically based on old_value/old_formula.
            let old_comp = if self.arrow_undo.is_some() {
                self.engine.read_computed_overlay_cell(sheet, row, col)
            } else {
                None
            };

            if self.engine.graph_admission_enabled() {
                let admission =
                    self.engine
                        .graph
                        .preview_value_mutation(addr.sheet_id, row, col)?;
                self.engine.preflight_graph_admission(admission)?;
            }
            if old_formula.is_none() {
                old_formula = self.engine.read_cell_formula_ast(sheet, row, col);
            }

            let delta_old_sem = if old_formula.is_some() {
                None
            } else {
                Some(old_value.clone().unwrap_or(LiteralValue::Empty))
            };

            let start_len = unsafe { (&*capture_ptr).len() };

            // Safety: `capture_ptr` comes from a unique operation-local `&mut MutationCapture`.
            let capture = unsafe { &mut *capture_ptr };
            self.engine.edit_with_capture(capture, |editor| {
                editor.set_cell_value_with_old_state(
                    addr,
                    value.clone(),
                    old_value.clone(),
                    old_formula.clone(),
                );
            })?;
            self.engine.record_structural_change(StructuralScope::Cell {
                sheet: addr.sheet_id,
                row: addr.coord.row(),
                col: addr.coord.col(),
            });

            if let Some(undo_ptr) = self.arrow_undo {
                // 1) Spill snapshot operations (computed overlay rect restore).
                let new_events = &unsafe { (&*capture_ptr).events() }[start_len..];
                let undo = unsafe { &mut *undo_ptr };
                self.engine
                    .record_spill_ops_into_arrow_undo(undo, new_events);

                // 2) Delta/computed overlay single-cell deltas.
                let new_comp = self.engine.read_computed_overlay_cell(sheet, row, col);
                let sheet_id = self.engine.graph.sheet_id_mut(sheet);
                let row0 = row.saturating_sub(1);
                let col0 = col.saturating_sub(1);
                let delta_new_sem = Some(value.clone());
                undo.record_delta_cell(sheet_id, row0, col0, delta_old_sem, delta_new_sem);
                undo.record_computed_cell(sheet_id, row0, col0, old_comp, new_comp);
            }
            Ok(())
        } else {
            self.engine
                .set_cell_value(sheet, row, col, value)
                .map_err(crate::engine::EditorError::from)
        }
    }

    #[inline]
    pub fn set_cell_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
    ) -> Result<(), crate::engine::EditorError> {
        if self.capture.is_some() {
            let old_value = self.engine.read_cell_value(sheet, row, col);
            let mut old_formula = self.engine.read_cell_formula_ast(sheet, row, col);
            let addr = self.addr_for(sheet, row, col);
            let Some(capture_ptr) = self.capture else {
                return Err(crate::engine::EditorError::TransactionFailed {
                    reason: "action_with_logger: missing mutation capture".to_string(),
                });
            };

            let admitted_formula = if self.engine.graph_admission_enabled() {
                let placement =
                    CellRef::new(addr.sheet_id, Coord::from_excel(row, col, true, true));
                let ingested = self.engine.ingest_pipeline().ingest_formula(
                    FormulaAstInput::Tree(ast.clone()),
                    placement,
                    None,
                )?;
                let admission = self.engine.graph.preview_formula_mutations(&[(
                    addr.sheet_id,
                    row,
                    col,
                    ingested.dep_plan.clone(),
                )])?;
                self.engine.preflight_graph_admission(admission)?;
                Some((ingested.ast_id, ingested.dep_plan))
            } else {
                None
            };
            if old_formula.is_none() {
                old_formula = self.engine.read_cell_formula_ast(sheet, row, col);
            }
            let delta_old = if self.arrow_undo.is_some() {
                if old_formula.is_some() {
                    None
                } else {
                    Some(old_value.clone().unwrap_or(LiteralValue::Empty))
                }
            } else {
                None
            };
            let start_len = unsafe { (&*capture_ptr).len() };

            // Safety: `capture_ptr` comes from a unique operation-local `&mut MutationCapture`.
            let capture = unsafe { &mut *capture_ptr };
            self.engine.edit_with_capture(capture, |editor| {
                if let Some((ast_id, plan)) = admitted_formula {
                    editor.set_cell_formula_with_prepared_plan(
                        addr,
                        ast.clone(),
                        old_value,
                        old_formula,
                        ast_id,
                        plan,
                    );
                } else {
                    editor.set_cell_formula_with_old_state(
                        addr,
                        ast.clone(),
                        old_value,
                        old_formula,
                    );
                }
            })?;
            self.engine.record_structural_change(StructuralScope::Cell {
                sheet: addr.sheet_id,
                row: addr.coord.row(),
                col: addr.coord.col(),
            });

            if let Some(undo_ptr) = self.arrow_undo {
                let new_events = &unsafe { (&*capture_ptr).events() }[start_len..];
                let undo = unsafe { &mut *undo_ptr };
                self.engine
                    .record_spill_ops_into_arrow_undo(undo, new_events);
                let delta_new: Option<LiteralValue> = None;
                let sheet_id = self.engine.graph.sheet_id_mut(sheet);
                let row0 = row.saturating_sub(1);
                let col0 = col.saturating_sub(1);
                undo.record_delta_cell(sheet_id, row0, col0, delta_old, delta_new);
            }
            Ok(())
        } else {
            self.engine
                .set_cell_formula(sheet, row, col, ast)
                .map_err(crate::engine::EditorError::from)
        }
    }

    #[inline]
    pub fn set_row_hidden(
        &mut self,
        sheet: &str,
        row_1based: u32,
        hidden: bool,
        source: RowVisibilitySource,
    ) -> Result<(), crate::engine::EditorError> {
        if self.capture.is_some() {
            let sheet_id = self.engine.ensure_known_sheet_id(sheet)?;
            let row0 = Engine::<R>::normalize_row_1based(row_1based)?;
            let old_hidden = self
                .engine
                .row_visibility
                .get(&sheet_id)
                .map(|state| state.is_row_hidden(row0, Some(source)))
                .unwrap_or(false);
            if old_hidden == hidden {
                return Ok(());
            }

            let _ = self
                .engine
                .set_row_hidden_by_sheet_id(sheet_id, row0, hidden, source);

            let Some(capture_ptr) = self.capture else {
                return Err(crate::engine::EditorError::TransactionFailed {
                    reason: "action_with_logger: missing mutation capture".to_string(),
                });
            };
            unsafe { &mut *capture_ptr }.record(crate::engine::ChangeEvent::SetRowVisibility {
                sheet_id,
                row0,
                source,
                old_hidden,
                new_hidden: hidden,
            });

            Ok(())
        } else {
            self.engine
                .set_row_hidden(sheet, row_1based, hidden, source)
        }
    }

    #[inline]
    pub fn set_rows_hidden(
        &mut self,
        sheet: &str,
        start_row_1based: u32,
        end_row_1based: u32,
        hidden: bool,
        source: RowVisibilitySource,
    ) -> Result<(), crate::engine::EditorError> {
        if self.capture.is_some() {
            let sheet_id = self.engine.ensure_known_sheet_id(sheet)?;
            let (start_row0, end_row0) =
                Engine::<R>::normalize_row_range_1based(start_row_1based, end_row_1based)?;

            let Some(capture_ptr) = self.capture else {
                return Err(crate::engine::EditorError::TransactionFailed {
                    reason: "action_with_logger: missing mutation capture".to_string(),
                });
            };
            let capture = unsafe { &mut *capture_ptr };

            for row0 in start_row0..=end_row0 {
                let old_hidden = self
                    .engine
                    .row_visibility
                    .get(&sheet_id)
                    .map(|state| state.is_row_hidden(row0, Some(source)))
                    .unwrap_or(false);
                if old_hidden == hidden {
                    continue;
                }

                let _ = self
                    .engine
                    .set_row_hidden_by_sheet_id(sheet_id, row0, hidden, source);

                capture.record(crate::engine::ChangeEvent::SetRowVisibility {
                    sheet_id,
                    row0,
                    source,
                    old_hidden,
                    new_hidden: hidden,
                });
            }

            Ok(())
        } else {
            self.engine
                .set_rows_hidden(sheet, start_row_1based, end_row_1based, hidden, source)
        }
    }

    #[inline]
    pub fn insert_rows(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::ShiftSummary, crate::engine::EditorError> {
        if count == 0 {
            return Ok(crate::engine::ShiftSummary::default());
        }
        if self.capture.is_some() {
            let Some(capture_ptr) = self.capture else {
                return Err(crate::engine::EditorError::TransactionFailed {
                    reason: "action_atomic: missing mutation capture".to_string(),
                });
            };

            let sheet_id = self.engine.graph.sheet_id_mut(sheet);
            let before0 = before.saturating_sub(1);
            let occupancy = self.engine.structural_row_occupancy(sheet, sheet_id);
            let affected_region = Engine::<R>::structural_row_region(sheet_id, before0);

            // Graph structural insert (logged) - no snapshot bump.
            let summary = {
                let capture = unsafe { &mut *capture_ptr };
                let mut out: Result<crate::engine::ShiftSummary, crate::engine::EditorError> =
                    Ok(crate::engine::ShiftSummary::default());
                self.engine.edit_with_capture(capture, |editor| {
                    editor.set_structural_occupancy(occupancy);
                    out = editor.insert_rows(sheet_id, before0, count);
                })?;
                out?
            };

            // Arrow insert (truth) + undo op.
            self.engine.ensure_arrow_sheet(sheet);
            if let Some(asheet) = self.engine.arrow_sheets.sheet_mut(sheet) {
                asheet.insert_rows(before0 as usize, count as usize);
            }
            self.engine
                .purge_derived_formats_after_row(sheet_id, before0);
            self.engine
                .shift_row_visibility_insert(sheet_id, before0, count);
            self.engine.mark_moved_formula_vertices_dirty(&summary);
            self.engine
                .clear_computed_overlay_after_row(sheet, before0 as usize);
            self.engine
                .record_structural_change(StructuralScope::Region(affected_region));
            if let Some(undo_ptr) = self.arrow_undo {
                unsafe { &mut *undo_ptr }.record_insert_rows(sheet_id, before0, count);
            }
            Ok(summary)
        } else {
            self.engine.insert_rows(sheet, before, count)
        }
    }

    #[inline]
    pub fn delete_rows(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::ShiftSummary, crate::engine::EditorError> {
        if count == 0 {
            return Ok(crate::engine::ShiftSummary::default());
        }
        if self.atomic_policy {
            return Err(crate::engine::EditorError::TransactionUnsupported {
                reason:
                    "delete_rows is not supported inside atomic actions (conservative rollback policy)"
                        .to_string(),
            });
        }
        self.engine.delete_rows(sheet, start, count)
    }

    #[inline]
    pub fn insert_columns(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::ShiftSummary, crate::engine::EditorError> {
        if count == 0 {
            return Ok(crate::engine::ShiftSummary::default());
        }
        if self.capture.is_some() {
            let Some(capture_ptr) = self.capture else {
                return Err(crate::engine::EditorError::TransactionFailed {
                    reason: "action_atomic: missing mutation capture".to_string(),
                });
            };

            let sheet_id = self.engine.graph.sheet_id_mut(sheet);
            let before0 = before.saturating_sub(1);
            let occupancy = self.engine.structural_column_occupancy();
            let affected_region = Engine::<R>::structural_col_region(sheet_id, before0);

            let summary = {
                let capture = unsafe { &mut *capture_ptr };
                let mut out: Result<crate::engine::ShiftSummary, crate::engine::EditorError> =
                    Ok(crate::engine::ShiftSummary::default());
                self.engine.edit_with_capture(capture, |editor| {
                    editor.set_structural_occupancy(occupancy);
                    out = editor.insert_columns(sheet_id, before0, count);
                })?;
                out?
            };

            self.engine.ensure_arrow_sheet(sheet);
            if let Some(asheet) = self.engine.arrow_sheets.sheet_mut(sheet) {
                asheet.insert_columns(before0 as usize, count as usize);
            }
            self.engine
                .purge_derived_formats_after_col(sheet_id, before0);
            self.engine.mark_moved_formula_vertices_dirty(&summary);
            self.engine
                .clear_computed_overlay_after_col(sheet, before0 as usize);
            self.engine
                .record_structural_change(StructuralScope::Region(affected_region));
            if let Some(undo_ptr) = self.arrow_undo {
                unsafe { &mut *undo_ptr }.record_insert_cols(sheet_id, before0, count);
            }
            Ok(summary)
        } else {
            self.engine.insert_columns(sheet, before, count)
        }
    }

    #[inline]
    pub fn delete_columns(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::ShiftSummary, crate::engine::EditorError> {
        if count == 0 {
            return Ok(crate::engine::ShiftSummary::default());
        }
        if self.atomic_policy {
            return Err(crate::engine::EditorError::TransactionUnsupported {
                reason:
                    "delete_columns is not supported inside atomic actions (conservative rollback policy)"
                        .to_string(),
            });
        }
        self.engine.delete_columns(sheet, start, count)
    }

    /// Start an action from within an action.
    ///
    /// Nested actions are currently disallowed (ticket 614), so this will return a
    /// `EditorError::TransactionFailed` while an outer action is active.
    #[inline]
    pub fn action<T>(
        &mut self,
        name: impl AsRef<str>,
        f: impl FnOnce(&mut EngineAction<'_, R>) -> Result<T, crate::engine::EditorError>,
    ) -> Result<T, crate::engine::EditorError> {
        self.engine.action(name, f)
    }
}

struct ActionDepthGuard<'a, R> {
    engine: *mut Engine<R>,
    _marker: std::marker::PhantomData<&'a mut Engine<R>>,
}

impl<'a, R> Drop for ActionDepthGuard<'a, R> {
    fn drop(&mut self) {
        // Safety: the guard is created from a unique `&mut Engine` borrow and lives no longer
        // than the surrounding `Engine::action` call.
        unsafe {
            let e = &mut *self.engine;
            e.action_depth = e.action_depth.saturating_sub(1);
        }
    }
}

#[derive(Default)]
struct SourceCache {
    scalars: FxHashMap<(String, Option<u64>), LiteralValue>,
    tables: FxHashMap<(String, Option<u64>), Arc<dyn crate::traits::Table>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct VisibilityMaskCacheKey {
    sheet_id: SheetId,
    start_row0: u32,
    end_row0: u32,
    mode: VisibilityMaskMode,
    version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StructuralScope {
    Cell { sheet: SheetId, row: u32, col: u32 },
    Region(Region),
    Sheet(SheetId),
    RemovedSheet(SheetId),
    OpaqueGlobal,
    AllSheets,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum LoggedEditImpact {
    NoOp,
    DataOnly,
    Topology,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoggedEditDirection {
    Original,
    InverseReplay,
    ForwardReplay,
}

#[derive(Clone, Copy)]
struct InvalidationBaseline {
    snapshot_id: u64,
    topology_epoch: u64,
}

struct SourceCacheSession {
    cache: Arc<std::sync::RwLock<SourceCache>>,
}

impl Drop for SourceCacheSession {
    fn drop(&mut self) {
        if let Ok(mut g) = self.cache.write() {
            *g = SourceCache::default();
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct EvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: std::time::Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableMetadata {
    pub name: String,
    pub sheet: String,
    pub start_row: u32,
    pub start_col: u32,
    pub end_row: u32,
    pub end_col: u32,
    pub header_row: bool,
    pub headers: Vec<String>,
    pub totals_row: bool,
}

/// Read-only engine counters used by benchmark/instrumentation tooling.
///
/// These counters are deliberately observational: collecting them must not mutate engine state or
/// alter formula evaluation semantics.
///
/// The `formula_plane_*` counters are always `0`: FormulaPlane spans were removed and the
/// dependency authority is the only runtime path. They are kept for source compatibility.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineBaselineStats {
    pub graph_vertex_count: usize,
    pub graph_formula_vertex_count: usize,
    pub graph_edge_count: usize,
    pub dirty_vertex_count: usize,
    pub evaluation_vertex_count: usize,
    pub formula_ast_root_count: usize,
    pub formula_ast_node_count: usize,
    pub staged_formula_count: usize,
    pub formula_plane_active_span_count: usize,
    pub formula_plane_producer_result_entries: usize,
    pub formula_plane_consumer_read_entries: usize,
    pub formula_plane_mixed_topology_cache_builds: u64,
    pub formula_plane_mixed_topology_cache_hits: u64,
    pub formula_plane_mixed_topology_cache_overflows: u64,
    pub formula_plane_dirty_pending_events: usize,
    pub formula_plane_dirty_region_events_recorded: u64,
    pub formula_plane_dirty_span_region_events_recorded: u64,
    pub formula_plane_dirty_whole_span_seeds_recorded: u64,
    pub formula_plane_dirty_global_invalidations: u64,
    pub formula_plane_structural_span_candidates: u64,
    pub formula_plane_cycle_member_span_demotions: u64,
    pub formula_plane_array_result_span_demotions: u64,
    /// Members of exactly converged iterative SCCs currently retained across
    /// recalcs (#368).
    pub retained_scc_members: usize,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct VirtualDepTelemetry {
    pub candidate_vertices_total: usize,
    pub vdeps_vertices_total: usize,
    pub vdeps_edges_total: usize,
    pub builder_elapsed_ms_total: u128,
    pub schedule_virtual_passes: usize,
    pub schedule_static_passes: usize,
    pub schedule_cache_hits: usize,
    pub schedule_cache_misses: usize,
    pub reused_schedule_vertices_total: usize,
    pub replan_iterations: usize,
    pub changed_vdeps_total: usize,
    pub bailout_reason: Option<&'static str>,
    pub fallback_mode_activations: u64,
}

/// Per-recalc telemetry for SCC evaluation under `CycleDetection::Runtime`
/// (spec `formualizer-cycle-semantics-spec.md` §10).
///
/// Collection is unconditional: SCC tasks are rare relative to ordinary
/// vertex evaluation and the counters are a handful of integer adds per
/// task, so no config flag gates them (unlike [`VirtualDepTelemetry`],
/// which pays per-schedule costs). Counters reset at the start of every
/// evaluation request.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct CycleTelemetry {
    /// SCC tasks executed (static SCCs that reached Runtime evaluation).
    pub static_sccs: usize,
    /// SCC tasks whose live subgraph was acyclic — values produced.
    pub phantom_sccs: usize,
    /// Distinct live cycles witnessed across all SCC tasks.
    pub live_cycles_witnessed: usize,
    /// Cells stamped `#CIRC!` by Runtime SCC tasks.
    pub circ_cells_stamped: usize,
    /// Evaluation sweeps over (subsets of) SCC members, totalled across tasks
    /// (pass 1 included).
    pub settle_passes_total: usize,
    /// Largest pass count any single SCC task needed.
    pub max_passes_single_scc: usize,
    /// SCC tasks that entered iterative calculation (`CyclePolicy::Iterate`
    /// with a witnessed live cycle). RFC #113, Stage 3.
    pub iterated_sccs: usize,
    /// Iterating SCC tasks that stopped because every member passed the
    /// spec-§6 convergence test.
    pub converged_sccs: usize,
    /// SCC tasks that stopped at a pass cap. Under `CyclePolicy::Iterate`
    /// this is the Excel `max_iterations` cap (NOT an error — last values
    /// are kept; includes the no-convergence-test `max_iterations: 1`
    /// contract). Under `CyclePolicy::Error` it is the defensive acyclic
    /// settle cap (|SCC| + 2), which only a bug can hit.
    pub capped_sccs: usize,
    /// Largest `|Δ|` observed in any member's final-pass convergence
    /// comparison across iterating SCC tasks (numeric-class members only).
    /// `0.0` when no comparison ran (e.g. `max_iterations: 1`).
    pub max_abs_delta_at_stop: f64,
    /// Identical-bit NaN vs NaN member comparisons that were treated as
    /// converged (spec §6 NaN rule).
    pub nan_converged: usize,
    /// Retained iterative SCCs (#368) that had no dirty member at request begin and were therefore not
    /// re-run: their last exact fixed point is served as-is. Counted at
    /// request begin, so a demand-driven request that never reaches a
    /// retained SCC still reports it as reused.
    pub reused_sccs: usize,
    /// Members of the SCCs counted in `reused_sccs`.
    pub reused_scc_members: usize,
    /// Total wall-clock time spent inside Runtime SCC tasks.
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Copy)]
struct ScheduleBuildMeta {
    candidate_vertices: usize,
    vdeps_vertices: usize,
    vdeps_edges: usize,
    builder_elapsed_ms: u128,
    used_virtual_schedule: bool,
    schedule_cache_hit: bool,
    schedule_cache_eligible: bool,
}

#[cfg(any(test, feature = "benchmark_internal"))]
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct RecalcReuseProbe {
    pub schedule_requests: usize,
    pub schedule_cache_hits: usize,
    pub schedule_cache_misses: usize,
    pub schedule_cache_ineligible: usize,
    pub schedule_builds: usize,
    /// Program 3 plan reuse: misses served by restricting the base schedule.
    pub schedule_base_restrictions: usize,
    pub schedule_shared_handles: usize,
    pub schedule_retained_bytes: usize,
    pub legacy_target_requests: usize,
    pub target_schedule_builds: usize,
    pub demand_builds: usize,
    pub demand_vertices: usize,
    pub demand_clean_formulas: usize,
    pub demand_explicit_edges: usize,
    pub demand_virtual_builder_calls: usize,
}

#[cfg(any(test, feature = "benchmark_internal"))]
fn schedule_probe_retained_bytes(schedule: &crate::engine::Schedule) -> usize {
    fn vector_bytes<T>(values: &Vec<T>) -> usize {
        values.capacity() * std::mem::size_of::<T>()
    }

    [
        vector_bytes(&schedule.units),
        vector_bytes(&schedule.layers),
        vector_bytes(&schedule.cycles),
    ]
    .into_iter()
    .chain(
        schedule
            .layers
            .iter()
            .map(|layer| vector_bytes(&layer.vertices)),
    )
    .chain(schedule.cycles.iter().map(vector_bytes))
    .sum()
}

#[derive(Debug, Clone)]
struct CachedScheduleEntry {
    topology_epoch: u64,
    /// Authority `(store revision, rev.dyn)` the schedule was planned from
    /// (design §8.4; always 0 without `unified_authority`).
    authority_revision: (u64, u64),
    /// The request's vertex list as runs of consecutive ids (formula ids
    /// come in column runs, so a whole-workbook request is a few runs).
    candidate_vertices: VertexIdRuns,
    schedule: Arc<crate::engine::scheduler::Schedule>,
}

/// A vertex list stored as `(first id, run length)` runs of consecutive
/// ids, in list order.
#[derive(Debug, Clone, Default)]
struct VertexIdRuns(Vec<(u32, u32)>);

impl VertexIdRuns {
    fn from_slice(ids: &[VertexId]) -> Self {
        let mut runs: Vec<(u32, u32)> = Vec::new();
        for v in ids {
            match runs.last_mut() {
                Some((first, len)) if first.checked_add(*len) == Some(v.0) => *len += 1,
                _ => runs.push((v.0, 1)),
            }
        }
        runs.shrink_to_fit();
        Self(runs)
    }

    fn equals(&self, ids: &[VertexId]) -> bool {
        let mut rest = ids;
        for &(first, len) in &self.0 {
            let len = len as usize;
            if rest.len() < len {
                return false;
            }
            let (head, tail) = rest.split_at(len);
            if head
                .iter()
                .enumerate()
                .any(|(i, v)| v.0 != first.wrapping_add(i as u32))
            {
                return false;
            }
            rest = tail;
        }
        rest.is_empty()
    }

    fn heap_bytes(&self) -> usize {
        self.0.capacity() * std::mem::size_of::<(u32, u32)>()
    }

    /// The number of ids.
    fn len(&self) -> usize {
        self.0.iter().map(|&(_, len)| len as usize).sum()
    }
}

#[cfg(test)]
mod vertex_id_runs_tests {
    use super::{VertexId, VertexIdRuns};

    #[test]
    fn runs_compare_like_the_list() {
        let ids = |v: &[u32]| v.iter().map(|&i| VertexId(i)).collect::<Vec<_>>();
        let list = ids(&[5, 6, 7, 2, 3, 9, 10, 10]);
        let runs = VertexIdRuns::from_slice(&list);
        assert_eq!(runs.0, vec![(5, 3), (2, 2), (9, 2), (10, 1)]);
        assert!(runs.equals(&list));
        assert!(!runs.equals(&list[..7]));
        assert!(!runs.equals(&ids(&[5, 6, 7, 2, 3, 9, 10, 11])));
        assert!(!runs.equals(&ids(&[5, 6, 7, 2, 3, 9, 10, 10, 11])));
        assert!(VertexIdRuns::from_slice(&[]).equals(&[]));
        assert!(!VertexIdRuns::from_slice(&[]).equals(&list));
        let edge = ids(&[u32::MAX - 1, u32::MAX, 0]);
        assert!(VertexIdRuns::from_slice(&edge).equals(&edge));
    }
}

/// Uncacheable requests keep their schedule inline without a shared allocation.
enum EvaluationSchedule {
    Owned(crate::engine::scheduler::Schedule),
    Shared(Arc<crate::engine::scheduler::Schedule>),
}

impl std::ops::Deref for EvaluationSchedule {
    type Target = crate::engine::scheduler::Schedule;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(schedule) => schedule,
            Self::Shared(schedule) => schedule,
        }
    }
}

type ScheduleBuildOutput = (
    crate::engine::scheduler::Schedule,
    FxHashMap<VertexId, Vec<VertexId>>,
    ScheduleBuildMeta,
);

type EvaluationScheduleBuildOutput = (
    EvaluationSchedule,
    FxHashMap<VertexId, Vec<VertexId>>,
    ScheduleBuildMeta,
);

/// Opaque, revision-bound recalculation recipe.
#[derive(Debug)]
pub struct RecalcPlan {
    key: RecalcPlanKey,
    kind: RecalcPlanKind,
}

#[derive(Debug)]
struct RecalcPlanKey {
    engine_token: Arc<()>,
    revisions: PlanningRevisionSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlanningRevisionSnapshot {
    engine_topology_epoch: u64,
    graph_topology_revision: u64,
    staged: u64,
    symbols: u64,
    semantic: u64,
    provider: Option<u64>,
    deterministic_mode: crate::engine::DeterministicMode,
    budgets: crate::engine::EvaluationBudgets,
}

#[derive(Debug)]
enum RecalcPlanKind {
    CompatibilityFull {
        schedule: crate::engine::Schedule,
        has_dynamic_refs: bool,
    },
    Target {
        targets: Vec<crate::engine::EvaluationTarget>,
        scope: crate::engine::PrepareScope,
        topology: RecalcTopology,
        dynamic_policy: DynamicPlanPolicy,
    },
}

#[derive(Debug)]
enum RecalcTopology {
    RunLocalRecipe,
    Workbook,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DynamicPlanPolicy {
    BoundedTargetReplan,
}

impl RecalcPlan {
    /// Returns the retained compatibility schedule depth. Target plans retain a
    /// run-local recipe rather than a schedule, so their layer count is zero.
    pub fn layer_count(&self) -> usize {
        match &self.kind {
            RecalcPlanKind::CompatibilityFull { schedule, .. } => schedule.layers.len(),
            RecalcPlanKind::Target { .. } => 0,
        }
    }

    pub fn has_dynamic_refs(&self) -> bool {
        match &self.kind {
            RecalcPlanKind::CompatibilityFull {
                has_dynamic_refs, ..
            } => *has_dynamic_refs,
            RecalcPlanKind::Target { .. } => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn force_stale_reasons_for_test(
        &mut self,
        reasons: &[formualizer_common::PlanStaleReason],
    ) {
        use formualizer_common::PlanStaleReason;
        for reason in reasons {
            match reason {
                PlanStaleReason::Engine => {
                    self.key.engine_token = Arc::new(());
                }
                PlanStaleReason::Provider => {
                    self.key.revisions.provider = Some(
                        self.key
                            .revisions
                            .provider
                            .unwrap_or_default()
                            .wrapping_add(1),
                    );
                }
                PlanStaleReason::Semantic => {
                    self.key.revisions.semantic = self.key.revisions.semantic.wrapping_add(1);
                }
                PlanStaleReason::Budget => {
                    let current = self.key.revisions.budgets.work.max_work_units;
                    self.key.revisions.budgets.work.max_work_units =
                        Some(current.unwrap_or_default().wrapping_add(1));
                }
                PlanStaleReason::Staged => {
                    self.key.revisions.staged = self.key.revisions.staged.wrapping_add(1);
                }
                PlanStaleReason::Symbols => {
                    self.key.revisions.symbols = self.key.revisions.symbols.wrapping_add(1);
                }
                PlanStaleReason::Graph => {
                    self.key.revisions.graph_topology_revision =
                        self.key.revisions.graph_topology_revision.wrapping_add(1);
                }
                _ => {}
            }
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod criteria_mask_test_hooks {
    use std::cell::Cell;

    thread_local! {
        static MASK_CALLS_ROWS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
        static TEXT_SEGMENTS_TOTAL: Cell<usize> = const { Cell::new(0) };
        static TEXT_SEGMENTS_ALL_NULL: Cell<usize> = const { Cell::new(0) };
    }

    pub(crate) fn take_mask_work() -> (usize, usize) {
        MASK_CALLS_ROWS.with(|c| c.replace((0, 0)))
    }

    pub(crate) fn note_mask(rows: usize) {
        MASK_CALLS_ROWS.with(|c| {
            let (calls, work) = c.get();
            c.set((calls + 1, work + rows));
        });
    }

    pub fn reset_text_segment_counters() {
        TEXT_SEGMENTS_TOTAL.with(|c| c.set(0));
        TEXT_SEGMENTS_ALL_NULL.with(|c| c.set(0));
    }

    pub fn text_segment_counters() -> (usize, usize) {
        let a = TEXT_SEGMENTS_TOTAL.with(|c| c.get());
        let b = TEXT_SEGMENTS_ALL_NULL.with(|c| c.get());
        (a, b)
    }

    pub(crate) fn inc_total() {
        TEXT_SEGMENTS_TOTAL.with(|c| c.set(c.get() + 1));
    }
    pub(crate) fn inc_all_null() {
        TEXT_SEGMENTS_ALL_NULL.with(|c| c.set(c.get() + 1));
    }
}

#[cfg(test)]
pub(crate) mod visibility_mask_test_hooks {
    use std::cell::Cell;

    thread_local! {
        static HITS: Cell<usize> = const { Cell::new(0) };
        static MISSES: Cell<usize> = const { Cell::new(0) };
        static EVICTIONS: Cell<usize> = const { Cell::new(0) };
    }

    pub fn reset() {
        HITS.with(|c| c.set(0));
        MISSES.with(|c| c.set(0));
        EVICTIONS.with(|c| c.set(0));
    }

    pub fn counters() -> (usize, usize, usize) {
        let hits = HITS.with(|c| c.get());
        let misses = MISSES.with(|c| c.get());
        let evictions = EVICTIONS.with(|c| c.get());
        (hits, misses, evictions)
    }

    pub(crate) fn inc_hit() {
        HITS.with(|c| c.set(c.get() + 1));
    }

    pub(crate) fn inc_miss() {
        MISSES.with(|c| c.set(c.get() + 1));
    }

    pub(crate) fn inc_eviction() {
        EVICTIONS.with(|c| c.set(c.get() + 1));
    }
}

fn is_numeric_text_equality(pred: &crate::args::CriteriaPredicate) -> bool {
    match pred {
        crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Text(text)) => {
            text.trim().parse::<f64>().is_ok_and(f64::is_finite)
        }
        _ => false,
    }
}

fn compute_criteria_mask(
    view: &RangeView<'_>,
    col_in_view: usize,
    pred: &crate::args::CriteriaPredicate,
) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
    use crate::compute_prelude::{boolean, cmp, concat_arrays};
    use arrow::compute::kernels::comparison::{ilike, nilike};
    use arrow_array::{
        Array as _, ArrayRef, BooleanArray, Float64Array, StringArray, builder::BooleanBuilder,
    };

    // Helper: apply a numeric predicate to a single Float64Array chunk
    fn apply_numeric_pred(
        chunk: &Float64Array,
        pred: &crate::args::CriteriaPredicate,
    ) -> Option<BooleanArray> {
        match pred {
            crate::args::CriteriaPredicate::Gt(n) => {
                cmp::gt(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Ge(n) => {
                cmp::gt_eq(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Lt(n) => {
                cmp::lt(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Le(n) => {
                cmp::lt_eq(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Eq(v) => match v {
                formualizer_common::LiteralValue::Number(x) => {
                    cmp::eq(chunk, &Float64Array::new_scalar(*x)).ok()
                }
                formualizer_common::LiteralValue::Int(i) => {
                    cmp::eq(chunk, &Float64Array::new_scalar(*i as f64)).ok()
                }
                _ => None,
            },
            crate::args::CriteriaPredicate::Ne(v) => match v {
                formualizer_common::LiteralValue::Number(x) => {
                    cmp::neq(chunk, &Float64Array::new_scalar(*x)).ok()
                }
                formualizer_common::LiteralValue::Int(i) => {
                    cmp::neq(chunk, &Float64Array::new_scalar(*i as f64)).ok()
                }
                _ => None,
            },
            _ => None,
        }
    }

    // Check if this is a numeric predicate that can be applied per-chunk
    let is_numeric_pred = matches!(
        pred,
        crate::args::CriteriaPredicate::Gt(_)
            | crate::args::CriteriaPredicate::Ge(_)
            | crate::args::CriteriaPredicate::Lt(_)
            | crate::args::CriteriaPredicate::Le(_)
            | crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Number(_))
            | crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Int(_))
            | crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Number(_))
            | crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Int(_))
    );

    // OPTIMIZED PATH: For numeric predicates, apply per-chunk and concatenate boolean masks.
    // This avoids materializing the full numeric column (64-bit per element) and instead
    // concatenates boolean masks (1-bit per element) - a 64x memory reduction.
    if is_numeric_pred {
        let mut bool_parts: Vec<BooleanArray> = Vec::new();
        for res in view.numbers_slices() {
            let (_rs, _rl, cols_seg) = res.ok()?;
            if col_in_view < cols_seg.len() {
                let chunk = cols_seg[col_in_view].as_ref();
                let mask = apply_numeric_pred(chunk, pred)?;
                bool_parts.push(mask);
            }
        }

        if bool_parts.is_empty() {
            return None;
        } else if bool_parts.len() == 1 {
            return Some(std::sync::Arc::new(bool_parts.remove(0)));
        } else {
            // Concatenate boolean masks (much cheaper than concatenating Float64 arrays)
            let anys: Vec<&dyn arrow_array::Array> = bool_parts
                .iter()
                .map(|a| a as &dyn arrow_array::Array)
                .collect();
            let conc: ArrayRef = concat_arrays(&anys).ok()?;
            let ba = conc.as_any().downcast_ref::<BooleanArray>()?.clone();
            return Some(std::sync::Arc::new(ba));
        }
    }

    // Wildcards and numeric text equality can match non-text cells. The lowered
    // base lane is text-only, unlike the scalar matcher. Keep the vectorized
    // path for text-only data, but cache a scalar-equivalent mask for mixed data.
    if is_numeric_text_equality(pred)
        || matches!(pred, crate::args::CriteriaPredicate::TextLike { .. })
    {
        for tags in view.type_tags_slices() {
            let (_, _, cols) = tags.ok()?;
            let tags = cols.get(col_in_view)?;
            if tags.values().iter().any(|tag| {
                *tag == crate::arrow_store::TypeTag::Empty as u8
                    || *tag == crate::arrow_store::TypeTag::Number as u8
                    || *tag == crate::arrow_store::TypeTag::Boolean as u8
            }) {
                let mut mask = BooleanBuilder::new();
                for chunk in view.iter_row_chunks() {
                    let chunk = chunk.ok()?;
                    for row in chunk.row_start..chunk.row_start + chunk.row_len {
                        mask.append_value(crate::builtins::criteria_match(
                            pred,
                            &view.get_cell(row, col_in_view),
                        ));
                    }
                }
                return Some(std::sync::Arc::new(mask.finish()));
            }
        }
    }

    // SQL LIKE cannot directly represent spreadsheet tilde escapes or literal
    // SQL pattern punctuation. Let the bounded chunk fallback use the shared
    // spreadsheet matcher rather than rewriting these patterns into SQL syntax.
    if matches!(pred, crate::args::CriteriaPredicate::TextLike { pattern, .. }
        if pattern.contains(['~', '%', '_', '\\']))
    {
        return None;
    }

    // TEXT PATH: build masks per row-chunk using lowered text slices.
    // This avoids concatenating full-string columns just to compute a boolean mask.
    let (text_kind, text_pat, empty_special) = match pred {
        crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Text(t)) => {
            (0u8, t.to_lowercase(), t.is_empty())
        }
        crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Text(t)) => {
            (1u8, t.to_lowercase(), false)
        }
        crate::args::CriteriaPredicate::TextLike {
            pattern,
            case_insensitive,
        } => {
            let p = if *case_insensitive {
                pattern.to_lowercase()
            } else {
                pattern.clone()
            };
            (2u8, p.replace('*', "%").replace('?', "_"), false)
        }
        _ => return None,
    };

    let text_pat_is_empty = text_pat.is_empty();
    let ne_matches_blank = text_kind == 1 && !text_pat_is_empty;
    let pat = StringArray::new_scalar(text_pat);
    let mut bool_parts: Vec<BooleanArray> = Vec::new();

    let mut tag_slices = view.type_tags_slices();
    for res in view.iter_row_chunks() {
        let cs = res.ok()?;
        if cs.row_len == 0 {
            continue;
        }
        #[cfg(test)]
        criteria_mask_test_hooks::inc_total();

        let slices = view.slice_lowered_text(cs.row_start, cs.row_len);
        if col_in_view >= slices.len() {
            return None;
        }

        let seg_opt = slices[col_in_view].as_ref().map(|a| a.as_ref());
        if empty_special || (text_kind == 1 && text_pat_is_empty) {
            let (tag_start, tag_len, tags) = tag_slices.next()?.ok()?;
            if tag_start != cs.row_start || tag_len != cs.row_len {
                return None;
            }
            let tags = tags.get(col_in_view)?;
            // A null text lane is not a blank cell: base numeric/boolean/error
            // cells also have null text. Consult the overlay-aware type tags,
            // and inspect strings only to distinguish empty text from text.
            let strings = seg_opt.and_then(|a| a.as_any().downcast_ref::<StringArray>());
            let mut bb = BooleanBuilder::with_capacity(cs.row_len);
            for i in 0..cs.row_len {
                let blank = tags.value(i) == crate::arrow_store::TypeTag::Empty as u8
                    || (tags.value(i) == crate::arrow_store::TypeTag::Text as u8
                        && strings.is_some_and(|s| s.is_valid(i) && s.value(i).is_empty()));
                bb.append_value(if text_kind == 0 { blank } else { !blank });
            }
            #[cfg(test)]
            if seg_opt.is_none() {
                criteria_mask_test_hooks::inc_all_null();
            }
            bool_parts.push(bb.finish());
            continue;
        }
        let seg = match seg_opt {
            Some(s) => s,
            None => {
                #[cfg(test)]
                criteria_mask_test_hooks::inc_all_null();
                if (text_kind == 0 && empty_special) || ne_matches_blank {
                    // Eq("") treats nulls (Empty) as equal.
                    let mut bb = BooleanBuilder::with_capacity(cs.row_len);
                    bb.append_n(cs.row_len, true);
                    bool_parts.push(bb.finish());
                } else {
                    // For non-empty patterns, ilike/nilike return null on null inputs.
                    bool_parts.push(BooleanArray::new_null(cs.row_len));
                }
                continue;
            }
        };

        let seg_sa = seg.as_any().downcast_ref::<StringArray>()?;
        let mut m = match text_kind {
            0 => ilike(seg_sa, &pat).ok()?,
            1 => nilike(seg_sa, &pat).ok()?,
            2 => ilike(seg_sa, &pat).ok()?,
            _ => return None,
        };

        // Only fold blank/Empty (null) cells into the mask when the segment
        // actually contains any. The null-fill loop + or_kleene are pure
        // overhead on blank-free chunks, so a `<>text` (or `=""`) aggregation
        // over a column with no blanks stays fully vectorized on the ilike/
        // nilike result.
        if ((text_kind == 0 && empty_special) || ne_matches_blank) && seg_sa.null_count() > 0 {
            // Treat nulls as equal to empty string
            let mut bb = BooleanBuilder::with_capacity(seg_sa.len());
            for i in 0..seg_sa.len() {
                bb.append_value(seg_sa.is_null(i));
            }
            let nulls = bb.finish();
            m = boolean::or_kleene(&m, &nulls).ok()?;
        }

        bool_parts.push(m);
    }

    if bool_parts.is_empty() {
        None
    } else if bool_parts.len() == 1 {
        Some(std::sync::Arc::new(bool_parts.remove(0)))
    } else {
        let anys: Vec<&dyn arrow_array::Array> = bool_parts
            .iter()
            .map(|a| a as &dyn arrow_array::Array)
            .collect();
        let conc: ArrayRef = concat_arrays(&anys).ok()?;
        let ba = conc.as_any().downcast_ref::<BooleanArray>()?.clone();
        Some(std::sync::Arc::new(ba))
    }
}

#[derive(Debug, Clone)]
pub struct LayerInfo {
    pub vertex_count: usize,
    pub parallel_eligible: bool,
    pub sample_cells: Vec<String>, // Sample of up to 5 cell addresses
}

#[derive(Debug, Clone)]
pub struct EvalPlan {
    pub total_vertices_to_evaluate: usize,
    pub layers: Vec<LayerInfo>,
    pub cycles_detected: usize,
    pub dirty_count: usize,
    pub volatile_count: usize,
    pub parallel_enabled: bool,
    pub estimated_parallel_layers: usize,
    pub target_cells: Vec<String>,
}

/// Whether the configured FormulaPlane mode is ignored (spans never placed):
/// always, since the dependency authority is the runtime path (design §10).
#[inline]
fn plane_mode_ignored() -> bool {
    true
}

/// Test-support probe for sibling-crate tests: true when span placement is
/// off because the FormulaPlane mode is ignored (see `plane_mode_ignored`).
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn formula_plane_mode_ignored_for_test() -> bool {
    plane_mode_ignored()
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// # Panics
    /// Panics when `config.cycle` is invalid ([`CycleConfig::validate`],
    /// spec §2): `Iterate` with `detection: Static`, `max_iterations == 0`,
    /// or a negative/non-finite `max_change`. `EvalConfig::with_cycle`
    /// rejects these at build; this re-validates configs assembled via
    /// struct literals.
    pub fn new(resolver: R, config: EvalConfig) -> Self {
        // Under the unified authority the FormulaPlane mode is accepted and
        // ignored (design §10). Normalizing the stored mode keeps external
        // readers of `config` (loaders choosing a span-preparation route) on
        // the per-cell path; engine reads go through `formula_plane_mode()`.
        let config = if plane_mode_ignored() {
            EvalConfig {
                formula_plane_mode: FormulaPlaneMode::Off,
                ..config
            }
        } else {
            config
        };
        if let Err(msg) = config.cycle.validate() {
            panic!("invalid CycleConfig: {msg}");
        }
        crate::builtins::load_builtins();
        let resolved_resources = crate::engine::resource_ledger::resolve_evaluation_budgets(
            &config.evaluation_budgets,
            config.max_vertices,
            config.max_memory_mb,
            config.max_eval_time,
        );

        let clock = config.deterministic_mode.build_clock().unwrap_or_else(|_| {
            #[cfg(feature = "system-clock")]
            {
                Arc::new(crate::timezone::SystemClock::new(
                    crate::timezone::TimeZoneSpec::default(),
                ))
            }
            #[cfg(not(feature = "system-clock"))]
            {
                Arc::new(crate::timezone::FixedClock::new(
                    chrono::DateTime::UNIX_EPOCH,
                    crate::timezone::TimeZoneSpec::Utc,
                ))
            }
        });

        // Initialize thread pool based on config
        let thread_pool = if config.enable_parallel {
            let mut builder = ThreadPoolBuilder::new();
            if let Some(max_threads) = config.max_threads {
                builder = builder.num_threads(max_threads);
            }

            match builder.build() {
                Ok(pool) => Some(Arc::new(pool)),
                Err(_) => {
                    // Fall back to sequential evaluation if thread pool creation fails
                    None
                }
            }
        } else {
            None
        };

        // C1a retained/cache budgets are observational; cache defaults stay explicit.
        let lookup_cache_max_bytes = config.lookup_index_cache_max_bytes;
        let function_provider_revision_seen = resolver.planning_semantic_revision();
        let mut engine = Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            workbook_load_limits: crate::engine::WorkbookLoadLimits::default(),
            clock: crate::timezone::SnapshotClock::new(clock),
            thread_pool,
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            topology_epoch: 0,
            cached_static_schedule: None,
            recent_schedules: Vec::new(),
            base_schedule: None,
            #[cfg(any(test, feature = "benchmark_internal"))]
            recalc_reuse_probe: std::sync::Mutex::new(RecalcReuseProbe::default()),
            spill_mgr: ShimSpillManager::default(),
            arrow_sheets: SheetStore::default(),
            format_registry: crate::format::FormatRegistry::default(),
            derived_formats: Default::default(),
            #[cfg(test)]
            derived_format_operations_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            family_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            invariant_bound_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            lifted_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            chained_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            lane_clean_reads_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            criteria_kernel_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            memo_hits_for_test: std::sync::atomic::AtomicU64::new(0),
            compressed_at_build: None,
            #[cfg(test)]
            computed_overlay_set_explicit_entry_operations_for_test: 0,
            #[cfg(test)]
            computed_overlay_stale_clear_range_effects_for_test: 0,
            #[cfg(test)]
            computed_overlay_stale_clear_offset_attempts_for_test: 0,
            #[cfg(test)]
            computed_format_vector_allocations_for_test: std::sync::atomic::AtomicU64::new(0),
            has_edited: false,
            overlay_compactions: 0,
            computed_overlay_bytes_estimate: 0,
            computed_overlay_mirroring_disabled: false,
            force_materialize_range_views: false,
            row_bounds_cache: std::sync::RwLock::new(None),
            used_axis_bounds_cache: std::sync::RwLock::new(None),
            lookup_index_cache: LookupIndexCache::new(lookup_cache_max_bytes),
            source_cache: Arc::new(std::sync::RwLock::new(SourceCache::default())),
            source_formula_token: Arc::new(()),
            recalc_plan_token: Arc::new(()),
            staged_formulas: std::collections::HashMap::new(),
            staged_formula_index: StagedFormulaIndex::default(),
            blocked_pending_spills: Vec::new(),
            row_visibility: FxHashMap::default(),
            row_visibility_mask_cache: std::sync::RwLock::new(FxHashMap::default()),
            formula_parse_diagnostics: Vec::new(),
            last_formula_ingest_report: None,
            formula_ingest_report_total: FormulaIngestReport::default(),
            active_cancel_flag: None,
            active_evaluation_deadline: None,
            action_depth: 0,
            last_virtual_dep_telemetry: VirtualDepTelemetry::default(),
            virtual_dep_fallback_activations: 0,
            last_cycle_telemetry: CycleTelemetry::default(),
            next_evaluation_resource_request_id: 1,
            evaluation_resource_request_depth: 0,
            active_evaluation_resource_request: None,
            last_evaluation_resource_request: None,
            evaluation_resource_baseline: EvaluationResourceBaselineStats::default(),
            evaluation_resource_request_started_at: None,
            evaluation_resource_budgets: resolved_resources.budgets,
            evaluation_resource_config_diagnostic: resolved_resources.diagnostic,
            active_resource_ledger: None,
            source_cache_footprints: Vec::new(),
            source_cache_accounted: 0,
            pending_iterative_redirty: Vec::new(),
            retained_scc_members: FxHashMap::default(),
            next_retained_scc_id: 0,
            retained_scc_config_fingerprint: 0,
            retained_scc_function_epoch_seen: 0,
            retained_scc_provider_revision_seen: None,
            retained_scc_dirty_at_begin: Vec::new(),
            iterative_state_values: FxHashMap::default(),
            function_semantic_epoch_seen: crate::function_registry::semantic_epoch(),
            function_provider_revision_seen,
            #[cfg(feature = "tracing")]
            trace_evaluation_counters: TraceEvaluationCounters::default(),
            #[cfg(test)]
            evaluation_request_begin_count_for_test: 0,
            #[cfg(any(test, feature = "test-support"))]
            before_prepared_span_commit_hook: None,
            #[cfg(test)]
            before_target_preparation_commit_hook: None,
            #[cfg(test)]
            before_target_planning_snapshot_hook: None,
            #[cfg(test)]
            inject_target_semantic_stale_once_for_test: false,
            #[cfg(test)]
            force_virtual_dep_changes_remaining_for_test: 0,
            freshness: Default::default(),
            #[cfg(test)]
            fail_evaluation_commit_preflight_once_for_test: false,
            #[cfg(test)]
            target_preparation_fault_for_test: None,
            #[cfg(test)]
            force_non_cycle_schedule_fallback_for_test: false,
            #[cfg(test)]
            before_legacy_fallback_final_provider_sample_hook: None,
            #[cfg(test)]
            after_eager_proposal_commit_hook: None,
        };
        // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
        engine.config.arrow_storage_enabled = true;
        engine.config.delta_overlay_enabled = true;
        engine.config.write_formula_overlay_enabled = true;
        let default_sheet = engine.graph.default_sheet_name().to_string();
        engine.ensure_arrow_sheet(&default_sheet);
        engine
    }

    /// Create an Engine with a custom thread pool (for shared thread pool scenarios)
    ///
    /// # Panics
    /// Panics when `config.cycle` is invalid, exactly like [`Engine::new`].
    pub fn with_thread_pool(
        resolver: R,
        config: EvalConfig,
        thread_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        if let Err(msg) = config.cycle.validate() {
            panic!("invalid CycleConfig: {msg}");
        }
        crate::builtins::load_builtins();
        let resolved_resources = crate::engine::resource_ledger::resolve_evaluation_budgets(
            &config.evaluation_budgets,
            config.max_vertices,
            config.max_memory_mb,
            config.max_eval_time,
        );
        let clock = config.deterministic_mode.build_clock().unwrap_or_else(|_| {
            #[cfg(feature = "system-clock")]
            {
                Arc::new(crate::timezone::SystemClock::new(
                    crate::timezone::TimeZoneSpec::default(),
                ))
            }
            #[cfg(not(feature = "system-clock"))]
            {
                Arc::new(crate::timezone::FixedClock::new(
                    chrono::DateTime::UNIX_EPOCH,
                    crate::timezone::TimeZoneSpec::Utc,
                ))
            }
        });
        // C1a retained/cache budgets are observational; cache defaults stay explicit.
        let lookup_cache_max_bytes = config.lookup_index_cache_max_bytes;
        let function_provider_revision_seen = resolver.planning_semantic_revision();
        let mut engine = Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            workbook_load_limits: crate::engine::WorkbookLoadLimits::default(),
            clock: crate::timezone::SnapshotClock::new(clock),
            thread_pool: Some(thread_pool),
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            topology_epoch: 0,
            cached_static_schedule: None,
            recent_schedules: Vec::new(),
            base_schedule: None,
            #[cfg(any(test, feature = "benchmark_internal"))]
            recalc_reuse_probe: std::sync::Mutex::new(RecalcReuseProbe::default()),
            spill_mgr: ShimSpillManager::default(),
            arrow_sheets: SheetStore::default(),
            format_registry: crate::format::FormatRegistry::default(),
            derived_formats: Default::default(),
            #[cfg(test)]
            derived_format_operations_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            family_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            invariant_bound_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            lifted_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            chained_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            lane_clean_reads_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            criteria_kernel_members_for_test: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            memo_hits_for_test: std::sync::atomic::AtomicU64::new(0),
            compressed_at_build: None,
            #[cfg(test)]
            computed_overlay_set_explicit_entry_operations_for_test: 0,
            #[cfg(test)]
            computed_overlay_stale_clear_range_effects_for_test: 0,
            #[cfg(test)]
            computed_overlay_stale_clear_offset_attempts_for_test: 0,
            #[cfg(test)]
            computed_format_vector_allocations_for_test: std::sync::atomic::AtomicU64::new(0),
            has_edited: false,
            overlay_compactions: 0,
            computed_overlay_bytes_estimate: 0,
            computed_overlay_mirroring_disabled: false,
            force_materialize_range_views: false,
            row_bounds_cache: std::sync::RwLock::new(None),
            used_axis_bounds_cache: std::sync::RwLock::new(None),
            lookup_index_cache: LookupIndexCache::new(lookup_cache_max_bytes),
            source_cache: Arc::new(std::sync::RwLock::new(SourceCache::default())),
            source_formula_token: Arc::new(()),
            recalc_plan_token: Arc::new(()),
            staged_formulas: std::collections::HashMap::new(),
            staged_formula_index: StagedFormulaIndex::default(),
            blocked_pending_spills: Vec::new(),
            row_visibility: FxHashMap::default(),
            row_visibility_mask_cache: std::sync::RwLock::new(FxHashMap::default()),
            formula_parse_diagnostics: Vec::new(),
            last_formula_ingest_report: None,
            formula_ingest_report_total: FormulaIngestReport::default(),
            active_cancel_flag: None,
            active_evaluation_deadline: None,
            action_depth: 0,
            last_virtual_dep_telemetry: VirtualDepTelemetry::default(),
            virtual_dep_fallback_activations: 0,
            last_cycle_telemetry: CycleTelemetry::default(),
            next_evaluation_resource_request_id: 1,
            evaluation_resource_request_depth: 0,
            active_evaluation_resource_request: None,
            last_evaluation_resource_request: None,
            evaluation_resource_baseline: EvaluationResourceBaselineStats::default(),
            evaluation_resource_request_started_at: None,
            evaluation_resource_budgets: resolved_resources.budgets,
            evaluation_resource_config_diagnostic: resolved_resources.diagnostic,
            active_resource_ledger: None,
            source_cache_footprints: Vec::new(),
            source_cache_accounted: 0,
            pending_iterative_redirty: Vec::new(),
            retained_scc_members: FxHashMap::default(),
            next_retained_scc_id: 0,
            retained_scc_config_fingerprint: 0,
            retained_scc_function_epoch_seen: 0,
            retained_scc_provider_revision_seen: None,
            retained_scc_dirty_at_begin: Vec::new(),
            iterative_state_values: FxHashMap::default(),
            function_semantic_epoch_seen: crate::function_registry::semantic_epoch(),
            function_provider_revision_seen,
            #[cfg(feature = "tracing")]
            trace_evaluation_counters: TraceEvaluationCounters::default(),
            #[cfg(test)]
            evaluation_request_begin_count_for_test: 0,
            #[cfg(any(test, feature = "test-support"))]
            before_prepared_span_commit_hook: None,
            #[cfg(test)]
            before_target_preparation_commit_hook: None,
            #[cfg(test)]
            before_target_planning_snapshot_hook: None,
            #[cfg(test)]
            inject_target_semantic_stale_once_for_test: false,
            #[cfg(test)]
            force_virtual_dep_changes_remaining_for_test: 0,
            freshness: Default::default(),
            #[cfg(test)]
            fail_evaluation_commit_preflight_once_for_test: false,
            #[cfg(test)]
            target_preparation_fault_for_test: None,
            #[cfg(test)]
            force_non_cycle_schedule_fallback_for_test: false,
            #[cfg(test)]
            before_legacy_fallback_final_provider_sample_hook: None,
            #[cfg(test)]
            after_eager_proposal_commit_hook: None,
        };
        // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
        engine.config.arrow_storage_enabled = true;
        engine.config.delta_overlay_enabled = true;
        engine.config.write_formula_overlay_enabled = true;
        let default_sheet = engine.graph.default_sheet_name().to_string();
        engine.ensure_arrow_sheet(&default_sheet);
        engine
    }

    pub fn workbook_load_limits(&self) -> &crate::engine::WorkbookLoadLimits {
        &self.workbook_load_limits
    }

    pub fn set_workbook_load_limits(&mut self, limits: crate::engine::WorkbookLoadLimits) {
        self.workbook_load_limits = limits;
    }

    fn clear_source_cache(&self) {
        if let Ok(mut g) = self.source_cache.write() {
            *g = SourceCache::default();
        }
    }

    pub fn last_virtual_dep_telemetry(&self) -> &VirtualDepTelemetry {
        &self.last_virtual_dep_telemetry
    }

    /// Telemetry from Runtime SCC evaluation during the most recent
    /// evaluation request (always default-zero under `CycleDetection::Static`
    /// or when `enable_virtual_dep_telemetry` is off).
    pub fn last_cycle_telemetry(&self) -> &CycleTelemetry {
        &self.last_cycle_telemetry
    }

    /// Resource observations for the most recently completed public evaluation request.
    pub fn last_evaluation_resource_request_stats(
        &self,
    ) -> Option<&EvaluationResourceRequestStats> {
        self.last_evaluation_resource_request.as_ref()
    }

    /// Cumulative resource observations since engine creation or the last telemetry reset.
    pub fn evaluation_resource_baseline_stats(&self) -> EvaluationResourceBaselineStats {
        self.evaluation_resource_baseline
    }

    pub fn evaluation_resource_budgets(&self) -> &crate::engine::EvaluationBudgets {
        &self.evaluation_resource_budgets
    }

    /// At most one diagnostic is emitted for deprecated resource fields.
    pub fn evaluation_resource_config_diagnostic(
        &self,
    ) -> Option<&crate::engine::EvaluationResourceConfigDiagnostic> {
        self.evaluation_resource_config_diagnostic.as_ref()
    }

    /// Reset accumulated and last-request observations without reusing request IDs.
    pub fn reset_evaluation_resource_telemetry(&mut self) {
        self.evaluation_resource_baseline = EvaluationResourceBaselineStats::default();
        self.last_evaluation_resource_request = None;
    }

    // Reconcile without replay locks: several packages may share the same Arc.
    // Weak tokens neither retain dead packages nor duplicate their allocations.
    fn reconcile_source_cache_footprints(&mut self) -> Result<(), ExcelError> {
        self.blocked_pending_spills.retain(|&(vertex, anchor, _)| {
            self.graph.vertex_exists(vertex)
                && self.graph.get_cell_ref(vertex) == Some(anchor)
                && matches!(
                    self.graph.get_vertex_kind(vertex),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                )
        });
        if self.blocked_pending_spills.is_empty() {
            self.blocked_pending_spills = Vec::new();
        }
        let mut bytes = (self.blocked_pending_spills.capacity()
            * std::mem::size_of::<(VertexId, CellRef, Region)>()) as u64;
        self.source_cache_footprints.retain(|weak| {
            let Some(footprint) = weak.upgrade() else {
                return false;
            };
            bytes = bytes.saturating_add(footprint.load(std::sync::atomic::Ordering::Acquire));
            true
        });
        if let Some(ledger) = self.active_resource_ledger.as_mut() {
            ledger
                .release_retained(self.source_cache_accounted)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
            // Observe first: even a tightened budget cannot erase live ownership.
            ledger.observe_retained(bytes);
            self.source_cache_accounted = bytes;
            ledger
                .reserve_retained(0)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        Ok(())
    }

    fn duration_ns(duration: std::time::Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
    }

    fn observe_evaluation_resource_request<T>(
        &mut self,
        kind: EvaluationRequestKind,
        evaluate: impl FnOnce(&mut Self) -> Result<T, ExcelError>,
    ) -> Result<T, ExcelError> {
        let outermost = self.evaluation_resource_request_depth == 0;
        #[cfg(feature = "tracing")]
        if outermost {
            self.trace_evaluation_counters = TraceEvaluationCounters::default();
        }
        #[cfg(feature = "tracing")]
        let request_kind = if matches!(
            kind,
            EvaluationRequestKind::Full
                | EvaluationRequestKind::FullWithDelta
                | EvaluationRequestKind::FullCancellable
                | EvaluationRequestKind::FullLogged
        ) {
            "full"
        } else {
            "targeted"
        };
        #[cfg(feature = "tracing")]
        let _request_span = outermost.then(|| {
            crate::engine::trace::fz_span!(
                tracing::Level::INFO,
                "evaluate",
                "evaluate.request",
                kind = request_kind,
                mode = ?FormulaPlaneMode::Off
            )
        });
        if outermost {
            let request_id = self.next_evaluation_resource_request_id;
            self.next_evaluation_resource_request_id = request_id
                .checked_add(1)
                .expect("evaluation resource request ID exhausted");
            self.active_evaluation_resource_request = Some(EvaluationResourceRequestStats::new(
                request_id,
                kind,
                FormulaPlaneMode::Off,
                self.staged_formula_count(),
            ));
            self.evaluation_resource_baseline.record_started(request_id);
            self.evaluation_resource_request_started_at = Some(crate::instant::FzInstant::now());
            self.source_cache_accounted = 0;
            self.active_resource_ledger = Some(ResourceLedger::new(
                Some(request_id),
                self.evaluation_resource_budgets.clone(),
            ));
        }
        self.evaluation_resource_request_depth =
            self.evaluation_resource_request_depth.saturating_add(1);
        let result = if outermost {
            self.reconcile_source_cache_footprints()
                .and_then(|()| self.resource_checkpoint(0))
                .and_then(|()| evaluate(self))
        } else {
            evaluate(self)
        };
        if outermost && result.is_err() {
            self.freshness_abort_pass();
        }
        self.evaluation_resource_request_depth =
            self.evaluation_resource_request_depth.saturating_sub(1);

        let reconciliation = if outermost {
            self.reconcile_source_cache_footprints()
        } else {
            Ok(())
        };
        let result = result.and_then(|value| reconciliation.map(|()| value));
        if outermost {
            let total_ns = self
                .evaluation_resource_request_started_at
                .take()
                .map(|start| Self::duration_ns(start.elapsed()))
                .unwrap_or(0);
            let mut stats = self
                .active_evaluation_resource_request
                .take()
                .expect("outer evaluation resource request has active stats");
            let mut ledger = self
                .active_resource_ledger
                .take()
                .expect("outer evaluation resource request has active ledger");
            ledger.release_all_scratch();
            stats.ledger.update(ledger.snapshot());
            stats.outcome = match &result {
                Ok(_) => EvaluationRequestOutcome::Success,
                Err(error) if error.kind == ExcelErrorKind::Cancelled => {
                    EvaluationRequestOutcome::Cancelled
                }
                Err(_) => EvaluationRequestOutcome::Error,
            };
            if stats.dirty_lease == FormulaDirtyLeaseOutcome::Acquired {
                stats.dirty_lease = if stats.outcome == EvaluationRequestOutcome::Cancelled {
                    FormulaDirtyLeaseOutcome::RetainedOnCancellation
                } else {
                    FormulaDirtyLeaseOutcome::RetainedOnError
                };
            }
            stats.phases.total_ns = total_ns;
            let attributed = stats
                .phases
                .staged_prepare_ns
                .saturating_add(stats.phases.topology_ns)
                .saturating_add(stats.phases.materialization_ns);
            stats.phases.evaluation_ns = total_ns.saturating_sub(attributed);
            self.evaluation_resource_baseline.record_finished(&stats);
            self.last_evaluation_resource_request = Some(stats);
            crate::engine::trace::fz_event!(
                tracing::Level::INFO,
                "evaluate",
                "evaluate.summary",
                computed_vertices = self.trace_evaluation_counters.computed_vertices,
                cycles = self.trace_evaluation_counters.cycles,
                cancelled = matches!(
                    &result,
                    Err(error) if error.kind == ExcelErrorKind::Cancelled
                )
            );
        }
        result
    }

    pub fn set_evaluation_resource_budgets(&mut self, budgets: crate::engine::EvaluationBudgets) {
        self.evaluation_resource_budgets = budgets.clone();
        self.config.evaluation_budgets = budgets.clone();
        self.graph.set_evaluation_budgets(budgets);
    }

    #[cfg(test)]
    pub(crate) fn set_evaluation_budgets_for_test(
        &mut self,
        budgets: crate::engine::EvaluationBudgets,
    ) {
        self.set_evaluation_resource_budgets(budgets);
    }

    fn preflight_evaluation_commit_window(
        &mut self,
        bounded_writes: usize,
    ) -> Result<crate::instant::FzInstant, ExcelError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_evaluation_commit_preflight_once_for_test) {
            return Err(crate::engine::ResourceLedgerError::Exhausted(
                formualizer_common::ResourceExhaustionDetail {
                    reason: formualizer_common::ResourceExhaustionReason::Deadline,
                    limit: 0,
                    observed: 1,
                    request_id: self
                        .active_evaluation_resource_request
                        .as_ref()
                        .map(|stats| stats.request_id),
                },
            )
            .into_excel_error());
        }
        let estimate = std::time::Duration::from_nanos(
            u64::try_from(bounded_writes)
                .unwrap_or(u64::MAX)
                .saturating_mul(100),
        );
        if let Some(ledger) = self.active_resource_ledger.as_mut() {
            ledger
                .preflight_commit_window(estimate)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        if let Some(stats) = self.active_evaluation_resource_request.as_mut() {
            stats.evaluation_commit_preflight_count =
                stats.evaluation_commit_preflight_count.saturating_add(1);
            stats.evaluation_commit_estimated_ns = stats
                .evaluation_commit_estimated_ns
                .saturating_add(Self::duration_ns(estimate));
        }
        Ok(crate::instant::FzInstant::now())
    }

    fn observe_evaluation_commit_window(&mut self, started: crate::instant::FzInstant) {
        if let Some(stats) = self.active_evaluation_resource_request.as_mut() {
            stats.evaluation_commit_actual_ns = stats
                .evaluation_commit_actual_ns
                .saturating_add(Self::duration_ns(started.elapsed()));
        }
    }

    /// Post-work cancellation boundary: call after evaluating a unit (or a
    /// parallel group) and before committing it. A function that observed
    /// the request's token returns `Err(Cancelled)`, which the evaluator
    /// turns into a `#CANCELLED` value; any other result computed across the
    /// signal is equally not a finished result. Neither may publish: the
    /// caller returns this error before committing, so the unit stays dirty,
    /// the request reports `Cancelled`, and a failed pass restores the
    /// vertices it already committed (`freshness_abort_pass`). The token is
    /// the discriminator: a `#CANCELLED` value with no live cancellation is
    /// ordinary data and commits. Deadlines are not checked here; finished
    /// work is not discarded on a deadline.
    fn live_cancellation_after_work(&self, message: &'static str) -> Result<(), ExcelError> {
        if self
            .active_cancel_flag
            .as_ref()
            .is_some_and(|cancel| cancel.is_cancelled())
        {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(message));
        }
        Ok(())
    }

    fn cancellation_checkpoint(&self, message: &'static str) -> Result<(), ExcelError> {
        if self
            .active_cancel_flag
            .as_ref()
            .is_some_and(|cancel| cancel.is_cancelled())
        {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(message));
        }
        if self
            .active_evaluation_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(crate::engine::ResourceLedgerError::Exhausted(
                formualizer_common::ResourceExhaustionDetail {
                    reason: formualizer_common::ResourceExhaustionReason::Deadline,
                    limit: 0,
                    observed: 1,
                    request_id: self
                        .active_evaluation_resource_request
                        .as_ref()
                        .map(|request| request.request_id),
                },
            )
            .into_excel_error()
            .with_message(message));
        }
        Ok(())
    }

    fn resource_checkpoint(&mut self, work_units: u64) -> Result<(), ExcelError> {
        let Some(ledger) = self.active_resource_ledger.as_mut() else {
            return Ok(());
        };
        ledger
            .charge_work(work_units)
            .and_then(|()| ledger.checkpoint_deadline())
            .map_err(crate::engine::ResourceLedgerError::into_excel_error)
    }

    fn charge_bounded_work(&mut self, mut work_units: u64) -> Result<(), ExcelError> {
        if work_units == 0 {
            return self.resource_checkpoint(0);
        }
        while work_units > 0 {
            let chunk = work_units.min(256);
            self.resource_checkpoint(chunk)?;
            work_units -= chunk;
        }
        Ok(())
    }

    fn reserve_request_scratch(&mut self, bytes: u64) -> Result<(), ExcelError> {
        if let Some(ledger) = self.active_resource_ledger.as_mut() {
            // Exact request topology and activation of the scratch cap are C1b. C1a records
            // scoped ownership but must not introduce a new skip or terminal path.
            ledger.observe_scratch(bytes);
        }
        Ok(())
    }

    fn release_request_scratch(&mut self, bytes: u64) {
        if let Some(ledger) = self.active_resource_ledger.as_mut() {
            let released = ledger.release_scratch(bytes);
            debug_assert!(
                released.is_ok(),
                "request scratch release exceeded the outstanding reservation"
            );
        }
    }

    fn reserve_topology_scratch(&mut self, bytes: u64) -> Result<(), ExcelError> {
        self.active_resource_ledger
            .as_mut()
            .map_or(Ok(()), |ledger| ledger.reserve_schedule_discovery(bytes))
            .map_err(crate::engine::ResourceLedgerError::into_excel_error)
    }

    fn reserve_graph_source_scratch(&mut self, bytes: u64) -> Result<(), ExcelError> {
        self.active_resource_ledger
            .as_mut()
            .map_or(Ok(()), |ledger| ledger.reserve_graph_source(bytes))
            .map_err(crate::engine::ResourceLedgerError::into_excel_error)
    }

    fn with_request_scratch<T>(
        &mut self,
        bytes: u64,
        work: impl FnOnce(&mut Self) -> Result<T, ExcelError>,
    ) -> Result<T, ExcelError> {
        self.reserve_request_scratch(bytes)?;
        let result = work(self);
        self.release_request_scratch(bytes);
        result
    }

    fn observe_staged_preparation(
        &mut self,
        selected: usize,
        retained: usize,
        elapsed: std::time::Duration,
    ) {
        if let Some(stats) = self.active_evaluation_resource_request.as_mut() {
            stats.staged_selected = stats.staged_selected.saturating_add(selected as u64);
            stats.staged_retained = retained as u64;
            stats.phases.staged_prepare_ns = stats
                .phases
                .staged_prepare_ns
                .saturating_add(Self::duration_ns(elapsed));
        }
    }

    fn graph_admission_enabled(&self) -> bool {
        crate::engine::resource_ledger::graph_admission_enabled(&self.evaluation_resource_budgets)
    }

    fn preflight_graph_admission(
        &mut self,
        usage: crate::engine::resource_ledger::GraphAdmission,
    ) -> Result<(), ExcelError> {
        let request_id = self
            .active_evaluation_resource_request
            .as_ref()
            .map(|stats| stats.request_id);
        crate::engine::resource_ledger::preflight_graph_admission(
            &self.evaluation_resource_budgets,
            usage,
            request_id,
        )
        .map_err(crate::engine::ResourceLedgerError::into_excel_error)
    }

    fn prepared_legacy_admission(
        &mut self,
        plan: &PreparedLegacyGraphPlan,
        materialization_cells: u64,
    ) -> Result<(), ExcelError> {
        if !self.graph_admission_enabled() {
            return Ok(());
        }
        let stats = self.graph.baseline_stats();
        let added_edges = plan.planned_edge_count().ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::NImpl).with_message("graph edge count overflow")
        })?;
        let removed_edges = plan.removed_edge_count().ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::NImpl).with_message("graph edge count overflow")
        })?;
        self.preflight_graph_admission(crate::engine::resource_ledger::GraphAdmission {
            final_vertices: stats
                .graph_vertex_count
                .checked_add(plan.new_vertex_count())
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("graph vertex count overflow")
                })?,
            final_edges: stats
                .graph_edge_count
                .checked_sub(removed_edges)
                .and_then(|count| count.checked_add(added_edges))
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl).with_message("graph edge count overflow")
                })?,
            materialization_cells,
            added_vertices: plan.new_vertex_count(),
            added_edges,
        })
    }

    fn observe_target_admission_failure(
        &mut self,
        reason: formualizer_common::ResourceExhaustionReason,
    ) {
        if let Some(stats) = self.active_evaluation_resource_request.as_mut() {
            stats.target_admission_failure = Some(reason);
        }
    }

    fn observe_target_preparation_report(
        &mut self,
        report: &crate::engine::PreparedTargetGraphReport,
    ) {
        let reason_bit = |reason: crate::engine::OpaqueReason| -> u64 {
            let index = match reason {
                crate::engine::OpaqueReason::DynamicReference => 0,
                crate::engine::OpaqueReason::RuntimeTextReference => 1,
                crate::engine::OpaqueReason::UnknownFunction => 2,
                crate::engine::OpaqueReason::UnknownCustomFunction => 3,
                crate::engine::OpaqueReason::UnresolvedCrossSheetBinding => 4,
                crate::engine::OpaqueReason::UnresolvedName => 5,
                crate::engine::OpaqueReason::UnresolvedTable => 6,
                crate::engine::OpaqueReason::FormulaName => 7,
                crate::engine::OpaqueReason::DeferredSourcePackage => 8,
                crate::engine::OpaqueReason::UnsupportedSourceSemantics => 9,
                crate::engine::OpaqueReason::UncertainDefaultSheetBinding => 10,
            };
            1u64 << index
        };
        if let Some(stats) = self.active_evaluation_resource_request.as_mut() {
            stats.staged_selected = report.selected_staged_cells as u64;
            stats.staged_retained = report.retained_staged_cells as u64;
            stats.target_requested = report.requested_targets as u64;
            stats.target_normalized_regions = report.normalized_regions as u64;
            stats.target_scope_level = match &report.widened_scope {
                crate::engine::PrepareScope::Exact => 0,
                crate::engine::PrepareScope::Sheets(_) => 1,
                crate::engine::PrepareScope::Workbook => 2,
            };
            stats.target_widening_reason_bits = report
                .widening_reasons
                .iter()
                .copied()
                .fold(0, |bits, reason| bits | reason_bit(reason));
            stats.graph_source_scratch_estimated = report.estimated_scratch_bytes;
            stats.graph_source_scratch_observed = report.observed_scratch_bytes;
            stats.target_commit_estimated_work = report.estimated_commit_work;
            stats.target_commit_actual_work = report.actual_commit_work;
            stats.target_commit_window_ns = Self::duration_ns(report.commit_window);
            stats.phases.staged_prepare_ns = stats
                .phases
                .staged_prepare_ns
                .saturating_add(Self::duration_ns(report.commit_window));
        }
    }

    /// Begin a new evaluation request: reset per-recalc cycle telemetry and
    /// take the per-recalc volatile clock sample. Called at the start of
    /// every evaluation request that walks schedule units.
    fn begin_evaluation_request(&mut self) {
        self.freshness_begin_request();
        #[cfg(test)]
        {
            self.evaluation_request_begin_count_for_test = self
                .evaluation_request_begin_count_for_test
                .saturating_add(1);
        }
        self.last_cycle_telemetry = CycleTelemetry::default();
        self.graph.authority_sync();
        // Defensive: consumed at the end of the previous request; a request
        // that errored out mid-walk must not leak its members into this one.
        self.pending_iterative_redirty.clear();
        self.reconcile_retained_sccs_at_request_begin();
        // Spec §7.11: NOW()/TODAY() sample the clock ONCE per recalc; every
        // read within this request (including SCC iteration passes) observes
        // this sample.
        self.clock.refresh();
    }

    /// End-of-recalc redirty: volatile vertices (as always) plus members of
    /// SCCs that iterated this recalc without reaching a retainable fixed
    /// point (`CyclePolicy::Iterate`), so circular cells re-evaluate on every
    /// recalc exactly like Excel's iterative calculation (spec §4
    /// persistence / §7.6 accumulator / §7.11 volatile redirty). Retained
    /// SCCs (`retained_scc_members`, #368) are left clean. Replaces the bare
    /// `graph.redirty_volatiles()` call at every evaluation-flow exit; must
    /// run AFTER the flow's `clear_dirty_flags`.
    fn redirty_for_next_recalc(&mut self) {
        self.graph.redirty_volatiles();
        let pending = std::mem::take(&mut self.pending_iterative_redirty);
        let dirty_at_begin = std::mem::take(&mut self.retained_scc_dirty_at_begin);
        for (vertex, scc) in dirty_at_begin {
            if self.retained_scc_members.get(&vertex) == Some(&scc) {
                // No SCC task claimed this member during the request: the
                // cycle dissolved and it evaluated as an ordinary formula,
                // or the request never reached it. Either way it is no
                // longer a retained fixed point. Its persisted value is
                // obsolete only if it actually re-evaluated (clean now).
                self.retained_scc_members.remove(&vertex);
                if !self.graph.is_dirty(vertex) {
                    self.iterative_state_values.remove(&vertex);
                }
            }
        }
        if !self.iterative_state_values.is_empty() || !self.retained_scc_members.is_empty() {
            let graph = &self.graph;
            self.iterative_state_values
                .retain(|vertex, _| graph.is_live_formula_vertex(*vertex));
            self.retained_scc_members
                .retain(|vertex, _| graph.is_live_formula_vertex(*vertex));
        }
        // Refresh the §4-persistence snapshot for members that re-run each
        // recalc: these final values survive structural edits that clear the
        // computed overlay (the only value home in canonical mode) so the
        // next SCC task can re-seed from them (see `iterative_state_values`).
        for &vertex in &pending {
            if !self.graph.is_live_formula_vertex(vertex) {
                continue;
            }
            if let Some(cell) = self.graph.get_cell_ref(vertex) {
                let sheet_name = self.graph.sheet_name(cell.sheet_id);
                match self.get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1) {
                    Some(value) if !matches!(value, LiteralValue::Empty) => {
                        self.iterative_state_values.insert(vertex, value);
                    }
                    _ => {
                        self.iterative_state_values.remove(&vertex);
                    }
                }
            }
        }
        if !pending.is_empty() {
            self.graph.redirty_iterative_members(&pending);
        }
    }

    /// Hash of every `EvalConfig` knob that can change the result of a
    /// retained SCC without any edit reaching the dependency graph. The
    /// function registry is tracked separately and precisely (see
    /// `retained_scc_function_epoch_seen`).
    fn retained_scc_config_fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        let config = &self.config;
        std::mem::discriminant(&config.cycle.detection).hash(&mut hasher);
        match config.cycle.policy {
            CyclePolicy::Error => 0u8.hash(&mut hasher),
            CyclePolicy::Iterate {
                max_iterations,
                max_change,
            } => {
                1u8.hash(&mut hasher);
                max_iterations.hash(&mut hasher);
                max_change.to_bits().hash(&mut hasher);
            }
        }
        config.date_system.hash(&mut hasher);
        config.workbook_seed.hash(&mut hasher);
        std::mem::discriminant(&config.volatile_level).hash(&mut hasher);
        // `DeterministicMode` carries a timestamp and a timezone spec; hash
        // its Debug rendering rather than growing its derive set for this.
        format!("{:?}", config.deterministic_mode).hash(&mut hasher);
        config.range_expansion_limit.hash(&mut hasher);
        config.max_open_ended_rows.hash(&mut hasher);
        config.max_open_ended_cols.hash(&mut hasher);
        hasher.finish()
    }

    /// Request-begin bookkeeping for retained SCCs (#368): drop deleted
    /// vertices, invalidate everything when the config fingerprint moved
    /// (marking retained members dirty so their SCC tasks run in this
    /// request), and record how many retained SCCs are being reused, i.e.
    /// have no dirty member at request begin.
    fn reconcile_retained_sccs_at_request_begin(&mut self) {
        self.retained_scc_dirty_at_begin.clear();
        if self.retained_scc_members.is_empty() {
            return;
        }
        // Deleted vertices and members overwritten with a literal (the
        // vertex survives as a value cell) are no longer retained formulas.
        let graph = &self.graph;
        self.retained_scc_members
            .retain(|vertex, _| graph.is_live_formula_vertex(*vertex));
        if self.retained_scc_config_fingerprint() != self.retained_scc_config_fingerprint {
            let members: Vec<VertexId> = self.retained_scc_members.keys().copied().collect();
            self.retained_scc_members.clear();
            self.graph.mark_dirty_many(&members);
            return;
        }
        let changes =
            crate::function_registry::semantic_changes_since(self.retained_scc_function_epoch_seen);
        let global_changed = changes.epoch != self.retained_scc_function_epoch_seen;
        let provider_revision = self.resolver.planning_semantic_revision();
        let provider_changed = provider_revision != self.retained_scc_provider_revision_seen;
        if global_changed || provider_changed {
            let changed: BTreeSet<(String, String)> = changes.keys.into_iter().collect();
            let affected: Vec<VertexId> = self
                .retained_scc_members
                .keys()
                .copied()
                .filter(|&vertex| {
                    let Some(ast) = self.graph.get_formula(vertex) else {
                        return true;
                    };
                    (global_changed
                        && (!changes.complete || Self::ast_uses_changed_function(&ast, &changed)))
                        || (provider_changed && Self::ast_contains_function(&ast))
                })
                .collect();
            for vertex in &affected {
                self.retained_scc_members.remove(vertex);
            }
            if !affected.is_empty() {
                self.graph.mark_dirty_many(&affected);
            }
            self.retained_scc_function_epoch_seen = changes.epoch;
            self.retained_scc_provider_revision_seen = provider_revision;
        }
        let mut dirty_sccs: FxHashSet<u64> = FxHashSet::default();
        let mut all_sccs: FxHashSet<u64> = FxHashSet::default();
        for (&vertex, &scc) in &self.retained_scc_members {
            all_sccs.insert(scc);
            if self.graph.is_dirty(vertex) {
                dirty_sccs.insert(scc);
                self.retained_scc_dirty_at_begin.push((vertex, scc));
            }
        }
        let reused_members = self
            .retained_scc_members
            .values()
            .filter(|scc| !dirty_sccs.contains(scc))
            .count();
        let t = &mut self.last_cycle_telemetry;
        t.reused_sccs = all_sccs.len() - dirty_sccs.len();
        t.reused_scc_members = reused_members;
    }

    pub fn virtual_dep_fallback_activations(&self) -> u64 {
        self.virtual_dep_fallback_activations
    }

    #[cfg(test)]
    pub(crate) fn lookup_index_flights_built_for_test(&self) -> usize {
        self.lookup_index_cache
            .flights_built
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn last_lookup_index_cache_report(&self) -> LookupIndexCacheReport {
        self.lookup_index_cache.report()
    }

    fn lookup_view_contains_volatile(&self, view: &RangeView<'_>, sheet_id: SheetId) -> bool {
        let start_row = view.start_row();
        let end_row = view.end_row();
        let start_col = view.start_col();
        let end_col = view.end_col();
        for row in start_row..=end_row {
            let Ok(row_u32) = u32::try_from(row) else {
                return true;
            };
            for col in start_col..=end_col {
                let Ok(col_u32) = u32::try_from(col) else {
                    return true;
                };
                let cell_ref = self
                    .graph
                    .make_cell_ref_internal(sheet_id, row_u32, col_u32);
                if let Some(vertex_id) = self.graph.get_vertex_id_for_address(&cell_ref)
                    && self.graph.is_volatile(vertex_id)
                {
                    return true;
                }
            }
        }
        false
    }

    fn build_lookup_index_impl(
        &self,
        view: &RangeView<'_>,
        axis: LookupAxis,
    ) -> Option<Arc<LookupIndex>> {
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            self.lookup_index_cache.note_skipped_tiny();
            return None;
        }
        let len = match axis {
            LookupAxis::ColumnInView(col) => {
                if col >= cols {
                    self.lookup_index_cache.note_skipped_tiny();
                    return None;
                }
                rows
            }
            LookupAxis::RowInView(row) => {
                if row >= rows {
                    self.lookup_index_cache.note_skipped_tiny();
                    return None;
                }
                cols
            }
        };
        if len < 64 {
            self.lookup_index_cache.note_skipped_tiny();
            return None;
        }

        let sheet_id = self.graph.sheet_id(view.sheet_name())?;
        let key = LookupIndexKey {
            sheet_id,
            start_row: u32::try_from(view.start_row()).ok()?,
            start_col: u32::try_from(view.start_col()).ok()?,
            end_row: u32::try_from(view.end_row()).ok()?,
            end_col: u32::try_from(view.end_col()).ok()?,
            axis,
            snapshot_id: self.data_snapshot_id(),
        };
        if let Some(index) = self.lookup_index_cache.get(&key) {
            return Some(index);
        }
        if self
            .lookup_index_cache
            .would_exceed_cap(estimate_bytes(len, 0))
        {
            self.lookup_index_cache.note_skipped_cap();
            return None;
        }
        if !self.lookup_index_cache.should_build(key) {
            return None;
        }
        // Parallel members of a lookup family miss together: one builds.
        self.lookup_index_cache.single_flight(key, || {
            if let Some(index) = self.lookup_index_cache.recheck(&key) {
                return Some(index);
            }
            if self.lookup_index_cache.is_known_volatile(&key) {
                self.lookup_index_cache.note_skipped_volatile();
                return None;
            }
            if self.lookup_view_contains_volatile(view, sheet_id) {
                self.lookup_index_cache.note_volatile_key(key);
                self.lookup_index_cache.note_skipped_volatile();
                return None;
            }
            #[cfg(test)]
            self.lookup_index_cache
                .flights_built
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match LookupIndex::build(view, axis, self.config.date_system).ok()? {
                BuildOutcome::Built(index) => self.lookup_index_cache.insert_if_room(key, index),
                BuildOutcome::ErrorInLookupAxis => {
                    self.lookup_index_cache.note_skipped_error();
                    None
                }
                BuildOutcome::Degenerate => {
                    self.lookup_index_cache.note_skipped_tiny();
                    None
                }
            }
        })
    }

    fn reset_virtual_dep_telemetry_if_disabled(&mut self) {
        if !self.config.enable_virtual_dep_telemetry {
            self.last_virtual_dep_telemetry = VirtualDepTelemetry {
                fallback_mode_activations: self.virtual_dep_fallback_activations,
                ..VirtualDepTelemetry::default()
            };
        }
    }

    fn source_cache_session(&self) -> SourceCacheSession {
        self.clear_source_cache();
        SourceCacheSession {
            cache: self.source_cache.clone(),
        }
    }

    fn resolve_source_scalar_cached(
        &self,
        name: &str,
        version: Option<u64>,
    ) -> Result<LiteralValue, ExcelError> {
        let key = (name.to_string(), version);
        if let Ok(mut g) = self.source_cache.write() {
            if let Some(v) = g.scalars.get(&key) {
                return Ok(v.clone());
            }

            let v = self.resolver.resolve_source_scalar(name).map_err(|err| {
                if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("Unresolved source scalar: {name}"))
                } else {
                    err
                }
            })?;
            g.scalars.insert(key, v.clone());
            Ok(v)
        } else {
            self.resolver.resolve_source_scalar(name).map_err(|err| {
                if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("Unresolved source scalar: {name}"))
                } else {
                    err
                }
            })
        }
    }

    fn resolve_source_table_cached(
        &self,
        name: &str,
        version: Option<u64>,
    ) -> Result<Arc<dyn crate::traits::Table>, ExcelError> {
        let key = (name.to_string(), version);
        if let Ok(mut g) = self.source_cache.write() {
            if let Some(t) = g.tables.get(&key) {
                return Ok(t.clone());
            }

            let t = self.resolver.resolve_source_table(name).map_err(|err| {
                if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("Unresolved source table: {name}"))
                } else {
                    err
                }
            })?;
            let t: Arc<dyn crate::traits::Table> = Arc::from(t);
            g.tables.insert(key, t.clone());
            Ok(t)
        } else {
            self.resolver
                .resolve_source_table(name)
                .map_err(|err| {
                    if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("Unresolved source table: {name}"))
                    } else {
                        err
                    }
                })
                .map(Arc::from)
        }
    }

    fn source_table_to_range_view(
        &self,
        table: &dyn crate::traits::Table,
        spec: &Option<formualizer_parse::parser::TableSpecifier>,
    ) -> Result<RangeView<'static>, ExcelError> {
        use formualizer_parse::parser::{SpecialItem, TableSpecifier};

        let owned = match spec {
            Some(TableSpecifier::Column(c)) => {
                let c = c.trim();
                if c == "@" || c.contains('[') || c.contains(']') || c.contains(',') {
                    return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "Complex structured references not yet supported".to_string(),
                    ));
                }
                table.get_column(c)?.materialise().into_owned()
            }
            Some(TableSpecifier::ColumnRange(start, end)) => {
                let cols = table.columns();
                let start = start.trim();
                let end = end.trim();
                let start_key = start.to_lowercase();
                let end_key = end.to_lowercase();
                let start_idx = cols.iter().position(|n| n.to_lowercase() == start_key);
                let end_idx = cols.iter().position(|n| n.to_lowercase() == end_key);
                if let (Some(mut si), Some(mut ei)) = (start_idx, end_idx) {
                    if si > ei {
                        std::mem::swap(&mut si, &mut ei);
                    }
                    let h = table.data_height();
                    let w = ei - si + 1;
                    let mut rows = vec![vec![LiteralValue::Empty; w]; h];
                    for (offset, ci) in (si..=ei).enumerate() {
                        let cname = &cols[ci];
                        let col_range = table.get_column(cname)?;
                        let (rh, _) = col_range.dimensions();
                        for (r, row) in rows.iter_mut().enumerate().take(h.min(rh)) {
                            row[offset] = col_range.get(r, 0)?;
                        }
                    }
                    rows
                } else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Column range refers to unknown column(s)".to_string()));
                }
            }
            Some(TableSpecifier::SpecialItem(SpecialItem::Headers))
            | Some(TableSpecifier::Headers) => table
                .headers_row()
                .map(|r| r.materialise().into_owned())
                .unwrap_or_default(),
            Some(TableSpecifier::SpecialItem(SpecialItem::Totals))
            | Some(TableSpecifier::Totals) => table
                .totals_row()
                .map(|r| r.materialise().into_owned())
                .unwrap_or_default(),
            Some(TableSpecifier::SpecialItem(SpecialItem::Data)) | Some(TableSpecifier::Data) => {
                table
                    .data_body()
                    .map(|r| r.materialise().into_owned())
                    .unwrap_or_default()
            }
            Some(TableSpecifier::SpecialItem(SpecialItem::All)) | Some(TableSpecifier::All) => {
                let mut out: Vec<Vec<LiteralValue>> = Vec::new();
                if let Some(h) = table.headers_row() {
                    out.extend(h.iter_rows());
                }
                if let Some(body) = table.data_body() {
                    out.extend(body.iter_rows());
                }
                if let Some(tr) = table.totals_row() {
                    out.extend(tr.iter_rows());
                }
                out
            }
            Some(TableSpecifier::SpecialItem(SpecialItem::ThisRow)) => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                    "@ (This Row) requires table-aware context; not yet supported".to_string(),
                ));
            }
            Some(TableSpecifier::Row(_)) | Some(TableSpecifier::Combination(_)) => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("Complex structured references not yet supported".to_string()));
            }
            None => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("Table reference without specifier is unsupported".to_string()));
            }
        };

        Ok(RangeView::from_owned_rows(owned, self.config.date_system))
    }

    pub fn default_sheet_id(&self) -> SheetId {
        self.graph.default_sheet_id()
    }

    pub fn default_sheet_name(&self) -> &str {
        self.graph.default_sheet_name()
    }

    /// Update the workbook seed for deterministic RNGs in functions.
    pub fn set_workbook_seed(&mut self, seed: u64) {
        self.config.workbook_seed = seed;
    }

    /// Set the volatile level policy (Always/OnRecalc/OnOpen)
    pub fn set_volatile_level(&mut self, level: crate::traits::VolatileLevel) {
        self.config.volatile_level = level;
    }

    /// Set public temporal materialisation to native values or raw serials.
    pub fn set_temporal_egress(&mut self, policy: crate::engine::TemporalEgress) {
        self.config.temporal_egress = policy;
    }

    pub fn temporal_egress(&self) -> crate::engine::TemporalEgress {
        self.config.temporal_egress
    }

    /// Enable/disable deterministic evaluation mode (fixed clock + timezone).
    pub fn set_deterministic_mode(
        &mut self,
        mode: crate::engine::DeterministicMode,
    ) -> Result<(), ExcelError> {
        let clock = mode.build_clock()?;
        self.config.deterministic_mode = mode;
        self.clock = crate::timezone::SnapshotClock::new(clock);
        Ok(())
    }

    /// Inject a custom [`ClockProvider`](crate::timezone::ClockProvider) for
    /// volatile date/time builtins (`NOW()`, `TODAY()`).
    ///
    /// The provider is the clock *source*; per spec §7.11 the engine samples
    /// it once at the start of every evaluation request and all reads within
    /// that recalc (including SCC iteration passes) observe the frozen
    /// sample.
    pub fn set_clock(&mut self, clock: Arc<dyn crate::timezone::ClockProvider>) {
        self.clock = crate::timezone::SnapshotClock::new(clock);
    }

    fn validate_deterministic_mode(&self) -> Result<(), ExcelError> {
        self.config.deterministic_mode.validate()
    }

    pub fn sheet_id(&self, name: &str) -> Option<SheetId> {
        self.graph.sheet_id(name)
    }

    pub fn sheet_id_mut(&mut self, name: &str) -> SheetId {
        self.add_sheet(name)
            .unwrap_or_else(|_| self.graph.sheet_id_mut(name))
    }

    pub fn sheet_name(&self, id: SheetId) -> &str {
        self.graph.sheet_name(id)
    }

    pub fn add_sheet(&mut self, name: &str) -> Result<SheetId, ExcelError> {
        let id = self.graph.add_sheet(name)?;
        self.ensure_arrow_sheet(name);
        self.mark_topology_edited();
        Ok(id)
    }

    pub fn duplicate_sheet(&mut self, source: &str, new_name: &str) -> Result<SheetId, ExcelError> {
        let source_id = self.graph.sheet_id(source).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value).with_message("Source sheet does not exist")
        })?;
        if new_name.is_empty() || new_name.len() > 255 {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Invalid sheet name"));
        }
        if self.graph.sheet_id(new_name).is_some() {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Sheet '{new_name}' already exists")));
        }
        let new_id = self.graph.duplicate_sheet(source_id, new_name)?;

        if let Some(source_sheet) = self.arrow_sheets.sheet(source).cloned() {
            let mut copied_sheet = source_sheet;
            copied_sheet.name = Arc::<str>::from(new_name);
            self.arrow_sheets.sheets.push(copied_sheet);
        } else {
            self.ensure_arrow_sheet(new_name);
        }

        let duplicated_formulas = self
            .graph
            .formula_vertices()
            .into_iter()
            .filter(|vertex| {
                self.graph
                    .get_cell_ref(*vertex)
                    .is_some_and(|cell| cell.sheet_id == new_id)
            })
            .collect::<Vec<_>>();
        self.graph.mark_vertices_dirty_batch(&duplicated_formulas);
        self.mark_topology_edited();
        Ok(new_id)
    }

    fn ensure_arrow_sheet(&mut self, name: &str) {
        if self.arrow_sheets.sheet(name).is_some() {
            return;
        }
        self.arrow_sheets
            .sheets
            .push(crate::arrow_store::ArrowSheet {
                name: std::sync::Arc::<str>::from(name),
                date_system: self.config.date_system,
                columns: Vec::new(),
                nrows: 0,
                chunk_starts: Vec::new(),
                chunk_rows: 32 * 1024,
            });
    }

    pub fn remove_sheet(&mut self, sheet_id: SheetId) -> Result<(), ExcelError> {
        let name = self.graph.sheet_name(sheet_id).to_string();
        self.purge_derived_formats_for_sheet(sheet_id);
        self.graph.remove_sheet(sheet_id)?;
        self.arrow_sheets.sheets.retain(|s| s.name.as_ref() != name);
        // Sheet removal can change cross-sheet refs, names, and default-sheet
        // resolution. Until those domains have a complete exact dependency
        // proof, retain the documented graph-owned global invalidation.
        self.clear_all_computed_overlays();
        self.mark_all_formula_vertices_dirty();
        self.clear_staged_formulas_for_sheet(&name);
        if self.row_visibility.remove(&sheet_id).is_some() {
            self.invalidate_row_visibility_mask_cache();
        }
        self.record_structural_change(StructuralScope::RemovedSheet(sheet_id));
        self.mark_topology_edited();
        Ok(())
    }

    /// Helper to synchronize the Arrow-backed storage layer.
    fn rename_sheet_in_arrow_store(&mut self, target_name: &str, new_name: &str) -> bool {
        if let Some(asheet) = self
            .arrow_sheets
            .sheets
            .iter_mut()
            .find(|s| s.name.as_ref() == target_name)
        {
            asheet.name = std::sync::Arc::<str>::from(new_name);
            return true;
        }
        false
    }

    pub fn rename_sheet(&mut self, sheet_id: SheetId, new_name: &str) -> Result<(), ExcelError> {
        let old_name = self.graph.sheet_name(sheet_id).to_string();

        // Speculative Storage Update
        // Update name in storage FIRST so the Evaluator can find it during Graph rescue.
        self.rename_sheet_in_arrow_store(&old_name, new_name);

        // Graph Update (Metadata + Rescue Logic)
        match self.graph.rename_sheet(sheet_id, new_name) {
            Ok(_) => {
                self.rename_staged_formula_sheet(&old_name, new_name);
                // Success! Invalidate cache for the moved sheet
                let sheet_vertices: Vec<VertexId> = self
                    .graph
                    .grid_vertices_in_sheet(sheet_id)
                    .map(|(id, _)| id)
                    .collect();
                for v_id in sheet_vertices {
                    self.graph.mark_vertex_dirty(v_id);
                }
                // Sheet rename preserves SheetId and therefore formula dependencies.
                self.mark_topology_edited();
                Ok(())
            }
            Err(e) => {
                // ROLLBACK: Revert storage if graph rejected the name
                self.rename_sheet_in_arrow_store(new_name, &old_name);
                Err(e)
            }
        }
    }

    pub fn named_ranges_iter(
        &self,
    ) -> impl Iterator<Item = (&String, &crate::engine::named_range::NamedRange)> {
        self.graph.named_ranges_iter()
    }

    pub fn sheet_named_ranges_iter(
        &self,
    ) -> impl Iterator<Item = (&(SheetId, String), &crate::engine::named_range::NamedRange)> {
        self.graph.sheet_named_ranges_iter()
    }

    pub fn resolve_name_entry(
        &self,
        name: &str,
        current_sheet: SheetId,
    ) -> Option<&crate::engine::named_range::NamedRange> {
        self.graph.resolve_name_entry(name, current_sheet)
    }

    /// The [`NameScope`] an optional scope-sheet argument denotes.
    ///
    /// `None` means **workbook scope**, not "the default sheet": a caller that
    /// supplies no sheet context is asking about workbook-scoped names only.
    /// An unknown sheet name is a malformed query and errors rather than
    /// silently degrading to another sheet's scope (issue #110).
    ///
    /// This is the one owned derivation from `Option<&str>` to a name scope;
    /// every scope-taking entry point routes through it.
    pub(crate) fn name_query_scope(
        &self,
        scope_sheet: Option<&str>,
    ) -> Result<NameScope, ExcelError> {
        match scope_sheet {
            None => Ok(NameScope::Workbook),
            Some(sheet) => self
                .graph
                .sheet_id(sheet)
                .map(NameScope::Sheet)
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("name scope sheet not found: {sheet}"))
                }),
        }
    }

    /// Whether `name` resolves in the scope denoted by `scope_sheet`.
    ///
    /// `scope_sheet == None` asks about workbook scope only; a name scoped to a
    /// single sheet (including the default sheet) does not answer it. An unknown
    /// sheet name resolves nothing.
    /// Resolve a [`SharedSheetLocator`](crate::reference::SharedSheetLocator)
    /// against an explicit context sheet.
    ///
    /// Thin forwarder to
    /// [`SheetRegistry::resolve_locator`](crate::engine::sheet_registry::SheetRegistry::resolve_locator),
    /// the single owned derivation. `Current` resolves to `context_sheet`, never
    /// to the workbook's default sheet.
    fn resolve_sheet_locator(
        &self,
        locator: &crate::reference::SharedSheetLocator<'_>,
        context_sheet: SheetId,
    ) -> Result<SheetId, ExcelError> {
        self.graph
            .sheet_reg()
            .resolve_locator(locator, context_sheet)
    }

    pub fn has_name(&self, name: &str, scope_sheet: Option<&str>) -> bool {
        let Ok(scope) = self.name_query_scope(scope_sheet) else {
            return false;
        };
        self.graph
            .resolve_name_entry_in_scope(name, scope)
            .is_some()
    }

    /// The current value of `name` in the scope denoted by `scope_sheet`.
    ///
    /// Scoping follows [`Self::has_name`]: `None` is workbook scope only.
    pub fn resolved_name_value(
        &self,
        name: &str,
        scope_sheet: Option<&str>,
    ) -> Option<LiteralValue> {
        let scope = self.name_query_scope(scope_sheet).ok()?;
        let entry = self.graph.resolve_name_entry_in_scope(name, scope)?;
        self.graph.get_value(entry.vertex)
    }

    pub fn table_metadata(&self, name: &str) -> Option<TableMetadata> {
        let entry = self.graph.resolve_table_entry(name)?;
        Some(TableMetadata {
            name: entry.name.clone(),
            sheet: self.graph.sheet_name(entry.sheet_id()).to_string(),
            start_row: entry.range.start.coord.row() + 1,
            start_col: entry.range.start.coord.col() + 1,
            end_row: entry.range.end.coord.row() + 1,
            end_col: entry.range.end.coord.col() + 1,
            header_row: entry.header_row,
            headers: entry.headers.clone(),
            totals_row: entry.totals_row,
        })
    }

    /// Metadata for every defined table, ordered by name.
    pub fn tables(&self) -> Vec<TableMetadata> {
        self.graph
            .table_names()
            .into_iter()
            .filter_map(|name| self.table_metadata(&name))
            .collect()
    }

    pub fn named_ranges_snapshot(&self) -> Vec<crate::engine::named_range::NamedRangeSnapshot> {
        let mut out: Vec<crate::engine::named_range::NamedRangeSnapshot> = Vec::new();

        for (name, named) in self.graph.named_ranges_iter() {
            out.push(crate::engine::named_range::NamedRangeSnapshot {
                name: name.clone(),
                scope: NameScope::Workbook,
                definition: named.definition.clone(),
            });
        }

        for ((sheet_id, name), named) in self.graph.sheet_named_ranges_iter() {
            out.push(crate::engine::named_range::NamedRangeSnapshot {
                name: name.clone(),
                scope: NameScope::Sheet(*sheet_id),
                definition: named.definition.clone(),
            });
        }

        out.sort_by(|a, b| {
            let a_scope = match a.scope {
                NameScope::Workbook => (0u8, 0u32),
                NameScope::Sheet(id) => (1u8, u32::from(id)),
            };
            let b_scope = match b.scope {
                NameScope::Workbook => (0u8, 0u32),
                NameScope::Sheet(id) => (1u8, u32::from(id)),
            };
            a_scope.cmp(&b_scope).then_with(|| a.name.cmp(&b.name))
        });

        out
    }

    pub fn named_ranges_snapshot_for_sheet(
        &self,
        sheet_id: SheetId,
    ) -> Vec<crate::engine::named_range::NamedRangeSnapshot> {
        self.named_ranges_snapshot()
            .into_iter()
            .filter(|entry| match entry.scope {
                NameScope::Workbook => true,
                NameScope::Sheet(id) => id == sheet_id,
            })
            .collect()
    }

    pub fn define_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        self.graph.validate_define_name(name, scope)?;
        self.graph.define_name(name, definition, scope)?;
        self.record_structural_change(StructuralScope::AllSheets);

        self.mark_topology_edited();

        Ok(())
    }

    pub fn update_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        self.graph.validate_existing_name(name, scope)?;
        self.graph.update_name(name, definition, scope)?;
        self.record_structural_change(StructuralScope::AllSheets);

        self.mark_topology_edited();

        Ok(())
    }

    pub fn delete_name(&mut self, name: &str, scope: NameScope) -> Result<(), ExcelError> {
        self.graph.validate_existing_name(name, scope)?;
        self.graph.delete_name(name, scope)?;
        self.record_structural_change(StructuralScope::AllSheets);

        self.mark_topology_edited();

        Ok(())
    }

    pub fn define_table(
        &mut self,
        name: &str,
        range: crate::reference::RangeRef,
        header_row: bool,
        headers: Vec<String>,
        totals_row: bool,
    ) -> Result<(), ExcelError> {
        self.graph
            .define_table(name, range, header_row, headers, totals_row)?;
        self.record_structural_change(StructuralScope::AllSheets);
        self.mark_topology_edited();
        Ok(())
    }

    pub fn define_source_scalar(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.define_source_scalar(name, version)?;
        self.record_structural_change(StructuralScope::OpaqueGlobal);
        self.mark_topology_edited();
        Ok(())
    }

    pub fn define_source_table(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.define_source_table(name, version)?;
        self.record_structural_change(StructuralScope::OpaqueGlobal);
        self.mark_topology_edited();
        Ok(())
    }

    pub fn set_source_scalar_version(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.set_source_scalar_version(name, version)?;
        Ok(())
    }

    pub fn set_source_table_version(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.set_source_table_version(name, version)?;
        Ok(())
    }

    pub fn invalidate_source(&mut self, name: &str) -> Result<(), ExcelError> {
        self.graph.invalidate_source(name)?;
        Ok(())
    }

    pub fn vertex_value(&self, vertex: VertexId) -> Option<LiteralValue> {
        self.graph.get_value(vertex)
    }

    pub fn graph_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.graph.get_cell_value(sheet, row, col)
    }

    pub fn vertex_for_cell(&self, cell: &CellRef) -> Option<VertexId> {
        self.graph.get_vertex_for_cell(cell)
    }

    pub fn evaluation_vertices(&self) -> Vec<VertexId> {
        self.graph.get_evaluation_vertices()
    }

    /// Return read-only baseline counters for dispatch benchmarking.
    pub fn baseline_stats(&self) -> EngineBaselineStats {
        let graph = self.graph.baseline_stats();
        EngineBaselineStats {
            graph_vertex_count: graph.graph_vertex_count,
            graph_formula_vertex_count: graph.graph_formula_vertex_count,
            graph_edge_count: graph.graph_edge_count,
            dirty_vertex_count: graph.dirty_vertex_count,
            evaluation_vertex_count: graph.evaluation_vertex_count,
            formula_ast_root_count: graph.formula_ast_root_count,
            formula_ast_node_count: graph.formula_ast_node_count,
            staged_formula_count: self.staged_formula_count(),
            formula_plane_active_span_count: 0,
            formula_plane_producer_result_entries: 0,
            formula_plane_consumer_read_entries: 0,
            formula_plane_mixed_topology_cache_builds: 0,
            formula_plane_mixed_topology_cache_hits: 0,
            formula_plane_mixed_topology_cache_overflows: 0,
            formula_plane_dirty_pending_events: 0,
            formula_plane_dirty_region_events_recorded: 0,
            formula_plane_dirty_span_region_events_recorded: 0,
            formula_plane_dirty_whole_span_seeds_recorded: 0,
            formula_plane_dirty_global_invalidations: 0,
            formula_plane_structural_span_candidates: 0,
            formula_plane_cycle_member_span_demotions: 0,
            formula_plane_array_result_span_demotions: 0,
            retained_scc_members: self.retained_scc_members.len(),
        }
    }

    /// Mutation revision captured by read-only engine reports.
    pub(crate) fn inspection_mutation_revision(&self) -> u64 {
        self.snapshot_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn used_axis_bounds_cache_stats(&self) -> (usize, usize, usize, usize) {
        self.used_axis_bounds_cache
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().map(|cache| {
                    (
                        cache.row_hits.load(Ordering::Relaxed),
                        cache.row_misses.load(Ordering::Relaxed),
                        cache.col_hits.load(Ordering::Relaxed),
                        cache.col_misses.load(Ordering::Relaxed),
                    )
                })
            })
            .unwrap_or((0, 0, 0, 0))
    }

    pub fn set_first_load_assume_new(&mut self, enabled: bool) {
        self.graph.set_first_load_assume_new(enabled);
    }

    pub fn first_load_assume_new(&self) -> bool {
        self.graph.first_load_assume_new()
    }

    pub fn reset_ensure_touched(&mut self) {
        self.graph.reset_ensure_touched();
    }

    pub fn finalize_sheet_index(&mut self, sheet: &str) {
        self.graph.finalize_sheet_index(sheet);
    }

    /// Execute a named Engine action.
    ///
    /// Ticket 614 introduces this as the stable Engine-level transaction surface.
    /// For now actions are commit-only: they do not create changelog boundaries and they do not
    /// provide rollback/atomicity.
    ///
    /// Nested actions are deterministically handled by *disallowing* nesting: calling
    /// `Engine::action` while another action is active returns `EditorError::TransactionFailed`.
    pub fn action<T>(
        &mut self,
        name: impl AsRef<str>,
        f: impl FnOnce(&mut EngineAction<'_, R>) -> Result<T, crate::engine::EditorError>,
    ) -> Result<T, crate::engine::EditorError> {
        if self.action_depth != 0 {
            return Err(crate::engine::EditorError::TransactionFailed {
                reason: "Nested Engine::action calls are not supported (ticket 614: commit-only surface)"
                    .to_string(),
            });
        }

        self.action_depth = 1;
        let engine_ptr: *mut Engine<R> = self;
        let _guard = ActionDepthGuard {
            engine: engine_ptr,
            _marker: std::marker::PhantomData,
        };

        let mut tx = EngineAction {
            engine: self,
            name: name.as_ref().to_string(),
            capture: None,
            arrow_undo: None,
            atomic_policy: false,
        };
        f(&mut tx)
    }

    /// Execute a named Engine action with atomic commit/rollback semantics.
    ///
    /// This variant does not require a `ChangeLog` and uses an internal journal for rollback.
    pub fn action_atomic<T>(
        &mut self,
        name: impl Into<String>,
        f: impl FnOnce(&mut EngineAction<'_, R>) -> Result<T, crate::engine::EditorError>,
    ) -> Result<T, crate::engine::EditorError> {
        let (v, _j) = self.action_atomic_journal(name, f)?;
        Ok(v)
    }

    /// Like `action_atomic`, but returns the committed journal entry for undo/redo storage.
    pub fn action_atomic_journal<T>(
        &mut self,
        name: impl Into<String>,
        f: impl FnOnce(&mut EngineAction<'_, R>) -> Result<T, crate::engine::EditorError>,
    ) -> Result<(T, crate::engine::ActionJournal), crate::engine::EditorError> {
        if self.action_depth != 0 {
            return Err(crate::engine::EditorError::TransactionFailed {
                reason: "Nested Engine::action calls are not supported (deterministic rule)"
                    .to_string(),
            });
        }

        self.action_depth = 1;
        let engine_ptr: *mut Engine<R> = self;
        let _guard = ActionDepthGuard {
            engine: engine_ptr,
            _marker: std::marker::PhantomData,
        };

        let name_str = name.into();
        let mut capture = MutationCapture::new(Default::default());
        let start_len = capture.len();
        self.action_atomic_impl(&mut capture, start_len, true, name_str, f)
    }

    fn action_atomic_impl<T>(
        &mut self,
        capture: &mut MutationCapture,
        start_len: usize,
        expand_runs: bool,
        name: String,
        f: impl FnOnce(&mut EngineAction<'_, R>) -> Result<T, crate::engine::EditorError>,
    ) -> Result<(T, crate::engine::ActionJournal), crate::engine::EditorError> {
        let invalidation_baseline = self.invalidation_baseline();
        let mut arrow_undo = crate::engine::ArrowUndoBatch::default();
        let arrow_ptr: *mut crate::engine::ArrowUndoBatch = &mut arrow_undo;

        let capture_ptr: *mut MutationCapture = capture;
        let mut tx = EngineAction {
            engine: self,
            name: name.clone(),
            capture: Some(capture_ptr),
            arrow_undo: Some(arrow_ptr),
            atomic_policy: true,
        };

        let res = f(&mut tx);

        // Capture graph structural delta for this action. The journal is a
        // public value: run records (Program 2) are expanded into it, except
        // for a caller that only publishes the capture to its change log
        // (`expand_runs` false), where the journal (plain events) drives
        // invalidation and records count as topology changes; a rollback
        // still replays the expanded events.
        let capture_ref = unsafe { &*capture_ptr };
        let has_runs = capture_ref.lazy_len() > 0;
        let graph_events: Vec<crate::engine::ChangeEvent> = if expand_runs || res.is_err() {
            capture_ref.expanded_events_from(start_len, 0)
        } else {
            capture_ref.events()[start_len..].to_vec()
        };
        let graph_batch = crate::engine::GraphUndoBatch {
            events: graph_events,
        };
        let affected_cells = arrow_undo.ops.len();
        let journal = crate::engine::ActionJournal {
            name,
            graph: graph_batch,
            arrow: arrow_undo,
            affected_cells,
        };

        match res {
            Ok(v) => {
                if !journal.graph.is_empty() || !journal.arrow.is_empty() || has_runs {
                    for event in &journal.graph.events {
                        self.record_change_for_event(event);
                    }
                    let mut impact = Self::classify_change_events(
                        &journal.graph.events,
                        LoggedEditDirection::Original,
                    )
                    .max(Self::classify_arrow_undo(&journal.arrow));
                    if has_runs {
                        impact = impact.max(LoggedEditImpact::Topology);
                    }
                    self.apply_logged_edit_impact(impact, invalidation_baseline);
                }
                Ok((v, journal))
            }
            Err(e) => {
                if let Err(rb) = self.rollback_from_action_journal(&journal, invalidation_baseline)
                {
                    return Err(crate::engine::EditorError::TransactionFailed {
                        reason: format!(
                            "Engine::action_atomic rollback failed after error '{e}': {rb}"
                        ),
                    });
                }
                if !journal.graph.is_empty() || !journal.arrow.is_empty() {
                    for event in &journal.graph.events {
                        self.record_change_for_event(event);
                    }
                }
                Err(e)
            }
        }
    }

    /// Execute a named Engine action, logging graph changes into the provided ChangeLog.
    ///
    /// Ticket 615: this variant provides atomicity. If the action returns an error, it rolls back:
    /// - Dependency graph structural edits (via inverse ChangeEvents)
    /// - Arrow-truth overlay writes mirrored from ChangeEvents
    /// - ChangeLog entries (published only after a successful commit)
    pub fn action_with_logger<T>(
        &mut self,
        log: &mut crate::engine::ChangeLog,
        name: impl AsRef<str>,
        f: impl FnOnce(&mut EngineAction<'_, R>) -> Result<T, crate::engine::EditorError>,
    ) -> Result<T, crate::engine::EditorError> {
        if self.action_depth != 0 {
            return Err(crate::engine::EditorError::TransactionFailed {
                reason: "Nested Engine::action calls are not supported (deterministic rule)"
                    .to_string(),
            });
        }

        self.action_depth = 1;
        let engine_ptr: *mut Engine<R> = self;
        let _guard = ActionDepthGuard {
            engine: engine_ptr,
            _marker: std::marker::PhantomData,
        };

        let name_str = name.as_ref().to_string();
        let mut capture = MutationCapture::new(log.current_meta());
        let start_len = capture.len();
        capture.begin_compound(name_str.clone());

        // Mutation correctness uses the complete private capture. The provided ChangeLog remains
        // an observability sink and is not touched until the action outcome is known.
        let res = self.action_atomic_impl(&mut capture, start_len, false, name_str, f);
        capture.close_compounds();

        match res {
            Ok((v, _journal)) => {
                log.publish_capture(capture);
                Ok(v)
            }
            Err(e) => {
                // Preserve sequence/group gaps without retaining failed events or evicting history.
                log.discard_capture(capture);
                Err(e)
            }
        }
    }

    fn rollback_from_action_journal(
        &mut self,
        journal: &crate::engine::ActionJournal,
        invalidation_baseline: InvalidationBaseline,
    ) -> Result<(), crate::engine::EditorError> {
        // Invalidate first so a partial inverse failure cannot leave a changed
        // graph behind an apparently current schedule or lookup cache.
        self.invalidate_for_action_journal(
            journal,
            LoggedEditDirection::InverseReplay,
            invalidation_baseline,
        );
        // 1) Roll back the dependency graph structure.
        journal.graph.undo(&mut self.graph)?;
        // 2) Roll back engine row-visibility sidecar events.
        self.apply_inverse_row_visibility_events(&journal.graph.events);
        // 3) Roll back Arrow-truth overlays.
        self.apply_arrow_undo_batch(&journal.arrow, /*undo=*/ true);
        Ok(())
    }

    fn rollback_from_change_events(
        &mut self,
        events: &[crate::engine::ChangeEvent],
        invalidation_baseline: InvalidationBaseline,
    ) -> Result<(), crate::engine::EditorError> {
        use crate::engine::ChangeEvent;

        // Fail closed before applying inverses because replay can return after
        // only part of the batch has been restored.
        self.invalidate_for_change_events(
            events,
            LoggedEditDirection::InverseReplay,
            invalidation_baseline,
        );

        // 1) Roll back the dependency graph.
        self.graph
            .authority_set_replay(crate::engine::authority::history::Replay::Undo);
        let rolled_back = (|| {
            let mut editor = crate::engine::VertexEditor::new(&mut self.graph);
            let mut compound_stack: Vec<usize> = Vec::new();
            for (i, ev) in events.iter().enumerate().rev() {
                match ev {
                    ChangeEvent::CompoundEnd { depth } => {
                        compound_stack.push(*depth);
                        if let Some(description) =
                            crate::engine::graph::editor::change_log::compound_start_description(
                                i,
                                |j| &events[j],
                            )
                        {
                            editor.inverse_compound_end(description);
                        }
                    }
                    ChangeEvent::CompoundStart { depth, .. } => {
                        if compound_stack.last() == Some(depth) {
                            compound_stack.pop();
                        }
                        editor.apply_inverse(ev.clone())?;
                    }
                    ChangeEvent::SetRowVisibility { .. } => {
                        // Engine-side metadata handled after dropping graph editor borrow.
                    }
                    _ => {
                        editor.apply_inverse(ev.clone())?;
                    }
                }
            }
            Ok::<_, crate::engine::EditorError>(())
        })();
        self.graph
            .authority_set_replay(crate::engine::authority::history::Replay::Forward);
        rolled_back?;

        // 2) Roll back engine row-visibility metadata.
        for ev in events.iter().rev() {
            self.apply_inverse_row_visibility_event(ev);
        }

        // 3) Roll back Arrow-truth overlays mirrored from those ChangeEvents.
        for ev in events.iter().rev() {
            self.mirror_inverse_change_to_arrow(ev);
        }

        Ok(())
    }

    fn read_cell_formula_ast(&self, sheet: &str, row: u32, col: u32) -> Option<ASTNode> {
        let sheet_id = self.graph.sheet_id(sheet)?;
        let coord = Coord::from_excel(row, col, true, true);
        let cell = CellRef::new(sheet_id, coord);
        let vid = self.graph.get_vertex_for_cell(&cell)?;
        self.graph.get_formula(vid)
    }

    pub fn define_name_with_logger(
        &mut self,
        log: &mut crate::engine::ChangeLog,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), crate::engine::EditorError> {
        self.graph
            .validate_define_name(name, scope)
            .map_err(crate::engine::EditorError::Excel)?;

        {
            let mut editor = crate::engine::VertexEditor::with_logger(&mut self.graph, log);
            editor.define_name(name, definition, scope)?;
        }
        self.record_structural_change(StructuralScope::AllSheets);

        self.mark_topology_edited();

        Ok(())
    }

    pub fn update_name_with_logger(
        &mut self,
        log: &mut crate::engine::ChangeLog,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), crate::engine::EditorError> {
        self.graph
            .validate_existing_name(name, scope)
            .map_err(crate::engine::EditorError::Excel)?;
        {
            let mut editor = crate::engine::VertexEditor::with_logger(&mut self.graph, log);
            editor.update_name(name, definition, scope)?;
        }
        self.record_structural_change(StructuralScope::AllSheets);

        self.mark_topology_edited();

        Ok(())
    }

    pub fn delete_name_with_logger(
        &mut self,
        log: &mut crate::engine::ChangeLog,
        name: &str,
        scope: NameScope,
    ) -> Result<(), crate::engine::EditorError> {
        self.graph
            .validate_existing_name(name, scope)
            .map_err(crate::engine::EditorError::Excel)?;
        {
            let mut editor = crate::engine::VertexEditor::with_logger(&mut self.graph, log);
            editor.delete_name(name, scope)?;
        }
        self.record_structural_change(StructuralScope::AllSheets);

        self.mark_topology_edited();

        Ok(())
    }

    pub fn edit_with_logger<T>(
        &mut self,
        log: &mut crate::engine::ChangeLog,
        f: impl FnOnce(&mut crate::engine::VertexEditor) -> T,
    ) -> Result<T, crate::engine::EditorError> {
        let mut capture = MutationCapture::new(log.current_meta());
        let result = self.edit_with_capture(&mut capture, f);
        capture.close_compounds();
        match result {
            Ok(value) => {
                log.publish_capture(capture);
                Ok(value)
            }
            Err(error) => {
                log.discard_capture(capture);
                Err(error)
            }
        }
    }

    fn edit_with_capture<T>(
        &mut self,
        capture: &mut MutationCapture,
        f: impl FnOnce(&mut crate::engine::VertexEditor) -> T,
    ) -> Result<T, crate::engine::EditorError> {
        let invalidation_baseline = self.invalidation_baseline();
        let start_len = capture.len();
        let lazy_start = capture.lazy_len();

        // Provide a spill snapshot reader so VertexEditor can snapshot Arrow-truth spill values
        // (graph value cache is intentionally empty in canonical mode).
        struct ArrowSpillReader<'a> {
            sheets: &'a crate::arrow_store::SheetStore,
        }
        impl crate::engine::graph::editor::vertex_editor::SpillValueReader for ArrowSpillReader<'_> {
            fn read_cell_value(
                &self,
                sheet: &str,
                row: u32,
                col: u32,
            ) -> Option<formualizer_common::LiteralValue> {
                use formualizer_common::LiteralValue;
                let asheet = self.sheets.sheet(sheet)?;
                let r0 = row.saturating_sub(1) as usize;
                let c0 = col.saturating_sub(1) as usize;
                let v = asheet.get_cell_value(r0, c0);
                if matches!(v, LiteralValue::Empty) {
                    None
                } else {
                    Some(v)
                }
            }
        }

        let ret = {
            let spill_reader = ArrowSpillReader {
                sheets: &self.arrow_sheets,
            };
            let mut editor = crate::engine::VertexEditor::with_capture_and_spill_reader(
                &mut self.graph,
                capture,
                &spill_reader,
            );
            f(&mut editor)
        };

        // Plain events only: run records (Program 2) stand for
        // `FormulaAdjusted` events, which have no forward effect here but
        // topology invalidation.
        let new_events = capture.events()[start_len..].to_vec();
        let new_runs = capture.lazy_len() > lazy_start;
        if new_events.iter().any(|event| {
            matches!(
                event,
                ChangeEvent::DefineName { .. }
                    | ChangeEvent::UpdateName { .. }
                    | ChangeEvent::DeleteName { .. }
            )
        }) {
            let all = capture.expanded_events_from(start_len, lazy_start);
            self.rollback_from_change_events(&all, invalidation_baseline)?;
            return Err(crate::engine::EditorError::TransactionUnsupported {
                reason: "name mutations must use Engine's prepared logged-name APIs".to_string(),
            });
        }

        // Mirror value-impacting graph events to Arrow for forward edits.
        // This keeps Arrow overlays (delta + computed) consistent when edits clear/commit spills.
        self.clear_logged_cell_format_states(&new_events);
        for ev in &new_events {
            self.mirror_forward_change_to_arrow(ev);
        }
        for ev in &new_events {
            self.record_change_for_event(ev);
        }

        // Atomic EngineAction calls publish one invalidation for their complete
        // journal at commit/rollback. Direct logged edits publish here.
        if self.action_depth == 0 {
            let mut impact =
                Self::classify_change_events(&new_events, LoggedEditDirection::Original);
            if new_runs {
                impact = impact.max(LoggedEditImpact::Topology);
            }
            self.apply_logged_edit_impact(impact, invalidation_baseline);
        }

        Ok(ret)
    }

    pub(crate) fn preflight_replay_admission(
        &mut self,
        events: &[ChangeEvent],
        forward: bool,
    ) -> Result<(), crate::engine::EditorError> {
        if !self.graph_admission_enabled() {
            return Ok(());
        }
        let mut vertex_delta = 0i128;
        let mut edge_delta = 0i128;
        let mut added_vertices = 0usize;
        let mut added_edges = 0usize;
        let mut formula_cells = BTreeSet::new();
        for event in events {
            match event {
                ChangeEvent::AddVertex {
                    formula,
                    coord,
                    sheet_id,
                    ..
                } => {
                    let delta = if forward { 1 } else { -1 };
                    vertex_delta += delta;
                    if forward {
                        added_vertices = added_vertices.saturating_add(1);
                        if formula.is_some() {
                            formula_cells.insert((*sheet_id, coord.row(), coord.col()));
                        }
                    }
                }
                ChangeEvent::RemoveVertex {
                    old_formula,
                    coord,
                    sheet_id,
                    ..
                } => {
                    let delta = if forward { -1 } else { 1 };
                    vertex_delta += delta;
                    if !forward {
                        added_vertices = added_vertices.saturating_add(1);
                        if old_formula.is_some()
                            && let (Some(sheet_id), Some(coord)) = (sheet_id, coord)
                        {
                            formula_cells.insert((*sheet_id, coord.row(), coord.col()));
                        }
                    }
                }
                ChangeEvent::EdgeAdded { .. } => {
                    let delta = if forward { 1 } else { -1 };
                    edge_delta += delta;
                    if forward {
                        added_edges = added_edges.saturating_add(1);
                    }
                }
                ChangeEvent::EdgeRemoved { .. } => {
                    let delta = if forward { -1 } else { 1 };
                    edge_delta += delta;
                    if !forward {
                        added_edges = added_edges.saturating_add(1);
                    }
                }
                ChangeEvent::SetFormula {
                    addr, old_formula, ..
                } => {
                    if forward || old_formula.is_some() {
                        formula_cells.insert((addr.sheet_id, addr.coord.row(), addr.coord.col()));
                    }
                }
                _ => {}
            }
        }
        let stats = self.graph.baseline_stats();
        let final_vertices = i128::try_from(stats.graph_vertex_count)
            .ok()
            .and_then(|count| count.checked_add(vertex_delta))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| {
                crate::engine::EditorError::Excel(
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("replay vertex count overflow"),
                )
            })?;
        let final_edges = i128::try_from(stats.graph_edge_count)
            .ok()
            .and_then(|count| count.checked_add(edge_delta))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| {
                crate::engine::EditorError::Excel(
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("replay edge count overflow"),
                )
            })?;
        self.preflight_graph_admission(crate::engine::resource_ledger::GraphAdmission {
            final_vertices,
            final_edges,
            materialization_cells: formula_cells.len() as u64,
            added_vertices,
            added_edges,
        })
        .map_err(crate::engine::EditorError::Excel)
    }

    /// Undo the last group still retained by the provided audit log.
    ///
    /// Disabled, zero-cap, and evicted history is unavailable on this index-based path. Use an
    /// explicit `ActionJournal` with `undo_action` when undo must be independent of audit retention.
    pub fn undo_logged(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
        log: &mut crate::engine::ChangeLog,
    ) -> Result<(), crate::engine::EditorError> {
        let pending_events = log
            .last_group_indices()
            .into_iter()
            .map(|index| log.events()[index].clone())
            .collect::<Vec<_>>();
        self.preflight_replay_admission(&pending_events, false)?;
        let invalidation_baseline = self.invalidation_baseline();
        // UndoEngine can fail after partially applying the batch, so publish
        // invalidation before replay rather than only on the success path.
        self.invalidate_for_change_events(
            &pending_events,
            LoggedEditDirection::InverseReplay,
            invalidation_baseline,
        );
        let batch = undo.undo(&mut self.graph, log)?;
        for item in batch.iter().rev() {
            self.apply_inverse_row_visibility_event(&item.event);
            self.apply_inverse_staged_formula_event(&item.event);
        }
        if !batch.is_empty() {
            let events = batch
                .iter()
                .map(|item| item.event.clone())
                .collect::<Vec<_>>();
            self.clear_logged_cell_format_states(&events);
        }
        self.mirror_undo_batch_to_arrow(&batch);
        if !batch.is_empty() {
            for item in &batch {
                self.record_change_for_event(&item.event);
            }
        }
        crate::engine::trace::fz_event!(
            tracing::Level::INFO,
            "history",
            "history.replay",
            op = "undo",
            events_replayed = batch.len(),
            ownership_resyncs = batch.len()
        );
        Ok(())
    }

    pub fn redo_logged(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
        log: &mut crate::engine::ChangeLog,
    ) -> Result<(), crate::engine::EditorError> {
        let pending_events = undo.pending_redo_events();
        self.preflight_replay_admission(&pending_events, true)?;
        let invalidation_baseline = self.invalidation_baseline();
        self.invalidate_for_change_events(
            &pending_events,
            LoggedEditDirection::ForwardReplay,
            invalidation_baseline,
        );
        let batch = undo.redo(&mut self.graph, log)?;
        for item in &batch {
            self.apply_forward_row_visibility_event(&item.event);
            self.apply_forward_staged_formula_event(&item.event);
        }
        if !batch.is_empty() {
            let events = batch
                .iter()
                .map(|item| item.event.clone())
                .collect::<Vec<_>>();
            self.clear_logged_cell_format_states(&events);
        }
        self.mirror_redo_batch_to_arrow(&batch);
        if !batch.is_empty() {
            for item in &batch {
                self.record_change_for_event(&item.event);
            }
        }
        crate::engine::trace::fz_event!(
            tracing::Level::INFO,
            "history",
            "history.replay",
            op = "redo",
            events_replayed = batch.len(),
            ownership_resyncs = batch.len()
        );
        Ok(())
    }

    /// Undo the last committed atomic action using the journal stack.
    ///
    /// This path does not require a `ChangeLog`.
    pub fn undo_action(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
    ) -> Result<(), crate::engine::EditorError> {
        let Some(journal) = undo.pop_undo_action() else {
            return Ok(());
        };
        if let Err(error) = self.preflight_replay_admission(&journal.graph.events, false) {
            undo.push_done_action(journal);
            return Err(error);
        }
        let invalidation_baseline = self.invalidation_baseline();

        self.invalidate_for_action_journal(
            &journal,
            LoggedEditDirection::InverseReplay,
            invalidation_baseline,
        );
        journal.graph.undo(&mut self.graph)?;
        self.apply_inverse_row_visibility_events(&journal.graph.events);
        self.apply_arrow_undo_batch(&journal.arrow, /*undo=*/ true);
        if !journal.graph.is_empty() || !journal.arrow.is_empty() {
            for event in &journal.graph.events {
                self.record_change_for_event(event);
            }
        }

        #[cfg(feature = "tracing")]
        let events_replayed = journal.graph.events.len();
        crate::engine::trace::fz_event!(
            tracing::Level::INFO,
            "history",
            "history.replay",
            op = "undo",
            events_replayed,
            ownership_resyncs = events_replayed
        );
        undo.push_redo_action(journal);
        Ok(())
    }

    /// Redo the last undone atomic action using the journal stack.
    ///
    /// This path does not require a `ChangeLog`.
    pub fn redo_action(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
    ) -> Result<(), crate::engine::EditorError> {
        let Some(journal) = undo.pop_redo_action() else {
            return Ok(());
        };
        if let Err(error) = self.preflight_replay_admission(&journal.graph.events, true) {
            undo.push_redo_action(journal);
            return Err(error);
        }
        let invalidation_baseline = self.invalidation_baseline();
        self.invalidate_for_action_journal(
            &journal,
            LoggedEditDirection::ForwardReplay,
            invalidation_baseline,
        );
        journal.graph.redo(&mut self.graph)?;
        self.apply_forward_row_visibility_events(&journal.graph.events);
        self.apply_arrow_undo_batch(&journal.arrow, /*undo=*/ false);
        if !journal.graph.is_empty() || !journal.arrow.is_empty() {
            for event in &journal.graph.events {
                self.record_change_for_event(event);
            }
        }

        #[cfg(feature = "tracing")]
        let events_replayed = journal.graph.events.len();
        crate::engine::trace::fz_event!(
            tracing::Level::INFO,
            "history",
            "history.replay",
            op = "redo",
            events_replayed,
            ownership_resyncs = events_replayed
        );
        undo.push_done_action(journal);
        Ok(())
    }

    fn cellref_to_sheet_row_col(&self, addr: &crate::reference::CellRef) -> (String, u32, u32) {
        let sheet = self.graph.sheet_name(addr.sheet_id).to_string();
        // Coord stores 0-based indices.
        let row = addr.coord.row() + 1;
        let col = addr.coord.col() + 1;
        (sheet, row, col)
    }

    fn mirror_undo_batch_to_arrow(
        &mut self,
        batch: &[crate::engine::graph::editor::undo_engine::UndoBatchItem],
    ) {
        // Undo applies inverses in reverse order.
        for item in batch.iter().rev() {
            self.mirror_inverse_change_to_arrow(&item.event);
        }
    }

    fn mirror_redo_batch_to_arrow(
        &mut self,
        batch: &[crate::engine::graph::editor::undo_engine::UndoBatchItem],
    ) {
        // Redo applies events in forward order.
        for item in batch.iter() {
            self.mirror_forward_change_to_arrow(&item.event);
        }
    }

    fn mirror_inverse_change_to_arrow(&mut self, ev: &crate::engine::ChangeEvent) {
        use crate::engine::ChangeEvent;
        use formualizer_common::LiteralValue;

        match ev {
            ChangeEvent::SetValue {
                addr,
                old_value,
                old_formula,
                ..
            } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                if old_formula.is_some() {
                    self.clear_delta_overlay_cell(&sheet, row, col);
                } else {
                    let v = old_value.clone().unwrap_or(LiteralValue::Empty);
                    self.mirror_value_to_overlay(&sheet, row, col, &v);
                }
            }
            ChangeEvent::SetFormula {
                addr,
                old_value,
                old_formula,
                ..
            } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                if old_formula.is_some() {
                    self.clear_delta_overlay_cell(&sheet, row, col);
                } else {
                    let v = old_value.clone().unwrap_or(LiteralValue::Empty);
                    self.mirror_value_to_overlay(&sheet, row, col, &v);
                }
            }
            ChangeEvent::SpillCommitted { old, new, .. } => {
                // Inverse: restore `old` (or clear if none).
                self.mirror_spill_snapshot(new, /*clear_only=*/ true);
                if let Some(snap) = old {
                    self.mirror_spill_snapshot(snap, /*clear_only=*/ false);
                }
            }
            ChangeEvent::SpillCleared { old, .. } => {
                // Inverse: restore prior spill.
                self.mirror_spill_snapshot(old, /*clear_only=*/ false);
            }
            ChangeEvent::SetRowVisibility { .. } => {
                // Engine-side metadata only; no Arrow overlay effect.
            }
            _ => {}
        }
    }

    fn mirror_forward_change_to_arrow(&mut self, ev: &crate::engine::ChangeEvent) {
        use crate::engine::ChangeEvent;

        match ev {
            ChangeEvent::SetValue { addr, new, .. } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                self.mirror_value_to_overlay(&sheet, row, col, new);
            }
            ChangeEvent::SetFormula { addr, .. } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                self.clear_delta_overlay_cell(&sheet, row, col);
                // Keep any computed overlay for this cell as-is; it will be recomputed on demand.
            }
            ChangeEvent::SpillCommitted { old, new, .. } => {
                if let Some(snap) = old {
                    self.mirror_spill_snapshot(snap, /*clear_only=*/ true);
                }
                self.mirror_spill_snapshot(new, /*clear_only=*/ false);
            }
            ChangeEvent::SpillCleared { old, .. } => {
                self.mirror_spill_snapshot(old, /*clear_only=*/ true);
            }
            ChangeEvent::SetRowVisibility { .. } => {
                // Engine-side metadata only; no Arrow overlay effect.
            }
            _ => {
                // Other graph structural operations do not have direct value effects in Arrow.
            }
        }
    }

    fn mirror_spill_snapshot(
        &mut self,
        snap: &crate::engine::graph::editor::change_log::SpillSnapshot,
        clear_only: bool,
    ) {
        use formualizer_common::LiteralValue;

        let mut i = 0usize;
        for row in &snap.values {
            for v in row {
                if let Some(cell) = snap.target_cells.get(i) {
                    let (sheet, r, c) = self.cellref_to_sheet_row_col(cell);
                    let out = if clear_only {
                        LiteralValue::Empty
                    } else {
                        v.clone()
                    };
                    self.mirror_value_to_computed_overlay(&sheet, r, c, &out);
                }
                i += 1;
            }
        }
        // If target_cells is longer than values (should not happen), clear remaining cells.
        if clear_only {
            for cell in snap.target_cells.iter().skip(i) {
                let (sheet, r, c) = self.cellref_to_sheet_row_col(cell);
                self.mirror_value_to_computed_overlay(&sheet, r, c, &LiteralValue::Empty);
            }
        }
    }

    pub fn set_default_sheet_by_name(&mut self, name: &str) {
        self.graph.set_default_sheet_by_name(name);
    }

    pub fn set_default_sheet_by_id(&mut self, id: SheetId) {
        self.graph.set_default_sheet_by_id(id);
    }

    pub fn set_sheet_index_mode(&mut self, mode: crate::engine::SheetIndexMode) {
        self.graph.set_sheet_index_mode(mode);
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn mark_all_formulas_dirty_for_test(&mut self) {
        self.mark_all_formula_vertices_dirty();
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn take_criteria_mask_work_for_test() -> (usize, usize) {
        criteria_mask_test_hooks::take_mask_work()
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn lookup_index_cache_report_for_test(&self) -> LookupIndexCacheReport {
        self.lookup_index_cache.report()
    }

    #[cfg(any(test, feature = "benchmark_internal"))]
    #[doc(hidden)]
    pub fn reset_recalc_reuse_probe(&mut self) {
        *self.recalc_reuse_probe.get_mut().unwrap() = RecalcReuseProbe::default();
    }

    #[cfg(any(test, feature = "benchmark_internal"))]
    #[doc(hidden)]
    pub fn recalc_reuse_probe(&self) -> RecalcReuseProbe {
        let mut probe = self.recalc_reuse_probe.lock().unwrap().clone();
        if let Some(cached) = self.cached_static_schedule.as_ref() {
            let entry_bytes = |e: &CachedScheduleEntry| {
                std::mem::size_of::<CachedScheduleEntry>()
                    + e.candidate_vertices.heap_bytes()
                    + std::mem::size_of::<crate::engine::Schedule>()
                    + 2 * std::mem::size_of::<usize>()
                    + schedule_probe_retained_bytes(&e.schedule)
            };
            probe.schedule_retained_bytes = entry_bytes(cached)
                + self.recent_schedules.iter().map(entry_bytes).sum::<usize>()
                + self.base_schedule.as_ref().map_or(0, entry_bytes)
                + self.recent_schedules.capacity() * std::mem::size_of::<CachedScheduleEntry>();
        }
        probe
    }

    #[cfg(test)]
    pub(crate) fn cached_static_schedule_for_test(&self) -> Option<Arc<crate::engine::Schedule>> {
        self.cached_static_schedule
            .as_ref()
            .map(|cached| Arc::clone(&cached.schedule))
    }

    fn clear_cached_static_schedule(&mut self) {
        self.cached_static_schedule = None;
        self.recent_schedules.clear();
        self.base_schedule = None;
    }

    /// Keep a replaced schedule among the recent ones when it is still
    /// current and small; drop stale ones and the oldest beyond the bounds.
    fn retain_recent_schedule(&mut self, entry: CachedScheduleEntry) {
        const RECENT_SCHEDULES: usize = 8;
        const RECENT_SCHEDULE_VERTICES: usize = 65_536;
        let revision = self.schedule_cache_authority_revision();
        let epoch = self.topology_epoch;
        self.recent_schedules
            .retain(|e| e.topology_epoch == epoch && e.authority_revision == revision);
        let current =
            |e: &CachedScheduleEntry| e.topology_epoch == epoch && e.authority_revision == revision;
        if self.base_schedule.as_ref().is_some_and(|b| !current(b)) {
            self.base_schedule = None;
        }
        // The largest current schedule becomes the base; a replaced base
        // may still join the recent ones.
        let mut entry = entry;
        if current(&entry)
            && entry.candidate_vertices.len() > BASE_SCHEDULE_MIN_VERTICES
            && self
                .base_schedule
                .as_ref()
                .is_none_or(|b| entry.candidate_vertices.len() > b.candidate_vertices.len())
        {
            match self.base_schedule.replace(entry) {
                Some(previous) => entry = previous,
                None => return,
            }
        }
        if entry.topology_epoch != epoch
            || entry.authority_revision != revision
            || entry.candidate_vertices.len() > RECENT_SCHEDULE_VERTICES / 4
        {
            return;
        }
        self.recent_schedules.push(entry);
        let mut total: usize = self
            .recent_schedules
            .iter()
            .map(|e| e.candidate_vertices.len())
            .sum();
        while self.recent_schedules.len() > RECENT_SCHEDULES || total > RECENT_SCHEDULE_VERTICES {
            let oldest = self.recent_schedules.remove(0);
            total -= oldest.candidate_vertices.len();
        }
    }

    fn invalidation_baseline(&self) -> InvalidationBaseline {
        InvalidationBaseline {
            snapshot_id: self.snapshot_id.load(std::sync::atomic::Ordering::Relaxed),
            topology_epoch: self.topology_epoch,
        }
    }

    fn classify_change_events(
        events: &[crate::engine::ChangeEvent],
        direction: LoggedEditDirection,
    ) -> LoggedEditImpact {
        use crate::engine::ChangeEvent;

        events
            .iter()
            .map(|event| match event {
                ChangeEvent::CompoundStart { .. } | ChangeEvent::CompoundEnd { .. } => {
                    LoggedEditImpact::NoOp
                }
                ChangeEvent::SetRowVisibility { .. }
                | ChangeEvent::SetValue {
                    old_formula: None,
                    old_value: Some(_),
                    ..
                } => LoggedEditImpact::DataOnly,
                // Original canonical writes can lack an Arrow old value while
                // updating an existing placeholder. Inverse replay actually
                // removes that vertex; a later redo recreates it.
                ChangeEvent::SetValue {
                    old_formula: None,
                    old_value: None,
                    ..
                } if direction == LoggedEditDirection::Original => LoggedEditImpact::DataOnly,
                ChangeEvent::SetValue { .. }
                | ChangeEvent::SetFormula { .. }
                | ChangeEvent::AddVertex { .. }
                | ChangeEvent::RemoveVertex { .. }
                | ChangeEvent::VertexMoved { .. }
                | ChangeEvent::FormulaAdjusted { .. }
                | ChangeEvent::NamedRangeAdjusted { .. }
                | ChangeEvent::EdgeAdded { .. }
                | ChangeEvent::EdgeRemoved { .. }
                | ChangeEvent::DefineName { .. }
                | ChangeEvent::UpdateName { .. }
                | ChangeEvent::DeleteName { .. }
                | ChangeEvent::SpillCommitted { .. }
                | ChangeEvent::SpillCleared { .. }
                | ChangeEvent::StagedFormulaCellChanged { .. } => LoggedEditImpact::Topology,
            })
            .max()
            .unwrap_or(LoggedEditImpact::NoOp)
    }

    fn classify_arrow_undo(arrow: &crate::engine::ArrowUndoBatch) -> LoggedEditImpact {
        use crate::engine::ArrowOp;

        arrow
            .ops
            .iter()
            .map(|op| match op {
                ArrowOp::SetDeltaCell { .. } | ArrowOp::SetComputedCell { .. } => {
                    LoggedEditImpact::DataOnly
                }
                ArrowOp::RestoreComputedRect { .. }
                | ArrowOp::InsertRows { .. }
                | ArrowOp::InsertCols { .. } => LoggedEditImpact::Topology,
            })
            .max()
            .unwrap_or(LoggedEditImpact::NoOp)
    }

    fn apply_logged_edit_impact(
        &mut self,
        impact: LoggedEditImpact,
        baseline: InvalidationBaseline,
    ) {
        match impact {
            LoggedEditImpact::NoOp => {}
            LoggedEditImpact::DataOnly => {
                if self.topology_epoch == baseline.topology_epoch
                    && self.snapshot_id.load(std::sync::atomic::Ordering::Relaxed)
                        == baseline.snapshot_id
                {
                    self.mark_data_edited();
                }
            }
            LoggedEditImpact::Topology => {
                // Some structural entry points already publish topology
                // invalidation. Do not bump the same batch twice.
                if self.topology_epoch == baseline.topology_epoch {
                    self.mark_topology_edited();
                }
            }
        }
    }

    fn invalidate_for_change_events(
        &mut self,
        events: &[crate::engine::ChangeEvent],
        direction: LoggedEditDirection,
        baseline: InvalidationBaseline,
    ) {
        self.apply_logged_edit_impact(Self::classify_change_events(events, direction), baseline);
    }

    fn invalidate_for_action_journal(
        &mut self,
        journal: &crate::engine::ActionJournal,
        direction: LoggedEditDirection,
        baseline: InvalidationBaseline,
    ) {
        let impact = Self::classify_change_events(&journal.graph.events, direction)
            .max(Self::classify_arrow_undo(&journal.arrow));
        self.apply_logged_edit_impact(impact, baseline);
    }

    /// Mark data edited: bump snapshot and set edited flag.
    /// Value-only edits keep the stable-topology schedule cache alive.
    pub fn mark_data_edited(&mut self) {
        self.lookup_index_cache.clear();
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
    }

    /// Mark a topology-changing edit: bump snapshot + topology epoch and invalidate cached schedules.
    pub fn mark_topology_edited(&mut self) {
        self.lookup_index_cache.clear();
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.topology_epoch = self.topology_epoch.wrapping_add(1);
        self.graph.bump_topology_revision();
        self.clear_cached_static_schedule();
        if let Some(ledger) = self.active_resource_ledger.as_mut() {
            let released = ledger.account_mixed_cache(0);
            debug_assert!(released.is_ok());
        }
        self.has_edited = true;
        // Eager sync at a topology edit, so read-only (`&self`) plans and
        // inspection see a current authority (not during a load or an open
        // structural capture).
        self.graph.authority_sync_eager();
    }

    fn mark_all_formula_vertices_dirty(&mut self) {
        let vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();
        for vertex in vertices {
            self.graph.mark_vertex_dirty(vertex);
        }
    }

    fn mark_moved_formula_vertices_dirty(
        &mut self,
        summary: &crate::engine::graph::editor::vertex_editor::ShiftSummary,
    ) {
        for vertex in &summary.vertices_moved {
            if self.graph.has_formula(*vertex) {
                self.graph.mark_vertex_dirty(*vertex);
            }
        }
    }

    /// Access Arrow sheet store (read-only)
    pub fn sheet_store(&self) -> &SheetStore {
        &self.arrow_sheets
    }

    /// True when any sheet carries manual/filter row-visibility state.
    /// Used by load-time freshness checks (see `Engine::adopt_file_sheets`).
    pub(crate) fn has_row_visibility_state(&self) -> bool {
        !self.row_visibility.is_empty()
    }

    /// Access Arrow sheet store (mutable)
    pub fn sheet_store_mut(&mut self) -> &mut SheetStore {
        &mut self.arrow_sheets
    }

    pub fn has_staged_formulas(&self) -> bool {
        !self.staged_formulas.is_empty()
    }

    pub fn staged_formula_count(&self) -> usize {
        self.staged_formulas.values().map(StagedSheet::len).sum()
    }

    /// Stage a formula text instead of inserting into the graph (used when deferring is enabled).
    pub fn stage_formula_text(&mut self, sheet: &str, row: u32, col: u32, text: String) {
        self.staged_formulas
            .entry(sheet.to_string())
            .or_default()
            .stage(row, col, text);
        self.staged_formula_index.stage(sheet, row, col);
        if let Some(sheet) = self.graph.sheet_id(sheet) {
            self.invalidate_pending_spills(StructuralScope::Cell {
                sheet,
                row: row.saturating_sub(1),
                col: col.saturating_sub(1),
            });
        }
    }

    fn index_removed_staged_sheet(&mut self, sheet: &str, staged: &StagedSheet) {
        for (row, col, _) in &staged.entries {
            self.staged_formula_index.remove(sheet, *row, *col);
        }
        if staged.deferred_package.is_some() {
            self.staged_formula_index.set_package(sheet, None);
        }
    }

    fn restore_staged_sheet(&mut self, sheet: String, staged: StagedSheet) {
        self.staged_formulas.insert(sheet, staged);
    }

    #[doc(hidden)]
    pub fn source_formula_ingress(&mut self) -> SourceFormulaIngress<'_, R> {
        SourceFormulaIngress { engine: self }
    }

    #[doc(hidden)]
    /// Test-only fault-injection seam. Not part of the supported API; it exists so
    /// integration tests in sibling crates can fail a commit at an exact point.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_before_prepared_span_commit_hook(
        &mut self,
        hook: impl FnOnce() + Send + Sync + 'static,
    ) {
        self.before_prepared_span_commit_hook = Some(Box::new(hook));
    }

    #[doc(hidden)]
    /// Test-only fault-injection seam, matching `set_after_eager_proposal_commit_hook`.
    #[cfg(test)]
    pub(crate) fn set_before_target_preparation_commit_hook(
        &mut self,
        hook: impl FnOnce() + Send + Sync + 'static,
    ) {
        self.before_target_preparation_commit_hook = Some(Box::new(hook));
    }

    fn stage_deferred_formula_package(&mut self, package: crate::engine::DeferredFormulaPackage) {
        if let Ok(replay) = package.replay.lock()
            && let Some(footprint) = replay.selection_cache_footprint()
            && !self
                .source_cache_footprints
                .iter()
                .any(|known| known.ptr_eq(&footprint))
        {
            self.source_cache_footprints.push(footprint);
        }
        let sheet = package.sheet_name.clone();
        let staged = self.staged_formulas.entry(sheet.clone()).or_default();
        debug_assert!(staged.deferred_package.is_none());
        staged.deferred_package = Some(package);
        staged.reconcile_attached_deferred_package();
        self.staged_formula_index
            .set_package(&sheet, staged.deferred_package.as_ref());
        if let Some(sheet_id) = self.graph.sheet_id(&sheet) {
            let package = self
                .staged_formulas
                .get(&sheet)
                .and_then(|staged| staged.deferred_package.as_ref());
            for &(vertex, anchor, region) in &self.blocked_pending_spills {
                if anchor.sheet_id == sheet_id
                    && self.graph.vertex_exists(vertex)
                    && self.graph.get_cell_ref(vertex) == Some(anchor)
                    && self.staged_formula_index.package_occupies_spill(
                        &sheet,
                        (anchor.coord.row() + 1, anchor.coord.col() + 1),
                        (
                            region.rows.query_bounds().1 + 1,
                            region.cols.query_bounds().1 + 1,
                        ),
                        |point| package.is_some_and(|package| package.suppressed.contains(&point)),
                    )
                {
                    self.graph.mark_vertex_dirty(vertex);
                }
            }
        }
    }

    pub fn clear_staged_formula_text(&mut self, sheet: &str, row: u32, col: u32) -> Option<String> {
        let mut removed = None;
        let mut remove_sheet = false;
        let mut had_package = false;
        if let Some(entries) = self.staged_formulas.get_mut(sheet) {
            had_package = entries.deferred_package.is_some();
            removed = entries.remove(row, col);
            remove_sheet = entries.is_empty();
        }
        let ordinary_removed = self.staged_formula_index.remove(sheet, row, col);
        if !ordinary_removed && had_package {
            self.staged_formula_index.touch_package(sheet);
        }
        if remove_sheet {
            self.staged_formulas.remove(sheet);
            self.staged_formula_index.set_package(sheet, None);
        }
        if (ordinary_removed || had_package)
            && let Some(sheet) = self.graph.sheet_id(sheet)
        {
            self.invalidate_pending_spills(StructuralScope::Cell {
                sheet,
                row: row.saturating_sub(1),
                col: col.saturating_sub(1),
            });
        }
        removed
    }

    pub fn clear_staged_formulas_for_sheet(&mut self, sheet: &str) {
        if self.staged_formulas.remove(sheet).is_some() {
            self.staged_formula_index.clear_sheet(sheet);
            if let Some(sheet_id) = self.graph.sheet_id(sheet) {
                self.invalidate_pending_spills(StructuralScope::Sheet(sheet_id));
            }
        }
    }

    pub fn rename_staged_formula_sheet(&mut self, old: &str, new: &str) {
        let Some(entries) = self.staged_formulas.remove(old) else {
            return;
        };
        self.staged_formula_index.clear_sheet(old);
        let (formulas, mut package) = entries.into_parts();
        for (row, col, text) in formulas {
            self.stage_formula_text(new, row, col, text);
        }
        if let Some(package) = package.as_mut() {
            package.sheet_name = new.to_string();
        }
        if let Some(package) = package {
            self.stage_deferred_formula_package(package);
        }
    }

    /// Get a staged formula text for a given cell if present (cloned).
    pub fn get_staged_formula_text(&self, sheet: &str, row: u32, col: u32) -> Option<String> {
        self.staged_formulas
            .get(sheet)
            .and_then(|v| v.get(row, col))
    }

    pub fn formula_parse_diagnostics(&self) -> &[FormulaParseDiagnostic] {
        &self.formula_parse_diagnostics
    }

    pub fn take_formula_parse_diagnostics(&mut self) -> Vec<FormulaParseDiagnostic> {
        std::mem::take(&mut self.formula_parse_diagnostics)
    }

    pub fn clear_formula_parse_diagnostics(&mut self) {
        self.formula_parse_diagnostics.clear();
    }

    pub fn last_formula_ingest_report(&self) -> Option<&FormulaIngestReport> {
        self.last_formula_ingest_report.as_ref()
    }

    pub fn formula_ingest_report_total(&self) -> &FormulaIngestReport {
        &self.formula_ingest_report_total
    }

    #[cfg(test)]
    pub(crate) fn set_before_target_planning_snapshot_hook_for_test(
        &mut self,
        hook: impl FnOnce() + Send + Sync + 'static,
    ) {
        self.before_target_planning_snapshot_hook = Some(Box::new(hook));
    }

    #[cfg(test)]
    pub(crate) fn inject_target_semantic_stale_once_for_test(&mut self) {
        self.inject_target_semantic_stale_once_for_test = true;
    }

    #[cfg(test)]
    pub(crate) fn force_virtual_dep_changes_for_test(&mut self, rounds: usize) {
        self.force_virtual_dep_changes_remaining_for_test = rounds;
    }

    #[cfg(test)]
    pub(crate) fn fail_evaluation_commit_preflight_once_for_test(&mut self) {
        self.fail_evaluation_commit_preflight_once_for_test = true;
    }

    #[cfg(test)]
    pub(crate) fn set_target_preparation_fault_for_test(
        &mut self,
        fault: crate::engine::target_preparation::TargetPreparationFault,
    ) {
        self.target_preparation_fault_for_test = Some(fault);
    }

    #[cfg(test)]
    pub(crate) fn staged_formula_index_revision_for_test(&self) -> u64 {
        self.staged_formula_index.revision()
    }

    #[cfg(test)]
    pub(crate) fn deferred_package_for_test(
        &self,
        sheet: &str,
    ) -> &crate::engine::DeferredFormulaPackage {
        self.staged_formulas
            .get(sheet)
            .unwrap()
            .deferred_package
            .as_ref()
            .unwrap()
    }

    #[cfg(test)]
    pub(crate) fn staged_formula_index_is_consistent_for_test(&self) -> bool {
        let ordinary_storage = self
            .staged_formulas
            .values()
            .map(|sheet| sheet.entries.len())
            .sum::<usize>();
        let package_storage = self
            .staged_formulas
            .values()
            .filter(|sheet| sheet.deferred_package.is_some())
            .count();
        ordinary_storage == self.staged_formula_index.ordinary_count()
            && package_storage == self.staged_formula_index.package_count()
            && self.staged_formulas.iter().all(|(name, sheet)| {
                sheet.entries.iter().all(|(row, col, _)| {
                    let leases = self
                        .staged_formula_index
                        .leases_in_region(name, *row, *col, *row, *col);
                    leases.len() == 1 && leases[0].row == *row && leases[0].col == *col
                })
            })
    }

    #[cfg(test)]
    pub(crate) fn evaluation_request_begin_count_for_test(&self) -> u64 {
        self.evaluation_request_begin_count_for_test
    }

    #[cfg(test)]
    pub(crate) fn set_before_legacy_fallback_final_provider_sample_hook(
        &mut self,
        hook: impl FnOnce() + Send + Sync + 'static,
    ) {
        self.before_legacy_fallback_final_provider_sample_hook = Some(Box::new(hook));
    }

    #[cfg(test)]
    pub(crate) fn set_after_eager_proposal_commit_hook(
        &mut self,
        hook: impl FnOnce() + Send + Sync + 'static,
    ) {
        self.after_eager_proposal_commit_hook = Some(Box::new(hook));
    }

    #[cfg(test)]
    pub(crate) fn topology_epoch_for_test(&self) -> u64 {
        self.topology_epoch
    }

    #[cfg(test)]
    pub(crate) fn graph_topology_revision_for_test(&self) -> u64 {
        self.graph.topology_revision()
    }

    fn record_formula_ingest_report(&mut self, report: FormulaIngestReport) {
        self.formula_ingest_report_total.mode = report.mode;
        self.formula_ingest_report_total.accumulate(&report);
        self.last_formula_ingest_report = Some(report);
    }

    fn collect_planning_function_requests(
        ast: &ASTNode,
        requests: &mut Vec<(String, String, usize)>,
    ) {
        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                requests.push((String::new(), name.clone(), args.len()));
                for arg in args {
                    Self::collect_planning_function_requests(arg, requests);
                }
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                Self::collect_planning_function_requests(left, requests);
                Self::collect_planning_function_requests(right, requests);
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                Self::collect_planning_function_requests(expr, requests);
            }
            ASTNodeType::Call { callee, args } => {
                Self::collect_planning_function_requests(callee, requests);
                for arg in args {
                    Self::collect_planning_function_requests(arg, requests);
                }
            }
            ASTNodeType::Array(rows) => {
                for cell in rows.iter().flatten() {
                    Self::collect_planning_function_requests(cell, requests);
                }
            }
            ASTNodeType::Literal(_) | ASTNodeType::Omitted | ASTNodeType::Reference { .. } => {}
        }
    }

    fn prepared_function_semantics_changed(
        &self,
        preparation: &crate::engine::FormulaCompressedPreparation,
        guard: &crate::function_registry::SemanticEpochReadGuard,
    ) -> bool {
        if preparation.function_semantic_epoch == guard.epoch() {
            return false;
        }

        guard
            .semantic_changes_affect_requests_since(preparation.function_semantic_epoch, Vec::new())
    }

    pub(crate) fn prepare_source_formula_families(
        &mut self,
        sheet_name: &str,
        families: &[crate::engine::SourceFormulaFamily],
    ) -> crate::engine::FormulaCompressedPreparation {
        crate::engine::FormulaCompressedPreparation {
            engine_token: Arc::clone(&self.source_formula_token),
            function_semantic_epoch: crate::function_registry::semantic_epoch(),
            function_provider_revision: None,
            function_semantics_used: false,
            sheet_name: Arc::from(sheet_name),
            rejected: BTreeMap::new(),
            eager_replay: Vec::new(),
            preparation_spool_replays: 0,
            clean_rejected_anchor_counts: [0; 3],
            fragmented_rejected_anchor_counts: [0; 3],
            exact_replay: None,
            replay_disposition: crate::engine::FormulaReplayDisposition::default(),
        }
    }

    fn prepare_source_formula_proposals(
        &mut self,
        sheet_name: &str,
        families: &[crate::engine::SourceFormulaFamily],
        authority_partitions: &[crate::engine::PartitionedSourceFormulaFamily],
        replay_partitions: &[crate::engine::PartitionedSourceFormulaFamily],
        formula_record_count: u64,
        replay: Arc<std::sync::Mutex<Box<dyn crate::engine::DeferredFormulaReplay>>>,
        suppressed: &BTreeSet<(u32, u32)>,
        consumed: &FxHashSet<(crate::engine::SourceFamilyId, crate::engine::SourceCoord)>,
        consumed_engine: Option<&Arc<()>>,
    ) -> Result<crate::engine::FormulaCompressedPreparation, ExcelError> {
        let mut preparation = self.prepare_source_formula_families(sheet_name, families);
        preparation.exact_replay = Some(Arc::clone(&replay));
        preparation
            .replay_disposition
            .register_consumed_members(consumed_engine, consumed.iter().copied());
        if !preparation
            .replay_disposition
            .consumed_engine_matches(&self.source_formula_token)
        {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("ResidualConsumedEngineMismatch"));
        }
        preparation
            .replay_disposition
            .extend_suppressed_excel_coords(suppressed.iter().copied());
        Ok(preparation)
    }

    fn formula_batch_from_exact_replay(
        &mut self,
        sheet_name: &str,
        replayed: impl IntoIterator<Item = crate::engine::DeferredReplayFormula>,
    ) -> Result<FormulaIngestBatch, ExcelError> {
        let mut cache = rustc_hash::FxHashMap::default();
        let mut formulas = Vec::new();
        for record in replayed {
            let key = if record.text.starts_with('=') {
                record.text
            } else {
                format!("={}", record.text)
            };
            let ast_id = if let Some(cached) = cache.get(&key) {
                *cached
            } else {
                let parsed = match formualizer_parse::parser::parse(&key) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        let Some(parsed) = self.handle_formula_parse_error(
                            sheet_name,
                            record.row,
                            record.col,
                            &key,
                            error.to_string(),
                        )?
                        else {
                            continue;
                        };
                        parsed
                    }
                };
                let ast_id = self.intern_formula_ast(&parsed);
                cache.insert(key.clone(), ast_id);
                ast_id
            };
            formulas.push(
                FormulaIngestRecord::new(record.row, record.col, ast_id, Some(Arc::from(key)))
                    .with_source_proof(record.source_order, record.family, record.partition_owner),
            );
        }
        Ok(FormulaIngestBatch::new(sheet_name.to_string(), formulas))
    }

    fn prepare_target_combined_legacy_graph(
        &self,
        packages: &[PreparedTargetSourcePackage],
        ordinary: &[PreparedOrdinaryStagedFormula],
    ) -> Result<(PreparedLegacyGraphPlan, usize), ExcelError> {
        let mut planned_by_coord = BTreeMap::new();
        for package in packages {
            for (row, col, ast_id, plan) in &package.legacy {
                planned_by_coord.insert((package.sheet_id, *row, *col), (*ast_id, plan.clone()));
            }
        }
        for formula in ordinary {
            if let Some((ast_id, plan)) = formula.ast_id.zip(formula.plan.clone()) {
                planned_by_coord.insert(
                    (formula.sheet_id, formula.lease.row, formula.lease.col),
                    (ast_id, plan),
                );
            }
        }
        let planned = planned_by_coord
            .into_iter()
            .map(|((sheet_id, row, col), (ast_id, plan))| (sheet_id, row, col, ast_id, plan))
            .collect::<Vec<_>>();
        let formula_count = planned.len();
        let graph = self
            .graph
            .prepare_legacy_graph_plan_multi_sheet(planned)
            .map_err(|error| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("target graph preparation failed: {error}"))
            })?;
        Ok((graph, formula_count))
    }

    fn replay_target_coordinates(
        &mut self,
        replay: &Arc<std::sync::Mutex<Box<dyn crate::engine::DeferredFormulaReplay>>>,
        coordinates: &[(u32, u32)],
        deadline: Option<std::time::Instant>,
        scratch: &mut u64,
    ) -> Result<Option<Vec<crate::engine::DeferredReplayFormula>>, ExcelError> {
        if coordinates.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut guard = replay.lock().map_err(|_| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message("deferred formula spool lock poisoned")
        })?;
        let retained = guard.selection_cache_footprint().is_some();
        let records = guard.replay_selected_exact(coordinates, &mut |work, bytes| {
            self.target_preparation_checkpoint(deadline, work)?;
            if retained && let Some(ledger) = self.active_resource_ledger.as_mut() {
                ledger
                    .reserve_retained(bytes)
                    .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
                self.source_cache_accounted = self.source_cache_accounted.saturating_add(bytes);
            }
            self.reserve_graph_source_scratch(bytes)?;
            *scratch = scratch.saturating_add(bytes);
            Ok(())
        });
        drop(guard);
        let reconciled = self.reconcile_source_cache_footprints();
        let records = records?;
        reconciled?;
        Ok(records)
    }

    fn prepare_target_exact_source_selection(
        &mut self,
        sheet: &str,
        lease: StagedPackageLease,
        coordinates: Vec<(u32, u32)>,
        previous: &BTreeSet<(u32, u32)>,
        allow_partial_shared: bool,
        deadline: Option<std::time::Instant>,
        scratch: &mut u64,
    ) -> Result<Option<PreparedTargetSourcePackage>, ExcelError> {
        let package = self
            .staged_formulas
            .get(sheet)
            .and_then(|s| s.deferred_package.as_ref())
            .unwrap();
        if !package.source_geometry_complete
            || ((!package.families.is_empty() || !package.partitioned_families.is_empty())
                && !package.coordinates_cover_families)
            || package.reconciliation_replay.is_some()
        {
            return Ok(None);
        }
        let selected_points: BTreeSet<_> = coordinates
            .into_iter()
            .filter(|point| !package.suppressed.contains(point))
            .collect();
        let mut prepared = PreparedTargetSourcePackage::empty_selection(
            sheet,
            self.graph.sheet_id(sheet).unwrap(),
            lease,
        );
        if selected_points.is_empty() {
            return Ok(Some(prepared));
        }
        let mut routing = crate::engine::FormulaReplayDisposition::default();
        for partition in &package.partitioned_families {
            routing
                .register_partition(partition, false)
                .map_err(|message| ExcelError::new(ExcelErrorKind::Value).with_message(message))?;
        }
        routing.extend_suppressed_excel_coords(package.suppressed.iter().copied());
        routing.register_consumed_members(
            package.consumed_engine.as_ref(),
            package.consumed_members.iter().copied(),
        );
        let replay = Arc::clone(&package.replay);
        let source_report = package.accounting_report();
        prepared.source_report = source_report;
        prepared.disposition = routing.clone();

        let replay_points: BTreeSet<_> = selected_points
            .iter()
            .copied()
            .filter(|&(row, col)| !prepared.direct_contains(row, col))
            .collect();
        let coordinates: Vec<_> = replay_points.iter().copied().collect();
        let records = self.replay_target_coordinates(&replay, &coordinates, deadline, scratch)?;
        prepared.spool_replays = u64::from(!coordinates.is_empty());
        let Some(mut replay_records) = records else {
            return Ok(None);
        };
        for record in &mut replay_records {
            if record.family.is_none() {
                record.partition_owner = routing
                    .ordinary_disposition(crate::engine::SourceCoord {
                        row: record.row.saturating_sub(1),
                        col: record.col.saturating_sub(1),
                    })
                    .1;
            }
        }
        replay_records.sort_by_key(|record| record.source_order);
        if replay_records
            .iter()
            .any(|record| !selected_points.contains(&(record.row, record.col)))
            || replay_records
                .windows(2)
                .any(|records| records[0].source_order == records[1].source_order)
        {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("invalid indexed exact source selection"));
        }
        let represented: BTreeSet<_> = replay_records.iter().map(|r| (r.row, r.col)).collect();
        if represented != replay_points {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("incomplete indexed exact source selection"));
        }
        // The last source record must agree with compressed ownership evidence.
        // Earlier overridden records retain ordering but demand no dependencies.
        let source = self
            .staged_formulas
            .get(sheet)
            .unwrap()
            .deferred_package
            .as_ref()
            .unwrap();
        let contains = |rect: crate::engine::SourceRect, coord: crate::engine::SourceCoord| {
            coord.row >= rect.start.row
                && coord.row <= rect.end.row
                && coord.col >= rect.start.col
                && coord.col <= rect.end.col
        };
        let mut checked = BTreeSet::new();
        for record in replay_records.iter().rev() {
            if !checked.insert((record.row, record.col)) {
                continue;
            }
            let coord = crate::engine::SourceCoord {
                row: record.row - 1,
                col: record.col - 1,
            };
            let agrees = |owner, shared: bool| {
                record.family == shared.then_some(owner)
                    && record.partition_owner.or(record.family) == Some(owner)
            };
            let mut valid = true;
            for family in &source.families {
                let owns = match &family.members {
                    crate::engine::SourceFamilyMembers::CompleteDomain(domain) => {
                        contains(domain.rect(), coord)
                    }
                    crate::engine::SourceFamilyMembers::ExplicitMembers(members) => {
                        members.as_slice().binary_search(&coord).is_ok()
                    }
                };
                if owns && !agrees(family.source_id, true) {
                    valid = false;
                }
            }
            for family in &source.partitioned_families {
                if family
                    .fragments
                    .iter()
                    .any(|fragment| contains(fragment.rect(), coord))
                    && !agrees(family.source_id, true)
                {
                    valid = false;
                }
                if let Some(member) = family
                    .legacy_members
                    .as_slice()
                    .iter()
                    .find(|member| member.coord == coord)
                    && !agrees(
                        family.source_id,
                        member.kind == crate::engine::PartitionLegacyMemberKind::SharedFamilyMember,
                    )
                {
                    valid = false;
                }
            }
            if !valid {
                return Err(ExcelError::new(ExcelErrorKind::Value)
                    .with_message("indexed source ownership mismatch"));
            }
        }
        prepared.selected_points = Some(selected_points);
        prepared.replay_records = replay_records;
        Ok(Some(prepared))
    }

    fn prepare_target_source_package(
        &mut self,
        sheet: &str,
        lease: StagedPackageLease,
        deadline: Option<std::time::Instant>,
    ) -> Result<PreparedTargetSourcePackage, ExcelError> {
        let sheet_id = self.graph.sheet_id(sheet).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("deferred source sheet not found: {sheet}"))
        })?;
        let (
            source_report,
            families,
            partitions,
            replay,
            invalidated,
            suppressed,
            consumed_members,
            consumed_engine,
            reconciliation_replay,
        ) = {
            let package = self
                .staged_formulas
                .get(sheet)
                .and_then(|staged| staged.deferred_package.as_ref())
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("staged deferred source package is unavailable")
                })?;
            if package.sheet_name != sheet {
                return Err(ExcelError::new(ExcelErrorKind::Value)
                    .with_message("deferred formula package sheet mismatch"));
            }
            (
                package.accounting_report(),
                package.families.clone(),
                package.partitioned_families.clone(),
                Arc::clone(&package.replay),
                package.invalidated.clone(),
                package.suppressed.clone(),
                package.consumed_members.clone(),
                package.consumed_engine.clone(),
                package.reconciliation_replay.clone(),
            )
        };

        let mut replay_disposition = crate::engine::FormulaReplayDisposition::default();
        for partition in &partitions {
            replay_disposition
                .register_partition(partition, false)
                .map_err(|reason| ExcelError::new(ExcelErrorKind::Value).with_message(reason))?;
        }
        replay_disposition.extend_suppressed_excel_coords(suppressed.iter().copied());
        replay_disposition
            .register_consumed_members(consumed_engine.as_ref(), consumed_members.iter().copied());
        if !replay_disposition.consumed_engine_matches(&self.source_formula_token) {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("ResidualConsumedEngineMismatch"));
        }
        self.target_preparation_checkpoint(deadline, 1)?;
        let mut replay_records = if let Some(mut records) = reconciliation_replay {
            records.retain(|record| {
                let Some((row, col)) = record.row.checked_sub(1).zip(record.col.checked_sub(1))
                else {
                    return true;
                };
                let coord = crate::engine::SourceCoord { row, col };
                let disposition = record.family.map_or_else(
                    || replay_disposition.ordinary_disposition(coord).0,
                    |family| replay_disposition.shared_disposition(family, coord),
                );
                !matches!(
                    disposition,
                    crate::engine::FormulaReplayCoordinateDisposition::Direct
                        | crate::engine::FormulaReplayCoordinateDisposition::Suppressed
                )
            });
            records
        } else {
            replay
                .lock()
                .map_err(|_| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("deferred formula spool lock poisoned")
                })?
                .replay_partitioned(&replay_disposition, &partitions)
                .map_err(|message| ExcelError::new(ExcelErrorKind::Value).with_message(message))?
        };
        replay_records.sort_by_key(|record| record.source_order);
        for chunk in replay_records.chunks(256) {
            self.target_preparation_checkpoint(deadline, chunk.len() as u64)?;
        }
        if replay_records
            .windows(2)
            .any(|records| records[0].source_order == records[1].source_order)
        {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("duplicate deferred source-order proof"));
        }

        let disposition = replay_disposition;
        let direct_families = 0usize;
        let direct_cells = 0u64;
        let direct_fragments = 0u64;
        let direct_complete_families = 0u64;
        let direct_complete_cells = 0u64;
        let direct_partition_families = 0u64;
        let direct_partition_cells = 0u64;
        let anchor_parses = 0u64;
        let anchor_asts = 0u64;
        let anchor_analyses = 0u64;

        Ok(PreparedTargetSourcePackage {
            sheet: sheet.to_string(),
            sheet_id,
            lease,
            selected_points: None,
            complete_selections: Default::default(),
            deferred_shared: false,
            direct_domains: Vec::new(),
            source_report,
            replay_records,
            spool_replays: 1,
            disposition,
            legacy: Vec::new(),
            direct_families,
            direct_cells,
            direct_fragments,
            direct_complete_families,
            direct_complete_cells,
            direct_partition_families,
            direct_partition_cells,
            anchor_parses,
            anchor_asts,
            anchor_analyses,
        })
    }

    fn fallback_planning_snapshot(
        &self,
        batch: &FormulaIngestBatch,
    ) -> Result<crate::function_registry::RegistryPlanningSnapshot, ExcelError> {
        let mut requests = Vec::new();
        for formula in &batch.formulas {
            let ast = self
                .graph
                .data_store()
                .retrieve_ast(formula.ast_id, self.graph.sheet_reg())
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("ordered fallback AST is unavailable")
                })?;
            Self::collect_planning_function_requests(&ast, &mut requests);
        }
        requests.sort();
        requests.dedup();
        crate::function_registry::RegistryPlanningSnapshot::capture_for_requests(
            &self.resolver,
            requests,
        )
        .map_err(|error| ExcelError::new(ExcelErrorKind::Value).with_message(format!("{error:?}")))
    }

    fn prepare_legacy_batch_fallback(
        &mut self,
        batch: FormulaIngestBatch,
        function_provider: &dyn crate::traits::FunctionProvider,
    ) -> Result<
        (
            crate::engine::graph::prepared_legacy_graph::PreparedLegacyGraphPlan,
            u64,
        ),
        ExcelError,
    > {
        let formula_count = batch.formulas.len() as u64;
        let sheet_id = self.graph.sheet_id_mut(&batch.sheet_name);
        let mut planned = Vec::with_capacity(batch.formulas.len());
        for record in batch.formulas {
            let placement = CellRef::new(
                sheet_id,
                Coord::from_excel(record.row, record.col, true, true),
            );
            let ingested = self
                .graph
                .ingest_pipeline(function_provider)
                .enable_function_semantics()
                .ingest_formula(
                    FormulaAstInput::RawArena(record.ast_id),
                    placement,
                    record.formula_text,
                )
                .map_err(|error| {
                    ExcelError::new(ExcelErrorKind::Value).with_message(format!("{error:?}"))
                })?;
            planned.push((record.row, record.col, ingested.ast_id, ingested.dep_plan));
        }
        let plan = self
            .graph
            .prepare_legacy_graph_plan(sheet_id, planned)
            .map_err(|error| {
                ExcelError::new(ExcelErrorKind::Value).with_message(error.to_string())
            })?;
        Ok((plan, formula_count))
    }

    fn publish_compressed_partial_report(
        &mut self,
        report: &FormulaIngestReport,
        direct_report: &FormulaIngestReport,
    ) {
        if direct_report.source_family_promoted == 0
            && direct_report.graph_formula_cells_materialized == 0
        {
            return;
        }
        let mut published = report.clone();
        published.accumulate(direct_report);
        self.record_formula_ingest_report(published);
    }

    fn finish_compressed_formula_sources(
        &mut self,
        batches: Vec<(
            FormulaIngestBatch,
            crate::engine::FormulaCompressedSourceReport,
            crate::engine::FormulaCompressedPreparation,
        )>,
    ) -> Result<FormulaIngestReport, ExcelError> {
        self.observe_function_semantic_epoch()?;
        if batches.iter().any(|(_, _, preparation)| {
            !Arc::ptr_eq(&preparation.engine_token, &self.source_formula_token)
        }) {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("compressed source preparation belongs to another engine"));
        }
        if batches.iter().any(|(fallback, _, preparation)| {
            preparation.sheet_name.as_ref() != fallback.sheet_name
        }) {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("compressed source preparation sheet mismatch"));
        }
        let initial_guard = crate::function_registry::semantic_epoch_read_guard();
        let initial_provider_revision = self.resolver.planning_semantic_revision();
        let mut fallback_batches = Vec::with_capacity(batches.len());
        let mut stale_fallback_batches = Vec::new();
        let mut pending_preparations = Vec::new();
        for (mut fallback, mut source, mut preparation) in batches {
            for formula in fallback.formulas.drain(..) {
                let source_order = formula.source_order.ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value).with_message(
                        "compressed source supplied formulas without source-order proof",
                    )
                })?;
                let text = formula.formula_text.ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value).with_message(
                        "ordered compressed fallback formula has no exact source text",
                    )
                })?;
                preparation
                    .eager_replay
                    .push(crate::engine::DeferredReplayFormula {
                        source_order,
                        row: formula.row,
                        col: formula.col,
                        text: text.to_string(),
                        family: formula.source_family,
                        partition_owner: formula.partition_owner,
                    });
            }
            preparation
                .eager_replay
                .sort_by_key(|record| record.source_order);
            source.source_spool_replays = source
                .source_spool_replays
                .saturating_add(preparation.preparation_spool_replays);
            let stale_reason = preparation
                .function_semantics_used
                .then(|| {
                    if preparation.function_provider_revision != initial_provider_revision {
                        Some("FunctionProviderRevisionChanged")
                    } else if self.prepared_function_semantics_changed(&preparation, &initial_guard)
                    {
                        Some("FunctionSemanticEpochChanged")
                    } else {
                        None
                    }
                })
                .flatten();
            let stale_semantics = stale_reason.is_some();
            for reason in preparation.rejected.values() {
                *source.fallback_reasons.entry(reason.clone()).or_default() += 1;
            }
            // No family is ever placed directly: every family replays.
            source.replay_families = source.families_seen;
            source.replay_cells = source.family_cells_seen;
            let compressed = crate::engine::FormulaCompressedSourceBatch::new(
                fallback.sheet_name.clone(),
                source,
            );
            if stale_semantics {
                stale_fallback_batches.push((fallback, compressed));
            } else {
                fallback_batches.push((fallback, compressed));
            }
            pending_preparations.push((preparation, stale_semantics));
        }
        drop(initial_guard);

        // Build known fallback graphs first. Stale batches are forced through legacy ingest.
        let configured_mode = self.config.formula_plane_mode;
        self.config.formula_plane_mode = FormulaPlaneMode::Off;
        let stale_result =
            self.ingest_compressed_formula_source_batches_inner(stale_fallback_batches, false);
        self.config.formula_plane_mode = configured_mode;
        let mut report = stale_result?;
        report.mode = configured_mode;

        self.config.formula_plane_mode = FormulaPlaneMode::Off;
        let fallback_result =
            self.ingest_compressed_formula_source_batches_inner(fallback_batches, false);
        self.config.formula_plane_mode = configured_mode;
        match fallback_result {
            Ok(fallback_report) => report.accumulate(&fallback_report),
            Err(error) => {
                self.record_formula_ingest_report(report);
                return Err(error);
            }
        }

        let mut direct_report =
            FormulaIngestReport::with_mode(FormulaPlaneMode::AuthoritativeExperimental);
        for (preparation, _) in &pending_preparations {
            direct_report.source_anchor_parses = direct_report.source_anchor_parses.saturating_add(
                preparation.clean_rejected_anchor_counts[0]
                    .saturating_add(preparation.fragmented_rejected_anchor_counts[0]),
            );
            direct_report.source_anchor_asts = direct_report.source_anchor_asts.saturating_add(
                preparation.clean_rejected_anchor_counts[1]
                    .saturating_add(preparation.fragmented_rejected_anchor_counts[1]),
            );
            direct_report.source_anchor_analyses =
                direct_report.source_anchor_analyses.saturating_add(
                    preparation.clean_rejected_anchor_counts[2]
                        .saturating_add(preparation.fragmented_rejected_anchor_counts[2]),
                );
        }
        loop {
            #[cfg(any(test, feature = "test-support"))]
            if let Some(hook) = self.before_prepared_span_commit_hook.take() {
                hook();
            }
            let commit_guard = crate::function_registry::semantic_epoch_read_guard();
            let commit_provider_revision = self.resolver.planning_semantic_revision();
            let mut newly_stale = Vec::new();
            let mut current = Vec::new();
            for pending in pending_preparations.drain(..) {
                let stale_reason = pending
                    .0
                    .function_semantics_used
                    .then(|| {
                        if pending.0.function_provider_revision != commit_provider_revision {
                            Some("FunctionProviderRevisionChanged")
                        } else if self
                            .prepared_function_semantics_changed(&pending.0, &commit_guard)
                        {
                            Some("FunctionSemanticEpochChanged")
                        } else {
                            None
                        }
                    })
                    .flatten();
                if let Some(reason) = stale_reason {
                    newly_stale.push((pending, reason));
                } else {
                    current.push(pending);
                }
            }
            if !newly_stale.is_empty() {
                drop(commit_guard);
                for ((mut preparation, was_initially_stale), reason) in newly_stale {
                    preparation
                        .eager_replay
                        .sort_by_key(|record| record.source_order);
                    direct_report.source_spool_replays =
                        direct_report.source_spool_replays.saturating_add(1);
                    if !was_initially_stale {
                        direct_report
                            .fallback_reasons
                            .entry(reason.to_string())
                            .or_default();
                    }
                    preparation.function_semantics_used = false;
                    if !preparation.eager_replay.is_empty() {
                        current.push((preparation, false));
                    }
                }
                pending_preparations = current;
                continue;
            }
            drop(commit_guard);

            // Replay fallback families in source order.
            enum SourceProposal {
                KnownFallback {
                    source_order: crate::engine::SourceFormulaOrder,
                    records: Vec<crate::engine::DeferredReplayFormula>,
                },
            }

            impl SourceProposal {
                fn source_order(&self) -> crate::engine::SourceFormulaOrder {
                    match self {
                        Self::KnownFallback { source_order, .. } => *source_order,
                    }
                }
            }

            for (mut preparation, _) in current {
                let mut proposals = Vec::with_capacity(preparation.eager_replay.len());
                let mut family_fallbacks: BTreeMap<_, Vec<_>> = BTreeMap::new();
                for record in preparation.eager_replay.drain(..) {
                    if let Some(owner) = record.partition_owner.or(record.family) {
                        family_fallbacks.entry(owner).or_default().push(record);
                    } else {
                        proposals.push(SourceProposal::KnownFallback {
                            source_order: record.source_order,
                            records: vec![record],
                        });
                    }
                }
                for (_, mut records) in family_fallbacks {
                    records.sort_by_key(|record| record.source_order);
                    if records
                        .windows(2)
                        .any(|window| window[0].source_order == window[1].source_order)
                    {
                        self.publish_compressed_partial_report(&report, &direct_report);
                        return Err(ExcelError::new(ExcelErrorKind::Value)
                            .with_message("duplicate exact-replay source-order proof"));
                    }
                    let Some(source_order) = records.first().map(|record| record.source_order)
                    else {
                        self.publish_compressed_partial_report(&report, &direct_report);
                        return Err(ExcelError::new(ExcelErrorKind::Value)
                            .with_message("empty exact-replay fallback family"));
                    };
                    proposals.push(SourceProposal::KnownFallback {
                        source_order,
                        records,
                    });
                }
                proposals.sort_by_key(SourceProposal::source_order);
                if proposals
                    .windows(2)
                    .any(|window| window[0].source_order() == window[1].source_order())
                {
                    self.publish_compressed_partial_report(&report, &direct_report);
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("ambiguous compressed source-order proof"));
                }

                for proposal in proposals {
                    match proposal {
                        SourceProposal::KnownFallback { records, .. } => {
                            let batch = match self.formula_batch_from_exact_replay(
                                preparation.sheet_name.as_ref(),
                                records,
                            ) {
                                Ok(batch) => batch,
                                Err(error) => {
                                    self.publish_compressed_partial_report(&report, &direct_report);
                                    return Err(error);
                                }
                            };
                            if batch.is_empty() {
                                continue;
                            }
                            let snapshot = match self.fallback_planning_snapshot(&batch) {
                                Ok(snapshot) => snapshot,
                                Err(error) => {
                                    self.publish_compressed_partial_report(&report, &direct_report);
                                    return Err(error);
                                }
                            };
                            let commit_guard =
                                crate::function_registry::semantic_epoch_read_guard();
                            let provider_revision_initial =
                                self.resolver.planning_semantic_revision();
                            if (commit_guard.epoch() != snapshot.epoch()
                                && snapshot.semantic_changes_affect_requests_since_guarded(
                                    &commit_guard,
                                    snapshot.epoch(),
                                ))
                                || snapshot.provider_revision().is_some_and(|revision| {
                                    Some(revision) != provider_revision_initial
                                })
                            {
                                drop(commit_guard);
                                self.publish_compressed_partial_report(&report, &direct_report);
                                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                                    "ordered fallback planning snapshot became stale",
                                ));
                            }
                            let (plan, formula_count) = match self
                                .prepare_legacy_batch_fallback(batch, &snapshot)
                            {
                                Ok(plan) => plan,
                                Err(error) => {
                                    drop(commit_guard);
                                    self.publish_compressed_partial_report(&report, &direct_report);
                                    return Err(error);
                                }
                            };
                            let provider_revision_after =
                                self.resolver.planning_semantic_revision();
                            if provider_revision_after != provider_revision_initial {
                                drop(commit_guard);
                                self.publish_compressed_partial_report(&report, &direct_report);
                                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                                    "function provider changed while preparing ordered fallback",
                                ));
                            }
                            let graph_vertices = plan.new_vertex_count();
                            let Some(graph_edges) = plan.planned_edge_count() else {
                                drop(commit_guard);
                                self.publish_compressed_partial_report(&report, &direct_report);
                                return Err(ExcelError::new(ExcelErrorKind::Value)
                                    .with_message("prepared ordered fallback size overflow"));
                            };
                            if let Err(error) = self.prepared_legacy_admission(&plan, formula_count)
                            {
                                drop(commit_guard);
                                self.publish_compressed_partial_report(&report, &direct_report);
                                return Err(error);
                            }
                            if let Err(error) =
                                self.graph.validate_prepared_legacy_graph_plan(&plan)
                            {
                                drop(commit_guard);
                                self.publish_compressed_partial_report(&report, &direct_report);
                                return Err(ExcelError::new(ExcelErrorKind::Value)
                                    .with_message(error.to_string()));
                            }
                            #[cfg(test)]
                            if let Some(hook) = self
                                .before_legacy_fallback_final_provider_sample_hook
                                .take()
                            {
                                hook();
                            }
                            let provider_revision_final =
                                self.resolver.planning_semantic_revision();
                            if provider_revision_final != provider_revision_initial {
                                drop(commit_guard);
                                self.publish_compressed_partial_report(&report, &direct_report);
                                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                                    "function provider changed after ordered fallback validation",
                                ));
                            }
                            let graph_formulas =
                                self.graph.apply_prevalidated_legacy_graph_plan(plan);
                            direct_report.formula_cells_seen = direct_report
                                .formula_cells_seen
                                .saturating_add(formula_count);
                            direct_report.graph_formula_cells_materialized = direct_report
                                .graph_formula_cells_materialized
                                .saturating_add(graph_formulas as u64);
                            direct_report.graph_vertices_created = direct_report
                                .graph_vertices_created
                                .saturating_add(graph_vertices as u64);
                            direct_report.graph_edges_created = direct_report
                                .graph_edges_created
                                .saturating_add(graph_edges as u64);
                        }
                    }
                    #[cfg(test)]
                    if let Some(hook) = self.after_eager_proposal_commit_hook.take() {
                        hook();
                    }
                }
            }
            break;
        }
        report.accumulate(&direct_report);
        self.record_formula_ingest_report(report.clone());
        Ok(report)
    }
    /// Ingest replayed per-cell formulas while preserving compressed source counters.
    pub(crate) fn ingest_compressed_formula_source_batches(
        &mut self,
        batches: Vec<(
            FormulaIngestBatch,
            crate::engine::FormulaCompressedSourceBatch,
        )>,
    ) -> Result<FormulaIngestReport, ExcelError> {
        self.ingest_compressed_formula_source_batches_inner(batches, true)
    }

    fn ingest_compressed_formula_source_batches_inner(
        &mut self,
        batches: Vec<(
            FormulaIngestBatch,
            crate::engine::FormulaCompressedSourceBatch,
        )>,
        publish_report: bool,
    ) -> Result<FormulaIngestReport, ExcelError> {
        let mut source_counts = [0_u64; 11];
        let mut source_report = crate::engine::FormulaCompressedSourceReport::default();
        let mut formula_batches = Vec::with_capacity(batches.len());
        let mut compressed_families = Vec::new();
        let mut partitioned_families = Vec::new();
        for (batch, compressed_batch) in batches {
            let (sheet_name, compressed, families, partitions) = compressed_batch.into_parts();
            if sheet_name.as_ref() != batch.sheet_name {
                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                    "compressed formula source sheet does not match its replay batch",
                ));
            }
            compressed_families.push((batch.sheet_name.clone(), families));
            partitioned_families.push((batch.sheet_name.clone(), partitions));
            source_counts[0] = source_counts[0].saturating_add(compressed.source_formula_events);
            source_counts[1] = source_counts[1].saturating_add(compressed.source_ordinary_events);
            source_counts[2] =
                source_counts[2].saturating_add(compressed.source_shared_anchor_events);
            source_counts[3] =
                source_counts[3].saturating_add(compressed.source_shared_descendant_events);
            source_counts[4] = source_counts[4].saturating_add(compressed.source_unknown_events);
            source_counts[5] =
                source_counts[5].saturating_add(compressed.source_formula_records_spooled);
            source_counts[6] =
                source_counts[6].saturating_add(compressed.source_spool_encoded_bytes);
            source_counts[7] = source_counts[7].max(compressed.source_spool_peak_memory_bytes);
            source_counts[8] =
                source_counts[8].saturating_add(compressed.source_spool_spilled_bytes);
            source_counts[9] = source_counts[9].saturating_add(compressed.source_spool_spill_files);
            source_counts[10] = source_counts[10].saturating_add(compressed.source_spool_replays);
            source_report.families_seen = source_report
                .families_seen
                .saturating_add(compressed.families_seen);
            source_report.family_cells_seen = source_report
                .family_cells_seen
                .saturating_add(compressed.family_cells_seen);
            source_report.source_clean_families = source_report
                .source_clean_families
                .saturating_add(compressed.source_clean_families);
            source_report.source_clean_cells = source_report
                .source_clean_cells
                .saturating_add(compressed.source_clean_cells);
            source_report.source_fragmentable_families = source_report
                .source_fragmentable_families
                .saturating_add(compressed.source_fragmentable_families);
            source_report.source_fragmentable_cells = source_report
                .source_fragmentable_cells
                .saturating_add(compressed.source_fragmentable_cells);
            source_report.source_fragment_count = source_report
                .source_fragment_count
                .saturating_add(compressed.source_fragment_count);
            source_report.source_isolated_fallback_cells = source_report
                .source_isolated_fallback_cells
                .saturating_add(compressed.source_isolated_fallback_cells);
            source_report.source_hole_exclusions = source_report
                .source_hole_exclusions
                .saturating_add(compressed.source_hole_exclusions);
            source_report.source_ordinary_exclusions = source_report
                .source_ordinary_exclusions
                .saturating_add(compressed.source_ordinary_exclusions);
            source_report.source_partition_failures = source_report
                .source_partition_failures
                .saturating_add(compressed.source_partition_failures);
            source_report.replay_families = source_report
                .replay_families
                .saturating_add(compressed.replay_families);
            source_report.replay_cells = source_report
                .replay_cells
                .saturating_add(compressed.replay_cells);
            source_report.forward_descendants = source_report
                .forward_descendants
                .saturating_add(compressed.forward_descendants);
            source_report.evidence_limit_fallbacks = source_report
                .evidence_limit_fallbacks
                .saturating_add(compressed.evidence_limit_fallbacks);
            source_report.evidence_peak_bytes = source_report
                .evidence_peak_bytes
                .max(compressed.evidence_peak_bytes);
            for (reason, count) in compressed.fallback_reasons {
                *source_report.fallback_reasons.entry(reason).or_default() += count;
            }
            formula_batches.push(batch);
        }
        self.ingest_formula_batches_inner(
            formula_batches,
            source_counts,
            Some(source_report),
            compressed_families,
            partitioned_families,
            publish_report,
        )
    }

    pub fn ingest_formula_batches(
        &mut self,
        batches: Vec<FormulaIngestBatch>,
    ) -> Result<FormulaIngestReport, ExcelError> {
        let has_formulas = batches.iter().any(|batch| !batch.formulas.is_empty());
        let report = self.ingest_formula_batches_inner(
            batches,
            [0; 11],
            None,
            Vec::new(),
            Vec::new(),
            true,
        )?;
        if has_formulas {
            self.mark_topology_edited();
        }
        Ok(report)
    }

    fn ingest_formula_batches_unpublished(
        &mut self,
        batches: Vec<FormulaIngestBatch>,
    ) -> Result<FormulaIngestReport, ExcelError> {
        self.ingest_formula_batches_inner(batches, [0; 11], None, Vec::new(), Vec::new(), false)
    }

    fn ingest_formula_batches_inner(
        &mut self,
        batches: Vec<FormulaIngestBatch>,
        source_counts: [u64; 11],
        source_report: Option<crate::engine::FormulaCompressedSourceReport>,
        compressed_families: Vec<(String, Vec<crate::engine::SourceFormulaFamily>)>,
        partitioned_families: Vec<(String, Vec<crate::engine::PartitionedSourceFormulaFamily>)>,
        publish_report: bool,
    ) -> Result<FormulaIngestReport, ExcelError> {
        self.observe_function_semantic_epoch()?;
        let formula_cells_seen = batches.iter().map(|batch| batch.len() as u64).sum();
        #[cfg(feature = "tracing")]
        let arena_nodes_before = self.graph.data_store().memory_usage().total_ast_nodes;
        #[cfg(feature = "tracing")]
        let route = if !partitioned_families.is_empty() {
            "partitioned"
        } else if !compressed_families.is_empty() {
            "compressed_source"
        } else if source_report.is_some() {
            "replay"
        } else {
            "ordinary"
        };
        let _ingest_span = crate::engine::trace::fz_span!(
            tracing::Level::INFO,
            "ingest",
            "ingest.batch",
            mode = ?FormulaPlaneMode::Off,
            route,
            formula_cells = formula_cells_seen
        );
        let mut report = FormulaIngestReport::with_mode(FormulaPlaneMode::Off);
        let materialize_batches = batches;
        report.formula_cells_seen = formula_cells_seen;
        report.source_formula_events = source_counts[0];
        report.source_ordinary_events = source_counts[1];
        report.source_shared_anchor_events = source_counts[2];
        report.source_shared_descendant_events = source_counts[3];
        report.source_unknown_events = source_counts[4];
        report.source_formula_records_spooled = source_counts[5];
        report.source_spool_encoded_bytes = source_counts[6];
        report.source_spool_peak_memory_bytes = source_counts[7];
        report.source_spool_spilled_bytes = source_counts[8];
        report.source_spool_spill_files = source_counts[9];
        report.source_spool_replays = source_counts[10];
        if let Some(source) = source_report {
            report.source_families_seen = source.families_seen;
            report.source_family_cells_seen = source.family_cells_seen;
            report.source_family_shadow_eligible = source.source_clean_families;
            report.source_family_shadow_eligible_cells = source.source_clean_cells;
            report.source_partitioned_families_seen = source.source_fragmentable_families;
            report.source_partition_holes = source.source_hole_exclusions;
            report.source_partition_ordinary_exceptions = source.source_ordinary_exclusions;
            report.source_partition_failures = source.source_partition_failures;
            report.source_partition_surviving_cells = source.source_fragmentable_cells;
            report.source_family_fallback = report
                .source_family_fallback
                .saturating_add(source.replay_families);
            report.source_family_fallback_cells = report
                .source_family_fallback_cells
                .saturating_add(source.replay_cells);
            report.source_forward_descendants = source.forward_descendants;
            report.source_evidence_limit_fallbacks = source.evidence_limit_fallbacks;
            report.source_evidence_peak_bytes = source.evidence_peak_bytes;
            for (reason, count) in source.fallback_reasons {
                let total = report.fallback_reasons.entry(reason).or_default();
                *total = total.saturating_add(count);
            }
        }

        // A first load without graph admission plans in the builder, one
        // chunk at a time (a load error fails the load, so the builder's
        // partial application on a planning error is unobservable).
        if self.graph.first_load_assume_new()
            && !self.graph_admission_enabled()
            && !materialize_batches.iter().all(FormulaIngestBatch::is_empty)
        {
            let mut builder =
                crate::engine::ingest_builder::BulkIngestBuilder::new(&mut self.graph);
            for batch in materialize_batches {
                if batch.is_empty() {
                    continue;
                }
                let sheet_id = builder
                    .add_sheet_checked(&batch.sheet_name)
                    .ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("unknown ingest sheet: {}", batch.sheet_name))
                    })?;
                builder.add_formula_refs(
                    sheet_id,
                    batch.formulas.into_iter().map(|record| {
                        (
                            record.row,
                            record.col,
                            crate::engine::graph::FormulaRef::of_ingested(
                                record.ast_id,
                                record.member_anchor,
                            ),
                        )
                    }),
                );
            }
            let summary = builder.finish_with_provider(&self.resolver)?;
            report.graph_formula_cells_materialized = summary.formulas as u64;
            report.graph_vertices_created = summary.vertices as u64;
            report.graph_edges_created = summary.edges as u64;
        } else if !materialize_batches.iter().all(FormulaIngestBatch::is_empty) {
            let mut prepared_by_sheet: BTreeMap<String, Vec<_>> = BTreeMap::new();
            for batch in materialize_batches {
                if batch.is_empty() {
                    continue;
                }
                let sheet_id = self.graph.sheet_id(&batch.sheet_name).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("unknown ingest sheet: {}", batch.sheet_name))
                })?;
                let mut pipeline = self.ingest_pipeline();
                // Plan each record and keep only what the graph needs (the
                // pipeline's per-formula facts are dropped at once).
                let prepared = prepared_by_sheet.entry(batch.sheet_name).or_default();
                prepared.reserve(batch.formulas.len());
                for record in batch.formulas {
                    let placement = CellRef::new(
                        sheet_id,
                        Coord::from_excel(record.row, record.col, true, true),
                    );
                    let input = match record.member_anchor {
                        Some(anchor) => FormulaAstInput::Member {
                            template: record.ast_id,
                            anchor,
                        },
                        None => FormulaAstInput::RawArena(record.ast_id),
                    };
                    let formula = pipeline.ingest_formula(input, placement, None)?;
                    prepared.push((
                        record.row,
                        record.col,
                        crate::engine::graph::FormulaRef::of_ingested(
                            formula.ast_id,
                            formula.member_anchor,
                        ),
                        formula.dep_plan,
                    ));
                }
            }
            let admission_preflighted = self.graph_admission_enabled();
            if admission_preflighted {
                let mut preview = Vec::new();
                for (sheet_name, formulas) in &prepared_by_sheet {
                    let sheet_id = self.graph.sheet_id(sheet_name).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("unknown ingest sheet: {sheet_name}"))
                    })?;
                    preview.extend(
                        formulas
                            .iter()
                            .map(|(row, col, _, plan)| (sheet_id, *row, *col, plan.clone())),
                    );
                }
                let admission = self.graph.preview_formula_mutations(&preview)?;
                self.preflight_graph_admission(admission)?;
            }

            let mut builder = self.begin_bulk_ingest();
            if admission_preflighted {
                builder.mark_admission_preflighted();
            }
            for (sheet_name, formulas) in prepared_by_sheet {
                if formulas.is_empty() {
                    continue;
                }
                let sheet_id = builder.add_sheet(&sheet_name);
                builder.add_formula_plans(sheet_id, formulas);
            }
            let summary = builder.finish()?;
            report.graph_formula_cells_materialized = summary.formulas as u64;
            report.graph_vertices_created = summary.vertices as u64;
            report.graph_edges_created = summary.edges as u64;
        }

        crate::engine::trace::fz_event!(
            tracing::Level::INFO,
            "ingest",
            "ingest.summary",
            candidate_cells = report.shadow_candidate_cells,
            accepted_span_cells = report.shadow_accepted_span_cells,
            fallback_cells = report.shadow_fallback_cells,
            spans_created = report.shadow_spans_created,
            templates_interned = report.shadow_templates_interned,
            graph_vertices_created = report.graph_vertices_created,
            graph_edges_created = report.graph_edges_created,
            arena_nodes_delta = self
                .graph
                .data_store()
                .memory_usage()
                .total_ast_nodes
                .saturating_sub(arena_nodes_before)
        );
        if publish_report {
            self.record_formula_ingest_report(report.clone());
        }
        Ok(report)
    }

    fn dedup_formula_parse_diagnostics_since(&mut self, start: usize) {
        let mut unique = Vec::new();
        for diagnostic in self.formula_parse_diagnostics.drain(start..) {
            let duplicate = unique.iter().any(|prior: &FormulaParseDiagnostic| {
                prior.sheet == diagnostic.sheet
                    && prior.row == diagnostic.row
                    && prior.col == diagnostic.col
                    && prior.formula == diagnostic.formula
                    && prior.policy == diagnostic.policy
            });
            if !duplicate {
                unique.push(diagnostic);
            }
        }
        self.formula_parse_diagnostics.extend(unique);
    }

    pub fn handle_formula_parse_error(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        formula: &str,
        message: String,
    ) -> Result<Option<ASTNode>, ExcelError> {
        let policy = self.config.formula_parse_policy;

        if policy == FormulaParsePolicy::Strict {
            let col_a1 = col_letters_from_1based(col).unwrap_or_else(|_| "?".to_string());
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "Formula parse error at {sheet}!{col_a1}{row}: {message}"
            )));
        }

        self.formula_parse_diagnostics.push(FormulaParseDiagnostic {
            sheet: sheet.to_string(),
            row,
            col,
            formula: formula.to_string(),
            message: message.clone(),
            policy,
        });

        match policy {
            FormulaParsePolicy::Strict => unreachable!(),
            FormulaParsePolicy::KeepCachedValue => Ok(None),
            FormulaParsePolicy::AsText => Ok(Some(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Text(formula.to_string())),
                None,
            ))),
            FormulaParsePolicy::CoerceToError => {
                let err = ExcelError::new(ExcelErrorKind::Error)
                    .with_message(format!("Malformed formula: {message}"));
                Ok(Some(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Error(err)),
                    None,
                )))
            }
        }
    }

    #[cfg(test)]
    fn target_preparation_fault(
        &mut self,
        seam: crate::engine::target_preparation::TargetPreparationFault,
    ) -> Result<(), ExcelError> {
        if self.target_preparation_fault_for_test == Some(seam) {
            self.target_preparation_fault_for_test = None;
            Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("injected target preparation fault: {seam:?}")))
        } else {
            Ok(())
        }
    }

    fn preparation_stale(
        reason: formualizer_common::PreparationStaleReason,
        message: impl Into<String>,
    ) -> ExcelError {
        ExcelError::new(ExcelErrorKind::Value)
            .with_message(message)
            .with_extra(formualizer_common::ExcelErrorExtra::PreparationStale { reason })
    }

    fn preparation_revision_stale_reason(
        assumptions: &crate::engine::PreparationRevision,
        current: &crate::engine::PreparationRevision,
        planning_requests: &BTreeSet<(String, String, usize)>,
        staged_leases_match: bool,
    ) -> Option<formualizer_common::PreparationStaleReason> {
        if assumptions.graph != current.graph {
            Some(formualizer_common::PreparationStaleReason::Graph)
        } else if assumptions.staged != current.staged || !staged_leases_match {
            Some(formualizer_common::PreparationStaleReason::Staged)
        } else if assumptions.symbols != current.symbols {
            Some(formualizer_common::PreparationStaleReason::Symbols)
        } else if assumptions.provider != current.provider {
            Some(formualizer_common::PreparationStaleReason::Provider)
        } else if assumptions.semantic != current.semantic
            && crate::function_registry::semantic_changes_affect_requests_since(
                assumptions.semantic,
                planning_requests.iter().cloned(),
            )
        {
            Some(formualizer_common::PreparationStaleReason::Semantic)
        } else {
            None
        }
    }

    fn preparation_revisions(&self) -> crate::engine::PreparationRevision {
        crate::engine::PreparationRevision {
            graph: self.graph.topology_revision(),
            authority: 0,
            authority_indexes: 0,
            authority_indexed_plane: 0,
            staged: self.staged_formula_index.revision(),
            symbols: self.graph.symbol_revision(),
            semantic: crate::function_registry::semantic_epoch(),
            provider: self.resolver.planning_semantic_revision(),
        }
    }

    fn planning_revision_snapshot(&self) -> PlanningRevisionSnapshot {
        let registry_guard = crate::function_registry::semantic_epoch_read_guard();
        let provider = self.resolver.planning_semantic_revision();
        let semantic = registry_guard.epoch();
        PlanningRevisionSnapshot {
            engine_topology_epoch: self.topology_epoch,
            graph_topology_revision: self.graph.topology_revision(),
            staged: self.staged_formula_index.revision(),
            symbols: self.graph.symbol_revision(),
            semantic,
            provider,
            deterministic_mode: self.config.deterministic_mode.clone(),
            budgets: self.evaluation_resource_budgets.clone(),
        }
    }

    fn recalc_plan_key(&self) -> RecalcPlanKey {
        RecalcPlanKey {
            engine_token: Arc::clone(&self.recalc_plan_token),
            revisions: self.planning_revision_snapshot(),
        }
    }

    fn plan_stale(reason: formualizer_common::PlanStaleReason) -> ExcelError {
        ExcelError::new(ExcelErrorKind::Value)
            .with_message(format!("recalculation plan is stale: {}", reason.as_str()))
            .with_extra(formualizer_common::ExcelErrorExtra::PlanStale { reason })
    }

    fn validate_recalc_plan_key(&self, key: &RecalcPlanKey) -> Result<(), ExcelError> {
        use formualizer_common::PlanStaleReason;

        if !Arc::ptr_eq(&key.engine_token, &self.recalc_plan_token) {
            return Err(Self::plan_stale(PlanStaleReason::Engine));
        }

        let current = self.planning_revision_snapshot();
        let expected = &key.revisions;
        let stale = if expected.provider != current.provider {
            Some(PlanStaleReason::Provider)
        } else if expected.semantic != current.semantic {
            Some(PlanStaleReason::Semantic)
        } else if expected.budgets != current.budgets
            || expected.deterministic_mode != current.deterministic_mode
        {
            Some(PlanStaleReason::Budget)
        } else if expected.staged != current.staged {
            Some(PlanStaleReason::Staged)
        } else if expected.symbols != current.symbols {
            Some(PlanStaleReason::Symbols)
        } else if expected.graph_topology_revision != current.graph_topology_revision
            || expected.engine_topology_epoch != current.engine_topology_epoch
        {
            Some(PlanStaleReason::Graph)
        } else {
            None
        };
        stale.map_or(Ok(()), |reason| Err(Self::plan_stale(reason)))
    }

    fn target_preparation_checkpoint(
        &mut self,
        deadline: Option<std::time::Instant>,
        work: u64,
    ) -> Result<(), ExcelError> {
        if self
            .active_cancel_flag
            .as_ref()
            .is_some_and(|cancel| cancel.is_cancelled())
        {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("target graph preparation cancelled"));
        }
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(crate::engine::ResourceLedgerError::Exhausted(
                formualizer_common::ResourceExhaustionDetail {
                    reason: formualizer_common::ResourceExhaustionReason::Deadline,
                    limit: 0,
                    observed: 1,
                    request_id: self
                        .active_evaluation_resource_request
                        .as_ref()
                        .map(|stats| stats.request_id),
                },
            )
            .into_excel_error());
        }
        self.charge_bounded_work(work)
    }

    fn opaque_reason_in_ast(
        &self,
        ast: &ASTNode,
        provider: &dyn crate::traits::FunctionProvider,
    ) -> Option<crate::engine::OpaqueReason> {
        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                let canonical = name.rsplit('.').next().unwrap_or(name).to_ascii_uppercase();
                if canonical == "INDIRECT" {
                    return Some(crate::engine::OpaqueReason::RuntimeTextReference);
                }
                let Some(function) = provider.get_function_for_planning("", name) else {
                    return Some(crate::engine::OpaqueReason::UnknownFunction);
                };
                let caps = function.caps();
                if caps.contains(FnCaps::DYNAMIC_DEPENDENCY)
                    || caps.contains(FnCaps::RETURNS_REFERENCE)
                {
                    return Some(crate::engine::OpaqueReason::DynamicReference);
                }
                args.iter()
                    .find_map(|arg| self.opaque_reason_in_ast(arg, provider))
            }
            ASTNodeType::Call { .. } => Some(crate::engine::OpaqueReason::UnknownCustomFunction),
            ASTNodeType::UnaryOp { expr, .. } => self.opaque_reason_in_ast(expr, provider),
            ASTNodeType::BinaryOp { left, right, .. } => self
                .opaque_reason_in_ast(left, provider)
                .or_else(|| self.opaque_reason_in_ast(right, provider)),
            ASTNodeType::Array(rows) => rows
                .iter()
                .flat_map(|row| row.iter())
                .find_map(|item| self.opaque_reason_in_ast(item, provider)),
            ASTNodeType::Reference {
                reference:
                    ReferenceType::Cell {
                        sheet: Some(sheet), ..
                    }
                    | ReferenceType::Range {
                        sheet: Some(sheet), ..
                    },
                ..
            } if self.graph.sheet_id(sheet).is_none() => {
                Some(crate::engine::OpaqueReason::UnresolvedCrossSheetBinding)
            }
            ASTNodeType::Reference {
                reference:
                    ReferenceType::External(_)
                    | ReferenceType::Cell3D { .. }
                    | ReferenceType::Range3D { .. },
                ..
            } => Some(crate::engine::OpaqueReason::UnresolvedCrossSheetBinding),
            ASTNodeType::Literal(_) | ASTNodeType::Omitted | ASTNodeType::Reference { .. } => None,
        }
    }

    fn target_planning_snapshot(
        &mut self,
        ast: &ASTNode,
        planning_requests: &mut BTreeSet<(String, String, usize)>,
    ) -> Result<crate::function_registry::RegistryPlanningSnapshot, ExcelError> {
        #[cfg(test)]
        if std::mem::take(&mut self.inject_target_semantic_stale_once_for_test) {
            return Err(Self::preparation_stale(
                formualizer_common::PreparationStaleReason::Semantic,
                "injected target semantic preparation movement",
            ));
        }
        #[cfg(test)]
        if let Some(hook) = self.before_target_planning_snapshot_hook.take() {
            hook();
        }
        let mut requests = Vec::new();
        Self::collect_planning_function_requests(ast, &mut requests);
        requests.sort();
        requests.dedup();
        planning_requests.extend(requests.iter().cloned());
        crate::function_registry::RegistryPlanningSnapshot::capture_for_requests(
            &self.resolver,
            requests,
        )
        .map_err(|error| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("target planning snapshot unavailable: {error:?}"))
        })
    }

    fn target_planning_snapshot_stale_reason(
        snapshot: &crate::function_registry::RegistryPlanningSnapshot,
        assumptions: &crate::engine::PreparationRevision,
    ) -> Option<formualizer_common::PreparationStaleReason> {
        if snapshot
            .provider_revision()
            .is_some_and(|revision| Some(revision) != assumptions.provider)
        {
            Some(formualizer_common::PreparationStaleReason::Provider)
        } else if snapshot.epoch() != assumptions.semantic
            && snapshot.semantic_changes_affect_requests_since(assumptions.semantic)
        {
            Some(formualizer_common::PreparationStaleReason::Semantic)
        } else {
            None
        }
    }

    fn widen_target_preparation(
        policy: crate::engine::OpaquePreparePolicy,
        scope: &mut crate::engine::PrepareScope,
        reasons: &mut Vec<crate::engine::OpaqueReason>,
        reason: crate::engine::OpaqueReason,
    ) -> Result<bool, ExcelError> {
        if policy == crate::engine::OpaquePreparePolicy::Error {
            return Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message(format!("opaque target preparation semantics: {reason:?}")));
        }
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
        if !matches!(scope, crate::engine::PrepareScope::Workbook) {
            *scope = crate::engine::PrepareScope::Workbook;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn widen_target_preparation_to_sheet(
        policy: crate::engine::OpaquePreparePolicy,
        scope: &mut crate::engine::PrepareScope,
        reasons: &mut Vec<crate::engine::OpaqueReason>,
        reason: crate::engine::OpaqueReason,
        sheet: &str,
    ) -> Result<bool, ExcelError> {
        if policy == crate::engine::OpaquePreparePolicy::Error {
            return Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message(format!("opaque target preparation semantics: {reason:?}")));
        }
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
        match scope {
            crate::engine::PrepareScope::Exact => {
                *scope = crate::engine::PrepareScope::Sheets(vec![sheet.to_string()]);
                Ok(true)
            }
            crate::engine::PrepareScope::Sheets(sheets) => {
                if sheets.iter().any(|candidate| candidate == sheet) {
                    Ok(false)
                } else {
                    sheets.push(sheet.to_string());
                    sheets.sort();
                    Ok(true)
                }
            }
            crate::engine::PrepareScope::Workbook => Ok(false),
        }
    }

    fn ast_has_proven_sheet_local_dynamic(
        ast: &ASTNode,
        provider: &dyn crate::traits::FunctionProvider,
    ) -> bool {
        fn classify(ast: &ASTNode, provider: &dyn crate::traits::FunctionProvider) -> (bool, bool) {
            match &ast.node_type {
                ASTNodeType::Function { name, args } => {
                    let Some(function) = provider.get_function_for_planning("", name) else {
                        return (false, false);
                    };
                    let caps = function.caps();
                    let dynamic = caps.contains(FnCaps::DYNAMIC_DEPENDENCY)
                        || caps.contains(FnCaps::RETURNS_REFERENCE);
                    let canonical = name.rsplit('.').next().unwrap_or(name);
                    if dynamic
                        && !canonical.eq_ignore_ascii_case("OFFSET")
                        && !canonical.eq_ignore_ascii_case("INDEX")
                    {
                        return (false, true);
                    }
                    let mut has_dynamic = dynamic;
                    for arg in args {
                        let (safe, child_dynamic) = classify(arg, provider);
                        if !safe {
                            return (false, has_dynamic || child_dynamic);
                        }
                        has_dynamic |= child_dynamic;
                    }
                    (true, has_dynamic)
                }
                ASTNodeType::UnaryOp { expr, .. } => classify(expr, provider),
                ASTNodeType::BinaryOp { left, right, .. } => {
                    let (left_safe, left_dynamic) = classify(left, provider);
                    let (right_safe, right_dynamic) = classify(right, provider);
                    (left_safe && right_safe, left_dynamic || right_dynamic)
                }
                ASTNodeType::Array(rows) => {
                    let mut has_dynamic = false;
                    for item in rows.iter().flatten() {
                        let (safe, child_dynamic) = classify(item, provider);
                        if !safe {
                            return (false, has_dynamic || child_dynamic);
                        }
                        has_dynamic |= child_dynamic;
                    }
                    (true, has_dynamic)
                }
                ASTNodeType::Reference {
                    reference:
                        ReferenceType::Cell { sheet: None, .. }
                        | ReferenceType::Range { sheet: None, .. },
                    ..
                }
                | ASTNodeType::Literal(_)
                | ASTNodeType::Omitted => (true, false),
                ASTNodeType::Call { .. } | ASTNodeType::Reference { .. } => (false, false),
            }
        }

        let (safe, dynamic) = classify(ast, provider);
        safe && dynamic
    }

    fn table_selection_region(
        &self,
        entry: &crate::engine::graph::TableEntry,
        selection: &crate::engine::TableSelection,
    ) -> Result<PreparationRegion, ExcelError> {
        let mut start_row = entry.range.start.coord.row() + 1;
        let mut end_row = entry.range.end.coord.row() + 1;
        let mut start_col = entry.range.start.coord.col() + 1;
        let mut end_col = entry.range.end.coord.col() + 1;
        match selection {
            crate::engine::TableSelection::Whole => {}
            crate::engine::TableSelection::Headers => {
                if !entry.header_row {
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message(format!("table {} has no header row", entry.name)));
                }
                end_row = start_row;
            }
            crate::engine::TableSelection::Data => {
                if entry.header_row {
                    start_row = start_row.saturating_add(1);
                }
                if entry.totals_row {
                    end_row = end_row.saturating_sub(1);
                }
            }
            crate::engine::TableSelection::Totals => {
                if !entry.totals_row {
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message(format!("table {} has no totals row", entry.name)));
                }
                start_row = end_row;
            }
            crate::engine::TableSelection::Column(column) => {
                let index = entry.col_index(column).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("unknown table column: {column}"))
                })?;
                start_col = start_col.saturating_add(index as u32);
                end_col = start_col;
            }
            crate::engine::TableSelection::Columns { start, end } => {
                let first = entry.col_index(start).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("unknown table column: {start}"))
                })?;
                let last = entry.col_index(end).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("unknown table column: {end}"))
                })?;
                if first > last {
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("table column selection is reversed"));
                }
                start_col = start_col.saturating_add(first as u32);
                end_col = entry
                    .range
                    .start
                    .coord
                    .col()
                    .saturating_add(last as u32)
                    .saturating_add(1);
            }
        }
        if start_row > end_row && matches!(selection, crate::engine::TableSelection::Data) {
            start_row = entry.range.start.coord.row() + 1;
            end_row = start_row;
        }
        if start_row > end_row || start_col > end_col {
            return Err(
                ExcelError::new(ExcelErrorKind::Value).with_message("table selection is empty")
            );
        }
        Ok(PreparationRegion {
            sheet: self.graph.sheet_name(entry.sheet_id()).to_string(),
            sheet_id: entry.sheet_id(),
            start_row,
            start_col,
            end_row,
            end_col,
        })
    }

    #[cfg(test)]
    pub(crate) fn reset_target_root_dedup_probes_for_test() {
        TARGET_ROOT_DEDUP_PROBES.with(|probes| probes.set(0));
    }

    #[cfg(test)]
    pub(crate) fn target_root_dedup_probes_for_test() -> usize {
        TARGET_ROOT_DEDUP_PROBES.with(std::cell::Cell::get)
    }

    pub(crate) fn resolve_target_producers(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
    ) -> Result<Vec<crate::engine::target_preparation::TargetProducer>, ExcelError> {
        use crate::engine::target_preparation::TargetProducer;

        let request_id = self
            .active_evaluation_resource_request
            .as_ref()
            .map(|request| request.request_id);
        let mut roots = OrderedTargetProducers::with_capacity(targets.len())
            .map_err(|_| target_root_allocation_error(targets.len(), request_id))?;
        let resolve_region = |engine: &mut Self,
                              region: Region,
                              value_only: Option<CellRef>,
                              roots: &mut OrderedTargetProducers|
         -> Result<(), ExcelError> {
            let before = roots.len();
            for anchor in engine.graph.spill_anchors_in_region(
                region.sheet_id(),
                region.axis_ranges().0.query_bounds().0,
                region.axis_ranges().1.query_bounds().0,
                region.axis_ranges().0.query_bounds().1,
                region.axis_ranges().1.query_bounds().1,
            ) {
                roots
                    .push(TargetProducer::Legacy(anchor))
                    .map_err(|_| target_root_allocation_error(roots.len() + 1, request_id))?;
            }
            for vertex in engine.graph.vertices_in_region(
                region.sheet_id(),
                region.axis_ranges().0.query_bounds().0,
                region.axis_ranges().0.query_bounds().1,
                region.axis_ranges().1.query_bounds().0,
                region.axis_ranges().1.query_bounds().1,
            ) {
                let vertex = engine
                    .graph
                    .get_cell_ref(vertex)
                    .and_then(|cell| engine.graph.spill_registry_anchor_for_cell(cell))
                    .unwrap_or(vertex);
                // `vertices_in_region` is a sheet-index query and a sheet index holds only
                // grid-addressed vertices, so a region can never yield a symbol: names,
                // tables and external sources have no position for a region to cover.
                // Symbol roots come from the by-name lookups below instead.
                if matches!(
                    engine.graph.get_vertex_kind(vertex),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                ) {
                    roots
                        .push(TargetProducer::Legacy(vertex))
                        .map_err(|_| target_root_allocation_error(roots.len() + 1, request_id))?;
                }
            }
            if roots.len() == before
                && let Some(cell) = value_only
            {
                roots
                    .push(TargetProducer::ValueOnly(cell))
                    .map_err(|_| target_root_allocation_error(roots.len() + 1, request_id))?;
            }
            Ok(())
        };

        for target in targets {
            match target {
                crate::engine::EvaluationTarget::Cell { sheet, row, col } => {
                    if *row == 0 || *col == 0 {
                        return Err(ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("target cell coordinates are one-based"));
                    }
                    let sheet_id = self.graph.sheet_id(sheet).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("target sheet not found: {sheet}"))
                    })?;
                    let cell = CellRef::new(sheet_id, Coord::from_excel(*row, *col, true, true));
                    resolve_region(
                        self,
                        Region::point(sheet_id, *row - 1, *col - 1),
                        Some(cell),
                        &mut roots,
                    )?;
                }
                crate::engine::EvaluationTarget::Range(range) => {
                    let sheet_id = self.graph.sheet_id(&range.sheet).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("target sheet not found: {}", range.sheet))
                    })?;
                    resolve_region(
                        self,
                        Region::rect(
                            sheet_id,
                            range.start_row - 1,
                            range.end_row - 1,
                            range.start_col - 1,
                            range.end_col - 1,
                        ),
                        None,
                        &mut roots,
                    )?;
                }
                crate::engine::EvaluationTarget::Name { name, scope_sheet } => {
                    let scope = self.name_query_scope(scope_sheet.as_deref())?;
                    if let Some(entry) = self.graph.resolve_name_entry_in_scope(name, scope) {
                        roots
                            .push(TargetProducer::Symbol(entry.vertex))
                            .map_err(|_| {
                                target_root_allocation_error(roots.len() + 1, request_id)
                            })?;
                    }
                }
                crate::engine::EvaluationTarget::Table { name, .. } => {
                    if let Some(entry) = self.graph.resolve_table_entry(name) {
                        roots
                            .push(TargetProducer::Symbol(entry.vertex))
                            .map_err(|_| {
                                target_root_allocation_error(roots.len() + 1, request_id)
                            })?;
                    }
                }
            }
        }
        Ok(roots.into_vec())
    }

    /// Transactionally prepare the complete ordinary staged demand closure for typed targets.
    ///
    /// This method prepares graph topology only. It does not evaluate target values.
    pub fn prepare_graph_for_targets(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        options: crate::engine::TargetEvalOptions<'_>,
    ) -> Result<crate::engine::PreparedTargetGraphReport, ExcelError> {
        let previous_budgets = self.evaluation_resource_budgets.clone();
        let diagnostics_len = self.formula_parse_diagnostics.len();
        let previous_report = self.last_formula_ingest_report.clone();
        if let Some(budgets) = options.budgets {
            self.evaluation_resource_budgets = budgets.clone();
        }
        let previous_graph_budget_override = self
            .graph
            .set_admission_budget_override(Some(self.evaluation_resource_budgets.clone()));
        // Hoist the call's cancellation onto the engine for its duration, so the
        // preparation checkpoints observe it. This is a standalone entry point, so
        // the previous value is restored rather than cleared.
        let previous_cancel = self.active_cancel_flag.take();
        self.active_cancel_flag = options.cancel.clone();
        let result = self.observe_evaluation_resource_request(
            EvaluationRequestKind::TargetPreparation,
            |engine| engine.prepare_graph_for_targets_unobserved(targets, &options),
        );
        self.active_cancel_flag = previous_cancel;
        self.graph
            .set_admission_budget_override(previous_graph_budget_override);
        self.evaluation_resource_budgets = previous_budgets;
        if result.is_err() {
            self.formula_parse_diagnostics.truncate(diagnostics_len);
            self.last_formula_ingest_report = previous_report;
        }
        result
    }

    fn prepare_graph_for_targets_unobserved(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        options: &crate::engine::TargetEvalOptions<'_>,
    ) -> Result<crate::engine::PreparedTargetGraphReport, ExcelError> {
        let scratch_checkpoint = self
            .active_resource_ledger
            .as_ref()
            .map_or(0, crate::engine::ResourceLedger::scratch_checkpoint);
        let result = self.prepare_graph_for_targets_transaction(targets, options);
        let release = self
            .active_resource_ledger
            .as_mut()
            .map_or(Ok(()), |ledger| {
                ledger.release_scratch_to(scratch_checkpoint)
            });
        match (result, release) {
            (result, Ok(())) => result,
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error.into_excel_error()),
        }
    }

    fn prepare_graph_for_targets_transaction(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        options: &crate::engine::TargetEvalOptions<'_>,
    ) -> Result<crate::engine::PreparedTargetGraphReport, ExcelError> {
        use crate::engine::{
            OpaqueReason, PreparationOutcome, PrepareScope, PreparedTargetGraphReport,
            TableSelection,
        };

        self.target_preparation_checkpoint(options.deadline, 0)?;
        self.observe_function_semantic_epoch()?;
        let assumptions = self.preparation_revisions();
        let ledger_at_start = self
            .active_resource_ledger
            .as_ref()
            .map(|ledger| ledger.snapshot());
        let diagnostics_len = self.formula_parse_diagnostics.len();
        let report_len = self.last_formula_ingest_report.clone();
        let request_id = options.request_id.or_else(|| {
            self.active_evaluation_resource_request
                .as_ref()
                .map(|stats| stats.request_id)
        });

        let mut scope = PrepareScope::Exact;
        let mut reasons = Vec::new();
        let mut regions = VecDeque::new();
        let mut deferred_shared_regions = VecDeque::new();
        let mut normalized = Vec::with_capacity(targets.len());
        let mut symbol_vertices = VecDeque::new();
        for target in targets {
            self.target_preparation_checkpoint(options.deadline, 1)?;
            match target {
                crate::engine::EvaluationTarget::Cell { sheet, row, col } => {
                    if *row == 0 || *col == 0 {
                        return Err(ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("target cell coordinates are one-based"));
                    }
                    let sheet_id = self.graph.sheet_id(sheet).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("target sheet not found: {sheet}"))
                    })?;
                    regions.push_back(PreparationRegion {
                        sheet: sheet.clone(),
                        sheet_id,
                        start_row: *row,
                        start_col: *col,
                        end_row: *row,
                        end_col: *col,
                    });
                    normalized.push(target.clone());
                }
                crate::engine::EvaluationTarget::Range(range) => {
                    if range.start_row == 0
                        || range.start_col == 0
                        || range.end_row < range.start_row
                        || range.end_col < range.start_col
                    {
                        return Err(ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("invalid target range"));
                    }
                    let sheet_id = self.graph.sheet_id(&range.sheet).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("target sheet not found: {}", range.sheet))
                    })?;
                    regions.push_back(PreparationRegion {
                        sheet: range.sheet.clone(),
                        sheet_id,
                        start_row: range.start_row,
                        start_col: range.start_col,
                        end_row: range.end_row,
                        end_col: range.end_col,
                    });
                    normalized.push(target.clone());
                }
                crate::engine::EvaluationTarget::Name { name, scope_sheet } => {
                    let name_scope = self.name_query_scope(scope_sheet.as_deref())?;
                    if let Some(entry) = self.graph.resolve_name_entry_in_scope(name, name_scope) {
                        symbol_vertices.push_back(entry.vertex);
                    } else {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnresolvedName,
                        )?;
                    }
                    normalized.push(target.clone());
                }
                crate::engine::EvaluationTarget::Table { name, selection } => {
                    if let Some(entry) = self.graph.resolve_table_entry(name) {
                        let region = self.table_selection_region(entry, selection)?;
                        symbol_vertices.push_back(entry.vertex);
                        regions.push_back(region);
                    } else {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnresolvedTable,
                        )?;
                    }
                    normalized.push(target.clone());
                }
            }
        }

        let mut visited_regions = FxHashSet::default();
        let mut visited_vertices = FxHashSet::default();
        let mut selected = FxHashSet::default();
        let mut prepared = Vec::new();
        let mut pending_diagnostics = Vec::new();
        let mut planning_requests = BTreeSet::new();
        let mut selected_cells = Vec::new();
        let mut workbook_seeded = false;
        let mut sheet_scope_seeded = BTreeSet::new();
        let mut indexed_query_sheets = FxHashSet::default();
        let mut discovery_scratch_reserved = 0u64;
        let mut package_encountered = false;
        let mut selected_package_sheets = FxHashSet::default();
        let mut selected_package_points: BTreeMap<String, BTreeSet<(u32, u32)>> = BTreeMap::new();
        let mut prepared_packages: Vec<PreparedTargetSourcePackage> = Vec::new();
        let authoritative_with_ordinary = false;
        let has_unknown_package_sheet = self
            .staged_formula_index
            .package_sheets()
            .any(|sheet| self.graph.sheet_id(sheet).is_none());

        loop {
            if let PrepareScope::Sheets(sheets) = &scope {
                for sheet in sheets.clone() {
                    if !sheet_scope_seeded.insert(sheet.clone()) {
                        continue;
                    }
                    let Some(sheet_id) = self.graph.sheet_id(&sheet) else {
                        package_encountered = true;
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnsupportedSourceSemantics,
                        )?;
                        continue;
                    };
                    for lease in self.staged_formula_index.leases_for_sheet(&sheet) {
                        self.target_preparation_checkpoint(options.deadline, 1)?;
                        regions.push_back(PreparationRegion {
                            sheet: sheet.clone(),
                            sheet_id,
                            start_row: lease.row,
                            start_col: lease.col,
                            end_row: lease.row,
                            end_col: lease.col,
                        });
                    }
                    if self
                        .staged_formula_index
                        .package_lease_for_sheet(&sheet)
                        .is_some()
                    {
                        regions.push_back(PreparationRegion {
                            sheet: sheet.clone(),
                            sheet_id,
                            start_row: 1,
                            start_col: 1,
                            end_row: self.workbook_load_limits.max_sheet_rows,
                            end_col: self.workbook_load_limits.max_sheet_cols,
                        });
                    }
                }
            }
            if matches!(scope, PrepareScope::Workbook) && !workbook_seeded {
                workbook_seeded = true;
                for (sheet, lease) in self.staged_formula_index.all_leases() {
                    self.target_preparation_checkpoint(options.deadline, 1)?;
                    let Some(sheet_id) = self.graph.sheet_id(&sheet) else {
                        package_encountered = true;
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnsupportedSourceSemantics,
                        )?;
                        continue;
                    };
                    regions.push_back(PreparationRegion {
                        sheet,
                        sheet_id,
                        start_row: lease.row,
                        start_col: lease.col,
                        end_row: lease.row,
                        end_col: lease.col,
                    });
                }
                let package_sheets = self
                    .staged_formula_index
                    .package_sheets()
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                for sheet in package_sheets {
                    let Some(sheet_id) = self.graph.sheet_id(&sheet) else {
                        package_encountered = true;
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnsupportedSourceSemantics,
                        )?;
                        continue;
                    };
                    regions.push_back(PreparationRegion {
                        sheet,
                        sheet_id,
                        start_row: 1,
                        start_col: 1,
                        end_row: self.workbook_load_limits.max_sheet_rows,
                        end_col: self.workbook_load_limits.max_sheet_cols,
                    });
                }
            }

            // Finish known ordinary/name dependency discovery before expanding
            // partial shared demands. A queued SUM may complete those families.
            let allow_partial_shared = regions.is_empty() && symbol_vertices.is_empty();
            let next_region = if allow_partial_shared {
                deferred_shared_regions.pop_front()
            } else {
                regions.pop_front()
            };
            let Some(region) = next_region else {
                if let Some(vertex) = symbol_vertices.pop_front() {
                    self.target_preparation_checkpoint(options.deadline, 1)?;
                    if !visited_vertices.insert(vertex) || !self.graph.vertex_exists(vertex) {
                        continue;
                    }
                    let vertex_is_dynamic = self.graph.is_dynamic(vertex);
                    if let Some(ast) = self.graph.get_formula(vertex) {
                        let snapshot =
                            self.target_planning_snapshot(&ast, &mut planning_requests)?;
                        if let Some(reason) =
                            Self::target_planning_snapshot_stale_reason(&snapshot, &assumptions)
                        {
                            return Err(Self::preparation_stale(
                                reason,
                                "target planning snapshot became stale during discovery",
                            ));
                        }
                        let opaque = self.opaque_reason_in_ast(&ast, &snapshot);
                        if let Some(reason) =
                            opaque.or(vertex_is_dynamic.then_some(OpaqueReason::DynamicReference))
                        {
                            if reason == OpaqueReason::DynamicReference
                                && Self::ast_has_proven_sheet_local_dynamic(&ast, &snapshot)
                            {
                                let sheet = self.graph.get_vertex_sheet_id(vertex);
                                let sheet = self.graph.sheet_name(sheet).to_string();
                                Self::widen_target_preparation_to_sheet(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    reason,
                                    &sheet,
                                )?;
                            } else {
                                Self::widen_target_preparation(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    reason,
                                )?;
                            }
                        }
                    } else if vertex_is_dynamic {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::DynamicReference,
                        )?;
                    }
                    if let Some(anchor) = self
                        .graph
                        .get_cell_ref(vertex)
                        .and_then(|cell| self.graph.spill_registry_anchor_for_cell(cell))
                    {
                        symbol_vertices.push_back(anchor);
                    }
                    if let Some(cell) = self.graph.get_cell_ref(vertex) {
                        let sheet = self.graph.sheet_name(cell.sheet_id).to_string();
                        regions.push_back(PreparationRegion {
                            sheet,
                            sheet_id: cell.sheet_id,
                            start_row: cell.coord.row() + 1,
                            start_col: cell.coord.col() + 1,
                            end_row: cell.coord.row() + 1,
                            end_col: cell.coord.col() + 1,
                        });
                    }
                    // The formula's direct precedents, from the authority:
                    // cells and ranges become regions, symbol rows (names,
                    // tables, sources) their vertices.
                    match self.graph.authority_vertex_precedents(vertex) {
                        Some(precedents) => {
                            for (sheet_id, rect) in precedents {
                                self.target_preparation_checkpoint(options.deadline, 1)?;
                                if sheet_id == crate::engine::authority::geom::SYMBOL_SHEET {
                                    for slot in rect.r0..=rect.r1 {
                                        if let Some(symbol) =
                                            self.graph.authority_host().symbols().vertex(slot)
                                        {
                                            symbol_vertices.push_back(symbol);
                                        }
                                    }
                                    continue;
                                }
                                let sheet = self.graph.sheet_name(sheet_id).to_string();
                                regions.push_back(PreparationRegion {
                                    sheet,
                                    sheet_id,
                                    start_row: rect.r0 + 1,
                                    start_col: rect.c0 + 1,
                                    end_row: (rect.r1 + 1)
                                        .min(self.workbook_load_limits.max_sheet_rows),
                                    end_col: (rect.c1 + 1)
                                        .min(self.workbook_load_limits.max_sheet_cols),
                                });
                            }
                        }
                        // The authority cannot answer (failed host): widen.
                        None => {
                            if Self::widen_target_preparation(
                                options.opaque_policy,
                                &mut scope,
                                &mut reasons,
                                OpaqueReason::UnresolvedCrossSheetBinding,
                            )? {
                                continue;
                            }
                        }
                    }
                    if let Some(name) = self.graph.named_range_by_vertex(vertex).cloned() {
                        match &name.definition {
                            NamedDefinition::Cell(cell) => regions.push_back(PreparationRegion {
                                sheet: self.graph.sheet_name(cell.sheet_id).to_string(),
                                sheet_id: cell.sheet_id,
                                start_row: cell.coord.row() + 1,
                                start_col: cell.coord.col() + 1,
                                end_row: cell.coord.row() + 1,
                                end_col: cell.coord.col() + 1,
                            }),
                            NamedDefinition::Range(range) => regions.push_back(PreparationRegion {
                                sheet: self.graph.sheet_name(range.start.sheet_id).to_string(),
                                sheet_id: range.start.sheet_id,
                                start_row: range.start.coord.row() + 1,
                                start_col: range.start.coord.col() + 1,
                                end_row: range.end.coord.row() + 1,
                                end_col: range.end.coord.col() + 1,
                            }),
                            NamedDefinition::Formula {
                                ast,
                                dependencies,
                                range_deps,
                            } => {
                                let snapshot =
                                    self.target_planning_snapshot(ast, &mut planning_requests)?;
                                if let Some(reason) = Self::target_planning_snapshot_stale_reason(
                                    &snapshot,
                                    &assumptions,
                                ) {
                                    return Err(Self::preparation_stale(
                                        reason,
                                        "target planning snapshot became stale during discovery",
                                    ));
                                }
                                if let Some(reason) = self.opaque_reason_in_ast(ast, &snapshot) {
                                    Self::widen_target_preparation(
                                        options.opaque_policy,
                                        &mut scope,
                                        &mut reasons,
                                        reason,
                                    )?;
                                }
                                for dependency in dependencies {
                                    self.target_preparation_checkpoint(options.deadline, 1)?;
                                    symbol_vertices.push_back(*dependency);
                                }
                                for range in range_deps {
                                    self.target_preparation_checkpoint(options.deadline, 1)?;
                                    // `Current` is the sheet this name's formula
                                    // was interpreted on, which is the sheet its
                                    // vertex is placed on -- the same derivation
                                    // the formula-vertex arm above uses. It is
                                    // never the workbook default sheet, and an
                                    // unresolvable `Name` widens instead of
                                    // silently landing on some other sheet.
                                    let context_sheet = self.graph.get_vertex_sheet_id(vertex);
                                    let Ok(sheet_id) =
                                        self.resolve_sheet_locator(&range.sheet, context_sheet)
                                    else {
                                        if Self::widen_target_preparation(
                                            options.opaque_policy,
                                            &mut scope,
                                            &mut reasons,
                                            OpaqueReason::UnresolvedCrossSheetBinding,
                                        )? {
                                            break;
                                        }
                                        continue;
                                    };
                                    regions.push_back(PreparationRegion {
                                        sheet: self.graph.sheet_name(sheet_id).to_string(),
                                        sheet_id,
                                        start_row: range
                                            .start_row
                                            .map_or(1, |bound| bound.index + 1),
                                        start_col: range
                                            .start_col
                                            .map_or(1, |bound| bound.index + 1),
                                        end_row: range.end_row.map_or(
                                            self.workbook_load_limits.max_sheet_rows,
                                            |bound| bound.index + 1,
                                        ),
                                        end_col: range.end_col.map_or(
                                            self.workbook_load_limits.max_sheet_cols,
                                            |bound| bound.index + 1,
                                        ),
                                    });
                                }
                            }
                            NamedDefinition::Literal(_) => {}
                        }
                    }
                    if let Some(table) = self.graph.table_by_vertex(vertex) {
                        regions
                            .push_back(self.table_selection_region(table, &TableSelection::Whole)?);
                    }
                    continue;
                }
                break;
            };

            self.target_preparation_checkpoint(options.deadline, 1)?;
            if !visited_regions.insert(region.clone()) && !allow_partial_shared {
                continue;
            }
            if let PrepareScope::Sheets(sheets) = &mut scope
                && !sheets.iter().any(|sheet| sheet == &region.sheet)
            {
                sheets.push(region.sheet.clone());
                sheets.sort();
            }
            let package_match = self.staged_formula_index.package_for_region(
                &region.sheet,
                region.start_row,
                region.start_col,
                region.end_row,
                region.end_col,
            );
            let package_lease = match package_match {
                Some(Ok(lease)) => Some(lease),
                Some(Err(()))
                    if region.start_row == 1
                        && region.start_col == 1
                        && region.end_row == self.workbook_load_limits.max_sheet_rows
                        && region.end_col == self.workbook_load_limits.max_sheet_cols =>
                {
                    self.staged_formula_index
                        .package_lease_for_sheet(&region.sheet)
                }
                Some(Err(())) => {
                    Self::widen_target_preparation(
                        options.opaque_policy,
                        &mut scope,
                        &mut reasons,
                        OpaqueReason::DeferredSourcePackage,
                    )?;
                    None
                }
                None => None,
            };
            let compatibility_before_package_replay = package_lease.is_some()
                && (authoritative_with_ordinary || has_unknown_package_sheet);
            let package_lease = if compatibility_before_package_replay {
                package_encountered = true;
                Self::widen_target_preparation(
                    options.opaque_policy,
                    &mut scope,
                    &mut reasons,
                    OpaqueReason::UnsupportedSourceSemantics,
                )?;
                None
            } else {
                package_lease
            };
            if let Some(package_lease) = package_lease
                && !selected_package_sheets.contains(&region.sheet)
            {
                self.target_preparation_checkpoint(options.deadline, 1)?;
                let mut points = self.staged_formula_index.package_points_in_region(
                    &region.sheet,
                    region.start_row,
                    region.start_col,
                    region.end_row,
                    region.end_col,
                );
                if let Some(selected) = selected_package_points.get(&region.sheet) {
                    points.retain(|point| !selected.contains(point));
                }

                if let Some(selected) = selected_package_points.get(&region.sheet) {
                    points.retain(|point| !selected.contains(point));
                }
                let permit_partial = allow_partial_shared
                    || (points.len() == 1
                        && regions.is_empty()
                        && symbol_vertices.is_empty()
                        && deferred_shared_regions.is_empty());
                let partial = self.prepare_target_exact_source_selection(
                    &region.sheet,
                    package_lease,
                    points,
                    selected_package_points
                        .get(&region.sheet)
                        .unwrap_or(&BTreeSet::new()),
                    permit_partial,
                    options.deadline,
                    &mut discovery_scratch_reserved,
                )?;
                let mut package = if let Some(mut package) = partial {
                    if package.deferred_shared {
                        deferred_shared_regions.push_back(region.clone());
                    }
                    // A later range can complete a family touched earlier in this
                    // request. Retire only that family's earlier legacy proposals;
                    // nothing has been published and its dependencies remain demanded.
                    if !package.direct_domains.is_empty() {
                        for prior in prepared_packages
                            .iter_mut()
                            .filter(|prior| prior.sheet == region.sheet)
                        {
                            prior
                                .replay_records
                                .retain(|record| !package.direct_contains(record.row, record.col));
                            prior
                                .legacy
                                .retain(|(row, col, _, _)| !package.direct_contains(*row, *col));
                            if let Some(points) = prior.selected_points.as_mut() {
                                points.retain(|&(row, col)| !package.direct_contains(row, col));
                            }
                        }
                    }
                    if selected_package_points.contains_key(&region.sheet) {
                        package.source_report = Default::default();
                    }
                    let points = package.selected_points.as_ref().unwrap();
                    if !points.is_empty() {
                        selected_package_points
                            .entry(region.sheet.clone())
                            .or_default()
                            .extend(points.iter().copied());
                    }
                    package
                } else {
                    selected_package_sheets.insert(region.sheet.clone());
                    self.prepare_target_source_package(
                        &region.sheet,
                        package_lease,
                        options.deadline,
                    )?
                };
                if package
                    .selected_points
                    .as_ref()
                    .is_none_or(|points| !points.is_empty())
                {
                    let mut final_fallback = BTreeMap::new();
                    for record in package.fallback_records() {
                        final_fallback.insert((record.row, record.col), record.clone());
                    }
                    let batch = self.formula_batch_from_exact_replay(
                        &region.sheet,
                        final_fallback.into_values(),
                    )?;
                    for record in batch.formulas {
                        self.target_preparation_checkpoint(options.deadline, 1)?;
                        let ast = self
                            .graph
                            .data_store()
                            .retrieve_ast(record.ast_id, self.graph.sheet_reg())
                            .ok_or_else(|| {
                                ExcelError::new(ExcelErrorKind::Value)
                                    .with_message("target fallback AST is unavailable")
                            })?;
                        let snapshot =
                            self.target_planning_snapshot(&ast, &mut planning_requests)?;
                        if let Some(reason) =
                            Self::target_planning_snapshot_stale_reason(&snapshot, &assumptions)
                        {
                            return Err(Self::preparation_stale(
                                reason,
                                "target fallback planning snapshot became stale during discovery",
                            ));
                        }
                        let proven_sheet_local_dynamic =
                            Self::ast_has_proven_sheet_local_dynamic(&ast, &snapshot);
                        if let Some(reason) = self.opaque_reason_in_ast(&ast, &snapshot) {
                            if reason == OpaqueReason::DynamicReference
                                && proven_sheet_local_dynamic
                            {
                                Self::widen_target_preparation_to_sheet(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    reason,
                                    &region.sheet,
                                )?;
                            } else {
                                Self::widen_target_preparation(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    reason,
                                )?;
                            }
                        }
                        let placement = CellRef::new(
                            package.sheet_id,
                            Coord::from_excel(record.row, record.col, true, true),
                        );
                        let ingested = self
                            .graph
                            .ingest_pipeline(&snapshot)
                            .enable_function_semantics()
                            .ingest_formula(
                                FormulaAstInput::RawArena(record.ast_id),
                                placement,
                                record.formula_text,
                            )?;
                        if ingested.dep_plan.dynamic {
                            if proven_sheet_local_dynamic {
                                Self::widen_target_preparation_to_sheet(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    OpaqueReason::DynamicReference,
                                    &region.sheet,
                                )?;
                            } else {
                                Self::widen_target_preparation(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    OpaqueReason::DynamicReference,
                                )?;
                            }
                        }
                        for dep in &ingested.dep_plan.direct_cell_deps {
                            self.target_preparation_checkpoint(options.deadline, 1)?;
                            regions.push_back(PreparationRegion {
                                sheet: self.graph.sheet_name(dep.sheet_id).to_string(),
                                sheet_id: dep.sheet_id,
                                start_row: dep.coord.row().saturating_add(1),
                                start_col: dep.coord.col().saturating_add(1),
                                end_row: dep.coord.row().saturating_add(1),
                                end_col: dep.coord.col().saturating_add(1),
                            });
                        }
                        for range in &ingested.dep_plan.range_deps {
                            self.target_preparation_checkpoint(options.deadline, 1)?;
                            // `Current` is the sheet the staged package's formula
                            // lives on.
                            let Ok(dependency_sheet) =
                                self.resolve_sheet_locator(&range.sheet, package.sheet_id)
                            else {
                                Self::widen_target_preparation(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    OpaqueReason::UnresolvedCrossSheetBinding,
                                )?;
                                continue;
                            };
                            regions.push_back(PreparationRegion {
                                sheet: self.graph.sheet_name(dependency_sheet).to_string(),
                                sheet_id: dependency_sheet,
                                start_row: range.start_row.map_or(1, |bound| bound.index + 1),
                                start_col: range.start_col.map_or(1, |bound| bound.index + 1),
                                end_row: range
                                    .end_row
                                    .map_or(self.workbook_load_limits.max_sheet_rows, |bound| {
                                        bound.index + 1
                                    }),
                                end_col: range
                                    .end_col
                                    .map_or(self.workbook_load_limits.max_sheet_cols, |bound| {
                                        bound.index + 1
                                    }),
                            });
                        }
                        for name in ingested
                            .dep_plan
                            .resolved_named_refs
                            .iter()
                            .chain(&ingested.dep_plan.named_refs)
                        {
                            self.target_preparation_checkpoint(options.deadline, 1)?;
                            if let Some(entry) =
                                self.graph.resolve_name_entry(name, package.sheet_id)
                            {
                                symbol_vertices.push_back(entry.vertex);
                            } else if self.graph.resolve_source_scalar_entry(name).is_none()
                                && self.graph.resolve_source_table_entry(name).is_none()
                            {
                                Self::widen_target_preparation(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    OpaqueReason::UnresolvedName,
                                )?;
                            }
                        }
                        for table in &ingested.dep_plan.table_refs {
                            self.target_preparation_checkpoint(options.deadline, 1)?;
                            if let Some(entry) = self.graph.resolve_table_entry(table) {
                                symbol_vertices.push_back(entry.vertex);
                            } else if self.graph.resolve_source_table_entry(table).is_none() {
                                Self::widen_target_preparation(
                                    options.opaque_policy,
                                    &mut scope,
                                    &mut reasons,
                                    OpaqueReason::UnresolvedTable,
                                )?;
                            }
                        }
                        package.legacy.push((
                            record.row,
                            record.col,
                            ingested.ast_id,
                            ingested.dep_plan,
                        ));
                    }
                    prepared_packages.push(package);
                }
            }
            let leases = self.staged_formula_index.leases_in_region(
                &region.sheet,
                region.start_row,
                region.start_col,
                region.end_row,
                region.end_col,
            );
            for lease in leases {
                self.target_preparation_checkpoint(options.deadline, 1)?;
                let sheet_id = self.graph.sheet_id(&region.sheet).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("staged formula sheet not found: {}", region.sheet))
                })?;
                let key = (region.sheet.clone(), lease.row, lease.col, lease.generation);
                if !selected.insert(key) {
                    continue;
                }
                let text = self
                    .staged_formulas
                    .get(&region.sheet)
                    .and_then(|sheet| sheet.get_ordinary(lease.row, lease.col))
                    .ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Value)
                            .with_message("staged formula index is stale")
                    })?
                    .to_string();
                let formula = if text.starts_with('=') {
                    text.clone()
                } else {
                    format!("={text}")
                };
                self.target_preparation_checkpoint(options.deadline, 1)?;
                let ast = match formualizer_parse::parser::parse(&formula) {
                    Ok(ast) => ast,
                    Err(error) => {
                        if self.config.formula_parse_policy == FormulaParsePolicy::Strict {
                            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                                format!(
                                    "Formula parse error at {}!{}{}: {error}",
                                    region.sheet,
                                    col_letters_from_1based(lease.col)
                                        .unwrap_or_else(|_| "?".to_string()),
                                    lease.row
                                ),
                            ));
                        }
                        pending_diagnostics.push(FormulaParseDiagnostic {
                            sheet: region.sheet.clone(),
                            row: lease.row,
                            col: lease.col,
                            formula: formula.clone(),
                            message: error.to_string(),
                            policy: self.config.formula_parse_policy,
                        });
                        match self.config.formula_parse_policy {
                            FormulaParsePolicy::KeepCachedValue => {
                                selected_cells.push(
                                    formualizer_common::RangeAddress::new(
                                        region.sheet.clone(),
                                        lease.row,
                                        lease.col,
                                        lease.row,
                                        lease.col,
                                    )
                                    .expect("selected staged coordinates are valid"),
                                );
                                prepared.push(PreparedOrdinaryStagedFormula {
                                    sheet: region.sheet.clone(),
                                    sheet_id,
                                    lease,
                                    ast_id: None,
                                    plan: None,
                                });
                                continue;
                            }
                            FormulaParsePolicy::AsText => ASTNode::new(
                                ASTNodeType::Literal(LiteralValue::Text(formula.clone())),
                                None,
                            ),
                            FormulaParsePolicy::CoerceToError => ASTNode::new(
                                ASTNodeType::Literal(LiteralValue::Error(
                                    ExcelError::new(ExcelErrorKind::Error)
                                        .with_message(format!("Malformed formula: {error}")),
                                )),
                                None,
                            ),
                            FormulaParsePolicy::Strict => unreachable!(),
                        }
                    }
                };
                self.target_preparation_checkpoint(options.deadline, 1)?;
                let snapshot = self.target_planning_snapshot(&ast, &mut planning_requests)?;
                self.target_preparation_checkpoint(options.deadline, 1)?;
                if let Some(reason) =
                    Self::target_planning_snapshot_stale_reason(&snapshot, &assumptions)
                {
                    return Err(Self::preparation_stale(
                        reason,
                        "target planning snapshot became stale during discovery",
                    ));
                }
                if let Some(reason) = self.opaque_reason_in_ast(&ast, &snapshot) {
                    if reason == OpaqueReason::DynamicReference
                        && Self::ast_has_proven_sheet_local_dynamic(&ast, &snapshot)
                    {
                        Self::widen_target_preparation_to_sheet(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            reason,
                            &region.sheet,
                        )?;
                    } else {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            reason,
                        )?;
                    }
                }
                let proven_sheet_local_dynamic =
                    Self::ast_has_proven_sheet_local_dynamic(&ast, &snapshot);
                let placement = CellRef::new(
                    sheet_id,
                    Coord::from_excel(lease.row, lease.col, true, true),
                );
                let ingested = self.graph.ingest_pipeline(&snapshot).ingest_formula(
                    FormulaAstInput::Tree(ast),
                    placement,
                    Some(Arc::from(formula)),
                )?;
                self.target_preparation_checkpoint(options.deadline, 1)?;
                if ingested.dep_plan.dynamic {
                    if proven_sheet_local_dynamic {
                        Self::widen_target_preparation_to_sheet(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::DynamicReference,
                            &region.sheet,
                        )?;
                    } else {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::DynamicReference,
                        )?;
                    }
                }
                for dep in &ingested.dep_plan.direct_cell_deps {
                    self.target_preparation_checkpoint(options.deadline, 1)?;
                    regions.push_back(PreparationRegion {
                        sheet: self.graph.sheet_name(dep.sheet_id).to_string(),
                        sheet_id: dep.sheet_id,
                        start_row: dep.coord.row() + 1,
                        start_col: dep.coord.col() + 1,
                        end_row: dep.coord.row() + 1,
                        end_col: dep.coord.col() + 1,
                    });
                }
                for range in &ingested.dep_plan.range_deps {
                    self.target_preparation_checkpoint(options.deadline, 1)?;
                    // `Current` is the sheet the staged formula lives on.
                    let Ok(dependency_sheet) = self.resolve_sheet_locator(&range.sheet, sheet_id)
                    else {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnresolvedCrossSheetBinding,
                        )?;
                        continue;
                    };
                    regions.push_back(PreparationRegion {
                        sheet: self.graph.sheet_name(dependency_sheet).to_string(),
                        sheet_id: dependency_sheet,
                        start_row: range.start_row.map_or(1, |bound| bound.index + 1),
                        start_col: range.start_col.map_or(1, |bound| bound.index + 1),
                        end_row: range
                            .end_row
                            .map_or(self.workbook_load_limits.max_sheet_rows, |bound| {
                                bound.index + 1
                            }),
                        end_col: range
                            .end_col
                            .map_or(self.workbook_load_limits.max_sheet_cols, |bound| {
                                bound.index + 1
                            }),
                    });
                }
                for name in ingested
                    .dep_plan
                    .resolved_named_refs
                    .iter()
                    .chain(&ingested.dep_plan.named_refs)
                {
                    self.target_preparation_checkpoint(options.deadline, 1)?;
                    if let Some(entry) = self.graph.resolve_name_entry(name, sheet_id) {
                        symbol_vertices.push_back(entry.vertex);
                    } else if self.graph.resolve_source_scalar_entry(name).is_none()
                        && self.graph.resolve_source_table_entry(name).is_none()
                    {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnresolvedName,
                        )?;
                    }
                }
                for table in &ingested.dep_plan.table_refs {
                    self.target_preparation_checkpoint(options.deadline, 1)?;
                    if let Some(entry) = self.graph.resolve_table_entry(table) {
                        symbol_vertices.push_back(entry.vertex);
                    } else if self.graph.resolve_source_table_entry(table).is_none() {
                        Self::widen_target_preparation(
                            options.opaque_policy,
                            &mut scope,
                            &mut reasons,
                            OpaqueReason::UnresolvedTable,
                        )?;
                    }
                }
                selected_cells.push(
                    formualizer_common::RangeAddress::new(
                        region.sheet.clone(),
                        lease.row,
                        lease.col,
                        lease.row,
                        lease.col,
                    )
                    .expect("selected staged coordinates are valid"),
                );
                prepared.push(PreparedOrdinaryStagedFormula {
                    sheet: region.sheet.clone(),
                    sheet_id,
                    lease,
                    ast_id: Some(ingested.ast_id),
                    plan: Some(ingested.dep_plan),
                });
            }

            if indexed_query_sheets.insert(region.sheet_id) {
                self.graph.prepare_sheet_index_for_query(region.sheet_id);
                let bytes = (self.graph.sheet_index_vertex_count(region.sheet_id) as u64)
                    .saturating_mul(32);
                self.reserve_graph_source_scratch(bytes)?;
                discovery_scratch_reserved = discovery_scratch_reserved.saturating_add(bytes);
            }
            let spill_anchors = self.graph.spill_anchors_in_region(
                region.sheet_id,
                region.start_row - 1,
                region.start_col - 1,
                region.end_row - 1,
                region.end_col - 1,
            );
            for anchor in spill_anchors {
                self.target_preparation_checkpoint(options.deadline, 1)?;
                symbol_vertices.push_back(anchor);
            }
            let vertices = self.graph.vertices_in_region(
                region.sheet_id,
                region.start_row - 1,
                region.end_row - 1,
                region.start_col - 1,
                region.end_col - 1,
            );
            for vertex in vertices {
                self.target_preparation_checkpoint(options.deadline, 1)?;
                symbol_vertices.push_back(vertex);
            }
        }

        #[cfg(test)]
        self.target_preparation_fault(
            crate::engine::target_preparation::TargetPreparationFault::AfterDiscovery,
        )?;

        if package_encountered {
            Self::widen_target_preparation(
                options.opaque_policy,
                &mut scope,
                &mut reasons,
                OpaqueReason::UnsupportedSourceSemantics,
            )?;
            self.target_preparation_checkpoint(options.deadline, 0)?;
            let selected_count = self.staged_formula_count();
            let selected_packages = self
                .staged_formulas
                .values()
                .filter_map(|staged| staged.deferred_package.as_ref())
                .map(|package| package.families.len() + package.partitioned_families.len())
                .sum();
            self.formula_parse_diagnostics.truncate(diagnostics_len);
            self.last_formula_ingest_report = report_len;
            #[cfg(test)]
            if let Some(hook) = self.before_target_preparation_commit_hook.take() {
                hook();
            }
            self.target_preparation_checkpoint(options.deadline, 0)?;
            #[cfg(test)]
            self.target_preparation_fault(
                crate::engine::target_preparation::TargetPreparationFault::FinalRevisionValidation,
            )?;
            let current_revisions = self.preparation_revisions();
            if let Some(reason) = Self::preparation_revision_stale_reason(
                &assumptions,
                &current_revisions,
                &planning_requests,
                true,
            ) {
                return Err(Self::preparation_stale(
                    reason,
                    "target compatibility preparation plan is stale",
                ));
            }
            #[cfg(test)]
            self.target_preparation_fault(
                crate::engine::target_preparation::TargetPreparationFault::FinalGraphValidation,
            )?;
            let commit_work_before = self
                .active_resource_ledger
                .as_ref()
                .map_or(0, |ledger| ledger.snapshot().work_charged);
            let commit_started = crate::instant::FzInstant::now();
            self.build_graph_all_unobserved()?;
            let commit_window = commit_started.elapsed();
            let ledger_after = self
                .active_resource_ledger
                .as_ref()
                .map(|ledger| ledger.snapshot());
            let actual_commit_work = ledger_after
                .map_or(0, |snapshot| snapshot.work_charged)
                .saturating_sub(commit_work_before);
            let observed_scratch_bytes = ledger_after
                .map_or(0, |snapshot| snapshot.scratch_peak)
                .saturating_sub(ledger_at_start.map_or(0, |snapshot| snapshot.scratch_current));
            let revisions = assumptions.clone();
            let report = PreparedTargetGraphReport {
                request_id: request_id.unwrap_or_default(),
                requested_targets: targets.len(),
                normalized_regions: visited_regions.len(),
                normalized_target_list: normalized,
                selected_staged_cells: selected_count,
                selected_source_families: selected_packages,
                retained_staged_cells: self.staged_formula_count(),
                selected_cells,
                retained_cells: Vec::new(),
                widened_scope: PrepareScope::Workbook,
                widening_reasons: reasons,
                revisions,
                commit_window,
                estimated_scratch_bytes: discovery_scratch_reserved
                    .saturating_add((selected_count as u64).saturating_mul(256)),
                observed_scratch_bytes,
                estimated_commit_work: selected_count as u64,
                actual_commit_work,
                outcome: PreparationOutcome::CompatibilityPrepared,
            };
            self.observe_target_preparation_report(&report);
            return Ok(report);
        }

        prepared.sort_by_key(|formula| formula.lease.insertion_order);
        for package in &prepared_packages {
            if let Some(points) = &package.selected_points {
                selected_cells.extend(points.iter().filter_map(|&(row, col)| {
                    formualizer_common::RangeAddress::new(&package.sheet, row, col, row, col).ok()
                }));
                continue;
            }
            selected_cells.extend(package.replay_records.iter().filter_map(|record| {
                formualizer_common::RangeAddress::new(
                    package.sheet.clone(),
                    record.row,
                    record.col,
                    record.row,
                    record.col,
                )
                .ok()
            }));
        }
        let (legacy_graph, planned_formula_count) =
            self.prepare_target_combined_legacy_graph(&prepared_packages, &prepared)?;
        let new_vertices = legacy_graph.new_vertex_count();
        let new_edges = legacy_graph.planned_edge_count().ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::NImpl).with_message("target graph edge count overflow")
        })?;
        let removed_edges = legacy_graph.removed_edge_count().ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::NImpl).with_message("target graph edge count overflow")
        })?;
        let current = self.graph.baseline_stats();
        let final_vertices = current
            .graph_vertex_count
            .checked_add(new_vertices)
            .ok_or_else(|| {
                crate::engine::ResourceLedgerError::Exhausted(
                    formualizer_common::ResourceExhaustionDetail {
                        reason: formualizer_common::ResourceExhaustionReason::ArithmeticOverflow,
                        limit: u64::MAX,
                        observed: u64::MAX,
                        request_id,
                    },
                )
                .into_excel_error()
            })?;
        let final_edges = current
            .graph_edge_count
            .checked_sub(removed_edges)
            .and_then(|count| count.checked_add(new_edges))
            .ok_or_else(|| {
                crate::engine::ResourceLedgerError::Exhausted(
                    formualizer_common::ResourceExhaustionDetail {
                        reason: formualizer_common::ResourceExhaustionReason::ArithmeticOverflow,
                        limit: u64::MAX,
                        observed: u64::MAX,
                        request_id,
                    },
                )
                .into_excel_error()
            })?;
        #[cfg(test)]
        self.target_preparation_fault(
            crate::engine::target_preparation::TargetPreparationFault::Admission,
        )?;
        let resource = |reason, limit: u64, observed: u64| {
            crate::engine::ResourceLedgerError::Exhausted(
                formualizer_common::ResourceExhaustionDetail {
                    reason,
                    limit,
                    observed,
                    request_id,
                },
            )
            .into_excel_error()
        };
        let admission = crate::engine::resource_ledger::GraphAdmission {
            final_vertices,
            final_edges,
            materialization_cells: planned_formula_count as u64,
            added_vertices: new_vertices,
            added_edges: new_edges,
        };
        let materialized_bytes = admission
            .materialized_graph_bytes()
            .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        if let Err(error) = self.preflight_graph_admission(admission) {
            if let formualizer_common::ExcelErrorExtra::Resource { detail } = &error.extra {
                self.observe_target_admission_failure(detail.reason);
            }
            return Err(error);
        }
        let selected_package_records = prepared_packages
            .iter()
            .map(|package| package.replay_records.len() as u64)
            .sum::<u64>();
        let planned_working_bytes = (prepared.len() as u64)
            .saturating_add(selected_package_records)
            .saturating_mul(256)
            .saturating_add((visited_regions.len() as u64).saturating_mul(128))
            .saturating_add((visited_vertices.len() as u64).saturating_mul(32))
            .saturating_add((selected.len() as u64).saturating_mul(96))
            .saturating_add(materialized_bytes);
        let residual_scratch = selected_package_points
            .iter()
            .map(|(sheet, points)| {
                let source = self
                    .staged_formulas
                    .get(sheet)
                    .unwrap()
                    .deferred_package
                    .as_ref()
                    .unwrap();
                (source.families.len() as u64)
                    .saturating_mul(256)
                    .saturating_add(
                        source
                            .partitioned_families
                            .iter()
                            .map(|family| {
                                256u64
                                    .saturating_add(family.fragments.len() as u64 * 32)
                                    .saturating_add(family.legacy_members.len() as u64 * 32)
                            })
                            .sum::<u64>(),
                    )
                    // Direct complete domains need coordinate suppression, not
                    // a per-member residual split/proof or legacy AST reserve.
                    .saturating_add(points.len() as u64 * 32)
                    .saturating_add(
                        prepared_packages
                            .iter()
                            .filter(|package| &package.sheet == sheet)
                            .map(|package| package.replay_records.len() as u64 * 480)
                            .sum::<u64>(),
                    )
            })
            .sum::<u64>();
        let scratch_bytes = discovery_scratch_reserved
            .saturating_add(planned_working_bytes)
            .saturating_add(residual_scratch);
        let remaining_scratch = scratch_bytes.saturating_sub(discovery_scratch_reserved);
        if let Err(error) = self.reserve_graph_source_scratch(remaining_scratch) {
            self.observe_target_admission_failure(
                formualizer_common::ResourceExhaustionReason::ScratchMemory,
            );
            return Err(error);
        }

        let mut residual_sources = BTreeMap::new();
        let mut residual_owners = BTreeMap::new();
        for (sheet, points) in &selected_package_points {
            let selected: BTreeMap<_, BTreeSet<_>> = prepared_packages
                .iter()
                .filter(|package| &package.sheet == sheet)
                .flat_map(|package| &package.replay_records)
                .filter_map(|record| {
                    record.partition_owner.or(record.family).map(|owner| {
                        (
                            owner,
                            crate::engine::SourceCoord {
                                row: record.row - 1,
                                col: record.col - 1,
                            },
                        )
                    })
                })
                .fold(BTreeMap::new(), |mut map, (owner, coord)| {
                    map.entry(owner).or_default().insert(coord);
                    map
                });
            let metadata_work = self
                .staged_formulas
                .get(sheet)
                .unwrap()
                .deferred_package
                .as_ref()
                .map_or(0, |source| {
                    source.families.len() as u64
                        + source
                            .partitioned_families
                            .iter()
                            .map(|family| {
                                1 + family.fragments.len() as u64 * family.fragments.len() as u64
                                    + family.legacy_members.len() as u64
                            })
                            .sum::<u64>()
                });
            let split_work = selected
                .values()
                .map(|points| points.len() as u64 * 128)
                .sum::<u64>();
            self.target_preparation_checkpoint(
                options.deadline,
                metadata_work
                    .saturating_add(split_work)
                    .saturating_add(points.len() as u64),
            )?;
            let source = self
                .staged_formulas
                .get(sheet)
                .unwrap()
                .deferred_package
                .as_ref()
                .unwrap();
            let complete: BTreeSet<_> = prepared_packages
                .iter()
                .filter(|package| &package.sheet == sheet)
                .flat_map(|package| package.complete_selections.iter().copied())
                .collect();
            let residual = source
                .residual_sources(&selected, &complete, &self.workbook_load_limits)
                .map_err(|reason| ExcelError::new(ExcelErrorKind::Value).with_message(reason))?;
            residual_owners.insert(
                sheet.clone(),
                residual
                    .1
                    .iter()
                    .map(|family| family.source_id)
                    .collect::<BTreeSet<_>>(),
            );
            residual_sources.insert(sheet.clone(), residual);
        }

        // Reserve residual suppression before the revision-validated commit window.
        // Hash-set insertion and point-index removal below cannot allocate.
        for (sheet, points) in &selected_package_points {
            let package = self
                .staged_formulas
                .get_mut(sheet)
                .unwrap()
                .deferred_package
                .as_mut()
                .unwrap();
            let owner_count = prepared_packages
                .iter()
                .filter(|p| &p.sheet == sheet)
                .flat_map(|p| &p.replay_records)
                .filter_map(|record| record.partition_owner.or(record.family))
                .filter(|owner| residual_owners[sheet].contains(owner))
                .count();
            package
                .consumed_members
                .try_reserve(owner_count)
                .map_err(|_| {
                    resource(
                        formualizer_common::ResourceExhaustionReason::ScratchMemory,
                        0,
                        owner_count as u64 * 24,
                    )
                })?;
            package.suppressed.try_reserve(points.len()).map_err(|_| {
                resource(
                    formualizer_common::ResourceExhaustionReason::ScratchMemory,
                    0,
                    points.len() as u64 * 16,
                )
            })?;
        }

        let estimated_commit_duration = std::time::Duration::from_nanos(
            (new_vertices as u64)
                .saturating_add(new_edges as u64)
                .saturating_add(prepared.len() as u64)
                .saturating_add(selected_package_records)
                .max(1)
                .saturating_mul(100),
        );
        if options.deadline.is_some_and(|deadline| {
            std::time::Instant::now()
                .checked_add(estimated_commit_duration)
                .is_none_or(|finish| finish > deadline)
        }) {
            self.observe_target_admission_failure(
                formualizer_common::ResourceExhaustionReason::Deadline,
            );
            return Err(resource(
                formualizer_common::ResourceExhaustionReason::Deadline,
                0,
                1,
            ));
        }
        #[cfg(test)]
        if let Some(hook) = self.before_target_preparation_commit_hook.take() {
            hook();
        }
        self.target_preparation_checkpoint(options.deadline, 0)?;
        #[cfg(test)]
        self.target_preparation_fault(
            crate::engine::target_preparation::TargetPreparationFault::FinalRevisionValidation,
        )?;
        let current_revisions = self.preparation_revisions();
        let staged_leases_match = prepared.iter().all(|formula| {
            self.staged_formula_index
                .lease_matches(&formula.sheet, formula.lease)
        }) && prepared_packages.iter().all(|package| {
            self.staged_formula_index
                .package_lease_matches(&package.sheet, package.lease)
        });
        let stale_reason = Self::preparation_revision_stale_reason(
            &assumptions,
            &current_revisions,
            &planning_requests,
            staged_leases_match,
        );
        if let Some(reason) = stale_reason {
            return Err(Self::preparation_stale(
                reason,
                "target graph preparation plan is stale",
            ));
        }
        #[cfg(test)]
        self.target_preparation_fault(
            crate::engine::target_preparation::TargetPreparationFault::FinalGraphValidation,
        )?;
        self.graph
            .validate_prepared_legacy_graph_plan(&legacy_graph)
            .map_err(|error| {
                Self::preparation_stale(
                    formualizer_common::PreparationStaleReason::Graph,
                    format!("target graph preparation plan is stale: {error}"),
                )
            })?;
        #[cfg(test)]
        self.target_preparation_fault(
            crate::engine::target_preparation::TargetPreparationFault::Reservation,
        )?;
        self.graph.reserve_prepared_legacy_graph_plan(&legacy_graph);
        self.formula_parse_diagnostics
            .try_reserve(pending_diagnostics.len())
            .map_err(|_| {
                resource(
                    formualizer_common::ResourceExhaustionReason::Admission,
                    pending_diagnostics.len() as u64,
                    pending_diagnostics.len() as u64,
                )
            })?;
        self.target_preparation_checkpoint(options.deadline, 0)?;
        #[cfg(test)]
        self.target_preparation_fault(
            crate::engine::target_preparation::TargetPreparationFault::BeforeFirstMutation,
        )?;

        let commit_started = crate::instant::FzInstant::now();
        let committed = self
            .graph
            .apply_prevalidated_legacy_graph_plan(legacy_graph);
        for formula in &prepared {
            let removed = self
                .staged_formulas
                .get_mut(&formula.sheet)
                .and_then(|sheet| sheet.remove_ordinary(formula.lease.row, formula.lease.col));
            debug_assert!(removed.is_some());
            let index_removed = self.staged_formula_index.remove(
                &formula.sheet,
                formula.lease.row,
                formula.lease.col,
            );
            debug_assert!(index_removed);
        }
        for package in &prepared_packages {
            if let Some(points) = &package.selected_points {
                let staged = self.staged_formulas.get_mut(&package.sheet).unwrap();
                let source = staged.deferred_package.as_mut().unwrap();
                source.consumed_engine = Some(Arc::clone(&self.source_formula_token));
                // The revision-validated graph commit establishes this exact source
                // ownership proof. Later edits retain the exclusion, not old text.
                source
                    .consumed_members
                    .extend(package.replay_records.iter().filter_map(|record| {
                        record
                            .partition_owner
                            .or(record.family)
                            .filter(|owner| residual_owners[&package.sheet].contains(owner))
                            .map(|owner| {
                                (
                                    owner,
                                    crate::engine::SourceCoord {
                                        row: record.row - 1,
                                        col: record.col - 1,
                                    },
                                )
                            })
                    }));
                source.suppressed.extend(points.iter().copied());
                source.source_accounted = true;
                self.staged_formula_index
                    .consume_package_points(&package.sheet, points);
                if source.suppressed.len() >= source.source_coordinates.len() {
                    staged.deferred_package = None;
                    self.staged_formula_index.set_package(&package.sheet, None);
                }
            } else {
                let removed = self
                    .staged_formulas
                    .get_mut(&package.sheet)
                    .and_then(|staged| staged.deferred_package.take());
                debug_assert!(removed.is_some());
                self.staged_formula_index.set_package(&package.sheet, None);
            }
        }
        for (sheet, (families, partitions)) in residual_sources {
            if let Some(source) = self
                .staged_formulas
                .get_mut(&sheet)
                .and_then(|staged| staged.deferred_package.as_mut())
            {
                source.families = families;
                source.partitioned_families = partitions;
                source
                    .consumed_members
                    .retain(|(owner, _)| residual_owners[&sheet].contains(owner));
                self.staged_formula_index.update_package_family_count(
                    &sheet,
                    source.families.len() + source.partitioned_families.len(),
                );
            }
        }
        let empty_sheets = self
            .staged_formulas
            .iter()
            .filter_map(|(sheet, staged)| staged.is_empty().then_some(sheet.clone()))
            .collect::<Vec<_>>();
        for sheet in empty_sheets {
            self.staged_formulas.remove(&sheet);
        }
        if committed > 0 {
            self.mark_topology_edited();
        }
        self.formula_parse_diagnostics.extend(pending_diagnostics);
        if !prepared.is_empty() || !prepared_packages.is_empty() {
            let mut ingest_delta = FormulaIngestReport::with_mode(FormulaPlaneMode::Off);
            ingest_delta.formula_cells_seen = (prepared.len() as u64).saturating_add(
                prepared_packages
                    .iter()
                    .map(|package| {
                        (package.replay_records.len() as u64).saturating_add(
                            if package.selected_points.is_some() {
                                package.direct_cells
                            } else {
                                0
                            },
                        )
                    })
                    .sum::<u64>(),
            );
            ingest_delta.graph_formula_cells_materialized = committed as u64;
            ingest_delta.graph_vertices_created = new_vertices as u64;
            ingest_delta.graph_edges_created = new_edges as u64;
            for package in &prepared_packages {
                let source = &package.source_report;
                ingest_delta.source_formula_events = ingest_delta
                    .source_formula_events
                    .saturating_add(source.source_formula_events);
                ingest_delta.source_formula_records_spooled = ingest_delta
                    .source_formula_records_spooled
                    .saturating_add(source.source_formula_records_spooled);
                ingest_delta.source_spool_encoded_bytes = ingest_delta
                    .source_spool_encoded_bytes
                    .saturating_add(source.source_spool_encoded_bytes);
                ingest_delta.source_spool_peak_memory_bytes = ingest_delta
                    .source_spool_peak_memory_bytes
                    .max(source.source_spool_peak_memory_bytes);
                ingest_delta.source_spool_spilled_bytes = ingest_delta
                    .source_spool_spilled_bytes
                    .saturating_add(source.source_spool_spilled_bytes);
                ingest_delta.source_spool_spill_files = ingest_delta
                    .source_spool_spill_files
                    .saturating_add(source.source_spool_spill_files);
                ingest_delta.source_spool_replays = ingest_delta
                    .source_spool_replays
                    .saturating_add(source.source_spool_replays)
                    .saturating_add(package.spool_replays);
                ingest_delta.source_families_seen = ingest_delta
                    .source_families_seen
                    .saturating_add(source.families_seen);
                ingest_delta.source_family_cells_seen = ingest_delta
                    .source_family_cells_seen
                    .saturating_add(source.family_cells_seen);
                ingest_delta.source_family_shadow_eligible = ingest_delta
                    .source_family_shadow_eligible
                    .saturating_add(source.source_clean_families);
                ingest_delta.source_family_shadow_eligible_cells = ingest_delta
                    .source_family_shadow_eligible_cells
                    .saturating_add(source.source_clean_cells);
                ingest_delta.source_partitioned_families_seen = ingest_delta
                    .source_partitioned_families_seen
                    .saturating_add(source.source_fragmentable_families);
                ingest_delta.source_partition_holes = ingest_delta
                    .source_partition_holes
                    .saturating_add(source.source_hole_exclusions);
                ingest_delta.source_partition_ordinary_exceptions = ingest_delta
                    .source_partition_ordinary_exceptions
                    .saturating_add(source.source_ordinary_exclusions);
                ingest_delta.source_partition_surviving_cells = ingest_delta
                    .source_partition_surviving_cells
                    .saturating_add(source.source_fragmentable_cells);
                for (reason, count) in &source.fallback_reasons {
                    let total = ingest_delta
                        .fallback_reasons
                        .entry(reason.clone())
                        .or_default();
                    *total = total.saturating_add(*count);
                }

                ingest_delta.source_family_fallback = ingest_delta
                    .source_family_fallback
                    .saturating_add(source.families_seen);
                ingest_delta.source_family_fallback_cells = ingest_delta
                    .source_family_fallback_cells
                    .saturating_add(source.family_cells_seen);
            }
            self.record_formula_ingest_report(ingest_delta);
        }
        let commit_window = commit_started.elapsed();
        let retained_cells = self
            .staged_formula_index
            .all_leases()
            .into_iter()
            .filter_map(|(sheet, lease)| {
                formualizer_common::RangeAddress::new(
                    sheet, lease.row, lease.col, lease.row, lease.col,
                )
                .ok()
            })
            .collect::<Vec<_>>();
        let observed_scratch_bytes = self
            .active_resource_ledger
            .as_ref()
            .map_or(0, |ledger| ledger.snapshot().scratch_peak)
            .saturating_sub(ledger_at_start.map_or(0, |snapshot| snapshot.scratch_current));
        let committed_spans = 0u64;
        let selected_source_families = prepared_packages
            .iter()
            .filter(|package| package.selected_points.is_none())
            .map(|package| package.lease.family_count)
            .sum::<usize>()
            + prepared_packages
                .iter()
                .filter(|package| package.selected_points.is_some())
                .flat_map(|package| package.replay_records.iter())
                .filter_map(|record| record.partition_owner.or(record.family))
                .chain(
                    prepared_packages
                        .iter()
                        .flat_map(|package| package.complete_selections.iter().copied()),
                )
                .collect::<BTreeSet<_>>()
                .len();
        let selected_staged_cells = prepared.len().saturating_add(
            prepared_packages
                .iter()
                .map(|package| {
                    package
                        .selected_points
                        .as_ref()
                        .map_or(package.replay_records.len(), BTreeSet::len)
                })
                .sum::<usize>(),
        );
        let actual_commit_work = (new_vertices as u64)
            .saturating_add(new_edges as u64)
            .saturating_add(committed as u64)
            .saturating_add(committed_spans)
            .saturating_add(prepared.len() as u64)
            .saturating_add(
                prepared_packages
                    .iter()
                    .map(|package| {
                        package
                            .selected_points
                            .as_ref()
                            .map_or(1, |points| points.len() as u64)
                    })
                    .sum::<u64>(),
            );
        let report = PreparedTargetGraphReport {
            request_id: request_id.unwrap_or_default(),
            requested_targets: targets.len(),
            normalized_regions: visited_regions.len(),
            normalized_target_list: normalized,
            selected_staged_cells,
            selected_source_families,
            retained_staged_cells: self.staged_formula_count(),
            selected_cells,
            retained_cells,
            widened_scope: scope,
            widening_reasons: reasons,
            revisions: assumptions,
            commit_window,
            estimated_scratch_bytes: scratch_bytes,
            observed_scratch_bytes,
            estimated_commit_work: (new_vertices as u64)
                .saturating_add(new_edges as u64)
                .saturating_add(prepared.len() as u64)
                .saturating_add(selected_package_records),
            actual_commit_work,
            outcome: PreparationOutcome::Prepared,
        };
        self.observe_target_preparation_report(&report);
        Ok(report)
    }

    /// Build graph for all staged formulas.
    pub fn build_graph_all(&mut self) -> Result<(), formualizer_parse::ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::Full, |engine| {
            engine.build_graph_all_unobserved()
        })
    }

    fn build_graph_all_unobserved(&mut self) -> Result<(), formualizer_parse::ExcelError> {
        let selected = self.staged_formula_count();
        let started = crate::instant::FzInstant::now();
        self.resource_checkpoint(selected as u64)?;
        let scratch_bytes = (selected as u64).saturating_mul(256);
        let result = self.with_request_scratch(scratch_bytes, |engine| {
            let index_snapshot = engine.staged_formula_index.clone();
            let collected = std::mem::take(&mut engine.staged_formulas)
                .into_iter()
                .collect();
            engine.staged_formula_index.clear_all();
            engine.build_graph_from_staged_batches(collected, false, index_snapshot)
        });
        self.observe_staged_preparation(selected, self.staged_formula_count(), started.elapsed());
        result
    }

    /// Build graph for specific sheets (consuming only those staged entries).
    pub fn build_graph_for_sheets<'a, I: IntoIterator<Item = &'a str>>(
        &mut self,
        sheets: I,
    ) -> Result<(), formualizer_parse::ExcelError> {
        let mut sheets = sheets.into_iter();
        self.observe_evaluation_resource_request(EvaluationRequestKind::Targeted, move |engine| {
            let name_scratch = (sheets.size_hint().0 as u64).saturating_mul(64);
            engine.with_request_scratch(name_scratch, |engine| {
                // Allocation failure follows the baseline process-fatal policy; it is not a
                // recoverable resource error or a new staged-preparation route.
                let names = sheets.by_ref().map(str::to_string).collect::<Vec<_>>();
                engine.charge_bounded_work(names.len() as u64)?;
                engine.build_graph_for_sheet_names_unobserved(names)
            })
        })
    }

    fn build_graph_for_sheet_names_unobserved(
        &mut self,
        sheets: Vec<String>,
    ) -> Result<(), formualizer_parse::ExcelError> {
        let started = crate::instant::FzInstant::now();
        let selected = sheets
            .iter()
            .filter_map(|sheet| self.staged_formulas.get(sheet))
            .map(StagedSheet::len)
            .sum::<usize>();
        self.resource_checkpoint(selected as u64)?;
        let scratch_bytes = (selected as u64).saturating_mul(256);
        self.reserve_request_scratch(scratch_bytes)?;
        let index_snapshot = self.staged_formula_index.clone();
        let mut collected = Vec::new();
        for sheet in sheets {
            if let Some(staged) = self.staged_formulas.remove(&sheet) {
                self.index_removed_staged_sheet(&sheet, &staged);
                collected.push((sheet, staged));
            }
        }
        let result = self.build_graph_from_staged_batches(collected, true, index_snapshot);
        self.release_request_scratch(scratch_bytes);
        self.observe_staged_preparation(selected, self.staged_formula_count(), started.elapsed());
        result
    }

    fn build_graph_from_staged_batches(
        &mut self,
        collected: StagedFormulaBatches,
        share_parse_cache_across_sheets: bool,
        staged_index_snapshot: StagedFormulaIndex,
    ) -> Result<(), formualizer_parse::ExcelError> {
        if collected.is_empty() {
            return Ok(());
        }
        for (sheet, _) in &collected {
            let _ = self.add_sheet(sheet);
        }

        let diagnostics_len = self.formula_parse_diagnostics.len();
        let mut collected = collected;
        let prepared = match self
            .prepare_staged_formula_batches(&mut collected, share_parse_cache_across_sheets)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.formula_parse_diagnostics.truncate(diagnostics_len);
                for (sheet, staged) in collected {
                    self.restore_staged_sheet(sheet, staged);
                }
                self.staged_formula_index = staged_index_snapshot;
                return Err(error);
            }
        };
        let (ordinary, compressed, direct, may_fail) = prepared;

        // A first build (no formula in the graph yet) goes through the eager
        // first load's machinery: the builder pre-allocates each column's
        // targets as one id run, installs family members as virtual runs and
        // builds the authority once at the end (decision 26).
        // The first load's builder plans and applies one chunk at a time, so
        // a planning error would leave earlier chunks in the graph; the
        // incremental path plans everything first. Check every distinct
        // formula first, in the incremental path's order (it reports the
        // same first error and leaves the graph untouched).
        let first_build = if self.deferred_build_can_be_first_load() {
            if let Err(error) = self.check_staged_formula_plans(
                ordinary
                    .iter()
                    .chain(compressed.iter().map(|(batch, _)| batch)),
                &may_fail,
            ) {
                self.formula_parse_diagnostics.truncate(diagnostics_len);
                for (sheet, staged) in collected {
                    self.restore_staged_sheet(sheet, staged);
                }
                self.staged_formula_index = staged_index_snapshot;
                return Err(error);
            }
            self.deferred_build_as_first_load()
        } else {
            None
        };
        let built_sheets: Vec<String> = if first_build.is_some() {
            collected.iter().map(|(sheet, _)| sheet.clone()).collect()
        } else {
            Vec::new()
        };

        // Keep the original source/spool alive through every fallible ingestion route.
        // Graph admission may have committed a prefix; replay replaces those placements
        // rather than treating their cached values as authoritative source.
        let result = (|| {
            if !ordinary.is_empty() {
                self.ingest_formula_batches(ordinary)?;
            }
            if !compressed.is_empty() {
                self.ingest_compressed_formula_source_batches(compressed)?;
            }
            if !direct.is_empty() {
                self.finish_compressed_formula_sources(direct)?;
            }
            Ok(())
        })();
        if let Some(saved) = first_build {
            self.leave_deferred_first_load(saved, &built_sheets);
        }
        if let Err(error) = result {
            self.formula_parse_diagnostics.truncate(diagnostics_len);
            for (sheet, staged) in collected {
                self.restore_staged_sheet(sheet, staged);
            }
            self.staged_formula_index = staged_index_snapshot;
            return Err(error);
        }
        self.dedup_formula_parse_diagnostics_since(diagnostics_len);
        Ok(())
    }

    /// Enter the first-load ingest mode for a deferred build when the graph
    /// holds no formula yet and no load is in progress: the settings the
    /// calamine loader uses for its eager first load (lazy sheet index, no
    /// small-range expansion, first-load fast path). Returns the settings
    /// to restore, or `None` when the build takes the incremental path.
    fn deferred_build_can_be_first_load(&self) -> bool {
        !self.graph.first_load_assume_new()
            && !self.graph_admission_enabled()
            && self.graph.formula_vertex_count() == 0
    }

    /// Plan each distinct formula of `batches` that may fail planning once
    /// (a member is planned through its template, staged before it) and
    /// drop the plans: the first planning error, in the order the
    /// incremental ingest meets it.
    fn check_staged_formula_plans<'b>(
        &mut self,
        batches: impl Iterator<Item = &'b FormulaIngestBatch>,
        may_fail: &FxHashSet<crate::engine::arena::AstNodeId>,
    ) -> Result<(), ExcelError> {
        if may_fail.is_empty() {
            return Ok(());
        }
        let mut seen: FxHashSet<(SheetId, crate::engine::arena::AstNodeId)> = FxHashSet::default();
        for batch in batches {
            let sheet_id = self.graph.sheet_id(&batch.sheet_name).ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Ref)
                    .with_message(format!("unknown ingest sheet: {}", batch.sheet_name))
            })?;
            let mut pipeline = self.ingest_pipeline();
            for record in &batch.formulas {
                if record.member_anchor.is_some()
                    || !may_fail.contains(&record.ast_id)
                    || !seen.insert((sheet_id, record.ast_id))
                {
                    continue;
                }
                let placement = CellRef::new(
                    sheet_id,
                    Coord::from_excel(record.row, record.col, true, true),
                );
                pipeline.ingest_formula(
                    FormulaAstInput::RawArena(record.ast_id),
                    placement,
                    None,
                )?;
            }
        }
        Ok(())
    }

    fn deferred_build_as_first_load(&mut self) -> Option<(crate::engine::SheetIndexMode, usize)> {
        if !self.deferred_build_can_be_first_load() {
            return None;
        }
        let saved = (
            self.graph.get_config().sheet_index_mode,
            self.config.range_expansion_limit,
        );
        self.graph
            .set_sheet_index_mode(crate::engine::SheetIndexMode::Lazy);
        self.config.range_expansion_limit = 0;
        self.graph.set_first_load_assume_new(true);
        self.graph.reset_ensure_touched();
        Some(saved)
    }

    /// Leave the first-load mode of [`Self::deferred_build_as_first_load`]:
    /// the authority is built once here, as at the end of an eager load.
    fn leave_deferred_first_load(
        &mut self,
        (index_mode, range_limit): (crate::engine::SheetIndexMode, usize),
        sheets: &[String],
    ) {
        self.graph.set_first_load_assume_new(false);
        self.graph.reset_ensure_touched();
        self.graph.set_sheet_index_mode(index_mode);
        self.config.range_expansion_limit = range_limit;
        for sheet in sheets {
            self.graph.finalize_sheet_index(sheet);
        }
    }

    fn prepare_staged_formula_batches(
        &mut self,
        collected: &mut StagedFormulaBatches,
        share_parse_cache_across_sheets: bool,
    ) -> Result<PreparedStagedFormulaBatches, formualizer_parse::ExcelError> {
        let mut ordinary = Vec::new();
        let mut compressed = Vec::new();
        let mut direct = Vec::new();
        let mut may_fail = FxHashSet::default();
        let mut cache: rustc_hash::FxHashMap<String, Option<crate::engine::arena::AstNodeId>> =
            rustc_hash::FxHashMap::default();
        cache.reserve(4096);

        for (sheet, staged) in collected {
            if !share_parse_cache_across_sheets {
                cache.clear();
            }
            // Load-time family grouping, as the eager first load does:
            // relative copies of the formula above (or to the left) become
            // members of its family and are never interned.
            let mut grouper = crate::engine::FormulaFamilyGrouper::new();
            // The staged texts, then the deferred package's replayed ones,
            // are walked once (no combined copy: a replay can be the whole
            // sheet).
            let mut replayed_records = Vec::new();
            let deferred_source = None;
            let mut deferred_fallback = None;
            if let Some(package) = staged.deferred_package.as_mut() {
                if package.sheet_name != *sheet {
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("deferred formula package sheet mismatch"));
                }
                let eligible: Vec<_> = package
                    .families
                    .iter()
                    .filter(|family| !package.invalidated.contains(&family.source_id))
                    .cloned()
                    .collect();
                let eligible_partitions: Vec<_> = package
                    .partitioned_families
                    .iter()
                    .filter(|family| !package.invalidated.contains(&family.source_id))
                    .cloned()
                    .collect();

                let mut replay_disposition = crate::engine::FormulaReplayDisposition::default();
                for partition in &eligible_partitions {
                    replay_disposition
                        .register_partition(partition, false)
                        .map_err(|reason| {
                            ExcelError::new(ExcelErrorKind::Value).with_message(reason)
                        })?;
                }
                replay_disposition
                    .extend_suppressed_excel_coords(package.suppressed.iter().copied());
                let replayed = package
                    .replay
                    .lock()
                    .map_err(|_| {
                        ExcelError::new(ExcelErrorKind::Value)
                            .with_message("deferred formula spool lock poisoned")
                    })?
                    .replay(&replay_disposition)
                    .map_err(|message| {
                        ExcelError::new(ExcelErrorKind::Value).with_message(message)
                    })?;
                replayed_records = replayed;
                let mut report = package.accounting_report();
                report.source_spool_replays = report.source_spool_replays.saturating_add(1);
                deferred_fallback = Some((report, package.families.clone(), eligible_partitions));
            }

            let n_entries = staged.entries.len() + replayed_records.len();
            let entries = staged
                .entries
                .iter()
                .cloned()
                .map(|(row, col, text)| (row, col, text, None))
                .chain(replayed_records.into_iter().map(|record| {
                    (
                        record.row,
                        record.col,
                        record.text,
                        Some((record.source_order, record.family, record.partition_owner)),
                    )
                }));
            let mut formulas = Vec::with_capacity(n_entries);
            let staged_order_base = u64::MAX.saturating_sub(n_entries as u64);
            for (entry_index, (row, col, txt, source_proof)) in entries.enumerate() {
                let key = if txt.starts_with('=') {
                    txt
                } else {
                    format!("={txt}")
                };
                let staged_record = if let Some(cached) = cache.get(&key) {
                    cached.map(|ast_id| {
                        self.note_staged_formula(&mut grouper, row, col, ast_id);
                        FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(key)))
                    })
                } else {
                    let parsed = match formualizer_parse::parser::parse(&key) {
                        Ok(parsed) => Some(parsed),
                        Err(error) => self.handle_formula_parse_error(
                            sheet,
                            row,
                            col,
                            &key,
                            error.to_string(),
                        )?,
                    };
                    match parsed {
                        Some(ast) => {
                            let record = self.stage_formula_ast(&mut grouper, row, col, &ast, None);
                            if !record.is_family_member() && self.formula_may_fail_planning(&ast) {
                                may_fail.insert(record.ast_id);
                            }
                            // A member's text is not worth caching: relative
                            // copies do not repeat their text.
                            if record.is_family_member() {
                                Some(record)
                            } else {
                                let ast_id = record.ast_id;
                                cache.insert(key.clone(), Some(ast_id));
                                Some(FormulaIngestRecord::new(
                                    row,
                                    col,
                                    ast_id,
                                    Some(Arc::<str>::from(key)),
                                ))
                            }
                        }
                        None => {
                            cache.insert(key, None);
                            None
                        }
                    }
                };

                if let Some(mut formula) = staged_record {
                    if let Some((order, family, owner)) = source_proof {
                        formula = formula.with_source_proof(order, family, owner);
                    } else if deferred_source.is_some() {
                        formula = formula.with_source_proof(
                            crate::engine::SourceFormulaOrder::new(
                                staged_order_base.saturating_add(entry_index as u64),
                            ),
                            None,
                            None,
                        );
                    }
                    formulas.push(formula);
                }
            }

            let batch = FormulaIngestBatch::new(sheet.clone(), formulas);
            if let Some((report, preparation)) = deferred_source {
                direct.push((batch, report, preparation));
            } else if let Some((report, families, partitions)) = deferred_fallback {
                let source_batch = crate::engine::FormulaCompressedSourceBatch::with_proposals(
                    batch.sheet_name.clone(),
                    report,
                    families,
                    partitions,
                );
                compressed.push((batch, source_batch));
            } else if !batch.is_empty() {
                ordinary.push(batch);
            }
        }
        Ok((ordinary, compressed, direct, may_fail))
    }

    /// Whether planning `ast` can fail: a reference to a sheet that does not
    /// exist, an external, 3-D or table reference, or a reversed range.
    /// Unqualified cell and range references, names, literals and calls
    /// always plan.
    fn formula_may_fail_planning(&self, ast: &formualizer_parse::parser::ASTNode) -> bool {
        use formualizer_parse::parser::{ASTNodeType, ReferenceType};
        let sheet_missing = |sheet: &Option<String>| {
            sheet
                .as_deref()
                .is_some_and(|s| self.graph.sheet_id(s).is_none())
        };
        match &ast.node_type {
            ASTNodeType::Literal(_) | ASTNodeType::Omitted => false,
            ASTNodeType::Reference { reference, .. } => match reference {
                ReferenceType::Cell { sheet, .. } => sheet_missing(sheet),
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    ..
                } => {
                    sheet_missing(sheet)
                        || matches!((start_row, end_row), (Some(a), Some(b)) if a > b)
                        || matches!((start_col, end_col), (Some(a), Some(b)) if a > b)
                }
                ReferenceType::NamedRange(_) => false,
                _ => true,
            },
            ASTNodeType::UnaryOp { expr, .. } => self.formula_may_fail_planning(expr),
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.formula_may_fail_planning(left) || self.formula_may_fail_planning(right)
            }
            ASTNodeType::Function { args, .. } => {
                args.iter().any(|a| self.formula_may_fail_planning(a))
            }
            ASTNodeType::Call { callee, args } => {
                self.formula_may_fail_planning(callee)
                    || args.iter().any(|a| self.formula_may_fail_planning(a))
            }
            ASTNodeType::Array(rows) => rows
                .iter()
                .flatten()
                .any(|a| self.formula_may_fail_planning(a)),
        }
    }

    /// Begin bulk Arrow ingest for base values (Phase A)
    pub fn begin_bulk_ingest_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkIngestBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkIngestBuilder::new(self)
    }

    /// Begin bulk updates to Arrow store (Phase C)
    pub fn begin_bulk_update_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkUpdateBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkUpdateBuilder::new(self)
    }

    fn ensure_known_sheet_id(&self, sheet: &str) -> Result<SheetId, crate::engine::EditorError> {
        self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )
    }

    fn normalize_row_1based(row_1based: u32) -> Result<u32, crate::engine::EditorError> {
        if row_1based == 0 {
            return Err(crate::engine::EditorError::OutOfBounds { row: 0, col: 0 });
        }
        Ok(row_1based - 1)
    }

    fn normalize_row_range_1based(
        start_row_1based: u32,
        end_row_1based: u32,
    ) -> Result<(u32, u32), crate::engine::EditorError> {
        if start_row_1based == 0 || end_row_1based == 0 {
            return Err(crate::engine::EditorError::OutOfBounds { row: 0, col: 0 });
        }
        if start_row_1based > end_row_1based {
            return Err(crate::engine::EditorError::TransactionFailed {
                reason: "Row range start is greater than end".to_string(),
            });
        }
        Ok((start_row_1based - 1, end_row_1based - 1))
    }

    fn invalidate_row_visibility_mask_cache(&self) {
        if let Ok(mut cache) = self.row_visibility_mask_cache.write() {
            cache.clear();
        }
    }

    fn set_row_hidden_by_sheet_id(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        hidden: bool,
        source: RowVisibilitySource,
    ) -> bool {
        let changed = {
            let state = self.row_visibility.entry(sheet_id).or_default();
            state.set_row_hidden(row0, hidden, source)
        };

        let remove_entry = self
            .row_visibility
            .get(&sheet_id)
            .map(|state| state.is_empty())
            .unwrap_or(false);
        if remove_entry {
            self.row_visibility.remove(&sheet_id);
        }

        if changed {
            self.invalidate_row_visibility_mask_cache();
        }

        changed
    }

    fn set_rows_hidden_by_sheet_id(
        &mut self,
        sheet_id: SheetId,
        start_row0: u32,
        end_row0: u32,
        hidden: bool,
        source: RowVisibilitySource,
    ) -> bool {
        let changed = {
            let state = self.row_visibility.entry(sheet_id).or_default();
            state.set_rows_hidden(start_row0, end_row0, hidden, source)
        };

        let remove_entry = self
            .row_visibility
            .get(&sheet_id)
            .map(|state| state.is_empty())
            .unwrap_or(false);
        if remove_entry {
            self.row_visibility.remove(&sheet_id);
        }

        if changed {
            self.invalidate_row_visibility_mask_cache();
        }

        changed
    }

    fn shift_row_visibility_insert(&mut self, sheet_id: SheetId, before0: u32, count: u32) {
        if count == 0 {
            return;
        }
        let mut changed = false;
        let remove_entry = if let Some(state) = self.row_visibility.get_mut(&sheet_id) {
            changed = state.insert_rows(before0, count);
            state.is_empty()
        } else {
            false
        };
        if remove_entry {
            self.row_visibility.remove(&sheet_id);
        }
        if changed {
            self.invalidate_row_visibility_mask_cache();
        }
    }

    fn shift_row_visibility_delete(&mut self, sheet_id: SheetId, start0: u32, count: u32) {
        if count == 0 {
            return;
        }
        let mut changed = false;
        let remove_entry = if let Some(state) = self.row_visibility.get_mut(&sheet_id) {
            changed = state.delete_rows(start0, count);
            state.is_empty()
        } else {
            false
        };
        if remove_entry {
            self.row_visibility.remove(&sheet_id);
        }
        if changed {
            self.invalidate_row_visibility_mask_cache();
        }
    }

    fn apply_inverse_row_visibility_event(&mut self, event: &crate::engine::ChangeEvent) {
        if let crate::engine::ChangeEvent::SetRowVisibility {
            sheet_id,
            row0,
            source,
            old_hidden,
            ..
        } = event
        {
            let _ = self.set_row_hidden_by_sheet_id(*sheet_id, *row0, *old_hidden, *source);
        }
    }

    fn apply_forward_row_visibility_event(&mut self, event: &crate::engine::ChangeEvent) {
        if let crate::engine::ChangeEvent::SetRowVisibility {
            sheet_id,
            row0,
            source,
            new_hidden,
            ..
        } = event
        {
            let _ = self.set_row_hidden_by_sheet_id(*sheet_id, *row0, *new_hidden, *source);
        }
    }

    fn apply_inverse_row_visibility_events(&mut self, events: &[crate::engine::ChangeEvent]) {
        for event in events.iter().rev() {
            self.apply_inverse_row_visibility_event(event);
        }
    }

    fn apply_forward_row_visibility_events(&mut self, events: &[crate::engine::ChangeEvent]) {
        for event in events {
            self.apply_forward_row_visibility_event(event);
        }
    }

    fn apply_inverse_staged_formula_event(&mut self, event: &crate::engine::ChangeEvent) {
        if let crate::engine::ChangeEvent::StagedFormulaCellChanged {
            sheet,
            row,
            col,
            old,
            ..
        } = event
        {
            self.apply_staged_formula_cell(sheet, *row, *col, old.as_deref());
        }
    }

    fn apply_forward_staged_formula_event(&mut self, event: &crate::engine::ChangeEvent) {
        if let crate::engine::ChangeEvent::StagedFormulaCellChanged {
            sheet,
            row,
            col,
            new,
            ..
        } = event
        {
            self.apply_staged_formula_cell(sheet, *row, *col, new.as_deref());
        }
    }

    /// Set a single cell's staged formula text to `target` (clearing it when
    /// `None`). Used by undo/redo replay of per-cell staged-formula deltas.
    fn apply_staged_formula_cell(&mut self, sheet: &str, row: u32, col: u32, target: Option<&str>) {
        match target {
            Some(text) => self.stage_formula_text(sheet, row, col, text.to_string()),
            None => {
                self.clear_staged_formula_text(sheet, row, col);
            }
        }
    }

    pub fn set_row_hidden(
        &mut self,
        sheet: &str,
        row_1based: u32,
        hidden: bool,
        source: RowVisibilitySource,
    ) -> Result<(), crate::engine::EditorError> {
        self.observe_function_semantic_epoch()
            .map_err(crate::engine::EditorError::Excel)?;
        self.observe_function_semantic_epoch()
            .map_err(crate::engine::EditorError::Excel)?;
        let sheet_id = self.ensure_known_sheet_id(sheet)?;
        let row0 = Self::normalize_row_1based(row_1based)?;
        if self.set_row_hidden_by_sheet_id(sheet_id, row0, hidden, source) {
            self.record_structural_change(StructuralScope::Region(Region::whole_row(
                sheet_id, row0,
            )));
            self.mark_data_edited();
        }
        Ok(())
    }

    pub fn set_rows_hidden(
        &mut self,
        sheet: &str,
        start_row_1based: u32,
        end_row_1based: u32,
        hidden: bool,
        source: RowVisibilitySource,
    ) -> Result<(), crate::engine::EditorError> {
        let sheet_id = self.ensure_known_sheet_id(sheet)?;
        let (start_row0, end_row0) =
            Self::normalize_row_range_1based(start_row_1based, end_row_1based)?;
        if self.set_rows_hidden_by_sheet_id(sheet_id, start_row0, end_row0, hidden, source) {
            if start_row0 == end_row0 {
                self.record_structural_change(StructuralScope::Region(Region::whole_row(
                    sheet_id, start_row0,
                )));
            } else {
                self.record_structural_change(StructuralScope::Sheet(sheet_id));
            }
            self.mark_data_edited();
        }
        Ok(())
    }

    pub fn is_row_hidden(
        &self,
        sheet: &str,
        row_1based: u32,
        source: Option<RowVisibilitySource>,
    ) -> Option<bool> {
        let sheet_id = self.graph.sheet_id(sheet)?;
        let row0 = row_1based.checked_sub(1)?;
        Some(
            self.row_visibility
                .get(&sheet_id)
                .map(|state| state.is_row_hidden(row0, source))
                .unwrap_or(false),
        )
    }

    pub fn row_visibility_version(&self, sheet: &str) -> Option<u64> {
        let sheet_id = self.graph.sheet_id(sheet)?;
        Some(
            self.row_visibility
                .get(&sheet_id)
                .map(|state| state.version())
                .unwrap_or(0),
        )
    }

    fn build_row_visibility_mask_for_view(
        &self,
        view: &RangeView<'_>,
        mode: VisibilityMaskMode,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        let sheet_rows = view.sheet().nrows as usize;
        if sheet_rows == 0 || view.start_row() >= sheet_rows {
            return Some(std::sync::Arc::new(arrow_array::BooleanArray::new_null(0)));
        }

        let sheet_id = self.graph.sheet_id(view.sheet_name())?;
        let start_row0 = view.start_row() as u32;
        let end_row0 = view.end_row().min(sheet_rows.saturating_sub(1)) as u32;
        let version = self
            .row_visibility
            .get(&sheet_id)
            .map(|state| state.version())
            .unwrap_or(0);
        let key = VisibilityMaskCacheKey {
            sheet_id,
            start_row0,
            end_row0,
            mode,
            version,
        };

        if let Ok(cache) = self.row_visibility_mask_cache.read()
            && let Some(mask) = cache.get(&key)
        {
            #[cfg(test)]
            visibility_mask_test_hooks::inc_hit();
            return Some(mask.clone());
        }

        #[cfg(test)]
        visibility_mask_test_hooks::inc_miss();

        let state = self.row_visibility.get(&sheet_id);
        let mut out = Vec::with_capacity((end_row0 - start_row0 + 1) as usize);
        for row0 in start_row0..=end_row0 {
            let manual_hidden = state
                .map(|s| s.is_row_hidden(row0, Some(RowVisibilitySource::Manual)))
                .unwrap_or(false);
            let filter_hidden = state
                .map(|s| s.is_row_hidden(row0, Some(RowVisibilitySource::Filter)))
                .unwrap_or(false);

            let include = match mode {
                VisibilityMaskMode::IncludeAll => true,
                VisibilityMaskMode::ExcludeManualHidden => !manual_hidden,
                VisibilityMaskMode::ExcludeFilterHidden => !filter_hidden,
                VisibilityMaskMode::ExcludeManualOrFilterHidden => {
                    !(manual_hidden || filter_hidden)
                }
            };
            out.push(include);
        }

        let mask = std::sync::Arc::new(arrow_array::BooleanArray::from(out));
        if let Ok(mut cache) = self.row_visibility_mask_cache.write() {
            const MAX_CACHE_ENTRIES: usize = 4096;
            if cache.len() >= MAX_CACHE_ENTRIES {
                cache.clear();
                #[cfg(test)]
                visibility_mask_test_hooks::inc_eviction();
            }
            cache.insert(key, mask.clone());
        }

        Some(mask)
    }

    fn observe_function_semantic_epoch(&mut self) -> Result<bool, ExcelError> {
        let changes =
            crate::function_registry::semantic_changes_since(self.function_semantic_epoch_seen);
        let global_changed = changes.epoch != self.function_semantic_epoch_seen;
        let provider_revision = self.resolver.planning_semantic_revision();
        let provider_changed = provider_revision != self.function_provider_revision_seen;
        if !global_changed && !provider_changed {
            return Ok(false);
        }

        let changed = !changes.keys.is_empty();
        if global_changed && changed || provider_changed {
            self.cached_static_schedule = None;
            self.recent_schedules.clear();
            self.base_schedule = None;
        }
        self.function_semantic_epoch_seen = changes.epoch;
        self.function_provider_revision_seen = provider_revision;
        Ok(false)
    }

    pub(crate) fn ast_uses_changed_function(
        ast: &ASTNode,
        changed: &BTreeSet<(String, String)>,
    ) -> bool {
        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                let normalized = name.to_uppercase();
                let mut spellings = vec![(String::new(), normalized.clone())];
                let mut stripped = normalized.as_str();
                loop {
                    let Some(rest) = ["_XLFN.", "_XLL.", "_XLWS."]
                        .iter()
                        .find_map(|prefix| stripped.strip_prefix(prefix))
                    else {
                        break;
                    };
                    stripped = rest;
                    spellings.push((String::new(), stripped.to_string()));
                }
                let resolved = crate::function_registry::resolve("", name);
                let directly_changed = spellings.iter().any(|spelling| changed.contains(spelling))
                    || resolved.as_ref().is_some_and(|resolved| {
                        changed.contains(&(
                            resolved.namespace.clone(),
                            resolved.canonical_name.clone(),
                        ))
                    });
                directly_changed
                    || resolved.is_none()
                    || args
                        .iter()
                        .any(|arg| Self::ast_uses_changed_function(arg, changed))
            }
            ASTNodeType::Call { callee, args } => {
                Self::ast_uses_changed_function(callee, changed)
                    || args
                        .iter()
                        .any(|arg| Self::ast_uses_changed_function(arg, changed))
            }
            ASTNodeType::UnaryOp { expr, .. } => Self::ast_uses_changed_function(expr, changed),
            ASTNodeType::BinaryOp { left, right, .. } => {
                Self::ast_uses_changed_function(left, changed)
                    || Self::ast_uses_changed_function(right, changed)
            }
            ASTNodeType::Array(rows) => rows
                .iter()
                .flatten()
                .any(|node| Self::ast_uses_changed_function(node, changed)),
            _ => false,
        }
    }

    fn ast_contains_function(ast: &ASTNode) -> bool {
        match &ast.node_type {
            ASTNodeType::Function { .. } => true,
            ASTNodeType::Call { callee, args } => {
                Self::ast_contains_function(callee) || args.iter().any(Self::ast_contains_function)
            }
            ASTNodeType::UnaryOp { expr, .. } => Self::ast_contains_function(expr),
            ASTNodeType::BinaryOp { left, right, .. } => {
                Self::ast_contains_function(left) || Self::ast_contains_function(right)
            }
            ASTNodeType::Array(rows) => rows.iter().flatten().any(Self::ast_contains_function),
            ASTNodeType::Literal(_) | ASTNodeType::Omitted | ASTNodeType::Reference { .. } => false,
        }
    }

    fn structural_row_region(sheet_id: SheetId, start_row0: u32) -> Region {
        Region::rows_from(sheet_id, start_row0)
    }

    fn structural_col_region(sheet_id: SheetId, start_col0: u32) -> Region {
        Region::cols_from(sheet_id, start_col0)
    }

    #[cfg(test)]
    pub(crate) fn force_non_cycle_schedule_fallback_for_test(&mut self) {
        self.force_non_cycle_schedule_fallback_for_test = true;
    }

    fn materialize_deferred_sheet_before_structural_edit(
        &mut self,
        sheet: &str,
    ) -> Result<(), crate::engine::EditorError> {
        if self.staged_formulas.contains_key(sheet) {
            self.build_graph_for_sheets([sheet])?;
        }
        Ok(())
    }

    fn structural_row_occupancy(
        &self,
        sheet: &str,
        sheet_id: SheetId,
    ) -> crate::engine::graph::StructuralOccupancy {
        if !self.graph.has_compressed_range_readers() {
            return crate::engine::graph::StructuralOccupancy::default();
        }
        let mut occupancy = self.graph.structural_occupancy(sheet_id);
        if let Some(arrow_sheet) = self.arrow_sheets.sheet(sheet) {
            occupancy.include_arrow_sheet(arrow_sheet);
            occupancy
        } else {
            // Missing Arrow state cannot prove an apparently empty column empty.
            crate::engine::graph::StructuralOccupancy::conservative()
        }
    }

    fn structural_column_occupancy(&self) -> crate::engine::graph::StructuralOccupancy {
        // Arrow exposes occupied columns through chunk metadata and overlay maps,
        // but has no cheap occupied-row index. Column edits therefore deliberately
        // retain conservative cross-axis invalidation instead of scanning cells.
        crate::engine::graph::StructuralOccupancy::conservative()
    }

    /// Insert rows (1-based) and mirror into Arrow store when enabled
    pub fn insert_rows(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        if count == 0 {
            return Ok(crate::engine::graph::editor::vertex_editor::ShiftSummary::default());
        }
        self.observe_function_semantic_epoch()
            .map_err(crate::engine::EditorError::Excel)?;
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        self.materialize_deferred_sheet_before_structural_edit(sheet)?;
        let sheet_id = self.ensure_known_sheet_id(sheet)?;
        let before0 = before.saturating_sub(1);
        let affected_region = Self::structural_row_region(sheet_id, before0);
        let occupancy = self.structural_row_occupancy(sheet, sheet_id);
        let summary = {
            let mut editor =
                VertexEditor::new(&mut self.graph).with_structural_occupancy(occupancy);
            editor.insert_rows(sheet_id, before0, count)?
        };
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before0 as usize;
            asheet.insert_rows(before0, count as usize);
        }
        self.purge_derived_formats_after_row(sheet_id, before0);
        self.mark_moved_formula_vertices_dirty(&summary);
        self.clear_computed_overlay_after_row(sheet, before0 as usize);
        self.shift_row_visibility_insert(sheet_id, before0, count);
        self.record_structural_change(StructuralScope::Region(affected_region));
        self.mark_topology_edited();
        Ok(summary)
    }

    /// Delete rows (1-based) and mirror into Arrow store when enabled
    pub fn delete_rows(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        if count == 0 {
            return Ok(crate::engine::graph::editor::vertex_editor::ShiftSummary::default());
        }
        self.observe_function_semantic_epoch()
            .map_err(crate::engine::EditorError::Excel)?;
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        self.materialize_deferred_sheet_before_structural_edit(sheet)?;
        let sheet_id = self.ensure_known_sheet_id(sheet)?;
        let start0 = start.saturating_sub(1);
        let affected_region = Self::structural_row_region(sheet_id, start0);
        let occupancy = self.structural_row_occupancy(sheet, sheet_id);
        let summary = {
            let mut editor =
                VertexEditor::new(&mut self.graph).with_structural_occupancy(occupancy);
            editor.delete_rows(sheet_id, start0, count)?
        };
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start0 as usize;
            asheet.delete_rows(start0, count as usize);
        }
        self.purge_derived_formats_after_row(sheet_id, start0);
        self.mark_moved_formula_vertices_dirty(&summary);
        self.clear_computed_overlay_after_row(sheet, start0 as usize);
        self.shift_row_visibility_delete(sheet_id, start0, count);
        self.record_structural_change(StructuralScope::Region(affected_region));
        self.mark_topology_edited();
        Ok(summary)
    }

    /// Insert columns (1-based) and mirror into Arrow store when enabled
    pub fn insert_columns(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        if count == 0 {
            return Ok(crate::engine::graph::editor::vertex_editor::ShiftSummary::default());
        }
        self.observe_function_semantic_epoch()
            .map_err(crate::engine::EditorError::Excel)?;
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        self.materialize_deferred_sheet_before_structural_edit(sheet)?;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let before0 = before.saturating_sub(1);
        let affected_region = Self::structural_col_region(sheet_id, before0);
        let occupancy = self.structural_column_occupancy();
        let summary = {
            let mut editor =
                VertexEditor::new(&mut self.graph).with_structural_occupancy(occupancy);
            editor.insert_columns(sheet_id, before0, count)?
        };
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before0 as usize;
            asheet.insert_columns(before0, count as usize);
        }
        self.purge_derived_formats_after_col(sheet_id, before0);
        self.mark_moved_formula_vertices_dirty(&summary);
        self.clear_computed_overlay_after_col(sheet, before0 as usize);
        self.record_structural_change(StructuralScope::Region(affected_region));
        self.mark_topology_edited();
        Ok(summary)
    }

    /// Delete columns (1-based) and mirror into Arrow store when enabled
    pub fn delete_columns(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        if count == 0 {
            return Ok(crate::engine::graph::editor::vertex_editor::ShiftSummary::default());
        }
        self.observe_function_semantic_epoch()
            .map_err(crate::engine::EditorError::Excel)?;
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        self.materialize_deferred_sheet_before_structural_edit(sheet)?;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let start0 = start.saturating_sub(1);
        let affected_region = Self::structural_col_region(sheet_id, start0);
        let occupancy = self.structural_column_occupancy();
        let summary = {
            let mut editor =
                VertexEditor::new(&mut self.graph).with_structural_occupancy(occupancy);
            editor.delete_columns(sheet_id, start0, count)?
        };
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start0 as usize;
            asheet.delete_columns(start0, count as usize);
        }
        self.purge_derived_formats_after_col(sheet_id, start0);
        self.mark_moved_formula_vertices_dirty(&summary);
        self.clear_computed_overlay_after_col(sheet, start0 as usize);
        self.record_structural_change(StructuralScope::Region(affected_region));
        self.mark_topology_edited();
        Ok(summary)
    }
    /// Arrow-backed used row bounds across a column span (1-based inclusive cols).
    fn arrow_used_row_bounds(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sc0 = start_col.saturating_sub(1) as usize;
        let ec0 = end_col.saturating_sub(1) as usize;
        let col_hi = a.columns.len().saturating_sub(1);
        if sc0 > col_hi {
            return None;
        }
        let ec0 = ec0.min(col_hi);
        // Pass-scoped cache with snapshot guard
        let snap = self.data_snapshot_id();
        let mut min_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let sheet_id = self.graph.sheet_id(sheet)?;
            if let Some((Some(mv), _)) = self.row_bounds_cache.read().ok().and_then(|g| {
                g.as_ref()
                    .and_then(|c| c.get_row_bounds(sheet_id, ci, snap))
            }) {
                let mv = mv as usize;
                min_r0 = Some(min_r0.map(|m| m.min(mv)).unwrap_or(mv));
                continue;
            }
            // Compute and store
            let (min_c, max_c) = Self::scan_column_used_bounds(a, ci);
            if let Ok(mut g) = self.row_bounds_cache.write() {
                g.get_or_insert_with(|| RowBoundsCache::new(snap))
                    .put_row_bounds(sheet_id, ci, snap, (min_c, max_c));
            }
            if let Some(m) = min_c {
                min_r0 = Some(min_r0.map(|mm| mm.min(m as usize)).unwrap_or(m as usize));
            }
        }
        min_r0?;
        let mut max_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let sheet_id = self.graph.sheet_id(sheet)?;
            if let Some((_, Some(mv))) = self.row_bounds_cache.read().ok().and_then(|g| {
                g.as_ref()
                    .and_then(|c| c.get_row_bounds(sheet_id, ci, snap))
            }) {
                let mv = mv as usize;
                max_r0 = Some(max_r0.map(|m| m.max(mv)).unwrap_or(mv));
                continue;
            }
            let (_min_c, max_c) = Self::scan_column_used_bounds(a, ci);
            if let Ok(mut g) = self.row_bounds_cache.write() {
                g.get_or_insert_with(|| RowBoundsCache::new(snap))
                    .put_row_bounds(sheet_id, ci, snap, (_min_c, max_c));
            }
            if let Some(m) = max_c {
                max_r0 = Some(max_r0.map(|mm| mm.max(m as usize)).unwrap_or(m as usize));
            }
        }
        match (min_r0, max_r0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    fn scan_column_used_bounds(
        a: &crate::arrow_store::ArrowSheet,
        ci: usize,
    ) -> (Option<u32>, Option<u32>) {
        let col = &a.columns[ci];

        // Min: scan dense chunks first, then sparse chunks in ascending index order.
        let mut min_r0: Option<u32> = None;
        for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
            let tags = chunk.type_tag.values();
            for (off, &t) in tags.iter().enumerate() {
                let overlay_non_empty = chunk
                    .overlay
                    .get(off)
                    .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                    .unwrap_or(false)
                    || chunk
                        .computed_overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false);
                if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                    let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                        break;
                    };
                    let row0 = chunk_start + off;
                    min_r0 = Some(row0 as u32);
                    break;
                }
            }
            if min_r0.is_some() {
                break;
            }
        }
        if min_r0.is_none() && !col.sparse_chunks.is_empty() {
            let mut sparse_idxs: Vec<usize> = col.sparse_chunks.keys().copied().collect();
            sparse_idxs.sort_unstable();
            for chunk_idx in sparse_idxs {
                let Some(chunk) = col.sparse_chunks.get(&chunk_idx) else {
                    continue;
                };
                let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                    continue;
                };
                let tags = chunk.type_tag.values();
                for (off, &t) in tags.iter().enumerate() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(off)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let row0 = chunk_start + off;
                        min_r0 = Some(row0 as u32);
                        break;
                    }
                }
                if min_r0.is_some() {
                    break;
                }
            }
        }

        // Max: scan sparse chunks in descending index order, then dense chunks in reverse.
        let mut max_r0: Option<u32> = None;
        if !col.sparse_chunks.is_empty() {
            let mut sparse_idxs: Vec<usize> = col.sparse_chunks.keys().copied().collect();
            sparse_idxs.sort_unstable_by(|a, b| b.cmp(a));
            for chunk_idx in sparse_idxs {
                let Some(chunk) = col.sparse_chunks.get(&chunk_idx) else {
                    continue;
                };
                let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                    continue;
                };
                let tags = chunk.type_tag.values();
                for (rev_idx, &t) in tags.iter().enumerate().rev() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(rev_idx)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(rev_idx)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let row0 = chunk_start + rev_idx;
                        max_r0 = Some(row0 as u32);
                        break;
                    }
                }
                if max_r0.is_some() {
                    break;
                }
            }
        }
        if max_r0.is_none() {
            for (chunk_idx, chunk) in col.chunks.iter().enumerate().rev() {
                let tags = chunk.type_tag.values();
                for (rev_idx, &t) in tags.iter().enumerate().rev() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(rev_idx)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(rev_idx)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                            break;
                        };
                        let row0 = chunk_start + rev_idx;
                        max_r0 = Some(row0 as u32);
                        break;
                    }
                }
                if max_r0.is_some() {
                    break;
                }
            }
        }

        (min_r0, max_r0)
    }

    /// Arrow-backed used column bounds across a row span (1-based inclusive rows).
    fn arrow_used_col_bounds(
        &self,
        sheet: &str,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sr0 = start_row.saturating_sub(1) as usize;
        let er0 = end_row.saturating_sub(1) as usize;
        if sr0 > er0 {
            return None;
        }
        // Map start/end rows into chunk ranges
        // We will scan each column for any non-empty within [sr0..=er0]
        let mut min_c0: Option<usize> = None;
        let mut max_c0: Option<usize> = None;
        // Precompute chunk bounds for row range
        for (ci, col) in a.columns.iter().enumerate() {
            let mut any_in_range = false;

            let scan_chunk = |chunk_idx: usize, chunk: &crate::arrow_store::ColumnChunk| -> bool {
                let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                    return false;
                };
                let chunk_len = chunk.type_tag.len();
                if chunk_len == 0 {
                    return false;
                }
                let chunk_end = chunk_start + chunk_len.saturating_sub(1);
                // check intersection
                if sr0 > chunk_end || er0 < chunk_start {
                    return false;
                }
                let start_off = sr0.max(chunk_start) - chunk_start;
                let end_off = er0.min(chunk_end) - chunk_start;
                let tags = chunk.type_tag.values();
                for off in start_off..=end_off {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(off)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || tags[off] != crate::arrow_store::TypeTag::Empty as u8 {
                        return true;
                    }
                }
                false
            };

            for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
                if scan_chunk(chunk_idx, chunk) {
                    any_in_range = true;
                    break;
                }
            }

            if !any_in_range && !col.sparse_chunks.is_empty() {
                for (&chunk_idx, chunk) in col.sparse_chunks.iter() {
                    if scan_chunk(chunk_idx, chunk) {
                        any_in_range = true;
                        break;
                    }
                }
            }

            if any_in_range {
                min_c0 = Some(min_c0.map(|m| m.min(ci)).unwrap_or(ci));
                max_c0 = Some(max_c0.map(|m| m.max(ci)).unwrap_or(ci));
            }
        }
        match (min_c0, max_c0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    fn formula_row_bounds_for_columns(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        let sheet_id = self.graph.sheet_id(sheet)?;
        let sc0 = start_col.saturating_sub(1);
        let ec0 = end_col.saturating_sub(1);
        let mut min_r0: Option<u32> = None;
        let mut max_r0: Option<u32> = None;

        if self.graph.sheet_index(sheet_id).is_some() {
            for vid in self.graph.vertices_in_cols(sheet_id, sc0, ec0) {
                if !matches!(
                    self.graph.get_vertex_kind(vid),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                ) {
                    continue;
                }
                let Some(row0) = self.graph.vertex_grid_addr(vid).map(|addr| addr.row()) else {
                    continue;
                };
                min_r0 = Some(min_r0.map(|m| m.min(row0)).unwrap_or(row0));
                max_r0 = Some(max_r0.map(|m| m.max(row0)).unwrap_or(row0));
            }
        } else {
            for (vid, coord) in self.graph.grid_vertices_in_sheet(sheet_id) {
                if !matches!(
                    self.graph.get_vertex_kind(vid),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                ) {
                    continue;
                }
                let col0 = coord.col();
                if col0 < sc0 || col0 > ec0 {
                    continue;
                }
                let row0 = coord.row();
                min_r0 = Some(min_r0.map(|m| m.min(row0)).unwrap_or(row0));
                max_r0 = Some(max_r0.map(|m| m.max(row0)).unwrap_or(row0));
            }
        }

        match (min_r0, max_r0) {
            (Some(a0), Some(b0)) => Some((a0 + 1, b0 + 1)),
            _ => None,
        }
    }

    fn formula_col_bounds_for_rows(
        &self,
        sheet: &str,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        let sheet_id = self.graph.sheet_id(sheet)?;
        let sr0 = start_row.saturating_sub(1);
        let er0 = end_row.saturating_sub(1);
        let mut min_c0: Option<u32> = None;
        let mut max_c0: Option<u32> = None;

        if self.graph.sheet_index(sheet_id).is_some() {
            for vid in self.graph.vertices_in_rows(sheet_id, sr0, er0) {
                if !matches!(
                    self.graph.get_vertex_kind(vid),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                ) {
                    continue;
                }
                let Some(col0) = self.graph.vertex_grid_addr(vid).map(|addr| addr.col()) else {
                    continue;
                };
                min_c0 = Some(min_c0.map(|m| m.min(col0)).unwrap_or(col0));
                max_c0 = Some(max_c0.map(|m| m.max(col0)).unwrap_or(col0));
            }
        } else {
            for (vid, coord) in self.graph.grid_vertices_in_sheet(sheet_id) {
                if !matches!(
                    self.graph.get_vertex_kind(vid),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                ) {
                    continue;
                }
                let row0 = coord.row();
                if row0 < sr0 || row0 > er0 {
                    continue;
                }
                let col0 = coord.col();
                min_c0 = Some(min_c0.map(|m| m.min(col0)).unwrap_or(col0));
                max_c0 = Some(max_c0.map(|m| m.max(col0)).unwrap_or(col0));
            }
        }

        match (min_c0, max_c0) {
            (Some(a0), Some(b0)) => Some((a0 + 1, b0 + 1)),
            _ => None,
        }
    }

    fn union_used_bounds(
        first: Option<(u32, u32)>,
        second: Option<(u32, u32)>,
    ) -> Option<(u32, u32)> {
        match (first, second) {
            (Some((a0, b0)), Some((a1, b1))) => Some((a0.min(a1), b0.max(b1))),
            (Some(bounds), None) | (None, Some(bounds)) => Some(bounds),
            (None, None) => None,
        }
    }

    /// Mirror a single cell value into the Arrow overlay if enabled.
    /// Handles capacity growth, per-chunk overlay set, and heuristic compaction.
    fn mirror_value_to_overlay(&mut self, sheet: &str, row: u32, col: u32, value: &LiteralValue) {
        if !(self.config.arrow_storage_enabled && self.config.delta_overlay_enabled) {
            return;
        }
        if self.arrow_sheets.sheet(sheet).is_none() {
            self.arrow_sheets
                .sheets
                .push(crate::arrow_store::ArrowSheet {
                    name: std::sync::Arc::<str>::from(sheet),
                    date_system: self.config.date_system,
                    columns: Vec::new(),
                    nrows: 0,
                    chunk_starts: Vec::new(),
                    chunk_rows: 32 * 1024,
                });
        }

        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;

        let asheet = self
            .arrow_sheets
            .sheet_mut(sheet)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }

        if row0 >= asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(row0 + 1);
        }
        if let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) {
            let ov =
                crate::arrow_store::OverlayValue::from_literal_value(value, asheet.date_system);
            let computed_delta = if let Some(ch) = asheet.ensure_column_chunk_mut(col0, ch_idx) {
                let _ = ch.overlay.set(in_off, ov);
                let format = match value {
                    LiteralValue::Date(_) => Some(crate::format::FormatId::DATE),
                    LiteralValue::DateTime(_) => Some(crate::format::FormatId::DATETIME),
                    LiteralValue::Time(_) => Some(crate::format::FormatId::TIME),
                    LiteralValue::Duration(_) => Some(crate::format::FormatId::DURATION),
                    _ => None,
                };
                ch.overlay.set_format(in_off, format);
                // A user edit must invalidate any computed (formula/spill) overlay entry at
                // this cell. Otherwise, if the delta overlay later compacts into the base lanes
                // (clearing `overlay`), a stale `computed_overlay=Empty` could incorrectly mask
                // the edited base value under the read cascade.
                ch.computed_overlay.remove(in_off)
            } else {
                return;
            };
            // Heuristic compaction: > len/50 or > 1024
            let abs_threshold = 1024usize;
            let frac_den = 50usize;
            let freed = asheet.maybe_compact_chunk(col0, ch_idx, abs_threshold, frac_den);
            if freed > 0 {
                self.overlay_compactions = self.overlay_compactions.saturating_add(1);
            }
            self.adjust_computed_overlay_bytes(computed_delta);
        }
    }

    /// Remove a delta-overlay entry for a single cell (if present).
    ///
    /// This is used when transitioning a cell to a formula so that any previous user-edit overlay
    /// does not continue to mask computed overlay outputs.
    fn clear_delta_overlay_cell(&mut self, sheet: &str, row: u32, col: u32) {
        if !(self.config.arrow_storage_enabled && self.config.delta_overlay_enabled) {
            return;
        }
        let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) else {
            return;
        };
        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;
        if row0 >= asheet.nrows as usize {
            return;
        }
        if col0 >= asheet.columns.len() {
            return;
        }
        let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) else {
            return;
        };
        if let Some(ch) = asheet.columns[col0].chunk_mut(ch_idx) {
            let _ = ch.overlay.remove(in_off);
        }
    }

    fn clear_computed_overlay_after_row(&mut self, sheet: &str, start_row0: usize) {
        if !(self.config.arrow_storage_enabled && self.config.write_formula_overlay_enabled) {
            return;
        }

        let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) else {
            return;
        };
        if start_row0 >= asheet.nrows as usize {
            return;
        }

        let starts = asheet.chunk_starts.clone();
        let nrows = asheet.nrows as usize;
        let mut delta = 0isize;
        for col in &mut asheet.columns {
            for (chunk_idx, ch) in col.chunks.iter_mut().enumerate() {
                let Some(&chunk_start) = starts.get(chunk_idx) else {
                    continue;
                };
                let chunk_end = starts
                    .get(chunk_idx + 1)
                    .copied()
                    .unwrap_or(nrows)
                    .min(chunk_start.saturating_add(ch.len()));
                if chunk_end <= start_row0 {
                    continue;
                }
                if chunk_start >= start_row0 {
                    delta = delta.saturating_sub(ch.computed_overlay.clear() as isize);
                } else {
                    let start_in_chunk = start_row0.saturating_sub(chunk_start).min(ch.len());
                    delta = delta
                        .saturating_add(ch.computed_overlay.remove_range(start_in_chunk..ch.len()));
                }
            }

            for (chunk_idx, ch) in &mut col.sparse_chunks {
                let Some(&chunk_start) = starts.get(*chunk_idx) else {
                    continue;
                };
                let chunk_end = starts
                    .get(*chunk_idx + 1)
                    .copied()
                    .unwrap_or(nrows)
                    .min(chunk_start.saturating_add(ch.len()));
                if chunk_end <= start_row0 {
                    continue;
                }
                if chunk_start >= start_row0 {
                    delta = delta.saturating_sub(ch.computed_overlay.clear() as isize);
                } else {
                    let start_in_chunk = start_row0.saturating_sub(chunk_start).min(ch.len());
                    delta = delta
                        .saturating_add(ch.computed_overlay.remove_range(start_in_chunk..ch.len()));
                }
            }
        }
        self.adjust_computed_overlay_bytes(delta);
    }

    fn clear_computed_overlay_after_col(&mut self, sheet: &str, start_col0: usize) {
        if !(self.config.arrow_storage_enabled && self.config.write_formula_overlay_enabled) {
            return;
        }

        let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) else {
            return;
        };
        if start_col0 >= asheet.columns.len() {
            return;
        }

        let mut delta = 0isize;
        for col in asheet.columns.iter_mut().skip(start_col0) {
            for ch in &mut col.chunks {
                delta = delta.saturating_sub(ch.computed_overlay.clear() as isize);
            }
            for ch in col.sparse_chunks.values_mut() {
                delta = delta.saturating_sub(ch.computed_overlay.clear() as isize);
            }
        }
        self.adjust_computed_overlay_bytes(delta);
    }

    #[inline]
    fn literal_to_overlay_value(
        value: &LiteralValue,
        date_system: crate::engine::DateSystem,
    ) -> crate::arrow_store::OverlayValue {
        crate::arrow_store::OverlayValue::from_literal_value(value, date_system)
    }

    fn arrow_sheet_date_system(&self, sheet: &str) -> crate::engine::DateSystem {
        self.arrow_sheets
            .sheet(sheet)
            .map(|sheet| sheet.date_system)
            .unwrap_or(self.config.date_system)
    }

    /// Read a single cell's delta overlay entry (if present), preserving the distinction between
    /// absent and explicit `Empty`.
    fn read_delta_overlay_cell(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        if !(self.config.arrow_storage_enabled && self.config.delta_overlay_enabled) {
            return None;
        }
        let asheet = self.arrow_sheets.sheet(sheet)?;
        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;
        if row0 >= asheet.nrows as usize || col0 >= asheet.columns.len() {
            return None;
        }
        let (ch_idx, in_off) = asheet.chunk_of_row(row0)?;
        let ch = asheet.columns[col0].chunk(ch_idx)?;
        ch.overlay
            .get_scalar(in_off)
            .map(|ov| ov.to_literal_for(asheet.date_system))
    }

    /// Read a single cell's computed overlay entry (if present), preserving the distinction
    /// between absent and explicit `Empty`.
    fn read_computed_overlay_cell(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return None;
        }
        let asheet = self.arrow_sheets.sheet(sheet)?;
        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;
        if row0 >= asheet.nrows as usize || col0 >= asheet.columns.len() {
            return None;
        }
        let (ch_idx, in_off) = asheet.chunk_of_row(row0)?;
        let ch = asheet.columns[col0].chunk(ch_idx)?;
        ch.computed_overlay
            .get_scalar(in_off)
            .map(|ov| ov.to_literal_for(asheet.date_system))
    }

    fn set_delta_overlay_cell_raw(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: Option<LiteralValue>,
    ) {
        if !(self.config.arrow_storage_enabled && self.config.delta_overlay_enabled) {
            return;
        }

        self.ensure_arrow_sheet(sheet);
        let date_system = self.arrow_sheet_date_system(sheet);
        let ov_opt = value
            .as_ref()
            .map(|value| Self::literal_to_overlay_value(value, date_system));
        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;
        let asheet = self
            .arrow_sheets
            .sheet_mut(sheet)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }
        if row0 >= asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(row0 + 1);
        }

        let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) else {
            return;
        };
        let Some(ch) = asheet.ensure_column_chunk_mut(col0, ch_idx) else {
            return;
        };

        if let Some(ov) = ov_opt {
            let _ = ch.overlay.set(in_off, ov);
        } else {
            let _ = ch.overlay.remove(in_off);
        }
    }

    fn set_computed_overlay_cell_raw(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: Option<LiteralValue>,
    ) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return;
        }

        self.ensure_arrow_sheet(sheet);
        let date_system = self.arrow_sheet_date_system(sheet);
        let ov_opt = value
            .as_ref()
            .map(|value| Self::literal_to_overlay_value(value, date_system));
        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;
        let asheet = self
            .arrow_sheets
            .sheet_mut(sheet)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }
        if row0 >= asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(row0 + 1);
        }

        let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) else {
            return;
        };
        let Some(ch) = asheet.ensure_column_chunk_mut(col0, ch_idx) else {
            return;
        };

        let delta = if let Some(ov) = ov_opt {
            ch.computed_overlay.set(in_off, ov)
        } else {
            ch.computed_overlay.remove(in_off)
        };
        self.adjust_computed_overlay_bytes(delta);
    }

    fn apply_arrow_undo_batch(&mut self, batch: &crate::engine::ArrowUndoBatch, undo: bool) {
        use crate::engine::ArrowOp;

        let iter: Box<dyn Iterator<Item = &ArrowOp>> = if undo {
            Box::new(batch.ops.iter().rev())
        } else {
            Box::new(batch.ops.iter())
        };

        for op in iter {
            match op {
                ArrowOp::SetDeltaCell {
                    sheet_id,
                    row0,
                    col0,
                    old,
                    new,
                } => {
                    let sheet = self.graph.sheet_name(*sheet_id).to_string();
                    let v = if undo { old.clone() } else { new.clone() };
                    self.set_delta_overlay_cell_raw(&sheet, row0 + 1, col0 + 1, v);
                }
                ArrowOp::SetComputedCell {
                    sheet_id,
                    row0,
                    col0,
                    old,
                    new,
                } => {
                    let sheet = self.graph.sheet_name(*sheet_id).to_string();
                    let v = if undo { old.clone() } else { new.clone() };
                    self.set_computed_overlay_cell_raw(&sheet, row0 + 1, col0 + 1, v);
                }
                ArrowOp::RestoreComputedRect {
                    sheet_id,
                    sr0,
                    sc0,
                    er0,
                    ec0,
                    old,
                    new,
                } => {
                    let sheet = self.graph.sheet_name(*sheet_id).to_string();
                    let vals = if undo { old } else { new };
                    let height = (*er0).saturating_sub(*sr0) as usize + 1;
                    let width = (*ec0).saturating_sub(*sc0) as usize + 1;
                    for r in 0..height {
                        for c in 0..width {
                            let v = vals
                                .get(r)
                                .and_then(|row| row.get(c))
                                .cloned()
                                .unwrap_or(LiteralValue::Empty);
                            self.set_computed_overlay_cell_raw(
                                &sheet,
                                *sr0 + 1 + r as u32,
                                *sc0 + 1 + c as u32,
                                Some(v),
                            );
                        }
                    }
                }
                ArrowOp::InsertRows {
                    sheet_id,
                    before0,
                    count,
                } => {
                    let sheet = self.graph.sheet_name(*sheet_id).to_string();
                    self.ensure_arrow_sheet(&sheet);
                    if let Some(asheet) = self.arrow_sheets.sheet_mut(&sheet) {
                        if undo {
                            asheet.delete_rows(*before0 as usize, *count as usize);
                        } else {
                            asheet.insert_rows(*before0 as usize, *count as usize);
                        }
                    }
                    self.purge_derived_formats_after_row(*sheet_id, *before0);
                }
                ArrowOp::InsertCols {
                    sheet_id,
                    before0,
                    count,
                } => {
                    let sheet = self.graph.sheet_name(*sheet_id).to_string();
                    self.ensure_arrow_sheet(&sheet);
                    if let Some(asheet) = self.arrow_sheets.sheet_mut(&sheet) {
                        if undo {
                            asheet.delete_columns(*before0 as usize, *count as usize);
                        } else {
                            asheet.insert_columns(*before0 as usize, *count as usize);
                        }
                    }
                    self.purge_derived_formats_after_col(*sheet_id, *before0);
                }
            }
        }
    }

    fn record_spill_ops_into_arrow_undo(
        &mut self,
        undo: &mut crate::engine::ArrowUndoBatch,
        events: &[crate::engine::ChangeEvent],
    ) {
        use crate::engine::ChangeEvent;
        use formualizer_common::LiteralValue;

        #[allow(clippy::type_complexity)]
        let rect_from_snapshot =
            |snap: &crate::engine::graph::editor::change_log::SpillSnapshot|
             -> Option<(SheetId, u32, u32, u32, u32, Vec<Vec<LiteralValue>>)> {
                if snap.target_cells.is_empty() {
                    return None;
                }
                let sheet_id = snap.target_cells[0].sheet_id;
                let sr0 = snap.target_cells[0].coord.row();
                let sc0 = snap.target_cells[0].coord.col();
                if snap.values.is_empty() || snap.values[0].is_empty() {
                    return None;
                }
                let h = snap.values.len() as u32;
                let w = snap.values[0].len() as u32;
                let er0 = sr0.saturating_add(h.saturating_sub(1));
                let ec0 = sc0.saturating_add(w.saturating_sub(1));
                Some((sheet_id, sr0, sc0, er0, ec0, snap.values.clone()))
            };

        for ev in events {
            match ev {
                ChangeEvent::SpillCommitted { old, new, .. } => {
                    if let Some((sid, sr0, sc0, er0, ec0, new_vals)) = rect_from_snapshot(new) {
                        let old_vals = if let Some(old_snap) = old {
                            rect_from_snapshot(old_snap)
                                .map(|(_, _, _, _, _, v)| v)
                                .unwrap_or_else(|| {
                                    vec![
                                        vec![LiteralValue::Empty; new_vals[0].len()];
                                        new_vals.len()
                                    ]
                                })
                        } else {
                            vec![vec![LiteralValue::Empty; new_vals[0].len()]; new_vals.len()]
                        };
                        undo.record_restore_computed_rect(
                            sid, sr0, sc0, er0, ec0, old_vals, new_vals,
                        );
                    }
                }
                ChangeEvent::SpillCleared { old, .. } => {
                    if let Some((sid, sr0, sc0, er0, ec0, old_vals)) = rect_from_snapshot(old) {
                        let new_vals =
                            vec![vec![LiteralValue::Empty; old_vals[0].len()]; old_vals.len()];
                        undo.record_restore_computed_rect(
                            sid, sr0, sc0, er0, ec0, old_vals, new_vals,
                        );
                    }
                }
                _ => {}
            }
        }
    }

    /// Mirror a value into the computed overlay (formula/spill outputs).
    ///
    /// This path is subject to `EvalConfig.max_overlay_memory_bytes`.
    /// If the cap is exceeded, computed overlays are compacted into base lanes.
    fn mirror_value_to_computed_overlay(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: &LiteralValue,
    ) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return;
        }
        if self.computed_overlay_mirroring_disabled {
            return;
        }

        let date_system = self.arrow_sheet_date_system(sheet);
        let ov = Self::literal_to_overlay_value(value, date_system);
        self.write_computed_overlay_value_0based(
            sheet,
            row.saturating_sub(1),
            col.saturating_sub(1),
            ov,
        );
    }

    fn record_derived_format(&self, vertex_id: VertexId, format: Option<crate::format::FormatId>) {
        if let Some(cell) = self.graph.get_cell_ref(vertex_id) {
            self.record_derived_format_at(cell, format);
        }
    }

    fn record_derived_format_at(&self, cell: CellRef, format: Option<crate::format::FormatId>) {
        #[cfg(test)]
        self.derived_format_operations_for_test
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let format = format.filter(|id| *id != crate::format::FormatId::GENERAL);
        self.derived_formats.set(cell, format);
    }

    fn clear_cell_format_state(&mut self, sheet: &str, cell: CellRef) {
        self.derived_formats.set(cell, None);
        if let Some(arrow) = self.arrow_sheets.sheet_mut(sheet) {
            arrow.clear_format(cell.coord.row() as usize, cell.coord.col() as usize);
        }
    }

    fn clear_logged_cell_format_states(&mut self, events: &[ChangeEvent]) {
        let cells = events
            .iter()
            .filter_map(|event| match event {
                ChangeEvent::SetValue { addr, .. } | ChangeEvent::SetFormula { addr, .. } => {
                    Some(*addr)
                }
                _ => None,
            })
            .collect::<FxHashSet<_>>();
        for cell in cells {
            let sheet = self.graph.sheet_name(cell.sheet_id).to_string();
            self.clear_cell_format_state(&sheet, cell);
        }
    }

    fn purge_derived_formats_after_row(&mut self, sheet_id: SheetId, start0: u32) {
        self.derived_formats
            .retain(|cell| cell.sheet_id != sheet_id || cell.coord.row() < start0);
    }

    fn purge_derived_formats_after_col(&mut self, sheet_id: SheetId, start0: u32) {
        self.derived_formats
            .retain(|cell| cell.sheet_id != sheet_id || cell.coord.col() < start0);
    }

    fn purge_derived_formats_for_sheet(&mut self, sheet_id: SheetId) {
        self.derived_formats
            .retain(|cell| cell.sheet_id != sheet_id);
    }

    #[cfg(test)]
    pub(crate) fn debug_computed_overlay_format_0based(
        &self,
        sheet: &str,
        row0: u32,
        col0: u32,
    ) -> Option<crate::format::FormatId> {
        let sheet = self.arrow_sheets.sheet(sheet)?;
        let (chunk_idx, row_in_chunk) = sheet.chunk_of_row(row0 as usize)?;
        sheet
            .columns
            .get(col0 as usize)?
            .chunk(chunk_idx)?
            .computed_overlay
            .get_format(row_in_chunk)
    }

    #[cfg(test)]
    pub(crate) fn debug_computed_overlay_chunk_has_formats_0based(
        &self,
        sheet: &str,
        row0: u32,
        col0: u32,
    ) -> bool {
        let Some(sheet) = self.arrow_sheets.sheet(sheet) else {
            return false;
        };
        let Some((chunk_idx, _)) = sheet.chunk_of_row(row0 as usize) else {
            return false;
        };
        sheet
            .columns
            .get(col0 as usize)
            .and_then(|column| column.chunk(chunk_idx))
            .is_some_and(|chunk| chunk.computed_overlay.has_formats())
    }

    #[cfg(test)]
    pub(crate) fn debug_clear_derived_format_0based(&mut self, sheet: &str, row0: u32, col0: u32) {
        if let Some(sheet_id) = self.graph.sheet_id(sheet) {
            self.derived_formats
                .set(CellRef::new_absolute(sheet_id, row0, col0), None);
        }
    }

    #[cfg(test)]
    pub(crate) fn debug_record_derived_format_0based(
        &self,
        sheet: &str,
        row0: u32,
        col0: u32,
        format: Option<crate::format::FormatId>,
    ) {
        if let Some(sheet_id) = self.graph.sheet_id(sheet) {
            self.record_derived_format_at(CellRef::new_absolute(sheet_id, row0, col0), format);
        }
    }

    #[cfg(test)]
    pub(crate) fn debug_derived_format_0based(
        &self,
        sheet: &str,
        row0: u32,
        col0: u32,
    ) -> Option<crate::format::FormatId> {
        let sheet_id = self.graph.sheet_id(sheet)?;
        self.derived_formats
            .get(&CellRef::new_absolute(sheet_id, row0, col0))
    }

    #[cfg(test)]
    pub(crate) fn debug_reset_format_write_operation_counts(&mut self) {
        self.derived_format_operations_for_test
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.computed_overlay_set_explicit_entry_operations_for_test = 0;
        self.computed_overlay_stale_clear_range_effects_for_test = 0;
        self.computed_overlay_stale_clear_offset_attempts_for_test = 0;
        self.computed_format_vector_allocations_for_test
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn debug_format_write_operation_counts(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.derived_format_operations_for_test
                .load(std::sync::atomic::Ordering::Relaxed),
            self.computed_overlay_set_explicit_entry_operations_for_test,
            self.computed_format_vector_allocations_for_test
                .load(std::sync::atomic::Ordering::Relaxed),
            self.computed_overlay_stale_clear_range_effects_for_test,
            self.computed_overlay_stale_clear_offset_attempts_for_test,
        )
    }

    fn write_computed_overlay_format_0based(
        &mut self,
        sheet: &str,
        row0: u32,
        col0: u32,
        format: Option<crate::format::FormatId>,
    ) {
        self.ensure_arrow_sheet(sheet);
        let (row0, col0) = (row0 as usize, col0 as usize);
        let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) else {
            return;
        };
        if col0 >= asheet.columns.len() {
            asheet.insert_columns(asheet.columns.len(), col0 + 1 - asheet.columns.len());
        }
        if row0 >= asheet.nrows as usize {
            asheet.ensure_row_capacity(row0 + 1);
        }
        let Some((chunk, offset)) = asheet.chunk_of_row(row0) else {
            return;
        };
        if let Some(chunk) = asheet.ensure_column_chunk_mut(col0, chunk) {
            chunk.computed_overlay.set_format(offset, format);
        }
    }

    /// One unbuffered computed write of `value` and its derived `format` at
    /// `cell` (a single sheet lookup; the same writes as
    /// `write_computed_overlay_value_0based` then
    /// `write_computed_overlay_format_0based`).
    fn write_computed_cell_0based(
        &mut self,
        cell: CellRef,
        value: &LiteralValue,
        format: Option<crate::format::FormatId>,
    ) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
            || self.computed_overlay_mirroring_disabled
        {
            return;
        }
        let sheet = self.graph.sheet_name(cell.sheet_id);
        let index = match self
            .arrow_sheets
            .sheets
            .iter()
            .position(|s| s.name.as_ref() == sheet)
        {
            Some(index) => index,
            None => {
                let sheet = sheet.to_string();
                self.ensure_arrow_sheet(&sheet);
                self.arrow_sheets.sheets.len() - 1
            }
        };
        let (row0, col0) = (cell.coord.row() as usize, cell.coord.col() as usize);
        let asheet = &mut self.arrow_sheets.sheets[index];
        let ov = Self::literal_to_overlay_value(value, asheet.date_system);
        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }
        if row0 >= asheet.nrows as usize {
            asheet.ensure_row_capacity(row0 + 1);
        }
        let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) else {
            return;
        };
        let Some(ch) = asheet.ensure_column_chunk_mut(col0, ch_idx) else {
            return;
        };
        let delta = ch.computed_overlay.set_scalar(in_off, ov);
        ch.computed_overlay.set_format(in_off, format);
        self.adjust_computed_overlay_bytes(delta);
        if let Some(cap) = self.config.max_overlay_memory_bytes
            && self.computed_overlay_bytes_estimate > cap
        {
            self.disable_computed_overlay_mirroring_due_to_budget(cap);
        }
    }

    fn write_computed_overlay_value_0based(
        &mut self,
        sheet: &str,
        row0: u32,
        col0: u32,
        value: OverlayValue,
    ) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return;
        }
        if self.computed_overlay_mirroring_disabled {
            return;
        }

        self.ensure_arrow_sheet(sheet);

        let row0 = row0 as usize;
        let col0 = col0 as usize;
        let asheet = self
            .arrow_sheets
            .sheet_mut(sheet)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }

        if row0 >= asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(row0 + 1);
        }

        let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) else {
            return;
        };
        let Some(ch) = asheet.ensure_column_chunk_mut(col0, ch_idx) else {
            return;
        };

        let delta = ch.computed_overlay.set_scalar(in_off, value);
        self.adjust_computed_overlay_bytes(delta);

        if let Some(cap) = self.config.max_overlay_memory_bytes
            && self.computed_overlay_bytes_estimate > cap
        {
            self.disable_computed_overlay_mirroring_due_to_budget(cap);
        }
    }

    pub(crate) fn plan_computed_write_coalescing(
        &self,
        buffer: &ComputedWriteBuffer,
    ) -> ComputedWriteCoalescingPlan {
        self.plan_computed_write_coalescing_from_writes(
            buffer.writes().iter().cloned(),
            buffer.formats_present,
        )
    }

    fn plan_owned_computed_write_coalescing(
        &self,
        writes: Vec<ComputedWrite>,
        formats_present: bool,
    ) -> ComputedWriteCoalescingPlan {
        self.plan_computed_write_coalescing_from_writes(writes, formats_present)
    }

    fn plan_computed_write_coalescing_from_writes(
        &self,
        writes: impl IntoIterator<Item = ComputedWrite>,
        formats_present: bool,
    ) -> ComputedWriteCoalescingPlan {
        let mut groups: BTreeMap<ComputedWriteChunkKey, Vec<ComputedWriteChunkEntryPlan>> =
            BTreeMap::new();
        let mut input_cells = 0usize;
        // One sheet lookup per sheet run of writes, not per cell.
        let mut located: Option<(SheetId, Option<&crate::arrow_store::ArrowSheet>)> = None;

        for write in writes {
            match write {
                ComputedWrite::Cell {
                    seq,
                    sheet_id,
                    row0,
                    col0,
                    value,
                    format_id,
                } => {
                    input_cells = input_cells.saturating_add(1);
                    let sheet = match located {
                        Some((id, sheet)) if id == sheet_id => sheet,
                        _ => {
                            let sheet = self.arrow_sheets.sheet(self.graph.sheet_name(sheet_id));
                            located = Some((sheet_id, sheet));
                            sheet
                        }
                    };
                    let (chunk_idx, chunk_start_row0, row_in_chunk) = match sheet {
                        Some(sheet) => {
                            Self::locate_row_in_sheet_for_computed_write_plan(sheet, row0 as usize)
                        }
                        None => Self::locate_row_in_empty_sheet_for_computed_write_plan(
                            row0 as usize,
                            32 * 1024,
                        ),
                    };
                    groups
                        .entry(ComputedWriteChunkKey {
                            sheet_id,
                            col0,
                            chunk_idx,
                            chunk_start_row0,
                        })
                        .or_default()
                        .push(ComputedWriteChunkEntryPlan {
                            row_in_chunk,
                            seq,
                            value,
                            format_id,
                        });
                }
                ComputedWrite::Run {
                    seq,
                    sheet_id,
                    row0,
                    col0,
                    entries,
                } => {
                    input_cells = input_cells.saturating_add(entries.len());
                    let sheet = match located {
                        Some((id, sheet)) if id == sheet_id => sheet,
                        _ => {
                            let sheet = self.arrow_sheets.sheet(self.graph.sheet_name(sheet_id));
                            located = Some((sheet_id, sheet));
                            sheet
                        }
                    };
                    // Rows are located one by one (a binary search, no sheet
                    // lookup); the group map is touched once per chunk
                    // segment of the run.
                    let mut segment: Vec<ComputedWriteChunkEntryPlan> = Vec::new();
                    let mut segment_key: Option<ComputedWriteChunkKey> = None;
                    for (k, (value, format_id)) in entries.into_iter().enumerate() {
                        let row = row0.saturating_add(k as u32) as usize;
                        let (chunk_idx, chunk_start_row0, row_in_chunk) = match sheet {
                            Some(sheet) => {
                                Self::locate_row_in_sheet_for_computed_write_plan(sheet, row)
                            }
                            None => Self::locate_row_in_empty_sheet_for_computed_write_plan(
                                row,
                                32 * 1024,
                            ),
                        };
                        let key = ComputedWriteChunkKey {
                            sheet_id,
                            col0,
                            chunk_idx,
                            chunk_start_row0,
                        };
                        if segment_key != Some(key)
                            && let Some(done) = segment_key.replace(key)
                        {
                            groups.entry(done).or_default().append(&mut segment);
                        }
                        segment.push(ComputedWriteChunkEntryPlan {
                            row_in_chunk,
                            seq,
                            value,
                            format_id,
                        });
                    }
                    if let Some(done) = segment_key {
                        groups.entry(done).or_default().append(&mut segment);
                    }
                }
                ComputedWrite::Rect {
                    seq,
                    sheet_id,
                    sr0,
                    sc0,
                    values,
                } => {
                    for (r_off, row) in values.into_iter().enumerate() {
                        for (c_off, value) in row.into_iter().enumerate() {
                            input_cells = input_cells.saturating_add(1);
                            self.push_computed_write_plan_entry(
                                &mut groups,
                                seq,
                                sheet_id,
                                sr0.saturating_add(r_off as u32),
                                sc0.saturating_add(c_off as u32),
                                value,
                                None,
                            );
                        }
                    }
                }
            }
        }

        let mut plan = ComputedWriteCoalescingPlan {
            chunks: Vec::with_capacity(groups.len()),
            input_cells,
            coalesced_cells: 0,
            overwritten_cells: 0,
        };
        for (key, entries) in groups {
            let computed_lane_has_formats =
                self.computed_overlay_chunk_has_formats(key.sheet_id, key.col0, key.chunk_idx);
            let (chunk_plan, overwritten) = ComputedWriteChunkPlan::from_group(
                key,
                entries,
                formats_present,
                computed_lane_has_formats,
            );
            #[cfg(test)]
            if matches!(
                &chunk_plan.format_effect,
                ComputedWriteChunkFormatEffect::SetExplicit(_)
            ) {
                self.computed_format_vector_allocations_for_test
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            plan.coalesced_cells = plan
                .coalesced_cells
                .saturating_add(chunk_plan.entries.len());
            plan.overwritten_cells = plan.overwritten_cells.saturating_add(overwritten);
            plan.chunks.push(chunk_plan);
        }
        debug_assert_eq!(
            plan.input_cells,
            plan.coalesced_cells.saturating_add(plan.overwritten_cells)
        );
        plan
    }

    fn push_computed_write_plan_entry(
        &self,
        groups: &mut BTreeMap<ComputedWriteChunkKey, Vec<ComputedWriteChunkEntryPlan>>,
        seq: u64,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        value: OverlayValue,
        format_id: Option<crate::format::FormatId>,
    ) {
        let (chunk_idx, chunk_start_row0, row_in_chunk) =
            self.locate_computed_write_chunk(sheet_id, row0);
        let key = ComputedWriteChunkKey {
            sheet_id,
            col0,
            chunk_idx,
            chunk_start_row0,
        };
        groups
            .entry(key)
            .or_default()
            .push(ComputedWriteChunkEntryPlan {
                row_in_chunk,
                seq,
                value,
                format_id,
            });
    }

    fn computed_overlay_chunk_has_formats(
        &self,
        sheet_id: SheetId,
        col0: u32,
        chunk_idx: usize,
    ) -> bool {
        let sheet_name = self.graph.sheet_name(sheet_id);
        self.arrow_sheets
            .sheet(sheet_name)
            .and_then(|sheet| sheet.columns.get(col0 as usize))
            .and_then(|column| column.chunk(chunk_idx))
            .is_some_and(|chunk| chunk.computed_overlay.has_formats())
    }

    fn locate_computed_write_chunk(&self, sheet_id: SheetId, row0: u32) -> (usize, u32, usize) {
        let sheet_name = self.graph.sheet_name(sheet_id);
        if let Some(sheet) = self.arrow_sheets.sheet(sheet_name) {
            return Self::locate_row_in_sheet_for_computed_write_plan(sheet, row0 as usize);
        }
        Self::locate_row_in_empty_sheet_for_computed_write_plan(row0 as usize, 32 * 1024)
    }

    fn locate_row_in_sheet_for_computed_write_plan(
        sheet: &crate::arrow_store::ArrowSheet,
        row0: usize,
    ) -> (usize, u32, usize) {
        if row0 < sheet.nrows as usize
            && let Some((chunk_idx, row_in_chunk)) = sheet.chunk_of_row(row0)
        {
            let chunk_start = sheet.chunk_starts.get(chunk_idx).copied().unwrap_or(0);
            return (chunk_idx, chunk_start as u32, row_in_chunk);
        }

        let chunk_rows = sheet.chunk_rows.max(1);
        if sheet.chunk_starts.is_empty() {
            return Self::locate_row_in_empty_sheet_for_computed_write_plan(row0, chunk_rows);
        }

        let mut chunk_idx = sheet.chunk_starts.len().saturating_sub(1);
        let mut chunk_start = sheet.chunk_starts[chunk_idx];
        while chunk_start.saturating_add(chunk_rows) <= row0 {
            chunk_idx = chunk_idx.saturating_add(1);
            chunk_start = chunk_start.saturating_add(chunk_rows);
        }
        (
            chunk_idx,
            chunk_start as u32,
            row0.saturating_sub(chunk_start),
        )
    }

    fn locate_row_in_empty_sheet_for_computed_write_plan(
        row0: usize,
        chunk_rows: usize,
    ) -> (usize, u32, usize) {
        let chunk_rows = chunk_rows.max(1);
        let chunk_idx = row0 / chunk_rows;
        let chunk_start = chunk_idx.saturating_mul(chunk_rows);
        (
            chunk_idx,
            chunk_start as u32,
            row0.saturating_sub(chunk_start),
        )
    }

    #[cfg(test)]
    pub(crate) fn debug_plan_computed_write_coalescing(
        &self,
        buffer: &ComputedWriteBuffer,
    ) -> ComputedWriteCoalescingPlan {
        self.plan_computed_write_coalescing(buffer)
    }

    pub(crate) fn flush_computed_write_buffer(
        &mut self,
        buffer: &mut ComputedWriteBuffer,
    ) -> Result<(), ExcelError> {
        if buffer.is_empty() {
            return Ok(());
        }

        // Keep ownership of all pending writes until the final request
        // checkpoint and bounded commit-window preflight succeed so failures
        // remain retry safe. The synchronization and flush that follow are
        // infallible mutations with no cancellation point.
        self.resource_checkpoint(0)?;
        let commit_started = self.preflight_evaluation_commit_window(buffer.len())?;
        let (writes, formats_present) = buffer.take_writes();
        let plan = self.plan_owned_computed_write_coalescing(writes, formats_present);
        self.flush_computed_write_plan(plan);
        self.observe_evaluation_commit_window(commit_started);

        Ok(())
    }

    fn flush_computed_write_plan(&mut self, plan: ComputedWriteCoalescingPlan) {
        for chunk in plan.chunks {
            self.flush_computed_write_chunk_plan(chunk);
        }
    }

    fn flush_computed_write_chunk_plan(&mut self, chunk: ComputedWriteChunkPlan) {
        match &chunk.shape {
            ComputedWriteChunkPlanShape::Point => {
                self.flush_computed_write_chunk_plan_as_points(chunk);
            }
            ComputedWriteChunkPlanShape::SparseOffsets { .. } => {
                self.flush_computed_write_chunk_plan_as_sparse_fragment_or_points(chunk);
            }
            ComputedWriteChunkPlanShape::DenseRange { .. } => {
                self.flush_computed_write_chunk_plan_as_dense_fragment(chunk);
            }
            ComputedWriteChunkPlanShape::RunRange { len, runs, .. } => {
                if Self::should_emit_computed_run_fragment(*len, *runs) {
                    self.flush_computed_write_chunk_plan_as_run_fragment(chunk);
                } else {
                    self.flush_computed_write_chunk_plan_as_dense_fragment(chunk);
                }
            }
        }
    }

    #[inline]
    fn should_emit_computed_run_fragment(len: usize, runs: usize) -> bool {
        runs <= len / 2
    }

    fn flush_computed_write_chunk_plan_as_points(&mut self, chunk: ComputedWriteChunkPlan) {
        let sheet_name = self.graph.sheet_name(chunk.sheet_id).to_string();
        for entry in chunk.entries {
            let row0 = chunk
                .chunk_start_row0
                .saturating_add(entry.row_in_chunk as u32);
            self.write_computed_overlay_value_0based(&sheet_name, row0, chunk.col0, entry.value);
        }
        self.apply_computed_overlay_format_effect(
            chunk.sheet_id,
            chunk.col0,
            chunk.chunk_idx,
            chunk.format_effect,
        );
    }

    fn flush_computed_write_chunk_plan_as_sparse_fragment_or_points(
        &mut self,
        chunk: ComputedWriteChunkPlan,
    ) {
        let point_estimate = Self::computed_write_chunk_plan_point_estimate(&chunk);
        let sheet_id = chunk.sheet_id;
        let col0 = chunk.col0;
        let chunk_idx = chunk.chunk_idx;
        let chunk_start_row0 = chunk.chunk_start_row0;
        let format_effect = chunk.format_effect;
        let items: Vec<(usize, OverlayValue)> = chunk
            .entries
            .into_iter()
            .map(|entry| (entry.row_in_chunk, entry.value))
            .collect();
        match OverlayFragment::sparse_offsets_if_estimated_smaller_than_points(
            items,
            point_estimate,
        ) {
            Some(Ok(fragment)) => {
                self.apply_computed_overlay_fragment(sheet_id, col0, chunk_idx, fragment);
            }
            Some(Err(cells)) => {
                self.flush_computed_overlay_cells_as_points(
                    sheet_id,
                    col0,
                    chunk_start_row0,
                    cells,
                );
            }
            None => {}
        }
        self.apply_computed_overlay_format_effect(sheet_id, col0, chunk_idx, format_effect);
    }

    #[inline]
    fn computed_write_chunk_plan_point_estimate(chunk: &ComputedWriteChunkPlan) -> usize {
        chunk
            .entries
            .iter()
            .map(|entry| ComputedWriteBuffer::estimate_value_bytes(&entry.value))
            .fold(0usize, usize::saturating_add)
    }

    fn flush_computed_overlay_cells_as_points(
        &mut self,
        sheet_id: SheetId,
        col0: u32,
        chunk_start_row0: u32,
        cells: Vec<(usize, OverlayValue)>,
    ) {
        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        for (row_in_chunk, value) in cells {
            let row0 = chunk_start_row0.saturating_add(row_in_chunk as u32);
            self.write_computed_overlay_value_0based(&sheet_name, row0, col0, value);
        }
    }

    fn flush_computed_write_chunk_plan_as_dense_fragment(&mut self, chunk: ComputedWriteChunkPlan) {
        if chunk.entries.is_empty() {
            return;
        }
        let start = chunk.entries[0].row_in_chunk;
        let values: Vec<OverlayValue> =
            chunk.entries.into_iter().map(|entry| entry.value).collect();
        if let Some(fragment) = OverlayFragment::dense_range(start, values) {
            self.apply_computed_overlay_fragment(
                chunk.sheet_id,
                chunk.col0,
                chunk.chunk_idx,
                fragment,
            );
        }
        self.apply_computed_overlay_format_effect(
            chunk.sheet_id,
            chunk.col0,
            chunk.chunk_idx,
            chunk.format_effect,
        );
    }

    fn flush_computed_write_chunk_plan_as_run_fragment(&mut self, chunk: ComputedWriteChunkPlan) {
        if chunk.entries.is_empty() {
            return;
        }
        let start = chunk.entries[0].row_in_chunk;
        let values: Vec<OverlayValue> =
            chunk.entries.into_iter().map(|entry| entry.value).collect();
        if let Some(fragment) = OverlayFragment::run_range(start, values) {
            self.apply_computed_overlay_fragment(
                chunk.sheet_id,
                chunk.col0,
                chunk.chunk_idx,
                fragment,
            );
        }
        self.apply_computed_overlay_format_effect(
            chunk.sheet_id,
            chunk.col0,
            chunk.chunk_idx,
            chunk.format_effect,
        );
    }

    fn apply_computed_overlay_format_effect(
        &mut self,
        sheet_id: SheetId,
        col0: u32,
        chunk_idx: usize,
        effect: ComputedWriteChunkFormatEffect,
    ) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
            || self.computed_overlay_mirroring_disabled
        {
            return;
        }

        let sheet_name = self.graph.sheet_name(sheet_id);
        let Some(sheet) = self.arrow_sheets.sheet_mut(sheet_name) else {
            return;
        };
        let Some(chunk) = sheet
            .columns
            .get_mut(col0 as usize)
            .and_then(|column| column.chunk_mut(chunk_idx))
        else {
            return;
        };
        match effect {
            ComputedWriteChunkFormatEffect::NoFormatWork => {}
            ComputedWriteChunkFormatEffect::ClearStale(ComputedWriteFormatClear::Range {
                start,
                end,
            }) => {
                #[cfg(test)]
                {
                    self.computed_overlay_stale_clear_range_effects_for_test = self
                        .computed_overlay_stale_clear_range_effects_for_test
                        .saturating_add(1);
                }
                chunk.computed_overlay.clear_format_range(start, end);
            }
            ComputedWriteChunkFormatEffect::ClearStale(ComputedWriteFormatClear::Offsets(
                offsets,
            )) => {
                #[cfg(test)]
                {
                    self.computed_overlay_stale_clear_offset_attempts_for_test = self
                        .computed_overlay_stale_clear_offset_attempts_for_test
                        .saturating_add(offsets.len() as u64);
                }
                chunk.computed_overlay.clear_format_offsets(&offsets);
            }
            ComputedWriteChunkFormatEffect::SetExplicit(formats) => {
                #[cfg(test)]
                {
                    self.computed_overlay_set_explicit_entry_operations_for_test = self
                        .computed_overlay_set_explicit_entry_operations_for_test
                        .saturating_add(formats.len() as u64);
                }
                for (row_in_chunk, format_id) in formats {
                    chunk.computed_overlay.set_format(row_in_chunk, format_id);
                }
            }
        }
    }

    fn apply_computed_overlay_fragment(
        &mut self,
        sheet_id: SheetId,
        col0: u32,
        chunk_idx: usize,
        fragment: OverlayFragment,
    ) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return;
        }
        if self.computed_overlay_mirroring_disabled {
            return;
        }

        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        self.ensure_arrow_sheet(&sheet_name);

        let col0 = col0 as usize;
        let asheet = self
            .arrow_sheets
            .sheet_mut(&sheet_name)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }

        let start_row0 = asheet
            .chunk_starts
            .get(chunk_idx)
            .copied()
            .unwrap_or_else(|| chunk_idx.saturating_mul(asheet.chunk_rows.max(1)));
        let required_rows =
            start_row0.saturating_add(fragment.max_covered_offset().saturating_add(1));
        if required_rows > asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(required_rows);
        }

        let Some(ch) = asheet.ensure_column_chunk_mut(col0, chunk_idx) else {
            return;
        };
        let delta = ch.computed_overlay.apply_fragment(fragment);
        self.adjust_computed_overlay_bytes(delta);

        if let Some(cap) = self.config.max_overlay_memory_bytes
            && self.computed_overlay_bytes_estimate > cap
        {
            self.disable_computed_overlay_mirroring_due_to_budget(cap);
        }
    }

    #[inline]
    fn adjust_computed_overlay_bytes(&mut self, delta: isize) {
        if delta >= 0 {
            self.computed_overlay_bytes_estimate = self
                .computed_overlay_bytes_estimate
                .saturating_add(delta as usize);
        } else {
            self.computed_overlay_bytes_estimate = self
                .computed_overlay_bytes_estimate
                .saturating_sub((-delta) as usize);
        }
    }

    fn clear_all_computed_overlays(&mut self) {
        let mut freed_total = 0usize;
        for sh in self.arrow_sheets.sheets.iter_mut() {
            for col in sh.columns.iter_mut() {
                for ch in col.chunks.iter_mut() {
                    freed_total = freed_total.saturating_add(ch.computed_overlay.clear());
                }
                for ch in col.sparse_chunks.values_mut() {
                    freed_total = freed_total.saturating_add(ch.computed_overlay.clear());
                }
            }
        }
        self.computed_overlay_bytes_estimate = self
            .computed_overlay_bytes_estimate
            .saturating_sub(freed_total);
    }

    fn disable_computed_overlay_mirroring_due_to_budget(&mut self, _cap: usize) {
        // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
        // Handle budget pressure by compacting computed overlays into base lanes.
        self.compact_all_computed_overlays();
    }

    /// Fold all computed overlay entries across all sheets into their base arrays.
    /// This preserves data while freeing overlay memory, allowing mirroring to continue.
    fn compact_all_computed_overlays(&mut self) {
        let mut freed_total = 0usize;
        for sheet in self.arrow_sheets.sheets.iter_mut() {
            for col_idx in 0..sheet.columns.len() {
                // Dense chunks
                let num_dense = sheet.columns[col_idx].chunks.len();
                for ch_idx in 0..num_dense {
                    freed_total += sheet.compact_computed_overlay_chunk(col_idx, ch_idx);
                }
                // Sparse chunks
                let sparse_keys: Vec<usize> = sheet.columns[col_idx]
                    .sparse_chunks
                    .keys()
                    .copied()
                    .collect();
                for ch_idx in sparse_keys {
                    freed_total += sheet.compact_computed_overlay_sparse_chunk(col_idx, ch_idx);
                }
            }
        }
        self.computed_overlay_bytes_estimate = self
            .computed_overlay_bytes_estimate
            .saturating_sub(freed_total);
        self.overlay_compactions = self.overlay_compactions.saturating_add(1);
    }

    fn mirror_vertex_value_to_overlay(&mut self, vertex_id: VertexId, value: &LiteralValue) {
        let _ = self.record_vertex_value_to_overlay(vertex_id, value, None);
    }

    fn record_vertex_value_to_overlay(
        &mut self,
        vertex_id: VertexId,
        value: &LiteralValue,
        computed_writes: Option<&mut ComputedWriteBuffer>,
    ) -> Result<(), ExcelError> {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return Ok(());
        }
        if self.computed_overlay_mirroring_disabled {
            return Ok(());
        }
        if !matches!(
            self.graph.get_vertex_kind(vertex_id),
            VertexKind::FormulaScalar | VertexKind::FormulaArray
        ) {
            return Ok(());
        }
        let Some(cell) = self.graph.get_cell_ref(vertex_id) else {
            return Ok(());
        };
        let Some(buffer) = computed_writes else {
            // Unbuffered: one sheet lookup for the value and its format.
            let format_id = self.derived_formats.get(&cell);
            self.write_computed_cell_0based(cell, value, format_id);
            return Ok(());
        };
        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
        let date_system = self.arrow_sheet_date_system(&sheet_name);
        let ov = Self::literal_to_overlay_value(value, date_system);
        {
            let format_id = self.derived_formats.get(&cell);
            buffer.push_cell_with_format(
                cell.sheet_id,
                cell.coord.row(),
                cell.coord.col(),
                ov,
                format_id,
            );
            if self.should_flush_computed_write_buffer(buffer) {
                self.flush_computed_write_buffer(buffer)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn should_flush_computed_write_buffer(&self, buffer: &ComputedWriteBuffer) -> bool {
        self.config.max_overlay_memory_bytes.is_some_and(|cap| {
            if cap == 0 {
                return false;
            }
            self.computed_overlay_bytes_estimate
                .saturating_add(buffer.estimated_bytes())
                > cap
        })
    }

    /// Estimated memory usage for computed overlays (formula/spill mirroring).
    pub fn overlay_memory_usage(&self) -> usize {
        self.computed_overlay_bytes_estimate
    }

    #[cfg(test)]
    pub(crate) fn debug_overlay_compactions(&self) -> u64 {
        self.overlay_compactions
    }

    #[cfg(test)]
    pub(crate) fn debug_recompute_computed_overlay_bytes(&mut self) -> usize {
        let mut total = 0usize;
        for sheet in &self.arrow_sheets.sheets {
            for column in &sheet.columns {
                for chunk in &column.chunks {
                    total = total.saturating_add(chunk.computed_overlay.estimated_bytes());
                }
                for chunk in column.sparse_chunks.values() {
                    total = total.saturating_add(chunk.computed_overlay.estimated_bytes());
                }
            }
        }
        self.computed_overlay_bytes_estimate = total;
        total
    }

    fn resolve_sheet_locator_for_write(
        &mut self,
        loc: formualizer_common::SheetLocator<'_>,
        current_sheet: &str,
    ) -> Result<SheetId, ExcelError> {
        Ok(match loc {
            formualizer_common::SheetLocator::Id(id) => id,
            formualizer_common::SheetLocator::Name(name) => self.graph.sheet_id_mut(name.as_ref()),
            formualizer_common::SheetLocator::Current => self.graph.sheet_id_mut(current_sheet),
        })
    }

    fn resolve_sheet_locator_for_read(
        &self,
        loc: formualizer_common::SheetLocator<'_>,
        current_sheet: &str,
    ) -> Result<SheetId, ExcelError> {
        match loc {
            formualizer_common::SheetLocator::Id(id) => Ok(id),
            formualizer_common::SheetLocator::Name(name) => self
                .graph
                .sheet_id(name.as_ref())
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref)),
            formualizer_common::SheetLocator::Current => self
                .graph
                .sheet_id(current_sheet)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref)),
        }
    }

    /// Set a cell value
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        self.observe_function_semantic_epoch()?;
        let sheet_existed = self.graph.sheet_id(sheet).is_some();
        let sheet_id = self.graph.sheet_id_mut(sheet);
        let cell_ref = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
        let replaced_formula =
            self.graph
                .get_vertex_id_for_address(&cell_ref)
                .is_some_and(|vertex| {
                    matches!(
                        self.graph.get_vertex_kind(vertex),
                        VertexKind::FormulaScalar | VertexKind::FormulaArray
                    )
                });
        self.graph.set_cell_value(sheet, row, col, value.clone())?;
        self.clear_cell_format_state(sheet, cell_ref);
        self.record_changed_cell(sheet, row, col);
        if !sheet_existed || replaced_formula {
            self.mark_topology_edited();
        }
        // Mirror into Arrow overlay when enabled
        self.mirror_value_to_overlay(sheet, row, col, &value);
        // Advance snapshot to reflect external mutation.
        self.mark_data_edited();
        Ok(())
    }

    /// Record a single-cell change: invalidates pending spills blocked on it.
    fn record_changed_cell(&mut self, sheet: &str, row: u32, col: u32) {
        let sheet_id = self.graph.sheet_id_mut(sheet);
        self.record_structural_change(StructuralScope::Cell {
            sheet: sheet_id,
            row: row.saturating_sub(1),
            col: col.saturating_sub(1),
        });
    }

    fn record_change_for_event(&mut self, event: &ChangeEvent) {
        match event {
            ChangeEvent::SetValue { addr, .. } | ChangeEvent::SetFormula { addr, .. } => {
                self.record_structural_change(StructuralScope::Cell {
                    sheet: addr.sheet_id,
                    row: addr.coord.row(),
                    col: addr.coord.col(),
                });
            }
            ChangeEvent::SpillCommitted { new, .. } => {
                if let Some(scope) = Self::structural_scope_from_cells(&new.target_cells) {
                    self.record_structural_change(scope);
                }
            }
            ChangeEvent::SpillCleared { old, .. } => {
                if let Some(scope) = Self::structural_scope_from_cells(&old.target_cells) {
                    self.record_structural_change(scope);
                }
            }
            ChangeEvent::DefineName { .. }
            | ChangeEvent::UpdateName { .. }
            | ChangeEvent::DeleteName { .. }
            | ChangeEvent::NamedRangeAdjusted { .. } => {
                // Direct name events are preflighted by the logged-name APIs.
                // Structural entry points preflight spans before emitting a
                // NamedRangeAdjusted event. Epoch changes only rebuild caches.
                self.record_structural_change(StructuralScope::AllSheets);
            }
            ChangeEvent::VertexMoved { .. } | ChangeEvent::FormulaAdjusted { .. } => {
                // Structural entry points publish their axis delta once after
                // the graph and Arrow commits.
            }
            ChangeEvent::SetRowVisibility { sheet_id, row0, .. } => {
                self.record_structural_change(StructuralScope::Region(Region::whole_row(
                    *sheet_id, *row0,
                )));
            }
            ChangeEvent::AddVertex { .. }
            | ChangeEvent::RemoveVertex { .. }
            | ChangeEvent::EdgeAdded { .. }
            | ChangeEvent::EdgeRemoved { .. }
            | ChangeEvent::CompoundStart { .. }
            | ChangeEvent::CompoundEnd { .. }
            | ChangeEvent::StagedFormulaCellChanged { .. } => {}
        }
    }

    fn record_structural_change(&mut self, scope: StructuralScope) {
        self.invalidate_pending_spills(scope);
    }

    fn structural_scope_from_cells(cells: &[CellRef]) -> Option<StructuralScope> {
        let first = cells.first()?;
        let sheet_id = first.sheet_id;
        if cells.iter().any(|cell| cell.sheet_id != sheet_id) {
            return Some(StructuralScope::OpaqueGlobal);
        }
        let mut row_start = first.coord.row();
        let mut row_end = row_start;
        let mut col_start = first.coord.col();
        let mut col_end = col_start;
        for cell in cells.iter().skip(1) {
            row_start = row_start.min(cell.coord.row());
            row_end = row_end.max(cell.coord.row());
            col_start = col_start.min(cell.coord.col());
            col_end = col_end.max(cell.coord.col());
        }
        Some(StructuralScope::Region(Region::rect(
            sheet_id, row_start, row_end, col_start, col_end,
        )))
    }

    pub fn set_cell_value_ref(
        &mut self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_write(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        self.set_cell_value(
            &sheet_name,
            owned.coord.row() + 1,
            owned.coord.col() + 1,
            value,
        )
    }

    pub fn set_cell_formula_ref(
        &mut self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
        ast: ASTNode,
    ) -> Result<(), ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_write(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        self.set_cell_formula(
            &sheet_name,
            owned.coord.row() + 1,
            owned.coord.col() + 1,
            ast,
        )
    }

    pub fn get_cell_value_ref(
        &self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_read(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id);
        Ok(self.get_cell_value(sheet_name, owned.coord.row() + 1, owned.coord.col() + 1))
    }

    pub fn resolve_range_view_sheet_ref<'c>(
        &'c self,
        r: &formualizer_common::SheetRef<'_>,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        use formualizer_common::SheetLocator;

        let sheet_to_opt_name = |loc: SheetLocator<'_>| -> Result<Option<String>, ExcelError> {
            match loc {
                SheetLocator::Current => Ok(None),
                SheetLocator::Name(name) => Ok(Some(name.as_ref().to_string())),
                SheetLocator::Id(id) => Ok(Some(self.graph.sheet_name(id).to_string())),
            }
        };

        let rt = match r {
            formualizer_common::SheetRef::Cell(cell) => ReferenceType::Cell {
                sheet: sheet_to_opt_name(cell.sheet.clone())?,
                row: cell.coord.row() + 1,
                col: cell.coord.col() + 1,
                row_abs: cell.coord.row_abs(),
                col_abs: cell.coord.col_abs(),
            },
            formualizer_common::SheetRef::Range(range) => ReferenceType::Range {
                sheet: sheet_to_opt_name(range.sheet.clone())?,
                start_row: range.start_row.map(|b| b.index + 1),
                start_col: range.start_col.map(|b| b.index + 1),
                end_row: range.end_row.map(|b| b.index + 1),
                end_col: range.end_col.map(|b| b.index + 1),
                start_row_abs: range.start_row.map(|b| b.abs).unwrap_or(false),
                start_col_abs: range.start_col.map(|b| b.abs).unwrap_or(false),
                end_row_abs: range.end_row.map(|b| b.abs).unwrap_or(false),
                end_col_abs: range.end_col.map(|b| b.abs).unwrap_or(false),
            },
        };

        crate::traits::EvaluationContext::resolve_range_view(self, &rt, current_sheet)
    }

    /// Set a cell formula
    pub fn set_cell_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
    ) -> Result<(), ExcelError> {
        self.observe_function_semantic_epoch()?;
        let sheet_id = self.graph.sheet_id_mut(sheet);
        let placement = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
        let ingested = {
            let mut pipeline = self.ingest_pipeline();
            pipeline.ingest_formula(FormulaAstInput::Tree(ast), placement, None)?
        };
        self.graph.set_cell_formula_with_plan(
            sheet,
            row,
            col,
            ingested.ast_id,
            &ingested.dep_plan,
            ingested.dep_plan.volatile,
            ingested.dep_plan.dynamic,
        )?;
        self.clear_cell_format_state(sheet, placement);
        self.record_changed_cell(sheet, row, col);

        // If the cell previously held a user value in the delta overlay, it must not continue
        // to mask the formula result under Arrow-canonical reads (overlay precedence is
        // delta -> computed -> base). Remove the overlay entry instead of writing `Empty`,
        // because an explicit `Empty` overlay would still take precedence over computed values.
        self.clear_delta_overlay_cell(sheet, row, col);

        // Advance snapshot to reflect external mutation
        self.mark_topology_edited();
        Ok(())
    }

    /// Bulk set many formulas on a sheet. Skips per-cell snapshot bumping and minimizes edge rebuilds.
    pub fn bulk_set_formulas<I>(&mut self, sheet: &str, items: I) -> Result<usize, ExcelError>
    where
        I: IntoIterator<Item = (u32, u32, ASTNode)>,
    {
        let collected: Vec<(u32, u32, ASTNode)> = items.into_iter().collect();
        let edited_cells: Vec<(u32, u32)> = collected.iter().map(|(r, c, _)| (*r, *c)).collect();
        let sheet_id = self.graph.sheet_id_mut(sheet);
        let ingested = {
            let mut pipeline = self.ingest_pipeline();
            let inputs = collected.into_iter().map(|(row, col, ast)| {
                let placement = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
                (FormulaAstInput::Tree(ast), placement, None)
            });
            pipeline.ingest_batch(inputs)?
        };
        let planned: Vec<(u32, u32, AstNodeId, DependencyPlanRow)> = ingested
            .into_iter()
            .map(|formula| {
                (
                    formula.placement.coord.row() + 1,
                    formula.placement.coord.col() + 1,
                    formula.ast_id,
                    formula.dep_plan,
                )
            })
            .collect();
        let n = self.graph.bulk_set_formulas_with_plans(sheet, planned)?;
        for (row, col) in edited_cells {
            let cell = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
            self.clear_cell_format_state(sheet, cell);
            self.record_changed_cell(sheet, row, col);
        }
        // Single topology bump after batch
        if n > 0 {
            self.mark_topology_edited();
        }
        Ok(n)
    }

    #[inline]
    fn normalize_public_cell_read(v: LiteralValue) -> Option<LiteralValue> {
        match v {
            LiteralValue::Empty => None,
            LiteralValue::Int(i) => Some(LiteralValue::Number(i as f64)),
            other => Some(other),
        }
    }

    fn materialize_temporal_egress(
        value: LiteralValue,
        class: Option<&formualizer_common::numfmt::FormatClass>,
        policy: crate::engine::TemporalEgress,
        date_system: crate::engine::DateSystem,
    ) -> LiteralValue {
        use formualizer_common::numfmt::FormatClass;
        if policy == crate::engine::TemporalEgress::Serial {
            return value;
        }
        let LiteralValue::Number(serial) = value else {
            return value;
        };
        match class {
            Some(FormatClass::Date) => {
                formualizer_common::try_serial_to_date_for(date_system, serial)
                    .map(LiteralValue::Date)
                    .unwrap_or(LiteralValue::Number(serial))
            }
            Some(FormatClass::DateTime) => {
                formualizer_common::try_serial_to_datetime_for(date_system, serial)
                    .map(LiteralValue::DateTime)
                    .unwrap_or(LiteralValue::Number(serial))
            }
            Some(FormatClass::Time) => {
                let seconds = (serial.rem_euclid(1.0) * 86_400.0).round() as u32 % 86_400;
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds, 0)
                    .map(LiteralValue::Time)
                    .unwrap_or(LiteralValue::Number(serial))
            }
            Some(FormatClass::Duration) => {
                let nanos = (serial * 86_400.0 * 1_000_000_000.0).round();
                if nanos.is_finite() && nanos >= i64::MIN as f64 && nanos <= i64::MAX as f64 {
                    LiteralValue::Duration(chrono::Duration::nanoseconds(nanos as i64))
                } else {
                    LiteralValue::Number(serial)
                }
            }
            _ => LiteralValue::Number(serial),
        }
    }

    pub(crate) fn effective_format_id(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Option<crate::format::FormatId> {
        let arrow = self.arrow_sheets.sheet(sheet).and_then(|arrow| {
            arrow.format_id(
                row.saturating_sub(1) as usize,
                col.saturating_sub(1) as usize,
            )
        });
        arrow.or_else(|| {
            let sheet_id = self.graph.sheet_id(sheet)?;
            let cell = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
            self.derived_formats.get(&cell)
        })
    }

    /// Get a cell value through the single temporal egress boundary.
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        let raw = self.read_cell_value(sheet, row, col)?;
        let format = self.effective_format_id(sheet, row, col);
        let class = format.and_then(|id| self.format_registry.class(id));
        Self::normalize_public_cell_read(Self::materialize_temporal_egress(
            raw,
            class,
            self.config.temporal_egress,
            self.config.date_system,
        ))
    }

    /// Read a rectangular range through the temporal egress boundary.
    pub fn get_range_values(
        &self,
        sheet: &str,
        sr: u32,
        sc: u32,
        er: u32,
        ec: u32,
    ) -> Vec<Vec<LiteralValue>> {
        let height = er.saturating_sub(sr).saturating_add(1) as usize;
        let width = ec.saturating_sub(sc).saturating_add(1) as usize;
        let Some(asheet) = self.sheet_store().sheet(sheet) else {
            return vec![vec![LiteralValue::Empty; width]; height];
        };
        let view = asheet.range_view(
            sr.saturating_sub(1) as usize,
            sc.saturating_sub(1) as usize,
            er.saturating_sub(1) as usize,
            ec.saturating_sub(1) as usize,
        );
        let sheet_id = self.graph.sheet_id(sheet);
        let derived_formats = &self.derived_formats;
        let has_derived_formats =
            sheet_id.is_some_and(|sheet_id| derived_formats.any(|cell| cell.sheet_id == sheet_id));
        let mut out = Vec::with_capacity(height);
        if !asheet.has_formats() && !has_derived_formats {
            for rr in 0..height {
                let mut row = Vec::with_capacity(width);
                for cc in 0..width {
                    row.push(view.get_cell(rr, cc));
                }
                out.push(row);
            }
            return out;
        }
        let format_registry = &self.format_registry;
        for rr in 0..height {
            let mut row = Vec::with_capacity(width);
            for cc in 0..width {
                let raw = view.get_cell(rr, cc);
                let row0 = sr.saturating_sub(1).saturating_add(rr as u32);
                let col0 = sc.saturating_sub(1).saturating_add(cc as u32);
                let format = asheet.format_id(row0 as usize, col0 as usize).or_else(|| {
                    let cell = CellRef::new(sheet_id?, Coord::new(row0, col0, true, true));
                    derived_formats.get(&cell)
                });
                let class = format.and_then(|id| format_registry.class(id));
                row.push(Self::materialize_temporal_egress(
                    raw,
                    class,
                    self.config.temporal_egress,
                    self.config.date_system,
                ));
            }
            out.push(row);
        }
        out
    }

    /// Unified internal read API for a single cell value (Arrow-truth).
    pub(crate) fn read_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        let asheet = self.sheet_store().sheet(sheet)?;
        let r0 = row.saturating_sub(1) as usize;
        let c0 = col.saturating_sub(1) as usize;
        let v = asheet.get_cell_value(r0, c0);
        if matches!(v, LiteralValue::Empty) {
            None
        } else {
            Some(v)
        }
    }

    /// Unified internal read API for a range of cell values (Arrow-truth).
    pub(crate) fn read_range_values(
        &self,
        sheet: &str,
        sr: u32,
        sc: u32,
        er: u32,
        ec: u32,
    ) -> RangeView<'_> {
        let Some(asheet) = self.sheet_store().sheet(sheet) else {
            return RangeView::from_owned_rows(Vec::new(), self.config.date_system);
        };
        if er < sr || ec < sc {
            return asheet.range_view(1, 1, 0, 0);
        }
        let sr0 = sr.saturating_sub(1) as usize;
        let sc0 = sc.saturating_sub(1) as usize;
        let er0 = er.saturating_sub(1) as usize;
        let ec0 = ec.saturating_sub(1) as usize;
        asheet.range_view(sr0, sc0, er0, ec0)
    }

    /// Get formula AST (if any) and current stored value for a cell
    pub fn get_cell(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Option<(Option<formualizer_parse::ASTNode>, Option<LiteralValue>)> {
        let v = self.get_cell_value(sheet, row, col);
        let sheet_id = self.graph.sheet_id(sheet)?;
        let coord = Coord::from_excel(row, col, true, true);
        let cell = CellRef::new(sheet_id, coord);
        if let Some(vid) = self.graph.get_vertex_for_cell(&cell) {
            let ast = self.graph.get_formula(vid);
            Some((ast, v))
        } else if v.is_some() || self.graph.had_legacy_cell_vertex(&cell) {
            // A referenced or emptied value cell has no vertex (decision
            // 27), but it is a cell the graph knows, as it was when it had
            // one (the interactive formula edit routes on this).
            Some((None, v))
        } else {
            None
        }
    }

    /// Begin batch operations - defer CSR rebuilds for better performance
    pub fn begin_batch(&mut self) {
        self.graph.begin_batch();
    }

    /// End batch operations and trigger CSR rebuild
    pub fn end_batch(&mut self) {
        self.graph.end_batch();
    }

    /// Begin a deferred-dirty scope for a multi-edit batch: while active,
    /// every edit's dirty propagation queues its sources instead of running
    /// a full BFS per edit, and the outermost `end_deferred_dirty` flushes
    /// the union with ONE multi-source propagation (O(component) instead of
    /// O(edits × component)). See `DependencyGraph::begin_deferred_dirty`.
    ///
    /// Callers MUST run `end_deferred_dirty` on every exit path, including
    /// error returns; evaluation entry points `debug_assert` no scope leaked.
    pub fn begin_deferred_dirty(&mut self) {
        self.graph.begin_deferred_dirty();
    }

    /// End a deferred-dirty scope, flushing the queued propagation when the
    /// outermost scope closes. See `Engine::begin_deferred_dirty`.
    pub fn end_deferred_dirty(&mut self) {
        let _ = self.graph.end_deferred_dirty();
    }

    /// Total vertices processed by dirty-propagation BFS loops since graph
    /// creation. Perf-shape observability only (cross-crate tests assert
    /// batched edits propagate O(component), not O(edits × component)).
    pub fn dirty_propagation_visits(&self) -> u64 {
        self.graph.dirty_propagation_visits()
    }

    /// Evaluate a single vertex.
    /// This is the core of the sequential evaluation logic for Milestone 3.1.
    #[inline]
    fn record_cell_if_changed(
        delta: &mut DeltaCollector,
        cell: &CellRef,
        old: &LiteralValue,
        new: &LiteralValue,
    ) {
        if old != new {
            delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
        }
    }

    pub fn evaluate_vertex(&mut self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::Vertex, |engine| {
            engine.observe_function_semantic_epoch()?;
            // A direct request selects exactly one vertex, regardless of its formula kind.
            engine.resource_checkpoint(1)?;
            if !engine.graph.vertex_exists(vertex_id) {
                return engine.evaluate_vertex_impl(vertex_id, None);
            }
            let is_formula = matches!(
                engine.graph.get_vertex_kind(vertex_id),
                VertexKind::FormulaScalar | VertexKind::FormulaArray
            );
            if is_formula {
                engine.begin_evaluation_request();
                #[cfg(any(test, feature = "legacy_oracle"))]
                engine.graph.flush_pending_edge_deltas();
                let roots = [crate::engine::target_preparation::TargetProducer::Legacy(
                    vertex_id,
                )];
                engine.evaluate_legacy_target_roots(&roots, None)?;
            }
            engine.evaluate_vertex_impl(vertex_id, None)
        })
    }

    /// Same rejection publication for owned arrays and pre-admitted range views.
    fn publish_oversized_spill(
        &mut self,
        vertex_id: VertexId,
        error: ExcelError,
        mut delta: Option<&mut DeltaCollector>,
    ) -> LiteralValue {
        self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
        let anchor = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let spill_val = LiteralValue::Error(error);
        if let Some(d) = delta {
            let old = self
                .read_cell_value(
                    self.graph.sheet_name(anchor.sheet_id),
                    anchor.coord.row() + 1,
                    anchor.coord.col() + 1,
                )
                .unwrap_or(LiteralValue::Empty);
            if old != spill_val {
                d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
            }
        }
        self.graph.update_vertex_value_ref(vertex_id, &spill_val);
        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
            self.mirror_value_to_computed_overlay(
                &sheet_name,
                anchor.coord.row() + 1,
                anchor.coord.col() + 1,
                &spill_val,
            );
        }
        spill_val
    }

    fn evaluate_vertex_impl(
        &mut self,
        vertex_id: VertexId,
        delta: Option<&mut DeltaCollector>,
    ) -> Result<LiteralValue, ExcelError> {
        // Preserve the direct evaluator's compatibility behavior for invalid IDs, literal cells,
        // names, and other non-formula vertices. Only formula publication needs the C1a final
        // deadline checkpoint and effects pipeline.
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }
        if self.active_resource_ledger.is_some()
            && matches!(
                self.graph.get_vertex_kind(vertex_id),
                VertexKind::FormulaScalar | VertexKind::FormulaArray
            )
        {
            let value = self
                .evaluate_vertex_immutable(vertex_id)
                .unwrap_or_else(LiteralValue::Error);
            let effects = self.plan_vertex_effects(vertex_id, value.clone(), None)?;
            // Do not publish the selected result until the outer request's deadline succeeds.
            self.resource_checkpoint(0)?;
            let mut delta = delta;
            for effect in &effects {
                self.apply_effect_with_computed_writes(effect, delta.as_deref_mut(), None, None)?;
            }
            return Ok(value);
        }

        let mut delta = delta;

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let view = match kind {
            VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                if let Some(view) = self.graph.formula_view(vertex_id) {
                    view
                } else {
                    return Ok(LiteralValue::Number(0.0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                if let Some(cell_ref) = self.graph.get_cell_ref(vertex_id) {
                    let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                    let row = cell_ref.coord.row() + 1;
                    let col = cell_ref.coord.col() + 1;
                    if let Some(v) = self.read_cell_value(sheet_name, row, col) {
                        return Ok(v);
                    }
                }
                return Ok(LiteralValue::Number(0.0));
            }
            VertexKind::NamedScalar => {
                let value = self.evaluate_named_scalar(vertex_id, sheet_id)?;
                return Ok(value);
            }
            VertexKind::NamedArray => {
                let value = self.evaluate_named_array(vertex_id, sheet_id)?;
                return Ok(value);
            }
            VertexKind::InfiniteRange
            | VertexKind::Range
            | VertexKind::External
            | VertexKind::Table => {
                // Not directly evaluatable here.
                return Ok(LiteralValue::Number(0.0));
            }
        };

        // The interpreter uses a reference to the engine as the context.
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);

        let result = interpreter.evaluate_formula_view(
            view,
            self.graph.data_store(),
            self.graph.sheet_reg(),
        );

        // If array result, perform spill from the anchor cell
        match result {
            Ok(cv) => {
                let derived_format = cv.format_id();
                self.record_derived_format(vertex_id, derived_format);
                let oversized_range = crate::engine::result_finalization::range_spill_error(
                    &cv,
                    self.config.spill.max_spill_cells,
                );
                let is_oversized_range = oversized_range.is_some();
                let result_literal = if let Some(error) = oversized_range {
                    drop(cv);
                    LiteralValue::Error(error)
                } else {
                    crate::engine::result_finalization::finalize_formula_result(cv.into_literal())
                };
                let output_sheet_name = sheet_name.to_string();
                self.write_computed_overlay_format_0based(
                    &output_sheet_name,
                    cell_ref.coord.row(),
                    cell_ref.coord.col(),
                    derived_format,
                );
                if is_oversized_range {
                    let LiteralValue::Error(error) = result_literal else {
                        unreachable!()
                    };
                    self.graph.set_kind(vertex_id, VertexKind::FormulaArray);
                    return Ok(self.publish_oversized_spill(
                        vertex_id,
                        error,
                        delta.as_deref_mut(),
                    ));
                }
                match result_literal {
                    LiteralValue::Array(rows) => {
                        // Update kind to FormulaArray for tracking
                        self.graph
                            .set_kind(vertex_id, crate::engine::vertex::VertexKind::FormulaArray);
                        // Build target cells rectangle starting from anchor
                        let anchor = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .expect("cell ref for vertex");
                        let sheet_id = anchor.sheet_id;
                        let h = rows.len() as u32;
                        let w = rows.first().map(|r| r.len()).unwrap_or(0) as u32;

                        // Hard cap to avoid vertex explosion from huge dynamic arrays.
                        let spill_cells = (h as u64).saturating_mul(w as u64);
                        if spill_cells > self.config.spill.max_spill_cells as u64 {
                            let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                                .with_message("SpillTooLarge")
                                .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                    expected_rows: h,
                                    expected_cols: w,
                                });
                            return Ok(self.publish_oversized_spill(
                                vertex_id,
                                spill_err,
                                delta.as_deref_mut(),
                            ));
                        }
                        // Bounds check to avoid out-of-range writes (align to AbsCoord capacity)
                        const PACKED_MAX_ROW: u32 = 1_048_575; // 20-bit max
                        const PACKED_MAX_COL: u32 = 16_383; // 14-bit max
                        let end_row = anchor.coord.row().saturating_add(h).saturating_sub(1);
                        let end_col = anchor.coord.col().saturating_add(w).saturating_sub(1);
                        if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
                            self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
                            let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                                .with_message("Spill exceeds sheet bounds")
                                .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                    expected_rows: h,
                                    expected_cols: w,
                                });
                            let spill_val = LiteralValue::Error(spill_err.clone());
                            if let Some(d) = delta.as_deref_mut() {
                                let old = self
                                    .read_cell_value(
                                        self.graph.sheet_name(anchor.sheet_id),
                                        anchor.coord.row() + 1,
                                        anchor.coord.col() + 1,
                                    )
                                    .unwrap_or(LiteralValue::Empty);
                                if old != spill_val {
                                    d.record_cell(
                                        anchor.sheet_id,
                                        anchor.coord.row(),
                                        anchor.coord.col(),
                                    );
                                }
                            }
                            self.graph.update_vertex_value_ref(vertex_id, &spill_val);
                            if self.config.arrow_storage_enabled
                                && self.config.delta_overlay_enabled
                                && self.config.write_formula_overlay_enabled
                            {
                                let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                                self.mirror_value_to_computed_overlay(
                                    &sheet_name,
                                    anchor.coord.row() + 1,
                                    anchor.coord.col() + 1,
                                    &spill_val,
                                );
                            }
                            return Ok(spill_val);
                        }
                        let mut targets = Vec::new();
                        for r in 0..h {
                            for c in 0..w {
                                targets.push(self.graph.make_cell_ref_internal(
                                    sheet_id,
                                    anchor.coord.row() + r,
                                    anchor.coord.col() + c,
                                ));
                            }
                        }

                        // Plan spill via spill manager shim
                        match self.spill_mgr.reserve(
                            vertex_id,
                            anchor,
                            SpillShape { rows: h, cols: w },
                            SpillMeta {
                                epoch: self.recalc_epoch,
                                config: self.config.spill,
                            },
                        ) {
                            Ok(()) => {
                                // Commit: write values to grid
                                // Default conflict policy is Error + FirstWins; reserve() enforces in-flight locks
                                // and plan_spill_region enforces overlap with committed formulas/spills/values.
                                if let Err(e) = self.commit_spill_and_mirror(
                                    vertex_id,
                                    &targets,
                                    rows.clone(),
                                    delta.as_deref_mut(),
                                    None,
                                ) {
                                    if e.kind != ExcelErrorKind::Spill {
                                        return Err(e);
                                    }
                                    // If commit fails, mark as error
                                    self.clear_spill_projection_and_mirror(
                                        vertex_id,
                                        delta.as_deref_mut(),
                                    );
                                    if let Some(d) = delta.as_deref_mut() {
                                        let old = self
                                            .read_cell_value(
                                                self.graph.sheet_name(anchor.sheet_id),
                                                anchor.coord.row() + 1,
                                                anchor.coord.col() + 1,
                                            )
                                            .unwrap_or(LiteralValue::Empty);
                                        let new = LiteralValue::Error(e.clone());
                                        if old != new {
                                            d.record_cell(
                                                anchor.sheet_id,
                                                anchor.coord.row(),
                                                anchor.coord.col(),
                                            );
                                        }
                                    }
                                    let err_val = LiteralValue::Error(e.clone());
                                    self.graph.update_vertex_value_ref(vertex_id, &err_val);
                                    if self.config.arrow_storage_enabled
                                        && self.config.delta_overlay_enabled
                                        && self.config.write_formula_overlay_enabled
                                    {
                                        let sheet_name =
                                            self.graph.sheet_name(anchor.sheet_id).to_string();
                                        self.mirror_value_to_computed_overlay(
                                            &sheet_name,
                                            anchor.coord.row() + 1,
                                            anchor.coord.col() + 1,
                                            &err_val,
                                        );
                                    }
                                    return Ok(err_val);
                                }
                                // Anchor shows the top-left value, like Excel
                                let top_left = rows
                                    .first()
                                    .and_then(|r| r.first())
                                    .cloned()
                                    .unwrap_or(LiteralValue::Empty);
                                self.graph.update_vertex_value_ref(vertex_id, &top_left);
                                Ok(top_left)
                            }
                            Err(e) => {
                                self.clear_spill_projection_and_mirror(
                                    vertex_id,
                                    delta.as_deref_mut(),
                                );
                                let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                                    .with_message(
                                        e.message.unwrap_or_else(|| "Spill blocked".to_string()),
                                    )
                                    .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                        expected_rows: h,
                                        expected_cols: w,
                                    });
                                let spill_val = LiteralValue::Error(spill_err.clone());
                                if let Some(d) = delta.as_deref_mut() {
                                    let old = self
                                        .read_cell_value(
                                            self.graph.sheet_name(anchor.sheet_id),
                                            anchor.coord.row() + 1,
                                            anchor.coord.col() + 1,
                                        )
                                        .unwrap_or(LiteralValue::Empty);
                                    if old != spill_val {
                                        d.record_cell(
                                            anchor.sheet_id,
                                            anchor.coord.row(),
                                            anchor.coord.col(),
                                        );
                                    }
                                }
                                self.graph.update_vertex_value_ref(vertex_id, &spill_val);
                                if self.config.arrow_storage_enabled
                                    && self.config.delta_overlay_enabled
                                    && self.config.write_formula_overlay_enabled
                                {
                                    let sheet_name =
                                        self.graph.sheet_name(anchor.sheet_id).to_string();
                                    self.mirror_value_to_computed_overlay(
                                        &sheet_name,
                                        anchor.coord.row() + 1,
                                        anchor.coord.col() + 1,
                                        &spill_val,
                                    );
                                }
                                Ok(spill_val)
                            }
                        }
                    }
                    other => {
                        // Scalar result: store value and ensure any previous spill is cleared
                        let spill_cells = self
                            .graph
                            .spill_cells_for_anchor(vertex_id)
                            .map(|cells| cells.to_vec())
                            .unwrap_or_default();
                        if let Some(d) = delta.as_deref_mut()
                            && let Some(anchor) = self.graph.get_cell_ref_for_vertex(vertex_id)
                        {
                            if spill_cells.is_empty() {
                                let old = self
                                    .read_cell_value(
                                        self.graph.sheet_name(anchor.sheet_id),
                                        anchor.coord.row() + 1,
                                        anchor.coord.col() + 1,
                                    )
                                    .unwrap_or(LiteralValue::Empty);
                                if old != other {
                                    d.record_cell(
                                        anchor.sheet_id,
                                        anchor.coord.row(),
                                        anchor.coord.col(),
                                    );
                                }
                            } else {
                                for cell in spill_cells.iter() {
                                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                                    let old = self
                                        .get_cell_value(
                                            sheet_name,
                                            cell.coord.row() + 1,
                                            cell.coord.col() + 1,
                                        )
                                        .unwrap_or(LiteralValue::Empty);
                                    let new = if cell.sheet_id == anchor.sheet_id
                                        && cell.coord.row() == anchor.coord.row()
                                        && cell.coord.col() == anchor.coord.col()
                                    {
                                        other.clone()
                                    } else {
                                        LiteralValue::Empty
                                    };
                                    Self::record_cell_if_changed(d, cell, &old, &new);
                                }
                            }
                        }
                        self.graph.clear_spill_region(vertex_id);
                        if let Some(scope) = Self::structural_scope_from_cells(&spill_cells) {
                            self.record_structural_change(scope);
                        }
                        if self.config.arrow_storage_enabled
                            && self.config.delta_overlay_enabled
                            && self.config.write_formula_overlay_enabled
                        {
                            let empty = LiteralValue::Empty;
                            for cell in spill_cells.iter() {
                                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                                self.mirror_value_to_computed_overlay(
                                    &sheet_name,
                                    cell.coord.row() + 1,
                                    cell.coord.col() + 1,
                                    &empty,
                                );
                            }
                        }
                        self.graph.update_vertex_value_ref(vertex_id, &other);
                        // Optionally mirror into Arrow overlay for Arrow-backed reads
                        if self.config.arrow_storage_enabled
                            && self.config.delta_overlay_enabled
                            && self.config.write_formula_overlay_enabled
                        {
                            let anchor = self
                                .graph
                                .get_cell_ref(vertex_id)
                                .expect("cell ref for vertex");
                            let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                            self.mirror_value_to_computed_overlay(
                                &sheet_name,
                                anchor.coord.row() + 1,
                                anchor.coord.col() + 1,
                                &other,
                            );
                        }
                        Ok(other)
                    }
                }
            }
            Err(e) => {
                // Runtime Excel error: store as a cell value instead of propagating
                // as an exception so bulk eval paths don't fail the whole pass.
                let spill_cells = self
                    .graph
                    .spill_cells_for_anchor(vertex_id)
                    .map(|cells| cells.to_vec())
                    .unwrap_or_default();
                let err_val = LiteralValue::Error(e.clone());
                if let Some(d) = delta
                    && let Some(anchor) = self.graph.get_cell_ref_for_vertex(vertex_id)
                {
                    if spill_cells.is_empty() {
                        let old = self
                            .read_cell_value(
                                self.graph.sheet_name(anchor.sheet_id),
                                anchor.coord.row() + 1,
                                anchor.coord.col() + 1,
                            )
                            .unwrap_or(LiteralValue::Empty);
                        if old != err_val {
                            d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
                        }
                    } else {
                        for cell in spill_cells.iter() {
                            let sheet_name = self.graph.sheet_name(cell.sheet_id);
                            let old = self
                                .get_cell_value(
                                    sheet_name,
                                    cell.coord.row() + 1,
                                    cell.coord.col() + 1,
                                )
                                .unwrap_or(LiteralValue::Empty);
                            let new = if cell.sheet_id == anchor.sheet_id
                                && cell.coord.row() == anchor.coord.row()
                                && cell.coord.col() == anchor.coord.col()
                            {
                                err_val.clone()
                            } else {
                                LiteralValue::Empty
                            };
                            Self::record_cell_if_changed(d, cell, &old, &new);
                        }
                    }
                }
                self.graph.clear_spill_region(vertex_id);
                if let Some(scope) = Self::structural_scope_from_cells(&spill_cells) {
                    self.record_structural_change(scope);
                }
                if self.config.arrow_storage_enabled
                    && self.config.delta_overlay_enabled
                    && self.config.write_formula_overlay_enabled
                {
                    let empty = LiteralValue::Empty;
                    for cell in spill_cells.iter() {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        self.mirror_value_to_computed_overlay(
                            &sheet_name,
                            cell.coord.row() + 1,
                            cell.coord.col() + 1,
                            &empty,
                        );
                    }
                }
                self.graph.update_vertex_value_ref(vertex_id, &err_val);
                if self.config.arrow_storage_enabled
                    && self.config.delta_overlay_enabled
                    && self.config.write_formula_overlay_enabled
                {
                    let anchor = self
                        .graph
                        .get_cell_ref(vertex_id)
                        .expect("cell ref for vertex");
                    let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                    self.mirror_value_to_computed_overlay(
                        &sheet_name,
                        anchor.coord.row() + 1,
                        anchor.coord.col() + 1,
                        &err_val,
                    );
                }
                Ok(err_val)
            }
        }
    }

    fn evaluate_named_scalar(
        &mut self,
        vertex_id: VertexId,
        sheet_id: SheetId,
    ) -> Result<LiteralValue, ExcelError> {
        let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name)
                .with_message("Named range metadata missing".to_string())
        })?;

        match &named_range.definition {
            NamedDefinition::Cell(cell_ref) => {
                let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                let row = cell_ref.coord.row() + 1;
                let col = cell_ref.coord.col() + 1;

                if let Some(dep_vertex) = self.graph.get_vertex_for_cell(cell_ref)
                    && matches!(
                        self.graph.get_vertex_kind(dep_vertex),
                        VertexKind::FormulaScalar | VertexKind::FormulaArray
                    )
                {
                    // Graph does not cache cell/formula values; ensure the precedent is evaluated.
                    let value = self.evaluate_vertex(dep_vertex)?;
                    self.graph.update_vertex_value_ref(vertex_id, &value);
                    Ok(value)
                } else {
                    let value = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    self.graph.update_vertex_value_ref(vertex_id, &value);
                    Ok(value)
                }
            }
            NamedDefinition::Literal(v) => {
                let out = v.clone();
                self.graph.update_vertex_value_ref(vertex_id, &out);
                Ok(out)
            }
            NamedDefinition::Formula { ast, .. } => {
                let context_sheet = match named_range.scope {
                    NameScope::Sheet(id) => id,
                    NameScope::Workbook => sheet_id,
                };
                let sheet_name = self.graph.sheet_name(context_sheet);
                let cell_ref = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                match interpreter.evaluate_ast(ast) {
                    Ok(cv) => {
                        let value = cv.into_literal();
                        match value {
                            LiteralValue::Array(_) => {
                                let err = ExcelError::new(ExcelErrorKind::NImpl)
                                    .with_message("Array result in scalar named range".to_string());
                                let err_val = LiteralValue::Error(err.clone());
                                self.graph.update_vertex_value_ref(vertex_id, &err_val);
                                Ok(err_val)
                            }
                            other => {
                                self.graph.update_vertex_value_ref(vertex_id, &other);
                                Ok(other)
                            }
                        }
                    }
                    Err(err) => {
                        let err_val = LiteralValue::Error(err.clone());
                        self.graph.update_vertex_value_ref(vertex_id, &err_val);
                        Ok(err_val)
                    }
                }
            }
            NamedDefinition::Range(_) => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Range-valued name evaluated as scalar".to_string())),
        }
    }

    fn evaluate_named_array(
        &mut self,
        vertex_id: VertexId,
        sheet_id: SheetId,
    ) -> Result<LiteralValue, ExcelError> {
        let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name)
                .with_message("Named range metadata missing".to_string())
        })?;

        let out = match &named_range.definition {
            NamedDefinition::Range(range_ref) => {
                if range_ref.start.sheet_id != range_ref.end.sheet_id {
                    return Err(ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Named range cannot span sheets".to_string()));
                }

                let sheet_name = self.graph.sheet_name(range_ref.start.sheet_id);
                let sr0 = range_ref.start.coord.row();
                let sc0 = range_ref.start.coord.col();
                let er0 = range_ref.end.coord.row();
                let ec0 = range_ref.end.coord.col();
                if sr0 > er0 || sc0 > ec0 {
                    return Err(ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Invalid named range bounds".to_string()));
                }

                let h = (er0 - sr0 + 1) as usize;
                let w = (ec0 - sc0 + 1) as usize;
                let cell_count = (h as u64).saturating_mul(w as u64);
                if cell_count > self.config.spill.max_spill_cells as u64 {
                    return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "Named range too large to materialize as an array".to_string(),
                    ));
                }

                let mut rows = Vec::with_capacity(h);
                for r0 in sr0..=er0 {
                    let mut row = Vec::with_capacity(w);
                    for c0 in sc0..=ec0 {
                        let v = self
                            .get_cell_value(sheet_name, r0 + 1, c0 + 1)
                            .unwrap_or(LiteralValue::Empty);
                        row.push(v);
                    }
                    rows.push(row);
                }
                LiteralValue::Array(rows)
            }
            NamedDefinition::Cell(cell_ref) => {
                let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                let row = cell_ref.coord.row() + 1;
                let col = cell_ref.coord.col() + 1;
                let v = self
                    .get_cell_value(sheet_name, row, col)
                    .unwrap_or(LiteralValue::Empty);
                LiteralValue::Array(vec![vec![v]])
            }
            NamedDefinition::Literal(v) => LiteralValue::Array(vec![vec![v.clone()]]),
            NamedDefinition::Formula { ast, .. } => {
                let context_sheet = match named_range.scope {
                    NameScope::Sheet(id) => id,
                    NameScope::Workbook => sheet_id,
                };
                let sheet_name = self.graph.sheet_name(context_sheet);
                let cell_ref = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                match interpreter.evaluate_ast(ast) {
                    Ok(cv) => {
                        let v = cv.into_literal();
                        match v {
                            LiteralValue::Array(_) => v,
                            other => LiteralValue::Array(vec![vec![other]]),
                        }
                    }
                    Err(err) => LiteralValue::Error(err),
                }
            }
        };

        self.graph.update_vertex_value_ref(vertex_id, &out);
        Ok(out)
    }

    fn replan_exhausted_error(&self, limit: usize, context: &str) -> ExcelError {
        crate::engine::ResourceLedgerError::Exhausted(
            formualizer_common::ResourceExhaustionDetail {
                reason: formualizer_common::ResourceExhaustionReason::WorkUnits,
                limit: limit as u64,
                observed: limit.saturating_add(1) as u64,
                request_id: self
                    .active_evaluation_resource_request
                    .as_ref()
                    .map(|stats| stats.request_id),
            },
        )
        .into_excel_error()
        .with_message(format!("{context} did not converge after {limit} replans"))
    }

    fn transient_target_preparation_stale(error: &ExcelError) -> bool {
        matches!(
            &error.extra,
            formualizer_common::ExcelErrorExtra::PreparationStale {
                reason: formualizer_common::PreparationStaleReason::Semantic
                    | formualizer_common::PreparationStaleReason::Provider
            }
        )
    }

    fn prepare_graph_for_routed_evaluation(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        options: &crate::engine::TargetEvalOptions<'_>,
    ) -> Result<crate::engine::PreparedTargetGraphReport, ExcelError> {
        const MAX_TRANSIENT_PREPARATION_RETRIES: usize = 2;
        let mut retries = 0usize;
        loop {
            match self.prepare_graph_for_targets_unobserved(targets, options) {
                Err(error)
                    if Self::transient_target_preparation_stale(&error)
                        && retries < MAX_TRANSIENT_PREPARATION_RETRIES =>
                {
                    retries = retries.saturating_add(1);
                }
                result => return result,
            }
        }
    }

    fn evaluate_mixed_targets(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        delta: Option<&mut DeltaCollector>,
    ) -> Result<EvalResult, ExcelError> {
        let _source_cache = self.source_cache_session();
        let cancel = self.active_cancel_flag.clone();
        let options = crate::engine::TargetEvalOptions {
            request_id: self
                .active_evaluation_resource_request
                .as_ref()
                .map(|stats| stats.request_id),
            cancel,
            deadline: None,
            budgets: None,
            opaque_policy: crate::engine::OpaquePreparePolicy::Widen,
        };
        self.prepare_and_execute_target_recipe(targets, &options, delta)
    }

    fn prepare_and_execute_target_recipe(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        options: &crate::engine::TargetEvalOptions<'_>,
        delta: Option<&mut DeltaCollector>,
    ) -> Result<EvalResult, ExcelError> {
        let preparation = self.prepare_graph_for_routed_evaluation(targets, options)?;
        self.execute_prepared_target_recipe(targets, &preparation.widened_scope, delta)
    }

    fn execute_prepared_target_recipe(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        scope: &crate::engine::PrepareScope,
        delta: Option<&mut DeltaCollector>,
    ) -> Result<EvalResult, ExcelError> {
        self.require_unified_authority()?;
        if matches!(scope, crate::engine::PrepareScope::Workbook)
            && let Some(stats) = self.active_evaluation_resource_request.as_mut()
        {
            stats.workbook_exact_attempts = stats.workbook_exact_attempts.max(1);
        }
        let mut roots = self.resolve_target_producers(targets)?;
        if let crate::engine::PrepareScope::Sheets(sheets) = scope {
            let request_id = self
                .active_evaluation_resource_request
                .as_ref()
                .map(|request| request.request_id);
            let root_count = roots.len();
            let mut widened_roots =
                OrderedTargetProducers::from_ordered(std::mem::take(&mut roots))
                    .map_err(|_| target_root_allocation_error(root_count, request_id))?;
            let sheet_ids = sheets
                .iter()
                .filter_map(|sheet| self.graph.sheet_id(sheet))
                .collect::<FxHashSet<_>>();
            for vertex in self.graph.formula_vertices() {
                if sheet_ids.contains(&self.graph.get_vertex_sheet_id(vertex)) {
                    widened_roots
                        .push(crate::engine::target_preparation::TargetProducer::Legacy(
                            vertex,
                        ))
                        .map_err(|_| {
                            target_root_allocation_error(widened_roots.len() + 1, request_id)
                        })?;
                }
            }
            roots = widened_roots.into_vec();
        }
        self.begin_evaluation_request();
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.graph.flush_pending_edge_deltas();
        let workbook_scope = matches!(scope, crate::engine::PrepareScope::Workbook);
        if workbook_scope {
            if let Some(delta) = delta {
                self.evaluate_all_with_delta_collector(delta)
            } else {
                self.evaluate_all_legacy_impl()
            }
        } else {
            self.evaluate_legacy_target_roots(&roots, delta)
        }
    }

    fn legacy_coordinate_targets(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Vec<crate::engine::EvaluationTarget> {
        targets
            .iter()
            .map(|(sheet, row, col)| {
                // Compatibility APIs historically interned an unknown target sheet
                // and returned an empty value rather than rejecting the target.
                self.graph.sheet_id_mut(sheet);
                crate::engine::EvaluationTarget::Cell {
                    sheet: (*sheet).to_string(),
                    row: *row,
                    col: *col,
                }
            })
            .collect()
    }

    /// Evaluate the necessary mixed producer closure for typed cell, range, name, and table targets.
    pub fn evaluate_targets(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
    ) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::Targeted, |engine| {
            engine.observe_function_semantic_epoch()?;
            engine.validate_deterministic_mode()?;
            engine.evaluate_mixed_targets(targets, None)
        })
    }

    /// Evaluate typed targets with explicit preparation policy and request controls.
    pub fn evaluate_targets_with_options(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        options: crate::engine::TargetEvalOptions<'_>,
    ) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::Targeted, |engine| {
            engine.active_cancel_flag = options.cancel.clone();
            engine.active_evaluation_deadline = options.deadline;
            let result = (|| {
                engine.cancellation_checkpoint("Evaluation cancelled before target preparation")?;
                engine.observe_function_semantic_epoch()?;
                engine.validate_deterministic_mode()?;
                let _source_cache = engine.source_cache_session();
                engine.prepare_and_execute_target_recipe(targets, &options, None)
            })();
            engine.active_cancel_flag = None;
            engine.active_evaluation_deadline = None;
            result
        })
    }

    /// Evaluate typed targets and return the versioned run/region delta for the request.
    pub fn evaluate_targets_with_delta(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
    ) -> Result<(EvalResult, crate::engine::TargetEvalDelta), ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::CellsWithDelta, |engine| {
            engine.observe_function_semantic_epoch()?;
            engine.validate_deterministic_mode()?;
            let mut collector = DeltaCollector::new(DeltaMode::Cells);
            let result = engine.evaluate_mixed_targets(targets, Some(&mut collector))?;
            Ok((result, collector.finish_target()))
        })
    }

    /// Evaluate only the necessary precedents for specific target cells (demand-driven)
    pub fn evaluate_until(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::Targeted, |engine| {
            engine.evaluate_until_unobserved(targets)
        })
    }

    fn evaluate_until_unobserved(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<EvalResult, ExcelError> {
        self.observe_function_semantic_epoch()?;
        let targets = self.legacy_coordinate_targets(targets);
        self.evaluate_mixed_targets(&targets, None)
    }

    fn evaluate_until_with_delta_collector(
        &mut self,
        targets: &[(&str, u32, u32)],
        delta: &mut DeltaCollector,
    ) -> Result<EvalResult, ExcelError> {
        let targets = self.legacy_coordinate_targets(targets);
        self.evaluate_mixed_targets(&targets, Some(delta))
    }

    fn evaluate_legacy_target_roots(
        &mut self,
        roots: &[crate::engine::target_preparation::TargetProducer],
        mut delta: Option<&mut DeltaCollector>,
    ) -> Result<EvalResult, ExcelError> {
        use crate::engine::target_preparation::TargetProducer;
        #[cfg(any(test, feature = "benchmark_internal"))]
        {
            self.recalc_reuse_probe
                .get_mut()
                .unwrap()
                .legacy_target_requests += 1;
        }
        let start = crate::instant::FzInstant::now();
        let root_vertices = roots
            .iter()
            .filter_map(|root| match root {
                TargetProducer::Legacy(vertex) | TargetProducer::Symbol(vertex) => Some(*vertex),
                TargetProducer::ValueOnly(_) => None,
            })
            .collect::<Vec<_>>();
        let mut computed_vertices = 0usize;
        let mut cycle_errors = 0usize;
        let mut replans = 0usize;
        const MAX_REPLAN: usize = 5;
        self.graph.authority_sync();
        loop {
            let (precedents_to_eval, old_vdeps) = self.demand_subgraph(&root_vertices)?;
            if precedents_to_eval.is_empty() {
                break;
            }
            #[cfg(any(test, feature = "benchmark_internal"))]
            {
                self.recalc_reuse_probe
                    .get_mut()
                    .unwrap()
                    .target_schedule_builds += 1;
            }
            let schedule = {
                self.graph.authority_sync();
                let mut ledger = self.active_resource_ledger.take();
                let result = self.create_authority_schedule(
                    &precedents_to_eval,
                    &old_vdeps,
                    ledger.as_mut(),
                );
                self.active_resource_ledger = ledger;
                result?
            };
            self.begin_pass(&schedule);
            for (unit_index, &unit) in schedule.units.iter().enumerate() {
                self.cancellation_checkpoint("Evaluation cancelled before target schedule unit")?;
                match unit {
                    ScheduleUnit::Cycle(index) => {
                        if self.handle_cycle_unit(
                            schedule.unit_cycle(index),
                            delta.as_deref_mut(),
                            None,
                            None,
                        )? > 0
                        {
                            cycle_errors = cycle_errors.saturating_add(1);
                        }
                    }
                    ScheduleUnit::Layer(index) => {
                        let layer = schedule.unit_layer(index);
                        let evaluated = if let Some(delta) = delta.as_deref_mut() {
                            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                                self.evaluate_layer_parallel_with_delta(layer, delta)?
                            } else {
                                self.evaluate_layer_sequential_with_delta(layer, delta)?
                            }
                        } else if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                            self.evaluate_layer_parallel(layer)?
                        } else {
                            self.evaluate_layer_sequential(layer)?
                        };
                        computed_vertices = computed_vertices.saturating_add(evaluated);
                    }
                }
                if self.stop_after_unit(&schedule, unit_index) {
                    break;
                }
            }
            let changed = self.changed_virtual_dep_vertices(&precedents_to_eval, &old_vdeps);
            self.resource_checkpoint(0)?;
            if !self.finish_target_pass_dirty(&precedents_to_eval, &changed) {
                break;
            }
            if replans >= MAX_REPLAN {
                return Err(self.replan_exhausted_error(
                    MAX_REPLAN,
                    "targeted legacy dynamic dependency evaluation",
                ));
            }
            replans = replans.saturating_add(1);
        }
        self.redirty_for_next_recalc();
        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Build a revision-bound compatibility plan covering every prepared formula vertex.
    pub fn build_recalc_plan(&self) -> Result<RecalcPlan, ExcelError> {
        if self.has_staged_formulas() || self.staged_formula_index.has_packages() {
            return Err(
                Self::plan_stale(formualizer_common::PlanStaleReason::Staged).with_message(
                    "compatibility recalculation plans require all staged formulas to be prepared",
                ),
            );
        }
        let key = self.recalc_plan_key();
        let mut vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();
        vertices.sort_unstable();
        let has_dynamic_refs = vertices.iter().copied().any(|v| self.graph.is_dynamic(v));
        let schedule = if vertices.is_empty() {
            crate::engine::Schedule {
                units: Vec::new(),
                layers: Vec::new(),
                cycles: Vec::new(),
            }
        } else {
            self.create_evaluation_schedule_uncached(&vertices, None)?.0
        };
        self.validate_recalc_plan_key(&key)?;
        Ok(RecalcPlan {
            key,
            kind: RecalcPlanKind::CompatibilityFull {
                schedule,
                has_dynamic_refs,
            },
        })
    }

    /// Prepare stable typed targets and retain a revision-bound run-local recipe.
    pub fn build_recalc_plan_for_targets(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
    ) -> Result<RecalcPlan, ExcelError> {
        self.build_recalc_plan_for_targets_with_options(
            targets,
            crate::engine::TargetEvalOptions::default(),
        )
    }

    pub fn build_recalc_plan_for_targets_with_options(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
        options: crate::engine::TargetEvalOptions<'_>,
    ) -> Result<RecalcPlan, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::RecalcPlan, |engine| {
            engine.observe_function_semantic_epoch()?;
            engine.validate_deterministic_mode()?;
            let _source_cache = engine.source_cache_session();
            let preparation = engine.prepare_graph_for_routed_evaluation(targets, &options)?;
            #[cfg(any(test, feature = "legacy_oracle"))]
            engine.graph.flush_pending_edge_deltas();
            let topology = if matches!(
                preparation.widened_scope,
                crate::engine::PrepareScope::Workbook
            ) {
                RecalcTopology::Workbook
            } else {
                RecalcTopology::RunLocalRecipe
            };
            Ok(RecalcPlan {
                key: engine.recalc_plan_key(),
                kind: RecalcPlanKind::Target {
                    targets: targets.to_vec(),
                    scope: preparation.widened_scope,
                    topology,
                    dynamic_policy: DynamicPlanPolicy::BoundedTargetReplan,
                },
            })
        })
    }

    /// Evaluate using a previously constructed compatibility or target plan.
    pub fn evaluate_recalc_plan(&mut self, plan: &RecalcPlan) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::RecalcPlan, |engine| {
            engine.evaluate_recalc_plan_unobserved(plan)
        })
    }

    pub fn evaluate_recalc_plan_with_controls(
        &mut self,
        plan: &RecalcPlan,
        cancel: Option<crate::engine::CancelToken>,
        deadline: Option<Instant>,
    ) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::RecalcPlan, |engine| {
            engine.active_cancel_flag = cancel.clone();
            engine.active_evaluation_deadline = deadline;
            let result = engine.evaluate_recalc_plan_unobserved(plan);
            engine.active_cancel_flag = None;
            engine.active_evaluation_deadline = None;
            result
        })
    }

    fn evaluate_recalc_plan_unobserved(
        &mut self,
        plan: &RecalcPlan,
    ) -> Result<EvalResult, ExcelError> {
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.graph.flush_pending_edge_deltas();
        self.validate_recalc_plan_key(&plan.key)?;
        self.cancellation_checkpoint("Evaluation cancelled before recalculation plan execution")?;
        self.validate_deterministic_mode()?;

        match &plan.kind {
            RecalcPlanKind::Target {
                targets,
                scope,
                topology,
                dynamic_policy,
            } => {
                debug_assert_eq!(*dynamic_policy, DynamicPlanPolicy::BoundedTargetReplan);
                debug_assert_eq!(
                    matches!(topology, RecalcTopology::Workbook),
                    matches!(scope, crate::engine::PrepareScope::Workbook)
                );
                let _source_cache = self.source_cache_session();
                self.execute_prepared_target_recipe(targets, scope, None)
            }
            RecalcPlanKind::CompatibilityFull {
                schedule,
                has_dynamic_refs,
            } => {
                let _source_cache = self.source_cache_session();
                self.begin_evaluation_request();
                if *has_dynamic_refs {
                    self.virtual_dep_fallback_activations =
                        self.virtual_dep_fallback_activations.saturating_add(1);
                    return self.evaluate_all_coordinator();
                }

                let start = crate::instant::FzInstant::now();
                let dirty_vertices = self.graph.get_evaluation_vertices();
                if dirty_vertices.is_empty() {
                    return Ok(EvalResult {
                        computed_vertices: 0,
                        cycle_errors: 0,
                        elapsed: start.elapsed(),
                    });
                }

                let dirty_set: FxHashSet<VertexId> = dirty_vertices.iter().copied().collect();
                let mut computed_vertices = 0;
                let mut cycle_errors = 0;
                for &unit in &schedule.units {
                    self.cancellation_checkpoint(
                        "Evaluation cancelled before recalculation plan schedule unit",
                    )?;
                    match unit {
                        ScheduleUnit::Cycle(i) => {
                            let stamped = self.handle_cycle_unit(
                                schedule.unit_cycle(i),
                                None,
                                Some(&dirty_set),
                                None,
                            )?;
                            if stamped > 0 {
                                cycle_errors += 1;
                            }
                        }
                        ScheduleUnit::Layer(i) => {
                            let work: Vec<VertexId> = schedule
                                .unit_layer(i)
                                .vertices
                                .iter()
                                .copied()
                                .filter(|v| dirty_set.contains(v))
                                .collect();
                            if work.is_empty() {
                                continue;
                            }
                            let temp_layer = crate::engine::scheduler::Layer::new(work);
                            if self.thread_pool.is_some() && temp_layer.vertices.len() > 1 {
                                computed_vertices += self.evaluate_layer_parallel(&temp_layer)?;
                            } else {
                                computed_vertices += self.evaluate_layer_sequential(&temp_layer)?;
                            }
                        }
                    }
                }

                self.resource_checkpoint(0)?;
                self.graph.clear_dirty_flags(&dirty_vertices);
                self.redirty_for_next_recalc();
                Ok(EvalResult {
                    computed_vertices,
                    cycle_errors,
                    elapsed: start.elapsed(),
                })
            }
        }
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Refuse out-of-scope authority states before evaluation can demote spans
    /// or execute a legacy schedule. The public error type is unchanged; NImpl
    /// carries the exact internal Unsupported operation for the deferred-scope
    /// gate. Admission and allocation failures are not scope exceptions.
    fn require_unified_authority(&mut self) -> Result<(), ExcelError> {
        self.graph
            .authority()
            .map(|_| ())
            .map_err(Self::authority_excel_error)
    }

    fn authority_excel_error(error: crate::engine::authority::store::AuthorityError) -> ExcelError {
        use crate::engine::authority::store::AuthorityError;
        // Some unchanged behavioral tests assert only `error.kind`, hiding
        // the operation in their panic. The opt-in gate trace proves which
        // typed error was actually returned; it never changes that error.
        #[cfg(test)]
        if std::env::var_os("FZ_AUTHORITY_DEFERRED_TRACE").is_some() {
            eprintln!("M1B_AUTHORITY_ERROR {error:?}");
        }
        let kind = match error {
            AuthorityError::Unsupported { .. } => ExcelErrorKind::NImpl,
            _ => ExcelErrorKind::Error,
        };
        ExcelError::new(kind).with_message(format!("unified_authority: {error:?}"))
    }

    /// Evaluate all dirty/volatile vertices
    pub fn evaluate_all(&mut self) -> Result<EvalResult, ExcelError> {
        // `evaluate_all_unobserved` owns the `observe_function_semantic_epoch` guard.
        self.observe_evaluation_resource_request(EvaluationRequestKind::Full, |engine| {
            engine.evaluate_all_unobserved()
        })
    }

    fn evaluate_all_unobserved(&mut self) -> Result<EvalResult, ExcelError> {
        debug_assert!(
            !self.graph.deferred_dirty_active(),
            "deferred-dirty scope leaked into evaluate_all: a begin_deferred_dirty \
             was not balanced by end_deferred_dirty"
        );
        self.observe_function_semantic_epoch()?;
        self.lookup_index_cache.reset_counters();
        let _source_cache = self.source_cache_session();
        self.validate_deterministic_mode()?;
        if self.config.defer_graph_building {
            // Build graph for all staged formulas before evaluating
            self.build_graph_all()?;
        }
        self.evaluate_all_coordinator()
    }

    /// Coordinator for `evaluate_all`: starts the evaluation request and runs
    /// the per-cell pass.
    fn evaluate_all_coordinator(&mut self) -> Result<EvalResult, ExcelError> {
        self.require_unified_authority()?;
        self.begin_evaluation_request();
        self.evaluate_all_legacy_impl()
    }

    /// Walk a schedule's units in condensation order: stamp each cyclic SCC
    /// at its position and evaluate each layer (parallel when enabled).
    ///
    /// Returns `(computed_vertices, cycle_count)` where `cycle_count` is the
    /// number of Cycle units walked (the former `schedule.cycles.len()`).
    fn legacy_pass_run_units(
        &mut self,
        schedule: &crate::engine::scheduler::Schedule,
    ) -> Result<(usize, usize), ExcelError> {
        let mut computed_vertices = 0;
        let mut cycle_count = 0;
        self.begin_pass(schedule);
        for (unit_index, &unit) in schedule.units.iter().enumerate() {
            match unit {
                ScheduleUnit::Cycle(i) => {
                    if self.handle_cycle_unit(schedule.unit_cycle(i), None, None, None)? > 0 {
                        cycle_count += 1;
                    }
                }
                ScheduleUnit::Layer(i) => {
                    let layer = schedule.unit_layer(i);
                    if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                        computed_vertices += self.evaluate_layer_parallel(layer)?;
                    } else {
                        computed_vertices += self.evaluate_layer_sequential(layer)?;
                    }
                }
            }
            if self.stop_after_unit(schedule, unit_index) {
                break;
            }
        }
        Ok((computed_vertices, cycle_count))
    }

    /// Per-cell `evaluate_all` body, reached through the coordinator. This is
    /// an internal primitive; it must not be invoked directly from public APIs.
    ///
    /// Does NOT call `begin_evaluation_request` (cycle-telemetry reset +
    /// per-recalc clock sample): request begin happens at the public entry
    /// points / coordinators, so one request keeps one clock sample.
    fn evaluate_all_legacy_impl(&mut self) -> Result<EvalResult, ExcelError> {
        self.reset_virtual_dep_telemetry_if_disabled();
        let _span_eval =
            crate::engine::trace::fz_span!(tracing::Level::INFO, "evaluate", "evaluate.legacy");
        let start = crate::instant::FzInstant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;
        let mut replan_iterations = 0;
        const MAX_REPLAN: usize = 5;
        let mut telemetry = self
            .config
            .enable_virtual_dep_telemetry
            .then(|| self.start_virtual_dep_telemetry());

        loop {
            let to_evaluate = self.graph.get_evaluation_vertices();
            if to_evaluate.is_empty() {
                if let Some(t) = telemetry.as_mut()
                    && t.bailout_reason.is_none()
                {
                    t.bailout_reason = Some("no_work");
                }
                break;
            }

            let (schedule, old_vdeps, meta) = self.create_evaluation_schedule(&to_evaluate)?;
            if let Some(t) = telemetry.as_mut() {
                Self::accumulate_schedule_meta(t, &meta);
            }

            let (pass_computed, pass_cycles) = self.legacy_pass_run_units(&schedule)?;
            computed_vertices += pass_computed;
            cycle_errors += pass_cycles;

            // Check if dynamic dependencies changed
            let changed_vertices = self.changed_virtual_dep_vertices(&to_evaluate, &old_vdeps);
            if let Some(t) = telemetry.as_mut() {
                t.changed_vdeps_total += changed_vertices.len();
            }

            self.resource_checkpoint(0)?;
            if !self.finish_pass_dirty(&to_evaluate, &changed_vertices) {
                if let Some(t) = telemetry.as_mut() {
                    t.bailout_reason = Some("converged");
                }
                break;
            }
            if replan_iterations >= MAX_REPLAN {
                if let Some(mut t) = telemetry.take() {
                    t.bailout_reason = Some("max_replan");
                    t.replan_iterations = replan_iterations;
                    self.last_virtual_dep_telemetry = t;
                }
                return Err(
                    self.replan_exhausted_error(MAX_REPLAN, "dynamic dependency evaluation")
                );
            }

            replan_iterations += 1;
        }

        if let Some(mut t) = telemetry {
            t.replan_iterations = replan_iterations;
            self.last_virtual_dep_telemetry = t;
        }

        // Re-dirty volatile vertices for the next evaluation cycle
        self.redirty_for_next_recalc();

        // Advance recalc epoch after a full evaluation pass finishes
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    pub fn evaluate_all_with_target_delta(
        &mut self,
    ) -> Result<(EvalResult, crate::engine::TargetEvalDelta), ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::FullWithDelta, |engine| {
            engine.observe_function_semantic_epoch()?;
            let mut collector = DeltaCollector::new(DeltaMode::Cells);
            let result = engine.evaluate_all_with_delta_collector(&mut collector)?;
            Ok((result, collector.finish_target()))
        })
    }

    pub fn evaluate_all_with_delta(&mut self) -> Result<(EvalResult, EvalDelta), ExcelError> {
        self.evaluate_all_with_delta_policy(EvalDeltaCompatibilityPolicy::Unlimited)
    }

    pub fn evaluate_all_with_delta_policy(
        &mut self,
        policy: EvalDeltaCompatibilityPolicy,
    ) -> Result<(EvalResult, EvalDelta), ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::FullWithDelta, |engine| {
            engine.observe_function_semantic_epoch()?;
            let mut collector = DeltaCollector::new(DeltaMode::Cells);
            let result = engine.evaluate_all_with_delta_collector(&mut collector)?;
            Ok((result, collector.finish_with_policy(policy)?))
        })
    }

    fn evaluate_all_with_delta_collector(
        &mut self,
        delta: &mut DeltaCollector,
    ) -> Result<EvalResult, ExcelError> {
        let _source_cache = self.source_cache_session();
        if self.config.defer_graph_building {
            self.build_graph_all()?;
        }
        self.require_unified_authority()?;
        self.begin_evaluation_request();
        self.reset_virtual_dep_telemetry_if_disabled();
        let _span_eval = crate::engine::trace::fz_span!(
            tracing::Level::INFO,
            "evaluate",
            "evaluate.legacy_delta"
        );
        let start = crate::instant::FzInstant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let mut replan_iterations = 0;
        const MAX_REPLAN: usize = 5;
        let mut telemetry = self
            .config
            .enable_virtual_dep_telemetry
            .then(|| self.start_virtual_dep_telemetry());

        loop {
            let to_evaluate = self.graph.get_evaluation_vertices();
            if to_evaluate.is_empty() {
                if let Some(t) = telemetry.as_mut()
                    && t.bailout_reason.is_none()
                {
                    t.bailout_reason = Some("no_work");
                }
                break;
            }

            let (schedule, old_vdeps, meta) = self.create_evaluation_schedule(&to_evaluate)?;
            if let Some(t) = telemetry.as_mut() {
                Self::accumulate_schedule_meta(t, &meta);
            }

            self.begin_pass(&schedule);
            for (unit_index, &unit) in schedule.units.iter().enumerate() {
                match unit {
                    ScheduleUnit::Cycle(i) => {
                        if self.handle_cycle_unit(
                            schedule.unit_cycle(i),
                            Some(delta),
                            None,
                            None,
                        )? > 0
                        {
                            cycle_errors += 1;
                        }
                    }
                    ScheduleUnit::Layer(i) => {
                        let layer = schedule.unit_layer(i);
                        if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                            computed_vertices +=
                                self.evaluate_layer_parallel_with_delta(layer, delta)?;
                        } else {
                            computed_vertices +=
                                self.evaluate_layer_sequential_with_delta(layer, delta)?;
                        }
                    }
                }
                if self.stop_after_unit(&schedule, unit_index) {
                    break;
                }
            }

            let changed_vertices = self.changed_virtual_dep_vertices(&to_evaluate, &old_vdeps);
            if let Some(t) = telemetry.as_mut() {
                t.changed_vdeps_total += changed_vertices.len();
            }
            self.resource_checkpoint(0)?;
            if !self.finish_pass_dirty(&to_evaluate, &changed_vertices) {
                if let Some(t) = telemetry.as_mut() {
                    t.bailout_reason = Some("converged");
                }
                break;
            }
            if replan_iterations >= MAX_REPLAN {
                if let Some(mut t) = telemetry.take() {
                    t.bailout_reason = Some("max_replan");
                    t.replan_iterations = replan_iterations;
                    self.last_virtual_dep_telemetry = t;
                }
                return Err(
                    self.replan_exhausted_error(MAX_REPLAN, "dynamic dependency evaluation")
                );
            }
            replan_iterations += 1;
        }

        if let Some(mut t) = telemetry {
            t.replan_iterations = replan_iterations;
            self.last_virtual_dep_telemetry = t;
        }

        self.redirty_for_next_recalc();
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Convenience: demand-driven evaluation of a single cell by sheet name and row/col.
    ///
    /// This will evaluate only the minimal set of dirty / volatile precedents required
    /// to bring the target cell up-to-date (as if a user asked for that single value),
    /// rather than scheduling a full workbook recalc. If the cell is already clean and
    /// non-volatile, no vertices will be recomputed.
    ///
    /// Returns the (possibly newly computed) value stored for the cell afterwards.
    /// Empty cells return None. Errors are surfaced via the Result type.
    pub fn evaluate_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::Cell, |engine| {
            engine.evaluate_cell_unobserved(sheet, row, col)
        })
    }

    fn evaluate_cell_unobserved(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        if row == 0 || col == 0 {
            return Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Row and column must be >= 1".to_string()));
        }

        let result = self.evaluate_cells(&[(sheet, row, col)])?;

        match result.len() {
            0 => Ok(None),
            1 => {
                let v = result.into_iter().next().unwrap();
                Ok(v)
            }
            _ => unreachable!("evaluate_cells returned unexpected length"),
        }
    }

    /// Convenience: demand-driven evaluation of multiple cells; accepts a slice of
    /// (sheet, row, col) triples. The union of required dirty / volatile precedents
    /// is computed once and evaluated, which is typically faster than calling
    /// `evaluate_cell` repeatedly for a related set of targets.
    ///
    /// Returns the resulting values for each requested target in the same order.
    pub fn evaluate_cells(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::Cells, |engine| {
            engine.evaluate_cells_unobserved(targets)
        })
    }

    fn evaluate_cells_unobserved(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        self.observe_function_semantic_epoch()?;
        debug_assert!(
            !self.graph.deferred_dirty_active(),
            "deferred-dirty scope leaked into evaluate_cells: a begin_deferred_dirty \
             was not balanced by end_deferred_dirty"
        );
        self.validate_deterministic_mode()?;
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let typed_targets = self.legacy_coordinate_targets(targets);
        self.evaluate_mixed_targets(&typed_targets, None)?;
        Ok(targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect())
    }

    pub fn evaluate_cells_cancellable(
        &mut self,
        targets: &[(&str, u32, u32)],
        cancel: crate::engine::CancelToken,
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        self.observe_evaluation_resource_request(
            EvaluationRequestKind::CellsCancellable,
            |engine| {
                engine.observe_function_semantic_epoch()?;
                engine.active_cancel_flag = Some(cancel.clone());
                let res = engine.evaluate_cells_cancellable_impl(targets, cancel.as_flag());
                engine.active_cancel_flag = None;
                res
            },
        )
    }

    fn evaluate_cells_cancellable_impl(
        &mut self,
        targets: &[(&str, u32, u32)],
        cancel_flag: &AtomicBool,
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        self.validate_deterministic_mode()?;
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Evaluation cancelled before target preparation"));
        }
        let typed_targets = self.legacy_coordinate_targets(targets);
        self.evaluate_mixed_targets(&typed_targets, None)?;
        Ok(targets
            .iter()
            .map(|(sheet, row, col)| self.get_cell_value(sheet, *row, *col))
            .collect())
    }

    pub fn evaluate_cells_with_target_delta(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<(Vec<Option<LiteralValue>>, crate::engine::TargetEvalDelta), ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::CellsWithDelta, |engine| {
            engine.observe_function_semantic_epoch()?;
            engine.validate_deterministic_mode()?;
            if targets.is_empty() {
                return Ok((Vec::new(), crate::engine::TargetEvalDelta::default()));
            }
            let mut collector = DeltaCollector::new(DeltaMode::Cells);
            engine.evaluate_until_with_delta_collector(targets, &mut collector)?;
            let values = targets
                .iter()
                .map(|(sheet, row, col)| engine.get_cell_value(sheet, *row, *col))
                .collect();
            Ok((values, collector.finish_target()))
        })
    }

    pub fn evaluate_cells_with_delta(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<(Vec<Option<LiteralValue>>, EvalDelta), ExcelError> {
        self.evaluate_cells_with_delta_policy(targets, EvalDeltaCompatibilityPolicy::Unlimited)
    }

    pub fn evaluate_cells_with_delta_policy(
        &mut self,
        targets: &[(&str, u32, u32)],
        policy: EvalDeltaCompatibilityPolicy,
    ) -> Result<(Vec<Option<LiteralValue>>, EvalDelta), ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::CellsWithDelta, |engine| {
            engine.evaluate_cells_with_delta_unobserved(targets, policy)
        })
    }

    fn evaluate_cells_with_delta_unobserved(
        &mut self,
        targets: &[(&str, u32, u32)],
        policy: EvalDeltaCompatibilityPolicy,
    ) -> Result<(Vec<Option<LiteralValue>>, EvalDelta), ExcelError> {
        self.observe_function_semantic_epoch()?;
        self.validate_deterministic_mode()?;
        if targets.is_empty() {
            return Ok((Vec::new(), EvalDelta::default()));
        }
        let mut collector = DeltaCollector::new(DeltaMode::Cells);
        self.evaluate_until_with_delta_collector(targets, &mut collector)?;
        let values = targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect();
        Ok((values, collector.finish_with_policy(policy)?))
    }

    /// Get the evaluation plan for target cells without actually evaluating them
    pub fn get_eval_plan(&self, targets: &[(&str, u32, u32)]) -> Result<EvalPlan, ExcelError> {
        if targets.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: Vec::new(),
            });
        }
        if self.config.defer_graph_building && self.has_staged_formulas() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                "Evaluation plan requested with deferred graph; build first or call evaluate_*",
            ));
        }

        // Convert targets to A1 notation for consistency
        let addresses: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| format!("{}!{}{}", s, Self::col_to_letters(*c), r))
            .collect();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            if let Some(sheet_id) = self.graph.sheet_id(sheet) {
                let coord = Coord::from_excel(*row, *col, true, true);
                target_addrs.push(CellRef::new(sheet_id, coord));
            }
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Build demand subgraph with virtual edges (same as evaluate_until)
        let (precedents_to_eval, vdeps) = self.demand_subgraph(&target_vertex_ids)?;

        if precedents_to_eval.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Count dirty and volatile vertices
        let mut dirty_count = 0;
        let mut volatile_count = 0;
        for &vertex_id in &precedents_to_eval {
            if self.graph.is_dirty(vertex_id) {
                dirty_count += 1;
            }
            if self.graph.is_volatile(vertex_id) {
                volatile_count += 1;
            }
        }

        // Create schedule for the minimal subgraph honoring virtual edges
        let schedule = self.create_authority_schedule(&precedents_to_eval, &vdeps, None)?;

        // Build layer information
        let mut layers = Vec::new();
        let mut estimated_parallel_layers = 0;
        let parallel_enabled = self.config.enable_parallel && self.thread_pool.is_some();

        for layer in &schedule.layers {
            let parallel_eligible = parallel_enabled && layer.vertices.len() > 1;
            if parallel_eligible {
                estimated_parallel_layers += 1;
            }

            // Get sample cell addresses (up to 5)
            let sample_cells: Vec<String> = layer
                .vertices
                .iter()
                .take(5)
                .filter_map(|&vertex_id| {
                    self.graph
                        .get_cell_ref_for_vertex(vertex_id)
                        .map(|cell_ref| {
                            let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                            format!(
                                "{}!{}{}",
                                sheet_name,
                                Self::col_to_letters(cell_ref.coord.col().saturating_add(1)),
                                cell_ref.coord.row() + 1
                            )
                        })
                })
                .collect();

            layers.push(LayerInfo {
                vertex_count: layer.vertices.len(),
                parallel_eligible,
                sample_cells,
            });
        }

        Ok(EvalPlan {
            total_vertices_to_evaluate: precedents_to_eval.len(),
            layers,
            cycles_detected: schedule.cycles.len(),
            dirty_count,
            volatile_count,
            parallel_enabled,
            estimated_parallel_layers,
            target_cells: addresses,
        })
    }
    /// Helper to create a schedule, integrating virtual dependencies automatically.
    fn create_evaluation_schedule(
        &mut self,
        to_evaluate: &[VertexId],
    ) -> Result<EvaluationScheduleBuildOutput, ExcelError> {
        #[cfg(any(test, feature = "benchmark_internal"))]
        {
            self.recalc_reuse_probe.get_mut().unwrap().schedule_requests += 1;
        }
        // Fold pending edge deltas once per schedule build so traversal uses
        // the zero-allocation CSR slices (#125).
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.graph.flush_pending_edge_deltas();
        // The cache key includes the authority revision: sync first.
        self.graph.authority_sync();
        if self.can_use_static_schedule_cache(to_evaluate) {
            // A recent schedule for the same request becomes the current one.
            let revision = self.schedule_cache_authority_revision();
            let current = |e: &CachedScheduleEntry| {
                e.topology_epoch == self.topology_epoch && e.authority_revision == revision
            };
            if !self
                .cached_static_schedule
                .as_ref()
                .is_some_and(|c| current(c) && c.candidate_vertices.equals(to_evaluate))
                && let Some(i) = self.recent_schedules.iter().position(|e| {
                    current(e)
                        && e.candidate_vertices.len() == to_evaluate.len()
                        && e.candidate_vertices.equals(to_evaluate)
                })
            {
                let hit = self.recent_schedules.remove(i);
                if let Some(previous) = self.cached_static_schedule.replace(hit) {
                    self.retain_recent_schedule(previous);
                }
            }
            if let Some(cached) = self.cached_static_schedule.as_ref()
                && cached.topology_epoch == self.topology_epoch
                && cached.authority_revision == self.schedule_cache_authority_revision()
                && cached.candidate_vertices.equals(to_evaluate)
            {
                let meta = ScheduleBuildMeta {
                    candidate_vertices: to_evaluate.len(),
                    vdeps_vertices: 0,
                    vdeps_edges: 0,
                    builder_elapsed_ms: 0,
                    used_virtual_schedule: false,
                    schedule_cache_hit: true,
                    schedule_cache_eligible: true,
                };
                #[cfg(any(test, feature = "benchmark_internal"))]
                {
                    let mut probe = self.recalc_reuse_probe.lock().unwrap();
                    probe.schedule_cache_hits += 1;
                    probe.schedule_shared_handles += 1;
                }
                return Ok((
                    EvaluationSchedule::Shared(Arc::clone(&cached.schedule)),
                    FxHashMap::default(),
                    meta,
                ));
            }

            let (schedule, vdeps, mut meta) = match self.schedule_from_base(to_evaluate)? {
                Some(schedule) => (
                    schedule,
                    FxHashMap::default(),
                    ScheduleBuildMeta {
                        candidate_vertices: to_evaluate.len(),
                        vdeps_vertices: 0,
                        vdeps_edges: 0,
                        builder_elapsed_ms: 0,
                        used_virtual_schedule: false,
                        schedule_cache_hit: false,
                        schedule_cache_eligible: true,
                    },
                ),
                None => self.create_evaluation_schedule_active(to_evaluate)?,
            };
            meta.schedule_cache_hit = false;
            meta.schedule_cache_eligible = true;
            #[cfg(any(test, feature = "benchmark_internal"))]
            {
                self.recalc_reuse_probe
                    .get_mut()
                    .unwrap()
                    .schedule_cache_misses += 1;
            }
            let schedule = if vdeps.is_empty() {
                // Clone previously discarded builder spare capacity. Keep that compact
                // retained payload while sharing it with the current request.
                let mut schedule = schedule;
                schedule.units.shrink_to_fit();
                for layer in &mut schedule.layers {
                    layer.vertices.shrink_to_fit();
                }
                schedule.layers.shrink_to_fit();
                for cycle in &mut schedule.cycles {
                    cycle.shrink_to_fit();
                }
                schedule.cycles.shrink_to_fit();
                let schedule = Arc::new(schedule);
                #[cfg(any(test, feature = "benchmark_internal"))]
                {
                    self.recalc_reuse_probe
                        .get_mut()
                        .unwrap()
                        .schedule_shared_handles += 1;
                }
                let entry = CachedScheduleEntry {
                    topology_epoch: self.topology_epoch,
                    authority_revision: self.schedule_cache_authority_revision(),
                    candidate_vertices: VertexIdRuns::from_slice(to_evaluate),
                    schedule: Arc::clone(&schedule),
                };
                if let Some(previous) = self.cached_static_schedule.replace(entry) {
                    self.retain_recent_schedule(previous);
                }
                EvaluationSchedule::Shared(schedule)
            } else {
                EvaluationSchedule::Owned(schedule)
            };
            return Ok((schedule, vdeps, meta));
        }

        let (schedule, vdeps, mut meta) = self.create_evaluation_schedule_active(to_evaluate)?;
        meta.schedule_cache_hit = false;
        meta.schedule_cache_eligible = false;
        #[cfg(any(test, feature = "benchmark_internal"))]
        {
            self.recalc_reuse_probe
                .get_mut()
                .unwrap()
                .schedule_cache_ineligible += 1;
        }
        Ok((EvaluationSchedule::Owned(schedule), vdeps, meta))
    }

    /// Plan reuse: the base schedule restricted to `to_evaluate` when it
    /// is current, covers the request, has at least
    /// `BASE_SCHEDULE_MIN_REQUEST` candidates, and is at most
    /// `BASE_SCHEDULE_RATIO` times its size (restricting walks the whole
    /// base; planning costs far more per candidate).
    fn schedule_from_base(
        &mut self,
        to_evaluate: &[VertexId],
    ) -> Result<Option<crate::engine::scheduler::Schedule>, ExcelError> {
        let revision = self.schedule_cache_authority_revision();
        let current = |e: &&CachedScheduleEntry| {
            e.topology_epoch == self.topology_epoch && e.authority_revision == revision
        };
        // The base, or the current schedule when larger (the first
        // evaluation's, before a later request replaces it).
        let Some(base) = [
            self.base_schedule.as_ref(),
            self.cached_static_schedule.as_ref(),
        ]
        .into_iter()
        .flatten()
        .filter(current)
        .max_by_key(|e| e.candidate_vertices.len()) else {
            return Ok(None);
        };
        let base_len = base.candidate_vertices.len();
        if base_len <= BASE_SCHEDULE_MIN_VERTICES
            || to_evaluate.len() < BASE_SCHEDULE_MIN_REQUEST
            || to_evaluate.len() > base_len
            || base_len > to_evaluate.len().saturating_mul(BASE_SCHEDULE_RATIO)
        {
            return Ok(None);
        }
        let mut keep = crate::engine::idset::DenseIdSet::default();
        keep.extend(to_evaluate.iter().copied());
        let Some((schedule, kept)) = base.schedule.restrict(&keep) else {
            return Ok(None);
        };
        // Every requested vertex must be in the base.
        if kept != keep.len() {
            return Ok(None);
        }
        if let Some(ledger) = self.active_resource_ledger.as_mut() {
            let layers: usize = schedule
                .layers
                .iter()
                .map(|l| {
                    l.vertices.capacity() * std::mem::size_of::<VertexId>()
                        + l.runs.capacity()
                            * std::mem::size_of::<crate::engine::scheduler::LayerRun>()
                })
                .sum();
            let bytes = (keep.heap_bytes()
                + layers
                + schedule.layers.capacity()
                    * std::mem::size_of::<crate::engine::scheduler::Layer>()
                + schedule.units.capacity()
                    * std::mem::size_of::<crate::engine::scheduler::ScheduleUnit>()
                + schedule
                    .cycles
                    .iter()
                    .map(|c| c.capacity() * 4)
                    .sum::<usize>()) as u64;
            ledger
                .reserve_schedule_discovery(bytes)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
            ledger
                .release_scratch(bytes)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        #[cfg(debug_assertions)]
        self.debug_check_restricted_schedule(to_evaluate, &schedule);
        #[cfg(any(test, feature = "benchmark_internal"))]
        {
            self.recalc_reuse_probe
                .get_mut()
                .unwrap()
                .schedule_base_restrictions += 1;
        }
        Ok(Some(schedule))
    }

    /// Debug builds: a restricted schedule holds each requested vertex
    /// once and orders the request like a freshly planned schedule (every
    /// dependency among the request in an earlier unit).
    #[cfg(debug_assertions)]
    fn debug_check_restricted_schedule(
        &self,
        to_evaluate: &[VertexId],
        schedule: &crate::engine::scheduler::Schedule,
    ) {
        let mut position: FxHashMap<VertexId, usize> = FxHashMap::default();
        for (u, unit) in schedule.units.iter().enumerate() {
            let vertices: &[VertexId] = match *unit {
                crate::engine::scheduler::ScheduleUnit::Layer(i) => {
                    &schedule.unit_layer(i).vertices
                }
                crate::engine::scheduler::ScheduleUnit::Cycle(i) => schedule.unit_cycle(i),
            };
            for &v in vertices {
                assert!(
                    position.insert(v, u).is_none(),
                    "vertex twice in a restricted schedule"
                );
            }
        }
        assert_eq!(position.len(), to_evaluate.len());
        if to_evaluate.len() > 512 {
            return;
        }
        // Every arc among the request (the store's edge images) goes to an
        // earlier unit, or within one sequential layer (a chain) or cycle.
        let Ok(store) = self.graph.authority_plan_store() else {
            return;
        };
        let cells: Vec<(VertexId, (u16, u32, u32))> = to_evaluate
            .iter()
            .filter_map(|&v| self.graph.authority_cell_of_vertex(v).map(|c| (v, c)))
            .collect();
        for &(v, (sheet, row, col)) in &cells {
            let Some(owner) = store.owner_at((sheet, row, col)) else {
                continue;
            };
            let Ok(refined) = store.refine_owner_column(owner, col, row, row, None) else {
                continue;
            };
            for piece in &refined.pieces {
                for edge in &refined.edges[piece.edge_start..piece.edge_end] {
                    let Some(image) = edge.proj.forward(&piece.domain) else {
                        continue;
                    };
                    for &(d, (s2, r2, c2)) in &cells {
                        if d == v
                            || edge.proj.sheet != s2
                            || !(image.r0 <= r2
                                && r2 <= image.r1
                                && image.c0 <= c2
                                && c2 <= image.c1)
                        {
                            continue;
                        }
                        let (pd, pv) = (position[&d], position[&v]);
                        let same_ok = pd == pv
                            && match schedule.units[pv] {
                                crate::engine::scheduler::ScheduleUnit::Layer(i) => {
                                    schedule.unit_layer(i).sequential
                                }
                                crate::engine::scheduler::ScheduleUnit::Cycle(_) => true,
                            };
                        assert!(
                            pd < pv || same_ok,
                            "restricted schedule orders {v:?} before its precedent {d:?}"
                        );
                    }
                }
            }
        }
    }

    /// Compress family formulas once per authority build (see
    /// `EvalConfig::formula_compression`), when no staged or deferred
    /// formula package can hold arena ids.
    fn maybe_compress_formulas(&mut self) {
        if !self.config.formula_compression {
            return;
        }
        let builds = self.graph.authority_host().builds;
        if self.compressed_at_build == Some(builds) {
            return;
        }
        self.compressed_at_build = Some(builds);
        if self.has_staged_formulas() {
            // Staged packages hold arena ids: no compaction. Members that
            // are already compressed can still leave the per-cell maps.
            self.graph.virtualize_family_members();
            return;
        }
        let pool = self.thread_pool.clone();
        let (_, garbage) = self.graph.compress_family_formulas(pool.as_deref());
        self.graph.virtualize_family_members();
        // Freeing the dropped members' reference texts (one allocation each)
        // is most of compaction; with a pool it happens off the critical
        // path.
        match pool {
            Some(pool) if garbage.len() >= 1024 => pool.spawn(move || drop(garbage)),
            _ => drop(garbage),
        }
    }

    fn create_evaluation_schedule_active(
        &mut self,
        to_evaluate: &[VertexId],
    ) -> Result<ScheduleBuildOutput, ExcelError> {
        self.graph.authority_sync();
        self.maybe_compress_formulas();
        let mut ledger = self.active_resource_ledger.take();
        let result = self.create_evaluation_schedule_uncached(to_evaluate, ledger.as_mut());
        self.active_resource_ledger = ledger;
        result
    }

    fn create_evaluation_schedule_uncached(
        &self,
        to_evaluate: &[VertexId],
        #[allow(unused_variables)] ledger: Option<&mut ResourceLedger>,
    ) -> Result<ScheduleBuildOutput, ExcelError> {
        #[cfg(any(test, feature = "benchmark_internal"))]
        {
            self.recalc_reuse_probe.lock().unwrap().schedule_builds += 1;
        }
        let builder = VirtualDepBuilder::new(self);
        #[allow(unused_mut)]
        let (mut vdeps, augmented, builder_elapsed_ms, vdeps_edges) =
            if self.config.enable_virtual_dep_telemetry {
                let build_started = crate::instant::FzInstant::now();
                let (vdeps, augmented) = builder.build(to_evaluate);
                let builder_elapsed_ms = build_started.elapsed().as_millis();
                let vdeps_edges = vdeps.values().map(|deps| deps.len()).sum::<usize>();
                (vdeps, augmented, builder_elapsed_ms, vdeps_edges)
            } else {
                let (vdeps, augmented) = builder.build(to_evaluate);
                (vdeps, augmented, 0, 0)
            };

        // Replan hints from stale dynamic reads earlier in this request.
        {
            self.freshness_merge_hints(to_evaluate, &mut vdeps);
            self.freshness_extent_hints(to_evaluate, &mut vdeps);
        }
        let mut final_evaluate = to_evaluate.to_vec();
        if !augmented.is_empty() {
            final_evaluate.extend(augmented);
            final_evaluate.sort_unstable();
            final_evaluate.dedup();
        }

        let use_virtual = !vdeps.is_empty();

        let schedule = self.create_authority_schedule(&final_evaluate, &vdeps, ledger)?;

        let meta = ScheduleBuildMeta {
            candidate_vertices: to_evaluate.len(),
            vdeps_vertices: vdeps.len(),
            vdeps_edges,
            builder_elapsed_ms,
            used_virtual_schedule: use_virtual,
            schedule_cache_hit: false,
            schedule_cache_eligible: false,
        };

        Ok((schedule, vdeps, meta))
    }

    fn create_authority_schedule(
        &self,
        candidates: &[VertexId],
        vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
        mut ledger: Option<&mut ResourceLedger>,
    ) -> Result<crate::engine::scheduler::Schedule, ExcelError> {
        use crate::engine::authority::{
            geom::{Cover, Rect},
            plan_schedule, planner,
            proj::{AxisMap, RefProj},
            store::{EdgeKey, Tag},
        };
        let failure = |message: String| {
            ExcelError::new(ExcelErrorKind::Error)
                .with_message(format!("unified_authority planner: {message}"))
        };
        self.cancellation_checkpoint("Evaluation cancelled before authority planning")?;
        let store = self
            .graph
            .authority_plan_store()
            .map_err(Self::authority_excel_error)?;
        // One formula cell without hints (a tiny edit): its plan is the cell
        // alone unless it reads itself (`planner::plan_single`).
        if let [only] = candidates
            && vdeps.is_empty()
            && self.graph.authority_host().observed(*only).is_none()
            && let Some(cell) = self.graph.authority_cell_of_vertex(*only)
            && cell.0 != crate::engine::authority::geom::SYMBOL_SHEET
            && let Some(single) = planner::plan_single(store, cell)
        {
            #[cfg(debug_assertions)]
            {
                let mut cover = Cover::new();
                cover.insert_rect(cell.0, &Rect::new(cell.1, cell.2, cell.1, cell.2));
                let general = planner::plan_with_hints(store, &cover, &[], None, None, None)
                    .expect("general plan of one cell");
                assert_eq!(
                    general.cells.as_slice(),
                    &[single],
                    "single-cell plan differs from the planner at {cell:?}"
                );
            }
            let adapted = plan_schedule::schedule(
                &[single],
                0,
                None,
                |cell| {
                    self.graph
                        .authority_vertex_of_formula(cell.id, (cell.sheet, cell.row, cell.col))
                        .ok_or_else(|| failure("missing executor identity".to_owned()))
                },
                |_work| Ok(()),
            )
            .map_err(|error| match error {
                plan_schedule::ScheduleError::Runtime(error) => error,
                other => failure(format!("{other:?}")),
            })?;
            if let Some(ledger) = ledger {
                ledger
                    .reserve_schedule_discovery(adapted.peak_heap_bytes)
                    .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
                ledger
                    .release_scratch(adapted.peak_heap_bytes)
                    .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
            }
            return Ok(adapted.schedule);
        }
        // A small request of grid formula cells without hints: per-cell
        // arcs and longest-path levels (`planner::plan_small`).
        if (2..=planner::SMALL_PLAN_MAX).contains(&candidates.len())
            && vdeps.is_empty()
            && candidates
                .iter()
                .all(|&v| self.graph.authority_host().observed(v).is_none())
        {
            let cells: Option<Vec<(u16, u32, u32)>> = candidates
                .iter()
                .map(|&v| self.graph.authority_cell_of_vertex(v))
                .collect();
            if let Some(small) = cells.as_deref().and_then(|c| planner::plan_small(store, c)) {
                #[cfg(debug_assertions)]
                {
                    let mut cover = Cover::new();
                    for c in cells.as_deref().unwrap_or_default() {
                        cover.insert_rect(c.0, &Rect::new(c.1, c.2, c.1, c.2));
                    }
                    let general = planner::plan_with_hints(store, &cover, &[], None, None, None)
                        .expect("general plan of a small request");
                    let key = |c: &planner::OrderedCell| (c.sheet, c.row, c.col, c.id, c.owner);
                    let mut a: Vec<_> = general.cells.iter().map(key).collect();
                    let mut b: Vec<_> = small.iter().map(key).collect();
                    a.sort_unstable();
                    b.sort_unstable();
                    assert_eq!(a, b, "small plan cells differ from the planner");
                    assert!(
                        general.cells.iter().all(|c| c.cycle.is_none()),
                        "small plan of a cyclic request"
                    );
                }
                let adapted = plan_schedule::schedule(
                    &small,
                    0,
                    None,
                    |cell| {
                        self.graph
                            .authority_vertex_of_formula(cell.id, (cell.sheet, cell.row, cell.col))
                            .ok_or_else(|| failure("missing executor identity".to_owned()))
                    },
                    |_work| Ok(()),
                )
                .map_err(|error| match error {
                    plan_schedule::ScheduleError::Runtime(error) => error,
                    other => failure(format!("{other:?}")),
                })?;
                if let Some(ledger) = ledger {
                    ledger
                        .reserve_schedule_discovery(adapted.peak_heap_bytes)
                        .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
                    ledger
                        .release_scratch(adapted.peak_heap_bytes)
                        .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
                }
                return Ok(adapted.schedule);
            }
        }
        // Names are symbol-plane nodes (design §4.1): a name vertex plans as
        // the unit at its node, between its precedents and its readers.
        // Candidates become cells, sorted by (sheet, column, row) and
        // coalesced into row intervals: one cover insert per interval.
        let mut cover = Cover::new();
        {
            let cell_of = |&id: &VertexId| {
                self.graph
                    .authority_cell_of_vertex(id)
                    .map(|(sheet, row, col)| (sheet, col, row))
            };
            // A full recalc maps and sorts every formula: on the pool when
            // there is one (first eval's schedule is serial work otherwise).
            let cells: Vec<(u16, u32, u32)> = match self.thread_pool.as_deref() {
                Some(pool) if candidates.len() >= PARALLEL_SCHEDULE_MIN_CANDIDATES => {
                    use rayon::prelude::*;
                    pool.install(|| {
                        let mut cells: Vec<_> = candidates.par_iter().filter_map(cell_of).collect();
                        cells.par_sort_unstable();
                        cells
                    })
                }
                _ => {
                    let mut cells: Vec<_> = candidates.iter().filter_map(cell_of).collect();
                    cells.sort_unstable();
                    cells
                }
            };
            let mut i = 0;
            while i < cells.len() {
                let (sheet, col, r0) = cells[i];
                let mut r1 = r0;
                let mut j = i + 1;
                while j < cells.len() && cells[j].0 == sheet && cells[j].1 == col {
                    if cells[j].2 > r1 + 1 {
                        break;
                    }
                    r1 = r1.max(cells[j].2);
                    j += 1;
                }
                cover.insert_rect(sheet, &Rect::new(r0, col, r1, col));
                i = j;
            }
        }
        let mut hints = Vec::new();
        for (&reader, deps) in vdeps {
            let Some(reader) = self.graph.authority_cell_of_vertex(reader) else {
                continue;
            };
            for &dependency in deps {
                let Some(dep) = self.graph.authority_cell_of_vertex(dependency) else {
                    continue;
                };
                hints.push(planner::PlanHint {
                    reader: (reader.0, reader.2, reader.1),
                    edge: EdgeKey {
                        dep_sheet: reader.0,
                        tag: Tag::X,
                        lk: u32::MAX,
                        proj: RefProj {
                            sheet: dep.0,
                            rows: AxisMap::fixed(dep.1, dep.1),
                            cols: AxisMap::fixed(dep.2, dep.2),
                        },
                    },
                });
            }
        }
        // rdi_dyn: order each dynamic reader after its observed reads.
        let host = self.graph.authority_host();
        for &id in candidates.iter().filter(|_| host.has_observed()) {
            let Some(reads) = host.observed(id) else {
                continue;
            };
            let Some(reader) = self.graph.authority_cell_of_vertex(id) else {
                continue;
            };
            for &(sheet, r0, c0, r1, c1) in reads {
                hints.push(planner::PlanHint {
                    reader: (reader.0, reader.2, reader.1),
                    edge: EdgeKey {
                        dep_sheet: reader.0,
                        tag: Tag::X,
                        lk: u32::MAX,
                        proj: RefProj {
                            sheet,
                            rows: AxisMap::fixed(r0, r1),
                            cols: AxisMap::fixed(c0, c1),
                        },
                    },
                });
            }
        }
        hints.sort_unstable_by_key(|hint| hint.reader);
        let checkpoint = ledger
            .as_ref()
            .map_or(0, |ledger| ledger.scratch_checkpoint());
        let scratch_limit = ledger
            .as_ref()
            .and_then(|ledger| ledger.schedule_discovery_limit())
            .map(|limit| limit.saturating_sub(checkpoint));
        let ordered = planner::plan_with_hints(store, &cover, &hints, scratch_limit, None, None)
            .map_err(|error| failure(format!("{error:?}")))?;
        // max_work_units is an execution budget. Planning work must be capped
        // independently: charging it here changes the observable publication
        // boundary (e.g. a spill must commit before the next execution fails).
        let adapted = plan_schedule::schedule(
            &ordered.cells,
            ordered.heap_bytes(),
            scratch_limit,
            |cell| {
                self.graph
                    .authority_vertex_of_formula(cell.id, (cell.sheet, cell.row, cell.col))
                    .ok_or_else(|| failure("missing executor identity".to_owned()))
            },
            |_work| {
                self.cancellation_checkpoint("Evaluation cancelled during authority planning")?;
                if let Some(ledger) = ledger.as_deref_mut() {
                    ledger
                        .checkpoint_deadline()
                        .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
                }
                Ok(())
            },
        )
        .map_err(|error| match error {
            plan_schedule::ScheduleError::Runtime(error) => error,
            other => failure(format!("{other:?}")),
        })?;
        if let Some(ledger) = ledger {
            let peak = ordered.peak_heap_bytes.max(adapted.peak_heap_bytes);
            ledger
                .reserve_schedule_discovery(peak)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
            ledger
                .release_scratch(peak)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        // The planner's order is scratch now; a full recalc's is large.
        match self.thread_pool.as_deref() {
            Some(pool) if ordered.cells.len() >= PARALLEL_SCHEDULE_MIN_CANDIDATES => {
                pool.spawn(move || drop(ordered))
            }
            _ => drop(ordered),
        }
        Ok(adapted.schedule)
    }

    /// Static-schedule cache eligibility. Legacy excludes range readers:
    /// their order comes from per-request range virtual deps. Under the
    /// authority a range read is a static edge of the relation, so only
    /// dynamic readers (whose hints are per request) are excluded, and the
    /// key adds the authority revision (design §8.4).
    fn can_use_static_schedule_cache(&self, to_evaluate: &[VertexId]) -> bool {
        {
            // A dynamic reader is planned from its observed reads, which the
            // key covers (rev.dyn); one without them needs a pre-probe, and
            // request-scoped replan hints are never cached.
            let host = self.graph.authority_host();
            !to_evaluate.is_empty()
                && !self.freshness_has_hints()
                && to_evaluate
                    .iter()
                    .all(|&v| !self.graph.is_dynamic(v) || host.observed(v).is_some())
        }
    }

    fn schedule_cache_authority_revision(&self) -> (u64, u64) {
        {
            // rev.topology is `topology_epoch`; a symbol revision rebuilds the
            // store, so it is part of `revision`.
            let host = self.graph.authority_host();
            (host.revision(), host.rev_dyn())
        }
    }

    fn start_virtual_dep_telemetry(&self) -> VirtualDepTelemetry {
        VirtualDepTelemetry {
            fallback_mode_activations: self.virtual_dep_fallback_activations,
            ..VirtualDepTelemetry::default()
        }
    }

    fn accumulate_schedule_meta(telemetry: &mut VirtualDepTelemetry, meta: &ScheduleBuildMeta) {
        telemetry.candidate_vertices_total += meta.candidate_vertices;
        telemetry.vdeps_vertices_total += meta.vdeps_vertices;
        telemetry.vdeps_edges_total += meta.vdeps_edges;
        telemetry.builder_elapsed_ms_total += meta.builder_elapsed_ms;
        if meta.schedule_cache_eligible {
            if meta.schedule_cache_hit {
                telemetry.schedule_cache_hits += 1;
                telemetry.reused_schedule_vertices_total += meta.candidate_vertices;
            } else {
                telemetry.schedule_cache_misses += 1;
            }
        }
        if meta.used_virtual_schedule {
            telemetry.schedule_virtual_passes += 1;
        } else {
            telemetry.schedule_static_passes += 1;
        }
    }

    /// End-of-pass dirty bookkeeping; true when the loop must replan.
    /// Legacy: clear the pass, re-dirty readers whose pre-probe changed.
    /// Under the authority an armed pass keeps stale and unreached vertices
    /// dirty instead (design §8.2, `freshness.rs`).
    fn finish_pass_dirty(&mut self, to_evaluate: &[VertexId], changed: &[VertexId]) -> bool {
        self.finish_pass_dirty_scoped(to_evaluate, changed, true)
    }

    /// [`Self::finish_pass_dirty`] for a targeted pass: only its candidates
    /// decide whether to replan.
    fn finish_target_pass_dirty(&mut self, to_evaluate: &[VertexId], changed: &[VertexId]) -> bool {
        self.finish_pass_dirty_scoped(to_evaluate, changed, false)
    }

    fn finish_pass_dirty_scoped(
        &mut self,
        to_evaluate: &[VertexId],
        changed: &[VertexId],
        #[allow(unused_variables)] whole_workbook: bool,
    ) -> bool {
        self.freshness_finish_pass(to_evaluate, changed, whole_workbook)
    }

    /// Start a pass over `schedule` (arms the freshness recorder).
    fn begin_pass(
        &mut self,
        #[allow(unused_variables)] schedule: &crate::engine::scheduler::Schedule,
    ) {
        self.freshness_begin_pass(schedule);
    }

    /// FR4 layer barrier after unit `index`: true stops the pass.
    fn stop_after_unit(
        &mut self,
        #[allow(unused_variables)] schedule: &crate::engine::scheduler::Schedule,
        #[allow(unused_variables)] index: usize,
    ) -> bool {
        self.freshness_stop_after_unit(schedule, index)
    }

    fn changed_virtual_dep_vertices(
        &mut self,
        to_evaluate: &[VertexId],
        old_vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    ) -> Vec<VertexId> {
        #[cfg(test)]
        if self.force_virtual_dep_changes_remaining_for_test > 0
            && let Some(vertex) = to_evaluate.first().copied()
        {
            self.force_virtual_dep_changes_remaining_for_test -= 1;
            return vec![vertex];
        }
        // An armed pass detects stale dynamic reads directly; the pre-probe
        // comparison is legacy's substitute for that (design §8.2).
        if self.freshness_armed() {
            return Vec::new();
        }
        if !to_evaluate
            .iter()
            .copied()
            .any(|v| self.graph.is_dynamic(v))
        {
            return Vec::new();
        }

        let builder = VirtualDepBuilder::new(self);
        let (new_vdeps, _) = builder.build(to_evaluate);

        let mut candidates = FxHashSet::default();
        candidates.extend(old_vdeps.keys().copied());
        candidates.extend(new_vdeps.keys().copied());

        let mut changed = Vec::new();
        for v in candidates {
            if old_vdeps.get(&v) != new_vdeps.get(&v) {
                changed.push(v);
            }
        }
        changed
    }

    /// Build a demand-driven subgraph for the given targets, including ephemeral edges for
    /// compressed ranges, and returning the set of dirty/volatile precedents and virtual deps.
    /// Demand candidates of `targets` and their dynamic plan hints: under
    /// `unified_authority` from the authority's relation (design §8.3),
    /// otherwise from the legacy graph.
    #[allow(clippy::type_complexity)]
    fn demand_subgraph(
        &self,
        targets: &[VertexId],
    ) -> Result<
        (
            Vec<VertexId>,
            rustc_hash::FxHashMap<VertexId, Vec<VertexId>>,
        ),
        ExcelError,
    > {
        self.authority_demand_subgraph(targets)
    }

    /// Design §8.3: traverse precedents from the targets over the
    /// authority's static relation (symbol nodes are ordinary pieces) plus
    /// the dynamic readers' virtual dependencies, and collect what legacy's
    /// demand walk collects: dirty or volatile formula cells and every name
    /// passed through. No legacy dependency structure is read.
    #[allow(clippy::type_complexity)]
    fn authority_demand_subgraph(
        &self,
        targets: &[VertexId],
    ) -> Result<
        (
            Vec<VertexId>,
            rustc_hash::FxHashMap<VertexId, Vec<VertexId>>,
        ),
        ExcelError,
    > {
        use crate::engine::authority::geom::{Cell, Rect, SYMBOL_SHEET};
        use crate::engine::authority::store::TagFilter;
        use rustc_hash::{FxHashMap, FxHashSet};
        let store = self
            .graph
            .authority_plan_store()
            .map_err(Self::authority_excel_error)?;
        let ids = store.ids();
        let mut to_evaluate: FxHashSet<VertexId> = FxHashSet::default();
        let mut vdeps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default();
        let mut visited: FxHashSet<Cell> = FxHashSet::default();
        // (cell, authority id or NO_VID): the id lets the side array
        // translate the cell without a hash lookup.
        let mut stack: Vec<(Cell, u32)> = Vec::new();
        // Formula cells under `rect` on `sheet`: identity runs per column.
        let push_formulas = |stack: &mut Vec<(Cell, u32)>,
                             visited: &FxHashSet<Cell>,
                             sheet: u16,
                             rect: Rect| {
            for col in rect.c0..=rect.c1 {
                ids.visit_runs_in(sheet, col, rect.r0, rect.r1, &mut |h| {
                    let run = ids.run(h);
                    let r0 = run.row_start.max(rect.r0);
                    let r1 = (run.row_start + run.len - 1).min(rect.r1);
                    for row in r0..=r1 {
                        if !visited.contains(&(sheet, row, col)) {
                            stack.push(((sheet, row, col), run.first_id + (row - run.row_start)));
                        }
                    }
                });
            }
        };
        for &v in targets {
            if let Some(table) = self.graph.table_by_vertex(v) {
                // A table's demand is its range's, as legacy's table vertex
                // leads to the cells it covers (its symbol row has no
                // precedents).
                let (s, e) = (table.range.start, table.range.end);
                push_formulas(
                    &mut stack,
                    &visited,
                    s.sheet_id,
                    Rect::new(s.coord.row(), s.coord.col(), e.coord.row(), e.coord.col()),
                );
            } else if let Some(cell) = self.graph.authority_cell_of_vertex(v) {
                stack.push((cell, crate::engine::authority::identity::NO_VID));
            }
        }
        let mut hits = Vec::new();
        #[cfg(any(test, feature = "benchmark_internal"))]
        let (mut probe_vertices, mut probe_clean_formulas, mut probe_edges, mut probe_dynamic) =
            (0, 0, 0, 0);
        while let Some((cell, id)) = stack.pop() {
            if !visited.insert(cell) {
                continue;
            }
            let Some(v) = self.graph.authority_vertex_of_formula(id, cell) else {
                continue;
            };
            if !self.graph.vertex_exists(v) {
                continue;
            }
            #[cfg(any(test, feature = "benchmark_internal"))]
            {
                probe_vertices += 1;
            }
            match self.graph.get_vertex_kind(v) {
                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                    if self.graph.is_dirty(v) || self.graph.is_volatile(v) {
                        to_evaluate.insert(v);
                    } else {
                        #[cfg(any(test, feature = "benchmark_internal"))]
                        {
                            probe_clean_formulas += 1;
                        }
                    }
                }
                VertexKind::NamedScalar | VertexKind::NamedArray => {
                    to_evaluate.insert(v);
                }
                _ => {}
            }
            hits.clear();
            store.direct_precedents(cell, TagFilter::All, &mut hits);
            #[cfg(any(test, feature = "benchmark_internal"))]
            {
                probe_edges += hits.len();
            }
            for &(_, sheet, rect) in &hits {
                // DirtyExtents: a dirty spill anchor whose extent meets this
                // image is a demand precedent, ordered before `v` (§8.2).
                if sheet != SYMBOL_SHEET {
                    for anchor in self
                        .graph
                        .spill_anchors_in_region(sheet, rect.r0, rect.c0, rect.r1, rect.c1)
                    {
                        if anchor != v && self.graph.is_dirty(anchor) {
                            vdeps.entry(v).or_default().push(anchor);
                            if let Some(c) = self.graph.authority_cell_of_vertex(anchor) {
                                stack.push((c, crate::engine::authority::identity::NO_VID));
                            }
                        }
                    }
                }
                if sheet == SYMBOL_SHEET {
                    stack.extend((rect.r0..=rect.r1).map(|slot| {
                        (
                            (SYMBOL_SHEET, slot, 0),
                            crate::engine::authority::identity::NO_VID,
                        )
                    }));
                    continue;
                }
                push_formulas(&mut stack, &visited, sheet, rect);
            }
            if self.graph.is_dynamic(v) {
                #[cfg(any(test, feature = "benchmark_internal"))]
                {
                    probe_dynamic += 1;
                }
                // rdi_dyn: the observed reads are demand precedents.
                if let Some(reads) = self.graph.authority_host().observed(v) {
                    for &(sheet, r0, c0, r1, c1) in reads {
                        push_formulas(&mut stack, &visited, sheet, Rect::new(r0, c0, r1, c1));
                    }
                }
                let (vdeps_map, _) = VirtualDepBuilder::new(self).build(&[v]);
                // Pre-probe targets plus reads this request found dirty
                // (design §8.3: demand walks rdi ∪ hints).
                let hinted = self.freshness_hints(v).unwrap_or(&[]);
                if let Some(deps) = vdeps_map
                    .get(&v)
                    .map(|deps| deps.iter().chain(hinted))
                    .or(Some([].iter().chain(hinted)))
                {
                    for &u in deps {
                        vdeps.entry(v).or_default().push(u);
                        if let Some(c) = self.graph.authority_cell_of_vertex(u) {
                            stack.push((c, crate::engine::authority::identity::NO_VID));
                        }
                    }
                }
            }
        }
        let mut result: Vec<VertexId> = to_evaluate.into_iter().collect();
        result.sort_unstable();
        for deps in vdeps.values_mut() {
            deps.sort_unstable();
            deps.dedup();
        }
        #[cfg(any(test, feature = "benchmark_internal"))]
        {
            let mut probe = self.recalc_reuse_probe.lock().unwrap();
            probe.demand_builds += 1;
            probe.demand_vertices += probe_vertices;
            probe.demand_clean_formulas += probe_clean_formulas;
            probe.demand_explicit_edges += probe_edges;
            probe.demand_virtual_builder_calls += probe_dynamic;
        }
        Ok((result, vdeps))
    }

    /// Helper: convert 1-based column index to Excel-style letters (1 -> A, 27 -> AA)
    fn col_to_letters(col: u32) -> String {
        col_letters_from_1based(col).expect("column index must be >= 1")
    }

    /// Evaluate all dirty/volatile vertices with cancellation support
    pub fn evaluate_all_cancellable(
        &mut self,
        cancel: crate::engine::CancelToken,
    ) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::FullCancellable, |engine| {
            engine.observe_function_semantic_epoch()?;
            engine.active_cancel_flag = Some(cancel.clone());
            let res = engine.evaluate_all_cancellable_impl(cancel.as_flag());
            engine.active_cancel_flag = None;
            res
        })
    }

    fn evaluate_all_cancellable_impl(
        &mut self,
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let _source_cache = self.source_cache_session();
        self.validate_deterministic_mode()?;
        if self.config.defer_graph_building {
            self.build_graph_all()?;
        }
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Evaluation cancelled before scheduling".to_string()));
        }
        self.require_unified_authority()?;
        self.begin_evaluation_request();
        self.reset_virtual_dep_telemetry_if_disabled();
        let start = crate::instant::FzInstant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let mut replan_iterations = 0;
        const MAX_REPLAN: usize = 5;
        let mut telemetry = self
            .config
            .enable_virtual_dep_telemetry
            .then(|| self.start_virtual_dep_telemetry());

        loop {
            if cancel_flag.load(Ordering::Relaxed) {
                if let Some(mut t) = telemetry {
                    t.bailout_reason = Some("cancelled");
                    t.replan_iterations = replan_iterations;
                    self.last_virtual_dep_telemetry = t;
                }
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled before scheduling".to_string()));
            }

            let to_evaluate = self.graph.get_evaluation_vertices();
            if to_evaluate.is_empty() {
                if let Some(t) = telemetry.as_mut()
                    && t.bailout_reason.is_none()
                {
                    t.bailout_reason = Some("no_work");
                }
                break;
            }

            let (schedule, old_vdeps, meta) = self.create_evaluation_schedule(&to_evaluate)?;
            if let Some(t) = telemetry.as_mut() {
                Self::accumulate_schedule_meta(t, &meta);
            }

            // Walk units in condensation order, checking cancellation between
            // units (formerly between cycles and between layers).
            self.begin_pass(&schedule);
            for (unit_index, &unit) in schedule.units.iter().enumerate() {
                match unit {
                    ScheduleUnit::Cycle(i) => {
                        // Check cancellation between cycles
                        if cancel_flag.load(Ordering::Relaxed) {
                            if let Some(mut t) = telemetry {
                                t.bailout_reason = Some("cancelled");
                                t.replan_iterations = replan_iterations;
                                self.last_virtual_dep_telemetry = t;
                            }
                            return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                                "Evaluation cancelled during cycle handling".to_string(),
                            ));
                        }

                        if self.handle_cycle_unit(
                            schedule.unit_cycle(i),
                            None,
                            None,
                            Some(cancel_flag),
                        )? > 0
                        {
                            cycle_errors += 1;
                        }
                    }
                    ScheduleUnit::Layer(i) => {
                        let layer = schedule.unit_layer(i);
                        // Check cancellation between layers
                        if cancel_flag.load(Ordering::Relaxed) {
                            if let Some(mut t) = telemetry {
                                t.bailout_reason = Some("cancelled");
                                t.replan_iterations = replan_iterations;
                                self.last_virtual_dep_telemetry = t;
                            }
                            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                                .with_message("Evaluation cancelled between layers".to_string()));
                        }

                        // Evaluate vertices in this layer (parallel or sequential)
                        if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                            computed_vertices +=
                                self.evaluate_layer_parallel_cancellable(layer, cancel_flag)?;
                        } else {
                            computed_vertices +=
                                self.evaluate_layer_sequential_cancellable(layer, cancel_flag)?;
                        }
                    }
                }
                if self.stop_after_unit(&schedule, unit_index) {
                    break;
                }
            }

            let changed_vertices = self.changed_virtual_dep_vertices(&to_evaluate, &old_vdeps);
            if let Some(t) = telemetry.as_mut() {
                t.changed_vdeps_total += changed_vertices.len();
            }
            self.resource_checkpoint(0)?;
            if !self.finish_pass_dirty(&to_evaluate, &changed_vertices) {
                if let Some(t) = telemetry.as_mut() {
                    t.bailout_reason = Some("converged");
                }
                break;
            }
            if replan_iterations >= MAX_REPLAN {
                if let Some(mut t) = telemetry.take() {
                    t.bailout_reason = Some("max_replan");
                    t.replan_iterations = replan_iterations;
                    self.last_virtual_dep_telemetry = t;
                }
                return Err(
                    self.replan_exhausted_error(MAX_REPLAN, "dynamic dependency evaluation")
                );
            }
            replan_iterations += 1;
        }

        if let Some(mut t) = telemetry {
            t.replan_iterations = replan_iterations;
            self.last_virtual_dep_telemetry = t;
        }

        // Re-dirty volatile vertices for the next evaluation cycle
        self.redirty_for_next_recalc();
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate only the necessary precedents for specific target cells with cancellation support
    pub fn evaluate_until_cancellable(
        &mut self,
        targets: &[&str],
        cancel: crate::engine::CancelToken,
    ) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(
            EvaluationRequestKind::TargetedCancellable,
            |engine| {
                engine.observe_function_semantic_epoch()?;
                engine.active_cancel_flag = Some(cancel.clone());
                let res = engine.evaluate_until_cancellable_impl(targets, cancel.as_flag());
                engine.active_cancel_flag = None;
                res
            },
        )
    }

    fn evaluate_until_cancellable_impl(
        &mut self,
        targets: &[&str],
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Evaluation cancelled before target preparation"));
        }
        let mut typed_targets = Vec::with_capacity(targets.len());
        for target in targets {
            let (sheet, row, col) = self.parse_a1_notation(target)?;
            self.graph.sheet_id_mut(&sheet);
            typed_targets.push(crate::engine::EvaluationTarget::Cell { sheet, row, col });
        }
        self.evaluate_mixed_targets(&typed_targets, None)
    }

    fn parse_a1_notation(&self, address: &str) -> Result<(String, u32, u32), ExcelError> {
        let mut quoted = false;
        let mut separator = None;
        let bytes = address.as_bytes();
        let mut index = 0usize;
        while index < bytes.len() {
            match bytes[index] {
                b'\'' => {
                    if quoted && bytes.get(index + 1) == Some(&b'\'') {
                        index = index.saturating_add(1);
                    } else {
                        quoted = !quoted;
                    }
                }
                b'!' if !quoted => separator = Some(index),
                _ => {}
            }
            index = index.saturating_add(1);
        }
        if quoted {
            return Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Invalid quoted sheet reference `{address}`")));
        }
        let (sheet, cell_part) = match separator {
            Some(separator) => {
                let raw_sheet = &address[..separator];
                let sheet = if raw_sheet.starts_with('\'') && raw_sheet.ends_with('\'') {
                    raw_sheet[1..raw_sheet.len().saturating_sub(1)].replace("''", "'")
                } else {
                    raw_sheet.to_string()
                };
                (sheet, &address[separator + 1..])
            }
            None => (self.default_sheet_name().to_string(), address),
        };

        let (row, col, _, _) = parse_a1_1based(cell_part).map_err(|err| {
            ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Invalid cell reference `{cell_part}`: {err}"))
        })?;

        Ok((sheet, row, col))
    }

    /// Determine volatility using this engine's FunctionProvider, falling back to global registry.
    fn is_ast_volatile_with_provider(&self, ast: &ASTNode) -> bool {
        use formualizer_parse::parser::ASTNodeType;
        match &ast.node_type {
            ASTNodeType::Function { name, args, .. } => {
                if let Some(func) = self
                    .get_function("", name)
                    .or_else(|| crate::function_registry::get("", name))
                    && func.caps().contains(crate::function::FnCaps::VOLATILE)
                {
                    return true;
                }
                args.iter()
                    .any(|arg| self.is_ast_volatile_with_provider(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.is_ast_volatile_with_provider(left)
                    || self.is_ast_volatile_with_provider(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.is_ast_volatile_with_provider(expr),
            ASTNodeType::Array(rows) => rows.iter().any(|row| {
                row.iter()
                    .any(|cell| self.is_ast_volatile_with_provider(cell))
            }),
            _ => false,
        }
    }

    /// Evaluate a layer sequentially
    fn evaluate_layer_sequential(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        self.evaluate_layer_sequential_effects(layer)
    }

    fn update_vertex_value_with_delta(
        &mut self,
        vertex_id: VertexId,
        new_value: LiteralValue,
        delta: &mut DeltaCollector,
    ) {
        if delta.mode != DeltaMode::Off
            && let Some(cell) = self.graph.get_cell_ref_for_vertex(vertex_id)
        {
            let sheet_name = self.graph.sheet_name(cell.sheet_id);
            let old = self
                .read_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                .unwrap_or(LiteralValue::Empty);
            if old != new_value {
                delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
            }
        }
        self.graph.update_vertex_value_ref(vertex_id, &new_value);
        self.mirror_vertex_value_to_overlay(vertex_id, &new_value);
    }

    fn evaluate_layer_sequential_with_delta(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        self.evaluate_layer_sequential_with_delta_effects(layer, delta)
    }

    /// Evaluate a layer sequentially with cancellation support
    fn evaluate_layer_sequential_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        self.evaluate_layer_sequential_cancellable_effects(layer, cancel_flag)
    }

    /// Evaluate a layer sequentially with more frequent cancellation checks for demand-driven evaluation
    fn evaluate_layer_sequential_cancellable_demand_driven(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        self.evaluate_layer_sequential_cancellable_demand_driven_effects(layer, cancel_flag)
    }

    /// Evaluate a layer in parallel using the thread pool.
    ///
    /// Cost-adaptive: the layer starts sequentially in slices of doubling
    /// size (1, 2, 4, ... vertices, capped so a slice does not overshoot the
    /// probe) and hands the rest to the pool once the rest looks worth it
    /// (`PARALLEL_LAYER_WORTH` at the rate so far) or the probe
    /// (`PARALLEL_LAYER_PROBE`) is spent. A cheap layer never pays the pool's
    /// wake-up and join (most Enron layers are tens of µs of work); an
    /// expensive one goes parallel after a few vertices. Splitting a layer
    /// into consecutive sub-layers is a valid order: its vertices are
    /// independent.
    fn evaluate_layer_parallel(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        if layer.sequential {
            return self.evaluate_layer_sequential(layer);
        }
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        let len = layer.vertices.len();
        let buffered = buffer_layer_writes(layer);
        let (probe, worth) = (PARALLEL_LAYER_PROBE, PARALLEL_LAYER_WORTH);
        let start = crate::instant::FzInstant::now();
        let mut pos = 0usize;
        let mut step = 1usize;
        while pos < len {
            if pos > 0 {
                let elapsed = start.elapsed();
                // Rate so far (ns per vertex) and the rest at that rate.
                let per_vertex = elapsed.as_nanos() / pos as u128 + 1;
                let rest_estimate = per_vertex * (len - pos) as u128;
                if len - pos >= 2 && (elapsed >= probe || rest_estimate >= worth.as_nanos()) {
                    let rest = layer.sub_layer(pos, len);
                    // Expensive members (a SUMIF over a table) parallelize
                    // one per task; cheap ones keep runs of 8 together.
                    let min_chunk = if per_vertex >= EXPENSIVE_VERTEX_NS {
                        1
                    } else {
                        8
                    };
                    return Ok(pos + self.evaluate_layer_parallel_effects(&rest, min_chunk)?);
                }
                // Next slice: double, but no more than the rest of the probe
                // at the rate so far (a slice must not overshoot it).
                let fit = (probe.saturating_sub(elapsed).as_nanos() / per_vertex) as usize + 1;
                step = step.saturating_mul(2).min(fit);
            }
            let end = (pos + step).min(len);
            let slice = layer.sub_layer(pos, end);
            // A slice stops at the probe's end even if its members turn out
            // far more expensive than the rate so far predicted.
            pos += self.evaluate_layer_units_until(
                &slice,
                None,
                None,
                None,
                buffered,
                Some(start + probe),
            )?;
        }
        Ok(len)
    }

    fn evaluate_layer_parallel_with_delta(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        if layer.sequential {
            return self.evaluate_layer_sequential_with_delta(layer, delta);
        }
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        self.evaluate_layer_parallel_with_delta_effects(layer, delta)
    }

    /// Evaluate a layer in parallel with cancellation support
    fn evaluate_layer_parallel_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        if layer.sequential {
            return self.evaluate_layer_sequential_cancellable(layer, cancel_flag);
        }
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        self.evaluate_layer_parallel_cancellable_effects(layer, cancel_flag)
    }

    /// Evaluate a single vertex without mutating the graph (for parallel evaluation)
    fn evaluate_vertex_immutable(&self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let view = match kind {
            VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                if let Some(view) = self.graph.formula_view(vertex_id) {
                    view
                } else {
                    return Ok(LiteralValue::Number(0.0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                if let Some(cell_ref) = self.graph.get_cell_ref(vertex_id) {
                    let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                    let row = cell_ref.coord.row() + 1;
                    let col = cell_ref.coord.col() + 1;
                    if let Some(v) = self.read_cell_value(sheet_name, row, col) {
                        return Ok(v);
                    }
                }
                return Ok(LiteralValue::Number(0.0));
            }
            VertexKind::NamedScalar => {
                let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message("Named range metadata missing".to_string())
                })?;

                return match &named_range.definition {
                    NamedDefinition::Cell(cell_ref) => {
                        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                        Ok(self
                            .get_cell_value(
                                sheet_name,
                                cell_ref.coord.row() + 1,
                                cell_ref.coord.col() + 1,
                            )
                            .unwrap_or(LiteralValue::Empty))
                    }
                    NamedDefinition::Literal(v) => Ok(v.clone()),
                    NamedDefinition::Formula { ast, .. } => {
                        let context_sheet = match named_range.scope {
                            NameScope::Sheet(id) => id,
                            NameScope::Workbook => sheet_id,
                        };
                        let sheet_name = self.graph.sheet_name(context_sheet);
                        let cell_ref = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                        interpreter.evaluate_ast(ast).map(|cv| cv.into_literal())
                    }
                    NamedDefinition::Range(_) => Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Range-valued name evaluated as scalar".to_string())),
                };
            }
            VertexKind::NamedArray => {
                let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message("Named range metadata missing".to_string())
                })?;

                return match &named_range.definition {
                    NamedDefinition::Range(range_ref) => {
                        if range_ref.start.sheet_id != range_ref.end.sheet_id {
                            return Err(ExcelError::new(ExcelErrorKind::Ref)
                                .with_message("Named range cannot span sheets".to_string()));
                        }
                        let sheet_name = self.graph.sheet_name(range_ref.start.sheet_id);
                        let sr0 = range_ref.start.coord.row();
                        let sc0 = range_ref.start.coord.col();
                        let er0 = range_ref.end.coord.row();
                        let ec0 = range_ref.end.coord.col();
                        if sr0 > er0 || sc0 > ec0 {
                            return Err(ExcelError::new(ExcelErrorKind::Ref)
                                .with_message("Invalid named range bounds".to_string()));
                        }

                        let h = (er0 - sr0 + 1) as usize;
                        let w = (ec0 - sc0 + 1) as usize;
                        let cell_count = (h as u64).saturating_mul(w as u64);
                        if cell_count > self.config.spill.max_spill_cells as u64 {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "Named range too large to materialize as an array".to_string(),
                            ));
                        }

                        // `get_cell_value` per cell, with the sheet resolved
                        // once (`read_cell_formatted_in` is its body).
                        let sheet_id = range_ref.start.sheet_id;
                        let asheet = self.arrow_sheets.sheet(sheet_name);
                        let mut rows = Vec::with_capacity(h);
                        for r0 in sr0..=er0 {
                            let mut row = Vec::with_capacity(w);
                            for c0 in sc0..=ec0 {
                                row.push(
                                    self.read_cell_formatted_in(sheet_id, asheet, r0 + 1, c0 + 1)
                                        .0,
                                );
                            }
                            rows.push(row);
                        }
                        Ok(LiteralValue::Array(rows))
                    }
                    NamedDefinition::Cell(cell_ref) => {
                        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                        let row = cell_ref.coord.row() + 1;
                        let col = cell_ref.coord.col() + 1;
                        let v = self
                            .get_cell_value(sheet_name, row, col)
                            .unwrap_or(LiteralValue::Empty);
                        Ok(LiteralValue::Array(vec![vec![v]]))
                    }
                    NamedDefinition::Literal(v) => Ok(LiteralValue::Array(vec![vec![v.clone()]])),
                    NamedDefinition::Formula { ast, .. } => {
                        let context_sheet = match named_range.scope {
                            NameScope::Sheet(id) => id,
                            NameScope::Workbook => sheet_id,
                        };
                        let sheet_name = self.graph.sheet_name(context_sheet);
                        let cell_ref = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                        match interpreter.evaluate_ast(ast) {
                            Ok(cv) => {
                                let v = cv.into_literal();
                                match v {
                                    LiteralValue::Array(_) => Ok(v),
                                    other => Ok(LiteralValue::Array(vec![vec![other]])),
                                }
                            }
                            Err(err) => Ok(LiteralValue::Error(err)),
                        }
                    }
                };
            }
            VertexKind::InfiniteRange
            | VertexKind::Range
            | VertexKind::External
            | VertexKind::Table => {
                // Not directly evaluatable here.
                return Ok(LiteralValue::Number(0.0));
            }
        };

        // The interpreter uses a reference to the engine as the context
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        if let Some(result) =
            self.freshness_evaluate_recorded(vertex_id, sheet_name, cell_ref, view)
        {
            return result;
        }
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);

        interpreter
            .evaluate_formula_view(view, self.graph.data_store(), self.graph.sheet_reg())
            .map(|cv| {
                let format = cv.format_id();
                self.record_derived_format(vertex_id, format);
                crate::engine::result_finalization::finalize_published_calc_result(
                    cv,
                    self.config.spill.max_spill_cells,
                )
            })
    }

    /// Get access to the shared thread pool for parallel evaluation
    pub fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }
}

#[derive(Default)]
struct RowBoundsCache {
    snapshot: u64,
    // key: (sheet_id, col_idx)
    map: rustc_hash::FxHashMap<(u32, usize), (Option<u32>, Option<u32>)>,
}

impl RowBoundsCache {
    fn new(snapshot: u64) -> Self {
        Self {
            snapshot,
            map: Default::default(),
        }
    }
    fn get_row_bounds(
        &self,
        sheet_id: SheetId,
        col_idx: usize,
        snapshot: u64,
    ) -> Option<(Option<u32>, Option<u32>)> {
        if self.snapshot != snapshot {
            return None;
        }
        self.map.get(&(sheet_id as u32, col_idx)).copied()
    }
    fn put_row_bounds(
        &mut self,
        sheet_id: SheetId,
        col_idx: usize,
        snapshot: u64,
        bounds: (Option<u32>, Option<u32>),
    ) {
        if self.snapshot != snapshot {
            self.snapshot = snapshot;
            self.map.clear();
        }
        self.map.insert((sheet_id as u32, col_idx), bounds);
    }
}

struct UsedAxisBoundsCache {
    snapshot: u64,
    row_bounds_by_col_span: rustc_hash::FxHashMap<(SheetId, u32, u32), Option<(u32, u32)>>,
    col_bounds_by_row_span: rustc_hash::FxHashMap<(SheetId, u32, u32), Option<(u32, u32)>>,
    #[cfg(test)]
    row_hits: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    row_misses: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    col_hits: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    col_misses: std::sync::atomic::AtomicUsize,
}

impl UsedAxisBoundsCache {
    fn new(snapshot: u64) -> Self {
        Self {
            snapshot,
            row_bounds_by_col_span: Default::default(),
            col_bounds_by_row_span: Default::default(),
            #[cfg(test)]
            row_hits: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            row_misses: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            col_hits: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            col_misses: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn reset_for_snapshot(&mut self, snapshot: u64) {
        if self.snapshot != snapshot {
            self.snapshot = snapshot;
            self.row_bounds_by_col_span.clear();
            self.col_bounds_by_row_span.clear();
        }
    }

    fn get_row_bounds(
        &self,
        sheet_id: SheetId,
        start_col: u32,
        end_col: u32,
        snapshot: u64,
    ) -> Option<Option<(u32, u32)>> {
        if self.snapshot != snapshot {
            return None;
        }
        let cached = self
            .row_bounds_by_col_span
            .get(&(sheet_id, start_col, end_col))
            .copied();
        #[cfg(test)]
        if cached.is_some() {
            self.row_hits.fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    fn put_row_bounds(
        &mut self,
        sheet_id: SheetId,
        start_col: u32,
        end_col: u32,
        snapshot: u64,
        bounds: Option<(u32, u32)>,
    ) {
        self.reset_for_snapshot(snapshot);
        self.row_bounds_by_col_span
            .insert((sheet_id, start_col, end_col), bounds);
        #[cfg(test)]
        self.row_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn get_col_bounds(
        &self,
        sheet_id: SheetId,
        start_row: u32,
        end_row: u32,
        snapshot: u64,
    ) -> Option<Option<(u32, u32)>> {
        if self.snapshot != snapshot {
            return None;
        }
        let cached = self
            .col_bounds_by_row_span
            .get(&(sheet_id, start_row, end_row))
            .copied();
        #[cfg(test)]
        if cached.is_some() {
            self.col_hits.fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    fn put_col_bounds(
        &mut self,
        sheet_id: SheetId,
        start_row: u32,
        end_row: u32,
        snapshot: u64,
        bounds: Option<(u32, u32)>,
    ) {
        self.reset_for_snapshot(snapshot);
        self.col_bounds_by_row_span
            .insert((sheet_id, start_row, end_row), bounds);
        #[cfg(test)]
        self.col_misses.fetch_add(1, Ordering::Relaxed);
    }
}

// Phase 2 shim: in-process spill manager delegating to current graph methods.
#[derive(Default)]
pub struct ShimSpillManager {
    region_locks: RegionLockManager,
    pub(crate) active_locks: rustc_hash::FxHashMap<VertexId, u64>,
}

impl ShimSpillManager {
    pub(crate) fn reserve(
        &mut self,
        owner: VertexId,
        anchor_cell: CellRef,
        shape: SpillShape,
        _meta: SpillMeta,
    ) -> Result<(), ExcelError> {
        // Derive region from anchor + shape; enforce in-flight exclusivity only.
        let region = crate::engine::spill::Region {
            sheet_id: anchor_cell.sheet_id as u32,
            row_start: anchor_cell.coord.row(),
            row_end: anchor_cell
                .coord
                .row()
                .saturating_add(shape.rows)
                .saturating_sub(1),
            col_start: anchor_cell.coord.col(),
            col_end: anchor_cell
                .coord
                .col()
                .saturating_add(shape.cols)
                .saturating_sub(1),
        };
        match self.region_locks.reserve(region, owner) {
            Ok(id) => {
                if id != 0 {
                    self.active_locks.insert(owner, id);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Release any in-flight region reservation still held for `owner`.
    ///
    /// Reservations are normally released on commit/rollback, but if an anchor is
    /// abandoned without committing (e.g. cycle detection stamps it with #CIRC), a
    /// stale reservation could remain. This is a no-op when nothing is held.
    pub(crate) fn release_owner(&mut self, owner: VertexId) {
        if let Some(id) = self.active_locks.remove(&owner) {
            self.region_locks.release(id);
        }
    }

    pub(crate) fn commit_array_with_value_probe<F>(
        &mut self,
        graph: &mut DependencyGraph,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
        mut value_probe: F,
    ) -> Result<(), ExcelError>
    where
        F: FnMut(&DependencyGraph, &CellRef) -> Option<LiteralValue>,
    {
        use formualizer_common::{ExcelErrorExtra, ExcelErrorKind};

        // Re-run plan on concrete targets before committing to respect blockers.
        // This plan checks formula/spill ownership in the graph, but when the graph value cache
        // is disabled (Arrow-canonical mode), it cannot see non-empty value blockers.
        let plan_res = graph.plan_spill_region_allowing_formula_overwrite(
            anchor_vertex,
            targets,
            overwritable_formulas,
        );
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
        }

        if !graph.value_cache_enabled() {
            // Compute expected spill shape from the target rectangle for diagnostics.
            let (expected_rows, expected_cols) = if targets.is_empty() {
                (0u32, 0u32)
            } else {
                let mut min_r = u32::MAX;
                let mut max_r = 0u32;
                let mut min_c = u32::MAX;
                let mut max_c = 0u32;
                for cell in targets {
                    let r = cell.coord.row();
                    let c = cell.coord.col();
                    min_r = min_r.min(r);
                    max_r = max_r.max(r);
                    min_c = min_c.min(c);
                    max_c = max_c.max(c);
                }
                (
                    max_r.saturating_sub(min_r).saturating_add(1),
                    max_c.saturating_sub(min_c).saturating_add(1),
                )
            };

            let anchor_cell = graph
                .get_cell_ref(anchor_vertex)
                .expect("anchor cell ref for spill commit");

            for cell in targets {
                // Never treat the anchor as a blocker.
                if *cell == anchor_cell {
                    continue;
                }
                // Skip cells already known to be owned by a spill; plan() handled spill conflicts.
                if graph.spill_registry_anchor_for_cell(*cell).is_some() {
                    continue;
                }
                // Skip formula vertices in the target region; plan() handled them (or allowed).
                if let Some(vid) = graph.get_vertex_id_for_address(cell)
                    && vid != anchor_vertex
                {
                    match graph.get_vertex_kind(vid) {
                        crate::engine::vertex::VertexKind::FormulaScalar
                        | crate::engine::vertex::VertexKind::FormulaArray => {
                            // plan() already approved allowed overwrites.
                            continue;
                        }
                        _ => {}
                    }
                }

                if let Some(v) = value_probe(graph, cell)
                    && !matches!(v, LiteralValue::Empty)
                {
                    if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                        self.region_locks.release(id);
                    }
                    return Err(ExcelError::new(ExcelErrorKind::Spill)
                        .with_message("BlockedByValue")
                        .with_extra(ExcelErrorExtra::Spill {
                            expected_rows,
                            expected_cols,
                        }));
                }
            }
        }

        let commit_res = graph.commit_spill_region_atomic_with_fault(
            anchor_vertex,
            targets.to_vec(),
            rows,
            None,
        );
        if let Some(id) = self.active_locks.remove(&anchor_vertex) {
            self.region_locks.release(id);
        }
        commit_res.map(|_| ())
    }

    /// Commit a spill and mirror all written cells into Arrow overlay via the owning engine.
    pub(crate) fn commit_array_with_overlay<R: EvaluationContext>(
        &mut self,
        engine: &mut Engine<R>,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<(), ExcelError> {
        if let Err(error) = engine.guard_pending_spill_commit(anchor_vertex, targets) {
            self.release_owner(anchor_vertex);
            return Err(error);
        }
        // Re-run plan on concrete targets before committing to respect blockers.
        let plan_res = engine.graph.plan_spill_region_allowing_formula_overwrite(
            anchor_vertex,
            targets,
            overwritable_formulas,
        );
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
        }

        let commit_res = engine.graph.commit_spill_region_atomic_with_fault(
            anchor_vertex,
            targets.to_vec(),
            rows.clone(),
            None,
        );
        if let Some(id) = self.active_locks.remove(&anchor_vertex) {
            self.region_locks.release(id);
        }
        commit_res.map(|_| ())?;
        engine
            .blocked_pending_spills
            .retain(|entry| entry.0 != anchor_vertex);

        // Mirror into Arrow overlay when enabled
        if engine.config.arrow_storage_enabled
            && engine.config.delta_overlay_enabled
            && engine.config.write_formula_overlay_enabled
        {
            // Expect targets to be a contiguous rectangle row-major starting at some anchor
            for (idx, cell) in targets.iter().enumerate() {
                let (r_off, c_off) = {
                    if rows.is_empty() || rows[0].is_empty() {
                        (0usize, 0usize)
                    } else {
                        let width = rows[0].len();
                        (idx / width, idx % width)
                    }
                };
                let v = rows
                    .get(r_off)
                    .and_then(|r| r.get(c_off))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                let sheet_name = engine.graph.sheet_name(cell.sheet_id).to_string();
                engine.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &v,
                );
            }
        }
        Ok(())
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_shared_ref(
        &self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<formualizer_common::SheetRef<'static>, ExcelError> {
        use formualizer_common::{
            SheetCellRef as SharedCellRef, SheetLocator, SheetRangeRef as SharedRangeRef,
            SheetRef as SharedRef,
        };

        // Preserve anchor flags from the parsed reference when possible.
        let sr = match reference {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => {
                let row0 = row
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let col0 = col
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };
                let coord = formualizer_common::RelativeCoord::new(row0, col0, *row_abs, *col_abs);
                SharedRef::Cell(SharedCellRef::new(sheet_loc, coord))
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };
                let sr = start_row
                    .map(|r| {
                        r.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let sc = start_col
                    .map(|c| {
                        c.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let er = end_row
                    .map(|r| {
                        r.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let ec = end_col
                    .map(|c| {
                        c.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let range = SharedRangeRef::from_parts(
                    sheet_loc,
                    sr.map(|idx| formualizer_common::AxisBound::new(idx, *start_row_abs)),
                    sc.map(|idx| formualizer_common::AxisBound::new(idx, *start_col_abs)),
                    er.map(|idx| formualizer_common::AxisBound::new(idx, *end_row_abs)),
                    ec.map(|idx| formualizer_common::AxisBound::new(idx, *end_col_abs)),
                )
                .map_err(|_| ExcelError::new(ExcelErrorKind::Ref))?;
                SharedRef::Range(range)
            }
            _ => return Err(ExcelError::new(ExcelErrorKind::Ref)),
        };

        let current_id = self
            .graph
            .sheet_id(current_sheet)
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;

        let resolve_loc = |loc: SheetLocator<'_>| -> Result<SheetLocator<'static>, ExcelError> {
            match loc {
                SheetLocator::Current => Ok(SheetLocator::Id(current_id)),
                SheetLocator::Id(id) => Ok(SheetLocator::Id(id)),
                SheetLocator::Name(name) => {
                    let n = name.as_ref();
                    self.graph
                        .sheet_id(n)
                        .map(SheetLocator::Id)
                        .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                }
            }
        };

        match sr {
            SharedRef::Cell(cell) => {
                let owned = cell.into_owned();
                let sheet = resolve_loc(owned.sheet)?;
                Ok(SharedRef::Cell(SharedCellRef::new(sheet, owned.coord)))
            }
            SharedRef::Range(range) => {
                let owned = range.into_owned();
                let sheet = resolve_loc(owned.sheet)?;
                Ok(SharedRef::Range(SharedRangeRef {
                    sheet,
                    start_row: owned.start_row,
                    start_col: owned.start_col,
                    end_row: owned.end_row,
                    end_col: owned.end_col,
                }))
            }
        }
    }
}

// Implement the resolver traits for the Engine.
// This allows the interpreter to resolve references by querying the engine's graph.
impl<R> crate::traits::ReferenceResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        // This context-free trait method has no knowledge of the formula's
        // current sheet, so an unqualified (`None`) reference cannot be resolved
        // here. Previously this fell back to `default_sheet_name()`, which leaked
        // the reference onto an unrelated sheet (issue #110). Interpreter paths
        // already qualify references with the current sheet before reaching this
        // method (see `Interpreter::implicit_intersection_from_reference`), and
        // the sheet-aware scalar path goes through `resolve_cell_reference_value`
        // with an explicit `current_sheet`. Returning #REF! for an unqualified
        // reference here surfaces the missing context instead of silently
        // returning data from the wrong sheet.
        let Some(sheet_name) = sheet else {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                "Unqualified cell reference resolved without sheet context".to_string(),
            ));
        };
        // Prefer engine's unified accessor which consults Arrow store for base values
        // and falls back to graph for formulas and stored values.
        if let Some(v) = self.get_cell_value(sheet_name, row, col) {
            Ok(v)
        } else {
            // Excel semantics: empty cell coerces to 0 in numeric contexts
            Ok(LiteralValue::Number(0.0))
        }
    }
}

impl<R> crate::traits::RangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn crate::traits::Range>, ExcelError> {
        // For now, delegate range resolution to the external resolver.
        // A future optimization could be to handle this within the graph.
        self.resolver.resolve_range_reference(sheet, sr, sc, er, ec)
    }
}

impl<R> crate::traits::NamedRangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        self.resolver.resolve_named_range_reference(name)
    }
}

impl<R> crate::traits::TableResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_table_reference(
        &self,
        tref: &formualizer_parse::parser::TableReference,
    ) -> Result<Box<dyn crate::traits::Table>, ExcelError> {
        self.resolver.resolve_table_reference(tref)
    }
}

impl<R> crate::traits::SourceResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn source_scalar_version(&self, name: &str) -> Option<u64> {
        self.resolver.source_scalar_version(name)
    }

    fn resolve_source_scalar(&self, name: &str) -> Result<LiteralValue, ExcelError> {
        self.resolver.resolve_source_scalar(name)
    }

    fn source_table_version(&self, name: &str) -> Option<u64> {
        self.resolver.source_table_version(name)
    }

    fn resolve_source_table(
        &self,
        name: &str,
    ) -> Result<Box<dyn crate::traits::Table>, ExcelError> {
        self.resolver.resolve_source_table(name)
    }
}

// The Engine is a Resolver because it implements the constituent traits.
impl<R> crate::traits::Resolver for Engine<R> where R: EvaluationContext {}

// The Engine provides functions by delegating to its internal resolver.
impl<R> crate::traits::FunctionProvider for Engine<R>
where
    R: EvaluationContext,
{
    fn planning_semantic_revision(&self) -> Option<u64> {
        self.resolver.planning_semantic_revision()
    }

    fn get_function(
        &self,
        prefix: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        self.resolver.get_function(prefix, name)
    }

    fn get_function_for_planning(
        &self,
        prefix: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        self.resolver.get_function_for_planning(prefix, name)
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Semantic used coordinates exclude graph-only dependency placeholders.
    ///
    /// Non-empty base/overlay/computed cells come from Arrow storage, while
    /// scalar and array formulas come from graph formula kinds even before
    /// their results are materialized. The legacy graph fallback is omitted:
    /// `load_packed_to_vertex` entries are either represented by those sources
    /// or are `Empty` dependency placeholders, not a third value authority.
    pub(crate) fn semantic_used_rows_for_columns(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        let arrow_bounds = self
            .sheet_store()
            .sheet(sheet)
            .and_then(|_| self.arrow_used_row_bounds(sheet, start_col, end_col));
        let formula_bounds = self.formula_row_bounds_for_columns(sheet, start_col, end_col);
        Self::union_used_bounds(arrow_bounds, formula_bounds)
    }

    pub(crate) fn semantic_used_cols_for_rows(
        &self,
        sheet: &str,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        let arrow_bounds = self
            .sheet_store()
            .sheet(sheet)
            .and_then(|_| self.arrow_used_col_bounds(sheet, start_row, end_row));
        let formula_bounds = self.formula_col_bounds_for_rows(sheet, start_row, end_row);
        Self::union_used_bounds(arrow_bounds, formula_bounds)
    }
}

// Override EvaluationContext to provide thread pool access
impl<R> crate::traits::EvaluationContext for Engine<R>
where
    R: EvaluationContext,
{
    fn clock(&self) -> &dyn crate::timezone::ClockProvider {
        &self.clock
    }

    fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }

    fn cancellation_token(&self) -> Option<crate::engine::CancelToken> {
        self.active_cancel_flag.clone()
    }

    fn chunk_hint(&self) -> Option<usize> {
        // Use a simple heuristic from configuration (stripe width * height) as a default hint.
        let hint =
            (self.config.stripe_height as usize).saturating_mul(self.config.stripe_width as usize);
        Some(hint.clamp(1024, 1 << 20)) // clamp between 1K and ~1M
    }

    fn volatile_level(&self) -> crate::traits::VolatileLevel {
        self.config.volatile_level
    }

    fn workbook_seed(&self) -> u64 {
        self.config.workbook_seed
    }

    fn recalc_epoch(&self) -> u64 {
        self.recalc_epoch
    }

    fn workbook_sheet_count(&self) -> Option<usize> {
        Some(self.graph.sheet_reg().active_len())
    }

    fn sheet_index_by_name(&self, sheet: &str) -> Option<usize> {
        self.graph.sheet_reg().active_position(sheet)
    }

    fn current_sheet_index(&self, current_sheet: &str) -> Option<usize> {
        self.sheet_index_by_name(current_sheet)
    }

    fn inspect_reference(
        &self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<Option<ReferenceInfo>, ExcelError> {
        let sheet_info = |sheet_name: &str| -> Result<(SheetId, usize), ExcelError> {
            let sheet_id = self
                .graph
                .sheet_id(sheet_name)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
            let sheet_index = self
                .graph
                .sheet_reg()
                .active_position_by_id(sheet_id)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
            Ok((sheet_id, sheet_index))
        };

        let cell_info =
            |sheet_name: &str, row: u32, col: u32| -> Result<ReferenceInfo, ExcelError> {
                let (sheet_id, sheet_index) = sheet_info(sheet_name)?;
                let row0 = row
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let col0 = col
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                Ok(ReferenceInfo {
                    first_sheet_index: Some(sheet_index),
                    sheet_count: Some(1),
                    first_cell: Some(CellRef::new(sheet_id, Coord::new(row0, col0, true, true))),
                })
            };

        let range_info = |sheet_name: &str,
                          start_row: Option<u32>,
                          start_col: Option<u32>|
         -> Result<ReferenceInfo, ExcelError> {
            let (sheet_id, sheet_index) = sheet_info(sheet_name)?;
            let row = start_row.unwrap_or(1);
            let col = start_col.unwrap_or(1);
            let row0 = row
                .checked_sub(1)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
            let col0 = col
                .checked_sub(1)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
            Ok(ReferenceInfo {
                first_sheet_index: Some(sheet_index),
                sheet_count: Some(1),
                first_cell: Some(CellRef::new(sheet_id, Coord::new(row0, col0, true, true))),
            })
        };

        let info = match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                cell_info(sheet_name, *row, *col)?
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                ..
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                range_info(sheet_name, *start_row, *start_col)?
            }
            ReferenceType::Cell3D {
                sheet_first,
                sheet_last,
                row,
                col,
                ..
            } => {
                let first = cell_info(sheet_first, *row, *col)?;
                ReferenceInfo {
                    first_sheet_index: first.first_sheet_index,
                    sheet_count: self
                        .graph
                        .sheet_reg()
                        .active_span_len(sheet_first, sheet_last),
                    first_cell: first.first_cell,
                }
            }
            ReferenceType::Range3D {
                sheet_first,
                sheet_last,
                start_row,
                start_col,
                ..
            } => {
                let first = range_info(sheet_first, *start_row, *start_col)?;
                ReferenceInfo {
                    first_sheet_index: first.first_sheet_index,
                    sheet_count: self
                        .graph
                        .sheet_reg()
                        .active_span_len(sheet_first, sheet_last),
                    first_cell: first.first_cell,
                }
            }
            ReferenceType::NamedRange(name) => {
                let current_id = self
                    .graph
                    .sheet_id(current_sheet)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let named = self
                    .graph
                    .resolve_name_entry(name, current_id)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                match &named.definition {
                    NamedDefinition::Cell(cell) => ReferenceInfo {
                        first_sheet_index: self
                            .graph
                            .sheet_reg()
                            .active_position_by_id(cell.sheet_id),
                        sheet_count: Some(1),
                        first_cell: Some(*cell),
                    },
                    NamedDefinition::Range(range) => ReferenceInfo {
                        first_sheet_index: self
                            .graph
                            .sheet_reg()
                            .active_position_by_id(range.start.sheet_id),
                        sheet_count: Some(1),
                        first_cell: Some(range.start),
                    },
                    NamedDefinition::Literal(_) | NamedDefinition::Formula { .. } => {
                        ReferenceInfo {
                            first_sheet_index: None,
                            sheet_count: None,
                            first_cell: None,
                        }
                    }
                }
            }
            ReferenceType::Table(tref) => {
                let table = self
                    .graph
                    .resolve_table_entry(&tref.name)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                ReferenceInfo {
                    first_sheet_index: self
                        .graph
                        .sheet_reg()
                        .active_position_by_id(table.range.start.sheet_id),
                    sheet_count: Some(1),
                    first_cell: Some(table.range.start),
                }
            }
            ReferenceType::External(_) => return Err(ExcelError::new(ExcelErrorKind::Ref)),
        };

        Ok(Some(info))
    }

    fn formula_text_at_cell(&self, cell: CellRef) -> Result<Option<String>, ExcelError> {
        let sheet_name = self.graph.sheet_name(cell.sheet_id);
        if sheet_name.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Ref));
        }
        let row = cell.coord.row() + 1;
        let col = cell.coord.col() + 1;

        if let Some(entries) = self.staged_formulas.get(sheet_name)
            && let Some(text) = entries.get(row, col)
        {
            return Ok(Some(if text.starts_with('=') {
                text.to_owned()
            } else {
                format!("={text}")
            }));
        }

        let Some((Some(ast), _)) = self.get_cell(sheet_name, row, col) else {
            return Ok(None);
        };
        Ok(Some(formualizer_parse::pretty::canonical_formula(&ast)))
    }

    fn used_rows_for_columns(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        // Union Arrow-backed used-region with formula rows that have not been materialized yet.
        let sheet_id = self.graph.sheet_id(sheet)?;
        let snap = self.data_snapshot_id();
        if let Some(cached) = self.used_axis_bounds_cache.read().ok().and_then(|guard| {
            guard
                .as_ref()
                .and_then(|cache| cache.get_row_bounds(sheet_id, start_col, end_col, snap))
        }) {
            return cached;
        }

        let arrow_bounds = self
            .sheet_store()
            .sheet(sheet)
            .and_then(|_| self.arrow_used_row_bounds(sheet, start_col, end_col));
        let formula_bounds = self.formula_row_bounds_for_columns(sheet, start_col, end_col);
        let computed = if let Some(bounds) = Self::union_used_bounds(arrow_bounds, formula_bounds) {
            Some(bounds)
        } else {
            let sc0 = start_col.saturating_sub(1);
            let ec0 = end_col.saturating_sub(1);
            self.graph
                .used_row_bounds_for_columns(sheet_id, sc0, ec0)
                .map(|(a0, b0)| (a0 + 1, b0 + 1))
        };

        if let Ok(mut guard) = self.used_axis_bounds_cache.write() {
            guard
                .get_or_insert_with(|| UsedAxisBoundsCache::new(snap))
                .put_row_bounds(sheet_id, start_col, end_col, snap, computed);
        }

        computed
    }

    fn used_cols_for_rows(&self, sheet: &str, start_row: u32, end_row: u32) -> Option<(u32, u32)> {
        // Union Arrow-backed used-region with formula columns that have not been materialized yet.
        let sheet_id = self.graph.sheet_id(sheet)?;
        let snap = self.data_snapshot_id();
        if let Some(cached) = self.used_axis_bounds_cache.read().ok().and_then(|guard| {
            guard
                .as_ref()
                .and_then(|cache| cache.get_col_bounds(sheet_id, start_row, end_row, snap))
        }) {
            return cached;
        }

        let arrow_bounds = self
            .sheet_store()
            .sheet(sheet)
            .and_then(|_| self.arrow_used_col_bounds(sheet, start_row, end_row));
        let formula_bounds = self.formula_col_bounds_for_rows(sheet, start_row, end_row);
        let computed = if let Some(bounds) = Self::union_used_bounds(arrow_bounds, formula_bounds) {
            Some(bounds)
        } else {
            let sr0 = start_row.saturating_sub(1);
            let er0 = end_row.saturating_sub(1);
            self.graph
                .used_col_bounds_for_rows(sheet_id, sr0, er0)
                .map(|(a0, b0)| (a0 + 1, b0 + 1))
        };

        if let Ok(mut guard) = self.used_axis_bounds_cache.write() {
            guard
                .get_or_insert_with(|| UsedAxisBoundsCache::new(snap))
                .put_col_bounds(sheet_id, start_row, end_row, snap, computed);
        }

        computed
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        let _ = self.graph.sheet_id(sheet)?;
        // Excel-like upper bounds; we expose something finite but large.
        // Backends may override with real bounds.
        Some((1_048_576, 16_384)) // 1048576 rows, 16384 cols (XFD)
    }

    fn data_snapshot_id(&self) -> u64 {
        self.snapshot_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn backend_caps(&self) -> crate::traits::BackendCaps {
        crate::traits::BackendCaps {
            streaming: true,
            used_region: true,
            write: false,
            tables: false,
            async_stream: false,
        }
    }

    fn build_lookup_index(
        &self,
        view: &RangeView<'_>,
        axis: LookupAxis,
    ) -> Option<Arc<LookupIndex>> {
        self.build_lookup_index_impl(view, axis)
    }

    // Flats removed

    fn date_system(&self) -> crate::engine::DateSystem {
        self.config.date_system
    }
    /// New: resolve a reference into a RangeView (Phase 2 API)
    fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        match reference {
            ReferenceType::External(ext) => {
                let name = ext.raw.as_str();
                match ext.kind {
                    formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                        let Some(source) = self.graph.resolve_source_scalar_entry(name) else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined name: {name}")));
                        };
                        let version = source
                            .version
                            .or_else(|| self.resolver.source_scalar_version(name));
                        let v = self.resolve_source_scalar_cached(name, version)?;
                        Ok(RangeView::from_owned_rows(
                            vec![vec![v]],
                            self.config.date_system,
                        ))
                    }
                    formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                        let Some(source) = self.graph.resolve_source_table_entry(name) else {
                            // A deferred (whole-row/column) external range
                            // evaluates to #REF!, as it did before the parser
                            // kept it whole (see `unbound_external_range_defers`).
                            let kind =
                                if crate::engine::refs::unbound_external_range_defers(&ext.kind) {
                                    ExcelErrorKind::Ref
                                } else {
                                    ExcelErrorKind::Name
                                };
                            return Err(ExcelError::new(kind)
                                .with_message(format!("Undefined table: {name}")));
                        };
                        let version = source
                            .version
                            .or_else(|| self.resolver.source_table_version(name));
                        let table = self.resolve_source_table_cached(name, version)?;
                        let spec = Some(formualizer_parse::parser::TableSpecifier::Data);
                        self.source_table_to_range_view(table.as_ref(), &spec)
                    }
                }
            }
            ReferenceType::Range { .. } => {
                let shared = self.resolve_shared_ref(reference, current_sheet)?;
                let formualizer_common::SheetRef::Range(range) = shared else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                };
                // No context sheet is available here, so an unresolved locator
                // is #REF! rather than a guess (issue #110).
                let sheet_id = match range.sheet {
                    formualizer_common::SheetLocator::Id(id) => id,
                    formualizer_common::SheetLocator::Current
                    | formualizer_common::SheetLocator::Name(_) => {
                        return Err(ExcelError::new(ExcelErrorKind::Ref));
                    }
                };
                let sheet_name = self.graph.sheet_name(sheet_id);

                let bounded_range = if range.start_row.is_some()
                    && range.start_col.is_some()
                    && range.end_row.is_some()
                    && range.end_col.is_some()
                {
                    Some(RangeRef::try_from_shared(range.as_ref())?)
                } else {
                    None
                };

                let sr = bounded_range
                    .as_ref()
                    .map(|r| r.start.coord.row() + 1)
                    .or_else(|| range.start_row.map(|b| b.index + 1));
                let sc = bounded_range
                    .as_ref()
                    .map(|r| r.start.coord.col() + 1)
                    .or_else(|| range.start_col.map(|b| b.index + 1));
                let er = bounded_range
                    .as_ref()
                    .map(|r| r.end.coord.row() + 1)
                    .or_else(|| range.end_row.map(|b| b.index + 1));
                let ec = bounded_range
                    .as_ref()
                    .map(|r| r.end.coord.col() + 1)
                    .or_else(|| range.end_col.map(|b| b.index + 1));

                let extent = resolve_used_extent_with_fallback(
                    OpenRangeBounds {
                        start_row: sr,
                        start_column: sc,
                        end_row: er,
                        end_column: ec,
                    },
                    ExtentPolicy::EvaluationCompat {
                        fallback_row: None,
                        fallback_column: None,
                    },
                    || {
                        self.sheet_bounds(sheet_name)
                            .map(|_| self.config.max_open_ended_rows)
                    },
                    || {
                        self.sheet_bounds(sheet_name)
                            .map(|_| self.config.max_open_ended_cols)
                    },
                    |first, last| self.used_rows_for_columns(sheet_name, first, last),
                    |first, last| self.used_cols_for_rows(sheet_name, first, last),
                );
                let (sr, sc, er, ec) = extent
                    .map(|extent| {
                        (
                            extent.start_row,
                            extent.start_column,
                            extent.end_row,
                            extent.end_column,
                        )
                    })
                    .unwrap_or((1, 1, 0, 0));

                if self.force_materialize_range_views {
                    if er < sr || ec < sc {
                        return Ok(RangeView::from_owned_rows(
                            Vec::new(),
                            self.config.date_system,
                        ));
                    }
                    let h = (er - sr + 1) as u64;
                    let w = (ec - sc + 1) as u64;
                    let cell_count = h.saturating_mul(w);
                    if cell_count <= self.config.spill.max_spill_cells as u64 {
                        let mut rows: Vec<Vec<LiteralValue>> = Vec::with_capacity(h as usize);
                        for r in sr..=er {
                            let mut rowv: Vec<LiteralValue> = Vec::with_capacity(w as usize);
                            for c in sc..=ec {
                                rowv.push(
                                    self.get_cell_value(sheet_name, r, c)
                                        .unwrap_or(LiteralValue::Empty),
                                );
                            }
                            rows.push(rowv);
                        }
                        return Ok(RangeView::from_owned_rows(rows, self.config.date_system));
                    }
                }

                let Some(asheet) = self.sheet_store().sheet(sheet_name) else {
                    return Ok(RangeView::from_owned_rows(
                        Vec::new(),
                        self.config.date_system,
                    ));
                };

                let rv = if er < sr || ec < sc {
                    asheet.range_view(1, 1, 0, 0)
                } else {
                    let sr0 = sr.saturating_sub(1) as usize;
                    let sc0 = sc.saturating_sub(1) as usize;
                    let er0 = er.saturating_sub(1) as usize;
                    let ec0 = ec.saturating_sub(1) as usize;
                    asheet.range_view(sr0, sc0, er0, ec0)
                };

                Ok(rv)
            }
            ReferenceType::Cell { .. } => {
                let shared = self.resolve_shared_ref(reference, current_sheet)?;
                let formualizer_common::SheetRef::Cell(cell) = shared else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                };
                let addr = CellRef::try_from_shared(cell)?;
                let sheet_id = addr.sheet_id;
                let sheet_name = self.graph.sheet_name(sheet_id);
                let row = addr.coord.row() + 1;
                let col = addr.coord.col() + 1;

                if self.force_materialize_range_views {
                    let v = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    return Ok(RangeView::from_owned_rows(
                        vec![vec![v]],
                        self.config.date_system,
                    ));
                }

                if let Some(asheet) = self.sheet_store().sheet(sheet_name) {
                    let r0 = row.saturating_sub(1) as usize;
                    let c0 = col.saturating_sub(1) as usize;
                    let rv = asheet.range_view(r0, c0, r0, c0);
                    Ok(rv)
                } else {
                    let v = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    Ok(RangeView::from_owned_rows(
                        vec![vec![v]],
                        self.config.date_system,
                    ))
                }
            }
            ReferenceType::NamedRange(name) => {
                if let Some(current_id) = self.graph.sheet_id(current_sheet)
                    && let Some(named) = self.graph.resolve_name_entry(name, current_id)
                {
                    match &named.definition {
                        NamedDefinition::Cell(cell_ref) => {
                            let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                            if self.force_materialize_range_views {
                                let v = self
                                    .get_cell_value(
                                        sheet_name,
                                        cell_ref.coord.row() + 1,
                                        cell_ref.coord.col() + 1,
                                    )
                                    .unwrap_or(LiteralValue::Empty);
                                return Ok(RangeView::from_owned_rows(
                                    vec![vec![v]],
                                    self.config.date_system,
                                ));
                            } else {
                                let asheet = self
                                    .sheet_store()
                                    .sheet(sheet_name)
                                    .expect("Arrow sheet missing for named cell");
                                let r0 = cell_ref.coord.row() as usize;
                                let c0 = cell_ref.coord.col() as usize;
                                let rv = asheet.range_view(r0, c0, r0, c0);
                                return Ok(rv);
                            }
                        }
                        NamedDefinition::Range(range_ref) => {
                            let sheet_name = self.graph.sheet_name(range_ref.start.sheet_id);
                            let sr = range_ref.start.coord.row() + 1;
                            let sc = range_ref.start.coord.col() + 1;
                            let er = range_ref.end.coord.row() + 1;
                            let ec = range_ref.end.coord.col() + 1;
                            if self.force_materialize_range_views {
                                let h = (er.saturating_sub(sr) + 1) as u64;
                                let w = (ec.saturating_sub(sc) + 1) as u64;
                                let cell_count = h.saturating_mul(w);
                                if cell_count <= self.config.spill.max_spill_cells as u64 {
                                    let mut rows: Vec<Vec<LiteralValue>> =
                                        Vec::with_capacity(h as usize);
                                    for r in sr..=er {
                                        let mut rowv: Vec<LiteralValue> =
                                            Vec::with_capacity(w as usize);
                                        for c in sc..=ec {
                                            rowv.push(
                                                self.get_cell_value(sheet_name, r, c)
                                                    .unwrap_or(LiteralValue::Empty),
                                            );
                                        }
                                        rows.push(rowv);
                                    }
                                    return Ok(RangeView::from_owned_rows(
                                        rows,
                                        self.config.date_system,
                                    ));
                                }
                            }
                            let asheet = self
                                .sheet_store()
                                .sheet(sheet_name)
                                .expect("Arrow sheet missing for named range");
                            let sr0 = range_ref.start.coord.row() as usize;
                            let sc0 = range_ref.start.coord.col() as usize;
                            let er0 = range_ref.end.coord.row() as usize;
                            let ec0 = range_ref.end.coord.col() as usize;
                            let rv = asheet.range_view(sr0, sc0, er0, ec0);
                            return Ok(rv);
                        }
                        NamedDefinition::Literal(v) => {
                            return Ok(RangeView::from_owned_rows(
                                vec![vec![v.clone()]],
                                self.config.date_system,
                            ));
                        }
                        NamedDefinition::Formula { .. } => {
                            if let Some(value) = self.graph.get_value(named.vertex) {
                                return Ok(RangeView::from_owned_rows(
                                    vec![vec![value]],
                                    self.config.date_system,
                                ));
                            }
                        }
                    }
                }

                if let Some(source) = self.graph.resolve_source_scalar_entry(name) {
                    let version = source
                        .version
                        .or_else(|| self.resolver.source_scalar_version(name));
                    let v = self.resolve_source_scalar_cached(name, version)?;
                    return Ok(RangeView::from_owned_rows(
                        vec![vec![v]],
                        self.config.date_system,
                    ));
                }

                let data = self.resolver.resolve_named_range_reference(name)?;
                Ok(RangeView::from_owned_rows(data, self.config.date_system))
            }
            ReferenceType::Table(tref) => {
                if let Some(table) = self.graph.resolve_table_entry(&tref.name) {
                    let sheet_name = self.graph.sheet_name(table.range.start.sheet_id);
                    let asheet = self
                        .sheet_store()
                        .sheet(sheet_name)
                        .expect("Arrow sheet missing for table reference");

                    let sr0 = table.range.start.coord.row() as usize;
                    let sc0 = table.range.start.coord.col() as usize;
                    let er0 = table.range.end.coord.row() as usize;
                    let ec0 = table.range.end.coord.col() as usize;

                    let has_totals = table.totals_row;
                    let has_headers = table.header_row;
                    let data_sr = if has_headers {
                        sr0.saturating_add(1)
                    } else {
                        sr0
                    };
                    let data_er = if has_totals {
                        er0.saturating_sub(1)
                    } else {
                        er0
                    };

                    let select = |sr: usize, sc: usize, er: usize, ec: usize| {
                        if sr > er || sc > ec {
                            asheet.range_view(1, 1, 0, 0)
                        } else {
                            asheet.range_view(sr, sc, er, ec)
                        }
                    };

                    let av = match &tref.specifier {
                        None => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "Table reference without specifier is unsupported".to_string(),
                            ));
                        }
                        Some(formualizer_parse::parser::TableSpecifier::Column(col)) => {
                            let Some(idx) = table.col_index(col) else {
                                return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                                    "Column refers to unknown table column".to_string(),
                                ));
                            };
                            let c0 = sc0 + idx;
                            select(data_sr, c0, data_er, c0)
                        }
                        Some(formualizer_parse::parser::TableSpecifier::ColumnRange(
                            start,
                            end,
                        )) => {
                            let Some(si) = table.col_index(start) else {
                                return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                                    "Column range refers to unknown column(s)".to_string(),
                                ));
                            };
                            let Some(ei) = table.col_index(end) else {
                                return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                                    "Column range refers to unknown column(s)".to_string(),
                                ));
                            };
                            let (mut a, mut b) = (si, ei);
                            if a > b {
                                std::mem::swap(&mut a, &mut b);
                            }
                            let c_start = sc0 + a;
                            let c_end = sc0 + b;
                            select(data_sr, c_start, data_er, c_end)
                        }
                        Some(formualizer_parse::parser::TableSpecifier::All)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::All,
                        )) => select(sr0, sc0, er0, ec0),
                        Some(formualizer_parse::parser::TableSpecifier::Data)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::Data,
                        )) => select(data_sr, sc0, data_er, ec0),
                        Some(formualizer_parse::parser::TableSpecifier::Headers)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::Headers,
                        )) => {
                            if !has_headers {
                                asheet.range_view(1, 1, 0, 0)
                            } else {
                                select(sr0, sc0, sr0, ec0)
                            }
                        }
                        Some(formualizer_parse::parser::TableSpecifier::Totals)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::Totals,
                        )) => {
                            if !has_totals {
                                asheet.range_view(1, 1, 0, 0)
                            } else {
                                select(er0, sc0, er0, ec0)
                            }
                        }
                        Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::ThisRow,
                        )) => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "@ (This Row) requires table-aware context; not yet supported"
                                    .to_string(),
                            ));
                        }
                        Some(formualizer_parse::parser::TableSpecifier::Row(_))
                        | Some(formualizer_parse::parser::TableSpecifier::Combination(_)) => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "Complex structured references not yet supported".to_string(),
                            ));
                        }
                    };

                    return Ok(av);
                }

                if let Some(source) = self.graph.resolve_source_table_entry(&tref.name) {
                    let version = source
                        .version
                        .or_else(|| self.resolver.source_table_version(&tref.name));
                    let table = self.resolve_source_table_cached(&tref.name, version)?;
                    return self.source_table_to_range_view(table.as_ref(), &tref.specifier);
                }

                // Fallback: materialize via Resolver::resolve_range_like tranche 1.
                // A table nobody defines (an unbound reference kept by the
                // BestEffort preparation policy) is `#NAME?`, the kind Strict
                // reports at preparation, not the resolver's "not implemented".
                let boxed = self
                    .resolve_range_like(&ReferenceType::Table(tref.clone()))
                    .map_err(|e| {
                        if e.kind == ExcelErrorKind::NImpl {
                            ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Unknown table: {}", tref.name))
                        } else {
                            e
                        }
                    })?;
                let owned = boxed.materialise().into_owned();
                Ok(RangeView::from_owned_rows(owned, self.config.date_system))
            }
            ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {
                Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("3D references are not yet supported".to_string()))
            }
        }
    }

    fn resolve_cell_format(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Option<crate::format::FormatId> {
        self.effective_format_id(sheet.unwrap_or(current_sheet), row, col)
    }

    fn format_class(
        &self,
        format: crate::format::FormatId,
    ) -> Option<formualizer_common::numfmt::FormatClass> {
        self.format_registry.class(format).cloned()
    }

    fn record_cell_derived_format(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
        format: Option<crate::format::FormatId>,
    ) {
        if let Some(sheet_id) = self.graph.sheet_id(sheet) {
            let cell = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
            self.record_derived_format_at(cell, format);
        }
    }

    fn resolve_cell_reference_value(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Result<LiteralValue, ExcelError> {
        let sheet_name = sheet.unwrap_or(current_sheet);
        if self.graph.sheet_id(sheet_name).is_none() {
            return Err(ExcelError::new(ExcelErrorKind::Ref));
        }
        Ok(self
            .get_cell_value(sheet_name, row, col)
            .unwrap_or(LiteralValue::Empty))
    }

    fn resolve_cell_reference_value_formatted(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Result<(LiteralValue, Option<crate::format::FormatId>), ExcelError> {
        // `resolve_cell_reference_value` + `resolve_cell_format` with one
        // sheet lookup of each kind.
        let sheet_name = sheet.unwrap_or(current_sheet);
        let Some(sheet_id) = self.graph.sheet_id(sheet_name) else {
            return Err(ExcelError::new(ExcelErrorKind::Ref));
        };
        let asheet = self.arrow_sheets.sheet(sheet_name);
        Ok(self.read_cell_formatted_in(sheet_id, asheet, row, col))
    }

    fn build_criteria_mask(
        &self,
        view: &RangeView<'_>,
        col_in_view: usize,
        pred: &crate::args::CriteriaPredicate,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        #[cfg(any(test, feature = "test-support"))]
        criteria_mask_test_hooks::note_mask(view.dims().0);
        if view.dims().1 == 0 {
            return None;
        }
        // If the view is logically open-ended but the backing sheet has no physical rows,
        // treat the mask as empty (0-len) rather than attempting to build a huge mask.
        let sheet_rows = view.sheet().nrows as usize;
        if sheet_rows == 0 || view.start_row() >= sheet_rows {
            return Some(std::sync::Arc::new(arrow_array::BooleanArray::new_null(0)));
        }
        compute_criteria_mask(view, col_in_view, pred)
    }

    fn build_row_visibility_mask(
        &self,
        view: &RangeView<'_>,
        mode: VisibilityMaskMode,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        self.build_row_visibility_mask_for_view(view, mode)
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    fn clear_spill_projection_and_mirror(
        &mut self,
        anchor_vertex: VertexId,
        delta: Option<&mut DeltaCollector>,
    ) {
        let spill_cells = self
            .graph
            .spill_cells_for_anchor(anchor_vertex)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();
        if spill_cells.is_empty() {
            return;
        }

        if let Some(delta) = delta
            && delta.mode != DeltaMode::Off
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id);
                let old = self
                    .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                    .unwrap_or(LiteralValue::Empty);
                if old != empty {
                    delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                }
            }
        }

        self.graph.clear_spill_region(anchor_vertex);
        if let Some(scope) = Self::structural_scope_from_cells(&spill_cells) {
            self.record_structural_change(scope);
        }

        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &empty,
                );
            }
        }
    }

    /// Apply the evaluation outcome for one cyclic SCC: stamp `#CIRC!` on its
    /// (optionally filtered) members via `stamp_cycle_error`.
    ///
    /// This is the single per-SCC application point used by every schedule
    /// consumer walking `Schedule::units` (pre-work for #112, where cyclic
    /// SCCs will gain runtime verdicts instead of an unconditional stamp).
    ///
    /// `dirty_filter` preserves the recalc-plan quirk: when `Some(dirty)`,
    /// only members present in the set are stamped.
    ///
    /// Returns the number of vertices stamped (0 when a filter excludes every
    /// member), so callers can keep their site-specific `cycle_errors`
    /// accounting.
    fn apply_cycle_outcome(
        &mut self,
        cycle: &[VertexId],
        mut delta: Option<&mut DeltaCollector>,
        dirty_filter: Option<&FxHashSet<VertexId>>,
    ) -> usize {
        let circ_error = LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Circular dependency detected".to_string()),
        );
        let mut stamped = 0usize;
        for &vertex_id in cycle {
            if let Some(filter) = dirty_filter
                && !filter.contains(&vertex_id)
            {
                continue;
            }
            self.stamp_cycle_error(vertex_id, &circ_error, delta.as_deref_mut());
            stamped += 1;
        }
        stamped
    }

    /// Stamp a vertex with `#CIRC!` as part of cycle handling.
    ///
    /// Unlike a bare `update_vertex_value`, this first tears down any spill the
    /// vertex previously anchored: it clears the spilled cells, releases the graph
    /// spill registry, drops any lingering region reservation, and mirrors the
    /// cleared cells into the computed overlay — the same teardown a normal scalar/
    /// error result performs (see `apply_non_array_result_from_parallel` /
    /// `clear_spill_projection_and_mirror`). Without this, a #CIRC stamp on a former
    /// spill anchor would leave stale spilled values and a reserved region behind
    /// (issue #111).
    ///
    /// When `delta` is provided, the cleared spill cells are recorded (by
    /// `clear_spill_projection_and_mirror`) and the anchor's own #CIRC change is
    /// recorded here, matching how other result paths emit deltas.
    fn stamp_cycle_error(
        &mut self,
        vertex_id: VertexId,
        circ_error: &LiteralValue,
        mut delta: Option<&mut DeltaCollector>,
    ) {
        // Tear down any previous spill projection/region before overwriting the anchor.
        if self.graph.spill_registry_has_anchor(vertex_id) {
            self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
        }
        // Drop any reservation that was never committed (defensive; normally released
        // on the prior successful commit).
        self.spill_mgr.release_owner(vertex_id);

        // Record the anchor's own #CIRC delta, like other result paths.
        if let Some(d) = delta
            && d.mode != DeltaMode::Off
            && let Some(cell) = self.graph.get_cell_ref_for_vertex(vertex_id)
        {
            let sheet_name = self.graph.sheet_name(cell.sheet_id);
            let old = self
                .read_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                .unwrap_or(LiteralValue::Empty);
            if old != *circ_error {
                d.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
            }
        }

        self.graph.update_vertex_value_ref(vertex_id, circ_error);
        self.mirror_vertex_value_to_overlay(vertex_id, circ_error);
    }

    /// Dispatch point for one `ScheduleUnit::Cycle` (RFC #112, Stage 2).
    ///
    /// * `CycleDetection::Static` — today's behavior, byte-for-byte: stamp
    ///   `#CIRC!` on the (optionally dirty-filtered) members.
    /// * `CycleDetection::Runtime` — evaluate the SCC via
    ///   [`Self::evaluate_scc_unit`]. The recalc-plan dirty quirk maps to:
    ///   no dirty member → skip the task entirely (values stand); any dirty
    ///   member → the whole SCC evaluates (an SCC cannot be partially
    ///   evaluated).
    ///
    /// Returns the number of `#CIRC!`-stamped vertices, so call sites can
    /// keep their `cycle_errors` accounting (`> 0` ⇒ count the unit).
    fn handle_cycle_unit(
        &mut self,
        cycle: &[VertexId],
        mut delta: Option<&mut DeltaCollector>,
        dirty_filter: Option<&FxHashSet<VertexId>>,
        cancel_flag: Option<&AtomicBool>,
    ) -> Result<usize, ExcelError> {
        self.resource_checkpoint(cycle.len() as u64)?;
        match self.config.cycle.detection {
            CycleDetection::Static => {
                Ok(self.apply_cycle_outcome(cycle, delta.as_deref_mut(), dirty_filter))
            }
            CycleDetection::Runtime => {
                if let Some(filter) = dirty_filter
                    && !cycle.iter().any(|v| filter.contains(v))
                {
                    return Ok(0);
                }
                // Both policies share `evaluate_scc_unit`; they differ only
                // in the settle loop's live-cycle arm (Error stamps,
                // Iterate keeps passing — RFC #113).
                self.evaluate_scc_unit(cycle, delta, cancel_flag)
            }
        }
    }

    /// Evaluate one statically-cyclic SCC under `CycleDetection::Runtime`
    /// (design doc `formualizer-stage2-scc-evaluation-design.md` §3; contract
    /// spec §3; Iterate policy arm per RFC #113).
    ///
    /// Phantom SCCs (live-acyclic) produce ordinary values under both
    /// policies; live cycles get `#CIRC!` with live-cycle-only blast radius
    /// under `CyclePolicy::Error`, or Excel-style iterative calculation
    /// (converge per spec §6 or cap at `max_iterations` passes) under
    /// `CyclePolicy::Iterate`. Runs sequentially on the
    /// coordinating thread; commits are write-through per member (no
    /// `ComputedWriteBuffer` — that buffer is scoped to layer evaluation and
    /// always flushed before a Cycle unit runs, G1), so later members' scalar
    /// *and* range reads observe earlier members' results through the overlay
    /// cascade. Deltas are recorded once per member at end of task (G11).
    ///
    /// Returns the number of vertices stamped `#CIRC!`.
    ///
    /// `pub(crate)` so tests can drive SCC shapes (e.g. name-vertex members)
    /// that ingest-time cycle rejection makes unreachable via public edits.
    pub(crate) fn evaluate_scc_unit(
        &mut self,
        cycle: &[VertexId],
        mut delta: Option<&mut DeltaCollector>,
        cancel_flag: Option<&AtomicBool>,
    ) -> Result<usize, ExcelError> {
        struct SccMember {
            vertex: VertexId,
            cell: Option<CellRef>,
        }

        let task_start = crate::instant::FzInstant::now();

        // ── 0. Member order (spec §7.13): cells ascending (sheet, row, col);
        // name vertices after, lexicographic by folded canonical name; any
        // other vertex kind (defensive — `get_evaluation_vertices` only emits
        // formula/name kinds) last by id, never evaluated.
        let mut cell_members: Vec<(VertexId, CellRef)> = Vec::new();
        let mut name_members: Vec<(VertexId, String)> = Vec::new();
        let mut other_members: Vec<VertexId> = Vec::new();
        for &v in cycle {
            match self.graph.get_vertex_kind(v) {
                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                    match self.graph.get_cell_ref(v) {
                        Some(cell) => cell_members.push((v, cell)),
                        None => other_members.push(v),
                    }
                }
                VertexKind::NamedScalar | VertexKind::NamedArray => {
                    match self.graph.name_key_for_vertex(v) {
                        Some(key) => name_members.push((v, key)),
                        None => other_members.push(v),
                    }
                }
                _ => other_members.push(v),
            }
        }
        cell_members.sort_unstable_by_key(|(_, c)| (c.sheet_id, c.coord.row(), c.coord.col()));
        name_members.sort_unstable_by(|(av, ak), (bv, bk)| ak.cmp(bk).then(av.cmp(bv)));
        other_members.sort_unstable();

        let cell_refs: Vec<CellRef> = cell_members.iter().map(|(_, c)| *c).collect();
        let name_keys: Vec<String> = name_members.iter().map(|(_, k)| k.clone()).collect();
        let mut members: Vec<SccMember> = Vec::with_capacity(cycle.len());
        for (v, c) in &cell_members {
            members.push(SccMember {
                vertex: *v,
                cell: Some(*c),
            });
        }
        for (v, _) in &name_members {
            members.push(SccMember {
                vertex: *v,
                cell: None,
            });
        }
        for v in &other_members {
            members.push(SccMember {
                vertex: *v,
                cell: None,
            });
        }
        let n = members.len();
        // Indices addressable by the collector (cells + names); `other`
        // members can be neither edge sources nor targets.
        let recordable = cell_refs.len() + name_keys.len();

        let circ_error = LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Circular dependency detected".to_string()),
        );

        // ── 0b. Spec-§4 persistence repair: structural edits clear computed
        // overlays wholesale (`clear_computed_overlay_after_row/_col`), but
        // an iterating member's committed value is cycle STATE, not a
        // recomputable cache — and in canonical mode the overlay is its ONLY
        // home. If the overlay entry vanished since the last recalc, re-seed
        // it from the end-of-recalc snapshot (`iterative_state_values`) so
        // pass-1 reads (scalar AND range, via the overlay cascade) observe
        // the persisted value instead of silently restarting at Empty→0.
        // (Found by the iterate edge corpus: inserting/deleting an unrelated
        // row reset accumulators, violating spec §4/§7.15.)
        if !self.iterative_state_values.is_empty() {
            let restore: Vec<(VertexId, LiteralValue)> = members
                .iter()
                .filter_map(|m| {
                    let cell = m.cell?;
                    let persisted = self.iterative_state_values.get(&m.vertex)?;
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let overlay = self
                        .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if matches!(overlay, LiteralValue::Empty) {
                        Some((m.vertex, persisted.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            for (vertex, value) in restore {
                self.mirror_vertex_value_to_overlay(vertex, &value);
            }
        }

        // ── 1. Pre-task value snapshot (overlay-first for cells — G3; the
        // graph value map may be evicted in value-cache-disabled mode).
        let snapshot: Vec<LiteralValue> = members
            .iter()
            .map(|m| match m.cell {
                Some(cell) => {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    self.get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty)
                }
                None => self
                    .graph
                    .get_value(m.vertex)
                    .unwrap_or(LiteralValue::Empty),
            })
            .collect();

        // ── 2. Pre-scan: spill anchors (FormulaArray) are stamped `#CIRC!`
        // with full spill teardown (spec §7.9, #115) and excluded from
        // evaluation. They stay recordable edge TARGETS (readers see `#CIRC!`
        // and propagate). Non-evaluable defensive members are excluded too.
        let mut excluded = vec![false; n];
        let mut last_value = snapshot.clone();
        let mut stamped = 0usize;
        for (i, m) in members.iter().enumerate() {
            match self.graph.get_vertex_kind(m.vertex) {
                VertexKind::FormulaArray => {
                    // Deltas for the cleared spill-region cells (non-members)
                    // can only be recorded here; the anchor's own delta is
                    // covered by the end-of-task snapshot comparison (dedup).
                    self.stamp_cycle_error(m.vertex, &circ_error, delta.as_deref_mut());
                    excluded[i] = true;
                    last_value[i] = circ_error.clone();
                    stamped += 1;
                }
                VertexKind::FormulaScalar | VertexKind::NamedScalar | VertexKind::NamedArray => {}
                _ => excluded[i] = true,
            }
        }

        let collector = LiveEdgeCollector::new_with_names(&cell_refs, &name_keys);

        // Per-member live out-edges, refreshed whenever a member re-runs.
        let mut out_edges: Vec<Vec<u32>> = vec![Vec::new(); n];
        // Position of each member in the most recent pass (-1 = did not run).
        let mut pos: Vec<i64> = vec![-1; n];
        // Whether each member's committed value changed in the most recent pass.
        let mut changed = vec![false; n];

        // Evaluate-and-commit one member; returns Ok(true) when the member was
        // stamped `#CIRC!` (array result — would-be spill anchor, spec §7.9).
        macro_rules! run_member {
            ($i:expr) => {{
                let i: usize = $i;
                let m = &members[i];
                if i < recordable {
                    collector.set_current(i as u32);
                }
                let value = {
                    let ctx = RecordingContext::new(&*self, &collector);
                    match self.evaluate_vertex_recorded(m.vertex, &ctx, &collector) {
                        Ok(v) => v,
                        Err(e) => LiteralValue::Error(e),
                    }
                };
                let is_cell_formula = m.cell.is_some();
                if is_cell_formula && matches!(value, LiteralValue::Array(_)) {
                    // A member that *would* spill inside an SCC gets the
                    // conservative §7.9 verdict. It has never spilled before
                    // (a prior spill would make it FormulaArray, pre-stamped
                    // above), so there is no projection to tear down.
                    self.stamp_cycle_error(m.vertex, &circ_error, None);
                    excluded[i] = true;
                    stamped += 1;
                    changed[i] = last_value[i] != circ_error;
                    last_value[i] = circ_error.clone();
                } else {
                    self.graph.update_vertex_value_ref(m.vertex, &value);
                    self.mirror_vertex_value_to_overlay(m.vertex, &value);
                    // §7.14 invariant (G2): a formula member must never be
                    // shadowed by a user/delta overlay entry, or iteration
                    // reads would silently diverge from committed values.
                    #[cfg(debug_assertions)]
                    if let Some(cell) = m.cell {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        debug_assert!(
                            self.read_delta_overlay_cell(
                                &sheet_name,
                                cell.coord.row() + 1,
                                cell.coord.col() + 1
                            )
                            .is_none(),
                            "user overlay must never shadow a formula SCC member ({sheet_name}!r{}c{})",
                            cell.coord.row() + 1,
                            cell.coord.col() + 1
                        );
                    }
                    changed[i] = last_value[i] != value;
                    last_value[i] = value;
                }
            }};
        }

        let check_cancel = |flag: Option<&AtomicBool>| -> Result<(), ExcelError> {
            if let Some(flag) = flag
                && flag.load(Ordering::Relaxed)
            {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled during SCC evaluation".to_string()));
            }
            Ok(())
        };

        // ── 3. Pass 1: all evaluable members in member order.
        check_cancel(cancel_flag)?;
        let mut passes = 1usize;
        {
            let mut p = 0i64;
            for i in 0..n {
                if excluded[i] {
                    continue;
                }
                run_member!(i);
                pos[i] = p;
                p += 1;
            }
        }

        // ── 4. Settle loop (design doc §3 step 4; RFC #113 policy arm).
        //
        // Acyclic classifications settle stale readers exactly (identical
        // under both policies — phantom SCCs never iterate). A witnessed
        // live cycle dispatches on policy: `Error` stamps `#CIRC!` and
        // stops; `Iterate` keeps running full passes over all members in
        // member order until converged (spec §6) or capped at
        // `max_iterations` total passes. A live cycle that only appears
        // mid-settle takes the same arm, and a cycle that dissolves
        // mid-iteration falls back to exact acyclic settling.
        //
        // Defensive acyclic budget: the acyclic settle is monotone, so more
        // than |SCC| + 2 settle passes can only be a bug; cap hits stamp the
        // remainder and set telemetry. Tracked via `settle_passes` so
        // iteration passes (legitimately many) don't consume the budget.
        let policy = self.config.cycle.policy;
        let cap = n + 2;
        let mut witnessed_cycles = 0usize;
        let mut capped = false;
        // ── Iterate-policy state ──
        let mut iterating = false;
        let mut converged = false;
        let mut exact_fixed_point = false;
        // Values committed by the last *full* pass; `None` until the first
        // iteration pass runs (pass 1 has no predecessor to compare against)
        // and reset when a settle pass runs (no cross-kind comparisons).
        let mut prev_pass: Option<Vec<LiteralValue>> = None;
        // Final-round convergence stats (overwritten per round so the values
        // reported are the ones observed at stop).
        let mut iter_max_delta = 0f64;
        let mut iter_nan_converged = 0usize;
        // Acyclic stale-reader re-eval passes (defensive budget; under pure
        // Error flow `1 + settle_passes == passes`, preserving Stage-2
        // behavior exactly).
        let mut settle_passes = 0usize;
        loop {
            // Drain this pass's recordings; members that ran replace their
            // out-edge set, members that didn't keep last-known edges.
            let drained = collector.take_edges();
            for i in 0..n {
                if pos[i] >= 0 {
                    out_edges[i].clear();
                }
            }
            for (from, to) in drained {
                debug_assert!(
                    pos[from as usize] >= 0,
                    "edge from a member that did not run"
                );
                out_edges[from as usize].push(to);
            }
            let mut edges: Vec<(u32, u32)> = Vec::new();
            for (i, outs) in out_edges.iter().enumerate() {
                if excluded[i] {
                    continue;
                }
                for &t in outs {
                    edges.push((i as u32, t));
                }
            }
            edges.sort_unstable();
            edges.dedup();

            let analysis = analyze_live_graph(n, &edges);

            if analysis.cycle_count > 0 {
                // Classification repeats every iteration pass under
                // `Iterate`; record the widest single witness instead of
                // accumulating so the count stays "distinct live cycles".
                witnessed_cycles = witnessed_cycles.max(analysis.cycle_count);
                match policy {
                    CyclePolicy::Error => {
                        // POLICY (Error): stamp every member of a live cycle,
                        // then one settling pass over the remaining members in
                        // live-topological order so error propagation
                        // downstream is consistent (spec §3.4). Blast radius =
                        // live cycles only.
                        for i in 0..n {
                            if analysis.in_cycle[i] && !excluded[i] {
                                self.stamp_cycle_error(members[i].vertex, &circ_error, None);
                                excluded[i] = true;
                                last_value[i] = circ_error.clone();
                                stamped += 1;
                            }
                        }
                        check_cancel(cancel_flag)?;
                        let order: Vec<usize> = analysis
                            .topo
                            .iter()
                            .map(|&i| i as usize)
                            .filter(|&i| !excluded[i])
                            .collect();
                        if !order.is_empty() {
                            passes += 1;
                            for i in order {
                                run_member!(i);
                            }
                        }
                        break;
                    }
                    CyclePolicy::Iterate {
                        max_iterations,
                        max_change,
                    } => {
                        // POLICY (Iterate), spec §3.5/§6.
                        iterating = true;

                        // Convergence test: the full pass that just completed
                        // vs the previous full pass, per the spec-§6 rules.
                        // `prev_pass` is `None` until an iteration pass has
                        // run — pass 1 has no predecessor, so no convergence
                        // test occurs before the second pass (spec §6).
                        if let Some(prev) = &prev_pass {
                            let mut round_max_delta = 0f64;
                            let mut round_nan = 0usize;
                            let mut all_converged = true;
                            let mut round_exact = true;
                            for i in 0..n {
                                if excluded[i] {
                                    // Stamped mid-iteration (array result,
                                    // §7.9): the value is pinned and cannot
                                    // change again — trivially settled.
                                    continue;
                                }
                                let out = crate::engine::convergence::values_converged(
                                    &prev[i],
                                    &last_value[i],
                                    max_change,
                                    self.config.date_system,
                                );
                                if out.nan_converged {
                                    round_nan += 1;
                                }
                                if let Some(d) = out.abs_delta {
                                    round_max_delta = round_max_delta.max(d);
                                    if d != 0.0 {
                                        round_exact = false;
                                    }
                                }
                                if !out.converged {
                                    all_converged = false;
                                }
                            }
                            // Overwrite (not max): telemetry reports the
                            // round observed at stop.
                            iter_max_delta = round_max_delta;
                            iter_nan_converged = round_nan;
                            if all_converged {
                                exact_fixed_point = round_exact;
                                converged = true;
                                break;
                            }
                        }

                        // ── Pass-counting reconciliation (spec §6/§7.6):
                        // `max_iterations` counts TOTAL passes, pass 1
                        // included, and pass 1 has already run by the time a
                        // live cycle is first witnessed here. The budget is
                        // therefore checked BEFORE evaluating anything more:
                        // with `max_iterations: 1` we stop right here — each
                        // member was evaluated exactly once this recalc (the
                        // Excel accumulator contract) and no convergence test
                        // ran (`prev_pass` is still `None`). Capping keeps
                        // the last committed values and is NOT an error
                        // (Excel parity); telemetry records it.
                        if passes >= max_iterations as usize {
                            capped = true;
                            break;
                        }

                        check_cancel(cancel_flag)?;
                        // One more full pass over every evaluable member in
                        // member order (Gauss–Seidel: each commit is visible
                        // to later members within the pass). Live edges
                        // re-record — guards can flip near convergence
                        // (§7.3) — so classification repeats next time
                        // around, and a cycle that dissolves drops back to
                        // the exact acyclic settle below.
                        prev_pass = Some(last_value.clone());
                        for x in pos.iter_mut() {
                            *x = -1;
                        }
                        changed.fill(false);
                        passes += 1;
                        let mut p = 0i64;
                        for i in 0..n {
                            if excluded[i] {
                                continue;
                            }
                            run_member!(i);
                            pos[i] = p;
                            p += 1;
                        }
                        continue;
                    }
                }
            }

            // Acyclic: find stale readers — members whose live read of `to`
            // happened before `to`'s value changed in the pass that just ran.
            let mut stale: Vec<usize> = Vec::new();
            for i in 0..n {
                if excluded[i] {
                    continue;
                }
                let is_stale = out_edges[i].iter().any(|&t| {
                    let t = t as usize;
                    changed[t] && (pos[i] < 0 || (pos[t] >= 0 && pos[i] < pos[t]))
                });
                if is_stale {
                    stale.push(i);
                }
            }
            if stale.is_empty() {
                break; // values exact — phantom SCC (or dissolved live cycle)
            }
            if 1 + settle_passes >= cap {
                // Defensive only; hitting this is a bug (loud telemetry).
                capped = true;
                for (i, m) in members.iter().enumerate() {
                    if !excluded[i] {
                        self.stamp_cycle_error(m.vertex, &circ_error, None);
                        excluded[i] = true;
                        last_value[i] = circ_error.clone();
                        stamped += 1;
                    }
                }
                break;
            }

            check_cancel(cancel_flag)?;
            // Re-evaluate stale readers in live-topo order, recording fresh
            // edges (branches may flip on re-eval — spec §7.3 — which is why
            // classification repeats).
            // A settle pass is a partial sweep: drop the full-pass baseline
            // so a live cycle (re)appearing afterwards never compares values
            // across mixed pass kinds.
            prev_pass = None;
            let topo_pos = analysis.topo_positions();
            stale.sort_unstable_by_key(|&i| topo_pos[i]);
            for x in pos.iter_mut() {
                *x = -1;
            }
            changed.fill(false);
            passes += 1;
            settle_passes += 1;
            for (p, i) in stale.into_iter().enumerate() {
                run_member!(i);
                pos[i] = p as i64;
            }
        }

        // Post-work boundary: members are committed write-through, so a
        // member that ran across a live cancellation is already visible; the
        // task still must not complete (no delta, retention or iteration
        // state). The members stay dirty for the retry, as with the
        // mid-task checks above.
        self.live_cancellation_after_work(SCC_CANCELLED)?;

        // Iteration that ended because the live cycle dissolved and the
        // acyclic settle reached exactness counts as converged (values are
        // exact, strictly better than threshold-converged). The defensive
        // settle cap (`capped` + stamping) is not.
        if iterating && !converged && !capped {
            converged = true;
        }

        // ── 5. End of task: one delta per member whose final value differs
        // from the pre-task snapshot (spec §3 side-effect rule, G11).
        collector.clear_current();
        if let Some(d) = delta
            && d.mode != DeltaMode::Off
        {
            for (i, m) in members.iter().enumerate() {
                if let Some(cell) = m.cell
                    && last_value[i] != snapshot[i]
                {
                    d.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                }
            }
        }

        // Members of an SCC that iterated re-evaluate on EVERY recalc, like
        // Excel's circular cells: register them for the end-of-recalc
        // volatile-like redirty (see `pending_iterative_redirty`). Marking
        // any one member propagates around the (strongly connected) SCC and
        // to downstream dependents, but all members are registered so the
        // contract survives partial structural edits between recalcs.
        //
        // Exception (#368): an SCC that stopped on an
        // exact fixed point — every member reproduced its previous value
        // bit-for-bit before the pass cap, no NaN identity, no volatile or
        // dynamic-reference member — cannot change on a re-run with the same
        // inputs, so it is retained clean. The dirty graph decides when it
        // runs again. Tolerance-only convergence (|Δ| < max_change but ≠ 0),
        // capped SCCs (including the `max_iterations: 1` accumulator
        // contract) and volatile cycles keep the per-recalc redirty.
        // Whatever the outcome, this task supersedes any earlier retention
        // of its members.
        if !self.retained_scc_members.is_empty() {
            for m in members.iter() {
                self.retained_scc_members.remove(&m.vertex);
            }
        }
        if iterating {
            let retain = converged
                && !capped
                && exact_fixed_point
                && iter_nan_converged == 0
                && members
                    .iter()
                    .all(|m| !self.graph.is_volatile(m.vertex) && !self.graph.is_dynamic(m.vertex));
            if retain {
                if self.retained_scc_members.is_empty() {
                    self.retained_scc_config_fingerprint = self.retained_scc_config_fingerprint();
                    self.retained_scc_function_epoch_seen =
                        crate::function_registry::semantic_epoch();
                    self.retained_scc_provider_revision_seen =
                        self.resolver.planning_semantic_revision();
                }
                let scc_id = self.next_retained_scc_id;
                self.next_retained_scc_id = self.next_retained_scc_id.wrapping_add(1);
                for (i, m) in members.iter().enumerate() {
                    self.retained_scc_members.insert(m.vertex, scc_id);
                    // §4 persistence snapshot, written once: retained
                    // members do not pass through `redirty_for_next_recalc`.
                    if matches!(last_value[i], LiteralValue::Empty) {
                        self.iterative_state_values.remove(&m.vertex);
                    } else {
                        self.iterative_state_values
                            .insert(m.vertex, last_value[i].clone());
                    }
                }
            } else {
                self.pending_iterative_redirty
                    .extend(members.iter().map(|m| m.vertex));
            }
        } else if !self.iterative_state_values.is_empty() {
            // The cycle dissolved (phantom settle or `#CIRC!` stamping):
            // these members are ordinary formulas again and must not carry
            // stale iteration state into a future cycle.
            for m in members.iter() {
                self.iterative_state_values.remove(&m.vertex);
            }
        }

        {
            let t = &mut self.last_cycle_telemetry;
            t.static_sccs += 1;
            if witnessed_cycles == 0 && stamped == 0 && !capped {
                t.phantom_sccs += 1;
            }
            t.live_cycles_witnessed += witnessed_cycles;
            t.circ_cells_stamped += stamped;
            t.settle_passes_total += passes;
            t.max_passes_single_scc = t.max_passes_single_scc.max(passes);
            if iterating {
                t.iterated_sccs += 1;
                if converged {
                    t.converged_sccs += 1;
                }
                t.max_abs_delta_at_stop = t.max_abs_delta_at_stop.max(iter_max_delta);
                t.nan_converged += iter_nan_converged;
            }
            if capped {
                t.capped_sccs += 1;
            }
            t.elapsed_ms += task_start.elapsed().as_millis();
        }

        Ok(stamped)
    }

    /// Recorded sibling of [`Self::evaluate_vertex_immutable`]: evaluates one
    /// SCC member's AST via an [`Interpreter`] over a [`RecordingContext`] so
    /// reads that actually occur are captured as live edges. Value semantics
    /// must match `evaluate_vertex_immutable` exactly (including the missing-
    /// AST `Number(0.0)` quirk, G14); named Cell/Range/Literal definitions
    /// delegate to it after recording the definition region by hand (those
    /// reads bypass the context).
    fn evaluate_vertex_recorded(
        &self,
        vertex_id: VertexId,
        ctx: &RecordingContext<'_, R>,
        collector: &LiveEdgeCollector,
    ) -> Result<LiteralValue, ExcelError> {
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        match kind {
            VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                let Some(view) = self.graph.formula_view(vertex_id) else {
                    return Ok(LiteralValue::Number(0.0)); // G14 quirk
                };
                let sheet_name = self.graph.sheet_name(sheet_id);
                let cell_ref = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .expect("cell ref for vertex");
                let interpreter = Interpreter::new_with_cell(ctx, sheet_name, cell_ref);
                interpreter
                    .evaluate_formula_view(view, self.graph.data_store(), self.graph.sheet_reg())
                    .map(|cv| {
                        let format = cv.format_id();
                        self.record_derived_format(vertex_id, format);
                        crate::engine::result_finalization::finalize_published_calc_result(
                            cv,
                            self.config.spill.max_spill_cells,
                        )
                    })
            }
            VertexKind::NamedScalar | VertexKind::NamedArray => {
                let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message("Named range metadata missing".to_string())
                })?;

                match &named_range.definition {
                    NamedDefinition::Formula { ast, .. } => {
                        let context_sheet = match named_range.scope {
                            NameScope::Sheet(id) => id,
                            NameScope::Workbook => sheet_id,
                        };
                        let sheet_name = self.graph.sheet_name(context_sheet);
                        let cell_ref = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                        let interpreter = Interpreter::new_with_cell(ctx, sheet_name, cell_ref);
                        if kind == VertexKind::NamedScalar {
                            interpreter.evaluate_ast(ast).map(|cv| cv.into_literal())
                        } else {
                            match interpreter.evaluate_ast(ast) {
                                Ok(cv) => match cv.into_literal() {
                                    v @ LiteralValue::Array(_) => Ok(v),
                                    other => Ok(LiteralValue::Array(vec![vec![other]])),
                                },
                                Err(err) => Ok(LiteralValue::Error(err)),
                            }
                        }
                    }
                    NamedDefinition::Cell(cell_ref) => {
                        // The definition is read via direct grid access in
                        // `evaluate_vertex_immutable`; record the live edge
                        // by hand before delegating.
                        collector.record_scalar(
                            cell_ref.sheet_id,
                            cell_ref.coord.row(),
                            cell_ref.coord.col(),
                        );
                        self.evaluate_vertex_immutable(vertex_id)
                    }
                    NamedDefinition::Range(range_ref) => {
                        if range_ref.start.sheet_id == range_ref.end.sheet_id {
                            collector.record_rect(
                                range_ref.start.sheet_id,
                                range_ref.start.coord.row(),
                                range_ref.start.coord.col(),
                                range_ref.end.coord.row(),
                                range_ref.end.coord.col(),
                            );
                        }
                        self.evaluate_vertex_immutable(vertex_id)
                    }
                    NamedDefinition::Literal(_) => self.evaluate_vertex_immutable(vertex_id),
                }
            }
            _ => self.evaluate_vertex_immutable(vertex_id),
        }
    }

    /// Pending source occupancy is independent of formula preparation and value caches.
    fn pending_spill_occupied(&self, anchor: CellRef, end_row: u32, end_col: u32) -> bool {
        let sheet = self.graph.sheet_name(anchor.sheet_id);
        let package = self
            .staged_formulas
            .get(sheet)
            .and_then(|staged| staged.deferred_package.as_ref());
        self.staged_formula_index.occupies_spill(
            sheet,
            (anchor.coord.row() + 1, anchor.coord.col() + 1),
            (end_row + 1, end_col + 1),
            |point| package.is_some_and(|package| package.suppressed.contains(&point)),
        )
    }

    fn remember_pending_spill(
        &mut self,
        vertex: VertexId,
        anchor: CellRef,
        region: Region,
    ) -> Result<(), ExcelError> {
        self.cancellation_checkpoint("pending spill occupancy")?;
        self.resource_checkpoint(1)?;
        if let Some(entry) = self
            .blocked_pending_spills
            .iter_mut()
            .find(|entry| entry.0 == vertex)
        {
            *entry = (vertex, anchor, region);
            return Ok(());
        }
        if self.blocked_pending_spills.len() == self.blocked_pending_spills.capacity() {
            // Geometric growth avoids copying every existing retry entry for
            // every new blocked anchor. Admit the entire capacity increment.
            let additional = self.blocked_pending_spills.capacity().max(1);
            let bytes =
                (additional as u64)
                    .saturating_mul(std::mem::size_of::<(VertexId, CellRef, Region)>() as u64);
            if let Some(ledger) = self.active_resource_ledger.as_mut() {
                ledger
                    .reserve_retained(bytes)
                    .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
                self.source_cache_accounted = self.source_cache_accounted.saturating_add(bytes);
            }
            if self
                .blocked_pending_spills
                .try_reserve_exact(additional)
                .is_err()
            {
                if let Some(ledger) = self.active_resource_ledger.as_mut() {
                    ledger
                        .release_retained(bytes)
                        .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
                    self.source_cache_accounted -= bytes;
                }
                return Err(crate::engine::ResourceLedgerError::Exhausted(
                    formualizer_common::ResourceExhaustionDetail {
                        reason: formualizer_common::ResourceExhaustionReason::RetainedMemory,
                        limit: u64::MAX,
                        observed: bytes,
                        request_id: None,
                    },
                )
                .into_excel_error());
            }
        }
        self.blocked_pending_spills.push((vertex, anchor, region));
        Ok(())
    }

    // Successful edits wake only intersecting attempted regions. Keep the entry
    // until evaluation: logged edits may still roll back, and materializing a
    // pending formula must not destroy its anchor's occupancy retry information.
    fn invalidate_pending_spills(&mut self, scope: StructuralScope) {
        if let StructuralScope::RemovedSheet(sheet) = scope {
            self.blocked_pending_spills
                .retain(|entry| entry.1.sheet_id != sheet);
            return;
        }
        for &(vertex, anchor, region) in &self.blocked_pending_spills {
            let affected = match scope {
                StructuralScope::Cell { sheet, row, col } => {
                    region.intersects(&Region::point(sheet, row, col))
                }
                StructuralScope::Region(changed) => region.intersects(&changed),
                StructuralScope::Sheet(sheet) | StructuralScope::RemovedSheet(sheet) => {
                    region.sheet_id() == sheet
                }
                StructuralScope::OpaqueGlobal | StructuralScope::AllSheets => true,
            };
            if affected
                && self.graph.vertex_exists(vertex)
                && self.graph.get_cell_ref(vertex) == Some(anchor)
                && matches!(
                    self.graph.get_vertex_kind(vertex),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                )
            {
                self.graph.mark_vertex_dirty(vertex);
            }
        }
    }

    fn guard_pending_spill_commit(
        &mut self,
        anchor_vertex: VertexId,
        targets: &[CellRef],
    ) -> Result<(), ExcelError> {
        let Some(anchor) = self.graph.get_cell_ref(anchor_vertex) else {
            return Ok(());
        };
        let Some(last) = targets.last() else {
            return Ok(());
        };
        let occupied = self.pending_spill_occupied(anchor, last.coord.row(), last.coord.col());
        if (occupied
            || self
                .blocked_pending_spills
                .iter()
                .any(|entry| entry.0 == anchor_vertex))
            && let Err(error) = self.remember_pending_spill(
                anchor_vertex,
                anchor,
                Region::rect(
                    anchor.sheet_id,
                    anchor.coord.row(),
                    last.coord.row(),
                    anchor.coord.col(),
                    last.coord.col(),
                ),
            )
        {
            self.spill_mgr.release_owner(anchor_vertex);
            return Err(error);
        }
        if occupied {
            self.spill_mgr.release_owner(anchor_vertex);
            return Err(ExcelError::new(ExcelErrorKind::Spill)
                .with_message("Spill blocked")
                .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                    expected_rows: last.coord.row() - anchor.coord.row() + 1,
                    expected_cols: last.coord.col() - anchor.coord.col() + 1,
                }));
        }
        Ok(())
    }

    /// Commit spill via shim and mirror resulting cells into Arrow overlay.
    fn commit_spill_and_mirror(
        &mut self,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
        delta: Option<&mut DeltaCollector>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<(), ExcelError> {
        self.guard_pending_spill_commit(anchor_vertex, targets)?;
        let prev_spill_cells = self
            .graph
            .spill_cells_for_anchor(anchor_vertex)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();

        if let Some(delta) = delta
            && delta.mode != DeltaMode::Off
        {
            let target_set: std::collections::HashSet<CellRef, CoordBuildHasher> =
                targets.iter().copied().collect();
            let empty = LiteralValue::Empty;

            // Clears (prev - targets)
            for cell in prev_spill_cells.iter() {
                if target_set.contains(cell) {
                    continue;
                }
                let sheet_name = self.graph.sheet_name(cell.sheet_id);
                let old = self
                    .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                    .unwrap_or(LiteralValue::Empty);
                if old != empty {
                    delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                }
            }

            // Writes (targets)
            if !targets.is_empty() && !rows.is_empty() && !rows[0].is_empty() {
                let width = rows[0].len();
                for (idx, cell) in targets.iter().enumerate() {
                    let r_off = idx / width;
                    let c_off = idx % width;
                    let new = rows
                        .get(r_off)
                        .and_then(|r| r.get(c_off))
                        .cloned()
                        .unwrap_or(LiteralValue::Empty);
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if old != new {
                        delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
            } else {
                // Degenerate shapes: if we have targets but no rows, treat as writing Empty.
                for cell in targets.iter() {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if !matches!(old, LiteralValue::Empty) {
                        delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
            }
        }

        // Commit via shim (releases locks). When the graph value cache is disabled (Arrow-canonical
        // values), plan/commit must consult Arrow storage to detect non-empty value blockers.
        let arrow_sheets = &self.arrow_sheets;
        self.spill_mgr.commit_array_with_value_probe(
            &mut self.graph,
            anchor_vertex,
            targets,
            rows.clone(),
            overwritable_formulas,
            |g, cell| {
                let sheet_name = g.sheet_name(cell.sheet_id);
                let asheet = arrow_sheets.sheet(sheet_name)?;
                let r0 = cell.coord.row() as usize;
                let c0 = cell.coord.col() as usize;
                let v = asheet.get_cell_value(r0, c0);
                if matches!(v, LiteralValue::Empty) {
                    None
                } else {
                    Some(v)
                }
            },
        )?;

        self.blocked_pending_spills
            .retain(|entry| entry.0 != anchor_vertex);
        if let Some(scope) = Self::structural_scope_from_cells(&prev_spill_cells) {
            self.record_structural_change(scope);
        }
        if let Some(scope) = Self::structural_scope_from_cells(targets) {
            self.record_structural_change(scope);
        }

        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            if !prev_spill_cells.is_empty() {
                let target_set: std::collections::HashSet<CellRef, CoordBuildHasher> =
                    targets.iter().copied().collect();
                let empty = LiteralValue::Empty;
                for cell in prev_spill_cells.iter() {
                    if !target_set.contains(cell) {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        self.mirror_value_to_computed_overlay(
                            &sheet_name,
                            cell.coord.row() + 1,
                            cell.coord.col() + 1,
                            &empty,
                        );
                    }
                }
            }

            for (idx, cell) in targets.iter().enumerate() {
                if rows.is_empty() || rows[0].is_empty() {
                    break;
                }
                let width = rows[0].len();
                let r_off = idx / width;
                let c_off = idx % width;
                let v = rows[r_off][c_off].clone();
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &v,
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/authority_schedule_execution.rs"]
mod authority_schedule_execution;

#[cfg(test)]
#[path = "tests/pending_spill.rs"]
mod pending_spill_tests;

#[cfg(test)]
#[path = "tests/spill_batch_abort_192.rs"]
mod spill_batch_abort_192;

// ── Effects pipeline (ticket 603) ──────────────────────────────────────────
//
// Compute → Plan → Apply separation for evaluation side-effects.

use crate::engine::effects::Effect;
use crate::engine::graph::editor::change_log::{ChangeEvent, ChangeLog, SpillSnapshot};

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Plan effects for a single vertex after its value has been computed.
    ///
    /// This reads graph state but only performs lightweight mutations
    /// (`set_kind`, `spill_mgr.reserve`) that are needed for correctness
    /// during the planning phase.  Value-changing mutations are deferred to
    /// `apply_effect`.
    pub(crate) fn plan_vertex_effects(
        &mut self,
        vertex_id: VertexId,
        computed_value: LiteralValue,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<Vec<Effect>, ExcelError> {
        // FR3: a stale dynamic reader publishes nothing; FR2: anything else
        // leaves the dirty set at its commit (design §8.2).
        if self.freshness_armed() {
            if self.freshness_drop_stale(vertex_id) {
                return Ok(Vec::new());
            }
            let effects = self.plan_vertex_effects_unrecorded(
                vertex_id,
                computed_value,
                overwritable_formulas,
            )?;
            self.freshness_mark_committed(vertex_id);
            return Ok(effects);
        }
        self.plan_vertex_effects_unrecorded(vertex_id, computed_value, overwritable_formulas)
    }

    fn plan_vertex_effects_unrecorded(
        &mut self,
        vertex_id: VertexId,
        computed_value: LiteralValue,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<Vec<Effect>, ExcelError> {
        let kind = self.graph.get_vertex_kind(vertex_id);
        let is_formula = matches!(kind, VertexKind::FormulaScalar | VertexKind::FormulaArray);

        // If this vertex's cell is currently covered by a spill from a different
        // anchor, ignore the computed result.  Formula vertices are exempt:
        // they must still evaluate so that overlapping spills produce #SPILL!.
        if !is_formula {
            if let Some(cell) = self.graph.get_cell_ref(vertex_id)
                && let Some(owner) = self.graph.spill_registry_anchor_for_cell(cell)
                && owner != vertex_id
            {
                return Ok(Vec::new());
            }
            // Non-formula vertices: store value as-is (arrays remain arrays; no spill).
            return Ok(vec![Effect::WriteCell {
                vertex_id,
                value: computed_value,
            }]);
        }

        match computed_value {
            LiteralValue::Array(rows) => {
                self.plan_array_effects(vertex_id, rows, overwritable_formulas)
            }
            other => self.plan_scalar_effects(vertex_id, other),
        }
    }

    /// Plan effects for a formula vertex that produced a scalar/error result.
    fn plan_scalar_effects(
        &mut self,
        vertex_id: VertexId,
        value: LiteralValue,
    ) -> Result<Vec<Effect>, ExcelError> {
        // Range admission substitutes the same cap error before allocating rows.
        // Preserve the owned-array rejection's kind, release and clear effects.
        if let LiteralValue::Error(error) = &value
            && error.kind == ExcelErrorKind::Spill
            && error.message.as_deref() == Some("SpillTooLarge")
            && let formualizer_common::ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } = error.extra
            && u64::from(expected_rows).saturating_mul(u64::from(expected_cols))
                > u64::from(self.config.spill.max_spill_cells)
        {
            self.graph.set_kind(vertex_id, VertexKind::FormulaArray);
            return self.plan_spill_error_effects(
                vertex_id,
                "SpillTooLarge",
                expected_rows,
                expected_cols,
            );
        }
        if !matches!(&value, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Spill) {
            self.blocked_pending_spills
                .retain(|entry| entry.0 != vertex_id);
        }
        let has_spill = self
            .graph
            .spill_cells_for_anchor(vertex_id)
            .is_some_and(|c| !c.is_empty());

        let mut effects = Vec::new();
        if has_spill {
            effects.push(Effect::SpillClear {
                anchor_vertex: vertex_id,
            });
        }
        effects.push(Effect::WriteCell { vertex_id, value });
        Ok(effects)
    }

    /// Plan effects for a formula vertex that produced an array result.
    fn plan_array_effects(
        &mut self,
        vertex_id: VertexId,
        rows: Vec<Vec<LiteralValue>>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<Vec<Effect>, ExcelError> {
        // Lightweight mutation needed for correct spill-blocking checks.
        self.graph.set_kind(vertex_id, VertexKind::FormulaArray);

        let anchor = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let sheet_id = anchor.sheet_id;
        let h = rows.len() as u32;
        let w = rows.first().map(|r| r.len()).unwrap_or(0) as u32;

        // Hard cap to avoid vertex explosion from huge dynamic arrays.
        let spill_cells = (h as u64).saturating_mul(w as u64);
        if spill_cells > self.config.spill.max_spill_cells as u64 {
            return self.plan_spill_error_effects(vertex_id, "SpillTooLarge", h, w);
        }

        // Bounds check to avoid out-of-range writes (align to AbsCoord capacity).
        const PACKED_MAX_ROW: u32 = 1_048_575;
        const PACKED_MAX_COL: u32 = 16_383;
        let end_row = anchor.coord.row().saturating_add(h).saturating_sub(1);
        let end_col = anchor.coord.col().saturating_add(w).saturating_sub(1);
        if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
            return self.plan_spill_error_effects(vertex_id, "Spill exceeds sheet bounds", h, w);
        }

        let mut targets = Vec::new();
        for r in 0..h {
            for c in 0..w {
                targets.push(self.graph.make_cell_ref_internal(
                    sheet_id,
                    anchor.coord.row() + r,
                    anchor.coord.col() + c,
                ));
            }
        }

        if h != 0 && w != 0 {
            let occupied = self.pending_spill_occupied(anchor, end_row, end_col);
            if occupied
                || self
                    .blocked_pending_spills
                    .iter()
                    .any(|entry| entry.0 == vertex_id)
            {
                self.spill_mgr.release_owner(vertex_id);
                self.remember_pending_spill(
                    vertex_id,
                    anchor,
                    Region::rect(
                        sheet_id,
                        anchor.coord.row(),
                        end_row,
                        anchor.coord.col(),
                        end_col,
                    ),
                )?;
            }
            if occupied {
                return self.plan_spill_error_effects(vertex_id, "Spill blocked", h, w);
            }
        }

        // Region lock via spill manager.
        match self.spill_mgr.reserve(
            vertex_id,
            anchor,
            SpillShape { rows: h, cols: w },
            SpillMeta {
                epoch: self.recalc_epoch,
                config: self.config.spill,
            },
        ) {
            Ok(()) => {
                // Validate spill region is available.
                if let Err(_e) = self.graph.plan_spill_region_allowing_formula_overwrite(
                    vertex_id,
                    &targets,
                    overwritable_formulas,
                ) {
                    return self.plan_spill_error_effects(vertex_id, "Spill blocked", h, w);
                }

                // Arrow-canonical mode: graph planning cannot see non-empty value blockers because
                // cell values are not cached in the dependency graph. Consult Arrow storage to
                // detect occupied cells in the target region.
                if !self.graph.value_cache_enabled() {
                    let sheet_name = self.graph.sheet_name(sheet_id);
                    if let Some(asheet) = self.sheet_store().sheet(sheet_name) {
                        for cell in targets.iter() {
                            // Allow overwriting the anchor itself.
                            if *cell == anchor {
                                continue;
                            }
                            // Allow cells already owned by a spill (plan() validated spill ownership).
                            if self.graph.spill_registry_anchor_for_cell(*cell).is_some() {
                                continue;
                            }
                            // Skip formula blockers; plan() handled them (or allowed).
                            if let Some(vid) = self.graph.get_vertex_id_for_address(cell)
                                && vid != vertex_id
                            {
                                match self.graph.get_vertex_kind(vid) {
                                    VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                                        continue;
                                    }
                                    _ => {}
                                }
                            }

                            let v = asheet.get_cell_value(
                                cell.coord.row() as usize,
                                cell.coord.col() as usize,
                            );
                            if !matches!(v, LiteralValue::Empty) {
                                return self.plan_spill_error_effects(
                                    vertex_id,
                                    "BlockedByValue",
                                    h,
                                    w,
                                );
                            }
                        }
                    }
                }

                let top_left = rows
                    .first()
                    .and_then(|r| r.first())
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);

                let mut effects = Vec::new();
                // Clear previous spill if any.
                let has_prev = self
                    .graph
                    .spill_cells_for_anchor(vertex_id)
                    .is_some_and(|c| !c.is_empty());
                if has_prev {
                    effects.push(Effect::SpillClear {
                        anchor_vertex: vertex_id,
                    });
                }
                effects.push(Effect::SpillCommit {
                    anchor_vertex: vertex_id,
                    anchor_cell: anchor,
                    target_cells: targets,
                    values: rows,
                });
                effects.push(Effect::WriteCell {
                    vertex_id,
                    value: top_left,
                });
                Ok(effects)
            }
            Err(e) => {
                let msg = e.message.unwrap_or_else(|| "Spill blocked".to_string());
                self.plan_spill_error_effects(vertex_id, &msg, h, w)
            }
        }
    }

    /// Build the effect list for a spill that failed validation.
    fn plan_spill_error_effects(
        &mut self,
        vertex_id: VertexId,
        message: &str,
        expected_rows: u32,
        expected_cols: u32,
    ) -> Result<Vec<Effect>, ExcelError> {
        self.spill_mgr.release_owner(vertex_id);
        let spill_err = ExcelError::new(ExcelErrorKind::Spill)
            .with_message(message)
            .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            });
        let spill_val = LiteralValue::Error(spill_err);

        let effects = vec![
            Effect::SpillClear {
                anchor_vertex: vertex_id,
            },
            Effect::WriteCell {
                vertex_id,
                value: spill_val,
            },
        ];
        Ok(effects)
    }

    /// Apply a single effect, performing the actual graph mutations.
    pub(crate) fn apply_effect(
        &mut self,
        effect: &Effect,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
    ) -> Result<(), ExcelError> {
        self.apply_effect_with_computed_writes(effect, delta, log, None)
    }

    fn apply_effect_with_computed_writes(
        &mut self,
        effect: &Effect,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
        computed_writes: Option<&mut ComputedWriteBuffer>,
    ) -> Result<(), ExcelError> {
        match effect {
            Effect::WriteCell { vertex_id, value } => {
                self.apply_write_cell(*vertex_id, value, delta, computed_writes)?;
            }
            Effect::SpillClear { anchor_vertex } => {
                self.apply_spill_clear(*anchor_vertex, delta, log, computed_writes)?;
            }
            Effect::SpillCommit {
                anchor_vertex,
                anchor_cell: _,
                target_cells,
                values,
            } => {
                self.apply_spill_commit(
                    *anchor_vertex,
                    target_cells,
                    values.clone(),
                    delta,
                    log,
                    computed_writes,
                )?;
            }
        }
        Ok(())
    }

    /// Apply a WriteCell effect.
    fn apply_write_cell(
        &mut self,
        vertex_id: VertexId,
        value: &LiteralValue,
        delta: Option<&mut DeltaCollector>,
        mut computed_writes: Option<&mut ComputedWriteBuffer>,
    ) -> Result<(), ExcelError> {
        if let Some(d) = delta
            && d.mode != DeltaMode::Off
        {
            if let Some(buffer) = computed_writes.as_deref_mut() {
                self.flush_computed_write_buffer(buffer)?;
            }
            if let Some(cell) = self.graph.get_cell_ref_for_vertex(vertex_id) {
                let sheet_name = self.graph.sheet_name(cell.sheet_id);
                let old = self
                    .read_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                    .unwrap_or(LiteralValue::Empty);
                if old != *value {
                    d.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                }
            }
        }
        self.graph.update_vertex_value_ref(vertex_id, value);
        self.record_vertex_value_to_overlay(vertex_id, value, computed_writes)?;
        Ok(())
    }

    /// Apply a SpillClear effect.
    fn apply_spill_clear(
        &mut self,
        anchor_vertex: VertexId,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
        computed_writes: Option<&mut ComputedWriteBuffer>,
    ) -> Result<(), ExcelError> {
        if let Some(buffer) = computed_writes {
            self.flush_computed_write_buffer(buffer)?;
        }

        let spill_cells = self
            .graph
            .spill_cells_for_anchor(anchor_vertex)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();
        if spill_cells.is_empty() {
            return Ok(());
        }

        // Snapshot for ChangeLog before clearing.
        let snapshot = if log.is_some() {
            self.snapshot_spill_for_anchor(anchor_vertex)
        } else {
            None
        };

        // Record delta for cleared cells.
        if let Some(d) = delta
            && d.mode != DeltaMode::Off
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id);
                let old = self
                    .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                    .unwrap_or(LiteralValue::Empty);
                if old != empty {
                    d.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                }
            }
        }

        self.graph.clear_spill_region(anchor_vertex);
        if let Some(scope) = Self::structural_scope_from_cells(&spill_cells) {
            self.record_structural_change(scope);
        }

        // Mirror Empty to Arrow overlay for cleared cells.
        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &empty,
                );
            }
        }

        // ChangeLog.
        if let Some(log) = log
            && let Some(old) = snapshot
        {
            log.record(ChangeEvent::SpillCleared {
                anchor: anchor_vertex,
                old,
            });
        }
        Ok(())
    }

    /// Apply a SpillCommit effect.
    fn apply_spill_commit(
        &mut self,
        anchor_vertex: VertexId,
        target_cells: &[CellRef],
        values: Vec<Vec<LiteralValue>>,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
        computed_writes: Option<&mut ComputedWriteBuffer>,
    ) -> Result<(), ExcelError> {
        self.guard_pending_spill_commit(anchor_vertex, target_cells)?;
        if let Some(buffer) = computed_writes {
            self.flush_computed_write_buffer(buffer)?;
        }

        // Snapshot for ChangeLog before commit.
        let old_snapshot = if log.is_some() {
            self.snapshot_spill_for_anchor(anchor_vertex)
        } else {
            None
        };

        // Delegate to existing commit_spill_and_mirror for delta + overlay logic.
        self.commit_spill_and_mirror(
            anchor_vertex,
            target_cells,
            values.clone(),
            delta,
            None, // overwritable_formulas already validated in plan phase
        )?;

        // ChangeLog.
        if let Some(log) = log {
            log.record(ChangeEvent::SpillCommitted {
                anchor: anchor_vertex,
                old: old_snapshot,
                new: SpillSnapshot {
                    target_cells: target_cells.to_vec(),
                    values,
                },
            });
        }
        Ok(())
    }

    /// Snapshot a spill region for ChangeLog recording.
    ///
    /// Extracted from `VertexEditor::snapshot_spill_for_anchor` to be usable
    /// without creating a `VertexEditor`.
    fn snapshot_spill_for_anchor(&self, anchor: VertexId) -> Option<SpillSnapshot> {
        let cells = self.graph.spill_cells_for_anchor(anchor)?.to_vec();
        if cells.is_empty() {
            return None;
        }

        let max = self.config.spill.max_spill_cells as usize;
        let mut cells = cells;
        if cells.len() > max {
            cells.truncate(max);
        }

        let first = *cells.first().expect("non-empty spill cells");
        let sheet_name = self.graph.sheet_name(first.sheet_id).to_string();
        let row0 = first.coord.row();
        let col0 = first.coord.col();

        let mut max_row = row0;
        let mut max_col = col0;
        let mut by_coord: FxHashMap<(u32, u32), LiteralValue> = FxHashMap::default();
        for cell in &cells {
            max_row = max_row.max(cell.coord.row());
            max_col = max_col.max(cell.coord.col());
            let v = self
                .get_cell_value(&sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                .unwrap_or(LiteralValue::Empty);
            by_coord.insert((cell.coord.row(), cell.coord.col()), v);
        }

        let rows = (max_row - row0 + 1) as usize;
        let cols = (max_col - col0 + 1) as usize;
        let mut values: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows);
        for r in 0..rows {
            let mut row: Vec<LiteralValue> = Vec::with_capacity(cols);
            for c in 0..cols {
                row.push(
                    by_coord
                        .get(&(row0 + r as u32, col0 + c as u32))
                        .cloned()
                        .unwrap_or(LiteralValue::Empty),
                );
            }
            values.push(row);
        }

        Some(SpillSnapshot {
            target_cells: cells,
            values,
        })
    }

    fn flush_before_range_dependent_vertex(
        &mut self,
        vertex_id: VertexId,
        computed_writes: &mut ComputedWriteBuffer,
    ) -> Result<(), ExcelError> {
        if self.graph.reads_compressed_range(vertex_id) {
            self.flush_computed_write_buffer(computed_writes)?;
        }
        Ok(())
    }

    fn plan_vertex_effects_with_computed_flush(
        &mut self,
        vertex_id: VertexId,
        computed_value: LiteralValue,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
        computed_writes: &mut ComputedWriteBuffer,
    ) -> Result<Vec<Effect>, ExcelError> {
        if matches!(&computed_value, LiteralValue::Array(_)) {
            self.flush_computed_write_buffer(computed_writes)?;
        }
        self.plan_vertex_effects(vertex_id, computed_value, overwritable_formulas)
    }

    // ── Layer evaluation via effects pipeline ──────────────────────────────

    fn evaluate_small_layer_direct_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
        cancel_flag: Option<&AtomicBool>,
        cancel_check_every: usize,
        cancel_message: &'static str,
    ) -> Result<usize, ExcelError> {
        let cancel = cancel_flag.map(|flag| (flag, cancel_check_every, cancel_message));
        self.evaluate_layer_units(layer, delta, log, cancel, false)
    }

    /// Sequential layer walk over its units (single cells and family runs):
    /// each unit evaluates, then its vertices' effects apply in order. With
    /// `buffered`, computed writes coalesce in a layer buffer (flushed before
    /// a unit that reads a compressed range, before array results, and at
    /// the end); otherwise they apply directly. `cancel` = (flag, check every
    /// N vertices, message).
    fn evaluate_layer_units(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
        cancel: Option<(&AtomicBool, usize, &'static str)>,
        buffered: bool,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_units_until(layer, delta, log, cancel, buffered, None)
    }

    /// [`Self::evaluate_layer_units`] that stops before the next unit once
    /// `stop_at` has passed; returns the vertices evaluated (a prefix of
    /// the layer, all committed).
    fn evaluate_layer_units_until(
        &mut self,
        layer: &super::scheduler::Layer,
        mut delta: Option<&mut DeltaCollector>,
        mut log: Option<&mut ChangeLog>,
        cancel: Option<(&AtomicBool, usize, &'static str)>,
        buffered: bool,
        stop_at: Option<crate::instant::FzInstant>,
    ) -> Result<usize, ExcelError> {
        // A chain unit: its run through the chain lift, or else cell by
        // cell in row order, each written before the next reads it.
        let mut chain_values = None;
        if layer.sequential && !layer.runs.is_empty() {
            chain_values = match layer.runs.as_slice() {
                [run] if run.start == 0 && run.len as usize == layer.vertices.len() => {
                    self.try_chain_lift(*run, &layer.vertices)
                }
                _ => None,
            };
            if chain_values.is_none() {
                let cells = super::scheduler::Layer {
                    vertices: layer.vertices.clone(),
                    runs: Vec::new(),
                    sequential: true,
                };
                return self.evaluate_layer_units_until(&cells, delta, log, cancel, false, stop_at);
            }
        }
        let chained = chain_values.is_some();
        // The chain lift computed every member: one block write, as a run.
        let buffered = buffered || chained;
        // A dynamic reader's targets are not always ordered before it (its
        // pre-probe or observed reads can miss them, e.g. after a structural
        // edit). In a buffered layer a member's dirty flag is cleared at its
        // commit but its value written at the flush: the members committed
        // since the last flush count as dirty for the reader's freshness
        // check, which then re-plans it after them.
        let track_unflushed = buffered
            && self.freshness_armed()
            && layer.vertices.iter().any(|&v| self.graph.is_dynamic(v));
        let mut committed_unit: &[VertexId] = &[];
        let mut computed_writes = ComputedWriteBuffer::default();
        let mut next_check = 0usize;
        let mut done = 0usize;
        for unit in layer_units(layer) {
            if done > 0
                && let Some(stop_at) = stop_at
                && crate::instant::FzInstant::now() >= stop_at
            {
                self.flush_computed_write_buffer(&mut computed_writes)?;
                if track_unflushed {
                    self.freshness_flushed();
                }
                return Ok(done);
            }
            if let Some((flag, every, message)) = cancel
                && every > 0
                && done >= next_check
            {
                next_check = (done / every + 1) * every;
                if flag.load(Ordering::Relaxed) {
                    if buffered {
                        self.flush_computed_write_buffer(&mut computed_writes)?;
                    }
                    return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                        .with_message(message.to_string()));
                }
            }
            if buffered && self.unit_reads_compressed_range(layer, unit) {
                self.flush_computed_write_buffer(&mut computed_writes)?;
            }
            // The previous unit's members are committed (dirty flags
            // cleared); while their values wait in the buffer, a dynamic
            // reader's read of them is stale (`freshness_dirty_reads`).
            if track_unflushed {
                if computed_writes.is_empty() {
                    self.freshness_flushed();
                } else {
                    self.freshness_note_unflushed(committed_unit);
                }
                committed_unit = unit_members(layer, unit);
            }
            let values = match (chain_values.take(), unit) {
                (Some(chain), LayerUnit::Run(run)) => {
                    if let Err(e) = self.live_cancellation_after_work(UNIT_CANCELLED) {
                        self.flush_computed_write_buffer(&mut computed_writes)?;
                        return Err(e);
                    }
                    let members =
                        &layer.vertices[run.start as usize..(run.start + run.len) as usize];
                    let delta_active = delta.as_deref().is_some_and(|d| d.mode != DeltaMode::Off);
                    match self.commit_run_numbers(
                        run,
                        members,
                        &chain,
                        delta_active,
                        Some(&mut computed_writes),
                    ) {
                        Ok(true) => {
                            done += chain.len();
                            continue;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            self.flush_computed_write_buffer(&mut computed_writes)?;
                            return Err(e);
                        }
                    }
                    members
                        .iter()
                        .copied()
                        .zip(chain.into_iter().map(LiteralValue::Number))
                        .collect()
                }
                (_, unit) => self.evaluate_unit_immutable(layer, unit),
            };
            // Post-work boundary: the unit ran while (or after) the request
            // was cancelled; it is not committed and stays dirty.
            if let Err(e) = self.live_cancellation_after_work(UNIT_CANCELLED) {
                self.flush_computed_write_buffer(&mut computed_writes)?;
                return Err(e);
            }
            done += values.len();
            if let LayerUnit::Run(run) = unit {
                let members = &layer.vertices[run.start as usize..(run.start + run.len) as usize];
                let delta_active = delta.as_deref().is_some_and(|d| d.mode != DeltaMode::Off);
                let committed = self.commit_run_scalars(
                    run,
                    members,
                    &values,
                    delta_active,
                    buffered.then_some(&mut computed_writes),
                );
                match committed {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        self.flush_computed_write_buffer(&mut computed_writes)?;
                        return Err(e);
                    }
                }
            }
            // A run unit's members were all evaluated before any commits
            // (FR5, FORM-192); a single cell is committed as it runs.
            let guarded = self.freshness_begin_batch_commit(&values);
            for (vertex_id, value) in values {
                let redirtied = self.freshness_batch_redirtied(guarded, vertex_id);
                let effects = if buffered {
                    self.plan_vertex_effects_with_computed_flush(
                        vertex_id,
                        value,
                        None,
                        &mut computed_writes,
                    )
                } else {
                    self.plan_vertex_effects(vertex_id, value, None)
                };
                let effects = match effects {
                    Ok(effects) => effects,
                    Err(e) => {
                        self.flush_computed_write_buffer(&mut computed_writes)?;
                        return Err(e);
                    }
                };
                for effect in &effects {
                    if let Err(e) = self.apply_effect_with_computed_writes(
                        effect,
                        delta.as_deref_mut(),
                        log.as_deref_mut(),
                        buffered.then_some(&mut computed_writes),
                    ) {
                        self.flush_computed_write_buffer(&mut computed_writes)?;
                        return Err(e);
                    }
                }
                self.freshness_keep_redirtied(redirtied, vertex_id);
            }
        }
        self.flush_computed_write_buffer(&mut computed_writes)?;
        if track_unflushed {
            self.freshness_flushed();
        }
        // Debug builds: every chain member equals the per-cell path, now
        // that the members above it are written.
        #[cfg(debug_assertions)]
        if chained {
            for &v in &layer.vertices {
                let cell = self.graph.get_cell_ref(v);
                let (sheet, row, col) = cell
                    .map(|c| {
                        (
                            self.graph.sheet_name(c.sheet_id).to_string(),
                            c.coord.row() + 1,
                            c.coord.col() + 1,
                        )
                    })
                    .expect("chain member cell");
                let written = self.get_cell_value(&sheet, row, col);
                let oracle = self
                    .evaluate_vertex_immutable(v)
                    .unwrap_or_else(LiteralValue::Error);
                assert!(
                    written
                        .as_ref()
                        .is_some_and(|w| same_value_bits(w, &oracle)),
                    "chain member {sheet}!R{row}C{col}: {written:?} vs per-cell {oracle:?}"
                );
            }
        }
        #[cfg(not(debug_assertions))]
        let _ = chained;
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially using the effects pipeline.
    fn evaluate_layer_sequential_effects(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        let buffered = buffer_layer_writes(layer);
        self.evaluate_layer_units(layer, None, None, None, buffered)
    }

    /// Evaluate a layer sequentially with delta collection via effects pipeline.
    fn evaluate_layer_sequential_with_delta_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        let buffered = buffer_layer_writes(layer);
        self.evaluate_layer_units(layer, Some(delta), None, None, buffered)
    }

    /// Evaluate a layer sequentially with cancellation support via effects pipeline.
    fn evaluate_layer_sequential_cancellable_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        let buffered = buffer_layer_writes(layer);
        let cancel = (cancel_flag, 256, "Evaluation cancelled within layer");
        self.evaluate_layer_units(layer, None, None, Some(cancel), buffered)
    }

    /// Evaluate a layer sequentially with more frequent cancellation for demand-driven eval.
    fn evaluate_layer_sequential_cancellable_demand_driven_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        let buffered = buffer_layer_writes(layer);
        let cancel = (
            cancel_flag,
            128,
            "Demand-driven evaluation cancelled within layer",
        );
        self.evaluate_layer_units(layer, None, None, Some(cancel), buffered)
    }

    /// Evaluate a layer in parallel, applying via effects pipeline.
    fn evaluate_layer_parallel_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        min_chunk: u32,
    ) -> Result<usize, ExcelError> {
        let thread_pool = self.thread_pool.as_ref().unwrap().clone();

        let phases = self.parallel_phases(layer);

        let inflight: rustc_hash::FxHashSet<VertexId> = layer.vertices.iter().copied().collect();
        let mut applied = 0usize;

        for (units, group) in &phases {
            let group = &group[..];
            if group.is_empty() {
                continue;
            }
            let mut computed_writes = ComputedWriteBuffer::default();

            let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
                thread_pool.install(|| self.evaluate_units_parallel(layer, units, None, min_chunk));

            // Post-work boundary: a group evaluated across a live
            // cancellation is not committed (it stays dirty).
            let results = results.and_then(|results| {
                self.live_cancellation_after_work(GROUP_CANCELLED)
                    .map(|()| results)
            });
            // FR3: a parallel group is one commit unit; one stale reader
            // drops the whole group (it stays dirty and replans).
            self.freshness_gate_group(group);
            match results {
                Ok(vertex_results) => {
                    let (vertex_results, committed) = self.commit_parallel_runs(
                        layer,
                        units,
                        vertex_results,
                        false,
                        &mut computed_writes,
                    )?;
                    applied = applied.saturating_add(committed);
                    // Arrays first, then scalars — establishes spill regions before
                    // scalar results that might land inside a spilled region.
                    // FR5 (FORM-192): members evaluated together must not
                    // clear a re-dirty from a spill committed before them.
                    let guarded = self.freshness_begin_batch_commit(&vertex_results);
                    let mut arrays: Vec<(VertexId, LiteralValue)> = Vec::new();
                    let mut others: Vec<(VertexId, LiteralValue)> = Vec::new();
                    for (vertex_id, result) in vertex_results {
                        if matches!(result, LiteralValue::Array(_)) {
                            arrays.push((vertex_id, result));
                        } else {
                            others.push((vertex_id, result));
                        }
                    }
                    for (vertex_id, result) in arrays {
                        let redirtied = self.freshness_batch_redirtied(guarded, vertex_id);
                        let effects = match self.plan_vertex_effects_with_computed_flush(
                            vertex_id,
                            result,
                            Some(&inflight),
                            &mut computed_writes,
                        ) {
                            Ok(effects) => effects,
                            Err(e) => {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        };
                        for effect in &effects {
                            if let Err(e) = self.apply_effect_with_computed_writes(
                                effect,
                                None,
                                None,
                                Some(&mut computed_writes),
                            ) {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        }
                        self.freshness_keep_redirtied(redirtied, vertex_id);
                        applied = applied.saturating_add(1);
                    }
                    // Make all array spill/top-left writes visible before scalar effects in this group.
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                    for (vertex_id, result) in others {
                        let redirtied = self.freshness_batch_redirtied(guarded, vertex_id);
                        let effects = match self.plan_vertex_effects_with_computed_flush(
                            vertex_id,
                            result,
                            Some(&inflight),
                            &mut computed_writes,
                        ) {
                            Ok(effects) => effects,
                            Err(e) => {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        };
                        for effect in &effects {
                            if let Err(e) = self.apply_effect_with_computed_writes(
                                effect,
                                None,
                                None,
                                Some(&mut computed_writes),
                            ) {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        }
                        self.freshness_keep_redirtied(redirtied, vertex_id);
                        applied = applied.saturating_add(1);
                    }
                    // Flush at the group boundary; phase1 must be visible before phase2.
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                }
                Err(e) => {
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                    return Err(e);
                }
            }
        }

        Ok(applied)
    }

    /// Evaluate a layer in parallel with delta collection via effects pipeline.
    fn evaluate_layer_parallel_with_delta_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        let thread_pool = self.thread_pool.as_ref().unwrap().clone();

        let phases = self.parallel_phases(layer);

        let inflight: rustc_hash::FxHashSet<VertexId> = layer.vertices.iter().copied().collect();
        let mut applied = 0usize;

        for (units, group) in &phases {
            let group = &group[..];
            if group.is_empty() {
                continue;
            }
            let mut computed_writes = ComputedWriteBuffer::default();
            let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
                thread_pool.install(|| self.evaluate_units_parallel(layer, units, None, 8));

            // Post-work boundary: a group evaluated across a live
            // cancellation is not committed (it stays dirty).
            let results = results.and_then(|results| {
                self.live_cancellation_after_work(GROUP_CANCELLED)
                    .map(|()| results)
            });
            // FR3: a parallel group is one commit unit; one stale reader
            // drops the whole group (it stays dirty and replans).
            self.freshness_gate_group(group);
            match results {
                Ok(vertex_results) => {
                    let (vertex_results, committed) = self.commit_parallel_runs(
                        layer,
                        units,
                        vertex_results,
                        delta.mode != DeltaMode::Off,
                        &mut computed_writes,
                    )?;
                    applied = applied.saturating_add(committed);
                    // FR5 (FORM-192): members evaluated together must not
                    // clear a re-dirty from a spill committed before them.
                    let guarded = self.freshness_begin_batch_commit(&vertex_results);
                    let mut arrays: Vec<(VertexId, LiteralValue)> = Vec::new();
                    let mut others: Vec<(VertexId, LiteralValue)> = Vec::new();
                    for (vertex_id, result) in vertex_results {
                        if matches!(result, LiteralValue::Array(_)) {
                            arrays.push((vertex_id, result));
                        } else {
                            others.push((vertex_id, result));
                        }
                    }
                    for (vertex_id, result) in arrays {
                        let redirtied = self.freshness_batch_redirtied(guarded, vertex_id);
                        let effects = match self.plan_vertex_effects_with_computed_flush(
                            vertex_id,
                            result,
                            Some(&inflight),
                            &mut computed_writes,
                        ) {
                            Ok(effects) => effects,
                            Err(e) => {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        };
                        for effect in &effects {
                            if let Err(e) = self.apply_effect_with_computed_writes(
                                effect,
                                Some(delta),
                                None,
                                Some(&mut computed_writes),
                            ) {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        }
                        self.freshness_keep_redirtied(redirtied, vertex_id);
                        applied = applied.saturating_add(1);
                    }
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                    for (vertex_id, result) in others {
                        let redirtied = self.freshness_batch_redirtied(guarded, vertex_id);
                        let effects = match self.plan_vertex_effects_with_computed_flush(
                            vertex_id,
                            result,
                            Some(&inflight),
                            &mut computed_writes,
                        ) {
                            Ok(effects) => effects,
                            Err(e) => {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        };
                        for effect in &effects {
                            if let Err(e) = self.apply_effect_with_computed_writes(
                                effect,
                                Some(delta),
                                None,
                                Some(&mut computed_writes),
                            ) {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        }
                        self.freshness_keep_redirtied(redirtied, vertex_id);
                        applied = applied.saturating_add(1);
                    }
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                }
                Err(e) => {
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                    return Err(e);
                }
            }
        }

        Ok(applied)
    }

    /// Evaluate a layer in parallel with cancellation support via effects pipeline.
    fn evaluate_layer_parallel_cancellable_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        let thread_pool = self.thread_pool.as_ref().unwrap().clone();

        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Parallel evaluation cancelled before starting".to_string()));
        }

        let phases = self.parallel_phases(layer);

        let inflight: rustc_hash::FxHashSet<VertexId> = layer.vertices.iter().copied().collect();
        let mut applied = 0usize;

        for (units, group) in &phases {
            let group = &group[..];
            if group.is_empty() {
                continue;
            }
            let mut computed_writes = ComputedWriteBuffer::default();

            let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> = thread_pool
                .install(|| self.evaluate_units_parallel(layer, units, Some(cancel_flag), 8));

            // Post-work boundary: a group evaluated across a live
            // cancellation is not committed (it stays dirty).
            let results = results.and_then(|results| {
                self.live_cancellation_after_work(GROUP_CANCELLED)
                    .map(|()| results)
            });
            // FR3: a parallel group is one commit unit; one stale reader
            // drops the whole group (it stays dirty and replans).
            self.freshness_gate_group(group);
            match results {
                Ok(vertex_results) => {
                    let (vertex_results, committed) = self.commit_parallel_runs(
                        layer,
                        units,
                        vertex_results,
                        false,
                        &mut computed_writes,
                    )?;
                    applied = applied.saturating_add(committed);
                    // FR5 (FORM-192): members evaluated together must not
                    // clear a re-dirty from a spill committed before them.
                    let guarded = self.freshness_begin_batch_commit(&vertex_results);
                    let mut arrays: Vec<(VertexId, LiteralValue)> = Vec::new();
                    let mut others: Vec<(VertexId, LiteralValue)> = Vec::new();
                    for (vertex_id, result) in vertex_results {
                        if matches!(result, LiteralValue::Array(_)) {
                            arrays.push((vertex_id, result));
                        } else {
                            others.push((vertex_id, result));
                        }
                    }
                    for (vertex_id, result) in arrays {
                        let redirtied = self.freshness_batch_redirtied(guarded, vertex_id);
                        let effects = match self.plan_vertex_effects_with_computed_flush(
                            vertex_id,
                            result,
                            Some(&inflight),
                            &mut computed_writes,
                        ) {
                            Ok(effects) => effects,
                            Err(e) => {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        };
                        for effect in &effects {
                            if let Err(e) = self.apply_effect_with_computed_writes(
                                effect,
                                None,
                                None,
                                Some(&mut computed_writes),
                            ) {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        }
                        self.freshness_keep_redirtied(redirtied, vertex_id);
                        applied = applied.saturating_add(1);
                    }
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                    for (vertex_id, result) in others {
                        let redirtied = self.freshness_batch_redirtied(guarded, vertex_id);
                        let effects = match self.plan_vertex_effects_with_computed_flush(
                            vertex_id,
                            result,
                            Some(&inflight),
                            &mut computed_writes,
                        ) {
                            Ok(effects) => effects,
                            Err(e) => {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        };
                        for effect in &effects {
                            if let Err(e) = self.apply_effect_with_computed_writes(
                                effect,
                                None,
                                None,
                                Some(&mut computed_writes),
                            ) {
                                self.flush_computed_write_buffer(&mut computed_writes)?;
                                return Err(e);
                            }
                        }
                        self.freshness_keep_redirtied(redirtied, vertex_id);
                        applied = applied.saturating_add(1);
                    }
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                }
                Err(e) => {
                    self.flush_computed_write_buffer(&mut computed_writes)?;
                    return Err(e);
                }
            }
        }

        Ok(applied)
    }

    // ── Top-level evaluate_all_logged ───────────────────────────────────────

    /// Evaluate all dirty/volatile vertices, recording effects into a ChangeLog.
    ///
    /// This is the same flow as `evaluate_all` but threads a ChangeLog through
    /// every effect application so that spill commits/clears are captured.
    pub fn evaluate_all_logged(&mut self, log: &mut ChangeLog) -> Result<EvalResult, ExcelError> {
        self.observe_evaluation_resource_request(EvaluationRequestKind::FullLogged, |engine| {
            engine.evaluate_all_logged_unobserved(log)
        })
    }

    fn evaluate_all_logged_unobserved(
        &mut self,
        log: &mut ChangeLog,
    ) -> Result<EvalResult, ExcelError> {
        self.observe_function_semantic_epoch()?;
        let _source_cache = self.source_cache_session();
        self.validate_deterministic_mode()?;
        if self.config.defer_graph_building {
            self.build_graph_all()?;
        }
        self.require_unified_authority()?;
        self.begin_evaluation_request();
        self.reset_virtual_dep_telemetry_if_disabled();
        let start = crate::instant::FzInstant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let mut replan_iterations = 0;
        const MAX_REPLAN: usize = 5;
        let mut telemetry = self
            .config
            .enable_virtual_dep_telemetry
            .then(|| self.start_virtual_dep_telemetry());

        log.begin_compound(format!("evaluate_all(epoch={})", self.recalc_epoch));

        let result = (|| -> Result<EvalResult, ExcelError> {
            loop {
                let to_evaluate = self.graph.get_evaluation_vertices();
                if to_evaluate.is_empty() {
                    if let Some(t) = telemetry.as_mut()
                        && t.bailout_reason.is_none()
                    {
                        t.bailout_reason = Some("no_work");
                    }
                    break;
                }

                let (schedule, old_vdeps, meta) = self.create_evaluation_schedule(&to_evaluate)?;
                if let Some(t) = telemetry.as_mut() {
                    Self::accumulate_schedule_meta(t, &meta);
                }

                // Walk units in condensation order: stamp cycles at their
                // position, evaluate layers with ChangeLog recording.
                self.begin_pass(&schedule);
                for (unit_index, &unit) in schedule.units.iter().enumerate() {
                    match unit {
                        ScheduleUnit::Cycle(i) => {
                            // Journal integration (design doc §4 last row): the
                            // ChangeLog in this path only records SpillClear /
                            // SpillCommit events; WriteCell effects are never
                            // logged (see `apply_write_cell`). Runtime SCC tasks
                            // write values directly and never spill (§7.9 stamps
                            // would-be anchors), and their spill *teardown* is the
                            // same unlogged `stamp_cycle_error` the Static path
                            // already uses here — so direct commits coexist with
                            // the journal cleanly, with identical semantics to
                            // Static. Pinned by `scc_runtime_cycles` tests.
                            if self.handle_cycle_unit(schedule.unit_cycle(i), None, None, None)? > 0
                            {
                                cycle_errors += 1;
                            }
                        }
                        ScheduleUnit::Layer(i) => {
                            computed_vertices +=
                                self.evaluate_layer_logged(schedule.unit_layer(i), log)?;
                        }
                    }
                    if self.stop_after_unit(&schedule, unit_index) {
                        break;
                    }
                }

                let changed_vertices = self.changed_virtual_dep_vertices(&to_evaluate, &old_vdeps);
                if let Some(t) = telemetry.as_mut() {
                    t.changed_vdeps_total += changed_vertices.len();
                }
                self.resource_checkpoint(0)?;
                if !self.finish_pass_dirty(&to_evaluate, &changed_vertices) {
                    if let Some(t) = telemetry.as_mut() {
                        t.bailout_reason = Some("converged");
                    }
                    break;
                }
                if replan_iterations >= MAX_REPLAN {
                    if let Some(mut t) = telemetry.take() {
                        t.bailout_reason = Some("max_replan");
                        t.replan_iterations = replan_iterations;
                        self.last_virtual_dep_telemetry = t;
                    }
                    return Err(
                        self.replan_exhausted_error(MAX_REPLAN, "dynamic dependency evaluation")
                    );
                }
                replan_iterations += 1;
            }

            if let Some(mut t) = telemetry {
                t.replan_iterations = replan_iterations;
                self.last_virtual_dep_telemetry = t;
            }

            self.redirty_for_next_recalc();
            self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

            Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            })
        })();
        log.end_compound();
        result
    }

    /// Evaluate a single layer with ChangeLog recording.
    fn evaluate_layer_logged(
        &mut self,
        layer: &super::scheduler::Layer,
        log: &mut ChangeLog,
    ) -> Result<usize, ExcelError> {
        self.resource_checkpoint(layer.vertices.len() as u64)?;
        self.evaluate_layer_units(layer, None, Some(log), None, true)
    }
}
