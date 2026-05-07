//! Internal FormulaPlane runtime store vocabulary for FP6.1.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use formualizer_common::LiteralValue;

use crate::SheetId;
use crate::engine::VertexId;
use crate::engine::arena::AstNodeId;

use super::ids::FormulaTemplateId;
use super::producer::{SpanReadSummary, SpanReadSummaryId, SpanReadSummaryStore};
use super::template_canonical::{
    CanonicalReference, LiteralSlotDescriptor, LiteralSlotId, SlotContext,
};

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FormulaPlaneEpoch(pub(crate) u64);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FormulaSpanId(pub(crate) u32);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FormulaOverlayEntryId(pub(crate) u32);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SpanMaskId(pub(crate) u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaSpanRef {
    pub(crate) id: FormulaSpanId,
    pub(crate) generation: u32,
    pub(crate) version: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaOverlayRef {
    pub(crate) id: FormulaOverlayEntryId,
    pub(crate) generation: u32,
    pub(crate) overlay_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PlacementCoord {
    pub(crate) sheet_id: SheetId,
    pub(crate) row: u32,
    pub(crate) col: u32,
}

impl PlacementCoord {
    pub(crate) fn new(sheet_id: SheetId, row: u32, col: u32) -> Self {
        Self { sheet_id, row, col }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PlacementDomain {
    RowRun {
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col: u32,
    },
    ColRun {
        sheet_id: SheetId,
        row: u32,
        col_start: u32,
        col_end: u32,
    },
    Rect {
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
    },
}

impl PlacementDomain {
    pub(crate) fn row_run(sheet_id: SheetId, row_start: u32, row_end: u32, col: u32) -> Self {
        Self::RowRun {
            sheet_id,
            row_start,
            row_end,
            col,
        }
    }

    pub(crate) fn col_run(sheet_id: SheetId, row: u32, col_start: u32, col_end: u32) -> Self {
        Self::ColRun {
            sheet_id,
            row,
            col_start,
            col_end,
        }
    }

    pub(crate) fn rect(
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
    ) -> Self {
        Self::Rect {
            sheet_id,
            row_start,
            row_end,
            col_start,
            col_end,
        }
    }

    pub(crate) fn sheet_id(&self) -> SheetId {
        match self {
            Self::RowRun { sheet_id, .. }
            | Self::ColRun { sheet_id, .. }
            | Self::Rect { sheet_id, .. } => *sheet_id,
        }
    }

    pub(crate) fn cell_count(&self) -> u64 {
        match self {
            Self::RowRun {
                row_start, row_end, ..
            } => (*row_end - *row_start + 1) as u64,
            Self::ColRun {
                col_start, col_end, ..
            } => (*col_end - *col_start + 1) as u64,
            Self::Rect {
                row_start,
                row_end,
                col_start,
                col_end,
                ..
            } => (*row_end - *row_start + 1) as u64 * (*col_end - *col_start + 1) as u64,
        }
    }

    pub(crate) fn contains(&self, coord: PlacementCoord) -> bool {
        if self.sheet_id() != coord.sheet_id {
            return false;
        }
        match self {
            Self::RowRun {
                row_start,
                row_end,
                col,
                ..
            } => coord.col == *col && coord.row >= *row_start && coord.row <= *row_end,
            Self::ColRun {
                row,
                col_start,
                col_end,
                ..
            } => coord.row == *row && coord.col >= *col_start && coord.col <= *col_end,
            Self::Rect {
                row_start,
                row_end,
                col_start,
                col_end,
                ..
            } => {
                coord.row >= *row_start
                    && coord.row <= *row_end
                    && coord.col >= *col_start
                    && coord.col <= *col_end
            }
        }
    }

    pub(crate) fn ordinal_of(&self, placement: PlacementCoord) -> Option<usize> {
        if self.sheet_id() != placement.sheet_id {
            return None;
        }
        match self {
            Self::RowRun {
                row_start,
                row_end,
                col,
                ..
            } => {
                if placement.col == *col && placement.row >= *row_start && placement.row <= *row_end
                {
                    Some((placement.row - *row_start) as usize)
                } else {
                    None
                }
            }
            Self::ColRun {
                row,
                col_start,
                col_end,
                ..
            } => {
                if placement.row == *row && placement.col >= *col_start && placement.col <= *col_end
                {
                    Some((placement.col - *col_start) as usize)
                } else {
                    None
                }
            }
            Self::Rect {
                row_start,
                row_end,
                col_start,
                col_end,
                ..
            } => {
                if placement.row >= *row_start
                    && placement.row <= *row_end
                    && placement.col >= *col_start
                    && placement.col <= *col_end
                {
                    let width = *col_end - *col_start + 1;
                    Some(
                        ((placement.row - *row_start) * width + (placement.col - *col_start))
                            as usize,
                    )
                } else {
                    None
                }
            }
        }
    }

    pub(crate) fn iter(&self) -> std::vec::IntoIter<PlacementCoord> {
        let mut coords = Vec::new();
        match self {
            Self::RowRun {
                sheet_id,
                row_start,
                row_end,
                col,
            } => {
                for row in *row_start..=*row_end {
                    coords.push(PlacementCoord::new(*sheet_id, row, *col));
                }
            }
            Self::ColRun {
                sheet_id,
                row,
                col_start,
                col_end,
            } => {
                for col in *col_start..=*col_end {
                    coords.push(PlacementCoord::new(*sheet_id, *row, col));
                }
            }
            Self::Rect {
                sheet_id,
                row_start,
                row_end,
                col_start,
                col_end,
            } => {
                for row in *row_start..=*row_end {
                    for col in *col_start..=*col_end {
                        coords.push(PlacementCoord::new(*sheet_id, row, col));
                    }
                }
            }
        }
        coords.into_iter()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResultRegion {
    domain: PlacementDomain,
}

impl ResultRegion {
    pub(crate) fn scalar_cells(domain: PlacementDomain) -> Self {
        Self { domain }
    }

    pub(crate) fn domain(&self) -> &PlacementDomain {
        &self.domain
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SpanBindingSetId(pub(crate) u32);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ValueRefSlotId(pub(crate) u16);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ValueRefSlotDescriptor {
    pub(crate) slot_id: ValueRefSlotId,
    pub(crate) preorder_index: u32,
    pub(crate) context: SlotContext,
    pub(crate) reference_pattern: CanonicalReference,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TemplateSlotMap {
    pub(crate) literal_slots_by_arena_node: FxHashMap<AstNodeId, LiteralSlotId>,
    pub(crate) residual_relative_row: bool,
    pub(crate) residual_relative_col: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct SpanBindingSet {
    pub(crate) span_ref: FormulaSpanRef,
    pub(crate) literal_slots: Arc<[LiteralSlotDescriptor]>,
    pub(crate) unique_literal_bindings: Vec<Box<[LiteralValue]>>,
    pub(crate) placement_literal_binding_ids: Box<[u32]>,
    pub(crate) value_ref_slots: Arc<[ValueRefSlotDescriptor]>,
    pub(crate) template_slot_map: TemplateSlotMap,
}

#[derive(Debug, Default)]
pub(crate) struct BindingStore {
    records: Vec<Option<SpanBindingSet>>,
    epoch: u64,
}

impl BindingStore {
    pub(crate) fn insert(&mut self, set: SpanBindingSet) -> SpanBindingSetId {
        let id = SpanBindingSetId(self.records.len() as u32);
        self.records.push(Some(set));
        self.epoch = self.epoch.saturating_add(1);
        id
    }

    pub(crate) fn get(&self, id: SpanBindingSetId) -> Option<&SpanBindingSet> {
        self.records.get(id.0 as usize)?.as_ref()
    }

    pub(crate) fn remove(&mut self, id: SpanBindingSetId) -> bool {
        let Some(slot) = self.records.get_mut(id.0 as usize) else {
            return false;
        };
        let removed = slot.take().is_some();
        if removed {
            self.epoch = self.epoch.saturating_add(1);
        }
        removed
    }

    pub(crate) fn set_span_ref(&mut self, id: SpanBindingSetId, span_ref: FormulaSpanRef) {
        if let Some(Some(set)) = self.records.get_mut(id.0 as usize) {
            set.span_ref = span_ref;
            self.epoch = self.epoch.saturating_add(1);
        }
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    #[cfg(test)]
    pub(crate) fn unique_vector_count(&self, id: SpanBindingSetId) -> Option<usize> {
        self.get(id).map(|set| set.unique_literal_bindings.len())
    }
}

#[derive(Debug, Default)]
pub(crate) struct TemplateStore {
    records: Vec<TemplateRecord>,
    intern: FxHashMap<Arc<str>, FormulaTemplateId>,
    epoch: u64,
}

#[derive(Debug)]
pub(crate) struct TemplateRecord {
    pub(crate) id: FormulaTemplateId,
    pub(crate) generation: u32,
    pub(crate) version: u32,
    pub(crate) ast_id: AstNodeId,
    pub(crate) origin_row: u32,
    pub(crate) origin_col: u32,
    pub(crate) exact_canonical_key: Arc<str>,
    pub(crate) parameterized_canonical_key: Arc<str>,
    pub(crate) formula_text: Option<Arc<str>>,
}

impl TemplateStore {
    pub(crate) fn intern_template(
        &mut self,
        canonical_key: Arc<str>,
        ast_id: AstNodeId,
        origin_row: u32,
        origin_col: u32,
        formula_text: Option<Arc<str>>,
    ) -> (FormulaTemplateId, bool) {
        self.intern_template_parameterized(
            Arc::clone(&canonical_key),
            canonical_key,
            ast_id,
            origin_row,
            origin_col,
            formula_text,
        )
    }

    pub(crate) fn intern_template_parameterized(
        &mut self,
        exact_canonical_key: Arc<str>,
        parameterized_canonical_key: Arc<str>,
        ast_id: AstNodeId,
        origin_row: u32,
        origin_col: u32,
        formula_text: Option<Arc<str>>,
    ) -> (FormulaTemplateId, bool) {
        let intern_key = Arc::<str>::from(format!(
            "{}|origin_col={origin_col}",
            parameterized_canonical_key
        ));
        if let Some(id) = self.intern.get(intern_key.as_ref()).copied() {
            return (id, false);
        }

        let id = FormulaTemplateId(self.records.len() as u32);
        self.records.push(TemplateRecord {
            id,
            generation: 0,
            version: 0,
            ast_id,
            origin_row,
            origin_col,
            exact_canonical_key,
            parameterized_canonical_key: Arc::clone(&parameterized_canonical_key),
            formula_text,
        });
        self.intern.insert(intern_key, id);
        self.epoch = self.epoch.saturating_add(1);
        (id, true)
    }

    pub(crate) fn get(&self, id: FormulaTemplateId) -> Option<&TemplateRecord> {
        self.records
            .get(id.0 as usize)
            .filter(|record| record.id == id)
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpanState {
    Active,
    Demoted,
}

#[derive(Clone, Debug)]
pub(crate) struct FormulaSpan {
    pub(crate) id: FormulaSpanId,
    pub(crate) generation: u32,
    pub(crate) sheet_id: SheetId,
    pub(crate) template_id: FormulaTemplateId,
    pub(crate) domain: PlacementDomain,
    pub(crate) result_region: ResultRegion,
    pub(crate) intrinsic_mask_id: Option<SpanMaskId>,
    pub(crate) read_summary_id: Option<SpanReadSummaryId>,
    pub(crate) binding_set_id: Option<SpanBindingSetId>,
    pub(crate) is_constant_result: bool,
    pub(crate) state: SpanState,
    pub(crate) version: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct NewFormulaSpan {
    pub(crate) sheet_id: SheetId,
    pub(crate) template_id: FormulaTemplateId,
    pub(crate) domain: PlacementDomain,
    pub(crate) result_region: ResultRegion,
    pub(crate) intrinsic_mask_id: Option<SpanMaskId>,
    pub(crate) read_summary_id: Option<SpanReadSummaryId>,
    pub(crate) binding_set_id: Option<SpanBindingSetId>,
    pub(crate) is_constant_result: bool,
}

#[derive(Debug)]
struct SpanSlot {
    generation: u32,
    span: Option<FormulaSpan>,
}

#[derive(Debug, Default)]
pub(crate) struct SpanStore {
    slots: Vec<SpanSlot>,
    epoch: u64,
}

impl SpanStore {
    pub(crate) fn insert(&mut self, new_span: NewFormulaSpan) -> FormulaSpanRef {
        assert_eq!(
            new_span.sheet_id,
            new_span.domain.sheet_id(),
            "FormulaSpan sheet_id must match placement domain sheet_id"
        );
        assert_eq!(
            new_span.sheet_id,
            new_span.result_region.domain().sheet_id(),
            "FormulaSpan sheet_id must match result region sheet_id"
        );

        let id = FormulaSpanId(self.slots.len() as u32);
        let generation = 0;
        let version = 0;
        let span = FormulaSpan {
            id,
            generation,
            sheet_id: new_span.sheet_id,
            template_id: new_span.template_id,
            domain: new_span.domain,
            result_region: new_span.result_region,
            intrinsic_mask_id: new_span.intrinsic_mask_id,
            read_summary_id: new_span.read_summary_id,
            binding_set_id: new_span.binding_set_id,
            is_constant_result: new_span.is_constant_result,
            state: SpanState::Active,
            version,
        };
        self.slots.push(SpanSlot {
            generation,
            span: Some(span),
        });
        self.epoch = self.epoch.saturating_add(1);
        FormulaSpanRef {
            id,
            generation,
            version,
        }
    }

    pub(crate) fn get(&self, span_ref: FormulaSpanRef) -> Option<&FormulaSpan> {
        let slot = self.slots.get(span_ref.id.0 as usize)?;
        if slot.generation != span_ref.generation {
            return None;
        }
        let span = slot.span.as_ref()?;
        (span.version == span_ref.version).then_some(span)
    }

    pub(crate) fn remove(&mut self, span_ref: FormulaSpanRef) -> bool {
        let Some(slot) = self.slots.get_mut(span_ref.id.0 as usize) else {
            return false;
        };
        let Some(span) = slot.span.as_ref() else {
            return false;
        };
        if slot.generation != span_ref.generation || span.version != span_ref.version {
            return false;
        }
        slot.span = None;
        slot.generation = slot.generation.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        true
    }

    pub(crate) fn find_at(&self, coord: PlacementCoord) -> Option<&FormulaSpan> {
        self.slots
            .iter()
            .rev()
            .filter_map(|slot| slot.span.as_ref())
            .find(|span| span.state == SpanState::Active && span.domain.contains(coord))
    }

    pub(crate) fn active_spans(&self) -> impl Iterator<Item = &FormulaSpan> {
        self.slots
            .iter()
            .filter_map(|slot| slot.span.as_ref())
            .filter(|span| span.state == SpanState::Active)
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UnsupportedReason {
    Dynamic,
    Volatile,
    Opaque,
    Structural,
    Other(Arc<str>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FormulaOverlayEntryKind {
    FormulaOverride(FormulaTemplateId),
    ValueOverride,
    Cleared,
    LegacyOwned(VertexId),
    Unsupported(UnsupportedReason),
}

#[derive(Clone, Debug)]
pub(crate) struct FormulaOverlayEntryRecord {
    pub(crate) id: FormulaOverlayEntryId,
    pub(crate) generation: u32,
    pub(crate) sheet_id: SheetId,
    pub(crate) domain: PlacementDomain,
    pub(crate) source_span: Option<FormulaSpanRef>,
    pub(crate) kind: FormulaOverlayEntryKind,
    pub(crate) created_epoch: u64,
}

#[derive(Debug)]
struct OverlaySlot {
    generation: u32,
    entry: Option<FormulaOverlayEntryRecord>,
}

#[derive(Debug, Default)]
pub(crate) struct FormulaOverlay {
    slots: Vec<OverlaySlot>,
    epoch: u64,
}

impl FormulaOverlay {
    pub(crate) fn insert(
        &mut self,
        sheet_id: SheetId,
        domain: PlacementDomain,
        kind: FormulaOverlayEntryKind,
        source_span: Option<FormulaSpanRef>,
    ) -> FormulaOverlayRef {
        assert_eq!(
            sheet_id,
            domain.sheet_id(),
            "FormulaOverlay sheet_id must match entry domain sheet_id"
        );

        let id = FormulaOverlayEntryId(self.slots.len() as u32);
        let generation = 0;
        self.epoch = self.epoch.saturating_add(1);
        let entry = FormulaOverlayEntryRecord {
            id,
            generation,
            sheet_id,
            domain,
            source_span,
            kind,
            created_epoch: self.epoch,
        };
        self.slots.push(OverlaySlot {
            generation,
            entry: Some(entry),
        });
        FormulaOverlayRef {
            id,
            generation,
            overlay_epoch: self.epoch,
        }
    }

    pub(crate) fn get(&self, overlay_ref: FormulaOverlayRef) -> Option<&FormulaOverlayEntryRecord> {
        let slot = self.slots.get(overlay_ref.id.0 as usize)?;
        if slot.generation != overlay_ref.generation {
            return None;
        }
        slot.entry.as_ref()
    }

    pub(crate) fn remove(&mut self, overlay_ref: FormulaOverlayRef) -> bool {
        let Some(slot) = self.slots.get_mut(overlay_ref.id.0 as usize) else {
            return false;
        };
        if slot.generation != overlay_ref.generation || slot.entry.is_none() {
            return false;
        }
        slot.entry = None;
        slot.generation = slot.generation.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        true
    }

    pub(crate) fn find_at(&self, coord: PlacementCoord) -> Option<FormulaOverlayRef> {
        self.slots.iter().rev().find_map(|slot| {
            let entry = slot.entry.as_ref()?;
            if entry.sheet_id == coord.sheet_id && entry.domain.contains(coord) {
                Some(FormulaOverlayRef {
                    id: entry.id,
                    generation: entry.generation,
                    overlay_epoch: self.epoch,
                })
            } else {
                None
            }
        })
    }

    pub(crate) fn refs_for_source_span(&self, span_ref: FormulaSpanRef) -> Vec<FormulaOverlayRef> {
        self.slots
            .iter()
            .filter_map(|slot| {
                let entry = slot.entry.as_ref()?;
                (entry.source_span == Some(span_ref)).then_some(FormulaOverlayRef {
                    id: entry.id,
                    generation: entry.generation,
                    overlay_epoch: self.epoch,
                })
            })
            .collect()
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[derive(Debug, Default)]
pub(crate) struct SpanProjectionCache {
    epoch: u64,
}

#[derive(Debug, Default)]
pub(crate) struct SpanDirtyStore {
    epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FormulaResolution {
    StagedFormula {
        text: Arc<str>,
    },
    Overlay(FormulaOverlayRef),
    SpanPlacement {
        span: FormulaSpanRef,
        template_id: FormulaTemplateId,
        placement: PlacementCoord,
    },
    LegacyVertex(VertexId),
    Empty,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaHandle {
    pub(crate) resolution: FormulaResolution,
    pub(crate) plane_epoch: FormulaPlaneEpoch,
    pub(crate) overlay_epoch: u64,
    pub(crate) span_epoch: u64,
}

#[derive(Debug, Default)]
pub(crate) struct FormulaPlane {
    pub(crate) templates: TemplateStore,
    pub(crate) spans: SpanStore,
    pub(crate) span_read_summaries: SpanReadSummaryStore,
    pub(crate) binding_sets: BindingStore,
    pub(crate) formula_overlay: FormulaOverlay,
    pub(crate) projection_cache: SpanProjectionCache,
    pub(crate) dirty: SpanDirtyStore,
    epoch: FormulaPlaneEpoch,
}

impl FormulaPlane {
    pub(crate) fn epoch(&self) -> FormulaPlaneEpoch {
        self.epoch
    }

    pub(crate) fn intern_template(
        &mut self,
        canonical_key: Arc<str>,
        ast_id: AstNodeId,
        origin_row: u32,
        origin_col: u32,
        formula_text: Option<Arc<str>>,
    ) -> FormulaTemplateId {
        self.intern_template_parameterized(
            Arc::clone(&canonical_key),
            canonical_key,
            ast_id,
            origin_row,
            origin_col,
            formula_text,
        )
    }

    pub(crate) fn intern_template_parameterized(
        &mut self,
        exact_canonical_key: Arc<str>,
        parameterized_canonical_key: Arc<str>,
        ast_id: AstNodeId,
        origin_row: u32,
        origin_col: u32,
        formula_text: Option<Arc<str>>,
    ) -> FormulaTemplateId {
        let (id, inserted) = self.templates.intern_template_parameterized(
            exact_canonical_key,
            parameterized_canonical_key,
            ast_id,
            origin_row,
            origin_col,
            formula_text,
        );
        if inserted {
            self.bump_epoch();
        }
        id
    }

    pub(crate) fn insert_span_read_summary(
        &mut self,
        summary: SpanReadSummary,
    ) -> SpanReadSummaryId {
        let id = self.span_read_summaries.insert(summary);
        self.bump_epoch();
        id
    }

    pub(crate) fn insert_binding_set(&mut self, set: SpanBindingSet) -> SpanBindingSetId {
        let id = self.binding_sets.insert(set);
        self.bump_epoch();
        id
    }

    pub(crate) fn set_binding_span_ref(&mut self, id: SpanBindingSetId, span_ref: FormulaSpanRef) {
        self.binding_sets.set_span_ref(id, span_ref);
        self.bump_epoch();
    }

    pub(crate) fn insert_span(&mut self, new_span: NewFormulaSpan) -> FormulaSpanRef {
        let span = self.spans.insert(new_span);
        self.bump_epoch();
        span
    }

    pub(crate) fn remove_span(&mut self, span_ref: FormulaSpanRef) -> bool {
        let binding_set_id = self
            .spans
            .get(span_ref)
            .and_then(|span| span.binding_set_id);
        let removed = self.spans.remove(span_ref);
        if removed {
            if let Some(binding_set_id) = binding_set_id {
                self.binding_sets.remove(binding_set_id);
            }
            self.bump_epoch();
        }
        removed
    }

    pub(crate) fn insert_overlay(
        &mut self,
        sheet_id: SheetId,
        domain: PlacementDomain,
        kind: FormulaOverlayEntryKind,
        source_span: Option<FormulaSpanRef>,
    ) -> FormulaOverlayRef {
        let overlay = self
            .formula_overlay
            .insert(sheet_id, domain, kind, source_span);
        self.bump_epoch();
        overlay
    }

    pub(crate) fn remove_overlay(&mut self, overlay_ref: FormulaOverlayRef) -> bool {
        let removed = self.formula_overlay.remove(overlay_ref);
        if removed {
            self.bump_epoch();
        }
        removed
    }

    pub(crate) fn remove_overlays_for_source_span(&mut self, span_ref: FormulaSpanRef) -> usize {
        let overlay_refs = self.formula_overlay.refs_for_source_span(span_ref);
        let mut removed = 0usize;
        for overlay_ref in overlay_refs {
            if self.remove_overlay(overlay_ref) {
                removed = removed.saturating_add(1);
            }
        }
        removed
    }

    pub(crate) fn resolve_formula_at(
        &self,
        coord: PlacementCoord,
        legacy_vertex: Option<VertexId>,
    ) -> FormulaHandle {
        let resolution = if let Some(overlay) = self.formula_overlay.find_at(coord) {
            FormulaResolution::Overlay(overlay)
        } else if let Some(span) = self.spans.find_at(coord) {
            FormulaResolution::SpanPlacement {
                span: FormulaSpanRef {
                    id: span.id,
                    generation: span.generation,
                    version: span.version,
                },
                template_id: span.template_id,
                placement: coord,
            }
        } else if let Some(vertex) = legacy_vertex {
            FormulaResolution::LegacyVertex(vertex)
        } else {
            FormulaResolution::Empty
        };

        FormulaHandle {
            resolution,
            plane_epoch: self.epoch,
            overlay_epoch: self.formula_overlay.epoch(),
            span_epoch: self.spans.epoch(),
        }
    }

    fn bump_epoch(&mut self) {
        self.epoch.0 = self.epoch.0.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    use super::*;
    use crate::engine::arena::DataStore;
    use crate::engine::sheet_registry::SheetRegistry;

    fn literal_ast_id(value: i64) -> AstNodeId {
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        data_store.store_ast(
            &ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(value)), None),
            &sheet_registry,
        )
    }

    fn template_id(store: &mut TemplateStore, key: &str) -> FormulaTemplateId {
        store
            .intern_template(
                Arc::<str>::from(key),
                literal_ast_id(1),
                1,
                1,
                Some(Arc::<str>::from("=1")),
            )
            .0
    }

    fn new_row_span(sheet_id: SheetId, template_id: FormulaTemplateId) -> NewFormulaSpan {
        let domain = PlacementDomain::row_run(sheet_id, 0, 9, 2);
        NewFormulaSpan {
            sheet_id,
            template_id,
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: None,
            binding_set_id: None,
            is_constant_result: false,
        }
    }

    fn plane_with_row_span() -> (FormulaPlane, FormulaTemplateId, FormulaSpanRef) {
        let mut plane = FormulaPlane::default();
        let template_id = plane.intern_template(
            Arc::<str>::from("template"),
            literal_ast_id(1),
            1,
            1,
            Some(Arc::<str>::from("=1")),
        );
        let span_ref = plane.insert_span(new_row_span(0, template_id));
        (plane, template_id, span_ref)
    }

    fn overlay_kind_at(plane: &FormulaPlane, coord: PlacementCoord) -> FormulaOverlayEntryKind {
        let handle = plane.resolve_formula_at(coord, None);
        let FormulaResolution::Overlay(overlay_ref) = handle.resolution else {
            panic!("expected overlay resolution, got {:?}", handle.resolution);
        };
        plane
            .formula_overlay
            .get(overlay_ref)
            .expect("overlay record")
            .kind
            .clone()
    }

    #[test]
    fn template_store_interns_equivalent_templates_once() {
        let mut store = TemplateStore::default();
        let (first, inserted_first) = store.intern_template(
            Arc::<str>::from("key:literal-one"),
            literal_ast_id(1),
            1,
            1,
            Some(Arc::<str>::from("=1")),
        );
        let (second, inserted_second) = store.intern_template(
            Arc::<str>::from("key:literal-one"),
            literal_ast_id(1),
            1,
            1,
            Some(Arc::<str>::from("=1")),
        );

        assert_eq!(first, second);
        assert!(inserted_first);
        assert!(!inserted_second);
        assert_eq!(store.len(), 1);
        assert_eq!(store.epoch(), 1);
    }

    #[test]
    fn formula_resolution_vocabulary_includes_staged_formula() {
        let resolution = FormulaResolution::StagedFormula {
            text: Arc::<str>::from("=1"),
        };

        assert!(matches!(
            resolution,
            FormulaResolution::StagedFormula { .. }
        ));
    }

    #[test]
    fn span_store_allocates_generational_span_ids() {
        let mut templates = TemplateStore::default();
        let template_id = template_id(&mut templates, "template");
        let mut spans = SpanStore::default();

        let first = spans.insert(new_row_span(0, template_id));
        let second = spans.insert(NewFormulaSpan {
            domain: PlacementDomain::col_run(0, 1, 0, 3),
            result_region: ResultRegion::scalar_cells(PlacementDomain::col_run(0, 1, 0, 3)),
            ..new_row_span(0, template_id)
        });

        assert_eq!(first.id, FormulaSpanId(0));
        assert_eq!(second.id, FormulaSpanId(1));
        assert_eq!(first.generation, 0);
        assert_eq!(second.generation, 0);
        assert!(spans.get(first).is_some());
        assert!(spans.get(second).is_some());
    }

    #[test]
    #[should_panic(expected = "FormulaSpan sheet_id must match placement domain sheet_id")]
    fn span_store_rejects_inconsistent_sheet_id_and_domain() {
        let mut templates = TemplateStore::default();
        let template_id = template_id(&mut templates, "template");
        let mut spans = SpanStore::default();
        let domain = PlacementDomain::row_run(1, 0, 9, 2);
        spans.insert(NewFormulaSpan {
            sheet_id: 0,
            template_id,
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: None,
            binding_set_id: None,
            is_constant_result: false,
        });
    }

    #[test]
    fn span_store_rejects_stale_generation_after_remove() {
        let mut templates = TemplateStore::default();
        let template_id = template_id(&mut templates, "template");
        let mut spans = SpanStore::default();
        let span_ref = spans.insert(new_row_span(0, template_id));

        assert!(spans.remove(span_ref));
        assert!(spans.get(span_ref).is_none());
        assert!(!spans.remove(span_ref));
    }

    #[test]
    fn placement_domain_row_run_iteration_is_correct() {
        let domain = PlacementDomain::row_run(7, 2, 4, 9);
        let coords: Vec<_> = domain.iter().collect();

        assert_eq!(
            coords,
            vec![
                PlacementCoord::new(7, 2, 9),
                PlacementCoord::new(7, 3, 9),
                PlacementCoord::new(7, 4, 9),
            ]
        );
    }

    #[test]
    fn placement_domain_col_run_iteration_is_correct() {
        let domain = PlacementDomain::col_run(7, 4, 2, 5);
        let coords: Vec<_> = domain.iter().collect();

        assert_eq!(
            coords,
            vec![
                PlacementCoord::new(7, 4, 2),
                PlacementCoord::new(7, 4, 3),
                PlacementCoord::new(7, 4, 4),
                PlacementCoord::new(7, 4, 5),
            ]
        );
    }

    #[test]
    fn placement_domain_rect_iteration_is_row_major_and_bounded() {
        let domain = PlacementDomain::rect(3, 1, 2, 4, 5);
        let coords: Vec<_> = domain.iter().collect();

        assert_eq!(
            coords,
            vec![
                PlacementCoord::new(3, 1, 4),
                PlacementCoord::new(3, 1, 5),
                PlacementCoord::new(3, 2, 4),
                PlacementCoord::new(3, 2, 5),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "FormulaOverlay sheet_id must match entry domain sheet_id")]
    fn formula_overlay_rejects_inconsistent_sheet_id_and_domain() {
        let mut overlay = FormulaOverlay::default();
        overlay.insert(
            0,
            PlacementDomain::row_run(1, 0, 0, 0),
            FormulaOverlayEntryKind::ValueOverride,
            None,
        );
    }

    #[test]
    fn formula_overlay_formula_override_resolves_with_template_id() {
        let (mut plane, _template_id, span_ref) = plane_with_row_span();
        let override_template = plane.intern_template(
            Arc::<str>::from("override-template"),
            literal_ast_id(2),
            1,
            1,
            Some(Arc::<str>::from("=2")),
        );
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 3, 3, 2),
            FormulaOverlayEntryKind::FormulaOverride(override_template),
            Some(span_ref),
        );

        assert_eq!(
            overlay_kind_at(&plane, PlacementCoord::new(0, 3, 2)),
            FormulaOverlayEntryKind::FormulaOverride(override_template)
        );
    }

    #[test]
    fn formula_overlay_value_override_resolves_as_formula_tombstone() {
        let (mut plane, _template_id, span_ref) = plane_with_row_span();
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 3, 3, 2),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span_ref),
        );

        assert_eq!(
            overlay_kind_at(&plane, PlacementCoord::new(0, 3, 2)),
            FormulaOverlayEntryKind::ValueOverride
        );
    }

    #[test]
    fn formula_overlay_cleared_resolves_as_formula_tombstone() {
        let (mut plane, _template_id, span_ref) = plane_with_row_span();
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 3, 3, 2),
            FormulaOverlayEntryKind::Cleared,
            Some(span_ref),
        );

        assert_eq!(
            overlay_kind_at(&plane, PlacementCoord::new(0, 3, 2)),
            FormulaOverlayEntryKind::Cleared
        );
    }

    #[test]
    fn formula_overlay_unsupported_resolves_with_reason() {
        let (mut plane, _template_id, span_ref) = plane_with_row_span();
        let reason = UnsupportedReason::Other(Arc::<str>::from("test unsupported"));
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 3, 3, 2),
            FormulaOverlayEntryKind::Unsupported(reason.clone()),
            Some(span_ref),
        );

        assert_eq!(
            overlay_kind_at(&plane, PlacementCoord::new(0, 3, 2)),
            FormulaOverlayEntryKind::Unsupported(reason)
        );
    }

    #[test]
    fn removed_overlay_ref_no_longer_masks_span() {
        let (mut plane, template_id, span_ref) = plane_with_row_span();
        let overlay_ref = plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 3, 3, 2),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span_ref),
        );
        assert!(matches!(
            plane
                .resolve_formula_at(PlacementCoord::new(0, 3, 2), None)
                .resolution,
            FormulaResolution::Overlay(_)
        ));

        assert!(plane.remove_overlay(overlay_ref));

        assert!(plane.formula_overlay.get(overlay_ref).is_none());
        assert_eq!(
            plane
                .resolve_formula_at(PlacementCoord::new(0, 3, 2), None)
                .resolution,
            FormulaResolution::SpanPlacement {
                span: span_ref,
                template_id,
                placement: PlacementCoord::new(0, 3, 2),
            }
        );
    }

    #[test]
    fn formula_overlay_masks_span_resolution() {
        let (mut plane, _template_id, span_ref) = plane_with_row_span();
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 3, 3, 2),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span_ref),
        );

        let handle = plane.resolve_formula_at(PlacementCoord::new(0, 3, 2), None);
        assert!(matches!(handle.resolution, FormulaResolution::Overlay(_)));
    }

    #[test]
    fn legacy_owned_overlay_prevents_span_resolution() {
        let mut plane = FormulaPlane::default();
        let template_id = plane.intern_template(
            Arc::<str>::from("template"),
            literal_ast_id(1),
            1,
            1,
            Some(Arc::<str>::from("=1")),
        );
        let span_ref = plane.insert_span(new_row_span(0, template_id));
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 4, 4, 2),
            FormulaOverlayEntryKind::LegacyOwned(VertexId(42)),
            Some(span_ref),
        );

        let handle = plane.resolve_formula_at(PlacementCoord::new(0, 4, 2), None);
        assert!(matches!(handle.resolution, FormulaResolution::Overlay(_)));
    }

    #[test]
    fn formula_resolution_prefers_overlay_over_span_over_legacy() {
        let mut plane = FormulaPlane::default();
        let template_id = plane.intern_template(
            Arc::<str>::from("template"),
            literal_ast_id(1),
            1,
            1,
            Some(Arc::<str>::from("=1")),
        );
        let span_ref = plane.insert_span(new_row_span(0, template_id));
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 5, 5, 2),
            FormulaOverlayEntryKind::Cleared,
            Some(span_ref),
        );

        let overlay = plane.resolve_formula_at(PlacementCoord::new(0, 5, 2), Some(VertexId(7)));
        let span = plane.resolve_formula_at(PlacementCoord::new(0, 6, 2), Some(VertexId(7)));
        let legacy = plane.resolve_formula_at(PlacementCoord::new(0, 11, 2), Some(VertexId(7)));
        let empty = plane.resolve_formula_at(PlacementCoord::new(0, 11, 2), None);

        assert!(matches!(overlay.resolution, FormulaResolution::Overlay(_)));
        assert!(matches!(
            span.resolution,
            FormulaResolution::SpanPlacement { .. }
        ));
        assert!(matches!(
            legacy.resolution,
            FormulaResolution::LegacyVertex(VertexId(7))
        ));
        assert_eq!(empty.resolution, FormulaResolution::Empty);
    }

    #[test]
    fn formula_resolution_returns_span_placement_without_legacy_materialization() {
        let mut plane = FormulaPlane::default();
        let template_id = plane.intern_template(
            Arc::<str>::from("template"),
            literal_ast_id(1),
            1,
            1,
            Some(Arc::<str>::from("=1")),
        );
        let span_ref = plane.insert_span(new_row_span(0, template_id));

        let handle = plane.resolve_formula_at(PlacementCoord::new(0, 2, 2), None);
        assert_eq!(
            handle.resolution,
            FormulaResolution::SpanPlacement {
                span: span_ref,
                template_id,
                placement: PlacementCoord::new(0, 2, 2),
            }
        );
    }

    #[test]
    fn formula_plane_epoch_increments_when_store_mutates() {
        let mut plane = FormulaPlane::default();
        assert_eq!(plane.epoch(), FormulaPlaneEpoch(0));

        let template_id = plane.intern_template(
            Arc::<str>::from("template"),
            literal_ast_id(1),
            1,
            1,
            Some(Arc::<str>::from("=1")),
        );
        assert_eq!(plane.epoch(), FormulaPlaneEpoch(1));

        let span_ref = plane.insert_span(new_row_span(0, template_id));
        assert_eq!(plane.epoch(), FormulaPlaneEpoch(2));

        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 2, 2, 2),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span_ref),
        );
        assert_eq!(plane.epoch(), FormulaPlaneEpoch(3));
    }
}
