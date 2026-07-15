use rustc_hash::FxHashSet;

use crate::engine::VertexId;
use crate::formula_plane::region_index::Region;
use crate::formula_plane::runtime::FormulaSpanRef;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WholeSpanDirtyReason {
    NewSpan,
    GlobalInvalidation,
    CycleRetry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FormulaDirtyEvent {
    Region(Region),
    WholeSpan(FormulaSpanRef),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaDirtyLease {
    generation: u64,
    prefix_len: usize,
    events: Vec<FormulaDirtyEvent>,
}

impl FormulaDirtyLease {
    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub(crate) fn regions(&self) -> impl Iterator<Item = Region> + '_ {
        self.events.iter().filter_map(|event| match event {
            FormulaDirtyEvent::Region(region) => Some(*region),
            FormulaDirtyEvent::WholeSpan(_) => None,
        })
    }

    pub(crate) fn whole_spans(&self) -> impl Iterator<Item = FormulaSpanRef> + '_ {
        self.events.iter().filter_map(|event| match event {
            FormulaDirtyEvent::Region(_) => None,
            FormulaDirtyEvent::WholeSpan(span_ref) => Some(*span_ref),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaDirtyStats {
    pub(crate) pending_events: usize,
    pub(crate) region_events_recorded: u64,
    pub(crate) whole_span_seeds_recorded: u64,
    pub(crate) global_whole_span_invalidations: u64,
}

#[derive(Debug, Default)]
pub(super) struct FormulaDirtyState {
    legacy_vertices: FxHashSet<VertexId>,
    events: Vec<FormulaDirtyEvent>,
    pending_regions_seen: FxHashSet<Region>,
    pending_spans_seen: FxHashSet<FormulaSpanRef>,
    lease_generation: u64,
    active_lease: Option<ActiveFormulaDirtyLease>,
    region_events_recorded: u64,
    whole_span_seeds_recorded: u64,
    global_whole_span_invalidations: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActiveFormulaDirtyLease {
    generation: u64,
    prefix_len: usize,
    extended: bool,
}

impl FormulaDirtyState {
    pub(super) fn legacy_len(&self) -> usize {
        self.legacy_vertices.len()
    }

    pub(super) fn legacy_contains(&self, vertex: &VertexId) -> bool {
        self.legacy_vertices.contains(vertex)
    }

    pub(super) fn legacy_insert(&mut self, vertex: VertexId) {
        self.legacy_vertices.insert(vertex);
    }

    pub(super) fn legacy_extend(&mut self, vertices: impl IntoIterator<Item = VertexId>) {
        self.legacy_vertices.extend(vertices);
    }

    pub(super) fn legacy_remove(&mut self, vertex: &VertexId) {
        self.legacy_vertices.remove(vertex);
    }

    pub(super) fn legacy_reserve(&mut self, additional: usize) {
        self.legacy_vertices.reserve(additional);
    }

    pub(super) fn legacy_iter(&self) -> impl Iterator<Item = &VertexId> {
        self.legacy_vertices.iter()
    }

    pub(super) fn record_region(&mut self, region: Region) {
        if self.pending_regions_seen.insert(region) {
            self.events.push(FormulaDirtyEvent::Region(region));
            self.region_events_recorded = self.region_events_recorded.saturating_add(1);
        }
    }

    pub(super) fn record_whole_spans(
        &mut self,
        spans: impl IntoIterator<Item = FormulaSpanRef>,
        reason: WholeSpanDirtyReason,
    ) {
        let mut inserted = 0u64;
        let mut saw_span = false;
        for span_ref in spans {
            saw_span = true;
            if self.pending_spans_seen.insert(span_ref) {
                self.events.push(FormulaDirtyEvent::WholeSpan(span_ref));
                inserted = inserted.saturating_add(1);
            }
        }
        self.whole_span_seeds_recorded = self.whole_span_seeds_recorded.saturating_add(inserted);
        if reason == WholeSpanDirtyReason::GlobalInvalidation && saw_span {
            self.global_whole_span_invalidations =
                self.global_whole_span_invalidations.saturating_add(1);
        }
    }

    pub(super) fn lease(&mut self) -> FormulaDirtyLease {
        self.lease_generation = self.lease_generation.wrapping_add(1);
        let generation = self.lease_generation;
        let prefix_len = self.events.len();
        self.active_lease = Some(ActiveFormulaDirtyLease {
            generation,
            prefix_len,
            extended: false,
        });
        let lease = FormulaDirtyLease {
            generation,
            prefix_len,
            events: self.events.clone(),
        };
        self.pending_regions_seen.clear();
        self.pending_spans_seen.clear();
        lease
    }

    pub(super) fn extend(&mut self, lease: FormulaDirtyLease) -> Option<FormulaDirtyLease> {
        let active = self.active_lease?;
        if active.generation != lease.generation
            || active.prefix_len != lease.prefix_len
            || active.extended
        {
            return None;
        }

        let prefix_len = self.events.len();
        self.active_lease = Some(ActiveFormulaDirtyLease {
            generation: active.generation,
            prefix_len,
            extended: true,
        });
        let lease = FormulaDirtyLease {
            generation: active.generation,
            prefix_len,
            events: self.events[..prefix_len].to_vec(),
        };
        self.pending_regions_seen.clear();
        self.pending_spans_seen.clear();
        Some(lease)
    }

    pub(super) fn ack(&mut self, lease: FormulaDirtyLease) -> bool {
        let Some(active) = self.active_lease else {
            return false;
        };
        if active.generation != lease.generation || active.prefix_len != lease.prefix_len {
            return false;
        }
        let prefix_len = lease.prefix_len.min(self.events.len());
        self.events.drain(..prefix_len);
        self.active_lease = None;
        true
    }

    pub(super) fn pending_regions(&self) -> impl Iterator<Item = Region> + '_ {
        self.events.iter().filter_map(|event| match event {
            FormulaDirtyEvent::Region(region) => Some(*region),
            FormulaDirtyEvent::WholeSpan(_) => None,
        })
    }

    pub(super) fn pending_whole_spans(&self) -> impl Iterator<Item = FormulaSpanRef> + '_ {
        self.events.iter().filter_map(|event| match event {
            FormulaDirtyEvent::Region(_) => None,
            FormulaDirtyEvent::WholeSpan(span_ref) => Some(*span_ref),
        })
    }

    pub(super) fn pending_event_count(&self) -> usize {
        self.events.len()
    }

    pub(super) fn stats(&self) -> FormulaDirtyStats {
        FormulaDirtyStats {
            pending_events: self.events.len(),
            region_events_recorded: self.region_events_recorded,
            whole_span_seeds_recorded: self.whole_span_seeds_recorded,
            global_whole_span_invalidations: self.global_whole_span_invalidations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formula_plane::runtime::FormulaSpanId;

    fn span(id: u32) -> FormulaSpanRef {
        FormulaSpanRef {
            id: FormulaSpanId(id),
            generation: 0,
            version: 0,
        }
    }

    #[test]
    fn lease_ack_preserves_identical_and_later_events() {
        let mut dirty = FormulaDirtyState::default();
        let region = Region::point(0, 1, 1);
        dirty.record_region(region);
        dirty.record_whole_spans([span(1)], WholeSpanDirtyReason::NewSpan);
        let lease = dirty.lease();
        dirty.record_region(region);
        dirty.record_region(region);
        dirty.record_whole_spans([span(1)], WholeSpanDirtyReason::CycleRetry);
        assert!(dirty.ack(lease));
        assert_eq!(dirty.pending_regions().collect::<Vec<_>>(), vec![region]);
        assert_eq!(
            dirty.pending_whole_spans().collect::<Vec<_>>(),
            vec![span(1)]
        );
    }

    #[test]
    fn extended_lease_owns_retry_seed_but_not_later_identical_event() {
        let mut dirty = FormulaDirtyState::default();
        let region = Region::point(0, 1, 1);
        let retry_span = span(1);
        dirty.record_region(region);
        let original = dirty.lease();

        dirty.record_whole_spans([retry_span], WholeSpanDirtyReason::CycleRetry);
        let extended = dirty.extend(original.clone()).expect("lease must extend");
        dirty.record_whole_spans([retry_span], WholeSpanDirtyReason::NewSpan);

        assert!(!dirty.ack(original));
        assert!(dirty.ack(extended));
        assert_eq!(dirty.pending_regions().collect::<Vec<_>>(), Vec::new());
        assert_eq!(
            dirty.pending_whole_spans().collect::<Vec<_>>(),
            vec![retry_span]
        );
    }

    #[test]
    fn stale_or_repeated_extension_cannot_expand_owned_prefix() {
        let mut dirty = FormulaDirtyState::default();
        dirty.record_region(Region::point(0, 1, 1));
        let original = dirty.lease();
        dirty.record_whole_spans([span(1)], WholeSpanDirtyReason::CycleRetry);
        let extended = dirty.extend(original.clone()).expect("lease must extend");

        dirty.record_whole_spans([span(2)], WholeSpanDirtyReason::NewSpan);
        assert!(dirty.extend(original).is_none());
        assert!(dirty.extend(extended.clone()).is_none());
        assert!(dirty.ack(extended));
        assert_eq!(
            dirty.pending_whole_spans().collect::<Vec<_>>(),
            vec![span(2)]
        );
    }

    #[test]
    fn abandoned_extended_lease_retains_owned_and_later_work() {
        let mut dirty = FormulaDirtyState::default();
        let region = Region::point(0, 1, 1);
        dirty.record_region(region);
        let original = dirty.lease();
        dirty.record_whole_spans([span(1)], WholeSpanDirtyReason::CycleRetry);
        let abandoned = dirty.extend(original).expect("lease must extend");
        dirty.record_whole_spans([span(1)], WholeSpanDirtyReason::NewSpan);
        drop(abandoned);

        let retry = dirty.lease();
        assert_eq!(retry.regions().collect::<Vec<_>>(), vec![region]);
        assert_eq!(
            retry.whole_spans().collect::<Vec<_>>(),
            vec![span(1), span(1)]
        );
    }

    #[test]
    fn stale_generation_cannot_ack_newer_lease() {
        let mut dirty = FormulaDirtyState::default();
        let first = Region::point(0, 1, 1);
        let second = Region::point(0, 2, 2);
        dirty.record_region(first);
        let stale = dirty.lease();
        dirty.record_region(second);
        let current = dirty.lease();
        assert!(!dirty.ack(stale));
        assert_eq!(dirty.pending_event_count(), 2);
        assert!(dirty.ack(current));
        assert_eq!(dirty.pending_event_count(), 0);
    }

    #[test]
    fn graph_owned_dirty_authority_source_audit() {
        let authority = include_str!("../../formula_plane/authority.rs");
        let evaluator = include_str!("../eval.rs");
        let graph = include_str!("mod.rs");
        assert!(!authority.contains("pending_changed_regions"));
        assert!(!authority.contains("record_changed_region"));
        assert!(!evaluator.contains("SpanSeedMode"));
        assert!(!graph.contains("dirty_vertices: FxHashSet"));
        assert!(graph.contains("formula_dirty: FormulaDirtyState"));
    }

    #[test]
    fn abandoned_lease_retains_exact_retry_work() {
        let mut dirty = FormulaDirtyState::default();
        let first = Region::point(0, 1, 1);
        let later = Region::point(0, 2, 2);
        dirty.record_region(first);
        let abandoned = dirty.lease();
        dirty.record_region(later);
        drop(abandoned);
        let retry = dirty.lease();
        assert_eq!(retry.regions().collect::<Vec<_>>(), vec![first, later]);
    }
}
