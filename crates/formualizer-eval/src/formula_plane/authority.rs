//! Graph-owned FormulaPlane authority shell.
//!
//! The authority owns accepted spans, tracks overlay punchouts, rebuilds
//! producer/read indexes, and projects edited regions into span-local dirty work
//! for opt-in authoritative FormulaPlane evaluation. Unsupported or demoted
//! formulas remain on the legacy dependency graph.

use rustc_hash::FxHashSet;

use super::producer::{FormulaConsumerReadIndex, FormulaProducerId, FormulaProducerResultIndex};
use super::region_index::{FormulaOverlayIndex, Region, SpanDomainIndex};
use super::runtime::{FormulaPlane, FormulaSpanRef};

#[derive(Debug, Default)]
pub(crate) struct FormulaAuthority {
    pub(crate) plane: FormulaPlane,
    pub(crate) producer_results: FormulaProducerResultIndex,
    pub(crate) consumer_reads: FormulaConsumerReadIndex,
    pub(crate) span_domains: SpanDomainIndex,
    pub(crate) overlays: FormulaOverlayIndex,
    pub(crate) indexes_epoch: u64,
    pub(crate) indexed_plane_epoch: u64,
    #[cfg(test)]
    pub(crate) prepared_append_failure_for_test: bool,
    /// Externally-observed changed regions accumulated since the last
    /// successful evaluation acknowledgement. Edits that intersect span read
    /// regions drive bounded span dirty work via `compute_dirty_closure`.
    pending_changed_regions: Vec<Region>,
    pending_seen: FxHashSet<Region>,
    pending_lease_generation: u64,
    active_pending_lease_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingChangedRegionsLease {
    generation: u64,
    prefix_len: usize,
    regions: Vec<Region>,
}

impl PendingChangedRegionsLease {
    pub(crate) fn regions(&self) -> &[Region] {
        &self.regions
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaAuthorityIndexReport {
    pub(crate) plane_epoch: u64,
    pub(crate) indexes_epoch: u64,
    pub(crate) spans_seen: usize,
    pub(crate) spans_indexed: usize,
    pub(crate) producer_result_entries: usize,
    pub(crate) consumer_read_entries: usize,
    pub(crate) missing_read_summary_count: usize,
    pub(crate) stale_or_invalid_summary_count: usize,
}

impl FormulaAuthority {
    pub(crate) fn indexes_epoch(&self) -> u64 {
        self.indexes_epoch
    }

    pub(crate) fn indexed_plane_epoch(&self) -> u64 {
        self.indexed_plane_epoch
    }

    pub(crate) fn active_span_count(&self) -> usize {
        self.plane.spans.active_spans().count()
    }

    pub(crate) fn active_span_refs(&self) -> Vec<FormulaSpanRef> {
        self.plane
            .spans
            .active_spans()
            .map(|span| FormulaSpanRef {
                id: span.id,
                generation: span.generation,
                version: span.version,
            })
            .collect()
    }

    pub(crate) fn record_changed_region(&mut self, region: Region) {
        if self.pending_seen.insert(region) {
            self.pending_changed_regions.push(region);
        }
    }

    pub(crate) fn lease_pending_changed_regions(&mut self) -> PendingChangedRegionsLease {
        self.pending_lease_generation = self.pending_lease_generation.wrapping_add(1);
        let generation = self.pending_lease_generation;
        self.active_pending_lease_generation = Some(generation);
        let lease = PendingChangedRegionsLease {
            generation,
            prefix_len: self.pending_changed_regions.len(),
            regions: self.pending_changed_regions.clone(),
        };
        // The leased prefix remains queued but no longer participates in
        // dedupe. Identical post-lease changes therefore append as new work,
        // while this set dedupes only within the new generation.
        self.pending_seen.clear();
        lease
    }

    pub(crate) fn ack_pending_changed_regions(&mut self, lease: PendingChangedRegionsLease) {
        if self.active_pending_lease_generation != Some(lease.generation) {
            return;
        }
        let prefix_len = lease.prefix_len.min(self.pending_changed_regions.len());
        self.pending_changed_regions.drain(..prefix_len);
        self.active_pending_lease_generation = None;
    }

    pub(crate) fn pending_changed_regions(&self) -> &[Region] {
        &self.pending_changed_regions
    }

    pub(crate) fn pending_changed_region_count(&self) -> usize {
        self.pending_changed_regions.len()
    }

    pub(crate) fn mark_all_active_spans_dirty(&mut self) {
        // Conservative escape hatch: invalidate every span by bumping the
        // authority index epoch. The FormulaPlane coordinator treats an unseen
        // epoch as `WholeAll`, which is the only representation that guarantees
        // self-dirtying for spans whose result region (rather than read region)
        // was structurally affected.
        if self.active_span_count() == 0 {
            return;
        }
        self.indexes_epoch = self.indexes_epoch.saturating_add(1);

        // Also publish result regions as changed regions so downstream span
        // consumers can be discovered through the normal dirty-closure path if
        // the caller evaluates before another epoch-bumping rebuild.
        let regions: Vec<Region> = self
            .plane
            .spans
            .active_spans()
            .map(|span| Region::from_domain(span.result_region.domain()))
            .collect();
        for region in regions {
            self.record_changed_region(region);
        }
    }

    pub(crate) fn rebuild_indexes(&mut self) -> FormulaAuthorityIndexReport {
        let mut producer_results = FormulaProducerResultIndex::default();
        let mut consumer_reads = FormulaConsumerReadIndex::default();
        let mut span_domains = SpanDomainIndex::default();
        let mut overlays = FormulaOverlayIndex::default();
        let mut report = FormulaAuthorityIndexReport {
            plane_epoch: self.plane.epoch().0,
            ..FormulaAuthorityIndexReport::default()
        };

        for span in self.plane.spans.active_spans() {
            report.spans_seen = report.spans_seen.saturating_add(1);
            let result_region =
                super::region_index::Region::from_domain(span.result_region.domain());
            let span_ref = FormulaSpanRef {
                id: span.id,
                generation: span.generation,
                version: span.version,
            };
            span_domains.insert_domain(span_ref, span.domain.clone());
            let producer = FormulaProducerId::Span(span.id);
            producer_results.insert_producer(producer, result_region);
            report.producer_result_entries = report.producer_result_entries.saturating_add(1);

            let Some(read_summary_id) = span.read_summary_id else {
                report.missing_read_summary_count =
                    report.missing_read_summary_count.saturating_add(1);
                continue;
            };
            let Some(read_summary) = self.plane.span_read_summaries.get(read_summary_id) else {
                report.stale_or_invalid_summary_count =
                    report.stale_or_invalid_summary_count.saturating_add(1);
                continue;
            };
            if read_summary.result_region != result_region {
                report.stale_or_invalid_summary_count =
                    report.stale_or_invalid_summary_count.saturating_add(1);
                continue;
            }

            for dependency in &read_summary.dependencies {
                consumer_reads.insert_read(
                    producer,
                    dependency.read_region,
                    read_summary.result_region,
                    dependency.projection,
                );
                report.consumer_read_entries = report.consumer_read_entries.saturating_add(1);
            }
            report.spans_indexed = report.spans_indexed.saturating_add(1);
        }

        for (entry, overlay_ref) in self.plane.formula_overlay.active_entries() {
            overlays.insert_overlay(overlay_ref, entry.domain.clone());
        }
        overlays.mark_built_from_overlay_epoch(self.plane.formula_overlay.epoch());

        self.indexes_epoch = self.indexes_epoch.saturating_add(1);
        self.indexed_plane_epoch = self.plane.epoch().0;
        report.indexes_epoch = self.indexes_epoch;
        self.producer_results = producer_results;
        self.consumer_reads = consumer_reads;
        self.span_domains = span_domains;
        self.overlays = overlays;
        report
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use formualizer_parse::parser::parse;

    use super::*;
    use crate::engine::arena::DataStore;
    use crate::engine::sheet_registry::SheetRegistry;
    use crate::formula_plane::producer::{
        AxisProjection, DirtyProjectionRule, ProducerDirtyDomain, ProjectionResult,
        SpanReadDependency, SpanReadSummary, compute_dirty_closure,
    };
    use crate::formula_plane::region_index::{Region, RegionKey};
    use crate::formula_plane::runtime::{
        FormulaSpanId, NewFormulaSpan, PlacementDomain, ResultRegion,
    };

    fn template(authority: &mut FormulaAuthority) -> crate::formula_plane::ids::FormulaTemplateId {
        authority.plane.intern_template(
            Arc::<str>::from("test-template"),
            {
                let mut data_store = DataStore::new();
                let sheet_registry = SheetRegistry::new();
                data_store.store_ast(&parse("=A1+1").unwrap(), &sheet_registry)
            },
            1,
            1,
            Some(Arc::<str>::from("=A1+1")),
        )
    }

    fn add_span_with_summary(
        authority: &mut FormulaAuthority,
        domain: PlacementDomain,
        summary: SpanReadSummary,
    ) -> FormulaSpanId {
        let sheet_id = domain.sheet_id();
        let template_id = template(authority);
        let read_summary_id = authority.plane.insert_span_read_summary(summary);
        authority
            .plane
            .insert_span(NewFormulaSpan {
                sheet_id,
                template_id,
                result_region: ResultRegion::scalar_cells(domain.clone()),
                domain,
                intrinsic_mask_id: None,
                read_summary_id: Some(read_summary_id),
                binding_set_id: None,
                is_constant_result: false,
            })
            .id
    }

    #[test]
    fn authority_rebuild_indexes_span_result_regions() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let summary = SpanReadSummary {
            result_region: Region::from_domain(&domain),
            dependencies: Vec::new(),
        };
        let span_id = add_span_with_summary(&mut authority, domain, summary);

        let report = authority.rebuild_indexes();

        assert_eq!(report.spans_seen, 1);
        assert_eq!(report.spans_indexed, 1);
        assert_eq!(report.producer_result_entries, 1);
        assert_eq!(report.consumer_read_entries, 0);
        assert_eq!(authority.producer_results.len(), 1);
        assert_eq!(authority.consumer_reads.len(), 0);
        assert_eq!(
            authority
                .producer_results
                .producer_result_region(FormulaProducerId::Span(span_id)),
            Some(Region::col_interval(0, 2, 0, 9))
        );
    }

    #[test]
    fn authority_rebuild_indexes_span_read_dependencies() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let result_region = Region::from_domain(&domain);
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        let read_region = projection.read_region_for_result(0, result_region).unwrap();
        let span_id = add_span_with_summary(
            &mut authority,
            domain,
            SpanReadSummary {
                result_region,
                dependencies: vec![SpanReadDependency {
                    read_region,
                    projection,
                }],
            },
        );

        let report = authority.rebuild_indexes();

        assert_eq!(report.spans_indexed, 1);
        assert_eq!(report.consumer_read_entries, 1);
        let dirty = authority
            .consumer_reads
            .query_changed_region(Region::point(0, 5, 1));
        assert_eq!(dirty.matches.len(), 1);
        assert_eq!(
            dirty.matches[0].value.consumer,
            FormulaProducerId::Span(span_id)
        );
        assert_eq!(
            dirty.matches[0].value.dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 2)]))
        );
    }

    #[test]
    fn authority_rebuild_indexes_missing_read_summary_counts_and_indexes_result() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let template_id = template(&mut authority);
        let span = authority
            .plane
            .insert_span(NewFormulaSpan {
                sheet_id: 0,
                template_id,
                result_region: ResultRegion::scalar_cells(domain.clone()),
                domain,
                intrinsic_mask_id: None,
                read_summary_id: None,
                binding_set_id: None,
                is_constant_result: false,
            })
            .id;

        let report = authority.rebuild_indexes();

        assert_eq!(report.spans_seen, 1);
        assert_eq!(report.spans_indexed, 0);
        assert_eq!(report.missing_read_summary_count, 1);
        assert_eq!(report.producer_result_entries, 1);
        assert_eq!(report.consumer_read_entries, 0);
        assert_eq!(
            authority
                .producer_results
                .producer_result_region(FormulaProducerId::Span(span)),
            Some(Region::col_interval(0, 2, 0, 9))
        );
    }

    #[test]
    fn authority_rebuild_indexes_stale_summary_counts_without_read_entry() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let mismatched_result = Region::col_interval(0, 3, 0, 9);
        add_span_with_summary(
            &mut authority,
            domain,
            SpanReadSummary {
                result_region: mismatched_result,
                dependencies: vec![SpanReadDependency {
                    read_region: Region::col_interval(0, 1, 0, 9),
                    projection: DirtyProjectionRule::WholeResult,
                }],
            },
        );

        let report = authority.rebuild_indexes();

        assert_eq!(report.spans_seen, 1);
        assert_eq!(report.spans_indexed, 0);
        assert_eq!(report.stale_or_invalid_summary_count, 1);
        assert_eq!(report.producer_result_entries, 1);
        assert_eq!(report.consumer_read_entries, 0);
        assert_eq!(authority.consumer_reads.len(), 0);
    }

    #[test]
    fn authority_dirty_closure_uses_rebuilt_indexes() {
        let mut authority = FormulaAuthority::default();
        let b_domain = PlacementDomain::row_run(0, 0, 9, 1);
        let c_domain = PlacementDomain::row_run(0, 0, 9, 2);
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };

        let b_result = Region::from_domain(&b_domain);
        let b_read = projection.read_region_for_result(0, b_result).unwrap();
        let b_span = add_span_with_summary(
            &mut authority,
            b_domain,
            SpanReadSummary {
                result_region: b_result,
                dependencies: vec![SpanReadDependency {
                    read_region: b_read,
                    projection,
                }],
            },
        );

        let c_result = Region::from_domain(&c_domain);
        let c_read = projection.read_region_for_result(0, c_result).unwrap();
        let c_span = add_span_with_summary(
            &mut authority,
            c_domain,
            SpanReadSummary {
                result_region: c_result,
                dependencies: vec![SpanReadDependency {
                    read_region: c_read,
                    projection,
                }],
            },
        );

        let report = authority.rebuild_indexes();
        assert_eq!(report.spans_indexed, 2);
        assert_eq!(report.consumer_read_entries, 2);

        let closure = compute_dirty_closure(
            &authority.consumer_reads,
            [Region::point(0, 5, 0)],
            |producer| authority.producer_results.producer_result_region(producer),
        );

        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 2);
        assert_eq!(closure.work[0].producer, FormulaProducerId::Span(b_span));
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 1)])
        );
        assert_eq!(closure.work[1].producer, FormulaProducerId::Span(c_span));
        assert_eq!(
            closure.work[1].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 2)])
        );
    }

    #[test]
    fn authority_rebuild_replaces_stale_index_entries() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let summary = SpanReadSummary {
            result_region: Region::from_domain(&domain),
            dependencies: Vec::new(),
        };
        let template_id = template(&mut authority);
        let read_summary_id = authority.plane.insert_span_read_summary(summary);
        let span_ref = authority.plane.insert_span(NewFormulaSpan {
            sheet_id: 0,
            template_id,
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: Some(read_summary_id),
            binding_set_id: None,
            is_constant_result: false,
        });
        let first = authority.rebuild_indexes();
        assert_eq!(first.producer_result_entries, 1);

        assert!(authority.plane.remove_span(span_ref));
        let second = authority.rebuild_indexes();
        assert_eq!(second.spans_seen, 0);
        assert_eq!(second.producer_result_entries, 0);
        assert_eq!(authority.producer_results.len(), 0);
        assert!(authority.indexes_epoch() > first.indexes_epoch);
    }

    #[test]
    fn pending_lease_ack_preserves_identical_region_recorded_after_lease() {
        let mut authority = FormulaAuthority::default();
        let region = Region::point(0, 1, 1);
        authority.record_changed_region(region);
        let lease = authority.lease_pending_changed_regions();
        authority.record_changed_region(region);
        authority.record_changed_region(region);
        assert_eq!(authority.pending_changed_regions(), &[region, region]);

        authority.ack_pending_changed_regions(lease);

        assert_eq!(authority.pending_changed_regions(), &[region]);
    }

    #[test]
    fn pending_lease_ack_preserves_different_later_regions_in_order() {
        let mut authority = FormulaAuthority::default();
        let leased_region = Region::point(0, 1, 1);
        let later_a = Region::point(0, 2, 2);
        let later_b = Region::point(0, 3, 3);
        authority.record_changed_region(leased_region);
        let lease = authority.lease_pending_changed_regions();
        authority.record_changed_region(later_a);
        authority.record_changed_region(later_b);
        authority.record_changed_region(later_a);

        authority.ack_pending_changed_regions(lease);

        assert_eq!(authority.pending_changed_regions(), &[later_a, later_b]);
    }

    #[test]
    fn pending_lease_multiple_ack_cycles_keep_generation_local_dedupe() {
        let mut authority = FormulaAuthority::default();
        let first = Region::point(0, 1, 1);
        let second = Region::point(0, 2, 2);
        authority.record_changed_region(first);
        let first_lease = authority.lease_pending_changed_regions();
        authority.record_changed_region(second);
        authority.record_changed_region(second);
        authority.ack_pending_changed_regions(first_lease);
        assert_eq!(authority.pending_changed_regions(), &[second]);

        let second_lease = authority.lease_pending_changed_regions();
        authority.record_changed_region(first);
        authority.record_changed_region(first);
        authority.ack_pending_changed_regions(second_lease);
        assert_eq!(authority.pending_changed_regions(), &[first]);

        let final_lease = authority.lease_pending_changed_regions();
        authority.ack_pending_changed_regions(final_lease);
        assert!(authority.pending_changed_regions().is_empty());
    }

    #[test]
    fn abandoned_pending_lease_retains_original_work_for_failed_evaluation_retry() {
        let mut authority = FormulaAuthority::default();
        let original = Region::point(0, 1, 1);
        let during_failed_evaluation = Region::point(0, 2, 2);
        authority.record_changed_region(original);
        let abandoned = authority.lease_pending_changed_regions();
        authority.record_changed_region(during_failed_evaluation);
        drop(abandoned);
        assert_eq!(
            authority.pending_changed_regions(),
            &[original, during_failed_evaluation]
        );

        let retry = authority.lease_pending_changed_regions();
        assert_eq!(retry.regions(), &[original, during_failed_evaluation]);
        authority.record_changed_region(original);
        authority.ack_pending_changed_regions(retry);
        assert_eq!(authority.pending_changed_regions(), &[original]);
    }
}
