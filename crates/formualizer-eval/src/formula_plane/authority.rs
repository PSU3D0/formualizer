//! Graph-owned FormulaPlane authority shell.
//!
//! This is intentionally inert in FP6.5R: normal formula ingest still
//! materializes every formula into the legacy dependency graph. The authority
//! shell establishes ownership on `DependencyGraph` and can rebuild producer/read
//! indexes from accepted spans, but those indexes are not wired into graph dirty
//! propagation, scheduling, or evaluation yet.

use rustc_hash::FxHashSet;

use super::producer::{FormulaConsumerReadIndex, FormulaProducerId, FormulaProducerResultIndex};
use super::region_index::RegionPattern;
use super::runtime::{FormulaPlane, FormulaSpanRef};

#[derive(Debug, Default)]
pub(crate) struct FormulaAuthority {
    pub(crate) plane: FormulaPlane,
    pub(crate) producer_results: FormulaProducerResultIndex,
    pub(crate) consumer_reads: FormulaConsumerReadIndex,
    indexes_epoch: u64,
    /// Externally-observed changed regions accumulated since the last
    /// `take_pending_changed_regions` call. Edits that intersect span read
    /// regions drive bounded span dirty work via `compute_dirty_closure`.
    pending_changed_regions: Vec<RegionPattern>,
    pending_seen: FxHashSet<RegionPattern>,
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

    pub(crate) fn record_changed_region(&mut self, region: RegionPattern) {
        if self.pending_seen.insert(region) {
            self.pending_changed_regions.push(region);
        }
    }

    pub(crate) fn take_pending_changed_regions(&mut self) -> Vec<RegionPattern> {
        self.pending_seen.clear();
        std::mem::take(&mut self.pending_changed_regions)
    }

    pub(crate) fn pending_changed_region_count(&self) -> usize {
        self.pending_changed_regions.len()
    }

    pub(crate) fn mark_all_active_spans_dirty(&mut self) {
        // Conservative escape hatch: invalidate every span by recording each
        // span's result region as a changed region. Used when edit semantics
        // cannot be projected exactly (e.g. structural edits) so the next eval
        // recomputes affected spans rather than serving stale results.
        let regions: Vec<RegionPattern> = self
            .plane
            .spans
            .active_spans()
            .map(|span| RegionPattern::from_domain(span.result_region.domain()))
            .collect();
        for region in regions {
            self.record_changed_region(region);
        }
    }

    pub(crate) fn rebuild_indexes(&mut self) -> FormulaAuthorityIndexReport {
        let mut producer_results = FormulaProducerResultIndex::default();
        let mut consumer_reads = FormulaConsumerReadIndex::default();
        let mut report = FormulaAuthorityIndexReport {
            plane_epoch: self.plane.epoch().0,
            ..FormulaAuthorityIndexReport::default()
        };

        for span in self.plane.spans.active_spans() {
            report.spans_seen = report.spans_seen.saturating_add(1);
            let result_region =
                super::region_index::RegionPattern::from_domain(span.result_region.domain());
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

        self.indexes_epoch = self.indexes_epoch.saturating_add(1);
        report.indexes_epoch = self.indexes_epoch;
        self.producer_results = producer_results;
        self.consumer_reads = consumer_reads;
        report
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use formualizer_parse::parser::parse;

    use super::*;
    use crate::formula_plane::producer::{
        AxisProjection, DirtyProjectionRule, ProducerDirtyDomain, ProjectionResult,
        SpanReadDependency, SpanReadSummary, compute_dirty_closure,
    };
    use crate::formula_plane::region_index::{RegionKey, RegionPattern};
    use crate::formula_plane::runtime::{
        FormulaSpanId, NewFormulaSpan, PlacementDomain, ResultRegion,
    };

    fn template(authority: &mut FormulaAuthority) -> crate::formula_plane::ids::FormulaTemplateId {
        authority.plane.intern_template(
            Arc::<str>::from("test-template"),
            Arc::new(parse("=A1+1").unwrap()),
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
            })
            .id
    }

    #[test]
    fn authority_rebuild_indexes_span_result_regions() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let summary = SpanReadSummary {
            result_region: RegionPattern::from_domain(&domain),
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
            Some(RegionPattern::col_interval(0, 2, 0, 9))
        );
    }

    #[test]
    fn authority_rebuild_indexes_span_read_dependencies() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let result_region = RegionPattern::from_domain(&domain);
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
            .query_changed_region(RegionPattern::point(0, 5, 1));
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
            Some(RegionPattern::col_interval(0, 2, 0, 9))
        );
    }

    #[test]
    fn authority_rebuild_indexes_stale_summary_counts_without_read_entry() {
        let mut authority = FormulaAuthority::default();
        let domain = PlacementDomain::row_run(0, 0, 9, 2);
        let mismatched_result = RegionPattern::col_interval(0, 3, 0, 9);
        add_span_with_summary(
            &mut authority,
            domain,
            SpanReadSummary {
                result_region: mismatched_result,
                dependencies: vec![SpanReadDependency {
                    read_region: RegionPattern::col_interval(0, 1, 0, 9),
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

        let b_result = RegionPattern::from_domain(&b_domain);
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

        let c_result = RegionPattern::from_domain(&c_domain);
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
            [RegionPattern::point(0, 5, 0)],
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
            result_region: RegionPattern::from_domain(&domain),
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
}
