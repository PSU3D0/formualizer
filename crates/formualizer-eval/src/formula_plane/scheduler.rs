//! Inert mixed FormulaProducerId scheduler substrate for FP6.5R.
//!
//! This module builds a producer-bounded topological schedule over legacy and
//! FormulaPlane span producers. It is deliberately pure: it does not mutate graph
//! dirty state, call the engine scheduler, evaluate formulas, or create proxy
//! graph nodes.

use std::collections::{BTreeMap, VecDeque};

use rustc_hash::FxHashSet;

use super::producer::{
    FormulaConsumerReadIndex, FormulaProducerId, FormulaProducerResultIndex, FormulaProducerWork,
    ProducerDirtyDomain, ProjectionResult,
};
use super::region_index::RegionPattern;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MixedSchedule {
    pub(crate) layers: Vec<MixedLayer>,
    pub(crate) stats: MixedScheduleStats,
    pub(crate) fallbacks: Vec<MixedScheduleFallback>,
}

impl MixedSchedule {
    pub(crate) fn is_authoritative_safe(&self) -> bool {
        self.fallbacks.is_empty() && self.stats.cycle_count == 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MixedLayer {
    pub(crate) work: Vec<FormulaProducerWork>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MixedScheduleConfig {
    pub(crate) max_precise_edge_regions: usize,
    pub(crate) max_edges: usize,
    /// Maximum observed candidates before edge derivation halts and the schedule
    /// is marked unsafe. This is an observability/fail-closed cap; the current
    /// read-index query API still materializes a query result before this count
    /// can be inspected.
    pub(crate) max_candidates: usize,
}

impl Default for MixedScheduleConfig {
    fn default() -> Self {
        Self {
            max_precise_edge_regions: 256,
            max_edges: 100_000,
            max_candidates: 100_000,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MixedScheduleStats {
    pub(crate) input_work_items: usize,
    pub(crate) merged_work_items: usize,
    pub(crate) unique_producers: usize,
    pub(crate) producer_result_region_lookups: usize,
    pub(crate) dirty_region_queries: usize,
    pub(crate) consumer_candidate_count: usize,
    pub(crate) edges_added: usize,
    pub(crate) duplicate_edges_skipped: usize,
    pub(crate) conservative_edge_derivation_count: usize,
    pub(crate) missing_result_region_count: usize,
    pub(crate) unsupported_projection_count: usize,
    pub(crate) no_intersection_candidate_count: usize,
    pub(crate) max_edges_exceeded_count: usize,
    pub(crate) max_candidates_exceeded_count: usize,
    pub(crate) cycle_count: usize,
    pub(crate) layers: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MixedScheduleFallback {
    pub(crate) producer: FormulaProducerId,
    pub(crate) reason: MixedScheduleFallbackReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MixedScheduleFallbackReason {
    MissingProducerResultRegion,
    UnsupportedProjection,
    MaxEdgesExceeded,
    MaxCandidatesExceeded,
    CycleDetected,
}

pub(crate) fn build_mixed_schedule(
    work: impl IntoIterator<Item = FormulaProducerWork>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
) -> MixedSchedule {
    build_mixed_schedule_with_config(
        work,
        producer_results,
        consumer_reads,
        &MixedScheduleConfig::default(),
    )
}

pub(crate) fn build_mixed_schedule_with_config(
    work: impl IntoIterator<Item = FormulaProducerWork>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    config: &MixedScheduleConfig,
) -> MixedSchedule {
    let (merged_work, mut stats) = merge_work_items(work);
    let scheduled = merged_work
        .iter()
        .map(|item| item.producer)
        .collect::<FxHashSet<_>>();
    stats.unique_producers = scheduled.len();
    stats.merged_work_items = merged_work.len();

    let mut fallbacks = Vec::new();
    let mut edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>> = BTreeMap::new();
    let mut reverse_edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>> =
        BTreeMap::new();

    let mut stop_edge_derivation = false;
    for item in &merged_work {
        if stop_edge_derivation {
            break;
        }
        let producer_result_region = match producer_results.producer_result_region(item.producer) {
            Some(region) => {
                stats.producer_result_region_lookups =
                    stats.producer_result_region_lookups.saturating_add(1);
                region
            }
            None => {
                stats.producer_result_region_lookups =
                    stats.producer_result_region_lookups.saturating_add(1);
                stats.missing_result_region_count =
                    stats.missing_result_region_count.saturating_add(1);
                fallbacks.push(MixedScheduleFallback {
                    producer: item.producer,
                    reason: MixedScheduleFallbackReason::MissingProducerResultRegion,
                });
                continue;
            }
        };

        let edge_regions = edge_derivation_regions(
            &item.dirty,
            producer_result_region,
            config.max_precise_edge_regions,
            &mut stats,
        );

        for changed_region in edge_regions {
            if stop_edge_derivation {
                break;
            }
            stats.dirty_region_queries = stats.dirty_region_queries.saturating_add(1);
            let query = consumer_reads.query_changed_region(changed_region);
            stats.consumer_candidate_count = stats
                .consumer_candidate_count
                .saturating_add(query.stats.candidate_count);
            if stats.consumer_candidate_count > config.max_candidates {
                stats.max_candidates_exceeded_count =
                    stats.max_candidates_exceeded_count.saturating_add(1);
                fallbacks.push(MixedScheduleFallback {
                    producer: item.producer,
                    reason: MixedScheduleFallbackReason::MaxCandidatesExceeded,
                });
                stop_edge_derivation = true;
                break;
            }

            for matched in query.matches {
                let candidate = matched.value;
                if candidate.consumer == item.producer || !scheduled.contains(&candidate.consumer) {
                    continue;
                }
                match candidate.dirty {
                    ProjectionResult::Exact(_) | ProjectionResult::Conservative { .. } => {
                        if add_edge(
                            item.producer,
                            candidate.consumer,
                            &mut edges,
                            &mut reverse_edges,
                        ) {
                            stats.edges_added = stats.edges_added.saturating_add(1);
                            if stats.edges_added > config.max_edges {
                                stats.max_edges_exceeded_count =
                                    stats.max_edges_exceeded_count.saturating_add(1);
                                fallbacks.push(MixedScheduleFallback {
                                    producer: item.producer,
                                    reason: MixedScheduleFallbackReason::MaxEdgesExceeded,
                                });
                                stop_edge_derivation = true;
                                break;
                            }
                        } else {
                            stats.duplicate_edges_skipped =
                                stats.duplicate_edges_skipped.saturating_add(1);
                        }
                    }
                    ProjectionResult::NoIntersection => {
                        stats.no_intersection_candidate_count =
                            stats.no_intersection_candidate_count.saturating_add(1);
                    }
                    ProjectionResult::Unsupported(_) => {
                        stats.unsupported_projection_count =
                            stats.unsupported_projection_count.saturating_add(1);
                        fallbacks.push(MixedScheduleFallback {
                            producer: candidate.consumer,
                            reason: MixedScheduleFallbackReason::UnsupportedProjection,
                        });
                    }
                }
            }
        }
    }

    let mut indegree = merged_work
        .iter()
        .map(|item| (item.producer, 0usize))
        .collect::<BTreeMap<_, _>>();
    for (consumer, deps) in &reverse_edges {
        if let Some(count) = indegree.get_mut(consumer) {
            *count = deps.len();
        }
    }

    let work_by_producer = merged_work
        .into_iter()
        .map(|item| (item.producer, item))
        .collect::<BTreeMap<_, _>>();
    let mut ready = indegree
        .iter()
        .filter_map(|(producer, count)| (*count == 0).then_some(*producer))
        .collect::<VecDeque<_>>();
    let mut scheduled_count = 0usize;
    let mut layers = Vec::new();

    while !ready.is_empty() {
        let mut layer_producers = Vec::new();
        for _ in 0..ready.len() {
            if let Some(producer) = ready.pop_front() {
                layer_producers.push(producer);
            }
        }
        let mut layer_work = Vec::with_capacity(layer_producers.len());
        for producer in layer_producers {
            scheduled_count = scheduled_count.saturating_add(1);
            if let Some(work) = work_by_producer.get(&producer) {
                layer_work.push(work.clone());
            }
            if let Some(consumers) = edges.get(&producer) {
                let mut consumers = consumers.iter().copied().collect::<Vec<_>>();
                consumers.sort_unstable();
                for consumer in consumers {
                    let Some(count) = indegree.get_mut(&consumer) else {
                        continue;
                    };
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        ready.push_back(consumer);
                    }
                }
            }
        }
        layer_work.sort_by_key(|item| item.producer);
        layers.push(MixedLayer { work: layer_work });
    }

    if scheduled_count < work_by_producer.len() {
        let cyclic = indegree
            .iter()
            .filter_map(|(producer, count)| (*count > 0).then_some(*producer))
            .collect::<Vec<_>>();
        stats.cycle_count = cyclic.len();
        for producer in cyclic {
            fallbacks.push(MixedScheduleFallback {
                producer,
                reason: MixedScheduleFallbackReason::CycleDetected,
            });
        }
    }

    stats.layers = layers.len();
    MixedSchedule {
        layers,
        stats,
        fallbacks,
    }
}

fn merge_work_items(
    work: impl IntoIterator<Item = FormulaProducerWork>,
) -> (Vec<FormulaProducerWork>, MixedScheduleStats) {
    let mut stats = MixedScheduleStats::default();
    let mut by_producer: BTreeMap<FormulaProducerId, DirtyAccumulator> = BTreeMap::new();
    for item in work {
        stats.input_work_items = stats.input_work_items.saturating_add(1);
        by_producer
            .entry(item.producer)
            .or_default()
            .push(item.dirty);
    }
    (
        by_producer
            .into_iter()
            .map(|(producer, dirty)| FormulaProducerWork {
                producer,
                dirty: dirty.finish(),
            })
            .collect(),
        stats,
    )
}

#[derive(Default)]
struct DirtyAccumulator {
    whole: bool,
    cells: Vec<super::region_index::RegionKey>,
    seen_cells: FxHashSet<super::region_index::RegionKey>,
    regions: Vec<RegionPattern>,
    seen_regions: FxHashSet<RegionPattern>,
}

impl DirtyAccumulator {
    fn push(&mut self, dirty: ProducerDirtyDomain) {
        if self.whole {
            return;
        }
        match dirty {
            ProducerDirtyDomain::Whole => {
                self.whole = true;
                self.cells.clear();
                self.seen_cells.clear();
                self.regions.clear();
                self.seen_regions.clear();
            }
            ProducerDirtyDomain::Cells(cells) => {
                for cell in cells {
                    if self.seen_cells.insert(cell) {
                        self.cells.push(cell);
                    }
                }
            }
            ProducerDirtyDomain::Regions(regions) => {
                for region in regions {
                    if self.seen_regions.insert(region) {
                        self.regions.push(region);
                    }
                }
            }
        }
    }

    fn finish(self) -> ProducerDirtyDomain {
        if self.whole {
            ProducerDirtyDomain::Whole
        } else if self.regions.is_empty() {
            ProducerDirtyDomain::Cells(self.cells)
        } else {
            let mut regions = self
                .cells
                .into_iter()
                .map(RegionPattern::Point)
                .collect::<Vec<_>>();
            regions.extend(self.regions);
            ProducerDirtyDomain::Regions(regions)
        }
    }
}

fn edge_derivation_regions(
    dirty: &ProducerDirtyDomain,
    producer_result_region: RegionPattern,
    max_precise_regions: usize,
    stats: &mut MixedScheduleStats,
) -> Vec<RegionPattern> {
    let regions = dirty.result_regions(producer_result_region);
    if regions.len() > max_precise_regions {
        stats.conservative_edge_derivation_count =
            stats.conservative_edge_derivation_count.saturating_add(1);
        vec![producer_result_region]
    } else {
        regions
    }
}

fn add_edge(
    source: FormulaProducerId,
    target: FormulaProducerId,
    edges: &mut BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>>,
    reverse_edges: &mut BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>>,
) -> bool {
    let inserted = edges.entry(source).or_default().insert(target);
    if inserted {
        reverse_edges.entry(target).or_default().insert(source);
    }
    inserted
}

#[cfg(test)]
mod tests {
    use crate::engine::VertexId;
    use crate::formula_plane::producer::{AxisProjection, DirtyProjectionRule};
    use crate::formula_plane::region_index::RegionKey;
    use crate::formula_plane::runtime::FormulaSpanId;

    use super::*;

    fn span(id: u32) -> FormulaProducerId {
        FormulaProducerId::Span(FormulaSpanId(id))
    }

    fn legacy(id: u32) -> FormulaProducerId {
        FormulaProducerId::Legacy(VertexId(id))
    }

    fn cell(sheet_id: crate::SheetId, row: u32, col: u32) -> RegionPattern {
        RegionPattern::point(sheet_id, row, col)
    }

    fn work(producer: FormulaProducerId, dirty: ProducerDirtyDomain) -> FormulaProducerWork {
        FormulaProducerWork { producer, dirty }
    }

    fn left_projection() -> DirtyProjectionRule {
        DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        }
    }

    #[test]
    fn mixed_schedule_independent_producers_share_layer() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        results.insert_producer(span(1), RegionPattern::col_interval(0, 1, 0, 9));
        results.insert_producer(span(2), RegionPattern::col_interval(0, 10, 0, 9));

        let schedule = build_mixed_schedule(
            [
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 0, 1)]),
                ),
                work(
                    span(2),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 0, 10)]),
                ),
            ],
            &results,
            &reads,
        );

        assert!(schedule.is_authoritative_safe());
        assert_eq!(schedule.layers.len(), 1);
        assert_eq!(schedule.layers[0].work.len(), 2);
        assert_eq!(schedule.stats.edges_added, 0);
        // Keep `reads` mutable in this test to prove no scheduler mutation is required.
        reads.insert_read(
            span(99),
            cell(0, 0, 0),
            cell(0, 0, 99),
            DirtyProjectionRule::WholeResult,
        );
    }

    #[test]
    fn mixed_schedule_orders_span_to_span_chain() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let b_result = RegionPattern::col_interval(0, 1, 0, 9);
        let c_result = RegionPattern::col_interval(0, 2, 0, 9);
        let projection = left_projection();
        results.insert_producer(span(1), b_result);
        results.insert_producer(span(2), c_result);
        reads.insert_read(span(2), b_result, c_result, projection);

        let schedule = build_mixed_schedule(
            [
                work(
                    span(2),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 2)]),
                ),
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 1)]),
                ),
            ],
            &results,
            &reads,
        );

        assert!(schedule.is_authoritative_safe());
        assert_eq!(schedule.layers.len(), 2);
        assert_eq!(schedule.layers[0].work[0].producer, span(1));
        assert_eq!(schedule.layers[1].work[0].producer, span(2));
        assert_eq!(schedule.stats.edges_added, 1);
    }

    #[test]
    fn mixed_schedule_orders_span_to_legacy() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let b_result = RegionPattern::col_interval(0, 1, 0, 99);
        let d_result = cell(0, 0, 3);
        results.insert_producer(span(1), b_result);
        results.insert_producer(legacy(10), d_result);
        reads.insert_read(
            legacy(10),
            b_result,
            d_result,
            DirtyProjectionRule::WholeResult,
        );

        let schedule = build_mixed_schedule(
            [
                work(legacy(10), ProducerDirtyDomain::Whole),
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 50, 1)]),
                ),
            ],
            &results,
            &reads,
        );

        assert!(schedule.is_authoritative_safe());
        assert_eq!(schedule.layers[0].work[0].producer, span(1));
        assert_eq!(schedule.layers[1].work[0].producer, legacy(10));
    }

    #[test]
    fn mixed_schedule_orders_legacy_to_span() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let a_result = cell(0, 0, 0);
        let b_result = RegionPattern::col_interval(0, 1, 0, 9);
        results.insert_producer(legacy(1), a_result);
        results.insert_producer(span(2), b_result);
        reads.insert_read(
            span(2),
            a_result,
            b_result,
            DirtyProjectionRule::WholeResult,
        );

        let schedule = build_mixed_schedule(
            [
                work(span(2), ProducerDirtyDomain::Whole),
                work(legacy(1), ProducerDirtyDomain::Whole),
            ],
            &results,
            &reads,
        );

        assert!(schedule.is_authoritative_safe());
        assert_eq!(schedule.layers[0].work[0].producer, legacy(1));
        assert_eq!(schedule.layers[1].work[0].producer, span(2));
    }

    #[test]
    fn mixed_schedule_merges_duplicate_dirty_work() {
        let mut results = FormulaProducerResultIndex::default();
        let reads = FormulaConsumerReadIndex::default();
        results.insert_producer(span(1), RegionPattern::col_interval(0, 1, 0, 99));

        let schedule = build_mixed_schedule(
            [
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 1)]),
                ),
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 7, 1)]),
                ),
            ],
            &results,
            &reads,
        );

        assert_eq!(schedule.stats.input_work_items, 2);
        assert_eq!(schedule.stats.merged_work_items, 1);
        assert_eq!(schedule.layers.len(), 1);
        assert_eq!(
            schedule.layers[0].work[0].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 1), RegionKey::new(0, 7, 1)])
        );
    }

    #[test]
    fn mixed_schedule_filters_no_intersection_candidates() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let b_result = RegionPattern::col_interval(0, 1, 0, 9);
        let c_result = RegionPattern::col_interval(0, 2, 0, 9);
        results.insert_producer(span(1), b_result);
        results.insert_producer(span(2), c_result);
        reads.insert_read(
            span(2),
            RegionPattern::WholeSheet { sheet_id: 0 },
            c_result,
            left_projection(),
        );

        let schedule = build_mixed_schedule(
            [
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 50, 25)]),
                ),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
            &results,
            &reads,
        );

        assert!(schedule.is_authoritative_safe());
        assert_eq!(schedule.layers.len(), 1);
        assert_eq!(schedule.stats.no_intersection_candidate_count, 1);
        assert_eq!(schedule.stats.edges_added, 0);
    }

    #[test]
    fn mixed_schedule_records_unsupported_projection() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        results.insert_producer(span(1), cell(0, 0, 0));
        results.insert_producer(span(2), RegionPattern::WholeSheet { sheet_id: 0 });
        reads.insert_read(
            span(2),
            RegionPattern::WholeSheet { sheet_id: 0 },
            RegionPattern::WholeSheet { sheet_id: 0 },
            left_projection(),
        );

        let schedule = build_mixed_schedule(
            [
                work(span(1), ProducerDirtyDomain::Whole),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
            &results,
            &reads,
        );

        assert!(!schedule.is_authoritative_safe());
        assert_eq!(schedule.stats.unsupported_projection_count, 1);
        assert!(schedule.fallbacks.iter().any(|fallback| {
            fallback.producer == span(2)
                && fallback.reason == MixedScheduleFallbackReason::UnsupportedProjection
        }));
    }

    #[test]
    fn mixed_schedule_detects_cycles_without_proxy_nodes() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let b_result = RegionPattern::col_interval(0, 1, 0, 9);
        let c_result = RegionPattern::col_interval(0, 2, 0, 9);
        results.insert_producer(span(1), b_result);
        results.insert_producer(span(2), c_result);
        reads.insert_read(
            span(1),
            c_result,
            b_result,
            DirtyProjectionRule::WholeResult,
        );
        reads.insert_read(
            span(2),
            b_result,
            c_result,
            DirtyProjectionRule::WholeResult,
        );

        let schedule = build_mixed_schedule(
            [
                work(span(1), ProducerDirtyDomain::Whole),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
            &results,
            &reads,
        );

        assert!(!schedule.is_authoritative_safe());
        assert_eq!(schedule.layers.len(), 0);
        assert_eq!(schedule.stats.cycle_count, 2);
        assert_eq!(
            schedule
                .fallbacks
                .iter()
                .filter(|fallback| fallback.reason == MixedScheduleFallbackReason::CycleDetected)
                .count(),
            2
        );
    }

    #[test]
    fn mixed_schedule_missing_result_region_records_fallback_but_keeps_root_work() {
        let results = FormulaProducerResultIndex::default();
        let reads = FormulaConsumerReadIndex::default();
        let schedule = build_mixed_schedule(
            [work(span(1), ProducerDirtyDomain::Whole)],
            &results,
            &reads,
        );

        assert!(!schedule.is_authoritative_safe());
        assert_eq!(schedule.layers.len(), 1);
        assert_eq!(schedule.layers[0].work[0].producer, span(1));
        assert_eq!(schedule.stats.missing_result_region_count, 1);
        assert_eq!(
            schedule.fallbacks[0].reason,
            MixedScheduleFallbackReason::MissingProducerResultRegion
        );
    }

    #[test]
    fn mixed_schedule_conservative_edge_derivation_caps_sparse_queries() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let b_result = RegionPattern::col_interval(0, 1, 0, 99);
        let c_result = RegionPattern::col_interval(0, 2, 0, 99);
        results.insert_producer(span(1), b_result);
        results.insert_producer(span(2), c_result);
        reads.insert_read(
            span(2),
            b_result,
            c_result,
            DirtyProjectionRule::WholeResult,
        );

        let dirty_cells = (0..10)
            .map(|row| RegionKey::new(0, row, 1))
            .collect::<Vec<_>>();
        let schedule = build_mixed_schedule_with_config(
            [
                work(span(1), ProducerDirtyDomain::Cells(dirty_cells)),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
            &results,
            &reads,
            &MixedScheduleConfig {
                max_precise_edge_regions: 4,
                ..MixedScheduleConfig::default()
            },
        );

        assert!(schedule.is_authoritative_safe());
        assert_eq!(schedule.stats.conservative_edge_derivation_count, 1);
        assert_eq!(schedule.stats.dirty_region_queries, 2); // one full-region query + span(2) whole query
        assert_eq!(schedule.layers[0].work[0].producer, span(1));
        assert_eq!(schedule.layers[1].work[0].producer, span(2));
    }

    #[test]
    fn mixed_schedule_records_edge_cap() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let source = cell(0, 0, 0);
        results.insert_producer(span(1), source);
        for id in 2..=4 {
            let producer = span(id);
            let result = cell(0, id, 0);
            results.insert_producer(producer, result);
            reads.insert_read(producer, source, result, DirtyProjectionRule::WholeResult);
        }

        let schedule = build_mixed_schedule_with_config(
            [
                work(span(1), ProducerDirtyDomain::Whole),
                work(span(2), ProducerDirtyDomain::Whole),
                work(span(3), ProducerDirtyDomain::Whole),
                work(span(4), ProducerDirtyDomain::Whole),
            ],
            &results,
            &reads,
            &MixedScheduleConfig {
                max_precise_edge_regions: 256,
                max_edges: 1,
                max_candidates: 100,
            },
        );

        assert!(!schedule.is_authoritative_safe());
        assert!(schedule.stats.max_edges_exceeded_count > 0);
        assert!(
            schedule.fallbacks.iter().any(|fallback| {
                fallback.reason == MixedScheduleFallbackReason::MaxEdgesExceeded
            })
        );
    }

    #[test]
    fn mixed_schedule_records_candidate_cap() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let source = cell(0, 0, 0);
        results.insert_producer(span(1), source);
        for id in 2..=4 {
            let producer = span(id);
            let result = cell(0, id, 0);
            results.insert_producer(producer, result);
            reads.insert_read(producer, source, result, DirtyProjectionRule::WholeResult);
        }

        let schedule = build_mixed_schedule_with_config(
            [
                work(span(1), ProducerDirtyDomain::Whole),
                work(span(2), ProducerDirtyDomain::Whole),
                work(span(3), ProducerDirtyDomain::Whole),
                work(span(4), ProducerDirtyDomain::Whole),
            ],
            &results,
            &reads,
            &MixedScheduleConfig {
                max_precise_edge_regions: 256,
                max_edges: 100,
                max_candidates: 1,
            },
        );

        assert!(!schedule.is_authoritative_safe());
        assert!(schedule.stats.max_candidates_exceeded_count > 0);
        assert!(schedule.fallbacks.iter().any(|fallback| {
            fallback.reason == MixedScheduleFallbackReason::MaxCandidatesExceeded
        }));
    }
}
