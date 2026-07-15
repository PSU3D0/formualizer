//! Mixed FormulaProducerId scheduling helpers.
//!
//! This module builds producer-bounded topological schedules across legacy and
//! FormulaPlane span producers. Schedule construction is deliberately pure: it
//! does not mutate graph dirty state, evaluate formulas, or create proxy graph
//! nodes.

use std::collections::{BTreeMap, VecDeque};

use rustc_hash::FxHashSet;

use super::producer::{
    DirtyProjectionRule, FormulaConsumerReadIndex, FormulaProducerId, FormulaProducerResultIndex,
    FormulaProducerWork, ProducerDirtyDomain, ProjectionResult,
};
use super::region_index::{BoundedRegionQueryResult, Region};

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
    CacheMemoryExceeded,
    CycleDetected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MixedTopologyConfig {
    pub(crate) max_candidates: usize,
    pub(crate) max_edges: usize,
    pub(crate) max_memory_bytes: usize,
    /// Memory retained beside `MixedTopology` in `CachedMixedTopology`.
    pub(crate) retained_memory_bytes: usize,
}

impl Default for MixedTopologyConfig {
    fn default() -> Self {
        Self {
            max_candidates: 100_000,
            max_edges: 100_000,
            max_memory_bytes: 64 * 1024 * 1024,
            retained_memory_bytes: 0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MixedTopologyCompileStats {
    pub(crate) producers: usize,
    pub(crate) candidates: usize,
    pub(crate) relationships: usize,
    pub(crate) estimated_memory_bytes: usize,
    pub(crate) candidate_overflow_count: usize,
    pub(crate) edge_overflow_count: usize,
    pub(crate) memory_overflow_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompiledRelationship {
    target: FormulaProducerId,
    read_region: Region,
    consumer_result_region: Region,
    projection: DirtyProjectionRule,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MixedTopology {
    relationships: BTreeMap<FormulaProducerId, Vec<CompiledRelationship>>,
    pub(crate) stats: MixedTopologyCompileStats,
    pub(crate) fallbacks: Vec<MixedScheduleFallback>,
    complete: bool,
}

impl MixedTopology {
    pub(crate) fn is_complete(&self) -> bool {
        self.complete
    }

    fn estimated_memory_bytes(
        relationships: &BTreeMap<FormulaProducerId, Vec<CompiledRelationship>>,
        fallback_capacity: usize,
        retained_memory_bytes: usize,
    ) -> Option<usize> {
        const TREE_ENTRY_OVERHEAD: usize = 4 * std::mem::size_of::<usize>();
        let mut bytes = retained_memory_bytes.checked_add(std::mem::size_of::<Self>())?;
        for compiled in relationships.values() {
            bytes = bytes.checked_add(
                std::mem::size_of::<FormulaProducerId>()
                    .checked_add(std::mem::size_of::<Vec<CompiledRelationship>>())?
                    .checked_add(TREE_ENTRY_OVERHEAD)?,
            )?;
            bytes = bytes.checked_add(
                compiled
                    .capacity()
                    .checked_mul(std::mem::size_of::<CompiledRelationship>())?,
            )?;
        }
        bytes = bytes.checked_add(
            fallback_capacity.checked_mul(std::mem::size_of::<MixedScheduleFallback>())?,
        )?;
        Some(bytes)
    }
}

pub(crate) fn compile_mixed_topology(
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    config: &MixedTopologyConfig,
) -> MixedTopology {
    let mut producers = Vec::new();
    let producer_count = producer_results.len();
    if producers.try_reserve_exact(producer_count).is_err() {
        return memory_overflow_topology(
            MixedTopologyCompileStats {
                producers: producer_count,
                ..MixedTopologyCompileStats::default()
            },
            producer_results
                .producers()
                .next()
                .unwrap_or(FormulaProducerId::Legacy(crate::engine::VertexId(0))),
            usize::MAX,
        );
    }
    producers.extend(producer_results.producers());
    let mut producer_set = FxHashSet::default();
    if producer_set.try_reserve(producers.len()).is_err() {
        return memory_overflow_topology(
            MixedTopologyCompileStats {
                producers: producers.len(),
                ..MixedTopologyCompileStats::default()
            },
            producers
                .first()
                .copied()
                .unwrap_or(FormulaProducerId::Legacy(crate::engine::VertexId(0))),
            usize::MAX,
        );
    }
    producer_set.extend(producers.iter().copied());
    let mut stats = MixedTopologyCompileStats {
        producers: producers.len(),
        ..MixedTopologyCompileStats::default()
    };
    let mut relationships = BTreeMap::new();
    let mut fallbacks = Vec::new();
    let mut complete = true;
    let initial_memory = MixedTopology::estimated_memory_bytes(
        &relationships,
        fallbacks.capacity(),
        config.retained_memory_bytes,
    );
    stats.estimated_memory_bytes = initial_memory.unwrap_or(usize::MAX);
    if initial_memory.is_none() || stats.estimated_memory_bytes > config.max_memory_bytes {
        return memory_overflow_topology(
            stats,
            producers
                .first()
                .copied()
                .unwrap_or(FormulaProducerId::Legacy(crate::engine::VertexId(0))),
            initial_memory.unwrap_or(usize::MAX),
        );
    }

    for producer in producers {
        let Some(result_region) = producer_results.producer_result_region(producer) else {
            complete = false;
            fallbacks.push(MixedScheduleFallback {
                producer,
                reason: MixedScheduleFallbackReason::MissingProducerResultRegion,
            });
            break;
        };
        let remaining = config.max_candidates.saturating_sub(stats.candidates);
        let query = consumer_reads.query_changed_region_bounded(result_region, remaining);
        let result = match query {
            BoundedRegionQueryResult::Incomplete {
                observed_candidates,
            } => {
                stats.candidates = stats.candidates.saturating_add(observed_candidates);
                stats.candidate_overflow_count = stats.candidate_overflow_count.saturating_add(1);
                complete = false;
                fallbacks.push(MixedScheduleFallback {
                    producer,
                    reason: MixedScheduleFallbackReason::MaxCandidatesExceeded,
                });
                break;
            }
            BoundedRegionQueryResult::Complete(result) => result,
        };
        stats.candidates = stats
            .candidates
            .saturating_add(result.stats.candidate_count);
        for matched in result.matches {
            let candidate = matched.value;
            if candidate.consumer == producer || !producer_set.contains(&candidate.consumer) {
                continue;
            }
            match candidate.dirty {
                ProjectionResult::NoIntersection => continue,
                ProjectionResult::Unsupported(_) => {
                    complete = false;
                    fallbacks.push(MixedScheduleFallback {
                        producer: candidate.consumer,
                        reason: MixedScheduleFallbackReason::UnsupportedProjection,
                    });
                    break;
                }
                ProjectionResult::Exact(_) | ProjectionResult::Conservative { .. } => {}
            }
            stats.relationships = stats.relationships.saturating_add(1);
            if stats.relationships > config.max_edges {
                stats.edge_overflow_count = stats.edge_overflow_count.saturating_add(1);
                complete = false;
                fallbacks.push(MixedScheduleFallback {
                    producer,
                    reason: MixedScheduleFallbackReason::MaxEdgesExceeded,
                });
                break;
            }
            let compiled = relationships.entry(producer).or_insert_with(Vec::new);
            if compiled.try_reserve(1).is_err() {
                stats.estimated_memory_bytes = usize::MAX;
                stats.memory_overflow_count = stats.memory_overflow_count.saturating_add(1);
                complete = false;
                fallbacks.push(MixedScheduleFallback {
                    producer,
                    reason: MixedScheduleFallbackReason::CacheMemoryExceeded,
                });
                break;
            }
            compiled.push(CompiledRelationship {
                target: candidate.consumer,
                read_region: candidate.read_region,
                consumer_result_region: candidate.consumer_result_region,
                projection: candidate.projection,
            });
            let estimated = MixedTopology::estimated_memory_bytes(
                &relationships,
                fallbacks.capacity(),
                config.retained_memory_bytes,
            );
            stats.estimated_memory_bytes = estimated.unwrap_or(usize::MAX);
            if estimated.is_none() || stats.estimated_memory_bytes > config.max_memory_bytes {
                stats.memory_overflow_count = stats.memory_overflow_count.saturating_add(1);
                complete = false;
                fallbacks.push(MixedScheduleFallback {
                    producer,
                    reason: MixedScheduleFallbackReason::CacheMemoryExceeded,
                });
                break;
            }
        }
        if !complete {
            break;
        }
    }

    if !complete {
        relationships.clear();
    }
    MixedTopology {
        relationships,
        stats,
        fallbacks,
        complete,
    }
}

fn memory_overflow_topology(
    mut stats: MixedTopologyCompileStats,
    producer: FormulaProducerId,
    estimated_memory_bytes: usize,
) -> MixedTopology {
    stats.estimated_memory_bytes = estimated_memory_bytes;
    stats.memory_overflow_count = stats.memory_overflow_count.saturating_add(1);
    let mut fallbacks = Vec::new();
    let _ = fallbacks.try_reserve_exact(1);
    fallbacks.push(MixedScheduleFallback {
        producer,
        reason: MixedScheduleFallbackReason::CacheMemoryExceeded,
    });
    MixedTopology {
        relationships: BTreeMap::new(),
        stats,
        fallbacks,
        complete: false,
    }
}

pub(crate) fn schedule_dirty_work(
    work: impl IntoIterator<Item = FormulaProducerWork>,
    producer_results: &FormulaProducerResultIndex,
    topology: &MixedTopology,
    max_precise_edge_regions: usize,
) -> MixedSchedule {
    let (merged_work, mut stats) = merge_work_items(work);
    let scheduled = merged_work
        .iter()
        .map(|item| item.producer)
        .collect::<FxHashSet<_>>();
    stats.unique_producers = scheduled.len();
    stats.merged_work_items = merged_work.len();
    let mut fallbacks = topology.fallbacks.clone();
    let mut edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>> = BTreeMap::new();
    let mut reverse_edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>> =
        BTreeMap::new();

    if topology.complete {
        for item in &merged_work {
            let Some(result_region) = producer_results.producer_result_region(item.producer) else {
                fallbacks.push(MixedScheduleFallback {
                    producer: item.producer,
                    reason: MixedScheduleFallbackReason::MissingProducerResultRegion,
                });
                continue;
            };
            let changed_regions = edge_derivation_regions(
                &item.dirty,
                result_region,
                max_precise_edge_regions,
                &mut stats,
            );
            for relationship in topology
                .relationships
                .get(&item.producer)
                .into_iter()
                .flatten()
            {
                if !scheduled.contains(&relationship.target) {
                    continue;
                }
                let mut applies = false;
                for changed in &changed_regions {
                    match relationship.projection.project_changed_region(
                        *changed,
                        relationship.read_region,
                        relationship.consumer_result_region,
                    ) {
                        ProjectionResult::Exact(_) | ProjectionResult::Conservative { .. } => {
                            applies = true;
                            break;
                        }
                        ProjectionResult::NoIntersection => {}
                        ProjectionResult::Unsupported(_) => {
                            fallbacks.push(MixedScheduleFallback {
                                producer: relationship.target,
                                reason: MixedScheduleFallbackReason::UnsupportedProjection,
                            });
                            break;
                        }
                    }
                }
                if applies
                    && add_edge(
                        item.producer,
                        relationship.target,
                        &mut edges,
                        &mut reverse_edges,
                    )
                {
                    stats.edges_added = stats.edges_added.saturating_add(1);
                }
            }
        }
    }

    finish_schedule(merged_work, edges, reverse_edges, stats, fallbacks)
}

fn finish_schedule(
    merged_work: Vec<FormulaProducerWork>,
    edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>>,
    reverse_edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>>,
    mut stats: MixedScheduleStats,
    mut fallbacks: Vec<MixedScheduleFallback>,
) -> MixedSchedule {
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
        let layer_len = ready.len();
        let mut layer_work = Vec::with_capacity(layer_len);
        for _ in 0..layer_len {
            let Some(producer) = ready.pop_front() else {
                continue;
            };
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
    regions: Vec<Region>,
    seen_regions: FxHashSet<Region>,
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
                .map(|key| Region::point(key.sheet_id, key.row, key.col))
                .collect::<Vec<_>>();
            regions.extend(self.regions);
            ProducerDirtyDomain::Regions(regions)
        }
    }
}

fn edge_derivation_regions(
    dirty: &ProducerDirtyDomain,
    producer_result_region: Region,
    max_precise_regions: usize,
    stats: &mut MixedScheduleStats,
) -> Vec<Region> {
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

    fn cell(sheet_id: crate::SheetId, row: u32, col: u32) -> Region {
        Region::point(sheet_id, row, col)
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
        results.insert_producer(span(1), Region::col_interval(0, 1, 0, 9));
        results.insert_producer(span(2), Region::col_interval(0, 10, 0, 9));

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
        let b_result = Region::col_interval(0, 1, 0, 9);
        let c_result = Region::col_interval(0, 2, 0, 9);
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
        let b_result = Region::col_interval(0, 1, 0, 99);
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
        let b_result = Region::col_interval(0, 1, 0, 9);
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
        results.insert_producer(span(1), Region::col_interval(0, 1, 0, 99));

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
        let b_result = Region::col_interval(0, 1, 0, 9);
        let c_result = Region::col_interval(0, 2, 0, 9);
        results.insert_producer(span(1), b_result);
        results.insert_producer(span(2), c_result);
        reads.insert_read(span(2), Region::whole_sheet(0), c_result, left_projection());

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
        results.insert_producer(span(2), Region::whole_sheet(0));
        reads.insert_read(
            span(2),
            Region::whole_sheet(0),
            Region::whole_sheet(0),
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
        let b_result = Region::col_interval(0, 1, 0, 9);
        let c_result = Region::col_interval(0, 2, 0, 9);
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
        let b_result = Region::col_interval(0, 1, 0, 99);
        let c_result = Region::col_interval(0, 2, 0, 99);
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

    fn two_producer_topology_inputs() -> (FormulaProducerResultIndex, FormulaConsumerReadIndex) {
        let mut results = FormulaProducerResultIndex::default();
        results.insert_producer(span(1), Region::col_interval(0, 0, 0, 2));
        results.insert_producer(span(2), cell(0, 10, 0));
        let mut reads = FormulaConsumerReadIndex::default();
        reads.insert_read(
            span(2),
            cell(0, 1, 0),
            cell(0, 10, 0),
            DirtyProjectionRule::WholeResult,
        );
        (results, reads)
    }

    #[test]
    fn compiled_topology_candidate_overflow_is_atomic() {
        let (results, reads) = two_producer_topology_inputs();
        let topology = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_candidates: 0,
                ..MixedTopologyConfig::default()
            },
        );
        assert!(!topology.is_complete());
        assert_eq!(topology.stats.candidate_overflow_count, 1);
        let schedule = schedule_dirty_work(
            [
                work(span(1), ProducerDirtyDomain::Whole),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
            &results,
            &topology,
            256,
        );
        assert!(!schedule.is_authoritative_safe());
        assert_eq!(schedule.layers.len(), 1, "partial edges must not escape");
    }

    #[test]
    fn compiled_topology_edge_and_memory_caps_fail_closed() {
        let (results, reads) = two_producer_topology_inputs();
        let edge_limited = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_edges: 0,
                ..MixedTopologyConfig::default()
            },
        );
        assert!(!edge_limited.is_complete());
        assert_eq!(edge_limited.stats.edge_overflow_count, 1);

        let memory_limited = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_memory_bytes: 0,
                ..MixedTopologyConfig::default()
            },
        );
        assert!(!memory_limited.is_complete());
        assert_eq!(memory_limited.stats.memory_overflow_count, 1);
    }

    #[test]
    fn zero_memory_budget_rejects_large_disjoint_retained_indexes() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        for id in 1..=5_000 {
            let producer = span(id);
            let result = Region::point(0, id, 0);
            results.insert_producer(producer, result);
            reads.insert_read(
                producer,
                Region::point(0, id, 100),
                result,
                DirtyProjectionRule::WholeResult,
            );
        }
        let retained_memory_bytes = results
            .estimated_memory_bytes()
            .and_then(|bytes| bytes.checked_add(reads.estimated_memory_bytes()?))
            .and_then(|bytes| {
                bytes.checked_add(5_000usize.checked_mul(
                    std::mem::size_of::<FormulaProducerId>()
                        + std::mem::size_of::<crate::formula_plane::runtime::FormulaSpanRef>()
                        + 4 * std::mem::size_of::<usize>(),
                )?)
            })
            .expect("bounded test accounting");

        let topology = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_memory_bytes: 0,
                retained_memory_bytes,
                ..MixedTopologyConfig::default()
            },
        );

        assert!(!topology.is_complete());
        assert_eq!(topology.stats.relationships, 0);
        assert_eq!(topology.stats.memory_overflow_count, 1);
        assert!(topology.stats.estimated_memory_bytes >= retained_memory_bytes);
        assert!(topology.fallbacks.iter().any(|fallback| {
            fallback.reason == MixedScheduleFallbackReason::CacheMemoryExceeded
        }));
    }

    #[test]
    fn cached_relationships_preserve_precise_dirty_placement() {
        let (results, reads) = two_producer_topology_inputs();
        let topology = compile_mixed_topology(&results, &reads, &MixedTopologyConfig::default());
        assert!(topology.is_complete());
        let unrelated = schedule_dirty_work(
            [
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 0, 0)]),
                ),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
            &results,
            &topology,
            256,
        );
        assert_eq!(unrelated.layers.len(), 1);

        let related = schedule_dirty_work(
            [
                work(
                    span(1),
                    ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 1, 0)]),
                ),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
            &results,
            &topology,
            256,
        );
        assert_eq!(related.layers.len(), 2);
    }
}
