//! Mixed FormulaProducerId scheduling helpers.
//!
//! This module builds producer-bounded topological schedules across legacy and
//! FormulaPlane span producers. Schedule construction is deliberately pure: it
//! does not mutate graph dirty state, evaluate formulas, or create proxy graph
//! nodes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MixedTopologyCompileResult {
    Cached(MixedTopology),
    CacheSkipped {
        reason: MixedScheduleFallbackReason,
        observed: MixedTopologyCompileStats,
    },
}

impl MixedTopologyCompileResult {
    pub(crate) fn observed(&self) -> &MixedTopologyCompileStats {
        match self {
            Self::Cached(topology) => &topology.stats,
            Self::CacheSkipped { observed, .. } => observed,
        }
    }
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
) -> MixedTopologyCompileResult {
    let mut producers = Vec::new();
    let producer_count = producer_results.len();
    if producers.try_reserve_exact(producer_count).is_err() {
        return memory_overflow_result(
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
        return memory_overflow_result(
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
        return memory_overflow_result(
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

    if complete {
        MixedTopologyCompileResult::Cached(MixedTopology {
            relationships,
            stats,
            fallbacks,
            complete: true,
        })
    } else {
        let reason = fallbacks
            .last()
            .map(|fallback| fallback.reason)
            .unwrap_or(MixedScheduleFallbackReason::CacheMemoryExceeded);
        MixedTopologyCompileResult::CacheSkipped {
            reason,
            observed: stats,
        }
    }
}

fn memory_overflow_result(
    mut stats: MixedTopologyCompileStats,
    producer: FormulaProducerId,
    estimated_memory_bytes: usize,
) -> MixedTopologyCompileResult {
    let _ = producer;
    stats.estimated_memory_bytes = estimated_memory_bytes;
    stats.memory_overflow_count = stats.memory_overflow_count.saturating_add(1);
    MixedTopologyCompileResult::CacheSkipped {
        reason: MixedScheduleFallbackReason::CacheMemoryExceeded,
        observed: stats,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExactEdge {
    source: FormulaProducerId,
    target: FormulaProducerId,
}

const EXACT_PAGE_SIZE: usize = 128;
const EXACT_RUN_SIZE: usize = 256;

fn push_fallback_once(
    fallbacks: &mut Vec<MixedScheduleFallback>,
    producer: FormulaProducerId,
    reason: MixedScheduleFallbackReason,
) {
    if !fallbacks
        .iter()
        .any(|fallback| fallback.producer == producer && fallback.reason == reason)
    {
        fallbacks.push(MixedScheduleFallback { producer, reason });
    }
}

fn inspect_exact_read(
    item: &FormulaProducerWork,
    source_region: Region,
    read: &super::producer::FormulaConsumerReadEntry,
    scheduled: &BTreeSet<FormulaProducerId>,
    max_precise_edge_regions: usize,
    stats: &mut MixedScheduleStats,
    fallbacks: &mut Vec<MixedScheduleFallback>,
) -> Option<ExactEdge> {
    stats.consumer_candidate_count = stats.consumer_candidate_count.saturating_add(1);
    if read.consumer == item.producer
        || !scheduled.contains(&read.consumer)
        || !source_region.intersects(&read.read_region)
    {
        return None;
    }
    let changed_regions =
        edge_derivation_regions(&item.dirty, source_region, max_precise_edge_regions, stats);
    for changed in changed_regions {
        match read.projection.project_changed_region(
            changed,
            read.read_region,
            read.consumer_result_region,
        ) {
            ProjectionResult::Exact(_) | ProjectionResult::Conservative { .. } => {
                return Some(ExactEdge {
                    source: item.producer,
                    target: read.consumer,
                });
            }
            ProjectionResult::NoIntersection => {
                stats.no_intersection_candidate_count =
                    stats.no_intersection_candidate_count.saturating_add(1);
            }
            ProjectionResult::Unsupported(_) => {
                stats.unsupported_projection_count =
                    stats.unsupported_projection_count.saturating_add(1);
                push_fallback_once(
                    fallbacks,
                    read.consumer,
                    MixedScheduleFallbackReason::UnsupportedProjection,
                );
            }
        }
    }
    None
}

fn finish_schedule_from_sorted_edges<E>(
    merged_work: Vec<FormulaProducerWork>,
    edges: &[ExactEdge],
    mut stats: MixedScheduleStats,
    mut fallbacks: Vec<MixedScheduleFallback>,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<MixedSchedule, E> {
    let work_by_producer = merged_work
        .into_iter()
        .map(|item| (item.producer, item))
        .collect::<BTreeMap<_, _>>();
    let mut indegree = work_by_producer
        .keys()
        .copied()
        .map(|producer| (producer, 0usize))
        .collect::<BTreeMap<_, _>>();
    for edge in edges {
        if let Some(count) = indegree.get_mut(&edge.target) {
            *count = count.saturating_add(1);
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(producer, count)| (*count == 0).then_some(*producer))
        .collect::<Vec<_>>();
    let mut layers = Vec::new();
    let mut scheduled_count = 0usize;
    while !ready.is_empty() {
        checkpoint(u64::try_from(edges.len().saturating_add(ready.len())).unwrap_or(u64::MAX))?;
        ready.sort_unstable();
        let current = std::mem::take(&mut ready);
        let mut layer_work = Vec::with_capacity(current.len());
        for producer in current {
            scheduled_count = scheduled_count.saturating_add(1);
            if let Some(work) = work_by_producer.get(&producer) {
                layer_work.push(work.clone());
            }
            let start = edges.partition_point(|edge| edge.source < producer);
            let end = edges.partition_point(|edge| edge.source <= producer);
            for edge in &edges[start..end] {
                if let Some(count) = indegree.get_mut(&edge.target) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        ready.push(edge.target);
                    }
                }
            }
        }
        layers.push(MixedLayer { work: layer_work });
    }
    if scheduled_count < work_by_producer.len() {
        for (&producer, &count) in &indegree {
            if count > 0 {
                push_fallback_once(
                    &mut fallbacks,
                    producer,
                    MixedScheduleFallbackReason::CycleDetected,
                );
                stats.cycle_count = stats.cycle_count.saturating_add(1);
            }
        }
    }
    stats.edges_added = edges.len();
    stats.layers = layers.len();
    Ok(MixedSchedule {
        layers,
        stats,
        fallbacks,
    })
}

pub(crate) fn schedule_dirty_work_paged<E>(
    work: impl IntoIterator<Item = FormulaProducerWork>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    max_precise_edge_regions: usize,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedSchedule, u64), E> {
    let (merged_work, mut stats) = merge_work_items(work);
    let scheduled = merged_work
        .iter()
        .map(|item| item.producer)
        .collect::<BTreeSet<_>>();
    stats.unique_producers = scheduled.len();
    stats.merged_work_items = merged_work.len();
    let mut fallbacks = Vec::new();
    let mut edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>> = BTreeMap::new();
    let mut reverse_edges: BTreeMap<FormulaProducerId, FxHashSet<FormulaProducerId>> =
        BTreeMap::new();
    let mut pages = 0_u64;
    for item in &merged_work {
        let Some(source_region) = producer_results.producer_result_region(item.producer) else {
            push_fallback_once(
                &mut fallbacks,
                item.producer,
                MixedScheduleFallbackReason::MissingProducerResultRegion,
            );
            stats.missing_result_region_count = stats.missing_result_region_count.saturating_add(1);
            continue;
        };
        stats.producer_result_region_lookups =
            stats.producer_result_region_lookups.saturating_add(1);
        for start in (0..consumer_reads.len()).step_by(EXACT_PAGE_SIZE) {
            let page = consumer_reads.entries_page(start, EXACT_PAGE_SIZE);
            checkpoint(u64::try_from(page.len()).unwrap_or(u64::MAX))?;
            pages = pages.saturating_add(1);
            for read in page {
                if let Some(edge) = inspect_exact_read(
                    item,
                    source_region,
                    read,
                    &scheduled,
                    max_precise_edge_regions,
                    &mut stats,
                    &mut fallbacks,
                ) {
                    if add_edge(edge.source, edge.target, &mut edges, &mut reverse_edges) {
                        stats.edges_added = stats.edges_added.saturating_add(1);
                    } else {
                        stats.duplicate_edges_skipped =
                            stats.duplicate_edges_skipped.saturating_add(1);
                    }
                }
            }
        }
    }
    Ok((
        finish_schedule(merged_work, edges, reverse_edges, stats, fallbacks),
        pages,
    ))
}

fn merge_sorted_unique(existing: &mut Vec<ExactEdge>, run: &[ExactEdge]) {
    let old = std::mem::take(existing);
    existing.reserve(old.len().saturating_add(run.len()));
    let mut left = old.into_iter().peekable();
    let mut right = run.iter().copied().peekable();
    let mut last = None;
    while left.peek().is_some() || right.peek().is_some() {
        let next = match (left.peek(), right.peek()) {
            (Some(left), Some(right)) if left <= right => *left,
            (Some(_), Some(right)) => *right,
            (Some(left), None) => *left,
            (None, Some(right)) => *right,
            (None, None) => break,
        };
        if left.peek() == Some(&next) {
            left.next();
        }
        if right.peek() == Some(&next) {
            right.next();
        }
        if last != Some(next) {
            existing.push(next);
            last = Some(next);
        }
    }
}

pub(crate) fn schedule_dirty_work_in_memory_runs<E>(
    work: impl IntoIterator<Item = FormulaProducerWork>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    max_precise_edge_regions: usize,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedSchedule, u64), E> {
    let (merged_work, mut stats) = merge_work_items(work);
    let scheduled = merged_work
        .iter()
        .map(|item| item.producer)
        .collect::<BTreeSet<_>>();
    stats.unique_producers = scheduled.len();
    stats.merged_work_items = merged_work.len();
    let mut fallbacks = Vec::new();
    let mut run = Vec::with_capacity(EXACT_RUN_SIZE);
    let mut merged_edges = Vec::new();
    let mut raw_edges = 0usize;
    let mut runs = 0_u64;
    for item in &merged_work {
        let Some(source_region) = producer_results.producer_result_region(item.producer) else {
            push_fallback_once(
                &mut fallbacks,
                item.producer,
                MixedScheduleFallbackReason::MissingProducerResultRegion,
            );
            continue;
        };
        stats.producer_result_region_lookups =
            stats.producer_result_region_lookups.saturating_add(1);
        for read in consumer_reads.entries() {
            checkpoint(1)?;
            if let Some(edge) = inspect_exact_read(
                item,
                source_region,
                read,
                &scheduled,
                max_precise_edge_regions,
                &mut stats,
                &mut fallbacks,
            ) {
                raw_edges = raw_edges.saturating_add(1);
                run.push(edge);
                if run.len() == EXACT_RUN_SIZE {
                    run.sort_unstable();
                    run.dedup();
                    merge_sorted_unique(&mut merged_edges, &run);
                    run.clear();
                    runs = runs.saturating_add(1);
                }
            }
        }
    }
    if !run.is_empty() {
        run.sort_unstable();
        run.dedup();
        merge_sorted_unique(&mut merged_edges, &run);
        runs = runs.saturating_add(1);
    }
    stats.duplicate_edges_skipped = raw_edges.saturating_sub(merged_edges.len());
    finish_schedule_from_sorted_edges(merged_work, &merged_edges, stats, fallbacks, checkpoint)
        .map(|schedule| (schedule, runs))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn schedule_dirty_work_native<E>(
    work: impl IntoIterator<Item = FormulaProducerWork>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    max_precise_edge_regions: usize,
    file: &mut std::fs::File,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedSchedule, u64, u64), NativeExactScheduleError<E>> {
    use std::io::{Read, Seek, SeekFrom, Write};

    let (merged_work, mut stats) = merge_work_items(work);
    let producers = merged_work
        .iter()
        .map(|item| item.producer)
        .collect::<Vec<_>>();
    let ordinal = producers
        .iter()
        .copied()
        .enumerate()
        .map(|(index, producer)| (producer, index as u64))
        .collect::<BTreeMap<_, _>>();
    let scheduled = producers.iter().copied().collect::<BTreeSet<_>>();
    stats.unique_producers = scheduled.len();
    stats.merged_work_items = merged_work.len();
    let mut fallbacks = Vec::new();
    let mut edge_count = 0_u64;
    file.set_len(0).map_err(NativeExactScheduleError::Io)?;
    file.seek(SeekFrom::Start(0))
        .map_err(NativeExactScheduleError::Io)?;
    for item in &merged_work {
        let Some(source_region) = producer_results.producer_result_region(item.producer) else {
            push_fallback_once(
                &mut fallbacks,
                item.producer,
                MixedScheduleFallbackReason::MissingProducerResultRegion,
            );
            continue;
        };
        let mut targets = BTreeSet::new();
        for read in consumer_reads.entries() {
            checkpoint(1).map_err(NativeExactScheduleError::Work)?;
            if let Some(edge) = inspect_exact_read(
                item,
                source_region,
                read,
                &scheduled,
                max_precise_edge_regions,
                &mut stats,
                &mut fallbacks,
            ) {
                targets.insert(edge.target);
            }
        }
        let source = ordinal[&item.producer];
        for target in targets {
            file.write_all(&source.to_le_bytes())
                .and_then(|()| file.write_all(&ordinal[&target].to_le_bytes()))
                .map_err(NativeExactScheduleError::Io)?;
            edge_count = edge_count.saturating_add(1);
        }
    }
    file.flush().map_err(NativeExactScheduleError::Io)?;
    let topology_bytes = edge_count.saturating_mul(16);
    let mut indegree = vec![0usize; producers.len()];
    file.seek(SeekFrom::Start(0))
        .map_err(NativeExactScheduleError::Io)?;
    let mut record = [0_u8; 16];
    for _ in 0..edge_count {
        file.read_exact(&mut record)
            .map_err(NativeExactScheduleError::Io)?;
        let target = u64::from_le_bytes(record[8..16].try_into().expect("fixed edge record"));
        if let Some(count) = indegree.get_mut(target as usize) {
            *count = count.saturating_add(1);
        }
    }
    let work_by_producer = merged_work
        .into_iter()
        .map(|item| (item.producer, item))
        .collect::<BTreeMap<_, _>>();
    let mut ready = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, count)| (*count == 0).then_some(index))
        .collect::<Vec<_>>();
    let mut layers = Vec::new();
    let mut scheduled_count = 0usize;
    let mut passes = 0_u64;
    while !ready.is_empty() {
        passes = passes.saturating_add(1);
        checkpoint(edge_count.saturating_add(ready.len() as u64))
            .map_err(NativeExactScheduleError::Work)?;
        ready.sort_unstable();
        let current = std::mem::take(&mut ready);
        let current_set = current.iter().copied().collect::<BTreeSet<_>>();
        let mut layer_work = Vec::with_capacity(current.len());
        for &index in &current {
            scheduled_count = scheduled_count.saturating_add(1);
            if let Some(work) = work_by_producer.get(&producers[index]) {
                layer_work.push(work.clone());
            }
        }
        file.seek(SeekFrom::Start(0))
            .map_err(NativeExactScheduleError::Io)?;
        for _ in 0..edge_count {
            file.read_exact(&mut record)
                .map_err(NativeExactScheduleError::Io)?;
            let source = u64::from_le_bytes(record[..8].try_into().expect("fixed edge record"));
            if !current_set.contains(&(source as usize)) {
                continue;
            }
            let target = u64::from_le_bytes(record[8..].try_into().expect("fixed edge record"));
            if let Some(count) = indegree.get_mut(target as usize) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    ready.push(target as usize);
                }
            }
        }
        layers.push(MixedLayer { work: layer_work });
    }
    if scheduled_count < producers.len() {
        for (index, count) in indegree.into_iter().enumerate() {
            if count > 0 {
                push_fallback_once(
                    &mut fallbacks,
                    producers[index],
                    MixedScheduleFallbackReason::CycleDetected,
                );
                stats.cycle_count = stats.cycle_count.saturating_add(1);
            }
        }
    }
    stats.edges_added = edge_count as usize;
    stats.layers = layers.len();
    Ok((
        MixedSchedule {
            layers,
            stats,
            fallbacks,
        },
        passes,
        topology_bytes,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) enum NativeExactScheduleError<E> {
    Work(E),
    Io(std::io::Error),
}

pub(crate) fn schedule_dirty_work_repeated_passes<E>(
    work: impl IntoIterator<Item = FormulaProducerWork>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    max_precise_edge_regions: usize,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedSchedule, u64), E> {
    let (merged_work, mut stats) = merge_work_items(work);
    let work_by_producer = merged_work
        .iter()
        .cloned()
        .map(|item| (item.producer, item))
        .collect::<BTreeMap<_, _>>();
    let mut remaining = work_by_producer.keys().copied().collect::<BTreeSet<_>>();
    stats.unique_producers = remaining.len();
    stats.merged_work_items = merged_work.len();
    let mut fallbacks = Vec::new();
    let mut layers = Vec::new();
    let mut passes = 0_u64;

    while !remaining.is_empty() {
        passes = passes.saturating_add(1);
        let pass_work = remaining
            .len()
            .saturating_mul(remaining.len())
            .saturating_mul(consumer_reads.len().max(1));
        checkpoint(u64::try_from(pass_work).unwrap_or(u64::MAX))?;
        let mut ready = Vec::new();

        for &consumer in &remaining {
            let mut has_precedent = false;
            'sources: for &source in &remaining {
                if source == consumer {
                    continue;
                }
                let Some(source_region) = producer_results.producer_result_region(source) else {
                    push_fallback_once(
                        &mut fallbacks,
                        source,
                        MixedScheduleFallbackReason::MissingProducerResultRegion,
                    );
                    continue;
                };
                stats.producer_result_region_lookups =
                    stats.producer_result_region_lookups.saturating_add(1);
                let source_work = &work_by_producer[&source];
                let changed_regions = edge_derivation_regions(
                    &source_work.dirty,
                    source_region,
                    max_precise_edge_regions,
                    &mut stats,
                );
                for read in consumer_reads
                    .entries()
                    .filter(|read| read.consumer == consumer)
                {
                    stats.consumer_candidate_count =
                        stats.consumer_candidate_count.saturating_add(1);
                    if !source_region.intersects(&read.read_region) {
                        continue;
                    }
                    for changed in &changed_regions {
                        match read.projection.project_changed_region(
                            *changed,
                            read.read_region,
                            read.consumer_result_region,
                        ) {
                            ProjectionResult::Exact(_) | ProjectionResult::Conservative { .. } => {
                                stats.edges_added = stats.edges_added.saturating_add(1);
                                has_precedent = true;
                                break 'sources;
                            }
                            ProjectionResult::NoIntersection => {
                                stats.no_intersection_candidate_count =
                                    stats.no_intersection_candidate_count.saturating_add(1);
                            }
                            ProjectionResult::Unsupported(_) => {
                                stats.unsupported_projection_count =
                                    stats.unsupported_projection_count.saturating_add(1);
                                push_fallback_once(
                                    &mut fallbacks,
                                    consumer,
                                    MixedScheduleFallbackReason::UnsupportedProjection,
                                );
                            }
                        }
                    }
                }
            }
            if !has_precedent {
                ready.push(consumer);
            }
        }

        if ready.is_empty() {
            stats.cycle_count = remaining.len();
            for producer in remaining.iter().copied() {
                push_fallback_once(
                    &mut fallbacks,
                    producer,
                    MixedScheduleFallbackReason::CycleDetected,
                );
            }
            break;
        }
        let layer_work = ready
            .iter()
            .filter_map(|producer| work_by_producer.get(producer).cloned())
            .collect();
        for producer in ready {
            remaining.remove(&producer);
        }
        layers.push(MixedLayer { work: layer_work });
    }

    stats.layers = layers.len();
    Ok((
        MixedSchedule {
            layers,
            stats,
            fallbacks,
        },
        passes,
    ))
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
        let result = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_candidates: 0,
                ..MixedTopologyConfig::default()
            },
        );
        let MixedTopologyCompileResult::CacheSkipped { reason, observed } = result else {
            panic!("candidate overflow must explicitly skip the cache");
        };
        assert_eq!(reason, MixedScheduleFallbackReason::MaxCandidatesExceeded);
        assert_eq!(observed.candidate_overflow_count, 1);
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
        assert_eq!(edge_limited.observed().edge_overflow_count, 1);
        assert!(matches!(
            edge_limited,
            MixedTopologyCompileResult::CacheSkipped { .. }
        ));

        let memory_limited = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_memory_bytes: 0,
                ..MixedTopologyConfig::default()
            },
        );
        assert_eq!(memory_limited.observed().memory_overflow_count, 1);
        assert!(matches!(
            memory_limited,
            MixedTopologyCompileResult::CacheSkipped { .. }
        ));
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

        let result = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_memory_bytes: 0,
                retained_memory_bytes,
                ..MixedTopologyConfig::default()
            },
        );
        let MixedTopologyCompileResult::CacheSkipped { reason, observed } = result else {
            panic!("zero retained budget must skip");
        };
        assert_eq!(reason, MixedScheduleFallbackReason::CacheMemoryExceeded);
        assert_eq!(observed.relationships, 0);
        assert_eq!(observed.memory_overflow_count, 1);
        assert!(observed.estimated_memory_bytes >= retained_memory_bytes);
    }

    fn exact_strategy_inputs(
        duplicate_reads: usize,
    ) -> (
        FormulaProducerResultIndex,
        FormulaConsumerReadIndex,
        Vec<FormulaProducerWork>,
    ) {
        let mut results = FormulaProducerResultIndex::default();
        let source = cell(0, 0, 0);
        let target = cell(0, 0, 1);
        results.insert_producer(span(1), source);
        results.insert_producer(span(2), target);
        let mut reads = FormulaConsumerReadIndex::default();
        for _ in 0..duplicate_reads {
            reads.insert_read(span(2), source, target, DirtyProjectionRule::WholeResult);
        }
        (
            results,
            reads,
            vec![
                work(span(1), ProducerDirtyDomain::Whole),
                work(span(2), ProducerDirtyDomain::Whole),
            ],
        )
    }

    #[test]
    fn exact_paged_traverses_finite_pages_and_dedupes_edges() {
        let (results, reads, work) = exact_strategy_inputs(EXACT_PAGE_SIZE + 1);
        let (schedule, pages) =
            schedule_dirty_work_paged(work, &results, &reads, 256, |_| Ok::<_, ()>(())).unwrap();
        assert!(pages >= 4, "two producers must each traverse two pages");
        assert_eq!(schedule.stats.edges_added, 1);
        assert_eq!(schedule.layers.len(), 2);
    }

    #[test]
    fn exact_in_memory_runs_sorts_merges_and_dedupes_bounded_runs() {
        let (results, reads, work) = exact_strategy_inputs(EXACT_RUN_SIZE + 1);
        let (schedule, runs) =
            schedule_dirty_work_in_memory_runs(work, &results, &reads, 256, |_| Ok::<_, ()>(()))
                .unwrap();
        assert!(runs >= 2);
        assert_eq!(schedule.stats.edges_added, 1);
        assert_eq!(schedule.layers.len(), 2);
    }

    #[test]
    fn exact_repeated_passes_charges_quadratic_scan_body() {
        let (results, reads, work) = exact_strategy_inputs(3);
        let mut charged = 0_u64;
        let (schedule, passes) =
            schedule_dirty_work_repeated_passes(work, &results, &reads, 256, |units| {
                charged = charged.saturating_add(units);
                Ok::<_, ()>(())
            })
            .unwrap();
        assert_eq!(passes, 2);
        assert!(charged >= 15, "4*3 plus 1*3 read scans must be charged");
        assert_eq!(schedule.layers.len(), 2);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn exact_native_writes_and_consumes_topology_edge_records() {
        use std::fs::OpenOptions;

        let (results, reads, work) = exact_strategy_inputs(3);
        let path = std::env::temp_dir().join(format!(
            "formualizer-native-scheduler-test-{}.tmp",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let (schedule, passes, topology_bytes) =
            schedule_dirty_work_native(work, &results, &reads, 256, &mut file, |_| Ok::<_, ()>(()))
                .unwrap_or_else(|_| panic!("native exact schedule must succeed"));
        assert_eq!(topology_bytes, 16);
        assert_eq!(file.metadata().unwrap().len(), topology_bytes);
        assert!(passes >= 2);
        assert_eq!(schedule.layers.len(), 2);
        drop(file);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn cached_relationships_preserve_precise_dirty_placement() {
        let (results, reads) = two_producer_topology_inputs();
        let MixedTopologyCompileResult::Cached(topology) =
            compile_mixed_topology(&results, &reads, &MixedTopologyConfig::default())
        else {
            panic!("default topology must cache");
        };
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
