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
struct CompiledPrecedent {
    source: FormulaProducerId,
    read_region: Region,
    consumer_result_region: Region,
    projection: DirtyProjectionRule,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MixedTopology {
    relationships: BTreeMap<FormulaProducerId, Vec<CompiledRelationship>>,
    precedents: BTreeMap<FormulaProducerId, Vec<CompiledPrecedent>>,
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
        precedents: &BTreeMap<FormulaProducerId, Vec<CompiledPrecedent>>,
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
        for compiled in precedents.values() {
            bytes = bytes.checked_add(
                std::mem::size_of::<FormulaProducerId>()
                    .checked_add(std::mem::size_of::<Vec<CompiledPrecedent>>())?
                    .checked_add(TREE_ENTRY_OVERHEAD)?,
            )?;
            bytes = bytes.checked_add(
                compiled
                    .capacity()
                    .checked_mul(std::mem::size_of::<CompiledPrecedent>())?,
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
    let mut precedents = BTreeMap::new();
    let mut fallbacks = Vec::new();
    let mut complete = true;
    let initial_memory = MixedTopology::estimated_memory_bytes(
        &relationships,
        &precedents,
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
            let precedent = precedents
                .entry(candidate.consumer)
                .or_insert_with(Vec::new);
            if precedent.try_reserve(1).is_err() {
                stats.estimated_memory_bytes = usize::MAX;
                stats.memory_overflow_count = stats.memory_overflow_count.saturating_add(1);
                complete = false;
                fallbacks.push(MixedScheduleFallback {
                    producer,
                    reason: MixedScheduleFallbackReason::CacheMemoryExceeded,
                });
                break;
            }
            precedent.push(CompiledPrecedent {
                source: producer,
                read_region: candidate.read_region,
                consumer_result_region: candidate.consumer_result_region,
                projection: candidate.projection,
            });
            let estimated = MixedTopology::estimated_memory_bytes(
                &relationships,
                &precedents,
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
            precedents,
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MixedDemandClosure {
    demanded: BTreeMap<FormulaProducerId, Vec<Region>>,
    pub(crate) producer_visits: usize,
    pub(crate) relationship_visits: usize,
}

impl MixedDemandClosure {
    pub(crate) fn contains(&self, producer: FormulaProducerId) -> bool {
        self.demanded.contains_key(&producer)
    }

    pub(crate) fn regions(&self, producer: FormulaProducerId) -> &[Region] {
        self.demanded
            .get(&producer)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn producers(&self) -> impl Iterator<Item = FormulaProducerId> + '_ {
        self.demanded.keys().copied()
    }

    pub(crate) fn estimated_memory_bytes(&self) -> u64 {
        const TREE_ENTRY_OVERHEAD: usize = 4 * std::mem::size_of::<usize>();
        self.demanded.values().fold(0usize, |bytes, regions| {
            bytes
                .saturating_add(std::mem::size_of::<FormulaProducerId>())
                .saturating_add(std::mem::size_of::<Vec<Region>>())
                .saturating_add(TREE_ENTRY_OVERHEAD)
                .saturating_add(
                    regions
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Region>()),
                )
        }) as u64
    }
}

fn insert_demand_region(
    demanded: &mut BTreeMap<FormulaProducerId, Vec<Region>>,
    queue: &mut VecDeque<(FormulaProducerId, Region)>,
    producer: FormulaProducerId,
    region: Region,
) {
    let regions = demanded.entry(producer).or_default();
    if regions.contains(&region) {
        return;
    }
    regions.push(region);
    queue.push_back((producer, region));
}

fn demanded_read_regions(
    demanded_result: Region,
    declared_read: Region,
    consumer_result: Region,
    projection: DirtyProjectionRule,
) -> Vec<Region> {
    let Some(demanded_result) = demanded_result.intersection(consumer_result) else {
        return Vec::new();
    };
    let projected = match projection {
        DirtyProjectionRule::WholeResult => vec![declared_read],
        _ => projection
            .read_regions_for_result(declared_read.sheet_id(), demanded_result)
            .unwrap_or_else(|_| vec![declared_read]),
    };
    projected
        .into_iter()
        .filter_map(|region| region.intersection(declared_read))
        .collect()
}

pub(crate) fn build_demand_closure_cached(
    roots: impl IntoIterator<Item = (FormulaProducerId, Region)>,
    producer_results: &FormulaProducerResultIndex,
    topology: &MixedTopology,
) -> MixedDemandClosure {
    debug_assert!(
        topology.complete,
        "partial retained topology is never demand authority"
    );
    let mut closure = MixedDemandClosure::default();
    let mut queue = VecDeque::new();
    for (producer, demanded) in roots {
        if let Some(result) = producer_results.producer_result_region(producer)
            && let Some(region) = demanded.intersection(result)
        {
            insert_demand_region(&mut closure.demanded, &mut queue, producer, region);
        }
    }
    while let Some((consumer, demanded_result)) = queue.pop_front() {
        closure.producer_visits = closure.producer_visits.saturating_add(1);
        for precedent in topology.precedents.get(&consumer).into_iter().flatten() {
            closure.relationship_visits = closure.relationship_visits.saturating_add(1);
            let Some(source_result) = producer_results.producer_result_region(precedent.source)
            else {
                continue;
            };
            for read in demanded_read_regions(
                demanded_result,
                precedent.read_region,
                precedent.consumer_result_region,
                precedent.projection,
            ) {
                if let Some(source_demand) = source_result.intersection(read) {
                    insert_demand_region(
                        &mut closure.demanded,
                        &mut queue,
                        precedent.source,
                        source_demand,
                    );
                }
            }
        }
    }
    closure
}

fn initialize_exact_demand(
    roots: impl IntoIterator<Item = (FormulaProducerId, Region)>,
    producer_results: &FormulaProducerResultIndex,
) -> (MixedDemandClosure, VecDeque<(FormulaProducerId, Region)>) {
    let mut closure = MixedDemandClosure::default();
    let mut queue = VecDeque::new();
    for (producer, demanded) in roots {
        if let Some(result) = producer_results.producer_result_region(producer)
            && let Some(region) = demanded.intersection(result)
        {
            insert_demand_region(&mut closure.demanded, &mut queue, producer, region);
        }
    }
    (closure, queue)
}

pub(crate) fn build_demand_closure_paged<E>(
    roots: impl IntoIterator<Item = (FormulaProducerId, Region)>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedDemandClosure, u64), E> {
    let mut indexed_pages = Vec::new();
    for start in (0..consumer_reads.len()).step_by(EXACT_PAGE_SIZE) {
        let page = consumer_reads.entries_page(start, EXACT_PAGE_SIZE);
        checkpoint(u64::try_from(page.len()).unwrap_or(u64::MAX))?;
        let mut by_consumer: BTreeMap<FormulaProducerId, Vec<_>> = BTreeMap::new();
        for read in page {
            by_consumer.entry(read.consumer).or_default().push(read);
        }
        indexed_pages.push(by_consumer);
    }

    let (mut closure, mut queue) = initialize_exact_demand(roots, producer_results);
    let mut page_visits = 0_u64;
    while let Some((consumer, demanded_result)) = queue.pop_front() {
        closure.producer_visits = closure.producer_visits.saturating_add(1);
        for page in &indexed_pages {
            let reads = page.get(&consumer).map(Vec::as_slice).unwrap_or_default();
            checkpoint(u64::try_from(reads.len()).unwrap_or(u64::MAX))?;
            page_visits = page_visits.saturating_add(1);
            inspect_exact_demand_reads(
                reads.iter().copied(),
                consumer,
                demanded_result,
                producer_results,
                &mut closure,
                &mut queue,
            );
        }
    }
    Ok((closure, page_visits))
}

pub(crate) fn build_demand_closure_in_memory_runs<E>(
    roots: impl IntoIterator<Item = (FormulaProducerId, Region)>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedDemandClosure, u64), E> {
    let mut runs = Vec::new();
    for start in (0..consumer_reads.len()).step_by(EXACT_RUN_SIZE) {
        let mut run = consumer_reads
            .entries_page(start, EXACT_RUN_SIZE)
            .iter()
            .collect::<Vec<_>>();
        checkpoint(u64::try_from(run.len()).unwrap_or(u64::MAX))?;
        run.sort_unstable_by_key(|read| read.consumer);
        runs.push(run);
    }

    let (mut closure, mut queue) = initialize_exact_demand(roots, producer_results);
    let mut passes = 0_u64;
    while let Some((consumer, demanded_result)) = queue.pop_front() {
        closure.producer_visits = closure.producer_visits.saturating_add(1);
        for run in &runs {
            let start = run.partition_point(|read| read.consumer < consumer);
            let end = run.partition_point(|read| read.consumer <= consumer);
            checkpoint(u64::try_from(end.saturating_sub(start)).unwrap_or(u64::MAX))?;
            passes = passes.saturating_add(1);
            inspect_exact_demand_reads(
                run[start..end].iter().copied(),
                consumer,
                demanded_result,
                producer_results,
                &mut closure,
                &mut queue,
            );
        }
    }
    Ok((closure, passes))
}

pub(crate) fn build_demand_closure_repeated_passes<E>(
    roots: impl IntoIterator<Item = (FormulaProducerId, Region)>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedDemandClosure, u64), E> {
    let (mut closure, mut queue) = initialize_exact_demand(roots, producer_results);
    let mut passes = 0_u64;
    while let Some((consumer, demanded_result)) = queue.pop_front() {
        closure.producer_visits = closure.producer_visits.saturating_add(1);
        for start in (0..consumer_reads.len()).step_by(EXACT_PAGE_SIZE) {
            let page = consumer_reads.entries_page(start, EXACT_PAGE_SIZE);
            checkpoint(u64::try_from(page.len()).unwrap_or(u64::MAX))?;
            passes = passes.saturating_add(1);
            inspect_exact_demand_reads(
                page.iter(),
                consumer,
                demanded_result,
                producer_results,
                &mut closure,
                &mut queue,
            );
        }
    }
    Ok((closure, passes))
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub(crate) enum NativeExactDemandError<E> {
    Work(E),
    Io(std::io::Error),
}

const NATIVE_CONSUMER_RECORD_BYTES: u64 = 16;
const NATIVE_CONSUMER_RUN_RECORDS: usize = 256;
// Includes the bounded sort run plus request-owned paths, handles, and merge buffers.
pub(crate) const NATIVE_EXACT_DEMAND_SCRATCH_BYTES: u64 = (NATIVE_CONSUMER_RUN_RECORDS as u64)
    * NATIVE_CONSUMER_RECORD_BYTES
    + std::mem::size_of::<Vec<(u64, u64)>>() as u64
    + 4 * NATIVE_CONSUMER_RECORD_BYTES
    + 1024;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NativeConsumerRecord {
    consumer_key: u64,
    entry_index: u64,
}

#[cfg(not(target_arch = "wasm32"))]
fn native_consumer_key(consumer: FormulaProducerId) -> u64 {
    match consumer {
        FormulaProducerId::Legacy(vertex) => u64::from(vertex.0),
        FormulaProducerId::Span(span) => (1_u64 << 32) | u64::from(span.0),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn write_native_consumer_record(
    file: &mut std::fs::File,
    record: NativeConsumerRecord,
) -> std::io::Result<()> {
    use std::io::Write;
    file.write_all(&record.consumer_key.to_le_bytes())?;
    file.write_all(&record.entry_index.to_le_bytes())
}

#[cfg(not(target_arch = "wasm32"))]
fn read_native_consumer_record_at(
    file: &mut std::fs::File,
    index: u64,
) -> std::io::Result<NativeConsumerRecord> {
    use std::io::{Read, Seek, SeekFrom};
    let offset = index
        .checked_mul(NATIVE_CONSUMER_RECORD_BYTES)
        .ok_or_else(|| std::io::Error::other("native consumer index offset overflow"))?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0_u8; NATIVE_CONSUMER_RECORD_BYTES as usize];
    file.read_exact(&mut bytes)?;
    Ok(NativeConsumerRecord {
        consumer_key: u64::from_le_bytes(bytes[..8].try_into().expect("fixed consumer key")),
        entry_index: u64::from_le_bytes(bytes[8..].try_into().expect("fixed entry index")),
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn merge_native_consumer_runs(
    source: &mut std::fs::File,
    destination: &mut std::fs::File,
    record_count: u64,
    run_width: u64,
) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    destination.set_len(0)?;
    destination.seek(SeekFrom::Start(0))?;
    let pair_width = run_width
        .checked_mul(2)
        .ok_or_else(|| std::io::Error::other("native consumer run width overflow"))?;
    let mut pair_start = 0_u64;
    while pair_start < record_count {
        let left_end = pair_start.saturating_add(run_width).min(record_count);
        let right_end = pair_start.saturating_add(pair_width).min(record_count);
        let mut left = pair_start;
        let mut right = left_end;
        let mut left_record = (left < left_end)
            .then(|| read_native_consumer_record_at(source, left))
            .transpose()?;
        let mut right_record = (right < right_end)
            .then(|| read_native_consumer_record_at(source, right))
            .transpose()?;
        while left_record.is_some() || right_record.is_some() {
            let take_left = match (left_record, right_record) {
                (Some(left), Some(right)) => left <= right,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let record = if take_left {
                let record = left_record.take().expect("left merge record");
                left = left.saturating_add(1);
                left_record = (left < left_end)
                    .then(|| read_native_consumer_record_at(source, left))
                    .transpose()?;
                record
            } else {
                let record = right_record.take().expect("right merge record");
                right = right.saturating_add(1);
                right_record = (right < right_end)
                    .then(|| read_native_consumer_record_at(source, right))
                    .transpose()?;
                record
            };
            write_native_consumer_record(destination, record)?;
        }
        pair_start = pair_start.saturating_add(pair_width);
    }
    destination.flush()
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn build_demand_closure_native<E>(
    roots: impl IntoIterator<Item = (FormulaProducerId, Region)>,
    producer_results: &FormulaProducerResultIndex,
    consumer_reads: &FormulaConsumerReadIndex,
    file: &mut std::fs::File,
    auxiliary_file: &mut std::fs::File,
    mut checkpoint: impl FnMut(u64) -> Result<(), E>,
) -> Result<(MixedDemandClosure, u64, u64), NativeExactDemandError<E>> {
    use std::io::{Seek, SeekFrom, Write};

    file.set_len(0).map_err(NativeExactDemandError::Io)?;
    auxiliary_file
        .set_len(0)
        .map_err(NativeExactDemandError::Io)?;
    file.seek(SeekFrom::Start(0))
        .map_err(NativeExactDemandError::Io)?;
    let mut run = Vec::new();
    run.try_reserve_exact(NATIVE_CONSUMER_RUN_RECORDS)
        .map_err(|_| {
            NativeExactDemandError::Io(std::io::Error::other(
                "native consumer run reservation failed",
            ))
        })?;
    for start in (0..consumer_reads.len()).step_by(NATIVE_CONSUMER_RUN_RECORDS) {
        let page = consumer_reads.entries_page(start, NATIVE_CONSUMER_RUN_RECORDS);
        run.clear();
        run.extend(
            page.iter()
                .enumerate()
                .map(|(offset, read)| NativeConsumerRecord {
                    consumer_key: native_consumer_key(read.consumer),
                    entry_index: start.saturating_add(offset) as u64,
                }),
        );
        run.sort_unstable();
        checkpoint(run.len() as u64).map_err(NativeExactDemandError::Work)?;
        for record in run.iter().copied() {
            write_native_consumer_record(file, record).map_err(NativeExactDemandError::Io)?;
        }
    }
    file.flush().map_err(NativeExactDemandError::Io)?;

    let record_count = consumer_reads.len() as u64;
    let record_bytes = record_count.saturating_mul(NATIVE_CONSUMER_RECORD_BYTES);
    let mut peak_disk_bytes = record_bytes;
    let mut run_width = NATIVE_CONSUMER_RUN_RECORDS as u64;
    let mut index_in_primary = true;
    while run_width < record_count {
        checkpoint(record_count).map_err(NativeExactDemandError::Work)?;
        if index_in_primary {
            merge_native_consumer_runs(file, auxiliary_file, record_count, run_width)
                .map_err(NativeExactDemandError::Io)?;
        } else {
            merge_native_consumer_runs(auxiliary_file, file, record_count, run_width)
                .map_err(NativeExactDemandError::Io)?;
        }
        peak_disk_bytes = peak_disk_bytes.max(record_bytes.saturating_mul(2));
        index_in_primary = !index_in_primary;
        run_width = run_width.saturating_mul(2);
    }
    let (index_file, obsolete_file) = if index_in_primary {
        (&mut *file, &mut *auxiliary_file)
    } else {
        (&mut *auxiliary_file, &mut *file)
    };
    obsolete_file
        .set_len(0)
        .map_err(NativeExactDemandError::Io)?;

    let (mut closure, mut queue) = initialize_exact_demand(roots, producer_results);
    let mut searches = 0_u64;
    while let Some((consumer, demanded_result)) = queue.pop_front() {
        closure.producer_visits = closure.producer_visits.saturating_add(1);
        let consumer_key = native_consumer_key(consumer);
        let mut lower = 0_u64;
        let mut upper = record_count;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            checkpoint(1).map_err(NativeExactDemandError::Work)?;
            let record = read_native_consumer_record_at(index_file, middle)
                .map_err(NativeExactDemandError::Io)?;
            if record.consumer_key < consumer_key {
                lower = middle.saturating_add(1);
            } else {
                upper = middle;
            }
        }
        let first = lower;
        upper = record_count;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            checkpoint(1).map_err(NativeExactDemandError::Work)?;
            let record = read_native_consumer_record_at(index_file, middle)
                .map_err(NativeExactDemandError::Io)?;
            if record.consumer_key <= consumer_key {
                lower = middle.saturating_add(1);
            } else {
                upper = middle;
            }
        }
        for index in first..lower {
            checkpoint(1).map_err(NativeExactDemandError::Work)?;
            let record = read_native_consumer_record_at(index_file, index)
                .map_err(NativeExactDemandError::Io)?;
            let Some(read) = usize::try_from(record.entry_index)
                .ok()
                .and_then(|entry| consumer_reads.entry(entry))
            else {
                continue;
            };
            inspect_exact_demand_reads(
                std::iter::once(read),
                consumer,
                demanded_result,
                producer_results,
                &mut closure,
                &mut queue,
            );
        }
        searches = searches.saturating_add(1);
    }
    Ok((closure, searches, peak_disk_bytes))
}

fn inspect_exact_demand_reads<'a>(
    reads: impl IntoIterator<Item = &'a super::producer::FormulaConsumerReadEntry>,
    consumer: FormulaProducerId,
    demanded_result: Region,
    producer_results: &FormulaProducerResultIndex,
    closure: &mut MixedDemandClosure,
    queue: &mut VecDeque<(FormulaProducerId, Region)>,
) {
    for read in reads {
        if read.consumer != consumer {
            continue;
        }
        closure.relationship_visits = closure.relationship_visits.saturating_add(1);
        for demanded_read in demanded_read_regions(
            demanded_result,
            read.read_region,
            read.consumer_result_region,
            read.projection,
        ) {
            for matched in producer_results.query(demanded_read).matches {
                let source = matched.value.producer;
                if source == consumer {
                    continue;
                }
                if let Some(source_demand) = matched.value.result_region.intersection(demanded_read)
                {
                    insert_demand_region(&mut closure.demanded, queue, source, source_demand);
                }
            }
        }
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

    #[cfg(not(target_arch = "wasm32"))]
    fn native_test_files(
        label: &str,
    ) -> (
        std::path::PathBuf,
        std::fs::File,
        std::path::PathBuf,
        std::fs::File,
    ) {
        let path = std::env::temp_dir().join(format!(
            "formualizer-native-{label}-{}.tmp",
            std::process::id()
        ));
        let auxiliary_path = path.with_extension("aux.tmp");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&auxiliary_path);
        let open = |path: &std::path::Path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap()
        };
        let file = open(&path);
        let auxiliary_file = open(&auxiliary_path);
        (path, file, auxiliary_path, auxiliary_file)
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
    fn cached_and_exact_demand_builders_match_full_closure_oracle() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let a = Region::col_interval(0, 0, 0, 99);
        let b = Region::col_interval(0, 1, 0, 99);
        let c = Region::col_interval(0, 2, 0, 99);
        results.insert_producer(span(1), a);
        results.insert_producer(span(2), b);
        results.insert_producer(span(3), c);
        reads.insert_read(span(2), a, b, left_projection());
        reads.insert_read(span(3), b, c, left_projection());
        let MixedTopologyCompileResult::Cached(topology) =
            compile_mixed_topology(&results, &reads, &MixedTopologyConfig::default())
        else {
            panic!("topology must cache");
        };
        let roots = vec![(span(3), Region::point(0, 42, 2))];
        let cached = build_demand_closure_cached(roots.clone(), &results, &topology);
        let paged =
            build_demand_closure_paged(roots.clone(), &results, &reads, |_| Ok::<_, ()>(()))
                .unwrap()
                .0;
        let runs = build_demand_closure_in_memory_runs(roots.clone(), &results, &reads, |_| {
            Ok::<_, ()>(())
        })
        .unwrap()
        .0;
        #[cfg(not(target_arch = "wasm32"))]
        let native = {
            let path = std::env::temp_dir().join(format!(
                "formualizer-native-demand-test-{}.tmp",
                std::process::id()
            ));
            let auxiliary_path = path.with_extension("aux.tmp");
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(&auxiliary_path);
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            let mut auxiliary_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&auxiliary_path)
                .unwrap();
            let result = build_demand_closure_native(
                roots.clone(),
                &results,
                &reads,
                &mut file,
                &mut auxiliary_file,
                |_| Ok::<_, ()>(()),
            )
            .unwrap()
            .0;
            drop(file);
            drop(auxiliary_file);
            std::fs::remove_file(path).unwrap();
            std::fs::remove_file(auxiliary_path).unwrap();
            result
        };
        #[cfg(target_arch = "wasm32")]
        let native = build_demand_closure_repeated_passes(roots.clone(), &results, &reads, |_| {
            Ok::<_, ()>(())
        })
        .unwrap()
        .0;
        let repeated =
            build_demand_closure_repeated_passes(roots, &results, &reads, |_| Ok::<_, ()>(()))
                .unwrap()
                .0;
        let oracle = BTreeMap::from([
            (span(1), vec![Region::point(0, 42, 0)]),
            (span(2), vec![Region::point(0, 42, 1)]),
            (span(3), vec![Region::point(0, 42, 2)]),
        ]);
        for closure in [&cached, &paged, &runs, &native, &repeated] {
            assert_eq!(closure.demanded, oracle);
        }
    }

    #[test]
    fn paged_demand_builds_ephemeral_consumer_pages_while_repeated_rescans() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let a = Region::point(0, 0, 0);
        let b = Region::point(0, 0, 1);
        let c = Region::point(0, 0, 2);
        results.insert_producer(span(1), a);
        results.insert_producer(span(2), b);
        results.insert_producer(span(3), c);
        reads.insert_read(span(2), a, b, left_projection());
        reads.insert_read(span(3), b, c, left_projection());
        for index in 0..1_000 {
            reads.insert_read(
                span(100 + index),
                Region::point(0, index, 10),
                Region::point(0, index, 11),
                left_projection(),
            );
        }
        let roots = vec![(span(3), c)];
        let mut paged_work = 0_u64;
        let paged = build_demand_closure_paged(roots.clone(), &results, &reads, |units| {
            paged_work = paged_work.saturating_add(units);
            Ok::<_, ()>(())
        })
        .unwrap()
        .0;
        let mut repeated_work = 0_u64;
        let repeated = build_demand_closure_repeated_passes(roots, &results, &reads, |units| {
            repeated_work = repeated_work.saturating_add(units);
            Ok::<_, ()>(())
        })
        .unwrap()
        .0;

        assert_eq!(paged, repeated);
        assert!(
            paged_work < repeated_work / 2,
            "{paged_work} vs {repeated_work}"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_demand_index_is_consumer_sorted_and_avoids_irrelevant_rescans() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let chain_len = 96_u32;
        for id in 1..=chain_len {
            let result = Region::point(0, id, 0);
            results.insert_producer(span(id), result);
            if id > 1 {
                reads.insert_read(
                    span(id),
                    Region::point(0, id - 1, 0),
                    result,
                    DirtyProjectionRule::WholeResult,
                );
            }
        }
        for id in 10_000..20_000_u32 {
            reads.insert_read(
                span(id),
                Region::point(1, id, 0),
                Region::point(1, id, 1),
                DirtyProjectionRule::WholeResult,
            );
        }
        let roots = vec![(span(chain_len), Region::point(0, chain_len, 0))];
        let (path, mut file, auxiliary_path, mut auxiliary_file) =
            native_test_files("demand-index");
        let mut native_work = 0_u64;
        let (native, searches, disk_bytes) = build_demand_closure_native(
            roots.clone(),
            &results,
            &reads,
            &mut file,
            &mut auxiliary_file,
            |units| {
                native_work = native_work.saturating_add(units);
                Ok::<_, ()>(())
            },
        )
        .unwrap();
        let mut repeated_work = 0_u64;
        let repeated = build_demand_closure_repeated_passes(roots, &results, &reads, |units| {
            repeated_work = repeated_work.saturating_add(units);
            Ok::<_, ()>(())
        })
        .unwrap()
        .0;

        assert_eq!(native, repeated);
        assert_eq!(searches, u64::from(chain_len));
        assert!(
            native_work < repeated_work / 4,
            "{native_work} vs {repeated_work}"
        );
        assert_eq!(
            disk_bytes,
            (reads.len() as u64)
                .saturating_mul(NATIVE_CONSUMER_RECORD_BYTES)
                .saturating_mul(2),
        );

        let record_count = reads.len() as u64;
        let index_file = if file.metadata().unwrap().len() == 0 {
            &mut auxiliary_file
        } else {
            &mut file
        };
        assert_eq!(
            index_file.metadata().unwrap().len(),
            record_count.saturating_mul(NATIVE_CONSUMER_RECORD_BYTES),
        );
        let records = (0..record_count)
            .map(|index| read_native_consumer_record_at(index_file, index).unwrap())
            .collect::<Vec<_>>();
        assert!(records.windows(2).all(|window| window[0] <= window[1]));
        for record in records {
            let read = reads.entry(record.entry_index as usize).unwrap();
            assert_eq!(record.consumer_key, native_consumer_key(read.consumer));
        }

        drop(file);
        drop(auxiliary_file);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(auxiliary_path).unwrap();
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_demand_index_io_failure_is_reported_for_safe_fallback() {
        let (results, reads, _) = exact_strategy_inputs(1);
        let roots = vec![(span(2), Region::point(0, 0, 1))];
        let (path, file, auxiliary_path, mut auxiliary_file) = native_test_files("demand-io");
        drop(file);
        let mut read_only = std::fs::File::open(&path).unwrap();
        let result = build_demand_closure_native(
            roots.clone(),
            &results,
            &reads,
            &mut read_only,
            &mut auxiliary_file,
            |_| Ok::<_, ()>(()),
        );
        assert!(matches!(result, Err(NativeExactDemandError::Io(_))));
        let fallback =
            build_demand_closure_repeated_passes(roots, &results, &reads, |_| Ok::<_, ()>(()))
                .unwrap()
                .0;
        assert_eq!(
            fallback.producers().collect::<Vec<_>>(),
            vec![span(1), span(2)]
        );
        drop(read_only);
        drop(auxiliary_file);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(auxiliary_path).unwrap();
    }

    #[test]
    fn demanded_member_includes_complete_mixed_scc_before_scheduling() {
        let mut results = FormulaProducerResultIndex::default();
        let mut reads = FormulaConsumerReadIndex::default();
        let first = cell(0, 0, 0);
        let second = cell(0, 0, 1);
        results.insert_producer(span(1), first);
        results.insert_producer(legacy(2), second);
        reads.insert_read(legacy(2), first, second, DirtyProjectionRule::WholeResult);
        reads.insert_read(span(1), second, first, DirtyProjectionRule::WholeResult);
        let MixedTopologyCompileResult::Cached(topology) =
            compile_mixed_topology(&results, &reads, &MixedTopologyConfig::default())
        else {
            panic!("topology must cache");
        };
        let closure = build_demand_closure_cached([(span(1), first)], &results, &topology);
        assert_eq!(
            closure.producers().collect::<Vec<_>>(),
            vec![legacy(2), span(1)]
        );
        let schedule = schedule_dirty_work(
            closure
                .producers()
                .map(|producer| work(producer, ProducerDirtyDomain::Whole)),
            &results,
            &topology,
            256,
        );
        assert_eq!(schedule.stats.cycle_count, 2);
        assert!(schedule.layers.is_empty());
    }

    #[test]
    fn precedent_adjacency_is_included_in_retained_cache_cap() {
        let (results, reads) = two_producer_topology_inputs();
        let unrestricted =
            compile_mixed_topology(&results, &reads, &MixedTopologyConfig::default());
        let bytes = unrestricted.observed().estimated_memory_bytes;
        assert!(bytes > std::mem::size_of::<MixedTopology>());
        let skipped = compile_mixed_topology(
            &results,
            &reads,
            &MixedTopologyConfig {
                max_memory_bytes: bytes.saturating_sub(1),
                ..MixedTopologyConfig::default()
            },
        );
        assert!(matches!(
            skipped,
            MixedTopologyCompileResult::CacheSkipped {
                reason: MixedScheduleFallbackReason::CacheMemoryExceeded,
                ..
            }
        ));
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
