//! Request-bounded relation reachability for source publication policy.
//!
//! This reports maintained static edges plus eligible installed grid-read
//! rectangles. It does not certify all semantic read kinds or decide policy.

use super::authority::{geom::Rect, level_index::QueryWorkBudget, store::AuthorityError};
use super::inspect::StateStamp;
use super::vertex::VertexId;
use crate::CellRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AffectedRoot {
    Cell(CellRef),
    Symbol(VertexId),
    Region(RegionPiece),
    CommittedSpill(VertexId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RegionPiece {
    pub sheet: u16,
    pub rect: Rect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClosureLimits {
    pub max_work: u64,
    pub max_output_items: usize,
    /// Requested owned vector capacity, including outputs and relocation peaks.
    /// Allocator metadata, excess physical allocation and total RSS are excluded.
    pub max_scratch_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClosureResource {
    Work,
    Output,
    Scratch,
}

#[derive(Debug)]
pub(crate) enum ClosureError {
    Authority(AuthorityError),
    Busy,
    InvalidRoot {
        index: usize,
    },
    InconsistentIdentity,
    StaleSnapshot,
    Limit {
        resource: ClosureResource,
        limit: u64,
        attempted: u64,
    },
    ArithmeticOverflow,
    Allocation,
}
impl From<AuthorityError> for ClosureError {
    fn from(error: AuthorityError) -> Self {
        Self::Authority(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClosureStamp {
    pub state: StateStamp,
    pub authority_revision: u64,
    pub symbol_revision: u64,
    pub observed_value_revision: u64,
    pub semantic_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadRelationCoverage {
    NotQueried,
    StaticAuthorityAndCommittedGridRectangles,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnknownReason {
    MissingObservation,
    UncommittedObservation,
    DirtyReader,
    OpaqueStaticRelation,
    RuntimeSymbolsUnrepresented,
    UnavailableSpillGeometry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnknownFact {
    pub region: RegionPiece,
    pub vertex: Option<VertexId>,
    pub reason: UnknownReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ClosureUsage {
    pub work: u64,
    pub output_items: usize,
    pub requested_capacity_peak_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct CurrentReadClosure<'engine> {
    // Keep the source engine immutably borrowed through the policy decision.
    owner: &'engine std::sync::Arc<()>,
    pub stamp: ClosureStamp,
    pub witnessed: Vec<RegionPiece>,
    pub uncertain_downstream: Vec<RegionPiece>,
    pub unknowns: Vec<UnknownFact>,
    pub coverage: ReadRelationCoverage,
    /// Other semantic read kinds are not certified by rectangle recording.
    pub semantic_reads_unattested: bool,
    /// A dirty or unavailable spill cannot certify prospective output geometry.
    pub unlocalized_spill_geometry: bool,
    pub usage: ClosureUsage,
}

impl CurrentReadClosure<'_> {
    pub(crate) fn belongs_to<R: crate::traits::EvaluationContext>(
        &self,
        engine: &super::Engine<R>,
    ) -> bool {
        std::sync::Arc::ptr_eq(self.owner, engine.affected_closure_owner())
    }
}

struct Budget {
    limits: ClosureLimits,
    usage: ClosureUsage,
    live_bytes: usize,
}
impl Budget {
    fn new(limits: ClosureLimits) -> Self {
        Self {
            limits,
            usage: ClosureUsage::default(),
            live_bytes: 0,
        }
    }
    fn output(&mut self) -> Result<(), ClosureError> {
        let attempted = self
            .usage
            .output_items
            .checked_add(1)
            .ok_or(ClosureError::ArithmeticOverflow)?;
        if attempted > self.limits.max_output_items {
            return Err(ClosureError::Limit {
                resource: ClosureResource::Output,
                limit: self.limits.max_output_items as u64,
                attempted: attempted as u64,
            });
        }
        self.usage.output_items = attempted;
        Ok(())
    }
    fn push<T>(&mut self, vector: &mut Vec<T>, value: T) -> Result<(), ClosureError> {
        self.charge_work(1)?;
        let size = std::mem::size_of::<T>();
        if size != 0 && vector.len() == vector.capacity() {
            let old = vector
                .capacity()
                .checked_mul(size)
                .ok_or(ClosureError::ArithmeticOverflow)?;
            let capacity = vector
                .capacity()
                .max(1)
                .checked_mul(2)
                .ok_or(ClosureError::ArithmeticOverflow)?;
            let requested = capacity
                .checked_mul(size)
                .ok_or(ClosureError::ArithmeticOverflow)?;
            let peak = self
                .live_bytes
                .checked_add(requested)
                .ok_or(ClosureError::ArithmeticOverflow)?;
            if peak > self.limits.max_scratch_bytes {
                return Err(ClosureError::Limit {
                    resource: ClosureResource::Scratch,
                    limit: self.limits.max_scratch_bytes as u64,
                    attempted: peak as u64,
                });
            }
            self.usage.requested_capacity_peak_bytes =
                self.usage.requested_capacity_peak_bytes.max(peak);
            vector
                .try_reserve_exact(capacity - vector.len())
                .map_err(|_| ClosureError::Allocation)?;
            // The contract bounds requested capacity, not physical allocator
            // behavior. Refuse an allocator-reported excess instead of silently
            // using unadmitted slots in a successful report.
            if vector.capacity() > capacity {
                return Err(ClosureError::Allocation);
            }
            self.live_bytes = self
                .live_bytes
                .checked_sub(old)
                .and_then(|bytes| bytes.checked_add(requested))
                .ok_or(ClosureError::ArithmeticOverflow)?;
        }
        vector.push(value);
        Ok(())
    }
    fn release<T>(&mut self, vector: Vec<T>) {
        let bytes = vector.capacity() * std::mem::size_of::<T>();
        self.live_bytes -= bytes;
        drop(vector);
    }
}
impl QueryWorkBudget for Budget {
    type Error = ClosureError;
    fn charge_work(&mut self, units: u64) -> Result<(), ClosureError> {
        let attempted = self
            .usage
            .work
            .checked_add(units)
            .ok_or(ClosureError::ArithmeticOverflow)?;
        if attempted > self.limits.max_work {
            return Err(ClosureError::Limit {
                resource: ClosureResource::Work,
                limit: self.limits.max_work,
                attempted,
            });
        }
        self.usage.work = attempted;
        Ok(())
    }
}

/// Add only uncovered geometry, without expanding columns/cells or using a
/// second maintained index. Temporary fragmentation shares the same admission.
fn add_region(
    piece: RegionPiece,
    coverage: &mut Vec<RegionPiece>,
    frontier: &mut Vec<RegionPiece>,
    budget: &mut Budget,
) -> Result<(), ClosureError> {
    let mut fragments = Vec::new();
    budget.push(&mut fragments, piece)?;
    for covered in coverage.iter() {
        budget.charge_work(1)?;
        if covered.sheet != piece.sheet {
            continue;
        }
        let mut next = Vec::new();
        for fragment in &fragments {
            budget.charge_work(1)?;
            for &rect in fragment.rect.subtract(&covered.rect).as_slice() {
                budget.push(
                    &mut next,
                    RegionPiece {
                        sheet: piece.sheet,
                        rect,
                    },
                )?;
            }
        }
        budget.release(fragments);
        fragments = next;
        if fragments.is_empty() {
            break;
        }
    }
    for &fragment in &fragments {
        budget.output()?;
        budget.push(coverage, fragment)?;
        budget.push(frontier, fragment)?;
    }
    budget.release(fragments);
    Ok(())
}

struct Reader<'engine> {
    vertex: VertexId,
    region: RegionPiece,
    reads: Option<&'engine [super::authority::host::ObservedRect]>,
}

fn add_unknown(
    fact: UnknownFact,
    unknowns: &mut Vec<UnknownFact>,
    budget: &mut Budget,
) -> Result<(), ClosureError> {
    for known in unknowns.iter() {
        budget.charge_work(1)?;
        if *known == fact {
            return Ok(());
        }
    }
    budget.output()?;
    budget.push(unknowns, fact)
}

impl<R: crate::traits::EvaluationContext> super::Engine<R> {
    fn affected_readers<'engine>(
        &'engine self,
        store: &super::authority::store::Store,
        unknowns: &mut Vec<UnknownFact>,
        budget: &mut Budget,
    ) -> Result<Vec<Reader<'engine>>, ClosureError> {
        use super::authority::store::{F_DYNAMIC, F_OPAQUE};
        let mut readers = Vec::new();
        store.visit_owner_slots_bounded(budget, &mut |sheet, domain, flags, budget| {
            if flags & F_OPAQUE != 0 {
                add_unknown(
                    UnknownFact {
                        region: RegionPiece {
                            sheet,
                            rect: domain,
                        },
                        vertex: None,
                        reason: UnknownReason::OpaqueStaticRelation,
                    },
                    unknowns,
                    budget,
                )?;
            }
            if flags & F_DYNAMIC == 0 {
                return Ok(());
            }
            for row in domain.r0..=domain.r1 {
                for column in domain.c0..=domain.c1 {
                    // Conservative admission for fixed-width numeric B-tree
                    // predecessor and identity lookups, not exact comparisons.
                    budget.charge_work(128)?;
                    let coordinate = (sheet, row, column);
                    let vertex = self
                        .graph
                        .authority_vertex_of_cell(coordinate)
                        .ok_or(ClosureError::InconsistentIdentity)?;
                    if !self.graph.vertex_exists_active(vertex)
                        || self.graph.authority_cell_of_vertex(vertex) != Some(coordinate)
                    {
                        return Err(ClosureError::InconsistentIdentity);
                    }
                    let region = RegionPiece {
                        sheet,
                        rect: Rect::cell(row, column),
                    };
                    let host = self.graph.authority_host();
                    let committed = host.committed_observed(vertex);
                    let dirty = self.graph.is_dirty(vertex);
                    let reason = if dirty {
                        Some(UnknownReason::DirtyReader)
                    } else if committed.is_none() {
                        Some(if host.observed(vertex).is_some() {
                            UnknownReason::UncommittedObservation
                        } else {
                            UnknownReason::MissingObservation
                        })
                    } else {
                        None
                    };
                    if let Some(reason) = reason {
                        add_unknown(
                            UnknownFact {
                                region,
                                vertex: Some(vertex),
                                reason,
                            },
                            unknowns,
                            budget,
                        )?;
                    }
                    let reads = if dirty { None } else { committed };
                    if let Some(reads) = reads {
                        for &(read_sheet, r0, c0, r1, c1) in reads {
                            budget.charge_work(1)?;
                            if !self.affected_region_valid(RegionPiece {
                                sheet: read_sheet,
                                rect: Rect { r0, c0, r1, c1 },
                            }) {
                                return Err(ClosureError::InconsistentIdentity);
                            }
                        }
                    }
                    budget.push(
                        &mut readers,
                        Reader {
                            vertex,
                            region,
                            reads,
                        },
                    )?;
                }
            }
            Ok(())
        })?;
        Ok(readers)
    }

    fn walk_affected(
        &self,
        store: &super::authority::store::Store,
        readers: &[Reader<'_>],
        coverage: &mut Vec<RegionPiece>,
        frontier: &mut Vec<RegionPiece>,
        unknowns: &mut Vec<UnknownFact>,
        unlocalized_spill_geometry: &mut bool,
        budget: &mut Budget,
    ) -> Result<(), ClosureError> {
        while let Some(piece) = frontier.pop() {
            budget.charge_work(1)?;
            store.visit_direct_dependents_bounded(
                piece.sheet,
                &piece.rect,
                super::authority::store::TagFilter::All,
                budget,
                &mut |sheet, rect, budget| {
                    add_region(RegionPiece { sheet, rect }, coverage, frontier, budget)
                },
            )?;
            for reader in readers {
                budget.charge_work(1)?;
                if piece.sheet == super::authority::geom::SYMBOL_SHEET {
                    add_unknown(
                        UnknownFact {
                            region: reader.region,
                            vertex: Some(reader.vertex),
                            reason: UnknownReason::RuntimeSymbolsUnrepresented,
                        },
                        unknowns,
                        budget,
                    )?;
                }
                if let Some(reads) = reader.reads {
                    for &(sheet, r0, c0, r1, c1) in reads {
                        budget.charge_work(1)?;
                        if piece.sheet == sheet && piece.rect.intersects(&Rect { r0, c0, r1, c1 }) {
                            add_region(reader.region, coverage, frontier, budget)?;
                            break;
                        }
                    }
                }
            }
            self.graph
                .visit_spill_anchors_bounded(budget, &mut |anchor, budget| {
                    budget.charge_work(128)?;
                    let cell = self
                        .graph
                        .get_cell_ref(anchor)
                        .ok_or(ClosureError::InconsistentIdentity)?;
                    if cell.sheet_id != piece.sheet
                        || !piece.rect.contains(cell.coord.row(), cell.coord.col())
                    {
                        return Ok(());
                    }
                    let extent = self.graph.spill_extent_for_anchor(anchor);
                    if self.graph.is_dirty(anchor)
                        || self.graph.spill_may_be_intruded(anchor)
                        || extent.is_none()
                        || extent.is_some_and(|(first, last)| {
                            first != cell || last.sheet_id != cell.sheet_id
                        })
                    {
                        *unlocalized_spill_geometry = true;
                        return add_unknown(
                            UnknownFact {
                                region: RegionPiece {
                                    sheet: cell.sheet_id,
                                    rect: Rect::cell(cell.coord.row(), cell.coord.col()),
                                },
                                vertex: Some(anchor),
                                reason: UnknownReason::UnavailableSpillGeometry,
                            },
                            unknowns,
                            budget,
                        );
                    }
                    let (first, last) = extent.unwrap();
                    let region = RegionPiece {
                        sheet: first.sheet_id,
                        rect: Rect::new(
                            first.coord.row(),
                            first.coord.col(),
                            last.coord.row(),
                            last.coord.col(),
                        ),
                    };
                    if !self.affected_region_valid(region) {
                        return Err(ClosureError::InconsistentIdentity);
                    }
                    add_region(region, coverage, frontier, budget)
                })?;
        }
        Ok(())
    }
}

impl<R: crate::traits::EvaluationContext> super::Engine<R> {
    fn affected_closure_stamp(&self) -> ClosureStamp {
        let host = self.graph.authority_host();
        ClosureStamp {
            state: StateStamp {
                mutation_revision: self.inspection_mutation_revision(),
                recalc_epoch: self.recalc_epoch,
            },
            authority_revision: host.revision,
            symbol_revision: self.graph.symbol_revision(),
            observed_value_revision: host.observed_value_revision(),
            semantic_revision: crate::function_registry::semantic_epoch_lock_free(),
        }
    }

    fn affected_region_valid(&self, region: RegionPiece) -> bool {
        region.sheet != super::authority::geom::SYMBOL_SHEET
            && !self.graph.sheet_name(region.sheet).is_empty()
            && region.rect.r0 <= region.rect.r1
            && region.rect.c0 <= region.rect.c1
            && region.rect.r1 < 1_048_576
            && region.rect.c1 < 16_384
    }

    /// Immutable, request-local relation query. Output borrows the engine so a
    /// policy decision cannot retain it across a safe mutable engine operation.
    /// Negative facts apply only to the reported relation and declared unknowns.
    pub(crate) fn current_read_affected_closure(
        &self,
        roots: &[AffectedRoot],
        limits: ClosureLimits,
    ) -> Result<CurrentReadClosure<'_>, ClosureError> {
        let stamp = self.affected_closure_stamp();
        if roots.is_empty() {
            return Ok(CurrentReadClosure {
                owner: self.affected_closure_owner(),
                stamp,
                witnessed: Vec::new(),
                uncertain_downstream: Vec::new(),
                unknowns: Vec::new(),
                coverage: ReadRelationCoverage::NotQueried,
                semantic_reads_unattested: true,
                unlocalized_spill_geometry: false,
                usage: ClosureUsage::default(),
            });
        }
        let store = self.graph.authority_plan_store()?;
        let host = self.graph.authority_host();
        if !self.affected_closure_idle() || host.structural_pending || host.replaying {
            return Err(ClosureError::Busy);
        }
        let mut budget = Budget::new(limits);
        let (mut witnessed, mut frontier) = (Vec::new(), Vec::new());
        let mut unknowns = Vec::new();
        let mut unlocalized_spill_geometry = false;
        for (index, root) in roots.iter().enumerate() {
            budget.charge_work(1)?;
            let region = match *root {
                AffectedRoot::Cell(cell) => RegionPiece {
                    sheet: cell.sheet_id,
                    rect: Rect::cell(cell.coord.row(), cell.coord.col()),
                },
                AffectedRoot::Region(region) => region,
                AffectedRoot::Symbol(vertex) => {
                    budget.charge_work(128)?;
                    if !self.graph.vertex_exists_active(vertex) {
                        return Err(ClosureError::InvalidRoot { index });
                    }
                    let Some((sheet, row, column)) = self.graph.authority_cell_of_vertex(vertex)
                    else {
                        return Err(ClosureError::InvalidRoot { index });
                    };
                    if sheet != super::authority::geom::SYMBOL_SHEET {
                        return Err(ClosureError::InvalidRoot { index });
                    }
                    add_region(
                        RegionPiece {
                            sheet,
                            rect: Rect::cell(row, column),
                        },
                        &mut witnessed,
                        &mut frontier,
                        &mut budget,
                    )?;
                    continue;
                }
                AffectedRoot::CommittedSpill(vertex) => {
                    budget.charge_work(128)?;
                    if !self.graph.is_spill_anchor(vertex)
                        || !self.graph.is_live_formula_vertex(vertex)
                    {
                        return Err(ClosureError::InvalidRoot { index });
                    }
                    let cell = self
                        .graph
                        .get_cell_ref(vertex)
                        .ok_or(ClosureError::InvalidRoot { index })?;
                    RegionPiece {
                        sheet: cell.sheet_id,
                        rect: Rect::cell(cell.coord.row(), cell.coord.col()),
                    }
                }
            };
            if !self.affected_region_valid(region) {
                return Err(ClosureError::InvalidRoot { index });
            }
            add_region(region, &mut witnessed, &mut frontier, &mut budget)?;
        }
        let readers = self.affected_readers(store, &mut unknowns, &mut budget)?;
        self.walk_affected(
            store,
            &readers,
            &mut witnessed,
            &mut frontier,
            &mut unknowns,
            &mut unlocalized_spill_geometry,
            &mut budget,
        )?;
        let mut uncertain_downstream = Vec::new();
        let mut seeded = 0;
        loop {
            while seeded < unknowns.len() {
                budget.charge_work(1)?;
                let region = unknowns[seeded].region;
                seeded += 1;
                add_region(
                    region,
                    &mut uncertain_downstream,
                    &mut frontier,
                    &mut budget,
                )?;
            }
            if frontier.is_empty() {
                break;
            }
            self.walk_affected(
                store,
                &readers,
                &mut uncertain_downstream,
                &mut frontier,
                &mut unknowns,
                &mut unlocalized_spill_geometry,
                &mut budget,
            )?;
        }
        budget.charge_work(1)?;
        if self.affected_closure_stamp() != stamp {
            return Err(ClosureError::StaleSnapshot);
        }
        budget.release(frontier);
        budget.charge_work(readers.len() as u64)?;
        budget.release(readers);
        Ok(CurrentReadClosure {
            owner: self.affected_closure_owner(),
            stamp,
            witnessed,
            uncertain_downstream,
            unknowns,
            coverage: ReadRelationCoverage::StaticAuthorityAndCommittedGridRectangles,
            semantic_reads_unattested: true,
            unlocalized_spill_geometry,
            usage: budget.usage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> ClosureLimits {
        ClosureLimits {
            max_work: 10_000,
            max_output_items: 100,
            max_scratch_bytes: 64 * 1024,
        }
    }
    fn piece(rect: Rect) -> RegionPiece {
        RegionPiece { sheet: 0, rect }
    }
    #[test]
    fn affected_closure_cover_keeps_exact_disjoint_sparse_geometry() {
        let mut budget = Budget::new(limits());
        let (mut cover, mut queue) = (Vec::new(), Vec::new());
        add_region(
            piece(Rect::new(1, 1, 3, 3)),
            &mut cover,
            &mut queue,
            &mut budget,
        )
        .unwrap();
        add_region(
            piece(Rect::new(2, 2, 4, 4)),
            &mut cover,
            &mut queue,
            &mut budget,
        )
        .unwrap();
        add_region(
            piece(Rect::new(1, 1, 3, 3)),
            &mut cover,
            &mut queue,
            &mut budget,
        )
        .unwrap();
        assert_eq!(cover.iter().map(|item| item.rect.area()).sum::<u64>(), 14);
        for (i, item) in cover.iter().enumerate() {
            for other in &cover[i + 1..] {
                assert!(!item.rect.intersects(&other.rect));
            }
        }
        add_region(
            piece(Rect::cell(1_048_575, 16_383)),
            &mut cover,
            &mut queue,
            &mut budget,
        )
        .unwrap();
        assert_eq!(cover.iter().map(|item| item.rect.area()).sum::<u64>(), 15);
        assert!(cover.len() <= 5);
        assert!(budget.usage.requested_capacity_peak_bytes <= limits().max_scratch_bytes);
    }
    #[test]
    fn affected_closure_limits_deny_before_vector_insertion() {
        for resource in [
            ClosureResource::Work,
            ClosureResource::Output,
            ClosureResource::Scratch,
        ] {
            let mut cap = limits();
            match resource {
                ClosureResource::Work => cap.max_work = 0,
                ClosureResource::Output => cap.max_output_items = 0,
                ClosureResource::Scratch => cap.max_scratch_bytes = 0,
            }
            let mut budget = Budget::new(cap);
            let (mut cover, mut queue) = (Vec::new(), Vec::new());
            assert!(
                matches!(add_region(piece(Rect::cell(0, 0)), &mut cover, &mut queue, &mut budget),
                Err(ClosureError::Limit { resource: found, .. }) if found == resource)
            );
            assert!(cover.is_empty() && queue.is_empty());
            assert!(budget.usage.work <= cap.max_work);
            assert!(budget.usage.requested_capacity_peak_bytes <= cap.max_scratch_bytes);
        }
    }
    #[test]
    fn affected_closure_scratch_accounts_relocation_and_both_owned_outputs() {
        let mut roomy = Budget::new(limits());
        let (mut cover, mut queue) = (Vec::new(), Vec::new());
        add_region(piece(Rect::cell(0, 0)), &mut cover, &mut queue, &mut roomy).unwrap();
        let needed = roomy.usage.requested_capacity_peak_bytes;
        assert!(needed >= 6 * std::mem::size_of::<RegionPiece>());
        for cap in [needed - 1, needed] {
            let mut restricted = limits();
            restricted.max_scratch_bytes = cap;
            let mut budget = Budget::new(restricted);
            let (mut cover, mut queue) = (Vec::new(), Vec::new());
            let result = add_region(piece(Rect::cell(0, 0)), &mut cover, &mut queue, &mut budget);
            assert_eq!(result.is_ok(), cap == needed);
            assert!(budget.usage.requested_capacity_peak_bytes <= cap);
        }
    }
}
