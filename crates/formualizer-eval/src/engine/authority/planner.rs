//! Store-to-ARC planning driver. Candidate discovery, independent refinement,
//! exact images and topology share one live scratch budget. Classification and
//! schedule emission are added downstream; this is not yet runtime ordering.

use super::arc_sweep::{Probe, Slice};
use super::arc_topology::{Topology, TopologyError, topology};
use super::candidates::discover;
use super::geom::Cover;
use super::store::{AuthorityError, EdgeKey, Store};

#[derive(Clone, Copy, Debug)]
pub(crate) struct PieceIdentity {
    pub owner: u32,
    pub first_id: u32,
}

#[derive(Debug)]
pub(crate) struct PlanningInput {
    pub slices: Vec<Slice>,
    pub identities: Vec<PieceIdentity>,
    pub probes: Vec<Probe>,
    /// Parallel to probes. Sweep probe IDs and emission pair witnesses retain
    /// the original projection for displacement proofs (including self hits).
    pub edges: Vec<EdgeKey>,
    pub cells: u64,
    /// R = sum of reference incidences over candidate cells, counted once.
    pub references: u64,
    pub work: u64,
    pub peak_heap_bytes: u64,
}
impl PlanningInput {
    pub fn heap_bytes(&self) -> u64 {
        (self.slices.capacity() * size_of::<Slice>()
            + self.identities.capacity() * size_of::<PieceIdentity>()
            + self.probes.capacity() * size_of::<Probe>()
            + self.edges.capacity() * size_of::<EdgeKey>()) as u64
    }
}

#[derive(Debug)]
pub(crate) struct PreparedPlan {
    pub input: PlanningInput,
    pub topology: Topology,
    pub peak_heap_bytes: u64,
}
impl PreparedPlan {
    pub fn heap_bytes(&self) -> u64 {
        self.input.heap_bytes() + self.topology.heap_bytes()
    }
    pub fn total_work(&self) -> u64 {
        self.input.work + self.topology.total_work()
    }
}

fn add(a: u64, b: u64) -> Result<u64, AuthorityError> {
    a.checked_add(b).ok_or(AuthorityError::Alloc)
}
fn bytes<T>(n: usize) -> Result<u64, AuthorityError> {
    n.checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(AuthorityError::Alloc)
}
fn reserve<T>(n: usize) -> Result<Vec<T>, AuthorityError> {
    let mut out = Vec::new();
    out.try_reserve_exact(n)
        .map_err(|_| AuthorityError::Alloc)?;
    Ok(out)
}
fn remaining(limit: Option<u64>, held: u64) -> Result<Option<u64>, AuthorityError> {
    limit
        .map(|limit| {
            limit.checked_sub(held).ok_or(AuthorityError::Admission {
                resource: "scratch",
                needed: held,
                limit,
            })
        })
        .transpose()
}

/// Two refinement passes avoid a growing vector or one retained allocation per
/// candidate. Both passes (including index queries and image construction) are
/// counted. All candidates + aggregate capacity + current helper scratch coexist
/// during filling, and are admitted together. No whole-store scan or cell
/// expansion occurs. The borrowed cover and Store are caller-owned.
pub(crate) fn input(
    store: &Store,
    cover: &Cover,
    scratch_limit: Option<u64>,
) -> Result<PlanningInput, AuthorityError> {
    let candidates = discover(store, cover, scratch_limit)?;
    let held = candidates.heap_bytes();
    let mut peak = candidates.peak_heap_bytes;
    let mut work = candidates.work.total();
    let mut pieces = 0usize;
    let mut probes = 0usize;
    let mut references = 0u64;
    for c in &candidates.slices {
        work += 1;
        let refined = store.refine_owner_column(
            c.owner,
            c.col,
            c.r0,
            c.r1,
            remaining(scratch_limit, held)?,
        )?;
        peak = peak.max(add(held, refined.peak_heap_bytes)?);
        work += refined.work.total();
        references = add(references, refined.cell_references)?;
        pieces = pieces
            .checked_add(refined.pieces.len())
            .ok_or(AuthorityError::Alloc)?;
        for piece in &refined.pieces {
            work += 1;
            for edge in &refined.edges[piece.edge_start..piece.edge_end] {
                work += 1;
                if edge.proj.forward(&piece.domain).is_some() {
                    probes = probes.checked_add(1).ok_or(AuthorityError::Alloc)?;
                }
            }
        }
    }
    let aggregate = add(
        add(bytes::<Slice>(pieces)?, bytes::<PieceIdentity>(pieces)?)?,
        add(bytes::<Probe>(probes)?, bytes::<EdgeKey>(probes)?)?,
    )?;
    let live = add(held, aggregate)?;
    remaining(scratch_limit, live)?;
    peak = peak.max(live);
    let mut out = PlanningInput {
        slices: reserve(pieces)?,
        identities: reserve(pieces)?,
        probes: reserve(probes)?,
        edges: reserve(probes)?,
        cells: candidates.cells,
        references,
        work: 0,
        peak_heap_bytes: 0,
    };
    for c in &candidates.slices {
        work += 1;
        let refined = store.refine_owner_column(
            c.owner,
            c.col,
            c.r0,
            c.r1,
            remaining(scratch_limit, live)?,
        )?;
        peak = peak.max(add(live, refined.peak_heap_bytes)?);
        work += refined.work.total();
        for piece in &refined.pieces {
            work += 1;
            let reader = out.slices.len();
            out.slices.push(Slice {
                sheet: piece.sheet,
                col: piece.domain.c0,
                r0: piece.domain.r0,
                r1: piece.domain.r1,
            });
            out.identities.push(PieceIdentity {
                owner: piece.owner,
                first_id: c
                    .first_id
                    .checked_add(piece.domain.r0 - c.r0)
                    .ok_or(AuthorityError::Alloc)?,
            });
            for &edge in &refined.edges[piece.edge_start..piece.edge_end] {
                work += 1;
                if let Some(image) = edge.proj.forward(&piece.domain) {
                    out.probes.push(Probe {
                        reader,
                        sheet: edge.proj.sheet,
                        image,
                    });
                    out.edges.push(edge);
                }
            }
        }
    }
    debug_assert_eq!(out.slices.len(), pieces);
    debug_assert_eq!(out.probes.len(), probes);
    out.work = work;
    out.peak_heap_bytes = peak;
    Ok(out)
}

/// Assemble discovery -> refinement/images -> sweep -> emission -> CSR/SCC.
/// Limits are explicit until the engine resource ledger supplies plan defaults.
/// Retained input witnesses are subtracted from topology's scratch allowance.
pub(crate) fn prepare(
    store: &Store,
    cover: &Cover,
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
    discovery_limit: Option<u64>,
) -> Result<PreparedPlan, TopologyError> {
    let input = input(store, cover, scratch_limit)?;
    let held = input.heap_bytes();
    let topology = topology(
        &input.slices,
        &input.probes,
        remaining(scratch_limit, held)?,
        arc_limit,
        discovery_limit,
    )?;
    let peak_heap_bytes = input
        .peak_heap_bytes
        .max(add(held, topology.peak_heap_bytes)?);
    Ok(PreparedPlan {
        input,
        topology,
        peak_heap_bytes,
    })
}
