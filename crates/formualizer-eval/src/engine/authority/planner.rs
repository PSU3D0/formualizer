//! Store-to-cell ARC planning driver. Discovery, refinement/images, topology,
//! affine classification, SCC-local exact fallback and per-cell levels share
//! one scratch budget. Runtime Schedule adaptation/resources remain separate.

use super::arc_sweep::{Probe, Slice};
use super::arc_topology::{Topology, TopologyError, topology};
use super::candidates::discover;
use super::geom::Cover;
use super::store::{AuthorityError, EdgeKey, Store};

#[derive(Clone, Copy, Debug)]
pub(crate) struct PieceIdentity {
    pub owner: u32,
    pub first_id: u32,
    pub probe_start: usize,
    pub probe_end: usize,
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
    pub classification: Classification,
    pub peak_heap_bytes: u64,
}
impl PreparedPlan {
    pub fn heap_bytes(&self) -> u64 {
        self.input.heap_bytes() + self.topology.heap_bytes() + self.classification.heap_bytes()
    }
    pub fn total_work(&self) -> u64 {
        self.input.work + self.topology.total_work() + self.classification.work
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

/// One refinement per candidate: each candidate's refined pieces are kept
/// until the exact output sizes are known, then copied into exactly
/// reserved outputs and dropped (refining twice, once to size and once to
/// fill, doubled the index queries and allocations per candidate, which
/// dominated planning of workbooks made of many small owners). Everything
/// that coexists is admitted together and counted in the peak: the
/// candidates, the kept refinements (and the list holding them), each
/// refinement's own temporaries, and the outputs. No whole-store scan or
/// cell expansion occurs. The borrowed cover and Store are caller-owned.
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
    let list_bytes = bytes::<super::refine::RefinedSlice>(candidates.slices.len())?;
    let mut live = add(held, list_bytes)?;
    remaining(scratch_limit, live)?;
    peak = peak.max(live);
    let mut refinements: Vec<super::refine::RefinedSlice> = reserve(candidates.slices.len())?;
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
        live = add(live, refined.retained_heap_bytes())?;
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
        refinements.push(refined);
    }
    let aggregate = add(
        add(bytes::<Slice>(pieces)?, bytes::<PieceIdentity>(pieces)?)?,
        add(bytes::<Probe>(probes)?, bytes::<EdgeKey>(probes)?)?,
    )?;
    let live = add(live, aggregate)?;
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
    for (c, refined) in candidates.slices.iter().zip(&refinements) {
        work += 1;
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
                probe_start: out.probes.len(),
                probe_end: 0,
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
            out.identities[reader].probe_end = out.probes.len();
        }
    }
    drop(refinements);
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
    prepare_input(input, scratch_limit, arc_limit, discovery_limit)
}

fn prepare_input(
    input: PlanningInput,
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
    discovery_limit: Option<u64>,
) -> Result<PreparedPlan, TopologyError> {
    let held = input.heap_bytes();
    let topology = topology(
        &input.slices,
        &input.probes,
        remaining(scratch_limit, held)?,
        arc_limit,
        discovery_limit,
    )?;
    let mut peak_heap_bytes = input
        .peak_heap_bytes
        .max(add(held, topology.peak_heap_bytes)?);
    let live = add(held, topology.heap_bytes())?;
    let classification = classify(&input, &topology, remaining(scratch_limit, live)?)?;
    peak_heap_bytes = peak_heap_bytes.max(add(live, classification.peak_heap_bytes)?);
    Ok(PreparedPlan {
        input,
        topology,
        classification,
        peak_heap_bytes,
    })
}

const NONE: usize = usize::MAX;
const DIRECTIONS: [(i64, i64); 16] = [
    (1, 0),
    (-1, 0),
    (0, 1),
    (0, -1),
    (1, 1),
    (1, -1),
    (-1, 1),
    (-1, -1),
    (2, 1),
    (1, 2),
    (2, -1),
    (1, -2),
    (-2, 1),
    (-1, 2),
    (-2, -1),
    (-1, -2),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    Auxiliary,
    Acyclic,
    /// The top SCC is already a cell SCC: reuse it, including ARC members.
    Cell {
        genuine_cycle: bool,
    },
    /// theta = k * (direction.row * row + direction.col * col) + sigma[piece].
    /// Offsets are theta - min; the span is max - min + 1 (not dense ranks).
    Affine {
        direction: (i64, i64),
        k: i64,
        min: i64,
        max: i64,
    },
    /// Unknown order; MUST refine exactly before schedule emission. Never cycle.
    Refine,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct ComponentClass {
    pub class: Class,
    pub real: usize,
    pub aux: bool,
    pub singletons: bool,
}
#[derive(Debug)]
pub(crate) struct Classification {
    pub components: Vec<ComponentClass>,
    pub sigma: Vec<i64>,
    pub work: u64,
    pub affine_work: u64,
    pub reused_cell_sccs: u64,
    pub peak_heap_bytes: u64,
}
impl Classification {
    pub fn heap_bytes(&self) -> u64 {
        (self.components.capacity() * size_of::<ComponentClass>()
            + self.sigma.capacity() * size_of::<i64>()) as u64
    }
}
#[derive(Clone, Copy)]
struct Displacement {
    next: usize,
    from: usize,
    to: usize,
    rows: (i64, i64),
    cols: (i64, i64),
}

fn axis_displacement(
    map: &super::proj::AxisMap,
    domain: (u32, u32),
    target: (u32, u32),
    max: u32,
) -> Option<(i64, i64)> {
    use super::proj::Bound;
    let (lo, hi) = map.invert(domain.0, domain.1, target.0, target.1, max)?;
    let ylo = map.window(lo, max).0.max(i64::from(target.0));
    let yhi = map.window(hi, max).1.min(i64::from(target.1));
    let mut min = i64::from(lo) - yhi;
    let mut max = i64::from(hi) - ylo;
    if let Bound::Rel(b) = map.hi {
        min = min.max(-i64::from(b));
    }
    if let Bound::Rel(a) = map.lo {
        max = max.min(-i64::from(a));
    }
    debug_assert!(min <= max);
    Some((min, max))
}

/// Classify the assembled topology, with one global witness bucketing pass.
/// There is no whole-graph rescan per SCC, per-SCC allocation, or estimated BF
/// work. Candidate-affine SCCs have at most 64 real members and no auxiliaries.
/// Every relaxation, initialization, direction, member and witness is counted.
fn classify(
    input: &PlanningInput,
    topo: &Topology,
    scratch_limit: Option<u64>,
) -> Result<Classification, TopologyError> {
    let count = topo.components.len();
    let real = input.slices.len();
    let witnesses = topo.emission.pair_witnesses.len();
    let peak = add(
        add(bytes::<ComponentClass>(count)?, bytes::<i64>(real)?)?,
        add(
            add(bytes::<usize>(real)?, bytes::<usize>(count)?)?,
            bytes::<Displacement>(witnesses)?,
        )?,
    )?;
    remaining(scratch_limit, peak)?;
    let mut out = Classification {
        components: reserve(count)?,
        sigma: reserve(real)?,
        work: 0,
        affine_work: 0,
        reused_cell_sccs: 0,
        peak_heap_bytes: peak,
    };
    let mut local: Vec<usize> = reserve(real)?;
    let mut heads: Vec<usize> = reserve(count)?;
    let mut boxes: Vec<Displacement> = reserve(witnesses)?;
    for _ in 0..real {
        out.work += 1;
        local.push(NONE);
        out.sigma.push(0);
    }
    for cid in 0..count {
        out.work += 1;
        heads.push(NONE);
        let mut c = ComponentClass {
            class: Class::Auxiliary,
            real: 0,
            aux: false,
            singletons: true,
        };
        let mut selfdep = false;
        for &v in topo.components.members(cid) {
            out.work += 1;
            if v >= real {
                c.aux = true;
                continue;
            }
            local[v] = c.real;
            c.real += 1;
            c.singletons &= input.slices[v].r0 == input.slices[v].r1;
            selfdep |= topo.emission.selfdep[v] != 0;
        }
        c.class = if c.real == 0 {
            Class::Auxiliary
        } else if c.singletons {
            out.reused_cell_sccs += 1;
            Class::Cell {
                genuine_cycle: c.real > 1 || selfdep,
            }
        } else if c.real == 1 && !c.aux && !selfdep {
            Class::Acyclic
        } else {
            Class::Refine
        };
        out.components.push(c);
    }
    for witness in &topo.emission.pair_witnesses {
        out.work += 1;
        let q = topo.sweep.probes[witness.hit];
        let probe = &input.probes[q];
        let from = witness.precedent;
        let to = probe.reader;
        let cid = topo.components.component_of[to];
        let c = out.components[cid];
        if topo.components.component_of[from] != cid
            || c.aux
            || c.real > 64
            || c.class != Class::Refine
        {
            continue;
        }
        let (reader, precedent) = (&input.slices[to], &input.slices[from]);
        let proj = &input.edges[q].proj;
        let rows = axis_displacement(
            &proj.rows,
            (reader.r0, reader.r1),
            (precedent.r0, precedent.r1),
            super::geom::MAX_ROW,
        )
        .ok_or(TopologyError::InvalidWitness)?;
        let cols = axis_displacement(
            &proj.cols,
            (reader.col, reader.col),
            (precedent.col, precedent.col),
            super::geom::MAX_COL,
        )
        .ok_or(TopologyError::InvalidWitness)?;
        boxes.push(Displacement {
            next: heads[cid],
            from: local[from],
            to: local[to],
            rows,
            cols,
        });
        heads[cid] = boxes.len() - 1;
    }
    for (cid, &head) in heads.iter().enumerate() {
        out.work += 1;
        let c = out.components[cid];
        if c.class != Class::Refine || c.aux || c.real > 64 {
            continue;
        }
        let n = c.real;
        let k = n as i64 + 1;
        'direction: for direction in DIRECTIONS {
            out.affine_work += 1;
            let mut sigma = [0i64; 64];
            out.affine_work += 64; // fixed stack initialization, not hidden
            for round in 0..=n {
                out.affine_work += 1;
                let mut changed = false;
                let mut at = head;
                while at != NONE {
                    out.affine_work += 1;
                    let b = boxes[at];
                    let min_axis = |a: i64, d: (i64, i64)| a * if a >= 0 { d.0 } else { d.1 };
                    let weight =
                        k * (min_axis(direction.0, b.rows) + min_axis(direction.1, b.cols)) - 1;
                    if b.from == b.to && weight < 0 {
                        continue 'direction;
                    }
                    let candidate = sigma[b.to] + weight;
                    if candidate < sigma[b.from] {
                        sigma[b.from] = candidate;
                        changed = true;
                    }
                    at = b.next;
                }
                if !changed {
                    let mut min = i64::MAX;
                    let mut max = i64::MIN;
                    for &v in topo.components.members(cid) {
                        out.affine_work += 1;
                        let s = &input.slices[v];
                        let offset = sigma[local[v]];
                        out.sigma[v] = offset;
                        let at = |row: u32| {
                            k * (direction.0 * i64::from(row) + direction.1 * i64::from(s.col))
                                + offset
                        };
                        min = min.min(at(s.r0)).min(at(s.r1));
                        max = max.max(at(s.r0)).max(at(s.r1));
                    }
                    out.components[cid].class = Class::Affine {
                        direction,
                        k,
                        min,
                        max,
                    };
                    break 'direction;
                }
                if round == n {
                    continue 'direction;
                }
            }
        }
    }
    out.work += out.affine_work;
    Ok(out)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OrderedCell {
    pub sheet: u16,
    pub row: u32,
    pub col: u32,
    pub id: u32,
    pub owner: u32,
    pub layer: u64,
    pub cycle: Option<u64>,
    /// Part of a chain unit: one piece whose cells read earlier rows of the
    /// piece (an affine order along rows); they share one layer and are
    /// evaluated in row order.
    pub chain: bool,
}
#[derive(Debug)]
pub(crate) struct OrderedPlan {
    pub prepared: PreparedPlan,
    pub cells: Vec<OrderedCell>,
    pub work: u64,
    pub fallback_work: u64,
    pub fallback_components: u64,
    pub fallback_hits: u64,
    pub fallback_reader_arcs: u64,
    pub fallback_structural: u64,
    pub fallback_references: u64,
    pub charged: u64,
    pub discoveries: u64,
    pub peak_heap_bytes: u64,
}
impl OrderedPlan {
    pub fn heap_bytes(&self) -> u64 {
        self.prepared.heap_bytes() + (self.cells.capacity() * size_of::<OrderedCell>()) as u64
    }
}

/// Expand only one unresolved component. Tarjan member order is not geometric;
/// sort real piece indices by counted radix, then expand in the already sorted
/// top input order. Never scan all top pieces/probes once per component.
fn expand_component(
    top: &PreparedPlan,
    cid: usize,
    limit: Option<u64>,
) -> Result<PlanningInput, AuthorityError> {
    let count = top.classification.components[cid].real;
    let sorting = bytes::<usize>(count.checked_mul(2).ok_or(AuthorityError::Alloc)?)?;
    remaining(limit, sorting)?;
    let mut members: Vec<usize> = reserve(count)?;
    let mut temp: Vec<usize> = reserve(count)?;
    let mut work = 0;
    let mut cells = 0usize;
    let mut refs = 0usize;
    for &v in top.topology.components.members(cid) {
        work += 1;
        if v >= top.input.slices.len() {
            continue;
        }
        let s = &top.input.slices[v];
        let id = &top.input.identities[v];
        let n = (s.r1 - s.r0 + 1) as usize;
        cells = cells.checked_add(n).ok_or(AuthorityError::Alloc)?;
        refs = refs
            .checked_add(
                n.checked_mul(id.probe_end - id.probe_start)
                    .ok_or(AuthorityError::Alloc)?,
            )
            .ok_or(AuthorityError::Alloc)?;
        members.push(v);
        temp.push(0);
    }
    // usize indices are at most 64 bits on supported Rust targets.
    for shift in (0..usize::BITS).step_by(8) {
        let mut histogram = [0usize; 256];
        work += 256;
        for &v in &members {
            work += 1;
            histogram[(v >> shift) & 255] += 1;
        }
        let mut position = 0;
        for h in &mut histogram {
            work += 1;
            let n = *h;
            *h = position;
            position += n;
        }
        for &v in &members {
            work += 1;
            let h = &mut histogram[(v >> shift) & 255];
            temp[*h] = v;
            *h += 1;
        }
        for (dst, &src) in members.iter_mut().zip(&temp) {
            work += 1;
            *dst = src;
        }
    }
    let aggregate = add(
        add(bytes::<Slice>(cells)?, bytes::<PieceIdentity>(cells)?)?,
        add(bytes::<Probe>(refs)?, bytes::<EdgeKey>(refs)?)?,
    )?;
    let peak = add(sorting, aggregate)?;
    remaining(limit, peak)?;
    let mut out = PlanningInput {
        slices: reserve(cells)?,
        identities: reserve(cells)?,
        probes: reserve(refs)?,
        edges: reserve(refs)?,
        cells: cells as u64,
        references: refs as u64,
        work: 0,
        peak_heap_bytes: peak,
    };
    for v in members {
        work += 1;
        let s = top.input.slices[v];
        let id = top.input.identities[v];
        for row in s.r0..=s.r1 {
            work += 1;
            let reader = out.slices.len();
            out.slices.push(Slice {
                r0: row,
                r1: row,
                ..s
            });
            let start = out.probes.len();
            for &edge in &top.input.edges[id.probe_start..id.probe_end] {
                work += 1;
                if let Some(image) = edge.proj.instantiate(row, s.col) {
                    out.probes.push(Probe {
                        reader,
                        sheet: edge.proj.sheet,
                        image,
                    });
                    out.edges.push(edge);
                }
            }
            out.identities.push(PieceIdentity {
                owner: id.owner,
                first_id: id.first_id + row - s.r0,
                probe_start: start,
                probe_end: out.probes.len(),
            });
        }
    }
    out.work = work;
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn emit_cells(
    input: &PlanningInput,
    piece: usize,
    base: u64,
    class: Class,
    sigma: i64,
    cycle: Option<u64>,
    chain: bool,
    cells: &mut Vec<OrderedCell>,
    work: &mut u64,
) -> Result<(), AuthorityError> {
    let s = input.slices[piece];
    let id = input.identities[piece];
    for row in s.r0..=s.r1 {
        *work += 1;
        let offset = if chain {
            0
        } else if let Class::Affine {
            direction, k, min, ..
        } = class
        {
            (k * (direction.0 * i64::from(row) + direction.1 * i64::from(s.col)) + sigma - min)
                as u64
        } else {
            0
        };
        cells.push(OrderedCell {
            sheet: s.sheet,
            row,
            col: s.col,
            id: id.first_id + row - s.r0,
            owner: id.owner,
            layer: add(base, offset)?,
            cycle,
            chain,
        });
    }
    Ok(())
}

/// A chain unit (Program 3): an affine component of one piece whose order
/// runs down its rows (theta grows with the row). Every arc inside the
/// component goes from a row to a later one, so row order is a topological
/// order of its cells: they can share one layer and be evaluated in row
/// order, and the component's dependents start one layer later instead of
/// one per row.
fn is_chain_component(prepared: &PreparedPlan, cid: usize, c: ComponentClass) -> bool {
    let Class::Affine {
        direction,
        k,
        min,
        max,
    } = c.class
    else {
        return false;
    };
    if c.real != 1 || c.aux || max <= min || direction.1 != 0 || direction.0 * k <= 0 {
        return false;
    }
    let slices = prepared.input.slices.len();
    let mut real = prepared
        .topology
        .components
        .members(cid)
        .iter()
        .filter(|&&v| v < slices);
    matches!((real.next(), real.next()), (Some(_), None))
}

/// End-to-end exact per-cell order, prior to adaptation to scheduler::Schedule.
/// Two topology levels at most: top SCCs are proven, reused (already cells), or
/// expanded independently. No unproven SCC is reported as a cycle. Both caps
/// are shared across top + all fallback components, not reset at recursion.
pub(crate) fn plan(
    store: &Store,
    cover: &Cover,
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
    discovery_limit: Option<u64>,
) -> Result<OrderedPlan, TopologyError> {
    let prepared = prepare(store, cover, scratch_limit, arc_limit, discovery_limit)?;
    plan_prepared(prepared, scratch_limit, arc_limit, discovery_limit)
}

/// The plan of a request that is one formula cell without hints, when the
/// cell does not read itself: the cell alone at layer 0, no cycle (what
/// [`plan`] orders for it; debug builds compare). Its only possible arc is
/// a self-read (every other node is outside the request), so `None` when
/// any of its edge images contains the cell, and the caller plans in
/// general. Skips discovery's and topology's per-request allocations.
pub(crate) fn plan_single(store: &Store, cell: (u16, u32, u32)) -> Option<OrderedCell> {
    let (sheet, row, col) = cell;
    let (id, _) = store.ids().lookup(cell)?;
    let owner = store.owner_at(cell)?;
    let refined = store.refine_owner_column(owner, col, row, row, None).ok()?;
    let [piece] = refined.pieces.as_slice() else {
        return None;
    };
    for edge in &refined.edges[piece.edge_start..piece.edge_end] {
        if let Some(image) = edge.proj.forward(&piece.domain)
            && edge.proj.sheet == sheet
            && image.r0 <= row
            && row <= image.r1
            && image.c0 <= col
            && col <= image.c1
        {
            return None;
        }
    }
    Some(OrderedCell {
        sheet,
        row,
        col,
        id,
        owner: piece.owner,
        layer: 0,
        cycle: None,
        chain: false,
    })
}

/// The largest request [`plan_small`] orders.
pub(crate) const SMALL_PLAN_MAX: usize = 32;

/// The plan of a small request of formula cells and names without hints
/// (Program 3): each cell's direct precedents among the request, from its
/// own owner column refined to its row, ordered by longest path (a cell's
/// layer is one more than its precedents' deepest). `None` when a cell
/// reads itself or the request has a cycle (the caller plans in general).
/// Its layers are a valid order; debug builds check it against the arcs
/// and the planner's cells. Skips discovery's and topology's work.
pub(crate) fn plan_small(store: &Store, cells: &[(u16, u32, u32)]) -> Option<Vec<OrderedCell>> {
    let n = cells.len();
    if !(2..=SMALL_PLAN_MAX).contains(&n) {
        return None;
    }
    let mut out = Vec::with_capacity(n);
    // preds[i]: bitmask of the request cells cell i reads.
    let mut preds = [0u64; SMALL_PLAN_MAX];
    for (i, &(sheet, row, col)) in cells.iter().enumerate() {
        // Symbol nodes (names) are cells of the symbol plane with owners of
        // their own, as the planner treats them.
        let (id, _) = store.ids().lookup((sheet, row, col))?;
        let owner = store.owner_at((sheet, row, col))?;
        let refined = store.refine_owner_column(owner, col, row, row, None).ok()?;
        let [piece] = refined.pieces.as_slice() else {
            return None;
        };
        for edge in &refined.edges[piece.edge_start..piece.edge_end] {
            let Some(image) = edge.proj.forward(&piece.domain) else {
                continue;
            };
            for (j, &(s2, r2, c2)) in cells.iter().enumerate() {
                if edge.proj.sheet == s2
                    && image.r0 <= r2
                    && r2 <= image.r1
                    && image.c0 <= c2
                    && c2 <= image.c1
                {
                    if i == j {
                        return None;
                    }
                    preds[i] |= 1 << j;
                }
            }
        }
        out.push(OrderedCell {
            sheet,
            row,
            col,
            id,
            owner: piece.owner,
            layer: 0,
            cycle: None,
            chain: false,
        });
    }
    // Longest-path levels by relaxation (n <= 32); a cycle never settles.
    let mut done = 0u64;
    for _ in 0..n {
        let mut progressed = false;
        for i in 0..n {
            if done >> i & 1 == 1 || preds[i] & !done != 0 {
                continue;
            }
            let mut layer = 0;
            let mut p = preds[i];
            while p != 0 {
                let j = p.trailing_zeros() as usize;
                p &= p - 1;
                layer = layer.max(out[j].layer + 1);
            }
            out[i].layer = layer;
            done |= 1 << i;
            progressed = true;
        }
        if !progressed {
            break;
        }
    }
    if done.count_ones() as usize != n {
        return None;
    }
    #[cfg(debug_assertions)]
    for i in 0..n {
        let mut p = preds[i];
        while p != 0 {
            let j = p.trailing_zeros() as usize;
            p &= p - 1;
            debug_assert!(out[j].layer < out[i].layer, "small plan order");
        }
    }
    Some(out)
}

/// Request-local exact cell hint. Sorted by (sheet, column, row).
#[derive(Clone, Copy, Debug)]
pub(crate) struct PlanHint {
    pub reader: (u16, u32, u32),
    pub edge: EdgeKey,
}

/// Hint-bearing requests refine the static pieces to cells before adding
/// absolute point hints. Thus a hint never becomes a dependency of neighboring
/// cells in a family, and the existing ARC SCC/affine machinery stays exact.
pub(crate) fn plan_with_hints(
    store: &Store,
    cover: &Cover,
    hints: &[PlanHint],
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
    discovery_limit: Option<u64>,
) -> Result<OrderedPlan, TopologyError> {
    if hints.is_empty() {
        return plan(store, cover, scratch_limit, arc_limit, discovery_limit);
    }
    let source = input(store, cover, scratch_limit)?;
    let n = usize::try_from(source.cells).map_err(|_| AuthorityError::Alloc)?;
    let mut probes = hints.len();
    let mut work = source.work;
    for (slice, id) in source.slices.iter().zip(&source.identities) {
        work += 1;
        probes = probes
            .checked_add((slice.r1 - slice.r0 + 1) as usize * (id.probe_end - id.probe_start))
            .ok_or(AuthorityError::Alloc)?;
    }
    let aggregate = add(
        add(bytes::<Slice>(n)?, bytes::<PieceIdentity>(n)?)?,
        add(bytes::<Probe>(probes)?, bytes::<EdgeKey>(probes)?)?,
    )?;
    let live = add(source.heap_bytes(), aggregate)?;
    remaining(scratch_limit, live)?;
    let mut expanded = PlanningInput {
        slices: reserve(n)?,
        identities: reserve(n)?,
        probes: reserve(probes)?,
        edges: reserve(probes)?,
        cells: source.cells,
        references: add(source.references, hints.len() as u64)?,
        work: 0,
        peak_heap_bytes: source.peak_heap_bytes.max(live),
    };
    let mut hint = 0;
    for (slice, id) in source.slices.iter().zip(&source.identities) {
        for row in slice.r0..=slice.r1 {
            work += 1;
            let reader = expanded.slices.len();
            expanded.slices.push(Slice {
                r0: row,
                r1: row,
                ..*slice
            });
            let start = expanded.probes.len();
            let domain = super::geom::Rect::cell(row, slice.col);
            for &edge in &source.edges[id.probe_start..id.probe_end] {
                work += 1;
                if let Some(image) = edge.proj.forward(&domain) {
                    expanded.probes.push(Probe {
                        reader,
                        sheet: edge.proj.sheet,
                        image,
                    });
                    expanded.edges.push(edge);
                }
            }
            let key = (slice.sheet, slice.col, row);
            while hint < hints.len() && hints[hint].reader < key {
                work += 1;
                hint += 1;
            }
            while hint < hints.len() && hints[hint].reader == key {
                work += 1;
                let edge = hints[hint].edge;
                if let Some(image) = edge.proj.forward(&domain) {
                    expanded.probes.push(Probe {
                        reader,
                        sheet: edge.proj.sheet,
                        image,
                    });
                    expanded.edges.push(edge);
                }
                hint += 1;
            }
            expanded.identities.push(PieceIdentity {
                owner: id.owner,
                first_id: id.first_id + row - slice.r0,
                probe_start: start,
                probe_end: expanded.probes.len(),
            });
        }
    }
    expanded.work = work;
    drop(source);
    let prepared = prepare_input(expanded, scratch_limit, arc_limit, discovery_limit)?;
    plan_prepared(prepared, scratch_limit, arc_limit, discovery_limit)
}

fn plan_prepared(
    prepared: PreparedPlan,
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
    discovery_limit: Option<u64>,
) -> Result<OrderedPlan, TopologyError> {
    let count = usize::try_from(prepared.input.cells).map_err(|_| AuthorityError::Alloc)?;
    let output_bytes = bytes::<OrderedCell>(count)?;
    let starts_bytes = bytes::<u64>(prepared.topology.components.len())?;
    let held = add(prepared.heap_bytes(), add(output_bytes, starts_bytes)?)?;
    remaining(scratch_limit, held)?;
    let mut out = OrderedPlan {
        peak_heap_bytes: prepared.peak_heap_bytes.max(held),
        work: prepared.total_work(),
        fallback_work: 0,
        fallback_components: 0,
        fallback_hits: 0,
        fallback_reader_arcs: 0,
        fallback_structural: 0,
        fallback_references: 0,
        charged: prepared.topology.emission.charged,
        discoveries: prepared.topology.sweep.hits.len() as u64,
        prepared,
        cells: reserve(count)?,
    };
    let mut starts: Vec<u64> = reserve(out.prepared.topology.components.len())?;
    starts.extend((0..out.prepared.topology.components.len()).map(|_| {
        out.work += 1;
        0
    }));
    let mut cycles = 0u64;
    for cid in (0..starts.len()).rev() {
        out.work += 1;
        let c = out.prepared.classification.components[cid];
        let base = starts[cid];
        let span = if c.class == Class::Refine {
            let sub = expand_component(&out.prepared, cid, remaining(scratch_limit, held)?)?;
            out.peak_heap_bytes = out.peak_heap_bytes.max(add(held, sub.peak_heap_bytes)?);
            let subheld = add(held, sub.heap_bytes())?;
            // Existing top charges never exceed these limits (prepare checked).
            let arcs_left = arc_limit.map(|n| n - out.charged);
            let discovery_left = discovery_limit.map(|n| n - out.discoveries);
            let topo = topology(
                &sub.slices,
                &sub.probes,
                remaining(scratch_limit, subheld)?,
                arcs_left,
                discovery_left,
            )?;
            out.peak_heap_bytes = out.peak_heap_bytes.max(add(subheld, topo.peak_heap_bytes)?);
            out.charged = add(out.charged, topo.emission.charged)?;
            out.discoveries = add(out.discoveries, topo.sweep.hits.len() as u64)?;
            out.fallback_components += 1;
            out.fallback_hits = add(out.fallback_hits, topo.emission.pairwise_equivalent_hits)?;
            out.fallback_reader_arcs = add(out.fallback_reader_arcs, topo.emission.reader_arcs)?;
            out.fallback_structural = add(
                out.fallback_structural,
                add(topo.emission.structural_arcs, topo.emission.aux_nodes)?,
            )?;
            out.fallback_references = add(out.fallback_references, sub.references)?;
            let before = out.work;
            out.work += sub.work + topo.total_work();
            let local_live = add(
                add(subheld, topo.heap_bytes())?,
                bytes::<u64>(topo.components.len())?,
            )?;
            remaining(scratch_limit, local_live)?;
            out.peak_heap_bytes = out.peak_heap_bytes.max(local_live);
            let mut local: Vec<u64> = reserve(topo.components.len())?;
            local.extend((0..topo.components.len()).map(|_| {
                out.work += 1;
                0
            }));
            let mut span = 0;
            for scid in (0..local.len()).rev() {
                out.work += 1;
                let mut real = 0;
                let mut selfdep = false;
                for &v in topo.components.members(scid) {
                    out.work += 1;
                    if v < sub.slices.len() {
                        real += 1;
                        selfdep |= topo.emission.selfdep[v] != 0;
                    }
                }
                let cycle = if real > 1 || selfdep {
                    let id = cycles;
                    cycles = add(cycles, 1)?;
                    Some(id)
                } else {
                    None
                };
                let next = add(local[scid], u64::from(real > 0))?;
                span = span.max(next);
                for &v in topo.components.members(scid) {
                    out.work += 1;
                    if v < sub.slices.len() {
                        emit_cells(
                            &sub,
                            v,
                            add(base, local[scid])?,
                            Class::Cell {
                                genuine_cycle: cycle.is_some(),
                            },
                            0,
                            cycle,
                            false,
                            &mut out.cells,
                            &mut out.work,
                        )?;
                    }
                    for &to in topo.graph.successors(v) {
                        out.work += 1;
                        let target = topo.components.component_of[to];
                        if target != scid {
                            local[target] = local[target].max(next);
                        }
                    }
                }
            }
            out.fallback_work += out.work - before;
            span
        } else {
            let chain = is_chain_component(&out.prepared, cid, c);
            let span = match c.class {
                Class::Auxiliary => 0,
                Class::Affine { .. } if chain => 1,
                Class::Affine { min, max, .. } => (max - min + 1) as u64,
                _ => 1,
            };
            let cycle = if matches!(
                c.class,
                Class::Cell {
                    genuine_cycle: true
                }
            ) {
                let id = cycles;
                cycles = add(cycles, 1)?;
                Some(id)
            } else {
                None
            };
            for &v in out.prepared.topology.components.members(cid) {
                out.work += 1;
                if v < out.prepared.input.slices.len() {
                    emit_cells(
                        &out.prepared.input,
                        v,
                        base,
                        c.class,
                        out.prepared.classification.sigma[v],
                        cycle,
                        chain,
                        &mut out.cells,
                        &mut out.work,
                    )?;
                }
            }
            span
        };
        let next = add(base, span)?;
        for &v in out.prepared.topology.components.members(cid) {
            out.work += 1;
            for &to in out.prepared.topology.graph.successors(v) {
                out.work += 1;
                let target = out.prepared.topology.components.component_of[to];
                if target != cid {
                    starts[target] = starts[target].max(next);
                }
            }
        }
    }
    debug_assert_eq!(out.cells.len(), count);
    Ok(out)
}
