//! Fallible ARC emission from counted column-hit intervals.
//!
//! Input real nodes are single-column refined pieces, in column/row order.
//! A hit is a half-open range of those pieces in one column. The counting
//! sweep and projection/refinement driver must establish these hit intervals;
//! this module does not discover them or assert a whole-planner bound.
//! Ownership is one-way: real -> leaf -> parent -> reader.

use super::plan_control::PlanControl;
use super::store::AuthorityError;
use formualizer_common::ExcelError;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Column {
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Hit {
    pub reader: usize,
    pub column: usize,
    /// Global real-node interval, contained in the named column.
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PairWitness {
    /// Input hit (hence projection) supplying the displacement box.
    pub hit: usize,
    pub precedent: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EmitWork {
    pub initialize: u64,
    pub columns: u64,
    pub hits: u64,
    pub pairwise: u64,
    pub canonical: u64,
    pub structural: u64,
}

impl EmitWork {
    pub fn total(self) -> u64 {
        self.initialize
            + self.columns
            + self.hits
            + self.pairwise
            + self.canonical
            + self.structural
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EmitError {
    Authority(AuthorityError),
    Runtime(ExcelError),
    InvalidInput,
    /// Emitted reader arcs + owner/tree arcs + auxiliary nodes. No partial
    /// graph escapes. The caller must also cap/count sweep work separately.
    SuperLinearWork {
        charged: u64,
        limit: u64,
        work: EmitWork,
    },
}

impl From<AuthorityError> for EmitError {
    fn from(error: AuthorityError) -> Self {
        Self::Authority(error)
    }
}

impl From<ExcelError> for EmitError {
    fn from(error: ExcelError) -> Self {
        Self::Runtime(error)
    }
}

#[derive(Debug)]
pub(crate) struct Emission {
    pub nodes: usize,
    pub arcs: Vec<(usize, usize)>,
    /// Includes self hits even though pairwise self-arcs are omitted. These
    /// are needed by affine classification; ARC hits have no pairwise box.
    pub pair_witnesses: Vec<PairWitness>,
    pub selfdep: Vec<u8>,
    pub reader_arcs: u64,
    pub structural_arcs: u64,
    pub aux_nodes: u64,
    pub pairwise_equivalent_hits: u64,
    pub charged: u64,
    pub work: EmitWork,
    pub peak_heap_bytes: u64,
}

impl Emission {
    pub fn heap_bytes(&self) -> u64 {
        (self.arcs.capacity() * size_of::<(usize, usize)>()
            + self.pair_witnesses.capacity() * size_of::<PairWitness>()
            + self.selfdep.capacity()) as u64
    }
}

fn bytes<T>(n: usize) -> Result<u64, AuthorityError> {
    n.checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(AuthorityError::Alloc)
}

fn add(a: u64, b: u64) -> Result<u64, AuthorityError> {
    a.checked_add(b).ok_or(AuthorityError::Alloc)
}

fn reserve<T>(n: usize) -> Result<Vec<T>, AuthorityError> {
    let mut out = Vec::new();
    out.try_reserve_exact(n)
        .map_err(|_| AuthorityError::Alloc)?;
    Ok(out)
}

fn admit(needed: u64, limit: Option<u64>) -> Result<(), AuthorityError> {
    if let Some(limit) = limit
        && needed > limit
    {
        return Err(AuthorityError::Admission {
            resource: "scratch",
            needed,
            limit,
        });
    }
    Ok(())
}

fn charge(used: &mut u64, n: u64, limit: Option<u64>, work: EmitWork) -> Result<(), EmitError> {
    *used = add(*used, n)?;
    if let Some(limit) = limit
        && *used > limit
    {
        return Err(EmitError::SuperLinearWork {
            charged: *used,
            limit,
            work,
        });
    }
    Ok(())
}

fn threshold(n: usize) -> usize {
    // ceil(log2(n+1)) == bit length of positive n; no n+1 overflow.
    2 * (usize::BITS - n.leading_zeros()) as usize
}

/// Implicit segment tree with leaves n..2n and root 1. Works for non-power
/// of two n as well: a selected node always denotes precisely its selected
/// leaves. No tree heap or recursive traversal is needed.
fn canonical(
    mut lo: usize,
    mut hi: usize,
    n: usize,
    work: &mut EmitWork,
    mut emit: impl FnMut(usize),
    control: &mut PlanControl<impl FnMut(u64) -> Result<(), ExcelError>>,
) -> Result<(), EmitError> {
    if lo == 0 && hi == n {
        work.canonical += 1;
        control.tick()?;
        emit(1);
        return Ok(());
    }
    lo += n;
    hi += n;
    while lo < hi {
        work.canonical += 1;
        control.tick()?;
        if lo & 1 != 0 {
            emit(lo);
            lo += 1;
        }
        if hi & 1 != 0 {
            hi -= 1;
            emit(hi);
        }
        lo /= 2;
        hi /= 2;
    }
    Ok(())
}

/// Exact reservation using a counted sizing pass, followed by a counted
/// filling pass. All helper-owned buffers coexist and are admitted together;
/// the caller subtracts borrowed input and other live plan storage first.
/// `arc_limit` is the remaining plan-wide structural/emission allowance.
pub(crate) fn emit(
    real: usize,
    columns: &[Column],
    hits: &[Hit],
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
) -> Result<Emission, EmitError> {
    emit_controlled(real, columns, hits, scratch_limit, arc_limit, |_| Ok(()))
}

pub(crate) fn emit_controlled(
    real: usize,
    columns: &[Column],
    hits: &[Hit],
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
    checkpoint: impl FnMut(u64) -> Result<(), ExcelError>,
) -> Result<Emission, EmitError> {
    let mut control = PlanControl::new(checkpoint)?;
    let mut work = EmitWork::default();
    let mut previous = 0;
    for col in columns {
        work.columns += 1;
        control.tick()?;
        if col.start != previous || col.start >= col.end || col.end > real {
            return Err(EmitError::InvalidInput);
        }
        previous = col.end;
    }
    if previous != real {
        return Err(EmitError::InvalidInput);
    }
    let temporary = bytes::<usize>(columns.len())?;
    admit(temporary, scratch_limit)?;
    let mut bases = reserve(columns.len())?;
    for _ in columns {
        work.initialize += 1;
        control.tick()?;
        bases.push(usize::MAX);
    }
    let mut nodes = real;
    let mut reader_arcs = 0u64;
    let mut structural_arcs = 0u64;
    let mut aux_nodes = 0u64;
    let mut witnesses = 0usize;
    let mut h = 0u64;
    let mut charged = 0u64;
    for hit in hits {
        work.hits += 1;
        control.tick()?;
        let Some(col) = columns.get(hit.column) else {
            return Err(EmitError::InvalidInput);
        };
        if hit.reader >= real || hit.start < col.start || hit.end > col.end || hit.start > hit.end {
            return Err(EmitError::InvalidInput);
        }
        let n = col.end - col.start;
        let k = hit.end - hit.start;
        h = add(h, k as u64)?;
        if k <= threshold(n) {
            witnesses = witnesses.checked_add(k).ok_or(AuthorityError::Alloc)?;
            for precedent in hit.start..hit.end {
                work.pairwise += 1;
                control.tick()?;
                if precedent != hit.reader {
                    reader_arcs = add(reader_arcs, 1)?;
                    charge(&mut charged, 1, arc_limit, work)?;
                }
            }
        } else {
            if bases[hit.column] == usize::MAX {
                let tree_nodes = n
                    .checked_mul(2)
                    .and_then(|n| n.checked_sub(1))
                    .ok_or(AuthorityError::Alloc)?;
                bases[hit.column] = nodes;
                nodes = nodes.checked_add(tree_nodes).ok_or(AuthorityError::Alloc)?;
                aux_nodes = add(aux_nodes, tree_nodes as u64)?;
                let arcs = add((tree_nodes - 1) as u64, n as u64)?;
                structural_arcs = add(structural_arcs, arcs)?;
                charge(&mut charged, add(tree_nodes as u64, arcs)?, arc_limit, work)?;
            }
            let mut count = 0u64;
            canonical(
                hit.start - col.start,
                hit.end - col.start,
                n,
                &mut work,
                |_| count += 1,
                &mut control,
            )?;
            reader_arcs = add(reader_arcs, count)?;
            charge(&mut charged, count, arc_limit, work)?;
        }
    }
    let arc_count =
        usize::try_from(add(reader_arcs, structural_arcs)?).map_err(|_| AuthorityError::Alloc)?;
    let peak = add(
        temporary,
        add(
            bytes::<(usize, usize)>(arc_count)?,
            add(bytes::<PairWitness>(witnesses)?, bytes::<u8>(real)?)?,
        )?,
    )?;
    admit(peak, scratch_limit)?;
    let mut arcs = reserve(arc_count)?;
    let mut pair_witnesses = reserve(witnesses)?;
    let mut selfdep = reserve(real)?;
    for _ in 0..real {
        work.initialize += 1;
        control.tick()?;
        selfdep.push(0);
    }
    for (hit_id, hit) in hits.iter().enumerate() {
        work.hits += 1;
        control.tick()?;
        let col = &columns[hit.column];
        if hit.start <= hit.reader && hit.reader < hit.end {
            selfdep[hit.reader] = 1;
        }
        let n = col.end - col.start;
        if hit.end - hit.start <= threshold(n) {
            for precedent in hit.start..hit.end {
                work.pairwise += 1;
                control.tick()?;
                pair_witnesses.push(PairWitness {
                    hit: hit_id,
                    precedent,
                });
                if precedent != hit.reader {
                    arcs.push((precedent, hit.reader));
                }
            }
        } else {
            let base = bases[hit.column];
            canonical(
                hit.start - col.start,
                hit.end - col.start,
                n,
                &mut work,
                |node| {
                    arcs.push((base + node - 1, hit.reader));
                },
                &mut control,
            )?;
        }
    }
    for (col, &base) in columns.iter().zip(&bases) {
        work.columns += 1;
        control.tick()?;
        if base == usize::MAX {
            continue;
        }
        let n = col.end - col.start;
        for node in 2..2 * n {
            work.structural += 1;
            control.tick()?;
            arcs.push((base + node - 1, base + node / 2 - 1));
        }
        for offset in 0..n {
            work.structural += 1;
            control.tick()?;
            arcs.push((col.start + offset, base + n + offset - 1));
        }
    }
    debug_assert_eq!(arcs.len(), arc_count);
    debug_assert_eq!(pair_witnesses.len(), witnesses);
    control.flush()?;
    Ok(Emission {
        nodes,
        arcs,
        pair_witnesses,
        selfdep,
        reader_arcs,
        structural_arcs,
        aux_nodes,
        pairwise_equivalent_hits: h,
        charged,
        work,
        peak_heap_bytes: peak,
    })
}
