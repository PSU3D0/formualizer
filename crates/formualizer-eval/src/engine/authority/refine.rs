//! Planner input refinement over independently partitioned owners and edges.
//!
//! An owner-column intersection is split at incident edge endpoints, not at
//! every cell and not by repeatedly crossing every edge with every piece.
//! The two sweeps cost O(events + output references); active-edge removal is
//! O(1). Output references are bounded by the cell-reference incidences in
//! the input slice, because every emitted piece contains at least one cell.
//! Discovery additionally pays the authority's counted dependent-index query.
//!
//! This is the input stage, NOT an evaluation planner. No runtime scheduler
//! uses it yet. See design addendum B-26.

use super::geom::Rect;
use super::store::{AuthorityError, EdgeKey, Store};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RefinementWork {
    pub index: u64,
    pub discovery: u64,
    pub sort: u64,
    pub events: u64,
    pub pieces: u64,
    pub references: u64,
}

impl RefinementWork {
    pub fn total(&self) -> u64 {
        self.index + self.discovery + self.sort + self.events + self.pieces + self.references
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefinedPiece {
    pub owner: u32,
    pub sheet: u16,
    pub domain: Rect,
    /// Half-open range in `RefinedSlice::edges`.
    pub edge_start: usize,
    pub edge_end: usize,
}

#[derive(Debug)]
pub struct RefinedSlice {
    pub pieces: Vec<RefinedPiece>,
    pub edges: Vec<EdgeKey>,
    pub work: RefinementWork,
    /// Maximum simultaneously live heap allocated by this call, including
    /// output, endpoint sorting, and both active-list link arrays.
    pub peak_heap_bytes: u64,
    /// Plan-local R contribution: sum of references over candidate cells,
    /// not edge-record count or emitted-piece reference count.
    pub cell_references: u64,
}

impl RefinedSlice {
    /// Heap bytes the result keeps (its output vectors).
    pub(crate) fn retained_heap_bytes(&self) -> u64 {
        (self.pieces.capacity() * size_of::<RefinedPiece>()
            + self.edges.capacity() * size_of::<EdgeKey>()) as u64
    }
}

#[derive(Clone, Copy, Default)]
struct Event {
    row: u32,
    edge: usize,
    start: bool,
}

fn bytes<T>(n: usize) -> Result<u64, AuthorityError> {
    n.checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(AuthorityError::Alloc)
}

fn add(a: u64, b: u64) -> Result<u64, AuthorityError> {
    a.checked_add(b).ok_or(AuthorityError::Alloc)
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

fn reserved<T>(n: usize) -> Result<Vec<T>, AuthorityError> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| AuthorityError::Alloc)?;
    Ok(v)
}

/// Stable three-byte radix sort: rows include the exclusive MAX_ROW+1.
/// Equal endpoints need no kind order: the sweep applies *all* events at a
/// row before emitting [row, next_row). This is deliberately distinct from
/// ARC's closed-interval START < Q_LO < Q_HI < END ordering.
///
/// Short lists (the common singleton or small-family column: a handful of
/// events) use an in-place comparison sort instead: the radix passes cost
/// three 256-bucket histograms regardless of length, which dominated
/// planning of workbooks made of many small owners (perf gate, red team #4).
fn sort_events(events: &mut [Event], temp: &mut [Event], work: &mut RefinementWork) {
    const RADIX_MIN: usize = 64;
    if events.len() < RADIX_MIN {
        let n = events.len() as u64;
        // Comparisons of an insertion/merge sort on n <= 64 elements.
        work.sort += n * u64::from(64 - n.leading_zeros()).max(1);
        events.sort_unstable_by_key(|e| e.row);
        return;
    }
    for shift in [0, 8, 16] {
        let mut counts = [0usize; 256];
        work.sort += 256; // histogram initialization
        for e in events.iter() {
            work.sort += 1;
            counts[((e.row >> shift) & 255) as usize] += 1;
        }
        let mut pos = 0;
        for n in &mut counts {
            work.sort += 1;
            let count = *n;
            *n = pos;
            pos += count;
        }
        for &e in events.iter() {
            work.sort += 1;
            let bucket = ((e.row >> shift) & 255) as usize;
            temp[counts[bucket]] = e;
            counts[bucket] += 1;
        }
        for (dst, src) in events.iter_mut().zip(temp.iter()) {
            work.sort += 1;
            *dst = *src;
        }
    }
}

const NONE: usize = usize::MAX;

/// Replay the active list without any allocation or search. The same walk
/// sizes and fills output. Every visit is counted, including sizing work.
fn sweep(
    domain: Rect,
    events: &[Event],
    prev: &mut [usize],
    next: &mut [usize],
    work: &mut RefinementWork,
    mut emit_piece: impl FnMut(Rect),
    mut emit_edge: impl FnMut(usize),
) {
    let mut head = NONE;
    let mut i = 0;
    let mut row = domain.r0;
    while row <= domain.r1 {
        while i < events.len() && events[i].row == row {
            work.events += 1;
            let e = events[i];
            if e.start {
                prev[e.edge] = NONE;
                next[e.edge] = head;
                if head != NONE {
                    prev[head] = e.edge;
                }
                head = e.edge;
            } else {
                let (p, n) = (prev[e.edge], next[e.edge]);
                if p == NONE {
                    head = n;
                } else {
                    next[p] = n;
                }
                if n != NONE {
                    prev[n] = p;
                }
            }
            i += 1;
        }
        let end = events.get(i).map_or(domain.r1 + 1, |e| e.row);
        debug_assert!(row < end);
        work.pieces += 1;
        emit_piece(Rect::new(row, domain.c0, end - 1, domain.c0));
        let mut active = head;
        while active != NONE {
            work.references += 1;
            emit_edge(active);
            active = next[active];
        }
        row = end;
    }
    // End events at domain.r1+1 need no list update and incur no sweep
    // visits. Their construction/sort was counted above; starts initialize
    // every used link afresh on the next pass.
}

impl Store {
    /// Refine one column of a live owner, intersected with candidate rows.
    /// The caller supplies a live owner handle (as for `owner_dom`). Empty
    /// intersections allocate nothing. Repeated calls retain no scratch.
    ///
    /// `scratch_limit` is the remaining plan budget, AFTER accounting for
    /// caller-owned candidate covers and any earlier, still-live output.
    /// This helper charges its output as scratch as well as temporary work.
    pub(crate) fn refine_owner_column(
        &self,
        owner: u32,
        col: u32,
        r0: u32,
        r1: u32,
        scratch_limit: Option<u64>,
    ) -> Result<RefinedSlice, AuthorityError> {
        let (sheet, owner_domain, _) = self.owner_dom(owner);
        let mut out = RefinedSlice {
            pieces: Vec::new(),
            edges: Vec::new(),
            work: RefinementWork::default(),
            peak_heap_bytes: 0,
            cell_references: 0,
        };
        if r0 > r1 || col < owner_domain.c0 || col > owner_domain.c1 {
            return Ok(out);
        }
        let Some(domain) = owner_domain.intersect(&Rect::new(r0, col, r1, col)) else {
            return Ok(out);
        };
        // The counting query keeps the first few edges on the stack (no
        // heap, so admission is unchanged): a short column is not queried
        // twice.
        const INLINE_EDGES: usize = 32;
        let mut inline: smallvec::SmallVec<[(EdgeKey, Rect); INLINE_EDGES]> =
            smallvec::SmallVec::new();
        let mut count = 0usize;
        out.work.index += self.visit_plan_edges(sheet, &domain, &mut |key, dep| {
            out.work.discovery += 1;
            if count < INLINE_EDGES {
                inline.push((key, dep));
            }
            count += 1;
        });
        let event_count = count.checked_mul(2).ok_or(AuthorityError::Alloc)?;
        let event_bytes = bytes::<Event>(event_count)?;
        let link_bytes = bytes::<usize>(count)?;
        let temporary = add(
            add(bytes::<EdgeKey>(count)?, add(event_bytes, event_bytes)?)?,
            add(link_bytes, link_bytes)?,
        )?;
        admit(temporary, scratch_limit)?;
        let mut keys = reserved::<EdgeKey>(count)?;
        let mut events = reserved::<Event>(event_count)?;
        let mut temp = reserved::<Event>(event_count)?;
        let mut prev = reserved::<usize>(count)?;
        let mut next = reserved::<usize>(count)?;
        // All backing buffers have been reserved. No push below allocates.
        for _ in 0..event_count {
            out.work.events += 1;
            temp.push(Event::default());
        }
        for _ in 0..count {
            out.work.events += 2;
            prev.push(NONE);
            next.push(NONE);
        }
        let mut add_edge = |key: EdgeKey, dep: Rect| {
            let dep = dep
                .intersect(&domain)
                .expect("index returned a disjoint edge");
            let edge = keys.len();
            keys.push(key);
            events.push(Event {
                row: dep.r0,
                edge,
                start: true,
            });
            events.push(Event {
                row: dep.r1 + 1,
                edge,
                start: false,
            });
            out.cell_references += dep.area();
        };
        if count <= INLINE_EDGES {
            for (key, dep) in inline {
                add_edge(key, dep);
            }
        } else {
            out.work.index += self.visit_plan_edges(sheet, &domain, &mut |key, dep| {
                out.work.discovery += 1;
                add_edge(key, dep);
            });
        }
        debug_assert_eq!(keys.len(), count);
        sort_events(&mut events, &mut temp, &mut out.work);
        let (mut pieces, mut references) = (0usize, 0usize);
        sweep(
            domain,
            &events,
            &mut prev,
            &mut next,
            &mut out.work,
            |_| pieces += 1,
            |_| references += 1,
        );
        let peak = add(
            temporary,
            add(
                bytes::<RefinedPiece>(pieces)?,
                bytes::<EdgeKey>(references)?,
            )?,
        )?;
        admit(peak, scratch_limit)?;
        out.pieces = reserved::<RefinedPiece>(pieces)?;
        out.edges = reserved::<EdgeKey>(references)?;
        // Piece boundaries are recovered from the counted active-list length
        // in a separate cursor; never scan the output for an owner or edge.
        let edge_cursor = std::cell::Cell::new(0usize);
        sweep(
            domain,
            &events,
            &mut prev,
            &mut next,
            &mut out.work,
            |domain| {
                let start = edge_cursor.get();
                if let Some(p) = out.pieces.last_mut() {
                    p.edge_end = start;
                }
                out.pieces.push(RefinedPiece {
                    owner,
                    sheet,
                    domain,
                    edge_start: start,
                    edge_end: start,
                });
            },
            |edge| {
                out.edges.push(keys[edge]);
                edge_cursor.set(edge_cursor.get() + 1);
            },
        );
        if let Some(p) = out.pieces.last_mut() {
            p.edge_end = out.edges.len();
        }
        debug_assert_eq!(out.pieces.len(), pieces);
        debug_assert_eq!(out.edges.len(), references);
        out.peak_heap_bytes = peak;
        Ok(out)
    }
}
