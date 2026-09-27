//! Counted closed-interval sweep for single-column refined pieces.
//! START < Q_LO < Q_HI < END is load-bearing: touching either endpoint hits.
//! Input slice order is (sheet, column, row); readers use that same numbering.

use super::arc_emit::{Column, Hit};
use super::geom::{MAX_COL, MAX_ROW, Rect};
use super::plan_control::PlanControl;
use super::store::AuthorityError;
use formualizer_common::ExcelError;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Slice {
    pub sheet: u16,
    pub col: u32,
    pub r0: u32,
    pub r1: u32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Probe {
    pub reader: usize,
    pub sheet: u16,
    pub image: Rect,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SweepWork {
    pub input: u64,
    pub initialize: u64,
    pub sort: u64,
    pub columns: u64,
    pub pairs: u64,
    pub keys: u64,
}
impl SweepWork {
    pub fn total(self) -> u64 {
        self.input + self.initialize + self.sort + self.columns + self.pairs + self.keys
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SweepError {
    Authority(AuthorityError),
    Runtime(ExcelError),
    InvalidInput,
    /// Candidate query-column incidences, including geometrically empty hits.
    DiscoveryLimit {
        needed: u64,
        limit: u64,
        work: SweepWork,
    },
}
impl From<AuthorityError> for SweepError {
    fn from(e: AuthorityError) -> Self {
        Self::Authority(e)
    }
}

impl From<ExcelError> for SweepError {
    fn from(error: ExcelError) -> Self {
        Self::Runtime(error)
    }
}

#[derive(Debug)]
pub(crate) struct Sweep {
    pub columns: Vec<Column>,
    pub hits: Vec<Hit>,
    /// Parallel to hits: the projection witness, independent of sorting.
    pub probes: Vec<usize>,
    pub work: SweepWork,
    pub peak_heap_bytes: u64,
}
impl Sweep {
    pub fn heap_bytes(&self) -> u64 {
        (self.columns.capacity() * size_of::<Column>()
            + self.hits.capacity() * size_of::<Hit>()
            + self.probes.capacity() * size_of::<usize>()) as u64
    }
}

#[derive(Clone, Copy, Default)]
struct Key {
    key: u64,
    payload: usize,
}

fn bytes<T>(n: usize) -> Result<u64, AuthorityError> {
    n.checked_mul(size_of::<T>())
        .and_then(|v| u64::try_from(v).ok())
        .ok_or(AuthorityError::Alloc)
}
fn add(a: u64, b: u64) -> Result<u64, AuthorityError> {
    a.checked_add(b).ok_or(AuthorityError::Alloc)
}
fn reserve<T>(n: usize) -> Result<Vec<T>, AuthorityError> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| AuthorityError::Alloc)?;
    Ok(v)
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
fn colkey(sheet: u16, col: u32) -> u64 {
    (u64::from(sheet) << 14) | u64::from(col)
}
fn event_key(column: usize, row: u32, kind: u64) -> u64 {
    ((column as u64) << 22) | (u64::from(row) << 2) | kind
}

/// Eight byte passes, stable, no comparison-sort tail and no heap histogram.
fn sort(
    keys: &mut [Key],
    temp: &mut [Key],
    work: &mut SweepWork,
    control: &mut PlanControl<impl FnMut(u64) -> Result<(), ExcelError>>,
) -> Result<(), SweepError> {
    if keys.is_empty() {
        return Ok(());
    }
    // Work is accounted per pass exactly as the element-wise loops count
    // it (256 buckets twice, three element passes), charged in batches.
    // A pass whose byte is equal in every key is a no-op of a stable sort
    // and is skipped (its work is still charged).
    let n = keys.len() as u64;
    let first = keys[0].key;
    let differs = keys.iter().fold(0u64, |d, k| d | (k.key ^ first));
    // A few keys (a small edit's plan): one stable comparison sort orders
    // exactly as the stable byte passes; the passes' work is still charged.
    let small = keys.len() <= SMALL_SORT;
    for shift in (0..64).step_by(8) {
        let pass = 512 + 3 * n;
        work.sort += pass;
        control.charge(pass)?;
        if small || (differs >> shift) & 255 == 0 {
            continue;
        }
        let mut counts = [0usize; 256];
        for k in keys.iter() {
            counts[((k.key >> shift) & 255) as usize] += 1;
        }
        let mut pos = 0;
        for c in &mut counts {
            let n = *c;
            *c = pos;
            pos += n;
        }
        for &k in keys.iter() {
            let digit = ((k.key >> shift) & 255) as usize;
            temp[counts[digit]] = k;
            counts[digit] += 1;
        }
        keys.copy_from_slice(&temp[..keys.len()]);
    }
    if small {
        keys.sort_by_key(|k| k.key);
    }
    Ok(())
}

/// Key counts sorted by one comparison sort instead of byte passes.
const SMALL_SORT: usize = 32;

/// The caller has already computed exact images from refined piece/edge
/// projections. No store scan, cell expansion, or per-probe binary search.
/// Borrowed slices/probes are excluded from the supplied remaining budget.
/// `discovery_limit` caps query-column incidences during the sizing merge,
/// before allocating or filling hit/event arrays. Empty row hits count too.
/// It is separate from the emitter's structural/arc allowance; a future
/// planner must pass the remaining per-plan discovery allowance at each level.
pub(crate) fn sweep(
    slices: &[Slice],
    probes: &[Probe],
    limit: Option<u64>,
    discovery_limit: Option<u64>,
) -> Result<Sweep, SweepError> {
    sweep_controlled(slices, probes, limit, discovery_limit, |_| Ok(()))
}

pub(crate) fn sweep_controlled(
    slices: &[Slice],
    probes: &[Probe],
    limit: Option<u64>,
    discovery_limit: Option<u64>,
    checkpoint: impl FnMut(u64) -> Result<(), ExcelError>,
) -> Result<Sweep, SweepError> {
    let mut control = PlanControl::new(checkpoint)?;
    let mut work = SweepWork::default();
    if slices.len() > u32::MAX as usize {
        return Err(SweepError::InvalidInput);
    }
    let mut columns = 0usize;
    let mut previous: Option<Slice> = None;
    for &s in slices {
        work.input += 1;
        control.tick()?;
        if s.col > MAX_COL || s.r0 > s.r1 || s.r1 > MAX_ROW {
            return Err(SweepError::InvalidInput);
        }
        if let Some(p) = previous {
            let (pk, sk) = (colkey(p.sheet, p.col), colkey(s.sheet, s.col));
            if sk < pk || (sk == pk && s.r0 <= p.r1) {
                return Err(SweepError::InvalidInput);
            }
            if sk != pk {
                columns += 1;
            }
        } else {
            columns += 1;
        }
        previous = Some(s);
    }
    for q in probes {
        work.input += 1;
        control.tick()?;
        if q.reader >= slices.len()
            || q.image.r0 > q.image.r1
            || q.image.c0 > q.image.c1
            || q.image.r1 > MAX_ROW
            || q.image.c1 > MAX_COL
        {
            return Err(SweepError::InvalidInput);
        }
    }
    let phase1 = add(
        add(bytes::<Column>(columns)?, bytes::<u64>(columns)?)?,
        bytes::<Key>(probes.len())?
            .checked_mul(2)
            .ok_or(AuthorityError::Alloc)?,
    )?;
    admit(phase1, limit)?;
    let mut cols: Vec<Column> = reserve(columns)?;
    let mut cc = reserve(columns)?;
    let mut starts = reserve(probes.len())?;
    let mut start_temp = reserve(probes.len())?;
    for (i, s) in slices.iter().enumerate() {
        work.input += 1;
        control.tick()?;
        let key = colkey(s.sheet, s.col);
        if cc.last() != Some(&key) {
            cc.push(key);
            cols.push(Column {
                start: i,
                end: i + 1,
            });
        } else {
            cols.last_mut().expect("column exists").end = i + 1;
        }
    }
    for (i, q) in probes.iter().enumerate() {
        work.input += 1;
        control.tick()?;
        starts.push(Key {
            key: colkey(q.sheet, q.image.c0),
            payload: i,
        });
        start_temp.push(Key::default());
        work.initialize += 1;
        control.tick()?;
    }
    sort(&mut starts, &mut start_temp, &mut work, &mut control)?;
    // First merge sizes exact query-column incidences. The fill merge repeats
    // the identical work, and both passes increment the counters.
    let mut pairs = 0usize;
    let mut ptr = 0;
    for k in &starts {
        work.input += 1;
        control.tick()?;
        while ptr < cc.len() && cc[ptr] < k.key {
            work.columns += 1;
            control.tick()?;
            ptr += 1;
        }
        let q = &probes[k.payload];
        let end = colkey(q.sheet, q.image.c1);
        let mut c = ptr;
        while c < cc.len() && cc[c] <= end {
            work.pairs += 1;
            control.tick()?;
            if let Some(limit) = discovery_limit
                && work.pairs > limit
            {
                return Err(SweepError::DiscoveryLimit {
                    needed: work.pairs,
                    limit,
                    work,
                });
            }
            pairs = pairs.checked_add(1).ok_or(AuthorityError::Alloc)?;
            c += 1;
        }
    }
    let key_count = slices
        .len()
        .checked_add(pairs)
        .and_then(|n| n.checked_mul(2))
        .ok_or(AuthorityError::Alloc)?;
    let phase2 = add(
        phase1,
        add(
            add(bytes::<Hit>(pairs)?, bytes::<usize>(pairs)?)?,
            bytes::<Key>(key_count)?
                .checked_mul(2)
                .ok_or(AuthorityError::Alloc)?,
        )?,
    )?;
    admit(phase2, limit)?;
    let mut hits = reserve(pairs)?;
    let mut query_ids = reserve(pairs)?;
    let mut keys = reserve(key_count)?;
    let mut temp = reserve(key_count)?;
    for (column, col) in cols.iter().enumerate() {
        work.columns += 1;
        control.tick()?;
        for s in &slices[col.start..col.end] {
            work.keys += 2;
            control.charge(2)?;
            keys.push(Key {
                key: event_key(column, s.r0, 0),
                payload: 0,
            });
            keys.push(Key {
                key: event_key(column, s.r1, 3),
                payload: 0,
            });
        }
    }
    ptr = 0;
    for k in &starts {
        work.input += 1;
        control.tick()?;
        while ptr < cc.len() && cc[ptr] < k.key {
            work.columns += 1;
            control.tick()?;
            ptr += 1;
        }
        let q = &probes[k.payload];
        let end = colkey(q.sheet, q.image.c1);
        let mut c = ptr;
        while c < cc.len() && cc[c] <= end {
            work.pairs += 1;
            control.tick()?;
            let id = hits.len();
            hits.push(Hit {
                reader: q.reader,
                column: c,
                start: 0,
                end: 0,
            });
            query_ids.push(k.payload);
            keys.push(Key {
                key: event_key(c, q.image.r0, 1),
                payload: id,
            });
            keys.push(Key {
                key: event_key(c, q.image.r1, 2),
                payload: id,
            });
            work.keys += 2;
            control.charge(2)?;
            c += 1;
        }
    }
    for _ in 0..key_count {
        work.initialize += 1;
        temp.push(Key::default());
        control.tick()?;
    }
    sort(&mut keys, &mut temp, &mut work, &mut control)?;
    let (mut current, mut started, mut ended) = (usize::MAX, 0, 0);
    for k in keys {
        work.keys += 1;
        control.tick()?;
        let column = (k.key >> 22) as usize;
        if column != current {
            current = column;
            started = cols[column].start;
            ended = started;
        }
        match k.key & 3 {
            0 => started += 1,
            1 => hits[k.payload].start = ended,
            2 => hits[k.payload].end = started,
            _ => ended += 1,
        }
    }
    control.flush()?;
    Ok(Sweep {
        columns: cols,
        hits,
        probes: query_ids,
        work,
        peak_heap_bytes: phase2,
    })
}
