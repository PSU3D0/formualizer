//! Fallible cell-plan to executor Schedule adapter. Numeric theta gaps never
//! allocate empty layers. Runtime IDs are explicitly resolved from cells: the
//! authority's private identity allocator is not the executor's allocator.

use super::plan_control::PlanControl;
use super::planner::OrderedCell;
use super::store::AuthorityError;
use crate::engine::scheduler::{Layer, LayerRun, Schedule, ScheduleUnit};
use crate::engine::vertex::VertexId;
use formualizer_common::ExcelError;

#[derive(Debug)]
pub(crate) enum ScheduleError {
    Authority(AuthorityError),
    Runtime(ExcelError),
}
impl From<AuthorityError> for ScheduleError {
    fn from(value: AuthorityError) -> Self {
        Self::Authority(value)
    }
}
impl From<ExcelError> for ScheduleError {
    fn from(value: ExcelError) -> Self {
        Self::Runtime(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Entry {
    pub cell: OrderedCell,
    pub vertex: VertexId,
}
impl Entry {
    fn group(self) -> (u64, Option<u64>) {
        (self.cell.layer, self.cell.cycle)
    }
    // LSD order: member key, cycle ID, acyclic/cyclic, numeric layer.
    // Acyclic cells at a layer precede all cycle units at that layer. The
    // member key of an acyclic cell is its position (sheet, column, row), so
    // a family's cells at one layer are adjacent and form execution runs;
    // cycle members keep runtime-ID order (the iteration order of a cycle
    // unit is observable).
    fn keys(self) -> [u64; 4] {
        let member = if self.cell.cycle.is_some() {
            u64::from(self.vertex.0)
        } else {
            // Rows below 2^24 and columns below 2^20 order exactly; beyond
            // that only run formation (checked explicitly) is affected.
            (u64::from(self.cell.sheet) << 44)
                | (u64::from(self.cell.col) << 24)
                | u64::from(self.cell.row)
        };
        [
            member,
            self.cell.cycle.unwrap_or(0),
            u64::from(self.cell.cycle.is_some()),
            self.cell.layer,
        ]
    }
    /// Byte `pass` of the LSD key (pass 8k + b is byte b of key k).
    #[inline]
    fn digit(self, pass: usize) -> usize {
        ((self.keys()[pass / 8] >> ((pass % 8) * 8)) & 255) as usize
    }
}

#[derive(Debug)]
pub(crate) struct ExecutablePlan {
    pub schedule: Schedule,
    /// Cell/owner/stable-ID side table in canonical schedule walk order.
    pub entries: Vec<Entry>,
    pub work: u64,
    /// Includes borrowed planner storage supplied as `held_bytes`.
    pub peak_heap_bytes: u64,
}
impl ExecutablePlan {
    /// This output alone; borrowed ordered-plan storage is not retained here.
    pub fn heap_bytes(&self) -> u64 {
        (self.entries.capacity() * size_of::<Entry>()
            + self.schedule.units.capacity() * size_of::<ScheduleUnit>()
            + self.schedule.layers.capacity() * size_of::<Layer>()
            + self.schedule.cycles.capacity() * size_of::<Vec<VertexId>>()
            + self
                .schedule
                .layers
                .iter()
                .map(|l| {
                    l.vertices.capacity() * size_of::<VertexId>()
                        + l.runs.capacity() * size_of::<LayerRun>()
                })
                .sum::<usize>()
            + self
                .schedule
                .cycles
                .iter()
                .map(|c| c.capacity() * size_of::<VertexId>())
                .sum::<usize>()) as u64
    }
}

fn bytes<T>(n: usize) -> Result<u64, AuthorityError> {
    n.checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(AuthorityError::Alloc)
}
fn sum(a: u64, b: u64) -> Result<u64, AuthorityError> {
    a.checked_add(b).ok_or(AuthorityError::Alloc)
}
fn admit(limit: Option<u64>, needed: u64) -> Result<(), AuthorityError> {
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
fn reserve<T>(n: usize) -> Result<Vec<T>, AuthorityError> {
    let mut out = Vec::new();
    out.try_reserve_exact(n)
        .map_err(|_| AuthorityError::Alloc)?;
    Ok(out)
}

/// Maximal runs (length >= 2) of consecutive rows of one column sharing an
/// owner: a family node's cells at one layer (a singleton owns one cell).
/// Visits each run as `(start, len)`.
fn for_each_family_run(entries: &[Entry], mut visit: impl FnMut(usize, usize)) {
    let mut i = 0;
    while i < entries.len() {
        let first = entries[i].cell;
        let mut j = i + 1;
        while j < entries.len() {
            let c = entries[j].cell;
            if c.owner != first.owner
                || c.sheet != first.sheet
                || c.col != first.col
                || c.row != first.row + (j - i) as u32
            {
                break;
            }
            j += 1;
        }
        if j - i >= 2 && first.sheet != crate::engine::authority::geom::SYMBOL_SHEET {
            visit(i, j - i);
        }
        i = j;
    }
}

fn family_runs(entries: &[Entry]) -> Result<Vec<LayerRun>, AuthorityError> {
    let mut n = 0;
    for_each_family_run(entries, |_, _| n += 1);
    let mut runs = reserve(n)?;
    for_each_family_run(entries, |start, len| {
        let c = entries[start].cell;
        runs.push(LayerRun {
            start: start as u32,
            len: len as u32,
            sheet: c.sheet,
            col: c.col,
            row0: c.row,
            owner: c.owner,
        });
    });
    Ok(runs)
}

/// `checkpoint` receives actual work deltas (at most 4096) for resource charging
/// and cancellation, including a zero-work entry checkpoint and a final flush.
/// The borrowed `cells` and other still-live planner capacities must be included
/// in `held_bytes`; `limit` is the simultaneous total budget, not an output cap.
/// Translation must be read-only. No executor-visible output escapes on error.
pub(crate) fn schedule(
    cells: &[OrderedCell],
    held_bytes: u64,
    limit: Option<u64>,
    mut translate: impl FnMut(&OrderedCell) -> Result<VertexId, ExcelError>,
    checkpoint: impl FnMut(u64) -> Result<(), ExcelError>,
) -> Result<ExecutablePlan, ScheduleError> {
    let mut work = PlanControl::new(checkpoint)?;
    let n = cells.len();
    let sorting = sum(held_bytes, sum(bytes::<Entry>(n)?, bytes::<Entry>(n)?)?)?;
    admit(limit, sorting)?;
    let mut entries = reserve(n)?;
    let mut temp = reserve(n)?;
    for cell in cells {
        work.tick()?;
        let entry = Entry {
            cell: *cell,
            vertex: translate(cell)?,
        };
        entries.push(entry);
        temp.push(entry);
    }
    if n > 1 {
        // Passes whose byte is equal in every entry are no-ops of a stable
        // sort: skip them (typically all but a few of the 32).
        let first = entries[0].keys();
        let mut differs = [0u64; 4];
        work.charge(n as u64)?;
        for entry in &entries {
            let keys = entry.keys();
            for k in 0..4 {
                differs[k] |= keys[k] ^ first[k];
            }
        }
        for pass in 0..32 {
            if (differs[pass / 8] >> ((pass % 8) * 8)) & 255 == 0 {
                continue;
            }
            // Two bucket passes and two element passes, charged in batches.
            work.charge(512 + 2 * n as u64)?;
            let mut hist = [0usize; 256];
            for entry in &entries {
                hist[entry.digit(pass)] += 1;
            }
            let mut start = 0;
            for count in &mut hist {
                let n = *count;
                *count = start;
                start += n;
            }
            for entry in &entries {
                let digit = entry.digit(pass);
                temp[hist[digit]] = *entry;
                hist[digit] += 1;
            }
            std::mem::swap(&mut entries, &mut temp);
        }
    }
    drop(temp);
    let mut layers = 0usize;
    let mut cycles = 0usize;
    let mut previous = None;
    for entry in &entries {
        work.tick()?;
        if previous != Some(entry.group()) {
            if entry.cell.cycle.is_some() {
                cycles += 1;
            } else {
                layers += 1;
            }
            previous = Some(entry.group());
        }
    }
    // Family runs of the acyclic groups (bounded by n / 2).
    let mut runs = 0usize;
    let mut start = 0;
    while start < n {
        let group = entries[start].group();
        let mut end = start + 1;
        while end < n && entries[end].group() == group {
            end += 1;
        }
        if group.1.is_none() {
            for_each_family_run(&entries[start..end], |_, _| runs += 1);
        }
        work.tick()?;
        start = end;
    }
    // Public ScheduleUnit indices are u32. Reject before allocation/casts.
    u32::try_from(layers).map_err(|_| AuthorityError::Alloc)?;
    u32::try_from(cycles).map_err(|_| AuthorityError::Alloc)?;
    let groups = layers.checked_add(cycles).ok_or(AuthorityError::Alloc)?;
    let output = sum(
        sum(bytes::<Entry>(n)?, bytes::<VertexId>(n)?)?,
        sum(
            bytes::<ScheduleUnit>(groups)?,
            sum(
                sum(bytes::<Layer>(layers)?, bytes::<Vec<VertexId>>(cycles)?)?,
                bytes::<LayerRun>(runs)?,
            )?,
        )?,
    )?;
    let simultaneous = sum(held_bytes, output)?;
    admit(limit, simultaneous)?;
    let mut schedule = Schedule {
        units: reserve(groups)?,
        layers: reserve(layers)?,
        cycles: reserve(cycles)?,
    };
    let mut start = 0;
    while start < n {
        work.tick()?;
        let group = entries[start].group();
        let mut end = start + 1;
        while end < n {
            work.tick()?;
            if entries[end].group() != group {
                break;
            }
            end += 1;
        }
        let mut vertices = reserve(end - start)?;
        for entry in &entries[start..end] {
            work.tick()?;
            vertices.push(entry.vertex);
        }
        if group.1.is_some() {
            schedule
                .units
                .push(ScheduleUnit::Cycle(schedule.cycles.len() as u32));
            schedule.cycles.push(vertices);
        } else {
            let runs = family_runs(&entries[start..end])?;
            schedule
                .units
                .push(ScheduleUnit::Layer(schedule.layers.len() as u32));
            schedule.layers.push(Layer { vertices, runs });
        }
        start = end;
    }
    work.flush()?;
    Ok(ExecutablePlan {
        schedule,
        entries,
        work: work.total(),
        peak_heap_bytes: sorting.max(simultaneous),
    })
}
