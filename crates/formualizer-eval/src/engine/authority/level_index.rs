//! Per-sheet geometric index: a Bentley–Saxe level structure (design §4.6,
//! promoted from SP-2's `level_index.rs`).
//!
//! - **Buffer:** at most [`BUF`] unsorted entries, scanned per query.
//! - **Levels:** level `j` is empty or holds exactly `BUF · 2^j` entries
//!   (tombstones included: merges carry dead entries so level sizes stay
//!   exact, SP-2 F-6.2), as two static STR-packed R-trees: one of 12-byte
//!   point leaves for 1×1 boxes (most singleton records), one of 20-byte
//!   box leaves for the rest.
//! - **Insert:** append to the buffer; when it fills, merge the buffer and
//!   levels `0..j` into the least empty level `j`. Each entry is rebuilt at
//!   most `log2(n / BUF) + 1` times: amortized O(log n) per insert.
//! - **Delete:** tombstone by `(level, kind, slot)` through a caller-owned
//!   location map (4 B per record id). `settle` rebuilds everything when
//!   dead entries exceed half the live ones (amortized O(1) per delete).
//!   A rebuild orders points before boxes (SP-2 F-6.3), so each level's
//!   point/box split is a function of the live totals.
//! - **Query:** buffer scan + two tree queries per level:
//!   `Q_idx = O(BUF + Σ visited nodes)`, independent of the inserts since
//!   the last rebuild (the spike's delta-list scan was Θ(n) per query).
//!
//! Every allocation has an exact, predictable size, and a counter-only
//! [`IndexShadow`] replays a remove → settle → insert sequence to predict
//! entries, heap bytes and the transient peak exactly (admission, §5.1).

use super::avl::ReserveError;
use super::geom::BoxT;

/// Buffer size and level-0 size.
pub const BUF: usize = 64;
/// R-tree fanout.
const FAN: usize = 16;
/// Location encoding: bits 26.. = level + 1 (0 = buffer); bit 25 = point
/// kind; bits 0..25 = slot.
const SLOT_BITS: u32 = 25;
const SLOT_MASK: u32 = (1 << SLOT_BITS) - 1;
const POINT_BIT: u32 = 1 << SLOT_BITS;
const TAG_SHIFT: u32 = SLOT_BITS + 1;
/// Location of an id that is not in the index.
pub const NONE: u32 = u32::MAX;

fn level_size(j: usize) -> usize {
    BUF << j
}

fn overlaps(a: &BoxT, b: &BoxT) -> bool {
    a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]
}

fn union(a: &BoxT, b: &BoxT) -> BoxT {
    [
        a[0].min(b[0]),
        a[1].min(b[1]),
        a[2].max(b[2]),
        a[3].max(b[3]),
    ]
}

fn is_point(b: &BoxT) -> bool {
    b[0] == b[2] && b[1] == b[3]
}

/// Transient entry used while merging levels.
type Entry = (BoxT, u32, bool);

/// Internal node counts per tree level for `n` leaves (bottom-up).
fn internal_sizes(n: usize) -> impl Iterator<Item = usize> {
    let step = |k: usize| (k > 1).then(|| k.div_ceil(FAN));
    std::iter::successors(step(n), move |&k| step(k))
}

/// Most levels an index can have (a level's slot index has `SLOT_BITS`
/// bits, so level `j` needs `BUF · 2^j ≤ 2^SLOT_BITS`).
const MAX_LEVELS: usize = 24;

/// A leaf layout of a static tree.
trait Leaf: Copy {
    const BYTES: usize;
    fn make(b: &BoxT, id: u32) -> Self;
    fn bbox(&self) -> BoxT;
    fn id(&self) -> u32;
}

/// A 1×1 box: `(row, col, id)`, 12 bytes.
#[derive(Clone, Copy, Debug)]
struct PointLeaf(u32, u32, u32);

impl Leaf for PointLeaf {
    const BYTES: usize = 12;
    fn make(b: &BoxT, id: u32) -> Self {
        PointLeaf(b[0], b[1], id)
    }
    fn bbox(&self) -> BoxT {
        [self.0, self.1, self.0, self.1]
    }
    fn id(&self) -> u32 {
        self.2
    }
}

#[derive(Clone, Copy, Debug)]
struct BoxLeaf(BoxT, u32);

impl Leaf for BoxLeaf {
    const BYTES: usize = 20;
    fn make(b: &BoxT, id: u32) -> Self {
        BoxLeaf(*b, id)
    }
    fn bbox(&self) -> BoxT {
        self.0
    }
    fn id(&self) -> u32 {
        self.1
    }
}

/// A static STR-packed R-tree with tombstones.
#[derive(Clone, Debug, Default)]
struct Tree<L> {
    leaves: Vec<L>,
    /// Internal levels bottom-up; node `i` of level `k` covers children
    /// `i*FAN .. (i+1)*FAN` of level `k-1` (or of the leaves for `k = 0`).
    nodes: Vec<Vec<BoxT>>,
    dead: Vec<u64>,
    ndead: usize,
}

impl<L: Leaf> Tree<L> {
    fn predicted_bytes(n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        n * L::BYTES
            + internal_sizes(n).sum::<usize>() * size_of::<BoxT>()
            + internal_sizes(n).count() * size_of::<Vec<BoxT>>()
            + n.div_ceil(64) * size_of::<u64>()
    }

    fn heap_bytes(&self) -> usize {
        self.leaves.capacity() * L::BYTES
            + self.nodes.capacity() * size_of::<Vec<BoxT>>()
            + self
                .nodes
                .iter()
                .map(|v| v.capacity() * size_of::<BoxT>())
                .sum::<usize>()
            + self.dead.capacity() * size_of::<u64>()
    }

    /// STR packing into pre-allocated buffers (no allocation): sort by
    /// column centre, cut into vertical slabs, sort each slab by row
    /// centre, then pack leaves of `FAN`. Deterministic.
    fn build_with(
        entries: &mut [Entry],
        tag: u32,
        kind: u32,
        loc: &mut [u32],
        bufs: TreeBufs<L>,
    ) -> Self {
        let n = entries.len();
        let TreeBufs {
            mut leaves,
            mut nodes,
            mut dead,
        } = bufs;
        if n == 0 {
            return Self {
                leaves,
                nodes,
                dead,
                ndead: 0,
            };
        }
        debug_assert!(leaves.capacity() >= n && nodes.len() == internal_sizes(n).count());
        entries.sort_unstable_by_key(|e| (u64::from(e.0[1]) + u64::from(e.0[3]), e.1));
        let leaves_n = n.div_ceil(FAN);
        let slabs = (leaves_n as f64).sqrt().ceil().max(1.0) as usize;
        let slab = leaves_n.div_ceil(slabs).max(1) * FAN;
        for chunk in entries.chunks_mut(slab) {
            chunk.sort_unstable_by_key(|e| (u64::from(e.0[0]) + u64::from(e.0[2]), e.1));
        }
        dead.resize(n.div_ceil(64), 0);
        let mut ndead = 0;
        for (slot, &(b, id, is_dead)) in entries.iter().enumerate() {
            leaves.push(L::make(&b, id));
            if is_dead {
                dead[slot / 64] |= 1 << (slot % 64);
                ndead += 1;
            } else {
                loc[id as usize] = (tag << TAG_SHIFT) | kind | slot as u32;
            }
        }
        for k in 0..nodes.len() {
            let (lower, upper) = nodes.split_at_mut(k);
            let lvl = &mut upper[0];
            if k == 0 {
                for chunk in leaves.chunks(FAN) {
                    lvl.push(
                        chunk
                            .iter()
                            .skip(1)
                            .fold(chunk[0].bbox(), |a, l: &L| union(&a, &l.bbox())),
                    );
                }
            } else {
                for chunk in lower[k - 1].chunks(FAN) {
                    lvl.push(chunk.iter().skip(1).fold(chunk[0], |a, b| union(&a, b)));
                }
            }
        }
        Self {
            leaves,
            nodes,
            dead,
            ndead,
        }
    }

    fn is_dead(&self, slot: usize) -> bool {
        self.dead[slot / 64] >> (slot % 64) & 1 == 1
    }

    fn kill(&mut self, slot: usize) {
        debug_assert!(!self.is_dead(slot));
        self.dead[slot / 64] |= 1 << (slot % 64);
        self.ndead += 1;
    }

    fn take(&self, point: bool, out: &mut Vec<Entry>) {
        for (slot, l) in self.leaves.iter().enumerate() {
            let _ = point;
            out.push((l.bbox(), l.id(), self.is_dead(slot)));
        }
    }

    /// Visit the live leaves meeting `q` until `visit` returns false;
    /// returns false when it stopped early.
    fn query(&self, q: &BoxT, work: &mut u64, visit: &mut dyn FnMut(u32) -> bool) -> bool {
        let n = self.leaves.len();
        if n == 0 {
            return true;
        }
        let levels = self.nodes.len();
        // Depth ≤ 8 for 2^25 leaves of fanout 16: at most 15 pending
        // siblings per level plus one node's children, on a fixed stack
        // (no heap).
        let mut stack = [(0usize, 0usize); 192];
        let mut top = 1;
        stack[0] = if levels == 0 {
            (usize::MAX, 0)
        } else {
            (levels - 1, 0)
        };
        while top > 0 {
            top -= 1;
            let (lvl, i) = stack[top];
            *work += 1;
            if lvl == usize::MAX {
                if overlaps(&self.leaves[i].bbox(), q)
                    && !self.is_dead(i)
                    && !visit(self.leaves[i].id())
                {
                    return false;
                }
                continue;
            }
            if !overlaps(&self.nodes[lvl][i], q) {
                continue;
            }
            let child_count = if lvl == 0 {
                n
            } else {
                self.nodes[lvl - 1].len()
            };
            let start = i * FAN;
            for c in start..(start + FAN).min(child_count) {
                stack[top] = if lvl == 0 {
                    (usize::MAX, c)
                } else {
                    (lvl - 1, c)
                };
                top += 1;
            }
        }
        true
    }
}

/// Buffers of one static tree, allocated at their exact sizes: leaves,
/// every internal level, the tombstone bitmap.
#[derive(Clone, Debug)]
struct TreeBufs<L> {
    leaves: Vec<L>,
    nodes: Vec<Vec<BoxT>>,
    dead: Vec<u64>,
}

impl<L: Leaf> TreeBufs<L> {
    fn try_new(n: usize) -> Result<Self, ReserveError> {
        let mut b = Self {
            leaves: Vec::new(),
            nodes: Vec::new(),
            dead: Vec::new(),
        };
        if n == 0 {
            return Ok(b);
        }
        b.leaves.try_reserve_exact(n).map_err(|_| ReserveError)?;
        b.dead
            .try_reserve_exact(n.div_ceil(64))
            .map_err(|_| ReserveError)?;
        b.nodes
            .try_reserve_exact(internal_sizes(n).count())
            .map_err(|_| ReserveError)?;
        for size in internal_sizes(n) {
            let mut v = Vec::new();
            v.try_reserve_exact(size).map_err(|_| ReserveError)?;
            b.nodes.push(v);
        }
        Ok(b)
    }

    fn alloc(n: usize) -> Self {
        Self::try_new(n).expect("level allocation")
    }

    fn heap_bytes(&self) -> usize {
        self.leaves.capacity() * L::BYTES
            + self.nodes.capacity() * size_of::<Vec<BoxT>>()
            + self
                .nodes
                .iter()
                .map(|v| v.capacity() * size_of::<BoxT>())
                .sum::<usize>()
            + self.dead.capacity() * size_of::<u64>()
    }
}

/// Buffers of one level build: `(points, boxes)` leaves.
#[derive(Clone, Debug)]
struct LevelBufs {
    points: TreeBufs<PointLeaf>,
    boxes: TreeBufs<BoxLeaf>,
}

impl LevelBufs {
    fn try_new(p: usize, b: usize) -> Result<Self, ReserveError> {
        Ok(Self {
            points: TreeBufs::try_new(p)?,
            boxes: TreeBufs::try_new(b)?,
        })
    }
}

/// Every allocation a planned operation sequence on one index needs, made
/// before the sequence runs (M1a correction B5: the apply allocates
/// nothing). Produced by [`LevelIndex::try_stage`] from the dry run's
/// [`IndexShadow`]; the level builds are consumed in the shadow's order.
#[derive(Clone, Debug, Default)]
pub struct IndexStage {
    /// Level buffers, in reverse order of use.
    levels: Vec<LevelBufs>,
    /// Merge/rebuild buffer, reused by every build of the sequence.
    temp: Vec<Entry>,
}

impl IndexStage {
    /// Heap bytes held by the stage.
    pub fn heap_bytes(&self) -> usize {
        self.levels.capacity() * size_of::<LevelBufs>()
            + self
                .levels
                .iter()
                .map(|l| l.points.heap_bytes() + l.boxes.heap_bytes())
                .sum::<usize>()
            + self.temp.capacity() * size_of::<Entry>()
    }
}

fn take_level_bufs(stage: Option<&mut IndexStage>, p: usize, b: usize) -> LevelBufs {
    match stage.and_then(|s| s.levels.pop()) {
        Some(l) => {
            debug_assert_eq!(
                (l.points.leaves.capacity(), l.boxes.leaves.capacity()),
                (p, b),
                "staged level out of order"
            );
            l
        }
        None => LevelBufs {
            points: TreeBufs::alloc(p),
            boxes: TreeBufs::alloc(b),
        },
    }
}

fn take_temp(stage: Option<&mut IndexStage>, n: usize) -> Vec<Entry> {
    match stage {
        Some(s) => {
            let mut t = std::mem::take(&mut s.temp);
            debug_assert!(t.capacity() >= n, "unstaged merge buffer");
            t.clear();
            t
        }
        None => Vec::with_capacity(n),
    }
}

fn put_temp(stage: Option<&mut IndexStage>, t: Vec<Entry>) {
    if let Some(s) = stage {
        s.temp = t;
    }
}

/// One static level: a points tree and a boxes tree.
#[derive(Clone, Debug)]
struct Level {
    points: Tree<PointLeaf>,
    boxes: Tree<BoxLeaf>,
}

impl Level {
    fn predicted_bytes(points: usize, boxes: usize) -> usize {
        Tree::<PointLeaf>::predicted_bytes(points) + Tree::<BoxLeaf>::predicted_bytes(boxes)
    }

    fn heap_bytes(&self) -> usize {
        self.points.heap_bytes() + self.boxes.heap_bytes()
    }

    fn ndead(&self) -> usize {
        self.points.ndead + self.boxes.ndead
    }

    /// Build level `j` from entries (points and boxes mixed) into `bufs`.
    fn build_with(entries: &mut [Entry], j: usize, loc: &mut [u32], bufs: LevelBufs) -> Level {
        // Partition points first (stable w.r.t. ids is not needed: the tree
        // build sorts).
        let split = partition_points(entries);
        let (pts, bxs) = entries.split_at_mut(split);
        let tag = j as u32 + 1;
        Level {
            points: Tree::build_with(pts, tag, POINT_BIT, loc, bufs.points),
            boxes: Tree::build_with(bxs, tag, 0, loc, bufs.boxes),
        }
    }

    #[cfg(test)]
    fn build(entries: &mut [Entry], j: usize, loc: &mut [u32]) -> Level {
        let p = entries.iter().filter(|e| is_point(&e.0)).count();
        let bufs = LevelBufs {
            points: TreeBufs::alloc(p),
            boxes: TreeBufs::alloc(entries.len() - p),
        };
        Self::build_with(entries, j, loc, bufs)
    }

    fn counts(&self) -> (usize, usize) {
        (self.points.leaves.len(), self.boxes.leaves.len())
    }
}

/// Move point entries to the front; returns their count.
fn partition_points(entries: &mut [Entry]) -> usize {
    let mut k = 0;
    for i in 0..entries.len() {
        if is_point(&entries[i].0) {
            entries.swap(i, k);
            k += 1;
        }
    }
    k
}

#[derive(Clone, Debug, Default)]
pub struct LevelIndex {
    buf: Vec<(BoxT, u32)>,
    levels: Vec<Option<Level>>,
    /// Peak heap bytes since `reset_peak` (retained + transient).
    peak: usize,
    live: u32,
    live_points: u32,
    dead: u32,
}

impl LevelIndex {
    fn enc_buf(slot: usize, point: bool) -> u32 {
        (if point { POINT_BIT } else { 0 }) | slot as u32
    }

    /// Where an id lives: `None` = buffer, `Some(j)` = level `j`.
    pub fn location(loc: u32) -> Option<usize> {
        match loc >> TAG_SHIFT {
            0 => None,
            t => Some(t as usize - 1),
        }
    }

    /// Whether a located entry is a 1×1 point.
    pub fn located_point(loc: u32) -> bool {
        loc & POINT_BIT != 0
    }

    pub fn live(&self) -> usize {
        self.live as usize
    }

    /// Stored entries, tombstones included.
    pub fn entries(&self) -> usize {
        (self.live + self.dead) as usize
    }

    pub fn heap_bytes(&self) -> usize {
        self.buf.capacity() * size_of::<(BoxT, u32)>()
            + self.levels.capacity() * size_of::<Option<Level>>()
            + self
                .levels
                .iter()
                .flatten()
                .map(Level::heap_bytes)
                .sum::<usize>()
    }

    pub fn reset_peak(&mut self) {
        self.peak = self.heap_bytes();
    }

    /// Highest heap bytes (retained + transient merge buffers) since the
    /// last `reset_peak`.
    pub fn peak(&self) -> usize {
        self.peak.max(self.heap_bytes())
    }

    fn ensure_levels(&mut self, j: usize) {
        if self.levels.len() <= j {
            let extra = j + 1 - self.levels.len();
            self.levels.reserve_exact(extra);
            self.levels.resize_with(j + 1, || None);
        }
    }

    pub fn insert(&mut self, b: BoxT, id: u32, loc: &mut [u32]) {
        self.insert_in(b, id, loc, None);
    }

    /// Insert with every allocation taken from `stage` when given (the
    /// store's apply: see [`Self::try_stage`]).
    pub fn insert_in(&mut self, b: BoxT, id: u32, loc: &mut [u32], stage: Option<&mut IndexStage>) {
        // The buffer grows exactly (one entry at a time up to `BUF`), so an
        // index's bytes are proportional to its entries from the first one.
        if self.buf.len() == self.buf.capacity() {
            debug_assert!(stage.is_none(), "unstaged buffer growth");
            self.buf.reserve_exact(1);
        }
        let point = is_point(&b);
        loc[id as usize] = Self::enc_buf(self.buf.len(), point);
        self.buf.push((b, id));
        self.live += 1;
        self.live_points += u32::from(point);
        if self.buf.len() == BUF {
            self.flush(loc, stage);
        }
    }

    /// Remove `id` (tombstone in a level; swap-remove in the buffer). Call
    /// `settle` after a batch of removals.
    pub fn remove(&mut self, id: u32, loc: &mut [u32]) {
        let l = loc[id as usize];
        debug_assert_ne!(l, NONE, "removing an id that is not indexed");
        let slot = (l & SLOT_MASK) as usize;
        let point = Self::located_point(l);
        match Self::location(l) {
            None => {
                self.buf.swap_remove(slot);
                if let Some(&(b, moved)) = self.buf.get(slot) {
                    loc[moved as usize] = Self::enc_buf(slot, is_point(&b));
                }
            }
            Some(j) => {
                let lv = self.levels[j].as_mut().expect("level of a live entry");
                if point {
                    lv.points.kill(slot);
                } else {
                    lv.boxes.kill(slot);
                }
                self.dead += 1;
            }
        }
        loc[id as usize] = NONE;
        self.live -= 1;
        self.live_points -= u32::from(point);
    }

    /// Rebuild when tombstones exceed half the live entries.
    pub fn settle(&mut self, loc: &mut [u32]) {
        self.settle_in(loc, None);
    }

    /// [`Self::settle`] with allocations from `stage`.
    pub fn settle_in(&mut self, loc: &mut [u32], stage: Option<&mut IndexStage>) {
        if self.dead > 0 && self.dead > self.live / 2 {
            self.rebuild_all(loc, stage);
        }
    }

    /// Reserve and pre-allocate everything the operation sequence the
    /// shadow `sh` replayed will allocate (buffer and level-vector growth,
    /// each level build, the merge buffer). Fallible; on error only spare
    /// capacity may have grown.
    pub fn try_stage(&mut self, sh: &IndexShadow) -> Result<IndexStage, ReserveError> {
        if sh.buf_cap > self.buf.capacity() {
            self.buf
                .try_reserve_exact(sh.buf_cap - self.buf.len())
                .map_err(|_| ReserveError)?;
        }
        if sh.levels_cap > self.levels.capacity() {
            self.levels
                .try_reserve_exact(sh.levels_cap - self.levels.len())
                .map_err(|_| ReserveError)?;
        }
        let mut st = IndexStage::default();
        st.levels
            .try_reserve_exact(sh.events.len())
            .map_err(|_| ReserveError)?;
        for &(p, b) in sh.events.iter().rev() {
            st.levels.push(LevelBufs::try_new(p as usize, b as usize)?);
        }
        st.temp
            .try_reserve_exact(sh.temp_max)
            .map_err(|_| ReserveError)?;
        Ok(st)
    }

    fn take_level(lv: &Level, out: &mut Vec<Entry>) {
        lv.points.take(true, out);
        lv.boxes.take(false, out);
    }

    fn flush(&mut self, loc: &mut [u32], mut stage: Option<&mut IndexStage>) {
        let j = self
            .levels
            .iter()
            .position(Option::is_none)
            .unwrap_or(self.levels.len());
        self.ensure_levels(j);
        let n = level_size(j);
        let temp = n * size_of::<Entry>();
        let mut entries = take_temp(stage.as_deref_mut(), n);
        entries.extend(self.buf.iter().map(|&(b, id)| (b, id, false)));
        self.buf.clear();
        // Old levels are merged one by one and freed; the peak model below
        // still counts them as coexisting with the new level (conservative).
        let mut old_bytes = 0;
        for i in 0..j {
            let lv = self.levels[i].take().expect("levels below j are full");
            Self::take_level(&lv, &mut entries);
            self.dead -= lv.ndead() as u32;
            old_bytes += lv.heap_bytes();
        }
        debug_assert_eq!(entries.len(), n);
        let p = entries.iter().filter(|e| is_point(&e.0)).count();
        let bufs = take_level_bufs(stage.as_deref_mut(), p, n - p);
        let new = Level::build_with(&mut entries, j, loc, bufs);
        self.dead += new.ndead() as u32;
        // Peak: old levels, the merge buffer and the new level coexist.
        let now = self.heap_bytes() + old_bytes + temp + new.heap_bytes();
        self.peak = self.peak.max(now);
        self.levels[j] = Some(new);
        put_temp(stage, entries);
    }

    /// Lay out live entries: points first, the buffer takes `n mod BUF`,
    /// the levels the binary digits of `n / BUF` in ascending order. The
    /// new levels are installed (the index holds no level on entry).
    fn lay_out(
        &mut self,
        entries: &mut [Entry],
        loc: &mut [u32],
        mut stage: Option<&mut IndexStage>,
    ) {
        let n = entries.len();
        let split = partition_points(entries);
        // Stable order inside each kind is irrelevant (levels sort), but
        // the kind order must be points first for the shadow's arithmetic.
        debug_assert!(entries[..split].iter().all(|e| is_point(&e.0)));
        let r = n % BUF;
        if r > self.buf.capacity() {
            debug_assert!(stage.is_none(), "unstaged buffer growth");
            self.buf.reserve_exact(r);
        }
        for &(b, id, _) in &entries[..r] {
            loc[id as usize] = Self::enc_buf(self.buf.len(), is_point(&b));
            self.buf.push((b, id));
        }
        let full = n / BUF;
        let mut at = r;
        for j in 0..usize::BITS as usize {
            if full >> j & 1 == 1 {
                let end = at + level_size(j);
                let seg = &mut entries[at..end];
                let p = seg.iter().filter(|e| is_point(&e.0)).count();
                let bufs = take_level_bufs(stage.as_deref_mut(), p, seg.len() - p);
                let lv = Level::build_with(seg, j, loc, bufs);
                self.ensure_levels(j);
                self.levels[j] = Some(lv);
                at = end;
            }
        }
    }

    fn rebuild_all(&mut self, loc: &mut [u32], mut stage: Option<&mut IndexStage>) {
        let total = self.entries();
        let temp = total * size_of::<Entry>();
        let mut entries = take_temp(stage.as_deref_mut(), total);
        entries.extend(self.buf.iter().map(|&(b, id)| (b, id, false)));
        self.buf.clear();
        let mut old_bytes = 0;
        for slot in self.levels.iter_mut() {
            if let Some(lv) = slot.take() {
                Self::take_level(&lv, &mut entries);
                old_bytes += lv.heap_bytes();
            }
        }
        entries.retain(|e| !e.2);
        debug_assert_eq!(entries.len(), self.live as usize);
        self.lay_out(&mut entries, loc, stage.as_deref_mut());
        // Peak: old levels, the merge buffer and the new levels coexist.
        let now = self.heap_bytes() + old_bytes + temp;
        self.peak = self.peak.max(now);
        self.dead = 0;
        put_temp(stage, entries);
    }

    /// Bulk load into an empty index with the rebuild layout.
    /// Bytes per item of the entry list a bulk load lays out (build
    /// scratch accounting).
    pub fn bulk_entry_bytes() -> usize {
        size_of::<Entry>()
    }

    pub fn bulk_load(&mut self, items: &[(BoxT, u32)], loc: &mut [u32]) {
        debug_assert_eq!(self.entries(), 0, "bulk_load into a non-empty index");
        let mut entries: Vec<Entry> = items.iter().map(|&(b, id)| (b, id, false)).collect();
        self.live = entries.len() as u32;
        self.live_points = entries.iter().filter(|e| is_point(&e.0)).count() as u32;
        self.lay_out(&mut entries, loc, None);
    }

    /// Visit every live id whose box overlaps `q`. Returns the work done
    /// (buffer entries tested + tree nodes visited).
    pub fn query(&self, q: &BoxT, visit: &mut dyn FnMut(u32)) -> u64 {
        self.query_until(q, &mut |id| {
            visit(id);
            true
        })
        .0
    }

    /// [`Self::query`] that stops as soon as `visit` returns false (bounded
    /// inspection). Returns the work and whether the query ran to the end.
    pub fn query_until(&self, q: &BoxT, visit: &mut dyn FnMut(u32) -> bool) -> (u64, bool) {
        let mut work = self.buf.len() as u64;
        for (b, id) in &self.buf {
            if overlaps(b, q) && !visit(*id) {
                return (work, false);
            }
        }
        for lv in self.levels.iter().flatten() {
            if !lv.points.query(q, &mut work, visit) || !lv.boxes.query(q, &mut work, visit) {
                return (work, false);
            }
        }
        (work, true)
    }

    /// Number of non-empty levels.
    pub fn level_count(&self) -> usize {
        self.levels.iter().flatten().count()
    }

    /// Counter-only replica (no heap allocation).
    pub fn shadow(&self) -> IndexShadow {
        debug_assert!(self.levels.len() <= MAX_LEVELS);
        let buf_points = self.buf.iter().filter(|(b, _)| is_point(b)).count();
        let mut levels = [None; MAX_LEVELS];
        for (j, l) in self.levels.iter().enumerate() {
            levels[j] = l.as_ref().map(|l| {
                let (p, b) = l.counts();
                (p, b, l.ndead())
            });
        }
        IndexShadow {
            buf_points,
            buf_boxes: self.buf.len() - buf_points,
            buf_cap: self.buf.capacity(),
            levels,
            nlevels: self.levels.len(),
            levels_cap: self.levels.capacity(),
            live_points: self.live_points as usize,
            live_boxes: (self.live - self.live_points) as usize,
            dead: self.dead as usize,
            peak: self.heap_bytes(),
            events: Vec::new(),
            temp_max: 0,
        }
    }
}

/// Counter-only replica of a [`LevelIndex`] for dry runs. Besides entries,
/// bytes and the peak it records every level build the replayed sequence
/// performs, so the real index can pre-allocate them ([`LevelIndex::try_stage`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexShadow {
    buf_points: usize,
    buf_boxes: usize,
    buf_cap: usize,
    /// Per level: `(points, boxes, dead)`; `nlevels` is the level vector's
    /// length.
    levels: [Option<(usize, usize, usize)>; MAX_LEVELS],
    nlevels: usize,
    levels_cap: usize,
    live_points: usize,
    live_boxes: usize,
    dead: usize,
    peak: usize,
    /// Level builds in order: `(points, boxes)` leaves.
    events: Vec<(u32, u32)>,
    /// Largest merge/rebuild buffer (entries).
    temp_max: usize,
}

impl IndexShadow {
    pub fn entries(&self) -> usize {
        self.live_points + self.live_boxes + self.dead
    }

    pub fn live(&self) -> usize {
        self.live_points + self.live_boxes
    }

    pub fn heap_bytes(&self) -> usize {
        self.buf_cap * size_of::<(BoxT, u32)>()
            + self.levels_cap * size_of::<Option<Level>>()
            + self.levels[..self.nlevels]
                .iter()
                .flatten()
                .map(|&(p, b, _)| Level::predicted_bytes(p, b))
                .sum::<usize>()
    }

    pub fn peak(&self) -> usize {
        self.peak.max(self.heap_bytes())
    }

    /// Bytes [`LevelIndex::try_stage`] pre-allocates as separate buffers:
    /// every level build and the merge buffer.
    pub fn stage_bytes(&self) -> usize {
        self.events
            .iter()
            .map(|&(p, b)| Level::predicted_bytes(p as usize, b as usize))
            .sum::<usize>()
            + self.events.len() * size_of::<LevelBufs>()
            + self.temp_max * size_of::<Entry>()
    }

    /// Heap of the shadow itself (planning scratch).
    pub fn scratch_bytes(&self) -> usize {
        self.events.capacity() * size_of::<(u32, u32)>()
    }

    /// Buffer and level-vector capacity growth `(bytes before, bytes
    /// after)` relative to `idx` (grown in place by `try_stage`).
    pub fn vec_growth(&self, idx: &LevelIndex) -> [(usize, usize); 2] {
        [
            (
                idx.buf.capacity() * size_of::<(BoxT, u32)>(),
                self.buf_cap.max(idx.buf.capacity()) * size_of::<(BoxT, u32)>(),
            ),
            (
                idx.levels.capacity() * size_of::<Option<Level>>(),
                self.levels_cap.max(idx.levels.capacity()) * size_of::<Option<Level>>(),
            ),
        ]
    }

    fn ensure_levels(&mut self, j: usize) {
        debug_assert!(j < MAX_LEVELS, "index past {MAX_LEVELS} levels");
        if self.nlevels <= j {
            self.levels_cap = self.levels_cap.max(j + 1);
            self.nlevels = j + 1;
        }
    }

    fn event(&mut self, p: usize, b: usize) -> Result<(), ReserveError> {
        if self.events.len() == self.events.capacity() {
            self.events.try_reserve(1).map_err(|_| ReserveError)?;
        }
        self.events.push((p as u32, b as u32));
        Ok(())
    }

    /// Insert one entry (`point` = 1×1 box).
    pub fn insert(&mut self, point: bool) {
        self.try_insert(point).expect("shadow allocation");
    }

    /// [`Self::insert`], fallible (the event list may grow).
    pub fn try_insert(&mut self, point: bool) -> Result<(), ReserveError> {
        if point {
            self.buf_points += 1;
            self.live_points += 1;
        } else {
            self.buf_boxes += 1;
            self.live_boxes += 1;
        }
        let buf = self.buf_points + self.buf_boxes;
        self.buf_cap = self.buf_cap.max(buf);
        if buf == BUF {
            let j = self.levels[..self.nlevels]
                .iter()
                .position(Option::is_none)
                .unwrap_or(self.nlevels);
            self.ensure_levels(j);
            let n = level_size(j);
            let (mut p, mut b, mut dead) = (self.buf_points, self.buf_boxes, 0);
            let mut old_bytes = 0;
            for i in 0..j {
                let (lp, lb, ld) = self.levels[i].take().expect("full");
                old_bytes += Level::predicted_bytes(lp, lb);
                p += lp;
                b += lb;
                dead += ld;
            }
            self.buf_points = 0;
            self.buf_boxes = 0;
            let now = self.heap_bytes()
                + old_bytes
                + n * size_of::<Entry>()
                + Level::predicted_bytes(p, b);
            self.peak = self.peak.max(now);
            self.levels[j] = Some((p, b, dead));
            self.temp_max = self.temp_max.max(n);
            self.event(p, b)?;
        }
        Ok(())
    }

    /// Remove the entry at location code `loc`.
    pub fn remove(&mut self, loc: u32) {
        let point = LevelIndex::located_point(loc);
        if point {
            self.live_points -= 1;
        } else {
            self.live_boxes -= 1;
        }
        match LevelIndex::location(loc) {
            None => {
                if point {
                    self.buf_points -= 1;
                } else {
                    self.buf_boxes -= 1;
                }
            }
            Some(j) => {
                self.dead += 1;
                if let Some((_, _, d)) = self.levels[j].as_mut() {
                    *d += 1;
                }
            }
        }
    }

    pub fn settle(&mut self) {
        self.try_settle().expect("shadow allocation");
    }

    /// [`Self::settle`], fallible (the event list may grow).
    pub fn try_settle(&mut self) -> Result<(), ReserveError> {
        if self.dead > 0 && self.dead > self.live() / 2 {
            self.rebuild_all()?;
        }
        Ok(())
    }

    fn rebuild_all(&mut self) -> Result<(), ReserveError> {
        let total = self.entries();
        let n = self.live();
        let old_bytes: usize = self.levels[..self.nlevels]
            .iter()
            .flatten()
            .map(|&(p, b, _)| Level::predicted_bytes(p, b))
            .sum();
        for l in self.levels.iter_mut() {
            *l = None;
        }
        // Points first: take the first `k` entries of the laid-out order.
        let mut pts = self.live_points;
        let mut take = |k: usize| {
            let p = pts.min(k);
            pts -= p;
            (p, k - p)
        };
        let r = n % BUF;
        let (bp, bb) = take(r);
        self.buf_cap = self.buf_cap.max(r);
        self.buf_points = bp;
        self.buf_boxes = bb;
        let full = n / BUF;
        self.temp_max = self.temp_max.max(total);
        for j in 0..usize::BITS as usize {
            if full >> j & 1 == 1 {
                self.ensure_levels(j);
                let (p, b) = take(level_size(j));
                self.levels[j] = Some((p, b, 0));
                self.event(p, b)?;
            }
        }
        let now = self.heap_bytes() + old_bytes + total * size_of::<Entry>();
        self.peak = self.peak.max(now);
        self.dead = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u32) -> u32 {
            (self.next() % u64::from(n)) as u32
        }
    }

    fn rand_box(g: &mut Rng) -> BoxT {
        let r = g.below(500);
        let c = g.below(40);
        if g.below(100) < 40 {
            [r, c, r, c]
        } else {
            [r, c, r + g.below(20), c + g.below(3)]
        }
    }

    #[test]
    fn predicted_level_bytes_match_build() {
        let mut g = Rng(3);
        for n in [1usize, 16, 17, 64, 65, 256, 300, 1024, 5000] {
            let mut entries: Vec<Entry> = (0..n as u32)
                .map(|i| (rand_box(&mut g), i, false))
                .collect();
            let p = entries.iter().filter(|e| is_point(&e.0)).count();
            let mut loc = vec![NONE; n];
            let lv = Level::build(&mut entries, 0, &mut loc);
            assert_eq!(lv.heap_bytes(), Level::predicted_bytes(p, n - p), "n={n}");
        }
    }

    /// Random insert/remove against brute force and the counter shadow:
    /// query results, entries, capacity and peak agree at every step.
    #[test]
    fn level_index_matches_brute_and_shadow() {
        let mut g = Rng(17);
        let mut idx = LevelIndex::default();
        let mut loc = vec![NONE; 20_000];
        let mut live: Vec<(BoxT, u32)> = Vec::new();
        let mut next = 0u32;
        let mut free: Vec<u32> = Vec::new();
        let (mut flushes, mut rebuilds) = (0, 0);
        for step in 0..12_000 {
            let mut sh = idx.shadow();
            idx.reset_peak();
            let levels_before = idx.level_count();
            if !live.is_empty() && g.below(100) < 45 {
                let k = g.below(live.len() as u32) as usize;
                let (_, id) = live.swap_remove(k);
                sh.remove(loc[id as usize]);
                sh.settle();
                let dead_before = idx.dead;
                idx.remove(id, &mut loc);
                idx.settle(&mut loc);
                if idx.dead == 0 && dead_before + 1 > 1 {
                    rebuilds += 1;
                }
                free.push(id);
            } else {
                let id = free.pop().unwrap_or_else(|| {
                    next += 1;
                    next - 1
                });
                let b = rand_box(&mut g);
                sh.insert(is_point(&b));
                idx.insert(b, id, &mut loc);
                live.push((b, id));
                if idx.buf.is_empty() || idx.level_count() != levels_before {
                    flushes += 1;
                }
            }
            assert_eq!(idx.entries(), sh.entries(), "step {step}");
            assert_eq!(idx.heap_bytes(), sh.heap_bytes(), "step {step}");
            assert_eq!(idx.peak(), sh.peak(), "step {step}");
            if step % 97 == 0 {
                let q = [g.below(500), g.below(40), 0, 0];
                let q = [q[0], q[1], q[0] + g.below(60), q[1] + g.below(6)];
                let mut got = Vec::new();
                idx.query(&q, &mut |id| got.push(id));
                got.sort_unstable();
                let mut want: Vec<u32> = live
                    .iter()
                    .filter(|(b, _)| overlaps(b, &q))
                    .map(|x| x.1)
                    .collect();
                want.sort_unstable();
                assert_eq!(got, want, "step {step}");
            }
        }
        assert!(
            flushes > 0 && rebuilds > 0,
            "flushes {flushes} rebuilds {rebuilds}"
        );
    }

    /// SP-2 property 8: query work is O(BUF + Σ levels) independent of the
    /// number of inserts since the last rebuild.
    #[test]
    fn query_work_bounded_under_inserts() {
        let mut idx = LevelIndex::default();
        let mut loc = vec![NONE; 150_000];
        for i in 0..150_000u32 {
            let r = i;
            idx.insert([r, 0, r, 0], i, &mut loc);
            if i % 997 == 0 {
                let q = [r / 2, 0, r / 2, 0];
                let mut hits = 0;
                let work = idx.query(&q, &mut |_| hits += 1);
                let levels = idx.level_count() as u64;
                assert!(levels <= 64 - (u64::from(i) / 64 + 1).leading_zeros() as u64 + 1);
                // Buffer + per level and tree one root-to-leaf path (≤ 5
                // nodes of fanout 16 for 2^20 entries) with its siblings.
                assert!(
                    work <= BUF as u64 + levels * 2 * 6 * FAN as u64,
                    "work {work} at {i}"
                );
            }
        }
    }
}
