//! ID1–ID6 property tests for the identity table (design §4.1, SP-3
//! property 1 and 7), against an independent map model.

use super::super::geom::Cell;
use super::super::identity::{FAMILY, IdError, IdentityTable, Vid};
use proptest::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

#[derive(Clone, Debug)]
enum Op {
    /// value/empty → formula at one cell (singleton, new id).
    Set(u16, u32, u32),
    /// A filled-down column family of `len` new ids.
    Family(u16, u32, u32, u32),
    /// formula → formula with an owner change (keeps the id, ID6).
    Reown(usize),
    /// formula → value (retires the id).
    Clear(usize),
    /// History replay of a retired id at its old cell (ID4).
    Resurrect(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u16..2, 0u32..40, 0u32..4).prop_map(|(s, r, c)| Op::Set(s, r, c)),
        2 => (0u16..2, 0u32..40, 0u32..4, 1u32..12).prop_map(|(s, r, c, n)| Op::Family(s, r, c, n)),
        3 => any::<usize>().prop_map(Op::Reown),
        4 => any::<usize>().prop_map(Op::Clear),
        1 => any::<usize>().prop_map(Op::Resurrect),
    ]
}

struct Model {
    live: FxHashMap<Cell, Vid>,
    /// Retired ids with the cell they last had.
    retired: Vec<(Vid, Cell)>,
    ever: FxHashSet<Vid>,
}

fn check_all(t: &IdentityTable, m: &Model) {
    t.check().expect("structural invariants");
    // ID1: every live formula cell resolves to exactly its id; round trips.
    for (&cell, &id) in &m.live {
        assert_eq!(t.id_of(cell), Some(id), "ID1 cell→id at {cell:?}");
        assert_eq!(t.locate(id), Some(cell), "id→cell for {id}");
    }
    let members: u64 = t.live_runs().map(|(_, r)| u64::from(r.len)).sum();
    assert_eq!(
        members as usize,
        m.live.len(),
        "ID1: run members == live cells"
    );
    // ID4: retired ids are not live.
    for &(id, _) in &m.retired {
        assert_eq!(t.locate(id), None, "ID4: retired id {id} reappeared");
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 400,
        rng_algorithm: proptest::test_runner::RngAlgorithm::ChaCha,
        ..ProptestConfig::default()
    })]

    #[test]
    fn identity_id1_to_id6(ops in proptest::collection::vec(op(), 1..120)) {
        let mut t = IdentityTable::new();
        let mut m = Model { live: FxHashMap::default(), retired: Vec::new(), ever: FxHashSet::default() };
        let mut owner = 0u32;
        for o in ops {
            match o {
                Op::Set(s, r, c) => {
                    if m.live.contains_key(&(s, r, c)) { continue; }
                    let before = t.next_id();
                    let h = t.place(s, r, c, 1, owner);
                    owner += 1;
                    prop_assert_eq!(t.run(h).first_id, before);
                    prop_assert!(m.ever.insert(before));
                    m.live.insert((s, r, c), before);
                }
                Op::Family(s, r, c, n) => {
                    if (r..r + n).any(|rr| m.live.contains_key(&(s, rr, c))) { continue; }
                    let first = t.next_id();
                    t.place(s, r, c, n, FAMILY);
                    for i in 0..n {
                        prop_assert!(m.ever.insert(first + i));
                        m.live.insert((s, r + i, c), first + i);
                    }
                }
                Op::Reown(k) => {
                    if m.live.is_empty() { continue; }
                    let mut cells: Vec<Cell> = m.live.keys().copied().collect();
                    cells.sort_unstable();
                    let cell = cells[k % cells.len()];
                    let cut = t.plan_cut(cell).expect("live cell has a run");
                    let h = t.apply_cut(&cut, true, owner).expect("kept");
                    owner += 1;
                    // ID6: formula→formula keeps the id.
                    prop_assert_eq!(t.id_of(cell), Some(m.live[&cell]));
                    prop_assert_eq!(t.run(h).len, 1);
                }
                Op::Clear(k) => {
                    if m.live.is_empty() { continue; }
                    let mut cells: Vec<Cell> = m.live.keys().copied().collect();
                    cells.sort_unstable();
                    let cell = cells[k % cells.len()];
                    let cut = t.plan_cut(cell).expect("live cell has a run");
                    prop_assert!(t.apply_cut(&cut, false, 0).is_none());
                    let id = m.live.remove(&cell).unwrap();
                    prop_assert_eq!(t.id_of(cell), None);
                    m.retired.push((id, cell));
                }
                Op::Resurrect(k) => {
                    if m.retired.is_empty() { continue; }
                    let i = k % m.retired.len();
                    let (id, cell) = m.retired[i];
                    if m.live.contains_key(&cell) {
                        // The cell is occupied again: replay must refuse.
                        let is_conflict = matches!(t.resurrect(id, cell, owner), Err(IdError::Conflict(_)));
                        prop_assert!(is_conflict);
                        continue;
                    }
                    t.resurrect(id, cell, owner).expect("replay of a retired id");
                    owner += 1;
                    m.retired.swap_remove(i);
                    m.live.insert(cell, id);
                    // A live id cannot be resurrected twice.
                    let is_conflict = matches!(t.resurrect(id, (cell.0, cell.1 + 100, cell.2), owner), Err(IdError::Conflict(_)));
                    prop_assert!(is_conflict);
                }
            }
            check_all(&t, &m);
        }
    }
}

/// N2 (re-review): filling the hole between two runs of one column gets a
/// fresh id; lookups on both sides and in the hole stay exact.
#[test]
fn hole_fill_between_runs_gets_fresh_id() {
    let mut t = IdentityTable::new();
    t.place(0, 0, 0, 1, FAMILY); // row 0: id 0
    t.place(0, 2, 0, 1, FAMILY); // row 2: id 1
    t.place(0, 4, 0, 1, FAMILY); // row 4: id 2
    t.place(0, 1, 0, 1, 7); // hole: id 3
    assert_eq!(t.id_of((0, 0, 0)), Some(0));
    assert_eq!(t.id_of((0, 1, 0)), Some(3));
    assert_eq!(t.id_of((0, 2, 0)), Some(1));
    assert_eq!(t.id_of((0, 3, 0)), None);
    assert_eq!(t.locate(3), Some((0, 1, 0)));
    t.check().unwrap();
}

/// B2 (red team): many splits of one family; lookups and reverse lookups
/// stay exact, and adjacent independently allocated families never merge.
#[test]
fn split_merge_reverse_lookup() {
    let mut t = IdentityTable::new();
    t.place(0, 0, 0, 100, FAMILY); // ids 0..100
    t.place(0, 100, 0, 50, FAMILY); // ids 100..150: contiguous rows and ids
    for r in (1..100).step_by(3) {
        let cut = t.plan_cut((0, r, 0)).unwrap();
        t.apply_cut(&cut, false, 0);
    }
    for r in 0..150u32 {
        let expect = (r >= 100 || r % 3 != 1).then_some(r);
        assert_eq!(t.id_of((0, r, 0)), expect, "row {r}");
        if let Some(id) = expect {
            assert_eq!(t.locate(id), Some((0, r, 0)));
        }
    }
    // Coalescing is refused across owners even with contiguous ids.
    let merged = t.coalesce_at((0, 100, 0), &|_, _| false);
    assert_eq!(merged, 0);
    t.check().unwrap();
}

/// The counter is checked before anything is allocated.
#[test]
fn counter_exhaustion_is_checked_first() {
    let t = IdentityTable::with_limit(10);
    assert!(t.check_alloc(10).is_ok());
    assert!(matches!(t.check_alloc(11), Err(IdError::Exhausted { .. })));
}

/// Dry-run replica of a cut sequence predicts slots exactly.
#[test]
fn identity_shadow_matches_cuts() {
    let mut t = IdentityTable::new();
    t.place(0, 0, 0, 20, FAMILY);
    t.place(0, 0, 1, 20, FAMILY);
    for (row, keep) in [(0u32, true), (5, false), (19, true), (7, true), (6, false)] {
        let cut = t.plan_cut((0, row, 0)).unwrap();
        let mut sh = t.shadow();
        IdentityTable::shadow_cut(&mut sh, &cut, keep);
        t.try_reserve_for(&sh).unwrap();
        t.apply_cut(&cut, keep, 3);
        assert_eq!(t.shadow(), sh, "cut at {row} keep {keep}");
        assert_eq!(t.heap_bytes(), sh.heap_bytes());
    }
    t.check().unwrap();
}
