//! M1a correction round (review B1–B5, store level): counter-based scaling
//! of slot planning and accounting, bounded retention under churn at a
//! fixed live size, complete scratch admission, and transactional
//! allocation under injected allocation failures (with the allocator's
//! measured peak and net checked against the prediction).

use super::super::geom::{Cell, Rect};
use super::super::identity::Vid;
use super::super::proj::RefProj;
use super::super::store::{
    AuthorityError, Counts, Digest, EdgeSpec, FormulaFacts, LkKey, MutationReport, OriginSpec,
    Store, Tag,
};
use super::alloc::measure;
use super::support::*;

fn one(l: u64, literal: u32) -> FormulaFacts {
    Formula {
        refs: vec![],
        l,
        literal,
    }
    .facts()
}

fn with_refs(refs: Vec<RefProj>, l: u64, literal: u32) -> FormulaFacts {
    Formula { refs, l, literal }.facts()
}

// ---------------------------------------------------------------- B1

/// Bulk build and range clear of N `=1` formulas (one literal, no edges):
/// the slot planning work is at most 2 visits per formula at every N (one
/// per row, plus one per page run and per touched page), and the whole
/// clear's dry-run work at most 4 per formula.
#[test]
fn slot_planning_work_is_linear_in_build_and_clear() {
    let mut rows = Vec::new();
    for n in [4_096u32, 16_384, 65_536] {
        let input = (0..n).map(|r| ((0, r, 0), one(1, 1))).collect();
        let mut s = Store::build(input);
        let build = s.stats.slot_work;
        let (w0, p0) = (s.stats.slot_work, s.stats.plan_work);
        s.clear_rect(0, Rect::new(0, 0, n - 1, 0)).unwrap();
        assert_eq!(s.formula_count(), 0);
        assert_eq!(s.slots().rows(), 0);
        assert_eq!(s.slots().pages(), 0, "emptied pages are freed");
        let (clear_slot, clear_plan) = (s.stats.slot_work - w0, s.stats.plan_work - p0);
        s.check().unwrap();
        let n64 = u64::from(n);
        assert!(build <= 2 * n64, "build slot work {build} for {n}");
        assert!(
            clear_slot <= 2 * n64,
            "clear slot work {clear_slot} for {n}"
        );
        assert!(
            clear_plan <= 4 * n64,
            "clear plan work {clear_plan} for {n}"
        );
        rows.push((n, build, clear_slot, clear_plan));
    }
    eprintln!("B1 work (n, build slot, clear slot, clear plan): {rows:?}");
}

// ---------------------------------------------------------------- B2

/// N unique-template appends, then N updates of one formula in a store of
/// N singletons: the dry-run/accounting work of every mutation is bounded
/// by the same constant at every N, and no census runs on the mutation
/// path.
#[test]
fn mutation_accounting_work_is_independent_of_store_size() {
    let mut maxes = Vec::new();
    for n in [1_024u32, 4_096, 16_384] {
        let mut s = Store::new();
        let census = super::super::store::census_calls();
        let mut max_append = 0;
        for r in 0..n {
            let w = s.stats.plan_work;
            s.set_formula((0, r, 0), &one(u64::from(r), 1)).unwrap();
            max_append = max_append.max(s.stats.plan_work - w);
        }
        let mut max_update = 0;
        for i in 0..n {
            let w = s.stats.plan_work;
            s.set_formula((0, n / 2, 0), &one(u64::from(n + i), 2))
                .unwrap();
            max_update = max_update.max(s.stats.plan_work - w);
        }
        assert_eq!(
            super::super::store::census_calls(),
            census,
            "a census ran on the mutation path"
        );
        assert_eq!(s.formula_count(), u64::from(n));
        s.check().unwrap();
        maxes.push((n, max_append, max_update));
    }
    eprintln!("B2 max work per mutation (n, append, update): {maxes:?}");
    let (_, a0, u0) = maxes[0];
    for &(n, a, u) in &maxes {
        assert!(
            a <= a0 && u <= u0,
            "work per mutation grew at {n}: {maxes:?}"
        );
    }
    assert!(a0 <= 32 && u0 <= 32, "{maxes:?}");
}

/// Re-review R2: a point edit's identity planning visits the mutated sheet
/// only. S sheets hold one independent `=1` each; editing a formula on
/// sheet 0, and appending a formula to a fresh sheet, cost the same work at
/// every S.
#[test]
fn point_edit_work_is_independent_of_sheet_count() {
    let mut maxes = Vec::new();
    for sheets in [16u16, 64, 256, 1_024] {
        let mut s = Store::new();
        let mut max_append = 0;
        for sh in 0..sheets {
            let w = s.stats.plan_work;
            s.set_formula((sh, 0, 0), &one(u64::from(sh), 1)).unwrap();
            max_append = max_append.max(s.stats.plan_work - w);
        }
        let mut max_edit = 0;
        for i in 0..64u64 {
            let w = s.stats.plan_work;
            s.set_formula((0, 0, 0), &one(10_000 + i, 2)).unwrap();
            max_edit = max_edit.max(s.stats.plan_work - w);
        }
        s.check().unwrap();
        maxes.push((sheets, max_append, max_edit));
    }
    eprintln!("R2 max work per mutation (sheets, append, edit): {maxes:?}");
    let (_, a0, e0) = maxes[0];
    for &(n, a, e) in &maxes {
        assert!(a <= a0 && e <= e0, "work grew with {n} sheets: {maxes:?}");
    }
}

/// Re-review N1: N formulas on sheet 0, formula i reading one value cell
/// on its own sheet i + 1, cleared by one range clear. Index planning looks
/// sheets up in O(log d), so the clear's work is O(N log N), where the
/// linear touched-sheet search was Θ(N²).
#[test]
fn range_clear_over_distinct_target_sheets_is_not_quadratic() {
    let mut runs = Vec::new();
    for n in [256u32, 1_024, 4_096] {
        let mut s = Store::new();
        for r in 0..n {
            let f = with_refs(vec![abs(0, 0, (r + 1) as u16)], 1, 1);
            s.set_formula((0, r, 0), &f).unwrap();
        }
        let w = s.stats.plan_work;
        s.clear_rect(0, Rect::new(0, 0, n - 1, 0)).unwrap();
        let work = s.stats.plan_work - w;
        assert_eq!(s.formula_count(), 0);
        s.check().unwrap();
        runs.push((n, work));
    }
    eprintln!("N1 range-clear work (n, work): {runs:?}");
    for &(n, work) in &runs {
        let log = u64::from(32 - n.leading_zeros());
        assert!(
            work <= 16 * u64::from(n) * log,
            "clear of {n} formulas over {n} sheets did {work} work: {runs:?}"
        );
    }
    // Quadrupling N must not come near sixteen-fold work.
    for w in runs.windows(2) {
        assert!(w[1].1 <= w[0].1 * 6, "super-linear growth: {runs:?}");
    }
}

// ---------------------------------------------------------------- B3

/// Run 128 warm-up steps, then 10,000 more; return the maximum retained
/// bytes of each phase.
fn churn(mut step: impl FnMut(&mut Store, u32)) -> (u64, u64) {
    let mut s = Store::new();
    let mut warm = 0;
    for i in 0..128 {
        step(&mut s, i);
        warm = warm.max(s.heap_bytes());
    }
    let mut late = 0;
    for i in 128..10_128 {
        step(&mut s, i);
        late = late.max(s.heap_bytes());
        if i % 1_000 == 0 {
            s.check().unwrap();
        }
    }
    s.check().unwrap();
    assert_eq!(s.formula_count(), 1);
    (warm, late)
}

/// One live formula (a literal, a relative and an absolute reference)
/// cleared and restored 10,000 times: fresh ids every time, bounded bytes.
#[test]
fn one_live_formula_clear_restore_is_bounded() {
    let f = with_refs(vec![rel(0, -1, 0), abs(2, 1, 0)], 1, 7);
    let (warm, late) = churn(|s, _| {
        let _ = s.clear_cell((0, 4, 3)).unwrap();
        s.set_formula((0, 4, 3), &f).unwrap();
    });
    assert!(
        late <= warm,
        "retention grew with history: warm {warm}, late {late}"
    );
}

/// One live formula replaced 10,000 times by a new unique template with a
/// new absolute reference (a new node group and a new edge group each
/// time, the old ones reclaimed): bounded bytes.
#[test]
fn one_live_formula_unique_template_replace_is_bounded() {
    let (warm, late) = churn(|s, i| {
        let f = with_refs(vec![abs(i, 1, 0), rel(0, -1, 0)], u64::from(i) + 100, i % 3);
        s.set_formula((0, 4, 3), &f).unwrap();
    });
    assert!(
        late <= warm,
        "retention grew with history: warm {warm}, late {late}"
    );
}

/// Review B3 repro: same formula punched and refilled.
#[test]
fn review_same_formula_punch_refill_has_bounded_slot_memory() {
    let f = Formula {
        refs: vec![],
        l: 1,
        literal: 1,
    }
    .facts();
    let mut s = Store::new();
    for _ in 0..128 {
        s.set_formula((0, 0, 0), &f).unwrap();
        s.clear_cell((0, 0, 0)).unwrap();
    }
    let warm = s.slots().heap_bytes();
    for _ in 0..4096 {
        s.set_formula((0, 0, 0), &f).unwrap();
        s.clear_cell((0, 0, 0)).unwrap();
    }
    assert_eq!(s.formula_count(), 0);
    assert!(
        s.slots().heap_bytes() <= warm * 2,
        "one-cell churn: warm={warm}, now={}",
        s.slots().heap_bytes()
    );
}

/// Review B3 repro: replaced unique groups.
#[test]
fn review_replaced_unique_groups_have_bounded_retention() {
    let mut s = Store::new();
    for l in 1..=128 {
        let mut f = Formula {
            refs: vec![],
            l,
            literal: 0,
        }
        .facts();
        f.literals.clear();
        s.set_formula((0, 0, 0), &f).unwrap();
    }
    let warm = s.heap_bytes();
    for l in 129..=4224 {
        let mut f = Formula {
            refs: vec![],
            l,
            literal: 0,
        }
        .facts();
        f.literals.clear();
        s.set_formula((0, 0, 0), &f).unwrap();
    }
    assert_eq!(s.formula_count(), 1);
    assert!(
        s.heap_bytes() <= warm * 2,
        "one live owner: warm={warm}, now={}",
        s.heap_bytes()
    );
}

/// Re-review R5 (the review's repro): build scratch includes what the input
/// owns (here a 1,000,000-token L stream).
#[test]
fn review_build_scratch_includes_owned_tokens() {
    let mut f = Formula {
        refs: vec![],
        l: 1,
        literal: 0,
    }
    .facts();
    f.literals.clear();
    f.ltokens = Some(vec![17u64; 1_000_000].into_boxed_slice());
    let token_bytes = f.ltokens.as_ref().unwrap().len() * size_of::<u64>();
    let (_s, scratch) = Store::build_keeping(vec![((0, 0, 0), f)], None).unwrap();
    assert!(
        scratch >= token_bytes as u64,
        "owned input tokens omitted: scratch={scratch}, tokens={token_bytes}"
    );
}

/// Re-review R5: a rebuild whose input and previous store already exceed
/// the scratch budget is rejected before the build allocates anything.
#[test]
fn rebuild_preflight_rejects_before_building() {
    use super::super::store::Budget;
    let mut f = one(1, 1);
    f.ltokens = Some(vec![17u64; 100_000].into_boxed_slice());
    let budget = Budget {
        scratch: Some(64 * 1024),
        ..Budget::default()
    };
    let (r, alloc) = measure(None, || {
        Store::rebuild(vec![((0, 0, 0), f)], None, budget).map(|_| ())
    });
    match r {
        Err(AuthorityError::Admission { resource, .. }) => assert_eq!(resource, "scratch"),
        other => panic!("preflight did not reject: {other:?}"),
    }
    // The rejected build never ran: nothing near a store's worth allocated.
    assert!(alloc.peak < 4 * 1024, "{alloc:?}");
}

/// Final gate R3, store level: one live formula whose single symbol edge
/// gets an ever-new LK key. The directory is compacted in place (no
/// rebuild), so its bytes stay bounded; each compaction's work is paid by
/// the dead keys it drops.
#[test]
fn ever_new_lk_keys_at_one_live_formula_stay_bounded() {
    let step = |s: &mut Store, i: u32| {
        let mut f = one(1, 1);
        f.edges = vec![EdgeSpec {
            proj: abs(5, 5, 0),
            tag: Tag::R1,
            origin: OriginSpec::Symbol(LkKey {
                ctx: 0,
                kind: 1,
                name: format!("Key_{i:08}").into_boxed_str(),
            }),
        }];
        s.set_formula((0, 0, 0), &f).unwrap();
    };
    let mut s = Store::new();
    let mut warm = 0;
    for i in 0..128 {
        step(&mut s, i);
        warm = warm.max(
            s.bytes_breakdown()
                .iter()
                .find(|c| c.0 == "lk_dir")
                .unwrap()
                .1,
        );
    }
    let mut late = 0;
    for i in 128..10_128 {
        step(&mut s, i);
        late = late.max(
            s.bytes_breakdown()
                .iter()
                .find(|c| c.0 == "lk_dir")
                .unwrap()
                .1,
        );
    }
    s.check().unwrap();
    eprintln!(
        "R3 store: lk_dir warm {warm} late {late}, compactions {}",
        s.stats.lk_compactions
    );
    assert!(s.lk_len() <= 2 * (s.egroup_slots() + 8) + 1);
    assert!(
        late <= warm,
        "LK directory grew with history: {warm} -> {late}"
    );
    assert!(s.stats.lk_compactions > 100);
    // Amortized: total compaction work stays within a constant per key ever
    // interned (10,128 here), whatever the history.
    assert!(
        s.stats.lk_compaction_work <= 3 * 10_128,
        "compaction work {}",
        s.stats.lk_compaction_work
    );
}

/// B-23: every allocation of a compaction may fail; the mutation still
/// commits, the compaction is skipped (counted), and the logical state
/// equals the unfailed run's.
#[test]
fn lk_compaction_allocation_failures_skip_cleanly() {
    let key = |i: u32| LkKey {
        ctx: 0,
        kind: 1,
        name: format!("Key_{i:08}").into_boxed_str(),
    };
    let facts = |i: u32| {
        let mut f = one(1, 1);
        f.edges = vec![EdgeSpec {
            proj: abs(5, 5, 0),
            tag: Tag::R1,
            origin: OriginSpec::Symbol(key(i)),
        }];
        f
    };
    // Grow the directory to just below the trigger; the next set compacts.
    let mut base = Store::new();
    let mut i = 0;
    loop {
        let c = base.stats.lk_compactions;
        let mut probe = base.clone();
        probe.set_formula((0, 0, 0), &facts(i)).unwrap();
        if probe.stats.lk_compactions > c {
            break;
        }
        base.set_formula((0, 0, 0), &facts(i)).unwrap();
        i += 1;
    }
    let next = facts(i);
    let mut clean = base.clone();
    let (_, m) = measure(None, || clean.set_formula((0, 0, 0), &next).unwrap());
    assert!(clean.stats.lk_compactions > base.stats.lk_compactions);
    let want = logical(&clean);
    let mut skipped = 0;
    for n in 1..=m.allocs {
        let mut s = base.clone();
        let skips = s.stats.lk_compaction_skips;
        let (r, _) = measure(Some(n), || s.set_formula((0, 0, 0), &next));
        s.check().unwrap();
        match r {
            Ok(_) => {
                // `logical` excludes bytes, which differ when skipped.
                assert_eq!(logical(&s), want, "allocation {n}");
                if s.stats.lk_compaction_skips > skips {
                    skipped += 1;
                }
            }
            Err(e) => {
                assert_eq!(e, AuthorityError::Alloc, "allocation {n}");
                assert_eq!(logical(&s), logical(&base), "allocation {n}");
            }
        }
    }
    eprintln!(
        "B-23 compaction fail-Nth: {} allocations, {skipped} skipped compactions",
        m.allocs
    );
    assert!(skipped > 0, "no failure landed in the compaction");
}

/// Final gate 2: a compaction's measured peak stays within the mutation's
/// reported peak, with one-byte names (no slack from key heap), and a
/// scratch budget below the compaction's transient skips it cleanly.
#[test]
fn lk_compaction_peak_is_admitted_and_reported() {
    let names = "abcdefghijklmnopqrstuvwxyz";
    let facts = |i: usize| {
        let mut f = one(1, 1);
        f.edges = vec![EdgeSpec {
            proj: abs(5, 5, 0),
            tag: Tag::R1,
            origin: OriginSpec::Symbol(LkKey {
                ctx: (i / 26) as u16,
                kind: 1,
                name: names[i % 26..i % 26 + 1].into(),
            }),
        }];
        f
    };
    let mut s = Store::new();
    let mut checked = 0;
    for i in 0..400usize {
        let f = facts(i);
        let c = s.stats.lk_compactions;
        let (r, m) = measure(None, || s.set_formula((0, 0, 0), &f).unwrap());
        if s.stats.lk_compactions > c {
            assert!(
                m.peak as u64 <= r.predicted_peak_above_before,
                "step {i}: measured peak {} above reported {}",
                m.peak,
                r.predicted_peak_above_before
            );
            checked += 1;
        }
    }
    assert!(checked > 5, "only {checked} compactions checked");
    s.check().unwrap();

    // Tight scratch: the compaction is skipped, nothing else changes. 200
    // live keys make the compaction's transient larger than the edit's.
    let mut base = Store::new();
    for r in 1..=200u32 {
        let mut f = one(1, 1);
        f.edges = vec![EdgeSpec {
            proj: abs(5, 5, 0),
            tag: Tag::R1,
            origin: OriginSpec::Symbol(LkKey {
                ctx: 999,
                kind: 1,
                name: format!("live{r}").into_boxed_str(),
            }),
        }];
        base.set_formula((0, r, 0), &f).unwrap();
    }
    let mut i = 0usize;
    loop {
        let mut probe = base.clone();
        probe.set_formula((0, 0, 0), &facts(i)).unwrap();
        if probe.stats.lk_compactions > 0 {
            break;
        }
        base.set_formula((0, 0, 0), &facts(i)).unwrap();
        i += 1;
    }
    let f = facts(i);
    let mut unbudgeted = base.clone();
    let r = unbudgeted.set_formula((0, 0, 0), &f).unwrap();
    let mut tight = base.clone();
    // Enough for the mutation, below the compaction's own transient.
    tight.budget.scratch = Some(r.predicted_transient);
    let before_lk = tight.lk_len();
    tight.set_formula((0, 0, 0), &f).unwrap();
    assert_eq!(tight.stats.lk_compactions, 0);
    assert_eq!(tight.stats.lk_compaction_skips, 1);
    assert_eq!(tight.lk_len(), before_lk + 1, "directory changed on skip");
    assert_eq!(logical(&tight), logical(&unbudgeted));
    tight.check().unwrap();
}

/// Final gate R5 (the review's repro): a zero-edge singleton on a high
/// sheet builds no sheet-sized role directories, and the measured peak
/// stays within retained + reported scratch.
#[test]
fn review_sparse_high_sheet_build_peak_is_bounded() {
    let ((s, scratch), alloc) = measure(None, || {
        let mut f = Formula {
            refs: vec![],
            l: 1,
            literal: 0,
        }
        .facts();
        f.literals.clear();
        Store::build_keeping(vec![((4095, 0, 0), f)], None).unwrap()
    });
    assert!(
        alloc.peak as u64 <= s.heap_bytes() + scratch,
        "peak={}, retained={}, scratch={scratch}",
        alloc.peak,
        s.heap_bytes()
    );
}

// ---------------------------------------------------------------- B5

/// Review B5(a) repro: zero scratch refuses the first literal-bearing
/// formula and leaves the state unchanged.
#[test]
fn review_zero_scratch_must_not_admit_allocating_plan() {
    let f = Formula {
        refs: vec![],
        l: 1,
        literal: 1,
    }
    .facts();
    let mut s = Store::new();
    s.budget.scratch = Some(0);
    let before = s.counts();
    let result = s.set_formula((0, 0, 0), &f);
    assert!(
        matches!(
            result,
            Err(AuthorityError::Admission {
                resource: "scratch",
                ..
            })
        ),
        "allocated plan/apply scratch under zero budget: {result:?}"
    );
    assert_eq!(s.counts(), before);
}

type Op = Box<dyn Fn(&mut Store) -> Result<MutationReport, AuthorityError>>;

/// Logical state: counts without the retained bytes (spare capacity may
/// remain after a failed reservation), live model bytes, the canonical
/// digest and every live cell's id.
fn logical(s: &Store) -> (Counts, u64, Digest, Vec<(Cell, Option<Vid>)>) {
    let mut c = s.counts();
    c.bytes = 0;
    let ids = s
        .formula_cells()
        .into_iter()
        .map(|c| (c, s.ids().id_of(c)))
        .collect();
    (c, s.live_model_bytes(), s.digest(), ids)
}

fn family(m: &mut Model, col: u32, rows: std::ops::Range<u32>, l: u64) {
    for r in rows {
        m.cells.insert(
            (0, r, col),
            Formula {
                refs: vec![rel(0, -1, 0)],
                l,
                literal: 1,
            },
        );
    }
}

/// Mutations covering every staged container: a new symbol key and new
/// groups, family punches with index flushes, a range clear with index
/// rebuilds and group reclamation, a kept id whose row grows, a unique
/// template replacing another (index rebuilds of the group tables), and
/// mutations whose repartitions commit.
fn scenarios() -> Vec<(String, Store, Op)> {
    let mut out: Vec<(String, Store, Op)> = Vec::new();

    let mut m = Model::default();
    family(&mut m, 3, 0..300, 0);
    family(&mut m, 5, 10..200, 0);
    let fams = build_from(&m);

    let sym = FormulaFacts {
        edges: vec![
            EdgeSpec {
                proj: abs(3, 3, 0),
                tag: Tag::X,
                origin: OriginSpec::Symbol(LkKey {
                    ctx: 0,
                    kind: 1,
                    name: "SymbolUnderTest".into(),
                }),
            },
            EdgeSpec {
                proj: rel(0, -1, 0),
                tag: Tag::R1,
                origin: OriginSpec::Text,
            },
        ],
        ..with_refs(vec![], 9, 4)
    };
    out.push((
        "new singleton with a new symbol key".into(),
        fams.clone(),
        Box::new(move |s| s.set_formula((0, 400, 9), &sym)),
    ));
    out.push((
        "punch a family".into(),
        fams.clone(),
        Box::new(|s| s.clear_cell((0, 150, 3))),
    ));
    let mut long = with_refs(vec![rel(0, -1, 0)], 0, 1);
    long.literals.extend([
        crate::engine::arena::ValueRef::from_raw(5),
        crate::engine::arena::ValueRef::from_raw(6),
    ]);
    out.push((
        "kept id with a longer literal row".into(),
        fams.clone(),
        Box::new(move |s| s.set_formula((0, 77, 3), &long)),
    ));

    // Many singletons with edges: indexes in levels, groups beyond the
    // minimum index size.
    let mut m = Model::default();
    for r in 0..400u32 {
        m.cells.insert(
            (0, r, 1 + r % 5),
            Formula {
                refs: vec![abs(r, 0, 0), rel(0, -1, 0)],
                l: u64::from(r) + 10,
                literal: r % 3,
            },
        );
    }
    let singles = build_from(&m);
    out.push((
        "range clear with index rebuilds and group reclamation".into(),
        singles.clone(),
        Box::new(|s| s.clear_rect(0, Rect::new(0, 0, 399, 3))),
    ));
    let fresh = with_refs(vec![abs(900, 7, 0)], 9_999, 2);
    out.push((
        "unique template replaces another".into(),
        singles.clone(),
        Box::new(move |s| s.set_formula((0, 13, 4), &fresh)),
    ));

    // Repartitions: refills between singletons until a trigger commits.
    let f = with_refs(vec![rel(0, -1, 0)], 0, 1);
    let mut s = Store::new();
    s.set_formula((0, 0, 1), &f).unwrap();
    s.set_formula((0, 2, 1), &f).unwrap();
    let mut found = 0;
    for r in [1u32, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] {
        let base = s.clone();
        let rep = s.set_formula((0, r, 1), &f).unwrap();
        if rep.repartitions > 0 && found < 2 {
            let g = f.clone();
            out.push((
                format!("refill at row {r} commits a repartition"),
                base,
                Box::new(move |s| s.set_formula((0, r, 1), &g)),
            ));
            found += 1;
        }
    }
    assert!(found > 0, "no repartition scenario");
    // Edge and node repartitions on a punched-and-refilled family.
    let mut m = Model::default();
    family(&mut m, 3, 0..300, 0);
    let mut s = build_from(&m);
    let g = with_refs(vec![rel(0, -1, 0)], 0, 1);
    let mut found = false;
    for r in (10..200u32).step_by(3) {
        s.clear_cell((0, r, 3)).unwrap();
        let base = s.clone();
        let committed = s.stats.repartitions_committed;
        s.set_formula((0, r, 3), &g).unwrap();
        if s.stats.repartitions_committed > committed + 1 {
            let g = g.clone();
            out.push((
                format!("refill at row {r} of a family commits edge and node repartitions"),
                base,
                Box::new(move |s| s.set_formula((0, r, 3), &g)),
            ));
            found = true;
            break;
        }
    }
    assert!(found, "no family refill commits two repartitions");
    out
}

/// Fail every allocation of every scenario in turn: a failure before the
/// apply returns `Alloc` with the logical state unchanged; a failure in a
/// repartition skips it (the mutation stands, the canonical relation and
/// ids equal the unfailed run's). Unfailed, the allocator's peak is within
/// the predicted peak and its net equals the accounted retained change.
#[test]
fn every_allocation_failure_is_transactional() {
    for (name, base, op) in scenarios() {
        let mut s = base.clone();
        let (r, m) = measure(None, || op(&mut s));
        let r = r.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(!m.failed);
        assert!(
            m.peak >= 0 && m.peak as u64 <= r.predicted_peak_above_before,
            "{name}: measured peak {} above predicted {}",
            m.peak,
            r.predicted_peak_above_before
        );
        assert_eq!(
            m.net,
            r.after.bytes as i64 - r.before.bytes as i64,
            "{name}: allocator net != accounted retained change"
        );
        s.check().unwrap_or_else(|e| panic!("{name}: {e}"));
        let expected = (s.digest(), logical(&s).3);
        let base_state = logical(&base);
        let (mut refused, mut skipped, mut spare) = (0, 0, 0);
        for n in 0..m.allocs {
            let mut s = base.clone();
            let (r, fm) = measure(Some(n), || op(&mut s));
            assert!(fm.failed, "{name}: allocation {n} not reached");
            match r {
                Err(AuthorityError::Alloc) => {
                    refused += 1;
                    assert_eq!(
                        logical(&s),
                        base_state,
                        "{name}: failure at allocation {n} changed the state"
                    );
                    if s.heap_bytes() != base.heap_bytes() {
                        spare += 1;
                    }
                }
                Ok(_) => {
                    skipped += 1;
                    assert_eq!(
                        (s.digest(), logical(&s).3),
                        expected,
                        "{name}: repartition failure at {n}"
                    );
                    assert!(s.stats.alloc_failures > base.stats.alloc_failures);
                }
                Err(e) => panic!("{name}: allocation {n}: unexpected {e}"),
            }
            s.check()
                .unwrap_or_else(|e| panic!("{name}: allocation {n}: {e}"));
        }
        eprintln!(
            "B5 {name}: {} allocations; {refused} refused (state unchanged, {spare} with spare capacity kept), {skipped} repartition skips; peak {} of predicted {}",
            m.allocs, m.peak, r.predicted_peak_above_before
        );
        assert!(refused > 0, "{name}");
    }
}
