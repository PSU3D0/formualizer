//! Exact admission and maintenance (final-review §5 item 2): SP-2's
//! documenting tests turned into corrected tests.
//!
//! - budget rejection (retained, scratch, identity counter) leaves the
//!   state unchanged;
//! - a zero-reference punch is charged (B8);
//! - repeated punch/refill scales linearly: run-owner work per edit is
//!   bounded independently of the family's width (SP-2 F-2);
//! - one band clear fragments a whole group, and the trigger repairs it;
//! - a node repartition never grows the retained bytes (F-3), and
//!   repartition re-indexing is admitted exactly (F-5);
//! - merges never re-enter the index behind the dry run's back (F-4);
//! - a skipped repartition is counted and later recovers.

use super::super::geom::{Cell, Rect};
use super::super::identity::IdError;
use super::super::store::{AuthorityError, Budget, Store};
use super::support::build_from;
use super::support::*;

fn fam(refs: Vec<super::super::proj::RefProj>, l: u64) -> Formula {
    Formula {
        refs,
        l,
        literal: 1,
    }
}

/// Column family `rows` of `=<col-1>{r}` on sheet 0, column `col`.
fn column_family(m: &mut Model, col: u32, rows: std::ops::Range<u32>) {
    for r in rows {
        m.cells.insert((0, r, col), fam(vec![rel(0, -1, 0)], 0));
    }
}

fn state(
    s: &Store,
) -> (
    super::super::store::Counts,
    super::super::store::Digest,
    u64,
) {
    (s.counts(), s.digest(), s.heap_bytes())
}

#[test]
fn budget_rejection_leaves_state_unchanged() {
    let mut m = Model::default();
    column_family(&mut m, 3, 0..200);
    column_family(&mut m, 5, 10..300);
    let mut s = build_from(&m);
    let mut g = Rng(0xB0D);
    let mut rejected = 0;
    let mut admitted = 0;
    for step in 0..300 {
        let limit = s.heap_bytes() + u64::from(g.below(400));
        s.budget = Budget {
            retained: Some(limit),
            scratch: None,
        };
        let before = state(&s);
        let cell: Cell = (0, g.below(320), 2 + g.below(5));
        let result = if g.chance(50) {
            s.clear_cell(cell)
        } else {
            let f = fam(vec![rel(0, -1, 0)], 0);
            s.set_formula(cell, &f.facts())
        };
        match result {
            Ok(r) => {
                admitted += 1;
                assert!(
                    r.actual.bytes <= limit,
                    "admitted past the limit at step {step}"
                );
            }
            Err(AuthorityError::Admission {
                resource,
                needed,
                limit: l,
            }) => {
                rejected += 1;
                assert_eq!(resource, "retained");
                assert!(needed > l);
                assert_eq!(
                    state(&s),
                    before,
                    "rejection changed the state at step {step}"
                );
            }
            Err(e) => panic!("unexpected {e}"),
        }
        s.check().unwrap();
    }
    assert!(
        rejected > 10 && admitted > 10,
        "rejected {rejected} admitted {admitted}"
    );

    // Scratch budget: every mutation that needs transient bytes is refused.
    s.budget = Budget {
        retained: None,
        scratch: Some(0),
    };
    let before = state(&s);
    let f = fam(vec![rel(0, -1, 0), abs(1, 1, 0)], 7);
    match s.set_formula((0, 400, 9), &f.facts()) {
        Err(AuthorityError::Admission { resource, .. }) => assert_eq!(resource, "scratch"),
        other => panic!("expected a scratch rejection, got {other:?}"),
    }
    assert_eq!(state(&s), before);

    // Identity counter: checked before anything is applied.
    let mut m2 = Model::default();
    column_family(&mut m2, 3, 0..5);
    let mut limited = Store::with_id_limit(5);
    for (c, f) in m2.cells.iter() {
        limited.set_formula(*c, &f.facts()).unwrap();
    }
    let before = state(&limited);
    match limited.set_formula((0, 9, 3), &fam(vec![rel(0, -1, 0)], 0).facts()) {
        Err(AuthorityError::Identity(IdError::Exhausted { .. })) => {}
        other => panic!("expected exhaustion, got {other:?}"),
    }
    assert_eq!(state(&limited), before);
    // formula→formula keeps its id and needs no new one.
    limited
        .set_formula((0, 2, 3), &fam(vec![rel(0, -2, 0)], 1).facts())
        .unwrap();
}

/// B8: a punch with zero new references still allocates (records and
/// owners split), is charged, and the dry run equals the commit.
#[test]
fn punch_zero_refs_admission_charged() {
    let mut m = Model::default();
    column_family(&mut m, 3, 0..100);
    let mut s = build_from(&m);
    let before = s.counts();
    assert_eq!((before.records, before.owners, before.nodes), (1, 1, 1));
    let r = s.clear_cell((0, 50, 3)).unwrap();
    assert_eq!(r.predicted, r.actual);
    assert_eq!(
        (r.actual.records, r.actual.owners, r.actual.nodes),
        (2, 2, 2)
    );
    assert_eq!(r.actual.runs, 2);
    assert!(r.actual.bytes > before.bytes, "the punch must be charged");
}

/// SP-2 F-2 corrected: run-owner work per edit does not grow with the
/// family's width, and total maintenance work is linear in the edits.
#[test]
fn repeated_punch_refill_scales_linearly() {
    let mut per_op = Vec::new();
    for width in [50u32, 200, 800] {
        let mut m = Model::default();
        // A row family: one row, `width` columns, `=<row above>`.
        for c in 0..width {
            m.cells.insert((0, 5, c), fam(vec![rel(-1, 0, 0)], 0));
        }
        let mut s = build_from(&m);
        let mut g = Rng(u64::from(width) * 31 + 7);
        let n = 600u64;
        let base = s.stats.clone();
        for _ in 0..n {
            let c = g.below(width);
            s.clear_cell((0, 5, c)).unwrap();
            s.set_formula((0, 5, c), &fam(vec![rel(-1, 0, 0)], 0).facts())
                .unwrap();
        }
        s.check().unwrap();
        let st = &s.stats;
        assert!(st.max_run_relabels_one_op <= 5, "width {width}: {st:?}");
        let work = (st.run_relabels - base.run_relabels)
            + (st.repartition_work - base.repartition_work)
            + (st.records_created - base.records_created)
            + (st.owners_created - base.owners_created);
        per_op.push(work as f64 / (2 * n) as f64);
        assert!(st.max_work_per_creation <= 8.0 + 4.0, "{st:?}");
    }
    // Width 800 vs 50: per-edit work within a small constant factor.
    assert!(per_op[2] <= per_op[0] * 2.0 + 2.0, "per-op work {per_op:?}");
}

/// B8: one band clear across a 12-record group doubles its records; the
/// trigger fires on the next punch and the result equals canon.
#[test]
fn whole_group_fragmentation_then_trigger() {
    let mut m = Model::default();
    // 12 separate column families of one template (12 records per edge group).
    for col in 0..12u32 {
        column_family(&mut m, 2 * col + 1, 0..20);
    }
    let mut s = build_from(&m);
    let g0 = s.counts();
    assert_eq!(g0.records, 12);
    let r = s.clear_rect(0, Rect::new(10, 0, 10, 30)).unwrap();
    assert_eq!(r.predicted, r.actual);
    assert_eq!(r.actual.records, 24, "one band clear doubles the group");
    assert_eq!(r.repartitions, 0, "L = 24 = 2B, within 2B + 2");
    // The next punches create pieces (C_g grows by 2 each); the trigger
    // C_g > 2·B_g + 2 fires within two of them.
    let checks = s.stats.repartition_checks;
    for cell in [(0, 3, 1), (0, 4, 3)] {
        s.clear_cell(cell).unwrap();
        m.cells.remove(&cell);
        s.check().unwrap();
        if s.stats.repartition_checks > checks {
            break;
        }
    }
    assert!(
        s.stats.repartition_checks > checks,
        "the trigger did not fire: {:?}",
        s.stats
    );
    for (c, _) in m.cells.clone() {
        if c.1 == 10 {
            m.cells.remove(&c);
        }
    }
    assert_eq!(s.digest(), build_from(&m).digest());
}

/// SP-2 F-3: two singletons and a refill between them become one node; the
/// retained bytes never grow through a committed repartition.
#[test]
fn node_repartition_never_grows_bytes() {
    let mut m = Model::default();
    let f = fam(vec![rel(0, -1, 0)], 0);
    m.cells.insert((0, 0, 1), f.clone());
    m.cells.insert((0, 2, 1), f.clone());
    let mut s = build_from(&m);
    s.set_formula((0, 1, 1), &f.facts()).unwrap();
    // Force enough creations to trigger (C_g > 2·B_g + 2).
    for r in 3..12u32 {
        s.set_formula((0, r, 1), &f.facts()).unwrap();
    }
    s.check().unwrap();
    assert!(s.stats.repartitions_committed > 0);
    assert_eq!(s.stats.repartition_live_up, 0, "{:?}", s.stats);
    let o = s.owner_at((0, 5, 1)).unwrap();
    assert!(s.owner_dom(o).2, "the column re-formed a family node");
}

/// SP-2 F-4: extending a family in place is not a merge, so post-commit
/// index entries equal the dry run exactly (80 families put the dependency
/// index into levels).
#[test]
fn no_merge_reentry_behind_the_dry_run() {
    let mut m = Model::default();
    for col in 0..80u32 {
        for r in 0..9u32 {
            m.cells.insert(
                (0, r, 2 * col + 1),
                fam(vec![rel(0, -1, 0), abs(0, 0, 0)], 0),
            );
        }
    }
    let mut s = build_from(&m);
    let r = s
        .set_formula(
            (0, 9, 7),
            &fam(vec![rel(0, -1, 0), abs(0, 0, 0)], 0).facts(),
        )
        .unwrap();
    assert_eq!(r.predicted, r.actual);
    assert!(r.observed_index_transient <= r.predicted_transient);
    s.check().unwrap();
}

/// A scratch-starved repartition is skipped and counted (the group is
/// suspended, invariants hold); a mutation refused for scratch leaves the
/// state unchanged; with scratch restored the next trigger recovers.
#[test]
fn skipped_repartition_is_counted_and_recovers() {
    let mut m = Model::default();
    column_family(&mut m, 3, 0..3000);
    let mut s = build_from(&m);
    s.budget.scratch = Some(40_000);
    let mut refused = 0;
    for r in (1..3000u32).step_by(3) {
        let before = state(&s);
        match s.clear_cell((0, r, 3)) {
            Ok(_) => {}
            Err(AuthorityError::Admission {
                resource: "scratch",
                ..
            }) => {
                refused += 1;
                assert_eq!(state(&s), before);
            }
            Err(e) => panic!("{e}"),
        }
    }
    s.check().unwrap();
    assert!(
        s.stats.repartition_skipped > 0,
        "{:?} refused {refused}",
        s.stats
    );
    s.budget.scratch = None;
    let skipped = s.stats.repartition_skipped;
    let checks = s.stats.repartition_checks;
    for r in (2..3000u32).step_by(3) {
        s.clear_cell((0, r, 3)).unwrap();
    }
    s.check().unwrap();
    assert_eq!(s.stats.repartition_skipped, skipped);
    assert!(s.stats.repartition_checks > checks);
}

/// Larger traces: enough records for level flushes and rebuilds; the dry
/// run is exact at every step, the observed index transient is within the
/// admitted one, and repartitions never grow the retained bytes.
#[test]
fn dry_run_exact_on_large_traces() {
    for seed in 0..3u64 {
        let mut g = Rng(0xD0 + seed);
        let mut m = Model::default();
        for col in 1..40u32 {
            let f = random_formula(&mut g, 400);
            for r in 2..(2 + g.below(300)) {
                if Model::valid(&f, (0, r, col)) {
                    m.cells.insert((0, r, col), f.clone());
                }
            }
        }
        let mut s = build_from(&m);
        for step in 0..1500 {
            let cell: Cell = (0, g.below(400), 1 + g.below(40));
            let r = if g.chance(45) {
                m.cells.remove(&cell);
                s.clear_cell(cell).unwrap()
            } else {
                let src = (cell.0, cell.1.saturating_sub(1), cell.2);
                let f = m
                    .cells
                    .get(&src)
                    .cloned()
                    .unwrap_or_else(|| random_formula(&mut g, 400));
                if !Model::valid(&f, cell) {
                    continue;
                }
                m.cells.insert(cell, f.clone());
                s.set_formula(cell, &f.facts()).unwrap()
            };
            assert_eq!(r.predicted, r.actual, "seed {seed} step {step}");
            assert!(r.observed_index_transient <= r.predicted_transient);
            if step % 100 == 0 {
                s.check().unwrap();
            }
        }
        s.check().unwrap();
        assert_eq!(s.stats.repartition_live_up, 0);
        assert_eq!(s.digest(), build_from(&m).digest(), "seed {seed}");
    }
}

/// Memory breakdown of an all-singleton irregular workbook (report aid).
#[test]
#[ignore]
fn singleton_memory_breakdown() {
    let mut g = Rng(42);
    let mut m = Model::default();
    let n = 16_384u32;
    for r in 0..n {
        let a = g.below(n) as i32 - r as i32;
        let b = g.below(n) as i32 - r as i32;
        m.cells.insert(
            (0, r, 1),
            Formula {
                refs: vec![rel(a, -1, 0), rel(b, -1, 0)],
                l: u64::from(r % 7),
                literal: r % 7,
            },
        );
    }
    let s = build_from(&m);
    for (k, v) in s.bytes_breakdown() {
        eprintln!("{k:>16} {v:>10} {:>8.1} B/formula", v as f64 / f64::from(n));
    }
    eprintln!(
        "total {} = {:.1} B/formula",
        s.heap_bytes(),
        s.heap_bytes() as f64 / f64::from(n)
    );
}
