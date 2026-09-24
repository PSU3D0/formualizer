use super::alloc::measure;
use super::support::{Formula, Rng, abs};
use crate::engine::authority::geom::Rect;
use crate::engine::authority::store::{AuthorityError, BuildInput, Store};
use std::collections::{BTreeMap, BTreeSet};

fn input(
    row: u32,
    col: u32,
    l: u64,
    refs: Vec<crate::engine::authority::proj::RefProj>,
) -> BuildInput {
    (
        (0, row, col),
        Formula {
            refs,
            l,
            literal: 0,
        }
        .facts(),
    )
}

#[test]
fn independent_owner_and_edge_partitions_are_refined() {
    // The L=1 owner spans columns 0..2 at rows 2..5. The same
    // reference also belongs to L=2 formulas in column 0 at rows 0..1.
    // Thus the reference's column-0 record extends above the owner and
    // cannot merge with its columns-1..2 record. Neither edge covers the
    // whole owner: this is NOT SP-1's exact-edge-set partition input.
    let mut inputs = Vec::new();
    for col in 0..3 {
        for row in 2..6 {
            inputs.push(input(row, col, 1, vec![abs(10, 0, 0)]));
        }
    }
    for row in 0..2 {
        inputs.push(input(row, 0, 2, vec![abs(10, 0, 0)]));
    }
    let store = Store::build(inputs);
    let owner = store.owner_at((0, 2, 0)).unwrap();
    let (_, domain, family) = store.owner_dom(owner);
    assert!(family);
    assert_eq!(domain, Rect::new(2, 0, 5, 2));
    let mut partial = 0;
    store.visit_plan_edges(0, &domain, &mut |_, dep| {
        assert!(!dep.contains_rect(&domain));
        partial += 1;
    });
    assert_eq!(partial, 2, "fixture must exercise independent partitions");
    for col in 0..3 {
        let refined = store.refine_owner_column(owner, col, 2, 5, None).unwrap();
        assert_eq!(refined.pieces.len(), 1);
        let piece = refined.pieces[0];
        assert_eq!(piece.domain, Rect::new(2, col, 5, col));
        assert_eq!((piece.edge_start, piece.edge_end), (0, 1));
        assert_eq!(refined.cell_references, 4);
        store.visit_plan_edges(0, &piece.domain, &mut |key, dep| {
            assert!(dep.contains_rect(&piece.domain));
            assert_eq!(key, refined.edges[0]);
        });
    }
}

#[test]
fn independently_maintained_row_boundaries_exercise_endpoint_sweep() {
    let mut inputs = Vec::new();
    for row in 0..64 {
        inputs.push(input(row, 0, 1, vec![abs(100, 0, 0)]));
        // This reference group has many more pieces than the target node
        // group. Their independent maintenance triggers cannot coincide.
        inputs.push(input(row * 2, 2, 2, vec![abs(100, 0, 0)]));
    }
    let mut store = Store::build(inputs);
    let facts = input(0, 0, 1, vec![abs(100, 0, 0)]).1;
    let mut exercised = false;
    for row in 10..30 {
        store.set_formula((0, row, 0), &facts).unwrap();
        let owner = store.owner_at((0, 0, 0)).unwrap();
        let (_, domain, _) = store.owner_dom(owner);
        let refined = store.refine_owner_column(owner, 0, 0, 63, None).unwrap();
        if refined.pieces.len() <= 1 {
            continue;
        }
        exercised = true;
        let mut cursor = domain.r0;
        for piece in &refined.pieces {
            assert_eq!(piece.domain.r0, cursor);
            cursor = piece.domain.r1 + 1;
            assert_eq!(piece.edge_end - piece.edge_start, 1);
            store.visit_plan_edges(0, &piece.domain, &mut |_, dep| {
                assert!(dep.contains_rect(&piece.domain));
            });
        }
        assert_eq!(cursor, domain.r1 + 1);
        assert_eq!(refined.cell_references, domain.area());
        break;
    }
    assert!(
        exercised,
        "fixture must contain row-partial edges inside one real owner"
    );
}

#[test]
fn generated_refinement_matches_raw_cell_references_after_edits() {
    // Independent cell map: no normalization by Store/Cover/RefProj.
    // Random edits also leave owner/edge partitions at different points
    // in their maintenance triggers.
    for seed in 1..=40 {
        let mut rng = Rng(seed);
        let mut cells = BTreeMap::new();
        let mut inputs = Vec::new();
        for row in 0..12 {
            for col in 0..5 {
                if rng.chance(80) {
                    let refs = (0..4)
                        .filter(|_| rng.chance(50))
                        .map(|i| abs(20 + i, 0, 0))
                        .collect::<Vec<_>>();
                    let l = u64::from(rng.below(3));
                    inputs.push(input(row, col, l, refs.clone()));
                    cells.insert((row, col), refs);
                }
            }
        }
        let mut store = Store::build(inputs);
        for _ in 0..20 {
            let (row, col) = (rng.below(12), rng.below(5));
            let refs = (0..4)
                .filter(|_| rng.chance(50))
                .map(|i| abs(20 + i, 0, 0))
                .collect::<Vec<_>>();
            let facts = input(row, col, u64::from(rng.below(3)), refs.clone()).1;
            store.set_formula((0, row, col), &facts).unwrap();
            cells.insert((row, col), refs);
        }
        let mut done = BTreeSet::new();
        let mut seen = BTreeMap::new();
        for &(row, col) in cells.keys() {
            let owner = store.owner_at((0, row, col)).unwrap();
            if !done.insert((owner, col)) {
                continue;
            }
            let refined = store.refine_owner_column(owner, col, 1, 10, None).unwrap();
            let mut r = 0;
            for piece in &refined.pieces {
                let got = refined.edges[piece.edge_start..piece.edge_end]
                    .iter()
                    .map(|e| e.proj)
                    .collect::<BTreeSet<_>>();
                assert_eq!(got.len(), piece.edge_end - piece.edge_start);
                store.visit_plan_edges(0, &piece.domain, &mut |_, dep| {
                    assert!(dep.contains_rect(&piece.domain));
                });
                for row in piece.domain.r0..=piece.domain.r1 {
                    let want = cells[&(row, col)].iter().copied().collect::<BTreeSet<_>>();
                    assert_eq!(got, want, "seed {seed}, row {row}, col {col}");
                    assert!(seen.insert((row, col), ()).is_none());
                    r += want.len() as u64;
                }
            }
            assert_eq!(refined.cell_references, r);
        }
        assert_eq!(
            seen.len(),
            cells.keys().filter(|(r, _)| (1..=10).contains(r)).count()
        );
    }
}

#[test]
fn empty_clipped_and_last_grid_row_slices_are_exact() {
    let last = crate::engine::authority::geom::MAX_ROW;
    for refs in [Vec::new(), vec![abs(0, 0, 0)]] {
        let store = Store::build(vec![input(last, 0, 1, refs.clone())]);
        let owner = store.owner_at((0, last, 0)).unwrap();
        for (col, r0, r1) in [(1, 0, last), (0, 0, last - 1), (0, 2, 1)] {
            let (result, m) = measure(None, || {
                store.refine_owner_column(owner, col, r0, r1, Some(0))
            });
            let result = result.unwrap();
            assert!(result.pieces.is_empty());
            assert!(result.edges.is_empty());
            assert_eq!(m.allocs, 0);
        }
        let result = store
            .refine_owner_column(owner, 0, last, last, None)
            .unwrap();
        assert_eq!(result.pieces.len(), 1);
        assert_eq!(result.pieces[0].domain, Rect::cell(last, 0));
        assert_eq!(result.edges.len(), refs.len());
    }
}

fn reference_fanout(n: u32) -> (Store, u32) {
    let store = Store::build(vec![input(0, 0, 1, (0..n).map(|r| abs(r, 2, 0)).collect())]);
    let owner = store.owner_at((0, 0, 0)).unwrap();
    (store, owner)
}

#[test]
fn refinement_counted_work_scales_at_three_sizes() {
    let mut prior = None;
    for n in [64, 256, 1024] {
        let (store, owner) = reference_fanout(n);
        let census = crate::engine::authority::store::census_calls();
        let refined = store.refine_owner_column(owner, 0, 0, 0, None).unwrap();
        assert_eq!(crate::engine::authority::store::census_calls(), census);
        assert_eq!(refined.edges.len(), n as usize);
        assert_eq!(refined.work.discovery, u64::from(2 * n));
        assert_eq!(refined.work.references, u64::from(2 * n));
        // A fixed 3*512 radix setup term plus actual linear loop visits.
        assert!(refined.work.total() <= 1536 + 48 * u64::from(n));
        if let Some(previous) = prior {
            assert!(refined.work.total() <= 4 * previous);
        }
        prior = Some(refined.work.total());
        println!(
            "REFINE n={n} work={:?} total={} peak={}",
            refined.work,
            refined.work.total(),
            refined.peak_heap_bytes
        );
    }
}

#[test]
fn every_refinement_allocation_is_fallible_and_exactly_admitted() {
    let (store, owner) = reference_fanout(80);
    let (result, observed) = measure(None, || store.refine_owner_column(owner, 0, 0, 0, None));
    let result = result.unwrap();
    assert_eq!(observed.peak as u64, result.peak_heap_bytes);
    assert_eq!(
        observed.net as usize,
        result.pieces.capacity() * size_of_val(&result.pieces[0])
            + result.edges.capacity() * size_of_val(&result.edges[0])
    );
    for nth in 0..observed.allocs {
        let (failed, m) = measure(Some(nth), || {
            store.refine_owner_column(owner, 0, 0, 0, None)
        });
        assert!(m.failed, "allocation {nth}");
        assert_eq!(failed.unwrap_err(), AuthorityError::Alloc);
        assert_eq!(m.net, 0, "failed scratch must not be retained");
        store.check().unwrap();
    }
    let (zero, m) = measure(None, || store.refine_owner_column(owner, 0, 0, 0, Some(0)));
    assert!(matches!(
        zero,
        Err(AuthorityError::Admission {
            resource: "scratch",
            ..
        })
    ));
    assert_eq!(m.allocs, 0);
    let too_small = store.refine_owner_column(owner, 0, 0, 0, Some(result.peak_heap_bytes - 1));
    assert!(matches!(
        too_small,
        Err(AuthorityError::Admission {
            resource: "scratch",
            ..
        })
    ));
    let exact = store
        .refine_owner_column(owner, 0, 0, 0, Some(result.peak_heap_bytes))
        .unwrap();
    assert_eq!(exact.pieces, result.pieces);
    assert_eq!(exact.edges, result.edges);
    println!(
        "REFINE fail-Nth={} peak={} retained={}",
        observed.allocs, observed.peak, observed.net
    );
}
