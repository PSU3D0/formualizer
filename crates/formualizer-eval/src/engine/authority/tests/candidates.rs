use super::alloc::measure;
use super::support::Formula;
use crate::engine::authority::candidates::discover;
use crate::engine::authority::geom::{Cover, Rect};
use crate::engine::authority::store::{AuthorityError, Store};

fn facts(l: u32) -> super::support::Formula {
    Formula {
        refs: vec![],
        l: l.into(),
        literal: 0,
    }
}

#[test]
fn candidates_generated_cover_intersections_preserve_ids_and_owners() {
    for seed in 1..=64u64 {
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 32
        };
        let mut inputs = Vec::new();
        let mut expected = Vec::new();
        let mut cover = Cover::new();
        for sheet in 0..3 {
            for col in 0..5 {
                for row in 0..16 {
                    let included = next() % 3 != 0;
                    if included {
                        cover.insert_rect(sheet, &Rect::cell(row, col));
                    }
                    if next() % 4 != 0 {
                        inputs.push(((sheet, row, col), facts((next() % 3) as u32).facts()));
                        if included {
                            expected.push((sheet, row, col));
                        }
                    }
                }
            }
        }
        // Empty sheets/columns do not become candidate output.
        cover.insert_rect(8, &Rect::new(0, 0, 100, 3));
        let mut store = Store::build(inputs);
        for &cell in expected.iter().step_by(7) {
            let id = store.ids().lookup(cell).unwrap().0;
            store.set_formula(cell, &facts(17).facts()).unwrap();
            assert_eq!(store.ids().lookup(cell).unwrap().0, id);
        }
        let out = discover(&store, &cover, None).unwrap();
        let mut actual = Vec::new();
        let mut last = None;
        for s in &out.slices {
            let key = (s.sheet, s.col, s.r0);
            assert!(last.is_none_or(|old| old < key));
            last = Some((s.sheet, s.col, s.r1));
            for row in s.r0..=s.r1 {
                let cell = (s.sheet, row, s.col);
                actual.push(cell);
                assert_eq!(
                    store.ids().lookup(cell).unwrap().0,
                    s.first_id + (row - s.r0)
                );
                assert_eq!(store.owner_at(cell), Some(s.owner));
            }
        }
        assert_eq!(actual, expected, "seed={seed}");
        assert_eq!(out.cells as usize, expected.len());
    }
}

#[test]
fn candidates_exact_admission_and_fail_each_allocation() {
    let store = Store::build(
        (0..80)
            .map(|row| ((0, row, 0), facts(row % 2).facts()))
            .collect(),
    );
    let mut cover = Cover::new();
    cover.insert_rect(0, &Rect::new(0, 0, 79, 0));
    let (result, m) = measure(None, || discover(&store, &cover, None));
    let out = result.unwrap();
    assert_eq!(m.allocs, 2);
    assert_eq!(m.peak as u64, out.peak_heap_bytes);
    assert_eq!(m.net as u64, out.heap_bytes());
    for nth in 0..m.allocs {
        let (result, failed) = measure(Some(nth), || discover(&store, &cover, None));
        assert_eq!(result.unwrap_err(), AuthorityError::Alloc);
        assert!(failed.failed);
        assert_eq!(failed.net, 0);
    }
    for limit in [0, out.peak_heap_bytes - 1] {
        let (result, failed) = measure(None, || discover(&store, &cover, Some(limit)));
        assert!(matches!(
            result.unwrap_err(),
            AuthorityError::Admission { .. }
        ));
        assert_eq!(failed.allocs, 0);
        assert_eq!(failed.net, 0);
    }
    discover(&store, &cover, Some(out.peak_heap_bytes)).unwrap();
    let empty = discover(&store, &Cover::new(), Some(0)).unwrap();
    assert_eq!(
        (empty.cells, empty.work.total(), empty.heap_bytes()),
        (0, 0, 0)
    );
    println!(
        "CANDIDATE_ALLOC allocations={} peak={} retained={}",
        m.allocs, m.peak, m.net
    );
}

#[test]
fn candidates_counted_fragmented_scaling() {
    for n in [1024, 4096, 16384] {
        let store = Store::build(
            (0..n)
                .map(|row| ((0, row, 0), facts(row % 2).facts()))
                .collect(),
        );
        let mut cover = Cover::new();
        cover.insert_rect(0, &Rect::new(0, 0, n - 1, 0));
        let out = discover(&store, &cover, None).unwrap();
        assert_eq!(out.slices.len(), n as usize);
        assert_eq!(out.cells, n as u64);
        assert_eq!(out.work.sort, 3584 + 21 * n as u64);
        assert!(out.work.total() <= 3648 + 32 * n as u64);
        println!(
            "CANDIDATES n={n} work={} run_index={} owner_index={} peak={}",
            out.work.total(),
            out.work.run_index,
            out.work.owner_index,
            out.peak_heap_bytes
        );
    }
}
