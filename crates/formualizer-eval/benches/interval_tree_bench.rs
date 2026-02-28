use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use formualizer_eval::engine::interval_tree::{BTreeIntervalTree, IntervalTree};
use std::collections::HashSet;

fn bench_tree_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("IntervalTree Full Suite");
    let sizes = [100, 1000, 5000, 10_000, 25_000];

    for n in sizes.iter() {
        // --- 1. INSERTION (Sequential) ---
        group.bench_with_input(BenchmarkId::new("BST/Insert/Sequential", n), n, |b, &n| {
            b.iter(|| {
                let mut tree = IntervalTree::new();
                for i in 0..n {
                    tree.insert(i, i, black_box(i));
                }
            });
        });
        group.bench_with_input(
            BenchmarkId::new("BTree/Insert/Sequential", n),
            n,
            |b, &n| {
                b.iter(|| {
                    let mut tree = BTreeIntervalTree::new();
                    for i in 0..n {
                        tree.insert(i, i, black_box(i));
                    }
                });
            },
        );

        // --- 2. QUERY (Point Overlap) ---
        let mut bst = IntervalTree::new();
        let mut btree = BTreeIntervalTree::new();
        for i in 0..*n {
            bst.insert(i, i, i);
            btree.insert(i, i, i);
        }

        group.bench_with_input(BenchmarkId::new("BST/Query/Point", n), n, |b, _| {
            b.iter(|| bst.query(black_box(n / 2), black_box(n / 2)))
        });
        group.bench_with_input(BenchmarkId::new("BTree/Query/Point", n), n, |b, _| {
            b.iter(|| btree.query(black_box(n / 2), black_box(n / 2)))
        });

        // --- 3. REMOVAL ---
        group.bench_with_input(BenchmarkId::new("BST/Remove", n), n, |b, &n| {
            b.iter_batched(
                || bst.clone(), // Setup: clone the tree to remove from
                |mut tree| tree.remove(black_box(n / 2), black_box(n / 2), &black_box(n / 2)),
                criterion::BatchSize::SmallInput,
            )
        });
        group.bench_with_input(BenchmarkId::new("BTree/Remove", n), n, |b, &n| {
            b.iter_batched(
                || btree.clone(),
                |mut tree| tree.remove(black_box(n / 2), black_box(n / 2), &black_box(n / 2)),
                criterion::BatchSize::SmallInput,
            )
        });

        // --- 4. BULK BUILD ---
        let data: Vec<(u32, HashSet<u32>)> = (0..*n).map(|i| (i, HashSet::from([i]))).collect();
        group.bench_with_input(BenchmarkId::new("BST/BulkBuild", n), n, |b, _| {
            b.iter(|| {
                let mut tree = IntervalTree::new();
                tree.bulk_build_points(black_box(data.clone()));
            })
        });
        group.bench_with_input(BenchmarkId::new("BTree/BulkBuild", n), n, |b, _| {
            b.iter(|| {
                let mut tree = BTreeIntervalTree::new();
                tree.bulk_build_points(black_box(data.clone()));
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_tree_operations);
criterion_main!(benches);
