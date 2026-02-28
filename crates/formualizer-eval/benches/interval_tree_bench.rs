use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use formualizer_eval::engine::interval_tree::IntervalTree;
use std::collections::HashSet;

fn bench_tree_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("IntervalTree");

    // Focused sizes: 1k for typical sheets, 10k for large/dense indices.
    // 25k is removed to keep the benchmark suite fast.
    let sizes = [1000, 5000, 10_000];

    for n in sizes.iter() {
        // --- 1. SEQUENTIAL INSERT ---
        // Proves O(log N) scaling and stack safety during monotonic row/col growth.
        group.bench_with_input(BenchmarkId::new("Insert/Sequential", n), n, |b, &n| {
            b.iter(|| {
                let mut tree = IntervalTree::new();
                for i in 0..n {
                    tree.insert(i, i, black_box(i));
                }
            });
        });

        // --- 2. POINT QUERY ---
        // The "hot path" for formula evaluation and range pruning.
        let mut tree = IntervalTree::new();
        for i in 0..*n {
            tree.insert(i, i, i);
        }

        group.bench_with_input(BenchmarkId::new("Query/Point", n), n, |b, _| {
            b.iter(|| tree.query(black_box(n / 2), black_box(n / 2)))
        });

        // --- 3. BULK BUILD ---
        // Simulates initial file loading or massive copy-paste operations.
        let data: Vec<(u32, HashSet<u32>)> = (0..*n).map(|i| (i, HashSet::from([i]))).collect();

        group.bench_with_input(BenchmarkId::new("BulkBuild", n), n, |b, _| {
            b.iter(|| {
                let mut bulk_tree = IntervalTree::new();
                bulk_tree.bulk_build_points(black_box(data.clone()));
            })
        });

        // --- 4. REMOVAL ---
        // Measures efficiency of cleaning up stale dependencies.
        group.bench_with_input(BenchmarkId::new("Remove", n), n, |b, &n| {
            b.iter_batched(
                || tree.clone(),
                |mut t| t.remove(black_box(n / 2), black_box(n / 2), &black_box(n / 2)),
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, bench_tree_operations);
criterion_main!(benches);
