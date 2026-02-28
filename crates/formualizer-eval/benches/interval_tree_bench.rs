use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use formualizer_eval::engine::interval_tree::IntervalTree;
use std::collections::HashSet;

fn bench_insert_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("IntervalTree Insertion Complexity");

    // We scale N to demonstrate the O(N^2) curve
    for n in [10, 100, 500, 1000, 2000, 4000, 8000, 16_000].iter() {
        // Worst Case: Sequential Insertion (creates a linked list)
        group.bench_with_input(BenchmarkId::new("Sequential Insert", n), n, |b, &n| {
            b.iter(|| {
                let mut tree = IntervalTree::new();
                for i in 0..n {
                    // black_box prevents the compiler from optimizing away the loop
                    tree.insert(i, i, black_box(i));
                }
            });
        });

        // Best Case: Bulk Build (O(N log N))
        group.bench_with_input(BenchmarkId::new("Bulk Build", n), n, |b, &n| {
            let data: Vec<(u32, HashSet<u32>)> = (0..n)
                .map(|i| {
                    let mut set = HashSet::new();
                    set.insert(i);
                    (i, set)
                })
                .collect();

            b.iter(|| {
                let mut tree = IntervalTree::new();
                tree.bulk_build_points(black_box(data.clone()));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_insert_complexity);
criterion_main!(benches);
