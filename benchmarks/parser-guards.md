# Parser resource guards: parser throughput

This compares the parser before and after resource guards were added (AST node, height, token and source limits, the bounded `BatchParser` cache and token-stream validation). Workbook load is not covered here.

- Baseline: `b9568714`, which is 0.10.1 plus the contributor parser fixes #501–#508, without guards.
- Candidate: the guarded parser at `ffbc56c0`. The changes after it do not touch `formualizer-parse` code.

## Method

- Rust 1.93.0 release profile (`codegen-units = 1`, LTO) on an AMD Ryzen 9 3900XT.
- `crates/formualizer-parse/examples/parser_guard_bench.rs` is identical in both arms. In the baseline its cache-capacity call is a no-op, so the baseline cache is unbounded.
- Each process was pinned to one core (`taskset -c 20`), with one untimed warm-up process per arm.
- Seven interleaved A/B pairs alternated order: base first in odd pairs, candidate first in even pairs.
- Input generation is excluded from timing. Parsing and AST destruction are included.
- Every workload's semantic digest matched between the arms in every pair.

The host was shared with other builds during the run, with a 1-minute load average of 11.6–26.3 on 24 threads. Single samples were sometimes disturbed by up to ±60%. So read the paired medians and per-arm minimums together, not any single sample.

## Paired medians (7 pairs)

The difference is the median over pairs of candidate/base − 1.

| Workload | b9568714 median ns | tip median ns | Median paired diff % | Pairs tip faster |
|---|---:|---:|---:|---:|
| short_distinct | 1,023.8 | 1,065.2 | +3.35 (range +0.8 to +58.0) | 0/7 |
| finance_arithmetic | 2,058.4 | 2,097.2 | +3.30 (range +0.3 to +17.2) | 0/7 |
| lookup_aggregate | 3,898.7 | 3,942.1 | +3.58 (range -2.9 to +15.0) | 1/7 |
| structured_quoted_unicode | 1,591.4 | 1,597.8 | +1.52 (range -2.6 to +87.6) | 3/7 |
| nested_if_12 | 11,577.0 | 12,817.8 | +5.53 (range +1.0 to +24.8) | 0/7 |
| wide_sum_64 | 22,317.4 | 18,656.2 | +4.85 (range -32.1 to +8.4) | 1/7 |
| array_8x8 | 13,395.9 | 12,400.5 | -0.67 (range -60.5 to +19.4) | 4/7 |
| left_chain_128 | 68,385.5 | 56,326.5 | +1.80 (range -41.3 to +17.1) | 3/7 |
| powers_parenthesized | 2,124.5 | 2,231.2 | +10.17 (range -2.9 to +29.4) | 1/7 |
| batch_repeat_64 | 698.9 | 880.2 | +8.63 (range +4.3 to +27.4) | 0/7 |
| batch_workingset_4096 | 728.9 | 846.0 | +7.54 (range +3.7 to +19.1) | 0/7 |
| batch_unique_stream | 1,500.5 | 1,573.9 | -9.52 (range -18.7 to +8.3) | 5/7 |

## Per-arm minimum over 7 samples

| Workload | b9568714 min ns | tip min ns | min diff % |
|---|---:|---:|---:|
| short_distinct | 1,001.2 | 1,036.0 | +3.47 |
| finance_arithmetic | 2,030.2 | 2,046.6 | +0.81 |
| lookup_aggregate | 3,687.3 | 3,784.4 | +2.63 |
| structured_quoted_unicode | 1,464.4 | 1,541.5 | +5.26 |
| nested_if_12 | 10,933.3 | 11,295.8 | +3.32 |
| wide_sum_64 | 17,491.2 | 18,233.2 | +4.24 |
| array_8x8 | 10,039.2 | 10,316.1 | +2.76 |
| left_chain_128 | 53,549.7 | 54,641.3 | +2.04 |
| powers_parenthesized | 1,993.9 | 2,062.5 | +3.44 |
| batch_repeat_64 | 683.9 | 713.3 | +4.31 |
| batch_workingset_4096 | 720.8 | 749.2 | +3.94 |
| batch_unique_stream | 1,405.0 | 1,247.2 | -11.24 |

## Reading

- Most workloads cost 2–5% more per parse at the minimum, and the paired medians agree in direction. Common short, finance and lookup formulas cost about 15–100 ns more per parse.
- The cost comes from per-node admission (node count and height) and the source and token checks. It is measured overhead, not a zero-cost claim.
- `batch_unique_stream` is faster (about 10%), most likely because the bounded FIFO cache keys on a shared `Arc<str>` and does not copy each source into a new `String` key.
- Repeated-batch workloads (`batch_repeat_64`, `batch_workingset_4096`) are 4–9% slower, but the bounded cache still serves their working sets.
- Seven samples on a loaded host cannot resolve differences of about 1%.

## Cache policy (from guard development)

- A 1,024-entry lexical cache thrashed at a 4,096-formula working set, at about +74% parse latency. The default is therefore 16,384 entries with an 8 MiB source/token payload bound.
- Cache hits do not reorder entries (FIFO). Working sets larger than either bound can lose reuse; tune them with `BatchParser::builder().cache_capacity(entries, bytes)`.

## Reproduction

```bash
# in each source tree (copy the example into the baseline tree first)
RUSTUP_TOOLCHAIN=1.93.0 cargo build --release -p formualizer-parse --example parser_guard_bench
taskset -c 20 target/release/examples/parser_guard_bench > sample.csv
```

Run one untimed warm-up per arm, then at least five pairs in alternating order, each in a separate process. Use a separate target directory for each source tree. `BENCH_CACHE_ENTRIES` is an untimed, benchmark-only override that isolates cache-retention policy. Normal runs use the production defaults.
