# Parser resource guards: release performance

Baseline: `0b69cd0b` (0.10.1 plus original parser PRs #501–508 and local Rust 1.99 compatibility). Compared against parser hardening; no folding or new execution representation. Five initial samples per arm were followed by five interleaved A/B pairs, alternating order. Figures below are the retained implementation’s interleaved measurements, not the discarded optimization.

## Method

Rust 1.99.0 release profile on Ryzen 9 3900XT. Parser processes pinned to CPU8; workbook processes to CPU8–11, Rayon4. Warm allocator/code/file caches; independent processes; no concurrent builds/tests during timing. Formula input generation and semantic fingerprint checks excluded from timing; parsing and AST destruction included. Workbook `program1-perf` uses eager public Workbook/Calamine loading; pre-read/admission checks excluded, load and first evaluation reported independently. Allocator counters disabled for timing and enabled only in separate memory processes. These are generated models and synthetic formula families, not a claim about every customer workbook. Five samples do not establish the absence of sub-percent regressions.

## Parser medians

| Workload | Baseline ns | Guarded ns | Difference ns | Difference % |
|---|---:|---:|---:|---:|
| short_distinct | 917.5 | 926.0 | 8.5 | 0.93 |
| finance_arithmetic | 1827.9 | 1856.8 | 28.9 | 1.58 |
| lookup_aggregate | 3306.1 | 3359.3 | 53.2 | 1.61 |
| structured_quoted_unicode | 1450.6 | 1413.5 | -37.1 | -2.56 |
| nested_if_12 | 10345.6 | 10419.4 | 73.8 | 0.71 |
| wide_sum_64 | 16509.4 | 16155.8 | -353.7 | -2.14 |
| array_8x8 | 9658.8 | 9280.0 | -378.8 | -3.92 |
| left_chain_128 | 50831.7 | 49541.9 | -1289.7 | -2.54 |
| powers_parenthesized | 1894.1 | 1913.7 | 19.6 | 1.04 |
| batch_repeat_64 | 655.9 | 653.8 | -2.1 | -0.32 |
| batch_workingset_4096 | 678.0 | 671.2 | -6.9 | -1.01 |
| batch_unique_stream | 1289.9 | 1136.4 | -153.4 | -11.90 |

Common short/finance/lookup formula medians increased by 8.5/28.9/53.2 ns (0.93/1.58/1.61%). Other shapes differ in both directions. This is measured overhead, not a zero-cost claim. All structural digests match.

## Workbook load medians

| Model | Formulas | Baseline ms | Guarded ms | Difference % |
|---|---:|---:|---:|---:|
| finance | 548 | 5.75 | 5.80 | 0.94 |
| ops | 15102 | 72.04 | 73.25 | 1.68 |
| chain100k | 99999 | 261.72 | 264.12 | 0.92 |
| shared10k | 10000 | 33.26 | 33.11 | -0.45 |
| nested10k | 10000 | 86.85 | 86.02 | -0.96 |
| finance1m | 1000000 | 4065.22 | 4012.08 | -1.31 |

The 1M model measured 4.065 → 4.012 seconds; this should not be described as a proven speedup. Ops medians rose 1.68%, but its individual paired changes were -0.39/+2.68/+0.22/+1.55/-0.87%; the evidence does not show a consistent direction. All fixture admission-rejection counts are zero; calculations succeeded and value digests match. No whole-workbook load regression was established outside the observed variation on this set; this does not prove all workloads unaffected.

## Policies that were measured and rejected

- A proposed 1,024-entry lexical cache thrashed at a 4,096-formula working set: about +74% parse latency. The retained default is 16,384 entries with the same 8 MiB source/token payload bound. Hits remain FIFO/non-mutating; larger working sets can still lose reuse and should tune capacity. Container overhead is separately bounded by entry count.
- A token-count certification fast path was mathematically safe but did not improve measured throughput (some short/mixed parser cases were slower). It was removed; retained release binaries rebuilt byte-identically to the measured guard candidate.

## Separate allocator-counted checks

One separate allocator-counted process per arm (not timing evidence) gave identical tracked live/peak bytes and evaluation digests:

| Model | Live after load B | Live after evaluation B | Peak through load/evaluation B |
|---|---:|---:|---:|
| shared10k | 2,091,307 | 2,224,462 | 8,727,080 |
| finance1m | 22,653,106 | 44,347,868 | 194,597,906 |

These counters do not prove unchanged RSS or transient allocation counts on all workloads, nor measure cache policy in a persistent batch parser.

## Reproduction

```bash
cargo +1.99.0 build --release -p formualizer-parse --example parser_guard_bench
taskset -c 8 target/release/examples/parser_guard_bench

cargo +1.99.0 build --release -p formualizer-bench-core --features formualizer_runner --bin program1-perf
RAYON_NUM_THREADS=4 taskset -c 8-11 target/release/program1-perf \
  --xlsx PATH --mode ephemeral --edits 0 --no-alloc-count
```

Run one warmup and retain five process results for each source variant, alternating A/B order. Use distinct target directories for different worktree sources. `BENCH_CACHE_ENTRIES` is an untimed benchmark-only override to isolate cache retention policy; baseline has no bound. Normal benchmark uses production defaults. Source text and cache payloads are not logged inside timed loops.

## Fixture identity

| Model | File | SHA256 |
|---|---|---|
| finance | real_finance_model_v1.xlsx | cc37cddc6d77036a3230ab3536fa46d30239ae7bfea8a0b82b9cdce9e15fbbd3 |
| ops | real_ops_model_v1.xlsx | 1cc1b1dc26c7c2c2908ef7ed20e36ebb28e6fe88b4362c417b85791c54ed5f5e |
| chain100k | chain_100k.xlsx | f2a9cc2f1e7e2f042a639cca3286757002363f4fe96565ca93c48d4cbfab8d93 |
| shared10k | shared-10000.xlsx | 86a774c0948676c725e968bb66be734916e48bcef32973cda17551354731b644 |
| nested10k | nested-10000.xlsx | fff240b35db954c2d8054585eacfc9ce5585195d9da13f72672849220bee173d |
| finance1m | fin_1m.xlsx | 94823c46cd53837d45802424527cf70c7a07515e38f655449ea16dd1efae6f1c |

The finance/ops corpus fixtures are generated by the repository; the 1M financial and shared/nested family fixtures are generated benchmark assets. Raw min/median/max samples and manifests are retained in the accompanying investigation artifacts; fixture hashes identify the exact inputs, not interchangeable same-sized models.
