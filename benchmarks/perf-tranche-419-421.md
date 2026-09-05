# Criteria, registry, and exact-lookup lifecycle measurements

Baseline: `2a8303d95acf2881c539e3700f7b8effc2e7bd35`. This tranche addresses #419, #420, and #421 without changing lookup matching rules or introducing a global predicate cache.

## Changes

- Criteria masks are lazily memoized per invocation/predicate/column, including unsupported results. Admission is limited to a 1 MiB conservative charge: array and backing buffers plus 512 bytes for Arc/buffer owners, allocation overhead and hash buckets. Over-budget masks retain the previous rebuild behavior; one current mask can exceed this additional-retention bound. Reduction order and mask padding stay unchanged. Unused text fallback lanes disappear; numeric fallback lanes are lazy.
- Ordinary registry hits use a read lock and clone only the current function Arc. Prefix misses retain write-locked alias insertion with a registration/ownership recheck. Normalization still allocates.
- Exact indexes and admission state are cleared at exclusive Engine snapshot mutation boundaries, covering data, topology and direct value edits. Concurrent admission checks the actual estimated size under the insertion lock, after duplicate detection. Text values, folded keys, hash capacity and spilled duplicate vectors are charged. Actual-size rejection is remembered until the next snapshot. Unrelated edits still invalidate conservatively; axis-local reuse is deferred.

## Method

The ignored `engine::tests::perf_tranche::release_probe` is a **cfg(test) release probe**, not a production executable or Criterion benchmark. It uses actual Engine Arrow storage and the Engine criteria/index paths, with an empty TestWorkbook as resolver. The thread-local allocation counter measures requested allocation/reallocation bytes, not peak/live memory. Parallel Engine allocation counts cover only the calling thread; the standalone registry probe sums its worker counters. Instrumentation overhead is present in both builds.

Rust 1.93.0, default eval features (`system-clock`), release optimization with repository `lto=true`, `codegen-units=1`; no profile/RUSTFLAGS overrides. Linux x86_64, Ryzen 9 3900XT, 12 cores/24 threads. These are shared-host observations, with unrelated rendering workloads and no CPU pinning. No benchmarks overlap our builds. Final order was baseline/fixed/fixed/baseline, seven samples per run. Warm criteria/Engine distributions exclude sample zero (12 samples); registry uses 14 samples. Lookup distributions use medians of late cycles 4-15 per run/sample/needle position, then summarize those 14 medians. IQRs below are descriptive, not confidence intervals. Cold/setup/edit-only criteria distributions have only two observations.

Fixtures are deterministic: criteria N=32768, 1/8/32 chunks, P=1/4, numeric alternating 0/1 or text alpha/beta, sums of 2, one/16 formulas. Tiny/sparse controls use eight rows, two populated criteria cells and four chunks. Registry uses 2048 independent nested ABS/ROUND formulas or arithmetic controls, one/four workers. Lookup uses 512 unique keys, numeric or 106-byte text, a 512000-byte cap, 24 MATCH/VLOOKUP/XLOOKUP formulas, 16 edit/recalc cycles and an additional no-edit dirty/recalc per cycle. Axis edits swap the queried key between first and last rows with different returns; unrelated edits touch F1. All output cells are checked outside timing.

Timing fixtures assert **zero active FormulaPlane spans**, including AuthoritativeExperimental-configured cases: these are configured-legacy measurements, not active-span speedups. Separate batch-ingested correctness coverage asserts three active spans (MATCH, VLOOKUP, nested ABS/ROUND), changed numeric/text answers and warm reuse. XLOOKUP retains its existing unsupported-canonical-template fallback.

## Results

Warm single-formula SUMIFS, P=4, microseconds, median [p25, p75]:

| Chunks | Numeric baseline | Numeric fixed | Text baseline | Text fixed |
| --- | ---: | ---: | ---: | ---: |
| 1 | 129 [129,130] | 127 [126,128] | 491 [487,494] | 490 [489,494] |
| 8 | 1040 [1025,1055] | 155 [155,160] | 3948 [3944,4176] | 525 [520,535] |
| 32 | 6248 [6180,6273] | 250 [249,258] | 18512 [18318,18589] | 624 [619,644] |

At 32 chunks/P=4, mask calls fall from 128 to 4; requested logical rows from 4194304 to 131072. Requested allocation bytes fall from 5761147 to 365019 (numeric) and 6316891 to 382479 (text). P=1 follows the same structural reduction, 32 to 1 calls. With 16 report formulas, calls fall 2048 to 64; numeric/text warm medians are 101.12/298.08 ms baseline versus 4.05/10.16 ms fixed. Setup remains approximately 3.0/3.6 ms. The edited-overlay recalc is 101.77/301.69 ms versus 4.19/10.35 ms (only two observations).

Tiny finite warm medians: 21.68 [21.45,24.16] to 10.83 [10.51,11.56] us; sparse whole-column: 23.00 [22.83,24.11] to 11.54 [11.44,12.88] us. Allocation bytes: 29256 to 13680 and 29841 to 14265. These are narrow four-chunk controls, not a general sparse-floor guarantee. Single-chunk dense text P=1 is essentially unchanged (137.59 to 139.00 us). `mask_logical_rows` counts requested view dimensions per mask call, not actual processed/allocated rows; the sparse fixture trims to eight rows. No separate physical-row counter or peak allocator measurement is claimed.

Registry, milliseconds, median [p25,p75]:

| Case | Baseline | Fixed |
| --- | ---: | ---: |
| Direct get, 1 worker, 200000 calls | 24.08 [23.95,24.36] | 12.48 [12.19,12.53] |
| Direct get, 4 workers, 800000 calls | 260.06 [249.91,268.11] | 104.87 [101.62,106.77] |
| Engine nested calls, Off, 1 worker | 4.44 [4.39,4.49] | 3.89 [3.88,4.01] |
| Engine nested calls, Off, 4 workers | 3.51 [3.07,3.57] | 2.40 [2.30,2.47] |
| Engine arithmetic, Off, 1 worker | 1.65 [1.61,1.68] | 1.61 [1.60,1.74] |
| Engine arithmetic, Off, 4 workers | 1.34 [1.31,1.43] | 1.31 [1.21,1.42] |

Direct get allocations fall from three to one per call (12 to 4 requested bytes/call averaged over ABS/ROUND). Structural tests show no resolution write path on registered/alias hits, including concurrent readers. Lock wait time is not separately profiled. Authoritative-configured legacy nested medians are 4.39 to 3.72 ms (one worker), 3.54 to 2.51 ms (four workers); do not extrapolate these to active-span execution or arbitrary providers.

Late unrelated-edit lookup recalc, Off, microseconds:

| Keys | Baseline median [p25,p75] | Fixed median [p25,p75] | Fixed no-edit warm |
| --- | ---: | ---: | ---: |
| Numeric | 1214 [1201,1228] | 195 [193,197] | 54 [53,54] |
| Text | 1936 [1911,1942] | 265 [264,266] | 58 [57,58] |

Baseline retains four obsolete entries/427008 estimated bytes, then reports zero builds/hits and 18 skipped-cap calls per edited evaluation (24 skipped on no-edit retries). Fixed retains two entries, 240128 numeric or 457216 text bytes: each edited evaluation has two builds/16 hits/six threshold skips/zero cap skips; no-edit evaluation has zero builds/24 hits. Changed-axis first/last medians are 191/226 us numeric and 266/321 us text; all changed answers pass. Cleanup intentionally moves work into edits: unrelated numeric edit median rises 2.00 to 6.35 us, text 2.05 to 35.72 us. Retained byte figures are estimated index charges, not total cache-map capacity or process RSS; index construction and externally held Arcs are outside admission accounting.

## Reproduction

Raw captures and compiled probe executables are local-only artifacts, not repository contents. This reviewable report, fixture source, baseline instrumentation patch, and summarizer provide the reproduction path below. Inspect and sanitize locally generated output before sharing it; do not commit binary archives or machine/environment dumps.

The baseline instrumentation patch SHA-256 is `a8fbf4f93b8e2edd29a54a6bf37a8a671f902e20aefcc5350de4693ac864a8b7`.

From the fixed checkout, build/copy each executable before rebuilding the other source. The emitted executable filename can vary with features/toolchain.

```bash
fix=$PWD
out=/tmp/formualizer-perf-reproduce
mkdir -p "$out"
export CARGO_TARGET_DIR="$out/target"
cargo +1.93.0 test -p formualizer-eval --lib --release --no-run -j 4
# Copy the executable path printed by Cargo to "$out/fixed-probe".
git worktree add --detach "$out/baseline-source" 2a8303d95acf2881c539e3700f7b8effc2e7bd35
git -C "$out/baseline-source" apply "$fix/benchmarks/perf-tranche-419-421-baseline.patch"
sed '/^\/\/ Regression tests/,$d' "$fix/crates/formualizer-eval/src/engine/tests/perf_tranche.rs" \
  > "$out/baseline-source/crates/formualizer-eval/src/engine/tests/perf_tranche.rs"
(cd "$out/baseline-source" && cargo +1.93.0 test -p formualizer-eval --lib --release --no-run -j 4)
# Copy the newly emitted executable to "$out/baseline-probe".
for run in baseline:1 fixed:1 fixed:2 baseline:2; do
  variant=${run%:*}; repeat=${run#*:}
  FZ_PERF_SAMPLES=7 "$out/$variant-probe" release_probe --ignored --nocapture --test-threads=1 \
    > "$out/paired-$variant-$repeat.txt" || exit 1
done
uv run python3 benchmarks/summarize-perf-tranche.py "$out"/paired-*.txt > "$out/summary.json"
```

Optional `FZ_PERF_CASE=criteria|registry|lookup` selects one family. The common measured-harness SHA-256 is `0589298ab9223a8b8842202d2b83ada610eb41038cff04c366f6a26d91f9fe0f` (source before the regression-test marker). Baseline instrumentation only adds cfg(test) mask counters and the probe module. Initial baseline measurements preceded production changes; final paired baselines rebuild that same base with the expanded common fixture.

## Correctness and gates

Eight new regressions cover mask scaling/laziness/bounded distinct masks, current registry handles/read-only hits, concurrent cap/duplicate races, payload accounting, all snapshot mutation boundaries and warm reuse, active-span moved-key parity, and one rejected oversized text build per snapshot. Existing criteria blank/null/error/wildcard/cross-sheet/padding/overlay, cancellation, lookup duplicate first/last/temporal/error/volatile, registry ownership/replacement/provider and workbook UDF suites remain unchanged.

`cargo +1.93.0 test -p formualizer-eval -p formualizer-workbook --tests -j 4 -- --test-threads=4`: 3057 passed, 16 ignored across 45 binaries (eval library: 2907 passed, 15 ignored). Format and `clippy` for these crates with `--tests -- -D warnings` pass. Full release eval library: 2906 passed, 15 ignored, one baseline-reproduced failure, `engine::arena::scalar::tests::test_scalar_arena_float_overflow` (expects a debug overflow panic absent in release). It passes in debug; no unrelated fix is included. No whole-workspace/optional-backend build or cross-platform claim is made.
