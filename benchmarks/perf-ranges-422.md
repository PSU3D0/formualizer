# Bounded range discovery and numeric/error projection (#422 subset)

Baseline: `8a912b50a086a8cdb0001ea10b6468073aa446ca` (after #431). A bounds chunk discovery with a private metadata cursor; B projects only numbers/errors instead of constructing all generic lanes and searching for the same chunk again per column. The public chunk shape, row/column order, physical overlap, cancellation checkpoints, and existing overlay cascade remain unchanged. Production logic changes only in `range_view.rs`.

This is a subset of #422, not a general range/workbook throughput claim. Other typed lanes still use the generic adapter. Missing chunks inside the view remain represented; requested-lane null caches may still allocate whole-chunk buffers. No lookup semantics, used-extent policy, default FormulaPlane mode, or global cache changes are included. #414's approximate-lookup scalar materialization remains open and unchanged.

## Method

Shared Linux x86_64 host, AMD Ryzen 9 3900XT (12 cores). Rust 1.93.0, native release, repository `lto=true`/`codegen-units=1`, empty rustflags. The normal example uses default eval features plus `test-support`, **library cfg(test) OFF**, and no test allocator/counter hooks. Engine fixtures use TestWorkbook, one worker, FormulaPlane Off, and Arrow/delta/computed overlays; outputs are on a separate sheet. This is not interactive/ephemeral workbook-mode or active-span performance coverage.

The exclusive window was 2026-09-05 14:50:02Z-14:50:41Z. No program builds or other tests overlapped it. Instrumented order: baseline/A/AB/AB/A/baseline; normal-example order: baseline/fixed/fixed/baseline. Each process uses seven samples. These are short shared-host observations, without CPU pinning; external activity, allocator/cache state and process-global initialization remain possible noise. Quartiles describe dispersion, not confidence intervals.

Direct samples build a fresh fixture, validate it through scalar reads, then measure one cold lane read and a warm batch. Construction, ingestion, range creation and scalar validation are excluded. Cold means cold lane providers, not cold CPU/process/OS state. Warm batches use 128 operations for tiny ranges, eight for dense ranges and four for sparse spans. Each reported direct distribution has **14 fixture batches from two process runs**, not 14 process launches or hundreds of independent timings.

Direct `Sum` is **SUM-like**, counting non-null errors before Arrow numeric summation, not implementing builtin SUM error propagation. Quoted numeric/sparse fixtures contain no errors. `Numbers` reads numeric slices and sums them; `Count` reads numeric null counts rather than scanning every value. Actual Engine SUM/COUNT is separate below. All results are checked outside timing; normal captures emit checksums, while instrumented captures assert answers but do not emit a checksum field.

## Counter-free results

Warm direct microseconds per operation, median [p25,p75], n=14 per variant. Default chunks contain 32768 rows; fragmented layouts are explicit stress controls.

| Fixture | Baseline | A+B |
| --- | ---: | ---: |
| Eight-row numeric head, one default chunk | 0.426 [0.424,0.433] | 0.088 [0.087,0.088] |
| Eight-row numeric tail, 32 default chunks | 1.613 [1.603,1.673] | 1.188 [1.151,1.224] |
| Eight-row numeric tail, 4096 x 256-row chunks | 5.186 [5.163,5.260] | 1.122 [1.062,1.151] |
| Dense 32768 x 8, one default chunk, Sum-like | 68.030 [66.934,68.785] | 51.445 [51.257,52.275] |
| Dense 32768 x 8, 32 x 1024-row chunks, Sum-like | 270.096 [266.855,272.079] | 89.705 [88.688,91.292] |
| Dense 32768 x 8, one default chunk, Count | 9.390 [9.321,10.072] | 0.480 [0.465,0.485] |
| Million-row sparse gap span, one column, 4096 chunks, Sum-like | 12054 [11986,12149] | 1469 [1409,1865] |

The sparse fixture has 1,048,576 rows, populated top/tail and missing intervening column chunks. **Both variants still yield 8192 segments and 2097152 presented row-segment rows across the two passes.** Its improvement is lane/null-array/owner work, not occupied-cell-only traversal. It excludes whole-column resolution and must not be described as an Engine `SUM(A:A)` result. Fixed p90 is 1984 us and per-run medians are 1509/1414 us, showing appreciable dispersion.

For the default 32-chunk numeric tail, cold medians are 22.843 [22.262,31.342] us baseline versus 7.108 [6.836,9.741] us fixed; p90 is 89.615/16.153 us. Do not generalize these dispersed cold observations.

Actual Engine **fresh first evaluation**, microseconds, n=14 fresh fixtures per variant:

| SUM+COUNT source layout | Baseline median [p25,p75] | A+B median [p25,p75] | Baseline / A+B p90 |
| --- | ---: | ---: | ---: |
| 32768 rows, one default chunk | 19.106 [17.761,21.906] | 15.425 [14.487,16.514] | 62.586 / 56.201 |
| 32768 rows, 128 x 256-row chunks | 16.251 [15.870,18.139] | 13.515 [13.247,13.960] | 19.569 / 17.542 |

The formulas read `A32761:A32768`; SUM=262116 and COUNT=8, with two computed vertices and zero active spans. Global registry initialization can affect the first Engine sample in each process. These are **not** the instrumented warm-dirty phase: that probe explicitly dirties formulas on the same Engine outside timing, while the public example rebuilds the Engine for each sample. Neither phase is no-op recalculation.

## Mechanism and trade-off

The instrumented probe is cfg(test), with thread-local work/requested-allocation counters; its timing ratios are not production-style ratios. A-only captures separate discovery from projection:

- Tiny tail candidates change 32/4096 -> 1, plus 12/26 search predicates, with one eight-row segment unchanged. At default chunk counts A alone is a small effect; projection supplies much of the improvement.
- Dense 32-chunk, eight-column Sum-like traversal: A retains baseline's 512 generic columns, 512 numeric/error re-searches and 3200 requested allocations. A+B removes generic/re-search work and records 576 allocations; requested bytes change 397312 -> 61440. Presented shape is unchanged.
- Sparse one-column span: fresh missing-lane arrays change 32752 -> 8188; requested bytes change 42625376 -> 11606384. Gaps and segment counts remain unchanged. These are cumulative allocation requests, not live/peak memory; presented rows are not exact Arrow kernel row visits.
- Instrumented exact VLOOKUP first/last/miss controls use 64-row axes. Zero cache cap forces typed fallback; warmed normal-cap queries assert cache hits and zero range segments. Fragmented warm-dirty medians (n=12) are 9.258 -> 7.319 us for fallback and 6.808 -> 6.823 us for indexed queries. The latter is a negative control, not an indexed-lookup improvement.

There is an accepted small regression: generic head-only `First` now computes both boundaries before yielding. Counter-free warm microseconds, n=14:

| Generic First | Baseline [p25,p75] | A+B [p25,p75] |
| --- | ---: | ---: |
| One default chunk | 0.365 [0.362,0.369] | 0.374 [0.372,0.375] |
| 32 default chunks, head | 0.365 [0.363,0.389] | 0.383 [0.381,0.388] |
| 4096 fragmented chunks, head | 0.365 [0.362,0.378] | 0.399 [0.398,0.400] |

The fragmented pooled increase is about 34 ns/9%, with the direction repeated in both runs. The default 32-chunk distributions overlap and its run medians reverse direction, so that percentage is not a stable effect. No first-chunk shortcut was added after measurement; not every generic reader improves.

Coverage is smaller than the investigation plan. There is no timed Engine A:A/used-bounds resolution, criteria/SUMIFS, MEDIAN, approximate lookup, edit-triggered recalculation, width-64 sweep, parallel scaling or active-FormulaPlane case. Mixed/sparse/wide and overlay instrumented controls are not all repeated in the normal example. The seven new independent segmentation/projection, sparse/structural/lifetime, cancellation, overlay, lane-locality and bit/error-order regressions pass in debug and release. Recorded full validation: debug eval/workbook 3065 passed, 17 ignored; release eval 2914 passed, 16 ignored, excluding only the separately baseline-reproduced `test_scalar_arena_float_overflow` release-only missing-panic failure. Targeted Clippy and formatting pass. These are recorded implementation checks, not a new CI claim.

## Source identity

The measured candidate was uncommitted; source hashes pin it instead of a fictitious fixed commit. SHA-256:

- `crates/formualizer-eval/src/engine/range_view.rs`: `ac2f72a1be9a07c91aeb36a780a5ac12632ad0038b227f213e07fa4ee5261761`.
- Instrumented `crates/formualizer-eval/src/engine/tests/perf_ranges.rs`: `37324a4206555f5d1dc1ff16be44bcfda51991d9d8cbeb6bc2efb1bce8272c66`.
- Counter-free `crates/formualizer-eval/examples/range_projection_probe.rs`: `cde1bbeee4d11bf6fd8e03d2d12b00367bbe963b154ac57cc7cd1abca707df07`.
- `benchmarks/perf-ranges-422-baseline.patch`: `148bcbef2267bcb7fd6afe2bb6342dc5c9ba763dfd31d1948b679a9efaaaae93`.
- `benchmarks/perf-ranges-422-a-only.patch`: `a3e2013cc6ecc6664904fd0e17bed34831677bbb66270b727122c2b2919401f7`.

Exact exclusions: baseline/A instrumented builds lack the appended `bounded_projection_tests` module; the measured A+B binary includes all seven tests. A pre-test A+B source snapshot is not the full source used to build that binary. Counter-free builds exclude all cfg(test) modules, including those tests, provider counters and the allocator probe. This report, minimal reproduction patches and summarizer were added after measurement; the production/harness sources were not changed. The public baseline patch omits the duplicate common probe body: copy the hash-identified source as instructed below.

## Reproduction

Build first, then measure without overlapping builds. Raw captures and binaries stay local; inspect any text before sharing it. From the candidate checkout, this creates a fresh baseline with only the example/stanza and uses **distinct target directories**; never trust cross-worktree artifact reuse based on source mtimes.

```bash
set -eu
fixed=$PWD
out=$(mktemp -d)
base="$out/program-ranges-baseline"
git worktree add --detach "$base" 8a912b50a086a8cdb0001ea10b6468073aa446ca
cp crates/formualizer-eval/examples/range_projection_probe.rs "$base/crates/formualizer-eval/examples/"
cat >> "$base/crates/formualizer-eval/Cargo.toml" <<'TOML'

[[example]]
name = "range_projection_probe"
path = "examples/range_projection_probe.rs"
required-features = ["test-support"]
TOML
(cd "$base" && CARGO_TARGET_DIR="$out/target-baseline" cargo +1.93.0 build \
  -p formualizer-eval --example range_projection_probe --features test-support --release -j4)
CARGO_TARGET_DIR="$out/target-fixed" cargo +1.93.0 build \
  -p formualizer-eval --example range_projection_probe --features test-support --release -j4
cp "$out/target-baseline/release/examples/range_projection_probe" "$out/production-baseline"
cp "$out/target-fixed/release/examples/range_projection_probe" "$out/production-fixed"
"$out/production-baseline" --validate
"$out/production-fixed" --validate
# Run only in a quiet measurement window, after all builds finish.
for run in baseline:1 fixed:1 fixed:2 baseline:2; do
  variant=${run%:*}; repeat=${run#*:}
  FZ_RANGES_SAMPLES=7 "$out/production-$variant" > "$out/production-$variant-$repeat.txt" || exit 1
done
uv run --no-project python3 benchmarks/summarize-perf-ranges.py "$out"/production-*.txt
```

For mechanism reproduction, apply `perf-ranges-422-baseline.patch` to that baseline and copy `crates/formualizer-eval/src/engine/tests/perf_ranges.rs` from the candidate into its matching path. The patch adds only test counters and the child-module declaration, reusing the existing allocator. Build/copy its eval lib test executable with `cargo +1.93.0 test -p formualizer-eval --lib --release --no-run -j4`. Then apply `perf-ranges-422-a-only.patch` on top for **A only** (bounded discovery, not projection), build/copy into a distinct A target. Build/copy the candidate for A+B in another target. The A-only patch intentionally changes the experimental variant's cursor; it is not an instrumentation-only patch or a second proposed production fix.

Validate each executable with `range_probe_fixtures_validate --test-threads=1`. After all builds finish, use `FZ_RANGES_SAMPLES=7 <executable> range_release_probe --ignored --nocapture --test-threads=1` in baseline/A/AB/AB/A/baseline order, saving names `instrumented-baseline-1.txt`, `instrumented-a-1.txt`, `instrumented-ab-1.txt`, etc. The summarizer accepts those files and `--work` emits per-iteration median allocation/work counts. It handles the libtest-prefixed first record. Neither patches nor report contain raw captures, binaries, archives or environment dumps.
