# Legacy schedule sharing (#427)

This bounded change replaces nested copies of the existing single-entry legacy schedule cache with immutable shared handles. It does not change cache eligibility, candidate/topology keys, scheduling algorithms, FormulaPlane defaults, or target routing. It adds no cache entry or persistent index.

Baseline is public commit `8f7c7338ee0b2bdecbcf3e681cc1a92a7236dc14`, including the separate #432 logged/replay invalidation correction. Both measured sides contain that exact correction. The measured candidate source was subsequently committed unchanged as `4eee5aa309aa6c797aa70312c27290e24c1c3c9c`; its hashes are below. This report, baseline observation patch, and summarizer were prepared afterward. Measurements identify that exact pair, not a later combined-main build containing other integrations.

## Mechanism and retained storage

`CachedScheduleEntry` holds an `Arc<Schedule>`. A private owned/shared wrapper leaves ineligible schedules inline, avoiding a shared allocation on those fallbacks. Cache hits share the same immutable payload; insertion shares it with the current request. The public `Schedule` and compatibility-plan representation are unchanged.

Before insertion, outer/nested vectors are compacted with `shrink_to_fit`. Baseline deep cloning implicitly discarded builder spare capacity; retaining that capacity directly would increase residency. Compaction preserves the measured compact payload while adding the shared allocation/header. It has miss-path costs, and Rust does not universally guarantee exact capacity after shrinking.

Separate **untimed** mechanism observations, four same-branch edits:

| Chain depth | Baseline/fixed deep-clone events | Baseline modeled copied buffers | Baseline modeled copied bytes | Fixed modeled copied buffers/bytes | Retained baseline/fixed bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 4 / 0 | 12 | 144 | 0 / 0 | 144 / 168 |
| 32 | 4 / 0 | 136 | 4,608 | 0 / 0 | 1,384 / 1,408 |
| 256 | 4 / 0 | 1,032 | 36,864 | 0 / 0 | 10,344 / 10,368 |

Both sides already have four cache hits and zero schedule builds here. Fixed records four shared-handle events. Source inspection and pointer/lifetime tests establish sharing and release; the byte/buffer figures model populated vectors at the removed clone call sites, not allocator-profile measurements. Retained estimates include entry/candidate/vector capacity and shared header/control storage, but exclude allocator metadata, transient peaks, and wrapper stack storage. All 21 mechanism pairs differ by +24 retained bytes on this native build after compaction. No RSS or peak/live allocation claim is made.

Alternating candidate vectors still miss/build every request. Dynamic/compressed-range candidates remain excluded. Ordinary targets and retained target recipes still rebuild legacy demand closure and target schedules. At depth256, four late-target requests on either side visit 1,040 demand vertices, including 1,024 clean formulas, rebuild four schedules, and compute eight formulas. A clean target still walks that ancestry with zero computed formulas. This change does not implement target-cache or closure reuse.

## Method and controls

The deterministic fixture is `crates/formualizer-eval/examples/recalc_reuse.rs`. It uses a persistent Engine per process sample, an empty TestWorkbook resolver, explicit FormulaPlane Off, and serial execution. It is a regular release example with `test-support` only, not a cfg(test) test executable. `benchmark_internal` and virtual telemetry are disabled for timing. The example refuses `--time` when mechanism instrumentation is enabled.

Rust 1.93.0 (`254b59607`, LLVM 21.1.8), Linux x86_64 on an AMD Ryzen 9 3900XT 12-Core Processor, default `system-clock` feature, repository release profile (`lto=true`, `codegen-units=1`), no RUSTFLAGS/profile overrides. Builds use `-j4` and separate targets and finish before capture. Known build/test activity was parked during the exclusive window. Affinity allowed 24 CPUs; no CPU pinning or governor control was used. Unrelated host activity cannot be excluded. Serial/test-support observations do not establish default-parallel, binding, file-I/O, or active-FormulaPlane performance.

The 21 cases are seven scenarios at depths 1, 32, and 256:

| Scenario | Work |
| --- | --- |
| `cold` | First full evaluation after construction; one request. |
| `same` | Repeated value edits at one additive chain's input, then full recalculation. |
| `alternating` | Alternate edits between two disjoint chains, then full recalculation. |
| `late-target` | Edit a side input to a two-formula tail while its long ancestry stays clean; ordinary typed target request. |
| `late-plan` | Same late edit through a retained target plan. |
| `clean-target` | Repeated ordinary target requests with no edit. |
| `noop` | Repeated clean full evaluations. |

Chains use addition, not exponential doubling. The late fixture adds `D1=B[last]+C1`, `E1=D1+1`, targeting E1. Warm input values alternate to avoid accidental no-op edits. Construction, first warm evaluation, retained-plan construction, result inspection and stdout are outside warm timing. Each timed loop includes edits/propagation, evaluation/effects and computed-count accumulation. Cold measures its first evaluation only; clean/no-op cases have no edit. There is no separately timed preparation, edit, scheduling, compute or effects breakdown.

Capture order was **baseline/fixed/fixed/baseline**, seven fresh-process samples per case per block. Warm samples contain 1,024 requests each; cold contains one. There are **14 process-level batch samples per side/case**, not 1,024 independent observations per sample. Total: 588 completed samples, 516,180 requests (about 0.52 million: 516,096 warm plus 84 cold), and 16,924,572 reported computed vertices. All completed samples are retained; no outlier deletion.

Every sample's exit, printed value and computed count is checked against the fixture oracle. For `alternating`, the printed result/checksum is **only the first branch's terminal** plus the overall computed count. The capture did not observe a second-branch timed checksum. Separate unit tests check the terminal of each edited branch across alternating edits. No full-workbook checksum is claimed.

Timed processes expose no cache counters. Expected cache/path behavior is established separately by the frozen source, ordinary-edit regressions, and 21 paired untimed mechanism controls, not inferred from timing alone.

### Window and interruption

The exclusive window ran from **2026-09-05 15:52:52.833288 UTC** to **16:16:17.596451 UTC**. The controller's 1,200-second invocation limit interrupted capture after 577 complete samples: final baseline block, clean-target depth256, sample3. No timing child remained alive. The same exclusive window continued with the remaining 11 samples: four clean-target and seven no-op samples at depth256. A possibly interrupted non-output attempt was retried and is disclosed, not represented as a completed sample. The continuation began at 16:14:57.922129 UTC.

The main results retain all seven samples in every block. The temporal gap particularly affects the final clean-target/no-op controls. Using only the three pre-continuation observations for that final clean-target block changes its balanced delta from -2.95% to -3.31%; this is a sensitivity check, not an exclusion used below.

## Counter-free results

For each sample, divide loop duration by actual request count: a **batch mean**, not an individual-request latency. Take the median of seven samples within each block, average the two baseline block medians and the two fixed block medians, then report `fixed / baseline - 1`. Negative means lower observed fixed elapsed time. ABBA balances ordering but cannot guarantee removal of drift. There are only two condition blocks per side in one machine/window; no confidence interval, latency percentile or formal significance claim is made.

| Scenario | Depth | Baseline us/request | Fixed us/request | Change |
| --- | ---: | ---: | ---: | ---: |
| cold | 1 | 46.373 | 42.996 | -7.28% |
| same | 1 | 3.294 | 3.259 | -1.07% |
| alternating | 1 | 3.882 | 3.941 | +1.53% |
| late-target | 1 | 241.501 | 232.093 | -3.90% |
| late-plan | 1 | 5.159 | 5.177 | +0.36% |
| clean-target | 1 | 233.817 | 224.712 | -3.89% |
| noop | 1 | 0.736 | 0.716 | -2.68% |
| cold | 32 | 112.037 | 105.921 | -5.46% |
| same | 32 | 23.900 | 22.926 | -4.07% |
| alternating | 32 | 29.777 | 28.796 | -3.29% |
| late-target | 32 | 2569.474 | 2520.418 | -1.91% |
| late-plan | 32 | 7.663 | 7.651 | -0.16% |
| clean-target | 32 | 2556.748 | 2524.906 | -1.25% |
| noop | 32 | 0.731 | 0.718 | -1.70% |
| cold | 256 | 635.903 | 609.228 | -4.19% |
| same | 256 | 161.275 | 150.074 | -6.95% |
| alternating | 256 | 202.698 | 190.117 | -6.21% |
| late-target | 256 | 19468.076 | 18958.429 | -2.62% |
| late-plan | 256 | 13.403 | 13.106 | -2.22% |
| clean-target | 256 | 19503.632 | 18929.065 | -2.95% |
| noop | 256 | 0.735 | 0.718 | -2.31% |

**Scoped interpretation:** same-chain depths32/256 have lower observed edit+evaluation medians (-4.07%/-6.95%), with both block-pair comparisons pointing down (-3.44%/-4.70% and -5.46%/-8.40%, respectively). Pooled 14-sample IQRs are 23.709-24.119 versus 22.662-23.039 us at depth32, and 159.503-163.026 versus 148.949-151.291 at depth256. This is an observed synthetic end-to-end difference, not just a clone counter reduction.

**Attribution limits:** unchanged no-op controls shift by -1.70% to -2.68%, and ordinary target controls also shift. The window cannot uniquely attribute the full 4-7% same-chain difference to removed copies rather than code layout, allocation layout or environment effects. Do not subtract a selected control to manufacture attribution, or generalize to arbitrary workbooks.

**Regressions/noise:** tiny alternating work is +1.53% slower, with both pair signs positive (+1.43%/+1.63%) and overlapping pooled IQRs; disclose this small-work signal without asserting significance. Same-chain depth1 has opposite pair signs (+0.43%/-2.51%) and no stable measurable gain. Late-plan depths1/32 likewise show no meaningful improvement. Tiny cold timing is one call with strong dispersion: baseline IQR 38.157-65.504 us versus fixed 36.760-48.083, and pair changes +21.34%/-27.13%. Its aggregate does not establish a reliable cold improvement. Larger cold/alternating observations include both insertion-copy removal and compaction costs; neither is separately timed.

**Target follow-up, not a result of this change:** ordinary late/clean targets remain around 19 ms/request at depth256 versus roughly 13 us for late edits through a retained recipe. This is a lead for investigating ordinary preparation/setup versus retained-recipe execution. It is not a measured phase breakdown, and sharing the full-recalc cache does not solve that target work. No target cache or closure index is included.

## Correctness and review

On the common corrected source, both sides pass all 23 #432 regressions and the original logged/commit/undo/redo example against independently invalidated and freshly constructed oracles. Logged/commit/redo produce B1=12/C1=11; undo produces B1=2/C1=3, each with a schedule miss rather than stale reuse. This correction is shared baseline, not an optimization result.

Fixed full debug eval library: **2,936 passed, 15 ignored**, serial test execution. Targeted fixed coverage includes seven schedule-cache tests and the retained-plan/numerical/dynamic/mode filter (19 tests). Ownership tests cover pointer identity, lifetime after invalidation, eventual release, cyclic payload results, and unchanged dynamic/range exclusion. The sparse-target test checks clean ancestry visits, retained recipe behavior and unrelated dirty ownership. Stable-registry probes use exact-filter subprocesses rather than suppressing semantic invalidation.

Clippy with warnings denied passed before the shared correction and was re-run successfully on the exact common-corrected source at `4eee5aa3` with `--all-targets --features test-support,benchmark_internal`. Independent review found no A-only blocker and independently reconciled the 588 sample controls, source/binary identity and reported deltas. No whole-workspace, cross-platform, full release test-suite, default-parallel or allocator-profile claim is made. Nonblocking future test/harness hardening includes guarding successful zero-test subprocess execution after renames and checking both alternating terminals in a future capture; neither was changed after measurement here.

## Source identity

Inspected SHA256 identifiers, not raw manifests:

| Artifact | SHA256 |
| --- | --- |
| Common fixture `examples/recalc_reuse.rs` | `068c69693325517250edb2d09d0889ef6bf04894cb93e993c429e6a5f228f397` |
| Corrected baseline `engine/eval.rs` with observation patch | `e904a446f20aaa7cd522465a3e79de06df3725773df36d85c2b8c492e4484c4f` |
| Candidate `engine/eval.rs` | `0217665b89ec40d4e60f9d19855f587b695fc0f0d2ba063db485b99bd778461d` |
| Common `crates/formualizer-eval/Cargo.toml` | `298c61eb7f9e7dcf2899e3bfbfa9f646bb0b0c127b756cbec887e9593491005a` |
| Baseline observation patch | `e546da3cef1c51592d23f447445a8b9c56bbbbb436206f5bd11cbf1a06fc433c` |
| Captured baseline counter-free executable | `78b075ac1df53cdbbe60c802f0746302883dc1ddf8e4c688ce2cfd1122047ea0` |
| Captured candidate counter-free executable | `6f570fa811e82a6cda53b03de7b8b76b7182b07c36d215c080b8a1288b1f9738` |

The minimal baseline patch adds only cfg-gated observations and two example declarations; copy the common example sources as below. It reproduces the measured baseline eval/manifest bytes directly from public 8f. It does not duplicate #432 or change baseline scheduling. Extra exploratory baseline unit-test additions are not needed to reproduce these release examples. Executable hashes identify the capture; cross-machine/build-directory binary reproducibility is not promised.

## Reproduction

All raw outputs, full manifests, executables and machine/environment dumps remain local-only. Do not commit generated captures or binary archives. The reproduction checks out the measured source commit rather than using a potentially newer integration head. No timing executable was rebuilt or retimed during publication preparation; Clippy separately rechecked the source.

Build everything before measuring, with no RUSTFLAGS/profile overrides. Separate baseline/fixed targets prevent source/cache confusion:

```bash
assets=$PWD
out=$(mktemp -d)
fixed=$out/fixed
git worktree add --detach "$fixed" 4eee5aa309aa6c797aa70312c27290e24c1c3c9c
git worktree add --detach "$out/baseline" 8f7c7338ee0b2bdecbcf3e681cc1a92a7236dc14
git -C "$out/baseline" apply "$assets/benchmarks/perf-recalc-427-baseline.patch"
cp "$fixed/crates/formualizer-eval/examples/recalc_reuse.rs" \
   "$fixed/crates/formualizer-eval/examples/recalc_invalidation.rs" \
   "$out/baseline/crates/formualizer-eval/examples/"
for side in baseline fixed; do
  source=$fixed
  if [ "$side" = baseline ]; then source=$out/baseline; fi
  for kind in mechanism timing; do
    features=test-support
    if [ "$kind" = mechanism ]; then features=test-support,benchmark_internal; fi
    (cd "$source" && RUSTC_WRAPPER= CARGO_TARGET_DIR="$out/target-$side" \
      cargo +1.93.0 build --locked --release -j4 -p formualizer-eval \
      --features "$features" --example recalc_reuse) || exit 1
    cp "$out/target-$side/release/examples/recalc_reuse" "$out/$side-$kind"
  done
done
sha256sum "$out/"*-timing "$out/"*-mechanism > "$out/executable-sha256.txt"
# Untimed mechanism controls, separate from timing:
for side in baseline fixed; do
  for depth in 1 32 256; do
    "$out/$side-mechanism" all "$depth" 4 || exit 1
  done > "$out/$side-mechanism.txt"
done
```

Only after an exclusive timing window is available, run the balanced capture. Allow more than 20 minutes on comparable hardware; ordinary-target cases with 1,024 requests can take about 20 seconds per sample. The example's `cold` scenario reports one actual iteration. These logs remain local; retain stderr/exit failures rather than silently selecting successful samples.

```bash
date -u +%FT%TZ > "$out/window-start.txt"
for block in 1:baseline 2:fixed 3:fixed 4:baseline; do
  run=${block%:*}; side=${block#*:}
  log=$out/run-$run-$side.txt
  for depth in 1 32 256; do
    for scenario in cold same alternating late-target late-plan clean-target noop; do
      for sample in 1 2 3 4 5 6 7; do
        printf '%s\n' "run=$run sample=$sample $out/$side-timing $scenario $depth 1024 --time" \
          >> "$out/argv.txt"
        "$out/$side-timing" "$scenario" "$depth" 1024 --time \
          >> "$log" 2>> "$out/stderr.txt" || exit 1
      done
    done
  done
done
date -u +%FT%TZ > "$out/window-end.txt"
uv run --no-project python3 benchmarks/summarize-perf-recalc.py \
  "$out/run-1-baseline.txt" "$out/run-2-fixed.txt" \
  "$out/run-3-fixed.txt" "$out/run-4-baseline.txt" > "$out/summary.json"
```

The standard-library summarizer runs no benchmark. It validates run order/names, seven samples per case, actual iteration/computed counts and reported terminal values, then emits block medians, pooled median/IQR/min/max and balanced/pair deltas. It does not turn 1,024 correlated requests into independent samples. Its output was checked against all 588 captured stdout rows and independently reviewed results; malformed/incomplete/control-failing inputs are rejected. Neither the summarizer nor these reproduction commands were part of the measured executable.
