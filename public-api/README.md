# Public Rust API snapshots

This directory records the `cargo-public-api` view of Formualizer's published Rust surface. CI reports any drift; reviewers still decide whether a change is semver-compatible and intended.

## Scope

The snapshots cover the eight crates published on crates.io and named by the release tracks:

- `formualizer`
- `formualizer-common`
- `formualizer-parse`
- `formualizer-eval`
- `formualizer-workbook`
- `formualizer-sheetport`
- `sheetport-spec`
- `formualizer-macros`

The initial post-#233 baseline is 35,445 lines and 4,469,997 bytes. `formualizer-eval` accounts for 29,393 lines and 3,857,614 bytes. This unexpectedly broad existing reachable and macro-authored surface is recorded mechanically rather than treated as a line-by-line API approval. It is an input to the final pre-tag API review. Future changes still produce localized diffs.

The following contracts are deliberately checked elsewhere:

- CFFI uses its installed C header and exported symbols/ABI versions.
- Python uses generated `.pyi` files that CI regenerates and diffs.
- WASM/npm uses wasm-bindgen and TypeScript package output.
- Bench, testkit, and xtask crates are unpublished internal tooling.

The CFFI, Python, and WASM Cargo packages are deliberately non-publishable. Their contracts are validated through the C header/ABI, generated Python stubs and wheels, and wasm-bindgen/TypeScript/npm artifacts; they remain outside this eight-crate snapshot set and every crates.io release track.

## Native feature profiles

`scripts/public-api.sh` is the single source of truth for these explicit maximal native allowlists. It always disables default features first and never uses `--all-features`. Before generation, Python 3 compares every governed package's `cargo metadata --no-deps` feature keys with the enabled, excluded, alias, and default classifications below. A new unclassified feature fails with instructions instead of silently changing or escaping the profile.

| Crate | Enabled native features | Rationale and exclusions |
| --- | --- | --- |
| `formualizer` | `common`, `parse`, `eval`, `workbook`, `sheetport`, `calamine`, `json`, `csv`, `umya`, `tracing`, `tracing_chrome`, `system-clock` | Includes every facade API gate and native I/O/diagnostic surface. Excludes the browser-only `js-runtime` and `wasm-js` profiles; `portable-wasm` is a redundant alias for gates already enabled. |
| `formualizer-common` | `serde` | Includes the only optional native integration. |
| `formualizer-parse` | `serde` | Includes serialized AST support. |
| `formualizer-eval` | `system-clock`, `tracing`, `tracing_chrome`, `perf_instrumentation`, `formula_plane_diagnostics` | Includes native clock and supported diagnostics. Excludes browser-only `js-runtime`, private `benchmark_internal`, and private test gate `test-support`. |
| `formualizer-workbook` | `system-clock`, `json`, `csv`, `calamine`, `umya`, `mmap`, `io_builtins`, `import_range`, `webservice`, `tracing`, `perf_instrumentation`, `compression`, `calamine_integration`, `umya_integration`, `wasm_plugins`, `wasm_runtime_wasmtime` | Includes all native loaders, I/O builtins, diagnostics, compression, and the native Wasmtime plugin runtime. Excludes browser-only `js-runtime` and private `benchmark_internal`. |
| `formualizer-sheetport` | `system-clock`, `umya` | Includes native clock and workbook adapter APIs. Excludes browser-only `js-runtime` and private `benchmark_internal`. |
| `sheetport-spec` | none | The crate has no Cargo features. |
| `formualizer-macros` | none | The proc-macro crate has no Cargo features. |

## Deterministic generation

Canonical generation is supported on Linux (`x86_64-unknown-linux-gnu`), including the repository devcontainer and the `ubuntu-24.04` CI runner. It requires rustup, Python 3 for feature-policy validation, and standard Linux utilities (`diff`, `flock`, `realpath`, and `mktemp`). Run review generation in that environment rather than treating output from macOS or Windows as canonical.

Generation pins these inputs and neutralizes host configuration:

- exact `cargo-public-api =0.52.0`, installed with Rust `1.93.0` under versioned `target/public-api-tools/` rather than replacing a global Cargo plugin
- rustdoc `nightly-2026-02-16` (`rustc 1.95.0-nightly`)
- target `x86_64-unknown-linux-gnu`
- `LC_ALL=C`, `TZ=UTC`, and `CARGO_TERM_COLOR=never`
- unset plain and encoded rust/rustdoc flags, inherited `CARGO`, compiler overrides, wrappers, target linker/runner overrides, and their Cargo build equivalents
- isolated `CARGO_TARGET_DIR=target/public-api` and `CARGO_HOME=target/public-api-cargo-home`

Snapshots omit cargo-public-api's blanket implementations and compiler auto-trait implementations. Omitting auto-trait implementations means these files do not detect changes to inferred `Send`, `Sync`, or similar auto traits; reviewers must assess those separately. Derive-generated and explicit implementations remain because traits such as `Clone`, `Eq`, and `Default` are downstream commitments even though retaining them makes the baseline larger.

## Update and review workflow

Install or verify the exact tools once:

```bash
scripts/public-api.sh setup
```

Check all snapshots using the same command as CI:

```bash
scripts/public-api.sh check
```

A crate selection is useful while iterating:

```bash
scripts/public-api.sh check formualizer-eval
```

Checks take a shared cooperating-process lock and updates take an exclusive lock under the preflighted, non-symlink `target/public-api-lock/` path. The lock is held from before generation through comparison or publication and rollback, so cooperating readers and writers never observe or produce an interleaved baseline.

When an API change is intentional, generation completes for every requested crate before publication. Candidates are then staged on the snapshot filesystem and installed as a rollback-protected multi-file transaction, so a detected command failure or handled signal restores the entire selected baseline:

```bash
scripts/public-api.sh update
# or: scripts/public-api.sh update formualizer-eval
scripts/public-api.sh check
```

Review the unified diff. Confirm that the native feature profile still represents the release surface, classify additions/removals/changes for semver impact, and commit the reviewed snapshots with the source change. Do not update snapshots merely to make CI green.

The rollback guarantee covers cooperating script processes and detected failures. Hostile non-cooperating filesystem mutation, `SIGKILL`, process crashes that bypass traps, and power loss are outside its scope. If a detected rollback cannot complete safely, the script retains the recoverable transaction backup directory and prints its path rather than deleting evidence or following an unsafe path.

`scripts/tests/public-api-concurrency.sh` deterministically pauses one update after its first file install, starts a second update with a distinct two-crate candidate set, and proves the second process waits and ultimately publishes one complete set rather than an interleaved baseline.
