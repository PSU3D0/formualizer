# `formualizer recalc`

Recompute every supported formula's cached value after editing an `.xlsx`, preserving source workbook structures. This command uses the strict cache-only XLSX path, with no Umya, LibreOffice or alternative-engine fallback. Formula results such as `#DIV/0!` are successful calculations, not command failures.

## Installation

Distribution channels are **planned**, not yet published:

```sh
uvx formualizer recalc book.xlsx                 # Python native wheel
pip install formualizer                        # then: formualizer recalc book.xlsx
python -m formualizer recalc book.xlsx          # same Python native CLI
npx formualizer-cli recalc book.xlsx             # npm native launcher
cargo install formualizer-cli                   # Rust crate
cargo binstall formualizer-cli                  # planned release binaries
```

Planned native platforms: Linux x64/arm64 (GNU and musl), macOS x64/arm64, Windows x64. npm will use separate `@formualizer/cli-<platform>` optional binary packages, with no WASM fallback. Availability of these channels awaits release authorization.

For a source checkout today:

```sh
cargo run --release -p formualizer-cli -- recalc book.xlsx
```

## Commands

```text
formualizer recalc <INPUT> [-o|--output <PATH>] [--check] [--json] [--max-errors <N>]
formualizer --version
formualizer help [recalc]
```

- The default writes **in place**, atomically. A true no-op leaves bytes and mtime untouched and reports `unchanged`.
- `-o PATH` explicitly publishes that output, even if the input caches are already current (`written`, with zero changes). The input is untouched unless PATH is the input itself. Existing destination permissions are preserved. Symlink destinations are refused.
- `--check` computes only; it never creates or changes a destination, even with `-o`. Current caches exit 0; stale caches exit 3. This includes spill-shape or attribute changes, not just cache-value changes.
- `--max-errors N` limits listed error-cell locations across all error kinds (default 20, 0 lists none). Total error counts are unaffected.
- Ctrl-C/SIGINT cooperatively cancels at the next library checkpoint (exit 130). Refusal, failure or cancellation observed before atomic publication leaves the destination untouched. Publication is not compare-and-swap against unrelated writers.

The library reads a bounded snapshot, writes a same-directory temporary file, preserves existing destination permissions, fsyncs the temporary file and renames it atomically. It does not guarantee directory-entry crash durability. See [cache-only XLSX](cache-only-xlsx.md) for eligibility, dynamic arrays, ownership and bounds. Defaults include 64 MiB input/output, 10,000 ZIP entries, 256 MiB expanded bytes, 128 MiB per worksheet/metadata part, 100,000 formulas, XML depth 128, 256 columns and 8,000,000 cells. No limit/thread tuning flags are exposed.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Written or unchanged; `--check`: current |
| 1 | I/O, non-ZIP input, engine/internal or output-stream error |
| 2 | Strict path declined this input: unsupported feature, package structure it will not guess about, or configured size limit; nothing written |
| 3 | `--check`: caches/shape would change; nothing written |
| 64 | Command-line usage error |
| 130 | Interrupted before publication |

A file without the XLSX ZIP local-header signature is an error (1). Malformed ZIPs that pass that check may be structured strict-path refusals (2). Refusals pass the library's feature/context through verbatim; there is no brittle message-text classification.

Human success/check output goes to stdout; refusals/errors go to stderr. `--version` and help print ordinary informational text.

## JSON

With `--json`, recalculation and usage-error outcomes produce exactly one JSON object on stdout, with no diagnostic text there. stderr is empty. The schema id is `formualizer.recalc/1`; additive fields need not bump the id. All keys below are present. Non-applicable fields are `null`, except `written`, which is always a boolean (false on failure or check). Usage errors have null input/output because argument parsing failed.

```json
{
  "schema": "formualizer.recalc/1",
  "status": "written",
  "input": "book.xlsx",
  "output": "book.xlsx",
  "written": true,
  "formula_cells": 1204,
  "cache_cells_changed": 37,
  "worksheet_parts_changed": 1,
  "evaluated": 1204,
  "error_cells": 2,
  "errors": [{"sheet": "Sheet1", "cell": "C4", "error": "#DIV/0!"}],
  "errors_truncated": true,
  "refusal": null,
  "message": "book.xlsx: recalculated 1204 formulas, 37 cached values changed, 2 error cells (Sheet1!C4 #DIV/0!) (written)"
}
```

`status` is one of `written`, `unchanged`, `current`, `stale`, `refused`, `error`, `interrupted`. `output` is the intended destination, or null for `--check`. Successful computation has numeric counters and an `errors` array (possibly empty). Refusals have `refusal: {"feature": "...", "context": "..."}`; errors/interruption have null counters. `errors_truncated` indicates that not all error locations were listed. Locations are deterministic, grouped by error token; sheet names containing `!` are handled correctly. `message` is a human-readable one-line diagnostic, not a stable machine interface.

`formula_cells`/`evaluated` count source formulas, including spill anchors but not generated members. `cache_cells_changed` counts physical caches inserted/replaced/cleared, so it may exceed the formula count. `worksheet_parts_changed` counts changed worksheets, not metadata parts. Attribute-only spill changes may have zero cache changes.

## Examples

```sh
# After an editor saves the workbook:
formualizer recalc book.xlsx --json

# Keep the original, always publishing a separate artifact:
formualizer recalc book.xlsx -o calculated.xlsx

# CI freshness check (exit 3 means stale):
formualizer recalc book.xlsx --check --json --max-errors 5

# Agent loop over explicit paths (no built-in batch mode):
for file in reports/*.xlsx; do formualizer recalc "$file" --json || break; done
```

## Non-goals

No editing, reading/dumping values, engine selection, configuration files, watch mode, directory/batch globbing, limit/thread tuning or legacy fallback. This writer does not claim Excel equivalence for every function or workbook. Tables, data tables and external links are among the strict refusals. Dynamic arrays and fixed-extent CSE arrays are supported; after an openpyxl re-save strips dynamic metadata, the array retains its fixed extent and its `A1#` readers return `#REF!`. See the library reference for fitting and admission policies.

## Embedding

The `formualizer_cli::run` library accepts argv including the program name, independent `Write` sinks and an optional shared `CancelToken`. It returns an exit code without exiting, installing signal handlers or assuming a TTY. Bindings use `default-features = false`; the standalone binary's `signals` feature installs SIGINT handling. See the Rust API docs for the exact signature.
