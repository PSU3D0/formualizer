# `formualizer recalc`

Recompute every supported formula's cached value after editing an `.xlsx`, preserving source workbook structures. This command uses the strict cache-only XLSX path, with no Umya, LibreOffice or alternative-engine fallback. Formula results such as `#DIV/0!` are successful calculations, not command failures.

## Installation

> **Not yet published:** these install channels go live with the first release that includes `formualizer recalc`. Until then, build from a source checkout with `cargo run --release -p formualizer-cli -- recalc book.xlsx`.

```sh
uvx formualizer recalc book.xlsx            # Python native wheel, no install step
pip install formualizer                     # then: formualizer recalc book.xlsx
python -m formualizer recalc book.xlsx      # same CLI through the Python module
npx @formualizer/cli recalc book.xlsx       # npm native launcher, no install step
npm i -g @formualizer/cli                   # then: formualizer recalc book.xlsx
cargo binstall formualizer-cli              # prebuilt release binary
cargo install formualizer-cli               # build from crates.io
```

Every channel installs the same `formualizer` command. Prebuilt binaries cover Linux x64/arm64 (glibc and static musl), macOS x64/arm64 and Windows x64. Each [GitHub release](https://github.com/psu3d0/formualizer/releases) also carries `formualizer-cli-v<version>-<target>.tar.gz` (`.zip` on Windows) archives with a `SHA256SUMS` file. The unscoped `formualizer` npm package is the WebAssembly library and does not contain the CLI. The CLI is not available in Pyodide.

## Commands

```text
formualizer recalc <INPUT> [-o|--output <PATH>] [--check] [--json] [--max-errors <N>]
formualizer --version
formualizer help [recalc]
```

| Option | Meaning |
| --- | --- |
| `<INPUT>` | The `.xlsx` to recalculate. |
| `-o, --output <PATH>` | Publish to PATH instead of replacing INPUT. Always written, even when nothing changed. |
| `--check` | Compute only and report whether caches are current. Never writes, even with `-o`. |
| `--json` | Print exactly one `formualizer.recalc/1` JSON object on stdout. |
| `--max-errors <N>` | List at most N error-cell locations (default 20; 0 lists none). Counts are unaffected. |
| `-h, --help` / `-V, --version` | Print help or `formualizer <version>` and exit 0. |

- The default writes **in place**, atomically. A true no-op leaves bytes and mtime untouched and reports `unchanged`.
- `-o PATH` explicitly publishes that output, even if the input caches are already current (`written`, with zero changes). The input is untouched unless PATH is the input itself. Existing destination permissions are preserved. Symlink destinations are refused (exit 2).
- `--check` reads the input and computes; current caches exit 0 (`current`), stale caches exit 3 (`stale`). Stale includes spill-shape or attribute changes, not just cache-value changes. Workbooks with volatile functions (`NOW`, `RAND`, ...) can report stale on every run, because each run samples them again.
- Ctrl-C/SIGINT cooperatively cancels at the next library checkpoint (exit 130). Refusal, failure or cancellation observed before atomic publication leaves the destination untouched. Publication is not compare-and-swap against unrelated writers. The Python and npm launchers forward Ctrl-C the same way.

The library reads a bounded snapshot, writes a same-directory temporary file, preserves existing destination permissions, fsyncs the temporary file and renames it atomically. It does not guarantee directory-entry crash durability. See [cache-only XLSX](cache-only-xlsx.md) for eligibility, dynamic arrays, tables, ownership and bounds. Defaults include 64 MiB input/output, 10,000 ZIP entries, 256 MiB expanded bytes, 128 MiB per worksheet/metadata part, 100,000 formulas, XML depth 128, 256 columns and 8,000,000 cells. No limit/thread tuning flags are exposed.

## Exit codes

| Code | Status | Meaning |
| --- | --- | --- |
| 0 | `written`, `unchanged`, `current` | Recalculated (or, with `--check`, already current). Formula error results still exit 0. |
| 1 | `error` | I/O failure, missing or non-ZIP input, engine/internal failure or output-stream error |
| 2 | `refused` | The strict path declined this input: unsupported feature, package structure it will not guess about, resource limit or symlink destination. Nothing written. |
| 3 | `stale` | `--check` only: caches or spill shape would change. Nothing written. |
| 64 | `error` | Command-line usage error |
| 130 | `interrupted` | Cancelled (Ctrl-C/SIGINT) before publication. Nothing written. |

A file without the XLSX ZIP local-header signature is an error (1). Malformed ZIPs that pass that check may be structured strict-path refusals (2). Refusals pass the library's feature/context through verbatim; see [refusal messages](cache-only-xlsx.md#refusal-messages) for their meaning.

Without `--json`, success and `--check` lines (exit 0 and 3) go to stdout; errors, refusals and usage errors go to stderr. `--version` and help print ordinary text to stdout.

## JSON

With `--json`, every outcome except help and version, including usage errors, produces exactly one JSON object on stdout, followed by a newline. Nothing is written to stderr. The schema id is `formualizer.recalc/1`; additive fields need not bump the id. All keys below are always present.

```json
{
  "schema": "formualizer.recalc/1",
  "status": "written",
  "input": "book.xlsx",
  "output": "book.xlsx",
  "written": true,
  "formula_cells": 5,
  "cache_cells_changed": 7,
  "worksheet_parts_changed": 1,
  "evaluated": 5,
  "error_cells": 1,
  "errors": [{"sheet": "Sheet1", "cell": "B2", "error": "#DIV/0!"}],
  "errors_truncated": false,
  "refusal": null,
  "message": "book.xlsx: recalculated 5 formulas, 7 cached values changed, 1 error cells (Sheet1!B2 #DIV/0!) (written)"
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `schema` | string | Always `formualizer.recalc/1`. |
| `status` | string | `written`, `unchanged`, `current`, `stale`, `refused`, `error` or `interrupted`. |
| `input` | string or null | The input path; null for usage errors. |
| `output` | string or null | The intended destination (the input unless `-o`); null for `--check` and usage errors. |
| `written` | boolean | Whether a file was published. Always false for `--check`, refusals, errors and interruption. |
| `formula_cells`, `evaluated` | integer or null | Source formulas, counting spill anchors but not generated spill members. |
| `cache_cells_changed` | integer or null | Physical caches inserted, replaced or cleared; can exceed the formula count. |
| `worksheet_parts_changed` | integer or null | Changed worksheets (not metadata parts). Nonzero means `stale` under `--check`. |
| `error_cells` | integer or null | Total formula cells whose result is an Excel error. |
| `errors` | array or null | Up to `--max-errors` `{sheet, cell, error}` locations, grouped by error token. |
| `errors_truncated` | boolean or null | True when `errors` lists fewer locations than `error_cells`. |
| `refusal` | object or null | `{"feature": ..., "context": ...}` when `status` is `refused`. |
| `message` | string | One-line human diagnostic. Not a stable machine interface. |

Counters, `errors` and `errors_truncated` are numbers/arrays after a successful computation (exit 0 or 3) and null otherwise. Sheet names containing `!` are split correctly. Attribute-only spill changes may have zero cache changes.

A refusal:

```json
{"schema":"formualizer.recalc/1","status":"refused","input":"model.xlsx","output":"model.xlsx","written":false,"formula_cells":null,"cache_cells_changed":null,"worksheet_parts_changed":null,"evaluated":null,"error_cells":null,"errors":null,"errors_truncated":null,"refusal":{"feature":"data-table formula","context":"worksheet"},"message":"model.xlsx: Unsupported feature: data-table formula in worksheet. Nothing was written."}
```

A usage error (exit 64):

```json
{"schema":"formualizer.recalc/1","status":"error","input":null,"output":null,"written":false,"formula_cells":null,"cache_cells_changed":null,"worksheet_parts_changed":null,"evaluated":null,"error_cells":null,"errors":null,"errors_truncated":null,"refusal":null,"message":"error: unexpected argument '--bogus' found ..."}
```

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

No editing, reading/dumping values, engine selection, configuration files, watch mode, directory/batch globbing, limit/thread tuning or legacy fallback. This writer does not claim Excel equivalence for every function or workbook. Data tables, external links and the other strict refusals are listed in [cache-only XLSX](cache-only-xlsx.md#strict-eligibility). Dynamic arrays, fixed-extent CSE arrays and Excel tables within the validated subset are supported; after an openpyxl re-save strips dynamic metadata, the array keeps its fixed extent and its `A1#` readers return `#REF!`.

## Embedding

The `formualizer_cli::run` library accepts argv including the program name, independent `Write` sinks and an optional shared `CancelToken`. It returns an exit code without exiting, installing signal handlers or assuming a TTY. Bindings use `default-features = false`; the standalone binary's `signals` feature installs SIGINT handling. See the Rust API docs for the exact signature.
