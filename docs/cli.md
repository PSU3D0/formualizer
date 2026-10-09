# `formualizer recalc`

Recompute every supported formula's cached value after editing an `.xlsx`, preserving source workbook structures. This command uses the strict cache-only XLSX path, with no Umya, LibreOffice or alternative-engine fallback. Formula results such as `#DIV/0!` are successful calculations, not command failures.

## Installation

```sh
uvx formualizer recalc book.xlsx            # Python native wheel, no install step
pip install formualizer                     # then: formualizer recalc book.xlsx
python -m formualizer recalc book.xlsx      # same CLI through the Python module
npx @formualizer/cli recalc book.xlsx       # npm native launcher, no install step
npm i -g @formualizer/cli                   # then: formualizer recalc book.xlsx
cargo binstall formualizer-cli              # prebuilt release binary
cargo install formualizer-cli               # build from crates.io
```

Every channel installs the same `formualizer` command. Prebuilt binaries cover Linux x64/arm64 (glibc and static musl), macOS x64/arm64 and Windows x64. Each [GitHub release](https://github.com/psu3d0/formualizer/releases) also carries `formualizer-cli-v<version>-<target>.tar.gz` (`.zip` on Windows) archives with a `SHA256SUMS` file. The unscoped `formualizer` npm package is the WebAssembly library and does not contain the CLI. The CLI is not available in Pyodide; the Pyodide wheel exposes the same recalculation as `formualizer.recalculate_xlsx_bytes` (in-memory bytes only, no system clock: see [reproducible runs](#reproducible-runs)).

## Commands

```text
formualizer recalc <INPUT> [-o|--output <PATH>] [--check] [--json] [--max-errors <N>]
                   [--now <TIMESTAMP>] [--tz <ZONE>] [--seed <U64>]
                   [--external-links <cached|refuse>]
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
| `--now <TIMESTAMP>` | Fix the instant `TODAY`/`NOW` see. RFC 3339 with an offset or `Z`, such as `2026-01-31T09:00:00Z`; a timestamp without one is a usage error. See [reproducible runs](#reproducible-runs). |
| `--tz <ZONE>` | Timezone `TODAY`/`NOW` are read in: `UTC` or `±HH:MM`. Defaults to the offset in `--now`, otherwise the host's local time. |
| `--seed <U64>` | Seed for `RAND`/`RANDBETWEEN` (0 to 2^64-1). Defaults to a fixed built-in seed. |
| `--external-links <POLICY>` | `cached` (default): formulas read the values Excel cached in the workbook for external links, and the report says so (`external_links` in the JSON, a message line). `refuse`: a workbook whose formulas or used defined names read any external link value is refused (exit 2, feature `external link values`); the decision is made after calculation and nothing is written. Workbooks whose links nothing reads still recalculate. Links are never refreshed under either policy. See [external links](cache-only-xlsx.md#external-links). |
| `-h, --help` / `-V, --version` | Print help or `formualizer <version>` and exit 0. |

- The default writes **in place**, atomically. A true no-op leaves bytes and mtime untouched and reports `unchanged`.
- `-o PATH` explicitly publishes that output, even if the input caches are already current (`written`, with zero changes). The input is untouched unless PATH is the input itself. Existing destination permissions are preserved. Symlink destinations are refused (exit 2).
- `--check` reads the input and computes; current caches exit 0 (`current`), stale caches exit 3 (`stale`). Stale includes spill-shape or attribute changes, not just cache-value changes. Workbooks using `TODAY`/`NOW` can report stale whenever the clock has moved on; pass the same `--now` that produced the file to make `--check` meaningful for them.
- Run `recalc` as the last step that writes the workbook. Any later edit leaves its caches stale.
- Ctrl-C/SIGINT cooperatively cancels at the next library checkpoint (exit 130). Refusal, failure or cancellation observed before atomic publication leaves the destination untouched. Publication is not compare-and-swap against unrelated writers. The Python and npm launchers forward Ctrl-C the same way.

The library reads a bounded snapshot, writes a same-directory temporary file, preserves existing destination permissions, fsyncs the temporary file and renames it atomically. It does not guarantee directory-entry crash durability. See [cache-only XLSX](cache-only-xlsx.md) for eligibility, dynamic arrays, tables, ownership and bounds. Defaults include 64 MiB input/output, 10,000 ZIP entries, 256 MiB expanded bytes, 128 MiB per worksheet/metadata part, 100,000 formulas, XML depth 128, 256 columns and 8,000,000 cells. No limit/thread tuning flags are exposed.

## Exit codes

| Code | Status | Meaning |
| --- | --- | --- |
| 0 | `written`, `unchanged`, `current` | Recalculated (or, with `--check`, already current). Formula error results still exit 0. |
| 1 | `error` | I/O failure, missing or non-ZIP input, engine/internal failure or output-stream error |
| 2 | `refused` | The strict path declined this input: unsupported feature, package structure it will not guess about, resource limit or symlink destination. Nothing written. |
| 3 | `stale` | `--check` only: caches or spill shape would change. Nothing written. |
| 64 | `error` | Command-line usage error, including an invalid `--now`, `--tz`, `--seed` or `--external-links` value |
| 130 | `interrupted` | Cancelled (Ctrl-C/SIGINT) before publication. Nothing written. |

A file without the XLSX ZIP local-header signature is an error (1). Malformed ZIPs that pass that check may be structured strict-path refusals (2). Workbooks saved by Excel (desktop, Mac and Online), LibreOffice, Google Sheets, openpyxl and Info-ZIP `zip` are admitted as containers; ZIP64, encryption, entry comments and unknown ZIP extra fields are refused (see [ZIP containers](cache-only-xlsx.md#zip-containers)). A stored formula the parser cannot read is a refusal (`unparseable formula`, with the sheet, cell and parser message in `context`), not an error. Refusals pass the library's feature/context through verbatim; see [refusal messages](cache-only-xlsx.md#refusal-messages) for their meaning.

Without `--json`, success and `--check` lines (exit 0 and 3) go to stdout; errors, refusals and usage errors go to stderr. `--version` and help print ordinary text to stdout.

## Reproducible runs

- **`RAND` is reproducible by default.** `RAND` and `RANDBETWEEN` derive each value from the seed and the cell's position, so recalculating the same workbook gives the same values on every run and machine. `--seed` picks a different, still reproducible, sequence.
- **Without `--now`, `TODAY`/`NOW` use the host clock** in its local timezone (or `--tz`), sampled once per run.
- **With `--now`, results are fully reproducible.** Two runs with the same `--now`, `--tz` and `--seed` produce byte-identical output, so `--check` is meaningful for volatile workbooks.
- `--tz` defaults to the offset written in `--now`: `--now 2026-03-02T00:30:00+01:00` and `--now 2026-03-01T23:30:00Z --tz +01:00` are the same run, and `TODAY()` is 2026-03-02 in both. Only `UTC` and fixed offsets are accepted; named zones such as `Europe/Paris` are not, because their offset depends on the date.
- `--tz` without `--now` keeps the host clock but reads it in that zone.
- Every computed `--json` report echoes the clock and seed it used (see `clock` and `seed` below). `clock.now` carries the offset that was applied, so `--now <clock.now> --seed <seed>` (plus `--tz <clock.timezone>` when it is not `Local`) replays any run exactly. A host-clock sample is taken to the whole second, the resolution of `NOW()`.
- Builds without a system clock refuse workbooks that use `TODAY`/`NOW` (exit 2) unless `--now` is given. The Pyodide wheel is such a build: its `recalculate_xlsx_bytes` refuses them unless `deterministic_timestamp_utc` is passed, and reports `clock.now` as `None` when no timestamp was given.

```sh
# Pin the clock and seed; --check then stays current until the inputs change:
formualizer recalc book.xlsx --now 2026-01-31T09:00:00Z --seed 7
formualizer recalc book.xlsx --check --now 2026-01-31T09:00:00Z --seed 7
```

The Python functions `recalculate_xlsx_file`/`recalculate_xlsx_bytes` take the same options as `rng_seed`, `deterministic_timestamp_utc` (an aware `datetime`) and `deterministic_timezone` (`"utc"`, `"+02:00"` or offset seconds; default UTC; requires the timestamp). The npm `recalculateXlsxBytes(bytes, errorLocationLimit, options)` takes `{rngSeed, deterministicTimestampUtc, deterministicTimezone}` with the same rules. Both return `clock` and `seed`. The `--external-links` policy is `external_links="cached"|"refuse"` in Python and `externalLinks: 'cached' | 'refuse'` in the npm options; a refusal raises an error.

## JSON

With `--json`, every outcome except help and version, including usage errors, produces exactly one JSON object on stdout, followed by a newline. Nothing is written to stderr. The schema id is `formualizer.recalc/1`; additive fields need not bump the id. All keys below are always present, except `external_links`, which appears only when the computation read cached external link values.

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
  "error_cells": 2,
  "errors": [
    {"sheet": "Sheet1", "cell": "B2", "error": "#DIV/0!", "message": null},
    {"sheet": "Sheet1", "cell": "C4", "error": "#NAME?", "message": "Unknown function: _xll.EURO"}
  ],
  "errors_truncated": false,
  "unknown_functions": [{"name": "_xll.EURO", "cells": 1}],
  "refusal": null,
  "clock": {"now": "2026-10-04T09:15:02+02:00", "timezone": "Local", "fixed": false},
  "seed": 17361606158148326741,
  "message": "book.xlsx: recalculated 5 formulas, 7 cached values changed, 2 error cells (Sheet1!B2 #DIV/0!, Sheet1!C4 #NAME? (Unknown function: _xll.EURO)) (written)\nunknown functions (cells produce #NAME?): _xll.EURO (1 cell)"
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `schema` | string | Always `formualizer.recalc/1`. |
| `status` | string | `written`, `unchanged`, `current`, `stale`, `refused`, `error` or `interrupted`. |
| `input` | string or null | The input path; null for usage errors. |
| `output` | string or null | The intended destination (the input unless `-o`); null for `--check` and usage errors. |
| `written` | boolean | Whether a file was published. Always false for `--check`, refusals, errors and interruption. |
| `formula_cells` | integer or null | Source formulas, counting spill anchors but not generated spill members. |
| `cache_cells_changed` | integer or null | Physical caches inserted, replaced or cleared; can exceed the formula count. |
| `worksheet_parts_changed` | integer or null | Changed worksheets (not metadata parts). Nonzero means `stale` under `--check`. |
| `error_cells` | integer or null | Total formula cells whose result is an Excel error. |
| `errors` | array or null | Up to `--max-errors` `{sheet, cell, error, message}` locations, grouped by error token. |
| `errors[].message` | string or null | The engine's reason for that cell's error: `Unknown function: NAME` for a function the engine does not implement (an add-in such as `_xll.EURO`, a VBA/macro function or any other unknown name), `Undefined name: NAME` for a name that is not defined. Null when the cell has no reason of its own, for example a `#NAME?` inherited from a precedent, or an ordinary `#DIV/0!`. |
| `errors_truncated` | boolean or null | True when `errors` lists fewer locations than `error_cells`. |
| `unknown_functions` | array or null | Every function the engine does not implement that some formula calls, as `{name, cells}` sorted by name, with the number of error cells that call it. Complete even when `errors` is truncated; empty when there are none. |
| `refusal` | object or null | `{"feature": ..., "context": ...}` when `status` is `refused`. |
| `clock` | object or null | The clock `TODAY`/`NOW` used: `now`, `timezone` and `fixed`. See [reproducible runs](#reproducible-runs). |
| `clock.now` | string or null | RFC 3339 instant, written in the UTC offset that was applied (`Z` for UTC; the host's offset at that instant for `Local`). Null only in builds without a system clock when `--now` is absent. |
| `clock.timezone` | string | `Local`, `UTC` or `±HH:MM`. |
| `clock.fixed` | boolean | True when `--now` set the instant. |
| `seed` | integer or null | The `RAND`/`RANDBETWEEN` seed, an unsigned 64-bit integer. JavaScript's `JSON.parse` rounds values above 2^53; read it as a big integer to replay. |
| `external_links` | object, optional | Present only after a successful computation that read values of external workbook links: `{"links_used": n, "refreshed": false, "policy": "cached"}`. `links_used` counts the links (`xl/externalLinks` parts) whose cached values a formula or used defined name read. Links are never refreshed, so the values are the ones Excel last stored in the workbook, and `refreshed` is always false. `policy` is the `--external-links` policy applied; it is always `cached` here, because under `refuse` such a run is refused instead. The message gains a line `external links: used the values cached in the workbook for n link(s) (not refreshed)`. See [external links](cache-only-xlsx.md#external-links). |
| `message` | string | One-line human diagnostic. Not a stable machine interface. |

Counters, `errors`, `errors_truncated`, `unknown_functions`, `clock` and `seed` are present after a successful computation (exit 0 or 3) and null otherwise. Sheet names containing `!` are split correctly. Attribute-only spill changes may have zero cache changes. A numeric cache within one unit in the 15th significant digit of the computed value is current and is not counted in `cache_cells_changed` (see [numeric precision](cache-only-xlsx.md#numeric-precision)).

### Functions the engine does not implement

A formula that calls a function the engine does not implement (an Excel add-in function such as `_xll.EURO`, a VBA or XLM macro function, or a function from a newer Excel version) evaluates to `#NAME?`, as it does in Excel without the add-in. The workbook is not refused: the `#NAME?` result is written like any other formula error and the run exits 0. Each such cell's `message` names the function, and `unknown_functions` counts the cells per function. Cells that depend on them inherit `#NAME?` with a null `message`. Decide from the reason whether the result is acceptable; formualizer cannot supply an add-in's values.

A refusal:

```json
{"schema":"formualizer.recalc/1","status":"refused","input":"model.xlsx","output":"model.xlsx","written":false,"formula_cells":null,"cache_cells_changed":null,"worksheet_parts_changed":null,"error_cells":null,"errors":null,"errors_truncated":null,"unknown_functions":null,"refusal":{"feature":"data-table formula","context":"worksheet"},"clock":null,"seed":null,"message":"model.xlsx: Unsupported feature: data-table formula in worksheet. Nothing was written."}
```

Under `--external-links refuse`, a workbook whose formulas read link values is refused after calculation; `context` names the first read and the message names the flag that allows it:

```json
{"schema":"formualizer.recalc/1","status":"refused","input":"linked.xlsx","output":"linked.xlsx","written":false,"formula_cells":null,"cache_cells_changed":null,"worksheet_parts_changed":null,"error_cells":null,"errors":null,"errors_truncated":null,"unknown_functions":null,"refusal":{"feature":"external link values","context":"Sheet1!A1: [1]Data!A1 (recalculating would use the values cached in the workbook for 1 external link; links are never refreshed)"},"clock":null,"seed":null,"message":"linked.xlsx: Unsupported feature: external link values in Sheet1!A1: [1]Data!A1 (recalculating would use the values cached in the workbook for 1 external link; links are never refreshed). Nothing was written. Pass --external-links cached to recalculate with them."}
```

A usage error (exit 64):

```json
{"schema":"formualizer.recalc/1","status":"error","input":null,"output":null,"written":false,"formula_cells":null,"cache_cells_changed":null,"worksheet_parts_changed":null,"error_cells":null,"errors":null,"errors_truncated":null,"unknown_functions":null,"refusal":null,"clock":null,"seed":null,"message":"error: unexpected argument '--bogus' found ..."}
```

## Examples

```sh
# After an editor saves the workbook:
formualizer recalc book.xlsx --json

# Keep the original, always publishing a separate artifact:
formualizer recalc book.xlsx -o calculated.xlsx

# CI freshness check (exit 3 means stale):
formualizer recalc book.xlsx --check --json --max-errors 5

# Reproducible output for a workbook using TODAY/NOW/RAND:
formualizer recalc book.xlsx --now 2026-01-31T09:00:00Z --seed 7

# Refuse workbooks whose formulas read external link values:
formualizer recalc book.xlsx --external-links refuse --json

# Agent loop over explicit paths (no built-in batch mode):
for file in reports/*.xlsx; do formualizer recalc "$file" --json || break; done
```

## Non-goals

No editing, reading/dumping values, engine selection, configuration files, watch mode, directory/batch globbing, limit/thread tuning, link refreshing or legacy fallback. This writer does not claim Excel equivalence for every function or workbook. External links are read from the values cached in the workbook (see [external links](cache-only-xlsx.md#external-links)); data tables and the other strict refusals are listed in [cache-only XLSX](cache-only-xlsx.md#strict-eligibility). Dynamic arrays, fixed-extent CSE arrays and Excel tables within the validated subset are supported; after an openpyxl re-save strips dynamic metadata, the array keeps its fixed extent and its `A1#` readers return `#REF!`.

## Embedding

The `formualizer_cli::run` library accepts argv including the program name, independent `Write` sinks and an optional shared `CancelToken`. It returns an exit code without exiting, installing signal handlers or assuming a TTY. Bindings use `default-features = false`; the standalone binary's `signals` feature installs SIGINT handling. See the Rust API docs for the exact signature.
