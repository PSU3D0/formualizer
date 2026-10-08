# formualizer-cli

The `formualizer` command: recalculate the cached formula values in an `.xlsx` after another tool has edited it, preserving formula text, styles and the rest of the package. It is part of [Formualizer](https://github.com/psu3d0/formualizer), a Rust spreadsheet engine.

## Install

```sh
cargo binstall formualizer-cli   # prebuilt binary from the GitHub release
cargo install formualizer-cli    # build from source
```

Both install a binary named `formualizer`. The same command also ships as a Python console script (`uvx formualizer recalc book.xlsx`), as the npm package `@formualizer/cli` (`npx @formualizer/cli recalc book.xlsx`) and as archives with `SHA256SUMS` on each [GitHub release](https://github.com/psu3d0/formualizer/releases).

## Usage

```text
formualizer recalc <INPUT> [-o|--output <PATH>] [--check] [--json] [--max-errors <N>]
                   [--now <TIMESTAMP>] [--tz <ZONE>] [--seed <U64>]
```

```sh
$ formualizer recalc book.xlsx
book.xlsx: recalculated 5 formulas, 7 cached values changed, 1 error cells (Sheet1!B2 #DIV/0!) (written)

$ formualizer recalc book.xlsx --check
book.xlsx: recalculated 5 formulas, 0 cached values changed, 1 error cells (Sheet1!B2 #DIV/0!) (current)

$ formualizer recalc book.xlsx -o calculated.xlsx --json
{"schema":"formualizer.recalc/1","status":"written","input":"book.xlsx","output":"calculated.xlsx","written":true,"formula_cells":5,"cache_cells_changed":0,"worksheet_parts_changed":0,"error_cells":1,"errors":[{"sheet":"Sheet1","cell":"B2","error":"#DIV/0!"}],"errors_truncated":false,"refusal":null,"clock":{"now":"2026-10-04T09:15:02+02:00","timezone":"Local","fixed":false},"seed":17361606158148326741,"message":"book.xlsx: recalculated 5 formulas, 0 cached values changed, 1 error cells (Sheet1!B2 #DIV/0!) (written)"}
```

- Default: recalculate in place, atomically; a no-op leaves the file untouched. `-o PATH` always writes PATH and leaves the input alone.
- `--check`: compute only and report whether caches are current. Never writes.
- `--json`: one `formualizer.recalc/1` object on stdout for every outcome, including refusals and usage errors.
- `--max-errors N`: list at most N error-cell locations (default 20).
- `--now TIMESTAMP` (RFC 3339 with an offset or `Z`), `--tz UTC|±HH:MM`, `--seed U64`: fix the `TODAY`/`NOW` clock and the `RAND` seed. `RAND` is reproducible by default; without `--now`, `TODAY`/`NOW` use host local time. With `--now`, reruns are byte-identical and `--check` is meaningful for volatile workbooks. JSON reports echo the `clock` and `seed` used, so any run can be replayed.

Formula results such as `#DIV/0!` are calculations, not command failures. External workbook links read the values Excel cached in the workbook and are never refreshed; the JSON report then carries `external_links`. Workbooks outside the supported subset (data tables, external references the link cache cannot serve, hidden-row `SUBTOTAL`, circular references and others) are refused as a whole and nothing is written.

## Exit codes

| Code | Status | Meaning |
| --- | --- | --- |
| 0 | `written`, `unchanged`, `current` | Success |
| 1 | `error` | I/O, input or internal error |
| 2 | `refused` | Unsupported workbook feature or resource limit; nothing written |
| 3 | `stale` | `--check` found caches that would change; nothing written |
| 64 | `error` | Usage error, including an invalid `--now`/`--tz`/`--seed` |
| 130 | `interrupted` | Cancelled by Ctrl-C before publication; nothing written |

## More

- [Recalc CLI documentation](https://www.formualizer.dev/docs/recalc-cli): install, agent workflow, full reference and the supported/refused list.
- [CLI reference on GitHub](https://github.com/psu3d0/formualizer/blob/main/docs/cli.md) and [eligibility contract](https://github.com/psu3d0/formualizer/blob/main/docs/cache-only-xlsx.md).
- Embedding: `formualizer_cli::run(args, stdout, stderr, cancel)` returns the exit code without exiting the process or installing signal handlers. Disable default features to drop the SIGINT handler.

## License

MIT OR Apache-2.0.
