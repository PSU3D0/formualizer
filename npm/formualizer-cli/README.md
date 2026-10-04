# @formualizer/cli

The native `formualizer` command for Node.js projects and agents. After a tool or an agent edits an `.xlsx`, `formualizer recalc` recomputes every supported formula's cached value and writes it back, preserving formula text and the rest of the workbook.

> **Not yet published:** this package goes live with the first release that includes `formualizer recalc`.

```sh
npx @formualizer/cli recalc book.xlsx           # one-off, no install
npm i -g @formualizer/cli                       # then: formualizer recalc book.xlsx
npm i -D @formualizer/cli                       # per project: npx formualizer recalc book.xlsx
```

```text
formualizer recalc <INPUT> [-o|--output <PATH>] [--check] [--json] [--max-errors <N>]
                   [--now <TIMESTAMP>] [--tz <ZONE>] [--seed <U64>]
```

- Default: recalculate in place, atomically. `-o calculated.xlsx` writes a separate file and leaves the input untouched.
- `--check`: compute only; exit 0 if caches are current, 3 if stale. Never writes.
- `--json`: print one `formualizer.recalc/1` JSON object with counts, error-cell locations, any refusal, and the `clock` and `seed` used.
- `--now 2026-01-31T09:00:00Z` (offset or `Z` required), `--tz UTC|±HH:MM`, `--seed <u64>`: fix `TODAY`/`NOW` and the `RAND` seed for byte-identical reruns. `RAND` is reproducible by default; without `--now`, `TODAY`/`NOW` use host local time.

Exit codes: 0 success, 1 error, 2 refused (unsupported workbook feature, nothing written), 3 `--check` found stale caches, 64 usage error, 130 interrupted. See the [CLI reference](https://www.formualizer.dev/docs/recalc-cli/cli-reference).

## For agents

Keep your workbook editor: save, then run `npx @formualizer/cli recalc file.xlsx --json`.
Branch on exit code/status, inspect `errors`, fix and repeat. Exit 2 means refused: do not retry the same unsupported workbook or claim recalculation.
Recalc must be the last writing step; never run concurrent writers on the same file.
See the [agent workflow](https://www.formualizer.dev/docs/recalc-cli/agent-workflow) and the [portable skill](https://github.com/psu3d0/formualizer/blob/main/skills/formualizer-recalc/SKILL.md).

## Supported platforms

This package contains only a small launcher. The binary comes from one optional dependency selected by npm through its `os`, `cpu` and `libc` fields:

| Platform | Package |
| --- | --- |
| Linux x64, glibc 2.17+ | `@formualizer/cli-linux-x64-gnu` |
| Linux arm64, glibc 2.17+ | `@formualizer/cli-linux-arm64-gnu` |
| Linux x64, musl (static) | `@formualizer/cli-linux-x64-musl` |
| Linux arm64, musl (static) | `@formualizer/cli-linux-arm64-musl` |
| macOS x64 | `@formualizer/cli-darwin-x64` |
| macOS arm64 | `@formualizer/cli-darwin-arm64` |
| Windows x64 | `@formualizer/cli-win32-x64-msvc` |

There are no install scripts and no WebAssembly fallback. Node.js 18 or newer is required.

## Unsupported platform or missing binary

If no binary is installed, the launcher prints the supported platforms and alternatives and exits 1. This happens on a platform outside the table, or when the optional dependency was skipped (`--omit=optional`, `--no-optional`, or a lockfile created on another platform). Then:

- On a supported platform, reinstall with optional dependencies enabled.
- Set `FORMUALIZER_CLI_BINARY` to the path of a `formualizer` binary you built or downloaded; the launcher runs it instead.
- Use another channel that runs the same Rust command: `uvx formualizer recalc book.xlsx` (Python wheel), `cargo install formualizer-cli` (builds from source on any Rust target), or the archives attached to each [GitHub release](https://github.com/psu3d0/formualizer/releases).

This package is separate from the unscoped `formualizer` npm package, which is the WebAssembly engine library and does not contain the CLI.

## License

MIT OR Apache-2.0.
