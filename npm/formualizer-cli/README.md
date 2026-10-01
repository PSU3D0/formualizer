# formualizer-cli

The native `formualizer` command for Node.js projects. After a tool or an agent edits an `.xlsx`, `formualizer recalc` recomputes every formula's cached value and writes it back, preserving the rest of the workbook.

```sh
npx formualizer-cli recalc book.xlsx          # one-off
npm install --save-dev formualizer-cli        # then: npx formualizer recalc book.xlsx
```

```text
formualizer recalc <INPUT> [-o|--output <PATH>] [--check] [--json] [--max-errors <N>]
```

Exit codes: 0 success, 1 error, 2 refused (unsupported workbook feature, nothing written), 3 `--check` found stale caches, 64 usage error, 130 interrupted. See the [CLI reference](https://github.com/psu3d0/formualizer/blob/main/docs/cli.md).

## How it installs

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

There are no install scripts and no WebAssembly fallback. Installing with `--omit=optional` (or a lockfile from another platform) leaves no binary; the launcher then prints the supported platforms and alternatives and exits 1. Set `FORMUALIZER_CLI_BINARY` to use a specific `formualizer` binary instead.

Other channels run the same Rust command: `uvx formualizer recalc book.xlsx` (Python wheel), `cargo install formualizer-cli`, or the archives attached to each [GitHub release](https://github.com/psu3d0/formualizer/releases).

This package is unrelated to the `formualizer` npm package, which is the WebAssembly engine library.

## License

MIT OR Apache-2.0.
