# Formualizer (Meta Crate)

The `formualizer` crate is the curated entry point for the Formualizer spreadsheet
ecosystem. It re-exports the components required to treat spreadsheets as typed,
deterministic programs while letting advanced users opt into individual crates as
needed.

## Intent

- Provide a single dependency that mirrors the “batteries-included” surface exposed
  by the CLI and forthcoming Python/Wasm bindings.
- Keep the implementation modular. Internal crates such as
  `formualizer-eval`, `formualizer-workbook`, `sheetport-spec`, and
  `formualizer-sheetport` continue to evolve independently with focused
  responsibilities.
- Offer feature flags so downstream consumers can slim the dependency graph when
  they only need a subset of the stack.

## Philosophy

1. **Contracts first.** Spreadsheets plus manifests behave like pure functions. The
   meta crate ensures those contracts are ergonomic to load, validate, and execute.
2. **Deterministic by default.** Every exported API favors predictable evaluation,
   idempotent changes, and auditable data flow.
3. **Interoperable layers.** The roll-up crate aims to be the stable façade for
   higher-level runtimes (agent orchestration, automation servers) without hiding
   the underlying building blocks.

## Status

Phase 0/1 (SheetPort spec) is complete. Phase 2 work is in progress to bind
manifests to real workbooks and surface runtime utilities. Expect the re-export
surface to grow as the runtime stabilizes.
