# DOCS_IMPL_PLAN.md

Status: Draft (phased)

## Objective

Implement the Formualizer docs site (Fumadocs) with a practical sequence that ships value early and scales to full function reference depth.

---

## Phase 0 — Preparation and guardrails

Goal: lock technical inputs and doc quality checks before site coding accelerates.

Tasks:

1. Ensure `xtask docs-schema` and `xtask docs-audit` are stable in CI.
2. Confirm generated schema blocks are present and deterministic.
3. Define content conventions for function summaries and formula examples.

Exit criteria:

- `docs-schema` check mode passes cleanly.
- `docs-audit` baseline established with clear failure categories.

---

## Phase 1 — Site scaffold and IA

Goal: launch a skeleton site with stable nav and routing.

Tasks:

1. Scaffold Fumadocs app under `docs-site/`.
2. Implement top-level sections from `DOCS_LAYOUT.md`.
3. Add landing page and quickstarts placeholders.
4. Add CI checks for docs build/link validation.

Exit criteria:

- Site builds and deploy preview works.
- Navigation structure is locked.

---

## Phase 2 — High-value authored pages

Goal: ship core documentation users need immediately.

Tasks:

1. Write strong pages for:
   - Intro
   - Rust/Python/JS quickstarts
   - custom functions guide
   - WASM plugin inspect/attach/bind guide
2. Add at least one embedded sandbox per top guide cluster.

Exit criteria:

- New users can complete first successful eval in each binding.

---

## Phase 3 — Function reference generation

Goal: produce scalable, searchable function docs.

Tasks:

1. Build content generator that reads colocated doc comments + generated schema blocks.
2. Emit function MDX/pages and function index artifacts.
3. Implement canonical/alias routing.

Exit criteria:

- all implemented functions have generated reference pages
- index artifact feeds search

---

## Phase 4 — Search and discoverability

Goal: make function docs discoverable quickly.

Tasks:

1. Integrate search provider supported by Fumadocs setup.
2. Ensure function name + alias + category indexing.
3. Add command-palette style function jump.

Exit criteria:

- function name lookup and alias lookup are reliable
- search feels fast and precise

---

## Phase 5 — Formula examples coverage sprint

Goal: close the docs-audit gap via parallel refinement.

Tasks:

1. Use `docs-audit --json-out` to shard by category/file.
2. Dispatch parallel subagents for summary/example writing.
3. Re-run `docs-schema --apply` then `docs-audit --strict` until green.

Exit criteria:

- docs-audit strict passes for targeted scope (or full builtins)

---

## Phase 6 — Sandbox deepening

Goal: convert docs into a product-like interactive experience.

Tasks:

1. Improve formula sandbox UX (share links, errors, examples).
2. Add parser/AST inspector route.
3. Optional: mini-grid workbook sandbox.

Exit criteria:

- users can experiment and validate behavior from docs directly

---

## Phase 7 — Release hardening

Goal: production-ready docs operation.

Tasks:

1. Add sitemap, metadata, canonical links.
2. Add broken-link checks and smoke tests in CI.
3. Add docs contribution guide and templates.

Exit criteria:

- stable docs release pipeline
- clear contributor path for docs updates

---

## CI gate recommendations

Minimum docs-related gates:

- `cargo run -p xtask -- docs-schema`
- `cargo run -p xtask -- docs-audit --strict` (scope-adjusted initially)
- docs build command (`site`)
- link checker

---

## Initial scope recommendation

Ship phases 1–4 first, then run phase 5 as a focused quality sprint.

This balances fast GTM with long-term quality.
