# DOCS_LAYOUT.md

Status: Draft (v1)

## Goal

Define the v1 information architecture for Formualizer docs so users can quickly:

- Understand what Formualizer is
- Get started in Rust/Python/JS fast
- Discover function behavior through searchable reference pages
- Validate behavior interactively with sandbox examples

This layout is optimized for:

1. Fast onboarding
2. Strong reference depth (300+ functions)
3. Searchability and SEO
4. Low maintenance through generated metadata

---

## Site location and structure

Recommended: keep docs site in this monorepo under `docs-site/`.

- `docs-site/` — Fumadocs app (Next.js)
- `docs-site/content/` — hand-authored + generated MDX/content collections
- source-of-truth function metadata remains colocated in Rust doc comments and generated via `xtask`

Rationale:
- keeps docs and engine changes in lockstep
- simplifies CI checks for generated schema/doc freshness
- avoids version drift across repos

---

## Top-level navigation

## 1) Introduction

- **What is Formualizer?**
- **Why Formualizer?** (performance, embeddings, language bindings)
- **Architecture at a glance**
- **Versioning + stability expectations**

## 2) Quickstarts

- **Rust quickstart**
- **Python quickstart**
- **JS/WASM quickstart**
- **First workbook in 5 minutes**

## 3) Core Concepts

- Parse -> AST -> Evaluate -> Workbook
- Dependency graph and recalculation
- Value/coercion/error semantics
- Spill behavior
- Named ranges and tables
- Source/table integration concepts (forward-looking)

## 4) Guides

- Workbook edits, batching, transactions
- Undo/redo + changelog behavior
- Custom functions (Rust/Python/JS)
- WASM plugin flow (inspect/attach/bind)
- LET/LAMBDA behavior and callable semantics
- Testing for deterministic behavior

## 5) Reference

- Rust API reference pointers and high-level map
- Python API reference
- JS/WASM API reference
- Error and type reference
- **Function reference** (generated; one page per builtin)

## 6) Playground

- Formula sandbox
- Grid sandbox (optional for v1 if needed)
- Parser/AST inspector (optional in v1)

---

## Function reference layout (generated)

Each function page should contain:

1. Function name and aliases
2. Human summary (short, original)
3. Generated schema block (from `xtask docs-schema`):
   - signature
   - min/max/variadic
   - normalized arg schema
   - caps
4. Formula example(s)
5. Optional Rust snippet
6. Related functions

Canonical route pattern:

- `/reference/functions/<canonical-name-lowercase>`

Alias routes should redirect to canonical pages.

---

## Search and indexing strategy

## Search requirements

Search must support:

- exact function matches (`SUM`, `XLOOKUP`)
- alias matches
- fuzzy function name typo tolerance
- category filtering (math, text, financial, lookup, ...)
- optional tags (`volatile`, `array`, `reference`, `dynamic`)

## Index artifact

Generate a function index artifact during docs build, e.g.:

- `docs-site/generated/functions-index.json`

Suggested fields:

- `name`
- `aliases[]`
- `category`
- `summary`
- `url`
- `signature`
- `tags[]`

## SEO posture

- static pages for function reference
- canonical URLs for each function
- sitemap generation
- predictable heading structure
- avoid duplicate content via alias redirects

---

## Content policy

- Keep summaries concise and original.
- Treat external function specs as factual input, not direct copy source.
- Use generated schema/signature blocks to avoid drift.
- Require formula examples for function pages; Rust snippets optional unless specifically useful.

---

## Non-goals for v1

- Full API docs replacement for docs.rs
- Rich scenario editor in playground for every feature
- Deep source/table provider docs beyond initial conceptual guidance

These can come in v2 after base docs launch.
