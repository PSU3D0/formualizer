# DOCS_SANDBOX_ARCH.md

Status: Draft (technical deep dive)

## Purpose

Define the technical approach for interactive docs sandboxes powered by the Formualizer WASM bindings.

The sandbox should make docs executable and trustworthy while staying lightweight.

---

## Scope

v1 sandbox features:

1. Formula input
2. Optional simple input cell map
3. Evaluation result / error display
4. Optional token/AST panel for parser docs

Out of scope for initial v1:

- full spreadsheet UI parity
- heavy persisted collaborative sessions
- advanced execution tracing UI

---

## Architecture overview

## Frontend

- Fumadocs page uses client component(s), e.g. `<FormulaSandbox />`
- Component lazy-loads WASM module from `bindings/wasm` package artifact
- Each sandbox instance owns isolated workbook state by default

## Runtime

- Use WASM workbook API for set/eval operations
- Keep eval synchronous from user interaction perspective
- Batch updates when applying multi-cell fixtures

## Data flow

1. User enters formula/input values
2. Sandbox initializes workbook if needed
3. Sandbox writes inputs + formula to target cell
4. Sandbox evaluates cell/workbook
5. UI renders value/error + optional debug panels

---

## Component model

## `FormulaSandbox`

Props (suggested):

- `initialFormula?: string`
- `initialInputs?: Array<{ sheet: string; row: number; col: number; value: unknown }>`
- `targetCell?: { sheet: string; row: number; col: number }`
- `showAst?: boolean`
- `showTokens?: boolean`

Behavior:

- safe defaults: one sheet (`Sheet1`) and target at `A1`
- resets local state per component instance
- supports permalink serialization (later phase)

---

## Security/performance considerations

- No remote code execution; formulas run in the embedded WASM engine only.
- Avoid unbounded recalculation loops via conservative sandbox limits.
- Debounce evaluations during typing.
- Prefer lazy loading of parser-heavy optional panels.

---

## Example fixture format

For function examples, support lightweight fixture metadata:

```yaml
formula: "=SUM(A1:A3)"
inputs:
  - { sheet: "Sheet1", row: 1, col: 1, value: 1 }
  - { sheet: "Sheet1", row: 2, col: 1, value: 2 }
  - { sheet: "Sheet1", row: 3, col: 1, value: 3 }
expect:
  value: 6
```

This can be embedded in MDX frontmatter or generated artifacts.

---

## Integration with function reference pages

Function pages can auto-render one or more sandbox cards:

- simple scalar example
- range example
- error/edge-case example

Reference page generation can map formula examples to sandbox props automatically.

---

## Testing strategy

1. Component unit tests for rendering and state transitions
2. E2E tests for formula evaluation correctness in browser runtime
3. Snapshot/regression tests for generated reference pages with sandbox embeds

---

## Future expansions

- parser explorer tab (tokens + AST + normalized form)
- mini-grid workbook sandbox
- shareable links encoding formula + inputs
- benchmarking pane for heavy formulas

---

## Implementation notes

- Keep sandbox code isolated in `docs-site/components/sandbox/*`
- Provide a single WASM loader utility to prevent duplicate initialization logic
- Avoid coupling docs rendering to engine internals beyond public wasm API surface
