# Builtin Docs Structured Metadata Spec (`yaml,docs`)

## Goal

Define a single, colocated source of truth in Rust docstrings for function-level narrative metadata that powers generated docs pages:

- concise summary + remarks + examples (already used)
- **related functions**
- **FAQ entries**

This keeps content near implementation while allowing deterministic docs generation via `xtask docs-ref`.

---

## Placement

In function doc comments, add a fenced `yaml,docs` block **after** examples and **before** schema markers.

```rust
/// # Examples
///
/// ```yaml,sandbox
/// title: "..."
/// formula: "=..."
/// expected: ...
/// ```
///
/// ```yaml,docs
/// related:
///   - SUM
///   - SUMIFS
/// faq:
///   - q: "When does this function return #DIV/0!?"
///     a: "..."
///   - q: "Is this two-tailed or one-tailed?"
///     a: "..."
/// ```
///
/// [formualizer-docgen:schema:start]
```

---

## YAML shape

```yaml
yaml,docs
related:         # optional list of function names (canonical or alias)
  - SUM
  - SUMIFS
faq:             # optional list (max 3 recommended)
  - q: "Question text"
    a: "Answer text"
  - q: "Question text"
    a: "Answer text"
```

### Validation rules

- `related`
  - optional
  - uppercased at generation time
  - de-duplicated
  - must resolve to known function names/aliases (unknown values dropped with warning)
- `faq`
  - optional
  - each item requires non-empty `q` and `a`
  - max 3 entries per function (soft warning over limit)
  - answers should be concise (recommended <= 280 chars)

---

## Generation behavior (`xtask docs-ref`)

### Related functions

Final "Related functions" list is built as:

1. explicit `related` entries from `yaml,docs`
2. deterministic auto-derived neighbors:
   - same category
   - signature/cap similarity
   - known pair patterns (e.g. `COUNT`/`COUNTA`, `XLOOKUP`/`XMATCH`)
3. de-dup + remove self

### FAQ section

- Render FAQ only when at least one valid FAQ entry exists.
- Use page heading: `## FAQ`.
- Render as simple markdown Q/A pairs.

Example render:

```md
## FAQ

### When does F.TEST return #DIV/0!?
It returns `#DIV/0!` when either input set has fewer than two numeric values or zero variance.
```

### FAQ JSON-LD (optional gate)

Only emit FAQ schema when:

- at least 2 FAQ entries
- all entries pass quality checks
- function page is not flagged as low-content

---

## Quality guardrails for swarm authoring

Subagents should follow these constraints:

- No generic FAQs like “What does X do?” unless answer is function-specific.
- At least one FAQ should cover edge behavior or error semantics.
- Prefer behaviorally meaningful related functions (not random same-category entries).
- Avoid copy/paste FAQ text reused across many functions.

### Lint checks to add in `docs-audit`

- flag duplicate FAQ question+answer pairs reused above threshold
- flag FAQ answers with placeholder terms (`TODO`, `TBD`, `coming soon`)
- flag unknown `related` function names
- flag too-short answers (e.g. `< 20 chars`)

---

## Backward compatibility

- Existing docs without `yaml,docs` continue to render.
- `related` can be fully auto-generated when metadata absent.
- FAQ section is omitted if no valid entries exist.

---

## Rollout plan

1. Implement parser support for `yaml,docs` in `xtask docs-ref`.
2. Add rendering blocks to generated MDX:
   - `## Related functions`
   - `## FAQ` (conditional)
3. Add lint checks in `xtask docs-audit`.
4. Start subagent swarm enrichment by category.
5. Regenerate docs and review quality sample before full merge.
