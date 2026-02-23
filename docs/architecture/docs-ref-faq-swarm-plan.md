# Docs-ref FAQ Swarm Plan (Tranche 4)

## Objective

Populate high-value, function-specific FAQ metadata in Rust docstrings using `yaml,docs` blocks so generated function pages can render a useful `## FAQ` section without generic boilerplate.

## Source of truth

- Rust builtin docstrings under `crates/formualizer-eval/src/builtins/**`
- Structured block:

```yaml,docs
related:
  - XLOOKUP
  - XMATCH
faq:
  - q: "When does this return #N/A?"
    a: "..."
  - q: "How does match mode affect behavior?"
    a: "..."
```

## Quality rules

- 1–3 FAQ entries per function.
- Each FAQ should be function-specific and behavior-focused.
- At least one FAQ should cover error/edge behavior where applicable.
- Avoid copy/paste Q/A text reused across many functions.
- Keep answers concise and concrete.

## Hard execution constraints (all waves)

1. Edit Rust files only in assigned scope.
2. No docs regeneration commands.
3. No test/check command execution.
4. No script-based bulk rewrites (including Python scripts).
5. No subagent re-dispatch.
6. Preserve existing schema marker blocks unchanged.

## Wave plan (max 3 parallel agents)

### Wave 1
- Shard A: `stats` + `math`
- Shard B: `lookup` + `reference-fns`
- Shard C: `text` + `logical` + `info`

### Wave 2
- Shard A: `financial` + `engineering`
- Shard B: `datetime` + `database`
- Shard C: `lambda` + `random` + cleanup leftovers

## Review gates between waves

After each wave:
1. Run `cargo check -p formualizer-eval` (integration owner only)
2. Run `cargo run -p xtask -- docs-ref --apply --allow-dirty` (integration owner only)
3. Spot-check generated FAQ quality on representative pages
4. Track duplication risk (same Q/A repeated too broadly)

## Acceptance criteria for tranche

- High-traffic categories (`stats`, `math`, `lookup`, `text`) have FAQ coverage with meaningful answers.
- Generated pages show useful `## FAQ` sections with low duplication.
- Build passes for docs site after regeneration.
