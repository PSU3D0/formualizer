# formualizer-macros

`formualizer-macros` bundles the procedural macros that power the
Formualizer spreadsheet engine. Today it exposes the `func_caps!` macro used to
annotate built-in function implementations with capability flags consumed by the
engine planner.

> **Note:** This crate is an internal implementation detail of the Formualizer
> workspace. It is published only to satisfy dependency resolution for other
> `formualizer-*` crates, and its APIs may change without notice outside the
> workspace. External consumers typically do not need to depend on it directly.

## What it provides

- **Capability synthesis** – generate `Function::caps()` bodies from a tidy list
  of capability identifiers.
- **Compile-time validation** – fail fast when an unknown capability flag is
  used or required traits are missing.
- **Shared across crates** – reused by `formualizer-eval` and any downstream
  crate that implements Formualizer built-ins.

## Example

```rust,ignore
use formualizer_eval::function::Function;
use formualizer_macros::func_caps;

struct Sum;

impl Function for Sum {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK);

    fn name(&self) -> &'static str { "SUM" }
    // Implement invoke() and other trait members here.
}
```

## When to depend on it

Consumers rarely need to reference `formualizer-macros` directly; it is pulled
in automatically by `formualizer-eval`. Add an explicit dependency if you are
crafting custom evaluation crates or extending Formualizer with your own
built-ins outside of the main workspace.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
