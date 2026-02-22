# Builtin Doc Comment Template

Use this template when documenting builtin function implementations in
`crates/formualizer-eval/src/builtins/**`.

```rust
/// One-sentence summary of what the function does.
///
/// # Formula example
/// ```excel
/// # returns: <expected>
/// =FUNCTION_NAME(<args>)
/// ```
///
/// # Rust example
/// ```rust,no_run
/// # use formualizer::doc_examples::eval_scalar;
/// let value = eval_scalar("=FUNCTION_NAME(<args>)")?;
/// // assert specific expected value here
/// # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
/// ```
impl Function for FunctionType {
    // ...
}
```

## Conventions

- Keep the summary short and user-facing.
- Prefer one formula example with concrete expected output.
- Use `formualizer::doc_examples::eval_scalar` to avoid verbose workbook setup.
- If a snippet should not run in doctests yet, keep `no_run`.
- Formula comments can use either `# ...` or `// ...`.
