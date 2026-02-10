use pyo3_stub_gen::Result;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    // `stub_info` is defined in `src/lib.rs` by `define_stub_info_gatherer!`.
    let stub = formualizer_py::stub_info()?;
    stub.generate()?;

    // Post-process the generated package stub to reflect the public Python API aliases
    // defined in `formualizer/__init__.py`.
    let stub_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "formualizer", "__init__.pyi"]
        .iter()
        .collect();

    let mut contents = fs::read_to_string(&stub_path)?;

    // Idempotent append.
    if !contents.contains("# Backwards compatible Py* aliases") {
        contents.push_str(
            r#"

# Backwards compatible Py* aliases
#
# Historically this package exported most symbols with a `Py...` prefix.
# Keep these aliases so older code continues to type-check.
PyToken = Token
PyTokenizer = Tokenizer
PyTokenizerIter = TokenizerIter
PyRefWalker = RefWalker
PyTokenType = TokenType
PyTokenSubType = TokenSubType
PyFormulaDialect = FormulaDialect
"#,
        );
        fs::write(&stub_path, contents)?;
    }

    Ok(())
}
