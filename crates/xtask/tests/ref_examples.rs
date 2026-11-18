use std::path::Path;
use std::process::Command;

#[test]
fn all_function_examples_match_generated_pages() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    // The read-only ref command evaluates every legacy and authored example,
    // checks case coverage and syntax, and compares the results with the pages.
    let outcome = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("ref")
        .current_dir(workspace)
        .output()
        .expect("run the reference generator");
    assert!(
        outcome.status.success(),
        "Function reference differs from the engine.\n{}\n{}",
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
}
