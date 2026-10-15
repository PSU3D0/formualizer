//! Every `f64` reduction over cell data goes through
//! `engine::eval::exact` (`CompensatedSum`, `CompensatedProduct` and the
//! `compensated_*` helpers). A plain `Iterator::sum`/`product` over `f64`,
//! or a call to Arrow's `sum` kernel, drifts with the row count and
//! reintroduces the bug `compensated_summation` pins. This scan fails the
//! build on any such site in the builtins or the evaluation kernels unless
//! the line, or the line above it, carries a `// bounded-sum:` comment
//! explaining why the term count cannot grow with the data (for example a
//! sum over regression coefficients).

use std::path::Path;

const SCANNED_DIRS: &[&str] = &["src/builtins", "src/engine/eval"];
const EXEMPT_FILES: &[&str] = &["src/engine/eval/exact.rs"];
const JUSTIFICATION: &str = "// bounded-sum:";

/// Substrings that mark a plain floating-point reduction.
const PLAIN_REDUCTIONS: &[&str] = &[
    ".sum::<f64>()",
    ".product::<f64>()",
    "kernels::aggregate::sum",
    "aggregate::sum_array",
];

/// `let name: f64 = <expr>.sum();` or `.product();` — the type ascription
/// is on the same line as the reduction.
fn is_typed_f64_fold(line: &str) -> bool {
    line.contains(": f64 =") && (line.contains(".sum()") || line.contains(".product()"))
}

fn is_test_path(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "tests")
        || path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with("_tests.rs"))
}

fn scan_file(path: &Path, offenders: &mut Vec<String>) {
    let source = std::fs::read_to_string(path).unwrap();
    let lines: Vec<&str> = source.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        let plain =
            PLAIN_REDUCTIONS.iter().any(|needle| line.contains(needle)) || is_typed_f64_fold(line);
        if !plain {
            continue;
        }
        let justified = line.contains(JUSTIFICATION)
            || index
                .checked_sub(1)
                .is_some_and(|previous| lines[previous].contains(JUSTIFICATION));
        if !justified {
            offenders.push(format!(
                "{}:{}: {}",
                path.strip_prefix(env!("CARGO_MANIFEST_DIR"))
                    .unwrap()
                    .display(),
                index + 1,
                line.trim()
            ));
        }
    }
}

fn visit(path: &Path, offenders: &mut Vec<String>) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            visit(&path, offenders);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs")
            && !is_test_path(&path)
            && !EXEMPT_FILES.iter().any(|exempt| path.ends_with(exempt))
        {
            scan_file(&path, offenders);
        }
    }
}

#[test]
fn floating_point_reductions_are_compensated_or_justified() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offenders = Vec::new();
    for dir in SCANNED_DIRS {
        visit(&root.join(dir), &mut offenders);
    }
    offenders.sort();
    assert!(
        offenders.is_empty(),
        "plain f64 reductions outside engine::eval::exact — use CompensatedSum / \
         compensated_sum / compensated_product, or add a `{JUSTIFICATION}` comment \
         on or above the line explaining why the term count is bounded:\n  {}",
        offenders.join("\n  ")
    );
}

/// The scan must actually see through the patterns it claims to catch.
#[test]
fn guard_recognizes_every_plain_reduction_shape() {
    for line in [
        "let total = values.iter().sum::<f64>();",
        "let total = values.iter().copied().product::<f64>();",
        "let total: f64 = values.iter().map(|x| x * x).sum();",
        "let total: f64 = values.iter().product();",
        "total += arrow::compute::kernels::aggregate::sum(col.as_ref()).unwrap_or(0.0);",
        "if let Some(s) = sum_array::<Float64Type, _>(arr) {",
    ] {
        let plain = PLAIN_REDUCTIONS.iter().any(|needle| line.contains(needle))
            || is_typed_f64_fold(line)
            || line.contains("sum_array::<");
        assert!(plain, "not recognized: {line}");
    }
    for line in [
        "let count: usize = rows.iter().map(Vec::len).sum();",
        "let n = runs.iter().map(|r| r.len as u64).sum::<u64>();",
    ] {
        let plain =
            PLAIN_REDUCTIONS.iter().any(|needle| line.contains(needle)) || is_typed_f64_fold(line);
        assert!(!plain, "false positive: {line}");
    }
}
