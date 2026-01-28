//! JSON-driven formula test runner.
//!
//! This module loads JSON test files from `tests/formula_tests/` and runs each
//! formula through the evaluator, comparing results. It gathers ALL failures
//! before reporting, rather than failing on the first mismatch.

#[cfg(test)]
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue, parse_a1_1based};
#[cfg(test)]
use formualizer_parse::Tokenizer;
#[cfg(test)]
use formualizer_parse::parser::Parser;
#[cfg(test)]
use serde::Deserialize;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::Path;

/// Represents a single test case from the JSON file.
#[cfg(test)]
#[derive(Debug, Deserialize)]
struct TestCase {
    formula: String,
    result: serde_json::Value,
    #[serde(default)]
    result_type: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    context: Option<serde_json::Value>,
}

/// Represents a test file containing multiple test cases.
#[cfg(test)]
#[derive(Debug, Deserialize)]
struct TestFile {
    name: String,
    #[serde(default)]
    generated: Option<String>,
    #[serde(default)]
    generator: Option<String>,
    #[serde(default)]
    context_data: Option<serde_json::Value>,
    tests: Vec<TestCase>,
}

/// A failure record for reporting.
#[cfg(test)]
#[derive(Debug)]
struct TestFailure {
    file: String,
    formula: String,
    description: String,
    expected: String,
    actual: String,
    error: Option<String>,
}

/// Create a workbook with all built-in functions registered.
#[cfg(test)]
fn create_test_workbook() -> crate::test_workbook::TestWorkbook {
    crate::test_workbook::TestWorkbook::new()
}

#[cfg(test)]
fn json_to_literal(value: &serde_json::Value) -> Option<LiteralValue> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(LiteralValue::Int(i))
            } else {
                n.as_f64().map(LiteralValue::Number)
            }
        }
        serde_json::Value::Bool(b) => Some(LiteralValue::Boolean(*b)),
        serde_json::Value::String(s) => {
            if s.starts_with('#') {
                Some(LiteralValue::Error(ExcelError::from_error_string(s)))
            } else {
                Some(LiteralValue::Text(s.clone()))
            }
        }
        serde_json::Value::Null => Some(LiteralValue::Empty),
        _ => None,
    }
}

#[cfg(test)]
fn apply_context(
    mut wb: crate::test_workbook::TestWorkbook,
    context: &serde_json::Value,
) -> crate::test_workbook::TestWorkbook {
    let Some(map) = context.as_object() else {
        return wb;
    };

    for (cell_ref, value) in map {
        let Ok((row, col, _, _)) = parse_a1_1based(cell_ref) else {
            continue;
        };
        if let Some(literal) = json_to_literal(value) {
            wb = wb.with_cell("Sheet1", row, col, literal);
        }
    }

    wb
}

/// Evaluate a formula string and return the result.
#[cfg(test)]
fn evaluate_formula(
    formula: &str,
    wb: &crate::test_workbook::TestWorkbook,
) -> Result<LiteralValue, String> {
    let tokenizer = Tokenizer::new(formula).map_err(|e| format!("Tokenizer error: {:?}", e))?;
    let mut parser = Parser::new(tokenizer.items, false);
    let ast = parser
        .parse()
        .map_err(|e| format!("Parse error: {}", e.message))?;

    let interpreter = wb.interpreter();
    let cv = interpreter
        .evaluate_ast(&ast)
        .map_err(|e| format!("Eval error: {:?}", e))?;

    Ok(cv.into_literal())
}

/// Compare a LiteralValue to an expected JSON value.
#[cfg(test)]
fn compare_result(
    actual: &LiteralValue,
    expected: &serde_json::Value,
    result_type: Option<&str>,
) -> bool {
    let _ = result_type; // Unused for now, but may be useful later
    compare_literal_json(actual, expected)
}

#[cfg(test)]
fn compare_literal_json(actual: &LiteralValue, expected: &serde_json::Value) -> bool {
    match expected {
        serde_json::Value::Number(n) => {
            let expected_num = n.as_f64().unwrap();
            match actual {
                LiteralValue::Number(actual_num) => {
                    // Use epsilon comparison for floating point
                    (actual_num - expected_num).abs() < 1e-9
                        || (expected_num != 0.0
                            && ((actual_num - expected_num) / expected_num).abs() < 1e-9)
                }
                LiteralValue::Int(actual_int) => (*actual_int as f64 - expected_num).abs() < 1e-9,
                _ => false,
            }
        }
        serde_json::Value::Bool(expected_bool) => {
            matches!(actual, LiteralValue::Boolean(actual_bool) if actual_bool == expected_bool)
        }
        serde_json::Value::String(expected_str) => {
            if expected_str.starts_with('#') {
                if let Some(expected_kind) = parse_error_kind_prefix(expected_str) {
                    matches!(actual, LiteralValue::Error(e) if e.kind == expected_kind)
                } else {
                    matches!(actual, LiteralValue::Error(e) if e.to_string() == *expected_str)
                }
            } else {
                matches!(actual, LiteralValue::Text(actual_str) if actual_str == expected_str)
            }
        }
        serde_json::Value::Null => matches!(actual, LiteralValue::Empty),
        serde_json::Value::Array(_) => compare_array(actual, expected),
        _ => false,
    }
}

#[cfg(test)]
fn compare_array(actual: &LiteralValue, expected: &serde_json::Value) -> bool {
    let actual_rows = match actual {
        LiteralValue::Array(rows) => rows,
        _ => return false,
    };

    let expected_rows = match expected {
        serde_json::Value::Array(rows) => rows,
        _ => return false,
    };

    let expected_matrix: Vec<Vec<&serde_json::Value>> = if expected_rows
        .iter()
        .all(|row| matches!(row, serde_json::Value::Array(_)))
    {
        expected_rows
            .iter()
            .map(|row| row.as_array().unwrap().iter().collect())
            .collect()
    } else {
        vec![expected_rows.iter().collect()]
    };

    if actual_rows.len() != expected_matrix.len() {
        return false;
    }

    for (row_idx, expected_row) in expected_matrix.iter().enumerate() {
        let actual_row = &actual_rows[row_idx];
        if actual_row.len() != expected_row.len() {
            return false;
        }
        for (col_idx, expected_cell) in expected_row.iter().enumerate() {
            if !compare_literal_json(&actual_row[col_idx], expected_cell) {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
fn parse_error_kind_prefix(value: &str) -> Option<ExcelErrorKind> {
    let trimmed = value.trim();
    if !trimmed.starts_with('#') {
        return None;
    }

    let end = trimmed
        .find(|c: char| c == ':' || c == ' ' || c == '(' || c == '[')
        .unwrap_or(trimmed.len());
    ExcelErrorKind::try_parse(&trimmed[..end])
}

/// Format a LiteralValue for display.
#[cfg(test)]
fn format_literal(lit: &LiteralValue) -> String {
    match lit {
        LiteralValue::Number(n) => format!("{}", n),
        LiteralValue::Int(i) => format!("{}", i),
        LiteralValue::Boolean(b) => format!("{}", b),
        LiteralValue::Text(s) => format!("\"{}\"", s),
        LiteralValue::Error(e) => format!("{}", e),
        LiteralValue::Empty => "empty".to_string(),
        LiteralValue::Array(arr) => {
            let rows: Vec<String> = arr
                .iter()
                .map(|row| {
                    let cells: Vec<String> = row.iter().map(format_literal).collect();
                    cells.join(",")
                })
                .collect();
            format!("{{{}}}", rows.join(";"))
        }
        LiteralValue::Date(d) => format!("date:{}", d),
        LiteralValue::DateTime(dt) => format!("datetime:{}", dt),
        LiteralValue::Time(t) => format!("time:{}", t),
        LiteralValue::Duration(dur) => format!("duration:{:?}", dur),
        LiteralValue::Pending => "pending".to_string(),
    }
}

/// Run all formula tests from JSON files.
/// Returns (passed_count, failures).
#[cfg(test)]
fn run_formula_tests(test_dir: &Path) -> (usize, Vec<TestFailure>) {
    let mut passed = 0;
    let mut failures = Vec::new();

    // Initialize function registry
    crate::builtins::load_builtins();

    let pattern = test_dir.join("*.json");
    let pattern_str = pattern.to_string_lossy();

    for entry in glob::glob(&pattern_str).expect("Failed to read glob pattern") {
        let path = match entry {
            Ok(p) => p,
            Err(e) => {
                failures.push(TestFailure {
                    file: "unknown".to_string(),
                    formula: "N/A".to_string(),
                    description: "Failed to read file entry".to_string(),
                    expected: "N/A".to_string(),
                    actual: "N/A".to_string(),
                    error: Some(format!("{}", e)),
                });
                continue;
            }
        };

        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                failures.push(TestFailure {
                    file: file_name,
                    formula: "N/A".to_string(),
                    description: "Failed to read file".to_string(),
                    expected: "N/A".to_string(),
                    actual: "N/A".to_string(),
                    error: Some(format!("{}", e)),
                });
                continue;
            }
        };

        let test_file: TestFile = match serde_json::from_str(&content) {
            Ok(tf) => tf,
            Err(e) => {
                failures.push(TestFailure {
                    file: file_name,
                    formula: "N/A".to_string(),
                    description: "Failed to parse JSON".to_string(),
                    expected: "N/A".to_string(),
                    actual: "N/A".to_string(),
                    error: Some(format!("{}", e)),
                });
                continue;
            }
        };

        for test_case in &test_file.tests {
            let mut wb = create_test_workbook();
            if let Some(context) = test_file.context_data.as_ref() {
                wb = apply_context(wb, context);
            }
            if let Some(context) = test_case.context.as_ref() {
                wb = apply_context(wb, context);
            }
            let description = test_case.description.clone().unwrap_or_default();

            match evaluate_formula(&test_case.formula, &wb) {
                Ok(actual) => {
                    let result_type = test_case.result_type.as_deref();
                    if compare_result(&actual, &test_case.result, result_type) {
                        passed += 1;
                    } else {
                        failures.push(TestFailure {
                            file: file_name.clone(),
                            formula: test_case.formula.clone(),
                            description,
                            expected: format!("{:?}", test_case.result),
                            actual: format_literal(&actual),
                            error: None,
                        });
                    }
                }
                Err(e) => {
                    // Check if the expected result is an error
                    if let serde_json::Value::String(expected_str) = &test_case.result {
                        if expected_str.starts_with('#') && expected_str.ends_with('!') {
                            // We expected an error but got a different error - still a failure
                            failures.push(TestFailure {
                                file: file_name.clone(),
                                formula: test_case.formula.clone(),
                                description,
                                expected: expected_str.clone(),
                                actual: "evaluation error".to_string(),
                                error: Some(e),
                            });
                        } else {
                            failures.push(TestFailure {
                                file: file_name.clone(),
                                formula: test_case.formula.clone(),
                                description,
                                expected: format!("{:?}", test_case.result),
                                actual: "evaluation error".to_string(),
                                error: Some(e),
                            });
                        }
                    } else {
                        failures.push(TestFailure {
                            file: file_name.clone(),
                            formula: test_case.formula.clone(),
                            description,
                            expected: format!("{:?}", test_case.result),
                            actual: "evaluation error".to_string(),
                            error: Some(e),
                        });
                    }
                }
            }
        }
    }

    (passed, failures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Main test function that runs all JSON formula tests.
    /// This gathers ALL failures before reporting.
    #[test]
    fn run_formula_test_suite() {
        // Find the test directory relative to the crate root
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let test_dir = PathBuf::from(manifest_dir)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests")
            .join("formula_tests");

        if !test_dir.exists() {
            eprintln!("Warning: Test directory does not exist: {:?}", test_dir);
            eprintln!("Skipping formula test suite.");
            return;
        }

        let (passed, failures) = run_formula_tests(&test_dir);

        // Report all failures at the end
        if !failures.is_empty() {
            eprintln!("\n=== {} FORMULA TEST FAILURES ===\n", failures.len());
            for (i, failure) in failures.iter().enumerate() {
                eprintln!(
                    "{}. [{}] {}\n   Formula: {}\n   Expected: {}\n   Actual: {}{}",
                    i + 1,
                    failure.file,
                    failure.description,
                    failure.formula,
                    failure.expected,
                    failure.actual,
                    failure
                        .error
                        .as_ref()
                        .map(|e| format!("\n   Error: {}", e))
                        .unwrap_or_default()
                );
            }
            eprintln!(
                "\n=== SUMMARY: {} passed, {} failed ===\n",
                passed,
                failures.len()
            );
            panic!(
                "Formula test suite: {} passed, {} failed",
                passed,
                failures.len()
            );
        }

        println!("\nFormula test suite: {} tests passed", passed);
    }
}
