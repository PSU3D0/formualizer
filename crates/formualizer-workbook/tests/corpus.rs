//! Compatibility corpus harness.
//!
//! Fixture layout:
//! - `tests/corpus/<category>/<fixture>/case.json`
//! - `tests/corpus/<category>/<fixture>/workbook.json` (JsonAdapter schema)
//! - `tests/corpus/<category>/<fixture>/expected.json`
//!
//! Bless mode:
//! - set `FZ_CORPUS_BLESS=1` to rewrite `expected.json` from current outputs.

use formualizer_common::{error::ExcelErrorKind, parse_a1_1based, LiteralValue};
use formualizer_workbook::{LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct CorpusCase {
    id: String,
    category: String,
    #[serde(default)]
    skip: Option<String>,
    #[serde(default = "default_workbook_path")]
    workbook: String,
    targets: Vec<String>,
    #[serde(default)]
    config: Option<CorpusConfig>,
}

#[derive(Debug, Deserialize)]
struct CorpusConfig {
    #[serde(default)]
    workbook_seed: Option<u64>,
    #[serde(default)]
    volatile_level: Option<String>,
}

fn default_workbook_path() -> String {
    "workbook.json".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExpectedSnapshot {
    results: BTreeMap<String, SnapValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
enum SnapValue {
    Number(f64),
    Boolean(bool),
    Text(String),
    Empty,
    Error(String),
    Array(Vec<Vec<SnapValue>>),
}

fn literal_to_snap(v: &LiteralValue) -> SnapValue {
    match v {
        LiteralValue::Number(n) => SnapValue::Number(*n),
        LiteralValue::Int(i) => SnapValue::Number(*i as f64),
        LiteralValue::Boolean(b) => SnapValue::Boolean(*b),
        LiteralValue::Text(s) => SnapValue::Text(s.clone()),
        LiteralValue::Empty => SnapValue::Empty,
        LiteralValue::Error(e) => SnapValue::Error(e.kind.to_string()),
        LiteralValue::Array(rows) => SnapValue::Array(
            rows.iter()
                .map(|r| r.iter().map(literal_to_snap).collect())
                .collect(),
        ),
        LiteralValue::Date(d) => SnapValue::Text(d.to_string()),
        LiteralValue::DateTime(dt) => SnapValue::Text(dt.to_string()),
        LiteralValue::Time(t) => SnapValue::Text(t.to_string()),
        LiteralValue::Duration(d) => SnapValue::Text(format!("{d:?}")),
        LiteralValue::Pending => SnapValue::Error(ExcelErrorKind::Calc.to_string()),
    }
}

fn snap_eq(a: &SnapValue, b: &SnapValue) -> bool {
    match (a, b) {
        (SnapValue::Number(x), SnapValue::Number(y)) => {
            let diff = (x - y).abs();
            diff <= 1e-9 || (*y != 0.0 && (diff / y.abs()) <= 1e-9)
        }
        (SnapValue::Boolean(x), SnapValue::Boolean(y)) => x == y,
        (SnapValue::Text(x), SnapValue::Text(y)) => x == y,
        (SnapValue::Empty, SnapValue::Empty) => true,
        (SnapValue::Error(x), SnapValue::Error(y)) => x == y,
        (SnapValue::Array(x), SnapValue::Array(y)) => {
            if x.len() != y.len() {
                return false;
            }
            for (xr, yr) in x.iter().zip(y.iter()) {
                if xr.len() != yr.len() {
                    return false;
                }
                for (xc, yc) in xr.iter().zip(yr.iter()) {
                    if !snap_eq(xc, yc) {
                        return false;
                    }
                }
            }
            true
        }
        _ => false,
    }
}

fn repo_root_from_manifest() -> PathBuf {
    // crates/formualizer-workbook -> crates -> repo root
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("repo root")
        .to_path_buf()
}

fn discover_cases(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = fs::read_dir(&dir) else {
            continue;
        };
        for ent in rd.flatten() {
            let path = ent.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|n| n == "case.json") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn parse_target(s: &str) -> Result<(String, u32, u32), String> {
    let (sheet, a1) = s
        .split_once('!')
        .ok_or_else(|| format!("target must be Sheet!A1, got {s:?}"))?;
    let (row, col, _, _) = parse_a1_1based(a1).map_err(|e| e.to_string())?;
    Ok((sheet.to_string(), row, col))
}

#[test]
#[cfg(feature = "json")]
fn corpus_smoke() {
    use formualizer_workbook::backends::JsonAdapter;

    let repo_root = repo_root_from_manifest();
    let corpus_root = repo_root.join("tests").join("corpus");
    if !corpus_root.exists() {
        eprintln!("[corpus] missing dir: {} (skipping)", corpus_root.display());
        return;
    }

    let bless = std::env::var("FZ_CORPUS_BLESS")
        .ok()
        .is_some_and(|v| v != "0");

    let cases = discover_cases(&corpus_root);
    assert!(
        !cases.is_empty(),
        "no corpus cases found under tests/corpus"
    );

    for case_path in cases {
        let case_dir = case_path.parent().expect("case dir");
        let case_raw = fs::read_to_string(&case_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", case_path.display()));
        let case: CorpusCase = serde_json::from_str(&case_raw)
            .unwrap_or_else(|e| panic!("parse {}: {e}", case_path.display()));

        if let Some(reason) = &case.skip {
            eprintln!("[corpus] skip {} ({}) - {}", case.id, case.category, reason);
            continue;
        }

        let workbook_path = case_dir.join(&case.workbook);
        let adapter = JsonAdapter::open_path(&workbook_path)
            .unwrap_or_else(|e| panic!("open {}: {e}", workbook_path.display()));

        let mut cfg = WorkbookConfig::ephemeral();
        cfg.eval.defer_graph_building = false;
        if let Some(user_cfg) = &case.config {
            if let Some(seed) = user_cfg.workbook_seed {
                cfg.eval.workbook_seed = seed;
            }
            if let Some(level) = user_cfg.volatile_level.as_deref() {
                cfg.eval.volatile_level = match level {
                    "always" => formualizer_eval::traits::VolatileLevel::Always,
                    "onrecalc" => formualizer_eval::traits::VolatileLevel::OnRecalc,
                    "onopen" => formualizer_eval::traits::VolatileLevel::OnOpen,
                    other => panic!("unknown volatile_level {other:?} in {}", case.id),
                };
            }
        }

        let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg)
            .unwrap_or_else(|e| panic!("load {}: {e}", case.id));
        wb.prepare_graph_all()
            .unwrap_or_else(|e| panic!("prepare_graph_all {}: {e}", case.id));

        let mut actual: BTreeMap<String, SnapValue> = BTreeMap::new();
        for t in &case.targets {
            let (sheet, row, col) = parse_target(t).unwrap_or_else(|e| panic!("{}: {e}", case.id));
            let v = wb
                .evaluate_cell(&sheet, row, col)
                .unwrap_or_else(|e| panic!("eval {} {t}: {e}", case.id));
            actual.insert(t.clone(), literal_to_snap(&v));
        }

        let expected_path = case_dir.join("expected.json");
        let actual_snap = ExpectedSnapshot { results: actual };

        if bless || !expected_path.exists() {
            let json = serde_json::to_string_pretty(&actual_snap)
                .unwrap_or_else(|e| panic!("serialize {}: {e}", case.id));
            fs::write(&expected_path, json + "\n")
                .unwrap_or_else(|e| panic!("write {}: {e}", expected_path.display()));
            eprintln!("[corpus] wrote snapshot: {}", expected_path.display());
            continue;
        }

        let expected_raw = fs::read_to_string(&expected_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", expected_path.display()));
        let expected: ExpectedSnapshot = serde_json::from_str(&expected_raw)
            .unwrap_or_else(|e| panic!("parse {}: {e}", expected_path.display()));

        let mut mismatches: Vec<String> = Vec::new();
        for (k, exp) in &expected.results {
            let Some(act) = actual_snap.results.get(k) else {
                mismatches.push(format!("missing actual for {k}"));
                continue;
            };
            if !snap_eq(act, exp) {
                mismatches.push(format!("{k}: expected={exp:?} actual={act:?}"));
            }
        }
        for k in actual_snap.results.keys() {
            if !expected.results.contains_key(k) {
                mismatches.push(format!("unexpected actual result for {k}"));
            }
        }

        if !mismatches.is_empty() {
            let act_json = serde_json::to_string_pretty(&actual_snap).unwrap_or_default();
            let exp_json = serde_json::to_string_pretty(&expected).unwrap_or_default();
            panic!(
                "corpus mismatch: {} ({})\n{}\n\nexpected.json:\n{}\n\nactual:\n{}\n\n(set FZ_CORPUS_BLESS=1 to update snapshots)",
                case.id,
                case.category,
                mismatches.join("\n"),
                exp_json,
                act_json
            );
        }
    }
}
