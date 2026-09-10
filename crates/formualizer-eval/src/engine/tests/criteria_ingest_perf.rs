//! Opt-in resource probe; does not run in ordinary correctness suites.
use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, DefaultFunctionContext, FunctionProvider};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use std::time::Instant;

fn rss() -> String {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
#[ignore = "million-cell criteria load/cold/repeated resource probe"]
fn criteria_ingest_resource_probe() {
    let rows: u32 = std::env::var("CRITERIA_PROBE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let mixed = std::env::var("CRITERIA_PROBE_MIXED").is_ok();
    let start = Instant::now();
    let mut config = arrow_eval_config();
    config.enable_parallel = false;
    let mut engine = Engine::new(TestWorkbook::new(), config);
    {
        let mut ingest = engine.begin_bulk_ingest_arrow();
        ingest.add_sheet("S", 1, 4096);
        for i in 0..rows {
            let value = if mixed && i % 4 == 0 {
                LiteralValue::Text("123x".into())
            } else if mixed && i % 4 == 1 {
                LiteralValue::Empty
            } else if mixed && i % 4 == 2 {
                LiteralValue::Boolean(true)
            } else {
                LiteralValue::Number(i as f64)
            };
            ingest.append_row("S", &[value]).unwrap();
        }
        ingest.finish().unwrap();
    }
    eprintln!(
        "load mixed={mixed} rows={rows} elapsed={:?} {}",
        start.elapsed(),
        rss()
    );
    let range = ASTNode::new(
        ASTNodeType::Reference {
            original: String::new(),
            reference: ReferenceType::range(
                Some("S".into()),
                Some(1),
                Some(1),
                Some(rows),
                Some(1),
            ),
        },
        None,
    );
    let patterns = if std::env::var("CRITERIA_PROBE_WILDCARD_FIRST").is_ok() {
        ["*", ""]
    } else {
        ["", "*"]
    };
    if let Ok(formulas) = std::env::var("CRITERIA_PROBE_FORMULAS") {
        let formulas: u32 = formulas.parse().unwrap();
        for row in 1..=formulas {
            let ast =
                formualizer_parse::parser::parse(format!("=COUNTIF(A1:A{rows},\"*\")")).unwrap();
            engine.set_cell_formula("S", row, 2, ast).unwrap();
        }
        crate::engine::eval::criteria_mask_test_hooks::take_mask_work();
        let start = Instant::now();
        engine.evaluate_all().unwrap();
        let work = crate::engine::eval::criteria_mask_test_hooks::take_mask_work();
        for row in 1..=formulas {
            assert_eq!(
                engine.get_cell_value("S", row, 2),
                Some(LiteralValue::Number(rows as f64))
            );
        }
        eprintln!(
            "shared-formulas count={formulas} elapsed={:?} masks={work:?} {}",
            start.elapsed(),
            rss()
        );
    }
    for pattern in patterns {
        let criterion = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Text(pattern.into())),
            None,
        );
        let function = engine.get_function("", "COUNTIF").unwrap();
        for run in 0..4 {
            let start = Instant::now();
            let interp = crate::interpreter::Interpreter::new(&engine, "S");
            let args = [
                ArgumentHandle::new(&range, &interp),
                ArgumentHandle::new(&criterion, &interp),
            ];
            let ctx = DefaultFunctionContext::new_with_sheet(&engine, None, "S");
            let result = function.dispatch(&args, &ctx).unwrap();
            let expected = if pattern == "*" {
                rows as f64
            } else if mixed {
                ((rows + 2) / 4) as f64
            } else {
                0.0
            };
            assert_eq!(result, LiteralValue::Number(expected));
            eprintln!(
                "query pattern={pattern:?} run={run} elapsed={:?} {}",
                start.elapsed(),
                rss()
            );
        }
    }
}
