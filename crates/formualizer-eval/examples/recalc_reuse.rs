//! Deterministic legacy recalculation probe. Default execution is untimed.
//! `--time` measures edit + evaluation batches, excluding fixture construction and output.
//! Build without `benchmark_internal` for timing, with it for mechanism observations.

use formualizer_common::LiteralValue;
use formualizer_eval::engine::{Engine, EvalConfig, EvaluationTarget, FormulaPlaneMode};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use std::hint::black_box;

type TestEngine = Engine<TestWorkbook>;

fn make_engine(depth: u32, alternating: bool, targeted: bool) -> TestEngine {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            formula_plane_mode: FormulaPlaneMode::Off,
            enable_parallel: false,
            ..EvalConfig::default()
        },
    );
    for branch in 0..if alternating { 2 } else { 1 } {
        let input_col = 1 + branch * 2;
        let formula_col = input_col + 1;
        let input = char::from(b'A' + (input_col - 1) as u8);
        let output = char::from(b'A' + (formula_col - 1) as u8);
        engine
            .set_cell_value("Sheet1", 1, input_col, LiteralValue::Int(1))
            .unwrap();
        for row in 1..=depth {
            let formula = if row == 1 {
                format!("={input}1+1")
            } else {
                format!("={output}{}+1", row - 1)
            };
            engine
                .set_cell_formula("Sheet1", row, formula_col, parse(formula).unwrap())
                .unwrap();
        }
    }
    if targeted {
        engine
            .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(10))
            .unwrap();
        for (col, formula) in [(4, format!("=B{depth}+C1")), (5, "=D1+1".to_string())] {
            engine
                .set_cell_formula("Sheet1", 1, col, parse(formula).unwrap())
                .unwrap();
        }
    }
    engine
}

fn run(scenario: &str, depth: u32, repeats: usize, timed: bool) {
    let alternating = scenario == "alternating";
    let targeted = matches!(scenario, "late-target" | "late-plan" | "clean-target");
    let mut engine = make_engine(depth, alternating, targeted);
    let targets = [EvaluationTarget::Cell {
        sheet: "Sheet1".to_string(),
        row: 1,
        col: 5,
    }];
    let count = if scenario == "cold" { 1 } else { repeats };
    if scenario != "cold" {
        engine.evaluate_all().unwrap();
    }
    let plan =
        (scenario == "late-plan").then(|| engine.build_recalc_plan_for_targets(&targets).unwrap());
    #[cfg(feature = "benchmark_internal")]
    engine.reset_recalc_reuse_probe();

    let start = timed.then(std::time::Instant::now);
    let mut computed = 0;
    for iteration in 0..count {
        let value = 2 + (iteration % 2) as i64;
        match scenario {
            "same" => engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(value))
                .unwrap(),
            "alternating" => engine
                .set_cell_value(
                    "Sheet1",
                    1,
                    1 + (iteration % 2) as u32 * 2,
                    LiteralValue::Int(2 + (iteration / 2 % 2) as i64),
                )
                .unwrap(),
            "late-target" | "late-plan" => engine
                .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(value))
                .unwrap(),
            "cold" | "noop" | "clean-target" => {}
            _ => panic!("unknown scenario: {scenario}"),
        }
        let result = if let Some(plan) = plan.as_ref() {
            engine.evaluate_recalc_plan(plan)
        } else if targeted {
            engine.evaluate_targets(&targets)
        } else {
            engine.evaluate_all()
        }
        .unwrap();
        computed += black_box(result.computed_vertices);
    }
    let elapsed_ns = start.map(|start| start.elapsed().as_nanos());
    let expected_computed = match scenario {
        "noop" | "clean-target" => 0,
        "late-target" | "late-plan" => count * 2,
        _ => count * depth as usize,
    };
    assert_eq!(computed, expected_computed);
    let result = engine.get_cell_value(
        "Sheet1",
        if targeted { 1 } else { depth },
        if targeted { 5 } else { 2 },
    );
    if scenario == "same" {
        assert_eq!(
            result,
            Some(LiteralValue::Number(
                f64::from(depth) + 2.0 + ((count - 1) % 2) as f64
            ))
        );
    }
    if matches!(scenario, "late-target" | "late-plan") {
        assert_eq!(
            result,
            Some(LiteralValue::Number(
                f64::from(depth) + 4.0 + ((count - 1) % 2) as f64
            ))
        );
    }
    println!(
        "scenario={scenario} depth={depth} repeats={count} computed={computed} result={result:?} edit_eval_ns={elapsed_ns:?}"
    );
    #[cfg(feature = "benchmark_internal")]
    {
        let probe = engine.recalc_reuse_probe();
        match scenario {
            "same" => assert_eq!(probe.schedule_cache_hits, count),
            "alternating" | "cold" => assert_eq!(probe.schedule_cache_misses, count),
            "late-target" | "late-plan" => {
                assert_eq!(probe.schedule_requests, 0);
                assert_eq!(probe.demand_vertices, count * (depth as usize + 4));
                assert_eq!(probe.target_schedule_builds, count);
            }
            "clean-target" => {
                assert_eq!(probe.demand_vertices, count * (depth as usize + 4));
                assert_eq!(probe.target_schedule_builds, 0);
            }
            "noop" => assert_eq!(probe.schedule_requests, 0),
            _ => unreachable!(),
        }
        println!("probe={probe:?}");
    }
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let timed = args.iter().any(|arg| arg == "--time");
    assert!(
        !timed || !cfg!(feature = "benchmark_internal"),
        "timing requires a build without mechanism instrumentation"
    );
    let positional = args
        .iter()
        .filter(|arg| arg.as_str() != "--time")
        .collect::<Vec<_>>();
    let scenario = positional.first().map_or("all", |arg| arg.as_str());
    let depth = positional
        .get(1)
        .map_or(256, |arg| arg.parse::<u32>().unwrap());
    let repeats = positional
        .get(2)
        .map_or(4, |arg| arg.parse::<usize>().unwrap());
    assert!(depth > 0 && repeats > 0);
    if scenario == "all" {
        for scenario in [
            "cold",
            "same",
            "alternating",
            "late-target",
            "late-plan",
            "clean-target",
            "noop",
        ] {
            run(scenario, depth, repeats, timed);
        }
    } else {
        run(scenario, depth, repeats, timed);
    }
}
