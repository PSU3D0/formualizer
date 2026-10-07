//! `Engine::bulk_set_formulas` must dirty the readers of the cells it writes,
//! exactly like `Engine::set_cell_formula` does for a single cell. Each
//! scenario runs through both APIs and through every recalculation route.

use std::sync::Arc;

use crate::engine::{Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

type TestEngine = Engine<TestWorkbook>;

#[derive(Clone, Copy, Debug)]
enum Api {
    Bulk,
    Single,
}

#[derive(Clone, Copy, Debug)]
enum Route {
    All,
    Until,
    Plan,
    Cell,
}

const APIS: [Api; 2] = [Api::Bulk, Api::Single];
const ROUTES: [Route; 4] = [Route::All, Route::Until, Route::Plan, Route::Cell];
/// Grouped (family) evaluation of copied-down formulas, on and off.
const FAMILY_EXECUTION: [bool; 2] = [true, false];

type Edit = (u32, u32, &'static str);
type Check = (&'static str, u32, u32, f64);

fn engine(family_execution: bool) -> TestEngine {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig {
            family_execution,
            ..EvalConfig::default()
        },
    )
}

fn formula(engine: &mut TestEngine, sheet: &str, row: u32, col: u32, src: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(src).unwrap())
        .unwrap();
}

fn value(engine: &mut TestEngine, sheet: &str, row: u32, col: u32, v: f64) {
    engine
        .set_cell_value(sheet, row, col, LiteralValue::Number(v))
        .unwrap();
}

fn apply(engine: &mut TestEngine, api: Api, sheet: &str, edits: &[Edit]) {
    match api {
        Api::Bulk => {
            let items: Vec<_> = edits
                .iter()
                .map(|(row, col, src)| (*row, *col, parse(src).unwrap()))
                .collect();
            assert_eq!(engine.bulk_set_formulas(sheet, items).unwrap(), edits.len());
        }
        Api::Single => {
            for (row, col, src) in edits {
                formula(engine, sheet, *row, *col, src);
            }
        }
    }
}

fn recalc(engine: &mut TestEngine, route: Route, checks: &[Check]) {
    let targets: Vec<(&str, u32, u32)> = checks.iter().map(|(s, r, c, _)| (*s, *r, *c)).collect();
    match route {
        Route::All => {
            engine.evaluate_all().unwrap();
        }
        Route::Until => {
            engine.evaluate_until(&targets).unwrap();
        }
        Route::Plan => {
            let plan = engine.build_recalc_plan().unwrap();
            engine.evaluate_recalc_plan(&plan).unwrap();
        }
        Route::Cell => {
            for (sheet, row, col) in targets {
                engine.evaluate_cell(sheet, row, col).unwrap();
            }
        }
    }
}

fn number(engine: &TestEngine, sheet: &str, row: u32, col: u32) -> Option<f64> {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => Some(n),
        Some(LiteralValue::Int(n)) => Some(n as f64),
        _ => None,
    }
}

/// Runs setup, a full evaluation, the edit, and a recalculation for every
/// API x route x family-execution setting, and collects every mismatch.
fn run_matrix(setup: fn(&mut TestEngine), sheet: &str, edits: &[Edit], checks: &[Check]) {
    let mut failures = Vec::new();
    for family in FAMILY_EXECUTION {
        for api in APIS {
            for route in ROUTES {
                let mut engine = engine(family);
                setup(&mut engine);
                engine.evaluate_all().unwrap();
                apply(&mut engine, api, sheet, edits);
                recalc(&mut engine, route, checks);
                for &(s, r, c, want) in checks {
                    let got = number(&engine, s, r, c);
                    if got != Some(want) {
                        failures.push(format!(
                            "family={family}/{api:?}/{route:?}: {s}!R{r}C{c} expected {want}, got {got:?}"
                        ));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "stale values:\n{}",
        failures.join("\n")
    );
}

#[test]
fn bulk_set_formulas_dirties_scalar_dependent() {
    run_matrix(
        |e| {
            formula(e, "Sheet1", 1, 1, "=1");
            formula(e, "Sheet1", 1, 2, "=A1+1");
        },
        "Sheet1",
        &[(1, 1, "=5")],
        &[("Sheet1", 1, 1, 5.0), ("Sheet1", 1, 2, 6.0)],
    );
}

#[test]
fn bulk_set_formulas_dirties_transitive_chain() {
    run_matrix(
        |e| {
            formula(e, "Sheet1", 1, 1, "=1");
            formula(e, "Sheet1", 1, 2, "=A1+1");
            formula(e, "Sheet1", 1, 3, "=B1*2");
        },
        "Sheet1",
        &[(1, 1, "=5")],
        &[("Sheet1", 1, 2, 6.0), ("Sheet1", 1, 3, 12.0)],
    );
}

#[test]
fn bulk_set_formulas_dirties_range_reader() {
    run_matrix(
        |e| {
            formula(e, "Sheet1", 1, 1, "=1");
            formula(e, "Sheet1", 2, 1, "=2");
            formula(e, "Sheet1", 3, 1, "=3");
            formula(e, "Sheet1", 1, 4, "=SUM(A1:A3)");
        },
        "Sheet1",
        &[(1, 1, "=5")],
        &[("Sheet1", 1, 4, 10.0)],
    );
}

#[test]
fn bulk_set_formulas_dirties_cross_sheet_reader() {
    run_matrix(
        |e| {
            e.add_sheet("Sheet2").unwrap();
            formula(e, "Sheet1", 1, 1, "=1");
            formula(e, "Sheet2", 1, 1, "=Sheet1!A1+1");
        },
        "Sheet1",
        &[(1, 1, "=5")],
        &[("Sheet2", 1, 1, 6.0)],
    );
}

#[test]
fn bulk_set_formulas_over_value_cell_dirties_reader() {
    run_matrix(
        |e| {
            value(e, "Sheet1", 1, 1, 1.0);
            formula(e, "Sheet1", 1, 2, "=A1+1");
        },
        "Sheet1",
        &[(1, 1, "=5")],
        &[("Sheet1", 1, 1, 5.0), ("Sheet1", 1, 2, 6.0)],
    );
}

#[test]
fn bulk_set_formulas_replacing_formula_dirties_reader() {
    run_matrix(
        |e| {
            value(e, "Sheet1", 1, 5, 1.0);
            value(e, "Sheet1", 2, 5, 9.0);
            formula(e, "Sheet1", 1, 1, "=E1");
            formula(e, "Sheet1", 1, 2, "=A1+1");
        },
        "Sheet1",
        &[(1, 1, "=E2*2")],
        &[("Sheet1", 1, 1, 18.0), ("Sheet1", 1, 2, 19.0)],
    );
}

#[test]
fn bulk_set_formulas_of_several_cells_dirties_all_readers() {
    run_matrix(
        |e| {
            formula(e, "Sheet1", 1, 1, "=1");
            formula(e, "Sheet1", 2, 1, "=2");
            formula(e, "Sheet1", 3, 1, "=3");
            formula(e, "Sheet1", 1, 2, "=A1+A2+A3");
            formula(e, "Sheet1", 1, 4, "=SUM(A1:A3)");
        },
        "Sheet1",
        &[(1, 1, "=10"), (2, 1, "=20"), (3, 1, "=A1+A2")],
        &[
            ("Sheet1", 3, 1, 30.0),
            ("Sheet1", 1, 2, 60.0),
            ("Sheet1", 1, 4, 60.0),
        ],
    );
}

#[test]
fn bulk_set_formulas_inside_formula_family_dirties_readers() {
    run_matrix(
        |e| {
            let mut records = Vec::new();
            for row in 1..=200u32 {
                value(e, "Sheet1", row, 1, row as f64);
                let src = format!("=A{row}*2");
                let ast_id = e.intern_formula_ast(&parse(&src).unwrap());
                records.push(FormulaIngestRecord::new(
                    row,
                    2,
                    ast_id,
                    Some(Arc::<str>::from(src.as_str())),
                ));
            }
            e.ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
                .unwrap();
            formula(e, "Sheet1", 100, 3, "=B100+1");
            formula(e, "Sheet1", 1, 4, "=SUM(B1:B200)");
        },
        "Sheet1",
        &[(100, 2, "=A100*5")],
        &[
            ("Sheet1", 100, 2, 500.0),
            ("Sheet1", 100, 3, 501.0),
            ("Sheet1", 1, 4, 40500.0),
        ],
    );
}

#[test]
fn bulk_set_formulas_inside_deferred_dirty_scope_dirties_readers() {
    for family in FAMILY_EXECUTION {
        let mut engine = engine(family);
        formula(&mut engine, "Sheet1", 1, 1, "=1");
        formula(&mut engine, "Sheet1", 1, 2, "=A1+1");
        engine.evaluate_all().unwrap();

        engine.begin_deferred_dirty();
        engine
            .bulk_set_formulas("Sheet1", vec![(1, 1, parse("=5").unwrap())])
            .unwrap();
        engine.end_deferred_dirty();
        engine.evaluate_all().unwrap();

        assert_eq!(
            number(&engine, "Sheet1", 1, 2),
            Some(6.0),
            "family={family}"
        );
    }
}
