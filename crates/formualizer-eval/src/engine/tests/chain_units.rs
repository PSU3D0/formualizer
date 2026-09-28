//! Program 3 chain units: a family whose members read the member above
//! (`=A1+1`, `=C2+D1` filled down) is one sequential layer, evaluated in
//! row order through the chain lift (or cell by cell when the lift cannot
//! carry a member exactly). Values must equal an engine that evaluates
//! every formula on its own (`formula_compression = false`, no lift),
//! through value and formula edits.
use super::common::arrow_eval_config;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const ROWS: u32 = 300;

fn engine(compress: bool, parallel: bool) -> Engine<TestWorkbook> {
    // The reference engine evaluates every member through the walk (no
    // lift: a chain unit then runs cell by cell, as per-cell layers do).
    let config = EvalConfig {
        formula_compression: compress,
        family_lift: compress,
        enable_parallel: parallel,
        ..arrow_eval_config()
    };
    let mut e = Engine::new(TestWorkbook::new(), config);
    for r in 1..=ROWS {
        e.set_cell_value(
            "Sheet1",
            r,
            1,
            LiteralValue::Number(f64::from(r % 11) - 3.5),
        )
        .unwrap();
    }
    e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(1.0))
        .unwrap();
    for r in 2..=ROWS {
        // B: a pure recurrence; C: a running total over A; D: scaled chain
        // reading another sheet; E: reads the finished chain.
        e.set_cell_formula("Sheet1", r, 2, parse(format!("=B{}+1", r - 1)).unwrap())
            .unwrap();
    }
    e.set_cell_formula("Sheet1", 1, 3, parse("=A1").unwrap())
        .unwrap();
    for r in 2..=ROWS {
        e.set_cell_formula("Sheet1", r, 3, parse(format!("=A{r}+C{}", r - 1)).unwrap())
            .unwrap();
    }
    for r in 1..=ROWS {
        e.set_cell_value("Data", r, 1, LiteralValue::Number(f64::from(r) * 0.25))
            .unwrap();
    }
    e.set_cell_formula("Sheet1", 1, 4, parse("=Data!A1").unwrap())
        .unwrap();
    for r in 2..=ROWS {
        e.set_cell_formula(
            "Sheet1",
            r,
            4,
            parse(format!("=D{}*0.5-Data!A{r}", r - 1)).unwrap(),
        )
        .unwrap();
    }
    for r in 1..=ROWS {
        e.set_cell_formula("Sheet1", r, 5, parse(format!("=C{r}*2+D{r}")).unwrap())
            .unwrap();
    }
    e
}

fn key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        other => format!("{other:?}"),
    }
}

fn assert_same(a: &Engine<TestWorkbook>, b: &Engine<TestWorkbook>, ctx: &str) {
    for r in 1..=ROWS {
        for c in 1..=5 {
            assert_eq!(
                key(a.get_cell_value("Sheet1", r, c)),
                key(b.get_cell_value("Sheet1", r, c)),
                "{ctx}: R{r}C{c}"
            );
        }
    }
}

#[test]
fn chain_units_match_per_cell_layers_through_edits() {
    for parallel in [false, true] {
        let mut a = engine(true, parallel);
        let mut b = engine(false, parallel);
        type Edit = fn(&mut Engine<TestWorkbook>);
        let edits: Vec<(&str, Edit)> = vec![
            ("first eval", |_| {}),
            ("value edit", |e| {
                e.set_cell_value("Sheet1", 40, 1, LiteralValue::Number(99.0))
                    .unwrap();
            }),
            ("text breaks the chain", |e| {
                e.set_cell_value("Sheet1", 120, 1, LiteralValue::Text("x".into()))
                    .unwrap();
            }),
            ("value over a member", |e| {
                e.set_cell_value("Sheet1", 150, 2, LiteralValue::Number(-7.0))
                    .unwrap();
            }),
            ("formula edit on a member", |e| {
                e.set_cell_formula("Sheet1", 200, 3, parse("=C199*2").unwrap())
                    .unwrap();
            }),
            ("date format on the input", |e| {
                e.set_cell_value(
                    "Sheet1",
                    60,
                    1,
                    LiteralValue::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
                )
                .unwrap();
            }),
            ("other sheet edit", |e| {
                e.set_cell_value("Data", 5, 1, LiteralValue::Number(1e6))
                    .unwrap();
            }),
        ];
        for (label, edit) in edits {
            edit(&mut a);
            edit(&mut b);
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("parallel={parallel} {label}"));
        }
        assert!(
            a.chained_members_for_test() >= u64::from(ROWS),
            "chain units took the chain lift"
        );
        assert_eq!(b.chained_members_for_test(), 0);
    }
}

/// The recurrence is one sequential layer; its readers are one layer
/// after it (not one per row).
#[test]
fn chain_unit_is_one_sequential_layer() {
    let mut e = engine(true, false);
    e.evaluate_all().unwrap();
    e.set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();
    let plan = e.get_eval_plan(&[("Sheet1", ROWS, 5)]).unwrap();
    assert!(
        plan.layers.len() <= 6,
        "a chain is one layer: {} layers",
        plan.layers.len()
    );
}
