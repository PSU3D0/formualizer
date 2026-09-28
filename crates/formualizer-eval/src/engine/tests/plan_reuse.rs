//! Program 3 plan reuse: a recalculation covered by the largest current
//! schedule (the first evaluation's) takes that schedule restricted to its
//! candidates instead of planning. Values must equal an engine that
//! evaluates the same workbook from scratch, through value edits that
//! dirty a family run's tail, a chain suffix, running-range readers and a
//! name, in sequential and parallel mode.
use super::common::arrow_eval_config;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const ROWS: u32 = 240;

type Edit = fn(&mut Engine<TestWorkbook>);

fn build(parallel: bool, edits: &[Edit]) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        enable_parallel: parallel,
        ..arrow_eval_config()
    };
    let mut e = Engine::new(TestWorkbook::new(), config);
    for r in 1..=ROWS {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r % 7) + 0.5))
            .unwrap();
    }
    for r in 1..=ROWS {
        // B: per-row family; C: chain over B; D: running total range;
        // E: reads another sheet and a name; F: conditional over C and D.
        e.set_cell_formula("Sheet1", r, 2, parse(format!("=A{r}*2")).unwrap())
            .unwrap();
        let c = if r == 1 {
            "=B1".to_string()
        } else {
            format!("=C{}+B{r}", r - 1)
        };
        e.set_cell_formula("Sheet1", r, 3, parse(c).unwrap())
            .unwrap();
        e.set_cell_formula("Sheet1", r, 4, parse(format!("=SUM(B$1:B{r})")).unwrap())
            .unwrap();
        e.set_cell_formula(
            "Sheet1",
            r,
            6,
            parse(format!("=IF(C{r}>D{r},C{r},D{r}+E{r})")).unwrap(),
        )
        .unwrap();
    }
    e.set_cell_value("Data", 1, 3, LiteralValue::Number(10.0))
        .unwrap();
    let data = e.sheet_id("Data").unwrap();
    e.define_name(
        "Scale",
        NamedDefinition::Cell(CellRef::new(data, Coord::from_excel(1, 2, true, true))),
        NameScope::Workbook,
    )
    .unwrap();
    for r in 1..=ROWS {
        e.set_cell_value("Data", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.set_cell_formula("Sheet1", r, 5, parse(format!("=Data!A{r}+Scale")).unwrap())
            .unwrap();
    }
    e.set_cell_formula("Data", 1, 2, parse("=Data!C1*3").unwrap())
        .unwrap();
    // A cycle outside every edit's closure stays out of the restriction.
    e.set_cell_formula("Sheet1", 1, 8, parse("=H2+1").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 2, 8, parse("=H1+1").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    for edit in edits {
        edit(&mut e);
    }
    e
}

fn key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        other => format!("{other:?}"),
    }
}

/// Registry-mutating tests elsewhere in the binary invalidate schedule
/// caches mid-test: the probe count runs in a child process.
fn run_in_subprocess(test_name: &str) -> bool {
    const CHILD_ENV: &str = "FZ_PLAN_REUSE_PROBE_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(format!("engine::tests::plan_reuse::{test_name}"))
        .args(["--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

#[test]
fn restricted_base_schedule_matches_fresh_evaluation() {
    if run_in_subprocess("restricted_base_schedule_matches_fresh_evaluation") {
        return;
    }
    let edits: Vec<(&str, Edit)> = vec![
        ("input in the middle", |e| {
            e.set_cell_value("Sheet1", 120, 1, LiteralValue::Number(99.0))
                .unwrap();
        }),
        ("input near the end", |e| {
            e.set_cell_value("Sheet1", 180, 1, LiteralValue::Number(-4.0))
                .unwrap();
        }),
        ("name input", |e| {
            e.set_cell_value("Data", 1, 3, LiteralValue::Number(0.5))
                .unwrap();
        }),
        ("other sheet input", |e| {
            e.set_cell_value("Data", 180, 1, LiteralValue::Number(7.0))
                .unwrap();
        }),
        ("text input", |e| {
            e.set_cell_value("Sheet1", 150, 1, LiteralValue::Text("x".into()))
                .unwrap();
        }),
    ];
    for parallel in [false, true] {
        let mut a = build(parallel, &[]);
        let mut done: Vec<Edit> = Vec::new();
        let mut restricted = 0;
        for (label, edit) in &edits {
            edit(&mut a);
            a.reset_recalc_reuse_probe();
            a.evaluate_all().unwrap();
            let p = a.recalc_reuse_probe();
            restricted += p.schedule_base_restrictions;
            done.push(*edit);
            let mut b = build(parallel, &done);
            b.evaluate_all().unwrap();
            for r in 1..=ROWS {
                for c in 1..=8 {
                    assert_eq!(
                        key(a.get_cell_value("Sheet1", r, c)),
                        key(b.get_cell_value("Sheet1", r, c)),
                        "parallel={parallel} {label}: R{r}C{c}"
                    );
                }
            }
        }
        assert!(
            restricted >= 4,
            "edits reuse the base schedule ({restricted})"
        );
    }
}
