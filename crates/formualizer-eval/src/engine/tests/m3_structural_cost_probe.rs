//! M3 cost probe (ignored): wall time of structural edits on an n-row sheet
//! (`M3PROBE_N`, default 3000; two per-row readers per row and one
//! whole-column reader), to compare the default build (legacy) with
//! `unified_authority` (resync from the transformed formulas). Prints;
//! asserts nothing.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parse;
use std::time::Instant;

#[test]
#[ignore = "timing probe; run explicitly"]
fn m3_structural_cost_probe() {
    let n: u32 = std::env::var("M3PROBE_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_000);
    let t0 = Instant::now();
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=n {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.set_cell_formula("Sheet1", r, 2, parse(format!("=A{r}*2")).unwrap())
            .unwrap();
        e.set_cell_formula("Sheet1", r, 3, parse(format!("=B{r}+A{r}")).unwrap())
            .unwrap();
        e.set_cell_formula("Sheet1", r, 4, parse(format!("=$G$1+C{r}")).unwrap())
            .unwrap();
    }
    e.set_cell_formula("Sheet1", 1, 5, parse("=SUM(B:B)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    eprintln!("M3PROBE n={n} setup {:?}", t0.elapsed());
    // Full-width dirty evaluation without a structural edit.
    for k in 0..2 {
        e.set_cell_value("Sheet1", 1, 7, LiteralValue::Number(f64::from(k)))
            .unwrap();
        let t = Instant::now();
        e.evaluate_all().unwrap();
        eprintln!("M3PROBE authority G1 edit (n dirty) eval {:?}", t.elapsed());
    }
    for (name, op) in [
        ("insert_rows top", 0),
        ("delete_rows top", 1),
        ("insert_columns", 2),
        ("delete_columns", 3),
    ] {
        let t = Instant::now();
        match op {
            0 => drop(e.insert_rows("Sheet1", 1, 1).unwrap()),
            1 => drop(e.delete_rows("Sheet1", 1, 1).unwrap()),
            2 => drop(e.insert_columns("Sheet1", 1, 1).unwrap()),
            _ => drop(e.delete_columns("Sheet1", 1, 1).unwrap()),
        }
        let edit = t.elapsed();
        let t = Instant::now();
        e.graph.authority().unwrap();
        let resync = t.elapsed();
        let t = Instant::now();
        e.evaluate_all().unwrap();
        let eval = t.elapsed();
        // Planner/eval baseline without a structural edit: a value edit
        // dirtying one row reader chain and the whole-column reader.
        e.set_cell_value("Sheet1", 10, 1, LiteralValue::Number(7.0))
            .unwrap();
        let t = Instant::now();
        e.evaluate_all().unwrap();
        eprintln!(
            "M3PROBE authority {name}: edit {edit:?} resync {resync:?} eval {eval:?} | value-edit eval {:?}",
            t.elapsed()
        );
    }
}
