//! Iterative cycles in a loaded workbook (Program 2 identity unification).
//!
//! The bulk loader allocates a sheet's formula vertices column by column
//! (one id run per column), so loaded formula cells are numbered
//! differently from the row-major staging order legacy used. Cycle
//! iteration order must not change: `evaluate_scc_unit` orders cell members
//! by position (sheet, row, col) (spec §7.13), never by vertex id. A cycle
//! that spans several rows and columns and does not converge within
//! `max_iterations` exposes any change of Gauss-Seidel order in its values;
//! they must equal the per-cell path's, at load and after an edit that adds
//! a member created after the load (a new, higher id).
use crate::common::build_workbook;
use formualizer_eval::engine::{CycleConfig, CycleDetection, CyclePolicy};
use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

const ROWS: u32 = 30;

fn cycle_xlsx() -> std::path::PathBuf {
    build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.set_name("Model");
        for r in 1..=ROWS {
            sh.get_cell_mut((1, r))
                .set_value_number(f64::from(r % 7) + 0.25);
        }
        // Filled-down families around the cycle (column-major ids).
        for r in 2..=ROWS {
            sh.get_cell_mut((5, r))
                .set_formula(format!("A{r}*2+E{}", r - 1));
            sh.get_cell_mut((6, r)).set_formula(format!("E{r}-A{r}"));
        }
        // A diverging cycle over rows 3..6 and columns B..D: every pass
        // grows it, so `max_iterations` stops it mid-flight and its values
        // depend on the member order of each pass.
        sh.get_cell_mut("B3").set_formula("C4*1.5+A3");
        sh.get_cell_mut("C4").set_formula("D3-B5*0.75+A4");
        sh.get_cell_mut("D3").set_formula("B3*1.25+B6");
        sh.get_cell_mut("B5").set_formula("D3*0.5-C4");
        sh.get_cell_mut("B6").set_formula("C4+B5*2");
        // Readers of the cycle, down a column.
        for r in 8..=ROWS {
            sh.get_cell_mut((3, r)).set_formula(format!("B$3+D$3*A{r}"));
        }
    })
}

/// The same model entered through the API, row by row: vertex ids follow
/// creation order (row-major), unlike the loader's column-major ids.
fn api_built(parallel: bool) -> Workbook {
    let mut wb = Workbook::new_with_config(config(false, parallel));
    wb.add_sheet("Model").unwrap();
    let mut formulas: Vec<(u32, u32, String)> = Vec::new();
    for r in 2..=ROWS {
        formulas.push((r, 5, format!("=A{r}*2+E{}", r - 1)));
        formulas.push((r, 6, format!("=E{r}-A{r}")));
    }
    for (a1, f) in [
        ("B3", "=C4*1.5+A3"),
        ("C4", "=D3-B5*0.75+A4"),
        ("D3", "=B3*1.25+B6"),
        ("B5", "=D3*0.5-C4"),
        ("B6", "=C4+B5*2"),
    ] {
        let col = u32::from(a1.as_bytes()[0] - b'A' + 1);
        let row: u32 = a1[1..].parse().unwrap();
        formulas.push((row, col, f.to_string()));
    }
    for r in 8..=ROWS {
        formulas.push((r, 3, format!("=B$3+D$3*A{r}")));
    }
    formulas.sort_by_key(|&(r, c, _)| (r, c));
    for r in 1..=ROWS {
        wb.set_value("Model", r, 1, LiteralValue::Number(f64::from(r % 7) + 0.25))
            .unwrap();
        for (_, c, f) in formulas.iter().filter(|(fr, _, _)| *fr == r) {
            wb.set_formula("Model", r, *c, f).unwrap();
        }
    }
    wb
}

fn config(per_cell: bool, parallel: bool) -> WorkbookConfig {
    let mut config = WorkbookConfig::ephemeral();
    config.eval.cycle = CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Iterate {
            max_iterations: 7,
            max_change: 1e-12,
        },
    };
    config.eval.enable_parallel = parallel;
    if per_cell {
        config.eval.family_execution = false;
        config.eval.family_kernels = false;
        config.eval.family_lift = false;
        config.eval.formula_compression = false;
    }
    config
}

fn load(path: &std::path::Path, per_cell: bool, parallel: bool) -> Workbook {
    let adapter = CalamineAdapter::open_path(path).unwrap();
    Workbook::from_reader(adapter, LoadStrategy::EagerAll, config(per_cell, parallel)).unwrap()
}

fn bits(wb: &Workbook) -> Vec<String> {
    let mut out = Vec::new();
    for r in 1..=ROWS + 2 {
        for c in 1..=8 {
            let v = wb.get_value("Model", r, c);
            out.push(match v {
                Some(LiteralValue::Number(x)) => format!("{r},{c}:{:016x}", x.to_bits()),
                other => format!("{r},{c}:{other:?}"),
            });
        }
    }
    out
}

#[test]
fn loaded_iterative_cycle_matches_per_cell_path_before_and_after_edits() {
    let path = cycle_xlsx();
    for parallel in [false, true] {
        // Loaded (column-major ids), loaded on the per-cell path, and
        // entered row by row through the API (creation-order ids).
        let mut engines = [
            load(&path, false, parallel),
            load(&path, true, parallel),
            api_built(parallel),
        ];
        let check = |engines: &[Workbook; 3], ctx: &str| {
            let want = bits(&engines[0]);
            assert_eq!(
                want,
                bits(&engines[1]),
                "{ctx}: per-cell path (parallel={parallel})"
            );
            assert_eq!(
                want,
                bits(&engines[2]),
                "{ctx}: API-built (parallel={parallel})"
            );
        };
        for wb in engines.iter_mut() {
            wb.evaluate_all().unwrap();
        }
        check(&engines, "first eval");
        // The cycle did not converge: its values keep moving.
        let first = bits(&engines[0]);
        for wb in engines.iter_mut() {
            wb.evaluate_all().unwrap();
        }
        check(&engines, "second eval");
        assert_ne!(
            first,
            bits(&engines[0]),
            "the cycle must not have converged"
        );

        // Mixed: a member created after the load (a higher id) joins the
        // cycle, between loaded members in position order.
        for wb in engines.iter_mut() {
            wb.set_formula("Model", 4, 7, "=B3*0.25+C4").unwrap();
            wb.set_formula("Model", 5, 2, "=D3*0.5-C4+G4").unwrap();
            wb.evaluate_all().unwrap();
        }
        check(&engines, "after edits");
        for wb in engines.iter_mut() {
            wb.set_value("Model", 3, 1, LiteralValue::Number(-2.5))
                .unwrap();
            wb.evaluate_all().unwrap();
        }
        check(&engines, "after a value edit");
    }
}
