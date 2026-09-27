//! Load-time family grouping in the umya loader (decision 26): loading with
//! grouping must be indistinguishable from loading every formula on its own
//! (`formula_compression = false`), eager and deferred, through edits.
use crate::common::build_workbook;
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

const ROWS: u32 = 60;
const COLS: u32 = 8;

fn fixture() -> std::path::PathBuf {
    build_workbook(|book| {
        let _ = book.new_sheet("Other");
        {
            let other = book.get_sheet_by_name_mut("Other").unwrap();
            for r in 1..=ROWS {
                other
                    .get_cell_mut((1, r))
                    .set_value_number(f64::from(r) * 0.25);
                other
                    .get_cell_mut((2, r))
                    .set_formula(format!("Sheet1!A{r}+A{r}"));
            }
        }
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((8, 1)).set_value_number(1.5);
        for r in 1..=ROWS {
            sh.get_cell_mut((1, r))
                .set_value_number(f64::from(r % 9) - 2.0);
            sh.get_cell_mut((2, r)).set_formula(format!("A{r}*$H$1+1"));
            sh.get_cell_mut((3, r)).set_formula(if r == 1 {
                "B1".to_string()
            } else {
                format!("C{}+B{r}", r - 1)
            });
            sh.get_cell_mut((4, r))
                .set_formula(format!("SUM($A$1:A{r})"));
            sh.get_cell_mut((5, r))
                .set_formula(format!("IF(A{r}>0,Other!A{r},-B{r})"));
            // Literals differ per row: not a family.
            sh.get_cell_mut((6, r)).set_formula(format!("A{r}+{r}"));
            // Two columns of the same shape side by side (left neighbour).
            sh.get_cell_mut((7, r)).set_formula(format!("F{r}*2"));
        }
    })
}

fn load(path: &std::path::Path, mut config: WorkbookConfig, grouping: bool) -> Workbook {
    config.eval.formula_compression = grouping;
    let adapter = UmyaAdapter::open_path(path).unwrap();
    Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap()
}

fn assert_same(label: &str, a: &Workbook, b: &Workbook, rows: u32) {
    for sheet in ["Sheet1", "Other"] {
        for r in 1..=rows {
            for c in 1..=COLS {
                assert_eq!(
                    a.get_formula(sheet, r, c),
                    b.get_formula(sheet, r, c),
                    "{label}: formula {sheet}!R{r}C{c}"
                );
                let (va, vb) = (a.get_value(sheet, r, c), b.get_value(sheet, r, c));
                let same = match (&va, &vb) {
                    (Some(LiteralValue::Number(x)), Some(LiteralValue::Number(y))) => {
                        x.to_bits() == y.to_bits()
                    }
                    _ => va == vb,
                };
                assert!(same, "{label}: value {sheet}!R{r}C{c}: {va:?} vs {vb:?}");
            }
        }
    }
}

#[test]
fn umya_load_time_grouping_matches_per_cell_load() {
    let path = fixture();
    for parallel in [false, true] {
        for deferred in [false, true] {
            let mut config = if deferred {
                WorkbookConfig::interactive()
            } else {
                WorkbookConfig::ephemeral()
            };
            config.eval.enable_parallel = parallel;
            let mut grouped = load(&path, config.clone(), true);
            let mut plain = load(&path, config, false);
            // Arena size right after the build (before the first
            // evaluation compresses anything).
            for wb in [&mut grouped, &mut plain] {
                wb.prepare_graph_all().unwrap();
            }
            let (g, p) = (
                grouped.engine().baseline_stats(),
                plain.engine().baseline_stats(),
            );
            assert_eq!(g.graph_formula_vertex_count, p.graph_formula_vertex_count);
            assert!(
                g.formula_ast_node_count * 2 < p.formula_ast_node_count,
                "deferred={deferred}: grouping should skip member ASTs: {} vs {} arena nodes",
                g.formula_ast_node_count,
                p.formula_ast_node_count
            );
            for wb in [&mut grouped, &mut plain] {
                wb.evaluate_all().unwrap();
            }
            assert_same("first eval", &grouped, &plain, ROWS);
            for wb in [&mut grouped, &mut plain] {
                wb.set_value("Sheet1", 9, 1, LiteralValue::Number(40.0))
                    .unwrap();
                wb.set_value("Sheet1", 1, 8, LiteralValue::Number(-0.5))
                    .unwrap();
                wb.set_formula("Sheet1", 20, 2, "=A20*10").unwrap();
                wb.evaluate_all().unwrap();
            }
            assert_same("edits", &grouped, &plain, ROWS);
            for wb in [&mut grouped, &mut plain] {
                wb.engine_mut().insert_rows("Sheet1", 10, 3).unwrap();
                wb.evaluate_all().unwrap();
            }
            assert_same("row insert", &grouped, &plain, ROWS + 3);
        }
    }
}
