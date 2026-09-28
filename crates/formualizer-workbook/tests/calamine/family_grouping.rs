//! Load-time family grouping (Program 2, P2-M2): relative copies of the
//! formula above (or to the left) are staged as family members and never
//! interned. Loading with grouping (`EvalConfig::formula_compression`,
//! the default) must be indistinguishable from loading every formula on
//! its own: same formula texts, same values at first evaluation, after
//! value and formula edits, and after a structural edit.
use crate::common::build_workbook;
use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

const SHEETS: [&str; 2] = ["Data", "Other Sheet"];
const ROWS: u32 = 40;
const COLS: u32 = 30;

fn family_xlsx() -> std::path::PathBuf {
    build_workbook(|book| {
        let _ = book.new_sheet("Other Sheet");
        {
            let other = book.get_sheet_by_name_mut("Other Sheet").unwrap();
            for r in 1..=ROWS {
                other
                    .get_cell_mut((1, r))
                    .set_value_number(f64::from(r) * 0.5);
            }
            // Cross-sheet family pointing back at Data.
            for r in 2..=ROWS {
                other
                    .get_cell_mut((2, r))
                    .set_formula(format!("Data!A{r}+A{r}"));
            }
        }
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.set_name("Data");
        sh.get_cell_mut("Z1").set_value_number(1.25);
        for r in 1..=ROWS {
            sh.get_cell_mut((1, r))
                .set_value_number(f64::from(r * 3 % 11));
        }
        for r in 2..=ROWS {
            // Vertical family with an absolute axis.
            sh.get_cell_mut((2, r)).set_formula(format!("A{r}*2+$Z$1"));
            // Cross-sheet (quoted name) family.
            sh.get_cell_mut((3, r))
                .set_formula(format!("'Other Sheet'!A{r}-A{r}"));
            // Growing window, rolling window and whole-column ranges.
            sh.get_cell_mut((4, r))
                .set_formula(format!("SUM(A$2:A{r})"));
            sh.get_cell_mut((5, r))
                .set_formula(format!("SUM(A{r}:B{})", r + 2));
            sh.get_cell_mut((6, r))
                .set_formula(format!("SUM(A:A)-A{r}"));
            // Varying literals: not members of one another.
            sh.get_cell_mut((7, r)).set_formula(format!("A{r}*{r}"));
            // Text literals and IF.
            sh.get_cell_mut((8, r))
                .set_formula(format!("IF(A{r}>5,\"big\",\"small\")"));
            // A defined name (defined after formulas at load).
            sh.get_cell_mut((9, r)).set_formula(format!("A{r}*Rate"));
            // Dynamic members get their own AST.
            sh.get_cell_mut((10, r))
                .set_formula(format!("OFFSET(A1,{},0)", r - 1));
            // Absolute-only duplicates (one arena root).
            sh.get_cell_mut((11, r)).set_formula("$A$2*2");
            // Lookup with an invariant table.
            sh.get_cell_mut((12, r))
                .set_formula(format!("VLOOKUP(A{r},$A$2:$B${ROWS},2,FALSE)"));
            // Lowercase references: texts that are not their rendering.
            sh.get_cell_mut((13, r)).set_formula(format!("a{r}+b{r}"));
            // A one-row offset family (reads the row above).
            sh.get_cell_mut((14, r))
                .set_formula(format!("A{}+N{}", r - 1, r - 1));
        }
        // Horizontal chain along a row and a 2D block with a hole.
        for c in 16..=COLS {
            let prev = col_name(c - 1);
            sh.get_cell_mut((c, 1)).set_formula(format!("{prev}1+1"));
            for r in 3..=8 {
                if (c, r) == (18, 5) {
                    sh.get_cell_mut((c, r)).set_formula("$Z$1*100");
                } else {
                    sh.get_cell_mut((c, r))
                        .set_formula(format!("{prev}{r}*(1+$Z$1)+$A{r}"));
                }
            }
        }
        sh.add_defined_name("Rate", "Data!$Z$1")
            .expect("add defined name");
    })
}

fn col_name(c: u32) -> String {
    let mut c = c;
    let mut s = Vec::new();
    while c > 0 {
        s.push(b'A' + ((c - 1) % 26) as u8);
        c = (c - 1) / 26;
    }
    s.reverse();
    String::from_utf8(s).unwrap()
}

fn load(path: &std::path::Path, grouping: bool, parallel: bool) -> Workbook {
    let mut config = WorkbookConfig::ephemeral();
    config.eval.formula_compression = grouping;
    config.eval.enable_parallel = parallel;
    let adapter = CalamineAdapter::open_path(path).unwrap();
    Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap()
}

fn same_value(a: &Option<LiteralValue>, b: &Option<LiteralValue>) -> bool {
    match (a, b) {
        (Some(LiteralValue::Number(x)), Some(LiteralValue::Number(y))) => {
            x.to_bits() == y.to_bits()
        }
        _ => a == b,
    }
}

fn assert_same(label: &str, grouped: &Workbook, plain: &Workbook, rows: u32) {
    for sheet in SHEETS {
        for r in 1..=rows {
            for c in 1..=COLS {
                let (fa, fb) = (
                    grouped.get_formula(sheet, r, c),
                    plain.get_formula(sheet, r, c),
                );
                assert_eq!(fa, fb, "{label}: formula at {sheet}!R{r}C{c}");
                let (va, vb) = (grouped.get_value(sheet, r, c), plain.get_value(sheet, r, c));
                assert!(
                    same_value(&va, &vb),
                    "{label}: value at {sheet}!R{r}C{c}: {va:?} vs {vb:?}"
                );
            }
        }
    }
}

#[test]
fn load_time_family_grouping_matches_per_cell_load() {
    let path = family_xlsx();
    for parallel in [false, true] {
        let mut grouped = load(&path, true, parallel);
        let mut plain = load(&path, false, parallel);
        // Grouped members were never interned.
        let (g, p) = (
            grouped.engine().baseline_stats(),
            plain.engine().baseline_stats(),
        );
        assert_eq!(g.graph_formula_vertex_count, p.graph_formula_vertex_count);
        assert!(
            g.formula_ast_node_count * 2 < p.formula_ast_node_count,
            "grouping should skip member ASTs: {} vs {} arena nodes",
            g.formula_ast_node_count,
            p.formula_ast_node_count
        );
        assert_same("load", &grouped, &plain, ROWS);

        for wb in [&mut grouped, &mut plain] {
            wb.evaluate_all().unwrap();
        }
        assert_same("first eval", &grouped, &plain, ROWS);

        for wb in [&mut grouped, &mut plain] {
            wb.set_value("Data", 7, 1, LiteralValue::Number(-4.5))
                .unwrap();
            wb.set_value("Data", 1, 26, LiteralValue::Number(0.75))
                .unwrap();
            wb.evaluate_all().unwrap();
        }
        assert_same("value edits", &grouped, &plain, ROWS);

        for wb in [&mut grouped, &mut plain] {
            wb.set_formula("Data", 10, 2, "=A10*3").unwrap();
            wb.set_formula("Data", 5, 17, "=P5-1").unwrap();
            wb.evaluate_all().unwrap();
        }
        assert_same("formula edits", &grouped, &plain, ROWS);

        for wb in [&mut grouped, &mut plain] {
            wb.engine_mut().insert_rows("Data", 6, 2).unwrap();
            wb.evaluate_all().unwrap();
        }
        assert_same("row insert", &grouped, &plain, ROWS + 2);
    }
}

fn load_config(path: &std::path::Path, config: WorkbookConfig) -> Workbook {
    let adapter = CalamineAdapter::open_path(path).unwrap();
    Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap()
}

/// Vertex id of every formula cell, column by column, and the graph's
/// size counters.
fn identity(
    wb: &Workbook,
    rows: u32,
) -> (
    Vec<Option<formualizer_eval::engine::vertex::VertexId>>,
    [usize; 3],
) {
    use formualizer_eval::reference::{CellRef, Coord};
    let engine = wb.engine();
    let mut ids = Vec::new();
    for sheet in SHEETS {
        let sid = engine.sheet_id(sheet).unwrap();
        for c in 1..=COLS {
            for r in 1..=rows {
                if wb.get_formula(sheet, r, c).is_some() {
                    let cell = CellRef::new(sid, Coord::from_excel(r, c, true, true));
                    ids.push(engine.vertex_for_cell(&cell));
                }
            }
        }
    }
    let stats = engine.baseline_stats();
    (
        ids,
        [
            stats.graph_vertex_count,
            stats.graph_formula_vertex_count,
            stats.formula_ast_node_count,
        ],
    )
}

/// Decision 26: a deferred (interactive) load builds its graph at the first
/// evaluation through the eager first load's grouping, id pre-allocation
/// and load-time virtual members: the same ids, vertex and arena counts as
/// the eager load, and the same values and formulas through edits.
#[test]
fn deferred_first_build_matches_the_eager_first_load() {
    let path = family_xlsx();
    for parallel in [false, true] {
        let mut eager_config = WorkbookConfig::ephemeral();
        eager_config.eval.enable_parallel = parallel;
        let mut deferred_config = WorkbookConfig::interactive();
        deferred_config.eval.enable_parallel = parallel;
        assert!(deferred_config.eval.defer_graph_building);
        let mut eager = load_config(&path, eager_config);
        let mut deferred = load_config(&path, deferred_config.clone());
        assert_eq!(
            deferred
                .engine()
                .baseline_stats()
                .graph_formula_vertex_count,
            0
        );
        for sheet in SHEETS {
            for r in 1..=ROWS {
                for c in 1..=COLS {
                    let (va, vb) = (
                        deferred.get_value(sheet, r, c),
                        eager.get_value(sheet, r, c),
                    );
                    assert!(same_value(&va, &vb), "load: value at {sheet}!R{r}C{c}");
                }
            }
        }

        for wb in [&mut eager, &mut deferred] {
            wb.evaluate_all().unwrap();
        }
        assert_same("first eval", &deferred, &eager, ROWS);
        // The same id runs: each column's formulas are pre-allocated as one
        // run in both (the old deferred build made chunk-sized runs).
        let runs = |ids: &[Option<formualizer_eval::engine::vertex::VertexId>]| {
            // `VertexId` is opaque outside the engine: read it off `Debug`.
            let num = |v: &formualizer_eval::engine::vertex::VertexId| -> i64 {
                format!("{v:?}")
                    .trim_start_matches("VertexId(")
                    .trim_end_matches(')')
                    .parse()
                    .unwrap()
            };
            let ids: Vec<i64> = ids.iter().map(|v| num(v.as_ref().unwrap())).collect();
            1 + ids.windows(2).filter(|w| w[1] != w[0] + 1).count()
        };
        let ((di, dc), (ei, ec)) = (identity(&deferred, ROWS), identity(&eager, ROWS));
        assert_eq!(dc, ec, "vertex, formula vertex and arena node counts");
        assert_eq!(runs(&di), runs(&ei), "identity runs");

        for wb in [&mut eager, &mut deferred] {
            wb.set_value("Data", 7, 1, LiteralValue::Number(-4.5))
                .unwrap();
            wb.set_formula("Data", 10, 2, "=A10*3").unwrap();
            wb.evaluate_all().unwrap();
            wb.engine_mut().insert_rows("Data", 6, 2).unwrap();
            wb.evaluate_all().unwrap();
        }
        assert_same("edits", &deferred, &eager, ROWS + 2);

        // Formula edits staged before the first evaluation (deferral keeps
        // them cheap) are part of the first build.
        let mut eager = load_config(&path, {
            let mut c = WorkbookConfig::ephemeral();
            c.eval.enable_parallel = parallel;
            c
        });
        let mut deferred = load_config(&path, deferred_config);
        for wb in [&mut eager, &mut deferred] {
            wb.set_formula("Data", 12, 2, "=A12*7").unwrap();
            wb.set_formula("Data", 30, 3, "=B30+1").unwrap();
            wb.set_value("Data", 3, 1, LiteralValue::Number(11.0))
                .unwrap();
            wb.evaluate_all().unwrap();
        }
        assert_same("staged edits", &deferred, &eager, ROWS);
    }
}
