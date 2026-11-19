//! LOOKUP reads a result reference shorter than its lookup vector past its
//! end, as Excel does (#586): `=LOOKUP(3,K20:K22,L20:L21)` reads `L22`. The
//! cells it may read there are a reference of the formula, its *footprint*
//! (`lookup_result_footprint`, here `L20:L22`), so an edit of `L22`, a
//! formula in `L22` and a structural edit of the result's sheet reach it.
//! Each engine test runs with the formula plane off and on and with formula
//! compression off and on; the AST-path test evaluates without a graph. The
//! formula plane's mode is currently ignored (`plane_mode_ignored`), so its
//! two runs are equivalent; they stay to cover it once it is honoured.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{CycleConfig, Engine, EvalConfig, FormulaPlaneMode};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;

fn configs() -> Vec<(String, EvalConfig)> {
    let mut out = Vec::new();
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for compression in [false, true] {
            let mut config = EvalConfig::default()
                .with_formula_plane_mode(mode)
                .with_parallel(false);
            config.formula_compression = compression;
            out.push((format!("{mode:?}, compression {compression}"), config));
        }
    }
    out
}

/// `"K12"` as 1-based (row, column).
fn a1(cell: &str) -> (u32, u32) {
    let split = cell.find(|c: char| c.is_ascii_digit()).unwrap();
    let col = cell[..split]
        .bytes()
        .fold(0, |acc, b| acc * 26 + u32::from(b - b'A' + 1));
    (cell[split..].parse().unwrap(), col)
}

fn set(e: &mut Engine<TestWorkbook>, sheet: &str, cell: &str, v: f64) {
    let (row, col) = a1(cell);
    e.set_cell_value(sheet, row, col, LiteralValue::Number(v))
        .unwrap();
}

fn formula(e: &mut Engine<TestWorkbook>, sheet: &str, cell: &str, text: &str) {
    let (row, col) = a1(cell);
    e.set_cell_formula(sheet, row, col, parse(text).unwrap())
        .unwrap();
}

/// The value of a cell as a number, a blank cell as 0.
fn num(e: &Engine<TestWorkbook>, sheet: &str, cell: &str) -> f64 {
    let (row, col) = a1(cell);
    match e.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(v)) => v,
        Some(LiteralValue::Int(v)) => v as f64,
        None | Some(LiteralValue::Empty) => 0.0,
        other => panic!("{sheet}!{cell} = {other:?}"),
    }
}

/// `K20:K22` = 1, 2, 3 and `L20:L22` = 10, 20, 30 on `sheet`.
fn grid(e: &mut Engine<TestWorkbook>, sheet: &str) {
    for (i, (k, l)) in [(1., 10.), (2., 20.), (3., 30.)].into_iter().enumerate() {
        set(e, sheet, &format!("K{}", 20 + i), k);
        set(e, sheet, &format!("L{}", 20 + i), l);
    }
}

#[test]
fn editing_the_cell_past_the_end_recalculates() {
    for (label, config) in configs() {
        for text in [
            "=LOOKUP(3,K20:K22,L20:L21)",
            "=_xlfn.LOOKUP(3,K20:K22,L20:L21)",
        ] {
            let mut e = Engine::new(TestWorkbook::default(), config.clone());
            grid(&mut e, "Sheet1");
            formula(&mut e, "Sheet1", "D1", text);
            e.evaluate_all().unwrap();
            assert_eq!(num(&e, "Sheet1", "D1"), 30.0, "{text} ({label})");
            set(&mut e, "Sheet1", "L22", 35.0);
            e.evaluate_all().unwrap();
            assert_eq!(num(&e, "Sheet1", "D1"), 35.0, "{text} after L22 ({label})");
        }
    }
}

/// A formula in the cell past the end is evaluated before the LOOKUP.
#[test]
fn a_formula_past_the_end_is_evaluated_first() {
    for (label, config) in configs() {
        let mut e = Engine::new(TestWorkbook::default(), config);
        grid(&mut e, "Sheet1");
        // The LOOKUP is written first, so that only its footprint orders it.
        formula(&mut e, "Sheet1", "D1", "=LOOKUP(3,K20:K22,L20:L21)");
        formula(&mut e, "Sheet1", "L22", "=L21+10");
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 30.0, "{label}");
        set(&mut e, "Sheet1", "L21", 25.0);
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "L22"), 35.0, "{label}");
        assert_eq!(num(&e, "Sheet1", "D1"), 35.0, "{label}");
    }
}

/// `Keys!K20:K22` and `Values!` result cells, with the formula on a third
/// sheet: a structural edit of `Values` changes no written reference of the
/// formula, and the value is right after the edit, with no other edit.
fn three_sheets(config: &EvalConfig, result: &str) -> Engine<TestWorkbook> {
    let mut e = Engine::new(TestWorkbook::default(), config.clone());
    for sheet in ["Keys", "Values", "Out"] {
        e.add_sheet(sheet).unwrap();
    }
    grid(&mut e, "Keys");
    grid(&mut e, "Values");
    set(&mut e, "Values", "L23", 40.0);
    // A one-row result: Values!P20:R20 = 100, 120, 130 (S20 = 140).
    for (cell, v) in [("P20", 100.), ("Q20", 120.), ("R20", 130.), ("S20", 140.)] {
        set(&mut e, "Values", cell, v);
    }
    formula(
        &mut e,
        "Out",
        "A1",
        &format!("=LOOKUP(3,Keys!K20:K22,Values!{result})"),
    );
    e.evaluate_all().unwrap();
    e
}

#[test]
fn structural_edits_of_the_result_sheet_reach_the_lookup() {
    for (label, config) in configs() {
        // Inserting row 22 on Values: L22 is blank, the 30 moves to L23.
        let mut e = three_sheets(&config, "L20:L21");
        assert_eq!(num(&e, "Out", "A1"), 30.0, "{label}");
        e.insert_rows("Values", 22, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Out", "A1"), 0.0, "row insert ({label})");

        // Deleting row 22 on Values: L22 is the former L23.
        let mut e = three_sheets(&config, "L20:L21");
        e.delete_rows("Values", 22, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Out", "A1"), 40.0, "row delete ({label})");

        // A one-cell result reads to the right: inserting column R on
        // Values leaves R20 blank, deleting it brings S20 there.
        let mut e = three_sheets(&config, "P20");
        assert_eq!(num(&e, "Out", "A1"), 130.0, "{label}");
        e.insert_columns("Values", 18, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Out", "A1"), 0.0, "column insert ({label})");
        let mut e = three_sheets(&config, "P20");
        e.delete_columns("Values", 18, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Out", "A1"), 140.0, "column delete ({label})");
    }
}

/// On one sheet, row edits move the written references; after each, the
/// value is right, and an edit of the cell now read past the end reaches it.
#[test]
fn row_edits_on_the_lookup_sheet_move_the_footprint() {
    for (label, config) in configs() {
        let mut e = Engine::new(TestWorkbook::default(), config);
        grid(&mut e, "Sheet1");
        formula(&mut e, "Sheet1", "D1", "=LOOKUP(3,K20:K22,L20:L21)");
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 30.0, "{label}");

        // Inside K20:K22, at the end of L20:L21: K20:K23 = 1,2,_,3, the
        // result stays L20:L21, and the 30 moves to L23.
        e.insert_rows("Sheet1", 22, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 30.0, "insert inside ({label})");
        set(&mut e, "Sheet1", "L23", 31.0);
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 31.0, "L23 after insert ({label})");

        // Deleting that row again: back to K20:K22, L22 = 31.
        e.delete_rows("Sheet1", 22, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 31.0, "delete ({label})");
        set(&mut e, "Sheet1", "L22", 32.0);
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 32.0, "L22 after delete ({label})");

        // Above row 20: everything moves down one row.
        e.insert_rows("Sheet1", 20, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 32.0, "insert above ({label})");
        set(&mut e, "Sheet1", "L23", 33.0);
        e.evaluate_all().unwrap();
        assert_eq!(
            num(&e, "Sheet1", "D1"),
            33.0,
            "L23 after insert above ({label})"
        );
    }
}

/// Rows 1..=100 of column D, one variant per test. `K1:K300` = the row
/// number, `L1:L300` = 10 × the row number, and row `r` holds 1000 × r in
/// the column a one-cell result `P{r}` reads past its end (`16 + r + 1`).
const FILL_ROWS: u32 = 100;

/// The three filled-down variants, as (formula at row `r`, value at row `r`,
/// shared): fully relative with a long-enough result (shared, no
/// footprint); `K$1` with a one-cell result, whose footprint grows with the
/// row (blocked); fully relative with a short result, which has a footprint
/// at every cell (blocked).
#[allow(clippy::type_complexity)]
fn fill_variants() -> [(&'static str, fn(u32) -> String, fn(u32) -> f64, bool); 3] {
    [
        (
            "relative, long enough",
            |r| format!("=LOOKUP(1E9,K{r}:K{},L{r}:L{})", r + 2, r + 2),
            |r| f64::from(10 * (r + 2)),
            true,
        ),
        (
            "K$1 with a one-cell result",
            |r| format!("=LOOKUP(1E9,K$1:K{},P{r})", r + 2),
            |r| f64::from(1000 * r),
            false,
        ),
        (
            "relative, short",
            |r| format!("=LOOKUP(1E9,K{r}:K{},L{r}:L{})", r + 2, r + 1),
            |r| f64::from(10 * (r + 2)),
            false,
        ),
    ]
}

fn fill_data(e: &mut Engine<TestWorkbook>) {
    for row in 1..=300u32 {
        e.set_cell_value("Sheet1", row, 11, LiteralValue::Number(f64::from(row)))
            .unwrap();
        e.set_cell_value("Sheet1", row, 12, LiteralValue::Number(f64::from(10 * row)))
            .unwrap();
    }
    for r in 1..=FILL_ROWS {
        e.set_cell_value(
            "Sheet1",
            r,
            16 + r + 1,
            LiteralValue::Number(f64::from(1000 * r)),
        )
        .unwrap();
    }
}

/// Whether the formula at `(row, 4)` is a compressed family member (its
/// formula is a shared template at an offset).
fn is_shared_member(e: &Engine<TestWorkbook>, row: u32) -> bool {
    let cell = e.graph.make_cell_ref("Sheet1", row, 4);
    e.graph
        .get_vertex_id_for_address(&cell)
        .is_some_and(|v| e.graph.get_formula_id(v).is_none())
}

fn check_fill(
    e: &Engine<TestWorkbook>,
    value: fn(u32) -> f64,
    changed: Option<(u32, f64)>,
    what: &str,
) {
    for r in 1..=FILL_ROWS {
        let want = match changed {
            Some((row, v)) if row == r => v,
            _ => value(r),
        };
        let got = e.get_cell_value("Sheet1", r, 4);
        assert_eq!(got, Some(LiteralValue::Number(want)), "row {r}: {what}");
    }
}

#[test]
fn filled_down_lookups_share_only_without_a_footprint() {
    for (label, config) in configs() {
        for (variant, text, value, shared) in fill_variants() {
            for bulk in [false, true] {
                let what = format!("{variant}, bulk {bulk} ({label})");
                let mut config = config.clone();
                config.defer_graph_building = bulk;
                let mut e = Engine::new(TestWorkbook::default(), config.clone());
                fill_data(&mut e);
                for r in 1..=FILL_ROWS {
                    if bulk {
                        e.stage_formula_text("Sheet1", r, 4, text(r));
                    } else {
                        e.set_cell_formula("Sheet1", r, 4, parse(text(r)).unwrap())
                            .unwrap();
                    }
                }
                e.evaluate_all().unwrap();
                check_fill(&e, value, None, &what);
                if config.formula_compression {
                    let members = (1..=FILL_ROWS).filter(|&r| is_shared_member(&e, r)).count();
                    if shared {
                        assert!(members > 0, "no shared member: {what}");
                    } else {
                        assert_eq!(members, 0, "shared members: {what}");
                    }
                }

                // Editing one cell a row reads past its end (or, for the
                // shared variant, its last result cell) changes that row
                // alone, then again after a formula of the family is
                // written anew.
                let edit = |e: &mut Engine<TestWorkbook>, r: u32, v: f64| {
                    let col = if variant.starts_with("K$1") {
                        16 + r + 1
                    } else {
                        12
                    };
                    let row = if variant.starts_with("K$1") { r } else { r + 2 };
                    e.set_cell_value("Sheet1", row, col, LiteralValue::Number(v))
                        .unwrap();
                };
                // Row r alone answers with that cell (`L{r+2}`, or the one
                // `P{r}` reads past its end).
                edit(&mut e, 50, -1.0);
                e.evaluate_all().unwrap();
                check_fill(&e, value, Some((50, -1.0)), &format!("{what}, edit"));
                edit(&mut e, 50, value(50));
                e.set_cell_formula("Sheet1", 60, 4, parse(text(60)).unwrap())
                    .unwrap();
                edit(&mut e, 70, -2.0);
                e.evaluate_all().unwrap();
                check_fill(&e, value, Some((70, -2.0)), &format!("{what}, resync"));
            }
        }
    }
}

/// Past the end of its result, LOOKUP reads only when both the lookup vector
/// and the result are cell or range references written in the formula: an
/// array, a computed value, a reference a function returns, a defined name
/// or a LET local in either place stays blank there (0), where Excel reads
/// further. The documented difference.
#[test]
fn a_result_without_a_written_reference_is_blank_past_its_end() {
    for (label, config) in configs() {
        let mut e = Engine::new(TestWorkbook::default(), config);
        grid(&mut e, "Sheet1");
        let sheet = e.graph.sheet_id("Sheet1").unwrap();
        let cell = |row, col| CellRef::new(sheet, Coord::from_excel(row, col, true, true));
        e.define_name(
            "Res",
            NamedDefinition::Range(RangeRef::new(cell(20, 12), cell(21, 12))),
            NameScope::Workbook,
        )
        .unwrap();
        let cases = [
            ("D1", "=LOOKUP(3,K20:K22,INDIRECT(\"L20:L21\"))"),
            ("D2", "=LOOKUP(3,K20:K22,Res)"),
            ("D3", "=LET(r,L20:L21,LOOKUP(3,K20:K22,r))"),
            ("D4", "=LOOKUP(3,K20:K22,IF(TRUE,L20:L21))"),
            ("D5", "=LOOKUP(3,{1,2,3},L20:L21)"),
            ("D6", "=LOOKUP(3,K20:K22*1,L20:L21)"),
        ];
        for (cell, text) in cases {
            formula(&mut e, "Sheet1", cell, text);
        }
        e.evaluate_all().unwrap();
        for (cell, text) in cases {
            assert_eq!(num(&e, "Sheet1", cell), 0.0, "{text} ({label})");
        }
    }
}

/// A LOOKUP inside its own footprint is circular, as in Excel. The
/// footprint is a range dependency, so it is treated as any range covering
/// its formula's cell (`=SUM(B:B)` in `B1`, #120): accepted when written,
/// `#CIRC!` under the default cycle policy, iterated under an iterative one.
#[test]
fn a_lookup_in_its_own_footprint_is_circular() {
    for (label, config) in configs() {
        let mut e = Engine::new(TestWorkbook::default(), config.clone());
        grid(&mut e, "Sheet1");
        formula(&mut e, "Sheet1", "L22", "=LOOKUP(3,K20:K22,L20:L21)");
        e.evaluate_all().unwrap();
        assert!(
            matches!(
                e.get_cell_value("Sheet1", 22, 12),
                Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Circ
            ),
            "{label}: {:?}",
            e.get_cell_value("Sheet1", 22, 12)
        );

        let mut config = config;
        config.cycle = CycleConfig::iterate(10, 0.001);
        let mut e = Engine::new(TestWorkbook::default(), config);
        grid(&mut e, "Sheet1");
        formula(&mut e, "Sheet1", "L22", "=LOOKUP(3,K20:K22,L20:L21)");
        e.evaluate_all().unwrap();
        assert!(
            !matches!(
                e.get_cell_value("Sheet1", 22, 12),
                Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Circ
            ),
            "{label}, iterative: {:?}",
            e.get_cell_value("Sheet1", 22, 12)
        );
    }
}

/// Without a graph, the interpreter reads past the end of a written result
/// reference too, and only of one.
#[test]
fn ast_path_reads_past_the_end_of_a_written_result() {
    crate::builtins::load_builtins();
    let mut wb = TestWorkbook::new();
    for (i, (k, l)) in [(1, 10), (2, 20), (3, 30)].into_iter().enumerate() {
        wb = wb
            .with_cell("Sheet1", 20 + i as u32, 11, LiteralValue::Int(k))
            .with_cell("Sheet1", 20 + i as u32, 12, LiteralValue::Int(l));
    }
    wb = wb.with_cell("Sheet1", 20, 18, LiteralValue::Int(130));
    let interpreter = wb.interpreter();
    for (text, want) in [
        ("=LOOKUP(3,K20:K22,L20:L21)", 30.0),
        ("=_xlfn.LOOKUP(3,K20:K22,L20:L21)", 30.0),
        ("=LOOKUP(3,K20:K22,P20)", 130.0),
        ("=LOOKUP(3,K20:K22,INDIRECT(\"L20:L21\"))", 0.0),
        ("=LET(r,L20:L21,LOOKUP(3,K20:K22,r))", 0.0),
    ] {
        let got = interpreter
            .evaluate_ast(&parse(text).unwrap())
            .unwrap()
            .into_literal();
        let got = match got {
            LiteralValue::Int(v) => v as f64,
            LiteralValue::Number(v) => v,
            other => panic!("{text} = {other:?}"),
        };
        assert_eq!(got, want, "{text}");
    }
}

/// However small, a footprint is one range dependency, never expanded into
/// cells: a row insertion inside it reaches the LOOKUP only as a range
/// reader (`structural_edits_of_the_result_sheet_reach_the_lookup`). A
/// written range of the same size is expanded, so each check discriminates.
#[test]
fn a_short_footprint_is_one_range_dependency() {
    use crate::engine::ingest_pipeline::FormulaAstInput;
    use crate::engine::plan::{
        DependencyPlanAst, build_dependency_plan, build_dependency_plan_mixed,
    };
    use crate::engine::sheet_registry::SheetRegistry;
    use formualizer_parse::parser::CollectPolicy;
    crate::builtins::load_builtins();
    let lookup = "=LOOKUP(3,K20:K22,L20:L21)";
    let written = "=SUM(K20:K22)+SUM(L20:L21)";

    // The graph: only the footprint makes the formula a range reader.
    for (text, reader) in [(lookup, true), (written, false)] {
        let mut e = Engine::new(TestWorkbook::default(), EvalConfig::default());
        grid(&mut e, "Sheet1");
        formula(&mut e, "Sheet1", "D1", text);
        assert_eq!(
            e.graph.has_compressed_range_readers(),
            reader,
            "graph: {text}"
        );
    }

    // The ingest pipeline: five cells and the footprint L20:L22.
    for (text, ranges) in [(lookup, 1), (written, 0)] {
        let mut e = Engine::new(TestWorkbook::default(), EvalConfig::default());
        let sheet = e.graph.sheet_id_mut("Sheet1");
        let placement = CellRef::new(sheet, Coord::from_excel(1, 4, true, true));
        let mut pipeline = e.ingest_pipeline();
        let ingested = pipeline
            .ingest_formula(FormulaAstInput::Tree(parse(text).unwrap()), placement, None)
            .unwrap();
        let plan = &ingested.dep_plan;
        assert_eq!(plan.direct_cell_deps.len(), 5, "ingest cells: {text}");
        assert_eq!(plan.range_deps.len(), ranges, "ingest ranges: {text}");
        if ranges == 1 {
            let range = &plan.range_deps[0];
            let index = |bound: Option<formualizer_common::AxisBound>| bound.map(|b| b.index);
            assert_eq!(
                [
                    range.start_row,
                    range.end_row,
                    range.start_col,
                    range.end_col
                ]
                .map(index),
                [Some(19), Some(21), Some(11), Some(11)],
                "the footprint L20:L22: {text}"
            );
        }
    }

    // The dependency plan, on a tree and on the arena.
    let policy = CollectPolicy {
        expand_small_ranges: true,
        range_expansion_limit: 16,
        include_names: true,
    };
    for (text, ranges) in [(lookup, 1), (written, 0)] {
        let ast = parse(text).unwrap();
        let mut registry = SheetRegistry::new();
        let plan = build_dependency_plan(
            &mut registry,
            std::iter::once(("Sheet1", 1, 4, &ast)),
            &policy,
            None,
        )
        .unwrap();
        assert_eq!(plan.per_formula_cells[0].len(), 5, "plan cells: {text}");
        assert_eq!(
            plan.per_formula_ranges[0].len(),
            ranges,
            "plan ranges: {text}"
        );

        let mut registry = SheetRegistry::new();
        registry.id_for("Sheet1");
        let mut store = crate::engine::arena::DataStore::new();
        let id = store.store_ast(&ast, &registry);
        let plan = build_dependency_plan_mixed(
            &mut registry,
            &store,
            std::iter::once(("Sheet1", 1, 4, DependencyPlanAst::Arena(id))),
            &policy,
            None,
        )
        .unwrap();
        assert_eq!(
            plan.per_formula_cells[0].len(),
            5,
            "arena plan cells: {text}"
        );
        assert_eq!(
            plan.per_formula_ranges[0].len(),
            ranges,
            "arena plan ranges: {text}"
        );
    }
}

/// A provider-local alias of LOOKUP (a name the global registry does not
/// know) runs the builtin, but the reference walkers cannot see the alias:
/// the call has no footprint, so LOOKUP does not read past its result's end
/// and gives 0 there, before and after an edit of that cell, never a stale
/// value. `=LOOKUP` and `=_xlfn.LOOKUP` keep their footprint
/// (`editing_the_cell_past_the_end_recalculates`).
#[test]
fn a_provider_alias_of_lookup_reads_blank_past_the_end_never_stale() {
    crate::builtins::load_builtins();
    let lookup = crate::function_registry::get("", "LOOKUP").unwrap();
    for (label, config) in configs() {
        let resolver = TestWorkbook::new()
            .with_function(std::sync::Arc::clone(&lookup))
            .with_alias("", "LOCAL_LOOKUP", "", "LOOKUP");
        let mut e = Engine::new(resolver, config);
        grid(&mut e, "Sheet1");
        formula(&mut e, "Sheet1", "D1", "=LOCAL_LOOKUP(3,K20:K22,L20:L21)");
        formula(&mut e, "Sheet1", "D2", "=LOCAL_LOOKUP(2,K20:K22,L20:L21)");
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 0.0, "{label}");
        assert_eq!(num(&e, "Sheet1", "D2"), 20.0, "{label}");
        set(&mut e, "Sheet1", "L22", 99.0);
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 0.0, "after L22 ({label})");
        set(&mut e, "Sheet1", "L21", 25.0);
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D2"), 25.0, "after L21 ({label})");
    }
}

/// A provider-local alias of LOOKUP that the global registry learns later:
/// its formulas, ingested with no footprint (blank past the end), get one
/// when the alias is registered globally, and then follow the cell past the
/// end of their result like `=LOOKUP`, never a stale value. The alias name is
/// this test's own, so the global registration touches no other test.
#[test]
fn a_lookup_alias_registered_globally_later_gets_its_footprint() {
    crate::builtins::load_builtins();
    let lookup = crate::function_registry::get("", "LOOKUP").unwrap();
    let mut engines: Vec<(String, Engine<TestWorkbook>)> = configs()
        .into_iter()
        .map(|(label, config)| {
            let resolver = TestWorkbook::new()
                .with_function(std::sync::Arc::clone(&lookup))
                .with_alias("", "FZ586_LATE_LOOKUP", "", "LOOKUP");
            let mut e = Engine::new(resolver, config);
            grid(&mut e, "Sheet1");
            formula(
                &mut e,
                "Sheet1",
                "D1",
                "=FZ586_LATE_LOOKUP(3,K20:K22,L20:L21)",
            );
            e.evaluate_all().unwrap();
            assert_eq!(num(&e, "Sheet1", "D1"), 0.0, "before ({label})");
            (label, e)
        })
        .collect();
    crate::function_registry::register_alias("", "FZ586_LATE_LOOKUP", "", "LOOKUP");
    for (label, e) in &mut engines {
        e.evaluate_all().unwrap();
        assert_eq!(num(e, "Sheet1", "D1"), 30.0, "registered ({label})");
        set(e, "Sheet1", "L22", 99.0);
        e.evaluate_all().unwrap();
        assert_eq!(num(e, "Sheet1", "D1"), 99.0, "after L22 ({label})");
    }
}

/// LOOKUP, or the constant 42 while `overridden` is set, under `name`: a
/// provider whose resolution of LOOKUP changes with no revision.
struct SwitchableLookup {
    name: &'static str,
    overridden: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl SwitchableLookup {
    fn builtin() -> std::sync::Arc<dyn crate::function::Function> {
        crate::builtins::load_builtins();
        crate::function_registry::get("", "LOOKUP").unwrap()
    }

    fn overridden(&self) -> bool {
        self.overridden.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl crate::function::Function for SwitchableLookup {
    fn name(&self) -> &'static str {
        self.name
    }
    fn min_args(&self) -> usize {
        2
    }
    fn caps(&self) -> crate::function::FnCaps {
        if self.overridden() {
            crate::function::FnCaps::PURE
        } else {
            Self::builtin().caps()
        }
    }
    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        Self::builtin().arg_schema()
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        if self.overridden() {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(42.0)))
        } else {
            Self::builtin().dispatch(args, ctx)
        }
    }
}

fn is_circ(e: &Engine<TestWorkbook>, sheet: &str, cell: &str) -> bool {
    let (row, col) = a1(cell);
    matches!(
        e.get_cell_value(sheet, row, col),
        Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Circ
    )
}

/// A provider without a revision switches LOOKUP to an override and back:
/// the engine notices on the next evaluation, and the LOOKUP inside its own
/// footprint stops being circular, then becomes circular again.
#[test]
fn a_revisionless_provider_switching_lookup_rebuilds_the_footprint() {
    for (label, config) in configs() {
        let overridden = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let resolver = TestWorkbook::new()
            .without_planning_revision()
            .with_function(std::sync::Arc::new(SwitchableLookup {
                name: "LOOKUP",
                overridden: std::sync::Arc::clone(&overridden),
            }));
        let mut e = Engine::new(resolver, config);
        grid(&mut e, "Sheet1");
        formula(&mut e, "Sheet1", "L22", "=LOOKUP(3,K20:K22,L20:L21)");
        e.evaluate_all().unwrap();
        assert!(is_circ(&e, "Sheet1", "L22"), "builtin ({label})");

        overridden.store(true, std::sync::atomic::Ordering::SeqCst);
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "L22"), 42.0, "overridden ({label})");
        set(&mut e, "Sheet1", "K21", 2.5);
        e.evaluate_all().unwrap();
        assert_eq!(
            num(&e, "Sheet1", "L22"),
            42.0,
            "after an input edit ({label})"
        );

        overridden.store(false, std::sync::atomic::Ordering::SeqCst);
        e.evaluate_all().unwrap();
        assert!(is_circ(&e, "Sheet1", "L22"), "builtin again ({label})");
    }
}

/// A spelling with several Excel prefixes never reads past its result's end:
/// an override under such a spelling is not circular inside the builtin's
/// footprint, and the builtin under it reads blank there.
#[test]
fn a_compound_prefix_spelling_has_no_footprint() {
    for (label, config) in configs() {
        let resolver = TestWorkbook::new().with_function(std::sync::Arc::new(SwitchableLookup {
            name: "_XLFN._XLWS.LOOKUP",
            overridden: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }));
        let mut e = Engine::new(resolver, config.clone());
        grid(&mut e, "Sheet1");
        formula(
            &mut e,
            "Sheet1",
            "L22",
            "=_xlfn._xlws.LOOKUP(3,K20:K22,L20:L21)",
        );
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "L22"), 42.0, "override ({label})");

        let mut e = Engine::new(TestWorkbook::default(), config);
        grid(&mut e, "Sheet1");
        formula(
            &mut e,
            "Sheet1",
            "D1",
            "=_xlfn._xlws.LOOKUP(3,K20:K22,L20:L21)",
        );
        formula(&mut e, "Sheet1", "D2", "=_xlfn.LOOKUP(3,K20:K22,L20:L21)");
        e.evaluate_all().unwrap();
        assert_eq!(num(&e, "Sheet1", "D1"), 0.0, "builtin, compound ({label})");
        assert_eq!(
            num(&e, "Sheet1", "D2"),
            30.0,
            "builtin, one prefix ({label})"
        );
    }
}

/// A filled-down provider-local alias of LOOKUP shares one template (the
/// global registry does not know it, so nothing keeps it from sharing).
/// Registered globally later, each row gets its own footprint: members are
/// materialized and own their formula, so `K$1:K{r+2}` with `P{r}` reads its
/// own tail cell, and an edit of that cell changes that row alone.
#[test]
fn a_filled_down_alias_registered_globally_later_rebuilds_each_member() {
    crate::builtins::load_builtins();
    let lookup = crate::function_registry::get("", "LOOKUP").unwrap();
    let text = |r: u32| format!("=FZ586_FILL_LOOKUP(1E9,K$1:K{},P{r})", r + 2);
    let mut engines = Vec::new();
    for (label, config) in configs() {
        for bulk in [false, true] {
            let mut config = config.clone();
            config.defer_graph_building = bulk;
            let resolver = TestWorkbook::new()
                .with_function(std::sync::Arc::clone(&lookup))
                .with_alias("", "FZ586_FILL_LOOKUP", "", "LOOKUP");
            let mut e = Engine::new(resolver, config.clone());
            fill_data(&mut e);
            for r in 1..=FILL_ROWS {
                if bulk {
                    e.stage_formula_text("Sheet1", r, 4, text(r));
                } else {
                    e.set_cell_formula("Sheet1", r, 4, parse(text(r)).unwrap())
                        .unwrap();
                }
            }
            e.evaluate_all().unwrap();
            let what = format!("bulk {bulk} ({label})");
            check_fill(&e, |_| 0.0, None, &format!("before, {what}"));
            if config.formula_compression {
                let members = (1..=FILL_ROWS).filter(|&r| is_shared_member(&e, r)).count();
                assert!(members > 0, "no shared member before: {what}");
            }
            engines.push((what, e));
        }
    }
    crate::function_registry::register_alias("", "FZ586_FILL_LOOKUP", "", "LOOKUP");
    for (what, e) in &mut engines {
        e.evaluate_all().unwrap();
        let value = |r: u32| f64::from(1000 * r);
        check_fill(e, value, None, &format!("registered, {what}"));
        e.set_cell_value("Sheet1", 50, 16 + 51, LiteralValue::Number(-1.0))
            .unwrap();
        e.evaluate_all().unwrap();
        check_fill(
            e,
            value,
            Some((50, -1.0)),
            &format!("after the tail edit, {what}"),
        );
    }
}
