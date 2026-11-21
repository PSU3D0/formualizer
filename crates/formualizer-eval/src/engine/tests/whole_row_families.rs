//! Whole-row formula family tests and timing probe (issue #387):
//! - family admission & tier-3 `AggregateKernel` residency (0 fallback) for whole-row
//!   (`1:1`, `3:13`, `$1:$1`, `Data!1:1`), whole-column (`A:A`), and open-ended ranges,
//! - exact edit/dirty projection & inversion (edits inside row band dirty exact dependents;
//!   edits outside dirty none),
//! - structural row/column inserts & deletes + undo/redo parity,
//! - bit-exact value/error parity against per-cell evaluation across edge cases.

use super::common::arrow_eval_config;
use crate::engine::authority::geom::Cell as AuthCell;
use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{ChangeLog, Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

/// Bit-level value key (`-0.0`, NaN payloads, and error kinds are distinct).
fn val_key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        Some(LiteralValue::Error(e)) => format!("E{:?}", e.kind),
        other => format!("{other:?}"),
    }
}

fn dirty_cells(e: &Engine<TestWorkbook>) -> Vec<AuthCell> {
    let mut out: Vec<AuthCell> = e
        .graph
        .vertices_with_formulas()
        .filter(|&v| e.graph.is_dirty(v))
        .filter_map(|v| e.graph.get_cell_ref(v))
        .map(|c| (c.sheet_id, c.coord.row(), c.coord.col()))
        .collect();
    out.sort_unstable();
    out
}

fn mixed_cell(seed: u32) -> LiteralValue {
    match seed % 11 {
        0 => LiteralValue::Number(-0.0),
        1 => LiteralValue::Text(format!("t{seed}")),
        2 => LiteralValue::Text("42".into()),
        3 => LiteralValue::Boolean(seed.is_multiple_of(2)),
        4 => LiteralValue::Empty,
        5 => LiteralValue::Number(0.0),
        6 => LiteralValue::Int(seed as i64 - 20),
        _ => LiteralValue::Number((seed as f64) * 1.25 - 15.0),
    }
}

/// Whole-row, whole-column, and open-ended aggregate families achieve 100%
/// family-run and Tier-3 `AggregateKernel` residency (0 fallback) in both
/// sequential and parallel execution, and compress into virtual members.
#[test]
fn whole_row_family_admission_and_zero_fallback_kernel_residency() {
    let rows = 60u32;
    let cols = 16usize;
    for parallel in [false, true] {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                enable_parallel: parallel,
                ..arrow_eval_config()
            },
        );
        {
            let mut ab = e.begin_bulk_ingest_arrow();
            ab.add_sheet("Data", cols, 64);
            let mut buf = Vec::with_capacity(cols);
            for r in 1..=rows + 12 {
                buf.clear();
                for c in 1..=cols {
                    buf.push(LiteralValue::Number((r as f64) + (c as f64) * 0.1));
                }
                ab.append_row("Data", &buf).unwrap();
            }
            ab.finish().unwrap();
        }
        type FamilySpec = (u32, fn(u32) -> String);
        let families: [FamilySpec; 10] = [
            (1, |r| format!("=SUM(Data!{r}:{r})")),
            (2, |r| format!("=SUM(Data!{}:{})", r + 2, r + 12)),
            (3, |r| format!("=AVERAGE(Data!{r}:{r})")),
            (4, |r| format!("=MIN(Data!{r}:{r})")),
            (5, |r| format!("=MAX(Data!{r}:{r})")),
            (6, |r| format!("=COUNT(Data!{r}:{r})")),
            (7, |_| "=SUM(Data!$1:$1)".to_string()),
            (8, |_| "=SUM(Data!A:A)".to_string()),
            (9, |r| format!("=SUM(Data!A{r}:A)")),
            (10, |r| format!("=SUM(Data!B{r}:{r})")),
        ];
        let mut builder = e.begin_bulk_ingest();
        let sheet = builder.add_sheet("Sheet1");
        let mut batch = Vec::with_capacity(rows as usize * families.len());
        for (col, make_f) in families {
            for r in 1..=rows {
                batch.push((r, col, parse(make_f(r)).unwrap()));
            }
        }
        builder.add_formulas(sheet, batch);
        builder.finish().unwrap();
        e.evaluate_all().unwrap();
        let expected_members = u64::from(rows) * families.len() as u64;
        assert_eq!(
            e.family_members_for_test(),
            expected_members,
            "all whole-row/open-range members must evaluate as family runs (parallel={parallel})"
        );
        assert_eq!(
            e.aggregate_kernel_members_for_test(),
            expected_members,
            "0 fallback: all whole-row/open-range aggregate members must hit AggregateKernel (parallel={parallel})"
        );
        let (virtual_members, _) = e.graph.virtual_member_counts();
        assert!(
            virtual_members >= (rows as usize - 1) * families.len(),
            "whole-row families must compress into virtual members (got {virtual_members}, parallel={parallel})"
        );
    }
}

/// Engine edit/dirty inversion for whole-row ranges: editing inside the row
/// band (including far columns up to XFD) dirties the exact dependent family
/// members; editing outside dirties none.
#[test]
fn whole_row_dirty_projection_and_inversion_precision() {
    let mut e = Engine::new(TestWorkbook::new(), arrow_eval_config());
    for r in 1..=40u32 {
        for c in 1..=8u32 {
            e.set_cell_value("Data", r, c, LiteralValue::Number((r * c) as f64))
                .unwrap();
        }
    }
    for r in 1..=25u32 {
        // Col 1: relative 1:1
        e.set_cell_formula(
            "Sheet1",
            r,
            1,
            parse(format!("=SUM(Data!{r}:{r})")).unwrap(),
        )
        .unwrap();
        // Col 2: sliding 3:13 (`r+2:r+12`)
        e.set_cell_formula(
            "Sheet1",
            r,
            2,
            parse(format!("=SUM(Data!{}:{})", r + 2, r + 12)).unwrap(),
        )
        .unwrap();
        // Col 3: absolute `$3:$13`
        e.set_cell_formula("Sheet1", r, 3, parse("=SUM(Data!$3:$13)").unwrap())
            .unwrap();
    }
    // Filled ACROSS columns in row 30: A30..J30 all read `=SUM(Data!1:1)`.
    for c in 1..=10u32 {
        e.set_cell_formula("Sheet1", 30, c, parse("=SUM(Data!1:1)").unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    assert!(dirty_cells(&e).is_empty());
    let s1 = e.graph.sheet_id("Sheet1").unwrap();
    let data = e.graph.sheet_id("Data").unwrap();

    // Edit Data!Z15 (row 15, col 26): inside col 1 row 15, col 2 rows 3..=13 (`r+2..r+12` hits 15),
    // outside col 3 (`$3:$13`) and row 30 (`1:1`).
    e.set_cell_value("Data", 15, 26, LiteralValue::Number(100.0))
        .unwrap();
    let dirty = dirty_cells(&e);
    let legacy = e.graph.legacy_closure_cells(&[(data, 14, 25)]);
    assert_eq!(
        dirty, legacy,
        "authority dirty set must match legacy closure"
    );
    let mut expected: Vec<AuthCell> = vec![(s1, 14, 0)];
    for r0 in 2..=12u32 {
        expected.push((s1, r0, 1));
    }
    expected.sort_unstable();
    assert_eq!(dirty, expected);
    e.evaluate_all().unwrap();
    assert!(dirty_cells(&e).is_empty());

    // Edit Data!XFD1 (row 1, col 16384): dirties Sheet1!A1 and filled-across A30..J30 only.
    e.set_cell_value("Data", 1, 16_384, LiteralValue::Number(7.0))
        .unwrap();
    let dirty = dirty_cells(&e);
    let legacy = e.graph.legacy_closure_cells(&[(data, 0, 16_383)]);
    assert_eq!(dirty, legacy);
    let mut expected_row1: Vec<AuthCell> = vec![(s1, 0, 0)];
    for c0 in 0..10u32 {
        expected_row1.push((s1, 29, c0));
    }
    expected_row1.sort_unstable();
    assert_eq!(dirty, expected_row1);
    e.evaluate_all().unwrap();
    // Verify far-column edit propagated into filled-across values.
    let expected_r1_sum: f64 = (1..=8).sum::<u32>() as f64 + 7.0;
    for c in 1..=10u32 {
        assert_eq!(
            e.get_cell_value("Sheet1", 30, c),
            Some(LiteralValue::Number(expected_r1_sum))
        );
    }

    // Edit Data!C50 (outside every whole-row band): 0 formulas dirty!
    e.set_cell_value("Data", 50, 3, LiteralValue::Number(999.0))
        .unwrap();
    assert!(
        dirty_cells(&e).is_empty(),
        "editing outside all row bands must mark 0 formulas dirty"
    );

    // Edit Data!B5 (inside `$3:$13`): dirties col 1 row 5, col 2 rows 1..=3, and ALL 25 rows of col 3.
    e.set_cell_value("Data", 5, 2, LiteralValue::Number(50.0))
        .unwrap();
    let dirty = dirty_cells(&e);
    let legacy = e.graph.legacy_closure_cells(&[(data, 4, 1)]);
    assert_eq!(dirty, legacy);
    assert_eq!(dirty.len(), 1 + 3 + 25);
    e.evaluate_all().unwrap();
    assert!(dirty_cells(&e).is_empty());
}

/// Bit-exact value and error parity across per-cell (`family_execution=false`),
/// tier-1 family walk (`family_kernels=false`), and tier-3 `AggregateKernel`
/// (sequential and parallel), including mixed types, errors, `-0.0`, overflow,
/// sparse far columns/rows, empty sheets, out-of-extent rows, open-ended
/// ranges, and computed formula layers.
#[test]
fn whole_row_value_and_error_parity_across_edge_cases() {
    type CellPos = (&'static str, u32, u32);
    let mut values: Vec<(CellPos, LiteralValue)> = Vec::new();
    let mut formulas: Vec<(CellPos, String)> = Vec::new();

    // Populate Data rows 1..=45 across cols 1..=12 with mixed types, plus sparse far cells.
    for r in 1..=45u32 {
        for c in 1..=12u32 {
            values.push((("Data", r, c), mixed_cell(r * 13 + c * 5)));
        }
    }
    // Sparse far-column and far-row entries (crossing chunk boundaries).
    values.push((("Data", 5, 120), LiteralValue::Number(123.5)));
    values.push((("Data", 40, 120), LiteralValue::Number(-42.25)));
    values.push((
        ("Data", 12, 50),
        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)),
    ));
    // All-empty row inside sheet bounds (row 30) to test AVERAGE -> #DIV/0!, MIN/MAX/SUM/COUNT -> 0.
    values.retain(|&((s, r, _), _)| !(s == "Data" && r == 30));

    // Computed layer on Calc sheet so whole-row formulas also read computed_overlay values!
    for r in 1..=40u32 {
        formulas.push((("Calc", r, 1), format!("=Data!A{r}*2")));
        formulas.push((("Calc", r, 2), format!("=Data!B{r}+1")));
    }

    // 55 rows of formulas (> Data's 45 rows, so rows 46..=55 test past-nrows clamping!).
    let f_rows = 55u32;
    for r in 1..=f_rows {
        formulas.push((("Sheet1", r, 1), format!("=SUM(Data!{r}:{r})")));
        formulas.push((("Sheet1", r, 2), format!("=SUM(Data!{}:{})", r + 2, r + 12)));
        formulas.push((("Sheet1", r, 3), format!("=AVERAGE(Data!{r}:{r})")));
        formulas.push((("Sheet1", r, 4), format!("=MIN(Data!{r}:{r})")));
        formulas.push((("Sheet1", r, 5), format!("=MAX(Data!{r}:{r})")));
        formulas.push((("Sheet1", r, 6), format!("=COUNT(Data!{r}:{r})")));
        formulas.push((("Sheet1", r, 7), "=SUM(Data!$1:$1)".into()));
        formulas.push((("Sheet1", r, 8), "=SUM(Data!A:A)".into()));
        formulas.push((("Sheet1", r, 9), format!("=SUM(Data!A{r}:A)")));
        formulas.push((("Sheet1", r, 10), format!("=SUM(Data!B{r}:{r})")));
        formulas.push((
            ("Sheet1", r, 11),
            format!("=SUM(Data!{r}:{r},Data!A{r}:C{r})"),
        ));
        formulas.push((("Sheet1", r, 12), format!("=SUM(Calc!{r}:{r})")));
        formulas.push((("Sheet1", r, 13), format!("=SUM(EmptySheet!{r}:{r})")));
        formulas.push((("Sheet1", r, 14), format!("=AVERAGE(EmptySheet!{r}:{r})")));
        formulas.push((("Sheet1", r, 15), format!("=MIN(EmptySheet!{r}:{r})")));
        formulas.push((("Sheet1", r, 16), format!("=COUNT(EmptySheet!{r}:{r})")));
    }

    let edits: Vec<(CellPos, LiteralValue)> = vec![
        (
            ("Data", 7, 3),
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
        ),
        (("Data", 12, 50), LiteralValue::Number(5.0)), // clear error
        (("Data", 30, 80), LiteralValue::Number(9.5)), // populate previously empty row
        (("Data", 5, 1), LiteralValue::Number(1e308)), // trigger overflow (#NUM!)
        (("Data", 5, 2), LiteralValue::Number(1e308)),
        (("Data", 5, 1), LiteralValue::Number(-0.0)), // recover with -0.0
        (("Data", 1, 1), LiteralValue::Number(42.0)), // edit invariant $1:$1 and A:A
    ];

    let run_mode = |config: EvalConfig| -> (Vec<Vec<String>>, u64) {
        let mut e = Engine::new(TestWorkbook::new(), config);
        e.add_sheet("EmptySheet").unwrap();
        for &((s, r, c), ref v) in &values {
            e.set_cell_value(s, r, c, v.clone()).unwrap();
        }
        for &((s, r, c), ref f) in &formulas {
            e.set_cell_formula(s, r, c, parse(f).unwrap()).unwrap();
        }
        let snap = |e: &Engine<TestWorkbook>| -> Vec<String> {
            formulas
                .iter()
                .map(|&((s, r, c), _)| val_key(e.get_cell_value(s, r, c)))
                .collect()
        };
        let mut history = Vec::new();
        e.evaluate_all().unwrap();
        history.push(snap(&e));
        for &((s, r, c), ref v) in &edits {
            e.set_cell_value(s, r, c, v.clone()).unwrap();
            e.evaluate_all().unwrap();
            history.push(snap(&e));
        }
        (history, e.aggregate_kernel_members_for_test())
    };

    for parallel in [false, true] {
        let base = EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let (oracle, oracle_kernels) = run_mode(EvalConfig {
            family_execution: false,
            ..base.clone()
        });
        let (tier1, tier1_kernels) = run_mode(EvalConfig {
            family_kernels: false,
            ..base.clone()
        });
        let (kernels, kernel_members) = run_mode(base);
        assert_eq!(oracle_kernels, 0);
        assert_eq!(tier1_kernels, 0);
        assert!(
            kernel_members >= u64::from(f_rows) * 16,
            "expected whole-row/open-range aggregate kernels to run (got {kernel_members}, parallel={parallel})"
        );
        assert_eq!(
            tier1, oracle,
            "tier1 vs oracle mismatch (parallel={parallel})"
        );
        assert_eq!(
            kernels, oracle,
            "kernels vs oracle mismatch (parallel={parallel})"
        );
    }
}

/// Structural row/column inserts and deletes + undo/redo on whole-row formula
/// families match the uncompressed per-cell engine identically in values and
/// adjusted formula texts.
#[test]
fn whole_row_structural_edits_and_undo_redo_parity() {
    fn make_engine(fast: bool) -> Engine<TestWorkbook> {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                formula_compression: fast,
                family_execution: fast,
                family_kernels: fast,
                ..arrow_eval_config()
            },
        );
        for r in 1..=30u32 {
            for c in 1..=6u32 {
                e.set_cell_value("Data", r, c, LiteralValue::Number((r * 10 + c) as f64))
                    .unwrap();
            }
        }
        for r in 1..=20u32 {
            e.set_cell_formula(
                "Sheet1",
                r,
                1,
                parse(format!("=SUM(Data!{r}:{r})")).unwrap(),
            )
            .unwrap();
            e.set_cell_formula(
                "Sheet1",
                r,
                2,
                parse(format!("=SUM(Data!{}:{})", r + 2, r + 6)).unwrap(),
            )
            .unwrap();
            e.set_cell_formula("Sheet1", r, 3, parse("=SUM(Data!$2:$5)").unwrap())
                .unwrap();
            e.set_cell_formula(
                "Sheet1",
                r,
                4,
                parse(format!("=AVERAGE(Data!{r}:{r})")).unwrap(),
            )
            .unwrap();
        }
        e.evaluate_all().unwrap();
        e
    }

    fn snapshot(e: &Engine<TestWorkbook>) -> Vec<String> {
        let mut out = Vec::new();
        for sheet in ["Sheet1", "Data"] {
            let sid = e.graph.sheet_id(sheet).unwrap();
            for r in 1..=36u32 {
                for c in 1..=8u32 {
                    let v = val_key(e.get_cell_value(sheet, r, c));
                    let cell = CellRef::new(sid, Coord::from_excel(r, c, true, true));
                    let f = e
                        .graph
                        .get_vertex_id_for_address(&cell)
                        .and_then(|vid| e.graph.get_formula(vid))
                        .map(|a| a.to_string());
                    out.push(format!("{sheet}!{r},{c}: {v} {f:?}"));
                }
            }
        }
        out
    }

    let mut fast = make_engine(true);
    let mut oracle = make_engine(false);
    let mut fast_log = ChangeLog::new();
    let mut oracle_log = ChangeLog::new();
    assert_eq!(snapshot(&fast), snapshot(&oracle), "initial state");

    let data_fast = fast.graph.sheet_id("Data").unwrap();
    let data_oracle = oracle.graph.sheet_id("Data").unwrap();
    let s1_fast = fast.graph.sheet_id("Sheet1").unwrap();
    let s1_oracle = oracle.graph.sheet_id("Sheet1").unwrap();

    // 1. Insert 2 rows in Data at 0-based row 3 (inside `$2:$5` and relative bands).
    fast_log.begin_compound("insert_data_rows".into());
    fast.edit_with_logger(&mut fast_log, |ed| ed.insert_rows(data_fast, 3, 2))
        .unwrap()
        .unwrap();
    fast_log.end_compound();
    oracle_log.begin_compound("insert_data_rows".into());
    oracle
        .edit_with_logger(&mut oracle_log, |ed| ed.insert_rows(data_oracle, 3, 2))
        .unwrap()
        .unwrap();
    oracle_log.end_compound();
    fast.evaluate_all().unwrap();
    oracle.evaluate_all().unwrap();
    assert_eq!(snapshot(&fast), snapshot(&oracle), "after insert_rows Data");

    // 2. Delete column C on Data (whole-row references stay whole-row, values shrink).
    fast_log.begin_compound("delete_data_col".into());
    fast.edit_with_logger(&mut fast_log, |ed| ed.delete_columns(data_fast, 2, 1))
        .unwrap()
        .unwrap();
    fast_log.end_compound();
    oracle_log.begin_compound("delete_data_col".into());
    oracle
        .edit_with_logger(&mut oracle_log, |ed| ed.delete_columns(data_oracle, 2, 1))
        .unwrap()
        .unwrap();
    oracle_log.end_compound();
    fast.evaluate_all().unwrap();
    oracle.evaluate_all().unwrap();
    assert_eq!(
        snapshot(&fast),
        snapshot(&oracle),
        "after delete_columns Data"
    );

    // 3. Insert column on Data and populate a value in the new column.
    fast_log.begin_compound("insert_data_col".into());
    fast.edit_with_logger(&mut fast_log, |ed| ed.insert_columns(data_fast, 1, 1))
        .unwrap()
        .unwrap();
    fast_log.end_compound();
    oracle_log.begin_compound("insert_data_col".into());
    oracle
        .edit_with_logger(&mut oracle_log, |ed| ed.insert_columns(data_oracle, 1, 1))
        .unwrap()
        .unwrap();
    oracle_log.end_compound();
    fast.set_cell_value("Data", 3, 2, LiteralValue::Number(77.0))
        .unwrap();
    oracle
        .set_cell_value("Data", 3, 2, LiteralValue::Number(77.0))
        .unwrap();
    fast.evaluate_all().unwrap();
    oracle.evaluate_all().unwrap();
    assert_eq!(
        snapshot(&fast),
        snapshot(&oracle),
        "after insert_columns Data + edit"
    );

    // 4. Delete row 8 on Data (invalidates single-row `=SUM(Data!8:8)` -> `#REF!`, shrinks bands).
    fast_log.begin_compound("delete_data_row".into());
    fast.edit_with_logger(&mut fast_log, |ed| ed.delete_rows(data_fast, 7, 1))
        .unwrap()
        .unwrap();
    fast_log.end_compound();
    oracle_log.begin_compound("delete_data_row".into());
    oracle
        .edit_with_logger(&mut oracle_log, |ed| ed.delete_rows(data_oracle, 7, 1))
        .unwrap()
        .unwrap();
    oracle_log.end_compound();
    fast.evaluate_all().unwrap();
    oracle.evaluate_all().unwrap();
    assert_eq!(snapshot(&fast), snapshot(&oracle), "after delete_rows Data");

    // 5. Insert rows on Sheet1 itself (shifting the formula family down).
    fast_log.begin_compound("insert_s1_rows".into());
    fast.edit_with_logger(&mut fast_log, |ed| ed.insert_rows(s1_fast, 5, 3))
        .unwrap()
        .unwrap();
    fast_log.end_compound();
    oracle_log.begin_compound("insert_s1_rows".into());
    oracle
        .edit_with_logger(&mut oracle_log, |ed| ed.insert_rows(s1_oracle, 5, 3))
        .unwrap()
        .unwrap();
    oracle_log.end_compound();
    fast.evaluate_all().unwrap();
    oracle.evaluate_all().unwrap();
    assert_eq!(
        snapshot(&fast),
        snapshot(&oracle),
        "after insert_rows Sheet1"
    );

    // 6. Undo & redo the structural edits and verify parity at each step.
    let mut fast_undo = UndoEngine::new();
    let mut oracle_undo = UndoEngine::new();
    for step in 0..2 {
        fast.undo_logged(&mut fast_undo, &mut fast_log).unwrap();
        oracle
            .undo_logged(&mut oracle_undo, &mut oracle_log)
            .unwrap();
        fast.evaluate_all().unwrap();
        oracle.evaluate_all().unwrap();
        assert_eq!(snapshot(&fast), snapshot(&oracle), "after undo step {step}");
    }
    for step in 0..2 {
        fast.redo_logged(&mut fast_undo, &mut fast_log).unwrap();
        oracle
            .redo_logged(&mut oracle_undo, &mut oracle_log)
            .unwrap();
        fast.evaluate_all().unwrap();
        oracle.evaluate_all().unwrap();
        assert_eq!(snapshot(&fast), snapshot(&oracle), "after redo step {step}");
    }
}

/// Repeatable `--release` timing probe for whole-row family workloads:
/// - 10,000 `=SUM(Data!1:1)` .. `=SUM(Data!10000:10000)` on a 64-col wide sheet
/// - 10,000 `=AVERAGE(Data!1:1)` .. `=AVERAGE(Data!10000:10000)` on a 64-col wide sheet
/// - 5,000 sliding-window `=SUM(Data!3:13)` .. `=SUM(Data!5002:5012)` on a 64-col wide sheet
/// - 5,000 invariant `=SUM(Data!$1:$1)` and `=SUM(Data!A:A)` filled down
///
/// Measures full evaluation + incremental single-cell edit recalculation.
#[test]
#[ignore = "manual repeatable timing probe; run with --release -- --ignored --nocapture"]
fn whole_row_family_release_timing_probe() {
    fn build_wide_engine(rows: u32, cols: usize, config: EvalConfig) -> Engine<TestWorkbook> {
        let mut e = Engine::new(TestWorkbook::new(), config);
        let mut ab = e.begin_bulk_ingest_arrow();
        ab.add_sheet("Data", cols, 1024);
        let mut buf = Vec::with_capacity(cols);
        for r in 0..rows {
            buf.clear();
            for c in 0..cols {
                buf.push(LiteralValue::Number(
                    ((r as usize * 7 + c * 3) % 97) as f64 + 0.5,
                ));
            }
            ab.append_row("Data", &buf).unwrap();
        }
        ab.finish().unwrap();
        e
    }

    fn bench_case(
        label: &str,
        data_rows: u32,
        data_cols: usize,
        formula_rows: u32,
        make_formula: impl Fn(u32) -> String,
        edit_row: u32,
        edit_col: u32,
        parallel: bool,
    ) {
        let cfg = EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let mut e = build_wide_engine(data_rows, data_cols, cfg);
        for r in 1..=formula_rows {
            e.set_cell_formula("Sheet1", r, 1, parse(make_formula(r)).unwrap())
                .unwrap();
        }
        let t0 = std::time::Instant::now();
        e.evaluate_all().unwrap();
        let cold_us = t0.elapsed().as_micros();

        let mut full_us_runs = Vec::with_capacity(5);
        for iter in 0..5u32 {
            let mut e2 = build_wide_engine(
                data_rows,
                data_cols,
                EvalConfig {
                    enable_parallel: parallel,
                    ..arrow_eval_config()
                },
            );
            for r in 1..=formula_rows {
                e2.set_cell_formula("Sheet1", r, 1, parse(make_formula(r)).unwrap())
                    .unwrap();
            }
            let _ = e2.graph.authority();
            let t = std::time::Instant::now();
            e2.evaluate_all().unwrap();
            full_us_runs.push(t.elapsed().as_micros());
            std::hint::black_box(e2.get_cell_value("Sheet1", (iter % formula_rows) + 1, 1));
        }
        full_us_runs.sort_unstable();
        let full_median_us = full_us_runs[full_us_runs.len() / 2];

        let t_inc = std::time::Instant::now();
        for step in 0..100u32 {
            let r = ((edit_row - 1 + step * 37) % formula_rows) + 1;
            e.set_cell_value(
                "Data",
                r,
                edit_col,
                LiteralValue::Number(1000.0 + step as f64),
            )
            .unwrap();
            e.evaluate_all().unwrap();
        }
        let inc_100_us = t_inc.elapsed().as_micros();

        eprintln!(
            "[whole-row-probe] {label:<28} par={parallel:<5} cold_full={cold_us:>8}us  median_full={full_median_us:>8}us  100x_inc_edit={inc_100_us:>7}us  family_members={}  agg_kernel={}",
            e.family_members_for_test(),
            e.aggregate_kernel_members_for_test(),
        );
    }

    for parallel in [false, true] {
        bench_case(
            "SUM(Data!r:r) 10k x 64c",
            10_000,
            64,
            10_000,
            |r| format!("=SUM(Data!{r}:{r})"),
            500,
            17,
            parallel,
        );
        bench_case(
            "AVERAGE(Data!r:r) 10k x 64c",
            10_000,
            64,
            10_000,
            |r| format!("=AVERAGE(Data!{r}:{r})"),
            500,
            17,
            parallel,
        );
        bench_case(
            "SUM(Data!r:r+10) 5k x 64c",
            5_020,
            64,
            5_000,
            |r| format!("=SUM(Data!{}:{})", r + 2, r + 12),
            500,
            17,
            parallel,
        );
        bench_case(
            "SUM(Data!$1:$1) 5k x 64c",
            1_000,
            64,
            5_000,
            |_| "=SUM(Data!$1:$1)".to_string(),
            1,
            17,
            parallel,
        );
        bench_case(
            "SUM(Data!A:A) 5k x 64c",
            1_000,
            64,
            5_000,
            |_| "=SUM(Data!A:A)".to_string(),
            500,
            1,
            parallel,
        );
    }
}
