//! Differential property test gate and targeted invariant tests for the unified
//! engine region-write primitive (`v0.16.0` Decision 2: B1, B3, B5, E1–E6, E9, E10).

use crate::arrow_store::{FormatRuns, IngestBuilder};
use crate::engine::{
    BulkCellTag, BulkColumn, CellBatch, DateSystem, DeferredFormulaPackage, DeferredFormulaReplay,
    DeferredReplayFormula, Engine, EvalConfig, FormulaCompressedSourceReport,
    FormulaReplayCoordinateDisposition, FormulaReplayDisposition, PackedColumnBlock,
    PlacementDomainTransport, SourceCoord, SourceFamilyId, SourceFamilyMembers,
    SourceFormulaFamily, SourceFormulaOrder,
};
use crate::format::FormatId;
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use chrono::{NaiveDate, NaiveTime};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use proptest::prelude::*;
use std::sync::Arc;

fn make_engine() -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn literal_eq_strict(a: &Option<LiteralValue>, b: &Option<LiteralValue>) -> bool {
    match (a, b) {
        (None, None)
        | (None, Some(LiteralValue::Empty))
        | (Some(LiteralValue::Empty), None)
        | (Some(LiteralValue::Empty), Some(LiteralValue::Empty)) => true,
        (Some(LiteralValue::Number(x)), Some(LiteralValue::Number(y))) => {
            x.to_bits() == y.to_bits()
        }
        (Some(LiteralValue::Error(e1)), Some(LiteralValue::Error(e2))) => e1.kind == e2.kind,
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

fn assert_engines_equivalent(
    ref_eng: &Engine<TestWorkbook>,
    bulk_eng: &Engine<TestWorkbook>,
    max_row: u32,
    max_col: u32,
    label: &str,
) {
    // Compare Data sheet cells (value, effective format, staged formula, live formula presence).
    for r in 1..=max_row {
        for c in 1..=max_col {
            let v_ref = ref_eng.get_cell_value("Data", r, c);
            let v_bulk = bulk_eng.get_cell_value("Data", r, c);
            assert!(
                literal_eq_strict(&v_ref, &v_bulk),
                "[{label}] Data!R{r}C{c} value mismatch: ref={v_ref:?} vs bulk={v_bulk:?}"
            );

            let f_ref = ref_eng.effective_format_id("Data", r, c);
            let f_bulk = bulk_eng.effective_format_id("Data", r, c);
            assert_eq!(
                f_ref, f_bulk,
                "[{label}] Data!R{r}C{c} format mismatch: ref={f_ref:?} vs bulk={f_bulk:?}"
            );

            let s_ref = ref_eng.get_staged_formula_text("Data", r, c);
            let s_bulk = bulk_eng.get_staged_formula_text("Data", r, c);
            assert_eq!(
                s_ref, s_bulk,
                "[{label}] Data!R{r}C{c} staged formula mismatch"
            );

            let g_ref = ref_eng.get_cell("Data", r, c).and_then(|(ast, _)| ast);
            let g_bulk = bulk_eng.get_cell("Data", r, c).and_then(|(ast, _)| ast);
            assert_eq!(
                g_ref.is_some(),
                g_bulk.is_some(),
                "[{label}] Data!R{r}C{c} formula presence mismatch"
            );
        }
    }

    // Compare used extents on Data.
    if let (Some(sid_ref), Some(sid_bulk)) = (ref_eng.sheet_id("Data"), bulk_eng.sheet_id("Data")) {
        assert_eq!(
            ref_eng
                .graph
                .used_row_bounds_for_columns(sid_ref, 0, max_col),
            bulk_eng
                .graph
                .used_row_bounds_for_columns(sid_bulk, 0, max_col),
            "[{label}] used row bounds mismatch"
        );
        assert_eq!(
            ref_eng.graph.used_col_bounds_for_rows(sid_ref, 0, max_row),
            bulk_eng
                .graph
                .used_col_bounds_for_rows(sid_bulk, 0, max_row),
            "[{label}] used col bounds mismatch"
        );
    }

    // Compare cross-sheet Summary dependents.
    for r in 1..=6 {
        let v_ref = ref_eng.get_cell_value("Summary", r, 1);
        let v_bulk = bulk_eng.get_cell_value("Summary", r, 1);
        assert!(
            literal_eq_strict(&v_ref, &v_bulk),
            "[{label}] Summary!R{r}C1 mismatch: ref={v_ref:?} vs bulk={v_bulk:?}"
        );
    }
}

#[derive(Clone, Debug)]
enum SeedKind {
    Empty,
    Number(f64),
    Text(&'static str),
    Bool(bool),
    Date(u32), // days offset from 2024-01-01
    Error(u8), // 1..=7
    FormulaAdd,
    Spill3x1,
    Spill1x3,
    StagedFormula,
    DateFormula,
}

fn arb_seed_kind() -> impl Strategy<Value = SeedKind> {
    prop_oneof![
        4 => Just(SeedKind::Empty),
        4 => prop::sample::select(vec![-5.5, -0.0, 0.0, 1.0, 42.25, 100.0]).prop_map(SeedKind::Number),
        2 => prop::sample::select(vec!["alpha", "beta", "gamma", ""]).prop_map(SeedKind::Text),
        2 => any::<bool>().prop_map(SeedKind::Bool),
        2 => (0u32..365).prop_map(SeedKind::Date),
        1 => (1u8..=7).prop_map(SeedKind::Error),
        2 => Just(SeedKind::FormulaAdd),
        1 => Just(SeedKind::Spill3x1),
        1 => Just(SeedKind::Spill1x3),
        1 => Just(SeedKind::StagedFormula),
        1 => Just(SeedKind::DateFormula),
    ]
}

fn arb_literal() -> impl Strategy<Value = LiteralValue> {
    prop_oneof![
        3 => Just(LiteralValue::Empty),
        4 => prop::sample::select(vec![-10.5, -0.0, 0.0, 1.0, 7.5, 99.0, f64::NAN, f64::INFINITY])
            .prop_map(LiteralValue::Number),
        2 => prop::sample::select(vec!["x", "y", "alpha", ""]).prop_map(|s| LiteralValue::Text(s.to_string())),
        2 => any::<bool>().prop_map(LiteralValue::Boolean),
        2 => (0u32..500).prop_map(|d| {
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(d as i64))
        }),
        1 => (0u32..12).prop_map(|h| {
            LiteralValue::Time(NaiveTime::from_hms_opt(h, 30, 0).unwrap())
        }),
        1 => (1i64..5).prop_map(|h| LiteralValue::Duration(chrono::Duration::hours(h))),
        1 => prop::sample::select(vec![
            ExcelErrorKind::Div,
            ExcelErrorKind::Na,
            ExcelErrorKind::Value,
            ExcelErrorKind::Ref,
            ExcelErrorKind::Num,
        ])
        .prop_map(|k| LiteralValue::Error(ExcelError::new(k))),
    ]
}

/// Seed two identical engines with base formats, values, formulas, live spills,
/// blocked spill waiters, staged formulas, an empty table, and cross-sheet readers.
fn seed_pair(
    grid: &[Vec<SeedKind>],
    seed_base_date_col: bool,
    seed_empty_table: bool,
) -> (Engine<TestWorkbook>, Engine<TestWorkbook>) {
    let mut ref_eng = make_engine();
    let mut bulk_eng = make_engine();

    for eng in [&mut ref_eng, &mut bulk_eng] {
        eng.add_sheet("Data").unwrap();
        eng.add_sheet("Summary").unwrap();

        if seed_base_date_col {
            // Seed an Arrow base sheet with a base DATE format lane in col 2 (B1:B10).
            let mut b = IngestBuilder::new("Data", 6, 32 * 1024, DateSystem::Excel1900);
            for _ in 0..10 {
                b.append_row(&[
                    LiteralValue::Empty,
                    LiteralValue::Number(45292.0),
                    LiteralValue::Empty,
                    LiteralValue::Empty,
                    LiteralValue::Empty,
                    LiteralValue::Empty,
                ])
                .unwrap();
            }
            let mut asheet = b.finish();
            if let Some(ch) = asheet.columns[1].chunk_mut(0) {
                ch.format = FormatRuns::replace_uniform_range(None, 10, 0, 10, FormatId::DATE.0);
            }
            let store = eng.sheet_store_mut();
            store.sheets.retain(|s| s.name.as_ref() != "Data");
            store.sheets.push(asheet);
        }

        for (ri, row) in grid.iter().enumerate() {
            let r = (ri as u32) + 1;
            for (ci, kind) in row.iter().enumerate() {
                let c = (ci as u32) + 1;
                match kind {
                    SeedKind::Empty => {}
                    SeedKind::Number(n) => {
                        eng.set_cell_value("Data", r, c, LiteralValue::Number(*n))
                            .unwrap();
                    }
                    SeedKind::Text(s) => {
                        eng.set_cell_value("Data", r, c, LiteralValue::Text((*s).to_string()))
                            .unwrap();
                    }
                    SeedKind::Bool(b) => {
                        eng.set_cell_value("Data", r, c, LiteralValue::Boolean(*b))
                            .unwrap();
                    }
                    SeedKind::Date(d) => {
                        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
                            + chrono::Duration::days(*d as i64);
                        eng.set_cell_value("Data", r, c, LiteralValue::Date(date))
                            .unwrap();
                    }
                    SeedKind::Error(code) => {
                        let err = ExcelError::new(crate::arrow_store::unmap_error_code(*code));
                        eng.set_cell_value("Data", r, c, LiteralValue::Error(err))
                            .unwrap();
                    }
                    SeedKind::FormulaAdd => {
                        let formula = if r == 1 && c == 1 { "=10+20" } else { "=A1+10" };
                        let ast = parse(formula).unwrap();
                        eng.set_cell_formula("Data", r, c, ast).unwrap();
                    }
                    SeedKind::Spill3x1 => {
                        let ast = parse("=SEQUENCE(3,1)").unwrap();
                        eng.set_cell_formula("Data", r, c, ast).unwrap();
                    }
                    SeedKind::Spill1x3 => {
                        let ast = parse("=SEQUENCE(1,3)").unwrap();
                        eng.set_cell_formula("Data", r, c, ast).unwrap();
                    }
                    SeedKind::StagedFormula => {
                        eng.stage_formula_text("Data", r, c, "=100+23".to_string());
                    }
                    SeedKind::DateFormula => {
                        let ast = parse("=DATE(2024,6,15)").unwrap();
                        eng.set_cell_formula("Data", r, c, ast).unwrap();
                    }
                }
            }
        }

        if seed_empty_table {
            let sid = eng.sheet_id("Data").unwrap();
            let range = RangeRef::new(
                CellRef::new(sid, Coord::from_excel(1, 3, true, true)),
                CellRef::new(sid, Coord::from_excel(3, 4, true, true)),
            );
            eng.define_table(
                "T_Empty",
                range,
                true,
                vec!["C1".into(), "C2".into()],
                false,
            )
            .unwrap();
            eng.graph.resize_table_body("T_Empty", range, true).unwrap();
        }

        // Cross-sheet readers on Summary (whole-column + bounded + COUNTIFS + table).
        eng.set_cell_formula("Summary", 1, 1, parse("=SUM(Data!A:A)").unwrap())
            .unwrap();
        eng.set_cell_formula("Summary", 2, 1, parse("=SUM(Data!A1:F10)").unwrap())
            .unwrap();
        eng.set_cell_formula(
            "Summary",
            3,
            1,
            parse(r#"=COUNTIFS(Data!A1:A10,"alpha")"#).unwrap(),
        )
        .unwrap();
        eng.set_cell_formula("Summary", 4, 1, parse("=COUNTA(Data!A1:F10)").unwrap())
            .unwrap();
        eng.set_cell_formula("Summary", 5, 1, parse("=SUM(Data!B:B)").unwrap())
            .unwrap();
        if seed_empty_table {
            eng.set_cell_formula("Summary", 6, 1, parse("=SUM(T_Empty[C1])").unwrap())
                .unwrap();
        }

        // Initial evaluation so spills, computed overlays, and derived formats are live.
        eng.evaluate_all().unwrap();
    }

    (ref_eng, bulk_eng)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        .. ProptestConfig::default()
    })]

    #[test]
    fn proptest_dense_and_ragged_values_matches_cell_by_cell(
        seed_grid in prop::collection::vec(prop::collection::vec(arb_seed_kind(), 6..=6), 6..=6),
        seed_base_date_col in any::<bool>(),
        seed_empty_table in any::<bool>(),
        start_row in 1u32..=5,
        start_col in 1u32..=4,
        write_rows in prop::collection::vec(prop::collection::vec(arb_literal(), 0..=4), 1..=5),
    ) {
        let (mut ref_eng, mut bulk_eng) = seed_pair(&seed_grid, seed_base_date_col, seed_empty_table);

        // Reference: cell-by-cell set_cell_value + clear_staged_formula_text.
        for (ri, row) in write_rows.iter().enumerate() {
            let r = start_row + ri as u32;
            for (ci, val) in row.iter().enumerate() {
                let c = start_col + ci as u32;
                ref_eng
                    .set_cell_value("Data", r, c, val.clone().finite_or_num_error())
                    .unwrap();
                ref_eng.clear_staged_formula_text("Data", r, c);
            }
        }

        // Bulk unified primitive.
        bulk_eng
            .set_region_values("Data", start_row, start_col, &write_rows)
            .unwrap();

        ref_eng.evaluate_all().unwrap();
        bulk_eng.evaluate_all().unwrap();

        assert_engines_equivalent(&ref_eng, &bulk_eng, 10, 8, "set_region_values");
    }

    #[test]
    fn proptest_dense_columns_matches_cell_by_cell(
        seed_grid in prop::collection::vec(prop::collection::vec(arb_seed_kind(), 6..=6), 6..=6),
        seed_base_date_col in any::<bool>(),
        seed_empty_table in any::<bool>(),
        start_row in 1u32..=5,
        start_col in 1u32..=3,
        nrows in 1usize..=5,
        f64_vals in prop::collection::vec(
            prop::sample::select(vec![-3.5, -0.0, 0.0, 1.0, 42.0, f64::NAN, f64::NEG_INFINITY]),
            5..=5
        ),
        f64_mask in any::<u8>(),
        i64_vals in prop::collection::vec(-1000i64..=1000, 5..=5),
        bool_vals in prop::collection::vec(any::<bool>(), 5..=5),
        str_vals in prop::collection::vec(prop::sample::select(vec!["alpha", "beta", "", "delta"]), 5..=5),
        date_serials in prop::collection::vec(45000.0f64..45500.0, 5..=5),
    ) {
        let (mut ref_eng, mut bulk_eng) = seed_pair(&seed_grid, seed_base_date_col, seed_empty_table);

        let f64_slice = &f64_vals[..nrows];
        let validity_arr = [f64_mask];
        let i64_slice = &i64_vals[..nrows];
        let bool_slice = &bool_vals[..nrows];

        // Pack Utf8 column
        let mut utf8_bytes = Vec::new();
        let mut utf8_offsets = vec![0u32];
        for s in &str_vals[..nrows] {
            utf8_bytes.extend_from_slice(s.as_bytes());
            utf8_offsets.push(utf8_bytes.len() as u32);
        }
        let date_slice: Vec<f64> = date_serials[..nrows].iter().map(|v| v.floor()).collect();

        // Reference cell-by-cell writes for the 6 columns:
        // col 0: Float64 w/ validity
        // col 1: Int64
        // col 2: Boolean
        // col 3: Utf8
        // col 4: Serial(DATE)
        // col 5: Null
        for ri in 0..nrows {
            let r = start_row + ri as u32;

            let v0 = if ((f64_mask >> ri) & 1) != 0 {
                LiteralValue::Number(f64_slice[ri]).finite_or_num_error()
            } else {
                LiteralValue::Empty
            };
            ref_eng.set_cell_value("Data", r, start_col, v0).unwrap();
            ref_eng.clear_staged_formula_text("Data", r, start_col);

            ref_eng
                .set_cell_value("Data", r, start_col + 1, LiteralValue::Number(i64_slice[ri] as f64))
                .unwrap();
            ref_eng.clear_staged_formula_text("Data", r, start_col + 1);

            ref_eng
                .set_cell_value("Data", r, start_col + 2, LiteralValue::Boolean(bool_slice[ri]))
                .unwrap();
            ref_eng.clear_staged_formula_text("Data", r, start_col + 2);

            ref_eng
                .set_cell_value("Data", r, start_col + 3, LiteralValue::Text(str_vals[ri].to_string()))
                .unwrap();
            ref_eng.clear_staged_formula_text("Data", r, start_col + 3);

            let d = formualizer_common::try_serial_to_date_for(DateSystem::Excel1900, date_slice[ri]).unwrap();
            ref_eng
                .set_cell_value("Data", r, start_col + 4, LiteralValue::Date(d))
                .unwrap();
            ref_eng.clear_staged_formula_text("Data", r, start_col + 4);

            ref_eng
                .set_cell_value("Data", r, start_col + 5, LiteralValue::Empty)
                .unwrap();
            ref_eng.clear_staged_formula_text("Data", r, start_col + 5);
        }

        let cols = [
            BulkColumn::Float64 {
                values: f64_slice,
                validity: Some(&validity_arr),
            },
            BulkColumn::Int64 {
                values: i64_slice,
                validity: None,
            },
            BulkColumn::Boolean {
                values: bool_slice,
                validity: None,
            },
            BulkColumn::Utf8 {
                bytes: &utf8_bytes,
                offsets: &utf8_offsets,
                validity: None,
            },
            BulkColumn::Serial {
                values: &date_slice,
                format: FormatId::DATE,
                validity: None,
            },
            BulkColumn::Null { len: nrows },
        ];

        bulk_eng
            .set_region_columns("Data", start_row, start_col, &cols)
            .unwrap();

        ref_eng.evaluate_all().unwrap();
        bulk_eng.evaluate_all().unwrap();

        assert_engines_equivalent(&ref_eng, &bulk_eng, 10, 9, "set_region_columns");
    }

    #[test]
    fn proptest_sparse_cells_with_duplicates_matches_cell_by_cell(
        seed_grid in prop::collection::vec(prop::collection::vec(arb_seed_kind(), 6..=6), 6..=6),
        seed_base_date_col in any::<bool>(),
        seed_empty_table in any::<bool>(),
        ops in prop::collection::vec(
            (1u32..=8, 1u32..=6, 0u8..=8, prop::sample::select(vec![-0.0, 0.0, 1.0, 2.0, 5.5, 45292.0, f64::NAN])),
            1..=20
        ),
    ) {
        let (mut ref_eng, mut bulk_eng) = seed_pair(&seed_grid, seed_base_date_col, seed_empty_table);

        // Build deduplicated string table: ["alpha", "beta", ""]
        let str_table = ["alpha", "beta", ""];
        let mut text_bytes = Vec::new();
        let mut text_offsets = vec![0u32];
        for s in str_table {
            text_bytes.extend_from_slice(s.as_bytes());
            text_offsets.push(text_bytes.len() as u32);
        }

        let mut rows = Vec::with_capacity(ops.len());
        let mut cols = Vec::with_capacity(ops.len());
        let mut tags = Vec::with_capacity(ops.len());
        let mut numbers = Vec::with_capacity(ops.len());

        for &(r, c, tag, raw_num) in &ops {
            let num = match tag {
                2 => if raw_num == 0.0 { 0.0 } else { 1.0 },
                3 => ((r + c) as usize % str_table.len()) as f64,
                4 => (((r + c) % 7) + 1) as f64,
                5 => if raw_num.is_finite() { 45292.0 } else { raw_num },
                6 => if raw_num.is_finite() { 1.0 } else { raw_num },
                7 => if raw_num.is_finite() { 45292.0 } else { raw_num },
                8 => if raw_num.is_finite() { if raw_num == 0.0 { 0.0 } else { 0.5 } } else { raw_num },
                _ => raw_num,
            };
            rows.push(r);
            cols.push(c);
            tags.push(tag);
            numbers.push(num);

            // Equivalent cell-by-cell LiteralValue for reference engine:
            let lit = match tag {
                0 => LiteralValue::Empty,
                1 => LiteralValue::Number(num).finite_or_num_error(),
                2 => LiteralValue::Boolean(num != 0.0),
                3 => LiteralValue::Text(str_table[num as usize].to_string()),
                4 => LiteralValue::Error(ExcelError::new(crate::arrow_store::unmap_error_code(num as u8))),
                5 => {
                    if num.is_finite() {
                        LiteralValue::DateTime(
                            formualizer_common::try_serial_to_datetime_subsec_for(DateSystem::Excel1900, num).unwrap()
                        )
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Num))
                    }
                }
                6 => {
                    if num.is_finite() {
                        LiteralValue::Duration(chrono::Duration::days(1))
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Num))
                    }
                }
                7 => {
                    if num.is_finite() {
                        LiteralValue::Date(
                            formualizer_common::try_serial_to_date_for(DateSystem::Excel1900, num).unwrap()
                        )
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Num))
                    }
                }
                8 => {
                    if num.is_finite() {
                        LiteralValue::Time(
                            formualizer_common::fraction_to_time(num).unwrap()
                        )
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Num))
                    }
                }
                _ => unreachable!(),
            };
            ref_eng.set_cell_value("Data", r, c, lit).unwrap();
            ref_eng.clear_staged_formula_text("Data", r, c);
        }

        let batch = CellBatch {
            rows: &rows,
            cols: &cols,
            tags: &tags,
            numbers: &numbers,
            text_bytes: &text_bytes,
            text_offsets: &text_offsets,
        };
        bulk_eng.set_region_cells("Data", &batch).unwrap();

        ref_eng.evaluate_all().unwrap();
        bulk_eng.evaluate_all().unwrap();

        assert_engines_equivalent(&ref_eng, &bulk_eng, 10, 8, "set_region_cells");
    }
}

#[test]
fn b1_bulk_write_releases_spill_child_of_external_anchor_and_preserves_user_value() {
    let mut eng = make_engine();
    eng.add_sheet("S").unwrap();
    // Anchor at A1 spills into A1:A5.
    eng.set_cell_formula("S", 1, 1, parse("=SEQUENCE(5,1)").unwrap())
        .unwrap();
    eng.evaluate_all().unwrap();
    assert_eq!(
        eng.get_cell_value("S", 3, 1),
        Some(LiteralValue::Number(3.0))
    );

    // Bulk overwrite ONLY A3:A4 (anchor A1 is outside the written rect!).
    let vals = [99.0, 100.0];
    let cols = [BulkColumn::Float64 {
        values: &vals,
        validity: None,
    }];
    eng.set_region_columns("S", 3, 1, &cols).unwrap();
    eng.evaluate_all().unwrap();

    // Anchor A1 must report #SPILL!, and A3:A4 must keep the user values!
    assert!(matches!(
        eng.get_cell_value("S", 1, 1),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Spill
    ));
    assert_eq!(
        eng.get_cell_value("S", 3, 1),
        Some(LiteralValue::Number(99.0))
    );
    assert_eq!(
        eng.get_cell_value("S", 4, 1),
        Some(LiteralValue::Number(100.0))
    );
    // Old non-intruded spill children A2 and A5 were cleared when A1 failed to spill.
    assert!(matches!(
        eng.get_cell_value("S", 2, 1),
        None | Some(LiteralValue::Empty)
    ));
    assert!(matches!(
        eng.get_cell_value("S", 5, 1),
        None | Some(LiteralValue::Empty)
    ));
}

#[test]
fn b3_bulk_write_clears_all_three_format_lanes_and_derived_formats() {
    let mut eng = make_engine();
    eng.add_sheet("S").unwrap();

    // Seed a 10-row sheet with base DATE format on col 1.
    let mut b = IngestBuilder::new("S", 1, 32 * 1024, DateSystem::Excel1900);
    for _ in 0..10 {
        b.append_row(&[LiteralValue::Number(45292.0)]).unwrap();
    }
    let mut asheet = b.finish();
    {
        let ch = asheet.columns[0].chunk_mut(0).unwrap();
        ch.format = FormatRuns::replace_uniform_range(None, 10, 0, 10, FormatId::DATE.0);
        ch.overlay.set_format(2, Some(FormatId::DATETIME));
        ch.computed_overlay.set_format(3, Some(FormatId::TIME));
    }
    {
        let store = eng.sheet_store_mut();
        store.sheets.retain(|s| s.name.as_ref() != "S");
        store.sheets.push(asheet);
    }
    // Also add a derived format at row 5 (0-based 4).
    eng.debug_record_derived_format_0based("S", 4, 0, Some(FormatId::DATE));

    // Overwrite rows 2..=6 (0-based 1..=5) with plain numbers via bulk write.
    let plain = [10.0, 20.0, 30.0, 40.0, 50.0];
    let cols = [BulkColumn::Float64 {
        values: &plain,
        validity: None,
    }];
    eng.set_region_columns("S", 2, 1, &cols).unwrap();

    // Untouched row 1 & row 7 keep their base DATE format.
    assert_eq!(eng.effective_format_id("S", 1, 1), Some(FormatId::DATE));
    assert_eq!(eng.effective_format_id("S", 7, 1), Some(FormatId::DATE));

    // Overwritten rows 2..=6 have ALL format lanes cleared and read back as Number.
    for (idx, r) in (2..=6).enumerate() {
        assert_eq!(
            eng.effective_format_id("S", r, 1),
            None,
            "row {r} format should be cleared"
        );
        assert_eq!(
            eng.get_cell_value("S", r, 1),
            Some(LiteralValue::Number(plain[idx]))
        );
    }
}

#[test]
fn b5_all_null_column_allocates_zero_chunks_and_dense_write_keeps_sparse_empty() {
    let mut eng = make_engine();
    eng.add_sheet("S").unwrap();

    let nums: Vec<f64> = (0..40_000).map(|i| i as f64).collect();
    let cols = [
        BulkColumn::Float64 {
            values: &nums,
            validity: None,
        },
        BulkColumn::Null { len: 40_000 },
    ];
    let report = eng.set_region_columns("S", 1, 1, &cols).unwrap();
    assert_eq!(report.start_row, 1);
    assert_eq!(report.end_row, 40_000);
    assert_eq!(report.start_col, 1);
    assert_eq!(report.end_col, 2);

    let asheet = eng.sheet_store().sheet("S").unwrap();
    assert_eq!(asheet.nrows, 40_000);
    assert_eq!(asheet.chunk_starts, vec![0, 32_768]);
    // Col 0 is dense (CriteriaMaskCache eligible).
    assert!(asheet.columns[0].sparse_chunks.is_empty());
    assert_eq!(asheet.columns[0].chunks.len(), 2);
    // Col 1 (all-null) allocated ZERO chunks!
    assert_eq!(asheet.columns[1].total_chunk_count(), 0);
}

#[test]
fn e2_computed_overlay_bytes_accounted_on_bulk_overwrite() {
    let mut eng = make_engine();
    eng.add_sheet("S").unwrap();
    for r in 1..=200 {
        eng.set_cell_formula("S", r, 1, parse(format!("={r}*2")).unwrap())
            .unwrap();
    }
    eng.evaluate_all().unwrap();
    let before = eng.debug_computed_overlay_bytes_estimate();
    assert!(
        before > 0,
        "evaluated formulas must populate computed_overlay"
    );

    // Overwrite all 200 formulas in one bulk write (full-chunk overwrite).
    let vals = vec![1.0; 200];
    eng.set_region_columns(
        "S",
        1,
        1,
        &[BulkColumn::Float64 {
            values: &vals,
            validity: None,
        }],
    )
    .unwrap();
    assert_eq!(
        eng.debug_computed_overlay_bytes_estimate(),
        0,
        "computed_overlay byte estimate must drop back to 0 after full overwrite"
    );
}

#[test]
fn e5_atomic_preflight_rejects_invalid_inputs_without_mutation() {
    let mut eng = make_engine();
    eng.add_sheet("S").unwrap();
    eng.set_cell_value("S", 1, 1, LiteralValue::Number(77.0))
        .unwrap();

    // 1. Int64 beyond ±2^53 rejected naming column.
    let bad_ints = [1i64, (1i64 << 53) + 1];
    let err = eng
        .set_region_columns(
            "S",
            1,
            1,
            &[BulkColumn::Int64 {
                values: &bad_ints,
                validity: None,
            }],
        )
        .unwrap_err();
    assert!(err.message().unwrap_or_default().contains("Column 0"));

    // 2. Unknown wire tags (9, 255/Skip, 99) rejected.
    for bad_tag in [9u8, 255u8, 99u8] {
        let block = PackedColumnBlock {
            tags: &[bad_tag],
            numbers: &[0.0],
            text_bytes: &[],
            text_offsets: &[],
        };
        assert!(
            eng.set_region_columns("S", 1, 1, &[BulkColumn::Mixed(block)])
                .is_err(),
            "tag {bad_tag} must fail preflight"
        );
    }

    // 3. Invalid error codes 0 and 14 rejected.
    for bad_code in [0.0, 14.0, f64::NAN] {
        let batch = CellBatch {
            rows: &[1],
            cols: &[1],
            tags: &[BulkCellTag::Error as u8],
            numbers: &[bad_code],
            text_bytes: &[],
            text_offsets: &[],
        };
        assert!(eng.set_region_cells("S", &batch).is_err());
    }

    // 4. Out-of-bounds string index & invalid UTF-8 rejected.
    let bad_str_idx = CellBatch {
        rows: &[1],
        cols: &[1],
        tags: &[BulkCellTag::Text as u8],
        numbers: &[1.0],
        text_bytes: b"hi",
        text_offsets: &[0, 2],
    };
    assert!(eng.set_region_cells("S", &bad_str_idx).is_err());

    let bad_utf8 = BulkColumn::Utf8 {
        bytes: &[0xff, 0xfe],
        offsets: &[0, 2],
        validity: None,
    };
    assert!(eng.set_region_columns("S", 1, 1, &[bad_utf8]).is_err());

    // 5. Short validity bitmap rejected.
    let vals = [1.0; 9];
    let short_mask = [0xffu8];
    assert!(
        eng.set_region_columns(
            "S",
            1,
            1,
            &[BulkColumn::Float64 {
                values: &vals,
                validity: Some(&short_mask),
            }]
        )
        .is_err()
    );

    // 6. Mismatched column lengths rejected.
    assert!(
        eng.set_region_columns(
            "S",
            1,
            1,
            &[
                BulkColumn::Float64 {
                    values: &[1.0, 2.0],
                    validity: None
                },
                BulkColumn::Float64 {
                    values: &[3.0],
                    validity: None
                },
            ]
        )
        .is_err()
    );

    // Existing cell must remain untouched after every failed preflight.
    assert_eq!(
        eng.get_cell_value("S", 1, 1),
        Some(LiteralValue::Number(77.0))
    );

    // 7. B1: CellBatch tags/rows length mismatch (both shorter and longer) must fail
    //    preflight cleanly before any vertex vacation or deferred-dirty leak.
    eng.set_cell_formula("S", 1, 1, parse("=40+2").unwrap())
        .unwrap();
    eng.set_cell_value("S", 2, 1, LiteralValue::Number(7.0))
        .unwrap();
    eng.set_cell_formula("S", 1, 3, parse("=A1+A2").unwrap())
        .unwrap();
    eng.evaluate_all().unwrap();
    assert_eq!(
        eng.get_cell_value("S", 1, 1),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(
        eng.get_cell_value("S", 1, 3),
        Some(LiteralValue::Number(49.0))
    );

    // tags.len() < rows.len()
    let short_tags = CellBatch {
        rows: &[1, 2],
        cols: &[1, 1],
        tags: &[BulkCellTag::Number as u8],
        numbers: &[5.0],
        text_bytes: &[],
        text_offsets: &[],
    };
    let err = eng.set_region_cells("S", &short_tags).unwrap_err();
    assert!(
        err.message().unwrap_or_default().contains("tags length"),
        "expected tags length mismatch error, got {err:?}"
    );
    assert!(
        eng.get_cell("S", 1, 1).and_then(|(ast, _)| ast).is_some(),
        "formula at A1 must not be vacated on failed preflight"
    );
    assert_eq!(
        eng.get_cell_value("S", 1, 1),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(
        eng.get_cell_value("S", 2, 1),
        Some(LiteralValue::Number(7.0))
    );

    // tags.len() > rows.len() (non-empty and empty rows)
    let long_tags = CellBatch {
        rows: &[1],
        cols: &[1],
        tags: &[BulkCellTag::Number as u8; 3],
        numbers: &[5.0, 6.0, 7.0],
        text_bytes: &[],
        text_offsets: &[],
    };
    assert!(eng.set_region_cells("S", &long_tags).is_err());

    let empty_rows_non_empty_tags = CellBatch {
        rows: &[],
        cols: &[],
        tags: &[BulkCellTag::Number as u8],
        numbers: &[5.0],
        text_bytes: &[],
        text_offsets: &[],
    };
    assert!(
        eng.set_region_cells("S", &empty_rows_non_empty_tags)
            .is_err()
    );

    assert!(
        eng.get_cell("S", 1, 1).and_then(|(ast, _)| ast).is_some(),
        "formula at A1 must remain intact after longer-tags rejection"
    );

    // Subsequent edit + evaluate_all must propagate dirty state cleanly.
    eng.set_cell_value("S", 2, 1, LiteralValue::Number(100.0))
        .unwrap();
    eng.evaluate_all().unwrap();
    assert_eq!(
        eng.get_cell_value("S", 1, 3),
        Some(LiteralValue::Number(142.0)),
        "recalc after rejected set_region_cells must stay healthy"
    );
}

#[test]
fn e6_range_suppress_on_deferred_shared_formula_package() {
    struct StaticReplay {
        records: Vec<DeferredReplayFormula>,
    }
    impl DeferredFormulaReplay for StaticReplay {
        fn formula_at(
            &mut self,
            row: u32,
            col: u32,
        ) -> Result<Option<DeferredReplayFormula>, String> {
            Ok(self
                .records
                .iter()
                .find(|r| r.row == row && r.col == col)
                .cloned())
        }

        fn replay(
            &mut self,
            disposition: &FormulaReplayDisposition,
        ) -> Result<Vec<DeferredReplayFormula>, String> {
            Ok(self
                .records
                .iter()
                .filter(|r| {
                    let coord = SourceCoord {
                        row: r.row - 1,
                        col: r.col - 1,
                    };
                    let d = r.family.map_or_else(
                        || disposition.ordinary_disposition(coord).0,
                        |f| disposition.shared_disposition(f, coord),
                    );
                    d != FormulaReplayCoordinateDisposition::Suppressed
                })
                .cloned()
                .collect())
        }
    }

    let mut eng = make_engine();
    eng.add_sheet("S").unwrap();
    // Seed values A1:A20 = 1..=20
    let col_a: Vec<f64> = (1..=20).map(|i| i as f64).collect();
    eng.set_region_columns(
        "S",
        1,
        1,
        &[BulkColumn::Float64 {
            values: &col_a,
            validity: None,
        }],
    )
    .unwrap();

    // Attach a deferred shared-formula package in B1:B20 (`=A1*10` .. `=A20*10`).
    let fid = SourceFamilyId {
        sheet_instance: 1,
        source_index: 0,
    };
    let records: Vec<DeferredReplayFormula> = (1..=20u32)
        .map(|r| DeferredReplayFormula {
            row: r,
            col: 2,
            text: format!("=A{r}*10"),
            source_order: SourceFormulaOrder::new(r as u64),
            family: Some(fid),
            partition_owner: None,
        })
        .collect();
    let family = SourceFormulaFamily {
        source_id: fid,
        source_order: SourceFormulaOrder::new(1),
        anchor_coord0: SourceCoord { row: 0, col: 1 },
        anchor_text: Arc::from("=A1*10"),
        member_count: 20,
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 19,
            col: 1,
        }),
    };
    let report = FormulaCompressedSourceReport {
        source_formula_records_spooled: 20,
        ..Default::default()
    };
    let pkg = DeferredFormulaPackage::new(
        "S".to_string(),
        report,
        vec![family],
        Vec::new(),
        Box::new(StaticReplay { records }),
    );
    eng.stage_deferred_formula_package(pkg);

    // Overwrite B5:B15 with literal 999.0 in one bulk write.
    let overrides = [999.0; 11];
    eng.set_region_columns(
        "S",
        5,
        2,
        &[BulkColumn::Float64 {
            values: &overrides,
            validity: None,
        }],
    )
    .unwrap();

    // Build graph & evaluate all.
    eng.build_graph_all().unwrap();
    eng.evaluate_all().unwrap();

    for r in 1..=20u32 {
        let expected = if (5..=15).contains(&r) {
            999.0
        } else {
            (r as f64) * 10.0
        };
        assert_eq!(
            eng.get_cell_value("S", r, 2),
            Some(LiteralValue::Number(expected)),
            "B{r} mismatch after deferred package range-suppress"
        );
    }
}

#[test]
fn e10_repeated_formula_bulk_overwrite_cycles_keep_retired_ids_bounded() {
    let mut eng = make_engine();
    eng.add_sheet("S").unwrap();
    let vals = vec![1.0; 50];

    for cycle in 0..5 {
        for r in 1..=50u32 {
            eng.set_cell_formula("S", r, 1, parse(format!("={r}+{cycle}")).unwrap())
                .unwrap();
        }
        eng.set_region_columns(
            "S",
            1,
            1,
            &[BulkColumn::Float64 {
                values: &vals,
                validity: None,
            }],
        )
        .unwrap();
        assert_eq!(
            eng.graph.retired_id_count(),
            50,
            "cycle {cycle}: retired_ids must revive same VertexIds and stay bounded at 50"
        );
    }
}

#[test]
fn n5_multi_chunk_full_partial_and_light_overlay_parity() {
    // Exercise all three chunk storage strategies (light overlay, partial rebuild,
    // full chunk replace) plus multi-chunk writes and sparse_chunks against per-cell
    // `set_cell_value` + `clear_staged_formula_text`.
    for &(total_rows, chunk_rows, light_rows_start, light_rows_len) in &[
        // 200 rows with chunk_rows=64 -> 4 chunks (64, 64, 64, 8), threshold=1:
        // 2-row write at rows 64..=65 touches 1 row in chunk 0 and 1 row in chunk 1 (both light overlay).
        (200usize, 64usize, 64u32, 2usize),
        // 250 rows with chunk_rows=100 -> 3 chunks (100, 100, 50), threshold=2:
        // 2-row write at rows 10..=11 touches 2 rows strictly inside chunk 0 (light overlay).
        (250usize, 100usize, 10u32, 2usize),
    ] {
        let mut ref_eng = make_engine();
        let mut bulk_eng = make_engine();
        let max_row = total_rows as u32;
        let max_col = 7u32;

        for eng in [&mut ref_eng, &mut bulk_eng] {
            eng.add_sheet("Data").unwrap();
            eng.add_sheet("Summary").unwrap();

            // Ingest base ArrowSheet with multiple chunks and a base DATE lane on col 2 (B)
            // plus DATETIME lane on col 3 (C) across every chunk.
            let mut b = IngestBuilder::new("Data", 6, chunk_rows, DateSystem::Excel1900);
            for r in 0..total_rows {
                b.append_row(&[
                    LiteralValue::Number((r + 1) as f64),
                    LiteralValue::Number(45_292.0 + (r % 30) as f64),
                    LiteralValue::Number(45_292.5 + (r % 10) as f64),
                    LiteralValue::Empty,
                    LiteralValue::Text(format!("row_{}", r % 7)),
                    LiteralValue::Boolean(r % 2 == 0),
                ])
                .unwrap();
            }
            let mut asheet = b.finish();
            for ch_idx in 0..asheet.chunk_starts.len() {
                let (_, ch_len) = asheet.chunk_extent(ch_idx).unwrap();
                if let Some(ch) = asheet.columns[1].chunk_mut(ch_idx) {
                    ch.format = FormatRuns::replace_uniform_range(
                        None,
                        ch_len,
                        0,
                        ch_len,
                        FormatId::DATE.0,
                    );
                }
                if let Some(ch) = asheet.columns[2].chunk_mut(ch_idx) {
                    ch.format = FormatRuns::replace_uniform_range(
                        None,
                        ch_len,
                        0,
                        ch_len,
                        FormatId::DATETIME.0,
                    );
                }
            }
            let store = eng.sheet_store_mut();
            store.sheets.retain(|s| s.name.as_ref() != "Data");
            store.sheets.push(asheet);

            // Seed user delta overlays, derived-format formulas, staged formulas, and a
            // cross-chunk SEQUENCE(70,1) spill starting at D50.
            eng.set_cell_value("Data", light_rows_start, 1, LiteralValue::Number(-99.5))
                .unwrap();
            eng.set_cell_formula(
                "Data",
                light_rows_start,
                2,
                parse("=DATE(2024,6,15)").unwrap(),
            )
            .unwrap();
            eng.set_cell_formula("Data", 50, 4, parse("=SEQUENCE(70,1)").unwrap())
                .unwrap();
            eng.stage_formula_text("Data", light_rows_start, 5, "=1+2".to_string());
            eng.stage_formula_text("Data", (chunk_rows as u32) + 5, 5, "=3+4".to_string());

            eng.set_cell_formula("Summary", 1, 1, parse("=SUM(Data!A:A)").unwrap())
                .unwrap();
            eng.set_cell_formula(
                "Summary",
                2,
                1,
                parse(format!("=SUM(Data!B1:B{total_rows})")).unwrap(),
            )
            .unwrap();
            eng.set_cell_formula(
                "Summary",
                3,
                1,
                parse(format!(r#"=COUNTIFS(Data!E1:E{total_rows},"row_0")"#)).unwrap(),
            )
            .unwrap();
            eng.set_cell_formula(
                "Summary",
                4,
                1,
                parse(format!("=COUNTA(Data!A1:G{total_rows})")).unwrap(),
            )
            .unwrap();
            eng.set_cell_formula("Summary", 5, 1, parse("=SUM(Data!D:D)").unwrap())
                .unwrap();
            eng.set_cell_formula("Summary", 6, 1, parse("=SUM(Data!G:G)").unwrap())
                .unwrap();

            eng.evaluate_all().unwrap();
        }

        // Step 1 (Light overlay via set_region_columns): small 2-row write hitting base DATE
        // lane, existing user delta overlay, derived-format formula, and staged formula.
        let light_a = vec![111.0; light_rows_len];
        let light_b = vec![222.0; light_rows_len];
        let light_e_bytes = b"patch0patch1";
        let light_e_offs = [0u32, 6, 12];
        bulk_eng
            .set_region_columns(
                "Data",
                light_rows_start,
                1,
                &[
                    BulkColumn::Float64 {
                        values: &light_a,
                        validity: None,
                    },
                    BulkColumn::Float64 {
                        values: &light_b,
                        validity: None,
                    },
                    BulkColumn::Null {
                        len: light_rows_len,
                    },
                    BulkColumn::Null {
                        len: light_rows_len,
                    },
                    BulkColumn::Utf8 {
                        bytes: light_e_bytes,
                        offsets: &light_e_offs[..=light_rows_len],
                        validity: None,
                    },
                ],
            )
            .unwrap();
        for i in 0..light_rows_len {
            let r = light_rows_start + i as u32;
            ref_eng
                .set_cell_value("Data", r, 1, LiteralValue::Number(light_a[i]))
                .unwrap();
            ref_eng
                .set_cell_value("Data", r, 2, LiteralValue::Number(light_b[i]))
                .unwrap();
            ref_eng
                .set_cell_value("Data", r, 3, LiteralValue::Empty)
                .unwrap();
            ref_eng
                .set_cell_value("Data", r, 4, LiteralValue::Empty)
                .unwrap();
            ref_eng
                .set_cell_value("Data", r, 5, LiteralValue::Text(format!("patch{i}")))
                .unwrap();
            for c in 1..=5 {
                ref_eng.clear_staged_formula_text("Data", r, c);
            }
        }
        // Confirm light-overlay actually populated the delta overlay on chunk 0 col 1 (B).
        {
            let asheet = bulk_eng.sheet_store().sheet("Data").unwrap();
            let (ch_idx, in_off) = asheet
                .chunk_of_row((light_rows_start - 1) as usize)
                .unwrap();
            let ch = asheet.columns[1].chunk(ch_idx).unwrap();
            assert!(
                ch.overlay.get(in_off).is_some(),
                "expected light-overlay path on chunk_rows={chunk_rows}"
            );
        }
        ref_eng.evaluate_all().unwrap();
        bulk_eng.evaluate_all().unwrap();
        assert_engines_equivalent(
            &ref_eng,
            &bulk_eng,
            max_row,
            max_col,
            &format!("chunk_rows={chunk_rows}/step1-light-overlay-cols"),
        );

        // Step 2 (Light overlay via set_region_cells + sparse chunk into unmaterialized col 7):
        let sparse_rows = [2u32, (chunk_rows as u32) + 2, max_row - 1];
        let sparse_cols = [2u32, 4u32, 7u32]; // col 2 base DATE, col 4 spill child, col 7 new sparse_chunk!
        let sparse_tags = [
            BulkCellTag::Number as u8,
            BulkCellTag::Number as u8,
            BulkCellTag::Date as u8,
        ];
        let sparse_nums = [777.0, 888.0, 45_300.0];
        let batch = CellBatch {
            rows: &sparse_rows,
            cols: &sparse_cols,
            tags: &sparse_tags,
            numbers: &sparse_nums,
            text_bytes: &[],
            text_offsets: &[],
        };
        bulk_eng.set_region_cells("Data", &batch).unwrap();
        ref_eng
            .set_cell_value(
                "Data",
                sparse_rows[0],
                sparse_cols[0],
                LiteralValue::Number(777.0),
            )
            .unwrap();
        ref_eng
            .set_cell_value(
                "Data",
                sparse_rows[1],
                sparse_cols[1],
                LiteralValue::Number(888.0),
            )
            .unwrap();
        ref_eng
            .set_cell_value(
                "Data",
                sparse_rows[2],
                sparse_cols[2],
                LiteralValue::Date(
                    formualizer_common::try_serial_to_date_for(DateSystem::Excel1900, 45_300.0)
                        .unwrap(),
                ),
            )
            .unwrap();
        ref_eng.evaluate_all().unwrap();
        bulk_eng.evaluate_all().unwrap();
        assert_engines_equivalent(
            &ref_eng,
            &bulk_eng,
            max_row,
            max_col,
            &format!("chunk_rows={chunk_rows}/step2-light-overlay-cells"),
        );

        // Step 3 (Partial rebuild across multiple chunks via set_region_values, ragged):
        let multi_start = (chunk_rows as u32) - 10;
        let multi_len = chunk_rows + 20; // spans chunks 0 -> 1 (and into chunk 2 for chunk_rows=64)
        let ragged_rows: Vec<Vec<LiteralValue>> = (0..multi_len)
            .map(|i| {
                if i % 3 == 0 {
                    vec![LiteralValue::Number((i * 2) as f64)]
                } else {
                    vec![
                        LiteralValue::Number((i * 2) as f64),
                        LiteralValue::Date(
                            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()
                                + chrono::Duration::days((i % 20) as i64),
                        ),
                        LiteralValue::Empty,
                        LiteralValue::Number(5.0),
                        LiteralValue::Text("row_0".to_string()),
                    ]
                }
            })
            .collect();
        bulk_eng
            .set_region_values("Data", multi_start, 1, &ragged_rows)
            .unwrap();
        for (ri, rvals) in ragged_rows.iter().enumerate() {
            let r = multi_start + ri as u32;
            for (ci, v) in rvals.iter().enumerate() {
                let c = (ci as u32) + 1;
                ref_eng.set_cell_value("Data", r, c, v.clone()).unwrap();
                ref_eng.clear_staged_formula_text("Data", r, c);
            }
        }
        ref_eng.evaluate_all().unwrap();
        bulk_eng.evaluate_all().unwrap();
        assert_engines_equivalent(
            &ref_eng,
            &bulk_eng,
            max_row,
            max_col,
            &format!("chunk_rows={chunk_rows}/step3-multi-chunk-partial-rebuild"),
        );

        // Step 4 (Exact chunk-aligned full-chunk replace via set_region_columns on chunk 1):
        let ch1_start_1based = (chunk_rows as u32) + 1;
        let full_nums: Vec<f64> = (0..chunk_rows).map(|i| 1000.0 + i as f64).collect();
        let full_dates: Vec<f64> = (0..chunk_rows)
            .map(|i| 45_500.0 + (i % 15) as f64)
            .collect();
        bulk_eng
            .set_region_columns(
                "Data",
                ch1_start_1based,
                1,
                &[
                    BulkColumn::Float64 {
                        values: &full_nums,
                        validity: None,
                    },
                    BulkColumn::Serial {
                        values: &full_dates,
                        format: FormatId::DATE,
                        validity: None,
                    },
                    BulkColumn::Null { len: chunk_rows },
                ],
            )
            .unwrap();
        for i in 0..chunk_rows {
            let r = ch1_start_1based + i as u32;
            ref_eng
                .set_cell_value("Data", r, 1, LiteralValue::Number(full_nums[i]))
                .unwrap();
            let d =
                formualizer_common::try_serial_to_date_for(DateSystem::Excel1900, full_dates[i])
                    .unwrap();
            ref_eng
                .set_cell_value("Data", r, 2, LiteralValue::Date(d))
                .unwrap();
            ref_eng
                .set_cell_value("Data", r, 3, LiteralValue::Empty)
                .unwrap();
        }
        ref_eng.evaluate_all().unwrap();
        bulk_eng.evaluate_all().unwrap();
        assert_engines_equivalent(
            &ref_eng,
            &bulk_eng,
            max_row,
            max_col,
            &format!("chunk_rows={chunk_rows}/step4-full-chunk-replace"),
        );
    }
}
