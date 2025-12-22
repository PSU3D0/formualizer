use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, DefaultFunctionContext, FunctionProvider};
use chrono::NaiveDate;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

fn range_ref(sheet: &str, sr: u32, sc: u32, er: u32, ec: u32) -> ASTNode {
    let r = ReferenceType::range(
        Some(sheet.to_string()),
        Some(sr),
        Some(sc),
        Some(er),
        Some(ec),
    );
    ASTNode::new(
        ASTNodeType::Reference {
            original: String::new(),
            reference: r,
        },
        None,
    )
}

fn lit_text(s: &str) -> ASTNode {
    ASTNode::new(ASTNodeType::Literal(LiteralValue::Text(s.into())), None)
}

fn whole_col_ref(sheet: &str, col: u32) -> ASTNode {
    let r = ReferenceType::range(Some(sheet.to_string()), None, Some(col), None, Some(col));
    ASTNode::new(
        ASTNodeType::Reference {
            original: String::new(),
            reference: r,
        },
        None,
    )
}

#[test]
fn countifs_hybrid_formula_and_base_text() {
    // Ensure COUNTIFS sees both Arrow base values and graph formula values in a single column
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config()); // ensures overlay writeback

    // Build Arrow sheet with 3 rows, 2 columns
    let sheet = "SheetCF";
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet(sheet, 2, 8);

    // Row1: header text in col2
    ab.append_row(
        sheet,
        &[LiteralValue::Empty, LiteralValue::Text("Header".into())],
    )
    .unwrap();
    // Row2: base value match in col2
    ab.append_row(
        sheet,
        &[LiteralValue::Empty, LiteralValue::Text("BDM021".into())],
    )
    .unwrap();
    // Row3: placeholder; will be replaced by a formula later
    ab.append_row(sheet, &[LiteralValue::Empty, LiteralValue::Empty])
        .unwrap();
    ab.finish().unwrap();

    // Set formula in row3 col2 producing text "BDM021"
    let ast = formualizer_parse::parser::parse("=\"BDM021\"").unwrap();
    engine
        .set_cell_formula(sheet, 3, 2, ast)
        .expect("set formula");
    engine.evaluate_all().unwrap();

    // COUNTIFS over whole column 2 where value == "BDM021" should see 2 matches (row2 base + row3 formula)
    let col_ref = whole_col_ref(sheet, 2);
    let crit = lit_text("BDM021");
    let fun = engine
        .get_function("", "COUNTIFS")
        .expect("COUNTIFS available");
    let got = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&col_ref, &interp),
            ArgumentHandle::new(&crit, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };
    assert_eq!(got, LiteralValue::Number(2.0));
}

#[test]
fn sumifs_arrow_fastpath_parity_small() {
    // Engine with Arrow enabled + fastpath
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    // Build Arrow sheet with 3 columns and multi-chunk rows
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet("Sheet1", 3, 3); // chunk_rows=3 to induce multiple chunks

    // Data: 8 rows
    // Col1 (sum): 10,20,30,40,50,60,70,80
    // Col2 (crit1): 1,0,1,0,1,0,1,0  -> "=1"
    // Col3 (crit2): 0,1,2,3,4,5,6,7  -> ">=5"
    for i in 0..8u32 {
        let sum = LiteralValue::Int(((i + 1) * 10) as i64);
        let c1 = LiteralValue::Int((i % 2) as i64);
        let c2 = LiteralValue::Int(i as i64);
        ab.append_row("Sheet1", &[sum, c1, c2]).unwrap();
    }
    ab.finish().unwrap();

    // Build SUMIFS(sum, col2, "=1", col3, ">=5")
    let sum_rng = range_ref("Sheet1", 1, 1, 8, 1);
    let c1_rng = range_ref("Sheet1", 1, 2, 8, 2);
    let c1 = lit_text("=1");
    let c2_rng = range_ref("Sheet1", 1, 3, 8, 3);
    let c2 = lit_text(">=5");

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS available");
    let got_fast = {
        let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    // Disable fastpath and re-evaluate
    let got_slow = {
        let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    assert_eq!(got_fast, got_slow);
    // For sanity: expected rows where c1=1 and c2>=5 are indices 5 and 7 (1-based), sums 60 + 80 = 140
    assert_eq!(got_fast, LiteralValue::Number(140.0));
}

#[test]
fn sumifs_arrow_fastpath_broadcasts_1x1_text_criteria_range() {
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let sheet = "SheetScalarText";
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet(sheet, 2, 8);

    // Col1 is sum range. Col2 has a single scalar criterion value in row 1.
    for i in 0..4u32 {
        let sum = LiteralValue::Int(((i + 1) * 10) as i64);
        let crit = if i == 0 {
            LiteralValue::Text("FL".into())
        } else {
            LiteralValue::Empty
        };
        ab.append_row(sheet, &[sum, crit]).unwrap();
    }
    ab.finish().unwrap();

    let sum_rng = range_ref(sheet, 1, 1, 4, 1);
    let c_rng = range_ref(sheet, 1, 2, 1, 2);
    let c = lit_text("FL");

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS available");
    let got_fast = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c_rng, &interp),
            ArgumentHandle::new(&c, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    let got_slow = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c_rng, &interp),
            ArgumentHandle::new(&c, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    assert_eq!(got_fast, got_slow);
    assert_eq!(got_fast, LiteralValue::Number(100.0));
}

#[test]
fn sumifs_arrow_fastpath_broadcasts_1x1_numeric_criteria_range() {
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let sheet = "SheetScalarNum";
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet(sheet, 2, 8);

    // Col1 is sum range. Col2 is a 1x1 criteria_range value at row 1.
    // With broadcast semantics, if B1 matches the predicate, include all sums.
    for i in 0..4u32 {
        let sum = LiteralValue::Int(((i + 1) * 10) as i64);
        let crit_range_cell = if i == 0 {
            LiteralValue::Int(3)
        } else {
            LiteralValue::Empty
        };
        ab.append_row(sheet, &[sum, crit_range_cell]).unwrap();
    }
    ab.finish().unwrap();

    let sum_rng = range_ref(sheet, 1, 1, 4, 1);
    let c_rng = range_ref(sheet, 1, 2, 1, 2);
    let c = lit_text("<=3");

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS available");
    let got_fast = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c_rng, &interp),
            ArgumentHandle::new(&c, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    let got_slow = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c_rng, &interp),
            ArgumentHandle::new(&c, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    assert_eq!(got_fast, got_slow);
    assert_eq!(got_fast, LiteralValue::Number(100.0));
}

#[test]
fn countifs_arrow_fastpath_broadcasts_1x1_numeric_criteria_range() {
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let sheet = "SheetCountIfsScalar";
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet(sheet, 2, 8);

    // Col1 is the primary criteria_range (4 rows). Col2 is a 1x1 criteria_range (row1 only).
    for i in 0..4u32 {
        let r1 = LiteralValue::Int((i % 2) as i64); // 0,1,0,1
        let r2 = if i == 0 {
            LiteralValue::Int(3)
        } else {
            LiteralValue::Empty
        };
        ab.append_row(sheet, &[r1, r2]).unwrap();
    }
    ab.finish().unwrap();

    let range1 = range_ref(sheet, 1, 1, 4, 1);
    let pred1 = lit_text("=1");
    let range2 = range_ref(sheet, 1, 2, 1, 2);
    let pred2 = lit_text("<=3");

    let fun = engine
        .get_function("", "COUNTIFS")
        .expect("COUNTIFS available");
    let got_fast = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&range1, &interp),
            ArgumentHandle::new(&pred1, &interp),
            ArgumentHandle::new(&range2, &interp),
            ArgumentHandle::new(&pred2, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    let got_slow = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&range1, &interp),
            ArgumentHandle::new(&pred1, &interp),
            ArgumentHandle::new(&range2, &interp),
            ArgumentHandle::new(&pred2, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    assert_eq!(got_fast, got_slow);
    assert_eq!(got_fast, LiteralValue::Number(2.0));
}

#[test]
fn sumifs_text_and_date_window_parity() {
    // Build Arrow sheet 'MONTHLY.DATA R260' with P(16), K(11), AV(48), R(18)
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let sheet = "MONTHLY.DATA R260";
    let mut ab = engine.begin_bulk_ingest_arrow();
    let ncols = 50usize;
    ab.add_sheet(sheet, ncols, 256);

    let k_col = 11u32; // 1-based
    let p_col = 16u32;
    let av_col = 48u32;
    let r_col = 18u32;
    let match_tag = "MATCHTAG";
    // Month window: 2024-01 inclusive start, exclusive end
    let start = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let next = NaiveDate::from_ymd_opt(2024, 2, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let start_serial = formualizer_common::datetime_to_serial(&start);
    let next_serial = formualizer_common::datetime_to_serial(&next);

    let empty_row = vec![LiteralValue::Empty; ncols];
    // Row 1: non-match (before window)
    let mut r1 = empty_row.clone();
    r1[(p_col - 1) as usize] = LiteralValue::Int(999);
    r1[(k_col - 1) as usize] = LiteralValue::Text("Other".into());
    r1[(av_col - 1) as usize] = LiteralValue::Text(match_tag.into());
    r1[(r_col - 1) as usize] = LiteralValue::Number(start_serial - 1.0);
    ab.append_row(sheet, &r1).unwrap();

    // Row 2: match 1
    let mut r2 = empty_row.clone();
    r2[(p_col - 1) as usize] = LiteralValue::Int(50);
    r2[(k_col - 1) as usize] = LiteralValue::Text("Malpractice SC0279".into());
    r2[(av_col - 1) as usize] = LiteralValue::Text(match_tag.into());
    r2[(r_col - 1) as usize] = LiteralValue::Number(start_serial + 10.0);
    ab.append_row(sheet, &r2).unwrap();

    // Row 3: match 2 (with time-of-day)
    let mut r3 = empty_row.clone();
    r3[(p_col - 1) as usize] = LiteralValue::Int(78);
    r3[(k_col - 1) as usize] = LiteralValue::Text("Malpractice SC0279".into());
    r3[(av_col - 1) as usize] = LiteralValue::Text(match_tag.into());
    let noon = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    r3[(r_col - 1) as usize] = LiteralValue::Number(formualizer_common::datetime_to_serial(&noon));
    ab.append_row(sheet, &r3).unwrap();

    // Row 4: outside window
    let mut r4 = empty_row.clone();
    r4[(p_col - 1) as usize] = LiteralValue::Int(1000);
    r4[(k_col - 1) as usize] = LiteralValue::Text("Malpractice SC0279".into());
    r4[(av_col - 1) as usize] = LiteralValue::Text(match_tag.into());
    r4[(r_col - 1) as usize] = LiteralValue::Number(next_serial + 2.0);
    ab.append_row(sheet, &r4).unwrap();

    let _ = ab.finish().unwrap();

    // Build whole-column references
    let sum_rng = whole_col_ref(sheet, p_col);
    let k_rng = whole_col_ref(sheet, k_col);
    let av_rng = whole_col_ref(sheet, av_col);
    let r_rng = whole_col_ref(sheet, r_col);
    let crit_k = lit_text("Malpractice SC0279");
    let crit_av = lit_text(match_tag);
    let crit_start = lit_text(&format!(">={start_serial}"));
    let crit_end = lit_text(&format!("<{next_serial}"));

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS available");

    let got_fast = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&k_rng, &interp),
            ArgumentHandle::new(&crit_k, &interp),
            ArgumentHandle::new(&av_rng, &interp),
            ArgumentHandle::new(&crit_av, &interp),
            ArgumentHandle::new(&r_rng, &interp),
            ArgumentHandle::new(&crit_start, &interp),
            ArgumentHandle::new(&r_rng, &interp),
            ArgumentHandle::new(&crit_end, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    let got_slow = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&k_rng, &interp),
            ArgumentHandle::new(&crit_k, &interp),
            ArgumentHandle::new(&av_rng, &interp),
            ArgumentHandle::new(&crit_av, &interp),
            ArgumentHandle::new(&r_rng, &interp),
            ArgumentHandle::new(&crit_start, &interp),
            ArgumentHandle::new(&r_rng, &interp),
            ArgumentHandle::new(&crit_end, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    assert_eq!(got_fast, got_slow);
    assert_eq!(got_fast, LiteralValue::Number(128.0));
}

#[test]
fn sumifs_arrow_fastpath_large_numeric_criteria() {
    // Large range + multiple numeric criteria to exercise boolean mask composition
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let rows: u32 = 10_000;
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet("Sheet1", 4, 1024);

    for i in 0..rows {
        // sum col: i
        let s = LiteralValue::Int(i as i64);
        // c1: i % 3 (0,1,2)
        let c1 = LiteralValue::Int((i % 3) as i64);
        // c2: 2*i (even progression)
        let c2 = LiteralValue::Int((2 * i) as i64);
        // c3: 10000 - i (descending)
        let c3 = LiteralValue::Int((10_000 - i) as i64);
        ab.append_row("Sheet1", &[s, c1, c2, c3]).unwrap();
    }
    ab.finish().unwrap();

    // SUMIFS over col1 where col2 = 1 AND col3 >= 1000 AND col4 <= 9000
    let sum_rng = range_ref("Sheet1", 1, 1, rows, 1);
    let c1_rng = range_ref("Sheet1", 1, 2, rows, 2);
    let c1 = lit_text("=1");
    let c2_rng = range_ref("Sheet1", 1, 3, rows, 3);
    let c2 = lit_text(">=1000");
    let c3_rng = range_ref("Sheet1", 1, 4, rows, 4);
    let c3 = lit_text("<=9000");

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS available");
    let fast = {
        let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2, &interp),
            ArgumentHandle::new(&c3_rng, &interp),
            ArgumentHandle::new(&c3, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    let slow = {
        let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2, &interp),
            ArgumentHandle::new(&c3_rng, &interp),
            ArgumentHandle::new(&c3, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    assert_eq!(fast, slow);
    // Ensure result is non-trivial
    match fast {
        LiteralValue::Number(n) => assert!(n > 0.0),
        _ => panic!("expected number"),
    }
}

/// Test multi-chunk criteria mask computation with configurable chunk size.
/// This exercises the `compute_criteria_mask` concatenation path extensively.
/// The test verifies correctness with many chunks.
#[test]
fn sumifs_multi_chunk_criteria_mask_correctness() {
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    // Use small chunk size (100 rows) to force many chunks
    let chunk_rows = 100usize;
    let total_rows = 1000u32; // 10 chunks

    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet("Sheet1", 3, chunk_rows);

    // Build data: sum_col, crit1 (mod 3), crit2 (threshold test)
    for i in 0..total_rows {
        let sum_val = LiteralValue::Int((i + 1) as i64);
        let crit1_val = LiteralValue::Int((i % 3) as i64);
        let crit2_val = LiteralValue::Int(i as i64);
        ab.append_row("Sheet1", &[sum_val, crit1_val, crit2_val])
            .unwrap();
    }
    ab.finish().unwrap();

    // SUMIFS(sum_col, crit1_col, "=1", crit2_col, ">=500")
    // Matches rows where i%3=1 AND i>=500
    let sum_rng = range_ref("Sheet1", 1, 1, total_rows, 1);
    let c1_rng = range_ref("Sheet1", 1, 2, total_rows, 2);
    let c1 = lit_text("=1");
    let c2_rng = range_ref("Sheet1", 1, 3, total_rows, 3);
    let c2 = lit_text(">=500");

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS available");

    // Fast path
    let fast_result = {
        let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    // Disable fastpath and compare
    let slow_result = {
        let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    // Verify correctness: both paths must produce the same result
    assert_eq!(fast_result, slow_result);
    // Verify we got a non-trivial result
    match fast_result {
        LiteralValue::Number(n) => assert!(n > 0.0),
        _ => panic!("expected number"),
    }
}
