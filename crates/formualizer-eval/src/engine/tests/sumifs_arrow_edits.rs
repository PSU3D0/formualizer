use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, DefaultFunctionContext, FunctionProvider};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

fn range_ref(sheet: &str, sr: u32, sc: u32, er: u32, ec: u32) -> ASTNode {
    let r = ReferenceType::Range {
        sheet: Some(sheet.to_string()),
        start_row: Some(sr),
        start_col: Some(sc),
        end_row: Some(er),
        end_col: Some(ec),
        start_row_abs: false,
        start_col_abs: false,
        end_row_abs: false,
        end_col_abs: false,
    };
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

#[test]
fn sumifs_arrow_edits_start_mid_end() {
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let sheet = "SheetEdits";
    let mut ab = engine.begin_bulk_ingest_arrow();
    // Create enough rows to likely force multiple chunks if chunk size is small,
    // or at least enough to have distinct start/mid/end.
    // Assuming default chunk size might be large (256?), let's do 100 rows.
    ab.add_sheet(sheet, 3, 1024);

    // Cols:
    // 0: Sum Range (Value)
    // 1: Criteria 1 (Group "A" or "B")
    // 2: Criteria 2 (Numeric > 10)

    let rows = 100;
    for i in 0..rows {
        let val = LiteralValue::Int((i + 1) as i64);
        let group = if i % 2 == 0 { "A" } else { "B" };
        let num = LiteralValue::Int(if i % 3 == 0 { 20 } else { 5 }); // 20 > 10, 5 < 10
        ab.append_row(sheet, &[val, LiteralValue::Text(group.into()), num])
            .unwrap();
    }
    ab.finish().unwrap();

    // SUMIFS(A:A, B:B, "A", C:C, ">10")
    // Matches: i even (A) AND i%3 == 0 (20 > 10).
    // Indices: 0, 6, 12, ...
    // i values: 0, 6, 12, ... 96.
    // Sum values: 1, 7, 13, ... 97.
    // Count: 0..100, step 6.
    // 0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96
    // (17 items)
    // Sum = sum(i+1 for i in sequence)
    // 1+7+13+...+97
    // Arithmetic series: n=17, a1=1, an=97. Sum = 17 * (1+97)/2 = 17 * 49 = 833.

    let sum_rng = range_ref(sheet, 1, 1, rows as u32, 1);
    let c1_rng = range_ref(sheet, 1, 2, rows as u32, 2);
    let c1 = lit_text("A");
    let c2_rng = range_ref(sheet, 1, 3, rows as u32, 3);
    let c2 = lit_text(">10");

    let eval = |eng: &Engine<TestWorkbook>| -> LiteralValue {
        let fun = eng.get_function("", "SUMIFS").expect("SUMIFS available");
        let interp = crate::interpreter::Interpreter::new(eng, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2, &interp),
        ];
        let fctx = DefaultFunctionContext::new_with_sheet(eng, None, eng.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap().into_literal()
    };

    // Initial check
    let res = eval(&engine);
    println!("Initial: {:?}", res);
    assert_eq!(res, LiteralValue::Number(833.0), "Initial Sum failed");

    // 1. Edit Start (Row 1, Index 0)
    engine
        .set_cell_value(sheet, 1, 2, LiteralValue::Text("B".into()))
        .unwrap();
    let res = eval(&engine);
    println!("After Start Edit: {:?}", res);
    assert_eq!(res, LiteralValue::Number(832.0), "Edit Start failed");

    // 2. Edit Middle (Row 50, Index 49)
    engine
        .set_cell_value(sheet, 50, 2, LiteralValue::Text("A".into()))
        .unwrap();
    engine
        .set_cell_value(sheet, 50, 3, LiteralValue::Int(30))
        .unwrap();
    let res = eval(&engine);
    println!("After Mid Edit: {:?}", res);
    assert_eq!(res, LiteralValue::Number(882.0), "Edit Middle failed");

    // 3. Edit End (Row 100, Index 99)
    engine
        .set_cell_value(sheet, 100, 2, LiteralValue::Text("A".into()))
        .unwrap();

    // Verify reads
    let v1 = engine.get_cell_value(sheet, 1, 2).unwrap();
    println!("Read Row 1 Col 2: {:?}", v1); // Expect "B"
    let v50 = engine.get_cell_value(sheet, 50, 2).unwrap();
    println!("Read Row 50 Col 2: {:?}", v50); // Expect "A"
    let v100 = engine.get_cell_value(sheet, 100, 2).unwrap();
    println!("Read Row 100 Col 2: {:?}", v100); // Expect "A"

    let res = eval(&engine);
    println!("After End Edit: {:?}", res);
    assert_eq!(res, LiteralValue::Number(982.0), "Edit End failed");
}
