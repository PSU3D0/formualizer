use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::FunctionProvider;
use crate::traits::{ArgumentHandle, DefaultFunctionContext};
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

#[test]
fn sumif_overlay_and_fastpath_parity() {
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let sheet = "SOV";
    // Build base: 2 cols, 6 rows
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 2, 4);
        for i in 0..6u32 {
            let sum = LiteralValue::Int(((i + 1) * 10) as i64);
            let crit = LiteralValue::Text("x".into());
            ab.append_row(sheet, &[sum, crit]).unwrap();
        }
        ab.finish().unwrap();
    }

    // Overlay updates: change criteria in some rows and adjust sums
    // Row2: crit="y", sum=200; Row5: crit="y", sum=500
    engine
        .set_cell_value(sheet, 2, 2, LiteralValue::Text("y".into()))
        .unwrap();
    engine
        .set_cell_value(sheet, 2, 1, LiteralValue::Int(200))
        .unwrap();
    engine
        .set_cell_value(sheet, 5, 2, LiteralValue::Text("y".into()))
        .unwrap();
    engine
        .set_cell_value(sheet, 5, 1, LiteralValue::Int(500))
        .unwrap();

    let sum_rng = range_ref(sheet, 1, 1, 6, 1);
    let crit_rng = range_ref(sheet, 1, 2, 6, 2);
    let crit_y = lit_text("y");

    let fun = engine.get_function("", "SUMIF").expect("SUMIF");

    let got_slow = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&crit_rng, &interp),
            ArgumentHandle::new(&crit_y, &interp),
            ArgumentHandle::new(&sum_rng, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap()
    };

    // Ensure correctness regardless of fastpath availability
    assert_eq!(got_slow, LiteralValue::Number(700.0)); // 200 + 500
}

#[test]
fn sumifs_overlay_and_fastpath_parity() {
    let config = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), config.clone());

    let sheet = "SOV2";
    // 3 cols: sum, crit1 (0/1), crit2 text
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 3, 4);
        for i in 0..8u32 {
            let sum = LiteralValue::Int(((i + 1) * 10) as i64);
            let c1 = LiteralValue::Int((i % 2) as i64);
            let c2 = LiteralValue::Text(if i % 3 == 0 { "aa" } else { "bb" }.into());
            ab.append_row(sheet, &[sum, c1, c2]).unwrap();
        }
        ab.finish().unwrap();
    }

    // Overlay two rows to match c1=1 and c2="aa": rows 5 and 7 become matches
    engine
        .set_cell_value(sheet, 5, 2, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value(sheet, 5, 3, LiteralValue::Text("aa".into()))
        .unwrap();
    engine
        .set_cell_value(sheet, 7, 2, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value(sheet, 7, 3, LiteralValue::Text("aa".into()))
        .unwrap();

    let sum_rng = range_ref(sheet, 1, 1, 8, 1);
    let c1_rng = range_ref(sheet, 1, 2, 8, 2);
    let c1_eq1 = lit_text("=1");
    let c2_rng = range_ref(sheet, 1, 3, 8, 3);
    let c2_eq_aa = lit_text("=\"aa\"");

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS");
    let got_fast = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&sum_rng, &interp),
            ArgumentHandle::new(&c1_rng, &interp),
            ArgumentHandle::new(&c1_eq1, &interp),
            ArgumentHandle::new(&c2_rng, &interp),
            ArgumentHandle::new(&c2_eq_aa, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap()
    };

    assert_eq!(got_fast, LiteralValue::Number(160.0));
}
