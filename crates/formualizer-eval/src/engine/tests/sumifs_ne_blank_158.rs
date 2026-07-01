use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, DefaultFunctionContext, FunctionProvider};
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

fn engine_with_debt_blank_equity() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet("Sheet1", 2, 8);
    ab.append_row(
        "Sheet1",
        &[LiteralValue::Text("Debt".into()), LiteralValue::Int(10)],
    )
    .unwrap();
    ab.append_row("Sheet1", &[LiteralValue::Empty, LiteralValue::Int(20)])
        .unwrap();
    ab.append_row(
        "Sheet1",
        &[LiteralValue::Text("Equity".into()), LiteralValue::Int(30)],
    )
    .unwrap();
    ab.finish().unwrap();
    engine
}

#[test]
fn sumifs_ne_text_includes_blank_criteria_cell() {
    let engine = engine_with_debt_blank_equity();
    let sum_rng = range_ref("Sheet1", 1, 2, 3, 2);
    let crit_rng = range_ref("Sheet1", 1, 1, 3, 1);
    let crit = lit_text("<>Debt");

    let fun = engine.get_function("", "SUMIFS").expect("SUMIFS available");
    let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
    let args = vec![
        ArgumentHandle::new(&sum_rng, &interp),
        ArgumentHandle::new(&crit_rng, &interp),
        ArgumentHandle::new(&crit, &interp),
    ];
    let fctx = DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
    let got = fun.dispatch(&args, &fctx).unwrap().into_literal();

    assert_eq!(got, LiteralValue::Number(50.0));
}

#[test]
fn countifs_ne_text_includes_blank_criteria_cell() {
    let engine = engine_with_debt_blank_equity();
    let crit_rng = range_ref("Sheet1", 1, 1, 3, 1);
    let crit = lit_text("<>Debt");

    let fun = engine
        .get_function("", "COUNTIFS")
        .expect("COUNTIFS available");
    let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
    let args = vec![
        ArgumentHandle::new(&crit_rng, &interp),
        ArgumentHandle::new(&crit, &interp),
    ];
    let fctx = DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
    let got = fun.dispatch(&args, &fctx).unwrap().into_literal();

    assert_eq!(got, LiteralValue::Number(2.0));
}

#[test]
fn sumif_ne_text_includes_blank_criteria_cell() {
    let engine = engine_with_debt_blank_equity();
    let crit_rng = range_ref("Sheet1", 1, 1, 3, 1);
    let crit = lit_text("<>Debt");
    let sum_rng = range_ref("Sheet1", 1, 2, 3, 2);

    let fun = engine.get_function("", "SUMIF").expect("SUMIF available");
    let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
    let args = vec![
        ArgumentHandle::new(&crit_rng, &interp),
        ArgumentHandle::new(&crit, &interp),
        ArgumentHandle::new(&sum_rng, &interp),
    ];
    let fctx = DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
    let got = fun.dispatch(&args, &fctx).unwrap().into_literal();

    assert_eq!(got, LiteralValue::Number(50.0));
}
