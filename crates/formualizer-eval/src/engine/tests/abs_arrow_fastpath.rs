use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, DefaultFunctionContext, FunctionProvider};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

fn range_ref(sheet: &str, sr: u32, sc: u32, er: u32, ec: u32) -> ASTNode {
    ASTNode::new(
        ASTNodeType::Reference {
            original: String::new(),
            reference: ReferenceType::Range {
                sheet: Some(sheet.to_string()),
                start_row: Some(sr),
                start_col: Some(sc),
                end_row: Some(er),
                end_col: Some(ec),
            },
        },
        None,
    )
}

#[test]
fn abs_arrow_array_result() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let mut builder = engine.begin_bulk_ingest_arrow();
    builder.add_sheet("SheetAbs", 2, 2);
    builder
        .append_row("SheetAbs", &[LiteralValue::Int(-5), LiteralValue::Int(2)])
        .unwrap();
    builder
        .append_row(
            "SheetAbs",
            &[LiteralValue::Number(-3.5), LiteralValue::Int(4)],
        )
        .unwrap();
    builder.finish().unwrap();

    let func = engine.get_function("", "ABS").expect("abs registered");
    let interp = crate::interpreter::Interpreter::new(&engine, "SheetAbs");
    let range = range_ref("SheetAbs", 1, 1, 2, 2);
    let args = vec![ArgumentHandle::new(&range, &interp)];
    let ctx = DefaultFunctionContext::new(&engine, None);
    let result = func.dispatch(&args, &ctx).expect("abs result");

    assert_eq!(
        result,
        LiteralValue::Array(vec![
            vec![LiteralValue::Number(5.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.5), LiteralValue::Number(4.0)],
        ])
    );
}
