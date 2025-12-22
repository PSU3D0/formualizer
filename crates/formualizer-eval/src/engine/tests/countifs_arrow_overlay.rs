use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::FunctionProvider;
use crate::traits::{ArgumentHandle, DefaultFunctionContext};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

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

fn lit_text(s: &str) -> ASTNode {
    ASTNode::new(ASTNodeType::Literal(LiteralValue::Text(s.into())), None)
}

#[test]
fn countifs_arrow_overlay_only_values() {
    // Ensures COUNTIFS sees overlay-injected values via Arrow-only read path
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg.clone());

    let sheet = "CFA";
    // Build 1 column of empty base values
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 4);
        for _ in 0..6 {
            ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }

    // Inject two overlay values via set_cell_value (no formulas)
    engine
        .set_cell_value(sheet, 2, 1, LiteralValue::Text("BDM021".into()))
        .unwrap();
    engine
        .set_cell_value(sheet, 5, 1, LiteralValue::Text("BDM021".into()))
        .unwrap();

    // Direct Arrow check
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let av = asheet.range_view(0, 0, 5, 0);
    assert_eq!(av.get_cell(1, 0), LiteralValue::Text("BDM021".into()));
    assert_eq!(av.get_cell(4, 0), LiteralValue::Text("BDM021".into()));

    // COUNTIFS over whole column for "BDM021" should be 2
    let col_ref = whole_col_ref(sheet, 1);
    let crit = lit_text("BDM021");
    let fun = engine.get_function("", "COUNTIFS").expect("COUNTIFS");
    let got = {
        let interp = crate::interpreter::Interpreter::new(&engine, sheet);
        let args = vec![
            ArgumentHandle::new(&col_ref, &interp),
            ArgumentHandle::new(&crit, &interp),
        ];
        let fctx =
            DefaultFunctionContext::new_with_sheet(&engine, None, engine.default_sheet_name());
        fun.dispatch(&args, &fctx).unwrap()
    };
    assert_eq!(got, LiteralValue::Number(2.0));
}
