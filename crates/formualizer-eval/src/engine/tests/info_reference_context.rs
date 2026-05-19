use super::common::abs_cell_ref;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::RangeRef;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ReferenceType, parse};

fn err_kind(value: Option<LiteralValue>) -> Option<ExcelErrorKind> {
    match value {
        Some(LiteralValue::Error(e)) => Some(e.kind),
        _ => None,
    }
}

fn num(value: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(value))
}

#[test]
fn context_sheet_metadata_tracks_add_rename_remove_without_name_cloning_api() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    let scratch = engine.add_sheet("Scratch").unwrap();
    let summary = engine.add_sheet("Summary").unwrap();

    assert_eq!(engine.workbook_sheet_count(), Some(4));
    assert_eq!(engine.sheet_index_by_name("Sheet1"), Some(1));
    assert_eq!(engine.sheet_index_by_name("Data"), Some(2));
    assert_eq!(engine.sheet_index_by_name("Scratch"), Some(3));
    assert_eq!(engine.sheet_index_by_name("Summary"), Some(4));
    assert_eq!(engine.current_sheet_index("Summary"), Some(4));

    engine.rename_sheet(data, "Inputs").unwrap();
    assert_eq!(engine.sheet_index_by_name("Data"), None);
    assert_eq!(engine.sheet_index_by_name("Inputs"), Some(2));

    engine.remove_sheet(scratch).unwrap();
    assert_eq!(engine.workbook_sheet_count(), Some(3));
    assert_eq!(engine.sheet_index_by_name("Summary"), Some(3));
    assert_eq!(engine.sheet_name(summary), "Summary");
}

#[test]
fn inspect_reference_covers_cells_ranges_names_tables_and_3d_without_materializing_values() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    engine.add_sheet("More").unwrap();

    let cell = ReferenceType::cell(Some("Data".to_string()), 2, 3);
    let info = engine.inspect_reference(&cell, "Sheet1").unwrap().unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.sheet_count, Some(1));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 2, 3)));

    let open_col = ReferenceType::range(Some("Data".to_string()), None, Some(2), None, Some(2));
    let info = engine
        .inspect_reference(&open_col, "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 1, 2)));

    engine
        .define_name(
            "InputCell",
            NamedDefinition::Cell(abs_cell_ref(data, 4, 5)),
            NameScope::Workbook,
        )
        .unwrap();
    let info = engine
        .inspect_reference(&ReferenceType::NamedRange("InputCell".into()), "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 4, 5)));

    engine
        .define_name(
            "ConstantName",
            NamedDefinition::Literal(LiteralValue::Int(7)),
            NameScope::Workbook,
        )
        .unwrap();
    let info = engine
        .inspect_reference(&ReferenceType::NamedRange("ConstantName".into()), "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_cell, None);
    assert_eq!(info.sheet_count, None);

    engine
        .define_table(
            "SalesTable",
            RangeRef::new(abs_cell_ref(data, 10, 2), abs_cell_ref(data, 12, 4)),
            true,
            vec!["A".into(), "B".into(), "C".into()],
            false,
        )
        .unwrap();
    let info = engine
        .inspect_reference(
            &ReferenceType::Table(formualizer_parse::parser::TableReference {
                name: "SalesTable".into(),
                specifier: None,
            }),
            "Sheet1",
        )
        .unwrap()
        .unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 10, 2)));

    let three_d = ReferenceType::Range3D {
        sheet_first: "Data".into(),
        sheet_last: "More".into(),
        start_row: Some(2),
        start_col: Some(1),
        end_row: Some(4),
        end_col: Some(3),
        start_row_abs: false,
        start_col_abs: false,
        end_row_abs: false,
        end_col_abs: false,
    };
    let info = engine
        .inspect_reference(&three_d, "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.sheet_count, Some(2));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 2, 1)));

    assert!(
        engine
            .inspect_reference(&ReferenceType::NamedRange("MissingName".into()), "Sheet1")
            .is_err()
    );
}

#[test]
fn formula_text_context_uses_staged_text_then_canonical_ast_and_tracks_edits() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    let a1 = abs_cell_ref(data, 1, 1);
    let a2 = abs_cell_ref(data, 2, 1);
    let a3 = abs_cell_ref(data, 3, 1);

    engine
        .set_cell_formula("Data", 1, 1, parse("=1+2").unwrap())
        .unwrap();
    assert_eq!(
        engine.formula_text_at_cell(a1).unwrap(),
        Some("=1 + 2".into())
    );

    engine
        .set_cell_formula("Data", 1, 1, parse("=SUM(1,2,3)").unwrap())
        .unwrap();
    assert_eq!(
        engine.formula_text_at_cell(a1).unwrap(),
        Some("=SUM(1, 2, 3)".into())
    );

    engine
        .set_cell_value("Data", 2, 1, LiteralValue::Text("not formula".into()))
        .unwrap();
    assert_eq!(engine.formula_text_at_cell(a2).unwrap(), None);

    engine.stage_formula_text("Data", 3, 1, "SUM(A1:A2)".into());
    assert_eq!(
        engine.formula_text_at_cell(a3).unwrap(),
        Some("=SUM(A1:A2)".into())
    );

    engine.stage_formula_text("Data", 3, 1, "=$A$1".into());
    assert_eq!(
        engine.formula_text_at_cell(a3).unwrap(),
        Some("=$A$1".into())
    );
}

#[test]
fn formula_text_sheet_sheets_and_isref_functions_evaluate_directly() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    engine.add_sheet("More").unwrap();

    engine
        .define_name(
            "InputCell",
            NamedDefinition::Cell(abs_cell_ref(data, 1, 1)),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .define_name(
            "ConstantName",
            NamedDefinition::Literal(LiteralValue::Int(7)),
            NameScope::Workbook,
        )
        .unwrap();

    engine
        .set_cell_formula("Data", 1, 1, parse("=1+2").unwrap())
        .unwrap();
    engine
        .set_cell_value("Data", 2, 1, LiteralValue::Int(42))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=FORMULATEXT(Data!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=FORMULATEXT(Data!A2)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Data", 3, 1, parse("=SHEET()").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=SHEET(Data!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=SHEET(\"More\")").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 3, parse("=SHEETS()").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 4, parse("=SHEETS(Data:More!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=ISREF(Data!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 2, parse("=ISREF(1+1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 3, parse("=ISREF(MissingName)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 4, parse("=ISREF(InputCell)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 5, parse("=ISREF(ConstantName)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("=1 + 2".into()))
    );
    assert_eq!(
        err_kind(engine.get_cell_value("Sheet1", 1, 2)),
        Some(ExcelErrorKind::Na)
    );
    assert_eq!(engine.get_cell_value("Data", 3, 1), num(2.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 1), num(2.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), num(3.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 3), num(3.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 4), num(2.0));
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Boolean(false))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Boolean(false))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 4),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 5),
        Some(LiteralValue::Boolean(false))
    );

    engine
        .set_cell_formula("Data", 1, 1, parse("=3+4").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("=3 + 4".into()))
    );
}
