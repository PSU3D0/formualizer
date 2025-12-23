use formualizer_common::LiteralValue;
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::reference::{CellRef, Coord};
use formualizer_workbook::Workbook;

#[test]
fn named_range_formula_recalculates_and_marks_dirty() {
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet1").unwrap();

    let sheet_id = workbook.engine_mut().sheet_id_mut("Sheet1");

    workbook
        .engine_mut()
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .expect("set seed value");

    let input_ref = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
    workbook
        .engine_mut()
        .define_name(
            "InputValue",
            NamedDefinition::Cell(input_ref),
            NameScope::Workbook,
        )
        .expect("define named range");

    let formula_ast = formualizer_parse::parser::parse("=InputValue*2").expect("parse formula");
    workbook
        .engine_mut()
        .set_cell_formula("Sheet1", 1, 2, formula_ast)
        .expect("set formula");

    workbook.evaluate_all().expect("initial evaluation");

    let initial_output = workbook
        .engine()
        .get_cell_value("Sheet1", 1, 2)
        .expect("output cell present");
    assert!(
        matches!(initial_output, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9),
        "expected 20 from formula, got {initial_output:?}"
    );

    let name_entry = workbook
        .engine()
        .resolve_name_entry("InputValue", sheet_id)
        .expect("named range entry");
    let name_vertex = name_entry.vertex;
    let formula_vertex = workbook
        .engine()
        .vertex_for_cell(&CellRef::new(sheet_id, Coord::new(0, 1, true, true)))
        .expect("formula vertex");

    let named_value = workbook.engine().vertex_value(name_vertex);
    assert!(
        matches!(named_value, Some(LiteralValue::Number(n)) if (n - 10.0).abs() < 1e-9),
        "expected named range value 10, got {named_value:?}"
    );

    workbook
        .engine_mut()
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(25.0))
        .expect("mutate named input");

    let pending = workbook.engine().evaluation_vertices();
    assert!(
        pending.contains(&name_vertex),
        "named range vertex should be dirtied after dependency edit"
    );
    assert!(
        pending.contains(&formula_vertex),
        "dependent formula should be dirtied after dependency edit"
    );

    workbook.evaluate_all().expect("re-evaluation");

    let updated_name_value = workbook.engine().vertex_value(name_vertex);
    assert!(
        matches!(updated_name_value, Some(LiteralValue::Number(n)) if (n - 25.0).abs() < 1e-9),
        "expected named range to reflect updated value, got {updated_name_value:?}"
    );

    let updated_output = workbook
        .engine()
        .get_cell_value("Sheet1", 1, 2)
        .expect("output cell after mutation");
    assert!(
        matches!(updated_output, LiteralValue::Number(n) if (n - 50.0).abs() < 1e-9),
        "expected updated output 50, got {updated_output:?}"
    );
}
