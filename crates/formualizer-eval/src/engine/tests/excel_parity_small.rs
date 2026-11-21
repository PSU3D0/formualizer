use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn number(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, value: f64) {
    engine
        .set_cell_value("Sheet1", row, col, LiteralValue::Number(value))
        .unwrap();
}

fn formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, text: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(text).unwrap())
        .unwrap();
}

fn new_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();
    engine
}

#[test]
fn ceiling_and_floor_math_take_a_mode() {
    let mut engine = new_engine();
    formula(&mut engine, 1, 1, "=CEILING.MATH(-2.5,1,1)");
    formula(&mut engine, 2, 1, "=CEILING.MATH(-2.5,1,0)");
    formula(&mut engine, 3, 1, "=FLOOR.MATH(-2.5,1,1)");
    formula(&mut engine, 4, 1, "=FLOOR.MATH(-2.5,1,0)");
    engine.evaluate_all().unwrap();
    let value = |row| engine.get_cell_value("Sheet1", row, 1);
    assert_eq!(value(1), Some(LiteralValue::Number(-3.0)));
    assert_eq!(value(2), Some(LiteralValue::Number(-2.0)));
    assert_eq!(value(3), Some(LiteralValue::Number(-2.0)));
    assert_eq!(value(4), Some(LiteralValue::Number(-3.0)));
}

#[test]
fn a_named_range_shrinks_when_part_of_it_is_deleted() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut engine = new_engine();
    for row in 1..=6 {
        number(&mut engine, row, 1, f64::from(row));
    }
    let sheet = engine.sheet_id("Sheet1").unwrap();
    let start = CellRef::new(sheet, Coord::from_excel(2, 1, true, true));
    let end = CellRef::new(sheet, Coord::from_excel(4, 1, true, true));
    engine
        .define_name(
            "Block",
            NamedDefinition::Range(RangeRef::new(start, end)),
            NameScope::Workbook,
        )
        .unwrap();
    formula(&mut engine, 1, 3, "=SUM(Block)");
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(9.0))
    );

    // Deleting the range's last row leaves rows 2 and 3.
    engine.delete_rows("Sheet1", 4, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(5.0))
    );

    // Deleting all of what's left gives #REF!.
    engine.delete_rows("Sheet1", 2, 2).unwrap();
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Error(_))
    ));
}

#[test]
fn rows_and_columns_pass_on_a_reference_error() {
    let mut engine = new_engine();
    formula(&mut engine, 1, 1, "=ROWS(#REF!)");
    formula(&mut engine, 2, 1, "=COLUMNS(#REF!)");
    engine.evaluate_all().unwrap();
    for row in 1..=2 {
        assert!(
            matches!(
                engine.get_cell_value("Sheet1", row, 1),
                Some(LiteralValue::Error(_))
            ),
            "row {row}"
        );
    }
}

#[test]
fn vlookup_looks_before_it_checks_the_column() {
    let mut engine = new_engine();
    number(&mut engine, 1, 1, 5.0);
    formula(&mut engine, 1, 3, "=VLOOKUP(6,A1:A1,2,FALSE)");
    formula(&mut engine, 2, 3, "=VLOOKUP(5,A1:A1,2,FALSE)");
    engine.evaluate_all().unwrap();
    let kind = |row| match engine.get_cell_value("Sheet1", row, 3) {
        Some(LiteralValue::Error(error)) => error.kind,
        other => panic!("not an error: {other:?}"),
    };
    assert_eq!(kind(1), formualizer_common::ExcelErrorKind::Na);
    assert_eq!(kind(2), formualizer_common::ExcelErrorKind::Ref);
}
