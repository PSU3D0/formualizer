use super::*;
use crate::xlsx::worksheet::events::Value;
use formualizer_common::LiteralValue;

#[test]
fn touched_rows_clear_strings_formats_duplicates_and_chunk_boundaries() {
    let strings = SharedStringTable::default();
    let styles = Styles::default();
    let limits = WorkbookLoadLimits::default();
    let mut sink = ValueSink::new(Context {
        name: "s",
        source: b"",
        strings: &strings,
        styles: &styles,
        dimensions: (4, 3),
        chunk_rows: 2,
        date_system: DateSystem::Excel1900,
        limits: &limits,
    })
    .unwrap();
    sink.push_decoded(1, 1, Value::Text(Cow::Owned("first".into())), None)
        .unwrap();
    sink.push_decoded(1, 1, Value::Text(Cow::Owned("last".into())), None)
        .unwrap();
    sink.push_decoded(1, 3, Value::Number(1.), Some(FormatId::DATE))
        .unwrap();
    sink.push_decoded(2, 2, Value::Boolean(true), None).unwrap();
    sink.push_decoded(3, 3, Value::Number(7.), None).unwrap();
    sink.observe_formula(3, 3).unwrap();
    sink.push_decoded(4, 1, Value::Number(9.), None).unwrap();
    let arrow = sink.finish((0, 0)).unwrap().arrow;
    assert_eq!(
        arrow.get_cell_value(0, 0),
        LiteralValue::Text("last".into())
    );
    assert_eq!(arrow.get_cell_value(1, 0), LiteralValue::Empty);
    assert_eq!(arrow.get_cell_value(1, 1), LiteralValue::Boolean(true));
    assert_eq!(arrow.get_cell_value(1, 2), LiteralValue::Empty);
    assert_eq!(arrow.get_cell_value(2, 2), LiteralValue::Empty);
    assert_eq!(arrow.format_id(1, 2), None);
    assert_eq!(arrow.get_cell_value(3, 0), LiteralValue::Number(9.));
    assert_eq!(arrow.get_cell_value(3, 1), LiteralValue::Empty);
}

#[test]
fn decoded_dimensions_still_reject_growth_and_population_over_budget() {
    let strings = SharedStringTable::default();
    let styles = Styles::default();
    let limits = WorkbookLoadLimits {
        max_sheet_rows: 3,
        max_sheet_cols: 3,
        max_sheet_logical_cells: 2,
        ..Default::default()
    };
    let context = || Context {
        name: "s",
        source: b"",
        strings: &strings,
        styles: &styles,
        dimensions: (1, 1),
        chunk_rows: 2,
        date_system: DateSystem::Excel1900,
        limits: &limits,
    };
    let mut sink = ValueSink::new(context()).unwrap();
    assert!(sink.push_decoded(0, 1, Value::Number(1.), None).is_err());
    assert!(sink.push_decoded(4, 1, Value::Number(1.), None).is_err());
    let mut sink = ValueSink::new(context()).unwrap();
    sink.push_decoded(1, 1, Value::Number(1.), None).unwrap();
    sink.push_decoded(1, 1, Value::Number(2.), None).unwrap();
    assert!(sink.push_decoded(1, 1, Value::Number(3.), None).is_err());
}

#[test]
fn decoded_out_of_order_and_undersized_hints_keep_sparse_values() {
    let strings = SharedStringTable::default();
    let styles = Styles::default();
    let limits = WorkbookLoadLimits::default();
    let mut sink = ValueSink::new(Context {
        name: "s",
        source: b"",
        strings: &strings,
        styles: &styles,
        dimensions: (1, 1),
        chunk_rows: 2,
        date_system: DateSystem::Excel1900,
        limits: &limits,
    })
    .unwrap();
    sink.push_decoded(3, 1, Value::Number(3.), None).unwrap();
    sink.push_decoded(1, 2, Value::Number(2.), None).unwrap();
    let values = sink.finish((0, 0)).unwrap();
    assert!(values.used_sparse_fallback);
    assert_eq!(values.arrow.get_cell_value(2, 0), LiteralValue::Number(3.));
    assert_eq!(values.arrow.get_cell_value(0, 1), LiteralValue::Number(2.));
}
