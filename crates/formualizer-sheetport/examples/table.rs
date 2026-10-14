use formualizer_common::LiteralValue;
use formualizer_sheetport::{EvalOptions, InputUpdate, PortValue, SheetPort, TableRow, TableValue};
use formualizer_workbook::Workbook;
use sheetport_spec::{
    LayoutDescriptor, LayoutKind, LayoutTermination, Manifest, Port, Schema, Selector, TableColumn,
    ValueType,
};
use std::collections::BTreeMap;

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut workbook = Workbook::new();
    workbook.add_sheet("Orders")?;
    workbook.set_value("Orders", 1, 1, LiteralValue::Text("sku".into()))?;
    workbook.set_value("Orders", 1, 2, LiteralValue::Text("quantity".into()))?;
    let location = Selector::layout(LayoutDescriptor {
        kind: LayoutKind::default(),
        sheet: "Orders".into(),
        header_row: 1,
        anchor_col: "A".into(),
        terminate: LayoutTermination::FirstBlankRow,
        marker_text: None,
    });
    let schema = Schema::table([
        TableColumn::new("sku", ValueType::String),
        TableColumn::new("quantity", ValueType::Integer),
    ])
    .with_keys(["sku".into()]);
    let manifest = Manifest::new("orders")
        .port(Port::input("orders", location.clone(), schema.clone()))
        .port(Port::output("result", location, schema));
    let mut session = SheetPort::new(&mut workbook, manifest)?;
    let rows = vec![TableRow::new(BTreeMap::from([
        ("sku".into(), LiteralValue::Text("ABC".into())),
        ("quantity".into(), LiteralValue::Int(3)),
    ]))];
    let mut inputs = InputUpdate::new();
    inputs.insert("orders", PortValue::Table(TableValue::new(rows)));
    session.write_inputs(inputs)?;
    let outputs = session.evaluate_once(EvalOptions::default())?;
    let Some(PortValue::Table(table)) = outputs.get("result") else {
        panic!("expected table")
    };
    assert_eq!(table.rows.len(), 1);
    assert_eq!(
        table.rows[0].get("sku"),
        Some(&LiteralValue::Text("ABC".into()))
    );
    Ok(())
}
