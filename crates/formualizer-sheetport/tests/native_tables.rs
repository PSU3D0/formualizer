use formualizer_common::LiteralValue;
use formualizer_sheetport::{
    EvalOptions, InputUpdate, PortValue, SheetPort, SheetPortError, TableRow, TableValue,
};
use formualizer_workbook::{Workbook, WorkbookConfig};
use sheetport_spec::Manifest;
use std::collections::BTreeMap;

const MANIFEST: &str = r#"
spec: fio
spec_version: "0.3.0"
capabilities: { profile: full-v0 }
manifest: { id: orders, name: Orders }
ports:
  - id: orders
    dir: in
    shape: table
    location: { table: { name: Orders, area: body } }
    schema:
      kind: table
      columns:
        - { name: seats, type: number }
        - { name: region, type: string }
        - { name: order_id, type: number }
  - id: total
    dir: out
    shape: scalar
    location: { a1: Outputs!A1 }
    schema: { type: number }
  - id: filtered
    dir: out
    shape: scalar
    location: { a1: Outputs!A2 }
    schema: { type: number }
"#;

fn workbook(changelog: bool, totals: bool) -> Workbook {
    let mut config = WorkbookConfig::ephemeral();
    config.enable_changelog = changelog;
    let mut wb = Workbook::new_with_config(config);
    for sheet in ["Orders", "Assumptions", "Outputs"] {
        wb.add_sheet(sheet).unwrap();
    }
    let headers: Vec<String> = ["order_id", "region", "seats", "unit_arr"]
        .into_iter()
        .map(String::from)
        .collect();
    for (i, name) in headers.iter().enumerate() {
        wb.set_value("Orders", 1, i as u32 + 1, LiteralValue::Text(name.clone()))
            .unwrap();
    }
    for i in 0..5 {
        wb.set_values(
            "Orders",
            i + 2,
            1,
            &[vec![
                LiteralValue::Number(i as f64),
                LiteralValue::Text(if i % 2 == 0 { "EMEA" } else { "AMER" }.into()),
                LiteralValue::Number(10.0 + i as f64),
                LiteralValue::Number(1000.0),
            ]],
        )
        .unwrap();
    }
    if totals {
        wb.set_value("Orders", 7, 3, LiteralValue::Number(60.0))
            .unwrap();
    }
    wb.define_table(
        "Orders",
        "Orders",
        (1, 1, if totals { 7 } else { 6 }, 4),
        headers,
        true,
        totals,
    )
    .unwrap();
    wb.set_value("Assumptions", 1, 2, LiteralValue::Text("EMEA".into()))
        .unwrap();
    wb.set_formula("Outputs", 1, 1, "=SUM(Orders[seats])")
        .unwrap();
    wb.set_formula(
        "Outputs",
        2,
        1,
        "=SUMIFS(Orders[seats], Orders[region], Assumptions!B1)",
    )
    .unwrap();
    wb.evaluate_all().unwrap();
    wb
}

fn update(count: usize) -> InputUpdate {
    let rows = (0..count)
        .map(|i| {
            TableRow::new(BTreeMap::from([
                ("order_id".into(), LiteralValue::Number(i as f64)),
                (
                    "region".into(),
                    LiteralValue::Text(if i % 2 == 0 { "EMEA" } else { "AMER" }.into()),
                ),
                ("seats".into(), LiteralValue::Number(i as f64 + 1.0)),
            ]))
        })
        .collect();
    let mut update = InputUpdate::new();
    update.insert("orders", PortValue::Table(TableValue::new(rows)));
    update
}

fn assert_outputs(port: &mut SheetPort<'_>, total: f64, filtered: f64) {
    let outputs = port.evaluate_once(EvalOptions::default()).unwrap();
    assert_eq!(
        outputs.get("total"),
        Some(&PortValue::Scalar(LiteralValue::Number(total)))
    );
    assert_eq!(
        outputs.get("filtered"),
        Some(&PortValue::Scalar(LiteralValue::Number(filtered)))
    );
}

#[test]
fn native_table_grow_shrink_and_history_restore_cells_extent_and_formulas() {
    for changelog in [true, false] {
        let mut wb = workbook(changelog, false);
        let baseline: Vec<_> = (1..=51)
            .flat_map(|r| (1..=4).map(move |c| (r, c)))
            .map(|(r, c)| {
                (
                    r,
                    c,
                    wb.get_value("Orders", r, c).unwrap_or(LiteralValue::Empty),
                )
            })
            .collect();
        let mut port = SheetPort::new(&mut wb, Manifest::from_yaml_str(MANIFEST).unwrap()).unwrap();
        assert_outputs(&mut port, 60.0, 36.0);
        assert_eq!(
            port.read_inputs()
                .unwrap()
                .get("orders")
                .unwrap()
                .as_table()
                .unwrap()
                .rows
                .len(),
            5
        );

        port.write_inputs(update(50)).unwrap();
        assert_eq!(
            port.workbook().table_metadata("Orders").unwrap().end_row,
            51
        );
        assert_outputs(&mut port, 1275.0, 625.0);
        assert_eq!(
            port.read_inputs()
                .unwrap()
                .get("orders")
                .unwrap()
                .as_table()
                .unwrap()
                .rows
                .len(),
            50
        );
        // Schema maps reordered, non-contiguous columns; the omitted column survives.
        assert_eq!(
            port.workbook().get_value("Orders", 2, 4),
            Some(LiteralValue::Number(1000.0))
        );

        port.write_inputs(update(3)).unwrap();
        assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 4);
        for row in 5..=51 {
            for col in 1..=3 {
                assert_eq!(
                    port.workbook()
                        .get_value("Orders", row, col)
                        .unwrap_or(LiteralValue::Empty),
                    LiteralValue::Empty
                );
            }
        }
        assert_outputs(&mut port, 6.0, 4.0);

        port.workbook_mut().undo().unwrap();
        assert_eq!(
            port.workbook().table_metadata("Orders").unwrap().end_row,
            51
        );
        assert_eq!(
            port.workbook().get_value("Orders", 51, 3),
            Some(LiteralValue::Number(50.0))
        );
        assert_outputs(&mut port, 1275.0, 625.0);
        port.workbook_mut().undo().unwrap();
        assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 6);
        for (r, c, value) in &baseline {
            assert_eq!(
                &port
                    .workbook()
                    .get_value("Orders", *r, *c)
                    .unwrap_or(LiteralValue::Empty),
                value
            );
        }
        assert_outputs(&mut port, 60.0, 36.0);
        port.workbook_mut().redo().unwrap();
        assert_eq!(
            port.workbook().table_metadata("Orders").unwrap().end_row,
            51
        );
        assert_outputs(&mut port, 1275.0, 625.0);
        port.workbook_mut().redo().unwrap();
        assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 4);
        assert_outputs(&mut port, 6.0, 4.0);
    }
}

#[test]
fn empty_native_input_retains_one_blank_body_row_and_is_reversible() {
    let mut wb = workbook(true, false);
    // The default native selector area is body.
    let manifest = Manifest::from_yaml_str(&MANIFEST.replace(", area: body", "")).unwrap();
    let mut port = SheetPort::new(&mut wb, manifest).unwrap();
    port.write_inputs(update(0)).unwrap();
    assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 2);
    assert_outputs(&mut port, 0.0, 0.0);
    for row in 2..=6 {
        for col in 1..=3 {
            assert_eq!(
                port.workbook()
                    .get_value("Orders", row, col)
                    .unwrap_or(LiteralValue::Empty),
                LiteralValue::Empty
            );
        }
    }
    port.workbook_mut().undo().unwrap();
    assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 6);
    assert_outputs(&mut port, 60.0, 36.0);
}

#[test]
fn native_totals_allow_fixed_size_writes_but_reject_resize_atomically() {
    let mut wb = workbook(true, true);
    let mut port = SheetPort::new(&mut wb, Manifest::from_yaml_str(MANIFEST).unwrap()).unwrap();
    let error = port.write_inputs(update(50)).unwrap_err();
    assert!(matches!(error, SheetPortError::UnsupportedSelector { .. }));
    assert!(error.to_string().contains("totals row"));
    assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 7);
    assert_outputs(&mut port, 60.0, 36.0);
    port.write_inputs(update(5)).unwrap();
    assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 7);
    assert_eq!(
        port.workbook().get_value("Orders", 7, 3),
        Some(LiteralValue::Number(60.0))
    );
    assert_outputs(&mut port, 15.0, 9.0);
    port.workbook_mut().undo().unwrap();
    assert_outputs(&mut port, 60.0, 36.0);
}

#[test]
fn native_writes_reject_header_totals_and_duplicate_column_mappings() {
    for area in ["header", "totals"] {
        let mut wb = workbook(true, true);
        let manifest =
            Manifest::from_yaml_str(&MANIFEST.replace("area: body", &format!("area: {area}")))
                .unwrap();
        let mut port = SheetPort::new(&mut wb, manifest).unwrap();
        assert!(matches!(
            port.write_inputs(update(5)),
            Err(SheetPortError::UnsupportedSelector { .. })
        ));
        assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 7);
    }
    let mut wb = workbook(true, false);
    let manifest = Manifest::from_yaml_str(
        &MANIFEST.replace("name: order_id, type: number", "name: SEATS, type: number"),
    )
    .unwrap();
    let mut port = SheetPort::new(&mut wb, manifest).unwrap();
    let error = port.read_inputs().unwrap_err();
    assert!(error.to_string().contains("distinct"), "{error}");
}
