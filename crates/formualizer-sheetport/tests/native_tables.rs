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

fn roundtrip_manifest(nullable: bool) -> Manifest {
    let input = MANIFEST.split("  - id: total").next().unwrap();
    let input = if nullable {
        format!("{input}    constraints: {{ nullable: true }}\n")
    } else {
        input.to_string()
    };
    let output = input
        .split("  - id: orders")
        .nth(1)
        .unwrap()
        .replace("dir: in", "dir: out");
    let scalars = MANIFEST.split("  - id: total").nth(1).unwrap();
    Manifest::from_yaml_str(&format!(
        "{input}  - id: rows{output}  - id: total{scalars}"
    ))
    .unwrap()
}

fn assert_roundtrip(port: &mut SheetPort<'_>, count: usize, empty: bool) {
    let inputs = port.read_inputs().unwrap();
    let outputs = port.read_outputs().unwrap();
    let input = inputs.get("orders").unwrap().as_table().unwrap();
    let output = outputs.get("rows").unwrap().as_table().unwrap();
    assert_eq!(input, output);
    assert_eq!(input.rows.len(), count);
    assert_eq!(
        port.workbook().table_metadata("Orders").unwrap().empty_body,
        empty
    );
}

fn assert_formula(port: &mut SheetPort<'_>, total: f64, filtered: f64) {
    port.evaluate_once(EvalOptions::default()).unwrap();
    assert_eq!(
        port.workbook().get_value("Outputs", 1, 1),
        Some(LiteralValue::Number(total))
    );
    assert_eq!(
        port.workbook().get_value("Outputs", 2, 1),
        Some(LiteralValue::Number(filtered))
    );
}

#[test]
fn logical_empty_native_body_roundtrips_and_replays_in_both_history_modes() {
    for changelog in [false, true] {
        for nullable in [false, true] {
            let mut wb = workbook(changelog, false);
            let mut port = SheetPort::new(&mut wb, roundtrip_manifest(nullable)).unwrap();
            let before = port.read_inputs().unwrap();
            let metadata = port.workbook().table_metadata("Orders").unwrap();
            port.write_inputs(update(0)).unwrap();
            assert_roundtrip(&mut port, 0, true);
            assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 2);
            // The unmapped unit_arr still contains 1000: logical emptiness cannot
            // be inferred by inspecting blank cells.
            assert_formula(&mut port, 0.0, 0.0);

            port.workbook_mut().undo().unwrap();
            assert_eq!(port.workbook().table_metadata("Orders").unwrap(), metadata);
            assert_eq!(port.read_inputs().unwrap(), before);
            assert_roundtrip(&mut port, 5, false);
            assert_formula(&mut port, 60.0, 36.0);
            port.workbook_mut().redo().unwrap();
            assert_roundtrip(&mut port, 0, true);
            assert_formula(&mut port, 0.0, 0.0);

            let failed = port.workbook_mut().action("materialize then fail", |tx| {
                tx.resize_table("Orders", 6)?;
                tx.set_value("Orders", 2, 3, LiteralValue::Number(9.0))?;
                tx.set_formula("Outputs", 9, 1, "=INVALID((")
            });
            assert!(failed.is_err());
            assert_roundtrip(&mut port, 0, true);
            assert_eq!(port.workbook().table_metadata("Orders").unwrap().end_row, 2);
            assert_formula(&mut port, 0.0, 0.0);

            port.workbook_mut().undo().unwrap();
            let failed = port.workbook_mut().action("empty then fail", |tx| {
                for row in 2..=6 {
                    for col in 1..=3 {
                        tx.set_value("Orders", row, col, LiteralValue::Empty)?;
                    }
                }
                tx.resize_table_body("Orders", 2, true)?;
                tx.set_formula("Outputs", 9, 1, "=INVALID((")
            });
            assert!(failed.is_err());
            assert_eq!(port.workbook().table_metadata("Orders").unwrap(), metadata);
            assert_eq!(port.read_inputs().unwrap(), before);
            assert_roundtrip(&mut port, 5, false);
            assert_formula(&mut port, 60.0, 36.0);
        }
    }
}

#[test]
fn nullable_blank_record_is_not_a_logically_empty_body() {
    for changelog in [false, true] {
        let mut wb = workbook(changelog, false);
        let mut port = SheetPort::new(&mut wb, roundtrip_manifest(true)).unwrap();
        port.write_inputs(update(0)).unwrap();
        let blank = TableValue::new(vec![TableRow::new(BTreeMap::from([
            ("seats".into(), LiteralValue::Empty),
            ("region".into(), LiteralValue::Empty),
            ("order_id".into(), LiteralValue::Empty),
        ]))]);
        let mut update = InputUpdate::new();
        update.insert("orders", PortValue::Table(blank.clone()));
        port.write_inputs(update).unwrap();
        assert_roundtrip(&mut port, 1, false);
        assert_eq!(
            port.read_inputs().unwrap().get("orders"),
            Some(&PortValue::Table(blank))
        );
        assert_formula(&mut port, 0.0, 0.0);
        port.workbook_mut().undo().unwrap();
        assert_roundtrip(&mut port, 0, true);
        port.workbook_mut().redo().unwrap();
        assert_roundtrip(&mut port, 1, false);
    }
}

#[test]
fn edits_and_explicit_resize_materialize_empty_body_reversibly() {
    for changelog in [false, true] {
        for kind in ["value", "formula", "resize"] {
            let mut wb = workbook(changelog, false);
            let mut port = SheetPort::new(&mut wb, roundtrip_manifest(true)).unwrap();
            port.write_inputs(update(0)).unwrap();
            let failed = port.workbook_mut().action("materialize then fail", |tx| {
                match kind {
                    "value" => tx.set_value("Orders", 2, 3, LiteralValue::Number(7.0))?,
                    "formula" => tx.set_formula("Orders", 2, 3, "=7")?,
                    _ => {
                        tx.resize_table("Orders", 2)?;
                    }
                }
                tx.set_formula("Outputs", 9, 1, "=INVALID((")
            });
            assert!(failed.is_err());
            assert_roundtrip(&mut port, 0, true);
            assert_formula(&mut port, 0.0, 0.0);
            port.workbook_mut()
                .action("materialize", |tx| match kind {
                    "value" => tx.set_value("Orders", 2, 3, LiteralValue::Number(7.0)),
                    "formula" => tx.set_formula("Orders", 2, 3, "=7"),
                    _ => tx.resize_table("Orders", 2).map(|_| ()),
                })
                .unwrap();
            assert_roundtrip(&mut port, 1, false);
            assert_formula(&mut port, if kind == "resize" { 0.0 } else { 7.0 }, 0.0);
            port.workbook_mut().undo().unwrap();
            assert_roundtrip(&mut port, 0, true);
            assert_formula(&mut port, 0.0, 0.0);
            port.workbook_mut().redo().unwrap();
            assert_roundtrip(&mut port, 1, false);
            assert_formula(&mut port, if kind == "resize" { 0.0 } else { 7.0 }, 0.0);
        }
    }
}

#[test]
fn logical_empty_body_excludes_unmapped_values_and_direct_engine_edits_restore_it() {
    for formula in [false, true] {
        let mut wb = workbook(false, false);
        wb.set_formula("Outputs", 3, 1, "=SUM(Orders[unit_arr])")
            .unwrap();
        wb.evaluate_all().unwrap();
        {
            let mut port = SheetPort::new(&mut wb, roundtrip_manifest(true)).unwrap();
            port.write_inputs(update(0)).unwrap();
        }
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Outputs", 3, 1),
            Some(LiteralValue::Number(0.0))
        );
        wb.engine_mut()
            .set_cell_value("Orders", 2, 3, LiteralValue::Empty)
            .unwrap();
        assert!(wb.table_metadata("Orders").unwrap().empty_body);
        if formula {
            let ast = formualizer_parse::parse("=7").unwrap();
            wb.engine_mut()
                .set_cell_formula("Orders", 2, 3, ast)
                .unwrap();
        } else {
            wb.engine_mut()
                .set_cell_value("Orders", 2, 3, LiteralValue::Number(7.0))
                .unwrap();
        }
        assert!(!wb.table_metadata("Orders").unwrap().empty_body);
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Outputs", 3, 1),
            Some(LiteralValue::Number(1000.0))
        );
    }
}

// Exercise the public Workbook methods directly: action's transaction setters
// use a different eager path and cannot catch deferred-ingestion regressions.
#[test]
fn bulk_and_deferred_writes_materialize_empty_native_tables() {
    for changelog in [false, true] {
        for kind in [
            "bulk_formula",
            "formula",
            "formulas",
            "values",
            "range_formula",
            "range_value",
        ] {
            let mut wb = workbook(changelog, false);
            wb.engine_mut().config.defer_graph_building = true;
            wb.set_formula(
                "Outputs",
                2,
                1,
                "=SUMIFS(Orders[seats], Orders[region], \"\")",
            )
            .unwrap();
            let mut port = SheetPort::new(&mut wb, roundtrip_manifest(true)).unwrap();
            port.write_inputs(update(0)).unwrap();
            assert_roundtrip(&mut port, 0, true);

            let wb = port.workbook_mut();
            wb.begin_action("public bulk edit");
            match kind {
                "bulk_formula" => {
                    wb.engine_mut()
                        .bulk_set_formulas(
                            "Orders",
                            [(2, 3, formualizer_parse::parse("=7").unwrap())],
                        )
                        .unwrap();
                }
                "formula" => wb.set_formula("Orders", 2, 3, "=7").unwrap(),
                "formulas" => wb
                    .set_formulas("Orders", 2, 3, &[vec!["=7".into()]])
                    .unwrap(),
                "values" => wb
                    .set_values("Orders", 2, 3, &[vec![LiteralValue::Number(7.0)]])
                    .unwrap(),
                _ => {
                    let data = if kind == "range_formula" {
                        formualizer_workbook::traits::CellData::from_formula("=7")
                    } else {
                        formualizer_workbook::traits::CellData::from_value(7.0)
                    };
                    wb.write_range("Orders", (2, 3), BTreeMap::from([((2, 3), data)]))
                        .unwrap();
                }
            }
            wb.end_action();
            assert_roundtrip(&mut port, 1, false);
            port.workbook_mut().evaluate_all().unwrap();
            assert_formula(&mut port, 7.0, 7.0);
            let inputs = port.read_inputs().unwrap();
            assert_eq!(
                inputs.get("orders").unwrap().as_table().unwrap().rows[0]
                    .values
                    .get("seats"),
                Some(&LiteralValue::Number(7.0))
            );
            if changelog && kind != "bulk_formula" {
                port.workbook_mut().undo().unwrap();
                assert_roundtrip(&mut port, 0, true);
                assert_formula(&mut port, 0.0, 0.0);
                port.workbook_mut().redo().unwrap();
                assert_roundtrip(&mut port, 1, false);
                assert_formula(&mut port, 7.0, 7.0);
            }
        }
    }
}

#[test]
fn rejected_deferred_formulas_preserve_logically_empty_native_tables() {
    for changelog in [false, true] {
        for kind in ["formula", "formulas", "range"] {
            let mut wb = workbook(changelog, false);
            wb.engine_mut().config.defer_graph_building = true;
            let mut port = SheetPort::new(&mut wb, roundtrip_manifest(true)).unwrap();
            port.write_inputs(update(0)).unwrap();
            let wb = port.workbook_mut();
            let result = match kind {
                "formula" => wb.set_formula("Orders", 2, 3, "=INVALID(("),
                "formulas" => wb.set_formulas("Orders", 2, 3, &[vec!["=INVALID((".into()]]),
                _ => wb.write_range(
                    "Orders",
                    (2, 3),
                    BTreeMap::from([(
                        (2, 3),
                        formualizer_workbook::traits::CellData::from_formula("=INVALID(("),
                    )]),
                ),
            };
            assert!(result.is_err());
            assert_roundtrip(&mut port, 0, true);
            assert_formula(&mut port, 0.0, 0.0);
        }
    }
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
