//! Runtime table definition (issue #212).
//!
//! Reproduces the reporter's scenario without the serialise-and-reload round
//! trip: build a sheet with `set_value`, define a table over it, and evaluate
//! structured references.
use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

fn workbook_with_data() -> Workbook {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    for (col, header) in [(1u32, "Nama"), (2, "Nilai")] {
        wb.set_value("S", 1, col, LiteralValue::Text(header.to_string()))
            .unwrap();
    }
    for (row, (name, score)) in [("Ani", 10.0), ("Budi", 20.0), ("Cici", 30.0)]
        .iter()
        .enumerate()
    {
        let row = row as u32 + 2;
        wb.set_value("S", row, 1, LiteralValue::Text((*name).to_string()))
            .unwrap();
        wb.set_value("S", row, 2, LiteralValue::Number(*score))
            .unwrap();
    }
    wb
}

#[test]
fn a_table_defined_at_runtime_answers_structured_references() {
    let mut wb = workbook_with_data();
    wb.define_table(
        "Table1",
        "S",
        (1, 1, 4, 2),
        vec!["Nama".into(), "Nilai".into()],
        true,
        false,
    )
    .expect("define table over existing cells");

    for (row, (formula, expected)) in [
        ("=SUM(Table1[Nilai])", 60.0),
        ("=COUNTA(Table1[#Headers])", 2.0),
        ("=COUNTA(Table1[#All])", 8.0),
        ("=SUMIF(Table1[Nama],\"Budi\",Table1[Nilai])", 20.0),
    ]
    .iter()
    .enumerate()
    {
        let row = row as u32 + 10;
        wb.set_formula("S", row, 4, formula).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("S", row, 4),
            Some(LiteralValue::Number(*expected)),
            "{formula}"
        );
    }
}

#[test]
fn edits_inside_a_runtime_table_propagate() {
    let mut wb = workbook_with_data();
    wb.define_table(
        "Table1",
        "S",
        (1, 1, 4, 2),
        vec!["Nama".into(), "Nilai".into()],
        true,
        false,
    )
    .unwrap();
    wb.set_formula("S", 10, 4, "=SUM(Table1[Nilai])").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 10, 4), Some(LiteralValue::Number(60.0)));

    wb.set_value("S", 3, 2, LiteralValue::Number(200.0))
        .unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 10, 4), Some(LiteralValue::Number(240.0)));
}

#[test]
fn resize_without_cell_edits_invalidates_structured_references_and_is_reversible() {
    for changelog in [false, true] {
        let mut config = formualizer_workbook::WorkbookConfig::ephemeral();
        config.enable_changelog = changelog;
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("S").unwrap();
        wb.set_values(
            "S",
            2,
            1,
            &[
                vec![LiteralValue::Number(10.0)],
                vec![LiteralValue::Number(20.0)],
                vec![LiteralValue::Number(30.0)],
            ],
        )
        .unwrap();
        wb.define_table(
            "Orders",
            "S",
            (1, 1, 2, 1),
            vec!["seats".into()],
            true,
            false,
        )
        .unwrap();
        wb.set_formula("S", 1, 3, "=SUM(Orders[seats])").unwrap();
        wb.set_formula("S", 2, 3, "=C1*2").unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 2, 3), Some(LiteralValue::Number(20.0)));
        assert_eq!(wb.resize_table("orders", 4).unwrap().end_row, 4);
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 2, 3), Some(LiteralValue::Number(120.0)));
        // New row stripes must follow a grow even after evaluation caches warm.
        wb.action("edit expanded row", |tx| {
            tx.set_value("S", 4, 1, LiteralValue::Number(40.0))
        })
        .unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 2, 3), Some(LiteralValue::Number(140.0)));
        wb.undo().unwrap();
        wb.undo().unwrap();
        assert_eq!(wb.table_metadata("Orders").unwrap().end_row, 2);
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 2, 3), Some(LiteralValue::Number(20.0)));
        wb.redo().unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 2, 3), Some(LiteralValue::Number(120.0)));
        // Changes outside a shrunken table no longer affect its aggregate.
        wb.resize_table("Orders", 2).unwrap();
        wb.set_value("S", 4, 1, LiteralValue::Number(99.0)).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 2, 3), Some(LiteralValue::Number(20.0)));
        assert!(wb.resize_table("Orders", 1).is_err());
        assert!(wb.resize_table("Orders", 1_048_577).is_err());
        assert!(wb.resize_table("missing", 4).is_err());
    }
}

#[test]
fn failed_action_restores_table_extent_values_and_future_invalidation() {
    for changelog in [false, true] {
        let mut config = formualizer_workbook::WorkbookConfig::ephemeral();
        config.enable_changelog = changelog;
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("S").unwrap();
        wb.set_value("S", 2, 1, LiteralValue::Number(10.0)).unwrap();
        wb.define_table(
            "Orders",
            "S",
            (1, 1, 2, 1),
            vec!["seats".into()],
            true,
            false,
        )
        .unwrap();
        wb.set_formula("S", 1, 3, "=SUM(Orders[seats])").unwrap();
        wb.evaluate_all().unwrap();
        let failed = wb.action("resize then fail", |tx| {
            tx.resize_table("Orders", 100)?;
            tx.set_value("S", 100, 1, LiteralValue::Number(99.0))?;
            tx.set_formula("S", 1, 4, "=INVALID((")
        });
        assert!(failed.is_err());
        assert_eq!(wb.table_metadata("Orders").unwrap().end_row, 2);
        assert_eq!(
            wb.get_value("S", 100, 1).unwrap_or(LiteralValue::Empty),
            LiteralValue::Empty
        );
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 1, 3), Some(LiteralValue::Number(10.0)));
        wb.action("future resize", |tx| {
            tx.resize_table("Orders", 100)?;
            tx.set_value("S", 100, 1, LiteralValue::Number(99.0))
        })
        .unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 1, 3), Some(LiteralValue::Number(109.0)));
        wb.undo().unwrap();
        assert_eq!(wb.table_metadata("Orders").unwrap().end_row, 2);
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("S", 1, 3), Some(LiteralValue::Number(10.0)));
    }
}

#[test]
fn defined_tables_are_listed_with_their_metadata() {
    let mut wb = workbook_with_data();
    wb.define_table(
        "Table1",
        "S",
        (1, 1, 4, 2),
        vec!["Nama".into(), "Nilai".into()],
        true,
        false,
    )
    .unwrap();
    let tables = wb.tables();
    assert_eq!(tables.len(), 1);
    let table = &tables[0];
    assert_eq!(table.name, "Table1");
    assert_eq!(table.sheet, "S");
    assert_eq!(
        (
            table.start_row,
            table.start_col,
            table.end_row,
            table.end_col
        ),
        (1, 1, 4, 2)
    );
    assert_eq!(table.headers, vec!["Nama".to_string(), "Nilai".to_string()]);
    assert!(table.header_row);
    assert!(!table.totals_row);
    assert_eq!(
        wb.table_metadata("Table1").map(|t| t.name),
        Some("Table1".to_string())
    );
}

/// The reporter found the JSON shape by feeding wrong values to `fromJson` and
/// following serde errors. The runtime API rejects the same mistakes by name.
#[test]
fn invalid_table_definitions_are_rejected_with_actionable_messages() {
    let mut wb = workbook_with_data();

    let err = wb
        .define_table("T", "S", (1, 1, 4, 2), vec!["only-one".into()], true, false)
        .expect_err("header count must match the column span");
    assert!(
        err.message().unwrap_or_default().contains("2 column"),
        "message should state the span, got {:?}",
        err.message()
    );

    let err = wb
        .define_table(
            "T",
            "Missing",
            (1, 1, 4, 2),
            vec!["a".into(), "b".into()],
            true,
            false,
        )
        .expect_err("unknown sheet");
    assert!(err.message().unwrap_or_default().contains("Missing"));

    let err = wb
        .define_table(
            "T",
            "S",
            (0, 1, 4, 2),
            vec!["a".into(), "b".into()],
            true,
            false,
        )
        .expect_err("rows are 1-based");
    assert!(err.message().unwrap_or_default().contains("1-based"));

    let err = wb
        .define_table(
            "T",
            "S",
            (4, 1, 1, 2),
            vec!["a".into(), "b".into()],
            true,
            false,
        )
        .expect_err("inverted range");
    assert!(err.message().unwrap_or_default().contains("inverted"));

    let err = wb
        .define_table(
            "T",
            "S",
            (1, 1, 1, 2),
            vec!["a".into(), "b".into()],
            true,
            false,
        )
        .expect_err("header row with no data rows");
    assert!(err.message().unwrap_or_default().contains("no data rows"));

    assert!(wb.tables().is_empty(), "no partial definition may survive");
}
