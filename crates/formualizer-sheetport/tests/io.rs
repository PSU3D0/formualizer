use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[path = "../../formualizer-workbook/tests/common.rs"]
mod workbook_common;

use chrono::NaiveDate;
use formualizer_common::LiteralValue;
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::reference::{CellRef, Coord};
use formualizer_sheetport::{
    BatchInput, BatchOptions, BatchProgress, ConstraintViolation, EvalOptions, InputSnapshot,
    InputUpdate, PortValue, SheetPort, SheetPortError, TableRow, TableValue,
};
use formualizer_workbook::{LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook};
use sheetport_spec::Manifest;
use workbook_common::build_workbook as build_umya_fixture;

const MANIFEST_YAML: &str = r#"
spec: fio
spec_version: "0.3.0"
manifest:
  id: sheetport-test
  name: SheetPort Test Manifest
  description: Test manifest for SheetPort runtime I/O
  workbook:
    uri: memory://test.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: warehouse_code
    dir: in
    shape: scalar
    location:
      a1: Inputs!B2
    schema:
      type: string
    constraints:
      pattern: "^[A-Z]{2}-\\d{3}$"

  - id: planning_window
    dir: in
    shape: record
    location:
      a1: Inputs!B1:C1
    schema:
      kind: record
      fields:
        month:
          type: integer
          location:
            a1: Inputs!B1
          constraints:
            min: 1
            max: 12
        year:
          type: integer
          location:
            a1: Inputs!C1

  - id: sku_inventory
    dir: in
    shape: table
    location:
      layout:
        sheet: Inventory
        header_row: 1
        anchor_col: A
        terminate: first_blank_row
    schema:
      kind: table
      columns:
        - name: sku
          type: string
          col: A
        - name: description
          type: string
          col: B
        - name: on_hand
          type: integer
          col: C
        - name: safety_stock
          type: integer
          col: D
        - name: lead_time_days
          type: integer
          col: E

  - id: restock_summary
    dir: out
    shape: record
    location:
      a1: Outputs!B2:B5
    schema:
      kind: record
      fields:
        total_skus:
          type: number
          location:
            a1: Outputs!B2
        units_to_order:
          type: number
          location:
            a1: Outputs!B3
        estimated_cost:
          type: number
          location:
            a1: Outputs!B4
        next_restock_date:
          type: date
          location:
            a1: Outputs!B5
"#;

fn build_workbook() -> Result<Workbook, SheetPortError> {
    let mut wb = Workbook::new();
    wb.add_sheet("Inputs");
    wb.add_sheet("Inventory");
    wb.add_sheet("Outputs");

    set_value(&mut wb, "Inputs", 2, 2, LiteralValue::Text("WH-001".into()))?;
    set_value(&mut wb, "Inputs", 1, 2, LiteralValue::Int(3))?;
    set_value(&mut wb, "Inputs", 1, 3, LiteralValue::Int(2025))?;

    // Headers
    set_value(&mut wb, "Inventory", 1, 1, LiteralValue::Text("sku".into()))?;
    set_value(
        &mut wb,
        "Inventory",
        1,
        2,
        LiteralValue::Text("description".into()),
    )?;
    set_value(
        &mut wb,
        "Inventory",
        1,
        3,
        LiteralValue::Text("on_hand".into()),
    )?;
    set_value(
        &mut wb,
        "Inventory",
        1,
        4,
        LiteralValue::Text("safety".into()),
    )?;
    set_value(
        &mut wb,
        "Inventory",
        1,
        5,
        LiteralValue::Text("lead".into()),
    )?;

    // Baseline rows
    set_value(
        &mut wb,
        "Inventory",
        2,
        1,
        LiteralValue::Text("SKU-001".into()),
    )?;
    set_value(
        &mut wb,
        "Inventory",
        2,
        2,
        LiteralValue::Text("Widget".into()),
    )?;
    set_value(&mut wb, "Inventory", 2, 3, LiteralValue::Int(30))?;
    set_value(&mut wb, "Inventory", 2, 4, LiteralValue::Int(12))?;
    set_value(&mut wb, "Inventory", 2, 5, LiteralValue::Int(5))?;

    set_value(
        &mut wb,
        "Inventory",
        3,
        1,
        LiteralValue::Text("SKU-002".into()),
    )?;
    set_value(
        &mut wb,
        "Inventory",
        3,
        2,
        LiteralValue::Text("Gadget".into()),
    )?;
    set_value(&mut wb, "Inventory", 3, 3, LiteralValue::Int(45))?;
    set_value(&mut wb, "Inventory", 3, 4, LiteralValue::Int(18))?;
    set_value(&mut wb, "Inventory", 3, 5, LiteralValue::Int(7))?;

    set_formula(&mut wb, "Outputs", 2, 2, "COUNTA(Inventory!A2:A100)")?;
    set_formula(&mut wb, "Outputs", 3, 2, "SUM(Inventory!C2:C100)")?;
    set_formula(&mut wb, "Outputs", 4, 2, "SUM(Inventory!E2:E100)")?;
    let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    set_value(&mut wb, "Outputs", 5, 2, LiteralValue::Date(date))?;

    wb.evaluate_all().map_err(SheetPortError::from)?;
    Ok(wb)
}

fn set_value(
    workbook: &mut Workbook,
    sheet: &str,
    row: u32,
    col: u32,
    value: LiteralValue,
) -> Result<(), SheetPortError> {
    workbook
        .set_value(sheet, row, col, value)
        .map_err(SheetPortError::from)
}

fn set_formula(
    workbook: &mut Workbook,
    sheet: &str,
    row: u32,
    col: u32,
    formula: &str,
) -> Result<(), SheetPortError> {
    workbook
        .set_formula(sheet, row, col, formula)
        .map_err(SheetPortError::from)
}

fn parse_manifest() -> Manifest {
    Manifest::from_yaml_str(MANIFEST_YAML).expect("manifest parses")
}

#[test]
fn named_range_io_roundtrip() -> Result<(), SheetPortError> {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest:
  id: named-range-test
  name: Named Range Manifest
  workbook:
    uri: memory://named.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: input_value
    dir: in
    shape: scalar
    location:
      name: InputValue
    schema:
      type: number
  - id: output_value
    dir: out
    shape: scalar
    location:
      name: OutputValue
    schema:
      type: number
"#;
    let manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet1");

    workbook
        .set_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .map_err(SheetPortError::from)?;

    let sheet_id = {
        let engine = workbook.engine_mut();
        engine.graph.sheet_id_mut("Sheet1")
    };

    {
        let engine = workbook.engine_mut();
        engine
            .graph
            .define_name(
                "InputValue",
                NamedDefinition::Cell(CellRef::new(sheet_id, Coord::new(0, 0, true, true))),
                NameScope::Workbook,
            )
            .map_err(SheetPortError::from)?;
    }

    set_formula(&mut workbook, "Sheet1", 1, 2, "InputValue*2")?;

    {
        let engine = workbook.engine_mut();
        engine
            .graph
            .define_name(
                "OutputValue",
                NamedDefinition::Cell(CellRef::new(sheet_id, Coord::new(0, 1, true, true))),
                NameScope::Workbook,
            )
            .map_err(SheetPortError::from)?;
    }

    workbook.evaluate_all().map_err(SheetPortError::from)?;

    {
        let value = workbook
            .engine()
            .get_cell_value("Sheet1", 1, 1)
            .unwrap_or(LiteralValue::Empty);
        assert_eq!(value, LiteralValue::Number(10.0));
    }

    let input_addr = workbook
        .named_range_address("InputValue")
        .expect("named range InputValue registered");
    let input_values = workbook.read_range(&input_addr);
    assert_eq!(
        input_values
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(LiteralValue::Empty),
        LiteralValue::Number(10.0)
    );

    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let inputs = sheetport.read_inputs()?;
    assert_scalar(
        &inputs,
        "input_value",
        |v| matches!(v, LiteralValue::Number(n) if (n - 10.0).abs() < 1e-9),
    );

    let mut update = InputUpdate::new();
    update.insert("input_value", PortValue::Scalar(LiteralValue::Number(25.0)));
    sheetport.write_inputs(update)?;

    let outputs = sheetport.evaluate_once(EvalOptions::default())?;
    match outputs.get("output_value") {
        Some(PortValue::Scalar(LiteralValue::Number(n))) => {
            assert_eq!(*n, 50.0)
        }
        other => panic!("expected scalar output value, got {other:?}"),
    }

    Ok(())
}

#[test]
fn singular_io_roundtrip() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let inputs = sheetport.read_inputs()?;
    assert_scalar(
        &inputs,
        "warehouse_code",
        |v| matches!(v, LiteralValue::Text(text) if text == "WH-001"),
    );

    assert_record_field(&inputs, "planning_window", "month", |v| {
        matches!(v, LiteralValue::Int(3))
    });

    let inventory = inputs
        .get("sku_inventory")
        .expect("inventory table present");
    match inventory {
        PortValue::Table(table) => {
            assert_eq!(table.rows.len(), 2);
            let first = table.rows[0]
                .values
                .get("sku")
                .cloned()
                .unwrap_or(LiteralValue::Empty);
            assert_eq!(first, LiteralValue::Text("SKU-001".into()));
        }
        other => panic!("expected table value, got {other:?}"),
    }

    let mut update = InputUpdate::new();
    update.insert(
        "warehouse_code",
        PortValue::Scalar(LiteralValue::Text("WH-900".into())),
    );

    let mut record_update = BTreeMap::new();
    record_update.insert("month".into(), LiteralValue::Int(9));
    update.insert("planning_window", PortValue::Record(record_update));

    let mut rows = Vec::new();
    rows.push(make_inventory_row("SKU-A", "Alpha", 40, 20, 5));
    rows.push(make_inventory_row("SKU-B", "Beta", 60, 25, 6));
    update.insert("sku_inventory", PortValue::Table(TableValue::new(rows)));

    sheetport.write_inputs(update)?;
    let inputs_after = sheetport.read_inputs()?;
    match inputs_after.get("sku_inventory") {
        Some(PortValue::Table(table)) => {
            assert_eq!(table.rows.len(), 2);
            assert_eq!(
                table.rows[0].values.get("on_hand"),
                Some(&LiteralValue::Int(40))
            );
            assert_eq!(
                table.rows[1].values.get("on_hand"),
                Some(&LiteralValue::Int(60))
            );
        }
        other => panic!("expected table after write, got {other:?}"),
    }
    let outputs = sheetport.evaluate_once(EvalOptions::default())?;
    let expected_total = sheetport
        .workbook()
        .get_value("Outputs", 2, 2)
        .unwrap_or(LiteralValue::Empty);
    let expected_units = sheetport
        .workbook()
        .get_value("Outputs", 3, 2)
        .unwrap_or(LiteralValue::Empty);
    let expected_cost = sheetport
        .workbook()
        .get_value("Outputs", 4, 2)
        .unwrap_or(LiteralValue::Empty);

    let summary = outputs.get("restock_summary").expect("summary present");
    match summary {
        PortValue::Record(map) => {
            assert_eq!(map.get("total_skus"), Some(&expected_total));
            assert_eq!(map.get("units_to_order"), Some(&expected_units));
            assert_eq!(map.get("estimated_cost"), Some(&expected_cost));
            assert!(matches!(
                map.get("next_restock_date"),
                Some(LiteralValue::Date(_)) | Some(LiteralValue::DateTime(_))
            ));
        }
        other => panic!("expected record summary, got {other:?}"),
    }

    Ok(())
}

#[test]
fn batch_executor_restores_baseline() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let progress_log: Arc<Mutex<Vec<(usize, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let baseline_inputs: InputSnapshot;

    {
        let manifest = parse_manifest();
        let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
        baseline_inputs = sheetport.read_inputs()?;

        let progress_clone = Arc::clone(&progress_log);
        let mut options = BatchOptions::default();
        options.progress = Some(Box::new(move |progress: BatchProgress<'_>| {
            progress_clone
                .lock()
                .unwrap()
                .push((progress.completed, progress.scenario_id.to_string()));
        }));

        let scenarios = vec![
            BatchInput::new(
                "scenario-a",
                make_update(
                    11,
                    vec![("SKU-X", "Xray", 25, 10, 3), ("SKU-Y", "Yankee", 30, 12, 4)],
                ),
            ),
            BatchInput::new(
                "scenario-b",
                make_update(12, vec![("SKU-Z", "Zulu", 15, 8, 2)]),
            ),
        ];

        let mut executor = sheetport.batch(options)?;
        let results = executor.run(scenarios)?;
        assert_eq!(results.len(), 2);
        for result in results {
            let summary = result
                .outputs
                .get("restock_summary")
                .expect("summary present");
            match summary {
                PortValue::Record(map) => {
                    assert!(map.contains_key("total_skus"));
                    assert!(map.contains_key("units_to_order"));
                    assert!(map.contains_key("estimated_cost"));
                }
                other => panic!("expected record summary, got {other:?}"),
            }
        }
    }

    let manifest = parse_manifest();
    let mut verify_port = SheetPort::new(&mut workbook, manifest)?;
    let after = verify_port.read_inputs()?;
    assert_eq!(after, baseline_inputs);

    let log = progress_log.lock().unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].1, "scenario-a");
    assert_eq!(log[1].1, "scenario-b");

    Ok(())
}

#[test]
fn write_inputs_rejects_pattern_violation() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let mut update = InputUpdate::new();
    update.insert(
        "warehouse_code",
        PortValue::Scalar(LiteralValue::Text("INVALID".into())),
    );

    let err = sheetport
        .write_inputs(update)
        .expect_err("expected constraint violation");
    let violations = expect_constraint(err);
    assert!(violations.iter().any(|v| v.port == "warehouse_code"));
    Ok(())
}

#[test]
fn write_inputs_rejects_out_of_range_record_field() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let mut record = BTreeMap::new();
    record.insert("month".into(), LiteralValue::Int(13));
    let mut update = InputUpdate::new();
    update.insert("planning_window", PortValue::Record(record));

    let err = sheetport
        .write_inputs(update)
        .expect_err("expected constraint violation");
    let violations = expect_constraint(err);
    assert!(
        violations
            .iter()
            .any(|v| v.path.ends_with("planning_window.month"))
    );
    Ok(())
}

#[test]
fn read_inputs_reports_manifest_violation() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    sheetport
        .workbook_mut()
        .set_value("Inputs", 2, 2, LiteralValue::Text("bad".into()))
        .map_err(SheetPortError::from)?;

    let err = sheetport.read_inputs().expect_err("expected violation");
    let violations = expect_constraint(err);
    assert!(violations.iter().any(|v| v.port == "warehouse_code"));
    Ok(())
}

#[test]
fn table_updates_require_all_columns() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let mut row_values = BTreeMap::new();
    row_values.insert("sku".into(), LiteralValue::Text("SKU-MISSING".into()));
    row_values.insert("description".into(), LiteralValue::Text("Missing".into()));
    row_values.insert("on_hand".into(), LiteralValue::Int(10));
    row_values.insert("safety_stock".into(), LiteralValue::Int(5));
    // deliberately omit lead_time_days

    let table = TableValue::new(vec![TableRow::new(row_values)]);
    let mut update = InputUpdate::new();
    update.insert("sku_inventory", PortValue::Table(table));

    let err = sheetport
        .write_inputs(update)
        .expect_err("expected table violation");
    let violations = expect_constraint(err);
    assert!(
        violations
            .iter()
            .any(|v| v.path.contains("sku_inventory[0].lead_time_days"))
    );
    Ok(())
}

#[test]
fn umya_loads_manifest_end_to_end() -> Result<(), SheetPortError> {
    let path = build_umya_inventory_fixture();
    let adapter = UmyaAdapter::open_path(&path).expect("open XLSX fixture");
    let mut workbook = Workbook::from_reader(adapter, LoadStrategy::EagerAll, Default::default())
        .map_err(SheetPortError::from)?;
    workbook.evaluate_all().map_err(SheetPortError::from)?;
    workbook
        .set_value(
            "Outputs",
            5,
            2,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
        )
        .map_err(SheetPortError::from)?;

    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let inputs = sheetport.read_inputs()?;
    assert_scalar(
        &inputs,
        "warehouse_code",
        |v| matches!(v, LiteralValue::Text(code) if code == "WH-001"),
    );

    let outputs = sheetport.evaluate_once(EvalOptions::default())?;
    assert!(matches!(
        outputs.get("restock_summary"),
        Some(PortValue::Record(_))
    ));
    Ok(())
}

#[test]
fn layout_table_stops_at_first_blank_row() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    // Skip row 4 intentionally so there is a blank row between existing data and the extra row.
    set_value(
        &mut workbook,
        "Inventory",
        5,
        1,
        LiteralValue::Text("SKU-EXTRA".into()),
    )?;
    set_value(
        &mut workbook,
        "Inventory",
        5,
        2,
        LiteralValue::Text("Spare Parts".into()),
    )?;
    set_value(&mut workbook, "Inventory", 5, 3, LiteralValue::Int(5))?;
    set_value(&mut workbook, "Inventory", 5, 4, LiteralValue::Int(2))?;
    set_value(&mut workbook, "Inventory", 5, 5, LiteralValue::Int(1))?;

    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let inputs = sheetport.read_inputs()?;
    match inputs.get("sku_inventory") {
        Some(PortValue::Table(table)) => {
            assert_eq!(
                table.rows.len(),
                2,
                "first blank row should terminate layout scan"
            );
        }
        other => panic!("expected inventory table, got {other:?}"),
    }
    Ok(())
}

#[test]
fn table_update_rejects_unknown_columns() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let mut row_values = BTreeMap::new();
    row_values.insert("sku".into(), LiteralValue::Text("SKU-EXTRA".into()));
    row_values.insert("description".into(), LiteralValue::Text("Extra".into()));
    row_values.insert("on_hand".into(), LiteralValue::Int(12));
    row_values.insert("safety_stock".into(), LiteralValue::Int(6));
    row_values.insert("lead_time_days".into(), LiteralValue::Int(3));
    row_values.insert("unexpected".into(), LiteralValue::Int(1));

    let table = TableValue::new(vec![TableRow::new(row_values)]);
    let mut update = InputUpdate::new();
    update.insert("sku_inventory", PortValue::Table(table));

    let err = sheetport
        .write_inputs(update)
        .expect_err("expected validation failure for unknown column");
    let violations = expect_constraint(err);
    assert!(
        violations
            .iter()
            .any(|v| v.path.contains("sku_inventory[0].unexpected"))
    );
    Ok(())
}

#[test]
fn partial_record_update_preserves_other_fields() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let baseline = sheetport.read_inputs()?;
    let original_year = match baseline.get("planning_window") {
        Some(PortValue::Record(map)) => map.get("year").cloned().unwrap_or(LiteralValue::Empty),
        other => panic!("expected record for planning_window, got {other:?}"),
    };

    let mut update = InputUpdate::new();
    let mut record = BTreeMap::new();
    record.insert("month".into(), LiteralValue::Int(11));
    update.insert("planning_window", PortValue::Record(record));
    sheetport.write_inputs(update)?;

    let after = sheetport.read_inputs()?;
    match after.get("planning_window") {
        Some(PortValue::Record(map)) => {
            assert_eq!(map.get("month"), Some(&LiteralValue::Int(11)));
            assert_eq!(map.get("year"), Some(&original_year));
        }
        other => panic!("expected record after update, got {other:?}"),
    }
    Ok(())
}

#[test]
fn batch_executor_handles_empty_scenarios() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let baseline: InputSnapshot;
    {
        let manifest = parse_manifest();
        let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
        baseline = sheetport.read_inputs()?;
        run_empty_batch(&mut sheetport)?;
    }
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let after = sheetport.read_inputs()?;
    assert_eq!(after, baseline);
    Ok(())
}

fn run_empty_batch<'a>(sheetport: &'a mut SheetPort<'a>) -> Result<(), SheetPortError> {
    let mut executor = sheetport.batch(BatchOptions::default())?;
    let results = executor.run(Vec::<BatchInput>::new())?;
    assert!(results.is_empty());
    Ok(())
}

fn build_umya_inventory_fixture() -> std::path::PathBuf {
    build_umya_fixture(|book| {
        let _ = book.new_sheet("Inputs");
        let _ = book.new_sheet("Inventory");
        let _ = book.new_sheet("Outputs");

        if let Some(inputs) = book.get_sheet_by_name_mut("Inputs") {
            inputs.get_cell_mut((2, 2)).set_value("WH-001");
            inputs.get_cell_mut((2, 1)).set_value_number(3.0);
            inputs.get_cell_mut((3, 1)).set_value_number(2025.0);
        }

        if let Some(inventory) = book.get_sheet_by_name_mut("Inventory") {
            inventory.get_cell_mut((1, 1)).set_value("sku");
            inventory.get_cell_mut((2, 1)).set_value("description");
            inventory.get_cell_mut((3, 1)).set_value("on_hand");
            inventory.get_cell_mut((4, 1)).set_value("safety_stock");
            inventory.get_cell_mut((5, 1)).set_value("lead_time_days");

            inventory.get_cell_mut((1, 2)).set_value("SKU-001");
            inventory.get_cell_mut((2, 2)).set_value("Widget");
            inventory.get_cell_mut((3, 2)).set_value_number(30.0);
            inventory.get_cell_mut((4, 2)).set_value_number(12.0);
            inventory.get_cell_mut((5, 2)).set_value_number(5.0);

            inventory.get_cell_mut((1, 3)).set_value("SKU-002");
            inventory.get_cell_mut((2, 3)).set_value("Gadget");
            inventory.get_cell_mut((3, 3)).set_value_number(45.0);
            inventory.get_cell_mut((4, 3)).set_value_number(18.0);
            inventory.get_cell_mut((5, 3)).set_value_number(7.0);
        }

        if let Some(outputs) = book.get_sheet_by_name_mut("Outputs") {
            outputs
                .get_cell_mut((2, 2))
                .set_formula("=COUNTA(Inventory!A2:A100)");
            outputs
                .get_cell_mut((2, 3))
                .set_formula("=SUM(Inventory!C2:C100)");
            outputs
                .get_cell_mut((2, 4))
                .set_formula("=SUM(Inventory!E2:E100)");
            outputs.get_cell_mut((2, 5)).set_value("2025-01-01");
        }
    })
}

fn make_inventory_row(
    sku: &str,
    description: &str,
    on_hand: i64,
    safety: i64,
    lead_time: i64,
) -> TableRow {
    let mut values = BTreeMap::new();
    values.insert("sku".into(), LiteralValue::Text(sku.into()));
    values.insert("description".into(), LiteralValue::Text(description.into()));
    values.insert("on_hand".into(), LiteralValue::Int(on_hand));
    values.insert("safety_stock".into(), LiteralValue::Int(safety));
    values.insert("lead_time_days".into(), LiteralValue::Int(lead_time));
    TableRow::new(values)
}

fn make_update(month: i64, rows: Vec<(&str, &str, i64, i64, i64)>) -> InputUpdate {
    let mut update = InputUpdate::new();
    let mut record = BTreeMap::new();
    record.insert("month".into(), LiteralValue::Int(month));
    update.insert("planning_window", PortValue::Record(record));

    let table_rows = rows
        .into_iter()
        .map(|(sku, desc, on_hand, safety, lead)| {
            make_inventory_row(sku, desc, on_hand, safety, lead)
        })
        .collect();
    update.insert(
        "sku_inventory",
        PortValue::Table(TableValue::new(table_rows)),
    );
    update
}

fn assert_scalar<F>(snapshot: &InputSnapshot, port: &str, predicate: F)
where
    F: Fn(&LiteralValue) -> bool,
{
    let value = snapshot
        .get(port)
        .unwrap_or_else(|| panic!("missing port {port}"));
    match value {
        PortValue::Scalar(lit) => assert!(predicate(&lit), "unexpected scalar value: {lit:?}"),
        other => panic!("expected scalar value for {port}, got {other:?}"),
    }
}

fn assert_record_field<F>(snapshot: &InputSnapshot, port: &str, field: &str, predicate: F)
where
    F: Fn(&LiteralValue) -> bool,
{
    let value = snapshot
        .get(port)
        .unwrap_or_else(|| panic!("missing port {port}"));
    match value {
        PortValue::Record(map) => {
            let lit = map
                .get(field)
                .unwrap_or_else(|| panic!("missing field {field}"));
            assert!(predicate(lit), "unexpected field value: {lit:?}");
        }
        other => panic!("expected record for {port}, got {other:?}"),
    }
}

fn expect_constraint(err: SheetPortError) -> Vec<ConstraintViolation> {
    match err {
        SheetPortError::ConstraintViolation { violations } => violations,
        other => panic!("expected constraint violation error, got {other:?}"),
    }
}
