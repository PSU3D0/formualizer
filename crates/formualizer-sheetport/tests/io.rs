use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[path = "../../formualizer-workbook/tests/common.rs"]
mod workbook_common;

use chrono::NaiveDate;
use formualizer_common::LiteralValue;
use formualizer_eval::traits::VolatileLevel;
use formualizer_sheetport::{
    BatchInput, BatchOptions, BatchProgress, ConstraintViolation, EvalOptions, InputSnapshot,
    InputUpdate, OutputSnapshot, PortValue, SheetPort, SheetPortError, TableRow, TableValue,
};
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
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
    default: "WH-900"
    constraints:
      pattern: "^[A-Z]{2}-\\d{3}$"

  - id: manager_note
    dir: in
    shape: scalar
    required: false
    location:
      a1: Inputs!D2
    schema:
      type: string

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
    default:
      month: 1
      year: 2024

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

const RNG_MANIFEST: &str = r#"
spec: fio
spec_version: "0.3.0"
manifest:
  id: rng-test
  name: RNG Test Manifest
  workbook:
    uri: memory://rng.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: rng_value
    dir: out
    shape: scalar
    location:
      a1: Random!A1
    schema:
      type: number
"#;

fn build_workbook() -> Result<Workbook, SheetPortError> {
    let mut wb = Workbook::new();
    wb.add_sheet("Inputs").map_err(SheetPortError::from)?;
    wb.add_sheet("Inventory").map_err(SheetPortError::from)?;
    wb.add_sheet("Outputs").map_err(SheetPortError::from)?;

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

fn build_rng_workbook() -> Result<Workbook, SheetPortError> {
    let mut wb = Workbook::new();
    wb.add_sheet("Random").map_err(SheetPortError::from)?;
    set_formula(&mut wb, "Random", 1, 1, "RAND()")?;
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

    let rows = vec![
        make_inventory_row("SKU-A", "Alpha", 40, 20, 5),
        make_inventory_row("SKU-B", "Beta", 60, 25, 6),
    ];
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
fn defaults_fill_missing_scalar() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    set_value(&mut workbook, "Inputs", 2, 2, LiteralValue::Empty)?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let inputs = sheetport.read_inputs()?;
    assert_scalar(
        &inputs,
        "warehouse_code",
        |v| matches!(v, LiteralValue::Text(text) if text == "WH-900"),
    );
    Ok(())
}

#[test]
fn record_defaults_fill_missing_fields() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    set_value(&mut workbook, "Inputs", 1, 2, LiteralValue::Empty)?;
    set_value(&mut workbook, "Inputs", 1, 3, LiteralValue::Empty)?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let inputs = sheetport.read_inputs()?;
    assert_record_field(&inputs, "planning_window", "month", |v| {
        matches!(v, LiteralValue::Int(1))
    });
    assert_record_field(&inputs, "planning_window", "year", |v| {
        matches!(v, LiteralValue::Int(2024))
    });
    Ok(())
}

#[test]
fn optional_ports_allow_empty_values() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;
    let manifest = parse_manifest();
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let inputs = sheetport.read_inputs()?;
    assert_scalar(&inputs, "manager_note", |v| {
        matches!(v, LiteralValue::Empty)
    });

    let mut update = InputUpdate::new();
    update.insert("manager_note", PortValue::Scalar(LiteralValue::Empty));
    sheetport.write_inputs(update)?;
    Ok(())
}

#[test]
fn eval_options_control_rng_behavior() -> Result<(), SheetPortError> {
    let mut workbook = build_rng_workbook()?;
    let manifest = Manifest::from_yaml_str(RNG_MANIFEST).expect("rng manifest parses");
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    sheetport
        .workbook_mut()
        .engine_mut()
        .set_volatile_level(VolatileLevel::OnRecalc);
    sheetport.workbook_mut().engine_mut().set_workbook_seed(1);

    let first_opts = EvalOptions {
        rng_seed: Some(1),
        ..EvalOptions::default()
    };

    let first = scalar_number(&sheetport.evaluate_once(first_opts.clone())?, "rng_value");

    set_formula(sheetport.workbook_mut(), "Random", 1, 1, "RAND()")?;

    let second_opts = EvalOptions {
        rng_seed: Some(2),
        ..EvalOptions::default()
    };
    let second = scalar_number(&sheetport.evaluate_once(second_opts.clone())?, "rng_value");
    assert_ne!(first, second, "rng_seed should influence volatile outputs");

    let frozen = EvalOptions {
        freeze_volatile: true,
        rng_seed: Some(5),
        ..EvalOptions::default()
    };

    let frozen_first = scalar_number(&sheetport.evaluate_once(frozen.clone())?, "rng_value");
    let frozen_second = scalar_number(&sheetport.evaluate_once(frozen.clone())?, "rng_value");
    assert_eq!(
        frozen_first, frozen_second,
        "freeze_volatile should stabilize RAND outputs for a fixed seed"
    );

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
        let options = BatchOptions {
            progress: Some(Box::new(move |progress: BatchProgress<'_>| {
                progress_clone
                    .lock()
                    .unwrap()
                    .push((progress.completed, progress.scenario_id.to_string()));
            })),
            ..Default::default()
        };

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
    let mut workbook = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
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
fn layout_table_until_marker_stops_before_marker() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;

    // Marker directly after last data row.
    set_value(
        &mut workbook,
        "Inventory",
        4,
        1,
        LiteralValue::Text("END".into()),
    )?;

    // Extra data after marker that must not be included.
    set_value(
        &mut workbook,
        "Inventory",
        5,
        1,
        LiteralValue::Text("SKU-EXTRA".into()),
    )?;

    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: until-marker, name: Until Marker }
ports:
  - id: sku_inventory
    dir: in
    shape: table
    location:
      layout:
        sheet: Inventory
        header_row: 1
        anchor_col: A
        terminate: until_marker
        marker_text: END
    schema:
      kind: table
      columns:
        - { name: sku, type: string, col: A }
        - { name: description, type: string, col: B }
        - { name: on_hand, type: integer, col: C }
        - { name: safety_stock, type: integer, col: D }
        - { name: lead_time_days, type: integer, col: E }
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let inputs = sheetport.read_inputs()?;
    match inputs.get("sku_inventory") {
        Some(PortValue::Table(table)) => {
            assert_eq!(table.rows.len(), 2);
            assert_eq!(
                table.rows[0].values.get("sku"),
                Some(&LiteralValue::Text("SKU-001".into()))
            );
            assert_eq!(
                table.rows[1].values.get("sku"),
                Some(&LiteralValue::Text("SKU-002".into()))
            );
        }
        other => panic!("expected inventory table, got {other:?}"),
    }

    Ok(())
}

#[test]
fn layout_table_sheet_end_terminates_at_sheet_end_or_falls_back() -> Result<(), SheetPortError> {
    let mut workbook = build_workbook()?;

    // Place data after a blank row so `sheet_end` differs from `first_blank_row` when
    // sheet dimensions are available.
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

    let has_dimensions = workbook.sheet_dimensions("Inventory").is_some();

    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: sheet-end, name: Sheet End }
ports:
  - id: sku_inventory
    dir: in
    shape: table
    location:
      layout:
        sheet: Inventory
        header_row: 1
        anchor_col: A
        terminate: sheet_end
    schema:
      kind: table
      columns:
        - { name: sku, type: string, col: A }
        - { name: description, type: string, col: B }
        - { name: on_hand, type: integer, col: C }
        - { name: safety_stock, type: integer, col: D }
        - { name: lead_time_days, type: integer, col: E }
    constraints:
      nullable: true
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");
    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;

    let inputs = sheetport.read_inputs()?;
    match inputs.get("sku_inventory") {
        Some(PortValue::Table(table)) => {
            if has_dimensions {
                assert!(
                    table.rows.len() >= 4,
                    "expected blank row + extra row included"
                );
                assert_eq!(table.rows[2].values.get("sku"), Some(&LiteralValue::Empty));
                assert_eq!(
                    table.rows[3].values.get("sku"),
                    Some(&LiteralValue::Text("SKU-EXTRA".into()))
                );
            } else {
                assert_eq!(
                    table.rows.len(),
                    2,
                    "sheet_end falls back when dimensions are unavailable"
                );
            }
        }
        other => panic!("expected inventory table, got {other:?}"),
    }

    Ok(())
}

#[test]
fn enum_constraints_use_exact_json_equality() -> Result<(), SheetPortError> {
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").map_err(SheetPortError::from)?;
    workbook
        .set_value("Sheet", 1, 1, LiteralValue::Number(5.0))
        .map_err(SheetPortError::from)?;

    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: enum-test, name: Enum Test }
ports:
  - id: x
    dir: in
    shape: scalar
    location: { a1: Sheet!A1 }
    schema: { type: integer }
    constraints: { enum: [5] }
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let err = match sheetport.read_inputs() {
        Ok(_) => panic!("expected enum constraint violation"),
        Err(err) => err,
    };
    let violations = expect_constraint(err);
    assert!(
        violations.iter().any(|v| v.path == "x"),
        "expected violation on x, got {violations:#?}"
    );
    Ok(())
}

#[test]
fn enum_constraints_accept_exact_float_match() -> Result<(), SheetPortError> {
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").map_err(SheetPortError::from)?;
    workbook
        .set_value("Sheet", 1, 1, LiteralValue::Number(5.0))
        .map_err(SheetPortError::from)?;

    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: enum-test-float, name: Enum Test Float }
ports:
  - id: x
    dir: in
    shape: scalar
    location: { a1: Sheet!A1 }
    schema: { type: integer }
    constraints: { enum: [5.0] }
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
    let inputs = sheetport.read_inputs()?;
    assert_scalar(
        &inputs,
        "x",
        |v| matches!(v, LiteralValue::Number(n) if (*n - 5.0).abs() < f64::EPSILON),
    );
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
        PortValue::Scalar(lit) => assert!(predicate(lit), "unexpected scalar value: {lit:?}"),
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

fn scalar_number(snapshot: &OutputSnapshot, port: &str) -> f64 {
    let value = snapshot
        .get(port)
        .unwrap_or_else(|| panic!("missing port {port}"));
    match value.as_scalar() {
        Some(LiteralValue::Number(n)) => *n,
        Some(LiteralValue::Int(i)) => *i as f64,
        other => panic!("expected numeric scalar for {port}, got {other:?}"),
    }
}
