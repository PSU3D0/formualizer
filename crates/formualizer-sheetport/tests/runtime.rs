use chrono::TimeZone;
use formualizer_common::LiteralValue;
use formualizer_eval::engine::DeterministicMode;
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::reference::{CellRef, Coord, RangeRef};
use formualizer_eval::timezone::TimeZoneSpec;
use formualizer_eval::traits::VolatileLevel;
use formualizer_sheetport::{EvalOptions, InputUpdate, PortValue, SheetPort, SheetPortSession};
use formualizer_workbook::Workbook;
use sheetport_spec::Manifest;

fn make_test_workbook() -> Workbook {
    let mut wb = Workbook::new();
    wb.add_sheet("Inputs").unwrap();
    wb.add_sheet("Inventory").unwrap();
    wb.add_sheet("Outputs").unwrap();
    wb
}

#[test]
fn sheetport_initializes_with_matching_workbook() {
    let yaml = include_str!("../../sheetport-spec/tests/fixtures/supply_planning.yaml");
    let manifest: Manifest = Manifest::from_yaml_str(yaml).expect("fixture parses");
    let mut workbook = make_test_workbook();

    SheetPort::new(&mut workbook, manifest).expect("sheetport binds to workbook");
}

#[test]
fn sheetport_fails_for_missing_sheet() {
    let yaml = include_str!("../../sheetport-spec/tests/fixtures/supply_planning.yaml");
    let manifest: Manifest = Manifest::from_yaml_str(yaml).expect("fixture parses");
    let mut workbook = Workbook::new();
    workbook.add_sheet("Inputs").unwrap();
    workbook.add_sheet("Outputs").unwrap();

    let err = match SheetPort::new(&mut workbook, manifest) {
        Ok(_) => panic!("expected missing sheet error"),
        Err(err) => err,
    };
    match err {
        formualizer_sheetport::SheetPortError::MissingSheet { port, sheet } => {
            assert_eq!(port, "sku_inventory");
            assert_eq!(sheet, "Inventory");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sheetport_session_supports_owned_workbook() {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest:
  id: session-test
  name: Session Test
  workbook:
    uri: memory://session.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: input_a
    dir: in
    shape: scalar
    location:
      a1: Sheet!A1
    schema:
      type: number
  - id: output_b
    dir: out
    shape: scalar
    location:
      a1: Sheet!B1
    schema:
      type: number
"#;
    let manifest: Manifest =
        Manifest::from_yaml_str(manifest_yaml).expect("session manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    workbook
        .set_value("Sheet", 1, 1, LiteralValue::Number(5.0))
        .expect("set A1");
    workbook
        .set_value("Sheet", 1, 2, LiteralValue::Number(10.0))
        .expect("set B1");

    let mut session = SheetPortSession::new(workbook, manifest).expect("session created");

    let inputs = session.read_inputs().expect("read inputs");
    match inputs.get("input_a") {
        Some(PortValue::Scalar(LiteralValue::Number(n))) => assert_eq!(*n, 5.0),
        other => panic!("unexpected input value: {other:?}"),
    }

    let mut update = InputUpdate::new();
    update.insert("input_a", PortValue::Scalar(LiteralValue::Number(7.5)));
    session.write_inputs(update).expect("write inputs");
    assert_eq!(
        session
            .workbook()
            .get_value("Sheet", 1, 1)
            .unwrap_or(LiteralValue::Empty),
        LiteralValue::Number(7.5)
    );

    let outputs = session
        .evaluate_once(EvalOptions::default())
        .expect("evaluate once");
    match outputs.get("output_b") {
        Some(PortValue::Scalar(LiteralValue::Number(n))) => assert_eq!(*n, 10.0),
        other => panic!("unexpected output value: {other:?}"),
    }
}

#[test]
fn evaluate_once_builds_all_staged_formulas_before_targeted_sheetport_eval() {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest:
  id: staged-cross-sheet
  name: Staged Cross Sheet
  workbook:
    uri: memory://staged-cross-sheet.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: output_a
    dir: out
    shape: scalar
    location:
      a1: Outputs!A1
    schema:
      type: number
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Inputs").unwrap();
    workbook.add_sheet("Calc").unwrap();
    workbook.add_sheet("Outputs").unwrap();
    workbook
        .set_value("Inputs", 1, 1, LiteralValue::Number(10.0))
        .expect("set input");
    workbook
        .set_formula("Calc", 1, 1, "=Inputs!A1*2")
        .expect("stage calc formula");
    workbook
        .set_formula("Outputs", 1, 1, "=Calc!A1+1")
        .expect("stage output formula");
    assert!(workbook.has_staged_formulas());

    let mut sheetport = SheetPort::new(&mut workbook, manifest).expect("sheetport binds");
    let outputs = sheetport
        .evaluate_once(EvalOptions::default())
        .expect("evaluate once");

    match outputs.get("output_a") {
        Some(PortValue::Scalar(LiteralValue::Number(n))) => assert_eq!(*n, 21.0),
        other => panic!("unexpected output value: {other:?}"),
    }
    assert_eq!(
        sheetport
            .workbook()
            .get_value("Outputs", 1, 1)
            .unwrap_or(LiteralValue::Empty),
        LiteralValue::Number(21.0)
    );
    assert!(!sheetport.workbook().has_staged_formulas());
}

#[test]
fn evaluate_once_invalid_deterministic_mode_does_not_leak_other_overrides() {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest:
  id: deterministic-leak-test
  name: Deterministic Leak Test
  workbook:
    uri: memory://deterministic-leak.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: output_a
    dir: out
    shape: scalar
    location:
      a1: Sheet!A1
    schema:
      type: number
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    workbook
        .set_value("Sheet", 1, 1, LiteralValue::Number(1.0))
        .expect("set A1");
    workbook.engine_mut().set_workbook_seed(7);
    workbook
        .engine_mut()
        .set_volatile_level(VolatileLevel::Always);
    let original_mode = workbook.engine().config.deterministic_mode.clone();

    let mut sheetport = SheetPort::new(&mut workbook, manifest).expect("sheetport binds");
    let timestamp = chrono::Utc
        .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
        .single()
        .expect("valid timestamp");

    let err = sheetport
        .evaluate_once(EvalOptions {
            freeze_volatile: true,
            rng_seed: Some(123),
            deterministic_mode: Some(DeterministicMode::Enabled {
                timestamp_utc: timestamp,
                timezone: TimeZoneSpec::Local,
            }),
            ..Default::default()
        })
        .expect_err("local timezone should be rejected in deterministic mode");

    match err {
        formualizer_sheetport::SheetPortError::Engine { .. } => {}
        other => panic!("unexpected error: {other:?}"),
    }

    assert_eq!(sheetport.workbook().engine().config.workbook_seed, 7);
    assert_eq!(
        sheetport.workbook().engine().config.volatile_level,
        VolatileLevel::Always
    );
    assert_eq!(
        &sheetport.workbook().engine().config.deterministic_mode,
        &original_mode
    );
}

#[test]
fn sheetport_accepts_full_profile_for_supported_selectors() {
    let manifest_yaml = r#"
 spec: fio
 spec_version: "0.3.0"
 capabilities: { profile: full-v0 }
 manifest: { id: profile-test, name: Profile Test }
 ports:
   - id: input_a
     dir: in
     shape: scalar
     location:
       a1: Sheet!A1
     schema:
       type: number
 "#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();

    SheetPort::new(&mut workbook, manifest).expect("supported full-v0 subset binds");
}

#[test]
fn symbolic_literal_name_output_is_not_normalized_to_an_address() {
    let manifest: Manifest = Manifest::from_yaml_str(
        r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: literal-name, name: Literal Name }
ports:
  - id: rate
    dir: out
    shape: scalar
    location: { name: Rate }
    schema: { type: number }
"#,
    )
    .unwrap();
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    workbook
        .engine_mut()
        .define_name(
            "Rate",
            NamedDefinition::Literal(LiteralValue::Int(42)),
            NameScope::Workbook,
        )
        .unwrap();
    let mut sheetport = SheetPort::new(&mut workbook, manifest).unwrap();
    let output = sheetport.evaluate_once(EvalOptions::default()).unwrap();
    assert_eq!(
        output.get("rate"),
        Some(&PortValue::Scalar(LiteralValue::Number(42.0)))
    );
}

#[test]
fn full_profile_native_table_output_uses_manifest_column_order() {
    let manifest: Manifest = Manifest::from_yaml_str(
        r#"
spec: fio
spec_version: "0.3.0"
capabilities: { profile: full-v0 }
manifest: { id: native-table, name: Native Table }
ports:
  - id: rows
    dir: out
    shape: table
    location:
      table: { name: Sales, area: body }
    schema:
      kind: table
      columns:
        - { name: Qty, type: number }
        - { name: Price, type: number }
"#,
    )
    .unwrap();
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    for (row, col, value) in [
        (1, 1, LiteralValue::Text("Qty".to_string())),
        (1, 2, LiteralValue::Text("Price".to_string())),
        (2, 1, LiteralValue::Int(3)),
        (2, 2, LiteralValue::Int(7)),
    ] {
        workbook.set_value("Sheet", row, col, value).unwrap();
    }
    let sheet = workbook.engine().sheet_id("Sheet").unwrap();
    workbook
        .engine_mut()
        .define_table(
            "Sales",
            RangeRef::new(
                CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
                CellRef::new(sheet, Coord::from_excel(2, 2, true, true)),
            ),
            true,
            vec!["Qty".to_string(), "Price".to_string()],
            false,
        )
        .unwrap();
    let mut sheetport = SheetPort::new(&mut workbook, manifest).unwrap();
    let output = sheetport.evaluate_once(EvalOptions::default()).unwrap();
    let PortValue::Table(table) = output.get("rows").unwrap() else {
        panic!("expected table output")
    };
    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.rows[0].values["Qty"], LiteralValue::Number(3.0));
    assert_eq!(table.rows[0].values["Price"], LiteralValue::Number(7.0));
}

#[test]
fn full_profile_empty_native_table_body_returns_zero_rows() {
    let manifest: Manifest = Manifest::from_yaml_str(
        r#"
spec: fio
spec_version: "0.3.0"
capabilities: { profile: full-v0 }
manifest: { id: empty-native-table, name: Empty Native Table }
ports:
  - id: rows
    dir: out
    shape: table
    location:
      table: { name: Empty, area: body }
    schema:
      kind: table
      columns:
        - { name: Qty, type: number }
"#,
    )
    .unwrap();
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    workbook
        .set_value("Sheet", 1, 1, LiteralValue::Text("Qty".to_string()))
        .unwrap();
    let sheet = workbook.engine().sheet_id("Sheet").unwrap();
    workbook
        .engine_mut()
        .define_table(
            "Empty",
            RangeRef::new(
                CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
                CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
            ),
            true,
            vec!["Qty".to_string()],
            false,
        )
        .unwrap();
    let mut sheetport = SheetPort::new(&mut workbook, manifest).unwrap();
    let output = sheetport.evaluate_once(EvalOptions::default()).unwrap();
    assert!(
        output
            .get("rows")
            .unwrap()
            .as_table()
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn symbolic_named_range_preserves_range_shape() {
    let manifest: Manifest = Manifest::from_yaml_str(
        r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: named-range, name: Named Range }
ports:
  - id: values
    dir: out
    shape: range
    location: { name: Values }
    schema: { kind: range, cell_type: number }
"#,
    )
    .unwrap();
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    workbook
        .set_value("Sheet", 1, 1, LiteralValue::Int(1))
        .unwrap();
    workbook
        .set_value("Sheet", 2, 1, LiteralValue::Int(2))
        .unwrap();
    let sheet = workbook.engine().sheet_id("Sheet").unwrap();
    workbook
        .engine_mut()
        .define_name(
            "Values",
            NamedDefinition::Range(RangeRef::new(
                CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
                CellRef::new(sheet, Coord::from_excel(2, 1, true, true)),
            )),
            NameScope::Workbook,
        )
        .unwrap();
    let mut sheetport = SheetPort::new(&mut workbook, manifest).unwrap();
    let output = sheetport.evaluate_once(EvalOptions::default()).unwrap();
    assert_eq!(
        output.get("values"),
        Some(&PortValue::Range(vec![
            vec![LiteralValue::Number(1.0)],
            vec![LiteralValue::Number(2.0)],
        ]))
    );
}

#[test]
fn full_v0_unsupported_free_form_selector_fails_typed() {
    let manifest: Manifest = Manifest::from_yaml_str(
        r#"
spec: fio
spec_version: "0.3.0"
capabilities: { profile: full-v0 }
manifest: { id: full-unsupported, name: Full Unsupported }
ports:
  - id: value
    dir: out
    shape: scalar
    location: { struct_ref: "Tbl[Col]" }
    schema: { type: number }
"#,
    )
    .unwrap();
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    assert!(matches!(
        SheetPort::new(&mut workbook, manifest),
        Err(formualizer_sheetport::SheetPortError::UnsupportedSelector { .. })
    ));
}

#[test]
fn sheetport_rejects_struct_ref_under_core_profile() {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: core-structref, name: Core StructRef }
ports:
  - id: input_a
    dir: in
    shape: scalar
    location: { struct_ref: "Tbl[Col]" }
    schema: { type: number }
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();

    let err = match SheetPort::new(&mut workbook, manifest) {
        Ok(_) => panic!("expected invalid manifest"),
        Err(err) => err,
    };
    match err {
        formualizer_sheetport::SheetPortError::InvalidManifest { issues } => {
            assert!(
                issues.iter().any(|issue| issue.path == "ports[0].location"),
                "expected location issue, got {issues:#?}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sheetport_rejects_table_selector_under_core_profile() {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: core-table, name: Core Table }
ports:
  - id: input_t
    dir: in
    shape: table
    location:
      table:
        name: Tbl
    schema:
      kind: table
      columns:
        - { name: a, type: number }
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();

    let err = match SheetPort::new(&mut workbook, manifest) {
        Ok(_) => panic!("expected invalid manifest"),
        Err(err) => err,
    };
    match err {
        formualizer_sheetport::SheetPortError::InvalidManifest { issues } => {
            assert!(
                issues.iter().any(|issue| issue.path == "ports[0].location"),
                "expected location issue, got {issues:#?}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn bounded_layout_exhaustion_is_typed() {
    let manifest: Manifest = Manifest::from_yaml_str(
        r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: bounded-layout, name: Bounded Layout }
ports:
  - id: rows
    dir: out
    shape: range
    location:
      layout:
        sheet: Sheet
        header_row: 1
        anchor_col: A
        terminate: first_blank_row
        max_scan_rows: 2
    schema: { kind: range, cell_type: string }
"#,
    )
    .unwrap();
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();
    workbook
        .set_value("Sheet", 1, 1, LiteralValue::Text("Header".to_string()))
        .unwrap();
    workbook
        .set_value("Sheet", 2, 1, LiteralValue::Text("Value".to_string()))
        .unwrap();
    workbook
        .set_value("Sheet", 3, 1, LiteralValue::Text("Still data".to_string()))
        .unwrap();
    let mut sheetport = SheetPort::new(&mut workbook, manifest).unwrap();
    let error = sheetport.evaluate_once(EvalOptions::default()).unwrap_err();
    assert!(matches!(
        error,
        formualizer_sheetport::SheetPortError::LayoutExhausted {
            limit: 2,
            observed: 2,
            ..
        }
    ));
}

#[test]
fn sheetport_rejects_scalar_layout_selector() {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: core-scalar-layout, name: Core Scalar Layout }
ports:
  - id: input_a
    dir: in
    shape: scalar
    location:
      layout:
        sheet: Sheet
        header_row: 1
        anchor_col: A
        terminate: first_blank_row
    schema: { type: number }
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();

    let err = match SheetPort::new(&mut workbook, manifest) {
        Ok(_) => panic!("expected invalid manifest"),
        Err(err) => err,
    };
    match err {
        formualizer_sheetport::SheetPortError::InvalidManifest { issues } => {
            assert!(
                issues.iter().any(|issue| issue.path == "ports[0].location"),
                "expected location issue, got {issues:#?}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn sheetport_rejects_record_field_struct_ref_under_core_profile() {
    let manifest_yaml = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: core-record-field, name: Core Record Field }
ports:
  - id: rec
    dir: in
    shape: record
    location: { a1: Sheet!A1:B1 }
    schema:
      kind: record
      fields:
        a:
          type: number
          location: { struct_ref: "Tbl[Col]" }
"#;
    let manifest: Manifest = Manifest::from_yaml_str(manifest_yaml).expect("manifest parses");

    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet").unwrap();

    let err = match SheetPort::new(&mut workbook, manifest) {
        Ok(_) => panic!("expected invalid manifest"),
        Err(err) => err,
    };
    match err {
        formualizer_sheetport::SheetPortError::InvalidManifest { issues } => {
            assert!(
                issues
                    .iter()
                    .any(|issue| issue.path == "ports[0].schema.fields.a.location"),
                "expected field location issue, got {issues:#?}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
