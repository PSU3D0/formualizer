use formualizer_common::LiteralValue;
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
fn sheetport_rejects_unsupported_profile() {
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

    let err = match SheetPort::new(&mut workbook, manifest) {
        Ok(_) => panic!("expected unsupported profile error"),
        Err(err) => err,
    };
    match err {
        formualizer_sheetport::SheetPortError::InvalidManifest { issues } => {
            assert!(
                issues
                    .iter()
                    .any(|issue| issue.path == "capabilities.profile"),
                "expected profile issue, got {issues:#?}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
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
