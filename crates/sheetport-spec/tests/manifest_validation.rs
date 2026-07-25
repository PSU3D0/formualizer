use sheetport_spec::{Constraints, Manifest, schema_json};

fn load_fixture(name: &str) -> Manifest {
    let path = format!("tests/fixtures/{}.yaml", name);
    let text = std::fs::read_to_string(path).expect("failed to read fixture");
    serde_yaml::from_str::<Manifest>(&text).expect("fixture should deserialize")
}

#[test]
fn supply_planning_fixture_validates() {
    let manifest = load_fixture("supply_planning");
    manifest.validate().expect("fixture should validate");
}

#[test]
fn pricing_simple_fixture_validates() {
    let manifest = load_fixture("pricing_simple");
    manifest.validate().expect("fixture should validate");
}

#[test]
fn inventory_until_marker_fixture_validates() {
    let manifest = load_fixture("inventory_until_marker");
    manifest.validate().expect("fixture should validate");
}

#[test]
fn report_output_fixture_validates() {
    let manifest = load_fixture("report_output");
    manifest.validate().expect("fixture should validate");
}

#[test]
fn out_port_default_rejected() {
    let mut manifest = load_fixture("supply_planning");
    let port = manifest
        .ports
        .iter_mut()
        .find(|p| p.dir == sheetport_spec::Direction::Out)
        .expect("expected at least one out port");
    port.default = Some(serde_json::json!({"bad": "value"}));

    let err = manifest.validate().expect_err("validation should fail");
    insta::assert_yaml_snapshot!("out_port_default_error", err.issues());
}

#[test]
fn schema_json_is_well_formed() {
    let schema_str = schema_json();
    let value: serde_json::Value =
        serde_json::from_str(schema_str).expect("schema must be valid JSON");
    assert!(value.is_object(), "schema root should be an object");
}

#[test]
fn bundled_schema_matches_generated() {
    let committed: serde_json::Value =
        serde_json::from_str(schema_json()).expect("schema must be valid JSON");
    let generated = sheetport_spec::generate_schema_value();
    if generated != committed {
        println!(
            "Generated schema:\n{}",
            sheetport_spec::generate_schema_json_pretty()
        );
    }
    assert_eq!(
        generated, committed,
        "bundled JSON schema is out of sync with generated definition"
    );
}

#[test]
fn constraint_min_greater_than_max_fails() {
    let mut manifest = load_fixture("supply_planning");
    let (idx, port) = manifest
        .ports
        .iter_mut()
        .enumerate()
        .find(|(_, port)| port.id == "warehouse_code")
        .expect("warehouse_code port present");
    let mut constraints = port.constraints.clone().unwrap_or(Constraints {
        min: None,
        max: None,
        r#enum: None,
        pattern: None,
        nullable: None,
    });
    constraints.min = Some(10.0);
    constraints.max = Some(5.0);
    port.constraints = Some(constraints);

    let err = manifest.validate().expect_err("validation should fail");
    let path = format!("ports[{}].constraints.min", idx);
    assert!(
        err.issues().iter().any(|issue| issue.path == path),
        "expected min/max issue at {path}, got {:#?}",
        err.issues()
    );
}

#[test]
fn constraint_enum_must_not_be_empty() {
    let mut manifest = load_fixture("supply_planning");
    let (idx, port) = manifest
        .ports
        .iter_mut()
        .enumerate()
        .find(|(_, port)| port.id == "warehouse_code")
        .expect("warehouse_code port present");
    let mut constraints = port.constraints.clone().unwrap_or(Constraints {
        min: None,
        max: None,
        r#enum: None,
        pattern: None,
        nullable: None,
    });
    constraints.r#enum = Some(Vec::new());
    port.constraints = Some(constraints);

    let err = manifest.validate().expect_err("validation should fail");
    let path = format!("ports[{}].constraints.enum", idx);
    assert!(
        err.issues().iter().any(|issue| issue.path == path),
        "expected enum issue at {path}, got {:#?}",
        err.issues()
    );
}

#[test]
fn constraint_pattern_must_compile() {
    let mut manifest = load_fixture("supply_planning");
    let (idx, port) = manifest
        .ports
        .iter_mut()
        .enumerate()
        .find(|(_, port)| port.id == "warehouse_code")
        .expect("warehouse_code port present");
    let mut constraints = port.constraints.clone().unwrap_or(Constraints {
        min: None,
        max: None,
        r#enum: None,
        pattern: None,
        nullable: None,
    });
    constraints.pattern = Some("[".to_string());
    port.constraints = Some(constraints);

    let err = manifest.validate().expect_err("validation should fail");
    let path = format!("ports[{}].constraints.pattern", idx);
    assert!(
        err.issues().iter().any(|issue| issue.path == path),
        "expected pattern issue at {path}, got {:#?}",
        err.issues()
    );
}

#[test]
fn record_field_constraints_validated() {
    let mut manifest = load_fixture("supply_planning");
    let (idx, port) = manifest
        .ports
        .iter_mut()
        .enumerate()
        .find(|(_, port)| port.id == "planning_window")
        .expect("planning_window port present");
    if let sheetport_spec::Schema::Record(record) = &mut port.schema
        && let Some(field) = record.fields.get_mut("month")
    {
        let mut constraints = field.constraints.clone().unwrap_or(Constraints {
            min: None,
            max: None,
            r#enum: None,
            pattern: None,
            nullable: None,
        });
        constraints.min = Some(20.0);
        constraints.max = Some(10.0);
        field.constraints = Some(constraints);
    }

    let err = manifest.validate().expect_err("validation should fail");
    let path = format!("ports[{}].schema.fields.month.constraints.min", idx);
    assert!(
        err.issues().iter().any(|issue| issue.path == path),
        "expected record field constraint issue at {path}, got {:#?}",
        err.issues()
    );
}

#[test]
fn capabilities_default_to_core_profile() {
    let manifest = load_fixture("supply_planning");
    assert_eq!(
        manifest.effective_profile(),
        sheetport_spec::Profile::CoreV0
    );
}

#[test]
fn capabilities_explicit_core_profile_parses() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
capabilities: { profile: core-v0 }
ports: []
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    assert_eq!(
        manifest.effective_profile(),
        sheetport_spec::Profile::CoreV0
    );
}

#[test]
fn scalar_layout_selector_rejected_under_core() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
ports:
  - id: x
    dir: in
    shape: scalar
    location:
      layout:
        sheet: Sheet1
        header_row: 1
        anchor_col: A
        terminate: first_blank_row
    schema: { type: number }
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let err = manifest.validate().expect_err("validation should fail");
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].location"),
        "expected location issue, got {:#?}",
        err.issues()
    );
}

#[test]
fn record_table_selector_rejected_under_core() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
ports:
  - id: r
    dir: in
    shape: record
    location:
      table:
        name: Tbl
    schema:
      kind: record
      fields:
        a: { type: number, location: { a1: Sheet1!A1 } }
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let err = manifest.validate().expect_err("validation should fail");
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].location"),
        "expected location issue, got {:#?}",
        err.issues()
    );
}

#[test]
fn struct_ref_selector_rejected_under_core() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
ports:
  - id: s
    dir: in
    shape: scalar
    location: { struct_ref: "Tbl[Col]" }
    schema: { type: number }
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let err = manifest.validate().expect_err("validation should fail");
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].location"),
        "expected location issue, got {:#?}",
        err.issues()
    );
}

#[test]
fn table_shape_rejects_a1_selector_even_under_full() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
capabilities: { profile: full-v0 }
ports:
  - id: t
    dir: in
    shape: table
    location: { a1: Sheet1!A1:B2 }
    schema:
      kind: table
      columns:
        - { name: a, type: number }
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let err = manifest.validate().expect_err("validation should fail");
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].location"),
        "expected location issue, got {:#?}",
        err.issues()
    );
}

#[test]
fn full_profile_allows_struct_ref_for_scalar() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
capabilities: { profile: full-v0 }
ports:
  - id: s
    dir: in
    shape: scalar
    location: { struct_ref: "Tbl[Col]" }
    schema: { type: number }
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    manifest
        .validate()
        .expect("full profile allows struct_ref scalar");
}

#[test]
fn full_profile_allows_table_selector_for_table() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
capabilities: { profile: full-v0 }
ports:
  - id: t
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
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    manifest
        .validate()
        .expect("full profile allows table selector");
}

#[test]
fn layout_kind_defaults_when_omitted() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
ports:
  - id: t
    dir: in
    shape: table
    location:
      layout:
        sheet: Sheet1
        header_row: 1
        anchor_col: A
        terminate: first_blank_row
    schema:
      kind: table
      columns:
        - { name: a, type: number }
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let port = &manifest.ports[0];
    match &port.location {
        sheetport_spec::Selector::Layout(selector) => {
            assert_eq!(
                selector.layout.kind,
                sheetport_spec::LayoutKind::HeaderContiguousV1
            );
        }
        other => panic!("expected layout selector, got {other:?}"),
    }
}

#[test]
fn enum_values_must_match_value_type_for_scalar() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
ports:
  - id: x
    dir: in
    shape: scalar
    location: { a1: Sheet1!A1 }
    schema: { type: integer }
    constraints:
      enum: ["bad", 1]
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let err = manifest.validate().expect_err("validation should fail");
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].constraints.enum[0]"),
        "expected enum typing issue, got {:#?}",
        err.issues()
    );
}

#[test]
fn min_max_constraints_require_numeric_type() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
ports:
  - id: s
    dir: in
    shape: scalar
    location: { a1: Sheet1!A1 }
    schema: { type: string }
    constraints:
      min: 1
      max: 5
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let err = manifest.validate().expect_err("validation should fail");
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].constraints.min"),
        "expected min typing issue, got {:#?}",
        err.issues()
    );
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].constraints.max"),
        "expected max typing issue, got {:#?}",
        err.issues()
    );
}

#[test]
fn date_enum_values_must_be_valid_dates() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: sample, name: Sample }
ports:
  - id: d
    dir: in
    shape: scalar
    location: { a1: Sheet1!A1 }
    schema: { type: date }
    constraints:
      enum: ["2025-01-01", "not-a-date"]
"#;
    let manifest: Manifest = serde_yaml::from_str(yaml).expect("manifest parses");
    let err = manifest.validate().expect_err("validation should fail");
    assert!(
        err.issues()
            .iter()
            .any(|issue| issue.path == "ports[0].constraints.enum[1]"),
        "expected date enum issue, got {:#?}",
        err.issues()
    );
}
