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
    if let sheetport_spec::Schema::Record(record) = &mut port.schema {
        if let Some(field) = record.fields.get_mut("month") {
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
    }

    let err = manifest.validate().expect_err("validation should fail");
    let path = format!("ports[{}].schema.fields.month.constraints.min", idx);
    assert!(
        err.issues().iter().any(|issue| issue.path == path),
        "expected record field constraint issue at {path}, got {:#?}",
        err.issues()
    );
}
