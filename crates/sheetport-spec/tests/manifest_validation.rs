use sheetport_spec::{Manifest, schema_json};

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
