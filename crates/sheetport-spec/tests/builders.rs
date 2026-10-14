use sheetport_spec::{Constraints, Manifest, Port, Schema, Selector, TableColumn, ValueType};

#[test]
fn fixture_json_matches_yaml() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for entry in std::fs::read_dir(fixtures).unwrap() {
        let path = entry.unwrap().path();
        if path
            .extension()
            .is_some_and(|extension| extension == "yaml")
        {
            let yaml: serde_json::Value =
                serde_yaml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let json: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(path.with_extension("json")).unwrap(),
            )
            .unwrap();
            assert_eq!(yaml, json, "{}", path.display());
        }
    }
}

#[test]
fn helpers_roundtrip_and_match_yaml() {
    let manifest = Manifest::new("loan-calculator")
        .port(
            Port::input("principal", "Calculator!B1", ValueType::Number).with_constraints(
                Constraints {
                    min: Some(0.0),
                    ..Default::default()
                },
            ),
        )
        .port(Port::output("payment", "Calculator!B4", ValueType::Number));
    manifest.validate().unwrap();
    let yaml = manifest.to_yaml().unwrap();
    let decoded = Manifest::from_yaml_str(&yaml).unwrap();
    assert_eq!(
        serde_json::to_value(&manifest).unwrap(),
        serde_json::to_value(decoded).unwrap()
    );
    let yaml = "spec: fio\nspec_version: 0.3.0\nmanifest: {id: loan-calculator, name: loan-calculator}\nports:\n- id: principal\n  dir: in\n  shape: scalar\n  location: {a1: 'Calculator!B1'}\n  schema: {type: number}\n  constraints: {min: 0}\n- id: payment\n  dir: out\n  shape: scalar\n  location: {a1: 'Calculator!B4'}\n  schema: {type: number}\n";
    assert_eq!(
        serde_json::to_value(&manifest).unwrap(),
        serde_json::to_value(Manifest::from_yaml_str(yaml).unwrap()).unwrap()
    );
}

#[test]
fn schemas_infer_shapes_and_json_uses_same_validation() {
    let manifest = Manifest::new("bad id").ports([
        Port::input(
            "rows",
            Selector::a1("Data!A1:B3"),
            Schema::table([TableColumn::new("sku", ValueType::String)]),
        ),
        Port::output("range", "Data!A1:A3", Schema::range(ValueType::Number)),
    ]);
    let yaml = Manifest::from_yaml_str(&manifest.to_yaml().unwrap()).unwrap();
    let json = Manifest::from_json_value(serde_json::to_value(&manifest).unwrap()).unwrap();
    let paths = |m: &Manifest| {
        m.validate()
            .unwrap_err()
            .issues()
            .iter()
            .map(|i| i.path.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(paths(&manifest), paths(&yaml));
    assert_eq!(paths(&manifest), paths(&json));
}
