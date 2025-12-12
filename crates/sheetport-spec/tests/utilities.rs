use serde_json::json;
use sheetport_spec::{Manifest, Schema, load_manifest_from_str, manifest_to_yaml};

#[test]
fn load_helpers_match_methods() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: util-test, name: Util Test }
ports: []
"#;
    let manifest_method = Manifest::from_yaml_str(yaml).unwrap();
    let manifest_fn = load_manifest_from_str(yaml).unwrap();
    assert_eq!(
        manifest_method.to_yaml().unwrap(),
        manifest_fn.to_yaml().unwrap()
    );
    assert_eq!(
        manifest_to_yaml(&manifest_method).unwrap(),
        manifest_method.to_yaml().unwrap()
    );
}

#[test]
fn normalize_orders_ports_and_tags() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest:
  id: normalize-test
  name: Normalize Test
  tags: ["supply", "finance", "supply"]
ports:
  - id: beta_port
    dir: in
    shape: table
    location:
      layout: { sheet: Inv, header_row: 1, anchor_col: A, terminate: first_blank_row }
    schema:
      kind: table
      columns:
        - { name: sku, type: string }
        - { name: warehouse, type: string }
      keys: ["warehouse", "sku", "sku"]
  - id: alpha_port
    dir: in
    shape: record
    location:
      a1: Inputs!A1:B1
    constraints:
      enum: ["Z", "A", "A"]
    schema:
      kind: record
      fields:
        month:
          type: integer
          location: { a1: Inputs!A1 }
          constraints:
            enum: [3, 1, 1, 2]
        region:
          type: string
          location: { a1: Inputs!B1 }
"#;

    let mut manifest = Manifest::from_yaml_str(yaml).unwrap();
    manifest.normalize();

    let tags = manifest.manifest.tags.unwrap();
    assert_eq!(tags, vec!["finance", "supply"]);

    let ids: Vec<_> = manifest.ports.iter().map(|p| p.id.as_str()).collect();
    assert_eq!(ids, vec!["alpha_port", "beta_port"]);

    if let Some(constraints) = &manifest.ports[0].constraints {
        assert_eq!(
            constraints.r#enum.as_ref().unwrap(),
            &vec![json!("A"), json!("Z")]
        );
    } else {
        panic!("expected constraints on alpha_port");
    }

    if let Schema::Record(record) = &manifest.ports[0].schema {
        let month = record.fields.get("month").unwrap();
        assert_eq!(
            month.constraints.as_ref().unwrap().r#enum.as_ref().unwrap(),
            &vec![json!(1), json!(2), json!(3)]
        );
    } else {
        panic!("alpha_port should be record");
    }

    if let Schema::Table(table) = &manifest.ports[1].schema {
        assert_eq!(
            table.keys.as_ref().unwrap(),
            &vec!["sku".to_string(), "warehouse".to_string()]
        );
    } else {
        panic!("beta_port should be table");
    }
}

#[test]
fn normalized_produces_sorted_copy() {
    let yaml = r#"spec: fio
spec_version: "0.3.0"
manifest: { id: norm-copy, name: Norm Copy }
ports:
  - id: b
    dir: out
    shape: scalar
    location: { a1: Outputs!B2 }
    schema: { type: string }
  - id: a
    dir: in
    shape: scalar
    location: { a1: Inputs!B2 }
    schema: { type: string }
"#;
    let manifest = Manifest::from_yaml_str(yaml).unwrap();
    let mut clone = manifest.clone();
    clone.normalize();
    let normalized = manifest.normalized();
    assert_eq!(clone.to_yaml().unwrap(), normalized.to_yaml().unwrap());
}

#[test]
fn crate_version_matches_spec_version() {
    assert_eq!(
        sheetport_spec::CURRENT_SPEC_VERSION,
        sheetport_spec::CRATE_VERSION
    );
}
