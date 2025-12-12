//! SheetPort manifest specification types and helpers.
//!
//! This crate defines the canonical data model for Formualizer I/O (FIO) manifests,
//! provides serde serialization/deserialization, JSON Schema metadata, and
//! validation helpers for authoring manifests that treat spreadsheets as pure
//! input/output functions.

mod manifest;
mod validation;

pub use manifest::*;
pub use validation::{ManifestIssue, ValidationError};

/// Version of this reference implementation crate.
///
/// This is expected to track the manifest `spec_version` for the supported spec.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the canonical JSON Schema (Draft 2019-09) for the current manifest version.
pub fn schema_json() -> &'static str {
    include_str!("../schema/fio-0.3.json")
}

/// Generate the JSON Schema for [`Manifest`] using schemars at runtime.
///
/// This is primarily used for verifying that the bundled schema file is up to date.
pub fn generate_schema_json_pretty() -> String {
    use schemars::generate::SchemaSettings;

    let settings = SchemaSettings::draft2019_09();
    let generator = settings.into_generator();
    let root = generator.into_root_schema_for::<Manifest>();
    serde_json::to_string_pretty(&root).expect("RootSchema should serialize to JSON")
}

/// Generate the JSON Schema as a `serde_json::Value`.
pub fn generate_schema_value() -> serde_json::Value {
    serde_json::from_str(&generate_schema_json_pretty()).expect("RootSchema JSON should be valid")
}

/// Load a manifest from any reader implementing [`std::io::Read`].
///
/// # Examples
/// ```
/// # use sheetport_spec::Manifest;
/// # let yaml = br#"spec: fio
/// # spec_version: "0.3.0"
/// # manifest: { id: sample, name: Sample }
/// # ports: []
/// # "#;
/// let manifest = Manifest::from_yaml_reader(&yaml[..]).unwrap();
/// assert_eq!(manifest.manifest.name, "Sample");
/// ```
pub fn load_manifest_from_reader<R: std::io::Read>(
    reader: R,
) -> Result<Manifest, serde_yaml::Error> {
    Manifest::from_yaml_reader(reader)
}

/// Load a manifest from a YAML string slice.
pub fn load_manifest_from_str(yaml: &str) -> Result<Manifest, serde_yaml::Error> {
    Manifest::from_yaml_str(yaml)
}

/// Serialize a manifest back to YAML, in a stable order.
pub fn manifest_to_yaml(manifest: &Manifest) -> Result<String, serde_yaml::Error> {
    manifest.to_yaml()
}
