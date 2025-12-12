use std::io::Write;

use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use tempfile::NamedTempFile;

#[test]
fn cli_reports_success() {
    let mut cmd = cargo_bin_cmd!("fio-lint");
    cmd.arg("tests/fixtures/supply_planning.yaml")
        .assert()
        .success()
        .stdout(predicate::str::contains("OK:"));
}

#[test]
fn cli_normalize_outputs_yaml() {
    let mut cmd = cargo_bin_cmd!("fio-lint");
    cmd.arg("tests/fixtures/supply_planning.yaml")
        .arg("--normalize")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("spec: fio"));
}

#[test]
fn cli_reports_validation_errors() {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "spec: not-fio
spec_version: \"0.3.0\"
manifest: {{ id: bad, name: Bad }}
ports: []
"
    )
    .unwrap();

    let mut cmd = cargo_bin_cmd!("fio-lint");
    cmd.arg(file.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("Manifest validation failed"))
        .stderr(predicate::str::contains("expected spec identifier"));
}
