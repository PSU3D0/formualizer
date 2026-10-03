use formualizer_cli::{CancelToken, run};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
    path::Path,
    process::Command,
};
const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
// Minimal source-XML package, adapted from workbook/tests/support/source_xlsx.rs.
fn fixture(rows: &str, tail: &str) -> Vec<u8> {
    let parts: BTreeMap<_, _> = [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        ("xl/worksheets/sheet1.xml", format!("<worksheet xmlns=\"{MAIN}\"><dimension ref=\"A1:B5\"/><sheetData>{rows}</sheetData>{tail}</worksheet>")),
    ].into_iter().collect();
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, value) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(value.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}
fn simple(cache: usize) -> Vec<u8> {
    fixture(
        &format!("<row r=\"1\"><c r=\"A1\"><f>1+1</f><v>{cache}</v></c></row>"),
        "",
    )
}
fn invoke(args: &[&str], cancel: Option<CancelToken>) -> (i32, String, String) {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = run(
        std::iter::once("formualizer").chain(args.iter().copied()),
        &mut out,
        &mut err,
        cancel,
    );
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}
fn json(path: &Path, extra: &[&str]) -> (i32, Value) {
    let mut args = vec!["recalc", path.to_str().unwrap(), "--json"];
    args.extend(extra);
    let (code, out, err) = invoke(&args, None);
    assert!(err.is_empty(), "{err}");
    assert_eq!(out.lines().count(), 1);
    (code, serde_json::from_str(&out).unwrap())
}
#[test]
fn fixed_cse_recalculates_with_exit_zero_and_no_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cse.xlsx");
    std::fs::write(&path, fixture("<row r=\"1\"><c r=\"A1\"><f t=\"array\" ref=\"A1:A3\">SEQUENCE(2)</f><v>99</v></c><c r=\"B1\"><f>SUM(A1:A3)</f><v>99</v></c></row>", "")).unwrap();
    let (code, report) = json(&path, &[]);
    assert_eq!(code, 0);
    assert_eq!(report["status"], "written");
    let output = std::fs::read(&path).unwrap();
    let mut archive = zip::ZipArchive::new(Cursor::new(&output)).unwrap();
    assert!(archive.by_name("xl/metadata.xml").is_err());
    let mut xml = String::new();
    archive
        .by_name("xl/worksheets/sheet1.xml")
        .unwrap()
        .read_to_string(&mut xml)
        .unwrap();
    assert!(xml.contains("ref=\"A1:A3\""));
    assert!(xml.contains("#N/A"));
    assert!(!xml.contains(" cm="));
    assert_eq!(json(&path, &[]).0, 0);
    assert_eq!(std::fs::read(&path).unwrap(), output);
}

#[test]
fn written_and_unchanged_preserve_bytes_and_mtime() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("book.xlsx");
    std::fs::write(&path, simple(99)).unwrap();
    let (code, report) = json(&path, &[]);
    assert_eq!(code, 0);
    assert_eq!(report["status"], "written");
    assert_eq!(report["written"], true);
    let bytes = std::fs::read(&path).unwrap();
    let time = std::fs::metadata(&path).unwrap().modified().unwrap();
    let (code, report) = json(&path, &[]);
    assert_eq!(code, 0);
    assert_eq!(report["status"], "unchanged");
    assert_eq!(report["written"], false);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), time);
}
#[test]
fn explicit_output_always_written_input_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    let output = dir.path().join("out.xlsx");
    for cache in [99, 2] {
        let bytes = simple(cache);
        std::fs::write(&input, &bytes).unwrap();
        let time = std::fs::metadata(&input).unwrap().modified().unwrap();
        let (code, report) = json(&input, &["-o", output.to_str().unwrap()]);
        assert_eq!(code, 0);
        assert_eq!(report["status"], "written");
        assert_eq!(std::fs::read(&input).unwrap(), bytes);
        assert_eq!(std::fs::metadata(&input).unwrap().modified().unwrap(), time);
        assert!(output.exists());
    }
}
#[test]
fn check_current_and_stale_never_write_even_with_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    let out = dir.path().join("out.xlsx");
    for (cache, code, status) in [(2, 0, "current"), (99, 3, "stale")] {
        let bytes = simple(cache);
        std::fs::write(&path, &bytes).unwrap();
        let time = std::fs::metadata(&path).unwrap().modified().unwrap();
        let (actual, report) = json(&path, &["--check", "-o", out.to_str().unwrap()]);
        assert_eq!(actual, code);
        assert_eq!(report["status"], status);
        assert_eq!(report["output"], Value::Null);
        assert_eq!(report["written"], false);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), time);
        assert!(!out.exists());
    }
}
#[test]
fn refusals_are_structured_and_do_not_publish() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    for bytes in [
        fixture(
            "<row r=\"1\"><c r=\"A1\"><f>1+1</f><v>99</v></c></row>",
            "<tableParts count=\"1\"/>",
        ),
        fixture(
            "<row r=\"1\"><c r=\"A1\"><f t=\"dataTable\" ref=\"A1:A2\">1+1</f><v>99</v></c></row>",
            "",
        ),
    ] {
        std::fs::write(&path, &bytes).unwrap();
        let (code, report) = json(&path, &[]);
        assert_eq!(code, 2, "{report}");
        assert_eq!(report["status"], "refused");
        assert!(report["refusal"]["feature"].is_string());
        assert!(report["refusal"]["context"].is_string());
        assert_eq!(report["written"], false);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}
#[test]
fn invalid_and_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    std::fs::write(&path, "not a zip").unwrap();
    for extra in [vec![], vec!["--check"]] {
        let (code, report) = json(&path, &extra);
        assert_eq!(code, 1);
        assert_eq!(report["status"], "error");
        assert_eq!(report["refusal"], Value::Null);
    }
    for args in [
        vec!["recalc"],
        vec!["recalc", "--json"],
        vec!["recalc", "x", "--max-errors", "-1", "--json"],
        vec!["recalc", "x", "--wat", "--json"],
    ] {
        let (code, out, err) = invoke(&args, None);
        assert_eq!(code, 64);
        if args.contains(&"--json") {
            let report: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(report["status"], "error");
            assert!(err.is_empty());
        } else {
            assert!(out.is_empty());
            assert!(err.contains("\nUsage:"));
            assert!(err.contains("\n\nFor more information"));
        }
    }
}
#[test]
fn errors_listed_and_globally_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    std::fs::write(&path, fixture("<row r=\"1\"><c r=\"A1\"><f>1/0</f><v>99</v></c><c r=\"B1\"><f>NA()</f><v>99</v></c></row>", "")).unwrap();
    for limit in ["20", "1", "0"] {
        let (code, report) = json(&path, &["--check", "--max-errors", limit]);
        assert_eq!(code, 3);
        assert_eq!(report["error_cells"], 2);
        let errors = report["errors"].as_array().unwrap();
        assert_eq!(errors.len(), limit.parse::<usize>().unwrap().min(2));
        assert_eq!(report["errors_truncated"], errors.len() < 2);
        for error in errors {
            assert_eq!(error["sheet"], "Sheet1");
            assert!(["A1", "B1"].contains(&error["cell"].as_str().unwrap()));
            assert!(["#DIV/0!", "#N/A"].contains(&error["error"].as_str().unwrap()));
        }
    }
}
#[test]
fn dynamic_spill_written_and_then_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    std::fs::write(&path, fixture("<row r=\"1\"><c r=\"A1\"><f>_xlfn.SEQUENCE(3)</f><v>99</v></c><c r=\"B1\"><f>SUM(A1#)</f><v>99</v></c></row>", "")).unwrap();
    let (code, report) = json(&path, &[]);
    assert_eq!(code, 0, "{report}");
    assert_eq!(report["formula_cells"], 2);
    assert_eq!(report["cache_cells_changed"], 4);
    let bytes = std::fs::read(&path).unwrap();
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut sheet = String::new();
    zip.by_name("xl/worksheets/sheet1.xml")
        .unwrap()
        .read_to_string(&mut sheet)
        .unwrap();
    assert!(sheet.contains("ref=\"A1:A3\""));
    assert!(sheet.contains("<v>6</v>"));
    assert!(zip.by_name("xl/metadata.xml").is_ok());
    assert_eq!(json(&path, &[]).1["status"], "unchanged");
}
/// The worksheet XML of the package at `path`.
fn sheet_xml(path: &Path) -> String {
    let mut zip = zip::ZipArchive::new(Cursor::new(std::fs::read(path).unwrap())).unwrap();
    let mut sheet = String::new();
    zip.by_name("xl/worksheets/sheet1.xml")
        .unwrap()
        .read_to_string(&mut sheet)
        .unwrap();
    sheet
}
/// The `<c r="cell" ...>...</c>` element of `cell` in `sheet`.
fn cell_xml<'a>(sheet: &'a str, cell: &str) -> &'a str {
    let start = sheet.find(&format!("<c r=\"{cell}\"")).expect(cell);
    let end = start + sheet[start..].find("</c>").expect(cell) + 4;
    &sheet[start..end]
}
#[test]
fn spill_blocked_by_a_formula_or_another_spill_is_written_as_spill_error() {
    let dir = tempfile::tempdir().unwrap();
    let by_value = dir.path().join("value.xlsx");
    std::fs::write(&by_value, fixture("<row r=\"1\"><c r=\"A1\"><f>_xlfn.SEQUENCE(3)</f><v>99</v></c></row><row r=\"3\"><c r=\"A3\"><v>99</v></c></row>", "")).unwrap();
    let (code, report) = json(&by_value, &[]);
    assert_eq!(code, 0, "{report}");
    let value_sheet = sheet_xml(&by_value);
    for (name, blocker, blocker_cache) in [
        (
            "formula",
            "<row r=\"3\"><c r=\"A3\"><f>1+1</f><v>0</v></c></row>",
            ("A3", "<v>2</v>"),
        ),
        (
            "spill",
            "<row r=\"2\"><c r=\"A2\"><f>_xlfn.SEQUENCE(2)</f><v>0</v></c></row>",
            ("A3", "<v>2</v>"),
        ),
    ] {
        let path = dir.path().join(format!("{name}.xlsx"));
        std::fs::write(&path, fixture(&format!("<row r=\"1\"><c r=\"A1\"><f>_xlfn.SEQUENCE(3)</f><v>99</v></c><c r=\"B1\"><f>A1+0</f><v>99</v></c></row>{blocker}"), "")).unwrap();
        // The binary passes a cancellation token, which takes the
        // cancellable evaluation path.
        let args = ["recalc", path.to_str().unwrap(), "--json"];
        let (code, out, err) = invoke(&args, Some(CancelToken::new()));
        assert!(err.is_empty(), "{name}: {err}");
        let report: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(code, 0, "{name}: {report}");
        assert_eq!(report["status"], "written", "{name}");
        let errors = report["errors"].as_array().unwrap();
        assert!(
            errors
                .iter()
                .any(|e| e["cell"] == "A1" && e["error"] == "#SPILL!"),
            "{name}: {report}"
        );
        assert!(
            errors
                .iter()
                .any(|e| e["cell"] == "B1" && e["error"] == "#SPILL!"),
            "{name}: dependent sees #SPILL!: {report}"
        );
        let sheet = sheet_xml(&path);
        assert_eq!(
            cell_xml(&sheet, "A1"),
            cell_xml(&value_sheet, "A1"),
            "{name}: encoding"
        );
        assert!(
            cell_xml(&sheet, blocker_cache.0).contains(blocker_cache.1),
            "{name}: {sheet}"
        );
        assert_eq!(json(&path, &[]).1["status"], "unchanged", "{name}");
    }
}
#[test]
fn schema_field_set_pinned_for_success_and_usage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    std::fs::write(&path, simple(2)).unwrap();
    let (_, usage, _) = invoke(&["recalc", "--json"], None);
    for report in [json(&path, &[]).1, serde_json::from_str(&usage).unwrap()] {
        let mut keys: Vec<_> = report
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        let mut expected = vec![
            "schema",
            "status",
            "input",
            "output",
            "written",
            "formula_cells",
            "cache_cells_changed",
            "worksheet_parts_changed",
            "evaluated",
            "error_cells",
            "errors",
            "errors_truncated",
            "refusal",
            "message",
        ];
        expected.sort_unstable();
        assert_eq!(keys, expected);
        assert_eq!(report["schema"], "formualizer.recalc/1");
    }
}
#[test]
fn pre_cancelled_is_interrupted_and_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    let bytes = simple(99);
    std::fs::write(&path, &bytes).unwrap();
    let token = CancelToken::new();
    token.cancel();
    let (code, out, err) = invoke(&["recalc", path.to_str().unwrap(), "--json"], Some(token));
    assert_eq!(code, 130);
    assert!(err.is_empty());
    let report: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["status"], "interrupted");
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}
#[cfg(unix)]
#[test]
fn symlink_destination_refused_and_permissions_preserved() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    let target = dir.path().join("target.xlsx");
    let link = dir.path().join("link.xlsx");
    let bytes = simple(99);
    std::fs::write(&path, &bytes).unwrap();
    std::fs::write(&target, b"sentinel").unwrap();
    symlink(&target, &link).unwrap();
    assert_eq!(json(&path, &["-o", link.to_str().unwrap()]).0, 2);
    assert_eq!(std::fs::read(&target).unwrap(), b"sentinel");
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    assert_eq!(json(&path, &[]).0, 0);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o640
    );
    for cache in [99, 2] {
        let bytes = simple(cache);
        std::fs::write(&target, &bytes).unwrap();
        let (code, report) = json(&link, &[]);
        assert_eq!(code, 2);
        assert_eq!(report["refusal"]["feature"], "symlink destination");
        assert_eq!(std::fs::read(&target).unwrap(), bytes);
    }
}
#[test]
fn built_binary_version_help_recalc_and_usage() {
    for args in [
        vec!["recalc", "--json", "--help"],
        vec!["--version", "--json"],
    ] {
        let (code, out, err) = invoke(&args, None);
        assert_eq!(code, 0);
        assert!(err.is_empty());
        assert!(!out.starts_with('{'));
        assert!(!out.is_empty());
    }
    let bin = env!("CARGO_BIN_EXE_formualizer");
    let version = Command::new(bin).arg("--version").output().unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap(),
        format!("formualizer {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(
        Command::new(bin)
            .args(["help", "recalc"])
            .output()
            .unwrap()
            .status
            .success()
    );
    let usage = Command::new(bin)
        .args(["recalc", "--json"])
        .output()
        .unwrap();
    assert_eq!(usage.status.code(), Some(64));
    assert!(serde_json::from_slice::<Value>(&usage.stdout).is_ok());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.xlsx");
    std::fs::write(&path, simple(99)).unwrap();
    let recalc = Command::new(bin)
        .args(["recalc", path.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    assert_eq!(recalc.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<Value>(&recalc.stdout).unwrap()["status"],
        "written"
    );
}
