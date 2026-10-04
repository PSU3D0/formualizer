//! `--now`, `--tz` and `--seed`, the JSON clock/seed echo, and help text.
use chrono::{NaiveDate, NaiveDateTime};
use formualizer_cli::run;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
    path::Path,
};
const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const DEFAULT_SEED: u64 = 0xF0F0_D0D0_AAAA_5555;

/// A one-sheet package: A1 `TODAY()`, A2 `NOW()`, A3 `RAND()`,
/// A4 `RANDBETWEEN(1,1000000000)`, all with stale caches.
fn volatile_book() -> Vec<u8> {
    let rows = "<row r=\"1\"><c r=\"A1\"><f>TODAY()</f><v>0</v></c></row>\
        <row r=\"2\"><c r=\"A2\"><f>NOW()</f><v>0</v></c></row>\
        <row r=\"3\"><c r=\"A3\"><f>RAND()</f><v>0</v></c></row>\
        <row r=\"4\"><c r=\"A4\"><f>RANDBETWEEN(1,1000000000)</f><v>0</v></c></row>";
    let parts: BTreeMap<_, _> = [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        ("xl/worksheets/sheet1.xml", format!("<worksheet xmlns=\"{MAIN}\"><dimension ref=\"A1:A4\"/><sheetData>{rows}</sheetData></worksheet>")),
    ].into_iter().collect();
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, value) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(value.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}
fn invoke(args: &[&str]) -> (i32, String, String) {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = run(
        std::iter::once("formualizer").chain(args.iter().copied()),
        &mut out,
        &mut err,
        None,
    );
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}
/// `recalc <input> --json <extra...>`: exit code and the single JSON report.
fn recalc(input: &Path, extra: &[&str]) -> (i32, Value) {
    let mut args = vec!["recalc", input.to_str().unwrap(), "--json"];
    args.extend(extra);
    let (code, out, err) = invoke(&args);
    assert!(err.is_empty(), "{err}");
    assert_eq!(out.lines().count(), 1, "{out}");
    (code, serde_json::from_str(&out).unwrap())
}
/// Cached numeric value of `cell` in the package at `path`.
fn cached(path: &Path, cell: &str) -> f64 {
    let mut zip = zip::ZipArchive::new(Cursor::new(std::fs::read(path).unwrap())).unwrap();
    let mut sheet = String::new();
    zip.by_name("xl/worksheets/sheet1.xml")
        .unwrap()
        .read_to_string(&mut sheet)
        .unwrap();
    let start = sheet.find(&format!("<c r=\"{cell}\"")).expect(cell);
    let c = &sheet[start..start + sheet[start..].find("</c>").unwrap()];
    let v = &c[c.find("<v>").expect(c) + 3..];
    v[..v.find("</v>").unwrap()].parse().unwrap()
}
fn serial(t: NaiveDateTime) -> f64 {
    let base = NaiveDate::from_ymd_opt(1899, 12, 30)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    (t - base).num_milliseconds() as f64 / 86_400_000.0
}
fn at(text: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M").unwrap()
}
/// TODAY/NOW caches equal the wall time `local` exactly.
fn assert_clock(path: &Path, local: NaiveDateTime, context: &str) {
    let today = serial(local.date().and_hms_opt(0, 0, 0).unwrap());
    assert_eq!(cached(path, "A1"), today, "TODAY {context}");
    assert!(
        (cached(path, "A2") - serial(local)).abs() < 1e-9,
        "NOW {context}: {} vs {}",
        cached(path, "A2"),
        serial(local)
    );
}

#[test]
fn now_and_tz_fix_today_and_now_across_a_date_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    let out = dir.path().join("out.xlsx");
    std::fs::write(&input, volatile_book()).unwrap();
    let o = out.to_str().unwrap();
    for (flags, local, echo_now, echo_tz) in [
        (
            vec!["--now", "2026-03-01T23:30:00Z"],
            "2026-03-01 23:30",
            "2026-03-01T23:30:00Z",
            "UTC",
        ),
        (
            vec!["--now", "2026-03-01T23:30:00Z", "--tz", "+01:00"],
            "2026-03-02 00:30",
            "2026-03-02T00:30:00+01:00",
            "+01:00",
        ),
        (
            vec!["--now", "2026-03-02T00:30:00+01:00"],
            "2026-03-02 00:30",
            "2026-03-02T00:30:00+01:00",
            "+01:00",
        ),
        (
            vec!["--now", "2026-03-02T00:30:00+01:00", "--tz", "utc"],
            "2026-03-01 23:30",
            "2026-03-01T23:30:00Z",
            "UTC",
        ),
        (
            vec!["--now", "2026-03-01T03:00:00Z", "--tz", "-05:00"],
            "2026-02-28 22:00",
            "2026-02-28T22:00:00-05:00",
            "-05:00",
        ),
        (
            vec!["--now=2026-03-01T03:00:00+00:00", "--tz=-05:00"],
            "2026-02-28 22:00",
            "2026-02-28T22:00:00-05:00",
            "-05:00",
        ),
    ] {
        let mut args = flags.clone();
        args.extend(["-o", o]);
        let (code, report) = recalc(&input, &args);
        assert_eq!(code, 0, "{report}");
        assert_clock(&out, at(local), &format!("{flags:?}"));
        assert_eq!(report["clock"]["now"], echo_now, "{flags:?}");
        assert_eq!(report["clock"]["timezone"], echo_tz, "{flags:?}");
        assert_eq!(report["clock"]["fixed"], true);
        assert_eq!(report["seed"], DEFAULT_SEED);
    }
}

#[test]
fn same_now_and_seed_are_byte_identical_and_seed_changes_rand() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    std::fs::write(&input, volatile_book()).unwrap();
    let run = |name: &str, extra: &[&str]| {
        let out = dir.path().join(name);
        let mut args = vec!["-o", out.to_str().unwrap()];
        args.extend(extra);
        assert_eq!(recalc(&input, &args).0, 0);
        out
    };
    let fixed = ["--now", "2026-01-31T09:00:00Z", "--seed", "42"];
    let (a, b) = (run("a.xlsx", &fixed), run("b.xlsx", &fixed));
    assert_eq!(std::fs::read(&a).unwrap(), std::fs::read(&b).unwrap());
    let c = run("c.xlsx", &["--now", "2026-01-31T09:00:00Z", "--seed", "43"]);
    assert_ne!(cached(&a, "A3"), cached(&c, "A3"));
    assert_ne!(cached(&a, "A4"), cached(&c, "A4"));
    assert_eq!(cached(&a, "A1"), cached(&c, "A1"));
    // RAND is reproducible without --seed, and the default is the echoed
    // seed. (Portable builds need --now for this workbook's TODAY/NOW.)
    let clock: &[&str] = if cfg!(feature = "system-clock") {
        &[]
    } else {
        &["--now", "2026-01-31T09:00:00Z"]
    };
    let (d, e) = (run("d.xlsx", clock), run("e.xlsx", clock));
    let seed = DEFAULT_SEED.to_string();
    let f = run("f.xlsx", &[clock, &["--seed", &seed]].concat());
    for cell in ["A3", "A4"] {
        assert_eq!(cached(&d, cell), cached(&e, cell));
        assert_eq!(cached(&d, cell), cached(&f, cell));
    }
}

#[test]
fn check_is_current_for_output_produced_with_the_same_flags() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    let out = dir.path().join("out.xlsx");
    std::fs::write(&input, volatile_book()).unwrap();
    let fixed = ["--now", "2026-01-31T09:00:00+02:00", "--seed", "7"];
    let mut args = vec!["-o", out.to_str().unwrap()];
    args.extend(fixed);
    assert_eq!(recalc(&input, &args).0, 0);
    let bytes = std::fs::read(&out).unwrap();
    let mut check = vec!["--check"];
    check.extend(fixed);
    let (code, report) = recalc(&out, &check);
    assert_eq!((code, report["status"].as_str()), (0, Some("current")));
    assert_eq!(report["clock"]["now"], "2026-01-31T09:00:00+02:00");
    // A different seed or instant makes the same output stale.
    for other in [
        ["--now", "2026-01-31T09:00:00+02:00", "--seed", "8"],
        ["--now", "2026-02-01T09:00:00+02:00", "--seed", "7"],
    ] {
        let mut check = vec!["--check"];
        check.extend(other);
        let (code, report) = recalc(&out, &check);
        assert_eq!((code, report["status"].as_str()), (3, Some("stale")));
    }
    // In-place recalculation with the same flags is a byte-identical no-op.
    let (code, report) = recalc(&out, &fixed);
    assert_eq!((code, report["status"].as_str()), (0, Some("unchanged")));
    assert_eq!(std::fs::read(&out).unwrap(), bytes);
}

#[test]
fn invalid_now_tz_and_seed_are_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    let bytes = volatile_book();
    std::fs::write(&input, &bytes).unwrap();
    let path = input.to_str().unwrap();
    for (flag, value) in [
        ("--now", "2026-01-31T09:00:00"),
        ("--now", "2026-01-31"),
        ("--now", "yesterday"),
        ("--tz", "Europe/Paris"),
        ("--tz", "Local"),
        ("--tz", "+24:00"),
        ("--tz", "+02:60"),
        ("--tz", "+2:00"),
        ("--tz", "0200"),
        ("--seed", "-1"),
        ("--seed", "18446744073709551616"),
        ("--seed", "abc"),
    ] {
        let assignment = format!("{flag}={value}");
        let (code, out, err) = invoke(&["recalc", path, &assignment, "--json"]);
        assert_eq!(code, 64, "{assignment}");
        assert!(err.is_empty(), "{assignment}: {err}");
        let report: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(out.lines().count(), 1);
        assert_eq!(report["status"], "error");
        assert_eq!(report["clock"], Value::Null);
        assert_eq!(report["seed"], Value::Null);
        let message = report["message"].as_str().unwrap();
        assert!(message.contains(flag), "{assignment}: {message}");
        let (code, out, err) = invoke(&["recalc", path, &assignment]);
        assert_eq!(code, 64, "{assignment}");
        assert!(out.is_empty());
        assert!(err.contains("invalid value"), "{assignment}: {err}");
        assert!(err.contains("For more information"), "{err}");
    }
    let (_, out, _) = invoke(&["recalc", path, "--now", "2026-01-31T09:00:00", "--json"]);
    assert!(out.contains("offset or Z"), "{out}");
    assert_eq!(std::fs::read(&input).unwrap(), bytes);
}

#[test]
fn clock_and_seed_echo_present_when_computed_and_null_otherwise() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    std::fs::write(&input, volatile_book()).unwrap();
    #[cfg(feature = "system-clock")]
    for extra in [vec![], vec!["--check"]] {
        let (code, report) = recalc(&input, &extra);
        assert!(code == 0 || code == 3, "{report}");
        assert_eq!(report["seed"], DEFAULT_SEED);
        assert_eq!(report["clock"]["fixed"], false);
        assert_eq!(report["clock"]["timezone"], "Local");
        assert!(
            chrono::DateTime::parse_from_rfc3339(report["clock"]["now"].as_str().unwrap()).is_ok(),
            "{report}"
        );
    }
    let refused = dir.path().join("refused.xlsx");
    std::fs::write(&refused, b"PK\x03\x04 not really a zip").unwrap();
    let (code, report) = recalc(&refused, &["--now", "2026-01-31T09:00:00Z", "--seed", "1"]);
    assert!(code == 1 || code == 2, "{report}");
    assert_eq!(report["clock"], Value::Null);
    assert_eq!(report["seed"], Value::Null);
}

/// The reported instant, read in the reported zone, is what TODAY/NOW cached,
/// and `--now <clock.now>` (plus `--tz` for a fixed zone) replays the run.
#[cfg(feature = "system-clock")]
#[test]
fn system_clock_run_replays_byte_identically_from_its_echo() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    std::fs::write(&input, volatile_book()).unwrap();
    for extra in [vec![], vec!["--tz", "+05:30"], vec!["--tz", "UTC"]] {
        let first = dir.path().join("first.xlsx");
        let mut args = vec!["-o", first.to_str().unwrap()];
        args.extend(&extra);
        let (code, report) = recalc(&input, &args);
        assert_eq!(code, 0, "{report}");
        let clock = &report["clock"];
        assert_eq!(clock["fixed"], false);
        let now = clock["now"].as_str().unwrap();
        let seen = chrono::DateTime::parse_from_rfc3339(now).unwrap();
        if let [_, tz] = extra.as_slice() {
            assert_eq!(clock["timezone"], *tz);
            let offset = if *tz == "UTC" { "Z" } else { tz };
            assert!(now.ends_with(offset), "{now}");
        } else {
            assert_eq!(clock["timezone"], "Local");
        }
        assert_clock(&first, seen.naive_local(), now);
        let replay = dir.path().join("replay.xlsx");
        let seed = report["seed"].to_string();
        let mut args = vec![
            "-o",
            replay.to_str().unwrap(),
            "--now",
            now,
            "--seed",
            &seed,
        ];
        let tz = clock["timezone"].as_str().unwrap();
        if tz != "Local" {
            args.extend(["--tz", tz]);
        }
        let (code, replayed) = recalc(&input, &args);
        assert_eq!(code, 0, "{replayed}");
        assert_eq!(replayed["clock"]["now"], now);
        assert_eq!(replayed["clock"]["fixed"], true);
        assert_eq!(
            std::fs::read(&first).unwrap(),
            std::fs::read(&replay).unwrap(),
            "{extra:?}"
        );
    }
}

/// Builds without a system clock refuse TODAY/NOW, unless `--now` fixes it.
#[cfg(not(feature = "system-clock"))]
#[test]
fn now_admits_today_and_now_without_a_system_clock() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    let bytes = volatile_book();
    std::fs::write(&input, &bytes).unwrap();
    let (code, report) = recalc(&input, &[]);
    assert_eq!(code, 2, "{report}");
    assert!(
        report["refusal"]["feature"]
            .as_str()
            .unwrap()
            .contains("wall clock")
    );
    assert_eq!(report["clock"], Value::Null);
    assert_eq!(std::fs::read(&input).unwrap(), bytes);
    let (code, report) = recalc(&input, &["--now", "2026-03-01T23:30:00-01:00"]);
    assert_eq!(code, 0, "{report}");
    assert_clock(&input, at("2026-03-01 23:30"), "portable");
    assert_eq!(report["clock"]["now"], "2026-03-01T23:30:00-01:00");
    assert_eq!(report["clock"]["fixed"], true);
}

fn assert_snapshot(name: &str, actual: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(name);
    if std::env::var_os("UPDATE_SNAPSHOTS").is_some() {
        std::fs::write(&path, actual).unwrap();
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        actual,
        expected,
        "{} differs; rerun with UPDATE_SNAPSHOTS=1 to accept",
        path.display()
    );
}

#[test]
fn help_text_snapshots() {
    for (args, name) in [
        (vec!["--help"], "help.txt"),
        (vec!["recalc", "--help"], "recalc-help.txt"),
    ] {
        let (code, out, err) = invoke(&args);
        assert_eq!(code, 0);
        assert!(err.is_empty());
        assert_snapshot(name, &out);
        // The docs site reproduces the help verbatim.
        let page = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs-site/content/docs/recalc-cli/cli-reference.mdx");
        if let Ok(site) = std::fs::read_to_string(&page) {
            let shown = format!("$ formualizer {}\n{out}", args.join(" "));
            assert!(site.contains(&shown), "{} is out of date", page.display());
        }
    }
    let (_, recalc_help, _) = invoke(&["help", "recalc"]);
    for needle in [
        "in place",
        "last step",
        "64",
        "130",
        "--now",
        "--tz",
        "--seed",
    ] {
        assert!(recalc_help.contains(needle), "{needle}");
    }
}
