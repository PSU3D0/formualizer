//! XLSX `<calcPr>` round-trip + iterative-calculation load (spec §9, RFC #113).
//!
//! Read path is the **calamine** backend (the RFC's load target). Fixtures are
//! authored with umya, then the `<calcPr>` element is injected into the written
//! `.xlsx` zip *directly with the `zip` crate in-test* (not via the crate's own
//! `calc_pr` writer) so the read-path assertions stay independent of the write
//! path. The round-trip test then exercises `Workbook::to_xlsx_bytes`
//! (the umya write path) explicitly.

use formualizer_eval::engine::{CycleConfig, CycleDetection, CyclePolicy};
use formualizer_workbook::{
    CalamineAdapter, CalcSettings, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook,
    WorkbookConfig,
};
use std::io::{Read, Write};

/// Build a minimal `.xlsx` (A1=value, B1=formula) with umya and return its bytes.
fn build_xlsx_bytes(build: impl FnOnce(&mut umya_spreadsheet::Worksheet)) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    {
        let ws = book.get_sheet_by_name_mut("Sheet1").unwrap();
        build(ws);
    }
    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut buf).unwrap();
    buf
}

/// Rewrite `xl/workbook.xml`'s `<calcPr>` element to `calc_pr_xml`, using the
/// `zip` crate directly (independent of the crate-under-test's writer). If no
/// `<calcPr>` exists, insert before `</workbook>`.
fn inject_calc_pr(xlsx: &[u8], calc_pr_xml: &str) -> Vec<u8> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(xlsx)).unwrap();
    let mut out = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut out));
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).unwrap();
            let name = entry.name().to_string();
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            if name == "xl/workbook.xml" {
                let mut xml = String::new();
                entry.read_to_string(&mut xml).unwrap();
                let rewritten = if let Some(start) = xml.find("<calcPr") {
                    // self-closing `<calcPr .../>`
                    let after = &xml[start..];
                    let end = after.find("/>").map(|i| start + i + 2).unwrap_or_else(|| {
                        start + after.find('>').map(|i| i + 1).unwrap_or(after.len())
                    });
                    format!("{}{}{}", &xml[..start], calc_pr_xml, &xml[end..])
                } else {
                    let close = xml.rfind("</workbook>").unwrap();
                    format!("{}{}{}", &xml[..close], calc_pr_xml, &xml[close..])
                };
                writer.start_file(name, opts).unwrap();
                writer.write_all(rewritten.as_bytes()).unwrap();
            } else {
                writer.start_file(name, opts).unwrap();
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).unwrap();
                writer.write_all(&bytes).unwrap();
            }
        }
        writer.finish().unwrap();
    }
    out
}

fn simple_fixture() -> Vec<u8> {
    build_xlsx_bytes(|ws| {
        ws.get_cell_mut((1, 1)).set_value_number(10); // A1
        ws.get_cell_mut((2, 1)).set_formula("A1*2"); // B1
    })
}

// ---------------------------------------------------------------------------
// Load: calcPr → CalcSettings on the backend
// ---------------------------------------------------------------------------

#[test]
fn calamine_parses_iterate_on_with_custom_count_and_delta() {
    let bytes = inject_calc_pr(
        &simple_fixture(),
        r#"<calcPr calcId="122211" calcMode="auto" iterate="1" iterateCount="42" iterateDelta="0.5" fullCalcOnLoad="1"/>"#,
    );
    let backend = CalamineAdapter::open_bytes(bytes).unwrap();
    let s = backend.calc_settings().expect("calcPr surfaced");
    assert!(s.iterate);
    assert_eq!(s.iterate_count, Some(42));
    assert_eq!(s.iterate_delta, Some(0.5));
    assert_eq!(s.calc_mode.as_deref(), Some("auto"));
    assert_eq!(s.full_calc_on_load, Some(true));
}

#[test]
fn calamine_parses_iterate_absent_as_no_iteration() {
    // A vanilla umya workbook writes `<calcPr calcId="122211"/>` (no iterate).
    let backend = CalamineAdapter::open_bytes(simple_fixture()).unwrap();
    let s = backend
        .calc_settings()
        .expect("calcPr present (umya emits one)");
    assert!(!s.iterate, "absent iterate must read as false");
}

#[test]
fn calamine_parses_iterate_zero_as_no_iteration() {
    let bytes = inject_calc_pr(
        &simple_fixture(),
        r#"<calcPr calcId="122211" iterate="0"/>"#,
    );
    let backend = CalamineAdapter::open_bytes(bytes).unwrap();
    let s = backend.calc_settings().unwrap();
    assert!(!s.iterate);
}

// ---------------------------------------------------------------------------
// Load → config mapping via Workbook::from_reader (detection-handling decision)
// ---------------------------------------------------------------------------

#[test]
fn from_reader_iterate_on_applies_runtime_iterate_config() {
    let bytes = inject_calc_pr(
        &simple_fixture(),
        r#"<calcPr calcId="122211" iterate="1" iterateCount="7" iterateDelta="0.01"/>"#,
    );
    let adapter = CalamineAdapter::open_bytes(bytes).unwrap();
    // Start from the engine default (detection: Static, policy: Error) — the
    // combo that would panic if `iterate` were applied without flipping
    // detection. from_reader must NOT panic and must produce a valid config.
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
        .expect("from_reader must not panic on loaded iterate config");
    let cycle = wb.engine().config.cycle;
    assert_eq!(cycle.detection, CycleDetection::Runtime);
    assert_eq!(
        cycle.policy,
        CyclePolicy::Iterate {
            max_iterations: 7,
            max_change: 0.01,
        }
    );
}

#[test]
fn from_reader_iterate_off_leaves_caller_cycle_config_untouched() {
    let adapter = CalamineAdapter::open_bytes(simple_fixture()).unwrap();
    // Caller explicitly chose the Static compat switch; loading a non-iterating
    // file must preserve it (spec §9: absent/0 → policy unchanged).
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.cycle = CycleConfig {
        detection: CycleDetection::Static,
        policy: CyclePolicy::Error,
    };
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg).unwrap();
    assert_eq!(wb.engine().config.cycle.detection, CycleDetection::Static);
    assert_eq!(wb.engine().config.cycle.policy, CyclePolicy::Error);
}

// ---------------------------------------------------------------------------
// End-to-end: convergent cycle, iterate enabled in the file → converged values
// ---------------------------------------------------------------------------

#[test]
fn from_reader_iterate_converges_arithmetic_cycle() {
    // Spec §7.4 arithmetic routing: B1 = g*X + (1-g)*C1, C1 = g*B1 + (1-g)*Y.
    // With g=0.5, X=10, Y=20:
    //   B1 = 0.5*10 + 0.5*C1 = 5 + 0.5*C1
    //   C1 = 0.5*B1 + 0.5*20 = 0.5*B1 + 10
    // Substituting: B1 = 5 + 0.5*(0.5*B1 + 10) = 10 + 0.25*B1
    //   => 0.75*B1 = 10 => B1 = 40/3 ≈ 13.3333, C1 = 0.5*B1 + 10 = 50/3 ≈ 16.6667.
    // |g·(1−g)| = 0.25 < 1, so iteration converges to this fixed point.
    let fixture = build_xlsx_bytes(|ws| {
        ws.get_cell_mut((1, 1)).set_value_number(10); // A1 = X
        ws.get_cell_mut((4, 1)).set_value_number(20); // D1 = Y
        ws.get_cell_mut((2, 1)).set_formula("0.5*A1 + 0.5*C1"); // B1
        ws.get_cell_mut((3, 1)).set_formula("0.5*B1 + 0.5*D1"); // C1
    });
    let bytes = inject_calc_pr(
        &fixture,
        r#"<calcPr calcId="122211" iterate="1" iterateCount="100" iterateDelta="0.001"/>"#,
    );
    let adapter = CalamineAdapter::open_bytes(bytes).unwrap();
    let mut wb =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .unwrap();
    let res = wb.evaluate_all().expect("evaluate_all");
    assert_eq!(res.cycle_errors, 0, "convergent cycle must not stamp #CIRC");

    let b1 = num(&wb, "Sheet1", 1, 2);
    let c1 = num(&wb, "Sheet1", 1, 3);
    assert!((b1 - 40.0 / 3.0).abs() < 0.01, "B1 converged to {b1}");
    assert!((c1 - 50.0 / 3.0).abs() < 0.01, "C1 converged to {c1}");
}

fn num(wb: &Workbook, sheet: &str, row: u32, col: u32) -> f64 {
    match wb.get_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Round-trip: load(iterate) → to_xlsx_bytes → re-load → settings preserved.
// Exercises the umya write path (post-process) + calcMode preservation.
// ---------------------------------------------------------------------------

#[cfg(feature = "umya")]
#[test]
fn roundtrip_iterate_settings_and_calc_mode_preserved() {
    let fixture = build_xlsx_bytes(|ws| {
        ws.get_cell_mut((1, 1)).set_value_number(1); // A1
        ws.get_cell_mut((2, 1)).set_formula("A1+1"); // B1
    });
    // iterate on with a distinctive count/delta + a calcMode we expect to survive.
    let bytes = inject_calc_pr(
        &fixture,
        r#"<calcPr calcId="122211" calcMode="manual" iterate="1" iterateCount="33" iterateDelta="0.25"/>"#,
    );

    // Load via the configured surface so the engine cycle config picks up iterate.
    let adapter = CalamineAdapter::open_bytes(bytes).unwrap();
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
        .unwrap();
    assert_eq!(
        wb.engine().config.cycle.policy,
        CyclePolicy::Iterate {
            max_iterations: 33,
            max_change: 0.25,
        }
    );

    // Save (umya write path post-processes calcPr from the active config).
    let saved = wb.to_xlsx_bytes().expect("to_xlsx_bytes");

    // Re-load and assert the iterate settings survived the write.
    let reloaded = CalamineAdapter::open_bytes(saved).unwrap();
    let s = reloaded.calc_settings().expect("calcPr round-tripped");
    assert!(s.iterate, "iterate flag lost on save");
    assert_eq!(s.iterate_count, Some(33));
    assert_eq!(s.iterate_delta, Some(0.25));
    // calcMode is round-trip-only but must be preserved (spec §9).
    assert_eq!(
        s.calc_mode.as_deref(),
        Some("manual"),
        "calcMode must be preserved across save"
    );
}

#[cfg(feature = "umya")]
#[test]
fn roundtrip_non_iterating_workbook_writes_iterate_off() {
    // A workbook with no cycle config (default Error) must save with iterate=0.
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("Sheet1").ok(); // umya seeds "Sheet1" already; ignore dup error
    wb.set_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    let saved = wb.to_xlsx_bytes().expect("to_xlsx_bytes");
    let reloaded = CalamineAdapter::open_bytes(saved).unwrap();
    let s = reloaded.calc_settings().expect("calcPr present");
    assert!(!s.iterate);
}

// ---------------------------------------------------------------------------
// Sanity: the public CalcSettings transport type is constructible by callers.
// ---------------------------------------------------------------------------

#[test]
fn calc_settings_default_is_non_iterating() {
    let s = CalcSettings::default();
    assert!(!s.iterate);
    assert_eq!(s.iterate_count, None);
}
