//! Iterate edge corpus — `<calcPr>` fuzz (RFC #112/#113, spec §9).
//!
//! Hostile/degenerate attribute values must never panic the load; this file
//! pins the graceful behavior: unparseable or out-of-domain knob values fall
//! back to the Excel defaults (100 / 0.001), `iterate` accepts OOXML boolean
//! spellings only, the first `<calcPr>` wins when duplicated, and a fully
//! valid config always reaches the engine (`CycleConfig::validate` passes).
//!
//! Harness: fixtures are authored with umya, then `xl/workbook.xml` is
//! rewritten in-zip — same pattern as `calcpr.rs` (read path stays
//! independent of the crate's own writer).

use formualizer_eval::engine::{CycleConfig, CycleDetection, CyclePolicy};
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
use std::io::{Read, Write};

/// Build a minimal `.xlsx` (A1=10, B1==A1*2) and return its bytes.
fn simple_fixture() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    {
        let ws = book.get_sheet_by_name_mut("Sheet1").unwrap();
        ws.get_cell_mut((1, 1)).set_value_number(10); // A1
        ws.get_cell_mut((2, 1)).set_formula("A1*2"); // B1
    }
    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut buf).unwrap();
    buf
}

/// Rewrite `xl/workbook.xml`, replacing the first `<calcPr .../>` element with
/// `calc_pr_xml` (which may contain SEVERAL elements, or be empty to drop it).
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

/// Load a fuzzed workbook end-to-end and return the resulting cycle config.
/// Must never panic regardless of the `<calcPr>` contents.
fn load_cycle_config(calc_pr_xml: &str) -> CycleConfig {
    let bytes = inject_calc_pr(&simple_fixture(), calc_pr_xml);
    let adapter = CalamineAdapter::open_bytes(bytes).expect("open must not fail");
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
        .expect("load must not fail");
    wb.engine().config.cycle
}

const EXCEL_DEFAULTS: CyclePolicy = CyclePolicy::Iterate {
    max_iterations: 100,
    max_change: 0.001,
};

#[test]
fn iterate_uppercase_true_is_accepted() {
    let cfg = load_cycle_config(r#"<calcPr calcId="1" iterate="TRUE"/>"#);
    assert_eq!(cfg.detection, CycleDetection::Runtime);
    assert_eq!(cfg.policy, EXCEL_DEFAULTS);
    cfg.validate().expect("loaded config must be valid");
}

#[test]
fn iterate_garbage_boolean_reads_as_off() {
    for v in ["yes", "2", "TRUE ", " 1 \u{a0}", "tRuE", "-1", "⊤"] {
        // NB: parse_xml_bool trims ASCII whitespace; "TRUE " and " 1 " are
        // therefore ON, asserted separately below. The rest must be OFF.
        if v.trim() == "TRUE" || v.trim() == "1" {
            continue;
        }
        let cfg = load_cycle_config(&format!(r#"<calcPr calcId="1" iterate="{v}"/>"#));
        assert_eq!(
            cfg.policy,
            CyclePolicy::Error,
            "iterate={v:?} must read as off"
        );
    }
    // Whitespace-padded canonical spellings are tolerated (trim).
    let cfg = load_cycle_config(r#"<calcPr calcId="1" iterate=" 1 "/>"#);
    assert_eq!(cfg.policy, EXCEL_DEFAULTS);
}

#[test]
fn iterate_count_zero_falls_back_to_excel_default() {
    // iterateCount="0" is out of domain (max_iterations 0 is a config error);
    // graceful behavior: ignore the attribute, keep the Excel default 100.
    let cfg = load_cycle_config(r#"<calcPr calcId="1" iterate="1" iterateCount="0"/>"#);
    assert_eq!(cfg.policy, EXCEL_DEFAULTS);
    cfg.validate().expect("never a panicking config");
}

#[test]
fn iterate_count_u32_overflow_falls_back_to_excel_default() {
    for v in ["4294967296", "99999999999999999999", "-5", "1e3", "ten", ""] {
        let cfg = load_cycle_config(&format!(
            r#"<calcPr calcId="1" iterate="1" iterateCount="{v}"/>"#
        ));
        assert_eq!(cfg.policy, EXCEL_DEFAULTS, "iterateCount={v:?}");
        cfg.validate().expect("never a panicking config");
    }
}

#[test]
fn iterate_count_u32_max_is_accepted_verbatim() {
    // In-domain extreme: parses; the engine accepts any max_iterations ≥ 1.
    let cfg = load_cycle_config(r#"<calcPr calcId="1" iterate="1" iterateCount="4294967295"/>"#);
    assert_eq!(
        cfg.policy,
        CyclePolicy::Iterate {
            max_iterations: u32::MAX,
            max_change: 0.001
        }
    );
    cfg.validate().expect("valid (if inadvisable) config");
}

#[test]
fn iterate_delta_out_of_domain_falls_back_to_excel_default() {
    // Negative, NaN, ±inf (incl. the parse-to-inf "1e999"), and garbage all
    // fall back to 0.001 — the engine rejects non-finite/negative max_change.
    for v in [
        "-1", "NaN", "nan", "inf", "-inf", "1e999", "-1e999", "0,5", "x",
    ] {
        let cfg = load_cycle_config(&format!(
            r#"<calcPr calcId="1" iterate="1" iterateDelta="{v}"/>"#
        ));
        assert_eq!(cfg.policy, EXCEL_DEFAULTS, "iterateDelta={v:?}");
        cfg.validate().expect("never a panicking config");
    }
}

#[test]
fn iterate_delta_zero_is_in_domain_and_kept() {
    // 0.0 is valid (§2: only NEGATIVE or non-finite is a config error) and
    // means "only exact non-numeric repeats converge".
    let cfg = load_cycle_config(r#"<calcPr calcId="1" iterate="1" iterateDelta="0"/>"#);
    assert_eq!(
        cfg.policy,
        CyclePolicy::Iterate {
            max_iterations: 100,
            max_change: 0.0
        }
    );
    cfg.validate().expect("zero delta is valid");
}

#[test]
fn duplicated_calc_pr_first_element_wins() {
    let cfg = load_cycle_config(
        r#"<calcPr calcId="1" iterate="1" iterateCount="7"/><calcPr calcId="2" iterate="0"/>"#,
    );
    assert_eq!(
        cfg.policy,
        CyclePolicy::Iterate {
            max_iterations: 7,
            max_change: 0.001
        },
        "first calcPr wins"
    );
}

#[test]
fn missing_calc_pr_leaves_caller_config_untouched() {
    let bytes = inject_calc_pr(&simple_fixture(), "");
    let adapter = CalamineAdapter::open_bytes(bytes).unwrap();
    let cfg_in = WorkbookConfig::ephemeral();
    let expected = cfg_in.eval.cycle;
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg_in)
        .expect("load without calcPr must not fail");
    assert_eq!(wb.engine().config.cycle, expected);
}

#[test]
fn open_ended_calc_pr_with_explicit_close_tag_parses() {
    // Producers that emit `<calcPr ...></calcPr>` instead of self-closing.
    let cfg = load_cycle_config(r#"<calcPr calcId="1" iterate="1" iterateCount="9"></calcPr>"#);
    assert_eq!(
        cfg.policy,
        CyclePolicy::Iterate {
            max_iterations: 9,
            max_change: 0.001
        }
    );
}

#[test]
fn fuzzed_workbook_still_evaluates_a_cycle_correctly() {
    // End-to-end: a fuzzed-but-valid iterate config drives real iteration.
    let mut book = umya_spreadsheet::new_file();
    {
        let ws = book.get_sheet_by_name_mut("Sheet1").unwrap();
        ws.get_cell_mut((1, 1)).set_value_number(5); // A1
        ws.get_cell_mut((2, 1)).set_formula("B1+A1"); // B1 accumulator
    }
    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut buf).unwrap();
    // iterateCount="0" → default 100; delta NaN → default 0.001.
    let bytes = inject_calc_pr(
        &buf,
        r#"<calcPr calcId="1" iterate="true" iterateCount="0" iterateDelta="NaN"/>"#,
    );
    let adapter = CalamineAdapter::open_bytes(bytes).unwrap();
    let mut wb =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .expect("load");
    wb.evaluate_all().expect("evaluate");
    // B1 = B1 + A1 with default cap 100 → 5 added 100 times per recalc.
    match wb.get_value("Sheet1", 1, 2) {
        Some(formualizer_workbook::LiteralValue::Number(n)) => assert_eq!(n, 500.0),
        other => panic!("expected accumulator result, got {other:?}"),
    }
}
