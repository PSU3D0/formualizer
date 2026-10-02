#![cfg(feature = "xlsx-recalc")]
mod support {
    pub mod source_xlsx;
}
use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
use support::source_xlsx::*;
fn source(formula: &str, hidden: &str) -> Vec<u8> {
    pack(&without_metadata(package(
        "A1:D3",
        &format!(
            "<row r=\"1\"><c r=\"A1\"><v>2</v></c><c r=\"B1\"><f>{}</f><v>99</v></c><c r=\"C1\"><f>B1*2</f><v>99</v></c><c r=\"D1\"><f>C1+1</f><v>99</v></c></row><row r=\"2\"{hidden}><c r=\"A2\"><v>3</v></c></row><row r=\"3\"><c r=\"A3\"><v>4</v></c></row>",
            quick_xml::escape::escape(formula)
        ),
        "",
    )))
}
#[test]
fn volatile_results_and_chains_are_one_current_evaluation() {
    for (formula, expected) in [
        ("SUM(OFFSET(A1,0,0,2,1))", Some(5.0)),
        ("INDIRECT(\"A1\")", Some(2.0)),
        ("SUBTOTAL(9,A1:A2)", Some(5.0)),
        ("SUBTOTAL(109,A1:A2)", Some(5.0)),
        ("AGGREGATE(9,0,A1:A2)", Some(5.0)),
        ("RAND()", None),
    ] {
        let out = recalculate_xlsx_bytes(&source(formula, ""), Default::default()).unwrap();
        let sheet = parse_sheet(&sheet_xml(&out.bytes));
        let b: f64 = sheet.cell("B1").v.as_ref().unwrap().parse().unwrap();
        if let Some(expected) = expected {
            assert_eq!(b, expected, "{formula}");
        } else {
            assert!((0.0..1.0).contains(&b));
        }
        let c: f64 = sheet.cell("C1").v.as_ref().unwrap().parse().unwrap();
        let d: f64 = sheet.cell("D1").v.as_ref().unwrap().parse().unwrap();
        assert_eq!(c, b * 2.0);
        assert_eq!(d, c + 1.0);
        assert_eq!(out.summary.evaluated, 3);
    }
}
#[test]
fn now_and_today_use_the_request_clock_sample() {
    use formualizer_common::{DateSystem, LiteralValue};
    for formula in ["NOW()", "TODAY()"] {
        let before = chrono::Utc::now().naive_utc();
        let mut options = XlsxRecalculateOptions::default();
        options.eval_config.deterministic_mode =
            formualizer_eval::engine::DeterministicMode::Enabled {
                timestamp_utc: before.and_utc(),
                timezone: formualizer_eval::timezone::TimeZoneSpec::Utc,
            };
        let out = recalculate_xlsx_bytes(&source(formula, ""), options).unwrap();
        let after = chrono::Utc::now().naive_utc();
        let serial = |t: chrono::NaiveDateTime| {
            if formula == "TODAY()" {
                LiteralValue::Date(t.date())
            } else {
                LiteralValue::DateTime(t)
            }
            .as_serial_number_for(DateSystem::Excel1900)
            .unwrap()
        };
        let sheet = parse_sheet(&sheet_xml(&out.bytes));
        let b: f64 = sheet.cell("B1").v.as_ref().unwrap().parse().unwrap();
        assert!(
            b >= serial(before) - 1.0 / 86400.0 && b <= serial(after) + 1.0 / 86400.0,
            "{formula}: {b}"
        );
        assert_eq!(
            sheet.cell("C1").v.as_ref().unwrap().parse::<f64>().unwrap(),
            b * 2.0
        );
    }
}
#[test]
fn hidden_rows_are_refused_only_when_aggregate_ranges_may_intersect() {
    for formula in [
        "SUBTOTAL(9,A1:A2)",
        "SUBTOTAL(109,A1:A2)",
        "AGGREGATE(9,0,A1:A2)",
    ] {
        let error = recalculate_xlsx_bytes(&source(formula, " hidden=\"1\""), Default::default())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("hidden row") && error.contains("B1"),
            "{error}"
        );
    }
    let out = recalculate_xlsx_bytes(
        &source("SUBTOTAL(109,A3:A3)", " hidden=\"1\""),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        parse_sheet(&sheet_xml(&out.bytes)).cell("B1").v.as_deref(),
        Some("4")
    );
}

#[cfg(not(feature = "system-clock"))]
#[test]
fn portable_date_time_formulas_refuse_the_epoch_fallback() {
    for formula in ["TODAY()", "NOW()", "IF(TRUE,NOW(),0)"] {
        let err = recalculate_xlsx_bytes(&source(formula, ""), Default::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("wall clock") && err.contains("B1"), "{err}");
    }
    let out = recalculate_xlsx_bytes(&source("\"TODAY()\"", ""), Default::default()).unwrap();
    assert_eq!(
        parse_sheet(&sheet_xml(&out.bytes)).cell("B1").v.as_deref(),
        Some("TODAY()")
    );
    let p = edit(
        without_metadata(package(
            "A1",
            "<row r=\"1\"><c r=\"A1\"><f>ClockValue</f><v>99</v></c></row>",
            "",
        )),
        "xl/workbook.xml",
        "</workbook>",
        "<definedNames><definedName name=\"ClockValue\">TODAY()</definedName></definedNames></workbook>",
    );
    let err = recalculate_xlsx_bytes(&pack(&p), Default::default())
        .unwrap_err()
        .to_string();
    assert!(err.contains("wall clock"), "{err}");
}

#[test]
fn active_filters_without_hidden_row_flags_are_not_guessed() {
    let filter = "<autoFilter ref=\"A1:A3\"><filterColumn colId=\"0\"><filters><filter val=\"3\"/></filters></filterColumn></autoFilter>";
    let filtered = |formula: &str| {
        let bytes = source(formula, "");
        let p = edit(
            unpack(&bytes),
            "xl/worksheets/sheet1.xml",
            "</worksheet>",
            &format!("{filter}</worksheet>"),
        );
        pack(&p)
    };
    for formula in ["SUBTOTAL(9,A1:A2)", "AGGREGATE(9,0,A1:A3)"] {
        assert!(
            recalculate_xlsx_bytes(&filtered(formula), Default::default()).is_err(),
            "{formula}"
        );
    }
    for (formula, value) in [("SUM(A1:A2)", "5"), ("SUBTOTAL(109,A1:A1)", "2")] {
        let out = recalculate_xlsx_bytes(&filtered(formula), Default::default()).unwrap();
        assert_eq!(
            parse_sheet(&sheet_xml(&out.bytes)).cell("B1").v.as_deref(),
            Some(value)
        );
    }
}
