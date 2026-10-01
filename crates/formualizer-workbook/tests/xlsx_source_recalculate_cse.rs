#![cfg(feature = "xlsx-recalc")]
mod support {
    pub mod source_xlsx;
}
use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
use support::source_xlsx::*;

fn source(formula: &str, extent: &str, children: &str) -> Vec<u8> {
    pack(&without_metadata(package(
        "A1:F6",
        &format!(
            "<row r=\"1\"><c r=\"A1\"><v>2</v></c><c r=\"B1\" s=\"1\"><f t=\"array\" ref=\"{extent}\">{formula}</f><v>99</v></c><c r=\"F1\"><f>SUM(B1:D3)</f><v>99</v></c></row>{children}"
        ),
        "",
    )))
}

#[test]
fn fixed_extent_inserts_missing_members_and_preserves_source() {
    let bytes = source("SEQUENCE(A1)", "B1:B3", "");
    let out = recalculate_xlsx_bytes(&bytes, Default::default()).unwrap();
    let sheet = parse_sheet(&sheet_xml(&out.bytes));
    assert_eq!(sheet.cell("B1").v.as_deref(), Some("1"));
    assert_eq!(sheet.cell("B2").v.as_deref(), Some("2"));
    assert_eq!(sheet.cell("B3").v.as_deref(), Some("#N/A"));
    assert_eq!(
        sheet
            .cell("B1")
            .f
            .as_ref()
            .unwrap()
            .get("ref")
            .map(String::as_str),
        Some("B1:B3")
    );
    assert!(!sheet.cell("B1").attrs.contains_key("cm"));
    assert_eq!(out.formula_cells, 2);
    let mut original = unpack(&bytes);
    let mut updated = unpack(&out.bytes);
    original.remove(SHEET);
    updated.remove(SHEET);
    assert_eq!(original, updated);
    let again = recalculate_xlsx_bytes(&out.bytes, Default::default()).unwrap();
    assert_eq!(again.bytes, out.bytes);
    assert_eq!(again.cache_cells_changed, 0);
}

#[test]
fn single_cell_fixed_array_never_spills() {
    let out =
        recalculate_xlsx_bytes(&source("SEQUENCE(10)", "B1", ""), Default::default()).unwrap();
    let sheet = parse_sheet(&sheet_xml(&out.bytes));
    assert_eq!(sheet.cell("B1").v.as_deref(), Some("1"));
    assert_eq!(sheet.cell("F1").v.as_deref(), Some("1"));
    assert!(!unpack(&out.bytes).contains_key(METADATA));
}

#[test]
fn fixed_extent_broadcast_truncation_and_error_fill() {
    for (formula, expected) in [
        ("7", ["7", "7", "7"]),
        ("SEQUENCE(5)", ["1", "2", "3"]),
        ("1/0", ["#DIV/0!", "#DIV/0!", "#DIV/0!"]),
    ] {
        let out =
            recalculate_xlsx_bytes(&source(formula, "B1:B3", ""), Default::default()).unwrap();
        let sheet = parse_sheet(&sheet_xml(&out.bytes));
        for (row, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                sheet.cell(&format!("B{}", row + 1)).v.as_deref(),
                Some(expected)
            );
        }
        assert_eq!(
            recalculate_xlsx_bytes(&out.bytes, Default::default())
                .unwrap()
                .bytes,
            out.bytes
        );
    }
}

#[test]
fn refuses_overlaps_metadata_data_tables_and_non_top_left_anchors() {
    for children in [
        "<row r=\"2\"><c r=\"B2\"><f t=\"array\" ref=\"B2:C2\">1</f><v>1</v></c></row>",
        "<row r=\"2\"><c r=\"B2\" cm=\"1\"><v>1</v></c></row>",
        "<row r=\"2\"><c r=\"B2\" vm=\"1\"><v>1</v></c></row>",
    ] {
        assert!(
            recalculate_xlsx_bytes(
                &source("SEQUENCE(3)", "B1:B3", children),
                Default::default()
            )
            .is_err()
        );
    }
    assert!(recalculate_xlsx_bytes(&source("1", "A1:B3", ""), Default::default()).is_err());
    let bytes = source("1", "B1:B3", "");
    let mut parts = unpack(&bytes);
    let xml = parts.get_mut(SHEET).unwrap();
    *xml = xml.replace("t=\"array\"", "t=\"dataTable\"");
    assert!(recalculate_xlsx_bytes(&pack(&parts), Default::default()).is_err());
}

#[test]
fn refuses_child_formula_and_over_cap() {
    assert!(
        recalculate_xlsx_bytes(
            &source(
                "SEQUENCE(3)",
                "B1:B3",
                "<row r=\"2\"><c r=\"B2\"><f>1</f><v>1</v></c></row>"
            ),
            Default::default()
        )
        .is_err()
    );
    let mut options = XlsxRecalculateOptions::default();
    options.eval_config.spill.max_spill_cells = 2;
    let error = recalculate_xlsx_bytes(&source("SEQUENCE(3)", "B1:B3", ""), options).unwrap_err();
    assert!(error.to_string().contains("cap"));
}
