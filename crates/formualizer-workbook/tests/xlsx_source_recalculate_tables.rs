#![cfg(feature = "xlsx-recalc")]
mod support { pub mod source_xlsx; }
use formualizer_workbook::recalculate_xlsx_bytes;
use support::source_xlsx::*;
const TABLE: &str = "xl/tables/table1.xml";
fn fixture(formula: &str) -> Parts {
    let mut p = without_metadata(package("A1:D4", &format!("<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Qty</t></is></c><c r=\"D1\"><f>{formula}</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>2</v></c></row><row r=\"3\"><c r=\"A3\"><v>3</v></c></row>"), "<tableParts count=\"1\"><tablePart r:id=\"rId3\"/></tableParts>"));
    p = edit(p, "xl/worksheets/_rels/sheet1.xml.rels", "</Relationships>", &format!("<Relationship Id=\"rId3\" Type=\"{OFFICE}/table\" Target=\"../tables/table1.xml\"/></Relationships>"));
    p = edit(p, TYPES, "</Types>", "<Override PartName=\"/xl/tables/table1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml\"/></Types>");
    p.insert(TABLE.into(), format!("<table xmlns=\"{MAIN}\" id=\"1\" name=\"Table1\" displayName=\"Table1\" ref=\"A1:A3\" totalsRowShown=\"0\"><autoFilter ref=\"A1:A3\"/><tableColumns count=\"1\"><tableColumn id=\"1\" name=\"Qty\"/></tableColumns><tableStyleInfo name=\"TableStyleMedium9\" showFirstColumn=\"0\" showLastColumn=\"0\" showRowStripes=\"1\" showColumnStripes=\"0\"/></table>"));
    p
}
#[test]
fn column_reference_preserves_table_and_reruns() {
    let p = fixture("SUM(Table1[Qty])");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(), Some("5"));
    let mut got = unpack(&out.bytes); got.remove(SHEET);
    let mut expected = p; expected.remove(SHEET); assert_eq!(got, expected);
    assert_eq!(recalculate_xlsx_bytes(&out.bytes, Default::default()).unwrap().bytes, out.bytes);
}
#[test]
fn unused_table_is_admitted() {
    let out = recalculate_xlsx_bytes(&pack(&fixture("1+2")), Default::default()).unwrap();
    assert_eq!(parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(), Some("3"));
}
#[test]
fn managed_column_missing_formula_is_refused() {
    let p = edit(fixture("1+2"), TABLE, "<tableColumn id=\"1\" name=\"Qty\"/>", "<tableColumn id=\"1\" name=\"Qty\"><calculatedColumnFormula>1+2</calculatedColumnFormula></tableColumn>");
    let error = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap_err().to_string();
    assert!(error.contains("write the formula into each row"), "{error}");
}
#[test]
fn blank_table_cells_obstruct_spills_and_dependents() {
    let p = edit(fixture("SUM(B1#)"), SHEET, "</is></c>", "</is></c><c r=\"B1\"><f>SEQUENCE(3,1)</f><v>99</v></c>");
    let p = edit(p, TABLE, "ref=\"A1:A3\"", "ref=\"B2:B4\" totalsRowCount=\"1\" headerRowCount=\"0\"");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    let sheet = parse_sheet(&sheet_xml(&out.bytes));
    assert_eq!(sheet.cell("B1").v.as_deref(), Some("#SPILL!"));
    assert_eq!(sheet.cell("D1").v.as_deref(), Some("#SPILL!"));
}
