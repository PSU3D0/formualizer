//! A recurrence filled down a column that reads a fixed cell in the same
//! column (the decay schedule `B3=0.2`, `B4=+B3/12`, `B8=18000`,
//! `B9=+B8-(B8*$B$4)` filled down) must read that cell at every size, as
//! Excel does: 17700, 17405, ...
use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

const SIZES: [u32; 5] = [31, 32, 33, 64, 1000];

/// Excel's values for rows 9.. (the same operations in the same order).
fn expected(n: u32) -> Vec<f64> {
    let rate = 0.2 / 12.0;
    let mut x = 18000.0f64;
    (0..n)
        .map(|_| {
            x -= x * rate;
            x
        })
        .collect()
}

fn letter(col: u32) -> char {
    char::from(b'A' + (col - 1) as u8)
}

fn schedule(n: u32, col: u32, anchor_formula: bool) -> Workbook {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 3, 2, LiteralValue::Number(0.2)).unwrap();
    if anchor_formula {
        wb.set_formula("S", 4, 2, "=+B3/12").unwrap();
    } else {
        wb.set_value("S", 4, 2, LiteralValue::Number(0.2 / 12.0))
            .unwrap();
    }
    wb.set_value("S", 8, col, LiteralValue::Number(18000.0))
        .unwrap();
    let l = letter(col);
    for r in 9..9 + n {
        wb.set_formula("S", r, col, &format!("=+{l}{p}-({l}{p}*$B$4)", p = r - 1))
            .unwrap();
    }
    wb
}

fn assert_schedule(wb: &Workbook, n: u32, col: u32, ctx: &str) {
    for (i, want) in expected(n).into_iter().enumerate() {
        let r = 9 + i as u32;
        assert_eq!(
            wb.get_value("S", r, col),
            Some(LiteralValue::Number(want)),
            "{ctx} n={n} {}{r}",
            letter(col)
        );
    }
}

#[test]
fn workbook_decay_schedule_reads_same_column_anchor() {
    for n in SIZES {
        for (col, anchor_formula) in [(2, true), (2, false), (5, true)] {
            let mut wb = schedule(n, col, anchor_formula);
            wb.evaluate_all().unwrap();
            let ctx = format!("col={col} anchor_formula={anchor_formula}");
            assert_schedule(&wb, n, col, &ctx);
            // A recalculation after the rate's input changes.
            wb.set_value("S", 3, 2, LiteralValue::Number(0.6)).unwrap();
            if !anchor_formula {
                wb.set_value("S", 4, 2, LiteralValue::Number(0.6 / 12.0))
                    .unwrap();
            }
            wb.evaluate_all().unwrap();
            let rate = 0.6 / 12.0;
            let mut x = 18000.0f64;
            for r in 9..9 + n {
                x -= x * rate;
                assert_eq!(
                    wb.get_value("S", r, col),
                    Some(LiteralValue::Number(x)),
                    "{ctx} n={n} after edit R{r}"
                );
            }
        }
    }
}

#[cfg(feature = "xlsx-recalc")]
mod xlsx {
    use super::{SIZES, expected};
    use calamine::{Data, Reader, Xlsx};
    use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
    use std::io::{Cursor, Write};
    use zip::ZipWriter;

    const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
    const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

    fn pack(sheet_data: &str) -> Vec<u8> {
        let parts = [
            (
                "[Content_Types].xml",
                "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned(),
            ),
            (
                "_rels/.rels",
                format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>"),
            ),
            (
                "xl/workbook.xml",
                format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Terrebonne\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>"),
            ),
            (
                "xl/_rels/workbook.xml.rels",
                format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>"),
            ),
            (
                "xl/worksheets/sheet1.xml",
                format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{sheet_data}</sheetData></worksheet>"),
            ),
        ];
        let mut z = ZipWriter::new(Cursor::new(Vec::new()));
        for (name, body) in parts {
            z.start_file(name, zip::write::SimpleFileOptions::default())
                .unwrap();
            z.write_all(body.as_bytes()).unwrap();
        }
        z.finish().unwrap().into_inner()
    }

    /// The schedule as Excel saves it, with `caches` as the members'
    /// cached values; `shared`: the run as one shared formula.
    fn schedule(n: u32, caches: &[f64], shared: bool) -> Vec<u8> {
        let rate = 0.2 / 12.0;
        let mut rows = format!(
            "<row r=\"3\"><c r=\"B3\"><v>0.2</v></c></row><row r=\"4\"><c r=\"B4\"><f>+B3/12</f><v>{rate}</v></c></row><row r=\"8\"><c r=\"B8\"><v>18000</v></c></row>"
        );
        for (i, cache) in caches.iter().enumerate().take(n as usize) {
            let r = 9 + i as u32;
            let p = r - 1;
            let f = if !shared {
                format!("<f>+B{p}-(B{p}*$B$4)</f>")
            } else if i == 0 {
                format!(
                    "<f t=\"shared\" ref=\"B9:B{}\" si=\"0\">+B{p}-(B{p}*$B$4)</f>",
                    8 + n
                )
            } else {
                "<f t=\"shared\" si=\"0\"/>".to_owned()
            };
            rows.push_str(&format!(
                "<row r=\"{r}\"><c r=\"B{r}\">{f}<v>{cache}</v></c></row>"
            ));
        }
        pack(&rows)
    }

    fn column_b(bytes: &[u8], n: u32) -> Vec<f64> {
        let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
        let range = x.worksheet_range("Terrebonne").unwrap();
        (0..n)
            .map(|i| match range.get_value((8 + i, 1)) {
                Some(Data::Float(v)) => *v,
                Some(Data::Int(v)) => *v as f64,
                other => panic!("B{}: {other:?}", 9 + i),
            })
            .collect()
    }

    /// Excel's own caches are kept: nothing to change.
    #[test]
    fn xlsx_recalc_keeps_excel_caches_of_decay_schedule() {
        for n in SIZES {
            for shared in [false, true] {
                let input = schedule(n, &expected(n), shared);
                let out =
                    recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
                assert_eq!(
                    out.cache_cells_changed, 0,
                    "n={n} shared={shared}: Excel's caches rewritten"
                );
                assert_eq!(
                    column_b(&out.bytes, n),
                    expected(n),
                    "n={n} shared={shared}"
                );
            }
        }
    }

    /// Stale caches are recalculated to Excel's values.
    #[test]
    fn xlsx_recalc_repairs_stale_caches_of_decay_schedule() {
        for n in SIZES {
            for shared in [false, true] {
                let input = schedule(n, &vec![18000.0; n as usize], shared);
                let out =
                    recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
                assert_eq!(
                    column_b(&out.bytes, n),
                    expected(n),
                    "n={n} shared={shared}"
                );
            }
        }
    }
}
