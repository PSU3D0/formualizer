#![cfg(feature = "xlsx")]
use formualizer_testkit::{
    materialize::{Materialize, Xlsx},
    patch_part,
    shape::{Cell, Range, Shape, r#gen},
};

#[test]
fn xlsx_materializes_and_part_patch_keeps_archive_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.xlsx");
    let shape = Shape::new().sheet("S", |s| {
        s.values(Range::col("A", 1..=2), r#gen::index_f64());
        s.family("doubled", Range::col("B", 1..=2), "=A{r}*2")
            .override_cell("B1", Cell::formula("=A1*2"));
    });
    Xlsx::new(&path).materialize(&shape).unwrap();
    patch_part(&path, "xl/worksheets/sheet1.xml", |xml| {
        format!("<!-- patched -->{xml}")
    })
    .unwrap();
    let workbook = umya_spreadsheet::reader::xlsx::read(&path).unwrap();
    assert!(workbook.get_sheet_by_name("S").is_some());
}
