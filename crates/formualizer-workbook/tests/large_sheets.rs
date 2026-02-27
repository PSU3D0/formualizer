use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

fn fill_col(wb: &mut Workbook, sheetname: &str, colnum: u32, start_rownum: u32, num_rows: u32) {
    let mut val: i64 = 0;
    for rownum in start_rownum..start_rownum + num_rows + 1 {
        wb.set_value(sheetname, rownum, colnum, LiteralValue::Int(val))
            .unwrap();
        val += 1;
    }
}

#[test]
fn single_large_column() {
    let mut wb = Workbook::new();
    let sheetname =
        "this is my test sheet with a longer name. And spaces. And other $%6& special characters!";
    wb.add_sheet(sheetname).unwrap();
    let n = 10_000;
    let expected_sum: f64 = (n * (n + 1) / 2) as f64;
    fill_col(&mut wb, &sheetname, 2, 1000, n);

    _ = wb.set_formula(sheetname, 1, 1, "=sum(B:B)").unwrap();
    let result = wb.evaluate_cell(sheetname, 1, 1).unwrap();

    assert_eq!(result, LiteralValue::Number(expected_sum));
}
