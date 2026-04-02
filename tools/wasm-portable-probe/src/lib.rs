use formualizer::Workbook;

#[unsafe(no_mangle)]
pub extern "C" fn probe_portable_runtime() -> i32 {
    let mut wb = Workbook::new();
    wb.add_sheet("Sheet1").expect("add sheet");

    // Touch both deterministic RNG and clock pathways so the linked artifact is
    // representative of the portable runtime surface.
    wb.set_formula("Sheet1", 1, 1, "=RAND()")
        .expect("set RAND formula");
    wb.set_formula("Sheet1", 1, 2, "=NOW()")
        .expect("set NOW formula");

    let _ = wb.evaluate_cell("Sheet1", 1, 1).expect("eval RAND");
    let _ = wb.evaluate_cell("Sheet1", 1, 2).expect("eval NOW");

    42
}
