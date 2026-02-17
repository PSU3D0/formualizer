use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

fn build_two_sheet_chain_workbook() -> std::path::PathBuf {
    build_workbook(|book| {
        let _ = book.new_sheet("Data");

        let s1 = book.get_sheet_by_name_mut("Sheet1").unwrap();
        s1.get_cell_mut((1, 1)).set_value_number(1.0); // A1
        s1.get_cell_mut((2, 1)).set_formula("=A1+1"); // B1
        s1.get_cell_mut((3, 1)).set_formula("=B1+1"); // C1

        let s2 = book.get_sheet_by_name_mut("Data").unwrap();
        s2.get_cell_mut((1, 1)).set_value_number(1.0); // A1
        s2.get_cell_mut((2, 1)).set_formula("=A1+1"); // B1
        s2.get_cell_mut((3, 1)).set_formula("=B1+1"); // C1
    })
}

#[test]
fn umya_stream_multi_sheet_single_recalc_after_edit_updates_both_sheets() {
    let path = build_two_sheet_chain_workbook();

    let mut backend = UmyaAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    backend.stream_into_engine(&mut engine).unwrap();

    // Initial calc
    engine.evaluate_all().unwrap();
    let s1_c1_initial = engine.get_cell_value("Sheet1", 1, 3);
    let s2_c1_initial = engine.get_cell_value("Data", 1, 3);
    assert_eq!(
        s1_c1_initial,
        Some(LiteralValue::Number(3.0)),
        "Sheet1!C1 should converge in one evaluate_all pass; Data!C1={s2_c1_initial:?}"
    );
    assert_eq!(s2_c1_initial, Some(LiteralValue::Number(3.0)));

    // Edit A1 on both sheets.
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Data", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // One recalc should update both formula chains.
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(12.0))
    );
    assert_eq!(
        engine.get_cell_value("Data", 1, 3),
        Some(LiteralValue::Number(12.0))
    );
}

#[test]
fn umya_workbook_single_recalc_after_edit_updates_both_sheets() {
    let path = build_two_sheet_chain_workbook();

    let backend = UmyaAdapter::open_path(&path).unwrap();
    let mut wb =
        Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .expect("load workbook");

    // Initial recalc
    wb.evaluate_all().unwrap();
    assert_eq!(
        wb.get_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(wb.get_value("Data", 1, 3), Some(LiteralValue::Number(3.0)));

    // Edit A1 on both sheets.
    wb.set_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    wb.set_value("Data", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // One recalc should update both formula chains.
    wb.evaluate_all().unwrap();

    assert_eq!(
        wb.get_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(12.0))
    );
    assert_eq!(wb.get_value("Data", 1, 3), Some(LiteralValue::Number(12.0)));
}
