use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[test]
fn sparse_overlay_write_does_not_densify_untouched_columns() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    let sheet = "Sheet1";
    let ncols: usize = 50;
    let chunk_rows: usize = 256;

    // Seed a dense base with a single 256-row chunk per column.
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, ncols, chunk_rows);
        let row = vec![LiteralValue::Empty; ncols];
        for _ in 0..chunk_rows {
            ab.append_row(sheet, &row).unwrap();
        }
        ab.finish().unwrap();
    }

    {
        let asheet = engine
            .sheet_store()
            .sheet(sheet)
            .expect("arrow sheet exists");
        assert_eq!(asheet.columns.len(), ncols);
        for c in 0..ncols {
            assert_eq!(
                asheet.columns[c].chunks.len(),
                1,
                "expected exactly one base chunk at start (col={c})"
            );
        }
    }

    // Write a single value far away. This should not force allocation of empty chunks for every column.
    let far_row: u32 = 100_000;
    engine
        .set_cell_value(sheet, far_row, 1, LiteralValue::Int(1))
        .unwrap();

    let asheet = engine
        .sheet_store()
        .sheet(sheet)
        .expect("arrow sheet exists");
    assert!(
        (asheet.nrows as u32) >= far_row,
        "sheet should track logical growth"
    );

    // Untouched columns should not be densified with hundreds of empty chunks.
    for c in 0..ncols {
        assert_eq!(
            asheet.columns[c].chunks.len(),
            1,
            "unexpected densification (col={c}, chunks={})",
            asheet.columns[c].chunks.len()
        );
    }

    // Reading the far value via ArrowRangeView should still work.
    let r0 = (far_row - 1) as usize;
    let av = asheet.range_view(r0, 0, r0, 0);
    assert_eq!(av.get_cell(0, 0), LiteralValue::Number(1.0));
}
