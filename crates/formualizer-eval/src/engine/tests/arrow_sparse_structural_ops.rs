use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[test]
fn sparse_overlay_survives_row_and_column_structural_ops_without_densifying_other_columns() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    let sheet = "Sheet1";
    let ncols: usize = 20;
    let chunk_rows: usize = 256;

    // Seed a dense base with a single chunk per column.
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, ncols, chunk_rows);
        let row = vec![LiteralValue::Empty; ncols];
        for _ in 0..chunk_rows {
            ab.append_row(sheet, &row).unwrap();
        }
        ab.finish().unwrap();
    }

    let far_row: u32 = 100_000;
    let far_col: u32 = 1;
    engine
        .set_cell_value(sheet, far_row, far_col, LiteralValue::Int(1))
        .unwrap();

    // Sanity: far value readable and other columns not densified.
    {
        let asheet = engine
            .sheet_store()
            .sheet(sheet)
            .expect("arrow sheet exists");
        let r0 = (far_row - 1) as usize;
        let av = asheet.range_view(r0, 0, r0, 0);
        assert_eq!(av.get_cell(0, 0), LiteralValue::Number(1.0));

        for c in 0..ncols {
            assert!(
                asheet.columns[c].total_chunk_count() <= 2,
                "unexpected pre-op densification (col={c}, chunks={}, sparse={})",
                asheet.columns[c].chunks.len(),
                asheet.columns[c].sparse_chunks.len()
            );
        }
    }

    // Insert 3 rows near the top; far value should shift down.
    engine.insert_rows(sheet, 10, 3).unwrap();
    let far_row_1 = far_row + 3;

    {
        let asheet = engine.sheet_store().sheet(sheet).unwrap();
        let r0 = (far_row_1 - 1) as usize;
        let av = asheet.range_view(r0, 0, r0, 0);
        assert_eq!(av.get_cell(0, 0), LiteralValue::Number(1.0));

        // Untouched columns should not be forced to allocate hundreds of empty chunks.
        for c in 0..ncols {
            assert!(
                asheet.columns[c].total_chunk_count() <= 6,
                "unexpected post-insert densification (col={c}, chunks={}, sparse={})",
                asheet.columns[c].chunks.len(),
                asheet.columns[c].sparse_chunks.len()
            );
        }
    }

    // Delete 2 rows near the top; far value should shift up.
    engine.delete_rows(sheet, 5, 2).unwrap();
    let far_row_2 = far_row_1 - 2;

    {
        let asheet = engine.sheet_store().sheet(sheet).unwrap();
        let r0 = (far_row_2 - 1) as usize;
        let av = asheet.range_view(r0, 0, r0, 0);
        assert_eq!(av.get_cell(0, 0), LiteralValue::Number(1.0));

        for c in 0..ncols {
            assert!(
                asheet.columns[c].total_chunk_count() <= 8,
                "unexpected post-delete densification (col={c}, chunks={}, sparse={})",
                asheet.columns[c].chunks.len(),
                asheet.columns[c].sparse_chunks.len()
            );
        }
    }

    // Insert an empty column before column A; far value should shift right.
    engine.insert_columns(sheet, 1, 1).unwrap();
    let far_col_1 = far_col + 1;

    {
        let asheet = engine.sheet_store().sheet(sheet).unwrap();
        let r0 = (far_row_2 - 1) as usize;
        let c0 = (far_col_1 - 1) as usize;
        let av = asheet.range_view(r0, c0, r0, c0);
        assert_eq!(av.get_cell(0, 0), LiteralValue::Number(1.0));

        // New leading column should be empty and cheap.
        assert!(
            asheet.columns[0].total_chunk_count() <= 1,
            "inserted empty column unexpectedly dense"
        );
    }

    // Delete the inserted column; far value should shift back left.
    engine.delete_columns(sheet, 1, 1).unwrap();

    {
        let asheet = engine.sheet_store().sheet(sheet).unwrap();
        let r0 = (far_row_2 - 1) as usize;
        let av = asheet.range_view(r0, 0, r0, 0);
        assert_eq!(av.get_cell(0, 0), LiteralValue::Number(1.0));
    }
}
