//! Regression tests for Red-Team B5 (lazy column allocation + write-path
//! `WorkbookLoadLimits` enforcement) and E1 (chunk allocation aligned with
//! `chunk_starts`).

use formualizer_common::LiteralValue;
use formualizer_eval::arrow_store::IngestBuilder;
use formualizer_eval::engine::DateSystem;
use formualizer_workbook::{CellData, Workbook, WorkbookConfig, WorkbookLoadLimits};
use std::collections::BTreeMap;

#[test]
fn b5_far_right_write_on_dense_30k_row_sheet_does_not_densify_intermediate_columns() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    // Seed a 30,000-row, 1-column dense sheet (reproducing Red-Team B5).
    let mut builder = IngestBuilder::new("S", 1, 32 * 1024, DateSystem::Excel1900);
    for r in 0..30_000 {
        builder
            .append_row(&[LiteralValue::Number(r as f64)])
            .unwrap();
    }
    let dense_sheet = builder.finish();
    {
        let store = wb.engine_mut().sheet_store_mut();
        if let Some(pos) = store.sheets.iter().position(|s| s.name.as_ref() == "S") {
            store.sheets[pos] = dense_sheet;
        } else {
            store.sheets.push(dense_sheet);
        }
    }

    // Write one far-right cell at column 2000.
    wb.set_value("S", 1, 2000, LiteralValue::Number(42.0))
        .unwrap();
    assert_eq!(wb.get_value("S", 1, 2000), Some(LiteralValue::Number(42.0)));
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(0.0)));
    assert_eq!(
        wb.get_value("S", 30_000, 1),
        Some(LiteralValue::Number(29_999.0))
    );

    let asheet = wb.engine().sheet_store().sheet("S").unwrap();
    assert_eq!(asheet.columns.len(), 2000);
    // Intermediate columns 1..1999 must have ZERO allocated chunks (neither dense nor sparse).
    for col in &asheet.columns[1..1999] {
        assert_eq!(
            col.total_chunk_count(),
            0,
            "untouched column {} must remain lazy",
            col.index
        );
    }
    // Total type_tag bytes across the whole sheet should stay ~60 KB (2 chunks of 30k),
    // not ~60 MB (2000 chunks of 30k).
    let total_tag_bytes: usize = asheet
        .columns
        .iter()
        .map(|col| {
            col.chunks
                .iter()
                .chain(col.sparse_chunks.values())
                .map(|ch| ch.type_tag.len())
                .sum::<usize>()
        })
        .sum();
    assert!(
        total_tag_bytes <= 100_000,
        "expected <= 100 KB tag bytes, got {total_tag_bytes}"
    );
}

#[test]
fn e1_large_set_values_chunks_align_with_chunk_starts() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb.add_sheet("S").unwrap();

    // 100,000 x 1 batch spans 4 chunks (chunk_rows = 32768).
    let rows: Vec<Vec<LiteralValue>> = (0..100_000)
        .map(|r| vec![LiteralValue::Number(r as f64)])
        .collect();
    wb.set_values("S", 1, 1, &rows).unwrap();

    let asheet = wb.engine().sheet_store().sheet("S").unwrap();
    assert_eq!(asheet.nrows, 100_000);
    assert_eq!(asheet.chunk_starts, vec![0, 32_768, 65_536, 98_304]);

    let col0 = &asheet.columns[0];
    assert!(
        col0.sparse_chunks.is_empty(),
        "write-built column starting at row 1 must stay in dense chunks (F4)"
    );
    assert_eq!(col0.chunks.len(), 4);
    for (idx, &start) in asheet.chunk_starts.iter().enumerate() {
        let end = asheet
            .chunk_starts
            .get(idx + 1)
            .copied()
            .unwrap_or(asheet.nrows as usize);
        let expected_len = end - start;
        let ch = col0.chunk(idx).expect("written chunk must exist");
        assert_eq!(
            ch.type_tag.len(),
            expected_len,
            "chunk {idx} length must match chunk_starts boundary"
        );
    }

    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(0.0)));
    assert_eq!(
        wb.get_value("S", 32_769, 1),
        Some(LiteralValue::Number(32_768.0))
    );
    assert_eq!(
        wb.get_value("S", 100_000, 1),
        Some(LiteralValue::Number(99_999.0))
    );
}

#[test]
fn b5_write_paths_enforce_workbook_load_limits_before_allocating() {
    let limits = WorkbookLoadLimits {
        max_sheet_rows: 100,
        max_sheet_cols: 20,
        max_sheet_logical_cells: 400,
        ..WorkbookLoadLimits::default()
    };
    let mut wb =
        Workbook::new_with_config(WorkbookConfig::interactive().with_ingest_limits(limits));
    wb.add_sheet("S").unwrap();

    // Row limit enforced on set_value / set_formula / set_values / set_formulas / write_range.
    let row_err = wb
        .set_value("S", 101, 1, LiteralValue::Number(1.0))
        .unwrap_err();
    let row_msg = row_err.to_string().to_lowercase();
    assert!(
        row_msg.contains("write") && !row_msg.contains("load budget"),
        "expected write-specific error message, got: {row_err}"
    );
    assert!(wb.set_formula("S", 101, 1, "=1").is_err());
    assert!(
        wb.set_values("S", 101, 1, &[vec![LiteralValue::Number(1.0)]])
            .is_err()
    );
    assert!(wb.set_formulas("S", 101, 1, &[vec!["=1".into()]]).is_err());
    let mut range_cells = BTreeMap::new();
    range_cells.insert((101, 1), CellData::from_value(1.0));
    assert!(wb.write_range("S", (1, 1), range_cells).is_err());

    // Column limit enforced.
    let col_err = wb
        .set_value("S", 1, 21, LiteralValue::Number(1.0))
        .unwrap_err();
    let col_msg = col_err.to_string().to_lowercase();
    assert!(
        col_msg.contains("write") && !col_msg.contains("load budget"),
        "expected write-specific error message, got: {col_err}"
    );
    assert!(
        wb.set_values("S", 1, 21, &[vec![LiteralValue::Number(1.0)]])
            .is_err()
    );

    // Per-batch cell budget enforced before allocation.
    let big_batch = vec![vec![LiteralValue::Number(1.0); 20]; 25];
    let batch_err = wb.set_values("S", 1, 1, &big_batch).unwrap_err();
    let batch_msg = batch_err.to_string().to_lowercase();
    assert!(
        batch_msg.contains("write") && !batch_msg.contains("load budget"),
        "expected write-specific error message, got: {batch_err}"
    );

    // No rejected write allocated rows or columns.
    assert_eq!(wb.sheet_dimensions("S"), Some((0, 0)));

    // Writes within limits succeed normally.
    let ok_batch = vec![vec![LiteralValue::Number(7.0); 10]; 20];
    wb.set_values("S", 1, 1, &ok_batch).unwrap();
    assert_eq!(wb.sheet_dimensions("S"), Some((20, 10)));
    assert_eq!(wb.get_value("S", 20, 10), Some(LiteralValue::Number(7.0)));
}

#[test]
fn f3_large_batch_inside_wide_existing_extent_succeeds_under_default_limits() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb.add_sheet("S").unwrap();

    // Fable's exact F3 sequence: 10,000x1, then far-right cell at col 16384,
    // then a 10,000x16 (160,000-cell) batch inside the existing extent.
    let col_a = vec![vec![LiteralValue::Number(1.0)]; 10_000];
    wb.set_values("S", 1, 1, &col_a).unwrap();
    wb.set_value("S", 1, 16_384, LiteralValue::Number(9.0))
        .unwrap();
    assert_eq!(wb.sheet_dimensions("S"), Some((10_000, 16_384)));

    let wide_block = vec![vec![LiteralValue::Number(2.0); 16]; 10_000];
    wb.set_values("S", 1, 1, &wide_block)
        .expect("160,000-cell write inside existing 10,000x16,384 extent must succeed");
    assert_eq!(
        wb.get_value("S", 10_000, 16),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        wb.get_value("S", 1, 16_384),
        Some(LiteralValue::Number(9.0))
    );

    // Reversed order (far-right cell first, then growing rows with a 10,000x16 block)
    // must also succeed under default limits.
    let mut wb2 = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb2.add_sheet("S").unwrap();
    wb2.set_value("S", 1, 16_384, LiteralValue::Number(9.0))
        .unwrap();
    wb2.set_values("S", 1, 1, &wide_block)
        .expect("160,000-cell write on wide lazy sheet must succeed");
    assert_eq!(
        wb2.get_value("S", 10_000, 16),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn f4_write_built_30k_sheet_keeps_written_columns_dense_for_sumifs() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb.add_sheet("S").unwrap();

    let rows: Vec<Vec<LiteralValue>> = (0..30_000)
        .map(|i| {
            vec![
                LiteralValue::Text(format!("k{}", i % 10)),
                LiteralValue::Number((i % 100) as f64),
            ]
        })
        .collect();
    wb.set_values("S", 1, 1, &rows).unwrap();
    wb.set_formula("S", 1, 3, r#"=SUMIFS($B$1:$B$30000,$A$1:$A$30000,"k3")"#)
        .unwrap();
    wb.set_formula("S", 2, 3, r#"=COUNTIFS($A$1:$A$30000,"k3")"#)
        .unwrap();
    wb.evaluate_all().unwrap();

    let expected_sum: f64 = (0..30_000)
        .filter(|i| i % 10 == 3)
        .map(|i| (i % 100) as f64)
        .sum();
    assert_eq!(
        wb.get_value("S", 1, 3),
        Some(LiteralValue::Number(expected_sum))
    );
    assert_eq!(wb.get_value("S", 2, 3), Some(LiteralValue::Number(3_000.0)));

    let asheet = wb.engine().sheet_store().sheet("S").unwrap();
    for col_idx in 0..2 {
        assert!(
            asheet.columns[col_idx].sparse_chunks.is_empty(),
            "write-built column {col_idx} must have no sparse chunks so CriteriaMaskCache stays active"
        );
        assert!(
            !asheet.columns[col_idx].chunks.is_empty(),
            "write-built column {col_idx} must keep dense chunks"
        );
    }
}
