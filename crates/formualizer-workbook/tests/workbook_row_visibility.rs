use formualizer_workbook::{IoError, Workbook};

#[test]
fn workbook_row_visibility_set_get_and_ranges() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    assert!(!wb.is_row_hidden("S", 2).unwrap());

    wb.set_row_hidden("S", 2, true).unwrap();
    assert!(wb.is_row_hidden("S", 2).unwrap());

    wb.set_rows_hidden("S", 4, 6, true).unwrap();
    for row in 4..=6 {
        assert!(wb.is_row_hidden("S", row).unwrap());
    }

    wb.set_rows_hidden("S", 5, 6, false).unwrap();
    assert!(wb.is_row_hidden("S", 4).unwrap());
    assert!(!wb.is_row_hidden("S", 5).unwrap());
    assert!(!wb.is_row_hidden("S", 6).unwrap());
}

#[test]
fn workbook_row_visibility_unknown_sheet_errors() {
    let mut wb = Workbook::new();
    assert!(wb.set_row_hidden("missing", 1, true).is_err());
    assert!(wb.set_rows_hidden("missing", 1, 2, true).is_err());
    assert!(wb.is_row_hidden("missing", 1).is_err());
}

#[test]
fn workbook_row_visibility_action_rollback_restores_row_visibility() {
    let mut wb = Workbook::new();
    wb.set_changelog_enabled(true);
    wb.add_sheet("S").unwrap();

    wb.set_row_hidden("S", 2, true).unwrap();

    let err = wb
        .action("row-visibility-rollback", |tx| -> Result<(), IoError> {
            tx.set_row_hidden("S", 2, false)?;
            tx.set_rows_hidden("S", 5, 6, true)?;
            Err(IoError::Backend {
                backend: "test".to_string(),
                message: "boom".to_string(),
            })
        })
        .unwrap_err();

    assert!(err.to_string().contains("boom"));

    assert!(wb.is_row_hidden("S", 2).unwrap());
    assert!(!wb.is_row_hidden("S", 5).unwrap());
    assert!(!wb.is_row_hidden("S", 6).unwrap());
}
