use formualizer_workbook::{LiteralValue, Workbook};

#[test]
fn linked_workbook_sheet_writes_are_rejected_without_creating_a_sheet() {
    let mut workbook = Workbook::new();
    for sheet in ["[1]Data", "[12]My Sheet", "[0]Data"] {
        assert!(
            workbook
                .set_value(sheet, 1, 1, LiteralValue::Number(7.0))
                .is_err()
        );
        assert!(workbook.engine().sheet_id(sheet).is_none());
    }
    for sheet in ["Data", "[x]Data", "[]Data", "prefix[1]Data"] {
        workbook
            .set_value(sheet, 1, 1, LiteralValue::Number(7.0))
            .unwrap();
        assert!(workbook.engine().sheet_id(sheet).is_some());
    }
}
