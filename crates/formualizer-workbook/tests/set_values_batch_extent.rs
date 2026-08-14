//! `set_values` pre-allocates Arrow capacity for the batch, and that
//! pre-allocation must reserve exactly the cells the batch writes.
//!
//! Trailing empty rows write nothing. Reserving a row for them inflates the
//! reported sheet extent, and for a batch anchored at the last grid row it
//! reserves a row that cannot exist at all.

use formualizer_common::LiteralValue as LV;
use formualizer_workbook::Workbook;

const LAST_GRID_ROW: u32 = 1_048_576;

fn wb() -> Workbook {
    let mut w = Workbook::new();
    w.add_sheet("S").unwrap();
    w
}

#[test]
fn trailing_empty_rows_do_not_extend_the_sheet() {
    let mut w = wb();
    w.set_values("S", 1, 1, &[vec![LV::Number(1.0)], vec![], vec![]])
        .unwrap();
    assert_eq!(
        w.sheet_dimensions("S"),
        Some((1, 1)),
        "two trailing empty rows wrote nothing, so the extent is row 1"
    );
}

#[test]
fn batch_at_the_last_grid_row_does_not_reserve_past_it() {
    let mut w = wb();
    w.set_values("S", LAST_GRID_ROW, 1, &[vec![LV::Number(11.0)], vec![]])
        .unwrap();
    assert_eq!(
        w.sheet_dimensions("S"),
        Some((LAST_GRID_ROW, 1)),
        "the trailing empty row must not reserve row {}",
        LAST_GRID_ROW + 1
    );
    assert_eq!(w.get_value("S", LAST_GRID_ROW, 1), Some(LV::Number(11.0)));
}

#[test]
fn interior_empty_row_still_reserves_through_the_last_populated_row() {
    let mut w = wb();
    w.set_values(
        "S",
        1,
        1,
        &[vec![LV::Number(1.0)], vec![], vec![LV::Number(3.0)]],
    )
    .unwrap();
    assert_eq!(w.sheet_dimensions("S"), Some((3, 1)));
    assert_eq!(w.get_value("S", 3, 1), Some(LV::Number(3.0)));
    assert_eq!(w.get_value("S", 2, 1), None, "interior gap stays empty");
}

#[test]
fn ragged_widths_reserve_the_widest_row_but_write_no_extra_cells() {
    let mut w = wb();
    w.set_values(
        "S",
        1,
        1,
        &[
            vec![LV::Number(1.0), LV::Number(2.0), LV::Number(3.0)],
            vec![LV::Number(4.0)],
        ],
    )
    .unwrap();
    assert_eq!(w.sheet_dimensions("S"), Some((2, 3)));
    assert_eq!(w.get_value("S", 2, 2), None);
    assert_eq!(w.get_value("S", 2, 3), None);
}

#[test]
fn all_rows_empty_writes_nothing() {
    let mut w = wb();
    w.set_values("S", 1, 1, &[vec![], vec![]]).unwrap();
    assert_eq!(w.sheet_dimensions("S"), Some((0, 0)));
}

// The same pre-allocation lives in `set_formulas_inner`, where the flawed
// extent computation predates the `set_values` batch path entirely.

#[test]
fn set_formulas_trailing_empty_rows_do_not_extend_the_sheet() {
    let mut w = wb();
    w.set_formulas("S", 1, 1, &[vec!["=1".to_string()], vec![], vec![]])
        .unwrap();
    assert_eq!(w.sheet_dimensions("S"), Some((1, 1)));
}

#[test]
fn set_formulas_at_the_last_grid_row_does_not_reserve_past_it() {
    let mut w = wb();
    w.set_formulas("S", LAST_GRID_ROW, 1, &[vec!["=13".to_string()], vec![]])
        .unwrap();
    assert_eq!(w.sheet_dimensions("S"), Some((LAST_GRID_ROW, 1)));
}
