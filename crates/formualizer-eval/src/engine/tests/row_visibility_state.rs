use crate::engine::row_visibility::{RowVisibilitySource, RowVisibilityState};

#[test]
fn set_get_single_row_hidden_state_is_source_specific() {
    let mut state = RowVisibilityState::default();

    assert!(!state.is_row_hidden(4, Some(RowVisibilitySource::Manual)));
    assert!(!state.is_row_hidden(4, Some(RowVisibilitySource::Filter)));
    assert!(!state.is_row_hidden(4, None));

    assert!(state.set_row_hidden(4, true, RowVisibilitySource::Manual));
    assert!(state.is_row_hidden(4, Some(RowVisibilitySource::Manual)));
    assert!(!state.is_row_hidden(4, Some(RowVisibilitySource::Filter)));
    assert!(state.is_row_hidden(4, None));

    assert!(state.set_row_hidden(4, true, RowVisibilitySource::Filter));
    assert!(state.is_row_hidden(4, Some(RowVisibilitySource::Manual)));
    assert!(state.is_row_hidden(4, Some(RowVisibilitySource::Filter)));
    assert!(state.is_row_hidden(4, None));

    assert!(state.set_row_hidden(4, false, RowVisibilitySource::Manual));
    assert!(!state.is_row_hidden(4, Some(RowVisibilitySource::Manual)));
    assert!(state.is_row_hidden(4, Some(RowVisibilitySource::Filter)));
    assert!(state.is_row_hidden(4, None));
}

#[test]
fn set_range_and_version_behavior() {
    let mut state = RowVisibilityState::default();
    assert_eq!(state.version(), 0);

    assert!(state.set_rows_hidden(1, 3, true, RowVisibilitySource::Manual));
    assert_eq!(
        state.rows_hidden(1, 3, Some(RowVisibilitySource::Manual)),
        vec![true; 3]
    );
    assert_eq!(
        state.rows_hidden(1, 3, Some(RowVisibilitySource::Filter)),
        vec![false; 3]
    );
    assert_eq!(state.version(), 1);

    // No-op write should not bump version.
    assert!(!state.set_rows_hidden(1, 3, true, RowVisibilitySource::Manual));
    assert_eq!(state.version(), 1);

    assert!(state.set_rows_hidden(2, 2, true, RowVisibilitySource::Filter));
    assert_eq!(state.version(), 2);
}

#[test]
fn insert_delete_rows_shift_hidden_bits() {
    let mut state = RowVisibilityState::default();

    state.set_row_hidden(1, true, RowVisibilitySource::Manual); // row 2
    state.set_row_hidden(4, true, RowVisibilitySource::Manual); // row 5
    state.set_row_hidden(3, true, RowVisibilitySource::Filter); // row 4

    // Insert two rows before row 4 (0-based row 3).
    assert!(state.insert_rows(3, 2));

    assert!(state.is_row_hidden(1, Some(RowVisibilitySource::Manual))); // unchanged
    assert!(state.is_row_hidden(6, Some(RowVisibilitySource::Manual))); // shifted from 4 -> 6
    assert!(state.is_row_hidden(5, Some(RowVisibilitySource::Filter))); // shifted from 3 -> 5

    // Delete 3 rows starting at row 3 (0-based row 2).
    assert!(state.delete_rows(2, 3));

    assert!(state.is_row_hidden(1, Some(RowVisibilitySource::Manual))); // unchanged
    assert!(!state.is_row_hidden(5, Some(RowVisibilitySource::Filter))); // deleted by range
    assert!(state.is_row_hidden(3, Some(RowVisibilitySource::Manual))); // shifted down from 6 -> 3
}
