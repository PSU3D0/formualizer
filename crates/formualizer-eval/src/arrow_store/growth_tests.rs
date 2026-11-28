//! Geometric chunk growth (`ChunkReserve`): a sheet grown a row at a time
//! must be indistinguishable from one grown by rebuilding each lane at its
//! exact length, which is what `ensure_row_capacity` did before, and must not
//! pay a rebuild per row.

use super::*;
use crate::engine::DateSystem;
use arrow_array::Array;
use proptest::prelude::*;

const CHUNK_ROWS: usize = 8;

/// `ensure_row_capacity` as it was before chunks reserved capacity: every
/// materialized chunk is rebuilt at exactly its boundary-derived length.
fn ensure_row_capacity_exact(sheet: &mut ArrowSheet, target_rows: usize) {
    if target_rows as u32 <= sheet.nrows {
        return;
    }
    let chunk_size = sheet.chunk_rows.max(1);
    if sheet.chunk_starts.is_empty() {
        sheet.chunk_starts.push(0);
    }
    let mut next_start =
        (sheet.chunk_starts.last().copied().unwrap_or(0) + chunk_size).max(sheet.nrows as usize);
    while next_start < target_rows {
        sheet.chunk_starts.push(next_start);
        next_start += chunk_size;
    }
    sheet.nrows = target_rows as u32;
    let starts = sheet.chunk_starts.clone();
    let required = |idx: usize| {
        let start = *starts.get(idx)?;
        let end = starts.get(idx + 1).copied().unwrap_or(target_rows);
        Some(end.saturating_sub(start))
    };
    for col in &mut sheet.columns {
        for (idx, ch) in col.chunks.iter_mut().enumerate() {
            if let Some(req) = required(idx) {
                ch.grow_len_to(req);
            }
        }
        for (idx, ch) in col.sparse_chunks.iter_mut() {
            if let Some(req) = required(*idx) {
                ch.grow_len_to(req);
            }
        }
    }
}

fn value_at(seed: usize) -> LiteralValue {
    match seed % 6 {
        0 => LiteralValue::Number(seed as f64 + 0.5),
        1 => LiteralValue::Text(format!("t{seed}")),
        2 => LiteralValue::Boolean(seed.is_multiple_of(4)),
        3 => LiteralValue::Empty,
        4 => LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
        _ => LiteralValue::Date(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1 + (seed % 28) as u32).unwrap(),
        ),
    }
}

fn seeded_sheet(rows: usize, cols: usize) -> ArrowSheet {
    let mut ingest = IngestBuilder::new("S", cols, CHUNK_ROWS, DateSystem::Excel1900);
    for r in 0..rows {
        let row: Vec<LiteralValue> = (0..cols).map(|c| value_at(r * 7 + c * 3)).collect();
        ingest.append_row(&row).unwrap();
    }
    ingest.finish()
}

fn lane_data(lane: Option<&dyn Array>) -> Option<arrow_array::ArrayRef> {
    lane.map(|a| a.slice(0, a.len()))
}

/// Everything a reader can observe about a chunk's base lanes.
fn assert_chunks_equal(a: &ColumnChunk, b: &ColumnChunk, at: &str) {
    assert_eq!(a.len(), b.len(), "{at}: len");
    assert_eq!(a.meta.len, b.meta.len, "{at}: meta.len");
    assert_eq!(a.type_tag.values(), b.type_tag.values(), "{at}: tags");
    assert_eq!(
        lane_data(a.numbers.as_deref().map(|x| x as &dyn Array)),
        lane_data(b.numbers.as_deref().map(|x| x as &dyn Array)),
        "{at}: numbers"
    );
    assert_eq!(
        lane_data(a.booleans.as_deref().map(|x| x as &dyn Array)),
        lane_data(b.booleans.as_deref().map(|x| x as &dyn Array)),
        "{at}: booleans"
    );
    assert_eq!(
        lane_data(a.errors.as_deref().map(|x| x as &dyn Array)),
        lane_data(b.errors.as_deref().map(|x| x as &dyn Array)),
        "{at}: errors"
    );
    assert_eq!(
        lane_data(a.text.as_deref()),
        lane_data(b.text.as_deref()),
        "{at}: text"
    );
    let ids = |ch: &ColumnChunk| ch.format.as_ref().map(|f| f.to_ids(ch.len()));
    let general =
        |ids: Option<Vec<u16>>, len: usize| ids.unwrap_or_else(|| vec![FormatId::GENERAL.0; len]);
    assert_eq!(
        general(ids(a), a.len()),
        general(ids(b), b.len()),
        "{at}: formats"
    );
}

/// The store's invariant, and equality with the exactly-grown twin.
fn assert_same(grown: &ArrowSheet, exact: &ArrowSheet, step: &str) {
    assert_eq!(grown.nrows, exact.nrows, "{step}: nrows");
    assert_eq!(
        grown.chunk_starts, exact.chunk_starts,
        "{step}: chunk_starts"
    );
    assert_eq!(grown.columns.len(), exact.columns.len(), "{step}: columns");
    for (ci, (a, b)) in grown.columns.iter().zip(&exact.columns).enumerate() {
        assert_eq!(a.chunks.len(), b.chunks.len(), "{step}: col {ci} chunks");
        let mut sparse_a: Vec<_> = a.sparse_chunks.keys().copied().collect();
        let mut sparse_b: Vec<_> = b.sparse_chunks.keys().copied().collect();
        sparse_a.sort_unstable();
        sparse_b.sort_unstable();
        assert_eq!(sparse_a, sparse_b, "{step}: col {ci} sparse chunks");
        let indexes = (0..a.chunks.len()).chain(sparse_a);
        for idx in indexes {
            let at = format!("{step}: col {ci} chunk {idx}");
            let (ca, cb) = (a.chunk(idx).unwrap(), b.chunk(idx).unwrap());
            // A chunk's lanes are exactly its boundary-derived length: the
            // reserve never leaks into what readers see.
            let (_, extent) = grown.chunk_extent(idx).unwrap();
            assert_eq!(ca.len(), extent, "{at}: logical length");
            for lane_len in [
                ca.numbers.as_ref().map(|l| l.len()),
                ca.booleans.as_ref().map(|l| l.len()),
                ca.errors.as_ref().map(|l| l.len()),
                ca.text.as_ref().map(|l| l.len()),
            ]
            .into_iter()
            .flatten()
            {
                assert_eq!(lane_len, extent, "{at}: lane length");
            }
            assert_chunks_equal(ca, cb, &at);
        }
    }
    for r in 0..grown.nrows as usize + 2 {
        for c in 0..grown.columns.len() {
            assert_eq!(
                grown.get_cell_value(r, c),
                exact.get_cell_value(r, c),
                "{step}: cell ({r}, {c})"
            );
            assert_eq!(
                grown.format_id(r, c),
                exact.format_id(r, c),
                "{step}: format ({r}, {c})"
            );
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    Grow(usize),
    Write { row: usize, col: usize, seed: usize },
    Compact { col: usize, chunk: usize },
    InsertRows { before: usize, count: usize },
    DeleteRows { start: usize, count: usize },
    InsertColumns { before: usize, count: usize },
    DeleteColumns { start: usize },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (1usize..=5).prop_map(Op::Grow),
        1 => (6usize..=20).prop_map(Op::Grow),
        4 => (0usize..64, 0usize..5, 0usize..1000)
            .prop_map(|(row, col, seed)| Op::Write { row, col, seed }),
        2 => (0usize..5, 0usize..8).prop_map(|(col, chunk)| Op::Compact { col, chunk }),
        1 => (0usize..64, 1usize..=10).prop_map(|(before, count)| Op::InsertRows { before, count }),
        1 => (0usize..64, 1usize..=10).prop_map(|(start, count)| Op::DeleteRows { start, count }),
        1 => (0usize..5, 1usize..=2).prop_map(|(before, count)| Op::InsertColumns { before, count }),
        1 => (0usize..5).prop_map(|start| Op::DeleteColumns { start }),
    ]
}

/// Applies `op` to `sheet`, growing with `grow`; everything else is shared.
fn apply(sheet: &mut ArrowSheet, op: &Op, grow: fn(&mut ArrowSheet, usize)) {
    match *op {
        Op::Grow(by) => grow(sheet, sheet.nrows as usize + by),
        Op::Write { row, col, seed } => {
            if sheet.nrows == 0 || sheet.columns.is_empty() {
                return;
            }
            let (row, col) = (row % sheet.nrows as usize, col % sheet.columns.len());
            let (ch_idx, off) = sheet.chunk_of_row(row).unwrap();
            let value = OverlayValue::from_literal_value(&value_at(seed), DateSystem::Excel1900);
            let ch = sheet.ensure_column_chunk_mut(col, ch_idx).unwrap();
            ch.overlay.set_scalar(off, value);
        }
        Op::Compact { col, chunk } => {
            // Thresholds of zero: fold any overlay into the base lanes, which
            // replaces them and orphans a reserve.
            sheet.maybe_compact_chunk(col, chunk, 0, 1);
        }
        Op::InsertRows { before, count } => {
            sheet.insert_rows(before % (sheet.nrows as usize + 1), count)
        }
        Op::DeleteRows { start, count } => {
            if sheet.nrows > 0 {
                sheet.delete_rows(start % sheet.nrows as usize, count);
            }
        }
        Op::InsertColumns { before, count } => {
            if sheet.columns.len() < 6 {
                sheet.insert_columns(before % (sheet.columns.len() + 1), count);
            }
        }
        Op::DeleteColumns { start } => {
            if sheet.columns.len() > 1 {
                sheet.delete_columns(start % sheet.columns.len(), 1);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Any interleaving of row-at-a-time growth with overlay writes,
    /// compaction and structural edits leaves the reserving sheet identical,
    /// lane for lane, to one grown by exact rebuilds.
    #[test]
    fn reserving_growth_is_indistinguishable_from_exact_growth(
        rows in 0usize..20,
        cols in 0usize..4,
        ops in proptest::collection::vec(op(), 1..60),
    ) {
        let mut grown = seeded_sheet(rows, cols);
        let mut exact = seeded_sheet(rows, cols);
        assert_same(&grown, &exact, "seed");
        for (i, op) in ops.iter().enumerate() {
            apply(&mut grown, op, |sheet, target| sheet.ensure_row_capacity(target));
            apply(&mut exact, op, ensure_row_capacity_exact);
            assert_same(&grown, &exact, &format!("after op {i} {op:?}"));
        }
    }
}

#[test]
fn row_at_a_time_growth_rebuilds_a_chunk_logarithmically() {
    // A full-size chunk grown one row at a time: the lanes are rebuilt only
    // when the reserve is exhausted, i.e. at each doubling.
    let chunk_rows = 32 * 1024;
    let mut ingest = IngestBuilder::new("S", 2, chunk_rows, DateSystem::Excel1900);
    ingest
        .append_row(&[LiteralValue::Number(1.0), LiteralValue::Text("a".into())])
        .unwrap();
    let mut sheet = ingest.finish();
    let backing = |sheet: &ArrowSheet| sheet.columns[0].chunks[0].type_tag.values().as_ptr();
    let mut rebuilds = 0;
    let mut last = backing(&sheet);
    for rows in 2..=chunk_rows + 100 {
        sheet.ensure_row_capacity(rows);
        if rows <= chunk_rows && backing(&sheet) != last {
            rebuilds += 1;
            last = backing(&sheet);
        }
    }
    // 16, 32, ..., 32768: twelve doublings, not 32,767 rebuilds.
    assert!(rebuilds <= 12, "{rebuilds} rebuilds");
    assert_eq!(sheet.nrows as usize, chunk_rows + 100);
    assert_eq!(sheet.chunk_starts, vec![0, chunk_rows]);
    for col in &sheet.columns {
        // The capacity is capped at a full chunk; nothing over-allocates past it.
        assert_eq!(col.chunks[0].len(), chunk_rows);
        // A full chunk cannot grow again, so it keeps no reserve.
        assert!(col.chunks[0].reserve.is_none());
    }
    assert_eq!(sheet.get_cell_value(0, 0), LiteralValue::Number(1.0));
    assert_eq!(sheet.get_cell_value(0, 1), LiteralValue::Text("a".into()));
    assert_eq!(
        sheet.get_cell_value(chunk_rows + 50, 0),
        LiteralValue::Empty
    );
}

#[test]
fn a_replaced_lane_orphans_the_reserve_instead_of_resurrecting_old_data() {
    let mut sheet = seeded_sheet(3, 1);
    sheet.ensure_row_capacity(4);
    assert!(sheet.columns[0].chunks[0].reserve_backs_lanes());
    // Code that replaces a base lane (compaction, bulk ingest) does not know
    // about the reserve.
    let replacement: Vec<Option<f64>> = vec![Some(10.0), Some(11.0), Some(12.0), Some(13.0)];
    let ch = &mut sheet.columns[0].chunks[0];
    ch.numbers = Some(Arc::new(Float64Array::from(replacement)));
    ch.type_tag = Arc::new(UInt8Array::from(vec![TypeTag::Number as u8; 4]));
    ch.text = None;
    ch.booleans = None;
    ch.errors = None;
    assert!(!ch.reserve_backs_lanes());
    sheet.ensure_row_capacity(5);
    for (row, expected) in [10.0, 11.0, 12.0, 13.0].into_iter().enumerate() {
        assert_eq!(sheet.get_cell_value(row, 0), LiteralValue::Number(expected));
    }
    assert_eq!(sheet.get_cell_value(4, 0), LiteralValue::Empty);
    assert_eq!(sheet.columns[0].chunks[0].len(), 5);
}

#[test]
fn growth_after_an_oversized_chunk_starts_the_next_chunk_after_its_rows() {
    // Inserting rows can leave a chunk longer than `chunk_rows`. Growing the
    // sheet used to start the next chunk at `start + chunk_rows`, inside that
    // chunk, orphaning its tail.
    let mut sheet = seeded_sheet(6, 1);
    sheet.insert_rows(2, 5);
    let before: Vec<LiteralValue> = (0..11).map(|r| sheet.get_cell_value(r, 0)).collect();
    let last_len = sheet.columns[0].chunks.last().unwrap().len();
    sheet.ensure_row_capacity(sheet.nrows as usize + 3);
    for (idx, col_chunk) in sheet.columns[0].chunks.iter().enumerate() {
        assert_eq!(
            col_chunk.len(),
            sheet.chunk_extent(idx).unwrap().1,
            "chunk {idx}"
        );
    }
    let after: Vec<LiteralValue> = (0..11).map(|r| sheet.get_cell_value(r, 0)).collect();
    assert_eq!(before, after);
    assert!(last_len > 0);
    assert_eq!(sheet.get_cell_value(13, 0), LiteralValue::Empty);
}

#[test]
fn format_runs_grow_in_place() {
    let mut runs = FormatRuns::from_ids(&[FormatId::DATE.0, FormatId::DATE.0]).unwrap();
    runs.grow_to(4);
    assert_eq!(
        runs.to_ids(4),
        vec![
            FormatId::DATE.0,
            FormatId::DATE.0,
            FormatId::GENERAL.0,
            FormatId::GENERAL.0
        ]
    );
    // Growing again extends the trailing General run rather than adding one.
    let before = runs.clone();
    runs.grow_to(6);
    assert_eq!(runs.to_ids(6)[..4], before.to_ids(4)[..]);
    assert_eq!(runs.to_ids(6)[4..], [FormatId::GENERAL.0; 2]);
    assert_eq!(runs.run_ends.len(), 2);
}

#[test]
fn a_lane_replaced_with_the_same_offsets_is_not_reverted() {
    // The replacement reuses the old lane's offsets and null buffers and only
    // differs in its bytes, so nothing but the lane's identity tells them
    // apart.
    let mut sheet = seeded_sheet(3, 1);
    sheet.ensure_row_capacity(4);
    let ch = &mut sheet.columns[0].chunks[0];
    let old = ch
        .text
        .as_ref()
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    let bytes: Vec<u8> = old.value_data().iter().map(|_| b'X').collect();
    let replacement = StringArray::new(
        old.offsets().clone(),
        arrow_buffer::Buffer::from_vec(bytes),
        old.nulls().cloned(),
    );
    let text_row = (0..3).find(|r| !old.is_null(*r)).unwrap();
    let expected = replacement.value(text_row).to_string();
    ch.text = Some(Arc::new(replacement));
    assert!(!ch.reserve_backs_lanes());
    sheet.ensure_row_capacity(5);
    assert_eq!(
        sheet.get_cell_value(text_row, 0),
        LiteralValue::Text(expected)
    );
}

#[test]
fn compaction_releases_the_reserve_and_the_lanes_it_retired() {
    let mut sheet = seeded_sheet(3, 1);
    sheet.ensure_row_capacity(4);
    let retired = Arc::downgrade(sheet.columns[0].chunks[0].text.as_ref().unwrap());
    let empty = OverlayValue::from_literal_value(&LiteralValue::Empty, DateSystem::Excel1900);
    for off in 0..4 {
        sheet.columns[0].chunks[0]
            .overlay
            .set_scalar(off, empty.clone());
    }
    assert!(sheet.maybe_compact_chunk(0, 0, 0, 1) > 0);
    let ch = &sheet.columns[0].chunks[0];
    assert!(ch.reserve.is_none());
    assert!(
        retired.upgrade().is_none(),
        "the old text lane is still owned"
    );
}

#[test]
fn planned_chunks_match_the_chunks_growth_creates() {
    // Planning code locates rows past the end of the sheet before the store
    // grows; both must agree, including after an oversized last chunk.
    for seed_rows in [0usize, 3, 8, 9] {
        for inserted in [0usize, 10] {
            let mut sheet = seeded_sheet(seed_rows, 1);
            if inserted > 0 {
                sheet.insert_rows(0, inserted);
            }
            let rows = sheet.nrows as usize;
            let planned: Vec<(usize, usize)> = (0..rows + 40)
                .map(|r| sheet.chunk_of_row_after_growth(r))
                .collect();
            let starts: Vec<usize> = (0..8).map(|i| sheet.chunk_start_after_growth(i)).collect();
            sheet.ensure_row_capacity(rows + 40);
            for (row, (idx, start)) in planned.into_iter().enumerate() {
                let (actual_idx, offset) = sheet.chunk_of_row(row).unwrap();
                assert_eq!((idx, start), (actual_idx, row - offset), "row {row}");
            }
            for (idx, start) in sheet.chunk_starts.iter().enumerate().take(8) {
                assert_eq!(starts[idx], *start, "chunk {idx}");
            }
        }
    }
}
