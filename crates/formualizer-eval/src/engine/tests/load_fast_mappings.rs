use crate::engine::tests::common::abs_cell_ref;
use crate::engine::{DependencyGraph, EvalConfig};
use formualizer_common::PackedSheetCell;

#[test]
fn packed_load_mappings_flush_to_cell_lookup_single_sheet() {
    let mut graph = DependencyGraph::new_with_config(EvalConfig::default());
    let sid = graph.sheet_id_mut("Sheet1");
    graph.set_first_load_assume_new(true);

    let packed = vec![
        PackedSheetCell::try_new(sid, 0, 0).unwrap(),
        PackedSheetCell::try_new(sid, 9_999, 1).unwrap(),
        PackedSheetCell::try_new(sid, 20_000, 2).unwrap(),
    ];

    let (vids, added) = graph.ensure_vertices_batch_packed_ordered(&packed);
    assert_eq!(vids.len(), packed.len());
    assert_eq!(added.len(), packed.len());

    graph.set_first_load_assume_new(false);

    for (idx, cell) in packed.iter().enumerate() {
        let addr = abs_cell_ref(cell.sheet_id(), cell.row0() + 1, cell.col0() + 1);
        assert_eq!(graph.get_vertex_for_cell(&addr), Some(vids[idx]));
    }
}

#[test]
fn packed_load_mappings_flush_to_cell_lookup_multi_sheet() {
    let mut graph = DependencyGraph::new_with_config(EvalConfig::default());
    let s1 = graph.sheet_id_mut("Sheet1");
    let s2 = graph.sheet_id_mut("Data");
    graph.set_first_load_assume_new(true);

    let packed = vec![
        PackedSheetCell::try_new(s1, 0, 0).unwrap(),
        PackedSheetCell::try_new(s2, 4, 3).unwrap(),
        PackedSheetCell::try_new(s1, 12_345, 7).unwrap(),
        PackedSheetCell::try_new(s2, 20_000, 2).unwrap(),
    ];

    let (vids, added) = graph.ensure_vertices_batch_packed_ordered(&packed);
    assert_eq!(vids.len(), packed.len());
    assert_eq!(added.len(), packed.len());

    graph.set_first_load_assume_new(false);

    for (idx, cell) in packed.iter().enumerate() {
        let addr = abs_cell_ref(cell.sheet_id(), cell.row0() + 1, cell.col0() + 1);
        assert_eq!(graph.get_vertex_for_cell(&addr), Some(vids[idx]));
    }
}

#[test]
fn packed_load_mappings_reuse_existing_vertices_within_first_load() {
    let mut graph = DependencyGraph::new_with_config(EvalConfig::default());
    let sid = graph.sheet_id_mut("Sheet1");
    graph.set_first_load_assume_new(true);

    let first = vec![
        PackedSheetCell::try_new(sid, 0, 0).unwrap(),
        PackedSheetCell::try_new(sid, 1, 0).unwrap(),
    ];
    let (vids1, added1) = graph.ensure_vertices_batch_packed_ordered(&first);
    assert_eq!(added1.len(), 2);

    let second = vec![
        PackedSheetCell::try_new(sid, 1, 0).unwrap(),
        PackedSheetCell::try_new(sid, 2, 0).unwrap(),
    ];
    let (vids2, added2) = graph.ensure_vertices_batch_packed_ordered(&second);
    assert_eq!(added2.len(), 1, "only the new packed cell should allocate");
    assert_eq!(vids2[0], vids1[1]);

    graph.set_first_load_assume_new(false);

    let addr_existing = abs_cell_ref(sid, 2, 1);
    let addr_new = abs_cell_ref(sid, 3, 1);
    assert_eq!(graph.get_vertex_for_cell(&addr_existing), Some(vids1[1]));
    assert_eq!(graph.get_vertex_for_cell(&addr_new), Some(vids2[1]));
}
