#[cfg(test)]
mod region_lock_tests {
    use formualizer_parse::{ExcelErrorKind, LiteralValue};

    use crate::engine::eval::ShimSpillManager;
    use crate::engine::spill::{Region, RegionLockManager, SpillMeta, SpillShape};
    use crate::engine::vertex::VertexId;
    use crate::engine::{Engine, EvalConfig};

    #[test]
    fn region_lock_overlap_fails() {
        let mut mgr = RegionLockManager::default();
        let a = Region {
            sheet_id: 1,
            row_start: 1,
            row_end: 2,
            col_start: 1,
            col_end: 3,
        };
        let b = Region {
            sheet_id: 1,
            row_start: 2,
            row_end: 3,
            col_start: 3,
            col_end: 4,
        }; // Overlaps at (2,3)
        let id1 = mgr.reserve(a, VertexId(1)).expect("first reserve ok");
        assert!(id1 > 0);
        let err = mgr.reserve(b, VertexId(2)).unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Spill);
        // Release to avoid leaks in test
        mgr.release(id1);
    }

    #[test]
    fn spill_lock_released_on_plan_failure() {
        // Set up a tiny engine and graph with a blocking value to force plan failure
        let wb = crate::test_workbook::TestWorkbook::new();
        let mut engine = Engine::new(wb, EvalConfig::default());

        // Anchor at Sheet1!A1
        engine
            .set_cell_formula(
                "Sheet1",
                1,
                1,
                formualizer_parse::parser::parse("={1,2}").unwrap(),
            )
            .unwrap();

        // Place a blocking value at A1's right neighbor (A1 spill includes B1)
        engine
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("X".into()))
            .unwrap();

        // Build inputs for shim directly
        let anchor_vertex = engine
            .graph
            .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 1))
            .copied()
            .expect("anchor vertex");
        let anchor_cell = engine.graph.make_cell_ref("Sheet1", 1, 1);
        let mut shim = ShimSpillManager::default();

        // Acquire in-flight lock
        shim.reserve(
            anchor_vertex,
            anchor_cell,
            SpillShape { rows: 1, cols: 2 },
            SpillMeta {
                epoch: engine.recalc_epoch,
                config: engine.config.spill,
            },
        )
        .expect("reserve ok");
        assert!(!shim.active_locks.is_empty());

        // Commit path will plan and fail due to blocker; shim should release lock on error
        let targets = vec![
            engine.graph.make_cell_ref("Sheet1", 1, 1),
            engine.graph.make_cell_ref("Sheet1", 1, 2),
        ];
        let values = vec![vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)]];
        let res = shim.commit_array(&mut engine.graph, anchor_vertex, &targets, values);
        assert!(res.is_err());
        assert!(shim.active_locks.is_empty());
    }
}
