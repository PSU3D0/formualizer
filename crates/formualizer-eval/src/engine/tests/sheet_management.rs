#[cfg(test)]
mod tests {
    use crate::engine::DependencyGraph;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parse;

    #[test]
    fn test_add_sheet() {
        let mut graph = DependencyGraph::new();

        // Add a new sheet
        let sheet2_id = graph.add_sheet("Sheet2").unwrap();
        assert!(sheet2_id > 0);

        // Add the same sheet again (should be idempotent)
        let sheet2_id_again = graph.add_sheet("Sheet2").unwrap();
        assert_eq!(sheet2_id, sheet2_id_again);

        // Verify we can use the new sheet
        graph
            .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(42.0))
            .unwrap();
        let value = graph.get_cell_value("Sheet2", 1, 1).unwrap();
        assert_eq!(value, LiteralValue::Number(42.0));
    }

    #[test]
    fn test_remove_sheet() {
        let mut graph = DependencyGraph::new();

        // Add sheets
        let sheet2_id = graph.add_sheet("Sheet2").unwrap();
        graph.add_sheet("Sheet3").unwrap();

        // Add some data to Sheet2
        graph
            .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(10.0))
            .unwrap();

        // Add a formula in Sheet1 that references Sheet2
        let formula = parse("=Sheet2!A1 * 2").unwrap();
        graph.set_cell_formula("Sheet1", 1, 2, formula).unwrap();

        // Remove Sheet2
        graph.remove_sheet(sheet2_id).unwrap();

        // The formula in Sheet1 should now have a #REF! error
        // (This would be verified after evaluation)

        // Sheet2 should no longer exist
        assert!(graph.sheet_id("Sheet2").is_none());

        // Cannot remove the last sheet
        let sheet1_id = graph.sheet_id("Sheet1").unwrap();
        let sheet3_id = graph.sheet_id("Sheet3").unwrap();
        graph.remove_sheet(sheet3_id).unwrap();

        // Now trying to remove Sheet1 should fail
        let result = graph.remove_sheet(sheet1_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_sheet() {
        let mut graph = DependencyGraph::new();

        // Add a sheet and some data
        let sheet2_id = graph.add_sheet("Sheet2").unwrap();
        graph
            .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(5.0))
            .unwrap();

        // Add a formula in Sheet1 that references Sheet2
        let formula = parse("=Sheet2!A1 + 10").unwrap();
        graph.set_cell_formula("Sheet1", 2, 1, formula).unwrap();

        // Rename Sheet2
        graph.rename_sheet(sheet2_id, "DataSheet").unwrap();

        // Verify the rename worked
        assert!(graph.sheet_id("Sheet2").is_none());
        assert!(graph.sheet_id("DataSheet").is_some());

        // The data should still be accessible with the new name
        let value = graph.get_cell_value("DataSheet", 1, 1).unwrap();
        assert_eq!(value, LiteralValue::Number(5.0));

        // Cannot rename to an existing name
        let result = graph.rename_sheet(sheet2_id, "Sheet1");
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_sheet() {
        let mut graph = DependencyGraph::new();

        // Set up source sheet with data and formulas
        graph.add_sheet("Source").unwrap();
        graph
            .set_cell_value("Source", 1, 1, LiteralValue::Number(10.0))
            .unwrap();
        graph
            .set_cell_value("Source", 2, 1, LiteralValue::Number(20.0))
            .unwrap();

        // Add an internal formula (references within the same sheet)
        let formula = parse("=A1 + A2").unwrap();
        graph.set_cell_formula("Source", 3, 1, formula).unwrap();

        // Add a cross-sheet reference
        graph
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0))
            .unwrap();
        let cross_formula = parse("=Sheet1!A1 * 2").unwrap();
        graph
            .set_cell_formula("Source", 4, 1, cross_formula)
            .unwrap();

        // Duplicate the sheet
        let source_id = graph.sheet_id("Source").unwrap();
        let copy_id = graph.duplicate_sheet(source_id, "SourceCopy").unwrap();
        assert!(copy_id != source_id);

        // Verify all data was copied
        assert_eq!(
            graph.get_cell_value("SourceCopy", 1, 1).unwrap(),
            LiteralValue::Number(10.0)
        );
        assert_eq!(
            graph.get_cell_value("SourceCopy", 2, 1).unwrap(),
            LiteralValue::Number(20.0)
        );

        // Internal references should point to the new sheet
        // Cross-sheet references should remain unchanged
        // (These would be verified after evaluation)

        // Cannot duplicate to an existing name
        let result = graph.duplicate_sheet(source_id, "Sheet1");
        assert!(result.is_err());
    }

    #[test]
    fn test_sheet_management_edge_cases() {
        let mut graph = DependencyGraph::new();

        // Test empty sheet name
        let result = graph.add_sheet("");
        assert!(result.is_ok()); // Empty names might be allowed depending on implementation

        // Test very long sheet name
        let long_name = "A".repeat(256);
        let result = graph.rename_sheet(0, &long_name);
        assert!(result.is_err());

        // Test renaming non-existent sheet
        let result = graph.rename_sheet(999, "NewName");
        assert!(result.is_err());

        // Test duplicating non-existent sheet
        let result = graph.duplicate_sheet(999, "Copy");
        assert!(result.is_err());
    }

    #[test]
    fn test_current_sheet_resolution_for_unsheeted_refs() {
        use crate::engine::{Engine, EvalConfig};
        use crate::test_workbook::TestWorkbook;

        let wb = TestWorkbook::new();
        let mut engine = Engine::new(wb, EvalConfig::default());
        engine.graph.add_sheet("Data").unwrap();

        engine
            .set_cell_value("Data", 1, 1, LiteralValue::Number(10.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(99.0))
            .unwrap();

        engine
            .set_cell_formula("Data", 1, 2, parse("=A1").unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();

        assert_eq!(
            engine.get_cell_value("Data", 1, 2),
            Some(LiteralValue::Number(10.0))
        );
    }

    #[test]
    fn test_unknown_sheet_reference_errors_on_resolve() {
        use crate::engine::{Engine, EvalConfig};
        use crate::test_workbook::TestWorkbook;
        use crate::traits::EvaluationContext;
        use formualizer_parse::parser::ReferenceType;

        let wb = TestWorkbook::new();
        let engine = Engine::new(wb, EvalConfig::default());
        let r = ReferenceType::cell(Some("NoSuchSheet".to_string()), 1, 1);
        assert!(engine.resolve_range_view(&r, "Sheet1").is_err());
    }
}
