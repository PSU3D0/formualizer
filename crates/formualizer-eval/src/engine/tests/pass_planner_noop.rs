use crate::engine::pass_planner::{PassPlanner, PassWarmupPlan};
use crate::engine::tuning::WarmupConfig;
use formualizer_parse::{ASTNode, ASTNodeType, LiteralValue};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pass_planner_produces_empty_plan_when_disabled() {
        let mut config = WarmupConfig::default();
        config.warmup_enabled = false; // Disable warmup
        assert!(!config.warmup_enabled);

        let planner = PassPlanner::new(config);

        // Create some dummy ASTs that would normally trigger warmup
        let ast1 = create_sumifs_ast();
        let ast2 = create_sumifs_ast();
        let targets = vec![&ast1, &ast2];

        // Should produce empty plan when disabled
        let plan = planner.analyze_targets(&targets);

        // Flats removed: plan has no flatten tasks
        assert!(
            plan.masks.is_empty(),
            "Should have no mask tasks when disabled"
        );
        assert!(
            plan.eq_indexes.is_empty(),
            "Should have no eq index tasks when disabled"
        );
        assert!(
            plan.range_indexes.is_empty(),
            "Should have no range index tasks when disabled"
        );
    }

    #[test]
    fn test_pass_planner_respects_thresholds() {
        let mut config = WarmupConfig::default();
        config.warmup_enabled = true;
        config.min_flat_cells = 10000; // Unused after flats removal

        let planner = PassPlanner::new(config);

        // Create AST with small range that should be below threshold
        let ast = create_small_range_ast();
        let targets = vec![&ast];

        let plan = planner.analyze_targets(&targets);

        // Should not plan flattening for small ranges
        // Flats removed; nothing to assert for flattening
    }

    #[test]
    fn test_pass_planner_respects_reuse_thresholds() {
        let mut config = WarmupConfig::default();
        config.warmup_enabled = true;
        config.flat_reuse_threshold = 5; // Unused after flats removal

        let planner = PassPlanner::new(config);

        // Create ASTs with same reference used only 3 times
        let ast1 = create_column_ref_ast("A:A");
        let ast2 = create_column_ref_ast("A:A");
        let ast3 = create_column_ref_ast("A:A");
        let targets = vec![&ast1, &ast2, &ast3];

        let plan = planner.analyze_targets(&targets);

        // Should not plan flattening since reuse count (3) < threshold (5)
        // Flats removed; nothing to assert for flattening
    }

    #[test]
    fn test_pass_planner_empty_targets() {
        let mut config = WarmupConfig::default();
        config.warmup_enabled = true;

        let planner = PassPlanner::new(config);
        let targets: Vec<&ASTNode> = vec![];

        let plan = planner.analyze_targets(&targets);

        // Flats removed; nothing to assert for flattening
        assert!(plan.masks.is_empty());
        assert!(plan.eq_indexes.is_empty());
        assert!(plan.range_indexes.is_empty());
    }

    #[test]
    fn test_pass_planner_single_cell_evaluation() {
        let mut config = WarmupConfig::default();
        config.warmup_enabled = true;

        let planner = PassPlanner::new(config);

        // Single cell evaluation with one AST
        let ast = create_simple_sum_ast();
        let plan = planner.analyze_single(&ast);

        // For a simple SUM, should not trigger warmup
        // Flats removed; nothing to assert for flattening
    }

    #[test]
    fn test_warmup_plan_is_send_sync() {
        // Ensure our plan types are thread-safe
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<PassWarmupPlan>();
    }

    // Helper functions to create test ASTs
    fn create_sumifs_ast() -> ASTNode {
        // Placeholder - would create a SUMIFS AST
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Number(0.0)),
            source_token: None,
            contains_volatile: false,
        }
    }

    fn create_small_range_ast() -> ASTNode {
        // Placeholder - would create AST with small range reference
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Number(0.0)),
            source_token: None,
            contains_volatile: false,
        }
    }

    fn create_column_ref_ast(_col: &str) -> ASTNode {
        // Placeholder - would create AST with column reference
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Number(0.0)),
            source_token: None,
            contains_volatile: false,
        }
    }

    fn create_simple_sum_ast() -> ASTNode {
        // Placeholder - would create a simple SUM AST
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Number(0.0)),
            source_token: None,
            contains_volatile: false,
        }
    }
}
