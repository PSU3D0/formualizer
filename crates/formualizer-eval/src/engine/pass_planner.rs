//! Global pass warmup planning (flats removed; masks/index placeholders)

use crate::engine::cache::CriteriaKey;
use crate::engine::tuning::WarmupConfig;
use formualizer_core::ASTNode;
use formualizer_core::parser::ReferenceType;

/// Plan for what to warm up before evaluation (flats removed)
#[derive(Debug, Clone, Default)]
pub struct PassWarmupPlan {
    pub masks: Vec<CriteriaKey>,
    pub eq_indexes: Vec<IndexKey>,
    pub range_indexes: Vec<IndexKey>,
}

/// Key for column index (Phase 4/5)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct IndexKey {
    pub sheet_id: u32,
    pub column: u32,
}

/// Information about hot criteria (placeholder)
#[derive(Debug, Clone)]
pub struct HotCriteria {
    pub key: CriteriaKey,
    pub count: usize,
}

/// Global pass planner
pub struct PassPlanner {
    config: WarmupConfig,
}

impl PassPlanner {
    pub fn new(config: WarmupConfig) -> Self {
        Self { config }
    }

    /// Analyze multiple target ASTs for batch evaluation (no flats)
    pub fn analyze_targets(&self, _targets: &[&ASTNode]) -> PassWarmupPlan {
        if !self.config.warmup_enabled {
            return PassWarmupPlan::default();
        }
        PassWarmupPlan {
            masks: Vec::new(),
            eq_indexes: Vec::new(),
            range_indexes: Vec::new(),
        }
    }

    /// Analyze single AST for single-cell evaluation (no flats)
    pub fn analyze_single(&self, _ast: &ASTNode) -> PassWarmupPlan {
        if !self.config.warmup_enabled {
            return PassWarmupPlan::default();
        }
        PassWarmupPlan {
            masks: Vec::new(),
            eq_indexes: Vec::new(),
            range_indexes: Vec::new(),
        }
    }

    /// Collect references from an AST (no-op for flats)
    fn collect_references(
        &self,
        _ast: &ASTNode,
        _counts: &mut std::collections::HashMap<String, (usize, usize)>,
    ) {
    }

    /// Recursively walk AST nodes (placeholder)
    fn walk_ast(
        &self,
        _node: &ASTNode,
        _counts: &mut std::collections::HashMap<String, (usize, usize)>,
    ) {
    }

    /// Collect criteria from IFS functions (Phase 3 placeholder)
    fn collect_criteria_from_ifs(&self, _args: &[ASTNode]) {}

    /// Estimate the cell count for a reference (unused)
    fn estimate_cell_count(&self, _reference: &ReferenceType) -> Option<usize> {
        Some(0)
    }
}
