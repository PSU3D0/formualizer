//! Global pass warmup planning

use crate::engine::cache::RangeKey;
use crate::engine::reference_fingerprint::ReferenceFingerprint;
use crate::engine::tuning::WarmupConfig;
use formualizer_core::ASTNode;
use formualizer_core::parser::{RefView, ReferenceType};
use std::collections::HashMap;

/// Plan for what to warm up before evaluation
#[derive(Debug, Clone, Default)]
pub struct PassWarmupPlan {
    /// Ranges to pre-flatten
    pub flatten: Vec<HotReference>,

    /// Criteria sets to pre-build masks for (Phase 3)
    pub masks: Vec<CriteriaKey>,

    /// Columns to build equality indexes for (Phase 4)
    pub eq_indexes: Vec<IndexKey>,

    /// Columns to build range indexes for (Phase 5)
    pub range_indexes: Vec<IndexKey>,
}

/// Key for criteria mask (Phase 3)
pub type CriteriaKey = String;

/// Key for column index (Phase 4/5)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct IndexKey {
    pub sheet_id: u32,
    pub column: u32,
}

/// Information about a hot reference
#[derive(Debug, Clone)]
pub struct HotReference {
    pub key: RangeKey,
    pub count: usize,
    pub cell_count: usize,
}

/// Information about hot criteria
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

    /// Analyze multiple target ASTs for batch evaluation
    pub fn analyze_targets(&self, targets: &[&ASTNode]) -> PassWarmupPlan {
        if !self.config.warmup_enabled {
            return PassWarmupPlan::default();
        }

        // Collect references from all targets
        let mut reference_counts: HashMap<String, (usize, usize)> = HashMap::new();
        for ast in targets {
            self.collect_references(ast, &mut reference_counts);
        }

        // Score and rank references
        let mut hot_refs: Vec<HotReference> = reference_counts
            .into_iter()
            .filter_map(|(key, (count, cell_count))| {
                // Apply minimum cell threshold
                if cell_count >= self.config.min_flat_cells {
                    // Check reuse threshold
                    if count >= self.config.flat_reuse_threshold {
                        Some(HotReference {
                            key,
                            count,
                            cell_count,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Sort by score (reuse × size)
        hot_refs.sort_by_key(|r| std::cmp::Reverse(r.count * r.cell_count));

        // Select top-K under memory budget
        let mut selected = Vec::new();
        let mut estimated_memory = 0;
        let memory_budget_bytes = self.config.flat_cache_mb_cap * 1024 * 1024;

        for hot_ref in hot_refs {
            // Estimate memory for this flat
            let estimated_size = hot_ref.cell_count * std::mem::size_of::<f64>();
            if estimated_memory + estimated_size <= memory_budget_bytes {
                selected.push(hot_ref);
                estimated_memory += estimated_size;
            } else {
                break; // Memory budget exhausted
            }
        }

        PassWarmupPlan {
            flatten: selected,
            masks: Vec::new(),         // Phase 3
            eq_indexes: Vec::new(),    // Phase 4
            range_indexes: Vec::new(), // Phase 5
        }
    }

    /// Analyze single AST for single-cell evaluation
    pub fn analyze_single(&self, ast: &ASTNode) -> PassWarmupPlan {
        if !self.config.warmup_enabled {
            return PassWarmupPlan::default();
        }

        // For single-cell, use lower thresholds
        let mut reference_counts: HashMap<String, (usize, usize)> = HashMap::new();
        self.collect_references(ast, &mut reference_counts);

        // For single-cell evaluation, even count=1 might be worth flattening if large enough
        let mut hot_refs: Vec<HotReference> = reference_counts
            .into_iter()
            .filter_map(|(key, (count, cell_count))| {
                if cell_count >= self.config.min_flat_cells {
                    Some(HotReference {
                        key,
                        count,
                        cell_count,
                    })
                } else {
                    None
                }
            })
            .collect();

        // Sort by cell count for single-cell (size matters more than reuse)
        hot_refs.sort_by_key(|r| std::cmp::Reverse(r.cell_count));

        // Take top few under budget
        hot_refs.truncate(5); // Limit for single-cell warmup

        PassWarmupPlan {
            flatten: hot_refs,
            masks: Vec::new(),
            eq_indexes: Vec::new(),
            range_indexes: Vec::new(),
        }
    }

    /// Collect references from an AST
    fn collect_references(&self, ast: &ASTNode, counts: &mut HashMap<String, (usize, usize)>) {
        // Walk the AST and collect references
        self.walk_ast(ast, counts);
    }

    /// Recursively walk AST nodes
    fn walk_ast(&self, node: &ASTNode, counts: &mut HashMap<String, (usize, usize)>) {
        use formualizer_core::parser::ASTNodeType;

        match &node.node_type {
            ASTNodeType::Reference { reference, .. } => {
                // Track this reference
                if let Some(cell_count) = self.estimate_cell_count(reference) {
                    let key = reference.fingerprint();
                    let entry = counts.entry(key).or_insert((0, cell_count));
                    entry.0 += 1; // Increment count
                }
            }
            ASTNodeType::Function { args, .. } => {
                // Recurse into function arguments
                for arg in args {
                    self.walk_ast(arg, counts);
                }
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.walk_ast(left, counts);
                self.walk_ast(right, counts);
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.walk_ast(expr, counts);
            }
            ASTNodeType::Array(rows) => {
                for row in rows {
                    for element in row {
                        self.walk_ast(element, counts);
                    }
                }
            }
            ASTNodeType::Literal(_) => {
                // No references to collect
            }
        }
    }

    /// Estimate the cell count for a reference
    fn estimate_cell_count(&self, reference: &ReferenceType) -> Option<usize> {
        match reference {
            ReferenceType::Cell { .. } => Some(1),
            ReferenceType::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                // Calculate dimensions if bounds are known
                match (start_row, start_col, end_row, end_col) {
                    (Some(sr), Some(sc), Some(er), Some(ec)) => {
                        let rows = (er - sr + 1) as usize;
                        let cols = (ec - sc + 1) as usize;
                        Some(rows * cols)
                    }
                    // Unbounded ranges - use a heuristic
                    (None, Some(sc), None, Some(ec)) => {
                        // Whole columns
                        let cols = (ec - sc + 1) as usize;
                        Some(1_000_000 * cols) // Assume 1M rows
                    }
                    (Some(sr), None, Some(er), None) => {
                        // Whole rows
                        let rows = (er - sr + 1) as usize;
                        Some(rows * 16384) // Assume 16K columns
                    }
                    _ => None, // Can't estimate
                }
            }
            _ => None, // Named ranges, tables - skip for now
        }
    }

    /// Check if a reference should be flattened based on size and config
    pub fn should_flatten(&self, reference: &ReferenceType, cell_count: usize) -> bool {
        if !self.config.warmup_enabled {
            return false;
        }

        // Check minimum cells threshold
        if cell_count < self.config.min_flat_cells {
            return false;
        }

        // Only flatten ranges
        matches!(reference, ReferenceType::Range { .. })
    }
}
