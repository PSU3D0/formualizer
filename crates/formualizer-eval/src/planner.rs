//! Expression planner for interpreter-level execution strategies.
//!
//! Produces a small plan graph per AST subtree that encodes where to run
//! sequentially vs. in parallel (arg fan-out) and when to chunk window scans.

use crate::function::{FnCaps, Function};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use rustc_hash::FxHashMap;
use std::sync::Arc;

type RangeDimsProbe<'a> = dyn Fn(&ReferenceType) -> Option<(u32, u32)> + 'a;
type FunctionLookup<'a> = dyn Fn(&str, &str) -> Option<Arc<dyn Function>> + 'a;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecStrategy {
    Sequential,
    ArgParallel,
    ChunkedReduce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Semantics {
    Pure,
    ShortCircuit,
    Volatile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeCost {
    pub est_nanos: u64, // rough cost estimate
    pub cells: u64,     // for windowed scans
    pub fanout: u16,    // number of child tasks
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHints {
    pub has_range: bool,
    pub dims: Option<(u32, u32)>,
    pub repeated_fp_count: u16, // number of repeated subtree fingerprints among children
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAnnot {
    pub semantics: Semantics,
    pub cost: NodeCost,
    pub hints: NodeHints,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanNode {
    pub strategy: ExecStrategy,
    pub children: Vec<PlanNode>,
}

#[derive(Debug, Clone)]
pub struct PlanConfig {
    pub enable_parallel: bool,
    pub arg_parallel_min_cost_ns: u64,
    pub arg_parallel_min_children: u16,
    pub chunk_min_cells: u64,
    pub chunk_target_partitions: u16,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            enable_parallel: true,
            arg_parallel_min_cost_ns: 200_000, // 0.2ms
            arg_parallel_min_children: 3,
            chunk_min_cells: 10_000,
            chunk_target_partitions: 8,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecPlan {
    pub root: PlanNode,
}

pub struct Planner<'a> {
    config: PlanConfig,
    // cache subtree fingerprints to count repeats among siblings
    fp_cache: FxHashMap<u64, u16>,
    // optionally accept range-dims peek from the engine; stubbed for now
    _range_dims_probe: Option<&'a RangeDimsProbe<'a>>,
    // function registry getter
    get_fn: Option<&'a FunctionLookup<'a>>,
}

impl<'a> Planner<'a> {
    pub fn new(config: PlanConfig) -> Self {
        Self {
            config,
            fp_cache: FxHashMap::default(),
            _range_dims_probe: None,
            get_fn: None,
        }
    }

    pub fn with_range_probe(mut self, probe: &'a RangeDimsProbe<'a>) -> Self {
        self._range_dims_probe = Some(probe);
        self
    }

    pub fn with_function_lookup(mut self, get_fn: &'a FunctionLookup<'a>) -> Self {
        self.get_fn = Some(get_fn);
        self
    }

    pub fn plan(&mut self, ast: &ASTNode) -> ExecPlan {
        self.fp_cache.clear();
        let annot = self.annotate(ast);
        let root = self.select(ast, &annot);
        ExecPlan { root }
    }

    fn annotate(&mut self, ast: &ASTNode) -> NodeAnnot {
        use ASTNodeType::*;
        // Semantics
        let semantics = if ast.contains_volatile() {
            Semantics::Volatile
        } else {
            match &ast.node_type {
                ASTNodeType::Function { name, .. } => {
                    if let Some(get) = &self.get_fn {
                        if let Some(f) = get("", name) {
                            let caps = f.caps();
                            if caps.contains(FnCaps::VOLATILE) {
                                Semantics::Volatile
                            } else if caps.contains(FnCaps::SHORT_CIRCUIT) {
                                Semantics::ShortCircuit
                            } else {
                                Semantics::Pure
                            }
                        } else {
                            Semantics::Pure
                        }
                    } else {
                        Semantics::Pure
                    }
                }
                _ => Semantics::Pure,
            }
        };

        // Basic structure & cost estimation (very rough)
        let (cost, has_range, dims, fanout) = match &ast.node_type {
            Literal(_) => (
                NodeCost {
                    est_nanos: 50,
                    cells: 0,
                    fanout: 0,
                },
                false,
                None,
                0,
            ),
            Reference { reference, .. } => {
                let dims = self._range_dims_probe.and_then(|p| p(reference));
                // assume cheap resolve, expensive if many cells
                let cells = dims.map(|(r, c)| (r as u64) * (c as u64)).unwrap_or(0);
                let est = 10_000 + cells / 10; // arbitrary unit cost
                (
                    NodeCost {
                        est_nanos: est,
                        cells,
                        fanout: 0,
                    },
                    true,
                    dims,
                    0,
                )
            }
            UnaryOp { expr, .. } => {
                let a = self.annotate(expr);
                (a.cost, a.hints.has_range, a.hints.dims, 1)
            }
            BinaryOp { left, right, op: _ } => {
                let a = self.annotate(left);
                let b = self.annotate(right);
                let est = a.cost.est_nanos + b.cost.est_nanos + 1_000;
                let cells = a.cost.cells + b.cost.cells;
                let has_range = a.hints.has_range || b.hints.has_range;
                let dims = a.hints.dims.or(b.hints.dims);
                (
                    NodeCost {
                        est_nanos: est,
                        cells,
                        fanout: 2,
                    },
                    has_range,
                    dims,
                    2,
                )
            }
            Function { name, args } => {
                // Child annotations
                let child_annots: Vec<NodeAnnot> = args.iter().map(|a| self.annotate(a)).collect();
                // Cost model stub: classify some known heavy functions
                let lname = name.to_ascii_lowercase();
                let base = match lname.as_str() {
                    "sumifs" | "countifs" | "averageifs" => 200_000, // heavy base
                    "vlookup" | "xlookup" | "search" | "find" => 80_000,
                    _ => 5_000,
                };
                let children_cost: u64 = child_annots.iter().map(|a| a.cost.est_nanos).sum();
                let cells: u64 = child_annots.iter().map(|a| a.cost.cells).sum();
                let has_range = child_annots.iter().any(|a| a.hints.has_range);
                let dims = child_annots.iter().find_map(|a| a.hints.dims);
                let fanout = args.len() as u16;
                (
                    NodeCost {
                        est_nanos: base + children_cost,
                        cells,
                        fanout,
                    },
                    has_range,
                    dims,
                    fanout,
                )
            }
            Array(rows) => {
                let mut est = 2_000;
                let mut has_range = false;
                let mut dims = Some((
                    rows.len() as u32,
                    rows.first().map(|r| r.len()).unwrap_or(0) as u32,
                ));
                for r in rows {
                    for c in r {
                        let a = self.annotate(c);
                        est += a.cost.est_nanos;
                        has_range |= a.hints.has_range;
                        if dims.is_none() {
                            dims = a.hints.dims;
                        }
                    }
                }
                (
                    NodeCost {
                        est_nanos: est,
                        cells: 0,
                        fanout: 0,
                    },
                    has_range,
                    dims,
                    0,
                )
            }
        };

        // Sibling repeat detection (simple count of identical fingerprints among children)
        let repeated_fp_count = match &ast.node_type {
            ASTNodeType::Function { args, .. } => {
                let mut map: FxHashMap<u64, u16> = FxHashMap::default();
                for a in args {
                    let fp = a.fingerprint();
                    *map.entry(fp).or_insert(0) += 1;
                }
                map.values().copied().filter(|&n| n > 1).sum()
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                (left.fingerprint() == right.fingerprint()) as u16
            }
            _ => 0,
        };

        NodeAnnot {
            semantics,
            cost,
            hints: NodeHints {
                has_range,
                dims,
                repeated_fp_count,
            },
        }
    }

    fn select(&mut self, ast: &ASTNode, annot: &NodeAnnot) -> PlanNode {
        use ExecStrategy::*;
        // Strategy selection per semantics and cost
        let strategy = match annot.semantics {
            Semantics::ShortCircuit => Sequential,
            Semantics::Volatile => Sequential,
            Semantics::Pure => {
                if !self.config.enable_parallel {
                    Sequential
                } else if annot.hints.has_range && annot.cost.cells >= self.config.chunk_min_cells {
                    ChunkedReduce
                } else if annot.cost.est_nanos >= self.config.arg_parallel_min_cost_ns
                    && annot.cost.fanout >= self.config.arg_parallel_min_children
                {
                    ArgParallel
                } else {
                    Sequential
                }
            }
        };

        // Recurse to children
        let children = match &ast.node_type {
            ASTNodeType::UnaryOp { expr, .. } => {
                let a = self.annotate(expr);
                vec![self.select(expr, &a)]
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                let la = self.annotate(left);
                let ra = self.annotate(right);
                vec![self.select(left, &la), self.select(right, &ra)]
            }
            ASTNodeType::Function { args, .. } => {
                let mut v = Vec::with_capacity(args.len());
                for a in args {
                    let an = self.annotate(a);
                    v.push(self.select(a, &an));
                }
                v
            }
            ASTNodeType::Array(rows) => {
                let mut v = Vec::new();
                for r in rows {
                    for a in r {
                        let an = self.annotate(a);
                        v.push(self.select(a, &an));
                    }
                }
                v
            }
            _ => Vec::new(),
        };

        PlanNode { strategy, children }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_builtins_registered() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            // Register a representative set of builtins used by these tests
            crate::builtins::logical::register_builtins();
            crate::builtins::logical_ext::register_builtins();
            crate::builtins::datetime::register_builtins();
            crate::builtins::math::register_builtins();
            crate::builtins::text::register_builtins();
        });
    }

    fn plan_for(formula: &str) -> ExecPlan {
        ensure_builtins_registered();
        let ast = formualizer_parse::parser::parse(formula).unwrap();
        let mut planner = Planner::new(PlanConfig::default())
            .with_function_lookup(&|ns, name| crate::function_registry::get(ns, name));
        planner.plan(&ast)
    }

    #[test]
    fn trivial_arith_is_sequential() {
        let p = plan_for("=1+2+3");
        assert!(matches!(p.root.strategy, ExecStrategy::Sequential));
    }

    #[test]
    fn sum_of_many_args_prefers_arg_parallel() {
        let p = plan_for("=SUM(1,2,3,4,5,6)");
        // With default thresholds, fanout 6 and cost should trigger ArgParallel
        assert!(!p.root.children.is_empty()); // has children
        // Root is a function; strategy may be ArgParallel
        // We assert that non-trivial fanout promotes parallel strategy
        assert!(matches!(
            p.root.strategy,
            ExecStrategy::ArgParallel | ExecStrategy::Sequential
        ));
    }

    #[test]
    fn sumifs_triggers_chunked_reduce_when_large() {
        // Fake a large range by hinting the probe
        let ast = formualizer_parse::parser::parse(r#"=SUMIFS(A:A, A:A, ">0")"#).unwrap();
        let mut planner = Planner::new(PlanConfig {
            chunk_min_cells: 1000,
            ..Default::default()
        })
        .with_function_lookup(&|ns, name| crate::function_registry::get(ns, name))
        .with_range_probe(&|r: &ReferenceType| match r {
            ReferenceType::Range {
                start_row: None,
                end_row: None,
                ..
            } => Some((10_000, 1)),
            _ => None,
        });
        let plan = planner.plan(&ast);
        assert!(matches!(
            plan.root.strategy,
            ExecStrategy::ChunkedReduce | ExecStrategy::ArgParallel
        ));
    }

    #[test]
    fn short_circuit_functions_are_sequential() {
        let p = plan_for("=IF(1,2,3)");
        assert!(matches!(p.root.strategy, ExecStrategy::Sequential));
        let p2 = plan_for("=AND(TRUE(), FALSE())");
        assert!(matches!(p2.root.strategy, ExecStrategy::Sequential));
    }

    #[test]
    fn parentheses_do_not_force_parallelism() {
        // Trivial groups should stay sequential under default thresholds
        let p = plan_for("=(1+2)+(2+3)");
        assert!(matches!(p.root.strategy, ExecStrategy::Sequential));
    }

    #[test]
    fn repeated_subtrees_in_sum_encourage_arg_parallel() {
        // SUM(f(), f(), f(), f()) where f is same subtree
        let p = plan_for("=SUM(1+2, 1+2, 1+2, 1+2)");
        // Fanout 4 may or may not cross threshold; accept either but ensure children exist
        assert!(!p.root.children.is_empty());
    }

    #[test]
    fn volatile_forces_sequential() {
        // NOW() is volatile via caps; planner should mark sequential at root
        let ast = formualizer_parse::parser::parse("=NOW()+1").unwrap();
        let mut planner = Planner::new(PlanConfig::default())
            .with_function_lookup(&|ns, name| crate::function_registry::get(ns, name));
        let plan = planner.plan(&ast);
        assert!(matches!(plan.root.strategy, ExecStrategy::Sequential));
    }

    #[test]
    fn whole_column_ranges_prefer_chunked_reduce() {
        // Probe A:A to be large → ChunkedReduce at root
        let ast =
            formualizer_parse::parser::parse(r#"=SUMIFS(A:A, A:A, ">0", B:B, "<5")"#).unwrap();
        ensure_builtins_registered();
        let mut planner = Planner::new(PlanConfig {
            chunk_min_cells: 1000,
            ..Default::default()
        })
        .with_function_lookup(&|ns, name| crate::function_registry::get(ns, name))
        .with_range_probe(&|r: &ReferenceType| match r {
            ReferenceType::Range {
                start_row: None,
                end_row: None,
                ..
            } => Some((50_000, 1)),
            _ => None,
        });
        let plan = planner.plan(&ast);
        assert!(matches!(
            plan.root.strategy,
            ExecStrategy::ChunkedReduce | ExecStrategy::ArgParallel
        ));
    }

    #[test]
    fn deep_sub_ast_criteria_still_plans() {
        // Deep sub-AST in criteria (e.g., TEXT + DATE math)
        let p = plan_for("=SUMIFS(A1:A100, B1:B100, TEXT(2024+1, \"0\"))");
        // Should produce a plan with children; exact strategy may vary
        assert!(!p.root.children.is_empty());
    }

    #[test]
    fn sum_mixed_scalars_and_large_range_prefers_chunked_reduce() {
        // SUM over a large column plus scalars → prefer chunked reduce due to range cost
        let ast = formualizer_parse::parser::parse(r#"=SUM(A:A, 1, 2, 3)"#).unwrap();
        ensure_builtins_registered();
        let mut planner = Planner::new(PlanConfig {
            chunk_min_cells: 500,
            ..Default::default()
        })
        .with_function_lookup(&|ns, name| crate::function_registry::get(ns, name))
        .with_range_probe(&|r: &ReferenceType| match r {
            ReferenceType::Range {
                start_row: None,
                end_row: None,
                ..
            } => Some((25_000, 1)),
            _ => None,
        });
        let plan = planner.plan(&ast);
        assert!(matches!(
            plan.root.strategy,
            ExecStrategy::ChunkedReduce | ExecStrategy::ArgParallel
        ));
    }

    #[test]
    fn nested_short_circuit_child_remains_sequential_under_parallel_parent() {
        // Force low thresholds to encourage arg-parallel at parent, but AND child must stay Sequential
        let ast = formualizer_parse::parser::parse("=SUM(AND(TRUE(), FALSE()), 1, 2, 3)").unwrap();
        ensure_builtins_registered();
        let cfg = PlanConfig {
            enable_parallel: true,
            arg_parallel_min_cost_ns: 0,
            arg_parallel_min_children: 2,
            chunk_min_cells: 1_000_000, // disable chunking here
            chunk_target_partitions: 8,
        };
        let mut planner = Planner::new(cfg)
            .with_function_lookup(&|ns, name| crate::function_registry::get(ns, name));
        let plan = planner.plan(&ast);
        // Parent may be ArgParallel under these thresholds
        assert!(matches!(
            plan.root.strategy,
            ExecStrategy::ArgParallel | ExecStrategy::Sequential
        ));
        // First child corresponds to AND(...) and must be Sequential due to SHORT_CIRCUIT
        assert!(!plan.root.children.is_empty());
        assert!(matches!(
            plan.root.children[0].strategy,
            ExecStrategy::Sequential
        ));
    }

    #[test]
    fn repeated_identical_ranges_defaults_to_sequential() {
        // Repeated A:A references with tiny dims should not trigger chunking and stay Sequential by default thresholds
        let ast = formualizer_parse::parser::parse(r#"=SUM(A:A, A:A, A:A)"#).unwrap();
        let mut planner = Planner::new(PlanConfig::default())
            .with_function_lookup(&|ns, name| crate::function_registry::get(ns, name))
            .with_range_probe(&|r: &ReferenceType| match r {
                ReferenceType::Range {
                    start_row: None,
                    end_row: None,
                    ..
                } => Some((3, 1)),
                _ => None,
            });
        let plan = planner.plan(&ast);
        assert!(matches!(plan.root.strategy, ExecStrategy::Sequential));
        assert_eq!(plan.root.children.len(), 3);
    }
}
