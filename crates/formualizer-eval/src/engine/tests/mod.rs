mod arena_debug;
mod cancellation;
mod change_log;
mod common;
mod cycle_detection;
mod demand_driven;
mod dependency;
mod dirty_propagation;
mod dirty_propagation_precision;
mod evaluation;
mod graph_basic;
mod graph_internal_helpers;
mod layer_evaluation;
//mod mark_dirty_benchmarks;
mod parallel;
mod range_dependencies;
mod range_property_tests;
mod schedule_integration;
mod sheet_index_integration;
//mod streaming_evaluation;
mod bulk_ingest;
mod column_operations;
mod debug_vertex_lifecycle;
mod dynamic_topo;
mod named_ranges;
mod range_operations;
mod row_operations;
mod sheet_management;
mod stripe_cleanup_tests;
mod stripe_streaming_integration;
mod stripe_tests;
mod striped_dirty_propagation;
mod tarjan_scc;
mod topo_layers;
mod transactions;
mod vertex_lifecycle;
mod volatile_rng;

mod infinite_ranges;
mod spill_atomic;
mod spill_basic;
mod spill_config_defaults;
mod spill_edges;

mod compressed_range_scheduler;
mod region_lock;
mod sumif_arrow_used_bounds;
mod sumifs_arrow_fastpath;
mod whole_column_sumifs;
mod window_width1_fastpath;

// Phase 1 tests
mod config_defaults;
mod context_default_noops;
mod pass_planner_noop;

// Phase 2 tests
mod countifs_warmup_fidelity;
mod pass_warmup_flatten;
mod sumifs_warmup_fidelity;
// mod sumifs_row_zip_parity;  // Requires more complex setup, will add later
mod pass_lifetime;

// Phase 3 tests
mod mask_cache_reuse;
mod mask_density_paths;
