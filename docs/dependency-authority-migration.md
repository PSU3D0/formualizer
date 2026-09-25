# Migrating to the dependency authority

Formualizer's evaluation engine now answers every dependency question from one structure: the region-node dependency authority (`formualizer_eval::engine::authority`). It replaces the legacy dependency graph: CSR/delta edge lists, range stripes, name link maps and the optional Pearce–Kelly order. The authority stores a formula family (a block of cells filled from one template) as one node with a few relative edges, instead of one vertex and one edge list per cell. It builds dirty closures, the evaluation schedule and cycles, demand for targeted evaluation, inspection results and structural-edit invalidation.

Evaluation is still per cell, and values are unchanged except for the corrections listed under [Behavior](#behavior). Vertex identities (`VertexId`, `Engine::vertex_for_cell`, `evaluate_vertex`) are unchanged.

## What you need to change

Most users need no change. The following low-level surfaces exposed legacy internals. They are gone from normal builds, and are available only with the `legacy_oracle` feature of `formualizer-eval`, which is meant for differential testing and never used at runtime.

| Removed from normal builds | Use instead |
|---|---|
| `engine::csr_edges`, `engine::delta_edges`, `engine::topo` (`CsrEdges`, `CsrMutableEdges`, `DynamicTopo`, …) | Nothing: the engine keeps no edge lists. |
| `engine::Scheduler` | `Engine::get_eval_plan` for a plan; `Schedule`/`Layer` remain as types. |
| `DependencyGraph::get_dependents`, `get_dependencies`, `get_range_dependencies` | `Engine::dependents`, `Engine::precedents`, `Engine::trace` (inspection API). |
| `DependencyGraph::add_dependency_edge`, `add_edges_nobatch`, `build_edges_from_adjacency`, `add_range_edges` | Nothing: dependencies come from formulas. Set a formula instead of adding an edge. |
| `DependencyGraph::rebuild_edges`, `flush_pending_edge_deltas`, `edges_delta_size`, `edges_rebuild_count` | Nothing: there are no edge deltas. |
| `NamedRange::dependents` (public field) | No equivalent. The public API has no query for the formulas and names that read a defined name. `Engine::dependents` and `Engine::trace` do not return them: inspection reports readers through cell and range references only, excluding name- and table-mediated readers, as legacy's inspection did. |

These became crate-private: `DependencyGraph::remove_all_edges`, `update_edge_grid_addr`, `add_range_deps_from_keys`.

Kept for compatibility, with changed meaning:

- `EvalConfig::use_dynamic_topo`, `pk_visit_budget`, `pk_compaction_interval_ops`, `pk_reject_cycle_edges`, `max_layer_width`, `enable_block_stripes`: accepted and ignored (they configure legacy structures that no longer exist). They will be removed in a later release.
- `ChangeEvent::RemoveVertex { old_dependencies, old_dependents }` and `VertexSnapshot::out_edges`: always empty. Undo restores the removed cell's value or formula, and the authority derives its edges from that.
- `VertexEditor::add_edge` / `remove_edge`: no-ops, as before. Journal replay of old `EdgeAdded`/`EdgeRemoved` events still works.
- `GraphBaselineStats::graph_edge_count` and the admission limit `graph_edge_hard_limit` (`ResourceExhaustionReason::GraphEdges`): still the number of direct dependency edges legacy would have held (cell references and ranges within `range_expansion_limit`, per formula). The count is kept per cell without edge lists.
- `FormulaPlaneMode`: accepted and ignored. FormulaPlane spans are never placed, and the authority's families play that role. `formula_plane_mode` reads back as `Off`.
- The `unified_authority` feature of `formualizer-eval` is a no-op, kept so existing `--features` lines still build.

New: `InspectionUnavailableReason::DependencyAuthorityUnavailable` (the enum is `#[non_exhaustive]`). Inspection returns it when the authority cannot answer, for example after a typed authority failure.

## Behavior

Legacy behavior is the specification. It changed only where legacy published a stale or wrong value:

- **Dynamic references (INDIRECT, OFFSET) never publish stale values.** A dynamic reader whose target is still dirty is re-planned in the same recalculation instead of keeping a value computed from the old target. Legacy could leave such a reader, or its static readers, one recalculation behind.
- **Readers of new spill cells are fresh in the same recalculation.** When a spill writes cells that a formula already read, that formula recalculates in the same request. Legacy did it one request later. A whole-column reader such as `SUM(C:C)` over a spill that commits earlier in the same pass now includes the spill.
- **Formulas inside a table read through a structured reference are ordered correctly.** `SUM(Table[Col])` recalculates after formulas in the table body. Legacy could order it before them.
- **Undoing a structural edit (row/column insert or delete) keeps restored formulas current** (FORM-000117). A later edit to a restored formula's precedent recalculates it.
- **A reference to a missing table evaluates to `#NAME?`** under the default `BestEffort` preparation policy. It was `#N/IMPL!`. See [preparation errors](preparation-error-policy.md).
- **Inspection work budgets count reported readers.** `DependentsOptions::max_work` / `TraceOptions::max_work` now charge one unit per reported reader, where legacy charged one per internal edge or stripe visited. A binding budget can therefore return a different number of results before it reports truncation. Unbounded results are unchanged.

## Performance and memory

Measured on the Enron sample and the real-model corpus, against the last legacy build:
- Retained heap after the first calculation is about 0.95× legacy.
- Load is 0.57× legacy on Enron (1.09× on the two small real models, 134 ms against 146 ms).
- First calculation after load is 0.81× on Enron and 0.84× on the real models.
- Single-cell edit + recalculation p50 is 0.47–0.75× for value edits and 0.14–0.24× for formula edits.

Very small workbooks (a few thousand formulas) can take a few milliseconds longer on the first calculation, because the authority is built then.

## Testing against legacy

Build `formualizer-eval` with `--features legacy_oracle` to keep legacy's structures beside the authority. `FZ_AUTHORITY_DIFF=strict|count|log:<path>` then compares every dirty propagation with legacy's closure. This crate's own tests always build the oracle.
