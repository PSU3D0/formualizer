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
- `FormulaPlaneMode`: accepted and ignored. FormulaPlane spans are never placed, and the authority's families play that role. an engine built with `Engine::new` stores `Off` in its config.
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

## FormulaPlane removal

The FormulaPlane span runtime (an earlier experiment that evaluated a formula family as one span) was removed after the authority became the only runtime path; with the mode ignored no span was ever placed, so no value changes.

- `FormulaPlaneMode`, `EvalConfig::formula_plane_mode`, `EvalConfig::with_formula_plane_mode`, `WorkbookConfig::with_span_evaluation` / `with_formula_plane_mode` and the Python and WASM toggles are accepted and ignored. An engine built with `Engine::new` stores `Off` in its config. Whatever the stored value, evaluation never places spans.
- The public module `formualizer_eval::formula_plane` is gone. Its descriptor types (template/run/partition/virtual-reference ids, grid shapes, the passive `FormulaRunStore` and span counters) had no engine use and no replacement. If you used the run store for scanning, copy `formualizer-bench-core`'s `formula_runs` module.
- `relocate_ast_for_template_placement` (hidden) moved to `formualizer_eval::engine::template::relocate`; the hidden `formula_plane_diagnostics` module moved to `engine::template::diagnostics` and keeps only `canonical_template_diagnostic`.
- `EngineBaselineStats::formula_plane_*` and `PreparationRevision::{authority, authority_indexes, authority_indexed_plane}` are always `0`. The `max_formula_plane_*` limits are ignored.

## Region-native execution and compression

Program 2 makes the authority's family node the unit of execution and of storage.

- **Execution.** Each schedule layer carries the runs of its family nodes (consecutive rows of one column of one node). A run evaluates through the node's template, relocated to each cell, and commits its scalar results as one unit. `SUM` and `AVERAGE` over bounded cell and range references use range kernels that merge overlays once per run and reduce each cell's slice in the scalar function's order (bit-identical results). Dynamic formulas (`OFFSET`, `INDIRECT`), cycle members and array results keep the per-cell path. A run whose template is operators and the built-in `IF` over cell references and literals (every member with the template's literal values) is evaluated column-wise: each referenced column segment is read once per run and each operator applies to all members through the interpreter's own operator code, so values, error precedence and number formats are the per-cell ones.
- **Storage.** After the authority is built, a family member whose formula is exactly its template relocated (literals, reference texts and all) stores a reference to the template, and the formula arena keeps only the trees that formula cells and the authority reference. A structural edit (row/column insert or delete, moves, sheet operations) gives every member its own tree back first; members are compressed again after the next build.
- **Switches.** `EvalConfig::family_execution`, `family_kernels`, `family_lift` and `formula_compression` (default `true`) turn the pieces off; values are the same either way. The per-cell path is the test oracle.

What you need to change:

| Before | Now |
|---|---|
| `DependencyGraph::get_formula_id(v)` for every formula vertex | `formula_view(v)`: `FormulaView { template, row_delta, col_delta }`. Evaluating or rendering `template` with the reference offset `(row_delta, col_delta)` gives exactly this cell's formula; `template` alone is the formula of the family's anchor cell, shared by every member. For a formula stored on its own cell the deltas are zero and `template` is today's id. `get_formula_id` still answers for those and returns `None` for compressed members, as do `get_formula_id_and_volatile` and `get_formula_node(_and_volatile)`. |
| Reading a member's tree from the arena | `DependencyGraph::get_formula(v)` (owned, instantiated; unchanged result). |
| `Layer { vertices }` | `Layer::new(vertices)`. Layer member order is by position for acyclic cells. |
| `EvalConfig { .. }` literals without a rest pattern | add `..Default::default()` (four new fields). |
| `EngineBaselineStats::dirty_vertex_count` counted value cells marked by edits (never cleared) | it counts formula and name vertices awaiting evaluation only. |

## Performance and memory

Measured on the Enron sample (27 workbooks) and the two real-model corpus workbooks, against the last legacy build (medians of two interleaved rounds):
- Retained heap after the first calculation is 0.944× legacy on Enron and 0.945× on the real models. Three small workbooks stay slightly above legacy (at most 1.025×, about 0.3 MB): their formulas are row-wise families, and the authority keeps one identity run per column (about 84 bytes each).
- Load is 0.56× legacy on Enron and 1.05× on the real models (134 ms against 126 ms: the authority is built when the load ends).
- First calculation after load is 0.78× on Enron and 0.75× on the real models. A workbook made of thousands of small formula groups can take longer (at most about 25 ms more in the sample), because planning costs a few microseconds per group.
- Single-cell edit + recalculation p50 is 0.47–0.72× for value edits and 0.13–0.24× for formula edits.

Defining, redefining or deleting a name, table or source after load costs work in that symbol and its readers only; it does not rebuild the dependency structure.

## Testing against legacy

Build `formualizer-eval` with `--features legacy_oracle` to keep legacy's structures beside the authority. `FZ_AUTHORITY_DIFF=strict|count|log:<path>` then compares every dirty propagation with legacy's closure. This crate's own tests always build the oracle.
