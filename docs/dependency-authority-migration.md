# Migrating to the dependency authority

Formualizer's evaluation engine now answers every dependency question from one structure: the region-node dependency authority (`formualizer_eval::engine::authority`). It replaces the legacy dependency graph: CSR/delta edge lists, range stripes, name link maps and the optional Pearce–Kelly order. The authority stores a formula family (a block of cells filled from one template) as one node with a few relative edges, instead of one vertex and one edge list per cell. It builds dirty closures, the evaluation schedule and cycles, demand for targeted evaluation, inspection results and structural-edit invalidation.

Values are unchanged except for the corrections listed under [Behavior](#behavior). Evaluation was per cell in Program 1; since Program 2 a family's runs of cells evaluate through the family's template as one unit (see [Region-native execution and compression](#region-native-execution-and-compression)), with the per-cell path as the fallback and the test oracle. Vertex identities (`VertexId`, `Engine::vertex_for_cell`, `evaluate_vertex`) keep their meaning; since Program 2 a formula cell's `VertexId` is also its authority id (see [one id space](#one-id-space)).

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

- **Execution.** Each schedule layer carries the runs of its family nodes (consecutive rows of one column of one node). A run evaluates through the node's template, relocated to each cell, and commits its scalar results as one unit. `SUM` and `AVERAGE` over bounded cell and range references use range kernels that merge overlays once per run and reduce each cell's slice in the scalar function's order (bit-identical results). Dynamic formulas (`OFFSET`, `INDIRECT`), cycle members and array results keep the per-cell path. A run whose template is operators and the built-in `IF` over cell references and literals (every member with the template's literal values) is evaluated column-wise: each referenced column segment is read once per run and each operator applies to all members through the interpreter's own operator code, so values, error precedence and number formats are the per-cell ones. A run of criteria aggregates (`SUMIF(S)`, `COUNTIF(S)`, `AVERAGEIF(S)`) or lookups (`VLOOKUP`, `HLOOKUP`, `MATCH`) with absolute range arguments evaluates each distinct tuple of its other argument values (and their formats) once and reuses that result for the members that repeat it.
- **Storage.** A loader that stages formulas through `Engine::stage_formula_ast` (the Calamine loader does) groups relative copies while it loads: a formula that is exactly the formula above it (or to its left) relocated to its cell is staged as a member of that family (`FormulaIngestRecord::is_family_member`) and its tree is never built. After the authority is built, any other family member whose formula is exactly its template relocated (literals, reference texts and all) stores a reference to the template, and the formula arena keeps only the trees that formula cells and the authority reference. A row or column insert or delete keeps members compressed and shifts their runs as blocks (see [Structural edits on family runs](#structural-edits-on-family-runs)); other structural operations (range moves, sheet operations) give every member its own tree back first. Members are compressed again after the next build.
- **Typed lanes and kernels.** The column-wise evaluation also takes `ROUND`, `ABS`, `MIN`, `MAX`, `SUM`, `AND`, `OR` and `IFERROR` over scalar operands, reading plain unformatted numbers from the merged number lanes; `MIN`, `MAX` and `COUNT` over moving windows use range kernels; `SUMIF(S)`, `COUNTIF(S)` and `AVERAGEIF(S)` over fixed ranges index them once per run. Each path reproduces the per-cell functions' own arithmetic and Arrow kernels, and hands anything else to the per-cell path.
- **Switches.** `EvalConfig::family_execution`, `family_kernels`, `family_lift` and `formula_compression` (default `true`) turn the pieces off; values are the same either way. The per-cell path is the test oracle.

What you need to change:

| Before | Now |
|---|---|
| `DependencyGraph::get_formula_id(v)` for every formula vertex | `formula_view(v)`: `FormulaView { template, row_delta, col_delta }`. Evaluating or rendering `template` with the reference offset `(row_delta, col_delta)` gives exactly this cell's formula; `template` alone is the formula of the family's anchor cell, shared by every member. For a formula stored on its own cell the deltas are zero and `template` is today's id. `get_formula_id` still answers for those and returns `None` for compressed members, as do `get_formula_id_and_volatile` and `get_formula_node(_and_volatile)`. |
| Reading a member's tree from the arena | `DependencyGraph::get_formula(v)` (owned, instantiated; unchanged result). |
| `Layer { vertices }` | `Layer::new(vertices)`. Layer member order is by position for acyclic cells. |
| `EvalConfig { .. }` literals without a rest pattern | add `..Default::default()` (four new fields). |
| `EngineBaselineStats::dirty_vertex_count` counted value cells marked by edits (never cleared) | it counts formula and name vertices awaiting evaluation only. |
| `DependencyGraph::get_vertex_id_for_address(&cell) -> Option<&VertexId>` | `-> Option<VertexId>` (by value; the cell map no longer holds compressed members). |
| `DependencyGraph::sheet_index(sheet)` listing every vertex of the sheet | it lists vertices that are not compressed family members; find a member with `get_vertex_for_cell`. |
| `AuthorityHost::vertex_of_id(id)` | the id is the vertex: `VertexId(id)` for a formula cell. `vertex_of_id_bytes` and `journal` are gone. |
| Authority ids of formula cells from the store's own counter | formula cell ids are vertex ids; symbol binding ids start at `HOST_SYMBOL_ID_BASE`. |
| Undo of a removed cell re-creating it on a new `VertexId` | undo restores the removed `VertexId`. |
| A loader interning every formula (`intern_formula_ast` + `FormulaIngestRecord::new`) | optional: `stage_formula_ast(&mut grouper, row, col, &ast, text)` with one `FormulaFamilyGrouper` per sheet (and `note_staged_formula` for a parse-cache hit) groups copies at load. `get_formula_id` returns `None` for such members right after load (use `formula_view` / `get_formula`). |

### One id space

A formula cell's `VertexId` is its dependency-authority id: the authority adopts the executor's vertex ids for formula cells (and the name vertex's id for a name's node) instead of numbering them itself. The rules of decision 9 hold, as amended for Program 2:

- An id is never renumbered and never given to another cell. A moved cell keeps its vertex, and so its id.
- While value cells have vertices, a formula cell's id belongs to the cell's vertex. A formula -> value -> formula edit keeps the id (as legacy did); Program 1's authority gave the new formula a fresh id.
- Undo and redo restore ids. Undoing a structural delete brings back the removed vertex itself, and so does undoing a formula typed over a value, which legacy replay handled by removing the vertex and later re-creating it. Legacy re-created such cells on new vertices.
- Symbol binding identities (names, tables, sources) keep the store's own counter, starting at `authority::identity::HOST_SYMBOL_ID_BASE` (2^31). Vertex ids stay below it (`vertex_store::MAX_VERTEX_ID`).
- Bulk loads number a sheet's formula cells column by column, so each column of a family is one id run (the authority's identity runs are also the planner's slices). Cycle iteration order is by cell position (spec §7.13) and does not depend on ids.

Program 3 removed value-cell vertices and amended the second rule (decision 27): see [Value cells without vertices](#value-cells-without-vertices).

### Family members without per-cell entries

A family member whose formula is its template relocated, in a column of consecutive vertex ids, has no entry in the graph's cell map, formula map or sheet index. It is stored in one run per column (sheet, column, rows, first id, template, anchor) and found by cell or by id through the run. Its vertex id and flags are unchanged, so dirty state, schedules and values are keyed as before. Its position, kind and edge count read the same through `VertexStore`; when a whole page of 1024 vertices consists of such members, the store keeps no rows for that page and derives them from the run. Any edit of a member moves it back into the maps first. Row and column inserts and deletes shift member runs as blocks (next section); range moves and sheet operations move all members back until the next authority build. With `formula_compression = false`, nothing is stored this way.

### Structural edits on family runs

Row and column inserts and deletes shift a run of family members as a block (contract decision 20.5). The run is split at an inserted row; each part moves as a whole, and its formula changes, if any, are the adjusted template. The block applies only when the reference adjuster gives the part's first and last members the same template (references are affine in the member's row, so every member between them agrees), no reference becomes `#REF!`, and every small range (expanded into cell dependencies) keeps its area. Other parts go back to the per-cell maps and take the per-cell path.

Observable behavior is unchanged: the change log holds the same events (a run's `FormulaAdjusted` events are one record, expanded in place the first time the log is read or indexed, so each record is expanded once and history already read is not copied again; `FormulaAdjusted` events are now in vertex-id order), and undo and redo replay them per cell. `ActionJournal::graph.events` is fully expanded.

## Value cells without vertices

Program 3 (contract decisions 27 and 28): only formula cells and names have vertices.

- **References.** A formula's reference to a value or empty cell creates no vertex, cell-map entry or sheet-index entry. The authority tracks references by position; direct dependency counts are one per referenced cell, as before.
- **Value edits.** Setting a value creates no vertex. A formula it replaces leaves the graph: its id retires into a positional side table. The cell's readers are dirtied by position (`Engine::set_cell_value`, batched value writes, spill children).
- **Ids** (decision 24 as amended by 27, option B). Ids belong to formula cells. A formula -> value -> formula edit at a cell takes the retired id back; undo and redo restore original ids; an id is never given to another cell or renumbered. Row and column inserts and deletes shift the side table; a delete keeps its band's retired ids for undo. A retired id is not visible through `get_vertex_id_for_address`.
- **Change logs.** Value cells have no `VertexMoved`, `RemoveVertex` or `AddVertex` events. A structural delete dirties the readers of its band (their cells change) rather than through removed value vertices.

What you need to change:

| Before | Now |
|---|---|
| `get_vertex_id_for_address(&cell)` / `get_vertex_for_cell` / `Engine::vertex_for_cell` returning a vertex for a value or empty cell | `None`. A formula cell still has its vertex. |
| `VertexEditor::set_cell_value(cell, v) -> VertexId` of the cell | `VertexId(0)` (value cells have no vertex). |
| `OperationSummary { affected_vertices, created_placeholders }` of a value edit starting with the edited cell's vertex | the dirtied formulas only; `created_placeholders` is empty. |
| Value-cell `VertexMoved` / `RemoveVertex` / `AddVertex` in change logs | none. |
| `graph_vertex_count` and the sheet index counting referenced or edited value cells | formula cells and names only. The used extent still counts them (an extent record, not vertices). |
| A value edit rejected under an `EvaluationBudgets` limit of zero new vertices | admitted: it allocates no vertex. Formula edits are still charged. |
| Evaluating a value cell's vertex (`evaluate_vertex`) | create an explicit vertex with `VertexEditor::add_vertex` first, or read the cell. |

## Performance and memory

Measured on the Enron sample (27 workbooks) and the two real-model corpus workbooks, against the last legacy build (medians of two interleaved rounds):
- Retained heap after the first calculation is 0.944× legacy on Enron and 0.945× on the real models. Three small workbooks stay slightly above legacy (at most 1.025×, about 0.3 MB): their formulas are row-wise families, and the authority keeps one identity run per column (about 84 bytes each).
- Load is 0.56× legacy on Enron and 1.05× on the real models (134 ms against 126 ms: the authority is built when the load ends).
- First calculation after load is 0.78× on Enron and 0.75× on the real models. A workbook made of thousands of small formula groups can take longer (at most about 25 ms more in the sample), because planning costs a few microseconds per group.
- Single-cell edit + recalculation p50 is 0.47–0.72× for value edits and 0.13–0.24× for formula edits.

Defining, redefining or deleting a name, table or source after load costs work in that symbol and its readers only; it does not rebuild the dependency structure.

## Testing against legacy

Build `formualizer-eval` with `--features legacy_oracle` to keep legacy's structures beside the authority. `FZ_AUTHORITY_DIFF=strict|count|log:<path>` then compares every dirty propagation with legacy's closure. This crate's own tests always build the oracle.
