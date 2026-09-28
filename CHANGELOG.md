# Changelog

All notable changes to Formualizer will be documented in this file.

## Unreleased

### Changed

- **Formula families evaluate as one unit, and family members share their formula** (Program 2). A block of cells filled from one template now evaluates through the template, one schedule unit per run of consecutive rows, with its results written as one unit. `SUM` and `AVERAGE` over bounded cell or range references run as range kernels: overlays are merged once per run and each cell reduces its own slice in the same order as the scalar function, so results are bit-identical (including `-0.0`). After the dependency structure is built, a family member whose formula is its template relocated stores a reference to the template instead of its own tree, and the formula arena drops the trees nobody references. A run of `SUMIF(S)`, `COUNTIF(S)`, `AVERAGEIF(S)`, `VLOOKUP`, `HLOOKUP` or `MATCH` whose range arguments are absolute evaluates each distinct combination of its other arguments once and reuses the result for the cells that repeat it. A run whose formula is operators (arithmetic, comparison, `&`, unary `+`/`-`/`%`) and the built-in `IF` over cell references and literals evaluates column-wise instead of walking the formula per cell, through the same operator code (bit-identical values, errors and number formats). Values, formula text and `get_formula` results are unchanged. Four `EvalConfig` switches turn the pieces off: `family_execution`, `family_kernels`, `family_lift` and `formula_compression` (all default `true`). See [region-native execution and compression](docs/dependency-authority-migration.md#region-native-execution-and-compression).
- **Breaking (low-level API):**
  - `DependencyGraph::get_formula_id` returns `None` for a compressed family member (its formula is a shared template at an offset). Use the new `DependencyGraph::formula_view(VertexId) -> Option<FormulaView>` (`template`, `row_delta`, `col_delta`; the struct is `#[non_exhaustive]`) or `get_formula` (the instantiated tree). `get_formula_id_and_volatile`, `get_formula_node` and `get_formula_node_and_volatile` also return `None` for members.
  - `Layer` gains a private field; build one with `Layer::new(vertices)`. Within a schedule layer, acyclic cells are ordered by position (sheet, column, row) rather than by vertex id; cycle members keep vertex-id order. Values are unchanged.
  - `EvalConfig` gains the public fields `family_execution`, `family_kernels`, `family_lift` and `formula_compression`; struct literals need `..Default::default()`.
  - `Function` gains the hidden, defaulted `family_kernel()` method and the hidden `FamilyKernel` enum (only the built-in `SUM`, `AVERAGE`, `IF`, the criteria aggregates and `VLOOKUP`/`HLOOKUP`/`MATCH` return one; an override registered under the same name keeps the scalar path).
  - `EngineBaselineStats::dirty_vertex_count` no longer counts value cells that an edit marked: they were never evaluated, so they stayed in the dirty set for the rest of the session.
  - **One id space.** A formula cell's `VertexId` is now also its dependency-authority id. The authority adopts the executor's vertex ids instead of numbering formula cells itself, so `AuthorityHost::vertex_of_id`, `vertex_of_id_bytes` and `journal` are removed, and authority ids read from `store().ids()` are vertex ids. Symbol binding identities (names, tables, sources) keep the store's own counter, which now starts at `authority::identity::HOST_SYMBOL_ID_BASE` (2^31); vertex ids stop at `vertex_store::MAX_VERTEX_ID` (2^31 - 1, previously `u32::MAX`).
  - **Undo restores the removed cell's `VertexId`** (decision 9, as amended). Undoing a structural delete, or undoing a formula typed over a value, now brings back the removed vertex itself; legacy re-created the cell on a new vertex. A formula cell keeps its id across formula -> value -> formula edits, as before: while value cells have vertices, the id belongs to the cell's vertex. No id is ever given to another cell.
  - Formula vertices created by a bulk load (`Workbook` loaders, `BulkIngestBuilder`) are numbered column by column per sheet, so each column of a family is one id run. They used to be numbered in staging order, with referenced cells interleaved. Cycle iteration order is by cell position (spec §7.13) and does not change.
  - Row and column inserts and deletes log their `FormulaAdjusted` events in vertex-id order; they were logged in the formula map's (hash) order. The events themselves, and every other event and its order, are unchanged.
  - `DependencyGraph::get_vertex_id_for_address` returns `Option<VertexId>`; it returned `Option<&VertexId>`, a borrow of the cell map, which no longer holds compressed family members. `DependencyGraph::sheet_index` / `sheet_index_mut` no longer list compressed family members. Use `get_vertex_for_cell` to find a member by cell.
- **Removed the FormulaPlane span runtime (internal; no behavior change).** Spans were never placed once the dependency authority became the only runtime path, so the span store, span evaluation and scheduling, demotion, mixed-topology and island paths, span counters and structural span shifting are deleted. The parts ingest still uses (canonical templates, the dependency-summary analyzer, slot maps, read summaries, regions) moved to `engine::template`. `FormulaPlaneMode`, `EvalConfig::formula_plane_mode`, `WorkbookConfig::with_span_evaluation` and the Python/WASM toggles are still accepted and ignored. Values are unchanged. The dependency graph also releases its first-load cell map after the load instead of keeping its capacity (about 60 B per loaded formula). See [migrating to the dependency authority](docs/dependency-authority-migration.md#formulaplane-removal).
- **Breaking (low-level API):** the public module `formualizer_eval::formula_plane` is removed with all its items: `FormulaTemplateId`, `FormulaRunId`, `FormulaFingerprint`, `DependencyShapeFingerprint`, `GridShape`, `GridExtent`, `RangeCardinality`, `PartitionId`, `VirtualSourceId`, `VirtualRangeId`, `VirtualSourceKey`, `VirtualProviderVersion`, `VirtualReferenceKind`, `VirtualReferenceVolatility`, `VirtualReferenceErrorKind`, `FormulaPlaneCandidateCell`, `CandidateFormulaRun`, `CandidateRunOrientation`, `SpanPartitionCounterOptions`, `SpanPartitionCounters`, `TemplateSpanPartitionCounters`, `compute_span_partition_counters`, `DEFAULT_CANDIDATE_ROW_BLOCK_SIZE`, `FormulaRunStore`, `FormulaRunStoreBuildOptions`, `FormulaRunStoreBuildReport`, `build_formula_run_store`, `FormulaRunDescriptor`, `FormulaRunShape`, `FormulaRejectedCell`, `FormulaRejectReason`, `FormulaTemplateArena`, `FormulaTemplateDescriptor`, `TemplateSupportStatus`, `SpanGapDescriptor`, `SpanGapKind`, `Fp2aCounterDelta`, `Fp2aReconciliation`, `DEFAULT_GAP_SCAN_MAX_PER_AXIS_GROUP`. None was used by the engine; the passive run store and span counters now live in the (unpublished) `formualizer-bench-core` as `formula_runs`. Other low-level changes:
  - the `#[doc(hidden)]` `formula_plane::structural::relocate_ast_for_template_placement` is now `engine::template::relocate::relocate_ast_for_template_placement`, and the `formula_plane_diagnostics` feature's module is `engine::template::diagnostics` with only `canonical_template_diagnostic` (the dependency-summary diagnostics are gone);
  - the `formula_plane_*` counters of `EngineBaselineStats` and `PreparationRevision::{authority, authority_indexes, authority_indexed_plane}` are kept but always `0`;
  - `EvalConfig::{max_formula_plane_cache_candidates, max_formula_plane_cache_edges, max_formula_plane_cache_bytes}` and `WorkbookLoadLimits::max_formula_plane_fallback_cells` are accepted and ignored;
  - the `#[doc(hidden)]` `FormulaCompressedPreparation::{is_direct, direct_family_count, direct_cell_count}` always report no directly placed family (as they did with the mode ignored);
  - tracing: the `structural.edit` event (which reported span counts) is gone, and `evaluate.summary` drops its always-zero `span_tasks`, `span_placements`, `memo_hits` and `skipped_punchouts` fields.
- **The region-node dependency authority replaces the legacy dependency graph** (Program 1, FORM-000169). Dirty propagation, the evaluation schedule and cycles, targeted-evaluation demand, `get_eval_plan`, inspection (`dependents`/`precedents`/`trace`) and structural-edit invalidation all come from one structure. It stores each formula family (cells filled from one template) as a single node with relative edges, not one vertex and edge list per cell. Evaluation stays per cell, and vertex identities are unchanged. On the Enron sample and the real-model corpus, retained heap is about 0.94× the last legacy build, load 0.56× (Enron; 1.05× on the two real models), first calculation 0.75–0.78×, and edit + recalculation p50 0.47–0.72× for value edits and 0.13–0.24× for formula edits. Values are unchanged except where legacy published stale results:
  - dynamic references (INDIRECT/OFFSET) whose target is still dirty re-plan in the same recalculation;
  - readers of newly spilled cells recalculate in the same request, and a whole-column reader sees a spill committed earlier in the same pass;
  - `Table[Col]` readers order after formulas in the table body;
  - restored formulas stay current after undoing a row/column insert or delete (FORM-000117);
  - a missing table under the `BestEffort` default evaluates to `#NAME?` (was `#N/IMPL!`).

  Also changed: inspection work budgets charge one unit per reported reader, and `FormulaPlaneMode` is accepted and ignored (spans are never placed). See [migrating to the dependency authority](docs/dependency-authority-migration.md).
- **Breaking (low-level API):** legacy's dependency structures are available only with the new `legacy_oracle` feature of `formualizer-eval` (a differential-testing aid, never a runtime path). The following are gone from normal builds:
  - modules `engine::csr_edges`, `engine::delta_edges`, `engine::topo`;
  - `engine::Scheduler`;
  - `DependencyGraph::{get_dependents, get_dependencies, get_range_dependencies, add_dependency_edge, add_edges_nobatch, build_edges_from_adjacency, add_range_edges, rebuild_edges, flush_pending_edge_deltas, edges_delta_size, edges_rebuild_count}`;
  - the public field `NamedRange::dependents` (no public replacement: inspection excludes name-mediated readers, as before).

  `EvalConfig`'s Pearce–Kelly and stripe knobs are accepted and ignored. `ChangeEvent::RemoveVertex`'s edge fields and `VertexSnapshot::out_edges` are always empty. `InspectionUnavailableReason` gains `DependencyAuthorityUnavailable`. The `unified_authority` feature is a no-op. The [migration note](docs/dependency-authority-migration.md) lists replacements.
- **Preparation policy default is now `BestEffort`.** A formula that references a sheet or table that does not exist is accepted instead of failing preparation ("Sheet not found", "Undefined table"). It evaluates to an error value while the target is missing, which `IFERROR` and friends see like any other cell error, and it re-binds and recalculates when the sheet or table is added (`add_sheet`, implicit sheet creation, `define_table`). This applies to direct assignment, batch and deferred ingest, and imported workbooks. The previous behavior is kept exactly under `PreparationPolicy::Strict`: set `EvalConfig::preparation_policy` / `with_preparation_policy(PreparationPolicy::Strict)` in Rust, or `EvaluationConfig.strict_preparation = True` in Python (new property). References to a *removed* sheet are unchanged under both policies (`#REF!`, healed when the sheet returns). Tests that used a missing sheet to provoke a preparation failure now opt into `Strict` explicitly. See [preparation errors](docs/preparation-error-policy.md).

### Performance

- Row and column inserts and deletes move each column run of copied formulas as one block: the run is split at the edit, its template is adjusted once (checked on the run's first and last formula), and its rows or column shift. Formulas the block cannot express (a reference that crosses the edit inside the run, one that becomes `#REF!`, a small range that grows or shrinks) keep the per-cell path. With a change log, the run's per-cell `FormulaAdjusted` events are kept as one record and built only when the log is read or undone; `ActionJournal`s still hold every event. Inserting two rows into the middle of a 50k-row family takes 22 ms (0.25 s before Program 2, 2.6 s earlier in this cycle: every per-cell move scanned all formulas) and the recalculation after it 8 ms instead of 30 ms; a logged insert 78 ms instead of 0.40 s.
- The evaluation schedule of a large plan sorts the planner's runs (a family's cells at one layer) instead of its cells: 1M-formula first calculation 611 -> 523 ms.
- `formualizer-common`'s `CoordBuildHasher` now finishes every hash with a full avalanche (murmur3 `fmix64`). For cell keys on one sheet, the bits a hash table indexes with depended only on the column and the top bits of the row, so all rows below 8192 of a column shared one probe position, and inserting a filled column into a cell-keyed map cost time proportional to the column for every cell (50k cells: 72 ms, now 1.2 ms). Hash values change; nothing depended on them.
- Family members no longer have entries in the dependency graph's cell map, formula map and sheet index. A column of relative copies with consecutive vertex ids is stored as one run (sheet, column, rows, first id, template) and found by cell or by id through the run. The bulk loader numbers each sheet's formula cells column by column so that a column is one run, and installs load-time members as runs directly. A member moves back into the maps when it is edited, and a structural edit moves them all back until the next rebuild. The vertex store keeps its per-vertex position, kind, cached-value and edge-count columns in pages of 1024 vertices, and a page filled by such members keeps no rows (they are derived from the run), leaving one byte of flags per member. On a 1M-formula model, the heap after the first calculation is 104 MB instead of 257 MB, the peak during it is 250 MB instead of 388 MB, and loading takes 8.8 s instead of 10.2 s. Values, formulas, vertex ids, change logs and undo/redo are the same with compression off (`EvalConfig::formula_compression = false`).
- Loading no longer builds a formula tree for every copied formula. While a sheet streams in, each formula is compared with the formula above it (or to its left), relocated to its cell; a copy becomes a reference to that family's template and its own tree is never built. Formula dependencies are planned 10,000 cells at a time instead of all at once, and the dependency authority shares one set of facts per family. The ingest builder no longer reserves a full 32k-row chunk of every column before a sheet's first row. Peak heap during load drops from 1.87 GB to 0.39 GB on a 1M-formula synthetic finance model, from 153 MB to 35 MB on the largest Enron workbook, and from 141 MB to 15 MB on a wide Enron sheet. Load takes about 0.7x the previous time on the Enron sample. `Engine::stage_formula_ast` and `FormulaFamilyGrouper` expose the same staging to other loaders.
- Families of formulas built from operators, `IF`, `ROUND`, `ABS`, `MIN`, `MAX`, `SUM` (of cells and expressions), `AND`, `OR` and `IFERROR` evaluate over typed number lanes. A cell that holds a plain number with no number format is read from the merged number lane of its column. Any other cell, and any member whose function operands are not plain numbers, takes the per-cell path, so values, errors and formats are unchanged. First calculation of the 1M-formula model takes 0.41x its previous time.
- `SUMIF(S)`, `COUNTIF(S)` and `AVERAGEIF(S)` families whose ranges are fixed index those ranges once per family. Numeric comparisons and text `=` / `<>` use the index; other criteria keep the previous path. A 1,000-row `COUNTIFS` report over 100k facts calculates in 37 ms instead of 147 ms. `MIN`, `MAX` and `COUNT` over moving windows use the same range kernels as `SUM` and `AVERAGE`.
- A recalculation of one formula cell skips the planner's discovery and topology passes unless the cell reads itself.
- Recalculation cost no longer grows with the size of the workbook or of the session. A recalculation that wrote a few cells into a column the first calculation had written as a block copied that block's untouched part (up to 32k cells per write) and left one more piece that every later read scanned; the written cells now shadow the block and fold back into it in bulk, and splitting a block shares its storage. The dirty set kept the capacity of the first calculation (every later recalculation iterated it) and kept every value cell an edit had marked. A one-cell recalculation of a 512k-row column takes about 5 µs, against 519 µs before.
- Layers of cheap formulas no longer wake the thread pool: a layer runs sequentially until the rest of it is estimated at 150 µs or it has run for 300 µs, then the rest runs in parallel. Formula-result number formats moved from one lock to a sharded map that is not locked at all when no result has a format. Compression decides per family in parallel and frees the dropped reference texts off the critical path; with a thread pool, its reference-text check no longer runs during load. Scheduling a full recalculation sorts on the pool, and a small recalculation's plan sorts by comparison. A committed family run enters the write buffer as one block.
- Loading keeps small sheets small. The Arrow ingest builder reserved every column's lanes for a full 32k-row chunk and Arrow kept that capacity in the finished arrays, so every column of a small sheet (and every sheet's last chunk) held 32k rows of storage. A chunk at most half full now keeps exact-size lanes. Retained heap after the first calculation is about 0.28x the previous build on the Enron sample and 0.17x on the two real models.
- A computed General result now clears a date or other number format the same cell's previous result left in the computed format lane on the small-layer write path, as the batched path already did.

- Evaluation per formula cell no longer writes a per-vertex format handoff map that nothing read, and a value edit no longer scans that map (it made every edit O(formulas)). Overlay point maps use a faster hash; reads shorter than an overlay's point map probe offsets instead of scanning every point, short point-only reads merge per offset, and merged overlay lanes are reused across reads of a chunk once they have been requested more than once. Sheet lookups by name no longer allocate. The planner's radix sorts skip passes over constant key bytes and charge their work in batches.
- Made repeated builtin loading a no-op once every builtin is registered. `load_builtins` runs from every formula-planning snapshot (once per ordered-fallback proposal on the Calamine authoritative route) and from template canonicalization, and it used to inspect the metadata of all ~409 builtins and take the registry write lock for each on every call. A completed pass is now remembered until a user registration displaces a builtin, and an already-registered builtin is detected under a read lock before any metadata inspection. User overrides of builtin names and their restoration by the next load behave as before. Remaining snapshot work is one metadata inspection per requested function.
- Formula preparation reuses per-shape analysis within one load or batch. Formulas that are relative copies of each other (filled down or across) share canonical keys, labels, slot descriptors, read projections and the dependency visit order; each cell still resolves its own references, range expansion, read summary and slot map. Tables, 3-D, external, spill and one-sided open references, temporal, error and array literals, and parsed-tree inputs keep the per-cell path. Reuse requires a function provider that reports a planning revision (providers without one keep the per-cell path), and is dropped when that revision or the global function registry changes, including a registration in the middle of a load. Memory per preparation pass is bounded: at most 16,384 shapes, a 1 Mi-token budget, formulas over 4,096 key tokens or 1,024 references are ineligible and stop the key walk at the bound, and retained specialization payloads (canonical keys and expression, literal text, names and sheet names) are charged an estimated byte size against a 64 MiB budget with a 256 KiB per-entry cap. Colliding shape hashes fall back to the per-cell path instead of growing comparison work. When a pass stops finding repeated shapes (families that differ by a literal per row), it backs off: after 64 formulas without a reuse it checks only every second formula, then every fourth, down to one in 64, and returns to checking every formula on the next reuse, so a family that starts late is still found within 192 of its formulas. Test builds re-derive every reuse on the per-cell path and compare the results.
- Stopped the Arrow ingest builder from allocating full-capacity lane builders it then drops. After a chunk flush, the next chunk's builders are created only when another row arrives, so the final flush of a sheet load (partial or at an exact chunk multiple) allocates nothing. Text payload space is no longer pre-reserved at 12 bytes per row for every column: the first chunk grows it on demand and later chunks reserve what the column's previous chunk used. Chunk layout, lane presence, null semantics and formats are unchanged.

### Fixed

- Fixed `SEQUENCE` with an empty optional argument, such as `=SEQUENCE(3,,2)` or `=SEQUENCE(3,1,,2)`. The empty slot was coerced as a value, which gave `#VALUE!`, a start of `0` or a step of `0`. `columns`, `start` and `step` now default to `1` when omitted, as Microsoft documents; an explicit value, including `0`, is still used.
- Fixed the comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) coercing across types. `TRUE=1` and `"5"=5` were `TRUE` and `TRUE>1` was `FALSE`, because both operands were converted to numbers. Excel ranks types instead, number < text < boolean, and compares values only within a type; a blank cell takes the other operand's type as its zero value. The operators now do the same, including element by element over arrays and ranges (`=SUMPRODUCT(--({1,"1",TRUE}=1))` is `1`). Arithmetic, lookups and `COUNTIF`/`SUMIF`-style criteria are unchanged. Part of #291.
- Fixed `INDEX` and `OFFSET` returning `#REF!` when their reference is a defined name, such as `=INDEX(Vals,3)` or `=SUM(OFFSET(Vals,1,0,2,1))` with `Vals` naming a range (#374). The name now resolves to the region it names, including a single cell, a range on another sheet and a sheet-scoped name. For a name bound to a constant or a formula, `INDEX` indexes its value and `OFFSET` answers `#VALUE!`, as Excel does. `OFFSET` also reports the error its reference produced (an undefined name is `#NAME?`) instead of always `#REF!`.
- Fixed LET locals and LAMBDA parameters bound to a range losing the range when passed where a reference is expected (#373). `=LET(r,A1:A3,MATCH(2,r,0))` answered `#NAME?` and `=LET(r,A1:A3,SUM(OFFSET(r,1,0,2,1)))` `#REF!`; Excel answers `2` and `5`. Such a local is now the range in every reference position: `MATCH`/`VLOOKUP`/`HLOOKUP`/`LOOKUP` lookup ranges, `ROWS`/`COLUMNS`, `OFFSET`'s base, selections by `IF` and `CHOOSE`, and reference-aware value consumers (`=LET(r,D1,SUM(r))` skips text in `D1` as `=SUM(D1)` does). A local bound to a computed value is refused with `#VALUE!` in a by-reference slot, as in Excel, and stays a value elsewhere. A local bound to a reference-returning function (`INDEX(...,0,1)`, `OFFSET`, `INDIRECT`) still holds only its value. `=LET(p,FALSE,OR(p))` now answers `FALSE` instead of `#REF!`.
  - **Low-level API:** `interpreter::LocalBinding` gains the `ValueWithReference { value, reference }` variant (exhaustive matches on `LocalBinding` need an arm), and `CustomCallable` gains `invoke_with_references`, which defaults to `invoke`, so existing implementors compile unchanged.
- Fixed `SUMIF`, `SUMIFS`, `COUNTIF`, `COUNTIFS`, `AVERAGEIF` and `AVERAGEIFS` when an error value stands where a range belongs, such as the `#REF!` Excel leaves in `=SUMIFS(F:F,#REF!,K2)` after a referenced column is deleted. The error was read as a one-cell range that matches nothing, so the sums and counts returned `0` and the averages `#DIV/0!`. Excel returns the error itself, and so do these functions now. Errors held in the cells of a criteria range are still skipped, as before.
- Fixed parsing of external-workbook ranges such as `[16]jan94!$A$53:$IV$163`, `[1]Sheet1!A:A` and `'[Book.xlsx]My Sheet'!A1:B2`. The tokenizer took the `[book]` qualifier for a structured-reference bracket and split the text at `:`, so the formula parsed as an external cell, the range operator and a *local* reference to the end corner on the formula's own sheet. It is now one external range reference, the same as the local form. In the engine it binds to the source table named by its full text and no longer depends on the local end-corner cell. Unbound, the outcomes are unchanged: a finite external range still fails preparation with `#NAME?`, and a whole-row or whole-column one still loads and evaluates to `#REF!`. Colons between two qualified operands (`[1]S!A1:[1]S!B2`) and between structured references (`Table1[A]:Table1[B]`) are still range operators.
- Fixed the engine's stored copy of postfix-call formulas such as `=LAMBDA(x,x+1)(B1)`. The formula arena replaced the call with a `#N/IMPL!` literal, so reading the formula back (`get_cell`) returned that literal, and anything rebuilt from the stored copy lost the callee and its references. The call is now stored with its callee and arguments, reads back as written, and its references follow row and column edits. Evaluating such a call still returns `#N/IMPL!`. `AstNodeData` is unchanged: a call is stored as a function node with the reserved name `#CALL`, which no parsed function can have.
- Fixed defining a name over a whole-sheet (or any range of 2^32 or more cells) range. The range's cell count was computed in 32 bits and wrapped, so the range passed the small-range expansion check and was expanded cell by cell: a panic in debug builds and an effectively endless loop in release. The count is now computed in 64 bits and compared with the limit before any narrowing, so such names bind as range dependencies like other large ranges.

## [0.9.3] - 2026-09-11

- Aligned Rust product crates and Python/npm bindings at 0.9.3. Parser/common move together to **3.1.2** because the date/time text parsing change below lives in `formualizer-common`; `formualizer-parse` moves in lockstep with no source change and product crates now pin `formualizer-parse = "3.1.2"`. SheetPort spec remains 0.3.1.

### Changed

- Date/time text parsing accepts more Excel-compatible forms: single-digit years (`"1/2/5"`), `24:00`/`24:00:00` as midnight, truncated fractional seconds on a full `HH:MM:SS.f` field, and month-year-only forms (`"Jan 2003"`, `"January 2003"`) resolving to the first of the month. The month-year form requires a four-digit year, so `"Jan 3"` is not read as `2003-01-01`, and fractional-second truncation only fires when the dot terminates a full `HH:MM:SS` field, so a single-colon `"12:00.5"` is rejected as `#VALUE!` rather than silently accepted as `12:00`. Partially addresses #290; the `"24:00"`-as-a-full-day arithmetic case, sub-second retention, and `DATEVALUE`/`TIMEVALUE` datetime routing are tracked separately in #416. (#398)

### Fixed

- Computed the standard normal CDF with the Hart (1968) rational approximation as given by West (2005), replacing the Abramowitz & Stegun 7.1.26 formula that was accurate only to about 7e-8. `NORM.S.DIST`, `NORM.DIST`, `LOGNORM.DIST`, `GAUSS` and `Z.TEST` now agree with a 90-digit reference to about 2e-16 absolute error across the sampled real line, and `NORM.S.DIST(0,TRUE)` is exactly 0.5. Corrected a mistyped Cody CALERF constant in the small-argument `ERF` branch that skewed `ERF`, `ERF.PRECISE`, `ERFC` and `ERFC.PRECISE` by up to about 1e-7 near the origin. Relative accuracy in the far tails, large-argument `ERFC`, the inverse-normal family and the `Z.TEST` sample-versus-population deviation convention are tracked separately in #464. Contributed by @cloudexible. (#458, #461)
- Fixed endian-sensitive Bessel word extraction and integer-overflow paths, preserving extreme-order parity. `BESSELJ`/`BESSELY` now reject recurrence orders above 1,000,000 with `#NUM!` instead of risking unbounded work; existing constant-time paths remain available. No order-only or Debye cutoff fabricates zero/infinity for representable results. (#465)

- Prevent spills from overwriting pending formulas without preparing blockers; retain bounded, admitted retry regions and release failed reservations. Existing interactive Empty-overlay visibility remains unchanged.

- Fixed logged workbook formula setters silently swallowing binding/admission failures and clearing existing spills on rejection. Rejected assignments now report errors like unlogged setters; existing deferred-validation and non-atomic batch policies remain unchanged. Added fallible low-level editor APIs without changing legacy signatures. See [formula assignment contracts](docs/formula-assignment-failures.md). (#451)
- Fixed dependency edges disappearing across split bulk-ingest calls and stale consumers after bulk formula replacement. Complete initial loads retain their fast path. (#456)
- Preserved imported/staged formula sources after compressed preparation failures so inspection, edits and retries remain possible; corrected inspection of formula text without a leading `=` without changing genuine string-literal formulas. (#452, #455)
- Fixed blank criteria masks mistaking absent numeric/boolean text-lane data for empty cells, without adding numeric-string caches. Mixed wildcard masks now preserve the existing scalar matching policy across imported and edited data. (#457)
- Counted implicit blanks in COUNTIF/COUNTBLANK ranges arithmetically while keeping physical views bounded, including direct whole-row/column extents and available spill members beyond graph placement bounds. This addresses part of #285, not all blank-range or cross-engine criteria semantics. See [criteria compatibility](docs/criteria-ingest-compatibility.md).
- Isolated ordinary and supported shared/fragmented Calamine formula targets from unrelated preparation failures using indexed source selection. Partial families retain validated residual compression; complete-family demands retain compressed preparation without per-member AST expansion. Preserve source order, selected edits, retries and retained locator admission. Unsupported/reconciliation-dependent sources remain conservative; shared coordinate metadata adds disclosed linear cold storage. See [target preparation](docs/imported-target-preparation.md). (#453)
- Documented and tested the existing preparation-error boundary: spreadsheet guards do not turn unresolved-sheet/table preparation or request-level admission/cancellation failures into fallback values. No new reference-error policy was introduced. (#454)

### Python bindings

- Added a Python undo/redo/cancel regression suite with real outcome assertions; the two pre-existing defects it exposes (#301, #412) are pinned as strict expected failures rather than hidden. Contributed by @Ocean82. (#401)

### Security and hardening

- Bumped the docs site's `fumadocs-mdx` from 14 to 15, dropping the transitive `js-yaml` dependency flagged by SNYK-JS-JSYAML-18313070, and aligned `fumadocs-core`/`fumadocs-ui` with the MDX upgrade. Docs-site only; no published package payload changes. Contributed by @Ocean82. (#402)

## [0.9.2] - 2026-09-10

- Aligned Rust product crates and Python/npm bindings at 0.9.2; parser/common remain at 3.1.1 and SheetPort spec at 0.3.1. Cache-only XLSX APIs are additive and explicitly invoked; Python support is native-only, not Pyodide.
- Added explicitly invoked cache-only XLSX recalculation through Calamine, available by default in `formualizer-workbook` and the facade through the removable `xlsx-recalc` feature, with bounded namespace-aware admission, typed cache repair, source-epoch handling and package-preserving ZIP32 writeback. Unsupported metadata/results fail without publishing partial output.
- Exposed the shared cache-only core through Rust byte/file APIs, Python byte/file functions and a WASM `Uint8Array` API, without changing existing workbook APIs.
- Added cancellable Calamine byte ingestion and cooperative read/row/replay checkpoints.

- Added `UmyaAdapter::into_document` for both Umya backends, allowing evaluator ingestion and rich-document ownership to share one import without cloning or serializing the workbook.

- Added the opt-in `umya3` backend to `formualizer-workbook` and the facade (0.9.1), sharing adapter algorithms with the unchanged Umya 2 backend.
- Fixed lost border colours on Umya 3.1 XLSX import and colour-selector collisions during export using paired, bounded compatibility I/O. The cold writer repairs emitted colour definitions and style references from the authoritative document, retaining RGB/indexed/theme identity and tint without reimporting a workbook or evaluator.

## [0.9.0] - 2026-09-06

### Highlights

- Improved lookup, reference-returning conditional, LET/LAMBDA, and temporal-value compatibility.
- Fixed stale results after structural, named-symbol, logged, and atomic edits, with broader FormulaPlane parity coverage.
- Reduced repeated criteria work, registry locking, range traversal, and schedule copying; exactly converged nonvolatile cycles can remain clean between recalculations.
- Added Python workbook clock control, hardened parser and binding error paths, and corrected release package contents.

### Compatibility notes

- Rust callers constructing or matching `RangeKey::OpenRect` must use its four independent optional axis bounds instead of the former two optional coordinates. (#371)
- Review temporal egress and runtime XLSX source selection below when upgrading. The `mmap` feature is now a compatibility no-op; mapping is an explicit runtime choice.
- Product crates and Python/npm bindings move to **0.9.0**; common/parse move together to **3.1.1**. `sheetport-spec` remains **0.3.1** and the internal C ABI crate remains **0.1.0**.

### Changed

- **Exactly converged circular cells are no longer re-run on every recalc.** Under `CyclePolicy::Iterate` the engine redirtied every iterating SCC at the end of each recalc, so every circular cell in a workbook was re-evaluated on every recalculation, like Excel. An SCC is now retained clean across recalcs when the recalc that iterated it stopped because every member reproduced its previous value exactly — `|Δ| == 0` for numbers, identity for text, booleans and errors, and never a NaN identity — before the `max_iterations` cap, with no volatile and no dynamic-reference (`INDIRECT`/`OFFSET`) member. Such an SCC is a fixed point of its own inputs, so re-running it with the same inputs cannot change a value: it runs again only when the dirty graph reaches a member (a precedent edit, a formula change, a structural edit, a name rebinding) or when a config knob that can change its result changes between recalcs (cycle policy or tolerance, date system, workbook seed, volatile level, deterministic mode, range-expansion limits) or when a function that a member calls is registered or replaced in the function registry. Tolerance-only convergence (`|Δ| < max_change` but non-zero), capped SCCs (including the `max_iterations: 1` accumulator contract) and volatile cycles keep re-running every recalc, exactly as before. Retention is unconditional (there is no knob): it changes only the work done, never a value, and retained SCCs are invalidated by the same machinery that invalidates every other formula. `CycleTelemetry::reused_sccs` and `CycleTelemetry::reused_scc_members` count the retained SCCs served without a re-run in a request, and `EngineBaselineStats::retained_scc_members` reports the retained population. Measured in release on 78 ring SCCs of 120 members each (9,360 circular cells) plus three downstream column families: a no-change recalc goes 70 ms -> 0.3 ms, an unrelated-cell edit 84 ms -> 0.9 ms, and an edit feeding one SCC goes 72 ms -> 2.6 ms with exactly one SCC re-run (two passes), on identical checksums. (#368)

- FormulaPlane correctness now covers mixed-pass named-symbol scheduling with dynamic fail-closed preflight, format preservation across broadcast and computed-overlay writeback, transactional demotion of retained spans before Off-mode legacy evaluation without FormulaPlane dispatch, and dependent healing after name redefinition.
- FormulaPlane now admits `IF`, `IFS`, and `CHOOSE` spans when every result arm is a single cell or a statically scalar expression. Range, open-range, named, volatile, and dynamic-reference arms remain on the legacy path.
- Added the `CELL` and `HYPERLINK` builtins. Per the determinism decision in #239, `CELL` implements only the workbook/cell-property info types — `contents`, `address`, `col`, `row` and `type` — while host-dependent selectors (`format`, `filename`, `parentheses`, `prefix`, `protect`, `width`) and calls without a reference return `#VALUE!`. Argument errors propagate with their own kind (`=CELL("row",1/0)` is `#DIV/0!`), `contents` and `type` accept a literal in the reference position, and 3-D references are rejected. `HYPERLINK` returns its friendly name or link location as text. `CELL` is context-dependent (`WorkbookMetadata`) like `SHEET`/`SHEETS`, so the formula-plane ingest rejects it. Off-sheet `address` qualification is fixed as part of #333; `CELL("contents", <blank>)` still yields Empty rather than Excel's `0` and is left open there pending the #319 blank-coercion study. (#329)
- Calamine XLSX path loading now selects its retained backing source at runtime with `XlsxPathSource::{SharedFile, DirectMmap}`. `SharedFile` remains the safe default; explicit `DirectMmap` never silently falls back and carries a caller-enforced no-destructive-mutation contract. Python exposes the same choice through `XlsxPathSource` on `load_workbook` and `Workbook.load_path`.

  **The `mmap` Cargo feature no longer selects any behaviour.** Enabling it does not change how `SpreadsheetReader::open_path` loads a file: the default source is `XlsxPathSource::SharedFile` with or without the feature, and `XlsxPathSource::DirectMmap` is the only way to obtain a memory mapping. The feature is retained purely so existing `features = ["mmap"]` declarations keep compiling; it is a no-op alias and can be dropped from downstream manifests. A test compiled only under `--features calamine,mmap` pins this.

  **`memmap2` is now an unconditional dependency of the `calamine` feature** (on `unix` and `windows` targets; it is not pulled in on wasm). It was previously reachable only through `mmap`. Keeping it behind `mmap` was considered and rejected: because mapping is now a runtime choice on a public enum variant, gating the dependency would make `XlsxPathSource::DirectMmap` return `ErrorKind::Unsupported` in every default `calamine` build — including the Python extension, which enables `calamine` but not `mmap` — and would reintroduce exactly the build-time behaviour selection this change removes. `memmap2` is a small, dependency-free crate, so the unconditional cost is one extra compilation unit.

- The Calamine `SharedFile` XLSX source now keeps a 64 KiB per-cursor readahead window behind the shared file lock, so a ZIP scanner's many small reads coalesce into few locked `seek`+`read` pairs. The window is keyed by absolute offset, so seeks landing inside it are served from memory. Measured on a 60 MiB, 12-sheet package with 48 scoped names and eight external-link targets, counting `read` syscalls on the workbook fd: metadata-forced 268 -> 17, open-only 69 -> 11; on a 46 KiB package, metadata-forced 255 -> 14. Wall time is unchanged. Snapshot semantics are unchanged — `SharedFile` was never a snapshot, and concurrent truncation still surfaces as ordinary EOF/I/O/ZIP errors.

- Named-range definitions now track structural inserts and deletes like all other references; previously they stayed pinned to their original coordinates. Deleting a band containing a name target now invalidates the definition to `#REF!`. (#170)
- Computed temporals are numeric during evaluation (`ISNUMBER`/`TYPE` now match Excel). Native scalar, range, table, Python, and SheetPort egress materializes date/time values from the cell's effective format; callers can opt into uniform raw serials. A known datetime class preserves midnight datetimes, while calamine's code-lossy date-ish signal can only classify pure fractions as time, day-plus-fraction serials as datetime, and integers as date.
- Arrow dependencies upgraded 58.2 → 59.2 across `formualizer-eval` (`arrow`, `arrow-array`, `arrow-buffer`, `arrow-schema`, `arrow-select`, `arrow-cast`). No API or behaviour change; full suite green on the pinned surface, native and wasm32.

- **Parser/SDK track `parse-v3.1.1`.** The previously published 3.1.0 track introduced `NumberFormat`, `FormatClass`, and `numfmt::builtin_code`. This release advances both parser-track crates to 3.1.1 for parser hardening and package-content corrections; product crates now pin `formualizer-parse = "3.1.1"`. Release preflight requires matching common/parse versions and actual registry availability before the product track can publish. (#439, #444)
- Pyodide wheel documentation now reflects distribution reality: native wheels are on PyPI, while Pyodide wheels are built and smoke-tested and retained as the `wheels-pyodide` Actions artifact for download or self-hosting from a compatible wheel URL.

### Performance

- Bounded range traversal to relevant row segments and projected numeric/error lanes without repeatedly materializing unrelated values. Preserved order, overlays, ownership and cancellation behavior. Synthetic results and the small generic-head regression are documented in the benchmark report; these are not universal workbook speedup claims. (#436)
- Shared immutable cached legacy schedules instead of deep-copying nested vectors on eligible requests. Cache eligibility and target routing are unchanged. Synthetic same-chain edit/recalculation medians improved, with shared-host attribution limits and a small alternating-case regression retained in the report. (#441)

- **Reduced criteria, registry, and exact-lookup overhead.** Criteria masks are memoized per invocation, predicate, and column with a conservative 1 MiB admission charge; ordinary registry hits take a read lock and clone only the current function handle; exact indexes are reclaimed at exclusive Engine snapshot-mutation boundaries with retained-payload admission accounting. Matching rules are unchanged; tranche probes show less repeated mask work and fewer direct-registry allocations, while no-edit lookup recalculation reuses the index. (#431)

### Fixed

- Resolved LET/LAMBDA locals before workbook named-range lookup in range-consuming arguments, so expressions such as `LET(r,A1:A3,SUM(r))` work and local names consistently shadow workbook names. (#339)
- Preserved selected references through `IF`, `IFS`, and `CHOOSE`, allowing compositions such as `OFFSET(IF(TRUE,A1,B1),2,0)`. `IFERROR` and `IFNA` remain value-returning. (#369)
- Propagated `IF` condition errors unchanged without evaluating either arm. (#372)
- Made bounded single-cell `INDEX` selection read only the selected cell, avoiding phantom cycles under runtime cycle detection with bulk ingest. Static-cycle behavior and other INDEX shapes remain unchanged. (#370)
- Preserved independent row/column bounds in open ranges, fixed zero-column plan labels, and handled wholly unbounded range dependencies without dropping coverage. (#371, #377)
- Treated an omitted `XLOOKUP` fallback as absent rather than numeric zero, and kept exact text needles from matching numeric cells. (#340)

- Logged formula/topology edits, atomic commits, and undo/redo now invalidate cached schedules and retained plans according to the replayed operation. Partial replay failures invalidate before returning; existing-cell value-only edits preserve eligible schedule hits while clearing lookup snapshots. (#435)

- Logged Engine mutations now use a complete operation-local capture for graph rollback, Arrow mirroring, FormulaPlane invalidation, and explicit action journals before publishing to the caller's optional audit `ChangeLog`. Disabled, zero-cap, and saturated audit policies no longer make a direct edit or atomic rollback incomplete, and failed actions no longer evict older retained audit entries. Retention policy is unchanged: index-based `undo_logged` can replay only events still present in its external log and cannot recover disabled or evicted history; explicit `ActionJournal` undo/redo remains the retention-independent path. (#434)

- **FormulaPlane first evaluation is linear again when a rejected legacy formula column sits next to accepted spans.** The mixed-schedule dirty projection merged every changed cell into its consumer's dirty domain with a clone-and-compare `Vec` union, so a column of `n` dirty legacy cells cost O(n²) on the all-dirty first evaluation whenever any span was present on the sheet. On a running-balance column (`InternalDependency` reject) plus two unrelated span families, the Authoritative penalty over `Off` grew 37 → 168 → 736 → 3,100 ms across 2,400 / 4,680 / 9,360 / 18,720 rows (4.2x to 4.5x per doubling, `probe-fp-reject-chain --spans 2`); it is now 2 → 13 → 9 → 56 ms, an Authoritative/Off ratio of 1.03x to 1.11x at every size. Dirty domains now accumulate through a hash-backed `DirtyDomainAccumulator` shared by the closure and the mixed scheduler, with identical output (arrival order, cross-kind dedupe, `Whole` absorption). `probe-fp-reject-chain --spans 2 --max-ratio 3` pins the shape in the parity workflow. (#405)
- Approximate `MATCH` over an array literal now applies the same sortedness guard as the reference-backed path and as `VLOOKUP`/`HLOOKUP`, so `MATCH(2.5, {3,2,1}, 1)` returns `#N/A` on data that is not ascending instead of a silently wrong position. Previously the array-literal branch called the unguarded search, so an ascending `MATCH` over descending data returned a match while the equivalent `VLOOKUP(..., TRUE)` returned `#N/A` — a new inconsistency introduced by the very change meant to make lookups consistent. The guard now lives in the shared `binary_search_match` (ascending for `match_type` 1, descending for -1). The `MATCH`, `VLOOKUP` and `HLOOKUP` docstrings, and the generated `vlookup`/`hlookup` reference pages, are corrected to describe the `#N/A`-on-unsorted behaviour rather than the previous "can return incorrect rows". (#283)
- Exact `MATCH`, `VLOOKUP` and `HLOOKUP` no longer select a blank candidate as numeric zero; their blank lookup values still select real numeric zero, including signed `-0`. In exact and wildcard modes, `XMATCH` and `XLOOKUP` instead match a blank lookup value only to a blank candidate, choosing the first or last blank according to search direction. Numeric zero never selects a blank, empty text remains distinct, and indexed and scan paths agree. (#319)
- FormulaPlane span evaluation now fails closed when a placement produces an array result instead of collapsing it to the array's top-left element. A span publishes exactly one value per placement, while the legacy evaluator routes `LiteralValue::Array` results into the spill planner, so the collapse would have broadcast a single value across the whole span while legacy spilled a rectangle. The sink now returns a typed error before anything is published; the coordinator drops the layer's uncommitted write buffer, transactionally demotes the offending span to legacy vertices, and replans, so the placements re-evaluate on the legacy path with correct spill semantics. No stock template reaches this today — it is a fail-closed backstop for future dynamic-array admission — and it is pinned by a test-only function with declared-scalar semantics that returns an array at runtime. (#388)
- **A row or column delete inside a FormulaPlane span no longer serves pre-delete values when the span's reads are displaced from its placements.** A span whose formulas read a band elsewhere on the sheet (e.g. `C150:C270` holding `=A{r-140}`) survived a delete that straddled its result domain by *compacting*: the domain closed over the deleted band, the template AST was kept verbatim and only the origin was carried through the delete. That keeps one relative offset for the whole surviving domain, but placements above the band keep their authored offset while placements below move up and must read the same cells as before — two different offsets. Deleting rows 200:201 left `C200` serving `=A60`'s value where Excel and the legacy per-cell path serve `=A62`'s; trimming the head with the origin row surviving diverged the same way. Compaction is now gated on an explicit frame identity (read, placement and origin displacement must satisfy `d_r == d_p - d_o` for every relative read bound of every surviving placement, and no read bound may land inside the deleted band); when it does not hold the span splits at the deleted band into an untouched upper half and a shifted lower half — the same surgery mid-domain inserts already use — and demotes to per-cell formulas when that split is not provably clean. Lockstep deletes, tail trims and whole-span shifts still compact into a single span. (#171)
- **Deleting or rebinding a name no longer leaves formula-backed dependent names serving stale values.** `DependencyGraph::delete_name` and `update_name` dirtied the deleted name's dependents with a non-propagating mark, which is correct for a grid formula (a leaf of the invalidation walk) but wrong for a dependent that is itself a name vertex: a `NamedDefinition::Formula` such as `B_name = "=A_name*2"` was dirtied and recomputed, while every cell reading `B_name` kept its cached value. With `A_name -> $A$1`, `B_name = "=A_name*2"` and `C1 = "=B_name"`, deleting `A_name` left `C1` at its old number instead of `#NAME?`, and later writes to `A1` did not reach it either. A cell referencing the deleted name directly always invalidated correctly; only the name-to-name edge was missed. Both paths now propagate through `mark_dirty_many` (which honours deferred-dirty scopes), and `delete_name` additionally re-extracts the dependency edges of affected name vertices via `rebuild_name_dependencies`, the same healing pass cell formulas already received, so a later define re-binds them. (#365)

- FormulaPlane computed-format writes now commit with values and stale-sideband purging only after authoritative commit-window preflight, without per-placement locks or virgin-lane format hash work. General recomputation clears only covered computed formats, and General source-format runs now fall through to computed formats while genuine source and user-overlay formats retain precedence.
- SheetPort ports now coerce temporal values to the manifest-declared type at the port boundary: `number`/`integer` ports receive serials regardless of the engine's `TemporalEgress` policy, and `date`/`datetime` ports receive native temporals; the manifest is the protocol contract, engine egress is only a default.
- Date arithmetic no longer conditionally propagates `LiteralValue::Date`; a position-keyed, non-sticky scalar format annotation carries temporal display class independently, fixing #312 without exposing annotations to bulk numeric kernels. Date-plus-time yields datetime, date-plus-percent and date-plus-plain-number yield date, and unspecified class pairs drop the annotation. Selection functions preserve the chosen scalar annotation, including `IFERROR`, `IFNA`, and scalar-argument `MAX`/`MIN`; `MAX`/`MIN` over multi-cell ranges do not yet recover the winning cell's format.
- Before merge, the unreleased format channel was hardened so row, column, and sheet structural edits purge affected derived-format positions, and value/formula writes invalidate format state in both ephemeral and interactive/changelog modes. This prevents shifted plain values and logged overwrites from inheriting stale temporal formats; neither pre-merge regression shipped.
- Temporal values saved through `Workbook::to_xlsx_bytes` round-trip as date-system-aware numeric serials instead of text, restoring arithmetic while leaving display and number-format fidelity to the format channel. (#355)

- Structural row inserts and deletes now invalidate compressed open-range readers when the edited axis intersects and an indexed occupied column crosses the range. Bounded formulas whose AST is adjusted remain conservatively dirtied, and column edits retain conservative cross-axis invalidation because Arrow has no cheap occupied-row index. (#313, #314)
- **A structural edit no longer reaches a defined name, table or external source.** Those three, plus sheet-scoped names, are identified by name and have no position on any sheet, but the graph gave them fabricated grid coordinates on a real user-visible sheet: the first workbook name landed on `Sheet1!$A$1`, the second on `$B$1`, and a table on its range's anchor cell. The only thing separating such a vertex from the actual cell at that address was its deliberate absence from the cell index — a convention, not a structural property. Two code paths broke it, and both are fixed by giving symbols their own address space. (#302, #304)

  A **default-sheet row or column insert** shifted a name's vertex onto an addressable cell *and* published it in the cell index at the new address. Every dependency resolved afterwards against that address then bound to the name instead of the cell. Three consequences followed, each measured against a matched control that varied only the insert row: a formula written later as `=A2+1` acquired `deps == ["NamedScalar@Sheet1!$A$2"]` rather than a direct cell edge, so editing the *name's* target dirtied an unrelated default-sheet formula; an ordinary data write at the hijacked address rewrote the name's own vertex from `NamedScalar` to `Cell`, silently destroying the name while `resolve_name_entry` kept returning it; and deleting the row the vertex then occupied stranded the name, after which `=Tracked+1` served `71` where `701` was correct. The same happened for sheet-scoped names, for tables, for expanded ranges that merely *covered* the address, and for the vertex's later positions after repeated inserts. (#304)

  A **structural edit on a completely unrelated sheet** dropped formula→name edges. Deleting row 1 or column 1 of the default sheet swept up any name vertex parked at `$A$1` as an ordinary resident of that row or column and deleted it, taking its edges with it — even when both the formula and the name's target lived on another sheet entirely. Subsequent changes to the name's target no longer dirtied the formula, which then served silently stale values. (#302)

  Symbol vertices now hold a `SymbolAddr` in an address space disjoint from the grid's, and the structures keyed by position — the cell index, the per-sheet range index, and the iteration that drives every structural edit — accept a `GridAddr`, which a symbol cannot produce. `NameScope` is now purely lookup metadata and no longer decides where a vertex lives. Evaluation results are unchanged; a name's scope, resolution and dirty propagation all behave exactly as before.

### Python bindings

- Added `Workbook.set_deterministic_clock` to pin time on an existing workbook for its next recalculation, including deterministic embedding and Pyodide use. (#341)
- Accepted `NImpl` and `Error` in `LiteralValue.error`, allowing all canonical error kinds to round-trip. (#354)
- Replaced panicking AST/Token conversions and a Sheet cache-lock unwrap with Python error propagation while preserving callback reentrancy guards. (#404)

### Packaging

- Included canonical project license texts in Rust, Python, and npm package payloads. Python's declared license remains MIT; shipping both repository texts does not change that declaration. The published `sheetport-spec` 0.3.1 payload is unchanged. (#444)
- Pinned native Rust/package release builds to Rust 1.93.0; Pyodide retains its xbuildenv-derived toolchain. Made release-preflight fixtures independent of the product version. (#442, #444)

### Security and hardening

- Bounded parser recursion to 72 parser frames so excessively nested formulas return a parser error instead of exhausting the stack, with accepted-boundary coverage for 64 nested calls and parentheses. The limit counts parser frames rather than Excel nesting levels; recursive destruction of very long flat ASTs remains a separate known limitation (#411). Original recursion guard contributed by @chiliec. (#408)
- Release preflight now fails closed when declared binding feature profiles drift from the canonical value-feature policy; Pyodide's explicit `system-clock` opt-out remains the only approved exception. (#433)

- Bumped the docs site to Next.js `16.2.11`, clearing nine npm advisories affecting `next` `16.2.6` (four high: GHSA-89xv-2m56-2m9x, GHSA-p9j2-gv94-2wf4, GHSA-6gpp-xcg3-4w24, GHSA-m99w-x7hq-7vfj). The docs site's `bun.lock` was removed; `pnpm-lock.yaml` is the only lockfile the site builds and deploys from.

### Known limitations

- Workbook undo can drop a dependent formula edge after writing a referenced empty cell (#301), leave a retained formula AST and dependency edge referring to different rows after undoing a logged row insertion (#303), truncate the changelog and lose the undone operation's audit record (#367), or leave an evaluated formula written to a fresh cell in place (#412). Long flat, left-associative chains can overflow the stack during recursive AST drop even when parsing returns `Ok` (#411).

### Contributors

Thanks to @Ocean82, @chiliec, and @tommy230 for contributed fixes, parser hardening, and independently captured lookup compatibility cases and reruns. The integration preserves original authorship. (#438, #439)

## [0.8.4] - 2026-08-14

### Changed

- **Loading a file no longer leaves a phantom default sheet, so sheet indices shift.** A freshly constructed `Workbook`/`Engine` is seeded with one default sheet (`Sheet1`), and every backend loader appended the file's sheets alongside it. Loading a workbook whose sheets were `Data` and `Extra` produced `sheet_names() == ["Sheet1", "Data", "Extra"]` — a sheet that does not exist in the file — and `SHEET()` on the file's first sheet returned `2` instead of `1`. The default sheet is now folded into the file's first sheet, so the same load produces `["Data", "Extra"]` with `SHEET()` returning `1` and `2`. This affects the calamine, umya, json and csv backends alike; previously only csv was accidentally immune, because its sheet name is `Sheet1` by default and so always collided with the seeded one. (#332)

  Concretely, for any file whose first sheet is not named `Sheet1`: `Workbook::sheet_names()` (and `wb.sheet_names` in the Python and WASM bindings) loses the leading `"Sheet1"`; `SHEET()` and `SHEETS()` return one less than before; engine `SheetId`s shift down by one (`Data#1` becomes `Data#0`); and a saved artifact no longer carries the injected empty sheet — measured, `to_xlsx_bytes()` after a calamine load wrote `["Sheet1", "Data", "Extra"]` into the output file and now writes `["Data", "Extra"]`. Sheets are addressed by name everywhere they are persisted (the JSON document format keys sheets by name, SheetPort manifests bind by sheet name), so no stored artifact embeds an index that this changes; the change is confined to in-memory ids, `SHEET()`/`SHEETS()` values, `sheet_names()` output, and the sheet list written into saved files.

  The fold happens only when the engine is otherwise untouched — one sheet, the default one, with no cells, no named ranges, no staged formulas and no row-visibility state. `EngineLoadStream::stream_into_engine` is public and takes a caller-supplied engine; when that engine already holds user content, the default sheet is left exactly as it is and the file's sheets are added alongside it, so existing data is never renamed away or overwritten. All four backends now route sheet registration through one shared entry point, `Engine::adopt_file_sheets`.

- **A file containing two sheets whose names differ only by case is now rejected at load** with `#VALUE!: Duplicate sheet name in workbook`. Sheet names are unique and case-insensitive in Excel, and `add_sheet` is idempotent, so such a file previously merged the two sheets silently and let the second sheet's cells overwrite the first's. (#332)

### Performance

- `Workbook::set_values` — the batch path behind `Sheet.setValues` in the JS/WASM binding — pre-allocates the Arrow sheet to the batch extent once instead of growing it a row at a time. Each per-cell growth rebuilt the whole column's type-tag array and every present lane, so a batch of N cells did O(N²) work. Measured natively on a single numeric column: 20,000 rows went from 835 ms to 126 ms, a 6.6× improvement, with per-cell cost flat rather than growing with N. Contributed externally. (#335)

### Fixed

- A name scoped to a single sheet no longer answers a query that asked for no scope. `has_name(name, None)` and `resolved_name_value(name, None)` mapped the absent scope to the *default* sheet, and sheet-scoped names are resolved before workbook-scoped ones, so a name scoped only to `Sheet1` was indistinguishable from a workbook-scoped name — while the same query against any other sheet correctly reported it absent. An unscoped query now means workbook scope, and nothing else. (#110)
- Target preparation no longer pulls in the default sheet for range dependencies that carry an unresolved sheet locator. A `Current` locator means "the sheet this reference lives on", and two sites substituted the workbook default instead — one of them via a `_` wildcard that also swallowed named-sheet locators, so a sheet name that failed to resolve silently became the default sheet. A named formula scoped to `Alpha` with a whole-column dependency selected cells on both `Alpha` and `Sheet1`; it now selects `Alpha` alone. Sheet resolution for these paths is now a single derivation that requires an explicit context sheet, and every locator variant is matched exhaustively so a new variant is a compile error rather than a silent default. (#110)

- Batch writes no longer report a sheet extent larger than the cells they wrote. `set_formulas` computed its pre-allocation from the row count including trailing empty rows, which write nothing, so a batch ending in an empty row reported one row too many — and a batch anchored at the last grid row reported row 1,048,577, which cannot exist. `set_values` inherited the same computation when it gained pre-allocation. Both now derive the extent from the last row that actually contains cells. (#335)

- Date-typed cells are treated as the numbers they are throughout the financial and statistical builtins. Excel has no separate date type on the sheet — a date cell holds a serial and is only formatted as a date — but three copy-pasted private coercion helpers plus two range collectors had no date arm, so `Date`/`DateTime`/`Time`/`Duration` cells were dropped or rejected. The consequences ranged from loud to invisible: `PRICE`, `YIELD`, `ACCRINT`, `ACCRINTM`, `TBILLPRICE`, `TBILLEQ`, `TBILLYIELD` returned `#VALUE!` when handed the date cells a workbook loaded from XLSX actually contains in their `settlement`/`maturity`/`issue` arguments; `NPV` returned `#VALUE!`; `XNPV`/`XIRR` returned `#NUM!` on their `values` argument; and `IRR`, `MIRR`, `MEDIAN`, `STDEV`, `VAR`, `LARGE`, `SMALL`, `PRODUCT`, `DEVSQ`, `PERCENTILE`, `QUARTILE`, `CORREL`, `SLOPE`, `RSQ`, `COVARIANCE`, `MAXA`, `MINA`, `AVERAGEA`, `STDEVA`, `VARA` and their siblings returned a *different number* with no error at all — while `SUM`, `AVERAGE` and `COUNT` over the very same range counted the date. All of these now agree with the numerically identical serial. (#328)
- `XNPV` and `XIRR` truncate date serials to whole days. A cell holding `2024-01-01 12:00` (serial 45292.5) now discounts as day 45292, matching Microsoft's documented remark that numbers in dates are truncated to integers, and reproduced against LibreOffice 24.2.7.2, which returns `308.187137202582` where formualizer previously returned `308.357947738306`. (#328)
- The date-serial conversion used by the financial builtins is now the crate-central `coercion::to_serial_strict` rather than three local duplicates, so it honours the workbook's date system and covers `Time`/`Duration` as well as `Date`/`DateTime`. (#328)

### Python bindings

- `SheetPortSession.evaluate_once`, `write_inputs`, `read_inputs` and `read_outputs` release the GIL while the engine runs. `SheetPortSession` owns a `Workbook` and `from_manifest_yaml` accepts a user-supplied one, so Python-backed custom functions are reachable from a SheetPort evaluation and hit exactly the deadlock fixed on `Workbook.evaluate_*`: the calling thread held the GIL waiting for the parallel layer while rayon workers blocked in `PyGILState_Ensure`. (#327)
- Using a `Workbook` from inside one of its own Python custom functions raises `RuntimeError` instead of hanging forever. The engine holds a non-reentrant lock for the duration of an evaluation, so `get_value` on an uncached cell, `set_value`, `sheet_names`, a nested `evaluate_*`, `Sheet.get_cell` and every other workbook operation could never be granted and blocked permanently — non-deterministically, since `get_value` served from the compatibility cache returned normally. `cancel()` and `reset_cancel()` remain safe from a callback, and a callback may still drive a *different* workbook. The contract is documented on `register_function`. (#327)
- The Python test suite is bounded by `faulthandler_timeout` with `faulthandler_exit_on_timeout`, and every CI job has a `timeout-minutes`. A deadlock regression now fails in 90 seconds with every thread's stack dumped, instead of running until GitHub's six-hour default. Measured: neither pytest-timeout's `thread` nor its `signal` method fires during a GIL deadlock, because both need to execute Python. (#327)

## [0.8.3] - 2026-08-13

### Fixed

- Approximate `MATCH`, `VLOOKUP`, and `HLOOKUP` skip error cells in the lookup range instead of propagating them. 0.8.2 introduced range-wide propagation, which diverged from desktop Excel on 8 of 10 measured oracle rows: a single `#DIV/0!` anywhere in a lookup column poisoned every approximate lookup over that column, where Excel answers normally. Error cells are projected out of the search exactly like blanks and other-class entries — they cannot be returned, do not break sortedness, and are not propagated even when a bisection probe lands on one. A range with nothing searchable left yields `#N/A`, and exact mode still never matches an error cell. Oracle contributed by an external reporter running desktop Excel 16.105.3 under AppleScript automation. (#326)
- Descending approximate `MATCH` returns the *first* entry of an exact-match run rather than the last, matching Excel; the last qualifying entry is still returned when the needle falls between two values. This affected ranges with fewer than eight searchable entries, whose linear path lacked the exact-match rule the bisection path already had. (#326)

## [0.8.2] - 2026-08-11

### Fixed

- Python wheels evaluate `TODAY()` and `NOW()` against the host clock. The binding did not enable the `system-clock` feature, so the engine fell back to a fixed clock at the UTC epoch and every volatile date/time builtin answered 1970-01-01 in every published wheel — silently, with no error and no type difference, so any ageing bucket or accrual-to-date returned a plausible wrong number. Reported externally. (#318)
- Temporal values compare as workbook-date-system serials across exact and approximate lookup paths, including warm lookup indexes. (#316)
- Approximate `MATCH`, `VLOOKUP`, and `HLOOKUP` skip blank and incomparable entries without losing original-range positions, while propagating lookup-range errors. (#317)
- Descending approximate `MATCH` returns the last qualifying entry for ranges with eight or more searchable values, including ranges with interior blanks. (#320)

## [0.8.1] - 2026-08-11

### Fixed

- Row and column deletion now recalculates formulas that read affected whole-column, whole-row, or other compressed open ranges, instead of returning stale pre-deletion values while reporting them as current. Insertion still does not invalidate position-sensitive open-range readers such as `MATCH` and `INDEX`; that pre-existing gap is tracked separately. (#306)
- Arithmetic on a date-typed cell no longer returns `#NUM!` when the result falls outside the representable date range. `Date`/`DateTime` operands keep their temporal tag when the resulting serial is representable, and degrade to a plain number when it is not, instead of manufacturing a numeric-domain error. This most visibly affected `end_date - start_date` accrual expressions whenever the period ran backwards: a perfectly ordinary negative day count became `#NUM!` and propagated through every downstream cell. NaN and infinity operands continue to error. Reported externally with a full reproduction and LibreOffice cross-check. (#310)

## [0.8.0] - 2026-08-10

### Engine introspection (new)

- Added `formualizer_eval::engine::inspect`, a public, engine-native introspection API: `inspect_cell`, `precedents`, `dependents`, `trace`, and `range_page`. Reports are owned, stamped snapshots addressed in A1 space, bounded by explicit `max_work` discovery budgets that degrade in band rather than failing, with truncation reported through `TruncationReport`/`OmittedCount`. Declared references come from the formula authority rather than the dependency graph, so precedent shape and source order survive; the graph supplies reverse reachability and state. Cycle classification is complete, not merely sound: a reachability post-pass guarantees that an unmarked link is safe to expand. Bounded `dependents` selection is canonical — the address-least N of the candidates discovered within budget — so truncated reports do not depend on internal representation.
- Reports are plane-independent: legacy and authoritative FormulaPlane engines return field-identical reports for identical logical workbook state, apart from state stamps, with two documented exceptions — per-cell staleness may be more conservative under FormulaPlane authority after structural edits and before re-evaluation, and reports produced under a binding `max_work` budget are representation-dependent in how much they discover.
- Python bindings expose all five entry points with keyword options matching core defaults, immutable typed reports, `Arc`-backed trace graphs whose node handles outlive their parents, A1-keyed mapping access, bounded `__repr__`, `to_dict()`, value equality and hashing where the core defines them, and a typed exception hierarchy carrying stable codes. Every output enum carries an explicit `Unknown` member so future core variants can never masquerade as a specific known one.
- WASM bindings expose the same surface with owned maps-as-objects reports, decimal-string `u64` stamps and omitted counts, stable tagged enum and value shapes, typed inspection errors including binding-side input validation, and an explicit hand-written TypeScript surface checked against real runtime output by a committed conformance gate.

### Added

- `formualizer_common::{CellAddress, RangeArea}` grid address types with validated construction and total `Display`, plus `address::format_a1_sheet_name` and `a1_sheet_name_needs_quoting`.
- A unified used-extent resolver behind explicit `ExtentPolicy` variants, replacing roughly two dozen ad hoc extent derivations while preserving each site's documented behaviour.
- A streaming, source-ordered semantic reference collector (`engine::refs`) now used by the graph, ingest, and plan-expansion paths, replacing three independent walks.

### Fixed

- Omitted function-argument slots are represented in the AST and evaluated as Excel does, instead of being silently treated as empty text or blank cells. (#277)
- `DCOUNTA` and `DGET` no longer mishandle empty-text criteria. (#281)
- `VLOOKUP` and `HLOOKUP` default `range_lookup` to approximate matching, matching Excel and LibreOffice. (#280)
- Formula results reached through a truly blank cell publish numeric `0`. (#279)
- Date and time text values coerce correctly in arithmetic. (#289)
- Wildcard `*` matching in criteria is correct for all patterns, verified by 64,687,036 differential comparisons against two independent reference implementations. (#284)
- `NPV` accepts variadic value arguments with Excel's reference and array semantics, verified against 804 corpus workbook cells. (#293)
- Saturating area arithmetic at three reference-collection sites, and heap-based iteration replacing recursion in reference collection, so deeply nested formulas no longer risk stack exhaustion.
- `Engine::get_cell` returned the anchor placement's literals for parameterized span families under FormulaPlane authority, producing wrong formula text.


### Python bindings

- Added immutable, typed reports for `Workbook.inspect_cell`, `precedents`, `dependents`, `trace`, and `range_page`, including A1-keyed trace-node access, bounded representations, serialization helpers, state stamps, and inspection-specific exceptions.

### Breaking changes

- VLOOKUP and HLOOKUP now default `range_lookup` to approximate matching (`TRUE`), matching Excel and LibreOffice. Existing formulas that relied on the old exact default should pass `FALSE` or `0` explicitly to keep exact matching; numeric zero and explicitly omitted slots are exact.

#### Parser/SDK 3.0 preparation

- `ASTNodeType` adds the `Omitted` variant for explicitly omitted function-argument slots. Exhaustive Rust matches and AST projections must handle the new node; it remains distinct from empty text and blank-cell values. (#277)
- `formualizer-common` and `formualizer-parse` now share version 3.0.0. The major records the removal of the four 2.0 token-vector/classifier `Parser` constructors in favor of source strings, `TokenStream`, and `ParserBuilder`; the deleted token-vector parser is not restored. See `docs/parser-sdk-3-migration.md`. (#257)
- Reported error/outcome vocabularies (`ExcelErrorKind`, resource and staleness reasons, `ExcelErrorExtra`, common coordinate/address/value errors, `RecoveryAction`, and `ParsingError`) are now non-exhaustive. Downstream output mappings use deliberate future fallbacks, while caller-supplied/core enums remain exhaustive. Serde unknown variants remain a separate wire-compatibility concern. (#257)
- Restored the inexpensive legacy `formualizer_common::value::{datetime_to_serial, serial_to_datetime}` paths as forwarding reexports; the root and `date_serial` paths remain available. (#257)

#### Rust product API freeze (#259)

- **FnCaps representation:** `bits`, `from_bits*`, and the `Flags::{Bits,Primitive}` associated representations widen from `u16` to `u32`, because `LOCAL_ENVIRONMENT` and `MAY_SPILL` use bits above 15. Callers persisting or transporting capability bits must use `u32`; inferred integer types generally need no source change.
- **Checked graph mutations:** `DependencyGraph::add_dependency_edge`, `DependencyGraph::bulk_insert_values`, and `DependencyGraph::set_cell_value_bulk_untracked` now return `Result<(), ExcelError>` instead of `()`. Propagate with `?` or deliberately handle each error; do not discard it.
- **Checked editor transaction:** `Engine::edit_with_logger` now returns `Result<T, EditorError>` instead of `T`. Propagate or handle validation and rollback failures.
- **Opaque cancellation:** `Engine::{evaluate_all_cancellable,evaluate_cells_cancellable,evaluate_until_cancellable,cancellation_token}`, `Workbook::{evaluate_all_cancellable,evaluate_cells_cancellable}`, `EvaluationContext::cancellation_token`, `FunctionContext::cancellation_token`, and `RangeView::with_cancel_token` use `CancelToken` rather than released raw `Arc<AtomicBool>` handles. Existing flags migrate with `CancelToken::from_flag(Arc::clone(&flag))`; context implementers should clone the handle, and hot loops should poll `is_cancelled()`. (#233)
- **Compatibility restoration (A):** The nine `formualizer_eval::builtins::datetime` paths (`create_date_normalized`, `date_to_serial`, `date_to_serial_for`, `datetime_to_serial`, `datetime_to_serial_for`, `serial_to_date`, `serial_to_datetime`, `serial_to_datetime_for`, and `time_to_fraction`), four-argument Excel-1900 `ArrowSheet::new_sparse`, explicit `new_sparse_with_date_system`, and the already-restored common date aliases are available again. The documented typed `#NUM!` hardening remains for 0.7.1 cases that panicked.
- **Accidental-surface narrowing (B):** Raw `TableEntry` lookup, the graph-only `first_load_assume_new` hint, dynamic-reference collector storage, policy/context variants, raw FormulaPlane tuple construction, concrete builtin implementations, default test support, and transaction prototypes are no longer supported public surface. Migrate direct callers to `TableMetadata`, `Engine::first_load_assume_new`, released reference-adjustment conveniences, opaque FormulaPlane descriptors/accessors, `builtins::load_builtins`, the opt-in `test-support` feature, or the retained production editor APIs as applicable; dynamic-reference collector storage has no supported public replacement.
- **Extensible outputs (C):** Approved report/output enums and structs are non-exhaustive. Downstream enum matches need `_`; report struct literals are unsupported, while fields on engine-returned values remain readable. Treat `TargetEvalDelta` sheet IDs and FormulaPlane route IDs/epochs as request/session-local telemetry that must not be persisted or replayed; Python and WASM map future SheetPort errors to generic fallbacks.
- **Package/freeze evidence (D):** CFFI, Python, and WASM Cargo packages are non-publishable on crates.io; use their C library/header, PyPI wheel, and npm channels instead.

### Added

- Exposed all five engine introspection entry points through the WASM workbook binding with owned maps-as-objects reports, decimal-string state stamps and omitted counts, stable tagged enum/value shapes, typed inspection errors, and an explicit TypeScript surface. Duration values retain the inherited `TimeDelta { secs: N, nanos: N }` debug string pending a coordinated pre-1.0 workbook value-convention revision.
- Added the read-only engine-native introspection API: stamped owned cell snapshots, declared precedents, bounded direct and range-covering dependents, bounded BFS trace DAGs with reachability-based cycle/convergence/spill semantics, and revision-checked semantic range pages. Reports preserve canonical sheet casing and source-ordered reference shape without semantic mutation, though inspection may warm snapshot-guarded performance caches. Inspection reports are plane-independent: legacy and authoritative FormulaPlane engines return field-identical semantics for identical logical workbook state, apart from state stamps. Two exceptions apply: after structural edits and before re-evaluation, per-cell staleness may be more conservative (`Dirty`) under FormulaPlane authority than legacy; and reports produced under a binding `max_work` budget are representation-dependent in how much they discover. Bounded dependents retain the first `max_results` by canonical address from all candidates discovered within `max_work`, making non-work-bound truncation deterministic across authorities. `formualizer-eval` exposes serialization behind its new non-default `serde` feature; stamps and request option structs deserialize as well as serialize. Ordinary dependents now leave `Dependent::via` empty, reserving it for spill-anchor queries, and `SnapshotOptions::include_values` uses the same plural spelling as the other options. The `formualizer` facade reexports the inspection DTOs as types-only until workbook and binding integration lands; call the engine methods through `formualizer-eval` for now.
- Added binding-neutral `CellAddress` and open-ended `RangeArea` types to `formualizer-common`, with checked in-grid 1-based construction, finite `RangeAddress` conversions, serde support, and total canonical quoted-sheet A1 display for cells, including `#REF!` sentinels for unchecked zero coordinates and unbounded bijective base-26 rendering beyond XFD. `TryFrom<RangeAddress> for CellAddress` now consistently reports `SheetAddressError`.
- Tables can be defined at runtime, without a serialise-and-reload round trip: `Workbook::define_table` (Rust), `Workbook.addTable` (WASM) and `Workbook.add_table` (Python), plus `tables()` / `getTables()` to list them. Tables are metadata over cells that already exist, so populate the region first; structured references resolve immediately afterwards and edits inside the region propagate. Definitions are validated up front -- unknown sheet, 1-based range violations, an inverted range, a header row with no data rows, and a header count that does not match the range width are all rejected by name rather than silently producing a table whose columns read outside its own range. The WASM binding rejects unknown keys instead of ignoring them and ships `TableDefinition`/`TableMetadata` TypeScript interfaces. (#212)
- Documented the JSON workbook format in `docs/json-workbook-format.md`, including the previously undocumented `tables` entry, the adjacently tagged cell-value shape, and the trap that a `tables` key outside a sheet is silently ignored. The worked example and the documented defaults are covered by tests. (#212)
- Added canonical checked Excel 1900/1904 date-serial conversion APIs to `formualizer-common`, including separate display semantics for serials 0 and 60 and source-compatible Excel-1900 wrappers for the existing common API.
- Added C5 revision-bound compatibility and typed target recalculation plans with deterministic `PlanStale` reasons, cross-engine rejection, value-edit reuse, and shared run-local C4 execution. SheetPort now builds compact symbolic target requests for cells, ranges, names, bounded layouts, and the supported full-v0 native-table output subset; one-shot and batch execution use the same target-plan path with explicit stale policy, option restoration, and baseline output restoration. Layout scan limits count data rows after the header, and `until_marker` retains its prior blank-row termination behavior.
- Completed C4 unified mixed target evaluation. Cell, range, cancellable, until, full, and delta paths now share typed target preparation and one request ledger across legacy vertices, FormulaPlane spans, symbols, spill anchors, and proven value-only cells. Accounted precedent adjacency and exact cache-skip builders preserve demanded closure locality without demotion; event-scoped dirty subleases acknowledge only completely flushed consumer closures; mixed SCCs retain exact demotion and existing cycle semantics; volatile and dynamic references use one epoch plus bounded runtime replanning and monotone workbook widening. Added versioned `TargetEvalDelta` run/region records for legacy, span, and spill writes while preserving unlimited-by-default `EvalDelta` compatibility and explicit caller-limited typed expansion overflow.
- Completed C3 deferred FormulaPlane source-unit preparation. Target discovery selects and consumes an intersecting deferred source package atomically, never splits residual replay ownership, preserves source order and exact replay/failure restoration, and publishes legacy graph work plus checked FormulaPlane authority in one transaction. Off replays the selected package to the legacy graph, Shadow prepares FormulaPlane placements but keeps legacy graph authority, and authoritative experimental mode commits supported placements directly while replaying exact fallbacks; unrelated packages remain staged.
- Completed C2 transactional target preparation for Off and Shadow modes. Typed targets use bounded sheet indexes, immutable function-planning snapshots, exact/Sheets/Workbook widening, prepared addition or replacement publication, scoped graph-source scratch, structured stale reasons, and common exact graph admission across direct, bulk, logged/replay, staged, demotion, fragmented, compressed-fallback, and generic graph mutation seams. C3 extends the same transaction to deferred FormulaPlane source packages.
- Added C1b exact non-materializing request topology for FormulaPlane mixed-cache skips. Candidate, edge, and retained-byte overflow now retains spans and selects paged/indexed, bounded in-memory, explicit native delete-on-drop scratch, or work-accounted no-disk repeated-pass construction; cache compilation publishes only complete topology or an explicit skip. Retained mixed-cache and topology/schedule-discovery scratch budgets are now active with typed exhaustion, strategy/pass/cap telemetry, skip streaks, and operator guidance; C2 activates graph admission and graph-source scratch.
- Added a reproducible C1-exit structural and cold-process gate at `docs/architecture/evaluation-c1-exit-gate.md`.
- Added the C1a evaluation resource contract with one all-unset-by-default `EvaluationBudgets` configuration, optional envelope-to-budget derivation, field-level legacy mapping diagnostics, checked request ledgers, common-mode work/deadline checkpoints, and typed resource exhaustion details. Explicit budget fields win only their conflicting legacy mappings; retained/scratch/cache settings are observational, and graph/materialization admission remains declarative until C2's single composed transaction. Recommended named constructors/default sets are deferred until calibration and will not be enums or modes.
- Added observational evaluation-resource telemetry with monotonic request IDs, typed cap reasons, FormulaPlane topology/cache/materialization/dirty-lease counters, staged preparation and phase timings, replay-spool storage counters, and cold-process load-envelope JSON reporting. Defaults, fallback strategies, errors, and evaluation behavior are unchanged.
- Added Calamine-backed in-memory XLSX loading to the native Python binding and made it the default for `Workbook.from_bytes` and `load_workbook_bytes`; Pyodide retains its Umya fallback.
- Added backend-neutral source-family ingest with anchor-once FormulaPlane authority for proven complete domains. Calamine supplies bounded XLSX evidence and exact replay for eager and deferred loading, with structural-edit, cycle-demotion, and source-family telemetry support.
- Added registry-owned function semantic contracts so safe current and future ordinary functions can use FormulaPlane authority without a secondary supported-name list, while exceptional and untrusted functions continue to replay conservatively.
- Added bounded fragmented shared-formula evidence, exact coordinate-disposition replay, one-analysis Shadow preparation, and typed ordinary-exception ownership as the replay-only foundation for transactional fragmented authority.
- Added graph-owned FormulaPlane dirty authority with generation-leased region and exact-span work, unified sparse legacy dirty tracking, explicit global-invalidation telemetry, and retry-safe prefix acknowledgement.
- Added exact span-region structural dirty events so row and column shifts schedule only moved or cleared placement intervals while preserving post-lease retry identity.

### Improved

- Consolidated the engine's graph, ingest, and planning AST dependency walks behind one crate-private semantic reference collector without changing their independent policies or behavior.
- Consolidated whole-axis and partially open used-extent resolution behind one internal policy-driven resolver while preserving the distinct runtime, virtual-dependency, graph, and semantic policies.
- Narrowed the published eval API to stable table metadata, reference-adjustment conveniences, builtin loading/date compatibility helpers, and opaque Formula Plane descriptors; test-only helpers now require the non-default `test-support` feature. (#259)
- Upgraded Calamine-backed XLSX loading to Calamine 0.36 and a single-pass value/formula metadata stream, preserving formula-only worksheet dimensions, cached-value semantics, load limits, shared-formula relocation, and malformed-family fallback.

### Testing / internal

- Added a fixed-seed AST-to-edge parity harness covering structural edits, compressed ranges, symbol vertices, dirty propagation, and mutation-tested edge-maintenance failures.
- Pinned a fourth AST-to-edge finding, `AST_EDGE_INSERT_SHIFTS_NAME_VERTEX_ONTO_GRID`: a default-sheet row or column insert shifts a workbook-name vertex off its `Sheet1!$A$1` home onto an addressable cell, so references resolved afterwards bind to the name vertex instead of the cell. The campaign carve-out for default-sheet inserts is narrowed to that shape, restoring formula and value overwrite coverage on the default sheet.

### Fixed

- `Engine::get_cell` now reconstructs placement-specific literals for parameterized authoritative FormulaPlane families instead of returning the anchor placement's formula text for every member.
- Whole-surface ranges whose cell count overflows `u32` are now always kept as compressed range dependencies instead of panicking or attempting expansion.
- Lookup wildcards now let `*` consume zero or more characters instead of at most one, including multi-star backtracking, `?` adjacency, `~` escapes, case-insensitive Unicode text, and exact-mode VLOOKUP/HLOOKUP/MATCH plus wildcard-mode XLOOKUP/XMATCH. (#284)
- `NPV` now accepts up to 254 scalar, reference, and array cash-flow arguments in argument order. Text, blank, and logical cells in references are ignored without consuming periods, direct text and computed-array text return `#VALUE!`, omitted slots count as zero, errors propagate, and `rate=-1` returns `#NUM!`. (#293)
- Arithmetic operators now coerce supported en-US date, time, and datetime text operands to serial numbers using the workbook's 1900 or 1904 date system. Two-digit years in slash and English month-name forms use the Excel 29/30 window, ISO dates require a four-digit year, slash dates use month/day/year ordering, and `T` is restricted to ISO datetimes; aggregate, comparison, criteria, `N`, and concatenation text semantics remain unchanged. (#289)
- `DATEVALUE` and `TIMEVALUE` now use the shared deterministic temporal parsers: two-digit date years use the Excel 29/30 window, surrounding whitespace is accepted, interior slash-date whitespace is rejected, and whitespace around time separators is accepted. `DATEVALUE` retains its pre-existing unambiguous day/month/year and year/month/day slash fallbacks for compatibility. (#289)
- Date, datetime, time, and duration literals used by `*`, `/`, `^`, unary minus, or `%` now convert under the workbook's selected date system instead of implicitly using the Excel-1900 system. (#289)
- Database functions now distinguish explicit empty text from genuinely blank field cells: `DCOUNTA` counts the former, while `DGET` ignores the latter, matching LibreOffice and Excel. (#281)
- Formula cells whose final result passes through a truly blank cell now publish numeric `0`, matching Excel and LibreOffice, while direct blank-cell inspection and range semantics remain unchanged. Blank elements in spilled results are likewise published as zero; JSON persistence and Python/WASM value surfaces now expose these computed results as `0` instead of empty/`None`/`null`. (#279)
- Explicitly omitted function arguments now retain their syntax through parsing and canonical rendering, coerce as Excel empty arguments (`0`, `FALSE`, or empty text by context), and count as numeric zero in aggregates without changing absent optional defaults or blank-reference behavior. (#277)
- `TEXT` with an empty format string (explicit `""` or an omitted second argument) now returns empty text, matching Excel and LibreOffice; it previously rendered the value as if unformatted. (#277)
- Restored the released `formualizer_eval::builtins::datetime` conversion helpers and sparse-sheet constructor compatibility paths, including explicit 1904 date-system handling. The eval decode wrappers retain released 0.7.1 component-clamping and negative-fraction behavior; as an intentional safety deviation, infinities and chrono date/duration overflow now return typed `#NUM!` instead of panicking. (#259)

- CFFI targeted cell evaluation now reports both the targeted graph-preparation error and the distinct full-graph fallback error when both preparation attempts fail.
- CFFI canonical formula rendering now honors the selected Excel or OpenFormula dialect while retaining canonical Excel output and existing input/output contracts.
- CFFI status errors now use canonical JSON string escaping, preserving valid round-trippable messages for control characters and other special text.
- CFFI cell and rectangular block entry points now reject zero, out-of-grid, and overflowing Excel coordinates with a typed status before touching workbook state, preventing packed-coordinate aborts.
- CFFI `RangeAddress` JSON and CBOR boundaries now reject zero, inverted, and out-of-grid ranges before formatting or workbook reads, preventing malformed-bound underflow and out-of-grid materialization.
- Python `Workbook.undo()` and `redo()` now invalidate the compatibility cell cache before replay, so `Workbook.get_value()` and existing `Sheet` handles immediately reflect authoritative values after successful, failed, or no-op replay without introducing a cache-lock panic.
- Authoritative FormulaPlane fallback no longer reports a false `#CIRC!` when a statically indexed compressed range contains the formula cell but `INDEX` selects a different cell. Whole-row, whole-column, bounded, omitted-column, zero, negative, and genuine self-selection cases now follow the same reference semantics in Off and authoritative modes.
- Deferred malformed-formula retries now publish one parse diagnostic per successful ingest event instead of duplicating a source record during authoritative replay; a later distinct ingest of the same malformed source still emits its own diagnostic.
- Compatibility-widened target preparation now performs the same final graph, authority, staged, symbol, provider, and relevant function-semantic revision validation as exact preparation before publishing. Provider changes and injected final-validation failures remain atomic in Off, Shadow, and authoritative modes.
- Prepared `sheetport-spec` 0.3.1 as a validator-only patch: layout `header_row` now rejects 0 and values above Excel's 1,048,576-row limit. The manifest protocol and canonical schema remain fio 0.3.0, while product dependency requirements adopt a 0.3.1 floor so packaged behavior matches the path source tested in the workspace. (#258)
- Authoritative FormulaPlane families are no longer replayed to the legacy graph when an unrelated function is registered between planning and commit. Semantic validation now scopes registry changes to the functions used by each prepared family while retaining conservative fallback for affected functions and truncated change history. (#241)
- Range-consuming builtins accept a scalar as a 1x1 array, matching Excel. `=TRANSPOSE(2)`, `=SORT(2)`, `=UNIQUE(2)`, `=TAKE(2,1)`, `=DROP(2,0)`, `=FILTER(2,1)`, XLOOKUP, XMATCH, SORTBY, GROUPBY, PIVOTBY, CHOOSEROWS and CHOOSECOLS previously returned `#REF!`. An error argument now also keeps its own error rather than being masked, so `=TRANSPOSE(NA())` is `#N/A` instead of `#REF!`. The promotion is opt-in through the new `ArgumentHandle::range_view_or_scalar`, not a change to `range_view` itself, because several builtins use a resolution failure as type dispatch: statistical functions coerce a direct scalar (`=MEDIAN(TRUE)` is `1`) while skipping a range cell of the same type, and a D-function's scalar criteria argument is an error rather than an empty criteria block that matches every row. (#224)
- Layout selectors are no longer capped by a declared row limit. `LayoutDescriptor.max_scan_rows` bounded both the scan and the preparation envelope, which meant a contiguous table longer than the limit failed with `LayoutExhausted` -- at the 100,000-row default, a 150,000-row table was unresolvable -- while a short table on a large sheet still declared a 100,000-row envelope and evaluated rows that were not part of its output. Both bounds are now derived from the sheet's used range via `sheet_dimensions()`, which is a property of the workbook and so stays deterministic for a given input. Measured cost is unchanged: preparation tracks populated cells, not envelope extent, and is flat (~9ms) across used ranges from 200 to 1,000,000 rows. `max_scan_rows` is removed from the manifest and the `fio-0.3` schema, which returns that schema to its released shape; manifests that set it are rejected as unknown fields, and manifests that omit it are unaffected. `SheetPortError::LayoutExhausted` remains as a defensive guard for stores that cannot report dimensions.
- Unified engine cancellation onto a single source. Cancellation previously arrived through two independent channels: `PrepareTargetsOptions::cancel` drove the target-preparation checkpoints while a separate `cancel_flag` argument drove evaluation, so a caller that supplied only the evaluation flag -- which is what `evaluate_targets_cancellable` did -- got no cancellation at any preparation checkpoint, the phase that dominates on large workbooks. There is now one `CancelToken` carried on `TargetEvalOptions` (renamed from `PrepareTargetsOptions`, which remains as a deprecated alias), hoisted onto the engine for the duration of a call and read by both phases. `CancelToken` replaces the raw `Arc<AtomicBool>` in engine, workbook and SheetPort entry points, hiding the representation so richer cancellation can be added without a break; use `CancelToken::from_flag` to adopt a flag you already own. (#229)
- Reduced the pre-0.8.0 public surface further: telemetry, resource-reason and ledger-error enums are now `#[non_exhaustive]` so new variants are not breaking changes, and the redundant `evaluate_targets_cancellable` / `evaluate_recalc_plan_cancellable` wrappers were removed in favour of the `..._with_options` and `..._with_controls` forms they were strict subsets of. Caller-supplied configuration enums such as `DiskScratchPolicy` remain exhaustive on purpose. (#229)
- Narrowed public API that was exposed only for tests and benchmarks before it could be frozen by a release: the engine's `ResourceLedger` accounting mechanism is now crate-internal (its budget, snapshot and error types remain public), `set_before_target_preparation_commit_hook` is crate-internal, and the cross-crate test seam `set_before_prepared_span_commit_hook` plus SheetPort's `workbook_for_benchmark` are marked `#[doc(hidden)]`.
- Date functions now follow the workbook's 1900 or 1904 date system instead of assuming 1900. `DATEVALUE`, `EDATE`, `EOMONTH`, `YEAR`/`MONTH`/`DAY`, `HOUR`/`MINUTE`/`SECOND`, `DAYS`, `DAYS360`, `DATEDIF`, `YEARFRAC`, `WEEKDAY`, `WEEKNUM`, `ISOWEEKNUM`, `WORKDAY`, `NETWORKDAYS`, and the bond day-count functions previously decoded every serial through a fixed Excel-1900 mapping, so a 1904 workbook stored dates correctly and then computed answers four years and a day off -- `YEAR(A1)` disagreed with `TEXT(A1,"yyyy")` on the same cell. Date-bearing literals passed to these functions were affected by the same fixed mapping and now resolve through the workbook's system as well. (#225)
- Planning snapshot capture is now validated against the functions a request actually named instead of the global registry epoch. Registering any function previously invalidated concurrent captures, which surfaced as spurious `RegistryChangedDuringCapture` results and, under authoritative FormulaPlane mode, as family fallbacks that misreported an unrelated registration as a provider revision change. Capture stays conservative when the bounded semantic change log has been truncated. (#223)
- Date and datetime values now retain the workbook's 1900 or 1904 date system through Arrow storage, sparse overlays, and FormulaPlane writes instead of decoding through an implicit 1900-only path.
- Dynamic-array builtins (`FILTER`, `SORT`, `SORTBY`, `UNIQUE`, `TRANSPOSE`, `TAKE`, `DROP`, `XLOOKUP`, `XMATCH`, `GROUPBY`, `PIVOTBY`) now accept computed array arguments such as `FILTER(A1:A3,B1:B3="x")`, `SORT(SEQUENCE(3))`, and `TRANSPOSE({1,2})` instead of rejecting them with `#REF!`. Their data arguments no longer claim reference semantics, and `range_view` resolves through the same cached value-or-reference path, so references keep their lazy views, volatile expressions are still evaluated once, and genuine reference failures are preserved. (#216, #218)
- Aggregate and range-consuming math functions now consume computed arrays through one cached value-or-reference path, preserving conditional reference fallback, exact errors, cancellation, and single evaluation of volatile or custom expressions. (#211)
- `TEXT` date formatting now follows the workbook's 1900 or 1904 date system, preserves Excel's display-only serial 60, rejects invalid serials safely, and carries rounded times across day boundaries without corrupting near-midnight datetimes. (#210)
- `CONCAT` and `TEXTJOIN` now expand range and computed-array arguments in row-major order with exact blank/error handling and Excel's 32,767-character limit, while legacy `CONCATENATE` retains scalar top-left behavior. (#209)
- Formula tokenization now accepts TAB, CR, LF, and CRLF as lexical whitespace in both tokenizer frontends while preserving raw source spans. Only an ASCII space can spell the range-intersection operator, whose semantic token/AST value is canonicalized to `" "` even when its source run also contains line breaks or tabs.
- Structural row and column deletes now rewrite invalidated reference leaves to ordinary `#REF!` error literals instead of magic `#REF` sheet sentinels. This preserves lazy `IF`/`IFERROR` semantics, dependency rewiring, formula display/reparse, undo/redo, whole-axis adjustment, cross-sheet locality, and legitimate worksheets named `#REF` across legacy and FormulaPlane structural paths.
- Fixed deferred `evaluate_cells_with_delta` requests missing transitive formula precedents staged on non-target sheets.
- Fixed experimental FormulaPlane capacity fallbacks evaluating legacy readers before required span results were available. Unsafe requests now transactionally demote exactly their scheduled spans, retain pending dirty regions across failed attempts, and fail closed behind a finite materialization limit.

### Security and hardening

- Added a credential-free release preflight that packages each Rust release track through a temporary local registry in publish order, verifies downstream archives against the exact prospective upstream bytes, and rejects source drift when a package version already exists on crates.io. Clean-checkout, symlink, archive-member, checksum, exclusive-lock, and exact isolated-tool guards keep the pre-tag check transactional; tag workflows repeat it before any publish job receives a registry token. (#257, #258)
- Upgraded PyO3 and NumPy bindings to 0.29.0, resolving the PyO3 out-of-bounds iterator and missing closure `Sync` advisories. Upgraded `pyo3-stub-gen` to 0.23.0, made existing clone-based Python extraction explicit, and aligned generated stubs with the private maturin extension-module path while preserving the public `formualizer` package API.

### Performance

- Experimental authoritative FormulaPlane evaluation now contracts proven span-disconnected legacy islands into the compressed legacy scheduler while retaining span-connected legacy producers in the mixed cycle detector. Dynamic references, names, spills, structural-summary uncertainty, and boundary-discovery overflow fail closed to the global mixed planner. (#251)
- Mixed FormulaPlane topology overflow now retains the bounded, accounted prefix and its indexes instead of discarding successful compile work. Paged schedule discovery reuses complete cached sources and derives only the uncovered tail exactly, so dense growing-range workbooks avoid rebuilding the same overflowed topology on every recalculation.
- Experimental authoritative FormulaPlane evaluation now uses the existing consumer-read interval index when exact schedule construction falls back after topology-cache overflow, avoiding full-table scans per dirty formula during the first evaluation of bulk-loaded workbooks. (#240)
- Experimental authoritative FormulaPlane evaluation now caches accounted consumer and precedent topology across warm and value-only evaluations. Exact graph, authority, semantic, provider, and dynamic-reference revisions invalidate or bypass stale cache generations; candidate, edge, and retained-byte cache overflow selects exact paged/run/native/repeated-pass request topology without span demotion, while span-free and warm no-dirty requests retain topology-free sparse paths.
- Proven complete source-formula families now parse and analyze one anchor and avoid per-descendant strings, ASTs, staging entries, and graph vertices. In same-machine release probes, a clean 100k-family load improved from 997 ms and 313 MiB RSS under forced replay to 129 ms and 26 MiB RSS; a 1M-family load completed in 2.2 s at 167 MiB RSS instead of 13.1 s at 3.0 GiB.

## [0.7.1] - 2026-07-02

### Fixed

- INDEX and OFFSET now clamp unbounded whole-column/whole-row range arguments (`B:B`, `2:2`, `Data!$A:$C`) to the sheet's used region instead of returning `#REF!`, restoring the common `INDEX(range, MATCH(...), MATCH(...))` lookup pattern. (#162, #163)
- INDEX supports `row_num`/`column_num` of 0 to return the entire column or row, matching Excel, in both the reference and array-constant paths. (#156)
- FIND and SEARCH index by character rather than byte, fixing incorrect positions and a panic on multi-byte UTF-8 text (e.g. `SEARCH("?z","éz")`). (#153)
- TEXT returns non-numeric text unchanged instead of `#VALUE!`, matching Excel; locale-ambiguous numeric-looking strings such as `"1.234,56"` still error. (#155)
- SUMIFS/COUNTIFS/SUMIF `<>text` criteria now match blank cells, matching Excel, with whole-column and edge-case coverage. (#160, #161)
- INDIRECT resolves defined names and tables when `a1_style` is FALSE. (#154)

### Performance

- Release builds (crates, Python wheels, npm/WASM packages) now compile with fat LTO and `codegen-units = 1` for smaller, faster artifacts. (#19, #20)

## [0.7.0] - 2026-06-12

### Breaking changes

- Added the `Iterate` variant to the public `CyclePolicy` enum; downstream code matching `CyclePolicy` exhaustively must add an arm. (#130)

### Added

- Added Excel-style iterative calculation: `CyclePolicy::Iterate` evaluates intentional circular references with configurable max-iteration and convergence-threshold settings, built on runtime SCC cycle detection via live-edge iteration so only genuinely cyclic cells iterate and short-circuited branches never create false cycles. (#118, #119, #130)
- Added XLSX `calcPr` round-trip so workbooks authored with iterative calculation enabled in Excel load and save with the same cycle configuration, plus Python and WASM/JS cycle-configuration surfaces. (#131)
- Added WASM cycle configuration on the plain `new Workbook(options)` constructor and a `lastCycleTelemetry()` accessor exposing iteration, convergence, and cycle-outcome telemetry for the most recent evaluation. (#138)
- Added FormulaPlane named-range support: formulas referencing defined names with concrete cell or range definitions now canonicalize, fingerprint, and evaluate as spans; names resolve per cell at projection time with the same scope/shadowing semantics as legacy evaluation, and define/update/delete of a name invalidates affected spans. (#147)
- Added FormulaPlane mixed-anchor range support so tail reads (e.g. `$A2:$A$100`) and running totals (e.g. `$B$2:$B2`) evaluate as spans with placement-precise dirty projection. (#145)
- Added MIT and Apache-2.0 license files to the repository.

### Improved

- Improved cycle evaluation infrastructure with condensation-ordered schedule units, per-SCC cycle outcomes, and a live-edge collector with lazy `SHORT_CIRCUIT` dispatch. (#116, #117, #118)
- Excluded FormulaPlane span members from static cycle detection so span-covered families do not produce spurious cycle verdicts. (#121)

### Performance

- Batched cell edits now run one multi-source dirty propagation per bulk operation instead of one per cell, making large `write_range`/`set_values` calls up to ~270x faster (15.9 s to 59 ms for a 20k-cell batch with changelog off). (#139)
- Changelog old-state capture is recorded directly at edit time instead of patched by an O(N²) reverse scan, reducing a 20k-cell batch with changelog on from 506 ms to 112 ms (combined with #139, ~144x end to end). (#140)
- Hot-path improvements from profiling: iterative Tarjan SCC (no recursion-depth limits), fixed exponentially repeated dispatch on deeply nested expressions, multi-source dirty marking, and aggregate infinity sanitization. (#136)
- Amortized CSR edge rebuilds across per-cell formula edits instead of rebuilding per edit. (#127)
- Recorded per-cell staged-formula deltas in the changelog instead of whole-sheet snapshots. (#128)
- Stored schedule units as indices instead of cloned layers. (#117)
- `IFERROR`/`IFNA` now short-circuit (the fallback branch is not evaluated when the value is clean), `SEQUENCE`/`RANDARRAY` reject array shapes beyond Excel grid limits before allocating, and order-statistic functions (`LARGE`, `SMALL`, `MEDIAN`, `PERCENTILE.*`, `QUARTILE.*`) use quickselect instead of full sorts (~12x on large ranges). (#141)
- Fixed a FormulaPlane mixed-mode interaction (degenerate rectangle routing and a repeated demote spin) that made authoritative evaluation slower than Off on mixed workbooks; the mixed corpus improved from 996 ms to 26 ms, faster than Off. (#143)
- Linearized the FormulaPlane reject path with O(1) candidate mapping on family rejection, reducing the ingest penalty of a 50k-cell dependent chain from 863 ms to 112 ms. (#146)

### Fixed

- Fixed unqualified references in cross-sheet formula contexts leaking to the default sheet. (#110, #114)
- Fixed spill projections and region locks not being torn down when a cycle stamps `#CIRC!`. (#115)
- Fixed whole-axis and stripe self-inclusion not being detected as circular references. (#129)
- Fixed `CycleTelemetry` not being populated on the workbook/Arrow evaluate path. (#124)
- Fixed two iterative-calculation edge-case bugs found by corpus testing, and hardened the pre-ship surface with per-cycle clock snapshots, Python telemetry, and persistence pins. (#134, #135)
- Fixed a wasm32 panic from clock access during evaluation under the JS runtime. (#138)
- Fixed the Umya XLSX loader registering defined names after eager formula ingest, which prevented named formulas from resolving at ingest time. (#147)

### Tooling and quality

- Added a property-test oracle evaluating random guarded workbooks against a reference lazy interpreter. (#122)
- Added standing SCC cost-model probes (phantom pairs, iterate workloads) and an iterative-calculation edge-case corpus. (#123, #132, #135)
- Added the FormulaPlane span-coverage corpus, `probe-fp-coverage`, and generator-driven pinning tests as the standing coverage measurement for fingerprint expansions. (#142)
- Added the adaptive formula partition architecture document describing the unified evaluation end state. (#148)
- Added cycle detection and iterative calculation guides, reference pages, and interactive sandboxes to the docs site. (#137)

## [0.6.0] - 2026-06-03

### Breaking changes

- Consolidated parser implementations by removing the legacy token-vector parser and making the source-span parser the public `Parser`; consumers relying on legacy parser internals should update to the canonical parser APIs. (#104)

### Added

- Added experimental opt-in FormulaPlane span evaluation for large copied-formula families. The default workbook path remains the stable dependency graph; span evaluation must be enabled explicitly through Rust, Python, WASM/JS, or C FFI configuration.
- Added sparse initial ingest paths for JSON, Umya, and Calamine loaders to avoid materializing formatting-only worksheet extent as populated cells.
- Added publishable Calamine-backed XLSX loading improvements that preserve sparse-friendly engine ingest while remaining compatible with the crates.io Calamine API.
- Added benchmark corpus tooling and structural invariants for Off/Auth parity, backend comparison, and FormulaPlane promotion metrics.
- Added idiomatic parser APIs including `Parser::new`, builder-based construction, `TokenStream` parsing, `FromStr`, and `TryFrom` conversions for `Parser` and `ASTNode`. (#104)
- Added 28 worksheet functions across engineering, info/reference, lookup/array-shape, and text categories, including Bessel functions, `FORMULATEXT`, `SHEET`, `SHEETS`, `ISREF`, `TOCOL`, `TOROW`, and byte-oriented text functions. (#101)

### Improved

- Improved FormulaPlane promotion and evaluation for arithmetic, lookup, criteria aggregate, whole-axis, cross-sheet, and affine literal formula families.
- Improved structural edit handling for promoted spans, including row/column insert/delete shifting, bounded dirty projection, and conservative demotion when required.
- Reduced FormulaPlane memory usage for integer-like affine literal families by encoding literal bindings compactly instead of retaining one dictionary entry per placement.
- Kept FormulaPlane structural demotion linear by pre-creating direct-dependency placeholder vertices before batched edge insertion and by clearing computed overlays by range instead of one cell at a time.
- Optimized holiday handling in `NETWORKDAYS`, `WORKDAY`, and their `.INTL` variants by deduplicating holidays once and using binary search during date loops. (#102)
- Aligned direct XLSX helper dependencies with the newer Calamine/`zip` stack where possible.

### Fixed

- Preserved default stable semantics by keeping FormulaPlane/span evaluation disabled unless explicitly requested.
- Preserved Off/Auth parity across the validated benchmark corpus while falling back to the legacy graph for unsupported span shapes.
- Fixed parser handling for leading empty function arguments such as `=FOO(,A1:C3)`, preserving the intended empty-argument arity. (#103, #104)
- Hardened FormulaPlane sheet lifecycle operations so add/remove/duplicate/rename operations preserve unrelated spans, demote only affected spans, reject unbounded references to unknown sheets without creating phantom sheets, and avoid region-index panics or iterator overflow. (#105)
- Fixed deferred graph-building evaluation so `evaluate_cell` and `evaluate_cells` drain all staged sheets before demand evaluation, preventing cross-sheet references to staged formula cells from resolving as `None`. (#106)
- Fixed date functions to coerce `Date` and `DateTime` cells through the common lenient numeric path, and corrected `EDATE`/`EOMONTH` negative-month year-boundary handling. (#107)
- Fixed named-range incremental evaluation by walking through `Named`/`Range` pass-through vertices in demand subgraphs and preserving named-range edges through CSR rebuilds. (#108)

### Security and hardening

- Updated lockfiles to pick up patched `openssl`, `thin-vec`, and `tmp` versions, clearing high-severity Dependabot alerts in Rust and benchmark harness dependencies.

### Known limitations

- FormulaPlane span evaluation remains experimental and opt-in in this release.
- Internal dependency chains such as running balances and cumulative schedules remain on the legacy dependency graph.
- Array-literal formula families are not span-promoted.
- Calamine-backed structured table metadata is still incomplete for some table-reference workloads; Umya remains the fuller XLSX compatibility path for those cases.
- Calamine formula-record streaming is deferred until the upstream API is available in a crates.io release.

## [0.5.9] - 2026-05-18

### Fixed

- Treated unary `+` as a pass-through (identity) operator to match Excel/LibreOffice semantics. Previously, `=+A1` returned `#VALUE!` when `A1` held a non-numeric string such as `"2014F"`; the leading-`=+` idiom is common in finance models carried over from Lotus 1-2-3 and now preserves text, booleans, and other non-numeric operand types. Unary `-` and `%` retain their numeric-coercion semantics. (#100)
- Preserved computed-overlay accounting when edits remove previously computed values, preventing stale overlay memory estimates and keeping later recalc flushes consistent. (#95)

### Performance

- Improved computed formula overlay flushing by buffering formula-result writes and coalescing them into sparse, dense, or run-length overlay fragments instead of emitting every result as an individual point write; narrow layers now use the direct point-write path so deep chains do not pay coalescing overhead when there is nothing to coalesce. In local `v0.5.8` → 0.5.9-candidate A/B runs with a 20 GiB process memory cap, `headline_100k_single_edit` incremental recalc improved from 22.01 ms to 6.89 ms (3.19x), `agg_countifs_multi_criteria_100k` incremental recalc improved from 9.80 ms to 8.35 ms (1.17x), and a 50k-row finance repeated-edit probe improved total recalc from 223.83 ms to 170.75 ms (1.31x) with flat peak RSS. The adversarial `chain_100k` watchlist scenario is much closer to baseline after the narrow-layer fast path (57.58 ms to 63.23 ms, 0.91x). (#95)
- Added finance-shaped recalc probes and computed-overlay observability coverage for dense, sparse, and run-length formula-result flush patterns.

### Changed

- Bumped Arrow dependencies from the 56.x series to `58.2.0` and Wasmtime from `42.0.2` to `43.0.2`. (#95, #97)
- Bumped the docs site to Next.js `16.2.6` to pick up current security fixes.

### Tooling and quality

- Hardened the WASM CI path with explicit portable-wasm and wasm-js profile checks, artifact import validation, and Node.js 24 for npm release builds.
- Refreshed Python development dependencies, including `pytest` `9.0.3`.

## [0.5.8] - 2026-04-27

### Breaking changes

- Bumped the parser SDK track to `2.0.0` because parser AST enums now expose additional variants for new Excel syntax, including LAMBDA immediate-invocation calls and 3D sheet references. Consumers that exhaustively match parser AST enums may need to handle the new cases.

### Added

- Added parser support for Excel reference operators, including `:` range composition and space intersection, with precedence coverage and pretty-printer round-trips. (#69)
- Added parser support for 3D sheet-range references such as `Sheet1:Sheet3!A1` and `Sheet1:Sheet3!A1:B2`. (#70)
- Added parser support for dynamic-array spill postfix references such as `A1#`. (#71)
- Added parser support for real structured/table reference parsing, including special items, column ranges, escapes, Unicode column names, and display round-trips. (#73)
- Added parser support for LAMBDA immediate invocation syntax such as `LAMBDA(x, x + 1)(2)`. (#68)
- Added a differential harness that compares the classic token parser and canonical span parser and documents remaining parser-front-end divergence. (#77)

### Fixed

- Accepted lowercase and mixed-case boolean literals such as `true` and `fAlSe` without misclassifying longer named ranges. (#72)
- Tightened scientific-notation tokenization so incomplete exponent forms no longer consume following operators or references. (#78)
- Preserved pending `A1:` prefixes before double-quoted strings instead of silently discarding them. (#79)
- Preserved error kind for sheet-qualified error literals and accepted lowercase sheet-qualified error literals. (#74)
- Recognized modern Excel `#SPILL!` and `#CALC!` error literals. (#75)
- Prevented R1C1-shaped inputs from being misclassified as structured table references while preserving valid A1 references such as `R1`. (#76)

### Tooling and quality

- Excluded Pyodide/Emscripten wheels from PyPI uploads while continuing to build and smoke-test them in release workflows.

## [0.5.7] - 2026-04-26

### Fixed

- Fixed unary minus precedence to bind tighter than exponentiation, matching Excel semantics (`=-2^2` now evaluates to `4` instead of `-4`). (#65)

### Performance

- Fixed O(N²) bulk-ingest scaling for row-major formulas by introducing `CoordBuildHasher` for packed coordinate keys and applying it to the hot dependency-graph and spill-commit maps. (#67)

## [0.5.6] - 2026-04-14

### Fixed

- Raised the default workbook logical-cell ingest budget from `8_000_000` to `128_000_000`, allowing much larger dense workbooks to load through the existing `load_workbook(...)`, `Workbook.load_path(...)`, and `recalculate_file(...)` paths while keeping row, column, and sparse-sheet guardrails in place. (#57)

## [0.5.5] - 2026-04-13

### Security and hardening

- Hardened native Wasmtime-backed plugins by enforcing fuel and memory budgets, revoking cached modules on unregister, capping guest ABI payload sizing, and bumping `wasmtime` to `42.0.2` to clear the current security advisories. (#42, #44, #51)
- Added workbook ingest guardrails for oversized logical sheets and extreme sparse-sheet ratios across JSON, Calamine, and Umya loaders. (#47)
- Hardened workbook coordinate validation across Python and wasm bindings so zero-based or non-positive coordinates are rejected consistently. (#45)

### Fixed

- Fixed `SheetPort` evaluation overrides leaking after invalid deterministic-mode requests and made staged input writes atomic across multi-port and range updates. (#43, #46)
- Restored whole/open-ended range dependency scheduling for far-formula rows and dynamic `INDIRECT` consumers, improving recalculation correctness for compressed and open-ended ranges. (#48)
- Fixed `INDEX` over single-row references so two-argument calls like `INDEX(A1:C1, 2)` resolve horizontally and match Excel/Python SDK expectations. (#50)

### Tooling and quality

- Bumped `next` in the docs site to `16.2.3` to resolve the remaining product-track Dependabot alert. (#52)

## [0.5.4] - 2026-04-06

### Fixed

- Fixed UTF-8-safe parsing for structured table specifiers so non-ASCII structured references no longer panic on invalid byte boundaries. (#40)
- Fixed Unicode case-insensitive matching for structured table names and headers, named ranges, database field/header matching, and exact/wildcard lookup text matching across parser, evaluator, and workbook integration paths. (#40)
- Fixed `SUMIFS` and related structured-table evaluation regressions for Unicode headers and criteria values, with new regression coverage across parser, engine, Arrow-backed evaluation, and workbook loader tests. (#40)

### Performance

- Improved text-heavy `MATCH`/`XMATCH`/`XLOOKUP` exact and wildcard scans by reusing cached lowered Arrow text lanes for view-backed searches and prepared text matchers for vector/reverse scan paths. In evaluator smoke benchmarks, this reduced lookup scan times by about `1.85x` for exact Arrow-view matches, `1.20x` for Arrow-view wildcards, `1.73x` for exact vector scans, and `3.05x` for vector wildcard scans.

## [0.5.3] - 2026-04-01

### Added

- Added explicit dual-runtime WebAssembly profiles: `portable-wasm` for raw/wasmtime-safe guests and `wasm-js` for browser/Node hosts via `wasm-bindgen`.
- Added CI validation for both wasm profiles, including a standalone portable wasm probe that inspects the final emitted `.wasm` import section to catch `wasm-bindgen`/browser regressions.

### Fixed

- Removed `wasm-bindgen`/JS runtime leakage from the portable wasm path by minimizing core chrono features, splitting ambient system clock support from the portable evaluator, and routing dynamic lookup randomness through the deterministic workbook-seeded RNG pathway.
- Preserved the browser/Node wasm story by making the JS binding crate explicitly opt into the `wasm-js` runtime profile instead of relying on incidental transitive behavior.
- Made GitHub release creation fall back gracefully to generated release notes when a `CHANGELOG.md` section for the tagged version is missing.

### Tooling and quality

- Excluded `formualizer-bench-core` from the default expensive workspace-wide clippy/test CI path so the comparative IronCalc benchmark harness no longer inflates baseline CI minutes.

## [0.5.2] - 2026-04-01

### Fixed

- Resolved all 9 open Dependabot security alerts (npm): bumped `next`, `rollup`, `picomatch`, `fumadocs-*`, and `brace-expansion` across docs-site, bindings/wasm, and benchmarks/harness.
- Enabled `formualizer-sheetport` standalone compilation for `wasm32-unknown-unknown` by removing the unconditional `umya_integration` feature and adding target-conditional `getrandom` wasm shims.
- Enabled `formualizer-eval` (and downstream `formualizer-workbook`, `formualizer-cffi`, `xtask`) compilation for `wasm32-unknown-unknown` by adding the same `getrandom` 0.2 + 0.3 wasm shims.

### Changed

- `formualizer-sheetport` no longer unconditionally enables the `umya_integration` feature on `formualizer-workbook`. Consumers needing umya support should enable the new `umya` feature on `formualizer-sheetport`.

## [0.5.1] - 2026-03-22

> Supersedes the incomplete `0.5.0` product release.

### Added

- Added pending symbolic formula healing so formulas referencing not-yet-defined names now evaluate as `#NAME?` and automatically heal when a matching workbook-scoped, sheet-scoped, or source-backed name is later created. (#33)
- Added the `RRI` financial function for equivalent rate-of-return calculations. (#25)

### Fixed

- Improved `IRR` convergence by using a two-phase solver with a Brent-method fallback, reducing `#NUM!` failures on difficult cash-flow patterns. (#24)
- Corrected `WEEKDAY`, `WEEKNUM`, and `DATEDIF("YD")` behavior by switching to serial-based date arithmetic that handles Excel's 1900 date-system quirks correctly. (#23)
- Hardened function arity validation with `min_args` checks to prevent panics when functions are called with too few arguments. (#26)
- Preserved workbook-global and sheet-local defined names across both Umya and Calamine import pathways, including correct local shadowing and same-name isolation across sheets. (#34)

### Performance

- Added recalculation plan reuse and static schedule caching for stable workbook topologies, improving repeat recalculation performance on unchanged dependency graphs. (#28)

### Tooling and quality

- Added a comparative benchmark harness with scenario plans, real-world anchors, and fairness-oriented reporting to improve performance validation and regression tracking. (#28)
- Expanded JSON-driven conformance coverage across info, logical, lookup, text, and date function families. (#32)

## [0.5.0] - 2026-03-22

- Incomplete product release due to partial publication during the release workflow. Superseded by `0.5.1`.

[Unreleased]: https://github.com/PSU3D0/formualizer/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/PSU3D0/formualizer/compare/v0.8.4...v0.9.0
[0.8.4]: https://github.com/PSU3D0/formualizer/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/PSU3D0/formualizer/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/PSU3D0/formualizer/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/PSU3D0/formualizer/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/PSU3D0/formualizer/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/PSU3D0/formualizer/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/PSU3D0/formualizer/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/PSU3D0/formualizer/compare/v0.5.9...v0.6.0
[0.5.9]: https://github.com/PSU3D0/formualizer/compare/v0.5.8...v0.5.9
[0.5.8]: https://github.com/PSU3D0/formualizer/compare/v0.5.7...v0.5.8
[0.5.7]: https://github.com/PSU3D0/formualizer/compare/v0.5.6...v0.5.7
[0.5.6]: https://github.com/PSU3D0/formualizer/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/PSU3D0/formualizer/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/PSU3D0/formualizer/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/PSU3D0/formualizer/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/PSU3D0/formualizer/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/PSU3D0/formualizer/compare/v0.4.4...v0.5.1
[0.5.0]: https://github.com/PSU3D0/formualizer/releases/tag/v0.5.0
