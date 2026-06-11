use super::DependencyGraph;
use super::vertex::VertexId;
use formualizer_common::ExcelError;
use rustc_hash::{FxHashMap, FxHashSet};

pub struct Scheduler<'a> {
    graph: &'a DependencyGraph,
}

#[derive(Debug, Clone)]
pub struct Layer {
    pub vertices: Vec<VertexId>,
}

/// One step of the canonical schedule walk: either an acyclic Kahn wave
/// (`Layer`, an index into `Schedule::layers`) or a cyclic SCC treated as a
/// single super-node (`Cycle`, an index into `Schedule::cycles`). Storing
/// indices keeps `Schedule::layers`/`Schedule::cycles` the single owners of
/// the vertex Vecs, so building or cloning a schedule never duplicates them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleUnit {
    Layer(u32),
    Cycle(u32),
}

#[derive(Debug, Clone)]
pub struct Schedule {
    /// Canonical walk order: condensation order over the dependency graph.
    /// Every `Cycle` unit appears after all units containing its external
    /// dependencies and before all units containing its external dependents.
    pub units: Vec<ScheduleUnit>,
    /// All cyclic SCCs (same contents as before `units` existed), for
    /// consumers that only need the set of cycles.
    pub cycles: Vec<Vec<VertexId>>,
    /// The `Layer` units in order, for consumers that only walk layers.
    pub layers: Vec<Layer>,
}

impl Schedule {
    fn from_parts(layers: Vec<Layer>, cycles: Vec<Vec<VertexId>>) -> Self {
        debug_assert!(
            cycles.is_empty(),
            "Schedule::from_parts is the cycle-free fast path"
        );
        let units = (0..layers.len() as u32).map(ScheduleUnit::Layer).collect();
        Schedule {
            units,
            cycles,
            layers,
        }
    }

    /// Resolve a `ScheduleUnit::Layer` index.
    pub fn unit_layer(&self, i: u32) -> &Layer {
        &self.layers[i as usize]
    }

    /// Resolve a `ScheduleUnit::Cycle` index.
    pub fn unit_cycle(&self, i: u32) -> &[VertexId] {
        &self.cycles[i as usize]
    }
}

impl<'a> Scheduler<'a> {
    pub fn new(graph: &'a DependencyGraph) -> Self {
        Self { graph }
    }

    pub fn create_schedule(&self, vertices: &[VertexId]) -> Result<Schedule, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!("scheduler", vertices = vertices.len()).entered();
        // 1. Find strongly connected components using Tarjan's algorithm
        #[cfg(feature = "tracing")]
        let _scc_span = tracing::info_span!("tarjan_scc").entered();
        let sccs = self.tarjan_scc(vertices)?;
        #[cfg(feature = "tracing")]
        drop(_scc_span);

        // 2. Separate cyclic from acyclic components
        let (cycles, acyclic_sccs) = self.separate_cycles(sccs);

        // 3. Topologically sort acyclic components into layers
        if self.graph.dynamic_topo_enabled() {
            // Dynamic-topo (PK) branch: pk_layers_for orders only the acyclic
            // subset, so we cannot interleave Cycle units by condensation
            // position here. Stay conservative on this experimental path and
            // emit ALL Cycle units first (matching today's stamp-cycles-first
            // semantics), then the pk layers as Layer units.
            let subset: Vec<VertexId> = acyclic_sccs.into_iter().flatten().collect();
            let layers = if subset.is_empty() {
                Vec::new()
            } else {
                self.graph
                    .pk_layers_for(&subset)
                    .unwrap_or(self.build_layers(vec![subset])?)
            };
            let mut units = Vec::with_capacity(cycles.len() + layers.len());
            let mut cycle_order: Vec<usize> = (0..cycles.len()).collect();
            cycle_order.sort_by_key(|&i| cycles[i].iter().copied().min());
            for i in cycle_order {
                units.push(ScheduleUnit::Cycle(i as u32));
            }
            units.extend((0..layers.len() as u32).map(ScheduleUnit::Layer));
            return Ok(Schedule {
                units,
                cycles,
                layers,
            });
        }

        if cycles.is_empty() {
            // Fast path: byte-for-byte today's layer construction.
            let layers = self.build_layers(acyclic_sccs)?;
            return Ok(Schedule::from_parts(layers, cycles));
        }

        // Cycle path: Kahn over the condensation (SCC-as-node).
        let (units, layers) = self.build_condensation_units(&cycles, acyclic_sccs, None)?;
        Ok(Schedule {
            units,
            cycles,
            layers,
        })
    }

    /// Create a schedule considering additional ephemeral (virtual) dependencies just for this pass.
    /// `vdeps` maps a vertex to extra dependency vertices that should be considered as incoming edges.
    pub fn create_schedule_with_virtual(
        &self,
        vertices: &[VertexId],
        vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    ) -> Result<Schedule, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!(
            "scheduler_with_virtual",
            vertices = vertices.len(),
            vdeps = vdeps.len()
        )
        .entered();
        // 1. SCC detection with virtual deps
        #[cfg(feature = "tracing")]
        let _scc_span = tracing::info_span!("tarjan_scc_with_virtual").entered();
        let sccs = self.tarjan_scc_with_virtual(vertices, vdeps)?;
        #[cfg(feature = "tracing")]
        drop(_scc_span);
        // 2. Separate cycles and acyclic components
        let (cycles, acyclic_sccs) = self.separate_cycles(sccs);
        // 3. Build layers over combined adjacency (graph + vdeps)
        #[cfg(feature = "tracing")]
        let _layers_span = tracing::info_span!("build_layers_with_virtual").entered();
        if cycles.is_empty() {
            // Fast path: byte-for-byte today's layer construction.
            let layers = self.build_layers_with_virtual(acyclic_sccs, vdeps)?;
            return Ok(Schedule::from_parts(layers, cycles));
        }
        // Cycle path: Kahn over the condensation (SCC-as-node), honoring
        // virtual deps as extra edges.
        let (units, layers) = self.build_condensation_units(&cycles, acyclic_sccs, Some(vdeps))?;
        Ok(Schedule {
            units,
            cycles,
            layers,
        })
    }

    /// Tarjan's strongly connected components algorithm
    pub fn tarjan_scc(&self, vertices: &[VertexId]) -> Result<Vec<Vec<VertexId>>, ExcelError> {
        self.tarjan_scc_impl(vertices, None)
    }

    /// Tarjan with virtual deps
    fn tarjan_scc_with_virtual(
        &self,
        vertices: &[VertexId],
        vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    ) -> Result<Vec<Vec<VertexId>>, ExcelError> {
        self.tarjan_scc_impl(vertices, Some(vdeps))
    }

    /// Iterative Tarjan over the scheduled subgraph, optionally honoring
    /// per-pass virtual dependency edges.
    ///
    /// The DFS uses an explicit frame stack (vertex, dependency list, next
    /// edge offset) instead of recursion, so depth is bounded by heap, not
    /// the thread stack — the recursive predecessor SIGABRTed around depth
    /// ~1500 in debug (2 MiB test stacks) on large SCCs AND on plain acyclic
    /// chains whose dependencies point at higher vertex ids (see
    /// `engine/tests/iterate_corpus_scale.rs`).
    ///
    /// Determinism contract: SCC emission order and within-SCC member order
    /// are byte-identical to the recursive version (pinned by the
    /// differential test in `engine/tests/tarjan_differential.rs` and the
    /// ordering invariants in `engine/tests/schedule_units.rs`):
    /// * dependencies are walked in adjacency order (base slice/Vec, then
    ///   the vertex's virtual deps, exactly as the recursive code iterated);
    /// * a vertex's SCC is emitted when its frame is exhausted and
    ///   `lowlink == index` (the same program point as the recursive
    ///   post-order emission);
    /// * SCC members are popped off the Tarjan stack in the same order.
    fn tarjan_scc_impl(
        &self,
        vertices: &[VertexId],
        vdeps: Option<&FxHashMap<VertexId, Vec<VertexId>>>,
    ) -> Result<Vec<Vec<VertexId>>, ExcelError> {
        /// One vertex's dependency list, materialized once per DFS frame.
        enum DepList<'g> {
            Slice(&'g [VertexId]),
            Owned(Vec<VertexId>),
        }
        impl DepList<'_> {
            #[inline]
            fn get(&self, i: usize) -> Option<VertexId> {
                match self {
                    DepList::Slice(s) => s.get(i).copied(),
                    DepList::Owned(v) => v.get(i).copied(),
                }
            }
        }

        let deps_of = |vertex: VertexId| -> DepList<'_> {
            // Edge order must match the recursive implementation: the base
            // adjacency (zero-copy slice when available), with the vertex's
            // virtual deps appended when present.
            if let Some(extra) = vdeps.and_then(|m| m.get(&vertex)) {
                let mut combined: Vec<VertexId> =
                    if let Some(base) = self.graph.dependencies_slice(vertex) {
                        base.to_vec()
                    } else {
                        self.graph.get_dependencies(vertex)
                    };
                combined.extend(extra.iter().copied());
                DepList::Owned(combined)
            } else if let Some(base) = self.graph.dependencies_slice(vertex) {
                DepList::Slice(base)
            } else {
                DepList::Owned(self.graph.get_dependencies(vertex))
            }
        };

        // Compact the candidate set into dense local ids once, up front. All
        // per-edge / per-frame bookkeeping (index, lowlink, on-stack) then
        // becomes a plain array op instead of an FxHashMap probe; the only
        // remaining hash per edge is the `VertexId -> local id` membership
        // lookup, which doubles as the old `vertex_set.contains` filter.
        // `VertexId` is a `u32` newtype, so `u32` local ids cannot overflow.
        const UNVISITED: u32 = u32::MAX;
        let mut local_of: FxHashMap<VertexId, u32> =
            FxHashMap::with_capacity_and_hasher(vertices.len(), Default::default());
        let mut vertex_of_local: Vec<VertexId> = Vec::with_capacity(vertices.len());
        for &v in vertices {
            if let std::collections::hash_map::Entry::Vacant(slot) = local_of.entry(v) {
                slot.insert(vertex_of_local.len() as u32);
                vertex_of_local.push(v);
            }
        }
        let n = vertex_of_local.len();

        let mut index_counter: u32 = 0;
        let mut indices: Vec<u32> = vec![UNVISITED; n];
        let mut lowlinks: Vec<u32> = vec![0; n];
        let mut on_stack: Vec<bool> = vec![false; n];
        let mut stack: Vec<u32> = Vec::with_capacity(n);
        let mut sccs: Vec<Vec<VertexId>> = Vec::new();

        // Explicit DFS frames: (local id, its dependency list, next edge
        // offset). Pre-sized to the worst case (one frame per vertex, e.g. a
        // single deep chain) so deep schedules never re-grow the stack.
        let mut frames: Vec<(u32, DepList<'_>, u32)> = Vec::with_capacity(n);

        for &root_vertex in vertices {
            let root = local_of[&root_vertex];
            if indices[root as usize] != UNVISITED {
                continue;
            }
            indices[root as usize] = index_counter;
            lowlinks[root as usize] = index_counter;
            index_counter += 1;
            stack.push(root);
            on_stack[root as usize] = true;
            frames.push((root, deps_of(root_vertex), 0));

            while let Some(frame) = frames.last_mut() {
                let vertex = frame.0 as usize;
                if let Some(dep_vertex) = frame.1.get(frame.2 as usize) {
                    frame.2 += 1;
                    // Only consider dependencies that are part of the current
                    // scheduling task.
                    let Some(&dep) = local_of.get(&dep_vertex) else {
                        continue;
                    };
                    let d = dep as usize;
                    if indices[d] == UNVISITED {
                        // Not yet visited: descend (the recursive call).
                        indices[d] = index_counter;
                        lowlinks[d] = index_counter;
                        index_counter += 1;
                        stack.push(dep);
                        on_stack[d] = true;
                        frames.push((dep, deps_of(dep_vertex), 0));
                    } else if on_stack[d] {
                        // On the Tarjan stack: in the current SCC.
                        let dep_index = indices[d];
                        if dep_index < lowlinks[vertex] {
                            lowlinks[vertex] = dep_index;
                        }
                    }
                } else {
                    // Vertex exhausted: emit its SCC if it is a root, then
                    // return to the parent frame, folding the child lowlink in
                    // (the post-recursion `min(lowlink[v], lowlink[dep])`).
                    let vertex_lowlink = lowlinks[vertex];
                    if vertex_lowlink == indices[vertex] {
                        let mut scc = Vec::new();
                        loop {
                            let w = stack.pop().unwrap();
                            on_stack[w as usize] = false;
                            scc.push(vertex_of_local[w as usize]);
                            if w as usize == vertex {
                                break;
                            }
                        }
                        sccs.push(scc);
                    }
                    frames.pop();
                    if let Some(parent) = frames.last() {
                        let p = parent.0 as usize;
                        if vertex_lowlink < lowlinks[p] {
                            lowlinks[p] = vertex_lowlink;
                        }
                    }
                }
            }
        }

        Ok(sccs)
    }

    /// Test-only visibility shim over the private virtual-deps entry, used by
    /// the differential test in `engine/tests/tarjan_differential.rs`.
    #[cfg(test)]
    pub(crate) fn tarjan_scc_with_virtual_for_tests(
        &self,
        vertices: &[VertexId],
        vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    ) -> Result<Vec<Vec<VertexId>>, ExcelError> {
        self.tarjan_scc_with_virtual(vertices, vdeps)
    }

    /// Recursive reference implementation, retained ONLY for the differential
    /// test (`engine/tests/tarjan_differential.rs`) that proves the iterative
    /// rewrite emits byte-identical SCC output. Never call on deep graphs
    /// without a large stack: it overflows around depth ~1500 in debug.
    #[cfg(test)]
    pub(crate) fn tarjan_scc_recursive_reference(
        &self,
        vertices: &[VertexId],
    ) -> Result<Vec<Vec<VertexId>>, ExcelError> {
        let mut index_counter = 0;
        let mut stack = Vec::new();
        let mut indices = FxHashMap::default();
        let mut lowlinks = FxHashMap::default();
        let mut on_stack = FxHashSet::default();
        let mut sccs = Vec::new();
        let vertex_set: FxHashSet<VertexId> = vertices.iter().copied().collect();

        for &vertex in vertices {
            if !indices.contains_key(&vertex) {
                self.tarjan_visit(
                    vertex,
                    &mut index_counter,
                    &mut stack,
                    &mut indices,
                    &mut lowlinks,
                    &mut on_stack,
                    &mut sccs,
                    &vertex_set,
                )?;
            }
        }

        Ok(sccs)
    }

    /// Recursive reference with virtual deps; see
    /// [`Self::tarjan_scc_recursive_reference`].
    #[cfg(test)]
    pub(crate) fn tarjan_scc_with_virtual_recursive_reference(
        &self,
        vertices: &[VertexId],
        vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    ) -> Result<Vec<Vec<VertexId>>, ExcelError> {
        let mut index_counter = 0;
        let mut stack = Vec::new();
        let mut indices = FxHashMap::default();
        let mut lowlinks = FxHashMap::default();
        let mut on_stack = FxHashSet::default();
        let mut sccs = Vec::new();
        let vertex_set: FxHashSet<VertexId> = vertices.iter().copied().collect();

        for &vertex in vertices {
            if !indices.contains_key(&vertex) {
                self.tarjan_visit_with_virtual(
                    vertex,
                    &mut index_counter,
                    &mut stack,
                    &mut indices,
                    &mut lowlinks,
                    &mut on_stack,
                    &mut sccs,
                    &vertex_set,
                    vdeps,
                )?;
            }
        }

        Ok(sccs)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn tarjan_visit(
        &self,
        vertex: VertexId,
        index_counter: &mut usize,
        stack: &mut Vec<VertexId>,
        indices: &mut FxHashMap<VertexId, usize>,
        lowlinks: &mut FxHashMap<VertexId, usize>,
        on_stack: &mut FxHashSet<VertexId>,
        sccs: &mut Vec<Vec<VertexId>>,
        vertex_set: &FxHashSet<VertexId>,
    ) -> Result<(), ExcelError> {
        // Set the depth index for vertex to the smallest unused index
        indices.insert(vertex, *index_counter);
        lowlinks.insert(vertex, *index_counter);
        *index_counter += 1;
        stack.push(vertex);
        on_stack.insert(vertex);

        // Consider successors of vertex (dependencies)
        if let Some(dependencies) = self.graph.dependencies_slice(vertex) {
            for &dependency in dependencies {
                // Only consider dependencies that are part of the current scheduling task
                if !vertex_set.contains(&dependency) {
                    continue;
                }

                if !indices.contains_key(&dependency) {
                    // Successor dependency has not yet been visited; recurse on it
                    self.tarjan_visit(
                        dependency,
                        index_counter,
                        stack,
                        indices,
                        lowlinks,
                        on_stack,
                        sccs,
                        vertex_set,
                    )?;
                    let dep_lowlink = lowlinks[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_lowlink));
                } else if on_stack.contains(&dependency) {
                    // Successor dependency is in stack and hence in the current SCC
                    let dep_index = indices[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_index));
                }
            }
        } else {
            let dependencies = self.graph.get_dependencies(vertex);
            for dependency in dependencies {
                // Only consider dependencies that are part of the current scheduling task
                if !vertex_set.contains(&dependency) {
                    continue;
                }

                if !indices.contains_key(&dependency) {
                    // Successor dependency has not yet been visited; recurse on it
                    self.tarjan_visit(
                        dependency,
                        index_counter,
                        stack,
                        indices,
                        lowlinks,
                        on_stack,
                        sccs,
                        vertex_set,
                    )?;
                    let dep_lowlink = lowlinks[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_lowlink));
                } else if on_stack.contains(&dependency) {
                    // Successor dependency is in stack and hence in the current SCC
                    let dep_index = indices[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_index));
                }
            }
        }

        // If vertex is a root node, pop the stack and print an SCC
        if lowlinks[&vertex] == indices[&vertex] {
            let mut scc = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack.remove(&w);
                scc.push(w);
                if w == vertex {
                    break;
                }
            }
            sccs.push(scc);
        }

        Ok(())
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn tarjan_visit_with_virtual(
        &self,
        vertex: VertexId,
        index_counter: &mut usize,
        stack: &mut Vec<VertexId>,
        indices: &mut FxHashMap<VertexId, usize>,
        lowlinks: &mut FxHashMap<VertexId, usize>,
        on_stack: &mut FxHashSet<VertexId>,
        sccs: &mut Vec<Vec<VertexId>>,
        vertex_set: &FxHashSet<VertexId>,
        vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    ) -> Result<(), ExcelError> {
        // Set the depth index for vertex to the smallest unused index
        indices.insert(vertex, *index_counter);
        lowlinks.insert(vertex, *index_counter);
        *index_counter += 1;
        stack.push(vertex);
        on_stack.insert(vertex);

        // Consider successors of vertex (dependencies) including virtual deps
        if let Some(extra) = vdeps.get(&vertex) {
            let mut dependencies: Vec<VertexId> =
                if let Some(base) = self.graph.dependencies_slice(vertex) {
                    base.to_vec()
                } else {
                    self.graph.get_dependencies(vertex)
                };
            dependencies.extend(extra.iter().copied());

            for dependency in dependencies {
                // Only consider dependencies that are part of the current scheduling task
                if !vertex_set.contains(&dependency) {
                    continue;
                }

                if !indices.contains_key(&dependency) {
                    // Successor dependency has not yet been visited; recurse on it
                    self.tarjan_visit_with_virtual(
                        dependency,
                        index_counter,
                        stack,
                        indices,
                        lowlinks,
                        on_stack,
                        sccs,
                        vertex_set,
                        vdeps,
                    )?;
                    let dep_lowlink = lowlinks[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_lowlink));
                } else if on_stack.contains(&dependency) {
                    // Successor dependency is in stack and hence in the current SCC
                    let dep_index = indices[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_index));
                }
            }
        } else if let Some(dependencies) = self.graph.dependencies_slice(vertex) {
            for &dependency in dependencies {
                // Only consider dependencies that are part of the current scheduling task
                if !vertex_set.contains(&dependency) {
                    continue;
                }

                if !indices.contains_key(&dependency) {
                    // Successor dependency has not yet been visited; recurse on it
                    self.tarjan_visit_with_virtual(
                        dependency,
                        index_counter,
                        stack,
                        indices,
                        lowlinks,
                        on_stack,
                        sccs,
                        vertex_set,
                        vdeps,
                    )?;
                    let dep_lowlink = lowlinks[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_lowlink));
                } else if on_stack.contains(&dependency) {
                    // Successor dependency is in stack and hence in the current SCC
                    let dep_index = indices[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_index));
                }
            }
        } else {
            let dependencies = self.graph.get_dependencies(vertex);
            for dependency in dependencies {
                // Only consider dependencies that are part of the current scheduling task
                if !vertex_set.contains(&dependency) {
                    continue;
                }

                if !indices.contains_key(&dependency) {
                    // Successor dependency has not yet been visited; recurse on it
                    self.tarjan_visit_with_virtual(
                        dependency,
                        index_counter,
                        stack,
                        indices,
                        lowlinks,
                        on_stack,
                        sccs,
                        vertex_set,
                        vdeps,
                    )?;
                    let dep_lowlink = lowlinks[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_lowlink));
                } else if on_stack.contains(&dependency) {
                    // Successor dependency is in stack and hence in the current SCC
                    let dep_index = indices[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_index));
                }
            }
        }

        // If vertex is a root node, pop the stack and produce an SCC
        if lowlinks[&vertex] == indices[&vertex] {
            let mut scc = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack.remove(&w);
                scc.push(w);
                if w == vertex {
                    break;
                }
            }
            sccs.push(scc);
        }

        Ok(())
    }

    pub(crate) fn separate_cycles(
        &self,
        sccs: Vec<Vec<VertexId>>,
    ) -> (Vec<Vec<VertexId>>, Vec<Vec<VertexId>>) {
        let mut cycles = Vec::new();
        let mut acyclic = Vec::new();

        for scc in sccs {
            if scc.len() > 1 || (scc.len() == 1 && self.has_self_loop(scc[0])) {
                cycles.push(scc);
            } else {
                acyclic.push(scc);
            }
        }

        (cycles, acyclic)
    }

    fn has_self_loop(&self, vertex: VertexId) -> bool {
        self.graph.has_self_loop(vertex)
    }

    pub(crate) fn build_layers(
        &self,
        acyclic_sccs: Vec<Vec<VertexId>>,
    ) -> Result<Vec<Layer>, ExcelError> {
        let vertices: Vec<VertexId> = acyclic_sccs.into_iter().flatten().collect();
        if vertices.is_empty() {
            return Ok(Vec::new());
        }
        let vertex_set: FxHashSet<VertexId> = vertices.iter().copied().collect();

        // Calculate in-degrees for all vertices in the acyclic subgraph
        let mut in_degrees: FxHashMap<VertexId, usize> = vertices.iter().map(|&v| (v, 0)).collect();
        for &vertex_id in &vertices {
            if let Some(dependencies) = self.graph.dependencies_slice(vertex_id) {
                for &dep_id in dependencies {
                    if vertex_set.contains(&dep_id)
                        && let Some(in_degree) = in_degrees.get_mut(&vertex_id)
                    {
                        *in_degree += 1;
                    }
                }
            } else {
                let dependencies = self.graph.get_dependencies(vertex_id);
                for dep_id in dependencies {
                    if vertex_set.contains(&dep_id)
                        && let Some(in_degree) = in_degrees.get_mut(&vertex_id)
                    {
                        *in_degree += 1;
                    }
                }
            }
        }

        // Initialize the queue with all nodes having an in-degree of 0
        let mut queue: std::collections::VecDeque<VertexId> = in_degrees
            .iter()
            .filter(|&(_, &in_degree)| in_degree == 0)
            .map(|(&v, _)| v)
            .collect();

        let mut layers = Vec::new();
        let mut processed_count = 0;

        while !queue.is_empty() {
            let mut current_layer_vertices = Vec::new();
            for _ in 0..queue.len() {
                let u = queue.pop_front().unwrap();
                current_layer_vertices.push(u);
                processed_count += 1;

                // For each dependent of u, reduce its in-degree
                if let Some(dependents) = self.graph.dependents_slice(u) {
                    for &v_dep in dependents {
                        if let Some(in_degree) = in_degrees.get_mut(&v_dep) {
                            *in_degree -= 1;
                            if *in_degree == 0 {
                                queue.push_back(v_dep);
                            }
                        }
                    }
                } else {
                    for v_dep in self.graph.get_dependents(u) {
                        if let Some(in_degree) = in_degrees.get_mut(&v_dep) {
                            *in_degree -= 1;
                            if *in_degree == 0 {
                                queue.push_back(v_dep);
                            }
                        }
                    }
                }
            }
            // Sort for deterministic output in tests
            current_layer_vertices.sort();
            layers.push(Layer {
                vertices: current_layer_vertices,
            });
        }

        if processed_count != vertices.len() {
            return Err(
                ExcelError::new(formualizer_common::ExcelErrorKind::Circ).with_message(
                    "Unexpected cycle detected in acyclic components during layer construction"
                        .to_string(),
                ),
            );
        }

        Ok(layers)
    }

    /// Kahn's algorithm over the condensation of the scheduled subgraph:
    /// each cyclic SCC is a super-node, each acyclic vertex a singleton node.
    ///
    /// Per Kahn wave we emit first the wave's `Cycle` units (ordered by
    /// smallest member `VertexId` for determinism), then one `Layer` unit with
    /// the wave's singleton vertices (sorted, as in `build_layers`). Within a
    /// wave there are no inter-node edges, so this ordering is semantically
    /// free. The result guarantees every `Cycle` unit appears after all units
    /// containing its external dependencies and before all units containing
    /// its external dependents (pinned by tests in
    /// `engine/tests/schedule_units.rs`).
    ///
    /// Returns the unit walk plus the `Layer` units in order (the
    /// compatibility `Schedule::layers` view).
    fn build_condensation_units(
        &self,
        cycles: &[Vec<VertexId>],
        acyclic_sccs: Vec<Vec<VertexId>>,
        vdeps: Option<&FxHashMap<VertexId, Vec<VertexId>>>,
    ) -> Result<(Vec<ScheduleUnit>, Vec<Layer>), ExcelError> {
        let singletons: Vec<VertexId> = acyclic_sccs.into_iter().flatten().collect();
        let cycle_node_count = cycles.len();
        let node_count = cycle_node_count + singletons.len();

        // Map every scheduled vertex to its condensation node.
        let mut node_of: FxHashMap<VertexId, usize> = FxHashMap::default();
        for (i, cycle) in cycles.iter().enumerate() {
            for &v in cycle {
                node_of.insert(v, i);
            }
        }
        for (j, &v) in singletons.iter().enumerate() {
            node_of.insert(v, cycle_node_count + j);
        }

        // Build in-degrees and the dependents adjacency from a single scan of
        // the dependency direction, so both sides are guaranteed consistent.
        // Edges to vertices outside the scheduled set are ignored
        // (membership == presence in `node_of`); intra-node edges are ignored.
        let mut in_degrees: Vec<usize> = vec![0; node_count];
        let mut node_dependents: Vec<Vec<usize>> = vec![Vec::new(); node_count];
        let scan_dep = |from_node: usize,
                        dep: VertexId,
                        in_degrees: &mut Vec<usize>,
                        node_dependents: &mut Vec<Vec<usize>>| {
            if let Some(&dep_node) = node_of.get(&dep)
                && dep_node != from_node
            {
                in_degrees[from_node] += 1;
                node_dependents[dep_node].push(from_node);
            }
        };
        for (&v, &n_v) in node_of.iter() {
            if let Some(deps) = self.graph.dependencies_slice(v) {
                for &dep in deps {
                    scan_dep(n_v, dep, &mut in_degrees, &mut node_dependents);
                }
            } else {
                for dep in self.graph.get_dependencies(v) {
                    scan_dep(n_v, dep, &mut in_degrees, &mut node_dependents);
                }
            }
            if let Some(extra) = vdeps.and_then(|m| m.get(&v)) {
                for &dep in extra {
                    scan_dep(n_v, dep, &mut in_degrees, &mut node_dependents);
                }
            }
        }

        // Kahn by waves over condensation nodes.
        let mut current: Vec<usize> = (0..node_count).filter(|&n| in_degrees[n] == 0).collect();
        let mut units = Vec::new();
        let mut layers = Vec::new();
        let mut processed_count = 0usize;
        while !current.is_empty() {
            let mut wave_cycles: Vec<usize> = current
                .iter()
                .copied()
                .filter(|&n| n < cycle_node_count)
                .collect();
            wave_cycles.sort_by_key(|&n| cycles[n].iter().copied().min());
            for n in wave_cycles {
                units.push(ScheduleUnit::Cycle(n as u32));
            }

            let mut wave_vertices: Vec<VertexId> = current
                .iter()
                .copied()
                .filter(|&n| n >= cycle_node_count)
                .map(|n| singletons[n - cycle_node_count])
                .collect();
            if !wave_vertices.is_empty() {
                // Sort for deterministic output, as in build_layers.
                wave_vertices.sort();
                units.push(ScheduleUnit::Layer(layers.len() as u32));
                layers.push(Layer {
                    vertices: wave_vertices,
                });
            }

            processed_count += current.len();
            let mut next = Vec::new();
            for &n in &current {
                for &dependent in &node_dependents[n] {
                    in_degrees[dependent] -= 1;
                    if in_degrees[dependent] == 0 {
                        next.push(dependent);
                    }
                }
            }
            current = next;
        }

        if processed_count != node_count {
            return Err(
                ExcelError::new(formualizer_common::ExcelErrorKind::Circ).with_message(
                    "Unexpected cycle detected in condensation during unit construction"
                        .to_string(),
                ),
            );
        }

        Ok((units, layers))
    }

    pub(crate) fn build_layers_with_virtual(
        &self,
        acyclic_sccs: Vec<Vec<VertexId>>,
        vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    ) -> Result<Vec<Layer>, ExcelError> {
        use std::collections::VecDeque;
        let vertices: Vec<VertexId> = acyclic_sccs.into_iter().flatten().collect();
        if vertices.is_empty() {
            return Ok(Vec::new());
        }
        let vertex_set: FxHashSet<VertexId> = vertices.iter().copied().collect();

        // Build combined adjacency (dependencies and dependents) within the subset
        let mut combined_deps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default();
        let mut combined_out: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default();
        for &v in &vertices {
            let mut deps: Vec<VertexId> = Vec::new();
            if let Some(base) = self.graph.dependencies_slice(v) {
                deps.extend(base.iter().copied().filter(|d| vertex_set.contains(d)));
            } else {
                deps.extend(
                    self.graph
                        .get_dependencies(v)
                        .into_iter()
                        .filter(|d| vertex_set.contains(d)),
                );
            }
            if let Some(extra) = vdeps.get(&v) {
                deps.extend(extra.iter().copied().filter(|d| vertex_set.contains(d)));
            }
            deps.sort_unstable();
            deps.dedup();
            combined_deps.insert(v, deps);
        }
        // invert
        for (&v, deps) in combined_deps.iter() {
            for &d in deps {
                combined_out.entry(d).or_default().push(v);
            }
        }
        // in-degrees
        let mut in_degrees: FxHashMap<VertexId, usize> = FxHashMap::default();
        for &v in &vertices {
            let indeg = combined_deps.get(&v).map(|v| v.len()).unwrap_or(0);
            in_degrees.insert(v, indeg);
        }
        // queue of 0 in-degree
        let mut queue: VecDeque<VertexId> = in_degrees
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&v, _)| v)
            .collect();

        let mut layers = Vec::new();
        let mut processed_count = 0;
        while !queue.is_empty() {
            let mut cur = Vec::new();
            for _ in 0..queue.len() {
                let u = queue.pop_front().unwrap();
                cur.push(u);
                processed_count += 1;
                if let Some(dependents) = combined_out.get(&u) {
                    for &w in dependents {
                        if let Some(ind) = in_degrees.get_mut(&w) {
                            *ind = ind.saturating_sub(1);
                            if *ind == 0 {
                                queue.push_back(w);
                            }
                        }
                    }
                }
            }
            cur.sort_unstable();
            layers.push(Layer { vertices: cur });
        }
        if processed_count != vertices.len() {
            return Err(
                ExcelError::new(formualizer_common::ExcelErrorKind::Circ).with_message(
                    "Unexpected cycle detected in acyclic components during layer construction (virtual)"
                        .to_string(),
                ),
            );
        }
        Ok(layers)
    }
}
