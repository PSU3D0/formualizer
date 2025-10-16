use super::DependencyGraph;
use super::vertex::VertexId;
use formualizer_common::ExcelError;
use rustc_hash::{FxHashMap, FxHashSet};

pub struct Scheduler<'a> {
    graph: &'a DependencyGraph,
}

#[derive(Debug)]
pub struct Layer {
    pub vertices: Vec<VertexId>,
}

#[derive(Debug)]
pub struct Schedule {
    pub layers: Vec<Layer>,
    pub cycles: Vec<Vec<VertexId>>,
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
        let layers = if self.graph.dynamic_topo_enabled() {
            let subset: Vec<VertexId> = acyclic_sccs.into_iter().flatten().collect();
            if subset.is_empty() {
                Vec::new()
            } else {
                self.graph
                    .pk_layers_for(&subset)
                    .unwrap_or(self.build_layers(vec![subset])?)
            }
        } else {
            self.build_layers(acyclic_sccs)?
        };

        Ok(Schedule { layers, cycles })
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
        let layers = self.build_layers_with_virtual(acyclic_sccs, vdeps)?;
        #[cfg(feature = "tracing")]
        drop(_layers_span);
        Ok(Schedule { layers, cycles })
    }

    /// Tarjan's strongly connected components algorithm
    pub fn tarjan_scc(&self, vertices: &[VertexId]) -> Result<Vec<Vec<VertexId>>, ExcelError> {
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

    /// Tarjan with virtual deps
    fn tarjan_scc_with_virtual(
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
        let dependencies = self.graph.get_dependencies(vertex);
        for &dependency in &dependencies {
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
        let mut dependencies = self.graph.get_dependencies(vertex).to_vec();
        if let Some(extra) = vdeps.get(&vertex) {
            dependencies.extend(extra.iter().copied());
        }
        for dependency in dependencies.into_iter() {
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
            let dependencies = self.graph.get_dependencies(vertex_id);
            for &dep_id in &dependencies {
                if vertex_set.contains(&dep_id)
                    && let Some(in_degree) = in_degrees.get_mut(&vertex_id)
                {
                    *in_degree += 1;
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
                for v_dep in self.graph.get_dependents(u) {
                    if let Some(in_degree) = in_degrees.get_mut(&v_dep) {
                        *in_degree -= 1;
                        if *in_degree == 0 {
                            queue.push_back(v_dep);
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
            let mut deps: Vec<VertexId> = self
                .graph
                .get_dependencies(v)
                .iter()
                .copied()
                .filter(|d| vertex_set.contains(d))
                .collect();
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
