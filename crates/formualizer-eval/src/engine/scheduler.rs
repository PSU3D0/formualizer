use super::graph::DependencyGraph;
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
        // 1. Find strongly connected components using Tarjan's algorithm
        let sccs = self.tarjan_scc(vertices)?;

        // 2. Separate cyclic from acyclic components
        let (cycles, acyclic_sccs) = self.separate_cycles(sccs);

        // 3. Topologically sort acyclic components into layers
        let layers = self.build_layers(acyclic_sccs)?;

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
        if let Some(v) = self.graph.vertices().get(vertex.as_index()) {
            for &dependency in &v.dependencies {
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
        if let Some(v) = self.graph.vertices().get(vertex.as_index()) {
            v.dependencies.contains(&vertex)
        } else {
            false
        }
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
            if let Some(vertex) = self.graph.vertices().get(vertex_id.as_index()) {
                for &dep_id in &vertex.dependencies {
                    if vertex_set.contains(&dep_id) {
                        if let Some(in_degree) = in_degrees.get_mut(&vertex_id) {
                            *in_degree += 1;
                        }
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
                if let Some(vertex) = self.graph.vertices().get(u.as_index()) {
                    for &v_dep in &vertex.dependents {
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
}
