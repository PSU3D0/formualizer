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
                    )?;
                    let dep_lowlink = lowlinks[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_lowlink));
                } else if on_stack.contains(&dependency) {
                    // Successor dependency is in stack and hence in the current SCC
                    let dep_index = indices[&dependency];
                    lowlinks.insert(vertex, lowlinks[&vertex].min(dep_index));
                }
                // If dependency is not on stack, then (vertex, dependency) is a cross-edge in DFS tree
                // and must be ignored
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

    fn separate_cycles(
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

    fn build_layers(&self, acyclic_sccs: Vec<Vec<VertexId>>) -> Result<Vec<Layer>, ExcelError> {
        // TODO: Implement layer building using Kahn's algorithm
        // For now, just return a single layer with all vertices
        let vertices: Vec<VertexId> = acyclic_sccs.into_iter().flatten().collect();
        if vertices.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![Layer { vertices }])
        }
    }
}
