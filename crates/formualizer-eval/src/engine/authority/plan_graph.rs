//! Counted, fallible graph kernel for ARC's real + auxiliary nodes.
//!
//! This is not a planner: callers must construct the exact ARC arcs, account
//! for their input storage, and apply the plan-wide work cap. Edges point from
//! precedent to reader. Tarjan numbers components in reverse topological order
//! (every inter-component arc has source > destination), allowing later level
//! propagation without sorting or rebuilding a condensation graph.

use super::plan_control::PlanControl;
use super::store::AuthorityError;
use formualizer_common::ExcelError;

const NONE: usize = usize::MAX;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GraphWork {
    pub initialize: u64,
    pub vertices: u64,
    pub edges: u64,
    pub dfs: u64,
    pub members: u64,
}

impl GraphWork {
    pub fn total(self) -> u64 {
        self.initialize + self.vertices + self.edges + self.dfs + self.members
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GraphError {
    Authority(AuthorityError),
    Runtime(ExcelError),
    /// A planner construction bug, never a cycle or an unsupported feature.
    InvalidEndpoint,
}

impl From<AuthorityError> for GraphError {
    fn from(error: AuthorityError) -> Self {
        Self::Authority(error)
    }
}

impl From<ExcelError> for GraphError {
    fn from(error: ExcelError) -> Self {
        Self::Runtime(error)
    }
}

fn bytes<T>(n: usize) -> Result<u64, AuthorityError> {
    n.checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(AuthorityError::Alloc)
}

fn add(a: u64, b: u64) -> Result<u64, AuthorityError> {
    a.checked_add(b).ok_or(AuthorityError::Alloc)
}

fn admit(needed: u64, limit: Option<u64>) -> Result<(), AuthorityError> {
    if let Some(limit) = limit
        && needed > limit
    {
        return Err(AuthorityError::Admission {
            resource: "scratch",
            needed,
            limit,
        });
    }
    Ok(())
}

fn reserve<T>(len: usize) -> Result<Vec<T>, AuthorityError> {
    let mut out = Vec::new();
    out.try_reserve_exact(len)
        .map_err(|_| AuthorityError::Alloc)?;
    Ok(out)
}

fn fill(
    out: &mut Vec<usize>,
    len: usize,
    value: usize,
    work: &mut GraphWork,
    control: &mut PlanControl<impl FnMut(u64) -> Result<(), ExcelError>>,
) -> Result<(), GraphError> {
    for _ in 0..len {
        work.initialize += 1;
        control.tick()?;
        out.push(value);
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct PlanGraph {
    offsets: Vec<usize>,
    targets: Vec<usize>,
    pub work: GraphWork,
    /// This call's peak only; borrowed caller inputs are charged by the caller.
    pub peak_heap_bytes: u64,
}

impl PlanGraph {
    /// Stable CSR construction, preserving the supplied order within a source.
    /// Duplicate arcs and self-arcs are legal. `scratch_limit` excludes storage
    /// already held by the caller (including the borrowed arc list).
    pub fn build(
        nodes: usize,
        arcs: &[(usize, usize)],
        scratch_limit: Option<u64>,
    ) -> Result<Self, GraphError> {
        Self::build_controlled(nodes, arcs, scratch_limit, |_| Ok(()))
    }

    /// The callback receives bounded actual work deltas, including entry and
    /// final checkpoints. Cancellation/resource errors propagate unchanged.
    pub fn build_controlled(
        nodes: usize,
        arcs: &[(usize, usize)],
        scratch_limit: Option<u64>,
        checkpoint: impl FnMut(u64) -> Result<(), ExcelError>,
    ) -> Result<Self, GraphError> {
        let mut control = PlanControl::new(checkpoint)?;
        let offset_len = nodes.checked_add(1).ok_or(AuthorityError::Alloc)?;
        let peak = add(
            bytes::<usize>(offset_len)?,
            add(bytes::<usize>(arcs.len())?, bytes::<usize>(nodes)?)?,
        )?;
        admit(peak, scratch_limit)?;
        let mut offsets = reserve(offset_len)?;
        let mut targets = reserve(arcs.len())?;
        let mut cursor = reserve(nodes)?;
        let mut work = GraphWork::default();
        fill(&mut offsets, offset_len, 0, &mut work, &mut control)?;
        fill(&mut targets, arcs.len(), 0, &mut work, &mut control)?;
        fill(&mut cursor, nodes, 0, &mut work, &mut control)?;
        for &(from, to) in arcs {
            work.edges += 1;
            control.tick()?;
            if from >= nodes || to >= nodes {
                return Err(GraphError::InvalidEndpoint);
            }
            offsets[from + 1] += 1;
        }
        for (node, position) in cursor.iter_mut().enumerate() {
            work.vertices += 1;
            control.tick()?;
            *position = offsets[node];
            offsets[node + 1] += *position;
        }
        for &(from, to) in arcs {
            work.edges += 1;
            control.tick()?;
            targets[cursor[from]] = to;
            cursor[from] += 1;
        }
        control.flush()?;
        Ok(Self {
            offsets,
            targets,
            work,
            peak_heap_bytes: peak,
        })
    }

    pub fn node_count(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn successors(&self, node: usize) -> &[usize] {
        &self.targets[self.offsets[node]..self.offsets[node + 1]]
    }

    pub fn heap_bytes(&self) -> u64 {
        ((self.offsets.capacity() + self.targets.capacity()) * size_of::<usize>()) as u64
    }

    /// Iterative Tarjan, O(nodes + arcs), no recursion or per-component heap.
    /// Every vector is admitted simultaneously before allocation. The supplied
    /// remaining limit excludes this graph and all other live caller storage.
    pub fn components(&self, scratch_limit: Option<u64>) -> Result<Components, GraphError> {
        self.components_controlled(scratch_limit, |_| Ok(()))
    }

    /// Checks inside initialization, arc traversal, DFS and SCC member pops,
    /// not merely between SCCs (one SCC can contain the entire workbook).
    pub fn components_controlled(
        &self,
        scratch_limit: Option<u64>,
        checkpoint: impl FnMut(u64) -> Result<(), ExcelError>,
    ) -> Result<Components, GraphError> {
        let mut control = PlanControl::new(checkpoint)?;
        let n = self.node_count();
        let offsets_len = n.checked_add(1).ok_or(AuthorityError::Alloc)?;
        // Output: component_of, members, offsets. Temporary: index, low,
        // next_arc, active stack, DFS stack. No separate on-stack map: a
        // discovered vertex is active iff component_of[v] == NONE.
        let peak = add(
            bytes::<usize>(n)?
                .checked_mul(7)
                .ok_or(AuthorityError::Alloc)?,
            bytes::<usize>(offsets_len)?,
        )?;
        admit(peak, scratch_limit)?;
        let mut component_of = reserve(n)?;
        let mut members = reserve(n)?;
        let mut offsets = reserve(offsets_len)?;
        let mut index = reserve(n)?;
        let mut low = reserve(n)?;
        let mut next_arc = reserve(n)?;
        let mut active = reserve(n)?;
        let mut dfs = reserve(n)?;
        let mut work = GraphWork::default();
        fill(&mut component_of, n, NONE, &mut work, &mut control)?;
        fill(&mut index, n, NONE, &mut work, &mut control)?;
        fill(&mut low, n, 0, &mut work, &mut control)?;
        fill(&mut next_arc, n, 0, &mut work, &mut control)?;
        fill(&mut offsets, 1, 0, &mut work, &mut control)?;
        let mut serial = 0;
        for root in 0..n {
            work.vertices += 1;
            control.tick()?;
            if index[root] != NONE {
                continue;
            }
            index[root] = serial;
            low[root] = serial;
            serial += 1;
            active.push(root);
            dfs.push(root);
            while let Some(&node) = dfs.last() {
                work.dfs += 1;
                control.tick()?;
                let neighbors = self.successors(node);
                if next_arc[node] < neighbors.len() {
                    work.edges += 1;
                    control.tick()?;
                    let to = neighbors[next_arc[node]];
                    next_arc[node] += 1;
                    if index[to] == NONE {
                        index[to] = serial;
                        low[to] = serial;
                        serial += 1;
                        active.push(to);
                        dfs.push(to);
                    } else if component_of[to] == NONE {
                        low[node] = low[node].min(index[to]);
                    }
                } else {
                    dfs.pop();
                    if low[node] == index[node] {
                        let component = offsets.len() - 1;
                        loop {
                            work.members += 1;
                            control.tick()?;
                            let member = active.pop().expect("Tarjan root is on active stack");
                            component_of[member] = component;
                            members.push(member);
                            if member == node {
                                break;
                            }
                        }
                        offsets.push(members.len());
                    }
                    if let Some(&parent) = dfs.last() {
                        low[parent] = low[parent].min(low[node]);
                    }
                }
            }
        }
        control.flush()?;
        Ok(Components {
            component_of,
            members,
            offsets,
            work,
            peak_heap_bytes: peak,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Components {
    pub component_of: Vec<usize>,
    members: Vec<usize>,
    offsets: Vec<usize>,
    pub work: GraphWork,
    pub peak_heap_bytes: u64,
}

impl Components {
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn members(&self, component: usize) -> &[usize] {
        &self.members[self.offsets[component]..self.offsets[component + 1]]
    }

    pub fn heap_bytes(&self) -> u64 {
        ((self.component_of.capacity() + self.members.capacity() + self.offsets.capacity())
            * size_of::<usize>()) as u64
    }
}
