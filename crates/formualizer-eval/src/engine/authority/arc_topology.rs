//! ARC topology stage over sorted refined slices and their exact images.
//! Not the complete planner: candidate discovery, classification/affine/cell
//! fallback, levels, cancellation and the engine resource ledger are pending.

use super::arc_emit::{Emission, EmitError, emit};
use super::arc_sweep::{Probe, Slice, Sweep, SweepError, sweep};
use super::plan_graph::{Components, GraphError, PlanGraph};
use super::store::AuthorityError;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TopologyError {
    Authority(AuthorityError),
    Sweep(SweepError),
    Emit(EmitError),
    Graph(GraphError),
}
impl From<AuthorityError> for TopologyError {
    fn from(e: AuthorityError) -> Self {
        Self::Authority(e)
    }
}

#[derive(Debug)]
pub(crate) struct Topology {
    pub sweep: Sweep,
    pub emission: Emission,
    pub graph: PlanGraph,
    pub components: Components,
    pub peak_heap_bytes: u64,
}
impl Topology {
    pub fn heap_bytes(&self) -> u64 {
        self.sweep.heap_bytes()
            + self.emission.heap_bytes()
            + self.graph.heap_bytes()
            + self.components.heap_bytes()
    }
    pub fn total_work(&self) -> u64 {
        self.sweep.work.total()
            + self.emission.work.total()
            + self.graph.work.total()
            + self.components.work.total()
    }
}
fn add(a: u64, b: u64) -> Result<u64, AuthorityError> {
    a.checked_add(b).ok_or(AuthorityError::Alloc)
}
fn remaining(limit: Option<u64>, held: u64) -> Result<Option<u64>, AuthorityError> {
    limit
        .map(|limit| {
            limit.checked_sub(held).ok_or(AuthorityError::Admission {
                resource: "scratch",
                needed: held,
                limit,
            })
        })
        .transpose()
}

/// All coexisting stage outputs are charged, not merely the largest helper
/// peak. Borrowed caller input is excluded from the supplied remaining limit.
/// Pair/hit witnesses stay live for future affine classification. No result
/// (including selfdep/SCCs) escapes on allocation or admission failure.
/// `arc_limit` covers emission/structural charges; `discovery_limit` separately
/// bounds query-column discovery before the quadratic hit arrays can be built.
pub(crate) fn topology(
    slices: &[Slice],
    probes: &[Probe],
    scratch_limit: Option<u64>,
    arc_limit: Option<u64>,
    discovery_limit: Option<u64>,
) -> Result<Topology, TopologyError> {
    let sweep =
        sweep(slices, probes, scratch_limit, discovery_limit).map_err(TopologyError::Sweep)?;
    let mut peak = sweep.peak_heap_bytes;
    let mut held = sweep.heap_bytes();
    let emission = emit(
        slices.len(),
        &sweep.columns,
        &sweep.hits,
        remaining(scratch_limit, held)?,
        arc_limit,
    )
    .map_err(TopologyError::Emit)?;
    peak = peak.max(add(held, emission.peak_heap_bytes)?);
    held = add(held, emission.heap_bytes())?;
    let graph = PlanGraph::build(
        emission.nodes,
        &emission.arcs,
        remaining(scratch_limit, held)?,
    )
    .map_err(TopologyError::Graph)?;
    peak = peak.max(add(held, graph.peak_heap_bytes)?);
    held = add(held, graph.heap_bytes())?;
    let components = graph.components(remaining(scratch_limit, held)?)?;
    peak = peak.max(add(held, components.peak_heap_bytes)?);
    Ok(Topology {
        sweep,
        emission,
        graph,
        components,
        peak_heap_bytes: peak,
    })
}
