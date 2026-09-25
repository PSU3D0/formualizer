//! Probes for gates and benches (feature `unified_authority` only; not part
//! of any public API surface — the feature is never default and is excluded
//! from the public-API snapshot).
//!
//! Cells are `(sheet id, row, col)`, 0-based.

use super::geom::{Cell, Rect};
use super::host::HostState;
use super::store::{AuthorityError, Store, TagFilter};
use crate::engine::Engine;

/// Size and state of an engine's authority.
#[derive(Clone, Debug)]
pub struct Summary {
    pub formulas: u64,
    pub records: u64,
    pub owners: u64,
    pub nodes: u64,
    pub runs: u64,
    pub slot_rows: u64,
    pub edge_groups: usize,
    pub node_groups: usize,
    /// Retained heap (capacity) bytes of the authority.
    pub authority_bytes: u64,
    /// Legacy dependency structures by component.
    pub legacy_bytes: Vec<(&'static str, usize)>,
    pub builds: u64,
    pub incremental_mutations: u64,
}

impl Summary {
    pub fn legacy_total(&self) -> u64 {
        self.legacy_bytes.iter().map(|(_, b)| *b as u64).sum()
    }
}

/// Sync the authority with the engine and summarize it.
pub fn sync<R>(e: &mut Engine<R>) -> Result<Summary, AuthorityError> {
    e.graph.authority()?;
    let host = e.graph.authority_host();
    let s = host.store();
    let c = s.counts();
    Ok(Summary {
        // Symbol nodes are not formula cells.
        formulas: s.formula_count() - host.symbols().len() as u64,
        records: c.records,
        owners: c.owners,
        nodes: c.nodes,
        runs: c.runs,
        slot_rows: c.slot_rows,
        edge_groups: s.edge_group_count(),
        node_groups: s.node_group_count(),
        authority_bytes: c.bytes,
        legacy_bytes: e.graph.legacy_dependency_bytes(),
        builds: host.builds(),
        incremental_mutations: host.incremental_mutations(),
    })
}

/// The host's state (`Failed` carries the typed error).
pub fn state<R>(e: &Engine<R>) -> HostState {
    e.graph.authority_host().state().clone()
}

/// Fold pending legacy CSR deltas into the base (legacy's settled state,
/// as after a first evaluation).
pub fn settle_legacy<R>(e: &mut Engine<R>) {
    e.graph.flush_pending_edge_deltas();
}

/// Authority direct dependents (R-1X) of one cell.
pub fn direct_dependents<R>(e: &mut Engine<R>, cell: Cell) -> Result<Vec<Cell>, AuthorityError> {
    e.graph.authority()?;
    let s = e.graph.authority_host().store();
    Ok(
        crate::engine::graph::DependencyGraph::authority_direct_grid_dependents(
            s,
            cell.0,
            &Rect::cell(cell.1, cell.2),
        )
        .cells(),
    )
}

/// Authority dirty closure (transitive dependents) of `cells`.
pub fn closure<R>(e: &mut Engine<R>, cells: &[Cell]) -> Result<Vec<Cell>, AuthorityError> {
    e.graph.authority()?;
    let s = e.graph.authority_host().store();
    let seeds: Vec<(u16, Rect)> = cells.iter().map(|c| (c.0, Rect::cell(c.1, c.2))).collect();
    Ok(s.dependents(&seeds, TagFilter::All)
        .0
        .cells()
        .into_iter()
        .filter(|c| c.0 != super::geom::SYMBOL_SHEET)
        .collect())
}

/// Authority direct precedent rectangles of one formula cell:
/// `(sheet, r0, c0, r1, c1)`.
pub fn precedents<R>(e: &mut Engine<R>, cell: Cell) -> Result<Vec<(u16, Rect)>, AuthorityError> {
    e.graph.authority()?;
    let s = e.graph.authority_host().store();
    let mut hits = Vec::new();
    s.direct_grid_precedents(cell, TagFilter::All, &mut hits);
    Ok(hits.into_iter().map(|(_, sh, r)| (sh, r)).collect())
}

/// Legacy direct dependents of one cell (the Δ(e) comparator).
pub fn legacy_direct_dependents<R>(e: &Engine<R>, cell: Cell) -> Vec<Cell> {
    e.graph.legacy_direct_dependent_cells(cell)
}

/// Legacy dirty closure of `cells` (the Δ(a) comparator).
pub fn legacy_closure<R>(e: &Engine<R>, cells: &[Cell]) -> Vec<Cell> {
    e.graph.legacy_closure_cells(cells)
}

/// Δ(a) comparator: run legacy's actual dirty propagation from `cells`
/// and return `(legacy dirty formula cells, authority marking of the same
/// propagation)`, both sorted; `None` when no seed has a legacy vertex.
/// Mutates legacy dirty flags and clears the authority's dirty cover.
#[allow(clippy::type_complexity)]
pub fn dirty_pair<R>(
    e: &mut Engine<R>,
    cells: &[Cell],
) -> Result<Option<(Vec<Cell>, Vec<Cell>)>, AuthorityError> {
    use crate::reference::{CellRef, Coord};
    let any = cells.iter().any(|&c| {
        e.graph
            .get_vertex_for_cell(&CellRef::new(c.0, Coord::new(c.1, c.2, true, true)))
            .is_some()
    });
    if !any {
        return Ok(None);
    }
    e.graph.dirty_propagation_pair(cells).map(Some)
}

/// Formula cells the authority holds.
pub fn formula_cells<R>(e: &mut Engine<R>) -> Result<Vec<Cell>, AuthorityError> {
    e.graph.authority()?;
    Ok(e.graph
        .authority_host()
        .store()
        .formula_cells()
        .into_iter()
        .filter(|c| c.0 != super::geom::SYMBOL_SHEET)
        .collect())
}

/// A fresh build from the engine's current formulas (not installed).
pub fn build_fresh<R>(e: &Engine<R>) -> Store {
    Store::build(e.graph.authority_build_input())
}

/// Maintained == rebuild: the maintained store's canonical content equals
/// a fresh build's.
pub fn maintained_equals_rebuild<R>(e: &mut Engine<R>) -> Result<bool, AuthorityError> {
    e.graph.authority()?;
    let fresh = build_fresh(e);
    Ok(e.graph.authority_host().store().digest() == fresh.digest())
}

/// Structural invariants of the maintained store.
pub fn check<R>(e: &mut Engine<R>) -> Result<Result<(), String>, AuthorityError> {
    e.graph.authority()?;
    Ok(e.graph.authority_host().store().check())
}

/// Maintenance statistics of the maintained store.
pub fn stats<R>(e: &Engine<R>) -> super::store::Stats {
    e.graph.authority_host().store().stats.clone()
}

/// One edge group: dependent sheet, whether it is R1, lookup-slot key id
/// (`u32::MAX` for text), projection, record rectangles.
pub type EdgeGroupView = (u16, bool, u32, super::proj::RefProj, Vec<Rect>);

/// Every non-empty edge group (loss reports).
pub fn edge_groups<R>(e: &mut Engine<R>) -> Result<Vec<EdgeGroupView>, AuthorityError> {
    e.graph.authority()?;
    Ok(e.graph
        .authority_host()
        .store()
        .edge_groups()
        .filter(|(_, rects)| !rects.is_empty())
        .map(|(k, rects)| {
            (
                k.dep_sheet,
                k.tag == super::store::Tag::R1,
                k.lk,
                k.proj,
                rects,
            )
        })
        .collect())
}

/// M5 recon timing: plan every formula cell the way a full recalculation
/// does (cover of all formula cells, `planner::prepare` + ordering, then the
/// Schedule adapter). Returns `(cells, cover_ms, prepare_ms, order_ms,
/// adapt_ms, fallback_components, fallback_work, work)`.
#[allow(clippy::type_complexity)]
pub fn plan_timing<R>(
    e: &mut Engine<R>,
) -> Result<(usize, f64, f64, f64, f64, u64, u64, u64), String> {
    use super::{plan_schedule, planner};
    use std::time::Instant;
    let ms = |t: Instant| t.elapsed().as_secs_f64() * 1000.0;
    e.graph.authority().map_err(|x| format!("{x:?}"))?;
    let t = Instant::now();
    let cells = e.graph.authority_host().store().formula_cells();
    let mut cover = super::geom::Cover::new();
    for &(s, r, c) in &cells {
        cover.insert_rect(s, &Rect::cell(r, c));
    }
    let cover_ms = ms(t);
    let store = e.graph.authority_host().store();
    let t = Instant::now();
    let prepared =
        planner::prepare(store, &cover, None, None, None).map_err(|x| format!("{x:?}"))?;
    let prepare_ms = ms(t);
    drop(prepared);
    let t = Instant::now();
    let ordered = planner::plan(store, &cover, None, None, None).map_err(|x| format!("{x:?}"))?;
    let order_ms = ms(t) - prepare_ms;
    let t = Instant::now();
    let adapted = plan_schedule::schedule(
        &ordered.cells,
        ordered.heap_bytes(),
        None,
        |cell| {
            let address = crate::reference::CellRef::new(
                cell.sheet,
                crate::reference::Coord::new(cell.row, cell.col, true, true),
            );
            e.graph
                .get_vertex_id_for_address(&address)
                .copied()
                .ok_or_else(|| {
                    formualizer_common::ExcelError::new(formualizer_common::ExcelErrorKind::Error)
                })
        },
        |_| Ok(()),
    )
    .map_err(|x| format!("{x:?}"))?;
    let adapt_ms = ms(t);
    drop(adapted);
    Ok((
        cells.len(),
        cover_ms,
        prepare_ms,
        order_ms,
        adapt_ms,
        ordered.fallback_components,
        ordered.fallback_work,
        ordered.work,
    ))
}
