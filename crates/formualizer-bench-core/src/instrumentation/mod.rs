use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    time::Instant,
};

use anyhow::Result;
use formualizer_workbook::Workbook;
use serde::Serialize;

pub mod dhat;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct EngineIntrospection {
    pub arena_node_count: Option<u64>,
    pub arena_node_bytes: Option<u64>,
    pub graph_vertex_count: Option<u64>,
    pub graph_edge_count: Option<u64>,
    pub graph_name_count: Option<u64>,
    pub plane_span_count: Option<u64>,
    pub plane_template_count: Option<u64>,
    pub plane_active_span_cells: Option<u64>,
    pub computed_overlay_cells: Option<u64>,
    pub delta_overlay_cells: Option<u64>,
    pub fragments_emitted: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PhaseReport {
    pub phase: String,
    pub edit_cycle: Option<usize>,
    pub edit_kind: Option<String>,
    pub wall_ms: f64,
    pub cpu_ms: f64,
    pub rss_start_mb: Option<f64>,
    pub rss_end_mb: Option<f64>,
    pub rss_peak_phase_mb: Option<f64>,
    pub allocs_count: Option<u64>,
    pub allocs_bytes: Option<u64>,
    pub allocs_max_bytes: Option<u64>,
    pub arena_node_count: Option<u64>,
    pub arena_node_bytes: Option<u64>,
    pub graph_vertex_count: Option<u64>,
    pub graph_edge_count: Option<u64>,
    pub graph_name_count: Option<u64>,
    pub plane_span_count: Option<u64>,
    pub plane_template_count: Option<u64>,
    pub plane_active_span_cells: Option<u64>,
    pub computed_overlay_cells: Option<u64>,
    pub delta_overlay_cells: Option<u64>,
    pub fragments_emitted: Option<u64>,
}

pub struct AllocationCounter {
    start: dhat::AllocationSnapshot,
}

impl AllocationCounter {
    pub fn start() -> Self {
        Self {
            start: dhat::snapshot(),
        }
    }

    pub fn stop(self) -> dhat::AllocationSnapshot {
        dhat::delta(self.start, dhat::snapshot())
    }
}

pub struct PhaseMetrics {
    phase: String,
    edit_cycle: Option<usize>,
    edit_kind: Option<String>,
    start: Instant,
    rss_start_mb: Option<f64>,
    peak_start_mb: Option<f64>,
    allocations: AllocationCounter,
}

impl PhaseMetrics {
    pub fn start(phase: impl Into<String>) -> Self {
        let (rss_start_mb, peak_start_mb) = linux_rss_mb();
        Self {
            phase: phase.into(),
            edit_cycle: None,
            edit_kind: None,
            start: Instant::now(),
            rss_start_mb,
            peak_start_mb,
            allocations: AllocationCounter::start(),
        }
    }

    pub fn with_edit(mut self, cycle: usize, kind: impl Into<String>) -> Self {
        self.edit_cycle = Some(cycle);
        self.edit_kind = Some(kind.into());
        self
    }

    pub fn finish(self, workbook: Option<&Workbook>) -> PhaseReport {
        let wall_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        let (rss_end_mb, peak_end_mb) = linux_rss_mb();
        let rss_peak_phase_mb = match (self.peak_start_mb, peak_end_mb) {
            (Some(start), Some(end)) => Some(end.max(start)),
            (None, Some(end)) => Some(end),
            (Some(start), None) => Some(start),
            (None, None) => None,
        };
        let allocations = self.allocations.stop();
        let engine = workbook.map(introspect_engine).unwrap_or_default();
        PhaseReport {
            phase: self.phase,
            edit_cycle: self.edit_cycle,
            edit_kind: self.edit_kind,
            wall_ms,
            // CPU collection is best-effort in this scaffold; wall clock is the documented fallback.
            cpu_ms: wall_ms,
            rss_start_mb: self.rss_start_mb,
            rss_end_mb,
            rss_peak_phase_mb,
            allocs_count: allocations.allocs_count,
            allocs_bytes: allocations.allocs_bytes,
            allocs_max_bytes: allocations.allocs_max_bytes,
            arena_node_count: engine.arena_node_count,
            arena_node_bytes: engine.arena_node_bytes,
            graph_vertex_count: engine.graph_vertex_count,
            graph_edge_count: engine.graph_edge_count,
            graph_name_count: engine.graph_name_count,
            plane_span_count: engine.plane_span_count,
            plane_template_count: engine.plane_template_count,
            plane_active_span_cells: engine.plane_active_span_cells,
            computed_overlay_cells: engine.computed_overlay_cells,
            delta_overlay_cells: engine.delta_overlay_cells,
            fragments_emitted: engine.fragments_emitted,
        }
    }
}

pub fn introspect_engine(workbook: &Workbook) -> EngineIntrospection {
    let stats = workbook.engine().baseline_stats();
    EngineIntrospection {
        // The existing public baseline stats expose formula AST node count; this is the closest
        // public arena-node counter available to bench-core without adding eval accessors.
        arena_node_count: Some(stats.formula_ast_node_count as u64),
        arena_node_bytes: None,
        graph_vertex_count: Some(stats.graph_vertex_count as u64),
        graph_edge_count: Some(stats.graph_edge_count as u64),
        graph_name_count: None,
        plane_span_count: Some(stats.formula_plane_active_span_count as u64),
        plane_template_count: None,
        plane_active_span_cells: None,
        computed_overlay_cells: None,
        delta_overlay_cells: None,
        fragments_emitted: None,
    }
}

pub fn introspection_notes() -> Vec<String> {
    vec![
        "arena_node_bytes: no public/pub(crate) bench-core-accessible accessor exposed; reported as null".to_string(),
        "graph_name_count: no public/pub(crate) bench-core-accessible accessor exposed; reported as null".to_string(),
        "plane_template_count: no public/pub(crate) bench-core-accessible accessor exposed; reported as null".to_string(),
        "plane_active_span_cells: no public/pub(crate) bench-core-accessible accessor exposed; reported as null".to_string(),
        "computed_overlay_cells: no public/pub(crate) bench-core-accessible accessor exposed; reported as null".to_string(),
        "delta_overlay_cells: no public/pub(crate) bench-core-accessible accessor exposed; reported as null".to_string(),
        "fragments_emitted: no public/pub(crate) bench-core-accessible accessor exposed; reported as null".to_string(),
    ]
}

pub struct Reporter;

impl Reporter {
    pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        serde_json::to_writer_pretty(BufWriter::new(file), value)?;
        Ok(())
    }

    pub fn write_text(path: &Path, text: &str) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = BufWriter::new(File::create(path)?);
        file.write_all(text.as_bytes())?;
        Ok(())
    }
}

pub fn linux_rss_mb() -> (Option<f64>, Option<f64>) {
    let status = std::fs::read_to_string("/proc/self/status").ok();
    let Some(status) = status else {
        return (None, None);
    };
    let mut current = None;
    let mut peak = None;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            current = parse_status_kb(rest).map(|kb| kb as f64 / 1024.0);
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            peak = parse_status_kb(rest).map(|kb| kb as f64 / 1024.0);
        }
    }
    (current, peak)
}

fn parse_status_kb(value: &str) -> Option<u64> {
    value
        .split_whitespace()
        .next()
        .and_then(|token| token.parse::<u64>().ok())
}
