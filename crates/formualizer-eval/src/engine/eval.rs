use crate::SheetId;
use crate::arrow_store::SheetStore;
use crate::engine::pass_planner::PassPlanner;
use crate::engine::range_view::RangeView;
use crate::engine::spill::{RegionLockManager, SpillMeta, SpillShape};
use crate::engine::warmup::{PassContext, WarmupExecutor};
use crate::engine::{DependencyGraph, EvalConfig, Scheduler, VertexId, VertexKind};
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord};
use crate::traits::FunctionProvider;
use crate::traits::{EvaluationContext, Resolver};
use chrono::Timelike;
use formualizer_core::parser::ReferenceType;
use formualizer_core::{ASTNode, ExcelError, ExcelErrorKind, LiteralValue};
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Engine<R> {
    pub graph: DependencyGraph,
    resolver: R,
    pub config: EvalConfig,
    thread_pool: Option<Arc<rayon::ThreadPool>>,
    pub recalc_epoch: u64,
    snapshot_id: std::sync::atomic::AtomicU64,
    spill_mgr: ShimSpillManager,
    /// Optional pass-scoped warmup context (flats/masks) available during evaluation
    warmup_pass_ctx: Option<PassContext>,
    /// Arrow-backed storage for sheet values (Phase A)
    arrow_sheets: SheetStore,
    /// True if any edit after bulk load; disables Arrow reads for parity
    has_edited: bool,
    /// Overlay compaction counter (Phase C instrumentation)
    overlay_compactions: u64,
}

#[derive(Debug)]
pub struct EvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: std::time::Duration,
}

#[derive(Debug, Clone)]
pub struct LayerInfo {
    pub vertex_count: usize,
    pub parallel_eligible: bool,
    pub sample_cells: Vec<String>, // Sample of up to 5 cell addresses
}

#[derive(Debug, Clone)]
pub struct EvalPlan {
    pub total_vertices_to_evaluate: usize,
    pub layers: Vec<LayerInfo>,
    pub cycles_detected: usize,
    pub dirty_count: usize,
    pub volatile_count: usize,
    pub parallel_enabled: bool,
    pub estimated_parallel_layers: usize,
    pub target_cells: Vec<String>,
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    pub fn new(resolver: R, config: EvalConfig) -> Self {
        crate::builtins::load_builtins();

        // Initialize thread pool based on config
        let thread_pool = if config.enable_parallel {
            let mut builder = ThreadPoolBuilder::new();
            if let Some(max_threads) = config.max_threads {
                builder = builder.num_threads(max_threads);
            }

            match builder.build() {
                Ok(pool) => Some(Arc::new(pool)),
                Err(_) => {
                    // Fall back to sequential evaluation if thread pool creation fails
                    None
                }
            }
        } else {
            None
        };

        Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            thread_pool,
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            warmup_pass_ctx: None,
            arrow_sheets: SheetStore::default(),
            has_edited: false,
            overlay_compactions: 0,
        }
    }

    /// Create an Engine with a custom thread pool (for shared thread pool scenarios)
    pub fn with_thread_pool(
        resolver: R,
        config: EvalConfig,
        thread_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        crate::builtins::load_builtins();
        Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            thread_pool: Some(thread_pool),
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            warmup_pass_ctx: None,
            arrow_sheets: SheetStore::default(),
            has_edited: false,
            overlay_compactions: 0,
        }
    }

    pub fn default_sheet_id(&self) -> SheetId {
        self.graph.default_sheet_id()
    }

    pub fn default_sheet_name(&self) -> &str {
        self.graph.default_sheet_name()
    }

    /// Update the workbook seed for deterministic RNGs in functions.
    pub fn set_workbook_seed(&mut self, seed: u64) {
        self.config.workbook_seed = seed;
    }

    /// Set the volatile level policy (Always/OnRecalc/OnOpen)
    pub fn set_volatile_level(&mut self, level: crate::traits::VolatileLevel) {
        self.config.volatile_level = level;
    }

    pub fn sheet_id(&self, name: &str) -> Option<SheetId> {
        self.graph.sheet_id(name)
    }

    pub fn set_default_sheet_by_name(&mut self, name: &str) {
        self.graph.set_default_sheet_by_name(name);
    }

    pub fn set_default_sheet_by_id(&mut self, id: SheetId) {
        self.graph.set_default_sheet_by_id(id);
    }

    pub fn set_sheet_index_mode(&mut self, mode: crate::engine::SheetIndexMode) {
        self.graph.set_sheet_index_mode(mode);
    }

    /// Mark data edited: bump snapshot and set edited flag
    pub fn mark_data_edited(&mut self) {
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
    }

    /// Access Arrow sheet store (read-only)
    pub fn sheet_store(&self) -> &SheetStore {
        &self.arrow_sheets
    }

    /// Access Arrow sheet store (mutable)
    pub fn sheet_store_mut(&mut self) -> &mut SheetStore {
        &mut self.arrow_sheets
    }

    /// Begin bulk Arrow ingest for base values (Phase A)
    pub fn begin_bulk_ingest_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkIngestBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkIngestBuilder::new(self)
    }

    /// Begin bulk updates to Arrow store (Phase C)
    pub fn begin_bulk_update_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkUpdateBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkUpdateBuilder::new(self)
    }

    /// Insert rows (1-based) and mirror into Arrow store when enabled
    pub fn insert_rows(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.insert_rows(sheet_id, before, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before.saturating_sub(1) as usize;
            asheet.insert_rows(before0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Delete rows (1-based) and mirror into Arrow store when enabled
    pub fn delete_rows(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.delete_rows(sheet_id, start, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start.saturating_sub(1) as usize;
            asheet.delete_rows(start0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Insert columns (1-based) and mirror into Arrow store when enabled
    pub fn insert_columns(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.insert_columns(sheet_id, before, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before.saturating_sub(1) as usize;
            asheet.insert_columns(before0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Delete columns (1-based) and mirror into Arrow store when enabled
    pub fn delete_columns(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.delete_columns(sheet_id, start, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start.saturating_sub(1) as usize;
            asheet.delete_columns(start0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }
    /// Arrow-backed used row bounds across a column span (1-based inclusive cols).
    fn arrow_used_row_bounds(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sc0 = start_col.saturating_sub(1) as usize;
        let ec0 = end_col.saturating_sub(1) as usize;
        let col_hi = a.columns.len().saturating_sub(1);
        if sc0 > col_hi {
            return None;
        }
        let ec0 = ec0.min(col_hi);
        // Scan for first non-empty row across requested columns (consider overlay)
        let mut min_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let col = &a.columns[ci];
            for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
                let tags = chunk.type_tag.values();
                for (off, &t) in tags.iter().enumerate() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let row0 = a.chunk_starts[chunk_idx] + off;
                        min_r0 = Some(min_r0.map(|m| m.min(row0)).unwrap_or(row0));
                        break;
                    }
                }
                if min_r0.is_some() {
                    break;
                }
            }
        }
        if min_r0.is_none() {
            return None;
        }
        // Scan for last non-empty row across requested columns (consider overlay)
        let mut max_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let col = &a.columns[ci];
            for (chunk_rel, chunk) in col.chunks.iter().enumerate().rev() {
                let chunk_idx = chunk_rel; // same index
                let tags = chunk.type_tag.values();
                for (rev_idx, &t) in tags.iter().enumerate().rev() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(rev_idx)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let row0 = a.chunk_starts[chunk_idx] + rev_idx;
                        max_r0 = Some(max_r0.map(|m| m.max(row0)).unwrap_or(row0));
                        break;
                    }
                }
                if max_r0.is_some() {
                    break;
                }
            }
        }
        match (min_r0, max_r0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    /// Arrow-backed used column bounds across a row span (1-based inclusive rows).
    fn arrow_used_col_bounds(
        &self,
        sheet: &str,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sr0 = start_row.saturating_sub(1) as usize;
        let er0 = end_row.saturating_sub(1) as usize;
        if sr0 > er0 {
            return None;
        }
        // Map start/end rows into chunk ranges
        // We will scan each column for any non-empty within [sr0..=er0]
        let mut min_c0: Option<usize> = None;
        let mut max_c0: Option<usize> = None;
        // Precompute chunk bounds for row range
        for (ci, col) in a.columns.iter().enumerate() {
            // Skip columns with no chunks (shouldn't happen)
            if col.chunks.is_empty() {
                continue;
            }
            let mut any_in_range = false;
            for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
                let chunk_start = a.chunk_starts[chunk_idx];
                let chunk_len = chunk.type_tag.len();
                let chunk_end = chunk_start + chunk_len.saturating_sub(1);
                // check intersection
                if sr0 > chunk_end || er0 < chunk_start {
                    continue;
                }
                let start_off = sr0.max(chunk_start) - chunk_start;
                let end_off = er0.min(chunk_end) - chunk_start;
                let tags = chunk.type_tag.values();
                for off in start_off..=end_off {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false);
                    if overlay_non_empty || tags[off] != crate::arrow_store::TypeTag::Empty as u8 {
                        any_in_range = true;
                        break;
                    }
                }
                if any_in_range {
                    break;
                }
            }
            if any_in_range {
                min_c0 = Some(min_c0.map(|m| m.min(ci)).unwrap_or(ci));
                max_c0 = Some(max_c0.map(|m| m.max(ci)).unwrap_or(ci));
            }
        }
        match (min_c0, max_c0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    /// Set a cell value
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        self.graph.set_cell_value(sheet, row, col, value.clone())?;
        // Mirror into Arrow overlay when enabled
        if self.config.arrow_storage_enabled && self.config.delta_overlay_enabled {
            if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
                let row0 = row.saturating_sub(1) as usize;
                let col0 = col.saturating_sub(1) as usize;
                if col0 < asheet.columns.len() {
                    if row0 >= asheet.nrows as usize {
                        asheet.ensure_row_capacity(row0 + 1);
                    }
                    if let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) {
                        let colref = &mut asheet.columns[col0];
                        let ch = &mut colref.chunks[ch_idx];
                        let ov = match value {
                            LiteralValue::Empty => crate::arrow_store::OverlayValue::Empty,
                            LiteralValue::Int(i) => {
                                crate::arrow_store::OverlayValue::Number(i as f64)
                            }
                            LiteralValue::Number(n) => crate::arrow_store::OverlayValue::Number(n),
                            LiteralValue::Boolean(b) => {
                                crate::arrow_store::OverlayValue::Boolean(b)
                            }
                            LiteralValue::Text(ref s) => {
                                crate::arrow_store::OverlayValue::Text(Arc::from(s.clone()))
                            }
                            LiteralValue::Error(ref e) => crate::arrow_store::OverlayValue::Error(
                                crate::arrow_store::map_error_code(e.kind),
                            ),
                            LiteralValue::Date(d) => {
                                let dt = d.and_hms_opt(0, 0, 0).unwrap();
                                let serial = crate::builtins::datetime::datetime_to_serial_for(
                                    self.config.date_system,
                                    &dt,
                                );
                                crate::arrow_store::OverlayValue::Number(serial)
                            }
                            LiteralValue::DateTime(dt) => {
                                let serial = crate::builtins::datetime::datetime_to_serial_for(
                                    self.config.date_system,
                                    &dt,
                                );
                                crate::arrow_store::OverlayValue::Number(serial)
                            }
                            LiteralValue::Time(t) => {
                                let serial = t.num_seconds_from_midnight() as f64 / 86_400.0;
                                crate::arrow_store::OverlayValue::Number(serial)
                            }
                            LiteralValue::Duration(d) => {
                                let serial = d.num_seconds() as f64 / 86_400.0;
                                crate::arrow_store::OverlayValue::Number(serial)
                            }
                            LiteralValue::Pending => crate::arrow_store::OverlayValue::Pending,
                            LiteralValue::Array(_) => crate::arrow_store::OverlayValue::Error(
                                crate::arrow_store::map_error_code(
                                    formualizer_common::ExcelErrorKind::Value,
                                ),
                            ),
                        };
                        ch.overlay.set(in_off, ov);
                        // Compact if overlay grows dense for this chunk
                        // Heuristic mirrors bulk-update thresholds: > len/50 or > 1024
                        let abs_threshold = 1024usize;
                        let frac_den = 50usize;
                        if asheet.maybe_compact_chunk(col0, ch_idx, abs_threshold, frac_den) {
                            self.overlay_compactions = self.overlay_compactions.saturating_add(1);
                        }
                    }
                }
            }
        }
        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(())
    }

    /// Set a cell formula
    pub fn set_cell_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
    ) -> Result<(), ExcelError> {
        let volatile = self.is_ast_volatile_with_provider(&ast);
        self.graph
            .set_cell_formula_with_volatility(sheet, row, col, ast, volatile)?;
        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(())
    }

    /// Bulk set many formulas on a sheet. Skips per-cell snapshot bumping and minimizes edge rebuilds.
    pub fn bulk_set_formulas<I>(&mut self, sheet: &str, items: I) -> Result<usize, ExcelError>
    where
        I: IntoIterator<Item = (u32, u32, ASTNode)>,
    {
        let n = self.graph.bulk_set_formulas(sheet, items)?;
        // Single snapshot bump after batch
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if n > 0 {
            self.has_edited = true;
        }
        Ok(n)
    }

    /// Get a cell value
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        // Prefer Arrow for non-formula cells. For formula cells, use graph value.
        if let Some(sheet_id) = self.graph.sheet_id(sheet) {
            // If a formula exists at this address, return graph value
            let coord = Coord::new(row, col, true, true);
            let addr = CellRef::new(sheet_id, coord);
            if let Some(vid) = self.graph.get_vertex_id_for_address(&addr) {
                match self.graph.get_vertex_kind(*vid) {
                    VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                        return self.graph.get_cell_value(sheet, row, col);
                    }
                    _ => {}
                }
            }
        }
        if let Some(asheet) = self.sheet_store().sheet(sheet) {
            let r0 = row.saturating_sub(1) as usize;
            let c0 = col.saturating_sub(1) as usize;
            let av = asheet.range_view(r0, c0, r0, c0);
            let v = av.get_cell(0, 0);
            return Some(v);
        }
        self.graph.get_cell_value(sheet, row, col)
    }

    /// Get formula AST (if any) and current stored value for a cell
    pub fn get_cell(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Option<(Option<formualizer_core::ASTNode>, Option<LiteralValue>)> {
        let v = self.get_cell_value(sheet, row, col);
        let sheet_id = self.graph.sheet_id(sheet)?;
        let coord = Coord::new(row, col, true, true);
        let cell = CellRef::new(sheet_id, coord);
        let vid = self.graph.get_vertex_for_cell(&cell)?;
        let ast = self.graph.get_formula(vid);
        Some((ast, v))
    }

    /// Begin batch operations - defer CSR rebuilds for better performance
    pub fn begin_batch(&mut self) {
        self.graph.begin_batch();
    }

    /// End batch operations and trigger CSR rebuild
    pub fn end_batch(&mut self) {
        self.graph.end_batch();
    }

    /// Evaluate a single vertex.
    /// This is the core of the sequential evaluation logic for Milestone 3.1.
    pub fn evaluate_vertex(&mut self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let ast = match kind {
            VertexKind::FormulaScalar => {
                // Get the formula AST
                if let Some(ast) = self.graph.get_formula(vertex_id) {
                    ast.clone()
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                // Check if there's a value stored
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0)); // Empty cells evaluate to 0
                }
            }
            VertexKind::FormulaArray => {
                if let Some(ast) = self.graph.get_formula(vertex_id) {
                    ast.clone()
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::InfiniteRange | VertexKind::Range | VertexKind::External => {
                // Not directly evaluatable here; return stored or 0
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
        };

        // The interpreter uses a reference to the engine as the context.
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
        let result = interpreter.evaluate_ast(&ast);

        // If array result, perform spill from the anchor cell
        match result {
            Ok(LiteralValue::Array(rows)) => {
                // Update kind to FormulaArray for tracking
                self.graph
                    .set_kind(vertex_id, crate::engine::vertex::VertexKind::FormulaArray);
                // Build target cells rectangle starting from anchor
                let anchor = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .expect("cell ref for vertex");
                let sheet_id = anchor.sheet_id;
                let h = rows.len() as u32;
                let w = rows.first().map(|r| r.len()).unwrap_or(0) as u32;
                // Bounds check to avoid out-of-range writes (align to PackedCoord capacity)
                const PACKED_MAX_ROW: u32 = 1_048_575; // 20-bit max
                const PACKED_MAX_COL: u32 = 16_383; // 14-bit max
                let end_row = anchor.coord.row.saturating_add(h).saturating_sub(1);
                let end_col = anchor.coord.col.saturating_add(w).saturating_sub(1);
                if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
                    let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                        .with_message("Spill exceeds sheet bounds")
                        .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                            expected_rows: h,
                            expected_cols: w,
                        });
                    let spill_val = LiteralValue::Error(spill_err.clone());
                    self.graph.update_vertex_value(vertex_id, spill_val.clone());
                    return Ok(spill_val);
                }
                let mut targets = Vec::new();
                for r in 0..h {
                    for c in 0..w {
                        targets.push(self.graph.make_cell_ref_internal(
                            sheet_id,
                            anchor.coord.row + r,
                            anchor.coord.col + c,
                        ));
                    }
                }

                // Plan spill via spill manager shim
                match self.spill_mgr.reserve(
                    vertex_id,
                    anchor,
                    SpillShape { rows: h, cols: w },
                    SpillMeta {
                        epoch: self.recalc_epoch,
                        config: self.config.spill,
                    },
                ) {
                    Ok(()) => {
                        // Commit: write values to grid
                        // Default conflict policy is Error + FirstWins; reserve() enforces in-flight locks
                        // and plan_spill_region enforces overlap with committed formulas/spills/values.
                        if let Err(e) = self.spill_mgr.commit_array(
                            &mut self.graph,
                            vertex_id,
                            &targets,
                            rows.clone(),
                        ) {
                            // If commit fails, mark as error
                            self.graph
                                .update_vertex_value(vertex_id, LiteralValue::Error(e.clone()));
                            return Ok(LiteralValue::Error(e));
                        }
                        // Anchor shows the top-left value, like Excel
                        let top_left = rows
                            .first()
                            .and_then(|r| r.first())
                            .cloned()
                            .unwrap_or(LiteralValue::Empty);
                        self.graph.update_vertex_value(vertex_id, top_left.clone());
                        Ok(top_left)
                    }
                    Err(e) => {
                        let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                            .with_message(e.message.unwrap_or_else(|| "Spill blocked".to_string()))
                            .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                expected_rows: h,
                                expected_cols: w,
                            });
                        let spill_val = LiteralValue::Error(spill_err.clone());
                        self.graph.update_vertex_value(vertex_id, spill_val.clone());
                        Ok(spill_val)
                    }
                }
            }
            Ok(other) => {
                // Scalar result: store value and ensure any previous spill is cleared
                self.graph.clear_spill_region(vertex_id);
                self.graph.update_vertex_value(vertex_id, other.clone());
                // Optionally mirror into Arrow overlay for Arrow-backed reads
                if self.config.arrow_storage_enabled
                    && self.config.delta_overlay_enabled
                    && self.config.write_formula_overlay_enabled
                {
                    let anchor = self
                        .graph
                        .get_cell_ref(vertex_id)
                        .expect("cell ref for vertex");
                    let sheet_name = self.graph.sheet_name(anchor.sheet_id);
                    // Reuse overlay logic from set_cell_value
                    if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet_name) {
                        let row0 = anchor.coord.row.saturating_sub(1) as usize;
                        let col0 = anchor.coord.col.saturating_sub(1) as usize;
                        if col0 < asheet.columns.len() {
                            if row0 >= asheet.nrows as usize {
                                asheet.ensure_row_capacity(row0 + 1);
                            }
                            if let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) {
                                let colref = &mut asheet.columns[col0];
                                let ch = &mut colref.chunks[ch_idx];
                                let ov = match &other {
                                    LiteralValue::Empty => crate::arrow_store::OverlayValue::Empty,
                                    LiteralValue::Int(i) => {
                                        crate::arrow_store::OverlayValue::Number(*i as f64)
                                    }
                                    LiteralValue::Number(n) => {
                                        crate::arrow_store::OverlayValue::Number(*n)
                                    }
                                    LiteralValue::Boolean(b) => {
                                        crate::arrow_store::OverlayValue::Boolean(*b)
                                    }
                                    LiteralValue::Text(s) => {
                                        crate::arrow_store::OverlayValue::Text(Arc::from(s.clone()))
                                    }
                                    LiteralValue::Error(e) => {
                                        crate::arrow_store::OverlayValue::Error(
                                            crate::arrow_store::map_error_code(e.kind),
                                        )
                                    }
                                    LiteralValue::Date(d) => {
                                        let dt = d.and_hms_opt(0, 0, 0).unwrap();
                                        let serial =
                                            crate::builtins::datetime::datetime_to_serial_for(
                                                self.config.date_system,
                                                &dt,
                                            );
                                        crate::arrow_store::OverlayValue::Number(serial)
                                    }
                                    LiteralValue::DateTime(dt) => {
                                        let serial =
                                            crate::builtins::datetime::datetime_to_serial_for(
                                                self.config.date_system,
                                                dt,
                                            );
                                        crate::arrow_store::OverlayValue::Number(serial)
                                    }
                                    LiteralValue::Time(t) => {
                                        let serial =
                                            t.num_seconds_from_midnight() as f64 / 86_400.0;
                                        crate::arrow_store::OverlayValue::Number(serial)
                                    }
                                    LiteralValue::Duration(d) => {
                                        let serial = d.num_seconds() as f64 / 86_400.0;
                                        crate::arrow_store::OverlayValue::Number(serial)
                                    }
                                    LiteralValue::Pending => {
                                        crate::arrow_store::OverlayValue::Pending
                                    }
                                    LiteralValue::Array(_) => {
                                        crate::arrow_store::OverlayValue::Error(
                                            crate::arrow_store::map_error_code(
                                                formualizer_common::ExcelErrorKind::Value,
                                            ),
                                        )
                                    }
                                };
                                ch.overlay.set(in_off, ov);
                            }
                        }
                    }
                }
                Ok(other)
            }
            Err(e) => {
                // Runtime Excel error: store as a cell value instead of propagating
                // as an exception so bulk eval paths don't fail the whole pass.
                self.graph.clear_spill_region(vertex_id);
                let err_val = LiteralValue::Error(e.clone());
                self.graph.update_vertex_value(vertex_id, err_val.clone());
                Ok(err_val)
            }
        }
    }

    /// Evaluate only the necessary precedents for specific target cells (demand-driven)
    pub fn evaluate_until(&mut self, targets: &[&str]) -> Result<EvalResult, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span_eval = tracing::info_span!("evaluate_until", targets = targets.len()).entered();
        let start = std::time::Instant::now();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for target in targets {
            // For now, assume simple A1-style references on default sheet
            // TODO: Parse complex references with sheets
            let (sheet, row, col) = Self::parse_a1_notation(target)?;
            let sheet_id = self.graph.sheet_id_mut(&sheet);
            let coord = Coord::new(row, col, true, true);
            target_addrs.push(CellRef::new(sheet_id, coord));
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Phase 1: Global warmup planning (no-op by default)
        if self.config.warmup.warmup_enabled {
            let mut pass_ctx = PassContext::new(&self.config.warmup);
            let planner = PassPlanner::new(self.config.warmup.clone());

            // Collect ASTs from target vertices for analysis
            let mut target_asts = Vec::new();
            for &vid in &target_vertex_ids {
                if let Some(ast) = self.graph.get_formula(vid) {
                    target_asts.push(ast);
                }
            }

            // Analyze and plan warmup
            let target_refs: Vec<&ASTNode> = target_asts.iter().collect();
            let plan = planner.analyze_targets(&target_refs);

            // Execute warmup
            let executor = WarmupExecutor::new(self.config.warmup.clone());
            let fctx = crate::traits::DefaultFunctionContext::new(self, None);
            let _ = executor.execute(&plan, &mut pass_ctx, &fctx);

            // Store pass context for use during evaluation (read-only)
            self.warmup_pass_ctx = Some(pass_ctx);
        }

        // Build demand subgraph with virtual edges for compressed ranges
        #[cfg(feature = "tracing")]
        let _span_sub = tracing::info_span!("demand_subgraph_build").entered();
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);
        #[cfg(feature = "tracing")]
        drop(_span_sub);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule for the minimal subgraph, honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        #[cfg(feature = "tracing")]
        let _span_sched =
            tracing::info_span!("schedule_build", vertices = precedents_to_eval.len()).entered();
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;
        #[cfg(feature = "tracing")]
        drop(_span_sched);

        // Handle cycles first
        let mut cycle_errors = 0;
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate layers (parallel when enabled, mirroring evaluate_all)
        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(layer)?;
            }
        }

        // Clear warmup context at end of evaluation
        self.warmup_pass_ctx = None;

        // Clear dirty flags for evaluated vertices
        self.graph.clear_dirty_flags(&precedents_to_eval);

        // Re-dirty volatile vertices
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate all dirty/volatile vertices
    pub fn evaluate_all(&mut self) -> Result<EvalResult, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span_eval = tracing::info_span!("evaluate_all").entered();
        let start = std::time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        // Handle cycles first by marking them with #CIRC!
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate acyclic layers (parallel or sequential based on config)
        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(layer)?;
            }
        }

        // Clear dirty flags for all evaluated vertices (including cycles)
        self.graph.clear_dirty_flags(&to_evaluate);

        // Re-dirty volatile vertices for the next evaluation cycle
        self.graph.redirty_volatiles();

        // Advance recalc epoch after a full evaluation pass finishes
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Convenience: demand-driven evaluation of a single cell by sheet name and row/col.
    ///
    /// This will evaluate only the minimal set of dirty / volatile precedents required
    /// to bring the target cell up-to-date (as if a user asked for that single value),
    /// rather than scheduling a full workbook recalc. If the cell is already clean and
    /// non-volatile, no vertices will be recomputed.
    ///
    /// Returns the (possibly newly computed) value stored for the cell afterwards.
    /// Empty cells return None. Errors are surfaced via the Result type.
    pub fn evaluate_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        let addr = format!("{}!{}{}", sheet, Self::col_to_letters(col), row);
        let _ = self.evaluate_until(&[addr.as_str()])?; // ignore detailed EvalResult here
        Ok(self.get_cell_value(sheet, row, col))
    }

    /// Convenience: demand-driven evaluation of multiple cells; accepts a slice of
    /// (sheet, row, col) triples. The union of required dirty / volatile precedents
    /// is computed once and evaluated, which is typically faster than calling
    /// `evaluate_cell` repeatedly for a related set of targets.
    ///
    /// Returns the resulting values for each requested target in the same order.
    pub fn evaluate_cells(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let addresses: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| format!("{}!{}{}", s, Self::col_to_letters(*c), r))
            .collect();
        let addr_refs: Vec<&str> = addresses.iter().map(|s| s.as_str()).collect();
        let _ = self.evaluate_until(&addr_refs)?;
        Ok(targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect())
    }

    /// Get the evaluation plan for target cells without actually evaluating them
    pub fn get_eval_plan(&self, targets: &[(&str, u32, u32)]) -> Result<EvalPlan, ExcelError> {
        if targets.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: Vec::new(),
            });
        }

        // Convert targets to A1 notation for consistency
        let addresses: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| format!("{}!{}{}", s, Self::col_to_letters(*c), r))
            .collect();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            if let Some(sheet_id) = self.graph.sheet_id(sheet) {
                let coord = Coord::new(*row, *col, true, true);
                target_addrs.push(CellRef::new(sheet_id, coord));
            }
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Build demand subgraph with virtual edges (same as evaluate_until)
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Count dirty and volatile vertices
        let mut dirty_count = 0;
        let mut volatile_count = 0;
        for &vertex_id in &precedents_to_eval {
            if self.graph.is_dirty(vertex_id) {
                dirty_count += 1;
            }
            if self.graph.is_volatile(vertex_id) {
                volatile_count += 1;
            }
        }

        // Create schedule for the minimal subgraph honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;

        // Build layer information
        let mut layers = Vec::new();
        let mut estimated_parallel_layers = 0;
        let parallel_enabled = self.config.enable_parallel && self.thread_pool.is_some();

        for layer in &schedule.layers {
            let parallel_eligible = parallel_enabled && layer.vertices.len() > 1;
            if parallel_eligible {
                estimated_parallel_layers += 1;
            }

            // Get sample cell addresses (up to 5)
            let sample_cells: Vec<String> = layer
                .vertices
                .iter()
                .take(5)
                .filter_map(|&vertex_id| {
                    self.graph
                        .get_cell_ref_for_vertex(vertex_id)
                        .map(|cell_ref| {
                            let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                            format!(
                                "{}!{}{}",
                                sheet_name,
                                Self::col_to_letters(cell_ref.coord.col),
                                cell_ref.coord.row
                            )
                        })
                })
                .collect();

            layers.push(LayerInfo {
                vertex_count: layer.vertices.len(),
                parallel_eligible,
                sample_cells,
            });
        }

        Ok(EvalPlan {
            total_vertices_to_evaluate: precedents_to_eval.len(),
            layers,
            cycles_detected: schedule.cycles.len(),
            dirty_count,
            volatile_count,
            parallel_enabled,
            estimated_parallel_layers,
            target_cells: addresses,
        })
    }

    /// Build a demand-driven subgraph for the given targets, including ephemeral edges for
    /// compressed ranges, and returning the set of dirty/volatile precedents and virtual deps.
    fn build_demand_subgraph(
        &self,
        target_vertices: &[VertexId],
    ) -> (
        Vec<VertexId>,
        rustc_hash::FxHashMap<VertexId, Vec<VertexId>>,
    ) {
        #[cfg(feature = "tracing")]
        let _span =
            tracing::info_span!("demand_subgraph", targets = target_vertices.len()).entered();
        use formualizer_core::parser::ReferenceType;
        use rustc_hash::{FxHashMap, FxHashSet};

        let mut to_evaluate: FxHashSet<VertexId> = FxHashSet::default();
        let mut visited: FxHashSet<VertexId> = FxHashSet::default();
        let mut stack: Vec<VertexId> = Vec::new();
        let mut vdeps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default(); // incoming deps per vertex

        for &t in target_vertices {
            stack.push(t);
        }

        while let Some(v) = stack.pop() {
            if !visited.insert(v) {
                continue;
            }
            if !self.graph.vertex_exists(v) {
                continue;
            }
            // Only schedule dirty/volatile formulas
            match self.graph.get_vertex_kind(v) {
                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                    if self.graph.is_dirty(v) || self.graph.is_volatile(v) {
                        to_evaluate.insert(v);
                    }
                }
                _ => {}
            }

            // Explicit dependencies (graph edges)
            for dep in self.graph.get_dependencies(v) {
                if self.graph.vertex_exists(dep) {
                    match self.graph.get_vertex_kind(dep) {
                        VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                            if !visited.contains(&dep) {
                                stack.push(dep);
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Compressed range dependencies → discover formula precedents in used/bounded window
            if let Some(ranges) = self.graph.get_range_dependencies(v) {
                let current_sheet_id = self.graph.get_vertex_sheet_id(v);
                for r in ranges {
                    if let ReferenceType::Range {
                        sheet,
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                    } = r
                    {
                        let sheet_id = sheet
                            .as_ref()
                            .and_then(|name| self.graph.sheet_id(name))
                            .unwrap_or(current_sheet_id);
                        let sheet_name = self.graph.sheet_name(sheet_id);

                        // Infer bounds like resolve_range_view
                        let mut sr = *start_row;
                        let mut sc = *start_col;
                        let mut er = *end_row;
                        let mut ec = *end_col;

                        if sr.is_none() && er.is_none() {
                            let scv = sc.unwrap_or(1);
                            let ecv = ec.unwrap_or(scv);
                            if let Some((min_r, max_r)) =
                                self.graph.used_row_bounds_for_columns(sheet_id, scv, ecv)
                            {
                                sr = Some(min_r);
                                er = Some(max_r);
                            } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                                sr = Some(1);
                                er = Some(max_rows);
                            }
                        }
                        if sc.is_none() && ec.is_none() {
                            let srv = sr.unwrap_or(1);
                            let erv = er.unwrap_or(srv);
                            if let Some((min_c, max_c)) =
                                self.graph.used_col_bounds_for_rows(sheet_id, srv, erv)
                            {
                                sc = Some(min_c);
                                ec = Some(max_c);
                            } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                                sc = Some(1);
                                ec = Some(max_cols);
                            }
                        }
                        if sr.is_some() && er.is_none() {
                            let scv = sc.unwrap_or(1);
                            let ecv = ec.unwrap_or(scv);
                            if let Some((_, max_r)) =
                                self.graph.used_row_bounds_for_columns(sheet_id, scv, ecv)
                            {
                                er = Some(max_r);
                            } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                                er = Some(max_rows);
                            }
                        }
                        if er.is_some() && sr.is_none() {
                            let scv = sc.unwrap_or(1);
                            let ecv = ec.unwrap_or(scv);
                            if let Some((min_r, _)) =
                                self.graph.used_row_bounds_for_columns(sheet_id, scv, ecv)
                            {
                                sr = Some(min_r);
                            } else {
                                sr = Some(1);
                            }
                        }
                        if sc.is_some() && ec.is_none() {
                            let srv = sr.unwrap_or(1);
                            let erv = er.unwrap_or(srv);
                            if let Some((_, max_c)) =
                                self.graph.used_col_bounds_for_rows(sheet_id, srv, erv)
                            {
                                ec = Some(max_c);
                            } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                                ec = Some(max_cols);
                            }
                        }
                        if ec.is_some() && sc.is_none() {
                            let srv = sr.unwrap_or(1);
                            let erv = er.unwrap_or(srv);
                            if let Some((min_c, _)) =
                                self.graph.used_col_bounds_for_rows(sheet_id, srv, erv)
                            {
                                sc = Some(min_c);
                            } else {
                                sc = Some(1);
                            }
                        }

                        let sr = sr.unwrap_or(1);
                        let sc = sc.unwrap_or(1);
                        let er = er.unwrap_or(sr.saturating_sub(1));
                        let ec = ec.unwrap_or(sc.saturating_sub(1));
                        if er < sr || ec < sc {
                            continue;
                        }

                        if let Some(index) = self.graph.sheet_index(sheet_id) {
                            // enumerate vertices in col range, filter row and kind
                            for u in index.vertices_in_col_range(sc, ec) {
                                let pc = self.graph.vertex_coord(u);
                                let row = pc.row();
                                if row < sr || row > er {
                                    continue;
                                }
                                match self.graph.get_vertex_kind(u) {
                                    VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                                        // only link and schedule if producer is dirty/volatile
                                        if (self.graph.is_dirty(u) || self.graph.is_volatile(u))
                                            && u != v
                                        {
                                            vdeps.entry(v).or_default().push(u);
                                            if !visited.contains(&u) {
                                                stack.push(u);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<VertexId> = to_evaluate.into_iter().collect();
        result.sort_unstable();
        // Dedup virtual deps
        for deps in vdeps.values_mut() {
            deps.sort_unstable();
            deps.dedup();
        }
        (result, vdeps)
    }

    /// Helper: convert 1-based column index to Excel-style letters (1 -> A, 27 -> AA)
    fn col_to_letters(mut col: u32) -> String {
        let mut s = String::new();
        while col > 0 {
            let rem = ((col - 1) % 26) as u8;
            s.push((b'A' + rem) as char);
            col = (col - 1) / 26;
        }
        s.chars().rev().collect()
    }

    /// Evaluate all dirty/volatile vertices with cancellation support
    pub fn evaluate_all_cancellable(
        &mut self,
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        // Handle cycles first by marking them with #CIRC!
        for cycle in &schedule.cycles {
            // Check cancellation between cycles
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled during cycle handling".to_string()));
            }

            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate acyclic layers sequentially with cancellation checks
        for layer in &schedule.layers {
            // Check cancellation between layers
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled between layers".to_string()));
            }

            // Evaluate vertices in this layer (parallel or sequential)
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices +=
                    self.evaluate_layer_parallel_cancellable(layer, cancel_flag)?;
            } else {
                computed_vertices +=
                    self.evaluate_layer_sequential_cancellable(layer, cancel_flag)?;
            }
        }

        // Clear dirty flags for all evaluated vertices (including cycles)
        self.graph.clear_dirty_flags(&to_evaluate);

        // Re-dirty volatile vertices for the next evaluation cycle
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate only the necessary precedents for specific target cells with cancellation support
    pub fn evaluate_until_cancellable(
        &mut self,
        targets: &[&str],
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for target in targets {
            let (sheet, row, col) = Self::parse_a1_notation(target)?;
            let sheet_id = self.graph.sheet_id_mut(&sheet);
            let coord = Coord::new(row, col, true, true);
            target_addrs.push(CellRef::new(sheet_id, coord));
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Build demand subgraph with virtual edges
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;

        // Handle cycles first
        let mut cycle_errors = 0;
        for cycle in &schedule.cycles {
            // Check cancellation between cycles
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                    "Demand-driven evaluation cancelled during cycle handling".to_string(),
                ));
            }

            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate layers with cancellation checks
        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            // Check cancellation between layers
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                    "Demand-driven evaluation cancelled between layers".to_string(),
                ));
            }

            // Evaluate vertices in this layer (parallel or sequential)
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices +=
                    self.evaluate_layer_parallel_cancellable(layer, cancel_flag)?;
            } else {
                computed_vertices +=
                    self.evaluate_layer_sequential_cancellable_demand_driven(layer, cancel_flag)?;
            }
        }

        // Clear dirty flags for evaluated vertices
        self.graph.clear_dirty_flags(&precedents_to_eval);

        // Re-dirty volatile vertices
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    fn parse_a1_notation(address: &str) -> Result<(String, u32, u32), ExcelError> {
        let parts: Vec<&str> = address.split('!').collect();
        let (sheet, cell_part) = if parts.len() == 2 {
            (parts[0].to_string(), parts[1])
        } else {
            ("Sheet1".to_string(), address) // Assume default sheet if not specified
        };

        let mut col_end = 0;
        for (i, c) in cell_part.chars().enumerate() {
            if c.is_alphabetic() {
                col_end = i + 1;
            } else {
                break;
            }
        }

        let col_str = &cell_part[..col_end];
        let row_str = &cell_part[col_end..];

        let row = row_str.parse::<u32>().map_err(|_| {
            ExcelError::new(ExcelErrorKind::Ref).with_message(format!("Invalid row: {row_str}"))
        })?;

        let mut col = 0;
        for c in col_str.to_uppercase().chars() {
            col = col * 26 + (c as u32 - 'A' as u32) + 1; // +1 for 1-based indexing
        }

        Ok((sheet, row, col))
    }

    /// Determine volatility using this engine's FunctionProvider, falling back to global registry.
    fn is_ast_volatile_with_provider(&self, ast: &ASTNode) -> bool {
        use formualizer_core::parser::ASTNodeType;
        match &ast.node_type {
            ASTNodeType::Function { name, args, .. } => {
                if let Some(func) = self
                    .get_function("", name)
                    .or_else(|| crate::function_registry::get("", name))
                {
                    if func.caps().contains(crate::function::FnCaps::VOLATILE) {
                        return true;
                    }
                }
                args.iter()
                    .any(|arg| self.is_ast_volatile_with_provider(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.is_ast_volatile_with_provider(left)
                    || self.is_ast_volatile_with_provider(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.is_ast_volatile_with_provider(expr),
            ASTNodeType::Array(rows) => rows.iter().any(|row| {
                row.iter()
                    .any(|cell| self.is_ast_volatile_with_provider(cell))
            }),
            _ => false,
        }
    }

    /// Find dirty precedents that need evaluation for the given target vertices
    fn find_dirty_precedents(&self, target_vertices: &[VertexId]) -> Vec<VertexId> {
        use rustc_hash::FxHashSet;

        let mut to_evaluate = FxHashSet::default();
        let mut visited = FxHashSet::default();
        let mut stack = Vec::new();

        // Start reverse traversal from target vertices
        for &target in target_vertices {
            stack.push(target);
        }

        while let Some(vertex_id) = stack.pop() {
            if !visited.insert(vertex_id) {
                continue; // Already processed
            }

            if self.graph.vertex_exists(vertex_id) {
                // Check if this vertex needs evaluation
                let kind = self.graph.get_vertex_kind(vertex_id);
                let needs_eval = match kind {
                    super::vertex::VertexKind::FormulaScalar
                    | super::vertex::VertexKind::FormulaArray => {
                        self.graph.is_dirty(vertex_id) || self.graph.is_volatile(vertex_id)
                    }
                    _ => false, // Values and empty cells don't need evaluation
                };

                if needs_eval {
                    to_evaluate.insert(vertex_id);
                }

                // Continue traversal to dependencies (precedents)
                let dependencies = self.graph.get_dependencies(vertex_id);
                for &dep_id in &dependencies {
                    if !visited.contains(&dep_id) {
                        stack.push(dep_id);
                    }
                }
            }
        }

        let mut result: Vec<VertexId> = to_evaluate.into_iter().collect();
        result.sort_unstable();
        result
    }

    /// Evaluate a layer sequentially
    fn evaluate_layer_sequential(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        for &vertex_id in &layer.vertices {
            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with cancellation support
    fn evaluate_layer_sequential_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            // Check cancellation every 256 vertices to balance responsiveness with performance
            if i % 256 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled within layer".to_string()));
            }

            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with more frequent cancellation checks for demand-driven evaluation
    fn evaluate_layer_sequential_cancellable_demand_driven(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            // Check cancellation more frequently for demand-driven evaluation (every 128 vertices)
            if i % 128 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Demand-driven evaluation cancelled within layer".to_string()));
            }

            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer in parallel using the thread pool
    fn evaluate_layer_parallel(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap();

        // Collect all evaluation results first, then update the graph sequentially
        let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
            thread_pool.install(|| {
                layer
                    .vertices
                    .par_iter()
                    .map(
                        |&vertex_id| match self.evaluate_vertex_immutable(vertex_id) {
                            Ok(v) => Ok((vertex_id, v)),
                            Err(e) => Ok((vertex_id, LiteralValue::Error(e))),
                        },
                    )
                    .collect()
            });

        // Update the graph with results sequentially (thread-safe)
        match results {
            Ok(vertex_results) => {
                for (vertex_id, result) in vertex_results {
                    self.graph.update_vertex_value(vertex_id, result);
                }
                Ok(layer.vertices.len())
            }
            Err(e) => Err(e),
        }
    }

    /// Evaluate a layer in parallel with cancellation support
    fn evaluate_layer_parallel_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap();

        // Check cancellation before starting parallel work
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Parallel evaluation cancelled before starting".to_string()));
        }

        // Collect all evaluation results first, then update the graph sequentially
        let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
            thread_pool.install(|| {
                layer
                    .vertices
                    .par_iter()
                    .map(|&vertex_id| {
                        // Check cancellation periodically during parallel work
                        if cancel_flag.load(Ordering::Relaxed) {
                            return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                                "Parallel evaluation cancelled during execution".to_string(),
                            ));
                        }
                        match self.evaluate_vertex_immutable(vertex_id) {
                            Ok(v) => Ok((vertex_id, v)),
                            Err(e) => Ok((vertex_id, LiteralValue::Error(e))),
                        }
                    })
                    .collect()
            });

        // Update the graph with results sequentially (thread-safe)
        match results {
            Ok(vertex_results) => {
                for (vertex_id, result) in vertex_results {
                    self.graph.update_vertex_value(vertex_id, result);
                }
                Ok(layer.vertices.len())
            }
            Err(e) => Err(e),
        }
    }

    /// Evaluate a single vertex without mutating the graph (for parallel evaluation)
    fn evaluate_vertex_immutable(&self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let ast = match kind {
            VertexKind::FormulaScalar => {
                // Get the formula AST
                if let Some(ast) = self.graph.get_formula(vertex_id) {
                    ast.clone()
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                // Check if there's a value stored
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0)); // Empty cells evaluate to 0
                }
            }
            _ => {
                return Ok(LiteralValue::Error(
                    ExcelError::new(formualizer_common::ExcelErrorKind::Na)
                        .with_message("Array formulas not yet supported".to_string()),
                ));
            }
        };

        // The interpreter uses a reference to the engine as the context
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
        interpreter.evaluate_ast(&ast)
    }

    /// Get access to the shared thread pool for parallel evaluation
    pub fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }
}

// Phase 2 shim: in-process spill manager delegating to current graph methods.
#[derive(Default)]
pub struct ShimSpillManager {
    region_locks: RegionLockManager,
    pub(crate) active_locks: rustc_hash::FxHashMap<VertexId, u64>,
}

impl ShimSpillManager {
    pub(crate) fn reserve(
        &mut self,
        owner: VertexId,
        anchor_cell: CellRef,
        shape: SpillShape,
        _meta: SpillMeta,
    ) -> Result<(), ExcelError> {
        // Derive region from anchor + shape; enforce in-flight exclusivity only.
        let region = crate::engine::spill::Region {
            sheet_id: anchor_cell.sheet_id as u32,
            row_start: anchor_cell.coord.row,
            row_end: anchor_cell
                .coord
                .row
                .saturating_add(shape.rows)
                .saturating_sub(1),
            col_start: anchor_cell.coord.col,
            col_end: anchor_cell
                .coord
                .col
                .saturating_add(shape.cols)
                .saturating_sub(1),
        };
        match self.region_locks.reserve(region, owner) {
            Ok(id) => {
                if id != 0 {
                    self.active_locks.insert(owner, id);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn commit_array(
        &mut self,
        graph: &mut DependencyGraph,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
    ) -> Result<(), ExcelError> {
        // Re-run plan on concrete targets before committing to respect blockers.
        let plan_res = graph.plan_spill_region(anchor_vertex, targets);
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
        }

        let commit_res = graph.commit_spill_region_atomic_with_fault(
            anchor_vertex,
            targets.to_vec(),
            rows,
            None,
        );
        if let Some(id) = self.active_locks.remove(&anchor_vertex) {
            self.region_locks.release(id);
        }
        commit_res.map(|_| ())
    }
}

// Implement the resolver traits for the Engine.
// This allows the interpreter to resolve references by querying the engine's graph.
impl<R> crate::traits::ReferenceResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        let sheet_name = sheet.unwrap_or("Sheet1"); // FIXME: This should use the current sheet context
        // Prefer engine's unified accessor which consults Arrow store for base values
        // and falls back to graph for formulas and stored values.
        if let Some(v) = self.get_cell_value(sheet_name, row, col) {
            Ok(v)
        } else {
            // Excel semantics: empty cell coerces to 0 in numeric contexts
            Ok(LiteralValue::Int(0))
        }
    }
}

impl<R> crate::traits::RangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn crate::traits::Range>, ExcelError> {
        // For now, delegate range resolution to the external resolver.
        // A future optimization could be to handle this within the graph.
        self.resolver.resolve_range_reference(sheet, sr, sc, er, ec)
    }
}

impl<R> crate::traits::NamedRangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        self.resolver.resolve_named_range_reference(name)
    }
}

impl<R> crate::traits::TableResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_table_reference(
        &self,
        tref: &formualizer_core::parser::TableReference,
    ) -> Result<Box<dyn crate::traits::Table>, ExcelError> {
        self.resolver.resolve_table_reference(tref)
    }
}

// The Engine is a Resolver because it implements the constituent traits.
impl<R> crate::traits::Resolver for Engine<R> where R: EvaluationContext {}

// The Engine provides functions by delegating to its internal resolver.
impl<R> crate::traits::FunctionProvider for Engine<R>
where
    R: EvaluationContext,
{
    fn get_function(
        &self,
        prefix: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        self.resolver.get_function(prefix, name)
    }
}

// Override EvaluationContext to provide thread pool access
impl<R> crate::traits::EvaluationContext for Engine<R>
where
    R: EvaluationContext,
{
    fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }

    fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool> {
        // Engine-wide cancellation is exposed via evaluate_all_cancellable APIs; default None here.
        None
    }

    fn chunk_hint(&self) -> Option<usize> {
        // Use a simple heuristic from configuration (stripe width * height) as a default hint.
        let hint =
            (self.config.stripe_height as usize).saturating_mul(self.config.stripe_width as usize);
        Some(hint.clamp(1024, 1 << 20)) // clamp between 1K and ~1M
    }

    fn volatile_level(&self) -> crate::traits::VolatileLevel {
        self.config.volatile_level
    }

    fn workbook_seed(&self) -> u64 {
        self.config.workbook_seed
    }

    fn recalc_epoch(&self) -> u64 {
        self.recalc_epoch
    }

    fn used_rows_for_columns(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        // Prefer Arrow-backed used-region; fallback to graph if formulas intersect region
        let sheet_id = self.graph.sheet_id(sheet)?;
        let mut arrow_ok = self.sheet_store().sheet(sheet).is_some();
        if arrow_ok {
            if let Some(bounds) = self.arrow_used_row_bounds(sheet, start_col, end_col) {
                return Some(bounds);
            }
        }
        self.graph
            .used_row_bounds_for_columns(sheet_id, start_col, end_col)
    }

    fn used_cols_for_rows(&self, sheet: &str, start_row: u32, end_row: u32) -> Option<(u32, u32)> {
        // Prefer Arrow-backed used-region; fallback to graph if formulas intersect region
        let sheet_id = self.graph.sheet_id(sheet)?;
        let mut arrow_ok = self.sheet_store().sheet(sheet).is_some();
        if arrow_ok {
            if let Some(bounds) = self.arrow_used_col_bounds(sheet, start_row, end_row) {
                return Some(bounds);
            }
        }
        self.graph
            .used_col_bounds_for_rows(sheet_id, start_row, end_row)
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        let _ = self.graph.sheet_id(sheet)?;
        // Excel-like upper bounds; we expose something finite but large.
        // Backends may override with real bounds.
        Some((1_048_576, 16_384)) // 1048576 rows, 16384 cols (XFD)
    }

    fn data_snapshot_id(&self) -> u64 {
        self.snapshot_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn backend_caps(&self) -> crate::traits::BackendCaps {
        crate::traits::BackendCaps {
            streaming: true,
            used_region: true,
            write: false,
            tables: false,
            async_stream: false,
        }
    }

    // Flats removed

    fn arrow_fastpath_enabled(&self) -> bool {
        self.config.arrow_fastpath_enabled
    }

    fn date_system(&self) -> crate::engine::DateSystem {
        self.config.date_system
    }
    /// New: resolve a reference into a RangeView (Phase 2 API)
    fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        match reference {
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                let sheet_id = self
                    .graph
                    .sheet_id(sheet_name)
                    .ok_or(ExcelError::new(ExcelErrorKind::Ref))?;

                let is_unbounded = start_row.is_none()
                    || end_row.is_none()
                    || start_col.is_none()
                    || end_col.is_none();
                let mut sr = *start_row;
                let mut sc = *start_col;
                let mut er = *end_row;
                let mut ec = *end_col;

                if sr.is_none() && er.is_none() {
                    // Full-column reference: anchor at row 1
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    sr = Some(1);
                    if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        er = Some(max_rows);
                    }
                }
                if sc.is_none() && ec.is_none() {
                    // Full-row reference: anchor at column 1
                    let srv = sr.unwrap_or(1);
                    let erv = er.unwrap_or(srv);
                    sc = Some(1);
                    if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                        ec = Some(max_c);
                    } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                        ec = Some(max_cols);
                    }
                }
                if sr.is_some() && er.is_none() {
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        er = Some(max_rows);
                    }
                }
                if er.is_some() && sr.is_none() {
                    // Open start: anchor at row 1
                    sr = Some(1);
                }
                if sc.is_some() && ec.is_none() {
                    let srv = sr.unwrap_or(1);
                    let erv = er.unwrap_or(srv);
                    if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                        ec = Some(max_c);
                    } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                        ec = Some(max_cols);
                    }
                }
                if ec.is_some() && sc.is_none() {
                    // Open start: anchor at column 1
                    sc = Some(1);
                }

                let sr = sr.unwrap_or(1);
                let sc = sc.unwrap_or(1);
                let er = er.unwrap_or(sr.saturating_sub(1));
                let ec = ec.unwrap_or(sc.saturating_sub(1));

                // Prefer a hybrid RangeView when an Arrow sheet exists: pull formula/edited cell values
                // from the graph and base values from Arrow. This preserves correctness across mixed
                // columns instead of falling back wholly to graph (which would miss base values).
                if let Some(asheet) = self.sheet_store().sheet(sheet_name) {
                    let sr0 = sr.saturating_sub(1) as usize;
                    let sc0 = sc.saturating_sub(1) as usize;
                    let er0 = er.saturating_sub(1) as usize;
                    let ec0 = ec.saturating_sub(1) as usize;
                    let av = asheet.range_view(sr0, sc0, er0, ec0);
                    return Ok(RangeView::from_hybrid(&self.graph, sheet_id, sr, sc, av));
                }
                Ok(RangeView::from_graph(&self.graph, sheet_id, sr, sc, er, ec))
            }
            ReferenceType::Cell { sheet, row, col } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                // Prefer graph value when present (covers formula results and edited values)
                if let Some(v) = self.graph.get_cell_value(sheet_name, *row, *col) {
                    return Ok(RangeView::from_borrowed(Box::leak(Box::new(vec![vec![v]]))));
                }
                // Fallback to Arrow store for base values when available and storage enabled
                if let Some(asheet) = self.sheet_store().sheet(sheet_name) {
                    let r0 = row.saturating_sub(1) as usize;
                    let c0 = col.saturating_sub(1) as usize;
                    let av = asheet.range_view(r0, c0, r0, c0);
                    let v = av.get_cell(0, 0);
                    return Ok(RangeView::from_borrowed(Box::leak(Box::new(vec![vec![v]]))));
                }
                Ok(RangeView::from_borrowed(Box::leak(Box::new(vec![vec![
                    LiteralValue::Empty,
                ]]))))
            }
            ReferenceType::NamedRange(name) => {
                let data = self.resolver.resolve_named_range_reference(name)?;
                Ok(RangeView::from_borrowed(Box::leak(Box::new(data))))
            }
            ReferenceType::Table(tref) => {
                // Materialize via Resolver::resolve_range_like tranche 1
                let boxed = self.resolve_range_like(&ReferenceType::Table(tref.clone()))?;
                {
                    let owned = boxed.materialise().into_owned();
                    Ok(RangeView::from_borrowed(Box::leak(Box::new(owned))))
                }
            }
        }
    }
}
