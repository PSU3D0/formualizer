use crate::SheetId;
use crate::engine::graph::DependencyGraph;
use crate::engine::vertex::{VertexId, VertexKind};
use crate::reference::RangeRef;
use formualizer_common::{ExcelError, ExcelErrorKind};

#[inline]
fn normalize_table_key(name: &str) -> String {
    name.to_lowercase()
}

/// Native workbook table (Excel ListObject) metadata.
#[derive(Debug, Clone)]
pub(crate) struct TableEntry {
    pub(crate) name: String,
    pub(crate) range: RangeRef,
    pub(crate) header_row: bool,
    pub(crate) headers: Vec<String>,
    pub(crate) totals_row: bool,
    pub(crate) empty_body: bool,
    pub(crate) vertex: VertexId,
}

impl TableEntry {
    pub fn sheet_id(&self) -> SheetId {
        self.range.start.sheet_id
    }

    pub fn col_index(&self, header: &str) -> Option<usize> {
        // Column names can end in a space, which a parsed reference has lost.
        let header_key = header.trim().to_lowercase();
        self.headers
            .iter()
            .position(|h| h.trim().to_lowercase() == header_key)
    }
}

impl DependencyGraph {
    /// Empty table cells obstruct arrays just as populated table cells do.
    pub(crate) fn table_intersects_spill(
        &self,
        anchor: crate::reference::CellRef,
        end_row: u32,
        end_col: u32,
    ) -> bool {
        self.tables.values().any(|t| {
            t.sheet_id() == anchor.sheet_id
                && t.range.start.coord.row() <= end_row
                && anchor.coord.row() <= t.range.end.coord.row()
                && t.range.start.coord.col() <= end_col
                && anchor.coord.col() <= t.range.end.coord.col()
        })
    }

    #[inline]
    fn table_lookup_key(&self, name: &str) -> String {
        if self.config.case_sensitive_tables {
            name.to_string()
        } else {
            normalize_table_key(name)
        }
    }

    fn canonical_table_name(&self, name: &str) -> Option<String> {
        let key = self.table_lookup_key(name);
        self.tables_lookup.get(&key).cloned()
    }

    pub(crate) fn resolve_table_entry(&self, name: &str) -> Option<&TableEntry> {
        if self.config.case_sensitive_tables {
            self.tables.get(name)
        } else {
            let key = self.table_lookup_key(name);
            self.tables_lookup
                .get(&key)
                .and_then(|canon| self.tables.get(canon))
        }
    }

    pub(crate) fn table_by_vertex(&self, vertex: VertexId) -> Option<&TableEntry> {
        self.table_vertex_lookup
            .get(&vertex)
            .and_then(|name| self.tables.get(name))
    }

    /// Canonical names of every defined table, sorted for deterministic output.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.values().map(|t| t.name.clone()).collect();
        names.sort();
        names
    }

    pub fn define_table(
        &mut self,
        name: &str,
        range: RangeRef,
        header_row: bool,
        headers: Vec<String>,
        totals_row: bool,
    ) -> Result<(), ExcelError> {
        if name.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("Table name cannot be empty".to_string()));
        }

        let key = self.table_lookup_key(name);
        if let Some(existing) = self.tables_lookup.get(&key) {
            return Err(ExcelError::new(ExcelErrorKind::Name).with_message(format!(
                "Table collision under normalization: '{name}' conflicts with '{existing}'"
            )));
        }

        // A table has a range, but the *vertex* that represents the table symbol has no
        // position: it is identified by name. Parking it on the range's anchor cell put it
        // in the sheet index, where grid queries and structural edits could reach it (#304).
        // Dependencies on the table's cells are carried by the stripe registration below.
        let sheet_id = range.start.sheet_id;
        let vertex = self.allocate_symbol_vertex(VertexKind::Table, sheet_id);

        // Register stripes for the table region so cell edits inside the table propagate
        // to formulas that depend on the table. The totals row is left out: its formulas
        // read the table, so it can't also feed the table's symbol without a false cycle.
        let deps_range = Self::table_range_without_totals(&range, totals_row);
        self.register_table_range_deps(vertex, &deps_range);

        let entry = TableEntry {
            name: name.to_string(),
            range,
            header_row,
            headers,
            totals_row,
            empty_body: range.start.coord.row() + 1 + u32::from(header_row)
                > (range.end.coord.row() + 1).saturating_sub(u32::from(totals_row)),
            vertex,
        };

        let original = name.to_string();
        self.tables.insert(original.clone(), entry);
        self.tables_lookup
            .insert(self.table_lookup_key(&original), original.clone());
        self.table_vertex_lookup.insert(vertex, original);
        self.authority_note_symbol(Some(vertex));
        self.bump_symbol_revision();
        self.resolve_pending_symbol("table", name);
        Ok(())
    }

    pub fn update_table(
        &mut self,
        name: &str,
        new_range: RangeRef,
        header_row: bool,
        headers: Vec<String>,
        totals_row: bool,
    ) -> Result<(), ExcelError> {
        let Some(canon) = self.canonical_table_name(name) else {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Unknown table: {name}")));
        };

        let vertex = self.tables.get(&canon).map(|t| t.vertex).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name).with_message(format!("Unknown table: {name}"))
        })?;

        // Replace range deps (cleans old stripes).
        self.remove_dependent_edges(vertex);
        let deps_range = Self::table_range_without_totals(&new_range, totals_row);
        self.register_table_range_deps(vertex, &deps_range);

        if let Some(existing) = self.tables.get_mut(&canon) {
            existing.range = new_range;
            existing.header_row = header_row;
            existing.headers = headers;
            existing.totals_row = totals_row;
            existing.empty_body = new_range.start.coord.row() + 1 + u32::from(header_row)
                > (new_range.end.coord.row() + 1).saturating_sub(u32::from(totals_row));
        }

        // Propagate to dependents.
        self.mark_dirty(vertex);
        self.authority_note_symbol(Some(vertex));
        self.bump_symbol_revision();
        Ok(())
    }

    /// Resize only the row extent, preserving the table identity and columns.
    pub fn resize_table(
        &mut self,
        name: &str,
        new_range: RangeRef,
    ) -> Result<RangeRef, ExcelError> {
        self.resize_table_body(name, new_range, false)
    }

    pub fn resize_table_body(
        &mut self,
        name: &str,
        new_range: RangeRef,
        empty_body: bool,
    ) -> Result<RangeRef, ExcelError> {
        let entry = self.resolve_table_entry(name).cloned().ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name).with_message(format!("Unknown table: {name}"))
        })?;
        if new_range.start != entry.range.start
            || new_range.end.sheet_id != entry.sheet_id()
            || new_range.end.coord.col() != entry.range.end.coord.col()
            || new_range.end.coord.row() < entry.range.start.coord.row()
        {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Table resize must preserve its sheet, start, and columns"));
        }
        let old_range = entry.range;
        if old_range == new_range && entry.empty_body == empty_body {
            return Ok(old_range);
        }
        let sheet_id = entry.sheet_id();
        self.update_table(
            name,
            new_range,
            entry.header_row,
            entry.headers,
            entry.totals_row,
        )?;
        let canon = self.canonical_table_name(name).expect("validated table");
        self.tables
            .get_mut(&canon)
            .expect("validated table")
            .empty_body = empty_body;
        // Also invalidate ordinary cell/range readers in the old/new union.
        self.mark_dirty_rects(&[(
            sheet_id,
            old_range.start.coord.row(),
            old_range.end.coord.row().max(new_range.end.coord.row()),
            old_range.start.coord.col(),
            old_range.end.coord.col(),
        )]);
        Ok(old_range)
    }

    /// Logically empty tables whose reserved body contains this cell.
    pub(crate) fn empty_tables_at(&self, cell: crate::reference::CellRef) -> Vec<TableEntry> {
        self.tables
            .values()
            .filter(|t| {
                t.empty_body
                    && t.sheet_id() == cell.sheet_id
                    && cell.coord.row() >= t.range.start.coord.row() + u32::from(t.header_row)
                    && cell.coord.row()
                        <= t.range
                            .end
                            .coord
                            .row()
                            .saturating_sub(u32::from(t.totals_row))
                    && cell.coord.col() >= t.range.start.coord.col()
                    && cell.coord.col() <= t.range.end.coord.col()
            })
            .cloned()
            .collect()
    }

    pub fn delete_table(&mut self, name: &str) -> Result<(), ExcelError> {
        let Some(canon) = self.canonical_table_name(name) else {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Unknown table: {name}")));
        };

        let Some(entry) = self.tables.remove(&canon) else {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Unknown table: {name}")));
        };

        self.tables_lookup.remove(&self.table_lookup_key(&canon));

        let vertex = entry.vertex;
        self.table_vertex_lookup.remove(&vertex);

        // Clean range deps / stripes.
        self.remove_dependent_edges(vertex);

        // Mark deleted for debuggability; edges already removed.
        self.store.mark_deleted(vertex, true);
        self.vertex_values.remove(&vertex);
        self.vertex_formulas.remove(&vertex);
        self.clear_formula_vertex_dirty(vertex);
        self.volatile_vertices.remove(&vertex);
        self.authority_note_symbol(Some(vertex));
        self.bump_symbol_revision();

        Ok(())
    }

    fn table_range_without_totals(range: &RangeRef, totals_row: bool) -> RangeRef {
        let mut out = *range;
        if totals_row && range.end.coord.row() > range.start.coord.row() {
            out.end.coord = crate::reference::Coord::new(
                range.end.coord.row() - 1,
                range.end.coord.col(),
                range.end.coord.row_abs(),
                range.end.coord.col_abs(),
            );
        }
        out
    }

    fn register_table_range_deps(&mut self, table_vertex: VertexId, range: &RangeRef) {
        use crate::reference::SharedRangeRef;
        use crate::reference::SharedSheetLocator;
        use formualizer_common::AxisBound;

        // Reuse the same range-deps machinery as formulas/names.
        let sheet_loc = SharedSheetLocator::Id(range.start.sheet_id);
        let sr = AxisBound::new(range.start.coord.row(), range.start.coord.row_abs());
        let sc = AxisBound::new(range.start.coord.col(), range.start.coord.col_abs());
        let er = AxisBound::new(range.end.coord.row(), range.end.coord.row_abs());
        let ec = AxisBound::new(range.end.coord.col(), range.end.coord.col_abs());

        if let Ok(r) = SharedRangeRef::from_parts(sheet_loc, Some(sr), Some(sc), Some(er), Some(ec))
        {
            self.add_range_dependent_edges(table_vertex, &[r.into_owned()], range.start.sheet_id);
        }
    }
}
