use crate::SheetId;
use crate::engine::graph::DependencyGraph;
use crate::engine::vertex::{VertexId, VertexKind};
use crate::reference::RangeRef;
use formualizer_common::{ExcelError, ExcelErrorKind};

#[inline]
fn normalize_ascii_key(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Native workbook table (Excel ListObject) metadata.
#[derive(Debug, Clone)]
pub struct TableEntry {
    pub name: String,
    pub range: RangeRef,
    pub header_row: bool,
    pub headers: Vec<String>,
    pub totals_row: bool,
    pub vertex: VertexId,
}

impl TableEntry {
    pub fn sheet_id(&self) -> SheetId {
        self.range.start.sheet_id
    }

    pub fn col_index(&self, header: &str) -> Option<usize> {
        self.headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(header))
    }
}

impl DependencyGraph {
    #[inline]
    fn table_lookup_key(&self, name: &str) -> String {
        if self.config.case_sensitive_tables {
            name.to_string()
        } else {
            normalize_ascii_key(name)
        }
    }

    fn canonical_table_name(&self, name: &str) -> Option<String> {
        let key = self.table_lookup_key(name);
        self.tables_lookup.get(&key).cloned()
    }

    pub fn resolve_table_entry(&self, name: &str) -> Option<&TableEntry> {
        if self.config.case_sensitive_tables {
            self.tables.get(name)
        } else {
            let key = self.table_lookup_key(name);
            self.tables_lookup
                .get(&key)
                .and_then(|canon| self.tables.get(canon))
        }
    }

    pub fn table_by_vertex(&self, vertex: VertexId) -> Option<&TableEntry> {
        self.table_vertex_lookup
            .get(&vertex)
            .and_then(|name| self.tables.get(name))
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

        let anchor = range.start;
        let sheet_id = anchor.sheet_id;
        let packed_coord = formualizer_common::Coord::new(anchor.coord.row(), anchor.coord.col());
        let vertex = self.store.allocate(packed_coord, sheet_id, 0x01);
        self.edges.add_vertex(packed_coord, vertex.0);
        self.sheet_index_mut(sheet_id)
            .add_vertex(packed_coord, vertex);
        self.store.set_kind(vertex, VertexKind::Table);

        // Register stripes for the full table region so cell edits inside the table
        // propagate to formulas that depend on the table.
        self.register_table_range_deps(vertex, &range);

        let entry = TableEntry {
            name: name.to_string(),
            range,
            header_row,
            headers,
            totals_row,
            vertex,
        };

        let original = name.to_string();
        self.tables.insert(original.clone(), entry);
        self.tables_lookup
            .insert(self.table_lookup_key(&original), original.clone());
        self.table_vertex_lookup.insert(vertex, original);
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
        self.register_table_range_deps(vertex, &new_range);

        if let Some(existing) = self.tables.get_mut(&canon) {
            existing.range = new_range;
            existing.header_row = header_row;
            existing.headers = headers;
            existing.totals_row = totals_row;
        }

        // Propagate to dependents.
        self.mark_dirty(vertex);
        Ok(())
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
        self.dirty_vertices.remove(&vertex);
        self.volatile_vertices.remove(&vertex);

        Ok(())
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
