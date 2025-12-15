use crate::SheetId;
use crate::engine::graph::DependencyGraph;
use crate::engine::vertex::{VertexId, VertexKind};
use formualizer_common::{Coord as AbsCoord, ExcelError, ExcelErrorKind};

#[derive(Debug, Clone)]
pub struct SourceScalarEntry {
    pub name: String,
    pub vertex: VertexId,
    pub version: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SourceTableEntry {
    pub name: String,
    pub vertex: VertexId,
    pub version: Option<u64>,
}

impl DependencyGraph {
    fn next_source_coord(&mut self) -> AbsCoord {
        const COLS: u32 = 16_384;
        const SOURCE_ROW_OFFSET: u32 = 524_288;

        let seq = self.source_vertex_seq;
        self.source_vertex_seq = self.source_vertex_seq.wrapping_add(1);

        let row = (seq / COLS)
            .saturating_add(SOURCE_ROW_OFFSET)
            .min(0x000F_FFFF);
        let col = seq % COLS;
        AbsCoord::new(row, col)
    }

    fn allocate_source_vertex(&mut self) -> VertexId {
        let coord = self.next_source_coord();
        let sheet_id: SheetId = self.default_sheet_id;
        let vertex = self.store.allocate(coord, sheet_id, 0x01);
        self.edges.add_vertex(coord, vertex.0);
        self.store.set_kind(vertex, VertexKind::External);
        vertex
    }

    pub fn resolve_source_scalar_entry(&self, name: &str) -> Option<&SourceScalarEntry> {
        self.source_scalars.get(name)
    }

    pub fn resolve_source_table_entry(&self, name: &str) -> Option<&SourceTableEntry> {
        self.source_tables.get(name)
    }

    pub fn define_source_scalar(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        if name.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("Source name cannot be empty".to_string()));
        }
        if self.source_scalars.contains_key(name) || self.source_tables.contains_key(name) {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Source already defined: {name}")));
        }

        let vertex = self.allocate_source_vertex();
        self.source_vertex_lookup.insert(vertex, name.to_string());
        self.mark_volatile(vertex, version.is_none());

        let entry = SourceScalarEntry {
            name: name.to_string(),
            vertex,
            version,
        };
        self.source_scalars.insert(name.to_string(), entry);
        Ok(())
    }

    pub fn define_source_table(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        if name.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("Source name cannot be empty".to_string()));
        }
        if self.source_tables.contains_key(name) || self.source_scalars.contains_key(name) {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Source already defined: {name}")));
        }

        let vertex = self.allocate_source_vertex();
        self.source_vertex_lookup.insert(vertex, name.to_string());
        self.mark_volatile(vertex, version.is_none());

        let entry = SourceTableEntry {
            name: name.to_string(),
            vertex,
            version,
        };
        self.source_tables.insert(name.to_string(), entry);
        Ok(())
    }

    pub fn set_source_scalar_version(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        let vertex = {
            let entry = self.source_scalars.get_mut(name).ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Unknown source: {name}"))
            })?;

            if entry.version == version {
                return Ok(());
            }

            entry.version = version;
            entry.vertex
        };

        self.mark_volatile(vertex, version.is_none());
        self.mark_dirty(vertex);
        Ok(())
    }

    pub fn set_source_table_version(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        let vertex = {
            let entry = self.source_tables.get_mut(name).ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Unknown source: {name}"))
            })?;

            if entry.version == version {
                return Ok(());
            }

            entry.version = version;
            entry.vertex
        };

        self.mark_volatile(vertex, version.is_none());
        self.mark_dirty(vertex);
        Ok(())
    }

    pub fn invalidate_source(&mut self, name: &str) -> Result<(), ExcelError> {
        if let Some(s) = self.source_scalars.get(name) {
            self.mark_dirty(s.vertex);
            return Ok(());
        }
        if let Some(t) = self.source_tables.get(name) {
            self.mark_dirty(t.vertex);
            return Ok(());
        }
        Err(ExcelError::new(ExcelErrorKind::Name).with_message(format!("Unknown source: {name}")))
    }
}
