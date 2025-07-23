use formualizer_common::LiteralValue;
use formualizer_core::parser::{ASTNode, ReferenceType};

/// 🔮 Scalability Hook: Engine-internal vertex identity (opaque for future sharding support)
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct VertexId(pub(crate) u32);

impl VertexId {
    pub(crate) fn new(id: u32) -> Self {
        Self(id)
    }

    pub(crate) fn as_index(self) -> usize {
        self.0 as usize
    }

    // 🔮 Scalability Hook: Future sharding support
    #[allow(dead_code)]
    fn shard_id(&self) -> u16 {
        (self.0 >> 16) as u16
    }

    #[allow(dead_code)]
    fn local_id(&self) -> u16 {
        self.0 as u16
    }
}

#[derive(Debug, Clone)]
pub enum VertexKind {
    /// An implicitly created placeholder cell that has not been defined.
    Empty,

    /// Literal value or cached scalar result
    Value(LiteralValue),

    /// Formula that evaluates to a scalar
    FormulaScalar {
        ast: ASTNode,
        result: Option<LiteralValue>,
        dirty: bool,
        volatile: bool,
    },

    /// Formula that returns an array (no spill detection yet)
    FormulaArray {
        ast: ASTNode,
        results: Option<Vec<Vec<LiteralValue>>>,
        dims: (usize, usize), // expected rows, cols
        dirty: bool,
        volatile: bool,
    },

    /// Infinite range placeholder (A:A, 1:1)
    InfiniteRange { reference: ReferenceType },
}

/// 🔮 Scalability Hook: Extract metadata for future SoA memory layout
#[derive(Debug, Clone)]
pub struct VertexMetadata {
    pub id: VertexId,
    pub flags: u32, // dirty, volatile, etc (bit flags for future efficiency)
    pub kind_tag: u8,
}

impl VertexMetadata {
    pub fn new(id: VertexId, kind: &VertexKind) -> Self {
        let mut flags = 0u32;
        let kind_tag = match kind {
            VertexKind::Empty => 0,
            VertexKind::Value(_) => 1,
            VertexKind::FormulaScalar {
                dirty, volatile, ..
            } => {
                if *dirty {
                    flags |= 0x01;
                }
                if *volatile {
                    flags |= 0x02;
                }
                2
            }
            VertexKind::FormulaArray {
                dirty, volatile, ..
            } => {
                if *dirty {
                    flags |= 0x01;
                }
                if *volatile {
                    flags |= 0x02;
                }
                3
            }
            VertexKind::InfiniteRange { .. } => 4,
        };

        Self {
            id,
            flags,
            kind_tag,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.flags & 0x01 != 0
    }

    pub fn is_volatile(&self) -> bool {
        self.flags & 0x02 != 0
    }
}

#[derive(Debug, Clone)]
pub struct Vertex {
    pub kind: VertexKind,
    pub sheet: String,
    pub row: Option<u32>,            // None for ranges/named
    pub col: Option<u32>,            // None for ranges/named
    pub dependencies: Vec<VertexId>, // What this depends on
    pub dependents: Vec<VertexId>,   // What depends on this
}

impl Vertex {
    pub fn new_empty(sheet: String, row: Option<u32>, col: Option<u32>) -> Self {
        Self {
            kind: VertexKind::Empty,
            sheet,
            row,
            col,
            dependencies: Vec::new(),
            dependents: Vec::new(),
        }
    }

    pub fn new_value(
        sheet: String,
        row: Option<u32>,
        col: Option<u32>,
        value: LiteralValue,
    ) -> Self {
        Self {
            kind: VertexKind::Value(value),
            sheet,
            row,
            col,
            dependencies: Vec::new(),
            dependents: Vec::new(),
        }
    }

    pub fn new_formula_scalar(
        sheet: String,
        row: Option<u32>,
        col: Option<u32>,
        ast: ASTNode,
        volatile: bool,
    ) -> Self {
        Self {
            kind: VertexKind::FormulaScalar {
                ast,
                result: None,
                dirty: true,
                volatile,
            },
            sheet,
            row,
            col,
            dependencies: Vec::new(),
            dependents: Vec::new(),
        }
    }

    /// Get metadata for this vertex (🔮 scalability hook)
    pub fn metadata(&self, id: VertexId) -> VertexMetadata {
        VertexMetadata::new(id, &self.kind)
    }
}
