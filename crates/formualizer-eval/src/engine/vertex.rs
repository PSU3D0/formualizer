use crate::SheetId;
use formualizer_common::LiteralValue;
use formualizer_core::parser::{ASTNode, ReferenceType};
use std::borrow::Cow;

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

#[repr(u8)]
#[derive(Debug, Clone, PartialEq)]
pub enum VertexKind {
    /// An implicitly created placeholder cell that has not been defined.
    Empty = 0,

    /// Literal value or cached scalar result
    Value(LiteralValue) = 1,

    /// Formula that evaluates to a scalar
    FormulaScalar {
        ast: ASTNode,
        result: Option<LiteralValue>,
        dirty: bool,
        volatile: bool,
    } = 2,

    /// Formula that returns an array (no spill detection yet)
    FormulaArray {
        ast: ASTNode,
        results: Option<Vec<Vec<LiteralValue>>>,
        dims: (usize, usize), // expected rows, cols
        dirty: bool,
        volatile: bool,
    } = 3,

    /// Infinite range placeholder (A:A, 1:1)
    InfiniteRange { reference: ReferenceType } = 4,

    /// Cell reference - used in SoA representation
    Cell = 5,

    /// Range reference - used in SoA representation  
    Range = 6,

    /// External reference - used in SoA representation
    External = 7,
}

impl VertexKind {
    #[inline]
    pub fn from_tag(tag: u8) -> Self {
        match tag {
            0 => VertexKind::Empty,
            5 => VertexKind::Cell,
            6 => VertexKind::Range,
            7 => VertexKind::External,
            // For now, default to Cell for other values
            // In full implementation, this would handle all cases
            _ => VertexKind::Cell,
        }
    }

    #[inline]
    pub fn to_tag(self) -> u8 {
        match self {
            VertexKind::Empty => 0,
            VertexKind::Value(_) => 1,
            VertexKind::FormulaScalar { .. } => 2,
            VertexKind::FormulaArray { .. } => 3,
            VertexKind::InfiniteRange { .. } => 4,
            VertexKind::Cell => 5,
            VertexKind::Range => 6,
            VertexKind::External => 7,
        }
    }
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
            VertexKind::Cell => 5,
            VertexKind::Range => 6,
            VertexKind::External => 7,
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
    pub sheet_id: SheetId,
    pub row: Option<u32>,            // None for ranges/named
    pub col: Option<u32>,            // None for ranges/named
    pub dependencies: Vec<VertexId>, // What this depends on
    pub dependents: Vec<VertexId>,   // What depends on this
}

impl Vertex {
    pub fn new_empty(sheet_id: SheetId, row: Option<u32>, col: Option<u32>) -> Self {
        Self {
            kind: VertexKind::Empty,
            sheet_id,
            row,
            col,
            dependencies: Vec::new(),
            dependents: Vec::new(),
        }
    }

    pub fn new_value(
        sheet_id: SheetId,
        row: Option<u32>,
        col: Option<u32>,
        value: LiteralValue,
    ) -> Self {
        Self {
            kind: VertexKind::Value(value),
            sheet_id,
            row,
            col,
            dependencies: Vec::new(),
            dependents: Vec::new(),
        }
    }

    pub fn new_formula_scalar(
        sheet_id: SheetId,
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
            sheet_id,
            row,
            col,
            dependencies: Vec::new(),
            dependents: Vec::new(),
        }
    }

    /// Returns the cached value of the vertex if available, without cloning.
    pub fn value(&self) -> Cow<LiteralValue> {
        match &self.kind {
            VertexKind::Value(v) => Cow::Borrowed(v),
            VertexKind::FormulaScalar { result, .. } => {
                if let Some(v) = result {
                    Cow::Borrowed(v)
                } else {
                    Cow::Owned(LiteralValue::Empty)
                }
            }
            VertexKind::Empty => Cow::Owned(LiteralValue::Empty),
            _ => Cow::Owned(LiteralValue::Error(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Value,
            ))),
        }
    }

    /// Get metadata for this vertex (🔮 scalability hook)
    pub fn metadata(&self, id: VertexId) -> VertexMetadata {
        VertexMetadata::new(id, &self.kind)
    }
}
