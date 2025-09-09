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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VertexKind {
    /// An implicitly created placeholder cell that has not been defined.
    Empty = 0,

    /// Cell with a literal value (value stored in arena/hashmap)
    Cell = 1,

    /// Formula that evaluates to a scalar (AST stored separately)
    FormulaScalar = 2,

    /// Formula that returns an array (AST stored separately)
    FormulaArray = 3,

    /// Infinite range placeholder (A:A, 1:1)
    InfiniteRange = 4,

    /// Range reference
    Range = 5,

    /// External reference
    External = 6,
}

impl VertexKind {
    #[inline]
    pub fn from_tag(tag: u8) -> Self {
        match tag {
            0 => VertexKind::Empty,
            1 => VertexKind::Cell,
            2 => VertexKind::FormulaScalar,
            3 => VertexKind::FormulaArray,
            4 => VertexKind::InfiniteRange,
            5 => VertexKind::Range,
            6 => VertexKind::External,
            _ => VertexKind::Empty,
        }
    }

    #[inline]
    pub fn to_tag(self) -> u8 {
        self as u8
    }
}
