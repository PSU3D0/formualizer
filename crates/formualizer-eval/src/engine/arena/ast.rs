/// AST arena with structural sharing and deduplication
/// Stores formula AST nodes efficiently with content-addressable storage
use super::string_interner::{StringId, StringInterner};
use super::value_ref::ValueRef;
use formualizer_parse::parser::{ExternalRefKind, TableSpecifier};
use rustc_hash::FxHashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Reference to an AST node in the arena
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct AstNodeId(u32);

impl AstNodeId {
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for AstNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AstNode({})", self.0)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableSpecId(u32);

impl TableSpecId {
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

/// Compact representation of AST nodes in the arena
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AstNodeData {
    /// Literal value
    Literal(ValueRef),

    /// Cell or range reference
    Reference {
        original_id: StringId,    // Original reference string
        ref_type: CompactRefType, // Compact reference representation
    },

    /// Unary operation
    UnaryOp { op_id: StringId, expr_id: AstNodeId },

    /// Binary operation
    BinaryOp {
        op_id: StringId,
        left_id: AstNodeId,
        right_id: AstNodeId,
    },

    /// Function call
    Function {
        name_id: StringId,
        args_offset: u32, // Index into args array
        args_count: u16,  // Number of arguments
    },

    /// Array literal
    Array {
        rows: u16,
        cols: u16,
        elements_offset: u32, // Index into elements array
    },
}

/// Identifies a sheet either by stable registry id or by unresolved name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SheetKey {
    Id(u16),
    Name(StringId),
}

/// Compact representation of reference types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactRefType {
    Cell {
        sheet: Option<SheetKey>,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    },
    Range {
        sheet: Option<SheetKey>,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    },
    External {
        raw_id: StringId,
        book_id: StringId,
        sheet_id: StringId,
        kind: ExternalRefKind,
    },
    NamedRange(StringId),
    Table {
        name_id: StringId,
        specifier_id: Option<TableSpecId>,
    },
}

/// Arena for storing AST nodes with deduplication
pub struct AstArena {
    /// Node storage
    nodes: Vec<AstNodeData>,

    /// Hash -> node index for deduplication
    dedup_map: FxHashMap<u64, AstNodeId>,

    /// Function arguments storage (flattened)
    function_args: Vec<AstNodeId>,

    /// Array elements storage (flattened)
    array_elements: Vec<AstNodeId>,

    /// String pool for operators and function names
    strings: StringInterner,

    /// Structured table specifiers
    table_specs: Vec<TableSpecifier>,
    table_spec_dedup: FxHashMap<u64, TableSpecId>,

    /// Statistics
    dedup_hits: usize,
}

impl AstArena {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            dedup_map: FxHashMap::default(),
            function_args: Vec::new(),
            array_elements: Vec::new(),
            strings: StringInterner::new(),
            table_specs: Vec::new(),
            table_spec_dedup: FxHashMap::default(),
            dedup_hits: 0,
        }
    }

    pub fn with_capacity(node_cap: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(node_cap),
            dedup_map: FxHashMap::with_capacity_and_hasher(node_cap, Default::default()),
            function_args: Vec::with_capacity(node_cap * 2), // Assume avg 2 args
            array_elements: Vec::with_capacity(node_cap),
            strings: StringInterner::with_capacity(node_cap / 10),
            table_specs: Vec::new(),
            table_spec_dedup: FxHashMap::default(),
            dedup_hits: 0,
        }
    }

    /// Insert a node, deduplicating if it already exists
    pub fn insert(&mut self, node: AstNodeData) -> AstNodeId {
        // Compute hash
        let hash = self.hash_node(&node);

        // Check for existing node
        if let Some(&id) = self.dedup_map.get(&hash) {
            // Verify it's actually the same (handle hash collisions)
            if self.nodes[id.0 as usize] == node {
                self.dedup_hits += 1;
                return id;
            }
        }

        // Add new node
        let id = AstNodeId(self.nodes.len() as u32);
        self.nodes.push(node);
        self.dedup_map.insert(hash, id);
        id
    }

    /// Insert a literal node
    pub fn insert_literal(&mut self, value: ValueRef) -> AstNodeId {
        self.insert(AstNodeData::Literal(value))
    }

    /// Insert a reference node
    pub fn insert_reference(&mut self, original: &str, ref_type: CompactRefType) -> AstNodeId {
        let original_id = self.strings.intern(original);
        self.insert(AstNodeData::Reference {
            original_id,
            ref_type,
        })
    }

    /// Insert a unary operation node
    pub fn insert_unary_op(&mut self, op: &str, expr: AstNodeId) -> AstNodeId {
        let op_id = self.strings.intern(op);
        self.insert(AstNodeData::UnaryOp {
            op_id,
            expr_id: expr,
        })
    }

    /// Insert a binary operation node
    pub fn insert_binary_op(&mut self, op: &str, left: AstNodeId, right: AstNodeId) -> AstNodeId {
        let op_id = self.strings.intern(op);
        self.insert(AstNodeData::BinaryOp {
            op_id,
            left_id: left,
            right_id: right,
        })
    }

    /// Insert a function call node
    pub fn insert_function(&mut self, name: &str, args: Vec<AstNodeId>) -> AstNodeId {
        let name_id = self.strings.intern(name);
        let args_offset = self.function_args.len() as u32;
        let args_count = args.len() as u16;

        self.function_args.extend(args);

        self.insert(AstNodeData::Function {
            name_id,
            args_offset,
            args_count,
        })
    }

    /// Insert an array literal node
    pub fn insert_array(&mut self, rows: u16, cols: u16, elements: Vec<AstNodeId>) -> AstNodeId {
        assert_eq!(
            elements.len(),
            (rows * cols) as usize,
            "Array dimensions don't match element count"
        );

        let elements_offset = self.array_elements.len() as u32;
        self.array_elements.extend(elements);

        self.insert(AstNodeData::Array {
            rows,
            cols,
            elements_offset,
        })
    }

    /// Get a node by ID
    pub fn get(&self, id: AstNodeId) -> Option<&AstNodeData> {
        self.nodes.get(id.0 as usize)
    }

    /// Get function arguments for a function node
    pub fn get_function_args(&self, id: AstNodeId) -> Option<&[AstNodeId]> {
        match self.get(id)? {
            AstNodeData::Function {
                args_offset,
                args_count,
                ..
            } => {
                let start = *args_offset as usize;
                let end = start + *args_count as usize;
                Some(&self.function_args[start..end])
            }
            _ => None,
        }
    }

    /// Get array elements for an array node
    pub fn get_array_elements(&self, id: AstNodeId) -> Option<&[AstNodeId]> {
        match self.get(id)? {
            AstNodeData::Array {
                rows,
                cols,
                elements_offset,
            } => {
                let start = *elements_offset as usize;
                let count = (*rows * *cols) as usize;
                let end = start + count;
                Some(&self.array_elements[start..end])
            }
            _ => None,
        }
    }

    pub fn get_array_elements_info(&self, id: AstNodeId) -> Option<(u16, u16, &[AstNodeId])> {
        match self.get(id)? {
            AstNodeData::Array { rows, cols, .. } => {
                let elements = self.get_array_elements(id)?;
                Some((*rows, *cols, elements))
            }
            _ => None,
        }
    }

    /// Resolve a string ID to its content
    pub fn resolve_string(&self, id: StringId) -> &str {
        self.strings.resolve(id)
    }

    /// Get the string interner (for external use)
    pub fn strings(&self) -> &StringInterner {
        &self.strings
    }

    /// Get mutable access to the string interner
    pub fn strings_mut(&mut self) -> &mut StringInterner {
        &mut self.strings
    }

    pub fn intern_table_specifier(&mut self, specifier: &TableSpecifier) -> TableSpecId {
        let hash = {
            let mut hasher = DefaultHasher::new();
            specifier.hash(&mut hasher);
            hasher.finish()
        };

        if let Some(&id) = self.table_spec_dedup.get(&hash)
            && self
                .table_specs
                .get(id.0 as usize)
                .is_some_and(|existing| existing == specifier)
        {
            return id;
        }

        let id = TableSpecId(self.table_specs.len() as u32);
        self.table_specs.push(specifier.clone());
        self.table_spec_dedup.insert(hash, id);
        id
    }

    pub fn resolve_table_specifier(&self, id: TableSpecId) -> Option<&TableSpecifier> {
        self.table_specs.get(id.0 as usize)
    }

    /// Compute hash for a node
    fn hash_node(&self, node: &AstNodeData) -> u64 {
        let mut hasher = DefaultHasher::new();
        node.hash(&mut hasher);
        hasher.finish()
    }

    /// Get statistics about the arena
    pub fn stats(&self) -> AstArenaStats {
        AstArenaStats {
            node_count: self.nodes.len(),
            dedup_hits: self.dedup_hits,
            string_count: self.strings.len(),
            table_spec_count: self.table_specs.len(),
            total_args: self.function_args.len(),
            total_array_elements: self.array_elements.len(),
        }
    }

    /// Returns memory usage in bytes (approximate)
    pub fn memory_usage(&self) -> usize {
        self.nodes.capacity() * std::mem::size_of::<AstNodeData>()
            + self.dedup_map.capacity() * (8 + 4) // hash + id
            + self.function_args.capacity() * 4
            + self.array_elements.capacity() * 4
            + self.strings.memory_usage()
            + self.table_specs.capacity() * std::mem::size_of::<TableSpecifier>()
            + self.table_spec_dedup.capacity() * (8 + 4)
    }

    /// Clear all nodes from the arena
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.dedup_map.clear();
        self.function_args.clear();
        self.array_elements.clear();
        self.strings.clear();
        self.table_specs.clear();
        self.table_spec_dedup.clear();
        self.dedup_hits = 0;
    }
}

impl Default for AstArena {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for AstArena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AstArena")
            .field("nodes", &self.nodes.len())
            .field("dedup_hits", &self.dedup_hits)
            .field("strings", &self.strings.len())
            .finish()
    }
}

/// Statistics about the AST arena
#[derive(Debug, Clone)]
pub struct AstArenaStats {
    pub node_count: usize,
    pub dedup_hits: usize,
    pub string_count: usize,
    pub table_spec_count: usize,
    pub total_args: usize,
    pub total_array_elements: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ast_arena_literal() {
        let mut arena = AstArena::new();

        let lit1 = arena.insert_literal(ValueRef::small_int(42).unwrap());
        let lit2 = arena.insert_literal(ValueRef::boolean(true));

        assert_ne!(lit1, lit2);

        match arena.get(lit1) {
            Some(AstNodeData::Literal(v)) => {
                assert_eq!(v.as_small_int(), Some(42));
            }
            _ => panic!("Expected literal node"),
        }
    }

    #[test]
    fn test_ast_arena_deduplication() {
        let mut arena = AstArena::new();

        // Insert same literal twice
        let lit1 = arena.insert_literal(ValueRef::small_int(42).unwrap());
        let lit2 = arena.insert_literal(ValueRef::small_int(42).unwrap());

        assert_eq!(lit1, lit2); // Should be deduplicated
        assert_eq!(arena.stats().dedup_hits, 1);
    }

    #[test]
    fn test_ast_arena_binary_op() {
        let mut arena = AstArena::new();

        let left = arena.insert_literal(ValueRef::small_int(1).unwrap());
        let right = arena.insert_literal(ValueRef::small_int(2).unwrap());
        let add = arena.insert_binary_op("+", left, right);

        match arena.get(add) {
            Some(AstNodeData::BinaryOp {
                op_id,
                left_id,
                right_id,
            }) => {
                assert_eq!(arena.resolve_string(*op_id), "+");
                assert_eq!(*left_id, left);
                assert_eq!(*right_id, right);
            }
            _ => panic!("Expected binary op node"),
        }
    }

    #[test]
    fn test_ast_arena_function() {
        let mut arena = AstArena::new();

        let arg1 = arena.insert_literal(ValueRef::small_int(10).unwrap());
        let arg2 = arena.insert_literal(ValueRef::small_int(20).unwrap());
        let arg3 = arena.insert_literal(ValueRef::small_int(30).unwrap());

        let func = arena.insert_function("SUM", vec![arg1, arg2, arg3]);

        match arena.get(func) {
            Some(AstNodeData::Function {
                name_id,
                args_count,
                ..
            }) => {
                assert_eq!(arena.resolve_string(*name_id), "SUM");
                assert_eq!(*args_count, 3);
            }
            _ => panic!("Expected function node"),
        }

        let args = arena.get_function_args(func).unwrap();
        assert_eq!(args, &[arg1, arg2, arg3]);
    }

    #[test]
    fn test_ast_arena_structural_sharing() {
        let mut arena = AstArena::new();

        // Create "A1" reference that will be shared
        let a1_ref = arena.insert_reference(
            "A1",
            CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 1,
                row_abs: false,
                col_abs: false,
            },
        );

        // Create "A1 + 1"
        let one = arena.insert_literal(ValueRef::small_int(1).unwrap());
        let expr1 = arena.insert_binary_op("+", a1_ref, one);

        // Create "A1 * 2"
        let two = arena.insert_literal(ValueRef::small_int(2).unwrap());
        let expr2 = arena.insert_binary_op("*", a1_ref, two);

        // A1 reference should be shared
        assert_eq!(arena.stats().node_count, 5); // A1, 1, +expr, 2, *expr

        // Try to insert A1 again - should be deduplicated
        let a1_ref2 = arena.insert_reference(
            "A1",
            CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 1,
                row_abs: false,
                col_abs: false,
            },
        );
        assert_eq!(a1_ref, a1_ref2);
    }

    #[test]
    fn test_ast_arena_array() {
        let mut arena = AstArena::new();

        let elements = vec![
            arena.insert_literal(ValueRef::small_int(1).unwrap()),
            arena.insert_literal(ValueRef::small_int(2).unwrap()),
            arena.insert_literal(ValueRef::small_int(3).unwrap()),
            arena.insert_literal(ValueRef::small_int(4).unwrap()),
        ];

        let array = arena.insert_array(2, 2, elements.clone());

        match arena.get(array) {
            Some(AstNodeData::Array { rows, cols, .. }) => {
                assert_eq!(*rows, 2);
                assert_eq!(*cols, 2);
            }
            _ => panic!("Expected array node"),
        }

        let stored_elements = arena.get_array_elements(array).unwrap();
        assert_eq!(stored_elements, &elements[..]);
    }

    #[test]
    fn test_ast_arena_complex_expression() {
        let mut arena = AstArena::new();

        // Build: SUM(A1:A10) + IF(B1 > 0, C1, D1)

        // A1:A10 range
        let range = arena.insert_reference(
            "A1:A10",
            CompactRefType::Range {
                sheet: None,
                start_row: 1,
                start_col: 1,
                end_row: 10,
                end_col: 1,
                start_row_abs: false,
                start_col_abs: false,
                end_row_abs: false,
                end_col_abs: false,
            },
        );

        // SUM(A1:A10)
        let sum = arena.insert_function("SUM", vec![range]);

        // B1 reference
        let b1 = arena.insert_reference(
            "B1",
            CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 2,
                row_abs: false,
                col_abs: false,
            },
        );

        // 0 literal
        let zero = arena.insert_literal(ValueRef::small_int(0).unwrap());

        // B1 > 0
        let condition = arena.insert_binary_op(">", b1, zero);

        // C1 and D1 references
        let c1 = arena.insert_reference(
            "C1",
            CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 3,
                row_abs: false,
                col_abs: false,
            },
        );
        let d1 = arena.insert_reference(
            "D1",
            CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 4,
                row_abs: false,
                col_abs: false,
            },
        );

        // IF(B1 > 0, C1, D1)
        let if_expr = arena.insert_function("IF", vec![condition, c1, d1]);

        // Final: SUM(...) + IF(...)
        let final_expr = arena.insert_binary_op("+", sum, if_expr);

        // Verify structure
        assert!(arena.get(final_expr).is_some());
        // Note: zero literal gets deduplicated if used multiple times
        // We have: range, sum, b1, zero, condition(>), c1, d1, if_expr, final_expr(+)
        // That's 9 unique nodes (zero is deduplicated)
        assert_eq!(arena.stats().node_count, 9); // All unique nodes except deduplicated zero
    }

    #[test]
    fn test_ast_arena_string_deduplication() {
        let mut arena = AstArena::new();

        // Use same operator multiple times
        let one = arena.insert_literal(ValueRef::small_int(1).unwrap());
        let two = arena.insert_literal(ValueRef::small_int(2).unwrap());
        let three = arena.insert_literal(ValueRef::small_int(3).unwrap());

        let add1 = arena.insert_binary_op("+", one, two);
        let add2 = arena.insert_binary_op("+", two, three);
        let add3 = arena.insert_binary_op("+", one, three);

        // "+" should be interned only once
        assert_eq!(arena.strings().len(), 1);
    }

    #[test]
    fn test_ast_arena_clear() {
        let mut arena = AstArena::new();

        arena.insert_literal(ValueRef::small_int(1).unwrap());
        arena.insert_literal(ValueRef::small_int(2).unwrap());
        let left = arena.insert_literal(ValueRef::small_int(3).unwrap());
        let right = arena.insert_literal(ValueRef::small_int(4).unwrap());
        arena.insert_binary_op("+", left, right);

        assert_eq!(arena.stats().node_count, 5);

        arena.clear();

        assert_eq!(arena.stats().node_count, 0);
        assert_eq!(arena.strings().len(), 0);
    }
}
