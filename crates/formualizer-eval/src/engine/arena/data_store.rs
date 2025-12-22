/// Unified data storage for all value types using arenas
/// Provides conversion between LiteralValue and ValueRef
use super::array::ArrayArena;
use super::ast::{AstArena, AstNodeId, CompactRefType, SheetKey};
use super::error_arena::{ErrorArena, ErrorRef};
use super::scalar::ScalarArena;
use super::string_interner::{StringId, StringInterner};
use super::value_ref::ValueRef;
use crate::engine::sheet_registry::SheetRegistry;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{
    ASTNode, ASTNodeType, ExternalBookRef, ExternalReference, ReferenceType, TableReference,
};

/// Centralized data storage using arenas
#[derive(Debug)]
pub struct DataStore {
    /// Scalar values (floats and large integers)
    scalars: ScalarArena,

    /// String values
    strings: StringInterner,

    /// Array values
    arrays: ArrayArena,

    /// AST nodes for formulas
    asts: AstArena,

    /// Error storage with message preservation
    errors: ErrorArena,
}

impl DataStore {
    pub fn new() -> Self {
        Self {
            scalars: ScalarArena::new(),
            strings: StringInterner::new(),
            arrays: ArrayArena::new(),
            asts: AstArena::new(),
            errors: ErrorArena::new(),
        }
    }

    /// Batch store literal values; returns ValueRefs in same order.
    pub fn store_values_batch<I>(&mut self, values: I) -> Vec<ValueRef>
    where
        I: IntoIterator<Item = LiteralValue>,
    {
        let iter = values.into_iter();
        let (lower, _) = iter.size_hint();
        let mut out = Vec::with_capacity(lower);
        for v in iter {
            out.push(self.store_value(v));
        }
        out
    }

    /// Batch store ASTs; returns AstNodeIds in same order.
    pub fn store_asts_batch<'a, I>(
        &mut self,
        asts: I,
        sheet_registry: &SheetRegistry,
    ) -> Vec<AstNodeId>
    where
        I: IntoIterator<Item = &'a ASTNode>,
    {
        let iter = asts.into_iter();
        let (lower, _) = iter.size_hint();
        let mut out = Vec::with_capacity(lower);
        for ast in iter {
            out.push(self.store_ast(ast, sheet_registry));
        }
        out
    }

    pub fn with_capacity(estimated_cells: usize) -> Self {
        Self {
            scalars: ScalarArena::with_capacity(estimated_cells),
            strings: StringInterner::with_capacity(estimated_cells / 10),
            arrays: ArrayArena::with_capacity(estimated_cells / 100),
            asts: AstArena::with_capacity(estimated_cells / 2),
            errors: ErrorArena::with_capacity(estimated_cells / 20),
        }
    }

    /// Store a LiteralValue and return a ValueRef
    pub fn store_value(&mut self, value: LiteralValue) -> ValueRef {
        match value {
            LiteralValue::Empty => ValueRef::empty(),

            LiteralValue::Number(n) => {
                // Store as float in scalar arena
                let idx = self.scalars.insert_float(n);
                ValueRef::number(idx.as_u32())
            }

            LiteralValue::Text(s) => {
                let id = self.strings.intern(&s);
                ValueRef::string(id.as_u32())
            }

            LiteralValue::Boolean(b) => ValueRef::boolean(b),

            LiteralValue::Error(err) => self.store_error(&err),

            LiteralValue::Array(array) => {
                // Convert nested array to ValueRefs
                let rows = array.len() as u32;
                let cols = array.first().map(|r| r.len()).unwrap_or(0) as u32;

                let elements: Vec<ValueRef> = array
                    .into_iter()
                    .flatten()
                    .map(|v| self.store_value(v))
                    .collect();

                let array_ref = self.arrays.insert(rows, cols, elements);
                ValueRef::array(array_ref.as_u32())
            }

            LiteralValue::DateTime(dt) => {
                // Store serial number as float
                let serial = formualizer_common::datetime_to_serial(&dt);
                let idx = self.scalars.insert_float(serial);
                ValueRef::date_time(idx.as_u32())
            }

            LiteralValue::Date(d) => {
                // Convert date to datetime at midnight
                let dt = d.and_hms_opt(0, 0, 0).unwrap();
                let serial = formualizer_common::datetime_to_serial(&dt);
                let idx = self.scalars.insert_float(serial);
                ValueRef::date_time(idx.as_u32())
            }

            LiteralValue::Time(t) => {
                // Store time as fractional day
                use chrono::Timelike;
                let seconds = (t.hour() * 3600 + t.minute() * 60 + t.second()) as f64;
                let fraction = seconds / 86400.0;
                let idx = self.scalars.insert_float(fraction);
                ValueRef::date_time(idx.as_u32())
            }

            LiteralValue::Duration(dur) => {
                // Store as integer seconds (chrono::Duration has num_seconds())
                let secs = dur.num_seconds();
                let idx = self.scalars.insert_integer(secs);
                let raw_index = idx.as_u32() & 0x7FFF_FFFF;
                ValueRef::duration(raw_index)
            }

            LiteralValue::Int(i) => {
                // Try to use small int optimization
                if let Some(vref) = ValueRef::small_int(i as i32) {
                    vref
                } else {
                    // Store as large integer
                    let idx = self.scalars.insert_integer(i);
                    ValueRef::large_int(idx.as_u32())
                }
            }

            LiteralValue::Pending => ValueRef::pending(),
        }
    }

    /// Retrieve a LiteralValue from a ValueRef
    pub fn retrieve_value(&self, value_ref: ValueRef) -> LiteralValue {
        use super::value_ref::ValueType;

        match value_ref.value_type() {
            ValueType::Empty => LiteralValue::Empty,

            ValueType::SmallInt => {
                // Small integers are inlined
                if let Some(i) = value_ref.as_small_int() {
                    LiteralValue::Int(i as i64)
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::LargeInt => {
                if let Some(idx) = value_ref.arena_index() {
                    let scalar_ref = super::scalar::ScalarRef::from_raw(idx | (1 << 31));
                    if let Some(i) = self.scalars.get_integer(scalar_ref) {
                        LiteralValue::Int(i)
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                    }
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::Number => {
                if let Some(idx) = value_ref.arena_index() {
                    let scalar_ref = super::scalar::ScalarRef::from_raw(idx);
                    if let Some(f) = self.scalars.get_float(scalar_ref) {
                        LiteralValue::Number(f)
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                    }
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::String => {
                if let Some(idx) = value_ref.arena_index() {
                    let string_id = StringId::from_raw(idx);
                    let s = self.strings.resolve(string_id);
                    LiteralValue::Text(s.to_string())
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::Boolean => {
                if let Some(b) = value_ref.as_boolean() {
                    LiteralValue::Boolean(b)
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::Error => {
                if let Some(error_ref_raw) = value_ref.as_error_ref() {
                    let error_ref = ErrorRef::from_raw(error_ref_raw);
                    if let Some(error) = self.errors.get(error_ref) {
                        LiteralValue::Error(error)
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                    }
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::Array => {
                if let Some(idx) = value_ref.arena_index() {
                    let array_ref = super::array::ArrayRef::from_raw(idx);
                    if let Some(array_2d) = self.arrays.get_2d(array_ref) {
                        // Convert back to LiteralValue array
                        let result: Vec<Vec<LiteralValue>> = array_2d
                            .into_iter()
                            .map(|row| row.into_iter().map(|v| self.retrieve_value(v)).collect())
                            .collect();
                        LiteralValue::Array(result)
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                    }
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::DateTime => {
                if let Some(idx) = value_ref.arena_index() {
                    let scalar_ref = super::scalar::ScalarRef::from_raw(idx);
                    if let Some(serial) = self.scalars.get_float(scalar_ref) {
                        let dt = formualizer_common::serial_to_datetime(serial);
                        LiteralValue::DateTime(dt)
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                    }
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::Duration => {
                if let Some(idx) = value_ref.arena_index() {
                    let scalar_ref = super::scalar::ScalarRef::from_raw(idx | (1 << 31));
                    if let Some(secs) = self.scalars.get_integer(scalar_ref) {
                        let dur = chrono::Duration::seconds(secs);
                        LiteralValue::Duration(dur)
                    } else {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                    }
                } else {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
                }
            }

            ValueType::Pending => LiteralValue::Pending,

            ValueType::FormulaAst => {
                // Formula ASTs shouldn't be returned as values
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))
            }
        }
    }

    /// Store an AST node and return its ID
    pub fn store_ast(&mut self, ast: &ASTNode, sheet_registry: &SheetRegistry) -> AstNodeId {
        self.convert_ast_node(ast, sheet_registry)
    }

    /// Retrieve an AST node from its ID
    pub fn retrieve_ast(&self, id: AstNodeId, sheet_registry: &SheetRegistry) -> Option<ASTNode> {
        self.reconstruct_ast_node(id, sheet_registry)
    }

    pub fn resolve_ast_string(&self, id: StringId) -> &str {
        self.asts.resolve_string(id)
    }

    pub fn reconstruct_reference_type_for_eval(
        &self,
        ref_type: &CompactRefType,
        sheet_registry: &SheetRegistry,
    ) -> ReferenceType {
        self.reconstruct_reference_type(ref_type, sheet_registry)
    }

    pub fn get_node(&self, id: AstNodeId) -> Option<&super::ast::AstNodeData> {
        self.asts.get(id)
    }

    pub fn get_args(&self, id: AstNodeId) -> Option<&[AstNodeId]> {
        self.asts.get_function_args(id)
    }

    pub fn get_array_elems(&self, id: AstNodeId) -> Option<(u16, u16, &[AstNodeId])> {
        self.asts.get_array_elements_info(id)
    }

    /// Convert ASTNode to arena representation
    fn convert_ast_node(&mut self, node: &ASTNode, sheet_registry: &SheetRegistry) -> AstNodeId {
        match &node.node_type {
            ASTNodeType::Literal(lit) => {
                let value_ref = self.store_value(lit.clone());
                self.asts.insert_literal(value_ref)
            }

            ASTNodeType::Reference {
                original,
                reference,
            } => {
                let ref_type = self.convert_reference_type(reference, sheet_registry);
                self.asts.insert_reference(original, ref_type)
            }

            ASTNodeType::UnaryOp { op, expr } => {
                let expr_id = self.convert_ast_node(expr, sheet_registry);
                self.asts.insert_unary_op(op, expr_id)
            }

            ASTNodeType::BinaryOp { op, left, right } => {
                let left_id = self.convert_ast_node(left, sheet_registry);
                let right_id = self.convert_ast_node(right, sheet_registry);
                self.asts.insert_binary_op(op, left_id, right_id)
            }

            ASTNodeType::Function { name, args } => {
                let arg_ids: Vec<AstNodeId> = args
                    .iter()
                    .map(|arg| self.convert_ast_node(arg, sheet_registry))
                    .collect();
                self.asts.insert_function(name, arg_ids)
            }

            ASTNodeType::Array(rows) => {
                let total_elements = rows.iter().map(|r| r.len()).sum();
                let mut elements = Vec::with_capacity(total_elements);

                let rows_count = rows.len() as u16;
                let cols_count = rows.first().map(|r| r.len()).unwrap_or(0) as u16;

                for row in rows {
                    for elem in row {
                        elements.push(self.convert_ast_node(elem, sheet_registry));
                    }
                }

                self.asts.insert_array(rows_count, cols_count, elements)
            }
        }
    }

    /// Convert ReferenceType to CompactRefType
    fn convert_reference_type(
        &mut self,
        ref_type: &ReferenceType,
        sheet_registry: &SheetRegistry,
    ) -> CompactRefType {
        match ref_type {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => {
                let sheet = match sheet.as_ref() {
                    Some(s) => match sheet_registry.get_id(s) {
                        Some(id) => Some(SheetKey::Id(id)),
                        None => Some(SheetKey::Name(self.asts.strings_mut().intern(s))),
                    },
                    None => None,
                };
                CompactRefType::Cell {
                    sheet,
                    row: *row,
                    col: *col,
                    row_abs: *row_abs,
                    col_abs: *col_abs,
                }
            }

            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                let sheet = match sheet.as_ref() {
                    Some(s) => match sheet_registry.get_id(s) {
                        Some(id) => Some(SheetKey::Id(id)),
                        None => Some(SheetKey::Name(self.asts.strings_mut().intern(s))),
                    },
                    None => None,
                };
                // For optional range bounds, use 0/u32::MAX as sentinels for unbounded
                CompactRefType::Range {
                    sheet,
                    start_row: start_row.unwrap_or(0),
                    start_col: start_col.unwrap_or(0),
                    end_row: end_row.unwrap_or(u32::MAX),
                    end_col: end_col.unwrap_or(u32::MAX),
                    start_row_abs: *start_row_abs,
                    start_col_abs: *start_col_abs,
                    end_row_abs: *end_row_abs,
                    end_col_abs: *end_col_abs,
                }
            }

            ReferenceType::External(ext) => {
                let raw_id = self.asts.strings_mut().intern(&ext.raw);
                let book_id = self.asts.strings_mut().intern(ext.book.token());
                let sheet_id = self.asts.strings_mut().intern(&ext.sheet);
                CompactRefType::External {
                    raw_id,
                    book_id,
                    sheet_id,
                    kind: ext.kind,
                }
            }

            ReferenceType::NamedRange(name) => {
                let string_id = self.asts.strings_mut().intern(name);
                CompactRefType::NamedRange(string_id)
            }

            ReferenceType::Table(table_ref) => {
                let name_id = self.asts.strings_mut().intern(&table_ref.name);
                let specifier_id = table_ref
                    .specifier
                    .as_ref()
                    .map(|specifier| self.asts.intern_table_specifier(specifier));
                CompactRefType::Table {
                    name_id,
                    specifier_id,
                }
            }
        }
    }

    /// Reconstruct an ASTNode from arena representation
    fn reconstruct_ast_node(
        &self,
        id: AstNodeId,
        sheet_registry: &SheetRegistry,
    ) -> Option<ASTNode> {
        use super::ast::AstNodeData;

        let node_data = self.asts.get(id)?;

        let node_type = match node_data {
            AstNodeData::Literal(value_ref) => {
                let lit = self.retrieve_value(*value_ref);
                ASTNodeType::Literal(lit)
            }

            AstNodeData::Reference {
                original_id,
                ref_type,
            } => {
                let original = self.asts.resolve_string(*original_id).to_string();
                let reference = self.reconstruct_reference_type(ref_type, sheet_registry);
                ASTNodeType::Reference {
                    original,
                    reference,
                }
            }

            AstNodeData::UnaryOp { op_id, expr_id } => {
                let op = self.asts.resolve_string(*op_id).to_string();
                let expr = Box::new(self.reconstruct_ast_node(*expr_id, sheet_registry)?);
                ASTNodeType::UnaryOp { op, expr }
            }

            AstNodeData::BinaryOp {
                op_id,
                left_id,
                right_id,
            } => {
                let op = self.asts.resolve_string(*op_id).to_string();
                let left = Box::new(self.reconstruct_ast_node(*left_id, sheet_registry)?);
                let right = Box::new(self.reconstruct_ast_node(*right_id, sheet_registry)?);
                ASTNodeType::BinaryOp { op, left, right }
            }

            AstNodeData::Function { name_id, .. } => {
                let name = self.asts.resolve_string(*name_id).to_string();
                let arg_ids = self.asts.get_function_args(id)?;
                let args: Vec<ASTNode> = arg_ids
                    .iter()
                    .filter_map(|&arg_id| self.reconstruct_ast_node(arg_id, sheet_registry))
                    .collect();
                ASTNodeType::Function { name, args }
            }

            AstNodeData::Array { rows, cols, .. } => {
                let elements = self.asts.get_array_elements(id)?;
                let mut result = Vec::with_capacity(*rows as usize);

                for r in 0..*rows {
                    let mut row = Vec::with_capacity(*cols as usize);
                    for c in 0..*cols {
                        let idx = (r * *cols + c) as usize;
                        if let Some(&elem_id) = elements.get(idx)
                            && let Some(node) = self.reconstruct_ast_node(elem_id, sheet_registry)
                        {
                            row.push(node);
                        }
                    }
                    result.push(row);
                }

                ASTNodeType::Array(result)
            }
        };

        Some(ASTNode {
            node_type,
            source_token: None, // Token information is not preserved in arena
            contains_volatile: false,
        })
    }

    /// Reconstruct a ReferenceType from CompactRefType
    fn reconstruct_reference_type(
        &self,
        ref_type: &CompactRefType,
        sheet_registry: &SheetRegistry,
    ) -> ReferenceType {
        match ref_type {
            CompactRefType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => {
                let sheet = match sheet {
                    Some(SheetKey::Id(id)) => Some(sheet_registry.name(*id).to_string()),
                    Some(SheetKey::Name(name_id)) => {
                        Some(self.asts.resolve_string(*name_id).to_string())
                    }
                    None => None,
                };
                ReferenceType::Cell {
                    sheet,
                    row: *row,
                    col: *col,
                    row_abs: *row_abs,
                    col_abs: *col_abs,
                }
            }

            CompactRefType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                let sheet = match sheet {
                    Some(SheetKey::Id(id)) => Some(sheet_registry.name(*id).to_string()),
                    Some(SheetKey::Name(name_id)) => {
                        Some(self.asts.resolve_string(*name_id).to_string())
                    }
                    None => None,
                };
                // Convert sentinel values back to None
                ReferenceType::Range {
                    sheet,
                    start_row: if *start_row == 0 {
                        None
                    } else {
                        Some(*start_row)
                    },
                    start_col: if *start_col == 0 {
                        None
                    } else {
                        Some(*start_col)
                    },
                    end_row: if *end_row == u32::MAX {
                        None
                    } else {
                        Some(*end_row)
                    },
                    end_col: if *end_col == u32::MAX {
                        None
                    } else {
                        Some(*end_col)
                    },
                    start_row_abs: *start_row_abs,
                    start_col_abs: *start_col_abs,
                    end_row_abs: *end_row_abs,
                    end_col_abs: *end_col_abs,
                }
            }

            CompactRefType::External {
                raw_id,
                book_id,
                sheet_id,
                kind,
            } => {
                let raw = self.asts.resolve_string(*raw_id).to_string();
                let book = self.asts.resolve_string(*book_id).to_string();
                let sheet = self.asts.resolve_string(*sheet_id).to_string();
                ReferenceType::External(ExternalReference {
                    raw,
                    book: ExternalBookRef::Token(book),
                    sheet,
                    kind: *kind,
                })
            }

            CompactRefType::NamedRange(string_id) => {
                let name = self.asts.resolve_string(*string_id).to_string();
                ReferenceType::NamedRange(name)
            }

            CompactRefType::Table {
                name_id,
                specifier_id,
            } => {
                let name = self.asts.resolve_string(*name_id).to_string();
                let specifier = specifier_id
                    .and_then(|id| self.asts.resolve_table_specifier(id))
                    .cloned();
                ReferenceType::Table(TableReference { name, specifier })
            }
        }
    }

    /// Store an error with message preservation
    fn store_error(&mut self, error: &ExcelError) -> ValueRef {
        let error_ref = self.errors.insert(error);
        ValueRef::error(error_ref.as_u32())
    }

    /// Get memory usage statistics
    pub fn memory_usage(&self) -> DataStoreStats {
        DataStoreStats {
            scalar_bytes: self.scalars.memory_usage(),
            string_bytes: self.strings.memory_usage(),
            array_bytes: self.arrays.memory_usage(),
            ast_bytes: self.asts.memory_usage(),
            error_bytes: self.errors.memory_usage(),
            total_scalars: self.scalars.len(),
            total_strings: self.strings.len(),
            total_arrays: self.arrays.len(),
            total_ast_nodes: self.asts.stats().node_count,
            total_errors: self.errors.len(),
        }
    }

    /// Clear all data from the store
    pub fn clear(&mut self) {
        self.scalars.clear();
        self.strings.clear();
        self.arrays.clear();
        self.asts.clear();
        self.errors.clear();
    }
}

impl Default for DataStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about data store memory usage
#[derive(Debug, Clone)]
pub struct DataStoreStats {
    pub scalar_bytes: usize,
    pub string_bytes: usize,
    pub array_bytes: usize,
    pub ast_bytes: usize,
    pub error_bytes: usize,
    pub total_scalars: usize,
    pub total_strings: usize,
    pub total_arrays: usize,
    pub total_ast_nodes: usize,
    pub total_errors: usize,
}

impl DataStoreStats {
    pub fn total_bytes(&self) -> usize {
        self.scalar_bytes + self.string_bytes + self.array_bytes + self.ast_bytes + self.error_bytes
    }
}

// Helper trait implementations for ArrayRef and ScalarRef
impl super::array::ArrayRef {
    pub fn from_raw(raw: u32) -> Self {
        super::array::ArrayRef(raw)
    }
}

impl super::scalar::ScalarRef {
    pub fn from_raw(raw: u32) -> Self {
        Self { raw }
    }

    pub fn as_u32(self) -> u32 {
        self.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_store_empty_value() {
        let mut store = DataStore::new();
        let value_ref = store.store_value(LiteralValue::Empty);
        assert!(value_ref.is_empty());

        let retrieved = store.retrieve_value(value_ref);
        assert_eq!(retrieved, LiteralValue::Empty);
    }

    #[test]
    fn test_data_store_number() {
        let mut store = DataStore::new();
        let value_ref = store.store_value(LiteralValue::Number(42.5));

        let retrieved = store.retrieve_value(value_ref);
        assert_eq!(retrieved, LiteralValue::Number(42.5));
    }

    #[test]
    fn test_data_store_text() {
        let mut store = DataStore::new();
        let value_ref = store.store_value(LiteralValue::Text("Hello".to_string()));

        let retrieved = store.retrieve_value(value_ref);
        assert_eq!(retrieved, LiteralValue::Text("Hello".to_string()));
    }

    #[test]
    fn test_data_store_boolean() {
        let mut store = DataStore::new();

        let true_ref = store.store_value(LiteralValue::Boolean(true));
        let false_ref = store.store_value(LiteralValue::Boolean(false));

        assert_eq!(store.retrieve_value(true_ref), LiteralValue::Boolean(true));
        assert_eq!(
            store.retrieve_value(false_ref),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn test_data_store_error() {
        let mut store = DataStore::new();

        let error = ExcelError::new(ExcelErrorKind::Div);
        let value_ref = store.store_value(LiteralValue::Error(error.clone()));

        let retrieved = store.retrieve_value(value_ref);
        match retrieved {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Div),
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_data_store_array() {
        let mut store = DataStore::new();

        let array = vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
        ];

        let value_ref = store.store_value(LiteralValue::Array(array.clone()));
        let retrieved = store.retrieve_value(value_ref);

        assert_eq!(retrieved, LiteralValue::Array(array));
    }

    #[test]
    fn test_data_store_ast_literal() {
        let mut store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        sheet_registry.id_for("Sheet1");

        let ast = ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Number(42.0)),
            source_token: None,
            contains_volatile: false,
        };

        let ast_id = store.store_ast(&ast, &sheet_registry);
        let retrieved = store.retrieve_ast(ast_id, &sheet_registry).unwrap();

        match retrieved.node_type {
            ASTNodeType::Literal(lit) => assert_eq!(lit, LiteralValue::Number(42.0)),
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_data_store_ast_binary_op() {
        let mut store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        sheet_registry.id_for("Sheet1");

        let ast = ASTNode {
            node_type: ASTNodeType::BinaryOp {
                op: "+".to_string(),
                left: Box::new(ASTNode {
                    node_type: ASTNodeType::Literal(LiteralValue::Number(1.0)),
                    source_token: None,
                    contains_volatile: false,
                }),
                right: Box::new(ASTNode {
                    node_type: ASTNodeType::Literal(LiteralValue::Number(2.0)),
                    source_token: None,
                    contains_volatile: false,
                }),
            },
            source_token: None,
            contains_volatile: false,
        };

        let ast_id = store.store_ast(&ast, &sheet_registry);
        let retrieved = store.retrieve_ast(ast_id, &sheet_registry).unwrap();

        match retrieved.node_type {
            ASTNodeType::BinaryOp { op, left, right } => {
                assert_eq!(op, "+");
                match left.node_type {
                    ASTNodeType::Literal(lit) => assert_eq!(lit, LiteralValue::Number(1.0)),
                    _ => panic!("Expected literal"),
                }
                match right.node_type {
                    ASTNodeType::Literal(lit) => assert_eq!(lit, LiteralValue::Number(2.0)),
                    _ => panic!("Expected literal"),
                }
            }
            _ => panic!("Expected binary op"),
        }
    }

    #[test]
    fn test_data_store_ast_function() {
        let mut store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        sheet_registry.id_for("Sheet1");

        let ast = ASTNode {
            node_type: ASTNodeType::Function {
                name: "SUM".to_string(),
                args: vec![
                    ASTNode {
                        node_type: ASTNodeType::Literal(LiteralValue::Number(1.0)),
                        source_token: None,
                        contains_volatile: false,
                    },
                    ASTNode {
                        node_type: ASTNodeType::Literal(LiteralValue::Number(2.0)),
                        source_token: None,
                        contains_volatile: false,
                    },
                ],
            },
            source_token: None,
            contains_volatile: false,
        };

        let ast_id = store.store_ast(&ast, &sheet_registry);
        let retrieved = store.retrieve_ast(ast_id, &sheet_registry).unwrap();

        match retrieved.node_type {
            ASTNodeType::Function { name, args } => {
                assert_eq!(name, "SUM");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected function"),
        }
    }

    #[test]
    fn test_data_store_memory_stats() {
        let mut store = DataStore::new();

        // Add some data
        store.store_value(LiteralValue::Number(42.0));
        store.store_value(LiteralValue::Text("Hello".to_string()));
        store.store_value(LiteralValue::Array(vec![vec![LiteralValue::Number(1.0)]]));

        let stats = store.memory_usage();
        assert!(stats.total_bytes() > 0);
        assert_eq!(stats.total_scalars, 2); // 42.0 and 1.0
        assert_eq!(stats.total_strings, 1); // "Hello"
        assert_eq!(stats.total_arrays, 1);
    }

    #[test]
    fn test_data_store_clear() {
        let mut store = DataStore::new();

        store.store_value(LiteralValue::Number(42.0));
        store.store_value(LiteralValue::Text("Hello".to_string()));

        let stats = store.memory_usage();
        assert!(stats.total_scalars > 0);
        assert!(stats.total_strings > 0);

        store.clear();

        let stats = store.memory_usage();
        assert_eq!(stats.total_scalars, 0);
        assert_eq!(stats.total_strings, 0);
    }
}
