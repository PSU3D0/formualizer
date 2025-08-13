use formualizer_common::error::{ExcelError, ExcelErrorKind};
use formualizer_common::value::LiteralValue;
use formualizer_core::parser::{ReferenceType, TableReference};

/// A minimal in-process resolver for Python bindings.
///
/// Engine uses its own EvaluationContext implementation for core range/cell resolution
/// via the dependency graph. This resolver only needs to satisfy trait bounds and
/// can error out on features we don't expose yet (named ranges, tables, external IO).
#[derive(Default, Debug, Clone, Copy)]
pub struct PyResolver;

impl formualizer_eval::traits::ReferenceResolver for PyResolver {
    fn resolve_cell_reference(
        &self,
        _sheet: Option<&str>,
        _row: u32,
        _col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        // Not used: Engine's EvaluationContext handles cells via graph.
        Err(ExcelError::from(ExcelErrorKind::NImpl))
    }
}

impl formualizer_eval::traits::RangeResolver for PyResolver {
    fn resolve_range_reference(
        &self,
        _sheet: Option<&str>,
        _sr: Option<u32>,
        _sc: Option<u32>,
        _er: Option<u32>,
        _ec: Option<u32>,
    ) -> Result<Box<dyn formualizer_eval::traits::Range>, ExcelError> {
        // Not used: Engine's EvaluationContext handles ranges via graph for cell/range refs.
        Err(ExcelError::from(ExcelErrorKind::NImpl))
    }
}

impl formualizer_eval::traits::NamedRangeResolver for PyResolver {
    fn resolve_named_range_reference(
        &self,
        _name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        Err(ExcelError::from(ExcelErrorKind::Name))
    }
}

impl formualizer_eval::traits::TableResolver for PyResolver {
    fn resolve_table_reference(
        &self,
        _tref: &TableReference,
    ) -> Result<Box<dyn formualizer_eval::traits::Table>, ExcelError> {
        Err(ExcelError::from(ExcelErrorKind::NImpl))
    }
}

impl formualizer_eval::traits::FunctionProvider for PyResolver {
    fn get_function(
        &self,
        _ns: &str,
        _name: &str,
    ) -> Option<std::sync::Arc<dyn formualizer_eval::function::Function>> {
        // Defer to global registry via Engine fallback
        None
    }
}

impl formualizer_eval::traits::Resolver for PyResolver {}

impl formualizer_eval::traits::EvaluationContext for PyResolver {
    fn resolve_range_storage<'c>(
        &'c self,
        _reference: &ReferenceType,
        _current_sheet: &str,
    ) -> Result<formualizer_eval::engine::range_stream::RangeStorage<'c>, ExcelError> {
        Err(ExcelError::from(ExcelErrorKind::NImpl))
    }
}
