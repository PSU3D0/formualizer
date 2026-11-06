//! Catch only calls into the host; engine scheduling/cache mutation stays outside.
use crate::engine::{
    lookup_index_cache::{LookupAxis, LookupIndex},
    row_visibility::VisibilityMaskMode,
};
use crate::{CellRef, engine::range_view::RangeView};
use crate::{
    external_callbacks::{self, CallbackState, Label},
    function::Function,
    traits::*,
};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::{ReferenceType, TableReference};
use std::sync::Arc;

pub(crate) struct GuardedResolver<R> {
    pub(crate) inner: R,
    pub(crate) callbacks: Arc<CallbackState>,
}
impl<R> GuardedResolver<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            callbacks: Arc::default(),
        }
    }
}
struct Fallback;
impl ReferenceResolver for Fallback {
    fn resolve_cell_reference(
        &self,
        _: Option<&str>,
        _: u32,
        _: u32,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Empty)
    }
}
impl RangeResolver for Fallback {
    fn resolve_range_reference(
        &self,
        _: Option<&str>,
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        Err(ExcelError::new_ref())
    }
}
impl NamedRangeResolver for Fallback {
    fn resolve_named_range_reference(&self, _: &str) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        Err(ExcelError::new_name())
    }
}
impl TableResolver for Fallback {
    fn resolve_table_reference(&self, _: &TableReference) -> Result<Box<dyn Table>, ExcelError> {
        Err(ExcelError::new_ref())
    }
}
impl FunctionProvider for Fallback {
    fn get_function(&self, _: &str, _: &str) -> Option<Arc<dyn Function>> {
        None
    }
}
impl SourceResolver for Fallback {}
impl Resolver for Fallback {}
impl EvaluationContext for Fallback {}
static FALLBACK: Fallback = Fallback;

impl<R: EvaluationContext> ReferenceResolver for GuardedResolver<R> {
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        let label = Label("resolve_cell_reference", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_cell_reference(sheet, row, col)
        })?
    }
}

impl<R: EvaluationContext> RangeResolver for GuardedResolver<R> {
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        let label = Label("resolve_range_reference", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_range_reference(sheet, sr, sc, er, ec)
        })?
        .map(|inner| {
            Box::new(GuardedRange {
                inner,
                callbacks: self.callbacks.clone(),
                source: sheet.unwrap_or("range").into(),
            }) as Box<dyn Range>
        })
    }
}

impl<R: EvaluationContext> NamedRangeResolver for GuardedResolver<R> {
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        let label = Label("resolve_named_range_reference", Some(name));
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_named_range_reference(name)
        })?
    }
}

impl<R: EvaluationContext> TableResolver for GuardedResolver<R> {
    fn resolve_table_reference(
        &self,
        tref: &formualizer_parse::parser::TableReference,
    ) -> Result<Box<dyn Table>, ExcelError> {
        let label = Label("resolve_table_reference", Some(&tref.name));
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_table_reference(tref)
        })?
        .map(|inner| {
            Box::new(GuardedTable {
                inner,
                callbacks: self.callbacks.clone(),
                source: tref.name.clone(),
            }) as Box<dyn Table>
        })
    }
}

impl<R: EvaluationContext> SourceResolver for GuardedResolver<R> {
    fn source_scalar_version(&self, _name: &str) -> Option<u64> {
        let label = Label("source_scalar_version", Some(_name));
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.source_scalar_version(_name),
            || self.inner.source_scalar_version(_name),
        )
    }
    fn resolve_source_scalar(&self, name: &str) -> Result<LiteralValue, ExcelError> {
        let label = Label("resolve_source_scalar", Some(name));
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_source_scalar(name)
        })?
    }
    fn source_table_version(&self, _name: &str) -> Option<u64> {
        let label = Label("source_table_version", Some(_name));
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.source_table_version(_name),
            || self.inner.source_table_version(_name),
        )
    }
    fn resolve_source_table(&self, name: &str) -> Result<Box<dyn Table>, ExcelError> {
        let label = Label("resolve_source_table", Some(name));
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_source_table(name)
        })?
        .map(|inner| {
            Box::new(GuardedTable {
                inner,
                callbacks: self.callbacks.clone(),
                source: name.into(),
            }) as Box<dyn Table>
        })
    }
}

impl<R: EvaluationContext> FunctionProvider for GuardedResolver<R> {
    fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn Function>> {
        let label = Label("get_function", Some(name));
        match external_callbacks::callback(&self.callbacks, label, || {
            self.inner.get_function(ns, name)
        }) {
            Ok(value) => value,
            Err(error) => {
                self.callbacks.provider_failed(name, &error);
                Some(Arc::new(CallbackFailure(error)))
            }
        }
    }
    fn get_function_for_planning(&self, _ns: &str, _name: &str) -> Option<Arc<dyn Function>> {
        let label = Label("get_function_for_planning", Some(_name));
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.get_function_for_planning(_ns, _name),
            || self.inner.get_function_for_planning(_ns, _name),
        )
    }
    fn planning_semantic_revision(&self) -> Option<u64> {
        let label = Label("planning_semantic_revision", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.planning_semantic_revision(),
            || self.inner.planning_semantic_revision(),
        )
    }
    fn function_capabilities(&self, ns: &str, name: &str) -> Option<crate::function::FnCaps> {
        let label = Label("function_capabilities", Some(name));
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.function_capabilities(ns, name),
            || self.inner.function_capabilities(ns, name),
        )
    }
    fn function_semantic_identity(
        &self,
        ns: &str,
        name: &str,
        arity: usize,
    ) -> Option<crate::function_contract::FunctionSemanticIdentity> {
        let label = Label("function_semantic_identity", Some(name));
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.function_semantic_identity(ns, name, arity),
            || self.inner.function_semantic_identity(ns, name, arity),
        )
    }
}

impl<R: EvaluationContext> EvaluationContext for GuardedResolver<R> {
    fn operation_diagnostic_collector(
        &self,
    ) -> Option<&crate::operation_diagnostics::OperationDiagnosticCollector> {
        let label = Label("operation_diagnostic_collector", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.operation_diagnostic_collector(),
            || self.inner.operation_diagnostic_collector(),
        )
    }
    fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        let label = Label("thread_pool", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.thread_pool(),
            || self.inner.thread_pool(),
        )
    }
    fn cancellation_token(&self) -> Option<crate::engine::CancelToken> {
        let label = Label("cancellation_token", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.cancellation_token(),
            || self.inner.cancellation_token(),
        )
    }
    fn chunk_hint(&self) -> Option<usize> {
        let label = Label("chunk_hint", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.chunk_hint(),
            || self.inner.chunk_hint(),
        )
    }
    fn materialized_array_cells(&self) -> u64 {
        let label = Label("materialized_array_cells", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.materialized_array_cells(),
            || self.inner.materialized_array_cells(),
        )
    }
    fn resolve_range_view<'c>(
        &'c self,
        _reference: &ReferenceType,
        _current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        let label = Label("resolve_range_view", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_range_view(_reference, _current_sheet)
        })?
    }
    fn resolve_cell_reference_value(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Result<LiteralValue, ExcelError> {
        let label = Label("resolve_cell_reference_value", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner
                .resolve_cell_reference_value(sheet, row, col, current_sheet)
        })?
    }
    fn resolve_cell_format(
        &self,
        _sheet: Option<&str>,
        _row: u32,
        _col: u32,
        _current_sheet: &str,
    ) -> Option<crate::format::FormatId> {
        let label = Label("resolve_cell_format", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.resolve_cell_format(_sheet, _row, _col, _current_sheet),
            || {
                self.inner
                    .resolve_cell_format(_sheet, _row, _col, _current_sheet)
            },
        )
    }
    fn resolve_cell_reference_value_formatted(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Result<(LiteralValue, Option<crate::format::FormatId>), ExcelError> {
        let label = Label("resolve_cell_reference_value_formatted", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner
                .resolve_cell_reference_value_formatted(sheet, row, col, current_sheet)
        })?
    }
    fn format_class(
        &self,
        format: crate::format::FormatId,
    ) -> Option<formualizer_common::numfmt::FormatClass> {
        let label = Label("format_class", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.format_class(format),
            || self.inner.format_class(format),
        )
    }
    fn record_cell_derived_format(
        &self,
        _sheet: &str,
        _row: u32,
        _col: u32,
        _format: Option<crate::format::FormatId>,
    ) {
        let label = Label("record_cell_derived_format", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.record_cell_derived_format(_sheet, _row, _col, _format),
            || {
                self.inner
                    .record_cell_derived_format(_sheet, _row, _col, _format)
            },
        )
    }
    fn locale(&self) -> crate::locale::Locale {
        let label = Label("locale", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.locale(),
            || self.inner.locale(),
        )
    }
    fn named_binding_presence(&self, _name: &str, _current_sheet: &str) -> Option<bool> {
        let label = Label("named_binding_presence", Some(_name));
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.named_binding_presence(_name, _current_sheet),
            || self.inner.named_binding_presence(_name, _current_sheet),
        )
    }
    fn workbook_sheet_count(&self) -> Option<usize> {
        let label = Label("workbook_sheet_count", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.workbook_sheet_count(),
            || self.inner.workbook_sheet_count(),
        )
    }
    fn sheet_index_by_name(&self, _sheet: &str) -> Option<usize> {
        let label = Label("sheet_index_by_name", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.sheet_index_by_name(_sheet),
            || self.inner.sheet_index_by_name(_sheet),
        )
    }
    fn current_sheet_index(&self, current_sheet: &str) -> Option<usize> {
        let label = Label("current_sheet_index", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.current_sheet_index(current_sheet),
            || self.inner.current_sheet_index(current_sheet),
        )
    }
    fn inspect_reference(
        &self,
        _reference: &ReferenceType,
        _current_sheet: &str,
    ) -> Result<Option<ReferenceInfo>, ExcelError> {
        let label = Label("inspect_reference", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.inspect_reference(_reference, _current_sheet)
        })?
    }
    fn has_formula_at_cell(&self, cell: CellRef) -> Result<bool, ExcelError> {
        let label = Label("has_formula_at_cell", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.has_formula_at_cell(cell)
        })?
    }
    fn formula_text_at_cell(&self, _cell: CellRef) -> Result<Option<String>, ExcelError> {
        let label = Label("formula_text_at_cell", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.formula_text_at_cell(_cell)
        })?
    }
    fn resolve_spill_reference(
        &self,
        _anchor: &ReferenceType,
        _current_sheet: &str,
    ) -> Result<ReferenceType, ExcelError> {
        let label = Label("resolve_spill_reference", None);
        external_callbacks::callback(&self.callbacks, label, || {
            self.inner.resolve_spill_reference(_anchor, _current_sheet)
        })?
    }
    fn clock(&self) -> &dyn crate::timezone::ClockProvider {
        let label = Label("clock", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.clock(),
            || self.inner.clock(),
        )
    }
    fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
        let label = Label("timezone", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.timezone(),
            || self.inner.timezone(),
        )
    }
    fn volatile_level(&self) -> VolatileLevel {
        let label = Label("volatile_level", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.volatile_level(),
            || self.inner.volatile_level(),
        )
    }
    fn workbook_seed(&self) -> u64 {
        let label = Label("workbook_seed", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.workbook_seed(),
            || self.inner.workbook_seed(),
        )
    }
    fn recalc_epoch(&self) -> u64 {
        let label = Label("recalc_epoch", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.recalc_epoch(),
            || self.inner.recalc_epoch(),
        )
    }
    fn used_rows_for_columns(
        &self,
        _sheet: &str,
        _start_col: u32,
        _end_col: u32,
    ) -> Option<(u32, u32)> {
        let label = Label("used_rows_for_columns", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.used_rows_for_columns(_sheet, _start_col, _end_col),
            || {
                self.inner
                    .used_rows_for_columns(_sheet, _start_col, _end_col)
            },
        )
    }
    fn used_cols_for_rows(
        &self,
        _sheet: &str,
        _start_row: u32,
        _end_row: u32,
    ) -> Option<(u32, u32)> {
        let label = Label("used_cols_for_rows", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.used_cols_for_rows(_sheet, _start_row, _end_row),
            || self.inner.used_cols_for_rows(_sheet, _start_row, _end_row),
        )
    }
    fn sheet_bounds(&self, _sheet: &str) -> Option<(u32, u32)> {
        let label = Label("sheet_bounds", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.sheet_bounds(_sheet),
            || self.inner.sheet_bounds(_sheet),
        )
    }
    fn data_snapshot_id(&self) -> u64 {
        let label = Label("data_snapshot_id", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.data_snapshot_id(),
            || self.inner.data_snapshot_id(),
        )
    }
    fn backend_caps(&self) -> BackendCaps {
        let label = Label("backend_caps", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.backend_caps(),
            || self.inner.backend_caps(),
        )
    }
    fn date_system(&self) -> crate::engine::DateSystem {
        let label = Label("date_system", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.date_system(),
            || self.inner.date_system(),
        )
    }
    fn build_lookup_index(
        &self,
        _view: &RangeView<'_>,
        _axis: LookupAxis,
    ) -> Option<std::sync::Arc<LookupIndex>> {
        let label = Label("build_lookup_index", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.build_lookup_index(_view, _axis),
            || self.inner.build_lookup_index(_view, _axis),
        )
    }
    fn build_criteria_mask(
        &self,
        _view: &RangeView<'_>,
        _col_in_view: usize,
        _pred: &crate::args::CriteriaPredicate,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        let label = Label("build_criteria_mask", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.build_criteria_mask(_view, _col_in_view, _pred),
            || self.inner.build_criteria_mask(_view, _col_in_view, _pred),
        )
    }
    fn build_row_visibility_mask(
        &self,
        _view: &RangeView<'_>,
        _mode: VisibilityMaskMode,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        let label = Label("build_row_visibility_mask", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.build_row_visibility_mask(_view, _mode),
            || self.inner.build_row_visibility_mask(_view, _mode),
        )
    }
    fn nested_subtotal_cells(
        &self,
        _view: &RangeView<'_>,
        _include_aggregate: bool,
    ) -> Option<Vec<(usize, usize, usize)>> {
        let label = Label("nested_subtotal_cells", None);
        external_callbacks::optional_callback(
            &self.callbacks,
            label,
            || FALLBACK.nested_subtotal_cells(_view, _include_aggregate),
            || self.inner.nested_subtotal_cells(_view, _include_aggregate),
        )
    }
}
impl<R: EvaluationContext> Resolver for GuardedResolver<R> {}

struct CallbackFailure(ExcelError);
impl Function for CallbackFailure {
    fn name(&self) -> &'static str {
        "__RESOLVER_CALLBACK_FAILURE__"
    }
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::empty()
    }
    fn reports_known_limitations(&self) -> bool {
        true
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if let formualizer_common::ExcelErrorExtra::FunctionPanicked { function, .. } =
            self.0.extra()
        {
            ctx.report_unavailable_operation(
                crate::operation_diagnostics::UnavailableOperation::FunctionPanicked {
                    name: function.clone(),
                },
            );
        }
        Err(self.0.clone())
    }
}

struct GuardedRange {
    inner: Box<dyn Range>,
    callbacks: Arc<CallbackState>,
    source: String,
}
impl std::fmt::Debug for GuardedRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GuardedRange")
    }
}
impl Range for GuardedRange {
    fn get(&self, row: usize, col: usize) -> Result<LiteralValue, ExcelError> {
        external_callbacks::callback(
            &self.callbacks,
            Label("range.get", Some(&self.source)),
            || self.inner.get(row, col),
        )?
    }
    fn dimensions(&self) -> (usize, usize) {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("range.dimensions", Some(&self.source)),
            || (1, 1),
            || self.inner.dimensions(),
        )
    }
    fn is_sparse(&self) -> bool {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("range.is_sparse", Some(&self.source)),
            || false,
            || self.inner.is_sparse(),
        )
    }
    fn is_infinite(&self) -> bool {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("range.is_infinite", Some(&self.source)),
            || false,
            || self.inner.is_infinite(),
        )
    }
    fn materialise(&self) -> std::borrow::Cow<'_, [Vec<LiteralValue>]> {
        match external_callbacks::callback(
            &self.callbacks,
            Label("range.materialise", Some(&self.source)),
            || self.inner.materialise(),
        ) {
            Ok(value) => value,
            Err(error) => std::borrow::Cow::Owned(vec![vec![LiteralValue::Error(error)]]),
        }
    }
    fn iter_cells<'a>(&'a self) -> Box<dyn Iterator<Item = LiteralValue> + 'a> {
        let state = self.callbacks.clone();
        let source = self.source.clone();
        match external_callbacks::callback(&state, Label("range.iter_cells", Some(&source)), || {
            self.inner.iter_cells()
        }) {
            Ok(mut iter) => {
                let mut failed = false;
                Box::new(std::iter::from_fn(move || {
                    if failed {
                        return None;
                    }
                    match external_callbacks::callback(
                        &state,
                        Label("range.iter_cells.next", Some(&source)),
                        || iter.next(),
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            failed = true;
                            Some(LiteralValue::Error(error))
                        }
                    }
                }))
            }
            Err(error) => Box::new(std::iter::once(LiteralValue::Error(error))),
        }
    }
    fn iter_rows<'a>(&'a self) -> Box<dyn Iterator<Item = Vec<LiteralValue>> + 'a> {
        let state = self.callbacks.clone();
        let source = self.source.clone();
        match external_callbacks::callback(&state, Label("range.iter_rows", Some(&source)), || {
            self.inner.iter_rows()
        }) {
            Ok(mut iter) => {
                let mut failed = false;
                Box::new(std::iter::from_fn(move || {
                    if failed {
                        return None;
                    }
                    match external_callbacks::callback(
                        &state,
                        Label("range.iter_rows.next", Some(&source)),
                        || iter.next(),
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            failed = true;
                            Some(vec![LiteralValue::Error(error)])
                        }
                    }
                }))
            }
            Err(error) => Box::new(std::iter::once(vec![LiteralValue::Error(error)])),
        }
    }
    fn as_any(&self) -> &dyn std::any::Any {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("range.as_any", Some(&self.source)),
            || self as &dyn std::any::Any,
            || self.inner.as_any(),
        )
    }
}
struct GuardedTable {
    inner: Box<dyn Table>,
    callbacks: Arc<CallbackState>,
    source: String,
}
impl std::fmt::Debug for GuardedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GuardedTable")
    }
}
impl GuardedTable {
    fn range(&self, range: Box<dyn Range>) -> Box<dyn Range> {
        Box::new(GuardedRange {
            inner: range,
            callbacks: self.callbacks.clone(),
            source: self.source.clone(),
        })
    }
}
impl Table for GuardedTable {
    fn get_cell(&self, row: usize, column: &str) -> Result<LiteralValue, ExcelError> {
        external_callbacks::callback(
            &self.callbacks,
            Label("table.get_cell", Some(&self.source)),
            || self.inner.get_cell(row, column),
        )?
    }
    fn get_column(&self, column: &str) -> Result<Box<dyn Range>, ExcelError> {
        external_callbacks::callback(
            &self.callbacks,
            Label("table.get_column", Some(&self.source)),
            || self.inner.get_column(column),
        )?
        .map(|range| self.range(range))
    }
    fn columns(&self) -> Vec<String> {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("table.columns", Some(&self.source)),
            Vec::new,
            || self.inner.columns(),
        )
    }
    fn data_height(&self) -> usize {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("table.data_height", Some(&self.source)),
            || 0,
            || self.inner.data_height(),
        )
    }
    fn has_headers(&self) -> bool {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("table.has_headers", Some(&self.source)),
            || false,
            || self.inner.has_headers(),
        )
    }
    fn has_totals(&self) -> bool {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("table.has_totals", Some(&self.source)),
            || false,
            || self.inner.has_totals(),
        )
    }
    fn headers_row(&self) -> Option<Box<dyn Range>> {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("table.headers_row", Some(&self.source)),
            || None,
            || self.inner.headers_row(),
        )
        .map(|r| self.range(r))
    }
    fn totals_row(&self) -> Option<Box<dyn Range>> {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("table.totals_row", Some(&self.source)),
            || None,
            || self.inner.totals_row(),
        )
        .map(|r| self.range(r))
    }
    fn data_body(&self) -> Option<Box<dyn Range>> {
        external_callbacks::optional_callback(
            &self.callbacks,
            Label("table.data_body", Some(&self.source)),
            || None,
            || self.inner.data_body(),
        )
        .map(|r| self.range(r))
    }
    fn clone_box(&self) -> Box<dyn Table> {
        let inner = match external_callbacks::callback(
            &self.callbacks,
            Label("table.clone_box", Some(&self.source)),
            || self.inner.clone_box(),
        ) {
            Ok(table) => table,
            Err(error) => Box::new(FailedTable(error)),
        };
        Box::new(Self {
            inner,
            callbacks: self.callbacks.clone(),
            source: self.source.clone(),
        })
    }
}
#[derive(Debug, Clone)]
struct FailedTable(ExcelError);
impl Table for FailedTable {
    fn get_cell(&self, _: usize, _: &str) -> Result<LiteralValue, ExcelError> {
        Err(self.0.clone())
    }
    fn get_column(&self, _: &str) -> Result<Box<dyn Range>, ExcelError> {
        Err(self.0.clone())
    }
    fn clone_box(&self) -> Box<dyn Table> {
        Box::new(self.clone())
    }
}
