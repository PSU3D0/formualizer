use crate::{
    CellRef,
    broadcast::{broadcast_shape, project_index},
    coercion,
    traits::{ArgumentHandle, DefaultFunctionContext, EvaluationContext},
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use formualizer_parse::{BinaryOperator, UnaryOperator};
use rustc_hash::FxHashMap;
use std::{borrow::Cow, sync::Arc};

use crate::engine::arena::ast::{CALL_NODE_NAME, SheetKey};
use crate::engine::arena::{AstNodeData, AstNodeId, CompactRefType, DataStore};
use crate::engine::sheet_registry::SheetRegistry;
use crate::engine::template::canonical::LiteralSlotId;
use crate::engine::used_extent::{
    ExtentPolicy, OpenRangeBounds, resolve_used_extent_with_fallback,
};

/// A spill reference whose operand is not a single-cell reference or name.
fn spill_operand_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::Ref)
        .with_message("Spill reference operand must be a single cell or a name")
}

/// Postfix calls (`LAMBDA(x,x+1)(B1)`) are parsed and stored but not evaluated;
/// the parsed tree and its arena copy fail the same way.
fn call_expression_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::NImpl)
        .with_message("Immediate-invocation calls are not yet supported")
}

pub(crate) fn probe_range_dimensions<C: EvaluationContext + ?Sized>(
    context: &C,
    current_sheet: &str,
    reference: &ReferenceType,
) -> Option<(u32, u32)> {
    match reference {
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            ..
        } => {
            let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
            let extent = resolve_used_extent_with_fallback(
                OpenRangeBounds {
                    start_row: *start_row,
                    start_column: *start_col,
                    end_row: *end_row,
                    end_column: *end_col,
                },
                ExtentPolicy::EvaluationCompat {
                    fallback_row: None,
                    fallback_column: None,
                },
                || context.sheet_bounds(sheet_name).map(|bounds| bounds.0),
                || context.sheet_bounds(sheet_name).map(|bounds| bounds.1),
                |first, last| context.used_rows_for_columns(sheet_name, first, last),
                |first, last| context.used_cols_for_rows(sheet_name, first, last),
            );
            let Some(extent) = extent else {
                return Some((0, 0));
            };
            Some((
                extent.end_row - extent.start_row + 1,
                extent.end_column - extent.start_column + 1,
            ))
        }
        ReferenceType::Cell { .. } => Some((1, 1)),
        _ => None,
    }
}

#[derive(Clone)]
pub enum LocalBinding {
    Value(LiteralValue),
    /// A local bound to a spreadsheet reference expression.
    ///
    /// `value` is exactly what [`LocalBinding::Value`] would have carried, so the
    /// value path is unchanged; `reference` additionally lets a by-ref argument
    /// slot see the range the local was bound to.
    ValueWithReference {
        value: LiteralValue,
        reference: ReferenceType,
    },
    Callable(Arc<dyn crate::traits::CustomCallable>),
}

#[derive(Clone, Default)]
pub struct LocalEnv {
    head: Option<Arc<EnvFrame>>,
}

#[derive(Clone)]
struct EnvFrame {
    parent: Option<Arc<EnvFrame>>,
    bindings: FxHashMap<String, LocalBinding>,
}

impl LocalEnv {
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    fn norm(name: &str) -> String {
        name.to_ascii_uppercase()
    }

    pub fn lookup(&self, name: &str) -> Option<LocalBinding> {
        self.head.as_ref()?;
        let key = Self::norm(name);
        let mut cur = self.head.as_ref().cloned();
        while let Some(frame) = cur {
            if let Some(v) = frame.bindings.get(&key) {
                return Some(v.clone());
            }
            cur = frame.parent.clone();
        }
        None
    }

    pub fn with_binding(&self, name: &str, value: LocalBinding) -> Self {
        let mut bindings = FxHashMap::default();
        bindings.insert(Self::norm(name), value);
        Self {
            head: Some(Arc::new(EnvFrame {
                parent: self.head.clone(),
                bindings,
            })),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct InterpreterParameterBindings<'a> {
    pub(crate) literal_slots_by_node: &'a FxHashMap<AstNodeId, LiteralSlotId>,
    pub(crate) literal_values: &'a [LiteralValue],
    /// Program 3: values of a family template's run-invariant calls,
    /// computed once per run (the same cells for every member).
    pub(crate) invariant_values: Option<&'a InvariantValues>,
}

/// Values of run-invariant calls by node (see `InterpreterParameterBindings`).
pub(crate) type InvariantValues =
    FxHashMap<AstNodeId, (LiteralValue, Option<crate::format::FormatId>)>;

/// Internal fast path; recording contexts deliberately retain their read hooks.
pub(crate) trait ArenaCellResolver {
    fn resolve_arena_range(
        &self,
        reference: &CompactRefType,
        current: crate::SheetId,
        row_delta: i64,
        col_delta: i64,
    ) -> Result<crate::engine::range_view::RangeView<'_>, ExcelError>;

    fn resolve_arena_cell(
        &self,
        sheet: Option<SheetKey>,
        current: crate::SheetId,
        row: u32,
        col: u32,
    ) -> Result<(LiteralValue, Option<crate::format::FormatId>), ExcelError>;
}

type ResolvedCallable = (
    Option<std::sync::Arc<dyn crate::function::Function>>,
    Option<crate::function::FnCaps>,
);

/// Immutable function snapshot shared by every interpreter in one evaluation request.
#[derive(Default)]
pub(crate) struct ResolvedFunctions {
    functions: Vec<Option<std::sync::Arc<dyn crate::function::Function>>>,
    slots: rustc_hash::FxHashMap<crate::engine::arena::StringId, usize>,
    revision: Option<(u64, Option<u64>, u64)>,
    ast_revision: Option<(u64, Option<u64>, u64, usize, u64)>,
    ast_functions: rustc_hash::FxHashMap<Box<str>, ResolvedCallable>,
    capabilities: Vec<Option<crate::function::FnCaps>>,
}

impl ResolvedFunctions {
    pub(crate) fn refresh(
        &mut self,
        store: &DataStore,
        provider: &dyn crate::traits::FunctionProvider,
    ) {
        let revision = (
            crate::function_registry::semantic_epoch_lock_free(),
            provider.planning_semantic_revision(),
            store.resolution_revision(),
        );
        if self.revision != Some(revision) || revision.1.is_none() {
            self.functions.clear();
            self.capabilities.clear();
            self.slots.clear();
            self.ast_revision = None;
        }
        // New names append without invalidating existing slots. Non-function
        // edits do not change the reset revision or allocate here.
        for id in store.function_names() {
            if self.slots.contains_key(&id) {
                continue;
            }
            let name = store.resolve_ast_string(id);
            let function = if name == CALL_NODE_NAME {
                None
            } else {
                provider.get_function("", name)
            };
            let caps = function.as_ref().map(|function| function.caps());
            self.slots.insert(id, self.functions.len());
            self.functions.push(function);
            self.capabilities.push(caps);
        }
        // Capture before resolving so racing registration invalidates next pass.
        self.revision = Some(revision);
    }

    /// Whether the table describes `store` and `provider` as they are now:
    /// same registry epoch, a known and unchanged provider revision, same
    /// arena reset revision, and every function name in the arena resolved.
    /// Callers outside an evaluation request (which refreshes the table) use
    /// this to decide between the table and live provider lookups.
    pub(crate) fn is_current(
        &self,
        store: &DataStore,
        provider: &dyn crate::traits::FunctionProvider,
    ) -> bool {
        let Some((epoch, Some(provider_revision), arena_revision)) = self.revision else {
            return false;
        };
        epoch == crate::function_registry::semantic_epoch_lock_free()
            && provider.planning_semantic_revision() == Some(provider_revision)
            && store.resolution_revision() == arena_revision
            && store
                .function_names()
                .all(|id| self.slots.contains_key(&id))
    }

    pub(crate) fn refresh_ast_functions<'n>(
        &mut self,
        store: &DataStore,
        provider: &dyn crate::traits::FunctionProvider,
        named: impl Iterator<Item = &'n ASTNode>,
        symbol_revision: u64,
    ) {
        let Some((epoch, provider_revision, arena_revision)) = self.revision else {
            return;
        };
        let revision = (
            epoch,
            provider_revision,
            arena_revision,
            self.functions.len(),
            symbol_revision,
        );
        if self.ast_revision == Some(revision) {
            return;
        }
        fn collect(node: &ASTNode, names: &mut std::collections::BTreeSet<String>) {
            match &node.node_type {
                ASTNodeType::Function { name, args } => {
                    names.insert(name.clone());
                    for arg in args {
                        collect(arg, names);
                    }
                }
                ASTNodeType::UnaryOp { expr, .. } => collect(expr, names),
                ASTNodeType::BinaryOp { left, right, .. } => {
                    collect(left, names);
                    collect(right, names);
                }
                ASTNodeType::Array(rows) => {
                    for node in rows.iter().flatten() {
                        collect(node, names);
                    }
                }
                _ => {}
            }
        }
        let mut names = std::collections::BTreeSet::new();
        let definitions_changed = self
            .ast_revision
            .is_none_or(|previous| previous.4 != symbol_revision);
        if definitions_changed {
            for node in named {
                collect(node, &mut names);
            }
            self.ast_functions.clear();
        }
        for (id, slot) in &self.slots {
            let name = store.resolve_ast_string(*id);
            if self.ast_functions.contains_key(name) {
                continue;
            }
            let function = self.functions.get(*slot).cloned().flatten();
            let caps = self.capabilities.get(*slot).copied().flatten();
            self.ast_functions.insert(name.into(), (function, caps));
        }
        for name in names {
            self.ast_functions
                .entry(name.into_boxed_str())
                .or_insert_with_key(|name| {
                    let function = provider.get_function("", name);
                    let caps = function.as_ref().map(|function| function.caps());
                    (function, caps)
                });
        }
        self.ast_revision = Some(revision);
    }

    /// Runtime snapshot lookup for run planners; intentionally not the provider's
    /// potentially distinct `get_function_for_planning` contract.
    pub(crate) fn get_named(&self, name: &str) -> Option<&dyn crate::function::Function> {
        self.ast_functions.get(name)?.0.as_deref()
    }

    pub(crate) fn get(
        &self,
        id: crate::engine::arena::StringId,
    ) -> Option<&dyn crate::function::Function> {
        self.functions
            .get(*self.slots.get(&id)?)
            .and_then(|f| f.as_deref())
    }
}

enum ResolvedFunction<'a> {
    Borrowed(&'a dyn crate::function::Function),
    Owned(std::sync::Arc<dyn crate::function::Function>),
}
impl std::ops::Deref for ResolvedFunction<'_> {
    type Target = dyn crate::function::Function;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(f) => *f,
            Self::Owned(f) => f.as_ref(),
        }
    }
}

pub struct Interpreter<'a> {
    pub context: &'a dyn EvaluationContext,
    resolved_functions: Option<&'a ResolvedFunctions>,
    arena_cells: Option<&'a dyn ArenaCellResolver>,
    current_sheet: &'a str,
    current_cell: Option<crate::CellRef>,
    local_env: LocalEnv,
    reference_row_delta: i64,
    reference_col_delta: i64,
    disable_ast_planner: bool,
    parameter_bindings: Option<InterpreterParameterBindings<'a>>,
}

impl<'a> Interpreter<'a> {
    pub fn new(context: &'a dyn EvaluationContext, current_sheet: &'a str) -> Self {
        Self {
            context,
            resolved_functions: None,
            arena_cells: None,
            current_sheet,
            current_cell: None,
            local_env: LocalEnv::default(),
            reference_row_delta: 0,
            reference_col_delta: 0,
            disable_ast_planner: false,
            parameter_bindings: None,
        }
    }

    pub fn new_with_cell(
        context: &'a dyn EvaluationContext,
        current_sheet: &'a str,
        cell: crate::CellRef,
    ) -> Self {
        Self {
            context,
            resolved_functions: None,
            arena_cells: None,
            current_sheet,
            current_cell: Some(cell),
            local_env: LocalEnv::default(),
            reference_row_delta: 0,
            reference_col_delta: 0,
            disable_ast_planner: false,
            parameter_bindings: None,
        }
    }

    pub(crate) fn with_arena_cells(mut self, resolver: &'a dyn ArenaCellResolver) -> Self {
        self.arena_cells = Some(resolver);
        self
    }

    pub(crate) fn with_resolved_functions(mut self, table: &'a ResolvedFunctions) -> Self {
        self.resolved_functions = Some(table);
        self
    }

    fn ast_function(&self, name: &str) -> Option<ResolvedFunction<'_>> {
        if let Some(table) = self.resolved_functions
            && let Some(function) = table.ast_functions.get(name)
        {
            return function.0.as_deref().map(ResolvedFunction::Borrowed);
        }
        self.context
            .get_function("", name)
            .map(ResolvedFunction::Owned)
    }

    pub(crate) fn ast_function_capabilities(&self, name: &str) -> Option<crate::function::FnCaps> {
        if let Some(table) = self.resolved_functions
            && let Some(caps) = table.ast_functions.get(name)
        {
            return caps.1;
        }
        self.context.function_capabilities("", name)
    }

    fn ast_function_arc(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        if ns.is_empty()
            && let Some(table) = self.resolved_functions
            && let Some(function) = table.ast_functions.get(name)
        {
            return function.0.clone();
        }
        self.context.get_function(ns, name)
    }

    fn arena_function(
        &self,
        id: crate::engine::arena::StringId,
        store: &DataStore,
    ) -> Option<ResolvedFunction<'_>> {
        if let Some(table) = self.resolved_functions {
            table.get(id).map(ResolvedFunction::Borrowed)
        } else {
            let name = store.resolve_ast_string(id);
            if name == CALL_NODE_NAME {
                return None;
            }
            self.context
                .get_function("", name)
                .map(ResolvedFunction::Owned)
        }
    }

    pub(crate) fn arena_function_capabilities(
        &self,
        id: crate::engine::arena::StringId,
        store: &DataStore,
    ) -> Option<crate::function::FnCaps> {
        if let Some(table) = self.resolved_functions {
            table
                .capabilities
                .get(*table.slots.get(&id)?)
                .copied()
                .flatten()
        } else {
            self.context
                .function_capabilities("", store.resolve_ast_string(id))
        }
    }

    pub fn current_sheet(&self) -> &'a str {
        self.current_sheet
    }

    pub fn local_env(&self) -> &LocalEnv {
        &self.local_env
    }

    pub(crate) fn with_current_cell(&self, cell: crate::CellRef) -> Self {
        Self {
            context: self.context,
            resolved_functions: self.resolved_functions,
            arena_cells: self.arena_cells,
            current_sheet: self.current_sheet,
            current_cell: Some(cell),
            local_env: self.local_env.clone(),
            reference_row_delta: self.reference_row_delta,
            reference_col_delta: self.reference_col_delta,
            disable_ast_planner: self.disable_ast_planner,
            parameter_bindings: self.parameter_bindings,
        }
    }

    pub fn with_local_env(&self, env: LocalEnv) -> Self {
        Self {
            context: self.context,
            resolved_functions: self.resolved_functions,
            arena_cells: self.arena_cells,
            current_sheet: self.current_sheet,
            current_cell: self.current_cell,
            local_env: env,
            reference_row_delta: self.reference_row_delta,
            reference_col_delta: self.reference_col_delta,
            disable_ast_planner: self.disable_ast_planner,
            parameter_bindings: self.parameter_bindings,
        }
    }

    pub(crate) fn with_parameter_bindings(
        &self,
        bindings: InterpreterParameterBindings<'a>,
    ) -> Self {
        Self {
            context: self.context,
            resolved_functions: self.resolved_functions,
            arena_cells: self.arena_cells,
            current_sheet: self.current_sheet,
            current_cell: self.current_cell,
            local_env: self.local_env.clone(),
            reference_row_delta: self.reference_row_delta,
            reference_col_delta: self.reference_col_delta,
            disable_ast_planner: self.disable_ast_planner,
            parameter_bindings: Some(bindings),
        }
    }

    fn effective_reference<'r>(
        &self,
        reference: &'r ReferenceType,
    ) -> Result<Cow<'r, ReferenceType>, ExcelError> {
        if self.reference_row_delta == 0 && self.reference_col_delta == 0 {
            return Ok(Cow::Borrowed(reference));
        }

        Ok(Cow::Owned(relocate_reference_for_offset(
            reference,
            self.reference_row_delta,
            self.reference_col_delta,
        )?))
    }

    fn resolve_local_reference(
        &self,
        reference: &ReferenceType,
    ) -> Option<crate::traits::CalcValue<'a>> {
        if self.local_env.is_empty() {
            return None;
        }
        let name = match reference {
            ReferenceType::NamedRange(name) => name,
            _ => return None,
        };
        match self.local_env.lookup(name)? {
            LocalBinding::Value(v) => Some(crate::traits::CalcValue::Scalar(v)),
            LocalBinding::ValueWithReference { value, .. } => {
                Some(crate::traits::CalcValue::Scalar(value))
            }
            LocalBinding::Callable(c) => Some(crate::traits::CalcValue::Callable(c)),
        }
    }

    fn resolve_local_callable(&self, name: &str) -> Option<Arc<dyn crate::traits::CustomCallable>> {
        if self.local_env.is_empty() {
            return None;
        }
        match self.local_env.lookup(name)? {
            LocalBinding::Callable(c) => Some(c),
            LocalBinding::Value(_) | LocalBinding::ValueWithReference { .. } => None,
        }
    }

    pub fn resolve_local_name(&self, name: &str) -> Option<LocalBinding> {
        self.local_env.lookup(name)
    }

    /// The spreadsheet reference a LET/LAMBDA local was bound to, when it was
    /// bound to a reference expression rather than a computed value.
    pub(crate) fn resolve_local_bound_reference(&self, name: &str) -> Option<ReferenceType> {
        if self.local_env.is_empty() {
            return None;
        }
        match self.local_env.lookup(name)? {
            LocalBinding::ValueWithReference { reference, .. } => Some(reference),
            LocalBinding::Value(_) | LocalBinding::Callable(_) => None,
        }
    }

    pub fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<crate::engine::range_view::RangeView<'c>, ExcelError> {
        self.context
            .resolve_range_view(reference, current_sheet)
            .map(|view| view.with_cell_budget(self.context.materialized_array_cells()))
    }

    /// Evaluate an AST node in a reference context and return a ReferenceType.
    /// This is used for range combinators (e.g., ":"), by-ref argument flows,
    /// and spill planning. Functions that can return references must set
    /// `FnCaps::RETURNS_REFERENCE` and override `eval_reference`.
    pub fn evaluate_ast_as_reference(&self, node: &ASTNode) -> Result<ReferenceType, ExcelError> {
        match &node.node_type {
            ASTNodeType::Reference { reference, .. } => {
                self.reference_for_current_offset(reference)
            }
            ASTNodeType::Function { name, args } => {
                if let Some(fun) = self.ast_function(name) {
                    // Build handles; allow function to decide reference semantics
                    let handles: smallvec::SmallVec<[ArgumentHandle; 4]> =
                        args.iter().map(|n| ArgumentHandle::new(n, self)).collect();
                    let fctx = DefaultFunctionContext::new_with_sheet(
                        self.context,
                        None,
                        self.current_sheet,
                    );
                    if let Some(res) = fun.eval_reference(&handles, &fctx) {
                        res
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("Function does not return a reference"))
                    }
                } else {
                    Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Unknown function: {name}")))
                }
            }
            ASTNodeType::BinaryOp { op, left, right } if *op == BinaryOperator::Range => {
                let lref = self.evaluate_ast_as_reference(left)?;
                let rref = self.evaluate_ast_as_reference(right)?;
                crate::reference::combine_references(&lref, &rref)
            }
            ASTNodeType::UnaryOp { op, expr } if *op == UnaryOperator::Spill => {
                self.ast_spill_reference(expr)
            }
            ASTNodeType::Array(_)
            | ASTNodeType::UnaryOp { .. }
            | ASTNodeType::BinaryOp { .. }
            | ASTNodeType::Call { .. }
            | ASTNodeType::Literal(_)
            | ASTNodeType::Omitted => Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Expression cannot be used as a reference")),
        }
    }

    pub(crate) fn try_evaluate_ast_as_reference(
        &self,
        node: &ASTNode,
    ) -> Option<Result<ReferenceType, ExcelError>> {
        let ASTNodeType::Function { name, args } = &node.node_type else {
            return Some(self.evaluate_ast_as_reference(node));
        };
        let fun = match self.ast_function(name) {
            Some(fun) => fun,
            None => {
                return Some(Err(ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Unknown function: {name}"))));
            }
        };
        let handles: smallvec::SmallVec<[ArgumentHandle; 4]> = args
            .iter()
            .map(|arg| ArgumentHandle::new(arg, self))
            .collect();
        let fctx = DefaultFunctionContext::new_with_sheet(self.context, None, self.current_sheet);
        fun.eval_reference(&handles, &fctx)
    }

    pub(crate) fn evaluate_arena_ast_as_reference(
        &self,
        node_id: AstNodeId,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<ReferenceType, ExcelError> {
        let node = data_store.get_node(node_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value).with_message("Missing AST node")
        })?;

        match node {
            AstNodeData::Reference { ref_type, .. } => {
                let ref_type = data_store.compact_ref(*ref_type);
                let reference =
                    data_store.reconstruct_reference_type_for_eval(ref_type, sheet_registry);
                self.reference_for_current_offset(&reference)
            }
            AstNodeData::Function { name_id, .. } => {
                let fun = self.arena_function(*name_id, data_store).ok_or_else(|| {
                    let name = data_store.resolve_ast_string(*name_id);
                    if name == CALL_NODE_NAME {
                        call_expression_error()
                    } else {
                        ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Unknown function: {name}"))
                    }
                })?;

                let args = data_store.get_args(node_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value).with_message("Missing function args")
                })?;

                let handles: smallvec::SmallVec<[ArgumentHandle; 4]> = args
                    .iter()
                    .copied()
                    .map(|arg_id| {
                        ArgumentHandle::new_arena(arg_id, self, data_store, sheet_registry)
                    })
                    .collect();

                let fctx =
                    DefaultFunctionContext::new_with_sheet(self.context, None, self.current_sheet);

                fun.eval_reference(&handles, &fctx).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Function does not return a reference")
                })?
            }
            AstNodeData::BinaryOp {
                operator,
                left_id,
                right_id,
            } => {
                let op = *operator;
                if op != BinaryOperator::Range {
                    return Err(ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Expression cannot be used as a reference"));
                }
                let lref =
                    self.evaluate_arena_ast_as_reference(*left_id, data_store, sheet_registry)?;
                let rref =
                    self.evaluate_arena_ast_as_reference(*right_id, data_store, sheet_registry)?;
                crate::reference::combine_references(&lref, &rref)
            }
            AstNodeData::UnaryOp { operator, expr_id } if *operator == UnaryOperator::Spill => {
                self.arena_spill_reference(*expr_id, data_store, sheet_registry)
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Expression cannot be used as a reference")),
        }
    }

    /// The anchor a spill operand names: a single-cell reference or a name,
    /// including a LET/LAMBDA local bound to one. Anything else is `#REF!`.
    fn spill_anchor_operand(&self, reference: &ReferenceType) -> Result<ReferenceType, ExcelError> {
        let anchor = match reference {
            ReferenceType::NamedRange(name) if self.resolve_local_name(name).is_some() => self
                .resolve_local_bound_reference(name)
                .ok_or_else(spill_operand_error)?,
            _ => self
                .reference_for_current_offset(reference)
                .map_err(|_| spill_operand_error())?,
        };
        match anchor {
            ReferenceType::Cell { .. } | ReferenceType::NamedRange(_) => Ok(anchor),
            _ => Err(spill_operand_error()),
        }
    }

    /// Resolve a spill reference (`A1#`, `ANCHORARRAY(A1)`) whose operand is
    /// the written `reference`, to the anchor's current spill rectangle.
    pub(crate) fn resolve_spill_reference(
        &self,
        reference: &ReferenceType,
    ) -> Result<ReferenceType, ExcelError> {
        let anchor = self.spill_anchor_operand(reference)?;
        self.context
            .resolve_spill_reference(&anchor, self.current_sheet)
    }

    /// The `#` operator on an AST operand: only a written reference qualifies.
    pub(crate) fn ast_spill_reference(
        &self,
        operand: &ASTNode,
    ) -> Result<ReferenceType, ExcelError> {
        match &operand.node_type {
            ASTNodeType::Reference { reference, .. } => self.resolve_spill_reference(reference),
            _ => Err(spill_operand_error()),
        }
    }

    /// The `#` operator on an arena operand: only a written reference qualifies.
    pub(crate) fn arena_spill_reference(
        &self,
        operand: AstNodeId,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<ReferenceType, ExcelError> {
        match data_store.get_node(operand) {
            Some(AstNodeData::Reference { ref_type, .. }) => {
                let ref_type = data_store.compact_ref(*ref_type);
                let reference =
                    data_store.reconstruct_reference_type_for_eval(ref_type, sheet_registry);
                self.resolve_spill_reference(&reference)
            }
            _ => Err(spill_operand_error()),
        }
    }

    pub(crate) fn try_evaluate_arena_ast_as_reference(
        &self,
        node_id: AstNodeId,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Option<Result<ReferenceType, ExcelError>> {
        let node = match data_store.get_node(node_id) {
            Some(node) => node,
            None => {
                return Some(Err(
                    ExcelError::new(ExcelErrorKind::Value).with_message("Missing AST node")
                ));
            }
        };
        let AstNodeData::Function { name_id, .. } = node else {
            return Some(self.evaluate_arena_ast_as_reference(node_id, data_store, sheet_registry));
        };
        let fun = match self.arena_function(*name_id, data_store) {
            Some(fun) => fun,
            None => {
                let name = data_store.resolve_ast_string(*name_id);
                if name == CALL_NODE_NAME {
                    return Some(Err(call_expression_error()));
                }
                return Some(Err(ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Unknown function: {name}"))));
            }
        };
        let args = match data_store.get_args(node_id) {
            Some(args) => args,
            None => {
                return Some(Err(
                    ExcelError::new(ExcelErrorKind::Value).with_message("Missing function args")
                ));
            }
        };
        let handles: smallvec::SmallVec<[ArgumentHandle; 4]> = args
            .iter()
            .copied()
            .map(|arg_id| ArgumentHandle::new_arena(arg_id, self, data_store, sheet_registry))
            .collect();
        let fctx = DefaultFunctionContext::new_with_sheet(self.context, None, self.current_sheet);
        fun.eval_reference(&handles, &fctx)
    }

    /* ===================  public  =================== */
    pub fn evaluate_ast(&self, node: &ASTNode) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        self.evaluate_ast_uncached(node)
    }

    pub(crate) fn evaluate_ast_with_offset(
        &self,
        node: &ASTNode,
        row_delta: i64,
        col_delta: i64,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        let offset = Self {
            context: self.context,
            resolved_functions: self.resolved_functions,
            arena_cells: self.arena_cells,
            current_sheet: self.current_sheet,
            current_cell: self.current_cell,
            local_env: self.local_env.clone(),
            reference_row_delta: row_delta,
            reference_col_delta: col_delta,
            disable_ast_planner: true,
            parameter_bindings: self.parameter_bindings,
        };
        offset.evaluate_ast_uncached(node)
    }

    pub(crate) fn reference_for_current_offset(
        &self,
        reference: &ReferenceType,
    ) -> Result<ReferenceType, ExcelError> {
        self.effective_reference(reference)
            .map(|reference| reference.into_owned())
    }

    /// Evaluate a formula given as a template plus offset (a compressed
    /// family member, or a formula on its own cell at offset zero).
    pub(crate) fn evaluate_formula_view(
        &self,
        view: crate::engine::graph::FormulaView,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        if view.row_delta == 0 && view.col_delta == 0 {
            self.evaluate_arena_ast(view.template, data_store, sheet_registry)
        } else {
            self.evaluate_arena_ast_with_offset(
                view.template,
                view.row_delta,
                view.col_delta,
                data_store,
                sheet_registry,
            )
        }
    }

    pub(crate) fn evaluate_arena_ast_with_offset(
        &self,
        node_id: AstNodeId,
        row_delta: i64,
        col_delta: i64,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        let offset = Self {
            context: self.context,
            resolved_functions: self.resolved_functions,
            arena_cells: self.arena_cells,
            current_sheet: self.current_sheet,
            current_cell: self.current_cell,
            local_env: self.local_env.clone(),
            reference_row_delta: row_delta,
            reference_col_delta: col_delta,
            disable_ast_planner: true,
            parameter_bindings: self.parameter_bindings,
        };
        offset.evaluate_arena_ast(node_id, data_store, sheet_registry)
    }

    #[inline]
    fn annotated(
        value: LiteralValue,
        format: Option<crate::format::FormatId>,
    ) -> crate::traits::CalcValue<'a> {
        match format {
            Some(format) => crate::traits::CalcValue::AnnotatedScalar(value, format),
            None => crate::traits::CalcValue::Scalar(value),
        }
    }

    fn annotate_cell_value(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> crate::traits::CalcValue<'a> {
        match self
            .context
            .resolve_cell_format(sheet, row, col, self.current_sheet)
        {
            Some(format) => crate::traits::CalcValue::AnnotatedScalar(value, format),
            None => crate::traits::CalcValue::Scalar(value),
        }
    }

    fn binary_format(
        &self,
        op: char,
        left: Option<crate::format::FormatId>,
        right: Option<crate::format::FormatId>,
    ) -> Option<crate::format::FormatId> {
        use formualizer_common::numfmt::FormatClass;
        let class =
            |id: Option<crate::format::FormatId>| id.and_then(|id| self.context.format_class(id));
        let left = class(left);
        let right = class(right);
        let is_plain = |class: &Option<FormatClass>| {
            matches!(
                class,
                None | Some(FormatClass::General | FormatClass::Number { .. })
            )
        };
        // This table is intentionally closed. LibreOffice measurement establishes
        // Date+Time and Date+Percent; unlisted pairs (including Date+Date,
        // Duration+Date, Date+Currency, DateTime+Time, and Date+Text) drop the
        // annotation rather than guessing a display class.
        match (op, left.as_ref(), right.as_ref()) {
            ('+', Some(FormatClass::Date), Some(FormatClass::Time))
            | ('+', Some(FormatClass::Time), Some(FormatClass::Date)) => {
                Some(crate::format::FormatId::DATETIME)
            }
            ('+', Some(FormatClass::Date), Some(FormatClass::Percent { .. }))
            | ('+', Some(FormatClass::Percent { .. }), Some(FormatClass::Date)) => {
                Some(crate::format::FormatId::DATE)
            }
            ('+' | '-', Some(FormatClass::Date), r) if is_plain(&r.cloned()) => {
                Some(crate::format::FormatId::DATE)
            }
            ('+', l, Some(FormatClass::Date)) if is_plain(&l.cloned()) => {
                Some(crate::format::FormatId::DATE)
            }
            ('+' | '-', Some(FormatClass::Time), r) if is_plain(&r.cloned()) => {
                Some(crate::format::FormatId::TIME)
            }
            ('+', l, Some(FormatClass::Time)) if is_plain(&l.cloned()) => {
                Some(crate::format::FormatId::TIME)
            }
            ('+' | '-', Some(FormatClass::DateTime), r) if is_plain(&r.cloned()) => {
                Some(crate::format::FormatId::DATETIME)
            }
            ('+', l, Some(FormatClass::DateTime)) if is_plain(&l.cloned()) => {
                Some(crate::format::FormatId::DATETIME)
            }
            ('+' | '-', Some(FormatClass::Duration), r) if is_plain(&r.cloned()) => {
                Some(crate::format::FormatId::DURATION)
            }
            ('+', l, Some(FormatClass::Duration)) if is_plain(&l.cloned()) => {
                Some(crate::format::FormatId::DURATION)
            }
            _ => None,
        }
    }

    fn annotate_numeric_result(
        &self,
        value: LiteralValue,
        format: Option<crate::format::FormatId>,
    ) -> crate::traits::CalcValue<'a> {
        match (value, format) {
            (value @ LiteralValue::Number(_), Some(format)) => {
                crate::traits::CalcValue::AnnotatedScalar(value, format)
            }
            (value, _) => crate::traits::CalcValue::Scalar(value),
        }
    }

    /// A unary operator other than `@` on an evaluated operand (shared by
    /// the AST walk and the elementwise lift).
    pub(crate) fn apply_unary_op(
        &self,
        op: UnaryOperator,
        expr: crate::traits::CalcValue<'a>,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        // For now, materialize for operators. Future: virtual range ops.
        let v = expr.into_literal();
        match v {
            LiteralValue::Array(arr) => self
                .map_array(arr, |cell| self.eval_unary_scalar(op, cell))
                .map(crate::traits::CalcValue::Scalar),
            other => self
                .eval_unary_scalar(op, other)
                .map(crate::traits::CalcValue::Scalar),
        }
    }

    /// A binary operator other than `:` on evaluated operands and their
    /// format annotations (shared by the AST walk and the elementwise lift).
    pub(crate) fn apply_binary_op(
        &self,
        op: BinaryOperator,
        left: LiteralValue,
        left_format: Option<crate::format::FormatId>,
        right: LiteralValue,
        right_format: Option<crate::format::FormatId>,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        if matches!(
            op,
            BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::Greater
                | BinaryOperator::Less
                | BinaryOperator::GreaterEqual
                | BinaryOperator::LessEqual
        ) {
            return self
                .compare(op, left, right)
                .map(crate::traits::CalcValue::Scalar);
        }

        match op {
            BinaryOperator::Add | BinaryOperator::Subtract => {
                let byte = if op == BinaryOperator::Add {
                    b'+'
                } else {
                    b'-'
                };
                let value = self.numeric_binary(left, right, byte)?;
                if left_format.is_none() && right_format.is_none() {
                    // The closed temporal-format algebra cannot produce an
                    // annotation from two unformatted operands. Avoid both its
                    // classification/matching and the numeric annotation pass.
                    Ok(crate::traits::CalcValue::Scalar(value))
                } else {
                    Ok(self.annotate_numeric_result(
                        value,
                        self.binary_format(char::from(byte), left_format, right_format),
                    ))
                }
            }
            BinaryOperator::Multiply => self
                .numeric_binary(left, right, b'*')
                .map(crate::traits::CalcValue::Scalar),
            BinaryOperator::Divide => self
                .numeric_binary(left, right, b'/')
                .map(crate::traits::CalcValue::Scalar),
            BinaryOperator::Power => self
                .numeric_binary(left, right, b'^')
                .map(crate::traits::CalcValue::Scalar),
            BinaryOperator::Concat => self
                .concat_values(left, right)
                .map(crate::traits::CalcValue::Scalar),
            _ => {
                Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message(format!("Binary op '{op}'")))
            }
        }
    }

    /// Resolve only a direct compact range, retaining local/recording fallbacks.
    /// The argument caller attaches its context's cancellation and cell budget.
    pub(crate) fn resolve_arena_argument_range(
        &self,
        node_id: AstNodeId,
        data_store: &DataStore,
    ) -> Option<Result<crate::engine::range_view::RangeView<'a>, ExcelError>> {
        let resolver = self.arena_cells?;
        let cell = self.current_cell?;
        if !self.local_env.is_empty() {
            return None;
        }
        let AstNodeData::Reference { ref_type, .. } = data_store.get_node(node_id)? else {
            return None;
        };
        let reference = data_store.compact_ref(*ref_type);
        if !matches!(reference, CompactRefType::Range { .. }) {
            return None;
        }
        Some(resolver.resolve_arena_range(
            reference,
            cell.sheet_id,
            self.reference_row_delta,
            self.reference_col_delta,
        ))
    }

    pub(crate) fn evaluate_arena_ast(
        &self,
        node_id: AstNodeId,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        let node = data_store.get_node(node_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value).with_message("Missing AST node")
        })?;

        match node {
            AstNodeData::Literal(vref) => {
                if let Some(bindings) = self.parameter_bindings
                    && let Some(slot_id) = bindings.literal_slots_by_node.get(&node_id)
                    && let Some(value) = bindings.literal_values.get(slot_id.0 as usize)
                {
                    return Ok(crate::traits::CalcValue::Scalar(value.clone()));
                }
                Ok(crate::traits::CalcValue::Scalar(
                    data_store.retrieve_value(*vref),
                ))
            }
            AstNodeData::Omitted => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0))),
            AstNodeData::Reference { ref_type, .. } => {
                let ref_type = data_store.compact_ref(*ref_type);
                if self.local_env.is_empty()
                    && matches!(ref_type, CompactRefType::Range { .. })
                    && let (Some(resolver), Some(cell)) = (self.arena_cells, self.current_cell)
                {
                    let view = resolver
                        .resolve_arena_range(
                            ref_type,
                            cell.sheet_id,
                            self.reference_row_delta,
                            self.reference_col_delta,
                        )?
                        .with_cancel_token(self.context.cancellation_token())
                        .with_cell_budget(self.context.materialized_array_cells());
                    return Ok(crate::traits::CalcValue::Range(view));
                }
                if self.local_env.is_empty()
                    && let CompactRefType::Cell {
                        sheet,
                        row,
                        col,
                        row_abs,
                        col_abs,
                    } = ref_type
                    && *row > 0
                    && *col > 0
                {
                    let row = shift_axis_for_offset(*row, self.reference_row_delta, *row_abs)?;
                    let col = shift_axis_for_offset(*col, self.reference_col_delta, *col_abs)?;
                    if let (Some(resolver), Some(cell)) = (self.arena_cells, self.current_cell) {
                        let (value, format) =
                            resolver.resolve_arena_cell(*sheet, cell.sheet_id, row, col)?;
                        return Ok(Self::annotated(value, format));
                    }
                    let sheet_name = match sheet {
                        Some(SheetKey::Id(id)) => Some(sheet_registry.name(*id)),
                        Some(SheetKey::Name(name_id)) => {
                            Some(data_store.resolve_ast_string(*name_id))
                        }
                        None => None,
                    };
                    let (value, format) = self.context.resolve_cell_reference_value_formatted(
                        sheet_name,
                        row,
                        col,
                        self.current_sheet,
                    )?;
                    Ok(Self::annotated(value, format))
                } else {
                    let reference =
                        data_store.reconstruct_reference_type_for_eval(ref_type, sheet_registry);
                    let reference = self.effective_reference(&reference)?;
                    if let Some(local) = self.resolve_local_reference(&reference) {
                        return Ok(local);
                    }
                    self.eval_reference_to_calc(&reference)
                }
            }
            AstNodeData::UnaryOp { operator, expr_id } => {
                let op = *operator;
                if op == UnaryOperator::Spill {
                    let reference =
                        self.arena_spill_reference(*expr_id, data_store, sheet_registry)?;
                    return self.eval_reference_to_calc(&reference);
                }
                if op == UnaryOperator::ImplicitIntersection {
                    // Prefer reference-aware implicit intersection so we don't depend on
                    // RangeView absolute coordinates (important for lightweight test contexts).
                    // The reference is placed at this cell (a shared family's
                    // arena reference is in its origin frame), as on the
                    // plain-reference path above.
                    if let Some(AstNodeData::Reference { ref_type, .. }) =
                        data_store.get_node(*expr_id)
                    {
                        let reference = data_store.reconstruct_reference_type_for_eval(
                            data_store.compact_ref(*ref_type),
                            sheet_registry,
                        );
                        let v = match self.effective_reference(&reference) {
                            Ok(reference) => self.implicit_intersection_from_reference(&reference),
                            Err(e) => LiteralValue::Error(e),
                        };
                        return Ok(crate::traits::CalcValue::Scalar(v));
                    }

                    let expr = self.evaluate_arena_ast(*expr_id, data_store, sheet_registry)?;
                    let v = self.eval_implicit_intersection_calc(expr);
                    return Ok(crate::traits::CalcValue::Scalar(v));
                }
                let expr = self.evaluate_arena_ast(*expr_id, data_store, sheet_registry)?;
                self.apply_unary_op(op, expr)
            }
            AstNodeData::BinaryOp {
                operator,
                left_id,
                right_id,
            } => {
                let op = *operator;
                if op == BinaryOperator::Range {
                    let lref =
                        self.evaluate_arena_ast_as_reference(*left_id, data_store, sheet_registry)?;
                    let rref = self.evaluate_arena_ast_as_reference(
                        *right_id,
                        data_store,
                        sheet_registry,
                    )?;
                    return match crate::reference::combine_references(&lref, &rref) {
                        Ok(_r) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Ref).with_message(
                                "Reference produced by ':' cannot be used directly as a value",
                            ),
                        ))),
                        Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
                    };
                }

                let left_calc = self.evaluate_arena_ast(*left_id, data_store, sheet_registry)?;
                let left_format = left_calc.format_id();
                let left = left_calc.into_literal();
                let right_calc = self.evaluate_arena_ast(*right_id, data_store, sheet_registry)?;
                let right_format = right_calc.format_id();
                let right = right_calc.into_literal();
                self.apply_binary_op(op, left, left_format, right, right_format)
            }
            AstNodeData::Array { .. } => {
                let (rows, cols, elements) =
                    data_store.get_array_elems(node_id).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::Value).with_message("Invalid array")
                    })?;

                let rows_usize = rows as usize;
                let cols_usize = cols as usize;
                let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows_usize);
                for r in 0..rows_usize {
                    let mut row = Vec::with_capacity(cols_usize);
                    for c in 0..cols_usize {
                        let idx = r * cols_usize + c;
                        if let Some(&elem_id) = elements.get(idx) {
                            row.push(
                                self.evaluate_arena_ast(elem_id, data_store, sheet_registry)?
                                    .into_literal(),
                            );
                        }
                    }
                    out.push(row);
                }

                Ok(crate::traits::CalcValue::Range(
                    crate::engine::range_view::RangeView::from_owned_rows(
                        out,
                        self.context.date_system(),
                    ),
                ))
            }
            AstNodeData::Function { name_id, .. } => {
                if let Some(bindings) = self.parameter_bindings
                    && let Some(values) = bindings.invariant_values
                    && let Some((value, format)) = values.get(&node_id)
                {
                    return Ok(Self::annotated(value.clone(), *format));
                }
                let function = self.arena_function(*name_id, data_store);
                if function.is_none() && data_store.resolve_ast_string(*name_id) == CALL_NODE_NAME {
                    return Err(call_expression_error());
                }
                let args = data_store.get_args(node_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value).with_message("Missing function args")
                })?;

                if let Some(fun) = function {
                    let handles: smallvec::SmallVec<[ArgumentHandle; 4]> = args
                        .iter()
                        .copied()
                        .map(|arg_id| {
                            ArgumentHandle::new_arena(arg_id, self, data_store, sheet_registry)
                        })
                        .collect();

                    let fctx = DefaultFunctionContext::new_with_sheet(
                        self.context,
                        self.current_cell,
                        self.current_sheet,
                    );

                    return fun.dispatch(&handles, &fctx);
                }

                let name = data_store.resolve_ast_string(*name_id);
                if let Some(callable) = self.resolve_local_callable(name) {
                    let mut eval_args = Vec::with_capacity(args.len());
                    let mut arg_references = Vec::with_capacity(args.len());
                    for arg_id in args {
                        // Same rule as the AST twin above.
                        arg_references.push(
                            ArgumentHandle::new_arena(*arg_id, self, data_store, sheet_registry)
                                .bound_reference_in_env(&self.local_env),
                        );
                        eval_args.push(
                            self.evaluate_arena_ast(*arg_id, data_store, sheet_registry)?
                                .into_literal(),
                        );
                    }
                    return callable.invoke_with_references(self, &eval_args, &arg_references);
                }

                Err(ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Unknown function: {name}")))
            }
        }
    }

    fn evaluate_ast_uncached(
        &self,
        node: &ASTNode,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        if self.disable_ast_planner {
            return self.eval_tree_uncached(node);
        }

        // Plan-aware evaluation: build a plan for this node and execute accordingly.
        // Provide the planner with a lightweight range-dimension probe and function lookup
        // so it can select chunked reduction and arg-parallel strategies where appropriate.
        let current_sheet = self.current_sheet.to_string();
        let range_probe = |reference: &ReferenceType| {
            probe_range_dimensions(self.context, &current_sheet, reference)
        };
        let fn_lookup = |ns: &str, name: &str| self.ast_function_arc(ns, name);

        let mut planner = crate::planner::Planner::new(crate::planner::PlanConfig::default())
            .with_range_probe(&range_probe)
            .with_function_lookup(&fn_lookup);
        let plan = planner.plan(node);
        self.eval_with_plan(node, &plan.root)
    }

    fn eval_tree_uncached(
        &self,
        node: &ASTNode,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        match &node.node_type {
            ASTNodeType::Literal(v) => Ok(crate::traits::CalcValue::Scalar(v.clone())),
            ASTNodeType::Omitted => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0))),
            ASTNodeType::Reference { reference, .. } => self.eval_ast_reference_to_calc(reference),
            ASTNodeType::UnaryOp { op, expr } if *op == UnaryOperator::Spill => {
                self.eval_reference_to_calc(&self.ast_spill_reference(expr)?)
            }
            ASTNodeType::UnaryOp { op, expr } => self
                .eval_unary(*op, expr)
                .map(crate::traits::CalcValue::Scalar),
            ASTNodeType::BinaryOp { op, left, right } => self.eval_binary(*op, left, right),
            ASTNodeType::Function { name, args } => self.eval_function_to_calc(name, args),
            ASTNodeType::Call { .. } => Err(call_expression_error()),
            ASTNodeType::Array(rows) => self.eval_array_literal_to_calc(rows),
        }
    }

    fn eval_with_plan(
        &self,
        node: &ASTNode,
        plan_node: &crate::planner::PlanNode,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        match &node.node_type {
            ASTNodeType::Literal(v) => Ok(crate::traits::CalcValue::Scalar(v.clone())),
            ASTNodeType::Omitted => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0))),
            ASTNodeType::Reference { reference, .. } => self.eval_ast_reference_to_calc(reference),
            ASTNodeType::UnaryOp { op, expr } if *op == UnaryOperator::Spill => {
                self.eval_reference_to_calc(&self.ast_spill_reference(expr)?)
            }
            ASTNodeType::UnaryOp { op, expr } => {
                // For now, reuse existing unary implementation (which recurses).
                // In a later phase, we can map plan_node.children[0].
                self.eval_unary(*op, expr)
                    .map(crate::traits::CalcValue::Scalar)
            }
            ASTNodeType::BinaryOp { op, left, right } => self.eval_binary(*op, left, right),
            ASTNodeType::Function { name, args } => {
                let strategy = plan_node.strategy;
                if let Some(fun) = self.ast_function(name) {
                    use crate::function::FnCaps;
                    use crate::planner::ExecStrategy;
                    let caps = fun.caps();

                    // Short-circuit or volatile: always sequential
                    if caps.contains(FnCaps::SHORT_CIRCUIT) || caps.contains(FnCaps::VOLATILE) {
                        return self.eval_function_to_calc(name, args);
                    }

                    // Windowed/chunked strategies are handled by the unified `eval()` path.

                    // Arg-parallel: prewarm subexpressions and then dispatch
                    if matches!(strategy, ExecStrategy::ArgParallel)
                        && caps.contains(FnCaps::PARALLEL_ARGS)
                    {
                        // Sequential prewarm of subexpressions (safe without Sync bounds)
                        for arg in args {
                            match &arg.node_type {
                                ASTNodeType::Reference { reference, .. } => {
                                    if let Ok(reference) = self.effective_reference(reference) {
                                        let _ = self
                                            .context
                                            .resolve_range_view(&reference, self.current_sheet);
                                    }
                                }
                                _ => {
                                    let _ = self.evaluate_ast(arg);
                                }
                            }
                        }
                        return self.eval_function_to_calc(name, args);
                    }

                    // Default path
                    return self.eval_function_to_calc(name, args);
                }
                self.eval_function_to_calc(name, args)
            }
            ASTNodeType::Call { .. } => Err(call_expression_error()),
            ASTNodeType::Array(rows) => self.eval_array_literal_to_calc(rows),
        }
    }

    /* ===================  reference  =================== */
    fn eval_ast_reference_to_calc(
        &self,
        reference: &ReferenceType,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        if !self.local_env.is_empty() {
            let reference = self.effective_reference(reference)?;
            if let Some(local) = self.resolve_local_reference(&reference) {
                return Ok(local);
            }
            return self.eval_reference_to_calc(&reference);
        }

        if let ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } = reference
        {
            let row = shift_axis_for_offset(*row, self.reference_row_delta, *row_abs)?;
            let col = shift_axis_for_offset(*col, self.reference_col_delta, *col_abs)?;
            let (value, format) = self.context.resolve_cell_reference_value_formatted(
                sheet.as_deref(),
                row,
                col,
                self.current_sheet,
            )?;
            return Ok(Self::annotated(value, format));
        }

        let reference = self.effective_reference(reference)?;
        self.eval_reference_to_calc(&reference)
    }

    fn eval_reference_to_calc(
        &self,
        reference: &ReferenceType,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        if let ReferenceType::Cell {
            sheet, row, col, ..
        } = reference
        {
            let value = self.context.resolve_cell_reference_value(
                sheet.as_deref(),
                *row,
                *col,
                self.current_sheet,
            )?;
            return Ok(self.annotate_cell_value(sheet.as_deref(), *row, *col, value));
        }

        let view = self
            .context
            .resolve_range_view(reference, self.current_sheet)?
            .with_cancel_token(self.context.cancellation_token())
            .with_cell_budget(self.context.materialized_array_cells());
        Ok(crate::traits::CalcValue::Range(view))
    }

    fn eval_reference(&self, reference: &ReferenceType) -> Result<LiteralValue, ExcelError> {
        self.eval_reference_to_calc(reference)
            .map(|cv| cv.into_literal())
    }

    /* ===================  unary ops  =================== */
    fn eval_unary(&self, op: UnaryOperator, expr: &ASTNode) -> Result<LiteralValue, ExcelError> {
        if op == UnaryOperator::ImplicitIntersection {
            if let ASTNodeType::Reference { reference, .. } = &expr.node_type {
                let reference = self.effective_reference(reference)?;
                return Ok(self.implicit_intersection_from_reference(&reference));
            }

            let cv = self.evaluate_ast(expr)?;
            return Ok(self.eval_implicit_intersection_calc(cv));
        }

        let v = self.evaluate_ast(expr)?.into_literal();
        match v {
            LiteralValue::Array(arr) => {
                self.map_array(arr, |cell| self.eval_unary_scalar(op, cell))
            }
            other => self.eval_unary_scalar(op, other),
        }
    }

    fn eval_unary_scalar(
        &self,
        op: UnaryOperator,
        v: LiteralValue,
    ) -> Result<LiteralValue, ExcelError> {
        match op {
            // Excel/LibreOffice treat unary `+` as a pass-through (identity) operator,
            // not as a numeric coercion. `=+"2014F"` returns the text "2014F"; only the
            // unary `-` form coerces operands to numbers. The `=+A1` idiom is common in
            // finance models (Lotus 1-2-3 carry-over) and must preserve text labels.
            UnaryOperator::Plus => Ok(v),
            UnaryOperator::Minus => self.apply_number_unary(v, b'-'),
            UnaryOperator::Percent => self.apply_number_unary(v, b'%'),
            _ => {
                Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(format!("Unary op '{op}'")))
            }
        }
    }

    fn eval_implicit_intersection_calc(&self, cv: crate::traits::CalcValue<'a>) -> LiteralValue {
        let (cur_r0, cur_c0) = match self.current_cell {
            Some(cell) => (cell.coord.row() as usize, cell.coord.col() as usize),
            None => (0usize, 0usize),
        };

        match cv {
            crate::traits::CalcValue::Scalar(v)
            | crate::traits::CalcValue::AnnotatedScalar(v, _) => match v {
                LiteralValue::Array(arr) => {
                    if arr.is_empty() || arr.first().map(|r| r.is_empty()).unwrap_or(true) {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    arr[0][0].clone()
                }
                other => other,
            },
            crate::traits::CalcValue::Range(rv) => {
                if rv.is_empty() {
                    return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                }

                // Array results (array literals and many dynamic-array functions) are materialized
                // into an owned RangeView with a temporary backing sheet ("__tmp").
                // For explicit @, interpret these as anchored at the formula cell and select the
                // top-left element.
                if rv.sheet_name() == "__tmp" {
                    return rv.get_cell(0, 0);
                }

                if let Some(v) = rv.as_1x1() {
                    return v;
                }

                let (rows, cols) = rv.dims();
                let sr = rv.start_row();
                let sc = rv.start_col();
                let er = rv.end_row();
                let ec = rv.end_col();

                // Excel-compatible implicit intersection (simplified):
                // - Nx1: pick by row
                // - 1xM: pick by column
                // - NxM: pick by (row,col)
                if cols == 1 {
                    if cur_r0 < sr || cur_r0 > er {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    let rel_r = cur_r0 - sr;
                    return rv.get_cell(rel_r, 0);
                }

                if rows == 1 {
                    if cur_c0 < sc || cur_c0 > ec {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    let rel_c = cur_c0 - sc;
                    return rv.get_cell(0, rel_c);
                }

                if cur_r0 < sr || cur_r0 > er || cur_c0 < sc || cur_c0 > ec {
                    return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                }
                let rel_r = cur_r0 - sr;
                let rel_c = cur_c0 - sc;
                rv.get_cell(rel_r, rel_c)
            }
            crate::traits::CalcValue::Callable(_) => LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Calc).with_message("LAMBDA value must be invoked"),
            ),
        }
    }

    fn implicit_intersection_from_reference(&self, reference: &ReferenceType) -> LiteralValue {
        let (cur_r1, cur_c1) = match self.current_cell {
            Some(cell) => (
                cell.coord.row().saturating_add(1),
                cell.coord.col().saturating_add(1),
            ),
            None => (1u32, 1u32),
        };

        match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(self.current_sheet);
                match self
                    .context
                    .resolve_cell_reference(Some(sheet_name), *row, *col)
                {
                    Ok(v) => v,
                    Err(e) => LiteralValue::Error(e),
                }
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(self.current_sheet);

                // Open-ended references (`A:A`, `21:21`, `A5:A`) intersect by
                // their declared sheet bounds, not by a used-region view: a
                // whole row's intersection with column G is G on that row even
                // when the row's used cells end before G.
                const SHEET_ROWS: u32 = 1_048_576;
                const SHEET_COLS: u32 = 16_384;
                let (sr, sc, er, ec) = (
                    start_row.unwrap_or(1),
                    start_col.unwrap_or(1),
                    end_row.unwrap_or(SHEET_ROWS),
                    end_col.unwrap_or(SHEET_COLS),
                );

                // Normalize bounds (A10:A1 is legal syntax; treat as swapped).
                let (mut sr, mut er) = (sr, er);
                let (mut sc, mut ec) = (sc, ec);
                if sr > er {
                    std::mem::swap(&mut sr, &mut er);
                }
                if sc > ec {
                    std::mem::swap(&mut sc, &mut ec);
                }

                let pick = if sc == ec {
                    // Column vector: intersect by row
                    if cur_r1 < sr || cur_r1 > er {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    (cur_r1, sc)
                } else if sr == er {
                    // Row vector: intersect by column
                    if cur_c1 < sc || cur_c1 > ec {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    (sr, cur_c1)
                } else {
                    // 2D: require both axes
                    if cur_r1 < sr || cur_r1 > er || cur_c1 < sc || cur_c1 > ec {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    (cur_r1, cur_c1)
                };

                match self
                    .context
                    .resolve_cell_reference(Some(sheet_name), pick.0, pick.1)
                {
                    Ok(v) => v,
                    Err(e) => LiteralValue::Error(e),
                }
            }
            // A bounded external range intersects by its declared bounds, like
            // a range on a worksheet; its values come from the source view.
            ReferenceType::External(ext) => {
                let formualizer_parse::parser::ExternalRefKind::Range {
                    start_row: Some(sr),
                    start_col: Some(sc),
                    end_row: Some(er),
                    end_col: Some(ec),
                    ..
                } = ext.kind
                else {
                    return match self.eval_reference_to_calc(reference) {
                        Ok(cv) => self.eval_implicit_intersection_calc(cv),
                        Err(e) => LiteralValue::Error(e),
                    };
                };
                let (sr, er) = (sr.min(er), sr.max(er));
                let (sc, ec) = (sc.min(ec), sc.max(ec));
                let (row, col) = if sc == ec {
                    if cur_r1 < sr || cur_r1 > er {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    (cur_r1, sc)
                } else if sr == er {
                    if cur_c1 < sc || cur_c1 > ec {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    (sr, cur_c1)
                } else {
                    if cur_r1 < sr || cur_r1 > er || cur_c1 < sc || cur_c1 > ec {
                        return LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value));
                    }
                    (cur_r1, cur_c1)
                };
                match self.eval_reference_to_calc(reference) {
                    Ok(crate::traits::CalcValue::Range(rv)) => {
                        rv.get_cell((row - sr) as usize, (col - sc) as usize)
                    }
                    Ok(cv) => self.eval_implicit_intersection_calc(cv),
                    Err(e) => LiteralValue::Error(e),
                }
            }
            // Named ranges / tables: fall back to materializing and intersecting.
            other => {
                let cv = match self.eval_reference_to_calc(other) {
                    Ok(cv) => cv,
                    Err(e) => return LiteralValue::Error(e),
                };
                self.eval_implicit_intersection_calc(cv)
            }
        }
    }

    fn apply_number_unary(&self, v: LiteralValue, op: u8) -> Result<LiteralValue, ExcelError> {
        match crate::coercion::to_arithmetic_number_with_locale(
            &v,
            &self.context.locale(),
            self.context.date_system(),
        ) {
            Ok(n) => match unary_f64(op, n) {
                Ok(n2) => Ok(LiteralValue::Number(n2)),
                Err(e) => Ok(LiteralValue::Error(e)),
            },
            Err(e) => Ok(LiteralValue::Error(e)),
        }
    }

    /* ===================  binary ops  =================== */
    fn eval_binary(
        &self,
        op: BinaryOperator,
        left_node: &ASTNode,
        right_node: &ASTNode,
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        let left_calc = self.evaluate_ast(left_node)?;
        let left_format = left_calc.format_id();
        let left = left_calc.into_literal();
        let right_calc = self.evaluate_ast(right_node)?;
        let right_format = right_calc.format_id();
        let right = right_calc.into_literal();
        if op == BinaryOperator::Range {
            let left_ref = self.evaluate_ast_as_reference(left_node)?;
            let right_ref = self.evaluate_ast_as_reference(right_node)?;
            return match crate::reference::combine_references(&left_ref, &right_ref) {
                Ok(_) => Err(ExcelError::new(ExcelErrorKind::Ref)
                    .with_message("Reference produced by ':' cannot be used directly as a value")),
                Err(error) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(error))),
            };
        }
        self.apply_binary_op(op, left, left_format, right, right_format)
    }

    /* ===================  function calls  =================== */
    fn eval_function_to_calc(
        &self,
        name: &str,
        args: &[ASTNode],
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        if let Some(fun) = self.ast_function(name) {
            let handles: smallvec::SmallVec<[ArgumentHandle; 4]> =
                args.iter().map(|n| ArgumentHandle::new(n, self)).collect();
            // Use the function's built-in dispatch method with a narrow FunctionContext
            let fctx = DefaultFunctionContext::new_with_sheet(
                self.context,
                self.current_cell,
                self.current_sheet,
            );
            return fun.dispatch(&handles, &fctx);
        }

        if let Some(callable) = self.resolve_local_callable(name) {
            let mut eval_args = Vec::with_capacity(args.len());
            let mut arg_references = Vec::with_capacity(args.len());
            for arg in args {
                // Same rule `LET` uses to keep a bound range: a
                // syntactic reference expression, or a local that itself
                // carries a bound reference; nothing else.
                arg_references
                    .push(ArgumentHandle::new(arg, self).bound_reference_in_env(&self.local_env));
                eval_args.push(self.evaluate_ast(arg)?.into_literal());
            }
            return callable.invoke_with_references(self, &eval_args, &arg_references);
        }

        // Include the function name in the error message for better debugging
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Name).with_message(format!("Unknown function: {name}")),
        )))
    }

    fn eval_function(&self, name: &str, args: &[ASTNode]) -> Result<LiteralValue, ExcelError> {
        self.eval_function_to_calc(name, args)
            .map(|cv| cv.into_literal())
    }

    pub fn function_context(&self, cell_ref: Option<&CellRef>) -> DefaultFunctionContext<'_> {
        DefaultFunctionContext::new_with_sheet(self.context, cell_ref.cloned(), self.current_sheet)
    }

    /* ===================  array literal  =================== */
    fn eval_array_literal_to_calc(
        &self,
        rows: &[Vec<ASTNode>],
    ) -> Result<crate::traits::CalcValue<'a>, ExcelError> {
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let mut r = Vec::with_capacity(row.len());
            for cell in row {
                r.push(self.evaluate_ast(cell)?.into_literal());
            }
            out.push(r);
        }
        Ok(crate::traits::CalcValue::Range(
            crate::engine::range_view::RangeView::from_owned_rows(out, self.context.date_system()),
        ))
    }

    fn eval_array_literal(&self, rows: &[Vec<ASTNode>]) -> Result<LiteralValue, ExcelError> {
        self.eval_array_literal_to_calc(rows)
            .map(|cv| cv.into_literal())
    }

    /// `+ - * / ^` on two operands: arrays broadcast, each element pair is
    /// coerced to numbers (first error wins, left first) and combined by
    /// [`arith_f64`].
    fn numeric_binary(
        &self,
        left: LiteralValue,
        right: LiteralValue,
        op: u8,
    ) -> Result<LiteralValue, ExcelError> {
        self.broadcast_apply(left, right, |l, r| {
            let a = crate::coercion::to_arithmetic_number_with_locale(
                &l,
                &self.context.locale(),
                self.context.date_system(),
            );
            let b = crate::coercion::to_arithmetic_number_with_locale(
                &r,
                &self.context.locale(),
                self.context.date_system(),
            );
            match (a, b) {
                (Ok(a), Ok(b)) => match arith_f64(op, a, b) {
                    Ok(n) => Ok(LiteralValue::Number(n)),
                    Err(e) => Ok(LiteralValue::Error(e)),
                },
                (Err(e), _) | (_, Err(e)) => Ok(LiteralValue::Error(e)),
            }
        })
    }

    /// The `&` operator. Like the arithmetic operators it works element by
    /// element over arrays and ranges, and an error operand is the result (the
    /// left one when both are errors) instead of being spelled into the text.
    fn concat_values(
        &self,
        left: LiteralValue,
        right: LiteralValue,
    ) -> Result<LiteralValue, ExcelError> {
        fn concat_scalar(
            left: LiteralValue,
            right: LiteralValue,
        ) -> Result<LiteralValue, ExcelError> {
            Ok(match (left, right) {
                (LiteralValue::Error(error), _) | (_, LiteralValue::Error(error)) => {
                    LiteralValue::Error(error)
                }
                (left, right) => LiteralValue::Text(format!(
                    "{}{}",
                    crate::coercion::to_text_invariant(&left),
                    crate::coercion::to_text_invariant(&right)
                )),
            })
        }
        // Scalars (every member of a lifted family) skip the broadcast.
        if matches!(left, LiteralValue::Array(_)) || matches!(right, LiteralValue::Array(_)) {
            self.broadcast_apply(left, right, concat_scalar)
        } else {
            concat_scalar(left, right)
        }
    }

    fn map_array<F>(&self, arr: Vec<Vec<LiteralValue>>, f: F) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(LiteralValue) -> Result<LiteralValue, ExcelError> + Copy,
    {
        let mut out = Vec::with_capacity(arr.len());
        for row in arr {
            let mut new_row = Vec::with_capacity(row.len());
            for cell in row {
                new_row.push(match f(cell) {
                    Ok(v) => v,
                    Err(e) => LiteralValue::Error(e),
                });
            }
            out.push(new_row);
        }
        Ok(LiteralValue::Array(out))
    }

    fn combine_arrays<F>(
        &self,
        l: Vec<Vec<LiteralValue>>,
        r: Vec<Vec<LiteralValue>>,
        f: F,
    ) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(LiteralValue, LiteralValue) -> Result<LiteralValue, ExcelError> + Copy,
    {
        // Use strict broadcasting across dimensions
        let l_shape = (l.len(), l.first().map(|r| r.len()).unwrap_or(0));
        let r_shape = (r.len(), r.first().map(|r| r.len()).unwrap_or(0));
        let target = match broadcast_shape(&[l_shape, r_shape]) {
            Ok(s) => s,
            Err(e) => return Ok(LiteralValue::Error(e)),
        };
        // A column and a row broadcast to their product: check it before
        // allocating (the inputs were checked when they were materialised).
        if let Some(e) = crate::engine::resource_ledger::materialized_cells_exceed(
            target.0,
            target.1,
            self.context.materialized_array_cells(),
        ) {
            return Ok(LiteralValue::Error(e));
        }

        let mut out = Vec::with_capacity(target.0);
        for i in 0..target.0 {
            let mut row = Vec::with_capacity(target.1);
            for j in 0..target.1 {
                let (li, lj) = project_index((i, j), l_shape);
                let (ri, rj) = project_index((i, j), r_shape);
                let lv = l
                    .get(li)
                    .and_then(|r| r.get(lj))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                let rv = r
                    .get(ri)
                    .and_then(|r| r.get(rj))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                row.push(match f(lv, rv) {
                    Ok(v) => v,
                    Err(e) => LiteralValue::Error(e),
                });
            }
            out.push(row);
        }
        Ok(LiteralValue::Array(out))
    }

    fn broadcast_apply<F>(
        &self,
        left: LiteralValue,
        right: LiteralValue,
        f: F,
    ) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(LiteralValue, LiteralValue) -> Result<LiteralValue, ExcelError> + Copy,
    {
        use LiteralValue::*;
        match (left, right) {
            (Array(l), Array(r)) => self.combine_arrays(l, r, f),
            (Array(arr), v) => {
                let shape_l = (arr.len(), arr.first().map(|r| r.len()).unwrap_or(0));
                let shape_r = (1usize, 1usize);
                let target = match broadcast_shape(&[shape_l, shape_r]) {
                    Ok(s) => s,
                    Err(e) => return Ok(LiteralValue::Error(e)),
                };
                let mut out = Vec::with_capacity(target.0);
                for i in 0..target.0 {
                    let mut row = Vec::with_capacity(target.1);
                    for j in 0..target.1 {
                        let (li, lj) = project_index((i, j), shape_l);
                        let lv = arr
                            .get(li)
                            .and_then(|r| r.get(lj))
                            .cloned()
                            .unwrap_or(LiteralValue::Empty);
                        row.push(match f(lv, v.clone()) {
                            Ok(vv) => vv,
                            Err(e) => LiteralValue::Error(e),
                        });
                    }
                    out.push(row);
                }
                Ok(LiteralValue::Array(out))
            }
            (v, Array(arr)) => {
                let shape_l = (1usize, 1usize);
                let shape_r = (arr.len(), arr.first().map(|r| r.len()).unwrap_or(0));
                let target = match broadcast_shape(&[shape_l, shape_r]) {
                    Ok(s) => s,
                    Err(e) => return Ok(LiteralValue::Error(e)),
                };
                let mut out = Vec::with_capacity(target.0);
                for i in 0..target.0 {
                    let mut row = Vec::with_capacity(target.1);
                    for j in 0..target.1 {
                        let (ri, rj) = project_index((i, j), shape_r);
                        let rv = arr
                            .get(ri)
                            .and_then(|r| r.get(rj))
                            .cloned()
                            .unwrap_or(LiteralValue::Empty);
                        row.push(match f(v.clone(), rv) {
                            Ok(vv) => vv,
                            Err(e) => LiteralValue::Error(e),
                        });
                    }
                    out.push(row);
                }
                Ok(LiteralValue::Array(out))
            }
            (l, r) => f(l, r),
        }
    }

    /* ---------- coercion helpers ---------- */
    fn coerce_number(&self, v: &LiteralValue) -> Result<f64, ExcelError> {
        coercion::to_number_lenient(v)
    }

    fn coerce_text(&self, v: &LiteralValue) -> String {
        coercion::to_text_invariant(v)
    }

    /* ---------- comparison ---------- */
    fn compare(
        &self,
        op: BinaryOperator,
        left: LiteralValue,
        right: LiteralValue,
    ) -> Result<LiteralValue, ExcelError> {
        use LiteralValue::*;
        if matches!(left, Error(_)) {
            return Ok(left);
        }
        if matches!(right, Error(_)) {
            return Ok(right);
        }

        // arrays: element‑wise with broadcasting
        match (left, right) {
            (Array(l), Array(r)) => self.combine_arrays(l, r, |a, b| self.compare(op, a, b)),
            (Array(arr), v) => self.broadcast_apply(Array(arr), v, |a, b| self.compare(op, a, b)),
            (v, Array(arr)) => self.broadcast_apply(v, Array(arr), |a, b| self.compare(op, a, b)),
            (l, r) => {
                let res = match (l, r) {
                    // Same-rank fast paths. These agree with `cmp_ranked` below
                    // and exist only to avoid the rank/`Empty` dance.
                    (Number(a), Number(b)) => self.cmp_f64(a, b, op),
                    (Int(a), Int(b)) => self.cmp_f64(a as f64, b as f64, op),
                    (Int(a), Number(b)) => self.cmp_f64(a as f64, b, op),
                    (Number(a), Int(b)) => self.cmp_f64(a, b as f64, op),
                    (Boolean(a), Boolean(b)) => {
                        self.cmp_f64(if a { 1.0 } else { 0.0 }, if b { 1.0 } else { 0.0 }, op)
                    }
                    (Text(a), Text(b)) => self.cmp_text(&a, &b, op),
                    (a, b) => self.cmp_ranked(a, b, op),
                };
                Ok(LiteralValue::Boolean(res))
            }
        }
    }

    /// Mixed-type relational comparison.
    ///
    /// Excel does not coerce across types in `<`, `<=`, `>`, `>=`, `=`, `<>`.
    /// It applies a pure type rank, identical for all six operators:
    ///
    /// ```text
    /// number  <  text  <  boolean
    /// ```
    ///
    /// Only when both operands share a rank are they compared within the type
    /// (measured in Excel for Mac 16.105.3, en_US). Consequences of the rank: `TRUE=1` is FALSE, `1<TRUE` is TRUE,
    /// `"5"=5` is FALSE (numeric text never coerces), `"5">4` is TRUE,
    /// `"TRUE"=TRUE` is FALSE, and `"Z"<FALSE` is TRUE.
    ///
    /// Errors never reach here: `compare` short-circuits them above.
    fn cmp_ranked(&self, l: LiteralValue, r: LiteralValue, op: BinaryOperator) -> bool {
        use LiteralValue::*;

        // A never-written cell is polymorphic rather than ranked: it adopts the
        // other operand's type and behaves as that type's zero value, so
        // `Z1=0`, `Z1=""`, `Z1=FALSE` and `Z1<TRUE` are all TRUE.
        let (l, r) = match (l, r) {
            (Empty, Empty) => (Number(0.0), Number(0.0)),
            (Empty, r) => (empty_as_zero_of(&r), r),
            (l, Empty) => {
                let e = empty_as_zero_of(&l);
                (l, e)
            }
            (l, r) => (l, r),
        };

        let (lr, rr) = (excel_type_rank(&l), excel_type_rank(&r));
        if lr != rr {
            return self.cmp_f64(lr as f64, rr as f64, op);
        }

        match (l, r) {
            (Text(a), Text(b)) => self.cmp_text(&a, &b, op),
            (Boolean(a), Boolean(b)) => {
                self.cmp_f64(if a { 1.0 } else { 0.0 }, if b { 1.0 } else { 0.0 }, op)
            }
            // Same rank, number class: Int/Number and the date/time/duration
            // serial-bearing variants. Text is never parsed here.
            (a, b) => {
                let an = crate::coercion::to_number_strict(&a).ok();
                let bn = crate::coercion::to_number_strict(&b).ok();
                if let (Some(a), Some(b)) = (an, bn) {
                    self.cmp_f64(a, b, op)
                } else {
                    // Only `Pending` (and any future non-numeric, non-text,
                    // non-boolean variant) lands here; keep the legacy text
                    // fallback so those pairs behave as before.
                    self.cmp_text(
                        &crate::coercion::to_text_invariant(&a),
                        &crate::coercion::to_text_invariant(&b),
                        op,
                    )
                }
            }
        }
    }

    fn cmp_f64(&self, a: f64, b: f64, op: BinaryOperator) -> bool {
        cmp_f64(a, b, op)
    }
    fn cmp_text(&self, a: &str, b: &str, op: BinaryOperator) -> bool {
        let loc = self.context.locale();
        let (a, b) = (loc.fold_case_invariant(a), loc.fold_case_invariant(b));
        self.cmp_f64(a.cmp(&b) as i32 as f64, 0.0, op)
    }
}

/// A numeric comparison operator on two numbers (shared by the walk and
/// the typed lift).
#[inline]
pub(crate) fn cmp_f64(a: f64, b: f64, op: BinaryOperator) -> bool {
    match op {
        BinaryOperator::Equal => a == b,
        BinaryOperator::NotEqual => a != b,
        BinaryOperator::Greater => a > b,
        BinaryOperator::Less => a < b,
        BinaryOperator::GreaterEqual => a >= b,
        BinaryOperator::LessEqual => a <= b,
        _ => unreachable!(),
    }
}

/// `+ - * / ^` on two numbers after coercion: the result, or the error
/// the operator yields (`#DIV/0!`, `#NUM!` for a negative base with a
/// fractional exponent or a non-finite result). Shared by the walk and
/// the typed lift, so both are bit-identical.
#[inline]
pub(crate) fn arith_f64(op: u8, a: f64, b: f64) -> Result<f64, ExcelError> {
    let v = match op {
        b'+' => a + b,
        b'-' => a - b,
        b'*' => a * b,
        b'/' => {
            if b == 0.0 {
                return Err(ExcelError::from_error_string("#DIV/0!"));
            }
            a / b
        }
        b'^' => {
            // Excel domain: negative base with non-integer exponent -> #NUM!
            if a < 0.0 && b.fract() != 0.0 {
                return Err(ExcelError::new_num());
            }
            a.powf(b)
        }
        _ => unreachable!("arithmetic operator"),
    };
    crate::coercion::sanitize_numeric(v)
}

/// Unary `-` and `%` on a number after coercion (shared by the walk and
/// the typed lift).
#[inline]
pub(crate) fn unary_f64(op: u8, n: f64) -> Result<f64, ExcelError> {
    crate::coercion::sanitize_numeric(match op {
        b'-' => -n,
        b'%' => n / 100.0,
        _ => unreachable!("unary operator"),
    })
}

/// Excel's relational type rank: `number < text < boolean`.
///
/// The number class covers `Int`, `Number` and the serial-bearing temporal
/// variants (`Date`, `DateTime`, `Time`, `Duration`), because on a sheet a date
/// cell *is* a number. `Empty` is deliberately not ranked here — it is
/// polymorphic and must be resolved against the other operand before ranking
/// (see `Interpreter::cmp_ranked`). `Error` never reaches ranking.
fn excel_type_rank(v: &LiteralValue) -> u8 {
    match v {
        LiteralValue::Text(_) => 1,
        LiteralValue::Boolean(_) => 2,
        _ => 0,
    }
}

/// The zero value of `other`'s type, used to give a blank operand the type of
/// whatever it is compared against: `0` against a number, `""` against text,
/// `FALSE` against a boolean.
fn empty_as_zero_of(other: &LiteralValue) -> LiteralValue {
    match other {
        LiteralValue::Text(_) => LiteralValue::Text(String::new()),
        LiteralValue::Boolean(_) => LiteralValue::Boolean(false),
        _ => LiteralValue::Number(0.0),
    }
}

fn relocate_reference_for_offset(
    reference: &ReferenceType,
    row_delta: i64,
    col_delta: i64,
) -> Result<ReferenceType, ExcelError> {
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => Ok(ReferenceType::Cell {
            sheet: sheet.clone(),
            row: shift_axis_for_offset(*row, row_delta, *row_abs)?,
            col: shift_axis_for_offset(*col, col_delta, *col_abs)?,
            row_abs: *row_abs,
            col_abs: *col_abs,
        }),
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
        } => Ok(ReferenceType::Range {
            sheet: sheet.clone(),
            start_row: shift_optional_axis_for_offset(*start_row, row_delta, *start_row_abs)?,
            start_col: shift_optional_axis_for_offset(*start_col, col_delta, *start_col_abs)?,
            end_row: shift_optional_axis_for_offset(*end_row, row_delta, *end_row_abs)?,
            end_col: shift_optional_axis_for_offset(*end_col, col_delta, *end_col_abs)?,
            start_row_abs: *start_row_abs,
            start_col_abs: *start_col_abs,
            end_row_abs: *end_row_abs,
            end_col_abs: *end_col_abs,
        }),
        // Defined names are placement-invariant: a relocated copy of the
        // formula references the same name, resolved at evaluation time.
        ReferenceType::NamedRange(name) => Ok(ReferenceType::NamedRange(name.clone())),
        ReferenceType::Table(_)
        | ReferenceType::Cell3D { .. }
        | ReferenceType::Range3D { .. }
        | ReferenceType::External(_) => Err(unsupported_reference_relocation_error()),
    }
}

fn shift_optional_axis_for_offset(
    value: Option<u32>,
    delta: i64,
    is_absolute: bool,
) -> Result<Option<u32>, ExcelError> {
    value
        .map(|value| shift_axis_for_offset(value, delta, is_absolute))
        .transpose()
}

pub(crate) fn shift_axis_for_offset(
    value: u32,
    delta: i64,
    is_absolute: bool,
) -> Result<u32, ExcelError> {
    if is_absolute {
        return Ok(value);
    }
    let shifted = i64::from(value) + delta;
    if shifted < 1 || shifted > i64::from(u32::MAX) {
        return Err(unsupported_reference_relocation_error());
    }
    Ok(shifted as u32)
}

fn unsupported_reference_relocation_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::Ref)
        .with_message("Unsupported reference relocation for FormulaPlane span evaluation")
}

#[cfg(test)]
mod format_algebra_tests {
    use super::*;

    #[test]
    fn compact_argument_ranges_match_owned_resolution() -> Result<(), Box<dyn std::error::Error>> {
        use crate::traits::ResolvedArgument;
        let mut config = EvalConfig::default();
        config.evaluation_budgets.scratch.materialized_array_cells = Some(4);
        let mut engine = Engine::new(TestWorkbook::new(), config);
        let sheet_id = engine.add_sheet("Other")?;
        engine.set_cell_value("Other", 2, 2, LiteralValue::Number(17.0))?;
        engine.set_cell_value("Other", 4, 3, LiteralValue::Text("end".into()))?;
        let template = formualizer_parse::parser::parse("=A1:C3")?;
        let cell = crate::CellRef::new(
            sheet_id,
            crate::reference::Coord::from_excel(1, 1, true, true),
        );
        for qualified in [false, true] {
            for open_rows in [false, true] {
                for open_cols in [false, true] {
                    for absolute in [false, true] {
                        for delta in [-2, 0, 2] {
                            let reference = ReferenceType::Range {
                                sheet: qualified.then(|| "Other".to_string()),
                                start_row: (!open_rows).then_some(1),
                                start_col: (!open_cols).then_some(1),
                                end_row: (!open_rows).then_some(3),
                                end_col: (!open_cols).then_some(3),
                                start_row_abs: absolute,
                                start_col_abs: absolute,
                                end_row_abs: absolute,
                                end_col_abs: absolute,
                            };
                            let mut ast = template.clone();
                            ast.node_type = ASTNodeType::Reference {
                                original: reference.to_string(),
                                reference,
                            };
                            let mut store = DataStore::new();
                            let registry = engine.graph.sheet_reg();
                            let id = store.store_ast(&ast, registry);
                            let mut slow =
                                Interpreter::new(&engine, "Other").with_current_cell(cell);
                            slow.reference_row_delta = delta;
                            slow.reference_col_delta = delta;
                            let fast = slow.with_current_cell(cell).with_arena_cells(&engine);
                            assert!(fast.resolve_arena_argument_range(id, &store).is_some());
                            assert!(slow.resolve_arena_argument_range(id, &store).is_none());
                            let a = ArgumentHandle::new_arena(id, &fast, &store, registry)
                                .resolve_once();
                            let b = ArgumentHandle::new_arena(id, &slow, &store, registry)
                                .resolve_once();
                            match (a, b) {
                                (
                                    Ok(ResolvedArgument::Range(a)),
                                    Ok(ResolvedArgument::Range(b)),
                                ) => {
                                    assert_eq!(a.dims(), b.dims());
                                    assert_eq!(
                                        (a.start_row(), a.start_col()),
                                        (b.start_row(), b.start_col())
                                    );
                                    assert_eq!(
                                        a.materialization_error().map(|e| e.kind),
                                        b.materialization_error().map(|e| e.kind)
                                    );
                                    let (rows, cols) = a.dims();
                                    for row in 0..rows.min(8) {
                                        for col in 0..cols.min(8) {
                                            assert_eq!(a.get_cell(row, col), b.get_cell(row, col));
                                        }
                                    }
                                }
                                (
                                    Ok(ResolvedArgument::ReferenceError(a)),
                                    Ok(ResolvedArgument::ReferenceError(b)),
                                ) => assert_eq!(a, b),
                                (Err(a), Err(b)) => assert_eq!(a, b),
                                (a, b) => assert_eq!(format!("{a:?}"), format!("{b:?}")),
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn compact_ranges_match_owned_resolution_after_relocation() -> Result<(), ExcelError> {
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let sheet_id = engine.add_sheet("Other")?;
        engine.set_cell_value("Other", 2, 2, LiteralValue::Number(17.0))?;
        engine.set_cell_value("Other", 4, 3, LiteralValue::Text("end".into()))?;
        for qualified in [false, true] {
            for open_rows in [false, true] {
                for open_cols in [false, true] {
                    for absolute in [false, true] {
                        for delta in [-2, 0, 2] {
                            let compact = CompactRefType::Range {
                                sheet: qualified.then_some(SheetKey::Id(sheet_id)),
                                start_row: if open_rows { 0 } else { 1 },
                                start_col: if open_cols { 0 } else { 1 },
                                end_row: if open_rows { u32::MAX } else { 3 },
                                end_col: if open_cols { u32::MAX } else { 3 },
                                start_row_abs: absolute,
                                start_col_abs: absolute,
                                end_row_abs: absolute,
                                end_col_abs: absolute,
                            };
                            let owned = ReferenceType::Range {
                                sheet: qualified.then(|| "Other".to_string()),
                                start_row: (!open_rows).then_some(1),
                                start_col: (!open_cols).then_some(1),
                                end_row: (!open_rows).then_some(3),
                                end_col: (!open_cols).then_some(3),
                                start_row_abs: absolute,
                                start_col_abs: absolute,
                                end_row_abs: absolute,
                                end_col_abs: absolute,
                            };
                            let expected = relocate_reference_for_offset(&owned, delta, delta)
                                .and_then(|r| {
                                    crate::traits::EvaluationContext::resolve_range_view(
                                        &engine, &r, "Other",
                                    )
                                });
                            let actual =
                                engine.resolve_arena_range(&compact, sheet_id, delta, delta);
                            match (actual, expected) {
                                (Ok(a), Ok(b)) => {
                                    assert_eq!(a.dims(), b.dims());
                                    assert_eq!(
                                        (a.start_row(), a.start_col()),
                                        (b.start_row(), b.start_col())
                                    );
                                    let (rows, cols) = a.dims();
                                    for row in 0..rows.min(8) {
                                        for col in 0..cols.min(8) {
                                            assert_eq!(a.get_cell(row, col), b.get_cell(row, col));
                                        }
                                    }
                                }
                                (Err(a), Err(b)) => assert_eq!(a.kind, b.kind),
                                (a, b) => assert_eq!(format!("{a:?}"), format!("{b:?}")),
                            }
                        }
                    }
                }
            }
        }
        engine.rename_sheet(sheet_id, "Renamed")?;
        assert!(
            engine
                .arrow_sheet_by_id(sheet_id)
                .is_some_and(|s| s.name.as_ref() == "Renamed")
        );
        assert!(engine.arrow_sheet_by_id_mut(sheet_id).is_some());
        let other = engine.add_sheet("Third")?;
        engine.sheet_store_mut().sheets.reverse();
        assert!(
            engine
                .arrow_sheet_by_id(sheet_id)
                .is_some_and(|s| s.name.as_ref() == "Renamed")
        );
        engine.remove_sheet(other)?;
        assert!(engine.arrow_sheet_by_id(sheet_id).is_some());
        Ok(())
    }

    use crate::engine::{EvalConfig, eval::Engine};
    use crate::format::FormatId;
    use crate::test_workbook::TestWorkbook;

    #[test]
    fn binary_fastpath_preserves_values_and_format_annotations() {
        let engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let interpreter = Interpreter::new(&engine, "Sheet1");
        let values = [
            LiteralValue::Number(2.5),
            LiteralValue::Int(2),
            LiteralValue::Text("3".into()),
            LiteralValue::Text("not numeric".into()),
            LiteralValue::Boolean(true),
            LiteralValue::Empty,
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)),
            LiteralValue::Array(vec![vec![LiteralValue::Number(1.0)]]),
        ];
        let formats = [
            None,
            Some(FormatId::DATE),
            Some(FormatId::TIME),
            Some(FormatId(49)),
        ];
        for (op, byte) in [
            (BinaryOperator::Add, b'+'),
            (BinaryOperator::Subtract, b'-'),
        ] {
            for left in &values {
                for right in &values {
                    for left_format in formats {
                        for right_format in formats {
                            let expected = interpreter
                                .numeric_binary(left.clone(), right.clone(), byte)
                                .map(|value| {
                                    interpreter.annotate_numeric_result(
                                        value,
                                        interpreter.binary_format(
                                            char::from(byte),
                                            left_format,
                                            right_format,
                                        ),
                                    )
                                });
                            let actual = interpreter.apply_binary_op(
                                op,
                                left.clone(),
                                left_format,
                                right.clone(),
                                right_format,
                            );
                            match (actual, expected) {
                                (Ok(actual), Ok(expected)) => {
                                    assert_eq!(actual.format_id(), expected.format_id());
                                    assert_eq!(actual.into_literal(), expected.into_literal());
                                }
                                (Err(actual), Err(expected)) => assert_eq!(actual, expected),
                                _ => panic!("operator result kind changed"),
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn temporal_binary_format_algebra_pins_positive_and_negative_cases() {
        let engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let interpreter = Interpreter::new(&engine, "Sheet1");

        assert_eq!(
            interpreter.binary_format('+', Some(FormatId::DATE), Some(FormatId::TIME)),
            Some(FormatId::DATETIME)
        );
        assert_eq!(
            interpreter.binary_format('+', Some(FormatId::DATE), Some(FormatId(9))),
            Some(FormatId::DATE),
            "Date + Percent follows the measured temporal-wins rule"
        );
        assert_eq!(
            interpreter.binary_format('-', Some(FormatId::DATE), Some(FormatId::DATE)),
            None,
            "Date - Date is an unformatted duration in days"
        );
        assert_eq!(
            interpreter.binary_format('+', Some(FormatId::DATE), Some(FormatId(49))),
            None,
            "Date + Text must not acquire a temporal annotation"
        );
        for (left, right) in [
            (FormatId::DATE, FormatId::DATE),
            (FormatId::DURATION, FormatId::DATE),
            (FormatId::DATE, FormatId(5)),
            (FormatId::DATETIME, FormatId::TIME),
        ] {
            assert_eq!(
                interpreter.binary_format('+', Some(left), Some(right)),
                None
            );
        }
    }
}

#[cfg(test)]
mod resolved_function_tests {
    use super::*;
    use crate::function::Function;
    use crate::traits::FunctionProvider;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    struct TestFunction;
    impl Function for TestFunction {
        fn name(&self) -> &'static str {
            "FN_TABLE_GLOBAL_REFRESH_TEST"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _: &'c [ArgumentHandle<'a, 'b>],
            _: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(1.0)))
        }
    }
    #[derive(Default)]
    struct Provider {
        generation: AtomicU64,
        calls: AtomicUsize,
        function: RwLock<Option<Arc<dyn Function>>>,
    }
    impl FunctionProvider for Provider {
        fn planning_semantic_revision(&self) -> Option<u64> {
            Some(self.generation.load(Ordering::Acquire))
        }
        fn function_capabilities(&self, _: &str, _: &str) -> Option<crate::function::FnCaps> {
            self.function.read().unwrap().as_ref().map(|f| f.caps())
        }
        fn get_function(&self, _: &str, _: &str) -> Option<Arc<dyn Function>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.function.read().unwrap().clone()
        }
    }
    fn arena(name: &str) -> (DataStore, crate::engine::arena::StringId) {
        let mut store = DataStore::new();
        let registry = SheetRegistry::new();
        let ast = ASTNode::new(
            ASTNodeType::Function {
                name: name.into(),
                args: vec![],
            },
            None,
        );
        let node = store.store_ast(&ast, &registry);
        let Some(AstNodeData::Function { name_id, .. }) = store.get_node(node) else {
            panic!("function node")
        };
        let id = *name_id;
        (store, id)
    }
    #[test]
    fn function_table_tracks_distinct_names_not_interner_size() {
        let mut store = DataStore::new();
        let registry = SheetRegistry::new();
        for row in 1..=200_000 {
            store.store_ast(
                &formualizer_parse::parser::parse(format!("=A{row}+1")).unwrap(),
                &registry,
            );
        }
        store.store_ast(
            &formualizer_parse::parser::parse("=ABS(1)").unwrap(),
            &registry,
        );
        let mut table = ResolvedFunctions::default();
        table.refresh(&store, &Provider::default());
        assert_eq!(table.functions.len(), 1);
        assert_eq!(table.capabilities.len(), 1);
        assert_eq!(table.slots.len(), 1);
    }

    #[test]
    fn formula_edits_preserve_existing_function_slots() {
        let provider = Provider::default();
        let (mut store, id) = arena("EXISTING");
        let mut table = ResolvedFunctions::default();
        table.refresh(&store, &provider);
        let slot = table.slots[&id];
        let revision = table.revision;
        store.store_ast(
            &formualizer_parse::parser::parse("=EXISTING()+1").unwrap(),
            &SheetRegistry::new(),
        );
        table.refresh(&store, &provider);
        // Unrelated registry tests may advance the global epoch concurrently.
        // Slot reuse is guaranteed only while that semantic revision is stable.
        if table.revision == revision {
            assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        }
        let previous_calls = provider.calls.load(Ordering::Relaxed);
        let previous_revision = table.revision;
        store.store_ast(
            &formualizer_parse::parser::parse("=NEW_NAME()").unwrap(),
            &SheetRegistry::new(),
        );
        table.refresh(&store, &provider);
        if table.revision == previous_revision {
            assert_eq!(provider.calls.load(Ordering::Relaxed), previous_calls + 1);
        }
        if table.revision == revision {
            assert_eq!(table.slots[&id], slot);
        }
        assert_eq!(table.functions.len(), 2);
    }

    #[test]
    fn unversioned_provider_is_resolved_fresh_each_request() {
        struct Unversioned(Provider);
        impl FunctionProvider for Unversioned {
            fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn Function>> {
                self.0.get_function(ns, name)
            }
        }
        let provider = Unversioned(Provider::default());
        let (store, id) = arena("CHANGING");
        let mut table = ResolvedFunctions::default();
        table.refresh(&store, &provider);
        assert!(table.get(id).is_none());
        *provider.0.function.write().unwrap() = Some(Arc::new(TestFunction));
        table.refresh(&store, &provider);
        assert!(table.get(id).is_some());
        let previous = table.functions[0].clone().unwrap();
        *provider.0.function.write().unwrap() = Some(Arc::new(TestFunction));
        table.refresh(&store, &provider);
        assert!(!Arc::ptr_eq(
            &previous,
            table.functions[0].as_ref().unwrap()
        ));
    }

    #[test]
    fn hits_and_misses_are_read_only_and_local_generations_refresh() {
        let provider = Provider::default();
        let (store, id) = arena("MISSING_THEN_REGISTERED");
        let mut table = ResolvedFunctions::default();
        table.refresh(&store, &provider);
        for _ in 0..10 {
            assert!(table.get(id).is_none());
        }
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        *provider.function.write().unwrap() = Some(Arc::new(TestFunction));
        provider.generation.fetch_add(1, Ordering::Release);
        // Registration during a pass cannot mutate the published snapshot.
        assert!(table.get(id).is_none());
        table.refresh(&store, &provider);
        assert!(table.get(id).is_some());
        let calls = provider.calls.load(Ordering::Relaxed);
        for _ in 0..10 {
            assert!(table.get(id).is_some());
        }
        assert_eq!(provider.calls.load(Ordering::Relaxed), calls);
        *provider.function.write().unwrap() = None;
        provider.generation.fetch_add(1, Ordering::Release);
        assert!(table.get(id).is_some());
        table.refresh(&store, &provider);
        assert!(table.get(id).is_none());
    }
    #[test]
    fn global_epoch_and_excel_prefix_aliases_refresh() {
        struct Global;
        impl FunctionProvider for Global {
            fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn Function>> {
                crate::function_registry::get(ns, name)
            }
        }
        let (store, id) = arena("_xlfn.FN_TABLE_GLOBAL_REFRESH_TEST");
        let mut table = ResolvedFunctions::default();
        table.refresh(&store, &Global);
        assert!(table.get(id).is_none());
        crate::function_registry::register_function(Arc::new(TestFunction));
        table.refresh(&store, &Global);
        assert_eq!(
            table.get(id).unwrap().name(),
            "FN_TABLE_GLOBAL_REFRESH_TEST"
        );
        let previous = table
            .functions
            .get(*table.slots.get(&id).unwrap())
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        crate::function_registry::register_function(Arc::new(TestFunction));
        table.refresh(&store, &Global);
        let replacement = table
            .functions
            .get(*table.slots.get(&id).unwrap())
            .unwrap()
            .as_ref()
            .unwrap();
        assert!(!Arc::ptr_eq(&previous, replacement));
    }
}
