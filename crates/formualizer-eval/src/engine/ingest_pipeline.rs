//! FP8 IngestPipeline: arena-native ingest parity path.
//!
//! The pipeline is intentionally side-effect-light: it interns/reuses formula AST
//! nodes in the engine arena and returns dependency-planning facts without
//! adding graph edges or creating
//! vertices. Graph materialization remains in the existing graph code.

use crate::SheetId;
use crate::engine::arena::{
    AstNodeData, AstNodeId, AstNodeMetadata, CanonicalLabels, CompactRefType, DataStore, SheetKey,
    StringId, ValueRef,
};
use crate::engine::graph::DependencyGraph;
use crate::engine::plan::{DependencyPlan, F_HAS_NAMES, F_HAS_RANGES, F_HAS_TABLES, F_VOLATILE};
use crate::engine::shape_memo::{
    self, MemoValidity, ShapeLookup, ShapeMemo, Specialization, VisitTrace,
};
use crate::engine::sheet_registry::SheetRegistry;
use crate::engine::vertex::VertexId;
use crate::function::FnCaps;
use crate::reference::{CellRef, Coord, RangeRef, SharedRangeRef, SharedRef, SharedSheetLocator};
use crate::traits::FunctionProvider;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{
    ASTNode, ASTNodeType, CollectPolicy, ExternalRefKind, ReferenceType, SpecialItem,
    TableSpecifier,
};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NamedEntryRef {
    pub(crate) vertex: VertexId,
}

#[derive(Clone, Debug)]
pub(crate) struct TableEntrySnapshot {
    pub(crate) name: String,
    pub(crate) range: RangeRef,
    pub(crate) header_row: bool,
    pub(crate) headers: Vec<String>,
    pub(crate) vertex: VertexId,
}

impl TableEntrySnapshot {
    fn sheet_id(&self) -> SheetId {
        self.range.start.sheet_id
    }

    fn col_index(&self, header: &str) -> Option<usize> {
        let header_key = header.to_lowercase();
        self.headers
            .iter()
            .position(|h| h.to_lowercase() == header_key)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourceEntryRef {
    pub(crate) vertex: VertexId,
}

type NameResolveFn<'a> = dyn Fn(&str, SheetId) -> Option<NamedEntryRef> + 'a;
type TableResolveFn<'a> = dyn Fn(&str) -> Option<TableEntrySnapshot> + 'a;
type TableContainingCellFn<'a> = dyn Fn(CellRef) -> Option<TableEntrySnapshot> + 'a;
type SourceResolveFn<'a> = dyn Fn(&str) -> Option<SourceEntryRef> + 'a;

pub(crate) struct NameRegistryView<'a> {
    resolve: Box<NameResolveFn<'a>>,
}

impl<'a> NameRegistryView<'a> {
    pub(crate) fn new(resolve: impl Fn(&str, SheetId) -> Option<NamedEntryRef> + 'a) -> Self {
        Self {
            resolve: Box::new(resolve),
        }
    }

    pub(crate) fn resolve(&self, name: &str, current_sheet: SheetId) -> Option<NamedEntryRef> {
        (self.resolve)(name, current_sheet)
    }
}

pub(crate) struct TableRegistryView<'a> {
    resolve: Box<TableResolveFn<'a>>,
    containing_cell: Box<TableContainingCellFn<'a>>,
}

impl<'a> TableRegistryView<'a> {
    pub(crate) fn new(
        resolve: impl Fn(&str) -> Option<TableEntrySnapshot> + 'a,
        containing_cell: impl Fn(CellRef) -> Option<TableEntrySnapshot> + 'a,
    ) -> Self {
        Self {
            resolve: Box::new(resolve),
            containing_cell: Box::new(containing_cell),
        }
    }

    pub(crate) fn resolve(&self, name: &str) -> Option<TableEntrySnapshot> {
        (self.resolve)(name)
    }

    pub(crate) fn find_containing_cell(&self, cell: CellRef) -> Option<TableEntrySnapshot> {
        (self.containing_cell)(cell)
    }
}

pub(crate) struct SourceRegistryView<'a> {
    resolve_scalar: Box<SourceResolveFn<'a>>,
    resolve_table: Box<SourceResolveFn<'a>>,
}

impl<'a> SourceRegistryView<'a> {
    pub(crate) fn new(
        resolve_scalar: impl Fn(&str) -> Option<SourceEntryRef> + 'a,
        resolve_table: impl Fn(&str) -> Option<SourceEntryRef> + 'a,
    ) -> Self {
        Self {
            resolve_scalar: Box::new(resolve_scalar),
            resolve_table: Box::new(resolve_table),
        }
    }

    pub(crate) fn resolve_scalar(&self, name: &str) -> Option<SourceEntryRef> {
        (self.resolve_scalar)(name)
    }

    pub(crate) fn resolve_table(&self, name: &str) -> Option<SourceEntryRef> {
        (self.resolve_table)(name)
    }
}

pub(crate) struct IngestPipeline<'a> {
    data_store: &'a mut DataStore,
    sheet_registry: &'a mut SheetRegistry,
    names: NameRegistryView<'a>,
    tables: TableRegistryView<'a>,
    sources: SourceRegistryView<'a>,
    function_provider: &'a dyn FunctionProvider,
    policy: CollectPolicy,
    function_semantics_enabled: bool,
    /// Shape memo for this pipeline's lifetime, allocated at the second
    /// arena formula (a one-formula pipeline cannot hit).
    shape_memo: Option<Box<ShapeMemo>>,
    memo_enabled: bool,
    arena_formulas_seen: bool,
    /// `PreparationPolicy::BestEffort`: a missing sheet or table becomes a
    /// pending symbol link instead of a preparation error.
    unbound_pending: bool,
}

impl<'a> IngestPipeline<'a> {
    pub(crate) fn new(
        data_store: &'a mut DataStore,
        sheet_registry: &'a mut SheetRegistry,
        names: NameRegistryView<'a>,
        tables: TableRegistryView<'a>,
        sources: SourceRegistryView<'a>,
        function_provider: &'a dyn FunctionProvider,
        policy: CollectPolicy,
    ) -> Self {
        Self {
            data_store,
            sheet_registry,
            names,
            tables,
            sources,
            function_provider,
            policy,
            function_semantics_enabled: true,
            shape_memo: None,
            memo_enabled: true,
            arena_formulas_seen: false,
            unbound_pending: false,
        }
    }

    /// Explicit metadata oracle for the hidden FP8 parity harness; never part
    /// of dependency ingestion.
    pub(crate) fn parity_metadata(&self, ast: &ASTNode, placement: CellRef) -> AstNodeMetadata {
        compute_tree_metadata(
            ast,
            self.data_store,
            self.function_provider,
            placement,
            self.function_semantics_enabled,
        )
    }

    pub(crate) fn with_unbound_pending(mut self, on: bool) -> Self {
        self.unbound_pending = on;
        self
    }

    /// Under `BestEffort`, record an unresolvable sheet/table as a pending
    /// symbol link (it re-binds when added); otherwise keep the error.
    fn defer_unbound(
        &self,
        kind: &str,
        name: Option<&str>,
        plan: &mut DependencyPlanRow,
        error: ExcelError,
    ) -> Result<(), ExcelError> {
        match name {
            Some(name)
                if self.unbound_pending
                    && !(kind == "sheet" && DependencyGraph::is_tombstone_sheet(name)) =>
            {
                plan.named_refs
                    .push(DependencyGraph::unbound_symbol_key(kind, name));
                Ok(())
            }
            _ => Err(error),
        }
    }

    /// Disable the shape memo (differential tests compare both paths).
    #[cfg(test)]
    pub(crate) fn without_shape_memo(mut self) -> Self {
        self.memo_enabled = false;
        self
    }

    #[cfg(test)]
    pub(crate) fn memo_counts(&self) -> crate::engine::shape_memo::MemoCounts {
        let mut counts = self
            .shape_memo
            .as_ref()
            .map(|memo| memo.counts)
            .unwrap_or_default();
        // The first arena formula is a bypass counted before the memo exists.
        counts.bypasses += u64::from(self.arena_formulas_seen);
        counts
    }

    #[cfg(test)]
    pub(crate) fn shape_memo_state(&self) -> Option<&ShapeMemo> {
        self.shape_memo.as_deref()
    }

    /// Continue with the shape memo of an earlier pipeline over the same
    /// graph (bulk planning in chunks). The memo revalidates itself against
    /// the function provider's revision and the registry epoch.
    pub(crate) fn with_memo_state(mut self, state: Option<MemoState>) -> Self {
        if let Some((memo, seen)) = state {
            self.shape_memo = memo;
            self.arena_formulas_seen = seen;
        }
        self
    }

    /// The memo, for [`Self::with_memo_state`] on the next pipeline.
    pub(crate) fn take_memo_state(&mut self) -> MemoState {
        (self.shape_memo.take(), self.arena_formulas_seen)
    }

    pub(crate) fn enable_function_semantics(mut self) -> Self {
        self.function_semantics_enabled = true;
        self
    }

    pub(crate) fn ingest_formula(
        &mut self,
        ast: FormulaAstInput<'_>,
        placement: CellRef,
        formula_text: Option<Arc<str>>,
    ) -> Result<IngestedFormula, ExcelError> {
        if let FormulaAstInput::Member { template, anchor } = ast {
            return self.ingest_member(template, anchor, placement);
        }
        if let FormulaAstInput::RawArena(id) = ast
            && self.memo_enabled
            && std::mem::replace(&mut self.arena_formulas_seen, true)
        {
            // A pipeline's first arena formula cannot hit; it skips the key
            // walk and the memo is only allocated from the second one on.
            let mut memo = self.shape_memo.take().unwrap_or_default();
            let outcome =
                self.ingest_formula_memoized(&mut memo, id, placement, None, &formula_text);
            if outcome.is_none() {
                memo.counts.bypasses += 1;
            }
            self.shape_memo = Some(memo);
            match outcome {
                Some(result) => return result,
                None => {
                    return self
                        .ingest_formula_unmemoized(ast, placement, formula_text, None)
                        .map(|(formula, _)| formula);
                }
            }
        }
        self.ingest_formula_unmemoized(ast, placement, formula_text, None)
            .map(|(formula, _)| formula)
    }

    /// A load-time family member: the memo path keyed by the template at
    /// its anchor (the same relative shape), with the member's references
    /// relocated on a hit; otherwise the per-cell path over the member's
    /// instantiated tree. A dynamic member gets its own interned AST
    /// (dynamic formulas are evaluated per cell from their own AST).
    fn ingest_member(
        &mut self,
        template: AstNodeId,
        anchor: (u32, u32),
        placement: CellRef,
    ) -> Result<IngestedFormula, ExcelError> {
        let offset = (
            i64::from(placement.coord.row()) - i64::from(anchor.0),
            i64::from(placement.coord.col()) - i64::from(anchor.1),
        );
        let mut outcome = None;
        if self.memo_enabled && std::mem::replace(&mut self.arena_formulas_seen, true) {
            let mut memo = self.shape_memo.take().unwrap_or_default();
            outcome =
                self.ingest_formula_memoized(&mut memo, template, placement, Some(offset), &None);
            if outcome.is_none() {
                memo.counts.bypasses += 1;
            }
            self.shape_memo = Some(memo);
        }
        let mut formula = match outcome {
            Some(result) => result?,
            None => {
                let input = self.member_tree_input(template, offset)?;
                self.ingest_formula_unmemoized(input, placement, None, None)?
                    .0
            }
        };
        formula.member_anchor = Some(anchor);
        if formula.dep_plan.dynamic {
            let FormulaAstInput::MemberTree { tree, .. } =
                self.member_tree_input(template, offset)?
            else {
                unreachable!("member tree input");
            };
            formula.ast_id = self.data_store.store_ast(&tree, self.sheet_registry);
            formula.member_anchor = None;
        }
        Ok(formula)
    }

    fn member_tree_input(
        &self,
        template: AstNodeId,
        (dr, dc): (i64, i64),
    ) -> Result<FormulaAstInput<'static>, ExcelError> {
        let tree = self
            .data_store
            .retrieve_ast(template, self.sheet_registry)
            .ok_or_else(missing_ast_error)?;
        let tree = crate::engine::template::relocate::instantiate_member_ast(&tree, dr, dc)?;
        Ok(FormulaAstInput::MemberTree { tree, template })
    }

    /// Memo path for arena inputs. `None` means the formula takes the
    /// unmemoized per-cell path. With `offset`, `id` is a family template
    /// valid at `placement - offset` and the formula is it relocated.
    fn ingest_formula_memoized(
        &mut self,
        memo: &mut ShapeMemo,
        id: AstNodeId,
        placement: CellRef,
        offset: Option<(i64, i64)>,
        formula_text: &Option<Arc<str>>,
    ) -> Option<Result<IngestedFormula, ExcelError>> {
        // The key walk is relative to the formula's own cell: a member's
        // key is its template's at the anchor.
        let key_placement = match offset {
            None => placement,
            Some((dr, dc)) => CellRef::new(
                placement.sheet_id,
                Coord::new(
                    (i64::from(placement.coord.row()) - dr) as u32,
                    (i64::from(placement.coord.col()) - dc) as u32,
                    true,
                    true,
                ),
            ),
        };
        // Never take the global registry lock here: callers may already hold
        // a semantic-epoch read guard, and a second read behind a waiting
        // writer deadlocks. The provider revision and the registry's epoch
        // mirror are lock-free; a function registered mid-pipeline changes
        // the epoch and clears the memo before any later lookup. A provider
        // without a planning revision cannot prove its semantics unchanged
        // between cells, so it gets no memo.
        let validity = MemoValidity::current(self.function_provider);
        validity.provider_revision?;
        if !memo.should_probe() {
            return None;
        }
        #[cfg(test)]
        {
            memo.key_walks += 1;
        }
        let eligible = shape_memo::shape_tokens(
            self.data_store,
            id,
            key_placement,
            &mut memo.tokens,
            &mut memo.refs,
        );
        if !eligible {
            return None;
        }
        memo.revalidate(validity);
        let shape = match memo.lookup_shape() {
            ShapeLookup::Shape(shape, inserted) => {
                if inserted {
                    memo.counts.shape_misses += 1;
                } else {
                    memo.counts.shape_hits += 1;
                }
                shape
            }
            ShapeLookup::FirstSighting => {
                memo.counts.first_sightings += 1;
                return None;
            }
            ShapeLookup::Full => return None,
        };
        if matches!(
            memo.specialization(shape, placement.sheet_id),
            Some(Some(_))
        ) {
            memo.counts.specialization_hits += 1;
        }
        match memo.specialization(shape, placement.sheet_id) {
            Some(Some(specialization)) => {
                let formula = self
                    .instantiate_specialization(
                        specialization,
                        &memo.refs,
                        id,
                        placement,
                        offset,
                        formula_text.clone(),
                    )
                    .ok()?;
                memo.record_hit();
                // Test builds re-derive every hit on the per-cell path.
                #[cfg(test)]
                self.verify_memo_hit(
                    &formula,
                    id,
                    placement,
                    offset,
                    formula_text.clone(),
                    validity,
                );
                Some(Ok(formula))
            }
            Some(None) => None,
            None if !memo.can_insert_specialization(shape) => None,
            None => {
                memo.counts.specialization_misses += 1;
                let input = match offset {
                    None => FormulaAstInput::RawArena(id),
                    Some(offset) => match self.member_tree_input(id, offset) {
                        Ok(input) => input,
                        Err(error) => return Some(Err(error)),
                    },
                };
                let result = self.ingest_formula_unmemoized(
                    input,
                    placement,
                    formula_text.clone(),
                    Some(memo.refs.len()),
                );
                Some(result.map(|(formula, traced)| {
                    let specialization = traced.and_then(|mut trace| {
                        if !trace.valid {
                            return None;
                        }
                        Some(Specialization {
                            volatile: formula.dep_plan.volatile,
                            dynamic: formula.dep_plan.dynamic,
                            visit: std::mem::take(&mut trace.visit).into_boxed_slice(),
                        })
                    });
                    memo.insert_specialization(shape, placement.sheet_id, specialization);
                    formula
                }))
            }
        }
    }

    /// Product 3: per-placement work on a specialization hit.
    fn instantiate_specialization(
        &mut self,
        specialization: &Specialization,
        refs: &[CompactRefType],
        ast_id: AstNodeId,
        placement: CellRef,
        offset: Option<(i64, i64)>,
        _formula_text: Option<Arc<str>>,
    ) -> Result<IngestedFormula, ExcelError> {
        let mut dep_plan = DependencyPlanRow::default();
        for &index in specialization.visit.iter() {
            let compact = &refs[index as usize];
            // A member's references are its template's relocated (cell and
            // range axes; names are unchanged by relocation).
            let relocated = match (offset, compact) {
                (Some((dr, dc)), CompactRefType::Cell { .. } | CompactRefType::Range { .. }) => {
                    Some(
                        crate::engine::graph::authority_host::relocate_compact_ref(compact, dr, dc)
                            .ok_or_else(|| {
                                ExcelError::new(ExcelErrorKind::Ref)
                                    .with_message("family member reference out of bounds")
                            })?,
                    )
                }
                _ => None,
            };
            let reference = self.data_store.reconstruct_reference_type_for_eval(
                relocated.as_ref().unwrap_or(compact),
                self.sheet_registry,
            );
            let semantic = crate::engine::refs::classify(&reference);
            self.collect_reference(semantic, placement.sheet_id, &mut dep_plan)?;
        }
        dep_plan.volatile = specialization.volatile;
        dep_plan.dynamic = specialization.dynamic;
        dep_plan.dedup_and_sort();
        Ok(IngestedFormula {
            ast_id,
            member_anchor: None,
            placement,
            dep_plan,
        })
    }

    #[cfg(test)]
    fn verify_memo_hit(
        &mut self,
        memoized: &IngestedFormula,
        id: AstNodeId,
        placement: CellRef,
        offset: Option<(i64, i64)>,
        formula_text: Option<Arc<str>>,
        validity: MemoValidity,
    ) {
        let input = match offset {
            None => FormulaAstInput::RawArena(id),
            Some(offset) => self
                .member_tree_input(id, offset)
                .expect("member tree for memo verification"),
        };
        let reference = self
            .ingest_formula_unmemoized(input, placement, formula_text, None)
            .map(|(formula, _)| formula);
        // A provider revision or registry epoch change makes both paths
        // observe moving semantics; only compare against unchanged validity.
        if MemoValidity::current(self.function_provider) != validity {
            return;
        }
        match reference {
            Ok(reference) => {
                if let Some(field) = ingested_formula_difference(memoized, &reference) {
                    panic!("shape memo mismatch at {placement:?} in field {field}");
                }
            }
            Err(error) => {
                panic!("shape memo hit at {placement:?} where the per-cell path fails: {error:?}")
            }
        }
    }

    /// The unchanged per-cell path. With `trace_refs` (the key walk's
    /// reference-node count), also records the dependency visit sequence over
    /// the tree it walks. No FormulaPlane payload is constructed.
    fn ingest_formula_unmemoized(
        &mut self,
        ast: FormulaAstInput<'_>,
        placement: CellRef,
        _formula_text: Option<Arc<str>>,
        trace_refs: Option<usize>,
    ) -> Result<(IngestedFormula, Option<VisitTrace>), ExcelError> {
        let (ast_id, ast_for_oracles) = match ast {
            FormulaAstInput::Tree(mut tree) => {
                self.rewrite_structured_references_for_cell(&mut tree, placement)?;
                let ast_id = self.data_store.store_ast(&tree, self.sheet_registry);
                (ast_id, tree)
            }
            FormulaAstInput::RawArena(id) => {
                let needs_rewrite = self.data_store.ast_needs_structural_rewrite(id);
                if needs_rewrite {
                    let mut tree = self
                        .data_store
                        .retrieve_ast(id, self.sheet_registry)
                        .ok_or_else(missing_ast_error)?;
                    self.rewrite_structured_references_for_cell(&mut tree, placement)?;
                    let rewritten_id = self.data_store.store_ast(&tree, self.sheet_registry);
                    (rewritten_id, tree)
                } else {
                    let tree = self
                        .data_store
                        .retrieve_ast(id, self.sheet_registry)
                        .ok_or_else(missing_ast_error)?;
                    (id, tree)
                }
            }
            FormulaAstInput::MemberTree { tree, template } => (template, tree),
            FormulaAstInput::Member { .. } => {
                unreachable!("members are planned through ingest_member")
            }
            FormulaAstInput::_Lifetime(_) => unreachable!("marker variant is not constructible"),
        };

        let mut trace = trace_refs.and_then(|expected| {
            let mut keys = Vec::with_capacity(expected);
            shape_memo::tree_reference_keys(&ast_for_oracles, &mut keys);
            (keys.len() == expected)
                .then(|| VisitTrace::new(&keys))
                .flatten()
        });
        let mut dep_plan = DependencyPlanRow::default();
        self.collect_dependencies_tree(
            &ast_for_oracles,
            placement.sheet_id,
            &mut dep_plan,
            trace.as_mut(),
        )?;
        dep_plan.volatile = self.ast_is_volatile(&ast_for_oracles);
        dep_plan.dynamic = self.ast_is_dynamic(&ast_for_oracles);
        dep_plan.dedup_and_sort();

        let formula = IngestedFormula {
            ast_id,
            member_anchor: None,
            placement,
            dep_plan,
        };
        Ok((formula, trace))
    }

    pub(crate) fn ingest_batch<'b, I>(
        &mut self,
        formulas: I,
    ) -> Result<Vec<IngestedFormula>, ExcelError>
    where
        I: IntoIterator<Item = (FormulaAstInput<'b>, CellRef, Option<Arc<str>>)>,
    {
        let iter = formulas.into_iter();
        let (lower, _) = iter.size_hint();
        let mut out = Vec::with_capacity(lower);
        for (ast, placement, formula_text) in iter {
            out.push(self.ingest_formula(ast, placement, formula_text)?);
        }
        Ok(out)
    }

    fn ast_is_dynamic(&self, ast: &ASTNode) -> bool {
        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                let dynamic = self.function_semantics_enabled
                    && self.function_provider.function_semantic_identity("", name, args.len())
                        .is_some_and(|identity| {
                            identity.caps.contains(FnCaps::DYNAMIC_DEPENDENCY)
                                || identity.contract.dependency
                                    == crate::function_contract::FunctionDependencySemantics::Dynamic
                        });
                dynamic || args.iter().any(|arg| self.ast_is_dynamic(arg))
            }
            ASTNodeType::UnaryOp { expr, .. } => self.ast_is_dynamic(expr),
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.ast_is_dynamic(left) || self.ast_is_dynamic(right)
            }
            ASTNodeType::Array(rows) => rows.iter().flatten().any(|arg| self.ast_is_dynamic(arg)),
            ASTNodeType::Call { callee, args } => {
                self.ast_is_dynamic(callee) || args.iter().any(|arg| self.ast_is_dynamic(arg))
            }
            _ => false,
        }
    }

    fn ast_is_volatile(&self, ast: &ASTNode) -> bool {
        if ast.contains_volatile() {
            return true;
        }
        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                self.function_provider
                    .function_capabilities("", name)
                    .is_some_and(|caps| caps.contains(FnCaps::VOLATILE))
                    || args.iter().any(|arg| self.ast_is_volatile(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.ast_is_volatile(left) || self.ast_is_volatile(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.ast_is_volatile(expr),
            ASTNodeType::Array(rows) => rows
                .iter()
                .any(|row| row.iter().any(|cell| self.ast_is_volatile(cell))),
            ASTNodeType::Call { callee, args } => {
                self.ast_is_volatile(callee) || args.iter().any(|arg| self.ast_is_volatile(arg))
            }
            ASTNodeType::Literal(_) | ASTNodeType::Omitted | ASTNodeType::Reference { .. } => false,
        }
    }

    fn collect_dependencies_tree(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
        plan: &mut DependencyPlanRow,
        trace: Option<&mut VisitTrace>,
    ) -> Result<(), ExcelError> {
        struct Context<'pipeline, 'plan, 'engine, 'trace> {
            pipeline: &'pipeline mut IngestPipeline<'engine>,
            current_sheet_id: SheetId,
            plan: &'plan mut DependencyPlanRow,
            trace: Option<&'trace mut VisitTrace>,
        }

        fn local_binding_style(
            context: &Context<'_, '_, '_, '_>,
            name: &str,
            arity: usize,
        ) -> crate::engine::refs::LocalBindingStyle {
            use crate::engine::refs::LocalBindingStyle;
            use crate::function_contract::FunctionArgumentDependencyContract as Arguments;

            context
                .pipeline
                .function_provider
                .function_semantic_identity("", name, arity)
                .filter(|identity| {
                    identity.contract.environment
                        == crate::function_contract::FunctionEnvironmentSemantics::LocalBindings
                })
                .and_then(|identity| identity.contract.precision)
                .map_or(LocalBindingStyle::None, |precision| {
                    match precision.arguments {
                        Arguments::LocalBindingPairs => LocalBindingStyle::LocalBindingPairs,
                        Arguments::LambdaParameters => LocalBindingStyle::LambdaParameters,
                        _ => LocalBindingStyle::None,
                    }
                })
        }

        fn consume(
            context: &mut Context<'_, '_, '_, '_>,
            reference: crate::engine::refs::SemanticReference<'_>,
        ) -> Result<(), ExcelError> {
            if let Some(trace) = context.trace.as_deref_mut() {
                trace.record(shape_memo::semantic_reference_key(&reference));
            }
            context
                .pipeline
                .collect_reference(reference, context.current_sheet_id, context.plan)
        }

        let mut context = Context {
            pipeline: self,
            current_sheet_id,
            plan,
            trace,
        };
        crate::engine::refs::visit_tree_references(ast, &mut context, local_binding_style, consume)
    }

    fn collect_reference(
        &mut self,
        reference: crate::engine::refs::SemanticReference<'_>,
        current_sheet_id: SheetId,
        plan: &mut DependencyPlanRow,
    ) -> Result<(), ExcelError> {
        use crate::engine::refs::SemanticReference;

        match reference {
            SemanticReference::ExternalSource(ext) => match ext.kind {
                ExternalRefKind::Cell { .. } => {
                    let name = ext.raw.as_str();
                    if self.sources.resolve_scalar(name).is_some() {
                        plan.source_refs.push(name.to_string());
                        Ok(())
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined name: {name}")))
                    }
                }
                ExternalRefKind::Range { .. } => {
                    let name = ext.raw.as_str();
                    if self.sources.resolve_table(name).is_some() {
                        plan.source_refs.push(name.to_string());
                        Ok(())
                    } else if crate::engine::refs::unbound_external_range_defers(&ext.kind) {
                        plan.named_refs.push(name.to_string());
                        Ok(())
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined table: {name}")))
                    }
                }
            },
            SemanticReference::Cell(cell) => {
                let sheet_id =
                    match self.resolve_reference_sheet(cell.sheet.name(), current_sheet_id) {
                        Ok(id) => id,
                        Err(e) => return self.defer_unbound("sheet", cell.sheet.name(), plan, e),
                    };
                plan.direct_cell_deps.push(CellRef::new(
                    sheet_id,
                    Coord::from_excel(cell.row, cell.col, true, true),
                ));
                Ok(())
            }
            SemanticReference::OpenRange(range) => {
                let sheet_name = range.sheet.name();
                if let Some(SharedRef::Range(range)) = range.original.to_sheet_ref_lossy() {
                    let owned = range.into_owned();
                    let sheet_id = match self.resolve_shared_sheet(owned.sheet, current_sheet_id) {
                        Ok(id) => id,
                        Err(e) => return self.defer_unbound("sheet", sheet_name, plan, e),
                    };
                    plan.range_deps.push(SharedRangeRef {
                        sheet: SharedSheetLocator::Id(sheet_id),
                        start_row: owned.start_row,
                        start_col: owned.start_col,
                        end_row: owned.end_row,
                        end_col: owned.end_col,
                    });
                }
                Ok(())
            }
            SemanticReference::FiniteRange(range) => {
                let (sr, sc, er, ec) = range
                    .finite_bounds()
                    .expect("finite reference must have all bounds");
                if range.is_reversed() {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                }

                // This ingest policy intentionally remains independent from the
                // dependency planner's historical default limit of 16.
                let area = range.saturating_area().expect("finite area");
                if self.policy.expand_small_ranges
                    && area <= self.policy.range_expansion_limit as u64
                {
                    let sheet_id = match self
                        .resolve_reference_sheet(range.sheet.name(), current_sheet_id)
                    {
                        Ok(id) => id,
                        Err(e) => return self.defer_unbound("sheet", range.sheet.name(), plan, e),
                    };
                    for row in sr..=er {
                        for col in sc..=ec {
                            plan.direct_cell_deps.push(CellRef::new(
                                sheet_id,
                                Coord::from_excel(row, col, true, true),
                            ));
                        }
                    }
                } else if let Some(SharedRef::Range(shared)) = range.original.to_sheet_ref_lossy() {
                    let owned = shared.into_owned();
                    let sheet_id = match self.resolve_shared_sheet(owned.sheet, current_sheet_id) {
                        Ok(id) => id,
                        Err(e) => return self.defer_unbound("sheet", range.sheet.name(), plan, e),
                    };
                    plan.range_deps.push(SharedRangeRef {
                        sheet: SharedSheetLocator::Id(sheet_id),
                        start_row: owned.start_row,
                        start_col: owned.start_col,
                        end_row: owned.end_row,
                        end_col: owned.end_col,
                    });
                }
                Ok(())
            }
            SemanticReference::Name(name) => {
                if self.names.resolve(name, current_sheet_id).is_some() {
                    plan.resolved_named_refs.push(name.to_string());
                } else if self.sources.resolve_scalar(name).is_some() {
                    plan.source_refs.push(name.to_string());
                } else {
                    plan.named_refs.push(name.to_string());
                }
                Ok(())
            }
            SemanticReference::Table(tref) => {
                if self.tables.resolve(&tref.name).is_some() {
                    plan.table_refs.push(tref.name.clone());
                    Ok(())
                } else if self.sources.resolve_table(&tref.name).is_some() {
                    plan.source_refs.push(tref.name.clone());
                    Ok(())
                } else {
                    self.defer_unbound(
                        "table",
                        Some(&tref.name),
                        plan,
                        ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined table: {}", tref.name)),
                    )
                }
            }
            SemanticReference::ThreeDimensional(_) | SemanticReference::Unsupported(_) => Ok(()),
        }
    }

    fn resolve_reference_sheet(
        &mut self,
        sheet: Option<&str>,
        current_sheet_id: SheetId,
    ) -> Result<SheetId, ExcelError> {
        match sheet {
            Some(name) => self.sheet_registry.get_id(name).ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Ref)
                    .with_message(format!("Sheet not found: {name}"))
            }),
            None => Ok(current_sheet_id),
        }
    }

    fn resolve_shared_sheet(
        &mut self,
        sheet: SharedSheetLocator<'static>,
        current_sheet_id: SheetId,
    ) -> Result<SheetId, ExcelError> {
        self.sheet_registry
            .resolve_locator(&sheet, current_sheet_id)
    }

    fn rewrite_structured_references_for_cell(
        &self,
        ast: &mut ASTNode,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        self.rewrite_structured_references_node(ast, cell)
    }

    fn rewrite_structured_references_node(
        &self,
        node: &mut ASTNode,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        match &mut node.node_type {
            ASTNodeType::Reference { reference, .. } => {
                self.rewrite_structured_reference(reference, cell)
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.rewrite_structured_references_node(expr, cell)
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                let left_rewritten = self.rewrite_structured_references_node(left, cell)?;
                let right_rewritten = self.rewrite_structured_references_node(right, cell)?;
                Ok(left_rewritten || right_rewritten)
            }
            ASTNodeType::Function { args, .. } => {
                let mut rewritten = false;
                for arg in args {
                    rewritten |= self.rewrite_structured_references_node(arg, cell)?;
                }
                Ok(rewritten)
            }
            ASTNodeType::Call { callee, args } => {
                let mut rewritten = self.rewrite_structured_references_node(callee, cell)?;
                for arg in args {
                    rewritten |= self.rewrite_structured_references_node(arg, cell)?;
                }
                Ok(rewritten)
            }
            ASTNodeType::Array(rows) => {
                let mut rewritten = false;
                for row in rows {
                    for item in row {
                        rewritten |= self.rewrite_structured_references_node(item, cell)?;
                    }
                }
                Ok(rewritten)
            }
            ASTNodeType::Literal(_) | ASTNodeType::Omitted => Ok(false),
        }
    }

    fn rewrite_structured_reference(
        &self,
        reference: &mut ReferenceType,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        let ReferenceType::Table(tref) = reference else {
            return Ok(false);
        };
        if !tref.name.is_empty() {
            return Ok(false);
        }

        let col_name = match &tref.specifier {
            Some(TableSpecifier::Combination(parts)) => {
                let mut saw_this_row = false;
                let mut col: Option<&str> = None;
                for part in parts {
                    match part.as_ref() {
                        TableSpecifier::SpecialItem(SpecialItem::ThisRow) => saw_this_row = true,
                        TableSpecifier::Column(c) => {
                            if col.is_some() {
                                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                    "This-row structured reference with multiple columns is not supported".to_string(),
                                ));
                            }
                            col = Some(c.as_str());
                        }
                        other => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                format!(
                                    "Unsupported this-row structured reference component: {other}"
                                ),
                            ));
                        }
                    }
                }
                if !saw_this_row {
                    return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "Unnamed structured reference requires a this-row selector".to_string(),
                    ));
                }
                col.ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "This-row structured reference missing column selector".to_string(),
                    )
                })?
            }
            _ => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                    "Unnamed structured reference form is not supported".to_string(),
                ));
            }
        };

        let Some(table) = self.tables.find_containing_cell(cell) else {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("This-row structured reference used outside a table".to_string()));
        };

        let row0 = cell.coord.row();
        let col0 = cell.coord.col();
        let sr0 = table.range.start.coord.row();
        let sc0 = table.range.start.coord.col();
        let er0 = table.range.end.coord.row();
        let ec0 = table.range.end.coord.col();

        if table.sheet_id() != cell.sheet_id || row0 < sr0 || row0 > er0 || col0 < sc0 || col0 > ec0
        {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("This-row structured reference used outside a table".to_string()));
        }
        if table.header_row && row0 == sr0 {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                "This-row structured references are not valid in the table header row".to_string(),
            ));
        }
        let data_start = if table.header_row { sr0 + 1 } else { sr0 };
        if row0 < data_start {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                "This-row structured references require a data/totals row context".to_string(),
            ));
        }

        let Some(idx) = table.col_index(col_name) else {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(format!(
                "Unknown table column in this-row reference: {col_name}"
            )));
        };
        *reference = ReferenceType::Cell {
            sheet: None,
            row: row0 + 1,
            col: sc0 + idx as u32 + 1,
            row_abs: true,
            col_abs: true,
        };
        Ok(true)
    }
}

/// A pipeline's shape memo and whether it has seen an arena formula.
pub(crate) type MemoState = (Option<Box<ShapeMemo>>, bool);

pub(crate) enum FormulaAstInput<'a> {
    Tree(ASTNode),
    RawArena(AstNodeId),
    /// A load-time family member (P2-M2): `template` is the formula at the
    /// 0-based `anchor` cell and this cell's formula is it relocated. The
    /// member is planned without interning its own AST.
    Member {
        template: AstNodeId,
        anchor: (u32, u32),
    },
    /// Internal: a member's instantiated tree, planned as `template`.
    MemberTree {
        tree: ASTNode,
        template: AstNodeId,
    },
    #[doc(hidden)]
    _Lifetime(PhantomData<&'a ()>),
}

pub(crate) struct IngestedFormula {
    pub(crate) ast_id: AstNodeId,
    /// `Some(anchor)` when `ast_id` is a family template valid at the
    /// 0-based anchor cell rather than this formula's own AST.
    pub(crate) member_anchor: Option<(u32, u32)>,
    pub(crate) placement: CellRef,
    pub(crate) dep_plan: DependencyPlanRow,
}

/// First field in which two ingest results differ, or `None` if they are
/// identical in every field (differential memo verification).
#[cfg(test)]
pub(crate) fn ingested_formula_difference(
    a: &IngestedFormula,
    b: &IngestedFormula,
) -> Option<&'static str> {
    let checks = [
        ("ast_id", a.ast_id == b.ast_id),
        ("member_anchor", a.member_anchor == b.member_anchor),
        ("placement", a.placement == b.placement),
        ("dep_plan", a.dep_plan == b.dep_plan),
    ];
    checks
        .into_iter()
        .find_map(|(field, equal)| (!equal).then_some(field))
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DependencyPlanRow {
    pub(crate) direct_cell_deps: Vec<CellRef>,
    pub(crate) range_deps: Vec<SharedRangeRef<'static>>,
    pub(crate) named_refs: Vec<String>,
    pub(crate) table_refs: Vec<String>,
    pub(crate) source_refs: Vec<String>,
    pub(crate) external_refs: Vec<String>,
    pub(crate) volatile: bool,
    pub(crate) dynamic: bool,
    pub(crate) resolved_named_refs: Vec<String>,
}

impl DependencyPlanRow {
    fn dedup_and_sort(&mut self) {
        self.direct_cell_deps.sort();
        self.direct_cell_deps.dedup();
        dedup_strings(&mut self.named_refs);
        dedup_strings(&mut self.table_refs);
        dedup_strings(&mut self.source_refs);
        dedup_strings(&mut self.external_refs);
        dedup_strings(&mut self.resolved_named_refs);
        let mut ranges = Vec::new();
        for range in self.range_deps.drain(..) {
            if !ranges.contains(&range) {
                ranges.push(range);
            }
        }
        self.range_deps = ranges;
    }
}

impl From<DependencyPlanRow> for DependencyPlan {
    fn from(row: DependencyPlanRow) -> Self {
        let mut plan = DependencyPlan::default();
        plan.per_formula_flags.push(
            (if row.volatile { F_VOLATILE } else { 0 })
                | (if !row.range_deps.is_empty() {
                    F_HAS_RANGES
                } else {
                    0
                })
                | (if !row.named_refs.is_empty() || !row.resolved_named_refs.is_empty() {
                    F_HAS_NAMES
                } else {
                    0
                })
                | (if !row.table_refs.is_empty() {
                    F_HAS_TABLES
                } else {
                    0
                }),
        );
        plan.per_formula_names.push(row.named_refs);
        plan.per_formula_tables.push(row.table_refs);
        plan
    }
}

fn dedup_strings(values: &mut Vec<String>) {
    values.sort();
    values.dedup();
}

fn missing_ast_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::Value).with_message("Missing interned formula AST")
}

fn compute_tree_metadata(
    ast: &ASTNode,
    data_store: &DataStore,
    function_provider: &dyn FunctionProvider,
    placement: CellRef,
    allow_function_semantics: bool,
) -> AstNodeMetadata {
    fn visit(
        ast: &ASTNode,
        data_store: &DataStore,
        function_provider: &dyn FunctionProvider,
        placement: CellRef,
        allow_function_semantics: bool,
    ) -> AstNodeMetadata {
        let (mut data, child_metadata) = match &ast.node_type {
            ASTNodeType::Literal(value) => (
                AstNodeData::Literal(canonical_literal_value_ref(value.clone())),
                Vec::new(),
            ),
            ASTNodeType::Omitted => (AstNodeData::Omitted, Vec::new()),
            ASTNodeType::Reference {
                original,
                reference,
            } => (
                AstNodeData::Reference {
                    original_id: ast_string_id(data_store, original),
                    ref_type: compact_ref_type_from_ast(reference, data_store),
                },
                Vec::new(),
            ),
            ASTNodeType::UnaryOp { op, expr } => (
                AstNodeData::UnaryOp {
                    operator: *op,
                    expr_id: AstNodeId::from_u32(0),
                },
                vec![visit(
                    expr,
                    data_store,
                    function_provider,
                    placement,
                    allow_function_semantics,
                )],
            ),
            ASTNodeType::BinaryOp { op, left, right } => (
                AstNodeData::BinaryOp {
                    operator: *op,
                    left_id: AstNodeId::from_u32(0),
                    right_id: AstNodeId::from_u32(0),
                },
                vec![
                    visit(
                        left,
                        data_store,
                        function_provider,
                        placement,
                        allow_function_semantics,
                    ),
                    visit(
                        right,
                        data_store,
                        function_provider,
                        placement,
                        allow_function_semantics,
                    ),
                ],
            ),
            ASTNodeType::Function { name, args } => (
                AstNodeData::Function {
                    name_id: ast_string_id(data_store, name),
                    args_offset: 0,
                    args_count: args.len() as u16,
                },
                args.iter()
                    .map(|arg| {
                        visit(
                            arg,
                            data_store,
                            function_provider,
                            placement,
                            allow_function_semantics,
                        )
                    })
                    .collect(),
            ),
            ASTNodeType::Array(rows) => {
                let child_metadata = rows
                    .iter()
                    .flat_map(|row| row.iter())
                    .map(|cell| {
                        visit(
                            cell,
                            data_store,
                            function_provider,
                            placement,
                            allow_function_semantics,
                        )
                    })
                    .collect();
                (
                    AstNodeData::Array {
                        rows: rows.len() as u16,
                        cols: rows.first().map(|row| row.len()).unwrap_or(0) as u16,
                        elements_offset: 0,
                    },
                    child_metadata,
                )
            }
            ASTNodeType::Call { callee, args } => {
                let mut child_metadata = Vec::with_capacity(args.len() + 1);
                child_metadata.push(visit(
                    callee,
                    data_store,
                    function_provider,
                    placement,
                    allow_function_semantics,
                ));
                child_metadata.extend(args.iter().map(|arg| {
                    visit(
                        arg,
                        data_store,
                        function_provider,
                        placement,
                        allow_function_semantics,
                    )
                }));
                (
                    AstNodeData::Function {
                        name_id: StringId::INVALID,
                        args_offset: 0,
                        args_count: child_metadata.len() as u16,
                    },
                    child_metadata,
                )
            }
        };

        normalize_node_for_canonical_metadata(&mut data, data_store, placement);
        let child_refs: Vec<&AstNodeMetadata> = child_metadata.iter().collect();
        let mut metadata = crate::engine::arena::canonical::compute_node_metadata(
            &data,
            &child_refs,
            data_store.ast_strings(),
            function_provider,
            allow_function_semantics,
        );
        if matches!(ast.node_type, ASTNodeType::Call { .. }) {
            metadata.labels = CanonicalLabels::default();
            for child in &child_metadata {
                metadata.labels.flags |= child.labels.flags;
                metadata.labels.rejects |= child.labels.rejects;
            }
            metadata.labels.rejects |= CanonicalLabels::REJECT_CALL_EXPRESSION;
        }
        metadata
    }

    visit(
        ast,
        data_store,
        function_provider,
        placement,
        allow_function_semantics,
    )
}

fn ast_string_id(data_store: &DataStore, value: &str) -> StringId {
    data_store
        .ast_strings()
        .get_id(value)
        .unwrap_or(StringId::INVALID)
}

fn compact_ref_type_from_ast(reference: &ReferenceType, data_store: &DataStore) -> CompactRefType {
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => CompactRefType::Cell {
            sheet: sheet
                .as_ref()
                .map(|sheet| SheetKey::Name(ast_string_id(data_store, sheet))),
            row: *row,
            col: *col,
            row_abs: *row_abs,
            col_abs: *col_abs,
        },
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
        } => CompactRefType::Range {
            sheet: sheet
                .as_ref()
                .map(|sheet| SheetKey::Name(ast_string_id(data_store, sheet))),
            start_row: start_row.unwrap_or(0),
            start_col: start_col.unwrap_or(0),
            end_row: end_row.unwrap_or(u32::MAX),
            end_col: end_col.unwrap_or(u32::MAX),
            start_row_abs: *start_row_abs,
            start_col_abs: *start_col_abs,
            end_row_abs: *end_row_abs,
            end_col_abs: *end_col_abs,
        },
        ReferenceType::External(ext) => CompactRefType::External {
            raw_id: ast_string_id(data_store, &ext.raw),
            book_id: ast_string_id(data_store, ext.book.token()),
            sheet_id: ast_string_id(data_store, &ext.sheet),
            kind: ext.kind,
        },
        ReferenceType::NamedRange(name) => {
            CompactRefType::NamedRange(ast_string_id(data_store, name))
        }
        ReferenceType::Table(table) => CompactRefType::Table {
            name_id: ast_string_id(data_store, &table.name),
            specifier_id: None,
        },
        ReferenceType::Cell3D {
            sheet_first,
            sheet_last,
            row,
            col,
            row_abs,
            col_abs,
        } => CompactRefType::Cell3D {
            sheet_first: ast_string_id(data_store, sheet_first),
            sheet_last: ast_string_id(data_store, sheet_last),
            row: *row,
            col: *col,
            row_abs: *row_abs,
            col_abs: *col_abs,
        },
        ReferenceType::Range3D {
            sheet_first,
            sheet_last,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => CompactRefType::Range3D {
            sheet_first: ast_string_id(data_store, sheet_first),
            sheet_last: ast_string_id(data_store, sheet_last),
            start_row: start_row.unwrap_or(0),
            start_col: start_col.unwrap_or(0),
            end_row: end_row.unwrap_or(u32::MAX),
            end_col: end_col.unwrap_or(u32::MAX),
            start_row_abs: *start_row_abs,
            start_col_abs: *start_col_abs,
            end_row_abs: *end_row_abs,
            end_col_abs: *end_col_abs,
        },
    }
}

fn normalize_node_for_canonical_metadata(
    node: &mut AstNodeData,
    _data_store: &DataStore,
    placement: CellRef,
) {
    match node {
        AstNodeData::Literal(_) | AstNodeData::Omitted => {}
        AstNodeData::Reference { ref_type, .. } => normalize_reference_axes(ref_type, placement),
        AstNodeData::UnaryOp { .. }
        | AstNodeData::BinaryOp { .. }
        | AstNodeData::Function { .. }
        | AstNodeData::Array { .. } => {}
    }
}

fn normalize_reference_axes(ref_type: &mut CompactRefType, placement: CellRef) {
    let anchor_row = placement.coord.row() + 1;
    let anchor_col = placement.coord.col() + 1;
    match ref_type {
        CompactRefType::Cell {
            row,
            col,
            row_abs,
            col_abs,
            ..
        } => {
            normalize_finite_axis(row, anchor_row, *row_abs);
            normalize_finite_axis(col, anchor_col, *col_abs);
        }
        CompactRefType::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
            ..
        } => {
            normalize_start_axis(start_row, anchor_row, *start_row_abs);
            normalize_start_axis(start_col, anchor_col, *start_col_abs);
            normalize_end_axis(end_row, anchor_row, *end_row_abs);
            normalize_end_axis(end_col, anchor_col, *end_col_abs);
        }
        CompactRefType::Cell3D {
            row,
            col,
            row_abs,
            col_abs,
            ..
        } => {
            normalize_finite_axis(row, anchor_row, *row_abs);
            normalize_finite_axis(col, anchor_col, *col_abs);
        }
        CompactRefType::Range3D {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
            ..
        } => {
            normalize_start_axis(start_row, anchor_row, *start_row_abs);
            normalize_start_axis(start_col, anchor_col, *start_col_abs);
            normalize_end_axis(end_row, anchor_row, *end_row_abs);
            normalize_end_axis(end_col, anchor_col, *end_col_abs);
        }
        CompactRefType::External { kind, .. } => match kind {
            ExternalRefKind::Cell {
                row,
                col,
                row_abs,
                col_abs,
            } => {
                normalize_finite_axis(row, anchor_row, *row_abs);
                normalize_finite_axis(col, anchor_col, *col_abs);
            }
            ExternalRefKind::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                normalize_optional_axis(start_row, anchor_row, *start_row_abs);
                normalize_optional_axis(start_col, anchor_col, *start_col_abs);
                normalize_optional_axis(end_row, anchor_row, *end_row_abs);
                normalize_optional_axis(end_col, anchor_col, *end_col_abs);
            }
        },
        CompactRefType::NamedRange(_) | CompactRefType::Table { .. } => {}
    }
}

fn normalize_start_axis(value: &mut u32, anchor: u32, absolute: bool) {
    if *value != 0 {
        normalize_finite_axis(value, anchor, absolute);
    }
}

fn normalize_end_axis(value: &mut u32, anchor: u32, absolute: bool) {
    if *value != u32::MAX {
        normalize_finite_axis(value, anchor, absolute);
    }
}

fn normalize_optional_axis(value: &mut Option<u32>, anchor: u32, absolute: bool) {
    if let Some(value) = value {
        normalize_finite_axis(value, anchor, absolute);
    }
}

fn normalize_finite_axis(value: &mut u32, anchor: u32, absolute: bool) {
    if !absolute {
        *value = ((i64::from(*value) - i64::from(anchor)) as i32 as u32) ^ 0x8000_0000;
    }
}

fn canonical_literal_value_ref(value: LiteralValue) -> ValueRef {
    match value {
        LiteralValue::Empty => ValueRef::empty(),
        LiteralValue::Boolean(value) => ValueRef::boolean(value),
        LiteralValue::Int(value) => i32::try_from(value)
            .ok()
            .and_then(ValueRef::small_int)
            .unwrap_or_else(|| {
                ValueRef::large_int(fnv1a_literal_payload(b"int", &value.to_le_bytes()))
            }),
        LiteralValue::Number(value) => ValueRef::number(fnv1a_literal_payload(
            b"number",
            &value.to_bits().to_le_bytes(),
        )),
        LiteralValue::Text(value) => {
            ValueRef::string(fnv1a_literal_payload(b"text", value.as_bytes()))
        }
        LiteralValue::Error(error) => ValueRef::error(fnv1a_literal_payload(
            b"error",
            error.to_string().as_bytes(),
        )),
        LiteralValue::Array(array) => ValueRef::array(fnv1a_literal_payload(
            b"array",
            format!("{array:?}").as_bytes(),
        )),
        LiteralValue::Date(value) => {
            ValueRef::date_time(fnv1a_literal_payload(b"date", value.to_string().as_bytes()))
        }
        LiteralValue::DateTime(value) => ValueRef::date_time(fnv1a_literal_payload(
            b"datetime",
            value.to_string().as_bytes(),
        )),
        LiteralValue::Time(value) => {
            ValueRef::date_time(fnv1a_literal_payload(b"time", value.to_string().as_bytes()))
        }
        LiteralValue::Duration(value) => ValueRef::duration(fnv1a_literal_payload(
            b"duration",
            &value
                .num_nanoseconds()
                .unwrap_or(value.num_seconds())
                .to_le_bytes(),
        )),
        LiteralValue::Pending => ValueRef::pending(),
    }
}

fn fnv1a_literal_payload(tag: &[u8], bytes: &[u8]) -> u32 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in tag.iter().chain(bytes) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash as u32 ^ (hash >> 32) as u32) & 0x0fff_ffff
}

impl DependencyGraph {
    pub(crate) fn ingest_pipeline<'a>(
        &'a mut self,
        function_provider: &'a dyn FunctionProvider,
    ) -> IngestPipeline<'a> {
        let range_expansion_limit = self.range_expansion_limit();
        let policy = CollectPolicy {
            expand_small_ranges: true,
            range_expansion_limit,
            include_names: true,
        };
        self.ingest_pipeline_with_policy(function_provider, policy)
    }

    pub(crate) fn ingest_pipeline_with_policy<'a>(
        &'a mut self,
        function_provider: &'a dyn FunctionProvider,
        policy: CollectPolicy,
    ) -> IngestPipeline<'a> {
        self.make_ingest_pipeline(function_provider, policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, EvalConfig};
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::parse;
    use proptest::prelude::*;
    use rustc_hash::FxHashSet;

    fn ensure_builtins_registered() {
        crate::builtins::load_builtins();
    }

    #[test]
    fn ingest_has_no_formula_plane_producer_sites() {
        // Census of executable producer call sites, not timings: these APIs
        // must not be reintroduced on either memo hits or misses.
        let production = include_str!("ingest_pipeline.rs")
            .split("#[cfg(test)]\nmod tests")
            .next()
            .unwrap();
        for producer in [
            "span_read_summary_from_projections(",
            "build_template_slot_map(",
            "compute_read_projections(",
            "canonicalize_template_with_provider(",
        ] {
            assert_eq!(
                production.matches(producer).count(),
                0,
                "dead producer {producer}"
            );
        }
        assert_eq!(
            std::mem::size_of::<IngestedFormula>(),
            std::mem::size_of::<(AstNodeId, Option<(u32, u32)>, CellRef, DependencyPlanRow)>()
        );
    }

    #[test]
    fn dependency_dynamic_flag_matches_metadata_oracle() {
        ensure_builtins_registered();
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let sheet = engine.graph.sheet_id_mut("Sheet1");
        let placement = CellRef::new(sheet, Coord::from_excel(2, 2, true, true));
        for text in [
            "=A1+1",
            "=OFFSET(A1,0,0)",
            "=IF(TRUE,INDIRECT(\"A1\"),0)",
            "=SUM(A1:A10)",
            "=ROW(A1)",
            "=RAND()+1",
            "=UNKNOWN(A1)",
            "={1,2;3,4}",
        ] {
            let ast = parse(text).unwrap();
            // Metadata uses the arena's interned function and identifier strings,
            // as the old ingest path did after storing the input AST.
            engine.intern_formula_ast(&ast);
            let mut pipeline = engine.ingest_pipeline();
            let expected = pipeline
                .parity_metadata(&ast, placement)
                .labels
                .has_flag(CanonicalLabels::FLAG_DYNAMIC);
            let result = pipeline
                .ingest_formula(FormulaAstInput::Tree(ast), placement, None)
                .unwrap();
            assert_eq!(result.dep_plan.dynamic, expected, "{text}");
        }
    }

    #[test]
    fn engine_constructs_and_runs_ingest_pipeline() {
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let sheet = engine.graph.sheet_id_mut("Sheet1");
        let placement = CellRef::new(sheet, Coord::from_excel(1, 1, true, true));
        let ast = parse("=A1+1").unwrap();
        let mut pipeline = engine.ingest_pipeline();
        let ingested = pipeline
            .ingest_formula(FormulaAstInput::Tree(ast), placement, None)
            .unwrap();
        assert_eq!(ingested.dep_plan.direct_cell_deps, vec![placement]);
    }

    // Frozen from `engine/ingest_pipeline.rs` at `ccfeaf83`. Only method names
    // were prefixed so the legacy and consolidated walks can run side by side.
    impl<'a> IngestPipeline<'a> {
        fn legacy_collect_dependencies_tree(
            &mut self,
            ast: &ASTNode,
            current_sheet_id: SheetId,
            plan: &mut DependencyPlanRow,
            local_scopes: &mut Vec<FxHashSet<String>>,
        ) -> Result<(), ExcelError> {
            match &ast.node_type {
                ASTNodeType::Reference { reference, .. } => {
                    self.legacy_collect_reference(reference, current_sheet_id, plan, local_scopes)
                }
                ASTNodeType::BinaryOp { left, right, .. } => {
                    self.legacy_collect_dependencies_tree(
                        left,
                        current_sheet_id,
                        plan,
                        local_scopes,
                    )?;
                    self.legacy_collect_dependencies_tree(
                        right,
                        current_sheet_id,
                        plan,
                        local_scopes,
                    )
                }
                ASTNodeType::UnaryOp { expr, .. } => self.legacy_collect_dependencies_tree(
                    expr,
                    current_sheet_id,
                    plan,
                    local_scopes,
                ),
                ASTNodeType::Function { name, args } => {
                    use crate::function_contract::FunctionArgumentDependencyContract as Arguments;
                    let argument_contract = self
                        .function_provider
                        .function_semantic_identity("", name, args.len())
                        .filter(|identity| {
                            identity.contract.environment
                            == crate::function_contract::FunctionEnvironmentSemantics::LocalBindings
                        })
                        .and_then(|identity| identity.contract.precision)
                        .map(|precision| precision.arguments);
                    match argument_contract {
                        Some(Arguments::LocalBindingPairs)
                            if args.len() >= 3 && args.len() % 2 == 1 =>
                        {
                            local_scopes.push(FxHashSet::default());
                            for pair_idx in (0..args.len() - 1).step_by(2) {
                                self.legacy_collect_dependencies_tree(
                                    &args[pair_idx + 1],
                                    current_sheet_id,
                                    plan,
                                    local_scopes,
                                )?;
                                if let ASTNodeType::Reference {
                                    reference: ReferenceType::NamedRange(local_name),
                                    ..
                                } = &args[pair_idx].node_type
                                    && let Some(scope) = local_scopes.last_mut()
                                {
                                    scope.insert(local_name.to_ascii_uppercase());
                                }
                            }
                            self.legacy_collect_dependencies_tree(
                                &args[args.len() - 1],
                                current_sheet_id,
                                plan,
                                local_scopes,
                            )?;
                            local_scopes.pop();
                            Ok(())
                        }
                        Some(Arguments::LambdaParameters) => {
                            if let Some(body) = args.last() {
                                let mut lambda_scope = FxHashSet::default();
                                for param in &args[..args.len().saturating_sub(1)] {
                                    if let ASTNodeType::Reference {
                                        reference: ReferenceType::NamedRange(param_name),
                                        ..
                                    } = &param.node_type
                                    {
                                        lambda_scope.insert(param_name.to_ascii_uppercase());
                                    }
                                }
                                local_scopes.push(lambda_scope);
                                self.legacy_collect_dependencies_tree(
                                    body,
                                    current_sheet_id,
                                    plan,
                                    local_scopes,
                                )?;
                                local_scopes.pop();
                            }
                            Ok(())
                        }
                        _ => {
                            for arg in args {
                                self.legacy_collect_dependencies_tree(
                                    arg,
                                    current_sheet_id,
                                    plan,
                                    local_scopes,
                                )?;
                            }
                            Ok(())
                        }
                    }
                }
                ASTNodeType::Call { callee, args } => {
                    self.legacy_collect_dependencies_tree(
                        callee,
                        current_sheet_id,
                        plan,
                        local_scopes,
                    )?;
                    for arg in args {
                        self.legacy_collect_dependencies_tree(
                            arg,
                            current_sheet_id,
                            plan,
                            local_scopes,
                        )?;
                    }
                    Ok(())
                }
                ASTNodeType::Array(rows) => {
                    for row in rows {
                        for item in row {
                            self.legacy_collect_dependencies_tree(
                                item,
                                current_sheet_id,
                                plan,
                                local_scopes,
                            )?;
                        }
                    }
                    Ok(())
                }
                ASTNodeType::Literal(_) | ASTNodeType::Omitted => Ok(()),
            }
        }

        fn legacy_collect_reference(
            &mut self,
            reference: &ReferenceType,
            current_sheet_id: SheetId,
            plan: &mut DependencyPlanRow,
            local_scopes: &[FxHashSet<String>],
        ) -> Result<(), ExcelError> {
            match reference {
                ReferenceType::External(ext) => match ext.kind {
                    ExternalRefKind::Cell { .. } => {
                        let name = ext.raw.as_str();
                        if self.sources.resolve_scalar(name).is_some() {
                            plan.source_refs.push(name.to_string());
                            Ok(())
                        } else {
                            Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined name: {name}")))
                        }
                    }
                    ExternalRefKind::Range { .. } => {
                        let name = ext.raw.as_str();
                        if self.sources.resolve_table(name).is_some() {
                            plan.source_refs.push(name.to_string());
                            Ok(())
                        } else {
                            Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined table: {name}")))
                        }
                    }
                },
                ReferenceType::Cell {
                    sheet, row, col, ..
                } => {
                    let sheet_id =
                        self.resolve_reference_sheet(sheet.as_deref(), current_sheet_id)?;
                    plan.direct_cell_deps.push(CellRef::new(
                        sheet_id,
                        Coord::from_excel(*row, *col, true, true),
                    ));
                    Ok(())
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    ..
                } => {
                    let has_unbounded = start_row.is_none()
                        || end_row.is_none()
                        || start_col.is_none()
                        || end_col.is_none();
                    if has_unbounded {
                        if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                            let owned = range.into_owned();
                            let sheet_id =
                                self.resolve_shared_sheet(owned.sheet, current_sheet_id)?;
                            plan.range_deps.push(SharedRangeRef {
                                sheet: SharedSheetLocator::Id(sheet_id),
                                start_row: owned.start_row,
                                start_col: owned.start_col,
                                end_row: owned.end_row,
                                end_col: owned.end_col,
                            });
                        }
                        return Ok(());
                    }

                    let (Some(sr), Some(sc), Some(er), Some(ec)) =
                        (*start_row, *start_col, *end_row, *end_col)
                    else {
                        return Err(ExcelError::new(ExcelErrorKind::Ref));
                    };
                    if sr > er || sc > ec {
                        return Err(ExcelError::new(ExcelErrorKind::Ref));
                    }

                    let height = er.saturating_sub(sr) + 1;
                    let width = ec.saturating_sub(sc) + 1;
                    let size = (width * height) as usize;
                    if self.policy.expand_small_ranges && size <= self.policy.range_expansion_limit
                    {
                        let sheet_id =
                            self.resolve_reference_sheet(sheet.as_deref(), current_sheet_id)?;
                        for row in sr..=er {
                            for col in sc..=ec {
                                plan.direct_cell_deps.push(CellRef::new(
                                    sheet_id,
                                    Coord::from_excel(row, col, true, true),
                                ));
                            }
                        }
                    } else if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                        let owned = range.into_owned();
                        let sheet_id = self.resolve_shared_sheet(owned.sheet, current_sheet_id)?;
                        plan.range_deps.push(SharedRangeRef {
                            sheet: SharedSheetLocator::Id(sheet_id),
                            start_row: owned.start_row,
                            start_col: owned.start_col,
                            end_row: owned.end_row,
                            end_col: owned.end_col,
                        });
                    }
                    Ok(())
                }
                ReferenceType::NamedRange(name) => {
                    let key = name.to_ascii_uppercase();
                    if local_scopes.iter().rev().any(|scope| scope.contains(&key)) {
                        return Ok(());
                    }
                    if self.names.resolve(name, current_sheet_id).is_some() {
                        plan.resolved_named_refs.push(name.to_string());
                    } else if self.sources.resolve_scalar(name).is_some() {
                        plan.source_refs.push(name.to_string());
                    } else {
                        plan.named_refs.push(name.to_string());
                    }
                    Ok(())
                }
                ReferenceType::Table(tref) => {
                    if self.tables.resolve(&tref.name).is_some() {
                        plan.table_refs.push(tref.name.clone());
                        Ok(())
                    } else if self.sources.resolve_table(&tref.name).is_some() {
                        plan.source_refs.push(tref.name.clone());
                        Ok(())
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined table: {}", tref.name)))
                    }
                }
                ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => Ok(()),
            }
        }
    }

    fn assert_ingest_walk_parity(
        pipeline: &mut IngestPipeline<'_>,
        ast: &ASTNode,
        current_sheet: SheetId,
    ) {
        use rustc_hash::FxHashSet;

        let mut old = DependencyPlanRow::default();
        let mut local_scopes: Vec<FxHashSet<String>> = Vec::new();
        let old_result = pipeline.legacy_collect_dependencies_tree(
            ast,
            current_sheet,
            &mut old,
            &mut local_scopes,
        );
        let mut new = DependencyPlanRow::default();
        let new_result = pipeline.collect_dependencies_tree(ast, current_sheet, &mut new, None);
        assert_eq!(old_result, new_result);
        if old_result.is_ok() {
            old.dedup_and_sort();
            new.dedup_and_sort();
            assert_eq!(old, new);
        }
    }

    #[test]
    fn frozen_ingest_walk_matches_reference_classes_scopes_and_errors() {
        ensure_builtins_registered();
        // The frozen reference walk has only Strict semantics.
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_preparation_policy(crate::engine::PreparationPolicy::Strict),
        );
        let sheet = engine.graph.sheet_id_mut("Sheet1");
        engine.graph.sheet_id_mut("Sheet2");
        let mut pipeline = engine.ingest_pipeline();
        let formulas = [
            "=SUM(A1,$B2,C$3,$D$4)",
            "=SUM(A1:A1,A1:B2,Sheet2!C3:D4,A1:A,A1:1,A:A,1:1)",
            "=SUM(Sheet2!A1,NamedThing)",
            "=Table1[#Data]",
            "=SUM([book]Sheet!A1,[book]Sheet!A1:B2)",
            "=SUM(Sheet1:Sheet2!A1,Sheet1:Sheet2!B2:C3)",
            "=SUM({A1,B2;C3,D4},IF(D4,,E5))",
            "=LET(x,A1,y,B2,x+y+C3)",
            "=LAMBDA(x,y,x+y+A1)",
            "=SUM(D4:B2)",
            "=Missing!A1",
        ];
        for formula in formulas {
            let ast = parse(formula).unwrap();
            for limit in [0, 1, 4, 16, 64] {
                pipeline.policy.range_expansion_limit = limit;
                assert_ingest_walk_parity(&mut pipeline, &ast, sheet);
            }
        }
    }

    #[test]
    fn overflow_sized_ranges_stay_compressed_in_ingest() {
        // The frozen oracle intentionally retains base's wrapping u32 multiply,
        // so overflow inputs are covered by direct behavior pins rather than
        // differential comparison (the debug oracle would panic).
        ensure_builtins_registered();
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_range_expansion_limit(64),
        );
        let sheet = engine.graph.sheet_id_mut("Sheet1");
        let mut pipeline = engine.ingest_pipeline();
        for formula in [
            "=SUM(A1:FLA983055)",
            "=SUM(A1:XFD262144)",
            "=SUM(A1:XFD1048576)",
        ] {
            let ast = parse(formula).unwrap();
            let mut plan = DependencyPlanRow::default();
            pipeline
                .collect_dependencies_tree(&ast, sheet, &mut plan, None)
                .unwrap();
            assert!(plan.direct_cell_deps.is_empty(), "{formula}");
            assert_eq!(plan.range_deps.len(), 1, "{formula}");
        }
    }

    fn dependency_atom() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("A1"),
            Just("$B2"),
            Just("C$3"),
            Just("A1:B2"),
            Just("B2:E6"),
            Just("A1:A"),
            Just("A1:1"),
            Just("A:A"),
            Just("1:1"),
            Just("Sheet2!E5"),
            Just("Sheet2!A1:C3"),
            Just("Missing!A1"),
            Just("NamedThing"),
            Just("Table1[#Data]"),
            Just("Sheet1:Sheet2!A1"),
            Just("IF(A1,,B2)"),
            Just("SUM(A1,SUM(B2,SUM(C3,D4)))"),
            Just("SUM({A1,B2;C3,D4})"),
            Just("D4:B2"),
            Just("LET(x,A1,y,B2,x+y+C3)"),
            Just("LAMBDA(x,y,x+y+A1)"),
        ]
    }

    // Fixed seeds make CI failures reproducible without committing persistence artifacts.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            rng_seed: proptest::test_runner::RngSeed::Fixed(0x494e_4745),
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_formulas_match_frozen_ingest_walk(
            atoms in prop::collection::vec(dependency_atom(), 1..8),
            limit in prop_oneof![Just(0usize), Just(1), Just(4), Just(16), Just(64)],
        ) {
            ensure_builtins_registered();
            let formula = format!("={}", atoms.join("+"));
            let ast = parse(&formula).unwrap_or_else(|error| panic!("{formula}: {error}"));
            let mut engine = Engine::new(
                TestWorkbook::new(),
                EvalConfig::default()
                    .with_range_expansion_limit(limit)
                    // The frozen reference walk has only Strict semantics.
                    .with_preparation_policy(crate::engine::PreparationPolicy::Strict),
            );
            let sheet = engine.graph.sheet_id_mut("Sheet1");
            engine.graph.sheet_id_mut("Sheet2");
            let mut pipeline = engine.ingest_pipeline();
            assert_ingest_walk_parity(&mut pipeline, &ast, sheet);
        }
    }
}
