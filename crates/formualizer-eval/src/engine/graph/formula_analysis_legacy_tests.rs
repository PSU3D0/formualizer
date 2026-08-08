#![allow(clippy::type_complexity)]

//! Differential oracle frozen from `ccfeaf83`.
//!
//! The legacy methods below are a test-only extraction of
//! `engine/graph/formula_analysis.rs` at that commit. Only method names were
//! prefixed with `legacy_` so the old and consolidated walks can coexist.

use super::*;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnresolvedNamePolicy {
    Error,
    Collect,
}

type LegacyResult = Result<
    (
        Vec<VertexId>,
        Vec<SharedRangeRef<'static>>,
        Vec<CellRef>,
        Vec<VertexId>,
        Vec<String>,
    ),
    ExcelError,
>;

impl DependencyGraph {
    fn legacy_extract_dependencies(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
    ) -> Result<
        (
            Vec<VertexId>,
            Vec<SharedRangeRef<'static>>,
            Vec<CellRef>,
            Vec<VertexId>,
        ),
        ExcelError,
    > {
        let (dependencies, ranges, placeholders, named_dependencies, _) = self
            .legacy_extract_dependencies_inner(
                ast,
                current_sheet_id,
                UnresolvedNamePolicy::Error,
            )?;
        Ok((dependencies, ranges, placeholders, named_dependencies))
    }
    fn legacy_extract_dependencies_inner(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
        unresolved_name_policy: UnresolvedNamePolicy,
    ) -> LegacyResult {
        let mut dependencies = FxHashSet::default();
        let mut range_dependencies: Vec<SharedRangeRef<'static>> = Vec::new();
        let mut created_placeholders = Vec::new();
        let mut named_dependencies = Vec::new();
        let mut unresolved_names = FxHashSet::default();
        let mut local_scopes: Vec<FxHashSet<String>> = Vec::new();
        self.legacy_extract_dependencies_recursive(
            ast,
            current_sheet_id,
            &mut dependencies,
            &mut range_dependencies,
            &mut created_placeholders,
            &mut named_dependencies,
            &mut unresolved_names,
            &mut local_scopes,
            unresolved_name_policy,
        )?;

        // Deduplicate range references.
        let mut deduped_ranges = Vec::new();
        for range_ref in range_dependencies {
            if !deduped_ranges.contains(&range_ref) {
                deduped_ranges.push(range_ref);
            }
        }

        named_dependencies.sort_unstable_by_key(|v| v.0);
        named_dependencies.dedup_by_key(|v| v.0);

        let mut unresolved_names: Vec<String> = unresolved_names.into_iter().collect();
        unresolved_names.sort();

        Ok((
            dependencies.into_iter().collect(),
            deduped_ranges,
            created_placeholders,
            named_dependencies,
            unresolved_names,
        ))
    }

    fn legacy_extract_dependencies_recursive(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
        dependencies: &mut FxHashSet<VertexId>,
        range_dependencies: &mut Vec<SharedRangeRef<'static>>,
        created_placeholders: &mut Vec<CellRef>,
        named_dependencies: &mut Vec<VertexId>,
        unresolved_names: &mut FxHashSet<String>,
        local_scopes: &mut Vec<FxHashSet<String>>,
        unresolved_name_policy: UnresolvedNamePolicy,
    ) -> Result<(), ExcelError> {
        match &ast.node_type {
            ASTNodeType::Reference { reference, .. } => match reference {
                ReferenceType::External(ext) => match ext.kind {
                    formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                        let name = ext.raw.as_str();
                        if let Some(source) = self.resolve_source_scalar_entry(name) {
                            dependencies.insert(source.vertex);
                        } else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined name: {name}")));
                        }
                    }
                    formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                        let name = ext.raw.as_str();
                        if let Some(source) = self.resolve_source_table_entry(name) {
                            dependencies.insert(source.vertex);
                        } else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined table: {name}")));
                        }
                    }
                },
                ReferenceType::Cell { .. } => {
                    let vertex_id = self.legacy_get_or_create_vertex_for_reference(
                        reference,
                        current_sheet_id,
                        created_placeholders,
                    )?;
                    dependencies.insert(vertex_id);
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    ..
                } => {
                    // If any bound is missing (infinite/partial range), always keep compressed.
                    let has_unbounded = start_row.is_none()
                        || end_row.is_none()
                        || start_col.is_none()
                        || end_col.is_none();
                    if has_unbounded {
                        if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                            let owned = range.into_owned();
                            let sheet_id = match owned.sheet {
                                SharedSheetLocator::Id(id) => id,
                                SharedSheetLocator::Current => current_sheet_id,
                                SharedSheetLocator::Name(name) => {
                                    self.resolve_existing_sheet_id(name.as_ref())?
                                }
                            };
                            range_dependencies.push(SharedRangeRef {
                                sheet: SharedSheetLocator::Id(sheet_id),
                                start_row: owned.start_row,
                                start_col: owned.start_col,
                                end_row: owned.end_row,
                                end_col: owned.end_col,
                            });
                        }
                    } else {
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

                        if size <= self.config.range_expansion_limit {
                            // Expand to individual cells.
                            let sheet_id = match sheet {
                                Some(name) => self.resolve_existing_sheet_id(name)?,
                                None => current_sheet_id,
                            };
                            for row in sr..=er {
                                for col in sc..=ec {
                                    let coord = Coord::from_excel(row, col, true, true);
                                    let addr = CellRef::new(sheet_id, coord);
                                    let vertex_id =
                                        self.get_or_create_vertex(&addr, created_placeholders);
                                    dependencies.insert(vertex_id);
                                }
                            }
                        } else {
                            // Keep as a compressed range dependency.
                            if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                                let owned = range.into_owned();
                                let sheet_id = match owned.sheet {
                                    SharedSheetLocator::Id(id) => id,
                                    SharedSheetLocator::Current => current_sheet_id,
                                    SharedSheetLocator::Name(name) => {
                                        self.resolve_existing_sheet_id(name.as_ref())?
                                    }
                                };
                                range_dependencies.push(SharedRangeRef {
                                    sheet: SharedSheetLocator::Id(sheet_id),
                                    start_row: owned.start_row,
                                    start_col: owned.start_col,
                                    end_row: owned.end_row,
                                    end_col: owned.end_col,
                                });
                            }
                        }
                    }
                }
                ReferenceType::NamedRange(name) => {
                    let key = name.to_ascii_uppercase();
                    if local_scopes.iter().rev().any(|scope| scope.contains(&key)) {
                        return Ok(());
                    }

                    if let Some(named_range) = self.resolve_name_entry(name, current_sheet_id) {
                        dependencies.insert(named_range.vertex);
                        named_dependencies.push(named_range.vertex);
                    } else if let Some(source) = self.resolve_source_scalar_entry(name) {
                        dependencies.insert(source.vertex);
                    } else {
                        match unresolved_name_policy {
                            UnresolvedNamePolicy::Error => {
                                return Err(ExcelError::new(ExcelErrorKind::Name)
                                    .with_message(format!("Undefined name: {name}")));
                            }
                            UnresolvedNamePolicy::Collect => {
                                unresolved_names.insert(name.to_string());
                            }
                        }
                    }
                }
                ReferenceType::Table(tref) => {
                    if let Some(table) = self.resolve_table_entry(&tref.name) {
                        dependencies.insert(table.vertex);
                    } else if let Some(source) = self.resolve_source_table_entry(&tref.name) {
                        dependencies.insert(source.vertex);
                    } else {
                        return Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined table: {}", tref.name)));
                    }
                }
                // 3D references parse correctly but aren't yet wired through
                // the dependency graph; treat them as no-op dependencies for
                // now so formulas containing them still load. Evaluation will
                // surface #N/IMPL! via the Resolver path.
                ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {}
            },
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.legacy_extract_dependencies_recursive(
                    left,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                    named_dependencies,
                    unresolved_names,
                    local_scopes,
                    unresolved_name_policy,
                )?;
                self.legacy_extract_dependencies_recursive(
                    right,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                    named_dependencies,
                    unresolved_names,
                    local_scopes,
                    unresolved_name_policy,
                )?;
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.legacy_extract_dependencies_recursive(
                    expr,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                    named_dependencies,
                    unresolved_names,
                    local_scopes,
                    unresolved_name_policy,
                )?;
            }
            ASTNodeType::Function { args, .. } => {
                for arg in args {
                    self.legacy_extract_dependencies_recursive(
                        arg,
                        current_sheet_id,
                        dependencies,
                        range_dependencies,
                        created_placeholders,
                        named_dependencies,
                        unresolved_names,
                        local_scopes,
                        unresolved_name_policy,
                    )?;
                }
            }
            ASTNodeType::Call { callee, args } => {
                // Walk both the callee and the call arguments so any references
                // they contain are tracked. Full evaluator semantics for
                // immediate-invocation calls are not yet implemented, but
                // dependency collection must still cover them.
                self.legacy_extract_dependencies_recursive(
                    callee,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                    named_dependencies,
                    unresolved_names,
                    local_scopes,
                    unresolved_name_policy,
                )?;
                for arg in args {
                    self.legacy_extract_dependencies_recursive(
                        arg,
                        current_sheet_id,
                        dependencies,
                        range_dependencies,
                        created_placeholders,
                        named_dependencies,
                        unresolved_names,
                        local_scopes,
                        unresolved_name_policy,
                    )?;
                }
            }
            ASTNodeType::Array(rows) => {
                for item in rows.iter().flatten() {
                    self.legacy_extract_dependencies_recursive(
                        item,
                        current_sheet_id,
                        dependencies,
                        range_dependencies,
                        created_placeholders,
                        named_dependencies,
                        unresolved_names,
                        local_scopes,
                        unresolved_name_policy,
                    )?;
                }
            }
            ASTNodeType::Literal(_) | ASTNodeType::Omitted => {}
        }
        Ok(())
    }

    /// Gets the VertexId for a reference, creating a placeholder vertex if it doesn't exist.
    fn legacy_get_or_create_vertex_for_reference(
        &mut self,
        reference: &ReferenceType,
        current_sheet_id: SheetId,
        created_placeholders: &mut Vec<CellRef>,
    ) -> Result<VertexId, ExcelError> {
        match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet_id = match sheet {
                    Some(name) => self.resolve_existing_sheet_id(name)?,
                    None => current_sheet_id,
                };
                let coord = Coord::from_excel(*row, *col, true, true);
                let addr = CellRef::new(sheet_id, coord);
                Ok(self.get_or_create_vertex(&addr, created_placeholders))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Expected a cell reference, but got a range or other type.")),
        }
    }
}

#[cfg(test)]
mod differential {
    use super::*;
    use crate::engine::EvalConfig;
    use formualizer_parse::parse;
    use proptest::prelude::*;

    fn configure_graph(graph: &mut DependencyGraph) -> SheetId {
        let sheet = graph.sheet_reg.id_for("Sheet1");
        graph.sheet_reg.id_for("Sheet2");
        graph.sheet_reg.id_for("Missing");
        graph
            .define_name(
                "NamedThing",
                crate::engine::named_range::NamedDefinition::Literal(LiteralValue::Number(1.0)),
                crate::engine::named_range::NameScope::Workbook,
            )
            .unwrap();
        sheet
    }

    fn graphs(limit: usize) -> (DependencyGraph, DependencyGraph, SheetId) {
        let config = EvalConfig::default().with_range_expansion_limit(limit);
        let mut legacy = DependencyGraph::new_with_config(config.clone());
        let mut consolidated = DependencyGraph::new_with_config(config);
        let legacy_sheet = configure_graph(&mut legacy);
        let consolidated_sheet = configure_graph(&mut consolidated);
        assert_eq!(legacy_sheet, consolidated_sheet);
        (legacy, consolidated, legacy_sheet)
    }

    fn normalize(
        result: Result<
            (
                Vec<VertexId>,
                Vec<SharedRangeRef<'static>>,
                Vec<CellRef>,
                Vec<VertexId>,
            ),
            ExcelError,
        >,
    ) -> Result<
        (
            Vec<VertexId>,
            Vec<SharedRangeRef<'static>>,
            Vec<CellRef>,
            Vec<VertexId>,
        ),
        ExcelError,
    > {
        result.map(|(mut dependencies, ranges, placeholders, mut names)| {
            dependencies.sort_unstable_by_key(|vertex| vertex.0);
            names.sort_unstable_by_key(|vertex| vertex.0);
            (dependencies, ranges, placeholders, names)
        })
    }

    fn assert_formula_parity(formula: &str, limit: usize) -> bool {
        let ast = parse(formula).unwrap_or_else(|error| panic!("{formula}: {error}"));
        let (mut legacy, mut consolidated, sheet) = graphs(limit);
        let old = normalize(legacy.legacy_extract_dependencies(&ast, sheet));
        let succeeded = old.is_ok();
        let new = normalize(consolidated.extract_dependencies(&ast, sheet));
        assert_eq!(old, new, "tree formula={formula}, limit={limit}");

        let config = EvalConfig::default().with_range_expansion_limit(limit);
        let mut arena = DependencyGraph::new_with_config(config);
        let arena_sheet = configure_graph(&mut arena);
        let ast_id = arena.data_store.store_ast(&ast, &arena.sheet_reg);
        let arena_result = normalize(arena.extract_dependencies_arena(ast_id, arena_sheet));
        assert_eq!(old, arena_result, "arena formula={formula}, limit={limit}");
        succeeded
    }

    #[test]
    fn frozen_walk_matches_all_reference_classes_and_error_outcomes() {
        let formulas = [
            "=SUM(A1,$B2,C$3,$D$4)",
            "=SUM(A1:A1,A1:B2,Sheet2!C3:D4,A1:A,A1:1,A:A,1:1)",
            "=SUM(Sheet2!A1,NamedThing,Table1[#Data])",
            "=SUM([book]Sheet!A1,[book]Sheet!A1:B2)",
            "=SUM(Sheet1:Sheet2!A1,Sheet1:Sheet2!B2:C3)",
            "=SUM({A1,B2;C3,D4},IF(D4,,E5))",
            "=SUM(D4:B2)",
            "=Missing!A1",
        ];
        for formula in formulas {
            for limit in [0, 1, 4, 16, 64] {
                assert_formula_parity(formula, limit);
            }
        }
    }

    #[test]
    fn overflow_sized_ranges_stay_compressed_in_graph() {
        // The frozen oracle intentionally retains base's wrapping u32 multiply,
        // so overflow inputs are covered by direct behavior pins rather than
        // differential comparison (the debug oracle would panic).
        for formula in [
            "=SUM(A1:FLA983055)",
            "=SUM(A1:XFD262144)",
            "=SUM(A1:XFD1048576)",
        ] {
            let ast = parse(formula).unwrap();
            let mut graph = DependencyGraph::new_with_config(
                EvalConfig::default().with_range_expansion_limit(64),
            );
            let sheet = configure_graph(&mut graph);
            let (dependencies, ranges, placeholders, names) =
                graph.extract_dependencies(&ast, sheet).unwrap();
            assert!(dependencies.is_empty(), "{formula}");
            assert_eq!(ranges.len(), 1, "{formula}");
            assert!(placeholders.is_empty(), "{formula}");
            assert!(names.is_empty(), "{formula}");
        }
    }

    fn atom() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("A1"),
            Just("$B2"),
            Just("C$3"),
            Just("$D$4"),
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
            Just("Sheet1:Sheet2!A1"),
            Just("IF(A1,,B2)"),
            Just("SUM(A1,SUM(B2,SUM(C3,D4)))"),
            Just("SUM({A1,B2;C3,D4})"),
        ]
    }

    // Fixed seeds make CI failures reproducible without committing persistence artifacts.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            rng_seed: proptest::test_runner::RngSeed::Fixed(0x4752_4150),
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_formulas_match_frozen_graph_walk(
            atoms in prop::collection::vec(atom(), 1..8),
            limit in prop_oneof![Just(0usize), Just(1), Just(4), Just(16), Just(64)],
        ) {
            let formula = format!("={}", atoms.join("+"));
            assert!(
                assert_formula_parity(&formula, limit),
                "generated graph fixture must exercise full successful outcomes"
            );
        }
    }
}
