//! AST-to-edge parity invariant harness for the engine's dual-write seam.
//!
//! The invariant is: for every formula vertex, the semantic references in its current retained
//! AST (classified by `engine::refs`) are represented by exactly the formula's outgoing graph
//! dependencies. A cell is represented by a direct edge (including an empty placeholder), a
//! finite range at or below `range_expansion_limit` by one edge per cell, a larger or open range
//! by one compressed range dependency, and a name or table by an edge to its resolving vertex.
//! `get_range_dependencies` exposes a stored pre-stripe copy, so stripe-index integrity is covered
//! behaviorally rather than by the structural comparison. Exact parity matters in both directions:
//! a missing dependency can leave a value silently stale, while a phantom dependency causes
//! spurious recalculation.
//!
//! The structural checker compares those representations after edit operations. The behavioral
//! checker independently mutates sampled AST-referenced cells through `Engine`, requires the
//! formula to become dirty, and undoes the mutation through the changelog. Structural failures
//! diagnose the representation; behavioral failures are the scheduling ground truth. If only one
//! fails, its panic reports both the formula and sampled cell so the disagreement is actionable.
//! Campaigns deliberately use this dirty-flag proxy as their oracle and never call `evaluate_all`;
//! value-level consequences are pinned in the dedicated ignored finding tests.
//!
//! The missing-edge direction silently contributes an empty expected set for unresolvable sheets,
//! names, or tables; reversed ranges; and external, three-dimensional, or unsupported references.
//! It is therefore vacuous for those references, while the phantom-edge direction remains covered.
//!
//! Pinned T2 findings, each an `#[ignore]`d test asserting the intended invariant and failing on
//! the live divergence: `AST_EDGE_UNDO_STRUCTURAL_ADJUSTMENT` (undo of a populated row insert
//! leaves data shifted and the edge one row above the AST reference),
//! `AST_EDGE_UNRELATED_DELETE_NAME` (a default-sheet row or column delete drops a cross-sheet
//! workbook-name edge), `AST_EDGE_UNDO_EMPTY_PLACEHOLDER` (undo and redo of a write to a
//! referenced empty placeholder drop its edge), and `AST_EDGE_INSERT_SHIFTS_NAME_VERTEX_ONTO_GRID`
//! (a default-sheet insert shifts a name vertex onto an addressable cell, so later references to
//! that address bind to the name vertex instead of the cell).
//!
//! Campaign seeds are fixed constants below. They are intentionally not persisted: a failure
//! prints its seed and operation index, which is enough to replay the deterministic campaign while
//! keeping the repository free of generated failure corpora.

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::SmallRng};

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::refs::{DeclaredSheet, LocalBindingStyle, SemanticReference};
use crate::engine::{ChangeLog, DependencyGraph, Engine, EvalConfig, VertexId, VertexKind};
use crate::reference::{CellRef, Coord, RangeRef, SharedRangeRef, SharedSheetLocator};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;

const MAX_ROW: u32 = 10;
const MAX_COL: u32 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RangeKey {
    sheet_id: u16,
    start_row: Option<u32>,
    start_col: Option<u32>,
    end_row: Option<u32>,
    end_col: Option<u32>,
}

impl RangeKey {
    fn contains(self, cell: CellRef) -> bool {
        self.sheet_id == cell.sheet_id
            && self.start_row.is_none_or(|start| cell.coord.row() >= start)
            && self.end_row.is_none_or(|end| cell.coord.row() <= end)
            && self.start_col.is_none_or(|start| cell.coord.col() >= start)
            && self.end_col.is_none_or(|end| cell.coord.col() <= end)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct DependencyShape {
    cells: BTreeSet<CellRef>,
    ranges: BTreeSet<RangeKey>,
    symbols: BTreeSet<VertexId>,
}

struct AstShapeContext<'a> {
    graph: &'a DependencyGraph,
    formula: VertexId,
    formula_cell: CellRef,
    shape: DependencyShape,
    behavioral_cells: BTreeSet<CellRef>,
}

fn no_local_bindings(_: &AstShapeContext<'_>, _: &str, _: usize) -> LocalBindingStyle {
    LocalBindingStyle::None
}

fn resolve_sheet(
    graph: &DependencyGraph,
    declared: DeclaredSheet<'_>,
    current_sheet: u16,
) -> Option<u16> {
    match declared {
        DeclaredSheet::Current => Some(current_sheet),
        DeclaredSheet::Name(name) => graph.sheet_id(name),
    }
}

fn range_key(
    graph: &DependencyGraph,
    range: crate::engine::refs::RangeReference<'_>,
    current_sheet: u16,
) -> Option<RangeKey> {
    Some(RangeKey {
        sheet_id: resolve_sheet(graph, range.sheet, current_sheet)?,
        start_row: range.start_row.map(|row| row.saturating_sub(1)),
        start_col: range.start_col.map(|col| col.saturating_sub(1)),
        end_row: range.end_row.map(|row| row.saturating_sub(1)),
        end_col: range.end_col.map(|col| col.saturating_sub(1)),
    })
}

fn sample_cells_in_range(key: RangeKey) -> [CellRef; 2] {
    let start_row = key.start_row.unwrap_or(0);
    let start_col = key.start_col.unwrap_or(0);
    let end_row = key.end_row.unwrap_or(start_row.saturating_add(MAX_ROW - 1));
    let end_col = key.end_col.unwrap_or(start_col.saturating_add(MAX_COL - 1));
    [
        CellRef::new(
            key.sheet_id,
            Coord::new(start_row.min(end_row), start_col.min(end_col), true, true),
        ),
        CellRef::new(
            key.sheet_id,
            Coord::new(end_row.max(start_row), end_col.max(start_col), true, true),
        ),
    ]
}

fn collect_ast_reference(
    context: &mut AstShapeContext<'_>,
    reference: SemanticReference<'_>,
) -> Result<(), ExcelError> {
    match reference {
        SemanticReference::Cell(cell) => {
            if let Some(sheet_id) =
                resolve_sheet(context.graph, cell.sheet, context.formula_cell.sheet_id)
            {
                let target =
                    CellRef::new(sheet_id, Coord::from_excel(cell.row, cell.col, true, true));
                context.shape.cells.insert(target);
                if target != context.formula_cell {
                    context.behavioral_cells.insert(target);
                }
            }
        }
        SemanticReference::FiniteRange(range) => {
            let Some(key) = range_key(context.graph, range, context.formula_cell.sheet_id) else {
                return Ok(());
            };
            if range.is_reversed() {
                return Ok(());
            }
            if range.saturating_area().unwrap_or(u64::MAX)
                <= context.graph.range_expansion_limit() as u64
            {
                let (start_row, start_col, end_row, end_col) = range
                    .finite_bounds()
                    .expect("finite reference must have finite bounds");
                for row in start_row..=end_row {
                    for col in start_col..=end_col {
                        let target =
                            CellRef::new(key.sheet_id, Coord::from_excel(row, col, true, true));
                        context.shape.cells.insert(target);
                        if target != context.formula_cell {
                            context.behavioral_cells.insert(target);
                        }
                    }
                }
            } else {
                context.shape.ranges.insert(key);
                context.behavioral_cells.extend(
                    sample_cells_in_range(key)
                        .into_iter()
                        .filter(|sample| *sample != context.formula_cell),
                );
                if key.contains(context.formula_cell) {
                    context.shape.cells.insert(context.formula_cell);
                }
            }
        }
        SemanticReference::OpenRange(range) => {
            let Some(key) = range_key(context.graph, range, context.formula_cell.sheet_id) else {
                return Ok(());
            };
            context.shape.ranges.insert(key);
            context.behavioral_cells.extend(
                sample_cells_in_range(key)
                    .into_iter()
                    .filter(|sample| *sample != context.formula_cell),
            );
            if key.contains(context.formula_cell) {
                context.shape.cells.insert(context.formula_cell);
            }
        }
        SemanticReference::Name(name) => {
            if let Some(named) = context
                .graph
                .resolve_name_entry(name, context.formula_cell.sheet_id)
            {
                context.shape.symbols.insert(named.vertex);
            }
        }
        SemanticReference::Table(table) => {
            if let Some(entry) = context.graph.resolve_table_entry(&table.name) {
                context.shape.symbols.insert(entry.vertex);
            }
        }
        SemanticReference::ExternalSource(_)
        | SemanticReference::ThreeDimensional(_)
        | SemanticReference::Unsupported(_) => {}
    }
    Ok(())
}

fn ast_shape(graph: &DependencyGraph, formula: VertexId) -> AstShapeContext<'_> {
    let formula_cell = graph
        .get_cell_ref(formula)
        .expect("formula vertex must have a cell address");
    let ast = graph
        .get_formula(formula)
        .expect("formula vertex must retain its AST");
    let mut context = AstShapeContext {
        graph,
        formula,
        formula_cell,
        shape: DependencyShape::default(),
        behavioral_cells: BTreeSet::new(),
    };
    crate::engine::refs::visit_tree_references(
        &ast,
        &mut context,
        no_local_bindings,
        collect_ast_reference,
    )
    .expect("retained formula reference collection must succeed");
    context
}

fn actual_range_key(
    graph: &DependencyGraph,
    formula_sheet: u16,
    range: &SharedRangeRef<'static>,
) -> Option<RangeKey> {
    let sheet_id = match &range.sheet {
        SharedSheetLocator::Id(id) => *id,
        SharedSheetLocator::Current => formula_sheet,
        SharedSheetLocator::Name(name) => graph.sheet_id(name.as_ref())?,
    };
    Some(RangeKey {
        sheet_id,
        start_row: range.start_row.map(|bound| bound.index),
        start_col: range.start_col.map(|bound| bound.index),
        end_row: range.end_row.map(|bound| bound.index),
        end_col: range.end_col.map(|bound| bound.index),
    })
}

fn graph_shape(graph: &DependencyGraph, formula: VertexId) -> DependencyShape {
    let formula_sheet = graph.get_vertex_sheet_id(formula);
    let mut shape = DependencyShape::default();
    for dependency in graph.get_dependencies(formula) {
        match graph.get_vertex_kind(dependency) {
            VertexKind::Cell
            | VertexKind::Empty
            | VertexKind::FormulaScalar
            | VertexKind::FormulaArray => {
                shape.cells.insert(
                    graph
                        .get_cell_ref(dependency)
                        .expect("cell-like dependency must have an address"),
                );
            }
            VertexKind::NamedScalar
            | VertexKind::NamedArray
            | VertexKind::Table
            | VertexKind::External
            | VertexKind::InfiniteRange
            | VertexKind::Range => {
                shape.symbols.insert(dependency);
            }
        }
    }
    if let Some(ranges) = graph.get_range_dependencies(formula) {
        shape.ranges.extend(
            ranges
                .iter()
                .filter_map(|range| actual_range_key(graph, formula_sheet, range)),
        );
    }
    shape
}

fn assert_structural_parity(engine: &Engine<TestWorkbook>, seed: u64, operation: usize) {
    for formula in engine.graph.formula_vertices() {
        let expected = ast_shape(&engine.graph, formula);
        let actual = graph_shape(&engine.graph, formula);
        assert_eq!(
            actual,
            expected.shape,
            "AST/edge structural divergence: seed={seed:#x} operation={operation} formula={} vertex={formula:?}",
            engine.graph.to_a1(expected.formula_cell),
        );
    }
}

fn behavior_pairs(engine: &Engine<TestWorkbook>) -> Vec<(VertexId, CellRef)> {
    let mut pairs = Vec::new();
    for formula in engine.graph.formula_vertices() {
        let expected = ast_shape(&engine.graph, formula);
        pairs.extend(expected.behavioral_cells.into_iter().filter_map(|cell| {
            let existing = engine.graph.get_vertex_for_cell(&cell)?;
            let sheet = engine.graph.sheet_name(cell.sheet_id);
            let value = engine.get_cell_value(sheet, cell.coord.row() + 1, cell.coord.col() + 1);
            (engine.graph.get_vertex_kind(existing) == VertexKind::Cell
                && value.is_some_and(|value| value != LiteralValue::Empty))
            .then_some((formula, cell))
        }));
    }
    pairs
}

fn assert_behavioral_parity(
    engine: &mut Engine<TestWorkbook>,
    seed: u64,
    operation: usize,
    sample_limit: usize,
) {
    let pairs = behavior_pairs(engine);
    if pairs.is_empty() {
        return;
    }
    let start = (seed as usize ^ operation.wrapping_mul(0x9e37)) % pairs.len();
    for offset in 0..sample_limit.min(pairs.len()) {
        let (formula, referenced_cell) = pairs[(start + offset) % pairs.len()];
        if !engine.graph.vertex_exists(formula) {
            continue;
        }
        let formula_cell = engine
            .graph
            .get_cell_ref(formula)
            .expect("formula vertex must retain its address during a value probe");
        let sheet = engine
            .graph
            .sheet_name(referenced_cell.sheet_id)
            .to_string();
        let row = referenced_cell.coord.row() + 1;
        let col = referenced_cell.coord.col() + 1;
        let formulas = engine.graph.formula_vertices();
        engine.graph.clear_dirty_flags(&formulas);

        let mut probe_log = ChangeLog::new();
        let marker = LiteralValue::Number(seed as f64 + operation as f64 + offset as f64 + 0.5);
        engine
            .action_with_logger(&mut probe_log, "ast-edge-behavior-probe", |action| {
                action.set_cell_value(&sheet, row, col, marker)
            })
            .expect("behavioral probe mutation must succeed");
        assert!(
            engine.graph.vertex_exists(formula) && engine.graph.is_dirty(formula),
            "AST/edge behavioral divergence: seed={seed:#x} operation={operation} formula={} referenced_cell={} (structural checker passed)",
            engine.graph.to_a1(formula_cell),
            engine.graph.to_a1(referenced_cell),
        );

        let mut probe_undo = UndoEngine::new();
        engine
            .undo_logged(&mut probe_undo, &mut probe_log)
            .expect("behavioral probe undo must succeed");
        assert_structural_parity(engine, seed, operation);
    }
}

#[derive(Clone, Copy)]
struct Campaign {
    seed: u64,
    operations: usize,
    expansion_limit: usize,
    assert_every: usize,
    behavior_samples: usize,
}

#[derive(Default)]
struct CampaignStats {
    undo: usize,
    redo: usize,
}

fn random_sheet<'a>(rng: &mut SmallRng, sheets: &'a [String]) -> &'a str {
    &sheets[rng.gen_range(0..sheets.len())]
}

fn random_formula(rng: &mut SmallRng, sheets: &[String], current_sheet: &str) -> String {
    let row1 = rng.gen_range(1..=MAX_ROW);
    let row2 = rng.gen_range(row1..=MAX_ROW);
    let col1 = rng.gen_range(1..=MAX_COL);
    let other = random_sheet(rng, sheets);
    let col_letter = Coord::col_to_letters(col1 - 1);
    match rng.gen_range(0..8) {
        0 => format!("={col_letter}{row1}+1"),
        1 => format!("=SUM(A{row1}:B{row2})"),
        2 => "=SUM(A1:H10)".to_string(),
        3 => "=SUM(A:A)".to_string(),
        4 if other != current_sheet => format!("='{other}'!{col_letter}{row1}*2"),
        5 if other != current_sheet => format!("=SUM('{other}'!A{row1}:B{row2})"),
        6 => "=Tracked+1".to_string(),
        _ => format!("={col_letter}{row1}+SUM(C1:C{row2})"),
    }
}

/// The workbook name's vertex physically occupies a cell on the default sheet. A default-sheet
/// insert at or above that cell shifts the vertex onto an addressable grid cell, after which any
/// later reference to that address binds to the name vertex instead of the cell — finding
/// `AST_EDGE_INSERT_SHIFTS_NAME_VERTEX_ONTO_GRID`, pinned by
/// `default_sheet_insertion_keeps_the_name_vertex_off_the_addressable_grid`. Campaigns keep their
/// default-sheet inserts strictly below and to the right of the name vertex so they exercise the
/// healthy shape; every other default-sheet edit, including formula and value overwrite of shifted
/// formulas, stays in play.
fn insert_clear_of_name_vertex(
    engine: &Engine<TestWorkbook>,
    sheet: &str,
    row: u32,
    col: u32,
) -> (u32, u32) {
    let Some(sheet_id) = engine.sheet_id(sheet) else {
        return (row, col);
    };
    let Some(named) = engine.graph.resolve_name_entry("Tracked", sheet_id) else {
        return (row, col);
    };
    let Some(anchor) = engine.graph.get_cell_ref(named.vertex) else {
        return (row, col);
    };
    if anchor.sheet_id != sheet_id {
        return (row, col);
    }
    (
        row.max(anchor.coord.row() + 2),
        col.max(anchor.coord.col() + 2),
    )
}

fn reset_history(log: &mut ChangeLog, undo: &mut UndoEngine, can_redo: &mut bool) {
    log.clear();
    *undo = UndoEngine::new();
    *can_redo = false;
}

fn run_campaign(campaign: Campaign) -> CampaignStats {
    let config = EvalConfig::default().with_range_expansion_limit(campaign.expansion_limit);
    let mut engine = Engine::new(TestWorkbook::new(), config);
    let mut sheets = vec!["Sheet1".to_string(), "Sheet2".to_string()];
    engine.add_sheet("Sheet2").unwrap();
    for sheet in &sheets {
        for row in 1..=MAX_ROW {
            for col in 1..=MAX_COL {
                engine
                    .set_cell_value(
                        sheet,
                        row,
                        col,
                        LiteralValue::Number(f64::from(row * MAX_COL + col)),
                    )
                    .unwrap();
            }
        }
    }
    let tracked_sheet = engine.sheet_id("Sheet1").unwrap();
    engine
        .define_name(
            "Tracked",
            NamedDefinition::Cell(CellRef::new(
                tracked_sheet,
                Coord::from_excel(2, 1, true, true),
            )),
            NameScope::Workbook,
        )
        .unwrap();

    let mut rng = SmallRng::seed_from_u64(campaign.seed);
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();
    let mut can_redo = false;
    let mut stats = CampaignStats::default();

    for operation in 0..campaign.operations {
        let force_redo = can_redo && rng.gen_bool(0.4);
        if force_redo {
            engine.redo_logged(&mut undo, &mut log).unwrap();
            can_redo = false;
            stats.redo += 1;
        }

        let choice = rng.gen_range(0..100);
        let sheet = random_sheet(&mut rng, &sheets).to_string();
        let row = rng.gen_range(1..=MAX_ROW);
        let col = rng.gen_range(1..=MAX_COL);

        match choice {
            _ if force_redo => {}
            0..=13 => {
                engine
                    .action_with_logger(&mut log, "campaign-set-value", |action| {
                        action.set_cell_value(
                            &sheet,
                            row,
                            col,
                            LiteralValue::Number(rng.gen_range(-100.0..100.0)),
                        )
                    })
                    .unwrap();
                undo = UndoEngine::new();
                can_redo = false;
            }
            14..=29 => {
                let formula = random_formula(&mut rng, &sheets, &sheet);
                engine
                    .action_with_logger(&mut log, "campaign-set-formula", |action| {
                        action.set_cell_formula(&sheet, row, col, parse(&formula).unwrap())
                    })
                    .unwrap();
                undo = UndoEngine::new();
                can_redo = false;
            }
            30..=40 => {
                let (insert_row, _) = insert_clear_of_name_vertex(&engine, &sheet, row, col);
                engine
                    .action_with_logger(&mut log, "campaign-insert-rows", |action| {
                        action.insert_rows(&sheet, insert_row, 1).map(|_| ())
                    })
                    .unwrap();
                reset_history(&mut log, &mut undo, &mut can_redo);
            }
            41..=51 => {
                let delete_sheet = if sheet == "Sheet1" { "Sheet2" } else { &sheet };
                engine.delete_rows(delete_sheet, row, 1).unwrap();
                reset_history(&mut log, &mut undo, &mut can_redo);
            }
            52..=61 => {
                let (_, insert_col) = insert_clear_of_name_vertex(&engine, &sheet, row, col);
                engine
                    .action_with_logger(&mut log, "campaign-insert-columns", |action| {
                        action.insert_columns(&sheet, insert_col, 1).map(|_| ())
                    })
                    .unwrap();
                reset_history(&mut log, &mut undo, &mut can_redo);
            }
            62..=71 => {
                let delete_sheet = if sheet == "Sheet1" { "Sheet2" } else { &sheet };
                engine.delete_columns(delete_sheet, col, 1).unwrap();
                reset_history(&mut log, &mut undo, &mut can_redo);
            }
            72..=79 => {
                let formulas = engine.graph.formula_vertices();
                if let Some(formula) = formulas.get(rng.gen_range(0..formulas.len().max(1))) {
                    let target = engine.graph.get_cell_ref(*formula).unwrap();
                    let target_sheet = engine.graph.sheet_name(target.sheet_id).to_string();
                    engine
                        .action_with_logger(&mut log, "campaign-delete-formula", |action| {
                            action.set_cell_value(
                                &target_sheet,
                                target.coord.row() + 1,
                                target.coord.col() + 1,
                                LiteralValue::Empty,
                            )
                        })
                        .unwrap();
                    undo = UndoEngine::new();
                    can_redo = false;
                }
            }
            80..=85 => {
                let target = CellRef::new(
                    engine.sheet_id(&sheet).unwrap(),
                    Coord::from_excel(row, col, true, true),
                );
                log.begin_compound("campaign-redefine-name".to_string());
                engine
                    .update_name_with_logger(
                        &mut log,
                        "Tracked",
                        NamedDefinition::Cell(target),
                        NameScope::Workbook,
                    )
                    .unwrap();
                log.end_compound();
                undo = UndoEngine::new();
                can_redo = false;
            }
            86..=89 if sheets.len() < 3 => {
                let name = format!("Sheet{}", sheets.len() + 1);
                engine.add_sheet(&name).unwrap();
                for seed_row in 1..=MAX_ROW {
                    for seed_col in 1..=MAX_COL {
                        engine
                            .set_cell_value(
                                &name,
                                seed_row,
                                seed_col,
                                LiteralValue::Number(f64::from(seed_row * MAX_COL + seed_col)),
                            )
                            .unwrap();
                    }
                }
                sheets.push(name);
                reset_history(&mut log, &mut undo, &mut can_redo);
            }
            90..=95 if !log.is_empty() => {
                engine.undo_logged(&mut undo, &mut log).unwrap();
                can_redo = true;
                stats.undo += 1;
            }
            96..=99 if can_redo => {
                engine.redo_logged(&mut undo, &mut log).unwrap();
                can_redo = false;
                stats.redo += 1;
            }
            _ => {
                engine
                    .set_cell_value(&sheet, row, col, LiteralValue::Number(operation as f64))
                    .unwrap();
                reset_history(&mut log, &mut undo, &mut can_redo);
            }
        }

        if (operation + 1) % campaign.assert_every == 0 || operation + 1 == campaign.operations {
            assert_structural_parity(&engine, campaign.seed, operation);
            assert_behavioral_parity(
                &mut engine,
                campaign.seed,
                operation,
                campaign.behavior_samples,
            );
        }
    }
    stats
}

// T2 finding AST_EDGE_UNDO_STRUCTURAL_ADJUSTMENT: see the matching T2 worker-report entry.
// Undo leaves populated data shifted, restores the AST to B6, and mis-restores its edge to B5.
#[test]
#[ignore = "known T2 divergence: undoing populated row insertion leaves data shifted and edge above AST"]
fn undoing_populated_row_insertion_restores_data_ast_and_edge_together() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet2").unwrap();
    for row in 1..=8 {
        engine
            .set_cell_value("Sheet2", row, 2, LiteralValue::Number(f64::from(row * 10)))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 6, 4, parse("='Sheet2'!B6*2").unwrap())
        .unwrap();
    let mut log = ChangeLog::new();
    engine
        .action_with_logger(&mut log, "insert-row", |action| {
            action.insert_rows("Sheet2", 1, 1).map(|_| ())
        })
        .unwrap();
    assert_structural_parity(&engine, 0x0adc_0303, 0);

    let mut undo = UndoEngine::new();
    engine.undo_logged(&mut undo, &mut log).unwrap();

    let shifted_values: Vec<_> = (2..=9)
        .map(|row| engine.get_cell_value("Sheet2", row, 2))
        .collect();
    let expected_shifted_values: Vec<_> = (1..=8)
        .map(|row| Some(LiteralValue::Number(f64::from(row * 10))))
        .collect();
    assert_eq!(shifted_values, expected_shifted_values);

    let formula = engine.graph.formula_vertices()[0];
    let ast = ast_shape(&engine.graph, formula);
    let actual = graph_shape(&engine.graph, formula);
    let sheet2 = engine.sheet_id("Sheet2").unwrap();
    let b5 = CellRef::new(sheet2, Coord::from_excel(5, 2, true, true));
    let b6 = CellRef::new(sheet2, Coord::from_excel(6, 2, true, true));
    assert_eq!(actual.cells, BTreeSet::from([b5]));
    assert_eq!(ast.shape.cells, BTreeSet::from([b6]));
    let structural_failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_structural_parity(&engine, 0x0adc_0303, 1);
    }));
    assert!(
        structural_failure.is_err(),
        "the structural checker must detect the B5/B6 split"
    );

    engine.graph.mark_vertex_dirty(formula);
    engine.evaluate_all().unwrap();
    let evaluated = engine.get_cell_value("Sheet1", 6, 4);
    eprintln!(
        "AST_EDGE_UNDO_STRUCTURAL_ADJUSTMENT: values B2:B9={shifted_values:?}; edge=Sheet2!B5; AST=Sheet2!B6; D6={evaluated:?}; correct D6=120"
    );
    assert_eq!(
        evaluated,
        Some(LiteralValue::Number(120.0)),
        "undo must restore the data and evaluate Sheet1!D6 from the restored Sheet2!B6 value"
    );
}

// T2 finding AST_EDGE_UNRELATED_DELETE_NAME: see the matching entry in the T2 worker report.
// GitHub issue #302's review extension covers both default-sheet row and column deletes. Either
// shape removes a workbook-name edge and strands formulas on another sheet.
#[test]
#[ignore = "known T2 divergence: unrelated default-sheet row or column delete drops a name edge"]
fn deleting_unrelated_default_sheet_row_or_column_preserves_cross_sheet_name_edge() {
    fn dependencies_after_delete(delete_row: bool) -> usize {
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        engine.add_sheet("Sheet2").unwrap();
        let target = CellRef::new(
            engine.sheet_id("Sheet2").unwrap(),
            Coord::from_excel(2, 6, true, true),
        );
        engine
            .define_name(
                "Tracked",
                NamedDefinition::Cell(target),
                NameScope::Workbook,
            )
            .unwrap();
        engine
            .set_cell_formula("Sheet2", 10, 6, parse("=Tracked+1").unwrap())
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .unwrap();
        assert_structural_parity(&engine, 0xc0de_0006, 0);
        if delete_row {
            engine.delete_rows("Sheet1", 1, 1).unwrap();
        } else {
            engine.delete_columns("Sheet1", 1, 1).unwrap();
        }
        engine
            .graph
            .get_dependencies(engine.graph.formula_vertices()[0])
            .len()
    }

    let after_column_delete = dependencies_after_delete(false);
    let after_row_delete = dependencies_after_delete(true);
    assert_eq!(after_column_delete, 0);
    assert_eq!(after_row_delete, 0);
    assert!(
        after_column_delete > 0 && after_row_delete > 0,
        "default-sheet row and column deletes must both preserve the cross-sheet name edge"
    );
}

// T2 finding AST_EDGE_UNDO_EMPTY_PLACEHOLDER: see the matching entry in the T2 worker report.
// GitHub issue #301's review extension confirms that redo also fails to rebuild the edge.
#[test]
#[ignore = "known T2 divergence: undo and redo of an empty-placeholder write drop its edge"]
fn undoing_value_write_to_referenced_empty_placeholder_preserves_formula_edge() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 4, 4, parse("=C6+1").unwrap())
        .unwrap();
    assert_structural_parity(&engine, 0xc0de_0005, 0);

    let mut log = ChangeLog::new();
    engine
        .action_with_logger(&mut log, "write-placeholder", |action| {
            action.set_cell_value("Sheet1", 6, 3, LiteralValue::Number(7.0))
        })
        .unwrap();
    let mut undo = UndoEngine::new();
    engine.undo_logged(&mut undo, &mut log).unwrap();
    let formula = engine.graph.formula_vertices()[0];
    assert!(engine.graph.get_dependencies(formula).is_empty());

    engine.redo_logged(&mut undo, &mut log).unwrap();
    let dependencies_after_redo = engine.graph.get_dependencies(formula);
    assert!(dependencies_after_redo.is_empty());
    let formulas = engine.graph.formula_vertices();
    engine.graph.clear_dirty_flags(&formulas);
    engine
        .set_cell_value("Sheet1", 6, 3, LiteralValue::Number(555.0))
        .unwrap();
    assert!(!engine.graph.is_dirty(formula));
    engine.evaluate_all().unwrap();
    assert_eq!(engine.get_cell_value("Sheet1", 4, 4), None);
    assert!(
        !dependencies_after_redo.is_empty(),
        "redo must rebuild the C6 edge so a later C6 write dirties and evaluates D4"
    );
}

// T2 finding AST_EDGE_INSERT_SHIFTS_NAME_VERTEX_ONTO_GRID: see the matching T2 worker-report entry.
// A workbook name's vertex physically occupies `Sheet1!$A$1` and is deliberately kept out of the
// cell index while it sits there. A default-sheet insert at or above it shifts the vertex onto an
// addressable grid cell and publishes it in the cell index, so every reference resolved afterwards
// binds to the name vertex instead of the cell: a phantom symbol edge plus a missing direct cell
// edge. No overwrite is involved, and the referring formula need not be one the insert shifted.
#[test]
#[ignore = "known T2 divergence: a default-sheet insert shifts a name vertex onto an addressable cell"]
fn default_sheet_insertion_keeps_the_name_vertex_off_the_addressable_grid() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet2").unwrap();
    let sheet1 = engine.sheet_id("Sheet1").unwrap();
    let sheet2 = engine.sheet_id("Sheet2").unwrap();
    // The name's target lives on Sheet2, so the default-sheet insert cannot move the target and
    // every value below is attributable to the vertex placement alone.
    engine
        .set_cell_value("Sheet2", 4, 4, LiteralValue::Number(7.0))
        .unwrap();
    engine
        .define_name(
            "Tracked",
            NamedDefinition::Cell(CellRef::new(sheet2, Coord::from_excel(4, 4, true, true))),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .set_cell_formula("Sheet2", 1, 1, parse("=Tracked+1").unwrap())
        .unwrap();
    assert_structural_parity(&engine, 0xc0de_0007, 0);

    let name_vertex = engine
        .graph
        .resolve_name_entry("Tracked", sheet2)
        .expect("workbook name must resolve")
        .vertex;
    let a1 = CellRef::new(sheet1, Coord::from_excel(1, 1, true, true));
    let a2 = CellRef::new(sheet1, Coord::from_excel(2, 1, true, true));
    assert_eq!(engine.graph.get_cell_ref(name_vertex), Some(a1));
    assert_eq!(engine.graph.get_vertex_for_cell(&a1), None);

    engine.insert_rows("Sheet1", 1, 1).unwrap();
    assert_eq!(engine.graph.get_cell_ref(name_vertex), Some(a2));
    assert_eq!(engine.graph.get_vertex_for_cell(&a2), Some(name_vertex));

    // A brand-new formula on a cell the insertion never touched, referencing the shifted address.
    engine
        .set_cell_formula("Sheet1", 7, 7, parse("=A2+1").unwrap())
        .unwrap();
    let referring = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet1, Coord::from_excel(7, 7, true, true)))
        .expect("the referring formula must exist");
    let actual = graph_shape(&engine.graph, referring);
    let expected_cells = {
        let expected = ast_shape(&engine.graph, referring);
        assert!(expected.shape.symbols.is_empty());
        expected.shape.cells.clone()
    };
    assert_eq!(actual.symbols, BTreeSet::from([name_vertex]));
    assert!(actual.cells.is_empty());
    assert_eq!(expected_cells, BTreeSet::from([a2]));
    let structural_failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_structural_parity(&engine, 0xc0de_0007, 1);
    }));
    assert!(
        structural_failure.is_err(),
        "the structural checker must detect the phantom name edge and the missing A2 edge"
    );

    // Consequence 1: the referring formula is now a dependent of the name, so editing the name's
    // target spuriously dirties a formula that never mentions the name.
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 7, 7),
        Some(LiteralValue::Number(1.0))
    );
    let formulas = engine.graph.formula_vertices();
    engine.graph.clear_dirty_flags(&formulas);
    engine
        .set_cell_value("Sheet2", 4, 4, LiteralValue::Number(70.0))
        .unwrap();
    let spuriously_dirty = engine.graph.is_dirty(referring);
    assert!(spuriously_dirty);
    engine.evaluate_all().unwrap();

    // Consequence 2: an ordinary data write at the shifted address overwrites the name's own
    // vertex, so the name entry now resolves to a plain default-sheet cell.
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(500.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.graph.get_vertex_kind(name_vertex), VertexKind::Cell);

    // Consequence 3: deleting the row the hijacked vertex now occupies strands the name, and
    // `=Tracked+1` stops tracking its own target.
    engine.delete_rows("Sheet1", 2, 1).unwrap();
    engine.evaluate_all().unwrap();
    let tracked_formula = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet2, Coord::from_excel(1, 1, true, true)))
        .expect("the name-consuming formula must exist");
    assert!(engine.graph.get_dependencies(tracked_formula).is_empty());
    let formulas = engine.graph.formula_vertices();
    engine.graph.clear_dirty_flags(&formulas);
    engine
        .set_cell_value("Sheet2", 4, 4, LiteralValue::Number(700.0))
        .unwrap();
    assert!(!engine.graph.is_dirty(tracked_formula));
    engine.evaluate_all().unwrap();
    let evaluated = engine.get_cell_value("Sheet2", 1, 1);
    eprintln!(
        "AST_EDGE_INSERT_SHIFTS_NAME_VERTEX_ONTO_GRID: name vertex Sheet1!$A$1 -> Sheet1!$A$2; \
Sheet1!G7 deps={:?} AST cells={:?}; name-target edit dirties Sheet1!G7={spuriously_dirty}; \
name vertex kind after Sheet1!A2=500 is Cell; Sheet2!A1={evaluated:?}; correct Sheet2!A1=701",
        actual.symbols, expected_cells,
    );
    assert_eq!(
        evaluated,
        Some(LiteralValue::Number(701.0)),
        "a default-sheet insert must not park the name vertex on an addressable cell: \
Sheet1!G7 must take a direct Sheet1!A2 edge, the name target must not dirty it, and \
`=Tracked+1` must still recompute from Sheet2!D4"
    );
}

#[test]
fn randomized_structural_edits_keep_ast_and_graph_dependencies_exact() {
    let campaigns = [
        Campaign {
            seed: 0xa57e_d9e0_0000_0000,
            operations: 50,
            expansion_limit: 0,
            assert_every: 1,
            behavior_samples: 1,
        },
        Campaign {
            seed: 0xa57e_d9e0_0000_0001,
            operations: 60,
            expansion_limit: 1,
            assert_every: 1,
            behavior_samples: 1,
        },
        Campaign {
            seed: 0xa57e_d9e0_0000_0010,
            operations: 120,
            expansion_limit: 16,
            assert_every: 10,
            behavior_samples: 2,
        },
        Campaign {
            seed: 0xa57e_d9e0_0000_0040,
            operations: 160,
            expansion_limit: 64,
            assert_every: 10,
            behavior_samples: 2,
        },
    ];
    let stats = campaigns
        .into_iter()
        .fold(CampaignStats::default(), |mut total, campaign| {
            let campaign_stats = run_campaign(campaign);
            total.undo += campaign_stats.undo;
            total.redo += campaign_stats.redo;
            total
        });
    eprintln!(
        "campaign history operations realized: undo={} redo={}",
        stats.undo, stats.redo
    );
    assert!(
        stats.redo > 0,
        "default campaigns must realize at least one redo operation"
    );
}

#[test]
fn changelog_undo_and_redo_formula_overwrite_preserve_ast_edge_parity() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    let mut log = ChangeLog::new();
    engine
        .action_with_logger(&mut log, "overwrite-formula", |action| {
            action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())
        })
        .unwrap();
    assert_structural_parity(&engine, 0xc0de_0008, 0);

    let mut undo = UndoEngine::new();
    engine.undo_logged(&mut undo, &mut log).unwrap();
    assert_structural_parity(&engine, 0xc0de_0008, 1);
    engine.redo_logged(&mut undo, &mut log).unwrap();
    assert_structural_parity(&engine, 0xc0de_0008, 2);
}

#[test]
fn row_and_column_adjustment_keeps_compressed_ranges_in_ast_edge_parity() {
    let config = EvalConfig::default().with_range_expansion_limit(0);
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_formula("Sheet1", 1, 8, parse("=SUM(A2:F9)").unwrap())
        .unwrap();
    assert_structural_parity(&engine, 0xc0de_0001, 0);

    engine.insert_rows("Sheet1", 2, 1).unwrap();
    assert_structural_parity(&engine, 0xc0de_0001, 1);
    assert_behavioral_parity(&mut engine, 0xc0de_0001, 1, 2);

    engine.delete_rows("Sheet1", 5, 1).unwrap();
    assert_structural_parity(&engine, 0xc0de_0001, 2);
    engine.insert_columns("Sheet1", 2, 1).unwrap();
    assert_structural_parity(&engine, 0xc0de_0001, 3);
    engine.delete_columns("Sheet1", 4, 1).unwrap();
    assert_structural_parity(&engine, 0xc0de_0001, 4);
    assert_behavioral_parity(&mut engine, 0xc0de_0001, 4, 2);
}

#[test]
fn range_area_equal_to_expansion_limit_uses_direct_edges() {
    let config = EvalConfig::default().with_range_expansion_limit(16);
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_formula("Sheet1", 1, 8, parse("=SUM(A2:B9)").unwrap())
        .unwrap();
    engine.insert_rows("Sheet1", 1, 1).unwrap();

    assert_structural_parity(&engine, 0xc0de_0010, 1);
    assert_behavioral_parity(&mut engine, 0xc0de_0010, 1, 2);
}

#[test]
fn deleting_referenced_row_and_column_turns_ast_to_ref_without_phantom_edges() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A2").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=C3").unwrap())
        .unwrap();

    engine.delete_rows("Sheet1", 2, 1).unwrap();
    engine.delete_columns("Sheet1", 3, 1).unwrap();
    assert_structural_parity(&engine, 0xc0de_0002, 2);

    for formula in engine.graph.formula_vertices() {
        let ast = engine.graph.get_formula(formula).unwrap();
        assert!(
            formualizer_parse::pretty::canonical_formula(&ast).contains("#REF!"),
            "deleted direct reference must be retained as #REF!: {ast:?}",
        );
        assert!(
            engine.graph.get_dependencies(formula).is_empty()
                && engine
                    .graph
                    .get_range_dependencies(formula)
                    .is_none_or(Vec::is_empty),
            "#REF! formula must not retain phantom graph dependencies",
        );
    }
}

#[test]
fn sheet_addition_then_adjustment_keeps_unqualified_edges_on_the_formula_sheet() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet2", 2, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet2", 1, 2, parse("=A2").unwrap())
        .unwrap();
    engine.insert_rows("Sheet2", 2, 1).unwrap();

    assert_structural_parity(&engine, 0xc0de_0003, 1);
    assert_behavioral_parity(&mut engine, 0xc0de_0003, 1, 1);
}

#[test]
fn names_and_tables_are_exact_symbol_vertices_and_dirty_their_dependents() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let named_cell = CellRef::new(sheet_id, Coord::from_excel(2, 1, true, true));
    engine
        .define_name(
            "Tracked",
            NamedDefinition::Cell(named_cell),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .define_table(
            "Sales",
            RangeRef::new(
                CellRef::new(sheet_id, Coord::from_excel(1, 3, true, true)),
                CellRef::new(sheet_id, Coord::from_excel(3, 4, true, true)),
            ),
            true,
            vec!["Item".into(), "Amount".into()],
            false,
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            6,
            parse("=Tracked+SUM(Sales[Amount])").unwrap(),
        )
        .unwrap();

    assert_structural_parity(&engine, 0xc0de_0004, 0);
    let formula = engine.graph.formula_vertices()[0];
    let formulas = engine.graph.formula_vertices();
    engine.graph.clear_dirty_flags(&formulas);
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(4.0))
        .unwrap();
    assert!(
        engine.graph.is_dirty(formula),
        "name target must dirty formula"
    );
    engine.graph.clear_dirty_flags(&formulas);
    engine
        .set_cell_value("Sheet1", 2, 4, LiteralValue::Number(8.0))
        .unwrap();
    assert!(
        engine.graph.is_dirty(formula),
        "table cell must dirty formula"
    );
}
