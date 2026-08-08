use crate::engine::inspect::*;
use std::sync::Arc;

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{CycleConfig, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{CellAddress, LiteralValue, RangeArea};
use formualizer_parse::parse;

fn address(sheet: &str, row: u32, column: u32) -> CellAddress {
    CellAddress::new(sheet, row, column).unwrap()
}

fn engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn set_formula(engine: &mut Engine<TestWorkbook>, row: u32, column: u32, formula: &str) {
    engine
        .set_cell_formula("Model", row, column, parse(formula).unwrap())
        .unwrap();
}

#[test]
fn public_precedents_preserve_source_order_shape_and_first_occurrence() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    set_formula(
        &mut engine,
        1,
        2,
        "=A1+A1+SUM(A2:A4)+SUM(INDIRECT(A3))+INDIRECT(\"A4\")",
    );

    let report = engine
        .precedents(&address("model", 1, 2), &PrecedentOptions::default())
        .unwrap();
    assert_eq!(report.cell.sheet, "Model");
    assert_eq!(report.precedents.len(), 3);
    assert_eq!(
        report.precedents[0].reference,
        SemanticReference::Cell(address("Model", 1, 1))
    );
    assert!(matches!(
        &report.precedents[1].reference,
        SemanticReference::Range { declared, .. }
            if declared.start_row == Some(2) && declared.end_row == Some(4)
    ));
    assert_eq!(
        report.precedents[2].reference,
        SemanticReference::Cell(address("Model", 3, 1))
    );
    assert!(
        report
            .precedents
            .iter()
            .all(|precedent| precedent.provenance == Provenance::Declared)
    );

    let repeated = engine
        .precedents(&address("Model", 1, 2), &PrecedentOptions::default())
        .unwrap();
    assert_eq!(report, repeated);
}

#[test]
fn public_trace_distinguishes_diamond_convergence_and_cycles() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 2, 4, LiteralValue::Number(1.0))
        .unwrap();
    set_formula(&mut engine, 1, 1, "=B1+C1");
    set_formula(&mut engine, 1, 2, "=D2");
    set_formula(&mut engine, 1, 3, "=D2");

    let graph = engine
        .trace(
            &[address("Model", 1, 1)],
            &TraceOptions::default().with_range_member_budget(0),
        )
        .unwrap();
    let d2 = graph
        .nodes
        .iter()
        .find(|node| node.cell.address == address("Model", 2, 4))
        .unwrap()
        .id;
    let dispositions: Vec<_> = graph
        .nodes
        .iter()
        .flat_map(|node| &node.links)
        .flat_map(|link| &link.targets)
        .filter(|target| target.node == d2)
        .map(|target| target.disposition)
        .collect();
    assert_eq!(
        dispositions,
        vec![LinkDisposition::Expanded, LinkDisposition::Convergent]
    );

    let mut cycle_engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.0)),
    );
    cycle_engine
        .set_cell_formula("Model", 3, 1, parse("=A3").unwrap())
        .unwrap();
    let cycle = cycle_engine
        .trace(&[address("Model", 3, 1)], &TraceOptions::default())
        .unwrap();
    assert_eq!(cycle.nodes.len(), 1);
    assert_eq!(
        cycle.nodes[0].links[0].targets[0].disposition,
        LinkDisposition::Cycle
    );
}

#[test]
fn compressed_self_containing_range_is_cycle_even_with_no_member_budget() {
    let mut engine = engine();
    set_formula(&mut engine, 1, 1, "=SUM(A:A)");
    let graph = engine
        .trace(
            &[address("Model", 1, 1)],
            &TraceOptions::default().with_range_member_budget(0),
        )
        .unwrap();
    assert!(matches!(
        graph.nodes[0].links[0].reference,
        SemanticReference::Range { .. }
    ));
    assert_eq!(
        graph.nodes[0].links[0].targets[0].disposition,
        LinkDisposition::Cycle
    );
}

#[test]
fn trace_budgets_are_global_and_have_boundary_exactness() {
    let mut engine = engine();
    for row in 1..=3 {
        engine
            .set_cell_value("Model", row, 1, LiteralValue::Number(row.into()))
            .unwrap();
    }
    set_formula(&mut engine, 1, 4, "=SUM(A1:A3)+B1");

    let links = engine
        .precedents(
            &address("Model", 1, 4),
            &PrecedentOptions::default().with_max_links(1),
        )
        .unwrap();
    assert_eq!(links.precedents.len(), 1);
    assert_eq!(links.truncation.omitted, Some(OmittedCount::AtLeast(1)));

    let graph = engine
        .trace(
            &[address("Model", 1, 4)],
            &TraceOptions::default()
                .with_range_member_budget(1)
                .with_max_depth(0),
        )
        .unwrap();
    assert_eq!(graph.nodes.len(), 3);
    assert_eq!(
        graph.nodes[0].links[0].targets[0].disposition,
        LinkDisposition::Elided
    );
    assert_eq!(
        graph.nodes[0].links[0].omitted,
        Some(OmittedCount::Exact(2))
    );

    let node_limited = engine
        .trace(
            &[address("Model", 1, 4)],
            &TraceOptions::default().with_max_nodes(1),
        )
        .unwrap();
    assert_eq!(node_limited.nodes.len(), 1);
    assert!(node_limited.truncation.incomplete);
}

#[test]
fn compressed_range_marks_an_unexpanded_root_ancestor_as_cycle() {
    let mut engine = engine();
    set_formula(&mut engine, 1, 1, "=B1");
    set_formula(&mut engine, 1, 2, "=SUM(A:A)");
    let graph = engine
        .trace(
            &[address("Model", 1, 1)],
            &TraceOptions::default().with_range_member_budget(0),
        )
        .unwrap();
    let b1 = graph
        .nodes
        .iter()
        .find(|node| node.cell.address == address("Model", 1, 2))
        .unwrap();
    assert!(b1.links[0].targets.iter().any(
        |target| target.node == graph.roots[0] && target.disposition == LinkDisposition::Cycle
    ));
}

#[test]
fn empty_cells_have_empty_public_traces_and_deferred_reverse_state_is_explicit() {
    let mut engine = engine();
    engine.add_sheet("Model").unwrap();
    let precedents = engine
        .precedents(&address("Model", 9, 9), &PrecedentOptions::default())
        .unwrap();
    assert!(precedents.precedents.is_empty());
    let trace = engine
        .trace(&[address("Model", 9, 9)], &TraceOptions::default())
        .unwrap();
    assert_eq!(trace.nodes.len(), 1);
    assert!(trace.nodes[0].links.is_empty());

    let mut config = EvalConfig::default();
    config.defer_graph_building = true;
    let mut deferred = Engine::new(TestWorkbook::new(), config);
    deferred.add_sheet("Model").unwrap();
    deferred.stage_formula_text("Model", 1, 2, "=A1".to_string());
    let declared = deferred
        .precedents(&address("Model", 1, 2), &PrecedentOptions::default())
        .unwrap();
    assert_eq!(declared.precedents.len(), 1);
    assert!(matches!(
        deferred.dependents(&address("Model", 1, 1), &DependentsOptions::default()),
        Err(InspectError::DependencyStateUnavailable { .. })
    ));
}

#[test]
fn public_dependents_include_direct_finite_and_infinite_range_readers() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 0;
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_value("Model", 20, 1, LiteralValue::Number(3.0))
        .unwrap();
    set_formula(&mut engine, 1, 2, "=A20");
    set_formula(&mut engine, 2, 2, "=SUM(A10:A30)");
    set_formula(&mut engine, 3, 2, "=SUM(A:A)");

    let report = engine
        .dependents(&address("Model", 20, 1), &DependentsOptions::default())
        .unwrap();
    assert_eq!(
        report
            .dependents
            .iter()
            .map(|dependent| dependent.cell.clone())
            .collect::<Vec<_>>(),
        vec![
            address("Model", 1, 2),
            address("Model", 2, 2),
            address("Model", 3, 2)
        ]
    );
    assert!(
        report
            .dependents
            .iter()
            .all(|dependent| dependent.via == vec![address("Model", 20, 1)])
    );

    let bounded = engine
        .dependents(
            &address("Model", 20, 1),
            &DependentsOptions::default().with_max_work(1),
        )
        .unwrap();
    assert!(bounded.truncation.incomplete);
    assert!(matches!(
        bounded.truncation.omitted,
        Some(OmittedCount::AtLeast(_))
    ));
}

#[test]
fn snapshots_are_honest_for_empty_never_evaluated_and_dirty_cached_formulas() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 5, 5, LiteralValue::Number(1.0))
        .unwrap();
    let empty = engine
        .inspect_cell(&address("Model", 1, 1), &SnapshotOptions::default())
        .unwrap();
    assert_eq!(empty.cell.formula, None);
    assert_eq!(empty.cell.value, None);
    assert_eq!(empty.cell.staleness, Staleness::Current);

    set_formula(&mut engine, 1, 2, "=1+1");
    let never = engine
        .inspect_cell(&address("Model", 1, 2), &SnapshotOptions::default())
        .unwrap();
    assert_eq!(never.cell.value, None);
    assert_eq!(never.cell.staleness, Staleness::NeverEvaluated);

    engine.evaluate_all().unwrap();
    let current = engine
        .inspect_cell(&address("Model", 1, 2), &SnapshotOptions::default())
        .unwrap();
    assert_eq!(current.cell.value, Some(LiteralValue::Number(2.0)));
    assert_eq!(current.cell.staleness, Staleness::Current);

    set_formula(&mut engine, 1, 2, "=3+4");
    let dirty = engine
        .inspect_cell(&address("Model", 1, 2), &SnapshotOptions::default())
        .unwrap();
    assert_eq!(dirty.cell.formula.as_deref(), Some("=3 + 4"));
    assert_eq!(dirty.cell.value, Some(LiteralValue::Number(2.0)));
    assert_eq!(dirty.cell.staleness, Staleness::Dirty);
}

#[test]
fn whole_column_empty_extent_and_revision_checked_paging_are_semantic() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 1, 2, LiteralValue::Number(7.0))
        .unwrap();
    set_formula(&mut engine, 2, 2, "=SUM(A:A)");
    let precedents = engine
        .precedents(&address("Model", 2, 2), &PrecedentOptions::default())
        .unwrap();
    assert!(matches!(
        precedents.precedents[0].reference,
        SemanticReference::Range {
            resolved: None,
            cell_count: 0,
            ..
        }
    ));

    let area = RangeArea::new("model", Some(1), Some(2), Some(2), Some(2)).unwrap();
    let first = engine
        .range_page(
            &area,
            &RangePageOptions::default()
                .with_limit(1)
                .with_include_values(false),
        )
        .unwrap();
    assert_eq!(first.declared.sheet, "Model");
    assert_eq!(first.total, 2);
    assert_eq!(first.next_offset, Some(1));
    assert!(!first.items[0].value_included);

    engine
        .set_cell_value("Model", 3, 2, LiteralValue::Number(9.0))
        .unwrap();
    let mismatch = engine
        .range_page(
            &area,
            &RangePageOptions::default().with_expected_stamp(first.stamp),
        )
        .unwrap_err();
    assert!(matches!(mismatch, InspectError::RevisionMismatch { .. }));
}

#[test]
fn spill_roles_links_readers_and_pages_use_public_entry_points() {
    let mut engine = engine();
    set_formula(&mut engine, 1, 1, "={1,2;3,4}");
    engine.evaluate_all().unwrap();
    set_formula(&mut engine, 1, 4, "=B2");

    let anchor = engine
        .inspect_cell(&address("Model", 1, 1), &SnapshotOptions::default())
        .unwrap();
    assert!(matches!(
        anchor.cell.spill,
        Some(SpillRole::Anchor { ref extent })
            if extent.start_row == 1 && extent.end_row == 2
                && extent.start_col == 1 && extent.end_col == 2
    ));
    let member = engine
        .inspect_cell(&address("Model", 2, 2), &SnapshotOptions::default())
        .unwrap();
    assert_eq!(
        member.cell.spill,
        Some(SpillRole::Member {
            anchor: address("Model", 1, 1)
        })
    );

    let member_trace = engine
        .trace(&[address("Model", 2, 2)], &TraceOptions::default())
        .unwrap();
    assert_eq!(
        member_trace.nodes[0].links[0].kind,
        TraceLinkKind::SpillAnchor
    );
    assert_eq!(
        member_trace.nodes[0].links[0].targets[0].disposition,
        LinkDisposition::Expanded
    );

    let readers = engine
        .dependents(&address("Model", 1, 1), &DependentsOptions::default())
        .unwrap();
    let reader = readers
        .dependents
        .iter()
        .find(|dependent| dependent.cell == address("Model", 1, 4))
        .unwrap();
    assert_eq!(reader.via, vec![address("Model", 2, 2)]);

    let page = engine
        .range_page(
            &RangeArea::new("Model", Some(1), Some(1), Some(2), Some(2)).unwrap(),
            &RangePageOptions::default(),
        )
        .unwrap();
    assert_eq!(page.items.len(), 4);
    assert!(matches!(
        page.items[3].spill,
        Some(SpillRole::Member { .. })
    ));
}

#[test]
fn every_public_inspection_call_is_state_preserving_and_stamped() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    set_formula(&mut engine, 1, 2, "=A1+A1");
    let cell = address("Model", 1, 2);
    let area = RangeArea::new("Model", Some(1), Some(1), Some(1), Some(2)).unwrap();

    let before_stats = engine.baseline_stats();
    let before_revision = engine.inspection_mutation_revision();
    let before_recalc = engine.recalc_epoch;
    let before_sheets = engine.graph.sheet_reg().all_sheets();
    let before_spills = engine.graph.spill_registry_counts();

    let snapshot = engine
        .inspect_cell(&cell, &SnapshotOptions::default())
        .unwrap();
    let precedents = engine
        .precedents(&cell, &PrecedentOptions::default())
        .unwrap();
    let dependents = engine
        .dependents(&address("Model", 1, 1), &DependentsOptions::default())
        .unwrap();
    let trace = engine
        .trace(std::slice::from_ref(&cell), &TraceOptions::default())
        .unwrap();
    let page = engine
        .range_page(&area, &RangePageOptions::default())
        .unwrap();

    let expected_stamp = StateStamp {
        mutation_revision: before_revision,
        recalc_epoch: before_recalc,
    };
    assert_eq!(snapshot.stamp, expected_stamp);
    assert_eq!(precedents.stamp, expected_stamp);
    assert_eq!(dependents.stamp, expected_stamp);
    assert_eq!(trace.stamp, expected_stamp);
    assert_eq!(page.stamp, expected_stamp);
    assert_eq!(engine.baseline_stats(), before_stats);
    assert_eq!(engine.inspection_mutation_revision(), before_revision);
    assert_eq!(engine.recalc_epoch, before_recalc);
    assert_eq!(engine.graph.sheet_reg().all_sheets(), before_sheets);
    assert_eq!(engine.graph.spill_registry_counts(), before_spills);
}

#[test]
fn unsupported_3d_reference_is_an_explicit_semantic_leaf() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Other", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    set_formula(&mut engine, 1, 2, "=Model:Other!A1");
    let report = engine
        .precedents(&address("Model", 1, 2), &PrecedentOptions::default())
        .unwrap();
    assert!(matches!(
        report.precedents[0].reference,
        SemanticReference::Unsupported { .. }
    ));
}

#[test]
fn names_and_structured_tables_retain_symbolic_resolution() {
    let mut engine = engine();
    engine.add_sheet("Model").unwrap();
    engine
        .define_name(
            "TaxRate",
            NamedDefinition::Literal(LiteralValue::Number(0.2)),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .define_name(
            "Twice",
            NamedDefinition::Formula {
                ast: parse("=2*3").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();

    let sheet_id = engine.sheet_id("Model").unwrap();
    let table_range = RangeRef::new(
        CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true)),
        CellRef::new(sheet_id, Coord::from_excel(3, 2, true, true)),
    );
    engine
        .define_table(
            "Sales",
            table_range,
            true,
            vec!["Region".into(), "Amount".into()],
            false,
        )
        .unwrap();
    set_formula(&mut engine, 2, 4, "=TaxRate+Twice+Sales[@Amount]");

    let report = engine
        .precedents(&address("Model", 2, 4), &PrecedentOptions::default())
        .unwrap();
    assert!(matches!(
        &report.precedents[0].reference,
        SemanticReference::Name {
            name,
            resolution: NameResolution::Literal(LiteralValue::Number(value)),
        } if name == "TaxRate" && *value == 0.2
    ));
    assert!(matches!(
        &report.precedents[1].reference,
        SemanticReference::Name {
            name,
            resolution: NameResolution::Formula { formula, .. },
        } if name == "Twice" && formula == "=2 * 3"
    ));
    assert!(matches!(
        &report.precedents[2].reference,
        SemanticReference::Table { name, specifier, resolved }
            if name == "Sales" && specifier.contains("Amount")
                && resolved.start_row == 2 && resolved.end_row == 2
                && resolved.start_col == 2 && resolved.end_col == 2
    ));
}

#[test]
fn dirty_trace_pairs_current_formula_with_cached_value_and_stamp() {
    let mut engine = engine();
    set_formula(&mut engine, 1, 1, "=1");
    engine.evaluate_all().unwrap();
    let old_stamp = engine
        .inspect_cell(&address("Model", 1, 1), &SnapshotOptions::default())
        .unwrap()
        .stamp;
    set_formula(&mut engine, 1, 1, "=2");
    let graph = engine
        .trace(&[address("Model", 1, 1)], &TraceOptions::default())
        .unwrap();
    assert_ne!(graph.stamp, old_stamp);
    assert_eq!(graph.nodes[0].cell.formula.as_deref(), Some("=2"));
    assert_eq!(graph.nodes[0].cell.value, Some(LiteralValue::Number(1.0)));
    assert_eq!(graph.nodes[0].cell.staleness, Staleness::Dirty);
}

#[test]
fn bounded_range_dependent_query_does_not_materialize_a_hundred_thousand_candidates() {
    const FORMULAS: u32 = 100_001;
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 0;
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine.add_sheet("Model").unwrap();
    let ast = parse("=SUM(A:A)").unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    let records = (1..=FORMULAS)
        .map(|row| FormulaIngestRecord::new(row, 2, ast_id, Some(Arc::from("=SUM(A:A)"))))
        .collect();
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Model", records)])
        .unwrap();

    let started = std::time::Instant::now();
    let report = engine
        .dependents(
            &address("Model", 1, 1),
            &DependentsOptions::default()
                .with_max_results(64)
                .with_max_work(64),
        )
        .unwrap();
    let elapsed = started.elapsed();
    assert!(report.truncation.incomplete);
    assert!(matches!(
        report.truncation.omitted,
        Some(OmittedCount::AtLeast(_))
    ));
    assert!(report.dependents.len() <= 64);
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "bounded query took {elapsed:?}"
    );
}

#[test]
fn multi_root_trace_reuses_overlapping_nodes_deterministically() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    set_formula(&mut engine, 1, 2, "=A1");
    set_formula(&mut engine, 1, 3, "=A1");
    let options = TraceOptions::default();
    let roots = [address("Model", 1, 2), address("Model", 1, 3)];
    let first = engine.trace(&roots, &options).unwrap();
    let second = engine.trace(&roots, &options).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.roots.len(), 2);
    assert_eq!(
        first
            .nodes
            .iter()
            .filter(|node| node.cell.address == address("Model", 1, 1))
            .count(),
        1
    );
}
