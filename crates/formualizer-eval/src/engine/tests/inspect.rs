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
    assert_eq!(
        node_limited.truncation.omitted,
        Some(OmittedCount::AtLeast(4))
    );
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
    assert!(matches!(
        engine.inspect_cell(&address("Missing", 1, 1), &SnapshotOptions::default()),
        Err(InspectError::SheetNotFound { .. })
    ));

    let config = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
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
    let config = EvalConfig {
        range_expansion_limit: 0,
        ..EvalConfig::default()
    };
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
            .all(|dependent| dependent.via.is_empty())
    );
    let infinite_only = engine
        .dependents(&address("Model", 40, 1), &DependentsOptions::default())
        .unwrap();
    assert_eq!(
        infinite_only
            .dependents
            .iter()
            .map(|dependent| dependent.cell.clone())
            .collect::<Vec<_>>(),
        vec![address("Model", 3, 2)]
    );

    let bounded = engine
        .dependents(
            &address("Model", 20, 1),
            &DependentsOptions::default().with_max_work(1),
        )
        .unwrap();
    assert!(bounded.truncation.incomplete);
    assert_eq!(bounded.truncation.omitted, None);
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
    let empty_column_page = engine
        .range_page(
            &RangeArea::new("Model", None, Some(1), None, Some(1)).unwrap(),
            &RangePageOptions::default(),
        )
        .unwrap();
    assert_eq!(empty_column_page.total, 0);
    assert_eq!(empty_column_page.resolved, None);
    assert!(empty_column_page.items.is_empty());

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
    let dependent_trace = engine
        .trace(
            &[address("Model", 1, 1)],
            &TraceOptions::default().with_direction(TraceDirection::Dependents),
        )
        .unwrap();
    assert!(
        dependent_trace.nodes[0]
            .links
            .iter()
            .any(|link| link.kind == TraceLinkKind::SpillReader)
    );

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

    let state = |engine: &Engine<TestWorkbook>| {
        (
            engine.baseline_stats(),
            engine.inspection_mutation_revision(),
            engine.recalc_epoch,
            engine.graph.sheet_reg().all_sheets(),
            engine.graph.spill_registry_counts(),
        )
    };
    let before = state(&engine);

    let snapshot = engine
        .inspect_cell(&cell, &SnapshotOptions::default())
        .unwrap();
    assert_eq!(state(&engine), before);
    let precedents = engine
        .precedents(&cell, &PrecedentOptions::default())
        .unwrap();
    assert_eq!(state(&engine), before);
    let dependents = engine
        .dependents(&address("Model", 1, 1), &DependentsOptions::default())
        .unwrap();
    assert_eq!(state(&engine), before);
    let trace = engine
        .trace(std::slice::from_ref(&cell), &TraceOptions::default())
        .unwrap();
    assert_eq!(state(&engine), before);
    let page = engine
        .range_page(&area, &RangePageOptions::default())
        .unwrap();
    assert_eq!(state(&engine), before);

    let expected_stamp = StateStamp {
        mutation_revision: before.1,
        recalc_epoch: before.2,
    };
    assert_eq!(snapshot.stamp, expected_stamp);
    assert_eq!(precedents.stamp, expected_stamp);
    assert_eq!(dependents.stamp, expected_stamp);
    assert_eq!(trace.stamp, expected_stamp);
    assert_eq!(page.stamp, expected_stamp);
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
    assert_eq!(graph.stamp.recalc_epoch, engine.recalc_epoch);
    assert_eq!(
        graph.stamp.mutation_revision,
        engine.inspection_mutation_revision()
    );
    assert_eq!(graph.nodes[0].cell.formula.as_deref(), Some("=2"));
    assert_eq!(graph.nodes[0].cell.value, Some(LiteralValue::Number(1.0)));
    assert_eq!(graph.nodes[0].cell.staleness, Staleness::Dirty);
}

#[test]
fn bounded_range_dependent_query_does_not_materialize_a_hundred_thousand_candidates() {
    const FORMULAS: u32 = 100_001;
    let config = EvalConfig {
        range_expansion_limit: 0,
        ..EvalConfig::default()
    };
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
    assert_eq!(report.truncation.omitted, None);
    assert!(report.dependents.len() <= 64);
    eprintln!("100001-range-dependent bounded query elapsed: {elapsed:?}");
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
    #[cfg(feature = "serde")]
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
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

#[test]
fn cycle_dispositions_are_reachability_based_for_entangled_graphs() {
    let make = || {
        Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.0)),
        )
    };
    let disposition = |graph: &TraceGraph, from: CellAddress, to: CellAddress| {
        let target = graph
            .nodes
            .iter()
            .find(|node| node.cell.address == to)
            .unwrap()
            .id;
        graph
            .nodes
            .iter()
            .find(|node| node.cell.address == from)
            .unwrap()
            .links
            .iter()
            .flat_map(|link| &link.targets)
            .find(|link_target| link_target.node == target)
            .unwrap()
            .disposition
    };

    let mut three = make();
    set_formula(&mut three, 1, 1, "=B1+C1");
    set_formula(&mut three, 1, 2, "=C1");
    set_formula(&mut three, 1, 3, "=B1");
    let graph = three
        .trace(&[address("Model", 1, 1)], &TraceOptions::default())
        .unwrap();
    assert_eq!(
        disposition(&graph, address("Model", 1, 2), address("Model", 1, 3)),
        LinkDisposition::Cycle
    );
    assert_eq!(
        disposition(&graph, address("Model", 1, 3), address("Model", 1, 2)),
        LinkDisposition::Cycle
    );

    let mut diamond = make();
    set_formula(&mut diamond, 1, 1, "=B1+C1");
    set_formula(&mut diamond, 1, 2, "=D1");
    set_formula(&mut diamond, 1, 3, "=D1");
    set_formula(&mut diamond, 1, 4, "=C1");
    let graph = diamond
        .trace(&[address("Model", 1, 1)], &TraceOptions::default())
        .unwrap();
    assert_eq!(
        disposition(&graph, address("Model", 1, 3), address("Model", 1, 4)),
        LinkDisposition::Cycle
    );
    assert_eq!(
        disposition(&graph, address("Model", 1, 4), address("Model", 1, 3)),
        LinkDisposition::Cycle
    );
}

#[test]
fn cycle_dispositions_match_independent_reachability_oracle() {
    fn lcg(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        *state
    }

    let mut sound_violations = 0usize;
    let mut completeness_violations = 0usize;
    for seed in 0u64..80 {
        let mut random = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(999);
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_cycle(CycleConfig::iterate(1, 0.0)),
        );
        engine.add_sheet("Model").unwrap();
        let count = 5u32;
        let mut adjacency = vec![Vec::new(); (count + 1) as usize];
        for row in 1..=count {
            let target_count = 1 + (lcg(&mut random) % 2) as u32;
            let mut targets = Vec::new();
            for _ in 0..target_count {
                let target = 1 + (lcg(&mut random) % u64::from(count)) as u32;
                if !targets.contains(&target) {
                    targets.push(target);
                }
            }
            adjacency[row as usize] = targets.clone();
            set_formula(
                &mut engine,
                row,
                1,
                &format!(
                    "={}",
                    targets
                        .iter()
                        .map(|target| format!("A{target}"))
                        .collect::<Vec<_>>()
                        .join("+")
                ),
            );
        }
        let graph = engine
            .trace(
                &[address("Model", 1, 1)],
                &TraceOptions::default()
                    .with_max_depth(50)
                    .with_max_nodes(64),
            )
            .unwrap();
        let reaches = |from: u32, to: u32| {
            let mut seen = vec![false; (count + 1) as usize];
            let mut stack = vec![from];
            while let Some(node) = stack.pop() {
                for &next in &adjacency[node as usize] {
                    if next == to {
                        return true;
                    }
                    if !seen[next as usize] {
                        seen[next as usize] = true;
                        stack.push(next);
                    }
                }
            }
            false
        };
        for node in &graph.nodes {
            let source = node.cell.address.row;
            for target in node.links.iter().flat_map(|link| link.targets.iter()) {
                let target_row = graph.nodes[target.node.0 as usize].cell.address.row;
                let oracle_cycle = source == target_row || reaches(target_row, source);
                let reported_cycle = target.disposition == LinkDisposition::Cycle;
                sound_violations += usize::from(reported_cycle && !oracle_cycle);
                completeness_violations += usize::from(oracle_cycle && !reported_cycle);
            }
        }
    }
    assert_eq!(sound_violations, 0);
    assert_eq!(completeness_violations, 0);
}

#[test]
fn roots_are_admitted_first_and_preserve_request_correspondence() {
    let mut engine = engine();
    engine.add_sheet("Model").unwrap();
    let distinct = [address("Model", 1, 1), address("Model", 1, 2)];
    assert!(matches!(
        engine.trace(&distinct, &TraceOptions::default().with_max_nodes(1)),
        Err(InspectError::InvalidOptions { .. })
    ));

    let duplicate = [address("Model", 1, 1), address("model", 1, 1)];
    let graph = engine
        .trace(&duplicate, &TraceOptions::default().with_max_nodes(1))
        .unwrap();
    assert_eq!(graph.roots.len(), duplicate.len());
    assert_eq!(graph.roots, vec![TraceNodeId(0), TraceNodeId(0)]);
    assert_eq!(graph.nodes.len(), 1);
}

#[test]
fn zero_budget_matrix_obeys_anchor_minimum_convention() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Model", 2, 1, LiteralValue::Number(2.0))
        .unwrap();
    set_formula(&mut engine, 1, 2, "=SUM(A1:A2)+A1");
    let root = address("Model", 1, 2);

    assert!(matches!(
        engine.trace(
            std::slice::from_ref(&root),
            &TraceOptions::default().with_max_nodes(0),
        ),
        Err(InspectError::InvalidOptions { .. })
    ));
    assert!(
        engine
            .trace(
                std::slice::from_ref(&root),
                &TraceOptions::default().with_max_depth(0),
            )
            .unwrap()
            .truncation
            .incomplete
    );
    assert!(
        engine
            .trace(
                std::slice::from_ref(&root),
                &TraceOptions::default().with_max_links(0),
            )
            .unwrap()
            .truncation
            .incomplete
    );
    assert!(
        engine
            .trace(
                std::slice::from_ref(&root),
                &TraceOptions::default().with_max_work(0),
            )
            .unwrap()
            .truncation
            .incomplete
    );
    assert!(
        engine
            .trace(
                std::slice::from_ref(&root),
                &TraceOptions::default().with_range_member_budget(0),
            )
            .unwrap()
            .truncation
            .incomplete
    );
    assert!(
        engine
            .precedents(&root, &PrecedentOptions::default().with_max_links(0))
            .unwrap()
            .truncation
            .incomplete
    );
    assert!(
        engine
            .precedents(&root, &PrecedentOptions::default().with_max_work(0))
            .unwrap()
            .truncation
            .incomplete
    );
    let results_zero = engine
        .dependents(
            &address("Model", 1, 1),
            &DependentsOptions::default().with_max_results(0),
        )
        .unwrap();
    assert!(results_zero.truncation.incomplete);
    assert_eq!(
        results_zero.truncation.omitted,
        Some(OmittedCount::AtLeast(1))
    );
    let work_zero = engine
        .dependents(
            &address("Model", 1, 1),
            &DependentsOptions::default().with_max_work(0),
        )
        .unwrap();
    assert!(work_zero.truncation.incomplete);
    assert_eq!(work_zero.truncation.omitted, None);
    let area = RangeArea::new("Model", Some(1), Some(1), Some(2), Some(1)).unwrap();
    assert!(matches!(
        engine.range_page(&area, &RangePageOptions::default().with_limit(0)),
        Err(InspectError::InvalidOptions { .. })
    ));
}

#[test]
fn range_pages_pin_row_major_order_and_exact_termination() {
    let mut engine = engine();
    for row in 1..=2 {
        for column in 1..=3 {
            engine
                .set_cell_value(
                    "Model",
                    row,
                    column,
                    LiteralValue::Number(f64::from(row * 10 + column)),
                )
                .unwrap();
        }
    }
    let area = RangeArea::new("Model", Some(1), Some(1), Some(2), Some(3)).unwrap();
    let page = engine
        .range_page(&area, &RangePageOptions::default().with_limit(6))
        .unwrap();
    assert_eq!(
        page.items
            .iter()
            .map(|item| (item.address.row, item.address.column))
            .collect::<Vec<_>>(),
        vec![(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)]
    );
    assert_eq!(page.next_offset, None);
}

#[test]
fn trace_max_links_is_global_across_nodes() {
    let mut engine = engine();
    set_formula(&mut engine, 1, 1, "=B1+C1");
    set_formula(&mut engine, 1, 2, "=D1");
    set_formula(&mut engine, 1, 3, "=E1");
    let graph = engine
        .trace(
            &[address("Model", 1, 1)],
            &TraceOptions::default().with_max_links(3),
        )
        .unwrap();
    let total_links: usize = graph.nodes.iter().map(|node| node.links.len()).sum();
    assert_eq!(total_links, 3);
    assert!(graph.truncation.incomplete);
}

#[test]
fn spill_dependent_via_is_sorted_and_ordinary_via_is_empty() {
    let mut engine = engine();
    set_formula(&mut engine, 1, 1, "={1,2;3,4}");
    engine.evaluate_all().unwrap();
    set_formula(&mut engine, 1, 4, "=B2+A2");
    let spill = engine
        .dependents(&address("Model", 1, 1), &DependentsOptions::default())
        .unwrap();
    let reader = spill
        .dependents
        .iter()
        .find(|dependent| dependent.cell == address("Model", 1, 4))
        .unwrap();
    assert_eq!(
        reader.via,
        vec![address("Model", 2, 1), address("Model", 2, 2)]
    );
    let ordinary = engine
        .dependents(&address("Model", 2, 1), &DependentsOptions::default())
        .unwrap();
    assert!(
        ordinary
            .dependents
            .iter()
            .all(|dependent| dependent.via.is_empty())
    );
}

#[test]
fn range_stripe_category_dedup_preserves_exact_work_boundary() {
    let config = EvalConfig {
        range_expansion_limit: 0,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_value("Model", 5, 1, LiteralValue::Number(1.0))
        .unwrap();
    set_formula(&mut engine, 20, 5, "=SUM(A:A)+SUM(1:10)");
    let report = engine
        .dependents(
            &address("Model", 5, 1),
            &DependentsOptions::default()
                .with_max_results(u32::MAX)
                .with_max_work(4),
        )
        .unwrap();
    assert!(!report.truncation.incomplete);
    assert_eq!(report.dependents.len(), 1);
}

#[test]
fn bounded_dependents_cover_block_stripes_and_cross_sheet_ranges() {
    let config = EvalConfig {
        range_expansion_limit: 0,
        enable_block_stripes: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine.add_sheet("Model").unwrap();
    engine.add_sheet("Other").unwrap();
    for row in 1..=12 {
        engine
            .set_cell_value("Model", row, 1, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    engine
        .set_cell_formula("Model", 20, 5, parse("=SUM(A5:C9)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Other", 30, 5, parse("=SUM(Model!A5:C9)").unwrap())
        .unwrap();
    let report = engine
        .dependents(
            &address("Model", 7, 2),
            &DependentsOptions::default()
                .with_max_results(u32::MAX)
                .with_max_work(u64::MAX),
        )
        .unwrap();
    assert_eq!(
        report
            .dependents
            .iter()
            .map(|dependent| dependent.cell.clone())
            .collect::<Vec<_>>(),
        vec![address("Model", 20, 5), address("Other", 30, 5)]
    );
}

#[test]
fn dirty_spill_roles_remain_last_evaluation_facts() {
    let mut engine = engine();
    for row in 1..=3 {
        engine
            .set_cell_value("Model", row, 1, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    set_formula(&mut engine, 1, 3, "=A1:A3");
    engine.evaluate_all().unwrap();
    set_formula(&mut engine, 1, 3, "=A1:A1");
    let anchor = engine
        .inspect_cell(&address("Model", 1, 3), &SnapshotOptions::default())
        .unwrap();
    let member2 = engine
        .inspect_cell(&address("Model", 2, 3), &SnapshotOptions::default())
        .unwrap();
    let member3 = engine
        .inspect_cell(&address("Model", 3, 3), &SnapshotOptions::default())
        .unwrap();
    assert_eq!(anchor.cell.staleness, Staleness::Dirty);
    assert!(matches!(
        anchor.cell.spill,
        Some(SpillRole::Anchor { ref extent }) if extent.end_row == 3
    ));
    for member in [member2, member3] {
        assert_eq!(member.cell.staleness, Staleness::Current);
        assert!(matches!(member.cell.spill, Some(SpillRole::Member { .. })));
    }
}

#[test]
fn fully_open_semantic_range_uses_the_whole_sheet_extent() {
    let mut engine = engine();
    engine
        .set_cell_value("Model", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Model", 100, 3, LiteralValue::Number(2.0))
        .unwrap();
    let area = RangeArea::new("Model", None, None, None, None).unwrap();
    let page = engine
        .range_page(&area, &RangePageOptions::default().with_limit(1))
        .unwrap();
    let resolved = page.resolved.unwrap();
    assert_eq!(
        (
            resolved.start_row,
            resolved.start_col,
            resolved.end_row,
            resolved.end_col
        ),
        (1, 1, 100, 3)
    );
    assert_eq!(page.total, 300);
    assert_eq!(page.items[0].address, address("Model", 1, 1));
}

#[test]
fn inspection_cache_warming_does_not_change_evaluation_results() {
    let build = |inspect_first: bool| {
        let mut engine = engine();
        engine
            .set_cell_value("Model", 1, 1, LiteralValue::Number(1.0))
            .unwrap();
        engine
            .set_cell_value("Model", 10, 1, LiteralValue::Number(2.0))
            .unwrap();
        set_formula(&mut engine, 1, 5, "=SUM(A:A)");
        if inspect_first {
            let area = RangeArea::new("Model", None, Some(1), None, Some(1)).unwrap();
            engine
                .range_page(&area, &RangePageOptions::default())
                .unwrap();
            engine
                .precedents(&address("Model", 1, 5), &PrecedentOptions::default())
                .unwrap();
            engine
                .trace(&[address("Model", 1, 5)], &TraceOptions::default())
                .unwrap();
        }
        engine
            .set_cell_value("Model", 500, 1, LiteralValue::Number(4.0))
            .unwrap();
        if inspect_first {
            engine
                .dependents(&address("Model", 500, 1), &DependentsOptions::default())
                .unwrap();
        }
        engine.evaluate_all().unwrap();
        engine.get_cell_value("Model", 1, 5)
    };
    assert_eq!(build(false), build(true));
}

#[test]
fn staged_formula_volatility_uses_the_function_registry() {
    let config = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine.add_sheet("Model").unwrap();
    engine.stage_formula_text("Model", 1, 1, "=NOW()".to_string());
    let report = engine
        .inspect_cell(&address("Model", 1, 1), &SnapshotOptions::default())
        .unwrap();
    assert!(report.cell.volatile);
}

#[cfg(feature = "serde")]
#[test]
fn stamps_and_options_round_trip_with_serde() {
    let stamp = StateStamp {
        mutation_revision: 7,
        recalc_epoch: 11,
    };
    assert_eq!(
        serde_json::from_str::<StateStamp>(&serde_json::to_string(&stamp).unwrap()).unwrap(),
        stamp
    );
    let options = SnapshotOptions::default().with_include_values(false);
    assert_eq!(
        serde_json::from_str::<SnapshotOptions>(&serde_json::to_string(&options).unwrap()).unwrap(),
        options
    );
}
