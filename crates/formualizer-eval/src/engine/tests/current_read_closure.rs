use super::common::arrow_eval_config;
use crate::{
    engine::affected_closure::{
        AffectedRoot, ClosureError, ClosureLimits, ClosureResource, ReadRelationCoverage,
        RegionPiece, UnknownReason,
    },
    engine::{Engine, EvalConfig},
    test_workbook::TestWorkbook,
};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn isolated_case(name: &str) -> bool {
    #[cfg(target_arch = "wasm32")]
    {
        let _ = name;
        false
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let full = format!("engine::tests::current_read_closure::{name}");
        if std::env::var("FZ_CURRENT_READ_CLOSURE_CHILD")
            .ok()
            .as_deref()
            == Some(full.as_str())
        {
            return false;
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &full, "--nocapture"])
            .env("FZ_CURRENT_READ_CLOSURE_CHILD", &full)
            .status()
            .unwrap();
        assert!(status.success(), "isolated closure fixture failed: {full}");
        true
    }
}

fn engine() -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    )
}
fn limits() -> ClosureLimits {
    ClosureLimits {
        max_work: 100_000,
        max_output_items: 1000,
        max_scratch_bytes: 2 * 1024 * 1024,
    }
}
fn contains(regions: &[RegionPiece], cell: crate::CellRef) -> bool {
    regions
        .iter()
        .any(|r| r.sheet == cell.sheet_id && r.rect.contains(cell.coord.row(), cell.coord.col()))
}
#[test]
fn current_read_closure_static_chain_and_cycles_are_sparse() {
    if isolated_case("current_read_closure_static_chain_and_cycles_are_sparse") {
        return;
    }
    let mut e = engine();
    for (row, column, formula) in [
        (1, 2, "=A1+1"),
        (1, 3, "=B1+1"),
        (2, 2, "=C2"),
        (2, 3, "=B2"),
    ] {
        e.set_cell_formula("Sheet1", row, column, parse(formula).unwrap())
            .unwrap();
    }
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let a = e.graph.make_cell_ref("Sheet1", 1, 1);
    let root_cycle = e.graph.make_cell_ref("Sheet1", 2, 2);
    let result = e
        .current_read_affected_closure(
            &[
                AffectedRoot::Cell(a),
                AffectedRoot::Cell(a),
                AffectedRoot::Cell(root_cycle),
            ],
            limits(),
        )
        .unwrap();
    for (row, column) in [(1, 1), (1, 2), (1, 3), (2, 2), (2, 3)] {
        assert!(contains(
            &result.witnessed,
            e.graph.make_cell_ref("Sheet1", row, column)
        ));
    }
    assert!(!contains(
        &result.witnessed,
        e.graph.make_cell_ref("Sheet1", 3, 1)
    ));
    assert!(result.belongs_to(&e));
}
#[test]
fn current_read_closure_uses_actual_dynamic_edges_to_a_fixed_point() {
    if isolated_case("current_read_closure_uses_actual_dynamic_edges_to_a_fixed_point") {
        return;
    }
    let mut e = engine();
    for (row, column, formula) in [
        (1, 2, "=INDIRECT(\"A1\")"),
        (1, 3, "=INDIRECT(\"B1\")"),
        (1, 4, "=C1+1"),
        (5, 2, "=INDIRECT(\"A5\")"),
        (5, 3, "=B5+1"),
    ] {
        e.set_cell_formula("Sheet1", row, column, parse(formula).unwrap())
            .unwrap();
    }
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.set_cell_value("Sheet1", 5, 1, LiteralValue::Number(50.0))
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let result = e
        .current_read_affected_closure(
            &[AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 1, 1))],
            limits(),
        )
        .unwrap();
    for column in 1..=4 {
        assert!(
            contains(
                &result.witnessed,
                e.graph.make_cell_ref("Sheet1", 1, column)
            ),
            "missing dynamic/static chain column{column}: {result:?}"
        );
    }
    for column in 1..=3 {
        let cell = e.graph.make_cell_ref("Sheet1", 5, column);
        assert!(!contains(&result.witnessed, cell));
        assert!(
            !contains(&result.uncertain_downstream, cell),
            "independent recorded island must be preserved"
        );
    }
}
#[test]
fn current_read_closure_uncommitted_dynamic_reader_has_explicit_uncertainty_cone() {
    if isolated_case(
        "current_read_closure_uncommitted_dynamic_reader_has_explicit_uncertainty_cone",
    ) {
        return;
    }
    let mut e = engine();
    for (column, formula) in [(2, "=INDIRECT(\"A1\")"), (3, "=B1+1")] {
        e.set_cell_formula("Sheet1", 1, column, parse(formula).unwrap())
            .unwrap();
    }
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let reader = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 2))
        .unwrap();
    e.graph
        .update_vertex_value(reader, LiteralValue::Number(999.0));
    let result = e
        .current_read_affected_closure(
            &[AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 9, 1))],
            limits(),
        )
        .unwrap();
    assert!(
        result
            .unknowns
            .iter()
            .any(|fact| fact.vertex == Some(reader)
                && fact.reason == UnknownReason::UncommittedObservation)
    );
    assert!(contains(
        &result.uncertain_downstream,
        e.graph.make_cell_ref("Sheet1", 1, 2)
    ));
    assert!(contains(
        &result.uncertain_downstream,
        e.graph.make_cell_ref("Sheet1", 1, 3)
    ));
}

fn signature(e: &Engine<TestWorkbook>) -> (u64, u64, u64, u64, u64) {
    let host = e.graph.authority_host();
    (
        e.inspection_mutation_revision(),
        e.recalc_epoch,
        host.revision(),
        host.rev_dyn(),
        host.observed_value_revision(),
    )
}

#[test]
fn current_read_closure_empty_absent_and_dirty_observations_are_distinct() {
    if isolated_case("current_read_closure_empty_absent_and_dirty_observations_are_distinct") {
        return;
    }
    let mut e = engine();
    for (row, formula) in [
        (1, "=IF(FALSE,INDIRECT(\"A1\"),9)"),
        (2, "=INDIRECT(\"A2\")"),
        (3, "=INDIRECT(\"A3\")"),
    ] {
        e.set_cell_formula("Sheet1", row, 2, parse(formula).unwrap())
            .unwrap();
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
    e.evaluate_all_for_snapshot(None).unwrap();
    let ids: Vec<_> = (1..=3)
        .map(|r| {
            e.graph
                .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", r, 2))
                .unwrap()
        })
        .collect();
    assert_eq!(
        e.graph.authority_host().committed_observed(ids[0]),
        Some([].as_slice())
    );
    e.graph.authority_host_mut().forget_observed(ids[1]);
    e.graph.set_dirty(ids[2], true);
    let result = e
        .current_read_affected_closure(
            &[AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 9, 1))],
            limits(),
        )
        .unwrap();
    assert!(
        !result
            .unknowns
            .iter()
            .any(|fact| fact.vertex == Some(ids[0]))
    );
    assert!(result.unknowns.iter().any(
        |fact| fact.vertex == Some(ids[1]) && fact.reason == UnknownReason::MissingObservation
    ));
    assert!(
        result
            .unknowns
            .iter()
            .any(|fact| fact.vertex == Some(ids[2]) && fact.reason == UnknownReason::DirtyReader)
    );
}

#[test]
fn current_read_closure_named_frontier_discloses_unrepresented_runtime_symbols() {
    if isolated_case("current_read_closure_named_frontier_discloses_unrepresented_runtime_symbols")
    {
        return;
    }
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut e = engine();
    e.define_name(
        "Locator",
        NamedDefinition::Literal(LiteralValue::Text("A1".into())),
        NameScope::Workbook,
    )
    .unwrap();
    e.set_cell_formula(
        "Sheet1",
        1,
        2,
        parse("=IF(FALSE,INDIRECT(\"A1\"),9)").unwrap(),
    )
    .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let named = e.graph.named_ranges_iter().next().unwrap().1.vertex;
    let reader = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 2))
        .unwrap();
    let result = e
        .current_read_affected_closure(&[AffectedRoot::Symbol(named)], limits())
        .unwrap();
    assert!(result.semantic_reads_unattested);
    assert!(
        result
            .unknowns
            .iter()
            .any(|fact| fact.vertex == Some(reader)
                && fact.reason == UnknownReason::RuntimeSymbolsUnrepresented)
    );
    assert!(contains(
        &result.uncertain_downstream,
        e.graph.make_cell_ref("Sheet1", 1, 2)
    ));
}

#[test]
fn current_read_closure_dynamic_name_identity_is_not_a_grid_vertex_cast() {
    if isolated_case("current_read_closure_dynamic_name_identity_is_not_a_grid_vertex_cast") {
        return;
    }
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut e = engine();
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.define_name(
        "Tracked",
        NamedDefinition::Formula {
            ast: parse("=INDIRECT(\"A1\")").unwrap(),
            dependencies: vec![],
            range_deps: vec![],
        },
        NameScope::Workbook,
    )
    .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=Tracked").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 3, parse("=B1+1").unwrap())
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let named = e.graph.named_ranges_iter().next().unwrap().1.vertex;
    let coordinate = e.graph.authority_cell_of_vertex(named).unwrap();
    assert_ne!(
        named.0, coordinate.1,
        "fixture must use distinct executor and slot identities"
    );
    let result = e
        .current_read_affected_closure(
            &[AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 1, 1))],
            limits(),
        )
        .unwrap();
    assert!(
        result
            .witnessed
            .iter()
            .any(|r| r.sheet == coordinate.0 && r.rect.contains(coordinate.1, coordinate.2))
    );
    assert!(contains(
        &result.witnessed,
        e.graph.make_cell_ref("Sheet1", 1, 2)
    ));
    assert!(contains(
        &result.witnessed,
        e.graph.make_cell_ref("Sheet1", 1, 3)
    ));
}

#[test]
fn current_read_closure_spill_extent_reaches_children_and_preserves_another_spill() {
    if isolated_case(
        "current_read_closure_spill_extent_reaches_children_and_preserves_another_spill",
    ) {
        return;
    }
    let mut e = engine();
    for (row, column, value) in [(1, 1, 10.0), (2, 1, 20.0), (5, 6, 50.0), (6, 6, 60.0)] {
        e.set_cell_value("Sheet1", row, column, LiteralValue::Number(value))
            .unwrap();
    }
    for (row, column, formula) in [
        (1, 2, "=INDIRECT(\"A1:A2\")"),
        (2, 3, "=B2+1"),
        (5, 7, "=INDIRECT(\"F5:F6\")"),
        (6, 8, "=G6+1"),
    ] {
        e.set_cell_formula("Sheet1", row, column, parse(formula).unwrap())
            .unwrap();
    }
    e.evaluate_all_for_snapshot(None).unwrap();
    let anchor = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 2))
        .unwrap();
    for root in [
        AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 1, 1)),
        AffectedRoot::CommittedSpill(anchor),
    ] {
        let result = e.current_read_affected_closure(&[root], limits()).unwrap();
        assert!(contains(
            &result.witnessed,
            e.graph.make_cell_ref("Sheet1", 2, 2)
        ));
        assert!(contains(
            &result.witnessed,
            e.graph.make_cell_ref("Sheet1", 2, 3)
        ));
        assert!(!contains(
            &result.witnessed,
            e.graph.make_cell_ref("Sheet1", 5, 7)
        ));
        assert!(!contains(
            &result.uncertain_downstream,
            e.graph.make_cell_ref("Sheet1", 6, 8)
        ));
        assert!(!result.unlocalized_spill_geometry);
    }
    e.graph.set_dirty(anchor, true);
    let result = e
        .current_read_affected_closure(&[AffectedRoot::CommittedSpill(anchor)], limits())
        .unwrap();
    assert!(result.unlocalized_spill_geometry);
    assert!(
        result
            .unknowns
            .iter()
            .any(|fact| fact.vertex == Some(anchor)
                && fact.reason == UnknownReason::UnavailableSpillGeometry)
    );
}

#[test]
fn current_read_closure_limits_and_success_leave_engine_state_unchanged() {
    if isolated_case("current_read_closure_limits_and_success_leave_engine_state_unchanged") {
        return;
    }
    let mut e = engine();
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=INDIRECT(\"A1\")").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 3, parse("=B1+1").unwrap())
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let before = signature(&e);
    let reader = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 2))
        .unwrap();
    let observed = e
        .graph
        .authority_host()
        .committed_observed(reader)
        .unwrap()
        .to_vec();
    let roots = [AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 1, 1))];
    let usage = {
        let report = e.current_read_affected_closure(&roots, limits()).unwrap();
        assert_eq!(
            report.usage.output_items,
            report.witnessed.len() + report.uncertain_downstream.len() + report.unknowns.len()
        );
        report.usage
    };
    for resource in [
        ClosureResource::Work,
        ClosureResource::Output,
        ClosureResource::Scratch,
    ] {
        let mut cap = limits();
        match resource {
            ClosureResource::Work => cap.max_work = usage.work - 1,
            ClosureResource::Output => cap.max_output_items = usage.output_items - 1,
            ClosureResource::Scratch => {
                cap.max_scratch_bytes = usage.requested_capacity_peak_bytes - 1
            }
        }
        assert!(
            matches!(e.current_read_affected_closure(&roots,cap),Err(ClosureError::Limit{resource:found,..}) if found==resource)
        );
        assert_eq!(signature(&e), before);
        assert_eq!(
            e.graph.authority_host().committed_observed(reader),
            Some(observed.as_slice())
        );
        match resource {
            ClosureResource::Work => cap.max_work = usage.work,
            ClosureResource::Output => cap.max_output_items = usage.output_items,
            ClosureResource::Scratch => cap.max_scratch_bytes = usage.requested_capacity_peak_bytes,
        }
        e.current_read_affected_closure(&roots, cap).unwrap();
        assert_eq!(signature(&e), before);
    }
}

#[test]
fn current_read_closure_empty_roots_and_whole_grid_stay_geometric() {
    if isolated_case("current_read_closure_empty_roots_and_whole_grid_stay_geometric") {
        return;
    }
    use crate::engine::authority::geom::Rect;
    let mut e = engine();
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let empty = e
        .current_read_affected_closure(
            &[],
            ClosureLimits {
                max_work: 0,
                max_output_items: 0,
                max_scratch_bytes: 0,
            },
        )
        .unwrap();
    assert_eq!(empty.coverage, ReadRelationCoverage::NotQueried);
    assert_eq!(empty.usage.work, 0);
    let sheet = e.graph.make_cell_ref("Sheet1", 1, 1).sheet_id;
    let result = e
        .current_read_affected_closure(
            &[AffectedRoot::Region(RegionPiece {
                sheet,
                rect: Rect::new(0, 0, 1_048_575, 16_383),
            })],
            limits(),
        )
        .unwrap();
    assert_eq!(result.witnessed.len(), 1);
    assert_eq!(result.witnessed[0].rect.area(), 17_179_869_184);
    assert!(result.usage.work < 100);
}

#[test]
fn current_read_closure_rejects_staged_busy_and_invalid_roots_without_sync() {
    if isolated_case("current_read_closure_rejects_staged_busy_and_invalid_roots_without_sync") {
        return;
    }
    use crate::engine::authority::geom::Rect;
    let mut e = engine();
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.evaluate_all_for_snapshot(None).unwrap();
    let root = AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 1, 1));
    e.begin_deferred_dirty();
    let before = signature(&e);
    assert!(matches!(
        e.current_read_affected_closure(&[root], limits()),
        Err(ClosureError::Busy)
    ));
    assert_eq!(signature(&e), before);
    e.end_deferred_dirty();
    for rect in [
        Rect {
            r0: 3,
            c0: 0,
            r1: 2,
            c1: 0,
        },
        Rect {
            r0: 0,
            c0: 0,
            r1: 1_048_576,
            c1: 0,
        },
    ] {
        let before = signature(&e);
        assert!(matches!(
            e.current_read_affected_closure(
                &[AffectedRoot::Region(RegionPiece { sheet: 0, rect })],
                limits()
            ),
            Err(ClosureError::InvalidRoot { .. })
        ));
        assert_eq!(signature(&e), before);
    }
    // Ordinary edits synchronize their relation incrementally; dirty static
    // values do not make that maintained relation stale.
    e.set_cell_formula("Sheet1", 2, 2, parse("=A1").unwrap())
        .unwrap();
    e.current_read_affected_closure(&[root], limits()).unwrap();
    e.stage_formula_text("Sheet1", 3, 2, "=A1".into());
    let before = signature(&e);
    assert!(matches!(
        e.current_read_affected_closure(&[root], limits()),
        Err(ClosureError::Busy)
    ));
    assert_eq!(signature(&e), before);
    let unbuilt = engine();
    let before = signature(&unbuilt);
    assert!(matches!(
        unbuilt.current_read_affected_closure(&[root], limits()),
        Err(ClosureError::Authority(_))
    ));
    assert_eq!(signature(&unbuilt), before);
}

#[test]
fn current_read_closure_native_next_request_dirty_is_not_snapshot_current() {
    if isolated_case("current_read_closure_native_next_request_dirty_is_not_snapshot_current") {
        return;
    }
    let mut e = engine();
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=INDIRECT(\"A1\")").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let reader = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 2))
        .unwrap();
    assert!(e.graph.is_dirty(reader));
    assert!(
        e.graph
            .authority_host()
            .committed_observed(reader)
            .is_some()
    );
    let report = e
        .current_read_affected_closure(
            &[AffectedRoot::Cell(e.graph.make_cell_ref("Sheet1", 1, 1))],
            limits(),
        )
        .unwrap();
    assert!(
        report
            .unknowns
            .iter()
            .any(|fact| fact.vertex == Some(reader) && fact.reason == UnknownReason::DirtyReader)
    );
    assert!(!contains(
        &report.witnessed,
        e.graph.make_cell_ref("Sheet1", 1, 2)
    ));
    assert!(contains(
        &report.uncertain_downstream,
        e.graph.make_cell_ref("Sheet1", 1, 2)
    ));
}

#[test]
fn current_read_closure_matches_independent_small_dag_model() {
    if isolated_case("current_read_closure_matches_independent_small_dag_model") {
        return;
    }
    for seed in 0..64u64 {
        let mut random = seed + 1;
        let mut e = engine();
        let mut addresses: Vec<String> = Vec::new();
        let mut cells = Vec::new();
        let mut parents: Vec<Option<usize>> = Vec::new();
        for input in 0..3 {
            let row = input + 1;
            e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
            addresses.push(format!("A{row}"));
            cells.push(e.graph.make_cell_ref("Sheet1", row, 1));
            parents.push(None);
        }
        for index in 3..19usize {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let parent = ((random >> 32) as usize) % index;
            let row = (index - 2) as u32;
            let formula = if (index + seed as usize).is_multiple_of(2) {
                format!("=INDIRECT(\"{}\")", addresses[parent])
            } else {
                format!("={}+1", addresses[parent])
            };
            e.set_cell_formula("Sheet1", row, 2, parse(&formula).unwrap())
                .unwrap();
            addresses.push(format!("B{row}"));
            cells.push(e.graph.make_cell_ref("Sheet1", row, 2));
            parents.push(Some(parent));
        }
        e.evaluate_all_for_snapshot(None).unwrap();
        for root in [0usize, 1, 3, 11] {
            let mut expected = 1u32 << root;
            loop {
                let before = expected;
                for (index, parent) in parents.iter().enumerate() {
                    if parent.is_some_and(|parent| expected & (1 << parent) != 0) {
                        expected |= 1 << index;
                    }
                }
                if before == expected {
                    break;
                }
            }
            let before = signature(&e);
            let report = e
                .current_read_affected_closure(&[AffectedRoot::Cell(cells[root])], limits())
                .unwrap();
            assert!(
                report.unknowns.is_empty(),
                "seed{seed} root{root}: {report:?}"
            );
            for (index, &cell) in cells.iter().enumerate() {
                assert_eq!(
                    contains(&report.witnessed, cell),
                    expected & (1 << index) != 0,
                    "seed{seed} root{root} address{}: {report:?}",
                    addresses[index]
                );
            }
            assert!(report.uncertain_downstream.is_empty());
            assert_eq!(signature(&e), before);
        }
    }
}

#[test]
fn current_read_closure_cross_sheet_family_range_needs_no_grid_expansion() {
    if isolated_case("current_read_closure_cross_sheet_family_range_needs_no_grid_expansion") {
        return;
    }
    use crate::engine::authority::geom::Rect;
    let mut e = engine();
    for row in 1..=16 {
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        e.set_cell_formula(
            "Second",
            row,
            2,
            parse(&format!("=Sheet1!A{row}+1")).unwrap(),
        )
        .unwrap();
    }
    e.evaluate_all_for_snapshot(None).unwrap();
    let source_sheet = e.graph.make_cell_ref("Sheet1", 1, 1).sheet_id;
    let report = e
        .current_read_affected_closure(
            &[AffectedRoot::Region(RegionPiece {
                sheet: source_sheet,
                rect: Rect::new(3, 0, 11, 0),
            })],
            limits(),
        )
        .unwrap();
    for row in 1..=16 {
        assert_eq!(
            contains(&report.witnessed, e.graph.make_cell_ref("Second", row, 2)),
            (4..=12).contains(&row)
        );
    }
    assert!(
        report.witnessed.len() < 8,
        "family must remain region geometry"
    );
}
