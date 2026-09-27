//! Actual-engine differential: the authority's Schedule and the legacy
//! Scheduler's Schedule feed the same existing unit executor. This does not
//! enable production cutover or substitute a toy value/SCC implementation.
use super::*;
use crate::engine::authority::geom::{Cover, Rect};
use crate::engine::authority::{plan_schedule, planner};
use crate::engine::{CycleConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parse;
use std::collections::BTreeMap;

fn build(formulas: &[String], cycle: CycleConfig) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        formula_plane_mode: FormulaPlaneMode::Off,
        enable_parallel: false,
        cycle,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    // Allocate IDs in reverse spatial order, independently from stable IDs.
    for (r, formula) in formulas.iter().enumerate().rev() {
        engine
            .set_cell_formula("Sheet1", r as u32 + 1, 1, parse(formula).unwrap())
            .unwrap();
    }
    engine
}

fn normalized_cycles(schedule: &super::super::scheduler::Schedule) -> Vec<Vec<VertexId>> {
    let mut cycles = schedule.cycles.clone();
    for cycle in &mut cycles {
        cycle.sort_unstable();
    }
    cycles.sort_unstable();
    cycles
}

fn compare(formulas: &[String]) {
    compare_with_policy(formulas, CycleConfig::iterate(32, 0.00001));
}

fn compare_with_policy(formulas: &[String], cycle: CycleConfig) {
    let mut actual = build(formulas, cycle);
    let mut legacy = build(formulas, cycle);
    // Repeat to exercise retained iterative state and redirty, not only cold
    // calculation. The oracle uses the actual public legacy evaluation route.
    for pass in 0..2 {
        actual.begin_evaluation_request();
        actual.graph.flush_pending_edge_deltas();
        let candidates = actual.graph.get_evaluation_vertices();
        let mut cover = Cover::new();
        for &id in &candidates {
            let cell = actual.graph.get_cell_ref_for_vertex(id).unwrap();
            cover.insert_rect(
                cell.sheet_id,
                &Rect::cell(cell.coord.row(), cell.coord.col()),
            );
        }
        let (vdeps, augmented) =
            VirtualDepBuilder::new(&actual).build_with_range_members(&candidates);
        assert!(augmented.is_empty());
        let reference = Scheduler::new(&actual.graph)
            .create_schedule_with_virtual(&candidates, &vdeps)
            .unwrap();
        let ordered = planner::plan(
            actual.graph.authority_host().store(),
            &cover,
            None,
            None,
            None,
        )
        .unwrap();
        let plan = plan_schedule::schedule(
            &ordered.cells,
            ordered.heap_bytes(),
            None,
            |cell| {
                let address = CellRef::new(cell.sheet, Coord::new(cell.row, cell.col, true, true));
                Ok(actual.graph.get_vertex_id_for_address(&address).unwrap())
            },
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(
            normalized_cycles(&plan.schedule),
            normalized_cycles(&reference),
            "SCCs pass={pass}: {formulas:?}"
        );
        let mut positions = BTreeMap::new();
        for (position, unit) in plan.schedule.units.iter().enumerate() {
            let (members, cyclic) = match *unit {
                ScheduleUnit::Layer(i) => (plan.schedule.unit_layer(i).vertices.as_slice(), false),
                ScheduleUnit::Cycle(i) => (plan.schedule.unit_cycle(i), true),
            };
            for member in members {
                assert!(positions.insert(*member, (position, cyclic)).is_none());
            }
        }
        assert_eq!(positions.len(), candidates.len());
        for &reader in &candidates {
            for dependency in actual
                .graph
                .get_dependencies(reader)
                .into_iter()
                .chain(vdeps.get(&reader).into_iter().flatten().copied())
            {
                if let Some(&(before, cyclic)) = positions.get(&dependency) {
                    let (after, _) = positions[&reader];
                    assert!(
                        before < after || (before == after && cyclic),
                        "schedule edge {dependency:?}->{reader:?}: {formulas:?}"
                    );
                }
            }
        }
        // No legacy scheduling occurs on this side of the differential.
        actual.legacy_pass_run_units(&plan.schedule).unwrap();
        actual.graph.clear_dirty_flags(&candidates);
        actual.redirty_for_next_recalc();
        actual.recalc_epoch = actual.recalc_epoch.wrapping_add(1);
        legacy.evaluate_all().unwrap();
        for row in 1..=formulas.len() as u32 {
            assert_eq!(
                actual.get_cell_value("Sheet1", row, 1),
                legacy.get_cell_value("Sheet1", row, 1),
                "values pass={pass} row={row}: {formulas:?}"
            );
        }
    }
}

#[test]
fn authority_schedule_actual_values_cycles_and_iteration() {
    for formulas in [
        vec!["=1", "=A1+1", "=SUM(A1:A2)", "=A2+A3"],
        vec!["=A2+1", "=A3+1", "=4", "=A1+1"],
        vec!["=(A2+1)/2", "=(A1+1)/2", "=SUM(A1:A2)", "=A3+1"],
        vec!["=A2+1", "=A1+1", "=A1+1"],
        vec!["=(A1+1)/2"],
        vec!["=IF(FALSE,A1,7)", "=A1+1"],
        vec!["=SUM(A1:A100)/2", "=1"],
        vec!["=IF(FALSE,A2,3)", "=A1+1", "=SUM(A1:A2)"],
    ] {
        compare(&formulas.into_iter().map(str::to_owned).collect::<Vec<_>>());
    }
}

#[test]
fn authority_schedule_actual_static_and_runtime_cycle_error_policy() {
    for detection in [CycleDetection::Static, CycleDetection::Runtime] {
        for formulas in [
            vec!["=A2+1", "=A1+1", "=A1+1"],
            vec!["=IF(FALSE,A2,3)", "=A1+1", "=SUM(A1:A2)"],
        ] {
            compare_with_policy(
                &formulas.into_iter().map(str::to_owned).collect::<Vec<_>>(),
                CycleConfig {
                    detection,
                    policy: CyclePolicy::Error,
                },
            );
        }
    }
}

#[test]
fn authority_schedule_generated_actual_legacy_differentials() {
    for seed in 0..96u32 {
        let n = 8 + seed % 17;
        let formulas: Vec<_> = (1..=n)
            .map(|row| {
                if seed < 64 {
                    if row == 1 {
                        "=1".to_owned()
                    } else if seed % 2 == 0 {
                        format!("=SUM(A1:A{})/{}+1", row - 1, row)
                    } else {
                        format!("=A{}+{}", row - 1, row)
                    }
                } else {
                    let next = row % n + 1;
                    format!("=(A{next}+{})/2", seed % 7)
                }
            })
            .collect();
        compare(&formulas);
    }
}
