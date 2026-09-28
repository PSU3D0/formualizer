use super::alloc::measure;
use crate::engine::authority::plan_schedule::{ScheduleError, schedule};
use crate::engine::authority::planner::OrderedCell;
use crate::engine::authority::store::AuthorityError;
use crate::engine::scheduler::ScheduleUnit;
use crate::engine::vertex::VertexId;
use formualizer_common::{ExcelError, ExcelErrorKind};

fn cell(id: u32, layer: u64, cycle: Option<u64>) -> OrderedCell {
    OrderedCell {
        sheet: 3,
        row: id,
        col: 7,
        id: id + 900,
        owner: 42,
        layer,
        cycle,
        chain: false,
    }
}
fn translate(cell: &OrderedCell) -> Result<VertexId, ExcelError> {
    // Deliberately unrelated to private stable IDs, and reverse spatial order.
    Ok(VertexId::new(100_000 - cell.row))
}
fn fixture(n: u32) -> Vec<OrderedCell> {
    (0..n)
        .rev()
        .map(|i| {
            cell(
                i,
                (i / 7) as u64 * 1_000_000,
                (i % 3 == 0).then_some((i / 7) as u64),
            )
        })
        .collect()
}

#[test]
fn schedule_adapter_sparse_layers_cycles_ids_and_side_table() {
    let input = vec![
        cell(1, u64::MAX, None),
        cell(2, 0, Some(u64::MAX)),
        cell(3, 0, None),
        cell(4, 0, Some(0)),
        cell(5, 0, None),
        cell(6, 0, Some(u64::MAX)),
    ];
    let mut charged = 0;
    let out = schedule(&input, 0, None, translate, |n| {
        assert!(n <= 4096);
        charged += n;
        Ok(())
    })
    .unwrap();
    assert_eq!(charged, out.work);
    assert_eq!(
        out.schedule.units,
        [
            ScheduleUnit::Layer(0),
            ScheduleUnit::Cycle(0),
            ScheduleUnit::Cycle(1),
            ScheduleUnit::Layer(1)
        ]
    );
    assert_eq!(
        out.schedule.layers[0].vertices,
        [VertexId::new(99997), VertexId::new(99995)]
    );
    // Acyclic cells order by authority id; rows 3 and 5 are not adjacent,
    // so the layer has no family run.
    assert!(out.schedule.layers[0].runs.is_empty());
    assert_eq!(out.schedule.cycles[0], [VertexId::new(99996)]);
    assert_eq!(
        out.schedule.cycles[1],
        [VertexId::new(99994), VertexId::new(99998)]
    );
    let order: Vec<_> = out.entries.iter().map(|e| e.cell.row).collect();
    assert_eq!(order, [3, 5, 4, 6, 2, 1]);
    for e in &out.entries {
        assert_eq!(e.cell.id, e.cell.row + 900);
        assert_eq!(e.cell.owner, 42);
        assert_eq!((e.cell.sheet, e.cell.col), (3, 7));
        assert_eq!(e.vertex, translate(&e.cell).unwrap());
    }
    let empty = schedule(&[], 0, Some(0), translate, |_| Ok(())).unwrap();
    assert_eq!(empty.work, 0);
    assert_eq!(empty.heap_bytes(), 0);
}

#[test]
fn schedule_adapter_fail_every_allocation_and_simultaneous_admission() {
    let input = fixture(127);
    let held = 12345;
    let (result, m) = measure(None, || schedule(&input, held, None, translate, |_| Ok(())));
    let out = result.unwrap();
    assert_eq!(out.heap_bytes(), m.net as u64);
    assert_eq!(out.peak_heap_bytes, held + m.peak as u64);
    for nth in 0..m.allocs {
        let (result, failed) = measure(Some(nth), || {
            schedule(&input, held, None, translate, |_| Ok(()))
        });
        assert!(failed.failed, "allocation {nth}");
        assert!(matches!(
            result,
            Err(ScheduleError::Authority(AuthorityError::Alloc))
        ));
        assert_eq!(failed.net, 0);
    }
    for limit in [0, held, out.peak_heap_bytes - 1] {
        let (result, failed) = measure(None, || {
            schedule(&input, held, Some(limit), translate, |_| Ok(()))
        });
        assert!(matches!(
            result,
            Err(ScheduleError::Authority(AuthorityError::Admission {
                resource: "scratch",
                ..
            }))
        ));
        assert_eq!(failed.net, 0);
    }
    schedule(&input, held, Some(out.peak_heap_bytes), translate, |_| {
        Ok(())
    })
    .unwrap();
    println!(
        "SCHEDULE_ALLOC allocations={} peak={} retained={} work={}",
        m.allocs, m.peak, m.net, out.work
    );
}

#[test]
fn schedule_adapter_cancellation_at_every_checkpoint_and_translation_failure() {
    // Constant key bytes skip their radix passes, so the work per cell is
    // smaller than with all 32 passes: 4096 cells give enough checkpoints.
    let input = fixture(4096);
    let mut calls = 0;
    schedule(&input, 0, None, translate, |_| {
        calls += 1;
        Ok(())
    })
    .unwrap();
    assert!(calls > 10);
    for fail in 0..calls {
        let mut call = 0;
        let (result, m) = measure(None, || {
            schedule(&input, 0, None, translate, |_| {
                let current = call;
                call += 1;
                if current == fail {
                    Err(ExcelError::new(ExcelErrorKind::Value))
                } else {
                    Ok(())
                }
            })
        });
        assert!(matches!(result, Err(ScheduleError::Runtime(_))));
        assert_eq!(m.net, 0);
    }
    let (result, m) = measure(None, || {
        schedule(
            &input,
            0,
            None,
            |_| Err(ExcelError::new(ExcelErrorKind::Ref)),
            |_| Ok(()),
        )
    });
    assert!(matches!(result, Err(ScheduleError::Runtime(_))));
    assert_eq!(m.net, 0);
}

#[test]
fn schedule_adapter_actual_legacy_scc_and_legality() {
    use crate::engine::authority::geom::{Cover, Rect};
    use crate::engine::authority::planner::plan;
    use crate::engine::scheduler::Scheduler;
    use crate::engine::virtual_deps::VirtualDepBuilder;
    use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
    use crate::reference::{CellRef, Coord};
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parse;
    use std::collections::BTreeMap;

    // Include a large range self-reference (not CSR-expanded), genuine cycles,
    // a cycle's downstream reader, and both directions of acyclic chains.
    let fixtures = [
        vec!["=1", "=A1+1", "=A2+1", "=SUM(A1:A3)"],
        vec!["=A2+1", "=A3+1", "=1", "=A1+1"],
        vec!["=A2+1", "=A1+1", "=A1+1", "=1"],
        vec!["=A1+1"],
        vec!["=SUM(A1:A100)"],
        vec!["=SUM(A1:A100)", "=A1+1", "=SUM(A1:A2)"],
    ];
    for formulas in fixtures {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                formula_plane_mode: FormulaPlaneMode::Off,
                cycle: crate::engine::CycleConfig::iterate_excel_defaults(),
                ..EvalConfig::default()
            },
        );
        for (i, text) in formulas.iter().enumerate().rev() {
            engine
                .set_cell_formula("Sheet1", i as u32 + 1, 1, parse(text).unwrap())
                .unwrap();
        }
        engine.graph.authority().unwrap();
        engine.graph.flush_pending_edge_deltas();
        let sid = engine.graph.sheet_id("Sheet1").unwrap();
        let addr = |r| CellRef::new(sid, Coord::new(r, 0, true, true));
        let vertices: Vec<_> = (0..formulas.len() as u32)
            .map(|r| engine.graph.get_vertex_id_for_address(&addr(r)).unwrap())
            .collect();
        let (vdeps, augmented) =
            VirtualDepBuilder::new(&engine).build_with_range_members(&vertices);
        assert!(augmented.is_empty());
        let legacy = Scheduler::new(&engine.graph)
            .create_schedule_with_virtual(&vertices, &vdeps)
            .unwrap();
        let mut cover = Cover::new();
        cover.insert_rect(sid, &Rect::new(0, 0, formulas.len() as u32 - 1, 0));
        let ordered = plan(
            engine.graph.authority_host().store(),
            &cover,
            None,
            None,
            None,
        )
        .unwrap();
        let actual = schedule(
            &ordered.cells,
            ordered.heap_bytes(),
            None,
            |cell| {
                assert_eq!(cell.sheet, sid);
                Ok(engine
                    .graph
                    .get_vertex_id_for_address(&addr(cell.row))
                    .unwrap())
            },
            |_| Ok(()),
        )
        .unwrap();
        let normalize = |cycles: &[Vec<VertexId>]| {
            let mut out: Vec<Vec<u32>> = cycles
                .iter()
                .map(|cycle| {
                    let mut ids: Vec<_> = cycle.iter().map(|id| id.0).collect();
                    ids.sort_unstable();
                    ids
                })
                .collect();
            out.sort_unstable();
            out
        };
        assert_eq!(
            normalize(&actual.schedule.cycles),
            normalize(&legacy.cycles),
            "actual legacy SCCs: {formulas:?}"
        );
        let mut positions = BTreeMap::new();
        for (position, unit) in actual.schedule.units.iter().enumerate() {
            let members = match unit {
                ScheduleUnit::Layer(i) => &actual.schedule.layers[*i as usize].vertices,
                ScheduleUnit::Cycle(i) => &actual.schedule.cycles[*i as usize],
            };
            for member in members {
                assert!(
                    positions
                        .insert(*member, (position, matches!(unit, ScheduleUnit::Cycle(_))))
                        .is_none()
                );
            }
        }
        assert_eq!(positions.len(), vertices.len());
        for reader in &vertices {
            for predecessor in engine
                .graph
                .get_dependencies(*reader)
                .into_iter()
                .chain(vdeps.get(reader).into_iter().flatten().copied())
            {
                if let Some(&(before, cycle)) = positions.get(&predecessor) {
                    let (after, _) = positions[reader];
                    assert!(
                        before < after || (before == after && cycle),
                        "legacy edge {predecessor:?} -> {reader:?}: {formulas:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn schedule_adapter_counted_linear_scaling() {
    let mut prev = 0;
    for n in [1024, 4096, 16384] {
        let input = fixture(n);
        let out = schedule(&input, 0, None, translate, |_| Ok(())).unwrap();
        if prev != 0 {
            assert!(out.work <= 4 * prev);
        }
        // 25 radix passes (position key for family runs) plus the run scan.
        assert!(out.work <= 56 * n as u64 + 25 * 512);
        println!(
            "SCHEDULE_SCALE cells={n} work={} peak={} retained={}",
            out.work,
            out.peak_heap_bytes,
            out.heap_bytes()
        );
        prev = out.work;
    }
}

/// Large plans whose cells come as runs (a family slice at one layer): the
/// adapter sorts the runs, not the cells, with exactly the cell sort's
/// order (debug builds also compare with the cell sort).
#[test]
fn schedule_adapter_sorts_input_runs_like_cells() {
    let mut input = Vec::new();
    let mut id = 0u32;
    // Slices of 500 rows over 8 columns and 2 sheets, emitted in a
    // scrambled slice order, layer by column, plus single cells and a cycle.
    let mut slices: Vec<(u16, u32, u32)> = Vec::new();
    for sheet in [1u16, 4] {
        for col in 0..8u32 {
            for block in 0..6u32 {
                slices.push((sheet, col, block * 500));
            }
        }
    }
    let mut k = 7usize;
    while !slices.is_empty() {
        k = (k * 31 + 11) % slices.len();
        let (sheet, col, row0) = slices.swap_remove(k);
        for r in row0..row0 + 500 {
            input.push(OrderedCell {
                sheet,
                row: r,
                col,
                id,
                owner: col,
                layer: u64::from(col % 3) * 10 + u64::from(sheet),
                cycle: None,
                chain: false,
            });
            id += 1;
        }
        if id.is_multiple_of(3) {
            input.push(OrderedCell {
                sheet,
                row: 90_000 + id,
                col: 40,
                id,
                owner: 999,
                layer: 5,
                cycle: Some(2),
                chain: false,
            });
            id += 1;
        }
    }
    let n = input.len();
    let out = schedule(
        &input,
        0,
        None,
        |c| Ok(VertexId::new(c.id + 1_000)),
        |_| Ok(()),
    )
    .unwrap();
    // Reference: sort by (layer, cyclic, cycle, member), stable.
    let mut want = input.clone();
    want.sort_by_key(|c| {
        let member = if c.cycle.is_some() {
            u64::from(c.id + 1_000)
        } else {
            (u64::from(c.sheet) << 44) | (u64::from(c.col) << 24) | u64::from(c.row)
        };
        (c.layer, c.cycle.is_some(), c.cycle.unwrap_or(0), member)
    });
    assert_eq!(out.entries.len(), n);
    for (e, w) in out.entries.iter().zip(&want) {
        assert_eq!(
            (e.cell.sheet, e.cell.row, e.cell.col, e.cell.id),
            (w.sheet, w.row, w.col, w.id)
        );
    }
    let layered: usize = out.schedule.layers.iter().map(|l| l.vertices.len()).sum();
    let cycled: usize = out.schedule.cycles.iter().map(|c| c.len()).sum();
    assert_eq!(layered + cycled, n);
}
