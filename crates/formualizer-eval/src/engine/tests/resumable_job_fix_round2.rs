use super::*;
use crate::engine::named_range::{NameScope, NamedDefinition};

fn make_tiny(family: bool, names: bool, tail: bool) -> Engine<TestWorkbook> {
    let mut e = engine(family);
    f(&mut e, 1, 10, "=1");
    f(&mut e, 2, 10, "=J1+1");
    f(&mut e, 3, 10, "=J2+1");
    f(&mut e, 1, 2, "=IF(J3>0,SEQUENCE(5),0)");
    if names {
        e.define_name(
            "SpillCell",
            NamedDefinition::Cell(e.graph.make_cell_ref("Sheet1", 5, 2)),
            NameScope::Workbook,
        )
        .unwrap();
        e.define_name(
            "SpillValues",
            NamedDefinition::Formula {
                ast: parse("=B1#").unwrap(),
                dependencies: vec![],
                range_deps: vec![],
            },
            NameScope::Workbook,
        )
        .unwrap();
    }
    for col in 20..40 {
        f(&mut e, 1, col, &format!("={col}"));
    }
    f(&mut e, 1, 4, "=INDIRECT(\"B5\")*2");
    if tail {
        f(&mut e, 2, 4, "=D1+1");
    }
    e
}
fn tiny_cases(names: bool) {
    let mut failures = Vec::new();
    for family in [false, true] {
        for job in [false, true] {
            for tail in [false, true] {
                let mut e = make_tiny(family, names, tail);
                let mut fresh = make_tiny(family, names, tail);
                fresh.evaluate_all().unwrap();
                let reader = e
                    .graph
                    .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 4))
                    .unwrap();
                let sheet = e.graph.sheet_id("Sheet1").unwrap();
                let mut installed_empty = false;
                let mut covered_at_install = false;
                let mut dirty_at_publish = false;
                let mut complete = false;
                for _ in 0..100 {
                    e.resumable_stop_after_units_for_test = Some(1);
                    let before_spill = v(&e, 1, 2);
                    let result = test_recalc_slice(&mut e, EvaluationStop::none(), job).unwrap();
                    if !installed_empty
                        && v(&e, 1, 2).is_none()
                        && v(&e, 1, 4) == Some(LiteralValue::Number(0.))
                        && !e.graph.is_dirty(reader)
                    {
                        installed_empty = true;
                        let observed = e
                            .graph
                            .authority_host()
                            .observed(reader)
                            .map(|r| r.to_vec());
                        println!(
                            "OBS names={names} family={family} job={job} dynamic={} observed={observed:?}",
                            e.graph.is_dynamic(reader)
                        );
                        assert_eq!(observed.as_deref(), Some([(sheet, 4, 1, 4, 1)].as_slice()));
                        e.graph.authority_sync();
                        covered_at_install = e
                            .graph
                            .authority_closure_of_rects(&[(
                                sheet,
                                crate::engine::authority::geom::Rect::new(4, 1, 4, 1),
                            )])
                            .contains(&reader);
                    }
                    if before_spill.is_none() && v(&e, 1, 2).is_some() {
                        dirty_at_publish = e.graph.is_dirty(reader);
                    }
                    if matches!(result, EvaluationProgress::Complete(_)) {
                        complete = true;
                        break;
                    }
                }
                assert!(complete && installed_empty);
                let actual = v(&e, 1, 4);
                let control = v(&fresh, 1, 4);
                println!(
                    "EMPTY_SPILL names={names} family={family} job={job} tail={tail} actual={actual:?} ordinary={control:?} covered={covered_at_install} dirty_at_publish={dirty_at_publish}"
                );
                if actual != Some(LiteralValue::Number(10.))
                    || control != actual
                    || !covered_at_install
                    || !dirty_at_publish
                    || (tail
                        && (v(&e, 2, 4) != Some(LiteralValue::Number(11.))
                            || v(&fresh, 2, 4) != v(&e, 2, 4)))
                {
                    failures.push((family, job, tail));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}
#[test]
fn named_future_spill_indirect_matches_ordinary() {
    tiny_cases(true);
}
#[test]
fn unnamed_future_spill_indirect_matches_ordinary() {
    tiny_cases(false);
}

#[test]
fn empty_grid_read_receipts_keep_future_coordinates() {
    use crate::engine::live_edges::{ReadLog, RecordingContext};
    use crate::traits::EvaluationContext;
    let mut e = engine(false);
    f(&mut e, 1, 10, "=1");
    assert_eq!(v(&e, 5, 2), None);
    let sheet = e.graph.sheet_id("Sheet1").unwrap();
    for (reference, expected) in [("B5", (sheet, 4, 1, 4, 1)), ("B5:B8", (sheet, 4, 1, 7, 1))] {
        let log = ReadLog::default();
        let ctx = RecordingContext::new(&e, &log);
        let reference = formualizer_parse::parser::ReferenceType::from_string(reference).unwrap();
        ctx.resolve_range_view(&reference, "Sheet1").unwrap();
        assert_eq!(log.take(), vec![expected]);
    }
}

#[test]
fn observed_routes_retarget_invalidate_and_clear() {
    use crate::engine::authority::{geom::Rect, host::AuthorityHost};
    let mut host = AuthorityHost::default();
    let reader = crate::engine::VertexId(123);
    let old = [(0, 4, 1, 4, 1)];
    let new = [(1, 6, 2, 8, 2)];
    let query = |host: &AuthorityHost, sheet, rect| {
        let mut out = Vec::new();
        host.observed_readers(sheet, &rect, &mut out);
        out
    };
    host.set_observed(reader, old.to_vec());
    assert_eq!(query(&host, 0, Rect::cell(4, 1)), vec![reader]);
    let revision = host.rev_dyn();
    host.set_observed_with_value(reader, old.to_vec(), false);
    assert_eq!(host.rev_dyn(), revision);
    assert_eq!(
        query(&host, 0, Rect::cell(4, 1)),
        vec![reader],
        "routing survives value invalidation"
    );
    host.set_observed(reader, new.to_vec());
    assert!(query(&host, 0, Rect::cell(4, 1)).is_empty());
    assert_eq!(query(&host, 1, Rect::cell(7, 2)), vec![reader]);
    host.forget_observed(reader);
    assert!(!host.has_observed_routes());
    assert!(query(&host, 1, Rect::cell(7, 2)).is_empty());
    host.set_observed(reader, old.to_vec());
    host.clear_observed();
    assert!(!host.has_observed_routes());
}

fn make_sweep(family: bool, mask: usize, depth: u32) -> Engine<TestWorkbook> {
    let mut e = engine(family);
    f(&mut e, 1, 10, "=1");
    for row in 2..=depth {
        f(&mut e, row, 10, &format!("=J{}+1", row - 1));
    }
    f(&mut e, 1, 2, &format!("=IF(J{depth}>0,SEQUENCE(5),0)"));
    e.define_name(
        "SpillCell",
        NamedDefinition::Cell(e.graph.make_cell_ref("Sheet1", 5, 2)),
        NameScope::Workbook,
    )
    .unwrap();
    e.define_name(
        "SpillValues",
        NamedDefinition::Formula {
            ast: parse("=B1#").unwrap(),
            dependencies: vec![],
            range_deps: vec![],
        },
        NameScope::Workbook,
    )
    .unwrap();
    let readers = [
        "=B5*2",
        "=INDIRECT(\"B5\")*2",
        "=OFFSET(B1,4,0)*2",
        "=SUM(B1#)",
        "=SpillCell*2",
        "=SUM(SpillValues)",
        "=SUM(INDIRECT(\"B5:B8\"))",
    ];
    for (i, s) in readers.iter().enumerate() {
        if mask & (1 << i) != 0 {
            f(&mut e, 1, 3 + i as u32, s);
            f(
                &mut e,
                2,
                3 + i as u32,
                &format!("={}1+1", (b'C' + i as u8) as char),
            );
        }
    }
    e
}
#[test]
fn future_spill_dynamic_reader_96_configuration_sweep() {
    let mut failures = Vec::new();
    for family in [false, true] {
        for job in [false, true] {
            for mask in [1, 2, 4, 8, 16, 32, 3, 6, 10, 18, 34, 127] {
                for depth in [3, 40] {
                    let mut e = make_sweep(family, mask, depth);
                    let mut fresh = make_sweep(family, mask, depth);
                    let mut complete = false;
                    for _ in 0..300 {
                        e.resumable_stop_after_units_for_test = Some(1);
                        if matches!(
                            test_recalc_slice(&mut e, EvaluationStop::none(), job).unwrap(),
                            EvaluationProgress::Complete(_)
                        ) {
                            complete = true;
                            break;
                        }
                    }
                    assert!(complete);
                    fresh.evaluate_all().unwrap();
                    let mut bad = Vec::new();
                    for col in 3..10 {
                        for row in 1..=2 {
                            if v(&e, row, col) != v(&fresh, row, col) {
                                bad.push((row, col, v(&e, row, col), v(&fresh, row, col)));
                            }
                        }
                    }
                    if !bad.is_empty() {
                        println!(
                            "SWEEP family={family} job={job} mask={mask} depth={depth} bad={bad:?}"
                        );
                        failures.push((family, job, mask, depth));
                    }
                }
            }
        }
    }
    println!("SWEEP failures={}/96", failures.len());
    assert!(failures.is_empty(), "{failures:?}");
}
