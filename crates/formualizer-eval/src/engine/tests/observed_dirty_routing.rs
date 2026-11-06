use crate::engine::{Engine, EvalConfig, EvaluationTarget};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn engine(family: bool) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            enable_parallel: false,
            ..Default::default()
        },
    )
}
fn formula(e: &mut Engine<TestWorkbook>, row: u32, col: u32, src: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(src).unwrap())
        .unwrap();
}
fn future_spill(family: bool) -> Engine<TestWorkbook> {
    let mut e = engine(family);
    formula(&mut e, 1, 10, "=1");
    formula(&mut e, 2, 10, "=J1+1");
    formula(&mut e, 3, 10, "=J2+1");
    formula(&mut e, 1, 2, "=IF(J3>0,SEQUENCE(5),0)");
    formula(&mut e, 1, 4, "=INDIRECT(\"B5\")*2");
    formula(&mut e, 2, 4, "=D1+1");
    e
}

#[test]
fn future_spill_dirties_indirect_and_static_descendant() {
    for family in [false, true] {
        for api in 0..4 {
            let mut e = future_spill(family);
            match api {
                0 => {
                    e.evaluate_all().unwrap();
                }
                1 => {
                    e.evaluate_cell("Sheet1", 2, 4).unwrap();
                }
                2 => {
                    e.evaluate_targets(&[EvaluationTarget::Cell {
                        sheet: "Sheet1".into(),
                        row: 2,
                        col: 4,
                    }])
                    .unwrap();
                }
                _ => {
                    e.evaluate_all_for_snapshot(None).unwrap();
                }
            }
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 4),
                Some(LiteralValue::Number(10.)),
                "family={family} api={api}"
            );
            assert_eq!(
                e.get_cell_value("Sheet1", 2, 4),
                Some(LiteralValue::Number(11.)),
                "family={family} api={api}"
            );
        }
    }
}

#[test]
fn retarget_and_formula_replacement_remove_observed_routes() {
    for family in [false, true] {
        let mut e = engine(family);
        e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(11.))
            .unwrap();
        e.set_cell_value("Sheet1", 2, 1, LiteralValue::Number(22.))
            .unwrap();
        e.set_cell_value("Sheet1", 1, 8, LiteralValue::Text("A1".into()))
            .unwrap();
        formula(&mut e, 1, 4, "=INDIRECT(H1)");
        e.evaluate_all().unwrap();
        let reader = e
            .graph
            .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 4))
            .unwrap();
        let sheet = e.graph.sheet_id("Sheet1").unwrap();
        let routed = |e: &Engine<TestWorkbook>, row| {
            e.graph
                .authority_closure_of_rects(&[(
                    sheet,
                    crate::engine::authority::geom::Rect::cell(row, 0),
                )])
                .contains(&reader)
        };
        assert!(routed(&e, 0));
        e.set_cell_value("Sheet1", 1, 8, LiteralValue::Text("A2".into()))
            .unwrap();
        e.evaluate_all().unwrap();
        assert!(!routed(&e, 0));
        assert!(routed(&e, 1));
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 4),
            Some(LiteralValue::Number(22.))
        );
        formula(&mut e, 1, 4, "=7");
        e.evaluate_all().unwrap();
        assert!(!routed(&e, 1));
    }
}

#[test]
fn empty_grid_read_receipts_keep_future_coordinates() {
    use crate::engine::live_edges::{ReadLog, RecordingContext};
    use crate::traits::EvaluationContext;
    let mut e = engine(false);
    formula(&mut e, 1, 10, "=1");
    assert_eq!(e.get_cell_value("Sheet1", 5, 2), None);
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
