use super::*;
use crate::test_workbook::TestWorkbook;

fn engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    engine
        .set_cell_formula("S", 1, 1, formualizer_parse::parse("=1").unwrap())
        .unwrap();
    engine
}

#[test]
fn scalar_effects_stay_inline_and_match_vec_oracle() {
    let mut engine = engine();
    let vertex = engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("S", 1, 1))
        .unwrap();
    for value in [
        LiteralValue::Number(42.0),
        LiteralValue::Empty,
        LiteralValue::Text("text".into()),
        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
    ] {
        let expected = vec![Effect::WriteCell {
            vertex_id: vertex,
            value: value.clone(),
        }];
        let actual = engine.plan_vertex_effects(vertex, value).unwrap();
        assert!(!actual.spilled());
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    }
}

// Compare the buffered commit path with the unchanged unbuffered effect
// application, including spill logs and delta reads which must flush first.
#[test]
fn scalar_spill_clear_and_pending_commits_match_unbuffered() {
    for mode in [DeltaMode::Off, DeltaMode::Cells] {
        for logged in [false, true] {
            let mut direct = engine();
            let mut buffered = engine();
            let mut buffer = ComputedWriteBuffer::default();
            let vertex = direct
                .graph
                .get_vertex_id_for_address(&direct.graph.make_cell_ref("S", 1, 1))
                .unwrap();
            let values = [
                LiteralValue::Number(4.0),
                LiteralValue::Array(vec![
                    vec![LiteralValue::Number(1.0)],
                    vec![LiteralValue::Number(2.0)],
                ]),
                LiteralValue::Text("cleared".into()),
                LiteralValue::Array(vec![vec![LiteralValue::Number(3.0)]]),
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)),
                LiteralValue::Empty,
            ];
            for value in values {
                let left = direct.plan_vertex_effects(vertex, value.clone()).unwrap();
                let right = buffered.plan_vertex_effects(vertex, value).unwrap();
                assert_eq!(format!("{left:?}"), format!("{right:?}"));
                let mut dl = DeltaCollector::new(mode);
                let mut dr = DeltaCollector::new(mode);
                let mut ll = ChangeLog::new();
                let mut lr = ChangeLog::new();
                for effect in &left {
                    direct
                        .apply_effect(effect, Some(&mut dl), logged.then_some(&mut ll))
                        .unwrap();
                }
                for effect in &right {
                    buffered
                        .apply_effect_with_computed_writes(
                            effect,
                            Some(&mut dr),
                            logged.then_some(&mut lr),
                            Some(&mut buffer),
                        )
                        .unwrap();
                }
                buffered.flush_computed_write_buffer(&mut buffer).unwrap();
                assert_eq!(dl.finish_target(), dr.finish_target());
                assert_eq!(ll.events(), lr.events());
                assert_eq!(
                    direct.graph.spill_registry_counts(),
                    buffered.graph.spill_registry_counts()
                );
                for row in 1..=3 {
                    assert_eq!(
                        direct.read_cell_value("S", row, 1),
                        buffered.read_cell_value("S", row, 1)
                    );
                }
            }
            // A staged formula blocks the spill; planning records a pending
            // retry which must be retired by a later non-spill scalar result.
            direct.stage_formula_text("S", 2, 1, "1".into());
            buffered.stage_formula_text("S", 2, 1, "1".into());
            for value in [
                LiteralValue::Array(vec![vec![LiteralValue::Number(1.0)]; 2]),
                LiteralValue::Number(9.0),
            ] {
                let left = direct.plan_vertex_effects(vertex, value.clone()).unwrap();
                let right = buffered.plan_vertex_effects(vertex, value).unwrap();
                assert_eq!(format!("{left:?}"), format!("{right:?}"));
                assert_eq!(
                    direct.blocked_pending_spills,
                    buffered.blocked_pending_spills
                );
                assert!(!right.spilled());
            }
            assert!(buffered.blocked_pending_spills.is_empty());
        }
    }
}
