//! FORM-329: deterministic first-pass dynamic read before a future spill.
use super::*;
use crate::engine::scheduler::Schedule;
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

#[test]
fn same_batch_observed_read_precedes_first_spill_publication() {
    crate::builtins::load_builtins();
    for family in [false, true] {
        for buffered in [false, true] {
            let mut engine = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    family_execution: family,
                    enable_parallel: false,
                    ..Default::default()
                },
            );
            engine
                .set_cell_formula("Sheet1", 1, 4, parse("=SEQUENCE(4)").unwrap())
                .unwrap();
            engine
                .set_cell_formula("Sheet1", 1, 6, parse("=INDIRECT(\"D4\")+1").unwrap())
                .unwrap();
            engine.require_unified_authority().unwrap();
            engine.begin_evaluation_request();
            let anchor = engine
                .graph
                .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 4))
                .unwrap();
            let reader = engine
                .graph
                .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 6))
                .unwrap();
            let schedule = Schedule {
                units: vec![ScheduleUnit::Layer(0)],
                layers: vec![crate::engine::scheduler::Layer::new(vec![reader, anchor])],
                cycles: vec![],
            };
            engine.begin_pass(&schedule);
            // Force the parallel-batch observation order without rayon or hash order:
            // both results exist before either commits, and the reader saw Empty.
            let read_value = engine.evaluate_vertex_immutable(reader).unwrap();
            assert_eq!(read_value, LiteralValue::Number(1.));
            let spill_value = engine.evaluate_vertex_immutable(anchor).unwrap();
            let results = vec![(reader, read_value.clone()), (anchor, spill_value.clone())];
            let guarded = engine.freshness_begin_batch_commit(&results);
            assert!(guarded);
            let mut writes = ComputedWriteBuffer::default();
            // Production parallel groups publish arrays before scalars.
            for (vertex, value) in [(anchor, spill_value), (reader, read_value)] {
                let redirtied = engine.freshness_batch_redirtied(guarded, vertex);
                let effects = engine.plan_vertex_effects(vertex, value).unwrap();
                for effect in &effects {
                    engine
                        .apply_effect_with_computed_writes(
                            effect,
                            None,
                            None,
                            buffered.then_some(&mut writes),
                        )
                        .unwrap();
                }
                engine.freshness_keep_redirtied(redirtied, vertex);
            }
            engine.flush_computed_write_buffer(&mut writes).unwrap();
            assert!(
                engine.finish_pass_dirty(&[reader, anchor], &[]),
                "FR5 must replan a new observed reader: family={family}, buffered={buffered}"
            );
            assert!(engine.graph.is_dirty(reader));
            engine.evaluate_all().unwrap();
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 6),
                Some(LiteralValue::Number(5.))
            );
        }
    }
}
