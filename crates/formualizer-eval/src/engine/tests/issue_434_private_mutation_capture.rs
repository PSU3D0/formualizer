use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{
    ChangeEvent, ChangeLog, EditorError, Engine, EvalConfig, FormulaIngestBatch,
    FormulaIngestRecord, FormulaPlaneMode, RowVisibilitySource,
};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorExtra, LiteralValue, PlanStaleReason};
use formualizer_parse::parser::parse;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

type TestEngine = Engine<TestWorkbook>;

#[derive(Clone, Copy, Debug)]
enum LogPolicy {
    Enabled,
    DefaultDisabled,
    ExplicitDisabled,
    Zero,
    Saturated,
}

const POLICIES: [LogPolicy; 5] = [
    LogPolicy::Enabled,
    LogPolicy::DefaultDisabled,
    LogPolicy::ExplicitDisabled,
    LogPolicy::Zero,
    LogPolicy::Saturated,
];

fn config() -> EvalConfig {
    EvalConfig {
        formula_plane_mode: FormulaPlaneMode::Off,
        enable_parallel: false,
        enable_virtual_dep_telemetry: true,
        ..EvalConfig::default()
    }
}

fn setup_chain() -> TestEngine {
    let mut engine = Engine::new(TestWorkbook::new(), config());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=B1+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

fn cell(engine: &TestEngine, row: u32, col: u32) -> CellRef {
    CellRef::new(
        engine.sheet_id("Sheet1").unwrap(),
        Coord::from_excel(row, col, true, true),
    )
}

fn sentinel_event(value: i64) -> ChangeEvent {
    ChangeEvent::SetValue {
        addr: CellRef::new(0, Coord::new(100, 0, true, true)),
        old_value: None,
        old_formula: None,
        new: LiteralValue::Int(value),
    }
}

fn log_for(policy: LogPolicy) -> ChangeLog {
    match policy {
        LogPolicy::Enabled => ChangeLog::new(),
        LogPolicy::DefaultDisabled => ChangeLog::default(),
        LogPolicy::ExplicitDisabled => {
            let mut log = ChangeLog::new();
            log.set_enabled(false);
            log
        }
        LogPolicy::Zero => ChangeLog::with_max_changelog_events(0),
        LogPolicy::Saturated => {
            let mut log = ChangeLog::with_max_changelog_events(3);
            for value in 1..=3 {
                log.record(sentinel_event(value));
            }
            log
        }
    }
}

fn reverse_direct(engine: &mut TestEngine, log: &mut ChangeLog) {
    let b1 = cell(engine, 1, 2);
    let c1 = cell(engine, 1, 3);
    engine
        .edit_with_logger(log, |editor| {
            editor.set_cell_formula(b1, parse("=C1+1").unwrap());
            editor.set_cell_formula(c1, parse("=A1+10").unwrap());
        })
        .unwrap();
}

fn reverse_action(engine: &mut TestEngine, log: &mut ChangeLog) {
    engine
        .action_with_logger(log, "reverse", |action| {
            action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
            action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())
        })
        .unwrap();
}

fn assert_reversed_graph(engine: &TestEngine) {
    let a1 = engine
        .graph
        .get_vertex_for_cell(&cell(engine, 1, 1))
        .unwrap();
    let b1 = engine
        .graph
        .get_vertex_for_cell(&cell(engine, 1, 2))
        .unwrap();
    let c1 = engine
        .graph
        .get_vertex_for_cell(&cell(engine, 1, 3))
        .unwrap();
    assert_eq!(engine.graph.get_dependencies(b1), vec![c1]);
    assert_eq!(engine.graph.get_dependencies(c1), vec![a1]);
}

fn assert_values(engine: &mut TestEngine, b1: f64, c1: f64) {
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(b1))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(c1))
    );
}

fn assert_audit_policy(policy: LogPolicy, log: &ChangeLog, enabled_len: usize) {
    match policy {
        LogPolicy::Enabled => {
            assert_eq!(log.len(), enabled_len, "policy={policy:?}");
        }
        LogPolicy::DefaultDisabled | LogPolicy::ExplicitDisabled | LogPolicy::Zero => {
            assert!(log.is_empty(), "policy={policy:?}");
        }
        LogPolicy::Saturated => assert_eq!(log.len(), 3),
    }
}

#[test]
fn direct_formula_capture_is_complete_for_every_audit_policy() {
    for policy in POLICIES {
        let mut engine = setup_chain();
        let plan = engine.build_recalc_plan().unwrap();
        let epoch = engine.topology_epoch_for_test();
        let mut log = log_for(policy);

        reverse_direct(&mut engine, &mut log);

        assert_reversed_graph(&engine);
        assert_values(&mut engine, 12.0, 11.0);
        assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
        assert_eq!(engine.topology_epoch_for_test(), epoch.wrapping_add(1));
        let error = engine.evaluate_recalc_plan(&plan).unwrap_err();
        assert!(matches!(
            error.extra,
            ExcelErrorExtra::PlanStale {
                reason: PlanStaleReason::Graph
            }
        ));
        assert_audit_policy(policy, &log, 2);
    }
}

#[test]
fn direct_existing_value_capture_keeps_warm_schedule_for_every_audit_policy() {
    for policy in POLICIES {
        let mut engine = setup_chain();
        let plan = engine.build_recalc_plan().unwrap();
        let epoch = engine.topology_epoch_for_test();
        let a1 = cell(&engine, 1, 1);
        let mut log = log_for(policy);

        engine
            .edit_with_logger(&mut log, |editor| {
                editor.set_cell_value(a1, LiteralValue::Int(5));
            })
            .unwrap();

        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(5.0))
        );
        assert_eq!(engine.topology_epoch_for_test(), epoch);
        assert_values(&mut engine, 6.0, 7.0);
        assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
        engine.evaluate_recalc_plan(&plan).unwrap();
        assert_audit_policy(policy, &log, 1);
    }
}

#[test]
fn atomic_success_and_rollback_are_independent_of_audit_policy() {
    for policy in POLICIES {
        let mut committed = setup_chain();
        let mut commit_log = log_for(policy);
        reverse_action(&mut committed, &mut commit_log);
        assert_reversed_graph(&committed);
        assert_values(&mut committed, 12.0, 11.0);
        assert_eq!(
            committed.last_virtual_dep_telemetry().schedule_cache_hits,
            0
        );
        assert_audit_policy(policy, &commit_log, 4);

        let mut rolled_back = setup_chain();
        let mut rollback_log = log_for(policy);
        let before_events = rollback_log.events().to_vec();
        let before_meta = (0..rollback_log.len())
            .map(|index| rollback_log.event_meta(index).cloned().unwrap_or_default())
            .collect::<Vec<_>>();
        let before_seq_group = (0..rollback_log.len())
            .map(|index| rollback_log.meta(index).unwrap())
            .collect::<Vec<_>>();

        let result = rolled_back.action_with_logger(
            &mut rollback_log,
            "failed multi-edit",
            |action| -> Result<(), EditorError> {
                action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(99))?;
                action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
                action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())?;
                Err(EditorError::TransactionFailed {
                    reason: "intentional".to_string(),
                })
            },
        );

        assert!(result.is_err(), "policy={policy:?}");
        assert_eq!(rollback_log.events(), before_events.as_slice());
        assert_eq!(
            (0..rollback_log.len())
                .map(|index| rollback_log.event_meta(index).cloned().unwrap_or_default())
                .collect::<Vec<_>>(),
            before_meta
        );
        assert_eq!(
            (0..rollback_log.len())
                .map(|index| rollback_log.meta(index).unwrap())
                .collect::<Vec<_>>(),
            before_seq_group
        );
        assert_eq!(
            rolled_back.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(1.0))
        );
        assert_values(&mut rolled_back, 2.0, 3.0);
    }
}

fn setup_spill() -> TestEngine {
    let mut engine = Engine::new(TestWorkbook::new(), config());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );
    engine
}

#[test]
fn spill_multievent_capture_rolls_back_for_every_audit_policy() {
    for policy in POLICIES {
        let mut engine = setup_spill();
        let mut log = log_for(policy);
        let result = engine.action_with_logger(
            &mut log,
            "failed spill clear",
            |action| -> Result<(), EditorError> {
                action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(9))?;
                Err(EditorError::TransactionFailed {
                    reason: "intentional".to_string(),
                })
            },
        );
        assert!(result.is_err(), "policy={policy:?}");
        assert_eq!(
            engine.get_cell_value("Sheet1", 2, 2),
            Some(LiteralValue::Number(4.0)),
            "policy={policy:?}"
        );
        assert!(engine.get_cell("Sheet1", 1, 1).unwrap().0.is_some());
    }
}

#[test]
fn explicit_action_journal_is_complete_and_replays_graph_arrow_and_visibility() {
    let mut engine = setup_chain();
    let (_, journal) = engine
        .action_atomic_journal("journal controls", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))?;
            action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
            action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())?;
            action.set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)
        })
        .unwrap();
    assert!(journal.graph.events.len() >= 4);
    assert!(!journal.arrow.is_empty());
    assert_reversed_graph(&engine);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_values(&mut engine, 16.0, 15.0);

    let mut undo = UndoEngine::new();
    undo.push_action(journal);
    engine.undo_action(&mut undo).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(false)
    );
    assert_values(&mut engine, 2.0, 3.0);

    engine.redo_action(&mut undo).unwrap();
    assert_reversed_graph(&engine);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_values(&mut engine, 16.0, 15.0);
}

#[test]
fn explicit_value_journal_preserves_data_only_schedule_controls() {
    let mut engine = setup_chain();
    let (_, journal) = engine
        .action_atomic_journal("value only", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))
        })
        .unwrap();
    let epoch = engine.topology_epoch_for_test();
    let mut undo = UndoEngine::new();
    undo.push_action(journal);

    assert_values(&mut engine, 6.0, 7.0);
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
    engine.undo_action(&mut undo).unwrap();
    assert_eq!(engine.topology_epoch_for_test(), epoch);
    assert_values(&mut engine, 2.0, 3.0);
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
    engine.redo_action(&mut undo).unwrap();
    assert_eq!(engine.topology_epoch_for_test(), epoch);
    assert_values(&mut engine, 6.0, 7.0);
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
}

fn named_cell(engine: &TestEngine, row: u32, col: u32) -> NamedDefinition {
    let cell = cell(engine, row, col);
    NamedDefinition::Range(RangeRef::new(cell, cell))
}

#[test]
fn generic_name_rejection_discards_disabled_and_saturated_audit() {
    for policy in [LogPolicy::DefaultDisabled, LogPolicy::Saturated] {
        let mut engine = setup_chain();
        let old_definition = named_cell(&engine, 1, 1);
        let new_definition = named_cell(&engine, 2, 1);
        engine
            .define_name("Data", old_definition.clone(), NameScope::Workbook)
            .unwrap();
        let mut log = log_for(policy);
        let retained_events = log.events().to_vec();
        let retained_meta = (0..log.len())
            .map(|index| log.event_meta(index).cloned().unwrap_or_default())
            .collect::<Vec<_>>();
        let retained_seq_group = (0..log.len())
            .map(|index| log.meta(index).unwrap())
            .collect::<Vec<_>>();

        let result = engine.edit_with_logger(&mut log, |editor| {
            editor.update_name("Data", new_definition, NameScope::Workbook)
        });

        assert!(result.is_err(), "policy={policy:?}");
        assert_eq!(log.events(), retained_events.as_slice());
        assert_eq!(
            (0..log.len())
                .map(|index| log.event_meta(index).cloned().unwrap_or_default())
                .collect::<Vec<_>>(),
            retained_meta
        );
        assert_eq!(
            (0..log.len())
                .map(|index| log.meta(index).unwrap())
                .collect::<Vec<_>>(),
            retained_seq_group
        );
        assert_eq!(
            engine
                .resolve_name_entry("Data", engine.sheet_id("Sheet1").unwrap())
                .unwrap()
                .definition,
            old_definition
        );
    }
}

#[test]
fn direct_result_return_remains_ok_with_inner_error() {
    let mut engine = setup_chain();
    let mut log = ChangeLog::new();
    let a1 = cell(&engine, 1, 1);

    let result: Result<Result<(), EditorError>, EditorError> =
        engine.edit_with_logger(&mut log, |editor| {
            editor.set_cell_value(a1, LiteralValue::Int(7));
            Err(EditorError::TransactionFailed {
                reason: "inner result".to_string(),
            })
        });

    assert!(matches!(
        result,
        Ok(Err(EditorError::TransactionFailed { .. }))
    ));
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(log.len(), 1);
}

fn setup_active_formula_plane_span() -> TestEngine {
    const ROWS: u32 = 120;
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            formula_plane_mode: FormulaPlaneMode::AuthoritativeExperimental,
            enable_parallel: false,
            ..EvalConfig::default()
        },
    );
    let mut formulas = Vec::with_capacity(ROWS as usize);
    for row in 1..=ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let source = format!("=A{row}*2");
        let ast = parse(&source).unwrap();
        let ast_id = engine.intern_formula_ast(&ast);
        formulas.push(FormulaIngestRecord::new(
            row,
            2,
            ast_id,
            Some(Arc::<str>::from(source)),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn active_formula_plane_value_edit_is_correct_for_every_audit_policy() {
    for policy in POLICIES {
        let mut engine = setup_active_formula_plane_span();
        let source = cell(&engine, 50, 1);
        let mut log = log_for(policy);

        engine
            .edit_with_logger(&mut log, |editor| {
                editor.set_cell_value(source, LiteralValue::Int(1_000));
            })
            .unwrap();

        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
        assert_eq!(
            engine.get_cell_value("Sheet1", 50, 1),
            Some(LiteralValue::Number(1_000.0))
        );
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 50, 2),
            Some(LiteralValue::Number(2_000.0)),
            "policy={policy:?}"
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 49, 2),
            Some(LiteralValue::Number(98.0)),
            "policy={policy:?}"
        );
        assert_eq!(
            engine
                .last_formula_plane_span_eval_report()
                .unwrap()
                .span_eval_placement_count,
            1,
            "policy={policy:?}"
        );
        assert_audit_policy(policy, &log, 1);
    }
}

#[test]
fn audit_publication_preserves_metadata_sequences_groups_and_caller_compounds() {
    let mut engine = setup_chain();
    let mut log = ChangeLog::new();
    log.set_actor_id(Some("actor".to_string()));
    log.set_correlation_id(Some("correlation".to_string()));
    log.set_reason(Some("reason".to_string()));
    log.begin_compound("caller".to_string());
    assert_eq!(log.compound_depth(), 1);

    engine
        .action_with_logger(&mut log, "insert", |action| {
            action.insert_rows("Sheet1", 1, 1).map(|_| ())
        })
        .unwrap();

    assert_eq!(log.compound_depth(), 1);
    let seqs = (0..log.len())
        .map(|index| log.meta(index).unwrap())
        .collect::<Vec<_>>();
    assert!(seqs.windows(2).all(|pair| pair[1].0 == pair[0].0 + 1));
    let group = seqs[0].1;
    assert!(group.is_some());
    assert!(seqs.iter().all(|(_, event_group)| *event_group == group));
    assert!(
        log.events()
            .iter()
            .any(|event| matches!(event, ChangeEvent::CompoundStart { depth: 2, .. }))
    );
    assert!(
        log.events()
            .iter()
            .any(|event| matches!(event, ChangeEvent::CompoundStart { depth: 3, .. }))
    );
    for index in 0..log.len() {
        let meta = log.event_meta(index).unwrap();
        assert_eq!(meta.actor_id.as_deref(), Some("actor"));
        assert_eq!(meta.correlation_id.as_deref(), Some("correlation"));
        assert_eq!(meta.reason.as_deref(), Some("reason"));
    }
    log.end_compound();
    assert_eq!(log.compound_depth(), 0);
}

#[test]
fn zero_cap_does_not_reset_a_caller_compound() {
    let mut engine = setup_chain();
    let mut log = ChangeLog::with_max_changelog_events(0);
    log.begin_compound("caller".to_string());
    assert_eq!(log.compound_depth(), 1);
    engine
        .action_with_logger(&mut log, "value", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))
        })
        .unwrap();
    assert!(log.is_empty());
    assert_eq!(log.compound_depth(), 1);
    log.end_compound();
    assert_eq!(log.compound_depth(), 0);
}

#[test]
fn saturated_publication_retains_aligned_audit_tail() {
    let mut engine = setup_chain();
    let mut log = ChangeLog::with_max_changelog_events(3);
    log.set_actor_id(Some("actor".to_string()));
    for value in 1..=3 {
        log.record(sentinel_event(value));
    }

    reverse_action(&mut engine, &mut log);

    assert_eq!(log.len(), 3);
    assert_eq!(
        (0..log.len())
            .map(|index| log.meta(index).unwrap().0)
            .collect::<Vec<_>>(),
        vec![4, 5, 6]
    );
    let group = log.meta(0).unwrap().1;
    assert!(group.is_some());
    assert!((0..log.len()).all(|index| log.meta(index).unwrap().1 == group));
    assert!(
        (0..log.len())
            .all(|index| { log.event_meta(index).unwrap().actor_id.as_deref() == Some("actor") })
    );
    assert!(matches!(
        log.events().last(),
        Some(ChangeEvent::CompoundEnd { depth: 1 })
    ));
}

#[test]
fn failed_action_preserves_sequence_and_group_gaps_without_evicting_history() {
    let mut engine = setup_chain();
    let mut log = ChangeLog::with_max_changelog_events(2);
    log.record(sentinel_event(1));
    log.record(sentinel_event(2));
    let retained = log.events().to_vec();

    let result =
        engine.action_with_logger(&mut log, "failed", |action| -> Result<(), EditorError> {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(9))?;
            Err(EditorError::TransactionFailed {
                reason: "intentional".to_string(),
            })
        });
    assert!(result.is_err());
    assert_eq!(log.events(), retained.as_slice());

    log.record(sentinel_event(3));
    assert_eq!(log.meta(log.len() - 1).unwrap().0, 5);
    engine
        .action_with_logger(&mut log, "success", |_action| Ok(()))
        .unwrap();
    let group = log.meta(log.len() - 1).unwrap().1.unwrap();
    assert_eq!(group, 2);
}

#[test]
fn panics_drop_private_capture_without_stranding_external_or_action_state() {
    let mut engine = setup_chain();
    let mut log = ChangeLog::new();
    log.record(sentinel_event(1));
    let retained = log.events().to_vec();

    let direct = catch_unwind(AssertUnwindSafe(|| {
        let a1 = cell(&engine, 1, 1);
        let _ = engine.edit_with_logger(&mut log, |editor| {
            editor.set_cell_value(a1, LiteralValue::Int(7));
            panic!("direct capture panic");
        });
    }));
    assert!(direct.is_err());
    assert_eq!(log.events(), retained.as_slice());
    assert_eq!(log.compound_depth(), 0);

    let action = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.action_with_logger(&mut log, "panic", |tx| -> Result<(), EditorError> {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(8))?;
            panic!("action capture panic");
        });
    }));
    assert!(action.is_err());
    assert_eq!(log.events(), retained.as_slice());
    assert_eq!(log.compound_depth(), 0);

    engine
        .action_with_logger(&mut log, "after panic", |tx| {
            tx.set_cell_value("Sheet1", 2, 1, LiteralValue::Int(9))
        })
        .unwrap();
}
