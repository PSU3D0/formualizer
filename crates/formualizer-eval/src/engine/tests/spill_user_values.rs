use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn formula(e: &mut Engine<TestWorkbook>, row: u32, col: u32, src: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(src).unwrap())
        .unwrap();
}
fn make(family: bool, parallel: bool) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            enable_parallel: parallel,
            max_threads: Some(4),
            ..Default::default()
        },
    );
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.))
        .unwrap();
    formula(&mut e, 1, 2, "=SEQUENCE(A1)");
    formula(&mut e, 1, 3, "=B1+1");
    e
}
#[test]
fn spill_shrink_preserves_later_user_value() {
    for family in [false, true] {
        for parallel in [false, true] {
            for preinstalled in [false, true] {
                let mut e = make(family, parallel);
                if preinstalled {
                    e.evaluate_all().unwrap();
                }
                e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.))
                    .unwrap();
                e.set_cell_value("Sheet1", 4, 2, LiteralValue::Number(99.))
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(
                    e.get_cell_value("Sheet1", 4, 2),
                    Some(LiteralValue::Number(99.)),
                    "family={family} parallel={parallel} preinstalled={preinstalled}"
                );
                assert_eq!(
                    e.get_cell_value("Sheet1", 1, 2),
                    Some(LiteralValue::Number(1.))
                );
                // Repeated teardown and a later user edit must also keep it.
                e.set_cell_value("Sheet1", 4, 2, LiteralValue::Number(100.))
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(
                    e.get_cell_value("Sheet1", 4, 2),
                    Some(LiteralValue::Number(100.))
                );
            }
        }
    }
}
#[test]
fn spill_clear_and_move_preserve_user_cells() {
    for family in [false, true] {
        for action in 0..3 {
            let mut e = make(family, false);
            e.evaluate_all().unwrap();
            e.set_cell_value("Sheet1", 4, 2, LiteralValue::Number(99.))
                .unwrap();
            match action {
                0 => {
                    e.set_cell_value("Sheet1", 1, 2, LiteralValue::Empty)
                        .unwrap();
                }
                1 => formula(&mut e, 1, 2, "=7"),
                2 => {
                    e.set_cell_value("Sheet1", 1, 2, LiteralValue::Empty)
                        .unwrap();
                    formula(&mut e, 1, 5, "=SEQUENCE(A1)");
                }
                _ => unreachable!(),
            }
            e.evaluate_all().unwrap();
            let row = 4;
            assert_eq!(
                e.get_cell_value("Sheet1", row, 2),
                Some(LiteralValue::Number(99.)),
                "family={family} action={action}"
            );
        }
    }
}
#[test]
fn user_value_inside_live_spill_blocks_and_recovers() {
    for family in [false, true] {
        let mut e = make(family, false);
        e.evaluate_all().unwrap();
        // Equal to the projected value: ownership, not value equality, matters.
        e.set_cell_value("Sheet1", 4, 2, LiteralValue::Number(4.))
            .unwrap();
        e.evaluate_all().unwrap();
        assert!(matches!(e.get_cell_value("Sheet1", 1, 2),
            Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Spill));
        assert_eq!(
            e.get_cell_value("Sheet1", 4, 2),
            Some(LiteralValue::Number(4.))
        );
        e.set_cell_value("Sheet1", 4, 2, LiteralValue::Empty)
            .unwrap();
        e.evaluate_all().unwrap();
        for row in 1..=5 {
            assert_eq!(
                e.get_cell_value("Sheet1", row, 2),
                Some(LiteralValue::Number(row as f64))
            );
        }
    }
}

#[test]
fn literal_blocker_removal_wakes_only_affected_anchor() {
    for family in [false, true] {
        for before_first_evaluation in [false, true] {
            let mut e = make(family, false);
            // Remove the descendant so computed_vertices counts only anchors.
            e.set_cell_value("Sheet1", 1, 3, LiteralValue::Empty)
                .unwrap();
            e.add_sheet("Aux").unwrap();
            e.set_cell_formula("Aux", 1, 2, parse("=SEQUENCE(5)").unwrap())
                .unwrap();
            if !before_first_evaluation {
                e.evaluate_all().unwrap();
            }
            e.set_cell_value("Sheet1", 3, 2, LiteralValue::Number(99.))
                .unwrap();
            e.set_cell_value("Sheet1", 4, 2, LiteralValue::Number(88.))
                .unwrap();
            e.set_cell_value("Aux", 3, 2, LiteralValue::Number(77.))
                .unwrap();
            e.evaluate_all().unwrap();

            e.set_cell_value("Sheet1", 3, 2, LiteralValue::Empty)
                .unwrap();
            let one = e.evaluate_all().unwrap();
            assert_eq!(one.computed_vertices, 1, "only the edited blocker's anchor");
            assert!(matches!(e.get_cell_value("Sheet1",1,2),
                Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Spill));
            e.set_cell_value("Aux", 3, 2, LiteralValue::Empty).unwrap();
            assert_eq!(
                e.evaluate_all().unwrap().computed_vertices,
                1,
                "other sheet only"
            );
            assert_eq!(
                e.get_cell_value("Aux", 5, 2),
                Some(LiteralValue::Number(5.))
            );
            e.set_cell_value("Sheet1", 4, 2, LiteralValue::Empty)
                .unwrap();
            assert_eq!(
                e.evaluate_all().unwrap().computed_vertices,
                1,
                "last blocker"
            );
            assert_eq!(
                e.get_cell_value("Sheet1", 5, 2),
                Some(LiteralValue::Number(5.))
            );
            e.set_cell_value("Aux", 20, 20, LiteralValue::Number(1.))
                .unwrap();
            assert_eq!(
                e.evaluate_all().unwrap().computed_vertices,
                0,
                "unrelated edit"
            );
        }
    }
}

#[test]
fn logged_engine_child_literal_survives_shrink_and_clear() {
    for family in [false, true] {
        for clear in [false, true] {
            let mut e = make(family, false);
            e.evaluate_all().unwrap();
            let cell = e.graph.make_cell_ref("Sheet1", 4, 2);
            let mut log = crate::engine::ChangeLog::new();
            log.set_enabled(true);
            e.edit_with_logger(&mut log, |editor| {
                editor.set_cell_value(cell, LiteralValue::Number(99.));
            })
            .unwrap();
            if clear {
                e.set_cell_value("Sheet1", 1, 2, LiteralValue::Empty)
                    .unwrap();
            } else {
                e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.))
                    .unwrap();
            }
            e.evaluate_all().unwrap();
            assert_eq!(
                e.get_cell_value("Sheet1", 4, 2),
                Some(LiteralValue::Number(99.))
            );
        }
    }
}
