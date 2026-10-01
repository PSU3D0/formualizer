use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[test]
fn copied_fixed_arrays_per_cell_sequential_and_parallel() {
    for parallel in [false, true] {
        for width in [1, 3] {
            let mut e = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    family_execution: false,
                    enable_parallel: parallel,
                    ..EvalConfig::default()
                },
            );
            for row in 1..=64 {
                e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(2.0))
                    .unwrap();
                e.set_cell_formula(
                    "Sheet1",
                    row,
                    2,
                    parse(&format!("=SEQUENCE(1,3,A{row})")).unwrap(),
                )
                .unwrap();
            }
            for row in 1..=64 {
                e.declare_fixed_array_formula("Sheet1", row, 2, 1, width)
                    .unwrap();
            }
            e.set_cell_formula("Sheet1", 1, 6, parse("=SUM(B1:D64)").unwrap())
                .unwrap();
            e.evaluate_all().unwrap();
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 6),
                Some(LiteralValue::Number(if width == 1 { 128.0 } else { 576.0 }))
            );
            assert_eq!(
                e.graph.spill_registry_counts().0,
                if width == 1 { 0 } else { 64 }
            );
            e.set_cell_value("Sheet1", 32, 1, LiteralValue::Number(5.0))
                .unwrap();
            e.evaluate_all().unwrap();
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 6),
                Some(LiteralValue::Number(if width == 1 { 131.0 } else { 585.0 }))
            );
            assert_eq!(e.memo_hits_for_test(), 0);
            assert_eq!(e.chained_members_for_test(), 0);
        }
    }
}

#[test]
fn single_cell_fixed_array_takes_top_left_without_spill() {
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: false,
            ..EvalConfig::default()
        },
    );
    e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3)").unwrap())
        .unwrap();
    e.declare_fixed_array_formula("Sheet1", 1, 1, 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(e.graph.spill_registry_counts(), (0, 0));
    let vertex = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 1))
        .unwrap();
    assert!(!e.graph.has_spill_anchors());
    assert!(!e.graph.is_spill_anchor(vertex));
    assert!(
        e.inspect_cell_result(&formualizer_common::CellAddress::new("Sheet1", 1, 1).unwrap())
            .unwrap()
            .spill
            .is_none()
    );
}

#[test]
fn fixed_range_results_fit_before_range_admission() {
    for rows in [1, 3] {
        let mut config = EvalConfig {
            family_execution: false,
            ..EvalConfig::default()
        };
        config.spill.max_spill_cells = rows;
        let mut e = Engine::new(TestWorkbook::new(), config);
        e.set_cell_value("Sheet1", 1, 3, LiteralValue::Number(7.0))
            .unwrap();
        e.set_cell_formula("Sheet1", 1, 2, parse("=C1:C100").unwrap())
            .unwrap();
        e.declare_fixed_array_formula("Sheet1", 1, 2, rows, 1)
            .unwrap();
        let vertex = e
            .graph
            .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 2))
            .unwrap();
        e.evaluate_vertex(vertex).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 2),
            Some(LiteralValue::Number(7.0))
        );
        if rows > 1 {
            assert_eq!(
                e.get_cell_value("Sheet1", 3, 2),
                Some(LiteralValue::Number(0.0))
            );
        }
    }
}

#[test]
fn fixed_arrays_fit_before_cap_block_dynamic_and_reject_spill_operator() {
    let mut config = EvalConfig {
        family_execution: false,
        ..EvalConfig::default()
    };
    config.spill.max_spill_cells = 3;
    let mut e = Engine::new(TestWorkbook::new(), config);
    e.set_cell_formula("Sheet1", 1, 2, parse("=SEQUENCE(100)").unwrap())
        .unwrap();
    e.declare_fixed_array_formula("Sheet1", 1, 2, 3, 1).unwrap();
    e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(1,3)").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 5, parse("=SUM(B1#)").unwrap())
        .unwrap();
    e.set_cell_formula(
        "Sheet1",
        2,
        5,
        parse("=SUM(_xlfn.ANCHORARRAY(B1))").unwrap(),
    )
    .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(3.0))
    );
    assert!(
        matches!(e.get_cell_value("Sheet1", 1, 1), Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Spill)
    );
    for row in 1..=2 {
        assert!(
            matches!(e.get_cell_value("Sheet1", row, 5), Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Ref)
        );
    }
}

#[test]
fn fixed_declaration_cancellation_retry_and_move_cleanup() {
    use crate::engine::CancelToken;
    for rows in [1, 3] {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: false,
                ..EvalConfig::default()
            },
        );
        e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3)").unwrap())
            .unwrap();
        e.declare_fixed_array_formula("Sheet1", 1, 1, rows, 1)
            .unwrap();
        let token = CancelToken::new();
        token.cancel();
        assert_eq!(
            e.evaluate_all_cancellable(token).unwrap_err().kind,
            ExcelErrorKind::Cancelled
        );
        e.evaluate_all().unwrap();
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(1.0))
        );
        e.insert_rows("Sheet1", 1, 1).unwrap();
        assert!(e.graph.fixed_single_arrays.is_empty());
        assert!(e.graph.fixed_array_shapes.is_empty());
    }
}

#[test]
fn error_fill_and_empty_members_remain_owned() {
    for expression in ["=1/0", "=D1:E1"] {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: false,
                ..EvalConfig::default()
            },
        );
        e.set_cell_formula("Sheet1", 1, 2, parse(expression).unwrap())
            .unwrap();
        e.declare_fixed_array_formula("Sheet1", 1, 2, 1, 2).unwrap();
        e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(1,3)").unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        assert!(
            matches!(e.get_cell_value("Sheet1", 1, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Spill)
        );
        let child = e.get_cell_value("Sheet1", 1, 3).unwrap();
        if expression == "=1/0" {
            assert!(matches!(child, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Div));
        } else {
            assert_eq!(child, LiteralValue::Number(0.0));
        }
    }
}

#[test]
fn declaration_clears_on_formula_replacement() {
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: false,
            ..EvalConfig::default()
        },
    );
    e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3)").unwrap())
        .unwrap();
    e.declare_fixed_array_formula("Sheet1", 1, 1, 1, 1).unwrap();
    e.evaluate_all().unwrap();
    e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.graph.spill_registry_counts().0, 1);
}

#[test]
fn fixed_extent_fits_and_readers_refresh() {
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: false,
            ..EvalConfig::default()
        },
    );
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=SEQUENCE(A1)").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 5, parse("=SUM(B1:B3)").unwrap())
        .unwrap();
    e.declare_fixed_array_formula("Sheet1", 1, 2, 3, 1).unwrap();
    e.evaluate_all().unwrap();
    assert!(
        matches!(e.get_cell_value("Sheet1", 3, 2), Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Na)
    );
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(e.get_cell_value("Sheet1", 4, 2), None);
}
