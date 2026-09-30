//! Range spill admission must precede decoding, but never constrain reductions.
// Run the ignored original 4M-cell fixture directly under /usr/bin/time -v;
// RSS is observational evidence, deliberately not a native unit assertion.
use crate::engine::{CancelToken, EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use crate::traits::RANGE_MATERIALIZED_CELLS;
use formualizer_common::{ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn formula(e: &mut Engine<TestWorkbook>, row: u32, col: u32, f: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(f).unwrap())
        .unwrap();
}

fn run(e: &mut Engine<TestWorkbook>, mode: usize) {
    match mode {
        0 => {
            e.evaluate_all().unwrap();
        }
        1 => {
            e.evaluate_all_cancellable(CancelToken::new()).unwrap();
        }
        2 => {
            e.evaluate_all_with_delta().unwrap();
        }
        3 => {
            e.evaluate_cells(&[("Sheet1", 1, 1)]).unwrap();
        }
        4 => {
            e.evaluate_cells_cancellable(&[("Sheet1", 1, 1)], CancelToken::new())
                .unwrap();
        }
        5 => {
            e.evaluate_all_logged(&mut crate::engine::ChangeLog::new())
                .unwrap();
        }
        6 => {
            let cell = e.graph.make_cell_ref("Sheet1", 0, 0);
            let vertex = e.graph.get_vertex_for_cell(&cell).unwrap();
            let result = e.evaluate_vertex(vertex).unwrap();
            if let LiteralValue::Error(error) = result {
                assert_eq!(error.message.as_deref(), Some("SpillTooLarge"));
                assert!(matches!(error.extra, ExcelErrorExtra::Spill { .. }));
            }
        }
        7 => {
            e.evaluate_cells_with_delta(&[("Sheet1", 1, 1)]).unwrap();
        }
        8 => {
            e.evaluate_until(&[("Sheet1", 1, 1)]).unwrap();
        }
        9 => {
            e.evaluate_until_cancellable(&["Sheet1!A1"], CancelToken::new())
                .unwrap();
        }
        _ => unreachable!(),
    }
}

fn engine(parallel: bool) -> Engine<TestWorkbook> {
    let mut config = EvalConfig {
        enable_parallel: parallel,
        ..Default::default()
    };
    config.spill.max_spill_cells = 3;
    Engine::new(TestWorkbook::new(), config)
}

fn assert_cap(e: &Engine<TestWorkbook>, h: u32, w: u32) {
    let Some(LiteralValue::Error(err)) = e.get_cell_value("Sheet1", 1, 1) else {
        panic!("expected spill error")
    };
    assert_eq!(err.kind, ExcelErrorKind::Spill);
    // Arrow's public error lane retains the kind, not diagnostic payloads.
    // The publication helper's unit test pins the complete pre-storage error.
    if err.extra != ExcelErrorExtra::None {
        assert_eq!(err.message.as_deref(), Some("SpillTooLarge"));
        assert_eq!(
            err.extra,
            ExcelErrorExtra::Spill {
                expected_rows: h,
                expected_cols: w
            }
        );
    }
}

#[test]
fn range_spill_admission_precedes_materialization() {
    for mode in 0..10 {
        let mut e = engine(false);
        // A blocker must not take precedence over the size cap.
        e.set_cell_value("Sheet1", 2, 1, LiteralValue::Number(9.0))
            .unwrap();
        formula(&mut e, 1, 1, "=B1:C2");
        RANGE_MATERIALIZED_CELLS.with(|c| c.set(0));
        run(&mut e, mode);
        assert_cap(&e, 2, 2);
        assert_eq!(
            RANGE_MATERIALIZED_CELLS.with(|c| c.get()),
            0,
            "entry {mode} decoded an inadmissible range"
        );
        assert_eq!(
            e.get_cell_value("Sheet1", 2, 1),
            Some(LiteralValue::Number(9.0))
        );
    }
}

#[test]
fn range_spill_admission_boundary_and_retry() {
    for parallel in [false, true] {
        for mode in 0..10 {
            let mut e = engine(parallel);
            for r in 1..=4 {
                e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(r as f64))
                    .unwrap();
            }
            formula(&mut e, 1, 1, "=B1:B3");
            run(&mut e, mode);
            assert_eq!(
                e.get_cell_value("Sheet1", 3, 1),
                Some(LiteralValue::Number(3.0))
            );
            formula(&mut e, 1, 1, "=B1:B4");
            run(&mut e, mode);
            assert_cap(&e, 4, 1);
            assert!(matches!(
                e.get_cell_value("Sheet1", 3, 1),
                None | Some(LiteralValue::Empty)
            ));
            // Limit changes alone do not promise dirtying; explicitly mark the anchor.
            e.config.spill.max_spill_cells = 4;
            formula(&mut e, 1, 1, "=B1:B4");
            run(&mut e, mode);
            assert_eq!(
                e.get_cell_value("Sheet1", 4, 1),
                Some(LiteralValue::Number(4.0))
            );
            e.config.spill.max_spill_cells = 3;
            formula(&mut e, 1, 1, "=B1:B2");
            run(&mut e, mode);
            assert_eq!(
                e.get_cell_value("Sheet1", 2, 1),
                Some(LiteralValue::Number(2.0))
            );
            assert!(matches!(
                e.get_cell_value("Sheet1", 4, 1),
                None | Some(LiteralValue::Empty)
            ));
        }
    }
}

#[test]
fn range_spill_admission_clears_dependent_readers_and_retries_dynamic_ranges() {
    for parallel in [false, true] {
        for mode in [0, 1, 2, 5] {
            let mut e = engine(parallel);
            for r in 1..=4 {
                e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(r as f64))
                    .unwrap();
            }
            // A dynamic range exercises the freshness recorder as well as publication.
            e.set_cell_value("Sheet1", 1, 5, LiteralValue::Number(3.0))
                .unwrap();
            formula(&mut e, 1, 4, "=SUM(A2:A4)");
            formula(&mut e, 1, 1, "=INDIRECT(\"B1:B\"&E1)");
            run(&mut e, mode);
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 4),
                Some(LiteralValue::Number(5.0))
            );
            e.set_cell_value("Sheet1", 1, 5, LiteralValue::Number(4.0))
                .unwrap();
            run(&mut e, mode);
            assert_cap(&e, 4, 1);
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 4),
                Some(LiteralValue::Number(0.0))
            );
            e.set_cell_value("Sheet1", 1, 5, LiteralValue::Number(2.0))
                .unwrap();
            run(&mut e, mode);
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 4),
                Some(LiteralValue::Number(2.0))
            );
        }
    }
}

#[test]
fn range_spill_admission_cancel_and_dirty_dynamic_sources_remain_retryable() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        e.set_cell_value("Sheet1", 1, 6, LiteralValue::Number(10.0))
            .unwrap();
        // Anchor inserted first; INDIRECT's range has no static source edges.
        formula(&mut e, 1, 1, "=INDIRECT(\"B1:B4\")");
        for r in 1..=4 {
            formula(&mut e, r, 2, &format!("=F1+{r}"));
        }
        let token = CancelToken::new();
        token.cancel();
        let err = e.evaluate_all_cancellable(token).unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Cancelled);
        e.evaluate_all_cancellable(CancelToken::new()).unwrap();
        assert_cap(&e, 4, 1);
        e.set_cell_value("Sheet1", 1, 6, LiteralValue::Number(20.0))
            .unwrap();
        formula(&mut e, 1, 1, "=INDIRECT(\"B1:B3\")");
        e.evaluate_all().unwrap();
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(21.0))
        );
        assert_eq!(
            e.get_cell_value("Sheet1", 3, 1),
            Some(LiteralValue::Number(23.0))
        );
        assert!(
            e.get_cell("Sheet1", 1, 1).unwrap().0.is_some(),
            "anchor formula was replaced"
        );
    }
}

#[test]
fn range_spill_admission_family_results_do_not_decode() {
    let mut e = engine(false);
    for row in 1..=4 {
        formula(&mut e, row, 1, "=$B$1:$C$2");
    }
    RANGE_MATERIALIZED_CELLS.with(|c| c.set(0));
    e.evaluate_all().unwrap();
    assert_cap(&e, 2, 2);
    assert_eq!(RANGE_MATERIALIZED_CELLS.with(|c| c.get()), 0);
}

#[test]
#[ignore = "standalone 4M-cell RSS probe; run under /usr/bin/time -v"]
fn range_spill_admission_original_4m_probe() {
    let mut e = engine(false);
    e.config.spill.max_spill_cells = 10_000;
    e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(1.0))
        .unwrap();
    formula(&mut e, 1, 1, "=B1:K400000");
    let start = std::time::Instant::now();
    e.evaluate_all().unwrap();
    assert_cap(&e, 400_000, 10);
    eprintln!("4M direct range publication elapsed: {:?}", start.elapsed());
}

#[test]
fn range_spill_admission_does_not_cap_intermediate_reductions() {
    let mut e = engine(false);
    for r in 1..=4 {
        e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(r as f64))
            .unwrap();
    }
    formula(&mut e, 1, 1, "=SUM(B1:B4)");
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
}
