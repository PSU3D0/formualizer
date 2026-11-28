//! Limits enforced while a single function runs (#619, #254, #621, #620).
//!
//! A request's deadline, `max_work_units` budget and cancellation token used
//! to be checked only between vertices, so one long-running builtin held the
//! worker for its whole run. Functions now poll one request-scoped token that
//! reports all three; the engine refuses the interrupted result and fails the
//! request with the real reason.

use std::time::{Duration, Instant};

use crate::engine::range_view::RangeView;
use crate::engine::{
    CancelToken, DateSystem, DeadlineResourceBudget, Engine, EvalConfig, EvaluationBudgets,
    WorkResourceBudget,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{
    ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue, ResourceExhaustionReason,
};
use formualizer_parse::parser::parse;

/// Formulas that run for many seconds unbudgeted (minutes in a debug build):
/// each does `22,500 x 22,500` element work inside one call.
const LONG_RUNNING: &[&str] = &[
    "=SUMIF(SEQUENCE(150,150),SEQUENCE(150,150))",
    "=COUNTIFS(SEQUENCE(150,150),SEQUENCE(150,150))",
    "=MATCH(SEQUENCE(150,150),SEQUENCE(150,150))",
    "=MATCH(SEQUENCE(150,150),SEQUENCE(150,150),0)",
    "=VLOOKUP(SEQUENCE(150,150),SEQUENCE(150,150),1)",
];

/// Far below any of [`LONG_RUNNING`]'s unbudgeted run times, far above the
/// time a stopped one needs to unwind, on a loaded debug build.
const PROMPT: Duration = Duration::from_secs(20);

fn engine_with(budgets: EvaluationBudgets, formula: &str) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_evaluation_budgets(budgets),
    );
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
        .unwrap();
    engine
}

fn eval_a1(formula: &str) -> LiteralValue {
    let mut engine = engine_with(EvaluationBudgets::default(), formula);
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 1)
        .unwrap_or(LiteralValue::Empty)
}

fn deadline(limit: Duration) -> EvaluationBudgets {
    EvaluationBudgets {
        deadline: DeadlineResourceBudget {
            max_elapsed: Some(limit),
        },
        ..EvaluationBudgets::default()
    }
}

fn work(units: u64) -> EvaluationBudgets {
    EvaluationBudgets {
        work: WorkResourceBudget {
            max_work_units: Some(units),
        },
        ..EvaluationBudgets::default()
    }
}

fn resource_reason(error: &ExcelError) -> Option<ResourceExhaustionReason> {
    match error.extra() {
        ExcelErrorExtra::Resource { detail } => Some(detail.reason),
        _ => None,
    }
}

fn is_error(value: &LiteralValue, kind: ExcelErrorKind) -> bool {
    matches!(value, LiteralValue::Error(e) if e.kind == kind)
}

#[test]
fn deadline_stops_a_long_running_builtin_and_reports_the_deadline() {
    for formula in LONG_RUNNING {
        let mut engine = engine_with(deadline(Duration::from_millis(150)), formula);
        let started = Instant::now();
        let error = engine
            .evaluate_all()
            .expect_err("a request past its deadline fails");
        let elapsed = started.elapsed();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::Deadline),
            "{formula}: {error:?}"
        );
        assert!(
            elapsed < PROMPT,
            "{formula} ran {elapsed:?} past a 150 ms deadline"
        );
        // The interrupted result is not a result: nothing was committed.
        let value = engine.get_cell_value("Sheet1", 1, 1);
        assert!(
            !matches!(&value, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Cancelled),
            "{formula}: #CANCELLED must not be committed, got {value:?}"
        );
    }
}

#[test]
fn work_budget_stops_a_long_running_builtin_and_reports_work_units() {
    for formula in LONG_RUNNING {
        let mut engine = engine_with(work(1_000_000), formula);
        let started = Instant::now();
        let error = engine
            .evaluate_all()
            .expect_err("a request over its work budget fails");
        let elapsed = started.elapsed();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::WorkUnits),
            "{formula}: {error:?}"
        );
        assert!(
            elapsed < PROMPT,
            "{formula} ran {elapsed:?} on a 1M-unit budget"
        );
    }
}

#[test]
fn cancel_token_stops_array_lookups_mid_function() {
    for formula in [
        "=MATCH(SEQUENCE(150,150),SEQUENCE(150,150))",
        "=VLOOKUP(SEQUENCE(150,150),SEQUENCE(150,150),1)",
    ] {
        let mut engine = engine_with(EvaluationBudgets::default(), formula);
        let token = CancelToken::new();
        let remote = token.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            remote.cancel();
        });
        let started = Instant::now();
        let error = engine
            .evaluate_all_cancellable(token)
            .expect_err("a cancelled request fails");
        let elapsed = started.elapsed();
        canceller.join().unwrap();
        assert_eq!(error.kind, ExcelErrorKind::Cancelled, "{formula}");
        assert!(
            elapsed < PROMPT,
            "{formula} ran {elapsed:?} after cancellation"
        );
    }
}

#[test]
fn an_error_handler_cannot_swallow_an_in_function_stop() {
    // IFERROR turns the stopped MATCH's #CANCELLED into 0; that 0 was computed
    // across the stop and must not be reported as the request's result.
    let formula = "=IFERROR(MATCH(SEQUENCE(150,150),SEQUENCE(150,150)),0)";
    let mut engine = engine_with(deadline(Duration::from_millis(150)), formula);
    let error = engine.evaluate_all().expect_err("deadline, not Ok(0)");
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::Deadline)
    );
    assert_ne!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(0.0))
    );

    let mut engine = engine_with(work(1_000_000), formula);
    let error = engine.evaluate_all().expect_err("work budget, not Ok(0)");
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::WorkUnits)
    );
}

#[test]
fn a_stopped_request_leaves_the_engine_usable_and_the_next_request_clean() {
    let mut engine = engine_with(
        work(1_000_000),
        "=MATCH(SEQUENCE(150,150),SEQUENCE(150,150))",
    );
    assert!(engine.evaluate_all().is_err());
    // The trip is per request: a cheap formula in the same engine evaluates.
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            parse("=SUM(MATCH({2,3},{1,2,3},0))").unwrap(),
        )
        .unwrap();
    engine
        .evaluate_all()
        .expect("the next request starts clean");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn budgets_that_are_not_reached_do_not_change_results() {
    for (formula, expected) in [
        ("=SUM(SUMIF(SEQUENCE(3,3),SEQUENCE(3,3)))", 45.0),
        ("=SUM(COUNTIFS(SEQUENCE(3,3),SEQUENCE(3,3)))", 9.0),
        ("=SUM(MATCH({2,3},{1,2,3},0))", 5.0),
        ("=SUM(VLOOKUP({1,3},{1,10;2,20;3,30},2))", 40.0),
    ] {
        let unbudgeted = eval_a1(formula);
        assert_eq!(unbudgeted, LiteralValue::Number(expected), "{formula}");
        let budgets = EvaluationBudgets {
            deadline: DeadlineResourceBudget {
                max_elapsed: Some(Duration::from_secs(600)),
            },
            work: WorkResourceBudget {
                max_work_units: Some(10_000),
            },
            ..EvaluationBudgets::default()
        };
        let mut engine = engine_with(budgets, formula);
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(unbudgeted),
            "{formula}"
        );
    }
}

// ---- DDB (#619) ----

fn number(value: LiteralValue) -> f64 {
    match value {
        LiteralValue::Number(n) => n,
        other => panic!("expected a number, got {other:?}"),
    }
}

#[test]
fn ddb_short_schedules_are_unchanged() {
    assert_eq!(number(eval_a1("=DDB(2400,300,10,1)")), 480.0);
    assert_eq!(number(eval_a1("=DDB(2400,300,10,2)")), 384.0);
    assert!((number(eval_a1("=DDB(2400,300,10,10)")) - 22.1225472).abs() < 1e-9);
    // factor > life: everything above salvage goes in period 1, nothing after.
    assert_eq!(number(eval_a1("=DDB(1000,100,2,1,3)")), 900.0);
    assert_eq!(number(eval_a1("=DDB(1000,100,2,2,3)")), 0.0);
}

#[test]
fn ddb_long_schedules_are_computed_without_iterating_every_period() {
    let started = Instant::now();
    let huge = number(eval_a1("=DDB(1000,0,1E10,1E10)"));
    assert!(started.elapsed() < PROMPT);
    // 1000 * (1 - 2e-10)^(1e10 - 1) * 2e-10 = 2e-7 * e^-2
    let expected = 2.0e-7 * (-2.0f64).exp();
    assert!(((huge - expected) / expected).abs() < 1e-9, "{huge}");

    // A period past i32::MAX is that period, not a saturated 2,147,483,647.
    let beyond = number(eval_a1("=DDB(1000,0,1E10,3E9)"));
    let saturated = number(eval_a1("=DDB(1000,0,1E10,2147483647)"));
    assert!(beyond < saturated, "{beyond} vs {saturated}");

    // The closed form continues the loop: just past the switch-over it agrees
    // with iterating, and it respects the salvage floor.
    let (cost, salvage, life, period) = (1.0e6f64, 10.0f64, 3.0e6f64, (1u32 << 20) + 1);
    let rate = 2.0 / life;
    let (mut value, mut expected) = (cost, 0.0);
    for _ in 1..=period {
        expected = value * rate;
        if value - expected < salvage {
            expected = (value - salvage).max(0.0);
        }
        value -= expected;
    }
    let closed = number(eval_a1("=DDB(1000000,10,3000000,1048577)"));
    assert!(
        ((closed - expected) / expected).abs() < 1e-9,
        "{closed} vs {expected}"
    );
    assert_eq!(number(eval_a1("=DDB(1000,999.9999,3000000,2999999)")), 0.0);
}

// ---- FREQUENCY (#619 catalog sweep) ----

#[test]
fn frequency_bins_by_search_with_unchanged_results() {
    // Unsorted bins, duplicates, values on a boundary, below and above.
    assert_eq!(
        eval_a1("=TEXTJOIN(\",\",TRUE,FREQUENCY({1,2,2,4,5,6,9,-3},{4,2,2}))"),
        LiteralValue::Text("4,0,1,3".into())
    );
    assert_eq!(
        eval_a1("=TEXTJOIN(\",\",TRUE,FREQUENCY({79,85,78,85,50,81,95,88,97},{70,79,89}))"),
        LiteralValue::Text("1,2,4,2".into())
    );
    // A million values into a million bins used to be 10^12 comparisons.
    let started = Instant::now();
    let total = eval_a1("=SUM(FREQUENCY(SEQUENCE(1000,1000),SEQUENCE(1000,1000)))");
    assert_eq!(total, LiteralValue::Number(1_000_000.0));
    assert!(started.elapsed() < PROMPT);
}

// ---- Wide arrays and LET (#621) ----

#[test]
fn an_array_wider_than_any_budget_is_num_before_it_is_stored() {
    // 1,048,576 columns: about 16 KB per column before the fix (17 GB).
    let started = Instant::now();
    assert!(is_error(&eval_a1("=TRANSPOSE(P:P)"), ExcelErrorKind::Num));
    assert!(is_error(
        &eval_a1("=TOROW(P1:P200000)"),
        ExcelErrorKind::Num
    ));
    assert!(is_error(
        &eval_a1("=SUM(TRANSPOSE(P:P))"),
        ExcelErrorKind::Num
    ));
    assert!(started.elapsed() < PROMPT);
    // An intermediate wider than the sheet but within budget still works.
    assert_eq!(
        eval_a1("=COLUMNS(TRANSPOSE(P1:P20000))"),
        LiteralValue::Number(20_000.0)
    );
}

#[test]
fn owned_views_reserve_by_row_count_and_refuse_unstorable_widths() {
    let wide = |cols: usize| vec![vec![LiteralValue::Number(1.0); cols]];
    let view = RangeView::try_from_owned_rows(wide(16_384), DateSystem::Excel1900, None).unwrap();
    assert_eq!(view.dims(), (1, 16_384));
    // One row per column is reserved, not a 1,024-row first chunk.
    assert_eq!(view.sheet().chunk_rows, 1);
    let error =
        RangeView::try_from_owned_rows(wide(131_073), DateSystem::Excel1900, None).unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Num);
    // The infallible constructor degrades to a 1x1 #NUM! instead of panicking.
    let view = RangeView::from_owned_rows(wide(131_073), DateSystem::Excel1900);
    assert_eq!(view.dims(), (1, 1));
    assert!(is_error(&view.get_cell(0, 0), ExcelErrorKind::Num));
    // Tall arrays are charged by cell count alone, as before.
    let tall = vec![vec![LiteralValue::Number(1.0)]; 200_000];
    assert!(RangeView::try_from_owned_rows(tall, DateSystem::Excel1900, None).is_ok());
}

#[test]
fn let_holds_a_bound_range_to_the_materialisation_budget() {
    // 25M cells is over the 2^24 budget: the name binds #NUM! instead of
    // copying 800 MB, and the error surfaces only where the copy is used.
    let started = Instant::now();
    assert_eq!(eval_a1("=LET(v,B1:Z1000000,0)"), LiteralValue::Number(0.0));
    assert!(is_error(
        &eval_a1("=LET(v,B1:Z1000000,v+0)"),
        ExcelErrorKind::Num
    ));
    // 4.7 billion cells: aborted the process before the fix.
    assert_eq!(
        eval_a1("=LET(v,G36:UHB327676,0)"),
        LiteralValue::Number(0.0)
    );
    assert!(started.elapsed() < PROMPT);
    // By-reference uses of the name never needed the copy.
    assert_eq!(
        eval_a1("=LET(v,B1:Z1000000,ROWS(v))"),
        LiteralValue::Number(1_000_000.0)
    );
    // Within budget nothing changes.
    assert_eq!(
        eval_a1("=LET(v,SEQUENCE(2,3),SUM(v)+ROWS(v))"),
        LiteralValue::Number(23.0)
    );
}

// ---- One-row spills (#620) ----

#[test]
fn a_one_row_spill_commits_in_linear_time_with_correct_values() {
    let mut engine = engine_with(EvaluationBudgets::default(), "=SEQUENCE(1,9947)");
    let started = Instant::now();
    engine.evaluate_all().unwrap();
    // Quadratic in the width before the fix: 14 s in a release build.
    assert!(started.elapsed() < PROMPT, "{:?}", started.elapsed());
    for col in [1u32, 2, 4_000, 9_947] {
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, col),
            Some(LiteralValue::Number(f64::from(col))),
            "column {col}"
        );
    }
    assert_eq!(engine.get_cell_value("Sheet1", 1, 9_948), None);
}

// ---- Row / column walks and literal conversion (#254) ----

fn grid(rows: usize, cols: usize) -> Vec<Vec<LiteralValue>> {
    (0..rows)
        .map(|r| {
            (0..cols)
                .map(|c| LiteralValue::Number((r * cols + c) as f64))
                .collect()
        })
        .collect()
}

fn view_with(rows: usize, cols: usize, token: &CancelToken) -> RangeView<'static> {
    RangeView::try_from_owned_rows(grid(rows, cols), DateSystem::Excel1900, None)
        .unwrap()
        .with_cancel_token(Some(token.clone()))
}

#[test]
fn row_and_column_walks_do_not_start_on_a_cancelled_token() {
    let token = CancelToken::new();
    let view = view_with(8, 8, &token);
    token.cancel();
    let mut visited = 0usize;
    let error = view
        .for_each_row(&mut |_| {
            visited += 1;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    let error = view
        .for_each_col(&mut |_| {
            visited += 1;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_eq!(visited, 0, "the token is polled before the first cell");
}

#[test]
fn row_walk_stops_within_one_poll_interval_of_cancellation() {
    // 10 columns: the token is polled every ceil(4096 / 10) = 410 rows.
    let token = CancelToken::new();
    let view = view_with(2_000, 10, &token);
    let mut rows_seen = 0usize;
    let error = view
        .for_each_row(&mut |row| {
            assert_eq!(row.len(), 10);
            rows_seen += 1;
            if rows_seen == 5 {
                token.cancel();
            }
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_eq!(
        rows_seen, 410,
        "cancelled at row 5, observed at the next poll"
    );
}

#[test]
fn column_walk_polls_inside_a_tall_column_and_skips_the_unfinished_one() {
    // 5,000-row columns: the second poll falls inside the first column, so a
    // token cancelled by another thread stops the walk mid-column. Here the
    // visitor cancels after column 1; column 2's buffering observes it.
    let token = CancelToken::new();
    let view = view_with(5_000, 4, &token);
    let mut cols_seen = 0usize;
    let error = view
        .for_each_col(&mut |col| {
            assert_eq!(col.len(), 5_000);
            cols_seen += 1;
            token.cancel();
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_eq!(
        cols_seen, 1,
        "the unfinished column is never handed to the visitor"
    );
}

#[test]
fn a_visitor_error_stops_the_walk_and_is_returned_unchanged() {
    let token = CancelToken::new();
    let view = view_with(10, 3, &token);
    let mut rows_seen = 0usize;
    let error = view
        .for_each_row(&mut |_| {
            rows_seen += 1;
            Err(ExcelError::new(ExcelErrorKind::Div))
        })
        .unwrap_err();
    assert_eq!((error.kind, rows_seen), (ExcelErrorKind::Div, 1));
}

#[test]
fn literal_conversion_reports_cancellation_as_an_error_value() {
    use crate::traits::CalcValue;
    let token = CancelToken::new();
    // Uncancelled: the copy is the array.
    let array = CalcValue::Range(view_with(3, 2, &token)).into_literal();
    assert_eq!(array, LiteralValue::Array(grid(3, 2)));
    // Cancelled: #CANCELLED, never a partial array and never silently Empty.
    let view = view_with(3, 2, &token);
    token.cancel();
    assert!(is_error(
        &CalcValue::Range(view).into_literal(),
        ExcelErrorKind::Cancelled
    ));
    // A single cell is a read, not a walk: it has no checkpoint.
    let cell = CalcValue::Range(view_with(1, 1, &token)).into_literal();
    assert_eq!(cell, LiteralValue::Number(0.0));
}

// ---- Review regressions ----

#[test]
fn a_lowered_array_budget_applies_to_the_shape_being_built() {
    use crate::engine::ScratchResourceBudget;
    let budgets = EvaluationBudgets {
        scratch: ScratchResourceBudget {
            materialized_array_cells: Some(20_000),
            ..ScratchResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    };
    let eval = |formula: &str| {
        let mut engine = engine_with(budgets.clone(), formula);
        engine.evaluate_all().unwrap();
        engine
            .get_cell_value("Sheet1", 1, 1)
            .unwrap_or(LiteralValue::Empty)
    };
    // 1 x 20,000 is wider than a sheet, so it is charged 128 rows per column:
    // far over a 20,000-cell budget, although it is only 20,000 cells.
    assert!(is_error(
        &eval("=COLUMNS(TRANSPOSE(B1:B20000))"),
        ExcelErrorKind::Num
    ));
    // Up to sheet width the charge is the cell count, as before.
    assert_eq!(
        eval("=COLUMNS(TRANSPOSE(B1:B16384))"),
        LiteralValue::Number(16_384.0)
    );
}

#[test]
fn a_refused_wide_array_is_an_error_to_shape_functions() {
    for formula in [
        "=COLUMNS(TRANSPOSE(B1:B131073))",
        "=ROWS(TRANSPOSE(B1:B131073))",
        "=COLUMNS(TOROW(B1:B200000))",
    ] {
        assert!(
            is_error(&eval_a1(formula), ExcelErrorKind::Num),
            "{formula}"
        );
    }
    assert_eq!(
        eval_a1("=COLUMNS(TRANSPOSE(B1:B131072))"),
        LiteralValue::Number(131_072.0)
    );
    assert!(is_error(&eval_a1("=ROWS(NA())"), ExcelErrorKind::Na));
}

#[test]
fn a_formula_name_does_not_cache_a_value_computed_across_a_stop() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut engine = engine_with(work(100), "=Expensive+1");
    let ast = parse("=IFERROR(SUM(MATCH(SEQUENCE(10),SEQUENCE(100))),987)").unwrap();
    engine
        .define_name(
            "Expensive",
            NamedDefinition::Formula {
                ast,
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();
    let error = engine.evaluate_all().expect_err("work budget");
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::WorkUnits)
    );
    let cached = engine.get_cell_value("Sheet1", 1, 1);
    assert!(
        !matches!(&cached, Some(LiteralValue::Number(n)) if *n == 988.0),
        "IFERROR's fallback over a stopped MATCH was published: {cached:?}"
    );
    assert!(
        !matches!(&cached, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Cancelled),
        "#CANCELLED was published: {cached:?}"
    );
}

#[test]
fn a_token_kept_from_a_finished_request_does_not_affect_the_next() {
    // Each request has its own limits: tripping the first leaves the engine's
    // next request with a clean, independent mirror.
    let mut engine = engine_with(
        work(1_000_000),
        "=MATCH(SEQUENCE(150,150),SEQUENCE(150,150))",
    );
    assert!(engine.evaluate_all().is_err());
    let stale = std::sync::Arc::clone(&engine.request_limits);
    assert!(stale.tripped().is_some());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(!std::sync::Arc::ptr_eq(&stale, &engine.request_limits));
    assert!(engine.request_limits.tripped().is_none());
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
}
