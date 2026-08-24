use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use chrono::NaiveDate;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn runtime_engine() -> Engine<TestWorkbook> {
    let cycle = CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Error,
    };
    Engine::new(
        TestWorkbook::new(),
        EvalConfig::default()
            .with_cycle(cycle)
            .with_virtual_dep_telemetry(true),
    )
}

fn set_formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, formula: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(formula).expect("parse formula"))
        .expect("set formula");
}

fn is_circ(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> bool {
    matches!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Circ
    )
}

fn build_guarded_chain(consumer_formula: &str) -> Engine<TestWorkbook> {
    let mut engine = runtime_engine();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Int(i64::from(row)))
            .expect("set row number");
        set_formula(&mut engine, row, 5, &format!("=IF(B{row}>=29,$C$9,0)"));
        if row == 1 {
            set_formula(&mut engine, row, 17, "=E1");
        } else {
            set_formula(&mut engine, row, 17, &format!("=Q{}+E{row}", row - 1));
        }
    }
    set_formula(&mut engine, 9, 3, consumer_formula);
    engine
}

#[test]
fn index_rect_edge_precision_acyclic_chain() {
    let mut engine = build_guarded_chain("=INDEX(Q1:Q100,24)");
    engine.evaluate_all().expect("evaluate");

    let c9 = engine.get_cell_value("Sheet1", 9, 3);
    let c9_is_zero = matches!(c9, Some(LiteralValue::Number(0.0) | LiteralValue::Int(0)));
    let circ_count = (1..=100)
        .flat_map(|row| (1..=17).map(move |col| (row, col)))
        .filter(|&(row, col)| is_circ(&engine, row, col))
        .count();
    assert!(
        c9_is_zero && circ_count == 0,
        "C9 expected numeric zero, got {c9:?}; expected zero Circ cells, got {circ_count}"
    );
}

#[test]
fn index_rect_edge_precision_if_selected_base() {
    let mut engine = build_guarded_chain("=INDEX(IF(B1=1,Q1:Q100,A1:A100),24,1)");
    engine.evaluate_all().expect("evaluate");

    let c9 = engine.get_cell_value("Sheet1", 9, 3);
    let c9_is_zero = matches!(c9, Some(LiteralValue::Number(0.0) | LiteralValue::Int(0)));
    let circ_count = (1..=100)
        .flat_map(|row| (1..=17).map(move |col| (row, col)))
        .filter(|&(row, col)| is_circ(&engine, row, col))
        .count();
    assert!(
        c9_is_zero && circ_count == 0,
        "C9 through IF-selected reference expected numeric zero, got {c9:?}; expected zero Circ cells, got {circ_count}"
    );
}

#[test]
fn index_rect_edge_precision_omitted_column_single_vector() {
    let mut engine = build_guarded_chain("=INDEX(Q1:Q100,24,)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Number(0.0) | LiteralValue::Int(0))
        ),
        "vertical vector with omitted column must resolve one cell"
    );
    assert_eq!(
        (1..=100)
            .flat_map(|row| (1..=17).map(move |col| (row, col)))
            .filter(|&(row, col)| is_circ(&engine, row, col))
            .count(),
        0
    );
}

#[test]
fn index_rect_edge_precision_omitted_row_single_vector() {
    let mut engine = runtime_engine();
    engine
        .set_cell_value("Sheet1", 1, 17, LiteralValue::Int(0))
        .expect("set Q1");
    engine
        .set_cell_value("Sheet1", 1, 18, LiteralValue::Int(0))
        .expect("set R1");
    set_formula(&mut engine, 1, 19, "=$C$9");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:S1,,2)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Number(0.0) | LiteralValue::Int(0))
        ),
        "horizontal vector with omitted row must resolve one cell"
    );
    assert!(!is_circ(&engine, 1, 19));
}

#[test]
fn index_rect_edge_genuine_cycle_still_detected() {
    let mut engine = build_guarded_chain("=INDEX(Q1:Q100,40)");
    engine.evaluate_all().expect("evaluate");
    assert!(is_circ(&engine, 9, 3), "C9 must remain Circ");
}

#[test]
fn index_selected_error_propagates() {
    let mut engine = runtime_engine();
    set_formula(&mut engine, 1, 17, "=1/0");
    engine
        .set_cell_value("Sheet1", 3, 17, LiteralValue::Int(9))
        .expect("set Q3");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:Q3,1)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Div
        ),
        "INDEX must propagate the selected cell's DIV/0 error"
    );
}

#[test]
fn index_unselected_error_is_ignored() {
    let mut engine = runtime_engine();
    engine
        .set_cell_value("Sheet1", 1, 17, LiteralValue::Int(7))
        .expect("set Q1");
    set_formula(&mut engine, 3, 17, "=NA()");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:Q3,1)+1");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Number(8.0) | LiteralValue::Int(8))
        ),
        "an unselected error must not affect the INDEX result"
    );
}

#[test]
fn index_selected_error_phantom_cycle_stays_acyclic() {
    let mut engine = runtime_engine();
    set_formula(&mut engine, 1, 17, "=1/0");
    set_formula(&mut engine, 3, 17, "=C9");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:Q3,1)");
    engine.evaluate_all().expect("evaluate");
    for (row, col, label) in [(9, 3, "C9"), (3, 17, "Q3")] {
        assert!(
            matches!(
                engine.get_cell_value("Sheet1", row, col),
                Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Div
            ),
            "{label} must propagate DIV/0 without a circular error"
        );
    }
}

/// Test-only INDEX stand-in with a non-None format policy. The precise path
/// must route its result through `apply_format_propagation`; with the real
/// IndexFn the policy is `None`, which makes the application unobservable, so
/// this marker function is what makes deleting that call falsifiable.
#[derive(Debug)]
struct MarkedIndexFn;

impl crate::function::Function for MarkedIndexFn {
    fn name(&self) -> &'static str {
        "INDEX.MARKED"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        &[]
    }

    fn propagate_format(
        &self,
        _result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        Some(MARKER_FORMAT)
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            formualizer_common::ExcelError::new(ExcelErrorKind::NImpl),
        )))
    }
}

const MARKER_FORMAT: crate::format::FormatId = crate::format::FormatId(30);

type PreciseDispatchResult = Option<(Option<crate::format::FormatId>, LiteralValue)>;

fn precise_dispatch_on(
    engine: &Engine<TestWorkbook>,
    function: &dyn crate::function::Function,
    formula: &str,
) -> PreciseDispatchResult {
    use formualizer_parse::parser::ASTNodeType;

    let interpreter = crate::interpreter::Interpreter::new(engine, "Sheet1");
    let ast = parse(formula).expect("valid INDEX formula");
    let ASTNodeType::Function { args, .. } = &ast.node_type else {
        panic!("expected a function call: {formula}");
    };
    let handles: Vec<crate::traits::ArgumentHandle<'_, '_>> = args
        .iter()
        .map(|arg| crate::traits::ArgumentHandle::new(arg, &interpreter))
        .collect();
    let ctx = interpreter.function_context(None);
    crate::builtins::reference_fns::IndexFn::precise_dispatch(function, &handles, &ctx)
        .map(|value| (value.format_id(), value.into_literal()))
}

#[test]
fn index_precise_path_applies_format_policy() {
    let mut engine = runtime_engine();
    engine
        .set_cell_value("Sheet1", 1, 17, LiteralValue::Int(7))
        .expect("set Q1");

    // Marker policy: the precise path must apply the dispatching function's
    // format policy to the materialized value.
    let (marked_format, _) = precise_dispatch_on(&engine, &MarkedIndexFn, "=INDEX(Q1:Q3,1)")
        .expect("precise path taken");
    assert_eq!(
        marked_format,
        Some(MARKER_FORMAT),
        "the precise path must route through apply_format_propagation"
    );

    // Control: INDEX itself declares no policy, so the same dispatch clears
    // any annotation.
    engine
        .set_cell_value(
            "Sheet1",
            2,
            17,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).expect("valid date")),
        )
        .expect("set Q2");
    let (unmarked_format, _) = precise_dispatch_on(
        &engine,
        &crate::builtins::reference_fns::IndexFn,
        "=INDEX(Q1:Q3,2)",
    )
    .expect("precise path taken");
    assert_eq!(unmarked_format, None, "INDEX drops source annotations");
}

/// Path-taken matrix for `precise_single_cell_selection`: the filter is a
/// perf gate whose rejections are re-rejected downstream, so only direct
/// taken/not-taken assertions can catch an off-by-one in its bounds.
#[test]
fn index_precise_path_taken_matrix() {
    let mut engine = runtime_engine();
    for (row, col, value) in [
        (1, 1, 1),
        (2, 1, 2),
        (3, 1, 3),
        (1, 2, 10),
        (2, 2, 20),
        (3, 2, 30),
    ] {
        engine
            .set_cell_value("Sheet1", row, col, LiteralValue::Int(value))
            .expect("set fixture cell");
    }

    let cases: &[(&str, Option<f64>)] = &[
        ("=INDEX(A1:B3,2,1)", Some(2.0)),
        ("=INDEX(A1:B3,3,2)", Some(30.0)), // both upper bounds inclusive
        ("=INDEX(A1:B3,1,1)", Some(1.0)),
        ("=INDEX(A1:B3,4,1)", None),    // row just past the rect
        ("=INDEX(A1:B3,2,3)", None),    // column just past the rect
        ("=INDEX(A1:B3,0,1)", None),    // zero selects a whole row/column
        ("=INDEX(A1:B3,2)", None),      // 2-arg over a 2D rect is not single-cell
        ("=INDEX(A1:A3,3)", Some(3.0)), // single-column vector boundary
        ("=INDEX(A1:A3,4)", None),
        ("=INDEX(A1:B1,2)", Some(10.0)), // single-row vector boundary
        ("=INDEX(A1:B1,3)", None),
    ];

    for (formula, expected) in cases {
        let result =
            precise_dispatch_on(&engine, &crate::builtins::reference_fns::IndexFn, formula);
        match (result, expected) {
            (Some((_, literal)), Some(expected)) => {
                let number = match literal {
                    LiteralValue::Int(int) => int as f64,
                    LiteralValue::Number(number) => number,
                    other => panic!("{formula}: expected a number, got {other:?}"),
                };
                assert_eq!(number, *expected, "{formula}");
            }
            (None, None) => {}
            (taken, _) => panic!(
                "{formula}: precise path taken = {}, expected {}",
                taken.is_some(),
                expected.is_some()
            ),
        }
    }
}

#[test]
fn sum_over_rect_keeps_whole_rect_edges() {
    let mut engine = build_guarded_chain("=SUM(Q1:Q100)");
    engine.evaluate_all().expect("evaluate");
    assert!(is_circ(&engine, 9, 3), "C9 must remain Circ");
}

#[test]
fn index_unbounded_column_selection_measured() {
    let mut engine = build_guarded_chain("=INDEX(Q:Q,24)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        is_circ(&engine, 9, 3),
        "unbounded INDEX retains whole-column live edges while resolving bounds"
    );
}

/// Direct bound assertions on `precise_single_cell_selection`. The dispatch
/// path re-rejects out-of-range selections in `reference_from_base`, so a
/// widened bound in this filter is invisible to the taken/not-taken matrix
/// above; only predicate-level assertions catch such an off-by-one.
#[test]
fn index_precise_selection_predicate_bounds() {
    use formualizer_parse::parser::ASTNodeType;

    let engine = runtime_engine();
    let checks: &[(&str, u32, u32, bool)] = &[
        ("=INDEX(A1:B3,2,1)", 3, 2, true),
        ("=INDEX(A1:B3,3,2)", 3, 2, true), // inclusive upper corner
        ("=INDEX(A1:B3,4,1)", 3, 2, false), // row one past the rect
        ("=INDEX(A1:B3,2,3)", 3, 2, false), // column one past the rect
        ("=INDEX(A1:B3,0,1)", 3, 2, false), // zero row selects a whole column
        ("=INDEX(A1:B3,1,0)", 3, 2, false), // zero column selects a whole row
        ("=INDEX(A1:B3,2)", 3, 2, false),  // 2-arg over a 2D rect
        ("=INDEX(A1:A3,3)", 3, 1, true),   // column-vector boundary
        ("=INDEX(A1:A3,4)", 3, 1, false),
        ("=INDEX(A1:B1,2)", 1, 2, true), // row-vector boundary
        ("=INDEX(A1:B1,3)", 1, 2, false),
    ];

    for (formula, rows, cols, expected) in checks {
        let interpreter = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        let ast = parse(formula).expect("valid INDEX formula");
        let ASTNodeType::Function { args, .. } = &ast.node_type else {
            panic!("expected a function call: {formula}");
        };
        let handles: Vec<crate::traits::ArgumentHandle<'_, '_>> = args
            .iter()
            .map(|arg| crate::traits::ArgumentHandle::new(arg, &interpreter))
            .collect();
        assert_eq!(
            crate::builtins::reference_fns::IndexFn::precise_single_cell_selection(
                &handles, *rows, *cols
            ),
            *expected,
            "{formula} with dims {rows}x{cols}"
        );
    }
}
