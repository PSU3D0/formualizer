use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[test]
fn let_and_lambda_basic_engine_parity() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=LET(x,2,x+3)").unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            parse("=LET(inc,LAMBDA(n,n+1),inc(41))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn lambda_closure_capture_and_shadowing_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula(
            "Sheet1",
            2,
            1,
            parse("=LET(k,10,addk,LAMBDA(n,n+k),addk(5))").unwrap(),
        )
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=LET(x,2,LET(x,5,x)+x)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(15.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn lambda_errors_surface_in_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=LAMBDA(x,x+1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            3,
            2,
            parse("=LET(inc,LAMBDA(n,n+1),inc(1,2))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 3, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Calc),
        other => panic!("expected #CALC!, got {other:?}"),
    }

    match engine.get_cell_value("Sheet1", 3, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Value),
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn let_lambda_case_insensitive_names_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 4, 1, parse("=LET(x,1,X+1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 2, parse("=LET(F,LAMBDA(n,n+1),f(1))").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn let_local_name_shadows_workbook_defined_name_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .define_name(
            "x",
            NamedDefinition::Literal(LiteralValue::Number(100.0)),
            NameScope::Workbook,
        )
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 5, 1, parse("=LET(X,1,x+1)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn lambda_param_shadowing_and_capture_snapshot_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula(
            "Sheet1",
            6,
            1,
            parse("=LET(n,5,f,LAMBDA(n,n+1),f(10))").unwrap(),
        )
        .unwrap();

    engine
        .set_cell_formula(
            "Sheet1",
            6,
            2,
            parse("=LET(k,1,f,LAMBDA(x,x+k),k,2,f(0))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 6, 1),
        Some(LiteralValue::Number(11.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 6, 2),
        Some(LiteralValue::Number(1.0))
    );
}

#[test]
fn let_undefined_symbol_and_non_invoked_lambda_errors_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 7, 1, parse("=LET(x,y,y,2,x)").unwrap())
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 7, 2, parse("=LET(f,LAMBDA(x,x+1),f)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 7, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Name),
        other => panic!("expected #NAME?, got {other:?}"),
    }

    match engine.get_cell_value("Sheet1", 7, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Calc),
        other => panic!("expected #CALC!, got {other:?}"),
    }
}

#[test]
fn nested_let_lambda_dependency_recalc_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            parse("=LET(a,A1,f,LAMBDA(x,LET(y,x+a,y)),f(2))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(12.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(22.0))
    );
}

#[test]
fn let_range_binding_feeds_aggregates_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    for (row, v) in [(1u32, 1.0), (2, 2.0), (3, 3.0)] {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(v))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=LET(r,B1:B3,SUM(r))").unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            2,
            1,
            parse("=LET(r,B1:B3,SUM(r)+COUNT(r))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(9.0))
    );
}

#[test]
fn let_range_binding_shadows_workbook_name_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .define_name(
            "r",
            NamedDefinition::Literal(LiteralValue::Number(100.0)),
            NameScope::Workbook,
        )
        .unwrap();
    for (row, v) in [(1u32, 1.0), (2, 2.0), (3, 3.0)] {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(v))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=LET(r,B1:B3,SUM(r))").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=SUM(r)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    // The LET-local binding wins inside the LET body...
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(6.0))
    );
    // ...while the workbook name still resolves outside it.
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(100.0))
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// A LET/LAMBDA local bound to a range, passed to a reference-taking argument
// slot, must resolve to the bound range rather than to an undefined workbook
// name. The cases below pin both the by-ref consumers that used to answer
// #NAME? and the value consumers that were already correct.
// ─────────────────────────────────────────────────────────────────────────────

/// Fill the shared grid: A1:A3 = 1,2,3 and B1:B3 = 10,20,30.
fn fill_local_ref_grid(engine: &mut Engine<TestWorkbook>) {
    for (row, a, b) in [(1u32, 1.0, 10.0), (2, 2.0, 20.0), (3, 3.0, 30.0)] {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(a))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(b))
            .unwrap();
    }
}

fn eval_over_local_ref_grid(formula: &str) -> LiteralValue {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    fill_local_ref_grid(&mut engine);
    engine
        .set_cell_formula("Sheet1", 1, 3, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.get_cell_value("Sheet1", 1, 3).unwrap()
}

/// Compare against Excel's numeric answer without pinning Int-vs-Number.
fn as_number(value: LiteralValue) -> f64 {
    match value {
        LiteralValue::Number(n) => n,
        LiteralValue::Int(i) => i as f64,
        other => panic!("expected a number, got {other:?}"),
    }
}

fn assert_number_over_grid(formula: &str, expected: f64) {
    assert_eq!(
        as_number(eval_over_local_ref_grid(formula)),
        expected,
        "{formula}"
    );
}

#[test]
fn match_over_let_range_local_uses_the_bound_range() {
    assert_number_over_grid("=LET(r,A1:A3,MATCH(2,r,0))", 2.0);
}

#[test]
fn vlookup_over_let_range_local_uses_the_bound_table() {
    assert_number_over_grid("=LET(t,A1:B3,VLOOKUP(2,t,2,FALSE))", 20.0);
}

#[test]
fn offset_over_let_range_local_uses_the_bound_range_as_its_base() {
    assert_number_over_grid("=LET(r,A1:A3,SUM(OFFSET(r,1,0,2,1)))", 5.0);
}

#[test]
fn match_over_let_range_local_inside_if_uses_the_bound_range() {
    assert_number_over_grid("=LET(r,A1:A3,IF(TRUE,MATCH(3,r,0),0))", 3.0);
}

#[test]
fn lookup_over_let_range_local_uses_the_bound_range() {
    assert_number_over_grid("=LET(r,A1:A3,LOOKUP(2,r,B1:B3))", 20.0);
}

#[test]
fn hlookup_over_let_range_local_reports_no_match_rather_than_an_unknown_name() {
    // Row 1 of A1:B3 holds no 2, so Excel's answer is #N/A, not #NAME?.
    match eval_over_local_ref_grid("=LET(t,A1:B3,HLOOKUP(2,t,2,FALSE))") {
        LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Na),
        other => panic!("expected #N/A, got {other:?}"),
    }
}

#[test]
fn rows_over_let_range_local_counts_the_bound_range() {
    assert_number_over_grid("=LET(r,A1:A3,ROWS(r))", 3.0);
}

#[test]
fn index_of_match_over_let_range_local_uses_the_bound_range() {
    assert_number_over_grid("=LET(r,A1:A3,INDEX(B1:B3,MATCH(3,r,0)))", 30.0);
}

#[test]
fn approximate_match_over_let_range_local_uses_the_bound_range() {
    assert_number_over_grid("=LET(r,A1:A3,MATCH(2.5,r,1))", 2.0);
}

#[test]
fn sum_over_let_range_local_stays_the_bound_range_total() {
    assert_number_over_grid("=LET(r,A1:A3,SUM(r))", 6.0);
}

#[test]
fn index_over_let_range_local_stays_the_bound_range_element() {
    assert_number_over_grid("=LET(r,A1:A3,INDEX(r,2))", 2.0);
}

#[test]
fn sumif_over_let_range_local_stays_the_bound_range_conditional_total() {
    assert_number_over_grid("=LET(r,A1:A3,SUMIF(r,\">1\"))", 5.0);
}

#[test]
fn xlookup_over_let_range_local_stays_the_bound_range_match() {
    assert_number_over_grid("=LET(r,A1:A3,XLOOKUP(2,r,B1:B3))", 20.0);
}

#[test]
fn sumproduct_over_let_range_local_stays_the_bound_range_product_sum() {
    assert_number_over_grid("=LET(r,A1:A3,SUMPRODUCT(r,B1:B3))", 140.0);
}

#[test]
fn choose_of_let_range_local_stays_the_bound_range_total() {
    assert_number_over_grid("=LET(r,A1:A3,SUM(CHOOSE(1,r,B1:B3)))", 6.0);
}

#[test]
fn countif_over_let_range_local_stays_the_bound_range_count() {
    assert_number_over_grid("=LET(r,A1:A3,COUNTIF(r,\">1\"))", 2.0);
}

#[test]
fn sumifs_over_let_range_local_stays_the_bound_range_conditional_total() {
    assert_number_over_grid("=LET(r,A1:A3,SUMIFS(B1:B3,r,\">1\"))", 50.0);
}

#[test]
fn choose_over_let_scalar_local_stays_the_selected_argument() {
    assert_number_over_grid("=LET(i,2,CHOOSE(i,10,20,30))", 20.0);
}

#[test]
fn or_over_let_scalar_local_stays_the_bound_boolean() {
    // A scalar local must not be forced onto the by-ref path.
    assert_eq!(
        eval_over_local_ref_grid("=LET(p,FALSE,OR(p))"),
        LiteralValue::Boolean(false)
    );
}

#[test]
fn match_of_let_scalar_local_against_a_range_stays_the_first_position() {
    assert_number_over_grid("=LET(k,2,MATCH(k,A1:A3,0))", 2.0);
}

#[test]
fn match_of_let_scalar_local_against_a_range_stays_the_last_position() {
    assert_number_over_grid("=LET(k,3,MATCH(k,A1:A3,0))", 3.0);
}

#[test]
fn nested_let_reusing_a_name_stays_lexically_scoped() {
    assert_number_over_grid("=LET(x,2,LET(x,5,x)+x)", 7.0);
}

#[test]
fn match_over_a_nested_let_rebinding_of_a_range_local_uses_the_inner_range() {
    // The inner LET rebinds the outer local to the same range under a new
    // name; the preserved reference must travel through both bindings.
    assert_number_over_grid("=LET(r,A1:A3,LET(s,r,MATCH(3,s,0)))", 3.0);
}

#[test]
fn match_over_a_lambda_range_parameter_uses_the_passed_range() {
    assert_number_over_grid("=LET(f,LAMBDA(v,MATCH(2,v,0)),f(A1:A3))", 2.0);
}

#[test]
fn let_range_local_wins_over_a_hidden_xlpm_workbook_name_of_the_same_spelling() {
    // Excel stores LAMBDA/LET parameter names as hidden `_xlpm.`-prefixed
    // workbook names; one defined as an error literal must not leak into the
    // local's resolution.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .define_name(
            "_xlpm.r",
            NamedDefinition::Literal(LiteralValue::Error(formualizer_common::ExcelError::new(
                ExcelErrorKind::Name,
            ))),
            NameScope::Workbook,
        )
        .unwrap();
    fill_local_ref_grid(&mut engine);
    // A plain-spelled workbook name colliding with the local's spelling, over
    // the other column, so taking the workbook route would answer 1 (B1 = 10
    // is not 2 either, so it would in fact be #N/A) rather than the local's 2.
    let sid = engine.sheet_id("Sheet1").unwrap();
    engine
        .define_name(
            "r",
            NamedDefinition::Range(RangeRef::new(
                CellRef::new(sid, Coord::from_excel(1, 2, true, true)),
                CellRef::new(sid, Coord::from_excel(3, 2, true, true)),
            )),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=LET(r,A1:A3,MATCH(2,r,0))").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        as_number(engine.get_cell_value("Sheet1", 1, 3).unwrap()),
        2.0
    );
}

/// Assert `formula` evaluates to an error of `kind` over the shared grid.
fn assert_error_kind_over_grid(formula: &str, kind: ExcelErrorKind) {
    match eval_over_local_ref_grid(formula) {
        LiteralValue::Error(e) => assert_eq!(e.kind, kind, "{formula}"),
        other => panic!("expected an error, got {other:?}"),
    }
}

#[test]
fn offset_over_a_lambda_range_parameter_uses_the_passed_range_as_its_base() {
    // A LAMBDA parameter, not just a LET binding: OFFSET needs a real
    // reference, so the passed range must survive the call boundary.
    assert_number_over_grid("=LET(f,LAMBDA(v,SUM(OFFSET(v,1,0,2,1))),f(A1:A3))", 5.0);
}

#[test]
fn offset_over_a_let_single_cell_local_uses_the_bound_cell_as_its_base() {
    assert_number_over_grid("=LET(r,A1,OFFSET(r,2,0))", 3.0);
}

#[test]
fn columns_over_let_range_local_counts_the_bound_range() {
    assert_number_over_grid("=LET(r,A1:B1,COLUMNS(r))", 2.0);
}

#[test]
fn single_cell_offset_over_let_range_local_selects_the_bound_range_cell() {
    assert_number_over_grid("=LET(r,A1:A3,OFFSET(r,1,0,1,1))", 2.0);
}

#[test]
fn let_local_bound_to_a_workbook_range_name_matches_inside_that_range() {
    // The bound expression is itself a workbook-scope name, so the local
    // carries a name-shaped reference rather than a literal A1 range.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    fill_local_ref_grid(&mut engine);
    let sid = engine.sheet_id("Sheet1").unwrap();
    engine
        .define_name(
            "Readings",
            NamedDefinition::Range(RangeRef::new(
                CellRef::new(sid, Coord::from_excel(1, 1, true, true)),
                CellRef::new(sid, Coord::from_excel(3, 1, true, true)),
            )),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            3,
            parse("=LET(x,Readings,MATCH(2,x,0))").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        as_number(engine.get_cell_value("Sheet1", 1, 3).unwrap()),
        2.0
    );
}

#[test]
fn offset_over_a_let_value_local_reports_a_value_error() {
    // A local bound to a plain value is not a reference; Excel answers
    // #VALUE! in a by-reference slot.
    assert_error_kind_over_grid("=LET(x,5,OFFSET(x,0,0))", ExcelErrorKind::Value);
}

#[test]
fn row_over_a_let_value_local_reports_a_value_error() {
    // Excel answers #VALUE!; see the note above.
    assert_error_kind_over_grid("=LET(x,5,ROW(x))", ExcelErrorKind::Value);
}

#[test]
fn column_over_a_let_value_local_reports_a_value_error() {
    // Excel answers #VALUE!; see the note above.
    assert_error_kind_over_grid("=LET(x,5,COLUMN(x))", ExcelErrorKind::Value);
}

#[test]
fn inner_value_local_shadows_an_outer_range_local_in_a_by_ref_slot() {
    // Excel's lexical scope: the inner `r` shadows the outer one, so MATCH must
    // NOT see A1:A3 - if it did, it would find 2 at index 2. It sees the scalar
    // 5 instead, and MATCH(2, 5, 0) is #N/A, which is also Excel's answer.
    //
    // This is the one place the `Some(Err(#VALUE!))` arm is observed end to
    // end: the by-ref accessor refuses the value-bound local, MATCH takes its
    // value fallback, and the #VALUE! never surfaces. It also pins that a preserved
    // reference is dropped when a nested LET rebinds the same identifier.
    assert_error_kind_over_grid("=LET(r,A1:A3,LET(r,5,MATCH(2,r,0)))", ExcelErrorKind::Na);
}

#[test]
fn inner_range_local_shadows_an_outer_range_local_in_a_by_ref_slot() {
    // The same shadowing rule the other way: the inner binding's range is the
    // one MATCH sees. Looking for 20 in A1:A3 would be #N/A; in B1:B3 it is 2.
    assert_number_over_grid("=LET(r,A1:A3,LET(r,B1:B3,MATCH(20,r,0)))", 2.0);
}

#[test]
fn let_range_local_on_another_sheet_resolves_against_that_sheet() {
    // A preserved ReferenceType that carries no sheet is resolved later against
    // the interpreter's current sheet. Bind the local to an explicitly
    // sheet-qualified range on a second sheet and consume it by reference from
    // Sheet1: the answer must come from Sheet2's column, not Sheet1's.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    fill_local_ref_grid(&mut engine);
    for (row, v) in [(1u32, 100.0), (2, 200.0), (3, 300.0)] {
        engine
            .set_cell_value("Sheet2", row, 1, LiteralValue::Number(v))
            .unwrap();
    }
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            3,
            parse("=LET(r,Sheet2!A1:A3,MATCH(200,r,0))").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        as_number(engine.get_cell_value("Sheet1", 1, 3).unwrap()),
        2.0
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// A range-bound local is a reference wherever a reference can go: through
// IF/CHOOSE selections, into a LAMBDA parameter via another local, and into
// consumers that treat references differently from values. Expected values
// were measured in Excel for the web.
// ─────────────────────────────────────────────────────────────────────────────

fn eval_column_over_local_ref_grid(formulas: &[&str]) -> Vec<LiteralValue> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    fill_local_ref_grid(&mut engine);
    // D1 holds the text "5" and D2 holds TRUE.
    engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Text("5".to_string()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 4, LiteralValue::Boolean(true))
        .unwrap();
    for (i, formula) in formulas.iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", i as u32 + 1, 10, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    (0..formulas.len())
        .map(|i| engine.get_cell_value("Sheet1", i as u32 + 1, 10).unwrap())
        .collect()
}

#[test]
fn range_bound_local_survives_if_and_choose_selection() {
    let cases = [
        ("=LET(r,A1:A3,SUM(OFFSET(IF(TRUE,r,B1:B3),1,0,2,1)))", 5.0),
        ("=LET(r,A1:A3,OFFSET(IF(TRUE,r,B1:B3),1,0,1,1))", 2.0),
        ("=LET(r,A1:A3,SUM(OFFSET(IF(FALSE,B1:B3,r),1,0,2,1)))", 5.0),
        ("=LET(r,A1:A3,ROWS(CHOOSE(1,r,B1:B3)))", 3.0),
        ("=LET(r,A1:A3,SUM(OFFSET(CHOOSE(2,B1:B3,r),1,0,2,1)))", 5.0),
        ("=LET(r,A1:A3,MATCH(2,IF(TRUE,r,B1:B3),0))", 2.0),
        ("=LET(r,A1:A3,SUM(IF(TRUE,r,B1:B3)))", 6.0),
    ];
    let formulas: Vec<&str> = cases.iter().map(|(f, _)| *f).collect();
    for ((formula, expected), actual) in
        cases.iter().zip(eval_column_over_local_ref_grid(&formulas))
    {
        assert_eq!(as_number(actual), *expected, "{formula}");
    }
}

#[test]
fn lambda_parameter_bound_through_a_nested_let_local_keeps_the_range() {
    let cases = [
        ("=LET(r,A1:A3,f,LAMBDA(v,SUM(OFFSET(v,1,0,2,1))),f(r))", 5.0),
        ("=LET(r,A1:A3,f,LAMBDA(v,MATCH(3,v,0)),LET(s,r,f(s)))", 3.0),
        (
            "=LET(r,A1:A3,f,LAMBDA(v,SUM(OFFSET(v,1,0,2,1))),LET(s,r,f(s)))",
            5.0,
        ),
    ];
    let formulas: Vec<&str> = cases.iter().map(|(f, _)| *f).collect();
    for ((formula, expected), actual) in
        cases.iter().zip(eval_column_over_local_ref_grid(&formulas))
    {
        assert_eq!(as_number(actual), *expected, "{formula}");
    }
}

#[test]
fn range_bound_local_has_reference_semantics_in_value_consumers() {
    // D1 is the text "5": a reference to it is skipped by SUM and COUNT and
    // makes OR answer #VALUE!, where the value "5" would be coerced.
    let values = eval_column_over_local_ref_grid(&[
        "=LET(r,D1,SUM(r))",
        "=LET(r,D1,COUNT(r))",
        "=LET(r,D1,COUNTA(r))",
        "=LET(p,D2,OR(p))",
        "=LET(p,D1,OR(p))",
    ]);
    assert_eq!(as_number(values[0].clone()), 0.0, "LET(r,D1,SUM(r))");
    assert_eq!(as_number(values[1].clone()), 0.0, "LET(r,D1,COUNT(r))");
    assert_eq!(as_number(values[2].clone()), 1.0, "LET(r,D1,COUNTA(r))");
    assert_eq!(values[3], LiteralValue::Boolean(true), "LET(p,D2,OR(p))");
    match &values[4] {
        LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value, "LET(p,D1,OR(p))"),
        other => panic!("LET(p,D1,OR(p)): expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn filled_down_let_local_follows_the_relative_range() {
    // F1:F3 hold one relative formula filled down: r is A1:A2, A2:A3, A3:A4.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    fill_local_ref_grid(&mut engine);
    for (row, formula) in [
        (1, "=LET(r,A1:A2,SUM(OFFSET(r,1,0,1,1)))"),
        (2, "=LET(r,A2:A3,SUM(OFFSET(r,1,0,1,1)))"),
        (3, "=LET(r,A3:A4,SUM(OFFSET(r,1,0,1,1)))"),
    ] {
        engine
            .set_cell_formula("Sheet1", row, 6, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    // Excel for the web: 2 and 3 for the first two rows; A4 is blank, so 0.
    for (row, expected) in [(1, 2.0), (2, 3.0), (3, 0.0)] {
        assert_eq!(
            as_number(engine.get_cell_value("Sheet1", row, 6).unwrap()),
            expected,
            "row {row}"
        );
    }

    // Editing A3 recalculates the row whose window reads it.
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(30.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    for (row, expected) in [(1, 2.0), (2, 30.0), (3, 0.0)] {
        assert_eq!(
            as_number(engine.get_cell_value("Sheet1", row, 6).unwrap()),
            expected,
            "row {row} after edit"
        );
    }
}
