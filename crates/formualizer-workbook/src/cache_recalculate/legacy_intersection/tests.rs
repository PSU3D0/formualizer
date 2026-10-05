use super::*;

fn rewritten(formula: &str) -> String {
    match lower(formula) {
        Lowering::Rewritten(text) => text,
        other => panic!("{formula}: expected a rewrite, got {other:?}"),
    }
}
fn unchanged(formula: &str) {
    assert_eq!(lower(formula), Lowering::Unchanged, "{formula}");
}
fn skipped(formula: &str) {
    assert!(
        matches!(lower(formula), Lowering::Skipped(_)),
        "{formula}: {:?}",
        lower(formula)
    );
}

#[test]
fn class_table_is_sorted_and_unique() {
    for pair in classes::FUNCTIONS.windows(2) {
        assert!(pair[0].0 < pair[1].0, "{} / {}", pair[0].0, pair[1].0);
    }
}

#[test]
fn corpus_shapes() {
    // Enron: lookup value in a value parameter.
    assert_eq!(
        rewritten("VLOOKUP($B$4:$B$2636,$C$11:$D$17,2,FALSE)"),
        "VLOOKUP(@($B$4:$B$2636),$C$11:$D$17,2,FALSE)"
    );
    // Whole-row comparison in IF's condition; IF returns a range reference
    // in a value context, so the result intersects too.
    assert_eq!(
        rewritten("IF(E2=21:21,E$22:E$23,\" \")"),
        "@(IF(E2=@(21:21),E$22:E$23,\" \"))"
    );
    // Formula root is a value.
    assert_eq!(rewritten("'Study 1b'!E134:E157"), "@('Study 1b'!E134:E157)");
    assert_eq!(
        rewritten("+'Hotlist - Identified '!B6:B11"),
        "+@('Hotlist - Identified '!B6:B11)"
    );
    assert_eq!(rewritten("Annual!F1:G1"), "@(Annual!F1:G1)");
    assert_eq!(rewritten("AF84+AJ84+BD84:BD85"), "AF84+AJ84+@(BD84:BD85)");
}

#[test]
fn reference_parameters_take_the_reference() {
    for f in [
        "SUM(A1:A10)",
        "SUMIF(A1:A10,\">0\",B1:B10)",
        "COUNTIFS(A:A,1,B:B,\"x\")",
        "VLOOKUP(A1,$C$1:$D$9,2,FALSE)",
        "MATCH(A1,B1:B9,0)",
        "INDEX(A1:A9,3)+0",
        "SUMPRODUCT(A1:A3*B1:B3)",
        "SUMPRODUCT((A1:A3>0)*B1:B3)",
        "ROWS(A1:A9)",
        "AND(A1,B1)",
        "A1+B2*3",
        "SUM(IF(A1>0,B1:B3,C1:C3))",
        "LOOKUP(A1,B1:B9,C1:C9)",
        "IRR(A1:A9)",
        "NPV(0.1,B1:B9)",
        "DSUM(A1:C9,\"x\",E1:E2)",
        "\"a:b\"&A1",
    ] {
        match lower(f) {
            Lowering::Unchanged => {}
            Lowering::Rewritten(t) if f.starts_with("INDEX") => {
                assert_eq!(t, "@(INDEX(A1:A9,3))+0")
            }
            other => panic!("{f}: {other:?}"),
        }
    }
}

#[test]
fn value_positions_intersect() {
    assert_eq!(rewritten("A1:A10*2"), "@(A1:A10)*2");
    assert_eq!(rewritten("SUM(A1:A3*2)"), "SUM(@(A1:A3)*2)");
    assert_eq!(rewritten("AND(A1:A3>0)"), "AND(@(A1:A3)>0)");
    assert_eq!(rewritten("LEN(A1:A3)"), "LEN(@(A1:A3))");
    assert_eq!(rewritten("SUM(LEN(A1:A3))"), "SUM(LEN(@(A1:A3)))");
    assert_eq!(rewritten("-A:A"), "-@(A:A)");
    assert_eq!(rewritten("(A1:A3)*2"), "@((A1:A3))*2");
    assert_eq!(rewritten("Price*Qty"), "@(Price)*@(Qty)");
    assert_eq!(rewritten("ROW(A1:A3)"), "@(ROW(A1:A3))");
    assert_eq!(rewritten("SUM(ROW(A1:A3))"), "SUM(@(ROW(A1:A3)))");
    // TRANSPOSE takes a value-class operand in a cell formula (BIFF `VO`).
    assert_eq!(rewritten("TRANSPOSE(A1:A3)"), "@(TRANSPOSE(@(A1:A3)))");
    // Known Excel quirk: TRANSPOSE needs array entry even inside SUMPRODUCT.
    assert_eq!(
        rewritten("SUMPRODUCT(TRANSPOSE(A1:A3))"),
        "SUMPRODUCT(TRANSPOSE(@(A1:A3)))"
    );
    assert_eq!(rewritten("{1,2,3}"), "@({1,2,3})");
    assert_eq!(rewritten("IFERROR(A1/B1,C1:C9)"), "@(IFERROR(A1/B1,C1:C9))");
    assert_eq!(
        rewritten("IF(A1:A9>0,\"y\",\"n\")"),
        "IF(@(A1:A9)>0,\"y\",\"n\")"
    );
    assert_eq!(rewritten("INDEX(A:A,5)"), "@(INDEX(A:A,5))");
    assert_eq!(rewritten("OFFSET(A1,0,0,3,1)"), "@(OFFSET(A1,0,0,3,1))");
    unchanged("SUM(OFFSET(A1,0,0,3,1))");
}

#[test]
fn array_context_does_not_intersect() {
    // SUMPRODUCT parameters are array class.
    unchanged("SUMPRODUCT(LEN(A1:A3))");
    assert_eq!(rewritten("MMULT(A1:B2,C1:D2)"), "@(MMULT(A1:B2,C1:D2))");
    // Forced value parameter even under an array context (BIFF VV).
    assert_eq!(
        rewritten("SUMPRODUCT(VLOOKUP(A1:A3,C1:D9,2,FALSE))"),
        "SUMPRODUCT(VLOOKUP(@(A1:A3),C1:D9,2,FALSE))"
    );
}

#[test]
fn single_cells_and_scalars_are_unchanged() {
    for f in [
        "A1",
        "Sheet2!B7",
        "A1:A1",
        "1+2",
        "\"text\"",
        "TODAY()",
        "NOW()-A1",
        "IF(A1,B1,C1)",
        "CONCATENATE(A1,\"x\")",
    ] {
        unchanged(f);
    }
}

#[test]
fn unclaimed_shapes_are_skipped() {
    skipped("LET(x,A1:A3,x*2)");
    skipped("XLOOKUP(A1,B1:B9,C1:C9)");
    skipped("FILTER(A1:A9,B1:B9>0)");
    skipped("MYUDF(A1:A3)");
    skipped("SUM(Sheet1:Sheet3!A1:A3)*A1:A2");
    skipped("Table1[Col]*2");
    skipped("EDATE(A1:A3,1)");
    skipped("N(A1:A3)");
    skipped("_xlfn.ANCHORARRAY(A1)");
}

#[test]
fn xlfn_prefix_and_case_are_normalised() {
    assert_eq!(rewritten("_xlfn.IFNA(a1:a3,0)"), "_xlfn.IFNA(@(a1:a3),0)");
    assert_eq!(
        rewritten("vlookup(A1:A3,C:D,2,0)"),
        "vlookup(@(A1:A3),C:D,2,0)"
    );
}

#[test]
fn nested_wraps_are_well_formed() {
    assert_eq!(
        rewritten("IF(A1:A3>0,B1:B3,C1:C3)"),
        "@(IF(@(A1:A3)>0,B1:B3,C1:C3))"
    );
    assert_eq!(
        rewritten("CHOOSE(A1:A2,B1:B9,C1)&\"\""),
        "@(CHOOSE(@(A1:A2),B1:B9,C1))&\"\""
    );
    assert_eq!(rewritten("=A1:A3"), "=@(A1:A3)");
    assert_eq!(rewritten("( A1:A3 )+1"), "@(( A1:A3 ))+1");
}

#[test]
fn prefilter_never_hides_a_rewrite() {
    for f in [
        "Price*2",
        "INDEX(A1,1)",
        "OFFSET(A1,0,0,3)",
        "INDIRECT(\"A1\")",
        "TRANSPOSE(A1)",
        "{1,2}",
        "A1:A2",
    ] {
        assert!(may_intersect(f), "{f}");
    }
    for f in [
        "A1+B2",
        "SUM(A1,B2)",
        "Sheet2!A1*2",
        "'My sheet'!$B$3",
        "\"Rate\"&A1",
        "TRUE",
    ] {
        assert!(!may_intersect(f), "{f}");
    }
}
