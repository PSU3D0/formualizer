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
    assert_eq!(
        rewritten("_xlfn.SINGLE(A1:A3)+B1:B3"),
        "_xlfn.SINGLE(A1:A3)+@(B1:B3)"
    );
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

#[test]
fn shape_cache_reuses_only_equivalent_shapes() {
    let mut lowerer = Lowerer::default();
    // Same shape, different cells: the second is answered from the cache.
    assert_eq!(
        lowerer.lower("IF(VLOOKUP(S2,$J$1:$L$9,3,FALSE)=1,A2,B2)"),
        Lowering::Unchanged
    );
    assert_eq!(
        lowerer.lower("IF(VLOOKUP(S3,$J$1:$L$9,3,FALSE)=1,A3,B3)"),
        Lowering::Unchanged
    );
    assert_eq!(lowerer.cache.len(), 1);
    // A cell, a single-cell range, a multi-cell range, and a name past the last column
    // are distinct shapes.
    assert_eq!(lowerer.lower("A1*2"), Lowering::Unchanged);
    assert_eq!(lowerer.lower("A1:A1*2"), Lowering::Unchanged);
    assert_eq!(
        lowerer.lower("A1:A3*2"),
        Lowering::Rewritten("@(A1:A3)*2".to_string())
    );
    // Multi-cell ranges of varying lengths reuse the cached rewrite pattern.
    assert_eq!(
        lowerer.lower("AA100:ZZ200*2"),
        Lowering::Rewritten("@(AA100:ZZ200)*2".to_string())
    );
    assert!(matches!(lowerer.lower("ZZZ1*2"), Lowering::Rewritten(_)));
    assert!(matches!(
        lowerer.lower("Sheet1:Sheet3!A1*2"),
        Lowering::Skipped(_)
    ));
    assert_eq!(lowerer.lower("Sheet2!B9*2"), Lowering::Unchanged);
}

#[test]
fn shape_cache_replays_rewrites_identically_to_uncached() {
    let cases = [
        "INDEX(Lookup!$B$2:$B$50001,MATCH(A2,Lookup!$A$2:$A$50001,0))",
        "INDEX(Lookup!$B$2:$B$50001,MATCH(A50001,Lookup!$A$2:$A$50001,0))",
        "=INDEX(Lookup!$C$2:$C$50001,MATCH(AA999,Lookup!$A$2:$A$50001,0))",
        "D2*INDEX(Assumptions!$D$16:$D$18,MATCH(D$1,Assumptions!$A$16:$A$18,0))*IF($B2=2026,1,1+Assumptions!$C$12)",
        "D1048*INDEX(Assumptions!$D$16:$D$18,MATCH(AA$100,Assumptions!$A$16:$A$18,0))*IF($BZ2048=2026,1,1+Assumptions!$C$12)",
        "IF(A1:A3>0,B1:B3,C1:C3)",
        "IF(AA10:AA300>0,BB10:BB300,CC10:CC300)",
        "1+(A1:A3)+2",
        "1+(AA100:ZZ200)+2",
        "Sheet1!A1:B2+Sheet2!C3:D4",
        "Sheet1!AA100:BB200+Sheet2!CC300:DD400",
        "A1:A1*2",
        "$B$20:B20*2",
        "VLOOKUP($B$4:$B$2636,$C$11:$D$17,2,FALSE)",
        "VLOOKUP($Z$40:$Z$99999,$E$1:$F$2,2,FALSE)",
        "Table1[Col]*2",
        "Table1[Col]*2",
    ];
    let mut lowerer = Lowerer::default();
    for f in cases {
        assert_eq!(lowerer.lower(f), lower(f), "mismatch for {f}");
    }
}

#[test]
fn shape_key_abstracts_cells_and_ranges_distinctly() {
    assert_eq!(shape_key("IF(S2=1,$A$2,B2)"), "IF(\u{1}=1,\u{1},\u{1})");
    assert_eq!(shape_key("A1:A1+B2"), "\u{3}+\u{1}");
    assert_eq!(shape_key("A1:A3+B2"), "\u{2}+\u{1}");
    assert_eq!(shape_key("Sheet1:Sheet3!A1"), "Sheet1:Sheet3!\u{1}");
    assert_eq!(shape_key("A1:B2!C3"), "A1:B2!\u{1}");
    assert_eq!(shape_key("Sheet2!B9&\"C3\""), "Sheet2!\u{1}&\"C3\"");
    assert_eq!(
        shape_key("'Q1 A1'!B2*LOG10(ZZZ1)"),
        "'Q1 A1'!\u{1}*LOG10(ZZZ1)"
    );
    assert_eq!(shape_key("1E5+A1"), "1E5+\u{1}");
}
