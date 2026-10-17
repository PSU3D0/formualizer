//! Formula text that used to crash, hang or return a wrapped value in a
//! builtin. Each formula is evaluated through `Workbook`, as a user would
//! type it. Hangs are checked with a deadline on a worker thread.

use std::sync::mpsc;
use std::time::Duration;

use formualizer_common::ExcelErrorKind;
use formualizer_workbook::{LiteralValue, Workbook};

/// Evaluates one formula in a fresh workbook and returns the value of the
/// formula cell (or the cells of its spill, row by row).
fn eval_spill(formula: &str, rows: u32, cols: u32) -> Vec<Vec<LiteralValue>> {
    let formula = formula.to_string();
    let (tx, rx) = mpsc::channel();
    let shown = formula.clone();
    std::thread::spawn(move || {
        let mut wb = Workbook::new();
        wb.add_sheet("S").unwrap();
        wb.set_formula("S", 1, 1, &formula).unwrap();
        wb.evaluate_all().unwrap();
        let out = (1..=rows)
            .map(|r| {
                (1..=cols)
                    .map(|c| wb.get_value("S", r, c).unwrap_or(LiteralValue::Empty))
                    .collect()
            })
            .collect();
        let _ = tx.send(out);
    });
    match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(v) => v,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{shown}: still running after 20 s"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{shown}: evaluation panicked"),
    }
}

fn eval(formula: &str) -> LiteralValue {
    eval_spill(formula, 1, 1).remove(0).remove(0)
}

fn assert_error(formula: &str, kind: ExcelErrorKind) {
    match eval(formula) {
        LiteralValue::Error(e) => assert_eq!(e.kind, kind, "{formula}"),
        other => panic!("{formula}: expected {kind:?}, got {other:?}"),
    }
}

fn number(formula: &str) -> f64 {
    match eval(formula) {
        LiteralValue::Number(n) => n,
        LiteralValue::Int(i) => i as f64,
        other => panic!("{formula}: expected a number, got {other:?}"),
    }
}

fn not_a_crash(formula: &str) {
    match eval(formula) {
        LiteralValue::Number(_) | LiteralValue::Int(_) | LiteralValue::Error(_) => {}
        other => panic!("{formula}: unexpected {other:?}"),
    }
}

#[test]
fn beta_distribution_at_the_symmetry_threshold_returns() {
    // Both argument orders pass the symmetry test after rounding here.
    let p = number("=BETA.DIST(0.1178597860988024,1,13.969316390268949,TRUE)");
    assert!((0.0..=1.0).contains(&p), "{p}");
    let legacy = number("=BETADIST(0.1178597860988024,1,13.969316390268949)");
    assert_eq!(p, legacy);
    not_a_crash("=BETA.INV(0.649450,10,9.23781e15)");
    not_a_crash("=BETAINV(0.649450,10,9.23781e15)");
    // Away from the threshold the value is unchanged.
    let v = number("=BETA.DIST(0.4,2,3,TRUE)");
    assert!((v - 0.5248).abs() < 1e-12, "{v}");
}

const MIXED: &str =
    r#"{5;TRUE;3;"zz";"a";FALSE;1;TRUE;0.5;2;3;0.5;"10";1;10;0.5;1;10;"b";"10";FALSE}"#;

fn mixed_sorted_ascending() -> Vec<LiteralValue> {
    let n = |v: f64| LiteralValue::Number(v);
    let t = |s: &str| LiteralValue::Text(s.to_string());
    let b = LiteralValue::Boolean;
    vec![
        n(0.5),
        n(0.5),
        n(0.5),
        n(1.0),
        n(1.0),
        n(1.0),
        n(2.0),
        n(3.0),
        n(3.0),
        n(5.0),
        n(10.0),
        n(10.0),
        t("10"),
        t("10"),
        t("a"),
        t("b"),
        t("zz"),
        b(false),
        b(false),
        b(true),
        b(true),
    ]
}

fn column(formula: &str, rows: u32) -> Vec<LiteralValue> {
    eval_spill(formula, rows, 1)
        .into_iter()
        .map(|mut r| r.remove(0))
        .collect()
}

#[test]
fn sort_mixed_types_uses_excel_order() {
    let asc = mixed_sorted_ascending();
    assert_eq!(column(&format!("=SORT({MIXED})"), 21), asc);
    let mut desc = asc.clone();
    desc.reverse();
    assert_eq!(column(&format!("=SORT({MIXED},1,-1)"), 21), desc);
    assert_eq!(column(&format!("=SORTBY({MIXED},{MIXED})"), 21), asc);
    assert_eq!(column(&format!("=SORTBY({MIXED},{MIXED},-1)"), 21), desc);
}

#[test]
fn fixed_and_dollar_reject_more_than_127_decimals() {
    for f in [
        "=FIXED(1,70000)",
        "=DOLLAR(1,70000)",
        "=FIXED(1,65535)",
        "=FIXED(1,128)",
        "=DOLLAR(1,128)",
        "=FIXED(1,-128)",
    ] {
        assert_error(f, ExcelErrorKind::Value);
    }
    assert_eq!(eval("=FIXED(1.5,1)"), LiteralValue::Text("1.5".to_string()));
    match eval("=FIXED(1,127)") {
        LiteralValue::Text(s) => assert_eq!(s.len(), 129, "{s}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn combinatorics_with_huge_counts_return_quickly() {
    assert_eq!(number("=COMBINA(0,1)"), 0.0);
    assert_eq!(number("=COMBINA(,1)"), 0.0);
    assert_eq!(number("=COMBINA(0,0)"), 1.0);
    assert_eq!(number("=COMBINA(4,3)"), 20.0);
    assert_error("=COMBINA(1E300,1E300)", ExcelErrorKind::Num);
    assert_eq!(number("=COMBINA(,1E300)"), 0.0);
    assert_error("=COMBIN(1E9,5E8)", ExcelErrorKind::Num);
    assert_error("=COMBIN(1E15,5E14)", ExcelErrorKind::Num);
    assert_error("=PERMUT(1E10,1E10)", ExcelErrorKind::Num);
    assert_error("=PERMUT(1E15,1E15)", ExcelErrorKind::Num);
    assert_error("=FACTDOUBLE(1E9)", ExcelErrorKind::Num);
    assert_error("=FACTDOUBLE(1E15)", ExcelErrorKind::Num);
    assert_eq!(number("=COMBIN(10,3)"), 120.0);
    assert_eq!(number("=PERMUT(10,3)"), 720.0);
    assert_eq!(number("=FACTDOUBLE(7)"), 105.0);
}

#[test]
fn binomial_family_with_huge_counts_returns_quickly() {
    assert_eq!(number("=BINOM.DIST(1E9,1E9,0.5,TRUE)"), 1.0);
    assert_eq!(number("=BINOM.DIST(1E15,1E15,0.5,TRUE)"), 1.0);
    assert_error("=BINOM.DIST(5E8,1E9,0.5,TRUE)", ExcelErrorKind::Num);
    assert_error("=BINOM.INV(1E9,0.5,0.5)", ExcelErrorKind::Num);
    assert_error("=BINOM.INV(1E15,0.5,0.5)", ExcelErrorKind::Num);
    assert_error("=CRITBINOM(1E15,0.5,0.5)", ExcelErrorKind::Num);
    assert_error("=BINOM.DIST.RANGE(1E15,0.5,0,1E15)", ExcelErrorKind::Num);
    assert_error("=HYPGEOM.DIST(5E8,1E9,1E9,2E9,TRUE)", ExcelErrorKind::Num);
    // Small counts are unchanged.
    assert!((number("=BINOM.DIST(6,10,0.5,TRUE)") - 0.828125).abs() < 1e-12);
    assert_eq!(number("=BINOM.INV(6,0.5,0.75)"), 4.0);
}

#[test]
fn base_caps_min_length_at_255() {
    assert_error("=BASE(1E15,2,1E15)", ExcelErrorKind::Num);
    assert_error("=BASE(1,2,256)", ExcelErrorKind::Num);
    match eval("=BASE(1,2,255)") {
        LiteralValue::Text(s) => assert_eq!(s.len(), 255),
        other => panic!("{other:?}"),
    }
}

#[test]
fn release_wrapping_arithmetic_returns_excel_values() {
    assert_eq!(number("=EVEN(1E300)"), 1e300);
    assert_eq!(number("=EVEN(-1E300)"), -1e300);
    assert_eq!(number("=ODD(1E300)"), 1e300);
    assert_eq!(number("=ODD(-1E300)"), -1e300);
    assert_eq!(number("=EVEN(1.5)"), 2.0);
    assert_eq!(number("=ODD(-2)"), -3.0);
    assert_error("=LCM(9999999967,9999999943)", ExcelErrorKind::Num);
    assert_eq!(number("=LCM(4,6)"), 12.0);
    assert_error("=TIME(1000000,0,0)", ExcelErrorKind::Num);
    assert_error("=TIME(0,32768,0)", ExcelErrorKind::Num);
    assert!(matches!(eval("=TIME(32767,0,0)"), LiteralValue::Time(_)));
    assert_error("=DATE(2147483647,1,1)", ExcelErrorKind::Num);
    assert_error("=DATE(2000,2147483647,1)", ExcelErrorKind::Num);
    assert_error("=SORT({1;2},-1E300)", ExcelErrorKind::Value);
    assert_error("=TAKE({1;2;3},-1E300)", ExcelErrorKind::Value);
    assert_error("=MULTINOMIAL(9E18,9E18)", ExcelErrorKind::Num);
}

#[test]
fn debug_overflow_sites_agree_with_release() {
    assert_eq!(
        eval(r#"=MID("abc",2,9223372036854775807)"#),
        LiteralValue::Text("bc".to_string())
    );
    assert_eq!(
        eval(r#"=REPLACE("abc",2,1E300,"x")"#),
        LiteralValue::Text("ax".to_string())
    );
    assert_eq!(number("=BITLSHIFT(1,-1E300)"), 0.0);
    assert_error("=BITRSHIFT(1,-1E300)", ExcelErrorKind::Num);
    assert_error("=BESSELI(1,1E9)", ExcelErrorKind::Num);
    not_a_crash("=DB(1000,100,1E300,1)");
    // A saturated life used to wrap to #NUM!; the schedule past 2^20 periods
    // is now closed-form instead of 2^31 loop iterations.
    assert_eq!(number("=DB(1000,100,1E300,1E300)"), 0.0);
    assert_eq!(number("=DB(1000,100,2000000,2000000)"), 0.0);
    assert!((number("=DB(1000,100,6,2)") - 217.239).abs() < 1e-9);
    not_a_crash("=RANDBETWEEN(-1,1E300)");
    assert_error("=NEGBINOMDIST(1E300,1E300,0.5)", ExcelErrorKind::Num);
    assert_error(
        "=NEGBINOM.DIST(1E300,2147483648,0.5,FALSE)",
        ExcelErrorKind::Num,
    );
    not_a_crash("=NEGBINOM.DIST(1E300,2147483648,0.5,TRUE)");
    not_a_crash("=BINOM.DIST.RANGE(1E300,,65536)");
    not_a_crash("=HYPGEOMDIST(TRUE,65536,1E300,1E300)");
    not_a_crash("=POISSON.DIST(1E300,2147483647,4294967296)");
    not_a_crash("=POISSON.DIST(1E300,2,FALSE)");
}

#[test]
fn groupby_integer_options_saturate_like_numbers() {
    // SEQUENCE yields an Int; the same value typed in is a Number. As the
    // sort_order argument both must sort ascending (saturated), where the Int
    // used to wrap to 0 (unsorted).
    let run = |code: &str| {
        let mut wb = Workbook::new();
        wb.add_sheet("S").unwrap();
        for (r, (k, v)) in [("b", 1.0), ("a", 2.0), ("b", 3.0)].into_iter().enumerate() {
            let r = r as u32 + 1;
            wb.set_value("S", r, 1, LiteralValue::Text(k.into()))
                .unwrap();
            wb.set_value("S", r, 2, LiteralValue::Number(v)).unwrap();
        }
        wb.set_formula(
            "S",
            1,
            5,
            &format!(r#"=GROUPBY(A1:A3,B1:B3,"SUM",0,0,{code})"#),
        )
        .unwrap();
        wb.evaluate_all().unwrap();
        (1..=2)
            .map(|r| (5..=6).map(|c| wb.get_value("S", r, c)).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    let sorted = run("4294967296");
    assert_eq!(sorted[0][0], Some(LiteralValue::Text("a".into())));
    assert_eq!(run("SEQUENCE(1,1,4294967296)"), sorted);
}
