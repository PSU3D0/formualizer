//! Excel-compatibility tests for the statistical builtins, written from the
//! Microsoft support pages for each function:
//!
//! - the Excel 2007 compatibility names (`NORMSDIST`, `TDIST`, `BETADIST`, ...)
//!   with their own argument lists and domain rules;
//! - the paired two-array functions (`CORREL`, `SLOPE`, `COVAR`, ...), which
//!   drop a pair when either side is not a number;
//! - multiple regression in `LINEST`, `LOGEST`, `TREND` and `GROWTH`.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

const SHEET: &str = "Sheet1";

fn engine() -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..EvalConfig::default()
        },
    )
}

fn set(engine: &mut Engine<TestWorkbook>, a1: &str, value: LiteralValue) {
    let (row, col) = a1_to_rc(a1);
    engine.set_cell_value(SHEET, row, col, value).unwrap();
}

fn num(engine: &mut Engine<TestWorkbook>, a1: &str, n: f64) {
    set(engine, a1, LiteralValue::Number(n));
}

fn a1_to_rc(a1: &str) -> (u32, u32) {
    let letters: String = a1.chars().take_while(|c| c.is_ascii_alphabetic()).collect();
    let digits = &a1[letters.len()..];
    let col = letters.chars().fold(0u32, |acc, c| {
        acc * 26 + (c.to_ascii_uppercase() as u32 - 'A' as u32 + 1)
    });
    (digits.parse().unwrap(), col)
}

/// Evaluate `formula` in a scratch cell far from the data and return the
/// top-left value.
fn eval_in(engine: &mut Engine<TestWorkbook>, formula: &str) -> LiteralValue {
    engine
        .set_cell_formula(SHEET, 1000, 30, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value(SHEET, 1000, 30)
        .unwrap_or(LiteralValue::Empty)
}

fn eval(formula: &str) -> LiteralValue {
    eval_in(&mut engine(), formula)
}

#[track_caller]
fn assert_close(got: LiteralValue, expected: f64, tol: f64, what: &str) {
    match got {
        LiteralValue::Number(n) => assert!(
            (n - expected).abs() <= tol,
            "{what}: got {n}, expected {expected} (tol {tol})"
        ),
        LiteralValue::Int(i) => assert!(
            (i as f64 - expected).abs() <= tol,
            "{what}: got {i}, expected {expected} (tol {tol})"
        ),
        other => panic!("{what}: expected {expected}, got {other:?}"),
    }
}

#[track_caller]
fn assert_error(got: LiteralValue, kind: ExcelErrorKind, what: &str) {
    match got {
        LiteralValue::Error(e) => assert_eq!(e.kind, kind, "{what}: {e:?}"),
        other => panic!("{what}: expected {kind:?}, got {other:?}"),
    }
}

#[track_caller]
fn check(formula: &str, expected: f64, tol: f64) {
    assert_close(eval(formula), expected, tol, formula);
}

#[track_caller]
fn check_err(formula: &str, kind: ExcelErrorKind) {
    assert_error(eval(formula), kind, formula);
}

/* ───────────────────── legacy (Excel 2007) statistical names ───────────────────── */

// Values are the worked examples on each function's support page
// (https://support.microsoft.com/en-us/excel/functions/<name>-function), at the
// precision the page prints.

#[test]
fn legacy_normal_family_matches_documented_examples() {
    check("=NORMSDIST(1.333333)", 0.908788726, 1e-9);
    check("=NORMSDIST(0)", 0.5, 1e-15);
    check("=NORMSINV(0.9088)", 1.3334, 1e-4);
    check("=NORMDIST(42,40,1.5,TRUE)", 0.9087888, 1e-7);
    check("=NORMDIST(42,40,1.5,FALSE)", 0.10934, 1e-5);
    check("=NORMINV(0.908789,40,1.5)", 42.000002, 1e-6);
    check("=LOGNORMDIST(4,3.5,1.2)", 0.0390836, 1e-7);
    check("=LOGINV(0.039084,3.5,1.2)", 4.0000252, 1e-6);
}

#[test]
fn legacy_normal_family_domain_errors() {
    check_err("=NORMSDIST(\"x\")", ExcelErrorKind::Value);
    check_err("=NORMSINV(0)", ExcelErrorKind::Num);
    check_err("=NORMSINV(1)", ExcelErrorKind::Num);
    check_err("=NORMDIST(1,0,0,TRUE)", ExcelErrorKind::Num);
    check_err("=NORMINV(0.5,0,-1)", ExcelErrorKind::Num);
    // LOGNORMDIST is the 3-argument cumulative form.
    check_err("=LOGNORMDIST(0,0,1)", ExcelErrorKind::Num);
    check_err("=LOGNORMDIST(1,0,0)", ExcelErrorKind::Num);
    check_err("=LOGINV(1,0,1)", ExcelErrorKind::Num);
}

#[test]
fn legacy_tdist_tails_and_domain() {
    check("=TDIST(1.959999998,60,2)", 0.054644930, 1e-9);
    check("=TDIST(1.959999998,60,1)", 0.027322465, 1e-9);
    // Deg_freedom and tails are truncated to integers.
    check("=TDIST(1,10.9,2)-TDIST(1,10,2)", 0.0, 0.0);
    check("=TDIST(1,10,1.9)-TDIST(1,10,1)", 0.0, 0.0);
    check("=TDIST(1,10,2.5)-TDIST(1,10,2)", 0.0, 0.0);
    check("=TDIST(0,5,2)", 1.0, 1e-15);
    check_err("=TDIST(-1,10,1)", ExcelErrorKind::Num);
    check_err("=TDIST(1,0.5,1)", ExcelErrorKind::Num);
    check_err("=TDIST(1,10,3)", ExcelErrorKind::Num);
    check_err("=TDIST(1,10,0)", ExcelErrorKind::Num);
}

#[test]
fn legacy_tinv_is_two_tailed_with_truncated_df() {
    check("=TINV(0.05464,60)", 1.96, 1e-4);
    check("=TINV(0.05,10)", 2.228138851986, 1e-9);
    check("=TINV(0.05,10.7)-TINV(0.05,10)", 0.0, 0.0);
    check_err("=TINV(0,10)", ExcelErrorKind::Num);
    check_err("=TINV(1.1,10)", ExcelErrorKind::Num);
    check_err("=TINV(0.5,0.9)", ExcelErrorKind::Num);
}

#[test]
fn legacy_chi_and_f_are_right_tailed() {
    check("=CHIDIST(18.307,10)", 0.0500006, 1e-7);
    check("=CHIINV(0.050001,10)", 18.306973, 1e-5);
    check("=FDIST(15.20686486,6,4)", 0.01, 1e-8);
    check("=FINV(0.01,6,4)", 15.206865, 1e-5);
    // Degrees of freedom are truncated.
    check("=CHIDIST(3,2.9)-CHIDIST(3,2)", 0.0, 0.0);
    check("=CHIINV(0.3,4.5)-CHIINV(0.3,4)", 0.0, 0.0);
    check("=FDIST(2,3.7,5.2)-FDIST(2,3,5)", 0.0, 0.0);
    check("=FINV(0.2,3.7,5.2)-FINV(0.2,3,5)", 0.0, 0.0);
    check_err("=CHIDIST(-1,2)", ExcelErrorKind::Num);
    check_err("=CHIDIST(1,0.5)", ExcelErrorKind::Num);
    check_err("=CHIDIST(1,1E11)", ExcelErrorKind::Num);
    check_err("=CHIINV(-0.1,2)", ExcelErrorKind::Num);
    check_err("=CHIINV(1.1,2)", ExcelErrorKind::Num);
    check_err("=FDIST(-1,2,3)", ExcelErrorKind::Num);
    check_err("=FDIST(1,0.5,3)", ExcelErrorKind::Num);
    check_err("=FDIST(1,3,1E10)", ExcelErrorKind::Num);
    check_err("=FINV(1.5,2,3)", ExcelErrorKind::Num);
    check_err("=FINV(0.5,2,1E10)", ExcelErrorKind::Num);
}

#[test]
fn legacy_beta_gamma_and_discrete_examples() {
    check("=BETADIST(2,8,10,1,3)", 0.6854706, 1e-7);
    check("=BETADIST(0.5,2,2)", 0.5, 1e-12);
    check("=BETAINV(0.685470581,8,10,1,3)", 2.0, 1e-6);
    check("=GAMMADIST(10.00001131,9,2,FALSE)", 0.032639, 1e-6);
    check("=GAMMADIST(10.00001131,9,2,TRUE)", 0.068094, 1e-6);
    check("=GAMMAINV(0.068094,9,2)", 10.0000112, 1e-4);
    check("=POISSON(2,5,TRUE)", 0.124652, 1e-6);
    check("=POISSON(2,5,FALSE)", 0.084224, 1e-6);
    check("=BINOMDIST(6,10,0.5,FALSE)", 0.2050781, 1e-7);
    check("=EXPONDIST(0.2,10,TRUE)", 0.86466472, 1e-8);
    check("=EXPONDIST(0.2,10,FALSE)", 1.35335283, 1e-8);
    check("=WEIBULL(105,20,100,TRUE)", 0.929581, 1e-6);
    check("=WEIBULL(105,20,100,FALSE)", 0.035589, 1e-6);
    check("=HYPGEOMDIST(1,4,8,20)", 0.3633, 1e-4);
    check("=NEGBINOMDIST(10,5,0.25)", 0.05504866, 1e-8);
    check("=CRITBINOM(6,0.5,0.75)", 4.0, 0.0);
}

#[test]
fn legacy_beta_gamma_and_discrete_domain_errors() {
    // BETADIST: x outside [A,B] or A = B.
    check_err("=BETADIST(0.5,0,2)", ExcelErrorKind::Num);
    check_err("=BETADIST(0.5,2,2,1,3)", ExcelErrorKind::Num);
    check_err("=BETADIST(4,2,2,1,3)", ExcelErrorKind::Num);
    check_err("=BETADIST(1,2,2,1,1)", ExcelErrorKind::Num);
    // BETAINV: probability <= 0 or > 1.
    check_err("=BETAINV(0,2,3)", ExcelErrorKind::Num);
    check_err("=BETAINV(1.2,2,3)", ExcelErrorKind::Num);
    check_err("=GAMMADIST(-1,2,1,TRUE)", ExcelErrorKind::Num);
    check_err("=POISSON(-1,2,TRUE)", ExcelErrorKind::Num);
    check_err("=BINOMDIST(3,2,0.5,TRUE)", ExcelErrorKind::Num);
    check_err("=EXPONDIST(1,0,TRUE)", ExcelErrorKind::Num);
    check_err("=WEIBULL(1,0,1,TRUE)", ExcelErrorKind::Num);
    // HYPGEOMDIST: sample_s outside its feasible range is #NUM!, not 0.
    check_err("=HYPGEOMDIST(5,4,8,20)", ExcelErrorKind::Num);
    check_err("=HYPGEOMDIST(-1,4,8,20)", ExcelErrorKind::Num);
    check_err("=HYPGEOMDIST(0,10,15,20)", ExcelErrorKind::Num);
    check_err("=HYPGEOMDIST(1,0,8,20)", ExcelErrorKind::Num);
    check_err("=HYPGEOMDIST(1,4,0,20)", ExcelErrorKind::Num);
    // NEGBINOMDIST: probability outside [0,1], number_f < 0, number_s < 1.
    check_err("=NEGBINOMDIST(1,2,1.5)", ExcelErrorKind::Num);
    check_err("=NEGBINOMDIST(-1,2,0.5)", ExcelErrorKind::Num);
    check_err("=NEGBINOMDIST(1,0.5,0.5)", ExcelErrorKind::Num);
}

#[test]
fn legacy_hypgeom_and_negbinom_truncate_arguments() {
    check(
        "=HYPGEOMDIST(1.9,4.2,8.7,20.1)-HYPGEOMDIST(1,4,8,20)",
        0.0,
        0.0,
    );
    check(
        "=NEGBINOMDIST(10.9,5.5,0.25)-NEGBINOMDIST(10,5,0.25)",
        0.0,
        0.0,
    );
}

/// Reference values for the legacy tail and inverse functions across shapes
/// and tails (computed independently with SciPy's `isf`/`ppf`/`sf`).
#[rustfmt::skip]
const REFERENCE_GRID: &[(&str, f64)] = &[
    ("=CHIINV(0.001,1)", 10.827566170662733),
    ("=CHIINV(0.001,3)", 16.26623619623813),
    ("=CHIINV(0.001,30)", 59.703064304429944),
    ("=CHIINV(0.001,200)", 267.5405278227572),
    ("=CHIINV(0.05,1)", 3.8414588206941285),
    ("=CHIINV(0.05,3)", 7.814727903251178),
    ("=CHIINV(0.05,30)", 43.77297182574217),
    ("=CHIINV(0.05,200)", 233.99426889232492),
    ("=CHIINV(0.5,1)", 0.4549364231195724),
    ("=CHIINV(0.5,3)", 2.3659738843753377),
    ("=CHIINV(0.5,30)", 29.336031516661585),
    ("=CHIINV(0.5,200)", 199.33372983863097),
    ("=CHIINV(0.95,1)", 0.003932140000019531),
    ("=CHIINV(0.95,3)", 0.35184631774927166),
    ("=CHIINV(0.95,30)", 18.49266098195347),
    ("=CHIINV(0.95,200)", 168.2785544366284),
    ("=CHIINV(0.999,1)", 1.570797149262492e-06),
    ("=CHIINV(0.999,3)", 0.02429758581569275),
    ("=CHIINV(0.999,30)", 11.587951045645058),
    ("=CHIINV(0.999,200)", 143.8427949900008),
    ("=FINV(0.001,1,1)", 405284.0679028482),
    ("=FINV(0.001,3,40)", 6.594539977661781),
    ("=FINV(0.001,20,5)", 25.394622094525214),
    ("=FINV(0.001,100,100)", 1.8674013821322328),
    ("=FINV(0.05,1,1)", 161.4476387975882),
    ("=FINV(0.05,3,40)", 2.8387453980206403),
    ("=FINV(0.05,20,5)", 4.558131497396519),
    ("=FINV(0.05,100,100)", 1.39171955165522),
    ("=FINV(0.5,1,1)", 1.0),
    ("=FINV(0.5,3,40)", 0.8022775178428054),
    ("=FINV(0.5,20,5)", 1.1106465112961703),
    ("=FINV(0.5,100,100)", 0.9999999999999994),
    ("=FINV(0.95,1,1)", 0.006193958657108205),
    ("=FINV(0.95,3,40)", 0.11635468339965845),
    ("=FINV(0.95,20,5)", 0.368882566260714),
    ("=FINV(0.95,100,100)", 0.7185355690452617),
    ("=BETAINV(0.001,0.5,0.5)", 2.4673990709169446e-06),
    ("=BETAINV(0.001,9,2)", 0.37627691091117454),
    ("=BETAINV(0.001,2,30)", 0.0014876861369423936),
    ("=BETAINV(0.001,50,50)", 0.3487478265970523),
    ("=BETAINV(0.05,0.5,0.5)", 0.0061558297024311365),
    ("=BETAINV(0.05,9,2)", 0.6058366975634952),
    ("=BETAINV(0.05,2,30)", 0.011585315861443594),
    ("=BETAINV(0.05,50,50)", 0.41810922158826574),
    ("=BETAINV(0.5,0.5,0.5)", 0.4999999999999999),
    ("=BETAINV(0.5,9,2)", 0.8377372718047538),
    ("=BETAINV(0.5,2,30)", 0.05355205211700271),
    ("=BETAINV(0.5,50,50)", 0.4999999999999999),
    ("=BETAINV(0.95,0.5,0.5)", 0.9938441702975689),
    ("=BETAINV(0.95,9,2)", 0.9632285621125349),
    ("=BETAINV(0.95,2,30)", 0.14409039131834475),
    ("=BETAINV(0.95,50,50)", 0.5818907784117342),
    ("=GAMMAINV(0.001,0.3,2)", 1.3945398193566689e-10),
    ("=GAMMAINV(0.001,1,2)", 0.002001000667167068),
    ("=GAMMAINV(0.001,9,2)", 4.90484880872755),
    ("=GAMMAINV(0.001,100,2)", 143.8427949900008),
    ("=GAMMAINV(0.05,0.3,2)", 6.42206939944593e-05),
    ("=GAMMAINV(0.05,1,2)", 0.10258658877510106),
    ("=GAMMAINV(0.05,9,2)", 9.390455080688984),
    ("=GAMMAINV(0.05,100,2)", 168.27855443662838),
    ("=GAMMAINV(0.5,0.3,2)", 0.14626227173390396),
    ("=GAMMAINV(0.5,1,2)", 1.386294361119891),
    ("=GAMMAINV(0.5,9,2)", 17.33790236874074),
    ("=GAMMAINV(0.5,100,2)", 199.33372983863097),
    ("=GAMMAINV(0.95,0.3,2)", 2.744699888201772),
    ("=GAMMAINV(0.95,1,2)", 5.991464547107979),
    ("=GAMMAINV(0.95,9,2)", 28.869299430392623),
    ("=GAMMAINV(0.95,100,2)", 233.99426889232492),
    ("=GAMMAINV(0.999,0.3,2)", 9.237872085582666),
    ("=GAMMAINV(0.999,1,2)", 13.815510557964274),
    ("=GAMMAINV(0.999,9,2)", 42.31239633167996),
    ("=GAMMAINV(0.999,100,2)", 267.5405278227572),
    ("=TINV(0.001,1)", 636.6192487687196),
    ("=TINV(0.001,2)", 31.59905457644362),
    ("=TINV(0.001,5)", 6.86882662588111),
    ("=TINV(0.001,60)", 3.4602004691963555),
    ("=TINV(0.05,1)", 12.706204736174705),
    ("=TINV(0.05,2)", 4.302652729749464),
    ("=TINV(0.05,5)", 2.5705818356363155),
    ("=TINV(0.05,60)", 2.0002978220142604),
    ("=TINV(0.5,1)", 1.0000000000000002),
    ("=TINV(0.5,2)", 0.8164965809277261),
    ("=TINV(0.5,5)", 0.7266868438004226),
    ("=TINV(0.5,60)", 0.6786007206481355),
    ("=TINV(0.95,1)", 0.07870170682461851),
    ("=TINV(0.95,2)", 0.07079923254047893),
    ("=TINV(0.95,5)", 0.06591485539302447),
    ("=TINV(0.95,60)", 0.06296962799081811),
    ("=CHIDIST(0.1,1)", 0.7518296340458492),
    ("=TDIST(0.1,1,1)", 0.4682744825694465),
    ("=FDIST(0.1,1,7)", 0.761050537242554),
    ("=CHIDIST(0.1,4)", 0.9987908957257497),
    ("=TDIST(0.1,4,1)", 0.4625779204697266),
    ("=FDIST(0.1,4,7)", 0.9790008602318434),
    ("=CHIDIST(0.1,30)", 1.0),
    ("=TDIST(0.1,30,1)", 0.4605048058951356),
    ("=FDIST(0.1,30,7)", 0.9999978304542261),
    ("=CHIDIST(2,1)", 0.15729920705028105),
    ("=TDIST(2,1,1)", 0.14758361765043326),
    ("=FDIST(2,1,7)", 0.20020007416624017),
    ("=CHIDIST(2,4)", 0.7357588823428847),
    ("=TDIST(2,4,1)", 0.05805826175840778),
    ("=FDIST(2,4,7)", 0.1990219283583713),
    ("=CHIDIST(2,30)", 0.9999999999997),
    ("=TDIST(2,30,1)", 0.027312522481491547),
    ("=FDIST(2,30,7)", 0.1730390416788052),
    ("=CHIDIST(10,1)", 0.001565402258002549),
    ("=TDIST(10,1,1)", 0.03172551743055357),
    ("=FDIST(10,1,7)", 0.01587780383518859),
    ("=CHIDIST(10,4)", 0.04042768199451279),
    ("=TDIST(10,4,1)", 0.0002810018113579955),
    ("=FDIST(10,4,7)", 0.0050727608200323675),
    ("=CHIDIST(10,30)", 0.9997737463238232),
    ("=TDIST(10,30,1)", 2.287625704114809e-11),
    ("=FDIST(10,30,7)", 0.0020624365675693855),
    ("=CHIDIST(40,1)", 2.5396285894708634e-10),
    ("=TDIST(40,1,1)", 0.007956089912025812),
    ("=FDIST(40,1,7)", 0.00039473814862792973),
    ("=CHIDIST(40,4)", 4.328422607120966e-08),
    ("=TDIST(40,4,1)", 1.1670081613006339e-06),
    ("=FDIST(40,4,7)", 6.563771150747113e-05),
    ("=CHIDIST(40,30)", 0.10486428110798468),
    ("=TDIST(40,30,1)", 6.863022597203209e-28),
    ("=FDIST(40,30,7)", 2.060909068255183e-05),
];

#[test]
fn legacy_tails_and_inverses_match_reference_grid() {
    let mut e = engine();
    let mut failures = Vec::new();
    for (formula, expected) in REFERENCE_GRID {
        match eval_in(&mut e, formula) {
            LiteralValue::Number(got)
                if (got - expected).abs() <= 1e-8 * expected.abs().max(1e-300) => {}
            other => failures.push(format!("{formula}: got {other:?}, expected {expected}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn legacy_names_and_xlfn_modern_names_both_resolve() {
    for (legacy, modern) in [
        ("=NORMSDIST(0.7)", "=_xlfn.NORM.S.DIST(0.7,TRUE)"),
        ("=NORMSINV(0.3)", "=_xlfn.NORM.S.INV(0.3)"),
        ("=NORMDIST(1,0.5,2,TRUE)", "=_xlfn.NORM.DIST(1,0.5,2,TRUE)"),
        ("=NORMINV(0.3,1,2)", "=_xlfn.NORM.INV(0.3,1,2)"),
        ("=TDIST(1.3,7,2)", "=_xlfn.T.DIST.2T(1.3,7)"),
        ("=TDIST(1.3,7,1)", "=_xlfn.T.DIST.RT(1.3,7)"),
        ("=TINV(0.2,7)", "=_xlfn.T.INV.2T(0.2,7)"),
        ("=CHIDIST(3,4)", "=_xlfn.CHISQ.DIST.RT(3,4)"),
        ("=CHIINV(0.3,4)", "=_xlfn.CHISQ.INV.RT(0.3,4)"),
        ("=FDIST(2,3,5)", "=_xlfn.F.DIST.RT(2,3,5)"),
        ("=FINV(0.2,3,5)", "=_xlfn.F.INV.RT(0.2,3,5)"),
        ("=BETADIST(0.3,2,3)", "=_xlfn.BETA.DIST(0.3,2,3,TRUE)"),
        ("=BETAINV(0.3,2,3)", "=_xlfn.BETA.INV(0.3,2,3)"),
        ("=GAMMADIST(2,3,1,TRUE)", "=_xlfn.GAMMA.DIST(2,3,1,TRUE)"),
        ("=GAMMAINV(0.4,3,1)", "=_xlfn.GAMMA.INV(0.4,3,1)"),
        ("=LOGNORMDIST(2,0.5,1)", "=_xlfn.LOGNORM.DIST(2,0.5,1,TRUE)"),
        ("=LOGINV(0.4,0.5,1)", "=_xlfn.LOGNORM.INV(0.4,0.5,1)"),
        ("=POISSON(3,2,TRUE)", "=_xlfn.POISSON.DIST(3,2,TRUE)"),
        (
            "=BINOMDIST(3,8,0.4,TRUE)",
            "=_xlfn.BINOM.DIST(3,8,0.4,TRUE)",
        ),
        ("=EXPONDIST(1,2,TRUE)", "=_xlfn.EXPON.DIST(1,2,TRUE)"),
        ("=WEIBULL(1,2,3,TRUE)", "=_xlfn.WEIBULL.DIST(1,2,3,TRUE)"),
        (
            "=HYPGEOMDIST(1,4,8,20)",
            "=_xlfn.HYPGEOM.DIST(1,4,8,20,FALSE)",
        ),
        (
            "=NEGBINOMDIST(3,2,0.4)",
            "=_xlfn.NEGBINOM.DIST(3,2,0.4,FALSE)",
        ),
        ("=CRITBINOM(6,0.5,0.75)", "=_xlfn.BINOM.INV(6,0.5,0.75)"),
    ] {
        let a = eval(legacy);
        let b = eval(modern);
        let (LiteralValue::Number(x), LiteralValue::Number(y)) = (&a, &b) else {
            panic!("{legacy} => {a:?}, {modern} => {b:?}");
        };
        assert!(
            (x - y).abs() <= 1e-12 * y.abs().max(1.0),
            "{legacy} = {x} but {modern} = {y}"
        );
    }
}

#[test]
fn legacy_names_are_case_insensitive_and_nest() {
    // The corpus's Black-Scholes sheet: price = S*N(d1) - K*e^(-rt)*N(d2).
    check(
        "=100*normsdist(0.35)-95*EXP(-0.05)*NormSDist(0.15)",
        100.0 * 0.636_830_651_175_619 - 95.0 * (-0.05f64).exp() * 0.559_617_692_370_242_5,
        1e-9,
    );
}
