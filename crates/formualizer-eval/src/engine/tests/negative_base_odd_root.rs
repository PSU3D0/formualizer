//! A negative base with a fractional exponent, for `^` and `POWER`. Excel
//! returns the real root when `1/exponent` is an odd integer (read at 15
//! significant digits) and `#NUM!` for every other fractional exponent of a
//! negative base. Expected values were measured in Excel 16.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

/// `(formula, Some(value))`, or `None` for `#NUM!`.
const CASES: &[(&str, Option<f64>)] = &[
    ("=(-8)^(1/3)", Some(-2.0)),
    ("=(-8)^(-1/3)", Some(-0.5)),
    ("=(-32)^0.2", Some(-2.0)),
    ("=(-1)^(1/7)", Some(-1.0)),
    ("=(-8)^0.333333333333333", Some(-2.0)),
    ("=POWER(-8,1/3)", Some(-2.0)),
    // -(0.5^(1/3)) - 1
    ("=(-50/100)^(1/3)-1", Some(-1.793_700_525_984_1)),
    ("=(-8)^(2/3)", None),
    ("=(-8)^(4/3)", None),
    ("=(-32)^(3/5)", None),
    ("=(-8)^(1/2)", None),
    ("=(-8)^0.3333", None),
    ("=POWER(-8,2/3)", None),
    ("=(-0.5)^(1/2)", None),
    // Integer exponents are unchanged.
    ("=(-2)^3", Some(-8.0)),
];

#[test]
fn negative_base_takes_real_odd_roots_only() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut e = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default().with_formula_plane_mode(mode),
        );
        for (i, (formula, _)) in CASES.iter().enumerate() {
            e.set_cell_formula("Sheet1", i as u32 + 1, 1, parse(formula).unwrap())
                .unwrap();
        }
        e.evaluate_all().unwrap();
        for (i, (formula, want)) in CASES.iter().enumerate() {
            let got = e.get_cell_value("Sheet1", i as u32 + 1, 1);
            match (want, &got) {
                (Some(w), Some(LiteralValue::Number(g))) => assert!(
                    (g - w).abs() < 1e-12,
                    "{formula} = {g}, Excel gives {w} ({mode:?})"
                ),
                (None, Some(LiteralValue::Error(err))) => {
                    assert_eq!(err.kind, ExcelErrorKind::Num, "{formula} ({mode:?})")
                }
                _ => panic!("{formula} = {got:?}, Excel gives {want:?} ({mode:?})"),
            }
        }
    }
}
