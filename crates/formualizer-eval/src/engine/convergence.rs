//! Per-member convergence test for `CyclePolicy::Iterate` (RFC #113,
//! spec `formualizer-cycle-semantics-spec.md` §6).
//!
//! Compares one SCC member's pass-*N* value against its pass-*N−1* value.
//! The rules are engine invariants, deliberately NOT configurable (spec §2
//! configurability rationale):
//!
//! | value pair | converged iff |
//! |---|---|
//! | numeric-class × numeric-class (Number/Int/Date/DateTime/Time/Duration) | `\|Δ\| < max_change` on f64 serials (absolute, strict) |
//! | Boolean × Boolean | equal |
//! | Text × Text | exactly equal (case-sensitive, no trim) |
//! | Error × Error | same `ExcelErrorKind` |
//! | Empty × Empty | converged |
//! | Array × Array | element-wise scalar rules; shape change ⇒ not converged |
//! | any type transition | not converged |
//!
//! NaN: identical-bit-pattern NaN vs NaN is **converged** (avoids permanent
//! caps from NaN leakage — the engine has no central NaN→`#NUM!` coercion)
//! and flags telemetry; NaN vs anything else is not converged.

use formualizer_common::{
    DateSystem, LiteralValue, date_to_serial_for, datetime_to_serial_for, time_to_fraction,
};

/// Outcome of one member comparison.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ConvergenceOutcome {
    pub converged: bool,
    /// `|Δ|` for numeric-class comparisons (feeds telemetry
    /// `max_abs_delta_at_stop`); `None` for non-numeric pairs and for
    /// NaN-involved comparisons (no meaningful delta).
    pub abs_delta: Option<f64>,
    /// An identical-bit NaN vs NaN pair converged (telemetry flag, spec §6).
    pub nan_converged: bool,
}

impl ConvergenceOutcome {
    fn converged() -> Self {
        Self {
            converged: true,
            abs_delta: None,
            nan_converged: false,
        }
    }
    fn not_converged() -> Self {
        Self {
            converged: false,
            abs_delta: None,
            nan_converged: false,
        }
    }
}

/// f64 serial for a numeric-class value; `None` for every other type.
/// Boolean is NOT numeric-class (spec §6: Boolean compares by equality, and
/// Boolean↔Number is a type transition).
fn numeric_serial(v: &LiteralValue, date_system: DateSystem) -> Option<f64> {
    match v {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        LiteralValue::Date(d) => Some(date_to_serial_for(date_system, d)),
        LiteralValue::DateTime(dt) => Some(datetime_to_serial_for(date_system, dt)),
        LiteralValue::Time(t) => Some(time_to_fraction(t)),
        LiteralValue::Duration(d) => Some(d.num_seconds() as f64 / 86_400.0),
        _ => None,
    }
}

/// Spec-§6 convergence test for one member: `prev` is the pass-*N−1* value,
/// `cur` the pass-*N* value.
pub(crate) fn values_converged(
    prev: &LiteralValue,
    cur: &LiteralValue,
    max_change: f64,
    date_system: DateSystem,
) -> ConvergenceOutcome {
    // Numeric-class pairs first: Int↔Number↔DateTime etc. cross-coerce on
    // f64 serials and are NOT type transitions.
    if let (Some(a), Some(b)) = (
        numeric_serial(prev, date_system),
        numeric_serial(cur, date_system),
    ) {
        if a.is_nan() || b.is_nan() {
            // Identical-bit NaN vs NaN converges (telemetry-flagged);
            // anything else involving NaN does not.
            if a.to_bits() == b.to_bits() {
                return ConvergenceOutcome {
                    converged: true,
                    abs_delta: None,
                    nan_converged: true,
                };
            }
            return ConvergenceOutcome::not_converged();
        }
        let delta = (b - a).abs();
        return ConvergenceOutcome {
            // Strict `<`, absolute (Excel semantics). A non-finite delta
            // (Inf-Inf etc.) is NaN or Inf and correctly fails this test.
            converged: delta < max_change,
            abs_delta: Some(delta),
            nan_converged: false,
        };
    }

    match (prev, cur) {
        (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => {
            if a == b {
                ConvergenceOutcome::converged()
            } else {
                ConvergenceOutcome::not_converged()
            }
        }
        (LiteralValue::Text(a), LiteralValue::Text(b)) => {
            // Exactly equal: case-sensitive, no trim.
            if a == b {
                ConvergenceOutcome::converged()
            } else {
                ConvergenceOutcome::not_converged()
            }
        }
        (LiteralValue::Error(a), LiteralValue::Error(b)) => {
            if a.kind == b.kind {
                ConvergenceOutcome::converged()
            } else {
                ConvergenceOutcome::not_converged()
            }
        }
        (LiteralValue::Empty, LiteralValue::Empty) => ConvergenceOutcome::converged(),
        (LiteralValue::Array(a), LiteralValue::Array(b)) => {
            // Element-wise with the scalar rules; shape change ⇒ not
            // converged (fixed rule — spill anchors are pre-stamped in SCCs,
            // so this only sees non-spilling array results).
            if a.len() != b.len() || a.iter().zip(b.iter()).any(|(ra, rb)| ra.len() != rb.len()) {
                return ConvergenceOutcome::not_converged();
            }
            let mut out = ConvergenceOutcome::converged();
            for (ra, rb) in a.iter().zip(b.iter()) {
                for (ea, eb) in ra.iter().zip(rb.iter()) {
                    let e = values_converged(ea, eb, max_change, date_system);
                    if !e.converged {
                        return ConvergenceOutcome::not_converged();
                    }
                    out.nan_converged |= e.nan_converged;
                    out.abs_delta = match (out.abs_delta, e.abs_delta) {
                        (Some(x), Some(y)) => Some(x.max(y)),
                        (x, y) => x.or(y),
                    };
                }
            }
            out
        }
        // Any type transition (incl. Empty↔anything, Error↔value,
        // Number↔Text, Boolean↔Number) — and the defensive `Pending`
        // variant, which must never be committed as an SCC value.
        _ => ConvergenceOutcome::not_converged(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, NaiveTime};
    use formualizer_common::{ExcelError, ExcelErrorKind};

    const DS: DateSystem = DateSystem::Excel1900;

    fn conv(prev: &LiteralValue, cur: &LiteralValue) -> ConvergenceOutcome {
        values_converged(prev, cur, 0.001, DS)
    }

    /* ── §6 row 1: numeric-class, |Δ| < max_change, strict + absolute ── */

    #[test]
    fn numbers_within_max_change_converge_strictly() {
        let a = LiteralValue::Number(1.0);
        assert!(conv(&a, &LiteralValue::Number(1.0009)).converged);
        assert!(!conv(&a, &LiteralValue::Number(1.1)).converged);
        // |Δ| == max_change is NOT converged (strict `<`) — exercised on an
        // exactly-representable boundary (0.5) to avoid float-rounding
        // artifacts in the test itself.
        assert!(
            !values_converged(
                &LiteralValue::Number(1.0),
                &LiteralValue::Number(1.5),
                0.5,
                DS
            )
            .converged
        );
        assert!(
            values_converged(
                &LiteralValue::Number(1.0),
                &LiteralValue::Number(1.25),
                0.5,
                DS
            )
            .converged
        );
        // Absolute, not relative: same |Δ| rule at large magnitudes.
        assert!(
            !values_converged(
                &LiteralValue::Number(1.0e9),
                &LiteralValue::Number(1.0e9 + 0.002),
                0.001,
                DS
            )
            .converged
        );
        assert_eq!(
            conv(&a, &LiteralValue::Number(1.0009)).abs_delta,
            Some(1.0009 - 1.0)
        );
    }

    #[test]
    fn int_number_cross_coercion_is_numeric_class_not_a_transition() {
        assert!(conv(&LiteralValue::Int(5), &LiteralValue::Number(5.0005)).converged);
        assert!(conv(&LiteralValue::Number(5.0005), &LiteralValue::Int(5)).converged);
        assert!(!conv(&LiteralValue::Int(5), &LiteralValue::Number(5.5)).converged);
        assert!(conv(&LiteralValue::Int(7), &LiteralValue::Int(7)).converged);
        assert!(!conv(&LiteralValue::Int(7), &LiteralValue::Int(8)).converged);
    }

    #[test]
    fn datetime_and_duration_compare_on_f64_serials() {
        let d1 = LiteralValue::Date(NaiveDate::from_ymd_opt(2026, 6, 9).unwrap());
        let d2 = LiteralValue::Date(NaiveDate::from_ymd_opt(2026, 6, 10).unwrap());
        assert!(conv(&d1, &d1.clone()).converged);
        assert!(!conv(&d1, &d2).converged); // Δ = 1 day = 1.0
        // Date vs its own serial Number: numeric-class cross-coercion.
        let serial = date_to_serial_for(DS, &NaiveDate::from_ymd_opt(2026, 6, 9).unwrap());
        assert!(conv(&d1, &LiteralValue::Number(serial)).converged);
        // Durations: 43 seconds = ~0.0005 days < 0.001.
        let s43 = LiteralValue::Duration(Duration::seconds(43));
        assert!(conv(&LiteralValue::Duration(Duration::zero()), &s43).converged);
        let s130 = LiteralValue::Duration(Duration::seconds(130));
        assert!(!conv(&LiteralValue::Duration(Duration::zero()), &s130).converged);
        // Time-of-day fractions.
        let t1 = LiteralValue::Time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let t2 = LiteralValue::Time(NaiveTime::from_hms_opt(0, 0, 43).unwrap());
        assert!(conv(&t1, &t2).converged);
    }

    /* ── §6 rows 2–5: Boolean / Text / Error / Empty ── */

    #[test]
    fn booleans_converge_on_equality_only() {
        let t = LiteralValue::Boolean(true);
        let f = LiteralValue::Boolean(false);
        assert!(conv(&t, &t.clone()).converged);
        assert!(!conv(&t, &f).converged);
    }

    #[test]
    fn text_is_exact_case_sensitive_no_trim() {
        let a = LiteralValue::Text("abc".into());
        assert!(conv(&a, &LiteralValue::Text("abc".into())).converged);
        assert!(!conv(&a, &LiteralValue::Text("ABC".into())).converged);
        assert!(!conv(&a, &LiteralValue::Text("abc ".into())).converged);
    }

    #[test]
    fn errors_converge_on_same_kind_only() {
        let div = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div));
        let div2 =
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div).with_message("other msg"));
        let na = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na));
        assert!(conv(&div, &div2).converged, "same kind, message ignored");
        assert!(!conv(&div, &na).converged);
    }

    #[test]
    fn empty_vs_empty_converges() {
        assert!(conv(&LiteralValue::Empty, &LiteralValue::Empty).converged);
    }

    /* ── §6 row 6: any type transition ── */

    #[test]
    fn type_transitions_never_converge() {
        let n = LiteralValue::Number(0.0);
        let cases: Vec<(LiteralValue, LiteralValue)> = vec![
            (n.clone(), LiteralValue::Text("0".into())),
            (LiteralValue::Text("0".into()), n.clone()),
            (LiteralValue::Empty, n.clone()),
            (n.clone(), LiteralValue::Empty),
            (
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
                n.clone(),
            ),
            (
                n.clone(),
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
            ),
            (LiteralValue::Boolean(true), LiteralValue::Int(1)),
            (LiteralValue::Empty, LiteralValue::Text(String::new())),
            (n.clone(), LiteralValue::Array(vec![vec![n.clone()]])),
        ];
        for (prev, cur) in cases {
            assert!(
                !values_converged(&prev, &cur, f64::MAX, DS).converged,
                "{prev:?} → {cur:?} must not converge even with a huge max_change"
            );
        }
    }

    /* ── §6 NaN rule ── */

    #[test]
    fn identical_bit_nan_converges_and_sets_flag() {
        let nan = LiteralValue::Number(f64::NAN);
        let out = conv(&nan, &LiteralValue::Number(f64::NAN));
        assert!(out.converged);
        assert!(out.nan_converged);
        assert_eq!(out.abs_delta, None);
    }

    #[test]
    fn different_bit_nan_and_nan_vs_value_do_not_converge() {
        let nan = f64::NAN;
        let other_nan = f64::from_bits(nan.to_bits() ^ 1);
        assert!(other_nan.is_nan());
        let out = conv(&LiteralValue::Number(nan), &LiteralValue::Number(other_nan));
        assert!(!out.converged);
        assert!(!out.nan_converged);
        assert!(!conv(&LiteralValue::Number(nan), &LiteralValue::Number(1.0)).converged);
        assert!(!conv(&LiteralValue::Number(1.0), &LiteralValue::Number(nan)).converged);
    }

    /* ── §6 row 7: arrays ── */

    #[test]
    fn arrays_compare_element_wise_with_scalar_rules() {
        let a = LiteralValue::Array(vec![vec![
            LiteralValue::Number(1.0),
            LiteralValue::Text("x".into()),
        ]]);
        let close = LiteralValue::Array(vec![vec![
            LiteralValue::Number(1.0005),
            LiteralValue::Text("x".into()),
        ]]);
        let far = LiteralValue::Array(vec![vec![
            LiteralValue::Number(2.0),
            LiteralValue::Text("x".into()),
        ]]);
        let out = conv(&a, &close);
        assert!(out.converged);
        assert_eq!(out.abs_delta, Some(1.0005 - 1.0));
        assert!(!conv(&a, &far).converged);
    }

    #[test]
    fn array_shape_change_does_not_converge() {
        let one = LiteralValue::Array(vec![vec![LiteralValue::Number(1.0)]]);
        let two = LiteralValue::Array(vec![vec![
            LiteralValue::Number(1.0),
            LiteralValue::Number(1.0),
        ]]);
        let tall = LiteralValue::Array(vec![
            vec![LiteralValue::Number(1.0)],
            vec![LiteralValue::Number(1.0)],
        ]);
        assert!(!conv(&one, &two).converged);
        assert!(!conv(&one, &tall).converged);
    }

    #[test]
    fn max_change_zero_means_only_exact_numeric_repeats_converge() {
        // Valid config (>= 0): strict `<` means only |Δ| < 0 would pass,
        // i.e. numeric values never converge; exact equality of Δ = 0 fails.
        let a = LiteralValue::Number(1.0);
        assert!(!values_converged(&a, &a.clone(), 0.0, DS).converged);
        // Non-numeric rules are unaffected by max_change.
        assert!(
            values_converged(
                &LiteralValue::Text("x".into()),
                &LiteralValue::Text("x".into()),
                0.0,
                DS
            )
            .converged
        );
    }
}
