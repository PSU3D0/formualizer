use super::*;
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::{ASTNode, ASTNodeType};
use std::sync::Arc;

fn evaluate_slice(name: &str, values: &[f64]) -> LiteralValue {
    let wb = TestWorkbook::new()
        .with_function(Arc::new(PmtFn))
        .with_function(Arc::new(IpmtFn))
        .with_function(Arc::new(PpmtFn))
        .with_function(Arc::new(CumipmtFn))
        .with_function(Arc::new(CumprincFn));
    let ctx = wb.interpreter();
    let nodes: Vec<_> = values
        .iter()
        .map(|&v| ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(v)), None))
        .collect();
    let args: Vec<_> = nodes.iter().map(|n| ArgumentHandle::new(n, &ctx)).collect();
    ctx.context
        .get_function("", name)
        .unwrap()
        .dispatch(&args, &ctx.function_context(None))
        .unwrap()
        .into_literal()
}

fn evaluate(name: &str, values: [f64; 6]) -> LiteralValue {
    evaluate_slice(name, &values)
}

fn amount_slice(name: &str, values: &[f64]) -> f64 {
    match evaluate_slice(name, values) {
        LiteralValue::Number(v) => v,
        other => panic!("{name}: expected number, got {other:?}"),
    }
}

fn amount(name: &str, values: [f64; 6]) -> f64 {
    amount_slice(name, &values)
}

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-7,
        "actual={actual}, expected={expected}"
    );
}

// Independent positive-cash-flow amortization schedule, not PMT/IPMT/PPMT.
// An annuity-due payment occurs at time zero; each later payment covers
// interest accrued on the balance remaining after the previous payment.
fn schedule(rate: f64, nper: usize, pv: f64, beginning: bool) -> Vec<(f64, f64)> {
    let payment = pv * rate
        / (1.0 - (1.0 + rate).powf(-(nper as f64)))
        / if beginning { 1.0 + rate } else { 1.0 };
    let mut balance = pv;
    (0..nper)
        .map(|period| {
            let interest = if beginning && period == 0 {
                0.0
            } else {
                balance * rate
            };
            let principal = payment - interest;
            balance -= principal;
            (-interest, -principal)
        })
        .collect()
}

#[test]
fn cumulative_loan_contract_interest() {
    close(
        amount("CUMIPMT", [0.005, 360.0, 100000.0, 1.0, 12.0, 0.0]),
        -5966.594589556309,
    );
}

#[test]
fn cumulative_loan_contract_principal() {
    close(
        amount("CUMPRINC", [0.005, 360.0, 100000.0, 1.0, 12.0, 0.0]),
        -1228.011712276774,
    );
}

#[test]
fn cumulative_loan_end_of_period_recurrence() {
    matches_timing_specific_recurrence(false);
}

#[test]
fn cumulative_loan_beginning_of_period_recurrence() {
    matches_timing_specific_recurrence(true);
}

fn matches_timing_specific_recurrence(beginning: bool) {
    for (rate, nper, pv) in [(0.005, 360, 100000.0), (0.03, 24, 2500.0)] {
        let oracle = schedule(rate, nper, pv, beginning);
        for (start, end) in [(1, 1), (2, 2), (12, 12), (1, 12), (13, 24), (1, nper)] {
            let args = [
                rate,
                nper as f64,
                pv,
                start as f64,
                end as f64,
                beginning as u8 as f64,
            ];
            close(
                amount("CUMIPMT", args),
                oracle[start - 1..end].iter().map(|p| p.0).sum(),
            );
            close(
                amount("CUMPRINC", args),
                oracle[start - 1..end].iter().map(|p| p.1).sum(),
            );
        }
        close(
            amount(
                "CUMPRINC",
                [
                    rate,
                    nper as f64,
                    pv,
                    1.0,
                    nper as f64,
                    beginning as u8 as f64,
                ],
            ),
            -pv,
        );
        for name in ["CUMIPMT", "CUMPRINC"] {
            let args = [rate, nper as f64, pv, 1.0, 24.0, beginning as u8 as f64];
            close(
                amount(name, args),
                amount(name, [rate, nper as f64, pv, 1.0, 12.0, args[5]])
                    + amount(name, [rate, nper as f64, pv, 13.0, 24.0, args[5]]),
            );
        }
    }
}

#[test]
fn cumulative_loan_single_payment_timing() {
    for timing in [0.0, 1.0] {
        close(
            amount("CUMIPMT", [0.07, 1.0, 900.0, 1.0, 1.0, timing]),
            if timing == 0.0 { -63.0 } else { 0.0 },
        );
        close(
            amount("CUMPRINC", [0.07, 1.0, 900.0, 1.0, 1.0, timing]),
            -900.0,
        );
    }
}

#[test]
fn cumulative_loan_fractional_arguments_truncate() {
    for name in ["CUMIPMT", "CUMPRINC"] {
        for timing in [0.0, 1.0] {
            close(
                amount(name, [0.005, 360.9, 100000.0, 1.9, 12.9, timing + 0.9]),
                amount(name, [0.005, 360.0, 100000.0, 1.0, 12.0, timing]),
            );
        }
    }
}

#[test]
fn cumulative_loan_invalid_domains() {
    for name in ["CUMIPMT", "CUMPRINC"] {
        for (index, invalid) in [
            (0, 0.0),
            (0, -0.005),
            (1, 0.0),
            (1, -1.0),
            (1, 0.9),
            (2, 0.0),
            (2, -100000.0),
            (3, 0.0),
            (3, 0.9),
            (3, -1.0),
            (3, 13.0),
            (4, 0.0),
            (4, 361.0),
            (5, -1.0),
            (5, 2.0),
        ] {
            let mut args = [0.005, 360.0, 100000.0, 1.0, 12.0, 0.0];
            args[index] = invalid;
            assert!(
                matches!(evaluate(name, args), LiteralValue::Error(e) if e.kind == ExcelErrorKind::Num),
                "{name}: {args:?}"
            );
        }
    }
}

#[test]
fn ipmt_ppmt_type1_annuity_due_repro() {
    let rate = 0.05 / 12.0;
    let nper = 24.0;
    let pv = 5000.0;

    // Period 1 with type=1 has zero interest; full payment goes to principal.
    close(amount("IPMT", [rate, 1.0, nper, pv, 0.0, 1.0]), 0.0);
    let pmt_t1 = amount_slice("PMT", &[rate, nper, pv, 0.0, 1.0]);
    close(amount("PPMT", [rate, 1.0, nper, pv, 0.0, 1.0]), pmt_t1);

    // Period 2 with type=1 must deduct period 1's payment from the balance.
    close(
        amount("IPMT", [rate, 2.0, nper, pv, 0.0, 1.0]),
        -19.923138525572575,
    );
    close(
        amount("PPMT", [rate, 2.0, nper, pv, 0.0, 1.0]),
        -198.52361533700965,
    );

    // Matches CUMIPMT / CUMPRINC single-period values for type=1.
    for per in 1..=24 {
        let p = per as f64;
        close(
            amount("IPMT", [rate, p, nper, pv, 0.0, 1.0]),
            amount("CUMIPMT", [rate, nper, pv, p, p, 1.0]),
        );
        close(
            amount("PPMT", [rate, p, nper, pv, 0.0, 1.0]),
            amount("CUMPRINC", [rate, nper, pv, p, p, 1.0]),
        );
    }
}

#[test]
fn ipmt_ppmt_identities_type0_and_type1() {
    let cases = [
        (0.05 / 12.0, 24usize, 5000.0, 0.0),
        (0.1, 4, 1000.0, -500.0),
        (0.05 / 12.0, 24, 5000.0, -1000.0),
        (-0.02, 12, 1000.0, 0.0),
        (-0.02, 12, 1000.0, -250.0),
        (0.0, 12, 1000.0, 0.0),
        (0.0, 12, 1000.0, -200.0),
    ];

    for (rate, nper, pv, fv) in cases {
        let nper_f = nper as f64;
        for pmt_type in [0.0, 1.0] {
            let pmt = amount_slice("PMT", &[rate, nper_f, pv, fv, pmt_type]);
            let mut sum_ppmt = 0.0;

            for per in 1..=nper {
                let p = per as f64;
                let ipmt = amount("IPMT", [rate, p, nper_f, pv, fv, pmt_type]);
                let ppmt = amount("PPMT", [rate, p, nper_f, pv, fv, pmt_type]);

                // IPMT + PPMT == PMT
                close(ipmt + ppmt, pmt);

                if pmt_type == 1.0 {
                    if per == 1 {
                        assert_eq!(ipmt, 0.0);
                    } else if rate != 0.0 {
                        // Exact Excel identity: IPMT(type=1) = IPMT(type=0) / (1 + rate)
                        let ipmt_t0 = amount("IPMT", [rate, p, nper_f, pv, fv, 0.0]);
                        close(ipmt, ipmt_t0 / (1.0 + rate));
                    } else {
                        assert_eq!(ipmt, 0.0);
                    }
                } else if rate == 0.0 {
                    assert_eq!(ipmt, 0.0);
                }

                sum_ppmt += ppmt;
            }

            // Sum of principal payments over 1..=nper:
            // For type=0 (or rate=0), ending balance at t=nper is -fv, so sum(PPMT) == -pv - fv.
            // For type=1 with rate != 0, payments stop at t=nper-1 and fv compounds one more
            // period to t=nper, so balance after the final payment is -fv / (1 + rate).
            let expected_principal_sum = if pmt_type == 1.0 && rate != 0.0 {
                -pv - fv / (1.0 + rate)
            } else {
                -pv - fv
            };
            close(sum_ppmt, expected_principal_sum);
        }
    }
}

#[test]
fn ipmt_ppmt_non_zero_fv_and_negative_rate_values() {
    // Verified against LibreOffice Calc
    close(amount("IPMT", [0.1, 1.0, 4.0, 1000.0, -500.0, 1.0]), 0.0);
    close(
        amount("IPMT", [0.1, 2.0, 4.0, 1000.0, -500.0, 1.0]),
        -81.1149634679047,
    );
    close(
        amount("IPMT", [0.1, 4.0, 4.0, 1000.0, -500.0, 1.0]),
        -58.4905290787644,
    );
    close(
        amount("PPMT", [0.1, 2.0, 4.0, 1000.0, -500.0, 1.0]),
        -107.735401853049,
    );
    close(
        amount("IPMT", [-0.02, 2.0, 12.0, 1000.0, 0.0, 1.0]),
        18.5122274899544,
    );
    assert_eq!(amount("IPMT", [0.0, 2.0, 12.0, 1000.0, -200.0, 1.0]), 0.0);
    close(
        amount("PPMT", [0.0, 2.0, 12.0, 1000.0, -200.0, 1.0]),
        -800.0 / 12.0,
    );
}
