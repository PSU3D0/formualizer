#![cfg(test)]

use super::consts::DEFAULT_EPS;
use formualizer_common::LiteralValue;

pub fn assert_close(a: f64, b: f64) {
    assert_close_eps(a, b, DEFAULT_EPS);
}

pub fn assert_close_eps(a: f64, b: f64, eps: f64) {
    assert!((a - b).abs() < eps, "{a} !~= {b} (eps={eps})");
}

pub fn assert_error_kind(v: &LiteralValue, code: &str) {
    match v {
        LiteralValue::Error(e) => assert_eq!(e, code),
        other => panic!("expected error {code}, got {other:?}"),
    }
}
