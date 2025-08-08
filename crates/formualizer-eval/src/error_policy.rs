use formualizer_common::LiteralValue;

/// Return Some(error) if either is an Error, following left-to-right precedence.
pub fn propagate_error2(left: &LiteralValue, right: &LiteralValue) -> Option<LiteralValue> {
    match (left, right) {
        (LiteralValue::Error(_), _) => Some(left.clone()),
        (_, LiteralValue::Error(_)) => Some(right.clone()),
        _ => None,
    }
}

/// Return Some(error) if the argument is an Error.
pub fn propagate_error1(v: &LiteralValue) -> Option<LiteralValue> {
    match v {
        LiteralValue::Error(_) => Some(v.clone()),
        _ => None,
    }
}
