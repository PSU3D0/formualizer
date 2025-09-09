#[macro_export]
macro_rules! arg {
    // ----- boolean -----
    ($h:expr => bool) => {{
        use formualizer_parse::types::LiteralValue as V;
        let v = $h.value()?;
        match v.as_ref() {
            V::Boolean(b) => Ok(*b),
            V::Number(n) => Ok(*n != 0.0),
            V::Int(i) => Ok(*i != 0),
            V::Empty => Ok(false),
            V::Error(e) => Err($crate::error::ExcelError::from(e.clone())),
            _ => Err($crate::error::ExcelError::new(
                $crate::error::ExcelErrorKind::Value,
            )),
        }
    }};
    // ----- number -----
    ($h:expr => f64) => {{
        use formualizer_parse::types::LiteralValue as V;
        let v = $h.value()?;
        match v.as_ref() {
            V::Number(n) => Ok(*n),
            V::Int(i) => Ok(*i as f64),
            V::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
            V::Empty => Ok(0.0),
            V::Error(e) => Err($crate::error::ExcelError::from(e.clone())),
            _ => Err($crate::error::ExcelError::new(
                $crate::error::ExcelErrorKind::Value,
            )),
        }
    }}; // more coercions (text, date, range) as you need…
}
