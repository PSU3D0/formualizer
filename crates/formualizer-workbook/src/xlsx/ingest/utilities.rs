//! Pinned text-utility boundary; no workbook reader APIs are used here.
#[cfg(test)]
thread_local! {
    static UTILITY_CALLS: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(all(test, feature = "xlsx-recalc"))]
pub(crate) fn take_utility_calls() -> (u64, u64) {
    UTILITY_CALLS.with(|calls| calls.replace((0, 0)))
}

/// Expand an uncertified shared-formula placement using the pinned utility.
pub(crate) fn expand_shared_formula_into(
    formula: &str,
    start: (u32, u32),
    end: (u32, u32),
    out: &mut String,
) -> Result<(), crate::xlsx::types::XlsxError> {
    #[cfg(test)]
    UTILITY_CALLS.with(|calls| {
        let (e, p) = calls.get();
        calls.set((e + 1, p));
    });
    crate::xlsx::types::expand_shared_formula_into(formula, start, end, out)
}

/// Parse a cache error token using the pinned compatibility vocabulary.
#[cfg(any(test, feature = "xlsx-recalc"))]
pub(crate) fn parse_error_token(
    token: &str,
) -> Result<crate::xlsx::types::CellErrorType, crate::xlsx::types::XlsxError> {
    #[cfg(test)]
    UTILITY_CALLS.with(|calls| {
        let (e, p) = calls.get();
        calls.set((e, p + 1));
    });
    token.parse()
}
