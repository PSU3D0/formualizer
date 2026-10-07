/// Locale contract for the engine.
///
/// Milestone 0 intentionally uses an invariant locale:
///
/// - Numeric parsing is ASCII/invariant only (`.` decimal separator; no thousands separators),
///   with support for trailing percent suffix (`"90%" -> 0.9`).
/// - Strings are case-folded with ASCII-only rules (`to_ascii_lowercase`).
///
/// This means locale-dependent inputs like `"1.234,56"` are *not* interpreted as numbers.
/// Callers should surface `#VALUE!` for locale-dependent numeric coercions (e.g. `VALUE()`)
/// rather than silently producing a wrong number.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Locale;

impl Locale {
    pub const fn invariant() -> Self {
        Locale
    }

    /// Parse a number using invariant rules (ASCII, dot decimal separator).
    ///
    /// Also supports percent-suffixed numeric text (e.g. "90%" -> 0.9),
    /// matching spreadsheet numeric-coercion behavior in numeric contexts.
    ///
    /// Only finite numbers are numbers to Excel: the spellings Rust's float
    /// parser also accepts (`"NaN"`, `"inf"`, `"-Infinity"`) and text that
    /// overflows (`"1e400"`) are rejected, so they stay text (`#VALUE!` in
    /// arithmetic).
    pub fn parse_number_invariant(&self, s: &str) -> Option<f64> {
        let trimmed = s.trim();
        let n = if let Some(without_pct) = trimmed.strip_suffix('%') {
            without_pct.trim().parse::<f64>().ok()? / 100.0
        } else {
            trimmed.parse::<f64>().ok()?
        };
        n.is_finite().then_some(n)
    }

    /// Case folding for comparisons; invariant = ASCII lower.
    pub fn fold_case_invariant(&self, s: &str) -> String {
        s.to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::Locale;

    #[test]
    fn parse_number_invariant_supports_percent_suffix() {
        let loc = Locale::invariant();
        assert_eq!(loc.parse_number_invariant("90%"), Some(0.9));
        assert_eq!(loc.parse_number_invariant(" 90.5% "), Some(0.905));
        assert_eq!(loc.parse_number_invariant("90 %"), Some(0.9));
    }

    #[test]
    fn parse_number_invariant_rejects_non_finite_spellings() {
        let loc = Locale::invariant();
        for text in [
            "NaN",
            "nan",
            "-NaN",
            "inf",
            "+inf",
            "-inf",
            "Infinity",
            "-infinity",
            "nan%",
            "1e400",
            "-1e400",
        ] {
            assert_eq!(loc.parse_number_invariant(text), None, "{text}");
        }
        assert_eq!(loc.parse_number_invariant("1e5"), Some(100_000.0));
        assert_eq!(loc.parse_number_invariant("-.5"), Some(-0.5));
    }

    #[test]
    fn parse_number_invariant_rejects_invalid_percent_text() {
        let loc = Locale::invariant();
        assert_eq!(loc.parse_number_invariant("abc%"), None);
        assert_eq!(loc.parse_number_invariant("%"), None);
        assert_eq!(loc.parse_number_invariant("90% trailing"), None);
    }
}
