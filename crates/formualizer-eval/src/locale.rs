/// Minimal locale model for culture-invariant parsing and case folding.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Locale;

impl Locale {
    pub const fn invariant() -> Self {
        Locale
    }

    /// Parse a number using invariant rules (ASCII, dot decimal separator).
    pub fn parse_number_invariant(&self, s: &str) -> Option<f64> {
        s.trim().parse::<f64>().ok()
    }

    /// Case folding for comparisons; invariant = ASCII lower.
    pub fn fold_case_invariant(&self, s: &str) -> String {
        s.to_ascii_lowercase()
    }
}
