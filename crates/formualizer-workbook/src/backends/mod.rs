#[cfg(feature = "calamine")]
pub mod calamine;

#[cfg(feature = "calamine")]
pub use calamine::{CalamineAdapter, XlsxPathSource};

#[cfg(feature = "json")]
pub mod json;

#[cfg(feature = "json")]
pub use json::JsonAdapter;

#[cfg(feature = "umya")]
pub mod umya;

#[cfg(feature = "umya")]
pub use umya::UmyaAdapter;

// The shared implementation intentionally uses accessors available in both
// Umya 2 and 3. They remain supported but are marked deprecated by Umya 3.
#[cfg(feature = "umya3")]
#[allow(deprecated)]
pub mod umya3;
#[cfg(feature = "umya3")]
pub use umya3::UmyaAdapter as Umya3Adapter;

#[cfg(any(feature = "umya", feature = "umya3", feature = "json"))]
mod formula_grouping;

#[cfg(any(feature = "umya", feature = "umya3"))]
mod formula_cache;
#[cfg(any(feature = "umya", feature = "umya3"))]
pub use formula_cache::{FormulaCacheUpdate, FormulaCacheUpdateRef};

#[cfg(feature = "csv")]
pub mod csv;

/// Whether a defined-name reference is a union (`A1:A5,C1:C5`): a comma
/// outside quoted sheet names. `'Sheet (A, B)'!$1:$3` is a single range.
#[cfg(any(feature = "calamine", feature = "umya", feature = "umya3"))]
pub(crate) fn has_union_comma(reference: &str) -> bool {
    let mut quoted = false;
    for c in reference.chars() {
        match c {
            // A doubled quote inside a quoted name toggles twice: no change.
            '\'' => quoted = !quoted,
            ',' if !quoted => return true,
            _ => {}
        }
    }
    false
}

#[cfg(feature = "csv")]
pub use csv::CsvAdapter;
