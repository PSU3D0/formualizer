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

#[cfg(any(feature = "umya", feature = "umya3"))]
mod formula_cache;
#[cfg(any(feature = "umya", feature = "umya3"))]
pub use formula_cache::{FormulaCacheUpdate, FormulaCacheUpdateRef};

#[cfg(feature = "csv")]
pub mod csv;

#[cfg(feature = "csv")]
pub use csv::CsvAdapter;
