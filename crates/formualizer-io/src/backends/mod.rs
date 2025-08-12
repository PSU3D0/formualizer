#[cfg(feature = "calamine")]
pub mod calamine;

#[cfg(feature = "calamine")]
pub use calamine::CalamineAdapter;

#[cfg(feature = "json")]
pub mod json;

#[cfg(feature = "json")]
pub use json::JsonAdapter;

#[cfg(feature = "umya")]
pub mod umya;

#[cfg(feature = "umya")]
pub use umya::UmyaAdapter;
