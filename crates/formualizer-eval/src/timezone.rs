/// Timezone support for date/time functions
use chrono::{Local, NaiveDateTime, Utc};

/// Timezone specification for date/time calculations
/// Excel behavior: always uses local timezone
/// This enum allows future extensions while maintaining Excel compatibility
#[derive(Clone, Debug, Default)]
pub enum TimeZoneSpec {
    /// Use the system's local timezone (Excel default behavior)
    #[default]
    Local,
    /// Use UTC timezone
    Utc,
    // Named timezone variant removed until feature introduced.
}

// (Derived Default provides Local)

impl TimeZoneSpec {
    /// Get the current datetime in the specified timezone
    pub fn now(&self) -> NaiveDateTime {
        match self {
            TimeZoneSpec::Local => Local::now().naive_local(),
            TimeZoneSpec::Utc => Utc::now().naive_utc(),
        }
    }

    /// Get today's date in the specified timezone
    pub fn today(&self) -> chrono::NaiveDate {
        self.now().date()
    }
}
