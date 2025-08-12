/// Timezone support for date/time functions
use chrono::{Local, NaiveDateTime, Utc};

/// Timezone specification for date/time calculations
/// Excel behavior: always uses local timezone
/// This enum allows future extensions while maintaining Excel compatibility
#[derive(Clone, Debug)]
pub enum TimeZoneSpec {
    /// Use the system's local timezone (Excel default behavior)
    Local,
    /// Use UTC timezone
    Utc,
    /// Use a specific IANA timezone (e.g., "America/New_York")
    /// Currently requires chrono-tz feature to be enabled
    #[cfg(feature = "chrono-tz")]
    Named(chrono_tz::Tz),
}

impl Default for TimeZoneSpec {
    fn default() -> Self {
        // Default to Excel behavior
        TimeZoneSpec::Local
    }
}

impl TimeZoneSpec {
    /// Get the current datetime in the specified timezone
    pub fn now(&self) -> NaiveDateTime {
        match self {
            TimeZoneSpec::Local => Local::now().naive_local(),
            TimeZoneSpec::Utc => Utc::now().naive_utc(),
            #[cfg(feature = "chrono-tz")]
            TimeZoneSpec::Named(tz) => {
                use chrono::TimeZone;
                tz.from_utc_datetime(&Utc::now().naive_utc()).naive_local()
            }
        }
    }

    /// Get today's date in the specified timezone
    pub fn today(&self) -> chrono::NaiveDate {
        self.now().date()
    }
}
