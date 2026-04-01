/// Clock/timezone support for date/time functions.
///
/// Note: deterministic evaluation requires that the evaluation clock is injectable.
/// Builtins should not call `Local::now()` / `Utc::now()` directly.
#[cfg(feature = "system-clock")]
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, Utc};
#[cfg(not(feature = "system-clock"))]
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};

/// Timezone specification for date/time calculations
/// Excel behavior: always uses local timezone
/// This enum allows future extensions while maintaining Excel compatibility
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum TimeZoneSpec {
    /// Use the system's local timezone (Excel default behavior)
    #[default]
    Local,
    /// Use UTC timezone
    Utc,
    /// Use a fixed offset from UTC (seconds east of UTC).
    ///
    /// This is the only timezone-like option permitted under deterministic mode.
    FixedOffsetSeconds(i32),
    // Named timezone variant removed until feature introduced.
}

// (Derived Default provides Local)

impl TimeZoneSpec {
    pub fn fixed_offset(&self) -> Option<FixedOffset> {
        match self {
            TimeZoneSpec::Utc => FixedOffset::east_opt(0),
            TimeZoneSpec::FixedOffsetSeconds(secs) => FixedOffset::east_opt(*secs),
            TimeZoneSpec::Local => None,
        }
    }

    pub fn validate_for_determinism(&self) -> Result<(), String> {
        match self {
            TimeZoneSpec::Local => Err(
                "Deterministic mode forbids `Local` timezone (use UTC or a fixed offset)"
                    .to_string(),
            ),
            TimeZoneSpec::Utc => Ok(()),
            TimeZoneSpec::FixedOffsetSeconds(secs) => {
                FixedOffset::east_opt(*secs).ok_or_else(|| {
                    format!("Invalid fixed offset: {secs} seconds (must be within +/-24h)")
                })?;
                Ok(())
            }
        }
    }
}

/// Injectable clock provider for volatile date/time builtins.
pub trait ClockProvider: std::fmt::Debug + Send + Sync {
    fn timezone(&self) -> &TimeZoneSpec;
    fn now(&self) -> NaiveDateTime;
    fn today(&self) -> NaiveDate {
        self.now().date()
    }
}

/// Default clock implementation: reads from the system clock.
///
/// Only available when the `system-clock` feature is enabled.
/// For portable wasm / raw wasmtime guests use `FixedClock` or inject your own `ClockProvider`.
#[cfg(feature = "system-clock")]
#[derive(Clone, Debug)]
pub struct SystemClock {
    timezone: TimeZoneSpec,
}

#[cfg(feature = "system-clock")]
impl SystemClock {
    pub fn new(timezone: TimeZoneSpec) -> Self {
        Self { timezone }
    }
}

#[cfg(feature = "system-clock")]
impl ClockProvider for SystemClock {
    fn timezone(&self) -> &TimeZoneSpec {
        &self.timezone
    }

    fn now(&self) -> NaiveDateTime {
        match &self.timezone {
            TimeZoneSpec::Local => Local::now().naive_local(),
            TimeZoneSpec::Utc => Utc::now().naive_utc(),
            TimeZoneSpec::FixedOffsetSeconds(secs) => {
                let off = FixedOffset::east_opt(*secs)
                    .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
                let utc_now: DateTime<Utc> = Utc::now();
                utc_now.with_timezone(&off).naive_local()
            }
        }
    }
}

/// Deterministic clock implementation: always returns the configured instant.
#[derive(Clone, Debug)]
pub struct FixedClock {
    timestamp_utc: DateTime<Utc>,
    timezone: TimeZoneSpec,
}

impl FixedClock {
    pub fn new(timestamp_utc: DateTime<Utc>, timezone: TimeZoneSpec) -> Self {
        Self {
            timestamp_utc,
            timezone,
        }
    }

    pub fn new_deterministic(
        timestamp_utc: DateTime<Utc>,
        timezone: TimeZoneSpec,
    ) -> Result<Self, String> {
        timezone.validate_for_determinism()?;
        Ok(Self::new(timestamp_utc, timezone))
    }

    fn now_in_timezone(&self) -> NaiveDateTime {
        match &self.timezone {
            TimeZoneSpec::Utc => self.timestamp_utc.naive_utc(),
            TimeZoneSpec::FixedOffsetSeconds(secs) => {
                let off = FixedOffset::east_opt(*secs).expect("validated fixed offset");
                self.timestamp_utc.with_timezone(&off).naive_local()
            }
            TimeZoneSpec::Local => {
                // Should be unreachable due to validation, but keep behaviour
                // predictable. Fall back to UTC when Local is unavailable
                // (portable wasm profile) rather than refusing to compile.
                #[cfg(feature = "system-clock")]
                {
                    self.timestamp_utc.with_timezone(&Local).naive_local()
                }
                #[cfg(not(feature = "system-clock"))]
                {
                    self.timestamp_utc.naive_utc()
                }
            }
        }
    }
}

impl ClockProvider for FixedClock {
    fn timezone(&self) -> &TimeZoneSpec {
        &self.timezone
    }

    fn now(&self) -> NaiveDateTime {
        self.now_in_timezone()
    }
}
