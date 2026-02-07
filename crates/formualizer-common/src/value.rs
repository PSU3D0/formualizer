use chrono::{Duration as ChronoDur, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use std::{
    fmt::{self, Display},
    hash::{Hash, Hasher},
};

use crate::ExcelError;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/* ───────────────────── Excel date-serial utilities ───────────────────
Excel's serial date system:
  Serial 1  = 1900-01-01
  Serial 59 = 1900-02-28
  Serial 60 = 1900-02-29  (phantom – doesn't exist, but Excel thinks it does)
  Serial 61 = 1900-03-01
Base date = 1899-12-31 so that serial 1 = base + 1 day = 1900-01-01.
Time is stored as fractional days (no timezone).
------------------------------------------------------------------- */

/// Base date for the 1900 date system. Serial 1 = base + 1 day = 1900-01-01.
const EXCEL_EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();

pub fn datetime_to_serial(dt: &NaiveDateTime) -> f64 {
    let days = (dt.date() - EXCEL_EPOCH).num_days();
    // Dates on or after 1900-03-01 get +1 to account for phantom Feb 29
    let serial_days = if dt.date() >= NaiveDate::from_ymd_opt(1900, 3, 1).unwrap() {
        days + 1
    } else {
        days
    };

    let secs_in_day = dt.time().num_seconds_from_midnight() as f64;
    serial_days as f64 + secs_in_day / 86_400.0
}

pub fn serial_to_datetime(serial: f64) -> NaiveDateTime {
    let days = serial.trunc() as i64;
    let frac_secs = (serial.fract() * 86_400.0).round() as i64;

    // Serial 60 is phantom 1900-02-29; map to 1900-02-28
    let date = if days == 60 {
        NaiveDate::from_ymd_opt(1900, 2, 28).unwrap()
    } else {
        // serial < 60: offset = serial (no phantom day yet)
        // serial > 60: offset = serial - 1 (skip phantom day)
        let offset = if days < 60 { days } else { days - 1 };
        EXCEL_EPOCH + ChronoDur::days(offset)
    };

    let time =
        NaiveTime::from_num_seconds_from_midnight_opt((frac_secs.rem_euclid(86_400)) as u32, 0)
            .unwrap();
    date.and_time(time)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DateSystem {
    Excel1900,
    Excel1904,
}

impl Display for DateSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateSystem::Excel1900 => write!(f, "1900"),
            DateSystem::Excel1904 => write!(f, "1904"),
        }
    }
}

/// An **interpeter** LiteralValue. This is distinct
/// from the possible types that can be stored in a cell.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Int(i64),
    Number(f64),
    Text(String),
    Boolean(bool),
    Array(Vec<Vec<LiteralValue>>),   // For array results
    Date(chrono::NaiveDate),         // For date values
    DateTime(chrono::NaiveDateTime), // For date/time values
    Time(chrono::NaiveTime),         // For time values
    Duration(chrono::Duration),      // For durations
    Empty,                           // For empty cells/optional arguments
    Pending,                         // For pending values

    Error(ExcelError),
}

impl Hash for LiteralValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            LiteralValue::Int(i) => i.hash(state),
            LiteralValue::Number(n) => n.to_bits().hash(state),
            LiteralValue::Text(s) => s.hash(state),
            LiteralValue::Boolean(b) => b.hash(state),
            LiteralValue::Array(a) => a.hash(state),
            LiteralValue::Date(d) => d.hash(state),
            LiteralValue::DateTime(dt) => dt.hash(state),
            LiteralValue::Time(t) => t.hash(state),
            LiteralValue::Duration(d) => d.hash(state),
            LiteralValue::Empty => state.write_u8(0),
            LiteralValue::Pending => state.write_u8(1),
            LiteralValue::Error(e) => e.hash(state),
        }
    }
}

impl Eq for LiteralValue {}

impl Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::Int(i) => write!(f, "{i}"),
            LiteralValue::Number(n) => write!(f, "{n}"),
            LiteralValue::Text(s) => write!(f, "{s}"),
            LiteralValue::Boolean(b) => write!(f, "{b}"),
            LiteralValue::Error(e) => write!(f, "{e}"),
            LiteralValue::Array(a) => write!(f, "{a:?}"),
            LiteralValue::Date(d) => write!(f, "{d}"),
            LiteralValue::DateTime(dt) => write!(f, "{dt}"),
            LiteralValue::Time(t) => write!(f, "{t}"),
            LiteralValue::Duration(d) => write!(f, "{d}"),
            LiteralValue::Empty => write!(f, ""),
            LiteralValue::Pending => write!(f, "Pending"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueError {
    ImplicitIntersection(String),
}

impl LiteralValue {
    /// Coerce
    pub fn coerce_to_single_value(&self) -> Result<LiteralValue, ValueError> {
        match self {
            LiteralValue::Array(arr) => {
                // Excel's implicit intersection or single LiteralValue coercion logic here
                // Simplest: take top-left or return #LiteralValue! if not 1x1
                if arr.len() == 1 && arr[0].len() == 1 {
                    Ok(arr[0][0].clone())
                } else if arr.is_empty() || arr[0].is_empty() {
                    Ok(LiteralValue::Empty) // Or maybe error?
                } else {
                    Err(ValueError::ImplicitIntersection(
                        "#LiteralValue! Implicit intersection failed".to_string(),
                    ))
                }
            }
            _ => Ok(self.clone()),
        }
    }

    pub fn as_serial_number(&self) -> Option<f64> {
        match self {
            LiteralValue::Date(d) => {
                let dt = d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                Some(datetime_to_serial(&dt))
            }
            LiteralValue::DateTime(dt) => Some(datetime_to_serial(dt)),
            LiteralValue::Time(t) => Some(t.num_seconds_from_midnight() as f64 / 86_400.0),
            LiteralValue::Duration(d) => Some(d.num_seconds() as f64 / 86_400.0),
            LiteralValue::Int(i) => Some(*i as f64),
            LiteralValue::Number(n) => Some(*n),
            LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// Build the appropriate `LiteralValue` from an Excel serial number.
    /// (Useful when a function returns a date/time).
    pub fn from_serial_number(serial: f64) -> Self {
        let dt = serial_to_datetime(serial);
        if dt.time() == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
            LiteralValue::Date(dt.date())
        } else {
            LiteralValue::DateTime(dt)
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            LiteralValue::Boolean(b) => *b,
            LiteralValue::Int(i) => *i != 0,
            LiteralValue::Number(n) => *n != 0.0,
            LiteralValue::Text(s) => !s.is_empty(),
            LiteralValue::Array(arr) => !arr.is_empty(),
            LiteralValue::Date(_) => true,
            LiteralValue::DateTime(_) => true,
            LiteralValue::Time(_) => true,
            LiteralValue::Duration(_) => true,
            LiteralValue::Error(_) => false,
            LiteralValue::Empty => false,
            LiteralValue::Pending => false,
        }
    }
}
