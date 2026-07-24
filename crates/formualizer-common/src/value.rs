use chrono::NaiveTime;
use std::{
    fmt::{self, Display},
    hash::{Hash, Hasher},
};

use crate::ExcelError;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

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

    /// Convert this value to a serial number in the selected date system.
    pub fn as_serial_number_for(&self, system: DateSystem) -> Option<f64> {
        match self {
            LiteralValue::Date(d) => Some(crate::date_to_serial_for(system, d)),
            LiteralValue::DateTime(dt) => Some(crate::datetime_to_serial_for(system, dt)),
            LiteralValue::Time(t) => Some(crate::time_to_fraction(t)),
            LiteralValue::Duration(d) => Some(d.num_seconds() as f64 / 86_400.0),
            LiteralValue::Int(i) => Some(*i as f64),
            LiteralValue::Number(n) => Some(*n),
            LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// Compatibility wrapper using the historical implicit Excel-1900 system.
    pub fn as_serial_number(&self) -> Option<f64> {
        self.as_serial_number_for(DateSystem::Excel1900)
    }

    /// Build the appropriate temporal value from an Excel serial number.
    ///
    /// This conversion uses representable `chrono` calendar values. In the
    /// Excel-1900 system, phantom serial 60 therefore becomes 1900-02-28 and
    /// re-encodes as serial 59. Use the display-parts API when the fictitious
    /// 1900-02-29 must remain distinguishable.
    pub fn try_from_serial_number_for(system: DateSystem, serial: f64) -> Result<Self, ExcelError> {
        let dt = crate::try_serial_to_datetime_for(system, serial)?;
        if dt.time() == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
            Ok(LiteralValue::Date(dt.date()))
        } else {
            Ok(LiteralValue::DateTime(dt))
        }
    }

    /// Checked convenience wrapper using the implicit Excel-1900 system.
    pub fn try_from_serial_number(serial: f64) -> Result<Self, ExcelError> {
        Self::try_from_serial_number_for(DateSystem::Excel1900, serial)
    }

    /// Compatibility wrapper using the historical implicit Excel-1900 system.
    ///
    /// Inputs outside Excel's calendar domain retain the legacy common
    /// behavior. New code should use [`Self::try_from_serial_number`] or
    /// [`Self::try_from_serial_number_for`] for checked conversion.
    pub fn from_serial_number(serial: f64) -> Self {
        let dt = crate::serial_to_datetime(serial);
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn literal_temporal_values_use_the_selected_date_system() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let value = LiteralValue::Date(date);
        assert_eq!(
            value.as_serial_number_for(DateSystem::Excel1900),
            Some(45_306.0)
        );
        assert_eq!(
            value.as_serial_number_for(DateSystem::Excel1904),
            Some(43_844.0)
        );
        assert_eq!(value.as_serial_number(), Some(45_306.0));
    }

    #[test]
    fn literal_serial_construction_is_checked_and_system_aware() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        for (system, serial) in [
            (DateSystem::Excel1900, 45_306.0),
            (DateSystem::Excel1904, 43_844.0),
        ] {
            assert_eq!(
                LiteralValue::try_from_serial_number_for(system, serial).unwrap(),
                LiteralValue::Date(date)
            );
        }
        assert!(LiteralValue::try_from_serial_number_for(DateSystem::Excel1900, -1.0).is_err());
        assert_eq!(
            LiteralValue::from_serial_number(45_306.0),
            LiteralValue::Date(date)
        );
        assert!(LiteralValue::try_from_serial_number(-1.0).is_err());
        assert_eq!(
            LiteralValue::from_serial_number(-1.0),
            LiteralValue::Date(NaiveDate::from_ymd_opt(1899, 12, 30).unwrap())
        );
    }
}
