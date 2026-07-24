//! Evaluator-local adapters for formula families that intentionally retain
//! Excel-1900 serial semantics.
//!
//! Canonical conversion and validation live in `formualizer-common`. These
//! adapters make the fixed date-system choice explicit while the affected
//! builtins are migrated separately to workbook-aware semantics.

use chrono::{NaiveDate, NaiveDateTime};
use formualizer_common::{DateSystem, ExcelError};

pub(crate) fn serial_to_date(serial: f64) -> Result<NaiveDate, ExcelError> {
    formualizer_common::try_serial_to_date_for(DateSystem::Excel1900, serial)
}

pub(crate) fn date_to_serial(date: &NaiveDate) -> f64 {
    formualizer_common::date_to_serial_for(DateSystem::Excel1900, date)
}

pub(crate) fn serial_to_datetime(serial: f64) -> Result<NaiveDateTime, ExcelError> {
    formualizer_common::try_serial_to_datetime_for(DateSystem::Excel1900, serial)
}

pub(crate) fn datetime_to_serial(datetime: &NaiveDateTime) -> f64 {
    formualizer_common::datetime_to_serial_for(DateSystem::Excel1900, datetime)
}
