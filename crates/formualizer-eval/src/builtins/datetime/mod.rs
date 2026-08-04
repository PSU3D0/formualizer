//! Date and time functions (Phase 3)
//! Functions implemented: TODAY, NOW, DATE, TIME, YEAR, MONTH, DAY,
//! HOUR, MINUTE, SECOND, DAYS, DAYS360, YEARFRAC, ISOWEEKNUM,
//! DATEVALUE, TIMEVALUE, EDATE, EOMONTH,
//! WEEKDAY, WEEKNUM, DATEDIF, NETWORKDAYS, WORKDAY,
//! NETWORKDAYS.INTL, WORKDAY.INTL

mod date_parts;
mod date_time;
mod date_value;
mod edate_eomonth;
mod serial;
mod today_now;
mod weekday_workday;

pub use serial::{
    create_date_normalized, date_to_serial, date_to_serial_for, datetime_to_serial,
    datetime_to_serial_for, serial_to_date, serial_to_datetime, serial_to_datetime_for,
    time_to_fraction,
};

pub(crate) fn register_builtins() {
    today_now::register_builtins();
    date_time::register_builtins();
    date_parts::register_builtins();
    date_value::register_builtins();
    edate_eomonth::register_builtins();
    weekday_workday::register_builtins();
}
