//! Date and time functions (Phase 3)
//! Functions implemented: TODAY, NOW, DATE, TIME, YEAR, MONTH, DAY,
//! HOUR, MINUTE, SECOND, DATEVALUE, TIMEVALUE, EDATE, EOMONTH,
//! WEEKDAY, WEEKNUM, DATEDIF, NETWORKDAYS, WORKDAY

mod date_parts;
mod date_time;
mod date_value;
mod edate_eomonth;
mod serial;
mod today_now;
mod weekday_workday;

pub use date_parts::*;
pub use date_time::*;
pub use date_value::*;
pub use edate_eomonth::*;
pub use serial::*;
pub use today_now::*;
pub use weekday_workday::*;

pub fn register_builtins() {
    today_now::register_builtins();
    date_time::register_builtins();
    date_parts::register_builtins();
    date_value::register_builtins();
    edate_eomonth::register_builtins();
    weekday_workday::register_builtins();
}
