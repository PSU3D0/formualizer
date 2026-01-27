pub mod aggregate;
pub mod combinatorics;
pub mod criteria_aggregates;
pub mod numeric;
pub mod reduction;
pub mod trig;

pub use aggregate::*;
pub use combinatorics::*;
pub use criteria_aggregates::*;
pub use trig::*;

/// Call the nested registration functions for built-in math functions.
pub fn register_builtins() {
    aggregate::register_builtins();
    combinatorics::register_builtins();
    criteria_aggregates::register_builtins();
    reduction::register_builtins();
    numeric::register_builtins();
    trig::register_builtins();
}
