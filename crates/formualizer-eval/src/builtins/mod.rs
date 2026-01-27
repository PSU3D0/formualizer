pub mod database; // Phase 4 database functions (DSUM, DAVERAGE, etc.)
pub mod datetime; // Phase 3 date and time functions
pub mod engineering; // Phase 2 engineering functions (bitwise, base conversion, complex numbers, etc.)
pub mod financial; // Phase 5 financial functions
pub mod info; // Sprint 9 info / error introspection
pub mod logical;
pub mod logical_ext;
pub mod lookup; // Sprint 4 classic lookup (partial)
pub mod math;
pub mod random;
pub mod reference_fns;
pub mod stats; // Phase 6 statistical basics + extended stats
pub mod text; // Phase 2 core text functions
mod utils;

#[cfg(test)]
mod tests;

pub fn load_builtins() {
    database::register_builtins();
    datetime::register_builtins();
    engineering::register_builtins();
    financial::register_builtins();
    logical::register_builtins();
    logical_ext::register_builtins();
    info::register_builtins();
    math::register_builtins();
    random::register_builtins();
    reference_fns::register_builtins();
    lookup::register_builtins();
    text::register_builtins();
    stats::register_builtins();
}
