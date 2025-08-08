pub mod logical;
pub mod math;
pub mod random;
pub mod reference_fns;
mod utils;

pub fn load_builtins() {
    logical::register_builtins();
    math::register_builtins();
    random::register_builtins();
    reference_fns::register_builtins();
}
