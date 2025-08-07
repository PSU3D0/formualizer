pub mod logical;
pub mod math;
pub mod random;
mod utils;

pub fn load_builtins() {
    logical::register_builtins();
    math::register_builtins();
    random::register_builtins();
}
