//! Financial functions
//! Functions implemented: PMT, PV, FV, NPV, NPER, RATE, IPMT, PPMT, SLN, SYD, DB, DDB,
//! XNPV, XIRR, DOLLARDE, DOLLARFR, ACCRINT, ACCRINTM, PRICE, YIELD

mod bonds;
mod depreciation;
mod tvm;

pub use bonds::*;
pub use depreciation::*;
pub use tvm::*;

pub fn register_builtins() {
    bonds::register_builtins();
    tvm::register_builtins();
    depreciation::register_builtins();
}
