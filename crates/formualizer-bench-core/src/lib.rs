pub mod result;
pub mod scenario;

#[cfg(feature = "xlsx")]
pub mod corpus;

pub use result::{BenchmarkResult, CorrectnessResult, MetricsResult};
pub use scenario::{BenchmarkSuite, Operation, Scenario};
