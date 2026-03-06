use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub engine: String,
    pub scenario: String,
    pub mode: String,
    pub status: String,
    pub metrics: MetricsResult,
    pub correctness: CorrectnessResult,
    #[serde(default)]
    pub notes: Vec<String>,
    pub timestamp: String,
    #[serde(default)]
    pub meta: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricsResult {
    #[serde(default)]
    pub load_ms: Option<f64>,
    #[serde(default)]
    pub full_eval_ms: Option<f64>,
    #[serde(default)]
    pub incremental_us: Option<f64>,
    #[serde(default)]
    pub peak_rss_mb: Option<f64>,
    #[serde(default)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrectnessResult {
    pub passed: bool,
    pub mismatches: u64,
    #[serde(default)]
    pub details: Vec<String>,
}
