use std::{collections::BTreeMap, path::Path};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSuite {
    pub version: u32,
    pub suite: String,
    pub profiles: BTreeMap<String, Profile>,
    pub engines: Vec<EngineRef>,
    pub scenarios: Vec<Scenario>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRef {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    pub id: String,
    pub name: String,
    pub profile: String,
    pub source: Source,
    pub operations: Vec<Operation>,
    pub verify: Verify,
    #[serde(default)]
    pub metrics: Option<ScenarioMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub kind: String,
    pub workbook_path: String,
    #[serde(default)]
    pub generator: Option<String>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub op: String,
    #[serde(flatten)]
    pub args: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verify {
    #[serde(default)]
    pub expected: BTreeMap<String, Value>,
    #[serde(default)]
    pub formula_checks: Vec<FormulaCheck>,
    #[serde(default)]
    pub expected_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormulaCheck {
    pub cell: String,
    #[serde(rename = "type")]
    pub check_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioMetrics {
    #[serde(default)]
    pub primary: Vec<String>,
    #[serde(default)]
    pub secondary: Vec<String>,
}

impl BenchmarkSuite {
    pub fn from_yaml_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed reading benchmark suite: {}", path.display()))?;
        let suite: Self = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed parsing benchmark suite yaml: {}", path.display()))?;
        Ok(suite)
    }

    pub fn scenario(&self, id: &str) -> Option<&Scenario> {
        self.scenarios.iter().find(|s| s.id == id)
    }
}
