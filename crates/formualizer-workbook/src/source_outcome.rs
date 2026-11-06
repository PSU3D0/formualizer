//! Request coverage is separate from final Excel errors. Complete means no
//! known/unverified executed gaps under admitted source assumptions, not
//! universal Excel equivalence or all-semantic-read certification.
use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCalculationCoverage {
    #[default]
    Complete,
    Partial,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageScope {
    #[default]
    None,
    Conservative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceUnavailableOperation {
    FunctionPanicked { name: String },
    UnknownFunction { name: String },
    UnimplementedFunction { name: String },
    UnimplementedFunctionMode { name: String, mode: String },
    UnverifiedFunctionMode { name: String, mode: String },
    UnverifiedProvider { name: String },
    PostfixInvocation,
    SourceFeature { name: String },
    InternalResult { token: String },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceOperationSample {
    pub operation: SourceUnavailableOperation,
    pub location: Option<String>,
}
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFunctionIncidence {
    pub cell_reason_count: usize,
    pub named_reason_count: usize,
    pub unlocated_reason_count: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceModeVerification {
    Unavailable,
    Unverified,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFunctionModeIncidence {
    pub name: String,
    pub mode: String,
    pub verification: SourceModeVerification,
    pub incidence: SourceFunctionIncidence,
}
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceDependencyEvidence {
    /// Scope is the admitted workbook graph and immutable cached sources. The
    /// engine does not attest arbitrary resolver/provider semantic read kinds.
    pub relation: String,
    pub source_domain: String,
    pub semantic_reads_unattested: bool,
    pub conservative_unknown_dynamic: bool,
    pub unknown_reasons: std::collections::BTreeMap<String, usize>,
    pub unknown_facts: usize,
    pub witnessed_regions: usize,
    pub uncertain_regions: usize,
    /// Admitted query work plus the publication membership upper bound.
    pub work: u64,
    pub query_work: u64,
    pub publication_membership_work_bound: u64,
    pub requested_capacity_peak_bytes: usize,
}
fn serialize_optional_counter<S: serde::Serializer>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        None => serializer.serialize_none(),
        Some(value) if *value <= 9_007_199_254_740_991 => serializer.serialize_some(value),
        Some(value) => serializer.serialize_some(&value.to_string()),
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceOperationDiagnostics {
    pub unavailable_functions: std::collections::BTreeMap<String, SourceFunctionIncidence>,
    /// Exact mode incidence, independent of bounded display samples.
    pub function_modes: Vec<SourceFunctionModeIncidence>,
    pub logical_cell_reason_count: usize,
    pub logical_root_cell_count: usize,
    pub logical_named_reason_count: usize,
    pub source_regions: usize,
    pub workbook_features: Vec<String>,
    pub unavailable_result_cells: usize,
    /// None is unmeasured; Some(0) is measured zero.
    #[serde(serialize_with = "serialize_optional_counter")]
    pub physical_unavailable_observations: Option<u64>,
    #[serde(serialize_with = "serialize_optional_counter")]
    pub physical_function_dispatches: Option<u64>,
    pub dependency_evidence: Option<SourceDependencyEvidence>,
    pub samples: Vec<SourceOperationSample>,
    pub samples_truncated: usize,
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    #[test]
    fn optional_physical_metrics_preserve_unmeasured_zero_and_large_exact_counts() {
        let mut report = SourceOperationDiagnostics::default();
        let unmeasured = serde_json::to_value(&report).unwrap();
        assert!(unmeasured["physicalFunctionDispatches"].is_null());
        report.physical_function_dispatches = Some(0);
        assert_eq!(
            serde_json::to_value(&report).unwrap()["physicalFunctionDispatches"],
            0
        );
        report.physical_function_dispatches = Some(u64::MAX);
        assert_eq!(
            serde_json::to_value(&report).unwrap()["physicalFunctionDispatches"],
            u64::MAX.to_string()
        );
    }
    #[test]
    fn mode_and_provider_uncertainty_have_distinct_portable_tags() {
        let mode = SourceUnavailableOperation::UnverifiedFunctionMode {
            name: "GROUPBY".into(),
            mode: "field_relationship".into(),
        };
        let provider = SourceUnavailableOperation::UnverifiedProvider {
            name: "CUSTOM".into(),
        };
        let known = SourceUnavailableOperation::UnimplementedFunctionMode {
            name: "CELL".into(),
            mode: "format".into(),
        };
        assert_eq!(
            serde_json::to_value(mode).unwrap()["kind"],
            "unverified_function_mode"
        );
        assert_eq!(
            serde_json::to_value(provider).unwrap()["kind"],
            "unverified_provider"
        );
        assert_eq!(
            serde_json::to_value(known).unwrap()["kind"],
            "unimplemented_function_mode"
        );
    }
}
