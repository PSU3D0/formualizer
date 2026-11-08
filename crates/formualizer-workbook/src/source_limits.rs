//! Portable partial limits. Missing fields preserve the selected native preset;
//! explicit null/bool/fractional values are invalid, never an unlimited budget.
use crate::{IoError, XlsxRecalculateLimits};
use serde::{Deserialize, Deserializer};

fn present<'de, D: Deserializer<'de>>(value: D) -> Result<Option<u64>, D::Error> {
    u64::deserialize(value).map(Some)
}
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(
    remote = "Self",
    default,
    deny_unknown_fields,
    rename_all = "camelCase"
)]
pub struct SourceRecalculateLimits {
    #[serde(deserialize_with = "present")]
    pub max_input_bytes: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_entries: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_expanded_bytes: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_worksheet_bytes: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_formula_cells: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_output_bytes: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_xml_depth: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_cells: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_columns: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_operation_roots: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_operation_text_bytes: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_affected_work: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_affected_regions: Option<u64>,
    #[serde(deserialize_with = "present")]
    pub max_affected_scratch_bytes: Option<u64>,
}
// Reuse derived field decoding but require a mapping at the portable JSON
// boundary; derived structs otherwise also accept positional sequences.
impl<'de> Deserialize<'de> for SourceRecalculateLimits {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LimitsVisitor;
        impl<'de> serde::de::Visitor<'de> for LimitsVisitor {
            type Value = SourceRecalculateLimits;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a source limits object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                SourceRecalculateLimits::deserialize(serde::de::value::MapAccessDeserializer::new(
                    map,
                ))
            }
        }
        deserializer.deserialize_map(LimitsVisitor)
    }
}

fn invalid(field: &str, message: &str) -> IoError {
    IoError::InvalidOptions {
        field: format!("limits.{field}"),
        message: message.into(),
    }
}
impl SourceRecalculateLimits {
    /// JSON-style limits have one target-independent domain. Byte/count caps
    /// fit wasm32; logical work is a JS-safe integer. Native Rust callers may
    /// configure the raw XlsxRecalculateLimits separately.
    pub fn apply(&self, limits: &mut XlsxRecalculateLimits) -> Result<(), IoError> {
        for (field, value) in [
            ("maxInputBytes", self.max_input_bytes),
            ("maxEntries", self.max_entries),
            ("maxExpandedBytes", self.max_expanded_bytes),
            ("maxWorksheetBytes", self.max_worksheet_bytes),
            ("maxFormulaCells", self.max_formula_cells),
            ("maxOutputBytes", self.max_output_bytes),
            ("maxXmlDepth", self.max_xml_depth),
            ("maxCells", self.max_cells),
            ("maxOperationRoots", self.max_operation_roots),
            ("maxOperationTextBytes", self.max_operation_text_bytes),
            ("maxAffectedRegions", self.max_affected_regions),
            ("maxAffectedScratchBytes", self.max_affected_scratch_bytes),
        ] {
            if value.is_some_and(|value| value > u64::from(u32::MAX)) {
                return Err(invalid(
                    field,
                    "must fit the portable wasm32 byte/count domain",
                ));
            }
        }
        if self
            .max_affected_work
            .is_some_and(|value| value > 9_007_199_254_740_991)
        {
            return Err(invalid(
                "maxAffectedWork",
                "must be a JS-safe nonnegative integer",
            ));
        }
        if self.max_columns.is_some_and(|value| value > 16_384) {
            return Err(invalid(
                "maxColumns",
                "must not exceed the XLSX column grid",
            ));
        }
        let mut next = limits.clone();
        macro_rules! assign {
            ($($field:ident),+ $(,)?)=>{$(if let Some(value)=self.$field {
                next.$field=usize::try_from(value).map_err(|_|invalid(stringify!($field),"exceeds target size"))?;
            })+};
        }
        assign!(
            max_input_bytes,
            max_entries,
            max_expanded_bytes,
            max_worksheet_bytes,
            max_formula_cells,
            max_output_bytes,
            max_xml_depth,
            max_cells,
            max_operation_roots,
            max_operation_text_bytes,
            max_affected_regions,
            max_affected_scratch_bytes
        );
        if let Some(value) = self.max_columns {
            next.max_columns = value as u32;
        }
        if let Some(value) = self.max_affected_work {
            next.max_affected_work = value;
        }
        *limits = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn portable_limits_preserve_preset_and_apply_explicit_zero() {
        let request: SourceRecalculateLimits =
            serde_json::from_str(r#"{"maxOperationRoots":0,"maxAffectedWork":17}"#).unwrap();
        let mut options = crate::XlsxRecalculateOptions::workbook();
        request.apply(&mut options.limits).unwrap();
        assert_eq!(options.limits.max_columns, 16_384);
        assert_eq!(options.limits.max_operation_roots, 0);
        assert_eq!(options.limits.max_affected_work, 17);
    }
    #[test]
    fn portable_limits_invalid_values_do_not_mutate_a_preset() {
        for json in [
            "[]",
            "[{}]",
            r#"{"maxOperationRoots":null}"#,
            r#"{"maxOperationRoots":true}"#,
            r#"{"maxOperationRoots":0.5}"#,
            r#"{"maxOperationRoots":-1}"#,
            r#"{"unknownLimit":1}"#,
            r#"{"maxAffectedWork":"17"}"#,
        ] {
            assert!(
                serde_json::from_str::<SourceRecalculateLimits>(json).is_err(),
                "{json}"
            );
        }
        for json in [
            r#"{"maxInputBytes":4294967296}"#,
            r#"{"maxColumns":16385}"#,
            r#"{"maxAffectedWork":9007199254740992}"#,
        ] {
            let request: SourceRecalculateLimits = serde_json::from_str(json).unwrap();
            let mut options = crate::XlsxRecalculateOptions::workbook();
            let before = options.limits.max_operation_roots;
            assert!(request.apply(&mut options.limits).is_err(), "{json}");
            assert_eq!(options.limits.max_operation_roots, before);
            assert_eq!(options.limits.max_columns, 16_384);
        }
    }
}
