//! Shared source policy decoding; adapters never interpret iteration semantics.
use formualizer::workbook::{SourceCalculationPolicy, XlsxRecalculateOptions};
use pyo3::prelude::*;

pub(crate) fn from_json(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    Ok(py
        .import("json")?
        .call_method1("loads", (value.to_string(),))?
        .unbind())
}

/// Decode the canonical partial limits DTO before opening any input package.
pub(crate) fn apply_limits(
    options: &mut XlsxRecalculateOptions,
    limits: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let Some(value) = limits.filter(|value| !value.is_none()) else {
        return Ok(());
    };
    let kwargs = pyo3::types::PyDict::new(value.py());
    kwargs.set_item("allow_nan", false)?;
    let text: String = value
        .py()
        .import("json")?
        .getattr("dumps")?
        .call((value,), Some(&kwargs))?
        .extract()?;
    let request: formualizer::workbook::SourceRecalculateLimits = serde_json::from_str(&text)
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
    request
        .apply(&mut options.limits)
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
}

pub(crate) fn apply(
    options: &mut XlsxRecalculateOptions,
    calculation: Option<&Bound<'_, PyAny>>,
    seed: bool,
    timestamp: bool,
    timezone: bool,
) -> PyResult<()> {
    let mut policy = if let Some(value) = calculation {
        let kwargs = pyo3::types::PyDict::new(value.py());
        kwargs.set_item("allow_nan", false)?;
        let text: String = value
            .py()
            .import("json")?
            .getattr("dumps")?
            .call((value,), Some(&kwargs))?
            .extract()?;
        serde_json::from_str::<SourceCalculationPolicy>(&text)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
    } else if let Some(policy) = &options.calculation {
        policy.clone()
    } else {
        return Ok(());
    };
    let conflict = |field| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "calculation conflicts with legacy {field}"
        ))
    };
    if seed {
        let value = options.eval_config.workbook_seed;
        if policy.workbook_seed.is_some_and(|v| v != value) {
            return Err(conflict("rng_seed"));
        }
        policy.workbook_seed = Some(value);
    }
    if let formualizer::eval::engine::DeterministicMode::Enabled {
        timestamp_utc,
        timezone: zone,
    } = &options.eval_config.deterministic_mode
    {
        if timestamp {
            if policy.fixed_now_utc.is_some_and(|v| v != *timestamp_utc) {
                return Err(conflict("deterministic_timestamp_utc"));
            }
            policy.fixed_now_utc = Some(*timestamp_utc);
        }
        if timezone {
            if policy.timezone.as_ref().is_some_and(|v| v != zone) {
                return Err(conflict("deterministic_timezone"));
            }
            policy.timezone = Some(zone.clone());
        } else if timestamp && policy.timezone.is_none() {
            policy.timezone = Some(zone.clone());
        }
    }
    policy
        .validate()
        .map_err(super::errors::workbook_error_to_pyerr)?;
    options.calculation = Some(policy);
    Ok(())
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use formualizer::workbook::{
        SourceOperationDiagnostics, SourceOperationSample, SourceUnavailableOperation,
    };

    #[test]
    fn canonical_provider_and_mode_dtos_survive_python_projection() {
        Python::initialize();
        Python::attach(|py| {
            for operation in [
                SourceUnavailableOperation::UnverifiedProvider {
                    name: "CUSTOM".into(),
                },
                SourceUnavailableOperation::UnverifiedFunctionMode {
                    name: "IF".into(),
                    mode: "callable_result".into(),
                },
                SourceUnavailableOperation::UnimplementedFunctionMode {
                    name: "CELL".into(),
                    mode: "format".into(),
                },
            ] {
                let sample = SourceOperationSample {
                    operation,
                    location: Some("S!A1".into()),
                };
                let canonical = serde_json::to_value(sample).unwrap();
                let projected = from_json(py, &canonical).unwrap();
                let encoded: String = py
                    .import("json")
                    .unwrap()
                    .call_method1("dumps", (projected,))
                    .unwrap()
                    .extract()
                    .unwrap();
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
                    canonical
                );
            }
        });
    }

    #[test]
    fn optional_physical_counters_keep_null_zero_and_large_decimal_strings() {
        Python::initialize();
        Python::attach(|py| {
            for value in [None, Some(0), Some(9_007_199_254_740_991), Some(u64::MAX)] {
                let report = SourceOperationDiagnostics {
                    physical_function_dispatches: value,
                    physical_unavailable_observations: value,
                    ..Default::default()
                };
                let projected = from_json(py, &serde_json::to_value(report).unwrap()).unwrap();
                for key in [
                    "physicalFunctionDispatches",
                    "physicalUnavailableObservations",
                ] {
                    let counter = projected.bind(py).get_item(key).unwrap();
                    match value {
                        None => assert!(counter.is_none()),
                        Some(value) if value > 9_007_199_254_740_991 => {
                            assert_eq!(counter.extract::<String>().unwrap(), value.to_string())
                        }
                        Some(value) => assert_eq!(counter.extract::<u64>().unwrap(), value),
                    }
                }
            }
        });
    }

    #[test]
    fn limits_preserve_preset_merge_and_validate_before_mutation() {
        Python::initialize();
        Python::attach(|py| {
            let mut options = XlsxRecalculateOptions::workbook();
            options.error_location_limit = 0;
            apply_limits(&mut options, None).unwrap();
            assert_eq!(options.limits.max_columns, 16_384);
            let request = py
                .import("json")
                .unwrap()
                .call_method1(
                    "loads",
                    (r#"{"maxOperationRoots":0,"maxAffectedWork":9007199254740991}"#,),
                )
                .unwrap();
            apply_limits(&mut options, Some(&request)).unwrap();
            assert_eq!(options.limits.max_operation_roots, 0);
            assert_eq!(options.limits.max_affected_work, 9_007_199_254_740_991);
            assert_eq!(options.error_location_limit, 0);
            assert_eq!(options.limits.max_columns, 16_384);
            for text in [
                r#"{"maxOperationRoots":null}"#,
                r#"{"maxInputBytes":4294967296}"#,
                "[]",
            ] {
                let request = py
                    .import("json")
                    .unwrap()
                    .call_method1("loads", (text,))
                    .unwrap();
                assert!(apply_limits(&mut options, Some(&request)).is_err());
                assert_eq!(options.limits.max_columns, 16_384);
            }
        });
    }
}
