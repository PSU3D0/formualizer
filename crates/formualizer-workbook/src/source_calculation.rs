//! Portable opt-in calculation policy for source-preserving XLSX requests.
mod wire;
use crate::IoError;
use chrono::{DateTime, Utc};
use formualizer_common::DateSystem;
use formualizer_eval::engine::{CycleConfig, CycleDetection, EvalConfig};
use formualizer_eval::timezone::TimeZoneSpec;
use serde::{Deserialize, Serialize};

/// Iteration intent is explicit; equality to an engine default is not intent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceIterationPolicy {
    #[default]
    Workbook,
    Disabled,
    Iterate {
        #[serde(default, rename = "maxIterations")]
        max_iterations: Option<u32>,
        #[serde(default, rename = "maxChange")]
        max_change: Option<f64>,
    },
}

/// Initial-state contract for a newly created source evaluator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceIterationInitialState {
    /// Defer iterative file publication until source-seed semantics are verified.
    #[default]
    RequireVerified,
    /// Explicitly calculate from the engine's fresh zero/cold state. This is an
    /// input assumption, not a promise to reproduce Excel's saved-state result.
    ColdStart,
}

/// Opting in preserves the source date system and uses runtime cycle detection.
/// Omit this policy entirely to retain the legacy raw EvalConfig route.
#[derive(Debug, Clone)]
pub struct SourceCalculationPolicy {
    pub iteration: SourceIterationPolicy,
    pub iteration_initial_state: SourceIterationInitialState,
    pub cycle_detection: CycleDetection,
    pub workbook_seed: Option<u64>,
    pub fixed_now_utc: Option<DateTime<Utc>>,
    pub timezone: Option<TimeZoneSpec>,
    pub max_elapsed_ms: Option<u64>,
    pub max_work_units: Option<u64>,
}
impl Default for SourceCalculationPolicy {
    fn default() -> Self {
        Self {
            iteration: SourceIterationPolicy::Workbook,
            iteration_initial_state: SourceIterationInitialState::RequireVerified,
            cycle_detection: CycleDetection::Runtime,
            workbook_seed: None,
            fixed_now_utc: None,
            timezone: None,
            max_elapsed_ms: None,
            max_work_units: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CalculationSettingOrigin {
    Caller,
    Workbook,
    ExcelDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceCalculationWarning {
    pub field: &'static str,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedSourceCalculation {
    pub cycle: CycleConfig,
    pub iteration_initial_state: SourceIterationInitialState,
    pub date_system: DateSystem,
    pub workbook_seed: u64,
    pub timezone: TimeZoneSpec,
    pub fixed_now_utc: Option<DateTime<Utc>>,
    pub iteration_origin: CalculationSettingOrigin,
    pub max_iterations_origin: Option<CalculationSettingOrigin>,
    pub max_change_origin: Option<CalculationSettingOrigin>,
    pub warnings: Vec<SourceCalculationWarning>,
    pub max_elapsed_ms: Option<u64>,
    pub max_work_units: Option<u64>,
}

impl SourceCalculationPolicy {
    /// Pure request validation; no package import or evaluator construction.
    pub fn validate(&self) -> Result<(), IoError> {
        for (field, value) in [
            ("maxElapsedMs", self.max_elapsed_ms),
            ("maxWorkUnits", self.max_work_units),
        ] {
            if value.is_some_and(|value| value > 9_007_199_254_740_991) {
                return Err(invalid(field, "portable budgets must be JS-safe integers"));
            }
        }
        if self.max_elapsed_ms == Some(0) {
            return Err(invalid("maxElapsedMs", "must be positive"));
        }
        if let SourceIterationPolicy::Iterate {
            max_iterations,
            max_change,
        } = self.iteration
        {
            if max_iterations == Some(0) {
                return Err(invalid("iteration.max_iterations", "must be at least one"));
            }
            if max_change.is_some_and(|v| !v.is_finite() || v < 0.0) {
                return Err(invalid(
                    "iteration.max_change",
                    "must be finite and nonnegative",
                ));
            }
            if self.cycle_detection == CycleDetection::Static {
                return Err(invalid(
                    "cycle_detection",
                    "iteration requires runtime detection",
                ));
            }
        }
        if self.fixed_now_utc.is_some() {
            if let Some(timezone) = &self.timezone {
                timezone
                    .validate_for_determinism()
                    .map_err(|message| invalid("timezone", message))?;
            }
        }
        Ok(())
    }

    /// Resolve declared fields before evaluator construction. The caller
    /// supplies the date system already validated by source admission. Standalone
    /// XML must have a MAIN-namespace workbook root and at most one direct
    /// MAIN calcPr child; ordinary loading retains its separate lenient adapter.
    pub fn resolve(
        &self,
        raw: &EvalConfig,
        workbook_xml: &[u8],
        date_system: DateSystem,
    ) -> Result<(EvalConfig, ResolvedSourceCalculation), IoError> {
        self.validate()?;
        let properties = crate::calc_pr::read_source_calc_properties(workbook_xml)?;
        self.resolve_from_properties(raw, properties.as_ref(), date_system)
    }

    /// Resolve already admitted metadata without rescanning source XML.
    pub(crate) fn resolve_from_properties(
        &self,
        raw: &EvalConfig,
        properties: Option<&crate::calc_pr::ParsedCalcProperties>,
        date_system: DateSystem,
    ) -> Result<(EvalConfig, ResolvedSourceCalculation), IoError> {
        use CalculationSettingOrigin::{Caller, ExcelDefault, Workbook};
        use formualizer_eval::engine::{CyclePolicy, DeterministicMode};
        self.validate()?;
        let source = properties.map(|properties| properties.settings.clone());
        let warnings = properties
            .into_iter()
            .flat_map(|properties| properties.invalid_fields.iter().copied())
            .map(|field| SourceCalculationWarning {
                field,
                message: if field == "fullPrecision" {
                    "invalid source precision; semantics unverified".into()
                } else {
                    "invalid source value; Excel default applied".into()
                },
            })
            .collect();
        let enabled_source = source.as_ref().filter(|s| s.iterate);
        let source_count = enabled_source
            .and_then(|s| s.iterate_count)
            .filter(|v| *v > 0);
        let source_delta = enabled_source
            .and_then(|s| s.iterate_delta)
            .filter(|v| v.is_finite() && *v >= 0.0);
        let (enabled, requested_count, requested_delta, origin) = match self.iteration {
            SourceIterationPolicy::Workbook => (
                enabled_source.is_some(),
                None,
                None,
                if source.is_some() {
                    Workbook
                } else {
                    ExcelDefault
                },
            ),
            SourceIterationPolicy::Disabled => (false, None, None, Caller),
            SourceIterationPolicy::Iterate {
                max_iterations,
                max_change,
            } => (true, max_iterations, max_change, Caller),
        };
        let count = requested_count
            .or(source_count)
            .unwrap_or(CyclePolicy::EXCEL_DEFAULT_MAX_ITERATIONS);
        let delta = requested_delta
            .or(source_delta)
            .unwrap_or(CyclePolicy::EXCEL_DEFAULT_MAX_CHANGE);
        let field_origin = |requested: bool, source: bool| {
            if requested {
                Caller
            } else if source {
                Workbook
            } else {
                ExcelDefault
            }
        };
        let mut config = raw.clone();
        config.date_system = date_system;
        config.cycle = CycleConfig {
            detection: self.cycle_detection,
            policy: if enabled {
                CyclePolicy::Iterate {
                    max_iterations: count,
                    max_change: delta,
                }
            } else {
                CyclePolicy::Error
            },
        };
        config
            .cycle
            .validate()
            .map_err(|message| invalid("cycle_detection", message))?;
        if let Some(seed) = self.workbook_seed {
            config.workbook_seed = seed;
        }
        if self.fixed_now_utc.is_some() || self.timezone.is_some() {
            let timezone = self.timezone.clone().unwrap_or_else(|| {
                if self.fixed_now_utc.is_some() {
                    TimeZoneSpec::Utc
                } else {
                    config.deterministic_mode.timezone().clone()
                }
            });
            config.deterministic_mode = match self.fixed_now_utc {
                Some(timestamp_utc) => DeterministicMode::Enabled {
                    timestamp_utc,
                    timezone,
                },
                None => match config.deterministic_mode {
                    DeterministicMode::Enabled { timestamp_utc, .. } => {
                        DeterministicMode::Enabled {
                            timestamp_utc,
                            timezone,
                        }
                    }
                    DeterministicMode::Disabled { .. } => DeterministicMode::Disabled { timezone },
                },
            };
        }
        config
            .deterministic_mode
            .validate()
            .map_err(|error| invalid("clock", error.to_string()))?;
        if let Some(ms) = self.max_elapsed_ms {
            config.evaluation_budgets.deadline.max_elapsed =
                Some(std::time::Duration::from_millis(ms));
        }
        if let Some(work) = self.max_work_units {
            config.evaluation_budgets.work.max_work_units = Some(work);
        }
        let budgets = config.resolved_evaluation_budgets();
        let settings = ResolvedSourceCalculation {
            cycle: config.cycle,
            iteration_initial_state: self.iteration_initial_state,
            date_system,
            workbook_seed: config.workbook_seed,
            timezone: config.deterministic_mode.timezone().clone(),
            fixed_now_utc: match config.deterministic_mode {
                DeterministicMode::Enabled { timestamp_utc, .. } => Some(timestamp_utc),
                _ => None,
            },
            iteration_origin: origin,
            max_iterations_origin: enabled
                .then(|| field_origin(requested_count.is_some(), source_count.is_some())),
            max_change_origin: enabled
                .then(|| field_origin(requested_delta.is_some(), source_delta.is_some())),
            warnings,
            max_elapsed_ms: budgets
                .deadline
                .max_elapsed
                .and_then(|v| u64::try_from(v.as_millis()).ok()),
            max_work_units: budgets.work.max_work_units,
        };
        Ok((config, settings))
    }
}

fn invalid(field: &str, message: impl Into<String>) -> IoError {
    IoError::InvalidOptions {
        field: field.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_eval::engine::CyclePolicy;
    const ENABLED: &[u8] =
        br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><calcPr iterate="1" iterateCount="23" iterateDelta="0.02"/></workbook>"#;
    #[test]
    fn empty_properties_are_present_but_do_not_enable_iteration() {
        let (_, report) = SourceCalculationPolicy::default().resolve(
            &EvalConfig::default(),
            br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><calcPr/></workbook>"#,
            DateSystem::Excel1900,
        ).unwrap();
        assert_eq!(report.iteration_origin, CalculationSettingOrigin::Workbook);
        assert_eq!(report.cycle.policy, CyclePolicy::Error);
        assert!(report.warnings.is_empty());
        assert_eq!(report.max_iterations_origin, None);
    }

    #[test]
    fn workbook_policy_reads_source_iteration() {
        let (config, report) = SourceCalculationPolicy::default()
            .resolve(&EvalConfig::default(), ENABLED, DateSystem::Excel1900)
            .unwrap();
        assert_eq!(config.cycle, CycleConfig::iterate(23, 0.02));
        assert_eq!(report.iteration_origin, CalculationSettingOrigin::Workbook);
    }
    #[test]
    fn explicit_disabled_overrides_enabled_source() {
        let policy = SourceCalculationPolicy {
            iteration: SourceIterationPolicy::Disabled,
            ..Default::default()
        };
        let (config, report) = policy
            .resolve(&EvalConfig::default(), ENABLED, DateSystem::Excel1900)
            .unwrap();
        assert_eq!(config.cycle.policy, CyclePolicy::Error);
        assert_eq!(config.cycle.detection, CycleDetection::Runtime);
        assert_eq!(report.iteration_origin, CalculationSettingOrigin::Caller);
    }
    #[test]
    fn explicit_fields_inherit_remaining_enabled_source_fields() {
        let policy = SourceCalculationPolicy {
            iteration: SourceIterationPolicy::Iterate {
                max_iterations: Some(7),
                max_change: None,
            },
            ..Default::default()
        };
        let (config, report) = policy
            .resolve(&EvalConfig::default(), ENABLED, DateSystem::Excel1900)
            .unwrap();
        assert_eq!(config.cycle, CycleConfig::iterate(7, 0.02));
        assert_eq!(
            report.max_iterations_origin,
            Some(CalculationSettingOrigin::Caller)
        );
        assert_eq!(
            report.max_change_origin,
            Some(CalculationSettingOrigin::Workbook)
        );
    }
    #[test]
    fn absent_disabled_and_invalid_source_settings_have_distinct_origins() {
        let policy = SourceCalculationPolicy::default();
        let (config, absent) = policy
            .resolve(
                &EvalConfig::default(),
                b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"/>",
                DateSystem::Excel1904,
            )
            .unwrap();
        assert_eq!(config.cycle.policy, CyclePolicy::Error);
        assert_eq!(config.date_system, DateSystem::Excel1904);
        assert_eq!(
            absent.iteration_origin,
            CalculationSettingOrigin::ExcelDefault
        );
        let (_, disabled) = policy
            .resolve(
                &EvalConfig::default(),
                b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><calcPr iterate=\"0\"/></workbook>",
                DateSystem::Excel1900,
            )
            .unwrap();
        assert_eq!(
            disabled.iteration_origin,
            CalculationSettingOrigin::Workbook
        );
        let (config, invalid) = policy.resolve(&EvalConfig::default(), b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><calcPr iterate=\"1\" iterateCount=\"0\" iterateDelta=\"NaN\"/></workbook>", DateSystem::Excel1900).unwrap();
        assert_eq!(config.cycle, CycleConfig::iterate_excel_defaults());
        assert_eq!(invalid.warnings.len(), 2);
        assert_eq!(
            invalid.max_change_origin,
            Some(CalculationSettingOrigin::ExcelDefault)
        );
        assert!(
            policy
                .resolve(&EvalConfig::default(), b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">", DateSystem::Excel1900)
                .is_err()
        );
    }
    #[test]
    fn disabled_source_fields_do_not_seed_explicit_iteration_and_seed_is_exact() {
        let policy = SourceCalculationPolicy {
            iteration: SourceIterationPolicy::Iterate {
                max_iterations: None,
                max_change: None,
            },
            workbook_seed: Some(u64::MAX),
            ..Default::default()
        };
        let (config, _) = policy.resolve(&EvalConfig::default(), b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><calcPr iterate=\"0\" iterateCount=\"23\" iterateDelta=\"0.02\"/></workbook>", DateSystem::Excel1900).unwrap();
        assert_eq!(config.cycle, CycleConfig::iterate_excel_defaults());
        assert_eq!(config.workbook_seed, u64::MAX);
    }
    #[test]
    fn clock_and_source_validation_are_portable() {
        let now = DateTime::from_timestamp(1_600_000_000, 0).unwrap();
        let policy = SourceCalculationPolicy {
            fixed_now_utc: Some(now),
            ..Default::default()
        };
        let (config, report) = policy
            .resolve(
                &EvalConfig::default(),
                b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"/>",
                DateSystem::Excel1900,
            )
            .unwrap();
        assert_eq!(config.deterministic_mode.timezone(), &TimeZoneSpec::Utc);
        assert_eq!(report.fixed_now_utc, Some(now));
        for xml in [b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"/><workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"/>".as_slice(), b"<other/>", b""] {
            assert!(
                policy
                    .resolve(&EvalConfig::default(), xml, DateSystem::Excel1900)
                    .is_err()
            );
        }
        assert!(policy
            .resolve(
                &EvalConfig::default(),
                b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><ext><calcPr iterate=\"1\"/></ext></workbook>",
                DateSystem::Excel1900,
            ).is_err());
    }
    #[test]
    fn static_iteration_is_a_typed_options_error() {
        let policy = SourceCalculationPolicy {
            cycle_detection: CycleDetection::Static,
            ..Default::default()
        };
        assert!(matches!(
            policy.resolve(&EvalConfig::default(), ENABLED, DateSystem::Excel1900),
            Err(IoError::InvalidOptions { .. })
        ));
    }
    #[test]
    fn invalid_request_is_rejected_without_evaluator() {
        for delta in [-1.0, f64::NAN, f64::INFINITY] {
            let policy = SourceCalculationPolicy {
                iteration: SourceIterationPolicy::Iterate {
                    max_iterations: Some(1),
                    max_change: Some(delta),
                },
                ..Default::default()
            };
            assert!(policy.validate().is_err());
        }
        let policy = SourceCalculationPolicy {
            iteration: SourceIterationPolicy::Iterate {
                max_iterations: Some(0),
                max_change: None,
            },
            ..Default::default()
        };
        assert!(policy.validate().is_err());
    }
}
