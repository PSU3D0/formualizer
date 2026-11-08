//! One portable request/receipt representation shared by language adapters.
use super::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};

fn object<'de, D: Deserializer<'de>, T: Deserialize<'de>>(deserializer: D) -> Result<T, D::Error> {
    struct ObjectVisitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for ObjectVisitor<T> {
        type Value = T;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a portable calculation record")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
            T::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }
    deserializer.deserialize_map(ObjectVisitor(std::marker::PhantomData))
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct Request {
    iteration: SourceIterationPolicy,
    iteration_initial_state: SourceIterationInitialState,
    cycle_detection: Detection,
    workbook_seed: Option<Seed>,
    fixed_now_utc: Option<String>,
    timezone: Option<Zone>,
    max_elapsed_ms: Option<u64>,
    max_work_units: Option<u64>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct IterationRequest {
    mode: IterationMode,
    max_iterations: Option<u32>,
    max_change: Option<f64>,
}
#[derive(Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum IterationMode {
    #[default]
    Workbook,
    Disabled,
    Iterate,
}
impl<'de> Deserialize<'de> for SourceIterationPolicy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value: IterationRequest = object(deserializer)?;
        if value.mode != IterationMode::Iterate
            && (value.max_iterations.is_some() || value.max_change.is_some())
        {
            return Err(serde::de::Error::custom(
                "iteration fields require mode iterate",
            ));
        }
        Ok(match value.mode {
            IterationMode::Workbook => Self::Workbook,
            IterationMode::Disabled => Self::Disabled,
            IterationMode::Iterate => Self::Iterate {
                max_iterations: value.max_iterations,
                max_change: value.max_change,
            },
        })
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Detection {
    #[default]
    Runtime,
    Static,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Seed {
    Decimal(String),
    SafeNumber(u64),
}
#[derive(Serialize)]
#[serde(untagged)]
enum Zone {
    Name(String),
    Offset(Offset),
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Offset {
    offset_seconds: i32,
}

impl<'de> Deserialize<'de> for Zone {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ZoneVisitor;
        impl<'de> serde::de::Visitor<'de> for ZoneVisitor {
            type Value = Zone;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a timezone name or offsetSeconds record")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Zone, E> {
                Ok(Zone::Name(value.to_owned()))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Zone, E> {
                Ok(Zone::Name(value))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Zone, A::Error> {
                Offset::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(Zone::Offset)
            }
        }
        deserializer.deserialize_any(ZoneVisitor)
    }
}

impl<'de> Deserialize<'de> for SourceCalculationPolicy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let request: Request = object(deserializer)?;
        let fail = |message: String| serde::de::Error::custom(message);
        let workbook_seed = match request.workbook_seed {
            None => None,
            Some(Seed::SafeNumber(n)) if n <= 9_007_199_254_740_991 => Some(n),
            Some(Seed::SafeNumber(_)) => {
                return Err(fail(
                    "workbookSeed numbers must be JS-safe; use a decimal string".into(),
                ));
            }
            Some(Seed::Decimal(text)) => {
                if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(fail("workbookSeed must be unsigned decimal digits".into()));
                }
                Some(
                    text.parse::<u64>()
                        .map_err(|_| fail("workbookSeed exceeds u64".into()))?,
                )
            }
        };
        let fixed_now_utc = request
            .fixed_now_utc
            .map(|text| {
                DateTime::parse_from_rfc3339(&text)
                    .map(|t| t.with_timezone(&Utc))
                    .map_err(|_| fail("fixedNowUtc must be RFC3339".into()))
            })
            .transpose()?;
        let timezone = request
            .timezone
            .map(|zone| match zone {
                Zone::Name(name) if name.eq_ignore_ascii_case("utc") => Ok(TimeZoneSpec::Utc),
                Zone::Name(name) if name.eq_ignore_ascii_case("local") => Ok(TimeZoneSpec::Local),
                Zone::Name(_) => Err(fail(
                    "timezone must be utc, local or an offsetSeconds object".into(),
                )),
                Zone::Offset(offset) => {
                    let zone = TimeZoneSpec::FixedOffsetSeconds(offset.offset_seconds);
                    zone.validate_for_determinism().map_err(fail)?;
                    Ok(zone)
                }
            })
            .transpose()?;
        let policy = Self {
            iteration: request.iteration,
            iteration_initial_state: request.iteration_initial_state,
            cycle_detection: match request.cycle_detection {
                Detection::Runtime => CycleDetection::Runtime,
                Detection::Static => CycleDetection::Static,
            },
            workbook_seed,
            fixed_now_utc,
            timezone,
            max_elapsed_ms: request.max_elapsed_ms,
            max_work_units: request.max_work_units,
        };
        policy.validate().map_err(|e| fail(e.to_string()))?;
        Ok(policy)
    }
}

impl Serialize for ResolvedSourceCalculation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use formualizer_eval::engine::CyclePolicy;
        let (enabled, count, delta) = match self.cycle.policy {
            CyclePolicy::Error => (false, None, None),
            CyclePolicy::Iterate {
                max_iterations,
                max_change,
            } => (true, Some(max_iterations), Some(max_change)),
        };
        let zone = match self.timezone {
            TimeZoneSpec::Utc => Zone::Name("utc".into()),
            TimeZoneSpec::Local => Zone::Name("local".into()),
            TimeZoneSpec::FixedOffsetSeconds(offset_seconds) => {
                Zone::Offset(Offset { offset_seconds })
            }
        };
        let mut state = serializer.serialize_struct("ResolvedSourceCalculation", 15)?;
        state.serialize_field(
            "cycleDetection",
            match self.cycle.detection {
                CycleDetection::Runtime => "runtime",
                CycleDetection::Static => "static",
            },
        )?;
        state.serialize_field("iterationEnabled", &enabled)?;
        state.serialize_field("maxIterations", &count)?;
        state.serialize_field("maxChange", &delta)?;
        state.serialize_field("iterationInitialState", &self.iteration_initial_state)?;
        state.serialize_field("iterationOrigin", &self.iteration_origin)?;
        state.serialize_field("maxIterationsOrigin", &self.max_iterations_origin)?;
        state.serialize_field("maxChangeOrigin", &self.max_change_origin)?;
        state.serialize_field(
            "dateSystem",
            match self.date_system {
                DateSystem::Excel1900 => "1900",
                DateSystem::Excel1904 => "1904",
            },
        )?;
        state.serialize_field("workbookSeed", &self.workbook_seed.to_string())?;
        state.serialize_field("timezone", &zone)?;
        state.serialize_field("fixedNowUtc", &self.fixed_now_utc.map(|t| t.to_rfc3339()))?;
        state.serialize_field("maxElapsedMs", &self.max_elapsed_ms.map(|n| n.to_string()))?;
        state.serialize_field("maxWorkUnits", &self.max_work_units.map(|n| n.to_string()))?;
        state.serialize_field("warnings", &self.warnings)?;
        state.end()
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use super::*;
    #[test]
    fn portable_seed_clock_and_unknown_fields_are_validated() {
        let policy: SourceCalculationPolicy = serde_json::from_str(r#"{"workbookSeed":"18446744073709551615","fixedNowUtc":"2024-01-02T03:04:05Z","iteration":{"mode":"disabled"}}"#).unwrap();
        let (_, report) = policy
            .resolve(
                &EvalConfig::default(),
                b"<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"/>",
                DateSystem::Excel1900,
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(report).unwrap()["workbookSeed"],
            u64::MAX.to_string()
        );
        for json in [
            r#"{"workbookSeed":9007199254740992}"#,
            r#"{"workbookSeed":"-1"}"#,
            r#"{"fixedNowUtc":"bad"}"#,
            r#"{"iteration":{"mode":"iterate","maxIterations":0}}"#,
            r#"{"iteration":{"mode":"disabled","maxIterations":2}}"#,
            r#"{"unknown":1}"#,
            r#"{"timezone":{"offsetSeconds":999999}}"#,
        ] {
            assert!(
                serde_json::from_str::<SourceCalculationPolicy>(json).is_err(),
                "{json}"
            );
        }
    }
}
