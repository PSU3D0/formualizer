//! Plain-data scenario descriptions and lifecycle scripts.
use crate::shape::{Role, Shape};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_workbook::Workbook;
use std::{
    collections::BTreeMap,
    env, fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

pub type CustomStep = Arc<dyn Fn(&mut Workbook) -> Result<(), String> + Send + Sync>;
pub type Oracle = Arc<dyn Fn(&Workbook) -> Result<(), String> + Send + Sync>;
/// Builder for an external xlsx corpus fixture. The size is passed through so
/// legacy small/medium/large ladders remain intact.
pub type XlsxFixture = Arc<dyn Fn(&Path, ScenarioSize) -> Result<PathBuf, String> + Send + Sync>;

#[derive(Clone)]
pub enum ScenarioSource {
    Shape(Shape),
    XlsxFixture(XlsxFixture),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Position {
    Absolute(u32),
    Middle,
}

#[derive(Clone)]
pub enum Step {
    Load,
    Prepare,
    EvaluateAll,
    NoOp,
    SetValue(Role, f64),
    SetFormula(Role, String),
    OverrideInterior,
    FormulaToLiteral,
    Undo,
    Redo,
    InsertRows { at: Position, count: u32 },
    DeleteRows { at: Position, count: u32 },
    InsertCols { at: Position, count: u32 },
    DeleteCols { at: Position, count: u32 },
    CancelAfter { checkpoints: usize },
    Custom(CustomStep),
}

impl fmt::Debug for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load => f.write_str("Load"),
            Self::Prepare => f.write_str("Prepare"),
            Self::EvaluateAll => f.write_str("EvaluateAll"),
            Self::NoOp => f.write_str("NoOp"),
            Self::SetValue(role, value) => {
                f.debug_tuple("SetValue").field(role).field(value).finish()
            }
            Self::SetFormula(role, formula) => f
                .debug_tuple("SetFormula")
                .field(role)
                .field(formula)
                .finish(),
            Self::OverrideInterior => f.write_str("OverrideInterior"),
            Self::FormulaToLiteral => f.write_str("FormulaToLiteral"),
            Self::Undo => f.write_str("Undo"),
            Self::Redo => f.write_str("Redo"),
            Self::InsertRows { at, count } => f
                .debug_struct("InsertRows")
                .field("at", at)
                .field("count", count)
                .finish(),
            Self::DeleteRows { at, count } => f
                .debug_struct("DeleteRows")
                .field("at", at)
                .field("count", count)
                .finish(),
            Self::InsertCols { at, count } => f
                .debug_struct("InsertCols")
                .field("at", at)
                .field("count", count)
                .finish(),
            Self::DeleteCols { at, count } => f
                .debug_struct("DeleteCols")
                .field("at", at)
                .field("count", count)
                .finish(),
            Self::CancelAfter { checkpoints } => f
                .debug_struct("CancelAfter")
                .field("checkpoints", checkpoints)
                .finish(),
            Self::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Script(pub Vec<Step>);
impl Script {
    pub fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self(steps.into_iter().collect())
    }
    pub fn standard() -> Self {
        Self::new([
            Step::Load,
            Step::Prepare,
            Step::EvaluateAll,
            Step::NoOp,
            Step::SetValue(Role::PrimaryInput, 3.0),
            Step::OverrideInterior,
            Step::FormulaToLiteral,
            Step::Undo,
            Step::Redo,
            Step::InsertRows {
                at: Position::Middle,
                count: 1,
            },
            Step::DeleteRows {
                at: Position::Middle,
                count: 1,
            },
        ])
    }
}

#[derive(Clone)]
pub enum Expect {
    Value {
        sheet: String,
        row: u32,
        col: u32,
        value: LiteralValue,
    },
    Oracle(Oracle),
    NoErrors(String),
    Parity,
    Structure(StructureExpect),
}

#[derive(Clone, Debug, Default)]
pub struct StructureExpect {
    /// Restrict the expectation to one FormulaPlane mode; `None` applies to all.
    pub mode: Option<FormulaPlaneMode>,
    /// Restrict the expectation to one materialization provenance; `None` applies to all.
    pub provenance: Option<Provenance>,
    pub active_spans: Option<usize>,
    pub demotions: Option<DemotionExpect>,
    pub topology_outcome: Option<String>,
    pub arena_nodes: Option<usize>,
    pub placed_families: Option<usize>,
    pub rejected_families: Option<usize>,
    pub graph_vertices: Option<usize>,
    pub graph_edges: Option<usize>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DemotionExpect {
    pub count: usize,
    pub reason: Option<String>,
}

macro_rules! dimension {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum $name { $($variant),+ }
        impl $name { fn value(self) -> &'static str { match self { $(Self::$variant => $value),+ } } }
    };
}
dimension!(Family { Coupled => "coupled", Independent => "independent", Fixed => "fixed", Chain => "chain", Reverse => "reverse", Stride => "stride", Window => "window", Blocks => "blocks", SameRelative => "same-relative", Expanding => "expanding", Shifted => "shifted", Lookup => "lookup", Irregular => "irregular" });
dimension!(Orientation { Vertical => "vertical", Horizontal => "horizontal" });
dimension!(Provenance { XlsxCalamine => "xlsx-calamine", XlsxUmya => "xlsx-umya", WorkbookApi => "workbook-api" });
dimension!(LifecycleOp { Load => "load", Evaluate => "evaluate", Edit => "edit", Structural => "structural", History => "history" });
dimension!(EnginePath { Legacy => "legacy", FormulaPlane => "formula-plane", Demoted => "demoted" });
dimension!(Purpose { Behavioral => "behavioral", EdgeCase => "edge-case", Parity => "parity", Trace => "trace", Benchmark => "benchmark" });
dimension!(SizeClass { Small => "small", Medium => "medium", Large => "large", Nightly => "nightly" });

#[derive(Clone, Debug, Default)]
pub struct Tags {
    pub family: Vec<Family>,
    pub orientation: Vec<Orientation>,
    pub provenance: Vec<Provenance>,
    pub lifecycle: Vec<LifecycleOp>,
    pub engine: Vec<EnginePath>,
    pub purpose: Vec<Purpose>,
    pub size: Vec<SizeClass>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScenarioSize {
    pub class: SizeClass,
    pub rows: u32,
}
impl ScenarioSize {
    pub const fn new(class: SizeClass, rows: u32) -> Self {
        Self { class, rows }
    }
}

#[derive(Clone)]
pub struct ScenarioSpec {
    pub id: String,
    pub description: String,
    pub shape: Shape,
    /// None means Shape(shape); external corpora may provide an xlsx factory.
    pub source: Option<ScenarioSource>,
    pub script: Script,
    pub expects: Vec<(usize, Expect)>,
    pub tags: Tags,
    pub modes: Vec<FormulaPlaneMode>,
    pub sizes: Vec<ScenarioSize>,
    pub expected_failures: Vec<ExpectedFailureSpec>,
}

/// A tracked defect: the run must fail in this mode (and provenance, when
/// given), with exactly the `failure` fingerprint. A matching run that passes,
/// or that fails anywhere else or with another message, is reported as a
/// failure: the marker is removed when the defect is fixed and cannot hide an
/// unrelated regression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedFailureSpec {
    pub mode: FormulaPlaneMode,
    /// `None` applies to every provenance.
    pub provenance: Option<Provenance>,
    pub failure: FailureFingerprint,
    pub reason: String,
}
impl ExpectedFailureSpec {
    pub fn matches(&self, mode: FormulaPlaneMode, provenance: Provenance) -> bool {
        self.mode == mode
            && self
                .provenance
                .is_none_or(|expected| expected == provenance)
    }
}

/// Which part of a step failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureStage {
    /// Applying the step (load, edit, evaluation, custom action) returned an error.
    Action,
    /// An expectation attached to the step did not hold.
    Expectation,
}

/// The exact failure a tracked defect produces: the failing step, the stage
/// and the complete message (without the `step N: ` prefix). Messages carry the
/// cell and the observed value (or the error text), so a different mismatch at
/// the same step does not match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureFingerprint {
    pub step: usize,
    pub stage: FailureStage,
    pub message: String,
}
impl FailureFingerprint {
    pub fn action(step: usize, message: impl Into<String>) -> Self {
        Self {
            step,
            stage: FailureStage::Action,
            message: message.into(),
        }
    }
    pub fn expectation(step: usize, message: impl Into<String>) -> Self {
        Self {
            step,
            stage: FailureStage::Expectation,
            message: message.into(),
        }
    }
}
impl fmt::Display for FailureFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stage = match self.stage {
            FailureStage::Action => "action",
            FailureStage::Expectation => "expectation",
        };
        write!(f, "step {} ({stage}): {}", self.step, self.message)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter(BTreeMap<String, Vec<String>>);
impl Filter {
    pub fn from_env() -> Result<Option<Self>, String> {
        match env::var("FZ_SCENARIO_FILTER") {
            Ok(value) if !value.trim().is_empty() => value.parse().map(Some),
            Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }
    pub fn matches(&self, spec: &ScenarioSpec) -> bool {
        self.0.iter().all(|(dimension, wanted)| {
            if dimension == "provenance" && spec.tags.provenance.is_empty() {
                return true;
            }
            let actual: Vec<&str> = match dimension.as_str() {
                "family" => spec.tags.family.iter().map(|v| v.value()).collect(),
                "orientation" => spec.tags.orientation.iter().map(|v| v.value()).collect(),
                "provenance" => spec.tags.provenance.iter().map(|v| v.value()).collect(),
                "lifecycle" => spec.tags.lifecycle.iter().map(|v| v.value()).collect(),
                "engine" => spec.tags.engine.iter().map(|v| v.value()).collect(),
                "purpose" => spec.tags.purpose.iter().map(|v| v.value()).collect(),
                "size" => spec.tags.size.iter().map(|v| v.value()).collect(),
                _ => return false,
            };
            wanted
                .iter()
                .any(|wanted| actual.contains(&wanted.as_str()))
        })
    }
    /// Test the run-time provenance axis without baking it into a scenario.
    pub fn allows_provenance(&self, provenance: Provenance) -> bool {
        self.0
            .get("provenance")
            .is_none_or(|wanted| wanted.iter().any(|value| value == provenance.value()))
    }
}
impl FromStr for Filter {
    type Err = String;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut dimensions = BTreeMap::new();
        for clause in input.split_whitespace().filter(|v| !v.is_empty()) {
            let (dimension, values) = clause.split_once(':').ok_or_else(|| {
                format!("invalid filter clause {clause:?}; expected dim:value[,value]")
            })?;
            if !matches!(
                dimension,
                "family"
                    | "orientation"
                    | "provenance"
                    | "lifecycle"
                    | "engine"
                    | "purpose"
                    | "size"
            ) {
                return Err(format!("unknown filter dimension {dimension:?}"));
            }
            let values: Vec<_> = values
                .split(',')
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .collect();
            if values.is_empty() {
                return Err(format!("empty filter dimension {dimension:?}"));
            }
            dimensions.insert(dimension.to_owned(), values);
        }
        Ok(Self(dimensions))
    }
}

/// Small lifecycle registry used by the standalone scenario runner.
pub fn built_in_registry(rows: u32) -> Vec<ScenarioSpec> {
    use crate::shape::{Extent, Range, Scale, r#gen};
    let make = |id: &str, description: &str, family: Family, shape: Shape| ScenarioSpec {
        id: id.into(),
        description: description.into(),
        shape,
        source: None,
        script: Script::standard(),
        expects: (0..Script::standard().0.len())
            .map(|step| {
                let expected = if step >= 4 { 3.0 } else { 1.0 };
                (
                    step,
                    Expect::Oracle(Arc::new(move |workbook| {
                        match workbook.get_value("S", 1, 1) {
                            Some(LiteralValue::Number(value)) if value == expected => Ok(()),
                            actual => Err(format!(
                                "primary input: expected {expected}, got {actual:?}"
                            )),
                        }
                    })),
                )
            })
            .collect(),
        tags: Tags {
            family: vec![family],
            orientation: vec![Orientation::Vertical],
            lifecycle: vec![
                LifecycleOp::Load,
                LifecycleOp::Evaluate,
                LifecycleOp::Edit,
                LifecycleOp::Structural,
                LifecycleOp::History,
            ],
            engine: vec![EnginePath::Legacy, EnginePath::FormulaPlane],
            purpose: vec![Purpose::Behavioral, Purpose::Trace],
            size: vec![SizeClass::Small],
            ..Tags::default()
        },
        modes: vec![
            FormulaPlaneMode::Off,
            FormulaPlaneMode::AuthoritativeExperimental,
        ],
        sizes: vec![ScenarioSize::new(SizeClass::Small, rows)],
        expected_failures: vec![],
    };
    let coupled = Shape::new().scale(Scale::rows(rows)).sheet("S", |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "recur",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=C{r-1}+A{r}",
        )
        .boundary("B1", "=A1");
        s.family(
            "half",
            Range::col("C", Extent::Abs(1)..=Extent::Rows),
            "=B{r}/2",
        );
        s.role("A1", Role::PrimaryInput);
        s.role("recur", Role::PrimaryFamily);
    });
    let independent = Shape::new().scale(Scale::rows(rows)).sheet("S", |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "independent",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=A{r}*2+1",
        );
        s.role("A1", Role::PrimaryInput);
        s.role("independent", Role::PrimaryFamily);
    });
    let fixed = Shape::new().scale(Scale::rows(rows)).sheet("S", |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "fixed",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            format!("=SUM($A$1:$A${rows})"),
        );
        s.role("A1", Role::PrimaryInput);
        s.role("fixed", Role::PrimaryFamily);
    });
    vec![
        make(
            "coupled",
            "two-predecessor coupled recurrence",
            Family::Coupled,
            coupled,
        ),
        make(
            "independent",
            "independent formula family",
            Family::Independent,
            independent,
        ),
        make(
            "fixed-absolute-sum",
            "fixed absolute SUM",
            Family::Fixed,
            fixed,
        ),
    ]
}

pub use crate::witnesses::witness_registry;

/// The standard witness size ladder. Witness value models and structural
/// goldens are bound to rows, so a rung is a registry rather than an override.
pub mod ladder {
    use super::{ScenarioSize, ScenarioSpec, SizeClass};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Rung {
        pub class: SizeClass,
        pub rows: u32,
    }

    pub const RUNGS: &[Rung] = &[
        Rung {
            class: SizeClass::Small,
            rows: 16,
        },
        Rung {
            class: SizeClass::Small,
            rows: 100,
        },
        Rung {
            class: SizeClass::Small,
            rows: 256,
        },
        Rung {
            class: SizeClass::Medium,
            rows: 1_000,
        },
        Rung {
            class: SizeClass::Medium,
            rows: 4_096,
        },
        Rung {
            class: SizeClass::Large,
            rows: 16_384,
        },
        Rung {
            class: SizeClass::Large,
            rows: 100_000,
        },
        Rung {
            class: SizeClass::Nightly,
            rows: 1_000_000,
        },
    ];
    pub fn by_rows_or_class(value: &str) -> Option<Vec<Rung>> {
        if value == "all" {
            return Some(RUNGS.to_vec());
        }
        if let Ok(rows) = value.parse::<u32>() {
            return RUNGS
                .iter()
                .copied()
                .find(|r| r.rows == rows)
                .map(|r| vec![r]);
        }
        let class = match value.to_ascii_lowercase().as_str() {
            "small" => SizeClass::Small,
            "medium" => SizeClass::Medium,
            "large" => SizeClass::Large,
            "nightly" => SizeClass::Nightly,
            _ => return None,
        };
        Some(RUNGS.iter().copied().filter(|r| r.class == class).collect())
    }
    /// Full coverage is preferable to pairwise here: 14 × 8 is only 112 specs.
    pub fn covering_set(rungs: impl IntoIterator<Item = Rung>) -> Vec<ScenarioSpec> {
        rungs
            .into_iter()
            .flat_map(|rung| {
                crate::witnesses::witness_registry(rung.rows)
                    .into_iter()
                    .map(move |mut spec| {
                        spec.id = format!("{}@{}", spec.id, rung.rows);
                        spec.tags.size = vec![rung.class];
                        spec.sizes = vec![ScenarioSize::new(rung.class, rung.rows)];
                        spec
                    })
            })
            .collect()
    }
}
