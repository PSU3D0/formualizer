//! FormulaPlane scenario corpus contracts and static registry.
//!
//! This module intentionally mirrors the shared dispatch plan. Scenarios are
//! statically registered and expose only object-safe methods so the corpus
//! runner can hold them as `Box<dyn Scenario>`.

use std::path::PathBuf;

use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

pub mod common;
mod s001_no_formulas_static_grid;
mod s002_single_column_trivial_family;
mod s003_finance_anchored_arithmetic_family;
mod s004_five_mixed_families;
mod s005_long_chain_family;

pub use s001_no_formulas_static_grid::S001NoFormulasStaticGrid;
pub use s002_single_column_trivial_family::S002SingleColumnTrivialFamily;
pub use s003_finance_anchored_arithmetic_family::S003FinanceAnchoredArithmeticFamily;
pub use s004_five_mixed_families::S004FiveMixedFamilies;
pub use s005_long_chain_family::S005LongChainFamily;

pub trait Scenario: Send + Sync {
    /// Stable, immutable identifier. Format: "sNNN-name".
    fn id(&self) -> &'static str;

    /// One-line human description.
    fn description(&self) -> &'static str;

    /// Categorical tag set. Use predefined tags from ScenarioTag enum.
    fn tags(&self) -> &'static [ScenarioTag];

    /// Build a workbook fixture to disk. Idempotent for given (path, params).
    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture>;

    /// Optional edit-cycle plan.
    fn edit_plan(&self) -> Option<EditPlan> {
        None
    }

    /// Expected result invariants checked by the runner after phases.
    fn invariants(&self, _phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        Vec::new()
    }
}

pub struct ScenarioBuildCtx {
    /// Target scale parameter: "small" / "medium" / "large".
    pub scale: ScenarioScale,
    /// Where to put the .xlsx fixture.
    pub fixture_dir: PathBuf,
    /// Workbook label (for fixture filename).
    pub label: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScenarioScale {
    Small,
    Medium,
    Large,
}

impl ScenarioScale {
    pub fn as_str(self) -> &'static str {
        match self {
            ScenarioScale::Small => "small",
            ScenarioScale::Medium => "medium",
            ScenarioScale::Large => "large",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScenarioTag {
    /// Categories
    NoFormulas,
    SingleColumnFamily,
    MultiColumnFamily,
    AnchoredArithmetic,
    LongChain,
    InternalDependency,
    LookupHeavy,
    AggregationHeavy,
    Mixed,
    MultiSheet,
    StructuredRefs,
    NamedRanges,
    Volatile,
    Dynamic,
    LetLambda,
    EmptyGaps,
    MixedTypes,
    ErrorPropagation,
    WholeColumnRefs,
    LargeArrayLiteral,

    /// Edit shapes
    SingleCellEdit,
    BulkEdit,
    InsertRows,
    DeleteRows,
    InsertColumns,
    DeleteColumns,
    SheetRename,
    UndoRedo,

    /// Engine paths
    SpanPromotable,
    LegacyOnly,
    CrossSheet,
}

pub struct ScenarioFixture {
    pub path: PathBuf,
    /// Workbook-level facts known at build time, used for invariant checks
    /// and reporter output (NOT for runtime decisions).
    pub metadata: FixtureMetadata,
}

pub struct FixtureMetadata {
    pub rows: u32,
    pub cols: u32,
    pub sheets: usize,
    pub formula_cells: u32,
    pub value_cells: u32,
    pub has_named_ranges: bool,
    pub has_tables: bool,
}

#[derive(Clone, Copy)]
pub struct EditPlan {
    /// Number of edit/recalc cycles to run.
    pub cycles: usize,
    /// Function called once per cycle. Mutates the workbook in place.
    /// Returns a label for the edit kind.
    pub apply: fn(&mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error>,
}

#[derive(Clone, Copy, Debug)]
pub enum ScenarioPhase {
    AfterLoad,
    AfterFirstEval,
    AfterEdit { cycle: usize, kind: &'static str },
    AfterRecalc { cycle: usize, kind: &'static str },
}

pub enum ScenarioInvariant {
    CellEquals {
        sheet: String,
        row: u32,
        col: u32,
        expected: LiteralValue,
    },
    NoErrorCells {
        sheet: String,
    },
}

pub struct ScenarioRegistry;

impl ScenarioRegistry {
    pub fn all() -> Vec<Box<dyn Scenario>> {
        vec![
            Box::new(S001NoFormulasStaticGrid::new()),
            Box::new(S002SingleColumnTrivialFamily::new()),
            Box::new(S003FinanceAnchoredArithmeticFamily::new()),
            Box::new(S004FiveMixedFamilies::new()),
            Box::new(S005LongChainFamily::new()),
        ]
    }
}
