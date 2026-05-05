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
mod s006_rect_family_10cols;
mod s007_fixed_anchor_family;
mod s008_two_anchored_families;
mod s009_heavy_arith_family;
mod s010_all_unique_singletons;
mod s011_vlookup_family_against_1k_table;
mod s012_vlookup_family_against_10k_table;
mod s013_sumifs_family_constant_criteria;
mod s014_sumifs_family_varying_criteria;
mod s015_index_match_chain;
mod s016_multi_sheet_5_tabs;
mod s017_cross_sheet_references_in_family;

pub use s001_no_formulas_static_grid::S001NoFormulasStaticGrid;
pub use s002_single_column_trivial_family::S002SingleColumnTrivialFamily;
pub use s003_finance_anchored_arithmetic_family::S003FinanceAnchoredArithmeticFamily;
pub use s004_five_mixed_families::S004FiveMixedFamilies;
pub use s005_long_chain_family::S005LongChainFamily;
pub use s006_rect_family_10cols::S006RectFamily10Cols;
pub use s007_fixed_anchor_family::S007FixedAnchorFamily;
pub use s008_two_anchored_families::S008TwoAnchoredFamilies;
pub use s009_heavy_arith_family::S009HeavyArithFamily;
pub use s010_all_unique_singletons::S010AllUniqueSingletons;
pub use s011_vlookup_family_against_1k_table::S011VlookupFamilyAgainst1kTable;
pub use s012_vlookup_family_against_10k_table::S012VlookupFamilyAgainst10kTable;
pub use s013_sumifs_family_constant_criteria::S013SumifsFamilyConstantCriteria;
pub use s014_sumifs_family_varying_criteria::S014SumifsFamilyVaryingCriteria;
pub use s015_index_match_chain::S015IndexMatchChain;
pub use s016_multi_sheet_5_tabs::S016MultiSheet5Tabs;
pub use s017_cross_sheet_references_in_family::S017CrossSheetReferencesInFamily;

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
            Box::new(S006RectFamily10Cols::new()),
            Box::new(S007FixedAnchorFamily::new()),
            Box::new(S008TwoAnchoredFamilies::new()),
            Box::new(S009HeavyArithFamily::new()),
            Box::new(S010AllUniqueSingletons::new()),
            Box::new(S011VlookupFamilyAgainst1kTable::new()),
            Box::new(S012VlookupFamilyAgainst10kTable::new()),
            Box::new(S013SumifsFamilyConstantCriteria::new()),
            Box::new(S014SumifsFamilyVaryingCriteria::new()),
            Box::new(S015IndexMatchChain::new()),
            Box::new(S016MultiSheet5Tabs::new()),
            Box::new(S017CrossSheetReferencesInFamily::new()),
        ]
    }
}
