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
mod s018_named_ranges_100;
mod s019_table_with_structured_refs;
mod s020_multi_table_cross_references;
mod s021_volatile_functions_sprinkled;
mod s022_dynamic_functions_offset_indirect;
mod s023_empty_cell_gaps_in_family;
mod s024_mixed_text_and_number_columns;
mod s025_errors_propagating_through_family;
mod s026_whole_column_refs_in_50k_formulas;
mod s027_large_array_literals;
mod s028_let_lambda_formulas;
mod s029_calc_tab_200_complex_cells;
mod s030_calc_and_data_tabs_mixed;
mod s031_finance_anchored_with_edit_cycles;
mod s032_family_with_row_insert_cycles;
mod s033_family_with_row_delete_cycles;
mod s034_family_with_column_insert;
mod s035_family_with_column_delete;
mod s036_multi_sheet_with_sheet_rename;
mod s037_named_range_update_cycles;
mod s038_bulk_edit_50_cells_per_cycle;
mod s039_undo_redo_of_bulk_edit;
mod s040_undo_redo_of_row_insert;
mod s041_table_grow_by_row_append;
mod s042_external_source_version_bump;
mod s043_if_short_circuit_with_erroring_else;
mod s044_ifs_chain_short_circuit;
mod s045_iferror_mixed_with_actual_errors;
mod s046_giant_ast_formula_200_deps;
mod s047_very_deep_chain;
mod s048_many_disjoint_families;
mod s049_vlookup_with_relative_key;
mod s050_vlookup_with_absolute_key;
mod s051_mixed_error_cascade;
mod s052_deeply_nested_if_chain;
mod s053_text_heavy_concatenation;
mod s054_recalc_after_add_then_delete_sheet;
mod s055_undo_after_mixed_edits;
mod s056_criteria_aggregates_with_array_criteria;
mod s057_named_range_redefined;
mod s058_volatile_non_volatile_mix;
mod s059_empty_sheet_with_cross_sheet_refs;
mod s060_self_referencing_table_row;
mod s061_index_with_constant_table;
mod s062_index_deeply_nested_in_if;
mod s063_index_with_table_edit;
mod s064_hlookup_family_horizontal_table;
mod s065_xlookup_exact_with_if_not_found_ref;
mod s066_xlookup_search_mode_2_exact;
mod s067_index_match_approximate_chain;
mod s068_vlookup_approximate_sorted_table;
mod s069_xlookup_wildcard_deeply_nested_if;

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
pub use s018_named_ranges_100::S018NamedRanges100;
pub use s019_table_with_structured_refs::S019TableWithStructuredRefs;
pub use s020_multi_table_cross_references::S020MultiTableCrossReferences;
pub use s021_volatile_functions_sprinkled::S021VolatileFunctionsSprinkled;
pub use s022_dynamic_functions_offset_indirect::S022DynamicFunctionsOffsetIndirect;
pub use s023_empty_cell_gaps_in_family::S023EmptyCellGapsInFamily;
pub use s024_mixed_text_and_number_columns::S024MixedTextAndNumberColumns;
pub use s025_errors_propagating_through_family::S025ErrorsPropagatingThroughFamily;
pub use s026_whole_column_refs_in_50k_formulas::S026WholeColumnRefsIn50kFormulas;
pub use s027_large_array_literals::S027LargeArrayLiterals;
pub use s028_let_lambda_formulas::S028LetLambdaFormulas;
pub use s029_calc_tab_200_complex_cells::S029CalcTab200ComplexCells;
pub use s030_calc_and_data_tabs_mixed::S030CalcAndDataTabsMixed;
pub use s031_finance_anchored_with_edit_cycles::S031FinanceAnchoredWithEditCycles;
pub use s032_family_with_row_insert_cycles::S032FamilyWithRowInsertCycles;
pub use s033_family_with_row_delete_cycles::S033FamilyWithRowDeleteCycles;
pub use s034_family_with_column_insert::S034FamilyWithColumnInsert;
pub use s035_family_with_column_delete::S035FamilyWithColumnDelete;
pub use s036_multi_sheet_with_sheet_rename::S036MultiSheetWithSheetRename;
pub use s037_named_range_update_cycles::S037NamedRangeUpdateCycles;
pub use s038_bulk_edit_50_cells_per_cycle::S038BulkEdit50CellsPerCycle;
pub use s039_undo_redo_of_bulk_edit::S039UndoRedoOfBulkEdit;
pub use s040_undo_redo_of_row_insert::S040UndoRedoOfRowInsert;
pub use s041_table_grow_by_row_append::S041TableGrowByRowAppend;
pub use s042_external_source_version_bump::S042ExternalSourceVersionBump;
pub use s043_if_short_circuit_with_erroring_else::S043IfShortCircuitWithErroringElse;
pub use s044_ifs_chain_short_circuit::S044IfsChainShortCircuit;
pub use s045_iferror_mixed_with_actual_errors::S045IferrorMixedWithActualErrors;
pub use s046_giant_ast_formula_200_deps::S046GiantAstFormula200Deps;
pub use s047_very_deep_chain::S047VeryDeepChain;
pub use s048_many_disjoint_families::S048ManyDisjointFamilies;
pub use s049_vlookup_with_relative_key::S049VlookupWithRelativeKey;
pub use s050_vlookup_with_absolute_key::S050VlookupWithAbsoluteKey;
pub use s051_mixed_error_cascade::S051MixedErrorCascade;
pub use s052_deeply_nested_if_chain::S052DeeplyNestedIfChain;
pub use s053_text_heavy_concatenation::S053TextHeavyConcatenation;
pub use s054_recalc_after_add_then_delete_sheet::S054RecalcAfterAddThenDeleteSheet;
pub use s055_undo_after_mixed_edits::S055UndoAfterMixedEdits;
pub use s056_criteria_aggregates_with_array_criteria::S056CriteriaAggregatesWithArrayCriteria;
pub use s057_named_range_redefined::S057NamedRangeRedefined;
pub use s058_volatile_non_volatile_mix::S058VolatileNonVolatileMix;
pub use s059_empty_sheet_with_cross_sheet_refs::S059EmptySheetWithCrossSheetRefs;
pub use s060_self_referencing_table_row::S060SelfReferencingTableRow;
pub use s061_index_with_constant_table::S061IndexWithConstantTable;
pub use s062_index_deeply_nested_in_if::S062IndexDeeplyNestedInIf;
pub use s063_index_with_table_edit::S063IndexWithTableEdit;
pub use s064_hlookup_family_horizontal_table::S064HlookupFamilyHorizontalTable;
pub use s065_xlookup_exact_with_if_not_found_ref::S065XlookupExactWithIfNotFoundRef;
pub use s066_xlookup_search_mode_2_exact::S066XlookupSearchMode2Exact;
pub use s067_index_match_approximate_chain::S067IndexMatchApproximateChain;
pub use s068_vlookup_approximate_sorted_table::S068VlookupApproximateSortedTable;
pub use s069_xlookup_wildcard_deeply_nested_if::S069XlookupWildcardDeeplyNestedIf;

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

    /// Modes under which this scenario is currently EXPECTED to fail invariant
    /// checks. The runner tracks failures on these modes as KNOWN (not as a
    /// regression). Default: empty (scenario expected to pass everywhere).
    ///
    /// Use sparingly. Each entry must reference a tracked bug or design
    /// limitation that the corpus is intentionally surfacing.
    fn expected_to_fail_under(&self) -> &'static [ExpectedFailure] {
        &[]
    }

    /// Off↔Auth full-cell parity divergences that are expected for this scenario.
    fn expected_divergences(&self) -> Vec<ExpectedDivergence> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedDivergence {
    pub phase: ExpectedDivergencePhase,
    pub reason: &'static str,
    pub action: ExpectedDivergenceAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedDivergencePhase {
    Any,
    AfterLoad,
    AfterFirstEval,
    AfterEdit,
    AfterRecalc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedDivergenceAction {
    Skip,
    RunAndNote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedFailure {
    pub mode: ExpectedFailureMode,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedFailureMode {
    AuthOnly,
    OffOnly,
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
    ShortCircuit,
    GiantAst,
    TextHeavy,
    ReferenceForwarding,

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
            Box::new(S018NamedRanges100::new()),
            Box::new(S019TableWithStructuredRefs::new()),
            Box::new(S020MultiTableCrossReferences::new()),
            Box::new(S021VolatileFunctionsSprinkled::new()),
            Box::new(S022DynamicFunctionsOffsetIndirect::new()),
            Box::new(S023EmptyCellGapsInFamily::new()),
            Box::new(S024MixedTextAndNumberColumns::new()),
            Box::new(S025ErrorsPropagatingThroughFamily::new()),
            Box::new(S026WholeColumnRefsIn50kFormulas::new()),
            Box::new(S027LargeArrayLiterals::new()),
            Box::new(S028LetLambdaFormulas::new()),
            Box::new(S029CalcTab200ComplexCells::new()),
            Box::new(S030CalcAndDataTabsMixed::new()),
            Box::new(S031FinanceAnchoredWithEditCycles::new()),
            Box::new(S032FamilyWithRowInsertCycles::new()),
            Box::new(S033FamilyWithRowDeleteCycles::new()),
            Box::new(S034FamilyWithColumnInsert::new()),
            Box::new(S035FamilyWithColumnDelete::new()),
            Box::new(S036MultiSheetWithSheetRename::new()),
            Box::new(S037NamedRangeUpdateCycles::new()),
            Box::new(S038BulkEdit50CellsPerCycle::new()),
            Box::new(S039UndoRedoOfBulkEdit::new()),
            Box::new(S040UndoRedoOfRowInsert::new()),
            Box::new(S041TableGrowByRowAppend::new()),
            Box::new(S042ExternalSourceVersionBump::new()),
            Box::new(S043IfShortCircuitWithErroringElse::new()),
            Box::new(S044IfsChainShortCircuit::new()),
            Box::new(S045IferrorMixedWithActualErrors::new()),
            Box::new(S046GiantAstFormula200Deps::new()),
            Box::new(S047VeryDeepChain::new()),
            Box::new(S048ManyDisjointFamilies::new()),
            Box::new(S049VlookupWithRelativeKey::new()),
            Box::new(S050VlookupWithAbsoluteKey::new()),
            Box::new(S051MixedErrorCascade::new()),
            Box::new(S052DeeplyNestedIfChain::new()),
            Box::new(S053TextHeavyConcatenation::new()),
            Box::new(S054RecalcAfterAddThenDeleteSheet::new()),
            Box::new(S055UndoAfterMixedEdits::new()),
            Box::new(S056CriteriaAggregatesWithArrayCriteria::new()),
            Box::new(S057NamedRangeRedefined::new()),
            Box::new(S058VolatileNonVolatileMix::new()),
            Box::new(S059EmptySheetWithCrossSheetRefs::new()),
            Box::new(S060SelfReferencingTableRow::new()),
            Box::new(S061IndexWithConstantTable::new()),
            Box::new(S062IndexDeeplyNestedInIf::new()),
            Box::new(S063IndexWithTableEdit::new()),
            Box::new(S064HlookupFamilyHorizontalTable::new()),
            Box::new(S065XlookupExactWithIfNotFoundRef::new()),
            Box::new(S066XlookupSearchMode2Exact::new()),
            Box::new(S067IndexMatchApproximateChain::new()),
            Box::new(S068VlookupApproximateSortedTable::new()),
            Box::new(S069XlookupWildcardDeeplyNestedIf::new()),
        ]
    }
}
