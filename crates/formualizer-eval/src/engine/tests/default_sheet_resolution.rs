//! Regression tests for default-sheet resolution leaks.
//!
//! `default_sheet_name`/`default_sheet_id` are legitimate stored configuration.
//! They are not a legitimate *resolution fallback*: substituting them for a
//! missing sheet context silently answers a question with data from an
//! unrelated sheet (issue #110).

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig, EvaluationTarget, TargetEvalOptions};
use crate::reference::{SharedRangeRef, SharedSheetLocator};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{AxisBound, LiteralValue};
use formualizer_parse::parser::parse;

fn engine_with_two_sheets() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet2").unwrap();
    engine
}

/// A name scoped to the default sheet must not answer a query that asked for
/// no scope at all. `None` means "workbook scope", not "the default sheet".
#[test]
fn sheet_scoped_name_does_not_leak_into_unscoped_has_name() {
    let mut engine = engine_with_two_sheets();
    let sheet1 = engine.sheet_id("Sheet1").unwrap();
    assert_eq!(
        sheet1,
        engine.default_sheet_id(),
        "precondition: Sheet1 is the default sheet"
    );

    engine
        .define_name(
            "SecretLocal",
            NamedDefinition::Literal(LiteralValue::Number(42.0)),
            NameScope::Sheet(sheet1),
        )
        .unwrap();

    assert!(
        engine.has_name("SecretLocal", Some("Sheet1")),
        "the name is in scope on the sheet it is scoped to"
    );
    assert!(
        !engine.has_name("SecretLocal", Some("Sheet2")),
        "the name is not in scope on another sheet"
    );
    assert!(
        !engine.has_name("SecretLocal", None),
        "an unscoped query asks about workbook scope only; a name scoped to the \
         default sheet must not answer it"
    );
}

#[test]
fn sheet_scoped_name_does_not_leak_into_unscoped_resolved_name_value() {
    let mut engine = engine_with_two_sheets();
    let sheet1 = engine.sheet_id("Sheet1").unwrap();

    engine
        .define_name(
            "SecretLocal",
            NamedDefinition::Literal(LiteralValue::Number(42.0)),
            NameScope::Sheet(sheet1),
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.resolved_name_value("SecretLocal", Some("Sheet1")),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(
        engine.resolved_name_value("SecretLocal", Some("Sheet2")),
        None
    );
    assert_eq!(
        engine.resolved_name_value("SecretLocal", None),
        None,
        "an unscoped read must not see a name scoped to the default sheet"
    );
}

/// Workbook-scoped names still answer both unscoped and sheet-scoped queries.
#[test]
fn workbook_scoped_name_answers_unscoped_and_sheet_scoped_queries() {
    let mut engine = engine_with_two_sheets();
    engine
        .define_name(
            "Global",
            NamedDefinition::Literal(LiteralValue::Number(7.0)),
            NameScope::Workbook,
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert!(engine.has_name("Global", None));
    assert!(engine.has_name("Global", Some("Sheet1")));
    assert!(engine.has_name("Global", Some("Sheet2")));
    assert_eq!(
        engine.resolved_name_value("Global", None),
        Some(LiteralValue::Number(7.0))
    );
}

/// `EvaluationTarget::Name { scope_sheet: None }` must not select a name that
/// only exists in the default sheet's scope.
#[test]
fn unscoped_name_target_does_not_select_default_sheet_scoped_name() {
    let mut engine = engine_with_two_sheets();
    let sheet1 = engine.sheet_id("Sheet1").unwrap();
    engine
        .define_name(
            "SecretLocal",
            NamedDefinition::Literal(LiteralValue::Number(42.0)),
            NameScope::Sheet(sheet1),
        )
        .unwrap();

    let target = EvaluationTarget::Name {
        name: "SecretLocal".to_string(),
        scope_sheet: None,
    };
    let report = engine
        .prepare_graph_for_targets(std::slice::from_ref(&target), TargetEvalOptions::default())
        .unwrap();
    assert!(
        !report.widening_reasons.is_empty(),
        "an unscoped target naming a sheet-scoped name is unresolved and must \
         widen, not silently resolve through the default sheet: {report:?}"
    );

    let scoped = EvaluationTarget::Name {
        name: "SecretLocal".to_string(),
        scope_sheet: Some("Sheet1".to_string()),
    };
    let scoped_report = engine
        .prepare_graph_for_targets(std::slice::from_ref(&scoped), TargetEvalOptions::default())
        .unwrap();
    assert!(
        scoped_report.widening_reasons.is_empty(),
        "the correctly scoped target resolves: {scoped_report:?}"
    );
}

// ---------------------------------------------------------------------------
// T2 - SharedSheetLocator resolution
// ---------------------------------------------------------------------------

/// A 1..=10 x 1..=10 compressed range dependency on `sheet`.
fn compressed_range(sheet: SharedSheetLocator<'static>) -> SharedRangeRef<'static> {
    SharedRangeRef {
        sheet,
        start_row: Some(AxisBound::new(0, true)),
        start_col: Some(AxisBound::new(0, true)),
        end_row: Some(AxisBound::new(9, true)),
        end_col: Some(AxisBound::new(9, true)),
    }
}

fn staging_engine() -> Engine<TestWorkbook> {
    let config = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    for sheet in ["Alpha", "Beta"] {
        engine.add_sheet(sheet).unwrap();
    }
    engine
}

/// Install a name whose stored definition carries `range_deps` verbatim.
///
/// `define_name` re-derives dependencies from the AST; `update_name`
/// deliberately does not, so this is the production path by which a caller's
/// own locators reach the graph.
fn define_name_with_range_deps(
    engine: &mut Engine<TestWorkbook>,
    name: &str,
    scope: NameScope,
    range_deps: Vec<SharedRangeRef<'static>>,
) {
    engine
        .define_name(
            name,
            NamedDefinition::Formula {
                ast: parse("=1").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            scope,
        )
        .unwrap();
    engine
        .update_name(
            name,
            NamedDefinition::Formula {
                ast: parse("=1").unwrap(),
                dependencies: Vec::new(),
                range_deps,
            },
            scope,
        )
        .unwrap();
}

fn selected_sheets(report: &crate::engine::PreparedTargetGraphReport) -> Vec<String> {
    let mut sheets = report
        .selected_cells
        .iter()
        .map(|cell| cell.sheet.to_string())
        .collect::<Vec<_>>();
    sheets.sort();
    sheets.dedup();
    sheets
}

/// `Current` in a named formula's range dependencies means the sheet the name
/// lives on, exactly as it does for a cell formula's range dependencies ~90
/// lines earlier in the same function. It must not become the default sheet.
#[test]
fn current_locator_in_named_formula_deps_uses_the_names_sheet_not_the_default() {
    let mut engine = staging_engine();
    let alpha = engine.sheet_id("Alpha").unwrap();
    assert_ne!(alpha, engine.default_sheet_id());

    // One staged formula inside the 1..10 x 1..10 box on each sheet.
    engine.stage_formula_text("Alpha", 3, 3, "=1".into());
    engine.stage_formula_text(
        engine.default_sheet_name().to_string().as_str(),
        3,
        3,
        "=2".into(),
    );

    define_name_with_range_deps(
        &mut engine,
        "AlphaBox",
        NameScope::Sheet(alpha),
        vec![compressed_range(SharedSheetLocator::Current)],
    );

    let target = EvaluationTarget::Name {
        name: "AlphaBox".to_string(),
        scope_sheet: Some("Alpha".to_string()),
    };
    let report = engine
        .prepare_graph_for_targets(&[target], TargetEvalOptions::default())
        .unwrap();

    assert_eq!(
        selected_sheets(&report),
        vec!["Alpha".to_string()],
        "the name is scoped to Alpha, so its `Current` range dependency covers          Alpha - not the workbook default sheet: {report:?}"
    );
}

/// The same site used a `_` wildcard, so it also swallowed `Name`: a
/// cross-sheet named-sheet dependency silently became the default sheet.
#[test]
fn named_sheet_locator_in_named_formula_deps_resolves_to_that_sheet() {
    let mut engine = staging_engine();
    let alpha = engine.sheet_id("Alpha").unwrap();

    engine.stage_formula_text("Beta", 3, 3, "=1".into());
    engine.stage_formula_text(
        engine.default_sheet_name().to_string().as_str(),
        3,
        3,
        "=2".into(),
    );

    define_name_with_range_deps(
        &mut engine,
        "BetaBox",
        NameScope::Sheet(alpha),
        vec![compressed_range(SharedSheetLocator::Name("Beta".into()))],
    );

    let target = EvaluationTarget::Name {
        name: "BetaBox".to_string(),
        scope_sheet: Some("Alpha".to_string()),
    };
    let report = engine
        .prepare_graph_for_targets(&[target], TargetEvalOptions::default())
        .unwrap();

    assert_eq!(
        selected_sheets(&report),
        vec!["Beta".to_string()],
        "a Beta! range dependency covers Beta, not the default sheet: {report:?}"
    );
}

/// And an unresolvable sheet name must widen rather than land on the default
/// sheet, matching how the cell-formula arm of the same function behaves.
#[test]
fn unresolvable_sheet_locator_in_named_formula_deps_widens() {
    let mut engine = staging_engine();
    let alpha = engine.sheet_id("Alpha").unwrap();

    engine.stage_formula_text(
        engine.default_sheet_name().to_string().as_str(),
        3,
        3,
        "=2".into(),
    );

    define_name_with_range_deps(
        &mut engine,
        "GhostBox",
        NameScope::Sheet(alpha),
        vec![compressed_range(SharedSheetLocator::Name(
            "NoSuchSheet".into(),
        ))],
    );

    let target = EvaluationTarget::Name {
        name: "GhostBox".to_string(),
        scope_sheet: Some("Alpha".to_string()),
    };
    let report = engine
        .prepare_graph_for_targets(&[target], TargetEvalOptions::default())
        .unwrap();

    assert!(
        report
            .widening_reasons
            .contains(&crate::engine::OpaqueReason::UnresolvedCrossSheetBinding),
        "an unresolved cross-sheet binding must be surfaced, not replaced by \
         the default sheet: {report:?}"
    );
}
