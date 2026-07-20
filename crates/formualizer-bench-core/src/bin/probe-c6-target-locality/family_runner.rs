use super::*;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use formualizer_bench_core::c6_calibration::TypedOracleValue;
use formualizer_bench_core::c6_calibration::families::{
    DIRTY_FORMULAS, FamilyFixtureShape, LAYOUT_BELOW_ENVELOPE_ROW, LAYOUT_BLANK_GUARD_ROW,
    LAYOUT_MAX_SCAN_ROWS, LAYOUT_PREPARATION_ENVELOPE_END_ROW, expected_typed_outputs,
    layout_manifest, scalar_manifest, table_manifest,
};
use formualizer_common::{ExcelErrorExtra, LiteralValue, PlanStaleReason, RangeAddress};
use formualizer_eval::engine::{EvaluationTarget, TableSelection};
use formualizer_eval::reference::{CellRef, Coord, RangeRef};
use formualizer_sheetport::{BatchInput, BatchOptions, InputUpdate, PortValue, SheetPort};
use formualizer_workbook::{IoError, NamedRangeScope};

pub(super) fn run_family_sample(
    fixture: &Path,
    formulas: u32,
    family: FixtureFamily,
    path: CalibrationPath,
    scope: TargetScope,
    warm_repeats: usize,
) -> Result<ChildReport> {
    anyhow::ensure!(
        scope == TargetScope::Full,
        "native breadth families support only --scope full"
    );
    let shape = formualizer_bench_core::c6_calibration::families::generate_family_fixture_shape(
        family, formulas,
    )?;
    let reachable_oracle = if family == FixtureFamily::Names && path == CalibrationPath::Sheetport {
        shape.reachable_formulas - 1
    } else {
        shape.reachable_formulas
    };
    let fixture_sha256 = sha256_file(fixture)?;
    let mut phases = PhaseTimings::default();
    let mut telemetry = BTreeMap::new();
    let mut gates = BTreeMap::new();
    let mut structural = BTreeMap::new();

    let load_started = Instant::now();
    let reader =
        CalamineAdapter::open_path(fixture).context("open family fixture with Calamine")?;
    let mut workbook = Workbook::from_reader(
        reader,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .context("interactive deferred family workbook load")?;
    phases.load = Some(TimedPhase::new(
        elapsed_ms(load_started),
        &["xlsx_open", "load"],
    ));
    let graph_after_load = graph_snapshot(&workbook);

    let dirty_started = Instant::now();
    workbook
        .prepare_graph_for_cells(&[("Dirty", DIRTY_FORMULAS, 2)])
        .context("prepare family dirty branch")?;
    workbook
        .evaluate_cell("Dirty", DIRTY_FORMULAS, 2)
        .context("evaluate family dirty branch")?;
    workbook
        .set_value("Dirty", 1, 1, LiteralValue::Number(40.0))
        .context("dirty family branch")?;
    phases.unrelated_dirty_setup = Some(TimedPhase::new(
        elapsed_ms(dirty_started),
        &[
            "target_resolution",
            "preparation",
            "evaluation",
            "unrelated_edit",
        ],
    ));
    let graph_after_setup = graph_snapshot(&workbook);

    let selector_setup = Instant::now();
    setup_selectors(&mut workbook, family)?;
    if matches!(family, FixtureFamily::Names | FixtureFamily::NativeTable) {
        phases.selector_setup = Some(TimedPhase::new(
            elapsed_ms(selector_setup),
            match family {
                FixtureFamily::Names => &["public_name_registration"],
                FixtureFamily::NativeTable => &["public_native_table_registration"],
                _ => unreachable!(),
            },
        ));
    }

    let expected = expected_typed_outputs(&shape, path, warm_repeats);
    let mut typed_outputs = Vec::new();
    let mut debug_outputs = Vec::new();
    let mut plan_stale_reason = None;
    let run_result = if path == CalibrationPath::Full {
        run_full_family(
            &mut workbook,
            &shape,
            warm_repeats,
            &mut phases,
            &mut telemetry,
            &mut typed_outputs,
            &mut gates,
            &mut structural,
        )
    } else if path == CalibrationPath::Sheetport {
        run_sheetport_family(
            &mut workbook,
            &shape,
            warm_repeats,
            &mut phases,
            &mut telemetry,
            &mut typed_outputs,
            &mut gates,
            &mut structural,
        )
    } else {
        run_direct_family(
            &mut workbook,
            &shape,
            path,
            warm_repeats,
            &mut phases,
            &mut telemetry,
            &mut typed_outputs,
            &mut plan_stale_reason,
        )
    };
    let mut typed_error = run_result.err().map(|error| format!("{error:#}"));
    if path == CalibrationPath::Plan && warm_repeats > 0 {
        gates.insert(
            "retained_plan_reused_across_value_edits".to_string(),
            typed_error.is_none(),
        );
    }
    debug_outputs.extend(
        typed_outputs
            .iter()
            .map(|values| values.iter().map(|value| format!("{value:?}")).collect()),
    );

    let typed_oracle_passed =
        typed_error.is_none() && typed_values_match(&typed_outputs, &expected);
    gates.insert("typed_output_oracle".to_string(), typed_oracle_passed);
    if family == FixtureFamily::Names && path == CalibrationPath::Plan {
        gates.insert(
            "deterministic_plan_stale_symbols".to_string(),
            plan_stale_reason.as_deref() == Some("symbols"),
        );
    }

    let graph_after_run = graph_snapshot(&workbook);
    let load_exact = graph_after_load.graph_formula_vertices == 0
        && graph_after_load.staged_formulas == formulas as usize
        && graph_after_load.dirty_vertices == 0;
    let setup_exact = graph_after_setup.graph_formula_vertices == DIRTY_FORMULAS as usize
        && graph_after_setup.staged_formulas == (formulas - DIRTY_FORMULAS) as usize
        && graph_after_setup.dirty_vertices == 9;
    gates.insert("fixture_load_counts_exact".to_string(), load_exact);
    gates.insert("dirty_setup_counts_exact".to_string(), setup_exact);

    if path == CalibrationPath::Full {
        gates.insert(
            "full_control_consumed_all_staging".to_string(),
            graph_after_run.staged_formulas == 0,
        );
        let dirty_oracle = full_dirty_oracle(family, warm_repeats);
        gates.insert(
            "full_control_consumed_unrelated_dirty".to_string(),
            graph_after_run.dirty_vertices == dirty_oracle.expected,
        );
        gates.insert(
            "full_control_dirty_residual_exact".to_string(),
            graph_after_run.dirty_vertices == dirty_oracle.expected,
        );
        structural.insert(
            "full_control_dirty_residual".to_string(),
            dirty_oracle.derivation,
        );
    } else {
        if shape.retained_formulas > 0 {
            let retained = graph_after_run.staged_formulas == shape.retained_formulas as usize;
            gates.insert("unrelated_staged_retained".to_string(), retained);
        }
        if family != FixtureFamily::Dynamic {
            gates.insert(
                "unrelated_dirty_retained".to_string(),
                graph_after_run.dirty_vertices >= 9,
            );
        }
    }
    let max_selected = telemetry
        .values()
        .map(|stats| stats.staged_selected)
        .max()
        .unwrap_or(0);
    let locality_ok = if path == CalibrationPath::Full {
        let dirty_oracle = full_dirty_oracle(family, warm_repeats);
        graph_after_run.staged_formulas == 0
            && graph_after_run.dirty_vertices == dirty_oracle.expected
    } else if family == FixtureFamily::Dynamic {
        let initial_preparation_stage = if path == CalibrationPath::Plan {
            "plan_build"
        } else {
            "first_evaluation"
        };
        let observed_mask = telemetry
            .get(initial_preparation_stage)
            .filter(|stats| stats.preparation_scope_level == 2)
            .map(|stats| stats.widening_reason_bits);
        let widened = observed_mask == Some(DYNAMIC_WIDENING_REASON_MASK);
        let exact_widened_selection = max_selected == u64::from(formulas - DIRTY_FORMULAS)
            && graph_after_run.staged_formulas == 0;
        gates.insert("workbook_widening_observed".to_string(), widened);
        gates.insert(
            "workbook_widening_selection_exact".to_string(),
            exact_widened_selection,
        );
        structural.insert(
            "dynamic_widening_scope".to_string(),
            "PrepareScope::Workbook".to_string(),
        );
        structural.insert(
            "dynamic_widening_reason_mask_expected".to_string(),
            "3 (0b11): DynamicReference | RuntimeTextReference".to_string(),
        );
        structural.insert(
            "dynamic_widening_reason_mask_observed".to_string(),
            observed_mask
                .map(|mask| format!("{mask} ({mask:#04b}) at {initial_preparation_stage}"))
                .unwrap_or_else(|| format!("not observed at {initial_preparation_stage}")),
        );
        widened && exact_widened_selection
    } else {
        let preparation_oracle = if family == FixtureFamily::Names {
            formulas - DIRTY_FORMULAS - shape.retained_formulas
        } else {
            reachable_oracle
        };
        let exact = max_selected == u64::from(preparation_oracle);
        gates.insert(
            "preparation_selected_formula_count_exact".to_string(),
            exact,
        );
        if family == FixtureFamily::Names {
            structural.insert(
                "name_preparation_package_oracle".to_string(),
                format!(
                    "{preparation_oracle} formulas: target closure plus the four-formula Names source package"
                ),
            );
        }
        exact
    };
    match family {
        FixtureFamily::CrossSheet => {
            gates.insert(
                "composed_cross_sheet_oracle".to_string(),
                typed_oracle_passed,
            );
        }
        FixtureFamily::Names => {
            gates.insert(
                "rebuilt_name_binding_outputs".to_string(),
                typed_oracle_passed,
            );
        }
        FixtureFamily::Layout if path == CalibrationPath::Sheetport => {
            gates.insert(
                "layout_package_commit_selected_chain_c4_c10".to_string(),
                max_selected == u64::from(shape.reachable_formulas),
            );
            gates.insert(
                "layout_six_retained_sheet_formulas_staged".to_string(),
                graph_after_run.staged_formulas == shape.retained_formulas as usize
                    && shape.retained_formulas == 6,
            );
        }
        FixtureFamily::NativeTable => {
            gates.insert(
                "headers_body_totals_typed".to_string(),
                typed_outputs.iter().all(|values| values.len() == 12),
            );
        }
        FixtureFamily::Dynamic if path != CalibrationPath::Sheetport => {
            gates.insert(
                "explicit_ref_error_preserved".to_string(),
                typed_outputs.iter().all(|values| {
                    values.get(1) == Some(&TypedOracleValue::Error("#REF!".to_string()))
                }),
            );
        }
        _ => {}
    }

    if typed_error.is_none() {
        let failed = gates
            .iter()
            .filter_map(|(gate, passed)| (!passed).then_some(gate.as_str()))
            .collect::<Vec<_>>();
        if !failed.is_empty() {
            typed_error = Some(format!("family gates failed: {}", failed.join(", ")));
        }
    }

    let flat = debug_outputs.iter().flatten().cloned().collect::<Vec<_>>();
    let (current_rss_bytes, peak_rss_bytes) = process_memory_bytes();

    Ok(ChildReport {
        schema_version: 3,
        family,
        path_schema_version: 3,
        selector_set: selector_set(family, path).to_string(),
        path,
        scope,
        formulas,
        reachable_oracle,
        fixture_sha256,
        status: if typed_error.is_none() {
            "ok".to_string()
        } else {
            "path_error".to_string()
        },
        phases,
        outputs: debug_outputs,
        analytical_expected_outputs: Vec::new(),
        analytical_output_oracle_passed: Some(typed_oracle_passed),
        typed_outputs,
        typed_expected_outputs: expected,
        family_gates: gates,
        structural_oracles: structural,
        plan_stale_reason,
        output_checksum: typed_error.is_none().then(|| checksum_values(&flat)),
        typed_error,
        telemetry,
        graph_after_load: Some(graph_after_load),
        graph_after_setup: Some(graph_after_setup),
        graph_after_run: Some(graph_after_run.clone()),
        unrelated_staged_retained: (path != CalibrationPath::Full && shape.retained_formulas > 0)
            .then_some(graph_after_run.staged_formulas == shape.retained_formulas as usize),
        unrelated_dirty_retained: (path != CalibrationPath::Full
            && family != FixtureFamily::Dynamic)
            .then_some(graph_after_run.dirty_vertices >= 9),
        oracle_within_one_percent: Some(locality_ok),
        exact_fixture_counts_passed: Some(load_exact && setup_exact),
        exact_locality_counts_passed: Some(locality_ok),
        current_rss_bytes,
        peak_rss_bytes,
    })
}

const DYNAMIC_WIDENING_REASON_MASK: u64 = 0b11;

struct FullDirtyOracle {
    expected: usize,
    derivation: String,
}

fn full_dirty_oracle(family: FixtureFamily, warm_repeats: usize) -> FullDirtyOracle {
    // Derive residuals from graph-producing operations, not observed baselines
    // or family-specific fixture totals.
    let dirty_setup_input = 1;
    let warm_input = usize::from(warm_repeats > 0);
    let volatile_dynamic_formulas = usize::from(family == FixtureFamily::Dynamic) * 2;
    let table_metadata_vertex =
        usize::from(family == FixtureFamily::NativeTable && warm_repeats > 0);
    let expected =
        dirty_setup_input + warm_input + volatile_dynamic_formulas + table_metadata_vertex;
    let table_proof = if family == FixtureFamily::NativeTable {
        if warm_repeats > 0 {
            "; +1 C6Table metadata vertex dirtied through its registered table-range dependency when warm evaluation changes table formula values, then retained because it is not a formula evaluation vertex (the public baseline exposes only its aggregate residual, not vertex identity)"
        } else {
            "; +0 C6Table metadata residual because no warm edit changes a table-range value"
        }
    } else {
        ""
    };
    FullDirtyOracle {
        expected,
        derivation: format!(
            "expected {expected}: 1 edited non-formula Dirty!A1 source; +{warm_input} edited non-formula {}!A1 source; +{volatile_dynamic_formulas} volatile INDIRECT formulas re-dirtied after evaluation{table_proof}",
            input_sheet(family)
        ),
    }
}

fn input_sheet(family: FixtureFamily) -> &'static str {
    if family == FixtureFamily::CrossSheet {
        "ChainA"
    } else {
        "Chain"
    }
}

fn selector_set(family: FixtureFamily, path: CalibrationPath) -> &'static str {
    match (family, path) {
        (FixtureFamily::Names, CalibrationPath::Sheetport) => "workbook_name",
        (FixtureFamily::Names, _) => "workbook_and_sheet_names",
        (FixtureFamily::NativeTable, _) => "headers_body_totals",
        (FixtureFamily::Layout, _) => "bounded_layout_table",
        (FixtureFamily::Dynamic, CalibrationPath::Sheetport) => "dynamic_value",
        (FixtureFamily::Dynamic, _) => "dynamic_value_and_explicit_error",
        (FixtureFamily::CrossSheet, _) => "cross_sheet_terminal",
        _ => "scalar_terminals",
    }
}

fn setup_selectors(workbook: &mut Workbook, family: FixtureFamily) -> Result<()> {
    match family {
        FixtureFamily::Names => {
            workbook.define_named_range(
                "WorkbookOutput",
                &RangeAddress::new("Names", 1, 2, 1, 2).unwrap(),
                NamedRangeScope::Workbook,
            )?;
            workbook.define_named_range(
                "SheetOutput",
                &RangeAddress::new("Names", 2, 2, 2, 2).unwrap(),
                NamedRangeScope::Sheet,
            )?;
        }
        FixtureFamily::NativeTable => {
            let sheet = workbook.engine().sheet_id("Table").context("Table sheet")?;
            workbook.engine_mut().define_table(
                "C6Table",
                RangeRef::new(
                    CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
                    CellRef::new(sheet, Coord::from_excel(3, 4, true, true)),
                ),
                true,
                vec![
                    "Label".to_string(),
                    "Count".to_string(),
                    "Value".to_string(),
                    "AsOf".to_string(),
                ],
                true,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn family_targets(shape: &FamilyFixtureShape) -> Vec<EvaluationTarget> {
    match shape.family {
        FixtureFamily::CrossSheet => vec![EvaluationTarget::Cell {
            sheet: shape.terminal_sheet.clone(),
            row: shape.terminal_row,
            col: shape.terminal_col,
        }],
        FixtureFamily::Names => vec![
            EvaluationTarget::Name {
                name: "WorkbookOutput".to_string(),
                scope_sheet: None,
            },
            EvaluationTarget::Name {
                name: "SheetOutput".to_string(),
                scope_sheet: Some("Names".to_string()),
            },
        ],
        FixtureFamily::NativeTable => vec![
            EvaluationTarget::Table {
                name: "C6Table".to_string(),
                selection: TableSelection::Headers,
            },
            EvaluationTarget::Table {
                name: "C6Table".to_string(),
                selection: TableSelection::Data,
            },
            EvaluationTarget::Table {
                name: "C6Table".to_string(),
                selection: TableSelection::Totals,
            },
        ],
        FixtureFamily::Dynamic => vec![EvaluationTarget::Range(
            RangeAddress::new("Dynamic", 1, 2, 2, 2).unwrap(),
        )],
        _ => unreachable!("direct target family"),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_full_family(
    workbook: &mut Workbook,
    shape: &FamilyFixtureShape,
    warm_repeats: usize,
    phases: &mut PhaseTimings,
    telemetry: &mut BTreeMap<String, EngineTelemetry>,
    outputs: &mut Vec<Vec<TypedOracleValue>>,
    gates: &mut BTreeMap<String, bool>,
    structural: &mut BTreeMap<String, String>,
) -> Result<()> {
    if matches!(
        shape.family,
        FixtureFamily::Layout | FixtureFamily::NativeTable
    ) {
        let bind = Instant::now();
        let yaml = if shape.family == FixtureFamily::Layout {
            layout_manifest(shape.terminal_row)
        } else {
            table_manifest()
        };
        let manifest = Manifest::from_yaml_str(&yaml)?;
        let mut sheetport = SheetPort::new(workbook, manifest)?;
        phases.bind_target_resolution = Some(TimedPhase::new(
            elapsed_ms(bind),
            &[
                "manifest_parse",
                "manifest_bind",
                "workbook_selector_validation",
            ],
        ));
        let prepare = Instant::now();
        sheetport.workbook_mut().prepare_graph_all()?;
        phases.preparation_plan_build =
            Some(TimedPhase::new(elapsed_ms(prepare), &["prepare_graph_all"]));
        let evaluate = Instant::now();
        sheetport.workbook_mut().evaluate_all()?;
        phases.first_evaluation = Some(TimedPhase::new(elapsed_ms(evaluate), &["evaluate_all"]));
        telemetry.insert(
            "first_evaluation".to_string(),
            collect_telemetry(sheetport.workbook()),
        );
        let read = Instant::now();
        let snapshot = sheetport.read_outputs()?;
        outputs.push(sheetport_output(&snapshot, shape.family)?);
        phases.output_read = Some(TimedPhase::new(
            elapsed_ms(read),
            &["actual_selector_resolution", "typed_output_read"],
        ));
        for repeat in 0..warm_repeats {
            let edit = Instant::now();
            sheetport.workbook_mut().set_value(
                input_sheet(shape.family),
                1,
                1,
                LiteralValue::Number(2.0 + repeat as f64),
            )?;
            phases.edit.push(TimedPhase::new(
                elapsed_ms(edit),
                &["value_edit_inside_selected_closure"],
            ));
            let evaluate = Instant::now();
            sheetport.workbook_mut().evaluate_all()?;
            phases
                .warm_evaluation
                .push(TimedPhase::new(elapsed_ms(evaluate), &["evaluate_all"]));
            telemetry.insert(
                format!("warm_{repeat}"),
                collect_telemetry(sheetport.workbook()),
            );
            let read = Instant::now();
            let snapshot = sheetport.read_outputs()?;
            outputs.push(sheetport_output(&snapshot, shape.family)?);
            phases.warm_output_read.push(TimedPhase::new(
                elapsed_ms(read),
                &["actual_selector_resolution", "typed_output_read"],
            ));
        }
        if shape.family == FixtureFamily::Layout {
            let exact_bounds = outputs.iter().all(|values| values.len() == 5);
            record_layout_bounds_gates(exact_bounds, gates, structural);
        }
        return Ok(());
    }

    let resolve = Instant::now();
    phases.bind_target_resolution = Some(TimedPhase::new(
        elapsed_ms(resolve),
        &["typed_output_address_resolution"],
    ));
    let prepare = Instant::now();
    workbook.prepare_graph_all()?;
    phases.preparation_plan_build =
        Some(TimedPhase::new(elapsed_ms(prepare), &["prepare_graph_all"]));
    let evaluate = Instant::now();
    workbook.evaluate_all()?;
    phases.first_evaluation = Some(TimedPhase::new(elapsed_ms(evaluate), &["evaluate_all"]));
    telemetry.insert("first_evaluation".to_string(), collect_telemetry(workbook));
    let read = Instant::now();
    outputs.push(read_family_outputs(workbook, shape));
    phases.output_read = Some(TimedPhase::new(
        elapsed_ms(read),
        &["typed_direct_output_read"],
    ));
    for repeat in 0..warm_repeats {
        let edit = Instant::now();
        workbook.set_value(
            input_sheet(shape.family),
            1,
            1,
            LiteralValue::Number(2.0 + repeat as f64),
        )?;
        phases.edit.push(TimedPhase::new(
            elapsed_ms(edit),
            &["value_edit_inside_selected_closure"],
        ));
        let evaluate = Instant::now();
        workbook.evaluate_all()?;
        phases
            .warm_evaluation
            .push(TimedPhase::new(elapsed_ms(evaluate), &["evaluate_all"]));
        telemetry.insert(format!("warm_{repeat}"), collect_telemetry(workbook));
        let read = Instant::now();
        outputs.push(read_family_outputs(workbook, shape));
        phases.warm_output_read.push(TimedPhase::new(
            elapsed_ms(read),
            &["typed_direct_output_read"],
        ));
    }
    if shape.family == FixtureFamily::Names {
        let probe = Instant::now();
        rebind_names(workbook)?;
        workbook.evaluate_all()?;
        outputs.push(read_family_outputs(workbook, shape));
        phases.name_binding_probe = Some(TimedPhase::new(
            elapsed_ms(probe),
            &[
                "public_name_binding_update",
                "evaluate_all",
                "typed_direct_output_read",
            ],
        ));
        telemetry.insert(
            "name_binding_probe".to_string(),
            collect_telemetry(workbook),
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_direct_family(
    workbook: &mut Workbook,
    shape: &FamilyFixtureShape,
    path: CalibrationPath,
    warm_repeats: usize,
    phases: &mut PhaseTimings,
    telemetry: &mut BTreeMap<String, EngineTelemetry>,
    outputs: &mut Vec<Vec<TypedOracleValue>>,
    plan_stale_reason: &mut Option<String>,
) -> Result<()> {
    let resolve = Instant::now();
    let targets = family_targets(shape);
    phases.bind_target_resolution = Some(TimedPhase::new(
        elapsed_ms(resolve),
        &["typed_target_construction"],
    ));

    let mut plan = None;
    if path == CalibrationPath::Plan {
        let started = Instant::now();
        plan = Some(workbook.build_recalc_plan_for_targets(&targets)?);
        phases.preparation_plan_build = Some(TimedPhase::new(
            elapsed_ms(started),
            &["target_preparation", "revision_bound_plan_build"],
        ));
        telemetry.insert("plan_build".to_string(), collect_telemetry(workbook));
    }

    let first = Instant::now();
    evaluate_direct(workbook, shape, path, &targets, plan.as_ref())?;
    phases.first_evaluation = Some(TimedPhase::new(
        elapsed_ms(first),
        match path {
            CalibrationPath::Cells => &[
                "target_preparation",
                "ephemeral_plan_build",
                "evaluation",
                "output_return",
            ],
            CalibrationPath::Targets => &["target_preparation", "evaluation"],
            CalibrationPath::Plan => &["evaluate_with_plan"],
            _ => unreachable!(),
        },
    ));
    telemetry.insert("first_evaluation".to_string(), collect_telemetry(workbook));
    let read = Instant::now();
    outputs.push(read_family_outputs(workbook, shape));
    phases.output_read = Some(TimedPhase::new(
        elapsed_ms(read),
        &["typed_direct_output_read"],
    ));

    for repeat in 0..warm_repeats {
        let edit = Instant::now();
        workbook.set_value(
            input_sheet(shape.family),
            1,
            1,
            LiteralValue::Number(2.0 + repeat as f64),
        )?;
        phases.edit.push(TimedPhase::new(
            elapsed_ms(edit),
            &["value_edit_inside_selected_closure"],
        ));
        let evaluate = Instant::now();
        evaluate_direct(workbook, shape, path, &targets, plan.as_ref())?;
        phases
            .warm_evaluation
            .push(TimedPhase::new(elapsed_ms(evaluate), &["evaluation"]));
        telemetry.insert(format!("warm_{repeat}"), collect_telemetry(workbook));
        let read = Instant::now();
        outputs.push(read_family_outputs(workbook, shape));
        phases.warm_output_read.push(TimedPhase::new(
            elapsed_ms(read),
            &["typed_direct_output_read"],
        ));
    }
    if shape.family == FixtureFamily::Names {
        let probe = Instant::now();
        rebind_names(workbook)?;
        if let Some(old_plan) = plan.as_ref() {
            let error = workbook
                .evaluate_with_plan(old_plan)
                .expect_err("name binding update must stale retained plan");
            *plan_stale_reason = stale_reason(&error).map(str::to_string);
            plan = Some(workbook.build_recalc_plan_for_targets(&targets)?);
        }
        evaluate_direct(workbook, shape, path, &targets, plan.as_ref())?;
        outputs.push(read_family_outputs(workbook, shape));
        phases.name_binding_probe = Some(TimedPhase::new(
            elapsed_ms(probe),
            if path == CalibrationPath::Plan {
                &[
                    "public_name_binding_update",
                    "exact_symbols_stale_probe",
                    "single_revision_bound_plan_rebuild",
                    "evaluation",
                    "typed_direct_output_read",
                ]
            } else {
                &[
                    "public_name_binding_update",
                    "evaluation",
                    "typed_direct_output_read",
                ]
            },
        ));
        telemetry.insert(
            "name_binding_probe".to_string(),
            collect_telemetry(workbook),
        );
    }
    Ok(())
}

fn evaluate_direct(
    workbook: &mut Workbook,
    shape: &FamilyFixtureShape,
    path: CalibrationPath,
    targets: &[EvaluationTarget],
    plan: Option<&formualizer_eval::engine::RecalcPlan>,
) -> Result<()> {
    match path {
        CalibrationPath::Cells => {
            let cells = match shape.family {
                FixtureFamily::CrossSheet => vec![(
                    shape.terminal_sheet.as_str(),
                    shape.terminal_row,
                    shape.terminal_col,
                )],
                FixtureFamily::Dynamic => vec![("Dynamic", 1, 2), ("Dynamic", 2, 2)],
                _ => unreachable!("cells family"),
            };
            workbook.evaluate_cells(&cells)?;
        }
        CalibrationPath::Targets => {
            workbook.evaluate_targets(targets)?;
        }
        CalibrationPath::Plan => {
            workbook.evaluate_with_plan(plan.context("retained target plan")?)?;
        }
        _ => unreachable!("direct family path"),
    }
    Ok(())
}

fn rebind_names(workbook: &mut Workbook) -> Result<()> {
    workbook.update_named_range(
        "WorkbookOutput",
        &RangeAddress::new("Names", 3, 2, 3, 2).unwrap(),
        NamedRangeScope::Workbook,
    )?;
    workbook.update_named_range(
        "SheetOutput",
        &RangeAddress::new("Names", 4, 2, 4, 2).unwrap(),
        NamedRangeScope::Sheet,
    )?;
    Ok(())
}

fn stale_reason(error: &IoError) -> Option<&'static str> {
    let IoError::Engine(excel) = error else {
        return None;
    };
    let ExcelErrorExtra::PlanStale { reason } = excel.extra else {
        return None;
    };
    Some(match reason {
        PlanStaleReason::Engine => "engine",
        PlanStaleReason::Provider => "provider",
        PlanStaleReason::Semantic => "semantic",
        PlanStaleReason::Budget => "budget",
        PlanStaleReason::Staged => "staged",
        PlanStaleReason::Symbols => "symbols",
        PlanStaleReason::Authority => "authority",
        PlanStaleReason::SpanGeneration => "span_generation",
        PlanStaleReason::Graph => "graph",
    })
}

fn read_family_outputs(workbook: &Workbook, shape: &FamilyFixtureShape) -> Vec<TypedOracleValue> {
    match shape.family {
        FixtureFamily::CrossSheet => vec![literal_to_typed(
            workbook
                .get_value(
                    &shape.terminal_sheet,
                    shape.terminal_row,
                    shape.terminal_col,
                )
                .unwrap_or(LiteralValue::Empty),
        )],
        FixtureFamily::Names => vec![
            literal_to_typed(
                workbook
                    .resolved_name_value("WorkbookOutput", None)
                    .unwrap_or(LiteralValue::Empty),
            ),
            literal_to_typed(
                workbook
                    .resolved_name_value("SheetOutput", Some("Names"))
                    .unwrap_or(LiteralValue::Empty),
            ),
        ],
        FixtureFamily::NativeTable => {
            let mut out = Vec::with_capacity(12);
            for row in 1..=3 {
                for col in 1..=4 {
                    let value = workbook
                        .get_value("Table", row, col)
                        .unwrap_or(LiteralValue::Empty);
                    out.push(if col == 2 && row > 1 {
                        integer_to_typed(value)
                    } else {
                        literal_to_typed(value)
                    });
                }
            }
            out
        }
        FixtureFamily::Dynamic => (1..=2)
            .map(|row| {
                literal_to_typed(
                    workbook
                        .get_value("Dynamic", row, 2)
                        .unwrap_or(LiteralValue::Empty),
                )
            })
            .collect(),
        _ => unreachable!("direct output family"),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_sheetport_family(
    workbook: &mut Workbook,
    shape: &FamilyFixtureShape,
    warm_repeats: usize,
    phases: &mut PhaseTimings,
    telemetry: &mut BTreeMap<String, EngineTelemetry>,
    outputs: &mut Vec<Vec<TypedOracleValue>>,
    gates: &mut BTreeMap<String, bool>,
    structural: &mut BTreeMap<String, String>,
) -> Result<()> {
    let bind = Instant::now();
    let yaml = match shape.family {
        FixtureFamily::Layout => layout_manifest(shape.terminal_row),
        FixtureFamily::NativeTable => table_manifest(),
        FixtureFamily::CrossSheet | FixtureFamily::Names | FixtureFamily::Dynamic => {
            scalar_manifest(shape.family, &shape.terminal_sheet, shape.terminal_row)
        }
        FixtureFamily::Scalar => unreachable!(),
    };
    let manifest = Manifest::from_yaml_str(&yaml)?;
    let mut sheetport = SheetPort::new(workbook, manifest)?;
    phases.bind_target_resolution = Some(TimedPhase::new(
        elapsed_ms(bind),
        &[
            "manifest_parse",
            "manifest_bind",
            "workbook_selector_validation",
        ],
    ));

    let first = Instant::now();
    let snapshot = sheetport.evaluate_once(EvalOptions::default())?;
    phases.first_evaluation = Some(TimedPhase::new(
        elapsed_ms(first),
        &[
            "actual_selector_resolution",
            "target_preparation",
            "evaluation",
            "typed_output_read",
        ],
    ));
    telemetry.insert(
        "first_evaluation".to_string(),
        collect_telemetry(sheetport.workbook()),
    );
    outputs.push(sheetport_output(&snapshot, shape.family)?);

    if shape.family == FixtureFamily::Names {
        for repeat in 0..warm_repeats {
            let edit = Instant::now();
            sheetport.workbook_mut().set_value(
                input_sheet(shape.family),
                1,
                1,
                LiteralValue::Number(2.0 + repeat as f64),
            )?;
            phases.edit.push(TimedPhase::new(
                elapsed_ms(edit),
                &["value_edit_inside_selected_closure"],
            ));
            let evaluate = Instant::now();
            let snapshot = sheetport.evaluate_once(EvalOptions::default())?;
            phases.warm_evaluation.push(TimedPhase::new(
                elapsed_ms(evaluate),
                &[
                    "actual_name_selector_resolution",
                    "target_evaluation",
                    "typed_output_read",
                ],
            ));
            telemetry.insert(
                format!("warm_{repeat}"),
                collect_telemetry(sheetport.workbook()),
            );
            outputs.push(sheetport_output(&snapshot, shape.family)?);
        }
        let probe = Instant::now();
        rebind_names(sheetport.workbook_mut())?;
        let snapshot = sheetport.evaluate_once(EvalOptions::default())?;
        outputs.push(sheetport_output(&snapshot, shape.family)?);
        phases.name_binding_probe = Some(TimedPhase::new(
            elapsed_ms(probe),
            &[
                "public_workbook_name_binding_update",
                "actual_name_selector_resolution",
                "target_evaluation",
                "typed_output_read",
            ],
        ));
        telemetry.insert(
            "name_binding_probe".to_string(),
            collect_telemetry(sheetport.workbook()),
        );
        return Ok(());
    }

    let completion_ms = Arc::new(Mutex::new(Vec::new()));
    let last = Arc::new(Mutex::new(Instant::now()));
    let completion_capture = Arc::clone(&completion_ms);
    let last_capture = Arc::clone(&last);
    let batch_options = BatchOptions {
        progress: Some(Box::new(move |_| {
            let now = Instant::now();
            let mut previous = last_capture.lock().expect("progress clock lock");
            completion_capture
                .lock()
                .expect("progress samples lock")
                .push(now.duration_since(*previous).as_secs_f64() * 1000.0);
            *previous = now;
        })),
        ..BatchOptions::default()
    };
    let plan_started = Instant::now();
    let mut batch = sheetport.batch(batch_options)?;
    phases.preparation_plan_build = Some(TimedPhase::new(
        elapsed_ms(plan_started),
        &[
            "input_baseline_read",
            "actual_selector_resolution",
            "target_preparation",
            "revision_bound_plan_build",
        ],
    ));
    telemetry.insert(
        "sheetport_batch_plan_build".to_string(),
        collect_telemetry(batch.workbook_for_benchmark()),
    );
    let cases = (0..warm_repeats)
        .map(|repeat| {
            let mut update = InputUpdate::new();
            update.insert(
                "input",
                PortValue::Scalar(LiteralValue::Number(2.0 + repeat as f64)),
            );
            BatchInput::new(format!("warm_{repeat}"), update)
        })
        .collect::<Vec<_>>();
    let batch_started = Instant::now();
    *last.lock().expect("progress clock lock") = batch_started;
    let results = batch.run(cases)?;
    let batch_ms = elapsed_ms(batch_started);
    telemetry.insert(
        "sheetport_batch_execution".to_string(),
        collect_telemetry(batch.workbook_for_benchmark()),
    );
    let restored_input = batch
        .workbook_for_benchmark()
        .get_value(input_sheet(shape.family), 1, 1);
    let below_guard_inside_envelope_evaluated =
        (shape.family == FixtureFamily::Layout).then(|| {
            matches!(
                batch.workbook_for_benchmark().get_value("Layout", 4, 3),
                Some(LiteralValue::Number(value)) if (value - 999.0).abs() < f64::EPSILON
            ) || matches!(
                batch.workbook_for_benchmark().get_value("Layout", 4, 3),
                Some(LiteralValue::Int(999))
            )
        });
    let below_envelope_committed_but_unevaluated =
        (shape.family == FixtureFamily::Layout).then(|| {
            batch
                .workbook_for_benchmark()
                .get_value("Layout", LAYOUT_BELOW_ENVELOPE_ROW, 3)
                .is_none_or(|value| value == LiteralValue::Empty)
        });
    let samples = completion_ms.lock().expect("progress samples lock").clone();
    anyhow::ensure!(
        samples.len() == warm_repeats,
        "SheetPort progress count: actual {}, expected {warm_repeats}",
        samples.len()
    );
    for sample in &samples {
        phases.warm_evaluation.push(TimedPhase::new(
            *sample,
            &["input_edit", "plan_evaluation", "typed_output_read"],
        ));
    }
    let sample_ms = samples.iter().sum::<f64>();
    anyhow::ensure!(
        batch_ms >= sample_ms,
        "SheetPort batch restoration accounting is negative"
    );
    for result in results {
        outputs.push(sheetport_output(&result.outputs, shape.family)?);
    }
    let restoration_passed = matches!(
        restored_input,
        Some(LiteralValue::Number(value)) if (value - 1.0).abs() < f64::EPSILON
    ) || matches!(restored_input, Some(LiteralValue::Int(1)));
    gates.insert("batch_baseline_restored".to_string(), restoration_passed);
    phases.batch_restore = Some(TimedPhase::new(
        batch_ms - sample_ms,
        &[
            "baseline_restore_edit",
            "plan_evaluation",
            "typed_output_read",
        ],
    ));

    if shape.family == FixtureFamily::Layout {
        let first = outputs.first().cloned().unwrap_or_default();
        let exact_bounds = first.len() == 5;
        record_layout_bounds_gates(exact_bounds, gates, structural);
        gates.insert(
            "layout_below_guard_inside_envelope_evaluated".to_string(),
            below_guard_inside_envelope_evaluated.unwrap_or(false),
        );
        gates.insert(
            "layout_below_envelope_committed_but_unevaluated".to_string(),
            below_envelope_committed_but_unevaluated.unwrap_or(false),
        );
    }
    Ok(())
}

fn record_layout_bounds_gates(
    exact_bounds: bool,
    gates: &mut BTreeMap<String, bool>,
    structural: &mut BTreeMap<String, String>,
) {
    let envelope_extends_past_guard = LAYOUT_MAX_SCAN_ROWS > LAYOUT_BLANK_GUARD_ROW;
    gates.insert(
        "layout_exact_resolved_bounds_A2_D2".to_string(),
        exact_bounds,
    );
    gates.insert(
        "layout_scan_envelope_extends_past_blank_guard".to_string(),
        envelope_extends_past_guard,
    );
    gates.insert(
        "layout_preparation_envelope_A2_D9".to_string(),
        LAYOUT_PREPARATION_ENVELOPE_END_ROW == 1 + LAYOUT_MAX_SCAN_ROWS,
    );
    gates.insert(
        "layout_blank_guard_terminated_before_envelope".to_string(),
        exact_bounds && envelope_extends_past_guard,
    );
    structural.insert(
        "resolved_layout_bounds".to_string(),
        format!(
            "Layout!A2:D2: blank guard row {LAYOUT_BLANK_GUARD_ROW} terminated output resolution before max_scan_rows={LAYOUT_MAX_SCAN_ROWS}; conservative preparation envelope A2:D{LAYOUT_PREPARATION_ENVELOPE_END_ROW} evaluates below-guard C4; Layout-sheet package fallback also commits C{LAYOUT_BELOW_ENVELOPE_ROW}, which remains Empty, while six separate Retained-sheet formulas remain staged"
        ),
    );
}

fn sheetport_output(
    snapshot: &formualizer_sheetport::OutputSnapshot,
    family: FixtureFamily,
) -> Result<Vec<TypedOracleValue>> {
    match family {
        FixtureFamily::CrossSheet | FixtureFamily::Names => match snapshot.get("output") {
            Some(PortValue::Scalar(value)) => Ok(vec![literal_to_typed(value.clone())]),
            other => anyhow::bail!("expected scalar SheetPort output, got {other:?}"),
        },
        FixtureFamily::Dynamic => match snapshot.get("value") {
            Some(PortValue::Scalar(value)) => Ok(vec![literal_to_typed(value.clone())]),
            other => anyhow::bail!("expected dynamic SheetPort scalar, got {other:?}"),
        },
        FixtureFamily::Layout => {
            let mut values = table_port_values(snapshot, "rows", false)?;
            match snapshot.get("breadth") {
                Some(PortValue::Scalar(value)) => values.push(literal_to_typed(value.clone())),
                other => anyhow::bail!("expected layout breadth scalar, got {other:?}"),
            }
            Ok(values)
        }
        FixtureFamily::NativeTable => {
            let mut values = Vec::new();
            values.extend(table_port_values(snapshot, "headers", true)?);
            values.extend(table_port_values(snapshot, "body", false)?);
            values.extend(table_port_values(snapshot, "totals", false)?);
            Ok(values)
        }
        FixtureFamily::Scalar => unreachable!(),
    }
}

fn table_port_values(
    snapshot: &formualizer_sheetport::OutputSnapshot,
    id: &str,
    header: bool,
) -> Result<Vec<TypedOracleValue>> {
    let Some(PortValue::Table(table)) = snapshot.get(id) else {
        anyhow::bail!("expected table SheetPort output `{id}`");
    };
    let mut values = Vec::new();
    for row in &table.rows {
        for column in ["Label", "Count", "Value", "AsOf"] {
            let value = row
                .values
                .get(column)
                .cloned()
                .unwrap_or(LiteralValue::Empty);
            values.push(if !header && column == "Count" {
                integer_to_typed(value)
            } else {
                literal_to_typed(value)
            });
        }
    }
    Ok(values)
}

fn integer_to_typed(value: LiteralValue) -> TypedOracleValue {
    match value {
        LiteralValue::Int(value) => TypedOracleValue::Integer(value),
        LiteralValue::Number(value) if value.fract() == 0.0 => {
            TypedOracleValue::Integer(value as i64)
        }
        other => literal_to_typed(other),
    }
}

fn literal_to_typed(value: LiteralValue) -> TypedOracleValue {
    match value {
        LiteralValue::Text(value) => TypedOracleValue::String(value),
        LiteralValue::Int(value) => TypedOracleValue::Integer(value),
        LiteralValue::Number(value) => TypedOracleValue::Number(value),
        LiteralValue::Date(value) => TypedOracleValue::Date(value.to_string()),
        LiteralValue::DateTime(value) => TypedOracleValue::Date(value.date().to_string()),
        LiteralValue::Boolean(value) => TypedOracleValue::Boolean(value),
        LiteralValue::Empty => TypedOracleValue::Empty,
        LiteralValue::Error(error) => TypedOracleValue::Error(error.kind.to_string()),
        other => TypedOracleValue::String(format!("{other:?}")),
    }
}

fn typed_values_match(
    actual: &[Vec<TypedOracleValue>],
    expected: &[Vec<TypedOracleValue>],
) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(actual, expected)| match (actual, expected) {
                        (TypedOracleValue::Number(actual), TypedOracleValue::Number(expected)) => {
                            (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0)
                        }
                        _ => actual == expected,
                    })
        })
}
