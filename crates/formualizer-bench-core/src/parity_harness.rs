use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{EvalConfig, FormulaPlaneMode};
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
use serde::Serialize;

use crate::scenarios::common::{fixture_path, set_invariant_scale};
use crate::scenarios::{
    ExpectedDivergenceAction, Scenario, ScenarioBuildCtx, ScenarioPhase, ScenarioScale,
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CellDivergence {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub off_value: String,
    pub auth_value: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParityPhaseReport {
    pub phase: String,
    pub off_duration_ms: f64,
    pub auth_duration_ms: f64,
    pub cells_compared: u64,
    pub divergences: Vec<CellDivergence>,
    pub timed_out: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParityScenarioReport {
    pub scenario_id: String,
    pub scale: String,
    pub fixture_path: String,
    pub phases: Vec<ParityPhaseReport>,
    pub phases_passed: usize,
    pub phases_failed: usize,
    pub total_divergences: usize,
    pub expected_divergence: Option<String>,
    pub skipped: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ParityOptions {
    pub phase_timeout_ms: u64,
    pub max_divergences_per_phase: usize,
    pub enable_parallel: bool,
}

impl Default for ParityOptions {
    fn default() -> Self {
        Self {
            phase_timeout_ms: 5_000,
            max_divergences_per_phase: 10,
            enable_parallel: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Off,
    Auth,
}

impl Mode {
    fn eval_mode(self) -> FormulaPlaneMode {
        match self {
            Mode::Off => FormulaPlaneMode::Off,
            Mode::Auth => FormulaPlaneMode::AuthoritativeExperimental,
        }
    }
}

pub fn default_phase_timeout_ms(scale: ScenarioScale) -> u64 {
    match scale {
        ScenarioScale::Small => 5_000,
        ScenarioScale::Medium => 15_000,
        ScenarioScale::Large => 60_000,
    }
}

pub fn scale_title(scale: ScenarioScale) -> &'static str {
    match scale {
        ScenarioScale::Small => "Small",
        ScenarioScale::Medium => "Medium",
        ScenarioScale::Large => "Large",
    }
}

pub fn parse_scale(scale: &str) -> Result<ScenarioScale> {
    match scale.to_ascii_lowercase().as_str() {
        "small" => Ok(ScenarioScale::Small),
        "medium" => Ok(ScenarioScale::Medium),
        "large" => Ok(ScenarioScale::Large),
        other => bail!("unknown --scale '{other}', expected small|medium|large"),
    }
}

pub fn run_scenario_parity(
    scenario: &dyn Scenario,
    scale: ScenarioScale,
    label: &str,
    fixture_dir: &Path,
    options: ParityOptions,
) -> ParityScenarioReport {
    let expected = scenario.expected_divergences();
    let expected_note = if expected.is_empty() {
        None
    } else {
        Some(
            expected
                .iter()
                .map(|entry| format!("{:?}: {}", entry.phase, entry.reason))
                .collect::<Vec<_>>()
                .join("; "),
        )
    };
    if expected
        .iter()
        .any(|entry| entry.action == ExpectedDivergenceAction::Skip)
    {
        return ParityScenarioReport {
            scenario_id: scenario.id().to_string(),
            scale: scale_title(scale).to_string(),
            fixture_path: String::new(),
            phases: Vec::new(),
            phases_passed: 0,
            phases_failed: 0,
            total_divergences: 0,
            expected_divergence: expected_note,
            skipped: true,
        };
    }

    match catch_unwind(AssertUnwindSafe(|| {
        run_scenario_parity_inner(scenario, scale, label, fixture_dir, options, expected_note)
    })) {
        Ok(Ok(report)) => report,
        Ok(Err(err)) => error_report(scenario, scale, err),
        Err(payload) => {
            let reason = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "non-string panic payload".to_string()
            };
            let mut report = error_report(scenario, scale, anyhow::anyhow!("panic: {reason}"));
            report.phases[0].phase = "Panic".to_string();
            report
        }
    }
}

fn error_report(
    scenario: &dyn Scenario,
    scale: ScenarioScale,
    err: anyhow::Error,
) -> ParityScenarioReport {
    ParityScenarioReport {
        scenario_id: scenario.id().to_string(),
        scale: scale_title(scale).to_string(),
        fixture_path: String::new(),
        phases: vec![ParityPhaseReport {
            phase: "HarnessError".to_string(),
            off_duration_ms: 0.0,
            auth_duration_ms: 0.0,
            cells_compared: 0,
            divergences: Vec::new(),
            timed_out: false,
            error: Some(format!("{err:#}")),
        }],
        phases_passed: 0,
        phases_failed: 1,
        total_divergences: 0,
        expected_divergence: None,
        skipped: false,
    }
}

fn run_scenario_parity_inner(
    scenario: &dyn Scenario,
    scale: ScenarioScale,
    label: &str,
    fixture_dir: &Path,
    options: ParityOptions,
    expected_note: Option<String>,
) -> Result<ParityScenarioReport> {
    set_invariant_scale(scale);
    std::fs::create_dir_all(fixture_dir)?;
    let ctx = ScenarioBuildCtx {
        scale,
        fixture_dir: fixture_dir.to_path_buf(),
        label: label.to_string(),
    };
    let expected_path = fixture_path(&ctx, scenario.id());
    let fixture = scenario
        .build_fixture(&ctx)
        .with_context(|| format!("build fixture for {}", scenario.id()))?;
    if fixture.path != expected_path {
        bail!(
            "fixture path mismatch for {}: got {}, expected {}",
            scenario.id(),
            fixture.path.display(),
            expected_path.display()
        );
    }

    let (mut off, off_load_ms) = open_workbook(&fixture.path, Mode::Off, options.enable_parallel)?;
    let (mut auth, auth_load_ms) =
        open_workbook(&fixture.path, Mode::Auth, options.enable_parallel)?;

    let mut phases = Vec::new();
    phases.push(compare_phase(
        "AfterLoad".to_string(),
        &off,
        &auth,
        off_load_ms,
        auth_load_ms,
        options,
        None,
    ));

    let (off_ms, off_err) = eval_phase(&mut off);
    let (auth_ms, auth_err) = eval_phase(&mut auth);
    let err = merge_errors(off_err, auth_err);
    phases.push(compare_phase(
        "AfterFirstEval".to_string(),
        &off,
        &auth,
        off_ms,
        auth_ms,
        options,
        err,
    ));
    if phases.last().is_some_and(|phase| phase.error.is_some()) {
        return Ok(finish_report(
            scenario,
            scale,
            fixture.path,
            phases,
            expected_note,
        ));
    }

    if let Some(plan) = scenario.edit_plan() {
        for cycle in 0..plan.cycles {
            let off_start = Instant::now();
            let off_kind = (plan.apply)(&mut off, cycle)
                .with_context(|| format!("apply off edit {cycle} for {}", scenario.id()))?;
            let off_ms = off_start.elapsed().as_secs_f64() * 1000.0;
            let auth_start = Instant::now();
            let auth_kind = (plan.apply)(&mut auth, cycle)
                .with_context(|| format!("apply auth edit {cycle} for {}", scenario.id()))?;
            let auth_ms = auth_start.elapsed().as_secs_f64() * 1000.0;
            if off_kind != auth_kind {
                bail!(
                    "edit kind mismatch for {} cycle {cycle}: off={off_kind} auth={auth_kind}",
                    scenario.id()
                );
            }
            let edit_phase_name = phase_name(ScenarioPhase::AfterEdit {
                cycle,
                kind: off_kind,
            });
            phases.push(compare_phase(
                edit_phase_name,
                &off,
                &auth,
                off_ms,
                auth_ms,
                options,
                None,
            ));

            let (off_ms, off_err) = eval_phase(&mut off);
            let (auth_ms, auth_err) = eval_phase(&mut auth);
            let err = merge_errors(off_err, auth_err);
            let recalc_phase_name = phase_name(ScenarioPhase::AfterRecalc {
                cycle,
                kind: off_kind,
            });
            phases.push(compare_phase(
                recalc_phase_name,
                &off,
                &auth,
                off_ms,
                auth_ms,
                options,
                err,
            ));
            if phases.last().is_some_and(|phase| phase.error.is_some()) {
                break;
            }
        }
    }

    Ok(finish_report(
        scenario,
        scale,
        fixture.path,
        phases,
        expected_note,
    ))
}

fn finish_report(
    scenario: &dyn Scenario,
    scale: ScenarioScale,
    fixture_path: PathBuf,
    phases: Vec<ParityPhaseReport>,
    expected_note: Option<String>,
) -> ParityScenarioReport {
    let phases_failed = phases
        .iter()
        .filter(|phase| phase.error.is_some() || !phase.divergences.is_empty() || phase.timed_out)
        .count();
    let phases_passed = phases.len().saturating_sub(phases_failed);
    let total_divergences = phases.iter().map(|phase| phase.divergences.len()).sum();
    ParityScenarioReport {
        scenario_id: scenario.id().to_string(),
        scale: scale_title(scale).to_string(),
        fixture_path: fixture_path.display().to_string(),
        phases,
        phases_passed,
        phases_failed,
        total_divergences,
        expected_divergence: expected_note,
        skipped: false,
    }
}

fn open_workbook(path: &Path, mode: Mode, enable_parallel: bool) -> Result<(Workbook, f64)> {
    let start = Instant::now();
    let mut config = WorkbookConfig::ephemeral();
    config.eval = EvalConfig::default().with_formula_plane_mode(mode.eval_mode());
    config.eval.enable_parallel = enable_parallel;
    let backend =
        UmyaAdapter::open_path(path).with_context(|| format!("open fixture {}", path.display()))?;
    let workbook = Workbook::from_reader(backend, LoadStrategy::EagerAll, config)
        .with_context(|| format!("load fixture {}", path.display()))?;
    Ok((workbook, start.elapsed().as_secs_f64() * 1000.0))
}

fn eval_phase(workbook: &mut Workbook) -> (f64, Option<String>) {
    let start = Instant::now();
    let err = workbook.evaluate_all().err().map(|err| format!("{err:#}"));
    (start.elapsed().as_secs_f64() * 1000.0, err)
}

fn merge_errors(off: Option<String>, auth: Option<String>) -> Option<String> {
    match (off, auth) {
        (None, None) => None,
        (Some(off), None) => Some(format!("off evaluate_all error: {off}")),
        (None, Some(auth)) => Some(format!("auth evaluate_all error: {auth}")),
        (Some(off), Some(auth)) => Some(format!(
            "off evaluate_all error: {off}; auth evaluate_all error: {auth}"
        )),
    }
}

fn compare_phase(
    phase: String,
    off: &Workbook,
    auth: &Workbook,
    off_duration_ms: f64,
    auth_duration_ms: f64,
    options: ParityOptions,
    error: Option<String>,
) -> ParityPhaseReport {
    let timed_out = options.phase_timeout_ms > 0
        && (off_duration_ms > options.phase_timeout_ms as f64
            || auth_duration_ms > options.phase_timeout_ms as f64);
    let (cells_compared, divergences) =
        compare_workbooks(off, auth, options.max_divergences_per_phase);
    ParityPhaseReport {
        phase,
        off_duration_ms,
        auth_duration_ms,
        cells_compared,
        divergences,
        timed_out,
        error,
    }
}

pub fn compare_workbooks(
    off: &Workbook,
    auth: &Workbook,
    max_divergences: usize,
) -> (u64, Vec<CellDivergence>) {
    let mut sheet_names = BTreeSet::new();
    sheet_names.extend(off.sheet_names());
    sheet_names.extend(auth.sheet_names());
    let mut cells_compared = 0;
    let mut divergences = Vec::new();
    for sheet in sheet_names {
        let off_dims = off.sheet_dimensions(&sheet).unwrap_or((0, 0));
        let auth_dims = auth.sheet_dimensions(&sheet).unwrap_or((0, 0));
        let rows = off_dims.0.max(auth_dims.0);
        let cols = off_dims.1.max(auth_dims.1);
        for row in 1..=rows {
            for col in 1..=cols {
                cells_compared += 1;
                let off_value = normalize_empty(off.get_value(&sheet, row, col));
                let auth_value = normalize_empty(auth.get_value(&sheet, row, col));
                if !literal_parity_equal(&off_value, &auth_value)
                    && divergences.len() < max_divergences
                {
                    divergences.push(CellDivergence {
                        sheet: sheet.clone(),
                        row,
                        col,
                        off_value: format_value(&off_value),
                        auth_value: format_value(&auth_value),
                    });
                }
            }
        }
    }
    (cells_compared, divergences)
}

fn normalize_empty(value: Option<LiteralValue>) -> Option<LiteralValue> {
    match value {
        None | Some(LiteralValue::Empty) => None,
        other => other,
    }
}

pub fn literal_parity_equal(off: &Option<LiteralValue>, auth: &Option<LiteralValue>) -> bool {
    match (off, auth) {
        (None, None) => true,
        (Some(LiteralValue::Number(a)), Some(LiteralValue::Number(b))) => {
            float_parity_equal(*a, *b)
        }
        (Some(LiteralValue::Array(a)), Some(LiteralValue::Array(b))) => array_parity_equal(a, b),
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

fn array_parity_equal(a: &[Vec<LiteralValue>], b: &[Vec<LiteralValue>]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(ra, rb)| {
        ra.len() == rb.len()
            && ra.iter().zip(rb.iter()).all(|(va, vb)| {
                literal_parity_equal(
                    &normalize_empty(Some(va.clone())),
                    &normalize_empty(Some(vb.clone())),
                )
            })
    })
}

pub fn float_parity_equal(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        true
    } else {
        a.to_bits() == b.to_bits()
    }
}

fn format_value(value: &Option<LiteralValue>) -> String {
    match value {
        Some(value) => format!("{value:?}"),
        None => "None".to_string(),
    }
}

fn phase_name(phase: ScenarioPhase) -> String {
    match phase {
        ScenarioPhase::AfterLoad => "AfterLoad".to_string(),
        ScenarioPhase::AfterFirstEval => "AfterFirstEval".to_string(),
        ScenarioPhase::AfterEdit { cycle, kind } => {
            format!("AfterEdit{{cycle={cycle},kind=\"{kind}\"}}")
        }
        ScenarioPhase::AfterRecalc { cycle, kind } => {
            format!("AfterRecalc{{cycle={cycle},kind=\"{kind}\"}}")
        }
    }
}

pub fn report_is_unexpected_failure(report: &ParityScenarioReport) -> bool {
    !report.skipped
        && report.expected_divergence.is_none()
        && (report.phases_failed > 0 || report.total_divergences > 0)
}
