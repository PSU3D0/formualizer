//! Scenario execution, structural assertions, and trace recording.
use crate::{
    materialize::{Artifact, Materialize as _, WorkbookApi, WorkbookRoute},
    scenario::{
        DemotionExpect, Expect, FailureFingerprint, Position, Provenance, ScenarioSize,
        ScenarioSource, ScenarioSpec, Step, StructureExpect,
    },
    shape::{Role, Shape},
};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{EngineBaselineStats, FormulaPlaneMode};
use formualizer_workbook::{Workbook, WorkbookConfig};
#[cfg(feature = "xlsx")]
use std::path::PathBuf;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tracing::{
    Event, Id, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    Layer, Registry,
    layer::{Context, SubscriberExt},
};

/// Tracing caches callsite interest globally. Scenario runs (including
/// unrecorded ones) are serialized so an unsubscribed execution cannot cache
/// an `fz.*` callsite as never interested while another run is collecting it.
static RECORDED_RUN_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedEvent {
    pub name: String,
    pub fields: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepBucket {
    pub events: Vec<RecordedEvent>,
    pub spans: Vec<String>,
}

#[derive(Clone, Default)]
pub struct Recorder {
    step: Arc<AtomicUsize>,
    buckets: Arc<Mutex<BTreeMap<usize, StepBucket>>>,
}
impl Recorder {
    pub fn bucket(&self, step: usize) -> StepBucket {
        self.buckets
            .lock()
            .unwrap()
            .get(&step)
            .cloned()
            .unwrap_or_default()
    }
    pub fn buckets(&self) -> BTreeMap<usize, StepBucket> {
        self.buckets.lock().unwrap().clone()
    }
    pub fn clear(&self) {
        self.buckets.lock().unwrap().clear();
    }
    fn select(&self, step: usize) {
        self.step.store(step, Ordering::SeqCst);
    }
}
#[derive(Default)]
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}
impl<S: Subscriber> Layer<S> for Recorder {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        if attrs.metadata().target().starts_with("fz.") {
            self.buckets
                .lock()
                .unwrap()
                .entry(self.step.load(Ordering::SeqCst))
                .or_default()
                .spans
                .push(attrs.metadata().name().to_owned());
        }
    }
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if !event.metadata().target().starts_with("fz.") {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.buckets
            .lock()
            .unwrap()
            .entry(self.step.load(Ordering::SeqCst))
            .or_default()
            .events
            .push(RecordedEvent {
                name: event.metadata().name().to_owned(),
                fields: fields.0,
            });
    }
}

#[derive(Clone)]
pub enum Materializer {
    WorkbookApi {
        route: WorkbookRoute,
        config: WorkbookConfig,
    },
    #[cfg(feature = "xlsx")]
    Xlsx {
        path: PathBuf,
        reader: XlsxReader,
        config: WorkbookConfig,
    },
}
/// Which workbook reader loads an xlsx artifact. The two readers take
/// different formula ingest routes and are therefore distinct provenances.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XlsxReader {
    Calamine,
    Umya,
}
impl Materializer {
    pub fn workbook_api(route: WorkbookRoute, config: WorkbookConfig) -> Self {
        Self::WorkbookApi { route, config }
    }
    #[cfg(feature = "xlsx")]
    pub fn xlsx(path: impl Into<PathBuf>, reader: XlsxReader, config: WorkbookConfig) -> Self {
        Self::Xlsx {
            path: path.into(),
            reader,
            config,
        }
    }
    fn config(&self) -> &WorkbookConfig {
        match self {
            Self::WorkbookApi { config, .. } => config,
            #[cfg(feature = "xlsx")]
            Self::Xlsx { config, .. } => config,
        }
    }
    pub fn provenance(&self) -> Provenance {
        match self {
            Self::WorkbookApi { .. } => Provenance::WorkbookApi,
            #[cfg(feature = "xlsx")]
            Self::Xlsx { reader, .. } => match reader {
                XlsxReader::Calamine => Provenance::XlsxCalamine,
                XlsxReader::Umya => Provenance::XlsxUmya,
            },
        }
    }
    fn mode(&self) -> FormulaPlaneMode {
        self.config().eval.formula_plane_mode
    }
    fn with_mode(&self, mode: FormulaPlaneMode) -> Self {
        let mut next = self.clone();
        match &mut next {
            Self::WorkbookApi { config, .. } => config.eval.formula_plane_mode = mode,
            #[cfg(feature = "xlsx")]
            Self::Xlsx { config, .. } => config.eval.formula_plane_mode = mode,
        };
        next
    }
}

#[derive(Clone, Debug)]
pub struct StepReport {
    pub index: usize,
    pub step: Step,
    pub stats: Option<EngineBaselineStats>,
}
#[derive(Clone, Debug)]
pub struct RunReport {
    pub steps: Vec<StepReport>,
    pub failure: Option<String>,
    /// A tracked failure surfaced by this run; it is intentionally visible but non-fatal.
    pub known_failure: Option<String>,
}
impl RunReport {
    pub fn is_ok(&self) -> bool {
        self.failure.is_none()
    }
    pub fn into_result(self) -> Result<Self, String> {
        match self.failure.clone() {
            Some(error) => Err(error),
            None => Ok(self),
        }
    }
}

/// True when the engine accepts and ignores the FormulaPlane mode (the
/// `unified_authority` build, M2): no span is ever placed, so an
/// `AuthoritativeExperimental` run is an `Off` run. Detected from the engine
/// itself, which stores `Off` for a requested authoritative mode then.
pub fn formula_plane_mode_ignored() -> bool {
    static IGNORED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *IGNORED.get_or_init(|| {
        let config = WorkbookConfig::interactive()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
        Workbook::new_with_config(config)
            .engine()
            .config
            .formula_plane_mode
            == FormulaPlaneMode::Off
    })
}

pub fn run(
    spec: &ScenarioSpec,
    mode: FormulaPlaneMode,
    size: ScenarioSize,
    materializer: Materializer,
    recorder: Option<&Recorder>,
) -> RunReport {
    if !spec.modes.contains(&mode) {
        return failed(format!(
            "scenario {} does not support mode {mode:?}",
            spec.id
        ));
    }
    if recorder.is_some() && materializer.config().eval.enable_parallel {
        return failed("Recorder requires config.eval.enable_parallel == false".into());
    }
    let mut shape = spec.shape.clone();
    shape.scale.rows = size.rows;
    if let Err(error) = validate_roles(&shape, &spec.script.0) {
        return failed(format!("scenario definition error: {error}"));
    }
    let _recording_guard = RECORDED_RUN_LOCK.lock().unwrap();
    if let Some(recorder) = recorder {
        recorder.clear();
    }
    let execute = || execute(spec, shape, size, mode, materializer, recorder);
    if let Some(recorder) = recorder {
        let subscriber = Registry::default().with(recorder.clone());
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        execute()
    } else {
        execute()
    }
}
fn failed(error: String) -> RunReport {
    RunReport {
        steps: vec![],
        failure: Some(error),
        known_failure: None,
    }
}

fn execute(
    spec: &ScenarioSpec,
    shape: Shape,
    size: ScenarioSize,
    mode: FormulaPlaneMode,
    materializer: Materializer,
    recorder: Option<&Recorder>,
) -> RunReport {
    let needs_parity = spec
        .expects
        .iter()
        .any(|(_, expect)| matches!(expect, Expect::Parity));
    let parity = if needs_parity {
        let other = if mode == FormulaPlaneMode::Off {
            FormulaPlaneMode::AuthoritativeExperimental
        } else {
            FormulaPlaneMode::Off
        };
        match execute_steps(
            &shape,
            &spec.script.0,
            Execution {
                source: spec.source.as_ref(),
                size,
                materializer: materializer.with_mode(other),
                recorder: None,
                expects: None,
                parity: None,
            },
        ) {
            Ok(result) => Some(result.snapshots),
            Err(error) => return failed(format!("parity run failed: {error}")),
        }
    } else {
        None
    };
    // With the mode ignored an authoritative run is an Off run: it carries
    // Off's defect markers, and the span-placement defects pinned for the
    // authoritative mode cannot occur.
    let marker_mode = if formula_plane_mode_ignored() {
        FormulaPlaneMode::Off
    } else {
        mode
    };
    let expected_failure = spec
        .expected_failures
        .iter()
        .find(|expected| expected.matches(marker_mode, materializer.provenance()))
        .cloned();
    let executed = match execute_steps(
        &shape,
        &spec.script.0,
        Execution {
            source: spec.source.as_ref(),
            size,
            materializer: materializer.with_mode(mode),
            recorder,
            expects: Some(&spec.expects),
            parity: parity.as_ref(),
        },
    ) {
        Ok(value) => value,
        Err(error) => {
            let Some(expected) = expected_failure else {
                return failed(error.to_string());
            };
            // Only the fingerprinted failure is known; an earlier or different
            // failure is a regression the marker must not hide.
            if !matches!(&error, RunError::Step(actual) if *actual == expected.failure) {
                return failed(format!(
                    "expected failure did not match ({:?}): expected {}, got {error} [{}]",
                    expected.provenance, expected.failure, expected.reason
                ));
            }
            let known = format!("{}: {error}", expected.reason);
            eprintln!("KNOWN {} {mode:?}: {known}", spec.id);
            return RunReport {
                steps: vec![],
                failure: None,
                known_failure: Some(known),
            };
        }
    };
    if let Some(expected) = expected_failure {
        return failed(format!(
            "expected failure did not occur ({:?}): expected {} [{}]",
            expected.provenance, expected.failure, expected.reason
        ));
    }
    RunReport {
        steps: executed.reports,
        failure: None,
        known_failure: None,
    }
}

/// Why a script execution stopped. Only a step failure can match an expected
/// failure fingerprint.
enum RunError {
    Step(FailureFingerprint),
    Script(String),
}
impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Step(failure) => write!(f, "step {}: {}", failure.step, failure.message),
            Self::Script(message) => f.write_str(message),
        }
    }
}

type CellSnapshot = (String, u32, u32, Option<LiteralValue>);
type StepSnapshot = Vec<CellSnapshot>;
type RunSnapshots = Vec<StepSnapshot>;

struct Executed {
    snapshots: RunSnapshots,
    reports: Vec<StepReport>,
}
struct Execution<'a> {
    source: Option<&'a ScenarioSource>,
    size: ScenarioSize,
    materializer: Materializer,
    recorder: Option<&'a Recorder>,
    expects: Option<&'a [(usize, Expect)]>,
    parity: Option<&'a RunSnapshots>,
}
fn execute_steps(
    shape: &Shape,
    steps: &[Step],
    context: Execution<'_>,
) -> Result<Executed, RunError> {
    let Execution {
        source,
        size,
        materializer,
        recorder,
        expects,
        parity,
    } = context;
    let mut workbook = None;
    let mut snapshots = Vec::with_capacity(steps.len());
    let mut reports = Vec::with_capacity(steps.len());
    for (index, step) in steps.iter().enumerate() {
        if let Some(recorder) = recorder {
            recorder.select(index);
        }
        apply_step(step, shape, source, size, &materializer, &mut workbook)
            .map_err(|message| RunError::Step(FailureFingerprint::action(index, message)))?;
        let snapshot = if let Some(wb) = workbook.as_ref() {
            shape
                .render()
                .map_err(|e| RunError::Script(e.to_string()))?
                .into_iter()
                .map(|cell| {
                    let value = wb.get_value(&cell.sheet, cell.row, cell.col);
                    (cell.sheet, cell.row, cell.col, value)
                })
                .collect()
        } else {
            vec![]
        };
        let stats = workbook.as_ref().map(|wb| wb.engine().baseline_stats());
        if let Some(expects) = expects {
            let wb = workbook.as_ref().ok_or_else(|| {
                RunError::Script(format!(
                    "step {index}: expectation requires a loaded workbook"
                ))
            })?;
            for (_, expect) in expects
                .iter()
                .filter(|(expected_step, _)| *expected_step == index)
            {
                check_expect_now(
                    expect,
                    StepContext {
                        step: index,
                        mode: materializer.mode(),
                        provenance: materializer.provenance(),
                        workbook: wb,
                        snapshot: &snapshot,
                        stats: stats.as_ref().unwrap(),
                        parity,
                        recorder,
                    },
                )
                .map_err(|message| {
                    RunError::Step(FailureFingerprint::expectation(index, message))
                })?;
            }
        }
        snapshots.push(snapshot);
        reports.push(StepReport {
            index,
            step: step.clone(),
            stats,
        });
    }
    if let Some((step, _)) =
        expects.and_then(|items| items.iter().find(|(step, _)| *step >= steps.len()))
    {
        return Err(RunError::Script(format!(
            "step {step}: expectation index is outside script"
        )));
    }
    workbook.ok_or_else(|| RunError::Script("script never loaded a workbook".to_owned()))?;
    Ok(Executed { snapshots, reports })
}
fn apply_step(
    step: &Step,
    shape: &Shape,
    source: Option<&ScenarioSource>,
    size: ScenarioSize,
    materializer: &Materializer,
    workbook: &mut Option<Workbook>,
) -> Result<(), String> {
    if matches!(step, Step::Load) {
        if workbook.is_some() {
            return Err("Load may only occur once".into());
        }
        let artifact = match source {
            Some(ScenarioSource::Shape(source_shape)) => match materializer {
                Materializer::WorkbookApi { route, config } => WorkbookApi::new(config.clone())
                    .route(*route)
                    .materialize(source_shape),
                #[cfg(feature = "xlsx")]
                Materializer::Xlsx { path, .. } => {
                    crate::materialize::Xlsx::new(path).materialize(source_shape)
                }
            },
            Some(ScenarioSource::XlsxFixture(factory)) => {
                let path = match materializer {
                    #[cfg(feature = "xlsx")]
                    Materializer::Xlsx { path, .. } => path.clone(),
                    Materializer::WorkbookApi { .. } => std::env::temp_dir().join(format!(
                        "formualizer-scenario-{}-{}.xlsx",
                        std::process::id(),
                        size.rows
                    )),
                };
                factory(&path, size)
                    .map(crate::materialize::Artifact::Xlsx)
                    .map_err(crate::materialize::MaterializeError::Backend)
            }
            None => match materializer {
                Materializer::WorkbookApi { route, config } => WorkbookApi::new(config.clone())
                    .route(*route)
                    .materialize(shape),
                #[cfg(feature = "xlsx")]
                Materializer::Xlsx { path, .. } => {
                    crate::materialize::Xlsx::new(path).materialize(shape)
                }
            },
        }
        .map_err(|e| e.to_string())?;
        *workbook = Some(match artifact {
            Artifact::Workbook(wb) => wb,
            Artifact::Xlsx(path) => {
                use formualizer_workbook::{
                    CalamineAdapter, LoadStrategy, SpreadsheetReader, UmyaAdapter,
                };
                let config = materializer.config().clone();
                let reader = match materializer {
                    #[cfg(feature = "xlsx")]
                    Materializer::Xlsx { reader, .. } => *reader,
                    Materializer::WorkbookApi { .. } => XlsxReader::Calamine,
                };
                match reader {
                    XlsxReader::Calamine => {
                        let backend =
                            CalamineAdapter::open_path(path).map_err(|e| e.to_string())?;
                        Workbook::from_reader(backend, LoadStrategy::EagerAll, config)
                            .map_err(|e| e.to_string())?
                    }
                    XlsxReader::Umya => {
                        let backend = UmyaAdapter::open_path(path).map_err(|e| e.to_string())?;
                        Workbook::from_reader(backend, LoadStrategy::EagerAll, config)
                            .map_err(|e| e.to_string())?
                    }
                }
            }
        });
        return Ok(());
    }
    let wb = workbook
        .as_mut()
        .ok_or_else(|| format!("{step:?} requires an earlier Load step"))?;
    let interior = || role_cell(shape, Role::PrimaryFamily, true);
    match step {
        Step::Load => unreachable!(),
        Step::Prepare => wb.prepare_graph_all().map_err(|e| e.to_string()),
        Step::EvaluateAll => wb.evaluate_all().map(|_| ()).map_err(|e| e.to_string()),
        Step::NoOp => Ok(()),
        Step::SetValue(role, value) => {
            let (s, r, c) = role_cell(shape, *role, false)?;
            wb.set_value(&s, r, c, LiteralValue::Number(*value))
                .map_err(|e| e.to_string())
        }
        Step::SetFormula(role, formula) => {
            let (s, r, c) = role_cell(shape, *role, false)?;
            wb.set_formula(&s, r, c, formula).map_err(|e| e.to_string())
        }
        Step::OverrideInterior => {
            let (s, r, c) = interior()?;
            wb.set_value(&s, r, c, LiteralValue::Number(7.0))
                .map_err(|e| e.to_string())
        }
        Step::FormulaToLiteral => {
            let (s, r, c) = interior()?;
            let value = wb
                .get_value(&s, r, c)
                .ok_or_else(|| format!("no value at {s}!R{r}C{c}"))?;
            wb.set_value(&s, r, c, value).map_err(|e| e.to_string())
        }
        Step::Undo => wb.undo().map_err(|e| e.to_string()),
        Step::Redo => wb.redo().map_err(|e| e.to_string()),
        Step::InsertRows { at, count } => {
            let (s, r, _) = interior()?;
            wb.engine_mut()
                .insert_rows(&s, resolve(*at, r), *count)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        Step::DeleteRows { at, count } => {
            let (s, r, _) = interior()?;
            wb.engine_mut()
                .delete_rows(&s, resolve(*at, r), *count)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        Step::InsertCols { at, count } => {
            let (s, _, c) = interior()?;
            wb.engine_mut()
                .insert_columns(&s, resolve(*at, c), *count)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        Step::DeleteCols { at, count } => {
            let (s, _, c) = interior()?;
            wb.engine_mut()
                .delete_columns(&s, resolve(*at, c), *count)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        Step::CancelAfter { checkpoints } => Err(format!(
            "Unsupported: CancelAfter {{ checkpoints: {checkpoints} }}; Workbook exposes no checkpoint-count cancellation hook"
        )),
        Step::Custom(action) => action(wb),
    }
}
fn resolve(position: Position, middle: u32) -> u32 {
    match position {
        Position::Absolute(value) => value,
        Position::Middle => middle,
    }
}
fn role_cell(shape: &Shape, role: Role, middle: bool) -> Result<(String, u32, u32), String> {
    let cells = shape.resolve_role(role).map_err(|e| e.to_string())?;
    Ok(cells[if middle { cells.len() / 2 } else { 0 }].clone())
}
fn validate_roles(shape: &Shape, steps: &[Step]) -> Result<(), String> {
    for step in steps {
        match step {
            Step::SetValue(role, _) | Step::SetFormula(role, _) => {
                shape.resolve_role(*role).map_err(|e| e.to_string())?;
            }
            Step::OverrideInterior
            | Step::FormulaToLiteral
            | Step::InsertRows { .. }
            | Step::DeleteRows { .. }
            | Step::InsertCols { .. }
            | Step::DeleteCols { .. } => {
                shape
                    .resolve_role(Role::PrimaryFamily)
                    .map_err(|e| e.to_string())?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Everything an expectation may consult after a step has been applied.
struct StepContext<'a> {
    step: usize,
    mode: FormulaPlaneMode,
    provenance: Provenance,
    workbook: &'a Workbook,
    snapshot: &'a StepSnapshot,
    stats: &'a EngineBaselineStats,
    parity: Option<&'a RunSnapshots>,
    recorder: Option<&'a Recorder>,
}
fn check_expect_now(expect: &Expect, context: StepContext<'_>) -> Result<(), String> {
    let StepContext {
        step,
        mode,
        provenance,
        workbook,
        snapshot,
        stats,
        parity,
        recorder,
    } = context;
    match expect {
        Expect::Value {
            sheet,
            row,
            col,
            value,
        } => {
            let actual = workbook.get_value(sheet, *row, *col);
            if actual.as_ref() == Some(value)
                || (matches!(value, LiteralValue::Empty) && actual.is_none())
            {
                Ok(())
            } else {
                Err(format!(
                    "value: expected {value:?}, got {actual:?} at {sheet}!R{row}C{col}"
                ))
            }
        }
        Expect::Oracle(oracle) => oracle(workbook),
        Expect::NoErrors(sheet) => {
            for (_, row, col, value) in snapshot.iter().filter(|(name, ..)| name == sheet) {
                if let Some(LiteralValue::Error(error)) = value {
                    return Err(format!("error at {sheet}!R{row}C{col}: {error}"));
                }
            }
            Ok(())
        }
        Expect::Parity => {
            let other = parity.ok_or_else(|| "internal parity snapshot missing".to_owned())?;
            if *snapshot == other[step] {
                Ok(())
            } else {
                Err(format!(
                    "parity: Off and AuthoritativeExperimental values differ: {snapshot:?} != {:?}",
                    other[step]
                ))
            }
        }
        Expect::Structure(expected) => {
            if expected.mode.is_some_and(|wanted| wanted != mode)
                || expected
                    .provenance
                    .is_some_and(|wanted| wanted != provenance)
            {
                return Ok(());
            }
            // Event-derived fields count cumulatively through this step: placement
            // happens at Prepare while demotion happens at first evaluation.
            let cumulative = recorder.map(|r| {
                let mut merged = StepBucket::default();
                for (_, bucket) in r.buckets().into_iter().filter(|(s, _)| *s <= step) {
                    merged.events.extend(bucket.events);
                    merged.spans.extend(bucket.spans);
                }
                merged
            });
            check_structure(expected, stats, cumulative.as_ref())
        }
    }
}
fn check_structure(
    expected: &StructureExpect,
    stats: &EngineBaselineStats,
    bucket: Option<&StepBucket>,
) -> Result<(), String> {
    fn field<T: PartialEq + std::fmt::Debug>(
        name: &str,
        expected: Option<&T>,
        actual: T,
    ) -> Result<(), String> {
        if let Some(expected) = expected
            && *expected != actual
        {
            return Err(format!(
                "field {name}: expected {expected:?}, got {actual:?}"
            ));
        }
        Ok(())
    }
    // Span-placement fields are internal representation; with the mode
    // ignored (unified_authority) no span is placed, so they are not checked.
    let spans = !formula_plane_mode_ignored();
    if spans {
        field(
            "active_spans",
            expected.active_spans.as_ref(),
            stats.formula_plane_active_span_count,
        )?;
    }
    field(
        "arena_nodes",
        expected.arena_nodes.as_ref(),
        stats.formula_ast_node_count,
    )?;
    field(
        "graph_vertices",
        expected.graph_vertices.as_ref(),
        stats.graph_formula_vertex_count,
    )?;
    field(
        "graph_edges",
        expected.graph_edges.as_ref(),
        stats.graph_edge_count,
    )?;
    if spans
        && (expected.demotions.is_some()
            || expected.topology_outcome.is_some()
            || expected.placed_families.is_some()
            || expected.rejected_families.is_some())
    {
        let bucket =
            bucket.ok_or_else(|| "structure event expectation requires a Recorder".to_owned())?;
        field(
            "placed_families",
            expected.placed_families.as_ref(),
            bucket
                .events
                .iter()
                .filter(|e| e.name == "fz.family.placed")
                .count(),
        )?;
        field(
            "rejected_families",
            expected.rejected_families.as_ref(),
            bucket
                .events
                .iter()
                .filter(|e| e.name == "fz.family.rejected")
                .count(),
        )?;
        if let Some(DemotionExpect { count, reason }) = &expected.demotions {
            let matching: Vec<_> = bucket
                .events
                .iter()
                .filter(|e| e.name == "fz.span.demoted")
                .collect();
            field("demotions", Some(count), matching.len())?;
            if let Some(reason) = reason
                && matching.iter().any(|event| {
                    event.fields.get("reason").map(|v| v.trim_matches('"')) != Some(reason.as_str())
                })
            {
                return Err(format!(
                    "field demotions.reason: expected {reason:?}, got {:?}",
                    matching
                        .iter()
                        .map(|e| e.fields.get("reason"))
                        .collect::<Vec<_>>()
                ));
            }
        }
        if let Some(outcome) = &expected.topology_outcome {
            let actual = bucket
                .events
                .iter()
                .find(|e| e.name == "fz.topology.compiled")
                .and_then(|e| e.fields.get("outcome"))
                .map(|v| v.trim_matches('"').to_owned());
            if actual.as_ref() != Some(outcome) {
                return Err(format!(
                    "field topology_outcome: expected {outcome:?}, got {actual:?}"
                ));
            }
        }
    }
    Ok(())
}
