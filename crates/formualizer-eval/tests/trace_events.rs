#![cfg(feature = "tracing")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use formualizer_common::LiteralValue;
use formualizer_eval::engine::{
    CancelToken, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use tracing::field::{Field, Visit};
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

// Tracing's global callsite interest cache can retain `never` when the same
// callsite is first exercised without a subscriber while another test is
// recording. Serialize these tests so future coverage cannot recreate that
// cross-test registration race.
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug)]
struct Recorded {
    name: String,
    level: tracing::Level,
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<Recorded>>>,
    spans: Arc<Mutex<Vec<String>>>,
}

#[derive(Default)]
struct FieldVisitor(BTreeMap<String, String>);

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for Recorder
where
    S: Subscriber,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        let metadata = attrs.metadata();
        if metadata.target().starts_with("fz.") {
            self.spans.lock().unwrap().push(metadata.name().to_owned());
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        if !metadata.target().starts_with("fz.") {
            return;
        }
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.events.lock().unwrap().push(Recorded {
            name: metadata.name().to_owned(),
            level: *metadata.level(),
            fields: visitor.0,
        });
    }
}

fn engine() -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
            .with_parallel(false),
    )
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn run_coupled() {
    let mut engine = engine();
    let mut formulas = Vec::with_capacity(400);
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let b = if row == 1 {
            "=A1".to_owned()
        } else {
            format!("=C{}+A{row}", row - 1)
        };
        formulas.push(record(&mut engine, row, 2, &b));
        formulas.push(record(&mut engine, row, 3, &format!("=B{row}/2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
}

fn run_independent() {
    let mut engine = engine();
    let mut formulas = Vec::with_capacity(200);
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}*2+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
}

fn captured(run: impl FnOnce()) -> Recorder {
    let recorder = Recorder::default();
    let subscriber = Registry::default().with(recorder.clone());
    tracing::subscriber::with_default(subscriber, run);
    recorder
}

fn assert_no_info_per_cell_fields(events: &[Recorded]) {
    let always_forbidden = ["cell", "value", "formula"];
    for event in events
        .iter()
        .filter(|event| event.level == tracing::Level::INFO)
    {
        for field in event.fields.keys() {
            assert!(
                !always_forbidden.contains(&field.as_str()),
                "INFO event {} has per-cell field {field}",
                event.name
            );
            if matches!(field.as_str(), "row" | "col") {
                assert_eq!(
                    event.name, "fz.overlay.punchout",
                    "only fz.overlay.punchout may carry INFO row/col fields"
                );
            }
        }
    }
}

/// Coupled families that form cycles: one summary, bounded INFO volume, no
/// per-cell INFO fields. (The FormulaPlane span placement and demotion
/// events are span-internal and are checked separately below.)
#[test]
fn coupled_boundaries_are_aggregate() {
    let _guard = TEST_LOCK.lock().unwrap();
    let recorder = captured(run_coupled);
    let events = recorder.events.lock().unwrap();
    let spans = recorder.spans.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.name == "fz.evaluate.summary")
            .count(),
        1
    );

    let info_count = events
        .iter()
        .filter(|event| event.level == tracing::Level::INFO)
        .count();
    assert!(info_count <= spans.len().saturating_mul(10));
    assert_no_info_per_cell_fields(&events);
}

/// An independent family never demotes (there are no spans to demote).
#[test]
fn independent_family_is_not_demoted() {
    let _guard = TEST_LOCK.lock().unwrap();
    let recorder = captured(run_independent);
    let events = recorder.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.name == "fz.span.demoted")
            .count(),
        0
    );
}

fn assert_one_summary(recorder: &Recorder, cancelled: bool) {
    let events = recorder.events.lock().unwrap();
    let summaries: Vec<_> = events
        .iter()
        .filter(|event| event.name == "fz.evaluate.summary")
        .collect();
    assert_eq!(summaries.len(), 1, "one summary per public request");
    assert_eq!(
        summaries[0].fields.get("cancelled").map(String::as_str),
        Some(if cancelled { "true" } else { "false" })
    );
}

#[test]
fn evaluation_requests_emit_one_summary_on_success_error_and_cancellation() {
    let _guard = TEST_LOCK.lock().unwrap();

    let success = captured(|| {
        engine().evaluate_all().unwrap();
    });
    assert_one_summary(&success, false);

    let error = captured(|| {
        let result = engine().evaluate_cell("Sheet1", 0, 1);
        assert!(result.is_err());
    });
    assert_one_summary(&error, false);

    let cancelled = captured(|| {
        let token = CancelToken::new();
        token.cancel();
        let result = engine().evaluate_all_cancellable(token);
        assert!(matches!(
            result,
            Err(error) if error.kind == formualizer_common::ExcelErrorKind::Cancelled
        ));
    });
    assert_one_summary(&cancelled, true);
}
