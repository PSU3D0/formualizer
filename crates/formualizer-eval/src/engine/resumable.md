Cooperative recalculation with installed work preserved between calls.

A job follows `begin_recalc → step_recalc* → Finished/Cancelled`. A returned
step error also ends it. `recalc_with` drives these calls and invokes a closure
between steps; breaking pauses an active job for later manual stepping.

Soft deadlines stop at unit boundaries. A family run or cycle is never split.
Only committed **and flushed** values become clean. Cancellation does not install
the unit that observes it. Graph build and scheduling each have their own step
boundary and remain indivisible. Phases describe the work just performed, not
a promise that the next call will fit the same budget.

Jobs currently evaluate single-threaded, regardless of the ordinary parallel
evaluation setting. Move the owning engine to a worker thread for native UI
responsiveness; do not share mutable engine access across threads.

Priority changes order only: the last request wins, dirty precedents run first,
and every touched family run stays whole. Above 65,536 formulas of expansion,
priority falls back to the full schedule without dropping work.

Other evaluation APIs are refused while an explicit job is active. Their
signatures still return ExcelError; match `ExcelErrorExtra::RecalcJobActive`
(the kind stays Value). Job methods return RecalcJobError instead.
`evaluate_all_until` offers an implicit continuation with fewer reports:
a foreign evaluation safely ends that implicit continuation.

Work limits and hard deadlines accumulate over the job's active step time;
idle time between calls is excluded. Scratch is per step. Retained schedules
count toward memory unless shared with the engine cache. Hard failures retain
normal abort semantics; unfinished work stays dirty and a new job may begin.

Reads between steps mix installed new values and old pending values. Use
viewport pending queries alongside the job phase; they report raw dirtiness,
and completion may re-dirty volatile or iterative formulas for the next request.
Changed regions are conservative; delta overflow returns whole sheet identities.

Supported edits fold into the job. Iterative cycles created by edits seed from
current values, so they may differ from a fresh calculation. Unversioned
providers are assumed stable; bump revisions or dirty affected cells on changes.
Raw store and load-time bulk-finish mutations are unsupported during a job.
Live-spill user writes retain the known FORM-000319 limitation. Builtin polling
(FORM-000321), graph slicing and parallel jobs (FORM-000323) are separate work.

Rust function panics are caught at dispatch and become `#VALUE!` with
`ExcelErrorExtra::FunctionPanicked { function, message }`. String payloads are
retained; other payloads have no message. Builtins, direct registrations,
Workbook callbacks, reference paths and parallel calls share this policy.
`IFERROR` can replace the cell error, but the executed root still records
`UnavailableOperation::FunctionPanicked` when operation diagnostics are enabled.
Source recalculation enables that evidence: coverage is Partial and
`requireComplete` refuses publication. Ordinary returned Excel errors keep
their existing completeness behavior. A panicking aggregate kernel discards
its buffer and fails every member of that run; no partial values are installed.

An engine-internal unwind through a request or job step still propagates.
The engine then permanently refuses work: job APIs return
`RecalcJobError::Poisoned`, and existing fallible mutation/evaluation APIs return
a Value error with `ExcelErrorExtra::EnginePoisoned` (editor APIs preserve it in
`EditorError::Excel`). Check `Engine::is_poisoned` or `Workbook::is_poisoned`.
Infallible mutators panic before mutation with
"formualizer engine is poisoned by an earlier internal panic; discard it".
`cancel_recalc` returns None without touching state. Reads and queries remain
available but may expose stale or partially installed values. Discard the
engine; no recovery is attempted. Raw mutable storage and direct public config
fields cannot be revoked, and must never be used to resume a poisoned engine.

On `wasm32-unknown-unknown`, Rust panics trap instead of unwinding:
`catch_unwind` and unwind guards cannot recover or mark an instance after a
trap. Discard that workbook/worker or instance. Python exceptions and JS throws
already return ordinary function errors; Rust-side internal panics retain
Python's workbook-lock poison behavior. Progress callbacks run between steps;
their panics retain the already-reached lifecycle state.

## Progress callback

```rust
use formualizer_eval::engine::{Engine, EvalConfig, EvaluationStop, RecalcOutcome};
use std::{ops::ControlFlow, time::Duration};
# use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
# use formualizer_eval::traits::*;
# use formualizer_parse::parser::{parse, TableReference};
# use std::sync::Arc;
# struct EmptyResolver;
# impl ReferenceResolver for EmptyResolver {
# fn resolve_cell_reference(&self, _: Option<&str>, _: u32, _: u32) -> Result<LiteralValue, ExcelError> { Ok(LiteralValue::Empty) }
# }
# impl RangeResolver for EmptyResolver {
# fn resolve_range_reference(&self, _: Option<&str>, _: Option<u32>, _: Option<u32>, _: Option<u32>, _: Option<u32>) -> Result<Box<dyn Range>, ExcelError> { Err(ExcelErrorKind::Ref.into()) }
# }
# impl NamedRangeResolver for EmptyResolver {
# fn resolve_named_range_reference(&self, _: &str) -> Result<Vec<Vec<LiteralValue>>, ExcelError> { Err(ExcelErrorKind::Name.into()) }
# }
# impl TableResolver for EmptyResolver {
# fn resolve_table_reference(&self, _: &TableReference) -> Result<Box<dyn Table>, ExcelError> { Err(ExcelErrorKind::Ref.into()) }
# }
# impl Resolver for EmptyResolver {}
# impl FunctionProvider for EmptyResolver {
# fn get_function(&self, _: &str, _: &str) -> Option<Arc<dyn Function>> { None }
# }
# impl SourceResolver for EmptyResolver {}
# impl EvaluationContext for EmptyResolver {}
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let mut engine = Engine::new(EmptyResolver, EvalConfig::default());
# engine.set_cell_formula("Sheet1", 1, 1, parse("=1")?)?;
# engine.set_cell_formula("Sheet1", 2, 1, parse("=A1+1")?)?;
let outcome = engine.recalc_with(
    Default::default(), EvaluationStop::after(Duration::from_millis(5)),
    |_, report| {
        let p = &report.progress;
        let filled = (p.completed_in_pass.saturating_mul(20)
            / p.scheduled_in_pass.max(1)).min(20);
        println!("[{}{}] pass {}: {} / {} (exact: {})",
            "#".repeat(filled), "-".repeat(20 - filled),
            p.pass, p.completed_in_pass, p.scheduled_in_pass, p.total_exact);
        ControlFlow::Continue(())
    },
)?;
assert!(matches!(outcome, RecalcOutcome::Finished(_)));
# Ok(()) }
```

## Viewport priority and pending values

```rust
use formualizer_eval::engine::{Engine, EvalConfig, EvaluationStop, EvaluationTarget};
# use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
# use formualizer_eval::traits::*;
# use formualizer_parse::parser::{parse, TableReference};
# use std::sync::Arc;
# struct EmptyResolver;
# impl ReferenceResolver for EmptyResolver {
# fn resolve_cell_reference(&self, _: Option<&str>, _: u32, _: u32) -> Result<LiteralValue, ExcelError> { Ok(LiteralValue::Empty) }
# }
# impl RangeResolver for EmptyResolver {
# fn resolve_range_reference(&self, _: Option<&str>, _: Option<u32>, _: Option<u32>, _: Option<u32>, _: Option<u32>) -> Result<Box<dyn Range>, ExcelError> { Err(ExcelErrorKind::Ref.into()) }
# }
# impl NamedRangeResolver for EmptyResolver {
# fn resolve_named_range_reference(&self, _: &str) -> Result<Vec<Vec<LiteralValue>>, ExcelError> { Err(ExcelErrorKind::Name.into()) }
# }
# impl TableResolver for EmptyResolver {
# fn resolve_table_reference(&self, _: &TableReference) -> Result<Box<dyn Table>, ExcelError> { Err(ExcelErrorKind::Ref.into()) }
# }
# impl Resolver for EmptyResolver {}
# impl FunctionProvider for EmptyResolver {
# fn get_function(&self, _: &str, _: &str) -> Option<Arc<dyn Function>> { None }
# }
# impl SourceResolver for EmptyResolver {}
# impl EvaluationContext for EmptyResolver {}
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let mut engine = Engine::new(EmptyResolver, EvalConfig::default());
# engine.set_cell_formula("Sheet1", 1, 1, parse("=1")?)?;
# engine.set_cell_formula("Sheet1", 2, 1, parse("=A1+1")?)?;
engine.begin_recalc(Default::default())?;
engine.prioritize_recalc(&[EvaluationTarget::Cell {
    sheet: "Sheet1".into(), row: 2, col: 1, // targets are one-based
}])?;
let sheet = engine.sheet_id("Sheet1").unwrap();
while engine.has_active_recalc_job() {
    engine.step_recalc(EvaluationStop::after(std::time::Duration::ZERO))?;
    if !engine.has_pending_in_rect(sheet, 0, 0, 1, 0) {
        // The zero-based inclusive viewport is ready to paint.
        println!("{:?}", engine.get_cell_value("Sheet1", 2, 1));
    }
}
# Ok(()) }
```

## Worker thread, channel summaries and cross-thread cancellation

The handshake makes this example deterministic. Real hosts can cancel at any
time; a long atomic unit can delay observation until its installation boundary.

```rust
use formualizer_eval::engine::{Engine, EvalConfig, EvaluationStop, CancelToken, RecalcOutcome};
use std::{ops::ControlFlow, sync::mpsc, thread};
# use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
# use formualizer_eval::traits::*;
# use formualizer_parse::parser::{parse, TableReference};
# use std::sync::Arc;
# struct EmptyResolver;
# impl ReferenceResolver for EmptyResolver {
# fn resolve_cell_reference(&self, _: Option<&str>, _: u32, _: u32) -> Result<LiteralValue, ExcelError> { Ok(LiteralValue::Empty) }
# }
# impl RangeResolver for EmptyResolver {
# fn resolve_range_reference(&self, _: Option<&str>, _: Option<u32>, _: Option<u32>, _: Option<u32>, _: Option<u32>) -> Result<Box<dyn Range>, ExcelError> { Err(ExcelErrorKind::Ref.into()) }
# }
# impl NamedRangeResolver for EmptyResolver {
# fn resolve_named_range_reference(&self, _: &str) -> Result<Vec<Vec<LiteralValue>>, ExcelError> { Err(ExcelErrorKind::Name.into()) }
# }
# impl TableResolver for EmptyResolver {
# fn resolve_table_reference(&self, _: &TableReference) -> Result<Box<dyn Table>, ExcelError> { Err(ExcelErrorKind::Ref.into()) }
# }
# impl Resolver for EmptyResolver {}
# impl FunctionProvider for EmptyResolver {
# fn get_function(&self, _: &str, _: &str) -> Option<Arc<dyn Function>> { None }
# }
# impl SourceResolver for EmptyResolver {}
# impl EvaluationContext for EmptyResolver {}
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let mut engine = Engine::new(EmptyResolver, EvalConfig::default());
# engine.set_cell_formula("Sheet1", 1, 1, parse("=1")?)?;
# engine.set_cell_formula("Sheet1", 2, 1, parse("=A1+1")?)?;
let token = CancelToken::new();
let worker_token = token.clone();
let (summary_tx, summary_rx) = mpsc::channel();
let (ready_tx, ready_rx) = mpsc::channel();
let (go_tx, go_rx) = mpsc::channel();
let worker = thread::spawn(move || {
    ready_tx.send(()).unwrap();
    go_rx.recv().unwrap();
    engine.recalc_with(Default::default(), EvaluationStop::cancel(worker_token),
        |_, report| {
            summary_tx.send((report.phase, report.step_computed_vertices)).unwrap();
            ControlFlow::Continue(())
        })
});
ready_rx.recv()?;
token.cancel(); // cancellation comes from the owning thread
go_tx.send(())?;
let outcome = worker.join().unwrap()?;
assert!(matches!(outcome, RecalcOutcome::Cancelled));
for (phase, computed) in summary_rx { println!("{phase:?}: {computed}"); }
# Ok(()) }
```
