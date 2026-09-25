//! Program 1 M0 scenario pins.
//!
//! Each pin is a committed scenario that later milestones must keep passing:
//!
//! - `pin.form117.*`: structural action undo (FORM-000117). The values right
//!   after undo are correct today; the later-precedent recalculation is the
//!   tracked defect, pinned with its correct expectation as a known failure
//!   until M3.
//! - `pin.sheet-delete-readd`: cross-sheet readers across sheet delete and
//!   re-add (the tombstone re-add path).
//! - `pin.preparation-policy` / `pin.preparation-policy-strict`: the
//!   preparation outcome of each route under the default BestEffort policy and
//!   under explicit Strict (docs/preparation-error-policy.md).
//! - `pin.span.*`: the FormulaPlane span-ownership regressions of the M3 perf
//!   line, ported as value-level scenarios. Where the base has the defect M3
//!   fixed, the authoritative mode is a known failure that names it.
//!
//! Expected values are computed from first principles; none is copied from an
//! observed engine run except where a pin says it records legacy behavior.
use crate::run::XlsxReader;
use crate::scenario::{
    EnginePath, Expect, ExpectedFailureSpec, FailureFingerprint, LifecycleOp, Provenance, Purpose,
    ScenarioSize, ScenarioSpec, Script, SizeClass, Step, Tags,
};
use crate::shape::{Cell, Extent, Range, Scale, Shape, r#gen};
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::graph::editor::undo_engine::UndoEngine;
use formualizer_eval::engine::{EditorError, FormulaPlaneMode};
use formualizer_parse::parser::parse;
use formualizer_workbook::workbook::WBResolver;
use formualizer_workbook::{Workbook, WorkbookConfig};
use std::sync::{Arc, Mutex};

const SHEET: &str = "S";

/// Script builder that tracks step indexes for expectations.
#[derive(Default)]
struct Pin {
    steps: Vec<Step>,
    expects: Vec<(usize, Expect)>,
}

impl Pin {
    fn loaded() -> Self {
        let mut pin = Self::default();
        pin.steps
            .extend([Step::Load, Step::Prepare, Step::EvaluateAll]);
        pin
    }
    fn custom(
        &mut self,
        action: impl Fn(&mut Workbook) -> Result<(), String> + Send + Sync + 'static,
    ) -> &mut Self {
        self.steps.push(Step::Custom(Arc::new(action)));
        self
    }
    /// Evaluate, then attach expectations to that evaluation step.
    fn eval(&mut self, expects: impl IntoIterator<Item = Expect>) -> &mut Self {
        self.steps.push(Step::EvaluateAll);
        let index = self.steps.len() - 1;
        self.expects
            .extend(expects.into_iter().map(|expect| (index, expect)));
        self
    }
    /// Expectations on the initial evaluation (step 2).
    fn initially(&mut self, expects: impl IntoIterator<Item = Expect>) -> &mut Self {
        self.expects
            .extend(expects.into_iter().map(|expect| (2, expect)));
        self
    }
    fn spec(
        self,
        id: &str,
        description: &str,
        shape: Shape,
        rows: u32,
        lifecycle: Vec<LifecycleOp>,
    ) -> ScenarioSpec {
        ScenarioSpec {
            id: format!("pin.{id}"),
            description: description.into(),
            shape,
            source: None,
            script: Script(self.steps),
            expects: self.expects,
            tags: Tags {
                lifecycle,
                engine: vec![EnginePath::Legacy, EnginePath::FormulaPlane],
                purpose: vec![Purpose::Behavioral, Purpose::EdgeCase],
                size: vec![SizeClass::Small],
                ..Tags::default()
            },
            modes: vec![
                FormulaPlaneMode::Off,
                FormulaPlaneMode::AuthoritativeExperimental,
            ],
            sizes: vec![ScenarioSize::new(SizeClass::Small, rows)],
            expected_failures: vec![],
        }
    }
}

/// Marks a tracked defect. `scopes` are `(mode, provenance)` pairs; a `None`
/// provenance covers every provenance. Each scope must fail with exactly
/// `failure` (the observed step and mismatch); markers accumulate.
fn known(
    mut spec: ScenarioSpec,
    scopes: &[(FormulaPlaneMode, Option<Provenance>)],
    failure: FailureFingerprint,
    reason: &str,
) -> ScenarioSpec {
    spec.expected_failures
        .extend(scopes.iter().map(|(mode, provenance)| ExpectedFailureSpec {
            mode: *mode,
            provenance: *provenance,
            failure: failure.clone(),
            reason: reason.into(),
        }));
    spec
}

/// The provenances whose authoritative runs place spans. The Calamine reader
/// bypasses placement (FORM-000128), so span defects do not show there.
const SPAN_PLACING: [(FormulaPlaneMode, Option<Provenance>); 2] = [
    (
        FormulaPlaneMode::AuthoritativeExperimental,
        Some(Provenance::WorkbookApi),
    ),
    (
        FormulaPlaneMode::AuthoritativeExperimental,
        Some(Provenance::XlsxUmya),
    ),
];

/// Every run of the column `col` over rows `1..=last` must equal `model`
/// (`None` = empty). Models are first-principles, not recorded runs.
fn column(
    col: u32,
    last: u32,
    model: impl Fn(u32) -> Option<f64> + Send + Sync + 'static,
) -> Expect {
    Expect::Oracle(Arc::new(move |wb: &Workbook| {
        let mut diffs = Vec::new();
        for row in 1..=last {
            let actual = wb.get_value(SHEET, row, col);
            let ok = match (model(row), &actual) {
                (None, None) | (None, Some(LiteralValue::Empty)) => true,
                (Some(want), Some(LiteralValue::Number(got))) => *got == want,
                (Some(want), Some(LiteralValue::Int(got))) => *got as f64 == want,
                _ => false,
            };
            if !ok {
                diffs.push(format!(
                    "R{row}C{col}: expected {:?}, got {actual:?}",
                    model(row)
                ));
            }
        }
        if diffs.is_empty() {
            Ok(())
        } else {
            let shown = diffs.len().min(8);
            Err(format!(
                "{} cells differ: {}",
                diffs.len(),
                diffs[..shown].join("; ")
            ))
        }
    }))
}

/// Column B of `single_family(200)` with the three live overrides at
/// `base` (value 999), `base+1` (`=A{base+1}*5` over `a(base+1)`) and
/// `base+2` (empty), where `a` is column A after the edit.
fn overrides_model(
    base: u32,
    a: impl Fn(u32) -> Option<f64> + Send + Sync + 'static,
) -> impl Fn(u32) -> Option<f64> + Send + Sync + 'static {
    move |row| {
        if row == base {
            Some(999.0)
        } else if row == base + 1 {
            a(row).map(|v| v * 5.0)
        } else if row == base + 2 {
            None
        } else {
            a(row).map(|v| v * 2.0)
        }
    }
}
/// Column A after inserting one row before `at` in rows `1..=200`.
fn a_after_insert(at: u32) -> impl Fn(u32) -> Option<f64> + Send + Sync + Copy + 'static {
    move |row| {
        if row < at {
            Some(f64::from(row))
        } else if row == at || row > 201 {
            None
        } else {
            Some(f64::from(row - 1))
        }
    }
}
fn a_original(row: u32) -> Option<f64> {
    (row <= 200).then_some(f64::from(row))
}

fn num(row: u32, col: u32, value: f64) -> Expect {
    at(SHEET, row, col, LiteralValue::Number(value))
}
fn empty(row: u32, col: u32) -> Expect {
    at(SHEET, row, col, LiteralValue::Empty)
}
fn at(sheet: &str, row: u32, col: u32, value: LiteralValue) -> Expect {
    Expect::Value {
        sheet: sheet.into(),
        row,
        col,
        value,
    }
}
fn error_at(sheet: &'static str, row: u32, col: u32, kind: ExcelErrorKind) -> Expect {
    Expect::Oracle(Arc::new(move |wb: &Workbook| {
        match wb.get_value(sheet, row, col) {
            Some(LiteralValue::Error(error)) if error.kind == kind => Ok(()),
            other => Err(format!(
                "{sheet}!R{row}C{col}: expected {kind:?}, got {other:?}"
            )),
        }
    }))
}

fn io(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn set_value(
    row: u32,
    col: u32,
    value: LiteralValue,
) -> impl Fn(&mut Workbook) -> Result<(), String> {
    move |wb| wb.set_value(SHEET, row, col, value.clone()).map_err(io)
}
fn set_formula(
    row: u32,
    col: u32,
    formula: &'static str,
) -> impl Fn(&mut Workbook) -> Result<(), String> {
    move |wb| wb.set_formula(SHEET, row, col, formula).map_err(io)
}

/// `A[r] = r` and `B[r] = A[r]*2` over `rows` rows (one span in Auth mode).
fn single_family(rows: u32) -> Shape {
    Shape::new().scale(Scale::rows(rows)).sheet(SHEET, |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "b",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=A{r}*2",
        );
    })
}

/// `single_family` plus `C[r] = B[r]+1` (two spans in Auth mode).
fn two_families(rows: u32) -> Shape {
    Shape::new().scale(Scale::rows(rows)).sheet(SHEET, |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "b",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=A{r}*2",
        );
        s.family(
            "c",
            Range::col("C", Extent::Abs(1)..=Extent::Rows),
            "=B{r}+1",
        );
    })
}

type Tx<'a> = formualizer_eval::engine::EngineAction<'a, WBResolver>;

/// An engine-level undo stack shared by the steps of one run. Each run resets
/// it at its journaled step, and runs are serialized by the runner.
#[derive(Clone, Default)]
struct Journal(Arc<Mutex<UndoEngine>>);
impl Journal {
    fn record(
        &self,
        name: &'static str,
        action: impl Fn(&mut Tx<'_>) -> Result<(), EditorError> + Send + Sync + 'static,
    ) -> impl Fn(&mut Workbook) -> Result<(), String> + Send + Sync + 'static {
        let journal = self.clone();
        move |wb| {
            let (_, entry) = wb
                .engine_mut()
                .action_atomic_journal(name, |tx| action(tx))
                .map_err(io)?;
            let mut undo = UndoEngine::new();
            undo.push_action(entry);
            *journal.0.lock().unwrap() = undo;
            Ok(())
        }
    }
    fn undo(&self) -> impl Fn(&mut Workbook) -> Result<(), String> + Send + Sync + 'static {
        let journal = self.clone();
        move |wb| {
            wb.engine_mut()
                .undo_action(&mut journal.0.lock().unwrap())
                .map_err(io)
        }
    }
    fn redo(&self) -> impl Fn(&mut Workbook) -> Result<(), String> + Send + Sync + 'static {
        let journal = self.clone();
        move |wb| {
            wb.engine_mut()
                .redo_action(&mut journal.0.lock().unwrap())
                .map_err(io)
        }
    }
}

/// B100 = 999, B101 = `=A101*5`, B102 = empty: the three override kinds.
fn live_overrides(pin: &mut Pin) {
    pin.custom(set_value(100, 2, LiteralValue::Number(999.0)))
        .custom(set_formula(101, 2, "=A101*5"))
        .custom(set_value(102, 2, LiteralValue::Empty));
}

const STRUCTURAL: [LifecycleOp; 4] = [
    LifecycleOp::Load,
    LifecycleOp::Evaluate,
    LifecycleOp::Edit,
    LifecycleOp::Structural,
];
const HISTORY: [LifecycleOp; 4] = [
    LifecycleOp::Load,
    LifecycleOp::Evaluate,
    LifecycleOp::Edit,
    LifecycleOp::History,
];
const ALL: [LifecycleOp; 5] = [
    LifecycleOp::Load,
    LifecycleOp::Evaluate,
    LifecycleOp::Edit,
    LifecycleOp::Structural,
    LifecycleOp::History,
];

/// FORM-000117: journal a row insert at 50 over live overrides, then undo.
fn form117(followup: bool) -> ScenarioSpec {
    let journal = Journal::default();
    let mut pin = Pin::loaded();
    live_overrides(&mut pin);
    pin.eval([num(100, 2, 999.0), num(101, 2, 505.0), empty(102, 2)]);
    pin.custom(journal.record("insert row", |tx: &mut Tx<'_>| {
        tx.insert_rows(SHEET, 50, 1).map(|_| ())
    }))
    .eval([num(101, 2, 999.0), num(102, 2, 505.0), empty(103, 2)])
    .custom(journal.undo())
    .eval([num(100, 2, 999.0), num(101, 2, 505.0), empty(102, 2)]);
    if !followup {
        return structural_known(
            pin.spec(
                "form117.structural-undo-values",
                "journaled row insert over live overrides, undone: restored values",
                single_family(200),
                200,
                ALL.to_vec(),
            ),
            form117_undo_republished(),
        );
    }
    pin.custom(set_value(101, 1, LiteralValue::Number(7.0)))
        .eval([num(101, 2, 35.0)]);
    // Span-placing authoritative runs fail earlier, at the undo itself (the
    // structural defect of `structural_known`); every other run reaches the
    // FORM-000117 recalculation at step 12.
    let spec = structural_known(
        pin.spec(
            "form117.later-precedent-recalc",
            "after structural undo a restored formula must follow a later precedent edit",
            single_family(200),
            200,
            ALL.to_vec(),
        ),
        form117_undo_republished(),
    );
    // Under the authority the undo's structural resync dirties the restored
    // formula's closure, so FORM-000117 is fixed there (Program 1 M5).
    if crate::run::formula_plane_mode_ignored() {
        return spec;
    }
    known(
        spec,
        &[
            (FormulaPlaneMode::Off, None),
            (
                FormulaPlaneMode::AuthoritativeExperimental,
                Some(Provenance::XlsxCalamine),
            ),
        ],
        FailureFingerprint::expectation(
            12,
            "value: expected Number(35.0), got Some(Number(505.0)) at S!R101C2",
        ),
        "FORM-000117: after undoing a structural action the restored formula keeps its stale value (505, expected 35) in both modes; absorbed by M3",
    )
}

/// The FORM-000117 journaled insert/undo scripts in span-placing runs: after
/// the undo (step 8) the formula override at row 102 shows the family result.
fn form117_undo_republished() -> FailureFingerprint {
    FailureFingerprint::expectation(
        8,
        "value: expected Number(505.0), got Some(Number(202.0)) at S!R102C2",
    )
}

/// Cross-sheet readers across sheet delete and re-add (the tombstone path).
fn sheet_delete_readd() -> ScenarioSpec {
    let rows = 200;
    let shape = Shape::new()
        .scale(Scale::rows(rows))
        .sheet(SHEET, |s| {
            s.family(
                "guarded",
                Range::col("A", Extent::Abs(1)..=Extent::Rows),
                "=IFERROR(Aux!A{r}*2,-1)",
            );
            s.family(
                "plain",
                Range::col("B", Extent::Abs(1)..=Extent::Rows),
                "=Aux!A{r}*2",
            );
        })
        .sheet("Aux", |s| {
            s.values(
                Range::col("A", Extent::Abs(1)..=Extent::Rows),
                r#gen::index_f64(),
            );
        });
    let mut pin = Pin::loaded();
    pin.initially([num(50, 1, 100.0), num(50, 2, 100.0)])
        .custom(|wb| wb.delete_sheet("Aux").map_err(io))
        .eval([
            num(50, 1, -1.0),
            error_at(SHEET, 50, 2, ExcelErrorKind::Ref),
        ])
        .custom(move |wb| {
            wb.add_sheet("Aux").map_err(io)?;
            for row in 1..=rows {
                wb.set_value("Aux", row, 1, LiteralValue::Number(f64::from(row + 10)))
                    .map_err(io)?;
            }
            Ok(())
        })
        .eval([num(50, 1, 120.0), num(50, 2, 120.0), num(200, 2, 420.0)]);
    pin.spec(
        "sheet-delete-readd",
        "cross-sheet readers over a deleted then re-added sheet",
        shape,
        rows,
        STRUCTURAL.to_vec(),
    )
}

fn span_pins() -> Vec<ScenarioSpec> {
    let mut specs = Vec::new();

    // A formula written inside a span is the cell's sole producer and follows
    // its own precedents.
    let mut pin = Pin::loaded();
    pin.custom(set_formula(100, 2, "=A100*5"))
        .eval([num(100, 2, 500.0), num(99, 2, 198.0)])
        .custom(set_value(100, 1, LiteralValue::Number(7.0)))
        .eval([num(100, 2, 35.0), num(101, 2, 202.0)]);
    specs.push(known(
        pin.spec(
            "span.formula-override-follows-precedent",
            "formula written inside a span follows its precedent",
            single_family(200),
            200,
            STRUCTURAL[..3].to_vec(),
        ),
        &SPAN_PLACING,
        FailureFingerprint::expectation(
            6,
            "value: expected Number(35.0), got Some(Number(14.0)) at S!R100C2",
        ),
        "base: after its precedent changes, a formula written inside a promoted span is overwritten by the family result (14 = A100*2, expected 35 = A100*5); fixed on the M3 perf line (3ec4d547), not on this base",
    ));

    // A value written inside a span stays when its old precedent changes.
    let mut pin = Pin::loaded();
    pin.custom(set_value(100, 2, LiteralValue::Number(999.0)))
        .eval([num(100, 2, 999.0)])
        .custom(set_value(100, 1, LiteralValue::Number(7.0)))
        .eval([num(100, 2, 999.0), num(99, 2, 198.0)]);
    specs.push(pin.spec(
        "span.value-override-sticks",
        "value written inside a span is not overwritten by the family",
        single_family(200),
        200,
        STRUCTURAL[..3].to_vec(),
    ));

    // An explicit empty value inside a span is a tombstone.
    let mut pin = Pin::loaded();
    pin.custom(set_value(100, 2, LiteralValue::Empty))
        .eval([empty(100, 2)])
        .custom(set_value(100, 1, LiteralValue::Number(7.0)))
        .eval([empty(100, 2), num(101, 2, 202.0)]);
    specs.push(pin.spec(
        "span.empty-override-is-tombstone",
        "empty value written inside a span stays empty",
        single_family(200),
        200,
        STRUCTURAL[..3].to_vec(),
    ));

    // Many point overrides inside one span.
    let mut pin = Pin::loaded();
    pin.custom(|wb| {
        for row in 1..=100 {
            wb.set_value(
                SHEET,
                row,
                2,
                LiteralValue::Number(10_000.0 + f64::from(row)),
            )
            .map_err(io)?;
        }
        Ok(())
    })
    .eval([
        num(1, 2, 10_001.0),
        num(100, 2, 10_100.0),
        num(101, 2, 202.0),
        num(200, 2, 400.0),
    ]);
    specs.push(pin.spec(
        "span.many-point-overrides",
        "one hundred value overrides inside a span",
        single_family(200),
        200,
        STRUCTURAL[..3].to_vec(),
    ));

    // Structural shifts of live overrides. Column models are first
    // principles (the M3 originals compared against Off mode).
    let insert_50 = a_after_insert(50);
    let mut pin = Pin::loaded();
    live_overrides(&mut pin);
    pin.eval([column(2, 201, overrides_model(100, a_original))])
        .custom(|wb| {
            wb.engine_mut()
                .insert_rows(SHEET, 50, 1)
                .map(|_| ())
                .map_err(io)
        })
        .eval([
            column(1, 201, insert_50),
            column(2, 201, overrides_model(101, insert_50)),
        ])
        .custom(set_value(102, 1, LiteralValue::Number(7.0)))
        .eval([num(101, 2, 999.0), num(102, 2, 35.0)]);
    specs.push(structural_known(
        pin.spec(
            "span.row-insert-with-live-overrides",
            "row insert above value, formula and empty overrides inside a span",
            single_family(200),
            200,
            STRUCTURAL.to_vec(),
        ),
        republished_override(8, "R102C2"),
    ));

    let insert_1 = a_after_insert(1);
    let mut pin = Pin::loaded();
    live_overrides(&mut pin);
    pin.eval([])
        .custom(|wb| {
            wb.engine_mut()
                .insert_rows(SHEET, 1, 1)
                .map(|_| ())
                .map_err(io)
        })
        .eval([
            column(1, 201, insert_1),
            column(2, 201, overrides_model(101, insert_1)),
        ]);
    specs.push(structural_known(
        pin.spec(
            "span.top-row-insert-shifts-overrides",
            "row insert at the top shifts live overrides",
            single_family(200),
            200,
            STRUCTURAL.to_vec(),
        ),
        republished_override(8, "R102C2"),
    ));

    // Deleting the value-override row: the formula override moves up to 100
    // and reads the shifted A100 (old A101).
    let a_after_delete = |row: u32| match row {
        1..=99 => Some(f64::from(row)),
        100..=199 => Some(f64::from(row + 1)),
        _ => None,
    };
    let mut pin = Pin::loaded();
    live_overrides(&mut pin);
    pin.eval([])
        .custom(|wb| {
            wb.engine_mut()
                .delete_rows(SHEET, 100, 1)
                .map(|_| ())
                .map_err(io)
        })
        .eval([
            column(1, 200, a_after_delete),
            column(2, 200, move |row| match row {
                100 => Some(505.0),
                101 => None,
                _ => a_after_delete(row).map(|v| v * 2.0),
            }),
        ]);
    specs.push(structural_known(
        pin.spec(
            "span.override-row-delete",
            "deleting a live-override row shifts the remaining overrides",
            single_family(200),
            200,
            STRUCTURAL.to_vec(),
        ),
        republished_override(8, "R100C2"),
    ));

    // Journaled row/column insert over live overrides, undo and redo.
    for columns in [false, true] {
        let journal = Journal::default();
        let original = overrides_model(100, a_original);
        let after_insert = move |pin: &mut Pin| {
            if columns {
                pin.eval([
                    column(2, 201, |_| None),
                    column(3, 201, overrides_model(100, a_original)),
                ]);
            } else {
                pin.eval([column(2, 201, overrides_model(101, insert_50))]);
            }
        };
        let mut pin = Pin::loaded();
        live_overrides(&mut pin);
        pin.eval([column(2, 201, original)]);
        pin.custom(
            journal.record("shift live overrides", move |tx: &mut Tx<'_>| {
                if columns {
                    tx.insert_columns(SHEET, 2, 1).map(|_| ())
                } else {
                    tx.insert_rows(SHEET, 50, 1).map(|_| ())
                }
            }),
        );
        after_insert(&mut pin);
        pin.custom(journal.undo()).eval([
            column(1, 201, a_original),
            column(2, 201, overrides_model(100, a_original)),
            column(3, 201, |_| None),
        ]);
        pin.custom(journal.redo());
        after_insert(&mut pin);
        specs.push(structural_known(
            pin.spec(
                if columns {
                    "span.undoable-column-insert-with-live-overrides"
                } else {
                    "span.undoable-row-insert-with-live-overrides"
                },
                "journaled insert over live overrides, undo and redo",
                single_family(200),
                200,
                ALL.to_vec(),
            ),
            republished_override(8, if columns { "R101C3" } else { "R102C2" }),
        ));
    }

    // Undo of an atomic value override restores the family formula.
    let journal = Journal::default();
    let mut pin = Pin::loaded();
    pin.custom(journal.record("value override", |tx: &mut Tx<'_>| {
        tx.set_cell_value(SHEET, 100, 2, LiteralValue::Number(777.0))
    }))
    .eval([num(100, 2, 777.0), num(100, 3, 778.0)])
    .custom(journal.undo())
    .eval([num(100, 2, 200.0), num(100, 3, 201.0)])
    .custom(set_value(100, 1, LiteralValue::Number(1000.0)))
    .eval([num(100, 2, 2000.0), num(100, 3, 2001.0)]);
    specs.push(pin.spec(
        "span.atomic-value-override-undo",
        "undo of an atomic value override restores the formula producer",
        two_families(200),
        200,
        HISTORY.to_vec(),
    ));

    // A formula override inside a producer span re-dirties its reader span;
    // undo restores the family formula, not a frozen value.
    for undo in [false, true] {
        let journal = Journal::default();
        let mut pin = Pin::loaded();
        pin.custom(journal.record("formula override", |tx: &mut Tx<'_>| {
            tx.set_cell_value(SHEET, 1, 1, LiteralValue::Number(1000.0))?;
            tx.set_cell_formula(SHEET, 100, 2, parse("=A100*5").unwrap())?;
            tx.set_cell_value(SHEET, 200, 1, LiteralValue::Number(2000.0))?;
            Ok(())
        }))
        .eval([
            num(1, 2, 2000.0),
            num(1, 3, 2001.0),
            num(100, 2, 500.0),
            num(100, 3, 501.0),
            num(200, 2, 4000.0),
            num(200, 3, 4001.0),
        ]);
        if undo {
            pin.custom(journal.undo()).eval([
                num(1, 1, 1.0),
                num(1, 2, 2.0),
                num(1, 3, 3.0),
                num(100, 2, 200.0),
                num(100, 3, 201.0),
                num(200, 2, 400.0),
                num(200, 3, 401.0),
            ]);
            pin.custom(set_value(100, 1, LiteralValue::Number(1000.0)))
                .eval([num(100, 2, 2000.0), num(100, 3, 2001.0)]);
        } else {
            pin.custom(set_value(100, 1, LiteralValue::Number(7.0)))
                .eval([num(100, 2, 35.0), num(100, 3, 36.0)]);
        }
        specs.push(pin.spec(
            if undo {
                "span.formula-override-undo-restores-producer"
            } else {
                "span.formula-override-redirties-reader"
            },
            "formula override in a producer span and its reader span",
            two_families(200),
            200,
            if undo {
                HISTORY.to_vec()
            } else {
                STRUCTURAL[..3].to_vec()
            },
        ));
    }

    // FORM-000115 (M2 qualification fixture): undo of a formula replacement
    // over a value override restores the value.
    let shape = Shape::new().scale(Scale::rows(128)).sheet(SHEET, |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "b",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=A{r}*2+1",
        );
    });
    let mut pin = Pin::loaded();
    pin.initially([num(10, 2, 21.0)])
        .custom(set_value(10, 1, LiteralValue::Number(20.0)))
        .eval([num(10, 2, 41.0)])
        .custom(set_value(10, 2, LiteralValue::Number(777.0)))
        .eval([num(10, 2, 777.0)])
        .custom(set_formula(10, 2, "=A10*3"))
        .eval([num(10, 2, 60.0)])
        .custom(|wb| wb.undo().map_err(io))
        .eval([num(10, 2, 777.0)])
        .custom(|wb| wb.redo().map_err(io))
        .eval([num(10, 2, 60.0)])
        .custom(set_value(10, 1, LiteralValue::Number(30.0)))
        .eval([num(10, 2, 90.0), num(11, 2, 23.0)]);
    specs.push(known(
        pin.spec(
            "span.override-replacement-undo",
            "undo of a formula replacing a value override restores the value",
            shape,
            128,
            HISTORY.to_vec(),
        ),
        &SPAN_PLACING,
        FailureFingerprint::expectation(
            10,
            "value: expected Number(777.0), got Some(Number(41.0)) at S!R10C2",
        ),
        "FORM-000115: in a promoted span, undoing a formula that replaced a value override restores the family result (41) instead of the value (777); fixed on the M3 perf line (3ec4d547), not on this base",
    ));

    // A fixed-range reader over a formula-source span follows a point edit
    // and its undo.
    let n = 256u32;
    let total = f64::from(n) * f64::from(n + 1) / 2.0;
    let shape = Shape::new().scale(Scale::rows(n)).sheet(SHEET, |s| {
        s.family(
            "source",
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            "={r}",
        );
        s.family(
            "reader",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=SUM($A$1:$A${n})",
        );
    });
    let mut pin = Pin::loaded();
    pin.initially([num(n, 2, total)])
        .custom(set_value(1, 1, LiteralValue::Number(10.0)))
        .eval([num(1, 2, total + 9.0), num(n, 2, total + 9.0)])
        .custom(|wb| wb.undo().map_err(io))
        .eval([num(1, 1, 1.0), num(n, 2, total)]);
    specs.push(pin.spec(
        "span.fixed-range-reader-over-source-edit",
        "fixed SUM reader over a formula-source family through a point edit and undo",
        shape,
        n,
        HISTORY.to_vec(),
    ));

    // A legacy reader of a span cell does not take over the cell's history.
    let shape = Shape::new().scale(Scale::rows(128)).sheet(SHEET, |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "b",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=A{r}*2",
        );
    });
    let mut pin = Pin::loaded();
    pin.custom(set_formula(1, 3, "=B5"))
        .eval([num(1, 3, 10.0)])
        .custom(set_value(5, 2, LiteralValue::Number(777.0)))
        .eval([num(1, 3, 777.0)])
        .custom(|wb| wb.undo().map_err(io))
        .eval([num(5, 2, 10.0), num(1, 3, 10.0)])
        .custom(set_value(5, 1, LiteralValue::Number(20.0)))
        .eval([num(5, 2, 40.0), num(1, 3, 40.0)]);
    specs.push(pin.spec(
        "span.reader-placeholder-keeps-span-history",
        "a legacy reader of a span cell does not take over its undo history",
        shape,
        128,
        HISTORY.to_vec(),
    ));

    specs
}

/// One preparation-failure case: a formula in `S!B1` next to `S!A1 = 1`.
struct PolicyCase {
    id: &'static str,
    formula: &'static str,
}

const POLICY_CASES: [PolicyCase; 9] = [
    PolicyCase {
        id: "missing-sheet",
        formula: "=NoSheet!A1",
    },
    PolicyCase {
        id: "missing-sheet-guarded",
        formula: "=IFERROR(NoSheet!A1,456)",
    },
    PolicyCase {
        id: "missing-table",
        formula: "=SUM(MissingTable[Amount])",
    },
    PolicyCase {
        id: "undefined-name",
        formula: "=MissingName",
    },
    PolicyCase {
        id: "undefined-name-guarded",
        formula: "=IFERROR(MissingName,456)",
    },
    PolicyCase {
        id: "unknown-function",
        formula: "=NOSUCHFUNCTION(1)",
    },
    PolicyCase {
        id: "external-cell",
        formula: "=[1]Other!A1",
    },
    PolicyCase {
        id: "external-range",
        formula: "=SUM([1]Other!A1:B2)",
    },
    PolicyCase {
        id: "external-column",
        formula: "=SUM([1]Other!$B:$B)",
    },
];

/// Preparation routes. `Engine*` routes write through the workbook's engine
/// with graph building eager or deferred; `Xlsx*` routes load a file.
#[derive(Clone, Copy, Debug)]
enum PolicyRoute {
    WorkbookSetFormula,
    EngineEager,
    EngineDeferred,
    XlsxCalamine,
    XlsxUmya,
}
const POLICY_ROUTES: [PolicyRoute; 5] = [
    PolicyRoute::WorkbookSetFormula,
    PolicyRoute::EngineEager,
    PolicyRoute::EngineDeferred,
    PolicyRoute::XlsxCalamine,
    PolicyRoute::XlsxUmya,
];

fn outcome_value(value: Option<LiteralValue>) -> String {
    match value {
        Some(LiteralValue::Error(error)) => format!("value {}", error.kind),
        Some(LiteralValue::Number(n)) => format!("value {n}"),
        Some(LiteralValue::Int(n)) => format!("value {n}"),
        other => format!("value {other:?}"),
    }
}
/// `"{stage} error {kind}"`, plus the wrapped kind when the error carries one
/// (the message text itself is not part of the policy).
fn outcome_error(stage: &str, error: impl std::fmt::Display) -> String {
    let text = error.to_string();
    let kind = text
        .find('#')
        .map(|start| {
            let rest = &text[start..];
            rest[..rest.find(':').unwrap_or(rest.len())].to_string()
        })
        .unwrap_or_else(|| "?".into());
    match text.find("kind: ") {
        Some(start) => {
            let rest = &text[start + 6..];
            let inner = &rest[..rest
                .find(|c: char| !c.is_alphanumeric())
                .unwrap_or(rest.len())];
            format!("{stage} error {kind} (wraps {inner})")
        }
        None => format!("{stage} error {kind}"),
    }
}

fn policy_config(mode: FormulaPlaneMode, deferred: bool, strict: bool) -> WorkbookConfig {
    let mut config = WorkbookConfig::interactive().with_formula_plane_mode(mode);
    config.eval.enable_parallel = false;
    config.eval.defer_graph_building = deferred;
    if strict {
        config.eval.preparation_policy = formualizer_eval::engine::PreparationPolicy::Strict;
    }
    config
}

/// Runs one case through one route and reports the outcome of assignment or
/// load, then of evaluation.
fn policy_outcome(
    route: PolicyRoute,
    case: &PolicyCase,
    mode: FormulaPlaneMode,
    strict: bool,
) -> String {
    let mut wb = match route {
        PolicyRoute::WorkbookSetFormula
        | PolicyRoute::EngineEager
        | PolicyRoute::EngineDeferred => {
            let deferred = !matches!(route, PolicyRoute::EngineEager);
            let mut wb = Workbook::new_with_config(policy_config(mode, deferred, strict));
            if let Err(error) = wb.add_sheet(SHEET) {
                return format!("setup error {error}");
            }
            if let Err(error) = wb.set_value(SHEET, 1, 1, LiteralValue::Number(1.0)) {
                return format!("setup error {error}");
            }
            let assigned = match route {
                PolicyRoute::WorkbookSetFormula => {
                    wb.set_formula(SHEET, 1, 2, case.formula).map_err(io)
                }
                _ => match parse(case.formula) {
                    Ok(ast) => wb
                        .engine_mut()
                        .set_cell_formula(SHEET, 1, 2, ast)
                        .map_err(io),
                    Err(error) => Err(error.to_string()),
                },
            };
            if let Err(error) = assigned {
                return outcome_error("set", error);
            }
            wb
        }
        PolicyRoute::XlsxCalamine | PolicyRoute::XlsxUmya => {
            let reader = if matches!(route, PolicyRoute::XlsxCalamine) {
                XlsxReader::Calamine
            } else {
                XlsxReader::Umya
            };
            let shape = Shape::new().scale(Scale::rows(1)).sheet(SHEET, |s| {
                s.values(Range::cells([(1, 1)]), r#gen::constant(Cell::number(1.0)));
                s.values(
                    Range::cells([(1, 2)]),
                    r#gen::constant(Cell::formula(case.formula)),
                );
            });
            let path = std::env::temp_dir().join(format!(
                "fz-pin-policy-{}-{}-{mode:?}-{reader:?}.xlsx",
                std::process::id(),
                case.id
            ));
            let artifact = crate::materialize::Materialize::materialize(
                &mut crate::materialize::Xlsx::new(&path),
                &shape,
            );
            let Ok(crate::materialize::Artifact::Xlsx(path)) = artifact else {
                return "setup error xlsx".into();
            };
            let config = policy_config(mode, true, strict);
            use formualizer_workbook::{
                CalamineAdapter, LoadStrategy, SpreadsheetReader, UmyaAdapter,
            };
            let loaded = match reader {
                XlsxReader::Calamine => {
                    CalamineAdapter::open_path(&path).map_err(io).and_then(|b| {
                        Workbook::from_reader(b, LoadStrategy::EagerAll, config).map_err(io)
                    })
                }
                XlsxReader::Umya => UmyaAdapter::open_path(&path).map_err(io).and_then(|b| {
                    Workbook::from_reader(b, LoadStrategy::EagerAll, config).map_err(io)
                }),
            };
            let _ = std::fs::remove_file(&path);
            match loaded {
                Ok(wb) => wb,
                Err(error) => return outcome_error("load", error),
            }
        }
    };
    match wb.evaluate_all() {
        Ok(_) => outcome_value(wb.get_value(SHEET, 1, 2)),
        Err(error) => outcome_error("eval", error),
    }
}

/// The preparation-failure table under the Strict policy (the default before decision 16), observed on the
/// Program 1 base (legacy behavior, decision 11). Rows are cases, columns
/// are routes in `POLICY_ROUTES` order. `Engine*` assignment validates
/// immediately whatever `defer_graph_building` says; the Workbook and xlsx
/// routes fail at the first evaluation. It changes only with a logged
/// semantics decision.
const POLICY_TABLE: [(&str, [&str; 5]); 9] = [
    (
        "missing-sheet",
        [
            "eval error #REF!",
            "set error #REF!",
            "set error #REF!",
            "eval error #REF!",
            "eval error #REF!",
        ],
    ),
    (
        "missing-sheet-guarded",
        [
            "eval error #REF!",
            "set error #REF!",
            "set error #REF!",
            "eval error #REF!",
            "eval error #REF!",
        ],
    ),
    (
        "missing-table",
        [
            "eval error #NAME?",
            "set error #NAME?",
            "set error #NAME?",
            "eval error #NAME?",
            "eval error #NAME?",
        ],
    ),
    (
        "undefined-name",
        [
            "value #NAME?",
            "value #NAME?",
            "value #NAME?",
            "value #NAME?",
            "value #NAME?",
        ],
    ),
    (
        "undefined-name-guarded",
        [
            "value 456",
            "value 456",
            "value 456",
            "value 456",
            "value 456",
        ],
    ),
    (
        "unknown-function",
        [
            "value #NAME?",
            "value #NAME?",
            "value #NAME?",
            "value #NAME?",
            "value #NAME?",
        ],
    ),
    (
        "external-cell",
        [
            "eval error #NAME?",
            "set error #NAME?",
            "set error #NAME?",
            "eval error #NAME?",
            "eval error #NAME?",
        ],
    ),
    (
        "external-range",
        [
            "eval error #NAME?",
            "set error #NAME?",
            "set error #NAME?",
            "eval error #NAME?",
            "eval error #NAME?",
        ],
    ),
    (
        "external-column",
        [
            "value #REF!",
            "value #REF!",
            "value #REF!",
            "value #REF!",
            "value #REF!",
        ],
    ),
];

/// In authoritative mode the Calamine route reports a preparation failure as
/// `#VALUE!` wrapping the original kind (observed; logged for the oracle).
fn expected_outcome(mode: FormulaPlaneMode, route: PolicyRoute, table: &str) -> String {
    if mode == FormulaPlaneMode::AuthoritativeExperimental
        && matches!(route, PolicyRoute::XlsxCalamine)
        && let Some(kind) = table.strip_prefix("eval error ")
    {
        let inner = match kind {
            "#REF!" => "Ref",
            "#NAME?" => "Name",
            other => other,
        };
        return format!("eval error #VALUE! (wraps {inner})");
    }
    table.to_string()
}

/// Renders the observed table (for the semantics log and for re-observation).
pub fn observe_preparation_policy(
    mode: FormulaPlaneMode,
    strict: bool,
) -> Vec<(&'static str, Vec<String>)> {
    POLICY_CASES
        .iter()
        .map(|case| {
            (
                case.id,
                POLICY_ROUTES
                    .iter()
                    .map(|route| policy_outcome(*route, case, mode, strict))
                    .collect(),
            )
        })
        .collect()
}

/// The same cases under the default BestEffort policy (decision 16,
/// 2026-09-25): an unbound sheet or table no longer fails assignment, load or
/// evaluation; the cell evaluates to the error Strict reported at preparation
/// (`#REF!` for a sheet, `#NAME?` for a table), which guards catch. External
/// references are unchanged.
fn best_effort_table() -> [(&'static str, [&'static str; 5]); 9] {
    let mut table = POLICY_TABLE;
    for (case, outcomes) in table.iter_mut() {
        let value = match *case {
            "missing-sheet" => "value #REF!",
            "missing-sheet-guarded" => "value 456",
            "missing-table" => "value #NAME?",
            _ => continue,
        };
        *outcomes = [value; 5];
    }
    table
}

fn preparation_policy(strict: bool) -> ScenarioSpec {
    let shape = Shape::new().scale(Scale::rows(1)).sheet(SHEET, |s| {
        s.values(Range::cells([(1, 1)]), r#gen::constant(Cell::number(1.0)));
    });
    let mut pin = Pin::default();
    pin.steps.push(Step::Load);
    pin.custom(move |wb| {
        let mode = wb.engine().config.formula_plane_mode;
        let observed = observe_preparation_policy(mode, strict);
        let table = if strict {
            POLICY_TABLE
        } else {
            best_effort_table()
        };
        let mut diffs = Vec::new();
        for ((case, outcomes), (expected_case, expected)) in observed.iter().zip(table) {
            assert_eq!(*case, expected_case);
            for ((route, got), want) in POLICY_ROUTES.iter().zip(outcomes).zip(expected) {
                let want = expected_outcome(mode, *route, want);
                if *got != want {
                    diffs.push(format!(
                        "{case} via {route:?}: expected {want:?}, got {got:?}"
                    ));
                }
            }
        }
        if diffs.is_empty() {
            Ok(())
        } else {
            Err(diffs.join("; "))
        }
    });
    let (id, description) = if strict {
        (
            "preparation-policy-strict",
            "preparation-failure outcome per route under the explicit Strict policy",
        )
    } else {
        (
            "preparation-policy",
            "preparation outcome per route under the default BestEffort policy",
        )
    };
    pin.spec(
        id,
        description,
        shape,
        1,
        vec![LifecycleOp::Load, LifecycleOp::Evaluate],
    )
}

/// Structural edits over spans with live overrides lose the overrides on the
/// Program 1 base (observed per pin, `failure` is the pin's first mismatch).
/// Fixed on the M3 perf line, which is not on this base.
fn structural_known(spec: ScenarioSpec, failure: FailureFingerprint) -> ScenarioSpec {
    known(
        spec,
        &SPAN_PLACING,
        failure,
        "base: a structural edit over a span with live point overrides republishes family results over the overrides (fixed on the M3 perf line by 3ec4d547, not on this base; M2/M3 must pass it)",
    )
}

/// A column oracle at `step` finds the one formula override at `cell` showing
/// the family result (202) instead of 505.
fn republished_override(step: usize, cell: &str) -> FailureFingerprint {
    FailureFingerprint::expectation(
        step,
        format!("1 cells differ: {cell}: expected Some(505.0), got Some(Number(202.0))"),
    )
}

/// All M0 pins. Registered beside the witnesses in the scenario runner.
pub fn pin_registry() -> Vec<ScenarioSpec> {
    let mut specs = vec![
        form117(false),
        form117(true),
        sheet_delete_readd(),
        preparation_policy(false),
        preparation_policy(true),
    ];
    specs.extend(span_pins());
    specs
}
