//! Named-reference span support: acceptance, dirty precision, name lifecycle
//! invalidation (define/update/delete), cross-sheet and shadowed resolution,
//! the InternalDependency guard, and precise fallback reasons.
//!
//! Defined names canonicalize by identity (raw text participates in the
//! template fingerprint) and resolve to all-absolute read regions per cell at
//! ingest. These tests pin (counter-style, never wall time):
//!
//! - workbook-scoped named-range/named-cell families promote to spans and
//!   evaluate to legacy-identical values, before and after edits;
//! - edits inside the resolved named region re-evaluate the span, edits
//!   outside do not;
//! - define/update/delete of a name demotes name-dependent spans so their
//!   cells re-ingest and re-resolve exactly like legacy formulas (the
//!   load-bearing invalidation hook: without it, a span keeps a stale
//!   resolved read region and misses edits inside the new region);
//! - sheet-scope shadowing resolves per placement sheet without
//!   cross-contaminating span families on other sheets;
//! - a name covering the family's own result column falls back
//!   (InternalDependency), and Literal-definition/undefined names fall back
//!   with `UnsupportedNamedReference`.

use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;

const SHEET: &str = "Sheet1";
/// Must be >= 100 (`MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS`).
const ROWS: u32 = 120;
const FIRST_ROW: u32 = 2;
const LAST_ROW: u32 = ROWS + 1;

fn engine_with_mode(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let cfg = EvalConfig::default().with_formula_plane_mode(mode);
    Engine::new(TestWorkbook::default(), cfg)
}

fn cell_ref(engine: &mut Engine<TestWorkbook>, sheet: &str, row1: u32, col1: u32) -> CellRef {
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    CellRef::new(sheet_id, Coord::new(row1 - 1, col1 - 1, true, true))
}

fn range_def(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    start_row1: u32,
    start_col1: u32,
    end_row1: u32,
    end_col1: u32,
) -> NamedDefinition {
    let start = cell_ref(engine, sheet, start_row1, start_col1);
    let end = cell_ref(engine, sheet, end_row1, end_col1);
    NamedDefinition::Range(RangeRef::new(start, end))
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn ingest_column(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    col: u32,
    formula_for_row: impl Fn(u32) -> String,
) -> crate::engine::FormulaIngestReport {
    let mut records = Vec::with_capacity(ROWS as usize);
    for row in FIRST_ROW..=LAST_ROW {
        let formula = formula_for_row(row);
        records.push(record(engine, row, col, &formula));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(sheet, records)])
        .expect("ingest formulas")
}

fn literal_eq(a: &LiteralValue, b: &LiteralValue) -> bool {
    fn as_num(v: &LiteralValue) -> Option<f64> {
        match v {
            LiteralValue::Number(n) => Some(*n),
            LiteralValue::Int(i) => Some(*i as f64),
            LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }
    match (as_num(a), as_num(b)) {
        (Some(x), Some(y)) => {
            let scale = x.abs().max(y.abs()).max(1.0);
            (x - y).abs() <= scale * 1e-9
        }
        _ => a == b,
    }
}

fn assert_column_parity(
    label: &str,
    sheet: &str,
    col: u32,
    auth: &Engine<TestWorkbook>,
    off: &Engine<TestWorkbook>,
) {
    for row in FIRST_ROW..=LAST_ROW {
        let got = auth.get_cell_value(sheet, row, col);
        let expected = off.get_cell_value(sheet, row, col);
        let equal = match (&got, &expected) {
            (Some(a), Some(b)) => literal_eq(a, b),
            (a, b) => a == b,
        };
        assert!(
            equal,
            "{label}: value mismatch at {sheet}!R{row}C{col}: auth={got:?} off={expected:?}"
        );
    }
}

fn set_value(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, value: f64) {
    engine
        .action_atomic_journal(format!("edit {sheet}!R{row}C{col}"), |tx| {
            tx.set_cell_value(sheet, row, col, LiteralValue::Number(value))?;
            Ok(())
        })
        .unwrap();
}

/// Data column B carries `row` as value; A carries `10*row`.
fn seed_named_workbook(engine: &mut Engine<TestWorkbook>) {
    for row in FIRST_ROW..=LAST_ROW {
        engine
            .set_cell_value(SHEET, row, 2, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(10.0 * row as f64))
            .unwrap();
    }
    let def = range_def(engine, SHEET, FIRST_ROW, 2, LAST_ROW, 2);
    engine
        .define_name("Data", def, NameScope::Workbook)
        .unwrap();
}

/// (a) Acceptance + first-eval/after-edit value parity, counter-pinned.
#[test]
fn named_range_family_promotes_to_span_with_value_parity() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        seed_named_workbook(engine);
    }
    let report = ingest_column(&mut auth, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    let _ = ingest_column(&mut off, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));

    assert_eq!(
        report.shadow_accepted_span_cells,
        u64::from(ROWS),
        "named-range family must span; histogram: {:?}",
        report.fallback_reasons
    );
    assert_eq!(report.shadow_fallback_cells, 0);
    assert!(report.shadow_spans_created >= 1);
    assert_eq!(auth.baseline_stats().formula_plane_active_span_count, 1);

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("first eval", SHEET, 3, &auth, &off);

    // Edit inside the named region and a relative precedent; values must stay
    // legacy-identical.
    for engine in [&mut auth, &mut off] {
        set_value(engine, SHEET, 10, 2, 5_000.0);
        set_value(engine, SHEET, 11, 1, 7_000.0);
    }
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("after edits", SHEET, 3, &auth, &off);
}

/// (b) Dirty precision: edits inside the resolved named region re-evaluate
/// the span; edits outside do not.
#[test]
fn named_range_edit_dirty_precision_is_region_bounded() {
    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    seed_named_workbook(&mut engine);
    let report = ingest_column(&mut engine, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    assert_eq!(report.shadow_accepted_span_cells, u64::from(ROWS));

    engine.evaluate_all().unwrap();
    let first = engine
        .last_formula_plane_span_eval_report()
        .expect("first eval must run the authoritative span pass");
    assert_eq!(first.span_eval_placement_count, u64::from(ROWS));

    // Inside the named region: every placement reads it.
    set_value(&mut engine, SHEET, 10, 2, 5_000.0);
    engine.evaluate_all().unwrap();
    let inside = engine
        .last_formula_plane_span_eval_report()
        .expect("edit inside named region must produce span work");
    assert_eq!(inside.span_eval_placement_count, u64::from(ROWS));

    // Relative precedent A11: exactly one placement reads it.
    set_value(&mut engine, SHEET, 11, 1, 7_000.0);
    engine.evaluate_all().unwrap();
    let relative = engine
        .last_formula_plane_span_eval_report()
        .expect("edit of a relative precedent must produce span work");
    assert_eq!(relative.span_eval_placement_count, 1);

    // Outside every read region (column F): no span recompute.
    set_value(&mut engine, SHEET, 10, 6, 9_999.0);
    engine.evaluate_all().unwrap();
    let outside_placements = engine
        .last_formula_plane_span_eval_report()
        .map(|report| report.span_eval_placement_count)
        .unwrap_or(0);
    assert_eq!(
        outside_placements, 0,
        "edit outside all read regions must not re-evaluate the span"
    );
}

/// (c) THE load-bearing invalidation test: update_name to a different region
/// must demote the span so cells re-resolve. Without the hook the span keeps
/// the stale resolved region and misses edits inside the new region.
#[test]
fn update_name_to_new_region_invalidates_spans_and_tracks_new_region() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        seed_named_workbook(engine);
        // Alternate data column D for the re-pointed name.
        for row in FIRST_ROW..=LAST_ROW {
            engine
                .set_cell_value(SHEET, row, 4, LiteralValue::Number(1_000.0 + row as f64))
                .unwrap();
        }
    }
    let report = ingest_column(&mut auth, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    let _ = ingest_column(&mut off, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    assert_eq!(report.shadow_accepted_span_cells, u64::from(ROWS));

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("before update_name", SHEET, 3, &auth, &off);

    for engine in [&mut auth, &mut off] {
        let def = range_def(engine, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
        engine
            .update_name("Data", def, NameScope::Workbook)
            .unwrap();
    }
    // The span resolved the old region; the invalidation hook must demote it.
    assert_eq!(
        auth.baseline_stats().formula_plane_active_span_count,
        0,
        "update_name must demote the name-dependent span"
    );
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("after update_name", SHEET, 3, &auth, &off);

    // Edit inside the NEW region (column D): values must track it.
    for engine in [&mut auth, &mut off] {
        set_value(engine, SHEET, 20, 4, 50_000.0);
    }
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("edit inside re-pointed region", SHEET, 3, &auth, &off);
}

/// (d) define_name of a sheet-scoped name AFTER ingest that shadows a
/// workbook-scoped name already resolved by spans on that sheet.
#[test]
fn sheet_scoped_define_after_ingest_invalidates_workbook_resolved_spans() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        seed_named_workbook(engine);
        for row in FIRST_ROW..=LAST_ROW {
            engine
                .set_cell_value(SHEET, row, 4, LiteralValue::Number(2_000.0 + row as f64))
                .unwrap();
        }
    }
    let report = ingest_column(&mut auth, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    let _ = ingest_column(&mut off, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    assert_eq!(report.shadow_accepted_span_cells, u64::from(ROWS));

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("before shadowing define", SHEET, 3, &auth, &off);

    for engine in [&mut auth, &mut off] {
        let sheet_id = engine.graph.sheet_id_mut(SHEET);
        let def = range_def(engine, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
        engine
            .define_name("Data", def, NameScope::Sheet(sheet_id))
            .unwrap();
    }
    assert_eq!(
        auth.baseline_stats().formula_plane_active_span_count,
        0,
        "shadowing define must demote spans that resolved through workbook scope"
    );
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("after shadowing define", SHEET, 3, &auth, &off);

    // Edits inside the shadowed (sheet-scoped) region must flow identically.
    for engine in [&mut auth, &mut off] {
        set_value(engine, SHEET, 30, 4, 77_000.0);
    }
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("edit inside shadowed region", SHEET, 3, &auth, &off);
}

/// (e) delete_name: cells fall back to legacy and evaluate to #NAME? exactly
/// like the Off-mode ground truth.
#[test]
fn delete_name_falls_back_to_name_error_parity() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        seed_named_workbook(engine);
    }
    let report = ingest_column(&mut auth, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    let _ = ingest_column(&mut off, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    assert_eq!(report.shadow_accepted_span_cells, u64::from(ROWS));

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();

    for engine in [&mut auth, &mut off] {
        engine.delete_name("Data", NameScope::Workbook).unwrap();
    }
    assert_eq!(auth.baseline_stats().formula_plane_active_span_count, 0);
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("after delete_name", SHEET, 3, &auth, &off);

    // And the parity target itself must be #NAME?, not a stale number.
    match off.get_cell_value(SHEET, FIRST_ROW, 3) {
        Some(LiteralValue::Error(err)) => {
            assert_eq!(err.kind, formualizer_common::ExcelErrorKind::Name)
        }
        other => panic!("expected #NAME? ground truth after delete_name, got {other:?}"),
    }
}

/// (f) Named CELL definition and a named range defined on ANOTHER sheet than
/// the formulas (cross-sheet resolution), counter-pinned with dirty checks.
#[test]
fn named_cell_and_cross_sheet_named_range_span_and_track_edits() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        engine.add_sheet("Sheet2").unwrap();
        for row in FIRST_ROW..=LAST_ROW {
            engine
                .set_cell_value("Sheet2", row, 2, LiteralValue::Number(row as f64))
                .unwrap();
            engine
                .set_cell_value(SHEET, row, 1, LiteralValue::Number(3.0 * row as f64))
                .unwrap();
        }
        engine
            .set_cell_value("Sheet2", 1, 5, LiteralValue::Number(41.5))
            .unwrap();
        let remote = range_def(engine, "Sheet2", FIRST_ROW, 2, LAST_ROW, 2);
        engine
            .define_name("RemoteData", remote, NameScope::Workbook)
            .unwrap();
        let cell = cell_ref(engine, "Sheet2", 1, 5);
        engine
            .define_name(
                "RemoteCell",
                NamedDefinition::Cell(cell),
                NameScope::Workbook,
            )
            .unwrap();
    }
    let report = ingest_column(&mut auth, SHEET, 3, |r| {
        format!("=SUM(RemoteData)+RemoteCell*2+A{r}")
    });
    let _ = ingest_column(&mut off, SHEET, 3, |r| {
        format!("=SUM(RemoteData)+RemoteCell*2+A{r}")
    });
    assert_eq!(
        report.shadow_accepted_span_cells,
        u64::from(ROWS),
        "cross-sheet named range + named cell must span; histogram: {:?}",
        report.fallback_reasons
    );

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("cross-sheet first eval", SHEET, 3, &auth, &off);

    // Edit inside the remote named region and the named cell.
    for engine in [&mut auth, &mut off] {
        set_value(engine, "Sheet2", 15, 2, 12_345.0);
        set_value(engine, "Sheet2", 1, 5, 100.25);
    }
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("cross-sheet after edits", SHEET, 3, &auth, &off);
}

/// Section-2 subtlety: the same name string resolving differently per sheet
/// (sheet-scope shadowing) must not cross-contaminate span families.
#[test]
fn shadowed_names_on_two_sheets_resolve_per_sheet_without_cross_contamination() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        engine.add_sheet("Sheet2").unwrap();
        for row in FIRST_ROW..=LAST_ROW {
            engine
                .set_cell_value(SHEET, row, 2, LiteralValue::Number(row as f64))
                .unwrap();
            engine
                .set_cell_value("Sheet2", row, 2, LiteralValue::Number(1_000.0 + row as f64))
                .unwrap();
            engine
                .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
                .unwrap();
            engine
                .set_cell_value("Sheet2", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
        }
        // Workbook-scoped X -> Sheet1!B; sheet-scoped X on Sheet2 -> Sheet2!B.
        let workbook = range_def(engine, SHEET, FIRST_ROW, 2, LAST_ROW, 2);
        engine
            .define_name("X", workbook, NameScope::Workbook)
            .unwrap();
        let sheet2_id = engine.graph.sheet_id_mut("Sheet2");
        let shadowed = range_def(engine, "Sheet2", FIRST_ROW, 2, LAST_ROW, 2);
        engine
            .define_name("X", shadowed, NameScope::Sheet(sheet2_id))
            .unwrap();
    }
    for sheet in [SHEET, "Sheet2"] {
        let report_auth = ingest_column(&mut auth, sheet, 3, |r| format!("=SUM(X)+A{r}"));
        let _ = ingest_column(&mut off, sheet, 3, |r| format!("=SUM(X)+A{r}"));
        assert_eq!(
            report_auth.shadow_accepted_span_cells,
            u64::from(ROWS),
            "{sheet}: shadowed-name family must span; histogram: {:?}",
            report_auth.fallback_reasons
        );
    }
    assert_eq!(auth.baseline_stats().formula_plane_active_span_count, 2);

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("shadowed Sheet1", SHEET, 3, &auth, &off);
    assert_column_parity("shadowed Sheet2", "Sheet2", 3, &auth, &off);

    // The two sheets must see different sums (proof the resolutions differ).
    let s1 = auth.get_cell_value(SHEET, FIRST_ROW, 3);
    let s2 = auth.get_cell_value("Sheet2", FIRST_ROW, 3);
    assert_ne!(s1, s2, "shadowed name resolutions must differ per sheet");

    // An edit inside Sheet2's shadowed region must not touch Sheet1 values.
    for engine in [&mut auth, &mut off] {
        set_value(engine, "Sheet2", 40, 2, 99_000.0);
    }
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("shadowed Sheet1 after Sheet2 edit", SHEET, 3, &auth, &off);
    assert_column_parity("shadowed Sheet2 after edit", "Sheet2", 3, &auth, &off);
}

/// (g) Self-overlap: a name whose region covers the family's own result
/// column must reject with InternalDependency and stay value-identical to
/// the Off-mode ground truth.
#[test]
fn name_covering_own_result_column_rejects_with_internal_dependency() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        for row in FIRST_ROW..=LAST_ROW {
            engine
                .set_cell_value(SHEET, row, 2, LiteralValue::Number(row as f64))
                .unwrap();
        }
        // Covers column C rows 2..=LAST_ROW: the formulas' own result region.
        let def = range_def(engine, SHEET, FIRST_ROW, 3, LAST_ROW, 3);
        engine
            .define_name("SelfRegion", def, NameScope::Workbook)
            .unwrap();
    }
    let report = ingest_column(&mut auth, SHEET, 3, |r| format!("=SUM(SelfRegion)*0+B{r}"));
    let _ = ingest_column(&mut off, SHEET, 3, |r| format!("=SUM(SelfRegion)*0+B{r}"));

    assert_eq!(report.shadow_accepted_span_cells, 0);
    assert_eq!(
        report
            .fallback_reasons
            .get("InternalDependency")
            .copied()
            .unwrap_or(0),
        u64::from(ROWS),
        "histogram: {:?}",
        report.fallback_reasons
    );
    assert_eq!(auth.baseline_stats().formula_plane_active_span_count, 0);

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("self-overlap", SHEET, 3, &auth, &off);
}

/// (h) Literal-definition and undefined names fall back with the precise
/// `UnsupportedNamedReference` reason, values legacy-identical.
#[test]
fn literal_and_undefined_names_fall_back_with_precise_reason() {
    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        for row in FIRST_ROW..=LAST_ROW {
            engine
                .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
                .unwrap();
        }
        engine
            .define_name(
                "LitName",
                NamedDefinition::Literal(LiteralValue::Number(2.5)),
                NameScope::Workbook,
            )
            .unwrap();
    }

    // Literal-definition name -> column C.
    let literal_report = ingest_column(&mut auth, SHEET, 3, |r| format!("=LitName+A{r}"));
    let _ = ingest_column(&mut off, SHEET, 3, |r| format!("=LitName+A{r}"));
    assert_eq!(literal_report.shadow_accepted_span_cells, 0);
    assert_eq!(
        literal_report
            .fallback_reasons
            .get("UnsupportedNamedReference")
            .copied()
            .unwrap_or(0),
        u64::from(ROWS),
        "histogram: {:?}",
        literal_report.fallback_reasons
    );

    // Undefined name -> column D.
    let undefined_report = ingest_column(&mut auth, SHEET, 4, |r| format!("=NoSuchName+A{r}"));
    let _ = ingest_column(&mut off, SHEET, 4, |r| format!("=NoSuchName+A{r}"));
    assert_eq!(undefined_report.shadow_accepted_span_cells, 0);
    assert_eq!(
        undefined_report
            .fallback_reasons
            .get("UnsupportedNamedReference")
            .copied()
            .unwrap_or(0),
        u64::from(ROWS),
        "histogram: {:?}",
        undefined_report.fallback_reasons
    );

    assert_eq!(auth.baseline_stats().formula_plane_active_span_count, 0);

    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("literal-name column", SHEET, 3, &auth, &off);
    assert_column_parity("undefined-name column", SHEET, 4, &auth, &off);

    // Ground-truth sanity: literal-name cells are numeric, undefined-name
    // cells are #NAME?.
    match off.get_cell_value(SHEET, FIRST_ROW, 3) {
        Some(LiteralValue::Number(n)) => assert!((n - (2.5 + FIRST_ROW as f64)).abs() < 1e-9),
        other => panic!("expected numeric literal-name ground truth, got {other:?}"),
    }
    match off.get_cell_value(SHEET, FIRST_ROW, 4) {
        Some(LiteralValue::Error(err)) => {
            assert_eq!(err.kind, formualizer_common::ExcelErrorKind::Name)
        }
        other => panic!("expected #NAME? ground truth for undefined name, got {other:?}"),
    }
}

#[test]
fn logged_name_update_forward_undo_redo_preserves_value_parity() {
    use crate::engine::ChangeLog;
    use crate::engine::graph::editor::undo_engine::UndoEngine;

    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    for engine in [&mut auth, &mut off] {
        seed_named_workbook(engine);
        for row in FIRST_ROW..=LAST_ROW {
            engine
                .set_cell_value(SHEET, row, 4, LiteralValue::Number(1_000.0 + row as f64))
                .unwrap();
        }
    }
    ingest_column(&mut auth, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    ingest_column(&mut off, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();

    let old_definition = range_def(&mut off, SHEET, FIRST_ROW, 2, LAST_ROW, 2);
    let new_definition_auth = range_def(&mut auth, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
    let new_definition_off = range_def(&mut off, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
    let mut log = ChangeLog::new();
    auth.update_name_with_logger(&mut log, "Data", new_definition_auth, NameScope::Workbook)
        .unwrap();
    off.update_name("Data", new_definition_off.clone(), NameScope::Workbook)
        .unwrap();
    assert_eq!(auth.baseline_stats().formula_plane_active_span_count, 0);
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("logged update", SHEET, 3, &auth, &off);

    let mut undo = UndoEngine::new();
    auth.undo_logged(&mut undo, &mut log).unwrap();
    off.update_name("Data", old_definition, NameScope::Workbook)
        .unwrap();
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("logged update undo", SHEET, 3, &auth, &off);

    auth.redo_logged(&mut undo, &mut log).unwrap();
    off.update_name("Data", new_definition_off, NameScope::Workbook)
        .unwrap();
    auth.evaluate_all().unwrap();
    off.evaluate_all().unwrap();
    assert_column_parity("logged update redo", SHEET, 3, &auth, &off);
}

#[test]
fn logged_name_define_and_delete_demote_exact_dependents() {
    use crate::engine::ChangeLog;

    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    seed_named_workbook(&mut engine);
    for row in FIRST_ROW..=LAST_ROW {
        engine
            .set_cell_value(SHEET, row, 4, LiteralValue::Number(2_000.0 + row as f64))
            .unwrap();
    }
    ingest_column(&mut engine, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    engine.evaluate_all().unwrap();

    let sheet_id = engine.graph.sheet_id_mut(SHEET);
    let shadow = range_def(&mut engine, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
    let mut log = ChangeLog::new();
    engine
        .define_name_with_logger(&mut log, "Data", shadow, NameScope::Sheet(sheet_id))
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().unwrap();

    ingest_column(&mut engine, SHEET, 5, |r| format!("=SUM(Data)+A{r}"));
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    engine
        .delete_name_with_logger(&mut log, "Data", NameScope::Sheet(sheet_id))
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value(SHEET, FIRST_ROW, 5),
        Some(LiteralValue::Number(_))
    ));
}

#[test]
fn logged_name_demotion_limit_and_fault_are_atomic_and_retryable() {
    use crate::engine::ChangeLog;
    use crate::engine::eval::FormulaSpanDemotionFault;

    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    seed_named_workbook(&mut engine);
    for row in FIRST_ROW..=LAST_ROW {
        engine
            .set_cell_value(SHEET, row, 4, LiteralValue::Number(3_000.0 + row as f64))
            .unwrap();
    }
    ingest_column(&mut engine, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    engine.evaluate_all().unwrap();
    let refs = engine.graph.formula_authority().active_span_refs();
    let old_definition = engine
        .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
        .unwrap()
        .definition
        .clone();
    let new_definition = range_def(&mut engine, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
    let mut log = ChangeLog::new();

    let original_limits = engine.workbook_load_limits().clone();
    let mut limited = original_limits.clone();
    limited.max_formula_plane_fallback_cells = 0;
    engine.set_workbook_load_limits(limited);
    assert!(
        engine
            .update_name_with_logger(
                &mut log,
                "Data",
                new_definition.clone(),
                NameScope::Workbook,
            )
            .is_err()
    );
    assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        old_definition
    );
    assert!(log.is_empty());

    engine.set_workbook_load_limits(original_limits);
    engine.set_formula_span_demotion_fault_for_test(FormulaSpanDemotionFault::BeforeFirstMutation);
    assert!(
        engine
            .update_name_with_logger(
                &mut log,
                "Data",
                new_definition.clone(),
                NameScope::Workbook,
            )
            .is_err()
    );
    assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        old_definition
    );
    assert!(log.is_empty());

    engine
        .update_name_with_logger(&mut log, "Data", new_definition, NameScope::Workbook)
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(log.len(), 1);
}

#[test]
fn generic_edit_with_logger_rejects_and_rolls_back_name_mutations() {
    use crate::engine::ChangeLog;

    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    seed_named_workbook(&mut engine);
    ingest_column(&mut engine, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    engine.evaluate_all().unwrap();
    let refs = engine.graph.formula_authority().active_span_refs();
    let old_definition = engine
        .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
        .unwrap()
        .definition
        .clone();
    let new_definition = range_def(&mut engine, SHEET, FIRST_ROW, 1, LAST_ROW, 1);
    let mut log = ChangeLog::new();
    let result = engine.edit_with_logger(&mut log, |editor| {
        editor.update_name("Data", new_definition, NameScope::Workbook)
    });
    assert!(result.is_err());
    assert!(log.is_empty());
    assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        old_definition
    );
}

#[test]
fn logged_name_undo_redo_faults_leave_history_and_authority_retryable() {
    use crate::engine::ChangeLog;
    use crate::engine::eval::FormulaSpanDemotionFault;
    use crate::engine::graph::editor::undo_engine::UndoEngine;

    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    seed_named_workbook(&mut engine);
    for row in FIRST_ROW..=LAST_ROW {
        engine
            .set_cell_value(SHEET, row, 4, LiteralValue::Number(4_000.0 + row as f64))
            .unwrap();
    }
    ingest_column(&mut engine, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    engine.evaluate_all().unwrap();

    let old_definition = engine
        .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
        .unwrap()
        .definition
        .clone();
    let new_definition = range_def(&mut engine, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
    let mut log = ChangeLog::new();
    engine
        .update_name_with_logger(
            &mut log,
            "Data",
            new_definition.clone(),
            NameScope::Workbook,
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    ingest_column(&mut engine, SHEET, 5, |r| format!("=SUM(Data)+A{r}"));
    engine.evaluate_all().unwrap();
    let undo_refs = engine.graph.formula_authority().active_span_refs();
    let undo_log_len = log.len();
    let mut undo = UndoEngine::new();
    engine.set_formula_span_demotion_fault_for_test(FormulaSpanDemotionFault::BeforeFirstMutation);
    assert!(engine.undo_logged(&mut undo, &mut log).is_err());
    assert_eq!(log.len(), undo_log_len);
    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        undo_refs
    );
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        new_definition
    );

    engine.undo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        old_definition
    );
    engine.evaluate_all().unwrap();

    ingest_column(&mut engine, SHEET, 6, |r| format!("=SUM(Data)+A{r}"));
    engine.evaluate_all().unwrap();
    let redo_refs = engine.graph.formula_authority().active_span_refs();
    let redo_log_len = log.len();
    engine.set_formula_span_demotion_fault_for_test(FormulaSpanDemotionFault::BeforeFirstMutation);
    assert!(engine.redo_logged(&mut undo, &mut log).is_err());
    assert_eq!(log.len(), redo_log_len);
    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        redo_refs
    );
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        old_definition
    );

    engine.redo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        new_definition
    );
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value(SHEET, FIRST_ROW, 6),
        Some(LiteralValue::Number(_))
    ));
}

#[test]
fn direct_name_update_demotes_disjoint_spans_as_one_retryable_batch() {
    use crate::engine::eval::FormulaSpanDemotionFault;

    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    seed_named_workbook(&mut engine);
    for row in FIRST_ROW..=LAST_ROW {
        engine
            .set_cell_value(SHEET, row, 4, LiteralValue::Number(6_000.0 + row as f64))
            .unwrap();
    }
    ingest_column(&mut engine, SHEET, 3, |r| format!("=SUM(Data)+A{r}"));
    ingest_column(&mut engine, SHEET, 5, |r| format!("=SUM(Data)+A{r}"));
    engine.evaluate_all().unwrap();

    let refs_before = engine.graph.formula_authority().active_span_refs();
    assert_eq!(refs_before.len(), 2);
    let old_definition = engine
        .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
        .unwrap()
        .definition
        .clone();
    let new_definition = range_def(&mut engine, SHEET, FIRST_ROW, 4, LAST_ROW, 4);
    let topology_before = engine.topology_epoch_for_test();
    let graph_revision_before = engine.graph_topology_revision_for_test();
    let dirty_before = engine.graph.formula_dirty_stats();

    engine.set_formula_span_demotion_fault_for_test(FormulaSpanDemotionFault::BeforeFirstMutation);
    assert!(
        engine
            .update_name("Data", new_definition.clone(), NameScope::Workbook,)
            .is_err()
    );
    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        refs_before
    );
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    assert_eq!(
        engine.graph_topology_revision_for_test(),
        graph_revision_before
    );
    assert_eq!(engine.graph.formula_dirty_stats(), dirty_before);
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        old_definition
    );

    engine
        .update_name("Data", new_definition.clone(), NameScope::Workbook)
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        engine
            .resolve_name_entry("Data", engine.graph.sheet_id(SHEET).unwrap())
            .unwrap()
            .definition,
        new_definition
    );
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value(SHEET, FIRST_ROW, 3),
        Some(LiteralValue::Number(_))
    ));
    assert!(matches!(
        engine.get_cell_value(SHEET, FIRST_ROW, 5),
        Some(LiteralValue::Number(_))
    ));
}
