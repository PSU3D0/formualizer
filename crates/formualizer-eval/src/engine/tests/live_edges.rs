//! Tests for the Stage-1 live-edge collector (pre-work for RFC #112).
//!
//! These drive a real `Engine` wrapped in `RecordingContext`, evaluating
//! formula ASTs via `Interpreter` directly — exactly how Stage-2 SCC tasks
//! will evaluate statically-cyclic members. Nothing here touches production
//! evaluation paths: `RecordingContext` is constructed only by this test
//! module, and no public `Engine` API exposes or stores a collector.

use crate::engine::live_edges::{LiveEdgeCollector, RecordingContext};
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use rustc_hash::FxHashSet;

fn parse(formula: &str) -> formualizer_parse::parser::ASTNode {
    formualizer_parse::parser::parse(formula).expect("valid formula")
}

fn new_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn cell(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> CellRef {
    CellRef::new(
        engine.sheet_id(sheet).expect("sheet exists"),
        Coord::from_excel(row, col, true, true),
    )
}

/// Evaluate `formula` as if it were the body of `member` (an SCC member at
/// `member_idx`), recording live edges into `collector`.
fn eval_as_member(
    engine: &Engine<TestWorkbook>,
    collector: &LiveEdgeCollector,
    member_idx: u32,
    sheet: &str,
    member: CellRef,
    formula: &str,
) -> LiteralValue {
    collector.set_current(member_idx);
    let ctx = RecordingContext::new(engine, collector);
    let interp = Interpreter::new_with_cell(&ctx, sheet, member);
    interp
        .evaluate_ast(&parse(formula))
        .map(|cv| cv.into_literal())
        .unwrap_or_else(LiteralValue::Error)
}

fn edges(collector: &LiveEdgeCollector) -> FxHashSet<(u32, u32)> {
    collector.take_edges()
}

fn set_num(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, v: f64) {
    engine
        .set_cell_value(sheet, row, col, LiteralValue::Number(v))
        .unwrap();
}

/* ─────────────────────────── 1. scalar reads ─────────────────────────── */

#[test]
fn scalar_read_of_member_records_edge() {
    let mut engine = new_engine();
    set_num(&mut engine, "Sheet1", 1, 1, 5.0); // A1 (member)
    set_num(&mut engine, "Sheet1", 1, 2, 7.0); // B1 (non-member)
    engine.evaluate_all().unwrap();

    // Members: [C1 (the evaluating member), A1].
    let c1 = cell(&engine, "Sheet1", 1, 3);
    let a1 = cell(&engine, "Sheet1", 1, 1);
    let collector = LiveEdgeCollector::new(&[c1, a1]);

    let v = eval_as_member(&engine, &collector, 0, "Sheet1", c1, "=A1");
    assert_eq!(v, LiteralValue::Number(5.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));

    // Reading a non-member records nothing.
    let v = eval_as_member(&engine, &collector, 0, "Sheet1", c1, "=B1");
    assert_eq!(v, LiteralValue::Number(7.0));
    assert!(edges(&collector).is_empty());
}

/* ──────────────────────── 2. short-circuiting ────────────────────────── */

/// For each (formula_no_edge, formula_edge) pair: the member read sits in a
/// branch that is untaken in the first formula and taken in the second.
fn assert_short_circuit_polarity(no_edge: &str, edge_expected: &str) {
    let mut engine = new_engine();
    set_num(&mut engine, "Sheet1", 1, 1, 5.0); // A1 (member)
    engine.evaluate_all().unwrap();

    let c1 = cell(&engine, "Sheet1", 1, 3);
    let a1 = cell(&engine, "Sheet1", 1, 1);
    let collector = LiveEdgeCollector::new(&[c1, a1]);

    eval_as_member(&engine, &collector, 0, "Sheet1", c1, no_edge);
    assert!(
        edges(&collector).is_empty(),
        "{no_edge}: untaken branch must record no live edge"
    );

    eval_as_member(&engine, &collector, 0, "Sheet1", c1, edge_expected);
    assert_eq!(
        edges(&collector),
        FxHashSet::from_iter([(0, 1)]),
        "{edge_expected}: taken branch must record the live edge"
    );
}

#[test]
fn if_short_circuit_polarity() {
    assert_short_circuit_polarity("=IF(TRUE, 1, A1)", "=IF(FALSE, 1, A1)");
}

#[test]
fn ifs_short_circuit_polarity() {
    assert_short_circuit_polarity("=IFS(TRUE, 1, TRUE, A1)", "=IFS(FALSE, 1, TRUE, A1)");
}

#[test]
fn choose_short_circuit_polarity() {
    assert_short_circuit_polarity("=CHOOSE(1, 9, A1)", "=CHOOSE(2, 9, A1)");
}

#[test]
fn switch_short_circuit_polarity() {
    // First: case 1 matches, default (A1) untaken. Second: default taken.
    assert_short_circuit_polarity("=SWITCH(1, 1, 9, A1)", "=SWITCH(2, 1, 9, A1)");
}

/* ─────────────────────────── 3. range reads ──────────────────────────── */

#[test]
fn range_read_intersects_members_exactly() {
    let mut engine = new_engine();
    for r in 1..=10 {
        set_num(&mut engine, "Sheet1", r, 2, r as f64); // B1:B10
        set_num(&mut engine, "Sheet1", r, 3, r as f64); // C1:C10
    }
    engine.evaluate_all().unwrap();

    let d1 = cell(&engine, "Sheet1", 1, 4);
    let b3 = cell(&engine, "Sheet1", 3, 2);
    let b7 = cell(&engine, "Sheet1", 7, 2);
    let collector = LiveEdgeCollector::new(&[d1, b3, b7]);

    let v = eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(B1:B10)");
    assert_eq!(v, LiteralValue::Number(55.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1), (0, 2)]));

    // A rect not containing any member records nothing.
    eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(C1:C10)");
    assert!(edges(&collector).is_empty());

    // Rect adjacent to a member (B4:B6 vs members B3/B7) records nothing.
    eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(B4:B6)");
    assert!(edges(&collector).is_empty());
}

#[test]
fn range_read_boundary_inclusion_at_rect_corners() {
    let mut engine = new_engine();
    for r in 1..=10 {
        set_num(&mut engine, "Sheet1", r, 2, 1.0); // B1:B10
    }
    engine.evaluate_all().unwrap();

    let d1 = cell(&engine, "Sheet1", 1, 4);
    let b1 = cell(&engine, "Sheet1", 1, 2); // top corner of B1:B10
    let b10 = cell(&engine, "Sheet1", 10, 2); // bottom corner of B1:B10
    let collector = LiveEdgeCollector::new(&[d1, b1, b10]);

    eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(B1:B10)");
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1), (0, 2)]));

    // One row inside: B2:B9 excludes both corner members.
    eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(B2:B9)");
    assert!(edges(&collector).is_empty());
}

/* ─────────────────── 4. whole-column (unbounded) reads ───────────────── */

#[test]
fn whole_column_read_resolves_used_bounds_and_records_member() {
    let mut engine = new_engine();
    for r in 1..=10 {
        set_num(&mut engine, "Sheet1", r, 2, r as f64); // B1:B10
    }
    engine.evaluate_all().unwrap();

    let d1 = cell(&engine, "Sheet1", 1, 4);
    let b5 = cell(&engine, "Sheet1", 5, 2);
    let a5 = cell(&engine, "Sheet1", 5, 1); // not in column B
    let collector = LiveEdgeCollector::new(&[d1, b5, a5]);

    let v = eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(B:B)");
    assert_eq!(v, LiteralValue::Number(55.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));
}

/* ───────────────────────── 5. named ranges ───────────────────────────── */

#[test]
fn named_range_region_containing_member_records_edge() {
    let mut engine = new_engine();
    for r in 1..=5 {
        set_num(&mut engine, "Sheet1", r, 2, r as f64); // B1:B5
    }
    engine.evaluate_all().unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let nr_range = RangeRef::new(
        CellRef::new(sheet_id, Coord::from_excel(2, 2, true, true)), // B2
        CellRef::new(sheet_id, Coord::from_excel(4, 2, true, true)), // B4
    );
    engine
        .define_name("NR", NamedDefinition::Range(nr_range), NameScope::Workbook)
        .unwrap();

    let d1 = cell(&engine, "Sheet1", 1, 4);
    let b3 = cell(&engine, "Sheet1", 3, 2); // inside NR
    let b5 = cell(&engine, "Sheet1", 5, 2); // outside NR
    let collector = LiveEdgeCollector::new(&[d1, b3, b5]);

    let v = eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(NR)");
    assert_eq!(v, LiteralValue::Number(9.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));
}

#[test]
fn table_column_region_containing_member_records_edge() {
    let mut engine = new_engine();
    // Table T over A1:B3: header row + 2 data rows.
    set_num(&mut engine, "Sheet1", 2, 1, 5.0);
    set_num(&mut engine, "Sheet1", 2, 2, 10.0);
    set_num(&mut engine, "Sheet1", 3, 1, 7.0);
    set_num(&mut engine, "Sheet1", 3, 2, 20.0);
    engine.evaluate_all().unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let range = RangeRef::new(
        CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true)),
        CellRef::new(sheet_id, Coord::from_excel(3, 2, true, true)),
    );
    engine
        .define_table(
            "Sales",
            range,
            true,
            vec!["Region".into(), "Amount".into()],
            false,
        )
        .unwrap();

    let d1 = cell(&engine, "Sheet1", 1, 4);
    let b2 = cell(&engine, "Sheet1", 2, 2); // inside Sales[Amount]
    let a2 = cell(&engine, "Sheet1", 2, 1); // in Sales[Region], not [Amount]
    let collector = LiveEdgeCollector::new(&[d1, b2, a2]);

    let v = eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=SUM(Sales[Amount])");
    assert_eq!(v, LiteralValue::Number(30.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));
}

/* ──────────────────── 6. dynamic reads (INDIRECT) ─────────────────────── */

#[test]
fn indirect_scalar_read_flows_through_wrapper() {
    let mut engine = new_engine();
    set_num(&mut engine, "Sheet1", 3, 2, 42.0); // B3 (member)
    engine.evaluate_all().unwrap();

    let d1 = cell(&engine, "Sheet1", 1, 4);
    let b3 = cell(&engine, "Sheet1", 3, 2);
    let collector = LiveEdgeCollector::new(&[d1, b3]);

    let v = eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=INDIRECT(\"B3\")");
    assert_eq!(v, LiteralValue::Number(42.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));

    // Dynamic *range* read: the rect is recorded at resolution time.
    let v = eval_as_member(
        &engine,
        &collector,
        0,
        "Sheet1",
        d1,
        "=SUM(INDIRECT(\"B1:B5\"))",
    );
    assert_eq!(v, LiteralValue::Number(42.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));

    // Dynamic read of a non-member records nothing.
    eval_as_member(&engine, &collector, 0, "Sheet1", d1, "=INDIRECT(\"C3\")");
    assert!(edges(&collector).is_empty());
}

/* ─────────────────────────── 8. attribution ──────────────────────────── */

#[test]
fn edges_attribute_to_current_member() {
    let mut engine = new_engine();
    set_num(&mut engine, "Sheet1", 1, 1, 1.0); // A1
    set_num(&mut engine, "Sheet1", 2, 1, 2.0); // A2
    engine.evaluate_all().unwrap();

    let a1 = cell(&engine, "Sheet1", 1, 1);
    let a2 = cell(&engine, "Sheet1", 2, 1);
    let collector = LiveEdgeCollector::new(&[a1, a2]);

    // A1's formula reads A2; A2's formula reads A1 (a 2-cycle).
    eval_as_member(&engine, &collector, 0, "Sheet1", a1, "=A2");
    eval_as_member(&engine, &collector, 1, "Sheet1", a2, "=A1");
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1), (1, 0)]));
}

/* ─────────────────────────── 9. self-edges ───────────────────────────── */

#[test]
fn member_ranging_over_itself_records_self_edge() {
    let mut engine = new_engine();
    for r in 1..=3 {
        set_num(&mut engine, "Sheet1", r, 2, r as f64); // B1:B3
    }
    engine.evaluate_all().unwrap();

    let b2 = cell(&engine, "Sheet1", 2, 2);
    let collector = LiveEdgeCollector::new(&[b2]);

    // B2's formula ranges over B1:B3, which includes B2 itself.
    eval_as_member(&engine, &collector, 0, "Sheet1", b2, "=SUM(B1:B3)");
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 0)]));
}

/* ─────────────────────────── 10. multi-sheet ─────────────────────────── */

#[test]
fn cross_sheet_qualified_reads_record_member_on_other_sheet() {
    let mut engine = new_engine();
    engine.add_sheet("Sheet2").unwrap();
    set_num(&mut engine, "Sheet1", 1, 1, 1.0); // Sheet1!A1
    set_num(&mut engine, "Sheet2", 1, 1, 9.0); // Sheet2!A1 (member)
    set_num(&mut engine, "Sheet2", 2, 1, 8.0); // Sheet2!A2
    engine.evaluate_all().unwrap();

    let s1_c1 = cell(&engine, "Sheet1", 1, 3);
    let s2_a1 = cell(&engine, "Sheet2", 1, 1);
    let collector = LiveEdgeCollector::new(&[s1_c1, s2_a1]);

    // Scalar qualified read from Sheet1.
    let v = eval_as_member(&engine, &collector, 0, "Sheet1", s1_c1, "=Sheet2!A1");
    assert_eq!(v, LiteralValue::Number(9.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));

    // Same coordinates on the *current* sheet must NOT match the member.
    eval_as_member(&engine, &collector, 0, "Sheet1", s1_c1, "=A1");
    assert!(edges(&collector).is_empty());

    // Qualified range read.
    let v = eval_as_member(
        &engine,
        &collector,
        0,
        "Sheet1",
        s1_c1,
        "=SUM(Sheet2!A1:A2)",
    );
    assert_eq!(v, LiteralValue::Number(17.0));
    assert_eq!(edges(&collector), FxHashSet::from_iter([(0, 1)]));
}

/* ──────────────────────────── 11. inertness ──────────────────────────── */

/// Structural inertness: the acyclic/hot path never constructs a
/// `RecordingContext` — no `Engine` API creates, stores, or exposes one (the
/// only constructor takes an externally-owned collector), so production
/// evaluation is untouched by this module. This test pins the cheap proxies
/// for that argument: the wrapper is two borrowed pointers (no owned state to
/// allocate), and wrapped evaluation is value-identical to bare evaluation.
#[test]
fn wrapper_is_inert_and_value_transparent() {
    assert_eq!(
        std::mem::size_of::<RecordingContext<'_, TestWorkbook>>(),
        2 * std::mem::size_of::<usize>(),
        "RecordingContext must stay two borrowed pointers"
    );

    let mut engine = new_engine();
    for r in 1..=10 {
        set_num(&mut engine, "Sheet1", r, 2, r as f64); // B1:B10
    }
    set_num(&mut engine, "Sheet1", 1, 1, 100.0); // A1
    engine.evaluate_all().unwrap();

    let d1 = cell(&engine, "Sheet1", 1, 4);
    let formula = "=SUM(B1:B10)+A1*2";

    // Bare engine context (production shape).
    let bare = {
        let interp = Interpreter::new_with_cell(&engine, "Sheet1", d1);
        interp.evaluate_ast(&parse(formula)).unwrap().into_literal()
    };

    // Wrapped with an empty membership: identical value, zero edges.
    let collector = LiveEdgeCollector::new(&[]);
    let ctx = RecordingContext::new(&engine, &collector);
    let wrapped = {
        let interp = Interpreter::new_with_cell(&ctx, "Sheet1", d1);
        interp.evaluate_ast(&parse(formula)).unwrap().into_literal()
    };

    assert_eq!(bare, wrapped);
    assert!(collector.take_edges().is_empty());
}

/// Reads observed before `set_current` is called are not attributable to any
/// member and must be dropped rather than mis-attributed.
#[test]
fn reads_without_current_member_are_dropped() {
    let mut engine = new_engine();
    set_num(&mut engine, "Sheet1", 1, 1, 5.0); // A1 (member)
    engine.evaluate_all().unwrap();

    let a1 = cell(&engine, "Sheet1", 1, 1);
    let collector = LiveEdgeCollector::new(&[a1]);
    let ctx = RecordingContext::new(&engine, &collector);
    let d1 = cell(&engine, "Sheet1", 1, 4);
    let interp = Interpreter::new_with_cell(&ctx, "Sheet1", d1);
    let _ = interp.evaluate_ast(&parse("=A1"));
    assert!(collector.take_edges().is_empty());
}

/* ───────────── 12. spatial rect index & per-member edge diff ──────────── */

#[test]
fn rect_index_matches_linear_scan_for_large_multi_sheet_scc() {
    let mut engine = new_engine();
    engine.add_sheet("Sheet2").unwrap();
    engine.add_sheet("Sheet3").unwrap();
    let s1 = engine.sheet_id("Sheet1").unwrap();
    let s2 = engine.sheet_id("Sheet2").unwrap();
    let s3 = engine.sheet_id("Sheet3").unwrap();

    // Build > 16 members across Sheet1 (2D grid + sparse outliers) and Sheet2
    // in intentionally unsorted order to exercise the lazy dual-axis RectIndex.
    let mut members: Vec<CellRef> = Vec::new();
    for r in (0..10u32).rev() {
        for c in (0..4u32).rev() {
            if (r + c) % 2 == 0 {
                members.push(CellRef::new(s1, Coord::new(r, c, true, true)));
            }
        }
    }
    for r in [0u32, 3, 7, 12, 100] {
        members.push(CellRef::new(s2, Coord::new(r, 2, true, true)));
    }
    assert!(members.len() > 20);

    let collector = LiveEdgeCollector::new(&members);
    let queries = [
        // Single column on Sheet1 (col-axis wins)
        (s1, 2, 1, 8, 1),
        // Single row on Sheet1 (row-axis wins)
        (s1, 4, 0, 4, 3),
        // 2D interior rect
        (s1, 1, 1, 6, 3),
        // Single-cell rect (hit and miss)
        (s1, 0, 0, 0, 0),
        (s1, 0, 1, 0, 1),
        // Disjoint rect outside bounds
        (s1, 50, 0, 60, 10),
        // Inverted/empty rect
        (s1, 5, 0, 2, 3),
        // Sheet2 partial and wide rects
        (s2, 3, 2, 12, 2),
        (s2, 0, 0, 200, 10),
        // Sheet with no SCC members at all
        (s3, 0, 0, 10, 10),
    ];

    for &(sid, sr, sc, er, ec) in &queries {
        collector.set_current(0);
        collector.record_rect(sid, sr, sc, er, ec);
        let got = collector.take_edges();
        let expected: FxHashSet<(u32, u32)> = members
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                er >= sr
                    && ec >= sc
                    && m.sheet_id == sid
                    && m.coord.row() >= sr
                    && m.coord.row() <= er
                    && m.coord.col() >= sc
                    && m.coord.col() <= ec
            })
            .map(|(i, _)| (0, i as u32))
            .collect();
        assert_eq!(
            got, expected,
            "rect mismatch for sheet={sid} ({sr},{sc})..=({er},{ec})"
        );
    }
}

#[test]
fn finish_member_targets_detects_exact_edge_deltas() {
    let engine = new_engine();
    let s1 = engine.sheet_id("Sheet1").unwrap();
    let cells: Vec<CellRef> = (0..20u32)
        .map(|r| CellRef::new(s1, Coord::new(r, 0, true, true)))
        .collect();
    let collector = LiveEdgeCollector::new(&cells);
    let mut out: Vec<u32> = Vec::new();

    // 0 -> 0: unchanged.
    collector.begin_member(0);
    assert!(!collector.finish_member_targets(&mut out));
    assert!(out.is_empty());

    // 0 -> [5]: changed.
    collector.begin_member(0);
    collector.record_scalar(s1, 5, 0);
    assert!(collector.finish_member_targets(&mut out));
    assert_eq!(out, vec![5]);

    // [5] -> [5]: unchanged (fast 1-edge path).
    collector.begin_member(0);
    collector.record_scalar(s1, 5, 0);
    assert!(!collector.finish_member_targets(&mut out));
    assert_eq!(out, vec![5]);

    // [5] -> [5, 5] (duplicate scalar): unchanged after dedup.
    collector.begin_member(0);
    collector.record_scalar(s1, 5, 0);
    collector.record_scalar(s1, 5, 0);
    assert!(!collector.finish_member_targets(&mut out));
    assert_eq!(out, vec![5]);

    // [5] -> [9, 2]: changed and sorted to [2, 9].
    collector.begin_member(0);
    collector.record_scalar(s1, 9, 0);
    collector.record_scalar(s1, 2, 0);
    assert!(collector.finish_member_targets(&mut out));
    assert_eq!(out, vec![2, 9]);

    // [2, 9] -> [2, 9] in reversed order: unchanged.
    collector.begin_member(0);
    collector.record_scalar(s1, 2, 0);
    collector.record_scalar(s1, 9, 0);
    assert!(!collector.finish_member_targets(&mut out));

    // [2, 9] -> rect 1..=4 + dup scalar 3: changed to [1, 2, 3, 4].
    collector.begin_member(0);
    collector.record_rect(s1, 1, 0, 4, 0);
    collector.record_scalar(s1, 3, 0);
    assert!(collector.finish_member_targets(&mut out));
    assert_eq!(out, vec![1, 2, 3, 4]);

    // Same rect again: unchanged.
    collector.begin_member(0);
    collector.record_rect(s1, 1, 0, 4, 0);
    assert!(!collector.finish_member_targets(&mut out));

    // Drop back to empty: changed.
    collector.begin_member(0);
    assert!(collector.finish_member_targets(&mut out));
    assert!(out.is_empty());
}
