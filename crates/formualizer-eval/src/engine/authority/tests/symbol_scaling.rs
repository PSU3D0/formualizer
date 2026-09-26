//! Scaling gates for symbol revisions outside a load scope (red team #1):
//! defining, redefining or deleting a name, table or source costs work in
//! the changed symbol and its readers, not in the number of symbols or
//! formulas already defined, and never rebuilds the authority.
//!
//! Work is counted, not timed: the host's symbol-sync work plus the store's
//! index, planning, slot and symbol-table work.

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn work(e: &Engine<TestWorkbook>) -> u64 {
    let host = e.graph.authority_host();
    let st = &host.store().stats;
    host.symbol_sync_work()
        + st.index_work
        + st.plan_work
        + st.slot_work
        + st.symbol_work
        + crate::engine::authority::identity::run_slots_walked()
}

/// An engine with `formulas` independent formula cells, built. The
/// formulas sit on every other row, so each keeps its own identity run
/// (contiguous formulas would coalesce into one run and hide per-run work;
/// pre-landing check §1).
fn engine(formulas: u32) -> Engine<TestWorkbook> {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for i in 1..=formulas {
        let r = 2 * i;
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(i)))
            .unwrap();
        e.set_cell_formula("Sheet1", r, 2, parse("=A1*2+1").unwrap())
            .unwrap();
    }
    e.graph.authority().unwrap();
    e
}

/// Runs `op(e, i)` for `i in 0..n` on a fresh engine with `formulas`
/// formulas; returns the work the ops did. Asserts no rebuild ran.
fn measure(
    formulas: u32,
    n: u32,
    setup: &dyn Fn(&mut Engine<TestWorkbook>, u32),
    op: &dyn Fn(&mut Engine<TestWorkbook>, u32),
) -> u64 {
    let mut e = engine(formulas);
    setup(&mut e, n);
    e.graph.authority().unwrap();
    let (builds, before) = (e.graph.authority_host().builds(), work(&e));
    for i in 0..n {
        op(&mut e, i);
    }
    e.graph.authority().unwrap();
    assert_eq!(
        e.graph.authority_host().builds(),
        builds,
        "a symbol revision rebuilt the authority"
    );
    let done = work(&e) - before;
    e.graph.authority_host().store().check().unwrap();
    done
}

/// Linear in the number of ops and independent of the formula count: the
/// per-op work at 4N is within 1.5x of the per-op work at N (quadratic
/// work would be 4x), and adding 2000 unrelated formulas changes it by at
/// most 1.5x (O(F) per op would be orders of magnitude).
fn assert_linear(
    what: &str,
    setup: &dyn Fn(&mut Engine<TestWorkbook>, u32),
    op: &dyn Fn(&mut Engine<TestWorkbook>, u32),
) {
    let small = measure(0, 100, setup, op);
    let large = measure(0, 400, setup, op);
    let with_formulas = measure(2000, 100, setup, op);
    let per = |w: u64, n: u64| w as f64 / n as f64;
    assert!(small > 0, "{what}: no work counted");
    assert!(
        per(large, 400) <= 1.5 * per(small, 100),
        "{what}: per-op work grows with the symbol count: {small} for 100, {large} for 400"
    );
    assert!(
        per(with_formulas, 100) <= 1.5 * per(small, 100),
        "{what}: per-op work grows with the formula count: {small} without, {with_formulas} with 2000 formulas"
    );
}

fn no_setup(_: &mut Engine<TestWorkbook>, _: u32) {}

fn define_literal(e: &mut Engine<TestWorkbook>, i: u32) {
    e.define_name(
        &format!("Name_{i}"),
        NamedDefinition::Literal(LiteralValue::Number(f64::from(i))),
        NameScope::Workbook,
    )
    .unwrap();
}

#[test]
fn unused_literal_names_scale_linearly() {
    assert_linear("define", &no_setup, &define_literal);
}

#[test]
fn unused_name_redefinition_and_deletion_scale_linearly() {
    let setup = |e: &mut Engine<TestWorkbook>, n: u32| {
        for i in 0..n {
            define_literal(e, i);
        }
    };
    assert_linear("update", &setup, &|e, i| {
        e.update_name(
            &format!("Name_{i}"),
            NamedDefinition::Literal(LiteralValue::Number(-1.0)),
            NameScope::Workbook,
        )
        .unwrap();
    });
    assert_linear("delete", &setup, &|e, i| {
        e.delete_name(&format!("Name_{i}"), NameScope::Workbook)
            .unwrap();
    });
}

#[test]
fn unused_cell_and_formula_names_scale_linearly() {
    assert_linear("cell names", &no_setup, &|e, i| {
        let sid = e.sheet_id("Sheet1").unwrap();
        e.define_name(
            &format!("CellName_{i}"),
            NamedDefinition::Cell(CellRef::new_absolute(sid, 5000 + i, 3)),
            NameScope::Workbook,
        )
        .unwrap();
    });
    assert_linear("formula names", &no_setup, &|e, i| {
        e.define_name(
            &format!("FormulaName_{i}"),
            NamedDefinition::Formula {
                ast: parse(format!("=Sheet1!$D${}*2", 5000 + i)).unwrap(),
                dependencies: vec![],
                range_deps: vec![],
            },
            NameScope::Workbook,
        )
        .unwrap();
    });
}

#[test]
fn unused_sources_scale_linearly() {
    assert_linear("sources", &no_setup, &|e, i| {
        e.define_source_scalar(&format!("Src_{i}"), Some(1))
            .unwrap();
        e.define_source_table(&format!("SrcT_{i}"), Some(1))
            .unwrap();
    });
}

#[test]
fn unused_tables_scale_linearly() {
    assert_linear("tables", &no_setup, &|e, i| {
        let sid = e.sheet_id("Sheet1").unwrap();
        let r = 5000 + i * 3;
        e.define_table(
            &format!("Tbl_{i}"),
            RangeRef::new(
                CellRef::new_absolute(sid, r, 10),
                CellRef::new_absolute(sid, r + 1, 11),
            ),
            true,
            vec!["a".into(), "b".into()],
            false,
        )
        .unwrap();
    });
}

/// Readers still bind incrementally: formulas and names written before
/// their name, table or source re-bind when it appears, and a later change
/// reaches them.
#[test]
fn late_symbols_bind_their_waiting_readers_without_a_rebuild() {
    let mut e = engine(10);
    let sid = e.sheet_id("Sheet1").unwrap();
    e.set_cell_value("Sheet1", 1, 5, LiteralValue::Number(3.0))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 6, parse("=Later+1").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 2, 6, parse("=Outer*10").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 3, 6, parse("=SUM(LateTbl[a])").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let builds = e.graph.authority_host().builds();
    e.define_name(
        "Later",
        NamedDefinition::Cell(CellRef::new_absolute(sid, 0, 4)),
        NameScope::Workbook,
    )
    .unwrap();
    e.define_name(
        "Outer",
        NamedDefinition::Formula {
            ast: parse("=Later*2").unwrap(),
            dependencies: vec![],
            range_deps: vec![],
        },
        NameScope::Workbook,
    )
    .unwrap();
    e.set_cell_value("Sheet1", 10, 12, LiteralValue::Number(5.0))
        .unwrap();
    e.define_table(
        "LateTbl",
        RangeRef::new(
            CellRef::new_absolute(sid, 8, 11),
            CellRef::new_absolute(sid, 9, 11),
        ),
        true,
        vec!["a".into()],
        false,
    )
    .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 6),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 6),
        Some(LiteralValue::Number(60.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 3, 6),
        Some(LiteralValue::Number(5.0))
    );
    e.set_cell_value("Sheet1", 1, 5, LiteralValue::Number(7.0))
        .unwrap();
    e.set_cell_value("Sheet1", 10, 12, LiteralValue::Number(6.0))
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 6),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 6),
        Some(LiteralValue::Number(140.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 3, 6),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(e.graph.authority_host().builds(), builds);
    // Deleting and redefining `Later` re-binds `Outer`'s node.
    e.delete_name("Later", NameScope::Workbook).unwrap();
    e.define_name(
        "Later",
        NamedDefinition::Literal(LiteralValue::Number(1.0)),
        NameScope::Workbook,
    )
    .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 6),
        Some(LiteralValue::Number(20.0))
    );
    e.update_name(
        "Later",
        NamedDefinition::Literal(LiteralValue::Number(2.0)),
        NameScope::Workbook,
    )
    .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 6),
        Some(LiteralValue::Number(40.0))
    );
    assert_eq!(e.graph.authority_host().builds(), builds);
    e.graph.authority_host().store().check().unwrap();
}
