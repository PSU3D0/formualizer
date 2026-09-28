//! Program 2 compression (P2-M2): family members leave the graph's
//! per-cell maps (cell map, formula map, sheet index) for virtual member
//! runs. The engine must be observably identical to one that keeps every
//! member materialized (`formula_compression = false`): values, formulas,
//! vertex ids, the change log, and undo/redo, through value and formula
//! edits on members, structural edits and their undo/redo.
use super::common::arrow_eval_config;
use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{ChangeLog, Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const ROWS: u32 = 120;

fn engine(compress: bool, parallel: bool) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        formula_compression: compress,
        enable_parallel: parallel,
        ..arrow_eval_config()
    };
    let mut e = Engine::new(TestWorkbook::new(), config);
    for r in 1..=ROWS {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(f64::from(r % 7)))
            .unwrap();
    }
    // Column by column, so each column's vertices are one id run.
    type Column = (u32, fn(u32) -> String);
    let cols: [Column; 5] = [
        (3, |r| format!("=A{r}*2+B{r}")),
        (4, |r| {
            if r == 1 {
                "=C1".to_string()
            } else {
                format!("=D{}+C{r}", r - 1)
            }
        }),
        (5, |r| format!("=SUM($A$1:A{r})")),
        (6, |r| format!("=IF(B{r}>3,C{r},-C{r})")),
        (7, |r| format!("=Sheet2!A{r}+E{r}")),
    ];
    for (c, f) in cols {
        for r in 1..=ROWS {
            e.set_cell_formula("Sheet1", r, c, parse(f(r)).unwrap())
                .unwrap();
        }
    }
    for r in 1..=ROWS {
        e.set_cell_value("Sheet2", r, 1, LiteralValue::Number(f64::from(r) * 0.5))
            .unwrap();
    }
    e
}

/// Bit-level key of a value.
fn key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        Some(LiteralValue::Error(e)) => format!("E{:?}", e.kind),
        other => format!("{other:?}"),
    }
}

/// Every cell of the used block: value, formula text and vertex id.
fn state(e: &Engine<TestWorkbook>) -> Vec<String> {
    let mut out = Vec::new();
    for sheet in ["Sheet1", "Sheet2"] {
        let Some(sid) = e.graph.sheet_id(sheet) else {
            continue;
        };
        for r in 1..=ROWS + 12 {
            for c in 1..=10 {
                let v = key(e.get_cell_value(sheet, r, c));
                let cell = CellRef::new(sid, Coord::from_excel(r, c, true, true));
                let vid = e.graph.get_vertex_id_for_address(&cell);
                let f = vid
                    .and_then(|v| e.graph.get_formula(v))
                    .map(|a| a.to_string());
                out.push(format!("{sheet}!{r},{c}: {v} {f:?} {vid:?}"));
            }
        }
    }
    out
}

fn assert_same(a: &Engine<TestWorkbook>, b: &Engine<TestWorkbook>, ctx: &str) {
    let (sa, sb) = (state(a), state(b));
    for (x, y) in sa.iter().zip(&sb) {
        assert_eq!(x, y, "{ctx}");
    }
    assert_eq!(sa.len(), sb.len(), "{ctx}");
}

fn virtual_count(e: &Engine<TestWorkbook>) -> usize {
    e.graph.virtual_member_counts().0
}

type Step = fn(&mut Engine<TestWorkbook>, &mut ChangeLog);

#[test]
fn virtual_members_match_materialized_engine_through_edits_and_history() {
    let steps: Vec<(&str, Step)> = vec![
        ("value edit", |e, _| {
            e.set_cell_value("Sheet1", 50, 1, LiteralValue::Number(-3.25))
                .unwrap();
        }),
        ("formula edit on a member", |e, _| {
            e.set_cell_formula("Sheet1", 60, 3, parse("=A60*10").unwrap())
                .unwrap();
        }),
        ("value over a member", |e, _| {
            e.set_cell_value("Sheet1", 70, 4, LiteralValue::Number(1000.0))
                .unwrap();
        }),
        ("logged insert rows", |e, log| {
            let s1 = e.graph.sheet_id("Sheet1").unwrap();
            e.edit_with_logger(log, |ed| ed.insert_rows(s1, 30, 3).map(|_| ()))
                .unwrap()
                .unwrap();
        }),
        ("logged delete rows", |e, log| {
            let s1 = e.graph.sheet_id("Sheet1").unwrap();
            e.edit_with_logger(log, |ed| ed.delete_rows(s1, 10, 2).map(|_| ()))
                .unwrap()
                .unwrap();
        }),
        ("logged member formula edit", |e, log| {
            let s1 = e.graph.sheet_id("Sheet1").unwrap();
            let cell = e.graph.make_cell_ref_internal(s1, 80, 5);
            log.begin_compound("member formula".into());
            e.edit_with_logger(log, |ed| {
                ed.set_cell_formula(cell, parse("=A81+1").unwrap())
            })
            .unwrap();
            log.end_compound();
        }),
        ("logged value over member", |e, log| {
            let s1 = e.graph.sheet_id("Sheet1").unwrap();
            let cell = e.graph.make_cell_ref_internal(s1, 90, 6);
            log.begin_compound("member value".into());
            e.edit_with_logger(log, |ed| ed.set_cell_value(cell, LiteralValue::Number(7.0)))
                .unwrap();
            log.end_compound();
        }),
        ("logged insert columns", |e, log| {
            let s1 = e.graph.sheet_id("Sheet1").unwrap();
            e.edit_with_logger(log, |ed| ed.insert_columns(s1, 3, 1).map(|_| ()))
                .unwrap()
                .unwrap();
        }),
        ("logged delete columns", |e, log| {
            let s1 = e.graph.sheet_id("Sheet1").unwrap();
            e.edit_with_logger(log, |ed| ed.delete_columns(s1, 3, 1).map(|_| ()))
                .unwrap()
                .unwrap();
        }),
    ];
    for parallel in [false, true] {
        let mut a = engine(true, parallel);
        let mut b = engine(false, parallel);
        a.evaluate_all().unwrap();
        b.evaluate_all().unwrap();
        assert!(
            virtual_count(&a) > 4 * ROWS as usize,
            "members must go virtual (got {})",
            virtual_count(&a)
        );
        assert_eq!(virtual_count(&b), 0);
        assert_same(&a, &b, "first eval");
        let (mut la, mut lb) = (ChangeLog::new(), ChangeLog::new());
        let mut n_logged = 0;
        for (name, step) in &steps {
            step(&mut a, &mut la);
            step(&mut b, &mut lb);
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, name);
            assert_eq!(la.events(), lb.events(), "{name}: change log");
            // After the rebuild, members are virtual again.
            assert!(virtual_count(&a) > 0, "{name}: nothing virtual");
            if name.starts_with("logged") {
                n_logged += 1;
            }
        }
        // Undo every logged step, then redo them, comparing each state.
        let (mut ua, mut ub) = (UndoEngine::new(), UndoEngine::new());
        for i in 0..n_logged {
            a.undo_logged(&mut ua, &mut la).unwrap();
            b.undo_logged(&mut ub, &mut lb).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("undo {i}"));
        }
        for i in 0..n_logged {
            a.redo_logged(&mut ua, &mut la).unwrap();
            b.redo_logged(&mut ub, &mut lb).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("redo {i}"));
        }
        assert_eq!(la.events(), lb.events(), "change log after history");
    }
}

#[test]
fn virtual_members_through_action_journal_undo_redo() {
    for parallel in [false, true] {
        let mut a = engine(true, parallel);
        let mut b = engine(false, parallel);
        a.evaluate_all().unwrap();
        b.evaluate_all().unwrap();
        assert!(virtual_count(&a) > 0);
        let (mut ua, mut ub) = (UndoEngine::new(), UndoEngine::new());
        type Act = fn(
            &mut crate::engine::EngineAction<'_, TestWorkbook>,
        ) -> Result<(), crate::engine::EditorError>;
        let acts: Vec<Act> = vec![
            |tx| tx.insert_rows("Sheet1", 40, 2).map(|_| ()),
            |tx| tx.set_cell_value("Sheet1", 45, 3, LiteralValue::Number(2.5)),
            |tx| tx.set_cell_formula("Sheet1", 47, 4, parse("=C47*3").unwrap()),
            |tx| tx.insert_columns("Sheet1", 2, 1).map(|_| ()),
        ];
        let mut states = vec![state(&a)];
        for (i, act) in acts.iter().enumerate() {
            let (_, ja) = a.action_atomic_journal(format!("a{i}"), act).unwrap();
            let (_, jb) = b.action_atomic_journal(format!("a{i}"), act).unwrap();
            ua.push_action(ja);
            ub.push_action(jb);
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("action {i}"));
            states.push(state(&a));
        }
        for i in (0..acts.len()).rev() {
            a.undo_action(&mut ua).unwrap();
            b.undo_action(&mut ub).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("undo action {i}"));
            assert_eq!(state(&a), states[i], "undo action {i} restores the state");
        }
        for i in 0..acts.len() {
            a.redo_action(&mut ua).unwrap();
            b.redo_action(&mut ub).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("redo action {i}"));
        }
    }
}

/// A virtual member keeps its vertex: lookups by cell and by vertex, the
/// formula view and the snapshot agree with the materialized engine, and
/// materializing one member splits its run without touching its neighbours.
#[test]
fn virtual_member_lookups_and_single_cell_materialization() {
    let mut a = engine(true, false);
    let mut b = engine(false, false);
    a.evaluate_all().unwrap();
    b.evaluate_all().unwrap();
    let (members, runs) = a.graph.virtual_member_counts();
    assert!(
        members > 0 && runs > 0 && runs * 10 < members,
        "{members} in {runs}"
    );
    let s1 = a.graph.sheet_id("Sheet1").unwrap();
    for r in [2u32, 55, ROWS] {
        let cell = CellRef::new(s1, Coord::from_excel(r, 3, true, true));
        let va = a.graph.get_vertex_for_cell(&cell).unwrap();
        let vb = b.graph.get_vertex_for_cell(&cell).unwrap();
        assert_eq!(va, vb);
        assert_eq!(a.graph.get_formula(va), b.graph.get_formula(vb));
        // The snapshot's `formula_ref` is the stored arena root, which
        // compression changes (the shared template): compare the rest.
        let (sa, sb) = (a.graph.snapshot_vertex(va), b.graph.snapshot_vertex(vb));
        assert_eq!(
            (
                sa.coord,
                sa.sheet_id,
                sa.kind,
                sa.flags,
                sa.value_ref,
                sa.out_edges
            ),
            (
                sb.coord,
                sb.sheet_id,
                sb.kind,
                sb.flags,
                sb.value_ref,
                sb.out_edges
            )
        );
        assert_eq!(a.graph.get_cell_ref_for_vertex(va), Some(cell));
    }
    assert_eq!(
        a.graph.vertices_with_formulas().count(),
        b.graph.vertices_with_formulas().count()
    );
    let before = virtual_count(&a);
    a.set_cell_formula("Sheet1", 55, 3, parse("=A55*4").unwrap())
        .unwrap();
    b.set_cell_formula("Sheet1", 55, 3, parse("=A55*4").unwrap())
        .unwrap();
    assert_eq!(virtual_count(&a), before - 1);
    a.evaluate_all().unwrap();
    b.evaluate_all().unwrap();
    assert_same(&a, &b, "after one member edit");
}

/// Families long enough to fill whole vertex pages (1024 vertices): the
/// pages keep no rows while every vertex in them is a virtual member, and
/// come back when a member is edited or a structural edit materializes the
/// members. The engine stays identical to the uncompressed one.
#[test]
fn member_vertex_pages_drop_rows_and_come_back() {
    const N: u32 = 3000;
    let build = |compress: bool| {
        let config = EvalConfig {
            formula_compression: compress,
            ..arrow_eval_config()
        };
        let mut e = Engine::new(TestWorkbook::new(), config);
        for r in 1..=N {
            e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r % 13)))
                .unwrap();
        }
        for r in 1..=N {
            e.set_cell_formula("Sheet1", r, 2, parse(format!("=A{r}*3-1")).unwrap())
                .unwrap();
        }
        for r in 1..=N {
            e.set_cell_formula("Sheet1", r, 3, parse(format!("=B{r}+A{r}")).unwrap())
                .unwrap();
        }
        e.evaluate_all().unwrap();
        e
    };
    let values = |e: &Engine<TestWorkbook>| -> Vec<String> {
        (1..=N + 4)
            .flat_map(|r| (1..=4).map(move |c| (r, c)))
            .map(|(r, c)| {
                let s1 = e.graph.sheet_id("Sheet1").unwrap();
                let cell = CellRef::new(s1, Coord::from_excel(r, c, true, true));
                let vid = e.graph.get_vertex_id_for_address(&cell);
                format!(
                    "{r},{c}: {} {:?} {vid:?}",
                    key(e.get_cell_value("Sheet1", r, c)),
                    vid.and_then(|v| e.graph.get_formula(v))
                        .map(|a| a.to_string())
                )
            })
            .collect()
    };
    let mut a = build(true);
    let mut b = build(false);
    // Three full 1024-id pages lie inside the member runs (value cells take
    // no ids since decision 27, so the runs start at the first id).
    assert!(
        a.graph.virtual_vertex_pages() >= 3,
        "full member pages keep no rows"
    );
    assert_eq!(b.graph.virtual_vertex_pages(), 0);
    assert_eq!(values(&a), values(&b));
    // A member edit in the middle of a dropped page.
    for e in [&mut a, &mut b] {
        e.set_cell_formula("Sheet1", 1500, 2, parse("=A1500*100").unwrap())
            .unwrap();
        e.set_cell_value("Sheet1", 2200, 3, LiteralValue::Number(-7.0))
            .unwrap();
        e.set_cell_value("Sheet1", 17, 1, LiteralValue::Number(99.0))
            .unwrap();
        e.evaluate_all().unwrap();
    }
    assert_eq!(values(&a), values(&b), "after member edits");
    // Structural edits (everything materializes, then goes virtual again)
    // and their undo.
    let mut la = ChangeLog::new();
    let mut lb = ChangeLog::new();
    for (e, log) in [(&mut a, &mut la), (&mut b, &mut lb)] {
        let s1 = e.graph.sheet_id("Sheet1").unwrap();
        e.edit_with_logger(log, |ed| ed.insert_rows(s1, 1000, 2).map(|_| ()))
            .unwrap()
            .unwrap();
        e.evaluate_all().unwrap();
    }
    assert_eq!(values(&a), values(&b), "after insert rows");
    assert_eq!(la.events(), lb.events(), "insert rows change log");
    assert!(a.graph.virtual_vertex_pages() > 0, "pages dropped again");
    let (mut ua, mut ub) = (UndoEngine::new(), UndoEngine::new());
    a.undo_logged(&mut ua, &mut la).unwrap();
    b.undo_logged(&mut ub, &mut lb).unwrap();
    a.evaluate_all().unwrap();
    b.evaluate_all().unwrap();
    assert_eq!(values(&a), values(&b), "after undo");
}

/// Decision 26: a deferred first build (`defer_graph_building`, staged
/// formula texts) goes through the eager first load's machinery: family
/// members are grouped (never interned) and installed as virtual runs by
/// the build itself, and each column's formulas are one id run even past
/// the builder's 10k-record chunks. Values equal an engine that keeps every
/// formula on its own.
#[test]
fn deferred_first_build_groups_preallocates_and_virtualizes() {
    const N: u32 = 11_000;
    for parallel in [false, true] {
        let deferred_config = EvalConfig {
            defer_graph_building: true,
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let mut deferred = Engine::new(TestWorkbook::new(), deferred_config);
        let mut plain = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                formula_compression: false,
                enable_parallel: parallel,
                ..arrow_eval_config()
            },
        );
        for e in [&mut deferred, &mut plain] {
            for r in 1..=N {
                e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r % 13)))
                    .unwrap();
            }
        }
        // Staged row by row (source order), two families.
        for r in 1..=N {
            let (b, c) = (
                format!("=A{r}*2+1"),
                if r == 1 {
                    "=B1".to_string()
                } else {
                    format!("=C{}+B{r}", r - 1)
                },
            );
            deferred.stage_formula_text("Sheet1", r, 2, b.clone());
            deferred.stage_formula_text("Sheet1", r, 3, c.clone());
            plain
                .set_cell_formula("Sheet1", r, 2, parse(&b).unwrap())
                .unwrap();
            plain
                .set_cell_formula("Sheet1", r, 3, parse(&c).unwrap())
                .unwrap();
        }
        let arena_before = deferred.graph.data_store().memory_usage().total_ast_nodes;
        deferred.build_graph_all().unwrap();
        // Members were never interned: a handful of templates.
        let arena_added = deferred.graph.data_store().memory_usage().total_ast_nodes - arena_before;
        assert!(arena_added < 64, "arena grew by {arena_added} nodes");
        // Virtual at build (not only after the first evaluation's
        // compression): every member but each column's anchors.
        assert!(
            virtual_count(&deferred) >= 2 * (N as usize) - 4,
            "virtual members after the build: {}",
            virtual_count(&deferred)
        );
        let sid = deferred.graph.sheet_id("Sheet1").unwrap();
        for col in [2, 3] {
            let ids: Vec<u32> = (1..=N)
                .map(|r| {
                    let cell = CellRef::new(sid, Coord::from_excel(r, col, true, true));
                    deferred.graph.get_vertex_id_for_address(&cell).unwrap().0
                })
                .collect();
            assert!(
                ids.windows(2).all(|w| w[1] == w[0] + 1),
                "column {col}: one id run"
            );
        }
        deferred.evaluate_all().unwrap();
        plain.evaluate_all().unwrap();
        for r in (1..=N).step_by(97).chain([N]) {
            for c in 1..=3 {
                assert_eq!(
                    key(deferred.get_cell_value("Sheet1", r, c)),
                    key(plain.get_cell_value("Sheet1", r, c)),
                    "R{r}C{c}"
                );
            }
        }
        deferred
            .set_cell_value("Sheet1", 7, 1, LiteralValue::Number(-2.5))
            .unwrap();
        plain
            .set_cell_value("Sheet1", 7, 1, LiteralValue::Number(-2.5))
            .unwrap();
        deferred.evaluate_all().unwrap();
        plain.evaluate_all().unwrap();
        for r in (1..=N).step_by(89).chain([N]) {
            assert_eq!(
                key(deferred.get_cell_value("Sheet1", r, 3)),
                key(plain.get_cell_value("Sheet1", r, 3)),
                "after edit R{r}"
            );
        }
    }
}

/// A deferred first build whose planning fails part-way (a formula after
/// the builder's first 10k-record chunk) leaves the graph without formulas
/// and the staged formulas in place, as the incremental path does.
#[test]
fn failed_deferred_first_build_leaves_the_graph_untouched() {
    const N: u32 = 11_000;
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            defer_graph_building: true,
            ..arrow_eval_config()
        },
    );
    for r in 1..=N {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.stage_formula_text("Sheet1", r, 2, format!("=A{r}*2"));
    }
    e.stage_formula_text("Sheet1", N, 3, "=SUM([1]Book!A1:B2)".to_string());
    let vertices = e.graph.vertex_len();
    let staged = e.staged_formula_count();
    let err = e.build_graph_all().unwrap_err();
    assert!(err.to_string().contains("Undefined table"), "{err}");
    assert_eq!(e.graph.formula_vertex_count(), 0);
    assert_eq!(e.graph.vertex_len(), vertices);
    assert_eq!(e.staged_formula_count(), staged);
    assert!(e.evaluate_all().is_err());
    assert_eq!(e.graph.formula_vertex_count(), 0);
}

/// Undoing a value typed into an empty cell dirties the cell's readers
/// (the cell has no vertex to remove: decision 27). Mirrors the Python
/// binding's `test_rewrite_previously_empty_precedent_after_undo`.
#[test]
fn undo_of_a_value_in_an_empty_cell_dirties_its_readers() {
    let mut e = Engine::new(TestWorkbook::new(), arrow_eval_config());
    e.set_cell_formula("S1", 4, 4, parse("=C6+1").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S1", 4, 4),
        Some(LiteralValue::Number(1.0))
    );
    let sid = e.graph.sheet_id("S1").unwrap();
    let c6 = CellRef::new(sid, Coord::from_excel(6, 3, true, true));
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();
    e.edit_with_logger(&mut log, |ed| {
        ed.set_cell_value(c6, LiteralValue::Number(10.0));
    })
    .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S1", 4, 4),
        Some(LiteralValue::Number(11.0))
    );
    e.undo_logged(&mut undo, &mut log).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("S1", 4, 4),
        Some(LiteralValue::Number(1.0))
    );
}
