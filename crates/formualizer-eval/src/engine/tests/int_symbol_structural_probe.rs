//! Integration probe (ignored): named symbols meeting structural edits,
//! sheet operations and history. Prints every visible value after each
//! step so a default build (legacy) and a `unified_authority` build can be
//! diffed line by line: `cargo test ... int_symbol_structural_probe --
//! --ignored --nocapture --test-threads=1 | grep ^PROBE`.

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{ChangeLog, Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

type E = Engine<TestWorkbook>;

fn n(v: f64) -> LiteralValue {
    LiteralValue::Number(v)
}

fn setup() -> E {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = e.sheet_id_mut("Data");
    e.sheet_id_mut("Other");
    for r in 1..=12 {
        e.set_cell_value("Data", r, 2, n(r as f64 * 10.0)).unwrap();
    }
    e.set_cell_value("Data", 2, 4, n(40.0)).unwrap();
    let c = |r, col| CellRef::new(data, Coord::from_excel(r, col, true, true));
    e.define_name(
        "RG",
        NamedDefinition::Range(RangeRef::new(c(2, 2), c(5, 2))),
        NameScope::Workbook,
    )
    .unwrap();
    e.define_name("CL", NamedDefinition::Cell(c(5, 2)), NameScope::Workbook)
        .unwrap();
    let f = |s: &str| NamedDefinition::Formula {
        ast: parse(s).unwrap(),
        dependencies: Vec::new(),
        range_deps: Vec::new(),
    };
    e.define_name("TF", f("=SUM(Data!$B$2:$B$5)"), NameScope::Workbook)
        .unwrap();
    e.define_name("SF", f("=$D$2"), NameScope::Sheet(data))
        .unwrap();
    e.define_name("NN", f("=TF*2"), NameScope::Workbook)
        .unwrap();
    e.define_name("LT", NamedDefinition::Literal(n(3.0)), NameScope::Workbook)
        .unwrap();
    e.define_name("OC", f("=Other!$A$1+1"), NameScope::Workbook)
        .unwrap();
    for (r, text) in [
        (1, "=SUM(RG)"),
        (2, "=CL"),
        (3, "=TF"),
        (4, "=NN+LT"),
        (5, "=TF+Data!B3"),
        (6, "=OC*2"),
        (7, "=ROWS(RG)"),
    ] {
        e.set_cell_formula("Sheet1", r, 1, parse(text).unwrap())
            .unwrap();
    }
    e.set_cell_formula("Data", 1, 1, parse("=SF+1").unwrap())
        .unwrap();
    e.set_cell_formula("Other", 2, 1, parse("=SUM(RG)*LT").unwrap())
        .unwrap();
    e.set_cell_value("Other", 1, 1, n(5.0)).unwrap();
    // A table on its own sheet (Tbl!E1:F6, header row) read through
    // structured references. (Removing a sheet that holds a table panics
    // in legacy, eval.rs "Arrow sheet missing for table reference"; not
    // probed. A formula in a table body is core's pinned legacy-wrong Δ.)
    let tbl = e.sheet_id_mut("Tbl");
    let t = |r, col| CellRef::new(tbl, Coord::from_excel(r, col, true, true));
    e.set_cell_value("Tbl", 1, 5, LiteralValue::Text("Qty".into()))
        .unwrap();
    e.set_cell_value("Tbl", 1, 6, LiteralValue::Text("Amt".into()))
        .unwrap();
    for r in 2..=6 {
        e.set_cell_value("Tbl", r, 5, n(r as f64)).unwrap();
        e.set_cell_value("Tbl", r, 6, n(r as f64 * 2.0)).unwrap();
    }
    e.define_table(
        "Sales",
        RangeRef::new(t(1, 5), t(6, 6)),
        true,
        vec!["Qty".into(), "Amt".into()],
        false,
    )
    .unwrap();
    e.set_cell_formula("Sheet1", 8, 1, parse("=SUM(Sales[Amt])").unwrap())
        .unwrap();
    e.set_cell_formula("Other", 3, 1, parse("=SUM(Sales[Qty])+TF").unwrap())
        .unwrap();
    e
}

fn sheets(e: &E) -> Vec<String> {
    let mut v: Vec<String> = ["Sheet1", "Data", "Other", "Facts", "Data2", "Later", "Tbl"]
        .iter()
        .filter(|s| e.sheet_id(s).is_some())
        .map(|s| s.to_string())
        .collect();
    v.sort();
    v
}

fn dump(e: &mut E, scen: &str, step: &str) {
    let r = e.evaluate_all();
    if let Err(err) = r {
        println!("PROBE {scen}\t{step}\tEVAL_ERR {err:?}");
    }
    for s in sheets(e) {
        for r in 1..=14 {
            for c in 1..=8 {
                if let Some(v) = e.get_cell_value(&s, r, c) {
                    println!("PROBE {scen}\t{step}\t{s}!{r},{c}\t{v:?}");
                }
            }
        }
    }
}

/// Rewrite every Data-like value so moved targets see new inputs.
fn bump(e: &mut E, sheet: &str, k: f64) {
    if e.sheet_id(sheet).is_none() {
        return;
    }
    for r in 1..=14 {
        for c in [2u32, 3, 4, 5, 6, 7] {
            if let Some(LiteralValue::Number(_)) = e.get_cell_value(sheet, r, c)
                && e.get_cell(sheet, r, c).is_some_and(|(a, _)| a.is_none())
            {
                e.set_cell_value(sheet, r, c, n(k * 1000.0 + (r * 10 + c) as f64))
                    .unwrap();
            }
        }
    }
    if e.sheet_id("Other").is_some() {
        e.set_cell_value("Other", 1, 1, n(k)).unwrap();
    }
}

fn run(scen: &str, op: impl FnOnce(&mut E) -> String, target: &str) {
    let mut e = setup();
    dump(&mut e, scen, "0-load");
    let res = op(&mut e);
    println!("PROBE {scen}\top\t{res}");
    dump(&mut e, scen, "1-op");
    bump(&mut e, target, 7.0);
    dump(&mut e, scen, "2-bump");
    bump(&mut e, target, 9.0);
    dump(&mut e, scen, "3-bump");
}

fn sid(e: &E, s: &str) -> u16 {
    e.sheet_id(s).unwrap()
}

#[test]
#[ignore = "integration probe; diff default vs unified_authority output"]
fn int_symbol_structural_probe() {
    run(
        "ins_rows_above",
        |e| format!("{:?}", e.insert_rows("Data", 1, 2).is_ok()),
        "Data",
    );
    run(
        "ins_rows_inside",
        |e| format!("{:?}", e.insert_rows("Data", 3, 1).is_ok()),
        "Data",
    );
    run(
        "ins_rows_table",
        |e| format!("{:?}", e.insert_rows("Tbl", 4, 2).is_ok()),
        "Tbl",
    );
    run(
        "ins_rows_table_above",
        |e| format!("{:?}", e.insert_rows("Tbl", 1, 2).is_ok()),
        "Tbl",
    );
    run(
        "del_rows_table",
        |e| format!("{:?}", e.delete_rows("Tbl", 3, 1).is_ok()),
        "Tbl",
    );
    run(
        "ins_cols_table",
        |e| format!("{:?}", e.insert_columns("Tbl", 6, 1).is_ok()),
        "Tbl",
    );
    run(
        "del_cols_table_before",
        |e| format!("{:?}", e.delete_columns("Tbl", 2, 1).is_ok()),
        "Tbl",
    );
    run(
        "rename_tbl",
        |e| {
            let d = sid(e, "Tbl");
            format!("{:?}", e.rename_sheet(d, "Facts"))
        },
        "Facts",
    );
    run(
        "ins_rows_below",
        |e| format!("{:?}", e.insert_rows("Data", 8, 1).is_ok()),
        "Data",
    );
    run(
        "del_rows_inside",
        |e| format!("{:?}", e.delete_rows("Data", 3, 2).is_ok()),
        "Data",
    );
    run(
        "del_rows_above",
        |e| format!("{:?}", e.delete_rows("Data", 1, 1).is_ok()),
        "Data",
    );
    run(
        "del_rows_whole",
        |e| format!("{:?}", e.delete_rows("Data", 2, 4).is_ok()),
        "Data",
    );
    run(
        "ins_cols_before",
        |e| format!("{:?}", e.insert_columns("Data", 1, 2).is_ok()),
        "Data",
    );
    run(
        "del_col_target",
        |e| format!("{:?}", e.delete_columns("Data", 2, 1).is_ok()),
        "Data",
    );
    run(
        "del_col_d",
        |e| format!("{:?}", e.delete_columns("Data", 4, 1).is_ok()),
        "Data",
    );
    run(
        "ins_rows_reader",
        |e| format!("{:?}", e.insert_rows("Sheet1", 1, 3).is_ok()),
        "Data",
    );
    run(
        "ins_rows_other",
        |e| format!("{:?}", e.insert_rows("Other", 1, 1).is_ok()),
        "Other",
    );
    run(
        "rename_data",
        |e| {
            let d = sid(e, "Data");
            format!("{:?}", e.rename_sheet(d, "Facts"))
        },
        "Facts",
    );
    run(
        "rename_back",
        |e| {
            let d = sid(e, "Data");
            let a = e.rename_sheet(d, "Facts");
            let b = e.rename_sheet(d, "Data");
            format!("{a:?} {b:?}")
        },
        "Data",
    );
    run(
        "remove_data",
        |e| {
            let d = sid(e, "Data");
            format!("{:?}", e.remove_sheet(d))
        },
        "Data",
    );
    run(
        "remove_other",
        |e| {
            let d = sid(e, "Other");
            format!("{:?}", e.remove_sheet(d))
        },
        "Other",
    );
    run(
        "remove_readd_data",
        |e| {
            let d = sid(e, "Data");
            let a = e.remove_sheet(d);
            let b = e.add_sheet("Data");
            for r in 1..=12 {
                e.set_cell_value("Data", r, 2, n(r as f64)).unwrap();
            }
            e.set_cell_value("Data", 2, 4, n(4.0)).unwrap();
            format!("{a:?} {}", b.is_ok())
        },
        "Data",
    );
    run(
        "remove_readd_other",
        |e| {
            let d = sid(e, "Other");
            let a = e.remove_sheet(d);
            let b = e.add_sheet("Other");
            e.set_cell_value("Other", 1, 1, n(11.0)).unwrap();
            format!("{a:?} {}", b.is_ok())
        },
        "Other",
    );
    run(
        "dup_data",
        |e| format!("{:?}", e.duplicate_sheet("Data", "Data2").is_ok()),
        "Data",
    );
    run(
        "dup_data_edit_copy",
        |e| format!("{:?}", e.duplicate_sheet("Data", "Data2").is_ok()),
        "Data2",
    );
    run(
        "move_range_target",
        |e| {
            let d = sid(e, "Data");
            let r = e.edit_with_logger(&mut ChangeLog::new(), |ed| {
                ed.move_range(d, 1, 1, 5, 3, d, 7, 5)
            });
            format!("{}", matches!(r, Ok(Ok(_))))
        },
        "Data",
    );
    run(
        "move_range_reader",
        |e| {
            let s = sid(e, "Sheet1");
            let r = e.edit_with_logger(&mut ChangeLog::new(), |ed| {
                ed.move_range(s, 0, 0, 7, 0, s, 0, 3)
            });
            format!("{}", matches!(r, Ok(Ok(_))))
        },
        "Data",
    );
    run(
        "journal_insert_undo",
        |e| {
            let mut u = UndoEngine::new();
            let (_, j) = e
                .action_atomic_journal("ins".to_string(), |tx| {
                    tx.insert_rows("Data", 1, 2)?;
                    Ok(())
                })
                .unwrap();
            u.push_action(j);
            let _ = e.evaluate_all();
            let a = e.undo_action(&mut u);
            format!("{a:?}")
        },
        "Data",
    );
    run(
        "journal_insert_undo_redo",
        |e| {
            let mut u = UndoEngine::new();
            let (_, j) = e
                .action_atomic_journal("ins".to_string(), |tx| {
                    tx.insert_rows("Data", 3, 2)?;
                    Ok(())
                })
                .unwrap();
            u.push_action(j);
            let _ = e.evaluate_all();
            let a = e.undo_action(&mut u);
            let _ = e.evaluate_all();
            let b = e.redo_action(&mut u);
            format!("{a:?} {b:?}")
        },
        "Data",
    );
    run(
        "logged_delete_undo",
        |e| {
            let mut u = UndoEngine::new();
            let mut log = ChangeLog::new();
            let d = sid(e, "Data");
            let r = e.edit_with_logger(&mut log, |ed| ed.delete_rows(d, 2, 2));
            let _ = e.evaluate_all();
            let a = e.undo_logged(&mut u, &mut log);
            format!("{} {a:?}", r.is_ok())
        },
        "Data",
    );
    run(
        "redefine_after_insert",
        |e| {
            let a = e.insert_rows("Data", 1, 1).is_ok();
            let _ = e.evaluate_all();
            let b = e.update_name(
                "TF",
                NamedDefinition::Formula {
                    ast: parse("=Data!$B$1*100").unwrap(),
                    dependencies: Vec::new(),
                    range_deps: Vec::new(),
                },
                NameScope::Workbook,
            );
            format!("{a} {b:?}")
        },
        "Data",
    );
    run(
        "delete_name_after_remove",
        |e| {
            let d = sid(e, "Data");
            let a = e.remove_sheet(d);
            let _ = e.evaluate_all();
            let b = e.delete_name("NN", NameScope::Workbook);
            format!("{a:?} {b:?}")
        },
        "Other",
    );
    run(
        "late_sheet_name",
        |e| {
            let a = e.define_name(
                "LTR",
                NamedDefinition::Formula {
                    ast: parse("=Later!$A$1*2").unwrap(),
                    dependencies: Vec::new(),
                    range_deps: Vec::new(),
                },
                NameScope::Workbook,
            );
            let b = e.set_cell_formula("Sheet1", 9, 1, parse("=LTR+1").unwrap());
            let _ = e.evaluate_all();
            let c = e.add_sheet("Later").is_ok();
            let d = e.set_cell_value("Later", 1, 1, n(21.0));
            let _ = e.evaluate_all();
            let f = e.insert_rows("Later", 1, 1).is_ok();
            let g = e.set_cell_value("Later", 2, 1, n(33.0));
            format!("{a:?} {b:?} {c} {d:?} {f} {g:?}")
        },
        "Later",
    );
}

/// A duplicated sheet's copied readers of a sheet-scoped name read the
/// duplicated name, which follows the copy's own cells (legacy values; the
/// authority must give the name its symbol node before readers bind).
#[test]
fn duplicated_sheet_scoped_name_feeds_copied_readers() {
    let mut e = setup();
    e.evaluate_all().unwrap();
    e.duplicate_sheet("Data", "Data2").unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Data2", 1, 1), Some(n(41.0)));
    e.set_cell_value("Data2", 2, 4, n(70.0)).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Data2", 1, 1), Some(n(71.0)));
    assert_eq!(e.get_cell_value("Data", 1, 1), Some(n(41.0)));
    e.set_cell_value("Data", 2, 4, n(90.0)).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Data", 1, 1), Some(n(91.0)));
    assert_eq!(e.get_cell_value("Data2", 1, 1), Some(n(71.0)));
}

/// Regressions found by the probe at M5 (legacy values; the probe diff is
/// the oracle): a table at the top of its sheet does not straddle a row
/// insertion at row 1, and moving the cells of a table (deleting a column
/// before it) flags only legacy's direct in-edge readers, so a structured
/// reference reader keeps its value; a name formula that spells a renamed
/// sheet's old name re-evaluates to #REF! when that sheet's cells change.
#[test]
fn m5_table_shift_and_renamed_sheet_name_keep_legacy_values() {
    let mut e = setup();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Sheet1", 8, 1), Some(n(40.0)));
    e.insert_rows("Tbl", 1, 2).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Sheet1", 8, 1), Some(n(40.0)));

    let mut e = setup();
    e.evaluate_all().unwrap();
    e.delete_columns("Tbl", 2, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Sheet1", 8, 1), Some(n(40.0)));

    let mut e = setup();
    e.evaluate_all().unwrap();
    let d = sid(&e, "Data");
    e.rename_sheet(d, "Facts").unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Sheet1", 3, 1), Some(n(140.0)));
    e.set_cell_value("Facts", 3, 2, n(1.0)).unwrap();
    e.evaluate_all().unwrap();
    assert!(matches!(
        e.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Error(ref err)) if err.kind == formualizer_common::ExcelErrorKind::Ref
    ));
}
