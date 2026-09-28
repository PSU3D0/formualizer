//! Cells a formula only references (and value cells) have no vertex
//! (decision 27), but the legacy graph's used extent counted them: an open
//! range over a column that has no value and no formula resolves to that
//! extent (evaluation-compat policy), so results over it depended on the
//! referenced cells. The graph keeps them in an extent record; these results
//! are the ones the legacy graph (main `19f30912`) gives.
use formualizer_common::RangeAddress;
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig, traits::NamedRangeScope};

const READERS: [&str; 8] = [
    "COUNTBLANK(Data!A:A)",
    "SUMPRODUCT(--(Data!A1:A=\"\"))",
    "ROWS(Data!A:A*1)",
    "ROWS(TOCOL(Data!A:A))",
    "ROWS(FILTER(Data!A:A,Data!A:A=\"\"))",
    "ROWS(Data!Z:Z*1)",
    "COLUMNS(Data!5000:5000*1)",
    "Data!A1:A",
];

fn workbook(interactive: bool) -> Workbook {
    let config = if interactive {
        WorkbookConfig::interactive()
    } else {
        WorkbookConfig::ephemeral()
    };
    let mut wb = Workbook::new_with_config(config);
    for sheet in ["Data", "Refs", "Out"] {
        wb.add_sheet(sheet).unwrap();
    }
    for r in 1..=10u32 {
        wb.set_value("Data", r, 3, LiteralValue::Number(f64::from(r)))
            .unwrap();
    }
    wb
}

fn read(wb: &mut Workbook) -> Vec<LiteralValue> {
    for (i, f) in READERS.iter().enumerate() {
        wb.set_formula("Out", 1, i as u32 * 2 + 1, f).unwrap();
    }
    wb.evaluate_all().unwrap();
    (0..READERS.len())
        .map(|i| {
            wb.get_value("Out", 1, i as u32 * 2 + 1)
                .unwrap_or(LiteralValue::Empty)
        })
        .collect()
}

fn n(v: f64) -> LiteralValue {
    LiteralValue::Number(v)
}

fn check(label: &str, setup: impl Fn(&mut Workbook), expected: [LiteralValue; 8]) {
    for interactive in [false, true] {
        let mut wb = workbook(interactive);
        setup(&mut wb);
        let got = read(&mut wb);
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(
                &got[i], want,
                "{label} (interactive {interactive}): {}",
                READERS[i]
            );
        }
    }
}

#[test]
fn dangling_references_bound_open_ranges_over_empty_columns() {
    // `Data!A1:A` spills its 5000 blanks (anchor 0), as the legacy graph's
    // placeholder at A5000 bounded it; without it the spill is 1,048,576
    // rows and fails.
    check(
        "cell references",
        |wb| {
            wb.set_formula("Refs", 1, 1, "Data!A5000").unwrap();
            wb.set_formula("Refs", 2, 1, "Data!Z7000").unwrap();
        },
        [
            n(1_048_576.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(7000.0),
            n(1.0),
            n(0.0),
        ],
    );
    // Every cell of a small range was a placeholder.
    check(
        "small range",
        |wb| {
            wb.set_formula("Refs", 1, 1, "SUM(Data!A4990:B5000)")
                .unwrap();
        },
        [
            n(1_048_576.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(1_048_576.0),
            n(2.0),
            n(0.0),
        ],
    );
    // So was the cell of a defined name.
    check(
        "defined name",
        |wb| {
            let addr = RangeAddress::new("Data", 5000, 1, 5000, 1).unwrap();
            wb.define_named_range("Far", &addr, NamedRangeScope::Workbook)
                .unwrap();
            wb.set_formula("Refs", 1, 1, "Far+0").unwrap();
        },
        [
            n(1_048_576.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(1_048_576.0),
            n(1.0),
            n(0.0),
        ],
    );
    // A placeholder outlived the formula that made it.
    check(
        "reference removed",
        |wb| {
            wb.set_formula("Refs", 1, 1, "Data!A5000").unwrap();
            wb.evaluate_all().unwrap();
            wb.set_value("Refs", 1, 1, n(1.0)).unwrap();
        },
        [
            n(1_048_576.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(1_048_576.0),
            n(1.0),
            n(0.0),
        ],
    );
    // An emptied value cell kept its vertex.
    check(
        "emptied value",
        |wb| {
            wb.set_value("Data", 5000, 1, n(4.0)).unwrap();
            wb.set_value("Data", 5000, 1, LiteralValue::Empty).unwrap();
        },
        [
            n(1_048_576.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(5000.0),
            n(1_048_576.0),
            n(1.0),
            n(0.0),
        ],
    );
    // A column with values is bounded by them, whatever is referenced below.
    check(
        "column with values",
        |wb| {
            for r in 1..=10u32 {
                wb.set_value("Data", r, 1, n(f64::from(r))).unwrap();
            }
            wb.set_formula("Refs", 1, 1, "Data!A5000").unwrap();
        },
        [
            n(1_048_566.0),
            n(0.0),
            n(10.0),
            n(10.0),
            n(1.0),
            n(1_048_576.0),
            n(1.0),
            n(1.0),
        ],
    );
}

#[test]
fn a_value_typed_below_a_reference_moves_the_extent_and_readers_follow() {
    for interactive in [false, true] {
        let mut wb = workbook(interactive);
        wb.set_formula("Refs", 1, 1, "Data!A5000").unwrap();
        let _ = read(&mut wb);
        wb.set_value("Data", 6000, 1, n(3.0)).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("Out", 1, 5), Some(n(6000.0)));
        assert_eq!(wb.get_value("Out", 1, 3), Some(n(5999.0)));
    }
}

/// Interactive: a formula typed into a cell whose formula was replaced by
/// an empty value goes through the graph (as it did when the emptied cell
/// kept its vertex), so the cell shows the formula's result.
#[test]
fn interactive_formula_into_an_emptied_formula_cell_evaluates() {
    for interactive in [false, true] {
        let mut wb = workbook(interactive);
        for r in 1..=3 {
            wb.set_formula("Out", r, 1, "Data!C1*2").unwrap();
        }
        wb.evaluate_all().unwrap();
        for r in 1..=3 {
            wb.set_value("Out", r, 1, LiteralValue::Empty).unwrap();
        }
        for r in 1..=3 {
            wb.set_formula("Out", r, 1, "Data!C1*2").unwrap();
        }
        wb.evaluate_all().unwrap();
        for r in 1..=3 {
            assert_eq!(
                wb.get_value("Out", r, 1),
                Some(n(2.0)),
                "interactive {interactive} row {r}"
            );
        }
    }
}
