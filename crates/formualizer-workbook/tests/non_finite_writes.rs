//! No cell stores NaN or an infinity: every write path stores them as `#NUM!`
//! (`LiteralValue::finite_or_num_error`), as the loaders do. Before, a NaN
//! written this way reached the sorting collectors (MEDIAN, SMALL, RANK, ...).

use std::collections::BTreeMap;

use formualizer_common::ExcelErrorKind;
use formualizer_workbook::{CellData, LiteralValue, Workbook, WorkbookConfig};

const BAD: [f64; 3] = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY];

fn is_num(value: Option<LiteralValue>) -> bool {
    matches!(value, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Num)
}

/// Writes 1, `bad`, 3 into A1:A3 with `write`, then checks the stored value
/// and a MEDIAN over it, with and without the changelog.
fn check(write: impl Fn(&mut Workbook, f64)) {
    for config in [WorkbookConfig::interactive(), WorkbookConfig::ephemeral()] {
        for bad in BAD {
            let mut wb = Workbook::new_with_config(config.clone());
            wb.add_sheet("S").unwrap();
            write(&mut wb, bad);
            wb.set_formula("S", 1, 2, "=MEDIAN(A1:A3)").unwrap();
            wb.evaluate_all().unwrap();
            assert!(
                is_num(wb.get_value("S", 2, 1)),
                "{bad}: {:?}",
                wb.get_value("S", 2, 1)
            );
            assert_eq!(wb.get_value("S", 3, 1), Some(LiteralValue::Number(3.0)));
            assert!(is_num(wb.get_value("S", 1, 2)), "{bad}");
        }
    }
}

fn column(bad: f64) -> Vec<Vec<LiteralValue>> {
    [1.0, bad, 3.0]
        .into_iter()
        .map(|n| vec![LiteralValue::Number(n)])
        .collect()
}

#[test]
fn set_value_stores_non_finite_as_num() {
    check(|wb, bad| {
        for (r, n) in [1.0, bad, 3.0].into_iter().enumerate() {
            wb.set_value("S", r as u32 + 1, 1, LiteralValue::Number(n))
                .unwrap();
        }
    });
}

#[test]
fn set_values_stores_non_finite_as_num() {
    check(|wb, bad| wb.set_values("S", 1, 1, &column(bad)).unwrap());
}

#[test]
fn write_range_stores_non_finite_as_num() {
    check(|wb, bad| {
        let cells: BTreeMap<(u32, u32), CellData> = [1.0, bad, 3.0]
            .into_iter()
            .enumerate()
            .map(|(r, n)| {
                (
                    (r as u32 + 1, 1),
                    CellData {
                        value: Some(LiteralValue::Number(n)),
                        formula: None,
                        style: None,
                    },
                )
            })
            .collect();
        wb.write_range("S", (1, 1), cells).unwrap();
    });
}

#[test]
fn action_writes_store_non_finite_as_num() {
    check(|wb, bad| {
        wb.action("values", |action| {
            action.set_values("S", 1, 1, &column(bad))
        })
        .unwrap();
    });
    check(|wb, bad| {
        wb.action("value", |action| {
            for (r, n) in [1.0, bad, 3.0].into_iter().enumerate() {
                action.set_value("S", r as u32 + 1, 1, LiteralValue::Number(n))?;
            }
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn finite_values_are_unchanged() {
    for v in [
        LiteralValue::Number(1.5),
        LiteralValue::Number(-0.0),
        LiteralValue::Number(f64::MAX),
        LiteralValue::Int(7),
        LiteralValue::Text("NaN".into()),
    ] {
        assert_eq!(v.clone().finite_or_num_error(), v);
    }
}
