use chrono::{Duration, NaiveDate, NaiveTime};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{DateSystem, Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

#[test]
fn duration_consumers_use_the_stored_serial_precision() {
    use formualizer_eval::arrow_store::OverlayValue;
    use formualizer_eval::format::FormatId;
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("S", 1, 1, LiteralValue::Duration(Duration::seconds(1)))
        .unwrap();
    let sheet = engine.sheet_store_mut().sheet_mut("S").unwrap();
    sheet.set_sparse_overlay_value(0, 0, OverlayValue::Duration(1.25 / 86400.0));
    sheet.set_sparse_overlay_format(0, 0, Some(FormatId::DURATION));
    for (col, formula) in [
        (2, "=A1*86400"),
        (3, "=SUM(A1:A1)*86400"),
        (4, "=COUNTIF(A1:A1,A1)"),
    ] {
        engine
            .set_cell_formula("S", 1, col, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    for col in [2, 3] {
        assert_eq!(
            engine.get_cell_value("S", 1, col),
            Some(LiteralValue::Number(1.25))
        );
    }
    assert_eq!(
        engine.get_cell_value("S", 1, 4),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("S", 1, 1),
        Some(LiteralValue::Duration(Duration::milliseconds(1250)))
    );
}

#[test]
fn temporal_serials_survive_family_execution() {
    for lift in [false, true] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_lift: lift,
                ..EvalConfig::default()
            },
        );
        let date = NaiveDate::from_ymd_opt(2001, 1, 16).unwrap();
        for row in 1..=64 {
            engine
                .set_cell_value("S", row, 1, LiteralValue::Date(date))
                .unwrap();
            for (col, text) in [
                (2, format!("=A{row}+0.5")),
                (3, format!("=B{row}*1")),
                (4, format!("=TEXT(A{row},\"mm/dd/yyyy\")")),
            ] {
                engine
                    .set_cell_formula("S", row, col, parse(&text).unwrap())
                    .unwrap();
            }
        }
        engine.evaluate_all().unwrap();
        for row in 1..=64 {
            assert_eq!(
                engine.get_cell_value("S", row, 2),
                Some(LiteralValue::DateTime(date.and_hms_opt(12, 0, 0).unwrap()))
            );
            assert_eq!(
                engine.get_cell_value("S", row, 3),
                Some(LiteralValue::Number(36907.5))
            );
            assert_eq!(
                engine.get_cell_value("S", row, 4),
                Some(LiteralValue::Text("01/16/2001".into()))
            );
        }
    }
}

#[test]
fn native_time_egress_retains_whole_days() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let time = NaiveTime::from_hms_opt(1, 30, 0).unwrap();
    engine
        .set_cell_value("S", 1, 1, LiteralValue::Time(time))
        .unwrap();
    engine
        .set_cell_formula("S", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("S", 1, 3, parse("=A1-1").unwrap())
        .unwrap();
    engine
        .set_cell_value("S", 2, 1, LiteralValue::Duration(Duration::minutes(1530)))
        .unwrap();
    engine
        .set_cell_value("S", 3, 1, LiteralValue::Duration(Duration::hours(-6)))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("S", 1, 1),
        Some(LiteralValue::Time(time))
    );
    let fraction = Duration::minutes(90);
    assert_eq!(
        engine.get_cell_value("S", 1, 2),
        Some(LiteralValue::Duration(Duration::days(1) + fraction))
    );
    assert_eq!(
        engine.get_cell_value("S", 1, 3),
        Some(LiteralValue::Duration(-Duration::days(1) + fraction))
    );
    assert_eq!(
        engine.get_cell_value("S", 2, 1),
        Some(LiteralValue::Duration(Duration::minutes(1530)))
    );
    assert_eq!(
        engine.get_cell_value("S", 3, 1),
        Some(LiteralValue::Duration(Duration::hours(-6)))
    );
}

#[test]
fn temporal_cells_are_serials_for_every_formula_consumer() {
    for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
        for lift in [false, true] {
            let mut engine = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    date_system: system,
                    family_lift: lift,
                    ..EvalConfig::default()
                },
            );
            let date = NaiveDate::from_ymd_opt(2001, 1, 16).unwrap();
            let serial = formualizer_common::date_to_serial_for(system, &date);
            for row in 1..=2 {
                engine
                    .set_cell_value("S", row, 1, LiteralValue::Date(date))
                    .unwrap();
                engine
                    .set_cell_value("S", row, 2, LiteralValue::Number(10.0))
                    .unwrap();
            }
            engine
                .set_cell_value(
                    "S",
                    3,
                    1,
                    LiteralValue::DateTime(date.and_hms_opt(12, 0, 0).unwrap()),
                )
                .unwrap();
            engine
                .set_cell_value(
                    "S",
                    4,
                    1,
                    LiteralValue::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
                )
                .unwrap();
            engine
                .set_cell_value(
                    "S",
                    5,
                    1,
                    LiteralValue::Date(NaiveDate::from_ymd_opt(2002, 1, 16).unwrap()),
                )
                .unwrap();
            engine
                .set_cell_value("S", 6, 1, LiteralValue::Date(date))
                .unwrap();
            engine
                .set_cell_value("S", 5, 2, LiteralValue::Number(110.0))
                .unwrap();
            engine
                .set_cell_value("S", 6, 2, LiteralValue::Number(-100.0))
                .unwrap();
            let cases = [
                (
                    "=TEXT(DATE(2001,1,16),\"mm/dd/yyyy\")",
                    LiteralValue::Text("01/16/2001".into()),
                ),
                (
                    "=DATE(2001,1,16)+0.5",
                    LiteralValue::DateTime(date.and_hms_opt(12, 0, 0).unwrap()),
                ),
                ("=COUNTIF(A1:A2,DATE(2001,1,16))", LiteralValue::Number(2.0)),
                (
                    "=TEXT(A1,\"mm/dd/yyyy\")",
                    LiteralValue::Text("01/16/2001".into()),
                ),
                ("=COUNTIF(A1:A2,A1)", LiteralValue::Number(2.0)),
                ("=SUMIF(A1:A2,A1,B1:B2)", LiteralValue::Number(20.0)),
                ("=MATCH(A1,A1:A2,0)", LiteralValue::Number(1.0)),
                ("=VLOOKUP(A1,A1:B2,2,FALSE)", LiteralValue::Number(10.0)),
                ("=MAX(A1:A2)", LiteralValue::Number(serial)),
                ("=MIN(A1:A2)", LiteralValue::Number(serial)),
                ("=SUM(A1:A2)", LiteralValue::Number(serial * 2.0)),
                ("=A1>DATE(2001,1,15)", LiteralValue::Boolean(true)),
                (
                    "=DATEVALUE(TEXT(A1,\"mm/dd/yyyy\"))",
                    LiteralValue::Date(date),
                ),
                ("=INT(A3)", LiteralValue::Number(serial)),
                ("=A4*24", LiteralValue::Number(12.0)),
                ("=(A4+0.25)*24", LiteralValue::Number(18.0)),
                ("=(A1+0.5)-A1", LiteralValue::Number(0.5)),
                ("=XIRR(B5:B6,A5:A6)", LiteralValue::Number(0.1)),
            ];
            for (i, (text, _)) in cases.iter().enumerate() {
                engine
                    .set_cell_formula("S", i as u32 + 1, 4, parse(text).unwrap())
                    .unwrap();
            }
            engine.evaluate_all().unwrap();
            for (i, (text, expected)) in cases.iter().enumerate() {
                let got = engine.get_cell_value("S", i as u32 + 1, 4).unwrap();
                if let (LiteralValue::Number(a), LiteralValue::Number(b)) = (&got, expected) {
                    assert!((a - b).abs() < 1e-8, "{text}: {got:?} != {expected:?}");
                } else {
                    assert_eq!(&got, expected, "{text}");
                }
            }
            assert_eq!(
                engine.get_cell_value("S", 1, 1),
                Some(LiteralValue::Date(date))
            );
            engine
                .set_cell_formula("S", 1, 5, parse("=A1+0.5").unwrap())
                .unwrap();
            engine.evaluate_all().unwrap();
            assert_eq!(
                engine.get_cell_value("S", 1, 5),
                Some(LiteralValue::DateTime(date.and_hms_opt(12, 0, 0).unwrap()))
            );
        }
    }
}
