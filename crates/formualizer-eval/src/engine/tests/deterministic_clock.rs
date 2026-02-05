use crate::engine::{DeterministicMode, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use crate::timezone::TimeZoneSpec;
use chrono::TimeZone;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType};

#[test]
fn now_and_today_use_injected_fixed_clock() {
    let wb = TestWorkbook::new();
    let fixed = chrono::Utc
        .with_ymd_and_hms(2025, 1, 15, 10, 0, 0)
        .single()
        .expect("valid fixed timestamp");

    let cfg = EvalConfig {
        deterministic_mode: DeterministicMode::Enabled {
            timestamp_utc: fixed,
            timezone: TimeZoneSpec::Utc,
        },
        ..Default::default()
    };
    let mut engine = Engine::new(wb, cfg);

    // A1 = NOW(), A2 = TODAY()
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            ASTNode {
                node_type: ASTNodeType::Function {
                    name: "NOW".into(),
                    args: vec![],
                },
                source_token: None,
                contains_volatile: true,
            },
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            2,
            1,
            ASTNode {
                node_type: ASTNodeType::Function {
                    name: "TODAY".into(),
                    args: vec![],
                },
                source_token: None,
                contains_volatile: true,
            },
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    let now_serial = match engine.get_cell_value("Sheet1", 1, 1).unwrap() {
        LiteralValue::Number(n) => n,
        v => panic!("Expected number, got {v:?}"),
    };
    let today_serial = match engine.get_cell_value("Sheet1", 2, 1).unwrap() {
        LiteralValue::Number(n) => n,
        v => panic!("Expected number, got {v:?}"),
    };

    let expected_now = crate::builtins::datetime::datetime_to_serial_for(
        engine.config.date_system,
        &fixed.naive_utc(),
    );
    let expected_today = crate::builtins::datetime::date_to_serial_for(
        engine.config.date_system,
        &fixed.date_naive(),
    );
    assert_eq!(now_serial, expected_now);
    assert_eq!(today_serial, expected_today);

    // Re-evaluating should remain stable (clock is fixed).
    engine.evaluate_all().unwrap();
    let now_serial_2 = match engine.get_cell_value("Sheet1", 1, 1).unwrap() {
        LiteralValue::Number(n) => n,
        _ => unreachable!(),
    };
    let today_serial_2 = match engine.get_cell_value("Sheet1", 2, 1).unwrap() {
        LiteralValue::Number(n) => n,
        _ => unreachable!(),
    };
    assert_eq!(now_serial, now_serial_2);
    assert_eq!(today_serial, today_serial_2);
}

#[test]
fn deterministic_mode_rejects_local_timezone() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    let fixed = chrono::Utc
        .with_ymd_and_hms(2025, 1, 15, 10, 0, 0)
        .single()
        .unwrap();

    let res = engine.set_deterministic_mode(DeterministicMode::Enabled {
        timestamp_utc: fixed,
        timezone: TimeZoneSpec::Local,
    });
    assert!(res.is_err());
}
