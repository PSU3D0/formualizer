use super::common::arrow_eval_config;
use crate::engine::eval::Engine;
use crate::engine::EvalConfig;
use crate::test_workbook::TestWorkbook;
use chrono::Timelike;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[test]
fn numeric_normalization_int_to_number_on_storage_and_read() {
    // Non-canonical mode
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );

    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=1+2").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(3.0))
    );

    // Canonical mode
    let mut cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn temporal_tags_preserved_across_computed_overlay_compaction() {
    let mut cfg = arrow_eval_config();
    // Force compaction on the first mirrored computed-overlay entry.
    cfg.max_overlay_memory_bytes = Some(0);

    let mut engine = Engine::new(TestWorkbook::default(), cfg.clone());

    let dt = chrono::NaiveDate::from_ymd_opt(2026, 2, 7)
        .unwrap()
        .and_hms_opt(12, 34, 56)
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::DateTime(dt))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=B1").unwrap())
        .unwrap();

    let dur = chrono::Duration::seconds(90);
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Duration(dur))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=B2").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    let sheet = engine.sheet_store().sheet("Sheet1").unwrap();
    let rv_dt = sheet.range_view(0, 0, 0, 0);
    let rv_dur = sheet.range_view(1, 0, 1, 0);

    let tags_dt: Vec<u8> = rv_dt
        .type_tags_slices()
        .map(|r| {
            let (_off, _len, cols) = r.unwrap();
            cols[0].value(0)
        })
        .collect();
    assert_eq!(tags_dt, vec![crate::arrow_store::TypeTag::DateTime as u8]);

    let tags_dur: Vec<u8> = rv_dur
        .type_tags_slices()
        .map(|r| {
            let (_off, _len, cols) = r.unwrap();
            cols[0].value(0)
        })
        .collect();
    assert_eq!(tags_dur, vec![crate::arrow_store::TypeTag::Duration as u8]);

    // Numeric serial remains in numeric lane.
    // Compute the expected serial from canonical reads of the source cells to avoid
    // sensitivity to float<->datetime round-trips.
    let expected_dt_serial = match engine.get_cell_value("Sheet1", 1, 2).unwrap() {
        LiteralValue::Date(d) => {
            let dt = d.and_hms_opt(0, 0, 0).unwrap();
            crate::builtins::datetime::datetime_to_serial_for(cfg.date_system, &dt)
        }
        LiteralValue::DateTime(dt) => {
            crate::builtins::datetime::datetime_to_serial_for(cfg.date_system, &dt)
        }
        LiteralValue::Time(t) => t.num_seconds_from_midnight() as f64 / 86_400.0,
        other => panic!("expected a temporal at B1, got {other:?}"),
    };
    let dt_serial = rv_dt.numbers_slices().next().unwrap().unwrap().2[0].value(0);
    assert!((dt_serial - expected_dt_serial).abs() < 1e-10);

    let expected_dur_serial = match engine.get_cell_value("Sheet1", 2, 2).unwrap() {
        LiteralValue::Duration(d) => d.num_seconds() as f64 / 86_400.0,
        other => panic!("expected a duration at B2, got {other:?}"),
    };
    let dur_serial = rv_dur.numbers_slices().next().unwrap().unwrap().2[0].value(0);
    assert!((dur_serial - expected_dur_serial).abs() < 1e-10);
}

#[test]
fn temporal_tags_preserved_across_delta_overlay_compaction() {
    let mut cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg.clone());

    // Build Arrow sheet with 1 column, 64 rows (single chunk of 64).
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 1, 64);
        for _ in 0..64 {
            ab.append_row("S", &[LiteralValue::Empty]).unwrap();
        }
        let _ = ab.finish().unwrap();
    }

    let dt = chrono::NaiveDate::from_ymd_opt(2026, 2, 7)
        .unwrap()
        .and_hms_opt(1, 2, 3)
        .unwrap();
    let dur = chrono::Duration::minutes(5);

    // Two edits -> overlay compaction triggers; DateTime tag must survive.
    engine
        .set_cell_value("S", 1, 1, LiteralValue::DateTime(dt))
        .unwrap();
    engine
        .set_cell_value("S", 2, 1, LiteralValue::Number(2.0))
        .unwrap();

    // Two more edits -> compaction triggers again; Duration tag must survive.
    engine
        .set_cell_value("S", 3, 1, LiteralValue::Duration(dur))
        .unwrap();
    engine
        .set_cell_value("S", 4, 1, LiteralValue::Number(4.0))
        .unwrap();

    let sheet = engine.sheet_store().sheet("S").unwrap();

    let rv_dt = sheet.range_view(0, 0, 0, 0);
    let tag_dt = rv_dt.type_tags_slices().next().unwrap().unwrap().2[0].value(0);
    assert_eq!(tag_dt, crate::arrow_store::TypeTag::DateTime as u8);

    let rv_dur = sheet.range_view(2, 0, 2, 0);
    let tag_dur = rv_dur.type_tags_slices().next().unwrap().unwrap().2[0].value(0);
    assert_eq!(tag_dur, crate::arrow_store::TypeTag::Duration as u8);

    // Serial in numeric lane.
    let expected_dt_serial =
        crate::builtins::datetime::datetime_to_serial_for(cfg.date_system, &dt);
    let dt_serial = rv_dt.numbers_slices().next().unwrap().unwrap().2[0].value(0);
    assert!((dt_serial - expected_dt_serial).abs() < 1e-10);

    let expected_dur_serial = dur.num_seconds() as f64 / 86_400.0;
    let dur_serial = rv_dur.numbers_slices().next().unwrap().unwrap().2[0].value(0);
    assert!((dur_serial - expected_dur_serial).abs() < 1e-10);
}

#[test]
fn error_mirroring_cycle_is_visible_under_canonical_reads() {
    let mut cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=B1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Circ),
        other => panic!("expected #CIRC! at A1, got {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Circ),
        other => panic!("expected #CIRC! at B1, got {other:?}"),
    }
}

#[test]
fn empty_semantics_spill_children_are_none_after_retraction_canonical_mode() {
    let mut cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    // Retract to a scalar.
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=42").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(42.0))
    );
    // Former spill children should be "empty" => None.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);
    assert_eq!(engine.get_cell_value("Sheet1", 2, 1), None);
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), None);
}
