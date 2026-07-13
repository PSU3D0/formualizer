use std::collections::BTreeSet;
use std::sync::Arc;

use super::common::abs_cell_ref;
use crate::engine::{
    DeferredFormulaPackage, DeferredFormulaReplay, DeferredReplayFormula, Engine, EvalConfig,
    ExplicitPartitionLegacyMembers, ExplicitSourceFamilyMembers, FormulaCompressedSourceBatch,
    FormulaCompressedSourceReport, FormulaIngestBatch, FormulaIngestRecord, FormulaParsePolicy,
    FormulaPlaneMode, FormulaReplayDisposition, PartitionLegacyMember, PartitionLegacyMemberKind,
    PartitionReconciliation, PartitionedSourceFormulaFamily, PlacementDomainTransport,
    RowVisibilitySource, SourceCoord, SourceFamilyId, SourceFamilyMembers, SourceFormulaFamily,
    SourceRect,
};
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

struct ExactTestReplay {
    formulas: Vec<(u32, u32, String, Option<SourceFamilyId>)>,
}

impl DeferredFormulaReplay for ExactTestReplay {
    fn replay(
        &mut self,
        disposition: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        Ok(self
            .formulas
            .iter()
            .filter_map(|(row, col, text, family)| {
                let coord = SourceCoord {
                    row: row.saturating_sub(1),
                    col: col.saturating_sub(1),
                };
                let (coordinate_disposition, partition_owner) = match family {
                    Some(family) => (
                        disposition.shared_disposition(*family, coord),
                        Some(*family),
                    ),
                    None => disposition.ordinary_disposition(coord),
                };
                matches!(
                    coordinate_disposition,
                    crate::engine::FormulaReplayCoordinateDisposition::LegacyShared
                        | crate::engine::FormulaReplayCoordinateDisposition::LegacyOrdinary
                )
                .then(|| DeferredReplayFormula {
                    row: *row,
                    col: *col,
                    text: text.clone(),
                    family: *family,
                    partition_owner,
                })
            })
            .collect())
    }

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String> {
        Ok(self
            .formulas
            .iter()
            .find(|(candidate_row, candidate_col, _, _)| {
                *candidate_row == row && *candidate_col == col
            })
            .map(|(row, col, text, family)| DeferredReplayFormula {
                row: *row,
                col: *col,
                text: text.clone(),
                family: *family,
                partition_owner: *family,
            }))
    }
}

struct TestDeferredReplay {
    text: &'static str,
    fail_once: bool,
    panic_at: bool,
}

impl DeferredFormulaReplay for TestDeferredReplay {
    fn replay(
        &mut self,
        _disposition: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        if self.fail_once {
            self.fail_once = false;
            return Err("injected replay failure".to_string());
        }
        Ok(vec![DeferredReplayFormula {
            row: 1,
            col: 1,
            text: self.text.to_string(),
            family: None,
            partition_owner: None,
        }])
    }

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String> {
        assert!(!self.panic_at, "injected formula_at panic");
        Ok(Some(DeferredReplayFormula {
            row,
            col,
            text: self.text.to_string(),
            family: None,
            partition_owner: None,
        }))
    }
}

fn deferred_package(text: &'static str, fail_once: bool, panic_at: bool) -> DeferredFormulaPackage {
    let report = FormulaCompressedSourceReport {
        source_formula_records_spooled: 1,
        ..FormulaCompressedSourceReport::default()
    };
    DeferredFormulaPackage::new(
        "Sheet1".to_string(),
        report,
        Vec::new(),
        Vec::new(),
        Box::new(TestDeferredReplay {
            text,
            fail_once,
            panic_at,
        }),
    )
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

#[test]
fn formula_plane_off_ingest_reports_graph_materialized_formulas() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Off);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 1, "=1+1"),
            record(&mut engine, 2, 1, "=A1+1"),
        ],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("ingest formulas");

    assert_eq!(report.mode, FormulaPlaneMode::Off);
    assert_eq!(report.formula_cells_seen, 2);
    assert_eq!(report.graph_formula_cells_materialized, 2);
    assert_eq!(engine.last_formula_ingest_report(), Some(&report));
    assert_eq!(engine.formula_ingest_report_total().formula_cells_seen, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);

    engine.evaluate_all().expect("evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn formula_plane_shadow_deferred_build_graph_all_materializes_all_formulas() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine.stage_formula_text("Sheet1", 1, 2, "=A1+1".to_string());
    engine.stage_formula_text("Sheet1", 2, 2, "=A2+1".to_string());
    assert_eq!(engine.staged_formula_count(), 2);

    engine.build_graph_all().expect("build staged formulas");
    engine.evaluate_all().expect("evaluate staged formulas");

    let report = engine
        .last_formula_ingest_report()
        .expect("formula ingest report");
    assert_eq!(report.mode, FormulaPlaneMode::Shadow);
    assert_eq!(report.formula_cells_seen, 2);
    assert_eq!(report.graph_formula_cells_materialized, 2);
    assert_eq!(report.shadow_accepted_span_cells, 0);
    assert_eq!(report.graph_formula_vertices_avoided_shadow, 0);
    assert_eq!(engine.staged_formula_count(), 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn formula_plane_authoritative_ingest_skips_accepted_span_graph_materialization() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.mode, FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(report.formula_cells_seen, 100);
    assert_eq!(report.shadow_accepted_span_cells, 100);
    assert_eq!(report.graph_formula_cells_materialized, 0);
    let stats = engine.baseline_stats();
    assert_eq!(stats.graph_formula_vertex_count, 0);
    assert_eq!(stats.formula_plane_active_span_count, 1);
    assert_eq!(stats.formula_plane_producer_result_entries, 1);
    assert_eq!(stats.formula_plane_consumer_read_entries, 1);

    engine.evaluate_all().expect("span-only mixed evaluate_all");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine
            .evaluate_cell("Sheet1", 1, 2)
            .expect("evaluate_cell routes through FormulaPlane coordinator"),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_text_resolves_authoritative_source_family_placements() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Inspect").unwrap();

    let family = SourceFormulaFamily {
        source_id: SourceFamilyId {
            sheet_instance: 1,
            source_index: 1,
        },
        anchor_coord0: SourceCoord { row: 0, col: 1 },
        anchor_text: Arc::from("A1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 99,
            col: 1,
        }),
        member_count: 100,
    };
    let preparation = engine.prepare_source_formula_families("Sheet1", &[family]);
    assert_eq!(preparation.direct_family_count(), 1);
    engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceReport::default(),
            preparation,
        )])
        .unwrap();

    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    let source_sheet = engine.graph.sheet_id("Sheet1").unwrap();
    for (inspect_row, source_row, expected) in [
        (1, 1, "=A1 + 1"),
        (2, 50, "=A50 + 1"),
        (3, 100, "=A100 + 1"),
    ] {
        let source = abs_cell_ref(source_sheet, source_row, 2);
        assert_eq!(
            engine.formula_text_at_cell(source).unwrap().as_deref(),
            Some(expected)
        );
        engine
            .set_cell_formula(
                "Inspect",
                inspect_row,
                1,
                parse(format!("=FORMULATEXT(Sheet1!B{source_row})")).unwrap(),
            )
            .unwrap();
    }

    engine.evaluate_all().unwrap();
    for (row, expected) in [(1, "=A1 + 1"), (2, "=A50 + 1"), (3, "=A100 + 1")] {
        assert_eq!(
            engine.get_cell_value("Inspect", row, 1),
            Some(LiteralValue::Text(expected.into()))
        );
    }
}

#[test]
fn formula_plane_authoritative_cross_sheet_family_promotes_and_dirty_propagates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Data").unwrap();

    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 1, &format!("=Data!A{row}*2")));
    }

    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    assert_eq!(report.graph_formula_cells_materialized, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    engine.evaluate_all().expect("initial cross-sheet evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 1),
        Some(LiteralValue::Number(100.0))
    );

    engine
        .set_cell_value("Data", 50, 1, LiteralValue::Number(1000.0))
        .unwrap();
    engine.evaluate_all().expect("dirty cross-sheet evaluate");

    assert_eq!(
        engine.get_cell_value("Sheet1", 49, 1),
        Some(LiteralValue::Number(98.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 1),
        Some(LiteralValue::Number(2000.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 51, 1),
        Some(LiteralValue::Number(102.0))
    );
}

#[test]
fn formula_plane_authoritative_sum_static_range_family_promotes() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=100 {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=A{row} * SUM($A$1:$A$10)"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    engine.evaluate_all().expect("initial range evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(5.0 * 55.0))
    );

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(100.0))
        .unwrap();
    engine.evaluate_all().expect("dirty range evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(100.0 * 150.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 2),
        Some(LiteralValue::Number(10.0 * 150.0))
    );
}

#[test]
fn formula_plane_authoritative_sumifs_family_promotes() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Data").unwrap();
    let mut expected = 0.0;
    for row in 1..=100 {
        let category = if row % 3 == 1 { "Type1" } else { "Type0" };
        let value = row as f64;
        if category == "Type1" {
            expected += value;
        }
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Text(category.to_string()))
            .unwrap();
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(value))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=SUMIFS(Data!$B$1:$B$100, Data!$A$1:$A$100, \"Type1\") + A{row}"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 1);

    engine.evaluate_all().expect("initial sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(expected + 1.0))
    );

    engine
        .set_cell_value("Data", 4, 2, LiteralValue::Number(1000.0))
        .unwrap();
    let updated_expected = expected - 4.0 + 1000.0;
    engine.evaluate_all().expect("dirty sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(updated_expected + 50.0))
    );
}

#[test]
fn formula_plane_authoritative_constant_sumifs_family_promotes_via_broadcast() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Data").unwrap();
    let mut expected = 0.0;
    for row in 1..=100 {
        let category = if row % 3 == 1 { "Type1" } else { "Type0" };
        let value = row as f64;
        if category == "Type1" {
            expected += value;
        }
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Text(category.to_string()))
            .unwrap();
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(value))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=50 {
        formulas.push(record(
            &mut engine,
            row,
            2,
            "=SUMIFS(Data!$B$1:$B$100, Data!$A$1:$A$100, \"Type1\")",
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 1);

    engine
        .evaluate_all()
        .expect("initial constant sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(expected))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 25, 2),
        Some(LiteralValue::Number(expected))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(expected))
    );

    engine
        .set_cell_value("Data", 4, 2, LiteralValue::Number(1000.0))
        .unwrap();
    let updated_expected = expected - 4.0 + 1000.0;
    engine
        .evaluate_all()
        .expect("dirty constant sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(updated_expected))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(updated_expected))
    );
}

#[test]
fn formula_plane_authoritative_demotes_small_non_constant_domains() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = match row % 10 {
            0 => format!("=A{row}*RAND()"),
            1 => format!("=A{row}+TODAY()"),
            2 => format!("=A{row}*NOW()"),
            _ => format!("=A{row}*2"),
        };
        formulas.push(record(&mut engine, row, 2, &formula));
    }

    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 0);
    assert_eq!(stats.graph_formula_vertex_count, 100);

    engine.evaluate_all().expect("evaluate demoted formulas");
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 9, 2),
        Some(LiteralValue::Number(18.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 99, 2),
        Some(LiteralValue::Number(198.0))
    );
}

#[test]
fn formula_plane_authoritative_demotes_99_cell_non_constant_runs() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = if row % 100 == 0 {
            format!("=A{row}/0")
        } else {
            format!("=A{row}*2")
        };
        formulas.push(record(&mut engine, row, 2, &formula));
    }

    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 0);
    assert_eq!(stats.graph_formula_vertex_count, 200);

    engine.evaluate_all().expect("evaluate demoted formulas");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 99, 2),
        Some(LiteralValue::Number(198.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 101, 2),
        Some(LiteralValue::Number(202.0))
    );
    assert!(matches!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Error(_))
    ));
    assert!(matches!(
        engine.get_cell_value("Sheet1", 200, 2),
        Some(LiteralValue::Error(_))
    ));
}

#[test]
fn formula_plane_authoritative_promotes_100_cell_non_constant_run() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}*2")));
    }

    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 1);
    assert_eq!(stats.graph_formula_vertex_count, 0);

    engine.evaluate_all().expect("evaluate promoted formulas");
    for row in 1..=100 {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(row as f64 * 2.0))
        );
    }
}

#[test]
fn formula_plane_authoritative_evaluate_all_orders_span_chain() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        formulas.push(record(&mut engine, row, 3, &format!("=B{row}+2")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.graph_formula_cells_materialized, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine.evaluate_all().expect("span chain evaluate_all");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn formula_plane_authoritative_mixed_accept_and_fallback_materializes_only_fallback() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let mut formulas = Vec::new();
    for row in 1..=100 {
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    formulas.push(record(&mut engine, 1, 3, "=1+1"));

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.formula_cells_seen, 101);
    assert_eq!(report.shadow_accepted_span_cells, 100);
    assert_eq!(report.shadow_fallback_cells, 1);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    let stats = engine.baseline_stats();
    assert_eq!(stats.graph_formula_vertex_count, 1);
    assert_eq!(stats.formula_plane_active_span_count, 1);
    engine
        .evaluate_all()
        .expect("mixed independent legacy/span runtime");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_authoritative_mixed_span_to_legacy_sum_evaluates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    formulas.push(record(&mut engine, 1, 4, "=SUM(B1:B100)"));

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.shadow_accepted_span_cells, 100);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    engine.evaluate_all().expect("span to legacy runtime");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(5150.0))
    );
}

#[test]
fn formula_plane_authoritative_mixed_legacy_to_span_evaluates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = vec![record(&mut engine, 1, 1, "=1+1")];
    for row in 1..=100 {
        if row != 1 {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
        }
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.shadow_accepted_span_cells, 100);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    engine.evaluate_all().expect("legacy to span runtime");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn formula_plane_authoritative_fallback_only_still_evaluates_legacy() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![record(&mut engine, 1, 1, "=1+1")],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative fallback ingest");

    assert_eq!(report.formula_cells_seen, 1);
    assert_eq!(report.shadow_accepted_span_cells, 0);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().expect("fallback-only evaluation");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_shadow_build_graph_for_sheets_reports_selected_sheet_only() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Other").unwrap();

    engine.stage_formula_text("Sheet1", 1, 1, "=1+1".to_string());
    engine.stage_formula_text("Other", 1, 1, "=1+2".to_string());

    engine
        .build_graph_for_sheets(["Sheet1"])
        .expect("build selected sheet");

    let report = engine
        .last_formula_ingest_report()
        .expect("formula ingest report");
    assert_eq!(report.mode, FormulaPlaneMode::Shadow);
    assert_eq!(report.formula_cells_seen, 1);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    assert_eq!(engine.staged_formula_count(), 1);
    assert!(engine.get_staged_formula_text("Other", 1, 1).is_some());
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 1);
}

#[test]
fn formula_plane_spill_commit_redirties_span_reading_spill_children() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let mut formulas = Vec::new();
    for row in 2..=101 {
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+10")));
    }
    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(10.0))
    );

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(3.0))
    );

    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 2,
        "expected spill-region notification to re-evaluate span, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(12.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(13.0))
    );
}

#[test]
fn formula_plane_source_invalidation_uses_conservative_whole_span_dirty() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.define_source_scalar("Feed", Some(1)).unwrap();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
    engine.evaluate_all().unwrap();

    engine.invalidate_source("Feed").unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
        "expected source invalidation to re-evaluate active spans, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn formula_plane_row_visibility_change_redirties_absolute_anchor_span() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=$A$1+A{row}*0+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    engine.ingest_formula_batches(batches).unwrap();
    engine.evaluate_all().unwrap();

    engine
        .set_row_hidden("Sheet1", 1, true, RowVisibilitySource::Manual)
        .unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
        "expected whole-row visibility notification to re-evaluate span, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn formula_plane_insert_rows_conservatively_redirties_sheet_spans() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    engine.ingest_formula_batches(batches).unwrap();
    engine.evaluate_all().unwrap();

    engine.insert_rows("Sheet1", 10, 1).unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
        "expected sheet structural notification to re-evaluate span, got {result:?}"
    );
}

#[test]
fn formula_plane_remove_sheet_redirties_surviving_spans() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let data_id = engine.add_sheet("Data").unwrap();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
    engine.evaluate_all().unwrap();

    engine.remove_sheet(data_id).unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
        "expected remove-sheet notification to re-evaluate surviving spans, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn formula_plane_remove_sheet_hosting_span_removes_active_span() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let other_id = engine.add_sheet("Other").unwrap();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Other", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Other", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    engine.remove_sheet(other_id).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert!(engine.graph.sheet_id("Other").is_none());
}

#[test]
fn generic_source_family_preparation_accepts_complete_domains_and_rejects_explicit_authority() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let id = |source_index| SourceFamilyId {
        sheet_instance: 1,
        source_index,
    };
    let complete = vec![
        SourceFormulaFamily {
            source_id: id(1),
            anchor_coord0: SourceCoord { row: 0, col: 0 },
            anchor_text: Arc::from("1+1"),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 2,
                col: 0,
            }),
            member_count: 3,
        },
        SourceFormulaFamily {
            source_id: id(2),
            anchor_coord0: SourceCoord { row: 0, col: 2 },
            anchor_text: Arc::from("1+1"),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::ColRun {
                row: 0,
                col_start: 2,
                col_end: 4,
            }),
            member_count: 3,
        },
        SourceFormulaFamily {
            source_id: id(3),
            anchor_coord0: SourceCoord { row: 4, col: 4 },
            anchor_text: Arc::from("1+1"),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::Rect(
                SourceRect {
                    start: SourceCoord { row: 4, col: 4 },
                    end: SourceCoord { row: 5, col: 5 },
                },
            )),
            member_count: 4,
        },
        SourceFormulaFamily {
            source_id: id(4),
            anchor_coord0: SourceCoord { row: 8, col: 8 },
            anchor_text: Arc::from("1+1"),
            members: SourceFamilyMembers::ExplicitMembers(
                ExplicitSourceFamilyMembers::try_new(vec![
                    SourceCoord { row: 8, col: 8 },
                    SourceCoord { row: 9, col: 8 },
                ])
                .unwrap(),
            ),
            member_count: 2,
        },
    ];

    let preparation = engine.prepare_source_formula_families("Sheet1", &complete);
    assert_eq!(preparation.direct_family_count(), 3);
    assert_eq!(preparation.direct_cell_count(), 10);
    assert_eq!(
        preparation.rejected.get(&id(4)).map(String::as_str),
        Some("ExplicitMembersRequireExactRecords")
    );

    let mut limits = engine.workbook_load_limits().clone();
    limits.max_sheet_rows = 2;
    limits.max_sheet_logical_cells = 2;
    engine.set_workbook_load_limits(limits);
    let limited = engine.prepare_source_formula_families("Sheet1", &complete[..1]);
    assert_eq!(
        limited.rejected.get(&id(1)).map(String::as_str),
        Some("CompleteDomainOutOfBounds")
    );

    engine.force_source_family_fallback_for_test(true);
    let forced = engine.prepare_source_formula_families("Sheet1", &complete[..1]);
    assert_eq!(forced.direct_family_count(), 0);
    assert_eq!(
        forced.rejected.get(&id(1)).map(String::as_str),
        Some("ForcedReplay")
    );
}

#[test]
fn selected_multi_sheet_build_preserves_caller_order_and_shared_parse_cache() {
    fn engine() -> Engine<TestWorkbook> {
        let cfg = EvalConfig {
            defer_graph_building: true,
            formula_parse_policy: FormulaParsePolicy::KeepCachedValue,
            ..EvalConfig::default()
        };
        let mut engine = Engine::new(TestWorkbook::default(), cfg);
        for sheet in ["Sheet1", "Other"] {
            engine.stage_formula_text(sheet, 1, 1, "=1+".to_string());
            engine.stage_formula_text(sheet, 2, 1, "=1+1".to_string());
        }
        engine
    }

    let mut all = engine();
    all.build_graph_all().unwrap();
    assert_eq!(all.formula_parse_diagnostics().len(), 2);
    assert_eq!(
        all.formula_parse_diagnostics()
            .iter()
            .map(|diagnostic| diagnostic.sheet.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["Other", "Sheet1"])
    );

    for (order, diagnostic_sheet) in [
        (["Sheet1", "Other"], "Sheet1"),
        (["Other", "Sheet1"], "Other"),
    ] {
        let mut selected = engine();
        selected.build_graph_for_sheets(order).unwrap();
        assert_eq!(selected.formula_parse_diagnostics().len(), 1);
        assert_eq!(
            selected.formula_parse_diagnostics()[0].sheet,
            diagnostic_sheet
        );
        assert_eq!(
            selected.formula_ingest_report_total(),
            all.formula_ingest_report_total()
        );
        assert_eq!(selected.staged_formula_count(), 0);
    }
}

#[test]
fn deferred_replay_failure_restores_package_and_retry_publishes_once() {
    let cfg = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_package("1+1", true, false));
    let before = engine.formula_ingest_report_total().clone();

    let error = engine.build_graph_all().unwrap_err();
    assert!(error.to_string().contains("injected replay failure"));
    assert_eq!(engine.staged_formula_count(), 1);
    assert_eq!(engine.formula_ingest_report_total(), &before);
    assert!(engine.last_formula_ingest_report().is_none());
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);

    engine.build_graph_all().unwrap();
    assert_eq!(engine.staged_formula_count(), 0);
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_spool_replays,
        1
    );
    assert_eq!(engine.formula_ingest_report_total().source_spool_replays, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 1);
}

#[test]
fn deferred_strict_parse_failure_restores_package_without_diagnostics_or_telemetry() {
    let cfg = EvalConfig {
        defer_graph_building: true,
        formula_parse_policy: FormulaParsePolicy::Strict,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_package("1+", false, false));
    let before = engine.formula_ingest_report_total().clone();

    assert!(engine.build_graph_all().is_err());
    assert_eq!(engine.staged_formula_count(), 1);
    assert!(engine.formula_parse_diagnostics().is_empty());
    assert_eq!(engine.formula_ingest_report_total(), &before);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);

    engine.config.formula_parse_policy = FormulaParsePolicy::AsText;
    engine.build_graph_all().unwrap();
    assert_eq!(engine.staged_formula_count(), 0);
    assert_eq!(engine.formula_parse_diagnostics().len(), 1);
    assert_eq!(engine.formula_ingest_report_total().source_spool_replays, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 1);
}

#[test]
fn deferred_poisoned_lock_failure_restores_package_without_publication() {
    let cfg = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_package("1+1", false, true));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine.stage_formula_text("Sheet1", 1, 1, "=2+2".to_string());
    }));
    assert!(panic.is_err());
    let before = engine.formula_ingest_report_total().clone();

    for _ in 0..2 {
        let error = engine.build_graph_all().unwrap_err();
        assert!(error.to_string().contains("lock poisoned"));
        assert_eq!(engine.staged_formula_count(), 1);
        assert_eq!(engine.formula_ingest_report_total(), &before);
        assert!(engine.last_formula_ingest_report().is_none());
        assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    }
}

#[test]
fn source_family_preparation_rejects_cross_engine_finalization_before_authority() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut origin = Engine::new(TestWorkbook::default(), cfg.clone());
    let mut other = Engine::new(TestWorkbook::default(), cfg);
    let family = SourceFormulaFamily {
        source_id: SourceFamilyId {
            sheet_instance: 1,
            source_index: 1,
        },
        anchor_coord0: SourceCoord { row: 0, col: 0 },
        anchor_text: Arc::from("1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 2,
            col: 0,
        }),
        member_count: 3,
    };
    let preparation = origin.prepare_source_formula_families("Sheet1", &[family]);
    assert_eq!(preparation.direct_family_count(), 1);
    let before = other.formula_ingest_report_total().clone();

    let error = other
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceReport::default(),
            preparation,
        )])
        .unwrap_err();

    assert!(error.to_string().contains("belongs to another engine"));
    assert_eq!(other.formula_ingest_report_total(), &before);
    assert!(other.last_formula_ingest_report().is_none());
    assert_eq!(other.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(other.baseline_stats().graph_formula_vertex_count, 0);
}

#[test]
fn partitioned_shadow_prepares_all_fragments_from_one_analysis_without_authority() {
    let family = PartitionedSourceFormulaFamily {
        source_id: SourceFamilyId {
            sheet_instance: 90,
            source_index: 1,
        },
        template_origin0: SourceCoord { row: 0, col: 1 },
        template_text: Arc::from("SUM($A$1)+1"),
        declared: SourceRect {
            start: SourceCoord { row: 0, col: 1 },
            end: SourceCoord { row: 7, col: 1 },
        },
        surviving_member_count: 7,
        fragments: vec![
            PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 2,
                col: 1,
            },
            PlacementDomainTransport::RowRun {
                row_start: 4,
                row_end: 6,
                col: 1,
            },
        ],
        legacy_members: ExplicitPartitionLegacyMembers::try_new(vec![PartitionLegacyMember {
            coord: SourceCoord { row: 3, col: 1 },
            kind: PartitionLegacyMemberKind::SharedFamilyMember,
        }])
        .unwrap(),
        reconciliation: PartitionReconciliation {
            shared_members: 7,
            ordinary_exceptions: 0,
            holes: 1,
        },
    };
    let source_report = FormulaCompressedSourceReport {
        source_fragmentable_families: 1,
        source_fragmentable_cells: 7,
        source_hole_exclusions: 1,
        ..FormulaCompressedSourceReport::default()
    };
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow),
    );
    let report = engine
        .ingest_compressed_formula_source_batches(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceBatch::with_proposals(
                "Sheet1",
                source_report,
                Vec::new(),
                vec![family],
            ),
        )])
        .unwrap();

    assert_eq!(report.source_anchor_parses, 1, "{report:?}");
    assert_eq!(report.source_anchor_asts, 1, "{report:?}");
    assert_eq!(report.source_anchor_analyses, 1, "{report:?}");
    assert_eq!(report.source_partition_analyses_reused, 1, "{report:?}");
    assert_eq!(report.source_partitioned_families_prepared, 1, "{report:?}");
    assert_eq!(report.source_partition_fragments_prepared, 2, "{report:?}");
    assert_eq!(report.source_partition_span_cells_prepared, 6, "{report:?}");
    assert_eq!(report.source_partition_fallback_cells, 1, "{report:?}");
    assert_eq!(report.source_partition_function_semantics, 1, "{report:?}");
    assert_eq!(report.source_partition_holes, 1, "{report:?}");
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
}

#[test]
fn partitioned_shadow_rejects_the_whole_family_when_one_fragment_fails() {
    let family = PartitionedSourceFormulaFamily {
        source_id: SourceFamilyId {
            sheet_instance: 90,
            source_index: 2,
        },
        template_origin0: SourceCoord { row: 0, col: 1 },
        template_text: Arc::from("A1048575+1"),
        declared: SourceRect {
            start: SourceCoord { row: 0, col: 1 },
            end: SourceCoord { row: 3, col: 1 },
        },
        surviving_member_count: 4,
        fragments: vec![
            PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 1,
                col: 1,
            },
            PlacementDomainTransport::RowRun {
                row_start: 2,
                row_end: 3,
                col: 1,
            },
        ],
        legacy_members: ExplicitPartitionLegacyMembers::try_new(Vec::new()).unwrap(),
        reconciliation: PartitionReconciliation {
            shared_members: 4,
            ordinary_exceptions: 0,
            holes: 0,
        },
    };
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow),
    );
    let report = engine
        .ingest_compressed_formula_source_batches(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceBatch::with_proposals(
                "Sheet1",
                FormulaCompressedSourceReport {
                    source_fragmentable_families: 1,
                    source_fragmentable_cells: 4,
                    ..FormulaCompressedSourceReport::default()
                },
                Vec::new(),
                vec![family],
            ),
        )])
        .unwrap();

    assert_eq!(report.source_partitioned_families_prepared, 0, "{report:?}");
    assert_eq!(report.source_partitioned_families_rejected, 1, "{report:?}");
    assert_eq!(report.source_partition_fragments_prepared, 0, "{report:?}");
    assert_eq!(report.shadow_accepted_span_cells, 0, "{report:?}");
    assert_eq!(report.shadow_fallback_cells, 4, "{report:?}");
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
}

#[test]
fn compressed_modes_accept_registry_resolved_nested_function_relocation() {
    fn family(text: &str) -> SourceFormulaFamily {
        SourceFormulaFamily {
            source_id: SourceFamilyId {
                sheet_instance: 91,
                source_index: 1,
            },
            anchor_coord0: SourceCoord { row: 0, col: 1 },
            anchor_text: Arc::from(text),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 99,
                col: 1,
            }),
            member_count: 100,
        }
    }

    let mut shadow = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow),
    );
    let report = shadow
        .ingest_compressed_formula_source_batches(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceBatch::with_families(
                "Sheet1",
                FormulaCompressedSourceReport::default(),
                vec![family("SUM(A1:A1)+_xlfn.ABS(A1)")],
            ),
        )])
        .unwrap();
    assert_eq!(report.source_compressed_families_prepared, 1, "{report:?}");
    assert_eq!(shadow.baseline_stats().formula_plane_active_span_count, 0);

    let mut authoritative = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    );
    for text in [
        "SUM(A1:A1)+_xlfn.ABS(A1)",
        "ROUND(A1,0)",
        "COUNTIF(A1:A1,\">0\")",
        "VLOOKUP(A1,A1:A1,1,FALSE)",
        "ROUND(ABS(A1)+SUM(A1:A1)+COUNTIF(A1:A1,\">0\")+VLOOKUP(A1,A1:A1,1,FALSE),0)",
    ] {
        let preparation = authoritative
            .source_formula_ingress()
            .prepare_families("Sheet1", &[family(text)])
            .unwrap();
        assert_eq!(
            preparation.direct_family_count(),
            1,
            "{text}: {:?}",
            preparation.rejected
        );
    }
}

#[test]
fn compressed_shadow_replays_when_runtime_provider_identity_mismatches_registry() {
    struct LocalAbs;
    impl crate::function::Function for LocalAbs {
        fn name(&self) -> &'static str {
            "ABS"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(9)))
        }
    }
    crate::builtins::load_builtins();
    let workbook = TestWorkbook::default().with_function(Arc::new(LocalAbs));
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut engine = Engine::new(workbook, cfg);
    let family = SourceFormulaFamily {
        source_id: SourceFamilyId {
            sheet_instance: 93,
            source_index: 1,
        },
        anchor_coord0: SourceCoord { row: 0, col: 1 },
        anchor_text: Arc::from("ABS(A1)"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 99,
            col: 1,
        }),
        member_count: 100,
    };
    let report = engine
        .ingest_compressed_formula_source_batches(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceBatch::with_families(
                "Sheet1",
                FormulaCompressedSourceReport::default(),
                vec![family],
            ),
        )])
        .unwrap();
    assert_eq!(report.source_compressed_families_prepared, 0, "{report:?}");
    assert_eq!(report.shadow_fallback_cells, 100, "{report:?}");
}

#[test]
fn authoritative_function_closure_admits_explicit_safe_custom_and_replays_untrusted_custom() {
    struct ExplicitSafe;
    impl crate::function::Function for ExplicitSafe {
        fn name(&self) -> &'static str {
            "SWATCH5_EXPLICIT_SAFE"
        }
        fn semantic_contract(
            &self,
            _arity: usize,
        ) -> Option<crate::function_contract::FunctionSemanticContract> {
            Some(crate::function_contract::FunctionSemanticContract::trusted_builtin_default(None))
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(7)))
        }
    }
    struct Untrusted;
    impl crate::function::Function for Untrusted {
        fn name(&self) -> &'static str {
            "SWATCH5_UNTRUSTED"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(9)))
        }
    }
    fn family(index: usize, text: &str) -> SourceFormulaFamily {
        SourceFormulaFamily {
            source_id: SourceFamilyId {
                sheet_instance: 95,
                source_index: index,
            },
            anchor_coord0: SourceCoord { row: 0, col: 1 },
            anchor_text: Arc::from(text),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 99,
                col: 1,
            }),
            member_count: 100,
        }
    }

    crate::function_registry::register_function(Arc::new(ExplicitSafe));
    crate::function_registry::register_function(Arc::new(Untrusted));
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    );
    let safe = engine
        .prepare_source_formula_families("Sheet1", &[family(1, "SWATCH5_EXPLICIT_SAFE()+A1")]);
    assert_eq!(safe.direct_family_count(), 1);
    let untrusted =
        engine.prepare_source_formula_families("Sheet1", &[family(2, "SWATCH5_UNTRUSTED()+A1")]);
    assert_eq!(untrusted.direct_family_count(), 0);
    assert_eq!(untrusted.rejected.len(), 1);
}

#[test]
fn authoritative_replays_every_exceptional_function_semantic_category() {
    for (source_index, text) in [
        (1, "RAND()+A1"),
        (2, "OFFSET(A1,0,0)"),
        (3, "LET(x,A1,x)"),
        (4, "UNREGISTERED_CLOSURE_FN(A1)"),
        (5, "SEQUENCE(1,1)"),
        (6, "CELL(\"filename\",A1)"),
        (7, "INDEX(A1:A2,1)"),
        (8, "ROW()"),
        (9, "IF(TRUE,A1,0)"),
        (10, "IFERROR(A1,0)"),
    ] {
        let mut engine = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default()
                .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
        );
        let family = SourceFormulaFamily {
            source_id: SourceFamilyId {
                sheet_instance: 96,
                source_index,
            },
            anchor_coord0: SourceCoord { row: 0, col: 1 },
            anchor_text: Arc::from(text),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 99,
                col: 1,
            }),
            member_count: 100,
        };
        let preparation = engine.prepare_source_formula_families("Sheet1", &[family]);
        assert_eq!(preparation.direct_family_count(), 0, "{text}");
        assert_eq!(preparation.rejected.len(), 1, "{text}");
    }
}

#[test]
fn compressed_shadow_replays_exceptional_and_unresolved_function_semantics() {
    for (source_index, text) in [
        (1, "RAND()+A1"),
        (2, "OFFSET(A1,0,0)"),
        (3, "LET(x,A1,x)"),
        (4, "UNREGISTERED_CLOSURE_FN(A1)"),
    ] {
        let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
        let mut engine = Engine::new(TestWorkbook::default(), cfg);
        let family = SourceFormulaFamily {
            source_id: SourceFamilyId {
                sheet_instance: 92,
                source_index,
            },
            anchor_coord0: SourceCoord { row: 0, col: 1 },
            anchor_text: Arc::from(text),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 99,
                col: 1,
            }),
            member_count: 100,
        };
        let report = engine
            .ingest_compressed_formula_source_batches(vec![(
                FormulaIngestBatch::new("Sheet1", Vec::new()),
                FormulaCompressedSourceBatch::with_families(
                    "Sheet1",
                    FormulaCompressedSourceReport::default(),
                    vec![family],
                ),
            )])
            .unwrap();
        assert_eq!(
            report.source_compressed_families_prepared, 0,
            "{text}: {report:?}"
        );
        assert_eq!(report.shadow_fallback_cells, 100, "{text}: {report:?}");
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    }
}

#[test]
fn compressed_shadow_counts_only_preparation_work_that_occurs() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let family = SourceFormulaFamily {
        source_id: SourceFamilyId {
            sheet_instance: 1,
            source_index: 1,
        },
        anchor_coord0: SourceCoord { row: 1, col: 0 },
        anchor_text: Arc::from("1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 2,
            col: 0,
        }),
        member_count: 3,
    };
    let batch = FormulaIngestBatch::new("Sheet1", Vec::new());
    let compressed = FormulaCompressedSourceBatch::with_families(
        "Sheet1",
        FormulaCompressedSourceReport::default(),
        vec![family],
    );

    let report = engine
        .ingest_compressed_formula_source_batches(vec![(batch, compressed)])
        .unwrap();

    assert_eq!(report.source_anchor_parses, 0);
    assert_eq!(report.source_anchor_asts, 0);
    assert_eq!(report.source_anchor_analyses, 0);
    assert_eq!(report.source_descendant_strings_avoided, 0);
    assert_eq!(report.source_descendant_events_avoided, 0);
    assert_eq!(report.source_descendant_analyses_avoided, 0);
    assert_eq!(
        report.fallback_reasons.get("CompleteDomainMemberMismatch"),
        Some(&1)
    );
}
#[test]
fn unrelated_semantic_epoch_change_does_not_replay_arithmetic_preparation() {
    crate::builtins::load_builtins();
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let family = SourceFormulaFamily {
        source_id: SourceFamilyId {
            sheet_instance: 1,
            source_index: 7,
        },
        anchor_coord0: SourceCoord { row: 0, col: 1 },
        anchor_text: Arc::from("A1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 99,
            col: 1,
        }),
        member_count: 100,
    };
    let replay = ExactTestReplay {
        formulas: (1..=100)
            .map(|row| {
                (
                    row,
                    2,
                    format!("=A{row}+1"),
                    Some(SourceFamilyId {
                        sheet_instance: 1,
                        source_index: 7,
                    }),
                )
            })
            .collect(),
    };
    let preparation = engine
        .source_formula_ingress()
        .prepare_families("Sheet1", &[family])
        .unwrap()
        .with_exact_replay(
            Arc::new(std::sync::Mutex::new(Box::new(replay))),
            BTreeSet::new(),
        );
    assert_eq!(preparation.direct_family_count(), 1);

    crate::function_registry::register_alias("", "__PREPARED_STALE_EPOCH_FIXTURE__", "", "ABS");
    let report = engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceReport {
                families_seen: 1,
                family_cells_seen: 100,
                ..Default::default()
            },
            preparation,
        )])
        .unwrap();

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert!(
        !report
            .fallback_reasons
            .contains_key("FunctionSemanticEpochChanged")
    );
}

#[test]
fn unrelated_commit_boundary_epoch_change_keeps_arithmetic_preparation() {
    crate::builtins::load_builtins();
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let family_id = SourceFamilyId {
        sheet_instance: 1,
        source_index: 8,
    };
    let family = SourceFormulaFamily {
        source_id: family_id,
        anchor_coord0: SourceCoord { row: 0, col: 1 },
        anchor_text: Arc::from("A1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 99,
            col: 1,
        }),
        member_count: 100,
    };
    let replay = ExactTestReplay {
        formulas: (1..=100)
            .map(|row| (row, 2, format!("=A{row}+1"), Some(family_id)))
            .collect(),
    };
    let preparation = engine
        .source_formula_ingress()
        .prepare_families("Sheet1", &[family])
        .unwrap()
        .with_exact_replay(
            Arc::new(std::sync::Mutex::new(Box::new(replay))),
            BTreeSet::new(),
        );
    engine.set_before_prepared_span_commit_hook(|| {
        crate::function_registry::register_alias("", "__PREPARED_COMMIT_RACE_FIXTURE__", "", "ABS");
    });

    engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceReport {
                families_seen: 1,
                family_cells_seen: 100,
                ..Default::default()
            },
            preparation,
        )])
        .unwrap();

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Number(1.0))
    );
}

#[test]
fn ordinary_supported_function_families_preserve_authoritative_behavior() {
    crate::builtins::load_builtins();
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let formulas = (1..=100)
        .map(|row| record(&mut engine, row, 2, &format!("=ABS(A{row})")))
        .collect();
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
}

#[test]
fn changed_function_ast_discovery_covers_stale_prefix_unresolved_and_calls() {
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    let changed = BTreeSet::from([(String::new(), "STALE_ALIAS".to_string())]);
    assert!(Engine::<TestWorkbook>::ast_uses_changed_function(
        &parse("=STALE_ALIAS(A1)").unwrap(),
        &changed
    ));

    let changed = BTreeSet::from([(String::new(), "SUM".to_string())]);
    assert!(Engine::<TestWorkbook>::ast_uses_changed_function(
        &parse("=_xlfn.SUM(A1)").unwrap(),
        &changed
    ));
    assert!(Engine::<TestWorkbook>::ast_uses_changed_function(
        &parse("=UNRESOLVED_CLOSURE_FN(A1)").unwrap(),
        &changed
    ));

    let callee_changed = ASTNode::new(
        ASTNodeType::Call {
            callee: Box::new(parse("=SUM(A1)").unwrap()),
            args: vec![parse("=ABS(A1)").unwrap()],
        },
        None,
    );
    assert!(Engine::<TestWorkbook>::ast_uses_changed_function(
        &callee_changed,
        &changed
    ));
    let changed = BTreeSet::from([(String::new(), "ABS".to_string())]);
    assert!(Engine::<TestWorkbook>::ast_uses_changed_function(
        &callee_changed,
        &changed
    ));
}

#[test]
fn semantic_epoch_guard_covers_public_formula_plane_flows() {
    let source = include_str!("../eval.rs");
    for signature in [
        "pub fn prepare_families(",
        "pub fn evaluate_vertex(",
        "pub fn evaluate_until(",
        "pub fn evaluate_recalc_plan(",
        "pub fn evaluate_all(",
        "pub fn evaluate_all_with_delta(",
        "pub fn evaluate_cells(",
        "pub fn evaluate_cells_cancellable(",
        "pub fn evaluate_cells_with_delta(",
        "pub fn evaluate_all_cancellable(",
        "pub fn evaluate_until_cancellable(",
        "pub fn evaluate_all_logged(",
        "fn ingest_formula_batches_inner(",
        "fn finish_eager_compressed_formula_sources(",
    ] {
        let start = source
            .find(signature)
            .unwrap_or_else(|| panic!("missing {signature}"));
        let body = &source[start..source.len().min(start + 1_200)];
        assert!(
            body.contains("observe_function_semantic_epoch"),
            "{signature} bypasses the common semantic epoch guard"
        );
    }
}

#[test]
fn unique_custom_replacement_publishes_epoch_without_parallel_builtin_contamination() {
    struct RaceFn;
    impl crate::function::Function for RaceFn {
        fn name(&self) -> &'static str {
            "__UNIQUE_EPOCH_RACE_FN__"
        }
        fn semantic_contract(
            &self,
            _arity: usize,
        ) -> Option<crate::function_contract::FunctionSemanticContract> {
            Some(crate::function_contract::FunctionSemanticContract::trusted_builtin_default(None))
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(1)))
        }
    }

    crate::function_registry::register_function(Arc::new(RaceFn));
    let before = crate::function_registry::semantic_epoch();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let writer_barrier = Arc::clone(&barrier);
    let writer = std::thread::spawn(move || {
        writer_barrier.wait();
        crate::function_registry::register_function(Arc::new(RaceFn));
        crate::function_registry::resolve_with_epoch("", "__UNIQUE_EPOCH_RACE_FN__").unwrap()
    });
    barrier.wait();
    let (published_epoch, resolved) = writer.join().unwrap();
    assert!(published_epoch > before);
    assert!(resolved.semantics.conforms());
}
