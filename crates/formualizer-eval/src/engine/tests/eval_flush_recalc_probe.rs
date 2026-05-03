use super::common::arrow_eval_config;
use crate::arrow_store::OverlayDebugStats;
use crate::engine::eval::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[derive(Debug, Clone, Copy)]
enum RecalcEditKind {
    Multiplier,
    DenseUnits,
    SparsePrices,
}

impl RecalcEditKind {
    fn name(self) -> &'static str {
        match self {
            RecalcEditKind::Multiplier => "multiplier",
            RecalcEditKind::DenseUnits => "dense_units",
            RecalcEditKind::SparsePrices => "sparse_prices",
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct RepeatedEditRecalcProbeRow {
    cycle: usize,
    edit_kind: &'static str,
    rows: usize,
    eval_ms: f64,
    overlay_memory_usage: usize,
    recomputed_overlay_memory_usage: usize,
    compactions: u64,
    formula_points: usize,
    formula_sparse_fragments: usize,
    formula_dense_fragments: usize,
    formula_run_fragments: usize,
    formula_covered_len: usize,
    rollup: f64,
    expected_rollup: f64,
}

struct FinanceRecalcFixture {
    engine: Engine<TestWorkbook>,
    units: Vec<f64>,
    prices: Vec<f64>,
    multiplier: f64,
}

impl FinanceRecalcFixture {
    fn new(rows: usize, chunk_rows: usize) -> Self {
        let mut cfg = arrow_eval_config();
        cfg.enable_parallel = false;
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        let mut units = Vec::with_capacity(rows);
        let mut prices = Vec::with_capacity(rows);
        let sheet = "Sheet1";

        {
            let mut ab = engine.begin_bulk_ingest_arrow();
            ab.add_sheet(sheet, 7, chunk_rows.max(1));
            for row0 in 0..rows {
                let unit = (row0 + 1) as f64;
                let price = 10.0 + (row0 % 17) as f64;
                units.push(unit);
                prices.push(price);
                ab.append_row(
                    sheet,
                    &[
                        LiteralValue::Number(unit),
                        LiteralValue::Number(price),
                        LiteralValue::Empty,
                        LiteralValue::Empty,
                        LiteralValue::Empty,
                        if row0 == 0 {
                            LiteralValue::Number(1.0)
                        } else {
                            LiteralValue::Empty
                        },
                        LiteralValue::Empty,
                    ],
                )
                .unwrap();
            }
            ab.finish().unwrap();
        }

        for row in 1..=rows {
            let formula = parse(format!("=A{row}*B{row}*$F$1")).unwrap();
            engine
                .set_cell_formula(sheet, row as u32, 3, formula)
                .unwrap();
        }
        engine
            .set_cell_formula(sheet, 1, 7, parse(format!("=SUM(C1:C{rows})")).unwrap())
            .unwrap();

        Self {
            engine,
            units,
            prices,
            multiplier: 1.0,
        }
    }

    fn apply_edit_cycle(&mut self, cycle: usize) -> RecalcEditKind {
        match cycle % 3 {
            0 => {
                self.multiplier = 1.0 + ((cycle % 5) as f64);
                self.engine
                    .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(self.multiplier))
                    .unwrap();
                RecalcEditKind::Multiplier
            }
            1 => {
                let start = (cycle * 37) % self.units.len().max(1);
                let len = self.units.len().min(16);
                for idx in 0..len {
                    let row0 = (start + idx) % self.units.len();
                    let value = 1000.0 + cycle as f64 + idx as f64;
                    self.units[row0] = value;
                    self.engine
                        .set_cell_value("Sheet1", row0 as u32 + 1, 1, LiteralValue::Number(value))
                        .unwrap();
                }
                RecalcEditKind::DenseUnits
            }
            _ => {
                let stride = 97usize;
                let edits = self.prices.len().min(16);
                for idx in 0..edits {
                    let row0 = (cycle * 53 + idx * stride) % self.prices.len();
                    let value = 20.0 + ((cycle + idx) % 23) as f64;
                    self.prices[row0] = value;
                    self.engine
                        .set_cell_value("Sheet1", row0 as u32 + 1, 2, LiteralValue::Number(value))
                        .unwrap();
                }
                RecalcEditKind::SparsePrices
            }
        }
    }

    fn expected_rollup(&self) -> f64 {
        self.units
            .iter()
            .zip(self.prices.iter())
            .map(|(unit, price)| unit * price * self.multiplier)
            .sum()
    }

    fn rollup(&self) -> f64 {
        match self.engine.get_cell_value("Sheet1", 1, 7) {
            Some(LiteralValue::Number(value)) => value,
            Some(other) => panic!("expected numeric rollup, got {other:?}"),
            None => panic!("missing rollup"),
        }
    }

    fn formula_overlay_stats(&self) -> OverlayDebugStats {
        let sheet = self.engine.sheet_store().sheet("Sheet1").unwrap();
        let column = &sheet.columns[2];
        let mut total = OverlayDebugStats::default();
        for chunk in &column.chunks {
            let stats = chunk.computed_overlay.debug_stats();
            total.points += stats.points;
            total.sparse_fragments += stats.sparse_fragments;
            total.dense_fragments += stats.dense_fragments;
            total.run_fragments += stats.run_fragments;
            total.covered_len += stats.covered_len;
        }
        for chunk in column.sparse_chunks.values() {
            let stats = chunk.computed_overlay.debug_stats();
            total.points += stats.points;
            total.sparse_fragments += stats.sparse_fragments;
            total.dense_fragments += stats.dense_fragments;
            total.run_fragments += stats.run_fragments;
            total.covered_len += stats.covered_len;
        }
        total
    }

    fn evaluate_cycle(
        &mut self,
        cycle: usize,
        edit_kind: RecalcEditKind,
    ) -> RepeatedEditRecalcProbeRow {
        let start = std::time::Instant::now();
        self.engine.evaluate_all().unwrap();
        let eval_ms = start.elapsed().as_secs_f64() * 1000.0;
        let expected_rollup = self.expected_rollup();
        let rollup = self.rollup();
        assert!(
            (rollup - expected_rollup).abs() < 1e-6,
            "cycle {cycle}: rollup {rollup} != expected {expected_rollup}"
        );
        let overlay_memory_usage = self.engine.overlay_memory_usage();
        let recomputed_overlay_memory_usage = self.engine.debug_recompute_computed_overlay_bytes();
        assert_eq!(overlay_memory_usage, recomputed_overlay_memory_usage);
        let stats = self.formula_overlay_stats();

        RepeatedEditRecalcProbeRow {
            cycle,
            edit_kind: edit_kind.name(),
            rows: self.units.len(),
            eval_ms,
            overlay_memory_usage,
            recomputed_overlay_memory_usage,
            compactions: self.engine.debug_overlay_compactions(),
            formula_points: stats.points,
            formula_sparse_fragments: stats.sparse_fragments,
            formula_dense_fragments: stats.dense_fragments,
            formula_run_fragments: stats.run_fragments,
            formula_covered_len: stats.covered_len,
            rollup,
            expected_rollup,
        }
    }
}

#[test]
fn repeated_edit_recalc_keeps_computed_overlays_bounded_and_correct() {
    let rows = 256usize;
    let mut fixture = FinanceRecalcFixture::new(rows, 64);
    fixture.engine.evaluate_all().unwrap();

    for cycle in 0..8 {
        let edit_kind = fixture.apply_edit_cycle(cycle);
        let row = fixture.evaluate_cycle(cycle, edit_kind);
        assert!(
            row.formula_points <= 32,
            "cycle {cycle}: sparse incremental edits should not degrade into a large point map: {row:?}"
        );
        assert_eq!(row.formula_covered_len, rows, "cycle {cycle}");
        assert!(
            row.formula_dense_fragments > 0
                || row.formula_run_fragments > 0
                || row.formula_sparse_fragments > 0,
            "cycle {cycle}: expected coalesced fragments, got {row:?}"
        );
        assert!(
            row.overlay_memory_usage < 8 * 1024 * 1024,
            "cycle {cycle}: overlay memory unexpectedly high: {}",
            row.overlay_memory_usage
        );
    }
}

#[test]
#[ignore = "manual repeated edit/recalc overlay probe; run with --ignored --nocapture"]
fn repeated_edit_recalc_overlay_observability_probe() {
    let rows = std::env::var("FORMUALIZER_RECALC_PROBE_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10_000)
        .max(1);
    let cycles = std::env::var("FORMUALIZER_RECALC_PROBE_CYCLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .max(1);
    let chunk_rows = std::env::var("FORMUALIZER_RECALC_PROBE_CHUNK_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(32 * 1024)
        .max(1);

    let mut fixture = FinanceRecalcFixture::new(rows, chunk_rows);
    let initial = std::time::Instant::now();
    fixture.engine.evaluate_all().unwrap();
    eprintln!(
        "initial_evaluate_ms={:.3}",
        initial.elapsed().as_secs_f64() * 1000.0
    );

    for cycle in 0..cycles {
        let edit_kind = fixture.apply_edit_cycle(cycle);
        let row = fixture.evaluate_cycle(cycle, edit_kind);
        println!("{}", serde_json::to_string(&row).unwrap());
    }
}
