use anyhow::Result;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric, sample_rows};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioInvariantMode, ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S084AffineRowLiteralsPeriodicOutliers {
    scale: ScaleState,
}

impl Default for S084AffineRowLiteralsPeriodicOutliers {
    fn default() -> Self {
        Self::new()
    }
}

impl S084AffineRowLiteralsPeriodicOutliers {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 1_000,
            ScenarioScale::Medium => 10_000,
            ScenarioScale::Large => 100_000,
        }
    }

    fn period(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 250,
            ScenarioScale::Medium => 2_500,
            ScenarioScale::Large => 5_000,
        }
    }
}

impl Scenario for S084AffineRowLiteralsPeriodicOutliers {
    fn id(&self) -> &'static str {
        "s084-affine-row-literals-periodic-outliers"
    }

    fn description(&self) -> &'static str {
        "Affine row literal family with periodic outliers; should make maximal affine spans and fallback only outliers."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::SpanPromotable]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let period = Self::period(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sh.get_cell_mut((1, 1)).set_value_number(1.0);
            for r in 1..=rows {
                let literal = if r % period == 0 { 1_000_000 + r } else { r };
                sh.get_cell_mut((2, r))
                    .set_formula(format!("=A$1*{literal}"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 1,
                formula_cells: rows,
                value_cells: 1,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let scale = self.scale.get_or_small();
        let rows = Self::rows(scale);
        let period = Self::period(scale);
        let outliers = rows / period;
        let expected_spans = outliers + u32::from(!rows.is_multiple_of(period));
        let mut out = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Auth),
            formula_plane_active_span_count: Some(expected_spans as u64),
            graph_formula_vertex_count: Some(outliers as u64),
            graph_edge_count: Some(outliers as u64),
            formula_ast_root_count: Some(outliers as u64),
        });
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Off),
            formula_plane_active_span_count: Some(0),
            graph_formula_vertex_count: Some(rows as u64),
            graph_edge_count: Some(rows as u64),
            formula_ast_root_count: Some(rows as u64),
        });
        if has_evaluated_formulas(phase) {
            for row in sample_rows(rows) {
                let expected = if row % period == 0 {
                    1_000_000.0 + row as f64
                } else {
                    row as f64
                };
                out.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(expected),
                });
            }
            out.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row: period,
                col: 2,
                expected: numeric(1_000_000.0 + period as f64),
            });
        }
        out
    }
}
