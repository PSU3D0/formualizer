use anyhow::Result;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric, sample_rows};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioInvariantMode, ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S083AffineRowLiteralsSingleOutlier {
    scale: ScaleState,
}

impl Default for S083AffineRowLiteralsSingleOutlier {
    fn default() -> Self {
        Self::new()
    }
}

impl S083AffineRowLiteralsSingleOutlier {
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

    fn outlier_row(rows: u32) -> u32 {
        rows / 2
    }
}

impl Scenario for S083AffineRowLiteralsSingleOutlier {
    fn id(&self) -> &'static str {
        "s083-affine-row-literals-single-outlier"
    }

    fn description(&self) -> &'static str {
        "Mostly affine row literal family with one outlier; should split into two spans and one graph fallback."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::SpanPromotable]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let outlier = Self::outlier_row(rows);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sh.get_cell_mut((1, 1)).set_value_number(1.0);
            for r in 1..=rows {
                let literal = if r == outlier { 999_999 } else { r };
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
        let rows = Self::rows(self.scale.get_or_small());
        let outlier = Self::outlier_row(rows);
        let mut out = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Auth),
            formula_plane_active_span_count: Some(2),
            graph_formula_vertex_count: Some(1),
            graph_edge_count: Some(1),
            formula_ast_root_count: Some(1),
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
                let expected = if row == outlier {
                    999_999.0
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
                row: outlier,
                col: 2,
                expected: numeric(999_999.0),
            });
        }
        out
    }
}
