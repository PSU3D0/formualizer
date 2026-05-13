use anyhow::Result;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric, sample_rows};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioInvariantMode, ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S082AffineColLiterals {
    scale: ScaleState,
}

impl Default for S082AffineColLiterals {
    fn default() -> Self {
        Self::new()
    }
}

impl S082AffineColLiterals {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn cols(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 1_000,
            ScenarioScale::Medium => 10_000,
            ScenarioScale::Large => 16_000,
        }
    }
}

impl Scenario for S082AffineColLiterals {
    fn id(&self) -> &'static str {
        "s082-affine-col-literals"
    }

    fn description(&self) -> &'static str {
        "Column-wise affine literal family =$A$1*{col}; currently documents that ingest grouping keeps this path legacy."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::SpanPromotable]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let cols = Self::cols(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sh.get_cell_mut((1, 1)).set_value_number(1.0);
            for c in 1..=cols {
                sh.get_cell_mut((c, 2)).set_formula(format!("=$A$1*{c}"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: 2,
                cols,
                sheets: 1,
                formula_cells: cols,
                value_cells: 1,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let cols = Self::cols(self.scale.get_or_small());
        let mut out = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Auth),
            formula_plane_active_span_count: Some(0),
            graph_formula_vertex_count: Some(cols as u64),
            graph_edge_count: Some(cols as u64),
            formula_ast_root_count: Some(cols as u64),
        });
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Off),
            formula_plane_active_span_count: Some(0),
            graph_formula_vertex_count: Some(cols as u64),
            graph_edge_count: Some(cols as u64),
            formula_ast_root_count: Some(cols as u64),
        });
        if has_evaluated_formulas(phase) {
            for col in sample_rows(cols) {
                out.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row: 2,
                    col,
                    expected: numeric(col as f64),
                });
            }
        }
        out
    }
}
