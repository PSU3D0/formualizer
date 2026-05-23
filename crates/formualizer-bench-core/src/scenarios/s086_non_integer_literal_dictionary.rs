use anyhow::Result;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric, sample_rows};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioInvariantMode, ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S086NonIntegerLiteralDictionary {
    scale: ScaleState,
}

impl Default for S086NonIntegerLiteralDictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl S086NonIntegerLiteralDictionary {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 1_000,
            ScenarioScale::Medium => 10_000,
            // Keep dictionary payload under the binding cap even at large scale.
            ScenarioScale::Large => 20_000,
        }
    }
}

impl Scenario for S086NonIntegerLiteralDictionary {
    fn id(&self) -> &'static str {
        "s086-non-integer-literal-dictionary"
    }

    fn description(&self) -> &'static str {
        "Unique non-integer numeric literals should remain dictionary encoded rather than affine-compressed."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::SpanPromotable]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sh.get_cell_mut((1, 1)).set_value_number(1.0);
            for r in 1..=rows {
                sh.get_cell_mut((2, r)).set_formula(format!("=A$1*{r}.5"));
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
        let mut out = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Auth),
            formula_plane_active_span_count: Some(1),
            graph_formula_vertex_count: Some(0),
            graph_edge_count: Some(0),
            formula_ast_root_count: Some(0),
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
                out.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(row as f64 + 0.5),
                });
            }
        }
        out
    }
}
