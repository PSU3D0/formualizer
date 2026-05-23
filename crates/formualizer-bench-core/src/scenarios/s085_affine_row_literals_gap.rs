use anyhow::Result;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric, sample_rows};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioInvariantMode, ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S085AffineRowLiteralsGap {
    scale: ScaleState,
}

impl Default for S085AffineRowLiteralsGap {
    fn default() -> Self {
        Self::new()
    }
}

impl S085AffineRowLiteralsGap {
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

    fn gap_start(rows: u32) -> u32 {
        rows / 2
    }

    fn gap_len(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 58,
            ScenarioScale::Medium | ScenarioScale::Large => 58,
        }
    }
}

impl Scenario for S085AffineRowLiteralsGap {
    fn id(&self) -> &'static str {
        "s085-affine-row-literals-gap"
    }

    fn description(&self) -> &'static str {
        "Affine row literal family with a hardcoded gap; should create two spans around the gap."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::SingleColumnFamily,
            ScenarioTag::SpanPromotable,
            ScenarioTag::EmptyGaps,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let gap_start = Self::gap_start(rows);
        let gap_end = gap_start + Self::gap_len(ctx.scale) - 1;
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sh.get_cell_mut((1, 1)).set_value_number(1.0);
            for r in 1..=rows {
                if (gap_start..=gap_end).contains(&r) {
                    sh.get_cell_mut((2, r)).set_value_number(42.0);
                } else {
                    sh.get_cell_mut((2, r)).set_formula(format!("=A$1*{r}"));
                }
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 1,
                formula_cells: rows - Self::gap_len(ctx.scale),
                value_cells: 1 + Self::gap_len(ctx.scale),
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let scale = self.scale.get_or_small();
        let rows = Self::rows(scale);
        let gap_start = Self::gap_start(rows);
        let gap_len = Self::gap_len(scale);
        let formula_cells = rows - gap_len;
        let mut out = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Auth),
            formula_plane_active_span_count: Some(2),
            graph_formula_vertex_count: Some(0),
            graph_edge_count: Some(0),
            formula_ast_root_count: Some(0),
        });
        out.push(ScenarioInvariant::EngineStats {
            mode: Some(ScenarioInvariantMode::Off),
            formula_plane_active_span_count: Some(0),
            graph_formula_vertex_count: Some(formula_cells as u64),
            graph_edge_count: Some(formula_cells as u64),
            formula_ast_root_count: Some(formula_cells as u64),
        });
        if has_evaluated_formulas(phase) {
            let gap_end = gap_start + gap_len - 1;
            for row in sample_rows(rows) {
                let expected = if (gap_start..=gap_end).contains(&row) {
                    42.0
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
                row: gap_start,
                col: 2,
                expected: numeric(42.0),
            });
        }
        out
    }
}
