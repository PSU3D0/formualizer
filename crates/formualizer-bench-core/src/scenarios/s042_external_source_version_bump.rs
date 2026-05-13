use anyhow::{Result, bail};
use formualizer_workbook::Workbook;

use super::common::ScaleState;
use super::{
    EditPlan, ExpectedFailure, ExpectedFailureMode, FixtureMetadata, Scenario, ScenarioBuildCtx,
    ScenarioFixture, ScenarioInvariant, ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S042ExternalSourceVersionBump {
    scale: ScaleState,
}

impl Default for S042ExternalSourceVersionBump {
    fn default() -> Self {
        Self::new()
    }
}

impl S042ExternalSourceVersionBump {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 1_000,
            ScenarioScale::Medium => 10_000,
            ScenarioScale::Large => 50_000,
        }
    }
}

impl Scenario for S042ExternalSourceVersionBump {
    fn id(&self) -> &'static str {
        "s042-external-source-version-bump"
    }

    fn description(&self) -> &'static str {
        "Planned external source version-bump scenario; escalates because XLSX Workbook fixtures cannot declare external sources and WBResolver has no value source API."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::Mixed]
    }

    fn expected_to_fail_under(&self) -> &'static [ExpectedFailure] {
        &[
            ExpectedFailure {
                mode: ExpectedFailureMode::OffOnly,
                reason: "Workbook (XLSX path via UmyaAdapter) has no public API to declare/populate external sources. JSON-backed Workbook can; XLSX cannot. PM follow-up: add cross-backend external-source surface or skip this scenario.",
            },
            ExpectedFailure {
                mode: ExpectedFailureMode::AuthOnly,
                reason: "Workbook (XLSX path via UmyaAdapter) has no public API to declare/populate external sources. JSON-backed Workbook can; XLSX cannot. PM follow-up: add cross-backend external-source surface or skip this scenario.",
            },
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        bail!(
            "s042 cannot be implemented with the current public Workbook/XLSX path: Engine exposes define_source_scalar/table and set_source_*_version, and the JSON backend can declare sources, but probe-corpus always loads UmyaAdapter XLSX fixtures; Workbook/WBResolver expose no public API to declare/populate SourceA!A{{r}} values during fixture load"
        )
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 5,
            apply: apply_edit,
        })
    }

    fn invariants(&self, _phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        Vec::new()
    }
}

fn apply_edit(_wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    bail!(
        "s042 edit cycle unavailable: no loaded external SourceA workbook source exists to version-bump"
    )
}

#[allow(dead_code)]
fn _metadata_if_supported(rows: u32) -> FixtureMetadata {
    FixtureMetadata {
        rows,
        cols: 1,
        sheets: 1,
        formula_cells: rows,
        value_cells: 0,
        has_named_ranges: false,
        has_tables: false,
    }
}
