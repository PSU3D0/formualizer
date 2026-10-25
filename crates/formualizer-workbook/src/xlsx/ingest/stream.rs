//! Shared engine installation and temporary load configuration.
//! Callers retain admission and cancellation ordering; source bytes are never edited.
use super::staging::engine_error;
use formualizer_eval::arrow_store::ArrowSheet;
use formualizer_eval::engine::{
    DeferredFormulaPackage, Engine, FormulaCompressedPreparation, FormulaCompressedSourceBatch,
    FormulaCompressedSourceReport, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
    PartitionedSourceFormulaFamily, SheetIndexMode, SourceFormulaFamily,
};
use formualizer_eval::traits::EvaluationContext;

pub(crate) struct LoadConfig {
    index: SheetIndexMode,
    range_limit: usize,
    first_load: bool,
    deferred: bool,
    formula_plane: FormulaPlaneMode,
}

impl LoadConfig {
    pub(crate) fn begin<C: EvaluationContext>(
        engine: &mut Engine<C>,
        external_values: bool,
        force_eager: bool,
    ) -> Self {
        let previous = Self {
            index: engine.config.sheet_index_mode,
            range_limit: engine.config.range_expansion_limit,
            first_load: engine.first_load_assume_new(),
            deferred: engine.config.defer_graph_building,
            formula_plane: engine.config.formula_plane_mode,
        };
        engine.set_sheet_index_mode(SheetIndexMode::Lazy);
        engine.config.range_expansion_limit = 0;
        if external_values {
            engine.config.formula_plane_mode = FormulaPlaneMode::Off;
        }
        if force_eager {
            engine.config.defer_graph_building = false;
        }
        engine.set_first_load_assume_new(true);
        engine.reset_ensure_touched();
        previous
    }

    pub(crate) fn restore<C: EvaluationContext>(self, engine: &mut Engine<C>) {
        engine.set_first_load_assume_new(self.first_load);
        engine.reset_ensure_touched();
        engine.set_sheet_index_mode(self.index);
        engine.config.range_expansion_limit = self.range_limit;
        engine.config.defer_graph_building = self.deferred;
        engine.config.formula_plane_mode = self.formula_plane;
    }
}

pub(crate) fn install_values<C: EvaluationContext>(
    engine: &mut Engine<C>,
    sheet: &str,
    values: ArrowSheet,
) {
    let store = engine.sheet_store_mut();
    if let Some(index) = store.sheets.iter().position(|s| s.name.as_ref() == sheet) {
        store.sheets[index] = values;
    } else {
        store.sheets.push(values);
    }
}

pub(crate) struct SheetFormulas {
    pub(crate) records: Vec<FormulaIngestRecord>,
    pub(crate) report: FormulaCompressedSourceReport,
    pub(crate) families: Vec<SourceFormulaFamily>,
    pub(crate) partitioned: Vec<PartitionedSourceFormulaFamily>,
    pub(crate) preparation: Option<FormulaCompressedPreparation>,
    pub(crate) deferred: Option<DeferredFormulaPackage>,
}

#[derive(Default)]
pub(crate) struct FormulaBatches {
    pub(crate) replay: Vec<(FormulaIngestBatch, FormulaCompressedSourceBatch)>,
    pub(crate) direct: Vec<(
        FormulaIngestBatch,
        FormulaCompressedSourceReport,
        FormulaCompressedPreparation,
    )>,
}

impl FormulaBatches {
    pub(crate) fn push<C: EvaluationContext>(
        &mut self,
        engine: &mut Engine<C>,
        sheet: &str,
        formulas: SheetFormulas,
    ) {
        if engine.config.defer_graph_building {
            if let Some(package) = formulas.deferred {
                engine.source_formula_ingress().stage_deferred(package);
            }
        } else if !formulas.records.is_empty() || formulas.report.source_formula_events != 0 {
            let batch = FormulaIngestBatch::new(sheet.to_owned(), formulas.records);
            if let Some(preparation) = formulas.preparation {
                self.direct.push((batch, formulas.report, preparation));
            } else {
                self.replay.push((
                    batch,
                    FormulaCompressedSourceBatch::with_proposals(
                        sheet.to_owned(),
                        formulas.report,
                        formulas.families,
                        formulas.partitioned,
                    ),
                ));
            }
        }
    }

    pub(crate) fn install_replay<C: EvaluationContext>(
        &mut self,
        engine: &mut Engine<C>,
    ) -> Result<(), crate::xlsx::types::XlsxReadError> {
        engine
            .source_formula_ingress()
            .ingest_replay_batches(std::mem::take(&mut self.replay))
            .map(|_| ())
            .map_err(engine_error)
    }

    pub(crate) fn install_direct<C: EvaluationContext>(
        &mut self,
        engine: &mut Engine<C>,
    ) -> Result<(), crate::xlsx::types::XlsxReadError> {
        engine
            .source_formula_ingress()
            .finish_prepared(std::mem::take(&mut self.direct))
            .map(|_| ())
            .map_err(engine_error)
    }
}
