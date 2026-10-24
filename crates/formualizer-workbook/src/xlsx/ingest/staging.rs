//! Formula parsing and family grouping shared by source adapters.
//! Evaluation text is normalized here without changing source text or marks.
use formualizer_eval::engine::{AstNodeId, Engine, FormulaFamilyGrouper, FormulaIngestRecord};
use formualizer_eval::traits::EvaluationContext;
use rustc_hash::FxHashMap;
use std::sync::Arc;

#[derive(Clone, Copy, Default)]
pub(crate) struct WorkbookSpoolUsage {
    pub(crate) bytes: u64,
    pub(crate) files: u32,
}

pub(crate) struct FormulaStaging {
    parse_cache: FxHashMap<String, Option<AstNodeId>>,
    pub(crate) formulas: Vec<FormulaIngestRecord>,
    pub(crate) grouper: FormulaFamilyGrouper,
    pub(crate) observed: usize,
    pub(crate) handed_to_engine: usize,
}

impl FormulaStaging {
    pub(crate) fn new() -> Self {
        let mut parse_cache = FxHashMap::default();
        parse_cache.reserve(4096);
        Self {
            parse_cache,
            formulas: Vec::new(),
            grouper: FormulaFamilyGrouper::new(),
            observed: 0,
            handed_to_engine: 0,
        }
    }

    pub(crate) fn stage<C: EvaluationContext>(
        &mut self,
        engine: &mut Engine<C>,
        sheet: &str,
        position: (u32, u32),
        formula: &str,
        shared: Option<usize>,
        debug: bool,
    ) -> Result<(), crate::xlsx::types::XlsxReadError> {
        let excel_row = position.0 + 1;
        let excel_col = position.1 + 1;
        let normalized = if formula.starts_with('=') {
            formula.to_string()
        } else {
            format!("={formula}")
        };
        if debug && self.observed < 16 {
            eprintln!("[fz][load] formula observed at R{excel_row}C{excel_col}");
        }
        if engine.config.defer_graph_building {
            engine.stage_formula_text(sheet, excel_row, excel_col, normalized);
            self.handed_to_engine += 1;
        } else {
            self.grouper.note_formula();
            let record = if let Some(cached) = self.parse_cache.get(&normalized) {
                self.grouper.note_parse_cache_hit();
                cached.map(|ast_id| {
                    engine.note_staged_formula(&mut self.grouper, excel_row, excel_col, ast_id);
                    FormulaIngestRecord::new(
                        excel_row,
                        excel_col,
                        ast_id,
                        Some(Arc::<str>::from(normalized.as_str())),
                    )
                })
            } else if let Some(record) = engine.stage_relocated_text(
                &mut self.grouper,
                excel_row,
                excel_col,
                &normalized,
                shared.map(|index| index as u64),
            ) {
                Some(record)
            } else {
                self.grouper.note_parse(normalized.len());
                let parsed = formualizer_parse::parser::parse(&normalized);
                let parsed_text = parsed.is_ok();
                let parsed = match parsed {
                    Ok(parsed) => Some(parsed),
                    Err(error) => engine
                        .handle_formula_parse_error(
                            sheet,
                            excel_row,
                            excel_col,
                            &normalized,
                            error.to_string(),
                        )
                        .map_err(engine_error)?,
                };
                match parsed {
                    Some(ast) => {
                        let record = engine.stage_formula_ast(
                            &mut self.grouper,
                            excel_row,
                            excel_col,
                            &ast,
                            None,
                        );
                        if parsed_text {
                            engine.note_parsed_text(
                                &mut self.grouper,
                                excel_row,
                                excel_col,
                                &normalized,
                                &ast,
                                &record,
                                shared.map(|index| index as u64),
                            );
                        }
                        // Relative family members need no repeated text cache.
                        if record.is_family_member() {
                            Some(record)
                        } else {
                            let ast_id = record.ast_id;
                            self.parse_cache.insert(normalized.clone(), Some(ast_id));
                            Some(FormulaIngestRecord::new(
                                excel_row,
                                excel_col,
                                ast_id,
                                Some(Arc::<str>::from(normalized)),
                            ))
                        }
                    }
                    None => {
                        self.parse_cache.insert(normalized, None);
                        None
                    }
                }
            };
            if let Some(record) = record {
                self.formulas.push(record);
                self.handed_to_engine += 1;
            }
        }
        self.observed += 1;
        Ok(())
    }
}

pub(crate) fn engine_error(error: impl std::fmt::Display) -> crate::xlsx::types::XlsxReadError {
    crate::xlsx::types::XlsxReadError::Io(std::io::Error::other(error.to_string()))
}
