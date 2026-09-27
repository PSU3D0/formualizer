//! Load-time family grouping for the adapters that hand the engine parsed
//! formula batches (umya, JSON), as the calamine loader does (decision 26):
//! a relative copy of the formula above (or to the left) is staged as a
//! member of that formula's family and never interned; other formulas are
//! parsed once per distinct text.
use formualizer_common::error::ExcelError;
use formualizer_eval::engine::{AstNodeId, Engine, FormulaFamilyGrouper, FormulaIngestRecord};
use formualizer_eval::traits::EvaluationContext;
use std::sync::Arc;

pub(crate) struct GroupedFormulaStaging {
    grouper: FormulaFamilyGrouper,
    parse_cache: rustc_hash::FxHashMap<String, Option<AstNodeId>>,
}

impl GroupedFormulaStaging {
    /// One staging per sheet.
    pub(crate) fn new() -> Self {
        Self {
            grouper: FormulaFamilyGrouper::new(),
            parse_cache: rustc_hash::FxHashMap::default(),
        }
    }

    /// Stage the formula `text` at 1-based `(row, col)` of `sheet`; `None`
    /// when a parse error is dropped by the parse policy.
    pub(crate) fn stage<R: EvaluationContext>(
        &mut self,
        engine: &mut Engine<R>,
        sheet: &str,
        row: u32,
        col: u32,
        text: &str,
    ) -> Result<Option<FormulaIngestRecord>, ExcelError> {
        let with_eq = if text.starts_with('=') {
            text.to_string()
        } else {
            format!("={text}")
        };
        if let Some(cached) = self.parse_cache.get(&with_eq) {
            return Ok(cached.map(|ast_id| {
                engine.note_staged_formula(&mut self.grouper, row, col, ast_id);
                FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(with_eq)))
            }));
        }
        match formualizer_parse::parser::parse(&with_eq) {
            Ok(parsed) => {
                let record = engine.stage_formula_ast(&mut self.grouper, row, col, &parsed, None);
                // A member's text is not worth caching: relative copies do
                // not repeat their text.
                if record.is_family_member() {
                    return Ok(Some(record));
                }
                let ast_id = record.ast_id;
                self.parse_cache.insert(with_eq.clone(), Some(ast_id));
                Ok(Some(FormulaIngestRecord::new(
                    row,
                    col,
                    ast_id,
                    Some(Arc::<str>::from(with_eq)),
                )))
            }
            Err(error) => {
                // A recovered formula is interned on its own (as before).
                let recovered = engine.handle_formula_parse_error(
                    sheet,
                    row,
                    col,
                    &with_eq,
                    error.to_string(),
                )?;
                Ok(recovered.map(|ast| {
                    let ast_id = engine.intern_formula_ast(&ast);
                    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(with_eq)))
                }))
            }
        }
    }
}
