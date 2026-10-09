//! Load-time formula families from source text: the selection switch and
//! the counters loaders report under `FZ_DEBUG_LOAD` / `FZ_DEBUG_RECALC`.

use std::sync::{Arc, Mutex};

use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use formualizer_parse::template_lex::{RelocatedMatch, SlotRef};

use super::formula_ingest::GroupedFamily;
use super::{Engine, FormulaFamilyGrouper, FormulaIngestRecord};
use crate::traits::EvaluationContext;

/// How load-time staging treats formula text that may be a relocated copy
/// of a formula already parsed on the same sheet. Selected per engine from
/// `FZ_SOURCE_FAMILIES` (`off`/`0`: `Off`; `oracle`: `Oracle`; otherwise
/// `On`) and overridable with `Engine::set_source_family_mode`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourceFamilyMode {
    /// Parse every formula (the previous behaviour).
    Off,
    /// Stage proven copies without parsing them.
    #[default]
    On,
    /// As `On`, and also parse every proven copy and check it against its
    /// template; a mismatch is counted, reported and parsed instead.
    Oracle,
}

impl SourceFamilyMode {
    pub fn from_env() -> Self {
        match std::env::var("FZ_SOURCE_FAMILIES") {
            Ok(v) if v == "0" || v.eq_ignore_ascii_case("off") => Self::Off,
            Ok(v) if v.eq_ignore_ascii_case("oracle") => Self::Oracle,
            _ => Self::On,
        }
    }
}

/// Load-time formula staging counters.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceFamilyCounters {
    /// Formula texts staged.
    pub formulas: u64,
    /// Formula texts parsed, and their bytes.
    pub parse_calls: u64,
    pub parse_bytes: u64,
    /// Formula texts answered by the per-load parse cache.
    pub parse_cache_hits: u64,
    /// Parsed formulas that became members of an adjacent family.
    pub parsed_members: u64,
    /// Parsed templates certified for relocation, and those that were not.
    pub templates_certified: u64,
    pub templates_uncertified: u64,
    /// Proven members staged without parsing: through shared-formula
    /// identity, and through adjacency.
    pub shared_members: u64,
    pub adjacent_members: u64,
    /// Unparsed candidates that fell back to parsing, by reason.
    pub fallback_no_template: u64,
    pub fallback_mismatch: u64,
    pub fallback_off_grid: u64,
    /// Oracle mode: proven members checked by parsing, and mismatches.
    pub oracle_checked: u64,
    pub oracle_mismatches: u64,
}

static PROCESS_TOTALS: Mutex<SourceFamilyCounters> = Mutex::new(SourceFamilyCounters {
    formulas: 0,
    parse_calls: 0,
    parse_bytes: 0,
    parse_cache_hits: 0,
    parsed_members: 0,
    templates_certified: 0,
    templates_uncertified: 0,
    shared_members: 0,
    adjacent_members: 0,
    fallback_no_template: 0,
    fallback_mismatch: 0,
    fallback_off_grid: 0,
    oracle_checked: 0,
    oracle_mismatches: 0,
});

/// Totals over every engine of this process (development tools).
#[doc(hidden)]
pub fn source_family_process_totals() -> SourceFamilyCounters {
    *PROCESS_TOTALS.lock().unwrap_or_else(|e| e.into_inner())
}

impl SourceFamilyCounters {
    pub fn accumulate(&mut self, o: &Self) {
        self.formulas += o.formulas;
        self.parse_calls += o.parse_calls;
        self.parse_bytes += o.parse_bytes;
        self.parse_cache_hits += o.parse_cache_hits;
        self.parsed_members += o.parsed_members;
        self.templates_certified += o.templates_certified;
        self.templates_uncertified += o.templates_uncertified;
        self.shared_members += o.shared_members;
        self.adjacent_members += o.adjacent_members;
        self.fallback_no_template += o.fallback_no_template;
        self.fallback_mismatch += o.fallback_mismatch;
        self.fallback_off_grid += o.fallback_off_grid;
        self.oracle_checked += o.oracle_checked;
        self.oracle_mismatches += o.oracle_mismatches;
    }

    pub(crate) fn publish(&self) {
        PROCESS_TOTALS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .accumulate(self);
    }

    /// One `key=value` line for debug output.
    pub fn debug_line(&self) -> String {
        format!(
            "formulas={} parse_calls={} parse_bytes={} cache_hits={} parsed_members={} \
             templates_certified={} templates_uncertified={} shared_members={} \
             adjacent_members={} fallback_no_template={} fallback_mismatch={} \
             fallback_off_grid={} oracle_checked={} oracle_mismatches={}",
            self.formulas,
            self.parse_calls,
            self.parse_bytes,
            self.parse_cache_hits,
            self.parsed_members,
            self.templates_certified,
            self.templates_uncertified,
            self.shared_members,
            self.adjacent_members,
            self.fallback_no_template,
            self.fallback_mismatch,
            self.fallback_off_grid,
            self.oracle_checked,
            self.oracle_mismatches,
        )
    }
}

/// A parsed family template certified for lexical relocation: its source
/// text and reference slots. A formula text that is this text with every
/// slot relocated by the offset from the template's anchor parses to the
/// template instantiated at that cell (see `certify_template`).
#[derive(Debug)]
pub(crate) struct LexicalTemplate {
    text: Box<str>,
    lexeme: formualizer_parse::template_lex::TemplateLexeme,
}

/// Why an unparsed candidate was not staged as a member.
enum Miss {
    NoTemplate,
    Mismatch,
    OffGrid,
}

/// The parsed tree's reference leaves, in source order; `None` when the
/// tree has a node kind templates exclude.
fn reference_leaves<'a>(
    ast: &'a ASTNode,
    out: &mut Vec<(&'a str, &'a ReferenceType)>,
) -> Option<()> {
    match &ast.node_type {
        ASTNodeType::Literal(_) | ASTNodeType::Omitted => {}
        ASTNodeType::Reference {
            original,
            reference,
        } => out.push((original, reference)),
        ASTNodeType::UnaryOp { expr, .. } => reference_leaves(expr, out)?,
        ASTNodeType::BinaryOp { left, right, .. } => {
            reference_leaves(left, out)?;
            reference_leaves(right, out)?;
        }
        ASTNodeType::Function { args, .. } => {
            for arg in args {
                reference_leaves(arg, out)?;
            }
        }
        ASTNodeType::Call { .. } | ASTNodeType::Array(_) => return None,
    }
    Some(())
}

fn slot_is(reference: &ReferenceType, slot: SlotRef) -> bool {
    match (reference, slot) {
        (
            ReferenceType::Cell {
                sheet: None,
                row,
                col,
                row_abs,
                col_abs,
            },
            SlotRef::Cell(p),
        ) => (*row, *col, *row_abs, *col_abs) == (p.row, p.col, p.row_abs, p.col_abs),
        (
            ReferenceType::Range {
                sheet: None,
                start_row: Some(sr),
                start_col: Some(sc),
                end_row: Some(er),
                end_col: Some(ec),
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            },
            SlotRef::Range(a, b),
        ) => {
            (*sr, *sc, *start_row_abs, *start_col_abs) == (a.row, a.col, a.row_abs, a.col_abs)
                && (*er, *ec, *end_row_abs, *end_col_abs) == (b.row, b.col, b.row_abs, b.col_abs)
        }
        _ => false,
    }
}

impl<R: EvaluationContext> Engine<R> {
    fn source_families_active(&self) -> bool {
        self.source_family_mode != SourceFamilyMode::Off && self.config.formula_compression
    }

    /// Certify `ast`, the parse of `text` interned as `template`, for
    /// lexical relocation. The parser's tokenizer finds the reference
    /// slots of `text` (a conservative subset: canonical local A1 cells and
    /// finite ranges, fixed bytes elsewhere); the tree's reference leaves
    /// must be exactly those slots, in order, each spelled as its
    /// reference's rendering, and the template must need no structural
    /// rewrite. Then replacing every slot by the canonical spelling of its
    /// relocated reference changes only those tokens' text, so the parse
    /// of the result is the template with each reference relocated and
    /// re-rendered, which is how a family member is instantiated.
    fn certify_template(
        &self,
        text: &str,
        ast: &ASTNode,
        template: crate::engine::arena::AstNodeId,
    ) -> Option<LexicalTemplate> {
        let lexeme = formualizer_parse::template_lex::lex_template(text)?;
        let mut leaves = Vec::with_capacity(lexeme.slots().len());
        reference_leaves(ast, &mut leaves)?;
        if leaves.len() != lexeme.slots().len() {
            return None;
        }
        for ((original, reference), slot) in leaves.iter().zip(lexeme.slots()) {
            if *original != &text[slot.start as usize..slot.end as usize]
                || !slot_is(reference, slot.reference)
                || reference.normalise() != *original
            {
                return None;
            }
        }
        if !self.graph.template_renders_all_refs(template) {
            return None;
        }
        Some(LexicalTemplate {
            text: text.into(),
            lexeme,
        })
    }

    /// Stage formula `text` (with its leading `=`) at 1-based `(row, col)`
    /// as a member of a family without parsing it, when it is exactly a
    /// certified template's text relocated to this cell. The candidates
    /// are the family of the formula directly above (else the one directly
    /// left), as load-time grouping of parsed formulas compares, then the
    /// root of the formula's shared-formula family (`shared`, a sheet-local
    /// identity) when one was certified. A member always references its
    /// family's root template, never a chain of copies. `None` means the
    /// caller parses the formula as before.
    #[doc(hidden)]
    pub fn stage_relocated_text(
        &mut self,
        grouper: &mut FormulaFamilyGrouper,
        row: u32,
        col: u32,
        text: &str,
        shared: Option<u64>,
    ) -> Option<FormulaIngestRecord> {
        if !self.source_families_active() {
            return None;
        }
        let (row0, col0) = (row.saturating_sub(1), col.saturating_sub(1));
        let above = row0
            .checked_sub(1)
            .and_then(|r| grouper.by_col.get(&col0).filter(|(row, _)| *row == r))
            .map(|(_, family)| family.clone());
        let adjacent = above.or_else(|| {
            col0.checked_sub(1)
                .and_then(|c| {
                    grouper
                        .last
                        .as_ref()
                        .filter(|(row, col, _)| *row == row0 && *col == c)
                })
                .map(|(_, _, family)| family.clone())
        });
        let mut miss = None;
        if let Some(family) = adjacent {
            match self.match_family(&family, row0, col0, text, &mut grouper.counters) {
                Ok(()) => {
                    grouper.counters.adjacent_members += 1;
                    return Some(self.stage_lexical_member(grouper, row, col, family));
                }
                Err(m) => miss = Some((m, family.template, family.anchor)),
            }
        }
        if let Some(family) = shared.and_then(|key| grouper.shared_roots.get(&key).cloned())
            && miss
                .as_ref()
                .is_none_or(|(_, t, a)| (*t, *a) != (family.template, family.anchor))
        {
            match self.match_family(&family, row0, col0, text, &mut grouper.counters) {
                Ok(()) => {
                    grouper.counters.shared_members += 1;
                    return Some(self.stage_lexical_member(grouper, row, col, family));
                }
                Err(m) => miss = Some((m, family.template, family.anchor)),
            }
        }
        if let Some((miss, _, _)) = miss {
            let c = &mut grouper.counters;
            match miss {
                Miss::NoTemplate => c.fallback_no_template += 1,
                Miss::Mismatch => c.fallback_mismatch += 1,
                Miss::OffGrid => c.fallback_off_grid += 1,
            }
        }
        None
    }

    fn match_family(
        &self,
        family: &GroupedFamily,
        row0: u32,
        col0: u32,
        text: &str,
        counters: &mut SourceFamilyCounters,
    ) -> Result<(), Miss> {
        let Some(lexical) = family.lexical.as_deref() else {
            return Err(Miss::NoTemplate);
        };
        let dr = i64::from(row0) - i64::from(family.anchor.0);
        let dc = i64::from(col0) - i64::from(family.anchor.1);
        match lexical.lexeme.match_relocated(&lexical.text, text, dr, dc) {
            RelocatedMatch::Match => {}
            RelocatedMatch::Mismatch => return Err(Miss::Mismatch),
            RelocatedMatch::OffGrid => return Err(Miss::OffGrid),
        }
        let oracle = self.source_family_mode == SourceFamilyMode::Oracle;
        if oracle || cfg!(debug_assertions) {
            let agrees = formualizer_parse::parser::parse(text).is_ok_and(|own| {
                self.graph
                    .relocated_member_oracle(family.template, family.anchor, row0, col0, &own)
            });
            if oracle {
                counters.oracle_checked += 1;
                if !agrees {
                    counters.oracle_mismatches += 1;
                    eprintln!(
                        "[fz][families] oracle mismatch at R{}C{}: {text:?} is not {:?} (anchor R{}C{}) relocated by ({dr}, {dc})",
                        row0 + 1,
                        col0 + 1,
                        lexical.text,
                        family.anchor.0 + 1,
                        family.anchor.1 + 1,
                    );
                    return Err(Miss::Mismatch);
                }
            } else {
                assert!(
                    agrees,
                    "relocated formula {text:?} at R{}C{} is not {:?} (anchor R{}C{}) relocated",
                    row0 + 1,
                    col0 + 1,
                    lexical.text,
                    family.anchor.0 + 1,
                    family.anchor.1 + 1,
                );
            }
        }
        Ok(())
    }

    fn stage_lexical_member(
        &mut self,
        grouper: &mut FormulaFamilyGrouper,
        row: u32,
        col: u32,
        family: GroupedFamily,
    ) -> FormulaIngestRecord {
        let (row0, col0) = (row.saturating_sub(1), col.saturating_sub(1));
        let record = FormulaIngestRecord::member(row, col, family.template, family.anchor);
        grouper.members += 1;
        grouper.note(row0, col0, family);
        record
    }

    /// After `text` at 1-based `(row, col)` was parsed to `ast` and staged
    /// as `record` (by [`Self::stage_formula_ast`]): certify a new template
    /// for lexical relocation, and make the formula's family the root of
    /// its shared-formula family (`shared`) when that has none yet. Only
    /// for a successful parse of exactly `text`.
    #[doc(hidden)]
    pub fn note_parsed_text(
        &mut self,
        grouper: &mut FormulaFamilyGrouper,
        row: u32,
        col: u32,
        text: &str,
        ast: &ASTNode,
        record: &FormulaIngestRecord,
        shared: Option<u64>,
    ) {
        if !self.source_families_active() {
            return;
        }
        let (row0, col0) = (row.saturating_sub(1), col.saturating_sub(1));
        if !record.is_family_member() {
            match self.certify_template(text, ast, record.ast_id) {
                Some(lexical) => {
                    grouper.counters.templates_certified += 1;
                    grouper.attach_lexical(row0, col0, Arc::new(lexical));
                }
                None => grouper.counters.templates_uncertified += 1,
            }
        }
        if let Some(key) = shared
            && !grouper.shared_roots.contains_key(&key)
            && let Some((_, family)) = grouper.by_col.get(&col0).filter(|(r, _)| *r == row0)
            && family.lexical.is_some()
        {
            let family = family.clone();
            grouper.shared_roots.insert(key, family);
        }
    }
}
