//! Calculation-name import from immutable workbook XML.
//! Scoped and unscoped scans deliberately preserve distinct reader compatibility.
//! Import policy is fixed before dependency or grounding caches are populated.
use crate::traits::{DefinedName, DefinedNameDefinition, DefinedNameScope};
use crate::xlsx::ingest::name_references;
use crate::xlsx::metadata::names::NameParsing;
use crate::xlsx::recalculate::package;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::engine::{CancelToken, Engine};
use formualizer_eval::reference::{CellRef, Coord};
use formualizer_parse::parser::{ASTNode, ReferenceType};
use quick_xml::{
    Reader as XmlReader,
    events::{BytesRef, BytesStart, Event},
};
use std::{
    collections::{HashMap, HashSet},
    io::BufRead,
    sync::{Arc, OnceLock},
};

use crate::xlsx::ingest::name_analysis::{CalculationNameAnalysis, analyze};
#[derive(Default)]
struct CalculationNameCandidates {
    ordinary_first: Option<usize>,
    legacy_first: Option<usize>,
    legacy_last: Option<usize>,
}
#[derive(Default)]
struct CalculationNameIndex {
    names: HashMap<(Option<usize>, String), CalculationNameCandidates>,
    sheets: HashMap<String, usize>,
}

/// Immutable selection of name import and validation semantics.
#[derive(Clone, Copy)]
pub(in crate::xlsx) struct NameImportPolicy {
    referenced_only: bool,
    strict_validation: bool,
}
impl NameImportPolicy {
    pub(in crate::xlsx) const COMPATIBLE_REFERENCED: Self = Self {
        referenced_only: true,
        strict_validation: false,
    };
    pub(in crate::xlsx) const STRICT_REFERENCED: Self = Self {
        referenced_only: true,
        strict_validation: true,
    };
}

pub(in crate::xlsx) struct CalculationNames {
    raw: Vec<(String, String, Option<usize>)>,
    calamine_defined_names: Vec<(String, String)>,
    sheet_names: Vec<String>,
    inert_sheet_names: HashSet<String>,
    defined_names: OnceLock<Vec<DefinedName>>,
    name_index: OnceLock<CalculationNameIndex>,
    name_analysis: OnceLock<CalculationNameAnalysis>,
    grounded_names: OnceLock<Vec<Option<ASTNode>>>,
    policy: NameImportPolicy,
    unregistrable_names: HashSet<String>,
    external_values: Option<Arc<crate::cached_external_links::ExternalValues>>,
    skipped_external_names: Vec<String>,
    refused_external_names: HashSet<String>,
    cancel: Option<CancelToken>,
}
impl CalculationNames {
    const EXCEL_MAX_ROWS: u32 = 1_048_576;
    const EXCEL_MAX_COLS: u32 = 16_384;
    pub(in crate::xlsx) fn new(
        bytes: &[u8],
        sheets: &[package::Sheet],
        cancel: Option<CancelToken>,
        policy: NameImportPolicy,
    ) -> Self {
        let calamine_defined_names = Self::scan_names(bytes, false)
            .into_iter()
            .map(|(name, text, _)| (name, text))
            .collect::<Vec<_>>();
        let mut raw = Self::scan_names(bytes, true);
        if raw.is_empty() {
            raw = calamine_defined_names
                .iter()
                .map(|(n, f)| (n.clone(), f.clone(), None))
                .collect();
        }
        Self {
            raw,
            calamine_defined_names,
            sheet_names: sheets.iter().map(|s| s.name.clone()).collect(),
            inert_sheet_names: if policy.strict_validation {
                HashSet::new()
            } else {
                sheets
                    .iter()
                    .filter(|sheet| sheet.inert)
                    .map(|sheet| sheet.name.to_lowercase())
                    .collect()
            },
            defined_names: OnceLock::new(),
            name_index: OnceLock::new(),
            name_analysis: OnceLock::new(),
            grounded_names: OnceLock::new(),
            policy,
            unregistrable_names: HashSet::new(),
            external_values: None,
            skipped_external_names: Vec::new(),
            refused_external_names: HashSet::new(),
            cancel,
        }
    }
    pub(in crate::xlsx) fn admit_external(
        &mut self,
        values: Arc<crate::cached_external_links::ExternalValues>,
        skipped: Vec<String>,
    ) {
        self.external_values = Some(values);
        self.skipped_external_names = skipped;
        self.name_index.take();
        self.name_analysis.take();
        self.grounded_names.take();
    }
    pub(in crate::xlsx) fn admit_external_snapshot(
        &mut self,
        snapshot: &crate::CachedExternalLinkValues,
    ) {
        self.admit_external(snapshot.values.clone(), snapshot.skipped_names.clone());
        self.refused_external_names = snapshot.refused_names.clone();
    }
    pub(in crate::xlsx) fn raw_calculation_names(&self) -> &Vec<(String, String, Option<usize>)> {
        &self.raw
    }
    fn lazy_defined_names(&self) -> &Vec<DefinedName> {
        self.defined_names.get_or_init(|| {
            if self.calamine_defined_names.is_empty() {
                return Vec::new();
            }
            let sheets = &self.sheet_names;
            let parsed: Vec<_> = self
                .raw
                .iter()
                .filter_map(|(n, f, s)| Self::convert_defined_name(n, f, *s, sheets))
                .collect();
            if parsed.is_empty() {
                Self::fallback_defined_names(&self.calamine_defined_names, sheets)
            } else {
                parsed
            }
        })
    }
    pub(in crate::xlsx) fn defined_names(
        &self,
    ) -> Result<Vec<DefinedName>, crate::xlsx::types::XlsxReadError> {
        Ok(self.lazy_defined_names().clone())
    }
    fn scan_names(bytes: &[u8], scoped: bool) -> Vec<(String, String, Option<usize>)> {
        let mut xml = XmlReader::from_reader(bytes);
        xml.config_mut().trim_text(false);
        // The scoped adapter ignored Empty events; the pinned unscoped reader
        // expanded them. Preserve that distinction before scoped fallback.
        xml.config_mut().expand_empty_elements = !scoped;
        let mut out = Vec::new();
        let mut buf = Vec::new();
        let mut inner = Vec::new();
        let mut inside = false;
        loop {
            buf.clear();
            match xml.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) if e.local_name().as_ref() == b"definedNames" => {
                    inside = true
                }
                Ok(Event::End(ref e)) if e.local_name().as_ref() == b"definedNames" && scoped => {
                    break;
                }
                Ok(Event::Start(ref e))
                    if (!scoped || inside) && e.local_name().as_ref() == b"definedName" =>
                {
                    let name = Self::decode_attr(&xml, e, b"name");
                    let local = scoped
                        .then(|| {
                            Self::decode_attr(&xml, e, b"localSheetId")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .flatten();
                    let mut value = String::new();
                    loop {
                        inner.clear();
                        match xml.read_event_into(&mut inner) {
                            Ok(Event::Text(t)) => match t.xml10_content() {
                                Ok(text) => value.push_str(&text),
                                Err(_) => return Vec::new(),
                            },
                            Ok(Event::GeneralRef(e)) => {
                                if Self::append_xml_entity(&e, &mut value).is_err() {
                                    return Vec::new();
                                }
                            }
                            Ok(Event::End(end)) if end.name() == e.name() => break,
                            Ok(Event::Eof) | Err(_) => return Vec::new(),
                            _ => {}
                        }
                    }
                    if let Some(name) = name {
                        out.push((name, value, local));
                    }
                }
                Ok(Event::Eof) => break,
                Err(_) => return Vec::new(),
                _ => {}
            }
        }
        out
    }
    fn external_admitted(&self, raw: &str) -> bool {
        self.external_values
            .as_ref()
            .is_some_and(|values| values.contains(raw))
    }

    fn is_inert_sheet(&self, name: &str) -> bool {
        !self.policy.strict_validation && self.inert_sheet_names.contains(&name.to_lowercase())
    }
    fn inert_scope(&self, scope: Option<usize>) -> bool {
        !self.policy.strict_validation
            && scope
                .and_then(|id| self.sheet_names.get(id))
                .is_some_and(|name| self.is_inert_sheet(name))
    }
    fn grounded_calculation_name(&self, i: usize) -> Option<ASTNode> {
        if self.inert_scope(self.raw_calculation_names()[i].2) {
            return None;
        }
        let name = &self.raw_calculation_names()[i].0;
        if self.policy.referenced_only
            && !self.legacy_calculation_name(name)
            && self.unregistrable_names.contains(&name.to_lowercase())
        {
            return None;
        }
        self.grounded_names
            .get_or_init(|| {
                self.raw_calculation_names()
                    .iter()
                    .map(|(name, text, scope)| self.grounded_name_ast(name, text, *scope))
                    .collect()
            })
            .get(i)
            .cloned()
            .flatten()
    }

    fn grounded_name_ast(&self, name: &str, text: &str, scope: Option<usize>) -> Option<ASTNode> {
        if self
            .refused_external_names
            .contains(&name.to_ascii_lowercase())
        {
            return formualizer_parse::parser::parse("=#REF!").ok();
        }
        use formualizer_parse::parser::ASTNodeType as N;
        if Self::cancellation_checkpoint(self.cancel.as_ref()).is_err() {
            return None;
        }
        let sheets = &self.sheet_names;
        if scope.is_some_and(|id| id >= sheets.len()) {
            return None;
        }
        let external = |raw: &str| self.external_admitted(raw);
        fn ground(
            ast: &mut ASTNode,
            base: Option<&String>,
            sheets: &[String],
            external: &dyn Fn(&str) -> bool,
        ) -> bool {
            match &mut ast.node_type {
                N::Literal(LiteralValue::Number(n)) => n.is_finite(),
                N::Literal(_) => true,
                N::Reference { reference, .. } => match reference {
                    ReferenceType::NamedRange(_) => true,
                    ReferenceType::Cell {
                        sheet,
                        row,
                        col,
                        row_abs,
                        col_abs,
                    } => {
                        if sheet.is_none() {
                            *sheet = base.cloned();
                        }
                        *row_abs
                            && *col_abs
                            && *row > 0
                            && *row <= CalculationNames::EXCEL_MAX_ROWS
                            && *col > 0
                            && *col <= CalculationNames::EXCEL_MAX_COLS
                            && sheet.as_ref().is_some_and(|s| sheets.contains(s))
                    }
                    ReferenceType::Range {
                        sheet,
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                        start_row_abs,
                        start_col_abs,
                        end_row_abs,
                        end_col_abs,
                    } => {
                        if sheet.is_none() {
                            *sheet = base.cloned();
                        }
                        let rows = start_row.zip(*end_row);
                        let cols = start_col.zip(*end_col);
                        let rows_ok = rows.is_some_and(|(s, e)| {
                            *start_row_abs
                                && *end_row_abs
                                && s > 0
                                && s <= e
                                && e <= CalculationNames::EXCEL_MAX_ROWS
                        }) || (start_row.is_none() && end_row.is_none());
                        let cols_ok = cols.is_some_and(|(s, e)| {
                            *start_col_abs
                                && *end_col_abs
                                && s > 0
                                && s <= e
                                && e <= CalculationNames::EXCEL_MAX_COLS
                        }) || (start_col.is_none() && end_col.is_none());
                        rows_ok && cols_ok && sheet.as_ref().is_some_and(|s| sheets.contains(s))
                    }
                    // An absolute reference whose cached value a
                    // recalculation serves (never on other load paths).
                    ReferenceType::External(ext) => {
                        use formualizer_parse::parser::ExternalRefKind as K;
                        let absolute = match ext.kind {
                            K::Cell {
                                row_abs, col_abs, ..
                            } => row_abs && col_abs,
                            K::Range {
                                start_row_abs,
                                start_col_abs,
                                end_row_abs,
                                end_col_abs,
                                ..
                            } => start_row_abs && start_col_abs && end_row_abs && end_col_abs,
                        };
                        absolute && external(&ext.raw)
                    }
                    _ => false,
                },
                N::UnaryOp { op, expr } => {
                    matches!(op.as_str(), "+" | "-" | "%") && ground(expr, base, sheets, external)
                }
                N::BinaryOp { op, left, right } => {
                    matches!(
                        op.as_str(),
                        "+" | "-" | "*" | "/" | "^" | "&" | "=" | "<>" | "<" | ">" | "<=" | ">="
                    ) && ground(left, base, sheets, external)
                        && ground(right, base, sheets, external)
                }
                N::Function { name, args } => {
                    // Context-dependent/reference-producing functions (INDIRECT,
                    // OFFSET, ROW(), etc.) require separate name semantics. Admit
                    // only this explicit pure scalar/aggregation subset.
                    matches!(
                        name.to_ascii_uppercase().as_str(),
                        "SUM"
                            | "AVERAGE"
                            | "MIN"
                            | "MAX"
                            | "COUNT"
                            | "COUNTA"
                            | "ABS"
                            | "ROUND"
                            | "ROUNDUP"
                            | "ROUNDDOWN"
                            | "IF"
                            | "AND"
                            | "OR"
                            | "NOT"
                    ) && args.iter_mut().all(|a| ground(a, base, sheets, external))
                }
                // Arrays, generic calls and omitted arguments are not supported name literals.
                _ => false,
            }
        }
        let mut ast = formualizer_parse::parser::parse(format!(
            "={}",
            text.trim().strip_prefix('=').unwrap_or(text.trim())
        ))
        .ok()?;
        let mut names_exist = true;
        ast.visit_refs(|r| {
            if let formualizer_parse::parser::RefView::NamedRange { name } = r {
                names_exist &= self.calculation_name_index(name, scope).is_some();
            }
        });
        ((!self.policy.referenced_only || self.legacy_calculation_name(name) || names_exist)
            && ground(
                &mut ast,
                scope.and_then(|id| sheets.get(id)),
                sheets,
                &external,
            ))
        .then_some(ast)
    }

    pub(in crate::xlsx) fn unimported_document_names(&self) -> Vec<String> {
        let cyclic = self.cyclic_calculation_names();
        let mut names = Vec::new();
        for (i, (name, _, _)) in self.raw_calculation_names().iter().enumerate() {
            if Self::cancellation_checkpoint(self.cancel.as_ref()).is_err() {
                return Vec::new();
            }
            if cyclic.contains(&i) || self.grounded_calculation_name(i).is_none() {
                names.push(if self.legacy_calculation_name(name) {
                    name.to_ascii_lowercase()
                } else {
                    name.to_lowercase()
                });
            }
        }
        names
    }

    fn calculation_name_dependencies(&self) -> Vec<Vec<usize>> {
        let raw = self.raw_calculation_names();
        raw.iter()
            .enumerate()
            .map(|(i, (name, text, scope))| {
                if Self::cancellation_checkpoint(self.cancel.as_ref()).is_err() {
                    return Vec::new();
                }
                let mut deps = HashSet::new();
                let referenced_only =
                    self.policy.referenced_only && !self.legacy_calculation_name(name);
                let ast = if referenced_only {
                    formualizer_parse::parser::parse(format!(
                        "={}",
                        text.trim().trim_start_matches('=')
                    ))
                    .ok()
                } else {
                    self.grounded_calculation_name(i)
                };
                if let Some(ast) = ast {
                    let mut reader = |name: &str| {
                        let index = if referenced_only {
                            self.calculation_name_index(name, *scope)
                        } else {
                            self.legacy_name_index(name, *scope)
                        };
                        if let Some(j) = index {
                            deps.insert(j);
                        }
                    };
                    if referenced_only {
                        name_references::visit(&ast, |name, _| reader(name));
                    } else {
                        ast.visit_refs(|r| {
                            if let formualizer_parse::parser::RefView::NamedRange { name } = r {
                                reader(name);
                            }
                        });
                    }
                }
                deps.into_iter().collect()
            })
            .collect()
    }

    fn calculation_name_index(&self, name: &str, scope: Option<usize>) -> Option<usize> {
        if !self.policy.referenced_only {
            return self.legacy_name_index(name, scope);
        }
        let (name, scope) = if let Some((sheet, name)) = name.rsplit_once('!') {
            let sheet = sheet
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .unwrap_or(sheet)
                .replace("''", "'");
            let scope = *self
                .calculation_name_index_map()
                .sheets
                .get(&sheet.to_lowercase())?;
            (name, Some(scope))
        } else {
            (name, scope)
        };
        let index = self.calculation_name_index_map();
        let key = name.to_lowercase();
        let lookup = |scope| {
            let ordinary = index
                .names
                .get(&(scope, key.clone()))
                .and_then(|n| n.ordinary_first);
            let legacy = index
                .names
                .get(&(scope, name.to_ascii_lowercase()))
                .and_then(|n| n.legacy_first);
            ordinary.into_iter().chain(legacy).min()
        };
        scope.and_then(|s| lookup(Some(s))).or_else(|| lookup(None))
    }

    fn calculation_name_index_map(&self) -> &CalculationNameIndex {
        self.name_index.get_or_init(|| {
            let mut index = CalculationNameIndex::default();
            for (i, (name, _, scope)) in self.raw_calculation_names().iter().enumerate() {
                if Self::cancellation_checkpoint(self.cancel.as_ref()).is_err() {
                    return index;
                }
                let ascii = index
                    .names
                    .entry((*scope, name.to_ascii_lowercase()))
                    .or_default();
                // Generic import chooses the last ASCII match; strict import
                // chooses the first match with Unicode folding for ordinary names.
                ascii.legacy_last = Some(i);
                if self.legacy_calculation_name(name) {
                    ascii.legacy_first.get_or_insert(i);
                } else {
                    index
                        .names
                        .entry((*scope, name.to_lowercase()))
                        .or_default()
                        .ordinary_first
                        .get_or_insert(i);
                }
            }
            for (i, sheet) in self.sheet_names.iter().enumerate() {
                if Self::cancellation_checkpoint(self.cancel.as_ref()).is_err() {
                    return index;
                }
                index.sheets.entry(sheet.to_lowercase()).or_insert(i);
            }
            index
        })
    }

    fn legacy_name_index(&self, name: &str, scope: Option<usize>) -> Option<usize> {
        let index = &self.calculation_name_index_map().names;
        let key = name.to_ascii_lowercase();
        index
            .get(&(scope, key.clone()))
            .and_then(|n| n.legacy_last)
            .or_else(|| index.get(&(None, key)).and_then(|n| n.legacy_last))
    }

    fn legacy_calculation_name(&self, name: &str) -> bool {
        let name = name.to_ascii_lowercase();
        let legacy = name.starts_with("_xlnm.");
        legacy || self.skipped_external_names.contains(&name)
    }

    pub(in crate::xlsx) fn skippable_name_registration_error(
        &self,
        name: &str,
        error: &ExcelError,
    ) -> bool {
        self.policy.referenced_only
            && !self.legacy_calculation_name(name)
            && error.kind == ExcelErrorKind::Name
            && error
                .message()
                .is_some_and(|m| m.starts_with("Invalid name: "))
    }

    fn reads_unregistrable_name(&self, ast: &ASTNode, scope: Option<usize>) -> bool {
        let mut reads = false;
        ast.visit_refs(|r| {
            if let formualizer_parse::parser::RefView::NamedRange { name } = r
                && let Some(i) = self.calculation_name_index(name, scope)
            {
                reads |= self
                    .unregistrable_names
                    .contains(&self.raw_calculation_names()[i].0.to_lowercase());
            }
        });
        reads
    }

    fn calculation_name_analysis(&self) -> &CalculationNameAnalysis {
        self.name_analysis.get_or_init(|| {
            analyze(self.calculation_name_dependencies(), || {
                Self::cancellation_checkpoint(self.cancel.as_ref()).is_err()
            })
        })
    }

    fn cyclic_calculation_names(&self) -> &HashSet<usize> {
        &self.calculation_name_analysis().cyclic
    }

    pub(in crate::xlsx) fn validate_calculation_names(
        &mut self,
        formulas: &[(String, String)],
        admitted_inert_sheets: bool,
    ) -> Result<(), crate::IoError> {
        if !self.policy.strict_validation {
            return Ok(());
        }
        let references = |ast: &ASTNode, scope: Option<usize>, out: &mut HashSet<usize>| {
            name_references::visit(ast, |name, indirect| {
                if let Some(i) = self.calculation_name_index(name, scope) {
                    let name = self.raw_calculation_names()[i].0.to_ascii_lowercase();
                    // Retain the existing document/external-name policy.
                    if !indirect
                        || (!name.starts_with("_xlnm.")
                            && !self.skipped_external_names.contains(&name))
                    {
                        out.insert(i);
                    }
                }
            })
        };
        let fail = |name: &str| crate::IoError::Unsupported {
            feature: "unsupported or cyclic calculation name".into(),
            context: name.into(),
        };
        let raw = self.raw_calculation_names();
        let mut used = HashSet::new();
        let mut legacy_used = HashSet::new();
        let legacy_references = |ast: &ASTNode, out: &mut HashSet<String>| {
            ast.visit_refs(|r| {
                if let formualizer_parse::parser::RefView::NamedRange { name } = r {
                    out.insert(name.rsplit('!').next().unwrap_or(name).to_ascii_lowercase());
                }
            });
        };
        let cyclic = self.cyclic_calculation_names();
        let newly_admitted = admitted_inert_sheets
            || raw.iter().enumerate().any(|(i, (name, _, _))| {
                !self.legacy_calculation_name(name)
                    && (cyclic.contains(&i) || self.grounded_calculation_name(i).is_none())
            });
        let mut nonliteral = false;
        for (sheet, f) in formulas {
            Self::cancellation_checkpoint(self.cancel.as_ref())
                .map_err(crate::IoError::XlsxRead)?;
            if let Ok(ast) = formualizer_parse::parser::parse(format!(
                "={}",
                f.trim().strip_prefix('=').unwrap_or(f.trim())
            )) {
                let scope = self
                    .sheet_names
                    .iter()
                    .position(|s| s.to_lowercase() == sheet.to_lowercase());
                nonliteral |= references(&ast, scope, &mut used);
                legacy_references(&ast, &mut legacy_used);
            }
        }
        if raw
            .iter()
            .any(|(name, _, _)| self.legacy_calculation_name(name))
        {
            for (_, text, _) in raw {
                Self::cancellation_checkpoint(self.cancel.as_ref())
                    .map_err(crate::IoError::XlsxRead)?;
                if let Ok(ast) = formualizer_parse::parser::parse(format!(
                    "={}",
                    text.trim().trim_start_matches('=')
                )) {
                    legacy_references(&ast, &mut legacy_used);
                }
            }
        }
        for (i, (_, _, scope)) in raw.iter().enumerate() {
            Self::cancellation_checkpoint(self.cancel.as_ref())
                .map_err(crate::IoError::XlsxRead)?;
            if !cyclic.contains(&i)
                && let Some(ast) = self.grounded_calculation_name(i)
            {
                references(&ast, *scope, &mut used);
            }
        }
        for (i, (name, _, scope)) in raw.iter().enumerate() {
            Self::cancellation_checkpoint(self.cancel.as_ref())
                .map_err(crate::IoError::XlsxRead)?;
            let grounded = self.grounded_calculation_name(i).is_some();
            let legacy = self.legacy_calculation_name(name);
            if newly_admitted
                && legacy
                && let Some(ast) = self.grounded_calculation_name(i)
            {
                let mut unresolved = false;
                ast.visit_refs(|r| {
                    if let formualizer_parse::parser::RefView::NamedRange { name } = r {
                        unresolved |= self.calculation_name_index(name, *scope).is_none();
                    }
                });
                // Keep a previously masked legacy name failure a refusal.
                // Legacy-only workbooks still use the original import path.
                if unresolved {
                    return Err(fail(name));
                }
            }
            if legacy && !grounded && legacy_used.contains(&name.to_ascii_lowercase()) {
                return Err(fail(name));
            }
            if used.contains(&i) || (legacy && grounded && cyclic.contains(&i)) {
                if cyclic.contains(&i) {
                    return Err(crate::IoError::Unsupported {
                        feature: "cyclic calculation name".into(),
                        context: name.clone(),
                    });
                }
                if !grounded {
                    return Err(fail(name));
                }
            }
        }
        if nonliteral && self.has_new_unimported_names() {
            return Err(crate::IoError::Unsupported {
                feature: "nonliteral INDIRECT in a workbook with unevaluable names".into(),
                context: "calculation formula".into(),
            });
        }
        self.calculation_name_order().map(|_| ())
    }

    pub(in crate::xlsx) fn has_new_unimported_names(&self) -> bool {
        self.unimported_document_names()
            .iter()
            .any(|name| !name.starts_with("_xlnm.") && !self.skipped_external_names.contains(name))
    }

    pub(in crate::xlsx) fn calculation_name_asts(&self) -> Result<Vec<ASTNode>, crate::IoError> {
        Ok(self
            .calculation_name_order()?
            .into_iter()
            .filter_map(|i| self.grounded_calculation_name(i))
            .collect())
    }

    fn calculation_name_order(&self) -> Result<Vec<usize>, crate::IoError> {
        use std::collections::VecDeque;
        let raw = self.raw_calculation_names();
        let cyclic = self.cyclic_calculation_names();
        let dependencies = &self.calculation_name_analysis().dependencies;
        let mut pending = vec![0usize; raw.len()];
        let mut readers = vec![Vec::new(); raw.len()];
        for (i, _) in raw.iter().enumerate() {
            Self::cancellation_checkpoint(self.cancel.as_ref())
                .map_err(crate::IoError::XlsxRead)?;
            if (self.policy.referenced_only && cyclic.contains(&i))
                || self.grounded_calculation_name(i).is_none()
            {
                if self.policy.referenced_only {
                    pending[i] = usize::MAX;
                }
                continue;
            }
            let deps = &dependencies[i];
            pending[i] = deps.len();
            for &j in deps {
                readers[j].push(i);
            }
        }
        let mut ready: VecDeque<_> = pending
            .iter()
            .enumerate()
            .filter_map(|(i, n)| (*n == 0).then_some(i))
            .collect();
        let mut order = Vec::with_capacity(raw.len());
        while let Some(i) = ready.pop_front() {
            Self::cancellation_checkpoint(self.cancel.as_ref())
                .map_err(crate::IoError::XlsxRead)?;
            order.push(i);
            for &reader in &readers[i] {
                pending[reader] -= 1;
                if pending[reader] == 0 {
                    ready.push_back(reader);
                }
            }
        }
        if !self.policy.referenced_only && order.len() != raw.len() {
            return Err(crate::IoError::Unsupported {
                feature: "cyclic calculation name".into(),
                context: raw[pending.iter().position(|n| *n != 0).unwrap()].0.clone(),
            });
        }
        Ok(order)
    }

    fn convert_defined_name(
        name: &str,
        raw_formula: &str,
        local_sheet_id: Option<usize>,
        sheet_names: &[String],
    ) -> Option<DefinedName> {
        NameParsing::convert_defined_name(name, raw_formula, local_sheet_id, sheet_names)
    }

    fn decode_attr<R: BufRead>(
        reader: &XmlReader<R>,
        start: &BytesStart<'_>,
        key: &[u8],
    ) -> Option<String> {
        NameParsing::decode_attr(reader, start, key)
    }

    fn append_xml_entity(
        entity: &BytesRef<'_>,
        buffer: &mut String,
    ) -> Result<(), quick_xml::Error> {
        NameParsing::append_xml_entity(entity, buffer)
    }

    fn fallback_defined_names(
        calamine_defined_names: &[(String, String)],
        sheet_names: &[String],
    ) -> Vec<DefinedName> {
        NameParsing::fallback_defined_names(calamine_defined_names, sheet_names)
    }

    fn cancellation_checkpoint(
        cancel: Option<&CancelToken>,
    ) -> Result<(), crate::xlsx::types::XlsxReadError> {
        if cancel.is_some_and(CancelToken::is_cancelled) {
            return Err(crate::xlsx::types::XlsxReadError::Io(
                std::io::Error::other("calamine load cancelled"),
            ));
        }
        Ok(())
    }

    pub(in crate::xlsx) fn import_diagnostics(
        &self,
    ) -> Result<Vec<crate::traits::NameImportDiagnostic>, crate::IoError> {
        let imported: HashSet<_> = self.calculation_name_order()?.into_iter().collect();
        Ok(self
            .raw_calculation_names()
            .iter()
            .enumerate()
            .filter(|(index, (name, _, scope))| {
                self.inert_scope(*scope)
                    || (!self.legacy_calculation_name(name) && !imported.contains(index))
            })
            .map(
                |(_, (name, definition, local))| crate::traits::NameImportDiagnostic {
                    name: name.clone(),
                    definition: definition.clone(),
                    scope_sheet: local.and_then(|index| self.sheet_names.get(index).cloned()),
                    local_sheet_id: *local,
                    message: if self.inert_scope(*local) {
                        "Defined name scoped to an omitted non-worksheet sheet"
                    } else {
                        "Unevaluable or cyclic defined name omitted from calculation"
                    }
                    .into(),
                },
            )
            .collect())
    }

    pub(in crate::xlsx) fn register<C: formualizer_eval::traits::EvaluationContext>(
        &mut self,
        engine: &mut Engine<C>,
    ) -> Result<(), crate::xlsx::types::XlsxReadError> {
        let cancel = self.cancel.clone();
        let scope_names = self.sheet_names.clone();
        {
            use rustc_hash::FxHashSet;

            Self::cancellation_checkpoint(cancel.as_ref())?;
            let mut defined: Vec<_> = if !self.policy.referenced_only {
                self.defined_names()?
            } else {
                self.calculation_name_order()
                    .map_err(|e| {
                        crate::xlsx::types::XlsxReadError::Io(std::io::Error::other(e.to_string()))
                    })?
                    .into_iter()
                    .filter_map(|i| {
                        let (name, text, scope) = &self.raw_calculation_names()[i];
                        Self::convert_defined_name(name, text, *scope, &scope_names)
                    })
                    .collect()
            };
            if self.policy.referenced_only {
                // Keep legacy document-name range/literal import unchanged,
                // including malformed ranges that create a missing sheet.
                defined.extend(
                    self.defined_names()?
                        .into_iter()
                        .filter(|dn| dn.name.to_ascii_lowercase().starts_with("_xlnm.")),
                );
            }
            let mut seen: FxHashSet<(DefinedNameScope, Option<String>, String)> =
                FxHashSet::default();

            for dn in defined {
                if dn
                    .scope_sheet
                    .as_deref()
                    .is_some_and(|name| self.is_inert_sheet(name))
                {
                    continue;
                }
                if let DefinedNameDefinition::Range { address } = &dn.definition
                    && self.is_inert_sheet(&address.sheet)
                {
                    continue;
                }
                Self::cancellation_checkpoint(cancel.as_ref())?;
                let key = (dn.scope.clone(), dn.scope_sheet.clone(), dn.name.clone());
                if !seen.insert(key) {
                    continue;
                }

                let scope = match dn.scope {
                    DefinedNameScope::Workbook => NameScope::Workbook,
                    DefinedNameScope::Sheet => {
                        let sheet_name = dn.scope_sheet.as_deref().ok_or_else(|| {
                            crate::xlsx::types::XlsxReadError::Io(std::io::Error::other(format!(
                                "sheet-scoped defined name `{}` missing scope_sheet",
                                dn.name
                            )))
                        })?;
                        let sid = engine.sheet_id(sheet_name).ok_or_else(|| {
                            crate::xlsx::types::XlsxReadError::Io(std::io::Error::other(format!(
                                "scope sheet not found: {sheet_name}"
                            )))
                        })?;
                        NameScope::Sheet(sid)
                    }
                };

                let definition = match dn.definition {
                    DefinedNameDefinition::Range { address } => {
                        let sheet_id = engine
                            .sheet_id(&address.sheet)
                            .or_else(|| engine.add_sheet(&address.sheet).ok())
                            .ok_or_else(|| {
                                crate::xlsx::types::XlsxReadError::Io(std::io::Error::other(
                                    format!("sheet not found: {}", address.sheet),
                                ))
                            })?;

                        let sr0 = address.start_row.saturating_sub(1);
                        let sc0 = address.start_col.saturating_sub(1);
                        let er0 = address.end_row.saturating_sub(1);
                        let ec0 = address.end_col.saturating_sub(1);

                        // A range past the grid (a built-in `_xlnm.Print_Area`
                        // ending at row 1048577, say) is left undefined, as an
                        // ordinary name with such a range is: references to it
                        // are #NAME?.
                        let (Ok(start), Ok(end)) = (
                            Coord::try_new(sr0, sc0, true, true),
                            Coord::try_new(er0, ec0, true, true),
                        ) else {
                            continue;
                        };
                        let start_ref = CellRef::new(sheet_id, start);
                        if sr0 == er0 && sc0 == ec0 {
                            NamedDefinition::Cell(start_ref)
                        } else {
                            let end_ref = CellRef::new(sheet_id, end);
                            let range_ref =
                                formualizer_eval::reference::RangeRef::new(start_ref, end_ref);
                            NamedDefinition::Range(range_ref)
                        }
                    }
                    DefinedNameDefinition::Literal { value } => NamedDefinition::Literal(value),
                };

                if let Err(error) = engine.define_name(&dn.name, definition, scope) {
                    if self.skippable_name_registration_error(&dn.name, &error) {
                        self.unregistrable_names.insert(dn.name.to_lowercase());
                    } else {
                        return Err(crate::xlsx::types::XlsxReadError::Io(
                            std::io::Error::other(error.to_string()),
                        ));
                    }
                }
            }
        }

        // Formula-valued names remain private to Calamine calculation import;
        // Engine::define_name owns cell/range/name dependency registration and
        // rebinds formulas that were ingested before these definitions.
        for index in self.calculation_name_order().map_err(|e| {
            crate::xlsx::types::XlsxReadError::Io(std::io::Error::other(e.to_string()))
        })? {
            let (name, text, local) = &self.raw_calculation_names()[index];
            Self::cancellation_checkpoint(cancel.as_ref())?;
            if Self::convert_defined_name(name, text, *local, &scope_names).is_some() {
                continue;
            }
            let Some(ast) = self.grounded_calculation_name(index) else {
                continue;
            };
            if self.policy.referenced_only && self.reads_unregistrable_name(&ast, *local) {
                // Leave the evaluated reader for the post-ingest name guard,
                // rather than surfacing a masked name failure as an I/O error.
                continue;
            }
            let scope = match local {
                Some(id) => {
                    NameScope::Sheet(engine.sheet_id(&scope_names[*id]).ok_or_else(|| {
                        crate::xlsx::types::XlsxReadError::Io(std::io::Error::other(
                            "name scope sheet missing",
                        ))
                    })?)
                }
                None => NameScope::Workbook,
            };
            if let Err(error) = engine.define_name(
                name,
                NamedDefinition::Formula {
                    ast,
                    dependencies: Vec::new(),
                    range_deps: Vec::new(),
                },
                scope,
            ) {
                if self.skippable_name_registration_error(name, &error) {
                    let name = name.to_lowercase();
                    self.unregistrable_names.insert(name);
                } else {
                    return Err(crate::xlsx::types::XlsxReadError::Io(
                        std::io::Error::other(error.to_string()),
                    ));
                }
            }
        }

        Ok(())
    }
}
