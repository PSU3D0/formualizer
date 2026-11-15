//! Legacy (pre-dynamic-array) implicit intersection for Excel-calculated
//! formulas, applied only to evaluation text; source formula bytes stay intact.
//!
//! An OOXML formula without dynamic-array metadata keeps its legacy meaning:
//! a multi-cell reference (or array) in a value position is reduced to one
//! value, by intersection with the formula's row or column. Excel 365 shows
//! these formulas with `@`; the file never contains it.
//!
//! Evidence, either of:
//! - the formula cell is listed in `xl/calcChain.xml`, which Excel writes for
//!   every formula it calculated and which the recalc writer never edits.
//!   openpyxl, XlsxWriter and umya-spreadsheet do not write a calc chain, so
//!   their unshared formulas keep dynamic-array evaluation;
//! - the cell belongs to a shared formula family, a form that cannot hold a
//!   dynamic array (those are stored as `t="array"` with `cm`), so a missing
//!   or incomplete calc chain says nothing about its meaning.
//!
//! Array formulas (CSE or dynamic) are never rewritten.
//!
//! Where intersection happens follows Excel's token classes: the cell formula
//! root is value class; function parameters convert their operands to
//! reference, value or array class (`classes`), and operators take value
//! operands. A value-class operand that can hold more than one cell is
//! wrapped in an explicit `@(...)`, which the engine already evaluates. The
//! stored `<f>` text is untouched.
#[cfg(test)]
thread_local! {
    static CHAIN_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
pub(in crate::xlsx) fn take_chain_reads() -> usize {
    CHAIN_READS.with(|n| n.replace(0))
}

use crate::XlsxRecalculateOptions;
use crate::legacy_formula::{Lowerer, Lowering, may_intersect};
use crate::xlsx::recalculate::{IoError, SheetPlan, checkpoint, package, xml};
use rustc_hash::FxHashSet;
use std::collections::{BTreeMap, HashSet};

/// Formula cells Excel calculated, by worksheet position in the workbook.
#[derive(Debug, Default)]
pub(in crate::xlsx) struct CalcChain {
    cells: Vec<FxHashSet<(u32, u32)>>,
}
impl CalcChain {
    pub fn contains(&self, sheet: usize, row: u32, col: u32) -> bool {
        self.cells
            .get(sheet)
            .is_some_and(|cells| cells.contains(&(row, col)))
    }
}

/// Read the calc chain. It is evidence only: a missing, malformed or
/// oversized part yields no evidence rather than a refusal.
/// Only unshared legacy scalar formulas that pass the lowerer's own cheap
/// screen need calc-chain evidence; a shared family member is its own.
/// Keep cancellation responsive on scalar-only files.
pub(in crate::xlsx) fn needs_calc_chain(
    plans: &[SheetPlan],
    options: &XlsxRecalculateOptions,
) -> Result<bool, IoError> {
    for plan in plans {
        for cell in &plan.cells {
            checkpoint(&options.cancel)?;
            if cell.formula_kind != "array"
                && cell.shared_id.is_none()
                && cell.cm().is_none()
                && !plan.ownership.children.contains_key(&(cell.row, cell.col))
                && may_intersect(&cell.formula_text)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
#[test]
fn cheap_chain_coordinates_preserve_legacy_parser_acceptance() {
    let by_id = BTreeMap::from([(1, 0)]);
    for reference in [
        "B1",
        "b1",
        "$B$1",
        "XFD1048576",
        "A0001",
        "A0",
        "XFE1",
        "A1048577",
        "bad",
    ] {
        let expected = formualizer_common::coord::parse_a1_1based(reference).ok();
        let xml = format!(
            "<calcChain xmlns=\"{}\"><c i=\"1\" r=\"{reference}\"/></calcChain>",
            xml::MAIN
        );
        let chain = read_chain(xml.as_bytes(), &by_id, 1, &Default::default()).unwrap();
        assert_eq!(chain.is_some(), expected.is_some(), "{reference}");
        if let (Some(chain), Some((row, col, _, _))) = (chain, expected) {
            assert!(chain.contains(0, row, col), "{reference}");
        }
    }
}

pub(in crate::xlsx) fn calc_chain(
    archive: &mut package::Archive<'_>,
    part: Option<&str>,
    sheets: &[package::Sheet],
    options: &XlsxRecalculateOptions,
) -> Result<CalcChain, IoError> {
    let Some(part) = part else {
        return Ok(CalcChain::default());
    };
    let Ok(data) = package::read_part(archive, part, options.limits.max_worksheet_bytes) else {
        return Ok(CalcChain::default());
    };
    let by_id: BTreeMap<u32, usize> = sheets
        .iter()
        .enumerate()
        .map(|(i, s)| (s.sheet_id, i))
        .collect();
    Ok(read_chain(&data, &by_id, sheets.len(), options)?.unwrap_or_default())
}

/// A lightweight namespace-checked pass over `calcChain/c` elements (the
/// part can list every formula cell of a large workbook). `None`: malformed
/// or foreign, which is no evidence.
fn read_chain(
    data: &[u8],
    by_id: &BTreeMap<u32, usize>,
    sheets: usize,
    options: &XlsxRecalculateOptions,
) -> Result<Option<CalcChain>, IoError> {
    use quick_xml::events::Event;
    use quick_xml::name::ResolveResult;
    #[cfg(test)]
    CHAIN_READS.with(|n| n.set(n.get().saturating_add(1)));
    let Ok(text) = std::str::from_utf8(data) else {
        return Ok(None);
    };
    let mut reader = quick_xml::NsReader::from_str(text);
    reader.config_mut().check_end_names = true;
    let mut chain = CalcChain {
        cells: vec![FxHashSet::default(); sheets],
    };
    let mut depth = 0usize;
    let mut roots = 0usize;
    let mut current: Option<usize> = None;
    let mut events = 0u64;
    loop {
        events += 1;
        if events & 4095 == 0 {
            checkpoint(&options.cancel)?;
        }
        let (resolved, event) = match reader.read_resolved_event() {
            Ok(x) => x,
            Err(_) => return Ok(None),
        };
        let (start, empty) = match &event {
            Event::Start(e) => (e, false),
            Event::Empty(e) => (e, true),
            Event::End(_) => {
                depth = depth.saturating_sub(1);
                continue;
            }
            Event::Eof => break,
            _ => continue,
        };
        let main =
            matches!(resolved, ResolveResult::Bound(ns) if ns.as_ref() == xml::MAIN.as_bytes());
        let local = start.local_name();
        if depth == 0 {
            roots += 1;
            if roots != 1 || !main || local.as_ref() != b"calcChain" {
                return Ok(None);
            }
        } else if depth == 1 && main && local.as_ref() == b"c" {
            let (mut r, mut i) = (None, None);
            for a in start.attributes() {
                let Ok(a) = a else { return Ok(None) };
                match a.key.as_ref() {
                    b"r" => r = Some(a.value),
                    b"i" => i = Some(a.value),
                    _ => {}
                }
            }
            if let Some(id) = i {
                current = std::str::from_utf8(&id)
                    .ok()
                    .and_then(|id| id.parse::<u32>().ok())
                    .and_then(|id| by_id.get(&id).copied());
                if current.is_none() {
                    return Ok(None);
                }
            }
            let cell = r
                .as_deref()
                .and_then(|r| std::str::from_utf8(r).ok())
                .and_then(|r| {
                    crate::xlsx::xml::plain_coord(r).or_else(|| {
                        // Preserve the old acceptance of noncanonical spellings
                        // (e.g. absolute coordinates); ordinary OOXML takes the
                        // allocation-free parser above.
                        formualizer_common::coord::parse_a1_1based(r)
                            .ok()
                            .map(|(row, col, _, _)| (row, col))
                    })
                });
            match (current, cell) {
                (Some(sheet), Some((row, col))) => {
                    chain.cells[sheet].insert((row, col));
                }
                _ => return Ok(None),
            }
        }
        if !empty {
            depth += 1;
        }
    }
    Ok((roots == 1 && depth == 0).then_some(chain))
}

/// Counters for one worksheet, reported in debug output and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(in crate::xlsx) struct Stats {
    pub rewritten: usize,
    pub skipped: usize,
}

/// Conservative evaluation-text lowering that makes legacy formulas of one worksheet
/// intersect. A cell qualifies when `chain` lists it or it belongs to a shared
/// family, unless it carries dynamic-array metadata. In a workbook with
/// external links (`external_range_names` present: the lowercased defined
/// names that hold an external range), formulas whose `[` may come from
/// external references are classified too; structured references still
/// skip them. No intermediate XML transport copy is constructed.
pub(in crate::xlsx) fn lowered_texts(
    sheet_index: usize,
    plan: &SheetPlan,
    chain: &CalcChain,
    table_names: &[String],
    external_range_names: Option<&HashSet<String>>,
    options: &XlsxRecalculateOptions,
) -> Result<(Vec<crate::xlsx::recalculate::FormulaTextTransform>, Stats), IoError> {
    let mut stats = Stats::default();
    let mut out = Vec::new();
    let mut lowerer = Lowerer::default();
    for cell in &plan.cells {
        checkpoint(&options.cancel)?;
        let formula = cell.formula_text.as_str();
        if formula.trim().is_empty()
            || cell.formula_kind == "array"
            || cell.cm().is_some()
            || plan.ownership.children.contains_key(&(cell.row, cell.col))
            || (cell.shared_id.is_none() && !chain.contains(sheet_index, cell.row, cell.col))
        {
            continue;
        }
        if !may_intersect(formula) {
            continue;
        }
        let folded = formula.to_lowercase();
        if (folded.contains('[') && external_range_names.is_none())
            || table_names.iter().any(|t| folded.contains(t.as_str()))
        {
            stats.skipped += 1;
            continue;
        }
        let text = match lowerer.lower(formula) {
            Lowering::Unchanged => continue,
            Lowering::Skipped(_) => {
                stats.skipped += 1;
                continue;
            }
            Lowering::Rewritten(text) => text,
        };
        if let Some(names) = external_range_names
            && let Some(name) = intersected_name(&text, names)
        {
            // Such a name's value has no worksheet position to intersect.
            return Err(crate::xlsx::recalculate::unsupported(
                "implicit intersection of a defined name holding an external range",
                format!("{}: {name}", cell.address()),
            ));
        }
        let raw = &plan.data[cell.formula_open.end..cell.formula_end];
        let Some(close) = raw.iter().rposition(|b| *b == b'<') else {
            continue;
        };
        stats.rewritten += 1;
        out.push(crate::xlsx::recalculate::FormulaTextTransform {
            span: cell.formula_open.end..cell.formula_open.end + close,
            text,
        });
    }
    Ok((out, stats))
}

/// A defined name of `names` (lowercased) that the rewrite wraps whole in
/// `@(...)`, possibly sheet-qualified. A linked workbook's name is matched
/// with its `[n]!` qualifier, so it never answers for a local name.
fn intersected_name<'t>(text: &'t str, names: &HashSet<String>) -> Option<&'t str> {
    text.match_indices("@(").find_map(|(at, _)| {
        let inner = &text[at + 2..];
        let inner = &inner[..inner.find(')')?];
        let trimmed = inner.trim();
        let key = if trimmed.starts_with('[') {
            trimmed
        } else {
            trimmed.rsplit('!').next().unwrap_or(trimmed).trim()
        };
        names.contains(&key.to_ascii_lowercase()).then_some(inner)
    })
}
