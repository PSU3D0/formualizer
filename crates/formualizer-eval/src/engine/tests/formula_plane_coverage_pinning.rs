//! Span-coverage pinning test for the FormulaPlane authoritative placement
//! verdicts over the shared `formualizer_testkit::fp_coverage` corpus.
//!
//! This is the regression net for fingerprint-coverage expansions: every
//! generator section pins either full span acceptance or a specific
//! `PlacementFallbackReason`. When a new reference kind gains span support
//! (named ranges, whole-axis refs, mixed-anchor ranges, ...), exactly one
//! section's expectation flips here — update the generator's verdict, not the
//! test logic.
//!
//! Each section is ingested into an isolated engine so the global
//! `FormulaIngestReport` histogram attributes cleanly to that section. Values
//! are additionally compared cell-by-cell against a `FormulaPlaneMode::Off`
//! engine: authoritative mode must never change results.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use formualizer_testkit::fp_coverage::{self, CoverageWorkbook, Section, SectionVerdict, generate};

/// Must be >= 100: non-constant spans below
/// `MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS` are demoted with `SmallDomain`,
/// which would mask the verdicts this test pins.
const ROWS_PER_SECTION: u32 = 120;
const SEED: u64 = 42;

fn build_engine(
    mode: FormulaPlaneMode,
    corpus: &CoverageWorkbook,
    section: &Section,
) -> Engine<TestWorkbook> {
    let cfg = EvalConfig::default().with_formula_plane_mode(mode);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    // Sections whose sheet carries no literal cells (e.g. cross_sheet) still
    // need the sheet to exist before formula ingest.
    engine.add_sheet(section.sheet).unwrap();
    for cell in &section.values {
        engine
            .set_cell_value(
                cell.sheet,
                cell.row,
                cell.col,
                LiteralValue::Number(cell.value),
            )
            .unwrap();
    }
    // Shared Data sheet (read by cross_sheet; harmless elsewhere, only set
    // when the section actually references it to keep engines minimal).
    if section.name == "cross_sheet" {
        for cell in &corpus.data_values {
            engine
                .set_cell_value(
                    cell.sheet,
                    cell.row,
                    cell.col,
                    LiteralValue::Number(cell.value),
                )
                .unwrap();
        }
    }
    for named in &corpus.named_ranges {
        if named.sheet != section.sheet {
            continue;
        }
        let sheet_id = engine.graph.sheet_id_mut(named.sheet);
        let start = CellRef::new(
            sheet_id,
            Coord::new(named.start_row - 1, named.start_col - 1, true, true),
        );
        let end = CellRef::new(
            sheet_id,
            Coord::new(named.end_row - 1, named.end_col - 1, true, true),
        );
        engine
            .define_name(
                named.name,
                NamedDefinition::Range(RangeRef::new(start, end)),
                NameScope::Workbook,
            )
            .unwrap();
    }
    engine
}

fn ingest_section(
    engine: &mut Engine<TestWorkbook>,
    section: &Section,
) -> crate::engine::FormulaIngestReport {
    let mut records = Vec::with_capacity(section.formulas.len());
    for cell in &section.formulas {
        let ast = parse(&cell.formula).unwrap_or_else(|e| {
            panic!("section {}: parse {:?}: {e:?}", section.name, cell.formula)
        });
        let ast_id = engine.intern_formula_ast(&ast);
        records.push(FormulaIngestRecord::new(
            cell.row,
            cell.col,
            ast_id,
            Some(Arc::<str>::from(cell.formula.as_str())),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(section.sheet, records)])
        .expect("ingest formulas")
}

fn assert_values_match(
    section: &Section,
    auth: &mut Engine<TestWorkbook>,
    off: &mut Engine<TestWorkbook>,
) {
    for cell in &section.formulas {
        let got = auth.get_cell_value(cell.sheet, cell.row, cell.col);
        let expected = off.get_cell_value(cell.sheet, cell.row, cell.col);
        let equal = match (&got, &expected) {
            (Some(a), Some(b)) => literal_eq(a, b),
            (a, b) => a == b,
        };
        assert!(
            equal,
            "section {}: value mismatch at {}!R{}C{} ({}): auth={got:?} off={expected:?}",
            section.name, cell.sheet, cell.row, cell.col, cell.formula
        );
    }
}

fn as_num(v: &LiteralValue) -> Option<f64> {
    match v {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn literal_eq(a: &LiteralValue, b: &LiteralValue) -> bool {
    match (as_num(a), as_num(b)) {
        (Some(x), Some(y)) => {
            let scale = x.abs().max(y.abs()).max(1.0);
            (x - y).abs() <= scale * 1e-9
        }
        _ => a == b,
    }
}

#[test]
fn fp_coverage_corpus_pins_section_verdicts_and_values() {
    let corpus = generate(ROWS_PER_SECTION, SEED, false);
    assert_eq!(corpus.sections.len(), fp_coverage::SECTION_COUNT);

    for section in &corpus.sections {
        let n = section.formulas.len() as u64;
        assert_eq!(n, ROWS_PER_SECTION as u64, "section {}", section.name);

        let mut auth = build_engine(
            FormulaPlaneMode::AuthoritativeExperimental,
            &corpus,
            section,
        );
        let report = ingest_section(&mut auth, section);
        assert_eq!(
            report.formula_cells_seen, n,
            "section {}: formula_cells_seen",
            section.name
        );

        match section.verdict {
            SectionVerdict::Span => {
                assert_eq!(
                    report.shadow_accepted_span_cells, n,
                    "section {}: expected all {n} cells span-accepted; fallback histogram: {:?}",
                    section.name, report.fallback_reasons
                );
                assert_eq!(
                    report.shadow_fallback_cells, 0,
                    "section {}: unexpected legacy cells; histogram: {:?}",
                    section.name, report.fallback_reasons
                );
                assert!(
                    report.shadow_spans_created >= 1,
                    "section {}: expected at least one span",
                    section.name
                );
            }
            SectionVerdict::Reject { placement_reason } => {
                assert_eq!(
                    report.shadow_accepted_span_cells, 0,
                    "section {}: expected zero span-accepted cells; histogram: {:?}",
                    section.name, report.fallback_reasons
                );
                let count = report
                    .fallback_reasons
                    .get(placement_reason)
                    .copied()
                    .unwrap_or(0);
                assert_eq!(
                    count, n,
                    "section {}: expected fallback reason {placement_reason:?} x{n}; histogram: {:?}",
                    section.name, report.fallback_reasons
                );
            }
        }

        // Authoritative mode must not change values: compare against Off.
        auth.evaluate_all().expect("evaluate authoritative");
        let mut off = build_engine(FormulaPlaneMode::Off, &corpus, section);
        let _ = ingest_section(&mut off, section);
        off.evaluate_all().expect("evaluate off");
        assert_values_match(section, &mut auth, &mut off);
    }
}

/// Combined run: all sections in one engine; the aggregate histogram is the
/// sum of the per-section expectations (no cross-section interference).
#[test]
fn fp_coverage_corpus_combined_totals() {
    let corpus = generate(ROWS_PER_SECTION, SEED, false);
    let n = ROWS_PER_SECTION as u64;

    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    for section in &corpus.sections {
        engine.add_sheet(section.sheet).unwrap();
    }
    for cell in corpus
        .sections
        .iter()
        .flat_map(|s| s.values.iter())
        .chain(corpus.data_values.iter())
    {
        engine
            .set_cell_value(
                cell.sheet,
                cell.row,
                cell.col,
                LiteralValue::Number(cell.value),
            )
            .unwrap();
    }
    for named in &corpus.named_ranges {
        let sheet_id = engine.graph.sheet_id_mut(named.sheet);
        let start = CellRef::new(
            sheet_id,
            Coord::new(named.start_row - 1, named.start_col - 1, true, true),
        );
        let end = CellRef::new(
            sheet_id,
            Coord::new(named.end_row - 1, named.end_col - 1, true, true),
        );
        engine
            .define_name(
                named.name,
                NamedDefinition::Range(RangeRef::new(start, end)),
                NameScope::Workbook,
            )
            .unwrap();
    }

    let mut batches = Vec::new();
    for section in &corpus.sections {
        let mut records = Vec::new();
        for cell in &section.formulas {
            let ast = parse(&cell.formula).unwrap();
            let ast_id = engine.intern_formula_ast(&ast);
            records.push(FormulaIngestRecord::new(
                cell.row,
                cell.col,
                ast_id,
                Some(Arc::<str>::from(cell.formula.as_str())),
            ));
        }
        batches.push(FormulaIngestBatch::new(section.sheet, records));
    }
    let report = engine.ingest_formula_batches(batches).expect("ingest");

    let expected_span_sections = corpus
        .sections
        .iter()
        .filter(|s| s.verdict == SectionVerdict::Span)
        .count() as u64;
    let expected_reject_sections = corpus.sections.len() as u64 - expected_span_sections;

    assert_eq!(report.formula_cells_seen, corpus.total_formula_cells());
    assert_eq!(
        report.shadow_accepted_span_cells,
        expected_span_sections * n
    );
    assert_eq!(report.shadow_fallback_cells, expected_reject_sections * n);

    let mut expected_histogram: BTreeMap<&'static str, u64> = BTreeMap::new();
    for section in &corpus.sections {
        if let SectionVerdict::Reject { placement_reason } = section.verdict {
            *expected_histogram.entry(placement_reason).or_default() += n;
        }
    }
    for (reason, count) in &expected_histogram {
        assert_eq!(
            report.fallback_reasons.get(*reason).copied().unwrap_or(0),
            *count,
            "combined histogram for {reason}: {:?}",
            report.fallback_reasons
        );
    }

    engine.evaluate_all().expect("evaluate combined");
}

/// Canonical-template reject kinds per section (template-canonicalization
/// layer, below placement). Only compiled with `formula_plane_diagnostics`.
#[cfg(feature = "formula_plane_diagnostics")]
#[test]
fn fp_coverage_corpus_pins_canonical_reject_kinds() {
    let corpus = generate(ROWS_PER_SECTION, SEED, false);
    for section in &corpus.sections {
        let cell = section.formulas.first().expect("non-empty section");
        let ast = parse(&cell.formula).unwrap();
        let diag = crate::formula_plane::diagnostics::canonical_template_diagnostic(
            &ast, cell.row, cell.col,
        );
        for expected in section.expected_canonical_reject_kinds {
            assert!(
                diag.reject_kinds.iter().any(|k| k == expected),
                "section {}: expected canonical reject kind {expected:?}, got {:?}",
                section.name,
                diag.reject_kinds
            );
        }
        if section.expected_canonical_reject_kinds.is_empty()
            && section.verdict == SectionVerdict::Span
        {
            assert!(
                diag.authority_supported,
                "section {}: canonical template unexpectedly rejected: {:?}",
                section.name, diag.reject_reasons
            );
        }
    }
}
