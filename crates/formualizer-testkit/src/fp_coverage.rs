//! Deterministic mixed-workbook generator for FormulaPlane span-coverage
//! measurement.
//!
//! Produces a realistic blend of formula families that real models use, with a
//! *known expected-support profile* per section: each section is either
//! expected to be fully accepted into FormulaPlane spans or expected to fall
//! back to the legacy graph with a specific `PlacementFallbackReason`.
//!
//! The generator is representation-agnostic (plain rows/cols/strings, no
//! spreadsheet backend types) so the same corpus drives:
//!
//! - the `probe-fp-coverage` bench-core binary (via an XLSX fixture), and
//! - the engine pinning test
//!   (`formualizer-eval/src/engine/tests/formula_plane_coverage_pinning.rs`)
//!   which is the regression net for upcoming fingerprint expansions. When a
//!   new reference kind gains span support (e.g. named ranges), exactly one
//!   section's expectation flips here.
//!
//! Layout: every section lives on its own sheet; formula rows are
//! `2..=rows_per_section + 1` (row 1 is reserved as a header/aux row). The
//! `cross_sheet` section additionally reads from the shared `Data` sheet.
//!
//! NOTE: FormulaPlane only promotes non-constant spans with at least
//! `MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS` (currently 100) members; callers
//! that assert `Span` verdicts must use `rows_per_section >= 100`.

/// Shared data sheet read by the `cross_sheet` section.
pub const DATA_SHEET: &str = "Data";

/// Number of sections emitted with `include_broken = false`.
///
/// Useful for sizing: total formula cells = `SECTION_COUNT * rows_per_section`.
pub const SECTION_COUNT: usize = 10;

/// Expected FormulaPlane placement verdict for every formula cell of a section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SectionVerdict {
    /// Every formula cell is expected to be accepted into a span.
    Span,
    /// Every formula cell is expected to stay on the legacy graph, recorded
    /// under this `PlacementFallbackReason` debug name in
    /// `FormulaIngestReport::fallback_reasons`.
    Reject { placement_reason: &'static str },
}

/// A literal input cell.
#[derive(Clone, Debug)]
pub struct ValueCell {
    pub sheet: &'static str,
    pub row: u32,
    pub col: u32,
    pub value: f64,
}

/// A formula cell (formula text includes the leading `=`).
#[derive(Clone, Debug)]
pub struct FormulaCell {
    pub sheet: &'static str,
    pub row: u32,
    pub col: u32,
    pub formula: String,
}

/// A workbook-scoped named range required by a section.
#[derive(Clone, Copy, Debug)]
pub struct NamedRangeSpec {
    pub name: &'static str,
    pub sheet: &'static str,
    /// 1-based inclusive bounds.
    pub start_row: u32,
    pub start_col: u32,
    pub end_row: u32,
    pub end_col: u32,
}

/// One homogeneous formula family with a known expected verdict.
#[derive(Clone, Debug)]
pub struct Section {
    pub name: &'static str,
    pub sheet: &'static str,
    pub verdict: SectionVerdict,
    /// Expected canonical-template reject kinds (diagnostic labels from
    /// `formula_plane_diagnostics`), empty for should-span sections.
    pub expected_canonical_reject_kinds: &'static [&'static str],
    pub values: Vec<ValueCell>,
    pub formulas: Vec<FormulaCell>,
    pub notes: &'static str,
}

/// Full generated corpus.
#[derive(Clone, Debug)]
pub struct CoverageWorkbook {
    pub sections: Vec<Section>,
    pub named_ranges: Vec<NamedRangeSpec>,
    /// Values on the shared `Data` sheet (read by `cross_sheet`).
    pub data_values: Vec<ValueCell>,
}

impl CoverageWorkbook {
    pub fn total_formula_cells(&self) -> u64 {
        self.sections.iter().map(|s| s.formulas.len() as u64).sum()
    }
}

/// Cheap deterministic value mixer so data columns are not trivially uniform.
/// (splitmix64 finalizer; stable across platforms.)
fn mix(seed: u64, a: u64, b: u64) -> f64 {
    let mut z = seed
        .wrapping_add(a.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(b.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // Map to a small positive range with one decimal, keeps SUM/IF behavior tame.
    ((z % 1000) as f64) / 10.0
}

/// Generate the coverage corpus.
///
/// * `rows_per_section` — formula cells per section (>= 100 required for
///   `Span` verdicts to hold; see module note on the promotion threshold).
/// * `seed` — perturbs data values only; the formula structure (and therefore
///   the expected-support profile) is independent of the seed.
/// * `include_broken` — include sections that are excluded by default because
///   they produce wrong values or panics under authoritative mode. Currently
///   there are none; the flag is the standing escape hatch demanded by the
///   probe contract.
pub fn generate(rows_per_section: u32, seed: u64, include_broken: bool) -> CoverageWorkbook {
    let n = rows_per_section;
    let first = 2u32; // formula/data rows are 2..=last
    let last = n + 1;
    let rows = first..=last;

    let mut sections: Vec<Section> = Vec::new();

    // ------------------------------------------------------------------
    // (a) row_arith — row-shifted arithmetic column (=B2*C2+A2 style).
    // Expected: SPAN (relative refs shift uniformly with the row).
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("RowArith", r, 1, r as f64)); // A
            values.push(val("RowArith", r, 2, mix(seed, r as u64, 1))); // B
            values.push(val("RowArith", r, 3, ((r % 7) + 1) as f64)); // C
            formulas.push(formula("RowArith", r, 4, format!("=B{r}*C{r}+A{r}")));
        }
        sections.push(Section {
            name: "row_arith",
            sheet: "RowArith",
            verdict: SectionVerdict::Span,
            expected_canonical_reject_kinds: &[],
            values,
            formulas,
            notes: "Row-shifted arithmetic =B{r}*C{r}+A{r}; canonical relative offsets, one span.",
        });
    }

    // ------------------------------------------------------------------
    // (b) anchored_agg — fully-anchored aggregate, identical every row
    // (=SUM($B$2:$B$last)). Expected: SPAN (constant-result span; exempt
    // from the 100-cell promotion threshold).
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("AnchoredAgg", r, 2, mix(seed, r as u64, 2))); // B
            formulas.push(formula(
                "AnchoredAgg",
                r,
                3,
                format!("=SUM($B$2:$B${last})"),
            ));
        }
        sections.push(Section {
            name: "anchored_agg",
            sheet: "AnchoredAgg",
            verdict: SectionVerdict::Span,
            expected_canonical_reject_kinds: &[],
            values,
            formulas,
            notes: "Anchored aggregate =SUM($B$2:$B$last), identical per row; constant-result span.",
        });
    }

    // ------------------------------------------------------------------
    // (c) sumifs_fixed — SUMIFS with fixed (absolute) value/criteria ranges
    // and a row-relative criterion cell. Expected: SPAN.
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("SumifsFixed", r, 1, (r % 5) as f64)); // A: category
            values.push(val("SumifsFixed", r, 2, mix(seed, r as u64, 3))); // B: amount
            formulas.push(formula(
                "SumifsFixed",
                r,
                3,
                format!("=SUMIFS($B$2:$B${last},$A$2:$A${last},A{r})"),
            ));
        }
        sections.push(Section {
            name: "sumifs_fixed",
            sheet: "SumifsFixed",
            verdict: SectionVerdict::Span,
            expected_canonical_reject_kinds: &[],
            values,
            formulas,
            notes: "SUMIFS with fixed criteria/value ranges, row-relative criterion cell.",
        });
    }

    // ------------------------------------------------------------------
    // (d) lookup — VLOOKUP column over an anchored table. Expected: SPAN.
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            // A: lookup keys, a permutation-ish walk over 1..=n.
            let key = ((u64::from(r - first) * 7) % u64::from(n)) + 1;
            values.push(val("Lookup", r, 1, key as f64));
            // H/I: sorted lookup table keyed 1..=n.
            values.push(val("Lookup", r, 8, (r - 1) as f64));
            values.push(val("Lookup", r, 9, mix(seed, (r - 1) as u64, 4)));
            formulas.push(formula(
                "Lookup",
                r,
                3,
                format!("=VLOOKUP(A{r},$H$2:$I${last},2,FALSE)"),
            ));
        }
        sections.push(Section {
            name: "lookup",
            sheet: "Lookup",
            verdict: SectionVerdict::Span,
            expected_canonical_reject_kinds: &[],
            values,
            formulas,
            notes: "VLOOKUP over anchored $H$2:$I$last table, exact match.",
        });
    }

    // ------------------------------------------------------------------
    // (e) nested_if_literals — nested IF whose literal operands vary per
    // row. Expected: SPAN via parameterized literal bindings (the
    // parameterized canonical template is shared; literals become slots).
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("NestedIf", r, 2, mix(seed, r as u64, 5))); // B
            let t = (r % 50) + 10;
            let m = (r % 3) + 2;
            let k = (r % 9) + 1;
            formulas.push(formula(
                "NestedIf",
                r,
                3,
                format!("=IF(B{r}>{t},B{r}*{m},IF(B{r}>{k},B{r}+{k},{m}))"),
            ));
        }
        sections.push(Section {
            name: "nested_if_literals",
            sheet: "NestedIf",
            verdict: SectionVerdict::Span,
            expected_canonical_reject_kinds: &[],
            values,
            formulas,
            notes: "Nested IF with per-row literal thresholds; spans via literal-slot bindings.",
        });
    }

    // ------------------------------------------------------------------
    // (f) whole_axis — whole-column reference =SUM(A:A).
    // Expected: SPAN. Whole-axis references gained canonical + placement
    // support (AxisRef::WholeAxis); pinned empirically on 2026-06-10.
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("WholeAxis", r, 1, mix(seed, r as u64, 6))); // A
            formulas.push(formula("WholeAxis", r, 3, "=SUM(A:A)".to_string()));
        }
        sections.push(Section {
            name: "whole_axis",
            sheet: "WholeAxis",
            verdict: SectionVerdict::Span,
            expected_canonical_reject_kinds: &[],
            values,
            formulas,
            notes: "Whole-column =SUM(A:A); whole-axis refs are span-supported (constant family).",
        });
    }

    // ------------------------------------------------------------------
    // (g) named_range — reference through a workbook-scoped defined name.
    // Expected: REJECT (canonical NamedReference -> placement
    // UnsupportedCanonicalTemplate). Flips to SPAN when named-range
    // fingerprinting lands.
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("NamedRange", r, 2, mix(seed, r as u64, 7))); // B
            formulas.push(formula(
                "NamedRange",
                r,
                3,
                format!("=SUM(CovNamedData)+A{r}"),
            ));
            values.push(val("NamedRange", r, 1, r as f64)); // A
        }
        sections.push(Section {
            name: "named_range",
            sheet: "NamedRange",
            verdict: SectionVerdict::Reject {
                placement_reason: "UnsupportedCanonicalTemplate",
            },
            expected_canonical_reject_kinds: &["named_reference"],
            values,
            formulas,
            notes: "=SUM(CovNamedData)+A{r}; canonical reject NamedReference.",
        });
    }

    // ------------------------------------------------------------------
    // (h) mixed_anchor — range with relative start / absolute end
    // (=SUM($A{r}:$A$last)): the read region shrinks as the row advances,
    // so member templates are not row-translation-equivalent.
    // Expected: REJECT.
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("MixedAnchor", r, 1, mix(seed, r as u64, 8))); // A
            formulas.push(formula(
                "MixedAnchor",
                r,
                3,
                format!("=SUM($A{r}:$A${last})"),
            ));
        }
        sections.push(Section {
            name: "mixed_anchor",
            sheet: "MixedAnchor",
            verdict: SectionVerdict::Reject {
                placement_reason: "UnsupportedDependencySummary",
            },
            expected_canonical_reject_kinds: &[],
            values,
            formulas,
            notes: "Mixed-anchor =SUM($A{r}:$A$last); canonical OK (MixedAnchors flag) but the \
                    per-row shrinking-tail read region has no supported dependency summary.",
        });
    }

    // ------------------------------------------------------------------
    // (i) volatile — TODAY()-based column. Expected: REJECT (canonical
    // VolatileFunction/ParserVolatileFlag -> UnsupportedCanonicalTemplate).
    // TODAY() (not NOW()) keeps values stable within a calendar day so the
    // probe's ON-vs-OFF value-equality check stays meaningful.
    // ------------------------------------------------------------------
    {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for r in rows.clone() {
            values.push(val("Volatile", r, 2, mix(seed, r as u64, 9))); // B
            formulas.push(formula("Volatile", r, 3, format!("=B{r}+TODAY()*0")));
        }
        sections.push(Section {
            name: "volatile",
            sheet: "Volatile",
            verdict: SectionVerdict::Reject {
                placement_reason: "UnsupportedCanonicalTemplate",
            },
            expected_canonical_reject_kinds: &["volatile_function"],
            values,
            formulas,
            notes: "=B{r}+TODAY()*0; volatile reject. TODAY (not NOW) keeps ON/OFF values equal.",
        });
    }

    // ------------------------------------------------------------------
    // (j) cross_sheet — row-shifted read from the shared Data sheet
    // (=Data!B{r}*2). Canonical templates support explicit sheet bindings.
    // Expected: SPAN.
    // ------------------------------------------------------------------
    {
        let mut formulas = Vec::new();
        for r in rows.clone() {
            formulas.push(formula("CrossSheet", r, 3, format!("=Data!B{r}*2")));
        }
        sections.push(Section {
            name: "cross_sheet",
            sheet: "CrossSheet",
            verdict: SectionVerdict::Span,
            expected_canonical_reject_kinds: &[],
            values: Vec::new(),
            formulas,
            notes: "Row-shifted cross-sheet =Data!B{r}*2; explicit sheet binding is supported.",
        });
    }

    // No sections are currently quarantined as broken under authoritative
    // mode. If one regresses (wrong values / panic), move it behind this flag
    // with a loud comment and a minimal repro reference.
    let _ = include_broken;

    let data_values = rows
        .clone()
        .map(|r| val(DATA_SHEET, r, 2, mix(seed, r as u64, 10)))
        .collect();

    let named_ranges = vec![NamedRangeSpec {
        name: "CovNamedData",
        sheet: "NamedRange",
        start_row: first,
        start_col: 2,
        end_row: last,
        end_col: 2,
    }];

    debug_assert_eq!(sections.len(), SECTION_COUNT);

    CoverageWorkbook {
        sections,
        named_ranges,
        data_values,
    }
}

fn val(sheet: &'static str, row: u32, col: u32, value: f64) -> ValueCell {
    ValueCell {
        sheet,
        row,
        col,
        value,
    }
}

fn formula(sheet: &'static str, row: u32, col: u32, formula: String) -> FormulaCell {
    FormulaCell {
        sheet,
        row,
        col,
        formula,
    }
}
