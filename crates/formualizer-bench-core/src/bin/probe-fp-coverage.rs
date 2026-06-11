//! FormulaPlane span-coverage probe.
//!
//! Standing measurement for fingerprint-coverage expansions: on a realistic
//! mixed workbook (shared `formualizer_testkit::fp_coverage` generator, the
//! same corpus pinned by the engine test
//! `formula_plane_coverage_pinning.rs`), reports:
//!
//! - what fraction of formula cells get accepted into spans under
//!   `FormulaPlaneMode::AuthoritativeExperimental`,
//! - the placement fallback-reason histogram (and the canonical-template
//!   reject-kind histogram from the diagnostics sidecar),
//! - wall-clock for load + first eval + warm recalc with FormulaPlane ON vs
//!   OFF on the same workbook (speedup-per-coverage-point baseline),
//! - a cell-by-cell ON-vs-OFF value-equality verdict (authoritative mode must
//!   never change results).
//!
//! Run:
//!
//! ```bash
//! cargo run -p formualizer-bench-core --features formualizer_runner \
//!   --release --bin probe-fp-coverage -- --rows-per-section 2000
//! ```

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --release --bin probe-fp-coverage"
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
mod probe {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    use anyhow::{Context, Result};
    use clap::Parser;
    use formualizer_eval::engine::{EvalConfig, FormulaPlaneMode};
    use formualizer_eval::formula_plane::diagnostics::canonical_template_diagnostic;
    use formualizer_parse::parser::parse;
    use formualizer_testkit::build_workbook;
    use formualizer_testkit::fp_coverage::{CoverageWorkbook, SectionVerdict, generate};
    use formualizer_workbook::{
        LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
    };
    use serde::Serialize;

    #[derive(Debug, Parser)]
    #[command(about = "FormulaPlane span-coverage probe over the shared fp_coverage mixed corpus")]
    pub struct Cli {
        /// Formula cells per section (12 sections; default ~24k cells total).
        #[arg(long, default_value_t = 2_000)]
        rows_per_section: u32,
        /// Seed perturbing data values (formula structure is seed-independent).
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Include generator sections quarantined for authoritative-mode bugs
        /// (currently none).
        #[arg(long, default_value_t = false)]
        include_broken: bool,
        /// Subset of section names to keep (comma separated); empty = all.
        #[arg(long, default_value = "")]
        only: String,
    }

    #[derive(Debug, Serialize, Clone)]
    struct ModeTimings {
        mode: String,
        load_ms: u128,
        eval_first_ms: u128,
        eval_warm_ms: u128,
    }

    #[derive(Debug, Serialize)]
    struct SectionReport {
        name: &'static str,
        sheet: &'static str,
        formula_cells: u64,
        expected_verdict: String,
        /// Evaluated value of the section's first formula cell (Off mode);
        /// guards against sections that "pass" equality by erroring in both
        /// modes (e.g. an unresolved defined name yielding #NAME? twice).
        sample_value: String,
        notes: &'static str,
    }

    #[derive(Debug, Serialize)]
    struct CoverageReport {
        formula_cells_seen: u64,
        accepted_span_cells: u64,
        legacy_cells: u64,
        coverage_pct: f64,
        spans_created: u64,
        templates_interned: u64,
        formula_vertices_avoided: u64,
        ast_roots_avoided: u64,
        edge_rows_avoided: u64,
        fallback_reasons: BTreeMap<String, u64>,
    }

    #[derive(Debug, Serialize)]
    struct ValueEquality {
        cells_compared: u64,
        mismatches: u64,
        examples: Vec<String>,
    }

    #[derive(Debug, Serialize)]
    struct ProbeReport {
        rows_per_section: u32,
        seed: u64,
        total_formula_cells: u64,
        sections: Vec<SectionReport>,
        coverage: CoverageReport,
        /// Canonical-template reject kinds weighted by section cell count
        /// (template-canonicalization layer, below placement).
        canonical_reject_kinds: BTreeMap<String, u64>,
        off: ModeTimings,
        auth: ModeTimings,
        speedup_first: f64,
        speedup_warm: f64,
        value_equality: ValueEquality,
    }

    fn build_fixture(corpus: &CoverageWorkbook) -> PathBuf {
        build_workbook(|book| {
            for section in &corpus.sections {
                let _ = book.new_sheet(section.sheet);
            }
            let _ = book.new_sheet(formualizer_testkit::fp_coverage::DATA_SHEET);

            for cell in corpus
                .sections
                .iter()
                .flat_map(|s| s.values.iter())
                .chain(corpus.data_values.iter())
            {
                book.get_sheet_by_name_mut(cell.sheet)
                    .expect("sheet exists")
                    .get_cell_mut((cell.col, cell.row))
                    .set_value_number(cell.value);
            }
            for cell in corpus.sections.iter().flat_map(|s| s.formulas.iter()) {
                book.get_sheet_by_name_mut(cell.sheet)
                    .expect("sheet exists")
                    .get_cell_mut((cell.col, cell.row))
                    .set_formula(cell.formula.trim_start_matches('='));
            }
            for named in &corpus.named_ranges {
                let address = format!(
                    "{}!${}${}:${}${}",
                    named.sheet,
                    col_letter(named.start_col),
                    named.start_row,
                    col_letter(named.end_col),
                    named.end_row,
                );
                book.get_sheet_by_name_mut(named.sheet)
                    .expect("named-range sheet exists")
                    .add_defined_name(named.name, &address)
                    .expect("add defined name");
            }
        })
    }

    fn col_letter(mut col: u32) -> String {
        let mut s = String::new();
        while col > 0 {
            let rem = ((col - 1) % 26) as u8;
            s.insert(0, (b'A' + rem) as char);
            col = (col - 1) / 26;
        }
        s
    }

    struct ModeRun {
        timings: ModeTimings,
        values: Vec<Option<LiteralValue>>,
        coverage: Option<CoverageReport>,
    }

    fn run_mode(mode: FormulaPlaneMode, path: &Path, corpus: &CoverageWorkbook) -> Result<ModeRun> {
        let mut config = WorkbookConfig::ephemeral();
        config.eval = EvalConfig::default().with_formula_plane_mode(mode);

        let load_start = Instant::now();
        let backend = UmyaAdapter::open_path(path)?;
        let mut wb = Workbook::from_reader(backend, LoadStrategy::EagerAll, config)?;
        let load_ms = load_start.elapsed().as_millis();

        let t = Instant::now();
        wb.evaluate_all()?;
        let eval_first_ms = t.elapsed().as_millis();

        let t = Instant::now();
        wb.evaluate_all()?;
        let eval_warm_ms = t.elapsed().as_millis();

        let mut values = Vec::new();
        for cell in corpus.sections.iter().flat_map(|s| s.formulas.iter()) {
            values.push(wb.get_value(cell.sheet, cell.row, cell.col));
        }

        let coverage = if mode == FormulaPlaneMode::AuthoritativeExperimental {
            let report = wb.engine().formula_ingest_report_total();
            let seen = report.formula_cells_seen;
            let accepted = report.shadow_accepted_span_cells;
            Some(CoverageReport {
                formula_cells_seen: seen,
                accepted_span_cells: accepted,
                legacy_cells: report.shadow_fallback_cells,
                coverage_pct: if seen == 0 {
                    0.0
                } else {
                    accepted as f64 * 100.0 / seen as f64
                },
                spans_created: report.shadow_spans_created,
                templates_interned: report.shadow_templates_interned,
                formula_vertices_avoided: report.graph_formula_vertices_avoided_shadow,
                ast_roots_avoided: report.ast_roots_avoided_shadow,
                edge_rows_avoided: report.edge_rows_avoided_shadow,
                fallback_reasons: report.fallback_reasons.clone(),
            })
        } else {
            None
        };

        Ok(ModeRun {
            timings: ModeTimings {
                mode: format!("{mode:?}"),
                load_ms,
                eval_first_ms,
                eval_warm_ms,
            },
            values,
            coverage,
        })
    }

    fn literal_eq(a: &LiteralValue, b: &LiteralValue) -> bool {
        let num = |v: &LiteralValue| -> Option<f64> {
            match v {
                LiteralValue::Number(n) => Some(*n),
                LiteralValue::Int(i) => Some(*i as f64),
                LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
                _ => None,
            }
        };
        match (num(a), num(b)) {
            (Some(x), Some(y)) => {
                let scale = x.abs().max(y.abs()).max(1.0);
                (x - y).abs() <= scale * 1e-9
            }
            _ => a == b,
        }
    }

    fn ratio(off: f64, auth: f64) -> f64 {
        if auth <= 0.0 {
            if off <= 0.0 { 1.0 } else { f64::INFINITY }
        } else {
            off / auth
        }
    }

    pub fn main() -> Result<()> {
        let cli = Cli::parse();
        let mut corpus = generate(cli.rows_per_section, cli.seed, cli.include_broken);
        if !cli.only.trim().is_empty() {
            let keep: Vec<&str> = cli.only.split(',').map(str::trim).collect();
            corpus.sections.retain(|s| keep.contains(&s.name));
            anyhow::ensure!(
                !corpus.sections.is_empty(),
                "--only {:?} matched no sections",
                cli.only
            );
            let kept_sheets: Vec<&str> = corpus.sections.iter().map(|s| s.sheet).collect();
            corpus
                .named_ranges
                .retain(|n| kept_sheets.contains(&n.sheet));
        }
        let total_formula_cells = corpus.total_formula_cells();
        eprintln!(
            "[probe-fp-coverage] {} sections, {} formula cells, seed {}",
            corpus.sections.len(),
            total_formula_cells,
            cli.seed
        );

        let path = build_fixture(&corpus);

        let off = run_mode(FormulaPlaneMode::Off, &path, &corpus)?;
        let auth = run_mode(FormulaPlaneMode::AuthoritativeExperimental, &path, &corpus)?;
        let coverage = auth.coverage.context("authoritative coverage report")?;

        // Value-equality verdict: authoritative mode must not change results.
        let mut mismatches = 0u64;
        let mut examples = Vec::new();
        let cells: Vec<_> = corpus
            .sections
            .iter()
            .flat_map(|s| s.formulas.iter().map(move |c| (s.name, c)))
            .collect();
        for (i, (section, cell)) in cells.iter().enumerate() {
            let equal = match (&off.values[i], &auth.values[i]) {
                (Some(a), Some(b)) => literal_eq(a, b),
                (a, b) => a == b,
            };
            if !equal {
                mismatches += 1;
                if examples.len() < 10 {
                    examples.push(format!(
                        "{section} {}!R{}C{} ({}): off={:?} auth={:?}",
                        cell.sheet, cell.row, cell.col, cell.formula, off.values[i], auth.values[i]
                    ));
                }
            }
        }

        // Canonical-template reject kinds, weighted by section size. Every
        // section is structurally homogeneous, so one representative formula
        // per section is exact.
        let mut canonical_reject_kinds: BTreeMap<String, u64> = BTreeMap::new();
        for section in &corpus.sections {
            if let Some(cell) = section.formulas.first() {
                let ast = parse(&cell.formula)?;
                let diag = canonical_template_diagnostic(&ast, cell.row, cell.col);
                for kind in diag.reject_kinds {
                    *canonical_reject_kinds.entry(kind).or_default() +=
                        section.formulas.len() as u64;
                }
            }
        }

        let report = ProbeReport {
            rows_per_section: cli.rows_per_section,
            seed: cli.seed,
            total_formula_cells,
            sections: {
                let mut offset = 0usize;
                corpus
                    .sections
                    .iter()
                    .map(|s| {
                        let sample_value = format!("{:?}", off.values[offset]);
                        offset += s.formulas.len();
                        SectionReport {
                            name: s.name,
                            sheet: s.sheet,
                            formula_cells: s.formulas.len() as u64,
                            expected_verdict: match s.verdict {
                                SectionVerdict::Span => "span".to_string(),
                                SectionVerdict::Reject { placement_reason } => {
                                    format!("reject:{placement_reason}")
                                }
                            },
                            sample_value,
                            notes: s.notes,
                        }
                    })
                    .collect()
            },
            coverage,
            canonical_reject_kinds,
            speedup_first: ratio(
                off.timings.eval_first_ms as f64,
                auth.timings.eval_first_ms as f64,
            ),
            speedup_warm: ratio(
                off.timings.eval_warm_ms as f64,
                auth.timings.eval_warm_ms as f64,
            ),
            off: off.timings,
            auth: auth.timings,
            value_equality: ValueEquality {
                cells_compared: cells.len() as u64,
                mismatches,
                examples,
            },
        };

        println!("{}", serde_json::to_string_pretty(&report)?);

        eprintln!();
        eprintln!(
            "# fp-coverage: {:.1}% span coverage ({}/{} cells, {} spans), first eval {}→{} ms, warm {}→{} ms, value mismatches {}",
            report.coverage.coverage_pct,
            report.coverage.accepted_span_cells,
            report.coverage.formula_cells_seen,
            report.coverage.spans_created,
            report.off.eval_first_ms,
            report.auth.eval_first_ms,
            report.off.eval_warm_ms,
            report.auth.eval_warm_ms,
            report.value_equality.mismatches,
        );
        let mut reasons: Vec<_> = report.coverage.fallback_reasons.iter().collect();
        reasons.sort_by(|a, b| b.1.cmp(a.1));
        for (reason, count) in reasons.iter().take(5) {
            eprintln!("  fallback {reason}: {count}");
        }

        if report.value_equality.mismatches > 0 {
            eprintln!("ERROR: authoritative mode changed values; see examples in JSON");
            std::process::exit(1);
        }
        Ok(())
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    probe::main()
}
