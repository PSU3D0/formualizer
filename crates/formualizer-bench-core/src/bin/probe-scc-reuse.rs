//! Probe for issue #368: the per-recalc cost of stable iterative SCCs with
//! and without `EvalConfig::reuse_converged_sccs`, across FormulaPlane modes.
//!
//! Workload shape mirrors the issue: `--sccs` independent convergent ring
//! SCCs of `--members` cells each (column B), `--downstream` chained column
//! families reading the SCC outputs (columns C..), and an unrelated family
//! block (columns G/H/I) used for the "unrelated edit" scenario.
//!
//! Every scenario runs under Off / Shadow / AuthoritativeExperimental and
//! reports wall time, `computed_vertices`, cycle telemetry, and span
//! coverage so the acyclic-downstream share and the SCC share of a no-change
//! recalc can be separated.
//!
//! ```bash
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-reuse -- --sccs 78 --members 120 --downstream 3
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-reuse -- --reuse false   # pre-#368 behavior
//! ```

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("requires --features formualizer_runner");
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
mod probe {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Instant;

    use anyhow::Result;
    use clap::Parser;
    use formualizer_common::LiteralValue;
    use formualizer_eval::engine::{
        CycleConfig, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
    };
    use formualizer_eval::test_workbook::TestWorkbook;
    use formualizer_parse::parser::parse;
    use serde::Serialize;

    const SHEET: &str = "Sheet1";

    #[derive(Debug, Parser)]
    #[command(
        about = "Issue #368 probe: stable iterative SCC recalc cost vs reuse knob and FormulaPlane mode"
    )]
    pub struct Cli {
        /// Number of independent ring SCCs.
        #[arg(long, default_value_t = 78)]
        sccs: usize,
        /// Members per ring SCC.
        #[arg(long, default_value_t = 120)]
        members: usize,
        /// Chained column families downstream of the SCC outputs (0..=3).
        #[arg(long, default_value_t = 3)]
        downstream: usize,
        /// Wrap the ring formula in ROUND(...,6) so the fixed point is exact.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        exact: bool,
        /// No-change recalc rounds.
        #[arg(long, default_value_t = 5)]
        recalcs: usize,
        /// Close the ring (head reads tail). `false` makes column B acyclic
        /// chains with the same InternalDependency shape but no SCCs.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        wrap: bool,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Comma list of modes: off,shadow,auth
        #[arg(long, default_value = "off,shadow,auth")]
        modes: String,
        /// `EvalConfig::reuse_converged_sccs` (default on; `false` restores
        /// the per-recalc redirty of every iterating SCC).
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        reuse: bool,
    }

    fn mix(seed: u64, a: u64) -> f64 {
        let mut z = seed.wrapping_add(a.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        1.0 + (z % 1000) as f64 / 100.0
    }

    fn record(
        engine: &mut Engine<TestWorkbook>,
        row: u32,
        col: u32,
        formula: &str,
    ) -> FormulaIngestRecord {
        let ast = parse(formula).unwrap_or_else(|e| panic!("parse {formula}: {e}"));
        let ast_id = engine.intern_formula_ast(&ast);
        FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
    }

    #[derive(Debug, Serialize, Clone)]
    struct Round {
        kind: String,
        ms: f64,
        computed_vertices: usize,
        static_sccs: usize,
        iterated_sccs: usize,
        converged_sccs: usize,
        capped_sccs: usize,
        settle_passes_total: usize,
        max_abs_delta_at_stop: f64,
        fp_active_spans: usize,
        fp_cycle_member_demotions: u64,
        fp_topology_cache_builds: u64,
    }

    #[derive(Debug, Serialize)]
    struct ModeReport {
        mode: String,
        formula_cells_seen: u64,
        accepted_span_cells: u64,
        legacy_cells: u64,
        spans_created: u64,
        fallback_reasons: BTreeMap<String, u64>,
        rounds: Vec<Round>,
        checksum_b: f64,
        checksum_tail: f64,
    }

    #[derive(Debug, Serialize)]
    struct Report {
        sccs: usize,
        members: usize,
        downstream: usize,
        exact: bool,
        rows: u32,
        modes: Vec<ModeReport>,
    }

    fn parse_mode(s: &str) -> FormulaPlaneMode {
        match s.trim() {
            "off" => FormulaPlaneMode::Off,
            "shadow" => FormulaPlaneMode::Shadow,
            "auth" => FormulaPlaneMode::AuthoritativeExperimental,
            other => panic!("unknown mode {other}"),
        }
    }

    fn snapshot(e: &Engine<TestWorkbook>, kind: &str, ms: f64, computed: usize) -> Round {
        let t = e.last_cycle_telemetry();
        let bs = e.baseline_stats();
        Round {
            kind: kind.to_string(),
            ms,
            computed_vertices: computed,
            static_sccs: t.static_sccs,
            iterated_sccs: t.iterated_sccs,
            converged_sccs: t.converged_sccs,
            capped_sccs: t.capped_sccs,
            settle_passes_total: t.settle_passes_total,
            max_abs_delta_at_stop: t.max_abs_delta_at_stop,
            fp_active_spans: bs.formula_plane_active_span_count,
            fp_cycle_member_demotions: bs.formula_plane_cycle_member_span_demotions,
            fp_topology_cache_builds: bs.formula_plane_mixed_topology_cache_builds,
        }
    }

    fn timed_eval(e: &mut Engine<TestWorkbook>, kind: &str) -> Round {
        let t0 = Instant::now();
        let r = e.evaluate_all().expect("evaluate_all");
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        snapshot(e, kind, ms, r.computed_vertices)
    }

    fn run_mode(cli: &Cli, mode: FormulaPlaneMode) -> ModeReport {
        let rows = (cli.sccs * cli.members) as u32;
        let mut e = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default()
                .with_formula_plane_mode(mode)
                .with_cycle(CycleConfig::iterate_excel_defaults())
                .with_reuse_converged_sccs(cli.reuse),
        );
        // Inputs: A feeds rings + downstream, G feeds the unrelated block.
        for r in 1..=rows {
            e.set_cell_value(SHEET, r, 1, LiteralValue::Number(mix(cli.seed, r as u64)))
                .unwrap();
            e.set_cell_value(
                SHEET,
                r,
                7,
                LiteralValue::Number(mix(cli.seed, r as u64 + 7919)),
            )
            .unwrap();
        }

        let mut records = Vec::with_capacity(rows as usize * (2 + cli.downstream));
        // Column B: ring SCCs. Member r reads B{r-1}; the ring head reads the
        // ring tail (wraparound). Contractive (0.5) so it converges.
        for k in 0..cli.sccs {
            let base = (k * cli.members) as u32 + 1;
            let tail = base + cli.members as u32 - 1;
            for r in base..=tail {
                let prev = if r == base { tail } else { r - 1 };
                let core = if r == base && !cli.wrap {
                    format!("A{r}")
                } else {
                    format!("0.5*B{prev}+A{r}")
                };
                let src = if cli.exact {
                    format!("=ROUND({core},6)")
                } else {
                    format!("={core}")
                };
                records.push(record(&mut e, r, 2, &src));
            }
        }
        // Downstream chained families reading SCC outputs.
        for r in 1..=rows {
            if cli.downstream >= 1 {
                records.push(record(&mut e, r, 3, &format!("=B{r}*2+A{r}")));
            }
            if cli.downstream >= 2 {
                records.push(record(&mut e, r, 4, &format!("=C{r}+1")));
            }
            if cli.downstream >= 3 {
                records.push(record(&mut e, r, 5, &format!("=D{r}*A{r}")));
            }
            // Unrelated block.
            records.push(record(&mut e, r, 8, &format!("=G{r}*3+1")));
            records.push(record(&mut e, r, 9, &format!("=H{r}+G{r}")));
        }
        let report = e
            .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, records)])
            .expect("ingest");

        let mut rounds = Vec::new();
        rounds.push(timed_eval(&mut e, "initial"));
        for i in 0..cli.recalcs {
            rounds.push(timed_eval(&mut e, &format!("no-change-{i}")));
        }
        // Unrelated edit: a G input feeding only the H/I families.
        e.set_cell_value(SHEET, 5, 7, LiteralValue::Number(123.0))
            .unwrap();
        rounds.push(timed_eval(&mut e, "unrelated-edit"));
        rounds.push(timed_eval(&mut e, "no-change-after-unrelated"));
        // Dependency edit: A input of ring 0's head (feeds one SCC + downstream row).
        e.set_cell_value(SHEET, 1, 1, LiteralValue::Number(50.0))
            .unwrap();
        rounds.push(timed_eval(&mut e, "dependency-edit-one-scc"));
        rounds.push(timed_eval(&mut e, "no-change-after-dependency"));
        // Same-value edit: rewrite A1 with its current value.
        e.set_cell_value(SHEET, 1, 1, LiteralValue::Number(50.0))
            .unwrap();
        rounds.push(timed_eval(&mut e, "same-value-edit"));

        let num = |v: Option<LiteralValue>| match v {
            Some(LiteralValue::Number(n)) => n,
            Some(LiteralValue::Int(i)) => i as f64,
            _ => f64::NAN,
        };
        let mut checksum_b = 0.0;
        let mut checksum_tail = 0.0;
        let tail_col = match cli.downstream {
            0 => 2,
            n => 2 + n as u32,
        };
        for r in 1..=rows {
            checksum_b += num(e.get_cell_value(SHEET, r, 2)) * (r as f64).sqrt();
            checksum_tail += num(e.get_cell_value(SHEET, r, tail_col)) * (r as f64).sqrt();
            checksum_tail += num(e.get_cell_value(SHEET, r, 9));
        }

        ModeReport {
            mode: format!("{mode:?}"),
            formula_cells_seen: report.formula_cells_seen,
            accepted_span_cells: report.shadow_accepted_span_cells,
            legacy_cells: report.shadow_fallback_cells,
            spans_created: report.shadow_spans_created,
            fallback_reasons: report.fallback_reasons.clone(),
            rounds,
            checksum_b,
            checksum_tail,
        }
    }

    pub fn main() -> Result<()> {
        let cli = Cli::parse();
        let modes: Vec<FormulaPlaneMode> = cli.modes.split(',').map(parse_mode).collect();
        let report = Report {
            sccs: cli.sccs,
            members: cli.members,
            downstream: cli.downstream,
            exact: cli.exact,
            rows: (cli.sccs * cli.members) as u32,
            modes: modes.into_iter().map(|m| run_mode(&cli, m)).collect(),
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    probe::main()
}
