//! FormulaPlane span-coverage AUDIT probe v2 — engine-direct differential.
//!
//! v1 (loader-based) hit two fixture artifacts that made it unusable:
//!   * `UmyaAdapter` does not carry the number-format channel at all, so every
//!     temporal/format class silently degraded to plain numbers;
//!   * `CalamineAdapter` under `AuthoritativeExperimental` takes the
//!     compressed/direct ingest route, which needs shared-formula groups in the
//!     source XLSX. umya-authored fixtures have none, so span residency was 0%
//!     for every class.
//!
//! v2 drives `Engine<TestWorkbook>` directly through the same public entry
//! points the engine's own plane tests use (`set_cell_value` +
//! `intern_formula_ast` + `ingest_formula_batches` + `evaluate_all` +
//! `get_cell_value`). That yields real spans AND real formats, and reads
//! through the single temporal-egress funnel.
//!
//! Comparison is STRICT: `LiteralValue` equality with `f64` compared on bit
//! pattern and errors compared on `ExcelErrorKind`. A separate "near" bucket
//! isolates <=1e-9 relative float drift from genuine divergence.

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("requires --features formualizer_runner");
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
mod probe {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use anyhow::Result;
    use chrono::NaiveDate;
    use clap::Parser;
    use formualizer_common::LiteralValue;
    use formualizer_eval::engine::{
        Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
    };
    use formualizer_eval::formula_plane::diagnostics::canonical_template_diagnostic;
    use formualizer_eval::test_workbook::TestWorkbook;
    use formualizer_parse::parser::parse;
    use serde::Serialize;

    const SHEET: &str = "Sheet1";

    #[derive(Debug, Parser)]
    #[command(about = "FormulaPlane span-coverage audit differential (engine-direct)")]
    pub struct Cli {
        /// Formula rows per class (>= 100 for non-constant span promotion).
        #[arg(long, default_value_t = 200)]
        rows: u32,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long, default_value = "")]
        only: String,
    }

    fn mix(seed: u64, a: u64) -> f64 {
        let mut z = seed.wrapping_add(a.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        ((z % 1000) as f64) / 10.0
    }

    /// A class: how to seed inputs, and the formula for each row.
    struct Class {
        name: &'static str,
        expectation: &'static str,
        notes: &'static str,
        /// Extra typed seeding beyond the standard A..D numeric columns.
        seed_extra: fn(&mut Engine<TestWorkbook>, u32, u32, u64),
        formula: fn(u32, u32, u32) -> String,
    }

    const FIRST: u32 = 2;
    /// Result column.
    const OUT_COL: u32 = 6;

    fn base_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
    }

    fn seed_none(_e: &mut Engine<TestWorkbook>, _first: u32, _last: u32, _seed: u64) {}

    /// Column H = Date-typed values (carries the DATE format class).
    fn seed_dates_h(e: &mut Engine<TestWorkbook>, first: u32, last: u32, _seed: u64) {
        for r in first..=last {
            let d = base_date() + chrono::Duration::days(r as i64);
            e.set_cell_value(SHEET, r, 8, LiteralValue::Date(d))
                .unwrap();
        }
    }

    /// Columns H and I both Date-typed (IF arms).
    fn seed_dates_hi(e: &mut Engine<TestWorkbook>, first: u32, last: u32, _seed: u64) {
        for r in first..=last {
            let d = base_date() + chrono::Duration::days(r as i64);
            let d2 = base_date() + chrono::Duration::days(2 * r as i64);
            e.set_cell_value(SHEET, r, 8, LiteralValue::Date(d))
                .unwrap();
            e.set_cell_value(SHEET, r, 9, LiteralValue::Date(d2))
                .unwrap();
        }
    }

    /// Single anchored Date cell at F10-equivalent ($J$1) for constant-result spans.
    fn seed_anchor_date(e: &mut Engine<TestWorkbook>, _first: u32, _last: u32, _seed: u64) {
        e.set_cell_value(SHEET, 1, 10, LiteralValue::Date(base_date()))
            .unwrap();
    }

    /// Lookup table in H (keys) / I (payload).
    fn seed_lookup(e: &mut Engine<TestWorkbook>, first: u32, last: u32, seed: u64) {
        for r in first..=last {
            e.set_cell_value(SHEET, r, 8, LiteralValue::Number(r as f64))
                .unwrap();
            e.set_cell_value(SHEET, r, 9, LiteralValue::Number(mix(seed, r as u64 + 31)))
                .unwrap();
        }
    }

    /// Row-1 payload for whole-row band reads.
    fn seed_row1(e: &mut Engine<TestWorkbook>, _first: u32, _last: u32, _seed: u64) {
        for c in 1..=4u32 {
            e.set_cell_value(SHEET, 1, c, LiteralValue::Number(10.0 * c as f64))
                .unwrap();
        }
    }

    fn classes() -> Vec<Class> {
        vec![
            Class {
                name: "ctl_row_arith",
                expectation: "span",
                notes: "Control: =B{r}*C{r}+A{r}.",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=B{r}*C{r}+A{r}"),
            },
            // ---- #357 / #364 format + temporal egress -------------------
            Class {
                name: "fmt_temporal_arith",
                expectation: "span",
                notes: "=H{r}+30 with a DATE-typed operand; result must egress \
                        as Date under both surfaces (format propagation).",
                seed_extra: seed_dates_h,
                formula: |r, _f, _l| format!("=H{r}+30"),
            },
            Class {
                name: "fmt_temporal_constant_span",
                expectation: "span",
                notes: "=$J$1+0 — constant-result span (single evaluation \
                        broadcast to every placement) with a DATE operand. \
                        Exercises the broadcast format path (fix/fp-format-broadcast-parity).",
                seed_extra: seed_anchor_date,
                formula: |_r, _f, _l| "=$J$1+0".to_string(),
            },
            Class {
                name: "fmt_propagate_if",
                expectation: "span",
                notes: "=IF(B{r}>50,H{r},I{r}) with DATE arms; propagate_format \
                        through a newly-admitted reference-returning function.",
                seed_extra: seed_dates_hi,
                formula: |r, _f, _l| format!("=IF(B{r}>50,H{r},I{r})"),
            },
            Class {
                name: "temporal_native_edate",
                expectation: "span",
                notes: "=EDATE(H{r},1) native temporal producer.",
                seed_extra: seed_dates_h,
                formula: |r, _f, _l| format!("=EDATE(H{r},1)"),
            },
            // ---- #372 reference-returning admission ---------------------
            Class {
                name: "if_scalar_arms",
                expectation: "span",
                notes: "=IF(B{r}>50,C{r},D{r}); admitted by feat/refreturn-span-admission.",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=IF(B{r}>50,C{r},D{r})"),
            },
            Class {
                name: "ifs_scalar_arms",
                expectation: "span",
                notes: "=IFS(B{r}>75,C{r},B{r}>50,D{r},TRUE,0).",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=IFS(B{r}>75,C{r},B{r}>50,D{r},TRUE,0)"),
            },
            Class {
                name: "choose_scalar_arms",
                expectation: "span",
                notes: "=CHOOSE(MOD(A{r},3)+1,B{r},C{r},D{r}).",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=CHOOSE(MOD(A{r},3)+1,B{r},C{r},D{r})"),
            },
            Class {
                name: "if_array_condition",
                expectation: "?",
                notes: "=IF($B$2:$B$4>50,C{r},D{r}) — ARRAY condition with scalar \
                        arms. The admission classifier only requires the condition \
                        to be `safe`, not `scalar`, and the MAY_SPILL reject is \
                        revoked by function NAME, so this shape is admitted.",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=IF($B$2:$B$4>50,C{r},D{r})"),
            },
            Class {
                name: "choose_array_index",
                expectation: "?",
                notes: "=CHOOSE($A$2:$A$4,B{r},C{r},D{r}) — ARRAY index argument.",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=CHOOSE($A$2:$A$4,B{r},C{r},D{r})"),
            },
            Class {
                name: "if_range_arm",
                expectation: "legacy",
                notes: "=SUM(IF(B{r}>50,$C$2:$C$4,$D$2:$D$4)) — RANGE arm; arms must \
                        be scalar, so admission must NOT fire.",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=SUM(IF(B{r}>50,$C$2:$C$4,$D$2:$D$4))"),
            },
            Class {
                name: "if_nested_index_arm",
                expectation: "legacy",
                notes: "=IF(B{r}>50,C{r},INDEX($B$2:$B$9,2)) — non-admitted \
                        reference-returning callee inside an admitted IF.",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=IF(B{r}>50,C{r},INDEX($B$2:$B$9,2))"),
            },
            // ---- INDEX / lookup ----------------------------------------
            Class {
                name: "index_ref",
                expectation: "legacy",
                notes: "=INDEX($B$2:$B${last},A{r}); INDEX is not on the admitted list.",
                seed_extra: seed_none,
                formula: |r, f, l| format!("=INDEX($B${f}:$B${l},A{r}-{})", f - 1),
            },
            Class {
                name: "vlookup",
                expectation: "span",
                notes: "=VLOOKUP(A{r},$H${f}:$I${l},2,FALSE) — scalar-result lookup.",
                seed_extra: seed_lookup,
                formula: |r, f, l| format!("=VLOOKUP(A{r},$H${f}:$I${l},2,FALSE)"),
            },
            // ---- #340 XLOOKUP class rules ------------------------------
            Class {
                name: "xlookup",
                expectation: "?",
                notes: "=XLOOKUP(A{r},$H${f}:$H${l},$I${f}:$I${l},0).",
                seed_extra: seed_lookup,
                formula: |r, f, l| format!("=XLOOKUP(A{r},$H${f}:$H${l},$I${f}:$I${l},0)"),
            },
            Class {
                name: "xmatch",
                expectation: "?",
                notes: "=XMATCH(A{r},$H${f}:$H${l},0).",
                seed_extra: seed_lookup,
                formula: |r, f, l| format!("=XMATCH(A{r},$H${f}:$H${l},0)"),
            },
            // ---- open / whole-axis ranges ------------------------------
            Class {
                name: "open_range_col",
                expectation: "span",
                notes: "=SUM(D:D)+A{r} whole-column read.",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=SUM(D:D)+A{r}"),
            },
            Class {
                name: "open_range_multicol",
                expectation: "span",
                notes: "=SUM(B:D)+A{r} multi-column whole-axis rect (the D:F class).",
                seed_extra: seed_none,
                formula: |r, _f, _l| format!("=SUM(B:D)+A{r}"),
            },
            Class {
                name: "open_range_rows",
                expectation: "span",
                notes: "=SUM($1:$1)+A{r} whole-row read; the band sits on row 1 so \
                        it does not intersect the formula column (the 3:13 class).",
                seed_extra: seed_row1,
                formula: |r, _f, _l| format!("=SUM($1:$1)+A{r}"),
            },
        ]
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

    #[derive(Debug, Serialize, Clone, Default)]
    struct Coverage {
        formula_cells_seen: u64,
        accepted_span_cells: u64,
        legacy_cells: u64,
        coverage_pct: f64,
        spans_created: u64,
        graph_formula_cells_materialized: u64,
        fallback_reasons: BTreeMap<String, u64>,
    }

    struct Run {
        values: Vec<Option<LiteralValue>>,
        /// Values after an edit to a precedent (incremental recalc parity).
        values_after_edit: Vec<Option<LiteralValue>>,
        coverage: Coverage,
    }

    fn run_mode(mode: FormulaPlaneMode, class: &Class, rows: u32, seed: u64) -> Run {
        let last = rows + 1;
        let mut e = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default().with_formula_plane_mode(mode),
        );
        for r in FIRST..=last {
            e.set_cell_value(SHEET, r, 1, LiteralValue::Number(r as f64))
                .unwrap();
            e.set_cell_value(SHEET, r, 2, LiteralValue::Number(mix(seed, r as u64)))
                .unwrap();
            e.set_cell_value(
                SHEET,
                r,
                3,
                LiteralValue::Number(mix(seed, r as u64 + 7919)),
            )
            .unwrap();
            e.set_cell_value(
                SHEET,
                r,
                4,
                LiteralValue::Number(mix(seed, r as u64 + 104_729)),
            )
            .unwrap();
        }
        (class.seed_extra)(&mut e, FIRST, last, seed);

        let records: Vec<_> = (FIRST..=last)
            .map(|r| {
                let src = (class.formula)(r, FIRST, last);
                record(&mut e, r, OUT_COL, &src)
            })
            .collect();
        let report = e
            .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, records)])
            .expect("ingest");
        let seen = report.formula_cells_seen;
        let accepted = report.shadow_accepted_span_cells;
        let coverage = Coverage {
            formula_cells_seen: seen,
            accepted_span_cells: accepted,
            legacy_cells: report.shadow_fallback_cells,
            coverage_pct: if seen == 0 {
                0.0
            } else {
                accepted as f64 * 100.0 / seen as f64
            },
            spans_created: report.shadow_spans_created,
            graph_formula_cells_materialized: report.graph_formula_cells_materialized,
            fallback_reasons: report.fallback_reasons.clone(),
        };

        e.evaluate_all().expect("evaluate");
        let values: Vec<_> = (FIRST..=last)
            .map(|r| e.get_cell_value(SHEET, r, OUT_COL))
            .collect();

        // Incremental recalc parity: perturb a precedent inside the read
        // region and re-evaluate.
        e.set_cell_value(SHEET, 10, 2, LiteralValue::Number(5_000.0))
            .unwrap();
        e.set_cell_value(SHEET, 11, 1, LiteralValue::Number(7_000.0))
            .unwrap();
        e.evaluate_all().expect("re-evaluate");
        let values_after_edit: Vec<_> = (FIRST..=last)
            .map(|r| e.get_cell_value(SHEET, r, OUT_COL))
            .collect();

        Run {
            values,
            values_after_edit,
            coverage,
        }
    }

    fn lit_identical(a: &LiteralValue, b: &LiteralValue) -> bool {
        match (a, b) {
            (LiteralValue::Error(x), LiteralValue::Error(y)) => x.kind == y.kind,
            (LiteralValue::Number(x), LiteralValue::Number(y)) => x.to_bits() == y.to_bits(),
            _ => a == b,
        }
    }

    fn compare(a: &Option<LiteralValue>, b: &Option<LiteralValue>) -> (bool, bool) {
        match (a, b) {
            (Some(x), Some(y)) => {
                if lit_identical(x, y) {
                    return (true, false);
                }
                let num = |v: &LiteralValue| match v {
                    LiteralValue::Number(n) => Some(*n),
                    LiteralValue::Int(i) => Some(*i as f64),
                    _ => None,
                };
                if let (Some(x), Some(y)) = (num(x), num(y)) {
                    let scale = x.abs().max(y.abs()).max(1.0);
                    if (x - y).abs() <= scale * 1e-9 {
                        return (false, true);
                    }
                }
                (false, false)
            }
            (None, None) => (true, false),
            _ => (false, false),
        }
    }

    fn shape(v: &Option<LiteralValue>) -> String {
        match v {
            None => "None".into(),
            Some(LiteralValue::Number(_)) => "Number".into(),
            Some(LiteralValue::Int(_)) => "Int".into(),
            Some(LiteralValue::Text(_)) => "Text".into(),
            Some(LiteralValue::Boolean(_)) => "Boolean".into(),
            Some(LiteralValue::Date(_)) => "Date".into(),
            Some(LiteralValue::DateTime(_)) => "DateTime".into(),
            Some(LiteralValue::Time(_)) => "Time".into(),
            Some(LiteralValue::Duration(_)) => "Duration".into(),
            Some(LiteralValue::Array(_)) => "Array".into(),
            Some(LiteralValue::Error(e)) => format!("Error({:?})", e.kind),
            Some(LiteralValue::Empty) => "Empty".into(),
            Some(LiteralValue::Pending) => "Pending".into(),
        }
    }

    #[derive(Debug, Serialize)]
    struct ClassReport {
        name: &'static str,
        expectation: &'static str,
        notes: &'static str,
        sample_formula: String,
        coverage: Coverage,
        canonical_authority_supported: bool,
        canonical_reject_kinds: Vec<String>,
        cells_compared: u64,
        identical: u64,
        near_miss: u64,
        divergent: u64,
        identical_after_edit: u64,
        divergent_after_edit: u64,
        off_shapes: BTreeMap<String, u64>,
        auth_shapes: BTreeMap<String, u64>,
        examples: Vec<String>,
        verdict: String,
    }

    #[derive(Debug, Serialize)]
    struct StaticProbe {
        formula: String,
        authority_supported: bool,
        reject_kinds: Vec<String>,
    }

    fn static_probes() -> Vec<StaticProbe> {
        let formulas = [
            "=B2*C2+A2",
            // #363 external references (issue #378)
            "=[1]Sheet1!A1",
            "='[1]Sheet1'!A1+B2",
            // 3-D
            "=SUM(Sheet1:Sheet3!A1)",
            // #329 CELL / HYPERLINK (not implemented today)
            "=CELL(\"row\",A2)",
            "=HYPERLINK(\"http://x\",\"y\")",
            // dynamic dependency
            "=INDIRECT(\"A\"&ROW())",
            "=OFFSET($A$1,A2,0)",
            // volatile
            "=RAND()",
            "=TODAY()",
            "=NOW()",
            // reference-returning
            "=IF(B2>1,C2,D2)",
            "=IFS(B2>1,C2,TRUE,D2)",
            "=CHOOSE(A2,B2,C2)",
            "=INDEX($B$2:$B$9,A2)",
            "=IF($B$2:$B$4>1,C2,D2)",
            "=CHOOSE($A$2:$A$4,B2,C2)",
            "=SUM(IF(B2>1,$C$2:$C$4,$D$2:$D$4))",
            "=IF(B2>1,C2,INDEX($B$2:$B$9,2))",
            "=IF(B2>1,C2,OFFSET(A1,1,1))",
            "=IF(B2>1,SUM($C$2:$C$4),0)",
            "=IF(B2>1,C2,IF(D2>1,A2,B2))",
            // array / spill
            "=SORT($A$2:$A$9)",
            "=UNIQUE($A$2:$A$9)",
            "=FILTER($A$2:$A$9,$B$2:$B$9>1)",
            "=SEQUENCE(3)",
            "={1,2;3,4}",
            "=SUM({1,2,3})",
            // lookup family
            "=XLOOKUP(A2,$H$2:$H$9,$I$2:$I$9,0)",
            "=XMATCH(A2,$H$2:$H$9,0)",
            "=VLOOKUP(A2,$H$2:$I$9,2,FALSE)",
            "=MATCH(A2,$H$2:$H$9,0)",
            // operators
            "=A1#",
            "=@A1:A9",
            // structured refs
            "=SUM(Table1[Amount])",
            "=Table1[@Amount]",
            // axis classes
            "=SUM(A:A)",
            "=SUM(D:F)",
            "=SUM($3:$13)",
            "=SUM(A1:A9)",
        ];
        formulas
            .iter()
            .map(|src| {
                let (ok, kinds) = match parse(src) {
                    Ok(ast) => {
                        let d = canonical_template_diagnostic(&ast, 2, 6);
                        (d.authority_supported, d.reject_kinds)
                    }
                    Err(e) => (false, vec![format!("PARSE_ERROR: {e}")]),
                };
                StaticProbe {
                    formula: (*src).to_string(),
                    authority_supported: ok,
                    reject_kinds: kinds,
                }
            })
            .collect()
    }

    #[derive(Debug, Serialize)]
    struct Report {
        rows: u32,
        seed: u64,
        classes: Vec<ClassReport>,
        static_probes: Vec<StaticProbe>,
        total_divergent_cells: u64,
        divergent_classes: Vec<String>,
    }

    /// Returns `total_divergent_cells` so the caller can turn it into an exit code.
    pub fn main() -> Result<u64> {
        let cli = Cli::parse();
        let keep: Vec<String> = cli
            .only
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let mut reports = Vec::new();
        let mut total_divergent = 0u64;
        let mut divergent_classes = Vec::new();

        for class in classes() {
            if !keep.is_empty() && !keep.iter().any(|k| k == class.name) {
                continue;
            }
            eprintln!("[probe-fp-audit2] class {}", class.name);
            let off = run_mode(FormulaPlaneMode::Off, &class, cli.rows, cli.seed);
            let auth = run_mode(
                FormulaPlaneMode::AuthoritativeExperimental,
                &class,
                cli.rows,
                cli.seed,
            );

            let mut identical = 0u64;
            let mut near = 0u64;
            let mut divergent = 0u64;
            let mut ident_edit = 0u64;
            let mut diverg_edit = 0u64;
            let mut examples = Vec::new();
            let mut off_shapes: BTreeMap<String, u64> = BTreeMap::new();
            let mut auth_shapes: BTreeMap<String, u64> = BTreeMap::new();

            for i in 0..off.values.len() {
                let (a, b) = (&off.values[i], &auth.values[i]);
                *off_shapes.entry(shape(a)).or_default() += 1;
                *auth_shapes.entry(shape(b)).or_default() += 1;
                let (same, nearly) = compare(a, b);
                if same {
                    identical += 1;
                } else if nearly {
                    near += 1;
                    if examples.len() < 6 {
                        examples.push(format!(
                            "NEAR row {} [{}] off={a:?} auth={b:?}",
                            FIRST + i as u32,
                            (class.formula)(FIRST + i as u32, FIRST, cli.rows + 1)
                        ));
                    }
                } else {
                    divergent += 1;
                    if examples.len() < 6 {
                        examples.push(format!(
                            "DIVERGENT row {} [{}] off={a:?} auth={b:?}",
                            FIRST + i as u32,
                            (class.formula)(FIRST + i as u32, FIRST, cli.rows + 1)
                        ));
                    }
                }

                let (same_e, near_e) =
                    compare(&off.values_after_edit[i], &auth.values_after_edit[i]);
                if same_e || near_e {
                    ident_edit += 1;
                } else {
                    diverg_edit += 1;
                    if examples.len() < 12 {
                        examples.push(format!(
                            "DIVERGENT-AFTER-EDIT row {} off={:?} auth={:?}",
                            FIRST + i as u32,
                            off.values_after_edit[i],
                            auth.values_after_edit[i]
                        ));
                    }
                }
            }

            let sample_formula = (class.formula)(FIRST, FIRST, cli.rows + 1);
            let (supported, kinds) = {
                let ast = parse(&sample_formula)?;
                let d = canonical_template_diagnostic(&ast, FIRST, OUT_COL);
                (d.authority_supported, d.reject_kinds)
            };

            let cov = auth.coverage.clone();
            let verdict = if divergent > 0 || diverg_edit > 0 {
                "PLANE-DIVERGENT"
            } else if cov.coverage_pct >= 99.0 {
                "PLANE-ALIGNED (accepted)"
            } else if cov.accepted_span_cells == 0 {
                "PLANE-NEUTRAL (rejected, values agree)"
            } else {
                "PLANE-ALIGNED (partial acceptance)"
            }
            .to_string();
            if divergent > 0 || diverg_edit > 0 {
                divergent_classes.push(class.name.to_string());
            }
            total_divergent += divergent + diverg_edit;

            // Off mode must never build spans.
            assert_eq!(
                off.coverage.accepted_span_cells, 0,
                "plane-off run accepted spans for {}",
                class.name
            );

            reports.push(ClassReport {
                name: class.name,
                expectation: class.expectation,
                notes: class.notes,
                sample_formula,
                coverage: cov,
                canonical_authority_supported: supported,
                canonical_reject_kinds: kinds,
                cells_compared: off.values.len() as u64,
                identical,
                near_miss: near,
                divergent,
                identical_after_edit: ident_edit,
                divergent_after_edit: diverg_edit,
                off_shapes,
                auth_shapes,
                examples,
                verdict,
            });
        }

        let report = Report {
            rows: cli.rows,
            seed: cli.seed,
            classes: reports,
            static_probes: static_probes(),
            total_divergent_cells: total_divergent,
            divergent_classes,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);

        eprintln!();
        eprintln!(
            "{:<28} {:>7}  {:<28} {:>6} {:>5} {:>6} {:>6}",
            "class", "cov%", "auth shapes", "ident", "near", "DIV", "DIV_ed"
        );
        for c in &report.classes {
            let shapes = c
                .auth_shapes
                .iter()
                .map(|(k, v)| format!("{k}:{v}"))
                .collect::<Vec<_>>()
                .join(",");
            eprintln!(
                "{:<28} {:>7.1}  {:<28} {:>6} {:>5} {:>6} {:>6}  {}",
                c.name,
                c.coverage.coverage_pct,
                shapes,
                c.identical,
                c.near_miss,
                c.divergent,
                c.divergent_after_edit,
                c.verdict
            );
        }
        eprintln!("# total divergent cells: {}", report.total_divergent_cells);
        Ok(report.total_divergent_cells)
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    // Acceptance gate: any divergent cell is a hard failure, so this probe can be
    // wired straight into CI (`.github/workflows/formula-plane-parity.yml`).
    let total_divergent_cells = probe::main()?;
    if total_divergent_cells != 0 {
        eprintln!(
            "[probe-fp-audit2] FAIL: acceptance gate is total_divergent_cells == 0, got {total_divergent_cells}"
        );
        std::process::exit(1);
    }
    Ok(())
}
