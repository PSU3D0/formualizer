//! Development checks for load-time formula families (not a test: it runs
//! over workbooks given on the command line).
//!
//! ```text
//! source_family_check oracle FILE...    # parse every proven copy and compare
//! source_family_check snapshot FILE...  # engine state, families off vs on
//! source_family_check time eager|deferred FILE  # one load + evaluate
//! ```
//!
//! `oracle` loads each file eagerly, deferred (and evaluates) and through
//! XLSX recalculation with `FZ_SOURCE_FAMILIES=oracle`, then prints one JSON
//! line per file and mode with the staging counters; `oracle_mismatches`
//! must be zero. `snapshot` loads each file with families off and on, eager
//! and deferred, and compares ordered sheets, names, cell values and formula
//! text before and after evaluation, after a second evaluation, after a row
//! insertion and evaluation, and parse diagnostics (deferred formula text
//! before evaluation on a sample of rows). It prints one JSON line per file
//! and mode and exits non-zero on any difference. Set
//! `SOURCE_FAMILY_CHECK_TRACE` for per-step timings on stderr.
#[cfg(all(feature = "xlsx-recalc", feature = "json", not(target_arch = "wasm32")))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    imp::main()
}

#[cfg(not(all(feature = "xlsx-recalc", feature = "json", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("this example requires native xlsx-recalc support");
}

#[cfg(all(feature = "xlsx-recalc", feature = "json", not(target_arch = "wasm32")))]
mod imp {
    use formualizer_eval::engine::{SourceFamilyCounters, source_family_process_totals};
    use formualizer_workbook::{
        CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
        XlsxRecalculateOptions, recalculate_xlsx_bytes,
    };
    use std::fmt::Write as _;
    use std::time::Instant;

    const MAX_CELLS: u64 = 20_000_000;

    fn set_families(value: &str) {
        // Single-threaded tool: the engine reads this when it is created.
        unsafe { std::env::set_var("FZ_SOURCE_FAMILIES", value) };
    }

    fn config(deferred: bool) -> WorkbookConfig {
        if deferred {
            WorkbookConfig::interactive()
        } else {
            let mut c = WorkbookConfig::ephemeral();
            c.enable_changelog = true;
            c
        }
    }

    fn load(path: &str, deferred: bool) -> Result<Workbook, String> {
        let adapter = CalamineAdapter::open_path(path).map_err(|e| e.to_string())?;
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, config(deferred))
            .map_err(|e| e.to_string())
    }

    fn delta(after: SourceFamilyCounters, before: SourceFamilyCounters) -> String {
        let d = |a: u64, b: u64| a - b;
        format!(
            "\"formulas\":{},\"parse_calls\":{},\"cache_hits\":{},\"parsed_members\":{},\
             \"shared_members\":{},\"adjacent_members\":{},\"templates_certified\":{},\
             \"fallback_mismatch\":{},\"fallback_off_grid\":{},\"fallback_no_template\":{},\
             \"oracle_checked\":{},\"oracle_mismatches\":{}",
            d(after.formulas, before.formulas),
            d(after.parse_calls, before.parse_calls),
            d(after.parse_cache_hits, before.parse_cache_hits),
            d(after.parsed_members, before.parsed_members),
            d(after.shared_members, before.shared_members),
            d(after.adjacent_members, before.adjacent_members),
            d(after.templates_certified, before.templates_certified),
            d(after.fallback_mismatch, before.fallback_mismatch),
            d(after.fallback_off_grid, before.fallback_off_grid),
            d(after.fallback_no_template, before.fallback_no_template),
            d(after.oracle_checked, before.oracle_checked),
            d(after.oracle_mismatches, before.oracle_mismatches),
        )
    }

    fn json_str(s: &str) -> String {
        serde_json::to_string(s).unwrap()
    }

    fn oracle(files: &[String]) -> bool {
        set_families("oracle");
        let mut clean = true;
        for path in files {
            for mode in ["eager", "deferred", "recalc"] {
                let before = source_family_process_totals();
                let error = match mode {
                    "recalc" => match std::fs::read(path) {
                        Ok(bytes) => {
                            recalculate_xlsx_bytes(&bytes, XlsxRecalculateOptions::default())
                                .err()
                                .map(|e| e.to_string())
                        }
                        Err(e) => Some(e.to_string()),
                    },
                    _ => match load(path, mode == "deferred") {
                        Ok(mut wb) => wb.evaluate_all().err().map(|e| e.to_string()),
                        Err(e) => Some(e),
                    },
                };
                let after = source_family_process_totals();
                clean &= after.oracle_mismatches == before.oracle_mismatches;
                println!(
                    "{{\"file\":{},\"mode\":\"{mode}\",{},\"error\":{}}}",
                    json_str(path),
                    delta(after, before),
                    error.map_or("null".to_string(), |e| json_str(&e)),
                );
            }
        }
        clean
    }

    fn value(v: Option<LiteralValue>) -> String {
        match v {
            None => "none".into(),
            Some(LiteralValue::Number(n)) => format!("n{:016x}", n.to_bits()),
            Some(other) => format!("{other:?}"),
        }
    }

    /// Ordered sheets, dimensions, every non-empty cell's value and
    /// formula text, names and parse diagnostics.
    /// With `sample`, formula text is read only on a deterministic sample
    /// of rows (the first five and about 40 more per sheet): inspecting a
    /// deferred formula before the graph is built replays the sheet's
    /// formula spool, so reading every cell would be quadratic.
    fn snapshot(wb: &Workbook, out: &mut Vec<String>, sample: bool) {
        for sheet in wb.sheet_names() {
            let (rows, cols) = wb.sheet_dimensions(&sheet).unwrap_or((0, 0));
            out.push(format!("sheet {sheet:?} {rows}x{cols}"));
            if u64::from(rows) * u64::from(cols) > MAX_CELLS {
                out.push("cells skipped (too large)".into());
                continue;
            }
            let stride = (rows / 40).max(1);
            for row in 1..=rows {
                let read_formula = !sample || row <= 5 || row % stride == 0;
                for col in 1..=cols {
                    let formula = if read_formula {
                        wb.get_formula(&sheet, row, col)
                    } else {
                        None
                    };
                    let v = wb.get_value(&sheet, row, col);
                    if formula.is_none() && matches!(v, None | Some(LiteralValue::Empty)) {
                        continue;
                    }
                    out.push(format!("{row},{col} {formula:?} {}", value(v)));
                }
            }
        }
        let mut names: Vec<String> = wb
            .engine()
            .named_ranges_snapshot()
            .iter()
            .map(|n| format!("{n:?}"))
            .collect();
        names.sort();
        out.extend(names);
        for d in wb.engine().formula_parse_diagnostics() {
            out.push(format!("diag {d:?}"));
        }
    }

    fn run(path: &str, deferred: bool, families: &str) -> Vec<String> {
        set_families(families);
        let t0 = Instant::now();
        let lap = |what: &str| {
            if std::env::var_os("SOURCE_FAMILY_CHECK_TRACE").is_some() {
                eprintln!("{what}: {:.1} ms", t0.elapsed().as_secs_f64() * 1e3);
            }
        };
        let mut out = Vec::new();
        let mut wb = match load(path, deferred) {
            Ok(wb) => wb,
            Err(e) => return vec![format!("load error {e}")],
        };
        lap("load");
        out.push("== before evaluation".into());
        snapshot(&wb, &mut out, deferred);
        lap("snapshot before evaluation");
        for pass in ["first", "second"] {
            out.push(format!("== {pass} evaluation"));
            if let Err(e) = wb.evaluate_all() {
                out.push(format!("evaluate error {e}"));
            }
            lap("evaluate");
            snapshot(&wb, &mut out, false);
            lap("snapshot");
        }
        if let Some(sheet) = wb.sheet_names().first().cloned() {
            out.push("== after insert_rows(2) and evaluation".into());
            match wb.engine_mut().insert_rows(&sheet, 2, 1) {
                Ok(_) => {
                    if let Err(e) = wb.evaluate_all() {
                        out.push(format!("evaluate error {e}"));
                    }
                }
                Err(e) => out.push(format!("insert error {e:?}")),
            }
            lap("insert + evaluate");
            snapshot(&wb, &mut out, false);
            lap("snapshot");
        }
        out
    }

    fn snapshots(files: &[String]) -> bool {
        let mut clean = true;
        for path in files {
            for deferred in [false, true] {
                let old = run(path, deferred, "off");
                let new = run(path, deferred, "on");
                let first = old
                    .iter()
                    .zip(&new)
                    .position(|(a, b)| a != b)
                    .or((old.len() != new.len()).then(|| old.len().min(new.len())));
                let mut diff = String::new();
                if let Some(i) = first {
                    clean = false;
                    let _ = write!(
                        diff,
                        "{} != {}",
                        old.get(i).map_or("<end>", String::as_str),
                        new.get(i).map_or("<end>", String::as_str)
                    );
                }
                println!(
                    "{{\"file\":{},\"mode\":\"{}\",\"lines\":{},\"equal\":{},\"first_difference\":{}}}",
                    json_str(path),
                    if deferred { "deferred" } else { "eager" },
                    old.len(),
                    first.is_none(),
                    json_str(&diff),
                );
            }
        }
        clean
    }

    fn time(mode: &str, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let t0 = Instant::now();
        let mut wb = load(path, mode == "deferred")?;
        let t1 = Instant::now();
        wb.evaluate_all()?;
        let t2 = Instant::now();
        println!(
            "{{\"mode\":\"{mode}\",\"load_ms\":{:.3},\"evaluate_ms\":{:.3},\"total_ms\":{:.3}}}",
            (t1 - t0).as_secs_f64() * 1e3,
            (t2 - t1).as_secs_f64() * 1e3,
            (t2 - t0).as_secs_f64() * 1e3,
        );
        Ok(())
    }

    pub(super) fn main() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        match args.first().map(String::as_str) {
            Some("oracle") => {
                if !oracle(&args[1..]) {
                    std::process::exit(1);
                }
            }
            Some("snapshot") => {
                if !snapshots(&args[1..]) {
                    std::process::exit(1);
                }
            }
            Some("time") if args.len() == 3 => time(&args[1], &args[2])?,
            _ => {
                return Err(
                    "usage: source_family_check oracle|snapshot FILE... | time eager|deferred FILE"
                        .into(),
                );
            }
        }
        Ok(())
    }
}
