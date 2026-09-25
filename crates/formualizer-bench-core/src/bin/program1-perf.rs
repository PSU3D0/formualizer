//! Program 1 M5 recon: legacy vs authority load / first calc / edit+recalc
//! and retained heap on one real workbook, through the public Workbook path
//! (Calamine, `LoadStrategy::EagerAll`). One workbook per process so the
//! allocator counters start clean. Build it with and without
//! `unified_authority`; with the feature it also reports the authority's
//! own bytes and legacy's dependency-structure bytes by component.
//!
//! ```bash
//! program1-perf --xlsx PATH [--mode ephemeral|interactive] [--edits N]
//! ```
//! Prints one JSON line.

#[cfg(feature = "formualizer_runner")]
mod imp {
    use anyhow::{Result, anyhow};
    use formualizer_parse::parser::ReferenceType;
    use formualizer_workbook::{
        CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
    };
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicIsize, Ordering};
    use std::time::Instant;

    struct Counting;
    static LIVE: AtomicIsize = AtomicIsize::new(0);
    static PEAK: AtomicIsize = AtomicIsize::new(0);

    fn bump(d: isize) {
        let now = LIVE.fetch_add(d, Ordering::Relaxed) + d;
        if d > 0 {
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
    }

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(layout) };
            if !p.is_null() {
                bump(layout.size() as isize);
            }
            p
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) };
            bump(-(layout.size() as isize));
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let p = unsafe { System.realloc(ptr, layout, new_size) };
            if !p.is_null() {
                bump(new_size as isize - layout.size() as isize);
            }
            p
        }
    }

    #[global_allocator]
    static GLOBAL: Counting = Counting;

    fn live() -> i64 {
        LIVE.load(Ordering::Relaxed) as i64
    }

    fn vm_hwm_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmHWM:"))
                    .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
            })
            .unwrap_or(0)
    }

    fn ms(t: Instant) -> f64 {
        t.elapsed().as_secs_f64() * 1000.0
    }

    fn pct(v: &mut [f64], p: f64) -> f64 {
        if v.is_empty() {
            return f64::NAN;
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[((v.len() - 1) as f64 * p).round() as usize]
    }

    struct Targets {
        /// (sheet, row1, col1, current number) value cells a formula reads.
        values: Vec<(String, u32, u32, f64)>,
        /// (sheet, row1, col1, formula text)
        formulas: Vec<(String, u32, u32, String)>,
        formula_count: usize,
        value_count: usize,
        /// every formula cell, for value digests
        all: Vec<(String, u32, u32)>,
    }

    fn mix(x: u64) -> u64 {
        let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Edit targets from an untimed pre-read: numeric value cells that some
    /// formula references (cell refs and range corners), and formula cells.
    fn targets(path: &str, n: usize) -> Result<Targets> {
        let mut a = CalamineAdapter::open_path(path).map_err(|e| anyhow!("open: {e}"))?;
        let sheets = a.sheet_names().map_err(|e| anyhow!("{e}"))?;
        let mut numbers: BTreeMap<(usize, u32, u32), f64> = BTreeMap::new();
        let mut formulas = Vec::new();
        let mut referenced: BTreeSet<(usize, u32, u32)> = BTreeSet::new();
        let find = |s: &str| {
            let s = s.trim_matches('\'');
            sheets.iter().position(|x| x.eq_ignore_ascii_case(s))
        };
        let mut value_count = 0;
        for (si, name) in sheets.iter().enumerate() {
            let data = a.read_sheet(name).map_err(|e| anyhow!("{e}"))?;
            for ((r, c), cell) in data.cells {
                if let Some(f) = cell.formula.filter(|f| !f.is_empty()) {
                    let text = if f.starts_with('=') {
                        f
                    } else {
                        format!("={f}")
                    };
                    if let Ok(ast) = formualizer_parse::parse(&text) {
                        for d in ast.get_dependencies() {
                            let (sh, row, col) = match d {
                                ReferenceType::Cell {
                                    sheet, row, col, ..
                                } => (sheet.clone(), *row, *col),
                                ReferenceType::Range {
                                    sheet,
                                    start_row: Some(row),
                                    start_col: Some(col),
                                    ..
                                } => (sheet.clone(), *row, *col),
                                _ => continue,
                            };
                            let s = match sh {
                                Some(s) => match find(&s) {
                                    Some(i) => i,
                                    None => continue,
                                },
                                None => si,
                            };
                            referenced.insert((s, row, col));
                        }
                    }
                    formulas.push((si, r, c, text));
                } else if let Some(v) = &cell.value {
                    value_count += 1;
                    if let LiteralValue::Number(x) = v {
                        numbers.insert((si, r, c), *x);
                    } else if let LiteralValue::Int(x) = v {
                        numbers.insert((si, r, c), *x as f64);
                    }
                }
            }
        }
        let mut vals: Vec<_> = referenced
            .iter()
            .filter_map(|k| numbers.get(k).map(|v| (*k, *v)))
            .collect();
        vals.sort_by_key(|((s, r, c), _)| {
            mix(((*s as u64) << 40) ^ ((*r as u64) << 16) ^ *c as u64)
        });
        vals.truncate(n);
        let formula_count = formulas.len();
        let all = formulas
            .iter()
            .map(|(s, r, c, _)| (sheets[*s].clone(), *r, *c))
            .collect();
        let mut fs = formulas;
        fs.sort_by_key(|(s, r, c, _)| {
            mix(((*s as u64) << 40) ^ ((*r as u64) << 16) ^ *c as u64 ^ 7)
        });
        fs.truncate(n);
        Ok(Targets {
            values: vals
                .into_iter()
                .map(|((s, r, c), v)| (sheets[s].clone(), r, c, v))
                .collect(),
            formulas: fs
                .into_iter()
                .map(|(s, r, c, t)| (sheets[s].clone(), r, c, t))
                .collect(),
            formula_count,
            value_count,
            all,
        })
    }

    /// FNV-1a over the debug rendering of every formula cell's value
    /// (numbers rounded to 12 significant digits).
    fn digest(wb: &Workbook, all: &[(String, u32, u32)]) -> String {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for (s, r, c) in all {
            let v = match wb.get_value(s, *r, *c) {
                Some(LiteralValue::Number(x)) => format!("{x:.12e}"),
                other => format!("{other:?}"),
            };
            for b in v.bytes().chain(std::iter::once(0xff)) {
                h ^= b as u64;
                h = h.wrapping_mul(0x100_0000_01b3);
            }
        }
        format!("{h:016x}")
    }

    pub fn main() -> Result<()> {
        let args: Vec<String> = std::env::args().collect();
        let get = |k: &str| {
            args.iter()
                .position(|a| a == k)
                .and_then(|i| args.get(i + 1))
                .cloned()
        };
        let path = get("--xlsx").ok_or_else(|| anyhow!("--xlsx PATH"))?;
        let mode = get("--mode").unwrap_or_else(|| "ephemeral".into());
        let edits: usize = get("--edits").map(|s| s.parse()).transpose()?.unwrap_or(20);
        let feature = cfg!(feature = "unified_authority");

        let tg = targets(&path, edits)?;
        let base = live();

        let config = match mode.as_str() {
            "interactive" => WorkbookConfig::interactive(),
            _ => WorkbookConfig::ephemeral(),
        };
        let t = Instant::now();
        let adapter = CalamineAdapter::open_path(&path).map_err(|e| anyhow!("open: {e}"))?;
        let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, config)
            .map_err(|e| anyhow!("load: {e}"))?;
        let load_ms = ms(t);
        let live_load = live() - base;

        // --presync (feature build): build the authority before the first
        // evaluation so its cost is timed apart from planning/execution.
        #[allow(unused_mut)]
        let mut presync_ms = f64::NAN;
        #[cfg(feature = "unified_authority")]
        if args.iter().any(|a| a == "--presync") {
            let t = Instant::now();
            let _ = formualizer_eval::engine::authority::probe::sync(wb.engine_mut());
            presync_ms = ms(t);
        }
        let t = Instant::now();
        let first = wb.evaluate_all();
        let first_ms = ms(t);
        let (first_ok, first_computed, first_err) = match &first {
            Ok(r) => (true, r.computed_vertices, String::new()),
            Err(e) => (false, 0, format!("{e}").chars().take(160).collect()),
        };
        let live_eval = live() - base;
        let digest_first = digest(&wb, &tg.all);
        if let Some(path) = get("--dump") {
            use std::io::Write as _;
            let mut f = std::fs::File::create(&path)?;
            for (s, r, c) in &tg.all {
                writeln!(f, "{s}!R{r}C{c}\t{:?}", wb.get_value(s, *r, *c))?;
            }
        }
        let peak_eval = PEAK.load(Ordering::Relaxed) as i64 - base;

        // Authority summary (feature build only); sync after the first eval
        // is a no-op when the host is already built (timed to prove it).
        #[allow(unused_mut)]
        let mut auth = serde_json::json!(null);
        #[cfg(feature = "unified_authority")]
        {
            use formualizer_eval::engine::authority::probe;
            let t = Instant::now();
            let before = live();
            let s = probe::sync(wb.engine_mut());
            let sync_ms = ms(t);
            let sync_growth = live() - before;
            auth = match s {
                Ok(s) => serde_json::json!({
                    "authority_bytes": s.authority_bytes,
                    "legacy": s.legacy_bytes.iter().map(|(k, v)| (k.to_string(), *v)).collect::<BTreeMap<_, _>>(),
                    "legacy_total": s.legacy_total(),
                    "records": s.records, "owners": s.owners, "nodes": s.nodes, "runs": s.runs,
                    "edge_groups": s.edge_groups, "node_groups": s.node_groups,
                    "builds": s.builds, "incremental": s.incremental_mutations,
                    "sync_ms": sync_ms, "sync_growth": sync_growth,
                    "state": format!("{:?}", probe::state(wb.engine())).chars().take(160).collect::<String>(),
                }),
                Err(e) => {
                    serde_json::json!({"error": format!("{e:?}").chars().take(160).collect::<String>(),
                    "sync_ms": sync_ms})
                }
            };
        }

        // Value edits: +1 on a referenced number, then evaluate_all.
        let (mut v_edit, mut v_recalc, mut v_total) = (vec![], vec![], vec![]);
        let mut v_computed = 0usize;
        let mut v_errors = 0usize;
        for (sheet, r, c, v) in &tg.values {
            let t = Instant::now();
            if wb
                .set_value(sheet, *r, *c, LiteralValue::Number(v + 1.0))
                .is_err()
            {
                v_errors += 1;
                continue;
            }
            let e = ms(t);
            let t2 = Instant::now();
            match wb.evaluate_all() {
                Ok(res) => v_computed += res.computed_vertices,
                Err(_) => v_errors += 1,
            }
            let rc = ms(t2);
            v_edit.push(e);
            v_recalc.push(rc);
            v_total.push(e + rc);
        }
        // Formula edits: re-set a formula to its own text, then evaluate_all.
        let (mut f_total, mut f_computed, mut f_errors) = (vec![], 0usize, 0usize);
        for (sheet, r, c, text) in &tg.formulas {
            let t = Instant::now();
            if wb.set_formula(sheet, *r, *c, text).is_err() {
                f_errors += 1;
                continue;
            }
            match wb.evaluate_all() {
                Ok(res) => f_computed += res.computed_vertices,
                Err(_) => f_errors += 1,
            }
            f_total.push(ms(t));
        }
        let live_end = live() - base;
        let digest_end = digest(&wb, &tg.all);

        // Decomposition (feature build only): the authority's own cost for
        // the questions legacy answers today — a full store build from the
        // graph's formulas, and the dirty closure of each value-edit seed —
        // against legacy's closure mirror on the same seeds.
        #[allow(unused_mut)]
        let mut decomp = serde_json::json!(null);
        #[cfg(feature = "unified_authority")]
        {
            use formualizer_eval::engine::authority::probe;
            let e = wb.engine_mut();
            let _ = probe::sync(e);
            let before = live();
            let t = Instant::now();
            let store = probe::build_fresh(e);
            let build_ms = ms(t);
            let build_heap = live() - before;
            drop(store);
            let (mut a_ms, mut l_ms, mut a_n, mut l_n) = (0.0, 0.0, 0usize, 0usize);
            let mut seeds = 0;
            for (sheet, r, c, _) in &tg.values {
                let Some(sid) = e.sheet_id(sheet) else {
                    continue;
                };
                let cell = (sid, r - 1, c - 1);
                let t = Instant::now();
                let a = probe::closure(e, &[cell]);
                a_ms += ms(t);
                let t = Instant::now();
                let l = probe::legacy_closure(e, &[cell]);
                l_ms += ms(t);
                if let Ok(a) = a {
                    a_n += a.len();
                }
                l_n += l.len();
                seeds += 1;
            }
            let plan = probe::plan_timing(e);
            decomp = serde_json::json!({
                "presync_ms": presync_ms,
                "plan_timing": match plan {
                    Ok((n, cov, prep, ord, adapt, fc, fw, w)) => serde_json::json!({
                        "cells": n, "cover_ms": cov, "prepare_ms": prep, "order_ms": ord,
                        "adapt_ms": adapt, "fallback_components": fc, "fallback_work": fw, "work": w}),
                    Err(m) => serde_json::json!({"error": m}),
                },
                "authority_build_ms": build_ms, "authority_build_heap": build_heap,
                "seeds": seeds, "authority_closure_ms": a_ms, "legacy_closure_ms": l_ms,
                "authority_closure_cells": a_n, "legacy_closure_cells": l_n,
            });
        }

        let out = serde_json::json!({
            "workbook": path,
            "build": if feature { "authority" } else { "legacy" },
            "mode": mode,
            "formulas": tg.formula_count,
            "values": tg.value_count,
            "load_ms": load_ms,
            "first_eval_ms": first_ms,
            "first_ok": first_ok,
            "first_computed": first_computed,
            "first_err": first_err,
            "live_after_load": live_load,
            "live_after_eval": live_eval,
            "peak_through_eval": peak_eval,
            "live_after_edits": live_end,
            "vm_hwm_kb": vm_hwm_kb(),
            "value_edits": v_total.len(),
            "value_edit_errors": v_errors,
            "value_computed": v_computed,
            "value_set_p50_ms": pct(&mut v_edit, 0.5),
            "value_recalc_p50_ms": pct(&mut v_recalc.clone(), 0.5),
            "value_total_p50_ms": pct(&mut v_total.clone(), 0.5),
            "value_total_p90_ms": pct(&mut v_total.clone(), 0.9),
            "value_total_sum_ms": v_total.iter().sum::<f64>(),
            "formula_edits": f_total.len(),
            "formula_edit_errors": f_errors,
            "formula_computed": f_computed,
            "formula_total_p50_ms": pct(&mut f_total.clone(), 0.5),
            "formula_total_p90_ms": pct(&mut f_total.clone(), 0.9),
            "formula_total_sum_ms": f_total.iter().sum::<f64>(),
            "digest_first": digest_first,
            "digest_end": digest_end,
            "authority": auth,
            "decomp": decomp,
        });
        println!("{out}");
        drop(wb);
        Ok(())
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    imp::main()
}

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("requires feature formualizer_runner");
    std::process::exit(2);
}
