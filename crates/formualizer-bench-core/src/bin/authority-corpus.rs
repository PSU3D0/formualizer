//! Program 1 M1a (FORM-000169) corpus harness for the unified authority:
//! Δ(a) dirty closure and Δ(e) direct dependents against the legacy graph,
//! maintained == rebuild under formula/value edits, and the retained-heap
//! gate (authority ≤ 1.0× legacy dependency structures), per workbook.
//!
//! Corpus and loading follow Packet B's exactness driver
//! (`region-graph-bench` on `spike/region-adjacency-tier`): 14 witnesses at
//! 4096/16384/100000 rows, the xlsx corpus (real + synthetic), two
//! all-singleton workbooks and the Enron sample manifest. Each workbook is
//! loaded into an `Engine` (FormulaPlane off: the legacy graph owns every
//! formula) with names via `define_name` and formulas via
//! `bulk_set_formulas` per sheet (bisected on rejection).
//!
//! Subcommands:
//!   list
//!   run --out DIR [--only SUBSTR[,SUBSTR]] [--edits N] [--closure-seeds N]
//!       [--direct-seeds N]
//!   losses --out DIR [--only SUBSTR]   edge records under the as-written
//!       unit-stride canon (the build), a strided partition of the same
//!       groups (stride loss) and Packet B's b′ keying (as-written or fully
//!       fixed, whichever more dependencies share; FF-keying loss), SP-2 F-1
//!
//! Counts only; nothing here is timed.

use anyhow::{Context, Result, anyhow, bail};
use formualizer_eval::engine::authority::probe;
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::engine::{Engine, EvalConfig, FormulaPlaneMode};
use formualizer_eval::reference::{CellRef, Coord, RangeRef};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_testkit::shape::{Cell as ShapeCell, Literal};
use formualizer_testkit::witnesses::{Kind, witness};
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader, traits::DefinedNameDefinition};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicIsize, Ordering};

// ------------------------------------------------------------ allocator

struct Counting;
static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE.fetch_add(
                new_size as isize - layout.size() as isize,
                Ordering::Relaxed,
            );
        }
        p
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

// ------------------------------------------------------------ snapshot

#[derive(Clone, Debug)]
enum Content {
    Formula(String),
    Value,
}

#[derive(Clone, Debug)]
enum NameTarget {
    Cell(u32, u32, u32),
    Range(u32, u32, u32, u32, u32),
    Other,
}

#[derive(Default)]
struct Snapshot {
    sheets: Vec<String>,
    /// `(sheet, row0, col0) -> content`
    cells: BTreeMap<(u32, u32, u32), Content>,
    names: Vec<(String, Option<u32>, NameTarget)>,
}

impl Snapshot {
    fn sheet_index(&self, name: &str) -> Option<u32> {
        self.sheets
            .iter()
            .position(|s| s.eq_ignore_ascii_case(name))
            .map(|i| i as u32)
    }
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9e37_79b9_7f4a_7c15)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: u32) -> u32 {
        (self.next_u64() % u64::from(n.max(1))) as u32
    }
    fn chance(&mut self, pct: u32) -> bool {
        self.below(100) < pct
    }
}

fn corpus_root() -> PathBuf {
    std::env::var("FZ_CORPUS_ROOT").map(PathBuf::from).unwrap_or_else(|_| {
        PathBuf::from(
            "/home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/formualizer/platform-dev/codebase/oss/formualizer/benchmarks/corpus",
        )
    })
}

fn enron_root() -> PathBuf {
    std::env::var("FZ_ENRON_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_default();
            PathBuf::from(home).join(".cache/formualizer-corpora/enron-hermans")
        })
}

fn default_corpus() -> Vec<String> {
    let mut ids = Vec::new();
    for rows in [4096u32, 16384, 100_000] {
        for k in Kind::ALL {
            ids.push(format!("witness:{}:{rows}", k.id()));
        }
    }
    for sub in ["real", "synthetic"] {
        let dir = corpus_root().join(sub);
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .map(|rd| rd.filter_map(|e| e.ok().map(|e| e.path())).collect())
            .unwrap_or_default();
        files.retain(|p| p.extension().is_some_and(|e| e == "xlsx"));
        files.sort();
        ids.extend(files.iter().map(|p| format!("xlsx:{}", p.display())));
    }
    ids.push("singleton:16384".into());
    ids.push("singleton:100000".into());
    let manifest = enron_root().join("sample-manifest.tsv");
    let mut seen = BTreeSet::new();
    if let Ok(text) = std::fs::read_to_string(&manifest) {
        for line in text.lines() {
            let first = line.split('\t').next().unwrap_or("").trim();
            if first.is_empty() || first.starts_with('#') || !first.ends_with(".xlsx") {
                continue;
            }
            let p = if Path::new(first).is_absolute() {
                PathBuf::from(first)
            } else {
                enron_root().join(first)
            };
            if seen.insert(p.clone()) {
                ids.push(format!("enron:{}", p.display()));
            }
        }
    }
    ids
}

fn witness_snapshot(id: &str, rows: u32) -> Result<Snapshot> {
    let kind = Kind::ALL
        .into_iter()
        .find(|k| k.id() == id)
        .ok_or_else(|| anyhow!("unknown witness {id}"))?;
    let mut shape = witness(kind, rows).shape;
    shape.scale.rows = rows;
    let rendered = shape.render().map_err(|e| anyhow!("{e}"))?;
    let mut snap = Snapshot::default();
    for s in &shape.sheets {
        snap.sheets.push(s.name.clone());
    }
    for rc in rendered {
        let sheet = snap.sheet_index(&rc.sheet).context("sheet")?;
        let content = match rc.cell {
            ShapeCell::Formula(f) => Content::Formula(f),
            ShapeCell::Value(Literal::Empty) => continue,
            ShapeCell::Value(_) => Content::Value,
        };
        snap.cells.insert((sheet, rc.row - 1, rc.col - 1), content);
    }
    Ok(snap)
}

fn xlsx_snapshot(path: &Path) -> Result<Snapshot> {
    let mut adapter =
        CalamineAdapter::open_path(path).map_err(|e| anyhow!("open {path:?}: {e}"))?;
    let sheets = adapter.sheet_names().map_err(|e| anyhow!("{e}"))?;
    let mut snap = Snapshot {
        sheets: sheets.clone(),
        ..Default::default()
    };
    for (i, name) in sheets.iter().enumerate() {
        let data = adapter
            .read_sheet(name)
            .map_err(|e| anyhow!("read {name}: {e}"))?;
        for ((r, c), cell) in data.cells {
            if r == 0 || c == 0 || r > 1_048_576 || c > 16_384 {
                continue;
            }
            let content = match cell.formula.filter(|f| !f.is_empty()) {
                Some(f) => Content::Formula(f),
                None if cell.value.is_some() => Content::Value,
                None => continue,
            };
            snap.cells.insert((i as u32, r - 1, c - 1), content);
        }
    }
    if let Ok(names) = adapter.defined_names() {
        for n in names {
            let scope = n.scope_sheet.as_deref().and_then(|s| snap.sheet_index(s));
            let target = match &n.definition {
                DefinedNameDefinition::Range { address } => {
                    match snap.sheet_index(&address.sheet) {
                        Some(s) if address.start_row >= 1 && address.start_col >= 1 => {
                            if (address.start_row, address.start_col)
                                == (address.end_row, address.end_col)
                            {
                                NameTarget::Cell(s, address.start_row - 1, address.start_col - 1)
                            } else {
                                NameTarget::Range(
                                    s,
                                    address.start_row - 1,
                                    address.start_col - 1,
                                    address.end_row - 1,
                                    address.end_col - 1,
                                )
                            }
                        }
                        _ => NameTarget::Other,
                    }
                }
                _ => NameTarget::Other,
            };
            snap.names.push((n.name.clone(), scope, target));
        }
    }
    Ok(snap)
}

/// All-singleton irregular workbook (Packet B's 1×1 budget case).
fn singleton_snapshot(n: u32, seed: u64) -> Snapshot {
    let mut g = Rng::new(seed);
    let mut snap = Snapshot {
        sheets: vec!["S".into()],
        ..Default::default()
    };
    for r in 0..n {
        snap.cells.insert((0, r, 0), Content::Value);
    }
    for r in 0..n {
        let a = g.below(n);
        let second = if r > 0 && g.chance(25) {
            format!("B{}", g.below(r) + 1)
        } else {
            format!("A{}", g.below(n) + 1)
        };
        snap.cells.insert(
            (0, r, 1),
            Content::Formula(format!("=A{}*{}+{}", a + 1, r % 7 + 1, second)),
        );
    }
    snap
}

fn load(id: &str) -> Result<Snapshot> {
    let snap = load_unchecked(id)?;
    if let Some((&(_, r, c), _)) = snap
        .cells
        .iter()
        .find(|((_, r, c), _)| *r > 1_048_575 || *c > 16_383)
    {
        bail!("cell ({r}, {c}) is outside the 1,048,576 x 16,384 grid; not representable");
    }
    Ok(snap)
}

fn load_unchecked(id: &str) -> Result<Snapshot> {
    if let Some(rest) = id.strip_prefix("witness:") {
        let (kind, rows) = rest.rsplit_once(':').context("witness:<kind>:<rows>")?;
        return witness_snapshot(kind, rows.parse()?);
    }
    if let Some(p) = id
        .strip_prefix("xlsx:")
        .or_else(|| id.strip_prefix("enron:"))
    {
        return xlsx_snapshot(Path::new(p));
    }
    if let Some(n) = id.strip_prefix("singleton:") {
        return Ok(singleton_snapshot(n.parse()?, 42));
    }
    bail!("unknown workbook id {id}")
}

// ------------------------------------------------------------ engine

type Items = Vec<(u32, u32, formualizer_parse::parser::ASTNode)>;

/// Insert a batch; a rejected formula fails its whole batch, so bisect.
fn insert(engine: &mut Engine<TestWorkbook>, sheet: &str, items: Items, rejected: &mut usize) {
    if items.is_empty() {
        return;
    }
    let single = items.len() == 1;
    let retry = (!single).then(|| items.clone());
    match engine.bulk_set_formulas(sheet, items) {
        Ok(_) => {}
        Err(_) if single => *rejected += 1,
        Err(_) => {
            let mut left = retry.expect("multi-item batch");
            let right = left.split_off(left.len() / 2);
            insert(engine, sheet, left, rejected);
            insert(engine, sheet, right, rejected);
        }
    }
}

struct Loaded {
    engine: Engine<TestWorkbook>,
    sheet_ids: Vec<u16>,
    rejected: usize,
    parse_errors: usize,
}

fn build_engine(snap: &Snapshot) -> Result<Loaded> {
    let config = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Off);
    let mut engine = Engine::new(TestWorkbook::new(), config);
    let mut sheet_ids = Vec::new();
    for name in &snap.sheets {
        let id = match engine.sheet_id(name) {
            Some(id) => id,
            None => engine
                .add_sheet(name)
                .map_err(|e| anyhow!("add sheet {name}: {e}"))?,
        };
        sheet_ids.push(id);
    }
    let cref =
        |s: u32, r: u32, c: u32| CellRef::new(sheet_ids[s as usize], Coord::new(r, c, true, true));
    for (name, scope, target) in &snap.names {
        let def = match *target {
            NameTarget::Cell(s, r, c) => NamedDefinition::Cell(cref(s, r, c)),
            NameTarget::Range(s, r0, c0, r1, c1) => {
                NamedDefinition::Range(RangeRef::new(cref(s, r0, c0), cref(s, r1, c1)))
            }
            NameTarget::Other => continue,
        };
        let scope = match scope {
            None => NameScope::Workbook,
            Some(s) => NameScope::Sheet(sheet_ids[*s as usize]),
        };
        let _ = engine.define_name(name, def, scope);
    }
    let mut parse_errors = 0;
    let mut rejected = 0;
    for (i, name) in snap.sheets.iter().enumerate() {
        let items: Items = snap
            .cells
            .range((i as u32, 0, 0)..(i as u32 + 1, 0, 0))
            .filter_map(|(&(_, r, c), content)| match content {
                Content::Formula(f) => {
                    let text = if f.starts_with('=') {
                        f.clone()
                    } else {
                        format!("={f}")
                    };
                    match formualizer_parse::parse(&text) {
                        Ok(ast) => Some((r + 1, c + 1, ast)),
                        Err(_) => {
                            parse_errors += 1;
                            None
                        }
                    }
                }
                Content::Value => None,
            })
            .collect();
        insert(&mut engine, name, items, &mut rejected);
    }
    Ok(Loaded {
        engine,
        sheet_ids,
        rejected,
        parse_errors,
    })
}

// ------------------------------------------------------------ run

#[derive(Default)]
struct Row {
    id: String,
    formulas: u64,
    rejected: usize,
    parse_errors: usize,
    records: u64,
    owners: u64,
    nodes: u64,
    runs: u64,
    slot_rows: u64,
    edge_groups: usize,
    node_groups: usize,
    authority_bytes: u64,
    authority_measured: i64,
    legacy_bytes: u64,
    direct_seeds: usize,
    direct_mismatches: usize,
    closure_seeds: usize,
    closure_mismatches: usize,
    edits: usize,
    post_direct_mismatches: usize,
    post_closure_mismatches: usize,
    maintained_eq_rebuild: bool,
    check_ok: bool,
    state: String,
    /// Δ(a) seeds without a legacy vertex (legacy cannot propagate them).
    closure_skipped: usize,
    /// Relation-closure (positive length) mismatches, pre and post edits.
    relation_mismatches: usize,
    first_mismatch: String,
}

impl Row {
    fn header() -> &'static str {
        "workbook\tformulas\trejected\tparse_errors\trecords\towners\tnodes\truns\tslot_rows\tedge_groups\tnode_groups\tauthority_bytes\tauthority_measured_heap\tlegacy_bytes\tratio_capacity\tratio_measured\tdirect_seeds\tdirect_mismatches\tclosure_seeds\tclosure_mismatches\tedits\tpost_direct_mismatches\tpost_closure_mismatches\tmaintained_eq_rebuild\tcheck_ok\tstate\tclosure_skipped\trelation_mismatches\tfirst_mismatch"
    }

    fn line(&self) -> String {
        let ratio = |a: f64| {
            if self.legacy_bytes == 0 {
                f64::NAN
            } else {
                a / self.legacy_bytes as f64
            }
        };
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.4}\t{:.4}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.id,
            self.formulas,
            self.rejected,
            self.parse_errors,
            self.records,
            self.owners,
            self.nodes,
            self.runs,
            self.slot_rows,
            self.edge_groups,
            self.node_groups,
            self.authority_bytes,
            self.authority_measured,
            self.legacy_bytes,
            ratio(self.authority_bytes as f64),
            ratio(self.authority_measured as f64),
            self.direct_seeds,
            self.direct_mismatches,
            self.closure_seeds,
            self.closure_mismatches,
            self.edits,
            self.post_direct_mismatches,
            self.post_closure_mismatches,
            self.maintained_eq_rebuild,
            self.check_ok,
            self.state,
            self.closure_skipped,
            self.relation_mismatches,
            self.first_mismatch.replace('\t', " "),
        )
    }
}

type Cell = (u16, u32, u32);

/// Direct-query seeds: every formula cell and the corners of its precedent
/// rectangles, sampled down to `max` (seeded).
fn seeds(engine: &mut Engine<TestWorkbook>, max: usize, g: &mut Rng) -> Result<Vec<Cell>> {
    let formulas = probe::formula_cells(engine).map_err(|e| anyhow!("{e}"))?;
    let mut set: BTreeSet<Cell> = formulas.iter().copied().collect();
    for &f in formulas.iter().take(20_000) {
        for (s, r) in probe::precedents(engine, f).map_err(|e| anyhow!("{e}"))? {
            set.insert((s, r.r0, r.c0));
            set.insert((s, r.r1, r.c1));
            if r.r1 < 1_048_575 {
                set.insert((s, r.r1 + 1, r.c0));
            }
        }
    }
    let mut v: Vec<Cell> = set.into_iter().collect();
    // Seeded partial Fisher–Yates down to `max`.
    if v.len() > max {
        for i in 0..max {
            let j = i + g.below((v.len() - i) as u32) as usize;
            v.swap(i, j);
        }
        v.truncate(max);
    }
    Ok(v)
}

fn compare(
    engine: &mut Engine<TestWorkbook>,
    direct: &[Cell],
    closure: &[Cell],
    row: &mut Row,
) -> Result<(usize, usize)> {
    let mut dm = 0;
    for &c in direct {
        let mine = probe::direct_dependents(engine, c).map_err(|e| anyhow!("{e}"))?;
        let legacy = probe::legacy_direct_dependents(engine, c);
        if mine != legacy {
            dm += 1;
            if row.first_mismatch.is_empty() {
                row.first_mismatch = format!(
                    "direct {c:?}: legacy {} authority {} e.g. legacy-only {:?} authority-only {:?}",
                    legacy.len(),
                    mine.len(),
                    legacy.iter().find(|x| mine.binary_search(x).is_err()),
                    mine.iter().find(|x| legacy.binary_search(x).is_err())
                );
            }
        }
    }
    // Δ(a): legacy's actual dirty propagation (`mark_dirty_many`) against
    // the authority's marking of the same propagation (formula seeds and
    // their closure). Only formula cells are compared (value sources are
    // affected, not dirtied; symbol vertices are not cells).
    let mut cm = 0;
    for &c in closure {
        // Relation closure (positive length) as a second comparison.
        let mine = probe::closure(engine, &[c]).map_err(|e| anyhow!("{e}"))?;
        let legacy = probe::legacy_closure(engine, &[c]);
        if mine != legacy {
            row.relation_mismatches += 1;
        }
        let Some((legacy, mine)) = probe::dirty_pair(engine, &[c]).map_err(|e| anyhow!("{e}"))?
        else {
            row.closure_skipped += 1;
            continue;
        };
        if mine != legacy {
            cm += 1;
            if row.first_mismatch.is_empty() {
                row.first_mismatch = format!(
                    "dirty {c:?}: legacy {} authority {} e.g. legacy-only {:?} authority-only {:?}",
                    legacy.len(),
                    mine.len(),
                    legacy.iter().find(|x| mine.binary_search(x).is_err()),
                    mine.iter().find(|x| legacy.binary_search(x).is_err())
                );
            }
        }
    }
    Ok((dm, cm))
}

fn run_one(id: &str, edits: usize, closure_seeds: usize, direct_seeds: usize) -> Result<Row> {
    let snap = load(id)?;
    let mut loaded = build_engine(&snap)?;
    let engine = &mut loaded.engine;
    let mut row = Row {
        id: id.to_string(),
        rejected: loaded.rejected,
        parse_errors: loaded.parse_errors,
        ..Default::default()
    };
    // Build at load (the host builds on first sync).
    probe::settle_legacy(engine);
    let sum = probe::sync(engine).map_err(|e| anyhow!("{e}"))?;
    row.formulas = sum.formulas;
    row.records = sum.records;
    row.owners = sum.owners;
    row.nodes = sum.nodes;
    row.runs = sum.runs;
    row.slot_rows = sum.slot_rows;
    row.edge_groups = sum.edge_groups;
    row.node_groups = sum.node_groups;
    row.authority_bytes = sum.authority_bytes;
    row.legacy_bytes = sum.legacy_total();
    // Measured heap of a fresh build (allocator delta, store kept alive).
    {
        let before = LIVE.load(Ordering::Relaxed);
        let store = probe::build_fresh(engine);
        row.authority_measured = (LIVE.load(Ordering::Relaxed) - before) as i64;
        drop(store);
    }
    let mut g = Rng::new(0x5EED ^ id.len() as u64);
    let direct = seeds(engine, direct_seeds, &mut g)?;
    let closure: Vec<Cell> = direct.iter().take(closure_seeds).copied().collect();
    row.direct_seeds = direct.len();
    row.closure_seeds = closure.len();
    let (dm, cm) = compare(engine, &direct, &closure, &mut row)?;
    row.direct_mismatches = dm;
    row.closure_mismatches = cm;

    // Maintenance: punch, restore and alter formula cells through the engine.
    let formulas: Vec<((u32, u32, u32), String)> = snap
        .cells
        .iter()
        .filter_map(|(&k, v)| match v {
            Content::Formula(f) => Some((k, f.clone())),
            Content::Value => None,
        })
        .collect();
    let mut touched: Vec<Cell> = Vec::new();
    if !formulas.is_empty() {
        for _ in 0..edits {
            let (k, text) = &formulas[g.below(formulas.len() as u32) as usize];
            let sheet = &snap.sheets[k.0 as usize];
            let text = if text.starts_with('=') {
                text.clone()
            } else {
                format!("={text}")
            };
            let op = g.below(3);
            let res = match op {
                0 => engine.set_cell_value(
                    sheet,
                    k.1 + 1,
                    k.2 + 1,
                    formualizer_common::LiteralValue::Number(1.0),
                ),
                1 => match formualizer_parse::parse(&text) {
                    Ok(ast) => engine.set_cell_formula(sheet, k.1 + 1, k.2 + 1, ast),
                    Err(_) => continue,
                },
                _ => match formualizer_parse::parse(format!("=({})+1", &text[1..])) {
                    Ok(ast) => engine.set_cell_formula(sheet, k.1 + 1, k.2 + 1, ast),
                    Err(_) => continue,
                },
            };
            if res.is_ok() {
                row.edits += 1;
                touched.push((loaded.sheet_ids[k.0 as usize], k.1, k.2));
            }
        }
    }
    row.maintained_eq_rebuild =
        probe::maintained_equals_rebuild(engine).map_err(|e| anyhow!("{e}"))?;
    row.check_ok = matches!(probe::check(engine), Ok(Ok(())));
    touched.sort_unstable();
    touched.dedup();
    let post_direct: Vec<Cell> = touched
        .iter()
        .copied()
        .chain(direct.iter().copied().take(512))
        .collect();
    let post_closure: Vec<Cell> = touched
        .iter()
        .copied()
        .take(64)
        .chain(closure.iter().copied().take(64))
        .collect();
    let (pdm, pcm) = compare(engine, &post_direct, &post_closure, &mut row)?;
    row.post_direct_mismatches = pdm;
    row.post_closure_mismatches = pcm;
    row.state = format!("{:?}", probe::state(engine));
    Ok(row)
}

// ------------------------------------------------------------ losses

const MAX_STRIDE: u32 = 8;

/// Packet B's greedy run cutting: contiguous, else a stride in 2..=8 with
/// at least three members, else a singleton. Returns (start, end, step).
fn cut_runs(sorted: &[u32]) -> Vec<(u32, u32, u32)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < sorted.len() {
        let start = sorted[i];
        if i + 1 < sorted.len() && sorted[i + 1] == start + 1 {
            let mut j = i + 1;
            while j + 1 < sorted.len() && sorted[j + 1] == sorted[j] + 1 {
                j += 1;
            }
            out.push((start, sorted[j], 1));
            i = j + 1;
            continue;
        }
        if i + 2 < sorted.len() {
            let st = sorted[i + 1] - start;
            if (2..=MAX_STRIDE).contains(&st) && sorted[i + 2] == sorted[i + 1] + st {
                let mut j = i + 2;
                while j + 1 < sorted.len() && sorted[j + 1] == sorted[j] + st {
                    j += 1;
                }
                out.push((start, sorted[j], st));
                i = j + 1;
                continue;
            }
        }
        out.push((start, start, 1));
        i += 1;
    }
    out
}

/// Packet B's strided rectangle partition of a cell set: per column cut
/// row runs, then per row lattice cut column runs. Returns the count.
fn strided_count(cells: &mut [(u32, u32)]) -> usize {
    cells.sort_unstable_by_key(|&(r, c)| (c, r));
    let mut runs: Vec<((u32, u32, u32), u32)> = Vec::new();
    let mut i = 0;
    while i < cells.len() {
        let col = cells[i].1;
        let mut j = i;
        while j < cells.len() && cells[j].1 == col {
            j += 1;
        }
        let rows: Vec<u32> = cells[i..j].iter().map(|&(r, _)| r).collect();
        for lat in cut_runs(&rows) {
            runs.push((lat, col));
        }
        i = j;
    }
    runs.sort_unstable();
    let mut n = 0;
    let mut i = 0;
    while i < runs.len() {
        let lat = runs[i].0;
        let mut j = i;
        while j < runs.len() && runs[j].0 == lat {
            j += 1;
        }
        let cols: Vec<u32> = runs[i..j].iter().map(|&(_, c)| c).collect();
        n += cut_runs(&cols).len();
        i = j;
    }
    n
}

fn losses_one(id: &str) -> Result<String> {
    use formualizer_eval::engine::authority::canon::{CanonWork, canon};
    use formualizer_eval::engine::authority::geom::Rect;
    let snap = load(id)?;
    let mut loaded = build_engine(&snap)?;
    let engine = &mut loaded.engine;
    let sum = probe::sync(engine).map_err(|e| anyhow!("{e}"))?;
    let groups = probe::edge_groups(engine).map_err(|e| anyhow!("{e}"))?;
    let canon_records: usize = groups.iter().map(|g| g.4.len()).sum();
    let mut strided = 0usize;
    let mut deps = 0u64;
    // b′: each dependency keyed as written or fully fixed, whichever key
    // more dependencies share (ties as written).
    type FfKey = (u16, bool, u32, u32, u32, u32, u32, u16);
    let mut ff_share: BTreeMap<FfKey, u64> = BTreeMap::new();
    let mut expanded: Vec<Vec<(u32, u32)>> = Vec::with_capacity(groups.len());
    for (_, _, _, _, rects) in &groups {
        let mut cells: Vec<(u32, u32)> = Vec::new();
        for r in rects {
            for c in r.c0..=r.c1 {
                for row in r.r0..=r.r1 {
                    cells.push((row, c));
                }
            }
        }
        deps += cells.len() as u64;
        expanded.push(cells);
    }
    let ff_key = |g: &(
        u16,
        bool,
        u32,
        formualizer_eval::engine::authority::proj::RefProj,
        Vec<Rect>,
    ),
                  cell: (u32, u32)|
     -> Option<FfKey> {
        let t = g.3.instantiate(cell.0, cell.1)?;
        Some((g.0, g.1, g.2, t.r0, t.c0, t.r1, t.c1, g.3.sheet))
    };
    for (g, cells) in groups.iter().zip(&expanded) {
        for &cell in cells {
            if let Some(k) = ff_key(g, cell) {
                *ff_share.entry(k).or_default() += 1;
            }
        }
    }
    let mut aw_cells: Vec<Vec<Rect>> = vec![Vec::new(); groups.len()];
    let mut ff_cells: BTreeMap<FfKey, Vec<Rect>> = BTreeMap::new();
    for (gi, (g, cells)) in groups.iter().zip(&expanded).enumerate() {
        let aw = cells.len() as u64;
        for &cell in cells {
            let k = ff_key(g, cell).expect("instantiates");
            if ff_share[&k] > aw {
                ff_cells
                    .entry(k)
                    .or_default()
                    .push(Rect::cell(cell.0, cell.1));
            } else {
                aw_cells[gi].push(Rect::cell(cell.0, cell.1));
            }
        }
    }
    let mut w = CanonWork::default();
    let mut ff_records = 0usize;
    for cells in aw_cells.iter().filter(|c| !c.is_empty()) {
        ff_records += canon(cells, &mut w).len();
    }
    for cells in ff_cells.values() {
        ff_records += canon(cells, &mut w).len();
    }
    for cells in expanded.iter_mut() {
        strided += strided_count(cells);
    }
    Ok(format!(
        "{id}\t{}\t{deps}\t{canon_records}\t{strided}\t{ff_records}\t{:.4}\t{:.4}",
        sum.formulas,
        canon_records as f64 / strided.max(1) as f64,
        canon_records as f64 / ff_records.max(1) as f64,
    ))
}

struct Args(Vec<String>);
impl Args {
    fn get(&self, key: &str) -> Option<String> {
        self.0
            .iter()
            .position(|a| a == key)
            .and_then(|i| self.0.get(i + 1).cloned())
    }
}

fn main() -> Result<()> {
    let args = Args(std::env::args().skip(1).collect());
    let cmd = args.0.first().cloned().unwrap_or_default();
    let mut ids = default_corpus();
    if let Some(f) = args.get("--only") {
        ids.retain(|id| f.split(',').any(|x| id.contains(x)));
    }
    match cmd.as_str() {
        "list" => {
            for id in ids {
                println!("{id}");
            }
            Ok(())
        }
        "run" => {
            let out = PathBuf::from(args.get("--out").context("--out DIR")?);
            std::fs::create_dir_all(&out)?;
            let edits: usize = args.get("--edits").map_or(Ok(200), |s| s.parse())?;
            let cs: usize = args.get("--closure-seeds").map_or(Ok(256), |s| s.parse())?;
            let ds: usize = args.get("--direct-seeds").map_or(Ok(4096), |s| s.parse())?;
            let path = out.join("authority-corpus.tsv");
            let fresh = !path.exists();
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            if fresh {
                writeln!(f, "{}", Row::header())?;
            }
            for id in ids {
                eprintln!("{id}");
                match run_one(&id, edits, cs, ds) {
                    Ok(row) => writeln!(f, "{}", row.line())?,
                    Err(e) => writeln!(f, "{id}\tERROR\t{e}")?,
                }
                f.flush()?;
            }
            Ok(())
        }
        "losses" => {
            let out = PathBuf::from(args.get("--out").context("--out DIR")?);
            std::fs::create_dir_all(&out)?;
            let path = out.join("authority-losses.tsv");
            let fresh = !path.exists();
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            if fresh {
                writeln!(
                    f,
                    "workbook\tformulas\tdependencies\tcanon_records\tstrided_records\tbprime_canon_records\tstride_loss\tff_keying_loss"
                )?;
            }
            for id in ids {
                eprintln!("{id}");
                match losses_one(&id) {
                    Ok(line) => writeln!(f, "{line}")?,
                    Err(e) => writeln!(f, "{id}\tERROR\t{e}")?,
                }
                f.flush()?;
            }
            Ok(())
        }
        _ => bail!("usage: authority-corpus list|run|losses --out DIR [--only S] [--edits N]"),
    }
}
