//! Program 1 value parity: legacy vs the unified authority on actual results.
//!
//! Build this binary twice, without and with `unified_authority`, run both on
//! the same corpus and diff the outputs. Each workbook loads through the
//! ordinary `Workbook` path (xlsx via calamine; witnesses rendered with their
//! values), evaluates, then runs deterministic edit rounds (value edits,
//! value clears, formula re-sets and rewrites), evaluating after each round.
//! Every step prints a digest of all cell values in the used range; with
//! `--dump-dir` it also writes the values, for localising a digest mismatch.
//!
//!   authority-values list
//!   authority-values run --out FILE [--only SUBSTR[,SUBSTR]] [--rounds N]
//!       [--edits N] [--dump-dir DIR]
//!
//! Output rows: `workbook  step  cells  digest  note` (note is the eval or
//! load error, or `ok`).

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("authority-values requires feature `formualizer_runner`");
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    enabled::main()
}

#[cfg(feature = "formualizer_runner")]
mod enabled {
    use anyhow::{Context, Result, anyhow, bail};
    use formualizer_common::LiteralValue;
    use formualizer_testkit::shape::{Cell as ShapeCell, Literal};
    use formualizer_testkit::witnesses::{Kind, witness};
    use formualizer_workbook::{
        CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
    };
    use std::collections::BTreeSet;
    use std::hash::{Hash, Hasher};
    use std::io::Write as _;
    use std::path::{Path, PathBuf};

    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed ^ 0x9e37_79b9_7f4a_7c15)
        }
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((self.0 >> 33) % (n.max(1) as u64)) as usize
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

    /// The `authority-corpus` workbook list (witnesses at 4096 rows only).
    fn corpus() -> Vec<String> {
        let mut ids: Vec<String> = Kind::ALL
            .into_iter()
            .map(|k| format!("witness:{}:4096", k.id()))
            .collect();
        for sub in ["real", "synthetic"] {
            let mut files: Vec<PathBuf> = std::fs::read_dir(corpus_root().join(sub))
                .map(|rd| rd.filter_map(|e| e.ok().map(|e| e.path())).collect())
                .unwrap_or_default();
            files.retain(|p| p.extension().is_some_and(|e| e == "xlsx"));
            files.sort();
            ids.extend(files.iter().map(|p| format!("xlsx:{}", p.display())));
        }
        let mut seen = BTreeSet::new();
        if let Ok(text) = std::fs::read_to_string(enron_root().join("sample-manifest.tsv")) {
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

    fn load(id: &str) -> Result<Workbook> {
        if let Some(rest) = id.strip_prefix("witness:") {
            let (kind, rows) = rest.rsplit_once(':').context("witness:<kind>:<rows>")?;
            let rows: u32 = rows.parse()?;
            let kind = Kind::ALL
                .into_iter()
                .find(|k| k.id() == kind)
                .ok_or_else(|| anyhow!("unknown witness {kind}"))?;
            let mut shape = witness(kind, rows).shape;
            shape.scale.rows = rows;
            let rendered = shape.render().map_err(|e| anyhow!("{e}"))?;
            let mut wb = Workbook::new_with_config(WorkbookConfig::interactive());
            for s in &shape.sheets {
                wb.add_sheet(&s.name).map_err(|e| anyhow!("{e}"))?;
            }
            for rc in rendered {
                match rc.cell {
                    ShapeCell::Formula(f) => {
                        let f = if f.starts_with('=') {
                            f
                        } else {
                            format!("={f}")
                        };
                        wb.set_formula(&rc.sheet, rc.row, rc.col, &f)
                            .map_err(|e| anyhow!("{e}"))?;
                    }
                    ShapeCell::Value(v) => {
                        let v = match v {
                            Literal::Number(n) => LiteralValue::Number(n),
                            Literal::Text(s) => LiteralValue::Text(s),
                            Literal::Bool(b) => LiteralValue::Boolean(b),
                            Literal::Empty => continue,
                        };
                        wb.set_value(&rc.sheet, rc.row, rc.col, v)
                            .map_err(|e| anyhow!("{e}"))?;
                    }
                }
            }
            return Ok(wb);
        }
        let path = id
            .strip_prefix("xlsx:")
            .or_else(|| id.strip_prefix("enron:"))
            .ok_or_else(|| anyhow!("unknown workbook id {id}"))?;
        let backend =
            CalamineAdapter::open_path(Path::new(path)).map_err(|e| anyhow!("open {path}: {e}"))?;
        Workbook::from_reader(
            backend,
            LoadStrategy::EagerAll,
            WorkbookConfig::interactive(),
        )
        .map_err(|e| anyhow!("load {path}: {e}"))
    }

    /// Canonical text of a value: exact floats, errors by kind.
    fn canon(v: &LiteralValue) -> String {
        match v {
            LiteralValue::Error(e) => format!("#{:?}", e.kind),
            LiteralValue::Number(n) => format!("n{:?}", n),
            other => format!("{other:?}"),
        }
    }

    type Cells = Vec<(String, u32, u32, String)>;

    fn snapshot(wb: &Workbook) -> Cells {
        let mut out = Vec::new();
        for sheet in wb.sheet_names() {
            let Some((rows, cols)) = wb.sheet_dimensions(&sheet) else {
                continue;
            };
            for r in 1..=rows {
                for c in 1..=cols {
                    if let Some(v) = wb.get_value(&sheet, r, c)
                        && !matches!(v, LiteralValue::Empty)
                    {
                        out.push((sheet.clone(), r, c, canon(&v)));
                    }
                }
            }
        }
        out
    }

    fn digest(cells: &Cells) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        cells.hash(&mut h);
        h.finish()
    }

    /// A value cell `(sheet, row, col)`.
    type ValueSite = (String, u32, u32);
    /// A formula cell `(sheet, row, col, text)`.
    type FormulaSite = (String, u32, u32, String);

    /// Edit sites taken from the loaded workbook: numeric value cells and
    /// formula cells (with their text), in a deterministic order.
    fn sites(wb: &Workbook) -> (Vec<ValueSite>, Vec<FormulaSite>) {
        let mut values = Vec::new();
        let mut formulas = Vec::new();
        for sheet in wb.sheet_names() {
            let Some((rows, cols)) = wb.sheet_dimensions(&sheet) else {
                continue;
            };
            for r in 1..=rows {
                for c in 1..=cols {
                    if let Some(f) = wb.get_formula(&sheet, r, c) {
                        formulas.push((sheet.clone(), r, c, f));
                    } else if matches!(wb.get_value(&sheet, r, c), Some(LiteralValue::Number(_))) {
                        values.push((sheet.clone(), r, c));
                    }
                }
            }
        }
        (values, formulas)
    }

    struct Out<'a> {
        file: &'a mut std::fs::File,
        dump: Option<PathBuf>,
        id: &'a str,
    }

    impl Out<'_> {
        fn step(&mut self, step: &str, wb: &Workbook, note: &str) -> Result<()> {
            let cells = snapshot(wb);
            writeln!(
                self.file,
                "{}\t{step}\t{}\t{:016x}\t{note}",
                self.id,
                cells.len(),
                digest(&cells)
            )?;
            self.file.flush()?;
            if let Some(dir) = &self.dump {
                let name: String = self
                    .id
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect();
                let mut f = std::fs::File::create(dir.join(format!("{name}.{step}.tsv")))?;
                for (s, r, c, v) in &cells {
                    writeln!(f, "{s}\t{r}\t{c}\t{v}")?;
                }
            }
            Ok(())
        }
    }

    fn note<T, E: std::fmt::Display>(r: std::result::Result<T, E>) -> String {
        match r {
            Ok(_) => "ok".into(),
            Err(e) => format!("ERR {e}").replace(['\t', '\n'], " "),
        }
    }

    fn run_one(id: &str, rounds: usize, edits: usize, out: &mut Out<'_>) -> Result<()> {
        let mut wb = match load(id) {
            Ok(wb) => wb,
            Err(e) => {
                writeln!(out.file, "{id}\tload\t0\t0\tERR {e}")?;
                return Ok(());
            }
        };
        let (values, formulas) = sites(&wb);
        let n = note(wb.evaluate_all());
        out.step("eval0", &wb, &n)?;
        let mut g = Rng::new(0xA17E ^ id.len() as u64);
        for round in 1..=rounds {
            for _ in 0..edits {
                let pick = g.below(100);
                if pick < 55 && !values.is_empty() {
                    let (s, r, c) = &values[g.below(values.len())];
                    let _ = wb.set_value(s, *r, *c, LiteralValue::Number(g.below(1000) as f64));
                } else if pick < 65 && !values.is_empty() {
                    let (s, r, c) = &values[g.below(values.len())];
                    let _ = wb.set_value(s, *r, *c, LiteralValue::Empty);
                } else if !formulas.is_empty() {
                    let (s, r, c, f) = &formulas[g.below(formulas.len())];
                    let body = f.strip_prefix('=').unwrap_or(f);
                    let text = if pick < 85 {
                        format!("={body}")
                    } else {
                        format!("=({body})+1")
                    };
                    let _ = wb.set_formula(s, *r, *c, &text);
                }
            }
            let n = note(wb.evaluate_all());
            out.step(&format!("round{round}"), &wb, &n)?;
        }
        Ok(())
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

    pub fn main() -> Result<()> {
        let args = Args(std::env::args().skip(1).collect());
        let mut ids = corpus();
        if let Some(f) = args.get("--only") {
            ids.retain(|id| f.split(',').any(|x| id.contains(x)));
        }
        match args.0.first().map(String::as_str) {
            Some("list") => {
                for id in ids {
                    println!("{id}");
                }
                Ok(())
            }
            Some("run") => {
                let path = PathBuf::from(args.get("--out").context("--out FILE")?);
                let rounds: usize = args.get("--rounds").map_or(Ok(4), |s| s.parse())?;
                let edits: usize = args.get("--edits").map_or(Ok(25), |s| s.parse())?;
                let dump = args.get("--dump-dir").map(PathBuf::from);
                if let Some(d) = &dump {
                    std::fs::create_dir_all(d)?;
                }
                let mut file = std::fs::File::create(&path)?;
                writeln!(file, "# authority-values")?;
                for id in ids {
                    eprintln!("{id}");
                    let mut out = Out {
                        file: &mut file,
                        dump: dump.clone(),
                        id: &id,
                    };
                    if let Err(e) = run_one(&id, rounds, edits, &mut out) {
                        writeln!(file, "{id}\tharness\t0\t0\tERR {e}")?;
                    }
                }
                Ok(())
            }
            _ => bail!("usage: authority-values list|run --out FILE [--only S]"),
        }
    }
}
