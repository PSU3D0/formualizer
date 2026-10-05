//! Chain units reading cells of their own column outside the run: an
//! anchor (`$B$4`, `B$4`, the seed row by absolute row, a cell below the
//! run) must read its value, as the per-cell path does. Every generated
//! layout is compared against an engine that evaluates each formula on its
//! own; debug builds also run the chain self-check against the per-cell
//! path for every chain unit.
use super::common::arrow_eval_config;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const SHEET: &str = "S";
/// First member row (1-based); the seed is the row above.
const FIRST: u32 = 9;
const SEED: f64 = 18000.0;

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// The default engine: family execution, lift and chain units.
    Candidate { parallel: bool },
    /// Family execution off (every formula cell on its own).
    NoFamily { parallel: bool },
    /// The oracle: no compression, no lift, no family execution.
    Reference,
}

fn config(mode: Mode) -> EvalConfig {
    match mode {
        Mode::Candidate { parallel } => EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        },
        Mode::NoFamily { parallel } => EvalConfig {
            enable_parallel: parallel,
            family_execution: false,
            ..arrow_eval_config()
        },
        Mode::Reference => EvalConfig {
            enable_parallel: false,
            family_execution: false,
            family_lift: false,
            formula_compression: false,
            ..arrow_eval_config()
        },
    }
}

const MODES: [Mode; 4] = [
    Mode::Candidate { parallel: false },
    Mode::Candidate { parallel: true },
    Mode::NoFamily { parallel: false },
    Mode::NoFamily { parallel: true },
];

/// Where the anchor cell sits in the run's column.
#[derive(Clone, Copy, Debug)]
enum AnchorRow {
    /// Row 4, above the run (the decay-schedule layout).
    Above,
    /// Below the run.
    Below,
}

/// What the anchor cell holds.
#[derive(Clone, Copy, Debug)]
enum AnchorKind {
    Constant,
    /// `=B3/12` (a formula in the anchor's column).
    Formula,
    /// A formula reading another sheet.
    FormulaOtherSheet,
    /// Nothing: reads as empty on every path.
    Empty,
}

/// One generated layout: a recurrence filled down `n` rows in each of
/// `cols` starting at row [`FIRST`], seeded with [`SEED`] in the row above.
#[derive(Clone, Debug)]
struct Layout {
    n: u32,
    cols: Vec<u32>,
    anchor_col: u32,
    anchor_row: AnchorRow,
    anchor_kind: AnchorKind,
    /// The member template: `{c}` the member's column letter, `{a}` the
    /// anchor's column letter, `{p}` the row above, `{l}` five rows up, `{r}` the member's row,
    /// `{ar}` the anchor's row, `{ar1}` the row above it.
    template: &'static str,
}

fn letter(col: u32) -> String {
    assert!((1..=26).contains(&col));
    char::from(b'A' + (col - 1) as u8).to_string()
}

impl Layout {
    fn anchor_row(&self) -> u32 {
        match self.anchor_row {
            AnchorRow::Above => 4,
            AnchorRow::Below => FIRST + self.n + 5,
        }
    }

    fn last_row(&self) -> u32 {
        (FIRST + self.n + 6).max(self.anchor_row() + 1)
    }

    fn formula(&self, col: u32, row: u32) -> String {
        self.template
            .replace("{c}", &letter(col))
            .replace("{a}", &letter(self.anchor_col))
            .replace("{ar1}", &(self.anchor_row() - 1).to_string())
            .replace("{ar}", &self.anchor_row().to_string())
            .replace("{p}", &(row - 1).to_string())
            .replace("{l}", &(row - 5).to_string())
            .replace("{r}", &row.to_string())
    }

    fn build(&self, mode: Mode) -> Engine<TestWorkbook> {
        let mut e = Engine::new(TestWorkbook::new(), config(mode));
        let ar = self.anchor_row();
        let a = self.anchor_col;
        // Inputs: column A, the other sheet, the anchor's rate cell above it.
        for r in 1..=self.last_row() {
            e.set_cell_value(SHEET, r, 1, LiteralValue::Number(f64::from(r % 7) + 0.5))
                .unwrap();
            e.set_cell_value("Other", r, a, LiteralValue::Number(0.01 * f64::from(r)))
                .unwrap();
        }
        e.set_cell_value(SHEET, ar - 1, a, LiteralValue::Number(0.2))
            .unwrap();
        match self.anchor_kind {
            AnchorKind::Constant => e
                .set_cell_value(SHEET, ar, a, LiteralValue::Number(0.2 / 12.0))
                .unwrap(),
            AnchorKind::Formula => e
                .set_cell_formula(
                    SHEET,
                    ar,
                    a,
                    parse(format!("=+{}{}/12", letter(a), ar - 1)).unwrap(),
                )
                .unwrap(),
            AnchorKind::FormulaOtherSheet => e
                .set_cell_formula(
                    SHEET,
                    ar,
                    a,
                    parse(format!("=Other!{}{}*2", letter(a), ar)).unwrap(),
                )
                .unwrap(),
            AnchorKind::Empty => {}
        }
        // A name for the anchor cell (templates reading it through `rate`).
        if self.template.contains("rate") {
            let sheet_id = e.sheet_id(SHEET).unwrap();
            e.define_name(
                "rate",
                NamedDefinition::Cell(CellRef::new(sheet_id, Coord::from_excel(ar, a, true, true))),
                NameScope::Workbook,
            )
            .unwrap();
        }
        for &c in &self.cols {
            e.set_cell_value(SHEET, FIRST - 1, c, LiteralValue::Number(SEED))
                .unwrap();
            for r in FIRST..FIRST + self.n {
                e.set_cell_formula(SHEET, r, c, parse(self.formula(c, r)).unwrap())
                    .unwrap();
            }
        }
        e
    }
}

fn key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        Some(LiteralValue::Error(e)) => format!("E{:?}", e.kind),
        other => format!("{other:?}"),
    }
}

fn assert_same(layout: &Layout, a: &Engine<TestWorkbook>, b: &Engine<TestWorkbook>, ctx: &str) {
    let max_col = layout
        .cols
        .iter()
        .copied()
        .max()
        .unwrap()
        .max(layout.anchor_col);
    for r in 1..=layout.last_row() {
        for c in 1..=max_col {
            let (x, y) = (a.get_cell_value(SHEET, r, c), b.get_cell_value(SHEET, r, c));
            assert_eq!(
                key(x.clone()),
                key(y.clone()),
                "{ctx}: {SHEET}!{}{r}: {x:?} vs per-cell {y:?}\n{layout:?}",
                letter(c)
            );
        }
    }
}

/// Edits that recalculate the chain: the anchor's input, then the seed.
fn edit(layout: &Layout, e: &mut Engine<TestWorkbook>, step: usize) {
    let (ar, a) = (layout.anchor_row(), layout.anchor_col);
    match step {
        0 => e
            .set_cell_value(SHEET, ar - 1, a, LiteralValue::Number(0.6))
            .unwrap(),
        1 => {
            for &c in &layout.cols {
                e.set_cell_value(SHEET, FIRST - 1, c, LiteralValue::Number(500.0))
                    .unwrap();
            }
        }
        2 => e
            .set_cell_value("Other", ar, a, LiteralValue::Number(0.75))
            .unwrap(),
        _ => unreachable!(),
    }
}

/// Every mode in `modes` equals the per-cell oracle on `layout`, at first
/// evaluation and after the first `edits` edits. Returns the members the
/// chain lift took.
fn check(layout: &Layout, modes: &[Mode], edits: usize) -> u64 {
    let mut reference = layout.build(Mode::Reference);
    reference.evaluate_all().unwrap();
    let mut engines: Vec<(Mode, Engine<TestWorkbook>)> =
        modes.iter().map(|&m| (m, layout.build(m))).collect();
    for (mode, e) in &mut engines {
        e.evaluate_all().unwrap();
        assert_same(layout, e, &reference, &format!("{mode:?} first eval"));
    }
    for step in 0..edits {
        edit(layout, &mut reference, step);
        reference.evaluate_all().unwrap();
        for (mode, e) in &mut engines {
            edit(layout, e, step);
            e.evaluate_all().unwrap();
            assert_same(layout, e, &reference, &format!("{mode:?} edit {step}"));
        }
    }
    engines
        .iter()
        .map(|(_, e)| e.chained_members_for_test())
        .sum()
}

/// Member templates over the run's own column and the anchor.
const TEMPLATES: &[&str] = &[
    // The decay schedule (absolute anchor).
    "=+{c}{p}-({c}{p}*${a}${ar})",
    // Row-absolute, column-relative.
    "={c}{p}*(1-{a}${ar})",
    // Column-absolute, row-relative: the anchor column's row above (the
    // member above itself when the run is in the anchor's column).
    "={c}{p}+${a}{p}",
    // A lagged read of the run's own column (rows above the run, then
    // other members).
    "={c}{p}-{c}{l}/100",
    // The seed by absolute row (the cell above the first member).
    "={c}{p}-{c}$8*{a}${ar}",
    // Explicit own-sheet prefix.
    "={c}{p}*(1-S!${a}${ar})",
    // The other sheet's cell at the anchor's address.
    "={c}{p}*(1-Other!${a}${ar})",
    // A range over the anchor.
    "={c}{p}*(1-SUM(${a}${ar1}:${a}${ar})/10)",
    // A defined name for the anchor.
    "={c}{p}*(1-rate)",
    // The anchor through INDEX.
    "={c}{p}*(1-INDEX(${a}${ar1}:${a}${ar},2))",
    // A cross-column input with the anchor.
    "={c}{p}+$A{r}*${a}${ar}",
    // Unary and power.
    "=-(-{c}{p})*(1+${a}${ar})^1",
];

/// Tiny deterministic generator (no extra dev-dependency).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        xs[(self.next() % xs.len() as u64) as usize]
    }
}

/// The decay schedule of the bug report, in its reported variants, at the
/// sizes around the planner's small-request limit.
#[test]
fn decay_schedule_reads_same_column_anchor() {
    for n in [31, 32, 33, 64, 1000] {
        for (cols, anchor_kind) in [
            (vec![2], AnchorKind::Formula),
            (vec![2], AnchorKind::Constant),
            (vec![5], AnchorKind::Formula),
        ] {
            let layout = Layout {
                n,
                cols,
                anchor_col: 2,
                anchor_row: AnchorRow::Above,
                anchor_kind,
                template: TEMPLATES[0],
            };
            for mode in MODES {
                let mut e = layout.build(mode);
                e.evaluate_all().unwrap();
                let c = layout.cols[0];
                // Excel: 18000 * (1 - 0.2/12)^k.
                let mut expect = SEED;
                for r in FIRST..FIRST + n {
                    expect -= expect * (0.2 / 12.0);
                    match e.get_cell_value(SHEET, r, c) {
                        Some(LiteralValue::Number(x)) => assert!(
                            (x - expect).abs() <= 1e-9 * expect.abs(),
                            "{mode:?} n={n} {layout:?} R{r}: {x} vs {expect}"
                        ),
                        other => panic!("{mode:?} n={n} R{r}: {other:?}"),
                    }
                }
                assert_eq!(
                    e.get_cell_value(SHEET, FIRST, c),
                    Some(LiteralValue::Number(17700.0))
                );
            }
        }
    }
}

/// The reported shape still takes the chain lift (the fast path is kept).
#[test]
fn decay_schedule_keeps_chain_lift() {
    let layout = Layout {
        n: 64,
        cols: vec![2],
        anchor_col: 2,
        anchor_row: AnchorRow::Above,
        anchor_kind: AnchorKind::Formula,
        template: TEMPLATES[0],
    };
    for n in [33u32, 64, 1000] {
        let layout = Layout {
            n,
            ..layout.clone()
        };
        let mut e = layout.build(Mode::Candidate { parallel: false });
        e.evaluate_all().unwrap();
        // The run's members (the planner may leave a few edge members to
        // their own layers).
        let chained = e.chained_members_for_test();
        assert!(chained + 3 >= u64::from(n), "n={n}: {chained} chained");
    }
}

/// Every template x anchor placement x anchor kind, with the run in the
/// anchor's column, beside it, and across two columns, against the
/// per-cell oracle (the chain-capable modes; the random layouts below
/// cover family execution off and every edit).
#[test]
fn chain_reads_match_per_cell_over_generated_layouts() {
    let mut chained = 0u64;
    for &template in TEMPLATES {
        for anchor_row in [AnchorRow::Above, AnchorRow::Below] {
            for anchor_kind in [
                AnchorKind::Constant,
                AnchorKind::Formula,
                AnchorKind::FormulaOtherSheet,
            ] {
                for cols in [vec![2], vec![5], vec![2, 3]] {
                    let layout = Layout {
                        n: 40,
                        cols,
                        anchor_col: 2,
                        anchor_row,
                        anchor_kind,
                        template,
                    };
                    chained += check(&layout, &MODES[..2], 1);
                }
            }
        }
    }
    assert!(chained > 0, "the generated layouts exercise the chain lift");
}

/// Random sizes and anchor columns around the small-plan limit.
#[test]
fn chain_reads_match_per_cell_over_random_layouts() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0..48 {
        let anchor_col = rng.pick(&[2u32, 3, 4]);
        let layout = Layout {
            n: rng.pick(&[2u32, 3, 31, 32, 33, 47, 64, 65]),
            cols: rng
                .pick(&[&[2u32][..], &[3], &[4], &[2, 3], &[3, 4], &[2, 4]])
                .to_vec(),
            anchor_col,
            anchor_row: rng.pick(&[AnchorRow::Above, AnchorRow::Below]),
            anchor_kind: rng.pick(&[
                AnchorKind::Constant,
                AnchorKind::Formula,
                AnchorKind::FormulaOtherSheet,
                AnchorKind::Empty,
            ]),
            template: rng.pick(TEMPLATES),
        };
        check(&layout, &MODES, 3);
    }
}
