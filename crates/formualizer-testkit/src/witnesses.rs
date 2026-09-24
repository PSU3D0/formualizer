//! Pre-M4 witness families as lifecycle scenarios.
//!
//! Each witness is a [`Shape`] plus a first-principles model of every family
//! cell under the lifecycle states the script produces. Structural goldens are
//! what the engine was observed to do at the registry size, not what it should
//! do; differences from the 4096-row pre-M4 matrix are noted per witness.
use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_workbook::Workbook;

use crate::scenario::{
    DemotionExpect, EnginePath, Expect, ExpectedFailureSpec, FailureFingerprint, Family,
    LifecycleOp, Orientation, Provenance, Purpose, ScenarioSize, ScenarioSpec, Script, SizeClass,
    Step, StructureExpect, Tags,
};
use crate::shape::{Cell, Extent, Range, Role, Scale, Shape, r#gen};

/// Lifecycle used by every witness: each mutation is followed by an evaluation
/// so value models can be checked at a settled state.
pub fn witness_script() -> Script {
    use crate::scenario::Position::Middle;
    Script::new([
        Step::Load,
        Step::Prepare,
        Step::EvaluateAll,
        Step::NoOp,
        Step::EvaluateAll,
        Step::SetValue(Role::PrimaryInput, 3.0),
        Step::EvaluateAll,
        Step::OverrideInterior,
        Step::EvaluateAll,
        Step::Undo,
        Step::EvaluateAll,
        Step::Redo,
        Step::EvaluateAll,
        Step::InsertRows {
            at: Middle,
            count: 1,
        },
        Step::EvaluateAll,
        Step::DeleteRows {
            at: Middle,
            count: 1,
        },
        Step::EvaluateAll,
    ])
}

const OVERRIDE_VALUE: f64 = 7.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Chain,
    Reverse,
    Stride,
    Window,
    Coupled,
    Independent,
    Horizontal,
    Blocks,
    Fixed,
    SameRelative,
    Expanding,
    Shifted,
    Lookup,
    Irregular,
}

impl Kind {
    pub const ALL: [Kind; 14] = [
        Kind::Chain,
        Kind::Reverse,
        Kind::Stride,
        Kind::Window,
        Kind::Coupled,
        Kind::Independent,
        Kind::Horizontal,
        Kind::Blocks,
        Kind::Fixed,
        Kind::SameRelative,
        Kind::Expanding,
        Kind::Shifted,
        Kind::Lookup,
        Kind::Irregular,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Kind::Chain => "chain",
            Kind::Reverse => "reverse",
            Kind::Stride => "stride",
            Kind::Window => "window",
            Kind::Coupled => "coupled",
            Kind::Independent => "independent",
            Kind::Horizontal => "horizontal",
            Kind::Blocks => "blocks",
            Kind::Fixed => "fixed-absolute-sum",
            Kind::SameRelative => "same-relative-sum",
            Kind::Expanding => "expanding-sum",
            Kind::Shifted => "shifted-window",
            Kind::Lookup => "lookup",
            Kind::Irregular => "irregular",
        }
    }

    fn family(self) -> Family {
        match self {
            Kind::Chain => Family::Chain,
            Kind::Reverse => Family::Reverse,
            Kind::Stride => Family::Stride,
            Kind::Window => Family::Window,
            Kind::Coupled => Family::Coupled,
            Kind::Independent | Kind::Horizontal => Family::Independent,
            Kind::Blocks => Family::Blocks,
            Kind::Fixed => Family::Fixed,
            Kind::SameRelative => Family::SameRelative,
            Kind::Expanding => Family::Expanding,
            Kind::Shifted => Family::Shifted,
            Kind::Lookup => Family::Lookup,
            Kind::Irregular => Family::Irregular,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Kind::Chain => "forward recurrence B[r]=B[r-1]+A[r]",
            Kind::Reverse => "reverse recurrence B[r]=B[r+1]+A[r] with B[n+1]=0",
            Kind::Stride => "stride-12 recurrence B[r]=B[r-12]+A[r]",
            Kind::Window => "two-predecessor window B[r]=B[r-1]/2+B[r-2]/4+A[r]",
            Kind::Coupled => "coupled columns B[r]=C[r-1]+A[r], C[r]=B[r]/2",
            Kind::Independent => "independent column B[r]=A[r]*2+1",
            Kind::Horizontal => "independent family laid out along row 2",
            Kind::Blocks => "independent column with a gap every 128 rows",
            Kind::Fixed => "fixed absolute SUM($A$1:$A$n) in every row",
            Kind::SameRelative => "identical relative SUM(A1:An) text in every row",
            Kind::Expanding => "expanding SUM(A$1:A[r])",
            Kind::Shifted => "eight-row moving window SUM(A[r]:A[r+7])",
            Kind::Lookup => "VLOOKUP into a fixed two-column table",
            Kind::Irregular => "irregular literal per row A1+A[r]+r*r",
        }
    }

    fn shape(self, rows: u32) -> Shape {
        let n = rows;
        let ones = || r#gen::constant(Cell::number(1.0));
        let all_rows = || Extent::Abs(1)..=Extent::Rows;
        Shape::new().scale(Scale::rows(rows)).sheet("S", |s| {
            match self {
                Kind::Horizontal => {
                    s.values(Range::row(Extent::Abs(1), all_rows()), ones());
                    s.family(
                        "family",
                        Range::row(Extent::Abs(2), all_rows()),
                        "={c}1*2+1",
                    );
                }
                Kind::Shifted => {
                    s.values(
                        Range::col("A", Extent::Abs(1)..=Extent::RowsPlus(7)),
                        ones(),
                    );
                    s.family("family", Range::col("B", all_rows()), "=SUM(A{r}:A{r+7})");
                }
                Kind::Reverse => {
                    s.values(Range::col("A", all_rows()), ones());
                    s.values(
                        Range::col("B", Extent::RowsPlus(1)..=Extent::RowsPlus(1)),
                        r#gen::constant(Cell::number(0.0)),
                    );
                    s.family("family", Range::col("B", all_rows()), "=B{r+1}+A{r}");
                }
                Kind::Lookup => {
                    s.values(Range::col("A", all_rows()), ones());
                    s.values(Range::col("D", all_rows()), r#gen::index_f64());
                    s.values(
                        Range::col("E", all_rows()),
                        Arc::new(|r, _, _| Cell::number(3.0 * r as f64)),
                    );
                    s.family(
                        "family",
                        Range::col("B", all_rows()),
                        "=VLOOKUP({r},$D$1:$E${n},2,FALSE)",
                    );
                }
                _ => {
                    s.values(Range::col("A", all_rows()), ones());
                    let b = Range::col("B", all_rows());
                    match self {
                        Kind::Chain => {
                            s.family("family", b, "=B{r-1}+A{r}").boundary("B1", "=A1");
                        }
                        Kind::Stride => {
                            let mut family = s.family("family", b, "=B{r-12}+A{r}");
                            for r in 1..=12.min(n) {
                                family = family.boundary(format!("B{r}"), format!("=A{r}"));
                            }
                        }
                        Kind::Window => {
                            s.family("family", b, "=B{r-1}/2+B{r-2}/4+A{r}")
                                .boundary("B1", "=A1")
                                .boundary("B2", "=B1/2+A2");
                        }
                        Kind::Coupled => {
                            s.family("family", b, "=C{r-1}+A{r}").boundary("B1", "=A1");
                            s.family("half", Range::col("C", all_rows()), "=B{r}/2");
                        }
                        Kind::Independent => {
                            s.family("family", b, "=A{r}*2+1");
                        }
                        Kind::Blocks => {
                            let family = s.family("family", b, "=A{r}*2+1");
                            // The 100-row rung deliberately leaves a 99-cell
                            // block: it documents the strict promotion cutoff.
                            if rows == 100 {
                                family.blocks(99, 1);
                            } else {
                                family.gap_every(128);
                            }
                        }
                        Kind::Fixed => {
                            s.family("family", b, "=SUM($A$1:$A${n})");
                        }
                        Kind::SameRelative => {
                            s.family("family", b, "=SUM(A1:A{n})");
                        }
                        Kind::Expanding => {
                            s.family("family", b, "=SUM(A$1:A{r})");
                        }
                        Kind::Irregular => {
                            s.family("family", b, "=A1+A{r}+{r}*{r}");
                        }
                        Kind::Horizontal | Kind::Shifted | Kind::Reverse | Kind::Lookup => {
                            unreachable!()
                        }
                    }
                }
            }
            s.role("A1", Role::PrimaryInput);
            s.role("family", Role::PrimaryFamily);
        })
    }

    /// Structural golden observed at 256 rows in AuthoritativeExperimental after
    /// the first evaluation (cumulative placements/demotions through that step).
    /// The trailing comment is the 4096-row observation from the pre-M4 matrix.
    fn structure_after_first_eval(self, rows: u32) -> StructureExpect {
        let mut expect = self.api_structure_after_first_eval(rows);
        expect.provenance = Some(Provenance::WorkbookApi);
        expect
    }

    /// The Umya reader hands ordinary cells to the same placement route as the
    /// API, so its goldens match the API goldens.
    fn umya_structure_after_first_eval(self, rows: u32) -> StructureExpect {
        let mut expect = self.api_structure_after_first_eval(rows);
        expect.provenance = Some(Provenance::XlsxUmya);
        expect
    }

    /// Observed for every witness when the same cells are loaded through the
    /// Calamine reader (FORM-000128): its source-formula ingress routes ordinary
    /// cells straight to legacy vertices and offers no span candidates, so
    /// nothing is placed or demoted. The Umya reader does not behave this way.
    fn calamine_structure_after_first_eval(self) -> StructureExpect {
        StructureExpect {
            mode: Some(FormulaPlaneMode::AuthoritativeExperimental),
            provenance: Some(Provenance::XlsxCalamine),
            placed_families: Some(0),
            active_spans: Some(0),
            demotions: Some(DemotionExpect {
                count: 0,
                reason: None,
            }),
            ..StructureExpect::default()
        }
    }

    fn api_structure_after_first_eval(self, rows: u32) -> StructureExpect {
        // The 100-cell promotion threshold is observable: 16-row families do
        // not place at all. Coupled has no eligible component at 16 either.
        if rows == 16 && self == Kind::Fixed {
            return StructureExpect {
                mode: Some(FormulaPlaneMode::AuthoritativeExperimental),
                placed_families: Some(1),
                active_spans: Some(1),
                demotions: None,
                ..StructureExpect::default()
            };
        }
        if rows == 16 {
            return StructureExpect {
                mode: Some(FormulaPlaneMode::AuthoritativeExperimental),
                placed_families: Some(0),
                active_spans: Some(0),
                demotions: Some(DemotionExpect {
                    count: 0,
                    reason: None,
                }),
                ..StructureExpect::default()
            };
        }
        if rows == 100 && self == Kind::Blocks {
            return StructureExpect {
                mode: Some(FormulaPlaneMode::AuthoritativeExperimental),
                placed_families: Some(0),
                active_spans: Some(0),
                demotions: Some(DemotionExpect {
                    count: 0,
                    reason: None,
                }),
                ..StructureExpect::default()
            };
        }
        // At exactly 100 rows only the B component of coupled is placed; it
        // remains active because its C partner is below the promotion cutoff.
        if rows == 100 && self == Kind::Coupled {
            return StructureExpect {
                mode: Some(FormulaPlaneMode::AuthoritativeExperimental),
                placed_families: Some(1),
                active_spans: Some(0),
                demotions: Some(DemotionExpect {
                    count: 1,
                    reason: Some("CycleMember".into()),
                }),
                ..StructureExpect::default()
            };
        }
        let (placed, active, demotions) = match self {
            // 4096: 0 spans. Own-result overlap rejected at placement.
            Kind::Chain | Kind::Reverse | Kind::Stride | Kind::Window => (0, 0, None),
            // 4096: 2 prepared -> 0. Producer-level cycle demotes both.
            Kind::Coupled => (
                2,
                0,
                Some(DemotionExpect {
                    count: 2,
                    reason: Some("CycleMember".into()),
                }),
            ),
            // 4096: 1 span.
            Kind::Independent | Kind::Fixed | Kind::Expanding | Kind::Shifted | Kind::Lookup => {
                (1, 1, None)
            }
            // 4096: 0 spans. Column-keyed grouping yields singletons.
            Kind::Horizontal => (0, 0, None),
            // 4096: 32 spans. Each 127-cell block clears the 100-cell threshold;
            // at 256 rows there are two blocks.
            Kind::Blocks => {
                let blocks = usize::try_from(rows.div_ceil(128)).unwrap_or(0);
                (blocks, blocks, None)
            }
            // 4096: 0 spans. Relative offsets differ per row -> distinct keys.
            Kind::SameRelative => (0, 0, None),
            // 4096: 0 spans. Distinct literal per row.
            Kind::Irregular => (0, 0, None),
        };
        StructureExpect {
            mode: Some(FormulaPlaneMode::AuthoritativeExperimental),
            placed_families: Some(placed),
            active_spans: Some(active),
            demotions,
            ..StructureExpect::default()
        }
    }

    fn engine_path(self) -> EnginePath {
        match self {
            Kind::Coupled => EnginePath::Demoted,
            Kind::Independent
            | Kind::Blocks
            | Kind::Fixed
            | Kind::Expanding
            | Kind::Shifted
            | Kind::Lookup => EnginePath::FormulaPlane,
            _ => EnginePath::Legacy,
        }
    }
}

/// Lifecycle state the value model is evaluated under.
#[derive(Clone, Copy, Debug)]
struct State {
    a1: f64,
    /// Interior override `(row, col)` of the primary family holding 7.0.
    override_at: Option<(u32, u32)>,
    /// Row at which one empty row was inserted (cells at or below shift by one).
    inserted_at: Option<u32>,
}

/// Expected `(row, col, value)` for every family cell under `state`.
fn model(kind: Kind, n: u32, state: State) -> Vec<(u32, u32, f64)> {
    let a = |r: u32| if r == 1 { state.a1 } else { 1.0 };
    let n_us = n as usize;
    let overridden = |r: u32, c: u32| state.override_at == Some((r, c));
    let mut out: Vec<(u32, u32, f64)> = Vec::new();
    match kind {
        Kind::Chain => {
            let mut prev = 0.0;
            for r in 1..=n {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    prev + a(r)
                };
                out.push((r, 2, v));
                prev = v;
            }
        }
        Kind::Reverse => {
            let mut next = 0.0;
            let mut rev = Vec::with_capacity(n_us);
            for r in (1..=n).rev() {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    next + a(r)
                };
                rev.push((r, 2, v));
                next = v;
            }
            rev.reverse();
            out.extend(rev);
        }
        Kind::Stride => {
            let mut b = vec![0.0; n_us + 1];
            for r in 1..=n {
                let i = r as usize;
                b[i] = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else if r <= 12 {
                    a(r)
                } else {
                    b[i - 12] + a(r)
                };
                out.push((r, 2, b[i]));
            }
        }
        Kind::Window => {
            let mut b = vec![0.0; n_us + 1];
            for r in 1..=n {
                let i = r as usize;
                b[i] = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else if r == 1 {
                    a(1)
                } else if r == 2 {
                    b[1] / 2.0 + a(2)
                } else {
                    b[i - 1] / 2.0 + b[i - 2] / 4.0 + a(r)
                };
                out.push((r, 2, b[i]));
            }
        }
        Kind::Coupled => {
            let mut prev_c = 0.0;
            for r in 1..=n {
                let b = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else if r == 1 {
                    a(1)
                } else {
                    prev_c + a(r)
                };
                let c = b / 2.0;
                out.push((r, 2, b));
                out.push((r, 3, c));
                prev_c = c;
            }
        }
        Kind::Independent => {
            for r in 1..=n {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    a(r) * 2.0 + 1.0
                };
                out.push((r, 2, v));
            }
        }
        Kind::Horizontal => {
            for c in 1..=n {
                let v = if overridden(2, c) {
                    OVERRIDE_VALUE
                } else {
                    a(c) * 2.0 + 1.0
                };
                out.push((2, c, v));
            }
        }
        Kind::Blocks => {
            for (i, r) in (1..=n).enumerate() {
                if (n == 100 && r == 100) || (n != 100 && (i + 1) % 128 == 0) {
                    continue;
                }
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    a(r) * 2.0 + 1.0
                };
                out.push((r, 2, v));
            }
        }
        Kind::Fixed | Kind::SameRelative => {
            let total = (n as f64 - 1.0) + state.a1;
            for r in 1..=n {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    total
                };
                out.push((r, 2, v));
            }
        }
        Kind::Expanding => {
            for r in 1..=n {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    (r as f64 - 1.0) + state.a1
                };
                out.push((r, 2, v));
            }
        }
        Kind::Shifted => {
            for r in 1..=n {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    (r..=r + 7).map(a).sum()
                };
                out.push((r, 2, v));
            }
        }
        Kind::Lookup => {
            for r in 1..=n {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    3.0 * r as f64
                };
                out.push((r, 2, v));
            }
        }
        Kind::Irregular => {
            for r in 1..=n {
                let v = if overridden(r, 2) {
                    OVERRIDE_VALUE
                } else {
                    state.a1 + a(r) + (r as f64) * (r as f64)
                };
                out.push((r, 2, v));
            }
        }
    }
    if let Some(at) = state.inserted_at {
        for cell in &mut out {
            if cell.0 >= at {
                cell.0 += 1;
            }
        }
    }
    out
}

fn oracle(kind: Kind, n: u32, state: State) -> Expect {
    Expect::Oracle(Arc::new(move |wb: &Workbook| {
        for (row, col, expected) in model(kind, n, state) {
            match wb.get_value("S", row, col) {
                Some(LiteralValue::Number(actual)) if (actual - expected).abs() <= 1e-9 => {}
                Some(LiteralValue::Int(actual)) if (actual as f64 - expected).abs() <= 1e-9 => {}
                other => {
                    return Err(format!(
                        "{}: S!R{row}C{col} expected {expected}, got {other:?} (state {state:?})",
                        kind.id()
                    ));
                }
            }
        }
        Ok(())
    }))
}

fn interior_cell(shape: &Shape) -> (u32, u32) {
    let cells = shape
        .resolve_role(Role::PrimaryFamily)
        .expect("witness shapes define a primary family");
    let (_, row, col) = cells[cells.len() / 2].clone();
    (row, col)
}

pub fn witness(kind: Kind, rows: u32) -> ScenarioSpec {
    let shape = kind.shape(rows);
    let (mid_row, mid_col) = interior_cell(&shape);
    let base = State {
        a1: 1.0,
        override_at: None,
        inserted_at: None,
    };
    let edited = State { a1: 3.0, ..base };
    let overridden = State {
        override_at: Some((mid_row, mid_col)),
        ..edited
    };
    let inserted = State {
        inserted_at: Some(mid_row),
        ..overridden
    };
    // Step indices follow `witness_script()`.
    let states = [
        (2, base),
        (4, base),
        (6, edited),
        (8, overridden),
        (10, edited),
        (12, overridden),
        (14, inserted),
        (16, overridden),
    ];
    let mut expects: Vec<(usize, Expect)> = states
        .iter()
        .flat_map(|(step, state)| {
            [
                (*step, oracle(kind, rows, *state)),
                (*step, Expect::NoErrors("S".into())),
            ]
        })
        .collect();
    // One million rows is nightly-only; do not claim a structural golden until
    // the recorded run is practical to collect.
    if rows < 1_000_000 {
        expects.push((2, Expect::Structure(kind.structure_after_first_eval(rows))));
        expects.push((
            2,
            Expect::Structure(kind.umya_structure_after_first_eval(rows)),
        ));
        expects.push((
            2,
            Expect::Structure(kind.calamine_structure_after_first_eval()),
        ));
    }
    ScenarioSpec {
        id: format!("witness.{}", kind.id()),
        description: kind.description().into(),
        shape,
        source: None,
        script: witness_script(),
        expects,
        tags: Tags {
            family: vec![kind.family()],
            orientation: vec![if kind == Kind::Horizontal {
                Orientation::Horizontal
            } else {
                Orientation::Vertical
            }],
            lifecycle: vec![
                LifecycleOp::Load,
                LifecycleOp::Evaluate,
                LifecycleOp::Edit,
                LifecycleOp::History,
                LifecycleOp::Structural,
            ],
            engine: vec![kind.engine_path()],
            purpose: vec![Purpose::Behavioral, Purpose::Trace],
            size: vec![size_class(rows)],
            ..Tags::default()
        },
        modes: vec![
            FormulaPlaneMode::Off,
            FormulaPlaneMode::AuthoritativeExperimental,
        ],
        sizes: vec![ScenarioSize::new(size_class(rows), rows)],
        expected_failures: known_failures(kind, rows),
    }
}

/// Tracked defects on the Program 1 base. At 16 rows only the fixed family
/// promotes; after undo/redo of the interior override, the row insert
/// republishes the family result over the override (18 instead of 7). The
/// b102ba90 goldens carried the M3 perf-line fix (3ec4d547), which is not on
/// this base. The Calamine reader never places spans (FORM-000128). The
/// fingerprint is the observed first mismatch, at the redo-then-insert
/// evaluation (step 14).
fn known_failures(kind: Kind, rows: u32) -> Vec<ExpectedFailureSpec> {
    if kind != Kind::Fixed || rows != 16 {
        return vec![];
    }
    [Provenance::WorkbookApi, Provenance::XlsxUmya]
        .into_iter()
        .map(|provenance| ExpectedFailureSpec {
            mode: FormulaPlaneMode::AuthoritativeExperimental,
            provenance: Some(provenance),
            failure: FailureFingerprint::expectation(
                14,
                "fixed-absolute-sum: S!R10C2 expected 7, got Some(Number(18.0)) (state State { a1: 3.0, override_at: Some((9, 2)), inserted_at: Some(9) })",
            ),
            reason: "base: a row insert over a promoted span republishes the family result over a live override (fixed on the M3 perf line by 3ec4d547, not on this base)".into(),
        })
        .collect()
}

fn size_class(rows: u32) -> SizeClass {
    match rows {
        0..=256 => SizeClass::Small,
        257..=4_096 => SizeClass::Medium,
        4_097..=100_000 => SizeClass::Large,
        _ => SizeClass::Nightly,
    }
}

/// All fourteen pre-M4 witnesses at `rows`. Value models and structural goldens
/// are bound to this size; build another registry for another rung of the ladder.
pub fn witness_registry(rows: u32) -> Vec<ScenarioSpec> {
    assert!(rows >= 16, "witnesses need at least 16 rows");
    Kind::ALL.iter().map(|kind| witness(*kind, rows)).collect()
}
