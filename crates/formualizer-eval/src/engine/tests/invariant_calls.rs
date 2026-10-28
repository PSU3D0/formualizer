//! Program 3: a family's run-invariant calls (every reference the same cell
//! or range for all members of a column run, through pure context-free
//! functions: `INDEX(MATCH(..))` over a fixed table keyed by an absolute-row
//! header) evaluate once per run inside the lift, and a lookup keyed by a
//! member's own cells reuses results across equal keys (general memo).
//! Values must equal an engine without the lift and kernels (every member
//! walked), through edits.
use super::common::arrow_eval_config;
use crate::args::ArgSchema;
use crate::engine::{Engine, EvalConfig};
use crate::function::{FnCaps, Function};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::ExcelError;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

const ROWS: u32 = 120;

struct CountedAbs {
    caps: FnCaps,
    calls: Arc<AtomicUsize>,
}

impl Function for CountedAbs {
    fn name(&self) -> &'static str {
        "ABS"
    }

    fn caps(&self) -> FnCaps {
        self.caps
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Fixed output keeps the PURE control valid; the counter observes
        // physical execution without using a volatile-result provider.
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CalcValue::Scalar(LiteralValue::Number(1.0)))
    }
}

const ELIGIBILITY_ROWS: u32 = 8;

fn eligibility_engine(
    formula: &str,
    caps: FnCaps,
    lift: bool,
    kernels: bool,
) -> (Engine<TestWorkbook>, Arc<AtomicUsize>) {
    crate::builtins::load_builtins();
    let calls = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new().with_function(Arc::new(CountedAbs {
        caps,
        calls: calls.clone(),
    }));
    let mut e = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: false,
            family_lift: lift,
            family_kernels: kernels,
            ..arrow_eval_config()
        },
    );
    for row in 1..=ELIGIBILITY_ROWS {
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))
            .unwrap();
        e.set_cell_formula(
            "Sheet1",
            row,
            2,
            parse(formula.replace('@', &row.to_string())).unwrap(),
        )
        .unwrap();
    }
    for row in 1..=2 {
        e.set_cell_value("Sheet1", row, 26, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    e.evaluate_all().unwrap();
    let expected = if formula.starts_with("=IF(A@>0,7,") {
        7.0
    } else {
        1.0
    };
    for row in 1..=ELIGIBILITY_ROWS {
        assert_eq!(
            e.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(expected)),
            "{formula} at row {row}"
        );
    }
    (e, calls)
}

#[test]
fn eligibility_lift_rejects_override_without_pure() {
    for formula in ["=IF(A@>0,7,ABS($Z$1))", "=IF(A@>0,ABS($Z$1),7)"] {
        let (e, calls) = eligibility_engine(formula, FnCaps::empty(), true, false);
        assert_eq!(e.lifted_members_for_test(), 0, "{formula}");
        assert_eq!(e.invariant_bound_members_for_test(), 0, "{formula}");
        let expected = if formula.starts_with("=IF(A@>0,7,") {
            0
        } else {
            ELIGIBILITY_ROWS as usize
        };
        assert_eq!(calls.load(Ordering::SeqCst), expected, "{formula}");
    }
}

#[test]
fn eligibility_binding_rejects_override_without_pure() {
    for formula in ["=IF(A@>0,7,ABS($Z$1))", "=IF(A@>0,ABS($Z$1),7)"] {
        let (e, calls) = eligibility_engine(formula, FnCaps::empty(), false, true);
        assert_eq!(e.lifted_members_for_test(), 0, "{formula}");
        assert_eq!(e.invariant_bound_members_for_test(), 0, "{formula}");
        let expected = if formula.starts_with("=IF(A@>0,7,") {
            0
        } else {
            ELIGIBILITY_ROWS as usize
        };
        assert_eq!(calls.load(Ordering::SeqCst), expected, "{formula}");
    }
}

#[test]
fn eligibility_lift_retains_pure_override() {
    let (e, calls) = eligibility_engine("=IF(A@>0,ABS($Z$1),7)", FnCaps::PURE, true, false);
    assert!(e.lifted_members_for_test() > 0);
    assert_eq!(e.invariant_bound_members_for_test(), 0);
    assert!(calls.load(Ordering::SeqCst) > 0);
}

#[test]
fn eligibility_binding_retains_pure_override() {
    let (e, calls) = eligibility_engine("=IF(A@>0,ABS($Z$1),7)", FnCaps::PURE, false, true);
    assert_eq!(e.lifted_members_for_test(), 0);
    assert!(e.invariant_bound_members_for_test() > 0);
    assert!(calls.load(Ordering::SeqCst) > 0);
}

#[test]
fn eligibility_specialized_memo_rejects_nonpure_key_subtrees() {
    for key in ["ABS(A@)", "ROUND(ABS(A@),0)", "ABS(A@)+0", "-(-ABS(A@))"] {
        let formula = format!("=MATCH({key},$Z$1:$Z$2,0)");
        let (e, calls) = eligibility_engine(&formula, FnCaps::empty(), false, true);
        assert_eq!(e.memo_hits_for_test(), 0, "{formula}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            ELIGIBILITY_ROWS as usize,
            "{formula}"
        );
    }
}

#[test]
fn eligibility_specialized_memo_retains_pure_key_subtrees() {
    for key in ["ABS(A@)", "ROUND(ABS(A@),0)", "ABS(A@)+0", "-(-ABS(A@))"] {
        let formula = format!("=MATCH({key},$Z$1:$Z$2,0)");
        let (e, _) = eligibility_engine(&formula, FnCaps::PURE, false, true);
        assert!(e.memo_hits_for_test() > 0, "{formula}");
    }
}

#[test]
fn eligibility_general_memo_rejects_override_without_pure() {
    let (e, calls) = eligibility_engine(
        "=MATCH(ABS(A@),$Z$1:$Z$2,0)+0",
        FnCaps::empty(),
        false,
        true,
    );
    assert_eq!(e.memo_hits_for_test(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), ELIGIBILITY_ROWS as usize);
}

#[test]
fn eligibility_general_memo_retains_pure_override() {
    let (e, _) = eligibility_engine("=MATCH(ABS(A@),$Z$1:$Z$2,0)+0", FnCaps::PURE, false, true);
    assert!(e.memo_hits_for_test() > 0);
}

fn engine(lift: bool, parallel: bool) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        family_lift: lift,
        family_kernels: lift,
        enable_parallel: parallel,
        ..arrow_eval_config()
    };
    let mut e = Engine::new(TestWorkbook::new(), config);
    // Lookup table on Assumptions: keys A2:A4 (text), rates B2:B4, and a
    // numeric table D2:E6.
    for (r, (k, v)) in [("North", 1.5), ("South", 2.0), ("East", 0.5)]
        .iter()
        .enumerate()
    {
        let r = r as u32 + 2;
        e.set_cell_value("Assumptions", r, 1, LiteralValue::Text((*k).into()))
            .unwrap();
        e.set_cell_value("Assumptions", r, 2, LiteralValue::Number(*v))
            .unwrap();
    }
    for r in 2..=6 {
        e.set_cell_value(
            "Assumptions",
            r,
            4,
            LiteralValue::Number(f64::from(r) * 10.0),
        )
        .unwrap();
        e.set_cell_value(
            "Assumptions",
            r,
            5,
            LiteralValue::Number(f64::from(r) / 4.0),
        )
        .unwrap();
    }
    // Headers row 1 (text keys per column), data in column A.
    for (c, k) in [(2, "South"), (3, "East"), (4, "West"), (5, "North")] {
        e.set_cell_value("Sheet1", 1, c, LiteralValue::Text(k.into()))
            .unwrap();
    }
    for r in 2..=ROWS {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r % 9) - 2.0))
            .unwrap();
    }
    for c in 2..=5u32 {
        let col = (b'A' + (c - 1) as u8) as char;
        for r in 2..=ROWS {
            let f = format!(
                "=$A{r}*INDEX(Assumptions!$B$2:$B$4,MATCH({col}$1,Assumptions!$A$2:$A$4,0))+IFERROR(VLOOKUP(30,Assumptions!$D$2:$E$6,2,0),-1)"
            );
            e.set_cell_formula("Sheet1", r, c, parse(&f).unwrap())
                .unwrap();
        }
    }
    // A lookup keyed by each member's own cell: the general memo keys it
    // by that cell's value (the keys repeat down the column).
    for r in 2..=ROWS {
        e.set_cell_formula(
            "Sheet1",
            r,
            7,
            parse(format!(
                "=INDEX(Assumptions!$E$2:$E$6,MATCH($A{r}+4,Assumptions!$D$2:$D$6,1))*2"
            ))
            .unwrap(),
        )
        .unwrap();
    }
    // A whole-template invariant family.
    for r in 2..=ROWS {
        e.set_cell_formula(
            "Sheet1",
            r,
            6,
            parse("=INDEX(Assumptions!$B$2:$B$4,MATCH(B$1,Assumptions!$A$2:$A$4,0))").unwrap(),
        )
        .unwrap();
    }
    e
}

fn key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        other => format!("{other:?}"),
    }
}

#[test]
fn run_invariant_calls_match_the_walk_through_edits() {
    for parallel in [false, true] {
        let mut a = engine(true, parallel);
        let mut b = engine(false, parallel);
        type Edit = fn(&mut Engine<TestWorkbook>);
        let edits: Vec<(&str, Edit)> = vec![
            ("first eval", |_| {}),
            ("table value", |e| {
                e.set_cell_value("Assumptions", 3, 2, LiteralValue::Number(-4.0))
                    .unwrap();
            }),
            ("header key", |e| {
                e.set_cell_value("Sheet1", 1, 4, LiteralValue::Text("North".into()))
                    .unwrap();
            }),
            ("key becomes a number", |e| {
                e.set_cell_value("Sheet1", 1, 3, LiteralValue::Number(3.0))
                    .unwrap();
            }),
            ("numeric table", |e| {
                e.set_cell_value("Assumptions", 3, 4, LiteralValue::Number(99.0))
                    .unwrap();
            }),
            ("data", |e| {
                e.set_cell_value("Sheet1", 50, 1, LiteralValue::Text("x".into()))
                    .unwrap();
            }),
        ];
        for (label, edit) in edits {
            edit(&mut a);
            edit(&mut b);
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            for r in 1..=ROWS {
                for c in 1..=7 {
                    assert_eq!(
                        key(a.get_cell_value("Sheet1", r, c)),
                        key(b.get_cell_value("Sheet1", r, c)),
                        "parallel={parallel} {label}: R{r}C{c}"
                    );
                }
            }
        }
        assert!(a.lifted_members_for_test() >= u64::from(ROWS));
        assert_eq!(b.lifted_members_for_test(), 0);
        assert!(
            a.memo_hits_for_test() > u64::from(ROWS) / 2,
            "general memo hits"
        );
    }
}
