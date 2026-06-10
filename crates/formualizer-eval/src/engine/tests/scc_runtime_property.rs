//! Stage 2b — property-test oracle for `CycleDetection::Runtime` (RFC #112).
//!
//! Strategy
//! --------
//! The hand-written cases in `scc_runtime_cycles.rs` pin individual spec rows.
//! This module attacks the same contract from the other side: thousands of
//! *random* guarded workbooks evaluated by the real engine are cross-checked
//! against an independent, deliberately-dumb **reference lazy interpreter**.
//!
//! The reference interpreter is the entire value of this test, so it is kept
//! obviously correct rather than clever. It runs in two explicit phases over
//! a parsed formula map (cells parsed with formualizer's own `parse`, so the
//! AST shape matches the engine exactly):
//!
//!   * **Phase 1 — membership.** For each cell, walk *live* edges from it with
//!     an explicit on-stack set; the cell is a live-cycle member iff the walk
//!     re-enters it. Liveness is real short-circuit: `IF` descends only the
//!     taken arm (an edge in an untaken branch is never traversed — exactly the
//!     spec's "live edge", §1/§5), while `SUM` over an explicit range reads
//!     *every* cell in the region. This member set is the engine's
//!     live-cycle-member set, which it stamps `#CIRC` structurally.
//!   * **Phase 2 — values.** Evaluate each cell lazily; members resolve to
//!     `#CIRC` directly (mirroring the structural stamp) and every other cell
//!     reads them as a settled `#CIRC` value, propagating it through
//!     arithmetic/comparison exactly as the engine's post-stamp re-evaluation
//!     does.
//!
//! The two phases agree on branch selection (shared guard evaluation), so the
//! oracle and the engine must agree cell-for-cell:
//!
//!   * where the oracle yields a value → the engine must yield that value;
//!   * where the oracle marks a cell a live-cycle member → the engine must
//!     stamp `#CIRC` there, and vice-versa.
//!
//! The generated subset is intentionally narrow (numbers, `+ - *`,
//! comparisons, `IF`, `NOT`, `SUM` over explicit ranges, boolean/number
//! guards) so coercion is trivial and the oracle is auditable by eye. No
//! division or text is generated, so the only errors that can ever surface are
//! `#CIRC` (the cycle verdict) and the `#VALUE!` the engine's `IF`/`NOT`
//! produce when an error reaches a *condition* — a documented, pre-#112 error
//! rule the oracle reproduces faithfully (see the KNOWN ENGINE QUIRK notes) so
//! the property stays sharp on cycle classification.

use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType, parse};

/* ════════════════════════════ engine harness ════════════════════════════ */

fn runtime_cfg() -> EvalConfig {
    EvalConfig::default()
        .with_cycle(CycleConfig {
            detection: CycleDetection::Runtime,
            policy: CyclePolicy::Error,
        })
        .with_virtual_dep_telemetry(true)
}

/* ════════════════════════ deterministic PRNG (xorshift64*) ═══════════════ */
//
// No clock-seeded randomness anywhere: every case is reproducible from its
// integer seed alone.

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Splitmix the seed once so adjacent seeds don't produce correlated
        // streams.
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Rng((z ^ (z >> 31)) | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u32) -> u32 {
        (self.next_u64() % n as u64) as u32
    }
    fn chance(&mut self, num: u32, den: u32) -> bool {
        self.below(den) < num
    }
}

/* ════════════════════════════ cell model ════════════════════════════════ */
//
// A workbook is a column of cells A1..A{n} on Sheet1. Each cell is either a
// boolean/number *value* (used mainly as a guard or DAG leaf) or a *formula*
// string in the generated subset. Keeping everything in one column keeps the
// member-order reasoning trivial and the failure dumps short.

#[derive(Clone, Debug)]
enum Cell {
    Value(LiteralValue),
    Formula(String),
}

struct Workbook {
    seed: u64,
    cells: Vec<Cell>, // index i ⇒ A{i+1}
}

impl Workbook {
    fn n(&self) -> usize {
        self.cells.len()
    }
    fn a(i: usize) -> String {
        format!("A{}", i + 1)
    }
}

/* ════════════════════════════ generator ═════════════════════════════════ */
//
// Shape mix per cell (after the leaf prefix):
//   * leaf value (number or boolean guard)
//   * DAG arithmetic over *earlier* cells (acyclic by construction)
//   * guarded mutual reference `=IF(g, lit, A{j})` where g is a guard cell —
//     j may point forward (potential back-edge) but the live edge only fires
//     when the guard selects it
//   * occasional genuine forward back-edge in a *non-guarded* arithmetic spot
//     (creates real cycles → the engine must produce #CIRC)
//   * small SUM over an explicit earlier range
//
// We never emit a *direct* self reference (`A1` inside A1) nor a dense range
// covering the cell: those are rejected at ingest in both modes (see the
// hand-written §7.1 tests) and are out of scope for the runtime-evaluation
// oracle.

fn gen_workbook(seed: u64) -> Workbook {
    let mut rng = Rng::new(seed);
    let n = 10 + rng.below(31) as usize; // 10..=40 cells
    let mut cells: Vec<Cell> = Vec::with_capacity(n);

    // A handful of guard cells up front so later IFs have stable guards to
    // read. Guards are booleans or small numbers (compared > 0).
    let n_guards = 2 + rng.below(3) as usize; // 2..=4 guards
    for _ in 0..n_guards {
        if rng.chance(1, 2) {
            cells.push(Cell::Value(LiteralValue::Boolean(rng.chance(1, 2))));
        } else {
            cells.push(Cell::Value(LiteralValue::Int(rng.below(3) as i64))); // 0,1,2
        }
    }
    // A couple of numeric leaves to anchor DAG arithmetic.
    for _ in 0..2 {
        cells.push(Cell::Value(LiteralValue::Int(1 + rng.below(9) as i64)));
    }

    while cells.len() < n {
        let i = cells.len();
        let f = gen_formula(&mut rng, i, n_guards, n);
        cells.push(Cell::Formula(f));
    }

    Workbook { seed, cells }
}

fn guard_ref(rng: &mut Rng, n_guards: usize) -> String {
    Workbook::a(rng.below(n_guards as u32) as usize)
}

/// Reference to some *earlier* non-guard cell (DAG-safe), or a guard.
fn earlier_ref(rng: &mut Rng, i: usize) -> String {
    // Earlier index in [0, i).
    Workbook::a(rng.below(i as u32) as usize)
}

/// Reference that may point forward (to a not-yet-defined cell), excluding the
/// cell itself — the source of potential cycles.
fn any_other_ref(rng: &mut Rng, i: usize, n: usize) -> String {
    loop {
        let j = rng.below(n as u32) as usize;
        if j != i {
            return Workbook::a(j);
        }
    }
}

fn gen_formula(rng: &mut Rng, i: usize, n_guards: usize, n: usize) -> String {
    // Weighted shape selection.
    match rng.below(100) {
        // 0..35: guarded mutual reference. Live edge fires only when guard
        // selects the ref arm. The ref may point forward (closing a static
        // SCC) but stays phantom unless the guard routes into it.
        0..=34 => {
            let g = guard_ref(rng, n_guards);
            let lit = 1 + rng.below(50);
            let other = any_other_ref(rng, i, n);
            if rng.chance(1, 2) {
                // ref in the FALSE arm
                format!("=IF({g},{lit},{other})")
            } else {
                // ref in the TRUE arm
                format!("=IF({g},{other},{lit})")
            }
        }
        // 35..55: guarded reference to an *earlier* cell only (always a DAG
        // edge, can never cycle) — exercises phantom/value paths densely.
        35..=54 => {
            let g = guard_ref(rng, n_guards);
            let lit = 1 + rng.below(50);
            if i == 0 {
                format!("={lit}")
            } else {
                let e = earlier_ref(rng, i);
                format!("=IF({g},{e},{lit})")
            }
        }
        // 55..75: DAG arithmetic over earlier cells (acyclic).
        55..=74 => {
            if i == 0 {
                format!("={}", 1 + rng.below(50))
            } else {
                let a = earlier_ref(rng, i);
                let b = earlier_ref(rng, i);
                let op = ["+", "-", "*"][rng.below(3) as usize];
                format!("={a}{op}{b}")
            }
        }
        // 75..85: comparison feeding an IF (guard built inline from a cell).
        75..=84 => {
            let lit = 1 + rng.below(20);
            let other = any_other_ref(rng, i, n);
            if i == 0 {
                format!("={lit}")
            } else {
                let e = earlier_ref(rng, i);
                let cmp = [">", "<", "="][rng.below(3) as usize];
                // NOT() wrapper sometimes, to exercise the guard subset.
                if rng.chance(1, 3) {
                    format!("=IF(NOT({e}{cmp}{lit}),{lit},{other})")
                } else {
                    format!("=IF({e}{cmp}{lit},{lit},{other})")
                }
            }
        }
        // 85..93: small SUM over an explicit earlier range (always-live read;
        // no short-circuit). Range stays strictly earlier ⇒ DAG.
        85..=92 => {
            if i < 3 {
                format!("={}", 1 + rng.below(50))
            } else {
                // Range A{lo+1}:A{hi+1} with hi < i.
                let hi = rng.below(i as u32) as usize;
                let lo = rng.below((hi + 1) as u32) as usize;
                format!("=SUM({}:{})", Workbook::a(lo), Workbook::a(hi))
            }
        }
        // 93..100: occasional GENUINE cycle — unguarded arithmetic that reads
        // a (possibly forward) cell. When the forward cell eventually reads
        // back, this is a live cycle the engine must mark #CIRC.
        _ => {
            let other = any_other_ref(rng, i, n);
            let lit = 1 + rng.below(9);
            let op = ["+", "*"][rng.below(2) as usize];
            format!("={other}{op}{lit}")
        }
    }
}

/* ════════════════════════ reference lazy interpreter ════════════════════ */
//
// The oracle. Parses each cell once (reusing formualizer's own parser so the
// AST shape matches the engine exactly), then evaluates a cell on demand with
// memoization and an explicit in-progress stack. Re-entry ⇒ #CIRC.

/// Error states the generated subset can produce. The cycle verdict comes in
/// two flavors that the engine treats differently downstream:
///
///   * `Circ` — an *active re-entry*: the cell currently being evaluated reads
///     back into itself along live edges. This is the engine's structural
///     "this cell is a live-cycle member" verdict and is stamped `#CIRC`
///     regardless of how the value is later consumed (an `IF` *guard* that
///     re-enters the cell still makes the cell a member — spec §7.3).
///   * `CircSettled` — a `#CIRC` value *read from an already-stamped member*
///     by a cell that is itself **not** a member. It still propagates as
///     `#CIRC` through arithmetic/comparison, but when fed into an `IF`/`NOT`
///     *condition* the engine's coercion turns it into `#VALUE!` (see the
///     KNOWN ENGINE QUIRK note on `eval_node`). Both map to the engine's
///     `#CIRC` *only* when they survive to a cell's final value; the split
///     exists purely to reproduce the IF-condition coercion faithfully.
///   * `Value` — the `#VALUE!` produced by that coercion.
///
/// Keeping this split is what lets the oracle stay a faithful mirror of the
/// engine's *documented* error handling while remaining sharp on the cycle
/// classification that is the actual subject of the test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EKind {
    Circ,
    CircSettled,
    Value,
}

impl EKind {
    /// Both circular flavors collapse to the engine's `#CIRC` cell value.
    fn is_circ(self) -> bool {
        matches!(self, EKind::Circ | EKind::CircSettled)
    }
}

/// Oracle value: a fully-evaluated scalar, or an error verdict.
#[derive(Clone, Debug, PartialEq)]
enum OVal {
    Num(f64),
    Bool(bool),
    Empty,
    Err(EKind),
}

impl OVal {
    fn is_err(&self) -> bool {
        matches!(self, OVal::Err(_))
    }
}

struct Oracle {
    asts: Vec<Option<ASTNode>>, // None for value cells
    values: Vec<LiteralValue>,  // literal value cells (and Empty otherwise)
    /// Globally-computed live-cycle membership (phase 1). A cell is a member
    /// iff a demand walk rooted at it, following only *live* (taken) edges,
    /// re-enters the root. This is the engine's live-cycle-member set, which
    /// it stamps `#CIRC` structurally during the SCC task.
    member: Vec<bool>,
    /// Per-root transient memo for the value phase. Scoped to a single demand
    /// root so a cell's value can never be poisoned by the re-entry context of
    /// an unrelated root (lazy cycle evaluation is inherently root-relative).
    memo: Vec<Option<OVal>>,
    /// Cells currently on the active evaluation stack of the *membership* walk.
    on_stack: Vec<bool>,
    /// The membership-walk root (re-entry into it ⇒ that root is a member).
    member_root: usize,
    member_hit: bool,
    n: usize,
}

impl Oracle {
    fn new(wb: &Workbook) -> Self {
        let n = wb.n();
        let mut asts = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for c in &wb.cells {
            match c {
                Cell::Value(v) => {
                    asts.push(None);
                    values.push(v.clone());
                }
                Cell::Formula(f) => {
                    asts.push(Some(parse(f).expect("oracle parse")));
                    values.push(LiteralValue::Empty);
                }
            }
        }
        let mut o = Oracle {
            asts,
            values,
            member: vec![false; n],
            memo: vec![None; n],
            on_stack: vec![false; n],
            member_root: 0,
            member_hit: false,
            n,
        };
        o.compute_membership();
        o
    }

    /// 1-based `(col, row)` from the parser → 0-based cell index in our single
    /// column. Returns None for any reference outside column A (col 1) or past
    /// the last generated row — the engine reads those as Empty/0.
    fn idx_of(&self, col: u32, row: u32) -> Option<usize> {
        // formualizer's parser yields 1-based row/col; our workbooks live in
        // column A (col == 1) only.
        if col != 1 || row == 0 {
            return None;
        }
        let i = (row - 1) as usize;
        if i < self.n { Some(i) } else { None }
    }

    /* ── phase 1: live-cycle membership ──────────────────────────────────── */

    fn compute_membership(&mut self) {
        for root in 0..self.n {
            for s in &mut self.on_stack {
                *s = false;
            }
            self.member_root = root;
            self.member_hit = false;
            self.member_walk(root);
            if self.member_hit {
                self.member[root] = true;
            }
        }
    }

    /// Walk live edges from `i`, setting `member_hit` if we re-enter the root.
    /// Pure liveness exploration: IF only descends into the taken branch
    /// (true short-circuit) using `live_branch`, SUM descends into every range
    /// cell. Returns nothing; the only output is `member_hit`.
    fn member_walk(&mut self, i: usize) {
        if i == self.member_root && self.on_stack[i] {
            self.member_hit = true;
            return;
        }
        if self.on_stack[i] {
            return; // a different cell's cycle — not our root's membership
        }
        if self.member_hit {
            return; // short-circuit: answer already known
        }
        let Some(ast) = self.asts[i].clone() else {
            return; // value cell: no outgoing edges
        };
        self.on_stack[i] = true;
        self.walk_node(&ast);
        self.on_stack[i] = false;
    }

    fn walk_node(&mut self, node: &ASTNode) {
        if self.member_hit {
            return;
        }
        match &node.node_type {
            ASTNodeType::Literal(_) => {}
            ASTNodeType::Reference { reference, .. } => self.walk_ref(reference),
            ASTNodeType::UnaryOp { expr, .. } => self.walk_node(expr),
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.walk_node(left);
                self.walk_node(right);
            }
            ASTNodeType::Function { name, args } => match name.to_ascii_uppercase().as_str() {
                "IF" => {
                    // Descend the guard (always read) and *only the live arm*.
                    self.walk_node(&args[0]);
                    match self.live_branch(&args[0]) {
                        Some(true) => self.walk_node(&args[1]),
                        Some(false) => {
                            if args.len() > 2 {
                                self.walk_node(&args[2]);
                            }
                        }
                        // Guard is itself circular/uncomputable: conservatively
                        // descend neither arm — the guard read alone already
                        // closes any live cycle that runs through the guard.
                        None => {}
                    }
                }
                "NOT" => self.walk_node(&args[0]),
                "SUM" => {
                    for arg in args {
                        self.walk_sum_arg(arg);
                    }
                }
                other => panic!("walk: unsupported function {other}"),
            },
            other => panic!("walk: unsupported node {other:?}"),
        }
    }

    fn walk_sum_arg(&mut self, arg: &ASTNode) {
        if let ASTNodeType::Reference {
            reference: ReferenceType::Range {
                start_row, end_row, ..
            },
            ..
        } = &arg.node_type
        {
            let (sr, er) = (start_row.unwrap(), end_row.unwrap());
            for r in sr..=er {
                if let Some(i) = self.idx_of(1, r) {
                    self.member_walk(i);
                }
            }
        } else {
            self.walk_node(arg);
        }
    }

    fn walk_ref(&mut self, reference: &ReferenceType) {
        if let ReferenceType::Cell { row, col, .. } = reference
            && let Some(i) = self.idx_of(*col, *row)
        {
            self.member_walk(i);
        }
    }

    /// Liveness of an IF guard during the membership walk. Evaluates the guard
    /// with `GuardEval` (the same arithmetic / short-circuit / error rules as
    /// the value phase, so arm selection is identical in both phases). A guard
    /// that resolves to a clean bool/number selects an arm; an error/circular
    /// guard returns None ⇒ descend neither arm (the guard's own reads are
    /// already walked, so any cycle through the guard is still detected).
    fn live_branch(&mut self, guard: &ASTNode) -> Option<bool> {
        match self.eval_guard(guard) {
            OVal::Bool(b) => Some(b),
            OVal::Num(n) => Some(n != 0.0),
            OVal::Empty => Some(false),
            OVal::Err(_) => None,
        }
    }

    /// Evaluate a guard expression for liveness, independent of the (not-yet
    /// finalized) member set. Guards in the generated subset only read leaf
    /// guard cells or strictly-earlier cells, so `GuardEval`'s plain lazy eval
    /// with re-entry detection always terminates and yields the same arm choice
    /// the value phase would.
    fn eval_guard(&mut self, guard: &ASTNode) -> OVal {
        let mut g = GuardEval {
            asts: &self.asts,
            values: &self.values,
            on_stack: vec![false; self.n],
            n: self.n,
        };
        g.eval_node(guard)
    }

    /* ── phase 2: values (members read as #CIRC) ─────────────────────────── */

    /// Demand the final value of cell `i` from a clean memo. Member cells
    /// resolve to `#CIRC` directly (matching the engine's structural stamp);
    /// every other cell evaluates lazily, reading members as `CircSettled`.
    fn value_of(&mut self, i: usize) -> OVal {
        for m in &mut self.memo {
            *m = None;
        }
        self.eval_cell(i)
    }

    fn eval_cell(&mut self, i: usize) -> OVal {
        // A live-cycle member is stamped #CIRC by the engine regardless of how
        // it is consumed; readers see it as a settled #CIRC value.
        if self.member[i] {
            return OVal::Err(EKind::CircSettled);
        }
        if let Some(v) = &self.memo[i] {
            return v.clone();
        }
        let result = match &self.asts[i] {
            None => lit_to_oval(&self.values[i]),
            Some(ast) => {
                let ast = ast.clone();
                self.eval_node(&ast)
            }
        };
        self.memo[i] = Some(result.clone());
        result
    }

    fn eval_node(&mut self, node: &ASTNode) -> OVal {
        match &node.node_type {
            ASTNodeType::Literal(v) => lit_to_oval(v),
            ASTNodeType::Reference { reference, .. } => self.eval_ref(reference),
            ASTNodeType::UnaryOp { op, expr } => {
                let v = self.eval_node(expr);
                if v.is_err() {
                    return v; // error propagates unchanged
                }
                match op.as_str() {
                    "-" => OVal::Num(-as_num(&v)),
                    "+" => OVal::Num(as_num(&v)),
                    other => panic!("oracle: unsupported unary op {other}"),
                }
            }
            ASTNodeType::BinaryOp { op, left, right } => {
                // Arithmetic and comparison always evaluate BOTH operands
                // (no short-circuit) — both reads are live. The leftmost
                // error propagates (matches the engine's operator handling,
                // incl. `compare`, which returns the first error operand).
                let l = self.eval_node(left);
                if l.is_err() {
                    return l;
                }
                let r = self.eval_node(right);
                if r.is_err() {
                    return r;
                }
                let (a, b) = (as_num(&l), as_num(&r));
                match op.as_str() {
                    "+" => OVal::Num(a + b),
                    "-" => OVal::Num(a - b),
                    "*" => OVal::Num(a * b),
                    ">" => OVal::Bool(a > b),
                    "<" => OVal::Bool(a < b),
                    "=" => OVal::Bool(a == b),
                    other => panic!("oracle: unsupported binary op {other}"),
                }
            }
            ASTNodeType::Function { name, args } => {
                let upper = name.to_ascii_uppercase();
                match upper.as_str() {
                    "IF" => {
                        // TRUE short-circuit: only the taken arm is evaluated.
                        let cond = self.eval_node(&args[0]);
                        if let OVal::Err(e) = cond {
                            return condition_error(e);
                        }
                        if as_bool(&cond) {
                            self.eval_node(&args[1])
                        } else if args.len() > 2 {
                            self.eval_node(&args[2])
                        } else {
                            OVal::Bool(false) // IF with no else ⇒ FALSE
                        }
                    }
                    "NOT" => {
                        let v = self.eval_node(&args[0]);
                        if let OVal::Err(e) = v {
                            return condition_error(e);
                        }
                        OVal::Bool(!as_bool(&v))
                    }
                    "SUM" => {
                        let mut acc = 0.0;
                        for arg in args {
                            match self.sum_arg(arg) {
                                Ok(x) => acc += x,
                                Err(e) => return OVal::Err(e),
                            }
                        }
                        OVal::Num(acc)
                    }
                    other => panic!("oracle: unsupported function {other}"),
                }
            }
            other => panic!("oracle: unsupported node {other:?}"),
        }
    }

    /// Sum a single SUM argument (scalar or explicit range). Returns Err(kind)
    /// if any read yields an error (SUM propagates the first error it sees).
    fn sum_arg(&mut self, arg: &ASTNode) -> Result<f64, EKind> {
        match &arg.node_type {
            ASTNodeType::Reference { reference, .. } => match reference {
                ReferenceType::Range {
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    ..
                } => {
                    // Generated ranges are always bounded and in column A
                    // (1-based start_col == end_col == 1).
                    let (sr, er) = (start_row.unwrap(), end_row.unwrap());
                    let col = start_col.unwrap_or(1);
                    debug_assert_eq!(col, end_col.unwrap_or(1));
                    let mut acc = 0.0;
                    for r in sr..=er {
                        if let Some(i) = self.idx_of(col, r) {
                            match self.eval_cell(i) {
                                OVal::Err(e) => return Err(e),
                                // Excel reference semantics: SUM ignores logical
                                // and empty cells *inside a range reference*
                                // (they only coerce when passed as direct scalar
                                // args). The generator never puts text in cells,
                                // so numbers are the only summed kind. This is a
                                // documented Excel rule, not a cycle behavior.
                                OVal::Num(x) => acc += x,
                                OVal::Bool(_) | OVal::Empty => {}
                            }
                        }
                    }
                    Ok(acc)
                }
                _ => match self.eval_ref(reference) {
                    OVal::Err(e) => Err(e),
                    v => Ok(as_num(&v)),
                },
            },
            _ => match self.eval_node(arg) {
                OVal::Err(e) => Err(e),
                v => Ok(as_num(&v)),
            },
        }
    }

    fn eval_ref(&mut self, reference: &ReferenceType) -> OVal {
        match reference {
            ReferenceType::Cell { row, col, .. } => match self.idx_of(*col, *row) {
                Some(i) => self.eval_cell(i),
                None => OVal::Empty, // off-sheet ⇒ empty/0
            },
            other => panic!("oracle: unexpected scalar reference {other:?}"),
        }
    }
}

/// A minimal, self-contained lazy evaluator used ONLY to resolve IF-guard
/// truth during the membership pre-pass (when the global member set isn't
/// finalized yet). In the generated subset, guards only ever read leaf guard
/// cells or *strictly earlier* cells (see the generator), so guards are always
/// acyclic and this evaluator never actually recurses into a cycle; the
/// re-entry guard is purely defensive. It applies the exact same arithmetic /
/// short-circuit / error rules as the value phase (sharing `as_num`/`as_bool`/
/// `condition_error`) so a guard's truth is identical in both phases.
struct GuardEval<'a> {
    asts: &'a [Option<ASTNode>],
    values: &'a [LiteralValue],
    on_stack: Vec<bool>,
    n: usize,
}

impl GuardEval<'_> {
    fn idx_of(&self, col: u32, row: u32) -> Option<usize> {
        if col != 1 || row == 0 {
            return None;
        }
        let i = (row - 1) as usize;
        if i < self.n { Some(i) } else { None }
    }

    fn eval_cell(&mut self, i: usize) -> OVal {
        if self.on_stack[i] {
            return OVal::Err(EKind::Circ);
        }
        self.on_stack[i] = true;
        let v = match &self.asts[i] {
            None => lit_to_oval(&self.values[i]),
            Some(ast) => {
                let ast = ast.clone();
                self.eval_node(&ast)
            }
        };
        self.on_stack[i] = false;
        v
    }

    fn eval_node(&mut self, node: &ASTNode) -> OVal {
        match &node.node_type {
            ASTNodeType::Literal(v) => lit_to_oval(v),
            ASTNodeType::Reference { reference, .. } => match reference {
                ReferenceType::Cell { row, col, .. } => match self.idx_of(*col, *row) {
                    Some(i) => self.eval_cell(i),
                    None => OVal::Empty,
                },
                _ => OVal::Empty, // a range used as a scalar guard never happens
            },
            ASTNodeType::UnaryOp { op, expr } => {
                let v = self.eval_node(expr);
                if v.is_err() {
                    return v;
                }
                match op.as_str() {
                    "-" => OVal::Num(-as_num(&v)),
                    "+" => OVal::Num(as_num(&v)),
                    other => panic!("guard: unsupported unary op {other}"),
                }
            }
            ASTNodeType::BinaryOp { op, left, right } => {
                let l = self.eval_node(left);
                if l.is_err() {
                    return l;
                }
                let r = self.eval_node(right);
                if r.is_err() {
                    return r;
                }
                let (a, b) = (as_num(&l), as_num(&r));
                match op.as_str() {
                    "+" => OVal::Num(a + b),
                    "-" => OVal::Num(a - b),
                    "*" => OVal::Num(a * b),
                    ">" => OVal::Bool(a > b),
                    "<" => OVal::Bool(a < b),
                    "=" => OVal::Bool(a == b),
                    other => panic!("guard: unsupported binary op {other}"),
                }
            }
            ASTNodeType::Function { name, args } => match name.to_ascii_uppercase().as_str() {
                "IF" => {
                    let cond = self.eval_node(&args[0]);
                    if let OVal::Err(e) = cond {
                        return condition_error(e);
                    }
                    if as_bool(&cond) {
                        self.eval_node(&args[1])
                    } else if args.len() > 2 {
                        self.eval_node(&args[2])
                    } else {
                        OVal::Bool(false)
                    }
                }
                "NOT" => {
                    let v = self.eval_node(&args[0]);
                    if let OVal::Err(e) = v {
                        return condition_error(e);
                    }
                    OVal::Bool(!as_bool(&v))
                }
                "SUM" => {
                    let mut acc = 0.0;
                    for arg in args {
                        match self.sum_arg(arg) {
                            Ok(x) => acc += x,
                            Err(e) => return OVal::Err(e),
                        }
                    }
                    OVal::Num(acc)
                }
                other => panic!("guard: unsupported function {other}"),
            },
            other => panic!("guard: unsupported node {other:?}"),
        }
    }

    fn sum_arg(&mut self, arg: &ASTNode) -> Result<f64, EKind> {
        if let ASTNodeType::Reference {
            reference: ReferenceType::Range {
                start_row, end_row, ..
            },
            ..
        } = &arg.node_type
        {
            let (sr, er) = (start_row.unwrap(), end_row.unwrap());
            let mut acc = 0.0;
            for r in sr..=er {
                if let Some(i) = self.idx_of(1, r) {
                    match self.eval_cell(i) {
                        OVal::Err(e) => return Err(e),
                        OVal::Num(x) => acc += x,
                        OVal::Bool(_) | OVal::Empty => {}
                    }
                }
            }
            Ok(acc)
        } else {
            match self.eval_node(arg) {
                OVal::Err(e) => Err(e),
                v => Ok(as_num(&v)),
            }
        }
    }
}

/// IF/NOT condition coercion of an error operand.
///
/// KNOWN ENGINE QUIRK (pre-#112, general error handling — NOT a cycle bug):
/// `IfFn::eval`/`NotFn` coerce a *non*-bool/*non*-number condition to
/// `#VALUE!` rather than propagating the condition's own error (the `IfFn`
/// docs even state "non-numeric/non-boolean conditions return #VALUE!").
///
/// The one case the engine does NOT reach this coercion for is a cell that is
/// itself a live-cycle member: such a cell is stamped `#CIRC` structurally
/// during the SCC task, before any IF body runs. The oracle mirrors that by
/// keeping an *active* re-entry (`Circ`) circular even when it flows through a
/// guard (spec §7.3), and only coercing a `#CIRC` that was *read from another,
/// already-stamped member* (`CircSettled`) — or any other error — to `#VALUE!`.
fn condition_error(e: EKind) -> OVal {
    match e {
        EKind::Circ => OVal::Err(EKind::Circ), // member: stays #CIRC
        EKind::CircSettled | EKind::Value => OVal::Err(EKind::Value),
    }
}

fn lit_to_oval(v: &LiteralValue) -> OVal {
    match v {
        LiteralValue::Int(i) => OVal::Num(*i as f64),
        LiteralValue::Number(n) => OVal::Num(*n),
        LiteralValue::Boolean(b) => OVal::Bool(*b),
        LiteralValue::Empty => OVal::Empty,
        other => panic!("oracle: unexpected literal {other:?}"),
    }
}

fn as_num(v: &OVal) -> f64 {
    match v {
        OVal::Num(n) => *n,
        OVal::Bool(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        OVal::Empty => 0.0,
        OVal::Err(_) => panic!("as_num on error value"),
    }
}

fn as_bool(v: &OVal) -> bool {
    match v {
        OVal::Bool(b) => *b,
        OVal::Num(n) => *n != 0.0,
        OVal::Empty => false,
        OVal::Err(_) => panic!("as_bool on error value"),
    }
}

/* ════════════════════════════ comparison ════════════════════════════════ */

/// Project an engine cell value onto the oracle's value space for comparison.
/// Returns Err with a description if the engine produced something the oracle
/// can't model (any non-#CIRC error, text, array …) — that itself is a
/// reportable discrepancy because the generator never emits such constructs.
fn engine_to_oval(v: &Option<LiteralValue>) -> Result<OVal, String> {
    match v {
        None | Some(LiteralValue::Empty) => Ok(OVal::Empty),
        Some(LiteralValue::Int(i)) => Ok(OVal::Num(*i as f64)),
        Some(LiteralValue::Number(n)) => Ok(OVal::Num(*n)),
        Some(LiteralValue::Boolean(b)) => Ok(OVal::Bool(*b)),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Circ => {
            Ok(OVal::Err(EKind::Circ))
        }
        // The only other error the generated subset can yield is the
        // documented IF/NOT condition-coercion `#VALUE!` (see the KNOWN ENGINE
        // QUIRK note). Any *other* error kind would be a genuine surprise and
        // is surfaced as an un-modelable value (test failure).
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Value => {
            Ok(OVal::Err(EKind::Value))
        }
        Some(other) => Err(format!("engine produced un-modelable value {other:?}")),
    }
}

/// Numeric equality with a tiny tolerance (the generated subset is exact
/// integer arithmetic, but `*`/chains can produce large magnitudes; compare
/// with a relative epsilon to be safe against f64 association).
fn approx_eq(a: f64, b: f64) -> bool {
    if a == b {
        return true;
    }
    let diff = (a - b).abs();
    let scale = a.abs().max(b.abs()).max(1.0);
    diff <= 1e-9 * scale
}

fn ovals_match(oracle: &OVal, engine: &OVal) -> bool {
    match (oracle, engine) {
        // Both #CIRC flavors collapse to the engine's single #CIRC value.
        (OVal::Err(a), OVal::Err(b)) if a.is_circ() && b.is_circ() => true,
        (OVal::Err(a), OVal::Err(b)) => a == b,
        (OVal::Num(a), OVal::Num(b)) => approx_eq(*a, *b),
        // The engine reports booleans as Boolean; the oracle distinguishes
        // Bool/Num but a guard cell read in numeric context can surface either
        // — accept numeric/bool cross-equality on 0/1.
        (OVal::Bool(a), OVal::Bool(b)) => a == b,
        (OVal::Bool(a), OVal::Num(b)) | (OVal::Num(b), OVal::Bool(a)) => {
            approx_eq(if *a { 1.0 } else { 0.0 }, *b)
        }
        (OVal::Empty, OVal::Empty) => true,
        (OVal::Empty, OVal::Num(n)) | (OVal::Num(n), OVal::Empty) => *n == 0.0,
        _ => false,
    }
}

/* ════════════════════════════ the property ══════════════════════════════ */

fn build_engine(wb: &Workbook) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), runtime_cfg());
    for (i, cell) in wb.cells.iter().enumerate() {
        let row = (i + 1) as u32;
        match cell {
            Cell::Value(v) => engine
                .set_cell_value("Sheet1", row, 1, v.clone())
                .expect("set value"),
            Cell::Formula(f) => {
                // A direct self-reference / cover would be rejected at ingest;
                // the generator never produces one, but guard anyway so a
                // generator bug surfaces as a clear panic, not a false pass.
                engine
                    .set_cell_formula("Sheet1", row, 1, parse(f).expect("parse"))
                    .unwrap_or_else(|e| {
                        panic!(
                            "seed {} A{row} formula {f:?} rejected at ingest: {e:?}\n{}",
                            wb.seed,
                            dump(wb)
                        )
                    });
            }
        }
    }
    engine
}

fn dump(wb: &Workbook) -> String {
    let mut s = format!("── workbook dump (seed {}) ──\n", wb.seed);
    for (i, c) in wb.cells.iter().enumerate() {
        let body = match c {
            Cell::Value(v) => format!("{v:?}"),
            Cell::Formula(f) => f.clone(),
        };
        s.push_str(&format!("  A{} = {}\n", i + 1, body));
    }
    s
}

/// Run one seed; returns a discrepancy description on mismatch.
fn check_seed(seed: u64) -> Result<(), String> {
    let wb = gen_workbook(seed);

    // Engine side.
    let mut engine = build_engine(&wb);
    engine
        .evaluate_all()
        .map_err(|e| format!("seed {seed}: evaluate_all errored: {e:?}\n{}", dump(&wb)))?;

    // Oracle side (independent fresh state).
    let mut oracle = Oracle::new(&wb);

    for i in 0..wb.n() {
        let row = (i + 1) as u32;
        let engine_raw = engine.get_cell_value("Sheet1", row, 1);
        let engine_oval = engine_to_oval(&engine_raw).map_err(|why| {
            format!(
                "seed {seed}: A{row}: {why} (oracle subset only emits numbers/bools/#CIRC)\n{}",
                dump(&wb)
            )
        })?;
        let oracle_oval = oracle.value_of(i);

        if !ovals_match(&oracle_oval, &engine_oval) {
            return Err(format!(
                "DISCREPANCY at seed {seed}, cell A{row}:\n  \
                 oracle = {oracle_oval:?}\n  engine = {engine_oval:?} (raw {engine_raw:?})\n{}",
                dump(&wb)
            ));
        }
    }
    Ok(())
}

/// Sanity self-check: the oracle must agree with the hand-written §7.2 guarded
/// pair (values) and §7.5 genuine cycle (#CIRC) on tiny fixed workbooks, so a
/// silently-broken oracle (e.g. one that never returns Circ) can't make the
/// whole property suite vacuously pass.
#[test]
fn oracle_self_check_known_shapes() {
    // §7.2 phantom guarded pair, guard TRUE ⇒ both 555.
    let wb = Workbook {
        seed: 0,
        cells: vec![
            Cell::Value(LiteralValue::Boolean(true)), // A1 guard
            Cell::Formula("=IF(A1,555,A3)".into()),   // A2
            Cell::Formula("=IF(A1,A2,999)".into()),   // A3
        ],
    };
    let mut o = Oracle::new(&wb);
    assert_eq!(o.value_of(1), OVal::Num(555.0));
    assert_eq!(o.value_of(2), OVal::Num(555.0));

    // Guard FALSE ⇒ both 999.
    let wb = Workbook {
        seed: 0,
        cells: vec![
            Cell::Value(LiteralValue::Boolean(false)),
            Cell::Formula("=IF(A1,555,A3)".into()),
            Cell::Formula("=IF(A1,A2,999)".into()),
        ],
    };
    let mut o = Oracle::new(&wb);
    assert_eq!(o.value_of(1), OVal::Num(999.0));
    assert_eq!(o.value_of(2), OVal::Num(999.0));

    // §7.5 genuine 2-cycle ⇒ both #CIRC (CircSettled maps to engine #CIRC).
    let wb = Workbook {
        seed: 0,
        cells: vec![Cell::Formula("=A2+1".into()), Cell::Formula("=A1+1".into())],
    };
    let mut o = Oracle::new(&wb);
    assert!(matches!(o.value_of(0), OVal::Err(k) if k.is_circ()));
    assert!(matches!(o.value_of(1), OVal::Err(k) if k.is_circ()));
    assert!(o.member[0] && o.member[1], "both cells are cycle members");

    // §7.3 guard read that is itself a live edge into the cycle ⇒ #CIRC.
    // A1 = IF(A2>0, A2+1, 5), A2 = A1: A1's guard always reads A2, A2 reads
    // A1 — live cycle regardless of guard truth.
    let wb = Workbook {
        seed: 0,
        cells: vec![
            Cell::Formula("=IF(A2>0,A2+1,5)".into()),
            Cell::Formula("=A1".into()),
        ],
    };
    let mut o = Oracle::new(&wb);
    assert!(
        matches!(o.value_of(0), OVal::Err(k) if k.is_circ()),
        "A1 #CIRC"
    );
    assert!(
        matches!(o.value_of(1), OVal::Err(k) if k.is_circ()),
        "A2 #CIRC"
    );

    // SUM over an explicit range that includes a live-cycle member ⇒ #CIRC;
    // over a clean range ⇒ the sum.
    let wb = Workbook {
        seed: 0,
        cells: vec![
            Cell::Value(LiteralValue::Int(2)),
            Cell::Value(LiteralValue::Int(3)),
            Cell::Formula("=SUM(A1:A2)".into()),
        ],
    };
    let mut o = Oracle::new(&wb);
    assert_eq!(o.value_of(2), OVal::Num(5.0));

    // Downstream non-member reading a member through an IF condition ⇒ #VALUE!
    // (the documented IfFn coercion). A1↔A2 live cycle; A3 reads A1 via guard.
    let wb = Workbook {
        seed: 0,
        cells: vec![
            Cell::Formula("=A2+1".into()),
            Cell::Formula("=A1+1".into()),
            Cell::Formula("=IF(A1>0,7,8)".into()),
        ],
    };
    let mut o = Oracle::new(&wb);
    assert!(!o.member[2], "A3 is a downstream reader, not a member");
    assert_eq!(o.value_of(2), OVal::Err(EKind::Value));
}

/// Self-check that the engine harness round-trips the same known shapes, so
/// the comparison plumbing (engine_to_oval / ovals_match) is exercised on
/// values AND on #CIRC independently of the random corpus.
#[test]
fn harness_self_check_engine_known_shapes() {
    let wb = Workbook {
        seed: 0,
        cells: vec![
            Cell::Value(LiteralValue::Boolean(true)),
            Cell::Formula("=IF(A1,555,A3)".into()),
            Cell::Formula("=IF(A1,A2,999)".into()),
        ],
    };
    let mut engine = build_engine(&wb);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine_to_oval(&engine.get_cell_value("Sheet1", 2, 1)).unwrap(),
        OVal::Num(555.0)
    );

    let wb = Workbook {
        seed: 0,
        cells: vec![Cell::Formula("=A2+1".into()), Cell::Formula("=A1+1".into())],
    };
    let mut engine = build_engine(&wb);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine_to_oval(&engine.get_cell_value("Sheet1", 1, 1)).unwrap(),
        OVal::Err(EKind::Circ)
    );
}

#[test]
#[ignore]
fn debug_classify_all() {
    // Categorize every per-cell divergence across the corpus so we see the
    // full landscape, not just the first failure per seed.
    let mut classes: std::collections::BTreeMap<String, usize> = Default::default();
    for seed in 1..=500u64 {
        let wb = gen_workbook(seed);
        let mut engine = build_engine(&wb);
        engine.evaluate_all().unwrap();
        let mut oracle = Oracle::new(&wb);
        for i in 0..wb.n() {
            let row = (i + 1) as u32;
            let raw = engine.get_cell_value("Sheet1", row, 1);
            let o = oracle.value_of(i);
            let e = engine_to_oval(&raw);
            let mismatch = match &e {
                Ok(ev) => !ovals_match(&o, ev),
                Err(_) => true,
            };
            if mismatch {
                let key = match (&o, &raw) {
                    (OVal::Err(k), Some(LiteralValue::Error(er))) if k.is_circ() => {
                        format!("oracle=Circ engine=Error({:?})", er.kind)
                    }
                    (_, Some(LiteralValue::Error(er))) => {
                        format!("oracle=value engine=Error({:?})", er.kind)
                    }
                    _ => format!("oracle={o:?} engine={raw:?}"),
                };
                *classes.entry(key).or_default() += 1;
            }
        }
    }
    println!("── divergence classes ──");
    for (k, c) in &classes {
        println!("  {c:5}  {k}");
    }
}

#[test]
#[ignore]
fn debug_dump_seed() {
    let seed: u64 = std::env::var("SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(23);
    let wb = gen_workbook(seed);
    let mut engine = build_engine(&wb);
    let res = engine.evaluate_all().unwrap();
    let mut oracle = Oracle::new(&wb);
    println!("{}", dump(&wb));
    println!("cycle_errors={}", res.cycle_errors);
    for i in 0..wb.n() {
        let row = (i + 1) as u32;
        let e = engine.get_cell_value("Sheet1", row, 1);
        let o = oracle.value_of(i);
        println!(
            "A{row}: engine={e:?}  oracle={o:?} member={}",
            oracle.member[i]
        );
    }
}

/// The main property: 200 seeded random workbooks, engine vs oracle, cell for
/// cell. On failure the message is self-contained (seed + full formula map)
/// so a human can reproduce by hand.
#[test]
fn random_guarded_workbooks_match_reference_oracle() {
    let mut first_failure: Option<String> = None;
    let mut failures = 0usize;
    let n_seeds: u64 = 500;

    for seed in 1..=n_seeds {
        if let Err(msg) = check_seed(seed) {
            failures += 1;
            if first_failure.is_none() {
                first_failure = Some(msg);
            }
        }
    }

    if let Some(msg) = first_failure {
        // The message is self-contained: it carries the seed and the full
        // formula map (see `check_seed`/`dump`), so a human can reproduce the
        // exact workbook by hand or via `SEED=<n> debug_dump_seed`.
        panic!("{failures}/{n_seeds} seeds diverged. First failure:\n\n{msg}");
    }
}

/// Distribution probe (not an assertion of exact counts — just a guard that
/// the corpus actually contains a healthy mix of phantom-value workbooks and
/// genuine-#CIRC workbooks, so the property isn't trivially satisfied by an
/// all-DAG corpus). Printed with `--nocapture`.
#[test]
fn corpus_has_both_value_and_circ_workbooks() {
    let mut with_circ = 0usize;
    let mut all_values = 0usize;
    let mut total_cells = 0usize;
    let mut circ_cells = 0usize;

    for seed in 1..=200u64 {
        let wb = gen_workbook(seed);
        let oracle = Oracle::new(&wb);
        let mut any_circ = false;
        for i in 0..wb.n() {
            total_cells += 1;
            if oracle.member[i] {
                any_circ = true;
                circ_cells += 1;
            }
        }
        if any_circ {
            with_circ += 1;
        } else {
            all_values += 1;
        }
    }

    println!(
        "corpus: 200 seeds, {total_cells} cells; {with_circ} workbooks contain #CIRC, \
         {all_values} are all-values; {circ_cells} #CIRC cells total"
    );

    // Both classes must be non-empty or the property is under-exercised.
    assert!(
        with_circ > 0,
        "no #CIRC workbooks generated — corpus too tame"
    );
    assert!(
        all_values > 0,
        "every workbook had a #CIRC — corpus too cyclic"
    );
}
