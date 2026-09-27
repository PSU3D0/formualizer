//! Program 2 elementwise lift (P2-M3): a family run whose template is
//! operators over cell references and literals evaluates column-wise. The
//! template is compiled once per run; each referenced column segment is read
//! with its sheet resolved once; each operator node applies to all members
//! through the interpreter's own operator functions (`apply_unary_op`,
//! `apply_binary_op`), so values, errors and format annotations are the
//! AST walk's by construction. Evaluation order is the walk's: an operand
//! error wins over the other operand exactly as `?` would.
//!
//! Anything else (functions, ranges, arrays, `@`, `:`, bound literals, a
//! member without a cell) leaves the run to the per-member template walk.

use super::*;
use crate::engine::arena::{AstNodeData, CompactRefType, DataStore, SheetKey};
use crate::engine::scheduler::LayerRun;
use crate::format::FormatId;
use crate::interpreter::shift_axis_for_offset as shift_axis;
use crate::traits::CalcValue;
use arrow_array::Array as _;

/// Nodes of a compiled template (children precede parents).
enum LiftNode {
    Value(LiteralValue),
    Cell {
        sheet: Option<SheetKey>,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    },
    Unary {
        op: &'static str,
        child: usize,
    },
    Binary {
        op: &'static str,
        left: usize,
        right: usize,
    },
    /// The built-in `IF` with 2 or 3 arguments.
    If {
        cond: usize,
        then: usize,
        otherwise: Option<usize>,
    },
}

pub(super) struct LiftProgram {
    nodes: Vec<LiftNode>,
}

/// Programs larger than this are left to the walk (bounded compile work).
const MAX_LIFT_NODES: usize = 256;

fn static_binary(op: &str) -> Option<&'static str> {
    Some(match op {
        "+" => "+",
        "-" => "-",
        "*" => "*",
        "/" => "/",
        "^" => "^",
        "&" => "&",
        "=" => "=",
        "<>" => "<>",
        ">" => ">",
        "<" => "<",
        ">=" => ">=",
        "<=" => "<=",
        _ => return None,
    })
}

fn static_unary(op: &str) -> Option<&'static str> {
    Some(match op {
        "+" => "+",
        "-" => "-",
        "%" => "%",
        _ => return None,
    })
}

impl LiftProgram {
    /// Compile `template`, or `None` when it is not liftable.
    pub(super) fn compile(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        template: AstNodeId,
    ) -> Option<Self> {
        let mut program = Self { nodes: Vec::new() };
        program.compile_node(functions, ds, template)?;
        // A template without any reference is a constant family: the walk
        // is as cheap, keep it there.
        program
            .nodes
            .iter()
            .any(|n| matches!(n, LiftNode::Cell { .. }))
            .then_some(program)
    }

    fn compile_node(
        &mut self,
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        id: AstNodeId,
    ) -> Option<usize> {
        if self.nodes.len() >= MAX_LIFT_NODES {
            return None;
        }
        let node = match ds.get_node(id)? {
            AstNodeData::Literal(vref) => {
                let value = ds.retrieve_value(*vref);
                if matches!(value, LiteralValue::Array(_)) {
                    return None;
                }
                LiftNode::Value(value)
            }
            AstNodeData::Omitted => LiftNode::Value(LiteralValue::Number(0.0)),
            AstNodeData::Reference {
                ref_type:
                    CompactRefType::Cell {
                        sheet,
                        row,
                        col,
                        row_abs,
                        col_abs,
                    },
                ..
            } if *row > 0 && *col > 0 => LiftNode::Cell {
                sheet: *sheet,
                row: *row,
                col: *col,
                row_abs: *row_abs,
                col_abs: *col_abs,
            },
            AstNodeData::UnaryOp { op_id, expr_id } => {
                let op = static_unary(ds.resolve_ast_string(*op_id))?;
                let expr_id = *expr_id;
                let child = self.compile_node(functions, ds, expr_id)?;
                LiftNode::Unary { op, child }
            }
            AstNodeData::BinaryOp {
                op_id,
                left_id,
                right_id,
            } => {
                let op = static_binary(ds.resolve_ast_string(*op_id))?;
                let (left_id, right_id) = (*left_id, *right_id);
                let left = self.compile_node(functions, ds, left_id)?;
                let right = self.compile_node(functions, ds, right_id)?;
                LiftNode::Binary { op, left, right }
            }
            AstNodeData::Function { name_id, .. } => {
                // Only the built-in IF (an override keeps `family_kernel`
                // `None`), with the arities its `eval` accepts.
                let fun = functions.get_function("", ds.resolve_ast_string(*name_id))?;
                if fun.family_kernel() != Some(crate::function::FamilyKernel::If) {
                    return None;
                }
                let args = ds.get_args(id)?;
                if !(2..=3).contains(&args.len()) {
                    return None;
                }
                let args: smallvec::SmallVec<[AstNodeId; 3]> = args.iter().copied().collect();
                let cond = self.compile_node(functions, ds, args[0])?;
                let then = self.compile_node(functions, ds, args[1])?;
                let otherwise = match args.get(2) {
                    Some(&arg) => Some(self.compile_node(functions, ds, arg)?),
                    None => None,
                };
                LiftNode::If {
                    cond,
                    then,
                    otherwise,
                }
            }
            _ => return None,
        };
        self.nodes.push(node);
        Some(self.nodes.len() - 1)
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// One cell's value and format on a resolved sheet (the body of
    /// `resolve_cell_reference_value_formatted`; the lift reads a column of
    /// cells through it with the sheet resolved once).
    pub(crate) fn read_cell_formatted_in(
        &self,
        sheet_id: SheetId,
        asheet: Option<&crate::arrow_store::ArrowSheet>,
        row: u32,
        col: u32,
    ) -> (LiteralValue, Option<crate::format::FormatId>) {
        let (r0, c0) = (
            row.saturating_sub(1) as usize,
            col.saturating_sub(1) as usize,
        );
        let format = asheet.and_then(|a| a.format_id(r0, c0)).or_else(|| {
            self.derived_formats.get(&CellRef::new(
                sheet_id,
                Coord::from_excel(row, col, true, true),
            ))
        });
        let raw = asheet
            .map(|a| a.get_cell_value(r0, c0))
            .filter(|v| !matches!(v, LiteralValue::Empty));
        let value = match raw {
            None => LiteralValue::Empty,
            Some(raw) => {
                let class = format.and_then(|id| self.format_registry.class(id));
                Self::normalize_public_cell_read(Self::materialize_temporal_egress(
                    raw,
                    class,
                    self.config.temporal_egress,
                    self.config.date_system,
                ))
                .unwrap_or(LiteralValue::Empty)
            }
        };
        (value, format)
    }

    /// Evaluate a run through the lift; `None` leaves it to the walk.
    /// Members must have the template's literal row (no bound literals).
    pub(super) fn evaluate_run_lifted(
        &self,
        program: &LiftProgram,
        run: LayerRun,
        anchor: (u32, u32),
        n: usize,
    ) -> Option<Vec<Lifted>> {
        let ds = self.graph.data_store();
        let reg = self.graph.sheet_reg();
        let current_sheet = self.graph.sheet_name(run.sheet);
        let col_delta = i64::from(run.col) - i64::from(anchor.1);
        let row_delta0 = i64::from(run.row0) - i64::from(anchor.0);
        let first =
            crate::reference::CellRef::new(run.sheet, Coord::new(run.row0, run.col, true, true));
        let interpreter =
            crate::interpreter::Interpreter::new_with_cell(self, current_sheet, first);
        // Each node is used once (the program is a tree): children's columns
        // are moved into their parent, and constants stay one value.
        let mut columns: Vec<Column> = Vec::with_capacity(program.nodes.len());
        for node in &program.nodes {
            let column = match node {
                LiftNode::Value(value) => Column::Const(Ok((value.clone(), None))),
                LiftNode::Cell {
                    sheet,
                    row,
                    col,
                    row_abs,
                    col_abs,
                } => {
                    let sheet_name = match sheet {
                        Some(SheetKey::Id(id)) => reg.name(*id),
                        Some(SheetKey::Name(name)) => ds.resolve_ast_string(*name),
                        None => current_sheet,
                    };
                    let resolved = self
                        .graph
                        .sheet_id(sheet_name)
                        .map(|id| (id, self.arrow_sheets.sheet(sheet_name)));
                    let col = shift_axis(*col, col_delta, *col_abs);
                    // One cell for every member: read it once.
                    if *row_abs {
                        let cell = shift_axis(*row, row_delta0, true)
                            .and_then(|row| col.clone().map(|col| (row, col)))
                            .and_then(|(row, col)| match resolved {
                                Some((sheet_id, asheet)) => {
                                    Ok(self.read_cell_formatted_in(sheet_id, asheet, row, col))
                                }
                                None => Err(ExcelError::new(ExcelErrorKind::Ref)),
                            });
                        if let Ok((LiteralValue::Array(_), _)) = cell {
                            return None;
                        }
                        Column::Const(cell)
                    } else {
                        Column::Lane(self.read_lane(resolved, *row, row_delta0, col, n)?)
                    }
                }
                LiftNode::Unary { op, child } => {
                    let apply = |operand: Lifted| {
                        let (value, format) = operand?;
                        interpreter
                            .apply_unary_op(op, calc(value, format))
                            .map(split)
                    };
                    match take(&mut columns, *child) {
                        Column::Const(v) => Column::Const(apply(v)),
                        Column::Lane(lane) => Column::Lane(lane.unary(op, n, apply)),
                    }
                }
                LiftNode::Binary { op, left, right } => {
                    let apply = |l: Lifted, r: Lifted| {
                        let (lv, lf) = l?;
                        let (rv, rf) = r?;
                        interpreter.apply_binary_op(op, lv, lf, rv, rf).map(split)
                    };
                    let (l, r) = (take(&mut columns, *left), take(&mut columns, *right));
                    match (l, r) {
                        (Column::Const(l), Column::Const(r)) => Column::Const(apply(l, r)),
                        (l, r) => Column::Lane(Lane::binary(op, l, r, n, apply)),
                    }
                }
                // `IfFn::eval` through `dispatch` (SHORT_CIRCUIT: arity was
                // checked at compile; the result's own format propagates,
                // GENERAL dropped). Both branches are computed column-wise;
                // they are pure, so only the taken one is observable.
                LiftNode::If {
                    cond,
                    then,
                    otherwise,
                } => {
                    let cond = take(&mut columns, *cond);
                    let then = take(&mut columns, *then);
                    let otherwise = match otherwise {
                        Some(k) => take(&mut columns, *k),
                        None => Column::Const(Ok((LiteralValue::Boolean(false), None))),
                    };
                    Column::Lane(Lane::choose(cond, then, otherwise, n))
                }
            };
            columns.push(column);
        }
        Some(columns.pop()?.into_lifted(n))
    }

    /// The typed lane of one relative cell reference over the run: rows
    /// `shift(row, row_delta0 + i)` of column `col`. A member's element is
    /// clean when the cell holds a number (tag `Number`, numeric lane set,
    /// after both overlays) and has no format (cell format lanes and
    /// derived formats): that is exactly the scalar read's
    /// `(Number(x), None)`. Every other element is the scalar read itself.
    /// `None` when a read returns an array (the walk decides).
    fn read_lane(
        &self,
        resolved: Option<(SheetId, Option<&crate::arrow_store::ArrowSheet>)>,
        row: u32,
        row_delta0: i64,
        col: Result<u32, ExcelError>,
        n: usize,
    ) -> Option<Lane> {
        let mut lane = Lane::with_len(LaneKind::Num, n);
        let boxed = |lane: &mut Lane, i: usize, value: Lifted| -> Option<()> {
            if let Ok((LiteralValue::Array(_), _)) = value {
                return None;
            }
            lane.boxed.push((i as u32, value));
            Some(())
        };
        let (col, (sheet_id, asheet)) = match (col, resolved) {
            (Ok(col), Some(r)) => (col, r),
            (Err(e), _) => {
                for i in 0..n {
                    // The walk shifts the row first: its error wins.
                    let v = shift_axis(row, row_delta0 + i as i64, false).and(Err(e.clone()));
                    boxed(&mut lane, i, v)?;
                }
                return Some(lane);
            }
            (Ok(_), None) => {
                for i in 0..n {
                    let v = shift_axis(row, row_delta0 + i as i64, false)
                        .and(Err(ExcelError::new(ExcelErrorKind::Ref)));
                    boxed(&mut lane, i, v)?;
                }
                return Some(lane);
            }
        };
        let check_derived = !self.derived_formats.is_empty();
        let mut i = 0usize;
        while i < n {
            // Rows that do not shift onto the grid are #REF! (walk order).
            let r1 = match shift_axis(row, row_delta0 + i as i64, false) {
                Ok(r1) => r1,
                Err(e) => {
                    boxed(&mut lane, i, Err(e))?;
                    i += 1;
                    continue;
                }
            };
            let r0 = (r1 - 1) as usize;
            let c0 = (col - 1) as usize;
            // The chunk segment starting at this row (or a single row where
            // the sheet has no data there).
            let seg = asheet.and_then(|a| {
                let (ci, off) = a.chunk_of_row(r0)?;
                let ch = a.columns.get(c0)?.chunk(ci)?;
                let len = (ch.len() - off).min(n - i);
                Some((ch, off, len))
            });
            let Some((ch, off, len)) = seg else {
                let v = Ok(self.read_cell_formatted_in(sheet_id, asheet, r1, col));
                boxed(&mut lane, i, v)?;
                i += 1;
                continue;
            };
            let range = off..off + len;
            let cascade =
                crate::arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
            let base_tags = ch.type_tag.slice(off, len);
            let base_nums = ch.numbers_or_null().slice(off, len);
            let (tags, nums) = if cascade.has_any_in_range(range.clone()) {
                let nums = ch
                    .merged_numbers(range.clone())
                    .unwrap_or_else(|| cascade.select_numbers(range.clone(), &base_nums));
                (cascade.select_type_tags(range.clone(), &base_tags), nums)
            } else {
                (Arc::new(base_tags), Arc::new(base_nums))
            };
            // Formats: none anywhere in the segment, or checked per cell.
            let formats_clear = !ch.overlay.has_formats()
                && !ch.computed_overlay.has_formats()
                && ch
                    .format
                    .as_ref()
                    .is_none_or(|runs| runs.all_general_in(off, len));
            for k in 0..len {
                let idx = i + k;
                let r1 = r1 + k as u32;
                let clean = tags.value(k) == crate::arrow_store::TypeTag::Number as u8
                    && nums.is_valid(k)
                    && (formats_clear || asheet.and_then(|a| a.format_id(r0 + k, c0)).is_none())
                    && (!check_derived
                        || self
                            .derived_formats
                            .get(&CellRef::new(
                                sheet_id,
                                Coord::from_excel(r1, col, true, true),
                            ))
                            .is_none());
                if clean {
                    lane.vals[idx] = nums.value(k);
                    #[cfg(test)]
                    self.lane_clean_reads_for_test
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    let v = Ok(self.read_cell_formatted_in(sheet_id, asheet, r1, col));
                    boxed(&mut lane, idx, v)?;
                }
            }
            i += len;
        }
        Some(lane)
    }
}

type Lifted = Result<(LiteralValue, Option<FormatId>), ExcelError>;

/// What the clean elements of a lane are.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LaneKind {
    /// `(Number(x), None)`.
    Num,
    /// `(Boolean(x != 0), None)`.
    Bool,
}

/// A node's values over the run as typed lanes: `vals[i]` is member i's
/// value when it is clean; `boxed` holds (ascending member index, value)
/// for every other member, computed on the scalar path.
struct Lane {
    kind: LaneKind,
    vals: Vec<f64>,
    boxed: Vec<(u32, Lifted)>,
}

/// One element of a column: clean (a number, or a boolean as 0/1) or the
/// scalar path's value.
enum Elem<'c> {
    Num(f64),
    Bool(bool),
    Boxed(&'c Lifted),
}

impl Elem<'_> {
    fn lifted(&self) -> Lifted {
        match self {
            Elem::Num(x) => Ok((LiteralValue::Number(*x), None)),
            Elem::Bool(b) => Ok((LiteralValue::Boolean(*b), None)),
            Elem::Boxed(v) => (*v).clone(),
        }
    }
}

/// Sequential reader of a column's elements (members in order).
struct Cursor<'c> {
    column: &'c Column,
    next_boxed: usize,
}

impl<'c> Cursor<'c> {
    fn new(column: &'c Column) -> Self {
        Self {
            column,
            next_boxed: 0,
        }
    }

    /// Member `i`'s element; members must be read in ascending order.
    #[inline]
    fn get(&mut self, i: usize) -> Elem<'c> {
        match self.column {
            Column::Const(Ok((LiteralValue::Number(x), None))) => Elem::Num(*x),
            Column::Const(v) => Elem::Boxed(v),
            Column::Lane(lane) => {
                if let Some((j, v)) = lane.boxed.get(self.next_boxed)
                    && *j as usize == i
                {
                    self.next_boxed += 1;
                    return Elem::Boxed(v);
                }
                match lane.kind {
                    LaneKind::Num => Elem::Num(lane.vals[i]),
                    LaneKind::Bool => Elem::Bool(lane.vals[i] != 0.0),
                }
            }
        }
    }
}

impl Lane {
    fn with_len(kind: LaneKind, n: usize) -> Self {
        Self {
            kind,
            vals: vec![0.0; n],
            boxed: Vec::new(),
        }
    }

    /// Store a scalar-path result: clean when it is a plain number (or a
    /// plain boolean in a boolean lane).
    #[inline]
    fn put(&mut self, i: usize, value: Lifted) {
        match (&value, self.kind) {
            (Ok((LiteralValue::Number(x), None)), LaneKind::Num) => self.vals[i] = *x,
            (Ok((LiteralValue::Boolean(b), None)), LaneKind::Bool) => {
                self.vals[i] = if *b { 1.0 } else { 0.0 }
            }
            _ => self.boxed.push((i as u32, value)),
        }
    }

    fn unary(self, op: &'static str, n: usize, apply: impl Fn(Lifted) -> Lifted) -> Lane {
        let column = Column::Lane(self);
        let mut out = Lane::with_len(LaneKind::Num, n);
        let mut cur = Cursor::new(&column);
        for i in 0..n {
            match (cur.get(i), op) {
                // `+` is a pass-through; `-` and `%` coerce (a number is
                // itself) and sanitize, as `eval_unary_scalar`.
                (Elem::Num(x), "+") => out.vals[i] = x,
                (Elem::Num(x), "-") => match crate::interpreter::unary_f64(b'-', x) {
                    Ok(v) => out.vals[i] = v,
                    Err(e) => out.put(i, Ok((LiteralValue::Error(e), None))),
                },
                (Elem::Num(x), "%") => match crate::interpreter::unary_f64(b'%', x) {
                    Ok(v) => out.vals[i] = v,
                    Err(e) => out.put(i, Ok((LiteralValue::Error(e), None))),
                },
                (e, _) => out.put(i, apply(e.lifted())),
            }
        }
        out
    }

    /// A binary operator: numbers on both sides take the f64 path
    /// (`arith_f64`, `cmp_f64`, the scalar path's own functions); anything
    /// else goes through `apply` (the scalar operator).
    fn binary(
        op: &'static str,
        l: Column,
        r: Column,
        n: usize,
        apply: impl Fn(Lifted, Lifted) -> Lifted,
    ) -> Lane {
        let arith = match op {
            "+" => Some(b'+'),
            "-" => Some(b'-'),
            "*" => Some(b'*'),
            "/" => Some(b'/'),
            "^" => Some(b'^'),
            _ => None,
        };
        let compare = matches!(op, "=" | "<>" | ">" | "<" | ">=" | "<=");
        let kind = if compare {
            LaneKind::Bool
        } else {
            LaneKind::Num
        };
        let mut out = Lane::with_len(kind, n);
        let (mut lc, mut rc) = (Cursor::new(&l), Cursor::new(&r));
        for i in 0..n {
            let (a, b) = (lc.get(i), rc.get(i));
            match (a, b) {
                (Elem::Num(a), Elem::Num(b)) if arith.is_some() => {
                    // `+`/`-` annotate from the operand formats: none here.
                    match crate::interpreter::arith_f64(arith.unwrap(), a, b) {
                        Ok(v) => out.vals[i] = v,
                        Err(e) => out.put(i, Ok((LiteralValue::Error(e), None))),
                    }
                }
                (Elem::Num(a), Elem::Num(b)) if compare => {
                    out.vals[i] = if crate::interpreter::cmp_f64(a, b, op) {
                        1.0
                    } else {
                        0.0
                    };
                }
                (a, b) => out.put(i, apply(a.lifted(), b.lifted())),
            }
        }
        out
    }

    /// `IF(cond, then, otherwise)` per member, as `IfFn::eval`: an operand
    /// error propagates, the condition is a boolean or a number (non-zero),
    /// empty is false, an error value is the result, anything else is
    /// `#VALUE!`; the taken branch's format propagates without GENERAL.
    fn choose(cond: Column, then: Column, otherwise: Column, n: usize) -> Lane {
        let kind = match (then.kind(), otherwise.kind()) {
            (Some(LaneKind::Bool), Some(LaneKind::Bool)) => LaneKind::Bool,
            _ => LaneKind::Num,
        };
        let mut out = Lane::with_len(kind, n);
        let (mut cc, mut tc, mut oc) = (
            Cursor::new(&cond),
            Cursor::new(&then),
            Cursor::new(&otherwise),
        );
        for i in 0..n {
            let (c, t, o) = (cc.get(i), tc.get(i), oc.get(i));
            let taken = match c {
                Elem::Num(x) => x != 0.0,
                Elem::Bool(b) => b,
                Elem::Boxed(v) => match v {
                    Err(e) => {
                        out.put(i, Err(e.clone()));
                        continue;
                    }
                    Ok((condition, _)) => match condition {
                        LiteralValue::Boolean(b) => *b,
                        LiteralValue::Number(x) => *x != 0.0,
                        LiteralValue::Int(x) => *x != 0,
                        LiteralValue::Empty => false,
                        LiteralValue::Error(error) => {
                            out.put(i, Ok((LiteralValue::Error(error.clone()), None)));
                            continue;
                        }
                        _ => {
                            out.put(
                                i,
                                Ok((
                                    LiteralValue::Error(
                                        ExcelError::new_value()
                                            .with_message("IF condition must be boolean or number"),
                                    ),
                                    None,
                                )),
                            );
                            continue;
                        }
                    },
                },
            };
            match if taken { t } else { o } {
                Elem::Num(x) if kind == LaneKind::Num => out.vals[i] = x,
                Elem::Bool(b) if kind == LaneKind::Bool => out.vals[i] = if b { 1.0 } else { 0.0 },
                e => {
                    let result = e.lifted().map(|(value, format)| {
                        (
                            value,
                            format.filter(|id| *id != crate::format::FormatId::GENERAL),
                        )
                    });
                    out.put(i, result);
                }
            }
        }
        out
    }
}

/// A node's values over the run: typed lanes, or one value for all.
enum Column {
    Const(Lifted),
    Lane(Lane),
}

impl Column {
    /// The kind of every clean element (a numeric constant is `Num`).
    fn kind(&self) -> Option<LaneKind> {
        match self {
            Column::Const(Ok((LiteralValue::Number(_), None))) => Some(LaneKind::Num),
            Column::Const(Ok((LiteralValue::Boolean(_), None))) => Some(LaneKind::Bool),
            Column::Const(_) => None,
            Column::Lane(lane) => Some(lane.kind),
        }
    }

    fn into_lifted(self, n: usize) -> Vec<Lifted> {
        let mut out = Vec::with_capacity(n);
        let mut cur = Cursor::new(&self);
        for i in 0..n {
            out.push(cur.get(i).lifted());
        }
        out
    }
}

fn take(columns: &mut [Column], idx: usize) -> Column {
    std::mem::replace(
        &mut columns[idx],
        Column::Const(Ok((LiteralValue::Empty, None))),
    )
}

fn calc<'a>(value: LiteralValue, format: Option<FormatId>) -> CalcValue<'a> {
    match format {
        Some(format) => CalcValue::AnnotatedScalar(value, format),
        None => CalcValue::Scalar(value),
    }
}

fn split(cv: CalcValue<'_>) -> (LiteralValue, Option<FormatId>) {
    let format = cv.format_id();
    (cv.into_literal(), format)
}
