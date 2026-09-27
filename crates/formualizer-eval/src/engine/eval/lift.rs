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

type Lifted = Result<(LiteralValue, Option<FormatId>), ExcelError>;

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
                    let mut out = Vec::with_capacity(n);
                    for i in 0..n {
                        // The walk's order: row shift, column shift, sheet.
                        let cell = shift_axis(*row, row_delta0 + i as i64, *row_abs)
                            .and_then(|row| col.clone().map(|col| (row, col)))
                            .and_then(|(row, col)| match resolved {
                                Some((sheet_id, asheet)) => {
                                    Ok(self.read_cell_formatted_in(sheet_id, asheet, row, col))
                                }
                                None => Err(ExcelError::new(ExcelErrorKind::Ref)),
                            });
                        // Operators on scalars give scalars: arrays can
                        // only come from a read (then the walk decides).
                        if let Ok((LiteralValue::Array(_), _)) = cell {
                            return None;
                        }
                        out.push(cell);
                    }
                    Column::Many(out)
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
                        Column::Many(vs) => Column::Many(vs.into_iter().map(apply).collect()),
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
                        (l, r) => Column::Many(
                            l.iter(n).zip(r.iter(n)).map(|(l, r)| apply(l, r)).collect(),
                        ),
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
                    Column::Many(
                        cond.iter(n)
                            .zip(then.iter(n))
                            .zip(otherwise.iter(n))
                            .map(|((condition, then), otherwise)| {
                                let (condition, _) = condition?;
                                let taken = match condition {
                                    LiteralValue::Boolean(b) => b,
                                    LiteralValue::Number(x) => x != 0.0,
                                    LiteralValue::Int(x) => x != 0,
                                    LiteralValue::Empty => false,
                                    LiteralValue::Error(error) => {
                                        return Ok((LiteralValue::Error(error), None));
                                    }
                                    _ => {
                                        return Ok((
                                            LiteralValue::Error(
                                                ExcelError::new_value().with_message(
                                                    "IF condition must be boolean or number",
                                                ),
                                            ),
                                            None,
                                        ));
                                    }
                                };
                                let (value, format) = if taken { then? } else { otherwise? };
                                Ok((
                                    value,
                                    format.filter(|id| *id != crate::format::FormatId::GENERAL),
                                ))
                            })
                            .collect(),
                    )
                }
            };
            columns.push(column);
        }
        Some(columns.pop()?.iter(n).collect())
    }
}

/// A node's values over the run: one per member, or one for all.
enum Column {
    Const(Lifted),
    Many(Vec<Lifted>),
}

impl Column {
    fn iter(self, n: usize) -> ColumnIter {
        match self {
            Column::Const(v) => ColumnIter::Const(v, n),
            Column::Many(vs) => ColumnIter::Many(vs.into_iter()),
        }
    }
}

enum ColumnIter {
    Const(Lifted, usize),
    Many(std::vec::IntoIter<Lifted>),
}

impl Iterator for ColumnIter {
    type Item = Lifted;

    #[inline]
    fn next(&mut self) -> Option<Lifted> {
        match self {
            ColumnIter::Const(v, left) => {
                if *left == 0 {
                    return None;
                }
                *left -= 1;
                Some(v.clone())
            }
            ColumnIter::Many(it) => it.next(),
        }
    }
}

fn take(columns: &mut [Column], idx: usize) -> Column {
    std::mem::replace(&mut columns[idx], Column::Many(Vec::new()))
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
