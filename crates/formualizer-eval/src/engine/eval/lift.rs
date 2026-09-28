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
    /// A built-in scalar function on typed lanes: a member whose operands
    /// are all clean takes the typed core (the builtin's own arithmetic);
    /// any other member is evaluated by the per-member walk.
    Builtin {
        kernel: crate::function::FamilyKernel,
        args: smallvec::SmallVec<[usize; 4]>,
    },
    /// Program 3: a call whose every reference is the same cell or range
    /// for every member of a column run (rows absolute or open, the run's
    /// column shared by all members) through pure, context-free functions:
    /// the walk evaluates it once per run, at the first member.
    Invariant {
        node: AstNodeId,
    },
}

/// Functions a run-invariant call may use: pure, and independent of the
/// cell evaluating them (no `ROW()`/`COLUMN()`-style context).
/// A function of `INVARIANT_FUNCTIONS` whose registered implementation is
/// not volatile, dynamic, environment-binding or spilling.
pub(super) fn pure_listed_function(
    functions: &dyn crate::traits::FunctionProvider,
    name: &str,
) -> bool {
    use crate::function::FnCaps;
    INVARIANT_FUNCTIONS
        .iter()
        .any(|f| f.eq_ignore_ascii_case(name))
        && functions.get_function("", name).is_some_and(|fun| {
            !fun.caps().intersects(
                FnCaps::VOLATILE
                    | FnCaps::DYNAMIC_DEPENDENCY
                    | FnCaps::LOCAL_ENVIRONMENT
                    | FnCaps::MAY_SPILL,
            )
        })
}

const INVARIANT_FUNCTIONS: &[&str] = &[
    "INDEX",
    "MATCH",
    "VLOOKUP",
    "HLOOKUP",
    "XLOOKUP",
    "XMATCH",
    "SUM",
    "SUMIF",
    "SUMIFS",
    "COUNT",
    "COUNTA",
    "COUNTIF",
    "COUNTIFS",
    "AVERAGE",
    "AVERAGEIF",
    "AVERAGEIFS",
    "MIN",
    "MAX",
    "SUMPRODUCT",
    "ROUND",
    "ABS",
    "IF",
    "IFERROR",
    "IFNA",
    "ISNA",
    "ISERROR",
    "ISNUMBER",
    "ISBLANK",
    "ISTEXT",
    "AND",
    "OR",
    "NOT",
    "CHOOSE",
    "MONTH",
    "YEAR",
    "DAY",
    "DATE",
    "LEFT",
    "RIGHT",
    "MID",
    "LEN",
    "FIND",
    "SEARCH",
    "TRIM",
    "UPPER",
    "LOWER",
    "CONCATENATE",
    "VALUE",
    "ISERR",
    "ISLOGICAL",
    "N",
    "INT",
    "MOD",
    "ROUNDUP",
    "ROUNDDOWN",
];

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
    /// Compile `template`, or `None` when it is not liftable. `names`
    /// tells whether a defined name is the same value for every member of
    /// the run (its second argument: the name is a call's direct argument,
    /// where a range is not intersected with the member).
    pub(super) fn compile(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        template: AstNodeId,
        names: &dyn Fn(&str, bool) -> bool,
    ) -> Option<Self> {
        let mut program = Self { nodes: Vec::new() };
        program.compile_node(functions, ds, template, names)?;
        // A template that is itself one run-invariant call (or name) stays
        // on the walk, member by member, like the walk's own invariant
        // binding (`invariant_subtrees` leaves the root out): lifting it
        // would evaluate it once per lifted chunk, and how a run is chunked
        // depends on the pool's thread count.
        if matches!(program.nodes.last(), Some(LiftNode::Invariant { .. })) {
            return None;
        }
        // A template without any reference is a constant family: the walk
        // is as cheap, keep it there (a run-invariant call is not).
        program
            .nodes
            .iter()
            .any(|n| matches!(n, LiftNode::Cell { .. } | LiftNode::Invariant { .. }))
            .then_some(program)
    }

    /// The maximal run-invariant calls strictly inside `root` (their value
    /// is one for the whole run; the walk can compute them once).
    pub(super) fn invariant_subtrees(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        root: AstNodeId,
        names: &dyn Fn(&str, bool) -> bool,
    ) -> Vec<AstNodeId> {
        fn visit(
            functions: &dyn crate::traits::FunctionProvider,
            ds: &DataStore,
            id: AstNodeId,
            is_root: bool,
            names: &dyn Fn(&str, bool) -> bool,
            out: &mut Vec<AstNodeId>,
        ) {
            match ds.get_node(id) {
                Some(AstNodeData::Function { .. }) => {
                    if !is_root && LiftProgram::invariant(functions, ds, id, names) {
                        out.push(id);
                    } else if let Some(args) = ds.get_args(id) {
                        for &arg in args {
                            visit(functions, ds, arg, false, names, out);
                        }
                    }
                }
                Some(AstNodeData::UnaryOp { expr_id, .. }) => {
                    visit(functions, ds, *expr_id, false, names, out)
                }
                Some(AstNodeData::BinaryOp {
                    left_id, right_id, ..
                }) => {
                    visit(functions, ds, *left_id, false, names, out);
                    visit(functions, ds, *right_id, false, names, out);
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        visit(functions, ds, root, true, names, &mut out);
        out
    }

    /// Whether `id` is a run-invariant expression (see `LiftNode::Invariant`).
    pub(super) fn invariant(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        id: AstNodeId,
        names: &dyn Fn(&str, bool) -> bool,
    ) -> bool {
        Self::invariant_in(functions, ds, id, false, names)
    }

    /// `as_arg`: `id` is a direct argument of a call. A range only there: as
    /// an operator's operand it is implicitly intersected with the member's
    /// row or column.
    fn invariant_in(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        id: AstNodeId,
        as_arg: bool,
        names: &dyn Fn(&str, bool) -> bool,
    ) -> bool {
        match ds.get_node(id) {
            Some(AstNodeData::Literal(vref)) => {
                !matches!(ds.retrieve_value(*vref), LiteralValue::Array(_))
            }
            Some(AstNodeData::Reference { ref_type, .. }) => match ref_type {
                CompactRefType::Cell { row_abs, row, .. } => *row_abs && *row > 0,
                CompactRefType::Range {
                    start_row,
                    end_row,
                    start_row_abs,
                    end_row_abs,
                    ..
                } => {
                    as_arg
                        && (*start_row_abs || *start_row == 0)
                        && (*end_row_abs || *end_row == u32::MAX)
                }
                CompactRefType::NamedRange(name) => names(ds.resolve_ast_string(*name), as_arg),
                _ => false,
            },
            Some(AstNodeData::UnaryOp { expr_id, .. }) => {
                Self::invariant_in(functions, ds, *expr_id, false, names)
            }
            Some(AstNodeData::BinaryOp {
                left_id, right_id, ..
            }) => {
                Self::invariant_in(functions, ds, *left_id, false, names)
                    && Self::invariant_in(functions, ds, *right_id, false, names)
            }
            Some(AstNodeData::Function { name_id, .. }) => {
                pure_listed_function(functions, ds.resolve_ast_string(*name_id))
                    && ds.get_args(id).is_some_and(|args| {
                        args.iter()
                            .all(|&a| Self::invariant_in(functions, ds, a, true, names))
                    })
            }
            _ => false,
        }
    }

    fn compile_node(
        &mut self,
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        id: AstNodeId,
        names: &dyn Fn(&str, bool) -> bool,
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
                let child = self.compile_node(functions, ds, expr_id, names)?;
                LiftNode::Unary { op, child }
            }
            AstNodeData::BinaryOp {
                op_id,
                left_id,
                right_id,
            } => {
                let op = static_binary(ds.resolve_ast_string(*op_id))?;
                let (left_id, right_id) = (*left_id, *right_id);
                let left = self.compile_node(functions, ds, left_id, names)?;
                let right = self.compile_node(functions, ds, right_id, names)?;
                LiftNode::Binary { op, left, right }
            }
            AstNodeData::Function { .. } if Self::invariant(functions, ds, id, names) => {
                LiftNode::Invariant { node: id }
            }
            // A defined name with one value for the run (`=I2/UOM`).
            AstNodeData::Reference {
                ref_type: CompactRefType::NamedRange(_),
                ..
            } if Self::invariant(functions, ds, id, names) => LiftNode::Invariant { node: id },
            AstNodeData::Function { name_id, .. } => {
                // Only the built-in IF (an override keeps `family_kernel`
                // `None`), with the arities its `eval` accepts.
                let fun = functions.get_function("", ds.resolve_ast_string(*name_id))?;
                use crate::function::FamilyKernel as K;
                let kernel = fun.family_kernel()?;
                let args = ds.get_args(id)?;
                let arity_ok = match kernel {
                    K::If => (2..=3).contains(&args.len()),
                    K::Round | K::IfError => args.len() == 2,
                    K::Abs => args.len() == 1,
                    K::Min | K::Max | K::And | K::Or => !args.is_empty() && args.len() <= 30,
                    // `SUM` of scalar operands (ranges do not compile).
                    K::Sum => !args.is_empty() && args.len() <= 30,
                    K::IsNumber
                    | K::IsText
                    | K::IsLogical
                    | K::IsBlank
                    | K::IsError
                    | K::IsErr
                    | K::IsNa
                    | K::Year
                    | K::Month
                    | K::Day => args.len() == 1,
                    K::Weekday => (1..=2).contains(&args.len()),
                    _ => false,
                };
                if !arity_ok {
                    return None;
                }
                if kernel != K::If {
                    let args: smallvec::SmallVec<[AstNodeId; 4]> = args.iter().copied().collect();
                    let mut compiled = smallvec::SmallVec::new();
                    for arg in args {
                        compiled.push(self.compile_node(functions, ds, arg, names)?);
                    }
                    self.nodes.push(LiftNode::Builtin {
                        kernel,
                        args: compiled,
                    });
                    return Some(self.nodes.len() - 1);
                }
                let args: smallvec::SmallVec<[AstNodeId; 3]> = args.iter().copied().collect();
                let cond = self.compile_node(functions, ds, args[0], names)?;
                let then = self.compile_node(functions, ds, args[1], names)?;
                let otherwise = match args.get(2) {
                    Some(&arg) => Some(self.compile_node(functions, ds, arg, names)?),
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
    ) -> Option<Vec<Option<Lifted>>> {
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
                LiftNode::Builtin { kernel, args } => {
                    let args: smallvec::SmallVec<[Column; 4]> =
                        args.iter().map(|&k| take(&mut columns, k)).collect();
                    Column::Lane(Lane::builtin(*kernel, &args, n, self.config.date_system))
                }
                // Once per run, exactly as the walk evaluates it for the
                // first member (every member sees the same cells).
                LiftNode::Invariant { node } => {
                    let value = interpreter
                        .evaluate_arena_ast_with_offset(*node, row_delta0, col_delta, ds, reg)
                        .map(split);
                    if let Ok((LiteralValue::Array(_), _)) = value {
                        return None;
                    }
                    Column::Const(value)
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

/// Program 3 chain units: a family run whose members read the member
/// above (`=A1+1` or `=C2+D1` filled down) evaluated member by member in
/// row order through the compiled template, the member above coming from
/// the previous result instead of a written cell. Only operators, literals
/// and cell references; the one reference into the run's own column must
/// be the cell directly above; a member result that is not a clean number
/// (no format) stops the chain (the caller takes the per-cell path), so the
/// carried value is exactly what reading the written cell would give.
impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Reading a chain member's cell right after it is written gives its
    /// result exactly: computed writes land, no user value shadows them
    /// (a formula cell's delta overlay entry), and no format (cell format
    /// lanes, overlay formats) annotates them.
    fn chain_cells_plain(&self, run: LayerRun, n: usize) -> bool {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
            || self.computed_overlay_mirroring_disabled
        {
            return false;
        }
        let sheet = self.graph.sheet_name(run.sheet);
        let Some(asheet) = self.arrow_sheets.sheet(sheet) else {
            return true;
        };
        let Some(column) = asheet.columns.get(run.col as usize) else {
            return true;
        };
        let (mut r, end) = (run.row0 as usize, run.row0 as usize + n);
        while r < end {
            let Some((ci, off)) = asheet.chunk_of_row(r) else {
                // Past the sheet's rows: nothing stored there.
                return true;
            };
            let Some(ch) = column.chunk(ci) else {
                let next = asheet
                    .chunk_starts
                    .get(ci + 1)
                    .copied()
                    .unwrap_or(asheet.nrows as usize);
                r = next.max(r + 1);
                continue;
            };
            let len = (ch.len() - off).min(end - r);
            if ch.overlay.has_any_in_range(off..off + len)
                || ch.overlay.has_formats()
                || ch.computed_overlay.has_formats()
                || ch
                    .format
                    .as_ref()
                    .is_some_and(|runs| !runs.all_general_in(off, len))
            {
                return false;
            }
            r += len.max(1);
        }
        true
    }

    pub(super) fn evaluate_chain_lifted(
        &self,
        program: &LiftProgram,
        run: LayerRun,
        anchor: (u32, u32),
        n: usize,
    ) -> Option<Vec<f64>> {
        enum Source {
            Const(LiteralValue),
            Above,
            Cell {
                sheet_id: SheetId,
                asheet: Option<usize>,
                row: u32,
                row_abs: bool,
                col: u32,
            },
        }
        let ds = self.graph.data_store();
        let reg = self.graph.sheet_reg();
        let current_sheet = self.graph.sheet_name(run.sheet);
        let col_delta = i64::from(run.col) - i64::from(anchor.1);
        let row_delta0 = i64::from(run.row0) - i64::from(anchor.0);
        // Excel (1-based) rows of the run.
        let (first_row, last_row) = (run.row0 + 1, run.row0 + n as u32);
        let mut sources: Vec<Option<Source>> = Vec::with_capacity(program.nodes.len());
        let mut above = false;
        for node in &program.nodes {
            let source = match node {
                LiftNode::Value(value) => Some(Source::Const(value.clone())),
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
                    let sheet_id = self.graph.sheet_id(sheet_name)?;
                    let col = shift_axis(*col, col_delta, *col_abs).ok()?;
                    let first = shift_axis(*row, row_delta0, *row_abs).ok()?;
                    // Every member's row stays on the grid.
                    if !*row_abs {
                        shift_axis(*row, row_delta0 + n as i64 - 1, false).ok()?;
                    }
                    if sheet_id == run.sheet && col == run.col + 1 {
                        if !*row_abs && first + 1 == first_row {
                            above = true;
                            Some(Source::Above)
                        } else if *row_abs && !(first_row..=last_row).contains(&first) {
                            Some(Source::Cell {
                                sheet_id,
                                asheet: None,
                                row: first,
                                row_abs: true,
                                col,
                            })
                        } else {
                            // Another member's cell: not a chain.
                            return None;
                        }
                    } else {
                        let asheet = self
                            .arrow_sheets
                            .sheets
                            .iter()
                            .position(|s| s.name.as_ref() == sheet_name);
                        Some(Source::Cell {
                            sheet_id,
                            asheet,
                            row: first,
                            row_abs: *row_abs,
                            col,
                        })
                    }
                }
                LiftNode::Unary { .. } | LiftNode::Binary { .. } => None,
                LiftNode::If { .. } | LiftNode::Builtin { .. } | LiftNode::Invariant { .. } => {
                    return None;
                }
            };
            sources.push(source);
        }
        if !above || !self.chain_cells_plain(run, n) {
            return None;
        }
        let first =
            crate::reference::CellRef::new(run.sheet, Coord::new(run.row0, run.col, true, true));
        let interpreter =
            crate::interpreter::Interpreter::new_with_cell(self, current_sheet, first);
        let read = |sheet_id: SheetId, asheet: Option<usize>, row: u32, col: u32| -> Lifted {
            let asheet = asheet.and_then(|i| self.arrow_sheets.sheets.get(i));
            Ok(self.read_cell_formatted_in(sheet_id, asheet, row, col))
        };
        // The cell above the first member: read (outside the run).
        let carry: Lifted = {
            let asheet = self
                .arrow_sheets
                .sheets
                .iter()
                .position(|s| s.name.as_ref() == current_sheet);
            if run.row0 == 0 {
                Err(ExcelError::new(ExcelErrorKind::Ref))
            } else {
                read(run.sheet, asheet, run.row0, run.col + 1)
            }
        };
        // Numbers stay unboxed: a clean number (no format) through `+ - *
        // / ^` and unary `-`/`%` takes the shared `arith_f64`/`unary_f64`,
        // exactly as the walk does; anything else is the boxed value.
        enum V {
            Num(f64),
            Boxed(Lifted),
        }
        fn typed(value: Lifted) -> V {
            match value {
                Ok((LiteralValue::Number(x), None)) => V::Num(x),
                other => V::Boxed(other),
            }
        }
        fn boxed(value: V) -> Lifted {
            match value {
                V::Num(x) => Ok((LiteralValue::Number(x), None)),
                V::Boxed(b) => b,
            }
        }
        let mut carry = typed(carry);
        let mut out = Vec::with_capacity(n);
        let mut values: Vec<V> = Vec::with_capacity(program.nodes.len());
        for i in 0..n {
            values.clear();
            for (node, source) in program.nodes.iter().zip(&sources) {
                let value: V = match (node, source) {
                    (_, Some(Source::Const(v))) => typed(Ok((v.clone(), None))),
                    (_, Some(Source::Above)) => match &carry {
                        V::Num(x) => V::Num(*x),
                        V::Boxed(b) => V::Boxed(b.clone()),
                    },
                    (
                        _,
                        Some(Source::Cell {
                            sheet_id,
                            asheet,
                            row,
                            row_abs,
                            col,
                        }),
                    ) => {
                        let r = if *row_abs { *row } else { *row + i as u32 };
                        typed(read(*sheet_id, *asheet, r, *col))
                    }
                    (LiftNode::Unary { op, child }, None) => {
                        let operand = std::mem::replace(&mut values[*child], V::Num(0.0));
                        match (operand, op.as_bytes()) {
                            (V::Num(x), [b @ (b'-' | b'%')]) => {
                                match crate::interpreter::unary_f64(*b, x) {
                                    Ok(v) => V::Num(v),
                                    Err(e) => V::Boxed(Err(e)),
                                }
                            }
                            (operand, _) => typed(boxed(operand).and_then(|(v, f)| {
                                interpreter.apply_unary_op(op, calc(v, f)).map(split)
                            })),
                        }
                    }
                    (LiftNode::Binary { op, left, right }, None) => {
                        let l = std::mem::replace(&mut values[*left], V::Num(0.0));
                        let r = std::mem::replace(&mut values[*right], V::Num(0.0));
                        match (l, r, op.as_bytes()) {
                            (V::Num(a), V::Num(b), [o @ (b'+' | b'-' | b'*' | b'/' | b'^')]) => {
                                match crate::interpreter::arith_f64(*o, a, b) {
                                    Ok(v) => V::Num(v),
                                    Err(e) => V::Boxed(Err(e)),
                                }
                            }
                            (l, r, _) => typed(match (boxed(l), boxed(r)) {
                                (Ok((lv, lf)), Ok((rv, rf))) => {
                                    interpreter.apply_binary_op(op, lv, lf, rv, rf).map(split)
                                }
                                (Err(e), _) | (_, Err(e)) => Err(e),
                            }),
                        }
                    }
                    _ => return None,
                };
                if let V::Boxed(Ok((LiteralValue::Array(_), _))) = value {
                    return None;
                }
                values.push(value);
            }
            // The next member reads this one's written cell: exact only for
            // a clean number (see the impl doc).
            match values.pop()? {
                V::Num(x) if x.is_finite() => {
                    carry = V::Num(x);
                    out.push(x);
                }
                _ => return None,
            }
        }
        Some(out)
    }
}

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
    /// Ascending members whose whole formula the per-member walk
    /// evaluates (a builtin operand was not clean).
    walk: Vec<u32>,
}

/// One element of a column: clean (a number, or a boolean as 0/1) or the
/// scalar path's value.
enum Elem<'c> {
    Num(f64),
    Bool(bool),
    Boxed(&'c Lifted),
    /// The member is evaluated by the walk.
    Walk,
}

impl Elem<'_> {
    fn lifted(&self) -> Lifted {
        match self {
            Elem::Num(x) => Ok((LiteralValue::Number(*x), None)),
            Elem::Bool(b) => Ok((LiteralValue::Boolean(*b), None)),
            Elem::Boxed(v) => (*v).clone(),
            Elem::Walk => unreachable!("walk elements are not materialized"),
        }
    }
}

/// Sequential reader of a column's elements (members in order).
struct Cursor<'c> {
    column: &'c Column,
    next_boxed: usize,
    next_walk: usize,
}

impl<'c> Cursor<'c> {
    fn new(column: &'c Column) -> Self {
        Self {
            column,
            next_boxed: 0,
            next_walk: 0,
        }
    }

    /// Member `i`'s element; members must be read in ascending order.
    #[inline]
    fn get(&mut self, i: usize) -> Elem<'c> {
        match self.column {
            Column::Const(Ok((LiteralValue::Number(x), None))) => Elem::Num(*x),
            Column::Const(Ok((LiteralValue::Boolean(b), None))) => Elem::Bool(*b),
            Column::Const(v) => Elem::Boxed(v),
            Column::Lane(lane) => {
                if let Some(&j) = lane.walk.get(self.next_walk)
                    && j as usize == i
                {
                    self.next_walk += 1;
                    // A walk member may also have a boxed placeholder.
                    if let Some((b, _)) = lane.boxed.get(self.next_boxed)
                        && *b as usize == i
                    {
                        self.next_boxed += 1;
                    }
                    return Elem::Walk;
                }
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
            walk: Vec::new(),
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
                (Elem::Walk, _) => out.walk.push(i as u32),
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
                (Elem::Walk, _) | (_, Elem::Walk) => out.walk.push(i as u32),
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
                Elem::Walk => {
                    out.walk.push(i as u32);
                    continue;
                }
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
                Elem::Walk => out.walk.push(i as u32),
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

impl Lane {
    /// A builtin over typed lanes. A member takes the typed core only when
    /// every operand element is clean (a number, or a boolean where the
    /// builtin reads booleans); the core is the builtin's own arithmetic
    /// on those values (see each arm). Any other member is left to the
    /// per-member walk, so its value, error and format are the walk's.
    fn builtin(
        kernel: crate::function::FamilyKernel,
        args: &[Column],
        n: usize,
        date_system: crate::engine::DateSystem,
    ) -> Lane {
        use crate::function::FamilyKernel as K;
        let kind = match kernel {
            K::And
            | K::Or
            | K::IsNumber
            | K::IsText
            | K::IsLogical
            | K::IsBlank
            | K::IsError
            | K::IsErr
            | K::IsNa => LaneKind::Bool,
            _ => LaneKind::Num,
        };
        let mut out = Lane::with_len(kind, n);
        let mut cursors: smallvec::SmallVec<[Cursor<'_>; 4]> =
            args.iter().map(Cursor::new).collect();
        let mut elems: smallvec::SmallVec<[Elem<'_>; 4]> = smallvec::SmallVec::new();
        for i in 0..n {
            elems.clear();
            elems.extend(cursors.iter_mut().map(|c| c.get(i)));
            let num = |e: &Elem<'_>| match e {
                Elem::Num(x) => Some(*x),
                _ => None,
            };
            let value: Option<f64> = match kernel {
                // `ROUND(n, digits)`: both coerce as numbers (a number is
                // itself; `digits` truncates to i32 as `as i32`), then
                // `round_digits`; no sanitizing, no format.
                K::Round => match (num(&elems[0]), num(&elems[1])) {
                    (Some(x), Some(d)) => {
                        Some(crate::builtins::math::numeric::round_digits(x, d as i32))
                    }
                    _ => None,
                },
                // `ABS(n)`: the coerced number's absolute value.
                K::Abs => num(&elems[0]).map(f64::abs),
                // `MIN`/`MAX`: a cell operand is a 1x1 range (its number,
                // since a clean cell has no error), any other operand a
                // coerced scalar; the running extremum replaces on strict
                // `<` / `>` from the first operand; `aggregate_result`
                // (non-finite is #NUM!) with the chosen operand's format
                // (none here). NaN operands are left to the walk (Arrow's
                // min/max of a range orders NaN differently).
                K::Min | K::Max => {
                    let mut acc: Option<f64> = None;
                    let mut ok = true;
                    for e in &elems {
                        match num(e) {
                            Some(x) if !x.is_nan() => {
                                let better = match acc {
                                    None => true,
                                    Some(c) if kernel == K::Min => x < c,
                                    Some(c) => x > c,
                                };
                                if better {
                                    acc = Some(x);
                                }
                            }
                            _ => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        let v = acc.unwrap_or(0.0);
                        if !v.is_finite() {
                            out.put(i, Ok((LiteralValue::Error(ExcelError::new_num()), None)));
                            continue;
                        }
                        Some(v)
                    } else {
                        None
                    }
                }
                // `SUM` of scalar operands: `total += x` in operand order
                // from 0.0 (a 1x1 range's Arrow sum adds 0.0 lanes, which
                // cannot change a total that is never -0.0), then
                // `aggregate_result`.
                K::Sum => {
                    let mut total = 0.0f64;
                    let mut ok = true;
                    for e in &elems {
                        match num(e) {
                            Some(x) => total += x,
                            None => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if !ok {
                        None
                    } else if total.is_finite() {
                        Some(total)
                    } else {
                        out.put(i, Ok((LiteralValue::Error(ExcelError::new_num()), None)));
                        continue;
                    }
                }
                // `AND`/`OR` over clean numbers and booleans: the first
                // decisive operand decides, otherwise the neutral value.
                K::And | K::Or => {
                    let mut decided = None;
                    let mut ok = true;
                    for e in &elems {
                        let truth = match e {
                            Elem::Num(x) => *x != 0.0,
                            Elem::Bool(b) => *b,
                            _ => {
                                ok = false;
                                break;
                            }
                        };
                        if decided.is_none() && truth == (kernel == K::Or) {
                            decided = Some(truth);
                        }
                    }
                    if ok {
                        let v = decided.unwrap_or(kernel == K::And);
                        out.vals[i] = if v { 1.0 } else { 0.0 };
                        continue;
                    }
                    None
                }
                // `IFERROR(value, fallback)`: a clean value is the result
                // (the fallback is not evaluated); errors take the walk.
                K::IfError => num(&elems[0]),
                // Type tests of a clean operand (a plain number or a plain
                // boolean): the builtin's verdict on that value.
                K::IsNumber
                | K::IsText
                | K::IsLogical
                | K::IsBlank
                | K::IsError
                | K::IsErr
                | K::IsNa => {
                    let verdict = match (&elems[0], kernel) {
                        (Elem::Num(_), K::IsNumber) | (Elem::Bool(_), K::IsLogical) => Some(true),
                        (Elem::Num(_) | Elem::Bool(_), _) => Some(false),
                        _ => None,
                    };
                    match verdict {
                        Some(v) => {
                            out.vals[i] = if v { 1.0 } else { 0.0 };
                            continue;
                        }
                        None => None,
                    }
                }
                // `YEAR`/`MONTH`/`DAY` of a clean number: the serial's
                // date in the workbook's system, an integer result (boxed:
                // the builtin returns `Int`); a serial off the calendar is
                // left to the walk.
                K::Year | K::Month | K::Day => {
                    if let Some(x) = num(&elems[0])
                        && let Ok(date) = formualizer_common::try_serial_to_date_for(date_system, x)
                    {
                        use chrono::Datelike;
                        let part = match kernel {
                            K::Year => i64::from(date.year()),
                            K::Month => i64::from(date.month()),
                            _ => i64::from(date.day()),
                        };
                        out.boxed
                            .push((i as u32, Ok((LiteralValue::Int(part), None))));
                        continue;
                    }
                    None
                }
                // `WEEKDAY(serial[, type])` of clean numbers: the type
                // truncates, a negative whole serial is #NUM!.
                K::Weekday => {
                    let return_type = match elems.get(1) {
                        None => Some(1),
                        Some(e) => num(e).map(|t| t.trunc() as i64),
                    };
                    if let (Some(x), Some(t)) = (num(&elems[0]), return_type) {
                        let whole = x.trunc() as i64;
                        let value = if whole < 0 {
                            LiteralValue::Error(ExcelError::new_num())
                        } else {
                            crate::builtins::datetime::weekday_workday::weekday_of_serial(
                                date_system,
                                whole,
                                t,
                            )
                        };
                        out.boxed.push((i as u32, Ok((value, None))));
                        continue;
                    }
                    None
                }
                _ => None,
            };
            match value {
                Some(v) => out.vals[i] = v,
                None => out.walk.push(i as u32),
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

    /// Every member's result; `None` for members the walk evaluates.
    fn into_lifted(self, n: usize) -> Vec<Option<Lifted>> {
        let mut out = Vec::with_capacity(n);
        let mut cur = Cursor::new(&self);
        for i in 0..n {
            out.push(match cur.get(i) {
                Elem::Walk => None,
                e => Some(e.lifted()),
            });
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
