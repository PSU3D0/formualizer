//! Least-squares regression shared by LINEST, LOGEST, TREND and GROWTH.
//!
//! Excel's LINEST documentation
//! (<https://support.microsoft.com/en-us/excel/functions/linest-function>) fixes
//! the behaviour implemented here:
//!
//! - When `known_y's` is a single column, each column of `known_x's` is a
//!   separate variable; when it is a single row, each row is. With one
//!   variable the two ranges may have any shape as long as the dimensions are
//!   equal. `known_x's` defaults to `{1,2,3,...}` in the shape of `known_y's`.
//! - The coefficients come back in reverse order of the variables,
//!   `{mn, ..., m1, b}`, and the statistics block is
//!   `{se_n..se_1, se_b; r2, sey; F, df; ssreg, ssresid}`, padded with `#N/A`.
//!   With `const = FALSE`, `b = 0` and `se_b` is `#N/A`.
//! - A variable that is a linear combination of the others (including a
//!   constant one when the intercept is fitted) is removed: it gets a
//!   coefficient and standard error of 0, and `df` increases by one.
//! - `df = n - k - 1` with the intercept and `n - k` without, where `k`
//!   counts the variables kept; the total sum of squares is taken about the
//!   mean only when the intercept is fitted.
//!
//! The fit uses Householder QR on the (centered, when `const` is TRUE)
//! design matrix, processing variables in order and dropping one whose
//! remaining norm is negligible, which is numerically stable and detects the
//! collinearity the documentation describes without forming `X'X`.

use super::scalar_like_value;
use crate::traits::{ArgumentHandle, CalcValue};
use formualizer_common::{ExcelError, LiteralValue};

/// A dense row-major matrix read from one argument.
struct Matrix {
    rows: usize,
    cols: usize,
    data: Vec<f64>,
}

impl Matrix {
    fn get(&self, r: usize, c: usize) -> f64 {
        self.data[r * self.cols + c]
    }
}

/// Whether an optional argument is omitted (`LINEST(y,,TRUE)`) or an empty
/// scalar. A reference never counts as omitted, even if its first cell is blank.
fn is_omitted(args: &[ArgumentHandle<'_, '_>], i: usize) -> bool {
    let Some(arg) = args.get(i) else {
        return true;
    };
    if arg.as_reference().is_ok() || matches!(arg.inline_array_literal(), Ok(Some(_))) {
        return false;
    }
    match arg.value() {
        Ok(CalcValue::Scalar(LiteralValue::Empty)) => true,
        Ok(CalcValue::Scalar(LiteralValue::Text(s))) => s.is_empty(),
        _ => false,
    }
}

fn logical_arg(
    args: &[ArgumentHandle<'_, '_>],
    i: usize,
    default: bool,
) -> Result<bool, ExcelError> {
    if is_omitted(args, i) {
        return Ok(default);
    }
    match scalar_like_value(&args[i])? {
        LiteralValue::Boolean(b) => Ok(b),
        LiteralValue::Number(n) => Ok(n != 0.0),
        LiteralValue::Int(i) => Ok(i != 0),
        LiteralValue::Error(e) => Err(e),
        LiteralValue::Empty => Ok(default),
        _ => Err(ExcelError::new_value()),
    }
}

fn cell_number(
    v: &LiteralValue,
    date_system: crate::engine::DateSystem,
) -> Result<f64, ExcelError> {
    match v {
        LiteralValue::Number(n) => Ok(*n),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Error(e) => Err(e.clone()),
        LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => {
            crate::coercion::to_serial_strict(v, date_system).map_err(|_| ExcelError::new_value())
        }
        // Blanks, text and logicals inside the data are #VALUE!, as in Excel.
        _ => Err(ExcelError::new_value()),
    }
}

/// Read an array argument; every cell must be a number.
fn read_matrix(arg: &ArgumentHandle<'_, '_>) -> Result<Matrix, ExcelError> {
    let date_system = arg.date_system();
    if let Some(rows) = arg.inline_array_literal()? {
        let cols = rows.first().map_or(0, Vec::len);
        let mut data = Vec::with_capacity(rows.len() * cols);
        for row in &rows {
            if row.len() != cols {
                return Err(ExcelError::new_value());
            }
            for v in row {
                data.push(cell_number(v, date_system)?);
            }
        }
        return Ok(Matrix {
            rows: rows.len(),
            cols,
            data,
        });
    }
    if let Ok(view) = arg.range_view() {
        let (rows, cols) = view.dims();
        let mut data = Vec::with_capacity(rows * cols);
        view.for_each_cell(&mut |v| {
            data.push(cell_number(v, date_system)?);
            Ok(())
        })?;
        return Ok(Matrix { rows, cols, data });
    }
    let v = scalar_like_value(arg)?;
    let n = match v {
        LiteralValue::Text(ref s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| ExcelError::new_value())?,
        LiteralValue::Boolean(b) => f64::from(u8::from(b)),
        other => cell_number(&other, date_system)?,
    };
    Ok(Matrix {
        rows: 1,
        cols: 1,
        data: vec![n],
    })
}

/// How the variables are laid out in `known_x's` (and therefore `new_x's`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// One variable; x has the same shape as y.
    Single,
    /// y is a column; each column of x is a variable.
    Columns,
    /// y is a row; each row of x is a variable.
    Rows,
}

/// The observations of one regression problem.
struct Problem {
    y: Vec<f64>,
    /// `vars[j][i]` is variable `j` at observation `i`.
    vars: Vec<Vec<f64>>,
    layout: Layout,
    /// `known_x's` as given (or the default), the default `new_x's`.
    x: Matrix,
}

fn build_problem(y: Matrix, x: Option<Matrix>) -> Result<Problem, ExcelError> {
    let n = y.data.len();
    if n == 0 {
        return Err(ExcelError::new_value());
    }
    let x = x.unwrap_or_else(|| Matrix {
        rows: y.rows,
        cols: y.cols,
        data: (1..=n).map(|i| i as f64).collect(),
    });
    let (layout, vars) = if x.rows == y.rows && x.cols == y.cols {
        (Layout::Single, vec![x.data.clone()])
    } else if y.cols == 1 && x.rows == n {
        let vars = (0..x.cols)
            .map(|c| (0..n).map(|r| x.get(r, c)).collect())
            .collect();
        (Layout::Columns, vars)
    } else if y.rows == 1 && x.cols == n {
        let vars = (0..x.rows)
            .map(|r| (0..n).map(|c| x.get(r, c)).collect())
            .collect();
        (Layout::Rows, vars)
    } else {
        return Err(ExcelError::new_ref());
    };
    Ok(Problem {
        y: y.data,
        vars,
        layout,
        x,
    })
}

/// Result of a least-squares fit.
struct Fit {
    /// Coefficient per variable, in variable order (0 for a removed variable).
    coef: Vec<f64>,
    /// Standard error per variable (0 for a removed variable).
    se: Vec<f64>,
    intercept: f64,
    /// Standard error of the intercept; `None` when `const` is FALSE.
    se_intercept: Option<f64>,
    r2: f64,
    sey: f64,
    f_stat: f64,
    df: f64,
    ss_reg: f64,
    ss_resid: f64,
}

/// A variable is redundant when the part of it not explained by the earlier
/// variables (and the constant) is below this fraction of its raw size.
const COLLINEAR_TOLERANCE: f64 = 1e-10;

fn fit(y: &[f64], vars: &[Vec<f64>], use_const: bool) -> Fit {
    let n = y.len();
    let k = vars.len();
    let nf = n as f64;
    let mean = |v: &[f64]| v.iter().sum::<f64>() / nf;
    let y_mean = if use_const { mean(y) } else { 0.0 };
    let x_means: Vec<f64> = vars
        .iter()
        .map(|v| if use_const { mean(v) } else { 0.0 })
        .collect();

    // Working copy of the (centered) design matrix, column-major, and of y.
    let mut a: Vec<Vec<f64>> = vars
        .iter()
        .zip(&x_means)
        .map(|(v, m)| v.iter().map(|x| x - m).collect())
        .collect();
    let mut qty: Vec<f64> = y.iter().map(|v| v - y_mean).collect();

    // Householder QR with in-order rank detection. `kept[j]` is the row of R
    // for variable j, or None if it was removed as redundant.
    let mut kept: Vec<Option<usize>> = vec![None; k];
    let mut reflectors: Vec<(usize, Vec<f64>, f64)> = Vec::new(); // (start row, v, beta)
    let mut r_cols: Vec<Vec<f64>> = Vec::new(); // R column for each kept variable
    let mut kept_vars: Vec<usize> = Vec::new();
    for j in 0..k {
        let raw_norm = vars[j].iter().map(|x| x * x).sum::<f64>().sqrt();
        let col = &mut a[j];
        for (start, v, beta) in &reflectors {
            apply_reflector(col, *start, v, *beta);
        }
        let rank = reflectors.len();
        let tail_norm = col[rank..].iter().map(|x| x * x).sum::<f64>().sqrt();
        if rank >= n || tail_norm <= COLLINEAR_TOLERANCE * raw_norm || tail_norm == 0.0 {
            continue;
        }
        let (v, beta, alpha) = householder(&col[rank..]);
        apply_reflector(col, rank, &v, beta);
        col[rank] = alpha;
        for x in col[rank + 1..].iter_mut() {
            *x = 0.0;
        }
        apply_reflector(&mut qty, rank, &v, beta);
        reflectors.push((rank, v, beta));
        r_cols.push(col[..=rank].to_vec());
        kept[j] = Some(rank);
        kept_vars.push(j);
    }
    let rank = kept_vars.len();

    // R is upper triangular (rank x rank): R[i][c] = r_cols[c][i] for i <= c.
    let r = |i: usize, c: usize| r_cols[c][i];
    let back_substitute = |rhs: &[f64]| {
        let mut out = vec![0.0; rank];
        for i in (0..rank).rev() {
            let s: f64 = rhs[i]
                - (i + 1..rank)
                    .zip(&out[i + 1..])
                    .map(|(c, oc)| r(i, c) * oc)
                    .sum::<f64>();
            out[i] = s / r(i, i);
        }
        out
    };
    let mut b = back_substitute(&qty);
    // One step of iterative refinement against the centered data removes most
    // of the rounding left by the solve (an exact fit comes back exact). The
    // residual is accumulated with error-free products and sums: a plainly
    // rounded residual of an almost exact fit is 0 and would correct nothing.
    if rank > 0 {
        let mut resid: Vec<f64> = (0..n)
            .map(|i| {
                compensated_residual(
                    y[i] - y_mean,
                    kept_vars
                        .iter()
                        .zip(&b)
                        .map(|(&j, bj)| (vars[j][i] - x_means[j], *bj)),
                )
            })
            .collect();
        for (start, v, beta) in &reflectors {
            apply_reflector(&mut resid, *start, v, *beta);
        }
        let delta = back_substitute(&resid);
        for (bi, di) in b.iter_mut().zip(delta) {
            *bi += di;
        }
    }
    let mut coef = vec![0.0; k];
    for (c, &j) in kept_vars.iter().enumerate() {
        coef[j] = b[c];
    }
    let intercept = if use_const {
        y_mean - coef.iter().zip(&x_means).map(|(c, m)| c * m).sum::<f64>()
    } else {
        0.0
    };

    let ss_resid: f64 = (0..n)
        .map(|i| {
            let pred = intercept + (0..k).map(|j| coef[j] * vars[j][i]).sum::<f64>();
            let e = y[i] - pred;
            e * e
        })
        .sum();
    let ss_total: f64 = if use_const {
        y.iter().map(|v| (v - y_mean) * (v - y_mean)).sum()
    } else {
        y.iter().map(|v| v * v).sum()
    };
    let ss_reg = ss_total - ss_resid;
    let df = n as f64 - rank as f64 - if use_const { 1.0 } else { 0.0 };
    let r2 = if ss_total == 0.0 {
        1.0
    } else {
        ss_reg / ss_total
    };
    let sey = if df > 0.0 {
        (ss_resid / df).sqrt()
    } else {
        f64::NAN
    };
    let f_stat = if df > 0.0 && rank > 0 && ss_resid > 0.0 {
        (ss_reg / rank as f64) / (ss_resid / df)
    } else {
        f64::NAN
    };

    // Standard errors from (R'R)^-1 = R^-1 R^-T: se_c = sey * ||row c of R^-1||.
    // `inv_cols[c]` is column c of R^-1 (upper triangular).
    let mut inv_cols: Vec<Vec<f64>> = Vec::with_capacity(rank);
    for c in 0..rank {
        let mut col = vec![0.0; rank];
        col[c] = 1.0 / r(c, c);
        for i in (0..c).rev() {
            let s: f64 = (i + 1..=c).map(|m| r(i, m) * col[m]).sum();
            col[i] = -s / r(i, i);
        }
        inv_cols.push(col);
    }
    let mut se = vec![0.0; k];
    for (c, &j) in kept_vars.iter().enumerate() {
        let row_norm2: f64 = inv_cols[c..].iter().map(|col| col[c] * col[c]).sum();
        se[j] = sey * row_norm2.sqrt();
    }
    let se_intercept = use_const.then(|| {
        // Var(b) = sey^2 (1/n + xbar' (Xc'Xc)^-1 xbar) = sey^2 (1/n + ||R^-T xbar||^2).
        let mut w = vec![0.0; rank];
        for c in 0..rank {
            let s: f64 = (0..c).map(|i| r(i, c) * w[i]).sum();
            w[c] = (x_means[kept_vars[c]] - s) / r(c, c);
        }
        sey * (1.0 / nf + w.iter().map(|v| v * v).sum::<f64>()).sqrt()
    });

    Fit {
        coef,
        se,
        intercept,
        se_intercept,
        r2,
        sey,
        f_stat,
        df,
        ss_reg,
        ss_resid,
    }
}

/// `target - sum(a * b)` with the rounding errors of each product and sum
/// carried along (two-product via fused multiply-add, two-sum compensation).
fn compensated_residual(target: f64, terms: impl Iterator<Item = (f64, f64)>) -> f64 {
    let mut sum = target;
    let mut err = 0.0;
    for (a, b) in terms {
        let p = a * b;
        let p_err = a.mul_add(b, -p);
        let t = sum - p;
        let bv = t - sum;
        let sum_err = (sum - (t - bv)) + (-p - bv);
        sum = t;
        err += sum_err - p_err;
    }
    sum + err
}

/// Householder vector for `x`: returns `(v, beta, alpha)` with
/// `(I - beta v v') x = alpha e1`.
fn householder(x: &[f64]) -> (Vec<f64>, f64, f64) {
    let norm = x.iter().map(|v| v * v).sum::<f64>().sqrt();
    let alpha = if x[0] > 0.0 { -norm } else { norm };
    let mut v = x.to_vec();
    v[0] -= alpha;
    let vv: f64 = v.iter().map(|t| t * t).sum();
    let beta = if vv == 0.0 { 0.0 } else { 2.0 / vv };
    (v, beta, alpha)
}

fn apply_reflector(col: &mut [f64], start: usize, v: &[f64], beta: f64) {
    let tail = &mut col[start..start + v.len()];
    let dot: f64 = tail.iter().zip(v).map(|(a, b)| a * b).sum();
    let s = beta * dot;
    for (t, vi) in tail.iter_mut().zip(v) {
        *t -= s * vi;
    }
}

fn number_or_num_error(n: f64) -> LiteralValue {
    if n.is_finite() {
        LiteralValue::Number(n)
    } else {
        LiteralValue::Error(ExcelError::new_num())
    }
}

fn na() -> LiteralValue {
    LiteralValue::Error(ExcelError::new_na())
}

/// Read `known_y's` (optionally log-transformed) and `known_x's`.
fn read_problem(args: &[ArgumentHandle<'_, '_>], log_y: bool) -> Result<Problem, ExcelError> {
    let mut y = read_matrix(&args[0])?;
    if log_y {
        for v in y.data.iter_mut() {
            if *v <= 0.0 {
                return Err(ExcelError::new_num());
            }
            *v = v.ln();
        }
    }
    let x = if is_omitted(args, 1) {
        None
    } else {
        Some(read_matrix(&args[1])?)
    };
    build_problem(y, x)
}

fn as_result(result: Result<LiteralValue, ExcelError>) -> Result<CalcValue<'static>, ExcelError> {
    Ok(CalcValue::Scalar(
        result.unwrap_or_else(LiteralValue::Error),
    ))
}

/// LINEST (`exponential = false`) and LOGEST (`exponential = true`).
pub(super) fn eval_linest<'b>(
    args: &[ArgumentHandle<'_, 'b>],
    exponential: bool,
) -> Result<CalcValue<'b>, ExcelError> {
    as_result((|| {
        let problem = read_problem(args, exponential)?;
        let use_const = logical_arg(args, 2, true)?;
        let stats = logical_arg(args, 3, false)?;
        let fit = fit(&problem.y, &problem.vars, use_const);
        let k = problem.vars.len();
        let width = k + 1;
        let out = |v: f64| {
            if exponential {
                number_or_num_error(v.exp())
            } else {
                number_or_num_error(v)
            }
        };
        let mut first: Vec<LiteralValue> = fit.coef.iter().rev().map(|&c| out(c)).collect();
        first.push(out(fit.intercept));
        if !stats {
            return Ok(LiteralValue::Array(vec![first]));
        }
        let mut second: Vec<LiteralValue> = fit
            .se
            .iter()
            .rev()
            .map(|&s| number_or_num_error(s))
            .collect();
        second.push(fit.se_intercept.map_or_else(na, number_or_num_error));
        let pair = |a: f64, b: f64| {
            let mut row = vec![number_or_num_error(a), number_or_num_error(b)];
            row.resize_with(width, na);
            row
        };
        Ok(LiteralValue::Array(vec![
            first,
            second,
            pair(fit.r2, fit.sey),
            pair(fit.f_stat, fit.df),
            pair(fit.ss_reg, fit.ss_resid),
        ]))
    })())
}

/// TREND (`exponential = false`) and GROWTH (`exponential = true`).
pub(super) fn eval_trend<'b>(
    args: &[ArgumentHandle<'_, 'b>],
    exponential: bool,
) -> Result<CalcValue<'b>, ExcelError> {
    as_result((|| {
        let problem = read_problem(args, exponential)?;
        let use_const = logical_arg(args, 3, true)?;
        let new_x = if is_omitted(args, 2) {
            None
        } else {
            Some(read_matrix(&args[2])?)
        };
        let fit = fit(&problem.y, &problem.vars, use_const);
        let predict = |point: &[f64]| {
            let v = fit.intercept + point.iter().zip(&fit.coef).map(|(x, c)| x * c).sum::<f64>();
            number_or_num_error(if exponential { v.exp() } else { v })
        };
        let new_x = new_x.unwrap_or(problem.x);
        let k = problem.vars.len();
        let rows = match problem.layout {
            Layout::Single => (0..new_x.rows)
                .map(|r| {
                    (0..new_x.cols)
                        .map(|c| predict(&[new_x.get(r, c)]))
                        .collect()
                })
                .collect(),
            Layout::Columns => {
                if new_x.cols != k {
                    return Err(ExcelError::new_ref());
                }
                (0..new_x.rows)
                    .map(|r| {
                        let point: Vec<f64> = (0..k).map(|c| new_x.get(r, c)).collect();
                        vec![predict(&point)]
                    })
                    .collect()
            }
            Layout::Rows => {
                if new_x.rows != k {
                    return Err(ExcelError::new_ref());
                }
                vec![
                    (0..new_x.cols)
                        .map(|c| {
                            let point: Vec<f64> = (0..k).map(|r| new_x.get(r, c)).collect();
                            predict(&point)
                        })
                        .collect(),
                ]
            }
        };
        Ok(LiteralValue::Array(rows))
    })())
}
