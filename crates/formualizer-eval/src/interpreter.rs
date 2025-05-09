use crate::traits::{ArgumentHandle, EvaluationContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

pub struct Interpreter {
    pub context: Box<dyn EvaluationContext>,
}

impl Interpreter {
    pub fn new(context: Box<dyn EvaluationContext>) -> Self {
        Self { context }
    }

    /* ===================  public  =================== */
    pub fn evaluate_ast(&self, node: &ASTNode) -> Result<LiteralValue, ExcelError> {
        match &node.node_type {
            ASTNodeType::Literal(v) => Ok(v.clone()),
            ASTNodeType::Reference { reference, .. } => self.eval_reference(reference),
            ASTNodeType::UnaryOp { op, expr } => self.eval_unary(op, expr),
            ASTNodeType::BinaryOp { op, left, right } => self.eval_binary(op, left, right),
            ASTNodeType::Function { name, args } => self.eval_function(name, args),
            ASTNodeType::Array(rows) => self.eval_array_literal(rows),
        }
    }

    /* ===================  reference  =================== */
    fn eval_reference(&self, reference: &ReferenceType) -> Result<LiteralValue, ExcelError> {
        match self.context.resolve_range_like(reference) {
            Ok(range) => {
                let (rows, cols) = range.dimensions();
                let data = range.materialise().into_owned();
                if rows == 1 && cols == 1 {
                    Ok(data[0][0].clone())
                } else {
                    Ok(LiteralValue::Array(data))
                }
            }
            Err(e) => Ok(LiteralValue::Error(e)),
        }
    }

    /* ===================  unary ops  =================== */
    fn eval_unary(&self, op: &str, expr: &ASTNode) -> Result<LiteralValue, ExcelError> {
        let v = self.evaluate_ast(expr)?;
        match v {
            LiteralValue::Array(arr) => {
                self.map_array(arr, |cell| self.eval_unary_scalar(op, cell))
            }
            other => self.eval_unary_scalar(op, other),
        }
    }

    fn eval_unary_scalar(&self, op: &str, v: LiteralValue) -> Result<LiteralValue, ExcelError> {
        match op {
            "+" => self.apply_number_unary(v, |n| n),
            "-" => self.apply_number_unary(v, |n| -n),
            "%" => self.apply_number_unary(v, |n| n / 100.0),
            _ => {
                Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message(format!("Unary op '{}'", op)))
            }
        }
    }

    fn apply_number_unary<F>(&self, v: LiteralValue, f: F) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(f64) -> f64,
    {
        match self.coerce_number(&v) {
            Ok(n) => Ok(LiteralValue::Number(f(n))),
            Err(e) => Ok(LiteralValue::Error(e)),
        }
    }

    /* ===================  binary ops  =================== */
    fn eval_binary(
        &self,
        op: &str,
        left: &ASTNode,
        right: &ASTNode,
    ) -> Result<LiteralValue, ExcelError> {
        // Comparisons use dedicated path.
        if matches!(op, "=" | "<>" | ">" | "<" | ">=" | "<=") {
            let l = self.evaluate_ast(left)?;
            let r = self.evaluate_ast(right)?;
            return self.compare(op, l, r);
        }

        let l_val = self.evaluate_ast(left)?;
        let r_val = self.evaluate_ast(right)?;

        match op {
            "+" => self.numeric_binary(l_val, r_val, |a, b| a + b),
            "-" => self.numeric_binary(l_val, r_val, |a, b| a - b),
            "*" => self.numeric_binary(l_val, r_val, |a, b| a * b),
            "/" => self.divide(l_val, r_val),
            "^" => self.power(l_val, r_val),
            "&" => Ok(LiteralValue::Text(format!(
                "{}{}",
                self.coerce_text(&l_val),
                self.coerce_text(&r_val)
            ))),
            ":" => Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("Range operator ':' inside value context")),
            _ => {
                Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message(format!("Binary op '{}'", op)))
            }
        }
    }

    /* ===================  function calls  =================== */
    fn eval_function(&self, name: &str, args: &[ASTNode]) -> Result<LiteralValue, ExcelError> {
        if let Some(fun) = self.context.get_function("", name) {
            let handles: Vec<ArgumentHandle> =
                args.iter().map(|n| ArgumentHandle::new(n, self)).collect();
            fun.eval(&handles, self.context.as_ref())
        } else {
            Ok(LiteralValue::Error(ExcelError::from_error_string("#NAME?")))
        }
    }

    /* ===================  array literal  =================== */
    fn eval_array_literal(&self, rows: &[Vec<ASTNode>]) -> Result<LiteralValue, ExcelError> {
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let mut r = Vec::with_capacity(row.len());
            for cell in row {
                r.push(self.evaluate_ast(cell)?);
            }
            out.push(r);
        }
        Ok(LiteralValue::Array(out))
    }

    /* ===================  helpers  =================== */
    fn numeric_binary<F>(
        &self,
        left: LiteralValue,
        right: LiteralValue,
        f: F,
    ) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(f64, f64) -> f64 + Copy,
    {
        use LiteralValue::*;
        match (left, right) {
            (Array(l), Array(r)) => self.combine_arrays(l, r, |a, b| self.numeric_binary(a, b, f)),
            (Array(arr), v) => self.map_array(arr, |x| self.numeric_binary(x, v.clone(), f)),
            (v, Array(arr)) => self.map_array(arr, |x| self.numeric_binary(v.clone(), x, f)),
            (l, r) => {
                let a = self.coerce_number(&l);
                let b = self.coerce_number(&r);
                match (a, b) {
                    (Ok(a), Ok(b)) => Ok(Number(f(a, b))),
                    (Err(e), _) | (_, Err(e)) => Ok(LiteralValue::Error(e)),
                }
            }
        }
    }

    fn divide(&self, left: LiteralValue, right: LiteralValue) -> Result<LiteralValue, ExcelError> {
        let denom_num = |v: &LiteralValue| self.coerce_number(v);
        use LiteralValue::*;
        match (left, right) {
            (Array(l), Array(r)) => self.combine_arrays(l, r, |a, b| self.divide(a, b)),
            (Array(arr), v) => self.map_array(arr, |x| self.divide(x, v.clone())),
            (v, Array(arr)) => self.map_array(arr, |x| self.divide(v.clone(), x)),
            (l, r) => {
                let d = denom_num(&r);
                if matches!(d, Ok(n) if n == 0.0) {
                    return Ok(LiteralValue::Error(ExcelError::from_error_string(
                        "#DIV/0!",
                    )));
                }
                let (ln, rn) = match (self.coerce_number(&l), d) {
                    (Ok(a), Ok(b)) => (a, b),
                    (Err(e), _) | (_, Err(e)) => {
                        return Ok(LiteralValue::Error(e));
                    }
                };
                Ok(LiteralValue::Number(ln / rn))
            }
        }
    }

    fn power(&self, left: LiteralValue, right: LiteralValue) -> Result<LiteralValue, ExcelError> {
        let try_pow = |a: f64, b: f64| {
            if a < 0.0 && b.fract() != 0.0 {
                None
            } else {
                Some(a.powf(b))
            }
        };
        self.numeric_binary(left, right, |a, b| try_pow(a, b).unwrap_or(f64::NAN))
            .map(|v| {
                if let LiteralValue::Number(n) = &v {
                    if n.is_nan() || n.is_infinite() {
                        return LiteralValue::Error(ExcelError::from_error_string("#NUM!"));
                    }
                }
                v
            })
    }

    fn map_array<F>(&self, arr: Vec<Vec<LiteralValue>>, f: F) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(LiteralValue) -> Result<LiteralValue, ExcelError> + Copy,
    {
        let mut out = Vec::with_capacity(arr.len());
        for row in arr {
            let mut new_row = Vec::with_capacity(row.len());
            for cell in row {
                new_row.push(match f(cell) {
                    Ok(v) => v,
                    Err(e) => LiteralValue::Error(e),
                });
            }
            out.push(new_row);
        }
        Ok(LiteralValue::Array(out))
    }

    fn combine_arrays<F>(
        &self,
        l: Vec<Vec<LiteralValue>>,
        r: Vec<Vec<LiteralValue>>,
        f: F,
    ) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(LiteralValue, LiteralValue) -> Result<LiteralValue, ExcelError> + Copy,
    {
        let rows = l.len().max(r.len());
        let cols = l
            .iter()
            .map(|r| r.len())
            .max()
            .unwrap_or(0)
            .max(r.iter().map(|r| r.len()).max().unwrap_or(0));
        let mut out = Vec::with_capacity(rows);
        for i in 0..rows {
            let mut row = Vec::with_capacity(cols);
            for j in 0..cols {
                let lv = l
                    .get(i)
                    .and_then(|r| r.get(j))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                let rv = r
                    .get(i)
                    .and_then(|r| r.get(j))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                row.push(match f(lv, rv) {
                    Ok(v) => v,
                    Err(e) => LiteralValue::Error(e),
                });
            }
            out.push(row);
        }
        Ok(LiteralValue::Array(out))
    }

    /* ---------- coercion helpers ---------- */
    fn coerce_number(&self, v: &LiteralValue) -> Result<f64, ExcelError> {
        use LiteralValue::*;
        match v {
            Number(n) => Ok(*n),
            Int(i) => Ok(*i as f64),
            Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
            Text(s) => s.trim().parse::<f64>().map_err(|_| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Cannot convert '{}' to number", s))
            }),
            Empty => Ok(0.0),
            _ if v.as_serial_number().is_some() => Ok(v.as_serial_number().unwrap()),
            Error(_) => Err(ExcelError::new(ExcelErrorKind::Value)),
            _ => Err(ExcelError::new(ExcelErrorKind::Value)),
        }
    }

    fn coerce_text(&self, v: &LiteralValue) -> String {
        use LiteralValue::*;
        match v {
            Text(s) => s.clone(),
            Number(n) => n.to_string(),
            Int(i) => i.to_string(),
            Boolean(b) => if *b { "TRUE" } else { "FALSE" }.into(),
            Error(e) => e.to_string(),
            Empty => "".into(),
            _ => format!("{:?}", v),
        }
    }

    /* ---------- comparison ---------- */
    fn compare(
        &self,
        op: &str,
        left: LiteralValue,
        right: LiteralValue,
    ) -> Result<LiteralValue, ExcelError> {
        use LiteralValue::*;
        if matches!(left, Error(_)) {
            return Ok(left);
        }
        if matches!(right, Error(_)) {
            return Ok(right);
        }

        // arrays: element‑wise
        match (left, right) {
            (Array(l), Array(r)) => self.combine_arrays(l, r, |a, b| self.compare(op, a, b)),
            (Array(arr), v) => self.map_array(arr, |x| self.compare(op, x, v.clone())),
            (v, Array(arr)) => self.map_array(arr, |x| self.compare(op, v.clone(), x)),
            (l, r) => {
                let res = match (l, r) {
                    (Number(a), Number(b)) => self.cmp_f64(a, b, op),
                    (Int(a), Number(b)) => self.cmp_f64(a as f64, b, op),
                    (Number(a), Int(b)) => self.cmp_f64(a, b as f64, op),
                    (Boolean(a), Boolean(b)) => {
                        self.cmp_f64(if a { 1.0 } else { 0.0 }, if b { 1.0 } else { 0.0 }, op)
                    }
                    (Text(a), Text(b)) => self.cmp_text(&a, &b, op),
                    (a, b) => {
                        // fallback to numeric coercion or text compare
                        let an = self.coerce_number(&a).ok();
                        let bn = self.coerce_number(&b).ok();
                        if let (Some(a), Some(b)) = (an, bn) {
                            self.cmp_f64(a, b, op)
                        } else {
                            self.cmp_text(&self.coerce_text(&a), &self.coerce_text(&b), op)
                        }
                    }
                };
                Ok(LiteralValue::Boolean(res))
            }
        }
    }

    fn cmp_f64(&self, a: f64, b: f64, op: &str) -> bool {
        match op {
            "=" => a == b,
            "<>" => a != b,
            ">" => a > b,
            "<" => a < b,
            ">=" => a >= b,
            "<=" => a <= b,
            _ => unreachable!(),
        }
    }
    fn cmp_text(&self, a: &str, b: &str, op: &str) -> bool {
        let (a, b) = (a.to_ascii_lowercase(), b.to_ascii_lowercase());
        self.cmp_f64(
            a.cmp(&b) as i32 as f64,
            0.0,
            match op {
                "=" => "=",
                "<>" => "<>",
                ">" => ">",
                "<" => "<",
                ">=" => ">=",
                "<=" => "<=",
                _ => unreachable!(),
            },
        )
    }
}
