use crate::{
    CellRef,
    broadcast::{broadcast_shape, project_index},
    traits::{ArgumentHandle, DefaultFunctionContext, EvaluationContext},
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::sync::Arc;

pub struct Interpreter<'a> {
    pub context: &'a dyn EvaluationContext,
    current_sheet: &'a str,
    current_cell: Option<crate::CellRef>,
    // Per-evaluation caches (interior mutability for &self API)
    subexpr_cache: RefCell<FxHashMap<u64, LiteralValue>>, // key: AST fingerprint
    // Cache only fully-owned ranges to avoid re-resolving identical references within a cell
    // Key: (effective_sheet, structural reference fingerprint)
    owned_range_cache: RefCell<FxHashMap<(String, u64), Arc<[Vec<LiteralValue>]>>>,
}

impl<'a> Interpreter<'a> {
    pub fn new(context: &'a dyn EvaluationContext, current_sheet: &'a str) -> Self {
        Self {
            context,
            current_sheet,
            current_cell: None,
            subexpr_cache: RefCell::new(FxHashMap::default()),
            owned_range_cache: RefCell::new(FxHashMap::default()),
        }
    }

    pub fn new_with_cell(
        context: &'a dyn EvaluationContext,
        current_sheet: &'a str,
        cell: crate::CellRef,
    ) -> Self {
        Self {
            context,
            current_sheet,
            current_cell: Some(cell),
            subexpr_cache: RefCell::new(FxHashMap::default()),
            owned_range_cache: RefCell::new(FxHashMap::default()),
        }
    }

    pub fn current_sheet(&self) -> &'a str {
        self.current_sheet
    }

    /// Resolve a reference with a small per-interpreter cache for fully-owned ranges.
    /// Streaming ranges are returned as-is (not cached) to avoid materializing large data.
    pub fn resolve_range_storage_cached<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<crate::engine::range_stream::RangeStorage<'c>, ExcelError> {
        use crate::engine::range_stream::RangeStorage;
        // Determine effective sheet key for cache
        let sheet_key = match reference {
            ReferenceType::Cell { sheet, .. } | ReferenceType::Range { sheet, .. } => {
                sheet.clone().unwrap_or_else(|| current_sheet.to_string())
            }
            ReferenceType::Table(_) | ReferenceType::NamedRange(_) => current_sheet.to_string(),
        };
        let ref_fp = {
            // Use a structural fingerprint of a synthetic AST node wrapping this reference
            let ast = ASTNode::new(
                ASTNodeType::Reference {
                    original: String::new(),
                    reference: reference.clone(),
                },
                None,
            );
            ast.fingerprint()
        };

        // Fast path: owned range present in cache
        if let Some(arc_rows) = self
            .owned_range_cache
            .borrow()
            .get(&(sheet_key.clone(), ref_fp))
            .cloned()
        {
            // Return a fresh owned clone to avoid RefCell borrow/lifetime complexities
            let data: Vec<Vec<LiteralValue>> = (*arc_rows).to_vec();
            return Ok(RangeStorage::Owned(std::borrow::Cow::Owned(data)));
        }

        // Resolve via context
        match self.context.resolve_range_storage(reference, current_sheet)? {
            RangeStorage::Owned(rows) => {
                // Materialize into Arc<[Vec<LiteralValue>]> and cache
                let owned: Vec<Vec<LiteralValue>> = rows.into_owned();
                let arc: Arc<[Vec<LiteralValue>]> = Arc::from(owned.into_boxed_slice());
                self.owned_range_cache
                    .borrow_mut()
                    .insert((sheet_key, ref_fp), arc.clone());
                let data: Vec<Vec<LiteralValue>> = (*arc).to_vec();
                Ok(RangeStorage::Owned(std::borrow::Cow::Owned(data)))
            }
            other @ RangeStorage::Stream(_) => Ok(other),
        }
    }

    /// Evaluate an AST node in a reference context and return a ReferenceType.
    /// This is used for range combinators (e.g., ":"), by-ref argument flows,
    /// and spill planning. Functions that can return references must set
    /// `FnCaps::RETURNS_REFERENCE` and override `eval_reference`.
    pub fn evaluate_ast_as_reference(&self, node: &ASTNode) -> Result<ReferenceType, ExcelError> {
        match &node.node_type {
            ASTNodeType::Reference { reference, .. } => Ok(reference.clone()),
            ASTNodeType::Function { name, args } => {
                if let Some(fun) = self.context.get_function("", name) {
                    // Build handles; allow function to decide reference semantics
                    let handles: Vec<ArgumentHandle> =
                        args.iter().map(|n| ArgumentHandle::new(n, self)).collect();
                    let fctx = DefaultFunctionContext::new(self.context, None);
                    if let Some(res) = fun.eval_reference(&handles, &fctx) {
                        res
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("Function does not return a reference"))
                    }
                } else {
                    Err(ExcelError::from(ExcelErrorKind::Name))
                }
            }
            ASTNodeType::Array(_)
            | ASTNodeType::UnaryOp { .. }
            | ASTNodeType::BinaryOp { .. }
            | ASTNodeType::Literal(_) => Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Expression cannot be used as a reference")),
        }
    }

    /* ===================  public  =================== */
    pub fn evaluate_ast(&self, node: &ASTNode) -> Result<LiteralValue, ExcelError> {
        if !node.contains_volatile() {
            let fp = node.fingerprint();
            if let Some(v) = self.subexpr_cache.borrow().get(&fp) {
                return Ok(v.clone());
            }
            let out = self.evaluate_ast_uncached(node)?;
            self.subexpr_cache.borrow_mut().insert(fp, out.clone());
            return Ok(out);
        }
        self.evaluate_ast_uncached(node)
    }

    fn evaluate_ast_uncached(&self, node: &ASTNode) -> Result<LiteralValue, ExcelError> {
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
        match self
            .context
            .resolve_range_storage(reference, self.current_sheet)
        {
            Ok(storage) => {
                // For a single cell reference, just return the value.
                if let ReferenceType::Cell { .. } = reference {
                    return Ok(storage
                        .to_iterator()
                        .next()
                        .map(|cow| cow.into_owned())
                        .unwrap_or(LiteralValue::Empty));
                }

                // For ranges, materialize into an array.
                let data: Vec<Vec<LiteralValue>> = match storage {
                    crate::engine::range_stream::RangeStorage::Owned(cow) => cow.into_owned(),
                    crate::engine::range_stream::RangeStorage::Stream(mut stream) => {
                        let (rows, cols) = stream.dimensions();
                        let mut data = Vec::with_capacity(rows as usize);
                        for _ in 0..rows {
                            let mut row_data = Vec::with_capacity(cols as usize);
                            for _ in 0..cols {
                                row_data.push(
                                    stream
                                        .next()
                                        .map(|c| c.into_owned())
                                        .unwrap_or(LiteralValue::Empty),
                                );
                            }
                            data.push(row_data);
                        }
                        data
                    }
                };

                if data.len() == 1 && data[0].len() == 1 {
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
                Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(format!("Unary op '{op}'")))
            }
        }
    }

    fn apply_number_unary<F>(&self, v: LiteralValue, f: F) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(f64) -> f64,
    {
        match crate::coercion::to_number_lenient_with_locale(&v, &self.context.locale()) {
            Ok(n) => match crate::coercion::sanitize_numeric(f(n)) {
                Ok(n2) => Ok(LiteralValue::Number(n2)),
                Err(e) => Ok(LiteralValue::Error(e)),
            },
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
                crate::coercion::to_text_invariant(&l_val),
                crate::coercion::to_text_invariant(&r_val)
            ))),
            ":" => {
                // Compute a combined reference; in value context return #REF! for now.
                let lref = self.evaluate_ast_as_reference(left)?;
                let rref = self.evaluate_ast_as_reference(right)?;
                match crate::reference::combine_references(&lref, &rref) {
                    Ok(_r) => Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                        "Reference produced by ':' cannot be used directly as a value",
                    )),
                    Err(e) => Ok(LiteralValue::Error(e)),
                }
            }
            _ => {
                Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message(format!("Binary op '{op}'")))
            }
        }
    }

    /* ===================  function calls  =================== */
    fn eval_function(&self, name: &str, args: &[ASTNode]) -> Result<LiteralValue, ExcelError> {
        if let Some(fun) = self.context.get_function("", name) {
            let handles: Vec<ArgumentHandle> =
                args.iter().map(|n| ArgumentHandle::new(n, self)).collect();
            // Use the function's built-in dispatch method with a narrow FunctionContext
            let fctx = DefaultFunctionContext::new(self.context, self.current_cell);
            fun.dispatch(&handles, &fctx)
        } else {
            Ok(LiteralValue::Error(ExcelError::from_error_string("#NAME?")))
        }
    }

    pub fn function_context(&self, cell_ref: Option<&CellRef>) -> DefaultFunctionContext<'_> {
        DefaultFunctionContext::new(self.context, cell_ref.cloned())
    }

    // Test-only helpers to introspect cache sizes
    #[cfg(test)]
    pub fn debug_subexpr_cache_len(&self) -> usize {
        self.subexpr_cache.borrow().len()
    }
    #[cfg(test)]
    pub fn debug_owned_range_cache_len(&self) -> usize {
        self.owned_range_cache.borrow().len()
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
        self.broadcast_apply(left, right, |l, r| {
            let a = crate::coercion::to_number_lenient_with_locale(&l, &self.context.locale());
            let b = crate::coercion::to_number_lenient_with_locale(&r, &self.context.locale());
            match (a, b) {
                (Ok(a), Ok(b)) => match crate::coercion::sanitize_numeric(f(a, b)) {
                    Ok(n2) => Ok(LiteralValue::Number(n2)),
                    Err(e) => Ok(LiteralValue::Error(e)),
                },
                (Err(e), _) | (_, Err(e)) => Ok(LiteralValue::Error(e)),
            }
        })
    }

    fn divide(&self, left: LiteralValue, right: LiteralValue) -> Result<LiteralValue, ExcelError> {
        self.broadcast_apply(left, right, |l, r| {
            let ln = crate::coercion::to_number_lenient_with_locale(&l, &self.context.locale());
            let rn = crate::coercion::to_number_lenient_with_locale(&r, &self.context.locale());
            let (a, b) = match (ln, rn) {
                (Ok(a), Ok(b)) => (a, b),
                (Err(e), _) | (_, Err(e)) => return Ok(LiteralValue::Error(e)),
            };
            if b == 0.0 {
                return Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#DIV/0!",
                )));
            }
            match crate::coercion::sanitize_numeric(a / b) {
                Ok(n) => Ok(LiteralValue::Number(n)),
                Err(e) => Ok(LiteralValue::Error(e)),
            }
        })
    }

    fn power(&self, left: LiteralValue, right: LiteralValue) -> Result<LiteralValue, ExcelError> {
        self.broadcast_apply(left, right, |l, r| {
            let ln = crate::coercion::to_number_lenient_with_locale(&l, &self.context.locale());
            let rn = crate::coercion::to_number_lenient_with_locale(&r, &self.context.locale());
            let (a, b) = match (ln, rn) {
                (Ok(a), Ok(b)) => (a, b),
                (Err(e), _) | (_, Err(e)) => return Ok(LiteralValue::Error(e)),
            };
            // Excel domain: negative base with non-integer exponent -> #NUM!
            if a < 0.0 && b.fract() != 0.0 {
                return Ok(LiteralValue::Error(ExcelError::from_error_string("#NUM!")));
            }
            match crate::coercion::sanitize_numeric(a.powf(b)) {
                Ok(n) => Ok(LiteralValue::Number(n)),
                Err(e) => Ok(LiteralValue::Error(e)),
            }
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
        // Use strict broadcasting across dimensions
        let l_shape = (l.len(), l.first().map(|r| r.len()).unwrap_or(0));
        let r_shape = (r.len(), r.first().map(|r| r.len()).unwrap_or(0));
        let target = match broadcast_shape(&[l_shape, r_shape]) {
            Ok(s) => s,
            Err(e) => return Ok(LiteralValue::Error(e)),
        };

        let mut out = Vec::with_capacity(target.0);
        for i in 0..target.0 {
            let mut row = Vec::with_capacity(target.1);
            for j in 0..target.1 {
                let (li, lj) = project_index((i, j), l_shape);
                let (ri, rj) = project_index((i, j), r_shape);
                let lv = l
                    .get(li)
                    .and_then(|r| r.get(lj))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                let rv = r
                    .get(ri)
                    .and_then(|r| r.get(rj))
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

    fn broadcast_apply<F>(
        &self,
        left: LiteralValue,
        right: LiteralValue,
        f: F,
    ) -> Result<LiteralValue, ExcelError>
    where
        F: Fn(LiteralValue, LiteralValue) -> Result<LiteralValue, ExcelError> + Copy,
    {
        use LiteralValue::*;
        match (left, right) {
            (Array(l), Array(r)) => self.combine_arrays(l, r, f),
            (Array(arr), v) => {
                let shape_l = (arr.len(), arr.first().map(|r| r.len()).unwrap_or(0));
                let shape_r = (1usize, 1usize);
                let target = match broadcast_shape(&[shape_l, shape_r]) {
                    Ok(s) => s,
                    Err(e) => return Ok(LiteralValue::Error(e)),
                };
                let mut out = Vec::with_capacity(target.0);
                for i in 0..target.0 {
                    let mut row = Vec::with_capacity(target.1);
                    for j in 0..target.1 {
                        let (li, lj) = project_index((i, j), shape_l);
                        let lv = arr
                            .get(li)
                            .and_then(|r| r.get(lj))
                            .cloned()
                            .unwrap_or(LiteralValue::Empty);
                        row.push(match f(lv, v.clone()) {
                            Ok(vv) => vv,
                            Err(e) => LiteralValue::Error(e),
                        });
                    }
                    out.push(row);
                }
                Ok(LiteralValue::Array(out))
            }
            (v, Array(arr)) => {
                let shape_l = (1usize, 1usize);
                let shape_r = (arr.len(), arr.first().map(|r| r.len()).unwrap_or(0));
                let target = match broadcast_shape(&[shape_l, shape_r]) {
                    Ok(s) => s,
                    Err(e) => return Ok(LiteralValue::Error(e)),
                };
                let mut out = Vec::with_capacity(target.0);
                for i in 0..target.0 {
                    let mut row = Vec::with_capacity(target.1);
                    for j in 0..target.1 {
                        let (ri, rj) = project_index((i, j), shape_r);
                        let rv = arr
                            .get(ri)
                            .and_then(|r| r.get(rj))
                            .cloned()
                            .unwrap_or(LiteralValue::Empty);
                        row.push(match f(v.clone(), rv) {
                            Ok(vv) => vv,
                            Err(e) => LiteralValue::Error(e),
                        });
                    }
                    out.push(row);
                }
                Ok(LiteralValue::Array(out))
            }
            (l, r) => f(l, r),
        }
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
                    .with_message(format!("Cannot convert '{s}' to number"))
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
            _ => format!("{v:?}"),
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

        // arrays: element‑wise with broadcasting
        match (left, right) {
            (Array(l), Array(r)) => self.combine_arrays(l, r, |a, b| self.compare(op, a, b)),
            (Array(arr), v) => self.broadcast_apply(Array(arr), v, |a, b| self.compare(op, a, b)),
            (v, Array(arr)) => self.broadcast_apply(v, Array(arr), |a, b| self.compare(op, a, b)),
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
                        let an = crate::coercion::to_number_lenient_with_locale(
                            &a,
                            &self.context.locale(),
                        )
                        .ok();
                        let bn = crate::coercion::to_number_lenient_with_locale(
                            &b,
                            &self.context.locale(),
                        )
                        .ok();
                        if let (Some(a), Some(b)) = (an, bn) {
                            self.cmp_f64(a, b, op)
                        } else {
                            self.cmp_text(
                                &crate::coercion::to_text_invariant(&a),
                                &crate::coercion::to_text_invariant(&b),
                                op,
                            )
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
        let loc = self.context.locale();
        let (a, b) = (loc.fold_case_invariant(a), loc.fold_case_invariant(b));
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
