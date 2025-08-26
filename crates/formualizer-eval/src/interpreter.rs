use crate::{
    broadcast::{broadcast_shape, project_index}, coercion, traits::{ArgumentHandle, DefaultFunctionContext, EvaluationContext}, CellRef
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};
// no Arc needed here after cache removal

pub struct Interpreter<'a> {
    pub context: &'a dyn EvaluationContext,
    current_sheet: &'a str,
    current_cell: Option<crate::CellRef>,
}

impl<'a> Interpreter<'a> {
    pub fn new(context: &'a dyn EvaluationContext, current_sheet: &'a str) -> Self {
        Self {
            context,
            current_sheet,
            current_cell: None,
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
        }
    }

    pub fn current_sheet(&self) -> &'a str {
        self.current_sheet
    }

    pub fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<crate::engine::range_view::RangeView<'c>, ExcelError> {
        self.context.resolve_range_view(reference, current_sheet)
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
                    Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Unknown function: {}", name)))
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
        self.evaluate_ast_uncached(node)
    }

    fn evaluate_ast_uncached(&self, node: &ASTNode) -> Result<LiteralValue, ExcelError> {
        // Plan-aware evaluation: build a plan for this node and execute accordingly.
        // Provide the planner with a lightweight range-dimension probe and function lookup
        // so it can select chunked reduction and arg-parallel strategies where appropriate.
        let current_sheet = self.current_sheet.to_string();
        let range_probe = |reference: &ReferenceType| -> Option<(u32, u32)> {
            // Mirror Engine::resolve_range_storage bound normalization without materialising
            use formualizer_core::parser::ReferenceType as RT;
            match reference {
                RT::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                } => {
                    let sheet_name = sheet.as_deref().unwrap_or(&current_sheet);
                    // Start with provided values, fill None from used-region or sheet bounds.
                    let mut sr = *start_row;
                    let mut sc = *start_col;
                    let mut er = *end_row;
                    let mut ec = *end_col;

                    // Column-only: rows are None on both ends
                    if sr.is_none() && er.is_none() {
                        // Full-column reference: anchor at row 1 for alignment across columns
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        sr = Some(1);
                        if let Some((_, max_r)) =
                            self.context.used_rows_for_columns(sheet_name, scv, ecv)
                        {
                            er = Some(max_r);
                        } else if let Some((max_rows, _)) = self.context.sheet_bounds(sheet_name) {
                            er = Some(max_rows);
                        }
                    }

                    // Row-only: cols are None on both ends
                    if sc.is_none() && ec.is_none() {
                        // Full-row reference: anchor at column 1 for alignment across rows
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        sc = Some(1);
                        if let Some((_, max_c)) =
                            self.context.used_cols_for_rows(sheet_name, srv, erv)
                        {
                            ec = Some(max_c);
                        } else if let Some((_, max_cols)) = self.context.sheet_bounds(sheet_name) {
                            ec = Some(max_cols);
                        }
                    }

                    // Partially bounded (e.g., A1:A or A:A10)
                    if sr.is_some() && er.is_none() {
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        if let Some((_, max_r)) =
                            self.context.used_rows_for_columns(sheet_name, scv, ecv)
                        {
                            er = Some(max_r);
                        } else if let Some((max_rows, _)) = self.context.sheet_bounds(sheet_name) {
                            er = Some(max_rows);
                        }
                    }
                    if er.is_some() && sr.is_none() {
                        // Open start: anchor at row 1
                        sr = Some(1);
                    }
                    if sc.is_some() && ec.is_none() {
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        if let Some((_, max_c)) =
                            self.context.used_cols_for_rows(sheet_name, srv, erv)
                        {
                            ec = Some(max_c);
                        } else if let Some((_, max_cols)) = self.context.sheet_bounds(sheet_name) {
                            ec = Some(max_cols);
                        }
                    }
                    if ec.is_some() && sc.is_none() {
                        // Open start: anchor at column 1
                        sc = Some(1);
                    }

                    let sr = sr.unwrap_or(1);
                    let sc = sc.unwrap_or(1);
                    let er = er.unwrap_or(sr.saturating_sub(1));
                    let ec = ec.unwrap_or(sc.saturating_sub(1));
                    if er < sr || ec < sc {
                        return Some((0, 0));
                    }
                    Some((er.saturating_sub(sr) + 1, ec.saturating_sub(sc) + 1))
                }
                RT::Cell { .. } => Some((1, 1)),
                _ => None,
            }
        };
        let fn_lookup = |ns: &str, name: &str| self.context.get_function(ns, name);

        let mut planner = crate::planner::Planner::new(crate::planner::PlanConfig::default())
            .with_range_probe(&range_probe)
            .with_function_lookup(&fn_lookup);
        let plan = planner.plan(node);
        self.eval_with_plan(node, &plan.root)
    }

    fn eval_with_plan(
        &self,
        node: &ASTNode,
        plan_node: &crate::planner::PlanNode,
    ) -> Result<LiteralValue, ExcelError> {
        match &node.node_type {
            ASTNodeType::Literal(v) => Ok(v.clone()),
            ASTNodeType::Reference { reference, .. } => self.eval_reference(reference),
            ASTNodeType::UnaryOp { op, expr } => {
                // For now, reuse existing unary implementation (which recurses).
                // In a later phase, we can map plan_node.children[0].
                self.eval_unary(op, expr)
            }
            ASTNodeType::BinaryOp { op, left, right } => self.eval_binary(op, left, right),
            ASTNodeType::Function { name, args } => {
                let strategy = plan_node.strategy;
                if let Some(fun) = self.context.get_function("", name) {
                    use crate::function::FnCaps;
                    use crate::planner::ExecStrategy;
                    let caps = fun.caps();

                    // Short-circuit or volatile: always sequential
                    if caps.contains(FnCaps::SHORT_CIRCUIT) || caps.contains(FnCaps::VOLATILE) {
                        return self.eval_function(name, args);
                    }

                    // Chunked reduce for windowed functions
                    if matches!(strategy, ExecStrategy::ChunkedReduce)
                        && caps.contains(FnCaps::WINDOWED)
                    {
                        let handles: Vec<ArgumentHandle> =
                            args.iter().map(|n| ArgumentHandle::new(n, self)).collect();
                        let fctx = DefaultFunctionContext::new(self.context, self.current_cell);
                        let mut w = crate::window_ctx::SimpleWindowCtx::new(
                            &handles,
                            &fctx,
                            crate::window_ctx::WindowSpec::default(),
                        );
                        if let Some(res) = fun.eval_window(&mut w) {
                            return res;
                        }
                        // Fallback to scalar/dispatch if window not implemented
                        return self.eval_function(name, args);
                    }

                    // Arg-parallel: prewarm subexpressions and then dispatch
                    if matches!(strategy, ExecStrategy::ArgParallel)
                        && caps.contains(FnCaps::PARALLEL_ARGS)
                    {
                        // Sequential prewarm of subexpressions (safe without Sync bounds)
                        for arg in args {
                            match &arg.node_type {
                                ASTNodeType::Reference { reference, .. } => {
                                    let _ = self
                                        .context
                                        .resolve_range_view(reference, self.current_sheet);
                                }
                                _ => {
                                    let _ = self.evaluate_ast(arg);
                                }
                            }
                        }
                        return self.eval_function(name, args);
                    }

                    // Default path
                    return self.eval_function(name, args);
                }
                self.eval_function(name, args)
            }
            ASTNodeType::Array(rows) => self.eval_array_literal(rows),
        }
    }

    /* ===================  reference  =================== */
    fn eval_reference(&self, reference: &ReferenceType) -> Result<LiteralValue, ExcelError> {
        let view = self
            .context
            .resolve_range_view(reference, self.current_sheet)?;

        match reference {
            ReferenceType::Cell { .. } => {
                // For a single cell reference, just return the value.
                Ok(view.as_1x1().unwrap_or(LiteralValue::Empty))
            }
            _ => {
                // For ranges, materialize into an array.
                let (rows, cols) = view.dims();
                let mut data = Vec::with_capacity(rows);

                view.for_each_row(&mut |row| {
                    let row_data: Vec<LiteralValue> = (0..cols)
                        .map(|c| row.get(c).cloned().unwrap_or(LiteralValue::Empty))
                        .collect();
                    data.push(row_data);
                    Ok(())
                })?;

                if data.len() == 1 && data[0].len() == 1 {
                    Ok(data[0][0].clone())
                } else {
                    Ok(LiteralValue::Array(data))
                }
            }
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
            // Include the function name in the error message for better debugging
            Ok(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Unknown function: {}", name)),
            ))
        }
    }

    pub fn function_context(&self, cell_ref: Option<&CellRef>) -> DefaultFunctionContext<'_> {
        DefaultFunctionContext::new(self.context, cell_ref.cloned())
    }

    // Test-only helpers removed: interpreter no longer maintains subexpression cache
    // owned range cache removed in RangeView migration

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
                return Ok(LiteralValue::Error(ExcelError::new_num()));
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
        coercion::to_number_lenient(v)
    }

    fn coerce_text(&self, v: &LiteralValue) -> String {
        coercion::to_text_invariant(v)
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
