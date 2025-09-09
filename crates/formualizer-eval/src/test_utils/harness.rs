#![cfg(test)]

use crate::function::Function;
use crate::interpreter::Interpreter;
use crate::test_utils::test_workbook::TestWorkbook;
use crate::traits::ArgumentHandle;
use formualizer_parse::parser::ASTNode;
use std::sync::Arc;

pub fn interp(wb: &TestWorkbook) -> Interpreter<'_> {
    wb.interpreter()
}

pub fn get_fn(ctx: &Interpreter<'_>, name: &str) -> Arc<dyn Function> {
    ctx.context
        .get_function("", name)
        .expect("function not found")
}

/// Build stable `(ASTNode, ArgumentHandle)` from a formatter closure
/// Build vector of `ArgumentHandle` from AST nodes borrowed from caller.
pub fn handles_from_nodes<'a, 'b>(
    ctx: &'a Interpreter<'b>,
    nodes: &'a [ASTNode],
) -> Vec<ArgumentHandle<'a, 'b>> {
    nodes.iter().map(|n| ArgumentHandle::new(n, ctx)).collect()
}

/// Build vector of AST nodes from numbers (use `handles_from_nodes` to get handles).
pub fn nodes_from_nums(nums: &[f64]) -> Vec<ASTNode> {
    nums.iter()
        .map(|n| crate::test_utils::ast_builders::make_num_ast(*n))
        .collect()
}
